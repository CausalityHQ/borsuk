//! Frozen Rust library API runner for the V271 fresh-query campaign.

use std::{
    env,
    error::Error,
    fs::{self, File},
    io::{BufReader, BufWriter, Read, Write},
    path::Path,
    time::Instant,
};

use borsuk::{
    resident_graph_build::build_graph_generation,
    resident_graph_generation::ResidentGraphGeneration,
    resident_graph_store::{hydrate_graph_generation, read_graph_head},
    resident_vector_graph::GraphSearchWorkspace,
};
use object_store::parse_url_opts;
use serde_json::json;
use url::Url;

const DIMS: usize = 768;
fn resident_cap(root: &[u8]) -> Result<usize, Box<dyn Error>> {
    let rows = serde_json::from_slice::<serde_json::Value>(root)?["rows"]
        .as_u64()
        .ok_or("root row count")?;
    Ok(usize::try_from(rows)?
        .checked_mul(2_500)
        .ok_or("resident cap overflow")?)
}

fn vectors(path: &Path, rows: usize) -> Result<Vec<(u64, Vec<f32>)>, Box<dyn Error>> {
    if !matches!(rows, 100_000 | 1_000_000 | 10_000_000)
        || fs::metadata(path)?.len() < (rows * DIMS * 4) as u64
    {
        return Err("source geometry".into());
    }
    let mut source = BufReader::new(File::open(path)?);
    let mut bytes = vec![0_u8; DIMS * 4];
    let mut data = Vec::with_capacity(rows);
    for id in 0..rows {
        source.read_exact(&mut bytes)?;
        let row = bytes
            .chunks_exact(4)
            .map(|word| f32::from_le_bytes(word.try_into().unwrap()))
            .collect::<Vec<_>>();
        data.push((id as u64, row));
    }
    Ok(data)
}

fn queries(path: &Path, expected: usize) -> Result<Vec<Vec<f32>>, Box<dyn Error>> {
    if fs::metadata(path)?.len() != (expected * DIMS * 4) as u64 {
        return Err("query geometry".into());
    }
    let bytes = fs::read(path)?;
    let mut result = Vec::with_capacity(expected);
    for row in bytes.chunks_exact(DIMS * 4) {
        let query = row
            .chunks_exact(4)
            .map(|word| f32::from_le_bytes(word.try_into().unwrap()))
            .collect::<Vec<_>>();
        if query.iter().any(|value| !value.is_finite()) || !query.iter().any(|value| *value != 0.0)
        {
            return Err("invalid query".into());
        }
        result.push(query);
    }
    Ok(result)
}

fn search(
    generation: ResidentGraphGeneration,
    query_path: &Path,
    expected: usize,
    raw_path: &Path,
    hydration: serde_json::Value,
    stress: bool,
) -> Result<(), Box<dyn Error>> {
    let panel = queries(query_path, expected)?;
    let mut worker = if stress {
        None
    } else {
        Some(generation.searcher()?)
    };
    let view = if stress {
        Some(generation.cosine_view()?)
    } else {
        None
    };
    let bound = view
        .as_ref()
        .map(|view| generation.bind(view))
        .transpose()?;
    let mut workspace = if stress {
        Some(GraphSearchWorkspace::new(generation.rows())?)
    } else {
        None
    };
    let mut run = |query: &[f32]| {
        if let (Some(bound), Some(workspace)) = (&bound, &mut workspace) {
            generation.search_dual_graph(bound, query, 100, 256, 256, 128, workspace)
        } else {
            worker.as_mut().unwrap().search(query, 100)
        }
    };
    run(&panel[0])?;
    let mut output = BufWriter::new(File::create(raw_path)?);
    let started = Instant::now();
    for (ordinal, query) in panel.iter().enumerate() {
        let query_started = Instant::now();
        let (ids, visits) = run(query)?;
        let latency_ns = query_started.elapsed().as_nanos() as u64;
        serde_json::to_writer(
            &mut output,
            &json!({"ordinal":ordinal,"ids":ids,"visits":visits,"latency_ns":latency_ns}),
        )?;
        output.write_all(b"\n")?;
    }
    output.flush()?;
    println!(
        "{}",
        json!({"root_sha256":generation.root_sha256(),"generation":generation.generation(),
            "rows":generation.rows(),"dimensions":generation.dimensions(),
            "queries":expected,"elapsed_ms":started.elapsed().as_secs_f64()*1000.0,
            "mode":if stress {"diagnostic-stress"} else {"default"},
            "hydration":hydration})
    );
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = env::args().collect::<Vec<_>>();
    match args.get(1).map(String::as_str) {
        Some("build") if args.len() == 5 => {
            let rows: usize = args[3].parse()?;
            let started = Instant::now();
            let sha = build_graph_generation(Path::new(&args[4]), vectors(Path::new(&args[2]), rows)?, 1)?;
            println!("{}",json!({"root_sha256":sha,"rows":rows,
                "build_ms":started.elapsed().as_secs_f64()*1000.0}));
        }
        Some("local" | "local_stress") if args.len() == 7 => {
            let root = fs::read(Path::new(&args[2]).join("root.json"))?;
            let cap = resident_cap(&root)?;
            let generation = ResidentGraphGeneration::open_local_authenticated(
                &root, &args[3], Path::new(&args[2]), cap, 1,
            )?;
            search(generation, Path::new(&args[4]), args[5].parse()?,
                Path::new(&args[6]), json!({"object_gets":0,"response_bytes":0}),
                args[1] == "local_stress")?;
        }
        Some("s3") if args.len() == 8 => {
            let (store, prefix) = parse_url_opts(&Url::parse(&args[2])?,
                [("aws_region", "eu-central-1")])?;
            let head = read_graph_head(store.as_ref(), &prefix).await?.ok_or("head missing")?;
            if head.root_sha256 != args[3] { return Err("wrong generation root".into()); }
            let cap = resident_cap(&head.root_bytes)?;
            let started = Instant::now();
            let (generation, stats) = hydrate_graph_generation(store.as_ref(), &prefix,
                &head, Path::new(&args[4]), cap, 1).await?;
            search(generation, Path::new(&args[5]), args[6].parse()?,
                Path::new(&args[7]), json!({"object_gets":stats.object_gets,
                    "response_bytes":stats.response_bytes,
                    "elapsed_ms":started.elapsed().as_secs_f64()*1000.0}), false)?;
        }
        _ => return Err("usage: resident_graph_frontier build RAW ROWS DIR | local|local_stress DIR TRUSTED_SHA QUERIES COUNT RAW_OUT | s3 URI TRUSTED_SHA CACHE QUERIES COUNT RAW_OUT".into()),
    }
    Ok(())
}
