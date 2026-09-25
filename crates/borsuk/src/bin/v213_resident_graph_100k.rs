//! Frozen ReLAION-100k resident vector graph gate.

use std::{
    env,
    error::Error,
    fs::{self, File},
    io::{BufRead, BufReader, BufWriter, Read, Write},
    path::Path,
    time::Instant,
};

use borsuk::{resident_fp16_tier::ResidentFp16Tier, resident_vector_graph::ResidentVectorGraph};
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

const ROWS: usize = 100_000;
const DIMS: usize = 768;
const EFS: [usize; 4] = [256, 512, 1024, 2048];
const SOURCE: &str = "a199e151b89a496ed20e39fdd951591bbfb4817d682e9111ebe2e1cab7ae550d";

#[derive(Deserialize)]
struct Request {
    query_ordinal: usize,
    query: Vec<f32>,
}

fn digest(path: &Path) -> Result<String, Box<dyn Error>> {
    let mut input = BufReader::new(File::open(path)?);
    let mut hash = Sha256::new();
    let mut block = [0u8; 1024 * 1024];
    loop {
        let count = input.read(&mut block)?;
        if count == 0 {
            break;
        }
        hash.update(&block[..count]);
    }
    Ok(format!("{:x}", hash.finalize()))
}

fn percentile(values: &mut [u64], pct: usize) -> u64 {
    values.sort_unstable();
    values[(values.len() * pct).div_ceil(100) - 1]
}

fn memory_kib(field: &str) -> Result<u64, Box<dyn Error>> {
    let status = fs::read_to_string("/proc/self/status")?;
    Ok(status
        .lines()
        .find(|line| line.starts_with(field))
        .ok_or("status field missing")?
        .split_whitespace()
        .nth(1)
        .ok_or("status value missing")?
        .parse::<u64>()?)
}

fn main() -> Result<(), Box<dyn Error>> {
    let args = env::args().collect::<Vec<_>>();
    if args.len() != 7 {
        return Err(
            "usage: v213_resident_graph_100k PREP PLANE VECTORS REQUESTS RAW SERVING".into(),
        );
    }
    let prep: Value = serde_json::from_slice(&fs::read(&args[1])?)?;
    if prep["schema"] != "borsuk-v213-graph-preparation-v1"
        || prep["source_sha256"] != SOURCE
        || prep["rows"].as_u64() != Some(ROWS as u64)
        || prep["dimensions"].as_u64() != Some(DIMS as u64)
    {
        return Err("graph preparation identity differs".into());
    }
    if prep["plane_sha256"].as_str() != Some(digest(Path::new(&args[2]))?.as_str())
        || prep["vectors_sha256"].as_str() != Some(digest(Path::new(&args[3]))?.as_str())
    {
        return Err("graph input digest differs".into());
    }
    let load_started = Instant::now();
    let plane = ResidentFp16Tier::open_authenticated(
        Path::new(&args[2]),
        prep["plane_sha256"]
            .as_str()
            .ok_or("plane digest missing")?,
        SOURCE,
        ROWS as u64,
        DIMS,
        prep["generation"].as_u64().ok_or("generation missing")?,
        200_000_000,
    )?;
    let hydration_ns = load_started.elapsed().as_nanos() as u64;
    let build_started = Instant::now();
    let mut input = BufReader::new(File::open(&args[3])?);
    let mut row = vec![0u8; DIMS * 4];
    let mut vectors = Vec::with_capacity(ROWS);
    for _ in 0..ROWS {
        input.read_exact(&mut row)?;
        vectors.push(
            row.chunks_exact(4)
                .map(|bytes| f32::from_le_bytes(bytes.try_into().unwrap()))
                .collect(),
        );
    }
    if input.read(&mut [0u8; 1])? != 0 {
        return Err("source vector file too long".into());
    }
    let graph = ResidentVectorGraph::build(&vectors, &plane, 32, 64, 128)?;
    drop(vectors);
    let build_ns = build_started.elapsed().as_nanos() as u64;
    let build_peak_rss_bytes = memory_kib("VmHWM:")? * 1024;
    let graph_bytes = graph.heap_bytes();
    let serving_rss_bytes = memory_kib("VmRSS:")? * 1024;
    let requests = BufReader::new(File::open(&args[4])?)
        .lines()
        .take(256)
        .map(|line| -> Result<Request, Box<dyn Error>> { Ok(serde_json::from_str(&line?)?) })
        .collect::<Result<Vec<_>, _>>()?;
    if requests.len() != 256 {
        return Err("query panel length differs".into());
    }
    let mut output = BufWriter::new(File::create(&args[5])?);
    let mut times = [Vec::<u64>::new(), Vec::new(), Vec::new(), Vec::new()];
    let mut visits = [Vec::<u64>::new(), Vec::new(), Vec::new(), Vec::new()];
    let wall = Instant::now();
    for (index, request) in requests.iter().enumerate() {
        if request.query_ordinal != index || request.query.len() != DIMS {
            return Err("query identity differs".into());
        }
        let mut arms = serde_json::Map::new();
        for (arm, ef) in EFS.into_iter().enumerate() {
            let started = Instant::now();
            let (ids, visited) = graph.search_with_visits(&request.query, &plane, 100, ef)?;
            let elapsed = started.elapsed().as_nanos() as u64;
            times[arm].push(elapsed);
            visits[arm].push(visited as u64);
            arms.insert(
                ef.to_string(),
                json!({"returned_ids":ids,"whole_ns":elapsed,"base_visits":visited}),
            );
        }
        serde_json::to_writer(
            &mut output,
            &json!({"ordinal":index,"arms":arms,"vector_body_gets":0}),
        )?;
        output.write_all(b"\n")?;
    }
    output.flush()?;
    let wall_ns = wall.elapsed().as_nanos() as u64;
    let mut summaries = serde_json::Map::new();
    for (arm, ef) in EFS.into_iter().enumerate() {
        let arm_wall_ns = times[arm].iter().sum::<u64>();
        summaries.insert(
            ef.to_string(),
            json!({
                "p50_ns":percentile(&mut times[arm],50), "p95_ns":percentile(&mut times[arm],95),
                "p99_ns":percentile(&mut times[arm],99),
                "base_visits_p95":percentile(&mut visits[arm],95),
                "sequential_qps":256.0/(arm_wall_ns as f64/1e9),
            }),
        );
    }
    let serving = json!({"schema":"borsuk-v213-resident-graph-100k-v1",
        "dataset":"ReLAION-100k D768","split":"development-first-256-already-used",
        "queries":256,"construction":{"m":32,"m0":64,"ef_construction":128},
        "arms":summaries,"build_ns":build_ns,"build_peak_rss_bytes":build_peak_rss_bytes,
        "cold_hydration_ns":hydration_ns,"serving_rss_bytes":serving_rss_bytes,
        "graph_heap_bytes":graph_bytes,"plane_resident_bytes":plane.resident_bytes(),
        "wall_ns":wall_ns,"throughput_all_arms_qps":1024.0/(wall_ns as f64/1e9),
        "vector_body_gets":0,"raw_sha256":digest(Path::new(&args[5]))?});
    fs::write(&args[6], format!("{}\n", serving))?;
    Ok(())
}
