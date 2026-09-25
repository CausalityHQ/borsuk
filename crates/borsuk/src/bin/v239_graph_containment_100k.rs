//! Sealed candidate lists for the graph versus flat PQ64 containment falsifier.

use std::{
    env,
    error::Error,
    fs::{self, File},
    io::{BufRead, BufReader, BufWriter, Read, Write},
    path::Path,
    time::Instant,
};

use borsuk::{
    pq64_nominee::Pq64Router,
    resident_fp16_tier::ResidentFp16Tier,
    resident_vector_graph::{GraphSearchWorkspace, ResidentPqCosineGraph, ResidentVectorGraph},
};
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

const ROWS: usize = 100_000;
const DIMS: usize = 768;
const QUERIES: usize = 1_000;
const SHORTLIST: usize = 4_096;
const SOURCE: &str = "a199e151b89a496ed20e39fdd951591bbfb4817d682e9111ebe2e1cab7ae550d";

#[derive(Deserialize)]
struct Request {
    query_ordinal: usize,
    query: Vec<f32>,
}

fn digest(path: &Path) -> Result<String, Box<dyn Error>> {
    let mut input = BufReader::new(File::open(path)?);
    let mut sha = Sha256::new();
    let mut block = [0u8; 1024 * 1024];
    loop {
        let n = input.read(&mut block)?;
        if n == 0 {
            break;
        }
        sha.update(&block[..n]);
    }
    Ok(format!("{:x}", sha.finalize()))
}

fn main() -> Result<(), Box<dyn Error>> {
    let args = env::args().collect::<Vec<_>>();
    if args.len() != 11 {
        return Err("usage: v239_graph_containment_100k PREP BUILD PLANE GRAPH MAP BOOKS CODES REQUESTS RAW SUMMARY".into());
    }
    let prep: Value = serde_json::from_slice(&fs::read(&args[1])?)?;
    let build: Value = serde_json::from_slice(&fs::read(&args[2])?)?;
    if prep["schema"] != "borsuk-v212-rust-100k-preparation-v1"
        || build["schema"] != "borsuk-v218-reachable-graph-build-v1"
        || prep["source_sha256"] != SOURCE
        || build["source_sha256"] != SOURCE
        || prep["generation"] != build["generation"]
        || prep["plane_sha256"] != build["plane_sha256"]
        || prep["plane_sha256"].as_str() != Some(digest(Path::new(&args[3]))?.as_str())
        || build["graph_sha256"].as_str() != Some(digest(Path::new(&args[4]))?.as_str())
        || prep["new_to_old_sha256"].as_str() != Some(digest(Path::new(&args[5]))?.as_str())
        || prep["books_raw_sha256"].as_str() != Some(digest(Path::new(&args[6]))?.as_str())
        || prep["codes_raw_sha256"].as_str() != Some(digest(Path::new(&args[7]))?.as_str())
        || prep["requests_sha256"].as_str() != Some(digest(Path::new(&args[8]))?.as_str())
    {
        return Err("100k graph/PQ input authority differs".into());
    }
    let plane = ResidentFp16Tier::open_authenticated(
        Path::new(&args[3]),
        prep["plane_sha256"]
            .as_str()
            .ok_or("plane digest missing")?,
        SOURCE,
        ROWS as u64,
        DIMS,
        prep["generation"].as_u64().ok_or("generation missing")?,
        200_000_000,
    )?;
    let graph = ResidentVectorGraph::open_authenticated(
        Path::new(&args[4]),
        build["graph_sha256"]
            .as_str()
            .ok_or("graph digest missing")?,
        &plane,
    )?;
    let raw_map = fs::read(&args[5])?;
    if raw_map.len() != ROWS * 4 {
        return Err("physical map length differs".into());
    }
    let old_for_new = raw_map
        .chunks_exact(4)
        .map(|b| u32::from_le_bytes(b.try_into().unwrap()) as usize)
        .collect::<Vec<_>>();
    let raw_books = fs::read(&args[6])?;
    if raw_books.len() != 64 * 256 * 12 * 4 {
        return Err("PQ books length differs".into());
    }
    let books = raw_books
        .chunks_exact(4)
        .map(|b| f32::from_le_bytes(b.try_into().unwrap()))
        .collect::<Vec<_>>();
    let codes = fs::read(&args[7])?;
    let pq = Pq64Router::new(
        ROWS,
        DIMS,
        256,
        1,
        vec![0.0; ROWS.div_ceil(256) * DIMS],
        books,
        codes,
    )
    .map_err(|e| format!("PQ construction: {e:?}"))?;
    let cosine = pq.cosine_view().map_err(|e| format!("PQ cosine: {e:?}"))?;
    let bound = ResidentPqCosineGraph::bind(&graph, &plane, &cosine, &old_for_new)?;
    let requests = BufReader::new(File::open(&args[8])?)
        .lines()
        .map(|line| -> Result<Request, Box<dyn Error>> { Ok(serde_json::from_str(&line?)?) })
        .collect::<Result<Vec<_>, _>>()?;
    if requests.len() != QUERIES
        || requests.iter().enumerate().any(|(i, r)| {
            r.query_ordinal != i || r.query.len() != DIMS || r.query.iter().any(|x| !x.is_finite())
        })
    {
        return Err("request panel differs".into());
    }
    let mut workspace = GraphSearchWorkspace::new(ROWS)?;
    let mut raw = BufWriter::new(File::create(&args[9])?);
    let wall = Instant::now();
    for (ordinal, request) in requests.iter().enumerate() {
        let start = Instant::now();
        let (graph_rows, graph_visits) =
            bound.nominate(&request.query, 100, SHORTLIST, SHORTLIST, &mut workspace)?;
        let graph_ns = start.elapsed().as_nanos() as u64;
        let start = Instant::now();
        let prepared = cosine
            .prepare_query(&request.query)
            .map_err(|e| format!("PQ query: {e:?}"))?;
        let mut flat = (0..ROWS)
            .map(|physical| -> Result<(f32, usize), Box<dyn Error>> {
                Ok((
                    prepared
                        .score_row(old_for_new[physical])
                        .map_err(|e| format!("PQ row: {e:?}"))?,
                    physical,
                ))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let by_score = |a: &(f32, usize), b: &(f32, usize)| b.0.total_cmp(&a.0).then(a.1.cmp(&b.1));
        flat.select_nth_unstable_by(SHORTLIST - 1, by_score);
        flat.truncate(SHORTLIST);
        flat.sort_unstable_by(by_score);
        let flat_ns = start.elapsed().as_nanos() as u64;
        let flat_rows = flat.iter().map(|&(_, row)| row).collect::<Vec<_>>();
        let graph_ids = graph_rows
            .iter()
            .map(|&row| plane.source_id(row))
            .collect::<Result<Vec<_>, _>>()?;
        let flat_ids = flat_rows
            .iter()
            .map(|&row| plane.source_id(row))
            .collect::<Result<Vec<_>, _>>()?;
        serde_json::to_writer(
            &mut raw,
            &json!({"ordinal":ordinal,
                "graph_ids":graph_ids,"flat_ids":flat_ids,
                "graph_rows":graph_rows,"flat_rows":flat_rows,
            "graph_visits":graph_visits,"graph_ns":graph_ns,"flat_ns":flat_ns}),
        )?;
        raw.write_all(b"\n")?;
    }
    raw.flush()?;
    fs::write(
        &args[10],
        serde_json::to_vec(&json!({
            "schema":"borsuk-v239-graph-containment-100k-v1",
            "dataset":"ReLAION-100k D768", "queries":QUERIES, "k":100,
            "ef":SHORTLIST, "max_shortlist":SHORTLIST,
            "source_sha256":SOURCE, "graph_sha256":build["graph_sha256"],
            "raw_sha256":digest(Path::new(&args[9]))?,
            "wall_ns":wall.elapsed().as_nanos() as u64,
        }))?,
    )?;
    Ok(())
}
