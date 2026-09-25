//! Frozen metric-consistent PQ-cosine graph and resident FP16 gate.

use borsuk::{
    pq64_nominee::Pq64Router, resident_fp16_tier::ResidentFp16Tier,
    resident_vector_graph::ResidentVectorGraph,
};
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    env,
    error::Error,
    fs::{self, File},
    io::{BufRead, BufReader, BufWriter, Read, Write},
    path::Path,
    time::Instant,
};

const ROWS: usize = 100_000;
const DIMS: usize = 768;
const SOURCE: &str = "a199e151b89a496ed20e39fdd951591bbfb4817d682e9111ebe2e1cab7ae550d";
const ARMS: [(usize, usize); 5] = [
    (1024, 1024),
    (2048, 1024),
    (2048, 2048),
    (4096, 1024),
    (4096, 2048),
];

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
        let n = input.read(&mut block)?;
        if n == 0 {
            break;
        }
        hash.update(&block[..n]);
    }
    Ok(format!("{:x}", hash.finalize()))
}
fn percentile(values: &mut [u64], pct: usize) -> u64 {
    values.sort_unstable();
    values[(values.len() * pct).div_ceil(100) - 1]
}
fn rss() -> Result<u64, Box<dyn Error>> {
    let text = fs::read_to_string("/proc/self/status")?;
    Ok(text
        .lines()
        .find(|line| line.starts_with("VmRSS:"))
        .ok_or("VmRSS missing")?
        .split_whitespace()
        .nth(1)
        .ok_or("VmRSS value missing")?
        .parse::<u64>()?
        * 1024)
}

fn peak_rss() -> Result<u64, Box<dyn Error>> {
    let text = fs::read_to_string("/proc/self/status")?;
    Ok(text
        .lines()
        .find(|line| line.starts_with("VmHWM:"))
        .ok_or("VmHWM missing")?
        .split_whitespace()
        .nth(1)
        .ok_or("VmHWM value missing")?
        .parse::<u64>()?
        * 1024)
}
fn main() -> Result<(), Box<dyn Error>> {
    let args = env::args().collect::<Vec<_>>();
    if args.len() != 11 {
        return Err("usage: v215_serve_pq_cosine_graph_100k PREP BUILD PLANE GRAPH MAP BOOKS CODES REQUESTS RAW SERVING".into());
    }
    let prep: Value = serde_json::from_slice(&fs::read(&args[1])?)?;
    let build: Value = serde_json::from_slice(&fs::read(&args[2])?)?;
    if prep["schema"] != "borsuk-v212-rust-100k-preparation-v1"
        || build["schema"] != "borsuk-v214-graph-build-v1"
        || prep["source_sha256"] != SOURCE
        || build["source_sha256"] != SOURCE
        || prep["plane_sha256"] != build["plane_sha256"]
        || prep["generation"] != build["generation"]
        || prep["plane_sha256"].as_str() != Some(digest(Path::new(&args[3]))?.as_str())
        || build["graph_sha256"].as_str() != Some(digest(Path::new(&args[4]))?.as_str())
        || prep["new_to_old_sha256"].as_str() != Some(digest(Path::new(&args[5]))?.as_str())
        || prep["books_raw_sha256"].as_str() != Some(digest(Path::new(&args[6]))?.as_str())
        || prep["codes_raw_sha256"].as_str() != Some(digest(Path::new(&args[7]))?.as_str())
        || prep["requests_sha256"].as_str() != Some(digest(Path::new(&args[8]))?.as_str())
    {
        return Err("graph/PQ serving input identity differs".into());
    }
    let started = Instant::now();
    let plane = ResidentFp16Tier::open_authenticated(
        Path::new(&args[3]),
        prep["plane_sha256"].as_str().ok_or("plane hash missing")?,
        SOURCE,
        ROWS as u64,
        DIMS,
        prep["generation"].as_u64().ok_or("generation missing")?,
        200_000_000,
    )?;
    let graph = ResidentVectorGraph::open_authenticated(
        Path::new(&args[4]),
        build["graph_sha256"].as_str().ok_or("graph hash missing")?,
        &plane,
    )?;
    let mapping = fs::read(&args[5])?;
    if mapping.len() != ROWS * 4 {
        return Err("row map length differs".into());
    }
    let old_for_new = mapping
        .chunks_exact(4)
        .map(|b| u32::from_le_bytes(b.try_into().unwrap()) as usize)
        .collect::<Vec<_>>();
    let bytes = fs::read(&args[6])?;
    if bytes.len() != 64 * 256 * 12 * 4 {
        return Err("PQ books length differs".into());
    }
    let books = bytes
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
    let cosine = pq
        .cosine_view()
        .map_err(|e| format!("PQ cosine preparation: {e:?}"))?;
    let hydration_ns = started.elapsed().as_nanos() as u64;
    let serving_rss_bytes = rss()?;
    let requests = BufReader::new(File::open(&args[8])?)
        .lines()
        .take(256)
        .map(|line| -> Result<Request, Box<dyn Error>> { Ok(serde_json::from_str(&line?)?) })
        .collect::<Result<Vec<_>, _>>()?;
    if requests.len() != 256 {
        return Err("query panel length differs".into());
    }
    let mut out = BufWriter::new(File::create(&args[9])?);
    let mut times = [
        Vec::<u64>::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
    ];
    let mut visits = [
        Vec::<u64>::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
    ];
    let wall = Instant::now();
    for (index, request) in requests.iter().enumerate() {
        if request.query_ordinal != index || request.query.len() != DIMS {
            return Err("query identity differs".into());
        }
        let mut arms = serde_json::Map::new();
        for (arm, (ef, shortlist)) in ARMS.into_iter().enumerate() {
            let start = Instant::now();
            let (ids, visited) = graph.search_pq_cosine_with_visits(
                &request.query,
                &plane,
                &cosine,
                &old_for_new,
                100,
                ef,
                shortlist,
            )?;
            let elapsed = start.elapsed().as_nanos() as u64;
            times[arm].push(elapsed);
            visits[arm].push(visited as u64);
            arms.insert(
                format!("{ef}-{shortlist}"),
                json!({"returned_ids":ids,"whole_ns":elapsed,"base_visits":visited}),
            );
        }
        serde_json::to_writer(
            &mut out,
            &json!({"ordinal":index,"arms":arms,"vector_body_gets":0}),
        )?;
        out.write_all(b"\n")?;
    }
    out.flush()?;
    let wall_ns = wall.elapsed().as_nanos() as u64;
    let mut summaries = serde_json::Map::new();
    for (arm, (ef, shortlist)) in ARMS.into_iter().enumerate() {
        let arm_wall_ns = times[arm].iter().sum::<u64>();
        summaries.insert(
            format!("{ef}-{shortlist}"),
            json!({
        "p50_ns":percentile(&mut times[arm],50),"p95_ns":percentile(&mut times[arm],95),
        "p99_ns":percentile(&mut times[arm],99),"base_visits_p95":percentile(&mut visits[arm],95),
        "sequential_qps":256.0/(arm_wall_ns as f64/1e9)}),
        );
    }
    fs::write(
        &args[10],
        format!(
            "{}\n",
            json!({
    "schema":"borsuk-v215-pq-cosine-graph-100k-v1","dataset":"ReLAION-100k D768",
    "split":"development-first-256-already-used","queries":256,"arms":summaries,
        "cold_hydration_ns":hydration_ns,"serving_rss_bytes":serving_rss_bytes,
        "serving_peak_rss_bytes":peak_rss()?,
    "graph_heap_bytes":graph.heap_bytes(),"plane_resident_bytes":plane.resident_bytes(),
    "pq_code_bytes":ROWS*64,"pq_cosine_norm_bytes":cosine.resident_bytes(),
    "vector_body_gets":0,"wall_ns":wall_ns,
    "throughput_all_arms_qps":1280.0/(wall_ns as f64/1e9),
    "raw_sha256":digest(Path::new(&args[9]))?})
        ),
    )?;
    Ok(())
}
