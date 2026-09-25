//! Frozen ReLAION-1M reachable-graph/PQ/FP16 request gate.

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

const ROWS: usize = 1_000_000;
const DIMS: usize = 768;
const QUERIES: usize = 1_000;
const WORKERS: usize = 8;
const SOURCE: &str = "2796b579f37afe99ca4aff57e282335a6a79ad30596645957d26326a0560cf86";
const ARMS: [(usize, usize); 4] = [(2048, 2048), (4096, 4096), (8192, 8192), (16384, 16384)];

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

fn memory(field: &str) -> Result<u64, Box<dyn Error>> {
    let status = fs::read_to_string("/proc/self/status")?;
    Ok(status
        .lines()
        .find(|line| line.starts_with(field))
        .ok_or("RSS field missing")?
        .split_whitespace()
        .nth(1)
        .ok_or("RSS value missing")?
        .parse::<u64>()?
        * 1024)
}

fn main() -> Result<(), Box<dyn Error>> {
    let args = env::args().collect::<Vec<_>>();
    if args.len() != 11 {
        return Err("usage: v219_serve_reachable_graph_1m PREP BUILD PLANE GRAPH MAP BOOKS CODES REQUESTS RAW SERVING".into());
    }
    let prep: Value = serde_json::from_slice(&fs::read(&args[1])?)?;
    let build: Value = serde_json::from_slice(&fs::read(&args[2])?)?;
    if prep["schema"] != "borsuk-v217-graph-1m-preparation-v1"
        || build["schema"] != "borsuk-v219-reachable-graph-build-1m-v1"
        || prep["source_sha256"] != SOURCE
        || build["source_sha256"] != SOURCE
        || prep["plane_sha256"] != build["plane_sha256"]
        || prep["generation"] != build["generation"]
        || prep["plane_sha256"].as_str() != Some(digest(Path::new(&args[3]))?.as_str())
        || build["graph_sha256"].as_str() != Some(digest(Path::new(&args[4]))?.as_str())
        || prep["map_sha256"].as_str() != Some(digest(Path::new(&args[5]))?.as_str())
        || prep["books_sha256"].as_str() != Some(digest(Path::new(&args[6]))?.as_str())
        || prep["codes_sha256"].as_str() != Some(digest(Path::new(&args[7]))?.as_str())
        || prep["requests_sha256"].as_str() != Some(digest(Path::new(&args[8]))?.as_str())
    {
        return Err("1M graph/PQ serving identity differs".into());
    }
    let started = Instant::now();
    let plane = ResidentFp16Tier::open_authenticated(
        Path::new(&args[3]),
        prep["plane_sha256"]
            .as_str()
            .ok_or("plane digest missing")?,
        SOURCE,
        ROWS as u64,
        DIMS,
        prep["generation"].as_u64().ok_or("generation missing")?,
        2_000_000_000,
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
    .map_err(|error| format!("PQ construction: {error:?}"))?;
    let cosine = pq
        .cosine_view()
        .map_err(|error| format!("PQ cosine: {error:?}"))?;
    let bound = ResidentPqCosineGraph::bind(&graph, &plane, &cosine, &old_for_new)?;
    let hydration_ns = started.elapsed().as_nanos() as u64;
    let hydrated_rss_bytes = memory("VmRSS:")?;
    let requests = BufReader::new(File::open(&args[8])?)
        .lines()
        .map(|line| -> Result<Request, Box<dyn Error>> { Ok(serde_json::from_str(&line?)?) })
        .collect::<Result<Vec<_>, _>>()?;
    if requests.len() != QUERIES
        || requests.iter().enumerate().any(|(index, row)| {
            row.query_ordinal != index
                || row.query.len() != DIMS
                || row.query.iter().any(|x| !x.is_finite())
        })
    {
        return Err("request panel differs".into());
    }

    let mut workspace = GraphSearchWorkspace::new(ROWS)?;
    let workspace_bytes = workspace.resident_bytes();
    let mut expected = vec![vec![Vec::<u64>::new(); QUERIES]; ARMS.len()];
    let mut times = vec![Vec::<u64>::with_capacity(QUERIES); ARMS.len()];
    let mut visits = vec![Vec::<u64>::with_capacity(QUERIES); ARMS.len()];
    let mut raw = BufWriter::new(File::create(&args[9])?);
    let sequential_wall = Instant::now();
    for (index, request) in requests.iter().enumerate() {
        let mut arms = serde_json::Map::new();
        for offset in 0..ARMS.len() {
            let arm = (index + offset) % ARMS.len();
            let (ef, shortlist) = ARMS[arm];
            let start = Instant::now();
            let (ids, count) = bound.search(&request.query, 100, ef, shortlist, &mut workspace)?;
            let elapsed = start.elapsed().as_nanos() as u64;
            times[arm].push(elapsed);
            visits[arm].push(count as u64);
            expected[arm][index] = ids.clone();
            arms.insert(
                format!("{ef}-{shortlist}"),
                json!({"returned_ids":ids,"whole_ns":elapsed,"base_visits":count}),
            );
        }
        serde_json::to_writer(
            &mut raw,
            &json!({"ordinal":index,"arms":arms,"vector_body_gets":0}),
        )?;
        raw.write_all(b"\n")?;
    }
    raw.flush()?;
    let sequential_wall_ns = sequential_wall.elapsed().as_nanos() as u64;
    drop(workspace);

    let mut summaries = serde_json::Map::new();
    for (arm, (ef, shortlist)) in ARMS.into_iter().enumerate() {
        let sequential_sum = times[arm].iter().sum::<u64>();
        let loaded_wall = Instant::now();
        let mut loaded = Vec::with_capacity(QUERIES);
        std::thread::scope(|scope| -> Result<(), Box<dyn Error>> {
            let mut handles = Vec::with_capacity(WORKERS);
            for worker in 0..WORKERS {
                let bound = &bound;
                let requests = &requests;
                let expected = &expected[arm];
                handles.push(scope.spawn(move || -> Result<Vec<u64>, String> {
                    let mut workspace =
                        GraphSearchWorkspace::new(ROWS).map_err(|e| e.to_string())?;
                    let mut worker_times = Vec::new();
                    for index in (worker..QUERIES).step_by(WORKERS) {
                        let start = Instant::now();
                        let (ids, _) = bound
                            .search(&requests[index].query, 100, ef, shortlist, &mut workspace)
                            .map_err(|e| e.to_string())?;
                        let elapsed = start.elapsed().as_nanos() as u64;
                        if ids != expected[index] {
                            return Err(format!("loaded ID mismatch at {index}"));
                        }
                        worker_times.push(elapsed);
                    }
                    Ok(worker_times)
                }));
            }
            for handle in handles {
                let value = handle
                    .join()
                    .map_err(|_| "loaded worker panic")?
                    .map_err(|error| format!("loaded worker: {error}"))?;
                loaded.extend(value);
            }
            Ok(())
        })?;
        if loaded.len() != QUERIES {
            return Err("loaded request count differs".into());
        }
        let loaded_wall_ns = loaded_wall.elapsed().as_nanos() as u64;
        summaries.insert(
            format!("{ef}-{shortlist}"),
            json!({
                "sequential":{"p50_ns":percentile(&mut times[arm],50),
                    "p90_ns":percentile(&mut times[arm],90),
                    "p95_ns":percentile(&mut times[arm],95),"p99_ns":percentile(&mut times[arm],99),
                    "qps":QUERIES as f64/(sequential_sum as f64/1e9)},
                "loaded":{"p50_ns":percentile(&mut loaded,50),
                    "p90_ns":percentile(&mut loaded,90),
                    "p95_ns":percentile(&mut loaded,95),"p99_ns":percentile(&mut loaded,99),
                    "qps":QUERIES as f64/(loaded_wall_ns as f64/1e9)},
                "base_visits_p95":percentile(&mut visits[arm],95),
            }),
        );
    }
    fs::write(
        &args[10],
        format!(
            "{}\n",
            json!({
                "schema":"borsuk-v219-reachable-graph-1m-serving-v1",
                "dataset":"ReLAION-1M D768","split":"validation-1000-already-used",
                "queries":QUERIES,"workers":WORKERS,"arms":summaries,
                "cold_hydration_ns":hydration_ns,"hydrated_rss_bytes":hydrated_rss_bytes,
                "process_peak_rss_bytes":memory("VmHWM:")?,
                "graph_heap_bytes":graph.heap_bytes(),"plane_resident_bytes":plane.resident_bytes(),
                "pq_code_bytes":ROWS*64,"pq_cosine_norm_bytes":cosine.resident_bytes(),
                "physical_map_bytes":old_for_new.capacity()*8,
                "worker_workspace_bytes":workspace_bytes*WORKERS,
                "sequential_wall_ns":sequential_wall_ns,"vector_body_gets":0,
                "raw_sha256":digest(Path::new(&args[9]))?,
            })
        ),
    )?;
    Ok(())
}
