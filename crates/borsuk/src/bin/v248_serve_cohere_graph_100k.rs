//! Frozen CoHere-100k source-only graph/PQ/FP16 transfer gate.

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
const WORKERS: usize = 8;
const ARMS: [(usize, usize); 3] = [(2048, 2048), (4096, 4096), (8192, 8192)];

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
    let exact_nav = args.len() == 12 && args[11] == "--exact-nav";
    let anchored = args.len() == 12 && args[11] == "--strided-anchors";
    if args.len() != 11 && !exact_nav && !anchored {
        return Err("usage: v248_serve_cohere_graph_100k PREP BUILD PLANE GRAPH MAP BOOKS CODES REQUESTS RAW SERVING [--exact-nav|--strided-anchors]".into());
    }
    let prep: Value = serde_json::from_slice(&fs::read(&args[1])?)?;
    let build: Value = serde_json::from_slice(&fs::read(&args[2])?)?;
    let source = prep["source_sha256"]
        .as_str()
        .ok_or("source hash missing")?;
    if prep["schema"] != "borsuk-v248-source-preparation-v1"
        || prep["dataset_id"] != "cohere-large-10m-768"
        || prep["staging_receipt_sha256"]
            != "0965aa0241199822dfac3410bba4edad5536ac0eb0aaa8ab83c216e8c5749a87"
        || (build["schema"] != "borsuk-v248-cohere-graph-build-v1"
            && build["schema"] != "borsuk-v249-cohere-pq-aligned-graph-build-v1"
            && build["schema"] != "borsuk-v250-cohere-diverse-graph-build-v1")
        || ((exact_nav || anchored)
            && build["schema"] != "borsuk-v250-cohere-diverse-graph-build-v1")
        || build["source_sha256"] != source
        || prep["artifacts"]["plane.bin"]["sha256"] != build["plane_sha256"]
        || prep["rows"].as_u64() != Some(ROWS as u64)
        || prep["dimensions"].as_u64() != Some(DIMS as u64)
        || prep["generation"] != build["generation"]
        || prep["artifacts"]["plane.bin"]["sha256"].as_str()
            != Some(digest(Path::new(&args[3]))?.as_str())
        || build["graph_sha256"].as_str() != Some(digest(Path::new(&args[4]))?.as_str())
        || prep["artifacts"]["map.u32"]["sha256"].as_str()
            != Some(digest(Path::new(&args[5]))?.as_str())
        || prep["artifacts"]["books.bin"]["sha256"].as_str()
            != Some(digest(Path::new(&args[6]))?.as_str())
        || prep["artifacts"]["codes.bin"]["sha256"].as_str()
            != Some(digest(Path::new(&args[7]))?.as_str())
    {
        return Err("CoHere-100k graph/PQ serving identity differs".into());
    }
    let started = Instant::now();
    let plane = ResidentFp16Tier::open_authenticated(
        Path::new(&args[3]),
        prep["artifacts"]["plane.bin"]["sha256"]
            .as_str()
            .ok_or("plane digest missing")?,
        source,
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
    let arms = if anchored {
        vec![(512, 0)]
    } else if exact_nav {
        vec![(512, 0), (1024, 0), (2048, 0)]
    } else {
        ARMS.to_vec()
    };
    let mut expected = vec![vec![Vec::<u64>::new(); QUERIES]; arms.len()];
    let mut times = vec![Vec::<u64>::with_capacity(QUERIES); arms.len()];
    let mut visits = vec![Vec::<u64>::with_capacity(QUERIES); arms.len()];
    let mut raw = BufWriter::new(File::create(&args[9])?);
    let sequential_wall = Instant::now();
    for (index, request) in requests.iter().enumerate() {
        let mut row_arms = serde_json::Map::new();
        for offset in 0..arms.len() {
            let arm = (index + offset) % arms.len();
            let (ef, shortlist) = arms[arm];
            let start = Instant::now();
            let (ids, count) = if anchored {
                graph.search_with_strided_anchors_workspace(
                    &request.query,
                    &plane,
                    100,
                    ef,
                    256,
                    &mut workspace,
                )?
            } else if exact_nav {
                graph.search_with_workspace(&request.query, &plane, 100, ef, &mut workspace)?
            } else {
                bound.search(&request.query, 100, ef, shortlist, &mut workspace)?
            };
            let elapsed = start.elapsed().as_nanos() as u64;
            times[arm].push(elapsed);
            visits[arm].push(count as u64);
            expected[arm][index] = ids.clone();
            row_arms.insert(
                if anchored {
                    format!("anchor-256-{ef}")
                } else if exact_nav {
                    format!("exact-{ef}")
                } else {
                    format!("{ef}-{shortlist}")
                },
                json!({"returned_ids":ids,"whole_ns":elapsed,"base_visits":count}),
            );
        }
        serde_json::to_writer(
            &mut raw,
            &json!({"ordinal":index,"arms":row_arms,"vector_body_gets":0}),
        )?;
        raw.write_all(b"\n")?;
    }
    raw.flush()?;
    let sequential_wall_ns = sequential_wall.elapsed().as_nanos() as u64;
    drop(workspace);

    let mut summaries = serde_json::Map::new();
    for (arm, (ef, shortlist)) in arms.into_iter().enumerate() {
        let sequential_sum = times[arm].iter().sum::<u64>();
        let loaded_wall = Instant::now();
        let mut loaded = Vec::with_capacity(QUERIES);
        std::thread::scope(|scope| -> Result<(), Box<dyn Error>> {
            let mut handles = Vec::with_capacity(WORKERS);
            for worker in 0..WORKERS {
                let bound = &bound;
                let graph = &graph;
                let plane = &plane;
                let requests = &requests;
                let expected = &expected[arm];
                handles.push(scope.spawn(move || -> Result<Vec<u64>, String> {
                    let mut workspace =
                        GraphSearchWorkspace::new(ROWS).map_err(|e| e.to_string())?;
                    let mut worker_times = Vec::new();
                    for index in (worker..QUERIES).step_by(WORKERS) {
                        let start = Instant::now();
                        let (ids, _) = if anchored {
                            graph.search_with_strided_anchors_workspace(
                                &requests[index].query,
                                plane,
                                100,
                                ef,
                                256,
                                &mut workspace,
                            )
                        } else if exact_nav {
                            graph.search_with_workspace(
                                &requests[index].query,
                                plane,
                                100,
                                ef,
                                &mut workspace,
                            )
                        } else {
                            bound.search(&requests[index].query, 100, ef, shortlist, &mut workspace)
                        }
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
            if anchored { format!("anchor-256-{ef}") } else if exact_nav { format!("exact-{ef}") } else { format!("{ef}-{shortlist}") },
            json!({
                "sequential":{"p50_ns":percentile(&mut times[arm],50),
                    "p90_ns":percentile(&mut times[arm],90),"p95_ns":percentile(&mut times[arm],95),"p99_ns":percentile(&mut times[arm],99),
                    "qps":QUERIES as f64/(sequential_sum as f64/1e9)},
                "loaded":{"p50_ns":percentile(&mut loaded,50),
                    "p90_ns":percentile(&mut loaded,90),"p95_ns":percentile(&mut loaded,95),"p99_ns":percentile(&mut loaded,99),
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
                "schema":if anchored {
                    "borsuk-v252-cohere-strided-anchor-v1"
                } else if exact_nav {
                    "borsuk-v251-cohere-fp16-navigation-v1"
                } else if build["schema"] == "borsuk-v250-cohere-diverse-graph-build-v1" {
                    "borsuk-v250-cohere-diverse-graph-100k-serving-v1"
                } else if build["schema"] == "borsuk-v249-cohere-pq-aligned-graph-build-v1" {
                    "borsuk-v249-cohere-pq-aligned-graph-100k-serving-v1"
                } else {"borsuk-v248-cohere-graph-100k-serving-v1"},
                "dataset":"CoHere-100k D768 cosine","split":"development-256-plus-validation-744-prior-used",
                "queries":QUERIES,"workers":WORKERS,"arms":summaries,
                "cold_hydration_ns":hydration_ns,"hydrated_rss_bytes":hydrated_rss_bytes,
                "process_peak_rss_bytes":memory("VmHWM:")?,
                "graph_heap_bytes":graph.heap_bytes(),"plane_resident_bytes":plane.resident_bytes(),
                "pq_code_bytes":ROWS*64,"pq_cosine_norm_bytes":cosine.resident_bytes(),
                "physical_map_bytes":old_for_new.capacity()*8,
                "worker_workspace_bytes":workspace_bytes*WORKERS,
                "anchor_scores_per_query":if anchored {256} else {0},
                "sequential_wall_ns":sequential_wall_ns,"vector_body_gets":0,
                "raw_sha256":digest(Path::new(&args[9]))?,
                "requests_sha256":digest(Path::new(&args[8]))?,
            })
        ),
    )?;
    Ok(())
}
