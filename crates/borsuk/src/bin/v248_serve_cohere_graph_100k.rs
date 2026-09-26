//! Frozen CoHere-100k source-only graph/PQ/FP16 transfer gate.

use std::{
    collections::HashSet,
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

fn graph_schema_matches(rows: usize, schema: &str, special: bool) -> bool {
    match rows {
        100_000 => {
            matches!(schema, "borsuk-v248-cohere-graph-build-v1"
                | "borsuk-v249-cohere-pq-aligned-graph-build-v1"
                | "borsuk-v250-cohere-diverse-graph-build-v1")
                && (!special || schema == "borsuk-v250-cohere-diverse-graph-build-v1")
        }
        1_000_000 => schema == "borsuk-v255-cohere-diverse-graph-build-1m-v1" && !special,
        _ => false,
    }
}

struct CoarsePq {
    centroids: Vec<f32>,
    offsets: Vec<usize>,
    postings: Vec<usize>,
}

impl CoarsePq {
    fn open(args: &[String], prep: &Value) -> Result<Self, Box<dyn Error>> {
        let manifest: Value = serde_json::from_slice(&fs::read(&args[15])?)?;
        let rows = prep["rows"].as_u64().ok_or("coarse row count missing")? as usize;
        if !matches!(rows, 100_000 | 1_000_000) {
            return Err("coarse row count differs".into());
        }
        if manifest["schema"] != "borsuk-v254-coarse-pq-v1"
            || manifest["rows"].as_u64() != Some(rows as u64)
            || manifest["dimensions"].as_u64() != Some(DIMS as u64)
            || manifest["centroids"].as_u64() != Some(rows.div_ceil(256) as u64)
            || manifest["rows_per_cell"] != 256
            || manifest["copies"] != 2
            || manifest["seed"] != 254
            || manifest["lloyd_iterations"] != 6
            || manifest["source_sha256"] != prep["source_sha256"]
            || manifest["codes_sha256"] != prep["artifacts"]["codes.bin"]["sha256"]
        {
            return Err("coarse PQ authority differs".into());
        }
        for (name, path) in [
            ("centroids.f32", &args[12]),
            ("offsets.u32", &args[13]),
            ("postings.u32", &args[14]),
        ] {
            if manifest["artifacts"][name]["sha256"].as_str()
                != Some(digest(Path::new(path))?.as_str())
                || manifest["artifacts"][name]["bytes"].as_u64() != Some(fs::metadata(path)?.len())
            {
                return Err("coarse PQ artifact differs".into());
            }
        }
        let raw_centroids = fs::read(&args[12])?;
        let raw_offsets = fs::read(&args[13])?;
        let raw_postings = fs::read(&args[14])?;
        let cells = rows.div_ceil(256);
        if raw_centroids.len() != cells * DIMS * 4
            || raw_offsets.len() != (cells + 1) * 4
            || raw_postings.len() != rows * 2 * 4
        {
            return Err("coarse PQ byte geometry differs".into());
        }
        let centroids = raw_centroids
            .chunks_exact(4)
            .map(|x| f32::from_le_bytes(x.try_into().unwrap()))
            .collect::<Vec<_>>();
        let offsets = raw_offsets
            .chunks_exact(4)
            .map(|x| u32::from_le_bytes(x.try_into().unwrap()) as usize)
            .collect::<Vec<_>>();
        let postings = raw_postings
            .chunks_exact(4)
            .map(|x| u32::from_le_bytes(x.try_into().unwrap()) as usize)
            .collect::<Vec<_>>();
        if centroids.len() != cells * DIMS
            || centroids.iter().any(|x| !x.is_finite())
            || offsets.len() != cells + 1
            || offsets[0] != 0
            || offsets[cells] != rows * 2
            || postings.len() != rows * 2
            || offsets
                .windows(2)
                .any(|pair| pair[0] > pair[1] || pair[1] - pair[0] > 8 * postings.len() / cells)
        {
            return Err("coarse PQ geometry differs".into());
        }
        let mut counts = vec![0u8; rows];
        for pair in offsets.windows(2) {
            let mut previous = None;
            for &row in &postings[pair[0]..pair[1]] {
                if row >= rows || previous.is_some_and(|prior| row <= prior) {
                    return Err("coarse PQ posting order differs".into());
                }
                if counts[row] == 2 {
                    return Err("coarse PQ duplicate row".into());
                }
                counts[row] += 1;
                previous = Some(row);
            }
        }
        if counts.iter().any(|&count| count != 2) {
            return Err("coarse PQ posting multiplicity differs".into());
        }
        Ok(Self {
            centroids,
            offsets,
            postings,
        })
    }

    fn candidates(&self, query: &[f32], seen: &mut HashSet<usize>) -> Vec<usize> {
        let mut cells = self
            .centroids
            .chunks_exact(DIMS)
            .enumerate()
            .map(|(cell, centroid)| {
                let score = centroid
                    .iter()
                    .zip(query)
                    .map(|(&x, &y)| f64::from(x) * f64::from(y))
                    .sum::<f64>();
                (score, cell)
            })
            .collect::<Vec<_>>();
        cells.sort_unstable_by(|a, b| b.0.total_cmp(&a.0).then(a.1.cmp(&b.1)));
        seen.clear();
        let mut rows = Vec::with_capacity(20_000);
        for &(_, cell) in cells.iter().take(32) {
            for &row in &self.postings[self.offsets[cell]..self.offsets[cell + 1]] {
                if seen.insert(row) {
                    rows.push(row);
                }
            }
        }
        rows
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    let args = env::args().collect::<Vec<_>>();
    let exact_nav = args.len() == 12 && args[11] == "--exact-nav";
    let anchored = args.len() == 12 && args[11] == "--strided-anchors";
    let global_pq = args.len() == 12 && args[11] == "--global-pq";
    let coarse_pq = args.len() == 16 && args[11] == "--coarse-pq";
    let hybrid = args.len() == 16 && args[11] == "--hybrid";
    if args.len() != 11 && !exact_nav && !anchored && !global_pq && !coarse_pq && !hybrid {
        return Err("usage: v248_serve_cohere_graph_100k PREP BUILD PLANE GRAPH MAP BOOKS CODES REQUESTS RAW SERVING [--exact-nav|--strided-anchors|--global-pq|--coarse-pq|--hybrid CENTROIDS OFFSETS POSTINGS MANIFEST]".into());
    }
    let prep: Value = serde_json::from_slice(&fs::read(&args[1])?)?;
    let build: Value = serde_json::from_slice(&fs::read(&args[2])?)?;
    let rows = prep["rows"].as_u64().ok_or("row count missing")? as usize;
    let source = prep["source_sha256"]
        .as_str()
        .ok_or("source hash missing")?;
    if prep["schema"] != "borsuk-v248-source-preparation-v1"
        || prep["dataset_id"] != "cohere-large-10m-768"
        || prep["staging_receipt_sha256"]
            != "0965aa0241199822dfac3410bba4edad5536ac0eb0aaa8ab83c216e8c5749a87"
        || !graph_schema_matches(rows, build["schema"].as_str().unwrap_or(""),
            exact_nav || anchored || global_pq || coarse_pq)
        || (rows == 100_000 && hybrid
            && build["schema"] != "borsuk-v250-cohere-diverse-graph-build-v1")
        || build["source_sha256"] != source
        || prep["artifacts"]["plane.bin"]["sha256"] != build["plane_sha256"]
        || build["rows"].as_u64() != Some(rows as u64)
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
        return Err("CoHere graph/PQ serving identity differs".into());
    }
    let started = Instant::now();
    let plane = ResidentFp16Tier::open_authenticated(
        Path::new(&args[3]),
        prep["artifacts"]["plane.bin"]["sha256"]
            .as_str()
            .ok_or("plane digest missing")?,
        source,
        rows as u64,
        DIMS,
        prep["generation"].as_u64().ok_or("generation missing")?,
        rows * 2_000,
    )?;
    let graph = ResidentVectorGraph::open_authenticated(
        Path::new(&args[4]),
        build["graph_sha256"]
            .as_str()
            .ok_or("graph digest missing")?,
        &plane,
    )?;
    let raw_map = fs::read(&args[5])?;
    if raw_map.len() != rows * 4 {
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
        rows,
        DIMS,
        256,
        1,
        vec![0.0; rows.div_ceil(256) * DIMS],
        books,
        codes,
    )
    .map_err(|error| format!("PQ construction: {error:?}"))?;
    let cosine = pq
        .cosine_view()
        .map_err(|error| format!("PQ cosine: {error:?}"))?;
    let bound = ResidentPqCosineGraph::bind(&graph, &plane, &cosine, &old_for_new)?;
    let coarse = if coarse_pq || hybrid {
        Some(CoarsePq::open(&args, &prep)?)
    } else {
        None
    };
    let mut new_for_old = vec![0; rows];
    for (new, &old) in old_for_new.iter().enumerate() {
        new_for_old[old] = new;
    }
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

    let mut workspace = GraphSearchWorkspace::new(rows)?;
    let workspace_bytes = workspace.resident_bytes();
    let arms = if hybrid {
        vec![(4096, 8192)]
    } else if global_pq || coarse_pq {
        vec![(0, 8192)]
    } else if anchored {
        vec![(512, 0)]
    } else if exact_nav {
        vec![(512, 0), (1024, 0), (2048, 0)]
    } else if rows == 1_000_000 {
        vec![(4096, 4096)]
    } else {
        ARMS.to_vec()
    };
    let mut expected = vec![vec![Vec::<u64>::new(); QUERIES]; arms.len()];
    let mut times = vec![Vec::<u64>::with_capacity(QUERIES); arms.len()];
    let mut visits = vec![Vec::<u64>::with_capacity(QUERIES); arms.len()];
    let mut raw = BufWriter::new(File::create(&args[9])?);
    let mut coarse_seen = HashSet::with_capacity(20_000);
    let sequential_wall = Instant::now();
    for (index, request) in requests.iter().enumerate() {
        let mut row_arms = serde_json::Map::new();
        for offset in 0..arms.len() {
            let arm = (index + offset) % arms.len();
            let (ef, shortlist) = arms[arm];
            let start = Instant::now();
            let (ids, count) = if hybrid {
                let graph_result = bound.search(&request.query, 100, ef, 4096, &mut workspace)?;
                let rows = coarse.as_ref().unwrap().candidates(&request.query, &mut coarse_seen);
                let old = cosine.nominate_rows(&request.query, &rows, shortlist)
                    .map_err(|error| format!("hybrid PQ: {error:?}"))?;
                let mut physical = old.into_iter().map(|row| new_for_old[row]).collect::<Vec<_>>();
                for id in graph_result.0 {
                    let old = usize::try_from(id)?;
                    physical.push(*new_for_old.get(old).ok_or("hybrid graph id outside map")?);
                }
                physical.sort_unstable();
                physical.dedup();
                (plane.rank_ordinals_cosine(&request.query, &physical, 100)?,
                 graph_result.1 + rows.len())
            } else if coarse_pq {
                let rows = coarse
                    .as_ref()
                    .unwrap()
                    .candidates(&request.query, &mut coarse_seen);
                let count = rows.len();
                let old = cosine
                    .nominate_rows(&request.query, &rows, shortlist)
                    .map_err(|error| format!("coarse PQ: {error:?}"))?;
                let physical = old
                    .into_iter()
                    .map(|row| new_for_old[row])
                    .collect::<Vec<_>>();
                (
                    plane.rank_ordinals_cosine(&request.query, &physical, 100)?,
                    count,
                )
            } else if global_pq {
                let old = cosine
                    .nominate_global(&request.query, shortlist)
                    .map_err(|error| format!("global PQ: {error:?}"))?;
                let physical = old
                    .into_iter()
                    .map(|row| new_for_old[row])
                    .collect::<Vec<_>>();
                (
                    plane.rank_ordinals_cosine(&request.query, &physical, 100)?,
                    shortlist,
                )
            } else if anchored {
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
                if hybrid {
                    "hybrid-4096-8192".to_string()
                } else if coarse_pq {
                    format!("coarse-pq-32-{shortlist}")
                } else if global_pq {
                    format!("global-pq-{shortlist}")
                } else if anchored {
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
        let mut loaded_raw = Vec::with_capacity(QUERIES);
        std::thread::scope(|scope| -> Result<(), Box<dyn Error>> {
            let mut handles = Vec::with_capacity(WORKERS);
            for worker in 0..WORKERS {
                let bound = &bound;
                let graph = &graph;
                let plane = &plane;
                let cosine = &cosine;
                let coarse = &coarse;
                let new_for_old = &new_for_old;
                let requests = &requests;
                let expected = &expected[arm];
                handles.push(scope.spawn(move || -> Result<Vec<(usize, u64)>, String> {
                    let mut workspace =
                        GraphSearchWorkspace::new(rows).map_err(|e| e.to_string())?;
                    let mut coarse_seen = HashSet::with_capacity(20_000);
                    let mut worker_times = Vec::new();
                    for index in (worker..QUERIES).step_by(WORKERS) {
                        let start = Instant::now();
                        let (ids, _) = if hybrid {
                            let graph_result = bound.search(
                                &requests[index].query, 100, ef, 4096, &mut workspace)
                                .map_err(|e| e.to_string())?;
                            let rows = coarse.as_ref().unwrap().candidates(
                                &requests[index].query, &mut coarse_seen);
                            let old = cosine.nominate_rows(&requests[index].query, &rows, shortlist)
                                .map_err(|e| format!("hybrid PQ: {e:?}"))?;
                            let mut physical = old.into_iter().map(|row| new_for_old[row])
                                .collect::<Vec<_>>();
                            for id in graph_result.0 {
                                let old = usize::try_from(id).map_err(|e| e.to_string())?;
                                physical.push(*new_for_old.get(old)
                                    .ok_or("hybrid graph id outside map")?);
                            }
                            physical.sort_unstable();
                            physical.dedup();
                            plane.rank_ordinals_cosine(&requests[index].query, &physical, 100)
                                .map(|ids| (ids, graph_result.1 + rows.len()))
                        } else if coarse_pq {
                            let rows = coarse
                                .as_ref()
                                .unwrap()
                                .candidates(&requests[index].query, &mut coarse_seen);
                            let count = rows.len();
                            let old = cosine
                                .nominate_rows(&requests[index].query, &rows, shortlist)
                                .map_err(|error| format!("coarse PQ: {error:?}"))?;
                            let physical = old
                                .into_iter()
                                .map(|row| new_for_old[row])
                                .collect::<Vec<_>>();
                            plane
                                .rank_ordinals_cosine(&requests[index].query, &physical, 100)
                                .map(|ids| (ids, count))
                        } else if global_pq {
                            let old = cosine
                                .nominate_global(&requests[index].query, shortlist)
                                .map_err(|error| format!("global PQ: {error:?}"))?;
                            let physical = old
                                .into_iter()
                                .map(|row| new_for_old[row])
                                .collect::<Vec<_>>();
                            plane
                                .rank_ordinals_cosine(&requests[index].query, &physical, 100)
                                .map(|ids| (ids, shortlist))
                        } else if anchored {
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
                        worker_times.push((index, elapsed));
                    }
                    Ok(worker_times)
                }));
            }
            for handle in handles {
                let value = handle
                    .join()
                    .map_err(|_| "loaded worker panic")?
                    .map_err(|error| format!("loaded worker: {error}"))?;
                loaded.extend(value.iter().map(|&(_, elapsed)| elapsed));
                loaded_raw.extend(value);
            }
            Ok(())
        })?;
        if loaded.len() != QUERIES {
            return Err("loaded request count differs".into());
        }
        if hybrid {
            loaded_raw.sort_unstable_by_key(|&(index, _)| index);
            let mut out = BufWriter::new(File::create("loaded-raw.jsonl")?);
            for (index, elapsed) in loaded_raw {
                serde_json::to_writer(&mut out, &json!({"ordinal":index,
                    "arm":"hybrid-4096-8192","whole_ns":elapsed}))?;
                out.write_all(b"\n")?;
            }
            out.flush()?;
        }
        let loaded_wall_ns = loaded_wall.elapsed().as_nanos() as u64;
        summaries.insert(
            if hybrid { "hybrid-4096-8192".to_string() } else if coarse_pq { format!("coarse-pq-32-{shortlist}") } else if global_pq { format!("global-pq-{shortlist}") } else if anchored { format!("anchor-256-{ef}") } else if exact_nav { format!("exact-{ef}") } else { format!("{ef}-{shortlist}") },
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
                "schema":if hybrid {
                    if rows == 1_000_000 {"borsuk-v257-cohere-hybrid-1m-v1"}
                    else {"borsuk-v256-cohere-hybrid-v1"}
                } else if coarse_pq {
                    "borsuk-v254-cohere-coarse-pq-v1"
                } else if global_pq {
                    "borsuk-v253-cohere-global-pq-v1"
                } else if anchored {
                    "borsuk-v252-cohere-strided-anchor-v1"
                } else if exact_nav {
                    "borsuk-v251-cohere-fp16-navigation-v1"
                } else if rows == 1_000_000 {
                    "borsuk-v255-cohere-diverse-graph-1m-serving-v1"
                } else if build["schema"] == "borsuk-v250-cohere-diverse-graph-build-v1" {
                    "borsuk-v250-cohere-diverse-graph-100k-serving-v1"
                } else if build["schema"] == "borsuk-v249-cohere-pq-aligned-graph-build-v1" {
                    "borsuk-v249-cohere-pq-aligned-graph-100k-serving-v1"
                } else {"borsuk-v248-cohere-graph-100k-serving-v1"},
                "dataset":if rows == 1_000_000 {"CoHere-1M D768 cosine"} else {"CoHere-100k D768 cosine"},"split":"development-256-plus-validation-744-prior-used",
                "queries":QUERIES,"workers":WORKERS,"arms":summaries,
                "cold_hydration_ns":hydration_ns,"hydrated_rss_bytes":hydrated_rss_bytes,
                "process_peak_rss_bytes":memory("VmHWM:")?,
                "graph_heap_bytes":graph.heap_bytes(),"plane_resident_bytes":plane.resident_bytes(),
                "pq_code_bytes":rows*64,"pq_cosine_norm_bytes":cosine.resident_bytes(),
                "physical_map_bytes":old_for_new.capacity()*8,
                "worker_workspace_bytes":workspace_bytes*WORKERS,
                "anchor_scores_per_query":if anchored {256} else {0},
                "pq_scores_per_query":if global_pq {rows} else {0},
                "coarse_probe_cells":if coarse_pq || hybrid {32} else {0},
                "fp16_rows_per_query_upper_bound":if hybrid {12_388} else {0},
                "fp16_rows_per_query":if coarse_pq || global_pq {8192} else {0},
                "sequential_wall_ns":sequential_wall_ns,"vector_body_gets":0,
                "raw_sha256":digest(Path::new(&args[9]))?,
                "loaded_raw_sha256":if hybrid {Some(digest(Path::new("loaded-raw.jsonl"))?)} else {None},
                "requests_sha256":digest(Path::new(&args[8]))?,
            })
        ),
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::graph_schema_matches;

    #[test]
    fn million_row_hybrid_accepts_only_the_diverse_million_graph() {
        assert!(graph_schema_matches(1_000_000,
            "borsuk-v255-cohere-diverse-graph-build-1m-v1", false));
        assert!(!graph_schema_matches(1_000_000,
            "borsuk-v250-cohere-diverse-graph-build-v1", false));
        assert!(!graph_schema_matches(1_000_000,
            "borsuk-v255-cohere-diverse-graph-build-1m-v1", true));
        assert!(graph_schema_matches(100_000,
            "borsuk-v250-cohere-diverse-graph-build-v1", true));
    }
}
