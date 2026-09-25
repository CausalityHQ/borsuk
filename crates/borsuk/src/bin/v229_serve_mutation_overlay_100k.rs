//! Frozen same-vector 1% mutation overlay gate on selected V218 graph.

use std::{
    env,
    error::Error,
    fs::{self, File},
    io::{BufRead, BufReader, BufWriter, Read, Write},
    path::Path,
    sync::Arc,
    time::Instant,
};

use borsuk::{
    resident_graph_generation::ResidentGraphGeneration,
    resident_graph_overlay::{ResidentGraphOverlay, ResidentMutation},
    resident_vector_graph::GraphSearchWorkspace,
};
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

const ROWS: usize = 100_000;
const DIMS: usize = 768;
const QUERIES: usize = 1_000;
const WORKERS: usize = 8;
const SOURCE: &str = "a199e151b89a496ed20e39fdd951591bbfb4817d682e9111ebe2e1cab7ae550d";
const GRAPH_SHA: &str = "d8b70919243a7cd6ecb9448ce23f776374738476c1882cbc6a651fb34753af2f";

#[derive(Deserialize)]
struct Request {
    query_ordinal: usize,
    query: Vec<f32>,
}

fn digest(path: &Path) -> Result<String, Box<dyn Error>> {
    let mut file = BufReader::new(File::open(path)?);
    let mut hash = Sha256::new();
    let mut buffer = [0_u8; 1024 * 1024];
    loop {
        let n = file.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        hash.update(&buffer[..n]);
    }
    Ok(format!("{:x}", hash.finalize()))
}

fn percentile(values: &mut [u64], pct: usize) -> u64 {
    values.sort_unstable();
    values[(values.len() * pct).div_ceil(100) - 1]
}

fn peak_rss() -> Result<u64, Box<dyn Error>> {
    Ok(fs::read_to_string("/proc/self/status")?
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
    if args.len() != 6 && args.len() != 8 {
        return Err(
            "usage: v229_serve_mutation_overlay_100k ARTIFACT_DIR REQUESTS BASE_RAW RAW SUMMARY [linear-10k|decoded-10k|blocked-10k LOADED_RAW]"
                .into(),
        );
    }
    let mode = args.get(6).map(String::as_str).unwrap_or("linear-1k");
    if !matches!(
        mode,
        "linear-1k" | "linear-10k" | "decoded-10k" | "blocked-10k"
    ) {
        return Err("unknown mutation gate mode".into());
    }
    let stride = if mode == "linear-1k" { 100 } else { 10 };
    let dir = Path::new(&args[1]);
    let artifact = |name: &str| -> Result<Value, Box<dyn Error>> {
        let path = dir.join(name);
        Ok(json!({"bytes":fs::metadata(&path)?.len(),"sha256":digest(&path)?}))
    };
    if digest(&dir.join("graph.bin"))? != GRAPH_SHA {
        return Err("selected V218 graph differs".into());
    }
    let root = serde_json::to_vec(&json!({
        "schema":"borsuk-resident-graph-generation-v1", "generation":212,
        "source_sha256":SOURCE,"rows":ROWS,"dimensions":DIMS,
        "plane":artifact("plane.bin")?,"graph":artifact("graph.bin")?,
        "map":artifact("map.u32")?,"books":artifact("books.bin")?,
        "codes":artifact("codes.bin")?,
    }))?;
    let root_sha = format!("{:x}", Sha256::digest(&root));
    let generation = Arc::new(ResidentGraphGeneration::open_local_authenticated(
        &root,
        &root_sha,
        dir,
        300_000_000,
        WORKERS,
    )?);
    let requests = BufReader::new(File::open(&args[2])?)
        .lines()
        .map(|line| -> Result<Request, Box<dyn Error>> { Ok(serde_json::from_str(&line?)?) })
        .collect::<Result<Vec<_>, _>>()?;
    let baseline = BufReader::new(File::open(&args[3])?)
        .lines()
        .map(|line| -> Result<Value, Box<dyn Error>> { Ok(serde_json::from_str(&line?)?) })
        .collect::<Result<Vec<_>, _>>()?;
    if requests.len() != QUERIES
        || baseline.len() != QUERIES
        || requests.iter().enumerate().any(|(ordinal, row)| {
            row.query_ordinal != ordinal
                || row.query.len() != DIMS
                || row.query.iter().any(|value| !value.is_finite())
        })
    {
        return Err("query panel differs".into());
    }
    let view = generation.cosine_view()?;
    let base = generation.bind(&view)?;
    let mut workspace = GraphSearchWorkspace::new(ROWS)?;
    for (ordinal, request) in requests.iter().enumerate() {
        let (ids, _) = base.search(&request.query, 100, 2048, 2048, &mut workspace)?;
        if baseline[ordinal]["ordinal"].as_u64() != Some(ordinal as u64)
            || baseline[ordinal]["arms"]["2048-2048"]["returned_ids"] != json!(ids)
        {
            return Err(format!("selected V218 ID replay differs at {ordinal}").into());
        }
    }
    drop(base);
    drop(view);
    let mutation_start = Instant::now();
    let mutations = (0..ROWS)
        .step_by(stride)
        .map(|ordinal| {
            Ok(ResidentMutation {
                id: generation.source_id(ordinal)?,
                vector: Some(generation.vector_f32(ordinal)?),
            })
        })
        .collect::<Result<Vec<_>, borsuk::resident_graph_generation::ResidentGraphGenerationError>>(
        )?;
    let cap = if mode == "linear-1k" {
        2_000_000
    } else {
        64 * 1024 * 1024
    };
    let mut overlay = ResidentGraphOverlay::new(generation, mutations, cap)?;
    let mutation_prepare_ns = mutation_start.elapsed().as_nanos() as u64;
    let decode_start = Instant::now();
    if mode == "decoded-10k" {
        overlay = overlay.with_decoded_delta(cap)?;
    } else if mode == "blocked-10k" {
        overlay = overlay.with_blocked_delta(cap)?;
    }
    let decode_ns = if matches!(mode, "decoded-10k" | "blocked-10k") {
        decode_start.elapsed().as_nanos() as u64
    } else {
        0
    };
    let overlay = Arc::new(overlay);
    let view = overlay.base().cosine_view()?;
    let bound = overlay.bind(&view)?;
    let mut raw = BufWriter::new(File::create(&args[4])?);
    let mut expected = Vec::with_capacity(QUERIES);
    let mut sequential_times = Vec::with_capacity(QUERIES);
    let mut visits = Vec::with_capacity(QUERIES);
    let mut masked_total = 0_u64;
    let mut delta_total = 0_u64;
    for (ordinal, request) in requests.iter().enumerate() {
        let start = Instant::now();
        let (ids, stats) = bound.search(&request.query, 100, 2048, 2048, &mut workspace)?;
        let elapsed = start.elapsed().as_nanos() as u64;
        sequential_times.push(elapsed);
        visits.push(stats.base_visits as u64);
        masked_total += stats.masked_shortlist_rows as u64;
        delta_total += stats.delta_rows_scanned as u64;
        serde_json::to_writer(
            &mut raw,
            &json!({
                "ordinal":ordinal,"returned_ids":ids,"whole_ns":elapsed,
                "base_visits":stats.base_visits,"masked_shortlist_rows":stats.masked_shortlist_rows,
                "delta_rows_scanned":stats.delta_rows_scanned,"vector_body_gets":0,
            }),
        )?;
        raw.write_all(b"\n")?;
        expected.push(ids);
    }
    raw.flush()?;
    let loaded_wall = Instant::now();
    let loaded_times = std::thread::scope(|scope| -> Result<Vec<(usize, u64)>, Box<dyn Error>> {
        let mut handles = Vec::with_capacity(WORKERS);
        for worker in 0..WORKERS {
            let overlay = &overlay;
            let requests = &requests;
            let expected = &expected;
            handles.push(scope.spawn(move || -> Result<Vec<(usize, u64)>, String> {
                let view = overlay
                    .base()
                    .cosine_view()
                    .map_err(|error| error.to_string())?;
                let bound = overlay.bind(&view).map_err(|error| error.to_string())?;
                let mut workspace =
                    GraphSearchWorkspace::new(ROWS).map_err(|error| error.to_string())?;
                let mut times = Vec::new();
                for ordinal in (worker..QUERIES).step_by(WORKERS) {
                    let start = Instant::now();
                    let (ids, _) = bound
                        .search(&requests[ordinal].query, 100, 2048, 2048, &mut workspace)
                        .map_err(|error| error.to_string())?;
                    times.push((ordinal, start.elapsed().as_nanos() as u64));
                    if ids != expected[ordinal] {
                        return Err(format!("loaded ID mismatch at {ordinal}"));
                    }
                }
                Ok(times)
            }));
        }
        let mut times = Vec::with_capacity(QUERIES);
        for handle in handles {
            times.extend(
                handle
                    .join()
                    .map_err(|_| "loaded worker panic")?
                    .map_err(|error| format!("loaded worker: {error}"))?,
            );
        }
        Ok(times)
    })?;
    let loaded_wall_ns = loaded_wall.elapsed().as_nanos() as u64;
    if loaded_times.len() != QUERIES {
        return Err("loaded count differs".into());
    }
    let mut loaded_times = loaded_times;
    loaded_times.sort_unstable_by_key(|row| row.0);
    if let Some(path) = args.get(7) {
        let mut out = BufWriter::new(File::create(path)?);
        for (ordinal, elapsed) in &loaded_times {
            serde_json::to_writer(&mut out, &json!({"ordinal":ordinal,"whole_ns":elapsed}))?;
            out.write_all(b"\n")?;
        }
        out.flush()?;
    }
    let mut loaded_values = loaded_times
        .into_iter()
        .map(|row| row.1)
        .collect::<Vec<_>>();
    fs::write(
        &args[5],
        format!(
            "{}\n",
            json!({
                "schema":if mode == "linear-1k" {
                    "borsuk-v229-mutation-overlay-100k-serving-v1"
                } else if mode == "blocked-10k" {
                    "borsuk-v234-blocked-delta-100k-serving-v1"
                } else {
                    "borsuk-v232-decoded-delta-100k-serving-v1"
                },
                "dataset":"ReLAION-100k D768","split":"development-256-plus-method-heldout-744-prior-used",
                "queries":QUERIES,"workers":WORKERS,"k":100,"ef":2048,"shortlist":2048,
                "upsert_rows":ROWS/stride,"upsert_stride":stride,"unchanged_vectors":true,
                "mode":mode,"mutation_prepare_ns":mutation_prepare_ns,"decode_ns":decode_ns,
                "root_sha256":root_sha,"graph_sha256":GRAPH_SHA,
                "overlay_resident_bytes":overlay.resident_bytes(),
                "loaded_raw_sha256":args.get(7).map(|path| digest(Path::new(path))).transpose()?,
                "loaded_wall_ns":loaded_wall_ns,
                "loaded":{"p50_ns":percentile(&mut loaded_values,50),
                    "p90_ns":percentile(&mut loaded_values,90),
                    "p95_ns":percentile(&mut loaded_values,95),
                    "p99_ns":percentile(&mut loaded_values,99),
                    "qps":QUERIES as f64/(loaded_wall_ns as f64/1e9)},
                "sequential":{"p50_ns":percentile(&mut sequential_times,50),
                    "p90_ns":percentile(&mut sequential_times,90),
                    "p95_ns":percentile(&mut sequential_times,95),
                    "p99_ns":percentile(&mut sequential_times,99)},
                "base_visits_p95":percentile(&mut visits,95),
                "masked_shortlist_rows_total":masked_total,
                "delta_rows_scanned_total":delta_total,
                "process_peak_rss_bytes":peak_rss()?,"vector_body_gets":0,
                "raw_sha256":digest(Path::new(&args[4]))?,
            })
        ),
    )?;
    Ok(())
}
