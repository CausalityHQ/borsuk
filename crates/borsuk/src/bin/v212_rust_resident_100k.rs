//! Bounded source-PQ plus resident-FP16 kernel falsifier on frozen 100k inputs.

use std::{
    cmp::Reverse,
    collections::{BinaryHeap, HashSet},
    env,
    error::Error,
    fs::{self, File},
    io::{BufRead, BufReader, BufWriter, Read, Write},
    path::Path,
    time::Instant,
};

use borsuk::{pq64_nominee::Pq64Router, resident_fp16_tier::ResidentFp16Tier};
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

const ROWS: usize = 100_000;
const DIMS: usize = 768;
const GENERATION: u64 = 212;
const CANDIDATES: usize = 32_768;
const SHORTLIST: usize = 4_096;
const SOURCE_SHA: &str = "a199e151b89a496ed20e39fdd951591bbfb4817d682e9111ebe2e1cab7ae550d";

#[derive(Deserialize)]
struct Request {
    query_ordinal: usize,
    query: Vec<f32>,
    nominees: Vec<usize>,
}

fn digest(path: &Path) -> Result<String, Box<dyn Error>> {
    let mut file = BufReader::new(File::open(path)?);
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 1024 * 1024];
    loop {
        let count = file.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        hasher.update(&buffer[..count]);
    }
    Ok(format!("{:x}", hasher.finalize()))
}

fn verified(path: &Path, expected: &str) -> Result<(), Box<dyn Error>> {
    if digest(path)? != expected {
        return Err("frozen input digest differs".into());
    }
    Ok(())
}

fn u32_array(path: &Path, expected: &str) -> Result<Vec<usize>, Box<dyn Error>> {
    verified(path, expected)?;
    let bytes = fs::read(path)?;
    if bytes.len() != ROWS * 4 {
        return Err("physical map length differs".into());
    }
    Ok(bytes
        .chunks_exact(4)
        .map(|part| u32::from_le_bytes(part.try_into().unwrap()) as usize)
        .collect())
}

fn books(path: &Path, expected: &str) -> Result<Vec<f32>, Box<dyn Error>> {
    verified(path, expected)?;
    let bytes = fs::read(path)?;
    if bytes.len() != 64 * 256 * 12 * 4 {
        return Err("PQ book length differs".into());
    }
    Ok(bytes
        .chunks_exact(4)
        .map(|part| f32::from_le_bytes(part.try_into().unwrap()))
        .collect())
}

fn nearest(seeds: &[usize], old_to_new: &[usize]) -> Result<Vec<usize>, Box<dyn Error>> {
    if seeds.len() != 512 {
        return Err("nominee count differs".into());
    }
    let mut queue = BinaryHeap::new();
    let mut seen = HashSet::with_capacity(CANDIDATES * 2);
    for &old in seeds {
        let &physical = old_to_new.get(old).ok_or("old nominee row differs")?;
        if physical >= ROWS || !seen.insert(physical) {
            return Err("nominee identity differs".into());
        }
        queue.push(Reverse((0usize, physical)));
    }
    let mut result = Vec::with_capacity(CANDIDATES);
    while result.len() < CANDIDATES {
        let Reverse((distance, physical)) = queue.pop().ok_or("neighborhood exhausted")?;
        result.push(physical);
        for neighbor in [physical.checked_sub(1), physical.checked_add(1)] {
            if let Some(next) = neighbor.filter(|value| *value < ROWS && seen.insert(*value)) {
                queue.push(Reverse((distance + 1, next)));
            }
        }
    }
    Ok(result)
}

fn percentile(sorted: &[u64], value: usize) -> u64 {
    sorted[(sorted.len() * value).div_ceil(100) - 1]
}

fn peak_rss() -> Result<u64, Box<dyn Error>> {
    let status = fs::read_to_string("/proc/self/status")?;
    let line = status
        .lines()
        .find(|line| line.starts_with("VmHWM:"))
        .ok_or("VmHWM missing")?;
    Ok(line
        .split_whitespace()
        .nth(1)
        .ok_or("VmHWM value missing")?
        .parse::<u64>()?
        * 1024)
}

fn main() -> Result<(), Box<dyn Error>> {
    let args = env::args().collect::<Vec<_>>();
    if args.len() != 9 {
        return Err("usage: v212_rust_resident_100k PREP_JSON PLANE OLD_TO_NEW NEW_TO_OLD BOOKS CODES REQUESTS RAW".into());
    }
    let prep: Value = serde_json::from_slice(&fs::read(&args[1])?)?;
    if prep["schema"] != "borsuk-v212-rust-100k-preparation-v1"
        || prep["source_sha256"] != SOURCE_SHA
        || prep["rows"].as_u64() != Some(ROWS as u64)
        || prep["dimensions"].as_u64() != Some(DIMS as u64)
        || prep["generation"].as_u64() != Some(GENERATION)
    {
        return Err("preparation identity differs".into());
    }
    let field = |key: &str| -> Result<&str, Box<dyn Error>> {
        Ok(prep[key].as_str().ok_or("preparation digest missing")?)
    };
    let started = Instant::now();
    let old_to_new = u32_array(Path::new(&args[3]), field("old_to_new_sha256")?)?;
    let new_to_old = u32_array(Path::new(&args[4]), field("new_to_old_sha256")?)?;
    let mut seen = vec![false; ROWS];
    for (old, &new) in old_to_new.iter().enumerate() {
        if new >= ROWS || seen[new] || new_to_old[new] != old {
            return Err("physical map is not bijective".into());
        }
        seen[new] = true;
    }
    let pq_books = books(Path::new(&args[5]), field("books_raw_sha256")?)?;
    verified(Path::new(&args[6]), field("codes_raw_sha256")?)?;
    let pq_codes = fs::read(&args[6])?;
    let router = Pq64Router::new(
        ROWS,
        DIMS,
        256,
        1,
        vec![0.0; ROWS.div_ceil(256) * DIMS],
        pq_books,
        pq_codes,
    )
    .map_err(|error| format!("PQ construction: {error:?}"))?;
    let plane = ResidentFp16Tier::open_authenticated(
        Path::new(&args[2]),
        field("plane_sha256")?,
        SOURCE_SHA,
        ROWS as u64,
        DIMS,
        GENERATION,
        200_000_000,
    )?;
    let hydration_ns = started.elapsed().as_nanos() as u64;
    verified(Path::new(&args[7]), field("requests_sha256")?)?;
    let requests = BufReader::new(File::open(&args[7])?)
        .lines()
        .take(256)
        .map(|line| -> Result<Request, Box<dyn Error>> { Ok(serde_json::from_str(&line?)?) })
        .collect::<Result<Vec<_>, _>>()?;
    if requests.len() != 256 {
        return Err("request panel length differs".into());
    }
    let mut output = BufWriter::new(File::create(&args[8])?);
    let mut nearest_times = Vec::new();
    let mut pq_times = Vec::new();
    let mut fp16_times = Vec::new();
    let mut whole_times = Vec::new();
    let wall = Instant::now();
    for (index, request) in requests.iter().enumerate() {
        if request.query_ordinal != index || request.query.len() != DIMS {
            return Err("request geometry differs".into());
        }
        let started = Instant::now();
        let physical = nearest(&request.nominees, &old_to_new)?;
        let nearest_ns = started.elapsed().as_nanos() as u64;
        let old_rows = physical
            .iter()
            .map(|&row| new_to_old[row])
            .collect::<Vec<_>>();
        let pq_started = Instant::now();
        let scores = router
            .score_rows(&request.query, &old_rows)
            .map_err(|error| format!("PQ score: {error:?}"))?;
        let mut ranked = (0..CANDIDATES).collect::<Vec<_>>();
        ranked.select_nth_unstable_by(SHORTLIST - 1, |&a, &b| {
            scores[a]
                .total_cmp(&scores[b])
                .then_with(|| physical[a].cmp(&physical[b]))
        });
        ranked.truncate(SHORTLIST);
        ranked.sort_unstable_by(|&a, &b| {
            scores[a]
                .total_cmp(&scores[b])
                .then_with(|| physical[a].cmp(&physical[b]))
        });
        let shortlist = ranked.iter().map(|&row| physical[row]).collect::<Vec<_>>();
        let pq_ns = pq_started.elapsed().as_nanos() as u64;
        let fp16_started = Instant::now();
        let ids = plane.rank_ordinals_cosine(&request.query, &shortlist, 100)?;
        let fp16_ns = fp16_started.elapsed().as_nanos() as u64;
        let whole_ns = started.elapsed().as_nanos() as u64;
        nearest_times.push(nearest_ns);
        pq_times.push(pq_ns);
        fp16_times.push(fp16_ns);
        whole_times.push(whole_ns);
        serde_json::to_writer(
            &mut output,
            &json!({"ordinal":index,
            "returned_ids":ids,"nearest_ns":nearest_ns,"pq_ns":pq_ns,
            "fp16_ns":fp16_ns,"whole_ns":whole_ns,"vector_body_gets":0}),
        )?;
        output.write_all(b"\n")?;
    }
    output.flush()?;
    let wall_ns = wall.elapsed().as_nanos() as u64;
    let percentiles = |times: &mut Vec<u64>| {
        times.sort_unstable();
        json!({"p50_ns":percentile(times,50),"p95_ns":percentile(times,95),
            "p99_ns":percentile(times,99)})
    };
    println!(
        "{}",
        json!({"schema":"borsuk-v212-rust-resident-100k-v1",
        "dataset":"ReLAION-100k D768","split":"development-first-256-already-used",
        "queries":256,"nearest":percentiles(&mut nearest_times),
        "pq":percentiles(&mut pq_times),"fp16":percentiles(&mut fp16_times),
        "whole":percentiles(&mut whole_times),"wall_ns":wall_ns,
        "throughput_qps":256.0/(wall_ns as f64/1e9),
        "cold_hydration_ns":hydration_ns,"process_peak_rss_bytes":peak_rss()?,
        "resident_plane_bytes":plane.resident_bytes(),"vector_body_gets":0,
        "raw_sha256":digest(Path::new(&args[8]))?})
    );
    Ok(())
}
