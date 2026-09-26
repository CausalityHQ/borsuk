//! Build the frozen CoHere-100k transfer graph from authenticated source rows.

use borsuk::{resident_fp16_tier::ResidentFp16Tier, resident_vector_graph::ResidentVectorGraph};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    env,
    error::Error,
    fs::{self, File},
    io::{BufReader, Read},
    path::Path,
    time::Instant,
};

const ROWS: usize = 100_000;
const DIMS: usize = 768;

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
    if args.len() != 6 {
        return Err("usage: v248_build_cohere_graph_100k PREP PLANE VECTORS GRAPH SUMMARY".into());
    }
    let prep: Value = serde_json::from_slice(&fs::read(&args[1])?)?;
    let source = prep["source_sha256"]
        .as_str()
        .ok_or("source hash missing")?;
    if prep["schema"] != "borsuk-v248-source-preparation-v1"
        || prep["dataset_id"] != "cohere-large-10m-768"
        || prep["staging_receipt_sha256"]
            != "0965aa0241199822dfac3410bba4edad5536ac0eb0aaa8ab83c216e8c5749a87"
        || prep["rows"].as_u64() != Some(ROWS as u64)
        || prep["dimensions"].as_u64() != Some(DIMS as u64)
        || prep["artifacts"]["plane.bin"]["sha256"].as_str()
            != Some(digest(Path::new(&args[2]))?.as_str())
        || prep["artifacts"]["vectors.raw"]["sha256"].as_str()
            != Some(digest(Path::new(&args[3]))?.as_str())
        || source
            != prep["artifacts"]["vectors.raw"]["sha256"]
                .as_str()
                .ok_or("raw hash missing")?
    {
        return Err("graph build input identity differs".into());
    }
    let started = Instant::now();
    let plane = ResidentFp16Tier::open_authenticated(
        Path::new(&args[2]),
        prep["artifacts"]["plane.bin"]["sha256"]
            .as_str()
            .ok_or("plane hash missing")?,
        source,
        ROWS as u64,
        DIMS,
        prep["generation"].as_u64().ok_or("generation missing")?,
        200_000_000,
    )?;
    let vectors = {
        let mut input = BufReader::new(File::open(&args[3])?);
        let mut row = vec![0u8; DIMS * 4];
        let mut vectors = Vec::with_capacity(ROWS);
        for _ in 0..ROWS {
            input.read_exact(&mut row)?;
            vectors.push(
                row.chunks_exact(4)
                    .map(|b| f32::from_le_bytes(b.try_into().unwrap()))
                    .collect(),
            );
        }
        if input.read(&mut [0u8; 1])? != 0 {
            return Err("graph source trailing bytes".into());
        }
        vectors
    };
    let graph = ResidentVectorGraph::build_batched(vectors, &plane, 32, 64, 128, 8)?;
    let structure = graph.structural_stats();
    if structure.reachable != ROWS
        || structure.below_four_indegree != 0
        || structure.max_degree > 256
    {
        return Err("reachable graph structural gate failed".into());
    }
    let heap_bytes = graph.heap_bytes();
    let graph_sha = graph.write_authenticated(Path::new(&args[4]))?;
    let elapsed = started.elapsed().as_nanos() as u64;
    fs::write(
        &args[5],
        format!(
            "{}\n",
            json!({"schema":"borsuk-v248-cohere-graph-build-v1",
        "construction_source":"authenticated-f32-source",
        "source_sha256":source,"plane_sha256":prep["artifacts"]["plane.bin"]["sha256"],
        "graph_sha256":graph_sha,"graph_bytes":fs::metadata(&args[4])?.len(),
        "graph_heap_bytes":heap_bytes,"structure":structure,"build_workers":8,
        "build_ns":elapsed,"build_peak_rss_bytes":peak_rss()?,
        "rows":ROWS,"dimensions":DIMS,"generation":prep["generation"]})
        ),
    )?;
    Ok(())
}
