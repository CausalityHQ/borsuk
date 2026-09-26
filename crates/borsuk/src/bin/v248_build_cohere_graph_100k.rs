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
    let pq_topology = args.len() == 9 && args[6] == "--pq-topology";
    let diverse = args.len() == 7 && args[6] == "--diverse";
    if args.len() != 6 && !pq_topology && !diverse {
        return Err("usage: v248_build_cohere_graph_100k PREP PLANE VECTORS GRAPH SUMMARY [--pq-topology BOOKS CODES | --diverse]".into());
    }
    let prep: Value = serde_json::from_slice(&fs::read(&args[1])?)?;
    let rows = prep["rows"].as_u64().ok_or("row count missing")? as usize;
    let source = prep["source_sha256"]
        .as_str()
        .ok_or("source hash missing")?;
    if prep["schema"] != "borsuk-v248-source-preparation-v1"
        || prep["dataset_id"] != "cohere-large-10m-768"
        || prep["staging_receipt_sha256"]
            != "0965aa0241199822dfac3410bba4edad5536ac0eb0aaa8ab83c216e8c5749a87"
        || !matches!(rows, 100_000 | 1_000_000)
        || (rows == 1_000_000 && !diverse)
        || prep["dimensions"].as_u64() != Some(DIMS as u64)
        || prep["artifacts"]["plane.bin"]["sha256"].as_str()
            != Some(digest(Path::new(&args[2]))?.as_str())
        || prep["artifacts"]["vectors.raw"]["sha256"].as_str()
            != Some(digest(Path::new(&args[3]))?.as_str())
        || source
            != prep["artifacts"]["vectors.raw"]["sha256"]
                .as_str()
                .ok_or("raw hash missing")?
        || (pq_topology
            && (prep["artifacts"]["books.bin"]["sha256"].as_str()
                != Some(digest(Path::new(&args[7]))?.as_str())
                || prep["artifacts"]["codes.bin"]["sha256"].as_str()
                    != Some(digest(Path::new(&args[8]))?.as_str())))
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
        rows as u64,
        DIMS,
        prep["generation"].as_u64().ok_or("generation missing")?,
        rows * 2_000,
    )?;
    let vectors = if pq_topology {
        let books = fs::read(&args[7])?;
        let codes = fs::read(&args[8])?;
        if books.len() != 64 * 256 * 12 * 4 || codes.len() != rows * 64 {
            return Err("PQ topology geometry differs".into());
        }
        let mut vectors = Vec::with_capacity(rows);
        for row in 0..rows {
            let mut vector = Vec::with_capacity(DIMS);
            for subspace in 0..64 {
                let word = codes[row * 64 + subspace] as usize;
                let first = (subspace * 256 + word) * 12 * 4;
                vector.extend(
                    books[first..first + 12 * 4]
                        .chunks_exact(4)
                        .map(|bytes| f32::from_le_bytes(bytes.try_into().unwrap())),
                );
            }
            vectors.push(vector);
        }
        vectors
    } else {
        let mut input = BufReader::new(File::open(&args[3])?);
        let mut row = vec![0u8; DIMS * 4];
        let mut vectors = Vec::with_capacity(rows);
        for _ in 0..rows {
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
    let graph = if diverse {
        ResidentVectorGraph::build_batched_diverse(vectors, &plane, 32, 64, 128, 8)?
    } else {
        ResidentVectorGraph::build_batched(vectors, &plane, 32, 64, 128, 8)?
    };
    let structure = graph.structural_stats();
    if structure.reachable != rows
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
            json!({"schema":if rows == 1_000_000 {"borsuk-v255-cohere-diverse-graph-build-1m-v1"} else if diverse {"borsuk-v250-cohere-diverse-graph-build-v1"} else if pq_topology {"borsuk-v249-cohere-pq-aligned-graph-build-v1"} else {"borsuk-v248-cohere-graph-build-v1"},
        "construction_source":if pq_topology {"authenticated-pq-reconstruction"} else {"authenticated-f32-source"},
        "source_sha256":source,"plane_sha256":prep["artifacts"]["plane.bin"]["sha256"],
        "graph_sha256":graph_sha,"graph_bytes":fs::metadata(&args[4])?.len(),
        "graph_heap_bytes":heap_bytes,"structure":structure,"build_workers":8,
        "build_ns":elapsed,"build_peak_rss_bytes":peak_rss()?,
        "rows":rows,"dimensions":DIMS,"generation":prep["generation"]})
        ),
    )?;
    Ok(())
}
