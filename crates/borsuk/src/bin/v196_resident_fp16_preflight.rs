//! Isolated used-panel latency and exact-ID gate for the resident FP16 tier.

use std::{
    env,
    error::Error,
    fs::File,
    io::{BufRead, BufReader},
    path::Path,
    time::Instant,
};

use borsuk::{native_source_tier::SourceCandidate, resident_fp16_tier::ResidentFp16Tier};
use serde::Deserialize;
use serde_json::json;

#[derive(Deserialize)]
struct Candidate {
    ordinal: u64,
    source_id: u64,
}

#[derive(Deserialize)]
struct Case {
    ordinal: u64,
    query: Vec<f32>,
    candidates: Vec<Candidate>,
    expected: Vec<u64>,
}

fn percentile(sorted: &[u64], numerator: usize) -> u64 {
    sorted[(sorted.len() * numerator).div_ceil(100) - 1]
}

fn peak_rss_bytes() -> Result<u64, Box<dyn Error>> {
    let status = std::fs::read_to_string("/proc/self/status")?;
    let line = status
        .lines()
        .find(|line| line.starts_with("VmHWM:"))
        .ok_or("VmHWM missing")?;
    let kib = line
        .split_whitespace()
        .nth(1)
        .ok_or("VmHWM value missing")?;
    Ok(kib.parse::<u64>()? * 1024)
}

fn main() -> Result<(), Box<dyn Error>> {
    let args = env::args().collect::<Vec<_>>();
    if args.len() != 10 {
        return Err("usage: v196_resident_fp16_preflight PLANE ARTIFACT_SHA SOURCE_SHA ROWS DIMS GENERATION BUDGET_BYTES CASES_JSONL FIRST_ORDINAL".into());
    }
    let rows: u64 = args[4].parse()?;
    let dimensions: usize = args[5].parse()?;
    let generation: u64 = args[6].parse()?;
    let budget: usize = args[7].parse()?;
    let first_ordinal: u64 = args[9].parse()?;
    let started = Instant::now();
    let tier = ResidentFp16Tier::open_authenticated(
        Path::new(&args[1]),
        &args[2],
        &args[3],
        rows,
        dimensions,
        generation,
        budget,
    )?;
    let hydration_ns = started.elapsed().as_nanos();
    let mut cases = Vec::new();
    for line in BufReader::new(File::open(&args[8])?).lines() {
        cases.push(serde_json::from_str::<Case>(&line?)?);
    }
    if cases.len() != 512 {
        return Err("V196 case count differs".into());
    }
    for (index, case) in cases.iter().enumerate() {
        if case.ordinal != first_ordinal + index as u64
            || case.query.len() != dimensions
            || case.candidates.len() != 128
            || case.expected.len() != 100
        {
            return Err(format!("V196 case geometry differs at {}", case.ordinal).into());
        }
    }
    let mut repetitions = Vec::with_capacity(10);
    for repetition in 0..=10 {
        let mut times = Vec::with_capacity(cases.len());
        for case in &cases {
            let roster = case
                .candidates
                .iter()
                .map(|candidate| SourceCandidate {
                    ordinal: candidate.ordinal,
                    source_id: candidate.source_id,
                })
                .collect::<Vec<_>>();
            let started = Instant::now();
            let actual = tier.rank_cosine(&case.query, &roster, 100)?;
            let elapsed = started.elapsed().as_nanos() as u64;
            if actual != case.expected {
                return Err(format!("V196 exact-ID mismatch at {}", case.ordinal).into());
            }
            times.push(elapsed);
        }
        if repetition > 0 {
            times.sort_unstable();
            repetitions.push(json!({
                "repetition": repetition,
                "p50_ns": percentile(&times, 50),
                "p95_ns": percentile(&times, 95),
                "p99_ns": percentile(&times, 99),
                "max_ns": times[times.len() - 1],
            }));
        }
    }
    println!(
        "{}",
        json!({
            "schema": "borsuk-resident-fp16-preflight-v2",
            "first_ordinal": first_ordinal,
            "rows": rows,
            "dimensions": dimensions,
            "generation": generation,
            "cases": cases.len(),
            "repetitions": repetitions,
            "exact_returned_sets": 5120,
            "hydration_ns": hydration_ns,
            "charged_plane_bytes": tier.resident_bytes(),
            "process_peak_rss_bytes": peak_rss_bytes()?,
        })
    );
    Ok(())
}
