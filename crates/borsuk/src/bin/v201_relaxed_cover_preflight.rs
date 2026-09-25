//! GT-blind exact-witness and latency screen for one-cap relaxations.

use std::{collections::BTreeMap, env, error::Error, fs, io::Write, path::Path, time::Instant};

use borsuk::{
    relaxed_priced_interval::{FixedCap, relaxed_priced_cover},
    unconstrained_priced_interval::{UnconstrainedCover, unconstrained_priced_cover},
};
use serde::Deserialize;
use serde_json::json;
use sha2::{Digest, Sha256};

const PLAN_SHA: &str = "0a61974457030d2e2ce828e7bbbbaf3d8acd9849e70a7aaafd0cb44b0c5dda00";
const WEIGHTS_SHA: &str = "797a83a7d830afd5cec491e70de3f49023a52c2f972af2045704b68df6f76313";
const TRACE_BUDGET: usize = 512 * 1024 * 1024;

#[derive(Deserialize)]
struct Weights {
    ordinal: usize,
    mandatory: Vec<usize>,
    weights: Vec<[i64; 2]>,
}

#[derive(Deserialize)]
struct Arm {
    feasible: bool,
    intervals: Vec<[usize; 2]>,
    units: usize,
    gets: usize,
    predicted_mass: i64,
}

#[derive(Deserialize)]
struct Plan {
    ordinal: usize,
    unit_cap: usize,
    optional_risk: Arm,
}

fn checked(path: &Path, expected: &str) -> Result<Vec<u8>, Box<dyn Error>> {
    let bytes = fs::read(path)?;
    if format!("{:x}", Sha256::digest(&bytes)) != expected {
        return Err(format!("SHA-256 differs: {}", path.display()).into());
    }
    Ok(bytes)
}

fn rows<T: for<'de> Deserialize<'de>>(bytes: &[u8]) -> Result<Vec<T>, Box<dyn Error>> {
    bytes
        .split(|&part| part == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| Ok(serde_json::from_slice::<T>(line)?))
        .collect()
}

fn percentile(values: &[u64], p: usize) -> u64 {
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    sorted[(sorted.len() * p).div_ceil(100) - 1]
}

fn peak_rss_bytes() -> Result<u64, Box<dyn Error>> {
    let status = fs::read_to_string("/proc/self/status")?;
    let value = status
        .lines()
        .find(|line| line.starts_with("VmHWM:"))
        .and_then(|line| line.split_whitespace().nth(1))
        .ok_or("VmHWM missing")?;
    Ok(value.parse::<u64>()? * 1024)
}

fn matches(reference: &Arm, actual: &UnconstrainedCover) -> bool {
    actual.intervals
        == reference
            .intervals
            .iter()
            .map(|pair| (pair[0], pair[1]))
            .collect::<Vec<_>>()
        && actual.mass == reference.predicted_mass
        && actual.units == reference.units
        && actual.gets == reference.gets
}

fn ptiles(values: &[u64]) -> serde_json::Value {
    if values.is_empty() {
        return serde_json::Value::Null;
    }
    json!({"p50":percentile(values,50),"p95":percentile(values,95),
        "p99":percentile(values,99)})
}

fn main() -> Result<(), Box<dyn Error>> {
    let args = env::args().collect::<Vec<_>>();
    if args.len() != 4 {
        return Err(
            "usage: v201_relaxed_cover_preflight WEIGHTS_JSONL PLANS_JSONL RAW_JSONL".into(),
        );
    }
    let weights = rows::<Weights>(&checked(Path::new(&args[1]), WEIGHTS_SHA)?)?;
    let plans = rows::<Plan>(&checked(Path::new(&args[2]), PLAN_SHA)?)?;
    if weights.len() != 1000 || plans.len() != 1000 {
        return Err("V201 cohort length differs".into());
    }
    let mut raw = std::io::BufWriter::new(fs::File::create(&args[3])?);
    let mut all_elapsed = Vec::with_capacity(1000);
    let mut linear_elapsed = Vec::new();
    let mut unit_elapsed = Vec::new();
    let mut get_elapsed = Vec::new();
    let mut tier_counts = [0_usize; 4];
    for (index, (source, plan)) in weights.iter().zip(&plans).enumerate() {
        if source.ordinal != index || plan.ordinal != index || !plan.optional_risk.feasible {
            return Err(format!("V201 source/plan ordinal or feasibility at {index}").into());
        }
        let mut weight_map = BTreeMap::new();
        for &[unit, weight] in &source.weights {
            let unit = usize::try_from(unit)?;
            if weight_map.insert(unit, weight).is_some() {
                return Err(format!("V201 duplicate weight at {index}").into());
            }
        }
        let query_started = Instant::now();
        let started = Instant::now();
        let linear =
            unconstrained_priced_cover(&weight_map, &source.mandatory, 31_250, 1000, 50000)
                .map_err(|error| format!("V201 linear cover at {index}: {error:?}"))?;
        let linear_ns = started.elapsed().as_nanos() as u64;
        linear_elapsed.push(linear_ns);
        let mut unit_ns = 0_u64;
        let mut get_ns = 0_u64;
        let (tier, chosen) = if linear.units <= plan.unit_cap && linear.gets <= 32 {
            tier_counts[0] += 1;
            ("linear", Some(linear))
        } else {
            let started = Instant::now();
            let unit = relaxed_priced_cover(
                &weight_map,
                &source.mandatory,
                31_250,
                FixedCap::Units,
                plan.unit_cap,
                1000,
                50000,
                TRACE_BUDGET,
            )
            .map_err(|error| format!("V201 unit relaxation at {index}: {error:?}"))?;
            unit_ns = started.elapsed().as_nanos() as u64;
            unit_elapsed.push(unit_ns);
            if unit.gets <= 32 {
                tier_counts[1] += 1;
                ("unit", Some(unit))
            } else {
                let started = Instant::now();
                let get = relaxed_priced_cover(
                    &weight_map,
                    &source.mandatory,
                    31_250,
                    FixedCap::Gets,
                    32,
                    1000,
                    50000,
                    TRACE_BUDGET,
                )
                .map_err(|error| format!("V201 GET relaxation at {index}: {error:?}"))?;
                get_ns = started.elapsed().as_nanos() as u64;
                get_elapsed.push(get_ns);
                if get.units <= plan.unit_cap {
                    tier_counts[2] += 1;
                    ("get", Some(get))
                } else {
                    tier_counts[3] += 1;
                    ("needs_2d", None)
                }
            }
        };
        if let Some(ref candidate) = chosen {
            if !matches(&plan.optional_risk, candidate) {
                return Err(format!("V201 exact feasible witness differs at {index} tier {tier}: actual {:?} vs reference {:?}",
                    candidate.intervals, plan.optional_risk.intervals).into());
            }
        }
        let total_ns = query_started.elapsed().as_nanos() as u64;
        all_elapsed.push(total_ns);
        writeln!(
            raw,
            "{}",
            json!({"ordinal":index,"tier":tier,
            "linear_ns":linear_ns,"unit_ns":unit_ns,"get_ns":get_ns,
            "total_ns":total_ns,"intervals":chosen.as_ref().map(|cover|&cover.intervals),
            "mass":chosen.as_ref().map(|cover|cover.mass),
            "units":chosen.as_ref().map(|cover|cover.units),
            "gets":chosen.as_ref().map(|cover|cover.gets)})
        )?;
    }
    raw.flush()?;
    println!(
        "{}",
        json!({"schema":"borsuk-v201-relaxed-cover-preflight-v1",
            "queries":1000,"linear_exact":tier_counts[0],
            "unit_exact":tier_counts[1],"get_exact":tier_counts[2],
            "needs_2d":tier_counts[3],
            "all_total_ns":ptiles(&all_elapsed),
            "linear_ns":ptiles(&linear_elapsed),
            "unit_ns":ptiles(&unit_elapsed),"get_ns":ptiles(&get_elapsed),
            "peak_process_rss_bytes":peak_rss_bytes()?
        })
    );
    Ok(())
}
