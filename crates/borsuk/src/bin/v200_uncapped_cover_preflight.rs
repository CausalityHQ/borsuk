//! GT-blind exact-witness and latency screen for the linear priced-cover path.

use std::{collections::BTreeMap, env, error::Error, fs, path::Path, time::Instant};

use borsuk::unconstrained_priced_interval::unconstrained_priced_cover;
use serde::Deserialize;
use serde_json::json;
use sha2::{Digest, Sha256};

const PLAN_SHA: &str = "0a61974457030d2e2ce828e7bbbbaf3d8acd9849e70a7aaafd0cb44b0c5dda00";

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

fn main() -> Result<(), Box<dyn Error>> {
    let args = env::args().collect::<Vec<_>>();
    if args.len() != 4 {
        return Err(
            "usage: v200_uncapped_cover_preflight WEIGHTS_JSONL WEIGHTS_SHA256 PLANS_JSONL".into(),
        );
    }
    let weights = rows::<Weights>(&checked(Path::new(&args[1]), &args[2])?)?;
    let plans = rows::<Plan>(&checked(Path::new(&args[3]), PLAN_SHA)?)?;
    if weights.len() != 1000 || plans.len() != 1000 {
        return Err("V200 cohort length differs".into());
    }
    let mut elapsed = Vec::with_capacity(1000);
    let mut admitted_elapsed = Vec::new();
    let mut admitted = 0_usize;
    let mut over_units = 0_usize;
    let mut over_gets = 0_usize;
    let mut site_counts = Vec::new();
    for (index, (source, plan)) in weights.iter().zip(&plans).enumerate() {
        if source.ordinal != index || plan.ordinal != index || !plan.optional_risk.feasible {
            return Err(format!("V200 source/plan ordinal or feasibility at {index}").into());
        }
        let mut weight_map = BTreeMap::new();
        for &[unit, weight] in &source.weights {
            let unit = usize::try_from(unit)?;
            if weight_map.insert(unit, weight).is_some() {
                return Err(format!("V200 duplicate weight at {index}").into());
            }
        }
        site_counts.push(source.weights.len() + source.mandatory.len());
        let started = Instant::now();
        let actual =
            unconstrained_priced_cover(&weight_map, &source.mandatory, 31_250, 1000, 50000)
                .map_err(|error| format!("V200 cover at {index}: {error:?}"))?;
        let duration = started.elapsed().as_nanos() as u64;
        elapsed.push(duration);
        if actual.units <= plan.unit_cap && actual.gets <= 32 {
            admitted += 1;
            admitted_elapsed.push(duration);
            let reference = &plan.optional_risk;
            if actual.intervals
                != reference
                    .intervals
                    .iter()
                    .map(|pair| (pair[0], pair[1]))
                    .collect::<Vec<_>>()
                || actual.mass != reference.predicted_mass
                || actual.units != reference.units
                || actual.gets != reference.gets
            {
                return Err(format!("V200 exact feasible witness differs at {index}").into());
            }
        } else {
            over_units += (actual.units > plan.unit_cap) as usize;
            over_gets += (actual.gets > 32) as usize;
        }
    }
    println!(
        "{}",
        json!({
            "schema":"borsuk-v200-uncapped-cover-preflight-v1",
            "queries":1000,"fast_path_admitted":admitted,
            "exact_interval_witnesses":admitted,
            "fallback_required":1000-admitted,
            "over_unit_cap":over_units,"over_get_cap":over_gets,
            "ranked_plus_mandatory_sites_p50":percentile(&site_counts.iter().map(|&n|n as u64).collect::<Vec<_>>(),50),
            "all_uncapped_ns":{"p50":percentile(&elapsed,50),"p95":percentile(&elapsed,95),"p99":percentile(&elapsed,99)},
            "admitted_ns":{"p50":percentile(&admitted_elapsed,50),"p95":percentile(&admitted_elapsed,95),"p99":percentile(&admitted_elapsed,99)},
            "peak_process_rss_bytes":peak_rss_bytes()?,
        })
    );
    Ok(())
}
