//! Bounded resident centroid-graph precursor coverage on sealed development queries.

use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fs;
use std::io::{self, BufRead};
use std::path::Path;
use std::time::Instant;

use borsuk::object_native_generation::{ObjectNativeGeneration, ObjectNativeLimits};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

fn checked(path: &str, hash: &str) -> Result<Vec<u8>, Box<dyn Error>> {
    let bytes = fs::read(path)?;
    if format!("{:x}", Sha256::digest(&bytes)) != hash {
        return Err(invalid("input identity").into());
    }
    Ok(bytes)
}

fn percentile(values: &[u64], percent: usize) -> u64 {
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    sorted[(sorted.len() * percent).div_ceil(100) - 1]
}

fn main() -> Result<(), Box<dyn Error>> {
    let args = std::env::args().collect::<Vec<_>>();
    if args.len() != 8 {
        return Err(
            "usage: v292_centroid_route ROOT ROOT_SHA SQ8 REQUESTS GT_U32 CANDIDATES_JSONL RESULT"
                .into(),
        );
    }
    let generation = ObjectNativeGeneration::open(
        Path::new(&args[1]),
        &args[2],
        ObjectNativeLimits {
            max_memory_bytes: 2 * 1024 * 1024 * 1024,
            max_active_queries: 1,
            max_query_bytes: 16_777_216,
            max_query_gets: 32,
            max_parallel_gets: 32,
            max_router_regions: 391,
            max_router_shortlist: 512,
            already_pinned_bytes: 0,
        },
    )
    .map_err(|_| invalid("generation authentication"))?;
    if generation.pages().rows() != 100_000 || generation.pages().dimensions() != 768 {
        return Err(invalid("generation geometry").into());
    }
    let sq8 = checked(&args[3], generation.pages().object_sha256())?;
    if sq8.len() != 78_000_000 {
        return Err(invalid("SQ8 geometry").into());
    }
    let id_to_page = sq8
        .chunks_exact(780)
        .enumerate()
        .map(|(row, bytes)| {
            (
                i64::from_le_bytes(bytes[..8].try_into().unwrap()),
                row / 256,
            )
        })
        .collect::<HashMap<_, _>>();
    if id_to_page.len() != 100_000 {
        return Err(invalid("duplicate source ID").into());
    }
    let requests = checked(
        &args[4],
        "1de0122f73d1b72e54498640b9701ce6d156b513629596447580c85fac302ba4",
    )?;
    let truth = checked(
        &args[5],
        "f6630d0edf06539752c3fbf129ae01e58d3a3cf7b6aefa4decaa9c979e8ba355",
    )?;
    let lines = requests.lines().collect::<Result<Vec<_>, _>>()?;
    if lines.len() != 64 || truth.len() != 25_600 {
        return Err(invalid("query roster").into());
    }
    let mut samples = Vec::with_capacity(64);
    let mut hits = Vec::with_capacity(64);
    let mut cpu_us = Vec::with_capacity(64);
    let mut max_visits = 0;
    let mut exact_count = true;
    for (ordinal, line) in lines.iter().enumerate() {
        let request: Value = serde_json::from_str(line)?;
        if request["query_ordinal"].as_u64() != Some(ordinal as u64) {
            return Err(invalid("query ordinal").into());
        }
        let query = serde_json::from_value::<Vec<f32>>(request["query"].clone())?;
        let started = Instant::now();
        let seed = generation
            .graph()
            .search(generation.centroids(), &query, 1, 128)?;
        let seed_page = seed.units.first().ok_or_else(|| invalid("no seed"))?.0 / 8;
        let found = generation.graph().search_pages_seeded(
            generation.centroids(),
            &query,
            &[seed_page],
            158,
            1272,
        )?;
        let elapsed = started.elapsed().as_micros() as u64;
        let visits = seed.unit_evaluations + found.unit_evaluations;
        if seed.unit_evaluations > 128 || found.unit_evaluations > 1272 || visits > 1400 {
            return Err(invalid("visit cap").into());
        }
        let mut pages = vec![seed_page];
        pages.extend(found.pages.iter().map(|&(page, _)| page));
        pages.sort_unstable();
        if pages.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(invalid("duplicate candidate page").into());
        }
        exact_count &= pages.len() == 159;
        let page_set = pages.iter().copied().collect::<HashSet<_>>();
        let hit_count = truth[ordinal * 400..(ordinal + 1) * 400]
            .chunks_exact(4)
            .map(|word| {
                let id = u32::from_le_bytes(word.try_into().unwrap()) as i64;
                id_to_page
                    .get(&id)
                    .is_some_and(|page| page_set.contains(page))
            })
            .filter(|hit| *hit)
            .count();
        max_visits = max_visits.max(visits);
        hits.push(hit_count as u64);
        cpu_us.push(elapsed);
        samples.push(json!({"query_ordinal":ordinal,"pages":pages,
            "candidate_gt_hits":hit_count,"unit_evaluations":visits,
            "seed_evaluations":seed.unit_evaluations,"page_evaluations":found.unit_evaluations,
            "route_us":elapsed,"work_exhausted":found.work_exhausted}));
    }
    let mean = hits.iter().sum::<u64>() as f64 / 64.0;
    let result = json!({"schema":"borsuk-v292-centroid-route-v1",
        "dataset":"CoHere first100k D768 cosine k100","split":"development0-63",
        "queries":64,"mean_candidate_gt_hits":mean,"p05_candidate_gt_hits":percentile(&hits,5),
        "all_candidate_counts_159":exact_count,"max_unit_evaluations":max_visits,
        "offline_route_wall_us":{"p50":percentile(&cpu_us,50),"p90":percentile(&cpu_us,90),
            "p95":percentile(&cpu_us,95),"p99":percentile(&cpu_us,99)},
        "advance":exact_count && mean >= 98.9 && percentile(&hits,5) >= 96});
    fs::write(
        &args[6],
        samples
            .iter()
            .map(|sample| format!("{sample}\n"))
            .collect::<String>(),
    )?;
    fs::write(&args[7], format!("{result}\n"))?;
    println!("{result}");
    Ok(())
}
