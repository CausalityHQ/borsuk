//! Exact Rust SQ8 replay of sealed V291 Python-selected physical pages.

use std::collections::HashSet;
use std::error::Error;
use std::fs;
use std::io::{self, BufRead};
use std::path::Path;

use borsuk::exact_sq8_nominee::Sq8Geometry;
use borsuk::object_native_generation::{ObjectNativeGeneration, ObjectNativeLimits};
use borsuk::returned_sq8::{ReturnedRange, rank_returned_ranges};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

const REQUESTS_SHA: &str = "1de0122f73d1b72e54498640b9701ce6d156b513629596447580c85fac302ba4";
const TRUTH_SHA: &str = "f6630d0edf06539752c3fbf129ae01e58d3a3cf7b6aefa4decaa9c979e8ba355";
const PLANS_SHA: &str = "82aa26a676f4b358e2de17ab48d7b5dba086cb5cb23963b8fa3fa437a240ea39";

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

fn checked(path: &str, digest: &str) -> Result<Vec<u8>, Box<dyn Error>> {
    let bytes = fs::read(path)?;
    if format!("{:x}", Sha256::digest(&bytes)) != digest {
        return Err(invalid("input digest differs").into());
    }
    Ok(bytes)
}

fn p05(hits: &[usize]) -> usize {
    let mut ordered = hits.to_vec();
    ordered.sort_unstable();
    ordered[3]
}

fn main() -> Result<(), Box<dyn Error>> {
    let args = std::env::args().collect::<Vec<_>>();
    if args.len() != 8 && args.len() != 9 {
        return Err(
            "usage: v291_rust_sq8_parity ROOT ROOT_SHA SQ8 REQUESTS GT_U32 PLANS_JSONL OUTPUT [PLANS_SHA]"
                .into(),
        );
    }
    let plans_hash = args.get(8).map_or(PLANS_SHA, String::as_str);
    let generation = ObjectNativeGeneration::open(
        Path::new(&args[1]),
        &args[2],
        ObjectNativeLimits {
            max_memory_bytes: 4 * 1024 * 1024 * 1024,
            max_active_queries: 1,
            max_query_bytes: 16 * 1024 * 1024,
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
    let requests = checked(&args[4], REQUESTS_SHA)?;
    let truth = checked(&args[5], TRUTH_SHA)?;
    let plans = checked(&args[6], plans_hash)?;
    if sq8.len() != 78_000_000 || truth.len() != 25_600 {
        return Err(invalid("SQ8 or truth geometry").into());
    }
    let request_lines = requests.lines().collect::<Result<Vec<_>, _>>()?;
    let plan_lines = plans.lines().collect::<Result<Vec<_>, _>>()?;
    if request_lines.len() != 64 || plan_lines.len() != 64 {
        return Err(invalid("query or plan count").into());
    }
    let mut fetched_hits = Vec::with_capacity(64);
    let mut returned_hits = Vec::with_capacity(64);
    let mut returned_differences = Vec::new();
    let mut max_gets = 0;
    let mut max_bytes = 0;
    let row_bytes = 780;
    for ordinal in 0..64 {
        let request: Value = serde_json::from_str(&request_lines[ordinal])?;
        let plan: Value = serde_json::from_str(&plan_lines[ordinal])?;
        if request["query_ordinal"].as_u64() != Some(ordinal as u64)
            || plan["query_ordinal"].as_u64() != Some(ordinal as u64)
        {
            return Err(invalid("query ordinal").into());
        }
        let query = serde_json::from_value::<Vec<f32>>(request["query"].clone())?;
        let pages = serde_json::from_value::<Vec<usize>>(plan["pages"].clone())?;
        if query.len() != 768
            || query.iter().any(|x| !x.is_finite())
            || pages.is_empty()
            || pages.len() > 84
            || pages.iter().any(|&p| p >= 391)
            || pages.windows(2).any(|pair| pair[0] >= pair[1])
        {
            return Err(invalid("query or page geometry").into());
        }
        let expected = truth[ordinal * 400..(ordinal + 1) * 400]
            .chunks_exact(4)
            .map(|word| u32::from_le_bytes(word.try_into().unwrap()) as i64)
            .collect::<HashSet<_>>();
        if expected.len() != 100 {
            return Err(invalid("truth roster").into());
        }
        let mut pairs = Vec::new();
        let mut first = pages[0];
        let mut last = first;
        for &page in &pages[1..] {
            if page == last + 1 {
                last = page;
            } else {
                pairs.push((first, last));
                first = page;
                last = page;
            }
        }
        pairs.push((first, last));
        let bytes = pages
            .iter()
            .map(|&p| (100_000 - p * 256).min(256) * row_bytes)
            .sum::<usize>();
        if pairs.len() > 32 || bytes > 16_777_216 {
            return Err(invalid("physical budget").into());
        }
        max_gets = max_gets.max(pairs.len());
        max_bytes = max_bytes.max(bytes);
        let mut ranges = Vec::with_capacity(pairs.len());
        for (first, last) in pairs {
            let start = first * 256 * row_bytes;
            let end = ((last + 1) * 256 * row_bytes).min(sq8.len());
            let payload = &sq8[start..end];
            generation
                .pages()
                .verify_payload(first, last, payload)
                .map_err(|_| invalid("page digest"))?;
            ranges.push(ReturnedRange {
                start,
                bytes: payload,
            });
        }
        let fetched = ranges
            .iter()
            .flat_map(|range| range.bytes.chunks_exact(row_bytes))
            .map(|row| i64::from_le_bytes(row[..8].try_into().unwrap()))
            .filter(|id| expected.contains(id))
            .count();
        let ranked = rank_returned_ranges(
            Sq8Geometry {
                rows: 100_000,
                dimensions: 768,
            },
            &ranges,
            &query,
            &generation.router().low,
            &generation.router().step,
            100,
            16_777_216,
        )
        .map_err(|_| invalid("Rust SQ8 ranking"))?;
        let returned = ranked
            .iter()
            .filter(|row| expected.contains(&row.id))
            .count();
        if plan["fetched_gt_hits"].as_u64() != Some(fetched as u64) {
            return Err(invalid("per-query page coverage parity").into());
        }
        if plan["returned_gt_hits"].as_u64() != Some(returned as u64) {
            returned_differences.push(json!({
                "query_ordinal":ordinal,
                "python_hits":plan["returned_gt_hits"],
                "rust_hits":returned,
            }));
        }
        fetched_hits.push(fetched);
        returned_hits.push(returned);
    }
    let result = json!({
        "schema":"borsuk-v291-rust-sq8-parity-v1",
        "dataset":"CoHere first100k D768 cosine k100", "split":"development0-63",
        "queries":64, "plans_sha256":plans_hash,
        "mean_fetched_gt_hits":fetched_hits.iter().sum::<usize>() as f64 / 64.0,
        "p05_fetched_gt_hits":p05(&fetched_hits),
        "mean_returned_gt_hits":returned_hits.iter().sum::<usize>() as f64 / 64.0,
        "p05_returned_gt_hits":p05(&returned_hits),
        "max_gets":max_gets, "max_planned_bytes":max_bytes,
        "fetched_per_query_parity":true,
        "returned_per_query_parity":returned_differences.is_empty(),
        "returned_differences":returned_differences,
    });
    fs::write(&args[7], format!("{result}\n"))?;
    println!("{result}");
    Ok(())
}
