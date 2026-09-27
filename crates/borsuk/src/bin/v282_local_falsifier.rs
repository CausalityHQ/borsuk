//! Paired V282 page-discovery falsifier using a local, authenticated SQ8 object.
//! Local reads measure quality and planned I/O, never S3 service latency.

use std::collections::HashSet;
use std::error::Error;
use std::fs;
use std::io::{self, BufRead, BufReader, BufWriter, Write};
use std::path::Path;
use std::time::Instant;

use borsuk::exact_sq8_nominee::Sq8Geometry;
use borsuk::object_native_generation::{ObjectNativeGeneration, ObjectNativeLimits};
use borsuk::returned_sq8::{ReturnedRange, rank_returned_ranges};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

fn rss_bytes() -> Result<u64, io::Error> {
    let status = fs::read_to_string("/proc/self/status")?;
    let line = status
        .lines()
        .find(|line| line.starts_with("VmRSS:"))
        .ok_or_else(|| invalid("VmRSS unavailable"))?;
    let kib = line
        .split_whitespace()
        .nth(1)
        .ok_or_else(|| invalid("VmRSS malformed"))?
        .parse::<u64>()
        .map_err(|_| invalid("VmRSS malformed"))?;
    Ok(kib * 1024)
}

fn main() -> Result<(), Box<dyn Error>> {
    let args = std::env::args().collect::<Vec<_>>();
    if args.len() != 7 {
        return Err(
            "usage: v282_local_falsifier ROOT ROOT_SHA SQ8 REQUESTS GT_U32 OUTPUT_JSONL".into(),
        );
    }
    let root = Path::new(&args[1]);
    let root_bytes = fs::read(root.join("manifest.json"))?;
    if format!("{:x}", Sha256::digest(&root_bytes)) != args[2] {
        return Err(invalid("root digest differs").into());
    }
    let rows = serde_json::from_slice::<Value>(&root_bytes)?["rows"]
        .as_u64()
        .ok_or_else(|| invalid("generation row count"))? as usize;
    let pages = rows.div_ceil(256);
    let regions = (1024 * pages).div_ceil(3907).min(pages);
    let generation = ObjectNativeGeneration::open(
        root,
        &args[2],
        ObjectNativeLimits {
            max_memory_bytes: 4 * 1024 * 1024 * 1024,
            max_active_queries: 1,
            max_query_bytes: 16 * 1024 * 1024,
            max_query_gets: 32,
            max_parallel_gets: 32,
            max_router_regions: regions,
            max_router_shortlist: 512.min(rows),
            already_pinned_bytes: 0,
        },
    )
    .map_err(|error| invalid(format!("generation open: {error:?}")))?;
    let dimensions = generation.pages().dimensions();
    let row_bytes = dimensions + 12;
    let sq8 = fs::read(&args[3])?;
    if sq8.len() != rows * row_bytes
        || format!("{:x}", Sha256::digest(&sq8)) != generation.router().sq8_sha256
    {
        return Err(invalid("local SQ8 identity differs").into());
    }
    let ids = sq8
        .chunks_exact(row_bytes)
        .map(|record| i64::from_le_bytes(record[..8].try_into().unwrap()))
        .collect::<Vec<_>>();
    let id_set = ids.iter().copied().collect::<HashSet<_>>();
    let truth = fs::read(&args[5])?;
    let source = BufReader::new(fs::File::open(&args[4])?);
    let mut output = BufWriter::new(fs::File::create_new(&args[6])?);
    let mut count = 0usize;
    for line in source.lines() {
        let request: Value = serde_json::from_str(&line?)?;
        let ordinal = request
            .get("query_ordinal")
            .or_else(|| request.get("ordinal"))
            .and_then(Value::as_u64)
            .ok_or_else(|| invalid("query ordinal"))? as usize;
        if ordinal != count || truth.len() < (ordinal + 1) * 400 {
            return Err(invalid("query/truth roster differs").into());
        }
        let query = serde_json::from_value::<Vec<f32>>(request["query"].clone())?;
        if query.len() != dimensions || query.iter().any(|value| !value.is_finite()) {
            return Err(invalid("query geometry differs").into());
        }
        let expected = truth[ordinal * 400..(ordinal + 1) * 400]
            .chunks_exact(4)
            .map(|word| u32::from_le_bytes(word.try_into().unwrap()) as i64)
            .collect::<HashSet<_>>();
        if expected.len() != 100 || expected.iter().any(|id| !id_set.contains(id)) {
            return Err(invalid("truth IDs differ from corpus").into());
        }
        let shortlist = generation
            .router()
            .router
            .nominate(&query, regions, 512.min(rows))
            .map_err(|error| invalid(format!("router: {error:?}")))?;
        let router_hits = shortlist
            .iter()
            .filter(|&&row| expected.contains(&ids[row]))
            .count();
        for arm in ["graph", "flat"] {
            let start = Instant::now();
            let plan = match arm {
                "graph" => generation.plan_pages(&query, regions, 512.min(rows), 100, 4),
                _ => generation.plan_pages_flat_control(&query, regions, 512.min(rows), 100, 4),
            }
            .map_err(|error| invalid(format!("plan: {error:?}")))?;
            let plan_us = start.elapsed().as_micros();
            let payloads = plan
                .ranges
                .iter()
                .map(|range| {
                    let first = range.start / (256 * row_bytes);
                    let last = (range.end - 1) / (256 * row_bytes);
                    let payload = &sq8[range.clone()];
                    generation
                        .pages()
                        .verify_payload(first, last, payload)
                        .map_err(|error| invalid(format!("page digest: {error:?}")))?;
                    Ok::<_, io::Error>(ReturnedRange {
                        start: range.start,
                        bytes: payload,
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;
            let fetched = payloads
                .iter()
                .flat_map(|range| range.bytes.chunks_exact(row_bytes))
                .map(|record| i64::from_le_bytes(record[..8].try_into().unwrap()))
                .collect::<HashSet<_>>();
            let fetched_hits = expected.intersection(&fetched).count();
            let rank_start = Instant::now();
            let scored = rank_returned_ranges(
                Sq8Geometry { rows, dimensions },
                &payloads,
                &query,
                &generation.router().low,
                &generation.router().step,
                100,
                16 * 1024 * 1024,
            )
            .map_err(|error| invalid(format!("SQ8 rank: {error:?}")))?;
            let rank_us = rank_start.elapsed().as_micros();
            let hits = scored
                .iter()
                .filter(|row| expected.contains(&row.id))
                .count();
            writeln!(
                output,
                "{}",
                json!({
                    "ordinal": ordinal, "arm": arm,
                    "router_shortlist_gt_hits": router_hits,
                    "fetched_gt_hits": fetched_hits,
                    "sq8_returned_gt_hits": hits,
                    "gets": plan.ranges.len(), "bytes": plan.planned_bytes,
                "selected_pages": plan.selected_pages.len(),
                "plan_us": plan_us, "rank_us": rank_us,
                "rss_bytes": rss_bytes()?,
                })
            )?;
        }
        count += 1;
    }
    if count != 1000 || truth.len() != count * 400 {
        return Err(invalid("expected exactly 1,000 queries and GT100 rows").into());
    }
    output.flush()?;
    Ok(())
}
