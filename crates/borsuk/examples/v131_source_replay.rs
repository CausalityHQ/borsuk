//! Development-only replay of the sealed V122 100k physical range evidence.
//! Inputs are authenticated by the Spot worker before this process starts.

use borsuk::exact_sq8_nominee::{ScoredNominee, Sq8Geometry};
use borsuk::native_source_id_map::{NativeSourceIdMap, write_source_id_map};
use borsuk::native_source_tier::{NativeSourceTier, write_source_tier};
use borsuk::returned_sq8::{ReturnedRange, rank_returned_ranges};
use serde::Deserialize;
use serde_json::json;
use std::{
    collections::HashSet,
    error::Error,
    fs::{self, File},
    io::{BufRead, BufReader, BufWriter, Write},
    path::Path,
    time::Instant,
};

const ROWS: usize = 100_000;
const DIMENSIONS: usize = 96;
const ROW_BYTES: usize = 8 + DIMENSIONS * 4;
const SQ8_ROW_BYTES: usize = 12 + DIMENSIONS;
const GENERATION: u64 = 131;

#[derive(Deserialize)]
struct Query {
    query_ordinal: usize,
    source_query_ordinal: usize,
    query: Vec<f32>,
}

#[derive(Deserialize)]
struct Evidence {
    query_ordinal: usize,
    source_query_ordinal: usize,
    truth_ids: Vec<u64>,
    nominees: Vec<usize>,
    candidate_ranges: Vec<[usize; 2]>,
    baseline_ranges: Vec<[usize; 2]>,
    candidate_returned_ids: Vec<i64>,
    baseline_returned_ids: Vec<i64>,
    candidate_hits: usize,
    baseline_hits: usize,
}

fn invalid(message: impl Into<String>) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, message.into())
}

fn raw_id(row: &[u8]) -> u64 {
    u64::from_le_bytes(row[..8].try_into().unwrap())
}

fn read_f32_plane(path: &Path) -> Result<Vec<f32>, Box<dyn Error>> {
    let bytes = fs::read(path)?;
    if bytes.len() != DIMENSIONS * 4 {
        return Err(invalid("quantization plane byte length").into());
    }
    Ok(bytes
        .chunks_exact(4)
        .map(|raw| f32::from_le_bytes(raw.try_into().unwrap()))
        .collect())
}

fn score_ranges(
    sq8: &[u8],
    query: &[f32],
    low: &[f32],
    step: &[f32],
    ranges: &[[usize; 2]],
) -> Result<Vec<ScoredNominee>, Box<dyn Error>> {
    if ranges.is_empty() || ranges.len() > 32 {
        return Err(invalid("physical GET count").into());
    }
    let mut returned = Vec::with_capacity(ranges.len());
    for &[start, end] in ranges {
        if start >= end || end > sq8.len() {
            return Err(invalid("physical byte range").into());
        }
        returned.push(ReturnedRange {
            start,
            bytes: &sq8[start..end],
        });
    }
    rank_returned_ranges(
        Sq8Geometry {
            rows: ROWS,
            dimensions: DIMENSIONS,
        },
        &returned,
        query,
        low,
        step,
        512,
        16_777_216,
    )
    .map_err(|error| invalid(format!("returned SQ8 ranking: {error:?}")).into())
}

fn percentile_ms(values: &[u128], rank: usize) -> f64 {
    let mut ordered = values.to_vec();
    ordered.sort_unstable();
    ordered[(ordered.len() * rank).div_ceil(100) - 1] as f64 / 1_000_000.0
}

fn main() -> Result<(), Box<dyn Error>> {
    let arguments = std::env::args().collect::<Vec<_>>();
    let input_sha = std::env::var("V131_SOURCE_SHA256")?;
    if arguments.len() != 9 {
        return Err(invalid("usage: v131_source_replay source.raw sq8.bin low.bin step.bin queries.jsonl evidence.jsonl output-dir summary.json").into());
    }
    let raw = fs::read(&arguments[1])?;
    if raw.len() != ROWS * ROW_BYTES {
        return Err(invalid("raw source byte length").into());
    }
    let mut ids = HashSet::with_capacity(ROWS);
    for (ordinal, row) in raw.chunks_exact(ROW_BYTES).enumerate() {
        if raw_id(row) != ordinal as u64 || !ids.insert(raw_id(row)) {
            return Err(invalid("raw source ID order or uniqueness").into());
        }
    }
    let output = Path::new(&arguments[7]);
    fs::create_dir(output)?;
    let build_started = Instant::now();
    let source_path = output.join("source-tier.bin");
    let source_artifact_sha = write_source_tier(
        &source_path,
        ROWS as u64,
        DIMENSIONS,
        GENERATION,
        &input_sha,
        raw.chunks_exact(ROW_BYTES).map(|row| {
            let vector = row[8..]
                .chunks_exact(4)
                .map(|value| f32::from_le_bytes(value.try_into().unwrap()))
                .collect();
            (raw_id(row), vector)
        }),
    )?;
    let map_path = output.join("source-id-map.bin");
    let map_sha = write_source_id_map(
        &map_path,
        ROWS as u64,
        GENERATION,
        &input_sha,
        &source_artifact_sha,
        raw.chunks_exact(ROW_BYTES).map(raw_id),
    )?;
    drop(raw);
    let source = NativeSourceTier::open_authenticated(
        &source_path,
        &source_artifact_sha,
        &input_sha,
        ROWS as u64,
        DIMENSIONS,
        GENERATION,
        65_536,
    )?;
    let map = NativeSourceIdMap::open_authenticated(
        &map_path,
        &map_sha,
        &input_sha,
        &source_artifact_sha,
        ROWS as u64,
        GENERATION,
    )?;
    let build_open_ms = build_started.elapsed().as_secs_f64() * 1000.0;
    let sq8 = fs::read(&arguments[2])?;
    if sq8.len() != ROWS * SQ8_ROW_BYTES {
        return Err(invalid("SQ8 object byte length").into());
    }
    let low = read_f32_plane(Path::new(&arguments[3]))?;
    let step = read_f32_plane(Path::new(&arguments[4]))?;
    let queries = BufReader::new(File::open(&arguments[5])?)
        .lines()
        .collect::<Result<Vec<_>, _>>()?;
    let evidence = BufReader::new(File::open(&arguments[6])?)
        .lines()
        .collect::<Result<Vec<_>, _>>()?;
    if queries.len() != 1000 || evidence.len() != 1000 {
        return Err(invalid("query/evidence line count").into());
    }
    let mut rows = BufWriter::new(File::create(output.join("replay.jsonl"))?);
    let mut totals = [0usize; 2];
    let mut sq8_totals = [0usize; 2];
    let mut sealed_order_mismatches = [0usize; 2];
    let mut sealed_set_mismatches = [0usize; 2];
    let mut sealed_set_delta_total = [0usize; 2];
    let mut hit_rows = [Vec::new(), Vec::new()];
    let mut rank_ns = [Vec::new(), Vec::new()];
    let mut offline_total_ns = [Vec::new(), Vec::new()];
    let mut block_reads = [Vec::new(), Vec::new()];
    let mut local_bytes = [Vec::new(), Vec::new()];
    let mut count = 0usize;
    for (query_line, evidence_line) in queries.iter().zip(&evidence) {
        let query: Query = serde_json::from_str(query_line)?;
        let sealed: Evidence = serde_json::from_str(evidence_line)?;
        if query.query_ordinal != count
            || sealed.query_ordinal != count
            || query.source_query_ordinal != 9000 + count
            || sealed.source_query_ordinal != query.source_query_ordinal
            || query.query.len() != DIMENSIONS
            || sealed.truth_ids.len() != 100
            || sealed.nominees.len() != 512
            || sealed
                .truth_ids
                .iter()
                .copied()
                .collect::<HashSet<_>>()
                .len()
                != 100
        {
            return Err(invalid(format!("query/evidence identity at {count}")).into());
        }
        let router_ids = sealed
            .nominees
            .iter()
            .map(|&ordinal| {
                if ordinal >= ROWS {
                    return Err(invalid("router physical ordinal"));
                }
                let offset = ordinal * SQ8_ROW_BYTES;
                let id = i64::from_le_bytes(sq8[offset..offset + 8].try_into().unwrap());
                u64::try_from(id).map_err(|_| invalid("negative SQ8 source ID"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let mut arm_rows = [serde_json::Value::Null, serde_json::Value::Null];
        let mut arms = [
            (
                0,
                &sealed.candidate_ranges,
                &sealed.candidate_returned_ids,
                sealed.candidate_hits,
            ),
            (
                1,
                &sealed.baseline_ranges,
                &sealed.baseline_returned_ids,
                sealed.baseline_hits,
            ),
        ];
        if count % 2 == 1 {
            arms.swap(0, 1);
        }
        for (arm, ranges, sealed_ids, sealed_hits) in arms {
            let arm_started = Instant::now();
            let sq8_ranked = score_ranges(&sq8, &query.query, &low, &step, ranges)?;
            let first_100 = sq8_ranked
                .iter()
                .take(100)
                .map(|row| row.id)
                .collect::<Vec<_>>();
            let rust_set = first_100.iter().copied().collect::<HashSet<_>>();
            let sealed_set = sealed_ids.iter().copied().collect::<HashSet<_>>();
            if rust_set.len() != 100 || sealed_set.len() != 100 {
                return Err(
                    invalid(format!("SQ8 top-100 IDs duplicate at {count} arm {arm}")).into(),
                );
            }
            let order_differs = first_100 != *sealed_ids;
            let set_delta = rust_set.symmetric_difference(&sealed_set).count();
            sealed_order_mismatches[arm] += usize::from(order_differs);
            sealed_set_mismatches[arm] += usize::from(set_delta != 0);
            sealed_set_delta_total[arm] += set_delta;
            let sealed_hit_count = sealed_ids
                .iter()
                .filter(|id| sealed.truth_ids.contains(&(**id as u64)))
                .count();
            if sealed_hit_count != sealed_hits {
                return Err(invalid(format!(
                    "sealed hit count differs at query {count} arm {arm}"
                ))
                .into());
            }
            let sq8_hits = first_100
                .iter()
                .filter(|id| sealed.truth_ids.contains(&(**id as u64)))
                .count();
            sq8_totals[arm] += sq8_hits;
            let expanded_ids = sq8_ranked
                .iter()
                .map(|row| u64::try_from(row.id).map_err(|_| invalid("negative SQ8 source ID")))
                .collect::<Result<Vec<_>, _>>()?;
            let candidates = map.resolve_union(&source, &router_ids, &expanded_ids)?;
            let began = Instant::now();
            let (exact, stats) = source.rank_exact_with_stats(&query.query, &candidates, 100)?;
            let elapsed_ns = began.elapsed().as_nanos();
            let total_ns = arm_started.elapsed().as_nanos();
            let hits = exact
                .iter()
                .filter(|row| sealed.truth_ids.contains(&row.source_id))
                .count();
            totals[arm] += hits;
            hit_rows[arm].push(hits);
            rank_ns[arm].push(elapsed_ns);
            offline_total_ns[arm].push(total_ns);
            block_reads[arm].push(stats.verified_block_reads);
            local_bytes[arm].push(stats.local_read_bytes);
            arm_rows[arm] = json!({
                "hits": hits,
                "sq8_hits": sq8_hits,
                "sq8_returned_ids": first_100,
                "sealed_order_differs": order_differs,
                "sealed_set_delta": set_delta,
                "candidate_count": candidates.len(),
                "source_rows": stats.source_rows,
                "verified_block_reads": stats.verified_block_reads,
                "local_read_bytes": stats.local_read_bytes,
                "source_rank_ns": elapsed_ns,
                "offline_total_ns": total_ns,
                "returned_ids": exact.iter().map(|row| row.source_id).collect::<Vec<_>>(),
            });
        }
        writeln!(
            rows,
            "{}",
            json!({
                "query_ordinal": count,
                "source_query_ordinal": query.source_query_ordinal,
                "candidate": arm_rows[0],
                "baseline": arm_rows[1],
            })
        )?;
        count += 1;
    }
    if count != 1000 {
        return Err(invalid("replay query count").into());
    }
    rows.flush()?;
    let p05_hits = hit_rows.each_ref().map(|hits| {
        let mut ordered = hits.clone();
        ordered.sort_unstable();
        ordered[49]
    });
    let report = json!({
        "schema": "borsuk-v131-source-replay-v1",
        "dataset": "deep-image-96-angular-random-100k",
        "split": "test-ordinals-9000-through-9999-used-development",
        "rows": ROWS,
        "dimensions": DIMENSIONS,
        "query_count": count,
        "source_sha256": input_sha,
        "source_artifact_sha256": source_artifact_sha,
        "map_artifact_sha256": map_sha,
        "source_digest_resident_bytes": source.resident_digest_bytes(),
        "map_entry_resident_bytes": map.resident_entry_bytes(),
        "build_and_authenticated_open_ms": build_open_ms,
        "source_hits": {"candidate": totals[0], "baseline": totals[1]},
        "same_run_rust_sq8_hits": {"candidate": sq8_totals[0], "baseline": sq8_totals[1]},
        "sealed_v122_order_mismatch_queries": {"candidate": sealed_order_mismatches[0], "baseline": sealed_order_mismatches[1]},
        "sealed_v122_set_mismatch_queries": {"candidate": sealed_set_mismatches[0], "baseline": sealed_set_mismatches[1]},
        "sealed_v122_set_symmetric_delta_total": {"candidate": sealed_set_delta_total[0], "baseline": sealed_set_delta_total[1]},
        "source_rank_p50_ms": {"candidate": percentile_ms(&rank_ns[0], 50), "baseline": percentile_ms(&rank_ns[1], 50)},
        "source_rank_p95_ms": {"candidate": percentile_ms(&rank_ns[0], 95), "baseline": percentile_ms(&rank_ns[1], 95)},
        "offline_total_p50_ms": {"candidate": percentile_ms(&offline_total_ns[0], 50), "baseline": percentile_ms(&offline_total_ns[1], 50)},
        "offline_total_p95_ms": {"candidate": percentile_ms(&offline_total_ns[0], 95), "baseline": percentile_ms(&offline_total_ns[1], 95)},
        "source_local_bytes_max": {"candidate": *local_bytes[0].iter().max().unwrap(), "baseline": *local_bytes[1].iter().max().unwrap()},
        "source_block_reads_max": {"candidate": *block_reads[0].iter().max().unwrap(), "baseline": *block_reads[1].iter().max().unwrap()},
        "p05_hits": {"candidate": p05_hits[0], "baseline": p05_hits[1]},
        "sub90_queries": {"candidate": hit_rows[0].iter().filter(|&&v| v<90).count(), "baseline": hit_rows[1].iter().filter(|&&v| v<90).count()},
        "offline_only": true,
    });
    fs::write(&arguments[8], format!("{report}\n"))?;
    println!("{report}");
    Ok(())
}
