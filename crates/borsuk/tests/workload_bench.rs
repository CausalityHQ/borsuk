#![allow(missing_docs)]

//! Mixed read/write workload over time.
//!
//! Starting from an empty index, run a stream of operations that are a chosen
//! percentage reads (searches) and the rest writes (add a batch of vectors, then
//! compact). At checkpoints along the stream we record how the index size,
//! resident memory, and per-operation latency have evolved. Sweeping the read
//! percentage (1/10/20/50/80/90%) shows how a read-heavy vs write-heavy workload
//! grows the index and moves latency over time, while resident memory stays
//! flat because routing is paged.
//!
//! Fast test (`workload_sweep_is_sound`) runs a short stream as a correctness
//! gate; the ignored `workload_sweep_gate` runs a longer stream and writes
//! `docs/web/assets/benchmarks/workload.csv` when `BORSUK_WORKLOAD_OUTPUT` is set.

use std::{env, fs, path::Path, time::Instant};

use borsuk::{BorsukIndex, CompactionOptions, IndexConfig, SearchOptions, VectorMetric};

const READ_PERCENTS: [u32; 6] = [1, 10, 20, 50, 80, 90];

struct WorkloadConfig {
    dimensions: usize,
    add_batch: usize,
    ops_total: usize,
    checkpoint_every: usize,
}

struct WorkloadRow {
    read_pct: u32,
    ops: usize,
    vectors: usize,
    resident_bytes: u64,
    read_p50_ms: f64,
    add_p50_ms: f64,
}

/// A checkpoint aggregated over repetitions: the index size is deterministic, so
/// only the read/add latencies carry a mean and sample standard deviation.
struct AggWorkloadRow {
    read_pct: u32,
    ops: usize,
    vectors: usize,
    resident_bytes: u64,
    read_p50_ms_mean: f64,
    read_p50_ms_std: f64,
    add_p50_ms_mean: f64,
    add_p50_ms_std: f64,
}

fn std_dev(values: &[f64], mean: f64) -> f64 {
    if values.len() < 2 {
        return 0.0;
    }
    let variance = values
        .iter()
        .map(|value| (value - mean) * (value - mean))
        .sum::<f64>()
        / (values.len() - 1) as f64;
    variance.sqrt()
}

fn mean(values: &[f64]) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    values.iter().sum::<f64>() / values.len() as f64
}

/// Run the workload `repetitions` times and aggregate each checkpoint's latency
/// across the runs. The op stream is deterministic, so checkpoints align by index
/// (same `ops`/`vectors`) and only latency varies run to run.
fn run_workload_repeated(
    read_pct: u32,
    config: &WorkloadConfig,
    repetitions: usize,
) -> Vec<AggWorkloadRow> {
    let runs: Vec<Vec<WorkloadRow>> = (0..repetitions.max(1))
        .map(|_| run_workload(read_pct, config))
        .collect();
    let checkpoints = runs[0].len();
    (0..checkpoints)
        .map(|i| {
            let read: Vec<f64> = runs.iter().map(|run| run[i].read_p50_ms).collect();
            let add: Vec<f64> = runs.iter().map(|run| run[i].add_p50_ms).collect();
            let read_mean = mean(&read);
            let add_mean = mean(&add);
            AggWorkloadRow {
                read_pct,
                ops: runs[0][i].ops,
                vectors: runs[0][i].vectors,
                resident_bytes: runs[0][i].resident_bytes,
                read_p50_ms_mean: read_mean,
                read_p50_ms_std: std_dev(&read, read_mean),
                add_p50_ms_mean: add_mean,
                add_p50_ms_std: std_dev(&add, add_mean),
            }
        })
        .collect()
}

fn noise(seed: u64) -> f32 {
    let mut z = seed.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^= z >> 31;
    (z as f64 / u64::MAX as f64) as f32 * 2.0 - 1.0
}

fn vector_for(record: usize, dimensions: usize) -> Vec<f32> {
    (0..dimensions)
        .map(|dim| noise(((record as u64) << 8) ^ dim as u64))
        .collect()
}

fn median(values: &mut [f64]) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    values.sort_by(f64::total_cmp);
    values[values.len() / 2]
}

fn run_workload(read_pct: u32, config: &WorkloadConfig) -> Vec<WorkloadRow> {
    let dir = tempfile::tempdir().unwrap();
    let mut index = BorsukIndex::create(IndexConfig {
        uri: dir.path().to_string_lossy().into_owned(),
        metric: VectorMetric::Euclidean,
        dimensions: config.dimensions,
        segment_max_vectors: config.add_batch.max(1),
        ram_budget_bytes: None,
        text: false,
        named_vectors: Default::default(),
    })
    .unwrap();

    let mut rows = Vec::new();
    let mut next_record = 0usize;
    let mut read_credit = 0i64;
    let mut read_samples: Vec<f64> = Vec::new();
    let mut add_samples: Vec<f64> = Vec::new();
    let query = vector_for(0xBEEF, config.dimensions);

    for op in 1..=config.ops_total {
        // Deterministic read/write interleaving at the requested ratio.
        read_credit += read_pct as i64;
        let do_read = read_credit >= 100 && next_record > 0;
        if do_read {
            read_credit -= 100;
            let started = Instant::now();
            let _ = index
                .search_ids(&query, SearchOptions::approx(10, borsuk::LeafMode::PqScan))
                .unwrap();
            read_samples.push(started.elapsed().as_secs_f64() * 1000.0);
        } else {
            let started = Instant::now();
            let batch: Vec<_> = (0..config.add_batch)
                .map(|i| {
                    let record = next_record + i;
                    borsuk::VectorRecord::new(
                        format!("v{record}"),
                        vector_for(record, config.dimensions),
                    )
                })
                .collect();
            index.add(batch).unwrap();
            next_record += config.add_batch;
            // Compact after each add, folding the new L0 batch into L1 leaves.
            index
                .compact(CompactionOptions {
                    source_level: 0,
                    target_level: 1,
                    max_segments: None,
                    min_segments: 1,
                    target_segment_max_vectors: Some(config.add_batch * 8),
                    target_segment_max_radius: None,
                })
                .unwrap();
            add_samples.push(started.elapsed().as_secs_f64() * 1000.0);
        }

        if op % config.checkpoint_every == 0 {
            let stats = index.stats();
            rows.push(WorkloadRow {
                read_pct,
                ops: op,
                vectors: stats.records,
                resident_bytes: stats.resident_bytes_estimate,
                read_p50_ms: median(&mut read_samples.clone()),
                add_p50_ms: median(&mut add_samples.clone()),
            });
            read_samples.clear();
            add_samples.clear();
        }
    }
    rows
}

fn workload_csv(rows: &[AggWorkloadRow]) -> String {
    let mut csv = String::from(
        "read_pct,ops,vectors,resident_bytes,read_p50_ms,read_p50_ms_std,add_p50_ms,add_p50_ms_std\n",
    );
    for row in rows {
        csv.push_str(&format!(
            "{},{},{},{},{:.3},{:.3},{:.3},{:.3}\n",
            row.read_pct,
            row.ops,
            row.vectors,
            row.resident_bytes,
            row.read_p50_ms_mean,
            row.read_p50_ms_std,
            row.add_p50_ms_mean,
            row.add_p50_ms_std,
        ));
    }
    csv
}

#[test]
fn workload_sweep_is_sound() {
    let config = WorkloadConfig {
        dimensions: 8,
        add_batch: 16,
        ops_total: 40,
        checkpoint_every: 10,
    };
    let mut all = Vec::new();
    for read_pct in READ_PERCENTS {
        all.extend(run_workload(read_pct, &config));
    }
    assert!(!all.is_empty());
    // A write-heavy stream (1% reads) ends with more vectors than a read-heavy
    // one (90% reads) after the same number of operations.
    let write_heavy = all
        .iter()
        .filter(|r| r.read_pct == 1)
        .map(|r| r.vectors)
        .max()
        .unwrap();
    let read_heavy = all
        .iter()
        .filter(|r| r.read_pct == 90)
        .map(|r| r.vectors)
        .max()
        .unwrap();
    assert!(
        write_heavy > read_heavy,
        "write-heavy workload ({write_heavy}) should ingest more than read-heavy ({read_heavy})"
    );
    // Resident memory stays small (paged routing) regardless of index size.
    for row in &all {
        assert!(
            row.resident_bytes < 5_000_000,
            "resident memory should stay bounded, got {}",
            row.resident_bytes
        );
    }
}

#[test]
#[ignore = "benchmark gate; run explicitly to regenerate workload.csv"]
fn workload_sweep_gate() {
    let config = WorkloadConfig {
        dimensions: 16,
        add_batch: 32,
        ops_total: 160,
        checkpoint_every: 16,
    };
    // Repeat the whole stream so each checkpoint's latency gets a mean ± std.
    let repetitions = 5;
    let mut all = Vec::new();
    for read_pct in READ_PERCENTS {
        all.extend(run_workload_repeated(read_pct, &config, repetitions));
    }
    let csv = workload_csv(&all);
    eprintln!("{csv}");
    if let Ok(output) = env::var("BORSUK_WORKLOAD_OUTPUT") {
        fs::write(Path::new(&output), csv).unwrap();
    }
}
