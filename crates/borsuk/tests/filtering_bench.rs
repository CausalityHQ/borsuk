#![allow(missing_docs)]

//! Filtered-search selectivity benchmark.
//!
//! Scenario: a multi-tenant index whose partition key (`tenant`) is uncorrelated
//! with the embedding geometry but *is* correlated with segments, because each
//! tenant is ingested as its own append-only batch. Every segment therefore holds
//! exactly one tenant, and — since the vectors are drawn from one global
//! distribution — the segments overlap heavily in space. Spatial routing alone
//! cannot separate tenants, so an unfiltered query must read almost every
//! segment. This isolates the value of metadata pruning: a `tenant` filter skips
//! whole segments by their metadata statistics, and the sweep shows bytes read
//! and segments searched collapsing as the filter grows more selective while
//! recall stays exact.
//!
//! The fast test (`filtering_selectivity_sweep_is_sound`) runs a small sweep as a
//! correctness gate. The ignored `filtering_selectivity_sweep_gate` runs a larger
//! sweep and writes `docs/web/assets/benchmarks/filtering.csv` when
//! `BORSUK_FILTERING_OUTPUT` is set.

use std::{env, fs, path::Path, time::Instant};

use borsuk::{BorsukIndex, Filter, IndexConfig, MetaValue, Metadata, SearchOptions, VectorMetric};

const K: usize = 10;

struct SweepConfig {
    tenants: usize,
    records_per_tenant: usize,
    dimensions: usize,
    segment_max_vectors: usize,
    queries: usize,
}

struct FilteringRow {
    label: String,
    selectivity_target: f64,
    dimensions: usize,
    records: usize,
    segments_total: usize,
    matching_records: usize,
    p50_ms: f64,
    p95_ms: f64,
    avg_segments_searched: f64,
    avg_segments_pruned_by_filter: f64,
    avg_bytes_read: f64,
    avg_rows_evaluated: f64,
    avg_rows_passed_filter: f64,
    id_recall_at_10: f64,
}

/// Deterministic pseudo-random float in [-1, 1] from a 64-bit seed (splitmix64).
fn noise(seed: u64) -> f32 {
    let mut z = seed.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^= z >> 31;
    (z as f64 / u64::MAX as f64) as f32 * 2.0 - 1.0
}

/// Global-distribution vector: independent of tenant, so segments overlap in space.
fn vector_for(record: usize, dimensions: usize) -> Vec<f32> {
    (0..dimensions)
        .map(|dim| noise(((record as u64) << 8) ^ dim as u64))
        .collect()
}

fn query_vector(index: usize, dimensions: usize) -> Vec<f32> {
    const QUERY_SALT: u64 = 0xD1B5_4A32_D192_ED03;
    (0..dimensions)
        .map(|dim| noise(QUERY_SALT ^ ((index as u64) << 12) ^ dim as u64))
        .collect()
}

fn euclidean(a: &[f32], b: &[f32]) -> f32 {
    a.iter()
        .zip(b.iter())
        .map(|(x, y)| (x - y) * (x - y))
        .sum::<f32>()
        .sqrt()
}

fn percentile(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let rank = (p * (sorted.len() as f64 - 1.0)).round() as usize;
    sorted[rank.min(sorted.len() - 1)]
}

fn run_sweep(config: &SweepConfig) -> Vec<FilteringRow> {
    let dir = tempfile::tempdir().unwrap();
    let mut index = BorsukIndex::create(IndexConfig {
        uri: dir.path().to_string_lossy().into_owned(),
        metric: VectorMetric::Euclidean,
        dimensions: config.dimensions,
        segment_max_vectors: config.segment_max_vectors,
        ram_budget_bytes: None,
    })
    .unwrap();

    // One append per tenant keeps each segment tenant-pure.
    let mut ground: Vec<(String, usize, Vec<f32>)> = Vec::new();
    for tenant in 0..config.tenants {
        let mut records = Vec::with_capacity(config.records_per_tenant);
        for local in 0..config.records_per_tenant {
            let global = tenant * config.records_per_tenant + local;
            let id = format!("t{tenant}-r{local}");
            let vector = vector_for(global, config.dimensions);
            let mut meta = Metadata::new();
            meta.insert("tenant".into(), MetaValue::Int(tenant as i64));
            ground.push((id.clone(), tenant, vector.clone()));
            records.push(borsuk::VectorRecord::new(id, vector).with_metadata(meta));
        }
        index.add(records).unwrap();
    }

    let records = config.tenants * config.records_per_tenant;
    let segments_total = index.stats().segments;

    // Selectivity levels: unfiltered baseline plus tenant-prefix ranges.
    let levels: [(&str, f64); 4] = [("100%", 1.0), ("25%", 0.25), ("5%", 0.05), ("1%", 0.01)];

    let queries: Vec<Vec<f32>> = (0..config.queries)
        .map(|q| query_vector(q, config.dimensions))
        .collect();

    let mut rows = Vec::new();
    for (label, target) in levels {
        let cutoff = if target >= 1.0 {
            config.tenants
        } else {
            ((config.tenants as f64 * target).round() as usize).max(1)
        };
        let filter = if target >= 1.0 {
            None
        } else {
            Some(Filter::from_json(&serde_json::json!({ "tenant": { "$lt": cutoff } })).unwrap())
        };
        let matching_records = if target >= 1.0 {
            records
        } else {
            cutoff * config.records_per_tenant
        };

        let mut latencies = Vec::with_capacity(queries.len());
        let mut segments_searched = 0.0;
        let mut segments_pruned = 0.0;
        let mut bytes_read = 0.0;
        let mut rows_evaluated = 0.0;
        let mut rows_passed = 0.0;
        let mut recall_sum = 0.0;

        for query in &queries {
            let mut options = SearchOptions::exact(K);
            if let Some(filter) = &filter {
                options = options.with_filter(filter.clone());
            }
            let started = Instant::now();
            let report = index.search_with_report(query, options).unwrap();
            latencies.push(started.elapsed().as_secs_f64() * 1000.0);

            segments_searched += report.segments_searched as f64;
            segments_pruned += report.segments_pruned_by_filter as f64;
            bytes_read += report.bytes_read as f64;
            rows_evaluated += report.rows_evaluated as f64;
            rows_passed += report.rows_passed_filter as f64;

            // Brute-force filtered ground truth: exact top-K among matching rows.
            let mut scored: Vec<(f32, &str)> = ground
                .iter()
                .filter(|(_, tenant, _)| target >= 1.0 || *tenant < cutoff)
                .map(|(id, _, vector)| (euclidean(query, vector), id.as_str()))
                .collect();
            scored.sort_by(|a, b| a.0.total_cmp(&b.0));
            let truth: Vec<&str> = scored.iter().take(K).map(|(_, id)| *id).collect();
            let got: Vec<String> = report.hits.iter().map(|hit| hit.id.to_string()).collect();
            let overlap = truth
                .iter()
                .filter(|id| got.iter().any(|g| g == *id))
                .count();
            recall_sum += overlap as f64 / truth.len().max(1) as f64;
        }

        let mut sorted = latencies.clone();
        sorted.sort_by(f64::total_cmp);
        let n = queries.len() as f64;
        rows.push(FilteringRow {
            label: label.to_string(),
            selectivity_target: target,
            dimensions: config.dimensions,
            records,
            segments_total,
            matching_records,
            p50_ms: percentile(&sorted, 0.50),
            p95_ms: percentile(&sorted, 0.95),
            avg_segments_searched: segments_searched / n,
            avg_segments_pruned_by_filter: segments_pruned / n,
            avg_bytes_read: bytes_read / n,
            avg_rows_evaluated: rows_evaluated / n,
            avg_rows_passed_filter: rows_passed / n,
            id_recall_at_10: recall_sum / n,
        });
    }
    rows
}

fn filtering_csv(rows: &[FilteringRow]) -> String {
    let mut csv = String::from(
        "selectivity,selectivity_target,dimensions,records,segments_total,matching_records,p50_ms,p95_ms,avg_segments_searched,avg_segments_pruned_by_filter,avg_bytes_read,avg_rows_evaluated,avg_rows_passed_filter,id_recall_at_10\n",
    );
    for row in rows {
        csv.push_str(&format!(
            "{},{:.6},{},{},{},{},{:.3},{:.3},{:.3},{:.3},{:.1},{:.1},{:.1},{:.6}\n",
            row.label,
            row.selectivity_target,
            row.dimensions,
            row.records,
            row.segments_total,
            row.matching_records,
            row.p50_ms,
            row.p95_ms,
            row.avg_segments_searched,
            row.avg_segments_pruned_by_filter,
            row.avg_bytes_read,
            row.avg_rows_evaluated,
            row.avg_rows_passed_filter,
            row.id_recall_at_10,
        ));
    }
    csv
}

#[test]
fn filtering_selectivity_sweep_is_sound() {
    let rows = run_sweep(&SweepConfig {
        tenants: 12,
        records_per_tenant: 24,
        dimensions: 8,
        segment_max_vectors: 64,
        queries: 16,
    });
    assert_eq!(rows.len(), 4);

    let csv = filtering_csv(&rows);
    assert!(csv.starts_with(
        "selectivity,selectivity_target,dimensions,records,segments_total,matching_records,p50_ms,p95_ms,avg_segments_searched,avg_segments_pruned_by_filter,avg_bytes_read,avg_rows_evaluated,avg_rows_passed_filter,id_recall_at_10\n"
    ));

    let baseline = &rows[0];
    let tightest = &rows[3];

    // Exact search is sound with or without pruning: recall stays 1.0.
    for row in &rows {
        assert!(
            row.id_recall_at_10 >= 0.999,
            "{} recall regressed to {}",
            row.label,
            row.id_recall_at_10
        );
    }
    // The unfiltered baseline prunes nothing.
    assert_eq!(baseline.avg_segments_pruned_by_filter, 0.0);
    // A tighter filter prunes strictly more segments and reads fewer bytes.
    assert!(tightest.avg_segments_pruned_by_filter > baseline.avg_segments_pruned_by_filter);
    assert!(tightest.avg_bytes_read < baseline.avg_bytes_read);
    assert!(tightest.matching_records < baseline.matching_records);
}

#[test]
#[ignore = "benchmark gate; run explicitly to regenerate filtering.csv"]
fn filtering_selectivity_sweep_gate() {
    let rows = run_sweep(&SweepConfig {
        tenants: 100,
        records_per_tenant: 200,
        dimensions: 16,
        segment_max_vectors: 256,
        queries: 50,
    });
    let csv = filtering_csv(&rows);
    eprintln!("{csv}");
    if let Ok(output) = env::var("BORSUK_FILTERING_OUTPUT") {
        fs::write(Path::new(&output), csv).unwrap();
    }
}
