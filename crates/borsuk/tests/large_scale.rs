#![allow(missing_docs)]

use std::{env, time::Instant};

use borsuk::{BorsukIndex, IndexConfig, LeafMode, SearchOptions, VectorMetric};

const DEFAULT_RECORDS: usize = 1_000_000;
const DEFAULT_DIMENSIONS: usize = 16;
const DEFAULT_SEGMENT_MAX_VECTORS: usize = 128;
const DEFAULT_BATCH_RECORDS: usize = 8_192;

#[test]
#[ignore = "heavy release gate; run explicitly for million-vector scale coverage"]
fn million_vector_local_search_scale_gate() {
    let record_count = env_usize("BORSUK_LARGE_SCALE_RECORDS", DEFAULT_RECORDS);
    assert!(
        record_count >= DEFAULT_RECORDS,
        "large-scale gate must run at least {DEFAULT_RECORDS} vectors; got {record_count}"
    );
    let dimensions = env_usize("BORSUK_LARGE_SCALE_DIMENSIONS", DEFAULT_DIMENSIONS);
    let segment_max_vectors = env_usize(
        "BORSUK_LARGE_SCALE_SEGMENT_MAX_VECTORS",
        DEFAULT_SEGMENT_MAX_VECTORS,
    );
    let batch_records = env_usize("BORSUK_LARGE_SCALE_BATCH_RECORDS", DEFAULT_BATCH_RECORDS);

    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();
    let mut index = BorsukIndex::create(IndexConfig {
        uri,
        metric: VectorMetric::Euclidean,
        dimensions,
        segment_max_vectors,
        ram_budget_bytes: None,
    })
    .unwrap();

    let ingest_started = Instant::now();
    let mut inserted = 0_usize;
    while inserted < record_count {
        let end = inserted.saturating_add(batch_records).min(record_count);
        let vectors = (inserted..end)
            .map(|seed| deterministic_vector(seed, dimensions))
            .collect::<Vec<_>>();
        let ids = index.add_vectors(vectors).unwrap();
        assert_eq!(ids.len(), end - inserted);
        inserted = end;
    }

    let stats = index.stats();
    assert_eq!(stats.records, record_count);
    assert_eq!(stats.dimensions, dimensions);
    assert!(stats.segments > 1);

    let query = deterministic_vector(42, dimensions);
    let exact_started = Instant::now();
    let exact = index
        .search_with_report(&query, SearchOptions::exact(1))
        .unwrap();
    assert_eq!(exact.hits.first().map(|hit| hit.id.as_str()), Some("42"));
    assert_eq!(exact.graph_bytes_read, 0);

    let approx_started = Instant::now();
    let approx = index
        .search_with_report(
            &query,
            SearchOptions::approx(10, LeafMode::PqScan)
                .with_max_segments(64)
                .with_max_candidates_per_segment(128),
        )
        .unwrap();
    assert!(!approx.hits.is_empty());
    assert!(approx.segments_searched <= 64);
    assert_eq!(approx.graph_bytes_read, 0);

    eprintln!(
        "large_scale records={} dimensions={} segments={} ingest_ms={} exact_ms={} approx_ms={} approx_bytes={} resident_bytes={}",
        stats.records,
        stats.dimensions,
        stats.segments,
        ingest_started.elapsed().as_millis(),
        exact_started.elapsed().as_millis(),
        approx_started.elapsed().as_millis(),
        approx.bytes_read,
        approx.resident_bytes_estimate,
    );
}

fn env_usize(name: &str, default: usize) -> usize {
    env::var(name)
        .ok()
        .map(|value| {
            value
                .parse::<usize>()
                .unwrap_or_else(|error| panic!("{name} must be a usize: {error}"))
        })
        .unwrap_or(default)
}

fn deterministic_vector(seed: usize, dimensions: usize) -> Vec<f32> {
    (0..dimensions)
        .map(|dimension| centered_unit(seed, dimension))
        .collect()
}

fn centered_unit(seed: usize, dimension: usize) -> f32 {
    let mixed = splitmix64(
        (seed as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15)
            ^ (dimension as u64).wrapping_mul(0xBF58_476D_1CE4_E5B9),
    );
    let unit = (mixed >> 40) as f32 / (1_u64 << 24) as f32;
    unit - 0.5
}

fn splitmix64(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = value;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}
