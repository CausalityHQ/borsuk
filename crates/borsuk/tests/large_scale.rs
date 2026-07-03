#![allow(missing_docs)]

use std::{env, time::Instant};

use borsuk::{
    BorsukIndex, CompactionOptions, IndexConfig, LeafMode, SearchHit, SearchOptions, SearchReport,
    VectorMetric,
};

const DEFAULT_RECORDS: usize = 1_000_000;
const DEFAULT_DIMENSIONS: usize = 16;
const DEFAULT_SEGMENT_MAX_VECTORS: usize = 128;
const DEFAULT_BATCH_RECORDS: usize = 8_192;
const DEFAULT_MAX_SEGMENTS: usize = 512;
const DEFAULT_MAX_CANDIDATES_PER_SEGMENT: usize = 128;
const DEFAULT_MIN_TIE_AWARE_RECALL: f32 = 0.95;
const DEFAULT_MAX_RESIDENT_BYTES: u64 = 128 * 1024 * 1024;

#[test]
fn tie_aware_recall_counts_equal_distance_large_scale_hits() {
    let exact = (0..10)
        .map(|idx| SearchHit {
            id: format!("exact-{idx}").into(),
            distance: 0.0,
        })
        .collect::<Vec<_>>();
    let actual = (0..10)
        .map(|idx| SearchHit {
            id: format!("equivalent-{idx}").into(),
            distance: 0.0,
        })
        .collect::<Vec<_>>();

    assert_eq!(tie_aware_recall_at_k(&exact, &actual, 10), 1.0);
}

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
    let max_segments = env_usize("BORSUK_LARGE_SCALE_MAX_SEGMENTS", DEFAULT_MAX_SEGMENTS);
    let max_candidates_per_segment = env_usize(
        "BORSUK_LARGE_SCALE_MAX_CANDIDATES_PER_SEGMENT",
        DEFAULT_MAX_CANDIDATES_PER_SEGMENT,
    );
    let min_tie_aware_recall = env_f32(
        "BORSUK_LARGE_SCALE_MIN_TIE_AWARE_RECALL",
        DEFAULT_MIN_TIE_AWARE_RECALL,
    );
    let max_resident_bytes = env_u64(
        "BORSUK_LARGE_SCALE_MAX_RESIDENT_BYTES",
        DEFAULT_MAX_RESIDENT_BYTES,
    );

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

    let compaction_started = Instant::now();
    let compaction = index
        .compact(CompactionOptions {
            source_level: 0,
            target_level: 1,
            max_segments: None,
            min_segments: 1,
            target_segment_max_vectors: Some(segment_max_vectors),
        })
        .unwrap();
    assert!(compaction.compacted);
    assert_eq!(compaction.segments_read, stats.segments);
    assert_eq!(compaction.records_rewritten, record_count);
    assert_eq!(compaction.graph_payloads_read, 0);
    assert_eq!(compaction.graph_bytes_read, 0);

    let compacted_stats = index.stats();
    assert_eq!(compacted_stats.records, record_count);
    assert!(compacted_stats.resident_bytes_estimate <= max_resident_bytes);

    let query = deterministic_vector(42, dimensions);
    let exact_started = Instant::now();
    let exact = index
        .search_with_report(&query, SearchOptions::exact(10))
        .unwrap();
    assert_eq!(exact.hits.first().map(|hit| hit.id.as_str()), Some("42"));
    assert_eq!(exact.graph_bytes_read, 0);
    assert!(exact.resident_bytes_estimate <= max_resident_bytes);

    let modes = [
        (LeafMode::PqScan, false),
        (LeafMode::VamanaPq, true),
        (LeafMode::Hybrid, true),
    ];
    for (leaf_mode, expect_graph_reads) in modes {
        let approx_started = Instant::now();
        let approx = index
            .search_with_report(
                &query,
                SearchOptions::approx(10, leaf_mode)
                    .with_max_segments(max_segments)
                    .with_max_candidates_per_segment(max_candidates_per_segment),
            )
            .unwrap();
        assert_high_recall_report(
            &exact.hits,
            &approx,
            min_tie_aware_recall,
            max_segments,
            max_resident_bytes,
            expect_graph_reads,
        );

        eprintln!(
            "large_scale_query mode={} recall={:.3} query_ms={} segments={} bytes={} graph_bytes={} resident_bytes={}",
            approx.leaf_mode,
            tie_aware_recall_at_k(&exact.hits, &approx.hits, 10),
            approx_started.elapsed().as_millis(),
            approx.segments_searched,
            approx.bytes_read,
            approx.graph_bytes_read,
            approx.resident_bytes_estimate,
        );
    }

    eprintln!(
        "large_scale records={} dimensions={} pre_segments={} post_segments={} ingest_ms={} compaction_ms={} exact_ms={} compaction_bytes_read={} compaction_bytes_written={} resident_bytes={}",
        stats.records,
        stats.dimensions,
        stats.segments,
        compacted_stats.segments,
        ingest_started.elapsed().as_millis(),
        compaction_started.elapsed().as_millis(),
        exact_started.elapsed().as_millis(),
        compaction.bytes_read,
        compaction.bytes_written,
        exact.resident_bytes_estimate,
    );
}

fn assert_high_recall_report(
    exact_hits: &[SearchHit],
    report: &SearchReport,
    min_tie_aware_recall: f32,
    max_segments: usize,
    max_resident_bytes: u64,
    expect_graph_reads: bool,
) {
    let recall = tie_aware_recall_at_k(exact_hits, &report.hits, 10);
    assert!(
        recall >= min_tie_aware_recall,
        "{} tie-aware recall@10 was {recall}, below {min_tie_aware_recall}; hits={:?}",
        report.leaf_mode,
        report.hits
    );
    assert!(report.segments_searched <= max_segments);
    assert!(report.resident_bytes_estimate <= max_resident_bytes);
    if expect_graph_reads {
        assert!(report.graph_bytes_read > 0);
        assert!(report.graph_candidates_added > 0);
    } else {
        assert_eq!(report.graph_bytes_read, 0);
        assert_eq!(report.graph_candidates_added, 0);
    }
}

fn tie_aware_recall_at_k(exact_hits: &[SearchHit], actual_hits: &[SearchHit], k: usize) -> f32 {
    assert!(k > 0, "k must be greater than zero");
    let exact_top = exact_hits.iter().take(k).collect::<Vec<_>>();
    if exact_top.is_empty() {
        return 0.0;
    }

    let kth_distance = exact_top.last().expect("exact_top is non-empty").distance;
    let tolerance = kth_distance.abs().max(1.0) * 1.0e-6;
    let accepted = actual_hits
        .iter()
        .take(k)
        .filter(|hit| hit.distance <= kth_distance + tolerance)
        .count();

    accepted as f32 / exact_top.len() as f32
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

fn env_u64(name: &str, default: u64) -> u64 {
    env::var(name)
        .ok()
        .map(|value| {
            value
                .parse::<u64>()
                .unwrap_or_else(|error| panic!("{name} must be a u64: {error}"))
        })
        .unwrap_or(default)
}

fn env_f32(name: &str, default: f32) -> f32 {
    env::var(name)
        .ok()
        .map(|value| {
            value
                .parse::<f32>()
                .unwrap_or_else(|error| panic!("{name} must be an f32: {error}"))
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
