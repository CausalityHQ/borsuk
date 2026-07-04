#![allow(missing_docs)]

use std::{env, fs, path::Path, time::Instant};

use borsuk::{
    BorsukIndex, CompactionOptions, IndexConfig, LeafMode, SearchHit, SearchOptions, SearchReport,
    VectorMetric, recall_at_k, tie_aware_recall_at_k,
};

const DEFAULT_RECORDS: usize = 1_000_000;
const DEFAULT_DIMENSIONS: usize = 16;
const DEFAULT_SEGMENT_MAX_VECTORS: usize = 128;
const DEFAULT_BATCH_RECORDS: usize = 8_192;
const DEFAULT_MAX_SEGMENTS: usize = 512;
const DEFAULT_ROUTING_PAGE_OVERFETCH: usize = 8;
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

    assert_eq!(
        tie_aware_recall_at_k(&hit_distances(&exact), &hit_distances(&actual), 10).unwrap(),
        1.0
    );
}

#[test]
fn large_scale_csv_includes_release_gate_metrics() {
    let run = LargeScaleRunSummary {
        records: 1_000_000,
        dimensions: 16,
        segment_max_vectors: 128,
        max_segments: 512,
        routing_page_overfetch: 8,
        max_candidates_per_segment: 128,
        pre_segments: 7_813,
        post_segments: 7_813,
        ingest_ms: 142_000,
        compaction_ms: 93_200,
        exact_ms: 6_890,
        compaction_bytes_read: 14_460_000,
        compaction_bytes_written: 18_880_000,
    };
    let queries = vec![LargeScaleQuerySummary {
        mode: "pq-scan".to_string(),
        tie_aware_recall_at_10: 1.0,
        id_recall_at_10: 1.0,
        termination_reason: "max-segments".to_string(),
        query_ms: 22,
        segments_searched: 512,
        bytes_read: 14_460_000,
        graph_bytes_read: 0,
        routing_page_indexes_read: 1,
        routing_pages_read: 8,
        resident_bytes: 61_000,
        records_considered: 65_536,
        records_scored: 65_536,
        graph_candidates_added: 0,
    }];

    let csv = large_scale_csv(&run, &queries);

    assert!(csv.starts_with("records,dimensions,segment_max_vectors,max_segments,routing_page_overfetch,max_candidates_per_segment,pre_segments,post_segments,ingest_ms,compaction_ms,exact_ms,compaction_bytes_read,compaction_bytes_written,mode,tie_aware_recall_at_10,id_recall_at_10,termination_reason,query_ms,segments_searched,bytes_read,graph_bytes_read,routing_page_indexes_read,routing_pages_read,resident_bytes,records_considered,records_scored,graph_candidates_added\n"));
    assert!(csv.contains("\n1000000,16,128,512,8,128,7813,7813,142000,93200,6890,14460000,18880000,pq-scan,1.000000,1.000000,max-segments,22,512,14460000,0,1,8,61000,65536,65536,0\n"));
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
    let routing_page_overfetch = env_usize(
        "BORSUK_LARGE_SCALE_ROUTING_PAGE_OVERFETCH",
        DEFAULT_ROUTING_PAGE_OVERFETCH,
    );
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
    let ingest_ms = ingest_started.elapsed().as_millis();

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
    let compaction_ms = compaction_started.elapsed().as_millis();

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
    let exact_ms = exact_started.elapsed().as_millis();

    let modes = [
        (LeafMode::PqScan, false),
        (LeafMode::VamanaPq, true),
        (LeafMode::Hybrid, true),
    ];
    let mut query_summaries = Vec::new();
    for (leaf_mode, expect_graph_reads) in modes {
        let approx_started = Instant::now();
        let approx = index
            .search_with_report(
                &query,
                SearchOptions::approx(10, leaf_mode)
                    .with_max_segments(max_segments)
                    .with_routing_page_overfetch(routing_page_overfetch)
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
        let query_ms = approx_started.elapsed().as_millis();
        let tie_aware_recall_at_10 = tie_aware_recall_at_k(
            &hit_distances(&exact.hits),
            &hit_distances(&approx.hits),
            10,
        )
        .unwrap();
        let id_recall_at_10 =
            recall_at_k(&hit_ids(&exact.hits), &hit_ids(&approx.hits), 10).expect("id recall");

        eprintln!(
            "large_scale_query mode={} tie_recall={:.3} id_recall={:.3} query_ms={} segments={} bytes={} graph_bytes={} routing_indexes={} routing_pages={} resident_bytes={}",
            approx.leaf_mode,
            tie_aware_recall_at_10,
            id_recall_at_10,
            query_ms,
            approx.segments_searched,
            approx.bytes_read,
            approx.graph_bytes_read,
            approx.routing_page_indexes_read,
            approx.routing_pages_read,
            approx.resident_bytes_estimate,
        );
        query_summaries.push(LargeScaleQuerySummary {
            mode: approx.leaf_mode.clone(),
            tie_aware_recall_at_10,
            id_recall_at_10,
            termination_reason: approx.termination_reason.to_string(),
            query_ms,
            segments_searched: approx.segments_searched,
            bytes_read: approx.bytes_read,
            graph_bytes_read: approx.graph_bytes_read,
            routing_page_indexes_read: approx.routing_page_indexes_read,
            routing_pages_read: approx.routing_pages_read,
            resident_bytes: approx.resident_bytes_estimate,
            records_considered: approx.records_considered,
            records_scored: approx.records_scored,
            graph_candidates_added: approx.graph_candidates_added,
        });
    }

    let run_summary = LargeScaleRunSummary {
        records: stats.records,
        dimensions: stats.dimensions,
        segment_max_vectors,
        max_segments,
        routing_page_overfetch,
        max_candidates_per_segment,
        pre_segments: stats.segments,
        post_segments: compacted_stats.segments,
        ingest_ms,
        compaction_ms,
        exact_ms,
        compaction_bytes_read: compaction.bytes_read,
        compaction_bytes_written: compaction.bytes_written,
    };

    eprintln!(
        "large_scale records={} dimensions={} pre_segments={} post_segments={} ingest_ms={} compaction_ms={} exact_ms={} compaction_bytes_read={} compaction_bytes_written={} resident_bytes={}",
        run_summary.records,
        run_summary.dimensions,
        run_summary.pre_segments,
        run_summary.post_segments,
        run_summary.ingest_ms,
        run_summary.compaction_ms,
        run_summary.exact_ms,
        run_summary.compaction_bytes_read,
        run_summary.compaction_bytes_written,
        exact.resident_bytes_estimate,
    );

    if let Ok(output_path) = env::var("BORSUK_LARGE_SCALE_OUTPUT") {
        write_large_scale_csv(Path::new(&output_path), &run_summary, &query_summaries).unwrap();
    }
}

struct LargeScaleRunSummary {
    records: usize,
    dimensions: usize,
    segment_max_vectors: usize,
    max_segments: usize,
    routing_page_overfetch: usize,
    max_candidates_per_segment: usize,
    pre_segments: usize,
    post_segments: usize,
    ingest_ms: u128,
    compaction_ms: u128,
    exact_ms: u128,
    compaction_bytes_read: u64,
    compaction_bytes_written: u64,
}

struct LargeScaleQuerySummary {
    mode: String,
    tie_aware_recall_at_10: f32,
    id_recall_at_10: f32,
    termination_reason: String,
    query_ms: u128,
    segments_searched: usize,
    bytes_read: u64,
    graph_bytes_read: u64,
    routing_page_indexes_read: usize,
    routing_pages_read: usize,
    resident_bytes: u64,
    records_considered: usize,
    records_scored: usize,
    graph_candidates_added: usize,
}

fn write_large_scale_csv(
    path: &Path,
    run: &LargeScaleRunSummary,
    queries: &[LargeScaleQuerySummary],
) -> std::io::Result<()> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent)?;
    }
    fs::write(path, large_scale_csv(run, queries))
}

fn large_scale_csv(run: &LargeScaleRunSummary, queries: &[LargeScaleQuerySummary]) -> String {
    let mut csv = String::from(
        "records,dimensions,segment_max_vectors,max_segments,routing_page_overfetch,max_candidates_per_segment,pre_segments,post_segments,ingest_ms,compaction_ms,exact_ms,compaction_bytes_read,compaction_bytes_written,mode,tie_aware_recall_at_10,id_recall_at_10,termination_reason,query_ms,segments_searched,bytes_read,graph_bytes_read,routing_page_indexes_read,routing_pages_read,resident_bytes,records_considered,records_scored,graph_candidates_added\n",
    );
    for query in queries {
        csv.push_str(&format!(
            "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{:.6},{:.6},{},{},{},{},{},{},{},{},{},{},{}\n",
            run.records,
            run.dimensions,
            run.segment_max_vectors,
            run.max_segments,
            run.routing_page_overfetch,
            run.max_candidates_per_segment,
            run.pre_segments,
            run.post_segments,
            run.ingest_ms,
            run.compaction_ms,
            run.exact_ms,
            run.compaction_bytes_read,
            run.compaction_bytes_written,
            query.mode,
            query.tie_aware_recall_at_10,
            query.id_recall_at_10,
            query.termination_reason,
            query.query_ms,
            query.segments_searched,
            query.bytes_read,
            query.graph_bytes_read,
            query.routing_page_indexes_read,
            query.routing_pages_read,
            query.resident_bytes,
            query.records_considered,
            query.records_scored,
            query.graph_candidates_added,
        ));
    }
    csv
}

fn assert_high_recall_report(
    exact_hits: &[SearchHit],
    report: &SearchReport,
    min_tie_aware_recall: f32,
    max_segments: usize,
    max_resident_bytes: u64,
    expect_graph_reads: bool,
) {
    let recall =
        tie_aware_recall_at_k(&hit_distances(exact_hits), &hit_distances(&report.hits), 10)
            .unwrap();
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

fn hit_distances(hits: &[SearchHit]) -> Vec<f32> {
    hits.iter().map(|hit| hit.distance).collect()
}

fn hit_ids(hits: &[SearchHit]) -> Vec<String> {
    hits.iter().map(|hit| hit.id.to_string()).collect()
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
