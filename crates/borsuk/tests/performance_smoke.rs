#![allow(missing_docs)]

use std::time::{Duration, Instant};

use borsuk::{
    BorsukIndex, IndexConfig, LeafMode, SearchHit, SearchMode, SearchOptions, VectorMetric,
    VectorRecord, recall_at_k, tie_aware_recall_at_k,
};

#[test]
fn approx_report_accepts_equal_distance_hits_with_different_ids() {
    let exact_report = synthetic_report(
        "flat-scan",
        (0..10)
            .map(|idx| SearchHit {
                id: format!("exact-{idx}").into(),
                distance: 0.0,
                metadata: None,
            })
            .collect(),
        0,
    );
    let approx_report = synthetic_report(
        "pq-scan",
        (0..10)
            .map(|idx| SearchHit {
                id: format!("equivalent-{idx}").into(),
                distance: 0.0,
                metadata: None,
            })
            .collect(),
        0,
    );

    assert_approx_report(&exact_report, &approx_report, "pq-scan", false);
}

#[test]
fn local_exact_and_approx_search_10k_x_64_stay_subsecond() {
    let (_dir, index) = build_index();
    let query = deterministic_vector(42, 64);
    let exact_report = index
        .search_with_report(&query, SearchOptions::exact(10))
        .unwrap();

    assert_eq!(exact_report.hits[0].id, "doc-42");
    assert_eq!(exact_report.leaf_mode, "flat-scan");
    assert!(exact_report.segments_total > 1);
    assert!(exact_report.segments_searched <= exact_report.segments_total);
    assert!(exact_report.bytes_read > 0);
    assert_eq!(exact_report.graph_bytes_read, 0);
    assert_eq!(exact_report.object_cache_hits, 0);
    assert!(exact_report.object_cache_misses > 0);
    assert!(exact_report.resident_bytes_estimate > 0);
    assert!(
        Duration::from_millis(exact_report.elapsed_ms) < Duration::from_secs(1),
        "local exact search took {} ms",
        exact_report.elapsed_ms
    );

    let graph_report = index
        .search_with_report(&query, approx_options(LeafMode::Graph))
        .unwrap();
    assert_approx_report(&exact_report, &graph_report, "graph", true);

    let vamana_pq_report = index
        .search_with_report(&query, approx_options(LeafMode::VamanaPq))
        .unwrap();
    assert_approx_report(&exact_report, &vamana_pq_report, "vamana-pq", true);

    let hybrid_report = index
        .search_with_report(&query, approx_options(LeafMode::Hybrid))
        .unwrap();
    assert_approx_report(&exact_report, &hybrid_report, "hybrid", true);

    let flat_report = index
        .search_with_report(&query, approx_options(LeafMode::FlatScan))
        .unwrap();
    assert_approx_report(&exact_report, &flat_report, "flat-scan", false);

    let sq_report = index
        .search_with_report(&query, approx_options(LeafMode::SqScan))
        .unwrap();
    assert_approx_report(&exact_report, &sq_report, "sq-scan", false);

    let pq_report = index
        .search_with_report(&query, approx_options(LeafMode::PqScan))
        .unwrap();
    assert_approx_report(&exact_report, &pq_report, "pq-scan", false);
}

/// Insertion latency is on the hot write path, so guard it against regressions.
/// We measure per-call `add()` latency for single-record and batched appends,
/// print the percentiles (visible under `--nocapture`), and assert generous
/// ceilings so the check stays stable on an unoptimized debug build.
///
/// Note the shape this surfaces: every `add()` publishes a new immutable segment
/// plus a fresh manifest, so a single-record append pays that whole fixed cost —
/// which is why one record costs about as much as a hundred. BORSUK is a
/// batch-oriented writer; amortized per-record latency drops sharply with batch
/// size. The ceilings here are deliberately loose regression guards, not SLAs.
#[test]
fn insert_latency_stays_bounded() {
    let dimensions = 64;
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();
    let mut index = BorsukIndex::create(IndexConfig {
        uri,
        metric: VectorMetric::Euclidean,
        dimensions,
        segment_max_vectors: 256,
        ram_budget_bytes: None,
        sparse: false,
        text: false,
    })
    .unwrap();

    // Warm the index so appends run against a non-trivial routing tree.
    let warm = 2_000;
    index
        .add(
            (0..warm)
                .map(|idx| {
                    VectorRecord::new(format!("doc-{idx}"), deterministic_vector(idx, dimensions))
                })
                .collect::<Vec<_>>(),
        )
        .unwrap();

    // Single-record inserts: unique appends. Each pays a full manifest publish,
    // so keep the count modest — this loop dominates the test's wall-clock.
    let single_count = 40;
    let mut single = Vec::with_capacity(single_count);
    for seed in warm..warm + single_count {
        let record = VectorRecord::new(
            format!("ins-{seed}"),
            deterministic_vector(seed, dimensions),
        );
        let start = Instant::now();
        index.add(vec![record]).unwrap();
        single.push(start.elapsed());
    }
    let single_p50 = percentile(&mut single, 0.50);
    let single_p95 = percentile(&mut single, 0.95);
    let single_p99 = percentile(&mut single, 0.99);
    println!(
        "single-record insert latency: p50={:.2}ms p95={:.2}ms p99={:.2}ms",
        single_p50.as_secs_f64() * 1e3,
        single_p95.as_secs_f64() * 1e3,
        single_p99.as_secs_f64() * 1e3,
    );

    // Batched inserts: appends of 100 records each — same fixed publish cost
    // amortized over the batch.
    let batch_count = 6;
    let mut batched = Vec::with_capacity(batch_count);
    let mut next = warm + single_count;
    for _ in 0..batch_count {
        let records = (next..next + 100)
            .map(|seed| {
                VectorRecord::new(
                    format!("ins-{seed}"),
                    deterministic_vector(seed, dimensions),
                )
            })
            .collect::<Vec<_>>();
        next += 100;
        let start = Instant::now();
        index.add(records).unwrap();
        batched.push(start.elapsed());
    }
    let batch_p50 = percentile(&mut batched, 0.50);
    let batch_p95 = percentile(&mut batched, 0.95);
    println!(
        "batch-of-100 insert latency: p50={:.2}ms p95={:.2}ms",
        batch_p50.as_secs_f64() * 1e3,
        batch_p95.as_secs_f64() * 1e3,
    );

    // Generous ceilings: a regression guard, not a tight SLA. A single append on
    // a fast local disk publishes a manifest in a few hundred ms; these bounds
    // leave wide margin for slower CI storage while still catching a blow-up.
    assert!(
        single_p95 < Duration::from_millis(2500),
        "single-record insert p95 was {:.2}ms",
        single_p95.as_secs_f64() * 1e3,
    );
    assert!(
        batch_p95 < Duration::from_secs(6),
        "batch-of-100 insert p95 was {:.2}ms",
        batch_p95.as_secs_f64() * 1e3,
    );
}

/// Nearest-rank percentile of a set of durations (sorts in place).
fn percentile(samples: &mut [Duration], quantile: f64) -> Duration {
    assert!(!samples.is_empty(), "percentile of empty sample set");
    samples.sort_unstable();
    let rank = (quantile * (samples.len() as f64 - 1.0)).round() as usize;
    samples[rank.min(samples.len() - 1)]
}

fn approx_options(leaf_mode: LeafMode) -> SearchOptions {
    SearchOptions {
        k: 10,
        mode: SearchMode::Approx {
            leaf_mode,
            eps: None,
            max_segments: Some(8),
            max_bytes: None,
            max_latency_ms: None,
            routing_page_overfetch: None,
            max_candidates_per_segment: Some(64),
        },
        guaranteed_recall: false,
        prefetch_depth: borsuk::DEFAULT_SEARCH_PREFETCH_DEPTH,
        filter: None,
        include_metadata: false,
    }
}

fn build_index() -> (tempfile::TempDir, BorsukIndex) {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();
    let mut index = BorsukIndex::create(IndexConfig {
        uri,
        metric: VectorMetric::Euclidean,
        dimensions: 64,
        segment_max_vectors: 256,
        ram_budget_bytes: None,
        sparse: false,
        text: false,
    })
    .unwrap();

    let records = (0..10_000)
        .map(|idx| VectorRecord::new(format!("doc-{idx}"), deterministic_vector(idx, 64)))
        .collect::<Vec<_>>();
    index.add(records).unwrap();
    (dir, index)
}

fn assert_approx_report(
    exact_report: &borsuk::SearchReport,
    approx_report: &borsuk::SearchReport,
    expected_leaf_mode: &str,
    expect_graph_reads: bool,
) {
    let exact_ids = exact_report
        .hits
        .iter()
        .map(|hit| hit.id.clone())
        .collect::<Vec<_>>();
    let approx_ids = approx_report
        .hits
        .iter()
        .map(|hit| hit.id.clone())
        .collect::<Vec<_>>();

    assert_eq!(approx_report.leaf_mode, expected_leaf_mode);
    let id_recall = recall_at_k(&exact_ids, &approx_ids, 10).unwrap();
    let tie_aware_recall = tie_aware_recall_at_k(
        &hit_distances(&exact_report.hits),
        &hit_distances(&approx_report.hits),
        10,
    )
    .unwrap();
    let min_recall = minimum_tie_aware_recall(expected_leaf_mode);
    assert!(
        tie_aware_recall >= min_recall,
        "{expected_leaf_mode} tie-aware recall was {tie_aware_recall}, id recall was {id_recall}; exact={exact_ids:?} approx={approx_ids:?}"
    );
    assert!(approx_report.segments_total > 1);
    assert!(approx_report.segments_searched <= approx_report.segments_total);
    assert!(approx_report.segments_searched <= 8);
    assert!(approx_report.segments_skipped > 0);
    assert!(approx_report.bytes_read > 0);
    assert!(approx_report.records_scored < approx_report.records_considered);
    assert!(approx_report.resident_bytes_estimate > 0);
    assert!(
        Duration::from_millis(approx_report.elapsed_ms) < Duration::from_secs(1),
        "local {expected_leaf_mode} approx search took {} ms",
        approx_report.elapsed_ms
    );
    if expect_graph_reads {
        assert!(approx_report.graph_bytes_read > 0);
        assert!(approx_report.graph_candidates_added > 0);
    } else {
        assert_eq!(approx_report.graph_bytes_read, 0);
        assert_eq!(approx_report.graph_candidates_added, 0);
    }
}

fn minimum_tie_aware_recall(leaf_mode: &str) -> f32 {
    match leaf_mode {
        "pq-scan" | "vamana-pq" | "hybrid" => 0.95,
        _ => 0.1,
    }
}

fn hit_distances(hits: &[SearchHit]) -> Vec<f32> {
    hits.iter().map(|hit| hit.distance).collect()
}

fn synthetic_report(
    leaf_mode: impl Into<String>,
    hits: Vec<SearchHit>,
    graph_bytes_read: u64,
) -> borsuk::SearchReport {
    borsuk::SearchReport {
        hits,
        leaf_mode: leaf_mode.into(),
        termination_reason: borsuk::SearchTerminationReason::Complete,
        recall_guarantee: borsuk::RecallGuarantee::Degraded,
        segments_total: 16,
        segments_searched: 8,
        segments_skipped: 8,
        routing_page_indexes_read: 0,
        routing_pages_read: 0,
        bytes_read: 1,
        prefetched_bytes_unused: 0,
        graph_bytes_read,
        object_cache_hits: 0,
        object_cache_misses: 1,
        cache_repairs: 0,
        records_considered: 128,
        records_scored: 64,
        graph_candidates_added: usize::from(graph_bytes_read > 0),
        resident_bytes_estimate: 1,
        elapsed_ms: 0,
        requests: Default::default(),
        rows_evaluated: 0,
        rows_passed_filter: 0,
        segments_pruned_by_filter: 0,
    }
}

fn deterministic_vector(seed: usize, dimensions: usize) -> Vec<f32> {
    (0..dimensions)
        .map(|dim| {
            if dim == 0 {
                seed as f32
            } else {
                dim as f32 / dimensions as f32
            }
        })
        .collect()
}
