#![allow(missing_docs)]

use std::time::Duration;

use borsuk::{
    BorsukIndex, IndexConfig, LeafMode, SearchHit, SearchMode, SearchOptions, VectorMetric,
    VectorRecord, recall_at_k,
};

#[test]
fn approx_report_accepts_equal_distance_hits_with_different_ids() {
    let exact_report = synthetic_report(
        "flat-scan",
        (0..10)
            .map(|idx| SearchHit {
                id: format!("exact-{idx}").into(),
                distance: 0.0,
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

fn approx_options(leaf_mode: LeafMode) -> SearchOptions {
    SearchOptions {
        k: 10,
        mode: SearchMode::Approx {
            leaf_mode,
            eps: None,
            max_segments: Some(8),
            max_bytes: None,
            max_latency_ms: None,
            max_candidates_per_segment: Some(64),
        },
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
    let tie_aware_recall = tie_aware_recall_at_k(&exact_report.hits, &approx_report.hits, 10);
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

fn synthetic_report(
    leaf_mode: impl Into<String>,
    hits: Vec<SearchHit>,
    graph_bytes_read: u64,
) -> borsuk::SearchReport {
    borsuk::SearchReport {
        hits,
        leaf_mode: leaf_mode.into(),
        termination_reason: borsuk::SearchTerminationReason::Complete,
        segments_total: 16,
        segments_searched: 8,
        segments_skipped: 8,
        routing_page_indexes_read: 0,
        routing_pages_read: 0,
        bytes_read: 1,
        graph_bytes_read,
        object_cache_hits: 0,
        object_cache_misses: 1,
        records_considered: 128,
        records_scored: 64,
        graph_candidates_added: usize::from(graph_bytes_read > 0),
        resident_bytes_estimate: 1,
        elapsed_ms: 0,
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
