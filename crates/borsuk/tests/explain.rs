#![allow(missing_docs)]

//! Query cost/explain planner: `explain` runs a search and reports its plan and
//! estimated object-storage cost — requests, bytes, routing pruning, cache
//! effectiveness, latency, and a dollar estimate.

use borsuk::{BorsukIndex, IndexConfig, QueryCostModel, SearchOptions, VectorMetric, VectorRecord};

fn config(uri: String) -> IndexConfig {
    IndexConfig {
        uri,
        metric: VectorMetric::Euclidean,
        dimensions: 2,
        segment_max_vectors: 2,
        ram_budget_bytes: None,
        text: false,
        named_vectors: Default::default(),
    }
}

#[test]
fn explain_reports_plan_and_cost_consistent_with_search() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().to_string();
    let mut index = BorsukIndex::create(config(uri)).unwrap();
    index
        .add(
            (0..20)
                .map(|i| VectorRecord::new(format!("r{i}"), vec![i as f32, 0.0]))
                .collect(),
        )
        .unwrap();
    index.compact(borsuk::CompactionOptions::default()).unwrap();

    let explained = index
        .explain(
            &[0.0, 0.0],
            SearchOptions::exact(3),
            QueryCostModel::default(),
        )
        .unwrap();

    // The plan mirrors the underlying search.
    let ids: Vec<String> = index
        .search_ids(&[0.0, 0.0], SearchOptions::exact(3))
        .unwrap();
    let explained_ids: Vec<String> = explained.hits.iter().map(|h| h.id.to_string()).collect();
    assert_eq!(explained_ids, ids);
    assert_eq!(explained.hits.len(), 3);

    // Routing pruning is accounted for: not every segment is read.
    assert!(explained.segments_total >= explained.segments_searched);
    assert_eq!(
        explained.segments_searched + explained.segments_skipped,
        explained.report.segments_searched + explained.report.segments_skipped
    );

    // Cost accounting: default S3 pricing is $0.40 / 1M GET, no egress, so the
    // estimate equals get_requests * 0.40 / 1e6 and is non-negative.
    assert!(explained.get_requests > 0);
    let expected = explained.get_requests as f64 / 1_000_000.0 * 0.40;
    assert!((explained.estimated_cost_usd - expected).abs() < 1e-12);
    assert!((0.0..=1.0).contains(&explained.cache_hit_ratio));
}

#[test]
fn cost_model_scales_requests_and_egress() {
    // 2M GET at $0.40/M = $0.80; 4 GiB egress at $0.09/GiB = $0.36.
    let model = QueryCostModel {
        request_price_per_million: 0.40,
        data_price_per_gib: 0.09,
    };
    let usd = model.estimate_usd(2_000_000, 4 * 1024 * 1024 * 1024);
    assert!((usd - (0.80 + 0.36)).abs() < 1e-9, "{usd}");

    // The default model charges nothing for same-region data transfer.
    let default_usd = QueryCostModel::default().estimate_usd(1_000_000, 10 * 1024 * 1024 * 1024);
    assert!((default_usd - 0.40).abs() < 1e-9, "{default_usd}");
}
