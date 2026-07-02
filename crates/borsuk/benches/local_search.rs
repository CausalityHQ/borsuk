#![allow(missing_docs)]

use std::path::PathBuf;

use borsuk::{
    BorsukIndex, IndexConfig, SearchMode, SearchOptions, SearchReport, VectorMetric, VectorRecord,
    recall_at_k,
};
use criterion::{Criterion, criterion_group, criterion_main};

struct BuiltIndex {
    _dir: tempfile::TempDir,
    uri: String,
    index: BorsukIndex,
}

fn deterministic_vector(seed: usize, dimensions: usize) -> Vec<f32> {
    (0..dimensions)
        .map(|dim| {
            let value = (seed.wrapping_mul(31) + dim.wrapping_mul(17)) % 997;
            value as f32 / 997.0
        })
        .collect()
}

fn approx_options() -> SearchOptions {
    SearchOptions {
        k: 10,
        mode: SearchMode::Approx {
            eps: None,
            max_segments: None,
            max_bytes: None,
            max_latency_ms: None,
            max_candidates_per_segment: Some(64),
        },
    }
}

fn build_index(record_count: usize, dimensions: usize) -> BuiltIndex {
    let dir = tempfile::tempdir().expect("temp dir");
    let uri = format!("file://{}", dir.path().display());
    let mut index = BorsukIndex::create(IndexConfig {
        uri: uri.clone(),
        metric: VectorMetric::Euclidean,
        dimensions,
        segment_max_vectors: 256,
        ram_budget_bytes: None,
    })
    .expect("create index");

    let records = (0..record_count)
        .map(|idx| VectorRecord::new(format!("doc-{idx}"), deterministic_vector(idx, dimensions)))
        .collect::<Vec<_>>();
    index.add(records).expect("insert vectors");
    BuiltIndex {
        _dir: dir,
        uri,
        index,
    }
}

fn bench_exact_search(c: &mut Criterion) {
    let built = build_index(10_000, 64);
    let query = deterministic_vector(42, 64);

    c.bench_function("local_exact_search_10k_x_64", |b| {
        b.iter(|| {
            built
                .index
                .search(&query, SearchOptions::exact(10))
                .expect("search")
        });
    });
}

fn bench_approx_report(c: &mut Criterion) {
    let built = build_index(10_000, 64);
    let query = deterministic_vector(42, 64);
    let exact = built
        .index
        .search_with_report(&query, SearchOptions::exact(10))
        .expect("exact search");
    assert_approx_report(&built.index, &query, &exact, false);

    c.bench_function("local_approx_report_10k_x_64", |b| {
        b.iter(|| {
            built
                .index
                .search_with_report(&query, approx_options())
                .expect("approx search")
        });
    });
}

fn bench_warm_cache_approx_report(c: &mut Criterion) {
    let built = build_index(10_000, 64);
    let cache = tempfile::tempdir().expect("cache dir");
    let index =
        BorsukIndex::open_with_cache(&built.uri, Some(PathBuf::from(cache.path()))).expect("open");
    let query = deterministic_vector(42, 64);
    let exact = built
        .index
        .search_with_report(&query, SearchOptions::exact(10))
        .expect("exact search");

    assert_approx_report(&index, &query, &exact, false);
    assert_approx_report(&index, &query, &exact, true);

    c.bench_function("local_warm_cache_approx_report_10k_x_64", |b| {
        b.iter(|| {
            index
                .search_with_report(&query, approx_options())
                .expect("warm cache approx search")
        });
    });
}

fn assert_approx_report(index: &BorsukIndex, query: &[f32], exact: &SearchReport, warm: bool) {
    let report = index
        .search_with_report(query, approx_options())
        .expect("approx report");
    let exact_ids = exact
        .hits
        .iter()
        .map(|hit| hit.id.clone())
        .collect::<Vec<_>>();
    let approx_ids = report
        .hits
        .iter()
        .map(|hit| hit.id.clone())
        .collect::<Vec<_>>();

    assert!(recall_at_k(&exact_ids, &approx_ids, 10).expect("recall") >= 0.1);
    assert!(report.records_scored < report.records_considered);
    assert!(report.graph_bytes_read > 0);

    if warm {
        assert!(report.object_cache_hits > 0);
        assert_eq!(report.object_cache_misses, 0);
    }
}

criterion_group!(
    benches,
    bench_exact_search,
    bench_approx_report,
    bench_warm_cache_approx_report
);
criterion_main!(benches);
