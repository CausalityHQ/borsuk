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

#[derive(Debug, Clone, Copy)]
enum Dataset {
    Uniform,
    Clustered,
    Adversarial,
}

impl Dataset {
    fn vector(self, seed: usize, dimensions: usize) -> Vec<f32> {
        match self {
            Self::Uniform => deterministic_vector(seed, dimensions),
            Self::Clustered => clustered_vector(seed, dimensions),
            Self::Adversarial => adversarial_vector(seed, dimensions),
        }
    }
}

fn deterministic_vector(seed: usize, dimensions: usize) -> Vec<f32> {
    (0..dimensions)
        .map(|dim| {
            let value = (seed.wrapping_mul(31) + dim.wrapping_mul(17)) % 997;
            value as f32 / 997.0
        })
        .collect()
}

fn clustered_vector(seed: usize, dimensions: usize) -> Vec<f32> {
    let cluster = seed % 16;
    (0..dimensions)
        .map(|dim| {
            let center = if dim % 16 == cluster { 8.0 } else { 0.0 };
            let jitter = (seed.wrapping_mul(37) + dim.wrapping_mul(19)) % 101;
            center + (jitter as f32 - 50.0) / 500.0
        })
        .collect()
}

fn adversarial_vector(seed: usize, dimensions: usize) -> Vec<f32> {
    (0..dimensions)
        .map(|dim| {
            let sign = if (seed + dim).is_multiple_of(2) {
                1.0
            } else {
                -1.0
            };
            let perturbation = (seed.wrapping_mul(13) + dim.wrapping_mul(7)) % 17;
            sign + perturbation as f32 / 10_000.0
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
    build_index_with_dataset(record_count, dimensions, Dataset::Uniform)
}

fn build_index_with_dataset(
    record_count: usize,
    dimensions: usize,
    dataset: Dataset,
) -> BuiltIndex {
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
        .map(|idx| VectorRecord::new(format!("doc-{idx}"), dataset.vector(idx, dimensions)))
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

fn bench_clustered_approx_report(c: &mut Criterion) {
    bench_dataset_approx_report(
        c,
        "local_clustered_approx_report_10k_x_64",
        Dataset::Clustered,
        42,
    );
}

fn bench_adversarial_approx_report(c: &mut Criterion) {
    bench_dataset_approx_report(
        c,
        "local_adversarial_approx_report_10k_x_64",
        Dataset::Adversarial,
        0,
    );
}

fn bench_dataset_approx_report(
    c: &mut Criterion,
    bench_name: &'static str,
    dataset: Dataset,
    query_seed: usize,
) {
    let built = build_index_with_dataset(10_000, 64, dataset);
    let query = dataset.vector(query_seed, 64);
    let exact = built
        .index
        .search_with_report(&query, SearchOptions::exact(10))
        .expect("exact search");
    assert_approx_report(&built.index, &query, &exact, false);

    c.bench_function(bench_name, |b| {
        b.iter(|| {
            built
                .index
                .search_with_report(&query, approx_options())
                .expect("approx search")
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
    bench_warm_cache_approx_report,
    bench_clustered_approx_report,
    bench_adversarial_approx_report
);
criterion_main!(benches);
