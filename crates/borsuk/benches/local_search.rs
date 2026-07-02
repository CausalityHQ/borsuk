#![allow(missing_docs)]

use borsuk::{BorsukIndex, IndexConfig, SearchOptions, VectorMetric, VectorRecord};
use criterion::{Criterion, criterion_group, criterion_main};

fn deterministic_vector(seed: usize, dimensions: usize) -> Vec<f32> {
    (0..dimensions)
        .map(|dim| {
            let value = (seed.wrapping_mul(31) + dim.wrapping_mul(17)) % 997;
            value as f32 / 997.0
        })
        .collect()
}

fn build_index(record_count: usize, dimensions: usize) -> (tempfile::TempDir, BorsukIndex) {
    let dir = tempfile::tempdir().expect("temp dir");
    let mut index = BorsukIndex::create(IndexConfig {
        uri: format!("file://{}", dir.path().display()),
        metric: VectorMetric::Euclidean,
        dimensions,
        segment_max_vectors: 256,
    })
    .expect("create index");

    let records = (0..record_count)
        .map(|idx| VectorRecord::new(format!("doc-{idx}"), deterministic_vector(idx, dimensions)))
        .collect::<Vec<_>>();
    index.add(records).expect("insert vectors");
    (dir, index)
}

fn bench_exact_search(c: &mut Criterion) {
    let (_dir, index) = build_index(10_000, 64);
    let query = deterministic_vector(42, 64);

    c.bench_function("local_exact_search_10k_x_64", |b| {
        b.iter(|| {
            index
                .search(&query, SearchOptions::exact(10))
                .expect("search")
        });
    });
}

criterion_group!(benches, bench_exact_search);
criterion_main!(benches);
