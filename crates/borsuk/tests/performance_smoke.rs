#![allow(missing_docs)]

use std::time::Duration;

use borsuk::{BorsukIndex, IndexConfig, SearchOptions, VectorMetric, VectorRecord};

#[test]
fn local_exact_search_10k_x_64_stays_subsecond() {
    let dir = tempfile::tempdir().unwrap();
    let uri = format!("file://{}", dir.path().display());
    let mut index = BorsukIndex::create(IndexConfig {
        uri,
        metric: VectorMetric::Euclidean,
        dimensions: 64,
        segment_max_vectors: 256,
    })
    .unwrap();

    let records = (0..10_000)
        .map(|idx| VectorRecord::new(format!("doc-{idx}"), deterministic_vector(idx, 64)))
        .collect::<Vec<_>>();
    index.add(records).unwrap();

    let query = deterministic_vector(42, 64);
    let report = index
        .search_with_report(&query, SearchOptions::exact(10))
        .unwrap();

    assert_eq!(report.hits[0].id, "doc-42");
    assert!(report.segments_total > 1);
    assert!(report.segments_searched <= report.segments_total);
    assert!(report.bytes_read > 0);
    assert!(
        Duration::from_millis(report.elapsed_ms) < Duration::from_secs(1),
        "local exact search took {} ms",
        report.elapsed_ms
    );
}

fn deterministic_vector(seed: usize, dimensions: usize) -> Vec<f32> {
    (0..dimensions)
        .map(|dim| seed as f32 + dim as f32 / dimensions as f32)
        .collect()
}
