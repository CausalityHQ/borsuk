#![allow(missing_docs)]

use borsuk::{BorsukIndex, IndexConfig, OpenOptions, SearchOptions, VectorMetric, VectorRecord};

fn build_index(segment_max_vectors: usize, record_count: usize) -> (tempfile::TempDir, String) {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();
    let mut index = BorsukIndex::create(IndexConfig {
        uri: uri.clone(),
        metric: VectorMetric::Euclidean,
        dimensions: 2,
        segment_max_vectors,
        ram_budget_bytes: None,
        text: false,
        named_vectors: Default::default(),
    })
    .unwrap();
    index
        .add(
            (0..record_count)
                .map(|id| VectorRecord::new(format!("v{id}"), vec![id as f32, 0.0]))
                .collect(),
        )
        .unwrap();
    assert!(index.stats().segments > 1);
    drop(index);
    (dir, uri)
}

#[test]
fn preload_serves_repeat_searches_from_ram_without_changing_results() {
    let (_dir, uri) = build_index(3, 12);
    let query = [5.25, 0.0];
    let options = SearchOptions::exact(12);

    let cold = BorsukIndex::open(&uri).unwrap();
    let cold_report = cold.search_with_report(&query, options.clone()).unwrap();
    assert!(
        cold_report.requests.gets > 0,
        "a non-preloaded search must fetch segment payloads"
    );

    let preloaded = BorsukIndex::open_with_options(
        &uri,
        OpenOptions {
            preload: true,
            ..OpenOptions::default()
        },
    )
    .unwrap();
    let first = preloaded
        .search_with_report(&query, options.clone())
        .unwrap();
    let second = preloaded.search_with_report(&query, options).unwrap();

    let cold_ids = cold_report
        .hits
        .iter()
        .map(|hit| hit.id.clone())
        .collect::<Vec<_>>();
    let first_ids = first
        .hits
        .iter()
        .map(|hit| hit.id.clone())
        .collect::<Vec<_>>();
    let second_ids = second
        .hits
        .iter()
        .map(|hit| hit.id.clone())
        .collect::<Vec<_>>();
    assert_eq!(first_ids, cold_ids);
    assert_eq!(second_ids, cold_ids);
    assert_eq!(first.requests.gets, 0);
    assert_eq!(second.requests.gets, 0);

    let already_warm = preloaded.warm().unwrap();
    assert_eq!(already_warm.segments_loaded, 0);
    assert!(already_warm.bytes_resident > 0);
}

#[test]
fn warm_reports_loaded_segments_and_is_idempotent() {
    let (_dir, uri) = build_index(2, 10);
    let index = BorsukIndex::open(&uri).unwrap();
    let active_segments = index.stats().segments;

    let first = index.warm().unwrap();
    let second = index.warm().unwrap();

    assert_eq!(first.segments_loaded, active_segments);
    assert!(first.bytes_resident > 0);
    assert_eq!(second.segments_loaded, 0);
    assert_eq!(second.bytes_resident, first.bytes_resident);
}

#[test]
fn warm_resolves_paged_routing_and_keeps_it_resident() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();
    let mut index = BorsukIndex::create_with_routing_page_fanout(
        IndexConfig {
            uri: uri.clone(),
            metric: VectorMetric::Euclidean,
            dimensions: 2,
            segment_max_vectors: 1,
            ram_budget_bytes: None,
            text: false,
            named_vectors: Default::default(),
        },
        4,
    )
    .unwrap();
    index
        .add(
            (0..24)
                .map(|id| VectorRecord::new(format!("v{id}"), vec![id as f32, 0.0]))
                .collect(),
        )
        .unwrap();
    drop(index);

    let paged = BorsukIndex::open(&uri).unwrap();
    assert!(paged.manifest().segments.is_empty());
    let active_segments = paged.stats().segments;
    assert_eq!(active_segments, 24);

    let first = paged.warm().unwrap();
    let second = paged.warm().unwrap();
    assert_eq!(first.segments_loaded, active_segments);
    assert!(first.bytes_resident > 0);
    assert_eq!(second.segments_loaded, 0);
    assert_eq!(second.bytes_resident, first.bytes_resident);

    let report = paged
        .search_with_report(&[11.5, 0.0], SearchOptions::exact(active_segments))
        .unwrap();
    assert_eq!(report.hits.len(), active_segments);
    assert_eq!(report.routing_page_indexes_read, 0);
    assert_eq!(report.routing_pages_read, 0);
    assert_eq!(report.requests.gets, 0);
}
