#![allow(missing_docs)]

use std::fs;

use borsuk::{
    BorsukIndex, CompactionOptions, GarbageCollectionOptions, IndexConfig, OpenOptions, SearchMode,
    SearchOptions, VectorMetric, VectorRecord,
};

#[test]
fn local_index_persists_segments_and_reopens_for_exact_search() {
    let dir = tempfile::tempdir().unwrap();
    let uri = format!("file://{}", dir.path().display());

    let mut index = BorsukIndex::create(IndexConfig {
        uri: uri.clone(),
        metric: VectorMetric::Euclidean,
        dimensions: 2,
        segment_max_vectors: 2,
        ram_budget_bytes: None,
    })
    .unwrap();

    index
        .add(vec![
            VectorRecord::new("a", vec![0.0, 0.0]),
            VectorRecord::new("b", vec![1.0, 0.0]),
            VectorRecord::new("c", vec![9.0, 0.0]),
        ])
        .unwrap();
    assert_eq!(
        index.manifest().pivots.len(),
        index.manifest().segments.len(),
        "active manifest must keep pivot summaries resident with segment routing"
    );

    let hits = index
        .search(
            &[0.2, 0.0],
            SearchOptions {
                k: 2,
                mode: SearchMode::Exact,
            },
        )
        .unwrap();

    assert_eq!(
        hits.iter().map(|hit| hit.id.as_str()).collect::<Vec<_>>(),
        vec!["a", "b"]
    );
    assert!(dir.path().join("CURRENT").exists());
    assert!(dir.path().join("manifests").exists());
    assert!(
        fs::read_dir(dir.path().join("segments/L0"))
            .unwrap()
            .count()
            > 0
    );

    let reopened = BorsukIndex::open(&uri).unwrap();
    let reopened_hits = reopened
        .search(&[8.5, 0.0], SearchOptions::exact(1))
        .unwrap();
    assert_eq!(reopened_hits[0].id, "c");
}

#[test]
fn local_index_searches_query_batches() {
    let dir = tempfile::tempdir().unwrap();
    let uri = format!("file://{}", dir.path().display());

    let mut index = BorsukIndex::create(IndexConfig {
        uri,
        metric: VectorMetric::Euclidean,
        dimensions: 2,
        segment_max_vectors: 1,
        ram_budget_bytes: None,
    })
    .unwrap();

    index
        .add(vec![
            VectorRecord::new("left", vec![0.0, 0.0]),
            VectorRecord::new("middle", vec![5.0, 0.0]),
            VectorRecord::new("right", vec![10.0, 0.0]),
        ])
        .unwrap();

    let results = index
        .search_batch(&[vec![0.1, 0.0], vec![9.9, 0.0]], SearchOptions::exact(1))
        .unwrap();

    assert_eq!(results.len(), 2);
    assert_eq!(results[0][0].id, "left");
    assert_eq!(results[1][0].id, "right");
}

#[test]
fn local_index_reports_query_batches() {
    let dir = tempfile::tempdir().unwrap();
    let uri = format!("file://{}", dir.path().display());

    let mut index = BorsukIndex::create(IndexConfig {
        uri,
        metric: VectorMetric::Euclidean,
        dimensions: 2,
        segment_max_vectors: 1,
        ram_budget_bytes: None,
    })
    .unwrap();

    index
        .add(vec![
            VectorRecord::new("left", vec![0.0, 0.0]),
            VectorRecord::new("middle", vec![5.0, 0.0]),
            VectorRecord::new("right", vec![10.0, 0.0]),
        ])
        .unwrap();

    let reports = index
        .search_batch_with_report(&[vec![0.1, 0.0], vec![9.9, 0.0]], SearchOptions::exact(1))
        .unwrap();

    assert_eq!(reports.len(), 2);
    assert_eq!(reports[0].hits[0].id, "left");
    assert_eq!(reports[1].hits[0].id, "right");
    assert_eq!(reports[0].segments_total, 3);
    assert_eq!(reports[1].segments_total, 3);
    assert!(reports[0].bytes_read > 0);
    assert!(reports[1].bytes_read > 0);
    assert!(reports[0].resident_bytes_estimate > 0);
    assert!(reports[1].resident_bytes_estimate > 0);
}

#[test]
fn local_index_reports_manifest_stats_without_scanning_storage() {
    let dir = tempfile::tempdir().unwrap();
    let uri = format!("file://{}", dir.path().display());

    let mut index = BorsukIndex::create(IndexConfig {
        uri: uri.clone(),
        metric: VectorMetric::Euclidean,
        dimensions: 2,
        segment_max_vectors: 2,
        ram_budget_bytes: Some(1_000_000),
    })
    .unwrap();

    index
        .add(vec![
            VectorRecord::new("a", vec![0.0, 0.0]),
            VectorRecord::new("b", vec![1.0, 0.0]),
            VectorRecord::new("c", vec![10.0, 0.0]),
        ])
        .unwrap();

    let stats = index.stats();
    assert_eq!(stats.metric, "euclidean");
    assert_eq!(stats.dimensions, 2);
    assert_eq!(stats.segment_max_vectors, 2);
    assert_eq!(stats.ram_budget_bytes, Some(1_000_000));
    assert_eq!(stats.manifest_version, 2);
    assert_eq!(stats.segments, 2);
    assert_eq!(stats.records, 3);
    assert!(stats.segment_bytes > 0);
    assert!(stats.graph_bytes > 0);
    assert!(stats.resident_bytes_estimate > 0);

    let reopened = BorsukIndex::open_with_options(
        &uri,
        OpenOptions {
            ram_budget_bytes: Some(500_000),
            ..OpenOptions::default()
        },
    )
    .unwrap();
    assert_eq!(reopened.stats().ram_budget_bytes, Some(500_000));
}

#[test]
fn create_rejects_too_small_ram_budget() {
    let dir = tempfile::tempdir().unwrap();
    let uri = format!("file://{}", dir.path().display());

    let err = BorsukIndex::create(IndexConfig {
        uri,
        metric: VectorMetric::Euclidean,
        dimensions: 2,
        segment_max_vectors: 2,
        ram_budget_bytes: Some(1),
    })
    .unwrap_err();

    assert!(err.to_string().contains("RAM budget exceeded"));
}

#[test]
fn ram_budget_persists_through_manifest_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let uri = format!("file://{}", dir.path().display());

    let index = BorsukIndex::create(IndexConfig {
        uri: uri.clone(),
        metric: VectorMetric::Euclidean,
        dimensions: 2,
        segment_max_vectors: 2,
        ram_budget_bytes: Some(1_000_000),
    })
    .unwrap();
    assert_eq!(index.manifest().config.ram_budget_bytes, Some(1_000_000));

    let reopened = BorsukIndex::open(&uri).unwrap();
    assert_eq!(reopened.manifest().config.ram_budget_bytes, Some(1_000_000));
}

#[test]
fn open_options_reject_too_small_runtime_ram_budget() {
    let dir = tempfile::tempdir().unwrap();
    let uri = format!("file://{}", dir.path().display());

    BorsukIndex::create(IndexConfig {
        uri: uri.clone(),
        metric: VectorMetric::Euclidean,
        dimensions: 2,
        segment_max_vectors: 2,
        ram_budget_bytes: None,
    })
    .unwrap();

    let err = BorsukIndex::open_with_options(
        &uri,
        OpenOptions {
            ram_budget_bytes: Some(1),
            ..OpenOptions::default()
        },
    )
    .unwrap_err();

    assert!(err.to_string().contains("RAM budget exceeded"));
}

#[test]
fn local_index_uses_binary_current_and_parquet_tables() {
    let dir = tempfile::tempdir().unwrap();
    let uri = format!("file://{}", dir.path().display());

    let mut index = BorsukIndex::create(IndexConfig {
        uri: uri.clone(),
        metric: VectorMetric::Euclidean,
        dimensions: 2,
        segment_max_vectors: 2,
        ram_budget_bytes: None,
    })
    .unwrap();

    index
        .add(vec![
            VectorRecord::new("a", vec![0.0, 0.0]),
            VectorRecord::new("b", vec![1.0, 0.0]),
            VectorRecord::new("c", vec![9.0, 0.0]),
        ])
        .unwrap();

    let current = fs::read(dir.path().join("CURRENT")).unwrap();
    assert_eq!(&current[0..4], b"BORS");
    assert!(!String::from_utf8_lossy(&current).contains("manifest-"));

    let manifest_files = collect_files_with_extension(dir.path().join("manifests"), "parquet");
    let routing_files = collect_files_with_extension(dir.path().join("routing"), "parquet");
    let segment_files = collect_files_with_extension(dir.path().join("segments"), "parquet");
    let graph_files = collect_files_with_extension(dir.path().join("graphs"), "parquet");
    let segment_routing_files = collect_files_with_prefix(&routing_files, "segments-");
    let pivot_routing_files = collect_files_with_prefix(&routing_files, "pivots-");

    assert!(
        !manifest_files.is_empty(),
        "manifest tables must be parquet"
    );
    assert!(!routing_files.is_empty(), "routing tables must be parquet");
    assert!(
        !segment_routing_files.is_empty(),
        "segment-summary routing tables must be parquet"
    );
    assert!(
        !pivot_routing_files.is_empty(),
        "pivot routing tables must be parquet"
    );
    assert_eq!(segment_files.len(), 2, "segments must be parquet");
    assert_eq!(graph_files.len(), 2, "local graphs must be parquet");

    let cache = tempfile::tempdir().unwrap();
    let reopened = BorsukIndex::open_with_cache(&uri, Some(cache.path().to_path_buf())).unwrap();
    assert_eq!(
        reopened.manifest().pivots.len(),
        reopened.manifest().segments.len(),
        "open_with_cache must load pivot summaries into the active manifest"
    );
    let cached_routing_files =
        collect_files_with_extension(cache.path().join("routing"), "parquet");
    let cached_pivot_routing_files = collect_files_with_prefix(&cached_routing_files, "pivots-");
    assert!(
        !cached_pivot_routing_files.is_empty(),
        "open_with_cache must load the active pivot routing table"
    );

    for path in manifest_files
        .iter()
        .chain(routing_files.iter())
        .chain(segment_files.iter())
        .chain(graph_files.iter())
    {
        assert_is_parquet_file(path);
    }

    assert!(
        index.manifest().segments.iter().all(|segment| {
            segment.graph_path.starts_with("graphs/L0/")
                && segment.graph_checksum.len() == 64
                && segment.graph_size_bytes > 0
        }),
        "active segment summaries must reference graph parquet blocks"
    );

    assert!(
        collect_files_with_extension(dir.path(), "borsuk").is_empty(),
        "JSON .borsuk manifests are not durable storage"
    );
    assert!(
        collect_files_with_extension(dir.path(), "kseg").is_empty(),
        "custom .kseg segments are not durable storage"
    );
}

#[test]
fn current_rejects_valid_manifest_table_swapped_under_active_version() {
    let dir = tempfile::tempdir().unwrap();
    let uri = format!("file://{}", dir.path().display());

    let mut index = BorsukIndex::create(IndexConfig {
        uri: uri.clone(),
        metric: VectorMetric::Euclidean,
        dimensions: 2,
        segment_max_vectors: 2,
        ram_budget_bytes: None,
    })
    .unwrap();

    index
        .add(vec![
            VectorRecord::new("a", vec![0.0, 0.0]),
            VectorRecord::new("b", vec![1.0, 0.0]),
        ])
        .unwrap();

    let manifest_v1 = dir
        .path()
        .join("manifests/manifest-00000000000000000001.parquet");
    let manifest_v2 = dir
        .path()
        .join("manifests/manifest-00000000000000000002.parquet");
    fs::copy(&manifest_v1, &manifest_v2).unwrap();

    let err = BorsukIndex::open(&uri).unwrap_err();

    assert!(
        err.to_string()
            .contains("CURRENT metadata checksum mismatch"),
        "unexpected error: {err}"
    );
}

#[test]
fn segment_local_graph_blocks_reopen_and_compact_with_segments() {
    let dir = tempfile::tempdir().unwrap();
    let uri = format!("file://{}", dir.path().display());

    let mut index = BorsukIndex::create(IndexConfig {
        uri: uri.clone(),
        metric: VectorMetric::Euclidean,
        dimensions: 2,
        segment_max_vectors: 2,
        ram_budget_bytes: None,
    })
    .unwrap();

    index
        .add(vec![
            VectorRecord::new("a", vec![0.0, 0.0]),
            VectorRecord::new("b", vec![1.0, 0.0]),
            VectorRecord::new("c", vec![8.0, 0.0]),
            VectorRecord::new("d", vec![9.0, 0.0]),
        ])
        .unwrap();

    let l0_graphs = collect_files_with_extension(dir.path().join("graphs/L0"), "parquet");
    assert_eq!(l0_graphs.len(), 2);
    for graph in &l0_graphs {
        assert_is_parquet_file(graph);
    }
    assert!(
        index
            .manifest()
            .segments
            .iter()
            .all(|summary| summary.graph_path.starts_with("graphs/L0/"))
    );

    let reopened = BorsukIndex::open(&uri).unwrap();
    assert_eq!(
        reopened
            .manifest()
            .segments
            .iter()
            .map(|summary| summary.graph_path.as_str())
            .collect::<Vec<_>>(),
        index
            .manifest()
            .segments
            .iter()
            .map(|summary| summary.graph_path.as_str())
            .collect::<Vec<_>>()
    );

    index
        .compact(CompactionOptions {
            source_level: 0,
            target_level: 1,
            max_segments: Some(2),
            min_segments: 2,
            target_segment_max_vectors: Some(4),
        })
        .unwrap();

    let l1_graphs = collect_files_with_extension(dir.path().join("graphs/L1"), "parquet");
    assert_eq!(l1_graphs.len(), 1);
    assert_is_parquet_file(&l1_graphs[0]);
    assert!(
        index
            .manifest()
            .segments
            .iter()
            .all(|summary| summary.graph_path.starts_with("graphs/L1/"))
    );
}

#[test]
fn approximate_search_obeys_segment_budget() {
    let dir = tempfile::tempdir().unwrap();
    let uri = format!("file://{}", dir.path().display());

    let mut index = BorsukIndex::create(IndexConfig {
        uri,
        metric: VectorMetric::Euclidean,
        dimensions: 2,
        segment_max_vectors: 1,
        ram_budget_bytes: None,
    })
    .unwrap();

    index
        .add(vec![
            VectorRecord::new("near", vec![0.0, 0.0]),
            VectorRecord::new("far", vec![100.0, 0.0]),
        ])
        .unwrap();

    let hits = index
        .search(
            &[0.0, 0.0],
            SearchOptions {
                k: 2,
                mode: SearchMode::Approx {
                    eps: Some(0.05),
                    max_segments: Some(1),
                    max_bytes: None,
                    max_latency_ms: None,
                    max_candidates_per_segment: None,
                },
            },
        )
        .unwrap();

    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].id, "near");
}

#[test]
fn approximate_search_obeys_byte_budget() {
    let dir = tempfile::tempdir().unwrap();
    let uri = format!("file://{}", dir.path().display());

    let mut index = BorsukIndex::create(IndexConfig {
        uri,
        metric: VectorMetric::Euclidean,
        dimensions: 2,
        segment_max_vectors: 1,
        ram_budget_bytes: None,
    })
    .unwrap();

    index
        .add(vec![
            VectorRecord::new("near", vec![0.0, 0.0]),
            VectorRecord::new("mid", vec![10.0, 0.0]),
            VectorRecord::new("far", vec![20.0, 0.0]),
        ])
        .unwrap();

    let report = index
        .search_with_report(
            &[0.0, 0.0],
            SearchOptions {
                k: 3,
                mode: SearchMode::Approx {
                    eps: None,
                    max_segments: None,
                    max_bytes: Some(1),
                    max_latency_ms: None,
                    max_candidates_per_segment: None,
                },
            },
        )
        .unwrap();

    assert_eq!(report.hits.len(), 1);
    assert_eq!(report.hits[0].id, "near");
    assert_eq!(report.segments_searched, 1);
    assert_eq!(report.segments_skipped, 2);
    assert!(report.bytes_read > 1);
}

#[test]
fn approximate_search_limits_exact_scoring_inside_each_segment() {
    let dir = tempfile::tempdir().unwrap();
    let uri = format!("file://{}", dir.path().display());

    let mut index = BorsukIndex::create(IndexConfig {
        uri,
        metric: VectorMetric::Euclidean,
        dimensions: 1,
        segment_max_vectors: 4,
        ram_budget_bytes: None,
    })
    .unwrap();

    index
        .add(vec![
            VectorRecord::new("near", vec![0.0]),
            VectorRecord::new("next", vec![0.2]),
            VectorRecord::new("far-a", vec![10.0]),
            VectorRecord::new("far-b", vec![20.0]),
        ])
        .unwrap();

    let report = index
        .search_with_report(
            &[0.05],
            SearchOptions {
                k: 1,
                mode: SearchMode::Approx {
                    eps: None,
                    max_segments: None,
                    max_bytes: None,
                    max_latency_ms: None,
                    max_candidates_per_segment: Some(2),
                },
            },
        )
        .unwrap();

    assert_eq!(report.hits[0].id, "near");
    assert_eq!(report.segments_total, 1);
    assert_eq!(report.records_considered, 4);
    assert_eq!(report.records_scored, 2);
}

#[test]
fn approximate_search_expands_candidates_from_segment_graph() {
    let dir = tempfile::tempdir().unwrap();
    let uri = format!("file://{}", dir.path().display());

    let mut index = BorsukIndex::create(IndexConfig {
        uri,
        metric: VectorMetric::Euclidean,
        dimensions: 2,
        segment_max_vectors: 4,
        ram_budget_bytes: None,
    })
    .unwrap();

    index
        .add(vec![
            VectorRecord::new("entry", vec![0.0, 0.0]),
            VectorRecord::new("true-neighbor", vec![0.0, 0.1]),
            VectorRecord::new("routing-decoy", vec![0.1, -0.1]),
            VectorRecord::new("far", vec![100.0, 100.0]),
        ])
        .unwrap();

    let report = index
        .search_with_report(
            &[0.04, 0.07],
            SearchOptions {
                k: 1,
                mode: SearchMode::Approx {
                    eps: None,
                    max_segments: None,
                    max_bytes: None,
                    max_latency_ms: None,
                    max_candidates_per_segment: Some(2),
                },
            },
        )
        .unwrap();

    assert_eq!(report.hits[0].id, "true-neighbor");
    assert_eq!(report.records_considered, 4);
    assert_eq!(report.records_scored, 2);
    assert!(report.graph_bytes_read > 0);
    assert_eq!(report.graph_candidates_added, 1);
}

#[test]
fn approximate_search_walks_segment_graph_beyond_first_hop() {
    let dir = tempfile::tempdir().unwrap();
    let uri = format!("file://{}", dir.path().display());

    let mut index = BorsukIndex::create(IndexConfig {
        uri,
        metric: VectorMetric::Euclidean,
        dimensions: 2,
        segment_max_vectors: 10,
        ram_budget_bytes: None,
    })
    .unwrap();

    index
        .add(vec![
            VectorRecord::new("aa-entry", vec![0.0, 0.0]),
            VectorRecord::new("bb-hop", vec![1.0, 1.0]),
            VectorRecord::new("cc-decoy-0", vec![-1.0, -1.0]),
            VectorRecord::new("cc-decoy-1", vec![-1.1, -1.1]),
            VectorRecord::new("cc-decoy-2", vec![-1.2, -1.2]),
            VectorRecord::new("cc-decoy-3", vec![-1.3, -1.3]),
            VectorRecord::new("cc-decoy-4", vec![-1.4, -1.4]),
            VectorRecord::new("cc-decoy-5", vec![-1.5, -1.5]),
            VectorRecord::new("cc-decoy-6", vec![-1.6, -1.6]),
            VectorRecord::new("zz-target", vec![2.0, 2.0]),
        ])
        .unwrap();

    let report = index
        .search_with_report(
            &[2.0, 2.0],
            SearchOptions {
                k: 1,
                mode: SearchMode::Approx {
                    eps: None,
                    max_segments: None,
                    max_bytes: None,
                    max_latency_ms: None,
                    max_candidates_per_segment: Some(3),
                },
            },
        )
        .unwrap();

    assert_eq!(report.hits[0].id, "zz-target");
    assert_eq!(report.records_considered, 10);
    assert_eq!(report.records_scored, 3);
    assert_eq!(report.graph_candidates_added, 2);
}

#[test]
fn read_through_cache_serves_segment_and_graph_after_source_removal() {
    let dir = tempfile::tempdir().unwrap();
    let cache = tempfile::tempdir().unwrap();
    let uri = format!("file://{}", dir.path().display());

    let mut writer = BorsukIndex::create(IndexConfig {
        uri: uri.clone(),
        metric: VectorMetric::Euclidean,
        dimensions: 2,
        segment_max_vectors: 4,
        ram_budget_bytes: None,
    })
    .unwrap();

    writer
        .add(vec![
            VectorRecord::new("entry", vec![0.0, 0.0]),
            VectorRecord::new("true-neighbor", vec![0.0, 0.1]),
            VectorRecord::new("routing-decoy", vec![0.1, -0.1]),
            VectorRecord::new("far", vec![100.0, 100.0]),
        ])
        .unwrap();

    let index = BorsukIndex::open_with_cache(&uri, Some(cache.path().to_path_buf())).unwrap();
    let report = index
        .search_with_report(
            &[0.04, 0.07],
            SearchOptions {
                k: 1,
                mode: SearchMode::Approx {
                    eps: None,
                    max_segments: None,
                    max_bytes: None,
                    max_latency_ms: None,
                    max_candidates_per_segment: Some(2),
                },
            },
        )
        .unwrap();
    assert_eq!(report.hits[0].id, "true-neighbor");
    assert!(report.graph_bytes_read > 0);
    assert_eq!(report.object_cache_hits, 0);
    assert_eq!(report.object_cache_misses, 2);

    let summary = &index.manifest().segments[0];
    let cached_segment = cache.path().join(&summary.path);
    let cached_graph = cache.path().join(&summary.graph_path);
    assert!(cached_segment.exists());
    assert!(cached_graph.exists());

    fs::remove_file(dir.path().join(&summary.path)).unwrap();
    fs::remove_file(dir.path().join(&summary.graph_path)).unwrap();

    let cached_report = index
        .search_with_report(
            &[0.04, 0.07],
            SearchOptions {
                k: 1,
                mode: SearchMode::Approx {
                    eps: None,
                    max_segments: None,
                    max_bytes: None,
                    max_latency_ms: None,
                    max_candidates_per_segment: Some(2),
                },
            },
        )
        .unwrap();
    assert_eq!(cached_report.hits[0].id, "true-neighbor");
    assert_eq!(cached_report.object_cache_hits, 2);
    assert_eq!(cached_report.object_cache_misses, 0);
    assert_eq!(cached_report.records_scored, 2);
}

#[test]
fn exact_search_reports_segments_skipped_and_bytes_read() {
    let dir = tempfile::tempdir().unwrap();
    let uri = format!("file://{}", dir.path().display());

    let mut index = BorsukIndex::create(IndexConfig {
        uri,
        metric: VectorMetric::Euclidean,
        dimensions: 2,
        segment_max_vectors: 1,
        ram_budget_bytes: None,
    })
    .unwrap();

    index
        .add(vec![
            VectorRecord::new("near", vec![0.0, 0.0]),
            VectorRecord::new("mid", vec![10.0, 0.0]),
            VectorRecord::new("far", vec![20.0, 0.0]),
        ])
        .unwrap();

    let report = index
        .search_with_report(&[0.0, 0.0], SearchOptions::exact(1))
        .unwrap();

    assert_eq!(report.hits[0].id, "near");
    assert_eq!(report.segments_total, 3);
    assert_eq!(report.segments_searched, 1);
    assert_eq!(report.segments_skipped, 2);
    assert!(report.bytes_read > 0);
    assert!(report.resident_bytes_estimate > 0);
    assert!(report.resident_bytes_estimate >= 3 * 2 * std::mem::size_of::<f32>() as u64);
}

#[test]
fn compact_rewrites_l0_segments_into_l1_without_mutating_old_segments() {
    let dir = tempfile::tempdir().unwrap();
    let uri = format!("file://{}", dir.path().display());

    let mut index = BorsukIndex::create(IndexConfig {
        uri: uri.clone(),
        metric: VectorMetric::Euclidean,
        dimensions: 2,
        segment_max_vectors: 1,
        ram_budget_bytes: None,
    })
    .unwrap();

    index
        .add(vec![
            VectorRecord::new("a", vec![0.0, 0.0]),
            VectorRecord::new("b", vec![1.0, 0.0]),
            VectorRecord::new("c", vec![8.0, 0.0]),
            VectorRecord::new("d", vec![9.0, 0.0]),
        ])
        .unwrap();

    let l0_before = collect_files_with_extension(dir.path().join("segments/L0"), "parquet");
    let l0_graphs_before = collect_files_with_extension(dir.path().join("graphs/L0"), "parquet");
    assert_eq!(l0_before.len(), 4);
    assert_eq!(l0_graphs_before.len(), 4);
    assert_eq!(index.manifest().segments.len(), 4);
    assert!(
        index
            .manifest()
            .segments
            .iter()
            .all(|segment| segment.level == 0)
    );

    let report = index
        .compact(CompactionOptions {
            source_level: 0,
            target_level: 1,
            max_segments: Some(4),
            min_segments: 2,
            target_segment_max_vectors: Some(2),
        })
        .unwrap();

    assert!(report.compacted);
    assert_eq!(report.segments_read, 4);
    assert_eq!(report.segments_written, 2);
    assert_eq!(report.records_rewritten, 4);
    assert!(report.bytes_read > 0);
    assert!(report.bytes_written > 0);
    assert_eq!(report.manifest_version, index.manifest().version);

    assert_eq!(index.manifest().segments.len(), 2);
    assert!(
        index
            .manifest()
            .segments
            .iter()
            .all(|segment| segment.level == 1)
    );
    assert!(
        index
            .manifest()
            .segments
            .iter()
            .all(|segment| segment.path.starts_with("segments/L1/"))
    );

    let l0_after = collect_files_with_extension(dir.path().join("segments/L0"), "parquet");
    let l0_graphs_after = collect_files_with_extension(dir.path().join("graphs/L0"), "parquet");
    let l1_after = collect_files_with_extension(dir.path().join("segments/L1"), "parquet");
    let l1_graphs_after = collect_files_with_extension(dir.path().join("graphs/L1"), "parquet");
    assert_eq!(
        l0_after, l0_before,
        "compaction must not mutate old L0 objects"
    );
    assert_eq!(
        l0_graphs_after, l0_graphs_before,
        "compaction must not mutate old L0 graph objects"
    );
    assert_eq!(l1_after.len(), 2);
    assert_eq!(l1_graphs_after.len(), 2);

    let reopened = BorsukIndex::open(&uri).unwrap();
    assert!(
        reopened
            .manifest()
            .segments
            .iter()
            .all(|segment| segment.level == 1)
    );
    let hits = reopened
        .search(&[8.5, 0.0], SearchOptions::exact(2))
        .unwrap();
    assert_eq!(
        hits.iter().map(|hit| hit.id.as_str()).collect::<Vec<_>>(),
        vec!["c", "d"]
    );
}

#[test]
fn gc_obsolete_segments_dry_runs_and_deletes_inactive_segments_only() {
    let dir = tempfile::tempdir().unwrap();
    let uri = format!("file://{}", dir.path().display());

    let mut index = BorsukIndex::create(IndexConfig {
        uri,
        metric: VectorMetric::Euclidean,
        dimensions: 2,
        segment_max_vectors: 1,
        ram_budget_bytes: None,
    })
    .unwrap();

    index
        .add(vec![
            VectorRecord::new("a", vec![0.0, 0.0]),
            VectorRecord::new("b", vec![1.0, 0.0]),
            VectorRecord::new("c", vec![8.0, 0.0]),
            VectorRecord::new("d", vec![9.0, 0.0]),
        ])
        .unwrap();
    index
        .compact(CompactionOptions {
            source_level: 0,
            target_level: 1,
            max_segments: Some(4),
            min_segments: 2,
            target_segment_max_vectors: Some(2),
        })
        .unwrap();

    let l0_before = collect_files_with_extension(dir.path().join("segments/L0"), "parquet");
    let l1_before = collect_files_with_extension(dir.path().join("segments/L1"), "parquet");
    let l0_graphs_before = collect_files_with_extension(dir.path().join("graphs/L0"), "parquet");
    let l1_graphs_before = collect_files_with_extension(dir.path().join("graphs/L1"), "parquet");
    assert_eq!(l0_before.len(), 4);
    assert_eq!(l1_before.len(), 2);
    assert_eq!(l0_graphs_before.len(), 4);
    assert_eq!(l1_graphs_before.len(), 2);

    let dry_run = index
        .gc_obsolete_segments(GarbageCollectionOptions { dry_run: true })
        .unwrap();
    assert_eq!(dry_run.objects_scanned, 12);
    assert_eq!(dry_run.objects_deleted, 0);
    assert_eq!(dry_run.candidates.len(), 8);
    assert!(dry_run.bytes_reclaimable > 0);
    assert_eq!(
        collect_files_with_extension(dir.path().join("segments/L0"), "parquet"),
        l0_before
    );
    assert_eq!(
        collect_files_with_extension(dir.path().join("graphs/L0"), "parquet"),
        l0_graphs_before
    );

    let deleted = index
        .gc_obsolete_segments(GarbageCollectionOptions { dry_run: false })
        .unwrap();
    assert_eq!(deleted.objects_scanned, 12);
    assert_eq!(deleted.objects_deleted, 8);
    assert_eq!(deleted.candidates, dry_run.candidates);
    assert_eq!(deleted.bytes_reclaimed, dry_run.bytes_reclaimable);
    assert!(collect_files_with_extension(dir.path().join("segments/L0"), "parquet").is_empty());
    assert!(collect_files_with_extension(dir.path().join("graphs/L0"), "parquet").is_empty());
    assert_eq!(
        collect_files_with_extension(dir.path().join("segments/L1"), "parquet"),
        l1_before
    );
    assert_eq!(
        collect_files_with_extension(dir.path().join("graphs/L1"), "parquet"),
        l1_graphs_before
    );

    let hits = index.search(&[8.5, 0.0], SearchOptions::exact(2)).unwrap();
    assert_eq!(
        hits.iter().map(|hit| hit.id.as_str()).collect::<Vec<_>>(),
        vec!["c", "d"]
    );
}

#[test]
fn index_rejects_vectors_with_wrong_dimension() {
    let dir = tempfile::tempdir().unwrap();
    let uri = format!("file://{}", dir.path().display());
    let mut index = BorsukIndex::create(IndexConfig {
        uri,
        metric: VectorMetric::Euclidean,
        dimensions: 3,
        segment_max_vectors: 32,
        ram_budget_bytes: None,
    })
    .unwrap();

    let err = index
        .add(vec![VectorRecord::new("bad", vec![1.0, 2.0])])
        .unwrap_err();
    assert!(err.to_string().contains("dimension mismatch"));
}

fn collect_files_with_extension(
    root: impl AsRef<std::path::Path>,
    extension: &str,
) -> Vec<std::path::PathBuf> {
    let root = root.as_ref();
    let mut files = Vec::new();
    if !root.exists() {
        return files;
    }

    for entry in fs::read_dir(root).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            files.extend(collect_files_with_extension(&path, extension));
        } else if path.extension().is_some_and(|actual| actual == extension) {
            files.push(path);
        }
    }
    files.sort();
    files
}

fn collect_files_with_prefix<'a>(
    files: &'a [std::path::PathBuf],
    prefix: &str,
) -> Vec<&'a std::path::PathBuf> {
    files
        .iter()
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with(prefix))
        })
        .collect()
}

fn assert_is_parquet_file(path: &std::path::Path) {
    let bytes = fs::read(path).unwrap();
    assert!(
        bytes.len() >= 8,
        "parquet file {} is unexpectedly short",
        path.display()
    );
    assert_eq!(
        &bytes[0..4],
        b"PAR1",
        "bad parquet header: {}",
        path.display()
    );
    assert_eq!(
        &bytes[bytes.len() - 4..],
        b"PAR1",
        "bad parquet footer: {}",
        path.display()
    );
}
