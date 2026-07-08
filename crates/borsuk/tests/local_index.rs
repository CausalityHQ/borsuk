#![allow(missing_docs)]

#[allow(dead_code)]
mod common;

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    sync::Arc,
    time::{Duration, Instant},
};

use arrow_array::{
    Array, ArrayRef, BinaryArray, FixedSizeListArray, Float32Array, Int64Array, RecordBatch,
    StringArray, UInt8Array, UInt16Array, UInt64Array, types::Float32Type,
};
use arrow_schema::{DataType, Field, Schema};
use borsuk::{
    AddReport, BorsukError, BorsukIndex, CompactionOptions, GarbageCollectionOptions, IndexConfig,
    LeafMode, Manifest, OpenOptions, RebuildOptions, RecallGuarantee, SearchMode, SearchOptions,
    SearchTerminationReason, SegmentSummary, VectorMetric, VectorRecord, leaf_mode_names,
};
use futures_util::TryStreamExt;
use object_store::{ObjectStore, memory::InMemory, path::Path as ObjectPath};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::{arrow::ArrowWriter, basic::Compression, file::properties::WriterProperties};

/// Open with routing summaries held resident in RAM. The library default is paged
/// (minimal RAM); these helpers pin the resident path for tests that assert
/// resident-only behavior (pivot/version validation at open, exact routing-page
/// read counts, or direct `manifest().segments`/`pivots` access).
fn open_resident(uri: &str) -> Result<BorsukIndex, BorsukError> {
    BorsukIndex::open_with_options(
        uri,
        OpenOptions {
            resident_routing: true,
            ..OpenOptions::default()
        },
    )
}

fn open_resident_cached(uri: &str, cache: std::path::PathBuf) -> Result<BorsukIndex, BorsukError> {
    BorsukIndex::open_with_options(
        uri,
        OpenOptions {
            resident_routing: true,
            cache_dir: Some(cache),
            ..OpenOptions::default()
        },
    )
}

#[test]
fn shared_in_memory_store_handles_see_published_data() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let store: Arc<dyn ObjectStore> = Arc::new(common::FaultInjectingObjectStore::new(inner));
    let mut writer = BorsukIndex::create_with_object_store(
        Arc::clone(&store),
        IndexConfig {
            uri: "memory:///shared".to_string(),
            metric: VectorMetric::Euclidean,
            dimensions: 2,
            segment_max_vectors: 2,
            ram_budget_bytes: None,
        },
    )
    .unwrap();

    writer
        .add(vec![
            VectorRecord::new("a", vec![0.0, 0.0]),
            VectorRecord::new("b", vec![1.0, 0.0]),
        ])
        .unwrap();

    let reader =
        BorsukIndex::open_with_object_store(Arc::clone(&store), "memory:///shared").unwrap();

    assert_eq!(
        reader
            .search_ids(&[0.2, 0.0], SearchOptions::exact(2))
            .unwrap(),
        ["a", "b"]
    );
}

#[test]
fn concurrent_adds_on_same_manifest_return_concurrent_modification() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let store: Arc<dyn ObjectStore> = Arc::new(common::FaultInjectingObjectStore::new(inner));
    let mut winner = BorsukIndex::create_with_object_store(
        Arc::clone(&store),
        IndexConfig {
            uri: "memory:///concurrent".to_string(),
            metric: VectorMetric::Euclidean,
            dimensions: 2,
            segment_max_vectors: 2,
            ram_budget_bytes: None,
        },
    )
    .unwrap();
    let mut loser =
        BorsukIndex::open_with_object_store(Arc::clone(&store), "memory:///concurrent").unwrap();

    winner
        .add(vec![VectorRecord::new("winner", vec![0.0, 0.0])])
        .unwrap();
    let error = loser
        .add(vec![VectorRecord::new("loser", vec![9.0, 0.0])])
        .unwrap_err();

    assert!(
        matches!(error, BorsukError::ConcurrentModification { .. }),
        "{error:?}"
    );
    let reopened =
        BorsukIndex::open_with_object_store(Arc::clone(&store), "memory:///concurrent").unwrap();
    assert_eq!(reopened.manifest().version, 2);
    assert_eq!(
        reopened
            .search_ids(&[0.0, 0.0], SearchOptions::exact(1))
            .unwrap(),
        ["winner"]
    );
    assert_eq!(reopened.get_vector("loser").unwrap(), None);
}

#[test]
fn concurrent_adds_racing_through_publish_return_concurrent_modification() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let setup_store: Arc<dyn ObjectStore> =
        Arc::new(common::FaultInjectingObjectStore::new(Arc::clone(&inner)));
    BorsukIndex::create_with_object_store(
        setup_store,
        IndexConfig {
            uri: "memory:///racing".to_string(),
            metric: VectorMetric::Euclidean,
            dimensions: 2,
            segment_max_vectors: 2,
            ram_budget_bytes: None,
        },
    )
    .unwrap();

    // Both writers open at version 1, pause at their first conditional write into the
    // version-2 namespace, and are released into publish together so the same-version
    // create race actually overlaps instead of running sequentially.
    let is_version_two_publish_object = |_: common::StoreOperation, path: &ObjectPath| {
        let path = path.as_ref();
        path.contains("-00000000000000000002.") || path.contains("/00000000000000000002/")
    };
    let barrier = Arc::new(std::sync::Barrier::new(2));
    let writers = [("left", 0.0_f32), ("right", 9.0_f32)].map(|(id, x)| {
        let inner = Arc::clone(&inner);
        let barrier = Arc::clone(&barrier);
        std::thread::spawn(move || {
            let store: Arc<dyn ObjectStore> = Arc::new(
                common::FaultInjectingObjectStore::new(inner)
                    .with_put_barrier(barrier, is_version_two_publish_object),
            );
            let mut writer =
                BorsukIndex::open_with_object_store(store, "memory:///racing").unwrap();
            writer
                .add(vec![VectorRecord::new(id, vec![x, 0.0])])
                .map(|_| (id, x))
        })
    });
    let outcomes = writers.map(|writer| writer.join().unwrap());

    let winners = outcomes
        .iter()
        .filter_map(|outcome| outcome.as_ref().ok().copied())
        .collect::<Vec<_>>();
    let losers = outcomes
        .iter()
        .filter_map(|outcome| outcome.as_ref().err())
        .collect::<Vec<_>>();
    assert_eq!(winners.len(), 1, "{outcomes:?}");
    assert_eq!(losers.len(), 1, "{outcomes:?}");
    assert!(
        matches!(losers[0], BorsukError::ConcurrentModification { .. }),
        "{:?}",
        losers[0]
    );

    let (winner_id, winner_x) = winners[0];
    let loser_id = if winner_id == "left" { "right" } else { "left" };
    let reopened =
        BorsukIndex::open_with_object_store(Arc::clone(&inner), "memory:///racing").unwrap();
    assert_eq!(reopened.manifest().version, 2);
    assert_eq!(
        reopened
            .search_ids(&[winner_x, 0.0], SearchOptions::exact(1))
            .unwrap(),
        [winner_id]
    );
    assert!(reopened.get_vector(winner_id).unwrap().is_some());
    assert_eq!(reopened.get_vector(loser_id).unwrap(), None);
}

#[test]
fn publish_crash_before_current_leaves_old_version_readable_and_skips_orphan_namespace() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let setup_store: Arc<dyn ObjectStore> =
        Arc::new(common::FaultInjectingObjectStore::new(Arc::clone(&inner)));
    let mut setup = BorsukIndex::create_with_object_store(
        setup_store,
        IndexConfig {
            uri: "memory:///orphan".to_string(),
            metric: VectorMetric::Euclidean,
            dimensions: 2,
            segment_max_vectors: 2,
            ram_budget_bytes: None,
        },
    )
    .unwrap();
    setup
        .add(vec![VectorRecord::new("base", vec![0.0, 0.0])])
        .unwrap();
    assert_eq!(setup.manifest().version, 2);

    let faulting_store: Arc<dyn ObjectStore> =
        Arc::new(common::FaultInjectingObjectStore::fail_nth_matching(
            Arc::clone(&inner),
            1,
            true,
            |operation, path| {
                operation == common::StoreOperation::Put && path.as_ref() == "CURRENT"
            },
        ));
    let mut crashing =
        BorsukIndex::open_with_object_store(faulting_store, "memory:///orphan").unwrap();
    crashing
        .add(vec![VectorRecord::new("orphaned", vec![1.0, 0.0])])
        .unwrap_err();

    let readable =
        BorsukIndex::open_with_object_store(Arc::clone(&inner), "memory:///orphan").unwrap();
    assert_eq!(readable.manifest().version, 2);
    assert_eq!(
        readable
            .search_ids(&[0.0, 0.0], SearchOptions::exact(1))
            .unwrap(),
        ["base"]
    );
    let objects = list_object_paths(Arc::clone(&inner));
    for path in [
        "routing/layers/00000000000000000003/L0/pages.parquet",
        "manifests/manifest-00000000000000000003.parquet",
        "routing/segments-00000000000000000003.parquet",
        "routing/pivots-00000000000000000003.parquet",
    ] {
        assert!(
            objects.iter().any(|object| object == path),
            "{path} must be durable before CURRENT is attempted"
        );
    }

    let mut recovered =
        BorsukIndex::open_with_object_store(Arc::clone(&inner), "memory:///orphan").unwrap();
    recovered
        .add(vec![VectorRecord::new("recovered", vec![2.0, 0.0])])
        .unwrap();

    assert_eq!(recovered.manifest().version, 4);
    let reopened =
        BorsukIndex::open_with_object_store(Arc::clone(&inner), "memory:///orphan").unwrap();
    assert_eq!(reopened.manifest().version, 4);
    assert_eq!(
        reopened.get_vector("recovered").unwrap(),
        Some(vec![2.0, 0.0])
    );
}

#[test]
fn local_index_persists_segments_and_reopens_for_exact_search() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

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

    let ids = index
        .search_ids(
            &[0.2, 0.0],
            SearchOptions {
                k: 2,
                mode: SearchMode::Exact,
                guaranteed_recall: false,
                prefetch_depth: borsuk::DEFAULT_SEARCH_PREFETCH_DEPTH,
                filter: None,
                include_metadata: false,
            },
        )
        .unwrap();

    assert_eq!(ids, vec!["a", "b"]);
    assert!(dir.path().join("CURRENT").exists());
    assert!(dir.path().join("manifests").exists());
    assert!(
        fs::read_dir(dir.path().join("segments/L0"))
            .unwrap()
            .count()
            > 0
    );

    let reopened = BorsukIndex::open(&uri).unwrap();
    let reopened_ids = reopened
        .search_ids(&[8.5, 0.0], SearchOptions::exact(1))
        .unwrap();
    assert_eq!(reopened_ids[0], "c");
}

#[test]
fn local_index_can_search_ids_vectors_and_load_vector_by_id() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

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
            VectorRecord::new("far", vec![9.0, 0.0]),
        ])
        .unwrap();

    let reopened = BorsukIndex::open(&uri).unwrap();

    assert_eq!(
        reopened
            .search_ids(&[0.8, 0.0], SearchOptions::exact(2))
            .unwrap(),
        ["b", "a"]
    );
    assert_eq!(
        reopened
            .search_vectors(&[0.8, 0.0], SearchOptions::exact(2))
            .unwrap(),
        [vec![1.0, 0.0], vec![0.0, 0.0]]
    );
    assert_eq!(reopened.get_vector("far").unwrap(), Some(vec![9.0, 0.0]));
    assert_eq!(reopened.get_vector("missing").unwrap(), None);
}

#[test]
fn get_vector_rejects_empty_record_ids() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

    let index = BorsukIndex::create(IndexConfig {
        uri,
        metric: VectorMetric::Euclidean,
        dimensions: 2,
        segment_max_vectors: 1,
        ram_budget_bytes: None,
    })
    .unwrap();

    let empty = index.get_vector("").unwrap_err();
    assert!(
        empty.to_string().contains("record ids must not be empty"),
        "{empty}"
    );

    let whitespace = index.get_vector(" \t ").unwrap_err();
    assert!(
        whitespace
            .to_string()
            .contains("record ids must not be empty"),
        "{whitespace}"
    );
}

#[test]
fn local_index_can_search_and_load_non_utf8_record_ids() {
    let dir = tempfile::tempdir().unwrap();
    let mut index = BorsukIndex::create(IndexConfig {
        uri: dir.path().to_string_lossy().into_owned(),
        metric: VectorMetric::Euclidean,
        dimensions: 2,
        segment_max_vectors: 2,
        ram_budget_bytes: None,
    })
    .unwrap();

    let id = vec![0, 159, 255, 7];
    index
        .add(vec![VectorRecord::new_bytes(id.clone(), vec![0.0, 0.0])])
        .unwrap();

    let expected_ids = std::slice::from_ref(&id);
    assert_eq!(
        index
            .search_id_bytes(&[0.0, 0.0], SearchOptions::exact(1))
            .unwrap(),
        expected_ids
    );
    assert_eq!(index.get_vector_by_id(&id).unwrap(), Some(vec![0.0, 0.0]));
    assert_eq!(
        index
            .search_vectors(&[0.0, 0.0], SearchOptions::exact(1))
            .unwrap(),
        [vec![0.0, 0.0]]
    );
}

#[test]
fn get_vector_skips_segments_that_cannot_contain_the_id() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

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
            VectorRecord::new("target", vec![0.0, 0.0]),
            VectorRecord::new("other", vec![1.0, 0.0]),
            VectorRecord::new("newest", vec![2.0, 0.0]),
        ])
        .unwrap();
    let newest_segment = dir.path().join(&index.manifest().segments[2].path);
    fs::write(
        newest_segment,
        b"corrupt unrelated segment that must be skipped",
    )
    .unwrap();

    assert_eq!(index.get_vector("target").unwrap(), Some(vec![0.0, 0.0]));
}

#[test]
fn explicit_id_add_skips_segments_that_cannot_contain_the_ids() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

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
            VectorRecord::new("old-a", vec![0.0, 0.0]),
            VectorRecord::new("old-b", vec![1.0, 0.0]),
        ])
        .unwrap();
    let old_segment = dir.path().join(&index.manifest().segments[0].path);
    fs::write(
        old_segment,
        b"corrupt unrelated segment that duplicate validation must skip",
    )
    .unwrap();

    let ids = index
        .add_vectors_with_ids(vec![vec![2.0, 0.0]], vec!["fresh".to_string()])
        .unwrap();

    assert_eq!(ids, ["fresh"]);
}

#[test]
fn local_index_rejects_duplicate_record_ids() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

    let mut index = BorsukIndex::create(IndexConfig {
        uri,
        metric: VectorMetric::Euclidean,
        dimensions: 2,
        segment_max_vectors: 2,
        ram_budget_bytes: None,
    })
    .unwrap();

    let batch_duplicate = index
        .add(vec![
            VectorRecord::new("dup", vec![0.0, 0.0]),
            VectorRecord::new("dup", vec![1.0, 0.0]),
        ])
        .unwrap_err();
    assert!(
        batch_duplicate.to_string().contains("duplicate record id"),
        "{batch_duplicate}"
    );

    index
        .add(vec![VectorRecord::new("existing", vec![0.0, 0.0])])
        .unwrap();

    let existing_duplicate = index
        .add(vec![VectorRecord::new("existing", vec![1.0, 0.0])])
        .unwrap_err();
    assert!(
        existing_duplicate
            .to_string()
            .contains("duplicate record id"),
        "{existing_duplicate}"
    );
}

#[test]
fn local_index_rejects_empty_record_ids() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

    let mut index = BorsukIndex::create(IndexConfig {
        uri,
        metric: VectorMetric::Euclidean,
        dimensions: 2,
        segment_max_vectors: 2,
        ram_budget_bytes: None,
    })
    .unwrap();

    let err = index
        .add(vec![VectorRecord::new("", vec![0.0, 0.0])])
        .unwrap_err();

    assert!(
        err.to_string().contains("record ids must not be empty"),
        "{err}"
    );
}

#[test]
fn local_index_rejects_non_finite_vectors_and_queries() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

    let mut index = BorsukIndex::create(IndexConfig {
        uri,
        metric: VectorMetric::Euclidean,
        dimensions: 2,
        segment_max_vectors: 2,
        ram_budget_bytes: None,
    })
    .unwrap();

    let record_error = index
        .add(vec![VectorRecord::new("nan", vec![f32::NAN, 0.0])])
        .unwrap_err();
    assert!(
        record_error.to_string().contains("finite f32 values"),
        "{record_error}"
    );

    let generated_error = index
        .add_vectors(vec![vec![f32::INFINITY, 0.0]])
        .unwrap_err();
    assert!(
        generated_error.to_string().contains("finite f32 values"),
        "{generated_error}"
    );

    index
        .add(vec![VectorRecord::new("valid", vec![0.0, 0.0])])
        .unwrap();

    let query_error = index
        .search_ids(&[f32::NEG_INFINITY, 0.0], SearchOptions::exact(1))
        .unwrap_err();
    assert!(
        query_error.to_string().contains("finite f32 values"),
        "{query_error}"
    );
}

#[test]
fn generated_vector_add_does_not_scan_existing_segment_payloads() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

    let mut index = BorsukIndex::create(IndexConfig {
        uri,
        metric: VectorMetric::Euclidean,
        dimensions: 2,
        segment_max_vectors: 1,
        ram_budget_bytes: None,
    })
    .unwrap();

    index
        .add(vec![VectorRecord::new("1", vec![0.0, 0.0])])
        .unwrap();
    let first_segment = dir.path().join(&index.manifest().segments[0].path);
    fs::write(first_segment, b"corrupt segment that must not be read").unwrap();

    let ids = index
        .add_vectors(vec![vec![2.0, 0.0], vec![3.0, 0.0]])
        .unwrap();

    assert_eq!(ids, ["2", "3"]);
}

#[test]
fn local_index_searches_query_batches() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

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

    let ids = index
        .search_ids_batch(&[vec![0.1, 0.0], vec![9.9, 0.0]], SearchOptions::exact(1))
        .unwrap();

    assert_eq!(
        ids,
        vec![vec!["left".to_string()], vec!["right".to_string()]]
    );
    let vectors = index
        .search_vectors_batch(&[vec![0.1, 0.0], vec![9.9, 0.0]], SearchOptions::exact(1))
        .unwrap();
    assert_eq!(vectors, vec![vec![vec![0.0, 0.0]], vec![vec![10.0, 0.0]]]);
}

#[test]
fn local_index_reports_query_batches() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

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
    let uri = dir.path().to_string_lossy().into_owned();

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
fn stats_use_routing_page_index_when_full_routing_table_is_empty() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

    let mut index = BorsukIndex::create(IndexConfig {
        uri: uri.clone(),
        metric: VectorMetric::Euclidean,
        dimensions: 2,
        segment_max_vectors: 1,
        ram_budget_bytes: None,
    })
    .unwrap();

    let records = (0..130)
        .map(|id| VectorRecord::new(format!("v{id}"), vec![id as f32, 0.0]))
        .collect::<Vec<_>>();
    index.add(records).unwrap();
    let expected_segment_bytes = index
        .manifest()
        .segments
        .iter()
        .map(|s| s.size_bytes)
        .sum::<u64>();
    let expected_graph_bytes = index
        .manifest()
        .segments
        .iter()
        .map(|s| s.graph_size_bytes)
        .sum::<u64>();

    let page_refs = routing_layer_page_index_paths(dir.path(), index.manifest().version, 0);
    assert_eq!(page_refs.len(), 2);
    fs::write(
        dir.path().join(&page_refs[0]),
        b"corrupt routing page that stats must not read",
    )
    .unwrap();
    rewrite_current_with_empty_routing_table(dir.path(), index.manifest());

    let reopened = BorsukIndex::open(&uri).unwrap();
    assert!(reopened.manifest().segments.is_empty());

    let stats = reopened.stats();
    assert_eq!(stats.segments, 130);
    assert_eq!(stats.records, 130);
    assert_eq!(stats.segment_bytes, expected_segment_bytes);
    assert_eq!(stats.graph_bytes, expected_graph_bytes);
}

#[test]
fn open_can_use_paged_routing_without_resident_segment_summaries() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

    let mut index = BorsukIndex::create(IndexConfig {
        uri: uri.clone(),
        metric: VectorMetric::Euclidean,
        dimensions: 2,
        segment_max_vectors: 1,
        ram_budget_bytes: None,
    })
    .unwrap();

    let records = (0..130)
        .map(|id| VectorRecord::new(format!("v{id}"), vec![id as f32, 0.0]))
        .collect::<Vec<_>>();
    index.add(records).unwrap();
    let full_resident_bytes = index.stats().resident_bytes_estimate;

    let reopened = BorsukIndex::open_with_options(
        &uri,
        OpenOptions {
            resident_routing: false,
            ram_budget_bytes: Some(full_resident_bytes - 1),
            ..OpenOptions::default()
        },
    )
    .unwrap();

    assert!(
        reopened.manifest().segments.is_empty(),
        "paged routing open should keep segment summaries out of the resident manifest"
    );
    let stats = reopened.stats();
    assert_eq!(stats.segments, 130);
    assert_eq!(stats.records, 130);
    assert!(stats.resident_bytes_estimate < full_resident_bytes);

    let report = reopened
        .search_with_report(
            &[129.0, 0.0],
            SearchOptions::approx(1, LeafMode::PqScan).with_max_segments(1),
        )
        .unwrap();

    assert_eq!(report.hits[0].id, "v129");
    assert_eq!(report.segments_total, 130);
    assert_eq!(report.segments_searched, 1);
    assert!(report.resident_bytes_estimate < full_resident_bytes);
}

#[test]
fn non_resident_search_lifecycle_keeps_segment_summaries_out_of_ram() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

    let mut index = BorsukIndex::create_with_routing_page_fanout(
        IndexConfig {
            uri: uri.clone(),
            metric: VectorMetric::Euclidean,
            dimensions: 2,
            segment_max_vectors: 1,
            ram_budget_bytes: None,
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
    let full_resident_bytes = index.stats().resident_bytes_estimate;

    let compaction = index
        .compact(CompactionOptions {
            source_level: 0,
            target_level: 1,
            max_segments: Some(24),
            min_segments: 2,
            target_segment_max_vectors: Some(1),
            target_segment_max_radius: None,
        })
        .unwrap();
    assert!(compaction.compacted);
    assert_eq!(compaction.records_rewritten, 24);
    assert!(
        index.manifest().segments.is_empty(),
        "compaction should leave segment summaries in routing pages, not resident RAM"
    );
    let compacted_stats = index.stats();
    assert_eq!(compacted_stats.records, 24);
    assert!(compacted_stats.routing_max_level >= 2);
    assert!(compacted_stats.resident_bytes_estimate < full_resident_bytes);

    let metadata_budget = compacted_stats.resident_bytes_estimate;
    drop(index);

    let reopened = BorsukIndex::open_with_options(
        &uri,
        OpenOptions {
            resident_routing: false,
            ram_budget_bytes: Some(metadata_budget),
            ..OpenOptions::default()
        },
    )
    .unwrap();

    assert!(reopened.manifest().segments.is_empty());
    let initial_resident_bytes = reopened.stats().resident_bytes_estimate;
    assert_eq!(initial_resident_bytes, metadata_budget);

    for id in [0, 1, 3, 5, 7, 9, 11, 13, 17, 19, 21, 23] {
        let report = reopened
            .search_with_report(
                &[id as f32, 0.0],
                SearchOptions::approx(1, LeafMode::PqScan)
                    .with_max_segments(1)
                    .with_routing_page_overfetch(1),
            )
            .unwrap();

        assert_eq!(report.hits[0].id.as_str(), format!("v{id}"));
        assert_eq!(report.resident_bytes_estimate, initial_resident_bytes);
        assert_eq!(
            reopened.stats().resident_bytes_estimate,
            initial_resident_bytes
        );
        assert!(
            reopened.manifest().segments.is_empty(),
            "query {id} must not repopulate resident segment summaries"
        );
    }
}

#[test]
fn approximate_search_drills_through_deep_paged_routing_tree() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

    let mut index = BorsukIndex::create_with_routing_page_fanout(
        IndexConfig {
            uri: uri.clone(),
            metric: VectorMetric::Euclidean,
            dimensions: 2,
            segment_max_vectors: 1,
            ram_budget_bytes: None,
        },
        4,
    )
    .unwrap();

    let mut records = (0..64)
        .map(|id| VectorRecord::new(format!("far-{id}"), vec![1000.0 + id as f32, 0.0]))
        .collect::<Vec<_>>();
    records.push(VectorRecord::new("near", vec![0.0, 0.0]));
    index.add(records).unwrap();
    assert_eq!(index.stats().routing_page_fanout, 4);
    assert_eq!(index.stats().routing_max_level, 3);

    let reopened = BorsukIndex::open_with_options(
        &uri,
        OpenOptions {
            resident_routing: false,
            ..OpenOptions::default()
        },
    )
    .unwrap();
    assert!(reopened.manifest().segments.is_empty());

    fs::write(
        dir.path().join(format!(
            "routing/layers/{:020}/L0/pages.parquet",
            index.manifest().version
        )),
        b"corrupt global L0 routing page index that deep search must not read",
    )
    .unwrap();

    let report = reopened
        .search_with_report(
            &[0.0, 0.0],
            SearchOptions::approx(1, LeafMode::PqScan)
                .with_max_segments(1)
                .with_routing_page_overfetch(1),
        )
        .unwrap();

    assert_eq!(report.hits[0].id, "near");
    assert_eq!(report.segments_total, 65);
    assert_eq!(report.segments_searched, 1);
    assert_eq!(report.routing_page_indexes_read, 1);
    assert_eq!(
        report.routing_pages_read, 4,
        "deep paged search should read one parent page per routing level plus the selected L0 leaf page"
    );
}

#[test]
fn deep_routing_compaction_reuses_untouched_parent_pages() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

    let mut index = BorsukIndex::create_with_routing_page_fanout(
        IndexConfig {
            uri: uri.clone(),
            metric: VectorMetric::Euclidean,
            dimensions: 2,
            segment_max_vectors: 1,
            ram_budget_bytes: None,
        },
        4,
    )
    .unwrap();

    index
        .add(
            (0..32)
                .map(|id| VectorRecord::new(format!("v{id}"), vec![id as f32, 0.0]))
                .collect(),
        )
        .unwrap();
    assert_eq!(index.stats().routing_max_level, 2);

    let first_compaction = index
        .compact(CompactionOptions {
            source_level: 0,
            target_level: 1,
            max_segments: Some(32),
            min_segments: 2,
            target_segment_max_vectors: Some(1),
            target_segment_max_radius: None,
        })
        .unwrap();
    assert!(first_compaction.compacted);
    assert_eq!(first_compaction.segments_read, 32);
    assert_eq!(first_compaction.records_rewritten, 32);
    assert_eq!(index.stats().routing_max_level, 2);
    assert!(
        routing_leaf_page_segments(dir.path(), index.manifest().version)
            .iter()
            .all(|segment| segment.level == 1),
        "L0->L1 compaction should rewrite every active summary to L1"
    );
    assert_eq!(
        index
            .search_ids(
                &[31.0, 0.0],
                SearchOptions::approx(1, LeafMode::PqScan)
                    .with_max_segments(1)
                    .with_routing_page_overfetch(1),
            )
            .unwrap(),
        ["v31"]
    );

    let before_version = index.manifest().version;
    let before_l1_parent_paths = routing_page_paths_at_level(dir.path(), before_version, 1);
    assert_eq!(
        before_l1_parent_paths.len(),
        2,
        "32 segments at fanout 4 should produce two L1 parent pages"
    );
    let before_leaf_paths = routing_leaf_page_paths(dir.path(), before_version);
    assert_eq!(before_leaf_paths.len(), 8);
    let before_segments = routing_leaf_page_segments(dir.path(), before_version);
    let selected_segment_bytes = before_segments
        .iter()
        .take(2)
        .map(|segment| segment.size_bytes)
        .sum::<u64>();
    let top_index_bytes = fs::metadata(dir.path().join(format!(
        "routing/layers/{before_version:020}/L2/pages.parquet"
    )))
    .unwrap()
    .len();
    let top_page_path = routing_page_paths_at_level(dir.path(), before_version, 2)
        .into_iter()
        .next()
        .unwrap();
    let expected_branch_bytes = top_index_bytes
        + fs::metadata(dir.path().join(top_page_path)).unwrap().len()
        + fs::metadata(dir.path().join(&before_l1_parent_paths[0]))
            .unwrap()
            .len()
        + fs::metadata(dir.path().join(&before_leaf_paths[0]))
            .unwrap()
            .len()
        + selected_segment_bytes;
    let untouched_parent_bytes = fs::metadata(dir.path().join(&before_l1_parent_paths[1]))
        .unwrap()
        .len();
    let untouched_parent_path = before_l1_parent_paths[1].clone();

    let second_compaction = index
        .compact(CompactionOptions {
            source_level: 1,
            target_level: 2,
            max_segments: Some(2),
            min_segments: 2,
            target_segment_max_vectors: Some(2),
            target_segment_max_radius: None,
        })
        .unwrap();

    assert!(second_compaction.compacted);
    assert_eq!(second_compaction.segments_read, 2);
    assert_eq!(second_compaction.segments_written, 1);
    assert_eq!(second_compaction.records_rewritten, 2);
    assert_eq!(second_compaction.routing_pages_read, 3);
    assert_eq!(
        second_compaction.bytes_read, expected_branch_bytes,
        "scoped L1 compaction should read the top index, selected branch pages, and selected payloads only"
    );
    assert!(
        second_compaction.bytes_read < expected_branch_bytes + untouched_parent_bytes,
        "scoped L1 compaction must not include the untouched L1 parent page in bytes_read"
    );

    let after_l1_parent_paths =
        routing_page_paths_at_level(dir.path(), index.manifest().version, 1);
    assert!(
        after_l1_parent_paths.contains(&untouched_parent_path),
        "content-addressed untouched parent page should be reused by path"
    );
    let after_segments = routing_leaf_page_segments(dir.path(), index.manifest().version);
    assert_eq!(
        after_segments
            .iter()
            .filter(|segment| segment.level == 2)
            .count(),
        1,
        "L1->L2 compaction should publish exactly one rewritten L2 summary"
    );
    assert!(
        after_segments
            .iter()
            .all(|segment| matches!(segment.level, 1 | 2)),
        "L1->L2 compaction should preserve untouched L1 summaries and publish rewritten L2 summaries"
    );
    assert_eq!(index.stats().records, 32);
    assert_eq!(
        index
            .search_ids(
                &[0.0, 0.0],
                SearchOptions::approx(1, LeafMode::PqScan)
                    .with_max_segments(1)
                    .with_routing_page_overfetch(1),
            )
            .unwrap(),
        ["v0"]
    );
    assert_eq!(
        index
            .search_ids(
                &[31.0, 0.0],
                SearchOptions::approx(1, LeafMode::PqScan)
                    .with_max_segments(1)
                    .with_routing_page_overfetch(1),
            )
            .unwrap(),
        ["v31"]
    );
}

#[test]
fn paged_routing_open_skips_resident_routing_and_pivots_decode() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

    let mut index = BorsukIndex::create(IndexConfig {
        uri: uri.clone(),
        metric: VectorMetric::Euclidean,
        dimensions: 2,
        segment_max_vectors: 1,
        ram_budget_bytes: None,
    })
    .unwrap();

    let records = (0..130)
        .map(|id| VectorRecord::new(format!("v{id}"), vec![id as f32, 0.0]))
        .collect::<Vec<_>>();
    index.add(records).unwrap();
    let full_resident_bytes = index.stats().resident_bytes_estimate;
    rewrite_current_routing_metadata(
        dir.path(),
        index.manifest(),
        None,
        Some(0),
        None,
        None,
        None,
    );

    let reopened = BorsukIndex::open_with_options(
        &uri,
        OpenOptions {
            resident_routing: false,
            ram_budget_bytes: Some(full_resident_bytes - 1),
            ..OpenOptions::default()
        },
    )
    .unwrap();

    assert!(reopened.manifest().segments.is_empty());
    assert!(reopened.manifest().pivots.is_empty());
    assert_eq!(reopened.stats().segments, 130);
}

#[test]
fn paged_routing_open_does_not_fetch_full_routing_or_pivots_metadata() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

    let mut index = BorsukIndex::create(IndexConfig {
        uri: uri.clone(),
        metric: VectorMetric::Euclidean,
        dimensions: 2,
        segment_max_vectors: 1,
        ram_budget_bytes: None,
    })
    .unwrap();

    let records = (0..130)
        .map(|id| VectorRecord::new(format!("v{id}"), vec![id as f32, 0.0]))
        .collect::<Vec<_>>();
    index.add(records).unwrap();

    fs::remove_file(dir.path().join(format!(
        "routing/segments-{:020}.parquet",
        index.manifest().version
    )))
    .unwrap();
    fs::remove_file(dir.path().join(format!(
        "routing/pivots-{:020}.parquet",
        index.manifest().version
    )))
    .unwrap();

    let reopened = BorsukIndex::open_with_options(
        &uri,
        OpenOptions {
            resident_routing: false,
            ..OpenOptions::default()
        },
    )
    .unwrap();

    assert!(reopened.manifest().segments.is_empty());
    assert!(reopened.manifest().pivots.is_empty());
    assert_eq!(reopened.stats().segments, 130);

    let resident_open = open_resident(&uri).unwrap_err();
    assert!(
        resident_open.to_string().contains("routing/segments-")
            || resident_open.to_string().contains("routing/pivots-"),
        "unexpected error: {resident_open}"
    );
}

#[test]
fn try_stats_rejects_corrupt_routing_page_index_when_full_routing_table_is_empty() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

    let mut index = BorsukIndex::create(IndexConfig {
        uri: uri.clone(),
        metric: VectorMetric::Euclidean,
        dimensions: 2,
        segment_max_vectors: 1,
        ram_budget_bytes: None,
    })
    .unwrap();

    index
        .add(vec![VectorRecord::new("a", vec![0.0, 0.0])])
        .unwrap();
    rewrite_current_with_empty_routing_table(dir.path(), index.manifest());
    fs::write(
        dir.path().join(format!(
            "routing/layers/{:020}/L0/pages.parquet",
            index.manifest().version
        )),
        b"corrupt routing page index",
    )
    .unwrap();

    let reopened = BorsukIndex::open(&uri).unwrap();
    let err = reopened.try_stats().unwrap_err();
    let message = err.to_string().to_ascii_lowercase();
    assert!(
        message.contains("parquet") || message.contains("routing layer page index"),
        "{err}"
    );
}

#[test]
fn create_rejects_too_small_ram_budget() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

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
    let uri = dir.path().to_string_lossy().into_owned();

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
fn open_with_cache_reads_fresh_current_after_external_publish() {
    let dir = tempfile::tempdir().unwrap();
    let cache = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

    let cached = BorsukIndex::create_with_cache(
        IndexConfig {
            uri: uri.clone(),
            metric: VectorMetric::Euclidean,
            dimensions: 2,
            segment_max_vectors: 2,
            ram_budget_bytes: None,
        },
        Some(cache.path().to_path_buf()),
    )
    .unwrap();
    assert_eq!(cached.manifest().version, 1);

    let mut writer = BorsukIndex::open(&uri).unwrap();
    writer
        .add(vec![VectorRecord::new("fresh", vec![0.0, 0.0])])
        .unwrap();
    assert_eq!(writer.manifest().version, 2);

    let reopened = BorsukIndex::open_with_cache(&uri, Some(cache.path().to_path_buf())).unwrap();

    assert_eq!(reopened.manifest().version, 2);
    assert_eq!(reopened.stats().records, 1);
    assert_eq!(
        reopened
            .search_ids(&[0.0, 0.0], SearchOptions::exact(1))
            .unwrap()[0],
        "fresh"
    );
}

#[test]
fn open_with_cache_refetches_current_metadata_when_cache_is_stale() {
    let dir = tempfile::tempdir().unwrap();
    let cache = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

    let mut cached = BorsukIndex::create_with_cache(
        IndexConfig {
            uri: uri.clone(),
            metric: VectorMetric::Euclidean,
            dimensions: 2,
            segment_max_vectors: 2,
            ram_budget_bytes: None,
        },
        Some(cache.path().to_path_buf()),
    )
    .unwrap();
    cached
        .add(vec![VectorRecord::new("fresh", vec![0.0, 0.0])])
        .unwrap();
    let version = cached.manifest().version;
    let cached_manifest = cache
        .path()
        .join(format!("manifests/manifest-{version:020}.parquet"));
    assert!(
        cached_manifest.exists(),
        "setup must populate the cached active manifest"
    );
    fs::write(&cached_manifest, b"stale cached manifest bytes").unwrap();

    let reopened = BorsukIndex::open_with_cache(&uri, Some(cache.path().to_path_buf())).unwrap();

    assert_eq!(reopened.manifest().version, version);
    assert_eq!(reopened.stats().records, 1);
    assert_eq!(
        reopened
            .search_ids(&[0.0, 0.0], SearchOptions::exact(1))
            .unwrap()[0],
        "fresh"
    );
    assert_ne!(
        fs::read(cached_manifest).unwrap(),
        b"stale cached manifest bytes"
    );
}

#[test]
fn open_options_reject_too_small_runtime_ram_budget() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

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
    let uri = dir.path().to_string_lossy().into_owned();

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
    let routing_layer_index_files = collect_files_with_file_name(&routing_files, "pages.parquet");
    let routing_page_files = collect_files_with_path_component(&routing_files, "pages");

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
    assert!(
        !routing_layer_index_files.is_empty(),
        "routing layer page indexes must be persisted as parquet"
    );
    assert!(
        !routing_page_files.is_empty(),
        "routing layer pages must be persisted as parquet"
    );
    let active_routing_page_index = dir.path().join(format!(
        "routing/layers/{:020}/L0/pages.parquet",
        index.manifest().version
    ));
    let routing_page_index_batch = first_parquet_batch(&active_routing_page_index);
    for field_name in [
        "manifest_version",
        "routing_level",
        "page_ordinal",
        "page_path",
        "page_checksum",
        "page_segments",
        "leaf_segments",
        "dimensions",
        "centroid",
        "radius",
        "id_bloom",
        "level_mask",
        "page_records",
        "page_segment_bytes",
        "page_graph_bytes",
    ] {
        assert!(
            routing_page_index_batch
                .schema()
                .field_with_name(field_name)
                .is_ok(),
            "routing page index is missing field {field_name}"
        );
    }
    let routing_layer_batch = first_parquet_batch(routing_page_files[0]);
    for field_name in [
        "manifest_version",
        "routing_level",
        "page_ordinal",
        "segment_ordinal",
        "segment_level",
        "centroid",
        "leaf_mode",
    ] {
        assert!(
            routing_layer_batch
                .schema()
                .field_with_name(field_name)
                .is_ok(),
            "routing page is missing field {field_name}"
        );
    }
    assert_eq!(segment_files.len(), 2, "segments must be parquet");
    assert_eq!(graph_files.len(), 2, "local graphs must be parquet");

    let cache = tempfile::tempdir().unwrap();
    let reopened = open_resident_cached(&uri, cache.path().to_path_buf()).unwrap();
    assert_eq!(
        reopened.manifest().pivots.len(),
        reopened.manifest().segments.len(),
        "resident open_with_cache must load pivot summaries into the active manifest"
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
fn publish_writes_parent_routing_layer_indexes() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

    let mut index = BorsukIndex::create(IndexConfig {
        uri: uri.clone(),
        metric: VectorMetric::Euclidean,
        dimensions: 2,
        segment_max_vectors: 1,
        ram_budget_bytes: None,
    })
    .unwrap();

    index
        .add(
            (0..130)
                .map(|id| VectorRecord::new(format!("v{id}"), vec![id as f32, 0.0]))
                .collect(),
        )
        .unwrap();

    let l0_page_paths = routing_layer_page_index_paths(dir.path(), index.manifest().version, 0);
    assert_eq!(l0_page_paths.len(), 2);

    let l1_page_paths = routing_layer_page_index_paths(dir.path(), index.manifest().version, 1);
    assert_eq!(
        l1_page_paths.len(),
        1,
        "130 leaf summaries should roll up into one parent routing page"
    );
    assert!(
        l1_page_paths[0].starts_with("routing/pages/L1/"),
        "parent routing page must be stored as a content-addressed L1 routing object"
    );
    assert_is_parquet_file(&dir.path().join(&l1_page_paths[0]));

    let l1_index_path = dir.path().join(format!(
        "routing/layers/{:020}/L1/pages.parquet",
        index.manifest().version
    ));
    let l1_batch = first_parquet_batch(&l1_index_path);
    assert_eq!(l1_batch.num_rows(), 1);
    assert_eq!(
        routing_layer_page_index_page_records(dir.path(), index.manifest().version, 1)[0],
        130
    );
    assert_eq!(
        routing_layer_page_index_leaf_segments(dir.path(), index.manifest().version, 1)[0],
        130
    );
}

#[test]
fn stats_expose_computed_routing_max_level() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

    let mut index = BorsukIndex::create(IndexConfig {
        uri,
        metric: VectorMetric::Euclidean,
        dimensions: 2,
        segment_max_vectors: 1,
        ram_budget_bytes: None,
    })
    .unwrap();

    index
        .add(
            (0..130)
                .map(|id| VectorRecord::new(format!("v{id}"), vec![id as f32, 0.0]))
                .collect(),
        )
        .unwrap();

    let stats = index.stats();
    assert_eq!(stats.routing_page_fanout, 128);
    assert_eq!(stats.routing_max_level, 1);
    assert_eq!(stats.routing_leaf_pages, 2);
    assert_eq!(stats.routing_pages, 3);
}

#[test]
fn routing_page_fanout_is_configurable_and_persisted() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

    let mut index = BorsukIndex::create_with_routing_page_fanout(
        IndexConfig {
            uri: uri.clone(),
            metric: VectorMetric::Euclidean,
            dimensions: 2,
            segment_max_vectors: 1,
            ram_budget_bytes: None,
        },
        4,
    )
    .unwrap();

    index
        .add(
            (0..17)
                .map(|id| VectorRecord::new(format!("v{id}"), vec![id as f32, 0.0]))
                .collect(),
        )
        .unwrap();

    let stats = index.stats();
    assert_eq!(stats.routing_page_fanout, 4);
    assert_eq!(stats.routing_max_level, 2);
    assert_eq!(stats.routing_leaf_pages, 5);
    assert_eq!(stats.routing_pages, 8);

    drop(index);
    let reopened = BorsukIndex::open_with_options(
        &uri,
        OpenOptions {
            resident_routing: false,
            ..OpenOptions::default()
        },
    )
    .unwrap();
    let reopened_stats = reopened.stats();
    assert_eq!(reopened_stats.routing_page_fanout, 4);
    assert_eq!(reopened_stats.routing_max_level, 2);
    assert_eq!(reopened_stats.routing_leaf_pages, 5);
    assert_eq!(reopened_stats.routing_pages, 8);
}

#[test]
fn add_with_report_counts_written_objects_and_reused_routing_pages() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

    let mut index = BorsukIndex::create_with_routing_page_fanout(
        IndexConfig {
            uri,
            metric: VectorMetric::Euclidean,
            dimensions: 2,
            segment_max_vectors: 1,
            ram_budget_bytes: None,
        },
        2,
    )
    .unwrap();

    let before = storage_file_sizes(dir.path());
    let (ids, report) = index
        .add_with_report(
            (0..4).map(|value| vec![value as f32, 0.0]).collect(),
            Some((0..4).map(|value| format!("v{value}")).collect()),
        )
        .unwrap();
    let after = storage_file_sizes(dir.path());

    assert_eq!(ids, ["v0", "v1", "v2", "v3"]);
    assert_add_report_matches_storage_delta(dir.path(), &before, &after, &report, 4);
    assert_eq!(report.segments_written, 4);
    assert_eq!(report.graph_payloads_written, 4);

    let before_pages = routing_page_paths_in_storage(dir.path());
    let before = storage_file_sizes(dir.path());
    let (ids, report) = index
        .add_with_report(vec![vec![4.0, 0.0]], Some(vec!["v4".to_string()]))
        .unwrap();
    let after = storage_file_sizes(dir.path());
    let after_pages = routing_page_paths_in_storage(dir.path());

    assert_eq!(ids, ["v4"]);
    assert_add_report_matches_storage_delta(dir.path(), &before, &after, &report, 1);
    assert_eq!(report.segments_written, 1);
    assert_eq!(report.graph_payloads_written, 1);
    assert_eq!(
        report.routing_pages_written,
        after_pages.len() - before_pages.len(),
        "reused content-addressed routing pages must not be counted as written"
    );
    assert!(
        report.routing_pages_written < index.stats().routing_pages,
        "second add must report only newly written pages, not all live pages"
    );
}

#[test]
fn graph_neighbors_is_configurable_validated_and_persisted() {
    let invalid_dir = tempfile::tempdir().unwrap();
    let invalid_err = BorsukIndex::create_with_graph_neighbors(
        IndexConfig {
            uri: invalid_dir.path().to_string_lossy().into_owned(),
            metric: VectorMetric::Euclidean,
            dimensions: 2,
            segment_max_vectors: 8,
            ram_budget_bytes: None,
        },
        0,
    )
    .unwrap_err();
    assert!(
        invalid_err
            .to_string()
            .contains("graph_neighbors must be greater than zero"),
        "{invalid_err}"
    );

    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();
    let mut index = BorsukIndex::create_with_graph_neighbors(
        IndexConfig {
            uri: uri.clone(),
            metric: VectorMetric::Euclidean,
            dimensions: 2,
            segment_max_vectors: 8,
            ram_budget_bytes: None,
        },
        2,
    )
    .unwrap();
    assert_eq!(index.graph_neighbors(), 2);

    index
        .add(
            (0..5)
                .map(|value| VectorRecord::new(format!("v{value}"), vec![value as f32, 0.0]))
                .collect(),
        )
        .unwrap();

    let graph_path = collect_files_with_extension(dir.path().join("graphs/L0"), "parquet")
        .into_iter()
        .next()
        .expect("add must write a graph payload");
    let batch = first_parquet_batch(&graph_path);
    let source_indexes = batch
        .column(
            batch
                .schema()
                .index_of("source_record_index")
                .expect("graph table must include source_record_index"),
        )
        .as_any()
        .downcast_ref::<UInt64Array>()
        .expect("source_record_index must be a u64 column");
    let mut out_degrees = BTreeMap::<u64, usize>::new();
    for row in 0..batch.num_rows() {
        *out_degrees.entry(source_indexes.value(row)).or_default() += 1;
    }
    assert!(
        out_degrees.values().all(|degree| *degree <= 2),
        "graph out-degree must honor configured graph_neighbors: {out_degrees:?}"
    );

    drop(index);
    let reopened = BorsukIndex::open(&uri).unwrap();
    assert_eq!(reopened.graph_neighbors(), 2);
}

#[test]
fn approximate_search_reads_persisted_routing_layer_pages() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

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
        ])
        .unwrap();

    let reopened = BorsukIndex::open(&uri).unwrap();
    let routing_page_files =
        collect_files_with_extension(dir.path().join("routing/pages"), "parquet");
    assert!(!routing_page_files.is_empty());
    fs::write(&routing_page_files[0], b"corrupt routing layer page").unwrap();

    let err = reopened
        .search_with_report(
            &[0.0, 0.0],
            SearchOptions::approx(1, LeafMode::PqScan).with_max_segments(1),
        )
        .unwrap_err();

    assert!(
        err.to_string().contains("routing layer page"),
        "unexpected error: {err}"
    );
}

#[test]
fn approximate_search_skips_unrelated_routing_leaf_pages() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

    let mut index = BorsukIndex::create(IndexConfig {
        uri: uri.clone(),
        metric: VectorMetric::Euclidean,
        dimensions: 2,
        segment_max_vectors: 1,
        ram_budget_bytes: None,
    })
    .unwrap();

    let mut records = (0..128)
        .map(|id| VectorRecord::new(format!("far-{id}"), vec![1000.0 + id as f32, 0.0]))
        .collect::<Vec<_>>();
    records.push(VectorRecord::new("near-a", vec![0.0, 0.0]));
    records.push(VectorRecord::new("near-b", vec![0.1, 0.0]));
    index.add(records).unwrap();

    let page_refs = routing_layer_page_index_paths(dir.path(), index.manifest().version, 0);
    assert_eq!(page_refs.len(), 2);
    fs::write(
        dir.path().join(&page_refs[0]),
        b"corrupt unrelated routing leaf page",
    )
    .unwrap();

    let reopened = open_resident(&uri).unwrap();
    let report = reopened
        .search_with_report(
            &[0.0, 0.0],
            SearchOptions::approx(1, LeafMode::PqScan).with_max_segments(1),
        )
        .unwrap();

    assert_eq!(report.hits[0].id, "near-a");
    assert_eq!(report.segments_total, 130);
    assert_eq!(report.segments_searched, 1);
    assert_eq!(report.routing_page_indexes_read, 1);
    assert_eq!(report.routing_pages_read, 1);
}

#[test]
fn approximate_search_walks_parent_routing_pages_without_l0_index() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

    let mut index = BorsukIndex::create(IndexConfig {
        uri: uri.clone(),
        metric: VectorMetric::Euclidean,
        dimensions: 2,
        segment_max_vectors: 1,
        ram_budget_bytes: None,
    })
    .unwrap();

    let mut records = (0..128)
        .map(|id| VectorRecord::new(format!("far-{id}"), vec![1000.0 + id as f32, 0.0]))
        .collect::<Vec<_>>();
    records.push(VectorRecord::new("near-a", vec![0.0, 0.0]));
    records.push(VectorRecord::new("near-b", vec![0.1, 0.0]));
    index.add(records).unwrap();

    let l1_page_paths = routing_layer_page_index_paths(dir.path(), index.manifest().version, 1);
    assert_eq!(l1_page_paths.len(), 1);

    let l0_index_path = dir.path().join(format!(
        "routing/layers/{:020}/L0/pages.parquet",
        index.manifest().version
    ));
    fs::write(l0_index_path, b"corrupt global L0 routing page index").unwrap();

    let reopened = BorsukIndex::open_with_options(
        &uri,
        OpenOptions {
            resident_routing: false,
            ..OpenOptions::default()
        },
    )
    .unwrap();
    let report = reopened
        .search_with_report(
            &[0.0, 0.0],
            SearchOptions::approx(1, LeafMode::PqScan).with_max_segments(1),
        )
        .unwrap();

    assert_eq!(report.hits[0].id, "near-a");
    assert_eq!(report.segments_total, 130);
    assert_eq!(report.segments_searched, 1);
}

#[test]
fn approximate_search_reports_segments_skipped_by_routing_page_pruning() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

    let mut index = BorsukIndex::create(IndexConfig {
        uri: uri.clone(),
        metric: VectorMetric::Euclidean,
        dimensions: 2,
        segment_max_vectors: 1,
        ram_budget_bytes: None,
    })
    .unwrap();

    let mut records = (0..128)
        .map(|id| VectorRecord::new(format!("far-{id}"), vec![1000.0 + id as f32, 0.0]))
        .collect::<Vec<_>>();
    records.push(VectorRecord::new("near-a", vec![0.0, 0.0]));
    records.push(VectorRecord::new("near-b", vec![0.1, 0.0]));
    index.add(records).unwrap();

    let reopened = BorsukIndex::open(&uri).unwrap();
    let report = reopened
        .search_with_report(
            &[0.0, 0.0],
            SearchOptions::approx(2, LeafMode::PqScan).with_max_segments(128),
        )
        .unwrap();

    assert_eq!(report.hits[0].id, "near-a");
    assert_eq!(report.segments_total, 130);
    assert_eq!(report.segments_searched, 2);
    assert_eq!(report.segments_skipped, 128);
}

#[test]
fn recall_guarantee_degrades_when_candidate_budget_loses_recall() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

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
            VectorRecord::new("a-far", vec![100.0, 100.0]),
            VectorRecord::new("z-near", vec![1.0, 1.0]),
            VectorRecord::new("zz-far", vec![101.0, 101.0]),
        ])
        .unwrap();

    let exact_ids = hit_ids(
        index
            .search_with_report(&[0.0, 0.0], SearchOptions::exact(1))
            .unwrap(),
    );
    let approx_report = index
        .search_with_report(
            &[0.0, 0.0],
            SearchOptions::approx(1, LeafMode::FlatScan).with_max_candidates_per_segment(1),
        )
        .unwrap();
    let approx_ids = hit_ids(approx_report.clone());

    assert_eq!(approx_report.recall_guarantee, RecallGuarantee::Degraded);
    assert!(recall_overlap(&exact_ids, &approx_ids, 1) < 1.0);
    assert_eq!(
        approx_report.termination_reason,
        SearchTerminationReason::Complete
    );
}

#[test]
fn recall_guarantee_degrades_when_routing_preselection_skips_segments() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

    let mut index = BorsukIndex::create(IndexConfig {
        uri: uri.clone(),
        metric: VectorMetric::Euclidean,
        dimensions: 2,
        segment_max_vectors: 1,
        ram_budget_bytes: None,
    })
    .unwrap();

    let mut records = (0..128)
        .map(|id| VectorRecord::new(format!("far-{id}"), vec![1000.0 + id as f32, 0.0]))
        .collect::<Vec<_>>();
    records.push(VectorRecord::new("near-a", vec![0.0, 0.0]));
    records.push(VectorRecord::new("near-b", vec![0.1, 0.0]));
    index.add(records).unwrap();

    let reopened = BorsukIndex::open(&uri).unwrap();
    let report = reopened
        .search_with_report(
            &[0.0, 0.0],
            SearchOptions::approx(2, LeafMode::PqScan).with_max_segments(128),
        )
        .unwrap();

    assert_eq!(report.termination_reason, SearchTerminationReason::Complete);
    assert_eq!(report.segments_skipped, 128);
    assert_eq!(report.recall_guarantee, RecallGuarantee::Degraded);
}

#[test]
fn guaranteed_recall_disables_routing_preselection_pruning() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

    let mut index = BorsukIndex::create(IndexConfig {
        uri: uri.clone(),
        metric: VectorMetric::Euclidean,
        dimensions: 2,
        segment_max_vectors: 1,
        ram_budget_bytes: None,
    })
    .unwrap();

    let mut records = (0..128)
        .map(|id| VectorRecord::new(format!("far-{id}"), vec![1000.0 + id as f32, 0.0]))
        .collect::<Vec<_>>();
    records.push(VectorRecord::new("near-a", vec![0.0, 0.0]));
    records.push(VectorRecord::new("near-b", vec![0.1, 0.0]));
    index.add(records).unwrap();

    let reopened = BorsukIndex::open(&uri).unwrap();
    let default_report = reopened
        .search_with_report(
            &[0.0, 0.0],
            SearchOptions::approx(2, LeafMode::PqScan).with_max_segments(1000),
        )
        .unwrap();
    let guaranteed_report = reopened
        .search_with_report(
            &[0.0, 0.0],
            SearchOptions::approx(2, LeafMode::PqScan)
                .with_max_segments(1000)
                .with_guaranteed_recall(),
        )
        .unwrap();

    assert!(default_report.segments_skipped > 0);
    assert_eq!(default_report.recall_guarantee, RecallGuarantee::Degraded);

    assert_eq!(
        guaranteed_report.termination_reason,
        SearchTerminationReason::Complete
    );
    assert_eq!(guaranteed_report.segments_skipped, 0);
    assert_eq!(guaranteed_report.segments_searched, 130);
    assert_eq!(
        guaranteed_report.recall_guarantee,
        RecallGuarantee::BudgetComplete
    );
    assert_eq!(
        hit_ids(guaranteed_report),
        vec!["near-a".to_string(), "near-b".to_string()]
    );
}

#[test]
fn recall_guarantee_reports_budget_complete_for_full_approximate_coverage() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

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
            VectorRecord::new("c", vec![2.0, 0.0]),
        ])
        .unwrap();

    let report = index
        .search_with_report(&[0.1, 0.0], SearchOptions::approx(2, LeafMode::PqScan))
        .unwrap();

    assert_eq!(report.termination_reason, SearchTerminationReason::Complete);
    assert_eq!(report.segments_skipped, 0);
    assert_eq!(report.recall_guarantee, RecallGuarantee::BudgetComplete);
}

#[test]
fn recall_guarantee_reports_exact_for_exact_search() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

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
        ])
        .unwrap();

    let report = index
        .search_with_report(&[0.0, 0.0], SearchOptions::exact(1))
        .unwrap();

    assert_eq!(report.recall_guarantee, RecallGuarantee::Exact);
}

#[test]
fn guaranteed_recall_returns_error_when_hard_budget_would_degrade() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

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
        ])
        .unwrap();

    let err = index
        .search_with_report(
            &[0.0, 0.0],
            SearchOptions::approx(1, LeafMode::PqScan)
                .with_max_segments(1)
                .with_guaranteed_recall(),
        )
        .unwrap_err();

    assert!(matches!(
        err,
        BorsukError::RecallGuaranteeViolated {
            reason: SearchTerminationReason::MaxSegments
        }
    ));
    assert_eq!(err.code(), "recall_guarantee_violated");
}

#[test]
fn guaranteed_recall_disables_candidate_truncation() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

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
            VectorRecord::new("a-far", vec![100.0, 100.0]),
            VectorRecord::new("z-near", vec![1.0, 1.0]),
            VectorRecord::new("zz-far", vec![101.0, 101.0]),
        ])
        .unwrap();

    let exact_ids = hit_ids(
        index
            .search_with_report(&[0.0, 0.0], SearchOptions::exact(1))
            .unwrap(),
    );
    let default_approx_ids = hit_ids(
        index
            .search_with_report(
                &[0.0, 0.0],
                SearchOptions::approx(1, LeafMode::FlatScan).with_max_candidates_per_segment(1),
            )
            .unwrap(),
    );
    let guaranteed_report = index
        .search_with_report(
            &[0.0, 0.0],
            SearchOptions::approx(1, LeafMode::FlatScan)
                .with_max_candidates_per_segment(1)
                .with_guaranteed_recall(),
        )
        .unwrap();
    let guaranteed_ids = hit_ids(guaranteed_report.clone());

    assert!(recall_overlap(&exact_ids, &default_approx_ids, 1) < 1.0);
    assert_eq!(recall_overlap(&exact_ids, &guaranteed_ids, 1), 1.0);
    assert_eq!(
        guaranteed_report.recall_guarantee,
        RecallGuarantee::BudgetComplete
    );
}

#[test]
fn approximate_search_opens_with_empty_full_routing_table_when_pages_exist() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

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
            VectorRecord::new("c", vec![9.0, 0.0]),
        ])
        .unwrap();
    rewrite_current_with_empty_routing_table(dir.path(), index.manifest());

    let reopened = BorsukIndex::open(&uri).unwrap();
    assert!(
        reopened.manifest().segments.is_empty(),
        "open should not materialize full segment summaries when the routing table is empty"
    );

    let report = reopened
        .search_with_report(
            &[0.0, 0.0],
            SearchOptions::approx(1, LeafMode::PqScan).with_max_segments(1),
        )
        .unwrap();

    assert_eq!(report.hits[0].id, "a");
    assert_eq!(report.segments_total, 3);
    assert_eq!(report.segments_searched, 1);
}

#[test]
fn search_report_counts_routing_page_bytes_when_routing_table_is_empty() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

    let mut index = BorsukIndex::create(IndexConfig {
        uri: uri.clone(),
        metric: VectorMetric::Euclidean,
        dimensions: 2,
        segment_max_vectors: 1,
        ram_budget_bytes: None,
    })
    .unwrap();

    let mut records = (0..128)
        .map(|id| VectorRecord::new(format!("far-{id}"), vec![1000.0 + id as f32, 0.0]))
        .collect::<Vec<_>>();
    records.push(VectorRecord::new("near-a", vec![0.0, 0.0]));
    records.push(VectorRecord::new("near-b", vec![0.1, 0.0]));
    index.add(records).unwrap();

    let selected_segment_bytes = index.manifest().segments[128].size_bytes;
    let page_refs = routing_layer_page_index_paths(dir.path(), index.manifest().version, 0);
    assert_eq!(page_refs.len(), 2);
    let selected_routing_page_bytes = fs::metadata(dir.path().join(&page_refs[1])).unwrap().len();
    let routing_page_index_bytes = fs::metadata(dir.path().join(format!(
        "routing/layers/{:020}/L0/pages.parquet",
        index.manifest().version
    )))
    .unwrap()
    .len();
    rewrite_current_with_empty_routing_table(dir.path(), index.manifest());

    let reopened = BorsukIndex::open(&uri).unwrap();
    assert!(reopened.manifest().segments.is_empty());

    let report = reopened
        .search_with_report(
            &[0.0, 0.0],
            SearchOptions::approx(1, LeafMode::PqScan).with_max_segments(1),
        )
        .unwrap();

    assert_eq!(report.hits[0].id, "near-a");
    assert_eq!(report.segments_searched, 1);
    assert!(
        report.bytes_read
            >= selected_segment_bytes + selected_routing_page_bytes + routing_page_index_bytes,
        "bytes_read should include routing page index bytes, routing page bytes, and selected segment bytes; got {}, selected segment was {}, routing page was {}, index was {}",
        report.bytes_read,
        selected_segment_bytes,
        selected_routing_page_bytes,
        routing_page_index_bytes
    );
}

#[test]
fn get_vector_uses_routing_pages_when_full_routing_table_is_empty() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

    let mut index = BorsukIndex::create(IndexConfig {
        uri: uri.clone(),
        metric: VectorMetric::Euclidean,
        dimensions: 2,
        segment_max_vectors: 1,
        ram_budget_bytes: None,
    })
    .unwrap();

    let mut records = (0..128)
        .map(|id| VectorRecord::new(format!("far-{id}"), vec![1000.0 + id as f32, 0.0]))
        .collect::<Vec<_>>();
    records.push(VectorRecord::new("target", vec![1.0, 2.0]));
    index.add(records).unwrap();

    let page_refs = routing_layer_page_index_paths(dir.path(), index.manifest().version, 0);
    assert_eq!(page_refs.len(), 2);
    fs::write(
        dir.path().join(&page_refs[0]),
        b"corrupt unrelated routing page for get_vector",
    )
    .unwrap();
    rewrite_current_with_empty_routing_table(dir.path(), index.manifest());

    let reopened = BorsukIndex::open(&uri).unwrap();
    assert!(reopened.manifest().segments.is_empty());

    let vector = reopened.get_vector("target").unwrap();
    assert_eq!(vector, Some(vec![1.0, 2.0]));
    assert_eq!(reopened.get_vector("missing").unwrap(), None);
}

#[test]
fn add_after_empty_routing_table_preserves_existing_routing_pages() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

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
            VectorRecord::new("old-a", vec![0.0, 0.0]),
            VectorRecord::new("old-b", vec![1.0, 0.0]),
        ])
        .unwrap();
    rewrite_current_with_empty_routing_table(dir.path(), index.manifest());

    let mut reopened = BorsukIndex::open(&uri).unwrap();
    assert!(reopened.manifest().segments.is_empty());

    reopened
        .add(vec![VectorRecord::new("new", vec![2.0, 0.0])])
        .unwrap();

    assert!(
        reopened.manifest().segments.is_empty(),
        "append in non-resident mode should keep segment summaries out of the manifest"
    );
    assert_eq!(reopened.get_vector("old-a").unwrap(), Some(vec![0.0, 0.0]));
    assert_eq!(reopened.get_vector("new").unwrap(), Some(vec![2.0, 0.0]));
    assert_eq!(reopened.try_stats().unwrap().records, 3);
    assert_eq!(
        reopened
            .search_ids(
                &[0.0, 0.0],
                SearchOptions::approx(1, LeafMode::PqScan).with_max_segments(1),
            )
            .unwrap(),
        ["old-a"]
    );
}

#[test]
fn generated_id_add_after_empty_routing_table_does_not_read_unrelated_parent_pages() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

    let mut index = BorsukIndex::create(IndexConfig {
        uri: uri.clone(),
        metric: VectorMetric::Euclidean,
        dimensions: 2,
        segment_max_vectors: 1,
        ram_budget_bytes: None,
    })
    .unwrap();

    let records = (0..129)
        .map(|id| VectorRecord::new(format!("old-{id}"), vec![id as f32, 0.0]))
        .collect::<Vec<_>>();
    index.add(records).unwrap();
    assert_eq!(
        routing_max_level_for_version(dir.path(), index.manifest().version),
        1
    );

    let top_parent_paths = routing_layer_page_index_paths(dir.path(), index.manifest().version, 1);
    assert_eq!(top_parent_paths.len(), 1);
    fs::write(
        dir.path().join(&top_parent_paths[0]),
        b"corrupt old parent page that append must not read",
    )
    .unwrap();
    rewrite_current_with_empty_routing_table(dir.path(), index.manifest());

    let mut reopened = BorsukIndex::open(&uri).unwrap();
    assert!(reopened.manifest().segments.is_empty());

    let ids = reopened.add_vectors(vec![vec![999.0, 0.0]]).unwrap();

    assert_eq!(ids, ["0"]);
    assert!(
        reopened.manifest().segments.is_empty(),
        "append in non-resident mode should keep segment summaries out of the manifest"
    );
    assert_eq!(reopened.get_vector("0").unwrap(), Some(vec![999.0, 0.0]));
}

#[test]
fn generated_id_add_after_empty_routing_table_reuses_rightmost_append_parent() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

    let mut index = BorsukIndex::create(IndexConfig {
        uri: uri.clone(),
        metric: VectorMetric::Euclidean,
        dimensions: 2,
        segment_max_vectors: 1,
        ram_budget_bytes: None,
    })
    .unwrap();

    let records = (0..129)
        .map(|id| VectorRecord::new(format!("old-{id}"), vec![id as f32, 0.0]))
        .collect::<Vec<_>>();
    index.add(records).unwrap();
    let top_parent_paths = routing_layer_page_index_paths(dir.path(), index.manifest().version, 1);
    assert_eq!(top_parent_paths.len(), 1);
    fs::write(
        dir.path().join(&top_parent_paths[0]),
        b"corrupt old parent page that repeated append must not read",
    )
    .unwrap();
    rewrite_current_with_empty_routing_table(dir.path(), index.manifest());

    let mut reopened = BorsukIndex::open(&uri).unwrap();
    assert_eq!(reopened.add_vectors(vec![vec![999.0, 0.0]]).unwrap(), ["0"]);
    assert_eq!(
        routing_layer_page_index_paths(dir.path(), reopened.manifest().version, 1).len(),
        2,
        "first sparse append should create one append parent beside the cold parent"
    );

    assert_eq!(
        reopened.add_vectors(vec![vec![1000.0, 0.0]]).unwrap(),
        ["1"]
    );

    assert_eq!(
        routing_layer_page_index_paths(dir.path(), reopened.manifest().version, 1).len(),
        2,
        "repeated small appends should reuse the rightmost append parent instead of growing one top ref per add"
    );
    assert_eq!(reopened.get_vector("0").unwrap(), Some(vec![999.0, 0.0]));
    assert_eq!(reopened.get_vector("1").unwrap(), Some(vec![1000.0, 0.0]));
}

#[test]
fn add_after_empty_routing_table_rejects_duplicate_ids_through_routing_pages() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

    let mut index = BorsukIndex::create(IndexConfig {
        uri: uri.clone(),
        metric: VectorMetric::Euclidean,
        dimensions: 2,
        segment_max_vectors: 1,
        ram_budget_bytes: None,
    })
    .unwrap();

    let mut records = (0..128)
        .map(|id| VectorRecord::new(format!("far-{id}"), vec![1000.0 + id as f32, 0.0]))
        .collect::<Vec<_>>();
    records.push(VectorRecord::new("dup", vec![0.0, 0.0]));
    index.add(records).unwrap();

    let page_refs = routing_layer_page_index_paths(dir.path(), index.manifest().version, 0);
    assert_eq!(page_refs.len(), 2);
    fs::write(
        dir.path().join(&page_refs[0]),
        b"corrupt unrelated routing page for duplicate validation",
    )
    .unwrap();
    rewrite_current_with_empty_routing_table(dir.path(), index.manifest());

    let mut reopened = BorsukIndex::open(&uri).unwrap();
    assert!(reopened.manifest().segments.is_empty());

    let err = reopened
        .add(vec![VectorRecord::new("dup", vec![9.0, 0.0])])
        .unwrap_err();

    assert!(
        err.to_string().contains("duplicate record id"),
        "unexpected error: {err}"
    );
}

#[test]
fn gc_preserves_active_objects_when_full_routing_table_is_empty() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

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
        ])
        .unwrap();
    rewrite_current_with_empty_routing_table(dir.path(), index.manifest());

    let mut reopened = BorsukIndex::open(&uri).unwrap();
    assert!(reopened.manifest().segments.is_empty());

    let deleted = reopened
        .gc_obsolete_segments(GarbageCollectionOptions {
            dry_run: false,
            min_age: Duration::ZERO,
        })
        .unwrap();

    assert_eq!(deleted.objects_deleted, 4);
    assert_eq!(deleted.routing_objects_deleted, 1);
    assert_eq!(deleted.tables_deleted, 3);
    assert_eq!(deleted.routing_page_indexes_read, 1);
    assert_eq!(deleted.routing_pages_read, 1);
    assert!(deleted.bytes_read > 0);
    assert_eq!(deleted.object_cache_hits, 0);
    assert_eq!(deleted.object_cache_misses, 2);
    assert_eq!(reopened.get_vector("a").unwrap(), Some(vec![0.0, 0.0]));
    assert_eq!(
        reopened
            .search_ids(
                &[0.0, 0.0],
                SearchOptions::approx(1, LeafMode::PqScan).with_max_segments(1),
            )
            .unwrap(),
        ["a"]
    );
}

#[test]
fn gc_with_zero_retention_removes_non_current_routing_and_table_objects() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

    let mut index = BorsukIndex::create_with_routing_page_fanout(
        IndexConfig {
            uri: uri.clone(),
            metric: VectorMetric::Euclidean,
            dimensions: 2,
            segment_max_vectors: 1,
            ram_budget_bytes: None,
        },
        2,
    )
    .unwrap();

    index
        .add(
            (0..8)
                .map(|id| VectorRecord::new(format!("v{id}"), vec![id as f32, 0.0]))
                .collect(),
        )
        .unwrap();
    index
        .compact(CompactionOptions {
            source_level: 0,
            target_level: 1,
            max_segments: Some(8),
            min_segments: 2,
            target_segment_max_vectors: Some(2),
            target_segment_max_radius: None,
        })
        .unwrap();
    index
        .compact(CompactionOptions {
            source_level: 1,
            target_level: 2,
            max_segments: Some(4),
            min_segments: 2,
            target_segment_max_vectors: Some(4),
            target_segment_max_radius: None,
        })
        .unwrap();

    let current_version = index.manifest().version;
    let expected_tables = current_metadata_table_paths(current_version);
    let expected_layer_indexes = current_routing_layer_index_paths(dir.path(), current_version);
    let expected_routing_pages = current_routing_page_paths(dir.path(), current_version);
    assert!(metadata_table_paths(dir.path()).len() > expected_tables.len());
    assert!(routing_layer_index_paths_in_storage(dir.path()).len() > expected_layer_indexes.len());
    assert!(routing_page_paths_in_storage(dir.path()).len() > expected_routing_pages.len());

    let deleted = index
        .gc_obsolete_segments(GarbageCollectionOptions {
            dry_run: false,
            min_age: Duration::ZERO,
        })
        .unwrap();

    assert!(deleted.tables_deleted > 0);
    assert!(deleted.routing_objects_deleted > 0);
    assert_eq!(metadata_table_paths(dir.path()), expected_tables);
    assert_eq!(
        routing_layer_index_paths_in_storage(dir.path()),
        expected_layer_indexes
    );
    assert_eq!(
        routing_page_paths_in_storage(dir.path()),
        expected_routing_pages
    );

    let reopened = BorsukIndex::open_with_options(
        &uri,
        OpenOptions {
            resident_routing: false,
            ..OpenOptions::default()
        },
    )
    .unwrap();
    assert!(reopened.manifest().segments.is_empty());
    assert_eq!(
        reopened
            .search_ids(&[6.1, 0.0], SearchOptions::exact(2))
            .unwrap(),
        ["v6", "v7"]
    );
}

#[test]
fn gc_refreshes_current_before_delete_from_stale_handle() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

    let mut index = BorsukIndex::create(IndexConfig {
        uri: uri.clone(),
        metric: VectorMetric::Euclidean,
        dimensions: 2,
        segment_max_vectors: 1,
        ram_budget_bytes: None,
    })
    .unwrap();
    index
        .add(vec![VectorRecord::new("base", vec![0.0, 0.0])])
        .unwrap();

    let mut stale = BorsukIndex::open(&uri).unwrap();
    let stale_version = stale.manifest().version;
    let mut writer = BorsukIndex::open(&uri).unwrap();
    writer
        .add(vec![VectorRecord::new("new-current", vec![10.0, 0.0])])
        .unwrap();
    assert_eq!(writer.manifest().version, stale_version + 1);

    stale
        .gc_obsolete_segments(GarbageCollectionOptions {
            dry_run: false,
            min_age: Duration::ZERO,
        })
        .unwrap();
    assert_eq!(stale.manifest().version, writer.manifest().version);

    let reopened = BorsukIndex::open(&uri).unwrap();
    assert_eq!(
        reopened
            .search_ids(&[0.0, 0.0], SearchOptions::exact(1))
            .unwrap(),
        ["base"]
    );
    assert_eq!(
        reopened
            .search_ids(&[10.0, 0.0], SearchOptions::exact(1))
            .unwrap(),
        ["new-current"]
    );
}

#[test]
fn gc_dry_run_reports_publish_orphans_newer_than_current() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let setup_store: Arc<dyn ObjectStore> =
        Arc::new(common::FaultInjectingObjectStore::new(Arc::clone(&inner)));
    let mut setup = BorsukIndex::create_with_object_store(
        setup_store,
        IndexConfig {
            uri: "memory:///gc-orphan".to_string(),
            metric: VectorMetric::Euclidean,
            dimensions: 2,
            segment_max_vectors: 2,
            ram_budget_bytes: None,
        },
    )
    .unwrap();
    setup
        .add(vec![VectorRecord::new("base", vec![0.0, 0.0])])
        .unwrap();

    let faulting_store: Arc<dyn ObjectStore> =
        Arc::new(common::FaultInjectingObjectStore::fail_nth_matching(
            Arc::clone(&inner),
            1,
            true,
            |operation, path| {
                operation == common::StoreOperation::Put && path.as_ref() == "CURRENT"
            },
        ));
    let mut crashing =
        BorsukIndex::open_with_object_store(faulting_store, "memory:///gc-orphan").unwrap();
    crashing
        .add(vec![VectorRecord::new("orphaned", vec![1.0, 0.0])])
        .unwrap_err();

    let mut reopened =
        BorsukIndex::open_with_object_store(Arc::clone(&inner), "memory:///gc-orphan").unwrap();
    assert_eq!(reopened.manifest().version, 2);
    let dry_run = reopened
        .gc_obsolete_segments(GarbageCollectionOptions {
            dry_run: true,
            min_age: Duration::ZERO,
        })
        .unwrap();

    for path in [
        "routing/layers/00000000000000000003/L0/pages.parquet",
        "manifests/manifest-00000000000000000003.parquet",
        "routing/segments-00000000000000000003.parquet",
        "routing/pivots-00000000000000000003.parquet",
    ] {
        assert!(
            dry_run.candidates.iter().any(|candidate| candidate == path),
            "dry-run GC candidates should include orphan `{path}`"
        );
    }
    assert!(
        dry_run
            .candidates
            .iter()
            .any(|candidate| candidate.starts_with("routing/pages/")),
        "dry-run GC should report orphaned routing page content"
    );
    assert_eq!(dry_run.objects_deleted, 0);
    assert_eq!(dry_run.routing_objects_deleted, 0);
    assert_eq!(dry_run.tables_deleted, 0);
}

#[test]
fn current_rejects_valid_manifest_table_swapped_under_active_version() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

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
fn current_rejects_pivot_table_manifest_version_mismatch() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

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

    rewrite_current_pivots_manifest_version(dir.path(), index.manifest(), 99);

    let err = open_resident(&uri).unwrap_err();

    assert!(
        err.to_string().contains("pivot table manifest_version"),
        "unexpected error: {err}"
    );
}

#[test]
fn search_rejects_segment_object_size_mismatch() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

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
    let summary = &index.manifest().segments[0];
    rewrite_current_routing_sizes(
        dir.path(),
        index.manifest(),
        Some(summary.size_bytes + 1),
        None,
    );

    let reopened = BorsukIndex::open(&uri).unwrap();
    let err = reopened
        .search_ids(&[0.0, 0.0], SearchOptions::exact(1))
        .unwrap_err();

    assert!(
        err.to_string().contains("segment object size mismatch"),
        "unexpected error: {err}"
    );
}

#[test]
fn search_rejects_segment_object_count_mismatch() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

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
    let summary = &index.manifest().segments[0];
    rewrite_current_routing_metadata(
        dir.path(),
        index.manifest(),
        None,
        Some(summary.object_count as u64 + 1),
        None,
        None,
        None,
    );

    let reopened = BorsukIndex::open(&uri).unwrap();
    let err = reopened
        .search_ids(&[0.0, 0.0], SearchOptions::exact(1))
        .unwrap_err();

    assert!(
        err.to_string().contains("segment object_count mismatch"),
        "unexpected error: {err}"
    );
}

#[test]
fn search_rejects_segment_metadata_id_mismatch() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

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
    rewrite_current_routing_metadata(
        dir.path(),
        index.manifest(),
        Some("routing-id-does-not-match-segment"),
        None,
        None,
        None,
        None,
    );

    let reopened = BorsukIndex::open(&uri).unwrap();
    let err = reopened
        .search_ids(&[0.0, 0.0], SearchOptions::exact(1))
        .unwrap_err();

    assert!(
        err.to_string().contains("segment metadata id mismatch"),
        "unexpected error: {err}"
    );
}

#[test]
fn graph_search_rejects_graph_object_size_mismatch() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

    let mut index = BorsukIndex::create(IndexConfig {
        uri: uri.clone(),
        metric: VectorMetric::Euclidean,
        dimensions: 2,
        segment_max_vectors: 4,
        ram_budget_bytes: None,
    })
    .unwrap();

    index
        .add(vec![
            VectorRecord::new("entry", vec![0.0, 0.0]),
            VectorRecord::new("near", vec![0.0, 0.1]),
            VectorRecord::new("far-a", vec![10.0, 10.0]),
            VectorRecord::new("far-b", vec![11.0, 11.0]),
        ])
        .unwrap();
    let summary = &index.manifest().segments[0];
    rewrite_current_routing_sizes(
        dir.path(),
        index.manifest(),
        None,
        Some(summary.graph_size_bytes + 1),
    );

    let reopened = BorsukIndex::open(&uri).unwrap();
    let err = reopened
        .search_with_report(
            &[0.0, 0.0],
            SearchOptions {
                k: 1,
                mode: SearchMode::Approx {
                    leaf_mode: LeafMode::Graph,
                    eps: None,
                    max_segments: None,
                    max_bytes: None,
                    max_latency_ms: None,
                    routing_page_overfetch: None,
                    max_candidates_per_segment: Some(2),
                },
                guaranteed_recall: false,
                prefetch_depth: borsuk::DEFAULT_SEARCH_PREFETCH_DEPTH,
                filter: None,
                include_metadata: false,
            },
        )
        .unwrap_err();

    assert!(
        err.to_string().contains("graph object size mismatch"),
        "unexpected error: {err}"
    );
}

#[test]
fn graph_search_rejects_graph_edges_for_missing_segment_records() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

    let mut index = BorsukIndex::create(IndexConfig {
        uri: uri.clone(),
        metric: VectorMetric::Euclidean,
        dimensions: 2,
        segment_max_vectors: 4,
        ram_budget_bytes: None,
    })
    .unwrap();

    index
        .add(vec![
            VectorRecord::new("entry", vec![0.0, 0.0]),
            VectorRecord::new("near", vec![0.0, 0.1]),
            VectorRecord::new("far-a", vec![10.0, 10.0]),
            VectorRecord::new("far-b", vec![11.0, 11.0]),
        ])
        .unwrap();
    rewrite_current_graph_object(
        dir.path(),
        index.manifest(),
        "entry",
        "missing-record-id",
        1.0,
    );

    let reopened = BorsukIndex::open(&uri).unwrap();
    let err = reopened
        .search_with_report(
            &[0.0, 0.0],
            SearchOptions {
                k: 1,
                mode: SearchMode::Approx {
                    leaf_mode: LeafMode::Graph,
                    eps: None,
                    max_segments: None,
                    max_bytes: None,
                    max_latency_ms: None,
                    routing_page_overfetch: None,
                    max_candidates_per_segment: Some(2),
                },
                guaranteed_recall: false,
                prefetch_depth: borsuk::DEFAULT_SEARCH_PREFETCH_DEPTH,
                filter: None,
                include_metadata: false,
            },
        )
        .unwrap_err();

    assert!(
        err.to_string()
            .contains("graph edge references missing segment record"),
        "unexpected error: {err}"
    );
}

#[test]
fn graph_search_rejects_graph_edge_distance_mismatch() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

    let mut index = BorsukIndex::create(IndexConfig {
        uri: uri.clone(),
        metric: VectorMetric::Euclidean,
        dimensions: 2,
        segment_max_vectors: 4,
        ram_budget_bytes: None,
    })
    .unwrap();

    index
        .add(vec![
            VectorRecord::new("entry", vec![0.0, 0.0]),
            VectorRecord::new("near", vec![0.0, 0.1]),
            VectorRecord::new("far-a", vec![10.0, 10.0]),
            VectorRecord::new("far-b", vec![11.0, 11.0]),
        ])
        .unwrap();
    rewrite_current_graph_object(dir.path(), index.manifest(), "entry", "near", 42.0);

    let reopened = BorsukIndex::open(&uri).unwrap();
    let err = reopened
        .search_with_report(
            &[0.0, 0.0],
            SearchOptions {
                k: 1,
                mode: SearchMode::Approx {
                    leaf_mode: LeafMode::Graph,
                    eps: None,
                    max_segments: None,
                    max_bytes: None,
                    max_latency_ms: None,
                    routing_page_overfetch: None,
                    max_candidates_per_segment: Some(2),
                },
                guaranteed_recall: false,
                prefetch_depth: borsuk::DEFAULT_SEARCH_PREFETCH_DEPTH,
                filter: None,
                include_metadata: false,
            },
        )
        .unwrap_err();

    assert!(
        err.to_string().contains("graph edge distance mismatch"),
        "unexpected error: {err}"
    );
}

#[test]
fn graph_search_rejects_self_referential_graph_edges() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

    let mut index = BorsukIndex::create(IndexConfig {
        uri: uri.clone(),
        metric: VectorMetric::Euclidean,
        dimensions: 2,
        segment_max_vectors: 4,
        ram_budget_bytes: None,
    })
    .unwrap();

    index
        .add(vec![
            VectorRecord::new("entry", vec![0.0, 0.0]),
            VectorRecord::new("near", vec![0.0, 0.1]),
            VectorRecord::new("far-a", vec![10.0, 10.0]),
            VectorRecord::new("far-b", vec![11.0, 11.0]),
        ])
        .unwrap();
    rewrite_current_graph_object(dir.path(), index.manifest(), "entry", "entry", 0.0);

    let reopened = BorsukIndex::open(&uri).unwrap();
    let err = reopened
        .search_with_report(
            &[0.0, 0.0],
            SearchOptions {
                k: 1,
                mode: SearchMode::Approx {
                    leaf_mode: LeafMode::Graph,
                    eps: None,
                    max_segments: None,
                    max_bytes: None,
                    max_latency_ms: None,
                    routing_page_overfetch: None,
                    max_candidates_per_segment: Some(2),
                },
                guaranteed_recall: false,
                prefetch_depth: borsuk::DEFAULT_SEARCH_PREFETCH_DEPTH,
                filter: None,
                include_metadata: false,
            },
        )
        .unwrap_err();

    assert!(
        err.to_string().contains("graph edge self-reference"),
        "unexpected error: {err}"
    );
}

#[test]
fn graph_search_rejects_duplicate_graph_edges() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

    let mut index = BorsukIndex::create(IndexConfig {
        uri: uri.clone(),
        metric: VectorMetric::Euclidean,
        dimensions: 2,
        segment_max_vectors: 4,
        ram_budget_bytes: None,
    })
    .unwrap();

    index
        .add(vec![
            VectorRecord::new("entry", vec![0.0, 0.0]),
            VectorRecord::new("near", vec![0.0, 0.1]),
            VectorRecord::new("far-a", vec![10.0, 10.0]),
            VectorRecord::new("far-b", vec![11.0, 11.0]),
        ])
        .unwrap();
    rewrite_current_graph_edges(
        dir.path(),
        index.manifest(),
        &[("entry", "near", 0.1), ("entry", "near", 0.1)],
    );

    let reopened = BorsukIndex::open(&uri).unwrap();
    let err = reopened
        .search_with_report(
            &[0.0, 0.0],
            SearchOptions {
                k: 1,
                mode: SearchMode::Approx {
                    leaf_mode: LeafMode::Graph,
                    eps: None,
                    max_segments: None,
                    max_bytes: None,
                    max_latency_ms: None,
                    routing_page_overfetch: None,
                    max_candidates_per_segment: Some(2),
                },
                guaranteed_recall: false,
                prefetch_depth: borsuk::DEFAULT_SEARCH_PREFETCH_DEPTH,
                filter: None,
                include_metadata: false,
            },
        )
        .unwrap_err();

    assert!(
        err.to_string().contains("duplicate graph edge"),
        "unexpected error: {err}"
    );
}

#[test]
fn graph_search_rejects_graph_source_out_degree_above_local_limit() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

    let mut index = BorsukIndex::create(IndexConfig {
        uri: uri.clone(),
        metric: VectorMetric::Euclidean,
        dimensions: 2,
        segment_max_vectors: 10,
        ram_budget_bytes: None,
    })
    .unwrap();

    index
        .add(vec![
            VectorRecord::new("entry", vec![0.0, 0.0]),
            VectorRecord::new("n1", vec![1.0, 0.0]),
            VectorRecord::new("n2", vec![2.0, 0.0]),
            VectorRecord::new("n3", vec![3.0, 0.0]),
            VectorRecord::new("n4", vec![4.0, 0.0]),
            VectorRecord::new("n5", vec![5.0, 0.0]),
            VectorRecord::new("n6", vec![6.0, 0.0]),
            VectorRecord::new("n7", vec![7.0, 0.0]),
            VectorRecord::new("n8", vec![8.0, 0.0]),
            VectorRecord::new("n9", vec![9.0, 0.0]),
        ])
        .unwrap();
    rewrite_current_graph_edges(
        dir.path(),
        index.manifest(),
        &[
            ("entry", "n1", 1.0),
            ("entry", "n2", 2.0),
            ("entry", "n3", 3.0),
            ("entry", "n4", 4.0),
            ("entry", "n5", 5.0),
            ("entry", "n6", 6.0),
            ("entry", "n7", 7.0),
            ("entry", "n8", 8.0),
            ("entry", "n9", 9.0),
        ],
    );

    let reopened = BorsukIndex::open(&uri).unwrap();
    let err = reopened
        .search_with_report(
            &[0.0, 0.0],
            SearchOptions {
                k: 1,
                mode: SearchMode::Approx {
                    leaf_mode: LeafMode::Graph,
                    eps: None,
                    max_segments: None,
                    max_bytes: None,
                    max_latency_ms: None,
                    routing_page_overfetch: None,
                    max_candidates_per_segment: Some(2),
                },
                guaranteed_recall: false,
                prefetch_depth: borsuk::DEFAULT_SEARCH_PREFETCH_DEPTH,
                filter: None,
                include_metadata: false,
            },
        )
        .unwrap_err();

    assert!(
        err.to_string()
            .contains("graph source out-degree exceeds local limit"),
        "unexpected error: {err}"
    );
}

#[test]
fn graph_search_rejects_empty_graph_for_multi_record_segment() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

    let mut index = BorsukIndex::create(IndexConfig {
        uri: uri.clone(),
        metric: VectorMetric::Euclidean,
        dimensions: 2,
        segment_max_vectors: 4,
        ram_budget_bytes: None,
    })
    .unwrap();

    // Four records in a single segment with a candidate budget of 2 keeps the
    // budget below the segment length, so the graph is genuinely traversed (a
    // budget covering the whole segment would flat-scan and legitimately skip
    // the empty graph). This keeps the empty-graph integrity guard exercised.
    index
        .add(vec![
            VectorRecord::new("entry", vec![0.0, 0.0]),
            VectorRecord::new("near", vec![0.0, 0.1]),
            VectorRecord::new("mid", vec![0.0, 0.5]),
            VectorRecord::new("far", vec![5.0, 5.0]),
        ])
        .unwrap();
    rewrite_current_graph_edges(dir.path(), index.manifest(), &[]);

    let reopened = BorsukIndex::open(&uri).unwrap();
    let err = reopened
        .search_with_report(
            &[0.0, 0.0],
            SearchOptions {
                k: 1,
                mode: SearchMode::Approx {
                    leaf_mode: LeafMode::Graph,
                    eps: None,
                    max_segments: None,
                    max_bytes: None,
                    max_latency_ms: None,
                    routing_page_overfetch: None,
                    max_candidates_per_segment: Some(2),
                },
                guaranteed_recall: false,
                prefetch_depth: borsuk::DEFAULT_SEARCH_PREFETCH_DEPTH,
                filter: None,
                include_metadata: false,
            },
        )
        .unwrap_err();

    assert!(
        err.to_string()
            .contains("graph table must contain at least one edge"),
        "unexpected error: {err}"
    );
}

#[test]
fn segment_local_graph_blocks_reopen_and_compact_with_segments() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

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

    let reopened = open_resident(&uri).unwrap();
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
            target_segment_max_radius: None,
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
    let uri = dir.path().to_string_lossy().into_owned();

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
        .search_with_report(
            &[0.0, 0.0],
            SearchOptions {
                k: 2,
                mode: SearchMode::Approx {
                    leaf_mode: LeafMode::Graph,
                    eps: Some(0.05),
                    max_segments: Some(1),
                    max_bytes: None,
                    max_latency_ms: None,
                    routing_page_overfetch: None,
                    max_candidates_per_segment: None,
                },
                guaranteed_recall: false,
                prefetch_depth: borsuk::DEFAULT_SEARCH_PREFETCH_DEPTH,
                filter: None,
                include_metadata: false,
            },
        )
        .map(|report| report.hits)
        .unwrap();

    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].id, "near");

    let report = index
        .search_with_report(
            &[0.0, 0.0],
            SearchOptions::approx(2, LeafMode::Graph).with_max_segments(1),
        )
        .unwrap();
    assert_eq!(
        report.termination_reason,
        SearchTerminationReason::MaxSegments
    );
}

#[test]
fn approximate_search_obeys_byte_budget() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

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

    let routing_only_report = index
        .search_with_report(
            &[0.0, 0.0],
            SearchOptions {
                k: 3,
                mode: SearchMode::Approx {
                    leaf_mode: LeafMode::Graph,
                    eps: None,
                    max_segments: None,
                    max_bytes: Some(1),
                    max_latency_ms: None,
                    routing_page_overfetch: None,
                    max_candidates_per_segment: None,
                },
                guaranteed_recall: false,
                prefetch_depth: borsuk::DEFAULT_SEARCH_PREFETCH_DEPTH,
                filter: None,
                include_metadata: false,
            },
        )
        .unwrap();

    assert_eq!(routing_only_report.hits.len(), 0);
    assert_eq!(routing_only_report.segments_searched, 0);
    assert_eq!(routing_only_report.segments_skipped, 3);
    assert!(routing_only_report.bytes_read > 1);
    assert_eq!(
        routing_only_report.termination_reason,
        SearchTerminationReason::MaxBytes
    );

    let first_segment_budget =
        routing_only_report.bytes_read + index.manifest().segments[0].size_bytes;
    let report = index
        .search_with_report(
            &[0.0, 0.0],
            SearchOptions {
                k: 3,
                mode: SearchMode::Approx {
                    leaf_mode: LeafMode::Graph,
                    eps: None,
                    max_segments: None,
                    max_bytes: Some(first_segment_budget),
                    max_latency_ms: None,
                    routing_page_overfetch: None,
                    max_candidates_per_segment: None,
                },
                guaranteed_recall: false,
                prefetch_depth: borsuk::DEFAULT_SEARCH_PREFETCH_DEPTH,
                filter: None,
                include_metadata: false,
            },
        )
        .unwrap();

    assert_eq!(report.hits.len(), 1);
    assert_eq!(report.hits[0].id, "near");
    assert_eq!(report.segments_searched, 1);
    assert_eq!(report.segments_skipped, 2);
    assert_eq!(report.bytes_read, first_segment_budget);
    assert_eq!(report.termination_reason, SearchTerminationReason::MaxBytes);
}

#[test]
fn search_prefetch_depth_preserves_serial_report_semantics() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let mut index = BorsukIndex::create_with_object_store(
        Arc::clone(&inner),
        IndexConfig {
            uri: "memory:///prefetch-equality".to_string(),
            metric: VectorMetric::Euclidean,
            dimensions: 2,
            segment_max_vectors: 1,
            ram_budget_bytes: None,
        },
    )
    .unwrap();

    index.add(prefetch_test_records(16)).unwrap();
    let reader =
        BorsukIndex::open_with_object_store(Arc::clone(&inner), "memory:///prefetch-equality")
            .unwrap();

    let serial = reader
        .search_with_report(
            &[7.25, 0.0],
            SearchOptions::exact(16).with_prefetch_depth(1),
        )
        .unwrap();
    let pipelined = reader
        .search_with_report(
            &[7.25, 0.0],
            SearchOptions::exact(16).with_prefetch_depth(8),
        )
        .unwrap();

    assert_eq!(pipelined.hits, serial.hits);
    assert_eq!(pipelined.termination_reason, serial.termination_reason);
    assert_eq!(pipelined.bytes_read, serial.bytes_read);
    assert_eq!(serial.prefetched_bytes_unused, 0);
    let _reported_separately = pipelined.prefetched_bytes_unused;

    assert!(
        reader.stats().segments > 1,
        "prefetch fixture must contain multiple segments"
    );
    let single_segment = reader
        .search_with_report(
            &[7.25, 0.0],
            SearchOptions::approx(1, LeafMode::PqScan)
                .with_max_segments(1)
                .with_prefetch_depth(1),
        )
        .unwrap();
    let prefetched_single_segment = reader
        .search_with_report(
            &[7.25, 0.0],
            SearchOptions::approx(1, LeafMode::PqScan)
                .with_max_segments(1)
                .with_prefetch_depth(8),
        )
        .unwrap();

    assert_eq!(single_segment.segments_searched, 1);
    assert_eq!(prefetched_single_segment.segments_searched, 1);
    assert_eq!(
        prefetched_single_segment.bytes_read,
        single_segment.bytes_read
    );
    assert_eq!(
        prefetched_single_segment.prefetched_bytes_unused, 0,
        "max_segments should prevent unused segment payload prefetches"
    );
}

#[test]
fn search_prefetch_depth_obeys_max_segments_payload_budget() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let mut writer = BorsukIndex::create_with_object_store(
        Arc::clone(&inner),
        IndexConfig {
            uri: "memory:///prefetch-max-segments".to_string(),
            metric: VectorMetric::Euclidean,
            dimensions: 2,
            segment_max_vectors: 1,
            ram_budget_bytes: None,
        },
    )
    .unwrap();
    writer.add(prefetch_test_records(16)).unwrap();

    let (counting_store, operation_log) =
        common::FaultInjectingObjectStore::new(inner).with_operation_log();
    let store: Arc<dyn ObjectStore> =
        Arc::new(counting_store.with_latency(Duration::from_millis(5)));
    let reader =
        BorsukIndex::open_with_object_store(store, "memory:///prefetch-max-segments").unwrap();
    let max_segments = 1;

    let report = reader
        .search_with_report(
            &[7.25, 0.0],
            SearchOptions::approx(1, LeafMode::PqScan)
                .with_max_segments(max_segments)
                .with_prefetch_depth(8),
        )
        .unwrap();

    let segment_payload_gets = operation_log.count_matching(|operation, path| {
        operation == common::StoreOperation::Get && path.starts_with("segments/")
    });
    assert_eq!(
        report.termination_reason,
        SearchTerminationReason::MaxSegments
    );
    assert!(
        segment_payload_gets <= max_segments,
        "segment payload GETs ({segment_payload_gets}) must not exceed max_segments ({max_segments})"
    );
}

#[test]
fn search_batch_reuses_request_scoped_routing_page_cache() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let mut writer = BorsukIndex::create_with_object_store(
        Arc::clone(&inner),
        IndexConfig {
            uri: "memory:///batch-routing-cache".to_string(),
            metric: VectorMetric::Euclidean,
            dimensions: 2,
            segment_max_vectors: 1,
            ram_budget_bytes: None,
        },
    )
    .unwrap();
    writer.add(prefetch_test_records(16)).unwrap();

    let (counting_store, operation_log) =
        common::FaultInjectingObjectStore::new(inner).with_operation_log();
    let store: Arc<dyn ObjectStore> = Arc::new(counting_store);
    let reader =
        BorsukIndex::open_with_object_store(store, "memory:///batch-routing-cache").unwrap();
    let options = SearchOptions::approx(1, LeafMode::PqScan)
        .with_max_segments(1)
        .with_routing_page_overfetch(1)
        .with_prefetch_depth(1);

    operation_log.clear();
    reader
        .search_with_report(&[3.0, 0.0], options.clone())
        .unwrap();
    let single_query_routing_page_gets = operation_log.count_matching(|operation, path| {
        operation == common::StoreOperation::Get && path.starts_with("routing/pages/")
    });
    assert!(
        single_query_routing_page_gets > 0,
        "test setup must fetch persisted routing pages"
    );

    operation_log.clear();
    let queries = vec![vec![3.0, 0.0]; 4];
    let reports = reader.search_batch_with_report(&queries, options).unwrap();
    let batch_routing_page_gets = operation_log.count_matching(|operation, path| {
        operation == common::StoreOperation::Get && path.starts_with("routing/pages/")
    });

    assert_eq!(reports.len(), queries.len());
    assert_eq!(
        batch_routing_page_gets, single_query_routing_page_gets,
        "request-scoped routing-page cache should fetch repeated batch routing pages once"
    );
}

#[test]
fn prefetch_depth_reduces_latency_on_slow_store() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let mut writer = BorsukIndex::create_with_object_store(
        Arc::clone(&inner),
        IndexConfig {
            uri: "memory:///prefetch-latency".to_string(),
            metric: VectorMetric::Euclidean,
            dimensions: 2,
            segment_max_vectors: 1,
            ram_budget_bytes: None,
        },
    )
    .unwrap();
    writer.add(prefetch_test_records(16)).unwrap();

    let serial_store: Arc<dyn ObjectStore> = Arc::new(
        common::FaultInjectingObjectStore::new(Arc::clone(&inner))
            .with_latency(Duration::from_millis(20)),
    );
    let serial_reader =
        BorsukIndex::open_with_object_store(serial_store, "memory:///prefetch-latency").unwrap();
    let serial_started = Instant::now();
    let serial = serial_reader
        .search_with_report(
            &[7.25, 0.0],
            SearchOptions::exact(16).with_prefetch_depth(1),
        )
        .unwrap();
    let serial_elapsed = serial_started.elapsed();

    let pipelined_store: Arc<dyn ObjectStore> = Arc::new(
        common::FaultInjectingObjectStore::new(Arc::clone(&inner))
            .with_latency(Duration::from_millis(20)),
    );
    let pipelined_reader =
        BorsukIndex::open_with_object_store(pipelined_store, "memory:///prefetch-latency").unwrap();
    let pipelined_started = Instant::now();
    let pipelined = pipelined_reader
        .search_with_report(
            &[7.25, 0.0],
            SearchOptions::exact(16).with_prefetch_depth(8),
        )
        .unwrap();
    let pipelined_elapsed = pipelined_started.elapsed();

    assert_eq!(pipelined.hits, serial.hits);
    assert!(
        pipelined_elapsed < serial_elapsed,
        "depth 8 should beat depth 1 on latency store: depth1={serial_elapsed:?}, depth8={pipelined_elapsed:?}"
    );
}

#[test]
fn approximate_search_rejects_invalid_budgets() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

    let mut index = BorsukIndex::create(IndexConfig {
        uri,
        metric: VectorMetric::Euclidean,
        dimensions: 2,
        segment_max_vectors: 1,
        ram_budget_bytes: None,
    })
    .unwrap();

    index
        .add(vec![VectorRecord::new("near", vec![0.0, 0.0])])
        .unwrap();

    for (options, expected) in [
        (
            SearchOptions {
                k: 1,
                mode: SearchMode::Approx {
                    eps: Some(-0.1),
                    leaf_mode: LeafMode::Graph,
                    max_segments: None,
                    max_bytes: None,
                    max_latency_ms: None,
                    routing_page_overfetch: None,
                    max_candidates_per_segment: None,
                },
                guaranteed_recall: false,
                prefetch_depth: borsuk::DEFAULT_SEARCH_PREFETCH_DEPTH,
                filter: None,
                include_metadata: false,
            },
            "eps must be finite and non-negative when set",
        ),
        (
            SearchOptions {
                k: 1,
                mode: SearchMode::Approx {
                    eps: Some(f32::NAN),
                    leaf_mode: LeafMode::Graph,
                    max_segments: None,
                    max_bytes: None,
                    max_latency_ms: None,
                    routing_page_overfetch: None,
                    max_candidates_per_segment: None,
                },
                guaranteed_recall: false,
                prefetch_depth: borsuk::DEFAULT_SEARCH_PREFETCH_DEPTH,
                filter: None,
                include_metadata: false,
            },
            "eps must be finite and non-negative when set",
        ),
        (
            SearchOptions {
                k: 1,
                mode: SearchMode::Approx {
                    eps: None,
                    leaf_mode: LeafMode::Graph,
                    max_segments: Some(0),
                    max_bytes: None,
                    max_latency_ms: None,
                    routing_page_overfetch: None,
                    max_candidates_per_segment: None,
                },
                guaranteed_recall: false,
                prefetch_depth: borsuk::DEFAULT_SEARCH_PREFETCH_DEPTH,
                filter: None,
                include_metadata: false,
            },
            "max_segments must be greater than zero when set",
        ),
        (
            SearchOptions {
                k: 1,
                mode: SearchMode::Approx {
                    eps: None,
                    leaf_mode: LeafMode::Graph,
                    max_segments: None,
                    max_bytes: Some(0),
                    max_latency_ms: None,
                    routing_page_overfetch: None,
                    max_candidates_per_segment: None,
                },
                guaranteed_recall: false,
                prefetch_depth: borsuk::DEFAULT_SEARCH_PREFETCH_DEPTH,
                filter: None,
                include_metadata: false,
            },
            "max_bytes must be greater than zero when set",
        ),
        (
            SearchOptions {
                k: 1,
                mode: SearchMode::Approx {
                    eps: None,
                    leaf_mode: LeafMode::Graph,
                    max_segments: None,
                    max_bytes: None,
                    max_latency_ms: Some(0),
                    routing_page_overfetch: None,
                    max_candidates_per_segment: None,
                },
                guaranteed_recall: false,
                prefetch_depth: borsuk::DEFAULT_SEARCH_PREFETCH_DEPTH,
                filter: None,
                include_metadata: false,
            },
            "max_latency_ms must be greater than zero when set",
        ),
        (
            SearchOptions {
                k: 1,
                mode: SearchMode::Approx {
                    eps: None,
                    leaf_mode: LeafMode::Graph,
                    max_segments: None,
                    max_bytes: None,
                    max_latency_ms: None,
                    routing_page_overfetch: None,
                    max_candidates_per_segment: Some(0),
                },
                guaranteed_recall: false,
                prefetch_depth: borsuk::DEFAULT_SEARCH_PREFETCH_DEPTH,
                filter: None,
                include_metadata: false,
            },
            "max_candidates_per_segment must be greater than zero when set",
        ),
    ] {
        let err = index.search_with_report(&[0.0, 0.0], options).unwrap_err();
        assert!(
            err.to_string().contains(expected),
            "expected `{expected}`, got `{err}`"
        );
    }
}

#[test]
fn compact_rejects_impossible_batch_thresholds() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

    let mut index = BorsukIndex::create(IndexConfig {
        uri,
        metric: VectorMetric::Euclidean,
        dimensions: 2,
        segment_max_vectors: 1,
        ram_budget_bytes: None,
    })
    .unwrap();

    let err = index
        .compact(CompactionOptions {
            source_level: 0,
            target_level: 1,
            max_segments: Some(1),
            min_segments: 2,
            target_segment_max_vectors: None,
            target_segment_max_radius: None,
        })
        .unwrap_err();

    assert!(
        err.to_string().contains(
            "min_segments must be less than or equal to max_segments when max_segments is set"
        ),
        "expected compaction batch validation error, got `{err}`"
    );
}

#[test]
fn compact_rejects_zero_target_segment_max_vectors_before_reading_routing_pages() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

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
        ])
        .unwrap();
    write_corrupt_l0_page_index(
        dir.path(),
        index.manifest().version,
        b"corrupt routing page index that validation must not read",
    );

    let err = index
        .compact(CompactionOptions {
            source_level: 0,
            target_level: 1,
            max_segments: Some(1),
            min_segments: 1,
            target_segment_max_vectors: Some(0),
            target_segment_max_radius: None,
        })
        .expect_err("target segment size validation should reject before reading routing pages");
    assert!(
        err.to_string()
            .contains("target_segment_max_vectors must be greater than zero"),
        "expected target segment size validation error, got `{err}`"
    );
}

#[test]
fn search_rejects_zero_k() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

    let mut index = BorsukIndex::create(IndexConfig {
        uri,
        metric: VectorMetric::Euclidean,
        dimensions: 2,
        segment_max_vectors: 1,
        ram_budget_bytes: None,
    })
    .unwrap();

    index
        .add(vec![VectorRecord::new("near", vec![0.0, 0.0])])
        .unwrap();

    for options in [
        SearchOptions::exact(0),
        SearchOptions::approx(0, LeafMode::Graph),
    ] {
        let err = index.search_with_report(&[0.0, 0.0], options).unwrap_err();
        assert!(
            err.to_string().contains("k must be greater than zero"),
            "unexpected error: {err}"
        );
    }
}

#[test]
fn approximate_search_limits_exact_scoring_inside_each_segment() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

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
                    leaf_mode: LeafMode::Graph,
                    eps: None,
                    max_segments: None,
                    max_bytes: None,
                    max_latency_ms: None,
                    routing_page_overfetch: None,
                    max_candidates_per_segment: Some(2),
                },
                guaranteed_recall: false,
                prefetch_depth: borsuk::DEFAULT_SEARCH_PREFETCH_DEPTH,
                filter: None,
                include_metadata: false,
            },
        )
        .unwrap();

    assert_eq!(report.hits[0].id, "near");
    assert_eq!(report.segments_total, 1);
    assert_eq!(report.records_considered, 4);
    assert_eq!(report.records_scored, 2);
}

#[test]
fn approximate_search_options_builder_sets_leaf_mode_and_budget() {
    let options = SearchOptions::approx(3, LeafMode::FlatScan).with_max_candidates_per_segment(2);

    assert_eq!(options.k, 3);
    assert_eq!(options.mode.leaf_mode(), LeafMode::FlatScan);
    let SearchMode::Approx {
        leaf_mode,
        max_candidates_per_segment,
        ..
    } = options.mode
    else {
        panic!("expected approximate search options");
    };
    assert_eq!(leaf_mode, LeafMode::FlatScan);
    assert_eq!(max_candidates_per_segment, Some(2));
}

#[test]
fn approximate_search_enforces_candidate_budget_when_k_is_larger() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

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
                k: 3,
                mode: SearchMode::Approx {
                    leaf_mode: LeafMode::Graph,
                    eps: None,
                    max_segments: None,
                    max_bytes: None,
                    max_latency_ms: None,
                    routing_page_overfetch: None,
                    max_candidates_per_segment: Some(2),
                },
                guaranteed_recall: false,
                prefetch_depth: borsuk::DEFAULT_SEARCH_PREFETCH_DEPTH,
                filter: None,
                include_metadata: false,
            },
        )
        .unwrap();

    assert_eq!(report.hits.len(), 2);
    assert_eq!(report.records_considered, 4);
    assert_eq!(report.records_scored, 2);
}

#[test]
fn approximate_flat_scan_leaf_mode_skips_segment_graph() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

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
                    leaf_mode: LeafMode::FlatScan,
                    eps: None,
                    max_segments: None,
                    max_bytes: None,
                    max_latency_ms: None,
                    routing_page_overfetch: None,
                    max_candidates_per_segment: Some(2),
                },
                guaranteed_recall: false,
                prefetch_depth: borsuk::DEFAULT_SEARCH_PREFETCH_DEPTH,
                filter: None,
                include_metadata: false,
            },
        )
        .unwrap();

    assert_eq!(report.leaf_mode, "flat-scan");
    assert_eq!(report.hits[0].id, "near");
    assert_eq!(report.records_considered, 4);
    assert_eq!(report.records_scored, 2);
    assert_eq!(report.graph_bytes_read, 0);
    assert_eq!(report.graph_candidates_added, 0);
}

#[test]
fn approximate_sq_scan_leaf_mode_uses_routing_codes_and_skips_segment_graph() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

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
            VectorRecord::new("routing-neighbor", vec![0.2, 0.0]),
            VectorRecord::new("graph-neighbor", vec![0.0, 0.1]),
            VectorRecord::new("far", vec![100.0, 100.0]),
        ])
        .unwrap();

    let report = index
        .search_with_report(
            &[0.19, 0.0],
            SearchOptions::approx(1, LeafMode::SqScan).with_max_candidates_per_segment(2),
        )
        .unwrap();

    assert_eq!(
        leaf_mode_names(),
        [
            "flat-scan",
            "sq-scan",
            "pq-scan",
            "graph",
            "vamana-pq",
            "hybrid"
        ]
    );
    assert_eq!(report.leaf_mode, "sq-scan");
    assert_eq!(report.hits[0].id, "routing-neighbor");
    assert_eq!(report.records_considered, 4);
    assert_eq!(report.records_scored, 2);
    assert_eq!(report.graph_bytes_read, 0);
    assert_eq!(report.graph_candidates_added, 0);
}

#[test]
fn approximate_pq_scan_leaf_mode_uses_compressed_scan_and_skips_segment_graph() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

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
            VectorRecord::new("a-routing-decoy", vec![-0.9, 0.0]),
            VectorRecord::new("true-neighbor", vec![0.0, 0.9]),
            VectorRecord::new("far", vec![100.0, 100.0]),
        ])
        .unwrap();

    let report = index
        .search_with_report(
            &[0.0, 0.9],
            SearchOptions::approx(1, LeafMode::PqScan).with_max_candidates_per_segment(1),
        )
        .unwrap();

    assert_eq!(
        leaf_mode_names(),
        [
            "flat-scan",
            "sq-scan",
            "pq-scan",
            "graph",
            "vamana-pq",
            "hybrid"
        ]
    );
    assert_eq!(report.leaf_mode, "pq-scan");
    assert_eq!(report.hits[0].id, "true-neighbor");
    assert_eq!(report.records_considered, 3);
    assert_eq!(report.records_scored, 1);
    assert_eq!(report.graph_bytes_read, 0);
    assert_eq!(report.graph_candidates_added, 0);
}

#[test]
fn approximate_routing_prefers_segments_with_matching_vector_signatures() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

    let mut index = BorsukIndex::create(IndexConfig {
        uri,
        metric: VectorMetric::Euclidean,
        dimensions: 2,
        segment_max_vectors: 4,
        ram_budget_bytes: None,
    })
    .unwrap();

    let mut records = Vec::new();
    for segment in 0..8 {
        records.extend([
            VectorRecord::new(format!("decoy-{segment}-a"), vec![-1.0, 0.0]),
            VectorRecord::new(format!("decoy-{segment}-b"), vec![1.0, 0.0]),
            VectorRecord::new(format!("decoy-{segment}-c"), vec![0.0, -1.0]),
            VectorRecord::new(format!("decoy-{segment}-d"), vec![0.0, 1.0]),
        ]);
    }
    for segment in 0..8 {
        records.extend([
            VectorRecord::new(format!("target-{segment}-a"), vec![-1.0, 0.0]),
            VectorRecord::new(format!("target-{segment}-b"), vec![1.0, 0.0]),
            VectorRecord::new(format!("target-{segment}-c"), vec![0.0, 1.0]),
            VectorRecord::new(format!("target-{segment}-query"), vec![0.0, 0.0]),
        ]);
    }
    index.add(records).unwrap();

    let report = index
        .search_with_report(
            &[0.0, 0.0],
            SearchOptions::approx(8, LeafMode::PqScan)
                .with_max_segments(8)
                .with_max_candidates_per_segment(4),
        )
        .unwrap();

    assert_eq!(report.segments_searched, 8);
    assert_eq!(report.records_scored, 32);
    assert!(
        report
            .hits
            .iter()
            .all(|hit| hit.id.starts_with("target-") && hit.distance == 0.0),
        "expected signature routing to select target segments, got {:?}",
        report.hits
    );
}

#[test]
fn approximate_page_routing_prefers_pages_with_matching_vector_signatures() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

    let mut index = BorsukIndex::create(IndexConfig {
        uri,
        metric: VectorMetric::Euclidean,
        dimensions: 2,
        segment_max_vectors: 1,
        ram_budget_bytes: None,
    })
    .unwrap();

    const TEST_ROUTING_PAGE_FANOUT: usize = 128;
    let mut records = (0..TEST_ROUTING_PAGE_FANOUT)
        .map(|idx| {
            let vector = match idx % 4 {
                0 => vec![-1.0, 0.0],
                1 => vec![1.0, 0.0],
                2 => vec![0.0, -1.0],
                _ => vec![0.0, 1.0],
            };
            VectorRecord::new(format!("decoy-{idx}"), vector)
        })
        .collect::<Vec<_>>();
    records.push(VectorRecord::new("target", vec![0.0, 0.0]));
    index.add(records).unwrap();

    let report = index
        .search_with_report(
            &[0.0, 0.0],
            SearchOptions::approx(1, LeafMode::PqScan)
                .with_max_segments(1)
                .with_max_candidates_per_segment(1),
        )
        .unwrap();

    assert_eq!(report.segments_searched, 1);
    assert_eq!(
        report.hits.first().map(|hit| hit.id.as_str()),
        Some("target")
    );
}

#[test]
fn approximate_vamana_pq_leaf_mode_uses_segment_graph_and_reports_mode() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

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
            SearchOptions::approx(1, LeafMode::VamanaPq).with_max_candidates_per_segment(2),
        )
        .unwrap();

    assert_eq!(report.leaf_mode, "vamana-pq");
    assert_eq!(report.hits[0].id, "true-neighbor");
    assert_eq!(report.records_considered, 4);
    assert_eq!(report.records_scored, 2);
    assert!(report.graph_bytes_read > 0);
    assert_eq!(report.graph_candidates_added, 1);
}

#[test]
fn approximate_vamana_pq_uses_pq_codes_for_graph_entry_points() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

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
            VectorRecord::new("a-routing-decoy", vec![-0.9, 0.0]),
            VectorRecord::new("true-neighbor", vec![0.0, 0.9]),
            VectorRecord::new("far", vec![100.0, 100.0]),
        ])
        .unwrap();

    let report = index
        .search_with_report(
            &[0.0, 0.9],
            SearchOptions::approx(1, LeafMode::VamanaPq).with_max_candidates_per_segment(2),
        )
        .unwrap();

    assert_eq!(report.leaf_mode, "vamana-pq");
    assert_eq!(report.hits[0].id, "true-neighbor");
    assert_eq!(report.records_considered, 3);
    assert_eq!(report.records_scored, 2);
    assert!(report.graph_bytes_read > 0);
    assert_eq!(report.graph_candidates_added, 1);
}

#[test]
fn approximate_vamana_pq_skips_graph_when_candidate_budget_cannot_expand() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

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
            VectorRecord::new("a-routing-decoy", vec![-0.9, 0.0]),
            VectorRecord::new("true-neighbor", vec![0.0, 0.9]),
            VectorRecord::new("far", vec![100.0, 100.0]),
        ])
        .unwrap();

    let report = index
        .search_with_report(
            &[0.0, 0.9],
            SearchOptions::approx(1, LeafMode::VamanaPq).with_max_candidates_per_segment(1),
        )
        .unwrap();

    assert_eq!(report.leaf_mode, "vamana-pq");
    assert_eq!(report.hits[0].id, "true-neighbor");
    assert_eq!(report.records_scored, 1);
    assert_eq!(report.graph_bytes_read, 0);
    assert_eq!(report.graph_candidates_added, 0);
}

#[test]
fn approximate_vamana_pq_skips_graph_when_candidate_budget_covers_segment() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

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
            SearchOptions::approx(1, LeafMode::VamanaPq).with_max_candidates_per_segment(4),
        )
        .unwrap();

    assert_eq!(report.leaf_mode, "vamana-pq");
    assert_eq!(report.hits[0].id, "true-neighbor");
    assert_eq!(report.records_considered, 4);
    assert_eq!(report.records_scored, 4);
    assert_eq!(report.graph_bytes_read, 0);
    assert_eq!(report.graph_candidates_added, 0);
}

#[test]
fn approximate_hybrid_leaf_mode_uses_stored_segment_leaf_mode() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

    let mut index = BorsukIndex::create(IndexConfig {
        uri: uri.clone(),
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
    rewrite_current_routing_leaf_mode(dir.path(), index.manifest(), "flat-scan");

    let reopened = BorsukIndex::open(&uri).unwrap();
    let hybrid_report = reopened
        .search_with_report(
            &[0.04, 0.07],
            SearchOptions::approx(1, LeafMode::Hybrid).with_max_candidates_per_segment(2),
        )
        .unwrap();

    assert_eq!(hybrid_report.leaf_mode, "hybrid");
    assert_eq!(hybrid_report.graph_bytes_read, 0);
    assert_eq!(hybrid_report.graph_candidates_added, 0);

    let explicit_graph_report = reopened
        .search_with_report(
            &[0.04, 0.07],
            SearchOptions::approx(1, LeafMode::Graph).with_max_candidates_per_segment(2),
        )
        .unwrap();

    assert_eq!(explicit_graph_report.leaf_mode, "graph");
    assert!(explicit_graph_report.graph_bytes_read > 0);
    assert_eq!(explicit_graph_report.graph_candidates_added, 1);
}

#[test]
fn approximate_hybrid_uses_stored_vamana_pq_leaf_mode() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

    let mut index = BorsukIndex::create(IndexConfig {
        uri: uri.clone(),
        metric: VectorMetric::Euclidean,
        dimensions: 2,
        segment_max_vectors: 4,
        ram_budget_bytes: None,
    })
    .unwrap();

    index
        .add(vec![
            VectorRecord::new("a-routing-decoy", vec![-0.9, 0.0]),
            VectorRecord::new("true-neighbor", vec![0.0, 0.9]),
            VectorRecord::new("far", vec![100.0, 100.0]),
        ])
        .unwrap();
    rewrite_current_routing_leaf_mode(dir.path(), index.manifest(), "vamana-pq");

    let reopened = BorsukIndex::open(&uri).unwrap();
    let report = reopened
        .search_with_report(
            &[0.0, 0.9],
            SearchOptions::approx(1, LeafMode::Hybrid).with_max_candidates_per_segment(2),
        )
        .unwrap();

    assert_eq!(report.leaf_mode, "hybrid");
    assert_eq!(report.hits[0].id, "true-neighbor");
    assert_eq!(report.records_considered, 3);
    assert_eq!(report.records_scored, 2);
    assert!(report.graph_bytes_read > 0);
    assert_eq!(report.graph_candidates_added, 1);
}

#[test]
fn approximate_hybrid_dispatches_mixed_l0_graph_and_l1_vamana_pq_leaves() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

    let mut index = BorsukIndex::create(IndexConfig {
        uri: uri.clone(),
        metric: VectorMetric::Euclidean,
        dimensions: 2,
        segment_max_vectors: 3,
        ram_budget_bytes: None,
    })
    .unwrap();

    index
        .add(vec![
            VectorRecord::new("a-routing-decoy", vec![-0.9, 0.0]),
            VectorRecord::new("true-neighbor", vec![0.0, 0.9]),
            VectorRecord::new("far", vec![100.0, 100.0]),
        ])
        .unwrap();
    index
        .compact(CompactionOptions {
            source_level: 0,
            target_level: 1,
            max_segments: Some(1),
            min_segments: 1,
            target_segment_max_vectors: Some(3),
            target_segment_max_radius: None,
        })
        .unwrap();
    index
        .add(vec![VectorRecord::new("fresh-l0-far", vec![50.0, 50.0])])
        .unwrap();

    let leaf_modes = routing_leaf_page_segments(dir.path(), index.manifest().version)
        .into_iter()
        .map(|segment| segment.leaf_mode)
        .collect::<Vec<_>>();
    assert!(leaf_modes.contains(&LeafMode::Graph), "{leaf_modes:?}");
    assert!(leaf_modes.contains(&LeafMode::VamanaPq), "{leaf_modes:?}");

    let report = index
        .search_with_report(
            &[0.0, 0.9],
            SearchOptions::approx(1, LeafMode::Hybrid).with_max_candidates_per_segment(2),
        )
        .unwrap();

    assert_eq!(report.leaf_mode, "hybrid");
    assert_eq!(report.hits[0].id, "true-neighbor");
    assert_eq!(report.segments_total, 2);
    assert_eq!(report.segments_searched, 2);
    assert_eq!(report.records_considered, 4);
    assert_eq!(report.records_scored, 3);
    assert!(report.graph_bytes_read > 0);
    assert_eq!(report.graph_candidates_added, 1);
}

#[test]
fn approximate_search_expands_candidates_from_segment_graph() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

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
                    leaf_mode: LeafMode::Graph,
                    eps: None,
                    max_segments: None,
                    max_bytes: None,
                    max_latency_ms: None,
                    routing_page_overfetch: None,
                    max_candidates_per_segment: Some(2),
                },
                guaranteed_recall: false,
                prefetch_depth: borsuk::DEFAULT_SEARCH_PREFETCH_DEPTH,
                filter: None,
                include_metadata: false,
            },
        )
        .unwrap();

    assert_eq!(report.hits[0].id, "true-neighbor");
    assert_eq!(report.leaf_mode, "graph");
    assert_eq!(report.records_considered, 4);
    assert_eq!(report.records_scored, 2);
    assert!(report.graph_bytes_read > 0);
    assert_eq!(report.graph_candidates_added, 1);
}

#[test]
fn approximate_search_walks_segment_graph_beyond_first_hop() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

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
                    leaf_mode: LeafMode::Graph,
                    eps: None,
                    max_segments: None,
                    max_bytes: None,
                    max_latency_ms: None,
                    routing_page_overfetch: None,
                    max_candidates_per_segment: Some(3),
                },
                guaranteed_recall: false,
                prefetch_depth: borsuk::DEFAULT_SEARCH_PREFETCH_DEPTH,
                filter: None,
                include_metadata: false,
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
    let uri = dir.path().to_string_lossy().into_owned();

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

    let index = open_resident_cached(&uri, cache.path().to_path_buf()).unwrap();
    let report = index
        .search_with_report(
            &[0.04, 0.07],
            SearchOptions {
                k: 1,
                mode: SearchMode::Approx {
                    leaf_mode: LeafMode::Graph,
                    eps: None,
                    max_segments: None,
                    max_bytes: None,
                    max_latency_ms: None,
                    routing_page_overfetch: None,
                    max_candidates_per_segment: Some(2),
                },
                guaranteed_recall: false,
                prefetch_depth: borsuk::DEFAULT_SEARCH_PREFETCH_DEPTH,
                filter: None,
                include_metadata: false,
            },
        )
        .unwrap();
    assert_eq!(report.hits[0].id, "true-neighbor");
    assert!(report.graph_bytes_read > 0);
    assert_eq!(report.object_cache_hits, 0);
    assert_eq!(report.object_cache_misses, 4);

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
                    leaf_mode: LeafMode::Graph,
                    eps: None,
                    max_segments: None,
                    max_bytes: None,
                    max_latency_ms: None,
                    routing_page_overfetch: None,
                    max_candidates_per_segment: Some(2),
                },
                guaranteed_recall: false,
                prefetch_depth: borsuk::DEFAULT_SEARCH_PREFETCH_DEPTH,
                filter: None,
                include_metadata: false,
            },
        )
        .unwrap();
    assert_eq!(cached_report.hits[0].id, "true-neighbor");
    assert_eq!(cached_report.object_cache_hits, 4);
    assert_eq!(cached_report.object_cache_misses, 0);
    assert_eq!(cached_report.records_scored, 2);
}

#[test]
fn read_through_cache_refetches_corrupt_segment_and_graph_payloads() {
    let dir = tempfile::tempdir().unwrap();
    let cache = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

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

    let index = open_resident_cached(&uri, cache.path().to_path_buf()).unwrap();
    index
        .search_with_report(
            &[0.04, 0.07],
            SearchOptions {
                k: 1,
                mode: SearchMode::Approx {
                    leaf_mode: LeafMode::Graph,
                    eps: None,
                    max_segments: None,
                    max_bytes: None,
                    max_latency_ms: None,
                    routing_page_overfetch: None,
                    max_candidates_per_segment: Some(2),
                },
                guaranteed_recall: false,
                prefetch_depth: borsuk::DEFAULT_SEARCH_PREFETCH_DEPTH,
                filter: None,
                include_metadata: false,
            },
        )
        .unwrap();

    let summary = &index.manifest().segments[0];
    let cached_segment = cache.path().join(&summary.path);
    let cached_graph = cache.path().join(&summary.graph_path);
    assert!(cached_segment.exists());
    assert!(cached_graph.exists());
    fs::write(&cached_segment, b"corrupt cached segment").unwrap();
    fs::write(&cached_graph, b"corrupt cached graph").unwrap();

    let repaired_report = index
        .search_with_report(
            &[0.04, 0.07],
            SearchOptions {
                k: 1,
                mode: SearchMode::Approx {
                    leaf_mode: LeafMode::Graph,
                    eps: None,
                    max_segments: None,
                    max_bytes: None,
                    max_latency_ms: None,
                    routing_page_overfetch: None,
                    max_candidates_per_segment: Some(2),
                },
                guaranteed_recall: false,
                prefetch_depth: borsuk::DEFAULT_SEARCH_PREFETCH_DEPTH,
                filter: None,
                include_metadata: false,
            },
        )
        .unwrap();

    assert_eq!(repaired_report.hits[0].id, "true-neighbor");
    assert_eq!(repaired_report.cache_repairs, 2);
    assert!(repaired_report.object_cache_misses >= 2);
    assert_ne!(fs::read(cached_segment).unwrap(), b"corrupt cached segment");
    assert_ne!(fs::read(cached_graph).unwrap(), b"corrupt cached graph");
}

#[test]
fn read_through_cache_reports_corrupt_segment_repair() {
    let dir = tempfile::tempdir().unwrap();
    let cache = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

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
            VectorRecord::new("true-neighbor", vec![0.0, 0.1]),
            VectorRecord::new("far", vec![100.0, 100.0]),
        ])
        .unwrap();

    let index = open_resident_cached(&uri, cache.path().to_path_buf()).unwrap();
    index
        .search_with_report(&[0.0, 0.0], SearchOptions::exact(1).with_prefetch_depth(1))
        .unwrap();

    let summary = &index.manifest().segments[0];
    let cached_segment = cache.path().join(&summary.path);
    assert!(cached_segment.exists());
    fs::write(&cached_segment, b"corrupt cached segment").unwrap();

    let repaired_report = index
        .search_with_report(&[0.0, 0.0], SearchOptions::exact(1).with_prefetch_depth(1))
        .unwrap();

    assert_eq!(repaired_report.hits[0].id, "true-neighbor");
    assert_eq!(repaired_report.cache_repairs, 1);
    assert!(repaired_report.object_cache_misses >= 1);
    assert_ne!(fs::read(cached_segment).unwrap(), b"corrupt cached segment");
}

#[test]
fn cache_max_bytes_evicts_oldest_objects_and_refetches() {
    let dir = tempfile::tempdir().unwrap();
    let cache = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

    let mut writer = BorsukIndex::create(IndexConfig {
        uri: uri.clone(),
        metric: VectorMetric::Euclidean,
        dimensions: 2,
        segment_max_vectors: 1,
        ram_budget_bytes: None,
    })
    .unwrap();

    writer
        .add(vec![
            VectorRecord::new("near", vec![0.0, 0.0]),
            VectorRecord::new("far", vec![10.0, 0.0]),
        ])
        .unwrap();

    let summaries = writer.manifest().segments.clone();
    assert_eq!(summaries.len(), 2);
    let near_summary = summaries
        .iter()
        .find(|summary| summary.centroid == vec![0.0, 0.0])
        .unwrap();
    let far_summary = summaries
        .iter()
        .find(|summary| summary.centroid == vec![10.0, 0.0])
        .unwrap();
    let cache_max_bytes = near_summary.size_bytes.max(far_summary.size_bytes);

    let index = BorsukIndex::open_with_options(
        &uri,
        OpenOptions {
            cache_dir: Some(cache.path().to_path_buf()),
            cache_max_bytes: Some(cache_max_bytes),
            ram_budget_bytes: None,
            resident_routing: true,
            ..OpenOptions::default()
        },
    )
    .unwrap();

    let first_report = index
        .search_with_report(&[0.0, 0.0], SearchOptions::exact(2).with_prefetch_depth(1))
        .unwrap();
    assert_eq!(hit_ids(first_report), ["near", "far"]);
    let cache_files = storage_file_sizes(cache.path());
    assert!(
        cache_files.values().sum::<u64>() <= cache_max_bytes,
        "bounded cache should stay within cache_max_bytes"
    );
    assert_eq!(
        cache_files
            .keys()
            .filter(|path| path.starts_with("segments/"))
            .count(),
        1,
        "bounded cache should retain exactly one segment file"
    );

    let refetched_report = index
        .search_with_report(&[0.0, 0.0], SearchOptions::exact(2).with_prefetch_depth(1))
        .unwrap();

    assert_eq!(hit_ids(refetched_report.clone()), ["near", "far"]);
    assert!(refetched_report.object_cache_misses > 0);
    assert_eq!(refetched_report.cache_repairs, 0);
    let cache_files = storage_file_sizes(cache.path());
    assert!(
        cache_files.values().sum::<u64>() <= cache_max_bytes,
        "bounded cache should stay within cache_max_bytes after refetch"
    );
    assert_eq!(
        cache_files
            .keys()
            .filter(|path| path.starts_with("segments/"))
            .count(),
        1,
        "bounded cache should retain exactly one segment file after refetch"
    );
}

#[test]
fn exact_search_reports_segments_skipped_and_bytes_read() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

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
fn exact_search_does_not_prune_equal_distance_ties() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

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
            VectorRecord::new("z-tie", vec![1.0, 0.0]),
            VectorRecord::new("a-tie", vec![-1.0, 0.0]),
        ])
        .unwrap();

    let report = index
        .search_with_report(&[0.0, 0.0], SearchOptions::exact(1))
        .unwrap();

    assert_eq!(report.hits[0].id, "a-tie");
    assert_eq!(report.segments_searched, 2);
    assert_eq!(report.segments_skipped, 0);
}

#[test]
fn exact_search_with_inner_product_does_not_use_centroid_lower_bound() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

    let mut index = BorsukIndex::create(IndexConfig {
        uri,
        metric: VectorMetric::InnerProduct,
        dimensions: 1,
        segment_max_vectors: 1,
        ram_budget_bytes: None,
    })
    .unwrap();

    index
        .add(vec![
            VectorRecord::new("low-dot", vec![1.0]),
            VectorRecord::new("high-dot", vec![10.0]),
        ])
        .unwrap();

    let report = index
        .search_with_report(&[1.0], SearchOptions::exact(1))
        .unwrap();

    assert_eq!(report.hits[0].id, "high-dot");
    assert_eq!(report.segments_searched, 2);
    assert_eq!(report.segments_skipped, 0);
}

#[test]
fn approximate_search_with_inner_product_ranks_segments_by_metric_distance() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

    let mut index = BorsukIndex::create(IndexConfig {
        uri,
        metric: VectorMetric::InnerProduct,
        dimensions: 1,
        segment_max_vectors: 1,
        ram_budget_bytes: None,
    })
    .unwrap();

    index
        .add(vec![
            VectorRecord::new("low-dot", vec![1.0]),
            VectorRecord::new("high-dot", vec![10.0]),
        ])
        .unwrap();

    let report = index
        .search_with_report(
            &[1.0],
            SearchOptions::approx(1, LeafMode::PqScan)
                .with_max_segments(1)
                .with_max_candidates_per_segment(1),
        )
        .unwrap();

    assert_eq!(report.hits[0].id, "high-dot");
    assert_eq!(report.segments_searched, 1);
}

#[test]
fn compact_rewrites_l0_segments_into_l1_without_mutating_old_segments() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

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
    assert!(
        index
            .manifest()
            .segments
            .iter()
            .all(|segment| segment.leaf_mode == LeafMode::Graph)
    );

    let report = index
        .compact(CompactionOptions {
            source_level: 0,
            target_level: 1,
            max_segments: Some(4),
            min_segments: 2,
            target_segment_max_vectors: Some(2),
            target_segment_max_radius: None,
        })
        .unwrap();

    assert!(report.compacted);
    assert_eq!(report.segments_read, 4);
    assert_eq!(report.segments_written, 2);
    assert_eq!(report.records_rewritten, 4);
    assert!(report.bytes_read > 0);
    assert!(report.bytes_written > 0);
    assert_eq!(report.object_cache_hits, 0);
    assert_eq!(report.object_cache_misses, 6);
    assert_eq!(report.manifest_version, index.manifest().version);

    assert!(
        index.manifest().segments.is_empty(),
        "compaction should leave active summaries in routing pages"
    );
    assert_eq!(index.stats().segments, 2);

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
    assert_eq!(
        report.bytes_written,
        total_file_bytes(&l1_after) + total_file_bytes(&l1_graphs_after),
        "compaction bytes_written should count new segment and graph payload bytes"
    );

    let reopened = BorsukIndex::open(&uri).unwrap();
    assert!(reopened.manifest().segments.is_empty());
    assert_eq!(reopened.stats().segments, 2);
    let ids = reopened
        .search_ids(&[8.5, 0.0], SearchOptions::exact(2))
        .unwrap();
    assert_eq!(ids, vec!["c", "d"]);
}

#[test]
fn compact_packs_vector_local_records_for_budgeted_high_recall_search() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

    let mut index = BorsukIndex::create(IndexConfig {
        uri,
        metric: VectorMetric::Euclidean,
        dimensions: 2,
        segment_max_vectors: 16,
        ram_budget_bytes: None,
    })
    .unwrap();

    let mut records = Vec::new();
    for round in 0..8 {
        for cluster in 0..16 {
            records.push(VectorRecord::new(
                format!("cluster-{cluster}-{round}"),
                vec![cluster as f32 * 100.0, round as f32 * 0.01],
            ));
        }
    }
    index.add(records).unwrap();

    let pre_compaction = index
        .search_with_report(
            &[0.0, 0.0],
            SearchOptions::approx(8, LeafMode::PqScan)
                .with_max_segments(2)
                .with_max_candidates_per_segment(16),
        )
        .unwrap();
    assert!(
        pre_compaction
            .hits
            .iter()
            .filter(|hit| hit.distance < 1.0)
            .count()
            < 8,
        "append-order L0 blobs should not already contain the whole query-local neighborhood: {:?}",
        pre_compaction.hits
    );

    index
        .compact(CompactionOptions {
            source_level: 0,
            target_level: 1,
            max_segments: Some(8),
            min_segments: 2,
            target_segment_max_vectors: Some(16),
            target_segment_max_radius: None,
        })
        .unwrap();
    let compacted_segments = routing_leaf_page_segments(dir.path(), index.manifest().version);
    assert!(
        compacted_segments
            .iter()
            .all(|segment| segment.leaf_mode == LeafMode::VamanaPq),
        "compacted L1+ segments should declare `vamana-pq` in routing metadata"
    );

    let post_compaction = index
        .search_with_report(
            &[0.0, 0.0],
            SearchOptions::approx(8, LeafMode::PqScan)
                .with_max_segments(2)
                .with_max_candidates_per_segment(16),
        )
        .unwrap();

    assert_eq!(post_compaction.segments_searched, 2);
    assert!(
        post_compaction
            .hits
            .iter()
            .all(|hit| hit.id.starts_with("cluster-0-") && hit.distance < 1.0),
        "compacted L1 blobs should pack the query-local neighborhood, got {:?}",
        post_compaction.hits
    );
}

#[test]
fn compact_default_rewrites_bounded_source_batch() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

    let mut index = BorsukIndex::create(IndexConfig {
        uri,
        metric: VectorMetric::Euclidean,
        dimensions: 2,
        segment_max_vectors: 1,
        ram_budget_bytes: None,
    })
    .unwrap();

    let records = (0..34)
        .map(|value| VectorRecord::new(value.to_string(), vec![value as f32, 0.0]))
        .collect::<Vec<_>>();
    index.add(records).unwrap();

    let report = index.compact(CompactionOptions::default()).unwrap();

    assert!(report.compacted);
    assert_eq!(report.segments_read, 32);
    assert_eq!(report.records_rewritten, 32);
    assert!(index.manifest().segments.is_empty());
    assert_eq!(index.stats().segments, 34);
    assert_eq!(index.get_vector("33").unwrap(), Some(vec![33.0, 0.0]));
}

#[test]
fn compact_reads_only_selected_source_leaf_payloads() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

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
            VectorRecord::new("c", vec![2.0, 0.0]),
            VectorRecord::new("d", vec![3.0, 0.0]),
        ])
        .unwrap();

    let selected_l0_id = index.manifest().segments[0].id.clone();
    for summary in index.manifest().segments.iter() {
        fs::write(
            dir.path().join(&summary.graph_path),
            b"corrupt graph that scoped compaction must not read",
        )
        .unwrap();
        if summary.id != selected_l0_id {
            fs::write(
                dir.path().join(&summary.path),
                b"corrupt unselected payload that scoped compaction must not read",
            )
            .unwrap();
        }
    }

    let report = index
        .compact(CompactionOptions {
            source_level: 0,
            target_level: 1,
            max_segments: Some(1),
            min_segments: 1,
            target_segment_max_vectors: Some(1),
            target_segment_max_radius: None,
        })
        .unwrap();

    assert!(report.compacted);
    assert_eq!(report.segments_read, 1);
    assert_eq!(report.object_cache_misses, 3);
    assert_eq!(report.object_cache_hits, 0);
    assert_eq!(report.records_rewritten, 1);
    assert!(index.manifest().segments.is_empty());
}

#[test]
fn compact_uses_paged_routing_even_when_summaries_are_resident() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

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
            VectorRecord::new("c", vec![9.0, 0.0]),
        ])
        .unwrap();
    assert!(
        !index.manifest().segments.is_empty(),
        "the handle starts with resident summaries after append"
    );

    let report = index
        .compact(CompactionOptions {
            source_level: 0,
            target_level: 1,
            max_segments: Some(2),
            min_segments: 2,
            target_segment_max_vectors: Some(2),
            target_segment_max_radius: None,
        })
        .unwrap();

    assert!(report.compacted);
    assert_eq!(report.segments_read, 2);
    assert_eq!(report.records_rewritten, 2);
    assert!(
        index.manifest().segments.is_empty(),
        "scoped compaction should publish through routing pages instead of keeping the full summary table resident"
    );
    assert_eq!(index.stats().segments, 2);
    assert_eq!(index.get_vector("a").unwrap(), Some(vec![0.0, 0.0]));
    assert_eq!(
        index
            .search_ids(&[9.0, 0.0], SearchOptions::exact(1))
            .unwrap(),
        ["c"]
    );
}

#[test]
fn compact_from_empty_routing_table_reads_only_selected_source_leaf_payloads() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

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
            VectorRecord::new("c", vec![9.0, 0.0]),
        ])
        .unwrap();
    let selected_payload_bytes =
        index.manifest().segments[0].size_bytes + index.manifest().segments[1].size_bytes;
    for summary in index.manifest().segments.iter() {
        fs::write(
            dir.path().join(&summary.graph_path),
            b"corrupt graph that non-resident compaction must not read",
        )
        .unwrap();
    }
    let page_refs = routing_layer_page_index_paths(dir.path(), index.manifest().version, 0);
    assert_eq!(page_refs.len(), 1);
    let routing_page_bytes = fs::metadata(dir.path().join(&page_refs[0])).unwrap().len();
    let routing_page_index_bytes = fs::metadata(dir.path().join(format!(
        "routing/layers/{:020}/L0/pages.parquet",
        index.manifest().version
    )))
    .unwrap()
    .len();
    let unselected_payload = dir.path().join(&index.manifest().segments[2].path);
    fs::write(
        unselected_payload,
        b"corrupt unselected payload that non-resident compaction must not read",
    )
    .unwrap();
    rewrite_current_with_empty_routing_table(dir.path(), index.manifest());

    let mut reopened = BorsukIndex::open(&uri).unwrap();
    assert!(reopened.manifest().segments.is_empty());

    let compaction = reopened
        .compact(CompactionOptions {
            source_level: 0,
            target_level: 1,
            max_segments: Some(2),
            min_segments: 2,
            target_segment_max_vectors: Some(2),
            target_segment_max_radius: None,
        })
        .unwrap();

    assert!(compaction.compacted);
    assert_eq!(compaction.segments_read, 2);
    assert_eq!(compaction.records_rewritten, 2);
    assert_eq!(compaction.graph_payloads_read, 0);
    assert_eq!(compaction.graph_bytes_read, 0);
    assert!(
        compaction.bytes_read
            >= selected_payload_bytes + routing_page_bytes + routing_page_index_bytes,
        "non-resident compaction bytes_read should include routing page index bytes, routing page bytes, and selected segment bytes; got {}, selected payloads were {}, routing page was {}, index was {}",
        compaction.bytes_read,
        selected_payload_bytes,
        routing_page_bytes,
        routing_page_index_bytes
    );
    assert_eq!(compaction.object_cache_misses, 4);
    assert_eq!(compaction.object_cache_hits, 0);
    assert!(
        reopened.manifest().segments.is_empty(),
        "non-resident compaction should keep segment summaries out of the active manifest"
    );
    assert_eq!(reopened.get_vector("a").unwrap(), Some(vec![0.0, 0.0]));
    assert_eq!(
        reopened
            .search_ids(
                &[0.0, 0.0],
                SearchOptions::approx(1, LeafMode::PqScan).with_max_segments(1),
            )
            .unwrap(),
        ["a"]
    );
}

#[test]
fn compact_from_empty_routing_table_skips_unrelated_routing_pages() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

    let mut index = BorsukIndex::create(IndexConfig {
        uri: uri.clone(),
        metric: VectorMetric::Euclidean,
        dimensions: 2,
        segment_max_vectors: 1,
        ram_budget_bytes: None,
    })
    .unwrap();

    let records = (0..128)
        .map(|id| VectorRecord::new(format!("v{id}"), vec![id as f32, 0.0]))
        .collect::<Vec<_>>();
    index.add(records).unwrap();
    index
        .compact(CompactionOptions {
            source_level: 0,
            target_level: 1,
            max_segments: Some(128),
            min_segments: 2,
            target_segment_max_vectors: Some(1),
            target_segment_max_radius: None,
        })
        .unwrap();
    index
        .add(vec![
            VectorRecord::new("tail-a", vec![1000.0, 0.0]),
            VectorRecord::new("tail-b", vec![1001.0, 0.0]),
        ])
        .unwrap();

    let page_refs = routing_leaf_page_paths(dir.path(), index.manifest().version);
    assert_eq!(page_refs.len(), 2);
    fs::write(
        dir.path().join(&page_refs[1]),
        b"corrupt L0-only routing page that L1 compaction must not read",
    )
    .unwrap();
    rewrite_current_with_empty_routing_table(dir.path(), index.manifest());

    let mut reopened = BorsukIndex::open(&uri).unwrap();
    assert!(reopened.manifest().segments.is_empty());

    let compaction = reopened
        .compact(CompactionOptions {
            source_level: 1,
            target_level: 2,
            max_segments: Some(1),
            min_segments: 1,
            target_segment_max_vectors: Some(2),
            target_segment_max_radius: None,
        })
        .unwrap();

    assert!(compaction.compacted);
    assert_eq!(compaction.segments_read, 1);
    assert_eq!(compaction.segments_written, 1);
    assert_eq!(reopened.get_vector("v0").unwrap(), Some(vec![0.0, 0.0]));
}

#[test]
fn compact_stops_leaf_page_reads_once_source_batch_is_covered() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

    let mut index = BorsukIndex::create(IndexConfig {
        uri,
        metric: VectorMetric::Euclidean,
        dimensions: 2,
        segment_max_vectors: 1,
        ram_budget_bytes: None,
    })
    .unwrap();

    let records = (0..130)
        .map(|id| VectorRecord::new(format!("v{id}"), vec![id as f32, 0.0]))
        .collect::<Vec<_>>();
    index.add(records).unwrap();

    let leaf_page_paths = routing_leaf_page_paths(dir.path(), index.manifest().version);
    assert_eq!(leaf_page_paths.len(), 2);
    fs::write(
        dir.path().join(&leaf_page_paths[1]),
        b"corrupt sibling source-level routing leaf page",
    )
    .unwrap();

    let compaction = index
        .compact(CompactionOptions {
            source_level: 0,
            target_level: 1,
            max_segments: Some(1),
            min_segments: 1,
            target_segment_max_vectors: Some(1),
            target_segment_max_radius: None,
        })
        .unwrap();

    assert!(compaction.compacted);
    assert_eq!(compaction.segments_read, 1);
    assert_eq!(compaction.records_rewritten, 1);
    assert_eq!(compaction.routing_page_indexes_read, 1);
    assert_eq!(
        compaction.routing_pages_read, 2,
        "selection should read the parent page and the selected L0 leaf only"
    );
    assert_eq!(compaction.graph_payloads_read, 0);
    assert_eq!(compaction.graph_bytes_read, 0);
    assert_eq!(index.get_vector("v0").unwrap(), Some(vec![0.0, 0.0]));
}

#[test]
fn compact_from_empty_routing_table_publishes_without_l0_page_index() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

    let mut index = BorsukIndex::create(IndexConfig {
        uri: uri.clone(),
        metric: VectorMetric::Euclidean,
        dimensions: 2,
        segment_max_vectors: 1,
        ram_budget_bytes: None,
    })
    .unwrap();

    let records = (0..130)
        .map(|id| VectorRecord::new(format!("v{id}"), vec![id as f32, 0.0]))
        .collect::<Vec<_>>();
    index.add(records).unwrap();
    index
        .compact(CompactionOptions {
            source_level: 0,
            target_level: 1,
            max_segments: Some(2),
            min_segments: 2,
            target_segment_max_vectors: Some(2),
            target_segment_max_radius: None,
        })
        .unwrap();

    let l1_page_paths = routing_layer_page_index_paths(dir.path(), index.manifest().version, 1);
    assert_eq!(l1_page_paths.len(), 1);
    rewrite_current_with_empty_routing_table(dir.path(), index.manifest());
    write_corrupt_l0_page_index(
        dir.path(),
        index.manifest().version,
        b"corrupt global L0 routing page index that scoped compaction must not read",
    );

    let mut reopened = BorsukIndex::open(&uri).unwrap();
    assert!(reopened.manifest().segments.is_empty());

    let compaction = reopened
        .compact(CompactionOptions {
            source_level: 1,
            target_level: 2,
            max_segments: Some(1),
            min_segments: 1,
            target_segment_max_vectors: Some(2),
            target_segment_max_radius: None,
        })
        .unwrap();

    assert!(compaction.compacted);
    assert_eq!(compaction.segments_read, 1);
    assert_eq!(compaction.segments_written, 1);
    assert!(reopened.manifest().segments.is_empty());
    assert_eq!(reopened.get_vector("v0").unwrap(), Some(vec![0.0, 0.0]));
}

#[test]
fn compact_overflow_from_empty_routing_table_publishes_without_l0_page_index() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

    let mut index = BorsukIndex::create(IndexConfig {
        uri: uri.clone(),
        metric: VectorMetric::Euclidean,
        dimensions: 2,
        segment_max_vectors: 65,
        ram_budget_bytes: None,
    })
    .unwrap();

    let records = (0..(130 * 65))
        .map(|id| VectorRecord::new(format!("v{id}"), vec![id as f32, 0.0]))
        .collect::<Vec<_>>();
    index.add(records).unwrap();
    index
        .compact(CompactionOptions {
            source_level: 0,
            target_level: 1,
            max_segments: Some(2),
            min_segments: 2,
            target_segment_max_vectors: Some(65),
            target_segment_max_radius: None,
        })
        .unwrap();

    let l1_page_paths = routing_layer_page_index_paths(dir.path(), index.manifest().version, 1);
    assert_eq!(l1_page_paths.len(), 1);
    rewrite_current_with_empty_routing_table(dir.path(), index.manifest());
    write_corrupt_l0_page_index(
        dir.path(),
        index.manifest().version,
        b"corrupt global L0 routing page index that overflow compaction must not read",
    );

    let mut reopened = BorsukIndex::open(&uri).unwrap();
    assert!(reopened.manifest().segments.is_empty());

    let compaction = reopened
        .compact(CompactionOptions {
            source_level: 1,
            target_level: 2,
            max_segments: Some(2),
            min_segments: 2,
            target_segment_max_vectors: Some(1),
            target_segment_max_radius: None,
        })
        .unwrap();

    assert!(compaction.compacted);
    assert_eq!(compaction.segments_read, 2);
    assert_eq!(compaction.records_rewritten, 130);
    assert!(reopened.manifest().segments.is_empty());
    assert_eq!(reopened.get_vector("v0").unwrap(), Some(vec![0.0, 0.0]));
    assert_eq!(reopened.get_vector("v129").unwrap(), Some(vec![129.0, 0.0]));
}

#[test]
fn compact_from_empty_routing_table_selects_source_batch_across_pages() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

    let mut index = BorsukIndex::create(IndexConfig {
        uri: uri.clone(),
        metric: VectorMetric::Euclidean,
        dimensions: 2,
        segment_max_vectors: 1,
        ram_budget_bytes: None,
    })
    .unwrap();

    index
        .add(vec![VectorRecord::new("first", vec![0.0, 0.0])])
        .unwrap();
    index
        .compact(CompactionOptions {
            source_level: 0,
            target_level: 1,
            max_segments: Some(1),
            min_segments: 1,
            target_segment_max_vectors: Some(1),
            target_segment_max_radius: None,
        })
        .unwrap();

    let tail_records = (0..129)
        .map(|id| VectorRecord::new(format!("tail-{id}"), vec![1000.0 + id as f32, 0.0]))
        .collect::<Vec<_>>();
    index.add(tail_records).unwrap();
    index
        .compact(CompactionOptions {
            source_level: 0,
            target_level: 1,
            max_segments: Some(1),
            min_segments: 1,
            target_segment_max_vectors: Some(1),
            target_segment_max_radius: None,
        })
        .unwrap();

    let page_refs = routing_leaf_page_paths(dir.path(), index.manifest().version);
    assert!(
        page_refs.len() >= 2,
        "setup must leave source-level segments spread across routing leaf pages"
    );
    rewrite_current_with_empty_routing_table(dir.path(), index.manifest());

    let mut reopened = BorsukIndex::open(&uri).unwrap();
    assert!(reopened.manifest().segments.is_empty());

    let compaction = reopened
        .compact(CompactionOptions {
            source_level: 1,
            target_level: 2,
            max_segments: Some(2),
            min_segments: 2,
            target_segment_max_vectors: Some(2),
            target_segment_max_radius: None,
        })
        .unwrap();

    assert!(compaction.compacted);
    assert_eq!(compaction.segments_read, 2);
    assert_eq!(compaction.segments_written, 1);
    assert_eq!(compaction.records_rewritten, 2);
    assert!(
        reopened.manifest().segments.is_empty(),
        "non-resident compaction should keep segment summaries out of the active manifest"
    );
    assert_eq!(reopened.get_vector("first").unwrap(), Some(vec![0.0, 0.0]));
    assert_eq!(
        reopened.get_vector("tail-0").unwrap(),
        Some(vec![1000.0, 0.0])
    );
}

#[test]
fn compact_reuses_unaffected_routing_layer_page_objects() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

    let mut index = BorsukIndex::create(IndexConfig {
        uri,
        metric: VectorMetric::Euclidean,
        dimensions: 2,
        segment_max_vectors: 1,
        ram_budget_bytes: None,
    })
    .unwrap();

    let stable_records = (0..128)
        .map(|id| VectorRecord::new(format!("stable-{id}"), vec![id as f32, 0.0]))
        .collect::<Vec<_>>();
    index.add(stable_records).unwrap();
    index
        .compact(CompactionOptions {
            source_level: 0,
            target_level: 1,
            max_segments: Some(128),
            min_segments: 2,
            target_segment_max_vectors: Some(1),
            target_segment_max_radius: None,
        })
        .unwrap();
    index
        .add(vec![
            VectorRecord::new("tail-a", vec![1000.0, 0.0]),
            VectorRecord::new("tail-b", vec![1001.0, 0.0]),
        ])
        .unwrap();

    let before_l0_page_objects =
        collect_files_with_extension(dir.path().join("routing/pages/L0"), "parquet");
    let before_l1_page_objects =
        collect_files_with_extension(dir.path().join("routing/pages/L1"), "parquet");
    let before_l1_segments =
        collect_files_with_extension(dir.path().join("segments/L1"), "parquet");
    let before_l1_graphs = collect_files_with_extension(dir.path().join("graphs/L1"), "parquet");
    let before_page_refs = routing_leaf_page_paths(dir.path(), index.manifest().version);
    assert_eq!(before_page_refs.len(), 2);
    let unchanged_page_ref = before_page_refs[0].clone();

    let report = index
        .compact(CompactionOptions {
            source_level: 0,
            target_level: 1,
            max_segments: Some(2),
            min_segments: 2,
            target_segment_max_vectors: Some(2),
            target_segment_max_radius: None,
        })
        .unwrap();

    let after_l0_page_objects =
        collect_files_with_extension(dir.path().join("routing/pages/L0"), "parquet");
    let after_l1_page_objects =
        collect_files_with_extension(dir.path().join("routing/pages/L1"), "parquet");
    let after_l1_segments = collect_files_with_extension(dir.path().join("segments/L1"), "parquet");
    let after_l1_graphs = collect_files_with_extension(dir.path().join("graphs/L1"), "parquet");
    let after_page_refs = routing_leaf_page_paths(dir.path(), index.manifest().version);
    let new_l1_segments = files_added_after(&before_l1_segments, &after_l1_segments);
    let new_l1_graphs = files_added_after(&before_l1_graphs, &after_l1_graphs);

    assert_eq!(
        after_page_refs[0], unchanged_page_ref,
        "compaction must reuse the untouched routing page object"
    );
    assert_eq!(
        after_l0_page_objects.len(),
        before_l0_page_objects.len() + 1,
        "scoped compaction should write only the dirty leaf routing page object"
    );
    assert_eq!(
        after_l1_page_objects.len(),
        before_l1_page_objects.len() + 1,
        "scoped compaction should rewrite the derived parent routing page object"
    );
    assert_eq!(
        report.bytes_written,
        total_file_bytes(&new_l1_segments) + total_file_bytes(&new_l1_graphs),
        "paged compaction bytes_written should count new segment and graph payload bytes"
    );
}

#[test]
fn rebuild_compacts_all_matching_segments_and_deletes_obsolete_objects_when_requested() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

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

    let report = index
        .rebuild(RebuildOptions {
            source_level: 0,
            target_level: 1,
            min_segments: 1,
            target_segment_max_vectors: Some(2),
            delete_obsolete: true,
        })
        .unwrap();

    assert!(report.compaction.compacted);
    assert_eq!(report.compaction.segments_read, 4);
    assert_eq!(report.compaction.segments_written, 2);
    assert_eq!(report.compaction.records_rewritten, 4);
    assert!(!report.garbage_collection.dry_run);
    assert_eq!(report.garbage_collection.objects_deleted, 17);
    assert_eq!(report.garbage_collection.routing_objects_deleted, 3);
    assert_eq!(report.garbage_collection.tables_deleted, 6);
    assert_eq!(report.garbage_collection.candidates.len(), 17);
    assert!(
        report.garbage_collection.bytes_reclaimed > 0,
        "rebuild cleanup should reclaim obsolete L0 segment and graph bytes"
    );
    for path in &report.garbage_collection.candidates {
        assert!(
            !dir.path().join(path).exists(),
            "obsolete object `{path}` should be deleted by rebuild cleanup"
        );
    }

    let ids = index
        .search_ids(&[0.0, 0.0], SearchOptions::exact(2))
        .unwrap();
    assert_eq!(ids, vec!["a", "b"]);
    assert_eq!(
        collect_files_with_extension(dir.path().join("segments/L0"), "parquet").len(),
        0
    );
    assert_eq!(
        collect_files_with_extension(dir.path().join("graphs/L0"), "parquet").len(),
        0
    );
}

#[test]
fn gc_obsolete_segments_dry_runs_and_deletes_inactive_segments_only() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

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
            target_segment_max_radius: None,
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
        .gc_obsolete_segments(GarbageCollectionOptions {
            dry_run: true,
            min_age: Duration::ZERO,
        })
        .unwrap();
    assert_eq!(dry_run.objects_scanned, 26);
    assert_eq!(dry_run.objects_deleted, 0);
    assert_eq!(dry_run.routing_objects_deleted, 0);
    assert_eq!(dry_run.tables_deleted, 0);
    assert_eq!(dry_run.candidates.len(), 17);
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
        .gc_obsolete_segments(GarbageCollectionOptions {
            dry_run: false,
            min_age: Duration::ZERO,
        })
        .unwrap();
    assert_eq!(deleted.objects_scanned, 26);
    assert_eq!(deleted.objects_deleted, 17);
    assert_eq!(deleted.routing_objects_deleted, 3);
    assert_eq!(deleted.tables_deleted, 6);
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

    let ids = index
        .search_ids(&[8.5, 0.0], SearchOptions::exact(2))
        .unwrap();
    assert_eq!(ids, vec!["c", "d"]);
}

#[test]
fn gc_retention_protects_young_objects() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

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
            target_segment_max_radius: None,
        })
        .unwrap();

    let protected = index
        .gc_obsolete_segments(GarbageCollectionOptions {
            dry_run: false,
            min_age: Duration::from_secs(3600),
        })
        .unwrap();
    assert!(protected.candidates.is_empty());
    assert_eq!(protected.objects_deleted, 0);

    let deleted = index
        .gc_obsolete_segments(GarbageCollectionOptions {
            dry_run: false,
            min_age: Duration::ZERO,
        })
        .unwrap();
    assert!(!deleted.candidates.is_empty());
    assert!(deleted.objects_deleted > 0);
}

#[test]
fn gc_retention_protects_objects_needed_by_reader_pinned_before_compaction() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

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

    // A reader pins the pre-compaction manifest version and stays open across GC.
    let reader = BorsukIndex::open(&uri).unwrap();

    // Every pre-compaction object was created well before min_age; only its
    // obsolescence is recent.
    age_all_files(dir.path(), Duration::from_secs(7200));
    let l0_segments = collect_files_with_extension(dir.path().join("segments/L0"), "parquet");
    let l0_graphs = collect_files_with_extension(dir.path().join("graphs/L0"), "parquet");
    assert_eq!(l0_segments.len(), 4);
    assert_eq!(l0_graphs.len(), 4);

    // Compact the old objects out of the active manifest just now.
    index
        .compact(CompactionOptions {
            source_level: 0,
            target_level: 1,
            max_segments: Some(4),
            min_segments: 2,
            target_segment_max_vectors: Some(2),
            target_segment_max_radius: None,
        })
        .unwrap();

    let report = index
        .gc_obsolete_segments(GarbageCollectionOptions {
            dry_run: false,
            min_age: Duration::from_secs(3600),
        })
        .unwrap();

    // The objects became unreferenced seconds ago, so retention must keep everything
    // the pinned reader still needs, regardless of creation time.
    assert!(
        !report
            .candidates
            .iter()
            .any(|path| path.starts_with("segments/L0/") || path.starts_with("graphs/L0/")),
        "{:?}",
        report.candidates
    );
    assert_eq!(
        collect_files_with_extension(dir.path().join("segments/L0"), "parquet"),
        l0_segments
    );
    assert_eq!(
        collect_files_with_extension(dir.path().join("graphs/L0"), "parquet"),
        l0_graphs
    );
    assert_eq!(
        reader
            .search_ids(&[0.0, 0.0], SearchOptions::exact(4))
            .unwrap(),
        ["a", "b", "c", "d"]
    );

    // Once the superseding version itself is older than min_age, the pre-compaction
    // version has been obsolete for at least min_age and its objects become deletable.
    age_all_files(dir.path(), Duration::from_secs(7200));
    let aged = index
        .gc_obsolete_segments(GarbageCollectionOptions {
            dry_run: false,
            min_age: Duration::from_secs(3600),
        })
        .unwrap();
    assert!(aged.objects_deleted > 0);
    assert!(collect_files_with_extension(dir.path().join("segments/L0"), "parquet").is_empty());
    assert!(collect_files_with_extension(dir.path().join("graphs/L0"), "parquet").is_empty());
    assert_eq!(
        index
            .search_ids(&[8.5, 0.0], SearchOptions::exact(2))
            .unwrap(),
        ["c", "d"]
    );
}

#[test]
fn gc_obsolete_segments_removes_cached_inactive_objects() {
    let dir = tempfile::tempdir().unwrap();
    let cache = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

    let mut cached = BorsukIndex::create_with_cache(
        IndexConfig {
            uri,
            metric: VectorMetric::Euclidean,
            dimensions: 2,
            segment_max_vectors: 1,
            ram_budget_bytes: None,
        },
        Some(cache.path().to_path_buf()),
    )
    .unwrap();

    cached
        .add(vec![
            VectorRecord::new("a", vec![0.0, 0.0]),
            VectorRecord::new("b", vec![1.0, 0.0]),
            VectorRecord::new("c", vec![8.0, 0.0]),
            VectorRecord::new("d", vec![9.0, 0.0]),
        ])
        .unwrap();
    cached
        .compact(CompactionOptions {
            source_level: 0,
            target_level: 1,
            max_segments: Some(4),
            min_segments: 2,
            target_segment_max_vectors: Some(2),
            target_segment_max_radius: None,
        })
        .unwrap();

    assert_eq!(
        collect_files_with_extension(cache.path().join("segments/L0"), "parquet").len(),
        4
    );
    assert_eq!(
        collect_files_with_extension(cache.path().join("graphs/L0"), "parquet").len(),
        4
    );

    let deleted = cached
        .gc_obsolete_segments(GarbageCollectionOptions {
            dry_run: false,
            min_age: Duration::ZERO,
        })
        .unwrap();

    assert_eq!(deleted.objects_deleted, 17);
    assert_eq!(deleted.routing_objects_deleted, 3);
    assert_eq!(deleted.tables_deleted, 6);
    assert!(collect_files_with_extension(cache.path().join("segments/L0"), "parquet").is_empty());
    assert!(collect_files_with_extension(cache.path().join("graphs/L0"), "parquet").is_empty());
    assert_eq!(
        collect_files_with_extension(cache.path().join("segments/L1"), "parquet").len(),
        2
    );
    assert_eq!(
        collect_files_with_extension(cache.path().join("graphs/L1"), "parquet").len(),
        2
    );
}

#[test]
fn index_rejects_vectors_with_wrong_dimension() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();
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

fn age_all_files(root: &std::path::Path, age: Duration) {
    let target = std::time::SystemTime::now() - age;
    for entry in fs::read_dir(root).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            age_all_files(&path, age);
        } else {
            fs::OpenOptions::new()
                .append(true)
                .open(&path)
                .unwrap()
                .set_modified(target)
                .unwrap();
        }
    }
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

fn storage_file_sizes(root: &std::path::Path) -> BTreeMap<String, u64> {
    fn collect(root: &std::path::Path, path: &std::path::Path, files: &mut BTreeMap<String, u64>) {
        if !path.exists() {
            return;
        }
        for entry in fs::read_dir(path).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                collect(root, &path, files);
            } else {
                let relative = path
                    .strip_prefix(root)
                    .unwrap()
                    .to_string_lossy()
                    .replace('\\', "/");
                files.insert(relative, fs::metadata(path).unwrap().len());
            }
        }
    }

    let mut files = BTreeMap::new();
    collect(root, root, &mut files);
    files
}

fn assert_add_report_matches_storage_delta(
    root: &std::path::Path,
    before: &BTreeMap<String, u64>,
    after: &BTreeMap<String, u64>,
    report: &AddReport,
    vectors_added: usize,
) {
    let added_paths = after
        .keys()
        .filter(|path| !before.contains_key(*path))
        .cloned()
        .collect::<Vec<_>>();
    let added_bytes = added_paths
        .iter()
        .map(|path| after.get(path).copied().unwrap())
        .sum::<u64>();
    let current_bytes = fs::metadata(root.join("CURRENT")).unwrap().len();
    let expected_total_bytes = added_bytes + current_bytes;

    assert_eq!(
        report.segments_written,
        added_paths
            .iter()
            .filter(|path| path.starts_with("segments/"))
            .count()
    );
    assert_eq!(
        report.graph_payloads_written,
        added_paths
            .iter()
            .filter(|path| path.starts_with("graphs/"))
            .count()
    );
    assert_eq!(
        report.routing_pages_written,
        added_paths
            .iter()
            .filter(|path| path.starts_with("routing/pages/"))
            .count()
    );
    assert_eq!(
        report.manifest_tables_written,
        added_paths
            .iter()
            .filter(|path| {
                path.starts_with("manifests/")
                    || path.starts_with("routing/segments-")
                    || path.starts_with("routing/pivots-")
                    || (path.starts_with("routing/layers/") && path.ends_with("/pages.parquet"))
            })
            .count()
    );
    assert_eq!(report.total_bytes_written, expected_total_bytes);
    assert_eq!(
        report.bytes_per_vector,
        expected_total_bytes as f64 / vectors_added as f64
    );
}

fn relative_parquet_files(root: &std::path::Path, prefix: &str) -> BTreeSet<String> {
    collect_files_with_extension(root.join(prefix), "parquet")
        .into_iter()
        .map(|path| {
            path.strip_prefix(root)
                .unwrap()
                .to_string_lossy()
                .replace('\\', "/")
        })
        .collect()
}

fn metadata_table_paths(root: &std::path::Path) -> BTreeSet<String> {
    let mut paths = relative_parquet_files(root, "manifests");
    paths.extend(
        relative_parquet_files(root, "routing")
            .into_iter()
            .filter(|path| {
                path.starts_with("routing/segments-") || path.starts_with("routing/pivots-")
            }),
    );
    paths
}

fn current_metadata_table_paths(version: u64) -> BTreeSet<String> {
    [
        format!("manifests/manifest-{version:020}.parquet"),
        format!("routing/segments-{version:020}.parquet"),
        format!("routing/pivots-{version:020}.parquet"),
    ]
    .into_iter()
    .collect()
}

fn routing_layer_index_paths_in_storage(root: &std::path::Path) -> BTreeSet<String> {
    relative_parquet_files(root, "routing/layers")
        .into_iter()
        .filter(|path| path.ends_with("/pages.parquet"))
        .collect()
}

fn current_routing_layer_index_paths(root: &std::path::Path, version: u64) -> BTreeSet<String> {
    let current_prefix = format!("routing/layers/{version:020}/");
    routing_layer_index_paths_in_storage(root)
        .into_iter()
        .filter(|path| path.starts_with(&current_prefix))
        .collect()
}

fn routing_page_paths_in_storage(root: &std::path::Path) -> BTreeSet<String> {
    relative_parquet_files(root, "routing/pages")
}

fn current_routing_page_paths(root: &std::path::Path, version: u64) -> BTreeSet<String> {
    let mut routing_level = routing_max_level_for_version(root, version);
    let mut page_paths = routing_layer_page_index_paths(root, version, routing_level);
    let mut live_paths = page_paths.iter().cloned().collect::<BTreeSet<_>>();

    while routing_level > 0 {
        let mut child_page_paths = Vec::new();
        for page_path in page_paths {
            let batch = first_parquet_batch(&root.join(page_path));
            child_page_paths.extend(page_paths_from_batch(&batch));
        }
        live_paths.extend(child_page_paths.iter().cloned());
        page_paths = child_page_paths;
        routing_level -= 1;
    }

    live_paths
}

fn files_added_after(
    before: &[std::path::PathBuf],
    after: &[std::path::PathBuf],
) -> Vec<std::path::PathBuf> {
    after
        .iter()
        .filter(|path| !before.contains(path))
        .cloned()
        .collect()
}

fn total_file_bytes(paths: &[std::path::PathBuf]) -> u64 {
    paths
        .iter()
        .map(|path| fs::metadata(path).unwrap().len())
        .sum()
}

fn rewrite_current_pivots_manifest_version(
    root: &std::path::Path,
    manifest: &Manifest,
    pivot_manifest_version: u64,
) {
    let manifest_path = root.join(format!(
        "manifests/manifest-{:020}.parquet",
        manifest.version
    ));
    let routing_path = root.join(format!("routing/segments-{:020}.parquet", manifest.version));
    let pivots_path = root.join(format!("routing/pivots-{:020}.parquet", manifest.version));
    let manifest_bytes = fs::read(manifest_path).unwrap();
    let routing_bytes = fs::read(routing_path).unwrap();
    let pivots_bytes = pivots_with_manifest_version(manifest, pivot_manifest_version);
    let checksum = current_metadata_checksum(&manifest_bytes, &routing_bytes, &pivots_bytes);

    fs::write(pivots_path, pivots_bytes).unwrap();
    fs::write(
        root.join("CURRENT"),
        encode_current_pointer(manifest.version, checksum),
    )
    .unwrap();
}

fn rewrite_current_with_empty_routing_table(root: &std::path::Path, manifest: &Manifest) {
    let manifest_path = root.join(format!(
        "manifests/manifest-{:020}.parquet",
        manifest.version
    ));
    let routing_path = root.join(format!("routing/segments-{:020}.parquet", manifest.version));
    let pivots_path = root.join(format!("routing/pivots-{:020}.parquet", manifest.version));
    let mut empty_manifest = manifest.clone();
    empty_manifest.segments.clear();

    let manifest_bytes = fs::read(manifest_path).unwrap();
    let routing_bytes = routing_with_metadata(&empty_manifest, None, None, None, None, None, None);
    let pivots_bytes = fs::read(pivots_path).unwrap();
    let checksum = current_metadata_checksum(&manifest_bytes, &routing_bytes, &pivots_bytes);

    fs::write(routing_path, routing_bytes).unwrap();
    fs::write(
        root.join("CURRENT"),
        encode_current_pointer(manifest.version, checksum),
    )
    .unwrap();
}

fn pivots_with_manifest_version(manifest: &Manifest, pivot_manifest_version: u64) -> Vec<u8> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("format_version", DataType::UInt16, false),
        Field::new("manifest_version", DataType::UInt64, false),
        Field::new("ordinal", DataType::UInt64, false),
        Field::new("pivot_id", DataType::Utf8, false),
        Field::new(
            "vector",
            DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float32, true)),
                manifest.config.dimensions as i32,
            ),
            false,
        ),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            array(UInt16Array::from_iter_values(
                manifest.pivots.iter().map(|_| 1),
            )),
            array(UInt64Array::from_iter_values(
                manifest.pivots.iter().map(|_| pivot_manifest_version),
            )),
            array(UInt64Array::from_iter_values(
                manifest.pivots.iter().map(|pivot| pivot.ordinal as u64),
            )),
            array(StringArray::from_iter_values(
                manifest.pivots.iter().map(|pivot| pivot.id.as_str()),
            )),
            array(fixed_f32_array(
                manifest.pivots.iter().map(|pivot| pivot.vector.as_slice()),
                manifest.config.dimensions,
            )),
        ],
    )
    .unwrap();

    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();
    let mut bytes = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut bytes, schema, Some(props)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    bytes
}

fn rewrite_current_routing_sizes(
    root: &std::path::Path,
    manifest: &Manifest,
    segment_size_bytes: Option<u64>,
    graph_size_bytes: Option<u64>,
) {
    rewrite_current_routing_metadata(
        root,
        manifest,
        None,
        None,
        segment_size_bytes,
        None,
        graph_size_bytes,
    );
}

fn rewrite_current_routing_leaf_mode(root: &std::path::Path, manifest: &Manifest, leaf_mode: &str) {
    rewrite_current_routing_metadata_with_leaf_mode(
        root,
        manifest,
        None,
        None,
        None,
        None,
        None,
        Some(leaf_mode),
    );
}

fn rewrite_current_graph_object(
    root: &std::path::Path,
    manifest: &Manifest,
    source_record_id: &str,
    neighbor_record_id: &str,
    neighbor_distance: f32,
) {
    rewrite_current_graph_edges(
        root,
        manifest,
        &[(source_record_id, neighbor_record_id, neighbor_distance)],
    );
}

fn rewrite_current_graph_edges(
    root: &std::path::Path,
    manifest: &Manifest,
    edges: &[(&str, &str, f32)],
) {
    let summary = &manifest.segments[0];
    let graph_bytes = graph_with_edges(summary, edges);
    let graph_checksum = blake3::hash(&graph_bytes).to_hex().to_string();
    fs::write(root.join(&summary.graph_path), &graph_bytes).unwrap();
    rewrite_current_routing_metadata(
        root,
        manifest,
        None,
        None,
        None,
        Some(graph_checksum.as_str()),
        Some(graph_bytes.len() as u64),
    );
}

fn graph_with_edges(summary: &SegmentSummary, edges: &[(&str, &str, f32)]) -> Vec<u8> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("format_version", DataType::UInt16, false),
        Field::new("segment_id", DataType::Utf8, false),
        Field::new("level", DataType::UInt8, false),
        Field::new("created_at_ms", DataType::Int64, false),
        Field::new("source_record_id", DataType::Utf8, false),
        Field::new("neighbor_record_id", DataType::Utf8, false),
        Field::new("neighbor_distance", DataType::Float32, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            array(UInt16Array::from_iter_values(edges.iter().map(|_| 1))),
            array(StringArray::from_iter_values(
                edges.iter().map(|_| summary.id.as_str()),
            )),
            array(UInt8Array::from_iter_values(
                edges.iter().map(|_| summary.level),
            )),
            array(Int64Array::from_iter_values(
                edges.iter().map(|_| summary.created_at.timestamp_millis()),
            )),
            array(StringArray::from_iter_values(
                edges.iter().map(|(source, _, _)| *source),
            )),
            array(StringArray::from_iter_values(
                edges.iter().map(|(_, neighbor, _)| *neighbor),
            )),
            array(Float32Array::from_iter_values(
                edges.iter().map(|(_, _, distance)| *distance),
            )),
        ],
    )
    .unwrap();

    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();
    let mut bytes = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut bytes, schema, Some(props)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    bytes
}

fn rewrite_current_routing_metadata(
    root: &std::path::Path,
    manifest: &Manifest,
    segment_id: Option<&str>,
    object_count: Option<u64>,
    segment_size_bytes: Option<u64>,
    graph_checksum: Option<&str>,
    graph_size_bytes: Option<u64>,
) {
    rewrite_current_routing_metadata_with_leaf_mode(
        root,
        manifest,
        segment_id,
        object_count,
        segment_size_bytes,
        graph_checksum,
        graph_size_bytes,
        None,
    );
}

#[allow(clippy::too_many_arguments)]
fn rewrite_current_routing_metadata_with_leaf_mode(
    root: &std::path::Path,
    manifest: &Manifest,
    segment_id: Option<&str>,
    object_count: Option<u64>,
    segment_size_bytes: Option<u64>,
    graph_checksum: Option<&str>,
    graph_size_bytes: Option<u64>,
    leaf_mode: Option<&str>,
) {
    let manifest_path = root.join(format!(
        "manifests/manifest-{:020}.parquet",
        manifest.version
    ));
    let routing_path = root.join(format!("routing/segments-{:020}.parquet", manifest.version));
    let pivots_path = root.join(format!("routing/pivots-{:020}.parquet", manifest.version));
    let rewritten_manifest = manifest_with_metadata(
        manifest,
        segment_id,
        object_count,
        segment_size_bytes,
        graph_checksum,
        graph_size_bytes,
        leaf_mode,
    );
    let manifest_bytes = fs::read(manifest_path).unwrap();
    let routing_bytes =
        routing_with_metadata(&rewritten_manifest, None, None, None, None, None, None);
    let pivots_bytes = fs::read(pivots_path).unwrap();
    let checksum = current_metadata_checksum(&manifest_bytes, &routing_bytes, &pivots_bytes);

    fs::write(routing_path, routing_bytes).unwrap();
    rewrite_routing_layer_pages(root, &rewritten_manifest);
    fs::write(
        root.join("CURRENT"),
        encode_current_pointer(manifest.version, checksum),
    )
    .unwrap();
}

#[allow(clippy::too_many_arguments)]
fn manifest_with_metadata(
    manifest: &Manifest,
    segment_id: Option<&str>,
    object_count: Option<u64>,
    segment_size_bytes: Option<u64>,
    graph_checksum: Option<&str>,
    graph_size_bytes: Option<u64>,
    leaf_mode: Option<&str>,
) -> Manifest {
    let mut rewritten = manifest.clone();
    for segment in &mut rewritten.segments {
        if let Some(segment_id) = segment_id {
            segment.id = segment_id.to_string();
        }
        if let Some(object_count) = object_count {
            segment.object_count = object_count as usize;
        }
        if let Some(segment_size_bytes) = segment_size_bytes {
            segment.size_bytes = segment_size_bytes;
        }
        if let Some(graph_checksum) = graph_checksum {
            segment.graph_checksum = graph_checksum.to_string();
        }
        if let Some(graph_size_bytes) = graph_size_bytes {
            segment.graph_size_bytes = graph_size_bytes;
        }
        if let Some(leaf_mode) = leaf_mode {
            segment.leaf_mode = leaf_mode.parse().unwrap();
        }
    }
    rewritten
}

fn routing_with_metadata(
    manifest: &Manifest,
    segment_id: Option<&str>,
    object_count: Option<u64>,
    segment_size_bytes: Option<u64>,
    graph_checksum: Option<&str>,
    graph_size_bytes: Option<u64>,
    leaf_mode: Option<&str>,
) -> Vec<u8> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("format_version", DataType::UInt16, false),
        Field::new("manifest_version", DataType::UInt64, false),
        Field::new("id", DataType::Utf8, false),
        Field::new("level", DataType::UInt8, false),
        Field::new("path", DataType::Utf8, false),
        Field::new("object_count", DataType::UInt64, false),
        Field::new("dimensions", DataType::UInt64, false),
        Field::new(
            "centroid",
            DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float32, true)),
                manifest.config.dimensions as i32,
            ),
            false,
        ),
        Field::new("radius", DataType::Float32, false),
        Field::new("checksum", DataType::Utf8, false),
        Field::new("size_bytes", DataType::UInt64, false),
        Field::new("graph_path", DataType::Utf8, false),
        Field::new("graph_checksum", DataType::Utf8, false),
        Field::new("graph_size_bytes", DataType::UInt64, false),
        Field::new("created_at_ms", DataType::Int64, false),
        Field::new("id_bloom", DataType::Binary, false),
        Field::new("leaf_mode", DataType::Utf8, false),
        Field::new("vector_signature_bloom", DataType::Binary, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            array(UInt16Array::from_iter_values(
                manifest.segments.iter().map(|_| 1),
            )),
            array(UInt64Array::from_iter_values(
                manifest.segments.iter().map(|_| manifest.version),
            )),
            array(StringArray::from_iter_values(
                manifest
                    .segments
                    .iter()
                    .map(|segment| segment_id.unwrap_or(segment.id.as_str())),
            )),
            array(UInt8Array::from_iter_values(
                manifest.segments.iter().map(|segment| segment.level),
            )),
            array(StringArray::from_iter_values(
                manifest
                    .segments
                    .iter()
                    .map(|segment| segment.path.as_str()),
            )),
            array(UInt64Array::from_iter_values(manifest.segments.iter().map(
                |segment| object_count.unwrap_or(segment.object_count as u64),
            ))),
            array(UInt64Array::from_iter_values(
                manifest
                    .segments
                    .iter()
                    .map(|segment| segment.dimensions as u64),
            )),
            array(fixed_f32_array(
                manifest
                    .segments
                    .iter()
                    .map(|segment| segment.centroid.as_slice()),
                manifest.config.dimensions,
            )),
            array(Float32Array::from_iter_values(
                manifest.segments.iter().map(|segment| segment.radius),
            )),
            array(StringArray::from_iter_values(
                manifest
                    .segments
                    .iter()
                    .map(|segment| segment.checksum.as_str()),
            )),
            array(UInt64Array::from_iter_values(manifest.segments.iter().map(
                |segment| segment_size_bytes.unwrap_or(segment.size_bytes),
            ))),
            array(StringArray::from_iter_values(
                manifest
                    .segments
                    .iter()
                    .map(|segment| segment.graph_path.as_str()),
            )),
            array(StringArray::from_iter_values(manifest.segments.iter().map(
                |segment| graph_checksum.unwrap_or(segment.graph_checksum.as_str()),
            ))),
            array(UInt64Array::from_iter_values(manifest.segments.iter().map(
                |segment| graph_size_bytes.unwrap_or(segment.graph_size_bytes),
            ))),
            array(Int64Array::from_iter_values(
                manifest
                    .segments
                    .iter()
                    .map(|segment| segment.created_at.timestamp_millis()),
            )),
            array(BinaryArray::from_iter_values(
                manifest
                    .segments
                    .iter()
                    .map(|segment| segment.id_bloom.as_slice()),
            )),
            array(StringArray::from_iter_values(manifest.segments.iter().map(
                |segment| {
                    leaf_mode
                        .map(ToOwned::to_owned)
                        .unwrap_or_else(|| segment.leaf_mode.to_string())
                },
            ))),
            array(BinaryArray::from_iter_values(
                manifest
                    .segments
                    .iter()
                    .map(|segment| segment.vector_signature_bloom.as_slice()),
            )),
        ],
    )
    .unwrap();

    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();
    let mut bytes = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut bytes, schema, Some(props)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    bytes
}

fn rewrite_routing_layer_pages(root: &std::path::Path, manifest: &Manifest) {
    let mut page_paths = Vec::new();
    let mut page_checksums = Vec::new();
    let mut page_segments = Vec::new();

    for (page_ordinal, segments) in manifest.segments.chunks(128).enumerate() {
        let bytes = routing_layer_page_with_segments(manifest, page_ordinal, segments);
        let checksum = blake3::hash(&bytes).to_hex().to_string();
        let relative_path = format!(
            "routing/pages/L0/{}/page-{}.parquet",
            &checksum[..2],
            checksum
        );
        let path = root.join(&relative_path);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, bytes).unwrap();
        page_paths.push(relative_path);
        page_checksums.push(checksum);
        page_segments.push(segments.len() as u64);
    }

    let index_bytes = routing_layer_page_index(manifest, page_paths, page_checksums, page_segments);
    let index_path = root.join(format!(
        "routing/layers/{:020}/L0/pages.parquet",
        manifest.version
    ));
    fs::create_dir_all(index_path.parent().unwrap()).unwrap();
    fs::write(index_path, index_bytes).unwrap();
}

fn routing_layer_page_with_segments(
    manifest: &Manifest,
    page_ordinal: usize,
    segments: &[SegmentSummary],
) -> Vec<u8> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("format_version", DataType::UInt16, false),
        Field::new("manifest_version", DataType::UInt64, false),
        Field::new("routing_level", DataType::UInt8, false),
        Field::new("page_ordinal", DataType::UInt64, false),
        Field::new("page_segments", DataType::UInt64, false),
        Field::new("segment_ordinal", DataType::UInt64, false),
        Field::new("segment_id", DataType::Utf8, false),
        Field::new("segment_level", DataType::UInt8, false),
        Field::new("object_count", DataType::UInt64, false),
        Field::new("dimensions", DataType::UInt64, false),
        Field::new(
            "centroid",
            DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float32, true)),
                manifest.config.dimensions as i32,
            ),
            false,
        ),
        Field::new("radius", DataType::Float32, false),
        Field::new("segment_path", DataType::Utf8, false),
        Field::new("segment_checksum", DataType::Utf8, false),
        Field::new("segment_size_bytes", DataType::UInt64, false),
        Field::new("graph_path", DataType::Utf8, false),
        Field::new("graph_checksum", DataType::Utf8, false),
        Field::new("graph_size_bytes", DataType::UInt64, false),
        Field::new("id_bloom", DataType::Binary, false),
        Field::new("leaf_mode", DataType::Utf8, false),
        Field::new("vector_signature_bloom", DataType::Binary, false),
        Field::new("created_at_ms", DataType::Int64, false),
    ]));
    let segment_start_ordinal = page_ordinal * 128;
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            array(UInt16Array::from_iter_values(segments.iter().map(|_| 1))),
            array(UInt64Array::from_iter_values(segments.iter().map(|_| 0))),
            array(UInt8Array::from_iter_values(segments.iter().map(|_| 0))),
            array(UInt64Array::from_iter_values(
                segments.iter().map(|_| page_ordinal as u64),
            )),
            array(UInt64Array::from_iter_values(
                segments.iter().map(|_| segments.len() as u64),
            )),
            array(UInt64Array::from_iter_values(
                segments
                    .iter()
                    .enumerate()
                    .map(|(index, _)| (segment_start_ordinal + index) as u64),
            )),
            array(StringArray::from_iter_values(
                segments.iter().map(|segment| segment.id.as_str()),
            )),
            array(UInt8Array::from_iter_values(
                segments.iter().map(|segment| segment.level),
            )),
            array(UInt64Array::from_iter_values(
                segments.iter().map(|segment| segment.object_count as u64),
            )),
            array(UInt64Array::from_iter_values(
                segments.iter().map(|segment| segment.dimensions as u64),
            )),
            array(fixed_f32_array(
                segments.iter().map(|segment| segment.centroid.as_slice()),
                manifest.config.dimensions,
            )),
            array(Float32Array::from_iter_values(
                segments.iter().map(|segment| segment.radius),
            )),
            array(StringArray::from_iter_values(
                segments.iter().map(|segment| segment.path.as_str()),
            )),
            array(StringArray::from_iter_values(
                segments.iter().map(|segment| segment.checksum.as_str()),
            )),
            array(UInt64Array::from_iter_values(
                segments.iter().map(|segment| segment.size_bytes),
            )),
            array(StringArray::from_iter_values(
                segments.iter().map(|segment| segment.graph_path.as_str()),
            )),
            array(StringArray::from_iter_values(
                segments
                    .iter()
                    .map(|segment| segment.graph_checksum.as_str()),
            )),
            array(UInt64Array::from_iter_values(
                segments.iter().map(|segment| segment.graph_size_bytes),
            )),
            array(BinaryArray::from_iter_values(
                segments.iter().map(|segment| segment.id_bloom.as_slice()),
            )),
            array(StringArray::from_iter_values(
                segments.iter().map(|segment| segment.leaf_mode.to_string()),
            )),
            array(BinaryArray::from_iter_values(
                segments
                    .iter()
                    .map(|segment| segment.vector_signature_bloom.as_slice()),
            )),
            array(Int64Array::from_iter_values(
                segments
                    .iter()
                    .map(|segment| segment.created_at.timestamp_millis()),
            )),
        ],
    )
    .unwrap();

    write_parquet_batch(batch, schema)
}

fn routing_layer_page_index(
    manifest: &Manifest,
    page_paths: Vec<String>,
    page_checksums: Vec<String>,
    page_segments: Vec<u64>,
) -> Vec<u8> {
    let page_summaries = manifest
        .segments
        .chunks(128)
        .map(|segments| {
            let centroid = routing_layer_page_centroid(manifest.config.dimensions, segments);
            let radius = routing_layer_page_radius(manifest, segments, &centroid);
            (centroid, radius)
        })
        .collect::<Vec<_>>();
    let schema = Arc::new(Schema::new(vec![
        Field::new("format_version", DataType::UInt16, false),
        Field::new("manifest_version", DataType::UInt64, false),
        Field::new("routing_level", DataType::UInt8, false),
        Field::new("page_ordinal", DataType::UInt64, false),
        Field::new("page_path", DataType::Utf8, false),
        Field::new("page_checksum", DataType::Utf8, false),
        Field::new("page_segments", DataType::UInt64, false),
        Field::new("dimensions", DataType::UInt64, false),
        Field::new(
            "centroid",
            DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float32, true)),
                manifest.config.dimensions as i32,
            ),
            false,
        ),
        Field::new("radius", DataType::Float32, false),
        Field::new("id_bloom", DataType::Binary, false),
    ]));
    let page_count = page_paths.len();
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            array(UInt16Array::from_iter_values((0..page_count).map(|_| 1))),
            array(UInt64Array::from_iter_values(
                (0..page_count).map(|_| manifest.version),
            )),
            array(UInt8Array::from_iter_values((0..page_count).map(|_| 0))),
            array(UInt64Array::from_iter_values(
                (0..page_count).map(|ordinal| ordinal as u64),
            )),
            array(StringArray::from_iter_values(
                page_paths.iter().map(String::as_str),
            )),
            array(StringArray::from_iter_values(
                page_checksums.iter().map(String::as_str),
            )),
            array(UInt64Array::from_iter_values(page_segments)),
            array(UInt64Array::from_iter_values(
                (0..page_count).map(|_| manifest.config.dimensions as u64),
            )),
            array(fixed_f32_array(
                page_summaries
                    .iter()
                    .map(|(centroid, _)| centroid.as_slice()),
                manifest.config.dimensions,
            )),
            array(Float32Array::from_iter_values(
                page_summaries.iter().map(|(_, radius)| *radius),
            )),
            array(BinaryArray::from_iter_values(
                manifest
                    .segments
                    .chunks(128)
                    .map(routing_layer_page_id_bloom)
                    .map(|bloom| bloom.into_iter().collect::<Vec<_>>())
                    .collect::<Vec<_>>()
                    .iter()
                    .map(Vec::as_slice),
            )),
        ],
    )
    .unwrap();

    write_parquet_batch(batch, schema)
}

fn routing_layer_page_centroid(dimensions: usize, segments: &[SegmentSummary]) -> Vec<f32> {
    let total_objects = segments
        .iter()
        .map(|segment| segment.object_count)
        .sum::<usize>()
        .max(1);
    let mut centroid = vec![0.0_f32; dimensions];
    for segment in segments {
        let weight = segment.object_count as f32 / total_objects as f32;
        for (coordinate, value) in centroid.iter_mut().zip(&segment.centroid) {
            *coordinate += value * weight;
        }
    }
    centroid
}

fn routing_layer_page_radius(
    manifest: &Manifest,
    segments: &[SegmentSummary],
    centroid: &[f32],
) -> f32 {
    segments.iter().fold(0.0_f32, |radius, segment| {
        let center_distance = manifest
            .config
            .metric
            .distance(centroid, &segment.centroid)
            .unwrap();
        radius.max(center_distance + segment.radius)
    })
}

fn routing_layer_page_id_bloom(segments: &[SegmentSummary]) -> Vec<u8> {
    let mut bloom = vec![0_u8; 128];
    for segment in segments {
        if segment.id_bloom.len() != bloom.len() {
            return Vec::new();
        }
        for (target, source) in bloom.iter_mut().zip(&segment.id_bloom) {
            *target |= source;
        }
    }
    bloom
}

fn write_parquet_batch(batch: RecordBatch, schema: Arc<Schema>) -> Vec<u8> {
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();
    let mut bytes = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut bytes, schema, Some(props)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    bytes
}

fn current_metadata_checksum(
    manifest_bytes: &[u8],
    routing_bytes: &[u8],
    pivots_bytes: &[u8],
) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    update_current_hasher(&mut hasher, b"manifest", manifest_bytes);
    update_current_hasher(&mut hasher, b"routing", routing_bytes);
    update_current_hasher(&mut hasher, b"pivots", pivots_bytes);
    *hasher.finalize().as_bytes()
}

fn update_current_hasher(hasher: &mut blake3::Hasher, label: &[u8], bytes: &[u8]) {
    hasher.update(&(label.len() as u64).to_le_bytes());
    hasher.update(label);
    hasher.update(&(bytes.len() as u64).to_le_bytes());
    hasher.update(bytes);
}

fn encode_current_pointer(version: u64, metadata_checksum: [u8; 32]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(46);
    bytes.extend_from_slice(b"BORS");
    bytes.extend_from_slice(&1_u16.to_le_bytes());
    bytes.extend_from_slice(&version.to_le_bytes());
    bytes.extend_from_slice(&metadata_checksum);
    bytes
}

fn array(array: impl Array + 'static) -> ArrayRef {
    Arc::new(array)
}

fn fixed_f32_array<'a>(
    values: impl IntoIterator<Item = &'a [f32]>,
    dimensions: usize,
) -> FixedSizeListArray {
    let values = values
        .into_iter()
        .map(|vector| Some(vector.iter().copied().map(Some).collect::<Vec<_>>()))
        .collect::<Vec<_>>();
    FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(values, dimensions as i32)
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

fn collect_files_with_file_name<'a>(
    files: &'a [std::path::PathBuf],
    file_name: &str,
) -> Vec<&'a std::path::PathBuf> {
    files
        .iter()
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name == file_name)
        })
        .collect()
}

fn collect_files_with_path_component<'a>(
    files: &'a [std::path::PathBuf],
    component: &str,
) -> Vec<&'a std::path::PathBuf> {
    files
        .iter()
        .filter(|path| {
            path.components()
                .any(|part| part.as_os_str().to_string_lossy() == component)
        })
        .collect()
}

fn routing_layer_page_index_paths(
    root: &std::path::Path,
    version: u64,
    routing_level: u8,
) -> Vec<String> {
    let index_path = root.join(format!(
        "routing/layers/{version:020}/L{routing_level}/pages.parquet"
    ));
    let batch = first_parquet_batch(&index_path);
    page_paths_from_batch(&batch)
}

fn routing_leaf_page_paths(root: &std::path::Path, version: u64) -> Vec<String> {
    let mut routing_level = routing_max_level_for_version(root, version);
    let mut page_paths = routing_layer_page_index_paths(root, version, routing_level);

    while routing_level > 0 {
        let mut child_page_paths = Vec::new();
        for page_path in page_paths {
            let batch = first_parquet_batch(&root.join(page_path));
            child_page_paths.extend(page_paths_from_batch(&batch));
        }
        page_paths = child_page_paths;
        routing_level -= 1;
    }

    page_paths
}

fn routing_page_paths_at_level(
    root: &std::path::Path,
    version: u64,
    target_level: u8,
) -> Vec<String> {
    let mut routing_level = routing_max_level_for_version(root, version);
    let mut page_paths = routing_layer_page_index_paths(root, version, routing_level);

    while routing_level > target_level {
        let mut child_page_paths = Vec::new();
        for page_path in page_paths {
            let batch = first_parquet_batch(&root.join(page_path));
            child_page_paths.extend(page_paths_from_batch(&batch));
        }
        page_paths = child_page_paths;
        routing_level -= 1;
    }

    page_paths
}

fn routing_max_level_for_version(root: &std::path::Path, version: u64) -> u8 {
    let layer_root = root.join(format!("routing/layers/{version:020}"));
    fs::read_dir(layer_root)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .filter_map(|name| name.strip_prefix('L')?.parse::<u8>().ok())
        .max()
        .unwrap_or(0)
}

fn page_paths_from_batch(batch: &RecordBatch) -> Vec<String> {
    let column = batch
        .column(
            batch
                .schema()
                .index_of("page_path")
                .expect("routing page index must include page_path"),
        )
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("page_path must be a string column");

    (0..batch.num_rows())
        .map(|row| column.value(row).to_string())
        .collect()
}

struct RoutingLeafSegment {
    level: u8,
    size_bytes: u64,
    leaf_mode: LeafMode,
}

fn routing_leaf_page_segments(root: &std::path::Path, version: u64) -> Vec<RoutingLeafSegment> {
    let mut segments = Vec::new();
    for page_path in routing_leaf_page_paths(root, version) {
        let batch = first_parquet_batch(&root.join(page_path));
        let level_column = batch
            .column(
                batch
                    .schema()
                    .index_of("segment_level")
                    .expect("routing leaf page must include segment_level"),
            )
            .as_any()
            .downcast_ref::<UInt8Array>()
            .expect("segment_level must be a u8 column");
        let size_column = batch
            .column(
                batch
                    .schema()
                    .index_of("segment_size_bytes")
                    .expect("routing leaf page must include segment_size_bytes"),
            )
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("segment_size_bytes must be a u64 column");
        let leaf_mode_column = batch
            .column(
                batch
                    .schema()
                    .index_of("leaf_mode")
                    .expect("routing leaf page must include leaf_mode"),
            )
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("leaf_mode must be a string column");
        segments.extend((0..batch.num_rows()).map(|row| RoutingLeafSegment {
            level: level_column.value(row),
            size_bytes: size_column.value(row),
            leaf_mode: leaf_mode_column.value(row).parse().unwrap(),
        }));
    }
    segments
}

fn write_corrupt_l0_page_index(root: &std::path::Path, version: u64, bytes: &[u8]) {
    let path = root.join(format!("routing/layers/{version:020}/L0/pages.parquet"));
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(path, bytes).unwrap();
}

fn routing_layer_page_index_page_records(
    root: &std::path::Path,
    version: u64,
    routing_level: u8,
) -> Vec<u64> {
    let index_path = root.join(format!(
        "routing/layers/{version:020}/L{routing_level}/pages.parquet"
    ));
    let batch = first_parquet_batch(&index_path);
    let column = batch
        .column(
            batch
                .schema()
                .index_of("page_records")
                .expect("routing page index must include page_records"),
        )
        .as_any()
        .downcast_ref::<UInt64Array>()
        .expect("page_records must be a u64 column");

    (0..batch.num_rows()).map(|row| column.value(row)).collect()
}

fn routing_layer_page_index_leaf_segments(
    root: &std::path::Path,
    version: u64,
    routing_level: u8,
) -> Vec<u64> {
    let index_path = root.join(format!(
        "routing/layers/{version:020}/L{routing_level}/pages.parquet"
    ));
    let batch = first_parquet_batch(&index_path);
    let column = batch
        .column(
            batch
                .schema()
                .index_of("leaf_segments")
                .expect("routing page index must include leaf_segments"),
        )
        .as_any()
        .downcast_ref::<UInt64Array>()
        .expect("leaf_segments must be a u64 column");

    (0..batch.num_rows()).map(|row| column.value(row)).collect()
}

fn hit_ids(report: borsuk::SearchReport) -> Vec<String> {
    report
        .hits
        .into_iter()
        .map(|hit| hit.id.to_utf8_string().unwrap())
        .collect()
}

fn recall_overlap(exact_ids: &[String], actual_ids: &[String], k: usize) -> f64 {
    let exact_top = exact_ids.iter().take(k).cloned().collect::<BTreeSet<_>>();
    if exact_top.is_empty() {
        return 1.0;
    }
    let actual_top = actual_ids.iter().take(k).cloned().collect::<BTreeSet<_>>();
    let overlap = exact_top.intersection(&actual_top).count();
    overlap as f64 / exact_top.len() as f64
}

fn prefetch_test_records(count: usize) -> Vec<VectorRecord> {
    (0..count)
        .map(|id| VectorRecord::new(format!("v{id}"), vec![id as f32, 0.0]))
        .collect()
}

fn first_parquet_batch(path: &std::path::Path) -> RecordBatch {
    let bytes = fs::read(path).unwrap();
    ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(bytes))
        .unwrap()
        .build()
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
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

fn list_object_paths(store: Arc<dyn ObjectStore>) -> Vec<String> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let mut paths = runtime
        .block_on(async {
            store
                .list(Some(&ObjectPath::from("")))
                .map_ok(|meta| meta.location.to_string())
                .try_collect::<Vec<_>>()
                .await
        })
        .unwrap();
    paths.sort();
    paths
}

#[test]
fn segment_cache_shares_decoded_segments_across_searches() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

    let mut index = BorsukIndex::create(IndexConfig {
        uri: uri.clone(),
        metric: VectorMetric::Euclidean,
        dimensions: 2,
        segment_max_vectors: 3,
        ram_budget_bytes: None,
    })
    .unwrap();
    let vectors = (0..12)
        .map(|i| vec![i as f32, (i % 3) as f32])
        .collect::<Vec<_>>();
    index.add_vectors(vectors).unwrap();
    assert!(index.stats().segments > 1);

    // Open with a decoded-segment cache but no on-disk byte cache, so the only
    // way a second search avoids re-decoding is the shared Arc<Segment> cache.
    let reopened = BorsukIndex::open_with_options(
        &uri,
        OpenOptions {
            segment_cache_max_bytes: Some(64 * 1024 * 1024),
            ..OpenOptions::default()
        },
    )
    .unwrap();

    let query = vec![4.0, 1.0];
    let first = reopened
        .search_with_report(&query, SearchOptions::exact(3))
        .unwrap();
    let second = reopened
        .search_with_report(&query, SearchOptions::exact(3))
        .unwrap();

    let first_ids = first.hits.iter().map(|h| h.id.clone()).collect::<Vec<_>>();
    let second_ids = second.hits.iter().map(|h| h.id.clone()).collect::<Vec<_>>();
    assert_eq!(
        first_ids, second_ids,
        "cached search must return the same hits"
    );

    // Cold pass decodes and caches every routed segment; the warm pass serves
    // them from the shared decoded cache: more memory hits, fewer bytes read.
    assert_eq!(first.object_cache_hits, 0);
    assert!(
        second.object_cache_hits > 0,
        "second search should hit the decoded-segment cache"
    );
    assert!(
        second.bytes_read < first.bytes_read,
        "cached search should read fewer bytes ({} vs {})",
        second.bytes_read,
        first.bytes_read
    );
}

#[test]
fn admission_gate_serializes_concurrent_searches_correctly() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

    let mut index = BorsukIndex::create(IndexConfig {
        uri: uri.clone(),
        metric: VectorMetric::Euclidean,
        dimensions: 2,
        segment_max_vectors: 3,
        ram_budget_bytes: None,
    })
    .unwrap();
    let vectors = (0..12)
        .map(|i| vec![i as f32, (i % 3) as f32])
        .collect::<Vec<_>>();
    let ids = index.add_vectors(vectors).unwrap();
    let expected = ids[4].clone();

    let reopened = Arc::new(
        BorsukIndex::open_with_options(
            &uri,
            OpenOptions {
                max_concurrent_searches: Some(1),
                ..OpenOptions::default()
            },
        )
        .unwrap(),
    );

    let handles = (0..8)
        .map(|_| {
            let index = Arc::clone(&reopened);
            let query = vec![4.0, 1.0];
            std::thread::spawn(move || index.search_ids(&query, SearchOptions::exact(1)).unwrap())
        })
        .collect::<Vec<_>>();
    for handle in handles {
        let hits = handle.join().expect("search worker should not panic");
        assert_eq!(hits, vec![expected.clone()]);
    }
}

#[test]
fn projected_pq_scan_matches_full_decode() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

    let mut index = BorsukIndex::create(IndexConfig {
        uri: uri.clone(),
        metric: VectorMetric::Euclidean,
        dimensions: 4,
        segment_max_vectors: 8,
        ram_budget_bytes: None,
    })
    .unwrap();
    let vectors = (0..16)
        .map(|i| vec![i as f32, (i % 4) as f32, (i % 3) as f32, (i % 5) as f32])
        .collect::<Vec<_>>();
    index.add_vectors(vectors).unwrap();
    assert!(index.stats().segments > 1);

    let query = vec![3.0, 1.0, 2.0, 1.0];
    let options = || SearchOptions::approx(4, LeafMode::PqScan).with_max_candidates_per_segment(4);

    // Projected path: no decoded cache + pq-scan + budget below segment length.
    let projected = BorsukIndex::open(&uri).unwrap();
    let projected_report = projected.search_with_report(&query, options()).unwrap();

    // Reference: the decoded cache forces the full-decode path (no projection).
    let full = BorsukIndex::open_with_options(
        &uri,
        OpenOptions {
            segment_cache_max_bytes: Some(8 * 1024 * 1024),
            ..OpenOptions::default()
        },
    )
    .unwrap();
    let full_report = full.search_with_report(&query, options()).unwrap();

    let projected_hits: Vec<_> = projected_report
        .hits
        .iter()
        .map(|hit| (hit.id.clone(), hit.distance))
        .collect();
    let full_hits: Vec<_> = full_report
        .hits
        .iter()
        .map(|hit| (hit.id.clone(), hit.distance))
        .collect();
    assert_eq!(
        projected_hits, full_hits,
        "projected pq-scan must match full decode exactly"
    );
    assert!(!projected_hits.is_empty());

    // Projected path returns correct vectors for the hits.
    let projected_vectors = projected.search_vectors(&query, options()).unwrap();
    let full_vectors = full.search_vectors(&query, options()).unwrap();
    assert_eq!(projected_vectors, full_vectors);
    assert!(projected_vectors.iter().all(|vector| vector.len() == 4));
}

#[test]
fn reports_expose_object_store_request_counts() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

    let mut index = BorsukIndex::create(IndexConfig {
        uri: uri.clone(),
        metric: VectorMetric::Euclidean,
        dimensions: 2,
        segment_max_vectors: 2,
        ram_budget_bytes: None,
    })
    .unwrap();

    let vectors: Vec<Vec<f32>> = (0..6).map(|id| vec![id as f32, 0.0]).collect();
    let ids: Vec<String> = (0..6).map(|id| format!("v{id}")).collect();
    let (_ids, add_report) = index.add_with_report(vectors, Some(ids)).unwrap();
    assert!(
        add_report.requests.puts > 0,
        "publishing segments must issue PUT requests: {:?}",
        add_report.requests
    );
    assert!(
        add_report.requests.total() >= add_report.requests.puts,
        "total must account for every counted request: {:?}",
        add_report.requests
    );

    // Default (paged) open resolves segments from routing pages on read.
    let reader = BorsukIndex::open(&uri).unwrap();
    let report = reader
        .search_with_report(&[0.0, 0.0], SearchOptions::approx(3, LeafMode::PqScan))
        .unwrap();
    assert!(
        report.requests.gets > 0,
        "a paged search must issue GET requests: {:?}",
        report.requests
    );
    assert_eq!(
        report.requests.total(),
        report.requests.gets
            + report.requests.puts
            + report.requests.deletes
            + report.requests.heads
            + report.requests.lists
    );
    assert_eq!(report.requests.puts, 0, "search must not write");
}

#[test]
fn delete_hides_records_from_search_and_get_and_keeps_tombstone_object() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

    let mut index = BorsukIndex::create(IndexConfig {
        uri: uri.clone(),
        metric: VectorMetric::Euclidean,
        dimensions: 2,
        segment_max_vectors: 4,
        ram_budget_bytes: None,
    })
    .unwrap();

    index
        .add(vec![
            VectorRecord::new("alpha", vec![0.0, 0.0]),
            VectorRecord::new("beta", vec![1.0, 0.0]),
            VectorRecord::new("gamma", vec![2.0, 0.0]),
        ])
        .unwrap();

    // Delete beta. Report reflects the newly tombstoned id.
    let report = index.delete_with_report(["beta"]).unwrap();
    assert_eq!(report.deleted, 1);
    assert_eq!(report.total_tombstoned, 1);
    assert!(report.published);
    assert!(report.requests.total() > 0);

    // get_vector returns None for the deleted id, still returns live ones.
    assert_eq!(index.get_vector("beta").unwrap(), None);
    assert_eq!(index.get_vector("alpha").unwrap(), Some(vec![0.0, 0.0]));

    // Search excludes the deleted id: nearest to beta's location is now alpha/gamma.
    let ids = index
        .search_ids(&[1.0, 0.0], SearchOptions::exact(3))
        .unwrap();
    assert!(
        !ids.contains(&"beta".to_string()),
        "deleted id must not appear: {ids:?}"
    );
    assert!(ids.contains(&"alpha".to_string()));
    assert!(ids.contains(&"gamma".to_string()));

    // Deleting again is a no-op (idempotent), no new version published.
    let again = index.delete_with_report(["beta"]).unwrap();
    assert_eq!(again.deleted, 0);
    assert!(!again.published);

    // Reopen (paged default) and confirm the tombstone survives + still filters.
    let reopened = BorsukIndex::open(&uri).unwrap();
    assert_eq!(reopened.get_vector("beta").unwrap(), None);

    // GC (live) must not delete the active tombstone object.
    let mut gc_index = BorsukIndex::open(&uri).unwrap();
    let gc = gc_index
        .gc_obsolete_segments(GarbageCollectionOptions {
            dry_run: false,
            min_age: std::time::Duration::ZERO,
        })
        .unwrap();
    assert!(!gc.dry_run);
    // The deleted id is still filtered after GC — the tombstone object was kept.
    let after_gc = BorsukIndex::open(&uri).unwrap();
    assert_eq!(after_gc.get_vector("beta").unwrap(), None);
    assert_eq!(after_gc.get_vector("alpha").unwrap(), Some(vec![0.0, 0.0]));
}

#[test]
fn compaction_reclaims_deleted_rows_and_readd_is_blocked() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

    let mut index = BorsukIndex::create(IndexConfig {
        uri: uri.clone(),
        metric: VectorMetric::Euclidean,
        dimensions: 2,
        segment_max_vectors: 2,
        ram_budget_bytes: None,
    })
    .unwrap();

    // Two L0 segments of two records each.
    index
        .add(vec![
            VectorRecord::new("a", vec![0.0, 0.0]),
            VectorRecord::new("b", vec![1.0, 0.0]),
            VectorRecord::new("c", vec![2.0, 0.0]),
            VectorRecord::new("d", vec![3.0, 0.0]),
        ])
        .unwrap();
    assert_eq!(index.stats().records, 4);

    index.delete(["b", "c"]).unwrap();
    // Re-adding a deleted id is blocked until purge.
    let readd = index.add(vec![VectorRecord::new("b", vec![9.0, 9.0])]);
    assert!(readd.is_err(), "re-add of a deleted id must be rejected");

    // Compact L0 -> L1: tombstoned rows are physically dropped.
    let report = index
        .compact(CompactionOptions {
            source_level: 0,
            target_level: 1,
            max_segments: None,
            min_segments: 2,
            target_segment_max_vectors: Some(4),
            target_segment_max_radius: None,
        })
        .unwrap();
    assert!(report.compacted);
    assert_eq!(
        report.records_rewritten, 2,
        "only live rows survive compaction"
    );
    assert_eq!(index.stats().records, 2);

    // Reopen and confirm only live records remain and deleted stay gone.
    let reopened = BorsukIndex::open(&uri).unwrap();
    assert_eq!(reopened.get_vector("a").unwrap(), Some(vec![0.0, 0.0]));
    assert_eq!(reopened.get_vector("d").unwrap(), Some(vec![3.0, 0.0]));
    assert_eq!(reopened.get_vector("b").unwrap(), None);
    assert_eq!(reopened.get_vector("c").unwrap(), None);
    let ids = reopened
        .search_ids(&[1.0, 0.0], SearchOptions::exact(4))
        .unwrap();
    assert_eq!(ids.len(), 2, "only live records are returned: {ids:?}");
}

#[test]
fn purge_clears_tombstone_and_reenables_readd() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

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
            VectorRecord::new("c", vec![2.0, 0.0]),
            VectorRecord::new("d", vec![3.0, 0.0]),
        ])
        .unwrap();

    index.delete(["b", "c"]).unwrap();
    assert_eq!(
        index.stats().records,
        4,
        "rows still physically present pre-purge"
    );

    let report = index.purge_with_report().unwrap();
    assert!(report.published);
    assert_eq!(report.records_purged, 2);
    assert_eq!(report.tombstones_cleared, 2);
    assert!(report.segments_rewritten >= 1);
    assert!(report.requests.total() > 0);

    // Rows physically gone; live records intact.
    assert_eq!(index.stats().records, 2);
    assert_eq!(index.get_vector("a").unwrap(), Some(vec![0.0, 0.0]));
    assert_eq!(index.get_vector("b").unwrap(), None);

    // Re-adding a purged id now succeeds and is searchable.
    index
        .add(vec![VectorRecord::new("b", vec![1.5, 0.0])])
        .unwrap();
    assert_eq!(index.get_vector("b").unwrap(), Some(vec![1.5, 0.0]));

    // Reopen (paged) and confirm the rebuilt index is consistent.
    let reopened = BorsukIndex::open(&uri).unwrap();
    let ids = reopened
        .search_ids(&[1.4, 0.0], SearchOptions::exact(5))
        .unwrap();
    assert!(ids.contains(&"b".to_string()));
    assert!(!ids.contains(&"c".to_string()));

    // A no-op purge (nothing deleted) reports zero and does not error.
    let noop = reopened_mut(&uri).purge_with_report().unwrap();
    assert_eq!(noop.records_purged, 0);
    assert_eq!(noop.tombstones_cleared, 0);
}

fn reopened_mut(uri: &str) -> BorsukIndex {
    BorsukIndex::open(uri).unwrap()
}

#[test]
fn compaction_radius_cap_splits_spread_out_bubbles() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

    let mut index = BorsukIndex::create(IndexConfig {
        uri: uri.clone(),
        metric: VectorMetric::Euclidean,
        dimensions: 2,
        segment_max_vectors: 2,
        ram_budget_bytes: None,
    })
    .unwrap();

    // Two well-separated clusters of two points each.
    index
        .add(vec![
            VectorRecord::new("a0", vec![0.0, 0.0]),
            VectorRecord::new("a1", vec![0.1, 0.0]),
            VectorRecord::new("b0", vec![100.0, 0.0]),
            VectorRecord::new("b1", vec![100.1, 0.0]),
        ])
        .unwrap();

    // Count-only compaction into a big leaf would mix both clusters into one
    // large-radius bubble. A radius cap of 1.0 forces a split at the cluster gap.
    let report = index
        .compact(CompactionOptions {
            source_level: 0,
            target_level: 1,
            max_segments: None,
            min_segments: 2,
            target_segment_max_vectors: Some(4),
            target_segment_max_radius: Some(1.0),
        })
        .unwrap();
    assert!(report.compacted);
    assert!(
        report.segments_written >= 2,
        "radius cap must split the two clusters: {} segments",
        report.segments_written
    );

    // Every compacted segment's bubble radius stays within the cap.
    let reopened = open_resident(&uri).unwrap();
    for summary in &reopened.manifest().segments {
        assert!(
            summary.radius <= 1.0 + 1e-4,
            "segment radius {} exceeds the cap",
            summary.radius
        );
    }

    // Results are still correct.
    let ids = reopened
        .search_ids(&[100.05, 0.0], SearchOptions::exact(2))
        .unwrap();
    assert!(ids.contains(&"b0".to_string()) && ids.contains(&"b1".to_string()));

    // Zero radius is rejected before any storage read.
    assert!(
        index
            .compact(CompactionOptions {
                source_level: 1,
                target_level: 2,
                max_segments: None,
                min_segments: 1,
                target_segment_max_vectors: None,
                target_segment_max_radius: Some(0.0),
            })
            .is_err()
    );
}

#[test]
fn maintenance_coordinates_instances_via_membership_and_leases() {
    use borsuk::MaintenanceConfig;
    let store: std::sync::Arc<dyn ObjectStore> = std::sync::Arc::new(InMemory::new());
    let uri = "memory:///maintenance";

    let mut writer = BorsukIndex::create_with_object_store(
        std::sync::Arc::clone(&store),
        IndexConfig {
            uri: uri.to_string(),
            metric: VectorMetric::Euclidean,
            dimensions: 2,
            segment_max_vectors: 2,
            ram_budget_bytes: None,
        },
    )
    .unwrap();
    // Enough L0 segments that a compaction pass has work to do.
    writer
        .add(vec![
            VectorRecord::new("a", vec![0.0, 0.0]),
            VectorRecord::new("b", vec![1.0, 0.0]),
            VectorRecord::new("c", vec![2.0, 0.0]),
            VectorRecord::new("d", vec![3.0, 0.0]),
        ])
        .unwrap();

    let mut instance_a =
        BorsukIndex::open_with_object_store(std::sync::Arc::clone(&store), uri).unwrap();
    let mut instance_b =
        BorsukIndex::open_with_object_store(std::sync::Arc::clone(&store), uri).unwrap();

    // First instance heartbeats and sees only itself.
    let report_a = instance_a
        .run_maintenance_once(&MaintenanceConfig::new("instance-a"))
        .unwrap();
    assert_eq!(report_a.active_instances, 1);
    assert_eq!(report_a.instance_rank, 0);
    // As the only live instance it owns every shard, so it ran maintenance on
    // the L0 batch (incremental split/merge and/or compaction).
    assert!(
        report_a.incremental || report_a.compacted,
        "sole instance should run maintenance"
    );

    // Second instance heartbeats; now the live membership is two.
    let report_b = instance_b
        .run_maintenance_once(&MaintenanceConfig::new("instance-b"))
        .unwrap();
    // instance_b's pass reading two fresh heartbeats proves membership works.
    assert_eq!(
        report_b.active_instances, 2,
        "membership should include both"
    );
    assert!(report_b.instance_rank <= 1);

    // Running maintenance again on instance_a keeps membership at two and stays
    // consistent (idempotent when there is nothing left to compact).
    let report_a2 = instance_a
        .run_maintenance_once(&MaintenanceConfig::new("instance-a"))
        .unwrap();
    assert_eq!(report_a2.active_instances, 2);
}

#[test]
fn incremental_maintenance_splits_oversized_bubbles() {
    use borsuk::IncrementalMaintenanceOptions;
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

    let mut index = BorsukIndex::create(IndexConfig {
        uri: uri.clone(),
        metric: VectorMetric::Euclidean,
        dimensions: 2,
        segment_max_vectors: 100,
        ram_budget_bytes: None,
    })
    .unwrap();
    let records: Vec<VectorRecord> = (0..300)
        .map(|id| VectorRecord::new(format!("v{id}"), vec![id as f32, 0.0]))
        .collect();
    index.add(records).unwrap();
    let before = index.stats().segments;
    assert_eq!(before, 3, "three 100-vector segments");

    // Treat >50 vectors as oversized: every segment splits locally.
    let report = index
        .run_incremental_maintenance(IncrementalMaintenanceOptions {
            max_segment_vectors: 50,
            max_segment_radius: None,
            min_segment_vectors: 0,
            max_operations: 8,
        })
        .unwrap();
    assert!(report.published);
    assert_eq!(report.splits, 3);
    assert_eq!(report.merges, 0);
    assert!(index.stats().segments > before, "splitting adds segments");
    assert_eq!(index.stats().records, 300, "no records lost");

    // Search still correct after local splits.
    let reopened = BorsukIndex::open(&uri).unwrap();
    let ids = reopened
        .search_ids(&[10.0, 0.0], SearchOptions::exact(1))
        .unwrap();
    assert_eq!(ids, ["v10"]);
}

#[test]
fn incremental_maintenance_merges_sparse_bubbles_after_deletes() {
    use borsuk::IncrementalMaintenanceOptions;
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

    let mut index = BorsukIndex::create(IndexConfig {
        uri: uri.clone(),
        metric: VectorMetric::Euclidean,
        dimensions: 2,
        segment_max_vectors: 100,
        ram_budget_bytes: None,
    })
    .unwrap();
    let records: Vec<VectorRecord> = (0..300)
        .map(|id| VectorRecord::new(format!("v{id}"), vec![id as f32, 0.0]))
        .collect();
    index.add(records).unwrap();
    assert_eq!(index.stats().segments, 3);

    // Delete most records so each segment becomes sparse.
    // Interleaved deletes: keep every 20th id, so each of the three segments
    // keeps ~5 live records and stays sparse-but-nonempty.
    let deleted: Vec<String> = (0..300)
        .filter(|id| id % 20 != 0)
        .map(|id| format!("v{id}"))
        .collect();
    index.delete(deleted).unwrap();

    // Merge segments whose live count is below 50; also reclaims the deleted rows.
    let report = index
        .run_incremental_maintenance(IncrementalMaintenanceOptions {
            max_segment_vectors: 100,
            max_segment_radius: None,
            min_segment_vectors: 50,
            max_operations: 8,
        })
        .unwrap();
    assert!(report.published);
    assert!(report.merges >= 1, "sparse segments must merge: {report:?}");
    assert_eq!(report.splits, 0);
    // The 15 surviving records are consolidated; deleted rows are physically gone.
    assert_eq!(index.stats().records, 15, "merge dropped tombstoned rows");
    assert!(index.stats().segments < 3, "merges reduce segment count");

    let reopened = BorsukIndex::open(&uri).unwrap();
    assert_eq!(reopened.get_vector("v280").unwrap(), Some(vec![280.0, 0.0]));
    assert_eq!(reopened.get_vector("v10").unwrap(), None);
}

#[test]
fn incremental_maintenance_shards_split_in_parallel_across_nodes() {
    use borsuk::IncrementalMaintenanceOptions;
    // Two nodes share one blob store and each compacts a disjoint slice of the
    // bubbles. The rebase-safe delta publish composes their concurrent manifest
    // updates, so no split is lost when the second node publishes over the first.
    let store: std::sync::Arc<dyn ObjectStore> = std::sync::Arc::new(InMemory::new());
    let uri = "memory:///parallel-maintenance";

    let mut writer = BorsukIndex::create_with_object_store(
        std::sync::Arc::clone(&store),
        IndexConfig {
            uri: uri.to_string(),
            metric: VectorMetric::Euclidean,
            dimensions: 2,
            segment_max_vectors: 100,
            ram_budget_bytes: None,
        },
    )
    .unwrap();
    let records: Vec<VectorRecord> = (0..800)
        .map(|id| VectorRecord::new(format!("v{id}"), vec![id as f32, 0.0]))
        .collect();
    writer.add(records).unwrap();
    let before = writer.stats().segments;
    assert_eq!(before, 8, "eight 100-vector segments");

    let options = || IncrementalMaintenanceOptions {
        max_segment_vectors: 50,
        max_segment_radius: None,
        min_segment_vectors: 0,
        max_operations: 64,
    };

    // Two independent handles open the same store (each starts from the same
    // 8-segment snapshot) and each runs its own shard of two.
    let mut node_a =
        BorsukIndex::open_with_object_store(std::sync::Arc::clone(&store), uri).unwrap();
    let mut node_b =
        BorsukIndex::open_with_object_store(std::sync::Arc::clone(&store), uri).unwrap();

    let report_a = node_a
        .run_incremental_maintenance_shard(options(), 0, 2)
        .unwrap();
    let report_b = node_b
        .run_incremental_maintenance_shard(options(), 1, 2)
        .unwrap();

    assert!(report_a.published && report_b.published);
    // Every original segment is split exactly once, and each shard handled a
    // disjoint subset — their split counts partition the eight segments.
    assert_eq!(
        report_a.splits + report_b.splits,
        8,
        "shards must together cover every segment exactly once: {report_a:?} {report_b:?}"
    );

    // The final published index reflects BOTH nodes' work: all segments split
    // (16 total) and no records were dropped by the concurrent publishes.
    let reopened = BorsukIndex::open_with_object_store(std::sync::Arc::clone(&store), uri).unwrap();
    assert_eq!(reopened.stats().segments, 16, "every bubble split in two");
    assert_eq!(reopened.stats().records, 800, "no records lost on rebase");
    let ids = reopened
        .search_ids(&[404.0, 0.0], SearchOptions::exact(1))
        .unwrap();
    assert_eq!(ids, ["v404"]);
}

#[test]
fn background_maintenance_thread_runs_and_stops_cleanly() {
    use borsuk::MaintenanceConfig;
    use std::time::Duration;
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

    let mut index = BorsukIndex::create(IndexConfig {
        uri: uri.clone(),
        metric: VectorMetric::Euclidean,
        dimensions: 2,
        segment_max_vectors: 2,
        ram_budget_bytes: None,
    })
    .unwrap();
    // Several L0 segments so a background compaction pass has work to do.
    index
        .add(vec![
            VectorRecord::new("a", vec![0.0, 0.0]),
            VectorRecord::new("b", vec![1.0, 0.0]),
            VectorRecord::new("c", vec![2.0, 0.0]),
            VectorRecord::new("d", vec![3.0, 0.0]),
            VectorRecord::new("e", vec![4.0, 0.0]),
            VectorRecord::new("f", vec![5.0, 0.0]),
        ])
        .unwrap();
    drop(index);

    // Spawn the background thread. It opens its own handle on the same store and
    // loops run_maintenance_once on a short interval.
    let handle = BorsukIndex::start_background_maintenance(
        uri.clone(),
        OpenOptions::default(),
        MaintenanceConfig::new("bg-node"),
        Duration::from_millis(50),
    );

    // Poll for the heartbeat object the loop writes each pass — proof the thread
    // actually ran maintenance rather than just spinning.
    let heartbeat = dir.path().join("maintenance/instances/bg-node");
    let mut ran = false;
    for _ in 0..40 {
        if heartbeat.exists() {
            ran = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    handle.stop();
    assert!(ran, "background thread should heartbeat within the timeout");

    // Store stays consistent and queryable after the thread stops.
    let reopened = BorsukIndex::open(&uri).unwrap();
    assert_eq!(reopened.stats().records, 6, "no records lost");
    let ids = reopened
        .search_ids(&[2.0, 0.0], SearchOptions::exact(1))
        .unwrap();
    assert_eq!(ids, ["c"]);
}

#[test]
fn metadata_is_stored_and_returned_by_get_record() {
    use borsuk::MetaValue;
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();
    let mut index = BorsukIndex::create(IndexConfig {
        uri: uri.clone(),
        metric: VectorMetric::Euclidean,
        dimensions: 2,
        segment_max_vectors: 8,
        ram_budget_bytes: None,
    })
    .unwrap();

    let meta = borsuk::Metadata::from([
        ("year".to_string(), MetaValue::Int(2021)),
        ("genre".to_string(), MetaValue::Str("comedy".to_string())),
        (
            "tags".to_string(),
            MetaValue::List(vec![MetaValue::Str("award".to_string())]),
        ),
    ]);
    index
        .add(vec![
            VectorRecord::new("a", vec![0.0, 0.0]).with_metadata(meta.clone()),
            VectorRecord::new("b", vec![1.0, 0.0]),
        ])
        .unwrap();

    // Same handle and a fresh reopen both return the stored metadata.
    assert_eq!(
        index.get_record("a").unwrap(),
        Some((vec![0.0, 0.0], meta.clone()))
    );
    assert_eq!(
        index.get_record("b").unwrap(),
        Some((vec![1.0, 0.0], borsuk::Metadata::new()))
    );
    let reopened = BorsukIndex::open(&uri).unwrap();
    assert_eq!(
        reopened.get_record("a").unwrap(),
        Some((vec![0.0, 0.0], meta))
    );
    // get_vector still works and ignores metadata.
    assert_eq!(reopened.get_vector("a").unwrap(), Some(vec![0.0, 0.0]));
}

#[test]
fn compaction_preserves_metadata() {
    use borsuk::MetaValue;
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();
    let mut index = BorsukIndex::create(IndexConfig {
        uri: uri.clone(),
        metric: VectorMetric::Euclidean,
        dimensions: 2,
        segment_max_vectors: 2,
        ram_budget_bytes: None,
    })
    .unwrap();

    // Two L0 segments of two records each, every record carrying metadata.
    let records: Vec<VectorRecord> = (0..4)
        .map(|i| {
            VectorRecord::new(format!("v{i}"), vec![i as f32, 0.0]).with_metadata(
                borsuk::Metadata::from([("i".to_string(), MetaValue::Int(i))]),
            )
        })
        .collect();
    index.add(records).unwrap();

    index
        .compact(CompactionOptions {
            source_level: 0,
            target_level: 1,
            max_segments: None,
            min_segments: 2,
            target_segment_max_vectors: Some(4),
            target_segment_max_radius: None,
        })
        .unwrap();

    let reopened = BorsukIndex::open(&uri).unwrap();
    for i in 0..4 {
        assert_eq!(
            reopened.get_record(&format!("v{i}")).unwrap(),
            Some((
                vec![i as f32, 0.0],
                borsuk::Metadata::from([("i".to_string(), MetaValue::Int(i))])
            ))
        );
    }
}

#[test]
fn segment_metadata_stats_persist_and_enable_pruning() {
    use borsuk::{Filter, MetaValue, Op};
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();
    let mut index = BorsukIndex::create(IndexConfig {
        uri: uri.clone(),
        metric: VectorMetric::Euclidean,
        dimensions: 2,
        segment_max_vectors: 2,
        ram_budget_bytes: None,
    })
    .unwrap();

    // First segment holds old years, second holds recent years.
    let years = [2001_i64, 2002, 2020, 2021];
    let records: Vec<VectorRecord> = years
        .iter()
        .enumerate()
        .map(|(i, year)| {
            VectorRecord::new(format!("v{i}"), vec![i as f32, 0.0]).with_metadata(
                borsuk::Metadata::from([("year".to_string(), MetaValue::Int(*year))]),
            )
        })
        .collect();
    index.add(records).unwrap();

    // Reopen resident so the manifest carries the persisted per-segment stats.
    let reopened = open_resident(&uri).unwrap();
    assert_eq!(reopened.manifest().segments.len(), 2);
    let recent = Filter::Cmp {
        path: "year".to_string(),
        op: Op::Gte,
        value: MetaValue::Int(2020),
    };
    let can_match: Vec<bool> = reopened
        .manifest()
        .segments
        .iter()
        .map(|s| s.metadata_stats.can_match(&recent))
        .collect();
    // The old-years segment is pruned; the recent one is kept.
    assert!(
        can_match.contains(&false),
        "a segment must be prunable: {can_match:?}"
    );
    assert!(
        can_match.contains(&true),
        "a segment must be searchable: {can_match:?}"
    );
}

#[test]
fn filtered_search_returns_only_matching_records_with_metadata_and_pruning() {
    use borsuk::{Filter, MetaValue, Op};
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();
    let mut index = BorsukIndex::create(IndexConfig {
        uri: uri.clone(),
        metric: VectorMetric::Euclidean,
        dimensions: 2,
        segment_max_vectors: 2,
        ram_budget_bytes: None,
    })
    .unwrap();

    // Six records along the x axis; genre alternates so filtered results are
    // spread across segments. seg-size 2 => 3 L0 segments.
    let genres = ["comedy", "drama", "comedy", "drama", "comedy", "drama"];
    let records: Vec<VectorRecord> = genres
        .iter()
        .enumerate()
        .map(|(i, genre)| {
            VectorRecord::new(format!("v{i}"), vec![i as f32, 0.0]).with_metadata(
                borsuk::Metadata::from([
                    ("genre".to_string(), MetaValue::Str((*genre).to_string())),
                    ("year".to_string(), MetaValue::Int(2000 + i as i64)),
                ]),
            )
        })
        .collect();
    index.add(records).unwrap();

    let comedy = Filter::Cmp {
        path: "genre".to_string(),
        op: Op::Eq,
        value: MetaValue::Str("comedy".to_string()),
    };

    // Query near x=0; the 3 nearest comedies are v0, v2, v4.
    let report = index
        .search_with_report(
            &[0.0, 0.0],
            SearchOptions::exact(3)
                .with_filter(comedy.clone())
                .with_include_metadata(true),
        )
        .unwrap();
    let ids: Vec<String> = report.hits.iter().map(|h| h.id.to_string()).collect();
    assert_eq!(ids, ["v0", "v2", "v4"], "only comedies, nearest first");
    // Every hit carries its metadata and actually matches the filter.
    for hit in &report.hits {
        let meta = hit.metadata.as_ref().expect("include_metadata");
        assert_eq!(
            meta.get("genre"),
            Some(&MetaValue::Str("comedy".to_string()))
        );
    }
    assert!(report.rows_evaluated >= 3);
    assert!(report.rows_passed_filter >= 3);

    // A filter no segment can satisfy prunes them all and returns nothing.
    let none = index
        .search_with_report(
            &[0.0, 0.0],
            SearchOptions::exact(3).with_filter(Filter::Cmp {
                path: "genre".to_string(),
                op: Op::Eq,
                value: MetaValue::Str("horror".to_string()),
            }),
        )
        .unwrap();
    assert!(none.hits.is_empty());
    assert!(
        none.segments_pruned_by_filter > 0,
        "horror is absent everywhere"
    );

    // Default search (no include_metadata) omits metadata.
    let plain = index
        .search_with_report(&[0.0, 0.0], SearchOptions::exact(1).with_filter(comedy))
        .unwrap();
    assert_eq!(plain.hits[0].id.to_string(), "v0");
    assert!(plain.hits[0].metadata.is_none());
}

#[test]
fn approx_filtered_search_prefilters_matches_outside_the_candidate_window() {
    use borsuk::{Filter, MetaValue, Op};
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();
    let mut index = BorsukIndex::create(IndexConfig {
        uri,
        metric: VectorMetric::Euclidean,
        dimensions: 2,
        segment_max_vectors: 128,
        ram_budget_bytes: None,
    })
    .unwrap();

    // One segment of 53 rows along the x axis. Only three far-away rows (x=50,
    // 51, 52) match the filter; the 50 rows nearest the query are non-matching.
    let records: Vec<VectorRecord> = (0..53)
        .map(|i| {
            let genre = if i >= 50 { "rare" } else { "common" };
            VectorRecord::new(format!("v{i}"), vec![i as f32, 0.0]).with_metadata(
                borsuk::Metadata::from([("genre".to_string(), MetaValue::Str(genre.to_string()))]),
            )
        })
        .collect();
    index.add(records).unwrap();

    let rare = Filter::Cmp {
        path: "genre".to_string(),
        op: Op::Eq,
        value: MetaValue::Str("rare".to_string()),
    };

    // A tight per-segment candidate budget: the five vector-nearest rows are all
    // "common", so a rank-then-filter scan would find no match in this window.
    // The prefilter instead ranks the three actual matches and returns the
    // nearest, v50 -- the same answer an exact search gives.
    let approx = index
        .search_ids(
            &[0.0, 0.0],
            SearchOptions::approx(1, LeafMode::PqScan)
                .with_max_candidates_per_segment(5)
                .with_filter(rare.clone()),
        )
        .unwrap();
    assert_eq!(
        approx,
        ["v50"],
        "prefilter finds matches outside the window"
    );

    let exact = index
        .search_ids(&[0.0, 0.0], SearchOptions::exact(1).with_filter(rare))
        .unwrap();
    assert_eq!(
        approx, exact,
        "approx prefilter agrees with exact ground truth"
    );
}

#[test]
fn list_records_paginates_live_records_and_skips_deleted() {
    use borsuk::MetaValue;
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();
    let mut index = BorsukIndex::create(IndexConfig {
        uri,
        metric: VectorMetric::Euclidean,
        dimensions: 2,
        segment_max_vectors: 2, // several segments
        ram_budget_bytes: None,
    })
    .unwrap();

    let records: Vec<VectorRecord> = (0..6)
        .map(|i| {
            VectorRecord::new(format!("v{i}"), vec![i as f32, 0.0]).with_metadata(
                borsuk::Metadata::from([("n".to_string(), MetaValue::Int(i as i64))]),
            )
        })
        .collect();
    index.add(records).unwrap();

    // Full listing returns every live record with its vector + metadata.
    let all = index.list_records(0, 100).unwrap();
    assert_eq!(all.len(), 6);
    let ids: BTreeSet<String> = all.iter().map(|(id, _, _)| id.to_string()).collect();
    assert_eq!(ids, (0..6).map(|i| format!("v{i}")).collect());
    let v3 = all
        .iter()
        .find(|(id, _, _)| id.as_bytes() == b"v3")
        .unwrap();
    assert_eq!(v3.1, vec![3.0, 0.0]);
    assert_eq!(v3.2.get("n"), Some(&MetaValue::Int(3)));

    // Pagination: offset + limit slices the stream without overlap.
    let first = index.list_records(0, 4).unwrap();
    let rest = index.list_records(4, 100).unwrap();
    assert_eq!(first.len(), 4);
    assert_eq!(rest.len(), 2);
    let paged: BTreeSet<String> = first
        .iter()
        .chain(rest.iter())
        .map(|(id, _, _)| id.to_string())
        .collect();
    assert_eq!(paged, ids);

    // Deleted records are skipped.
    index
        .delete(vec!["v2".to_string(), "v4".to_string()])
        .unwrap();
    let live: BTreeSet<String> = index
        .list_records(0, 100)
        .unwrap()
        .iter()
        .map(|(id, _, _)| id.to_string())
        .collect();
    assert_eq!(
        live,
        ["v0", "v1", "v3", "v5"]
            .iter()
            .map(|s| s.to_string())
            .collect()
    );
}
