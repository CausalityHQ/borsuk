#![allow(missing_docs)]

#[allow(dead_code)]
mod common;

use std::{collections::BTreeMap, sync::Arc, time::Duration};

use borsuk::{
    BorsukIndex, GarbageCollectionOptions, GroupCommitConfig, GroupCommitWriter, IndexConfig,
    SearchOptions, VectorMetric, VectorRecord, VectorSpec, WalConfig,
};
use futures_util::TryStreamExt;
use object_store::{ObjectStore, memory::InMemory, path::Path as ObjectPath};

const LARGE_OBJECT_BYTES: usize = 64 * 1024 * 1024 + 1;

#[test]
fn rejected_lane_head_publication_is_not_visible() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let uri = "memory:///pending-commit-rejected";
    drop(
        BorsukIndex::create_with_object_store(
            Arc::clone(&inner),
            IndexConfig {
                uri: uri.to_string(),
                metric: VectorMetric::Euclidean,
                dimensions: 2,
                segment_max_vectors: 16,
                ram_budget_bytes: None,
                text: false,
                named_vectors: BTreeMap::new(),
            },
        )
        .unwrap(),
    );
    let faulting: Arc<dyn ObjectStore> =
        Arc::new(common::FaultInjectingObjectStore::fail_nth_matching(
            Arc::clone(&inner),
            9,
            false,
            |operation, path| {
                operation == common::StoreOperation::Put && path.as_ref().ends_with("/HEAD")
            },
        ));
    let writer = GroupCommitWriter::new(
        BorsukIndex::open_with_object_store(faulting, uri).unwrap(),
        GroupCommitConfig {
            max_delay: Duration::ZERO,
            max_records: 1,
            worker_lanes: 1,
        },
    )
    .unwrap();

    assert!(
        writer
            .append(vec![VectorRecord::new("rejected", vec![1.0, 0.0])])
            .is_err()
    );
    drop(writer);
    assert_eq!(
        BorsukIndex::open_with_object_store(Arc::clone(&inner), uri)
            .unwrap()
            .get_vector("rejected")
            .unwrap(),
        None
    );
}

#[test]
fn accepted_retryable_lane_head_is_acknowledged_once() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let uri = "memory:///pending-commit-accepted-retryable";
    drop(
        BorsukIndex::create_with_object_store(
            Arc::clone(&inner),
            IndexConfig {
                uri: uri.to_string(),
                metric: VectorMetric::Euclidean,
                dimensions: 2,
                segment_max_vectors: 16,
                ram_budget_bytes: None,
                text: false,
                named_vectors: BTreeMap::new(),
            },
        )
        .unwrap(),
    );
    let faulting: Arc<dyn ObjectStore> =
        Arc::new(common::FaultInjectingObjectStore::accept_then_fail_nth_put(
            Arc::clone(&inner),
            9,
            |operation, path| {
                operation == common::StoreOperation::Put && path.as_ref().ends_with("/HEAD")
            },
        ));
    let writer = GroupCommitWriter::new(
        BorsukIndex::open_with_object_store(faulting, uri).unwrap(),
        GroupCommitConfig {
            max_delay: Duration::ZERO,
            max_records: 1,
            worker_lanes: 1,
        },
    )
    .unwrap();

    writer
        .append(vec![VectorRecord::new("durable", vec![1.0, 0.0])])
        .unwrap();
    drop(writer);
    let reopened = BorsukIndex::open_with_object_store(Arc::clone(&inner), uri).unwrap();
    assert_eq!(
        reopened.get_vector("durable").unwrap(),
        Some(vec![1.0, 0.0])
    );
    assert_eq!(reopened.list_records(0, 2).unwrap().len(), 1);
}

#[test]
fn multimodal_collection_transaction_is_invisible_when_root_publication_fails() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let uri = "memory:///multimodal-root-publication-failure";
    let mut setup = BorsukIndex::create_with_object_store(
        Arc::clone(&inner),
        IndexConfig {
            uri: uri.to_string(),
            metric: VectorMetric::Euclidean,
            dimensions: 2,
            segment_max_vectors: 16,
            ram_budget_bytes: None,
            text: false,
            named_vectors: BTreeMap::from([(
                "named".to_string(),
                VectorSpec {
                    dimensions: 2,
                    metric: VectorMetric::Euclidean,
                    kind: Default::default(),
                    element_type: Default::default(),
                },
            )]),
        },
    )
    .unwrap();
    setup
        .add(vec![
            VectorRecord::new("base", vec![10.0, 0.0]).with_named_vector("named", vec![10.0, 0.0]),
        ])
        .unwrap();
    drop(setup);

    let faulting: Arc<dyn ObjectStore> =
        Arc::new(common::FaultInjectingObjectStore::fail_nth_matching(
            Arc::clone(&inner),
            1,
            false,
            |operation, path| {
                operation == common::StoreOperation::Put
                    && path.as_ref().starts_with("collection/write-epochs/")
                    && path.as_ref().contains("/pending/")
                    && path.as_ref().ends_with(".commit")
            },
        ));
    let mut writer = BorsukIndex::open_with_object_store(faulting, uri).unwrap();
    writer
        .add(vec![
            VectorRecord::new("uncommitted", vec![0.0, 0.0])
                .with_named_vector("named", vec![0.0, 0.0]),
        ])
        .unwrap_err();
    drop(writer);

    let reopened = BorsukIndex::open_with_object_store(Arc::clone(&inner), uri).unwrap();
    assert_eq!(
        reopened
            .search_ids(&[0.0, 0.0], SearchOptions::exact(2))
            .unwrap(),
        ["base"]
    );
    assert_eq!(
        reopened
            .search_ids(
                &[0.0, 0.0],
                SearchOptions::exact(2).with_vector_name("named"),
            )
            .unwrap(),
        ["base"]
    );
}

#[test]
fn transient_root_publication_error_is_resolved_before_returning() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let uri = "memory:///transient-root-publication-error";
    drop(
        BorsukIndex::create_with_object_store(
            Arc::clone(&inner),
            IndexConfig {
                uri: uri.to_string(),
                metric: VectorMetric::Euclidean,
                dimensions: 2,
                segment_max_vectors: 16,
                ram_budget_bytes: None,
                text: false,
                named_vectors: BTreeMap::new(),
            },
        )
        .unwrap(),
    );

    let faulting: Arc<dyn ObjectStore> =
        Arc::new(common::FaultInjectingObjectStore::accept_then_fail_nth_put(
            Arc::clone(&inner),
            1,
            |operation, path| {
                operation == common::StoreOperation::Put
                    && path.as_ref().starts_with("collection/write-epochs/")
                    && path.as_ref().contains("/pending/")
                    && path.as_ref().ends_with(".commit")
            },
        ));
    let mut writer = BorsukIndex::open_with_object_store(faulting, uri).unwrap();
    writer
        .add(vec![VectorRecord::new("committed", vec![0.0, 0.0])])
        .unwrap();
    drop(writer);

    let reopened = BorsukIndex::open_with_object_store(Arc::clone(&inner), uri).unwrap();
    assert_eq!(
        reopened
            .search_ids(&[0.0, 0.0], SearchOptions::exact(1))
            .unwrap(),
        ["committed"]
    );
}

#[test]
fn vector_report_api_does_not_ack_when_pending_publication_fails() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let uri = "memory:///vector-report-root-reservation-order";
    drop(
        BorsukIndex::create_with_object_store(
            Arc::clone(&inner),
            IndexConfig {
                uri: uri.to_string(),
                metric: VectorMetric::Euclidean,
                dimensions: 2,
                segment_max_vectors: 16,
                ram_budget_bytes: None,
                text: false,
                named_vectors: BTreeMap::new(),
            },
        )
        .unwrap(),
    );

    let faulting: Arc<dyn ObjectStore> =
        Arc::new(common::FaultInjectingObjectStore::fail_nth_matching(
            Arc::clone(&inner),
            1,
            false,
            |operation, path| {
                operation == common::StoreOperation::Put
                    && path.as_ref().starts_with("collection/write-epochs/")
                    && path.as_ref().contains("/pending/")
                    && path.as_ref().ends_with(".commit")
            },
        ));
    let mut writer = BorsukIndex::open_with_object_store(faulting, uri).unwrap();
    writer
        .add_with_report(vec![vec![0.0, 0.0]], Some(vec!["uncommitted".to_string()]))
        .unwrap_err();
    drop(writer);

    let runtime = tokio::runtime::Runtime::new().unwrap();
    let objects = runtime
        .block_on(inner.list(None).try_collect::<Vec<_>>())
        .unwrap();
    assert!(objects.iter().all(|object| {
        !object
            .location
            .as_ref()
            .starts_with("collection/write-epochs/")
            || !object.location.as_ref().contains("/pending/")
    }));
}

#[test]
fn collection_transaction_is_invisible_when_pending_publication_fails() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let uri = "memory:///collection-frontier-failure";
    let mut setup = BorsukIndex::create_with_object_store(
        Arc::clone(&inner),
        IndexConfig {
            uri: uri.to_string(),
            metric: VectorMetric::Euclidean,
            dimensions: 2,
            segment_max_vectors: 16,
            ram_budget_bytes: None,
            text: false,
            named_vectors: BTreeMap::new(),
        },
    )
    .unwrap();
    setup
        .add(vec![VectorRecord::new("base", vec![10.0, 0.0])])
        .unwrap();
    drop(setup);

    let faulting: Arc<dyn ObjectStore> =
        Arc::new(common::FaultInjectingObjectStore::fail_nth_matching(
            Arc::clone(&inner),
            1,
            false,
            |operation, path| {
                operation == common::StoreOperation::Put
                    && path.as_ref().starts_with("collection/write-epochs/")
                    && path.as_ref().contains("/pending/")
                    && path.as_ref().ends_with(".commit")
            },
        ));
    let mut writer = BorsukIndex::open_with_object_store(faulting, uri).unwrap();
    writer
        .add(vec![VectorRecord::new("uncommitted", vec![0.0, 0.0])])
        .unwrap_err();
    drop(writer);

    let mut reopened = BorsukIndex::open_with_object_store(Arc::clone(&inner), uri).unwrap();
    assert_eq!(
        reopened
            .search_ids(&[0.0, 0.0], SearchOptions::exact(2))
            .unwrap(),
        ["base"]
    );
    // Advance the manifest-time safety fence and run cleanup. Recent immutable
    // WAL objects retain a reservation-TTL safety grace, but the failed
    // transaction must remain invisible throughout cleanup.
    reopened.flush().unwrap();
    reopened
        .gc_obsolete_segments(GarbageCollectionOptions {
            dry_run: false,
            min_age: Duration::ZERO,
        })
        .unwrap();
    assert_eq!(
        reopened
            .search_ids(&[0.0, 0.0], SearchOptions::exact(2))
            .unwrap(),
        ["base"]
    );
}

#[test]
fn modality_prepare_failure_prunes_already_prepared_primary_lane_history() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let uri = "memory:///collection-modality-prepare-failure";
    drop(
        BorsukIndex::create_with_object_store(
            Arc::clone(&inner),
            IndexConfig {
                uri: uri.to_string(),
                metric: VectorMetric::Euclidean,
                dimensions: 2,
                segment_max_vectors: 16,
                ram_budget_bytes: None,
                text: false,
                named_vectors: BTreeMap::from([(
                    "named".to_string(),
                    VectorSpec {
                        dimensions: 2,
                        metric: VectorMetric::Euclidean,
                        kind: Default::default(),
                        element_type: Default::default(),
                    },
                )]),
            },
        )
        .unwrap(),
    );

    let faulting: Arc<dyn ObjectStore> =
        Arc::new(common::FaultInjectingObjectStore::fail_nth_matching(
            Arc::clone(&inner),
            1,
            true,
            |operation, path| {
                operation == common::StoreOperation::Put
                    && path.as_ref().starts_with("vectors/named/cells/")
                    && path.as_ref().contains("/runs/records/")
            },
        ));
    let mut writer = BorsukIndex::open_with_object_store(faulting, uri).unwrap();
    writer
        .add(vec![
            VectorRecord::new("uncommitted", vec![0.0, 0.0])
                .with_named_vector("named", vec![0.0, 0.0]),
        ])
        .unwrap_err();
    drop(writer);

    let mut reopened = BorsukIndex::open_with_object_store(Arc::clone(&inner), uri).unwrap();
    reopened
        .add(vec![
            VectorRecord::new("fence", vec![1.0, 0.0]).with_named_vector("named", vec![1.0, 0.0]),
        ])
        .unwrap();
    reopened.flush().unwrap();
    reopened
        .gc_obsolete_segments(GarbageCollectionOptions {
            dry_run: false,
            min_age: Duration::ZERO,
        })
        .unwrap();
    assert_eq!(
        reopened
            .search_ids(&[0.0, 0.0], SearchOptions::exact(2))
            .unwrap(),
        ["fence"]
    );
    assert_eq!(
        reopened
            .search_ids(
                &[0.0, 0.0],
                SearchOptions::exact(2).with_vector_name("named"),
            )
            .unwrap(),
        ["fence"]
    );
}

#[test]
fn collection_open_does_not_read_obsolete_wal_frontier_heads() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let uri = "memory:///collection-single-frontier-snapshot";
    drop(
        BorsukIndex::create_with_object_store(
            Arc::clone(&inner),
            IndexConfig {
                uri: uri.to_string(),
                metric: VectorMetric::Euclidean,
                dimensions: 2,
                segment_max_vectors: 16,
                ram_budget_bytes: None,
                text: false,
                named_vectors: BTreeMap::from([(
                    "named".to_string(),
                    VectorSpec {
                        dimensions: 2,
                        metric: VectorMetric::Euclidean,
                        kind: Default::default(),
                        element_type: Default::default(),
                    },
                )]),
            },
        )
        .unwrap(),
    );
    let (traced, operations) =
        common::FaultInjectingObjectStore::new(Arc::clone(&inner)).with_operation_log();
    let traced: Arc<dyn ObjectStore> = Arc::new(traced);

    drop(BorsukIndex::open_with_object_store(traced, uri).unwrap());

    assert_eq!(
        operations.count_matching(|operation, path| {
            operation == common::StoreOperation::Get
                && path.starts_with("collection/wal-frontier/")
                && path.ends_with("/HEAD")
        }),
        0,
        "pending-only collection open must not read obsolete mutable frontier heads"
    );
}

#[test]
fn corrupt_active_pending_commit_is_hard_corruption() {
    let directory = tempfile::tempdir().unwrap();
    let uri = directory.path().to_string_lossy().into_owned();
    let mut index = BorsukIndex::create(IndexConfig {
        uri: uri.clone(),
        metric: VectorMetric::Euclidean,
        dimensions: 2,
        segment_max_vectors: 16,
        ram_budget_bytes: None,
        text: false,
        named_vectors: BTreeMap::new(),
    })
    .unwrap();
    index
        .add(vec![VectorRecord::new("committed", vec![0.0, 0.0])])
        .unwrap();
    drop(index);
    let epoch = std::fs::read_dir(directory.path().join("collection/write-epochs"))
        .unwrap()
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .find(|path| path.is_dir())
        .unwrap();
    let pending = std::fs::read_dir(epoch.join("pending"))
        .unwrap()
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .find(|path| {
            path.extension()
                .is_some_and(|extension| extension == "commit")
        })
        .unwrap();
    std::fs::write(pending, b"corrupt-pending-commit").unwrap();

    let error = BorsukIndex::open(&uri).unwrap_err();

    assert!(
        error.to_string().contains("collection control object")
            || error.to_string().contains("checksum"),
        "{error}"
    );
}

#[test]
fn collection_transaction_is_fully_visible_when_post_commit_flush_fails() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let uri = "memory:///collection-post-commit-failure";
    drop(
        BorsukIndex::create_with_object_store_and_wal(
            Arc::clone(&inner),
            IndexConfig {
                uri: uri.to_string(),
                metric: VectorMetric::Euclidean,
                dimensions: 2,
                segment_max_vectors: 16,
                ram_budget_bytes: None,
                text: false,
                named_vectors: BTreeMap::from([(
                    "named".to_string(),
                    VectorSpec {
                        dimensions: 2,
                        metric: VectorMetric::Euclidean,
                        kind: Default::default(),
                        element_type: Default::default(),
                    },
                )]),
            },
            WalConfig {
                enabled: true,
                flush_threshold_runs: usize::MAX,
                flush_threshold_records: 1,
                flush_threshold_bytes: u64::MAX,
                collection_flush_threshold_bytes: u64::MAX,
            },
        )
        .unwrap(),
    );

    let faulting: Arc<dyn ObjectStore> =
        Arc::new(common::FaultInjectingObjectStore::fail_nth_matching(
            Arc::clone(&inner),
            1,
            true,
            |operation, path| {
                operation == common::StoreOperation::Put && path.as_ref().starts_with("segments/")
            },
        ));
    let mut writer = BorsukIndex::open_with_object_store(faulting, uri).unwrap();
    writer
        .add(vec![
            VectorRecord::new("committed", vec![0.0, 0.0])
                .with_named_vector("named", vec![0.0, 0.0]),
        ])
        .unwrap();
    drop(writer);

    let reopened = BorsukIndex::open_with_object_store(Arc::clone(&inner), uri).unwrap();
    assert_eq!(
        reopened
            .search_ids(&[0.0, 0.0], SearchOptions::exact(1))
            .unwrap(),
        ["committed"]
    );
    assert_eq!(
        reopened
            .search_ids(
                &[0.0, 0.0],
                SearchOptions::exact(1).with_vector_name("named"),
            )
            .unwrap(),
        ["committed"]
    );
}

#[test]
fn transient_get_fault_during_search_returns_retryable_error() {
    let inner = seeded_index("memory:///transient-get");
    let faulting_store: Arc<dyn ObjectStore> =
        Arc::new(common::FaultInjectingObjectStore::fail_nth_matching(
            Arc::clone(&inner),
            2,
            true,
            |operation, path| operation == common::StoreOperation::Get && is_segment_path(path),
        ));
    let index =
        BorsukIndex::open_with_object_store(faulting_store, "memory:///transient-get").unwrap();

    let error = index
        .search_with_report(&[0.0, 0.0], SearchOptions::exact(3))
        .unwrap_err();

    assert_eq!(error.code(), "object_store_retryable", "{error:?}");
}

#[test]
fn missing_segment_during_search_returns_storage_not_found() {
    let inner = seeded_index("memory:///missing-segment");
    let faulting_store: Arc<dyn ObjectStore> = Arc::new(
        common::FaultInjectingObjectStore::fail_nth_matching_with_error(
            Arc::clone(&inner),
            1,
            false,
            common::InjectedErrorKind::NotFound,
            |operation, path| operation == common::StoreOperation::Head && is_segment_path(path),
        ),
    );
    let index =
        BorsukIndex::open_with_object_store(faulting_store, "memory:///missing-segment").unwrap();

    let error = index
        .search_with_report(&[0.0, 0.0], SearchOptions::exact(3))
        .unwrap_err();

    assert_eq!(error.code(), "object_store_not_found", "{error:?}");
}

#[test]
fn permission_denied_during_search_returns_storage_permission_denied() {
    let inner = seeded_index("memory:///permission-denied");
    let faulting_store: Arc<dyn ObjectStore> = Arc::new(
        common::FaultInjectingObjectStore::fail_nth_matching_with_error(
            Arc::clone(&inner),
            1,
            false,
            common::InjectedErrorKind::PermissionDenied,
            |operation, path| operation == common::StoreOperation::Head && is_segment_path(path),
        ),
    );
    let index =
        BorsukIndex::open_with_object_store(faulting_store, "memory:///permission-denied").unwrap();

    let error = index
        .search_with_report(&[0.0, 0.0], SearchOptions::exact(3))
        .unwrap_err();

    assert_eq!(error.code(), "object_store_permission_denied", "{error:?}");
}

#[test]
fn large_segment_payloads_use_multipart_upload() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let faulting_store: Arc<dyn ObjectStore> =
        Arc::new(common::FaultInjectingObjectStore::fail_nth_matching(
            Arc::clone(&inner),
            1,
            false,
            |operation, path| {
                operation == common::StoreOperation::MultipartPut && is_segment_path(path)
            },
        ));
    let mut index = BorsukIndex::create_with_object_store_and_wal(
        faulting_store,
        IndexConfig {
            uri: "memory:///multipart".to_string(),
            metric: VectorMetric::Euclidean,
            dimensions: 1,
            segment_max_vectors: 1,
            ram_budget_bytes: None,
            text: false,
            named_vectors: Default::default(),
        },
        WalConfig {
            enabled: true,
            flush_threshold_runs: usize::MAX,
            flush_threshold_records: usize::MAX,
            flush_threshold_bytes: u64::MAX,
            collection_flush_threshold_bytes: u64::MAX,
        },
    )
    .unwrap();
    let large_id = deterministic_bytes(LARGE_OBJECT_BYTES);

    // The default WAL keeps `add` append-only (a `wal/` object, not a `segments/`
    // path), so the large-segment multipart upload — and its injected fault — is
    // triggered by the flush that materializes the tail into a segment.
    index
        .add(vec![VectorRecord::new_bytes(large_id, vec![0.0])])
        .unwrap();
    let error = index.flush().unwrap_err();

    assert_eq!(error.code(), "object_store_retryable", "{error:?}");
}

fn seeded_index(uri: &str) -> Arc<dyn ObjectStore> {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let mut index = BorsukIndex::create_with_object_store(
        Arc::clone(&inner),
        IndexConfig {
            uri: uri.to_string(),
            metric: VectorMetric::Euclidean,
            dimensions: 2,
            segment_max_vectors: 1,
            ram_budget_bytes: None,
            text: false,
            named_vectors: Default::default(),
        },
    )
    .unwrap();
    index
        .add(vec![
            VectorRecord::new("near", vec![0.0, 0.0]),
            VectorRecord::new("mid", vec![5.0, 0.0]),
            VectorRecord::new("far", vec![10.0, 0.0]),
        ])
        .unwrap();
    // Flush the (default-on) WAL so the seeded records live in real segments;
    // these fault-injection tests target faults on `segments/` object reads,
    // which only happen once search reads segments rather than the WAL tail.
    index.flush().unwrap();
    inner
}

fn is_segment_path(path: &ObjectPath) -> bool {
    path.as_ref().starts_with("segments/")
}

fn deterministic_bytes(len: usize) -> Vec<u8> {
    let mut state = 0x4d59_5df4_d0f3_3173_u64;
    (0..len)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state as u8
        })
        .collect()
}
