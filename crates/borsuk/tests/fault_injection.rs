#![allow(missing_docs)]

#[allow(dead_code)]
mod common;

use std::{collections::BTreeMap, sync::Arc};

use borsuk::{
    BorsukIndex, IndexConfig, SearchOptions, VectorMetric, VectorRecord, VectorSpec, WalConfig,
};
use object_store::{ObjectStore, memory::InMemory, path::Path as ObjectPath};

const LARGE_OBJECT_BYTES: usize = 64 * 1024 * 1024 + 1;

#[test]
fn collection_transaction_is_invisible_when_root_commit_fails() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let uri = "memory:///collection-root-commit-failure";
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
            true,
            |operation, path| {
                operation == common::StoreOperation::Put
                    && path.as_ref().starts_with("collection/transactions/")
                    && path.as_ref().ends_with("/COMMIT")
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
        .unwrap_err();
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
