#![allow(missing_docs)]

#[allow(dead_code)]
mod common;

use std::sync::Arc;

use borsuk::{BorsukIndex, IndexConfig, SearchOptions, VectorMetric, VectorRecord};
use object_store::{ObjectStore, memory::InMemory, path::Path as ObjectPath};

const LARGE_OBJECT_BYTES: usize = 64 * 1024 * 1024 + 1;

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
    let mut index = BorsukIndex::create_with_object_store(
        faulting_store,
        IndexConfig {
            uri: "memory:///multipart".to_string(),
            metric: VectorMetric::Euclidean,
            dimensions: 1,
            segment_max_vectors: 1,
            ram_budget_bytes: None,
        },
    )
    .unwrap();
    let large_id = deterministic_bytes(LARGE_OBJECT_BYTES);

    let error = index
        .add(vec![VectorRecord::new_bytes(large_id, vec![0.0])])
        .unwrap_err();

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
