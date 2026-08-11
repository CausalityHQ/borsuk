#![allow(missing_docs)]

mod common;

use std::sync::Arc;

use borsuk::{
    BorsukIndex, CellWalConfig, IndexConfig, ObjectStore, PositionedLogWriter, VectorMetric,
    VectorRecord,
};
use object_store::memory::InMemory;

fn config(uri: &str) -> IndexConfig {
    IndexConfig {
        uri: uri.to_string(),
        metric: VectorMetric::Euclidean,
        dimensions: 2,
        segment_max_vectors: 1_000,
        ram_budget_bytes: None,
        text: false,
        named_vectors: Default::default(),
    }
}

#[test]
fn production_claim_configuration_remains_bounded() {
    assert_eq!(CellWalConfig::default().lane_count, 8);
    for lane_count in [1, 8, 64] {
        assert!(CellWalConfig { lane_count }.validate().is_ok());
    }
    for lane_count in [0, 65, u8::MAX] {
        assert!(CellWalConfig { lane_count }.validate().is_err());
    }
}

#[test]
fn public_exact_add_uses_claims_but_only_positioned_commit_durability() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (traced, operations) =
        common::FaultInjectingObjectStore::new(Arc::clone(&inner)).with_operation_log();
    let uri = "memory:///cell-claims-positioned-durability";
    let mut index = BorsukIndex::create_with_object_store(Arc::new(traced), config(uri)).unwrap();
    operations.clear();

    index
        .add(vec![VectorRecord::new("claimed", vec![1.0, 0.0])])
        .unwrap();

    assert_eq!(
        operations.count_matching(|operation, path| {
            operation == common::StoreOperation::Put && path.starts_with("positioned-log/heads/")
        }),
        1
    );
    assert_eq!(
        operations.count_matching(|_, path| {
            (path.starts_with("transactions/")
                && (path.ends_with("/COMMIT") || path.contains("/descriptors/")))
                || path.starts_with("cell-wal/")
                || (path.starts_with("cells/") && path.contains("/wal/"))
        }),
        0
    );
    let snapshot = PositionedLogWriter::open(uri, Arc::clone(&inner), 1)
        .unwrap()
        .reader()
        .snapshot()
        .unwrap();
    assert_eq!(snapshot.transactions.len(), 1);
    assert_eq!(
        BorsukIndex::open_with_object_store(inner, uri)
            .unwrap()
            .get_vector("claimed")
            .unwrap(),
        Some(vec![1.0, 0.0])
    );
}
