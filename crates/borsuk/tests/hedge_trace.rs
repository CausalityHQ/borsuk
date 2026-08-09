//! Reconciliation coverage for completed and cancelled hedge responses.

#[allow(dead_code)]
mod common;

use std::{sync::Arc, time::Duration};

use borsuk::{
    BorsukIndex, IndexConfig, LeafMode, OpenOptions, SearchOptions, StorageAccessTrace,
    VectorMetric, VectorRecord, install_storage_access_trace,
};
use object_store::{ObjectStore, memory::InMemory};

#[test]
fn completed_hedge_responses_reconcile_trace_and_query_backing_bytes() {
    let uri = "memory:///hedge-trace-reconciliation";
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let mut writer = BorsukIndex::create_with_object_store(
        Arc::clone(&inner),
        IndexConfig {
            uri: uri.to_string(),
            metric: VectorMetric::Euclidean,
            dimensions: 8,
            segment_max_vectors: 256,
            ram_budget_bytes: None,
            text: false,
            named_vectors: Default::default(),
        },
    )
    .unwrap();
    writer
        .add(
            (0..128)
                .map(|row| VectorRecord::new(format!("row-{row}"), vec![row as f32; 8]))
                .collect(),
        )
        .unwrap();
    writer.finish_bulk_load().unwrap();
    drop(writer);

    let directory = tempfile::tempdir().unwrap();
    let trace_path = directory.path().join("storage-access.csv");
    let trace = StorageAccessTrace::create(&trace_path).unwrap();
    install_storage_access_trace(trace.clone()).unwrap();
    let slow_payload: Arc<dyn ObjectStore> = Arc::new(
        common::FaultInjectingObjectStore::new(inner).with_get_payload_latency_for(
            Duration::from_millis(100),
            |operation, path| {
                operation == common::StoreOperation::Get
                    && path.as_ref().starts_with("global-pq/exact-bundles/")
            },
        ),
    );
    let reader = BorsukIndex::open_with_object_store_and_options(
        slow_payload,
        uri,
        OpenOptions {
            global_pq_slow_read_hedge_after: Some(Duration::from_millis(5)),
            ..OpenOptions::default()
        },
    )
    .unwrap();
    trace.reset().unwrap();

    let report = reader
        .search_with_report(
            &[0.0; 8],
            SearchOptions::approx(1, LeafMode::SrhtPqScan)
                .with_max_segments(1)
                .with_max_candidates_per_segment(1),
        )
        .unwrap();

    let (trace_requests, trace_bytes) = std::fs::read_to_string(trace_path)
        .unwrap()
        .lines()
        .skip(1)
        .filter_map(|line| {
            let columns = line.split(',').collect::<Vec<_>>();
            (columns[0] == "read").then(|| {
                (
                    columns[5].parse::<u64>().unwrap(),
                    columns[6].parse::<u64>().unwrap(),
                )
            })
        })
        .fold((0, 0), |(requests, bytes), (row_requests, row_bytes)| {
            (requests + row_requests, bytes + row_bytes)
        });
    assert!(report.requests.gets > 2, "the exact range must hedge");
    assert_eq!(trace_requests, report.requests.gets);
    assert_eq!(trace_bytes, report.backing_bytes_read);
}
