#![allow(missing_docs)]

mod common;

use std::{collections::BTreeMap, sync::Arc, time::Duration};

use borsuk::{
    BorsukIndex, GarbageCollectionOptions, IndexConfig, PositionedLogWriter,
    PositionedMutationModality, SearchOptions, VectorMetric, VectorRecord, VectorSpec, WalConfig,
};
use object_store::{ObjectStore, memory::InMemory, path::Path as ObjectPath};

const LARGE_ID_BYTES: usize = 9 * 1024 * 1024;

fn assert_no_legacy_mutation_authority(operations: &common::OperationLog) {
    assert_eq!(
        operations.count_matching(|operation, path| {
            matches!(
                operation,
                common::StoreOperation::Put | common::StoreOperation::MultipartPut
            ) && (path.starts_with("collection/write-epochs/")
                || path.starts_with("collection/wal")
                || path.starts_with("lane-log/")
                || path.starts_with("cell-wal/")
                || (path.starts_with("transactions/") && !path.ends_with("/STATE"))
                || (path.starts_with("cells/") && path.contains("/wal/"))
                || path.starts_with("tombstones/")
                || path.starts_with("bm25/")
                || path.starts_with("lexical/stats-delta/")
                || path == "id-directory/generated/NEXT")
        }),
        0,
        "a positioned mutation must not publish a second legacy authority: {:?}",
        operations.entries()
    );
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

    let faulting = common::FaultInjectingObjectStore::fail_nth_matching_with_error(
        Arc::clone(&inner),
        1,
        false,
        common::InjectedErrorKind::PermissionDenied,
        |operation, path| {
            operation == common::StoreOperation::Put
                && path.as_ref().starts_with("positioned-log/heads/")
        },
    );
    let (faulting, operations) = faulting.with_operation_log();
    let mut writer = BorsukIndex::open_with_object_store(Arc::new(faulting), uri).unwrap();
    operations.clear();
    let error = writer
        .add(vec![
            VectorRecord::new("uncommitted", vec![0.0, 0.0])
                .with_named_vector("named", vec![0.0, 0.0]),
        ])
        .unwrap_err();
    assert_eq!(error.code(), "object_store_permission_denied", "{error:?}");
    assert_eq!(
        operations.count_matching(|operation, path| {
            operation == common::StoreOperation::Put && path.starts_with("positioned-log/heads/")
        }),
        1,
        "the injected positioned head publication fault must be exercised exactly once"
    );
    assert_no_legacy_mutation_authority(&operations);
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
    assert_eq!(reopened.get_vector("uncommitted").unwrap(), None);
    let snapshot = PositionedLogWriter::open(uri, inner, 1)
        .unwrap()
        .reader()
        .snapshot()
        .unwrap();
    assert_eq!(snapshot.transactions.len(), 1);
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

    let faulting = common::FaultInjectingObjectStore::accept_then_fail_nth_put(
        Arc::clone(&inner),
        1,
        |operation, path| {
            operation == common::StoreOperation::Put
                && path.as_ref().starts_with("positioned-log/heads/")
        },
    );
    let (faulting, operations) = faulting.with_operation_log();
    let mut writer = BorsukIndex::open_with_object_store(Arc::new(faulting), uri).unwrap();
    operations.clear();
    let (ids, report) = writer
        .add_with_report(vec![vec![0.0, 0.0]], Some(vec!["committed".to_string()]))
        .unwrap();
    assert_eq!(ids, ["committed"]);
    let position = report
        .positioned_position
        .expect("reconciled append must return its authoritative position");
    assert_eq!(position.source_epoch, 1);
    assert_eq!(position.sequence, 1);
    assert_eq!(report.positioned_envelope_checksum.len(), 64);
    assert!(report.positioned_encoded_bytes > 0);
    assert_eq!(
        operations.count_matching(|operation, path| {
            operation == common::StoreOperation::Put && path.starts_with("positioned-log/heads/")
        }),
        1,
        "an accepted head PUT must reconcile without a second publication"
    );
    assert_eq!(
        operations.count_matching(|operation, path| {
            operation == common::StoreOperation::Get && path.starts_with("positioned-log/heads/")
        }),
        1,
        "the accepted-then-error response must reconcile from the authoritative head"
    );
    assert_no_legacy_mutation_authority(&operations);
    drop(writer);

    let reopened = BorsukIndex::open_with_object_store(Arc::clone(&inner), uri).unwrap();
    assert_eq!(
        reopened
            .search_ids(&[0.0, 0.0], SearchOptions::exact(1))
            .unwrap(),
        ["committed"]
    );
    assert_eq!(reopened.list_records(0, 10).unwrap().len(), 1);
    let snapshot = PositionedLogWriter::open(uri, inner, 1)
        .unwrap()
        .reader()
        .snapshot()
        .unwrap();
    assert_eq!(snapshot.transactions.len(), 1);
    assert_eq!(snapshot.transactions[0].position, position);
    assert_eq!(
        snapshot.envelope_checksums,
        [report.positioned_envelope_checksum]
    );
    let primary = snapshot.transactions[0]
        .payloads
        .iter()
        .filter(|payload| payload.modality == PositionedMutationModality::PrimaryDense)
        .collect::<Vec<_>>();
    assert_eq!(primary.len(), 1);
    assert_eq!(primary[0].rows, 1);
}

#[test]
fn vector_report_api_does_not_ack_when_positioned_envelope_upload_fails() {
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

    let faulting = common::FaultInjectingObjectStore::fail_nth_matching_with_error(
        Arc::clone(&inner),
        1,
        false,
        common::InjectedErrorKind::PermissionDenied,
        |operation, path| {
            operation == common::StoreOperation::Put
                && path.as_ref().starts_with("positioned-log/envelopes/")
        },
    );
    let (faulting, operations) = faulting.with_operation_log();
    let mut writer = BorsukIndex::open_with_object_store(Arc::new(faulting), uri).unwrap();
    operations.clear();
    let error = writer
        .add_with_report(vec![vec![0.0, 0.0]], Some(vec!["uncommitted".to_string()]))
        .unwrap_err();
    assert_eq!(error.code(), "object_store_permission_denied", "{error:?}");
    assert_eq!(
        operations.count_matching(|operation, path| {
            operation == common::StoreOperation::Put
                && path.starts_with("positioned-log/envelopes/")
        }),
        1,
        "the injected immutable envelope PUT fault must be exercised"
    );
    assert_eq!(
        operations.count_matching(|operation, path| {
            operation == common::StoreOperation::Put && path.starts_with("positioned-log/heads/")
        }),
        0,
        "an envelope failure must happen before positioned head authority"
    );
    assert_no_legacy_mutation_authority(&operations);
    drop(writer);

    let reopened = BorsukIndex::open_with_object_store(Arc::clone(&inner), uri).unwrap();
    assert_eq!(reopened.get_vector("uncommitted").unwrap(), None);
    assert!(reopened.list_records(0, 10).unwrap().is_empty());
    let snapshot = PositionedLogWriter::open(uri, inner, 1)
        .unwrap()
        .reader()
        .snapshot()
        .unwrap();
    assert!(snapshot.transactions.is_empty());
}

#[test]
fn collection_transaction_is_invisible_when_positioned_head_publication_fails() {
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

    let faulting = common::FaultInjectingObjectStore::fail_nth_matching_with_error(
        Arc::clone(&inner),
        1,
        false,
        common::InjectedErrorKind::PermissionDenied,
        |operation, path| {
            operation == common::StoreOperation::Put
                && path.as_ref().starts_with("positioned-log/heads/")
        },
    );
    let (faulting, operations) = faulting.with_operation_log();
    let mut writer = BorsukIndex::open_with_object_store(Arc::new(faulting), uri).unwrap();
    operations.clear();
    let error = writer
        .add(vec![VectorRecord::new("uncommitted", vec![0.0, 0.0])])
        .unwrap_err();
    assert_eq!(error.code(), "object_store_permission_denied", "{error:?}");
    assert_eq!(
        operations.count_matching(|operation, path| {
            operation == common::StoreOperation::Put && path.starts_with("positioned-log/heads/")
        }),
        1,
        "the injected positioned head publication fault must be exercised exactly once"
    );
    assert_no_legacy_mutation_authority(&operations);
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
    assert_eq!(reopened.get_vector("uncommitted").unwrap(), None);
}

#[test]
fn multimodal_payload_wave_failure_never_resurrects_after_reopen_flush_and_gc() {
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

    let faulting = common::FaultInjectingObjectStore::fail_nth_matching(
        Arc::clone(&inner),
        1,
        true,
        |operation, path| {
            operation == common::StoreOperation::Put
                && path.as_ref().starts_with("positioned-log/payloads/")
        },
    );
    let (faulting, operations) = faulting.with_operation_log();
    let mut writer = BorsukIndex::open_with_object_store(Arc::new(faulting), uri).unwrap();
    operations.clear();
    let error = writer
        .add(vec![
            VectorRecord::new("uncommitted", vec![0.0, 0.0])
                .with_named_vector("named", vec![0.0, 0.0]),
        ])
        .unwrap_err();
    assert_eq!(error.code(), "object_store_retryable", "{error:?}");
    assert!(
        operations.count_matching(|operation, path| {
            operation == common::StoreOperation::Put && path.starts_with("positioned-log/payloads/")
        }) >= 1,
        "the injected immutable payload-wave fault must match a payload PUT"
    );
    assert_eq!(
        operations.count_matching(|operation, path| {
            operation == common::StoreOperation::Put && path.starts_with("positioned-log/heads/")
        }),
        0,
        "payload-wave failure must precede the positioned head CAS"
    );
    assert_no_legacy_mutation_authority(&operations);
    drop(writer);

    let mut reopened = BorsukIndex::open_with_object_store(Arc::clone(&inner), uri).unwrap();
    assert!(
        reopened
            .search_ids(&[0.0, 0.0], SearchOptions::exact(2))
            .unwrap()
            .is_empty()
    );
    assert!(
        reopened
            .search_ids(
                &[0.0, 0.0],
                SearchOptions::exact(2).with_vector_name("named"),
            )
            .unwrap()
            .is_empty()
    );
    // Parallel immutable uploads may leave content-addressed orphans. They are
    // non-authoritative and need not be synchronously removed by this failure.
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
    drop(reopened);

    let reopened = BorsukIndex::open_with_object_store(Arc::clone(&inner), uri).unwrap();
    assert_eq!(reopened.get_vector("uncommitted").unwrap(), None);
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
fn positioned_retirement_fences_survive_later_flush_and_gc_cycles() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let uri = "memory:///positioned-retirement-repeated-cycles";
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

    let faulting = common::FaultInjectingObjectStore::fail_nth_matching_with_error(
        Arc::clone(&inner),
        1,
        false,
        common::InjectedErrorKind::PermissionDenied,
        |operation, path| {
            operation == common::StoreOperation::Put
                && path.as_ref().starts_with("positioned-log/heads/")
        },
    );
    let (faulting, failed_operations) = faulting.with_operation_log();
    let mut failed_writer = BorsukIndex::open_with_object_store(Arc::new(faulting), uri).unwrap();
    let error = failed_writer
        .add(vec![
            VectorRecord::new("uncommitted", vec![9.0, 0.0])
                .with_named_vector("named", vec![9.0, 0.0]),
        ])
        .unwrap_err();
    assert_eq!(error.code(), "object_store_permission_denied", "{error:?}");
    assert_eq!(
        failed_operations.count_matching(|operation, path| {
            operation == common::StoreOperation::Put && path.starts_with("positioned-log/heads/")
        }),
        1,
        "the failed transaction must reach the positioned head publication path"
    );
    assert_no_legacy_mutation_authority(&failed_operations);
    drop(failed_writer);

    let (traced, operations) =
        common::FaultInjectingObjectStore::new(Arc::clone(&inner)).with_operation_log();
    let traced: Arc<dyn ObjectStore> = Arc::new(traced);
    let mut writer = BorsukIndex::open_with_object_store(Arc::clone(&traced), uri).unwrap();

    writer
        .add(vec![
            VectorRecord::new("a", vec![0.0, 0.0]).with_named_vector("named", vec![0.0, 0.0]),
        ])
        .unwrap();
    writer.flush().unwrap();
    assert_eq!(
        writer
            .search_ids(&[0.0, 0.0], SearchOptions::exact(8))
            .unwrap(),
        ["a"]
    );
    assert_eq!(
        writer
            .search_ids(
                &[0.0, 0.0],
                SearchOptions::exact(8).with_vector_name("named"),
            )
            .unwrap(),
        ["a"]
    );

    writer
        .add(vec![
            VectorRecord::new("b", vec![1.0, 0.0]).with_named_vector("named", vec![1.0, 0.0]),
        ])
        .unwrap();
    writer.flush().unwrap();
    writer
        .gc_obsolete_segments(GarbageCollectionOptions {
            dry_run: false,
            min_age: Duration::ZERO,
        })
        .unwrap();
    writer.refresh().unwrap();

    let assert_exact_state = |index: &BorsukIndex| {
        assert_eq!(
            index
                .search_ids(&[0.0, 0.0], SearchOptions::exact(8))
                .unwrap(),
            ["a", "b"]
        );
        assert_eq!(
            index
                .search_ids(
                    &[0.0, 0.0],
                    SearchOptions::exact(8).with_vector_name("named"),
                )
                .unwrap(),
            ["a", "b"]
        );
        assert_eq!(index.get_vector("uncommitted").unwrap(), None);
    };
    assert_exact_state(&writer);
    drop(writer);

    let mut reopened = BorsukIndex::open_with_object_store(Arc::clone(&traced), uri).unwrap();
    assert_exact_state(&reopened);
    reopened.refresh().unwrap();
    reopened
        .gc_obsolete_segments(GarbageCollectionOptions {
            dry_run: false,
            min_age: Duration::ZERO,
        })
        .unwrap();
    drop(reopened);

    let reopened = BorsukIndex::open_with_object_store(Arc::clone(&traced), uri).unwrap();
    assert_exact_state(&reopened);
    assert_eq!(
        operations.count_matching(|operation, path| {
            operation == common::StoreOperation::Put && path.starts_with("positioned-log/heads/")
        }),
        4,
        "transactions A and B must each publish one head receipt and one materialization checkpoint"
    );
    assert_eq!(
        operations.count_matching(|operation, path| {
            operation == common::StoreOperation::Put
                && path.starts_with("positioned-log/envelopes/")
        }),
        2,
        "transactions A and B must each publish one immutable positioned envelope"
    );
    assert!(
        operations.count_matching(|operation, path| {
            operation == common::StoreOperation::Put && path.starts_with("positioned-log/payloads/")
        }) >= 4,
        "transactions A and B must publish real primary and named positioned payloads"
    );
    assert_no_legacy_mutation_authority(&operations);
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
fn corrupt_authoritative_positioned_head_is_hard_corruption() {
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
    let (_, report) = index
        .add_with_report(vec![vec![0.0, 0.0]], Some(vec!["committed".to_string()]))
        .unwrap();
    let position = report
        .positioned_position
        .expect("committed record must report its authoritative shard");
    drop(index);
    let authoritative_head = directory
        .path()
        .join("positioned-log/heads")
        .join(format!("{:02}.json", position.shard));
    assert!(
        authoritative_head.is_file(),
        "reported shard head must select the current authority deterministically"
    );
    std::fs::write(&authoritative_head, b"corrupt-positioned-head").unwrap();

    let error = BorsukIndex::open(&uri).unwrap_err();

    assert_eq!(error.code(), "invalid_storage", "{error:?}");
    assert!(
        error.to_string().contains("positioned shard head"),
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
            |operation, path| operation == common::StoreOperation::Get && is_segment_path(path),
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
            |operation, path| operation == common::StoreOperation::Get && is_segment_path(path),
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
    let injected: Arc<dyn ObjectStore> =
        Arc::new(common::FaultInjectingObjectStore::fail_nth_matching(
            Arc::clone(&inner),
            1,
            false,
            |operation, path| {
                operation == common::StoreOperation::MultipartPut && is_segment_path(path)
            },
        ));
    // Trace outside the injector so the rejected multipart attempt itself is
    // recorded even though the inner fault fires before forwarding the upload.
    let (faulting_store, operations) =
        common::FaultInjectingObjectStore::new(injected).with_operation_log();
    let mut index = BorsukIndex::create_with_object_store_and_wal(
        Arc::new(faulting_store),
        IndexConfig {
            uri: "memory:///multipart".to_string(),
            metric: VectorMetric::Euclidean,
            dimensions: 1,
            segment_max_vectors: 16,
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

    // Each ID appears in the primary, ID-directory, and route-plan payloads, so
    // 9 MiB keeps one append near 27 MiB with comfortable room below 64 MiB.
    // Flush coalesces eight incompressible IDs into one >72 MiB segment. The
    // smaller transactions also allow two pending appends per positioned shard,
    // making the bounded shard-selection retry deterministic under parallel tests.
    for ordinal in 0..8 {
        let seed = 0x4d59_5df4_d0f3_3173_u64
            .wrapping_add((ordinal as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15));
        let id = deterministic_bytes(LARGE_ID_BYTES, seed);
        let mut added = false;
        for attempt in 0..128 {
            match index.add(vec![VectorRecord::new_bytes(
                id.clone(),
                vec![ordinal as f32],
            )]) {
                Ok(()) => {
                    added = true;
                    break;
                }
                Err(error) if error.code() == "ingest_backpressure" && attempt < 127 => {
                    // This fixture deliberately disables materialization so four
                    // large transactions can form one multipart segment. A random
                    // transaction ID may collide with an already-near-full source
                    // shard; retrying the uncommitted request selects a fresh shard.
                }
                Err(error) => {
                    panic!("bounded positioned append {ordinal} failed: {error}")
                }
            }
        }
        assert!(
            added,
            "bounded positioned append {ordinal} exhausted retries"
        );
    }
    operations.clear();
    let error = index.flush().unwrap_err();

    assert_eq!(error.code(), "object_store_retryable", "{error:?}");
    assert_eq!(
        operations.count_matching(|operation, path| {
            operation == common::StoreOperation::MultipartPut && path.starts_with("segments/")
        }),
        1,
        "the injected multipart segment PUT must be exercised exactly once"
    );
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

fn deterministic_bytes(len: usize, mut state: u64) -> Vec<u8> {
    (0..len)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state as u8
        })
        .collect()
}
