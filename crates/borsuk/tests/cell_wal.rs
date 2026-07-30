#![allow(missing_docs)]

#[allow(dead_code)]
mod common;

use std::sync::{Arc, Barrier};

use borsuk::{
    BorsukIndex, CellWalConfig, CellWalObjectPaths, CellWalRunInput, CellWalRunKind, CellWalStore,
    IndexConfig, LogicalCellId, ObjectStore, VectorMetric, VectorRecord, cell_wal_transaction_id,
};
use futures_util::TryStreamExt;
use object_store::{ObjectStoreExt, memory::InMemory};

#[test]
fn production_cell_wal_defaults_to_eight_lanes_and_validates_bounds() {
    assert_eq!(CellWalConfig::default().lane_count, 8);
    for lane_count in [1, 8, 64] {
        assert!(CellWalConfig { lane_count }.validate().is_ok());
    }
    for lane_count in [0, 65, u8::MAX] {
        assert!(CellWalConfig { lane_count }.validate().is_err());
    }
}

#[test]
fn stable_writer_ids_choose_one_lane_without_exceeding_the_configured_count() {
    let config = CellWalConfig { lane_count: 8 };
    let first = config.lane_for_writer(b"writer-a").unwrap();
    assert_eq!(first, config.lane_for_writer(b"writer-a").unwrap());
    assert!(first < 8);

    let lanes = (0..32)
        .map(|writer| {
            config
                .lane_for_writer(format!("writer-{writer}").as_bytes())
                .unwrap()
        })
        .collect::<std::collections::BTreeSet<_>>();
    assert!(
        lanes.len() > 1,
        "32 stable writers must spread across lanes"
    );
    let all_lanes = (0..10_000)
        .map(|writer| {
            config
                .lane_for_writer(format!("coverage-writer-{writer}").as_bytes())
                .unwrap()
        })
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(all_lanes, (0..8).collect());
}

#[test]
fn cell_wal_paths_are_stable_and_do_not_use_physical_segment_ids() {
    let cell = LogicalCellId::new(3, 42);
    let paths = CellWalObjectPaths::new(cell, 7).unwrap();
    let transaction_id = "transaction-a";

    assert_eq!(paths.head(), "cells/3/42/wal/7/HEAD");
    assert_eq!(
        paths.run(
            transaction_id,
            CellWalRunKind::Records,
            &"ab".repeat(32),
            "parquet",
        ),
        format!(
            "cells/3/42/wal/7/runs/records/transactions/{transaction_id}/{}.parquet",
            "ab".repeat(32)
        )
    );
    assert_eq!(
        paths.frontier_node(transaction_id, &"cd".repeat(32)),
        format!(
            "cells/3/42/wal/7/frontier/transactions/{transaction_id}/{}.bin",
            "cd".repeat(32)
        )
    );
}

#[test]
fn identical_wal_payloads_use_transaction_scoped_paths() {
    let paths = CellWalObjectPaths::new(LogicalCellId::new(3, 42), 7).unwrap();
    let checksum = "ab".repeat(32);

    assert_ne!(
        paths.run(
            "transaction-a",
            CellWalRunKind::Records,
            &checksum,
            "parquet"
        ),
        paths.run(
            "transaction-b",
            CellWalRunKind::Records,
            &checksum,
            "parquet"
        ),
        "a new reservation must never reuse an old orphan's object timestamp"
    );
    assert_ne!(
        paths.frontier_node("transaction-a", &checksum),
        paths.frontier_node("transaction-b", &checksum),
        "frontier objects must be protected by their owning root reservation"
    );
}

fn store() -> Arc<dyn ObjectStore> {
    Arc::new(InMemory::new())
}

fn input(cell: LogicalCellId, value: impl AsRef<[u8]>) -> CellWalRunInput {
    CellWalRunInput {
        cell,
        kind: CellWalRunKind::Records,
        metadata: Vec::new(),
        bytes: value.as_ref().to_vec(),
        record_count: 1,
        extension: "parquet".to_string(),
    }
}

#[test]
fn run_inputs_reject_path_injection_and_role_codec_mismatches() {
    let wal = CellWalStore::new(
        store(),
        "memory:///cell-wal-input-validation",
        CellWalConfig::default(),
        b"writer".to_vec(),
    )
    .unwrap();
    for (kind, extension) in [
        (CellWalRunKind::Records, "../escape"),
        (CellWalRunKind::Records, "bin"),
        (CellWalRunKind::Tombstones, "vortex"),
        (CellWalRunKind::IdDirectory, "parquet"),
    ] {
        let error = wal
            .prepare_transaction(
                "invalid-input",
                &[CellWalRunInput {
                    cell: LogicalCellId::new(1, 0),
                    kind,
                    metadata: Vec::new(),
                    bytes: vec![1],
                    record_count: 1,
                    extension: extension.to_string(),
                }],
            )
            .unwrap_err();
        assert!(
            error.to_string().contains("extension"),
            "{kind:?} {extension}: {error}"
        );
    }
}

#[test]
fn independent_immutable_run_uploads_overlap() {
    let inner: Arc<dyn ObjectStore> = store();
    let (instrumented, concurrency) = common::FaultInjectingObjectStore::new(inner)
        .with_latency(std::time::Duration::from_millis(25))
        .with_put_concurrency_probe();
    let wal = CellWalStore::new(
        Arc::new(instrumented),
        "memory:///parallel-run-preparation",
        CellWalConfig::default(),
        b"parallel-writer".to_vec(),
    )
    .unwrap();
    let inputs = [
        input(LogicalCellId::new(1, 0), b"first"),
        input(LogicalCellId::new(1, 1), b"second"),
        input(LogicalCellId::new(1, 2), b"third"),
        input(LogicalCellId::new(1, 3), b"fourth"),
    ];

    wal.prepare_transaction("parallel-run-preparation", &inputs)
        .unwrap();

    assert!(
        concurrency.peak() >= 2,
        "independent immutable payload uploads must overlap; observed peak {}",
        concurrency.peak()
    );
}

#[test]
fn partial_multi_cell_prepare_failure_detaches_successful_lane_publications() {
    let inner: Arc<dyn ObjectStore> = store();
    let failed_cell = LogicalCellId::new(5, 2);
    let faulting: Arc<dyn ObjectStore> =
        Arc::new(common::FaultInjectingObjectStore::fail_nth_matching(
            Arc::clone(&inner),
            1,
            false,
            move |operation, path| {
                operation == common::StoreOperation::Put
                    && path.as_ref().starts_with("cells/5/2/wal/")
                    && path.as_ref().ends_with("/HEAD")
            },
        ));
    let wal = CellWalStore::new(
        faulting,
        "memory:///partial-multi-cell-prepare",
        CellWalConfig::default(),
        b"partial-writer".to_vec(),
    )
    .unwrap();

    wal.prepare_transaction(
        "partial-multi-cell-prepare",
        &[
            input(LogicalCellId::new(5, 1), b"published-before-peer-failure"),
            input(failed_cell, b"failed-publication"),
        ],
    )
    .unwrap_err();

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let heads = runtime
        .block_on(
            inner
                .list(Some(&object_store::path::Path::from("cells")))
                .try_collect::<Vec<_>>(),
        )
        .unwrap()
        .into_iter()
        .filter(|meta| meta.location.as_ref().ends_with("/HEAD"))
        .collect::<Vec<_>>();
    for head in heads {
        let bytes = runtime
            .block_on(async { inner.get(&head.location).await?.bytes().await })
            .unwrap();
        assert!(
            !bytes
                .windows(b"/frontier/".len())
                .any(|window| window == b"/frontier/"),
            "failed prepare left a run reachable from {}",
            head.location
        );
    }
}

#[test]
fn transient_lane_head_error_is_resolved_before_prepare_returns() {
    let inner: Arc<dyn ObjectStore> = store();
    let faulting: Arc<dyn ObjectStore> =
        Arc::new(common::FaultInjectingObjectStore::fail_nth_matching(
            Arc::clone(&inner),
            1,
            true,
            |operation, path| {
                operation == common::StoreOperation::Put
                    && path.as_ref().starts_with("cells/8/1/wal/")
                    && path.as_ref().ends_with("/HEAD")
            },
        ));
    let wal = CellWalStore::new(
        faulting,
        "memory:///transient-lane-head-error",
        CellWalConfig::default(),
        b"retrying-writer".to_vec(),
    )
    .unwrap();
    let cell = LogicalCellId::new(8, 1);

    wal.commit("transient-lane-head-error", &[input(cell, b"durable")])
        .unwrap();

    let visible = wal.committed_transactions_snapshot(&[cell]).unwrap();
    assert_eq!(visible.len(), 1);
    assert_eq!(visible[0].transaction_id, "transient-lane-head-error");
}

#[test]
fn transaction_snapshot_pins_typed_runs_and_metadata_atomically() {
    let object_store = store();
    let wal = CellWalStore::new(
        object_store,
        "memory:///typed-transaction",
        CellWalConfig::default(),
        b"typed-writer".to_vec(),
    )
    .unwrap();
    let cells = [LogicalCellId::new(4, 1), LogicalCellId::new(4, 2)];
    let mut records = input(cells[0], b"records");
    records.kind = CellWalRunKind::Records;
    let mut tombstones = input(cells[1], b"tombstones");
    tombstones.kind = CellWalRunKind::Tombstones;

    let prepared = wal
        .prepare_transaction_with_metadata(
            "typed-atomic",
            &[records, tombstones],
            br#"{"new_tombstone_ids":1}"#,
        )
        .unwrap();
    assert!(
        wal.committed_transactions_snapshot(&cells)
            .unwrap()
            .is_empty()
    );
    wal.commit_prepared(&prepared).unwrap();

    let (visible, retries) = wal
        .committed_transactions_snapshot_with_retries(&cells)
        .unwrap();
    assert_eq!(retries, 0);
    assert_eq!(visible.len(), 1);
    assert_eq!(visible[0].metadata, br#"{"new_tombstone_ids":1}"#);
    assert_eq!(
        visible[0]
            .runs
            .iter()
            .map(|run| run.kind)
            .collect::<std::collections::BTreeSet<_>>(),
        std::collections::BTreeSet::from([CellWalRunKind::Records, CellWalRunKind::Tombstones,])
    );
}

#[test]
fn fresh_index_catalog_persists_epoch_one_and_eight_wal_lanes() {
    let directory = tempfile::tempdir().unwrap();
    let uri = directory.path().to_string_lossy().into_owned();
    let index = BorsukIndex::create(IndexConfig {
        uri: uri.clone(),
        metric: VectorMetric::Euclidean,
        dimensions: 2,
        segment_max_vectors: 4,
        ram_budget_bytes: None,
        text: false,
        named_vectors: Default::default(),
    })
    .unwrap();
    assert_eq!(index.manifest().routing_epoch(), 1);
    assert_eq!(index.manifest().cell_wal_config(), CellWalConfig::default());
    drop(index);

    let reopened = BorsukIndex::open(&uri).unwrap();
    assert_eq!(reopened.manifest().routing_epoch(), 1);
    assert_eq!(
        reopened.manifest().cell_wal_config(),
        CellWalConfig::default()
    );
}

fn index_config(uri: String) -> IndexConfig {
    IndexConfig {
        uri,
        metric: VectorMetric::Euclidean,
        dimensions: 2,
        segment_max_vectors: 4,
        ram_budget_bytes: None,
        text: false,
        named_vectors: Default::default(),
    }
}

#[test]
fn index_add_is_visible_after_reopen_without_a_collection_current_swap() {
    let directory = tempfile::tempdir().unwrap();
    let uri = directory.path().to_string_lossy().into_owned();
    let mut index = BorsukIndex::create(index_config(uri.clone())).unwrap();
    let catalog_version = index.manifest().version;

    index
        .add(vec![VectorRecord::new("cell-tail", vec![1.0, 2.0])])
        .unwrap();

    assert_eq!(index.manifest().version, catalog_version);
    assert!(directory.path().join("cells/1/0/wal").is_dir());
    assert!(!directory.path().join("wal").exists());
    drop(index);

    let mut reopened = BorsukIndex::open(&uri).unwrap();
    assert_eq!(
        reopened
            .get_vector("cell-tail")
            .unwrap()
            .expect("cell WAL record"),
        vec![1.0, 2.0]
    );
    reopened.flush().unwrap();
    drop(reopened);

    let after_flush = BorsukIndex::open(&uri).unwrap();
    let records = after_flush.list_records(0, 10).unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].0.as_str(), "cell-tail");
}

#[test]
fn explicit_id_appends_do_not_touch_the_collection_wide_generated_id_counter() {
    let directory = tempfile::tempdir().unwrap();
    let uri = directory.path().to_string_lossy().into_owned();
    let mut index = BorsukIndex::create(index_config(uri)).unwrap();

    index
        .add(vec![VectorRecord::new("caller-owned-id", vec![1.0, 2.0])])
        .unwrap();

    assert!(
        !directory
            .path()
            .join("id-directory/generated/NEXT")
            .exists(),
        "cell-local explicit-id writes must not serialize through the global generated-id allocator"
    );
}

#[test]
fn explicit_id_appends_do_not_create_a_collection_wide_claim_gate() {
    let directory = tempfile::tempdir().unwrap();
    let uri = directory.path().to_string_lossy().into_owned();
    let mut index = BorsukIndex::create(index_config(uri)).unwrap();

    index
        .add(vec![VectorRecord::new("caller-owned-id", vec![1.0, 2.0])])
        .unwrap();

    assert!(
        !directory
            .path()
            .join("id-directory/claim-shards/GATE")
            .exists(),
        "disjoint explicit-ID batches must not serialize through a collection-wide gate"
    );
}

#[test]
fn explicit_id_batch_coordination_is_bounded_by_claim_shards() {
    let object_store = store();
    let mut index = BorsukIndex::create_with_object_store(
        Arc::clone(&object_store),
        IndexConfig {
            uri: "memory:///bounded-explicit-id-coordination".to_string(),
            metric: VectorMetric::Euclidean,
            dimensions: 2,
            segment_max_vectors: 1_000,
            ram_budget_bytes: None,
            text: false,
            named_vectors: Default::default(),
        },
    )
    .unwrap();
    let rows = 500_usize;
    let (_, report) = index
        .add_with_report(
            (0..rows)
                .map(|row| vec![row as f32, 0.0])
                .collect::<Vec<_>>(),
            Some((0..rows).map(|row| format!("explicit-{row}")).collect()),
        )
        .unwrap();

    assert!(
        report.requests.puts < 100,
        "one explicit-ID batch must coordinate by a fixed claim-shard bound, not one PUT per row: {:?}",
        report.requests
    );
}

#[test]
fn repeated_explicit_id_batches_do_not_rescan_the_accumulated_wal_frontier() {
    let object_store = store();
    let mut index = BorsukIndex::create_with_object_store(
        Arc::clone(&object_store),
        IndexConfig {
            uri: "memory:///incremental-explicit-id-coordination".to_string(),
            metric: VectorMetric::Euclidean,
            dimensions: 2,
            segment_max_vectors: 10_000,
            ram_budget_bytes: None,
            text: false,
            named_vectors: Default::default(),
        },
    )
    .unwrap();
    let mut final_report = None;
    for batch in 0..12 {
        let (_, report) = index
            .add_with_report(
                (0..64)
                    .map(|row| vec![(batch * 64 + row) as f32, 0.0])
                    .collect::<Vec<_>>(),
                Some(
                    (0..64)
                        .map(|row| format!("incremental-{}", batch * 64 + row))
                        .collect(),
                ),
            )
            .unwrap();
        final_report = Some(report);
    }

    let requests = final_report.unwrap().requests;
    assert!(
        requests.gets < 60,
        "an unchanged writer checkpoint must avoid rereading the accumulated WAL frontier: {requests:?}"
    );
}

#[test]
fn failed_multi_id_add_releases_claim_shards_after_duplicate_validation() {
    let directory = tempfile::tempdir().unwrap();
    let uri = directory.path().to_string_lossy().into_owned();
    let mut index = BorsukIndex::create(index_config(uri)).unwrap();

    let conflict_id = "already-claimed";
    index
        .add(vec![VectorRecord::new(conflict_id, vec![2.0, 3.0])])
        .unwrap();

    assert!(
        index
            .add(vec![
                VectorRecord::new("must-be-released", vec![1.0, 2.0]),
                VectorRecord::new(conflict_id, vec![2.0, 3.0]),
            ])
            .is_err()
    );
    index
        .add(vec![VectorRecord::new("must-be-released", vec![1.0, 2.0])])
        .expect("a failed batch must not leak claims for its earlier ids");
}

#[test]
fn concurrent_index_handles_append_without_collection_wide_current_contention() {
    let directory = tempfile::tempdir().unwrap();
    let uri = directory.path().to_string_lossy().into_owned();
    let created = BorsukIndex::create(index_config(uri.clone())).unwrap();
    let catalog_version = created.manifest().version;
    drop(created);

    let barrier = Arc::new(Barrier::new(2));
    let handles = ["left", "right"]
        .into_iter()
        .map(|id| {
            let barrier = Arc::clone(&barrier);
            let uri = uri.clone();
            std::thread::spawn(move || {
                let mut index = BorsukIndex::open(&uri).unwrap();
                barrier.wait();
                index
                    .add(vec![VectorRecord::new(id, vec![1.0, 2.0])])
                    .unwrap();
                index.manifest().version
            })
        })
        .collect::<Vec<_>>();
    for handle in handles {
        assert_eq!(handle.join().unwrap(), catalog_version);
    }

    let reopened = BorsukIndex::open(&uri).unwrap();
    let ids = reopened
        .list_records(0, 10)
        .unwrap()
        .into_iter()
        .map(|record| record.0.to_string())
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        ids,
        std::collections::BTreeSet::from(["left".to_string(), "right".to_string()])
    );
}

#[test]
fn gate_free_distinct_explicit_id_writers_all_commit() {
    const WRITERS: usize = 32;
    let object_store = store();
    let uri = "memory:///gate-free-distinct-explicit-ids";
    BorsukIndex::create_with_object_store(
        Arc::clone(&object_store),
        IndexConfig {
            uri: uri.to_string(),
            metric: VectorMetric::Euclidean,
            dimensions: 2,
            segment_max_vectors: 1_000,
            ram_budget_bytes: None,
            text: false,
            named_vectors: Default::default(),
        },
    )
    .unwrap();
    let barrier = Arc::new(Barrier::new(WRITERS));
    let handles = (0..WRITERS)
        .map(|writer| {
            let object_store = Arc::clone(&object_store);
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                let mut index = BorsukIndex::open_with_object_store(
                    object_store,
                    "memory:///gate-free-distinct-explicit-ids",
                )
                .unwrap();
                barrier.wait();
                index.add(vec![VectorRecord::new(
                    format!("distinct-writer-{writer:02}"),
                    vec![writer as f32, 0.0],
                )])
            })
        })
        .collect::<Vec<_>>();
    for handle in handles {
        handle.join().unwrap().unwrap();
    }

    let reopened = BorsukIndex::open_with_object_store(object_store, uri).unwrap();
    let ids = reopened
        .list_records(0, WRITERS)
        .unwrap()
        .into_iter()
        .map(|record| record.0.to_string())
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(ids.len(), WRITERS);
}

#[test]
fn concurrent_insert_only_batches_commit_a_shared_id_once() {
    let object_store = store();
    BorsukIndex::create_with_object_store(
        Arc::clone(&object_store),
        IndexConfig {
            uri: "memory:///concurrent-shared-explicit-id".to_string(),
            metric: VectorMetric::Euclidean,
            dimensions: 2,
            segment_max_vectors: 4,
            ram_budget_bytes: None,
            text: false,
            named_vectors: Default::default(),
        },
    )
    .unwrap();
    let barrier = Arc::new(Barrier::new(2));
    let writers = [1.0_f32, 2.0_f32].map(|value| {
        let object_store = Arc::clone(&object_store);
        let barrier = Arc::clone(&barrier);
        std::thread::spawn(move || {
            let mut index = BorsukIndex::open_with_object_store(
                object_store,
                "memory:///concurrent-shared-explicit-id",
            )
            .unwrap();
            barrier.wait();
            index.add(vec![VectorRecord::new("shared", vec![value, 0.0])])
        })
    });
    let outcomes = writers.map(|writer| writer.join().unwrap());

    assert_eq!(
        outcomes.iter().filter(|outcome| outcome.is_ok()).count(),
        1,
        "{outcomes:?}"
    );
    assert_eq!(
        outcomes.iter().filter(|outcome| outcome.is_err()).count(),
        1,
        "{outcomes:?}"
    );
    let reopened = BorsukIndex::open_with_object_store(
        object_store,
        "memory:///concurrent-shared-explicit-id",
    )
    .unwrap();
    assert_eq!(reopened.list_records(0, 10).unwrap().len(), 1);
}

#[test]
fn an_external_writer_invalidates_the_local_claim_checkpoint() {
    let object_store = store();
    let uri = "memory:///stale-explicit-id-checkpoint";
    let mut first = BorsukIndex::create_with_object_store(
        Arc::clone(&object_store),
        IndexConfig {
            uri: uri.to_string(),
            metric: VectorMetric::Euclidean,
            dimensions: 2,
            segment_max_vectors: 100,
            ram_budget_bytes: None,
            text: false,
            named_vectors: Default::default(),
        },
    )
    .unwrap();
    first
        .add(vec![VectorRecord::new("first-checkpoint", vec![1.0, 0.0])])
        .unwrap();

    let mut second = BorsukIndex::open_with_object_store(Arc::clone(&object_store), uri).unwrap();
    second
        .add(vec![VectorRecord::new("external-id", vec![2.0, 0.0])])
        .unwrap();

    let error = first
        .add(vec![VectorRecord::new("external-id", vec![3.0, 0.0])])
        .expect_err("a changed claim-shard version must force duplicate validation");
    assert!(error.to_string().contains("already exists"), "{error}");
}

#[test]
fn concurrent_generated_ids_are_unique_without_collection_manifest_updates() {
    let directory = tempfile::tempdir().unwrap();
    let uri = directory.path().to_string_lossy().into_owned();
    let created = BorsukIndex::create(index_config(uri.clone())).unwrap();
    let catalog_version = created.manifest().version;
    drop(created);

    let barrier = Arc::new(Barrier::new(16));
    let handles = (0..16)
        .map(|writer| {
            let barrier = Arc::clone(&barrier);
            let uri = uri.clone();
            std::thread::spawn(move || {
                let mut index = BorsukIndex::open(&uri).unwrap();
                barrier.wait();
                let ids = index.add_vectors(vec![vec![writer as f32, 1.0]]).unwrap();
                (ids[0].clone(), index.manifest().version)
            })
        })
        .collect::<Vec<_>>();
    let mut ids = std::collections::BTreeSet::new();
    for handle in handles {
        let (id, version) = handle.join().unwrap();
        assert_eq!(version, catalog_version);
        assert!(ids.insert(id));
    }
    assert_eq!(ids.len(), 16);

    let reopened = BorsukIndex::open(&uri).unwrap();
    assert_eq!(reopened.list_records(0, 32).unwrap().len(), 16);
}

#[test]
fn concurrent_same_id_upserts_reserve_distinct_generations() {
    let directory = tempfile::tempdir().unwrap();
    let uri = directory.path().to_string_lossy().into_owned();
    let mut created = BorsukIndex::create(index_config(uri.clone())).unwrap();
    created
        .add(vec![VectorRecord::new("shared", vec![0.0, 0.0])])
        .unwrap();
    created.flush().unwrap();
    let catalog_version = created.manifest().version;
    drop(created);

    let barrier = Arc::new(Barrier::new(16));
    let handles = (0..16)
        .map(|writer| {
            let barrier = Arc::clone(&barrier);
            let uri = uri.clone();
            std::thread::spawn(move || {
                let mut index = BorsukIndex::open(&uri).unwrap();
                barrier.wait();
                index
                    .upsert(vec![VectorRecord::new(
                        "shared",
                        vec![writer as f32 + 1.0, 0.0],
                    )])
                    .unwrap();
                index.manifest().version
            })
        })
        .collect::<Vec<_>>();
    for handle in handles {
        assert_eq!(handle.join().unwrap(), catalog_version);
    }

    let reopened = BorsukIndex::open(&uri).unwrap();
    let rows = reopened.list_records(0, 32).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0.as_str(), "shared");
    assert_ne!(rows[0].1, vec![0.0, 0.0]);
}

#[test]
fn large_upsert_batches_use_fixed_generation_shards() {
    let directory = tempfile::tempdir().unwrap();
    let uri = directory.path().to_string_lossy().into_owned();
    let mut index = BorsukIndex::create(index_config(uri)).unwrap();
    let rows = 500_usize;
    index
        .add(
            (0..rows)
                .map(|row| VectorRecord::new(format!("upsert-{row}"), vec![row as f32, 0.0]))
                .collect(),
        )
        .unwrap();

    index
        .upsert(
            (0..rows)
                .map(|row| VectorRecord::new(format!("upsert-{row}"), vec![0.0, row as f32]))
                .collect(),
        )
        .unwrap();

    assert!(
        !directory.path().join("id-directory/generations").exists(),
        "a batch upsert must not create one persistent generation counter per record"
    );
    let generation_shards = directory.path().join("id-directory/generation-shards");
    let shard_count = std::fs::read_dir(generation_shards)
        .unwrap()
        .filter_map(Result::ok)
        .filter(|entry| entry.path().join("NEXT").is_file())
        .count();
    assert!(
        (1..=16).contains(&shard_count),
        "generation allocation must touch at most the fixed shard count, got {shard_count}"
    );
}

#[test]
fn large_delete_batches_bound_generation_coordination_requests() {
    let object_store = store();
    let mut index = BorsukIndex::create_with_object_store(
        Arc::clone(&object_store),
        IndexConfig {
            uri: "memory:///bounded-delete-generation-coordination".to_string(),
            metric: VectorMetric::Euclidean,
            dimensions: 2,
            segment_max_vectors: 1_000,
            ram_budget_bytes: None,
            text: false,
            named_vectors: Default::default(),
        },
    )
    .unwrap();
    let rows = 500_usize;
    index
        .add(
            (0..rows)
                .map(|row| VectorRecord::new(format!("delete-{row}"), vec![row as f32, 0.0]))
                .collect(),
        )
        .unwrap();

    let report = index
        .delete_with_report((0..rows).map(|row| format!("delete-{row}")))
        .unwrap();

    assert_eq!(report.deleted, rows);
    assert!(
        report.requests.puts < 100,
        "one delete batch must reserve generation ranges per fixed shard, not per row: {:?}",
        report.requests
    );
    assert!(
        report.requests.gets < 100 && report.requests.heads < 100,
        "fixed generation shards must bound the complete coordination round trip: {:?}",
        report.requests
    );
}

#[test]
fn finish_bulk_load_freezes_multiple_logical_cells_and_routes_new_writes() {
    let directory = tempfile::tempdir().unwrap();
    let uri = directory.path().to_string_lossy().into_owned();
    let mut config = index_config(uri.clone());
    config.segment_max_vectors = 2;
    let mut index = BorsukIndex::create(config).unwrap();
    index
        .add(
            (0..8)
                .map(|value| {
                    VectorRecord::new(format!("base-{value}"), vec![value as f32 * 100.0, 0.0])
                })
                .collect(),
        )
        .unwrap();
    index.finish_bulk_load().unwrap();
    assert!(
        index.manifest().logical_cells().len() >= 4,
        "epoch-one cells must be independent of the bootstrap cell"
    );

    index
        .add(vec![
            VectorRecord::new("near-left", vec![0.0, 0.0]),
            VectorRecord::new("near-right", vec![700.0, 0.0]),
        ])
        .unwrap();
    drop(index);

    let reopened = BorsukIndex::open(&uri).unwrap();
    assert_eq!(
        reopened.get_vector("near-left").unwrap(),
        Some(vec![0.0, 0.0])
    );
    assert_eq!(
        reopened.get_vector("near-right").unwrap(),
        Some(vec![700.0, 0.0])
    );
}

#[test]
fn automatic_flush_threshold_is_applied_per_logical_cell() {
    let directory = tempfile::tempdir().unwrap();
    let uri = directory.path().to_string_lossy().into_owned();
    let mut config = index_config(uri.clone());
    config.segment_max_vectors = 1;
    let mut index = BorsukIndex::create_with_wal(
        config,
        borsuk::WalConfig {
            enabled: true,
            flush_threshold_runs: usize::MAX,
            flush_threshold_records: 3,
            flush_threshold_bytes: u64::MAX,
            collection_flush_threshold_bytes: u64::MAX,
        },
    )
    .unwrap();
    index
        .add(
            [0.0, 100.0, 200.0, 300.0]
                .into_iter()
                .map(|value| VectorRecord::new(format!("base-{value}"), vec![value, 0.0]))
                .collect(),
        )
        .unwrap();
    index.finish_bulk_load().unwrap();
    let catalog_version = index.manifest().version;

    index
        .add(vec![
            VectorRecord::new("left-a", vec![0.1, 0.0]),
            VectorRecord::new("left-b", vec![0.2, 0.0]),
            VectorRecord::new("right-a", vec![299.8, 0.0]),
            VectorRecord::new("right-b", vec![299.9, 0.0]),
        ])
        .unwrap();

    assert_eq!(
        index.manifest().version,
        catalog_version,
        "no cell crossed the three-record threshold"
    );
    assert_eq!(index.stats().wal_record_runs, 2);
    assert_eq!(index.stats().records, 8);
}

#[test]
fn automatic_flush_materializes_only_transactions_touching_the_hot_cell() {
    let directory = tempfile::tempdir().unwrap();
    let uri = directory.path().to_string_lossy().into_owned();
    let mut config = index_config(uri.clone());
    config.segment_max_vectors = 1;
    let mut index = BorsukIndex::create_with_wal(
        config,
        borsuk::WalConfig {
            enabled: true,
            flush_threshold_runs: usize::MAX,
            flush_threshold_records: 3,
            flush_threshold_bytes: u64::MAX,
            collection_flush_threshold_bytes: u64::MAX,
        },
    )
    .unwrap();
    index
        .add(
            [0.0, 100.0, 200.0, 300.0]
                .into_iter()
                .map(|value| VectorRecord::new(format!("base-{value}"), vec![value, 0.0]))
                .collect(),
        )
        .unwrap();
    index.finish_bulk_load().unwrap();

    index
        .add(vec![
            VectorRecord::new("left-a", vec![0.1, 0.0]),
            VectorRecord::new("left-b", vec![0.2, 0.0]),
            VectorRecord::new("right-atomic", vec![299.8, 0.0]),
        ])
        .unwrap();
    index
        .add(vec![VectorRecord::new("right-cold", vec![299.9, 0.0])])
        .unwrap();
    index
        .add(vec![VectorRecord::new("left-c", vec![0.3, 0.0])])
        .unwrap();

    assert_eq!(
        index.stats().wal_record_runs,
        1,
        "the independent cold-cell transaction must remain in its WAL lane"
    );
    drop(index);

    let reopened = BorsukIndex::open(&uri).unwrap();
    let ids = reopened
        .list_records(0, 16)
        .unwrap()
        .into_iter()
        .map(|row| row.0.to_string())
        .collect::<std::collections::BTreeSet<_>>();
    for id in ["left-a", "left-b", "left-c", "right-atomic", "right-cold"] {
        assert!(ids.contains(id), "missing {id}");
    }
    assert_eq!(reopened.stats().wal_record_runs, 1);
}

#[test]
fn flush_rewrites_lane_frontiers_without_losing_overlapping_writers() {
    let directory = tempfile::tempdir().unwrap();
    let uri = directory.path().to_string_lossy().into_owned();
    let mut created = BorsukIndex::create(index_config(uri.clone())).unwrap();
    created
        .add(vec![VectorRecord::new("seed", vec![0.0, 0.0])])
        .unwrap();
    drop(created);

    let barrier = Arc::new(Barrier::new(33));
    let mut handles = (0..32)
        .map(|writer| {
            let barrier = Arc::clone(&barrier);
            let uri = uri.clone();
            std::thread::spawn(move || {
                let mut index = BorsukIndex::open(&uri).unwrap();
                barrier.wait();
                index
                    .add(vec![VectorRecord::new(
                        format!("writer-{writer}"),
                        vec![writer as f32 + 1.0, 0.0],
                    )])
                    .unwrap();
            })
        })
        .collect::<Vec<_>>();
    let flush_barrier = Arc::clone(&barrier);
    let flush_uri = uri.clone();
    handles.push(std::thread::spawn(move || {
        let mut index = BorsukIndex::open(&flush_uri).unwrap();
        flush_barrier.wait();
        index.flush().unwrap();
    }));
    for handle in handles {
        handle.join().unwrap();
    }

    let reopened = BorsukIndex::open(&uri).unwrap();
    let ids = reopened
        .list_records(0, 64)
        .unwrap()
        .into_iter()
        .map(|row| row.0.to_string())
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(ids.len(), 33);
    assert!(ids.contains("seed"));
    for writer in 0..32 {
        assert!(ids.contains(&format!("writer-{writer}")));
    }
}

#[test]
fn compaction_rewrites_cell_base_without_losing_overlapping_writers() {
    let directory = tempfile::tempdir().unwrap();
    let uri = directory.path().to_string_lossy().into_owned();
    let mut created = BorsukIndex::create(index_config(uri.clone())).unwrap();
    created
        .add(vec![VectorRecord::new("seed", vec![0.0, 0.0])])
        .unwrap();
    drop(created);

    let barrier = Arc::new(Barrier::new(33));
    let mut handles = (0..32)
        .map(|writer| {
            let barrier = Arc::clone(&barrier);
            let uri = uri.clone();
            std::thread::spawn(move || {
                let mut index = BorsukIndex::open(&uri).unwrap();
                barrier.wait();
                index
                    .add(vec![VectorRecord::new(
                        format!("writer-{writer}"),
                        vec![writer as f32 + 1.0, 0.0],
                    )])
                    .unwrap();
            })
        })
        .collect::<Vec<_>>();
    let compact_barrier = Arc::clone(&barrier);
    let compact_uri = uri.clone();
    handles.push(std::thread::spawn(move || {
        let mut index = BorsukIndex::open(&compact_uri).unwrap();
        compact_barrier.wait();
        index.compact(borsuk::CompactionOptions::default()).unwrap();
    }));
    for handle in handles {
        handle.join().unwrap();
    }

    let reopened = BorsukIndex::open(&uri).unwrap();
    let ids = reopened
        .list_records(0, 64)
        .unwrap()
        .into_iter()
        .map(|row| row.0.to_string())
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(ids.len(), 33);
    assert!(ids.contains("seed"));
    for writer in 0..32 {
        assert!(ids.contains(&format!("writer-{writer}")));
    }
}

#[test]
fn prepared_multi_cell_runs_are_invisible_until_one_commit_marker() {
    let object_store = store();
    let wal = CellWalStore::new(
        Arc::clone(&object_store),
        "memory:///atomic",
        CellWalConfig::default(),
        b"writer-a".to_vec(),
    )
    .unwrap();
    let cells = [LogicalCellId::new(1, 3), LogicalCellId::new(1, 9)];
    let prepared = wal
        .prepare_transaction(
            "atomic-two-cells",
            &[input(cells[0], b"left"), input(cells[1], b"right")],
        )
        .unwrap();

    assert!(wal.committed_runs_snapshot(&cells).unwrap().is_empty());

    wal.commit_prepared(&prepared).unwrap();
    let visible = wal.committed_runs_snapshot(&cells).unwrap();
    assert_eq!(visible.len(), 2);
    assert!(
        visible
            .iter()
            .all(|run| run.transaction_id == "atomic-two-cells")
    );
}

#[test]
fn crash_after_prepare_can_be_recovered_without_exposing_partial_runs() {
    let object_store = store();
    let cells = [LogicalCellId::new(2, 4), LogicalCellId::new(2, 5)];
    let prepared = {
        let writer = CellWalStore::new(
            Arc::clone(&object_store),
            "memory:///prepare-crash",
            CellWalConfig::default(),
            b"writer-before-crash".to_vec(),
        )
        .unwrap();
        writer
            .prepare_transaction(
                "recover-prepared",
                &[input(cells[0], b"left"), input(cells[1], b"right")],
            )
            .unwrap()
    };

    let recovery = CellWalStore::new(
        object_store,
        "memory:///prepare-recovery",
        CellWalConfig::default(),
        b"recovery-writer".to_vec(),
    )
    .unwrap();
    assert!(recovery.committed_runs_snapshot(&cells).unwrap().is_empty());
    recovery.commit_prepared(&prepared).unwrap();
    assert_eq!(recovery.committed_runs_snapshot(&cells).unwrap().len(), 2);
}

#[test]
fn idempotency_key_reuses_one_transaction_and_rejects_changed_payloads() {
    let object_store = store();
    let wal = CellWalStore::new(
        Arc::clone(&object_store),
        "memory:///idempotent",
        CellWalConfig::default(),
        b"writer-a".to_vec(),
    )
    .unwrap();
    let cell = LogicalCellId::new(1, 0);
    let transaction_id = cell_wal_transaction_id(b"client-request-42").unwrap();
    let first = wal
        .commit(&transaction_id, &[input(cell, b"same")])
        .unwrap();
    let second = wal
        .commit(&transaction_id, &[input(cell, b"same")])
        .unwrap();

    assert_eq!(first, second);
    assert_eq!(wal.committed_runs_snapshot(&[cell]).unwrap().len(), 1);
    assert!(
        wal.commit(&transaction_id, &[input(cell, b"different")])
            .is_err()
    );
}

#[test]
fn thirty_two_same_lane_writers_rebase_without_losing_commits() {
    let object_store = store();
    let barrier = Arc::new(Barrier::new(32));
    let cell = LogicalCellId::new(7, 11);
    let handles = (0..32)
        .map(|writer| {
            let object_store = Arc::clone(&object_store);
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                let wal = CellWalStore::new(
                    object_store,
                    format!("memory:///hot-{writer}"),
                    CellWalConfig { lane_count: 1 },
                    format!("writer-{writer}").into_bytes(),
                )
                .unwrap();
                barrier.wait();
                wal.commit(
                    &format!("hot-{writer}"),
                    &[input(cell, format!("payload-{writer}"))],
                )
                .unwrap();
            })
        })
        .collect::<Vec<_>>();
    for handle in handles {
        handle.join().unwrap();
    }

    let reader = CellWalStore::new(
        Arc::clone(&object_store),
        "memory:///hot-reader",
        CellWalConfig { lane_count: 1 },
        b"reader".to_vec(),
    )
    .unwrap();
    let visible = reader.committed_runs_snapshot(&[cell]).unwrap();
    assert_eq!(visible.len(), 32);
    assert_eq!(
        visible
            .iter()
            .map(|run| run.transaction_id.as_str())
            .collect::<std::collections::BTreeSet<_>>()
            .len(),
        32
    );
}

#[test]
fn thirty_two_writers_spread_across_lanes_and_cells_without_loss() {
    let object_store = store();
    let barrier = Arc::new(Barrier::new(32));
    let cells = (0..4)
        .map(|cell| LogicalCellId::new(9, cell))
        .collect::<Vec<_>>();
    let handles = (0..32)
        .map(|writer| {
            let object_store = Arc::clone(&object_store);
            let barrier = Arc::clone(&barrier);
            let cell = cells[writer % cells.len()];
            std::thread::spawn(move || {
                let wal = CellWalStore::new(
                    object_store,
                    format!("memory:///distributed-{writer}"),
                    CellWalConfig::default(),
                    format!("writer-{writer}").into_bytes(),
                )
                .unwrap();
                barrier.wait();
                wal.commit(
                    &format!("distributed-{writer}"),
                    &[input(cell, format!("payload-{writer}"))],
                )
                .unwrap();
            })
        })
        .collect::<Vec<_>>();
    for handle in handles {
        handle.join().unwrap();
    }

    let reader = CellWalStore::new(
        object_store,
        "memory:///distributed-reader",
        CellWalConfig::default(),
        b"reader".to_vec(),
    )
    .unwrap();
    let visible = reader.committed_runs_snapshot(&cells).unwrap();
    assert_eq!(visible.len(), 32);
    assert!(
        visible
            .iter()
            .map(|run| run.lane)
            .collect::<std::collections::BTreeSet<_>>()
            .len()
            > 1
    );
}
