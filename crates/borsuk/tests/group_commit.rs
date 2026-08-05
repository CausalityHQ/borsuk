//! Process-local WAL group-commit integration coverage.

#[allow(dead_code)]
mod common;

use std::sync::{Arc, Barrier};

use borsuk::{
    BorsukIndex, GroupCommitConfig, GroupCommitWriter, IndexConfig, LeafMode, SearchOptions,
    VectorMetric, VectorRecord,
};
use futures_util::TryStreamExt;
use object_store::{ObjectStore, memory::InMemory};

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
fn drain_rebuilds_global_search_artifact_over_materialized_delta() {
    let directory = tempfile::tempdir().unwrap();
    let uri = directory.path().to_string_lossy().into_owned();
    let mut index = BorsukIndex::create(IndexConfig {
        uri: uri.clone(),
        metric: VectorMetric::Euclidean,
        dimensions: 8,
        segment_max_vectors: 16,
        ram_budget_bytes: None,
        text: false,
        named_vectors: Default::default(),
    })
    .unwrap();
    index
        .add(
            (0..128)
                .map(|row| {
                    VectorRecord::new(
                        format!("base-{row}"),
                        (0..8)
                            .map(|dimension| ((row * 17 + dimension * 11) % 101) as f32 / 101.0)
                            .collect(),
                    )
                })
                .collect(),
        )
        .unwrap();
    index.finish_bulk_load().unwrap();

    let delta = vec![10.0; 8];
    let writer = GroupCommitWriter::new(
        index,
        GroupCommitConfig {
            max_delay: std::time::Duration::ZERO,
            max_records: 1,
            worker_lanes: 1,
        },
    )
    .unwrap();
    writer
        .append(vec![VectorRecord::new("delta", delta.clone())])
        .unwrap();
    writer.drain().unwrap();

    let report = BorsukIndex::open(&uri)
        .unwrap()
        .search_with_report(
            &delta,
            SearchOptions::approx(1, LeafMode::SrhtPqScan)
                .with_max_segments(4)
                .with_max_candidates_per_segment(8),
        )
        .unwrap();
    assert_eq!(report.hits[0].id.as_str(), "delta");
    assert!(
        report.segments_searched <= 4,
        "drain must leave one read-optimized global artifact within the whole-query segment budget: {report:?}"
    );
}

#[test]
fn concurrent_appends_share_one_durable_wal_transaction() {
    const WRITERS: usize = 8;
    let directory = tempfile::tempdir().unwrap();
    let uri = directory.path().to_string_lossy().into_owned();
    let index = BorsukIndex::create(config(&uri)).unwrap();
    let writer = GroupCommitWriter::new(
        index,
        GroupCommitConfig {
            max_delay: std::time::Duration::from_millis(100),
            max_records: WRITERS,
            worker_lanes: 1,
        },
    )
    .unwrap();
    let barrier = Arc::new(Barrier::new(WRITERS));
    let handles = (0..WRITERS)
        .map(|ordinal| {
            let writer = writer.clone();
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                barrier.wait();
                writer
                    .append(vec![VectorRecord::new(
                        format!("grouped-{ordinal}"),
                        vec![ordinal as f32, 0.0],
                    )])
                    .unwrap()
            })
        })
        .collect::<Vec<_>>();
    let mut commit_sequences = Vec::new();
    let mut request_totals = Vec::new();
    for handle in handles {
        let receipt = handle.join().unwrap();
        assert_eq!(receipt.records, 1);
        assert_eq!(receipt.committed_records, WRITERS);
        commit_sequences.push(receipt.commit_sequence);
        request_totals.push(receipt.requests.total());
    }
    assert!(commit_sequences.iter().all(|sequence| *sequence == 1));
    assert!(
        request_totals
            .iter()
            .all(|requests| *requests == request_totals[0])
    );
    assert!(request_totals[0] > 0);

    let reopened = BorsukIndex::open(&uri).unwrap();
    assert_eq!(reopened.list_records(0, WRITERS).unwrap().len(), WRITERS);
    assert_eq!(reopened.stats().wal_record_runs, 1);
    assert!(
        !directory.path().join("id-directory/claim-pages").exists(),
        "the production group-commit path must not acquire strict-insert claims"
    );
}

#[test]
fn one_producer_can_pipeline_a_durable_group() {
    const RECORDS: usize = 8;
    let directory = tempfile::tempdir().unwrap();
    let uri = directory.path().to_string_lossy().into_owned();
    let index = BorsukIndex::create(config(&uri)).unwrap();
    let writer = GroupCommitWriter::new(
        index,
        GroupCommitConfig {
            max_delay: std::time::Duration::from_millis(100),
            max_records: RECORDS,
            worker_lanes: 1,
        },
    )
    .unwrap();
    let tickets = (0..RECORDS)
        .map(|ordinal| {
            writer
                .append_async(vec![VectorRecord::new(
                    format!("pipelined-{ordinal}"),
                    vec![ordinal as f32, 0.0],
                )])
                .unwrap()
        })
        .collect::<Vec<_>>();

    let receipts = tickets
        .into_iter()
        .map(|ticket| ticket.wait().unwrap())
        .collect::<Vec<_>>();
    assert!(
        receipts
            .iter()
            .all(|receipt| receipt.committed_records == RECORDS)
    );
    assert!(
        receipts
            .iter()
            .all(|receipt| receipt.commit_sequence == receipts[0].commit_sequence)
    );
    assert!(
        receipts
            .iter()
            .all(|receipt| receipt.commit_lane == receipts[0].commit_lane),
        "one producer must retain lane affinity so its pipeline forms groups"
    );
    drop(writer);
    assert_eq!(
        BorsukIndex::open(&uri)
            .unwrap()
            .list_records(0, RECORDS)
            .unwrap()
            .len(),
        RECORDS
    );
}

#[test]
fn independent_commit_lanes_publish_every_concurrent_append() {
    const LANES: usize = 4;
    const RECORDS: usize = 32;
    let directory = tempfile::tempdir().unwrap();
    let uri = directory.path().to_string_lossy().into_owned();
    let index = BorsukIndex::create(config(&uri)).unwrap();
    let writer = GroupCommitWriter::new(
        index,
        GroupCommitConfig {
            max_delay: std::time::Duration::ZERO,
            max_records: 1,
            worker_lanes: LANES,
        },
    )
    .unwrap();
    let barrier = Arc::new(Barrier::new(RECORDS));
    let handles = (0..RECORDS)
        .map(|ordinal| {
            let writer = writer.clone();
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                barrier.wait();
                writer
                    .append(vec![VectorRecord::new(
                        format!("lane-{ordinal}"),
                        vec![ordinal as f32, 0.0],
                    )])
                    .unwrap();
            })
        })
        .collect::<Vec<_>>();
    for handle in handles {
        handle.join().unwrap();
    }
    drop(writer);

    let reopened = BorsukIndex::open(&uri).unwrap();
    assert_eq!(reopened.list_records(0, RECORDS).unwrap().len(), RECORDS);
}

#[test]
fn independent_commit_lanes_report_lane_local_requests() {
    const LANES: usize = 4;
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let uri = "memory:///group-lane-local-request-counts";
    let writer = GroupCommitWriter::new(
        BorsukIndex::create_with_object_store(Arc::clone(&inner), config(uri)).unwrap(),
        GroupCommitConfig {
            max_delay: std::time::Duration::ZERO,
            max_records: 1,
            worker_lanes: LANES,
        },
    )
    .unwrap();
    let barrier = Arc::new(Barrier::new(LANES));
    let handles = (0..LANES)
        .map(|ordinal| {
            let writer = writer.clone();
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                barrier.wait();
                writer
                    .append(vec![VectorRecord::new(
                        format!("request-lane-{ordinal}"),
                        vec![ordinal as f32, 0.0],
                    )])
                    .unwrap()
            })
        })
        .collect::<Vec<_>>();
    let receipts = handles
        .into_iter()
        .map(|handle| handle.join().unwrap())
        .collect::<Vec<_>>();
    let by_lane = receipts
        .iter()
        .map(|receipt| (receipt.commit_lane, receipt.requests.total()))
        .collect::<std::collections::BTreeMap<_, _>>();
    assert_eq!(by_lane.len(), LANES);
    let minimum = *by_lane.values().min().unwrap();
    let maximum = *by_lane.values().max().unwrap();
    assert!(
        maximum - minimum <= 1,
        "only one lane may pay the one-time generation-lease reservation; lane zero must not be charged for other lanes: {by_lane:?}"
    );
}

#[test]
fn repeated_groups_amortize_last_write_wins_generation_coordination() {
    const GROUPS: usize = 12;
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (traced, operations) =
        common::FaultInjectingObjectStore::new(Arc::clone(&inner)).with_operation_log();
    let uri = "memory:///group-generation-lease";
    let index = BorsukIndex::create_with_object_store(Arc::new(traced), config(uri)).unwrap();
    operations.clear();
    let writer = GroupCommitWriter::new(
        index,
        GroupCommitConfig {
            max_delay: std::time::Duration::ZERO,
            max_records: 1,
            worker_lanes: 1,
        },
    )
    .unwrap();
    for ordinal in 0..GROUPS {
        writer
            .append(vec![VectorRecord::new(
                format!("leased-{ordinal}"),
                vec![ordinal as f32, 0.0],
            )])
            .unwrap();
    }

    let generation_gets = operations.count_matching(|operation, path| {
        operation == common::StoreOperation::Get
            && path.ends_with("id-directory/last-write-wins/NEXT")
    });
    let generation_puts = operations.count_matching(|operation, path| {
        operation == common::StoreOperation::Put
            && path.ends_with("id-directory/last-write-wins/NEXT")
    });
    assert_eq!(
        generation_gets, GROUPS,
        "each group must observe separately acknowledged external reservations"
    );
    assert_eq!(
        generation_puts, 1,
        "one range reservation should remove steady-state counter CAS writes"
    );
    drop(writer);
    assert_eq!(
        BorsukIndex::open_with_object_store(Arc::clone(&inner), uri)
            .unwrap()
            .list_records(0, GROUPS)
            .unwrap()
            .len(),
        GROUPS
    );
}

#[test]
fn pending_group_commits_keep_constant_coordination_cost_across_pressure_boundaries() {
    const GROUPS: usize = 600;
    const RECORDS_PER_GROUP: usize = 4;
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (traced, operations) =
        common::FaultInjectingObjectStore::new(Arc::clone(&inner)).with_operation_log();
    let uri = "memory:///pending-group-constant-cost";
    let index = BorsukIndex::create_with_object_store(Arc::new(traced), config(uri)).unwrap();
    operations.clear();
    let writer = GroupCommitWriter::new(
        index,
        GroupCommitConfig {
            max_delay: std::time::Duration::ZERO,
            max_records: RECORDS_PER_GROUP,
            worker_lanes: 1,
        },
    )
    .unwrap();

    for group in 0..GROUPS {
        let records = (0..RECORDS_PER_GROUP)
            .map(|record| {
                let ordinal = group * RECORDS_PER_GROUP + record;
                VectorRecord::new(format!("pending-{ordinal}"), vec![ordinal as f32, 0.0])
            })
            .collect();
        writer.append(records).unwrap();
    }
    drop(writer);

    assert_eq!(
        operations.count_matching(|_, path| path.starts_with("collection/wal-frontier/")),
        0,
        "the immutable pending path must not touch mutable frontier heads"
    );
    assert_eq!(
        operations.count_matching(|operation, path| {
            matches!(
                operation,
                common::StoreOperation::Get | common::StoreOperation::Head
            ) && path == "collection/CURRENT"
        }),
        GROUPS,
        "each group must pin schema generation exactly once until write-epoch leases replace this safety read"
    );
    assert_eq!(
        operations.count_matching(|operation, path| {
            operation == common::StoreOperation::Get
                && path.starts_with("transactions/")
                && path.ends_with("/STATE")
        }),
        0,
        "the pending create is the ACK boundary; state finalization belongs to checkpoint"
    );
    assert_eq!(
        operations.count_matching(|operation, path| {
            operation == common::StoreOperation::Put
                && path.starts_with("collection/write-epochs/")
                && path.contains("/pending/")
                && path.ends_with(".commit")
        }),
        GROUPS,
        "every durable group must end in exactly one immutable pending create"
    );
    assert_eq!(
        operations.count_matching(|operation, path| {
            operation == common::StoreOperation::Put && path == "collection/CURRENT"
        }),
        0,
        "group acknowledgements must not publish a catalog"
    );

    let reopened = BorsukIndex::open_with_object_store(Arc::clone(&inner), uri).unwrap();
    assert_eq!(
        reopened
            .list_records(0, GROUPS * RECORDS_PER_GROUP)
            .unwrap()
            .len(),
        GROUPS * RECORDS_PER_GROUP
    );
}

#[test]
fn ordinary_put_publishes_without_mutable_frontier_coordination() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (traced, operations) =
        common::FaultInjectingObjectStore::new(Arc::clone(&inner)).with_operation_log();
    let uri = "memory:///ordinary-put-pending";
    let mut index = BorsukIndex::create_with_object_store(Arc::new(traced), config(uri)).unwrap();
    operations.clear();

    index
        .put(vec![VectorRecord::new("ordinary", vec![1.0, 0.0])])
        .unwrap();

    assert_eq!(
        operations.count_matching(|_, path| path.starts_with("collection/wal-frontier/")),
        0,
        "ordinary mutations must use the same immutable publication path as group commit"
    );
    assert_eq!(
        operations.count_matching(|operation, path| {
            operation == common::StoreOperation::Put
                && path.starts_with("collection/write-epochs/")
                && path.contains("/pending/")
                && path.ends_with(".commit")
        }),
        1
    );
    drop(index);
    assert_eq!(
        BorsukIndex::open_with_object_store(inner, uri)
            .unwrap()
            .list_records(0, 10)
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn drain_checkpoints_every_preceding_group_and_removes_pending_objects() {
    const GROUPS: usize = 600;
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let uri = "memory:///group-drain-checkpoint";
    let writer = GroupCommitWriter::new(
        BorsukIndex::create_with_object_store(Arc::clone(&inner), config(uri)).unwrap(),
        GroupCommitConfig {
            max_delay: std::time::Duration::ZERO,
            max_records: 1,
            worker_lanes: 4,
        },
    )
    .unwrap();
    for ordinal in 0..GROUPS {
        writer
            .append(vec![VectorRecord::new(
                format!("drained-{ordinal}"),
                vec![ordinal as f32, 0.0],
            )])
            .unwrap();
    }

    writer.drain().unwrap();

    let reopened = BorsukIndex::open_with_object_store(Arc::clone(&inner), uri).unwrap();
    assert_eq!(reopened.list_records(0, GROUPS).unwrap().len(), GROUPS);
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let pending = runtime
        .block_on(
            inner
                .list(Some(&"collection/write-epochs".into()))
                .try_collect::<Vec<_>>(),
        )
        .unwrap()
        .into_iter()
        .filter(|object| object.location.as_ref().contains("/pending/"))
        .count();
    assert_eq!(
        pending, 0,
        "drain must retire every captured pending commit"
    );
}

#[test]
fn alternating_writer_lanes_preserve_sequential_last_write_wins() {
    let directory = tempfile::tempdir().unwrap();
    let uri = directory.path().to_string_lossy().into_owned();
    let index = BorsukIndex::create(config(&uri)).unwrap();
    let writer = GroupCommitWriter::new(
        index,
        GroupCommitConfig {
            max_delay: std::time::Duration::ZERO,
            max_records: 1,
            worker_lanes: 2,
        },
    )
    .unwrap();
    let first_lane = writer.clone();
    let second_lane = writer.clone();
    first_lane
        .append(vec![VectorRecord::new("same", vec![1.0, 0.0])])
        .unwrap();
    second_lane
        .append(vec![VectorRecord::new("same", vec![2.0, 0.0])])
        .unwrap();
    first_lane
        .append(vec![VectorRecord::new("same", vec![3.0, 0.0])])
        .unwrap();

    assert_eq!(
        BorsukIndex::open(&uri).unwrap().get_vector("same").unwrap(),
        Some(vec![3.0, 0.0]),
        "the latest acknowledged sequential append must win across lanes"
    );
}

#[test]
fn group_writer_observes_later_external_put_generation() {
    let directory = tempfile::tempdir().unwrap();
    let uri = directory.path().to_string_lossy().into_owned();
    let index = BorsukIndex::create(config(&uri)).unwrap();
    let writer = GroupCommitWriter::new(
        index,
        GroupCommitConfig {
            max_delay: std::time::Duration::ZERO,
            max_records: 1,
            worker_lanes: 1,
        },
    )
    .unwrap();
    writer
        .append(vec![VectorRecord::new("same", vec![1.0, 0.0])])
        .unwrap();
    let mut external = BorsukIndex::open(&uri).unwrap();
    external
        .put(vec![VectorRecord::new("same", vec![2.0, 0.0])])
        .unwrap();
    writer
        .append(vec![VectorRecord::new("same", vec![3.0, 0.0])])
        .unwrap();

    assert_eq!(
        BorsukIndex::open(&uri).unwrap().get_vector("same").unwrap(),
        Some(vec![3.0, 0.0]),
        "a group writer must advance past a separately acknowledged put"
    );
}

#[test]
fn live_generation_lease_fails_closed_when_durable_counter_disappears() {
    let directory = tempfile::tempdir().unwrap();
    let uri = directory.path().to_string_lossy().into_owned();
    let index = BorsukIndex::create(config(&uri)).unwrap();
    let writer = GroupCommitWriter::new(
        index,
        GroupCommitConfig {
            max_delay: std::time::Duration::ZERO,
            max_records: 1,
            worker_lanes: 1,
        },
    )
    .unwrap();
    writer
        .append(vec![VectorRecord::new("before-loss", vec![1.0, 0.0])])
        .unwrap();
    std::fs::remove_file(directory.path().join("id-directory/last-write-wins/NEXT")).unwrap();

    let error = writer
        .append(vec![VectorRecord::new("after-loss", vec![2.0, 0.0])])
        .unwrap_err();
    assert!(
        error.to_string().contains("disappeared"),
        "unexpected error: {error}"
    );
    assert!(
        BorsukIndex::open(&uri)
            .unwrap()
            .get_vector("after-loss")
            .unwrap()
            .is_none()
    );
}

#[test]
fn reopened_group_writer_advances_past_abandoned_generation_lease() {
    let directory = tempfile::tempdir().unwrap();
    let uri = directory.path().to_string_lossy().into_owned();
    let index = BorsukIndex::create(config(&uri)).unwrap();
    let writer = GroupCommitWriter::new(index, GroupCommitConfig::default()).unwrap();
    writer
        .append(vec![VectorRecord::new("same", vec![1.0, 0.0])])
        .unwrap();
    drop(writer);
    let reopened = BorsukIndex::open(&uri).unwrap();
    let writer = GroupCommitWriter::new(reopened, GroupCommitConfig::default()).unwrap();
    writer
        .append(vec![VectorRecord::new("same", vec![2.0, 0.0])])
        .unwrap();
    drop(writer);

    assert_eq!(
        BorsukIndex::open(&uri).unwrap().get_vector("same").unwrap(),
        Some(vec![2.0, 0.0])
    );
}

#[test]
fn group_commit_configuration_fails_closed() {
    let directory = tempfile::tempdir().unwrap();
    let uri = directory.path().to_string_lossy().into_owned();
    let index = BorsukIndex::create(config(&uri)).unwrap();
    let error = match GroupCommitWriter::new(
        index,
        GroupCommitConfig {
            max_delay: std::time::Duration::ZERO,
            max_records: 0,
            worker_lanes: 1,
        },
    ) {
        Ok(_) => panic!("zero-sized groups must be rejected"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("max_records must be positive"));
}

#[test]
fn one_invalid_group_fails_every_joined_caller_without_partial_visibility() {
    let directory = tempfile::tempdir().unwrap();
    let uri = directory.path().to_string_lossy().into_owned();
    let index = BorsukIndex::create(config(&uri)).unwrap();
    let writer = GroupCommitWriter::new(
        index,
        GroupCommitConfig {
            max_delay: std::time::Duration::from_millis(100),
            max_records: 2,
            worker_lanes: 1,
        },
    )
    .unwrap();
    let barrier = Arc::new(Barrier::new(2));
    let handles = (0..2)
        .map(|ordinal| {
            let writer = writer.clone();
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                barrier.wait();
                writer.append(vec![VectorRecord::new(
                    "duplicate",
                    vec![ordinal as f32, 0.0],
                )])
            })
        })
        .collect::<Vec<_>>();
    for handle in handles {
        assert!(handle.join().unwrap().is_err());
    }

    let reopened = BorsukIndex::open(&uri).unwrap();
    assert!(reopened.list_records(0, 8).unwrap().is_empty());
}

#[test]
fn cross_cell_group_uses_one_record_bundle_and_preserves_exact_recall() {
    let directory = tempfile::tempdir().unwrap();
    let uri = directory.path().to_string_lossy().into_owned();
    let mut index = BorsukIndex::create(IndexConfig {
        segment_max_vectors: 1,
        ..config(&uri)
    })
    .unwrap();
    index
        .add(vec![
            VectorRecord::new("base-left", vec![0.0, 0.0]),
            VectorRecord::new("base-right", vec![100.0, 0.0]),
        ])
        .unwrap();
    index.finish_bulk_load().unwrap();
    let writer = GroupCommitWriter::new(
        index,
        GroupCommitConfig {
            max_delay: std::time::Duration::from_millis(100),
            max_records: 2,
            worker_lanes: 1,
        },
    )
    .unwrap();
    let barrier = Arc::new(Barrier::new(2));
    let handles = [("new-left", vec![0.1, 0.0]), ("new-right", vec![99.9, 0.0])]
        .into_iter()
        .map(|(id, vector)| {
            let writer = writer.clone();
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                barrier.wait();
                writer.append(vec![VectorRecord::new(id, vector)]).unwrap()
            })
        })
        .collect::<Vec<_>>();
    for handle in handles {
        let receipt = handle.join().unwrap();
        assert_eq!(receipt.committed_records, 2);
        assert!(receipt.requests.total() < 100);
    }
    drop(writer);

    let reopened = BorsukIndex::open(&uri).unwrap();
    assert_eq!(reopened.stats().wal_record_runs, 1);
    for (id, vector) in [("new-left", [0.1, 0.0]), ("new-right", [99.9, 0.0])] {
        let report = reopened
            .search_with_report(&vector, SearchOptions::exact(1))
            .unwrap();
        assert_eq!(report.hits[0].id.as_str(), id);
    }
}

#[test]
fn sequential_groups_replace_the_same_id() {
    let directory = tempfile::tempdir().unwrap();
    let uri = directory.path().to_string_lossy().into_owned();
    let index = BorsukIndex::create(config(&uri)).unwrap();
    let writer = GroupCommitWriter::new(index, GroupCommitConfig::default()).unwrap();

    writer
        .append(vec![VectorRecord::new("shared", vec![1.0, 0.0])])
        .unwrap();
    writer
        .append(vec![VectorRecord::new("shared", vec![0.0, 1.0])])
        .unwrap();
    drop(writer);

    let reopened = BorsukIndex::open(&uri).unwrap();
    assert_eq!(reopened.list_records(0, 10).unwrap().len(), 1);
    assert_eq!(reopened.get_vector("shared").unwrap(), Some(vec![0.0, 1.0]));
}
