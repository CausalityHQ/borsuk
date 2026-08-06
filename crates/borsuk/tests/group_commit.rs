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

fn ids_in_ownership_lane(lane: usize, count: usize) -> Vec<String> {
    let mut ids = Vec::with_capacity(count);
    for ordinal in 0.. {
        let id = format!("ownership-{lane}-{ordinal}");
        let digest = blake3::hash(id.as_bytes());
        let hash = u64::from_le_bytes(digest.as_bytes()[..8].try_into().unwrap());
        if hash as usize % 8 == lane {
            ids.push(id);
            if ids.len() == count {
                return ids;
            }
        }
    }
    unreachable!()
}

#[test]
fn drain_keeps_global_base_and_searches_materialized_delta_without_rebuild() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (traced, operations) =
        common::FaultInjectingObjectStore::new(Arc::clone(&inner)).with_operation_log();
    let uri = "memory:///group-drain-global-delta";
    let mut index = BorsukIndex::create_with_object_store(
        Arc::new(traced),
        IndexConfig {
            uri: uri.to_string(),
            metric: VectorMetric::Euclidean,
            dimensions: 8,
            segment_max_vectors: 16,
            ram_budget_bytes: None,
            text: false,
            named_vectors: Default::default(),
        },
    )
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
    operations.clear();

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
    let tail_report = BorsukIndex::open_with_object_store(Arc::clone(&inner), uri)
        .unwrap()
        .search_with_report(
            &[9.9; 8],
            SearchOptions::approx(5, LeafMode::SrhtPqScan)
                .with_max_segments(4)
                .with_max_candidates_per_segment(8)
                .with_max_bytes(1),
        )
        .unwrap();
    assert_eq!(tail_report.hits[0].id.as_str(), "delta");
    assert!(tail_report.bytes_read > 0);
    assert_eq!(
        tail_report.global_scan_chunks_searched, 0,
        "lane-log bytes must consume the shared request budget before immutable search: {tail_report:?}"
    );
    writer.drain().unwrap();

    assert_eq!(
        operations.count_matching(|operation, path| {
            operation == common::StoreOperation::Put && path.starts_with("global-pq/")
        }),
        0,
        "drain must not rebuild the corpus-wide global PQ artifact"
    );
    let report = BorsukIndex::open_with_object_store(inner, uri)
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
        report.global_scan_chunks_searched > 0,
        "drain must retain and search the immutable global base: {report:?}"
    );
}

#[test]
fn concurrent_drains_serialize_one_materialization_frontier() {
    let directory = tempfile::tempdir().unwrap();
    let uri = directory.path().to_string_lossy().into_owned();
    let writer = GroupCommitWriter::new(
        BorsukIndex::create(config(&uri)).unwrap(),
        GroupCommitConfig {
            max_delay: std::time::Duration::ZERO,
            max_records: 1,
            worker_lanes: 4,
        },
    )
    .unwrap();
    writer
        .append(
            (0..32)
                .map(|ordinal| {
                    VectorRecord::new(
                        format!("concurrent-drain-{ordinal}"),
                        vec![ordinal as f32, 0.0],
                    )
                })
                .collect(),
        )
        .unwrap();
    let first = writer.clone();
    let second = writer.clone();
    let barrier = Arc::new(Barrier::new(2));
    let drains = [first, second]
        .into_iter()
        .map(|writer| {
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                barrier.wait();
                writer.drain()
            })
        })
        .collect::<Vec<_>>();

    for drain in drains {
        drain.join().unwrap().unwrap();
    }
    assert_eq!(
        BorsukIndex::open(&uri)
            .unwrap()
            .list_records(0, 64)
            .unwrap()
            .len(),
        32
    );
}

#[test]
fn repeated_upsert_drains_do_not_multiply_visible_ids() {
    let directory = tempfile::tempdir().unwrap();
    let uri = directory.path().to_string_lossy().into_owned();
    let writer = GroupCommitWriter::new(
        BorsukIndex::create(config(&uri)).unwrap(),
        GroupCommitConfig::default(),
    )
    .unwrap();
    for generation in 0..5 {
        writer
            .append(
                (0..32)
                    .map(|ordinal| {
                        VectorRecord::new(
                            format!("repeated-{ordinal}"),
                            vec![generation as f32, ordinal as f32],
                        )
                    })
                    .collect(),
            )
            .unwrap();
        writer.drain().unwrap();
    }

    let reopened = BorsukIndex::open(&uri).unwrap();
    assert_eq!(reopened.list_records(0, 1_000).unwrap().len(), 32);
    assert_eq!(
        reopened.get_vector("repeated-7").unwrap(),
        Some(vec![4.0, 7.0])
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
    let handles = ids_in_ownership_lane(0, WRITERS)
        .into_iter()
        .enumerate()
        .map(|(ordinal, id)| {
            let writer = writer.clone();
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                barrier.wait();
                writer
                    .append(vec![VectorRecord::new(id, vec![ordinal as f32, 0.0])])
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
    assert!(
        !directory.path().join("id-directory/claim-pages").exists(),
        "the production group-commit path must not acquire strict-insert claims"
    );
}

#[test]
fn lane_log_ack_is_one_put_and_visible_after_reopen() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let uri = "memory:///group-lane-log-cutover";
    let writer = GroupCommitWriter::new(
        BorsukIndex::create_with_object_store(Arc::clone(&inner), config(uri)).unwrap(),
        GroupCommitConfig {
            max_delay: std::time::Duration::ZERO,
            max_records: 1,
            worker_lanes: 1,
        },
    )
    .unwrap();

    let receipt = writer
        .append(vec![VectorRecord::new("durable", vec![1.0, 2.0])])
        .unwrap();

    assert_eq!(receipt.lane_receipts.len(), 1);
    assert!(receipt.lane_receipts[0].lease_epoch > 0);
    assert_eq!(receipt.requests.puts, 1);
    assert_eq!(receipt.requests.gets, 0);
    assert_eq!(receipt.requests.heads, 0);
    assert_eq!(receipt.requests.lists, 0);
    drop(writer);
    assert_eq!(
        BorsukIndex::open_with_object_store(inner, uri)
            .unwrap()
            .get_vector("durable")
            .unwrap(),
        Some(vec![1.0, 2.0])
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
    let tickets = ids_in_ownership_lane(0, RECORDS)
        .into_iter()
        .enumerate()
        .map(|(ordinal, id)| {
            writer
                .append_async(vec![VectorRecord::new(id, vec![ordinal as f32, 0.0])])
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
fn producer_clones_route_the_same_id_to_one_ownership_lane() {
    let directory = tempfile::tempdir().unwrap();
    let uri = directory.path().to_string_lossy().into_owned();
    let writer = GroupCommitWriter::new(
        BorsukIndex::create(config(&uri)).unwrap(),
        GroupCommitConfig {
            max_delay: std::time::Duration::ZERO,
            max_records: 1,
            worker_lanes: 4,
        },
    )
    .unwrap();
    let first = writer.clone();
    let second = writer.clone();

    let first_receipt = first
        .append(vec![VectorRecord::new("shared-id", vec![1.0, 0.0])])
        .unwrap();
    let second_receipt = second
        .append(vec![VectorRecord::new("shared-id", vec![2.0, 0.0])])
        .unwrap();

    assert_eq!(
        first_receipt.commit_lane, second_receipt.commit_lane,
        "producer identity must not change the ownership lane for an id"
    );
}

#[test]
fn one_append_fans_records_out_to_their_ownership_lanes() {
    let probe_directory = tempfile::tempdir().unwrap();
    let probe_uri = probe_directory.path().to_string_lossy().into_owned();
    let probe = GroupCommitWriter::new(
        BorsukIndex::create(config(&probe_uri)).unwrap(),
        GroupCommitConfig {
            max_delay: std::time::Duration::ZERO,
            max_records: 1,
            worker_lanes: 4,
        },
    )
    .unwrap();
    let mut ids_by_lane = std::collections::BTreeMap::new();
    for ordinal in 0..64 {
        let id = format!("probe-{ordinal}");
        let receipt = probe
            .append(vec![VectorRecord::new(
                id.clone(),
                vec![ordinal as f32, 0.0],
            )])
            .unwrap();
        ids_by_lane.entry(receipt.commit_lane).or_insert(id);
        if ids_by_lane.len() == 4 {
            break;
        }
    }
    assert_eq!(ids_by_lane.len(), 4, "probe ids must cover every test lane");
    let selected = ids_by_lane.into_iter().take(2).collect::<Vec<_>>();

    let directory = tempfile::tempdir().unwrap();
    let uri = directory.path().to_string_lossy().into_owned();
    let writer = GroupCommitWriter::new(
        BorsukIndex::create(config(&uri)).unwrap(),
        GroupCommitConfig {
            max_delay: std::time::Duration::ZERO,
            max_records: 1,
            worker_lanes: 4,
        },
    )
    .unwrap();
    let receipt = writer
        .append(
            selected
                .iter()
                .enumerate()
                .map(|(ordinal, (_, id))| VectorRecord::new(id.clone(), vec![ordinal as f32, 0.0]))
                .collect(),
        )
        .unwrap();

    assert_eq!(receipt.records, 2);
    assert_eq!(receipt.lane_receipts.len(), 2);
    assert_eq!(
        receipt
            .lane_receipts
            .iter()
            .map(|lane| lane.commit_lane)
            .collect::<std::collections::BTreeSet<_>>(),
        selected
            .iter()
            .map(|(lane, _)| *lane)
            .collect::<std::collections::BTreeSet<_>>()
    );
}

#[test]
fn independent_commit_lanes_report_lane_local_requests() {
    const LANES: usize = 4;
    const RECORDS: usize = 64;
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
    let barrier = Arc::new(Barrier::new(RECORDS));
    let handles = (0..RECORDS)
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
    assert_eq!(by_lane.len(), 8);
    let minimum = *by_lane.values().min().unwrap();
    let maximum = *by_lane.values().max().unwrap();
    assert!(
        maximum - minimum == 0,
        "every fixed ownership lane must report the same one-write acknowledgement cost: {by_lane:?}"
    );
}

#[test]
fn repeated_groups_have_zero_read_one_write_acknowledgements() {
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
    operations.clear();
    let mut receipts = Vec::with_capacity(GROUPS);
    for ordinal in 0..GROUPS {
        receipts.push(
            writer
                .append(vec![VectorRecord::new(
                    format!("leased-{ordinal}"),
                    vec![ordinal as f32, 0.0],
                )])
                .unwrap(),
        );
    }

    assert!(receipts.iter().all(|receipt| receipt.requests.puts == 1));
    assert!(receipts.iter().all(|receipt| receipt.requests.gets == 0));
    assert!(receipts.iter().all(|receipt| receipt.requests.heads == 0));
    assert!(receipts.iter().all(|receipt| receipt.requests.lists == 0));

    assert_eq!(
        operations.count_matching(|operation, _| operation == common::StoreOperation::Get),
        0
    );
    assert_eq!(
        operations.count_matching(|operation, _| operation == common::StoreOperation::Head),
        0
    );
    assert_eq!(
        operations.count_matching(|operation, _| operation == common::StoreOperation::List),
        0
    );
    assert!(
        operations.count_matching(|operation, _| operation == common::StoreOperation::Put)
            >= GROUPS,
        "post-ACK spill may add maintenance PUTs"
    );
    assert!(
        operations.count_matching(|operation, path| {
            operation == common::StoreOperation::Put
                && path.starts_with("lane-log/lanes/")
                && path.ends_with("/HEAD")
        }) >= GROUPS,
        "every acknowledgement PUT targets its authoritative lane HEAD; spill may add HEAD CASes"
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
fn small_inline_groups_do_not_trigger_synchronous_spill_maintenance() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (traced, operations) =
        common::FaultInjectingObjectStore::new(Arc::clone(&inner)).with_operation_log();
    let uri = "memory:///group-inline-spill";
    let index = BorsukIndex::create_with_object_store(Arc::new(traced), config(uri)).unwrap();
    let writer = GroupCommitWriter::new(
        index,
        GroupCommitConfig {
            max_delay: std::time::Duration::ZERO,
            max_records: 1,
            worker_lanes: 1,
        },
    )
    .unwrap();
    operations.clear();
    let ids = (0_u64..)
        .map(|ordinal| format!("spill-{ordinal}"))
        .filter(|id| {
            let digest = blake3::hash(id.as_bytes());
            let mut prefix = [0_u8; 8];
            prefix.copy_from_slice(&digest.as_bytes()[..8]);
            u64::from_le_bytes(prefix) % 8 == 0
        })
        .take(5)
        .collect::<Vec<_>>();

    for (ordinal, id) in ids.iter().enumerate() {
        let receipt = writer
            .append(vec![VectorRecord::new(id, vec![ordinal as f32, 0.0])])
            .unwrap();
        assert_eq!(receipt.requests.puts, 1, "spill must stay outside ACK");
    }

    assert_eq!(
        operations.count_matching(|operation, path| {
            operation == common::StoreOperation::Put && path.contains("/blocks/")
        }),
        0,
        "small groups must remain inline until the byte-bound or background materialization"
    );
    assert_eq!(
        operations.count_matching(|operation, path| {
            operation == common::StoreOperation::Put && path.ends_with("/HEAD")
        }),
        5,
        "each small group must issue only its acknowledgement HEAD CAS"
    );
    drop(writer);
    assert_eq!(
        BorsukIndex::open_with_object_store(inner, uri)
            .unwrap()
            .list_records(0, ids.len())
            .unwrap()
            .len(),
        ids.len()
    );
}

#[test]
fn background_materialization_keeps_sustained_ingest_below_the_hard_tail_bound() {
    const GROUPS: usize = 600;
    const RECORDS_PER_GROUP: usize = 4;
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (traced, _operations) =
        common::FaultInjectingObjectStore::new(Arc::clone(&inner)).with_operation_log();
    let uri = "memory:///pending-group-constant-cost";
    let index = BorsukIndex::create_with_object_store(Arc::new(traced), config(uri)).unwrap();
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
    writer.drain().unwrap();
    drop(writer);

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
fn preregistered_worker_lane_factors_preserve_ack_reopen_last_write_and_drain() {
    for worker_lanes in [1, 2, 4, 8] {
        let directory = tempfile::tempdir().unwrap();
        let uri = directory.path().to_string_lossy().into_owned();
        let writer = GroupCommitWriter::new(
            BorsukIndex::create(config(&uri)).unwrap(),
            GroupCommitConfig {
                max_delay: std::time::Duration::ZERO,
                max_records: 1,
                worker_lanes,
            },
        )
        .unwrap();

        let receipt = writer
            .append(vec![VectorRecord::new("same", vec![1.0, 0.0])])
            .unwrap();
        assert_eq!(receipt.records, 1);
        assert!(
            receipt.commit_lane < 8,
            "receipt identifies a persisted lane"
        );
        assert_eq!(
            BorsukIndex::open(&uri).unwrap().get_vector("same").unwrap(),
            Some(vec![1.0, 0.0]),
            "worker_lanes={worker_lanes} must expose acknowledged data after reopen"
        );

        writer
            .append(vec![VectorRecord::new("same", vec![2.0, 0.0])])
            .unwrap();
        writer.drain().unwrap();
        drop(writer);
        let reopened = BorsukIndex::open(&uri).unwrap();
        assert_eq!(
            reopened.get_vector("same").unwrap(),
            Some(vec![2.0, 0.0]),
            "worker_lanes={worker_lanes} must preserve last-write-wins through drain"
        );
        assert_eq!(reopened.list_records(0, 10).unwrap().len(), 1);
    }
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
fn lane_append_revives_an_id_deleted_by_the_manifest_writer() {
    let directory = tempfile::tempdir().unwrap();
    let uri = directory.path().to_string_lossy().into_owned();
    let mut index = BorsukIndex::create(config(&uri)).unwrap();
    index
        .put(vec![VectorRecord::new("revived", vec![1.0, 0.0])])
        .unwrap();
    index.delete(["revived"]).unwrap();
    let writer = GroupCommitWriter::new(index, GroupCommitConfig::default()).unwrap();

    writer
        .append(vec![VectorRecord::new("revived", vec![7.0, 0.0])])
        .unwrap();
    drop(writer);

    assert_eq!(
        BorsukIndex::open(&uri)
            .unwrap()
            .get_vector("revived")
            .unwrap(),
        Some(vec![7.0, 0.0])
    );
}

#[test]
fn live_lane_writer_does_not_recreate_a_disappeared_head() {
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
    let receipt = writer
        .append(vec![VectorRecord::new("before-loss", vec![1.0, 0.0])])
        .unwrap();
    std::fs::remove_file(
        directory
            .path()
            .join(format!("lane-log/lanes/{:04}/HEAD", receipt.commit_lane)),
    )
    .unwrap();

    let error = writer
        .append(vec![VectorRecord::new("before-loss", vec![2.0, 0.0])])
        .unwrap_err();
    assert!(
        error.to_string().contains("concurrent modification"),
        "unexpected error: {error}"
    );
    assert!(
        !directory
            .path()
            .join(format!("lane-log/lanes/{:04}/HEAD", receipt.commit_lane))
            .exists()
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
fn group_writer_rejects_modalities_not_yet_materialized_by_lane_log() {
    let directory = tempfile::tempdir().unwrap();
    let uri = directory.path().to_string_lossy().into_owned();
    let result = GroupCommitWriter::new(
        BorsukIndex::create(IndexConfig {
            text: true,
            ..config(&uri)
        })
        .unwrap(),
        GroupCommitConfig::default(),
    );
    let error = match result {
        Ok(_) => panic!("unsupported modality must fail closed"),
        Err(error) => error,
    };

    assert!(error.to_string().contains("text or named"), "{error}");
}

#[test]
fn same_id_upserts_in_one_group_commit_in_submission_order() {
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
    let first = writer
        .append_async(vec![VectorRecord::new("duplicate", vec![1.0, 0.0])])
        .unwrap();
    let second = writer
        .append_async(vec![VectorRecord::new("duplicate", vec![2.0, 0.0])])
        .unwrap();
    first.wait().unwrap();
    second.wait().unwrap();

    let reopened = BorsukIndex::open(&uri).unwrap();
    assert_eq!(
        reopened.get_vector("duplicate").unwrap(),
        Some(vec![2.0, 0.0])
    );
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
        assert!((1..=2).contains(&receipt.committed_records));
        assert!(receipt.requests.total() < 100);
    }
    drop(writer);

    let reopened = BorsukIndex::open(&uri).unwrap();
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

#[test]
fn unchanged_refresh_cost_does_not_scale_with_committed_lane_blocks() {
    let empty_directory = tempfile::tempdir().unwrap();
    let empty_uri = empty_directory.path().to_string_lossy().into_owned();
    drop(BorsukIndex::create(config(&empty_uri)).unwrap());
    let mut empty = BorsukIndex::open(&empty_uri).unwrap();
    let empty_before = empty.request_counts();
    assert!(!empty.refresh().unwrap());
    let empty_refresh = empty.request_counts().delta(&empty_before);

    let tail_directory = tempfile::tempdir().unwrap();
    let tail_uri = tail_directory.path().to_string_lossy().into_owned();
    let writer = GroupCommitWriter::new(
        BorsukIndex::create(config(&tail_uri)).unwrap(),
        GroupCommitConfig::default(),
    )
    .unwrap();
    writer
        .append(vec![VectorRecord::new("tail", vec![1.0, 0.0])])
        .unwrap();
    drop(writer);
    let mut with_tail = BorsukIndex::open(&tail_uri).unwrap();
    let tail_before = with_tail.request_counts();
    with_tail.refresh().unwrap();
    let tail_refresh = with_tail.request_counts().delta(&tail_before);

    assert_eq!(tail_refresh.gets, empty_refresh.gets);
    assert_eq!(tail_refresh.heads, empty_refresh.heads);
}
