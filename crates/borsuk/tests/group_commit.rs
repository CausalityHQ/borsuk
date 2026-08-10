//! Process-local WAL group-commit integration coverage.

#[allow(dead_code)]
mod common;

use std::{
    collections::HashSet,
    io::Cursor,
    sync::{Arc, Barrier},
};

use arrow_ipc::reader::StreamReader;
use arrow_schema::DataType;
use borsuk::{
    BorsukIndex, GROUP_COMMIT_STRIPE_COUNT, GroupCommitConfig, GroupCommitWriter,
    IncrementalMaintenanceOptions, IndexConfig, LeafMode, SearchOptions, SearchTerminationReason,
    VectorMetric, VectorRecord,
};
use bytes::Bytes;
use futures_util::TryStreamExt;
use object_store::{ObjectStore, ObjectStoreExt, memory::InMemory};

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
    assert_eq!(report.hits[0].distance, 0.0);
    assert_eq!(report.leaf_mode, "srht-pq-scan");
    assert!(report.segments_searched > 0);
    assert_eq!(report.global_leaf_directory_reads, 0);
    assert_eq!(report.global_leaf_directory_bytes, 0);
    assert_eq!(report.global_leaf_pages_read, 0);
    assert_eq!(report.global_leaf_page_bytes, 0);
    assert_eq!(report.global_leaf_exact_scores, 0);
}

#[test]
fn drain_encodes_one_record_without_segment_get_or_codebook_put() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (traced, operations) =
        common::FaultInjectingObjectStore::new(Arc::clone(&inner)).with_operation_log();
    let uri = "memory:///group-drain-direct-v11-record";
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

    let delta = VectorRecord::new("delta", vec![10.0; 8]);
    let writer = GroupCommitWriter::new(
        index,
        GroupCommitConfig {
            max_delay: std::time::Duration::ZERO,
            max_records: 1,
            worker_lanes: 1,
        },
    )
    .unwrap();
    writer.append(vec![delta]).unwrap();
    writer.drain().unwrap();

    let new_segment_paths = operations
        .matching_paths(|operation, path| {
            operation == common::StoreOperation::Put && path.starts_with("segments/")
        })
        .into_iter()
        .collect::<HashSet<_>>();
    assert!(!new_segment_paths.is_empty());
    assert_eq!(
        operations.count_matching(|operation, path| {
            operation == common::StoreOperation::Put && path.starts_with("manifests/")
        }),
        1,
        "segments and the level-zero V11 run must publish atomically in one manifest"
    );
    assert_eq!(
        operations.count_matching(|operation, path| {
            operation == common::StoreOperation::Get && new_segment_paths.contains(path)
        }),
        0,
        "direct leaf encoding must consume resident records without rereading new segments"
    );
    assert_eq!(
        operations.count_matching(|operation, path| {
            operation == common::StoreOperation::Put && path.contains("/codebooks/")
        }),
        0,
        "a direct drain must reuse the leaf epoch's resident codebook"
    );
    let reopened = BorsukIndex::open_with_object_store(inner, uri).unwrap();
    assert_eq!(reopened.stats().global_ann_layout_version, Some(11));
    assert_eq!(reopened.stats().global_leaf_runs, 2);
    assert_eq!(reopened.stats().global_leaf_max_level, Some(0));
}

#[test]
fn drain_fails_closed_when_the_incremental_run_manifest_cannot_be_published() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let uri = "memory:///group-drain-global-delta-fail-closed";
    let mut index = BorsukIndex::create_with_object_store(
        Arc::clone(&inner),
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
                .map(|row| VectorRecord::new(format!("base-{row}"), vec![row as f32; 8]))
                .collect(),
        )
        .unwrap();
    index.finish_bulk_load().unwrap();
    let manifest_version_before_drain = index.stats().manifest_version;
    drop(index);

    let faulted = common::FaultInjectingObjectStore::fail_nth_matching(
        Arc::clone(&inner),
        1,
        true,
        |operation, path| {
            operation == common::StoreOperation::Put && path.as_ref().starts_with("manifests/")
        },
    );
    let index = BorsukIndex::open_with_object_store(Arc::new(faulted), uri).unwrap();

    let writer = GroupCommitWriter::new(
        index,
        GroupCommitConfig {
            max_delay: std::time::Duration::ZERO,
            max_records: 32,
            worker_lanes: 1,
        },
    )
    .unwrap();
    writer
        .append(
            (0..32)
                .map(|row| VectorRecord::new(format!("delta-{row}"), vec![1_000.0; 8]))
                .collect(),
        )
        .unwrap();

    let error = writer
        .drain()
        .expect_err("drain must not expose an incremental run without its manifest");
    assert!(
        error.to_string().contains("injected Put failure"),
        "{error}"
    );
    let reopened = BorsukIndex::open_with_object_store(inner, uri).unwrap();
    assert_eq!(
        reopened.stats().manifest_version,
        manifest_version_before_drain
    );
    assert!(reopened.get_vector("delta-0").unwrap().is_some());
}

#[test]
fn cold_search_overlaps_independent_global_base_and_delta_reads() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let uri = "memory:///parallel-global-base-delta-search";
    let mut index = BorsukIndex::create_with_object_store(
        Arc::clone(&inner),
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
                .map(|row| VectorRecord::new(format!("base-{row}"), vec![row as f32 / 128.0; 8]))
                .collect(),
        )
        .unwrap();
    index.finish_bulk_load().unwrap();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let base_paths = runtime
        .block_on(
            inner
                .list(Some(&"global-leaf".into()))
                .try_collect::<Vec<_>>(),
        )
        .unwrap()
        .into_iter()
        .map(|meta| meta.location.to_string())
        .collect::<HashSet<_>>();
    index
        .add(
            (0..32)
                .map(|row| VectorRecord::new(format!("delta-{row}"), vec![10.0 + row as f32; 8]))
                .collect(),
        )
        .unwrap();
    index.flush().unwrap();
    drop(index);

    let all_paths = runtime
        .block_on(
            inner
                .list(Some(&"global-leaf".into()))
                .try_collect::<Vec<_>>(),
        )
        .unwrap()
        .into_iter()
        .map(|meta| meta.location.to_string())
        .collect::<HashSet<_>>();
    let delta_paths = all_paths
        .difference(&base_paths)
        .cloned()
        .collect::<HashSet<_>>();
    assert!(!base_paths.is_empty());
    assert!(!delta_paths.is_empty());

    let delayed = common::FaultInjectingObjectStore::new(inner)
        .with_latency(std::time::Duration::from_millis(25));
    let (delayed, gets) = delayed.with_get_group_concurrency_probe(base_paths, delta_paths);
    let reader = BorsukIndex::open_with_object_store(Arc::new(delayed), uri).unwrap();
    let report = reader
        .search_with_report(
            &[27.0; 8],
            SearchOptions::approx(3, LeafMode::SrhtPqScan)
                .with_max_segments(4)
                .with_max_candidates_per_segment(32),
        )
        .unwrap();

    assert_eq!(report.hits[0].id.as_str(), "delta-17");
    assert_eq!(report.leaf_mode, "bounded-arrow-leaf-v11");
    assert!(report.global_leaf_directory_reads >= 2);
    assert!(report.global_leaf_directory_bytes > 0);
    assert!(report.global_leaf_pages_read >= 2);
    assert!(report.global_leaf_page_bytes > 0);
    assert!(report.global_leaf_exact_scores > 0);
    assert_eq!(
        report.bytes_read,
        report.global_leaf_directory_bytes + report.global_leaf_page_bytes
    );
    assert!(
        report.global_base_approximate_us > 0,
        "cold base+delta search must report base routing/code-scan work: {report:?}"
    );
    assert!(
        report.global_base_exact_rerank_us > 0,
        "cold base+delta search must report base exact-rerank work: {report:?}"
    );
    assert!(
        gets.overlapped(),
        "cold base and immutable-delta reads remained serial: {report:?}"
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
fn lane_log_ack_publishes_extent_and_stripe_head_without_global_coordination() {
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
    assert_eq!(receipt.requests.puts, 2);
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
fn lane_log_ack_persists_a_stock_readable_arrow_mutation_extent() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let uri = "memory:///group-standard-arrow-extent";
    let writer = GroupCommitWriter::new(
        BorsukIndex::create_with_object_store(Arc::clone(&inner), config(uri)).unwrap(),
        GroupCommitConfig {
            max_delay: std::time::Duration::ZERO,
            max_records: 2,
            worker_lanes: 1,
        },
    )
    .unwrap();

    let receipt = writer
        .append(vec![
            VectorRecord::new("first", vec![1.0, 2.0]),
            VectorRecord::new("second", vec![3.0, 4.0]),
        ])
        .unwrap();
    assert_eq!(receipt.requests.puts, 2);
    assert_eq!(receipt.requests.gets, 0);
    assert_eq!(receipt.requests.heads, 0);
    assert_eq!(receipt.requests.lists, 0);

    let runtime = tokio::runtime::Runtime::new().unwrap();
    let extent = runtime
        .block_on(
            inner
                .list(Some(&"lane-log/lanes".into()))
                .try_collect::<Vec<_>>(),
        )
        .unwrap()
        .into_iter()
        .find(|object| {
            object.location.as_ref().contains("/extents/")
                && object.location.as_ref().ends_with(".arrow")
        })
        .expect("group commit must persist one standard Arrow extent");
    let bytes = runtime
        .block_on(async { inner.get(&extent.location).await?.bytes().await })
        .unwrap();
    let lane_receipt = &receipt.lane_receipts[0];
    assert_eq!(
        blake3::hash(&bytes).as_bytes(),
        &lane_receipt.extent_checksum,
        "receipt must authenticate the exact immutable extent"
    );
    let head_path = format!("lane-log/lanes/{:04}/HEAD", lane_receipt.commit_lane);
    let head_bytes = runtime
        .block_on(async { inner.get(&head_path.into()).await?.bytes().await })
        .unwrap();
    assert_eq!(
        blake3::hash(&head_bytes).as_bytes(),
        &lane_receipt.published_head_checksum,
        "receipt must authenticate the exact published stripe head"
    );
    let mut reader = StreamReader::try_new(Cursor::new(bytes), None).unwrap();
    let schema = reader.schema();

    assert_eq!(
        schema
            .metadata()
            .get("borsuk.object_role")
            .map(String::as_str),
        Some("mutation_extent")
    );
    assert_eq!(
        schema.field_with_name("mutation_hlc").unwrap().data_type(),
        &DataType::UInt64
    );
    assert_eq!(
        schema.field_with_name("id_state").unwrap().data_type(),
        &DataType::Utf8
    );
    assert_eq!(
        schema
            .field_with_name("mutation_writer")
            .unwrap()
            .data_type(),
        &DataType::FixedSizeBinary(16)
    );
    assert_eq!(
        schema
            .field_with_name("mutation_digest")
            .unwrap()
            .data_type(),
        &DataType::FixedSizeBinary(32)
    );
    assert_eq!(reader.next().unwrap().unwrap().num_rows(), 2);
    assert!(reader.next().is_none());
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
fn independent_group_writers_can_share_one_collection() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let uri = "memory:///independent-group-writers";
    let first_index =
        BorsukIndex::create_with_object_store(Arc::clone(&inner), config(uri)).unwrap();
    let writer_config = GroupCommitConfig {
        max_delay: std::time::Duration::ZERO,
        max_records: 1,
        worker_lanes: 1,
    };
    let first = GroupCommitWriter::new(first_index, writer_config).unwrap();
    let second = GroupCommitWriter::new(
        BorsukIndex::open_with_object_store(Arc::clone(&inner), uri).unwrap(),
        writer_config,
    )
    .unwrap();
    let ids = ids_in_ownership_lane(0, 2);
    let barrier = Arc::new(Barrier::new(2));
    let handles = [
        (first, ids[0].clone(), vec![1.0, 0.0]),
        (second, ids[1].clone(), vec![2.0, 0.0]),
    ]
    .into_iter()
    .map(|(writer, id, vector)| {
        let barrier = Arc::clone(&barrier);
        std::thread::spawn(move || {
            barrier.wait();
            writer.append(vec![VectorRecord::new(id, vector)]).unwrap();
        })
    })
    .collect::<Vec<_>>();
    for handle in handles {
        handle.join().unwrap();
    }

    let reopened = BorsukIndex::open_with_object_store(inner, uri).unwrap();
    assert_eq!(reopened.get_vector(&ids[0]).unwrap(), Some(vec![1.0, 0.0]));
    assert_eq!(reopened.get_vector(&ids[1]).unwrap(), Some(vec![2.0, 0.0]));
}

#[test]
fn thirty_two_independent_group_writers_share_one_collection() {
    const WRITERS: usize = 32;
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let uri = "memory:///thirty-two-independent-group-writers";
    let writer_config = GroupCommitConfig {
        max_delay: std::time::Duration::ZERO,
        max_records: 1,
        worker_lanes: 1,
    };
    let first_index =
        BorsukIndex::create_with_object_store(Arc::clone(&inner), config(uri)).unwrap();
    let mut writers = vec![GroupCommitWriter::new(first_index, writer_config).unwrap()];
    for _ in 1..WRITERS {
        writers.push(
            GroupCommitWriter::new(
                BorsukIndex::open_with_object_store(Arc::clone(&inner), uri).unwrap(),
                writer_config,
            )
            .unwrap(),
        );
    }

    for (ordinal, writer) in writers.iter().enumerate() {
        writer
            .append(vec![VectorRecord::new(
                format!("writer-{ordinal:02}"),
                vec![ordinal as f32, 0.0],
            )])
            .unwrap();
    }

    let reopened = BorsukIndex::open_with_object_store(inner, uri).unwrap();
    assert_eq!(reopened.list_records(0, WRITERS).unwrap().len(), WRITERS);
}

#[test]
fn thirty_two_writer_startup_reads_one_candidate_head_per_instance() {
    const WRITERS: usize = 32;
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (traced, operations) = common::FaultInjectingObjectStore::new(inner).with_operation_log();
    let store: Arc<dyn ObjectStore> = Arc::new(traced);
    let uri = "memory:///thirty-two-writer-startup-cost";
    let mut indexes =
        vec![BorsukIndex::create_with_object_store(Arc::clone(&store), config(uri)).unwrap()];
    for _ in 1..WRITERS {
        indexes.push(BorsukIndex::open_with_object_store(Arc::clone(&store), uri).unwrap());
    }
    operations.clear();

    let writer_config = GroupCommitConfig {
        max_delay: std::time::Duration::ZERO,
        max_records: 1,
        worker_lanes: 1,
    };
    let writers = indexes
        .into_iter()
        .map(|index| GroupCommitWriter::new(index, writer_config).unwrap())
        .collect::<Vec<_>>();

    assert_eq!(writers.len(), WRITERS);
    assert_eq!(
        operations.count_matching(|operation, path| {
            operation == common::StoreOperation::Get
                && path.starts_with("lane-log/lanes/")
                && path.ends_with("/HEAD")
        }),
        WRITERS,
        "fresh independent writers must consult one inactive stripe HEAD each"
    );
}

#[test]
fn one_writer_can_drain_while_another_writer_remains_live() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let uri = "memory:///independent-group-writer-drain";
    let writer_config = GroupCommitConfig {
        max_delay: std::time::Duration::ZERO,
        max_records: 1,
        worker_lanes: 1,
    };
    let first = GroupCommitWriter::new(
        BorsukIndex::create_with_object_store(Arc::clone(&inner), config(uri)).unwrap(),
        writer_config,
    )
    .unwrap();
    let second = GroupCommitWriter::new(
        BorsukIndex::open_with_object_store(Arc::clone(&inner), uri).unwrap(),
        writer_config,
    )
    .unwrap();

    first
        .append(vec![VectorRecord::new("shared", vec![1.0, 0.0])])
        .unwrap();
    second
        .append(vec![VectorRecord::new("second-only", vec![2.0, 0.0])])
        .unwrap();
    first.drain().unwrap();
    second
        .append(vec![VectorRecord::new("shared", vec![3.0, 0.0])])
        .unwrap();

    let reopened = BorsukIndex::open_with_object_store(inner, uri).unwrap();
    assert_eq!(reopened.get_vector("shared").unwrap(), Some(vec![3.0, 0.0]));
    assert_eq!(
        reopened.get_vector("second-only").unwrap(),
        Some(vec![2.0, 0.0])
    );
}

#[test]
fn drain_retires_owned_stripe_without_hiding_it_from_a_stale_reader() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (traced, operations) =
        common::FaultInjectingObjectStore::new(Arc::clone(&inner)).with_operation_log();
    let store: Arc<dyn ObjectStore> = Arc::new(traced);
    let uri = "memory:///drain-retired-stripe-manifest-fence";
    let index = BorsukIndex::create_with_object_store(Arc::clone(&store), config(uri)).unwrap();
    let mut stale = BorsukIndex::open_with_object_store(Arc::clone(&store), uri).unwrap();
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
        .append(vec![VectorRecord::new("retired", vec![1.0, 0.0])])
        .unwrap();
    writer.drain().unwrap();

    assert!(stale.refresh_wal_tail().unwrap());
    assert_eq!(stale.get_vector("retired").unwrap(), Some(vec![1.0, 0.0]));

    operations.clear();
    let current = BorsukIndex::open_with_object_store(store, uri).unwrap();
    assert_eq!(current.get_vector("retired").unwrap(), Some(vec![1.0, 0.0]));
    assert_eq!(
        operations.count_matching(|operation, path| {
            operation == common::StoreOperation::Get
                && path.starts_with("lane-log/lanes/")
                && path.ends_with("/HEAD")
        }),
        0,
        "a reader at the materializing manifest must omit the retired stripe HEAD"
    );
}

#[test]
fn append_after_drain_reactivates_the_retired_stripe_before_acknowledgement() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let uri = "memory:///append-after-retired-stripe";
    let writer = GroupCommitWriter::new(
        BorsukIndex::create_with_object_store(Arc::clone(&inner), config(uri)).unwrap(),
        GroupCommitConfig {
            max_delay: std::time::Duration::ZERO,
            max_records: 1,
            worker_lanes: 1,
        },
    )
    .unwrap();
    writer
        .append(vec![VectorRecord::new("before-drain", vec![1.0, 0.0])])
        .unwrap();
    writer.drain().unwrap();

    writer
        .append(vec![VectorRecord::new("after-drain", vec![2.0, 0.0])])
        .unwrap();

    let reopened = BorsukIndex::open_with_object_store(inner, uri).unwrap();
    assert_eq!(
        reopened.get_vector("after-drain").unwrap(),
        Some(vec![2.0, 0.0]),
        "an acknowledgement after retirement requires prior directory reactivation"
    );
}

#[test]
fn every_independent_group_writer_can_drain_after_a_peer_publishes() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let uri = "memory:///independent-group-writer-sequential-drains";
    let writer_config = GroupCommitConfig {
        max_delay: std::time::Duration::ZERO,
        max_records: 1,
        worker_lanes: 1,
    };
    let first = GroupCommitWriter::new(
        BorsukIndex::create_with_object_store(Arc::clone(&inner), config(uri)).unwrap(),
        writer_config,
    )
    .unwrap();
    let second = GroupCommitWriter::new(
        BorsukIndex::open_with_object_store(Arc::clone(&inner), uri).unwrap(),
        writer_config,
    )
    .unwrap();

    first
        .append(vec![VectorRecord::new("first", vec![1.0, 0.0])])
        .unwrap();
    second
        .append(vec![VectorRecord::new("second", vec![2.0, 0.0])])
        .unwrap();
    first.drain().unwrap();
    second.drain().unwrap();

    let reopened = BorsukIndex::open_with_object_store(inner, uri).unwrap();
    assert_eq!(reopened.list_records(0, 10).unwrap().len(), 2);
}

#[test]
fn released_peer_retires_a_tail_materialized_by_another_writer() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (traced, operations) =
        common::FaultInjectingObjectStore::new(Arc::clone(&inner)).with_operation_log();
    let store: Arc<dyn ObjectStore> = Arc::new(traced);
    let uri = "memory:///released-peer-retirement";
    let writer_config = GroupCommitConfig {
        max_delay: std::time::Duration::ZERO,
        max_records: 1,
        worker_lanes: 1,
    };
    let first = GroupCommitWriter::new(
        BorsukIndex::create_with_object_store(Arc::clone(&store), config(uri)).unwrap(),
        writer_config,
    )
    .unwrap();
    let second = GroupCommitWriter::new(
        BorsukIndex::open_with_object_store(Arc::clone(&store), uri).unwrap(),
        writer_config,
    )
    .unwrap();
    let first_lane = first
        .append(vec![VectorRecord::new("first", vec![1.0, 0.0])])
        .unwrap()
        .commit_lane;
    let second_lane = second
        .append(vec![VectorRecord::new("second", vec![2.0, 0.0])])
        .unwrap()
        .commit_lane;
    first.drain().unwrap();
    drop(second);

    operations.clear();
    let reopened = BorsukIndex::open_with_object_store(store, uri).unwrap();
    assert_eq!(reopened.list_records(0, 10).unwrap().len(), 2);
    let lane_head_reads = operations.matching_paths(|operation, path| {
        operation == common::StoreOperation::Get
            && path.starts_with("lane-log/lanes/")
            && path.ends_with("/HEAD")
    });
    assert_eq!(
        lane_head_reads.len(),
        0,
        "normal release must retire a peer stripe already covered by the published manifest; first={first_lane} second={second_lane} reads={lane_head_reads:?}"
    );
}

#[test]
fn group_writer_startup_fails_when_every_persisted_stripe_is_leased() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let uri = "memory:///group-writer-stripe-exhaustion";
    let first = GroupCommitWriter::new(
        BorsukIndex::create_with_object_store(Arc::clone(&inner), config(uri)).unwrap(),
        GroupCommitConfig {
            max_delay: std::time::Duration::ZERO,
            max_records: 1,
            worker_lanes: 64,
        },
    )
    .unwrap();
    let result = GroupCommitWriter::new(
        BorsukIndex::open_with_object_store(inner, uri).unwrap(),
        GroupCommitConfig {
            max_delay: std::time::Duration::ZERO,
            max_records: 1,
            worker_lanes: 1,
        },
    );
    let error = match result {
        Ok(_) => panic!("a sixty-fifth live worker stripe must not steal an active lease"),
        Err(error) => error,
    };
    assert!(
        error
            .to_string()
            .contains("persisted stripes are available")
    );
    drop(first);
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
            .collect::<std::collections::BTreeSet<_>>()
            .len(),
        2,
        "records assigned to distinct local workers must use distinct claimed stripes"
    );
}

#[test]
fn one_worker_coalesces_cross_ownership_records_into_one_stripe_extent() {
    const LANES: usize = 4;
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (instrumented, concurrency) = common::FaultInjectingObjectStore::new(inner)
        .with_latency(std::time::Duration::from_millis(25))
        .with_put_concurrency_probe();
    let uri = "memory:///group-parallel-epoch-extents";
    let writer = GroupCommitWriter::new(
        BorsukIndex::create_with_object_store(Arc::new(instrumented), config(uri)).unwrap(),
        GroupCommitConfig {
            max_delay: std::time::Duration::from_millis(10),
            max_records: LANES,
            worker_lanes: 1,
        },
    )
    .unwrap();
    let records = (0..LANES)
        .map(|lane| {
            VectorRecord::new(
                ids_in_ownership_lane(lane, 1).pop().unwrap(),
                vec![lane as f32, 0.0],
            )
        })
        .collect();

    let receipt = writer.append(records).unwrap();

    assert_eq!(receipt.lane_receipts.len(), 1);
    assert_eq!(receipt.requests.puts, 2);
    assert_eq!(
        concurrency.peak(),
        1,
        "one local worker stripe must persist one grouped extent"
    );
}

#[test]
fn independent_commit_lanes_report_lane_local_requests() {
    const LANES: usize = 4;
    const RECORDS: usize = 64;
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (traced, operations) = common::FaultInjectingObjectStore::new(inner).with_operation_log();
    let uri = "memory:///group-lane-local-request-counts";
    let writer = GroupCommitWriter::new(
        BorsukIndex::create_with_object_store(Arc::new(traced), config(uri)).unwrap(),
        GroupCommitConfig {
            max_delay: std::time::Duration::ZERO,
            max_records: 1,
            worker_lanes: LANES,
        },
    )
    .unwrap();
    operations.clear();
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
    assert_eq!(
        receipts
            .iter()
            .map(|receipt| receipt.commit_lane)
            .collect::<std::collections::BTreeSet<_>>()
            .len(),
        LANES
    );
    assert!(receipts.iter().all(|receipt| {
        receipt.requests.gets == 0
            && receipt.requests.puts == 2
            && receipt.requests.deletes == 0
            && receipt.requests.heads == 0
            && receipt.requests.lists == 0
    }));
    assert_eq!(
        receipts
            .iter()
            .map(|receipt| receipt.requests.gets)
            .sum::<u64>(),
        operations.count_matching(|operation, _| operation == common::StoreOperation::Get) as u64,
        "steady-state stripes must not read a global coordination object"
    );
    assert_eq!(
        receipts
            .iter()
            .map(|receipt| receipt.requests.puts)
            .sum::<u64>(),
        operations.count_matching(|operation, _| operation == common::StoreOperation::Put) as u64,
        "one extent PUT plus one stripe-head PUT must reconcile exactly"
    );
}

#[test]
fn repeated_groups_publish_a_fenced_head_after_the_extent() {
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

    assert!(receipts.iter().all(|receipt| receipt.requests.puts == 2));
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
    assert_eq!(
        operations.count_matching(|operation, _| operation == common::StoreOperation::Put),
        GROUPS * 2,
        "each group must create one extent and publish one stripe head"
    );
    assert_eq!(
        operations.count_matching(|_, path| { path.contains("id-directory/last-write-wins/NEXT") }),
        0,
        "ordinary group commit must never coordinate through a collection-wide counter"
    );
    assert_eq!(
        operations.count_matching(|operation, path| {
            operation == common::StoreOperation::Put
                && path.contains("/extents/")
                && path.ends_with(".arrow")
        }),
        GROUPS,
        "every acknowledgement PUT must create exactly one immutable extent"
    );
    assert_eq!(
        operations.count_matching(|operation, path| {
            operation == common::StoreOperation::Put
                && path.starts_with("lane-log/lanes/")
                && path.ends_with("/HEAD")
        }),
        GROUPS,
        "every acknowledgement must publish exactly one writer-stripe head"
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
fn small_groups_publish_only_immutable_extents_before_release() {
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
        assert_eq!(
            receipt.requests.puts, 2,
            "immutable extent plus fenced stripe-head publication"
        );
    }

    assert_eq!(
        operations.count_matching(|operation, path| {
            operation == common::StoreOperation::Put && path.contains("/blocks/")
        }),
        0,
        "v29 must never publish legacy mutable blocks"
    );
    assert_eq!(
        operations.count_matching(|operation, path| {
            operation == common::StoreOperation::Put
                && path.contains("/extents/")
                && path.ends_with(".arrow")
        }),
        5,
        "each small group must create exactly one immutable extent"
    );
    assert_eq!(
        operations.count_matching(|operation, path| {
            operation == common::StoreOperation::Put && path.ends_with("/HEAD")
        }),
        5,
        "every extent must be discoverable through a conditionally published stripe head before acknowledgement"
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
            receipt.commit_lane < usize::from(GROUP_COMMIT_STRIPE_COUNT),
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
fn live_lane_writer_does_not_recreate_a_disappeared_head_and_reopen_fails_closed() {
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

    assert!(
        writer
            .append(vec![VectorRecord::new("before-loss", vec![2.0, 0.0])])
            .is_err(),
        "an append cannot be acknowledged without its stripe-head publication fence"
    );
    assert!(
        !directory
            .path()
            .join(format!("lane-log/lanes/{:04}/HEAD", receipt.commit_lane))
            .exists()
    );
    drop(writer);
    assert!(
        BorsukIndex::open(&uri).is_err(),
        "a missing lease authority must fail closed during reopen"
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
    let empty_writer = GroupCommitWriter::new(
        BorsukIndex::create(config(&empty_uri)).unwrap(),
        GroupCommitConfig::default(),
    )
    .unwrap();
    drop(empty_writer);
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

#[test]
fn wal_tail_refresh_observes_acknowledged_records_without_manifest_reload() {
    let directory = tempfile::tempdir().unwrap();
    let uri = directory.path().to_string_lossy().into_owned();
    let index = BorsukIndex::create(config(&uri)).unwrap();
    let mut reader = BorsukIndex::open(&uri).unwrap();
    let writer = GroupCommitWriter::new(index, GroupCommitConfig::default()).unwrap();
    writer
        .append(vec![VectorRecord::new("tail-fast", vec![1.0, 0.0])])
        .unwrap();
    drop(writer);

    assert!(reader.refresh_wal_tail().unwrap());
    assert_eq!(
        reader.get_vector("tail-fast").unwrap(),
        Some(vec![1.0, 0.0])
    );
}

#[test]
fn future_segment_size_can_change_without_rebuilding_logical_cell_topology() {
    let directory = tempfile::tempdir().unwrap();
    let uri = directory.path().to_string_lossy().into_owned();
    let mut index = BorsukIndex::create(IndexConfig {
        segment_max_vectors: 1,
        ..config(&uri)
    })
    .unwrap();
    index
        .add(vec![
            VectorRecord::new("left", vec![1.0, 0.0]),
            VectorRecord::new("right", vec![0.0, 1.0]),
        ])
        .unwrap();
    index.finish_bulk_load().unwrap();
    let logical_cells = index.manifest().logical_cells().to_vec();

    index.set_segment_max_vectors(128).unwrap();
    drop(index);
    let reopened = BorsukIndex::open(&uri).unwrap();

    assert_eq!(reopened.manifest().logical_cells(), logical_cells);
    assert_eq!(reopened.manifest().segment_max_vectors(), 128);
}

#[test]
fn resident_v11_does_not_schedule_a_continuation_after_its_deadline() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let uri = "memory:///resident-v11-continuation-deadline";
    let suffix = "x".repeat(100 * 1024);
    let ids = (0..64)
        .map(|row| format!("row-{row:02}-{suffix}"))
        .collect::<Vec<_>>();
    let mut index = BorsukIndex::create_with_object_store(
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
    index
        .add(
            ids.iter()
                .enumerate()
                .map(|(row, id)| VectorRecord::new(id.clone(), vec![row as f32; 8]))
                .collect(),
        )
        .unwrap();
    index.finish_bulk_load().unwrap();
    index.delete([ids[0].as_str()]).unwrap();
    drop(index);

    let delayed = common::FaultInjectingObjectStore::new(Arc::clone(&inner)).with_get_latency_for(
        std::time::Duration::from_millis(40),
        |operation, path| {
            operation == common::StoreOperation::Get
                && (path.as_ref().contains("global-leaf/directories/")
                    || path.as_ref().contains("global-leaf/bundles/"))
        },
    );
    let (delayed, operations) = delayed.with_operation_log();
    let reader = BorsukIndex::open_with_object_store(Arc::new(delayed), uri).unwrap();
    operations.clear();

    let report = reader
        .search_with_report(
            &[0.0; 8],
            SearchOptions::approx(1, LeafMode::SrhtPqScan)
                .with_max_segments(4)
                .with_max_latency_ms(60),
        )
        .unwrap();

    assert!(report.hits.is_empty());
    assert_eq!(
        report.termination_reason,
        SearchTerminationReason::MaxLatency
    );
    assert_eq!(report.global_leaf_waves, 1);
    assert_eq!(report.global_leaf_continuations, 0);
    assert_eq!(report.global_leaf_pages_read, 1);
    let bundle_gets = operations.count_matching(|operation, path| {
        operation == common::StoreOperation::Get && path.contains("global-leaf/bundles/")
    });
    assert_eq!(bundle_gets, 1, "deadline scheduled an extra page wave");
}

#[test]
fn refresh_rejects_an_invalid_next_v11_before_publishing_it() {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (traced, operations) =
        common::FaultInjectingObjectStore::new(Arc::clone(&inner)).with_operation_log();
    let store: Arc<dyn ObjectStore> = Arc::new(traced);
    let uri = "memory:///refresh-invalid-next-v11";
    let mut writer = BorsukIndex::create_with_object_store(
        Arc::clone(&store),
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
    writer
        .add(
            (0..128)
                .map(|row| VectorRecord::new(format!("base-{row}"), vec![row as f32; 8]))
                .collect(),
        )
        .unwrap();
    writer.finish_bulk_load().unwrap();
    let mut reader = BorsukIndex::open_with_object_store(Arc::clone(&store), uri).unwrap();
    let old_version = reader.manifest().version;
    operations.clear();

    writer
        .add(
            (0..32)
                .map(|row| VectorRecord::new(format!("delta-{row}"), vec![256.0 + row as f32; 8]))
                .collect(),
        )
        .unwrap();
    writer.flush().unwrap();
    let next_descriptor = operations
        .matching_paths(|operation, path| {
            operation == common::StoreOperation::Put && path.contains("global-leaf/descriptors/")
        })
        .pop()
        .expect("online publication writes a V11 descriptor");
    runtime
        .block_on(inner.put(
            &next_descriptor.into(),
            Bytes::from_static(b"corrupt").into(),
        ))
        .unwrap();

    assert!(reader.refresh().is_err());
    assert_eq!(reader.manifest().version, old_version);
    let report = reader
        .search_with_report(
            &[0.0; 8],
            SearchOptions::approx(1, LeafMode::SrhtPqScan).with_max_segments(4),
        )
        .unwrap();
    assert_eq!(report.hits[0].id.as_str(), "base-0");
    assert_eq!(report.leaf_mode, "bounded-arrow-leaf-v11");
}

#[test]
fn refresh_preloads_v11_once_before_concurrent_queries_and_preserves_old_snapshot() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (traced, operations) =
        common::FaultInjectingObjectStore::new(Arc::clone(&inner)).with_operation_log();
    let store: Arc<dyn ObjectStore> = Arc::new(traced);
    let uri = "memory:///refresh-preloads-next-v11";
    let mut writer = BorsukIndex::create_with_object_store(
        Arc::clone(&store),
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
    writer
        .add(
            (0..128)
                .map(|row| VectorRecord::new(format!("base-{row}"), vec![row as f32; 8]))
                .collect(),
        )
        .unwrap();
    writer.finish_bulk_load().unwrap();
    let mut reader = BorsukIndex::open_with_object_store(Arc::clone(&store), uri).unwrap();
    let old_snapshot = reader.clone();
    writer
        .add(
            (0..32)
                .map(|row| VectorRecord::new(format!("delta-{row}"), vec![256.0 + row as f32; 8]))
                .collect(),
        )
        .unwrap();
    writer.flush().unwrap();
    operations.clear();

    assert!(reader.refresh().unwrap());
    let setup_gets = operations.count_matching(|operation, path| {
        operation == common::StoreOperation::Get
            && (path.contains("global-leaf/descriptors/") || path.contains("global-leaf/roots/"))
    });
    assert_eq!(
        setup_gets, 4,
        "refresh did not preload one descriptor and three roots"
    );
    operations.clear();

    std::thread::scope(|scope| {
        for _ in 0..2 {
            let query_reader = reader.clone();
            scope.spawn(move || {
                let report = query_reader
                    .search_with_report(
                        &[270.0; 8],
                        SearchOptions::approx(1, LeafMode::SrhtPqScan).with_max_segments(4),
                    )
                    .unwrap();
                assert_eq!(report.leaf_mode, "bounded-arrow-leaf-v11");
            });
        }
    });
    assert_eq!(
        operations.count_matching(|operation, path| {
            operation == common::StoreOperation::Get
                && (path.contains("global-leaf/descriptors/")
                    || path.contains("global-leaf/roots/"))
        }),
        0,
        "first queries repeated descriptor/root setup I/O after refresh"
    );
    let old_report = old_snapshot
        .search_with_report(
            &[0.0; 8],
            SearchOptions::approx(1, LeafMode::SrhtPqScan).with_max_segments(4),
        )
        .unwrap();
    assert_eq!(old_report.hits[0].id.as_str(), "base-0");
}

#[test]
fn maintenance_setup_read_failure_does_not_publish_or_advance_the_handle() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let uri = "memory:///maintenance-prepare-resident-pins-failure";
    let mut writer = BorsukIndex::create_with_object_store(
        Arc::clone(&inner),
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
    writer
        .add(
            (0..128)
                .map(|row| VectorRecord::new(format!("row-{row}"), vec![row as f32; 8]))
                .collect(),
        )
        .unwrap();
    writer.finish_bulk_load().unwrap();
    writer
        .add(
            (0..64)
                .map(|row| VectorRecord::new(format!("delta-{row}"), vec![1_000.0 + row as f32; 8]))
                .collect(),
        )
        .unwrap();
    writer.flush().unwrap();
    writer
        .delete(
            (0..64)
                .filter(|row| row % 8 != 0)
                .map(|row| format!("delta-{row}"))
                .collect::<Vec<_>>(),
        )
        .unwrap();
    writer.flush().unwrap();
    let old_version = writer.manifest().version;
    drop(writer);

    let faulted = common::FaultInjectingObjectStore::fail_nth_matching(
        Arc::clone(&inner),
        3,
        true,
        |operation, path| {
            operation == common::StoreOperation::Get
                && path.as_ref().contains("global-leaf/descriptors/")
        },
    );
    let mut maintainer = BorsukIndex::open_with_object_store(Arc::new(faulted), uri).unwrap();

    let error = maintainer
        .run_incremental_maintenance(IncrementalMaintenanceOptions {
            max_segment_vectors: usize::MAX,
            max_segment_radius: None,
            min_segment_vectors: 15,
            max_operations: 1,
        })
        .unwrap_err();
    assert!(error.to_string().contains("injected Get failure"));
    assert_eq!(
        maintainer.manifest().version,
        old_version,
        "failed setup advanced the publishing handle"
    );
    let current = BorsukIndex::open_with_object_store(inner, uri).unwrap();
    assert_eq!(
        current.manifest().version,
        old_version,
        "failed setup advanced CURRENT"
    );
}

#[test]
fn successful_purge_installs_prepared_pins_without_post_publish_setup_gets() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (traced, operations) =
        common::FaultInjectingObjectStore::new(Arc::clone(&inner)).with_operation_log();
    let uri = "memory:///purge-installs-prepared-resident-pins";
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
                .map(|row| VectorRecord::new(format!("row-{row}"), vec![row as f32; 8]))
                .collect(),
        )
        .unwrap();
    index.finish_bulk_load().unwrap();
    index.delete(["row-0"]).unwrap();

    operations.clear();
    index.purge_with_report().unwrap();
    assert_eq!(
        operations.count_matching(|operation, path| {
            operation == common::StoreOperation::Get
                && (path.contains("global-leaf/descriptors/")
                    || path.contains("global-leaf/roots/"))
        }),
        4,
        "purge must validate exactly one descriptor and three roots before publication"
    );
    operations.clear();
    let report = index
        .search_with_report(
            &[1.0; 8],
            SearchOptions::approx(1, LeafMode::SrhtPqScan).with_max_segments(4),
        )
        .unwrap();
    assert_eq!(report.leaf_mode, "bounded-arrow-leaf-v11");
    assert_eq!(
        operations.count_matching(|operation, path| {
            operation == common::StoreOperation::Get
                && (path.contains("global-leaf/descriptors/")
                    || path.contains("global-leaf/roots/"))
        }),
        0,
        "first post-purge query repeated setup reads"
    );
}
