//! Public positioned group-commit equivalence coverage.

mod common;

use std::{collections::BTreeMap, sync::Arc, thread, time::Duration};

use borsuk::{
    BorsukError, BorsukIndex, GroupCommitConfig, GroupCommitWriter, IndexConfig, ObjectStore,
    PositionedLogWriter, PositionedMutationModality, RequestCounts, SearchOptions,
    VectorElementType, VectorKind, VectorMetric, VectorRecord, VectorSpec,
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

fn writer(index: BorsukIndex, workers: usize) -> GroupCommitWriter {
    GroupCommitWriter::new(
        index,
        GroupCommitConfig {
            max_delay: Duration::from_millis(2),
            max_records: 1_024,
            workers,
        },
    )
    .unwrap()
}

fn logged_request_counts(operations: &common::OperationLog) -> RequestCounts {
    let mut counts = RequestCounts::default();
    for entry in operations.entries() {
        match entry.operation {
            common::StoreOperation::Put | common::StoreOperation::MultipartPut => counts.puts += 1,
            common::StoreOperation::Get => counts.gets += 1,
            common::StoreOperation::Head => counts.heads += 1,
            common::StoreOperation::Delete => counts.deletes += 1,
            common::StoreOperation::List => counts.lists += 1,
            common::StoreOperation::Copy | common::StoreOperation::Rename => {
                panic!("mutation telemetry has no slot for {:?}", entry.operation)
            }
        }
    }
    counts
}

fn logged_put_payload_bytes(operations: &common::OperationLog) -> u64 {
    operations
        .entries()
        .into_iter()
        .filter_map(|entry| entry.payload_bytes)
        .sum()
}

fn sum_request_counts(left: RequestCounts, right: RequestCounts) -> RequestCounts {
    RequestCounts {
        gets: left.gets + right.gets,
        puts: left.puts + right.puts,
        deletes: left.deletes + right.deletes,
        heads: left.heads + right.heads,
        lists: left.lists + right.lists,
    }
}

fn assert_no_legacy_mutation_writes(operations: &common::OperationLog) {
    assert_eq!(
        operations.count_matching(|operation, path| {
            matches!(
                operation,
                common::StoreOperation::Put | common::StoreOperation::MultipartPut
            ) && (path.starts_with("lane-log/")
                || path.starts_with("cell-wal/")
                || path.starts_with("transactions/")
                || (path.starts_with("cells/") && path.contains("/wal/"))
                || path.starts_with("tombstones/")
                || path.starts_with("bm25/")
                || path.starts_with("lexical/stats-delta/")
                || path == "id-directory/generated/NEXT")
        }),
        0,
        "public V12 mutation facades must not write legacy durability objects"
    );
}

#[test]
fn positioned_flush_never_reads_or_rewrites_legacy_cell_wal_lanes() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (traced, operations) = common::FaultInjectingObjectStore::new(inner).with_operation_log();
    let uri = "memory:///positioned-flush-skips-legacy-cell-wal";
    let mut index_config = config(uri);
    index_config.named_vectors.insert(
        "image".to_string(),
        VectorSpec {
            dimensions: 2,
            metric: VectorMetric::Euclidean,
            kind: VectorKind::Dense,
            element_type: VectorElementType::Float32,
        },
    );
    let mut index = BorsukIndex::create_with_object_store(Arc::new(traced), index_config).unwrap();
    index
        .add(vec![
            VectorRecord::new("row", vec![1.0, 0.0]).with_named_vector("image", vec![0.0, 1.0]),
        ])
        .unwrap();
    operations.clear();

    index.flush().unwrap();

    assert_eq!(
        operations.count_matching(|_, path| {
            path.starts_with("cell-wal/")
                || (path.starts_with("cells/") && path.contains("/wal/"))
                || path.starts_with("collection/wal")
                || path.starts_with("collection/write-epochs/")
        }),
        0,
        "the positioned flush path must not consult retired Cell-WAL lanes: {:?}",
        operations.entries()
    );
}

#[test]
fn generated_add_reconciles_an_accepted_positioned_head_error_once() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let uri = "memory:///positioned-generated-ambiguous-head";
    BorsukIndex::create_with_object_store(Arc::clone(&inner), config(uri)).unwrap();
    let faulted = common::FaultInjectingObjectStore::accept_then_fail_nth_put(
        Arc::clone(&inner),
        1,
        |operation, path| {
            operation == common::StoreOperation::Put
                && path.as_ref().starts_with("positioned-log/heads/")
        },
    );
    let (faulted, operations) = faulted.with_operation_log();
    let mut index = BorsukIndex::open_with_object_store(Arc::new(faulted), uri).unwrap();
    operations.clear();

    let (ids, report) = index
        .add_vectors_with_report(vec![vec![1.0, 0.0], vec![0.0, 1.0]])
        .unwrap();

    assert_eq!(ids.len(), 2);
    assert_ne!(ids[0], ids[1]);
    let position = report
        .positioned_position
        .expect("reconciled append must return its current source position");
    assert_eq!(report.positioned_envelope_checksum.len(), 64);
    assert!(report.positioned_encoded_bytes > 0);
    assert_eq!(report.requests, logged_request_counts(&operations));
    assert_eq!(
        report.total_bytes_written,
        logged_put_payload_bytes(&operations),
        "an accepted ambiguous head attempt must retain its submitted bytes"
    );
    assert_eq!(
        report.bytes_per_vector,
        report.total_bytes_written as f64 / 2.0
    );
    assert_eq!(
        operations.count_matching(|operation, path| {
            operation == common::StoreOperation::Put && path.starts_with("positioned-log/heads/")
        }),
        1,
        "an accepted ambiguous head PUT must reconcile instead of publishing again"
    );
    assert_no_legacy_mutation_writes(&operations);
    drop(index);

    let reopened = BorsukIndex::open_with_object_store(Arc::clone(&inner), uri).unwrap();
    assert_eq!(reopened.list_records(0, 10).unwrap().len(), 2);
    assert_eq!(reopened.get_vector(&ids[0]).unwrap(), Some(vec![1.0, 0.0]));
    assert_eq!(reopened.get_vector(&ids[1]).unwrap(), Some(vec![0.0, 1.0]));
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
    let primary_batches = snapshot.transactions[0]
        .payloads
        .iter()
        .filter(|payload| payload.modality == PositionedMutationModality::PrimaryDense)
        .collect::<Vec<_>>();
    assert_eq!(primary_batches.len(), 1);
    assert_eq!(primary_batches[0].rows, 2);
}

#[test]
fn grouped_append_reconciles_an_accepted_positioned_head_error_once() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let uri = "memory:///positioned-grouped-ambiguous-head";
    BorsukIndex::create_with_object_store(Arc::clone(&inner), config(uri)).unwrap();
    let faulted = common::FaultInjectingObjectStore::accept_then_fail_nth_put(
        Arc::clone(&inner),
        1,
        |operation, path| {
            operation == common::StoreOperation::Put
                && path.as_ref().starts_with("positioned-log/heads/")
        },
    );
    let (faulted, operations) = faulted.with_operation_log();
    let index = BorsukIndex::open_with_object_store(Arc::new(faulted), uri).unwrap();
    let grouped = writer(index, 1);
    operations.clear();

    let receipt = grouped
        .append(vec![
            VectorRecord::new("group-a", vec![1.0, 0.0]),
            VectorRecord::new("group-b", vec![0.0, 1.0]),
        ])
        .unwrap();

    let position = receipt
        .position
        .expect("reconciled group append must return its current source position");
    assert_eq!(receipt.records, 2);
    assert_eq!(receipt.committed_records, 2);
    assert_eq!(receipt.envelope_checksum.len(), 64);
    assert!(receipt.encoded_bytes > 0);
    assert_eq!(receipt.requests, logged_request_counts(&operations));
    assert_eq!(
        operations.count_matching(|operation, path| {
            operation == common::StoreOperation::Put && path.starts_with("positioned-log/heads/")
        }),
        1,
        "an accepted ambiguous group head PUT must reconcile instead of publishing again"
    );
    assert_no_legacy_mutation_writes(&operations);
    drop(grouped);

    let reopened = BorsukIndex::open_with_object_store(Arc::clone(&inner), uri).unwrap();
    assert_eq!(reopened.list_records(0, 10).unwrap().len(), 2);
    assert_eq!(
        reopened.get_vector("group-a").unwrap(),
        Some(vec![1.0, 0.0])
    );
    assert_eq!(
        reopened.get_vector("group-b").unwrap(),
        Some(vec![0.0, 1.0])
    );
    let snapshot = PositionedLogWriter::open(uri, inner, 1)
        .unwrap()
        .reader()
        .snapshot()
        .unwrap();
    assert_eq!(snapshot.transactions.len(), 1);
    assert_eq!(snapshot.transactions[0].position, position);
    assert_eq!(snapshot.envelope_checksums, [receipt.envelope_checksum]);
    let primary_batches = snapshot.transactions[0]
        .payloads
        .iter()
        .filter(|payload| payload.modality == PositionedMutationModality::PrimaryDense)
        .collect::<Vec<_>>();
    assert_eq!(primary_batches.len(), 1);
    assert_eq!(primary_batches[0].rows, 2);
}

#[test]
fn ordinary_and_group_writes_share_one_positioned_protocol() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (traced, operations) =
        common::FaultInjectingObjectStore::new(Arc::clone(&inner)).with_operation_log();
    let uri = "memory:///ordinary-group-positioned-protocol";
    let mut index = BorsukIndex::create_with_object_store(Arc::new(traced), config(uri)).unwrap();
    operations.clear();

    index
        .add(vec![VectorRecord::new("ordinary", vec![1.0, 0.0])])
        .unwrap();
    let grouped = writer(index, 1);
    let receipt = grouped
        .append(vec![VectorRecord::new("grouped", vec![0.0, 1.0])])
        .unwrap();
    assert!(receipt.position.is_some());
    assert_eq!(receipt.envelope_checksum.len(), 64);

    assert_eq!(
        operations.count_matching(|operation, path| {
            operation == common::StoreOperation::Put && path.starts_with("positioned-log/heads/")
        }),
        2
    );
    assert_eq!(
        operations.count_matching(|_, path| path.starts_with("lane-log/")),
        0
    );
    assert_eq!(
        operations.count_matching(|_, path| path.contains("cell-wal/commits/")),
        0
    );
    let reopened = BorsukIndex::open_with_object_store(inner, uri).unwrap();
    assert_eq!(
        reopened.get_vector("ordinary").unwrap(),
        Some(vec![1.0, 0.0])
    );
    assert_eq!(
        reopened.get_vector("grouped").unwrap(),
        Some(vec![0.0, 1.0])
    );
}

#[test]
fn grouped_upsert_is_last_write_wins_and_reopens() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let uri = "memory:///positioned-group-lww";
    let index = BorsukIndex::create_with_object_store(Arc::clone(&store), config(uri)).unwrap();
    let grouped = writer(index, 1);
    grouped
        .append(vec![VectorRecord::new("same", vec![1.0, 0.0])])
        .unwrap();
    grouped
        .append(vec![VectorRecord::new("same", vec![9.0, 0.0])])
        .unwrap();
    let reopened = BorsukIndex::open_with_object_store(store, uri).unwrap();
    assert_eq!(reopened.get_vector("same").unwrap(), Some(vec![9.0, 0.0]));
}

#[test]
fn ordinary_insert_still_rejects_duplicates() {
    let uri = "memory:///positioned-add-duplicate";
    let mut index = BorsukIndex::create(config(uri)).unwrap();
    index
        .add(vec![VectorRecord::new("duplicate", vec![1.0, 0.0])])
        .unwrap();
    assert!(
        index
            .add(vec![VectorRecord::new("duplicate", vec![2.0, 0.0])])
            .is_err()
    );
}

#[test]
fn one_caller_batch_has_one_atomic_position() {
    let uri = "memory:///positioned-one-caller-one-position";
    let index = BorsukIndex::create(config(uri)).unwrap();
    let grouped = writer(index, 8);
    let receipt = grouped
        .append(
            (0..128)
                .map(|row| VectorRecord::new(format!("id-{row}"), vec![row as f32, 0.0]))
                .collect(),
        )
        .unwrap();
    assert_eq!(receipt.records, 128);
    assert_eq!(receipt.committed_records, 128);
    assert!(receipt.position.is_some());
}

#[test]
fn one_eight_and_thirty_two_producers_converge_after_reopen() {
    for producers in [1, 8, 32] {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let uri = format!("memory:///positioned-producers-{producers}");
        let index =
            BorsukIndex::create_with_object_store(Arc::clone(&store), config(&uri)).unwrap();
        let grouped = writer(index, producers.min(8));
        let mut handles = Vec::new();
        for producer in 0..producers {
            let grouped = grouped.clone();
            handles.push(thread::spawn(move || {
                grouped
                    .append(vec![VectorRecord::new(
                        format!("producer-{producer}"),
                        vec![producer as f32, 1.0],
                    )])
                    .unwrap()
            }));
        }
        for handle in handles {
            assert!(handle.join().unwrap().position.is_some());
        }
        grouped.drain().unwrap();
        let reopened = BorsukIndex::open_with_object_store(store, &uri).unwrap();
        assert_eq!(
            reopened.list_records(0, producers + 1).unwrap().len(),
            producers
        );
    }
}

#[test]
fn warm_grouped_upsert_does_not_use_exact_id_claim_pages() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (traced, operations) =
        common::FaultInjectingObjectStore::new(Arc::clone(&inner)).with_operation_log();
    let uri = "memory:///positioned-group-no-claims";
    let index = BorsukIndex::create_with_object_store(Arc::new(traced), config(uri)).unwrap();
    let grouped = writer(index, 1);
    operations.clear();
    grouped
        .append(
            (0..64)
                .map(|row| VectorRecord::new(format!("id-{row}"), vec![row as f32, 0.0]))
                .collect(),
        )
        .unwrap();
    assert_eq!(
        operations.count_matching(|_, path| {
            path.starts_with("id-directory/claim-pages/")
                || path.starts_with("transactions/")
                || path.starts_with("positioned-log/claim-authorizations/")
        }),
        0
    );
}

#[test]
fn drain_is_only_a_barrier_and_writes_no_checkpoint() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (traced, operations) =
        common::FaultInjectingObjectStore::new(Arc::clone(&inner)).with_operation_log();
    let uri = "memory:///positioned-drain-barrier";
    let index = BorsukIndex::create_with_object_store(Arc::new(traced), config(uri)).unwrap();
    let grouped = writer(index, 2);
    grouped
        .append(vec![VectorRecord::new("row", vec![1.0, 1.0])])
        .unwrap();
    operations.clear();
    grouped.drain().unwrap();
    assert_eq!(
        operations.count_matching(|operation, _| operation == common::StoreOperation::Put),
        0
    );
}

#[test]
fn sixty_five_id_partitions_stay_in_one_bounded_positioned_transaction() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (traced, operations) =
        common::FaultInjectingObjectStore::new(Arc::clone(&inner)).with_operation_log();
    let uri = "memory:///positioned-bundled-id-directory";
    let index = BorsukIndex::create_with_object_store(Arc::new(traced), config(uri)).unwrap();
    let grouped = writer(index, 1);
    let mut partitions = std::collections::BTreeSet::new();
    let mut records = Vec::new();
    for ordinal in 0.. {
        let id = format!("partition-{ordinal}");
        let digest = blake3::hash(id.as_bytes());
        let partition = u16::from_le_bytes([digest.as_bytes()[0], digest.as_bytes()[1]]) % 4_096;
        if partitions.insert(partition) {
            records.push(VectorRecord::new(id, vec![ordinal as f32, 0.0]));
            if records.len() == 65 {
                break;
            }
        }
    }
    operations.clear();
    grouped.append(records).unwrap();
    assert_eq!(
        operations.count_matching(|operation, path| {
            operation == common::StoreOperation::Put && path.starts_with("positioned-log/heads/")
        }),
        1
    );
    let snapshot = PositionedLogWriter::open(uri, inner, 1)
        .unwrap()
        .reader()
        .snapshot()
        .unwrap();
    assert_eq!(snapshot.transactions.len(), 1);
    assert!(snapshot.transactions[0].payloads.len() <= 64);
    assert_eq!(
        snapshot.transactions[0]
            .payloads
            .iter()
            .filter(|payload| payload.role.contains("id-directory"))
            .count(),
        1
    );
}

#[test]
fn sixty_five_named_modalities_and_tombstones_stay_bounded_and_reopen() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let uri = "memory:///positioned-sixty-five-modalities";
    let mut collection = config(uri);
    for ordinal in 0..65 {
        collection.named_vectors.insert(
            format!("named-{ordinal:02}"),
            VectorSpec {
                dimensions: 2,
                metric: VectorMetric::Euclidean,
                kind: Default::default(),
                element_type: Default::default(),
            },
        );
    }
    let record = |value: f32| {
        let mut record = VectorRecord::new("entity", vec![value, 0.0]);
        for ordinal in 0..65 {
            record = record
                .with_named_vector(format!("named-{ordinal:02}"), vec![value, ordinal as f32]);
        }
        record
    };
    let mut index = BorsukIndex::create_with_object_store(Arc::clone(&store), collection).unwrap();
    index.add(vec![record(1.0)]).unwrap();
    index.upsert(vec![record(9.0)]).unwrap();
    drop(index);

    let mut reopened = BorsukIndex::open_with_object_store(Arc::clone(&store), uri).unwrap();
    assert_eq!(reopened.get_vector("entity").unwrap(), Some(vec![9.0, 0.0]));
    assert_eq!(
        reopened
            .search_with_report(
                &[9.0, 0.0],
                SearchOptions::exact(1).with_vector_name("named-00"),
            )
            .unwrap()
            .hits[0]
            .id
            .as_str(),
        "entity"
    );
    reopened.delete(["entity"]).unwrap();
    drop(reopened);

    let reopened = BorsukIndex::open_with_object_store(Arc::clone(&store), uri).unwrap();
    assert_eq!(reopened.get_vector("entity").unwrap(), None);
    let snapshot = PositionedLogWriter::open(uri, store, 1)
        .unwrap()
        .reader()
        .snapshot()
        .unwrap();
    assert!(
        snapshot
            .transactions
            .iter()
            .all(|transaction| transaction.payloads.len() <= 64)
    );
}

#[test]
fn generated_ids_use_only_positioned_truth_and_reopen_exactly() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (traced, operations) =
        common::FaultInjectingObjectStore::new(Arc::clone(&inner)).with_operation_log();
    let uri = "memory:///positioned-generated-ids";
    let mut index = BorsukIndex::create_with_object_store(Arc::new(traced), config(uri)).unwrap();
    operations.clear();
    let ids = index
        .add_vectors(vec![vec![1.0, 0.0], vec![0.0, 1.0]])
        .unwrap();
    assert_ne!(ids[0], ids[1]);
    assert_eq!(
        operations.count_matching(|_, path| path == "id-directory/generated/NEXT"),
        0
    );
    drop(index);
    let reopened = BorsukIndex::open_with_object_store(inner, uri).unwrap();
    assert_eq!(reopened.get_vector(&ids[0]).unwrap(), Some(vec![1.0, 0.0]));
    assert_eq!(reopened.get_vector(&ids[1]).unwrap(), Some(vec![0.0, 1.0]));
}

#[test]
fn upsert_and_delete_do_not_prewrite_legacy_mutation_objects() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (traced, operations) =
        common::FaultInjectingObjectStore::new(Arc::clone(&inner)).with_operation_log();
    let uri = "memory:///positioned-no-prewrite";
    let mut index = BorsukIndex::create_with_object_store(Arc::new(traced), config(uri)).unwrap();
    index
        .add(vec![VectorRecord::new("row", vec![1.0, 0.0])])
        .unwrap();
    operations.clear();
    index
        .upsert(vec![VectorRecord::new("row", vec![2.0, 0.0])])
        .unwrap();
    index.delete(["row"]).unwrap();

    assert_eq!(
        operations.count_matching(|_, path| {
            path.starts_with("lane-log/")
                || path.starts_with("cell-wal/")
                || path.starts_with("transactions/")
                || (path.starts_with("cells/") && path.contains("/wal/"))
                || path.starts_with("tombstones/")
                || path.starts_with("bm25/")
                || path.starts_with("lexical/stats-delta/")
                || path == "id-directory/generated/NEXT"
        }),
        0
    );
    drop(index);
    assert_eq!(
        BorsukIndex::open_with_object_store(inner, uri)
            .unwrap()
            .get_vector("row")
            .unwrap(),
        None
    );
}

#[test]
fn accepted_release_loss_still_allows_exactly_one_concurrent_add() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let faulted = common::FaultInjectingObjectStore::accept_then_fail_nth_put(
        Arc::clone(&inner),
        2,
        |operation, path| {
            operation == common::StoreOperation::Put
                && path.as_ref().starts_with("id-directory/claim-pages/")
        },
    );
    let (faulted, operations) = faulted.with_operation_log();
    let store: Arc<dyn ObjectStore> = Arc::new(faulted);
    let uri = "memory:///positioned-release-loss-concurrent-add";
    BorsukIndex::create_with_object_store(Arc::clone(&store), config(uri)).unwrap();
    let left = BorsukIndex::open_with_object_store(Arc::clone(&store), uri).unwrap();
    let right = BorsukIndex::open_with_object_store(Arc::clone(&store), uri).unwrap();
    operations.clear();
    let start = Arc::new(std::sync::Barrier::new(2));
    let handles = [left, right].map(|mut index| {
        let start = Arc::clone(&start);
        thread::spawn(move || {
            start.wait();
            index.add(vec![VectorRecord::new("same-id", vec![1.0, 0.0])])
        })
    });
    let results = handles.map(|handle| handle.join().unwrap());
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    assert_eq!(results.iter().filter(|result| result.is_err()).count(), 1);
    assert!(
        results
            .iter()
            .find_map(|result| result.as_ref().err())
            .unwrap()
            .to_string()
            .contains("already exists")
    );
    assert_eq!(
        operations.count_matching(|operation, path| {
            operation == common::StoreOperation::Put
                && path.starts_with("positioned-log/claim-authorizations/")
        }),
        1
    );
    let reopened = BorsukIndex::open_with_object_store(inner, uri).unwrap();
    assert_eq!(reopened.list_records(0, 10).unwrap().len(), 1);
    assert_eq!(
        reopened.get_vector("same-id").unwrap(),
        Some(vec![1.0, 0.0])
    );
}

#[test]
fn text_upsert_and_delete_reopen_without_legacy_delta_writes() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (traced, operations) =
        common::FaultInjectingObjectStore::new(Arc::clone(&inner)).with_operation_log();
    let uri = "memory:///positioned-text-reopen";
    let mut text_config = config(uri);
    text_config.text = true;
    let traced: Arc<dyn ObjectStore> = Arc::new(traced);
    let mut index =
        BorsukIndex::create_with_object_store(Arc::clone(&traced), text_config).unwrap();
    index
        .add(vec![
            VectorRecord::new("doc", vec![1.0, 0.0]).with_text("oldterm stable"),
        ])
        .unwrap();
    operations.clear();
    index
        .upsert(vec![
            VectorRecord::new("doc", vec![2.0, 0.0]).with_text("newterm stable"),
        ])
        .unwrap();
    drop(index);
    let mut reopened = BorsukIndex::open_with_object_store(Arc::clone(&traced), uri).unwrap();
    assert!(reopened.search_text("oldterm", 5).unwrap().hits.is_empty());
    assert_eq!(
        reopened.search_text("newterm", 5).unwrap().hits[0]
            .id
            .as_str(),
        "doc"
    );
    reopened.delete(["doc"]).unwrap();
    drop(reopened);
    assert!(
        BorsukIndex::open_with_object_store(inner, uri)
            .unwrap()
            .search_text("newterm", 5)
            .unwrap()
            .hits
            .is_empty()
    );
    assert_eq!(
        operations.count_matching(|_, path| {
            path.starts_with("transactions/")
                || (path.starts_with("cells/") && path.contains("/wal/"))
                || path.starts_with("tombstones/")
                || path.starts_with("lexical/stats-delta/")
                || path == "id-directory/generated/NEXT"
        }),
        0
    );
}

#[test]
fn late_interaction_replacement_and_delete_reopen_from_one_positioned_log() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (traced, operations) =
        common::FaultInjectingObjectStore::new(Arc::clone(&inner)).with_operation_log();
    let uri = "memory:///positioned-late-reopen";
    let mut late_config = config(uri);
    late_config.named_vectors = BTreeMap::from([(
        "tokens".to_string(),
        VectorSpec {
            dimensions: 2,
            metric: VectorMetric::InnerProduct,
            kind: VectorKind::LateInteraction,
            element_type: VectorElementType::Float32,
        },
    )]);
    let record = |value: f32, tokens: Vec<Vec<f32>>| {
        VectorRecord::new("entity", vec![value, 0.0])
            .with_late_interaction("tokens", tokens)
            .unwrap()
    };
    let traced: Arc<dyn ObjectStore> = Arc::new(traced);
    let mut index =
        BorsukIndex::create_with_object_store(Arc::clone(&traced), late_config).unwrap();
    index
        .add(vec![record(1.0, vec![vec![1.0, 0.0], vec![0.0, 1.0]])])
        .unwrap();
    operations.clear();
    index
        .upsert(vec![record(2.0, vec![vec![-1.0, 0.0]])])
        .unwrap();
    drop(index);
    let mut reopened = BorsukIndex::open_with_object_store(Arc::clone(&traced), uri).unwrap();
    assert_eq!(
        reopened
            .search_late_interaction("tokens", vec![vec![-1.0, 0.0]], 1)
            .unwrap()[0]
            .id
            .as_str(),
        "entity"
    );
    reopened.delete(["entity"]).unwrap();
    drop(reopened);
    assert!(
        BorsukIndex::open_with_object_store(inner, uri)
            .unwrap()
            .search_late_interaction("tokens", vec![vec![-1.0, 0.0]], 1)
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        operations.count_matching(|_, path| {
            path.starts_with("transactions/")
                || (path.starts_with("cells/") && path.contains("/wal/"))
                || path.starts_with("tombstones/")
                || path.starts_with("lexical/stats-delta/")
                || path == "id-directory/generated/NEXT"
        }),
        0
    );
}

#[test]
fn committed_cleanup_failure_clears_transaction_and_reports_position() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let auth_fault = common::FaultInjectingObjectStore::fail_nth_matching(
        Arc::clone(&inner),
        1,
        true,
        |operation, path| {
            operation == common::StoreOperation::Put
                && path
                    .as_ref()
                    .contains("positioned-log/claim-authorizations/")
        },
    );
    let release_fault = common::FaultInjectingObjectStore::accept_then_fail_nth_put(
        Arc::new(auth_fault),
        2,
        |operation, path| {
            operation == common::StoreOperation::Put
                && path.as_ref().starts_with("id-directory/claim-pages/")
        },
    );
    let uri = "memory:///positioned-committed-cleanup-error";
    let mut index =
        BorsukIndex::create_with_object_store(Arc::new(release_fault), config(uri)).unwrap();

    let error = index
        .add(vec![VectorRecord::new("committed", vec![1.0, 0.0])])
        .unwrap_err();
    let BorsukError::PositionedCommitCleanupFailed {
        source_epoch,
        shard: _,
        sequence,
        envelope_checksum,
        cleanup,
    } = error
    else {
        panic!("unexpected error after positioned commit: {error}");
    };
    assert_eq!(source_epoch, 1);
    assert!(sequence > 0);
    assert_eq!(envelope_checksum.len(), 64);
    assert!(!cleanup.is_empty());

    index
        .add(vec![VectorRecord::new("after", vec![0.0, 1.0])])
        .unwrap();
    assert!(
        index
            .add(vec![VectorRecord::new("committed", vec![9.0, 0.0])])
            .unwrap_err()
            .to_string()
            .contains("already exists")
    );
    drop(index);

    let reopened = BorsukIndex::open_with_object_store(inner, uri).unwrap();
    assert_eq!(
        reopened.get_vector("committed").unwrap(),
        Some(vec![1.0, 0.0])
    );
    assert_eq!(reopened.get_vector("after").unwrap(), Some(vec![0.0, 1.0]));
}

#[test]
fn facades_return_current_seam_receipt_and_group_failure_cannot_reuse_one() {
    let uri = "memory:///positioned-single-append-seam";
    let mut index = BorsukIndex::create(config(uri)).unwrap();
    let (_, ordinary) = index
        .add_with_report(vec![vec![1.0, 0.0]], Some(vec!["ordinary".to_string()]))
        .unwrap();
    assert!(ordinary.positioned_position.is_some());
    assert_eq!(ordinary.positioned_envelope_checksum.len(), 64);
    assert!(ordinary.positioned_encoded_bytes > 0);

    let grouped = writer(index, 1);
    let first = grouped
        .append(vec![VectorRecord::new("first", vec![0.0, 1.0])])
        .unwrap();
    assert!(
        grouped
            .append(vec![VectorRecord::new("invalid", vec![1.0])])
            .is_err()
    );
    let second = grouped
        .append(vec![VectorRecord::new("second", vec![2.0, 0.0])])
        .unwrap();
    assert_ne!(first.position, second.position);
    assert_ne!(first.envelope_checksum, second.envelope_checksum);
}

#[test]
fn mutation_facades_report_each_physical_request_exactly_once() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (traced, operations) =
        common::FaultInjectingObjectStore::new(Arc::clone(&inner)).with_operation_log();
    let uri = "memory:///positioned-request-telemetry";
    let mut index = BorsukIndex::create_with_object_store(Arc::new(traced), config(uri)).unwrap();

    operations.clear();
    let (_, ordinary) = index
        .add_with_report(vec![vec![1.0, 0.0]], Some(vec!["ordinary".to_string()]))
        .unwrap();
    assert_eq!(ordinary.requests, logged_request_counts(&operations));

    operations.clear();
    let (_, generated) = index.add_vectors_with_report(vec![vec![0.0, 1.0]]).unwrap();
    assert_eq!(generated.requests, logged_request_counts(&operations));

    operations.clear();
    let upsert = index
        .upsert_with_report(vec![VectorRecord::new("ordinary", vec![2.0, 0.0])])
        .unwrap();
    assert_eq!(upsert.requests, logged_request_counts(&operations));

    operations.clear();
    let deleted = index.delete(["ordinary"]).unwrap();
    assert_eq!(deleted.requests, logged_request_counts(&operations));

    let grouped = writer(index, 1);
    operations.clear();
    let grouped = grouped
        .append(vec![VectorRecord::new("grouped", vec![3.0, 0.0])])
        .unwrap();
    assert_eq!(grouped.requests, logged_request_counts(&operations));
}

#[test]
fn mutation_facades_report_each_submitted_put_payload_byte_exactly_once() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (traced, operations) =
        common::FaultInjectingObjectStore::new(Arc::clone(&inner)).with_operation_log();
    let uri = "memory:///positioned-write-byte-telemetry";
    let mut index = BorsukIndex::create_with_object_store(Arc::new(traced), config(uri)).unwrap();

    operations.clear();
    let (_, ordinary) = index
        .add_with_report(
            vec![vec![1.0, 0.0], vec![2.0, 0.0], vec![3.0, 0.0]],
            Some(vec![
                "ordinary-a".to_string(),
                "ordinary-b".to_string(),
                "ordinary-c".to_string(),
            ]),
        )
        .unwrap();
    assert_eq!(
        ordinary.total_bytes_written,
        logged_put_payload_bytes(&operations)
    );
    assert_eq!(
        ordinary.bytes_per_vector,
        ordinary.total_bytes_written as f64 / 3.0
    );

    operations.clear();
    let (_, generated) = index
        .add_vectors_with_report(vec![vec![0.0, 1.0], vec![0.0, 2.0]])
        .unwrap();
    assert_eq!(
        generated.total_bytes_written,
        logged_put_payload_bytes(&operations)
    );
    assert_eq!(
        generated.bytes_per_vector,
        generated.total_bytes_written as f64 / 2.0
    );

    operations.clear();
    let upsert = index
        .upsert_with_report(vec![
            VectorRecord::new("ordinary-a", vec![4.0, 0.0]),
            VectorRecord::new("ordinary-b", vec![5.0, 0.0]),
            VectorRecord::new("ordinary-c", vec![6.0, 0.0]),
        ])
        .unwrap();
    assert_eq!(
        upsert.total_bytes_written,
        logged_put_payload_bytes(&operations)
    );
    assert_eq!(
        upsert.bytes_per_vector,
        upsert.total_bytes_written as f64 / 3.0
    );
}

#[test]
fn overlapping_cloned_mutation_handles_have_disjoint_exact_report_scopes() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let barrier = Arc::new(std::sync::Barrier::new(2));
    let traced = common::FaultInjectingObjectStore::new(Arc::clone(&inner))
        .with_first_matching_puts_barrier(Arc::clone(&barrier), 2, |operation, path| {
            operation == common::StoreOperation::Put
                && path.as_ref().starts_with("id-directory/claim-pages/")
        });
    let (traced, overlap) = traced.with_put_concurrency_probe();
    let (traced, operations) = traced.with_operation_log();
    let uri = "memory:///positioned-overlapping-report-scopes";
    let index = BorsukIndex::create_with_object_store(Arc::new(traced), config(uri)).unwrap();
    let left = index.clone();
    let right = index;
    operations.clear();
    let mutations = [(left, "left"), (right, "right")].map(|(mut handle, id)| {
        thread::spawn(move || {
            handle
                .add_with_report(vec![vec![1.0, 0.0]], Some(vec![id.to_string()]))
                .map(|(_, report)| report)
        })
    });
    let [left, right] = mutations.map(|handle| handle.join().unwrap().unwrap());

    assert!(
        overlap.peak() >= 2,
        "the fault barrier must prove the two public mutations overlapped"
    );
    assert_eq!(
        sum_request_counts(left.requests, right.requests),
        logged_request_counts(&operations)
    );
    assert_eq!(
        left.total_bytes_written + right.total_bytes_written,
        logged_put_payload_bytes(&operations)
    );
}

#[test]
fn delete_receipts_are_request_local_across_stale_writers_and_reopen() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let uri = "memory:///positioned-delete-request-local";
    let mut index = BorsukIndex::create_with_object_store(Arc::clone(&inner), config(uri)).unwrap();
    index
        .add(vec![
            VectorRecord::new("alpha", vec![0.0, 0.0]),
            VectorRecord::new("beta", vec![1.0, 0.0]),
        ])
        .unwrap();

    let first = index.delete(["beta", "beta"]).unwrap();
    assert_eq!(first.ids_submitted, 1);
    assert!(first.published);
    assert_eq!(index.get_vector("beta").unwrap(), None);

    let repeated = index.delete(["beta"]).unwrap();
    assert_eq!(repeated.ids_submitted, 1);
    assert!(!repeated.published);
    drop(index);

    let mut reopened = BorsukIndex::open_with_object_store(Arc::clone(&inner), uri).unwrap();
    let reopened_repeat = reopened.delete(["beta"]).unwrap();
    assert_eq!(reopened_repeat.ids_submitted, 1);
    assert!(!reopened_repeat.published);
    assert_eq!(reopened.get_vector("beta").unwrap(), None);

    let mut same_left = BorsukIndex::open_with_object_store(Arc::clone(&inner), uri).unwrap();
    let mut same_right = BorsukIndex::open_with_object_store(Arc::clone(&inner), uri).unwrap();
    let same_left_report = same_left.delete(["alpha"]).unwrap();
    let same_right_report = same_right.delete(["alpha"]).unwrap();
    assert_eq!(same_left_report.ids_submitted, 1);
    assert_eq!(same_right_report.ids_submitted, 1);
    assert!(same_left_report.published);
    assert!(same_right_report.published);
    let same_final = BorsukIndex::open_with_object_store(Arc::clone(&inner), uri).unwrap();
    assert_eq!(same_final.get_vector("alpha").unwrap(), None);

    let different_inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let different_uri = "memory:///positioned-delete-stale-different";
    let mut seed =
        BorsukIndex::create_with_object_store(Arc::clone(&different_inner), config(different_uri))
            .unwrap();
    seed.add(vec![
        VectorRecord::new("left", vec![1.0, 0.0]),
        VectorRecord::new("right", vec![0.0, 1.0]),
    ])
    .unwrap();
    let mut different_left =
        BorsukIndex::open_with_object_store(Arc::clone(&different_inner), different_uri).unwrap();
    let mut different_right =
        BorsukIndex::open_with_object_store(Arc::clone(&different_inner), different_uri).unwrap();
    assert!(different_left.delete(["left"]).unwrap().published);
    assert!(different_right.delete(["right"]).unwrap().published);
    let different_final =
        BorsukIndex::open_with_object_store(different_inner, different_uri).unwrap();
    assert_eq!(different_final.get_vector("left").unwrap(), None);
    assert_eq!(different_final.get_vector("right").unwrap(), None);

    let put_inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let put_uri = "memory:///positioned-delete-report-after-put";
    let mut put_index = BorsukIndex::create_with_object_store(put_inner, config(put_uri)).unwrap();
    put_index
        .put(vec![VectorRecord::new("put-row", vec![3.0, 0.0])])
        .unwrap();
    let after_put = put_index.delete(["put-row"]).unwrap();
    assert_eq!(after_put.ids_submitted, 1);
    assert!(after_put.published);
    assert_eq!(put_index.get_vector("put-row").unwrap(), None);

    put_index
        .upsert(vec![VectorRecord::new("put-row", vec![4.0, 0.0])])
        .unwrap();
    assert_eq!(
        put_index.get_vector("put-row").unwrap(),
        Some(vec![4.0, 0.0])
    );
    let after_upsert = put_index.delete(["put-row", "put-row"]).unwrap();
    assert_eq!(after_upsert.ids_submitted, 1);
    assert!(after_upsert.published);
    assert_eq!(put_index.get_vector("put-row").unwrap(), None);
}

#[test]
fn materialization_rebases_duplicate_delete_upper_bound_to_page_cardinality() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let uri = "memory:///positioned-delete-materialization-rebase";
    let mut seed = BorsukIndex::create_with_object_store(Arc::clone(&inner), config(uri)).unwrap();
    seed.add(vec![VectorRecord::new("victim", vec![1.0, 0.0])])
        .unwrap();
    seed.flush().unwrap();

    let mut left = BorsukIndex::open_with_object_store(Arc::clone(&inner), uri).unwrap();
    let mut right = BorsukIndex::open_with_object_store(Arc::clone(&inner), uri).unwrap();
    assert!(left.delete(["victim"]).unwrap().published);
    assert!(right.delete(["victim"]).unwrap().published);

    let mut materializer = BorsukIndex::open_with_object_store(Arc::clone(&inner), uri).unwrap();
    assert_eq!(materializer.get_vector("victim").unwrap(), None);
    materializer.flush().unwrap();
    let purge = materializer.purge_with_report().unwrap();
    assert_eq!(purge.records_purged, 1);
    assert_eq!(
        purge.tombstones_cleared, 1,
        "the fenced stable page cardinality must replace duplicate tail contributions"
    );
}
