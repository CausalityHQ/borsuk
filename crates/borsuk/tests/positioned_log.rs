//! Standalone durability, boundedness, and recovery tests for the V12 positioned log.

mod common;

use std::{
    io::Cursor,
    sync::{Arc, Barrier},
    time::Duration,
};

use arrow_array::{ArrayRef, FixedSizeBinaryArray, RecordBatch, UInt64Array};
use arrow_ipc::{
    CompressionType,
    writer::{IpcWriteOptions, StreamWriter},
};
use arrow_schema::{DataType, Field, Schema};
use borsuk::{
    MAX_APPEND_ENCODED_BYTES, MAX_APPEND_ROWS, MAX_PAYLOADS_PER_TRANSACTION,
    MAX_PENDING_ENVELOPES_PER_SHARD, MAX_RECENT_COMMIT_RECEIPTS_PER_SHARD, PositionedLogWriter,
    PositionedMutationModality, PositionedMutationPayloadInput, PositionedPayloadFormat,
    SOURCE_SHARD_COUNT,
};
use bytes::Bytes;
use object_store::{
    ObjectStore, ObjectStoreExt, PutOptions, PutPayload, memory::InMemory, path::Path as ObjectPath,
};
use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};

const SCHEMA_FINGERPRINT: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

fn arrow_payload(hlc: u64) -> Vec<u8> {
    arrow_payload_rows(hlc, 1)
}

fn mutation_batch(first_hlc: u64, rows: usize) -> (Arc<Schema>, RecordBatch) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("mutation_hlc", DataType::UInt64, false),
        Field::new("mutation_writer", DataType::FixedSizeBinary(16), false),
        Field::new("mutation_digest", DataType::FixedSizeBinary(32), false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(UInt64Array::from_iter_values(
                first_hlc..first_hlc + rows as u64,
            )) as ArrayRef,
            Arc::new(FixedSizeBinaryArray::try_from_iter((0..rows).map(|_| [7_u8; 16])).unwrap())
                as ArrayRef,
            Arc::new(FixedSizeBinaryArray::try_from_iter((0..rows).map(|_| [9_u8; 32])).unwrap())
                as ArrayRef,
        ],
    )
    .unwrap();
    (schema, batch)
}

fn arrow_payload_rows(first_hlc: u64, rows: usize) -> Vec<u8> {
    let (schema, batch) = mutation_batch(first_hlc, rows);
    let mut bytes = Vec::new();
    let mut writer = StreamWriter::try_new(&mut bytes, &schema).unwrap();
    writer.write(&batch).unwrap();
    writer.finish().unwrap();
    drop(writer);
    bytes
}

fn compressed_arrow_payload(hlc: u64) -> Vec<u8> {
    let (schema, batch) = mutation_batch(hlc, 1);
    let options = IpcWriteOptions::default()
        .try_with_compression(Some(CompressionType::ZSTD))
        .unwrap();
    let mut bytes = Vec::new();
    let mut writer = StreamWriter::try_new_with_options(&mut bytes, &schema, options).unwrap();
    writer.write(&batch).unwrap();
    writer.finish().unwrap();
    drop(writer);
    bytes
}

fn parquet_payload(hlc: u64) -> Vec<u8> {
    let (schema, batch) = mutation_batch(hlc, 1);
    let mut bytes = Vec::new();
    let mut writer = ArrowWriter::try_new(
        &mut bytes,
        schema,
        Some(WriterProperties::builder().build()),
    )
    .unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    bytes
}

fn overwrite(store: &Arc<dyn ObjectStore>, path: &str, bytes: Vec<u8>) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime
        .block_on(store.put_opts(
            &ObjectPath::from(path),
            PutPayload::from(Bytes::from(bytes)),
            PutOptions::default(),
        ))
        .unwrap();
}

fn read(store: &Arc<dyn ObjectStore>, path: &str) -> Vec<u8> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime
        .block_on(async { store.get(&ObjectPath::from(path)).await?.bytes().await })
        .unwrap()
        .to_vec()
}

fn payload(role: impl Into<String>, hlc: u64) -> PositionedMutationPayloadInput {
    PositionedMutationPayloadInput {
        modality: PositionedMutationModality::PrimaryDense,
        role: role.into(),
        format: PositionedPayloadFormat::ArrowIpc,
        bytes: arrow_payload(hlc),
        rows: 1,
    }
}

fn create_writer(store: Arc<dyn ObjectStore>, epoch: u64) -> PositionedLogWriter {
    PositionedLogWriter::create("memory:///positioned-log", store, epoch).unwrap()
}

fn tx_shard(transaction_id: &str) -> u8 {
    blake3::hash(transaction_id.as_bytes()).as_bytes()[0] % SOURCE_SHARD_COUNT
}

fn transaction_ids_for_shard(shard: u8, count: usize) -> Vec<String> {
    (0_u64..)
        .map(|ordinal| format!("tx-{ordinal}"))
        .filter(|transaction_id| tx_shard(transaction_id) == shard)
        .take(count)
        .collect()
}

#[test]
fn arrow_and_parquet_payloads_reopen_from_one_typed_envelope() {
    let writer = create_writer(Arc::new(InMemory::new()), 7);
    let committed = writer
        .append(
            "typed-formats",
            SCHEMA_FINGERPRINT,
            vec![
                payload("arrow", 10),
                PositionedMutationPayloadInput {
                    modality: PositionedMutationModality::Sparse,
                    role: "parquet".to_owned(),
                    format: PositionedPayloadFormat::Parquet,
                    bytes: parquet_payload(11),
                    rows: 1,
                },
            ],
        )
        .unwrap();

    let snapshot = writer.reader().snapshot().unwrap();
    assert_eq!(snapshot.transactions.len(), 1);
    let envelope = &snapshot.transactions[0];
    assert_eq!(envelope.position, committed.position);
    assert_eq!(envelope.payloads.len(), 2);
    assert!(envelope.payloads[0] < envelope.payloads[1]);
}

#[test]
fn two_writers_rebase_one_shard_without_duplicate_visibility() {
    let base: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    create_writer(Arc::clone(&base), 7);
    let barrier = Arc::new(Barrier::new(2));
    let ids = transaction_ids_for_shard(9, 2);
    let mut operation_logs = Vec::new();
    let writers = (0..2)
        .map(|_| {
            let wrapped = common::FaultInjectingObjectStore::new(Arc::clone(&base))
                .with_put_barrier(Arc::clone(&barrier), |operation, path| {
                    operation == common::StoreOperation::Put
                        && path.as_ref().starts_with("positioned-log/heads/")
                });
            let (wrapped, operations) = wrapped.with_operation_log();
            let writer =
                PositionedLogWriter::open("memory:///positioned-log", Arc::new(wrapped), 7)
                    .unwrap();
            operations.clear();
            operation_logs.push(operations);
            writer
        })
        .collect::<Vec<_>>();

    let threads = writers
        .into_iter()
        .zip(ids)
        .enumerate()
        .map(|(ordinal, (writer, transaction_id))| {
            std::thread::spawn(move || {
                writer
                    .append(
                        &transaction_id,
                        SCHEMA_FINGERPRINT,
                        vec![payload("primary", ordinal as u64 + 1)],
                    )
                    .unwrap()
            })
        })
        .collect::<Vec<_>>();
    let mut committed = threads
        .into_iter()
        .map(|thread| thread.join().unwrap())
        .collect::<Vec<_>>();
    committed.sort_unstable_by_key(|receipt| receipt.position.sequence);

    assert_eq!(
        committed
            .iter()
            .map(|receipt| receipt.position.sequence)
            .collect::<Vec<_>>(),
        [1, 2]
    );
    let entries = operation_logs
        .iter()
        .flat_map(|operations| operations.entries())
        .collect::<Vec<_>>();
    assert_eq!(
        entries
            .iter()
            .filter(|entry| entry.operation == common::StoreOperation::Get
                && entry.path.starts_with("positioned-log/heads/"))
            .count(),
        1
    );
    assert_eq!(
        entries
            .iter()
            .filter(|entry| entry.operation == common::StoreOperation::Put
                && entry.path.starts_with("positioned-log/payloads/"))
            .count(),
        2
    );
    assert_eq!(
        entries
            .iter()
            .filter(|entry| entry.operation == common::StoreOperation::Put
                && entry.path.starts_with("positioned-log/envelopes/"))
            .count(),
        3
    );
    let head_puts = entries
        .iter()
        .filter(|entry| {
            entry.operation == common::StoreOperation::Put
                && entry.path.starts_with("positioned-log/heads/")
        })
        .collect::<Vec<_>>();
    assert_eq!(head_puts.len(), 3);
    assert!(
        head_puts
            .iter()
            .all(|entry| entry.put_mode == Some(common::LoggedPutMode::Update))
    );
    assert!(entries.iter().all(|entry| {
        entry.put_mode != Some(common::LoggedPutMode::Overwrite)
            && entry.operation != common::StoreOperation::List
            && entry.operation != common::StoreOperation::Head
            && entry.path != "collection/CURRENT"
    }));
    let snapshot = PositionedLogWriter::open("memory:///positioned-log", base, 7)
        .unwrap()
        .reader()
        .snapshot()
        .unwrap();
    assert_eq!(snapshot.transactions.len(), 2);
    assert_eq!(
        snapshot
            .transactions
            .iter()
            .map(|envelope| envelope.position.sequence)
            .collect::<Vec<_>>(),
        [1, 2]
    );
}

#[test]
fn append_uses_parallel_payload_wave_then_one_conditional_head_cas() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (traced, operations) = common::FaultInjectingObjectStore::new(inner).with_operation_log();
    let (traced, concurrency) = traced
        .with_latency(Duration::from_millis(10))
        .with_put_concurrency_probe();
    let writer = create_writer(Arc::new(traced), 7);
    operations.clear();

    let committed = writer
        .append(
            "tx-7",
            SCHEMA_FINGERPRINT,
            vec![payload("primary", 11), payload("named-image", 12)],
        )
        .unwrap();

    assert_eq!(committed.position.sequence, 1);
    let entries = operations.entries();
    let head = entries
        .iter()
        .position(|entry| entry.path.starts_with("positioned-log/heads/"))
        .unwrap();
    assert!(entries[..head].iter().all(|entry| {
        entry.path.starts_with("positioned-log/payloads/")
            || entry.path.starts_with("positioned-log/envelopes/")
    }));
    assert_eq!(head, entries.len() - 1);
    assert_eq!(entries[head].put_mode, Some(common::LoggedPutMode::Update));
    assert!(
        concurrency.peak() > 1,
        "immutable wave did not overlap PUTs"
    );
    assert_eq!(committed.requests.gets, 0);
    assert_eq!(committed.requests.heads, 0);
    assert_eq!(committed.requests.lists, 0);
    assert_eq!(committed.requests.puts, 4);
    assert!(
        entries
            .iter()
            .all(|entry| entry.path != "collection/CURRENT")
    );
}

#[test]
fn shared_payload_checksum_is_created_once_without_an_append_path_get() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (traced, operations) = common::FaultInjectingObjectStore::new(inner).with_operation_log();
    let writer = create_writer(Arc::new(traced), 7);
    operations.clear();
    let bytes = arrow_payload(11);

    let committed = writer
        .append(
            "shared-payload",
            SCHEMA_FINGERPRINT,
            vec![
                PositionedMutationPayloadInput {
                    modality: PositionedMutationModality::PrimaryDense,
                    role: "primary".to_owned(),
                    format: PositionedPayloadFormat::ArrowIpc,
                    bytes: bytes.clone(),
                    rows: 1,
                },
                PositionedMutationPayloadInput {
                    modality: PositionedMutationModality::NamedDense,
                    role: "named".to_owned(),
                    format: PositionedPayloadFormat::ArrowIpc,
                    bytes,
                    rows: 1,
                },
            ],
        )
        .unwrap();

    assert_eq!(committed.requests.gets, 0);
    assert_eq!(committed.requests.puts, 3);
    assert_eq!(
        operations.count_matching(|operation, path| {
            operation == common::StoreOperation::Put && path.starts_with("positioned-log/payloads/")
        }),
        1
    );
}

#[test]
fn existing_content_addressed_payload_is_verified_and_reported_separately() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (traced, operations) = common::FaultInjectingObjectStore::new(inner).with_operation_log();
    let writer = create_writer(Arc::new(traced), 7);
    let bytes = arrow_payload(11);
    writer
        .append(
            "collision-first",
            SCHEMA_FINGERPRINT,
            vec![PositionedMutationPayloadInput {
                modality: PositionedMutationModality::PrimaryDense,
                role: "primary".to_owned(),
                format: PositionedPayloadFormat::ArrowIpc,
                bytes: bytes.clone(),
                rows: 1,
            }],
        )
        .unwrap();
    operations.clear();

    let committed = writer
        .append(
            "collision-second",
            SCHEMA_FINGERPRINT,
            vec![PositionedMutationPayloadInput {
                modality: PositionedMutationModality::PrimaryDense,
                role: "primary".to_owned(),
                format: PositionedPayloadFormat::ArrowIpc,
                bytes,
                rows: 1,
            }],
        )
        .unwrap();
    assert_eq!(committed.requests.gets, 1);
    assert_eq!(committed.requests.puts, 3);
    assert_eq!(committed.requests.heads, 0);
    assert_eq!(committed.requests.lists, 0);
    assert_eq!(
        operations.count_matching(|operation, path| {
            operation == common::StoreOperation::Get && path.starts_with("positioned-log/payloads/")
        }),
        1
    );
    let entries = operations.entries();
    assert!(entries.iter().all(|entry| {
        entry.put_mode != Some(common::LoggedPutMode::Overwrite)
            && entry.path != "collection/CURRENT"
    }));
}

#[test]
fn retry_returns_the_same_position_without_duplicate_visibility() {
    let writer = create_writer(Arc::new(InMemory::new()), 7);
    let first = writer
        .append("tx-7", SCHEMA_FINGERPRINT, vec![payload("primary", 11)])
        .unwrap();
    let retry = writer
        .append("tx-7", SCHEMA_FINGERPRINT, vec![payload("primary", 11)])
        .unwrap();

    assert_eq!(retry, first);
    assert_eq!(writer.reader().snapshot().unwrap().transactions.len(), 1);
}

#[test]
fn unchanged_snapshot_reads_heads_but_performs_no_envelope_gets() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (traced, operations) = common::FaultInjectingObjectStore::new(inner).with_operation_log();
    let writer = create_writer(Arc::new(traced), 7);
    writer
        .append("tx-7", SCHEMA_FINGERPRINT, vec![payload("primary", 11)])
        .unwrap();
    let snapshot = writer.reader().snapshot().unwrap();
    operations.clear();

    let unchanged = writer
        .reader()
        .snapshot_if_changed(&snapshot.head_checksums)
        .unwrap();

    assert!(unchanged.is_none());
    assert_eq!(
        operations.count_matching(|operation, path| {
            operation == common::StoreOperation::Get
                && path.starts_with("positioned-log/envelopes/")
        }),
        0
    );
    assert_eq!(
        operations.count_matching(|operation, path| {
            operation == common::StoreOperation::Get && path.starts_with("positioned-log/heads/")
        }),
        usize::from(SOURCE_SHARD_COUNT)
    );
}

#[test]
fn checkpoint_preserves_recent_idempotence_then_evicts_exactly_at_sixty_four() {
    let writer = create_writer(Arc::new(InMemory::new()), 7);
    let shard = 3;
    let ids = transaction_ids_for_shard(shard, MAX_RECENT_COMMIT_RECEIPTS_PER_SHARD + 1);
    let mut first = None;
    for (ordinal, transaction_id) in ids.iter().enumerate() {
        let committed = writer
            .append(
                transaction_id,
                SCHEMA_FINGERPRINT,
                vec![payload("primary", ordinal as u64 + 1)],
            )
            .unwrap();
        if ordinal == 0 {
            first = Some(committed.clone());
        }
        writer
            .checkpoint_materialized_through(shard, committed.position.sequence, ordinal as u64 + 1)
            .unwrap();
        if ordinal + 1 == MAX_RECENT_COMMIT_RECEIPTS_PER_SHARD {
            let oldest = writer
                .append(&ids[0], SCHEMA_FINGERPRINT, vec![payload("primary", 1)])
                .unwrap();
            assert_eq!(oldest.position, first.as_ref().unwrap().position);
        }
    }

    let appended_again = writer
        .append(&ids[0], SCHEMA_FINGERPRINT, vec![payload("primary", 1)])
        .unwrap();
    assert_ne!(appended_again.position, first.unwrap().position);
}

#[test]
fn stale_cached_receipt_refreshes_after_external_recent_eviction() {
    let base: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    create_writer(Arc::clone(&base), 7);
    let stale =
        PositionedLogWriter::open("memory:///positioned-log", Arc::clone(&base), 7).unwrap();
    let shard = 13;
    let ids = transaction_ids_for_shard(shard, MAX_RECENT_COMMIT_RECEIPTS_PER_SHARD + 1);
    let first = stale
        .append(&ids[0], SCHEMA_FINGERPRINT, vec![payload("primary", 1)])
        .unwrap();
    let external =
        PositionedLogWriter::open("memory:///positioned-log", Arc::clone(&base), 7).unwrap();
    external
        .checkpoint_materialized_through(shard, first.position.sequence, 1)
        .unwrap();
    for (ordinal, transaction_id) in ids.iter().enumerate().skip(1) {
        let committed = external
            .append(
                transaction_id,
                SCHEMA_FINGERPRINT,
                vec![payload("primary", ordinal as u64 + 1)],
            )
            .unwrap();
        external
            .checkpoint_materialized_through(shard, committed.position.sequence, ordinal as u64 + 1)
            .unwrap();
    }

    let reused = stale
        .append(
            &ids[0],
            SCHEMA_FINGERPRINT,
            vec![payload("changed", 10_000)],
        )
        .unwrap();
    assert!(reused.position.sequence > first.position.sequence);
}

#[test]
fn stale_full_cache_refreshes_after_external_checkpoint() {
    let base: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    create_writer(Arc::clone(&base), 7);
    let stale =
        PositionedLogWriter::open("memory:///positioned-log", Arc::clone(&base), 7).unwrap();
    let shard = 17;
    let ids = transaction_ids_for_shard(shard, MAX_PENDING_ENVELOPES_PER_SHARD + 1);
    let mut last = None;
    for (ordinal, transaction_id) in ids[..MAX_PENDING_ENVELOPES_PER_SHARD].iter().enumerate() {
        last = Some(
            stale
                .append(
                    transaction_id,
                    SCHEMA_FINGERPRINT,
                    vec![payload("primary", ordinal as u64 + 1)],
                )
                .unwrap(),
        );
    }
    let external =
        PositionedLogWriter::open("memory:///positioned-log", Arc::clone(&base), 7).unwrap();
    external
        .checkpoint_materialized_through(shard, last.unwrap().position.sequence, 1)
        .unwrap();

    let committed = stale
        .append(
            &ids[MAX_PENDING_ENVELOPES_PER_SHARD],
            SCHEMA_FINGERPRINT,
            vec![payload("primary", 100)],
        )
        .unwrap();
    assert_eq!(committed.position.sequence, 65);
}

#[test]
fn checkpoint_refreshes_when_writer_opened_before_external_append() {
    let base: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    create_writer(Arc::clone(&base), 7);
    let stale =
        PositionedLogWriter::open("memory:///positioned-log", Arc::clone(&base), 7).unwrap();
    let external =
        PositionedLogWriter::open("memory:///positioned-log", Arc::clone(&base), 7).unwrap();
    let committed = external
        .append(
            "external-before-checkpoint",
            SCHEMA_FINGERPRINT,
            vec![payload("primary", 1)],
        )
        .unwrap();

    stale
        .checkpoint_materialized_through(committed.position.shard, committed.position.sequence, 1)
        .unwrap();
    assert!(stale.reader().snapshot().unwrap().transactions.is_empty());
}

#[test]
fn concurrent_checkpoint_generation_race_converges_on_one_materialized_prefix() {
    let base: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let initializer = create_writer(Arc::clone(&base), 7);
    let committed = initializer
        .append(
            "checkpoint-race",
            SCHEMA_FINGERPRINT,
            vec![payload("primary", 1)],
        )
        .unwrap();
    let shard = committed.position.shard;
    let sequence = committed.position.sequence;
    let barrier = Arc::new(Barrier::new(2));
    let threads = (0..2)
        .map(|_| {
            let wrapped = common::FaultInjectingObjectStore::new(Arc::clone(&base))
                .with_put_barrier(Arc::clone(&barrier), |operation, path| {
                    operation == common::StoreOperation::Put
                        && path.as_ref().starts_with("positioned-log/heads/")
                });
            let writer =
                PositionedLogWriter::open("memory:///positioned-log", Arc::new(wrapped), 7)
                    .unwrap();
            std::thread::spawn(move || writer.checkpoint_materialized_through(shard, sequence, 7))
        })
        .collect::<Vec<_>>();
    for thread in threads {
        thread.join().unwrap().unwrap();
    }
    let snapshot = PositionedLogWriter::open("memory:///positioned-log", base, 7)
        .unwrap()
        .reader()
        .snapshot()
        .unwrap();
    assert_eq!(
        snapshot.materialized_sequences[usize::from(shard)],
        sequence
    );
    assert_eq!(
        snapshot.materialized_collection_generations[usize::from(shard)],
        7
    );
}

#[test]
fn retryable_head_failure_without_receipt_rebases_and_retries() {
    let base: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    create_writer(Arc::clone(&base), 7);
    let faulted = common::FaultInjectingObjectStore::fail_nth_matching(
        Arc::clone(&base),
        1,
        true,
        |operation, path| {
            operation == common::StoreOperation::Put
                && path.as_ref().starts_with("positioned-log/heads/")
        },
    );
    let writer =
        PositionedLogWriter::open("memory:///positioned-log", Arc::new(faulted), 7).unwrap();

    let committed = writer
        .append(
            "retryable-rebase",
            SCHEMA_FINGERPRINT,
            vec![payload("primary", 1)],
        )
        .unwrap();
    assert_eq!(committed.position.sequence, 1);
    assert_eq!(committed.requests.gets, 1);
}

#[test]
fn exact_sixteen_retryable_head_failures_exhaust_the_cas_cap() {
    let base: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    create_writer(Arc::clone(&base), 7);
    let faulted = common::FaultInjectingObjectStore::fail_nth_matching(
        Arc::clone(&base),
        1,
        false,
        |operation, path| {
            operation == common::StoreOperation::Put
                && path.as_ref().starts_with("positioned-log/heads/")
        },
    );
    let (faulted, operations) = faulted.with_operation_log();
    let writer =
        PositionedLogWriter::open("memory:///positioned-log", Arc::new(faulted), 7).unwrap();
    operations.clear();

    let error = writer
        .append(
            "retryable-cap",
            SCHEMA_FINGERPRINT,
            vec![payload("primary", 1)],
        )
        .unwrap_err();
    assert!(error.to_string().contains("concurrent modification"));
    assert_eq!(
        operations.count_matching(|operation, path| {
            operation == common::StoreOperation::Get && path.starts_with("positioned-log/heads/")
        }),
        16
    );
    assert_eq!(
        operations.count_matching(|operation, path| {
            operation == common::StoreOperation::Put && path.starts_with("positioned-log/heads/")
        }),
        16
    );
    assert_eq!(
        operations.count_matching(|operation, path| {
            operation == common::StoreOperation::Put
                && path.starts_with("positioned-log/envelopes/")
        }),
        1
    );
    assert_eq!(
        operations.count_matching(|operation, path| {
            operation == common::StoreOperation::Put && path.starts_with("positioned-log/payloads/")
        }),
        1
    );
}

#[test]
fn pending_count_bound_rejects_the_sixty_fifth_transaction_before_any_put() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (traced, operations) = common::FaultInjectingObjectStore::new(inner).with_operation_log();
    let writer = create_writer(Arc::new(traced), 7);
    let ids = transaction_ids_for_shard(5, MAX_PENDING_ENVELOPES_PER_SHARD + 1);
    for (ordinal, transaction_id) in ids[..MAX_PENDING_ENVELOPES_PER_SHARD].iter().enumerate() {
        writer
            .append(
                transaction_id,
                SCHEMA_FINGERPRINT,
                vec![payload("primary", ordinal as u64 + 1)],
            )
            .unwrap();
    }
    operations.clear();

    let error = writer
        .append(
            &ids[MAX_PENDING_ENVELOPES_PER_SHARD],
            SCHEMA_FINGERPRINT,
            vec![payload("primary", 65)],
        )
        .unwrap_err();

    assert!(error.to_string().contains("backpressure"));
    assert_eq!(
        operations.count_matching(|operation, _| operation == common::StoreOperation::Put),
        0
    );
}

#[test]
fn invalid_inputs_and_conflicting_retry_fail_before_publication() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (traced, operations) = common::FaultInjectingObjectStore::new(inner).with_operation_log();
    let writer = create_writer(Arc::new(traced), 7);
    operations.clear();
    for (transaction_id, fingerprint, payloads) in [
        ("", SCHEMA_FINGERPRINT, vec![payload("primary", 1)]),
        ("tx", "schema-a", vec![payload("primary", 1)]),
        ("tx", SCHEMA_FINGERPRINT, vec![payload("", 1)]),
        ("empty-payload", SCHEMA_FINGERPRINT, Vec::new()),
    ] {
        assert!(
            writer
                .append(transaction_id, fingerprint, payloads)
                .is_err()
        );
    }
    assert_eq!(
        operations.count_matching(|operation, _| operation == common::StoreOperation::Put),
        0
    );

    writer
        .append("same-id", SCHEMA_FINGERPRINT, vec![payload("primary", 2)])
        .unwrap();
    let conflict = writer
        .append("same-id", SCHEMA_FINGERPRINT, vec![payload("primary", 3)])
        .unwrap_err();
    assert!(conflict.to_string().contains("conflict"));
}

#[test]
fn payload_and_envelope_failures_remain_invisible() {
    for failed_prefix in ["positioned-log/payloads/", "positioned-log/envelopes/"] {
        let base: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let initializer = create_writer(Arc::clone(&base), 7);
        drop(initializer);
        let faulted = common::FaultInjectingObjectStore::fail_nth_matching(
            Arc::clone(&base),
            1,
            false,
            move |operation, path| {
                operation == common::StoreOperation::Put && path.as_ref().starts_with(failed_prefix)
            },
        );
        let writer =
            PositionedLogWriter::open("memory:///positioned-log", Arc::new(faulted), 7).unwrap();

        assert!(
            writer
                .append("tx-fail", SCHEMA_FINGERPRINT, vec![payload("primary", 1)])
                .is_err()
        );
        assert!(writer.reader().snapshot().unwrap().transactions.is_empty());
    }
}

#[test]
fn lost_successful_head_response_is_reconciled_by_exact_receipt() {
    let base: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    create_writer(Arc::clone(&base), 7);
    let faulted = common::FaultInjectingObjectStore::accept_then_fail_nth_put(
        Arc::clone(&base),
        1,
        |operation, path| {
            operation == common::StoreOperation::Put
                && path.as_ref().starts_with("positioned-log/heads/")
        },
    );
    let (faulted, operations) = faulted.with_operation_log();
    let writer =
        PositionedLogWriter::open("memory:///positioned-log", Arc::new(faulted), 7).unwrap();
    operations.clear();

    let committed = writer
        .append("tx-lost", SCHEMA_FINGERPRINT, vec![payload("primary", 1)])
        .unwrap();

    assert_eq!(committed.position.sequence, 1);
    assert_eq!(committed.requests.gets, 1);
    assert_eq!(committed.requests.puts, 3);
    assert_eq!(committed.requests.heads, 0);
    assert_eq!(committed.requests.lists, 0);
    let entries = operations.entries();
    assert_eq!(
        entries
            .iter()
            .filter(|entry| entry.operation == common::StoreOperation::Get
                && entry.path.starts_with("positioned-log/heads/"))
            .count(),
        1
    );
    assert_eq!(
        entries
            .iter()
            .filter(|entry| entry.operation == common::StoreOperation::Put)
            .count(),
        3
    );
    assert!(entries.iter().all(|entry| {
        entry.put_mode != Some(common::LoggedPutMode::Overwrite)
            && entry.operation != common::StoreOperation::List
            && entry.operation != common::StoreOperation::Head
            && entry.path != "collection/CURRENT"
    }));
    assert_eq!(writer.reader().snapshot().unwrap().transactions.len(), 1);
}

#[test]
fn cold_open_reports_exact_sixty_four_head_gets() {
    let base: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    create_writer(Arc::clone(&base), 7);
    let (_writer, requests) =
        PositionedLogWriter::open_with_report("memory:///positioned-log", base, 7).unwrap();
    assert_eq!(requests.gets, u64::from(SOURCE_SHARD_COUNT));
    assert_eq!(requests.puts, 0);
    assert_eq!(requests.heads, 0);
    assert_eq!(requests.lists, 0);
}

#[test]
fn lost_successful_head_initialization_response_reconciles_exact_empty_head() {
    let base: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let faulted = common::FaultInjectingObjectStore::accept_then_fail_nth_put(
        Arc::clone(&base),
        1,
        |operation, path| {
            operation == common::StoreOperation::Put
                && path.as_ref().starts_with("positioned-log/heads/")
        },
    );
    let (faulted, operations) = faulted.with_operation_log();
    PositionedLogWriter::create("memory:///positioned-log", Arc::new(faulted), 7).unwrap();
    let entries = operations.entries();
    assert_eq!(
        entries
            .iter()
            .filter(|entry| entry.operation == common::StoreOperation::Put)
            .count(),
        usize::from(SOURCE_SHARD_COUNT)
    );
    assert_eq!(
        entries
            .iter()
            .filter(|entry| entry.operation == common::StoreOperation::Get)
            .count(),
        1
    );
    assert!(
        entries
            .iter()
            .filter_map(|entry| entry.put_mode)
            .all(|mode| { mode == common::LoggedPutMode::Create })
    );
}

#[test]
fn concurrent_source_initialization_race_converges_on_exact_empty_heads() {
    let base: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let barrier = Arc::new(Barrier::new(2));
    let threads = (0..2)
        .map(|_| {
            let wrapped = common::FaultInjectingObjectStore::new(Arc::clone(&base))
                .with_put_barrier(Arc::clone(&barrier), |operation, path| {
                    operation == common::StoreOperation::Put
                        && path.as_ref() == "positioned-log/heads/00.json"
                });
            std::thread::spawn(move || {
                PositionedLogWriter::create("memory:///positioned-log", Arc::new(wrapped), 7)
            })
        })
        .collect::<Vec<_>>();
    for thread in threads {
        thread.join().unwrap().unwrap();
    }
    let (_writer, requests) =
        PositionedLogWriter::open_with_report("memory:///positioned-log", base, 7).unwrap();
    assert_eq!(requests.gets, u64::from(SOURCE_SHARD_COUNT));
}

#[test]
fn payload_must_be_a_typed_container_with_truthful_rows() {
    let writer = create_writer(Arc::new(InMemory::new()), 7);
    let malformed = PositionedMutationPayloadInput {
        modality: PositionedMutationModality::PrimaryDense,
        role: "primary".to_owned(),
        format: PositionedPayloadFormat::ArrowIpc,
        bytes: Cursor::new(b"not-arrow".to_vec()).into_inner(),
        rows: 1,
    };
    assert!(
        writer
            .append("tx-bad", SCHEMA_FINGERPRINT, vec![malformed])
            .is_err()
    );

    let mut wrong_rows = payload("primary", 1);
    wrong_rows.rows = 2;
    assert!(
        writer
            .append("tx-rows", SCHEMA_FINGERPRINT, vec![wrong_rows])
            .is_err()
    );
}

#[test]
fn compressed_arrow_payload_is_rejected_before_decompression_or_publication() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (traced, operations) = common::FaultInjectingObjectStore::new(inner).with_operation_log();
    let writer = create_writer(Arc::new(traced), 7);
    operations.clear();
    let error = writer
        .append(
            "compressed-arrow",
            SCHEMA_FINGERPRINT,
            vec![PositionedMutationPayloadInput {
                modality: PositionedMutationModality::PrimaryDense,
                role: "primary".to_owned(),
                format: PositionedPayloadFormat::ArrowIpc,
                bytes: compressed_arrow_payload(1),
                rows: 1,
            }],
        )
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("compressed positioned Arrow IPC")
    );
    assert_eq!(
        operations.count_matching(|operation, _| operation == common::StoreOperation::Put),
        0
    );
}

#[test]
fn exact_payload_and_row_bounds_admit_then_bound_plus_one_rejects_before_put() {
    let writer = create_writer(Arc::new(InMemory::new()), 7);
    let exact_payloads = (0..MAX_PAYLOADS_PER_TRANSACTION)
        .map(|ordinal| payload(format!("role-{ordinal}"), ordinal as u64 + 1))
        .collect();
    writer
        .append("payload-exact", SCHEMA_FINGERPRINT, exact_payloads)
        .unwrap();

    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (traced, operations) = common::FaultInjectingObjectStore::new(inner).with_operation_log();
    let writer = create_writer(Arc::new(traced), 7);
    operations.clear();
    let too_many = (0..=MAX_PAYLOADS_PER_TRANSACTION)
        .map(|ordinal| payload(format!("role-{ordinal}"), ordinal as u64 + 1))
        .collect();
    assert!(
        writer
            .append("payload-over", SCHEMA_FINGERPRINT, too_many)
            .is_err()
    );
    assert_eq!(
        operations.count_matching(|operation, _| operation == common::StoreOperation::Put),
        0
    );

    let exact_rows = PositionedMutationPayloadInput {
        modality: PositionedMutationModality::PrimaryDense,
        role: "rows".to_owned(),
        format: PositionedPayloadFormat::ArrowIpc,
        bytes: arrow_payload_rows(1, MAX_APPEND_ROWS as usize),
        rows: MAX_APPEND_ROWS,
    };
    writer
        .append("rows-exact", SCHEMA_FINGERPRINT, vec![exact_rows])
        .unwrap();

    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (traced, operations) = common::FaultInjectingObjectStore::new(inner).with_operation_log();
    let writer = create_writer(Arc::new(traced), 7);
    operations.clear();
    let excessive_rows = PositionedMutationPayloadInput {
        modality: PositionedMutationModality::PrimaryDense,
        role: "rows".to_owned(),
        format: PositionedPayloadFormat::ArrowIpc,
        bytes: arrow_payload_rows(1, MAX_APPEND_ROWS as usize + 1),
        rows: MAX_APPEND_ROWS + 1,
    };
    assert!(
        writer
            .append("rows-over", SCHEMA_FINGERPRINT, vec![excessive_rows])
            .is_err()
    );
    assert_eq!(
        operations.count_matching(|operation, _| operation == common::StoreOperation::Put),
        0
    );
}

#[test]
fn declared_row_and_aggregate_encoded_bounds_reject_before_container_decode() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (traced, operations) = common::FaultInjectingObjectStore::new(inner).with_operation_log();
    let writer = create_writer(Arc::new(traced), 7);
    operations.clear();

    let row_error = writer
        .append(
            "predecode-row-bound",
            SCHEMA_FINGERPRINT,
            vec![PositionedMutationPayloadInput {
                modality: PositionedMutationModality::PrimaryDense,
                role: "rows".to_owned(),
                format: PositionedPayloadFormat::ArrowIpc,
                bytes: vec![0],
                rows: MAX_APPEND_ROWS + 1,
            }],
        )
        .unwrap_err();
    assert!(row_error.to_string().contains("hard row or byte bound"));

    let first_len = usize::try_from(MAX_APPEND_ENCODED_BYTES / 2).unwrap();
    let second_len = usize::try_from(MAX_APPEND_ENCODED_BYTES / 2 + 1).unwrap();
    let byte_error = writer
        .append(
            "predecode-byte-bound",
            SCHEMA_FINGERPRINT,
            vec![
                PositionedMutationPayloadInput {
                    modality: PositionedMutationModality::PrimaryDense,
                    role: "a".to_owned(),
                    format: PositionedPayloadFormat::ArrowIpc,
                    bytes: vec![0; first_len],
                    rows: 1,
                },
                PositionedMutationPayloadInput {
                    modality: PositionedMutationModality::Sparse,
                    role: "b".to_owned(),
                    format: PositionedPayloadFormat::ArrowIpc,
                    bytes: vec![0; second_len],
                    rows: 1,
                },
            ],
        )
        .unwrap_err();
    assert!(byte_error.to_string().contains("hard row or byte bound"));
    assert_eq!(
        operations.count_matching(|operation, _| operation == common::StoreOperation::Put),
        0
    );
}

#[test]
fn stale_epoch_and_sequence_overflow_fail_closed() {
    let base: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let writer = create_writer(Arc::clone(&base), 7);
    let transaction_id = "stale-epoch";
    let shard = tx_shard(transaction_id);
    let path = format!("positioned-log/heads/{shard:02}.json");
    let mut head = serde_json::from_slice::<serde_json::Value>(&read(&base, &path)).unwrap();
    head["source_epoch"] = 8.into();
    overwrite(&base, &path, serde_json::to_vec(&head).unwrap());
    let error = writer
        .append(
            transaction_id,
            SCHEMA_FINGERPRINT,
            vec![payload("primary", 1)],
        )
        .unwrap_err();
    assert!(error.to_string().contains("epoch"));

    let base: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    create_writer(Arc::clone(&base), 7);
    let transaction_id = "sequence-overflow";
    let shard = tx_shard(transaction_id);
    let path = format!("positioned-log/heads/{shard:02}.json");
    let mut head = serde_json::from_slice::<serde_json::Value>(&read(&base, &path)).unwrap();
    head["durable_sequence"] = u64::MAX.into();
    head["materialized_sequence"] = u64::MAX.into();
    head["materialized_collection_generation"] = 1.into();
    head["evicted_recent_through_collection_generation"] = 1.into();
    head["recent"] = serde_json::Value::Array(
        (u64::MAX - u64::try_from(MAX_RECENT_COMMIT_RECEIPTS_PER_SHARD).unwrap() + 1..=u64::MAX)
            .map(|sequence| {
                serde_json::json!({
                    "transaction_digest": format!("{sequence:064x}"),
                    "request_digest": "a".repeat(64),
                    "envelope_checksum": "b".repeat(64),
                    "sequence": sequence,
                    "rows": 1,
                    "encoded_bytes": 1,
                    "materialized_collection_generation": 1,
                })
            })
            .collect(),
    );
    overwrite(&base, &path, serde_json::to_vec(&head).unwrap());
    let (traced, operations) =
        common::FaultInjectingObjectStore::new(Arc::clone(&base)).with_operation_log();
    let writer =
        PositionedLogWriter::open("memory:///positioned-log", Arc::new(traced), 7).unwrap();
    operations.clear();
    let error = writer
        .append(
            transaction_id,
            SCHEMA_FINGERPRINT,
            vec![payload("primary", 1)],
        )
        .unwrap_err();
    assert!(error.to_string().contains("overflow"));
    assert_eq!(
        operations.count_matching(|operation, _| operation == common::StoreOperation::Put),
        0
    );
}

#[test]
fn corrupt_envelope_is_never_returned_by_a_snapshot() {
    let base: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let writer = create_writer(Arc::clone(&base), 7);
    let committed = writer
        .append("corrupt", SCHEMA_FINGERPRINT, vec![payload("primary", 1)])
        .unwrap();
    let path = format!(
        "positioned-log/envelopes/{}/{}.parquet",
        &committed.envelope_checksum[..2],
        committed.envelope_checksum
    );
    overwrite(&base, &path, b"not parquet".to_vec());

    let error = writer.reader().snapshot().unwrap_err();
    assert!(error.to_string().contains("checksum"));
}

#[test]
fn checkpoint_generation_must_advance_and_cannot_skip_durable_positions() {
    let writer = create_writer(Arc::new(InMemory::new()), 7);
    let committed = writer
        .append(
            "checkpoint",
            SCHEMA_FINGERPRINT,
            vec![payload("primary", 1)],
        )
        .unwrap();
    assert!(
        writer
            .checkpoint_materialized_through(
                committed.position.shard,
                committed.position.sequence + 1,
                1,
            )
            .is_err()
    );
    writer
        .checkpoint_materialized_through(committed.position.shard, committed.position.sequence, 2)
        .unwrap();
    writer
        .checkpoint_materialized_through(committed.position.shard, committed.position.sequence, 2)
        .unwrap();
    assert!(
        writer
            .checkpoint_materialized_through(
                committed.position.shard,
                committed.position.sequence,
                3
            )
            .is_err()
    );
    let snapshot = writer.reader().snapshot().unwrap();
    assert!(snapshot.transactions.is_empty());
    assert_eq!(
        snapshot.materialized_collection_generations[usize::from(committed.position.shard)],
        2
    );
}

#[test]
fn reader_pinned_before_checkpoint_loads_bounded_recent_envelopes() {
    let writer = create_writer(Arc::new(InMemory::new()), 7);
    let committed = writer
        .append(
            "generation-visible",
            SCHEMA_FINGERPRINT,
            vec![payload("primary", 1)],
        )
        .unwrap();
    writer
        .checkpoint_materialized_through(committed.position.shard, committed.position.sequence, 7)
        .unwrap();

    assert!(writer.reader().snapshot().unwrap().transactions.is_empty());
    let pinned_before = writer
        .reader()
        .snapshot_for_collection_generation(6)
        .unwrap();
    assert_eq!(pinned_before.transactions.len(), 1);
    assert_eq!(pinned_before.transactions[0].position, committed.position);
    let pinned_after = writer
        .reader()
        .snapshot_for_collection_generation(7)
        .unwrap();
    assert!(pinned_after.transactions.is_empty());
}

#[test]
fn generation_visibility_fails_closed_after_bounded_recent_eviction() {
    let writer = create_writer(Arc::new(InMemory::new()), 7);
    let shard = 19;
    let ids = transaction_ids_for_shard(shard, MAX_RECENT_COMMIT_RECEIPTS_PER_SHARD + 1);
    for (ordinal, transaction_id) in ids.iter().enumerate() {
        let committed = writer
            .append(
                transaction_id,
                SCHEMA_FINGERPRINT,
                vec![payload("primary", ordinal as u64 + 1)],
            )
            .unwrap();
        writer
            .checkpoint_materialized_through(shard, committed.position.sequence, ordinal as u64 + 1)
            .unwrap();
    }

    let expired = writer
        .reader()
        .snapshot_for_collection_generation(0)
        .unwrap_err();
    assert!(expired.to_string().contains("recent window"));
    let still_bounded = writer
        .reader()
        .snapshot_for_collection_generation(1)
        .unwrap();
    assert_eq!(still_bounded.transactions.len(), 64);
}
