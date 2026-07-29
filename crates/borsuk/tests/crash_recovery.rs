#![allow(missing_docs)]

//! Crash-recovery harness. BORSUK commits foreground mutations through
//! cell-local lane heads and one transaction marker; catalog/flush changes use
//! `CURRENT`. Every immutable payload is content-addressed and checksum-verified.
//! This harness simulates the
//! ways a process can die (or a store can rot) around those boundaries and pins
//! the durability contract on reopen:
//!
//!   * every ACKNOWLEDGED record is still searchable,
//!   * no phantom record (a write whose commit never landed) appears,
//!   * a torn/garbage metadata table or WAL object surfaces a graceful
//!     `BorsukError` — never a panic, and never silent data loss,
//!   * a torn TRAILING WAL object does not take down the earlier, independently
//!     committed records.
//!
//! It drives the engine through the real object-store operations and then
//! interrupts by mutating the persisted bytes directly (torn writes, lost
//! objects, un-swapped `CURRENT`), all deterministic — no wall clock, no `rand`.

use std::collections::BTreeMap;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;

use borsuk::{
    BorsukError, BorsukIndex, CellWalConfig, CellWalRunInput, CellWalRunKind, CellWalStore,
    IndexConfig, LogicalCellId, SearchOptions, VectorMetric, VectorRecord, WalConfig,
};
use bytes::Bytes;
use futures_util::TryStreamExt;
use object_store::{
    GetOptions, ObjectStore, PutOptions, PutPayload, memory::InMemory, path::Path as ObjectPath,
};

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

fn config(uri: &str) -> IndexConfig {
    IndexConfig {
        uri: uri.to_string(),
        metric: VectorMetric::Euclidean,
        dimensions: 2,
        segment_max_vectors: 4,
        ram_budget_bytes: None,
        text: false,
        named_vectors: BTreeMap::new(),
    }
}

/// A WAL with a threshold high enough that a modest batch of writes stays in the
/// un-flushed tail (so the reopen path recovers from WAL objects, not segments).
fn big_tail_wal() -> WalConfig {
    WalConfig {
        enabled: true,
        flush_threshold_runs: 1_000_000,
        flush_threshold_records: 1_000_000,
        flush_threshold_bytes: u64::MAX,
        collection_flush_threshold_bytes: u64::MAX,
    }
}

/// Snapshot every `(path, bytes)` in the store.
fn snapshot(store: &Arc<dyn ObjectStore>) -> Vec<(ObjectPath, Vec<u8>)> {
    runtime().block_on(async {
        let metas = store.list(None).try_collect::<Vec<_>>().await.unwrap();
        let mut out = Vec::with_capacity(metas.len());
        for meta in metas {
            let bytes = store
                .get_opts(&meta.location, GetOptions::default())
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap()
                .to_vec();
            out.push((meta.location, bytes));
        }
        out
    })
}

/// Rebuild a store from a snapshot, applying `mutate` to each `(path, bytes)`;
/// returning `None` from `mutate` drops that object.
fn rebuild(
    objects: &[(ObjectPath, Vec<u8>)],
    mut mutate: impl FnMut(&ObjectPath, &[u8]) -> Option<Vec<u8>>,
) -> Arc<dyn ObjectStore> {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    runtime().block_on(async {
        for (path, bytes) in objects {
            if let Some(new_bytes) = mutate(path, bytes) {
                store
                    .put_opts(
                        path,
                        PutPayload::from(Bytes::from(new_bytes)),
                        PutOptions::default(),
                    )
                    .await
                    .unwrap();
            }
        }
    });
    store
}

/// The relative immutable record-run paths in the cell WAL, sorted by path.
fn wal_paths(objects: &[(ObjectPath, Vec<u8>)]) -> Vec<ObjectPath> {
    let mut paths: Vec<ObjectPath> = objects
        .iter()
        .map(|(path, _)| path.clone())
        .filter(|path| {
            let s = path.as_ref();
            s.starts_with("cells/")
                && s.contains("/wal/")
                && s.contains("/runs/")
                && s.ends_with(".parquet")
        })
        .collect();
    paths.sort_by(|a, b| a.as_ref().cmp(b.as_ref()));
    paths
}

/// Build an index that leaves `count` records in the un-flushed WAL tail (plus a
/// couple of upserts/deletes for MVCC coverage), returning its store + expected
/// live id set.
fn build_tail_index(uri: &str, count: usize) -> (Arc<dyn ObjectStore>, Vec<String>) {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let mut index = BorsukIndex::create_with_object_store_and_wal(
        Arc::clone(&inner),
        config(uri),
        big_tail_wal(),
    )
    .unwrap();
    // Each add is a separate WAL object (one PUT per append).
    for value in 0..count {
        index
            .add(vec![VectorRecord::new(
                format!("r{value:04}"),
                vec![value as f32, 0.0],
            )])
            .unwrap();
    }
    // MVCC in the tail: supersede one id, delete another.
    index
        .upsert(vec![VectorRecord::new("r0001", vec![99.0, 99.0])])
        .unwrap();
    index.delete(["r0002"]).unwrap();
    assert!(
        !index.manifest().wal_frontier_is_empty(),
        "the batch must stay in the un-flushed tail"
    );

    let live: Vec<String> = (0..count)
        .map(|value| format!("r{value:04}"))
        .filter(|id| id != "r0002")
        .collect();
    (inner, live)
}

/// Assert the index at `store` recovers exactly `expected_live` ids, with the
/// upserted vector and no phantom/deleted ids — purely from the recovered WAL.
fn assert_recovers(store: &Arc<dyn ObjectStore>, uri: &str, expected_live: &[String]) {
    let index = BorsukIndex::open_with_object_store(Arc::clone(store), uri).unwrap();
    let mut listed: Vec<String> = index
        .list_records(0, 1_000_000)
        .unwrap()
        .into_iter()
        .map(|(id, _, _)| id.to_string())
        .collect();
    listed.sort();
    let mut expected = expected_live.to_vec();
    expected.sort();
    assert_eq!(listed, expected, "recovered id set diverged from the ACKs");
    // The upsert survived.
    assert_eq!(index.get_vector("r0001").unwrap(), Some(vec![99.0, 99.0]));
    // The delete survived.
    assert!(index.get_vector("r0002").unwrap().is_none());
    // A search finds a known-live record and never the deleted one.
    let hits = index
        .search_ids(&[0.0, 0.0], SearchOptions::exact(1000))
        .unwrap();
    assert!(hits.iter().any(|id| id == "r0000"));
    assert!(!hits.iter().any(|id| id == "r0002"));
}

#[test]
fn drop_after_wal_appends_before_flush_recovers_every_ack() {
    // Process death after N appends, before any flush: the un-flushed frontier was
    // published in each manifest, so a fresh open recovers every acknowledged
    // record from the WAL objects alone.
    let uri = "memory:///crash-tail";
    let (store, live) = build_tail_index(uri, 12);
    // A brand-new handle simulates a cold restart after the writer died.
    assert_recovers(&store, uri, &live);
}

#[test]
fn torn_trailing_wal_object_does_not_lose_earlier_committed_records() {
    // A single WAL object at the TAIL is corrupted after commit (bit rot / partial
    // replication). The engine must not lose — or crash on — the earlier,
    // independently committed records. It must surface a graceful error or skip
    // the torn object; it must NOT panic and must NOT silently drop good records.
    let uri = "memory:///crash-torn-tail";
    let (store, _live) = build_tail_index(uri, 12);
    let objects = snapshot(&store);
    let wal = wal_paths(&objects);
    assert!(
        wal.len() >= 3,
        "expected several WAL objects, got {}",
        wal.len()
    );
    let newest = wal.last().unwrap().clone();

    // Truncate the newest WAL object to half its bytes (a torn trailing write).
    let torn = rebuild(&objects, |path, bytes| {
        if *path == newest {
            Some(bytes[..bytes.len() / 2].to_vec())
        } else {
            Some(bytes.to_vec())
        }
    });

    // Contract: opening still succeeds (metadata-only), and reading either
    // surfaces a clean BorsukError OR returns the earlier committed records —
    // never a panic, never a wrong-but-silent answer.
    let result = catch_unwind(AssertUnwindSafe(|| {
        BorsukIndex::open_with_object_store(Arc::clone(&torn), uri)
            .and_then(|index| index.list_records(0, 1_000_000))
    }));
    assert!(
        result.is_ok(),
        "a torn trailing WAL object PANICKED the read path"
    );
    match result.unwrap() {
        Ok(records) => {
            // If reads recover gracefully, the earlier committed records must be
            // present (they were committed independently of the torn tail object).
            let ids: Vec<String> = records
                .into_iter()
                .map(|(id, _, _)| id.to_string())
                .collect();
            assert!(
                ids.iter().any(|id| id == "r0000"),
                "earlier committed record r0000 was silently lost to a torn trailing WAL object"
            );
        }
        Err(err) => {
            // Erroring cleanly is acceptable; a ChecksumMismatch on the torn object
            // is exactly the content-addressed guard doing its job.
            assert_eq!(
                err.code(),
                "checksum_mismatch",
                "unexpected error class for a torn WAL object: {err:?}"
            );
        }
    }
}

#[test]
fn byte_mutated_wal_object_is_caught_by_checksum_not_a_wrong_answer() {
    // A committed WAL object whose bytes are mutated in place (same length) must
    // be caught by its content-addressed checksum on read — never decoded into a
    // wrong-but-plausible record set.
    let uri = "memory:///crash-mutated-wal";
    let (store, _live) = build_tail_index(uri, 8);
    let objects = snapshot(&store);
    let wal = wal_paths(&objects);
    let target = wal[wal.len() / 2].clone();

    let mutated = rebuild(&objects, |path, bytes| {
        if *path == target {
            let mut bytes = bytes.to_vec();
            // Flip a deterministic interior byte (keep the length identical).
            let len = bytes.len();
            if len > 20 {
                bytes[len / 3] ^= 0x5A;
            }
            Some(bytes)
        } else {
            Some(bytes.to_vec())
        }
    });

    match BorsukIndex::open_with_object_store(Arc::clone(&mutated), uri) {
        Err(err) => assert_eq!(err.code(), "checksum_mismatch", "{err:?}"),
        Ok(index) => {
            let err = index
                .list_records(0, 1_000_000)
                .expect_err("a byte-mutated WAL object must be rejected, not silently accepted");
            assert_eq!(err.code(), "checksum_mismatch", "{err:?}");
            // And it never panics on the search path either.
            let result = catch_unwind(AssertUnwindSafe(|| {
                let _ = index.search_ids(&[0.0, 0.0], SearchOptions::exact(5));
            }));
            assert!(result.is_ok(), "search over a mutated WAL object panicked");
        }
    }
}

#[test]
fn prepared_cell_run_without_commit_marker_is_invisible() {
    let uri = "memory:///crash-uncommitted-cell-run";
    let (store, live_before) = build_tail_index(uri, 6);
    let wal = CellWalStore::new(
        Arc::clone(&store),
        uri,
        CellWalConfig::default(),
        b"crashed-writer".to_vec(),
    )
    .unwrap();
    wal.prepare_transaction(
        "never-committed",
        &[CellWalRunInput {
            cell: LogicalCellId::new(1, 0),
            kind: CellWalRunKind::Records,
            metadata: Vec::new(),
            bytes: b"uncommitted garbage must never be decoded".to_vec(),
            record_count: 1,
            extension: "parquet".to_string(),
        }],
    )
    .unwrap();
    assert_recovers(&store, uri, &live_before);
}

#[test]
fn torn_manifest_table_surfaces_a_clean_error_on_open() {
    // The manifest table is read (and checksum-validated against CURRENT) on
    // EVERY open, including the O(1) paged/metadata open. A torn (truncated)
    // current-version manifest table must therefore surface a clean BorsukError
    // on open — never a panic, never a wrong manifest. (The routing and pivots
    // tables are on the RESIDENT read path, not the paged one; their corruption
    // is covered by the format-fuzz harness, which confirms no panic there.)
    let uri = "memory:///crash-torn-manifest";
    let (store, _live) = build_tail_index(uri, 6);
    let objects = snapshot(&store);

    // Each write publishes a new version, so several versioned manifest tables
    // coexist on disk. CURRENT references the HIGHEST version — corrupt that one
    // (the lexically-largest path), otherwise we'd truncate an orphaned old table
    // CURRENT no longer points to.
    let target = objects
        .iter()
        .map(|(path, _)| path.clone())
        .filter(|path| path.as_ref().starts_with("manifests/"))
        .max_by(|a, b| a.as_ref().cmp(b.as_ref()))
        .expect("a manifest table must exist");
    let torn = rebuild(&objects, |path, bytes| {
        if *path == target {
            Some(bytes[..bytes.len() / 2].to_vec())
        } else {
            Some(bytes.to_vec())
        }
    });
    let result = catch_unwind(AssertUnwindSafe(|| {
        BorsukIndex::open_with_object_store(Arc::clone(&torn), uri)
            .and_then(|index| index.list_records(0, 10).map(|_| ()))
    }));
    assert!(
        result.is_ok(),
        "a torn manifest table PANICKED instead of erroring cleanly"
    );
    let outcome: Result<(), BorsukError> = result.unwrap();
    assert!(
        outcome.is_err(),
        "a torn current-version manifest table was silently accepted"
    );
}

#[test]
fn lost_current_pointer_reports_index_not_found() {
    // If the collection root pointer is gone (never written / lost), open must
    // report a clean IndexNotFound — the caller's cue that there is no committed
    // state — not a panic.
    let uri = "memory:///crash-lost-current";
    let (store, _live) = build_tail_index(uri, 4);
    let objects = snapshot(&store);
    let without_current = rebuild(&objects, |path, bytes| {
        if path.as_ref() == "collection/CURRENT" {
            None
        } else {
            Some(bytes.to_vec())
        }
    });
    let err = BorsukIndex::open_with_object_store(Arc::clone(&without_current), uri)
        .expect_err("open with no collection/CURRENT must fail");
    assert_eq!(err.code(), "index_not_found", "{err:?}");
}
