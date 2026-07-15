#![allow(missing_docs)]

//! Write-ahead-log (WAL) coverage. BORSUK's WAL is ON by default: a small
//! `add`/`upsert` is appended to an immutable `wal/<version>-<seq>.parquet`
//! object and its frontier is published in the SAME atomic manifest swap, so
//! the record is durable and visible immediately without building a
//! PQ/graph/segment. The un-flushed tail is unioned into every read, respecting
//! MVCC generations and the tombstone overlay, and is flushed into real
//! segments once it crosses a threshold (or on an explicit `flush()`).
//!
//! These tests pin: WAL-off byte-equivalence to the classic path,
//! read-your-writes, upsert/delete superseding a WAL-tail record, threshold and
//! explicit flush (tail empties, results identical, GC reclaims flushed WAL
//! objects while keeping live ones), durability across reopen, snapshot
//! isolation, and read-your-deletes across the WAL.

use std::collections::BTreeMap;
use std::time::Duration;

use borsuk::{
    BorsukIndex, GarbageCollectionOptions, IndexConfig, SearchOptions, VectorMetric, VectorRecord,
    WalConfig,
};

fn config(uri: String) -> IndexConfig {
    IndexConfig {
        uri,
        metric: VectorMetric::Euclidean,
        dimensions: 2,
        segment_max_vectors: 4,
        ram_budget_bytes: None,
        text: false,
        named_vectors: BTreeMap::new(),
    }
}

/// An enabled WAL with a low record threshold so flushes are easy to trigger.
fn small_wal() -> WalConfig {
    WalConfig {
        enabled: true,
        flush_threshold_records: 8,
        flush_threshold_bytes: u64::MAX,
    }
}

/// Count `wal/…parquet` objects currently on disk under the index root.
fn wal_object_count(root: &std::path::Path) -> usize {
    let wal_dir = root.join("wal");
    if !wal_dir.exists() {
        return 0;
    }
    std::fs::read_dir(wal_dir)
        .unwrap()
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.path().extension().is_some_and(|ext| ext == "parquet"))
        .count()
}

fn segment_count(root: &std::path::Path) -> usize {
    let l0 = root.join("segments/L0");
    if !l0.exists() {
        return 0;
    }
    std::fs::read_dir(l0)
        .unwrap()
        .filter_map(|entry| entry.ok())
        .map(|entry| {
            std::fs::read_dir(entry.path())
                .map(|inner| inner.filter_map(|e| e.ok()).count())
                .unwrap_or(0)
        })
        .sum()
}

#[test]
fn wal_disabled_matches_the_classic_segment_per_add_path() {
    // With the WAL explicitly disabled, `add` builds a segment synchronously and
    // writes no WAL object, exactly as before the WAL existed.
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().to_string();
    let mut index = BorsukIndex::create_with_wal(config(uri), WalConfig::disabled()).unwrap();

    index
        .add(vec![
            VectorRecord::new("a", vec![0.0, 0.0]),
            VectorRecord::new("b", vec![1.0, 0.0]),
        ])
        .unwrap();

    assert_eq!(
        wal_object_count(dir.path()),
        0,
        "disabled WAL writes no wal/ object"
    );
    assert!(
        segment_count(dir.path()) > 0,
        "disabled WAL builds a segment per add"
    );
    assert!(index.manifest().wal_frontier_is_empty());
    assert_eq!(
        index
            .search_ids(&[0.0, 0.0], SearchOptions::exact(1))
            .unwrap(),
        ["a"]
    );
}

#[test]
fn wal_is_on_by_default_and_add_writes_a_wal_object_not_a_segment() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().to_string();
    let mut index = BorsukIndex::create(config(uri)).unwrap();
    assert!(index.manifest().wal_enabled(), "WAL is on by default");

    index
        .add(vec![VectorRecord::new("a", vec![0.0, 0.0])])
        .unwrap();

    // One WAL object, no segment yet: the write skipped the PQ/graph/segment build.
    assert_eq!(wal_object_count(dir.path()), 1);
    assert_eq!(segment_count(dir.path()), 0);
}

#[test]
fn read_your_writes_sees_a_wal_added_record_before_any_flush() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().to_string();
    let mut index = BorsukIndex::create_with_wal(config(uri), small_wal()).unwrap();

    index
        .add(vec![VectorRecord::new("a", vec![0.0, 0.0])])
        .unwrap();

    // No flush: the record is still only in the WAL tail, yet every read sees it.
    assert_eq!(segment_count(dir.path()), 0);
    assert_eq!(
        index
            .search_ids(&[0.0, 0.0], SearchOptions::exact(1))
            .unwrap(),
        ["a"]
    );
    assert_eq!(index.get_vector("a").unwrap(), Some(vec![0.0, 0.0]));
    let listed = index.list_records(0, 10).unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].0.to_string(), "a");
}

#[test]
fn upsert_supersedes_a_wal_tail_record_before_flush() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().to_string();
    let mut index = BorsukIndex::create_with_wal(config(uri), small_wal()).unwrap();

    index
        .add(vec![VectorRecord::new("a", vec![0.0, 0.0])])
        .unwrap();
    // Upsert the id while its only copy is still in the WAL tail.
    index
        .upsert(vec![VectorRecord::new("a", vec![9.0, 9.0])])
        .unwrap();
    assert_eq!(segment_count(dir.path()), 0, "still un-flushed");

    // The newer generation wins in the merge: one live "a", the new vector.
    assert_eq!(index.get_vector("a").unwrap(), Some(vec![9.0, 9.0]));
    let hits = index
        .search_ids(&[9.0, 9.0], SearchOptions::exact(10))
        .unwrap();
    assert_eq!(hits.iter().filter(|id| *id == "a").count(), 1);
    assert_eq!(index.list_records(0, 10).unwrap().len(), 1);
}

#[test]
fn delete_hides_a_wal_tail_record_before_flush() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().to_string();
    let mut index = BorsukIndex::create_with_wal(config(uri), small_wal()).unwrap();

    index
        .add(vec![VectorRecord::new("a", vec![0.0, 0.0])])
        .unwrap();
    index.delete(["a"]).unwrap();
    assert_eq!(segment_count(dir.path()), 0, "still un-flushed");

    // Read-your-deletes across the WAL: the just-added tail record is suppressed.
    assert!(index.get_vector("a").unwrap().is_none());
    let hits = index
        .search_ids(&[0.0, 0.0], SearchOptions::exact(10))
        .unwrap();
    assert!(!hits.iter().any(|id| id == "a"));
    assert!(index.list_records(0, 10).unwrap().is_empty());
}

#[test]
fn add_rejects_an_id_already_live_in_the_wal_tail() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().to_string();
    let mut index = BorsukIndex::create_with_wal(config(uri), small_wal()).unwrap();

    index
        .add(vec![VectorRecord::new("a", vec![0.0, 0.0])])
        .unwrap();
    // `add` is insert-only and must see the un-flushed tail copy.
    assert!(
        index
            .add(vec![VectorRecord::new("a", vec![1.0, 1.0])])
            .is_err()
    );
}

#[test]
fn explicit_flush_materializes_the_tail_and_empties_the_frontier() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().to_string();
    let mut index = BorsukIndex::create_with_wal(config(uri), small_wal()).unwrap();

    index
        .add(vec![
            VectorRecord::new("a", vec![0.0, 0.0]),
            VectorRecord::new("b", vec![1.0, 0.0]),
        ])
        .unwrap();
    assert_eq!(wal_object_count(dir.path()), 1);
    assert_eq!(segment_count(dir.path()), 0);
    assert!(!index.manifest().wal_frontier_is_empty());

    index.flush().unwrap();

    // Frontier empties; records are now in real segments; results unchanged.
    assert!(index.manifest().wal_frontier_is_empty());
    assert!(segment_count(dir.path()) > 0);
    assert_eq!(
        index
            .search_ids(&[0.0, 0.0], SearchOptions::exact(2))
            .unwrap(),
        ["a", "b"]
    );
    // A second flush with an empty frontier is a no-op.
    index.flush().unwrap();
    assert!(index.manifest().wal_frontier_is_empty());
}

#[test]
fn crossing_the_record_threshold_auto_flushes() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().to_string();
    // Threshold of 8 records; add 8 in one batch to trip the auto-flush.
    let mut index = BorsukIndex::create_with_wal(config(uri), small_wal()).unwrap();

    let records = (0..8)
        .map(|value| VectorRecord::new(format!("v{value}"), vec![value as f32, 0.0]))
        .collect::<Vec<_>>();
    index.add(records).unwrap();

    // The add crossed the threshold, so the tail was flushed to segments.
    assert!(index.manifest().wal_frontier_is_empty());
    assert!(segment_count(dir.path()) > 0);
    assert_eq!(index.stats().records, 8);
}

#[test]
fn gc_reclaims_flushed_wal_objects_and_keeps_live_ones() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().to_string();
    let mut index = BorsukIndex::create_with_wal(config(uri), small_wal()).unwrap();

    // First write -> one live WAL object.
    index
        .add(vec![VectorRecord::new("a", vec![0.0, 0.0])])
        .unwrap();
    assert_eq!(wal_object_count(dir.path()), 1);

    // Flush -> that WAL object is now obsolete (dropped from the frontier).
    index.flush().unwrap();
    // Second write -> a fresh, live WAL object that GC must NOT touch.
    index
        .add(vec![VectorRecord::new("b", vec![1.0, 0.0])])
        .unwrap();
    assert_eq!(wal_object_count(dir.path()), 2, "one flushed + one live");
    assert!(!index.manifest().wal_frontier_is_empty());

    // GC with zero retention reclaims the flushed WAL object; the live one stays.
    index
        .gc_obsolete_segments(GarbageCollectionOptions {
            dry_run: false,
            min_age: Duration::ZERO,
        })
        .unwrap();
    assert_eq!(
        wal_object_count(dir.path()),
        1,
        "flushed WAL object reclaimed, live WAL object preserved"
    );

    // Both records remain visible: the flushed one via its segment, the live one
    // via the surviving WAL tail.
    assert_eq!(index.get_vector("a").unwrap(), Some(vec![0.0, 0.0]));
    assert_eq!(index.get_vector("b").unwrap(), Some(vec![1.0, 0.0]));
}

#[test]
fn flushed_index_pays_no_wal_read_cost() {
    // Once flushed, the frontier is empty, so reads take zero WAL I/O — a purely
    // read-heavy workload never re-reads WAL objects after a flush.
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().to_string();
    let mut index = BorsukIndex::create_with_wal(config(uri.clone()), small_wal()).unwrap();
    index
        .add(vec![
            VectorRecord::new("a", vec![0.0, 0.0]),
            VectorRecord::new("b", vec![1.0, 0.0]),
        ])
        .unwrap();
    index.flush().unwrap();
    assert!(index.manifest().wal_frontier_is_empty());

    // A fresh handle opens the flushed snapshot: an empty frontier means the read
    // path short-circuits the WAL union entirely.
    let reader = BorsukIndex::open(&uri).unwrap();
    assert!(reader.manifest().wal_frontier_is_empty());
    assert_eq!(
        reader
            .search_ids(&[0.0, 0.0], SearchOptions::exact(2))
            .unwrap(),
        ["a", "b"]
    );
}

#[test]
fn wal_state_is_durable_across_reopen_without_flush() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().to_string();
    {
        let mut index = BorsukIndex::create_with_wal(config(uri.clone()), small_wal()).unwrap();
        index
            .add(vec![VectorRecord::new("a", vec![0.0, 0.0])])
            .unwrap();
        index
            .upsert(vec![VectorRecord::new("a", vec![5.0, 5.0])])
            .unwrap();
        index
            .add(vec![VectorRecord::new("b", vec![1.0, 0.0])])
            .unwrap();
        index.delete(["b"]).unwrap();
        // Drop the handle WITHOUT flushing: the un-flushed WAL frontier was
        // published in the manifest, so it must survive on the object store alone.
    }

    let reopened = BorsukIndex::open(&uri).unwrap();
    assert!(!reopened.manifest().wal_frontier_is_empty());
    // The upserted vector and the deletion both survive the reopen, purely from
    // the recovered WAL tail.
    assert_eq!(reopened.get_vector("a").unwrap(), Some(vec![5.0, 5.0]));
    assert!(reopened.get_vector("b").unwrap().is_none());
    let hits = reopened
        .search_ids(&[5.0, 5.0], SearchOptions::exact(5))
        .unwrap();
    assert_eq!(hits.iter().filter(|id| *id == "a").count(), 1);
    assert!(!hits.iter().any(|id| id == "b"));
}

#[test]
fn wal_upsert_and_delete_survive_flush_then_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().to_string();
    {
        let mut index = BorsukIndex::create_with_wal(config(uri.clone()), small_wal()).unwrap();
        index
            .add(vec![
                VectorRecord::new("a", vec![0.0, 0.0]),
                VectorRecord::new("b", vec![1.0, 0.0]),
            ])
            .unwrap();
        index
            .upsert(vec![VectorRecord::new("a", vec![5.0, 5.0])])
            .unwrap();
        index.delete(["b"]).unwrap();
        // Materialize everything into segments, then drop.
        index.flush().unwrap();
        assert!(index.manifest().wal_frontier_is_empty());
    }

    let reopened = BorsukIndex::open(&uri).unwrap();
    assert!(reopened.manifest().wal_frontier_is_empty());
    assert_eq!(reopened.get_vector("a").unwrap(), Some(vec![5.0, 5.0]));
    assert!(reopened.get_vector("b").unwrap().is_none());
    let hits = reopened
        .search_ids(&[5.0, 5.0], SearchOptions::exact(5))
        .unwrap();
    assert_eq!(hits.iter().filter(|id| *id == "a").count(), 1);
    assert!(!hits.iter().any(|id| id == "b"));
}

#[test]
fn readers_are_snapshot_isolated_over_the_wal_tail() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().to_string();
    let mut writer = BorsukIndex::create_with_wal(config(uri.clone()), small_wal()).unwrap();
    writer
        .add(vec![VectorRecord::new("a", vec![0.0, 0.0])])
        .unwrap();

    // A reader opened now pins the manifest (and thus the WAL frontier) it saw.
    let reader = BorsukIndex::open(&uri).unwrap();
    assert_eq!(
        reader
            .search_ids(&[0.0, 0.0], SearchOptions::exact(5))
            .unwrap(),
        ["a"]
    );

    // The writer appends another WAL record, publishing a new frontier.
    writer
        .add(vec![VectorRecord::new("b", vec![1.0, 0.0])])
        .unwrap();

    // The existing reader still observes its frozen frontier snapshot — no "b".
    let seen = reader
        .search_ids(&[1.0, 0.0], SearchOptions::exact(5))
        .unwrap();
    assert!(
        !seen.iter().any(|id| id == "b"),
        "snapshot-isolated reader saw a write committed after it opened: {seen:?}"
    );

    // A freshly opened reader advances to the newest published frontier.
    let fresh = BorsukIndex::open(&uri).unwrap();
    let advanced = fresh
        .search_ids(&[1.0, 0.0], SearchOptions::exact(5))
        .unwrap();
    assert!(advanced.iter().any(|id| id == "b"));
}

#[test]
fn reopen_after_each_wal_write_yields_a_consistent_snapshot() {
    // Every published version is self-consistent even when writes stay in the
    // WAL: opening a brand-new handle at any point reflects exactly the writes
    // committed so far (atomic publication of the frontier — never half-applied).
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().to_string();
    let mut writer = BorsukIndex::create_with_wal(config(uri.clone()), small_wal()).unwrap();

    for i in 0..6 {
        writer
            .add(vec![VectorRecord::new(
                format!("r{i}"),
                vec![i as f32, 0.0],
            )])
            .unwrap();
        let snapshot = BorsukIndex::open(&uri).unwrap();
        assert_eq!(
            snapshot.list_records(0, 1000).unwrap().len(),
            i + 1,
            "snapshot after WAL write {i} was inconsistent"
        );
    }
}
