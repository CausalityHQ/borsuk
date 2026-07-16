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
    BorsukIndex, CompactionOptions, GarbageCollectionOptions, IndexConfig, SearchOptions,
    VectorMetric, VectorRecord, WalConfig,
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

/// Recursively count regular files under `root/dir` (0 when absent). Used to
/// prove the heavy per-segment leaf artifacts (dense-vector sidecars, graphs) do
/// NOT exist until compaction builds them.
fn file_count(root: &std::path::Path, dir: &str) -> usize {
    fn walk(path: &std::path::Path) -> usize {
        let Ok(entries) = std::fs::read_dir(path) else {
            return 0;
        };
        entries
            .filter_map(|entry| entry.ok())
            .map(|entry| {
                let path = entry.path();
                if path.is_dir() { walk(&path) } else { 1 }
            })
            .sum()
    }
    let target = root.join(dir);
    if target.exists() { walk(&target) } else { 0 }
}

/// Every visible record's `(id, vector)` pair, sorted by id, for cross-path
/// result equality checks.
fn all_records_sorted(index: &BorsukIndex) -> Vec<(String, Vec<f32>)> {
    let mut rows = index
        .list_records(0, 100_000)
        .unwrap()
        .into_iter()
        .map(|(id, vector, _)| (id.to_string(), vector))
        .collect::<Vec<_>>();
    rows.sort_by(|a, b| a.0.cmp(&b.0));
    rows
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

/// The ingest-side double-build is gone: a bulk `add` with the default WAL is
/// APPEND-ONLY — it materializes NO segment, dense-vector sidecar, or graph, only
/// WAL objects. Compaction is then the SINGLE build that materializes indexed
/// segments directly from the tail records (no discarded intermediate L0). The
/// result is identical, record-for-record, to the disabled-WAL synchronous
/// segment-per-add path fed the same records.
#[test]
fn bulk_add_is_append_only_and_compaction_is_the_single_build() {
    // Enough records (segment_max 4) that the OLD threshold-flush path would have
    // eagerly built many L0 segments (each with its dense-vector sidecar + graph)
    // during ingest. Under the default cap the whole batch stays in the tail.
    let records = (0..200)
        .map(|value| VectorRecord::new(format!("r{value:04}"), vec![value as f32, 1.0]))
        .collect::<Vec<_>>();

    // --- WAL-on path: bulk add, then a single compaction. ---
    let wal_dir = tempfile::tempdir().unwrap();
    let wal_uri = wal_dir.path().to_string_lossy().to_string();
    let mut wal_index = BorsukIndex::create(config(wal_uri)).unwrap();
    assert!(wal_index.manifest().wal_enabled());
    wal_index.add(records.clone()).unwrap();

    // Append-only: only WAL objects on disk. The expensive per-record leaf
    // artifacts (segment Parquet, dense-vector sidecar, graph) do NOT exist yet —
    // the write path built none of them.
    assert!(
        wal_object_count(wal_dir.path()) > 0,
        "bulk add must publish at least one WAL object"
    );
    assert!(
        !wal_index.manifest().wal_frontier_is_empty(),
        "the whole batch stays in the un-flushed tail (no auto-flush)"
    );
    assert_eq!(
        segment_count(wal_dir.path()),
        0,
        "no L0 segment is built on the append-only write path"
    );
    assert_eq!(
        file_count(wal_dir.path(), "vectors"),
        0,
        "no dense-vector sidecar is built on the write path"
    );
    assert_eq!(
        file_count(wal_dir.path(), "graphs"),
        0,
        "no per-segment graph is built on the write path"
    );
    // Read-your-writes over the un-flushed tail before any build.
    assert_eq!(wal_index.stats().records, records.len());
    assert_eq!(
        wal_index
            .search_ids(&[0.0, 1.0], SearchOptions::exact(1))
            .unwrap(),
        ["r0000"]
    );

    // Compaction is the single build: it consumes the tail records directly and
    // materializes the indexed cells (their sidecars/graphs). No intermediate L0
    // was ever read (there was none).
    let report = wal_index
        .compact(CompactionOptions {
            max_segments: None,
            ..CompactionOptions::default()
        })
        .unwrap();
    assert!(report.compacted);
    assert_eq!(
        report.records_rewritten,
        records.len(),
        "every record is rewritten exactly once by the single build"
    );
    assert!(
        wal_index.manifest().wal_frontier_is_empty(),
        "compaction empties the frontier — the tail is now in the built cells"
    );
    assert!(
        file_count(wal_dir.path(), "vectors") > 0,
        "compaction is where the dense-vector sidecars are built"
    );

    // --- Disabled-WAL path: the classic synchronous segment-per-add, same records. ---
    let sync_dir = tempfile::tempdir().unwrap();
    let sync_uri = sync_dir.path().to_string_lossy().to_string();
    let mut sync_index =
        BorsukIndex::create_with_wal(config(sync_uri), WalConfig::disabled()).unwrap();
    sync_index.add(records.clone()).unwrap();
    sync_index
        .compact(CompactionOptions {
            max_segments: None,
            ..CompactionOptions::default()
        })
        .unwrap();

    // Identical visible record set, record-for-record.
    assert_eq!(
        all_records_sorted(&wal_index),
        all_records_sorted(&sync_index),
        "WAL-on single-build results must equal the disabled-WAL synchronous path"
    );

    // Identical exact top-k for a spread of queries.
    for value in [0usize, 37, 128, 199] {
        let query = vec![value as f32, 1.0];
        assert_eq!(
            wal_index
                .search_ids(&query, SearchOptions::exact(5))
                .unwrap(),
            sync_index
                .search_ids(&query, SearchOptions::exact(5))
                .unwrap(),
            "exact top-k diverged from the synchronous path for query {value}"
        );
    }
}

/// MVCC across the un-flushed tail survives the DIRECT compaction that consumes
/// the tail: an upsert supersedes the earlier add, and a delete suppresses its id,
/// with the tail folded straight into the single build (no L0 materialize). The
/// compacted, frontier-cleared index reflects exactly the newest generation per
/// id and the deletions — identical to the disabled-WAL synchronous path.
#[test]
fn direct_compaction_of_the_tail_preserves_upsert_and_delete_supersede() {
    let build = |sync: bool| -> Vec<(String, Vec<f32>)> {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_string_lossy().to_string();
        let wal = if sync {
            WalConfig::disabled()
        } else {
            WalConfig::default()
        };
        let mut index = BorsukIndex::create_with_wal(config(uri), wal).unwrap();
        index
            .add(
                (0..40)
                    .map(|v| VectorRecord::new(format!("r{v:03}"), vec![v as f32, 0.0]))
                    .collect(),
            )
            .unwrap();
        // Upsert a fresh generation for some ids, delete others — all while (for the
        // WAL-on case) the originals are still only in the un-flushed tail.
        index
            .upsert(vec![
                VectorRecord::new("r005", vec![500.0, 0.0]),
                VectorRecord::new("r020", vec![520.0, 0.0]),
            ])
            .unwrap();
        index.delete(["r010", "r030"]).unwrap();
        if !sync {
            // The whole history is still in the tail — nothing flushed.
            assert!(!index.manifest().wal_frontier_is_empty());
            assert_eq!(segment_count(dir.path()), 0);
        }
        index
            .compact(CompactionOptions {
                max_segments: None,
                ..CompactionOptions::default()
            })
            .unwrap();
        if !sync {
            assert!(index.manifest().wal_frontier_is_empty());
        }
        // Deleted ids are gone; upserted ids carry the newest vector.
        assert!(index.get_vector("r010").unwrap().is_none());
        assert!(index.get_vector("r030").unwrap().is_none());
        assert_eq!(index.get_vector("r005").unwrap(), Some(vec![500.0, 0.0]));
        assert_eq!(index.get_vector("r020").unwrap(), Some(vec![520.0, 0.0]));
        assert_eq!(index.stats().records, 38, "40 added, 2 deleted");
        all_records_sorted(&index)
    };

    assert_eq!(
        build(false),
        build(true),
        "direct-tail compaction must equal the disabled-WAL synchronous path"
    );
}
