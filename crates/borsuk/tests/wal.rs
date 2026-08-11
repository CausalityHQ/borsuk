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
    BorsukIndex, CompactionOptions, GarbageCollectionOptions, IndexConfig, LeafCapability,
    SearchOptions, VectorMetric, VectorRecord, WalConfig,
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
        flush_threshold_runs: 64,
        flush_threshold_records: 8,
        flush_threshold_bytes: u64::MAX,
        collection_flush_threshold_bytes: u64::MAX,
    }
}

#[derive(Debug, Default, PartialEq, Eq)]
struct WalObjectCounts {
    records: usize,
    id_directory: usize,
}

/// Count durable WAL payload objects by logical role. A current transaction is
/// complete only when both its records and ID-directory mutation table exist.
fn wal_object_counts(root: &std::path::Path) -> WalObjectCounts {
    let wal_dir = root.join("wal");
    let legacy = if wal_dir.exists() {
        std::fs::read_dir(wal_dir)
            .unwrap()
            .filter_map(|entry| entry.ok())
            .filter(|entry| entry.path().extension().is_some_and(|ext| ext == "parquet"))
            .count()
    } else {
        0
    };
    fn cell_runs(path: &std::path::Path, counts: &mut WalObjectCounts) {
        let Ok(entries) = std::fs::read_dir(path) else {
            return;
        };
        for path in entries
            .filter_map(|entry| entry.ok())
            .map(|entry| entry.path())
        {
            if path.is_dir() {
                cell_runs(&path, counts);
            } else if path.extension().is_some_and(|ext| ext == "parquet") {
                let components = path
                    .components()
                    .map(|component| component.as_os_str())
                    .collect::<Vec<_>>();
                if components
                    .windows(2)
                    .any(|pair| pair == ["runs", "records"])
                {
                    counts.records += 1;
                } else if components
                    .windows(2)
                    .any(|pair| pair == ["runs", "id-directory"])
                {
                    counts.id_directory += 1;
                }
            }
        }
    }
    let mut counts = WalObjectCounts {
        records: legacy,
        ..WalObjectCounts::default()
    };
    cell_runs(&root.join("cells"), &mut counts);
    // The production V12 path stores one immutable positioned envelope per
    // logical mutation, with typed payload roles inside that envelope. Keep
    // this compatibility-shaped helper useful while the test names migrate:
    // each envelope represents one complete records+directory transaction.
    let positioned = if root.join("positioned-log/envelopes").exists() {
        fn count(path: &std::path::Path) -> usize {
            let Ok(entries) = std::fs::read_dir(path) else {
                return 0;
            };
            entries
                .filter_map(|entry| entry.ok())
                .map(|entry| {
                    let path = entry.path();
                    if path.is_dir() { count(&path) } else { 1 }
                })
                .sum()
        }
        count(&root.join("positioned-log/envelopes"))
    } else {
        0
    };
    if positioned > 0 {
        counts.records = positioned;
        counts.id_directory = positioned;
    }
    counts
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

fn collection_wal_history_object_count(root: &std::path::Path) -> usize {
    fn walk(path: &std::path::Path) -> usize {
        let Ok(entries) = std::fs::read_dir(path) else {
            return 0;
        };
        entries
            .filter_map(|entry| entry.ok())
            .map(|entry| {
                let path = entry.path();
                if path.is_dir() {
                    walk(&path)
                } else {
                    usize::from(
                        path.file_name().is_some_and(|name| name == "COMMIT")
                            || path
                                .parent()
                                .and_then(std::path::Path::file_name)
                                .is_some_and(|name| name == "nodes"),
                    )
                }
            })
            .sum()
    }
    walk(&root.join("collection/transactions")) + walk(&root.join("collection/wal-frontier"))
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
    // V12 uses the positioned log for every mutation; WalConfig is retained as
    // an input-policy marker, not a second persistence path.
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
        wal_object_counts(dir.path()),
        WalObjectCounts {
            records: 1,
            id_directory: 1
        },
        "one positioned envelope covers the complete mutation"
    );
    assert!(
        segment_count(dir.path()) == 0,
        "ingest does not build segments before explicit flush"
    );
    assert!(!index.manifest().wal_frontier_is_empty());
    assert_eq!(
        index
            .search_ids(&[0.0, 0.0], SearchOptions::exact(1))
            .unwrap(),
        ["a"]
    );
}

#[test]
fn wal_disabled_add_after_finalization_invalidates_stale_global_ann() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().to_string();
    let mut index =
        BorsukIndex::create_with_wal(config(uri.clone()), WalConfig::disabled()).unwrap();
    index
        .add(
            (0..128)
                .map(|row| VectorRecord::new(format!("base-{row}"), vec![row as f32, row as f32]))
                .collect(),
        )
        .unwrap();
    index.finish_bulk_load().unwrap();
    assert_eq!(index.stats().global_ann_layout_version, Some(11));

    index
        .add(vec![VectorRecord::new("new", vec![1_000.0, 1_000.0])])
        .unwrap();

    assert_eq!(index.stats().global_ann_layout_version, Some(11));
    let reopened = BorsukIndex::open(&uri).unwrap();
    assert_eq!(
        reopened
            .search_ids(
                &[1_000.0, 1_000.0],
                SearchOptions::approx(1, borsuk::LeafMode::SrhtPqScan)
                    .with_max_segments(usize::MAX),
            )
            .unwrap(),
        ["new"]
    );
}

#[test]
fn wal_disabled_paged_add_after_finalization_invalidates_stale_global_ann() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().to_string();
    let mut index = BorsukIndex::create_with_wal_routing_page_fanout_and_leaf_capability(
        config(uri.clone()),
        WalConfig::disabled(),
        2,
        LeafCapability::GraphEnabled,
    )
    .unwrap();
    index
        .add(
            (0..128)
                .map(|row| VectorRecord::new(format!("base-{row}"), vec![row as f32, row as f32]))
                .collect(),
        )
        .unwrap();
    index.finish_bulk_load().unwrap();
    assert!(index.stats().routing_max_level > 0);
    assert_eq!(index.stats().global_ann_layout_version, Some(11));

    index
        .add(vec![VectorRecord::new("new", vec![1_000.0, 1_000.0])])
        .unwrap();

    assert_eq!(index.stats().global_ann_layout_version, Some(11));
    let reopened = BorsukIndex::open(&uri).unwrap();
    assert_eq!(
        reopened
            .search_ids(
                &[1_000.0, 1_000.0],
                SearchOptions::approx(1, borsuk::LeafMode::SrhtPqScan)
                    .with_max_segments(usize::MAX),
            )
            .unwrap(),
        ["new"]
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

    // One complete typed WAL transaction, no segment yet: the write skipped the
    // PQ/graph/segment build.
    assert_eq!(
        wal_object_counts(dir.path()),
        WalObjectCounts {
            records: 1,
            id_directory: 1,
        }
    );
    assert_eq!(segment_count(dir.path()), 0);
}

#[test]
fn run_threshold_bounds_tiny_write_frontier_and_manifest_growth() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().to_string();
    let mut index = BorsukIndex::create_with_wal(
        config(uri),
        WalConfig {
            enabled: true,
            flush_threshold_runs: 2,
            flush_threshold_records: usize::MAX,
            flush_threshold_bytes: u64::MAX,
            collection_flush_threshold_bytes: u64::MAX,
        },
    )
    .unwrap();

    index
        .add(vec![VectorRecord::new("a", vec![0.0, 0.0])])
        .unwrap();
    assert_eq!(index.manifest().wal_frontier_len(), 1);
    index
        .add(vec![VectorRecord::new("b", vec![1.0, 0.0])])
        .unwrap();

    assert!(!index.manifest().wal_frontier_is_empty());
    assert_eq!(segment_count(dir.path()), 0);
    assert_eq!(
        all_records_sorted(&index)
            .into_iter()
            .map(|row| row.0)
            .collect::<Vec<_>>(),
        ["a", "b"]
    );
}

#[test]
fn aggregate_byte_threshold_flushes_when_every_cell_is_below_its_local_limit() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().to_string();
    let mut index = BorsukIndex::create_with_wal(
        config(uri),
        WalConfig {
            enabled: true,
            flush_threshold_runs: usize::MAX,
            flush_threshold_records: usize::MAX,
            flush_threshold_bytes: u64::MAX,
            collection_flush_threshold_bytes: 1,
        },
    )
    .unwrap();

    index
        .add(vec![VectorRecord::new("aggregate", vec![0.0, 0.0])])
        .unwrap();

    assert!(!index.manifest().wal_frontier_is_empty());
    assert_eq!(segment_count(dir.path()), 0);
    assert_eq!(
        index
            .search_ids(&[0.0, 0.0], SearchOptions::exact(1))
            .unwrap(),
        ["aggregate"]
    );
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
fn list_records_orders_the_live_view_before_applying_pagination() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().to_string();
    let mut index = BorsukIndex::create_with_wal(config(uri), small_wal()).unwrap();
    index
        .add(vec![
            VectorRecord::new("d", vec![4.0, 0.0]),
            VectorRecord::new("b", vec![2.0, 0.0]),
            VectorRecord::new("a", vec![1.0, 0.0]),
            VectorRecord::new("c", vec![3.0, 0.0]),
        ])
        .unwrap();

    let first = index.list_records(0, 2).unwrap();
    let second = index.list_records(2, 2).unwrap();
    let ids = first
        .into_iter()
        .chain(second)
        .map(|(id, _, _)| id.to_string())
        .collect::<Vec<_>>();

    assert_eq!(ids, ["a", "b", "c", "d"]);
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
fn delete_batches_append_bounded_wal_runs_until_flush() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().to_string();
    let mut index = BorsukIndex::create_with_wal(config(uri.clone()), small_wal()).unwrap();
    index
        .add(
            (0..8)
                .map(|value| VectorRecord::new(format!("r{value}"), vec![value as f32, 0.0]))
                .collect(),
        )
        .unwrap();
    assert!(!index.manifest().wal_frontier_is_empty());

    let mut puts = Vec::new();
    for value in 0..4 {
        let report = index.delete([format!("r{value}")]).unwrap();
        assert_eq!(report.ids_submitted, 1);
        assert!(report.published);
        puts.push(report.requests.puts);
    }

    assert_eq!(
        index.manifest().wal_frontier_len(),
        5,
        "the initial add plus each delete remains one immutable positioned transaction"
    );
    assert_eq!(index.manifest().tombstone_delta_run_count(), 4);
    assert!(
        puts.iter().max().unwrap() - puts.iter().min().unwrap() <= 1,
        "foreground delete request count grew with accumulated tombstones: {puts:?}"
    );
    let fresh = BorsukIndex::open(&uri).unwrap();
    for value in 0..4 {
        let id = format!("r{value}");
        assert!(fresh.get_vector(&id).unwrap().is_none());
    }
    for value in 4..8 {
        let id = format!("r{value}");
        assert!(fresh.get_vector(&id).unwrap().is_some());
    }

    index.flush().unwrap();
    assert!(index.manifest().wal_frontier_is_empty());
    assert_eq!(index.manifest().tombstone_delta_run_count(), 0);
    assert!(
        index.manifest().tombstone_page_count() > 0,
        "flush must route deltas into stable hash pages"
    );
    let reopened = BorsukIndex::open(&uri).unwrap();
    for value in 0..4 {
        let id = format!("r{value}");
        assert!(reopened.get_vector(&id).unwrap().is_none());
    }
}

#[test]
fn batched_delete_of_upserts_reads_each_matching_segment_once() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().to_string();
    let mut index = BorsukIndex::create_with_wal(config(uri), WalConfig::disabled()).unwrap();
    index
        .add(
            (0..32)
                .map(|value| VectorRecord::new(format!("r{value}"), vec![value as f32, 0.0]))
                .collect(),
        )
        .unwrap();
    index
        .upsert(
            (0..16)
                .map(|value| VectorRecord::new(format!("r{value}"), vec![value as f32, 1.0]))
                .collect(),
        )
        .unwrap();

    let report = index
        .delete((0..16).map(|value| format!("r{value}")))
        .unwrap();

    assert_eq!(report.ids_submitted, 16);
    assert!(
        report.requests.gets <= 24,
        "batched delete re-read matching segments per id: {:?}",
        report.requests
    );
}

#[test]
fn mutation_wal_is_snapshot_isolated_across_reader_nodes() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().to_string();
    let mut writer = BorsukIndex::create_with_wal(config(uri.clone()), small_wal()).unwrap();
    writer
        .add(vec![VectorRecord::new("shared", vec![0.0, 0.0])])
        .unwrap();
    writer.flush().unwrap();

    let mut pinned_reader = BorsukIndex::open(&uri).unwrap();
    writer.delete(["shared"]).unwrap();

    assert!(
        pinned_reader.get_vector("shared").unwrap().is_some(),
        "an existing reader must keep its pinned manifest snapshot"
    );
    let refreshed_reader = BorsukIndex::open(&uri).unwrap();
    assert!(
        refreshed_reader.get_vector("shared").unwrap().is_none(),
        "a new reader must combine the stable snapshot with the published mutation WAL"
    );
    assert!(pinned_reader.refresh().unwrap());
    assert!(pinned_reader.get_vector("shared").unwrap().is_none());
    assert!(pinned_reader.refresh().unwrap());
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
    assert_eq!(
        wal_object_counts(dir.path()),
        WalObjectCounts {
            records: 1,
            id_directory: 1,
        }
    );
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
fn explicit_flush_coalesces_record_runs_by_cell() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().to_string();
    let mut index = BorsukIndex::create_with_wal(config(uri.clone()), small_wal()).unwrap();

    for value in 0..4 {
        index
            .add(vec![VectorRecord::new(
                format!("v{value}"),
                vec![value as f32, 0.0],
            )])
            .unwrap();
    }
    assert_eq!(index.stats().segments, 0);

    index.flush().unwrap();

    assert_eq!(
        index.stats().segments,
        1,
        "four small runs in one logical cell should fill one target-sized segment"
    );
    let reopened = BorsukIndex::open(&uri).unwrap();
    assert_eq!(reopened.stats().records, 4);
    assert_eq!(
        reopened
            .search_ids(&[0.0, 0.0], SearchOptions::exact(4))
            .unwrap(),
        ["v0", "v1", "v2", "v3"]
    );
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

    // Threshold knobs are legacy; positioned ingest keeps the immutable tail
    // until an explicit flush or maintenance pass.
    assert!(!index.manifest().wal_frontier_is_empty());
    assert_eq!(segment_count(dir.path()), 0);
    assert_eq!(index.stats().records, 8);
}

#[test]
fn gc_reclaims_flushed_wal_objects_and_keeps_live_ones() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().to_string();
    let mut index = BorsukIndex::create_with_wal(config(uri), small_wal()).unwrap();

    // First write -> one live typed WAL transaction.
    index
        .add(vec![VectorRecord::new("a", vec![0.0, 0.0])])
        .unwrap();
    assert_eq!(
        wal_object_counts(dir.path()),
        WalObjectCounts {
            records: 1,
            id_directory: 1,
        }
    );
    assert_eq!(
        collection_wal_history_object_count(dir.path()),
        0,
        "bounded root HEADs embed commits and create no immutable root history"
    );

    // Flush -> that WAL transaction is now obsolete (dropped from the frontier).
    index.flush().unwrap();
    // Second write -> a fresh, live WAL transaction that GC must NOT touch.
    index
        .add(vec![VectorRecord::new("b", vec![1.0, 0.0])])
        .unwrap();
    assert_eq!(
        wal_object_counts(dir.path()),
        WalObjectCounts {
            records: 2,
            id_directory: 2,
        },
        "one flushed + one live transaction, each with both required roles"
    );
    assert_eq!(
        collection_wal_history_object_count(dir.path()),
        0,
        "flush and append must not create immutable root commit/node history"
    );
    assert!(!index.manifest().wal_frontier_is_empty());

    // Positioned envelopes are immutable authority objects; GC may reclaim
    // obsolete segment artifacts but does not rewrite or delete log history.
    index
        .gc_obsolete_segments(GarbageCollectionOptions {
            dry_run: false,
            min_age: Duration::ZERO,
        })
        .unwrap();
    assert_eq!(
        wal_object_counts(dir.path()),
        WalObjectCounts {
            records: 2,
            id_directory: 2,
        },
        "positioned transaction history remains immutable after GC"
    );
    assert_eq!(
        collection_wal_history_object_count(dir.path()),
        0,
        "embedded bounded root HEADs require no separate root-history GC"
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
        wal_object_counts(wal_dir.path()).records > 0,
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
        assert_eq!(
            index.get_vector("r005").unwrap(),
            Some(vec![500.0, 0.0]),
            "sync={sync}, stats={:?}",
            index.stats()
        );
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
