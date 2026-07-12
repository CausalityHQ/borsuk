#![allow(missing_docs)]

//! Consistency and durability guarantees. BORSUK publishes each change as a new
//! immutable, content-addressed manifest version and atomically swaps the
//! `CURRENT` pointer to it (a conditional PUT / compare-and-swap on stores that
//! support it). These tests pin the observable contract: read-your-writes within
//! a writer session, snapshot-isolated readers, and durability across reopen.

use std::collections::BTreeMap;

use borsuk::{BorsukIndex, IndexConfig, SearchOptions, VectorMetric, VectorRecord};

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

#[test]
fn read_your_writes_within_a_writer_session() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().to_string();
    let mut index = BorsukIndex::create(config(uri)).unwrap();

    index
        .add(vec![VectorRecord::new("a", vec![0.0, 0.0])])
        .unwrap();
    // The writing handle observes its own committed write immediately.
    assert_eq!(
        index
            .search_ids(&[0.0, 0.0], SearchOptions::exact(1))
            .unwrap(),
        ["a"]
    );
    index
        .upsert(vec![VectorRecord::new("a", vec![9.0, 9.0])])
        .unwrap();
    assert_eq!(index.get_record("a").unwrap().unwrap().0, vec![9.0, 9.0]);
}

#[test]
fn readers_are_snapshot_isolated() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().to_string();
    let mut writer = BorsukIndex::create(config(uri.clone())).unwrap();
    writer
        .add(vec![VectorRecord::new("a", vec![0.0, 0.0])])
        .unwrap();

    // A reader opened now sees the snapshot as of this manifest version.
    let reader = BorsukIndex::open(&uri).unwrap();
    assert_eq!(
        reader
            .search_ids(&[0.0, 0.0], SearchOptions::exact(5))
            .unwrap(),
        ["a"]
    );

    // The writer publishes a new version.
    writer
        .add(vec![VectorRecord::new("b", vec![1.0, 0.0])])
        .unwrap();

    // The existing reader still observes its frozen snapshot — no "b".
    let seen = reader
        .search_ids(&[1.0, 0.0], SearchOptions::exact(5))
        .unwrap();
    assert!(
        !seen.iter().any(|id| id == "b"),
        "reader saw uncommitted-to-it write: {seen:?}"
    );

    // A freshly opened reader advances to the newest published snapshot.
    let fresh = BorsukIndex::open(&uri).unwrap();
    let advanced = fresh
        .search_ids(&[1.0, 0.0], SearchOptions::exact(5))
        .unwrap();
    assert!(advanced.iter().any(|id| id == "b"));
}

#[test]
fn state_is_durable_across_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().to_string();
    {
        let mut index = BorsukIndex::create(config(uri.clone())).unwrap();
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
        // Drop the handle: nothing but the object store persists.
    }

    let reopened = BorsukIndex::open(&uri).unwrap();
    // The upserted vector and the deletion both survive the reopen.
    assert_eq!(reopened.get_record("a").unwrap().unwrap().0, vec![5.0, 5.0]);
    assert!(reopened.get_vector("b").unwrap().is_none());
    let hits = reopened
        .search_ids(&[5.0, 5.0], SearchOptions::exact(5))
        .unwrap();
    assert_eq!(hits.iter().filter(|id| *id == "a").count(), 1);
    assert!(!hits.iter().any(|id| id == "b"));
}

#[test]
fn reopen_after_each_step_always_yields_a_consistent_snapshot() {
    // Every published version is self-consistent: opening a brand-new handle at
    // any point reflects exactly the writes committed so far (atomic publication
    // — never a half-applied manifest).
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().to_string();
    let mut writer = BorsukIndex::create(config(uri.clone())).unwrap();

    for i in 0..8 {
        writer
            .add(vec![VectorRecord::new(
                format!("r{i}"),
                vec![i as f32, 0.0],
            )])
            .unwrap();
        let snapshot = BorsukIndex::open(&uri).unwrap();
        let count = snapshot.list_records(0, 1000).unwrap().len();
        assert_eq!(count, i + 1, "snapshot after write {i} was inconsistent");
    }
}
