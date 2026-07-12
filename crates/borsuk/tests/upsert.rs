#![allow(missing_docs)]

//! Versioned-upsert (MVCC overwrite) coverage. `upsert` replaces a record by id:
//! reads immediately see the new vector/metadata, the superseded generation is
//! dropped by compaction, and a previously deleted id is revived — the semantics
//! every major vector database exposes on `upsert`.

use std::collections::BTreeMap;

use borsuk::{
    BorsukIndex, CompactionOptions, IndexConfig, MetaValue, Metadata, SearchOptions, VectorKind,
    VectorMetric, VectorRecord, VectorSpec,
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

fn meta(value: i64) -> Metadata {
    let mut m = Metadata::new();
    m.insert("v".to_string(), MetaValue::Int(value));
    m
}

#[test]
fn upsert_overwrites_vector_and_metadata_visible_immediately() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().to_string();
    let mut index = BorsukIndex::create(config(uri)).unwrap();

    index
        .add(vec![
            VectorRecord::new("a", vec![1.0, 0.0]).with_metadata(meta(1)),
        ])
        .unwrap();
    // A near-[1,0] query finds "a".
    assert_eq!(
        index
            .search_ids(&[1.0, 0.0], SearchOptions::exact(1))
            .unwrap(),
        ["a"]
    );

    // Upsert moves "a" to [0,1] with new metadata.
    index
        .upsert(vec![
            VectorRecord::new("a", vec![0.0, 1.0]).with_metadata(meta(2)),
        ])
        .unwrap();

    // The record is fetched at its new location/metadata — no duplicate remains.
    let (vector, metadata) = index.get_record("a").unwrap().unwrap();
    assert_eq!(vector, vec![0.0, 1.0]);
    assert_eq!(metadata.get("v"), Some(&MetaValue::Int(2)));

    // Search near the OLD location no longer returns the stale "a" as nearest,
    // and near the NEW location it does; exactly one "a" is ever returned.
    let near_new = index
        .search_ids(&[0.0, 1.0], SearchOptions::exact(5))
        .unwrap();
    assert_eq!(near_new.iter().filter(|id| *id == "a").count(), 1);
    assert_eq!(near_new[0], "a");
}

#[test]
fn upsert_leaves_a_single_version_after_compaction() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().to_string();
    let mut index = BorsukIndex::create(config(uri)).unwrap();

    index
        .add(vec![VectorRecord::new("a", vec![1.0, 0.0])])
        .unwrap();
    for g in 0..5 {
        index
            .upsert(vec![VectorRecord::new("a", vec![g as f32, 1.0])])
            .unwrap();
    }
    // Repeated upserts must never surface duplicates.
    let hits = index
        .search_ids(&[4.0, 1.0], SearchOptions::exact(10))
        .unwrap();
    assert_eq!(hits.iter().filter(|id| *id == "a").count(), 1);

    index.compact(CompactionOptions::default()).unwrap();

    // After compaction the superseded generations are physically gone: exactly
    // one live "a" remains and it is the newest version.
    let all = index.list_records(0, 100).unwrap();
    assert_eq!(all.iter().filter(|(id, _, _)| *id == "a").count(), 1);
    let (vector, _) = index.get_record("a").unwrap().unwrap();
    assert_eq!(vector, vec![4.0, 1.0]);
}

#[test]
fn upsert_revives_a_deleted_id() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().to_string();
    let mut index = BorsukIndex::create(config(uri)).unwrap();

    index
        .add(vec![VectorRecord::new("a", vec![1.0, 0.0])])
        .unwrap();
    index.delete(["a"]).unwrap();
    assert!(index.get_vector("a").unwrap().is_none());
    // `add` is insert-only and still refuses a deleted id.
    assert!(
        index
            .add(vec![VectorRecord::new("a", vec![9.0, 9.0])])
            .is_err()
    );

    // `upsert` revives it.
    index
        .upsert(vec![VectorRecord::new("a", vec![0.0, 1.0])])
        .unwrap();
    let (vector, _) = index.get_record("a").unwrap().unwrap();
    assert_eq!(vector, vec![0.0, 1.0]);
    assert_eq!(
        index
            .search_ids(&[0.0, 1.0], SearchOptions::exact(1))
            .unwrap(),
        ["a"]
    );
}

#[test]
fn upsert_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().to_string();
    {
        let mut index = BorsukIndex::create(config(uri.clone())).unwrap();
        index
            .add(vec![VectorRecord::new("a", vec![1.0, 0.0])])
            .unwrap();
        index
            .upsert(vec![VectorRecord::new("a", vec![0.0, 1.0])])
            .unwrap();
    }
    let index = BorsukIndex::open(&uri).unwrap();
    let (vector, _) = index.get_record("a").unwrap().unwrap();
    assert_eq!(vector, vec![0.0, 1.0]);
    let hits = index
        .search_ids(&[0.0, 1.0], SearchOptions::exact(5))
        .unwrap();
    assert_eq!(hits.iter().filter(|id| *id == "a").count(), 1);
}

#[test]
fn upsert_replaces_named_and_sparse_named_vectors_in_lockstep() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().to_string();
    let mut config = config(uri);
    config.named_vectors.insert(
        "semantic".to_string(),
        VectorSpec {
            dimensions: 2,
            metric: VectorMetric::Euclidean,
            kind: VectorKind::Dense,
        },
    );
    config.named_vectors.insert(
        "lexical".to_string(),
        VectorSpec {
            dimensions: 16,
            metric: VectorMetric::InnerProduct,
            kind: VectorKind::Sparse,
        },
    );
    let mut index = BorsukIndex::create(config).unwrap();

    index
        .add(vec![
            VectorRecord::new("a", vec![1.0, 0.0])
                .with_named_vector("semantic", vec![1.0, 0.0])
                .with_named_sparse_vector("lexical", vec![1], vec![1.0])
                .unwrap(),
        ])
        .unwrap();

    // Replace every representation of "a" at once.
    index
        .upsert(vec![
            VectorRecord::new("a", vec![0.0, 1.0])
                .with_named_vector("semantic", vec![0.0, 1.0])
                .with_named_sparse_vector("lexical", vec![5], vec![1.0])
                .unwrap(),
        ])
        .unwrap();

    // The dense named leg now matches the new "semantic" vector, with no dup.
    let semantic = index
        .search_ids(
            &[0.0, 1.0],
            SearchOptions::exact(5).with_vector_name("semantic"),
        )
        .unwrap();
    assert_eq!(semantic.iter().filter(|id| *id == "a").count(), 1);
    assert_eq!(semantic[0], "a");

    // The sparse named leg matches the new term 5 and no longer term 1.
    let by_new_term = index
        .search_sparse_named("lexical", vec![5], vec![1.0], 5)
        .unwrap();
    assert_eq!(by_new_term.len(), 1);
    assert_eq!(by_new_term[0].id.to_string(), "a");
    let by_old_term = index
        .search_sparse_named("lexical", vec![1], vec![1.0], 5)
        .unwrap();
    assert!(by_old_term.is_empty());
}

#[test]
fn upsert_inserts_new_ids_like_add() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().to_string();
    let mut index = BorsukIndex::create(config(uri)).unwrap();

    // Upserting brand-new ids behaves as an insert.
    index
        .upsert(vec![
            VectorRecord::new("a", vec![1.0, 0.0]),
            VectorRecord::new("b", vec![0.0, 1.0]),
        ])
        .unwrap();
    assert_eq!(
        index
            .search_ids(&[1.0, 0.0], SearchOptions::exact(1))
            .unwrap(),
        ["a"]
    );
    assert_eq!(
        index
            .search_ids(&[0.0, 1.0], SearchOptions::exact(1))
            .unwrap(),
        ["b"]
    );
}
