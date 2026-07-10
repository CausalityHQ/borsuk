#![allow(missing_docs)]

use std::collections::BTreeMap;

use borsuk::{
    BorsukIndex, CompactionOptions, IndexConfig, SearchOptions, VectorMetric, VectorRecord,
    VectorSpec,
};

fn config(uri: String) -> IndexConfig {
    IndexConfig {
        uri,
        metric: VectorMetric::Euclidean,
        dimensions: 2,
        segment_max_vectors: 2,
        ram_budget_bytes: None,
        text: false,
        named_vectors: BTreeMap::from([(
            "lexical".to_string(),
            VectorSpec {
                dimensions: 4,
                metric: VectorMetric::Euclidean,
            },
        )]),
    }
}

#[test]
fn named_vector_search_is_independent_and_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().to_string();
    let mut index = BorsukIndex::create(config(uri.clone())).unwrap();

    index
        .add(vec![
            VectorRecord::new("primary-only", vec![0.0, 0.0]),
            VectorRecord::new("lexical-a", vec![10.0, 0.0])
                .with_named_vector("lexical", vec![0.0, 0.0, 0.0, 0.0]),
            VectorRecord::new("lexical-b", vec![20.0, 0.0])
                .with_named_vector("lexical", vec![9.0, 9.0, 9.0, 9.0]),
        ])
        .unwrap();

    assert_eq!(
        index
            .search_ids(&[0.1, 0.0], SearchOptions::exact(1))
            .unwrap(),
        ["primary-only"]
    );
    assert_eq!(
        index
            .search_ids(
                &[8.9, 9.0, 9.1, 9.0],
                SearchOptions::exact(2).with_vector_name("lexical"),
            )
            .unwrap(),
        ["lexical-b", "lexical-a"]
    );

    let reopened = BorsukIndex::open(&uri).unwrap();
    assert_eq!(reopened.stats().named_vectors, ["lexical"]);
    assert_eq!(
        reopened
            .search_ids(&[0.1, 0.0], SearchOptions::exact(1))
            .unwrap(),
        ["primary-only"]
    );
    assert_eq!(
        reopened
            .search_ids(
                &[8.9, 9.0, 9.1, 9.0],
                SearchOptions::exact(1).with_vector_name("lexical"),
            )
            .unwrap(),
        ["lexical-b"]
    );
}

#[test]
fn named_vector_add_rejects_undeclared_and_wrong_dimensions() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().to_string();
    let mut index = BorsukIndex::create(config(uri)).unwrap();

    let undeclared = index
        .add(vec![
            VectorRecord::new("bad-name", vec![0.0, 0.0])
                .with_named_vector("semantic", vec![1.0, 2.0, 3.0, 4.0]),
        ])
        .unwrap_err();
    assert!(
        undeclared.to_string().contains("undeclared named vector"),
        "{undeclared}"
    );

    let wrong_length = index
        .add(vec![
            VectorRecord::new("bad-dims", vec![0.0, 0.0])
                .with_named_vector("lexical", vec![1.0, 2.0, 3.0]),
        ])
        .unwrap_err();
    assert!(
        wrong_length
            .to_string()
            .contains("named vector `lexical` has 3 dimensions"),
        "{wrong_length}"
    );
}

#[test]
fn named_sparse_vector_matches_dense_named_vector() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().to_string();
    let mut index = BorsukIndex::create(config(uri)).unwrap();

    let sparse = VectorRecord::new("sparse", vec![100.0, 0.0])
        .with_named_sparse("lexical", vec![1, 3], vec![2.0, 4.0], 4)
        .unwrap();
    index
        .add(vec![
            sparse,
            VectorRecord::new("dense", vec![200.0, 0.0])
                .with_named_vector("lexical", vec![0.0, 2.1, 0.0, 4.1]),
        ])
        .unwrap();

    assert_eq!(
        index
            .search_ids(
                &[0.0, 2.0, 0.0, 4.0],
                SearchOptions::exact(1).with_vector_name("lexical"),
            )
            .unwrap(),
        ["sparse"]
    );
}

#[test]
fn compaction_applies_to_named_sub_indexes() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().to_string();
    let mut index = BorsukIndex::create(config(uri)).unwrap();

    index
        .add(vec![
            VectorRecord::new("a", vec![0.0, 0.0])
                .with_named_vector("lexical", vec![5.0, 5.0, 5.0, 5.0]),
            VectorRecord::new("b", vec![1.0, 0.0])
                .with_named_vector("lexical", vec![0.0, 0.0, 0.0, 0.0]),
            VectorRecord::new("c", vec![2.0, 0.0])
                .with_named_vector("lexical", vec![9.0, 9.0, 9.0, 9.0]),
        ])
        .unwrap();

    index.compact(CompactionOptions::default()).unwrap();

    assert_eq!(
        index
            .search_ids(
                &[0.1, 0.0, 0.0, 0.0],
                SearchOptions::exact(1).with_vector_name("lexical"),
            )
            .unwrap(),
        ["b"]
    );
}
