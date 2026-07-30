#![allow(missing_docs)]

use std::collections::BTreeMap;

use borsuk::{
    BorsukError, BorsukIndex, CompactionOptions, IndexConfig, OpenOptions, SearchOptions,
    VectorMetric, VectorRecord, VectorSpec, WalConfig,
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
                kind: Default::default(),
                element_type: Default::default(),
            },
        )]),
    }
}

#[test]
fn collection_ram_budget_rejects_aggregate_named_manifest_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().to_string();
    let mut collection_config = config(uri.clone());
    collection_config.named_vectors.insert(
        "image".to_string(),
        VectorSpec {
            dimensions: 8,
            metric: VectorMetric::Cosine,
            kind: Default::default(),
            element_type: Default::default(),
        },
    );
    let index = BorsukIndex::create(collection_config).unwrap();
    let root_bytes = index.stats().resident_bytes_estimate;
    let lexical_bytes = index
        .search_with_report(
            &[0.0; 4],
            SearchOptions::exact(1).with_vector_name("lexical"),
        )
        .unwrap()
        .resident_bytes_estimate;
    let image_bytes = index
        .search_with_report(&[0.0; 8], SearchOptions::exact(1).with_vector_name("image"))
        .unwrap()
        .resident_bytes_estimate;
    let aggregate = root_bytes
        .checked_add(lexical_bytes)
        .and_then(|bytes| bytes.checked_add(image_bytes))
        .unwrap();
    let individually_sufficient = root_bytes.max(lexical_bytes).max(image_bytes);
    assert!(aggregate > individually_sufficient);
    drop(index);

    let error = BorsukIndex::open_with_options(
        &uri,
        OpenOptions {
            ram_budget_bytes: Some(individually_sufficient),
            ..OpenOptions::default()
        },
    )
    .unwrap_err();

    assert!(matches!(
        error,
        BorsukError::RamBudgetExceeded {
            resident_bytes,
            budget_bytes,
        } if resident_bytes == aggregate && budget_bytes == individually_sufficient
    ));
}

#[test]
fn collection_ram_budget_rejects_growth_before_snapshot_publication() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().to_string();
    let index = BorsukIndex::create(config(uri.clone())).unwrap();
    let aggregate = index
        .stats()
        .resident_bytes_estimate
        .checked_add(
            index
                .search_with_report(
                    &[0.0; 4],
                    SearchOptions::exact(1).with_vector_name("lexical"),
                )
                .unwrap()
                .resident_bytes_estimate,
        )
        .unwrap();
    drop(index);

    let mut bounded = BorsukIndex::open_with_options(
        &uri,
        OpenOptions {
            ram_budget_bytes: Some(aggregate),
            ..OpenOptions::default()
        },
    )
    .unwrap();
    bounded
        .add(vec![
            VectorRecord::new("budgeted", vec![0.0, 0.0])
                .with_named_vector("lexical", vec![0.0, 0.0, 0.0, 0.0]),
        ])
        .unwrap();

    let error = bounded.flush().unwrap_err();
    assert!(matches!(
        error,
        BorsukError::RamBudgetExceeded {
            resident_bytes,
            budget_bytes,
        } if resident_bytes > budget_bytes && budget_bytes == aggregate
    ));
    drop(bounded);

    let reopened = BorsukIndex::open_with_options(
        &uri,
        OpenOptions {
            ram_budget_bytes: Some(aggregate),
            ..OpenOptions::default()
        },
    )
    .unwrap();
    assert_eq!(
        reopened
            .search_ids(&[0.0, 0.0], SearchOptions::exact(1))
            .unwrap(),
        ["budgeted"]
    );
    assert_eq!(
        reopened
            .search_ids(
                &[0.0; 4],
                SearchOptions::exact(1).with_vector_name("lexical"),
            )
            .unwrap(),
        ["budgeted"]
    );
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
fn child_modalities_reject_disabled_collection_wal() {
    let dir = tempfile::tempdir().unwrap();
    let error = BorsukIndex::create_with_wal(
        config(dir.path().to_string_lossy().into_owned()),
        WalConfig::disabled(),
    )
    .unwrap_err();

    assert!(
        error
            .to_string()
            .contains("require the collection WAL for atomic multimodal publication"),
        "{error}"
    );
}

#[test]
fn collection_snapshot_reopens_without_modality_current_pointers() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().to_string();
    let mut index = BorsukIndex::create(config(uri.clone())).unwrap();
    index
        .add(vec![
            VectorRecord::new("nearest-primary", vec![0.0, 0.0])
                .with_named_vector("lexical", vec![9.0, 9.0, 9.0, 9.0]),
            VectorRecord::new("nearest-named", vec![8.0, 8.0])
                .with_named_vector("lexical", vec![0.0, 0.0, 0.0, 0.0]),
        ])
        .unwrap();
    drop(index);

    assert!(dir.path().join("collection/CURRENT").is_file());
    assert!(!dir.path().join("CURRENT").exists());
    assert!(!dir.path().join("vectors/lexical/CURRENT").exists());

    let reopened = BorsukIndex::open(&uri).unwrap();
    assert_eq!(
        reopened
            .search_ids(&[0.0, 0.0], SearchOptions::exact(1))
            .unwrap(),
        ["nearest-primary"]
    );
    assert_eq!(
        reopened
            .search_ids(
                &[0.0, 0.0, 0.0, 0.0],
                SearchOptions::exact(1).with_vector_name("lexical"),
            )
            .unwrap(),
        ["nearest-named"]
    );
}

#[test]
fn collection_open_requires_root_current() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().to_string();
    drop(BorsukIndex::create(config(uri.clone())).unwrap());
    std::fs::remove_file(dir.path().join("collection/CURRENT")).unwrap();

    let error = BorsukIndex::open(&uri).unwrap_err();

    assert!(matches!(error, borsuk::BorsukError::IndexNotFound(_)));
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
