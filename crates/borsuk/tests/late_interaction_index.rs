#![allow(missing_docs)]

use std::collections::BTreeMap;

use borsuk::{
    BorsukIndex, CompactionOptions, IndexConfig, LateInteractionSearchOptions, SearchOptions,
    VectorElementType, VectorKind, VectorMetric, VectorRecord, VectorSpec,
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
            "tokens".to_string(),
            VectorSpec {
                dimensions: 2,
                metric: VectorMetric::InnerProduct,
                kind: VectorKind::LateInteraction,
                element_type: VectorElementType::Float16,
            },
        )]),
    }
}

#[test]
fn ordinary_dense_search_does_not_load_unrelated_late_interaction_sidecars() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().to_string();
    let mut index = BorsukIndex::create(config(uri.clone())).unwrap();
    index
        .add(vec![
            record("alpha", [0.0, 0.0], &[[1.0, 0.0]]),
            record("beta", [1.0, 0.0], &[[0.0, 1.0]]),
        ])
        .unwrap();
    index.flush().unwrap();
    let checksum = index.manifest().segments[0].checksum.clone();
    drop(index);

    let late_path = dir.path().join(format!(
        "late-interaction/tokens/{}/{}.arrow",
        &checksum[..2],
        checksum
    ));
    std::fs::remove_file(&late_path).unwrap();

    let reopened = BorsukIndex::open(&uri).unwrap();
    let hits = reopened
        .search_ids(&[0.0, 0.0], SearchOptions::exact(1))
        .unwrap();
    assert_eq!(hits, ["alpha"]);
    assert!(
        reopened
            .search_late_interaction("tokens", vec![vec![1.0, 0.0]], 1)
            .is_err()
    );
}

#[test]
fn repeated_late_queries_reuse_the_bounded_decoded_arrow_batch_cache() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().to_string();
    let mut index = BorsukIndex::create(config(uri.clone())).unwrap();
    index
        .add(vec![
            record("alpha", [0.0, 0.0], &[[1.0, 0.0]]),
            record("beta", [1.0, 0.0], &[[0.0, 1.0]]),
        ])
        .unwrap();
    index.flush().unwrap();
    let checksum = index.manifest().segments[0].checksum.clone();
    drop(index);

    let reopened = BorsukIndex::open(&uri).unwrap();
    let query = vec![vec![1.0, 0.0]];
    assert_eq!(
        ids(&reopened
            .search_late_interaction("tokens", query.clone(), 1)
            .unwrap()),
        ["alpha"]
    );
    let late_path = dir.path().join(format!(
        "late-interaction/tokens/{}/{}.arrow",
        &checksum[..2],
        checksum
    ));
    std::fs::remove_file(late_path).unwrap();

    assert_eq!(
        ids(&reopened
            .search_late_interaction("tokens", query, 1)
            .unwrap()),
        ["alpha"]
    );
}

fn record(id: &str, primary: [f32; 2], tokens: &[[f32; 2]]) -> VectorRecord {
    VectorRecord::new(id, primary.to_vec())
        .with_late_interaction(
            "tokens",
            tokens.iter().map(|token| token.to_vec()).collect(),
        )
        .unwrap()
}

fn ids(hits: &[borsuk::SearchHit]) -> Vec<String> {
    hits.iter().map(|hit| hit.id.to_string()).collect()
}

#[test]
fn token_ann_maxsim_survives_wal_flush_reopen_upsert_and_delete() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().to_string();
    let mut index = BorsukIndex::create(config(uri.clone())).unwrap();
    index
        .add(vec![
            record("alpha", [0.0, 0.0], &[[1.0, 0.0], [0.0, 1.0]]),
            record("beta", [1.0, 0.0], &[[0.7, 0.0], [0.0, 0.7]]),
            record("noise", [2.0, 0.0], &[[-1.0, 0.0], [0.0, -1.0]]),
        ])
        .unwrap();

    let query = vec![vec![1.0, 0.0], vec![0.0, 1.0]];
    let before_flush = index
        .search_late_interaction("tokens", query.clone(), 2)
        .unwrap();
    assert_eq!(ids(&before_flush), ["alpha", "beta"]);
    assert_eq!(before_flush[0].distance, -2.0);
    let bounded = index
        .search_late_interaction_with_report(
            "tokens",
            query.clone(),
            LateInteractionSearchOptions::bounded(2, 2),
        )
        .unwrap();
    assert_eq!(bounded.candidates_per_query_token, Some(2));
    assert_eq!(bounded.query_tokens, 2);
    assert!(bounded.token_hits_considered <= 4);
    assert!(bounded.candidate_entities <= 3);
    assert_eq!(bounded.hits.len(), 2);
    assert_eq!(bounded.wal_cells_examined, 2);
    assert_eq!(bounded.wal_lanes_examined, 2);
    assert_eq!(bounded.wal_runs_examined, 2);
    assert_eq!(bounded.wal_records_examined, 9);
    assert_eq!(bounded.wal_snapshot_retries, 0);
    assert_eq!(
        bounded.collection_resident_bytes,
        index.stats().collection_resident_bytes
    );
    assert!(bounded.retained_bytes <= bounded.retained_capacity_bytes);
    assert!(bounded.retained_peak_bytes <= bounded.retained_capacity_bytes);
    assert!(bounded.transient_bytes <= bounded.transient_capacity_bytes);
    assert!(bounded.transient_peak_bytes <= bounded.transient_capacity_bytes);

    index.flush().unwrap();
    drop(index);
    let mut reopened = BorsukIndex::open(&uri).unwrap();
    assert_eq!(
        ids(&reopened
            .search_late_interaction("tokens", query.clone(), 2)
            .unwrap()),
        ["alpha", "beta"]
    );

    reopened
        .upsert(vec![record(
            "alpha",
            [3.0, 0.0],
            &[[-0.5, 0.0], [0.0, -0.5]],
        )])
        .unwrap();
    assert_eq!(
        ids(&reopened
            .search_late_interaction("tokens", query.clone(), 2)
            .unwrap()),
        ["beta", "alpha"]
    );
    reopened.flush().unwrap();
    reopened.compact(CompactionOptions::default()).unwrap();
    drop(reopened);
    let mut reopened = BorsukIndex::open(&uri).unwrap();
    assert_eq!(
        ids(&reopened
            .search_late_interaction("tokens", query.clone(), 2)
            .unwrap()),
        ["beta", "alpha"]
    );

    reopened.delete(&["beta".to_string()]).unwrap();
    let after_delete = reopened
        .search_late_interaction("tokens", query, 2)
        .unwrap();
    assert_eq!(ids(&after_delete), ["alpha", "noise"]);
    assert!(!after_delete.iter().any(|hit| hit.id.as_str() == "beta"));
}
