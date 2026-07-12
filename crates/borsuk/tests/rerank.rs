#![allow(missing_docs)]

//! Built-in retrieve -> rerank -> top-k pipeline (`search_rerank`).

use std::collections::BTreeMap;

use borsuk::{
    BorsukIndex, IndexConfig, MetaValue, Metadata, SearchOptions, VectorMetric, VectorRecord,
};

fn config(uri: String) -> IndexConfig {
    IndexConfig {
        uri,
        metric: VectorMetric::Euclidean,
        dimensions: 2,
        segment_max_vectors: 8,
        ram_budget_bytes: None,
        text: false,
        named_vectors: BTreeMap::new(),
    }
}

fn with_priority(id: &str, x: f32, priority: i64) -> VectorRecord {
    let mut m = Metadata::new();
    m.insert("priority".to_string(), MetaValue::Int(priority));
    VectorRecord::new(id, vec![x, 0.0]).with_metadata(m)
}

#[test]
fn search_rerank_reorders_candidates_by_the_reranker_score() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().to_string();
    let mut index = BorsukIndex::create(config(uri)).unwrap();

    // Vector-nearest order for a query at 0 is a,b,c,d,e; priority is the reverse.
    index
        .add(vec![
            with_priority("a", 0.0, 1),
            with_priority("b", 1.0, 2),
            with_priority("c", 2.0, 3),
            with_priority("d", 3.0, 4),
            with_priority("e", 4.0, 5),
        ])
        .unwrap();

    // Retrieve the 5 nearest with metadata, then rerank by descending priority
    // and keep the top 3.
    let reranked = index
        .search_rerank(
            &[0.0, 0.0],
            SearchOptions::exact(5).with_include_metadata(true),
            3,
            |hits| {
                hits.iter()
                    .map(
                        |hit| match hit.metadata.as_ref().and_then(|m| m.get("priority")) {
                            Some(MetaValue::Int(p)) => *p as f32,
                            _ => 0.0,
                        },
                    )
                    .collect()
            },
        )
        .unwrap();

    let ids: Vec<String> = reranked.iter().map(|h| h.id.to_string()).collect();
    // Highest priority first: e, d, c.
    assert_eq!(ids, ["e", "d", "c"]);
    // distance carries -score, so it is monotonically increasing (best first).
    assert!(reranked.windows(2).all(|w| w[0].distance <= w[1].distance));
}

#[test]
fn search_rerank_rejects_a_score_count_mismatch() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().to_string();
    let mut index = BorsukIndex::create(config(uri)).unwrap();
    index
        .add(vec![with_priority("a", 0.0, 1), with_priority("b", 1.0, 2)])
        .unwrap();

    let err = index
        .search_rerank(&[0.0, 0.0], SearchOptions::exact(2), 2, |_| vec![1.0])
        .unwrap_err();
    assert!(err.to_string().contains("scores for"), "{err}");
}
