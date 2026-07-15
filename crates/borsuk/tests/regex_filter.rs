#![allow(missing_docs)]

use std::collections::BTreeMap;

use borsuk::{
    BorsukIndex, Filter, IndexConfig, MetaValue, Metadata, SearchOptions, VectorMetric,
    VectorRecord,
};

fn config(uri: String) -> IndexConfig {
    IndexConfig {
        uri,
        metric: VectorMetric::Euclidean,
        dimensions: 2,
        segment_max_vectors: 1,
        ram_budget_bytes: None,
        text: false,
        named_vectors: BTreeMap::new(),
    }
}

fn record(id: &str, name: &str, x: f32) -> VectorRecord {
    VectorRecord::new(id, vec![x, 0.0]).with_metadata(Metadata::from([(
        "name".to_string(),
        MetaValue::Str(name.to_string()),
    )]))
}

#[test]
fn exact_search_across_segments_returns_only_regex_matches() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().to_string();
    let mut index = BorsukIndex::create(config(uri)).unwrap();
    index
        .add(vec![
            record("match-a", "abcz", 0.0),
            record("miss-a", "abc", 1.0),
            record("match-b", "another-z", 2.0),
            record("miss-b", "zebra", 3.0),
        ])
        .unwrap();
    // Flush the default-on WAL so the records land in the multiple on-disk
    // segments this cross-segment regex-filter test asserts on.
    index.flush().unwrap();
    assert!(index.stats().segments > 1);

    let filter = Filter::from_json(&serde_json::json!({"name": {"$regex": "^a.*z$"}})).unwrap();
    let mut ids = index
        .search_ids(&[0.0, 0.0], SearchOptions::exact(10).with_filter(filter))
        .unwrap();
    ids.sort();

    assert_eq!(ids, ["match-a", "match-b"]);
}
