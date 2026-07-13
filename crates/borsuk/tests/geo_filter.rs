#![allow(missing_docs)]

//! Geo-radius metadata filtering: `{"loc": {"$geoRadius": {lat, lon, radius}}}`
//! keeps only records whose `[lat, lon]` point is within the great-circle radius.

use std::collections::BTreeMap;

use borsuk::{
    BorsukIndex, Filter, IndexConfig, SearchOptions, VectorMetric, VectorRecord, metadata_from_json,
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

fn geo_record(id: &str, lat: f64, lon: f64) -> VectorRecord {
    let meta = metadata_from_json(&serde_json::json!({ "loc": [lat, lon] })).unwrap();
    VectorRecord::new(id, vec![0.0, 0.0]).with_metadata(meta)
}

#[test]
fn geo_radius_filter_keeps_points_within_the_radius() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().to_string();
    let mut index = BorsukIndex::create(config(uri)).unwrap();

    // San Francisco, a point ~1.1 km north, and one ~80 km north.
    index
        .add(vec![
            geo_record("sf", 37.7749, -122.4194),
            geo_record("near", 37.7849, -122.4194),
            geo_record("far", 38.5000, -122.4194),
        ])
        .unwrap();

    let center = serde_json::json!({
        "loc": { "$geoRadius": { "lat": 37.7749, "lon": -122.4194, "radius": 2000.0 } }
    });
    let mut within_2km = index
        .search_ids(
            &[0.0, 0.0],
            SearchOptions::exact(10).with_filter(Filter::from_json(&center).unwrap()),
        )
        .unwrap();
    within_2km.sort();
    assert_eq!(
        within_2km,
        ["near", "sf"],
        "2 km radius should include sf and near, exclude far"
    );

    // A tight 500 m radius keeps only the center itself.
    let tight = serde_json::json!({
        "loc": { "$geoRadius": { "lat": 37.7749, "lon": -122.4194, "radius": 500.0 } }
    });
    let within_500m = index
        .search_ids(
            &[0.0, 0.0],
            SearchOptions::exact(10).with_filter(Filter::from_json(&tight).unwrap()),
        )
        .unwrap();
    assert_eq!(within_500m, ["sf"]);
}

#[test]
fn geo_radius_rejects_malformed_specs() {
    // Missing radius.
    let bad = serde_json::json!({ "loc": { "$geoRadius": { "lat": 1.0, "lon": 2.0 } } });
    assert!(Filter::from_json(&bad).is_err());
}
