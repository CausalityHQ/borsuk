#![allow(missing_docs)]

use std::env;

use borsuk::{
    BorsukIndex, CompactionOptions, GarbageCollectionOptions, IndexConfig, SearchMode,
    SearchOptions, VectorMetric, VectorRecord,
};
use uuid::Uuid;

#[test]
fn s3_compatible_index_round_trip_when_configured() {
    let Ok(base_uri) = env::var("BORSUK_S3_TEST_URI") else {
        return;
    };
    let uri = format!("{}/{}", base_uri.trim_end_matches('/'), Uuid::new_v4());

    let mut index = BorsukIndex::create(IndexConfig {
        uri: uri.clone(),
        metric: VectorMetric::Euclidean,
        dimensions: 2,
        segment_max_vectors: 2,
    })
    .unwrap();

    index
        .add(vec![
            VectorRecord::new("near", vec![0.0, 0.0]),
            VectorRecord::new("neighbor", vec![0.0, 0.1]),
            VectorRecord::new("mid", vec![5.0, 0.0]),
            VectorRecord::new("far", vec![10.0, 0.0]),
        ])
        .unwrap();

    let cache = tempfile::tempdir().unwrap();
    let mut reopened =
        BorsukIndex::open_with_cache(&uri, Some(cache.path().to_path_buf())).unwrap();
    let hits = reopened
        .search(&[0.1, 0.0], SearchOptions::exact(1))
        .unwrap();

    assert_eq!(hits[0].id, "near");

    let report = reopened
        .search_with_report(
            &[0.04, 0.07],
            SearchOptions {
                k: 1,
                mode: SearchMode::Approx {
                    eps: None,
                    max_segments: None,
                    max_latency_ms: None,
                    max_candidates_per_segment: Some(2),
                },
            },
        )
        .unwrap();
    assert_eq!(report.hits[0].id, "neighbor");
    assert!(report.graph_bytes_read > 0);
    assert!(cache.path().join("segments").exists());
    assert!(cache.path().join("graphs").exists());

    let compaction = reopened
        .compact(CompactionOptions {
            source_level: 0,
            target_level: 1,
            max_segments: Some(2),
            min_segments: 2,
            target_segment_max_vectors: Some(4),
        })
        .unwrap();
    assert!(compaction.compacted);
    assert_eq!(compaction.segments_written, 1);

    let gc = reopened
        .gc_obsolete_segments(GarbageCollectionOptions { dry_run: true })
        .unwrap();
    assert_eq!(gc.objects_deleted, 0);
    assert!(!gc.candidates.is_empty());
}
