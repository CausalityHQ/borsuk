#![allow(missing_docs)]

use std::path::PathBuf;

use borsuk::{
    BorsukIndex, CompactionOptions, GarbageCollectionOptions, IndexConfig, LeafMode, SearchMode,
    SearchOptions, VectorMetric, VectorRecord,
};
use uuid::Uuid;

fn main() -> borsuk::Result<()> {
    let base_uri = std::env::var("BORSUK_S3_TEST_URI").map_err(|_| {
        borsuk::BorsukError::InvalidStorage(
            "set BORSUK_S3_TEST_URI=s3://bucket/prefix before running this example".to_string(),
        )
    })?;
    let uri = format!(
        "{}/rust-example-{}",
        base_uri.trim_end_matches('/'),
        Uuid::new_v4()
    );
    let cache = std::env::temp_dir().join(format!("borsuk-rust-s3-cache-{}", Uuid::new_v4()));

    let mut index = BorsukIndex::create(IndexConfig {
        uri: uri.clone(),
        metric: VectorMetric::Euclidean,
        dimensions: 2,
        segment_max_vectors: 3,
        ram_budget_bytes: None,
    })?;

    index.add(vec![
        VectorRecord::new("entry", vec![0.0, 0.0]),
        VectorRecord::new("true-neighbor", vec![0.0, 0.1]),
        VectorRecord::new("routing-decoy", vec![0.1, -0.1]),
        VectorRecord::new("far", vec![100.0, 100.0]),
        VectorRecord::new("far2", vec![110.0, 100.0]),
        VectorRecord::new("far3", vec![100.0, 110.0]),
    ])?;

    let mut reopened = BorsukIndex::open_with_cache(&uri, Some(PathBuf::from(&cache)))?;
    let report = reopened.search_with_report(
        &[0.04, 0.07],
        SearchOptions {
            k: 1,
            mode: SearchMode::Approx {
                leaf_mode: LeafMode::Graph,
                eps: None,
                max_segments: None,
                max_bytes: None,
                max_latency_ms: None,
                routing_page_overfetch: None,
                max_candidates_per_segment: Some(2),
            },
            guaranteed_recall: false,
            prefetch_depth: borsuk::DEFAULT_SEARCH_PREFETCH_DEPTH,
        },
    )?;
    assert_eq!(report.hits[0].id, "true-neighbor");
    assert_eq!(reopened.get_vector("true-neighbor")?, Some(vec![0.0, 0.1]));
    assert!(report.bytes_read > 0);
    assert!(report.graph_bytes_read > 0);
    assert!(report.object_cache_misses > 0);

    let compaction = reopened.compact(CompactionOptions {
        source_level: 0,
        target_level: 1,
        max_segments: Some(2),
        min_segments: 2,
        target_segment_max_vectors: Some(6),
    })?;
    assert!(compaction.compacted);

    let gc = reopened.gc_obsolete_segments(GarbageCollectionOptions {
        dry_run: true,
        min_age: std::time::Duration::ZERO,
    })?;
    assert!(gc.dry_run);
    assert!(!gc.candidates.is_empty());

    println!(
        "uri={uri}\thit={}\tbytes_read={}\tgraph_bytes_read={}\tobject_cache_misses={}\tcompacted={}\tgc_candidates={}",
        report.hits[0].id,
        report.bytes_read,
        report.graph_bytes_read,
        report.object_cache_misses,
        compaction.compacted,
        gc.candidates.len()
    );

    if cache.exists() {
        std::fs::remove_dir_all(&cache).map_err(|source| borsuk::BorsukError::Io {
            path: cache,
            source,
        })?;
    }

    Ok(())
}
