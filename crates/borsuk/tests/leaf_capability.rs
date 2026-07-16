#![allow(missing_docs)]

//! Tests for the typed leaf-search capability fixed at index creation.
//!
//! A `PqScanOnly` index skips per-segment graph construction entirely, so it
//! must (a) never write a `graphs/` object, (b) still serve scan search,
//! get_vector, exact search, compaction, and GC correctly, and (c) reject a
//! graph-backed leaf mode at search time with a typed error. A `GraphEnabled`
//! (default) index keeps building graphs and serving graph/hybrid search.

use std::{fs, time::Duration};

use borsuk::{
    BorsukError, BorsukIndex, CompactionOptions, GarbageCollectionOptions, IndexConfig,
    LeafCapability, LeafMode, SearchMode, SearchOptions, VectorMetric, VectorRecord,
};

fn base_config(uri: String) -> IndexConfig {
    IndexConfig {
        uri,
        metric: VectorMetric::Euclidean,
        dimensions: 2,
        segment_max_vectors: 2,
        ram_budget_bytes: None,
        text: false,
        named_vectors: Default::default(),
    }
}

fn collect_files_with_extension(
    root: impl AsRef<std::path::Path>,
    extension: &str,
) -> Vec<std::path::PathBuf> {
    let root = root.as_ref();
    let mut files = Vec::new();
    if !root.exists() {
        return files;
    }
    for entry in fs::read_dir(root).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            files.extend(collect_files_with_extension(&path, extension));
        } else if path.extension().is_some_and(|actual| actual == extension) {
            files.push(path);
        }
    }
    files.sort();
    files
}

fn approx_options(k: usize, leaf_mode: LeafMode) -> SearchOptions {
    SearchOptions {
        k,
        mode: SearchMode::Approx {
            leaf_mode,
            eps: None,
            max_segments: None,
            max_bytes: None,
            max_latency_ms: None,
            routing_page_overfetch: None,
            max_candidates_per_segment: None,
            adaptive_stop: None,
            projected_reads: None,
        },
        guaranteed_recall: false,
        prefetch_depth: 8,
        filter: None,
        include_metadata: false,
        vector_name: String::new(),
    }
}

#[test]
fn pq_scan_only_index_writes_no_graph_objects_yet_serves_reads() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

    let mut index = BorsukIndex::create_with_leaf_capability(
        base_config(uri.clone()),
        LeafCapability::PqScanOnly,
    )
    .unwrap();
    assert_eq!(index.leaf_capability(), LeafCapability::PqScanOnly);

    index
        .add(vec![
            VectorRecord::new("a", vec![0.0, 0.0]),
            VectorRecord::new("b", vec![1.0, 0.0]),
            VectorRecord::new("c", vec![8.0, 0.0]),
            VectorRecord::new("d", vec![9.0, 0.0]),
        ])
        .unwrap();
    index.flush().unwrap();

    // No graph objects were written for L0 segments, and every summary carries an
    // empty graph triple.
    assert!(
        collect_files_with_extension(dir.path().join("graphs"), "parquet").is_empty(),
        "PqScanOnly index must not write any graph objects"
    );
    for summary in &index.manifest().segments {
        assert!(summary.graph_path.is_empty());
        assert!(summary.graph_checksum.is_empty());
        assert_eq!(summary.graph_size_bytes, 0);
    }

    // PqScan search returns correct results.
    let pq_hits = index
        .search_ids(&[8.2, 0.0], approx_options(2, LeafMode::PqScan))
        .unwrap();
    assert_eq!(pq_hits, ["c", "d"]);

    // Exact search returns correct results.
    let exact_hits = index
        .search_ids(&[0.2, 0.0], SearchOptions::exact(2))
        .unwrap();
    assert_eq!(exact_hits, ["a", "b"]);

    // get_vector round-trips.
    assert_eq!(index.get_vector("c").unwrap(), Some(vec![8.0, 0.0]));

    // Compaction to L1 still writes no graph objects and search stays correct.
    index
        .compact(CompactionOptions {
            source_level: 0,
            target_level: 1,
            max_segments: Some(2),
            min_segments: 2,
            target_segment_max_vectors: Some(4),
            target_segment_max_radius: None,
        })
        .unwrap();
    assert!(
        collect_files_with_extension(dir.path().join("graphs"), "parquet").is_empty(),
        "compaction of a PqScanOnly index must not write graph objects"
    );

    // GC of the now-obsolete L0 objects succeeds.
    index
        .gc_obsolete_segments(GarbageCollectionOptions {
            dry_run: false,
            min_age: Duration::ZERO,
        })
        .unwrap();

    // Reopen: the capability survives, no graphs exist, scan search still works.
    let reopened = BorsukIndex::open(&uri).unwrap();
    assert_eq!(reopened.leaf_capability(), LeafCapability::PqScanOnly);
    assert!(collect_files_with_extension(dir.path().join("graphs"), "parquet").is_empty());
    assert_eq!(
        reopened
            .search_ids(&[8.2, 0.0], approx_options(2, LeafMode::PqScan))
            .unwrap(),
        ["c", "d"]
    );
    assert_eq!(reopened.get_vector("a").unwrap(), Some(vec![0.0, 0.0]));
}

#[test]
fn pq_scan_only_index_rejects_graph_leaf_modes() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

    let mut index =
        BorsukIndex::create_with_leaf_capability(base_config(uri), LeafCapability::PqScanOnly)
            .unwrap();
    index
        .add(vec![
            VectorRecord::new("a", vec![0.0, 0.0]),
            VectorRecord::new("b", vec![1.0, 0.0]),
        ])
        .unwrap();
    index.flush().unwrap();

    for mode in [LeafMode::Graph, LeafMode::VamanaPq, LeafMode::Hybrid] {
        let error = index
            .search_ids(&[0.0, 0.0], approx_options(2, mode))
            .unwrap_err();
        match error {
            BorsukError::LeafModeNotConfigured {
                requested,
                capability,
            } => {
                assert_eq!(requested, mode);
                assert_eq!(capability, LeafCapability::PqScanOnly);
            }
            other => panic!("expected LeafModeNotConfigured for {mode:?}, got {other:?}"),
        }
        assert_eq!(
            index
                .search_ids(&[0.0, 0.0], approx_options(2, mode))
                .unwrap_err()
                .code(),
            "leaf_mode_not_configured"
        );
    }

    // Scan leaf modes are still accepted.
    for mode in [LeafMode::PqScan, LeafMode::SqScan, LeafMode::FlatScan] {
        index
            .search_ids(&[0.0, 0.0], approx_options(2, mode))
            .unwrap();
    }
}

#[test]
fn graph_enabled_default_index_builds_graphs_and_serves_graph_search() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

    // Default create == GraphEnabled.
    let mut index = BorsukIndex::create(base_config(uri.clone())).unwrap();
    assert_eq!(index.leaf_capability(), LeafCapability::GraphEnabled);

    index
        .add(vec![
            VectorRecord::new("a", vec![0.0, 0.0]),
            VectorRecord::new("b", vec![1.0, 0.0]),
            VectorRecord::new("c", vec![8.0, 0.0]),
            VectorRecord::new("d", vec![9.0, 0.0]),
        ])
        .unwrap();
    index.flush().unwrap();

    // Graph objects are written and referenced by every summary.
    let graphs = collect_files_with_extension(dir.path().join("graphs/L0"), "parquet");
    assert_eq!(graphs.len(), 2);
    for summary in &index.manifest().segments {
        assert!(summary.graph_path.starts_with("graphs/L0/"));
        assert!(!summary.graph_checksum.is_empty());
        assert!(summary.graph_size_bytes > 0);
    }

    // Graph and hybrid search both work.
    for mode in [LeafMode::Graph, LeafMode::VamanaPq, LeafMode::Hybrid] {
        let hits = index
            .search_ids(&[8.2, 0.0], approx_options(2, mode))
            .unwrap();
        assert_eq!(hits, ["c", "d"], "leaf mode {mode:?}");
    }

    let reopened = BorsukIndex::open(&uri).unwrap();
    assert_eq!(reopened.leaf_capability(), LeafCapability::GraphEnabled);
}
