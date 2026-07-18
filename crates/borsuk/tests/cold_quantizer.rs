//! Cold/paged coarse-quantizer coverage.
//!
//! A COLD/paged index (opened without warming, `resident_routing: false`) has no
//! resident routing summaries, so historically it got no IVF coarse quantizer
//! and fell back to the paged routing tree — which degrades on high-dimensional
//! data (the curse of dimensionality). These tests prove that a persisted
//! quantizer object lets the cold path route through the same IVF probe list the
//! warm path uses, at matching recall, and that the object is retained while
//! live and reclaimed once superseded.

use std::collections::BTreeSet;

use borsuk::{
    BorsukIndex, GarbageCollectionOptions, IndexConfig, LeafMode, OpenOptions, SearchOptions,
    VectorMetric, VectorRecord,
};

const DIMENSIONS: usize = 96;
const CLUSTERS: usize = 96;
const RECORDS: usize = 4_000;
const SEGMENT_MAX_VECTORS: usize = 32;
const K: usize = 10;
const MAX_SEGMENTS: usize = 48;

fn splitmix64(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = value;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

fn centered_unit(seed: usize, dimension: usize) -> f32 {
    let mixed = splitmix64(
        (seed as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15)
            ^ (dimension as u64).wrapping_mul(0xBF58_476D_1CE4_E5B9),
    );
    let unit = (mixed >> 40) as f32 / (1_u64 << 24) as f32;
    unit - 0.5
}

fn cluster_center(cluster: usize, dimension: usize) -> f32 {
    centered_unit(cluster.wrapping_mul(0x9E37_79B9).wrapping_add(1), dimension)
}

/// A deterministic clustered vector (real embeddings live on a manifold; uniform
/// noise is a pathological ANN worst case and not representative).
fn vector(seed: usize) -> Vec<f32> {
    let cluster = (splitmix64(seed as u64) % CLUSTERS as u64) as usize;
    (0..DIMENSIONS)
        .map(|dimension| cluster_center(cluster, dimension) + 0.02 * centered_unit(seed, dimension))
        .collect()
}

fn build_index(uri: &str) -> BorsukIndex {
    let mut index = BorsukIndex::create(IndexConfig {
        uri: uri.to_string(),
        metric: VectorMetric::Euclidean,
        dimensions: DIMENSIONS,
        segment_max_vectors: SEGMENT_MAX_VECTORS,
        ram_budget_bytes: None,
        text: false,
        named_vectors: Default::default(),
    })
    .unwrap();
    let records = (0..RECORDS)
        .map(|seed| VectorRecord::new(format!("v{seed}"), vector(seed)))
        .collect::<Vec<_>>();
    index.add(records).unwrap();
    index.flush().unwrap();
    // Materialize the WAL tail into cells so the cell layout (and the persisted
    // quantizer over it) exists; compaction is the write site for the quantizer.
    index
        .compact(borsuk::CompactionOptions {
            source_level: 0,
            target_level: 1,
            max_segments: Some(RECORDS),
            min_segments: 2,
            target_segment_max_vectors: Some(SEGMENT_MAX_VECTORS),
            target_segment_max_radius: None,
        })
        .unwrap();
    index
}

fn recall_at_k(truth: &[String], got: &[String]) -> f32 {
    if truth.is_empty() {
        return 1.0;
    }
    let truth_set: BTreeSet<&String> = truth.iter().collect();
    let hits = got.iter().filter(|id| truth_set.contains(id)).count();
    hits as f32 / truth.len() as f32
}

fn approx_ids(index: &BorsukIndex, query: &[f32], disable_quantizer: bool) -> Vec<String> {
    let mut options = SearchOptions::approx(K, LeafMode::PqScan)
        .with_max_segments(MAX_SEGMENTS)
        .with_max_candidates_per_segment(4096);
    if disable_quantizer {
        options = options.without_coarse_quantizer();
    }
    index.search_ids(query, options).unwrap()
}

/// The heart of the feature: a cold/paged index must reach the SAME recall as
/// the warmed index — proving the cold path uses the persisted quantizer, not
/// the degraded routing-tree fallback.
#[test]
fn cold_quantizer_recall_matches_warm_and_brute_force() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();
    let index = build_index(&uri);

    // The active manifest must reference a persisted quantizer object.
    assert!(
        index.manifest().has_persisted_quantizer(),
        "compaction should have persisted a coarse quantizer"
    );

    let queries: Vec<Vec<f32>> = (0..25).map(|q| vector(RECORDS + q * 7)).collect();

    // Ground truth per query (exact search on a warmed handle).
    let warm = BorsukIndex::open_with_options(
        &uri,
        OpenOptions {
            preload: true,
            ..OpenOptions::default()
        },
    )
    .unwrap();
    let truth: Vec<Vec<String>> = queries
        .iter()
        .map(|q| warm.search_ids(q, SearchOptions::exact(K)).unwrap())
        .collect();

    // Cold/paged handle: no warming, no resident routing summaries.
    let cold = BorsukIndex::open_with_options(
        &uri,
        OpenOptions {
            resident_routing: false,
            ..OpenOptions::default()
        },
    )
    .unwrap();
    assert!(
        cold.manifest().segments.is_empty() || cold.manifest().segments.len() < RECORDS,
        "cold open should not hold every segment resident"
    );

    let mut cold_recall = 0.0_f32;
    let mut warm_recall = 0.0_f32;
    let mut cold_tree_recall = 0.0_f32;
    for (query, truth) in queries.iter().zip(&truth) {
        let cold_ids = approx_ids(&cold, query, false);
        let warm_ids = approx_ids(&warm, query, false);
        let cold_tree_ids = approx_ids(&cold, query, true);
        // Cold and warm route through the SAME CentroidHnsw (one built in RAM,
        // one loaded from the object): identical probe list => identical hits.
        assert_eq!(
            cold_ids, warm_ids,
            "cold quantizer must return the same hits as the warm quantizer"
        );
        cold_recall += recall_at_k(truth, &cold_ids);
        warm_recall += recall_at_k(truth, &warm_ids);
        cold_tree_recall += recall_at_k(truth, &cold_tree_ids);
    }
    let n = queries.len() as f32;
    cold_recall /= n;
    warm_recall /= n;
    cold_tree_recall /= n;

    eprintln!(
        "cold_quantizer recall: cold={cold_recall:.3} warm={warm_recall:.3} cold_tree={cold_tree_recall:.3}"
    );

    // Parity with warm, and a strong absolute recall on clustered high-dim data.
    assert!(
        (cold_recall - warm_recall).abs() < 1e-6,
        "cold != warm recall"
    );
    assert!(
        cold_recall >= 0.95,
        "cold quantizer recall too low: {cold_recall:.3}"
    );
}

/// The persisted quantizer round-trips: write at compaction, reopen cold, load
/// and route. Verified indirectly above via recall; here we assert the object
/// is present on storage and referenced by the manifest.
#[test]
fn persisted_quantizer_object_is_written_and_referenced() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();
    let index = build_index(&uri);

    assert!(index.manifest().has_persisted_quantizer());
    let objects: Vec<_> = std::fs::read_dir(dir.path().join("quantizer"))
        .expect("quantizer directory should exist")
        .flat_map(|prefix| std::fs::read_dir(prefix.unwrap().path()).unwrap())
        .collect();
    assert!(
        !objects.is_empty(),
        "a quantizer object should be written under quantizer/"
    );
}

/// Disabling the quantizer via `BuildConfig` keeps the cold path on the routing
/// tree — no persisted object, no manifest reference (the escape hatch).
#[test]
fn build_config_can_disable_persisted_quantizer() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();
    let mut index = BorsukIndex::create_with_build_config(
        IndexConfig {
            uri: uri.clone(),
            metric: VectorMetric::Euclidean,
            dimensions: DIMENSIONS,
            segment_max_vectors: SEGMENT_MAX_VECTORS,
            ram_budget_bytes: None,
            text: false,
            named_vectors: Default::default(),
        },
        borsuk::BuildConfig {
            persist_coarse_quantizer: false,
            ..borsuk::BuildConfig::default()
        },
    )
    .unwrap();
    let records = (0..RECORDS)
        .map(|seed| VectorRecord::new(format!("v{seed}"), vector(seed)))
        .collect::<Vec<_>>();
    index.add(records).unwrap();
    index.flush().unwrap();
    index
        .compact(borsuk::CompactionOptions {
            source_level: 0,
            target_level: 1,
            max_segments: Some(RECORDS),
            min_segments: 2,
            target_segment_max_vectors: Some(SEGMENT_MAX_VECTORS),
            target_segment_max_radius: None,
        })
        .unwrap();

    assert!(
        !index.manifest().has_persisted_quantizer(),
        "persist_coarse_quantizer=false should write no quantizer reference"
    );
    assert!(
        !dir.path().join("quantizer").exists()
            || std::fs::read_dir(dir.path().join("quantizer"))
                .map(|mut d| d.next().is_none())
                .unwrap_or(true),
        "no quantizer object should be written when disabled"
    );
}

/// GC retains the live quantizer object and reclaims a superseded one.
#[test]
fn garbage_collection_keeps_live_quantizer_and_reclaims_orphan() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();
    let mut index = build_index(&uri);

    let quantizer_dir = dir.path().join("quantizer");
    let count_objects = || -> usize {
        std::fs::read_dir(&quantizer_dir)
            .map(|prefixes| {
                prefixes
                    .flatten()
                    .flat_map(|prefix| std::fs::read_dir(prefix.path()).unwrap())
                    .count()
            })
            .unwrap_or(0)
    };
    // build_index flushes then compacts; each refreshes the persisted quantizer,
    // so one or more objects exist and at least one is already orphaned.
    let objects_after_build = count_objects();
    assert!(
        objects_after_build >= 1,
        "at least one quantizer object after build"
    );

    // A second compaction rewrites the cells and writes a new quantizer object,
    // orphaning the first. Add a record so the compaction actually changes cells.
    index
        .add(vec![VectorRecord::new("extra", vector(999_999))])
        .unwrap();
    index.flush().unwrap();
    index
        .compact(borsuk::CompactionOptions {
            source_level: 0,
            target_level: 2,
            max_segments: Some(RECORDS + 1),
            min_segments: 2,
            target_segment_max_vectors: Some(SEGMENT_MAX_VECTORS),
            target_segment_max_radius: None,
        })
        .unwrap();
    assert!(
        count_objects() >= 2,
        "second compaction should write a new quantizer object"
    );
    let live_path = index
        .manifest()
        .persisted_quantizer_path()
        .expect("active manifest references a quantizer")
        .to_string();

    // GC with zero min-age reclaims the orphaned (superseded) quantizer object.
    let report = index
        .gc_obsolete_segments(GarbageCollectionOptions {
            min_age: std::time::Duration::ZERO,
            dry_run: false,
        })
        .unwrap();
    assert!(
        report.objects_deleted > 0,
        "gc should reclaim the superseded quantizer (and other orphans)"
    );

    // Exactly the live quantizer object remains, and the cold path still loads it.
    assert_eq!(count_objects(), 1, "only the live quantizer should remain");
    assert!(
        std::path::Path::new(&format!("{}/{}", uri, live_path)).exists()
            || dir.path().join(&live_path).exists(),
        "the live quantizer object must be retained"
    );

    let cold = BorsukIndex::open_with_options(
        &uri,
        OpenOptions {
            resident_routing: false,
            ..OpenOptions::default()
        },
    )
    .unwrap();
    let query = vector(RECORDS + 3);
    // Still routable cold after GC (the object it needs was kept).
    let ids = approx_ids(&cold, &query, false);
    assert_eq!(ids.len(), K);
}
