#![allow(missing_docs)]

//! Tests for the typed, persisted [`BuildConfig`] BUILD-tuning knobs.
//!
//! These cover the three guarantees the knobs must hold end to end:
//! - the headline [`SidecarCompression::Uncompressed`] sidecar builds a
//!   searchable index whose rerank returns byte-exact vectors (still lossless),
//! - a low `kmeans_sample_fraction` still recovers recall ~1.0 on a clustered
//!   set (exact rerank protects recall) and is deterministic for a fixed
//!   config+seed (same config -> byte-identical persisted objects),
//! - the config survives a manifest round-trip (create -> reopen), while a
//!   default config leaves the manifest byte-identical to a pre-knob index.

use std::fs;
use std::path::Path;

use borsuk::{
    BorsukIndex, BuildConfig, CompactionOptions, IndexConfig, SearchOptions, SidecarCompression,
    VectorMetric, VectorRecord,
};

fn base_config(uri: String, dimensions: usize) -> IndexConfig {
    IndexConfig {
        uri,
        metric: VectorMetric::Euclidean,
        dimensions,
        segment_max_vectors: 64,
        ram_budget_bytes: None,
        text: false,
        named_vectors: Default::default(),
    }
}

/// A deterministic set of tightly-clustered vectors: `clusters` well-separated
/// centers, each with `per_cluster` points jittered by a seeded LCG. Neighbour
/// recovery is easy, so any recall loss is attributable to the clustering knob,
/// not the data.
fn clustered_records(clusters: usize, per_cluster: usize, dimensions: usize) -> Vec<VectorRecord> {
    let mut state = 0x1234_5678_9abc_def0u64;
    let mut next_unit = || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        ((state >> 40) as f32 / (1u64 << 24) as f32) - 0.5
    };
    let mut records = Vec::with_capacity(clusters * per_cluster);
    for c in 0..clusters {
        let center: Vec<f32> = (0..dimensions)
            .map(|d| ((c * 37 + d * 13) % 101) as f32)
            .collect();
        for p in 0..per_cluster {
            let vector: Vec<f32> = center.iter().map(|&x| x + next_unit() * 0.05).collect();
            records.push(VectorRecord::new(format!("c{c}-p{p}"), vector));
        }
    }
    records
}

fn brute_force_top_k(records: &[VectorRecord], query: &[f32], k: usize) -> Vec<String> {
    let mut scored: Vec<(f32, String)> = records
        .iter()
        .map(|record| {
            let dist: f32 = record
                .vector
                .iter()
                .zip(query)
                .map(|(a, b)| (a - b) * (a - b))
                .sum();
            (dist, record.id.to_string())
        })
        .collect();
    scored.sort_by(|a, b| a.0.total_cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
    scored.into_iter().take(k).map(|(_, id)| id).collect()
}

fn build_and_compact(
    uri: String,
    dimensions: usize,
    build: BuildConfig,
    records: Vec<VectorRecord>,
) -> BorsukIndex {
    let mut index =
        BorsukIndex::create_with_build_config(base_config(uri, dimensions), build).unwrap();
    index.add(records).unwrap();
    index
        .compact(CompactionOptions {
            source_level: 0,
            target_level: 1,
            max_segments: None,
            min_segments: 1,
            target_segment_max_vectors: Some(64),
            target_segment_max_radius: None,
        })
        .unwrap();
    index
}

/// The sorted multiset of content hashes of the per-segment dense-vector
/// sidecars (`vectors/**/*.arrow`) under `root`.
///
/// The sidecar is the deterministic build product that captures the clustering
/// outcome: each object is exactly the vectors of one segment, in row order, so
/// two builds that partition the corpus identically produce the SAME sorted set
/// of sidecar hashes. (The segment parquet and graph objects embed a fresh
/// per-build random segment UUID, so their bytes differ run-to-run even for a
/// plain `create` — that non-determinism is pre-existing and orthogonal to the
/// clustering, so it is excluded here.)
fn sidecar_hash_multiset(root: &Path) -> Vec<String> {
    fn walk(dir: &Path, out: &mut Vec<String>) {
        for entry in fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                walk(&path, out);
            } else if path.extension().is_some_and(|ext| ext == "arrow") {
                let bytes = fs::read(&path).unwrap();
                out.push(blake3::hash(&bytes).to_hex().to_string());
            }
        }
    }
    let mut out = Vec::new();
    let vectors = root.join("vectors");
    if vectors.exists() {
        walk(&vectors, &mut out);
    }
    out.sort();
    out
}

fn approx_ids(index: &BorsukIndex, query: &[f32], k: usize) -> Vec<String> {
    index.search_ids(query, SearchOptions::exact(k)).unwrap()
}

#[test]
fn uncompressed_sidecar_builds_searchable_index_with_exact_vectors() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();
    let dimensions = 32;
    let records = clustered_records(8, 20, dimensions);

    let build = BuildConfig {
        sidecar_compression: SidecarCompression::Uncompressed,
        ..BuildConfig::default()
    };
    let index = build_and_compact(uri.clone(), dimensions, build, records.clone());
    assert_eq!(
        index.build_config().sidecar_compression,
        SidecarCompression::Uncompressed
    );

    // get_vector round-trips byte-exactly through the uncompressed sidecar.
    for record in &records {
        let id = record.id.to_string();
        let got = index.get_vector(&id).unwrap().unwrap();
        assert_eq!(got, record.vector, "vector {id} did not round-trip");
    }

    // Search over the uncompressed-sidecar index recovers the true top-k.
    for record in records.iter().step_by(11) {
        let expected = brute_force_top_k(&records, &record.vector, 5);
        let got = approx_ids(&index, &record.vector, 5);
        assert_eq!(got, expected, "uncompressed search missed neighbours");
    }

    // Reopen: the sidecar mode survives and search still works.
    let reopened = BorsukIndex::open(&uri).unwrap();
    assert_eq!(
        reopened.build_config().sidecar_compression,
        SidecarCompression::Uncompressed
    );
    let probe = &records[3];
    assert_eq!(
        reopened
            .search_ids(&probe.vector, SearchOptions::exact(1))
            .unwrap(),
        vec![probe.id.to_string()]
    );
}

#[test]
fn uncompressed_and_zstd_sidecars_return_the_same_top_k() {
    // The sidecar mode is a pure build/storage knob: rerank is exact, so a zstd
    // and an uncompressed index over the same data return byte-for-byte the same
    // neighbours. (The sidecar unit tests separately prove the size ordering on
    // structured data; on tiny clustered segments per-segment dictionary overhead
    // can invert it, so size is not asserted here.)
    let dimensions = 48;
    let records = clustered_records(10, 24, dimensions);

    let zstd_dir = tempfile::tempdir().unwrap();
    let zstd = build_and_compact(
        zstd_dir.path().to_string_lossy().into_owned(),
        dimensions,
        BuildConfig::default(),
        records.clone(),
    );

    let raw_dir = tempfile::tempdir().unwrap();
    let raw = build_and_compact(
        raw_dir.path().to_string_lossy().into_owned(),
        dimensions,
        BuildConfig {
            sidecar_compression: SidecarCompression::Uncompressed,
            ..BuildConfig::default()
        },
        records.clone(),
    );

    for record in records.iter().step_by(7) {
        let expected = brute_force_top_k(&records, &record.vector, 5);
        assert_eq!(approx_ids(&zstd, &record.vector, 5), expected);
        assert_eq!(approx_ids(&raw, &record.vector, 5), expected);
    }
}

#[test]
fn low_kmeans_sample_fraction_keeps_recall_and_is_deterministic() {
    let dimensions = 40;
    let records = clustered_records(12, 30, dimensions);
    let build = BuildConfig {
        kmeans_sample_fraction: 0.1,
        ..BuildConfig::default()
    };

    // Two independent builds with the SAME config + data.
    let dir_a = tempfile::tempdir().unwrap();
    let a = build_and_compact(
        dir_a.path().to_string_lossy().into_owned(),
        dimensions,
        build.clone(),
        records.clone(),
    );
    let dir_b = tempfile::tempdir().unwrap();
    let b = build_and_compact(
        dir_b.path().to_string_lossy().into_owned(),
        dimensions,
        build.clone(),
        records.clone(),
    );

    // Determinism: the seeded subsample makes the two builds emit byte-identical
    // content-addressed objects (segments, sidecars, graphs). Only the random
    // per-segment UUID filenames differ, so compare the content-hash multiset.
    assert_eq!(
        sidecar_hash_multiset(dir_a.path()),
        sidecar_hash_multiset(dir_b.path()),
        "sampled build was not deterministic for a fixed config+data"
    );

    // Recall ~1.0: exact rerank recovers the true neighbours despite fitting the
    // centroids on only a 10% subsample.
    let mut recovered = 0usize;
    let mut total = 0usize;
    for record in records.iter().step_by(3) {
        let expected = brute_force_top_k(&records, &record.vector, 5);
        let got = approx_ids(&a, &record.vector, 5);
        recovered += expected.iter().filter(|id| got.contains(id)).count();
        total += expected.len();
    }
    let recall = recovered as f64 / total as f64;
    assert!(recall >= 0.99, "sampled-kmeans recall {recall} below 0.99");

    // The two indexes also agree on results (same config -> same partitioning).
    let probe = &records[17];
    assert_eq!(
        approx_ids(&a, &probe.vector, 5),
        approx_ids(&b, &probe.vector, 5)
    );
}

#[test]
fn default_build_config_matches_plain_create_byte_for_byte() {
    let dimensions = 24;
    let records = clustered_records(6, 16, dimensions);

    // Plain `create` (no BuildConfig) vs an explicit default BuildConfig.
    let plain_dir = tempfile::tempdir().unwrap();
    let mut plain = BorsukIndex::create(base_config(
        plain_dir.path().to_string_lossy().into_owned(),
        dimensions,
    ))
    .unwrap();
    plain.add(records.clone()).unwrap();
    plain
        .compact(CompactionOptions {
            source_level: 0,
            target_level: 1,
            max_segments: None,
            min_segments: 1,
            target_segment_max_vectors: Some(64),
            target_segment_max_radius: None,
        })
        .unwrap();

    let default_dir = tempfile::tempdir().unwrap();
    let _default = build_and_compact(
        default_dir.path().to_string_lossy().into_owned(),
        dimensions,
        BuildConfig::default(),
        records.clone(),
    );

    // Content-addressed objects are byte-identical: a defaulted BuildConfig builds
    // exactly what plain `create` builds (segment UUIDs differ, contents don't).
    assert_eq!(
        sidecar_hash_multiset(plain_dir.path()),
        sidecar_hash_multiset(default_dir.path()),
        "default BuildConfig diverged from plain create"
    );

    // And the default index reports the default config.
    assert_eq!(plain.build_config(), &BuildConfig::default());
}

#[test]
fn build_config_survives_manifest_round_trip() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();
    let dimensions = 16;
    let build = BuildConfig {
        sidecar_compression: SidecarCompression::Zstd { level: 9 },
        kmeans_sample_fraction: 0.25,
        kmeans_max_iterations: Some(8),
        pq_codebook_sample: Some(1000),
        quantizer: borsuk::QuantizerKind::TurboQuant {
            seed: 12345,
            bits: 5,
        },
    };
    let index =
        BorsukIndex::create_with_build_config(base_config(uri.clone(), dimensions), build.clone())
            .unwrap();
    assert_eq!(index.build_config(), &build);
    drop(index);

    let reopened = BorsukIndex::open(&uri).unwrap();
    assert_eq!(
        reopened.build_config(),
        &build,
        "BuildConfig did not survive a manifest round-trip"
    );
}

#[test]
fn invalid_build_config_is_rejected_at_creation() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();
    let bad = BuildConfig {
        kmeans_sample_fraction: 1.5,
        ..BuildConfig::default()
    };
    let err = BorsukIndex::create_with_build_config(base_config(uri, 8), bad);
    assert!(
        err.is_err(),
        "an out-of-range sample fraction must be rejected"
    );
}
