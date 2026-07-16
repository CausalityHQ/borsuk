//! Measure the BUILD-time and storage impact of the [`BuildConfig`] knobs.
//!
//! Builds the same synthetic gist-like corpus three times under
//! `BORSUK_BUILD_TIMING`, printing each run's per-phase build breakdown plus the
//! on-disk dense-vector-sidecar footprint:
//!   (a) default             — zstd level 3 sidecar, cluster on all points
//!   (b) uncompressed sidecar — raw f32 rows, the fastest build
//!   (c) kmeans_sample=0.1    — fit centroids on a 10% seeded subsample
//!
//! The `vector_sidecar` phase shrinks dramatically for (b); the `voronoi_chunks`
//! phase shrinks for (c). Recall is unaffected either way (rerank is exact), so
//! this example reports only build time and storage.
//!
//! Run with:
//! `BORSUK_BUILD_TIMING=1 cargo run -p borsuk --example build_config_timing --release`
//! Override the corpus size with `LIMIT=<n>` (default 30000) and the dimension
//! with `DIM=<n>` (default 960, gist-like).

use std::env;
use std::error::Error;
use std::path::Path;
use std::time::Instant;

use borsuk::{
    BorsukIndex, BuildConfig, CompactionOptions, IndexConfig, SidecarCompression, VectorMetric,
    VectorRecord,
};

fn synthetic_records(count: usize, dimensions: usize) -> Vec<VectorRecord> {
    // Deterministic low-rank-ish corpus: a shared smooth basis plus a per-record
    // latent, lightly quantized, so the vectors carry the cross-row redundancy
    // real embeddings do (which zstd exploits) without any external dataset.
    let mut state: u64 = 0x9E37_79B9_7F4A_7C15;
    let mut next = || {
        state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        ((state >> 33) as f32 / u32::MAX as f32) - 0.5
    };
    let rank = 32.min(dimensions);
    let basis: Vec<Vec<f32>> = (0..rank)
        .map(|_| (0..dimensions).map(|_| next()).collect())
        .collect();
    (0..count)
        .map(|id| {
            let latent: Vec<f32> = (0..rank).map(|_| next() * 3.0).collect();
            let vector: Vec<f32> = (0..dimensions)
                .map(|d| {
                    let acc: f32 = (0..rank).map(|r| latent[r] * basis[r][d]).sum();
                    (acc * 8.0).round() / 8.0
                })
                .collect();
            VectorRecord::new(format!("v{id}"), vector)
        })
        .collect()
}

fn sidecar_bytes(root: &Path) -> u64 {
    fn walk(dir: &Path, acc: &mut u64) {
        if let Ok(entries) = std::fs::read_dir(dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    walk(&path, acc);
                } else if path.extension().is_some_and(|ext| ext == "arrow") {
                    *acc += std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
                }
            }
        }
    }
    let mut acc = 0;
    walk(&root.join("vectors"), &mut acc);
    acc
}

fn build(
    label: &str,
    build_config: BuildConfig,
    records: &[VectorRecord],
) -> Result<(), Box<dyn Error>> {
    let dir = tempfile::tempdir()?;
    let uri = dir.path().to_string_lossy().into_owned();
    let config = IndexConfig {
        uri,
        metric: VectorMetric::Euclidean,
        dimensions: records[0].vector.len(),
        segment_max_vectors: 2000,
        ram_budget_bytes: None,
        text: false,
        named_vectors: Default::default(),
    };
    let mut index = BorsukIndex::create_with_build_config(config, build_config)?;
    index.add(records.to_vec())?;
    // Compaction runs the voronoi clustering + dense-vector-sidecar encode, the
    // phases these knobs target.
    let start = Instant::now();
    index.compact(CompactionOptions {
        source_level: 0,
        target_level: 1,
        max_segments: None,
        min_segments: 1,
        target_segment_max_vectors: Some(2000),
        target_segment_max_radius: None,
    })?;
    let compaction_ms = start.elapsed().as_secs_f64() * 1000.0;
    let sidecar = sidecar_bytes(dir.path());
    borsuk::report_build_timing(label);
    eprintln!(
        "  {label}: compaction {compaction_ms:.1} ms, dense-vector-sidecar {sidecar} bytes ({:.2} MiB)\n",
        sidecar as f64 / (1024.0 * 1024.0)
    );
    Ok(())
}

fn main() -> Result<(), Box<dyn Error>> {
    if env::var_os("BORSUK_BUILD_TIMING").is_none() {
        // SAFETY: single-threaded example, set before any build work.
        unsafe { env::set_var("BORSUK_BUILD_TIMING", "1") };
    }
    let limit = env::var("LIMIT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(30_000usize);
    let dimensions = env::var("DIM")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(960usize);
    let records = synthetic_records(limit, dimensions);
    eprintln!("building {limit} x {dimensions} vectors, three configs\n");

    build("a-default-zstd3", BuildConfig::default(), &records)?;
    build(
        "b-uncompressed",
        BuildConfig {
            sidecar_compression: SidecarCompression::Uncompressed,
            ..BuildConfig::default()
        },
        &records,
    )?;
    build(
        "c-kmeans-sample-0.1",
        BuildConfig {
            kmeans_sample_fraction: 0.1,
            ..BuildConfig::default()
        },
        &records,
    )?;
    Ok(())
}
