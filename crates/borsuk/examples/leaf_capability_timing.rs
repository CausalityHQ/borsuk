//! Demonstrate that a `PqScanOnly` index skips per-segment graph construction.
//!
//! Builds the same synthetic corpus twice under `BORSUK_BUILD_TIMING`, once with
//! the default `GraphEnabled` capability and once with `PqScanOnly`, and prints
//! each run's per-phase build timing. The `graph_build` phase appears with a
//! non-zero cost for `GraphEnabled` and is entirely absent (zero calls) for
//! `PqScanOnly`.
//!
//! Run with:
//! `BORSUK_BUILD_TIMING=1 cargo run -p borsuk --example leaf_capability_timing --release`
//! Override the corpus size with `LIMIT=<n>` (default 30000).

use std::env;
use std::error::Error;

use borsuk::{BorsukIndex, IndexConfig, LeafCapability, VectorMetric, VectorRecord};

fn synthetic_records(count: usize, dimensions: usize) -> Vec<VectorRecord> {
    // Deterministic pseudo-random corpus; a plain LCG keeps the example dep-free.
    let mut state: u64 = 0x9E37_79B9_7F4A_7C15;
    let mut next = || {
        state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        ((state >> 33) as f32 / u32::MAX as f32) - 0.5
    };
    (0..count)
        .map(|id| {
            let vector = (0..dimensions).map(|_| next()).collect::<Vec<_>>();
            VectorRecord::new(format!("v{id}"), vector)
        })
        .collect()
}

fn build(
    label: &str,
    capability: LeafCapability,
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
    let mut index = BorsukIndex::create_with_leaf_capability(config, capability)?;
    index.add(records.to_vec())?;
    index.flush()?;
    borsuk::report_build_timing(label);
    Ok(())
}

fn main() -> Result<(), Box<dyn Error>> {
    // Turn on build timing before any instrumented call runs in this process.
    if env::var_os("BORSUK_BUILD_TIMING").is_none() {
        // SAFETY: single-threaded example, set before any build work.
        unsafe { env::set_var("BORSUK_BUILD_TIMING", "1") };
    }
    let limit = env::var("LIMIT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(30_000usize);
    let dimensions = 96;
    let records = synthetic_records(limit, dimensions);
    eprintln!("building {limit} x {dimensions} vectors\n");

    build("graph-enabled", LeafCapability::GraphEnabled, &records)?;
    eprintln!();
    build("pq-scan-only", LeafCapability::PqScanOnly, &records)?;
    Ok(())
}
