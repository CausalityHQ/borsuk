//! Lightweight, env-gated build-phase timing.
//!
//! Set `BORSUK_BUILD_TIMING=1` to have each instrumented build phase accumulate
//! its wall-clock into a process-global table and print a per-phase breakdown on
//! demand. When the variable is unset (the default, and always in production)
//! every call here is a couple of cheap atomic loads that return immediately, so
//! the instrumentation never affects the hot path.

use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

/// Ordered list of the phases we accumulate. Kept small and fixed so the table
/// is a flat array of atomics (no locking on the hot path).
#[derive(Clone, Copy)]
pub(crate) enum Phase {
    SegmentCentroidRadius,
    SegmentRoutingCodes,
    SegmentPqBounds,
    SegmentPqEncode,
    GraphBuild,
    VectorSidecar,
    FilterIndex,
    SegmentParquet,
    Bm25Sidecar,
    ObjectPuts,
    VoronoiChunks,
    CompactionSourceRead,
    LocalitySort,
}

const PHASE_COUNT: usize = 13;

const PHASE_NAMES: [&str; PHASE_COUNT] = [
    "segment_centroid_radius",
    "segment_routing_codes",
    "segment_pq_bounds",
    "segment_pq_encode",
    "graph_build",
    "vector_sidecar",
    "filter_index",
    "segment_parquet",
    "bm25_sidecar",
    "object_puts",
    "voronoi_chunks",
    "compaction_source_read",
    "locality_sort",
];

struct Table {
    nanos: [AtomicU64; PHASE_COUNT],
    calls: [AtomicU64; PHASE_COUNT],
}

fn table() -> &'static Table {
    static TABLE: OnceLock<Table> = OnceLock::new();
    TABLE.get_or_init(|| Table {
        nanos: std::array::from_fn(|_| AtomicU64::new(0)),
        calls: std::array::from_fn(|_| AtomicU64::new(0)),
    })
}

/// Whether timing is enabled, resolved once from the environment.
pub(crate) fn enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| {
        std::env::var_os("BORSUK_BUILD_TIMING")
            .is_some_and(|value| value != "0" && !value.is_empty())
    })
}

/// Time the closure and, if enabled, add its duration to `phase`. When timing is
/// off there is no `Instant::now()` call at all, so the wrapper is free.
#[inline]
pub(crate) fn timed<T>(phase: Phase, f: impl FnOnce() -> T) -> T {
    if !enabled() {
        return f();
    }
    let started = Instant::now();
    let out = f();
    let elapsed = started.elapsed().as_nanos() as u64;
    let idx = phase as usize;
    let table = table();
    table.nanos[idx].fetch_add(elapsed, Ordering::Relaxed);
    table.calls[idx].fetch_add(1, Ordering::Relaxed);
    out
}

/// Print the accumulated per-phase breakdown (if enabled) with a caller-supplied
/// label, then reset the counters so the next phase group starts clean.
pub(crate) fn report_and_reset(label: &str) {
    if !enabled() {
        return;
    }
    let table = table();
    eprintln!("BORSUK_BUILD_TIMING [{label}] per-phase totals:");
    let mut total_ms = 0.0_f64;
    for (name, (nanos_slot, calls_slot)) in
        PHASE_NAMES.iter().zip(table.nanos.iter().zip(&table.calls))
    {
        let nanos = nanos_slot.swap(0, Ordering::Relaxed);
        let calls = calls_slot.swap(0, Ordering::Relaxed);
        if calls == 0 {
            continue;
        }
        let ms = nanos as f64 / 1.0e6;
        total_ms += ms;
        eprintln!("  {name:<24} {ms:>10.3} ms   ({calls} calls)");
    }
    eprintln!("  {:<24} {:>10.3} ms", "TOTAL(instrumented)", total_ms);
}
