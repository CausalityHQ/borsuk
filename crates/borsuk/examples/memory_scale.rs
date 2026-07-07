#![allow(missing_docs)]

//! Reader × dataset memory-pressure sweep.
//!
//! Proves the headline production claim: with a bounded search concurrency, peak
//! resident memory stays roughly flat as the number of concurrent readers grows,
//! so ~1000 parallel users do not cost ~1000× RAM. For each dataset size and each
//! reader count it runs a concurrent query storm twice — once uncapped and once
//! with `max_concurrent_searches` set — sampling peak process RSS, and reports RSS
//! delta, per-reader bytes, QPS, and tail latency as CSV.
//!
//! Defaults are tractable (100k vectors × 64/256/1024 readers). Override with
//! `BORSUK_MEMSCALE_VECTORS`, `BORSUK_MEMSCALE_READERS`,
//! `BORSUK_MEMSCALE_QUERIES_PER_READER`, `BORSUK_MEMSCALE_CONCURRENCY_CAP`, and
//! `BORSUK_MEMSCALE_OUTPUT` (CSV path) for larger runs such as `1000000` vectors.

use std::{
    env,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

use borsuk::{
    BorsukIndex, CompactionOptions, IndexConfig, LeafMode, OpenOptions, SearchOptions,
    VectorMetric, VectorRecord,
};
use memory_stats::memory_stats;

const DIMENSIONS: usize = 16;
const ADD_BATCH: usize = 4096;
const SEGMENT_MAX_VECTORS: usize = 4096;
const TOP_K: usize = 10;
/// Per-query segment budget — a production query reads a few candidate leaves,
/// not the whole index. Bounds each query's I/O and keeps the storm realistic.
const MAX_SEGMENTS: usize = 8;

fn query_options() -> SearchOptions {
    SearchOptions::approx(TOP_K, LeafMode::PqScan).with_max_segments(MAX_SEGMENTS)
}

fn main() -> borsuk::Result<()> {
    let datasets = env_list("BORSUK_MEMSCALE_VECTORS", &[100_000]);
    let reader_counts = env_list("BORSUK_MEMSCALE_READERS", &[64, 256, 1024]);
    let queries_per_reader = env_usize("BORSUK_MEMSCALE_QUERIES_PER_READER", 20);
    let concurrency_cap = env_usize("BORSUK_MEMSCALE_CONCURRENCY_CAP", 16);

    let mut csv = String::from(
        "vectors,readers,concurrency_cap,rss_before_bytes,rss_peak_bytes,\
rss_peak_delta_bytes,per_reader_bytes,resident_metadata_bytes,qps,p50_ms,p95_ms\n",
    );
    println!(
        "vectors  readers  cap    rss_delta_MB  per_reader_KB  QPS      p50ms p95ms  resident_bytes"
    );

    for &vectors in &datasets {
        let uri = build_index(vectors)?;
        let resident = BorsukIndex::open(&uri)?.stats().resident_bytes_estimate;

        for &readers in &reader_counts {
            // Uncapped: each concurrent search decodes independently, so RSS
            // scales with readers. Capped: the admission gate bounds concurrent
            // decode, so RSS stays flat regardless of reader count.
            for cap in [0_usize, concurrency_cap] {
                let options = OpenOptions {
                    max_concurrent_searches: (cap > 0).then_some(cap),
                    ..OpenOptions::default()
                };
                let index = Arc::new(BorsukIndex::open_with_options(&uri, options)?);
                // Warm the routing path once.
                let _ = index.search_with_report(&synthetic_vector(1), query_options())?;
                let pass = run_storm(&index, vectors, readers, queries_per_reader);
                let per_reader = pass.rss_peak_delta / readers as u64;
                println!(
                    "{:>7}  {:>7}  {:>4}  {:>11.1}  {:>12.1}  {:>7.0}  {:>4} {:>5}  {}",
                    vectors,
                    readers,
                    cap,
                    pass.rss_peak_delta as f64 / (1024.0 * 1024.0),
                    per_reader as f64 / 1024.0,
                    pass.qps,
                    pass.p50_ms,
                    pass.p95_ms,
                    resident,
                );
                csv.push_str(&format!(
                    "{vectors},{readers},{cap},{},{},{},{per_reader},{resident},{:.1},{},{}\n",
                    pass.rss_before,
                    pass.rss_peak,
                    pass.rss_peak_delta,
                    pass.qps,
                    pass.p50_ms,
                    pass.p95_ms,
                ));
            }
        }
    }

    if let Ok(path) = env::var("BORSUK_MEMSCALE_OUTPUT") {
        std::fs::write(&path, csv).map_err(|source| borsuk::BorsukError::Io {
            path: path.into(),
            source,
        })?;
    }
    Ok(())
}

struct StormResult {
    rss_before: u64,
    rss_peak: u64,
    rss_peak_delta: u64,
    qps: f64,
    p50_ms: u64,
    p95_ms: u64,
}

fn run_storm(
    index: &Arc<BorsukIndex>,
    vectors: usize,
    readers: usize,
    queries_per_reader: usize,
) -> StormResult {
    let rss_before = current_rss();
    let peak = Arc::new(AtomicU64::new(rss_before));
    let stop = Arc::new(AtomicBool::new(false));
    let sampler = {
        let peak = Arc::clone(&peak);
        let stop = Arc::clone(&stop);
        thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                peak.fetch_max(current_rss(), Ordering::Relaxed);
                thread::sleep(Duration::from_millis(1));
            }
        })
    };

    let wall = Instant::now();
    let handles: Vec<_> = (0..readers)
        .map(|reader| {
            let index = Arc::clone(index);
            thread::spawn(move || {
                let mut latencies = Vec::with_capacity(queries_per_reader);
                for query_index in 0..queries_per_reader {
                    let target = (reader * 131 + query_index * 17) % vectors;
                    let report = index
                        .search_with_report(&synthetic_vector(target), query_options())
                        .expect("search failed");
                    latencies.push(report.elapsed_ms);
                }
                latencies
            })
        })
        .collect();

    let mut latencies = Vec::new();
    for handle in handles {
        latencies.extend(handle.join().expect("reader thread panicked"));
    }
    let elapsed = wall.elapsed().as_secs_f64().max(f64::MIN_POSITIVE);

    stop.store(true, Ordering::Relaxed);
    let _ = sampler.join();
    let rss_peak = peak.load(Ordering::Relaxed);

    latencies.sort_unstable();
    let queries = latencies.len();
    StormResult {
        rss_before,
        rss_peak,
        rss_peak_delta: rss_peak.saturating_sub(rss_before),
        qps: queries as f64 / elapsed,
        p50_ms: percentile(&latencies, 50),
        p95_ms: percentile(&latencies, 95),
    }
}

fn build_index(vectors: usize) -> borsuk::Result<String> {
    let dir = env::temp_dir().join(format!("borsuk-memscale-{vectors}"));
    if dir.exists() {
        std::fs::remove_dir_all(&dir).map_err(|source| borsuk::BorsukError::Io {
            path: dir.clone(),
            source,
        })?;
    }
    let uri = dir.to_string_lossy().into_owned();
    let mut index = BorsukIndex::create(IndexConfig {
        uri: uri.clone(),
        metric: VectorMetric::Euclidean,
        dimensions: DIMENSIONS,
        segment_max_vectors: SEGMENT_MAX_VECTORS,
        ram_budget_bytes: None,
    })?;
    for start in (0..vectors).step_by(ADD_BATCH) {
        let end = (start + ADD_BATCH).min(vectors);
        let batch: Vec<VectorRecord> = (start..end)
            .map(|id| VectorRecord::new(format!("v{id}"), synthetic_vector(id)))
            .collect();
        index.add(batch)?;
    }
    // Read-shape into compacted pq-scan leaves.
    index.compact(CompactionOptions {
        source_level: 0,
        target_level: 1,
        max_segments: None,
        min_segments: 1,
        target_segment_max_vectors: Some(SEGMENT_MAX_VECTORS),
        target_segment_max_radius: None,
    })?;
    Ok(uri)
}

fn current_rss() -> u64 {
    memory_stats()
        .map(|stats| stats.physical_mem as u64)
        .unwrap_or(0)
}

fn percentile(sorted: &[u64], pct: usize) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    sorted[(pct * (sorted.len() - 1)) / 100]
}

fn synthetic_vector(seed: usize) -> Vec<f32> {
    (0..DIMENSIONS)
        .map(|dim| {
            let x = (seed.wrapping_mul(2_654_435_761).wrapping_add(dim * 40_503)) % 10_007;
            x as f32 / 10_007.0
        })
        .collect()
}

fn env_usize(key: &str, default: usize) -> usize {
    env::var(key)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_list(key: &str, default: &[usize]) -> Vec<usize> {
    match env::var(key) {
        Ok(value) => {
            let parsed: Vec<usize> = value
                .split(',')
                .filter_map(|part| part.trim().parse().ok())
                .filter(|value| *value > 0)
                .collect();
            if parsed.is_empty() {
                default.to_vec()
            } else {
                parsed
            }
        }
        Err(_) => default.to_vec(),
    }
}
