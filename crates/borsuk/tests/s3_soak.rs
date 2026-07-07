#![allow(missing_docs)]

//! Request-rate soak test against a real S3-compatible object store (MinIO,
//! SeaweedFS, or AWS S3). It is gated on `BORSUK_S3_TEST_URI`: when the variable
//! is unset the test returns immediately so the default `cargo test` run stays
//! hermetic. `examples/minio/run-smoke.sh` and `examples/seaweedfs/run-smoke.sh`
//! set the variable and drive this test against a live server.
//!
//! The soak measures what production operators pay for: object-store requests per
//! query and per add, query throughput, tail latency, and how a warm decoded
//! segment cache trades resident RAM for a lower request rate. Dataset sizes are
//! overridable with `BORSUK_SOAK_VECTORS` and `BORSUK_SOAK_QUERIES` so CI can pick
//! a budget that fits its time box.

use std::{env, time::Instant};

use borsuk::{BorsukIndex, IndexConfig, LeafMode, OpenOptions, SearchOptions, VectorMetric};
use uuid::Uuid;

const DEFAULT_VECTORS: usize = 2_000;
const DEFAULT_QUERIES: usize = 200;
const DIMENSIONS: usize = 16;
const SEGMENT_MAX_VECTORS: usize = 128;
const ADD_BATCH: usize = 256;
const TOP_K: usize = 10;
/// Budget for the decoded-segment cache in the warm pass. Large enough to retain
/// the segments a repeated query set touches, small enough to stay bounded.
const SEGMENT_CACHE_BYTES: u64 = 64 * 1024 * 1024;

#[test]
fn s3_request_rate_soak_when_configured() {
    let Ok(base_uri) = env::var("BORSUK_S3_TEST_URI") else {
        return;
    };
    let vectors = env_usize("BORSUK_SOAK_VECTORS", DEFAULT_VECTORS);
    let queries = env_usize("BORSUK_SOAK_QUERIES", DEFAULT_QUERIES);
    let uri = format!("{}/{}", base_uri.trim_end_matches('/'), Uuid::new_v4());

    // ---- Build phase: measure requests per add ----------------------------
    let mut index = BorsukIndex::create(IndexConfig {
        uri: uri.clone(),
        metric: VectorMetric::Euclidean,
        dimensions: DIMENSIONS,
        segment_max_vectors: SEGMENT_MAX_VECTORS,
        ram_budget_bytes: None,
    })
    .expect("create index on S3");

    let mut add_requests = 0_u64;
    let mut add_wall = std::time::Duration::ZERO;
    for batch_start in (0..vectors).step_by(ADD_BATCH) {
        let batch_end = (batch_start + ADD_BATCH).min(vectors);
        let ids: Vec<String> = (batch_start..batch_end).map(|id| format!("v{id}")).collect();
        let vecs: Vec<Vec<f32>> = (batch_start..batch_end).map(synthetic_vector).collect();
        let started = Instant::now();
        let (_ids, report) = index
            .add_with_report(vecs, Some(ids))
            .expect("add batch to S3");
        add_wall += started.elapsed();
        add_requests += report.requests.total();
    }
    let requests_per_add = add_requests as f64 / vectors as f64;

    // ---- Cold query phase: paged open, no caches --------------------------
    let cold = BorsukIndex::open(&uri).expect("paged open from S3");
    let cold_stats = run_query_pass(&cold, queries, vectors);

    // ---- Warm query phase: decoded-segment cache trades RAM for requests --
    let warm_index = BorsukIndex::open_with_options(
        &uri,
        OpenOptions {
            segment_cache_max_bytes: Some(SEGMENT_CACHE_BYTES),
            ..OpenOptions::default()
        },
    )
    .expect("paged open with segment cache from S3");
    // Warm the cache with one pass, then measure the second.
    let _ = run_query_pass(&warm_index, queries, vectors);
    let warm = run_query_pass(&warm_index, queries, vectors);

    // ---- Report -----------------------------------------------------------
    let store = env::var("BORSUK_S3_TEST_URI").unwrap_or_default();
    println!("\n=== BORSUK S3 request-rate soak ===");
    println!("store base uri     : {store}");
    println!("vectors added      : {vectors}");
    println!("queries per pass   : {queries}");
    println!(
        "add                : {add_requests} requests total, {requests_per_add:.2} requests/vector, {:.1} adds/s",
        vectors as f64 / add_wall.as_secs_f64().max(f64::MIN_POSITIVE)
    );
    print_stats_line("cold  (paged, no cache)", &cold_stats);
    print_stats_line("warm  (segment cache)  ", &warm);
    println!(
        "cache effect       : requests/query {:.2} -> {:.2} ({:.0}% fewer), hit ratio {:.1}%",
        cold_stats.requests_per_query,
        warm.requests_per_query,
        percent_drop(cold_stats.requests_per_query, warm.requests_per_query),
        warm.cache_hit_ratio * 100.0
    );
    println!("===================================\n");

    // ---- Invariants -------------------------------------------------------
    assert!(add_requests > 0, "adds must issue object-store requests");
    assert!(
        cold_stats.requests_per_query > 0.0,
        "a paged query must issue object-store requests"
    );
    assert!(
        cold_stats.hits_nonempty,
        "every query must return at least one hit"
    );
    assert!(
        warm.total_requests <= cold_stats.total_requests,
        "a warm segment cache must never increase the request count: warm {} > cold {}",
        warm.total_requests,
        cold_stats.total_requests
    );
    // The request rate tracks per-query work, not dataset size: every request is
    // accounted for by the routing pages and segment/graph payloads a single query
    // reads (a couple of requests per object for size probe + fetch), so it stays a
    // small multiple of the objects touched and never approaches the dataset size.
    assert!(
        cold_stats.requests_per_query <= 4.0 * (cold_stats.objects_touched_per_query + 1.0),
        "requests/query {:.2} must stay proportional to the {:.2} objects each query touches",
        cold_stats.requests_per_query,
        cold_stats.objects_touched_per_query
    );
}

struct QueryPass {
    requests_per_query: f64,
    total_requests: u64,
    objects_touched_per_query: f64,
    cache_hit_ratio: f64,
    p50_ms: u64,
    p95_ms: u64,
    qps: f64,
    hits_nonempty: bool,
}

fn run_query_pass(index: &BorsukIndex, queries: usize, vectors: usize) -> QueryPass {
    let mut latencies = Vec::with_capacity(queries);
    let mut total_requests = 0_u64;
    let mut objects_touched = 0_u64;
    let mut cache_hits = 0_u64;
    let mut cache_lookups = 0_u64;
    let mut hits_nonempty = true;
    let wall = Instant::now();
    for q in 0..queries {
        // Query near an existing point so search always finds a leaf to read.
        let target = (q * 7 + 3) % vectors;
        let query = synthetic_vector(target);
        let report = index
            .search_with_report(&query, SearchOptions::approx(TOP_K, LeafMode::PqScan))
            .expect("search against S3");
        hits_nonempty &= !report.hits.is_empty();
        total_requests += report.requests.total();
        objects_touched += (report.routing_page_indexes_read
            + report.routing_pages_read
            + report.segments_searched) as u64;
        cache_hits += report.object_cache_hits as u64;
        cache_lookups += (report.object_cache_hits + report.object_cache_misses) as u64;
        latencies.push(report.elapsed_ms);
    }
    let elapsed = wall.elapsed().as_secs_f64().max(f64::MIN_POSITIVE);
    latencies.sort_unstable();
    QueryPass {
        requests_per_query: total_requests as f64 / queries as f64,
        total_requests,
        objects_touched_per_query: objects_touched as f64 / queries as f64,
        cache_hit_ratio: if cache_lookups == 0 {
            0.0
        } else {
            cache_hits as f64 / cache_lookups as f64
        },
        p50_ms: percentile(&latencies, 50),
        p95_ms: percentile(&latencies, 95),
        qps: queries as f64 / elapsed,
        hits_nonempty,
    }
}

fn print_stats_line(label: &str, pass: &QueryPass) {
    println!(
        "{label} : {:.2} requests/query, {:.0} QPS, p50 {} ms, p95 {} ms",
        pass.requests_per_query, pass.qps, pass.p50_ms, pass.p95_ms
    );
}

fn percentile(sorted: &[u64], pct: usize) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let rank = (pct * (sorted.len() - 1)) / 100;
    sorted[rank]
}

fn percent_drop(from: f64, to: f64) -> f64 {
    if from <= 0.0 {
        return 0.0;
    }
    ((from - to) / from * 100.0).max(0.0)
}

fn env_usize(key: &str, default: usize) -> usize {
    env::var(key)
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

fn synthetic_vector(seed: usize) -> Vec<f32> {
    (0..DIMENSIONS)
        .map(|dim| {
            let x = (seed.wrapping_mul(2_654_435_761).wrapping_add(dim * 40_503)) % 10_007;
            x as f32 / 10_007.0
        })
        .collect()
}
