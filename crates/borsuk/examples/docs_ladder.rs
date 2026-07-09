#![allow(missing_docs)]

//! The example ladder shown on the docs site, from a first search to production
//! tuning. Every snippet the website renders is extracted verbatim from the
//! `docs:` marker regions below, and this example is run in CI, so the code on
//! the page always compiles and runs. Keep the marker regions self-contained and
//! copy-pasteable; put throwaway setup outside the markers.

use borsuk::{
    BorsukIndex, IndexConfig, LeafMode, OpenOptions, SearchOptions, VectorMetric, VectorRecord,
};

fn fresh_dir(name: &str) -> borsuk::Result<String> {
    let dir = std::env::temp_dir().join(name);
    if dir.exists() {
        std::fs::remove_dir_all(&dir).map_err(|source| borsuk::BorsukError::Io {
            path: dir.clone(),
            source,
        })?;
    }
    Ok(dir.to_string_lossy().into_owned())
}

fn main() -> borsuk::Result<()> {
    rung_hello()?;
    rung_report()?;
    rung_tuning()?;
    rung_production()?;
    println!("docs ladder ok");
    Ok(())
}

// Rung 1 — the smallest complete program: create, add, search.
fn rung_hello() -> borsuk::Result<()> {
    let uri = fresh_dir("borsuk-ladder-hello")?;
    // docs:hello:start
    // Create an index. It lives entirely as files under `uri` — a local path
    // here, or an `s3://…` URI for object storage. Nothing else to run.
    let mut index = BorsukIndex::create(IndexConfig {
        uri,
        metric: VectorMetric::Euclidean,
        dimensions: 3,
        segment_max_vectors: 4096,
        ram_budget_bytes: None,
        sparse: false,
    })?;

    // Add a few vectors with your own ids.
    index.add(vec![
        VectorRecord::new("alpha", vec![0.0, 0.0, 0.0]),
        VectorRecord::new("beta", vec![1.0, 0.0, 0.0]),
        VectorRecord::new("gamma", vec![0.0, 5.0, 0.0]),
    ])?;

    // Ask for the 2 nearest neighbours. `exact` returns the true top-k.
    let ids = index.search_ids(&[0.1, 0.0, 0.0], SearchOptions::exact(2))?;
    assert_eq!(ids, ["alpha", "beta"]);
    println!("nearest: {ids:?}");
    // docs:hello:end
    Ok(())
}

// Rung 2 — read the report: what a query actually did, including request rate.
fn rung_report() -> borsuk::Result<()> {
    let uri = fresh_dir("borsuk-ladder-report")?;
    let mut index = BorsukIndex::create(IndexConfig {
        uri,
        metric: VectorMetric::Euclidean,
        dimensions: 3,
        segment_max_vectors: 4096,
        ram_budget_bytes: None,
        sparse: false,
    })?;
    index.add(vec![
        VectorRecord::new("alpha", vec![0.0, 0.0, 0.0]),
        VectorRecord::new("beta", vec![1.0, 0.0, 0.0]),
        VectorRecord::new("gamma", vec![0.0, 5.0, 0.0]),
    ])?;

    // docs:report:start
    // `search_with_report` returns the hits plus everything the query touched:
    // bytes read, segments searched, and the object-store requests it issued.
    let report = index.search_with_report(&[0.1, 0.0, 0.0], SearchOptions::exact(2))?;
    println!(
        "hits={:?} bytes_read={} segments_searched={} requests={} (gets={}, heads={})",
        report
            .hits
            .iter()
            .map(|h| h.id.to_string())
            .collect::<Vec<_>>(),
        report.bytes_read,
        report.segments_searched,
        report.requests.total(),
        report.requests.gets,
        report.requests.heads,
    );
    // docs:report:end
    Ok(())
}

// Rung 4 — tuning: trade recall against I/O with explicit budgets.
fn rung_tuning() -> borsuk::Result<()> {
    let uri = fresh_dir("borsuk-ladder-tuning")?;
    let mut index = BorsukIndex::create(IndexConfig {
        uri,
        metric: VectorMetric::Euclidean,
        dimensions: 3,
        segment_max_vectors: 2,
        ram_budget_bytes: None,
        sparse: false,
    })?;
    index.add(vec![
        VectorRecord::new("alpha", vec![0.0, 0.0, 0.0]),
        VectorRecord::new("beta", vec![1.0, 0.0, 0.0]),
        VectorRecord::new("gamma", vec![0.0, 5.0, 0.0]),
        VectorRecord::new("delta", vec![9.0, 0.0, 0.0]),
    ])?;

    // docs:tuning:start
    // Approximate search spends three explicit budgets instead of hidden magic:
    // how many segments to read, how much routing metadata to look ahead, and how
    // many rows to exact-score per segment. Pick a leaf mode, then tighten budgets
    // while watching the report — smaller budgets read less but can lower recall.
    let query = [0.1, 0.0, 0.0];
    let cheap = index.search_with_report(
        &query,
        SearchOptions::approx(2, LeafMode::PqScan)
            .with_max_segments(1)
            .with_max_candidates_per_segment(2),
    )?;
    let thorough = index.search_with_report(
        &query,
        SearchOptions::approx(2, LeafMode::PqScan)
            .with_max_segments(8)
            .with_routing_page_overfetch(8),
    )?;
    println!(
        "cheap: {} segments, {} bytes | thorough: {} segments, {} bytes",
        cheap.segments_searched, cheap.bytes_read, thorough.segments_searched, thorough.bytes_read,
    );
    // docs:tuning:end
    Ok(())
}

// Rung 5 — production: bound memory under concurrency and watch the request rate.
fn rung_production() -> borsuk::Result<()> {
    let uri = fresh_dir("borsuk-ladder-production")?;
    let mut index = BorsukIndex::create(IndexConfig {
        uri: uri.clone(),
        metric: VectorMetric::Euclidean,
        dimensions: 3,
        segment_max_vectors: 4096,
        ram_budget_bytes: None,
        sparse: false,
    })?;
    index.add(vec![
        VectorRecord::new("alpha", vec![0.0, 0.0, 0.0]),
        VectorRecord::new("beta", vec![1.0, 0.0, 0.0]),
        VectorRecord::new("gamma", vec![0.0, 5.0, 0.0]),
    ])?;
    drop(index);

    // docs:production:start
    // Open for serving. Paged routing (the default) keeps resident memory near
    // zero. A shared decoded-segment cache trades a fixed RAM budget for fewer
    // object-store reads on hot segments, and a concurrency cap bounds peak
    // working memory so 1000 callers don't mean 1000× RAM.
    let index = BorsukIndex::open_with_options(
        &uri,
        OpenOptions {
            segment_cache_max_bytes: Some(256 * 1024 * 1024),
            max_concurrent_searches: Some(64),
            ..OpenOptions::default()
        },
    )?;

    // Every report carries the object-store requests it issued, so you can chart
    // requests-per-query straight from production traffic.
    let report =
        index.search_with_report(&[0.1, 0.0, 0.0], SearchOptions::approx(2, LeafMode::PqScan))?;
    println!(
        "requests/query: {} (gets={}, heads={}, lists={})",
        report.requests.total(),
        report.requests.gets,
        report.requests.heads,
        report.requests.lists,
    );
    // docs:production:end
    Ok(())
}
