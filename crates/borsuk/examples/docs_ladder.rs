#![allow(missing_docs)]

//! The example ladder shown on the docs site, from a first search to production
//! tuning. Every snippet the website renders is extracted verbatim from the
//! `docs:` marker regions below, and this example is run in CI, so the code on
//! the page always compiles and runs. Keep the marker regions self-contained and
//! copy-pasteable; put throwaway setup outside the markers.

use borsuk::{
    BorsukIndex, Filter, HybridOptions, HybridQuery, IndexConfig, LeafMode, MetaValue, Metadata,
    OpenOptions, SearchOptions, VectorMetric, VectorRecord,
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
    rung_filter()?;
    rung_upsert()?;
    rung_hybrid()?;
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
        text: false,
        named_vectors: Default::default(),
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
        text: false,
        named_vectors: Default::default(),
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

// Rung 3 — filter by metadata: matches are found before ranking.
fn rung_filter() -> borsuk::Result<()> {
    let uri = fresh_dir("borsuk-ladder-filter")?;
    let mut index = BorsukIndex::create(IndexConfig {
        uri,
        metric: VectorMetric::Euclidean,
        dimensions: 2,
        segment_max_vectors: 4096,
        ram_budget_bytes: None,
        text: false,
        named_vectors: Default::default(),
    })?;

    // docs:filter:start
    // Attach schemaless metadata to any vector, then constrain a search with a
    // Pinecone-style operator dict. The filter is applied *before* ranking, so a
    // selective filter is fast and exact — whole segments that cannot match are
    // skipped unread.
    let genre = |value: &str| {
        let mut meta = Metadata::new();
        meta.insert("genre".into(), MetaValue::Str(value.into()));
        meta
    };
    index.add(vec![
        VectorRecord::new("a", vec![0.0, 0.0]).with_metadata(genre("comedy")),
        VectorRecord::new("b", vec![0.1, 0.0]).with_metadata(genre("drama")),
        VectorRecord::new("c", vec![0.2, 0.0]).with_metadata(genre("comedy")),
    ])?;

    let filter =
        Filter::from_json(&serde_json::json!({ "genre": { "$eq": "comedy" } })).expect("valid");
    let ids = index.search_ids(&[0.0, 0.0], SearchOptions::exact(5).with_filter(filter))?;
    assert_eq!(ids, ["a", "c"]);
    println!("filtered (genre=comedy): {ids:?}");
    // docs:filter:end
    Ok(())
}

// Rung 4 — update in place: `upsert` overwrites a record by id, atomically.
fn rung_upsert() -> borsuk::Result<()> {
    let uri = fresh_dir("borsuk-ladder-upsert")?;
    let mut index = BorsukIndex::create(IndexConfig {
        uri,
        metric: VectorMetric::Euclidean,
        dimensions: 2,
        segment_max_vectors: 4096,
        ram_budget_bytes: None,
        text: false,
        named_vectors: Default::default(),
    })?;

    // docs:upsert:start
    // `add` is insert-only; `upsert` inserts-or-replaces by id in one atomic
    // publish. Reads immediately see only the new version, and there is only ever
    // one live copy of an id — the superseded one is reclaimed by compaction.
    index.add(vec![
        VectorRecord::new("a", vec![0.0, 0.0]),
        VectorRecord::new("b", vec![1.0, 0.0]),
    ])?;
    index.upsert(vec![VectorRecord::new("a", vec![0.0, 9.0])])?; // move "a" away

    let near_origin = index.search_ids(&[0.0, 0.0], SearchOptions::exact(3))?;
    assert_eq!(near_origin[0], "b"); // "a" is now far from the origin
    assert_eq!(near_origin.iter().filter(|id| *id == "a").count(), 1);
    println!("after upsert, nearest origin: {near_origin:?}");
    // docs:upsert:end
    Ok(())
}

// Rung 5 — hybrid search: fuse a dense vector leg with a BM25 text leg.
fn rung_hybrid() -> borsuk::Result<()> {
    let uri = fresh_dir("borsuk-ladder-hybrid")?;
    let mut index = BorsukIndex::create(IndexConfig {
        uri,
        metric: VectorMetric::Euclidean,
        dimensions: 2,
        segment_max_vectors: 4096,
        ram_budget_bytes: None,
        text: true,
        named_vectors: Default::default(),
    })?;

    // docs:hybrid:start
    // Turn on `text` to index BM25 alongside the vectors, then fuse both legs in
    // one query. Reciprocal-rank fusion (the default) needs no tuning; switch to
    // weighted fusion when you want to lean on one leg.
    index.add(vec![
        VectorRecord::new("a", vec![0.0, 0.0]).with_text("red apple"),
        VectorRecord::new("b", vec![1.0, 0.0]).with_text("green apple pie"),
        VectorRecord::new("c", vec![0.0, 1.0]).with_text("blue sky"),
    ])?;

    let query = HybridQuery::new()
        .with_vector("", vec![0.0, 0.0])
        .with_text("apple");
    let report = index.search_hybrid(&query, HybridOptions::new(3))?;
    let ids: Vec<_> = report.hits.iter().map(|hit| hit.id.to_string()).collect();
    assert!(!ids.is_empty());
    println!("hybrid (dense + text): {ids:?}");
    // docs:hybrid:end
    Ok(())
}

// Rung 6 — tuning: trade recall against I/O with explicit budgets.
fn rung_tuning() -> borsuk::Result<()> {
    let uri = fresh_dir("borsuk-ladder-tuning")?;
    let mut index = BorsukIndex::create(IndexConfig {
        uri,
        metric: VectorMetric::Euclidean,
        dimensions: 3,
        segment_max_vectors: 2,
        ram_budget_bytes: None,
        text: false,
        named_vectors: Default::default(),
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

// Rung 8 — production: bound memory under concurrency and watch the request rate.
fn rung_production() -> borsuk::Result<()> {
    let uri = fresh_dir("borsuk-ladder-production")?;
    let mut index = BorsukIndex::create(IndexConfig {
        uri: uri.clone(),
        metric: VectorMetric::Euclidean,
        dimensions: 3,
        segment_max_vectors: 4096,
        ram_budget_bytes: None,
        text: false,
        named_vectors: Default::default(),
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
