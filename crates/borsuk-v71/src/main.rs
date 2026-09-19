//! Native single-round-trip SQ8 reader over object storage.
//!
//! V70 measured 140.2 ms and 39 requests per query for this design through a
//! Python and boto3 client, against 233.2 ms and 83 requests for the two-stage
//! PQ192 alternative at matched recall. That comparison could not be trusted to
//! choose between them: interpreter and client overhead per request is far
//! larger than a native client's, which inflates exactly the quantity the trade
//! turns on.
//!
//! This reads the same published objects with the crate's own object-store
//! client so the request-versus-bytes trade is measured where it will actually
//! be paid. Scoring never reconstructs a vector: each row carries its squared
//! norm, and the per-dimension scale folds into the query once.

use std::{
    env,
    error::Error,
    fs,
    ops::Range,
    path::PathBuf,
    sync::Arc,
    time::Instant,
};

use futures_util::stream::{self, StreamExt};
use object_store::{GetOptions, GetRange, ObjectStore, parse_url_opts, path::Path as ObjectPath};
use url::Url;

type BenchResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

const MAGIC: &[u8; 8] = b"BRSKV71\0";

struct Manifest {
    rows: usize,
    dimensions: usize,
    page_rows: usize,
    blocks_per_page: usize,
    pages: usize,
    queries: usize,
    neighbors: usize,
    low: Vec<f32>,
    span_step: Vec<f32>,
    summaries: Vec<f32>,
    query_vectors: Vec<f32>,
    truth: Vec<i64>,
}

fn read_u64(bytes: &[u8], cursor: &mut usize) -> u64 {
    let value = u64::from_le_bytes(bytes[*cursor..*cursor + 8].try_into().expect("8 bytes"));
    *cursor += 8;
    value
}

fn read_f32_vec(bytes: &[u8], cursor: &mut usize, count: usize) -> Vec<f32> {
    let mut out = Vec::with_capacity(count);
    for index in 0..count {
        let at = *cursor + index * 4;
        out.push(f32::from_le_bytes(bytes[at..at + 4].try_into().expect("4 bytes")));
    }
    *cursor += count * 4;
    out
}

fn read_i64_vec(bytes: &[u8], cursor: &mut usize, count: usize) -> Vec<i64> {
    let mut out = Vec::with_capacity(count);
    for index in 0..count {
        let at = *cursor + index * 8;
        out.push(i64::from_le_bytes(bytes[at..at + 8].try_into().expect("8 bytes")));
    }
    *cursor += count * 8;
    out
}

fn load_manifest(path: &PathBuf) -> BenchResult<Manifest> {
    let bytes = fs::read(path)?;
    if bytes.len() < 8 || &bytes[..8] != MAGIC {
        return Err("manifest magic differs".into());
    }
    let mut cursor = 8;
    let rows = read_u64(&bytes, &mut cursor) as usize;
    let dimensions = read_u64(&bytes, &mut cursor) as usize;
    let page_rows = read_u64(&bytes, &mut cursor) as usize;
    let blocks_per_page = read_u64(&bytes, &mut cursor) as usize;
    let pages = read_u64(&bytes, &mut cursor) as usize;
    let queries = read_u64(&bytes, &mut cursor) as usize;
    let neighbors = read_u64(&bytes, &mut cursor) as usize;
    let low = read_f32_vec(&bytes, &mut cursor, dimensions);
    let span_step = read_f32_vec(&bytes, &mut cursor, dimensions);
    let summaries = read_f32_vec(&bytes, &mut cursor, pages * blocks_per_page * dimensions);
    let query_vectors = read_f32_vec(&bytes, &mut cursor, queries * dimensions);
    let truth = read_i64_vec(&bytes, &mut cursor, queries * neighbors);
    if cursor != bytes.len() {
        return Err(format!("manifest has {} trailing bytes", bytes.len() - cursor).into());
    }
    Ok(Manifest {
        rows,
        dimensions,
        page_rows,
        blocks_per_page,
        pages,
        queries,
        neighbors,
        low,
        span_step,
        summaries,
        query_vectors,
        truth,
    })
}

/// Pages whose runs are separated by at most `gap` unselected pages become one
/// request, paying for the gap in bytes.
fn coalesce(sorted: &[usize], gap: usize) -> Vec<Range<usize>> {
    let mut ranges = Vec::new();
    let mut iterator = sorted.iter().copied();
    let Some(mut start) = iterator.next() else {
        return ranges;
    };
    let mut end = start;
    for page in iterator {
        if page <= end + gap + 1 {
            end = page;
        } else {
            ranges.push(start..end + 1);
            start = page;
            end = page;
        }
    }
    ranges.push(start..end + 1);
    ranges
}

fn route(manifest: &Manifest, query: &[f32], budget: usize) -> Vec<usize> {
    let dimensions = manifest.dimensions;
    let blocks = manifest.pages * manifest.blocks_per_page;
    let mut page_scores = vec![f32::INFINITY; manifest.pages];
    for block in 0..blocks {
        let offset = block * dimensions;
        let summary = &manifest.summaries[offset..offset + dimensions];
        let mut squared = 0.0f32;
        let mut inner = 0.0f32;
        for index in 0..dimensions {
            squared += summary[index] * summary[index];
            inner += summary[index] * query[index];
        }
        let score = squared - 2.0 * inner;
        let page = block / manifest.blocks_per_page;
        if score < page_scores[page] {
            page_scores[page] = score;
        }
    }
    let mut ordered: Vec<usize> = (0..manifest.pages).collect();
    let take = budget.min(ordered.len());
    ordered.select_nth_unstable_by(take - 1, |a, b| page_scores[*a].total_cmp(&page_scores[*b]));
    ordered.truncate(take);
    ordered.sort_unstable();
    ordered
}

struct QueryOutcome {
    returned: Vec<i64>,
    requests: usize,
    bytes: usize,
    route_ms: f64,
    io_ms: f64,
    scan_ms: f64,
}

async fn search(
    store: &Arc<dyn ObjectStore>,
    key: &ObjectPath,
    manifest: &Manifest,
    query: &[f32],
    budget: usize,
    gap: usize,
    concurrency: usize,
) -> BenchResult<QueryOutcome> {
    let row_bytes = 8 + 4 + manifest.dimensions;
    let started = Instant::now();
    let chosen = route(manifest, query, budget);
    let ranges = coalesce(&chosen, gap);
    let route_ms = started.elapsed().as_secs_f64() * 1000.0;

    let started = Instant::now();
    let byte_ranges: Vec<Range<usize>> = ranges
        .iter()
        .map(|range| {
            let first = range.start * manifest.page_rows;
            let last = (range.end * manifest.page_rows).min(manifest.rows);
            first * row_bytes..last * row_bytes
        })
        .collect();
    let requests = byte_ranges.len();
    let blobs: Vec<(usize, bytes::Bytes)> = stream::iter(byte_ranges.into_iter().map(|range| {
        let store = Arc::clone(store);
        let key = key.clone();
        async move {
            let options = GetOptions {
                range: Some(GetRange::Bounded(range.start as u64..range.end as u64)),
                ..GetOptions::default()
            };
            let payload = store.get_opts(&key, options).await?.bytes().await?;
            Ok::<(usize, bytes::Bytes), object_store::Error>((range.start, payload))
        }
    }))
    .buffer_unordered(concurrency)
    .collect::<Vec<_>>()
    .await
    .into_iter()
    .collect::<Result<Vec<_>, _>>()?;
    let io_ms = started.elapsed().as_secs_f64() * 1000.0;
    let bytes_read: usize = blobs.iter().map(|(_, body)| body.len()).sum();

    let started = Instant::now();
    // ||q-x||^2 = ||x||^2 - 2(code . weights + shift), with the per-dimension
    // scale folded into the query once so no row is ever reconstructed.
    let mut weights = vec![0.0f32; manifest.dimensions];
    let mut shift = 0.0f32;
    let mut query_norm = 0.0f32;
    for index in 0..manifest.dimensions {
        weights[index] = query[index] * manifest.span_step[index];
        shift += query[index] * manifest.low[index];
        query_norm += query[index] * query[index];
    }
    shift -= query_norm / 2.0;

    let mut best: Vec<(f32, i64)> = Vec::new();
    for (_, body) in &blobs {
        let count = body.len() / row_bytes;
        for row in 0..count {
            let base = row * row_bytes;
            let identifier =
                i64::from_le_bytes(body[base..base + 8].try_into().expect("8 bytes"));
            let norm = f32::from_le_bytes(body[base + 8..base + 12].try_into().expect("4 bytes"));
            let codes = &body[base + 12..base + row_bytes];
            let mut inner = 0.0f32;
            for index in 0..manifest.dimensions {
                inner += f32::from(codes[index]) * weights[index];
            }
            best.push((norm - 2.0 * (inner + shift), identifier));
        }
    }
    let take = manifest.neighbors.min(best.len());
    if take > 0 {
        best.select_nth_unstable_by(take - 1, |a, b| a.0.total_cmp(&b.0));
        best.truncate(take);
    }
    let returned = best.into_iter().map(|(_, id)| id).collect();
    let scan_ms = started.elapsed().as_secs_f64() * 1000.0;

    Ok(QueryOutcome {
        returned,
        requests,
        bytes: bytes_read,
        route_ms,
        io_ms,
        scan_ms,
    })
}

fn percentile(values: &[f64], quantile: f64) -> f64 {
    let mut ordered = values.to_vec();
    ordered.sort_by(f64::total_cmp);
    ordered[((ordered.len() - 1) as f64 * quantile).round() as usize]
}

fn required(name: &str) -> BenchResult<String> {
    env::var(name).map_err(|_| format!("missing required environment variable {name}").into())
}

fn optional_usize(name: &str, fallback: usize) -> BenchResult<usize> {
    match env::var(name) {
        Ok(value) => Ok(value.parse()?),
        Err(_) => Ok(fallback),
    }
}

fn self_test() -> BenchResult<()> {
    let ranges = coalesce(&[3, 4, 5, 9, 10, 40], 0);
    if ranges != vec![3..6, 9..11, 40..41] {
        return Err(format!("adjacent-only coalescing differs: {ranges:?}").into());
    }
    let ranges = coalesce(&[3, 4, 5, 9, 10, 40], 3);
    if ranges != vec![3..11, 40..41] {
        return Err(format!("gap-3 coalescing differs: {ranges:?}").into());
    }
    if !coalesce(&[], 4).is_empty() {
        return Err("empty coalescing differs".into());
    }
    if coalesce(&[7], 0) != vec![7..8] {
        return Err("single-page coalescing differs".into());
    }
    // Every selected page must survive into some range, at every gap.
    for gap in [0usize, 1, 2, 8, 64] {
        let selected = [1usize, 2, 17, 18, 19, 200, 201, 999];
        let ranges = coalesce(&selected, gap);
        for page in selected {
            if !ranges.iter().any(|range| range.contains(&page)) {
                return Err(format!("page {page} lost at gap {gap}").into());
            }
        }
    }
    println!("{{\"self_test\":\"passed\"}}");
    Ok(())
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> BenchResult<()> {
    if env::args().any(|argument| argument == "--self-test") {
        return self_test();
    }
    let uri = required("BORSUK_V71_URI")?;
    let region = required("BORSUK_V71_REGION")?;
    let manifest_path = PathBuf::from(required("BORSUK_V71_MANIFEST")?);
    let output = PathBuf::from(required("BORSUK_V71_OUTPUT")?);
    let budget = optional_usize("BORSUK_V71_PAGES", 128)?;
    let gap = optional_usize("BORSUK_V71_GAP", 8)?;
    let concurrency = optional_usize("BORSUK_V71_CONCURRENCY", 64)?;
    let measured = optional_usize("BORSUK_V71_QUERIES", 200)?;

    let manifest = load_manifest(&manifest_path)?;
    let url = Url::parse(&uri)?;
    let (store, key) = parse_url_opts(&url, [("region".to_string(), region.clone())])?;
    let store: Arc<dyn ObjectStore> = Arc::from(store);

    let measured = measured.min(manifest.queries);
    let mut totals = Vec::with_capacity(measured);
    let mut io = Vec::with_capacity(measured);
    let mut scan = Vec::with_capacity(measured);
    let mut route_times = Vec::with_capacity(measured);
    let mut requests = Vec::with_capacity(measured);
    let mut bytes_seen = Vec::with_capacity(measured);
    let mut hits = 0usize;
    let mut worst = manifest.neighbors;

    for index in 0..measured {
        let offset = index * manifest.dimensions;
        let query = &manifest.query_vectors[offset..offset + manifest.dimensions];
        let outcome = search(
            &store,
            &key,
            &manifest,
            query,
            budget,
            gap,
            concurrency,
        )
        .await?;
        let truth_offset = index * manifest.neighbors;
        let truth = &manifest.truth[truth_offset..truth_offset + manifest.neighbors];
        let found = outcome
            .returned
            .iter()
            .filter(|identifier| truth.contains(identifier))
            .count();
        hits += found;
        worst = worst.min(found);
        totals.push(outcome.route_ms + outcome.io_ms + outcome.scan_ms);
        io.push(outcome.io_ms);
        scan.push(outcome.scan_ms);
        route_times.push(outcome.route_ms);
        requests.push(outcome.requests as f64);
        bytes_seen.push(outcome.bytes as f64);
    }

    let report = serde_json::json!({
        "schema": "borsuk-v71-native-reader-result-v1",
        "claim_eligible": false,
        "evidence_kind": "measured-native-object-store-single-round-trip-sq8",
        "storage": "real-object-store-ranged-gets-no-local-cache",
        "cpu_path": "safe-rust-scalar-scan",
        "rows": manifest.rows,
        "dimensions": manifest.dimensions,
        "page_rows": manifest.page_rows,
        "router_pages": budget,
        "gap_pages": gap,
        "concurrency": concurrency,
        "queries": measured,
        "recall": {
            "aggregate_ppm": (hits as f64 * 1_000_000.0
                / (measured * manifest.neighbors) as f64).round() as u64,
            "worst_ppm": (worst * 10_000) as u64,
        },
        "latency_ms": {
            "total_p50": percentile(&totals, 0.50),
            "total_p95": percentile(&totals, 0.95),
            "total_p99": percentile(&totals, 0.99),
            "io_p50": percentile(&io, 0.50),
            "io_p95": percentile(&io, 0.95),
            "scan_p50": percentile(&scan, 0.50),
            "route_p50": percentile(&route_times, 0.50),
        },
        "requests_p50": percentile(&requests, 0.50),
        "requests_p95": percentile(&requests, 0.95),
        "bytes_p50": percentile(&bytes_seen, 0.50),
    });
    fs::write(&output, format!("{report}\n"))?;
    println!("{report}");
    Ok(())
}
