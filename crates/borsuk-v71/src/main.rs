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
//!
//! Routing is by resident per-row product-quantised codes rather than by page
//! summaries. V72 measured the difference offline: at 512 shortlisted rows a
//! 64-byte row code reaches 99.676% page containment in 21 requests and 27 MiB
//! where page summaries needed 69 requests and 63 MiB for 99.185%. The router
//! picks rows and their pages follow, instead of picking pages and hoping.

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
use rayon::prelude::*;
use wide::f32x8;
use object_store::{GetOptions, GetRange, ObjectStore, parse_url_opts, path::Path as ObjectPath};
use url::Url;

type BenchResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

const MAGIC: &[u8; 8] = b"BRSKV73\0";

struct Manifest {
    rows: usize,
    dimensions: usize,
    page_rows: usize,
    pages: usize,
    queries: usize,
    neighbors: usize,
    subspaces: usize,
    width: usize,
    low: Vec<f32>,
    span_step: Vec<f32>,
    codebooks: Vec<f32>,
    row_codes: Vec<u8>,
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
    let pages = read_u64(&bytes, &mut cursor) as usize;
    let queries = read_u64(&bytes, &mut cursor) as usize;
    let neighbors = read_u64(&bytes, &mut cursor) as usize;
    let subspaces = read_u64(&bytes, &mut cursor) as usize;
    let width = read_u64(&bytes, &mut cursor) as usize;
    if subspaces * width != dimensions {
        return Err("codebook subspaces times width differs from the dimension".into());
    }
    let low = read_f32_vec(&bytes, &mut cursor, dimensions);
    let span_step = read_f32_vec(&bytes, &mut cursor, dimensions);
    let codebooks = read_f32_vec(&bytes, &mut cursor, subspaces * 256 * width);
    let row_codes = bytes[cursor..cursor + rows * subspaces].to_vec();
    cursor += rows * subspaces;
    let query_vectors = read_f32_vec(&bytes, &mut cursor, queries * dimensions);
    let truth = read_i64_vec(&bytes, &mut cursor, queries * neighbors);
    if cursor != bytes.len() {
        return Err(format!("manifest has {} trailing bytes", bytes.len() - cursor).into());
    }
    Ok(Manifest {
        rows,
        dimensions,
        page_rows,
        pages,
        queries,
        neighbors,
        subspaces,
        width,
        low,
        span_step,
        codebooks,
        row_codes,
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

fn route(manifest: &Manifest, query: &[f32], shortlist: usize) -> Vec<usize> {
    // Asymmetric distance table: one squared distance per subspace codeword,
    // 64 KiB for a 64-subspace codebook, so the whole scan runs out of L2.
    let subspaces = manifest.subspaces;
    let width = manifest.width;
    let mut table = vec![0.0f32; subspaces * 256];
    for subspace in 0..subspaces {
        let slice = &query[subspace * width..(subspace + 1) * width];
        for codeword in 0..256 {
            let base = (subspace * 256 + codeword) * width;
            let centre = &manifest.codebooks[base..base + width];
            let mut squared = 0.0f32;
            for index in 0..width {
                let delta = centre[index] - slice[index];
                squared += delta * delta;
            }
            table[subspace * 256 + codeword] = squared;
        }
    }

    let mut scored: Vec<(f32, u32)> = manifest
        .row_codes
        .par_chunks_exact(subspaces)
        .enumerate()
        .map(|(row, codes)| {
            let mut total = 0.0f32;
            for subspace in 0..subspaces {
                total += table[subspace * 256 + usize::from(codes[subspace])];
            }
            (total, row as u32)
        })
        .collect();
    let take = shortlist.min(scored.len());
    scored.select_nth_unstable_by(take - 1, |a, b| a.0.total_cmp(&b.0));
    scored.truncate(take);

    let mut pages: Vec<usize> = scored
        .into_iter()
        .map(|(_, row)| row as usize / manifest.page_rows)
        .collect();
    pages.sort_unstable();
    pages.dedup();
    pages
}

/// One row's code-against-weight dot product, eight lanes at a time.
///
/// The scalar form of this loop cost 81 ms per query at M=128 - more than the
/// object-store I/O it was waiting on - because widening a byte to a float one
/// element at a time does not vectorise.
fn fused_inner(codes: &[u8], weights: &[f32]) -> f32 {
    let lanes = codes.len() / 8 * 8;
    let mut accumulator = f32x8::ZERO;
    for offset in (0..lanes).step_by(8) {
        let widened = f32x8::new([
            f32::from(codes[offset]),
            f32::from(codes[offset + 1]),
            f32::from(codes[offset + 2]),
            f32::from(codes[offset + 3]),
            f32::from(codes[offset + 4]),
            f32::from(codes[offset + 5]),
            f32::from(codes[offset + 6]),
            f32::from(codes[offset + 7]),
        ]);
        let scale = f32x8::new([
            weights[offset],
            weights[offset + 1],
            weights[offset + 2],
            weights[offset + 3],
            weights[offset + 4],
            weights[offset + 5],
            weights[offset + 6],
            weights[offset + 7],
        ]);
        accumulator = widened.mul_add(scale, accumulator);
    }
    let mut total = accumulator.reduce_add();
    for offset in lanes..codes.len() {
        total += f32::from(codes[offset]) * weights[offset];
    }
    total
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

    let mut best: Vec<(f32, i64)> = blobs
        .par_iter()
        .flat_map_iter(|(_, body)| {
            let count = body.len() / row_bytes;
            (0..count).map(move |row| {
                let base = row * row_bytes;
                let identifier =
                    i64::from_le_bytes(body[base..base + 8].try_into().expect("8 bytes"));
                let norm =
                    f32::from_le_bytes(body[base + 8..base + 12].try_into().expect("4 bytes"));
                let codes = &body[base + 12..base + row_bytes];
                (norm, identifier, codes)
            })
        })
        .map(|(norm, identifier, codes)| {
            (norm - 2.0 * (fused_inner(codes, &weights) + shift), identifier)
        })
        .collect();
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
    // The vectorised dot product must agree with the scalar form it replaced,
    // or every score is quietly wrong.
    let mut codes = Vec::new();
    let mut weights = Vec::new();
    let mut state = 12_345u64;
    for index in 0..775usize {
        state = state.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
        codes.push((state >> 33) as u8);
        weights.push(((state >> 20) as u32 % 2_000) as f32 / 1_000.0 - 1.0);
        if index >= 768 {
            let scalar: f32 = codes
                .iter()
                .zip(weights.iter())
                .map(|(code, weight)| f32::from(*code) * weight)
                .sum();
            let vectorised = fused_inner(&codes, &weights);
            if (scalar - vectorised).abs() > scalar.abs().max(1.0) * 1e-4 {
                return Err(format!(
                    "vectorised inner product {vectorised} differs from scalar {scalar} at width {}",
                    codes.len()
                )
                .into());
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
    let budget = optional_usize("BORSUK_V71_SHORTLIST", 512)?;
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
        "schema": "borsuk-v73-row-router-reader-result-v1",
        "claim_eligible": false,
        "evidence_kind": "measured-native-single-round-trip-sq8-with-resident-row-router",
        "storage": "real-object-store-ranged-gets-no-local-cache",
        "cpu_path": "safe-rust-simd-scan-and-adc-router",
        "rows": manifest.rows,
        "dimensions": manifest.dimensions,
        "page_rows": manifest.page_rows,
        "shortlist_rows": budget,
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
