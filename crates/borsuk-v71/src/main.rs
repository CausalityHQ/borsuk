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
//! Routing is hierarchical. V76 measured the flat version's ceiling: scoring
//! every row on every query is 64M table lookups at 1M rows, about 206 core-ms,
//! which caps a 48-core node near 107 QPS and would cost roughly 430 ms per
//! query at 100M. The router was O(N), and that - not its resident footprint -
//! was the scale wall.
//!
//! So a first level scores page summaries and keeps the best regions, and the
//! per-row codes are scanned only inside them. Rows sit in k-means chain order,
//! so a contiguous region is geometrically coherent and the first level loses
//! little. The second level then picks rows and their pages follow, which V72
//! showed beats picking pages directly: at 512 shortlisted rows a 64-byte row
//! code reached 99.676% containment in 21 requests where page summaries needed
//! 69 for 99.185%.

use std::{
    collections::HashSet,
    env,
    error::Error,
    fs::{self, File},
    future::Future,
    ops::Range,
    path::{Path, PathBuf},
    sync::Arc,
    time::Instant,
};

use arrow_array::{
    ArrayRef, FixedSizeListArray, Int64Array, RecordBatch, StringArray, UInt32Array, UInt64Array,
};
use arrow_schema::{DataType, Field, Schema};
use futures_util::stream::{self, StreamExt};
use object_store::{GetOptions, GetRange, ObjectStore, parse_url_opts, path::Path as ObjectPath};
use parquet::arrow::ArrowWriter;
use rayon::prelude::*;
use serde::Serialize;
use sha2::{Digest, Sha256};
use url::Url;
use wide::f32x8;

type BenchResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

const MAGIC: &[u8; 8] = b"BRSKV77\0";

struct Manifest {
    rows: usize,
    dimensions: usize,
    page_rows: usize,
    pages: usize,
    queries: usize,
    neighbors: usize,
    subspaces: usize,
    width: usize,
    blocks_per_page: usize,
    summaries: Vec<f32>,
    low: Vec<f32>,
    span_step: Vec<f32>,
    codebooks: Vec<f32>,
    row_codes: Vec<u8>,
    query_vectors: Vec<f32>,
    truth: Vec<i64>,
}

#[derive(Clone, Copy)]
enum InQueryCpu {
    Rayon,
    Sequential,
}

impl InQueryCpu {
    fn label(self) -> &'static str {
        match self {
            Self::Rayon => "rayon",
            Self::Sequential => "sequential",
        }
    }
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
        out.push(f32::from_le_bytes(
            bytes[at..at + 4].try_into().expect("4 bytes"),
        ));
    }
    *cursor += count * 4;
    out
}

fn read_i64_vec(bytes: &[u8], cursor: &mut usize, count: usize) -> Vec<i64> {
    let mut out = Vec::with_capacity(count);
    for index in 0..count {
        let at = *cursor + index * 8;
        out.push(i64::from_le_bytes(
            bytes[at..at + 8].try_into().expect("8 bytes"),
        ));
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
    let blocks_per_page = read_u64(&bytes, &mut cursor) as usize;
    if subspaces * width != dimensions {
        return Err("codebook subspaces times width differs from the dimension".into());
    }
    let summaries = read_f32_vec(&bytes, &mut cursor, pages * blocks_per_page * dimensions);
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
        blocks_per_page,
        summaries,
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

/// First level: keep the `regions` best pages by their summaries.
///
/// Dense and small - two 768-dimensional summaries per 256-row page is one
/// dot product per 128 rows - so this stays affordable where scanning every
/// row does not.
fn coarse_regions(
    manifest: &Manifest,
    query: &[f32],
    regions: usize,
    in_query_cpu: InQueryCpu,
) -> Vec<usize> {
    let dimensions = manifest.dimensions;
    let blocks = manifest.pages * manifest.blocks_per_page;
    let score = |block| {
        let summary = &manifest.summaries[block * dimensions..(block + 1) * dimensions];
        let mut squared = 0.0f32;
        let mut inner = 0.0f32;
        for index in 0..dimensions {
            squared += summary[index] * summary[index];
            inner += summary[index] * query[index];
        }
        squared - 2.0 * inner
    };
    let page_scores: Vec<f32> = match in_query_cpu {
        InQueryCpu::Rayon => (0..blocks).into_par_iter().map(score).collect(),
        InQueryCpu::Sequential => (0..blocks).map(score).collect(),
    };
    // A page scores as the best of its blocks.
    let mut best = vec![f32::INFINITY; manifest.pages];
    for (block, page_score) in page_scores.into_iter().enumerate() {
        let page = block / manifest.blocks_per_page;
        if page_score < best[page] {
            best[page] = page_score;
        }
    }
    let mut ordered: Vec<usize> = (0..manifest.pages).collect();
    let take = regions.min(ordered.len());
    ordered.select_nth_unstable_by(take - 1, |a, b| best[*a].total_cmp(&best[*b]));
    ordered.truncate(take);
    ordered
}

fn route(
    manifest: &Manifest,
    query: &[f32],
    shortlist: usize,
    regions: usize,
    in_query_cpu: InQueryCpu,
) -> Vec<usize> {
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

    // Second level: score only the rows inside the regions the first level kept.
    let candidates = coarse_regions(manifest, query, regions, in_query_cpu);
    let table = &table;
    let score_page = |page: &usize| {
        let first = page * manifest.page_rows;
        let last = ((page + 1) * manifest.page_rows).min(manifest.rows);
        (first..last).map(move |row| {
            let codes = &manifest.row_codes[row * subspaces..(row + 1) * subspaces];
            let mut total = 0.0f32;
            for subspace in 0..subspaces {
                total += table[subspace * 256 + usize::from(codes[subspace])];
            }
            (total, row as u32)
        })
    };
    let mut scored: Vec<(f32, u32)> = match in_query_cpu {
        InQueryCpu::Rayon => candidates.par_iter().flat_map_iter(score_page).collect(),
        InQueryCpu::Sequential => candidates.iter().flat_map(score_page).collect(),
    };
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
/// The scalar form cost 81 ms per query at M=128 - more than the object-store
/// I/O it was waiting on - because widening a byte to a float one element at a
/// time does not vectorise. Eight-lane accumulation took that to 2.5 ms, and
/// V77 then showed the scan still moving 10.3 MiB to do 20 MFLOP at 0.16
/// GFLOP/s per core, so it was not compute bound either: the lanes were being
/// assembled by eight bounds-checked loads apiece.
///
/// Taking fixed-size arrays out of the slices removes those checks and lets the
/// widening compile to a single load-and-convert.
fn fused_inner(codes: &[u8], weights: &[f32]) -> f32 {
    let mut accumulator = f32x8::ZERO;
    let mut second = f32x8::ZERO;
    let mut offset = 0;
    while offset + 16 <= codes.len() {
        let low: [u8; 8] = codes[offset..offset + 8].try_into().expect("8 bytes");
        let high: [u8; 8] = codes[offset + 8..offset + 16].try_into().expect("8 bytes");
        let low_weights: [f32; 8] = weights[offset..offset + 8].try_into().expect("8 floats");
        let high_weights: [f32; 8] = weights[offset + 8..offset + 16]
            .try_into()
            .expect("8 floats");
        accumulator =
            f32x8::from(low.map(f32::from)).mul_add(f32x8::from(low_weights), accumulator);
        second = f32x8::from(high.map(f32::from)).mul_add(f32x8::from(high_weights), second);
        offset += 16;
    }
    while offset + 8 <= codes.len() {
        let lane: [u8; 8] = codes[offset..offset + 8].try_into().expect("8 bytes");
        let scale: [f32; 8] = weights[offset..offset + 8].try_into().expect("8 floats");
        accumulator = f32x8::from(lane.map(f32::from)).mul_add(f32x8::from(scale), accumulator);
        offset += 8;
    }
    let mut total = (accumulator + second).reduce_add();
    for index in offset..codes.len() {
        total += f32::from(codes[index]) * weights[index];
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

const PASS_LABELS: [&str; 2] = ["first_connection_pass", "connection_reuse_pass"];

#[derive(Clone, Debug, Serialize)]
struct QueryEvidence {
    pass_label: String,
    query_ordinal: u32,
    latency_ns: u64,
    recall10_ppm: u32,
    recall100_ppm: u32,
    requests: u32,
    bytes: u64,
    returned_feature_row_ids: Vec<i64>,
}

#[derive(Clone, Debug, Serialize)]
struct PassAggregate {
    label: String,
    queries: u32,
    average_recall10_ppm: u32,
    average_recall100_ppm: u32,
    p05_recall100_ppm: u32,
    worst_recall100_ppm: u32,
    latency_p50_ns: u64,
    latency_p95_ns: u64,
    latency_p99_ns: u64,
    requests_total: u64,
    requests_mean_milli: u64,
    requests_p50: u32,
    requests_p95: u32,
    requests_p99: u32,
    requests_max: u32,
    bytes_total: u64,
    bytes_mean: u64,
    bytes_p50: u64,
    bytes_p95: u64,
    bytes_p99: u64,
    bytes_max: u64,
}

#[derive(Clone, Debug, Serialize)]
struct ThroughputCell {
    workers: usize,
    queries: usize,
    errors: usize,
    elapsed_seconds: f64,
    qps: f64,
    latency_p50_ms: f64,
    latency_p99_ms: f64,
}

#[derive(Clone, Debug, Serialize)]
struct BoundedReaderResult {
    schema: String,
    claim_eligible: bool,
    evidence_kind: String,
    storage: String,
    cpu_path: String,
    in_query_cpu: String,
    source_commit: String,
    manifest_sha256: String,
    sq8_sha256: String,
    rows: usize,
    dimensions: usize,
    page_rows: usize,
    shortlist_rows: usize,
    coarse_regions: usize,
    gap_pages: usize,
    concurrency: usize,
    queries_per_pass: usize,
    passes: Vec<PassAggregate>,
    throughput: Vec<ThroughputCell>,
    samples_sha256: String,
    samples_bytes: u64,
}

#[cfg(test)]
impl BoundedReaderResult {
    fn test_fixture(throughput: Vec<ThroughputCell>) -> Self {
        let aggregate = PassAggregate {
            label: "first_connection_pass".to_owned(),
            queries: 1,
            average_recall10_ppm: 1_000_000,
            average_recall100_ppm: 1_000_000,
            p05_recall100_ppm: 1_000_000,
            worst_recall100_ppm: 1_000_000,
            latency_p50_ns: 1,
            latency_p95_ns: 1,
            latency_p99_ns: 1,
            requests_total: 1,
            requests_mean_milli: 1_000,
            requests_p50: 1,
            requests_p95: 1,
            requests_p99: 1,
            requests_max: 1,
            bytes_total: 1,
            bytes_mean: 1,
            bytes_p50: 1,
            bytes_p95: 1,
            bytes_p99: 1,
            bytes_max: 1,
        };
        let mut reused = aggregate.clone();
        reused.label = "connection_reuse_pass".to_owned();
        Self {
            schema: "borsuk-bounded-reader-result-v1".to_owned(),
            claim_eligible: false,
            evidence_kind: "test".to_owned(),
            storage: "test".to_owned(),
            cpu_path: "test".to_owned(),
            in_query_cpu: "rayon".to_owned(),
            source_commit: "1".repeat(40),
            manifest_sha256: "2".repeat(64),
            sq8_sha256: "3".repeat(64),
            rows: 1,
            dimensions: 1,
            page_rows: 1,
            shortlist_rows: 1,
            coarse_regions: 1,
            gap_pages: 0,
            concurrency: 1,
            queries_per_pass: 1,
            passes: vec![aggregate, reused],
            throughput,
            samples_sha256: "4".repeat(64),
            samples_bytes: 1,
        }
    }
}

fn query_evidence(
    pass_label: &str,
    query_ordinal: usize,
    latency_ns: u64,
    requests: usize,
    bytes: usize,
    returned: Vec<i64>,
    truth: &[i64],
) -> BenchResult<QueryEvidence> {
    if !matches!(
        pass_label,
        "first_connection_pass" | "connection_reuse_pass"
    ) || returned.len() != 100
        || truth.len() < 100
        || returned.iter().collect::<HashSet<_>>().len() != returned.len()
        || latency_ns == 0
        || requests == 0
        || bytes == 0
    {
        return Err("bounded query evidence differs".into());
    }
    let hits10 = returned[..10]
        .iter()
        .filter(|identifier| truth[..10].contains(identifier))
        .count();
    let hits100 = returned
        .iter()
        .filter(|identifier| truth[..100].contains(identifier))
        .count();
    Ok(QueryEvidence {
        pass_label: pass_label.to_owned(),
        query_ordinal: u32::try_from(query_ordinal)?,
        latency_ns,
        recall10_ppm: u32::try_from(hits10 * 100_000)?,
        recall100_ppm: u32::try_from(hits100 * 10_000)?,
        requests: u32::try_from(requests)?,
        bytes: u64::try_from(bytes)?,
        returned_feature_row_ids: returned,
    })
}

fn integer_percentile(values: &[u64], quantile_ppm: u64) -> u64 {
    let mut ordered = values.to_vec();
    ordered.sort_unstable();
    let numerator = (ordered.len() as u64 - 1) * quantile_ppm;
    let index = (numerator + 500_000) / 1_000_000;
    ordered[index as usize]
}

fn aggregate_pass(label: &str, samples: &[QueryEvidence]) -> BenchResult<PassAggregate> {
    let selected = samples
        .iter()
        .filter(|sample| sample.pass_label == label)
        .collect::<Vec<_>>();
    if selected.is_empty() || selected.len() != samples.len() {
        return Err("bounded pass evidence differs".into());
    }
    let count = u64::try_from(selected.len())?;
    let recall10 = selected
        .iter()
        .map(|sample| u64::from(sample.recall10_ppm))
        .collect::<Vec<_>>();
    let recall100 = selected
        .iter()
        .map(|sample| u64::from(sample.recall100_ppm))
        .collect::<Vec<_>>();
    let latencies = selected
        .iter()
        .map(|sample| sample.latency_ns)
        .collect::<Vec<_>>();
    let requests = selected
        .iter()
        .map(|sample| u64::from(sample.requests))
        .collect::<Vec<_>>();
    let bytes = selected
        .iter()
        .map(|sample| sample.bytes)
        .collect::<Vec<_>>();
    let requests_total = requests.iter().sum::<u64>();
    let bytes_total = bytes.iter().sum::<u64>();
    Ok(PassAggregate {
        label: label.to_owned(),
        queries: u32::try_from(count)?,
        average_recall10_ppm: u32::try_from(recall10.iter().sum::<u64>() / count)?,
        average_recall100_ppm: u32::try_from(recall100.iter().sum::<u64>() / count)?,
        p05_recall100_ppm: u32::try_from(integer_percentile(&recall100, 50_000))?,
        worst_recall100_ppm: u32::try_from(*recall100.iter().min().expect("nonempty"))?,
        latency_p50_ns: integer_percentile(&latencies, 500_000),
        latency_p95_ns: integer_percentile(&latencies, 950_000),
        latency_p99_ns: integer_percentile(&latencies, 990_000),
        requests_total,
        requests_mean_milli: requests_total * 1_000 / count,
        requests_p50: u32::try_from(integer_percentile(&requests, 500_000))?,
        requests_p95: u32::try_from(integer_percentile(&requests, 950_000))?,
        requests_p99: u32::try_from(integer_percentile(&requests, 990_000))?,
        requests_max: u32::try_from(*requests.iter().max().expect("nonempty"))?,
        bytes_total,
        bytes_mean: bytes_total / count,
        bytes_p50: integer_percentile(&bytes, 500_000),
        bytes_p95: integer_percentile(&bytes, 950_000),
        bytes_p99: integer_percentile(&bytes, 990_000),
        bytes_max: *bytes.iter().max().expect("nonempty"),
    })
}

fn sha256_file(path: &Path) -> BenchResult<String> {
    let mut hasher = Sha256::new();
    let mut file = File::open(path)?;
    std::io::copy(&mut file, &mut hasher)?;
    Ok(format!("{:x}", hasher.finalize()))
}

fn write_samples(path: &Path, samples: &[QueryEvidence]) -> BenchResult<()> {
    if samples.is_empty()
        || samples
            .iter()
            .any(|sample| sample.returned_feature_row_ids.len() != 100)
    {
        return Err("bounded query samples differ".into());
    }
    let returned_values = samples
        .iter()
        .flat_map(|sample| sample.returned_feature_row_ids.iter().copied())
        .collect::<Vec<_>>();
    let item = Arc::new(Field::new("item", DataType::Int64, false));
    let returned = FixedSizeListArray::try_new(
        item.clone(),
        100,
        Arc::new(Int64Array::from(returned_values)),
        None,
    )?;
    let schema = Arc::new(Schema::new(vec![
        Field::new("pass_label", DataType::Utf8, false),
        Field::new("query_ordinal", DataType::UInt32, false),
        Field::new("latency_ns", DataType::UInt64, false),
        Field::new("recall10_ppm", DataType::UInt32, false),
        Field::new("recall100_ppm", DataType::UInt32, false),
        Field::new("requests", DataType::UInt32, false),
        Field::new("bytes", DataType::UInt64, false),
        Field::new(
            "returned_feature_row_ids",
            DataType::FixedSizeList(item, 100),
            false,
        ),
    ]));
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(
            samples
                .iter()
                .map(|sample| sample.pass_label.as_str())
                .collect::<Vec<_>>(),
        )),
        Arc::new(UInt32Array::from(
            samples
                .iter()
                .map(|sample| sample.query_ordinal)
                .collect::<Vec<_>>(),
        )),
        Arc::new(UInt64Array::from(
            samples
                .iter()
                .map(|sample| sample.latency_ns)
                .collect::<Vec<_>>(),
        )),
        Arc::new(UInt32Array::from(
            samples
                .iter()
                .map(|sample| sample.recall10_ppm)
                .collect::<Vec<_>>(),
        )),
        Arc::new(UInt32Array::from(
            samples
                .iter()
                .map(|sample| sample.recall100_ppm)
                .collect::<Vec<_>>(),
        )),
        Arc::new(UInt32Array::from(
            samples
                .iter()
                .map(|sample| sample.requests)
                .collect::<Vec<_>>(),
        )),
        Arc::new(UInt64Array::from(
            samples
                .iter()
                .map(|sample| sample.bytes)
                .collect::<Vec<_>>(),
        )),
        Arc::new(returned),
    ];
    let batch = RecordBatch::try_new(schema.clone(), arrays)?;
    let mut writer = ArrowWriter::try_new(File::create(path)?, schema, None)?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(())
}

fn select_top_neighbors(mut scored: Vec<(f32, i64)>, take: usize) -> Vec<i64> {
    let take = take.min(scored.len());
    if take == 0 {
        return Vec::new();
    }
    scored.select_nth_unstable_by(take - 1, |left, right| {
        left.0.total_cmp(&right.0).then(left.1.cmp(&right.1))
    });
    scored.truncate(take);
    scored.sort_unstable_by(|left, right| left.0.total_cmp(&right.0).then(left.1.cmp(&right.1)));
    scored
        .into_iter()
        .map(|(_, identifier)| identifier)
        .collect()
}

fn canonical_result_bytes(result: &BoundedReaderResult) -> BenchResult<Vec<u8>> {
    if result.schema != "borsuk-bounded-reader-result-v1"
        || result.claim_eligible
        || result.source_commit.len() != 40
        || result.manifest_sha256.len() != 64
        || result.sq8_sha256.len() != 64
        || result.samples_sha256.len() != 64
        || result.samples_bytes == 0
        || result.passes.len() != 2
        || result.throughput.iter().any(|cell| {
            !cell.elapsed_seconds.is_finite()
                || !cell.qps.is_finite()
                || !cell.latency_p50_ms.is_finite()
                || !cell.latency_p99_ms.is_finite()
                || cell.elapsed_seconds <= 0.0
                || cell.qps < 0.0
        })
    {
        return Err("bounded reader result differs".into());
    }
    let mut body = serde_json::to_vec(result)?;
    body.push(b'\n');
    Ok(body)
}

#[derive(Clone, Copy)]
struct SearchParameters {
    budget: usize,
    regions: usize,
    gap: usize,
    concurrency: usize,
    in_query_cpu: InQueryCpu,
}

async fn search(
    store: &Arc<dyn ObjectStore>,
    key: &ObjectPath,
    manifest: &Manifest,
    query: &[f32],
    parameters: SearchParameters,
) -> BenchResult<QueryOutcome> {
    let row_bytes = 8 + 4 + manifest.dimensions;
    let started = Instant::now();
    let chosen = route(
        manifest,
        query,
        parameters.budget,
        parameters.regions,
        parameters.in_query_cpu,
    );
    let ranges = coalesce(&chosen, parameters.gap);
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
    .buffer_unordered(parameters.concurrency)
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

    let score_blob = |(_, body): &(usize, bytes::Bytes)| {
        let count = body.len() / row_bytes;
        (0..count)
            .map(|row| {
                let base = row * row_bytes;
                let identifier =
                    i64::from_le_bytes(body[base..base + 8].try_into().expect("8 bytes"));
                let norm =
                    f32::from_le_bytes(body[base + 8..base + 12].try_into().expect("4 bytes"));
                let codes = &body[base + 12..base + row_bytes];
                (
                    norm - 2.0 * (fused_inner(codes, &weights) + shift),
                    identifier,
                )
            })
            .collect::<Vec<_>>()
    };
    let best: Vec<(f32, i64)> = match parameters.in_query_cpu {
        InQueryCpu::Rayon => blobs.par_iter().flat_map_iter(score_blob).collect(),
        InQueryCpu::Sequential => blobs.iter().flat_map(score_blob).collect(),
    };
    let returned = select_top_neighbors(best, manifest.neighbors);
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

async fn run_spawned_bounded<F, T>(
    jobs: Vec<F>,
    limit: usize,
) -> Vec<Result<T, tokio::task::JoinError>>
where
    F: Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    stream::iter(jobs.into_iter().map(tokio::spawn))
        .buffer_unordered(limit)
        .collect()
        .await
}

fn successful_qps(successful_queries: usize, elapsed_seconds: f64) -> f64 {
    successful_queries as f64 / elapsed_seconds
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

fn in_query_cpu() -> BenchResult<InQueryCpu> {
    match env::var("BORSUK_V71_IN_QUERY_CPU") {
        Err(_) => Ok(InQueryCpu::Rayon),
        Ok(value) if value == "rayon" => Ok(InQueryCpu::Rayon),
        Ok(value) if value == "sequential" => Ok(InQueryCpu::Sequential),
        Ok(value) => Err(format!("invalid BORSUK_V71_IN_QUERY_CPU {value}").into()),
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
    if coalesce(&[7], 0) != std::iter::once(7..8).collect::<Vec<_>>() {
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
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1);
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
    let samples_path = PathBuf::from(required("BORSUK_V71_SAMPLES")?);
    let source_commit = required("BORSUK_SOURCE_COMMIT")?;
    let expected_manifest_sha256 = required("BORSUK_MANIFEST_SHA256")?;
    let sq8_sha256 = required("BORSUK_SQ8_SHA256")?;
    let budget = optional_usize("BORSUK_V71_SHORTLIST", 512)?;
    let gap = optional_usize("BORSUK_V71_GAP", 8)?;
    let concurrency = optional_usize("BORSUK_V71_CONCURRENCY", 64)?;
    let regions = optional_usize("BORSUK_V71_REGIONS", 1024)?;
    let measured = optional_usize("BORSUK_V71_QUERIES", 200)?;
    let in_query_cpu = in_query_cpu()?;
    let search_parameters = SearchParameters {
        budget,
        regions,
        gap,
        concurrency,
        in_query_cpu,
    };

    let manifest_sha256 = sha256_file(&manifest_path)?;
    if manifest_sha256 != expected_manifest_sha256 {
        return Err("manifest SHA-256 differs".into());
    }
    let manifest = Arc::new(load_manifest(&manifest_path)?);
    if manifest.neighbors != 100 {
        return Err("bounded reader requires GT100".into());
    }
    let url = Url::parse(&uri)?;
    let (store, key) = parse_url_opts(&url, [("region".to_string(), region.clone())])?;
    let store: Arc<dyn ObjectStore> = Arc::from(store);

    let measured = measured.min(manifest.queries);
    let mut samples = Vec::with_capacity(measured * PASS_LABELS.len());
    let mut passes = Vec::with_capacity(PASS_LABELS.len());
    for pass_label in PASS_LABELS {
        let mut pass_samples = Vec::with_capacity(measured);
        for index in 0..measured {
            let offset = index * manifest.dimensions;
            let query = &manifest.query_vectors[offset..offset + manifest.dimensions];
            let started = Instant::now();
            let outcome = search(&store, &key, &manifest, query, search_parameters).await?;
            let latency_ns = u64::try_from(started.elapsed().as_nanos())?;
            let truth_offset = index * manifest.neighbors;
            let truth = &manifest.truth[truth_offset..truth_offset + manifest.neighbors];
            pass_samples.push(query_evidence(
                pass_label,
                index,
                latency_ns,
                outcome.requests,
                outcome.bytes,
                outcome.returned,
                truth,
            )?);
        }
        passes.push(aggregate_pass(pass_label, &pass_samples)?);
        samples.extend(pass_samples);
    }
    write_samples(&samples_path, &samples)?;
    let samples_sha256 = sha256_file(&samples_path)?;
    let samples_bytes = fs::metadata(&samples_path)?.len();

    // Throughput pass: the per-index ceiling has been asserted from S3's
    // documented per-prefix request rate but never measured. Driving many
    // queries concurrently against the same object settles it.
    let mut throughput = Vec::new();
    if optional_usize("BORSUK_V71_THROUGHPUT", 0)? == 1 {
        for workers in [8usize, 32, 128, 384] {
            let started = Instant::now();
            let mut errors = 0usize;
            let jobs = (0..workers * 8)
                .map(|slot| {
                    let store = Arc::clone(&store);
                    let key = key.clone();
                    let manifest = Arc::clone(&manifest);
                    async move {
                        let index = slot % manifest.queries;
                        let offset = index * manifest.dimensions;
                        let query =
                            manifest.query_vectors[offset..offset + manifest.dimensions].to_vec();
                        search(&store, &key, &manifest, &query, search_parameters).await
                    }
                })
                .collect::<Vec<_>>();
            let outcomes = run_spawned_bounded(jobs, workers).await;
            let elapsed = started.elapsed().as_secs_f64();
            let mut latencies = Vec::new();
            for outcome in outcomes {
                match outcome {
                    Ok(Ok(value)) => latencies.push(value.route_ms + value.io_ms + value.scan_ms),
                    Ok(Err(_)) | Err(_) => errors += 1,
                }
            }
            throughput.push(ThroughputCell {
                workers,
                queries: workers * 8,
                errors,
                elapsed_seconds: elapsed,
                qps: successful_qps(latencies.len(), elapsed),
                latency_p50_ms: if latencies.is_empty() {
                    0.0
                } else {
                    percentile(&latencies, 0.50)
                },
                latency_p99_ms: if latencies.is_empty() {
                    0.0
                } else {
                    percentile(&latencies, 0.99)
                },
            });
            println!(
                "{}",
                serde_json::to_string(throughput.last().expect("just pushed"))?
            );
        }
    }
    let report = BoundedReaderResult {
        schema: "borsuk-bounded-reader-result-v1".to_owned(),
        claim_eligible: false,
        evidence_kind: "measured-native-bounded-sq8-reader".to_owned(),
        storage: "real-object-store-ranged-gets-no-local-cache".to_owned(),
        cpu_path: "safe-rust-simd-scan-and-adc-router".to_owned(),
        in_query_cpu: in_query_cpu.label().to_owned(),
        source_commit,
        manifest_sha256,
        sq8_sha256,
        rows: manifest.rows,
        dimensions: manifest.dimensions,
        page_rows: manifest.page_rows,
        shortlist_rows: budget,
        coarse_regions: regions,
        gap_pages: gap,
        concurrency,
        queries_per_pass: measured,
        passes,
        throughput,
        samples_sha256,
        samples_bytes,
    };
    let report_bytes = canonical_result_bytes(&report)?;
    fs::write(&output, &report_bytes)?;
    print!("{}", String::from_utf8(report_bytes)?);
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{
        fs::File,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use parquet::file::reader::FileReader;

    use super::{
        BoundedReaderResult, InQueryCpu, Manifest, PASS_LABELS, ThroughputCell, aggregate_pass,
        canonical_result_bytes, query_evidence, route, run_spawned_bounded, select_top_neighbors,
        successful_qps, write_samples,
    };

    fn routing_fixture() -> Manifest {
        let dimensions = 4;
        let subspaces = 2;
        let width = 2;
        let mut codebooks = vec![0.0; subspaces * 256 * width];
        for (index, value) in codebooks.iter_mut().enumerate() {
            *value = ((index * 17 % 101) as f32 - 50.0) / 31.0;
        }
        Manifest {
            rows: 8,
            dimensions,
            page_rows: 2,
            pages: 4,
            queries: 0,
            neighbors: 2,
            subspaces,
            width,
            blocks_per_page: 1,
            summaries: vec![
                0.1, 0.2, 0.3, 0.4, 1.0, 0.5, 0.2, 0.1, -0.5, 0.7, 0.9, -0.2, 0.6, -0.8, 0.4, 0.3,
            ],
            low: vec![-1.0; dimensions],
            span_step: vec![2.0 / 255.0; dimensions],
            codebooks,
            row_codes: vec![3, 7, 11, 19, 23, 29, 31, 37, 41, 43, 47, 53, 59, 61, 67, 71],
            query_vectors: Vec::new(),
            truth: Vec::new(),
        }
    }

    fn returned_with_hits(hits: usize, top_ten_hits: usize) -> Vec<i64> {
        let mut returned = (0..top_ten_hits as i64).collect::<Vec<_>>();
        returned.extend((0..10 - top_ten_hits).map(|index| 10_000 + index as i64));
        returned.extend(10..10 + (hits - top_ten_hits) as i64);
        while returned.len() < 100 {
            returned.push(20_000 + returned.len() as i64);
        }
        returned
    }

    #[test]
    fn bounded_evidence_aggregates_recall_distribution_latency_and_io() {
        let truth = (0..100).collect::<Vec<i64>>();
        let samples = vec![
            query_evidence(
                "first_connection_pass",
                0,
                100,
                2,
                1_000,
                returned_with_hits(90, 10),
                &truth,
            )
            .unwrap(),
            query_evidence(
                "first_connection_pass",
                1,
                300,
                4,
                3_000,
                returned_with_hits(80, 8),
                &truth,
            )
            .unwrap(),
        ];

        let aggregate = aggregate_pass("first_connection_pass", &samples).unwrap();

        assert_eq!(aggregate.queries, 2);
        assert_eq!(aggregate.average_recall10_ppm, 900_000);
        assert_eq!(aggregate.average_recall100_ppm, 850_000);
        assert_eq!(aggregate.p05_recall100_ppm, 800_000);
        assert_eq!(aggregate.worst_recall100_ppm, 800_000);
        assert_eq!(aggregate.latency_p50_ns, 300);
        assert_eq!(aggregate.latency_p95_ns, 300);
        assert_eq!(aggregate.latency_p99_ns, 300);
        assert_eq!(aggregate.requests_total, 6);
        assert_eq!(aggregate.requests_p50, 4);
        assert_eq!(aggregate.bytes_total, 4_000);
        assert_eq!(aggregate.bytes_p50, 3_000);
    }

    #[test]
    fn bounded_evidence_serializer_rejects_nonfinite_throughput() {
        let result = BoundedReaderResult::test_fixture(vec![ThroughputCell {
            workers: 8,
            queries: 64,
            errors: 0,
            elapsed_seconds: 1.0,
            qps: f64::NAN,
            latency_p50_ms: 1.0,
            latency_p99_ms: 2.0,
        }]);

        assert!(canonical_result_bytes(&result).is_err());
    }

    #[test]
    fn bounded_evidence_writes_ranked_ids_to_parquet_for_two_fixed_passes() {
        assert_eq!(
            PASS_LABELS,
            ["first_connection_pass", "connection_reuse_pass"]
        );
        let truth = (0..100).collect::<Vec<i64>>();
        let samples = PASS_LABELS
            .iter()
            .enumerate()
            .map(|(ordinal, label)| {
                query_evidence(
                    label,
                    ordinal,
                    100,
                    2,
                    1_000,
                    returned_with_hits(90, 10),
                    &truth,
                )
                .unwrap()
            })
            .collect::<Vec<_>>();
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("samples.parquet");

        write_samples(&path, &samples).unwrap();

        let metadata =
            parquet::file::reader::SerializedFileReader::new(File::open(path).unwrap()).unwrap();
        assert_eq!(metadata.metadata().file_metadata().num_rows(), 2);
        assert_eq!(
            metadata.metadata().file_metadata().schema_descr().columns()[7]
                .path()
                .string(),
            "returned_feature_row_ids.list.item"
        );
    }

    #[test]
    fn bounded_evidence_top_ten_is_exactly_ranked_with_stable_ties() {
        let selected = select_top_neighbors(vec![(3.0, 30), (1.0, 20), (1.0, 10)], 2);
        assert_eq!(selected, vec![10, 20]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn throughput_jobs_execute_independently_under_the_admission_limit() {
        let active = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let jobs = (0..2)
            .map(|_| {
                let active = Arc::clone(&active);
                let peak = Arc::clone(&peak);
                async move {
                    let now = active.fetch_add(1, Ordering::SeqCst) + 1;
                    peak.fetch_max(now, Ordering::SeqCst);
                    // Models the synchronous routing/scoring work inside one
                    // async query future. Independent query tasks must overlap
                    // this work instead of serialising it on the parent task.
                    std::thread::sleep(Duration::from_millis(50));
                    active.fetch_sub(1, Ordering::SeqCst);
                    now
                }
            })
            .collect::<Vec<_>>();

        let outcomes = run_spawned_bounded(jobs, 2).await;
        assert!(outcomes.iter().all(Result::is_ok));
        assert_eq!(peak.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn throughput_qps_counts_only_successful_queries() {
        assert_eq!(successful_qps(7, 2.0), 3.5);
    }

    #[test]
    fn query_level_and_in_query_parallel_routing_choose_identical_pages() {
        let manifest = routing_fixture();
        let query = [0.25, -0.4, 0.75, 0.1];
        let parallel = route(&manifest, &query, 4, 3, InQueryCpu::Rayon);
        let query_level = route(&manifest, &query, 4, 3, InQueryCpu::Sequential);
        assert_eq!(query_level, parallel);
    }
}
