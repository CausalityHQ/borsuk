#![allow(missing_docs)]

use std::{
    env,
    error::Error,
    fs::{self, File},
    io::{self, BufReader, BufWriter, Read, Write},
    path::{Path, PathBuf},
    sync::Arc,
    thread,
    time::Instant,
};

use borsuk::{
    BorsukIndex, CompactionOptions, IndexConfig, LeafMode, SearchOptions, SearchReport,
    VectorMetric, VectorRecord, recall_at_k,
};
use serde::Deserialize;

const DEFAULT_QUERIES: usize = 1_000;
const DEFAULT_SEGMENT_MAX: usize = 4_096;
const DEFAULT_CONCURRENCY: &str = "1,2,4,8,16";
const INGEST_BATCH_SIZE: usize = 4_096;
const WRITE_BATCH_SIZE: usize = 1_024;
const ROUTING_OVERFETCH_SWEEP: &[usize] = &[1, 2, 4, 8, 16, 32, 64];
const RECALL_K: usize = 10;
const HIGH_RECALL_ROUTING_OVERFETCH: usize = 64;
const WRITE_FRACTION_DENOMINATOR: usize = 20;
// AWS S3 Standard GET/HEAD pricing: $0.40 per one million requests.
const PRICE_PER_REQUEST: f64 = 0.40 / 1_000_000.0;

type BenchResult<T> = Result<T, Box<dyn Error>>;

#[derive(Debug, Deserialize)]
struct DatasetMeta {
    name: String,
    metric: String,
    dim: usize,
    n_train: usize,
    n_test: usize,
    k: usize,
}

struct ResolvedConfig {
    dataset_dir: PathBuf,
    uri: String,
    cache_dir: PathBuf,
    limit: usize,
    queries: usize,
    output_dir: PathBuf,
    concurrency: Vec<usize>,
    segment_max: usize,
    _uri_temp: Option<tempfile::TempDir>,
    _cache_temp: Option<tempfile::TempDir>,
}

struct Dataset {
    meta: DatasetMeta,
    metric: VectorMetric,
    train_count: usize,
    queries: Arc<Vec<Vec<f32>>>,
    ground_truth: Vec<Vec<String>>,
}

#[derive(Default)]
struct QuerySummary {
    latencies_ms: Vec<f64>,
    recall_sum: f64,
    bytes_read: u128,
    billable_requests: u128,
}

impl QuerySummary {
    fn push(&mut self, elapsed_ms: f64, report: &SearchReport, recall: Option<f32>) {
        self.latencies_ms.push(elapsed_ms);
        self.recall_sum += recall.map_or(0.0, f64::from);
        self.bytes_read += u128::from(report.bytes_read);
        self.billable_requests +=
            u128::from(report.requests.gets.saturating_add(report.requests.heads));
    }

    fn count(&self) -> usize {
        self.latencies_ms.len()
    }

    fn recall(&self) -> f64 {
        mean(self.recall_sum, self.count())
    }

    fn average_bytes(&self) -> f64 {
        mean(self.bytes_read as f64, self.count())
    }

    fn average_requests(&self) -> f64 {
        mean(self.billable_requests as f64, self.count())
    }

    fn dollars_per_million_queries(&self) -> f64 {
        dollars_per_million_queries(self.average_requests())
    }
}

struct WriteRow {
    op: &'static str,
    ops: usize,
    wall_ms: f64,
    latencies_ms: Vec<f64>,
    bytes_read: u64,
    bytes_written: u64,
}

fn main() {
    if let Err(error) = run() {
        eprintln!("production_bench: {error}");
        std::process::exit(1);
    }
}

fn run() -> BenchResult<()> {
    let config = resolve_config()?;
    print_config(&config);
    let dataset = load_dataset(&config)?;
    fs::create_dir_all(&config.output_dir)?;

    let mut index = BorsukIndex::create(IndexConfig {
        uri: config.uri.clone(),
        metric: dataset.metric.clone(),
        dimensions: dataset.meta.dim,
        segment_max_vectors: config.segment_max,
        ram_budget_bytes: None,
        text: false,
        named_vectors: Default::default(),
    })?;

    let ingest_started = Instant::now();
    ingest_train(&mut index, &config.dataset_dir, &dataset)?;
    let ingest_ms = elapsed_ms(ingest_started);

    let compaction_started = Instant::now();
    let build_compaction = index.compact(CompactionOptions::default())?;
    let compaction_ms = elapsed_ms(compaction_started);
    eprintln!(
        "build dataset={} records={} ingest_ms={ingest_ms:.3} compaction_ms={compaction_ms:.3} compaction_bytes_read={} compaction_bytes_written={}",
        dataset.meta.name,
        dataset.train_count,
        build_compaction.bytes_read,
        build_compaction.bytes_written
    );
    drop(index);

    let reader = Arc::new(BorsukIndex::open_with_cache(
        &config.uri,
        Some(config.cache_dir.clone()),
    )?);
    warm_all_segments(&reader, &dataset.queries)?;
    write_recall_latency_csv(&config, &dataset, &reader)?;
    drop(reader);

    reset_cache(&config.cache_dir)?;
    let reader = Arc::new(BorsukIndex::open_with_cache(
        &config.uri,
        Some(config.cache_dir.clone()),
    )?);
    write_cold_warm_csv(&config, &dataset, &reader)?;
    write_concurrency_csv(&config, &dataset, &reader)?;

    // BorsukIndex is cloneable but has no storage-level "copy index" API. All read
    // measurements are complete, so this cloned handle is the isolated mutable
    // benchmark copy; it shares the configured backing URI with the built index.
    let mut write_index = reader.as_ref().clone();
    drop(reader);
    write_write_costs_csv(&config, &dataset, &mut write_index)?;
    Ok(())
}

fn resolve_config() -> BenchResult<ResolvedConfig> {
    let dataset_dir = env::var_os("BORSUK_BENCH_DATASET")
        .map(PathBuf::from)
        .ok_or_else(|| missing_dataset_error(None))?;
    if !dataset_dir.is_dir() {
        return Err(missing_dataset_error(Some(&dataset_dir)).into());
    }

    let (uri, uri_temp) = match non_empty_env("BORSUK_BENCH_URI") {
        Some(uri) => (uri, None),
        None => {
            let temp = tempfile::tempdir()?;
            (temp.path().to_string_lossy().into_owned(), Some(temp))
        }
    };
    let (cache_dir, cache_temp) = match env::var_os("BORSUK_BENCH_CACHE") {
        Some(path) if !path.is_empty() => (PathBuf::from(path), None),
        _ => {
            let temp = tempfile::tempdir()?;
            (temp.path().to_path_buf(), Some(temp))
        }
    };

    let limit = env_usize("BORSUK_BENCH_LIMIT", 0)?;
    let queries = env_usize("BORSUK_BENCH_QUERIES", DEFAULT_QUERIES)?;
    if queries == 0 {
        return Err(invalid_input("BORSUK_BENCH_QUERIES must be greater than zero").into());
    }
    let output_dir = env::var_os("BORSUK_BENCH_OUTPUT_DIR")
        .filter(|value| !value.is_empty())
        .map_or_else(env::current_dir, |value| Ok(PathBuf::from(value)))?;
    let concurrency = parse_concurrency(
        &env::var("BORSUK_BENCH_CONCURRENCY").unwrap_or_else(|_| DEFAULT_CONCURRENCY.to_string()),
    )?;
    let segment_max = env_usize("BORSUK_BENCH_SEGMENT_MAX", DEFAULT_SEGMENT_MAX)?;
    if segment_max == 0 {
        return Err(invalid_input("BORSUK_BENCH_SEGMENT_MAX must be greater than zero").into());
    }

    Ok(ResolvedConfig {
        dataset_dir,
        uri,
        cache_dir,
        limit,
        queries,
        output_dir,
        concurrency,
        segment_max,
        _uri_temp: uri_temp,
        _cache_temp: cache_temp,
    })
}

fn print_config(config: &ResolvedConfig) {
    let concurrency = config
        .concurrency
        .iter()
        .map(usize::to_string)
        .collect::<Vec<_>>()
        .join(",");
    eprintln!(
        "config dataset={} uri={} cache={} limit={} queries={} output_dir={} concurrency={} segment_max={}",
        config.dataset_dir.display(),
        config.uri,
        config.cache_dir.display(),
        config.limit,
        config.queries,
        config.output_dir.display(),
        concurrency,
        config.segment_max
    );
}

fn load_dataset(config: &ResolvedConfig) -> BenchResult<Dataset> {
    let meta_path = config.dataset_dir.join("meta.json");
    let meta: DatasetMeta = serde_json::from_reader(BufReader::new(File::open(&meta_path)?))?;
    if meta.dim == 0 || meta.n_train == 0 || meta.n_test == 0 {
        return Err(invalid_input("meta.json dimensions and row counts must be non-zero").into());
    }
    if meta.k < RECALL_K {
        return Err(invalid_input(&format!(
            "meta.json k must be at least {RECALL_K}, got {}",
            meta.k
        ))
        .into());
    }
    let metric = match meta.metric.as_str() {
        "cosine" => VectorMetric::Cosine,
        "euclidean" => VectorMetric::Euclidean,
        other => {
            return Err(invalid_input(&format!(
                "unsupported meta.json metric `{other}`; expected cosine or euclidean"
            ))
            .into());
        }
    };

    let train_path = config.dataset_dir.join("train.f32");
    let test_path = config.dataset_dir.join("test.f32");
    let neighbors_path = config.dataset_dir.join("neighbors.i32");
    validate_file_size(&train_path, meta.n_train, meta.dim, 4)?;
    validate_file_size(&test_path, meta.n_test, meta.dim, 4)?;
    validate_file_size(&neighbors_path, meta.n_test, meta.k, 4)?;

    let train_count = if config.limit == 0 {
        meta.n_train
    } else {
        config.limit.min(meta.n_train)
    };
    let query_count = config.queries.min(meta.n_test);
    let queries = Arc::new(read_f32_rows(&test_path, query_count, meta.dim)?);
    let ground_truth = read_ground_truth(&neighbors_path, query_count, meta.k, meta.n_train)?;

    Ok(Dataset {
        meta,
        metric,
        train_count,
        queries,
        ground_truth,
    })
}

fn validate_file_size(
    path: &Path,
    rows: usize,
    columns: usize,
    element_bytes: u64,
) -> BenchResult<()> {
    let expected = u64::try_from(rows)?
        .checked_mul(u64::try_from(columns)?)
        .and_then(|count| count.checked_mul(element_bytes))
        .ok_or_else(|| invalid_input(&format!("size overflow for {}", path.display())))?;
    let actual = fs::metadata(path)?.len();
    if actual != expected {
        return Err(invalid_input(&format!(
            "{} has {actual} bytes; expected {expected} from meta.json",
            path.display()
        ))
        .into());
    }
    Ok(())
}

fn read_f32_rows(path: &Path, rows: usize, dimensions: usize) -> BenchResult<Vec<Vec<f32>>> {
    let mut reader = BufReader::new(File::open(path)?);
    let mut result = Vec::with_capacity(rows);
    for _ in 0..rows {
        result.push(read_f32_vector(&mut reader, dimensions)?);
    }
    Ok(result)
}

fn read_f32_vector(reader: &mut impl Read, dimensions: usize) -> io::Result<Vec<f32>> {
    let mut vector = Vec::with_capacity(dimensions);
    let mut bytes = [0_u8; 4];
    for _ in 0..dimensions {
        reader.read_exact(&mut bytes)?;
        vector.push(f32::from_le_bytes(bytes));
    }
    Ok(vector)
}

fn read_ground_truth(
    path: &Path,
    rows: usize,
    neighbors_per_row: usize,
    n_train: usize,
) -> BenchResult<Vec<Vec<String>>> {
    let mut reader = BufReader::new(File::open(path)?);
    let mut rows_out = Vec::with_capacity(rows);
    let mut bytes = [0_u8; 4];
    for row in 0..rows {
        let mut neighbors = Vec::with_capacity(RECALL_K);
        for column in 0..neighbors_per_row {
            reader.read_exact(&mut bytes)?;
            let id = i32::from_le_bytes(bytes);
            if id < 0 || usize::try_from(id)? >= n_train {
                return Err(invalid_input(&format!(
                    "neighbors.i32 row {row} contains out-of-range id {id}"
                ))
                .into());
            }
            if column < RECALL_K {
                neighbors.push(id.to_string());
            }
        }
        rows_out.push(neighbors);
    }
    Ok(rows_out)
}

fn ingest_train(index: &mut BorsukIndex, dataset_dir: &Path, dataset: &Dataset) -> BenchResult<()> {
    let mut reader = BufReader::new(File::open(dataset_dir.join("train.f32"))?);
    let mut start = 0_usize;
    while start < dataset.train_count {
        let end = start
            .saturating_add(INGEST_BATCH_SIZE)
            .min(dataset.train_count);
        let mut records = Vec::with_capacity(end - start);
        for id in start..end {
            records.push(VectorRecord::new(
                id.to_string(),
                read_f32_vector(&mut reader, dataset.meta.dim)?,
            ));
        }
        index.add(records)?;
        start = end;
    }
    Ok(())
}

fn warm_all_segments(index: &BorsukIndex, queries: &[Vec<f32>]) -> BenchResult<()> {
    for query in queries {
        index.search_with_report(query, SearchOptions::exact(RECALL_K))?;
    }
    Ok(())
}

fn write_recall_latency_csv(
    config: &ResolvedConfig,
    dataset: &Dataset,
    index: &BorsukIndex,
) -> BenchResult<()> {
    let path = config.output_dir.join("bench_recall_latency.csv");
    let mut writer = csv_writer(&path)?;
    writeln!(
        writer,
        "mode,routing_page_overfetch,max_candidates,recall_at_10,p50_ms,p95_ms,p99_ms,avg_bytes_read,avg_gets_per_query,dollars_per_million_queries"
    )?;

    for &routing_page_overfetch in ROUTING_OVERFETCH_SWEEP {
        let options = approximate_options(routing_page_overfetch, config.segment_max);
        let summary = run_queries(
            index,
            &dataset.queries,
            Some(&dataset.ground_truth),
            options,
        )?;
        write_recall_row(
            &mut writer,
            "hybrid",
            routing_page_overfetch,
            config.segment_max,
            &summary,
        )?;
    }

    let exact = run_queries(
        index,
        &dataset.queries,
        Some(&dataset.ground_truth),
        SearchOptions::exact(RECALL_K),
    )?;
    write_recall_row(&mut writer, "exact", 0, 0, &exact)?;
    writer.flush()?;
    eprintln!(
        "wrote {} rows={} dataset={}",
        path.display(),
        ROUTING_OVERFETCH_SWEEP.len() + 1,
        dataset.meta.name
    );
    Ok(())
}

fn write_recall_row(
    writer: &mut impl Write,
    mode: &str,
    routing_page_overfetch: usize,
    max_candidates: usize,
    summary: &QuerySummary,
) -> io::Result<()> {
    writeln!(
        writer,
        "{mode},{routing_page_overfetch},{max_candidates},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3}",
        summary.recall(),
        percentile(&summary.latencies_ms, 0.50),
        percentile(&summary.latencies_ms, 0.95),
        percentile(&summary.latencies_ms, 0.99),
        summary.average_bytes(),
        summary.average_requests(),
        summary.dollars_per_million_queries()
    )
}

fn write_cold_warm_csv(
    config: &ResolvedConfig,
    dataset: &Dataset,
    index: &BorsukIndex,
) -> BenchResult<()> {
    let path = config.output_dir.join("bench_cold_warm.csv");
    let mut writer = csv_writer(&path)?;
    writeln!(
        writer,
        "phase,p50_ms,p95_ms,p99_ms,avg_bytes_read,avg_gets_per_query,dollars_per_million_queries"
    )?;
    for phase in ["cold", "warm"] {
        let summary = run_queries(
            index,
            &dataset.queries,
            None,
            approximate_options(HIGH_RECALL_ROUTING_OVERFETCH, config.segment_max),
        )?;
        writeln!(
            writer,
            "{phase},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3}",
            percentile(&summary.latencies_ms, 0.50),
            percentile(&summary.latencies_ms, 0.95),
            percentile(&summary.latencies_ms, 0.99),
            summary.average_bytes(),
            summary.average_requests(),
            summary.dollars_per_million_queries()
        )?;
    }
    writer.flush()?;
    eprintln!("wrote {} rows=2", path.display());
    Ok(())
}

fn write_concurrency_csv(
    config: &ResolvedConfig,
    dataset: &Dataset,
    index: &Arc<BorsukIndex>,
) -> BenchResult<()> {
    let path = config.output_dir.join("bench_concurrency.csv");
    let mut writer = csv_writer(&path)?;
    writeln!(
        writer,
        "workers,total_queries,qps,p50_ms,p95_ms,p99_ms,avg_bytes_read"
    )?;
    for &workers in &config.concurrency {
        let started = Instant::now();
        let mut handles = Vec::with_capacity(workers);
        for worker in 0..workers {
            let worker_index = Arc::clone(index);
            let queries = Arc::clone(&dataset.queries);
            let options = approximate_options(HIGH_RECALL_ROUTING_OVERFETCH, config.segment_max);
            handles.push(thread::spawn(move || -> Result<Vec<(f64, u64)>, String> {
                let mut measurements = Vec::new();
                for query_index in (worker..queries.len()).step_by(workers) {
                    let query_started = Instant::now();
                    let report = worker_index
                        .search_with_report(&queries[query_index], options.clone())
                        .map_err(|error| error.to_string())?;
                    measurements.push((elapsed_ms(query_started), report.bytes_read));
                }
                Ok(measurements)
            }));
        }

        let mut latencies_ms = Vec::with_capacity(dataset.queries.len());
        let mut bytes_read = 0_u128;
        for handle in handles {
            let measurements = handle
                .join()
                .map_err(|_| invalid_input("concurrency benchmark worker panicked"))?
                .map_err(|error| invalid_input(&format!("concurrency worker failed: {error}")))?;
            for (latency_ms, bytes) in measurements {
                latencies_ms.push(latency_ms);
                bytes_read += u128::from(bytes);
            }
        }
        let wall_seconds = started.elapsed().as_secs_f64();
        let total_queries = latencies_ms.len();
        let qps = if wall_seconds == 0.0 {
            total_queries as f64
        } else {
            total_queries as f64 / wall_seconds
        };
        writeln!(
            writer,
            "{workers},{total_queries},{qps:.3},{:.3},{:.3},{:.3},{:.3}",
            percentile(&latencies_ms, 0.50),
            percentile(&latencies_ms, 0.95),
            percentile(&latencies_ms, 0.99),
            mean(bytes_read as f64, total_queries)
        )?;
    }
    writer.flush()?;
    eprintln!("wrote {} rows={}", path.display(), config.concurrency.len());
    Ok(())
}

fn write_write_costs_csv(
    config: &ResolvedConfig,
    dataset: &Dataset,
    index: &mut BorsukIndex,
) -> BenchResult<()> {
    let write_ops = (dataset.train_count / WRITE_FRACTION_DENOMINATOR).max(1);
    let mut rows = Vec::with_capacity(4);
    rows.push(measure_upserts(config, dataset, index, write_ops)?);
    rows.push(measure_deletes(index, write_ops)?);

    let compact_started = Instant::now();
    let compact = index.compact(CompactionOptions::default())?;
    let compact_wall_ms = elapsed_ms(compact_started);
    rows.push(WriteRow {
        op: "compact",
        ops: 1,
        wall_ms: compact_wall_ms,
        latencies_ms: vec![compact_wall_ms],
        bytes_read: compact.bytes_read,
        bytes_written: compact.bytes_written,
    });

    let purge_started = Instant::now();
    let _purge = index.purge_with_report()?;
    let purge_wall_ms = elapsed_ms(purge_started);
    // PurgeReport exposes request counts and row/segment counts, but no byte
    // counters. The closest honest representation for this CSV is zero bytes.
    rows.push(WriteRow {
        op: "purge",
        ops: 1,
        wall_ms: purge_wall_ms,
        latencies_ms: vec![purge_wall_ms],
        bytes_read: 0,
        bytes_written: 0,
    });

    let path = config.output_dir.join("bench_write_costs.csv");
    let mut writer = csv_writer(&path)?;
    writeln!(
        writer,
        "op,ops,wall_ms,ops_per_s,p50_ms,p95_ms,bytes_read,bytes_written"
    )?;
    for row in &rows {
        let ops_per_second = if row.wall_ms == 0.0 {
            row.ops as f64
        } else {
            row.ops as f64 / (row.wall_ms / 1_000.0)
        };
        writeln!(
            writer,
            "{},{},{:.3},{ops_per_second:.3},{:.3},{:.3},{},{}",
            row.op,
            row.ops,
            row.wall_ms,
            percentile(&row.latencies_ms, 0.50),
            percentile(&row.latencies_ms, 0.95),
            row.bytes_read,
            row.bytes_written
        )?;
    }
    writer.flush()?;
    eprintln!("wrote {} rows={}", path.display(), rows.len());
    Ok(())
}

fn measure_upserts(
    config: &ResolvedConfig,
    dataset: &Dataset,
    index: &mut BorsukIndex,
    count: usize,
) -> BenchResult<WriteRow> {
    let mut reader = BufReader::new(File::open(config.dataset_dir.join("train.f32"))?);
    let started = Instant::now();
    let mut latencies_ms = Vec::with_capacity(count);
    let mut offset = 0_usize;
    while offset < count {
        let end = offset.saturating_add(WRITE_BATCH_SIZE).min(count);
        let mut records = Vec::with_capacity(end - offset);
        for id in offset..end {
            let mut vector = read_f32_vector(&mut reader, dataset.meta.dim)?;
            vector[0] += 1.0e-4;
            records.push(VectorRecord::new(id.to_string(), vector));
        }
        let batch_started = Instant::now();
        index.upsert(records)?;
        let per_op_ms = elapsed_ms(batch_started) / (end - offset) as f64;
        latencies_ms.extend(std::iter::repeat_n(per_op_ms, end - offset));
        offset = end;
    }
    let wall_ms = elapsed_ms(started);
    // upsert() has no report-returning variant. Unlike add_with_report(), it
    // cannot expose write bytes, so the byte columns remain zero rather than
    // substituting insert-only semantics for an actual MVCC upsert.
    Ok(WriteRow {
        op: "upsert",
        ops: count,
        wall_ms,
        latencies_ms,
        bytes_read: 0,
        bytes_written: 0,
    })
}

fn measure_deletes(index: &mut BorsukIndex, count: usize) -> BenchResult<WriteRow> {
    let started = Instant::now();
    let mut latencies_ms = Vec::with_capacity(count);
    let mut offset = 0_usize;
    while offset < count {
        let end = offset.saturating_add(WRITE_BATCH_SIZE).min(count);
        let ids = (offset..end).map(|id| id.to_string()).collect::<Vec<_>>();
        let batch_started = Instant::now();
        let report = index.delete_with_report(ids)?;
        let per_op_ms = elapsed_ms(batch_started) / (end - offset) as f64;
        latencies_ms.extend(std::iter::repeat_n(per_op_ms, report.deleted));
        offset = end;
    }
    let wall_ms = elapsed_ms(started);
    // DeleteReport reports tombstone counts and requests but does not expose the
    // tombstone object's bytes, so byte columns use zero rather than an estimate.
    Ok(WriteRow {
        op: "delete",
        ops: count,
        wall_ms,
        latencies_ms,
        bytes_read: 0,
        bytes_written: 0,
    })
}

fn run_queries(
    index: &BorsukIndex,
    queries: &[Vec<f32>],
    ground_truth: Option<&[Vec<String>]>,
    options: SearchOptions,
) -> BenchResult<QuerySummary> {
    let mut summary = QuerySummary::default();
    for (query_index, query) in queries.iter().enumerate() {
        let started = Instant::now();
        let report = index.search_with_report(query, options.clone())?;
        let recall = if let Some(truth) = ground_truth {
            let ids = report
                .hits
                .iter()
                .map(|hit| hit.id.to_utf8_string())
                .collect::<borsuk::Result<Vec<_>>>()?;
            Some(recall_at_k(&truth[query_index], &ids, RECALL_K)?)
        } else {
            None
        };
        summary.push(elapsed_ms(started), &report, recall);
    }
    Ok(summary)
}

fn approximate_options(routing_page_overfetch: usize, max_candidates: usize) -> SearchOptions {
    SearchOptions::approx(RECALL_K, LeafMode::Hybrid)
        .with_routing_page_overfetch(routing_page_overfetch)
        .with_max_candidates_per_segment(max_candidates)
}

fn reset_cache(path: &Path) -> io::Result<()> {
    if path.exists() {
        fs::remove_dir_all(path)?;
    }
    fs::create_dir_all(path)
}

fn csv_writer(path: &Path) -> io::Result<BufWriter<File>> {
    Ok(BufWriter::new(File::create(path)?))
}

fn percentile(values: &[f64], percentile: f64) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    let mut sorted = values.to_vec();
    sorted.sort_by(f64::total_cmp);
    let index = ((sorted.len() as f64 * percentile).ceil() as usize)
        .saturating_sub(1)
        .min(sorted.len() - 1);
    sorted[index]
}

fn mean(total: f64, count: usize) -> f64 {
    if count == 0 {
        0.0
    } else {
        total / count as f64
    }
}

fn dollars_per_million_queries(avg_requests_per_query: f64) -> f64 {
    avg_requests_per_query * 1_000_000.0 * PRICE_PER_REQUEST
}

fn elapsed_ms(started: Instant) -> f64 {
    started.elapsed().as_secs_f64() * 1_000.0
}

fn env_usize(name: &str, default: usize) -> BenchResult<usize> {
    match env::var(name) {
        Ok(value) => value.parse().map_err(|error| {
            invalid_input(&format!("{name} must be an unsigned integer: {error}")).into()
        }),
        Err(env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(error.into()),
    }
}

fn parse_concurrency(value: &str) -> BenchResult<Vec<usize>> {
    let workers = value
        .split(',')
        .map(str::trim)
        .map(|item| {
            item.parse::<usize>().map_err(|error| {
                invalid_input(&format!(
                    "BORSUK_BENCH_CONCURRENCY contains invalid value `{item}`: {error}"
                ))
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    if workers.is_empty() || workers.contains(&0) {
        return Err(invalid_input(
            "BORSUK_BENCH_CONCURRENCY must contain comma-separated positive worker counts",
        )
        .into());
    }
    Ok(workers)
}

fn non_empty_env(name: &str) -> Option<String> {
    env::var(name).ok().filter(|value| !value.trim().is_empty())
}

fn missing_dataset_error(path: Option<&Path>) -> io::Error {
    let location = path.map_or_else(
        || "BORSUK_BENCH_DATASET is not set".to_string(),
        |path| format!("dataset directory {} is missing", path.display()),
    );
    invalid_input(&format!(
        "{location}; run scripts/fetch_ann_dataset.py first, then set BORSUK_BENCH_DATASET"
    ))
}

fn invalid_input(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}
