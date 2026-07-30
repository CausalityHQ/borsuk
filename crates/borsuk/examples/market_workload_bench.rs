#![allow(missing_docs)]

//! Publication workload adapters that do not fit the dense ANN protocol.
//!
//! The market planner invokes this executable as `<workload> <build|query>`.
//! Dataset descriptors remain the authority for scale and generation/source
//! identity; this binary never silently substitutes a smaller corpus.

use std::{
    collections::BTreeMap,
    env,
    error::Error,
    fs,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    thread,
    time::Instant,
};

use arrow_array::{
    Array, FixedSizeListArray, Float16Array, Float32Array, LargeListArray, LargeStringArray,
    ListArray, RecordBatch, StringArray,
};
use borsuk::{
    BorsukIndex, BuildConfig, Filter, GlobalScanCodec, IndexConfig, LateInteractionSearchOptions,
    LateInteractionSearchReport, LeafMode, MetaValue, Metadata, OpenOptions, RecallGuarantee,
    SearchOptions, SearchReport, VectorElementType, VectorKind, VectorMetric, VectorRecord,
    VectorSpec,
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde::Deserialize;

type BenchResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

const K: usize = 10;

#[derive(Debug, Deserialize)]
struct DatasetDescriptor {
    dataset: String,
    workload: String,
    dimensions: usize,
    scale: serde_json::Value,
    adapter: String,
    benchmark: BenchmarkSpec,
}

#[derive(Debug, Deserialize)]
struct BenchmarkSpec {
    #[serde(default)]
    seed: u64,
    #[serde(default = "default_queries")]
    queries: usize,
    #[serde(default)]
    tenants: usize,
    #[serde(default)]
    records_per_tenant: usize,
    #[serde(default)]
    segment_max_vectors: usize,
    #[serde(default)]
    namespace_sizes: Vec<usize>,
    #[serde(default)]
    documents_file: String,
    #[serde(default)]
    queries_file: String,
    #[serde(default = "default_late_frontiers")]
    candidates_per_query_token: Vec<usize>,
    #[serde(default = "default_vector_element_type")]
    vector_element_type: VectorElementType,
}

fn default_queries() -> usize {
    100
}

fn default_vector_element_type() -> VectorElementType {
    VectorElementType::Float16
}

fn default_late_frontiers() -> Vec<usize> {
    vec![16, 32, 64, 128, 256]
}

#[derive(Clone)]
struct RuntimeConfig {
    dataset_dir: PathBuf,
    index_uri: String,
    output: PathBuf,
    cache_dir: PathBuf,
    cache_profile: String,
    cache_coverage_percent: usize,
    query_seed: u64,
    client_concurrency: usize,
    max_concurrent_searches: usize,
    max_concurrent_cell_decodes: usize,
    ram_budget_bytes: u64,
}

#[derive(Clone)]
struct FilterQuery {
    selectivity: f64,
    cutoff: usize,
    vector: Vec<f32>,
    truth: Vec<String>,
}

struct FilterSample {
    selectivity: f64,
    sample_index: usize,
    latency_ms: f64,
    recall_at_10: f64,
    fallback_exact: bool,
    leaf_mode: String,
    segments_searched: usize,
    segments_pruned: usize,
    rows_evaluated: usize,
    rows_passed: usize,
    bytes_read: u64,
    disk_reads: u64,
    backing_reads: u64,
    disk_bytes: u64,
    backing_bytes: u64,
    network_gets: u64,
    memory: MemoryEnvelope,
}

#[derive(Clone)]
struct NamespaceQuery {
    namespace: usize,
    vector: Vec<f32>,
    truth: Vec<String>,
}

struct NamespaceSample {
    phase: &'static str,
    namespace: usize,
    namespace_rows: usize,
    sample_index: usize,
    latency_ms: f64,
    recall_at_10: f64,
    bytes_read: u64,
    disk_reads: u64,
    backing_reads: u64,
    network_gets: u64,
    memory: MemoryEnvelope,
}

#[derive(Clone)]
struct LateQuery {
    query_id: String,
    tokens: Vec<Vec<f32>>,
    relevant_ids: Vec<String>,
}

struct LateSample {
    frontier: usize,
    sample_index: usize,
    query_id: String,
    latency_ms: f64,
    mrr_at_10: f64,
    recall_at_50: f64,
    token_search_ms: f64,
    rerank_ms: f64,
    query_tokens: usize,
    token_hits_considered: usize,
    candidate_entities: usize,
    bytes_read: u64,
    disk_cache_reads: u64,
    backing_reads: u64,
    disk_bytes: u64,
    backing_bytes: u64,
    network_gets: u64,
    memory: MemoryEnvelope,
}

#[derive(Clone, Copy)]
struct MemoryEnvelope {
    collection_resident_bytes: u64,
    retained_bytes: u64,
    retained_capacity_bytes: u64,
    retained_peak_bytes: u64,
    transient_bytes: u64,
    transient_capacity_bytes: u64,
    transient_peak_bytes: u64,
}

impl MemoryEnvelope {
    fn from_report(report: &SearchReport) -> Self {
        Self {
            collection_resident_bytes: report.collection_resident_bytes,
            retained_bytes: report.retained_bytes,
            retained_capacity_bytes: report.retained_capacity_bytes,
            retained_peak_bytes: report.retained_peak_bytes,
            transient_bytes: report.transient_bytes,
            transient_capacity_bytes: report.transient_capacity_bytes,
            transient_peak_bytes: report.transient_peak_bytes,
        }
    }

    fn from_late_report(report: &LateInteractionSearchReport) -> Self {
        Self {
            collection_resident_bytes: report.collection_resident_bytes,
            retained_bytes: report.retained_bytes,
            retained_capacity_bytes: report.retained_capacity_bytes,
            retained_peak_bytes: report.retained_peak_bytes,
            transient_bytes: report.transient_bytes,
            transient_capacity_bytes: report.transient_capacity_bytes,
            transient_peak_bytes: report.transient_peak_bytes,
        }
    }
}

fn main() {
    if let Err(error) = run() {
        eprintln!("market_workload_bench: {error}");
        std::process::exit(1);
    }
}

fn run() -> BenchResult<()> {
    let mut args = env::args().skip(1);
    let workload = args
        .next()
        .ok_or("usage: market_workload_bench <filter|namespace|late-interaction> <build|query>")?;
    let action = args
        .next()
        .ok_or("usage: market_workload_bench <filter|namespace|late-interaction> <build|query>")?;
    if args.next().is_some() || !matches!(action.as_str(), "build" | "query") {
        return Err("invalid market workload arguments".into());
    }
    let runtime = runtime_config()?;
    let descriptor = load_descriptor(&runtime.dataset_dir)?;
    fs::create_dir_all(&runtime.output)?;
    match (workload.as_str(), action.as_str()) {
        ("filter", "build") => filter_build(&runtime, &descriptor),
        ("filter", "query") => filter_query(&runtime, &descriptor),
        ("namespace", "build") => namespace_build(&runtime, &descriptor),
        ("namespace", "query") => namespace_query(&runtime, &descriptor),
        ("late-interaction", "build") => late_build(&runtime, &descriptor),
        ("late-interaction", "query") => late_query(&runtime, &descriptor),
        _ => Err(format!("unsupported market workload `{workload}`").into()),
    }
}

fn runtime_config() -> BenchResult<RuntimeConfig> {
    let required = |name: &str| -> BenchResult<String> {
        env::var(name).map_err(|_| format!("{name} must be set").into())
    };
    Ok(RuntimeConfig {
        dataset_dir: PathBuf::from(required("BORSUK_MARKET_DATASET")?),
        index_uri: required("BORSUK_MARKET_INDEX_URI")?,
        output: PathBuf::from(required("BORSUK_MARKET_OUTPUT")?),
        cache_dir: PathBuf::from(required("BORSUK_MARKET_CACHE_DIR")?),
        cache_profile: required("BORSUK_MARKET_CACHE_PROFILE")?,
        cache_coverage_percent: required("BORSUK_MARKET_CACHE_COVERAGE_PERCENT")?.parse()?,
        query_seed: env::var("BORSUK_MARKET_QUERY_SEED")
            .unwrap_or_else(|_| "0".to_string())
            .parse()?,
        client_concurrency: required("BORSUK_MARKET_CLIENT_CONCURRENCY")?.parse()?,
        max_concurrent_searches: required("BORSUK_MARKET_MAX_CONCURRENT_SEARCHES")?.parse()?,
        max_concurrent_cell_decodes: required("BORSUK_MARKET_MAX_CONCURRENT_CELL_DECODES")?
            .parse()?,
        ram_budget_bytes: required("BORSUK_MARKET_RAM_BUDGET_BYTES")?.parse()?,
    })
}

fn load_descriptor(dataset_dir: &Path) -> BenchResult<DatasetDescriptor> {
    let descriptor: DatasetDescriptor =
        serde_json::from_slice(&fs::read(dataset_dir.join("dataset.json"))?)?;
    if descriptor.dimensions == 0
        || descriptor.benchmark.queries == 0
        || descriptor.benchmark.segment_max_vectors == 0
    {
        return Err(
            "benchmark dimensions, queries, and segment_max_vectors must be positive".into(),
        );
    }
    Ok(descriptor)
}

fn validate_filter_descriptor(descriptor: &DatasetDescriptor) -> BenchResult<()> {
    if descriptor.adapter != "borsuk_filter" || descriptor.workload != "dense_ann_filters" {
        return Err("filter dataset descriptor adapter/workload mismatch".into());
    }
    let expected = descriptor
        .benchmark
        .tenants
        .checked_mul(descriptor.benchmark.records_per_tenant)
        .ok_or("filter scale overflows")?;
    let declared = descriptor
        .scale
        .as_u64()
        .ok_or("filter dataset scale must be an integer")? as usize;
    if declared != expected {
        return Err(format!(
            "filter dataset scale {declared} does not match tenants × records_per_tenant {expected}"
        )
        .into());
    }
    if descriptor.benchmark.tenants == 0
        || descriptor.benchmark.records_per_tenant < K
        || !descriptor.benchmark.records_per_tenant.is_multiple_of(K)
    {
        return Err(
            "filter tenants must be positive and records_per_tenant a multiple of 10".into(),
        );
    }
    Ok(())
}

fn validate_namespace_descriptor(descriptor: &DatasetDescriptor) -> BenchResult<()> {
    if descriptor.adapter != "borsuk_namespace" || descriptor.workload != "namespace_isolation" {
        return Err("namespace dataset descriptor adapter/workload mismatch".into());
    }
    if descriptor.benchmark.namespace_sizes.len() < 2
        || descriptor
            .benchmark
            .namespace_sizes
            .iter()
            .any(|rows| *rows < K || !rows.is_multiple_of(K))
    {
        return Err(
            "namespace_sizes must contain at least two sizes, each a positive multiple of 10"
                .into(),
        );
    }
    Ok(())
}

fn filter_build(runtime: &RuntimeConfig, descriptor: &DatasetDescriptor) -> BenchResult<()> {
    validate_filter_descriptor(descriptor)?;
    let spec = &descriptor.benchmark;
    let mut index = BorsukIndex::create_with_build_config(
        IndexConfig {
            uri: runtime.index_uri.clone(),
            metric: VectorMetric::Euclidean,
            dimensions: descriptor.dimensions,
            segment_max_vectors: spec.segment_max_vectors,
            ram_budget_bytes: Some(runtime.ram_budget_bytes),
            text: false,
            named_vectors: Default::default(),
        },
        BuildConfig {
            vector_element_type: spec.vector_element_type,
            global_scan_codec: GlobalScanCodec::SrhtPq,
            ..BuildConfig::default()
        },
    )?;
    let started = Instant::now();
    let batch_rows = spec.segment_max_vectors.clamp(K, 16_384);
    let mut accepted = 0_usize;
    for tenant in 0..spec.tenants {
        let mut first = 0;
        while first < spec.records_per_tenant {
            let end = (first + batch_rows).min(spec.records_per_tenant);
            let records = (first..end)
                .map(|row| {
                    let group = row / K;
                    let mut metadata = Metadata::new();
                    metadata.insert("tenant".into(), MetaValue::Int(tenant as i64));
                    VectorRecord::new(
                        filter_id(tenant, group, row % K),
                        filter_vector(spec.seed, tenant, group, descriptor.dimensions),
                    )
                    .with_metadata(metadata)
                })
                .collect::<Vec<_>>();
            let batch_len = records.len();
            index.add(records)?;
            accepted = accepted.saturating_add(batch_len);
            first = end;
        }
    }
    index.finish_bulk_load()?;
    let stats = index.stats();
    let elapsed_ms = started.elapsed().as_secs_f64() * 1000.0;
    let body = format!(
        concat!(
            "dataset,records,dimensions,tenants,records_per_tenant,vector_element_type,",
            "elapsed_ms,vectors_per_s,segment_bytes,vector_bytes,global_scan_bytes,",
            "total_active_bytes,bytes_per_vector,resident_bytes_estimate,ram_budget_bytes,",
            "collection_resident_bytes,retained_bytes,retained_capacity_bytes,retained_peak_bytes,",
            "transient_bytes,transient_capacity_bytes,transient_peak_bytes\n",
            "{},{},{},{},{},{},{:.6},{:.6},{},{},{},{},{:.6},{},{},{},{},{},{},{},{},{}\n"
        ),
        descriptor.dataset,
        accepted,
        descriptor.dimensions,
        spec.tenants,
        spec.records_per_tenant,
        spec.vector_element_type,
        elapsed_ms,
        accepted as f64 / (elapsed_ms / 1000.0).max(f64::EPSILON),
        stats.segment_bytes,
        stats.vector_bytes,
        stats.global_scan_bytes,
        stats
            .segment_bytes
            .saturating_add(stats.vector_bytes)
            .saturating_add(stats.graph_bytes)
            .saturating_add(stats.global_scan_bytes),
        (stats
            .segment_bytes
            .saturating_add(stats.vector_bytes)
            .saturating_add(stats.graph_bytes)
            .saturating_add(stats.global_scan_bytes)) as f64
            / accepted.max(1) as f64,
        stats.resident_bytes_estimate,
        runtime.ram_budget_bytes,
        stats.collection_resident_bytes,
        stats.retained_bytes,
        stats.retained_capacity_bytes,
        stats.retained_peak_bytes,
        stats.transient_bytes,
        stats.transient_capacity_bytes,
        stats.transient_peak_bytes,
    );
    fs::write(runtime.output.join("filter_build.csv"), body)?;
    Ok(())
}

fn filter_query(runtime: &RuntimeConfig, descriptor: &DatasetDescriptor) -> BenchResult<()> {
    validate_filter_descriptor(descriptor)?;
    if runtime.cache_coverage_percent > 100
        || !matches!(
            runtime.cache_profile.as_str(),
            "uncached" | "disk_cached" | "mixed_coverage"
        )
    {
        return Err("filter cache profile/coverage is invalid".into());
    }
    let index = Arc::new(BorsukIndex::open_with_options(
        &runtime.index_uri,
        OpenOptions {
            cache_dir: Some(runtime.cache_dir.clone()),
            ram_budget_bytes: Some(runtime.ram_budget_bytes),
            max_concurrent_searches: Some(runtime.max_concurrent_searches),
            max_concurrent_cell_decodes: Some(runtime.max_concurrent_cell_decodes),
            ..OpenOptions::default()
        },
    )?);
    let queries = Arc::new(filter_queries(descriptor));
    let prime_count = match runtime.cache_profile.as_str() {
        "uncached" => 0,
        "disk_cached" => queries.len(),
        "mixed_coverage" => queries.len().saturating_mul(runtime.cache_coverage_percent) / 100,
        _ => unreachable!("validated profile"),
    };
    for query in queries.iter().take(prime_count) {
        let _ = run_filter_query(&index, query)?;
    }

    let next = Arc::new(AtomicUsize::new(0));
    let samples = Arc::new(Mutex::new(Vec::with_capacity(queries.len())));
    let workers = runtime.client_concurrency.min(queries.len()).max(1);
    thread::scope(|scope| {
        for _ in 0..workers {
            let index = Arc::clone(&index);
            let queries = Arc::clone(&queries);
            let next = Arc::clone(&next);
            let samples = Arc::clone(&samples);
            scope.spawn(move || {
                loop {
                    let sample_index = next.fetch_add(1, Ordering::Relaxed);
                    let Some(query) = queries.get(sample_index) else {
                        break;
                    };
                    let result = run_filter_query(&index, query)
                        .map(|mut sample| {
                            sample.sample_index = sample_index;
                            sample
                        })
                        .unwrap_or_else(|error| panic!("filter query failed: {error}"));
                    samples
                        .lock()
                        .unwrap_or_else(|error| error.into_inner())
                        .push(result);
                }
            });
        }
    });
    let mut samples = Arc::try_unwrap(samples)
        .map_err(|_| "filter sample owners remain")?
        .into_inner()
        .unwrap_or_else(|error| error.into_inner());
    samples.sort_by_key(|sample| sample.sample_index);
    write_filter_samples(runtime, descriptor, &samples)?;
    write_filter_summary(runtime, descriptor, &samples)?;
    Ok(())
}

fn filter_queries(descriptor: &DatasetDescriptor) -> Vec<FilterQuery> {
    let spec = &descriptor.benchmark;
    let groups = spec.records_per_tenant / K;
    let selectivities = [1.0_f64, 0.1, 0.01, 0.001];
    (0..spec.queries)
        .map(|query| {
            let selectivity = selectivities[query % selectivities.len()];
            let cutoff =
                ((spec.tenants as f64 * selectivity).ceil() as usize).clamp(1, spec.tenants);
            let tenant = (query / selectivities.len()) % cutoff;
            let group = (query / selectivities.len()) % groups;
            FilterQuery {
                selectivity,
                cutoff,
                vector: filter_vector(spec.seed, tenant, group, descriptor.dimensions),
                truth: (0..K)
                    .map(|duplicate| filter_id(tenant, group, duplicate))
                    .collect(),
            }
        })
        .collect()
}

fn run_filter_query(index: &BorsukIndex, query: &FilterQuery) -> BenchResult<FilterSample> {
    let filter =
        Filter::from_json(&serde_json::json!({ "tenant": { "$lt": query.cutoff as i64 } }))?;
    let started = Instant::now();
    let report = index.search_with_report(
        &query.vector,
        SearchOptions::approx(K, LeafMode::SrhtPqScan).with_filter(filter),
    )?;
    let latency_ms = started.elapsed().as_secs_f64() * 1000.0;
    let overlap = report
        .hits
        .iter()
        .filter(|hit| {
            query
                .truth
                .iter()
                .any(|truth| truth.as_bytes() == hit.id.as_bytes())
        })
        .count();
    let memory = MemoryEnvelope::from_report(&report);
    Ok(FilterSample {
        selectivity: query.selectivity,
        sample_index: 0,
        latency_ms,
        recall_at_10: overlap as f64 / K as f64,
        fallback_exact: report.recall_guarantee == RecallGuarantee::Exact,
        leaf_mode: report.leaf_mode,
        segments_searched: report.segments_searched,
        segments_pruned: report.segments_pruned_by_filter,
        rows_evaluated: report.rows_evaluated,
        rows_passed: report.rows_passed_filter,
        bytes_read: report.bytes_read,
        disk_reads: report.disk_cache_reads,
        backing_reads: report.backing_reads,
        disk_bytes: report.disk_cache_bytes_read,
        backing_bytes: report.backing_bytes_read,
        network_gets: report.requests.gets,
        memory,
    })
}

fn write_filter_samples(
    runtime: &RuntimeConfig,
    descriptor: &DatasetDescriptor,
    samples: &[FilterSample],
) -> BenchResult<()> {
    let mut body = String::from(
        "dataset,cache_profile,target_cache_coverage_percent,client_concurrency,selectivity,sample_index,latency_ms,recall_at_10,fallback_exact,leaf_mode,segments_searched,segments_pruned,rows_evaluated,rows_passed,bytes_read,disk_reads,backing_reads,disk_bytes,backing_bytes,network_gets,ram_budget_bytes,collection_resident_bytes,retained_bytes,retained_capacity_bytes,retained_peak_bytes,transient_bytes,transient_capacity_bytes,transient_peak_bytes\n",
    );
    for sample in samples {
        body.push_str(&format!(
            "{},{},{},{},{:.6},{},{:.6},{:.6},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}\n",
            descriptor.dataset,
            runtime.cache_profile,
            runtime.cache_coverage_percent,
            runtime.client_concurrency,
            sample.selectivity,
            sample.sample_index,
            sample.latency_ms,
            sample.recall_at_10,
            sample.fallback_exact,
            sample.leaf_mode,
            sample.segments_searched,
            sample.segments_pruned,
            sample.rows_evaluated,
            sample.rows_passed,
            sample.bytes_read,
            sample.disk_reads,
            sample.backing_reads,
            sample.disk_bytes,
            sample.backing_bytes,
            sample.network_gets,
            runtime.ram_budget_bytes,
            sample.memory.collection_resident_bytes,
            sample.memory.retained_bytes,
            sample.memory.retained_capacity_bytes,
            sample.memory.retained_peak_bytes,
            sample.memory.transient_bytes,
            sample.memory.transient_capacity_bytes,
            sample.memory.transient_peak_bytes,
        ));
    }
    fs::write(runtime.output.join("filter_samples.csv"), body)?;
    Ok(())
}

fn write_filter_summary(
    runtime: &RuntimeConfig,
    descriptor: &DatasetDescriptor,
    samples: &[FilterSample],
) -> BenchResult<()> {
    let mut body = String::from(
        "dataset,cache_profile,target_cache_coverage_percent,client_concurrency,selectivity,samples,mean_ms,stddev_ms,p50_ms,p95_ms,p99_ms,max_ms,recall_at_10,fallback_exact_ratio,avg_segments_searched,avg_segments_pruned,avg_rows_evaluated,avg_rows_passed,avg_bytes_read,avg_disk_reads,avg_backing_reads,avg_disk_bytes,avg_backing_bytes,avg_network_gets\n",
    );
    for selectivity in [1.0_f64, 0.1, 0.01, 0.001] {
        let rows = samples
            .iter()
            .filter(|sample| sample.selectivity == selectivity)
            .collect::<Vec<_>>();
        if rows.is_empty() {
            continue;
        }
        let latencies = rows
            .iter()
            .map(|sample| sample.latency_ms)
            .collect::<Vec<_>>();
        let average = |value: &dyn Fn(&FilterSample) -> f64| {
            rows.iter().map(|sample| value(sample)).sum::<f64>() / rows.len() as f64
        };
        body.push_str(&format!(
            "{},{},{},{},{:.6},{},{:.6},{:.6},{:.6},{:.6},{:.6},{:.6},{:.6},{:.6},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3}\n",
            descriptor.dataset,
            runtime.cache_profile,
            runtime.cache_coverage_percent,
            runtime.client_concurrency,
            selectivity,
            rows.len(),
            mean(&latencies),
            sample_stddev(&latencies),
            percentile(&latencies, 0.50),
            percentile(&latencies, 0.95),
            percentile(&latencies, 0.99),
            latencies.iter().copied().fold(0.0_f64, f64::max),
            average(&|sample| sample.recall_at_10),
            average(&|sample| f64::from(sample.fallback_exact)),
            average(&|sample| sample.segments_searched as f64),
            average(&|sample| sample.segments_pruned as f64),
            average(&|sample| sample.rows_evaluated as f64),
            average(&|sample| sample.rows_passed as f64),
            average(&|sample| sample.bytes_read as f64),
            average(&|sample| sample.disk_reads as f64),
            average(&|sample| sample.backing_reads as f64),
            average(&|sample| sample.disk_bytes as f64),
            average(&|sample| sample.backing_bytes as f64),
            average(&|sample| sample.network_gets as f64),
        ));
    }
    fs::write(runtime.output.join("filter_summary.csv"), body)?;
    Ok(())
}

fn namespace_build(runtime: &RuntimeConfig, descriptor: &DatasetDescriptor) -> BenchResult<()> {
    validate_namespace_descriptor(descriptor)?;
    let spec = &descriptor.benchmark;
    let mut body = String::from(
        "dataset,namespace,records,dimensions,vector_element_type,elapsed_ms,vectors_per_s,segment_bytes,vector_bytes,global_scan_bytes,total_active_bytes,bytes_per_vector,resident_bytes_estimate,ram_budget_bytes,collection_resident_bytes,retained_bytes,retained_capacity_bytes,retained_peak_bytes,transient_bytes,transient_capacity_bytes,transient_peak_bytes\n",
    );
    for (namespace, &rows) in spec.namespace_sizes.iter().enumerate() {
        let uri = namespace_uri(&runtime.index_uri, namespace);
        let mut index = BorsukIndex::create_with_build_config(
            IndexConfig {
                uri,
                metric: VectorMetric::Euclidean,
                dimensions: descriptor.dimensions,
                segment_max_vectors: spec.segment_max_vectors,
                ram_budget_bytes: Some(runtime.ram_budget_bytes),
                text: false,
                named_vectors: Default::default(),
            },
            BuildConfig {
                vector_element_type: spec.vector_element_type,
                global_scan_codec: GlobalScanCodec::SrhtPq,
                ..BuildConfig::default()
            },
        )?;
        let started = Instant::now();
        let batch_rows = spec.segment_max_vectors.clamp(K, 16_384);
        let mut first = 0;
        while first < rows {
            let end = (first + batch_rows).min(rows);
            let records = (first..end)
                .map(|row| {
                    let group = row / K;
                    VectorRecord::new(
                        namespace_id(namespace, group, row % K),
                        namespace_vector(spec.seed, namespace, group, descriptor.dimensions),
                    )
                })
                .collect::<Vec<_>>();
            index.add(records)?;
            first = end;
        }
        index.finish_bulk_load()?;
        let elapsed_ms = started.elapsed().as_secs_f64() * 1000.0;
        let stats = index.stats();
        let active_bytes = stats
            .segment_bytes
            .saturating_add(stats.vector_bytes)
            .saturating_add(stats.graph_bytes)
            .saturating_add(stats.global_scan_bytes);
        body.push_str(&format!(
            "{},{},{},{},{},{:.6},{:.6},{},{},{},{},{:.6},{},{},{},{},{},{},{},{},{}\n",
            descriptor.dataset,
            namespace,
            rows,
            descriptor.dimensions,
            spec.vector_element_type,
            elapsed_ms,
            rows as f64 / (elapsed_ms / 1000.0).max(f64::EPSILON),
            stats.segment_bytes,
            stats.vector_bytes,
            stats.global_scan_bytes,
            active_bytes,
            active_bytes as f64 / rows as f64,
            stats.resident_bytes_estimate,
            runtime.ram_budget_bytes,
            stats.collection_resident_bytes,
            stats.retained_bytes,
            stats.retained_capacity_bytes,
            stats.retained_peak_bytes,
            stats.transient_bytes,
            stats.transient_capacity_bytes,
            stats.transient_peak_bytes,
        ));
    }
    fs::write(runtime.output.join("namespace_build.csv"), body)?;
    Ok(())
}

fn namespace_query(runtime: &RuntimeConfig, descriptor: &DatasetDescriptor) -> BenchResult<()> {
    validate_namespace_descriptor(descriptor)?;
    if runtime.cache_coverage_percent > 100 {
        return Err("namespace cache coverage must be at most 100".into());
    }
    let spec = &descriptor.benchmark;
    // Baseline and noisy-neighbour phases get independent cache directories
    // and handles, then receive identical priming. Otherwise the first phase
    // would warm the second and create a fake isolation speedup.
    let baseline_indexes = open_namespace_indexes(runtime, descriptor, "baseline")?;
    let noisy_indexes = open_namespace_indexes(runtime, descriptor, "noisy")?;
    let queries = namespace_queries(descriptor);
    let prime_count = match runtime.cache_profile.as_str() {
        "uncached" => 0,
        "disk_cached" => queries.len(),
        "mixed_coverage" => queries.len().saturating_mul(runtime.cache_coverage_percent) / 100,
        _ => return Err("namespace cache profile is invalid".into()),
    };
    for indexes in [&baseline_indexes, &noisy_indexes] {
        for query in queries.iter().take(prime_count) {
            let _ = run_namespace_one(
                &indexes[query.namespace],
                query,
                spec.namespace_sizes[query.namespace],
                "prime",
                0,
            )?;
        }
    }

    let mut samples = run_namespace_cases(
        &baseline_indexes,
        &queries,
        &spec.namespace_sizes,
        "baseline",
        runtime.client_concurrency,
    )?;

    let largest = spec.namespace_sizes.len() - 1;
    let foreground = queries
        .iter()
        .filter(|query| query.namespace != largest)
        .cloned()
        .collect::<Vec<_>>();
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let background_index = Arc::clone(&noisy_indexes[largest]);
    let background_stop = Arc::clone(&stop);
    let background = NamespaceQuery {
        namespace: largest,
        vector: namespace_vector(spec.seed, largest, 0, descriptor.dimensions),
        truth: (0..K)
            .map(|duplicate| namespace_id(largest, 0, duplicate))
            .collect(),
    };
    let namespace_rows = spec.namespace_sizes[largest];
    let noisy = thread::spawn(move || {
        let mut query_index = 0;
        while !background_stop.load(Ordering::Relaxed) {
            let _ = run_namespace_one(
                &background_index,
                &background,
                namespace_rows,
                "background",
                query_index,
            );
            query_index += 1;
        }
    });
    let noisy_samples = run_namespace_cases(
        &noisy_indexes,
        &foreground,
        &spec.namespace_sizes,
        "noisy_neighbour",
        runtime.client_concurrency,
    )?;
    stop.store(true, Ordering::Relaxed);
    noisy
        .join()
        .map_err(|_| "namespace background worker panicked")?;
    samples.extend(noisy_samples);
    samples.sort_by_key(|sample| (sample.phase, sample.namespace, sample.sample_index));
    write_namespace_samples(runtime, descriptor, &samples)?;
    write_namespace_summary(runtime, descriptor, &samples)?;
    Ok(())
}

fn open_namespace_indexes(
    runtime: &RuntimeConfig,
    descriptor: &DatasetDescriptor,
    phase: &str,
) -> borsuk::Result<Vec<Arc<BorsukIndex>>> {
    descriptor
        .benchmark
        .namespace_sizes
        .iter()
        .enumerate()
        .map(|(namespace, _)| {
            BorsukIndex::open_with_options(
                &namespace_uri(&runtime.index_uri, namespace),
                OpenOptions {
                    cache_dir: Some(
                        runtime
                            .cache_dir
                            .join(phase)
                            .join(format!("namespace-{namespace:04}")),
                    ),
                    ram_budget_bytes: Some(runtime.ram_budget_bytes),
                    max_concurrent_searches: Some(runtime.max_concurrent_searches),
                    max_concurrent_cell_decodes: Some(runtime.max_concurrent_cell_decodes),
                    ..OpenOptions::default()
                },
            )
            .map(Arc::new)
        })
        .collect()
}

fn namespace_queries(descriptor: &DatasetDescriptor) -> Vec<NamespaceQuery> {
    let spec = &descriptor.benchmark;
    (0..spec.queries)
        .map(|query| {
            let namespace = query % spec.namespace_sizes.len();
            let groups = spec.namespace_sizes[namespace] / K;
            let group = (query / spec.namespace_sizes.len()) % groups;
            NamespaceQuery {
                namespace,
                vector: namespace_vector(spec.seed, namespace, group, descriptor.dimensions),
                truth: (0..K)
                    .map(|duplicate| namespace_id(namespace, group, duplicate))
                    .collect(),
            }
        })
        .collect()
}

fn run_namespace_cases(
    indexes: &[Arc<BorsukIndex>],
    queries: &[NamespaceQuery],
    namespace_sizes: &[usize],
    phase: &'static str,
    client_concurrency: usize,
) -> BenchResult<Vec<NamespaceSample>> {
    let indexes = Arc::new(indexes.to_vec());
    let queries = Arc::new(queries.to_vec());
    let sizes = Arc::new(namespace_sizes.to_vec());
    let next = Arc::new(AtomicUsize::new(0));
    let samples = Arc::new(Mutex::new(Vec::with_capacity(queries.len())));
    let workers = client_concurrency.min(queries.len()).max(1);
    thread::scope(|scope| {
        for _ in 0..workers {
            let indexes = Arc::clone(&indexes);
            let queries = Arc::clone(&queries);
            let sizes = Arc::clone(&sizes);
            let next = Arc::clone(&next);
            let samples = Arc::clone(&samples);
            scope.spawn(move || {
                loop {
                    let sample_index = next.fetch_add(1, Ordering::Relaxed);
                    let Some(query) = queries.get(sample_index) else {
                        break;
                    };
                    let sample = run_namespace_one(
                        &indexes[query.namespace],
                        query,
                        sizes[query.namespace],
                        phase,
                        sample_index,
                    )
                    .unwrap_or_else(|error| panic!("namespace query failed: {error}"));
                    samples
                        .lock()
                        .unwrap_or_else(|error| error.into_inner())
                        .push(sample);
                }
            });
        }
    });
    Ok(Arc::try_unwrap(samples)
        .map_err(|_| "namespace sample owners remain")?
        .into_inner()
        .unwrap_or_else(|error| error.into_inner()))
}

fn run_namespace_one(
    index: &BorsukIndex,
    query: &NamespaceQuery,
    namespace_rows: usize,
    phase: &'static str,
    sample_index: usize,
) -> BenchResult<NamespaceSample> {
    let started = Instant::now();
    let report = index.search_with_report(
        &query.vector,
        SearchOptions::approx(K, LeafMode::SrhtPqScan),
    )?;
    let latency_ms = started.elapsed().as_secs_f64() * 1000.0;
    let overlap = report
        .hits
        .iter()
        .filter(|hit| {
            query
                .truth
                .iter()
                .any(|truth| truth.as_bytes() == hit.id.as_bytes())
        })
        .count();
    Ok(NamespaceSample {
        phase,
        namespace: query.namespace,
        namespace_rows,
        sample_index,
        latency_ms,
        recall_at_10: overlap as f64 / K as f64,
        bytes_read: report.bytes_read,
        disk_reads: report.disk_cache_reads,
        backing_reads: report.backing_reads,
        network_gets: report.requests.gets,
        memory: MemoryEnvelope::from_report(&report),
    })
}

fn write_namespace_samples(
    runtime: &RuntimeConfig,
    descriptor: &DatasetDescriptor,
    samples: &[NamespaceSample],
) -> BenchResult<()> {
    let mut body = String::from(
        "dataset,cache_profile,target_cache_coverage_percent,client_concurrency,phase,namespace,namespace_rows,sample_index,latency_ms,recall_at_10,bytes_read,disk_reads,backing_reads,network_gets,auth_failures,auth_overhead_ms,ram_budget_bytes,collection_resident_bytes,retained_bytes,retained_capacity_bytes,retained_peak_bytes,transient_bytes,transient_capacity_bytes,transient_peak_bytes\n",
    );
    for sample in samples {
        body.push_str(&format!(
            "{},{},{},{},{},{},{},{},{:.6},{:.6},{},{},{},{},0,0.0,{},{},{},{},{},{},{},{}\n",
            descriptor.dataset,
            runtime.cache_profile,
            runtime.cache_coverage_percent,
            runtime.client_concurrency,
            sample.phase,
            sample.namespace,
            sample.namespace_rows,
            sample.sample_index,
            sample.latency_ms,
            sample.recall_at_10,
            sample.bytes_read,
            sample.disk_reads,
            sample.backing_reads,
            sample.network_gets,
            runtime.ram_budget_bytes,
            sample.memory.collection_resident_bytes,
            sample.memory.retained_bytes,
            sample.memory.retained_capacity_bytes,
            sample.memory.retained_peak_bytes,
            sample.memory.transient_bytes,
            sample.memory.transient_capacity_bytes,
            sample.memory.transient_peak_bytes,
        ));
    }
    fs::write(runtime.output.join("namespace_samples.csv"), body)?;
    Ok(())
}

fn write_namespace_summary(
    runtime: &RuntimeConfig,
    descriptor: &DatasetDescriptor,
    samples: &[NamespaceSample],
) -> BenchResult<()> {
    let mut body = String::from(
        "dataset,cache_profile,target_cache_coverage_percent,client_concurrency,phase,namespace,namespace_rows,samples,mean_ms,stddev_ms,p50_ms,p95_ms,p99_ms,max_ms,recall_at_10,avg_bytes_read,avg_disk_reads,avg_backing_reads,avg_network_gets,noisy_neighbor_slowdown,auth_failures,auth_overhead_ms\n",
    );
    for phase in ["baseline", "noisy_neighbour"] {
        for (namespace, &namespace_rows) in descriptor.benchmark.namespace_sizes.iter().enumerate()
        {
            let rows = samples
                .iter()
                .filter(|sample| sample.phase == phase && sample.namespace == namespace)
                .collect::<Vec<_>>();
            if rows.is_empty() {
                continue;
            }
            let latencies = rows
                .iter()
                .map(|sample| sample.latency_ms)
                .collect::<Vec<_>>();
            let average = |value: &dyn Fn(&NamespaceSample) -> f64| {
                rows.iter().map(|sample| value(sample)).sum::<f64>() / rows.len() as f64
            };
            let baseline_mean = samples
                .iter()
                .filter(|sample| sample.phase == "baseline" && sample.namespace == namespace)
                .map(|sample| sample.latency_ms)
                .sum::<f64>()
                / samples
                    .iter()
                    .filter(|sample| sample.phase == "baseline" && sample.namespace == namespace)
                    .count()
                    .max(1) as f64;
            let slowdown = if phase == "noisy_neighbour" {
                mean(&latencies) / baseline_mean.max(f64::EPSILON)
            } else {
                1.0
            };
            body.push_str(&format!(
                "{},{},{},{},{},{},{},{},{:.6},{:.6},{:.6},{:.6},{:.6},{:.6},{:.6},{:.3},{:.3},{:.3},{:.3},{:.6},0,0.0\n",
                descriptor.dataset,
                runtime.cache_profile,
                runtime.cache_coverage_percent,
                runtime.client_concurrency,
                phase,
                namespace,
                namespace_rows,
                rows.len(),
                mean(&latencies),
                sample_stddev(&latencies),
                percentile(&latencies, 0.50),
                percentile(&latencies, 0.95),
                percentile(&latencies, 0.99),
                latencies.iter().copied().fold(0.0_f64, f64::max),
                average(&|sample| sample.recall_at_10),
                average(&|sample| sample.bytes_read as f64),
                average(&|sample| sample.disk_reads as f64),
                average(&|sample| sample.backing_reads as f64),
                average(&|sample| sample.network_gets as f64),
                slowdown,
            ));
        }
    }
    fs::write(runtime.output.join("namespace_summary.csv"), body)?;
    Ok(())
}

fn validate_late_descriptor(descriptor: &DatasetDescriptor) -> BenchResult<()> {
    if descriptor.adapter != "borsuk_late_interaction"
        || descriptor.workload != "late_interaction_maxsim"
    {
        return Err("late-interaction dataset descriptor adapter/workload mismatch".into());
    }
    if !matches!(
        descriptor.benchmark.vector_element_type,
        VectorElementType::Float32 | VectorElementType::Float16
    ) {
        return Err("late-interaction physical type must be float32 or float16".into());
    }
    if descriptor.benchmark.documents_file.is_empty()
        || descriptor.benchmark.queries_file.is_empty()
        || descriptor.benchmark.candidates_per_query_token.is_empty()
        || descriptor.benchmark.candidates_per_query_token.contains(&0)
    {
        return Err(
            "late-interaction documents_file, queries_file, and positive candidate frontiers are required"
                .into(),
        );
    }
    for relative in [
        &descriptor.benchmark.documents_file,
        &descriptor.benchmark.queries_file,
    ] {
        let path = Path::new(relative);
        if path.is_absolute() || path.components().any(|part| part.as_os_str() == "..") {
            return Err("late-interaction Parquet paths must be safe relative paths".into());
        }
    }
    Ok(())
}

fn late_build(runtime: &RuntimeConfig, descriptor: &DatasetDescriptor) -> BenchResult<()> {
    validate_late_descriptor(descriptor)?;
    let spec = &descriptor.benchmark;
    let mut index = BorsukIndex::create_with_build_config(
        IndexConfig {
            uri: runtime.index_uri.clone(),
            metric: VectorMetric::InnerProduct,
            dimensions: descriptor.dimensions,
            segment_max_vectors: spec.segment_max_vectors,
            ram_budget_bytes: Some(runtime.ram_budget_bytes),
            text: false,
            named_vectors: BTreeMap::from([(
                "tokens".to_string(),
                VectorSpec {
                    dimensions: descriptor.dimensions,
                    metric: VectorMetric::InnerProduct,
                    kind: VectorKind::LateInteraction,
                    element_type: spec.vector_element_type,
                },
            )]),
        },
        BuildConfig {
            vector_element_type: spec.vector_element_type,
            global_scan_codec: GlobalScanCodec::SrhtPq,
            ..BuildConfig::default()
        },
    )?;
    let documents = descriptor.benchmark.documents_file.as_str();
    let started = Instant::now();
    let mut accepted = 0_usize;
    for batch in parquet_batches(&runtime.dataset_dir.join(documents))? {
        let ids = batch
            .column_by_name("document_id")
            .ok_or("documents Parquet is missing document_id")?;
        let token_column = batch
            .column_by_name("tokens")
            .ok_or("documents Parquet is missing tokens")?;
        let mut records = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            let id = string_value(ids.as_ref(), row)?;
            let tokens = token_matrix(token_column.as_ref(), row, descriptor.dimensions)?;
            let primary = tokens
                .first()
                .cloned()
                .ok_or("late-interaction document has no tokens")?;
            records.push(VectorRecord::new(id, primary).with_late_interaction("tokens", tokens)?);
        }
        accepted = accepted.saturating_add(records.len());
        index.add(records)?;
    }
    let declared = descriptor
        .scale
        .as_u64()
        .ok_or("late-interaction scale must be an integer")? as usize;
    if accepted != declared {
        return Err(format!(
            "late-interaction documents contain {accepted} rows, descriptor declares {declared}"
        )
        .into());
    }
    index.finish_bulk_load()?;
    let elapsed_ms = started.elapsed().as_secs_f64() * 1000.0;
    let stats = index.stats();
    let active_bytes = stats
        .segment_bytes
        .saturating_add(stats.vector_bytes)
        .saturating_add(stats.graph_bytes)
        .saturating_add(stats.global_scan_bytes);
    fs::write(
        runtime.output.join("late_interaction_build.csv"),
        format!(
            concat!(
                "dataset,documents,token_dimensions,vector_element_type,elapsed_ms,documents_per_s,",
                "segment_bytes,vector_bytes,global_scan_bytes,total_active_bytes,bytes_per_document,resident_bytes_estimate,",
                "ram_budget_bytes,collection_resident_bytes,retained_bytes,retained_capacity_bytes,retained_peak_bytes,",
                "transient_bytes,transient_capacity_bytes,transient_peak_bytes\n",
                "{},{},{},{},{:.6},{:.6},{},{},{},{},{:.6},{},{},{},{},{},{},{},{},{}\n"
            ),
            descriptor.dataset,
            accepted,
            descriptor.dimensions,
            spec.vector_element_type,
            elapsed_ms,
            accepted as f64 / (elapsed_ms / 1000.0).max(f64::EPSILON),
            stats.segment_bytes,
            stats.vector_bytes,
            stats.global_scan_bytes,
            active_bytes,
            active_bytes as f64 / accepted.max(1) as f64,
            stats.resident_bytes_estimate,
            runtime.ram_budget_bytes,
            stats.collection_resident_bytes,
            stats.retained_bytes,
            stats.retained_capacity_bytes,
            stats.retained_peak_bytes,
            stats.transient_bytes,
            stats.transient_capacity_bytes,
            stats.transient_peak_bytes,
        ),
    )?;
    Ok(())
}

fn late_query(runtime: &RuntimeConfig, descriptor: &DatasetDescriptor) -> BenchResult<()> {
    validate_late_descriptor(descriptor)?;
    if runtime.cache_coverage_percent > 100 {
        return Err("late-interaction cache coverage must be at most 100".into());
    }
    let loaded_queries = read_late_queries(descriptor, &runtime.dataset_dir)?;
    let queries = Arc::new(
        permuted_positions(loaded_queries.len(), runtime.query_seed)
            .into_iter()
            .map(|position| loaded_queries[position].clone())
            .collect::<Vec<_>>(),
    );
    if queries.is_empty() {
        return Err("late-interaction query Parquet contains no rows".into());
    }
    let (prime_count, memory_preloaded) = late_cache_preparation(
        &runtime.cache_profile,
        queries.len(),
        runtime.cache_coverage_percent,
    )?;
    let mut all_samples = Vec::new();
    for &frontier in &descriptor.benchmark.candidates_per_query_token {
        let index = Arc::new(BorsukIndex::open_with_options(
            &runtime.index_uri,
            OpenOptions {
                cache_dir: Some(runtime.cache_dir.join(format!("frontier-{frontier:06}"))),
                ram_budget_bytes: Some(runtime.ram_budget_bytes),
                max_concurrent_searches: Some(runtime.max_concurrent_searches),
                max_concurrent_cell_decodes: Some(runtime.max_concurrent_cell_decodes),
                segment_cache_max_bytes: memory_preloaded.then_some(runtime.ram_budget_bytes),
                preload: memory_preloaded,
                ..OpenOptions::default()
            },
        )?);
        for query in queries.iter().take(prime_count) {
            let _ = run_late_one(&index, query, frontier, 0)?;
        }
        let next = Arc::new(AtomicUsize::new(0));
        let samples = Arc::new(Mutex::new(Vec::with_capacity(queries.len())));
        let workers = runtime.client_concurrency.min(queries.len()).max(1);
        thread::scope(|scope| {
            for _ in 0..workers {
                let index = Arc::clone(&index);
                let queries = Arc::clone(&queries);
                let next = Arc::clone(&next);
                let samples = Arc::clone(&samples);
                scope.spawn(move || {
                    loop {
                        let sample_index = next.fetch_add(1, Ordering::Relaxed);
                        let Some(query) = queries.get(sample_index) else {
                            break;
                        };
                        let sample = run_late_one(&index, query, frontier, sample_index)
                            .unwrap_or_else(|error| {
                                panic!("late-interaction query failed: {error}")
                            });
                        samples
                            .lock()
                            .unwrap_or_else(|error| error.into_inner())
                            .push(sample);
                    }
                });
            }
        });
        let mut frontier_samples = Arc::try_unwrap(samples)
            .map_err(|_| "late-interaction sample owners remain")?
            .into_inner()
            .unwrap_or_else(|error| error.into_inner());
        frontier_samples.sort_by_key(|sample| sample.sample_index);
        all_samples.extend(frontier_samples);
    }
    write_late_samples(runtime, descriptor, &all_samples)?;
    write_late_summary(runtime, descriptor, &all_samples)?;
    Ok(())
}

fn run_late_one(
    index: &BorsukIndex,
    query: &LateQuery,
    frontier: usize,
    sample_index: usize,
) -> BenchResult<LateSample> {
    let report = index.search_late_interaction_with_report(
        "tokens",
        query.tokens.clone(),
        LateInteractionSearchOptions::bounded(50, frontier),
    )?;
    let ranked = report
        .hits
        .iter()
        .map(|hit| hit.id.to_utf8_string())
        .collect::<borsuk::Result<Vec<_>>>()?;
    let first_relevant = ranked
        .iter()
        .take(10)
        .position(|id| query.relevant_ids.contains(id));
    let relevant_at_50 = ranked
        .iter()
        .take(50)
        .filter(|id| query.relevant_ids.contains(id))
        .count();
    Ok(LateSample {
        frontier,
        sample_index,
        query_id: query.query_id.clone(),
        latency_ms: report.elapsed_ms,
        mrr_at_10: first_relevant.map_or(0.0, |rank| 1.0 / (rank + 1) as f64),
        recall_at_50: relevant_at_50 as f64 / query.relevant_ids.len().max(1) as f64,
        token_search_ms: report.token_search_ms,
        rerank_ms: report.rerank_ms,
        query_tokens: report.query_tokens,
        token_hits_considered: report.token_hits_considered,
        candidate_entities: report.candidate_entities,
        bytes_read: report.bytes_read,
        disk_cache_reads: report.disk_cache_reads,
        backing_reads: report.backing_reads,
        disk_bytes: report.disk_cache_bytes_read,
        backing_bytes: report.backing_bytes_read,
        network_gets: report.requests.gets,
        memory: MemoryEnvelope::from_late_report(&report),
    })
}

fn read_late_queries(
    descriptor: &DatasetDescriptor,
    dataset_dir: &Path,
) -> BenchResult<Vec<LateQuery>> {
    let mut queries = Vec::new();
    for batch in parquet_batches(&dataset_dir.join(&descriptor.benchmark.queries_file))? {
        let ids = batch
            .column_by_name("query_id")
            .ok_or("queries Parquet is missing query_id")?;
        let token_column = batch
            .column_by_name("tokens")
            .ok_or("queries Parquet is missing tokens")?;
        let relevant_column = batch
            .column_by_name("relevant_ids")
            .ok_or("queries Parquet is missing relevant_ids")?;
        for row in 0..batch.num_rows() {
            queries.push(LateQuery {
                query_id: string_value(ids.as_ref(), row)?,
                tokens: token_matrix(token_column.as_ref(), row, descriptor.dimensions)?,
                relevant_ids: string_list_value(relevant_column.as_ref(), row)?,
            });
            if queries.len() == descriptor.benchmark.queries {
                return Ok(queries);
            }
        }
    }
    Ok(queries)
}

fn parquet_batches(path: &Path) -> BenchResult<Vec<RecordBatch>> {
    let reader = ParquetRecordBatchReaderBuilder::try_new(fs::File::open(path)?)?
        .with_batch_size(1024)
        .build()?;
    reader
        .map(|batch| batch.map_err(|error| error.into()))
        .collect()
}

fn string_value(array: &dyn Array, row: usize) -> BenchResult<String> {
    if let Some(values) = array.as_any().downcast_ref::<StringArray>() {
        return Ok(values.value(row).to_string());
    }
    if let Some(values) = array.as_any().downcast_ref::<LargeStringArray>() {
        return Ok(values.value(row).to_string());
    }
    Err("identifier column must be Utf8 or LargeUtf8".into())
}

fn token_matrix(array: &dyn Array, row: usize, dimensions: usize) -> BenchResult<Vec<Vec<f32>>> {
    let values = if let Some(list) = array.as_any().downcast_ref::<LargeListArray>() {
        list.value(row)
    } else if let Some(list) = array.as_any().downcast_ref::<ListArray>() {
        list.value(row)
    } else {
        return Err("tokens column must be List or LargeList".into());
    };
    let tokens = values
        .as_any()
        .downcast_ref::<FixedSizeListArray>()
        .ok_or("tokens values must be FixedSizeList")?;
    if usize::try_from(tokens.value_length())? != dimensions {
        return Err(format!(
            "token dimensions {} do not match descriptor {dimensions}",
            tokens.value_length()
        )
        .into());
    }
    (0..tokens.len())
        .map(|token| {
            let values = tokens.value(token);
            if let Some(values) = values.as_any().downcast_ref::<Float32Array>() {
                return Ok(values.values().to_vec());
            }
            if let Some(values) = values.as_any().downcast_ref::<Float16Array>() {
                return Ok(values.values().iter().copied().map(f32::from).collect());
            }
            Err("token values must be Float32 or Float16".into())
        })
        .collect()
}

fn string_list_value(array: &dyn Array, row: usize) -> BenchResult<Vec<String>> {
    let values = if let Some(list) = array.as_any().downcast_ref::<LargeListArray>() {
        list.value(row)
    } else if let Some(list) = array.as_any().downcast_ref::<ListArray>() {
        list.value(row)
    } else {
        return Err("relevant_ids must be List or LargeList".into());
    };
    (0..values.len())
        .map(|index| string_value(values.as_ref(), index))
        .collect()
}

fn write_late_samples(
    runtime: &RuntimeConfig,
    descriptor: &DatasetDescriptor,
    samples: &[LateSample],
) -> BenchResult<()> {
    let mut body = String::from(
        "dataset,cache_profile,target_cache_coverage_percent,client_concurrency,query_seed,frontier,sample_index,query_id,latency_ms,mrr_at_10,recall_at_50,token_search_ms,rerank_ms,query_tokens,token_hits_considered,candidate_entities,bytes_read,disk_cache_reads,backing_reads,disk_bytes,backing_bytes,network_gets,ram_budget_bytes,collection_resident_bytes,retained_bytes,retained_capacity_bytes,retained_peak_bytes,transient_bytes,transient_capacity_bytes,transient_peak_bytes\n",
    );
    for sample in samples {
        body.push_str(&format!(
            "{},{},{},{},{},{},{},{},{:.6},{:.6},{:.6},{:.6},{:.6},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}\n",
            descriptor.dataset,
            runtime.cache_profile,
            runtime.cache_coverage_percent,
            runtime.client_concurrency,
            runtime.query_seed,
            sample.frontier,
            sample.sample_index,
            sample.query_id,
            sample.latency_ms,
            sample.mrr_at_10,
            sample.recall_at_50,
            sample.token_search_ms,
            sample.rerank_ms,
            sample.query_tokens,
            sample.token_hits_considered,
            sample.candidate_entities,
            sample.bytes_read,
            sample.disk_cache_reads,
            sample.backing_reads,
            sample.disk_bytes,
            sample.backing_bytes,
            sample.network_gets,
            runtime.ram_budget_bytes,
            sample.memory.collection_resident_bytes,
            sample.memory.retained_bytes,
            sample.memory.retained_capacity_bytes,
            sample.memory.retained_peak_bytes,
            sample.memory.transient_bytes,
            sample.memory.transient_capacity_bytes,
            sample.memory.transient_peak_bytes,
        ));
    }
    fs::write(runtime.output.join("late_interaction_samples.csv"), body)?;
    Ok(())
}

fn write_late_summary(
    runtime: &RuntimeConfig,
    descriptor: &DatasetDescriptor,
    samples: &[LateSample],
) -> BenchResult<()> {
    let mut body = String::from(
        "dataset,cache_profile,target_cache_coverage_percent,client_concurrency,frontier,samples,mean_ms,stddev_ms,p50_ms,p95_ms,p99_ms,max_ms,mrr_at_10,recall_at_50,avg_token_search_ms,avg_rerank_ms,avg_query_tokens,avg_token_hits_considered,avg_candidate_entities,avg_bytes_read,avg_disk_bytes,avg_backing_bytes,avg_network_gets\n",
    );
    for &frontier in &descriptor.benchmark.candidates_per_query_token {
        let rows = samples
            .iter()
            .filter(|sample| sample.frontier == frontier)
            .collect::<Vec<_>>();
        if rows.is_empty() {
            continue;
        }
        let latencies = rows
            .iter()
            .map(|sample| sample.latency_ms)
            .collect::<Vec<_>>();
        let average = |value: &dyn Fn(&LateSample) -> f64| {
            rows.iter().map(|sample| value(sample)).sum::<f64>() / rows.len() as f64
        };
        body.push_str(&format!(
            "{},{},{},{},{},{},{:.6},{:.6},{:.6},{:.6},{:.6},{:.6},{:.6},{:.6},{:.6},{:.6},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3}\n",
            descriptor.dataset,
            runtime.cache_profile,
            runtime.cache_coverage_percent,
            runtime.client_concurrency,
            frontier,
            rows.len(),
            mean(&latencies),
            sample_stddev(&latencies),
            percentile(&latencies, 0.50),
            percentile(&latencies, 0.95),
            percentile(&latencies, 0.99),
            latencies.iter().copied().fold(0.0_f64, f64::max),
            average(&|sample| sample.mrr_at_10),
            average(&|sample| sample.recall_at_50),
            average(&|sample| sample.token_search_ms),
            average(&|sample| sample.rerank_ms),
            average(&|sample| sample.query_tokens as f64),
            average(&|sample| sample.token_hits_considered as f64),
            average(&|sample| sample.candidate_entities as f64),
            average(&|sample| sample.bytes_read as f64),
            average(&|sample| sample.disk_bytes as f64),
            average(&|sample| sample.backing_bytes as f64),
            average(&|sample| sample.network_gets as f64),
        ));
    }
    fs::write(runtime.output.join("late_interaction_summary.csv"), body)?;
    Ok(())
}

fn filter_id(tenant: usize, group: usize, duplicate: usize) -> String {
    format!("t{tenant:08}-g{group:08}-r{duplicate:02}")
}

fn namespace_uri(base: &str, namespace: usize) -> String {
    format!(
        "{}/namespaces/namespace-{namespace:04}",
        base.trim_end_matches('/')
    )
}

fn namespace_id(namespace: usize, group: usize, duplicate: usize) -> String {
    format!("n{namespace:04}-g{group:08}-r{duplicate:02}")
}

fn namespace_vector(seed: u64, namespace: usize, group: usize, dimensions: usize) -> Vec<f32> {
    filter_vector(
        seed ^ (namespace as u64).rotate_left(31),
        namespace,
        group,
        dimensions,
    )
}

fn filter_vector(seed: u64, tenant: usize, group: usize, dimensions: usize) -> Vec<f32> {
    let mut vector = (0..dimensions)
        .map(|dimension| {
            let mixed = splitmix64(
                seed ^ (group as u64).rotate_left(17) ^ (dimension as u64).rotate_left(39),
            );
            ((mixed >> 40) as f32 / ((1_u32 << 24) - 1) as f32).mul_add(2.0, -1.0)
        })
        .collect::<Vec<_>>();
    if let Some(first) = vector.first_mut() {
        // Larger than one binary16 ULP over the generated [-1, 1] range, so
        // tenant identity cannot disappear when the benchmark stores f16.
        *first += tenant as f32 * 0.01;
    }
    vector
}

fn splitmix64(mut value: u64) -> u64 {
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn permuted_positions(count: usize, seed: u64) -> Vec<usize> {
    let mut positions = (0..count).collect::<Vec<_>>();
    let mut state = seed ^ 0x9e37_79b9_7f4a_7c15;
    for upper in (1..count).rev() {
        state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mixed = splitmix64(state);
        positions.swap(upper, mixed as usize % (upper + 1));
    }
    positions
}

fn late_cache_preparation(
    cache_profile: &str,
    query_count: usize,
    coverage_percent: usize,
) -> BenchResult<(usize, bool)> {
    if coverage_percent > 100 {
        return Err("late-interaction cache coverage must be at most 100".into());
    }
    match cache_profile {
        "uncached" => Ok((0, false)),
        "disk_cached" => Ok((query_count, false)),
        "mixed_coverage" => Ok((query_count.saturating_mul(coverage_percent) / 100, false)),
        "memory_preloaded" => Ok((query_count, true)),
        _ => Err("late-interaction cache profile is invalid".into()),
    }
}

fn mean(values: &[f64]) -> f64 {
    values.iter().sum::<f64>() / values.len().max(1) as f64
}

fn sample_stddev(values: &[f64]) -> f64 {
    if values.len() < 2 {
        return 0.0;
    }
    let mean = mean(values);
    (values
        .iter()
        .map(|value| (value - mean).powi(2))
        .sum::<f64>()
        / (values.len() - 1) as f64)
        .sqrt()
}

fn percentile(values: &[f64], quantile: f64) -> f64 {
    let mut sorted = values.to_vec();
    sorted.sort_by(f64::total_cmp);
    let index = ((sorted.len().saturating_sub(1)) as f64 * quantile).round() as usize;
    sorted[index.min(sorted.len().saturating_sub(1))]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn filter_truth_is_stable_and_tenant_specific() {
        let first = filter_vector(7, 3, 11, 32);
        assert_eq!(first, filter_vector(7, 3, 11, 32));
        assert_ne!(first, filter_vector(7, 4, 11, 32));
        assert_ne!(first, filter_vector(7, 3, 12, 32));
        let canonical_tenants = (0..10)
            .map(|tenant| {
                VectorElementType::Float16
                    .canonicalize(&filter_vector(7, tenant, 11, 32))
                    .unwrap()
            })
            .collect::<Vec<_>>();
        assert!(
            canonical_tenants.windows(2).all(|pair| pair[0] != pair[1]),
            "every tenant separation must survive the declared physical precision"
        );
        assert_eq!(filter_id(3, 11, 2), "t00000003-g00000011-r02");
    }

    #[test]
    fn summaries_use_sample_standard_deviation() {
        assert_eq!(mean(&[1.0, 2.0, 3.0]), 2.0);
        assert!((sample_stddev(&[1.0, 2.0, 3.0]) - 1.0).abs() < f64::EPSILON);
        assert_eq!(percentile(&[3.0, 1.0, 2.0], 0.5), 2.0);
    }

    #[test]
    fn namespace_oracle_survives_float16_storage() {
        let first = namespace_vector(13, 2, 9, 32);
        assert_eq!(first, namespace_vector(13, 2, 9, 32));
        assert_ne!(
            VectorElementType::Float16.canonicalize(&first).unwrap(),
            VectorElementType::Float16
                .canonicalize(&namespace_vector(13, 3, 9, 32))
                .unwrap()
        );
        assert_eq!(namespace_id(2, 9, 4), "n0002-g00000009-r04");
    }

    #[test]
    fn late_query_permutation_is_seeded_and_membership_preserving() {
        let first = permuted_positions(20, 17);
        assert_eq!(first, permuted_positions(20, 17));
        assert_ne!(first, permuted_positions(20, 23));
        let mut sorted = first;
        sorted.sort_unstable();
        assert_eq!(sorted, (0..20).collect::<Vec<_>>());
    }

    #[test]
    fn late_cache_profiles_resolve_explicit_priming_and_preload() {
        assert_eq!(
            late_cache_preparation("uncached", 500, 50).unwrap(),
            (0, false)
        );
        assert_eq!(
            late_cache_preparation("mixed_coverage", 500, 25).unwrap(),
            (125, false)
        );
        assert_eq!(
            late_cache_preparation("disk_cached", 500, 0).unwrap(),
            (500, false)
        );
        assert_eq!(
            late_cache_preparation("memory_preloaded", 500, 0).unwrap(),
            (500, true)
        );
        assert!(late_cache_preparation("implicit", 500, 50).is_err());
    }
}
