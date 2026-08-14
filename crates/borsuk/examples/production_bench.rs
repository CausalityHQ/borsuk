#![allow(missing_docs)]

use std::{
    collections::BTreeSet,
    env,
    error::Error,
    fs::{self, File},
    io::{self, BufReader, BufWriter, Read, Write},
    path::{Path, PathBuf},
    str::FromStr,
    sync::Arc,
    thread,
    time::Instant,
};

use arrow_array::{
    Array, FixedSizeListArray, Float32Array, Int32Array, Int64Array, LargeListArray, ListArray,
    UInt32Array, UInt64Array,
};
use borsuk::{
    BorsukIndex, BuildConfig, CacheExecutionPolicy, CompactionOptions,
    DEFAULT_MAX_CONCURRENT_CELL_DECODES, DEFAULT_MAX_CONCURRENT_SEARCHES, GlobalPqLayout,
    GlobalScanCodec, IndexConfig, LeafCapability, LeafMode, OpenOptions, RequestCounts,
    SearchOptions, SearchReport, VectorElementType, VectorMetric, VectorRecord, WalConfig,
    WarmReport, recall_at_k, recommended_segment_max_vectors,
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde::Deserialize;

const DEFAULT_QUERIES: usize = 1_000;
const DEFAULT_CONCURRENCY: &str = "1,2,4,8,16";
// A positioned dense insert expands into record, ID-directory, and route-plan
// rows plus transaction metadata. Keep both its logical dense bytes and vector
// count comfortably below the immutable 64 MiB / 65,536-row append bounds.
const INGEST_DENSE_BATCH_BYTES: usize = 16 * 1024 * 1024;
const INGEST_BATCH_MAX_VECTORS: usize = 16_384;
const DEFAULT_WRITE_BATCH_SIZE: usize = 1_024;
// V12 persists the coarse-cell probe count in the authenticated codebook. The
// query-time sweep controls how many ranked leaf pages may be fetched. Keep the
// values aligned with the bounded V12 dispatcher so a benchmark can never
// silently measure the legacy segment path.
const DEFAULT_NPROBE_SWEEP: &[usize] = &[4, 8, 16, 32];
// V15 treats the public candidate knob as its whole-index exact-rerank row
// budget. Publication pins the preregistered depth instead of silently falling
// back to the persisted, corpus-size-aware serving default.
const DEFAULT_RECALL_CANDIDATES: &[usize] = &[512];
// Explicit pq-scan defaults for cache-state and concurrency measurements. Recall
// is recorded against the shipped ground truth in every selected serving row.
// Zero delegates to the persisted corpus-size-aware production default.
const SERVING_NPROBE: usize = 0;
// Serving and recall must measure the same qualified exact-rerank depth. An
// explicit environment override remains available for non-publication sweeps.
const SERVING_CANDIDATES: usize = 512;
const SERVING_PREFETCH_DEPTH: usize = 16;
const RECALL_K: usize = 10;
const HIGH_RECALL_ROUTING_OVERFETCH: usize = 64;
const WRITE_FRACTION_DENOMINATOR: usize = 20;
const CACHE_COVERAGE_COHORT_QUERIES: usize = 40;
const CACHE_COVERAGE_REPETITIONS: usize = 4;
const PRODUCTION_BENCH_SCHEMA_VERSION: &str = "borsuk-production-bench-v12";
const RECALL_LATENCY_HEADER: &str = "schema_version,scan_codec,turboquant_bits,turboquant_qjl_bits,turboquant_shards,cache_execution,execution_engine,phase,mode,nprobe,max_candidates,recall_at_10,samples,mean_ms,stddev_ms,p50_ms,p95_ms,p99_ms,max_ms,avg_global_leaf_directory_reads,avg_global_leaf_directory_bytes,avg_global_leaf_code_pages_read,avg_global_leaf_code_bytes,avg_global_leaf_pages_read,avg_global_leaf_page_bytes,avg_global_leaf_waves,avg_global_leaf_continuations,avg_global_leaf_exact_scores,avg_backing_reads,avg_backing_bytes_read,avg_bytes_read,avg_gets_per_query,dollars_per_million_queries";
const QUERY_SAMPLE_HEADER: &str = "schema_version,scan_codec,cache_execution,phase,mode,nprobe,max_candidates,sample_index,query_source_index,latency_ms,recall_at_10,execution_engine,segments_searched,global_leaf_directory_reads,global_leaf_directory_bytes,global_leaf_code_pages_read,global_leaf_code_bytes,global_leaf_pages_read,global_leaf_page_bytes,global_leaf_waves,global_leaf_continuations,global_leaf_exact_scores,bytes_read,decoded_cache_hits,disk_cache_reads,backing_reads,disk_cache_bytes_read,backing_bytes_read,network_gets,query_seed,repetition_id,ram_budget_bytes,collection_resident_bytes,retained_bytes,retained_capacity_bytes,retained_peak_bytes,transient_bytes,transient_capacity_bytes,transient_peak_bytes,global_leaf_code_requests,global_leaf_exact_requests";
const CACHE_STATE_HEADER: &str = "schema_version,scan_codec,turboquant_bits,turboquant_qjl_bits,turboquant_shards,cache_execution,execution_engine,phase,queries,recall_at_10,mean_ms,stddev_ms,p50_ms,p95_ms,p99_ms,max_ms,avg_global_leaf_directory_reads,avg_global_leaf_directory_bytes,avg_global_leaf_code_pages_read,avg_global_leaf_code_bytes,avg_global_leaf_pages_read,avg_global_leaf_page_bytes,avg_global_leaf_waves,avg_global_leaf_continuations,avg_global_leaf_exact_scores,avg_backing_reads,avg_backing_bytes_read,avg_bytes_read,avg_object_cache_misses,avg_network_gets,dollars_per_million_queries";
const CONCURRENCY_HEADER: &str = "schema_version,scan_codec,turboquant_bits,turboquant_qjl_bits,turboquant_shards,cache_execution,cache_profile,target_cache_coverage_percent,execution_engine,workers,total_queries,qps,mean_ms,stddev_ms,p50_ms,p95_ms,p99_ms,max_ms,avg_global_leaf_directory_reads,avg_global_leaf_directory_bytes,avg_global_leaf_code_pages_read,avg_global_leaf_code_bytes,avg_global_leaf_pages_read,avg_global_leaf_page_bytes,avg_global_leaf_waves,avg_global_leaf_continuations,avg_global_leaf_exact_scores,avg_backing_reads,avg_backing_bytes_read,avg_bytes_read";
const CONCURRENCY_SAMPLE_HEADER: &str = "schema_version,scan_codec,cache_execution,cache_profile,target_cache_coverage_percent,workers,sample_index,query_source_index,target_hot_set_member,latency_ms,recall_at_10,execution_engine,global_leaf_directory_reads,global_leaf_directory_bytes,global_leaf_code_pages_read,global_leaf_code_bytes,global_leaf_pages_read,global_leaf_page_bytes,global_leaf_waves,global_leaf_continuations,global_leaf_exact_scores,bytes_read,decoded_cache_hits,disk_cache_reads,backing_reads,decoded_cache_bytes_read,disk_cache_bytes_read,backing_bytes_read,network_gets,ram_budget_bytes,collection_resident_bytes,retained_bytes,retained_capacity_bytes,retained_peak_bytes,transient_bytes,transient_capacity_bytes,transient_peak_bytes";
const CACHE_COVERAGE_HEADER: &str = "schema_version,scan_codec,cache_execution,target_hot_query_fraction,repetition,cohort_position,query_class,query_index,execution_engine,observed_cache_tier,recall_at_10,latency_ms,segments_searched,global_leaf_directory_reads,global_leaf_directory_bytes,global_leaf_code_pages_read,global_leaf_code_bytes,global_leaf_pages_read,global_leaf_page_bytes,global_leaf_waves,global_leaf_continuations,global_leaf_exact_scores,decoded_cache_hits,disk_cache_reads,backing_reads,decoded_bytes_read,disk_bytes_read,backing_bytes_read,decoded_access_fraction,disk_access_fraction,backing_access_fraction,bytes_read,network_gets";
const BUILD_HEADER: &str = "logical_cell_catalog_checksum,logical_cells,logical_cell_dimensions,logical_cell_catalog_bytes,vector_element_type,scan_codec,turboquant_bits,turboquant_qjl_bits,turboquant_shards,build_layout,leaf_capability,segment_max_vectors,records,segment_bytes,vector_sidecar_bytes,graph_bytes,global_scan_bytes,total_active_index_bytes,bytes_per_vector,resident_bytes_estimate,ram_budget_bytes,collection_resident_bytes,retained_bytes,retained_capacity_bytes,retained_peak_bytes,transient_bytes,transient_capacity_bytes,transient_peak_bytes,ingest_ms,compaction_ms,compaction_bytes_read,compaction_bytes_written,storage_gets,storage_puts,storage_deletes,storage_heads,storage_lists,storage_bytes_read,storage_bytes_written";
const WRITE_COST_HEADER: &str = "op,configured_batch_records,ops,batches,wall_ms,ops_per_s,mean_batch_ms,stddev_batch_ms,p50_batch_ms,p95_batch_ms,p99_batch_ms,max_batch_ms,mean_amortized_ms,gets,puts,deletes,heads,lists,bytes_read,bytes_written";
const WRITE_SAMPLE_HEADER: &str =
    "op,batch_index,batch_records,batch_latency_ms,amortized_ms,gets,puts,deletes,heads,lists";
const LIFECYCLE_HEADER: &str = "configured_batch_records,inserted_vectors,logical_vector_bytes,insert_wall_ms,insert_vectors_per_s,first_batch_publish_ms,time_to_searchable_ms,searchable_samples,searchable_fraction,upsert_samples,upsert_correct_fraction,delete_samples,delete_absent_fraction,compact_delete_absent_fraction,purge_delete_absent_fraction,delta_flush_ms,time_to_fully_indexed_ms,wal_publish_bytes,indexed_delta_bytes,total_indexing_bytes,write_amplification,write_amplification_is_lower_bound,consolidation_ms,time_to_consolidated_ms,consolidated_global_bytes,consolidation_amplification";
const MUTATION_QUERY_HEADER: &str =
    "stage,queries,mean_ms,stddev_ms,p50_ms,p95_ms,p99_ms,max_ms,avg_bytes_read,avg_network_gets";
const MUTATION_QUERY_SAMPLE_HEADER: &str =
    "stage,sample_index,latency_ms,execution_engine,bytes_read,network_gets";
const MUTATION_QUERY_SAMPLES: usize = 100;
const DEFAULT_PRODUCTION_RAM_BUDGET_BYTES: u64 = borsuk::DEFAULT_RAM_BUDGET_BYTES;
// AWS S3 Standard GET pricing in eu-central-1 at the 2026-07-20 snapshot:
// $0.43 per one million requests. The checked-in dated cost model records the
// same regional value; callers evaluating another region should recompute from
// the raw request count rather than treating this convenience column as global.
const PRICE_PER_REQUEST: f64 = 0.43 / 1_000_000.0;

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
    write_batch_size: usize,
    write_ops: Option<usize>,
    update_percent: usize,
    delete_percent: usize,
    query_seed: u64,
    repetition_id: String,
    output_dir: PathBuf,
    concurrency: Vec<usize>,
    segment_max: usize,
    vector_element_type: VectorElementType,
    leaf_capability: LeafCapability,
    global_pq_layout: GlobalPqLayout,
    global_pq_code_bytes: Option<usize>,
    global_scan_codec: GlobalScanCodec,
    global_turboquant_bits: u8,
    global_turboquant_qjl_bits: u32,
    global_turboquant_shards: u32,
    logical_cell_catalog: Option<PathBuf>,
    logical_cells: Option<usize>,
    logical_cell_training_rows: Option<usize>,
    logical_cell_seed: u64,
    logical_cell_iterations: usize,
    cache_execution: CacheExecutionPolicy,
    force_segment_path: bool,
    ram_budget_bytes: Option<u64>,
    segment_cache_max_bytes: Option<u64>,
    disk_cache_max_bytes: Option<u64>,
    recall_nprobes: Vec<usize>,
    recall_candidates: Vec<usize>,
    recall_leaf_mode: LeafMode,
    serving_mode: ServingMode,
    serving_leaf_mode: LeafMode,
    serving_nprobe: usize,
    serving_candidates: usize,
    serving_prefetch_depth: usize,
    max_concurrent_searches: Option<usize>,
    max_concurrent_cell_decodes: Option<usize>,
    uncached_queries: usize,
    cache_profile: BenchmarkCacheProfile,
    cache_coverage_percent: usize,
    build_index: bool,
    build_only: bool,
    recall_only: bool,
    skip_recall: bool,
    skip_exact_recall: bool,
    recluster_build: bool,
    read_only: bool,
    insert_only: bool,
    preload_serving: bool,
    _uri_temp: Option<tempfile::TempDir>,
    _cache_temp: Option<tempfile::TempDir>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BenchmarkCacheProfile {
    All,
    Uncached,
    DiskCached,
    MixedCoverage,
}

impl BenchmarkCacheProfile {
    const fn as_str(self) -> &'static str {
        match self {
            Self::All => "all",
            Self::Uncached => "uncached",
            Self::DiskCached => "disk_cached",
            Self::MixedCoverage => "mixed_coverage",
        }
    }
}

impl FromStr for BenchmarkCacheProfile {
    type Err = io::Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "all" => Ok(Self::All),
            "uncached" => Ok(Self::Uncached),
            "disk_cached" | "disk-cached" => Ok(Self::DiskCached),
            "mixed_coverage" | "mixed-coverage" => Ok(Self::MixedCoverage),
            _ => Err(invalid_input(
                "BORSUK_BENCH_CACHE_PROFILE must be all, uncached, disk_cached, or mixed_coverage",
            )),
        }
    }
}

struct Dataset {
    meta: DatasetMeta,
    metric: VectorMetric,
    train_count: usize,
    source: DatasetVectorSource,
    queries: Arc<Vec<Vec<f32>>>,
    query_source_indices: Arc<Vec<usize>>,
    ground_truth: Vec<Vec<String>>,
}

enum DatasetVectorSource {
    Unavailable,
    RawF32,
    Parquet { train_files: Vec<PathBuf> },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ServingMode {
    Exact,
    Hybrid,
}

#[derive(Default)]
struct QuerySummary {
    latencies_ms: Vec<f64>,
    samples: Vec<QuerySample>,
    recall_sum: f64,
    /// Number of queries that contributed a recall figure (the denominator for
    /// [`QuerySummary::recall`]). Skips queries with no meaningful recall, e.g. a
    /// zero-norm query under cosine/angular.
    recall_count: usize,
    bytes_read: u128,
    billable_requests: u128,
    object_cache_misses: u128,
    global_leaf_directory_reads: u128,
    global_leaf_directory_bytes: u128,
    global_leaf_code_pages_read: u128,
    global_leaf_code_bytes: u128,
    global_leaf_pages_read: u128,
    global_leaf_page_bytes: u128,
    global_leaf_waves: u128,
    global_leaf_continuations: u128,
    global_leaf_exact_scores: u128,
    backing_reads: u128,
    backing_bytes_read: u128,
    execution_engines: BTreeSet<String>,
}

struct QuerySample {
    latency_ms: f64,
    recall: Option<f32>,
    execution_engine: String,
    segments_searched: usize,
    global_leaf_directory_reads: usize,
    global_leaf_directory_bytes: u64,
    global_leaf_code_pages_read: usize,
    global_leaf_code_requests: usize,
    global_leaf_code_bytes: u64,
    global_leaf_pages_read: usize,
    global_leaf_exact_requests: usize,
    global_leaf_page_bytes: u64,
    global_leaf_waves: usize,
    global_leaf_continuations: usize,
    global_leaf_exact_scores: usize,
    bytes_read: u64,
    decoded_cache_hits: usize,
    disk_cache_reads: u64,
    backing_reads: u64,
    disk_cache_bytes_read: u64,
    backing_bytes_read: u64,
    network_gets: u64,
    collection_resident_bytes: u64,
    retained_bytes: u64,
    retained_capacity_bytes: u64,
    retained_peak_bytes: u64,
    transient_bytes: u64,
    transient_capacity_bytes: u64,
    transient_peak_bytes: u64,
}

struct ConcurrencyMeasurement {
    position: usize,
    query_source_index: usize,
    target_hot_set_member: bool,
    latency_ms: f64,
    recall: f32,
    bytes_read: u64,
    decoded_cache_hits: usize,
    disk_cache_reads: u64,
    backing_reads: u64,
    decoded_cache_bytes_read: u64,
    disk_cache_bytes_read: u64,
    backing_bytes_read: u64,
    network_gets: u64,
    global_leaf_directory_reads: usize,
    global_leaf_directory_bytes: u64,
    global_leaf_code_pages_read: usize,
    global_leaf_code_bytes: u64,
    global_leaf_pages_read: usize,
    global_leaf_page_bytes: u64,
    global_leaf_waves: usize,
    global_leaf_continuations: usize,
    global_leaf_exact_scores: usize,
    execution_engine: String,
    collection_resident_bytes: u64,
    retained_bytes: u64,
    retained_capacity_bytes: u64,
    retained_peak_bytes: u64,
    transient_bytes: u64,
    transient_capacity_bytes: u64,
    transient_peak_bytes: u64,
}

impl QuerySummary {
    fn push(&mut self, elapsed_ms: f64, report: &SearchReport, recall: Option<f32>) {
        // Query-scoped tier counters are authoritative under parallel segment
        // reads. Summing per-segment logical byte totals can count overlapping
        // work more than once.
        let measured_bytes_read = report
            .disk_cache_bytes_read
            .saturating_add(report.backing_bytes_read);
        self.latencies_ms.push(elapsed_ms);
        self.samples.push(QuerySample {
            latency_ms: elapsed_ms,
            recall,
            execution_engine: execution_engine_label(report).to_string(),
            segments_searched: report.segments_searched,
            global_leaf_directory_reads: report.global_leaf_directory_reads,
            global_leaf_directory_bytes: report.global_leaf_directory_bytes,
            global_leaf_code_pages_read: report.global_leaf_code_pages_read,
            global_leaf_code_requests: report.global_leaf_code_requests,
            global_leaf_code_bytes: report.global_leaf_code_bytes,
            global_leaf_pages_read: report.global_leaf_pages_read,
            global_leaf_exact_requests: report.global_leaf_exact_requests,
            global_leaf_page_bytes: report.global_leaf_page_bytes,
            global_leaf_waves: report.global_leaf_waves,
            global_leaf_continuations: report.global_leaf_continuations,
            global_leaf_exact_scores: report.global_leaf_exact_scores,
            bytes_read: measured_bytes_read,
            decoded_cache_hits: report.decoded_cache_hits,
            disk_cache_reads: report.disk_cache_reads,
            backing_reads: report.backing_reads,
            disk_cache_bytes_read: report.disk_cache_bytes_read,
            backing_bytes_read: report.backing_bytes_read,
            network_gets: report.requests.gets.saturating_add(report.requests.heads),
            collection_resident_bytes: report.collection_resident_bytes,
            retained_bytes: report.retained_bytes,
            retained_capacity_bytes: report.retained_capacity_bytes,
            retained_peak_bytes: report.retained_peak_bytes,
            transient_bytes: report.transient_bytes,
            transient_capacity_bytes: report.transient_capacity_bytes,
            transient_peak_bytes: report.transient_peak_bytes,
        });
        if let Some(recall) = recall {
            self.recall_sum += f64::from(recall);
            self.recall_count += 1;
        }
        self.bytes_read += u128::from(measured_bytes_read);
        self.billable_requests +=
            u128::from(report.requests.gets.saturating_add(report.requests.heads));
        self.object_cache_misses += report.object_cache_misses as u128;
        self.global_leaf_directory_reads += report.global_leaf_directory_reads as u128;
        self.global_leaf_directory_bytes += u128::from(report.global_leaf_directory_bytes);
        self.global_leaf_code_pages_read += report.global_leaf_code_pages_read as u128;
        self.global_leaf_code_bytes += u128::from(report.global_leaf_code_bytes);
        self.global_leaf_pages_read += report.global_leaf_pages_read as u128;
        self.global_leaf_page_bytes += u128::from(report.global_leaf_page_bytes);
        self.global_leaf_waves += report.global_leaf_waves as u128;
        self.global_leaf_continuations += report.global_leaf_continuations as u128;
        self.global_leaf_exact_scores += report.global_leaf_exact_scores as u128;
        self.backing_reads += u128::from(report.backing_reads);
        self.backing_bytes_read += u128::from(report.backing_bytes_read);
        self.execution_engines
            .insert(execution_engine_label(report).to_string());
    }

    fn count(&self) -> usize {
        self.latencies_ms.len()
    }

    fn absorb(&mut self, mut other: Self) {
        self.latencies_ms.append(&mut other.latencies_ms);
        self.samples.append(&mut other.samples);
        self.recall_sum += other.recall_sum;
        self.recall_count += other.recall_count;
        self.bytes_read += other.bytes_read;
        self.billable_requests += other.billable_requests;
        self.object_cache_misses += other.object_cache_misses;
        self.global_leaf_directory_reads += other.global_leaf_directory_reads;
        self.global_leaf_directory_bytes += other.global_leaf_directory_bytes;
        self.global_leaf_code_pages_read += other.global_leaf_code_pages_read;
        self.global_leaf_code_bytes += other.global_leaf_code_bytes;
        self.global_leaf_pages_read += other.global_leaf_pages_read;
        self.global_leaf_page_bytes += other.global_leaf_page_bytes;
        self.global_leaf_waves += other.global_leaf_waves;
        self.global_leaf_continuations += other.global_leaf_continuations;
        self.global_leaf_exact_scores += other.global_leaf_exact_scores;
        self.backing_reads += other.backing_reads;
        self.backing_bytes_read += other.backing_bytes_read;
        self.execution_engines.append(&mut other.execution_engines);
    }

    /// Mean recall over the queries that contributed a recall figure. Queries
    /// with no meaningful recall — a zero-norm query under cosine/angular, whose
    /// distance to every vector is the metric max — are excluded from the average
    /// rather than dragging it toward zero.
    fn recall(&self) -> f64 {
        mean(self.recall_sum, self.recall_count)
    }

    fn average_bytes(&self) -> f64 {
        mean(self.bytes_read as f64, self.count())
    }

    fn average_requests(&self) -> f64 {
        mean(self.billable_requests as f64, self.count())
    }

    fn average_cache_misses(&self) -> f64 {
        mean(self.object_cache_misses as f64, self.count())
    }

    fn average_global_leaf_directory_reads(&self) -> f64 {
        mean(self.global_leaf_directory_reads as f64, self.count())
    }
    fn average_global_leaf_directory_bytes(&self) -> f64 {
        mean(self.global_leaf_directory_bytes as f64, self.count())
    }
    fn average_global_leaf_code_pages_read(&self) -> f64 {
        mean(self.global_leaf_code_pages_read as f64, self.count())
    }
    fn average_global_leaf_code_bytes(&self) -> f64 {
        mean(self.global_leaf_code_bytes as f64, self.count())
    }
    fn average_global_leaf_pages_read(&self) -> f64 {
        mean(self.global_leaf_pages_read as f64, self.count())
    }
    fn average_global_leaf_page_bytes(&self) -> f64 {
        mean(self.global_leaf_page_bytes as f64, self.count())
    }
    fn average_global_leaf_waves(&self) -> f64 {
        mean(self.global_leaf_waves as f64, self.count())
    }
    fn average_global_leaf_continuations(&self) -> f64 {
        mean(self.global_leaf_continuations as f64, self.count())
    }
    fn average_global_leaf_exact_scores(&self) -> f64 {
        mean(self.global_leaf_exact_scores as f64, self.count())
    }
    fn average_backing_reads(&self) -> f64 {
        mean(self.backing_reads as f64, self.count())
    }
    fn average_backing_bytes_read(&self) -> f64 {
        mean(self.backing_bytes_read as f64, self.count())
    }

    fn dollars_per_million_queries(&self) -> f64 {
        dollars_per_million_queries(self.average_requests())
    }

    fn execution_engine(&self) -> &str {
        if self.execution_engines.len() == 1 {
            self.execution_engines
                .first()
                .map_or("unknown", String::as_str)
        } else if self.execution_engines.is_empty() {
            "unknown"
        } else {
            "mixed"
        }
    }
}

fn execution_engine_label(report: &SearchReport) -> &str {
    if report.leaf_mode == "graph" && report.graph_candidates_added == 0 {
        "graph-no-expansion"
    } else {
        &report.leaf_mode
    }
}

struct WriteRow {
    op: &'static str,
    ops: usize,
    wall_ms: f64,
    latencies_ms: Vec<f64>,
    samples: Vec<WriteSample>,
    requests: RequestCounts,
    bytes_read: u64,
    bytes_written: u64,
}

struct WriteSample {
    op: &'static str,
    batch_index: usize,
    batch_records: usize,
    batch_latency_ms: f64,
    requests: RequestCounts,
}

struct InsertMeasurement {
    row: WriteRow,
    first_batch_publish_ms: f64,
}

struct UpsertMeasurement {
    row: WriteRow,
    expected_records: Vec<(String, Vec<f32>)>,
}

struct BuildMeasurement {
    logical_cell_catalog_checksum: String,
    logical_cells: u32,
    logical_cell_dimensions: u32,
    logical_cell_catalog_bytes: u64,
    layout: &'static str,
    ingest_ms: f64,
    compaction_ms: f64,
    compaction_bytes_read: u64,
    compaction_bytes_written: u64,
    storage_requests: RequestCounts,
    storage_bytes_read: u64,
    storage_bytes_written: u64,
    records: usize,
    segment_bytes: u64,
    vector_bytes: u64,
    graph_bytes: u64,
    global_scan_bytes: u64,
    resident_bytes_estimate: u64,
    collection_resident_bytes: u64,
    retained_bytes: u64,
    retained_capacity_bytes: u64,
    retained_peak_bytes: u64,
    transient_bytes: u64,
    transient_capacity_bytes: u64,
    transient_peak_bytes: u64,
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

    if config.build_index {
        let index_config = IndexConfig {
            uri: config.uri.clone(),
            metric: dataset.metric.clone(),
            dimensions: dataset.meta.dim,
            segment_max_vectors: config.segment_max,
            ram_budget_bytes: config.ram_budget_bytes,
            text: false,
            named_vectors: Default::default(),
        };
        let build_config = BuildConfig {
            vector_element_type: config.vector_element_type,
            global_pq_layout: config.global_pq_layout.clone(),
            global_pq_code_bytes: config.global_pq_code_bytes,
            global_scan_codec: config.global_scan_codec,
            global_turboquant_bits: config.global_turboquant_bits,
            global_turboquant_qjl_bits: config.global_turboquant_qjl_bits,
            global_turboquant_shards: config.global_turboquant_shards,
            ..BuildConfig::default()
        };
        // Each batch remains one durable and immediately visible positioned
        // transaction. A bulk build deliberately materializes at the bounded
        // receipt window instead of the online record/byte thresholds:
        // the normal 16,384-row batch equals the online record threshold and
        // would otherwise force one synchronous half-size segment per batch.
        // A shard that reaches its row/byte cap earlier self-materializes its
        // committed prefix before the unchanged append is retried.
        let wal = WalConfig::bulk_load(dataset.meta.dim);
        let mut index = match (&config.logical_cell_catalog, config.logical_cells) {
            (Some(path), Some(expected_cells)) => {
                let centroids = read_logical_cell_catalog(path, expected_cells, dataset.meta.dim)?;
                BorsukIndex::create_with_logical_cell_catalog_and_build_config(
                    index_config,
                    centroids,
                    wal,
                    config.leaf_capability,
                    build_config,
                )?
            }
            (None, Some(expected_cells)) => {
                let sample_rows = config
                    .logical_cell_training_rows
                    .unwrap_or_else(|| expected_cells.saturating_mul(32));
                if sample_rows < expected_cells || sample_rows > dataset.train_count {
                    return Err(invalid_input(&format!(
                        "logical-cell training rows must be in {expected_cells}..={}, got {sample_rows}",
                        dataset.train_count
                    ))
                    .into());
                }
                let sample = sample_logical_cell_training_vectors(
                    &config,
                    &dataset,
                    sample_rows,
                    config.logical_cell_seed,
                )?;
                let centroids = borsuk::train_logical_cell_centroids(
                    &sample,
                    dataset.metric.clone(),
                    expected_cells,
                    config.logical_cell_iterations,
                )?;
                BorsukIndex::create_with_logical_cell_catalog_and_build_config(
                    index_config,
                    centroids,
                    wal,
                    config.leaf_capability,
                    build_config,
                )?
            }
            (None, None) => BorsukIndex::create_with_wal_capability_and_build_config(
                index_config,
                wal,
                config.leaf_capability,
                build_config,
            )?,
            (Some(_), None) => unreachable!("logical-cell catalog configuration was validated"),
        };
        let catalog_evidence = index
            .logical_cell_catalog_evidence()
            .ok_or_else(|| invalid_input("created index has no logical-cell catalog evidence"))?;
        if config
            .logical_cells
            .is_some_and(|expected| catalog_evidence.1 as usize != expected)
        {
            return Err(invalid_input(&format!(
                "created logical-cell catalog has {} cells; expected {}",
                catalog_evidence.1,
                config.logical_cells.unwrap_or_default()
            ))
            .into());
        }

        let ingest_started = Instant::now();
        ingest_train(&mut index, &config.dataset_dir, &dataset)?;
        let ingest_ms = elapsed_ms(ingest_started);
        borsuk::report_build_timing("ingest")?;

        // Compare the low-memory ingest layout against an explicitly reclustered
        // layout. Both produce the same global product-PQ shortlist and recall;
        // reclustering may reduce exact-rerank GETs by colocating candidates.
        let compaction_started = Instant::now();
        let (layout, compaction_bytes_read, compaction_bytes_written) = if config.recluster_build {
            let report = index.compact(CompactionOptions {
                max_segments: None,
                ..CompactionOptions::default()
            })?;
            ("reclustered", report.bytes_read, report.bytes_written)
        } else {
            index.finish_bulk_load()?;
            ("ingest-preserving", 0, 0)
        };
        let compaction_ms = elapsed_ms(compaction_started);
        borsuk::report_build_timing("compaction")?;
        eprintln!(
            "build dataset={} records={} ingest_ms={ingest_ms:.3} compaction_ms={compaction_ms:.3} compaction_bytes_read={} compaction_bytes_written={}",
            dataset.meta.name, dataset.train_count, compaction_bytes_read, compaction_bytes_written
        );
        let stats = index.stats();
        let storage_requests = index.request_counts();
        let storage_bytes_read = index.backing_bytes_read();
        let storage_bytes_written = index.put_payload_bytes();
        let build = BuildMeasurement {
            logical_cell_catalog_checksum: catalog_evidence.0,
            logical_cells: catalog_evidence.1,
            logical_cell_dimensions: catalog_evidence.2,
            logical_cell_catalog_bytes: catalog_evidence.3,
            layout,
            ingest_ms,
            compaction_ms,
            compaction_bytes_read,
            compaction_bytes_written,
            storage_requests,
            storage_bytes_read,
            storage_bytes_written,
            records: stats.records,
            segment_bytes: stats.segment_bytes,
            vector_bytes: stats.vector_bytes,
            graph_bytes: stats.graph_bytes,
            global_scan_bytes: stats.global_scan_bytes,
            resident_bytes_estimate: stats.resident_bytes_estimate,
            collection_resident_bytes: stats.collection_resident_bytes,
            retained_bytes: stats.retained_bytes,
            retained_capacity_bytes: stats.retained_capacity_bytes,
            retained_peak_bytes: stats.retained_peak_bytes,
            transient_bytes: stats.transient_bytes,
            transient_capacity_bytes: stats.transient_capacity_bytes,
            transient_peak_bytes: stats.transient_peak_bytes,
        };
        write_build_csv(&config, &build)?;
    } else {
        eprintln!("build skipped: opening immutable index uri={}", config.uri);
    }
    if config.build_only {
        return Ok(());
    }

    if config.insert_only {
        let mut index = open_serving_index(&config)?;
        let write_ops = write_operation_count(dataset.train_count, config.write_ops)?;
        let insert = measure_inserts(&config, &dataset, &mut index, write_ops)?;
        let (samples, visible) = verify_insert_visibility(&dataset, &index, write_ops)?;
        if visible != samples {
            return Err(invalid_input(&format!(
                "durable insert visibility failed: {visible}/{samples} sampled records visible"
            ))
            .into());
        }
        write_cost_artifacts(&config, &[insert.row])?;
        fs::write(
            config.output_dir.join("INSERT_VISIBILITY_COMPLETE"),
            b"complete\n",
        )?;
        return Ok(());
    }

    if !config.skip_recall && config.cache_profile != BenchmarkCacheProfile::MixedCoverage {
        let reader = Arc::new(open_serving_index(&config)?);
        eprintln!("index build_config={:?}", reader.build_config());
        let preload_complete = if recall_preloads_local_snapshot(config.preload_serving) {
            warm_all_segments(&reader)?.coverage_complete
        } else {
            let _ = reader.prepare_serving_metadata()?;
            false
        };
        write_recall_latency_csv(&config, &dataset, &reader, preload_complete)?;
    }
    if config.recall_only {
        return Ok(());
    }

    reset_cache(&config.cache_dir)?;
    let open_started = Instant::now();
    let reader = Arc::new(open_serving_index(&config)?);
    eprintln!("index build_config={:?}", reader.build_config());
    let open_ms = elapsed_ms(open_started);
    let preload_started = Instant::now();
    let warm_report = config
        .preload_serving
        .then(|| warm_all_segments(&reader))
        .transpose()?;
    let preload_ms = if config.preload_serving {
        elapsed_ms(preload_started)
    } else {
        0.0
    };
    write_startup_csv(&config, open_ms, preload_ms, warm_report.as_ref())?;
    if config.cache_profile != BenchmarkCacheProfile::MixedCoverage {
        write_cold_warm_csv(
            &config,
            &dataset,
            &reader,
            warm_report.is_some_and(|report| report.coverage_complete),
        )?;
    }
    write_concurrency_csv(&config, &dataset, &reader)?;
    write_cache_coverage_csv(&config, &dataset)?;
    if config.read_only {
        return Ok(());
    }

    // BorsukIndex is cloneable but has no storage-level "copy index" API. All read
    // measurements are complete, so this cloned handle is the isolated mutable
    // benchmark copy; it shares the configured backing URI with the built index.
    let mut write_index = reader.as_ref().clone();
    drop(reader);
    write_write_costs_csv(&config, &dataset, &mut write_index)?;
    Ok(())
}

fn recall_preloads_local_snapshot(preload: bool) -> bool {
    preload
}

fn uses_memory_preloaded_phase(
    preload: bool,
    cache_execution: CacheExecutionPolicy,
    coverage_complete: bool,
) -> bool {
    preload && coverage_complete && !matches!(cache_execution, CacheExecutionPolicy::Scan)
}

fn uses_bounded_decoded_cache_phases(
    memory_preloaded: bool,
    leaf_mode: LeafMode,
    segment_cache_max_bytes: Option<u64>,
) -> bool {
    !memory_preloaded
        && segment_cache_max_bytes.is_some_and(|bytes| bytes > 0)
        && matches!(
            leaf_mode,
            LeafMode::Graph | LeafMode::VamanaPq | LeafMode::Hybrid
        )
}

fn effective_segment_cache_budget(config: &ResolvedConfig) -> Option<u64> {
    config.segment_cache_max_bytes.or_else(|| {
        config
            .preload_serving
            .then_some(config.ram_budget_bytes.unwrap_or(u64::MAX))
    })
}

fn open_serving_index(config: &ResolvedConfig) -> BenchResult<BorsukIndex> {
    let index = BorsukIndex::open_with_options(
        &config.uri,
        OpenOptions {
            cache_dir: Some(config.cache_dir.clone()),
            cache_max_bytes: config.disk_cache_max_bytes,
            ram_budget_bytes: config.ram_budget_bytes,
            segment_cache_max_bytes: effective_segment_cache_budget(config),
            // Routing summaries and the centroid graph are serving metadata.
            // Load/build them during open so neither cache-state measurement
            // charges one-time library initialization to the first query.
            resident_routing: true,
            max_concurrent_searches: config.max_concurrent_searches,
            max_concurrent_cell_decodes: config.max_concurrent_cell_decodes,
            ..OpenOptions::default()
        },
    )?;
    if config.serving_mode == ServingMode::Hybrid
        && config.serving_leaf_mode == config.global_scan_codec.leaf_mode()
    {
        let _ = index.prepare_serving_metadata()?;
    }
    Ok(index)
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
    let write_batch_size = env_usize("BORSUK_BENCH_WRITE_BATCH_SIZE", DEFAULT_WRITE_BATCH_SIZE)?;
    let write_ops = env_optional_cap("BORSUK_BENCH_WRITE_OPS", None)?;
    let update_percent = env_percentage("BORSUK_BENCH_UPDATE_PERCENT", 100)?;
    let delete_percent = env_percentage("BORSUK_BENCH_DELETE_PERCENT", 100)?;
    let query_seed = env_u64("BORSUK_BENCH_QUERY_SEED", 0)?;
    let repetition_id =
        non_empty_env("BORSUK_BENCH_REPETITION_ID").unwrap_or_else(|| "unspecified".to_string());
    if queries == 0 {
        return Err(invalid_input("BORSUK_BENCH_QUERIES must be greater than zero").into());
    }
    if write_batch_size == 0 {
        return Err(
            invalid_input("BORSUK_BENCH_WRITE_BATCH_SIZE must be greater than zero").into(),
        );
    }
    let output_dir = env::var_os("BORSUK_BENCH_OUTPUT_DIR")
        .filter(|value| !value.is_empty())
        .map_or_else(env::current_dir, |value| Ok(PathBuf::from(value)))?;
    let concurrency = parse_concurrency(
        &env::var("BORSUK_BENCH_CONCURRENCY").unwrap_or_else(|_| DEFAULT_CONCURRENCY.to_string()),
    )?;
    let layout_meta: DatasetMeta =
        serde_json::from_reader(BufReader::new(File::open(dataset_dir.join("meta.json"))?))?;
    let segment_max = env_usize(
        "BORSUK_BENCH_SEGMENT_MAX",
        recommended_segment_max_vectors(layout_meta.dim),
    )?;
    if segment_max == 0 {
        return Err(invalid_input("BORSUK_BENCH_SEGMENT_MAX must be greater than zero").into());
    }
    let vector_element_type = non_empty_env("BORSUK_BENCH_VECTOR_ELEMENT_TYPE").map_or(
        Ok::<VectorElementType, Box<dyn Error>>(VectorElementType::Float32),
        |value| {
            value
                .parse::<VectorElementType>()
                .map_err(|error| Box::<dyn Error>::from(invalid_input(&error.to_string())))
        },
    )?;
    let leaf_capability = non_empty_env("BORSUK_BENCH_LEAF_CAPABILITY")
        .map_or(Ok(default_build_leaf_capability()), |value| {
            parse_leaf_capability(&value)
        })?;
    let global_pq_layout = non_empty_env("BORSUK_BENCH_GLOBAL_PQ_LAYOUT")
        .map_or(Ok(GlobalPqLayout::Adaptive), |value| {
            parse_global_pq_layout(&value)
        })?;
    let global_pq_code_bytes = env_optional_cap("BORSUK_BENCH_GLOBAL_PQ_CODE_BYTES", None)?;
    let global_scan_codec = non_empty_env("BORSUK_BENCH_GLOBAL_SCAN_CODEC").map_or(
        Ok::<GlobalScanCodec, Box<dyn Error>>(GlobalScanCodec::SrhtPq),
        |value| {
            value
                .parse::<GlobalScanCodec>()
                .map_err(|error| Box::<dyn Error>::from(invalid_input(&error.to_string())))
        },
    )?;
    let global_turboquant_bits = u8::try_from(env_usize("BORSUK_BENCH_TURBOQUANT_BITS", 4)?)
        .map_err(|_| invalid_input("BORSUK_BENCH_TURBOQUANT_BITS must fit u8"))?;
    let global_turboquant_qjl_bits =
        u32::try_from(env_usize("BORSUK_BENCH_TURBOQUANT_QJL_BITS", 0)?)
            .map_err(|_| invalid_input("BORSUK_BENCH_TURBOQUANT_QJL_BITS must fit u32"))?;
    let global_turboquant_shards =
        u32::try_from(env_usize("BORSUK_BENCH_TURBOQUANT_SHARDS", 1)?)
            .map_err(|_| invalid_input("BORSUK_BENCH_TURBOQUANT_SHARDS must fit u32"))?;
    let logical_cell_catalog =
        non_empty_env("BORSUK_BENCH_LOGICAL_CELL_CATALOG").map(PathBuf::from);
    let logical_cells = env_optional_cap("BORSUK_BENCH_LOGICAL_CELLS", None)?;
    let logical_cell_training_rows =
        env_optional_cap("BORSUK_BENCH_LOGICAL_CELL_TRAINING_ROWS", None)?;
    let logical_cell_seed = env_u64("BORSUK_BENCH_LOGICAL_CELL_SEED", 0)?;
    let logical_cell_iterations = env_usize("BORSUK_BENCH_LOGICAL_CELL_ITERATIONS", 8)?;
    if logical_cell_iterations == 0 {
        return Err(invalid_input(
            "BORSUK_BENCH_LOGICAL_CELL_ITERATIONS must be greater than zero",
        )
        .into());
    }
    if logical_cell_catalog.is_some() && logical_cells.is_none() {
        return Err(invalid_input(
            "BORSUK_BENCH_LOGICAL_CELL_CATALOG requires BORSUK_BENCH_LOGICAL_CELLS",
        )
        .into());
    }
    if logical_cell_training_rows.is_some() && logical_cells.is_none() {
        return Err(invalid_input(
            "BORSUK_BENCH_LOGICAL_CELL_TRAINING_ROWS requires BORSUK_BENCH_LOGICAL_CELLS",
        )
        .into());
    }
    let cache_execution = non_empty_env("BORSUK_BENCH_CACHE_EXECUTION").map_or(
        Ok::<CacheExecutionPolicy, Box<dyn Error>>(CacheExecutionPolicy::Scan),
        |value| {
            value
                .parse::<CacheExecutionPolicy>()
                .map_err(|error| Box::<dyn Error>::from(invalid_input(&error.to_string())))
        },
    )?;
    let force_segment_path = env_flag("BORSUK_BENCH_FORCE_SEGMENT_PATH")?;
    let ram_budget_bytes = env_optional_byte_cap(
        "BORSUK_BENCH_RAM_BUDGET_BYTES",
        Some(DEFAULT_PRODUCTION_RAM_BUDGET_BYTES),
    )?;
    let segment_cache_max_bytes =
        env_optional_byte_cap("BORSUK_BENCH_SEGMENT_CACHE_MAX_BYTES", None)?;
    let disk_cache_max_bytes = env_optional_byte_cap("BORSUK_BENCH_DISK_CACHE_MAX_BYTES", None)?;
    let recall_nprobes = env_positive_list("BORSUK_BENCH_NPROBES", DEFAULT_NPROBE_SWEEP)?;
    let recall_candidates =
        env_positive_list("BORSUK_BENCH_CANDIDATES", DEFAULT_RECALL_CANDIDATES)?;
    let recall_leaf_mode = non_empty_env("BORSUK_BENCH_RECALL_LEAF_MODE")
        .map_or(Ok(default_recall_leaf_mode()), |value| {
            parse_leaf_mode(&value)
        })?;
    let serving_mode = non_empty_env("BORSUK_BENCH_SERVING_MODE")
        .map_or(Ok(ServingMode::Hybrid), |value| parse_serving_mode(&value))?;
    let serving_leaf_mode = non_empty_env("BORSUK_BENCH_SERVING_LEAF_MODE")
        .map_or(Ok(default_serving_leaf_mode()), |value| {
            parse_leaf_mode(&value)
        })?;
    for (name, leaf_mode) in [
        ("BORSUK_BENCH_RECALL_LEAF_MODE", recall_leaf_mode),
        ("BORSUK_BENCH_SERVING_LEAF_MODE", serving_leaf_mode),
    ] {
        if matches!(
            leaf_mode,
            LeafMode::PqScan
                | LeafMode::SrhtPqScan
                | LeafMode::FastTurboQuantMseScan
                | LeafMode::FastTurboQuantProdScan
        ) && leaf_mode != global_scan_codec.leaf_mode()
        {
            return Err(invalid_input(&format!(
                "{name}={leaf_mode} does not match BORSUK_BENCH_GLOBAL_SCAN_CODEC={global_scan_codec}"
            ))
            .into());
        }
    }
    validate_leaf_capability_modes(
        leaf_capability,
        recall_leaf_mode,
        serving_mode,
        serving_leaf_mode,
    )?;
    let serving_nprobe = env_usize("BORSUK_BENCH_SERVING_NPROBE", SERVING_NPROBE)?;
    if !force_segment_path {
        validate_v12_leaf_mode(
            "BORSUK_BENCH_RECALL_LEAF_MODE",
            recall_leaf_mode,
            global_scan_codec,
        )?;
        if serving_mode == ServingMode::Hybrid {
            validate_v12_leaf_mode(
                "BORSUK_BENCH_SERVING_LEAF_MODE",
                serving_leaf_mode,
                global_scan_codec,
            )?;
        }
        validate_v12_leaf_page_budgets(&recall_nprobes)?;
        validate_v12_candidate_budgets(&recall_candidates)?;
        if serving_mode == ServingMode::Hybrid && serving_nprobe != 0 {
            validate_v12_leaf_page_budgets(&[serving_nprobe])?;
        }
    }
    let serving_candidates = env_usize("BORSUK_BENCH_SERVING_CANDIDATES", SERVING_CANDIDATES)?;
    let serving_prefetch_depth = env_usize(
        "BORSUK_BENCH_SERVING_PREFETCH_DEPTH",
        SERVING_PREFETCH_DEPTH,
    )?;
    if serving_prefetch_depth == 0 {
        return Err(
            invalid_input("BORSUK_BENCH_SERVING_PREFETCH_DEPTH must be greater than zero").into(),
        );
    }
    let max_concurrent_searches = env_optional_cap(
        "BORSUK_BENCH_MAX_CONCURRENT_SEARCHES",
        Some(DEFAULT_MAX_CONCURRENT_SEARCHES),
    )?;
    let max_concurrent_cell_decodes = env_optional_cap(
        "BORSUK_BENCH_MAX_CONCURRENT_CELL_DECODES",
        Some(DEFAULT_MAX_CONCURRENT_CELL_DECODES),
    )?;
    let uncached_queries = env_usize("BORSUK_BENCH_UNCACHED_QUERIES", queries.min(100))?;
    if uncached_queries == 0 {
        return Err(
            invalid_input("BORSUK_BENCH_UNCACHED_QUERIES must be greater than zero").into(),
        );
    }
    let cache_profile = non_empty_env("BORSUK_BENCH_CACHE_PROFILE")
        .map_or(Ok(BenchmarkCacheProfile::All), |value| value.parse())?;
    let cache_coverage_percent = env_usize("BORSUK_BENCH_CACHE_COVERAGE_PERCENT", 50)?;
    if cache_coverage_percent > 100 {
        return Err(
            invalid_input("BORSUK_BENCH_CACHE_COVERAGE_PERCENT must be between 0 and 100").into(),
        );
    }
    let build_index = env_flag_with_default("BORSUK_BENCH_BUILD_INDEX", true)?;
    let build_only = env_flag("BORSUK_BENCH_BUILD_ONLY")?;
    validate_build_only(build_only, build_index)?;
    let recall_only = env_flag("BORSUK_BENCH_RECALL_ONLY")?;
    let skip_recall = env_flag("BORSUK_BENCH_SKIP_RECALL")?;
    let skip_exact_recall = env_flag("BORSUK_BENCH_SKIP_EXACT_RECALL")?;
    validate_phase_selection(recall_only, skip_recall)?;
    let read_only = env_flag("BORSUK_BENCH_READ_ONLY")?;
    let insert_only = env_flag("BORSUK_BENCH_INSERT_ONLY")?;
    validate_insert_only(insert_only, build_only, read_only)?;
    let preload_serving = env_flag("BORSUK_BENCH_PRELOAD_SERVING")?;
    let recluster_build = env_flag("BORSUK_BENCH_RECLUSTER_BUILD")?;

    Ok(ResolvedConfig {
        dataset_dir,
        uri,
        cache_dir,
        limit,
        queries,
        write_batch_size,
        write_ops,
        update_percent,
        delete_percent,
        query_seed,
        repetition_id,
        output_dir,
        concurrency,
        segment_max,
        vector_element_type,
        leaf_capability,
        global_pq_layout,
        global_pq_code_bytes,
        global_scan_codec,
        global_turboquant_bits,
        global_turboquant_qjl_bits,
        global_turboquant_shards,
        logical_cell_catalog,
        logical_cells,
        logical_cell_training_rows,
        logical_cell_seed,
        logical_cell_iterations,
        cache_execution,
        force_segment_path,
        ram_budget_bytes,
        segment_cache_max_bytes,
        disk_cache_max_bytes,
        recall_nprobes,
        recall_candidates,
        recall_leaf_mode,
        serving_mode,
        serving_leaf_mode,
        serving_nprobe,
        serving_candidates,
        serving_prefetch_depth,
        max_concurrent_searches,
        max_concurrent_cell_decodes,
        uncached_queries: uncached_queries.min(queries),
        cache_profile,
        cache_coverage_percent,
        build_index,
        build_only,
        recall_only,
        skip_recall,
        skip_exact_recall,
        recluster_build,
        read_only,
        insert_only,
        preload_serving,
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
    let recall_nprobes = join_usizes(&config.recall_nprobes);
    let recall_candidates = join_usizes(&config.recall_candidates);
    eprintln!(
        "config dataset={} uri={} cache={} limit={} queries={} write_batch_size={} write_ops={} uncached_queries={} output_dir={} concurrency={} segment_max={} vector_element_type={} leaf_capability={} global_scan_codec={} global_pq_layout={:?} global_pq_code_bytes={} turboquant_bits={} turboquant_qjl_bits={} turboquant_shards={} cache_execution={} force_segment_path={} ram_budget_bytes={} segment_cache_max_bytes={} recall_nprobes={} recall_candidates={} recall_leaf_mode={} serving_mode={:?} serving_leaf_mode={} serving_nprobe={} serving_candidates={} serving_prefetch_depth={} max_concurrent_searches={} max_concurrent_cell_decodes={} cache_profile={:?} cache_coverage_percent={} build_index={} build_only={} recall_only={} skip_recall={} skip_exact_recall={} recluster_build={} read_only={} insert_only={} preload_serving={}",
        config.dataset_dir.display(),
        config.uri,
        config.cache_dir.display(),
        config.limit,
        config.queries,
        config.write_batch_size,
        config
            .write_ops
            .map_or_else(|| "default".to_string(), |value| value.to_string()),
        config.uncached_queries,
        config.output_dir.display(),
        concurrency,
        config.segment_max,
        config.vector_element_type,
        config.leaf_capability,
        config.global_scan_codec,
        config.global_pq_layout,
        config
            .global_pq_code_bytes
            .map_or_else(|| "adaptive".to_string(), |value| value.to_string()),
        config.global_turboquant_bits,
        config.global_turboquant_qjl_bits,
        config.global_turboquant_shards,
        config.cache_execution,
        config.force_segment_path,
        config
            .ram_budget_bytes
            .map_or_else(|| "unbounded".to_string(), |value| value.to_string()),
        config
            .segment_cache_max_bytes
            .map_or_else(|| "disabled".to_string(), |value| value.to_string()),
        recall_nprobes,
        recall_candidates,
        config.recall_leaf_mode,
        config.serving_mode,
        config.serving_leaf_mode,
        config.serving_nprobe,
        config.serving_candidates,
        config.serving_prefetch_depth,
        config
            .max_concurrent_searches
            .map_or_else(|| "uncapped".to_string(), |value| value.to_string()),
        config
            .max_concurrent_cell_decodes
            .map_or_else(|| "uncapped".to_string(), |value| value.to_string()),
        config.cache_profile,
        config.cache_coverage_percent,
        config.build_index,
        config.build_only,
        config.recall_only || !config.build_index,
        config.skip_recall,
        config.skip_exact_recall,
        config.recluster_build,
        config.read_only,
        config.insert_only,
        config.preload_serving,
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
    let metric = dataset_metric(&meta.metric)?;

    let train_count = if config.limit == 0 {
        meta.n_train
    } else {
        config.limit.min(meta.n_train)
    };
    let query_count = config.queries.min(meta.n_test);
    let raw_train_path = config.dataset_dir.join("train.f32");
    let (source, queries_vec, shipped_ground_truth) = if raw_train_path.is_file() {
        let test_path = config.dataset_dir.join("test.f32");
        let neighbors_path = config.dataset_dir.join("neighbors.i32");
        validate_file_size(&raw_train_path, meta.n_train, meta.dim, 4)?;
        validate_file_size(&test_path, meta.n_test, meta.dim, 4)?;
        validate_file_size(&neighbors_path, meta.n_test, meta.k, 4)?;
        (
            DatasetVectorSource::RawF32,
            read_f32_rows(&test_path, query_count, meta.dim)?,
            read_ground_truth(&neighbors_path, query_count, meta.k, meta.n_train)?,
        )
    } else {
        let train_files = parquet_train_files_for_phase(
            &config.dataset_dir,
            meta.n_train,
            allow_missing_corpus_for_phase(
                config.build_index,
                config.insert_only,
                config.recall_only || config.read_only,
            ),
        )?;
        let test_path = config.dataset_dir.join("test.parquet");
        let neighbors_path = config.dataset_dir.join("neighbors.parquet");
        let queries = read_parquet_vectors(&test_path, query_count, meta.dim, "emb")?;
        let neighbors = read_parquet_neighbors(
            &neighbors_path,
            query_count,
            meta.k,
            meta.n_train,
            "neighbors_id",
        )?;
        (
            train_files.map_or(DatasetVectorSource::Unavailable, |train_files| {
                DatasetVectorSource::Parquet { train_files }
            }),
            queries,
            neighbors,
        )
    };

    // Zero-norm vectors (nytimes-256 ships some) are no longer filtered here: the
    // engine scores them at the norm-dependent metric's MAXIMUM distance, so every
    // corpus vector is indexed and a zero simply ranks last. Zero-norm QUERIES are
    // not dropped either — they run and just contribute no recall figure (handled
    // in `run_queries`). The streaming ingest path and the subset-vs-full ground
    // truth choice below are identical for every metric.

    // The dataset's neighbors.i32 is ground truth over the FULL corpus. When the
    // corpus is subset (BORSUK_BENCH_LIMIT), those neighbor ids mostly point to
    // rows that were never indexed, so recall would collapse toward zero. In that
    // case compute ground truth by brute force over the actually-indexed vectors;
    // only a full-corpus run uses the file's precomputed neighbors.
    let ground_truth = if train_count < meta.n_train {
        let corpus = match &source {
            DatasetVectorSource::Unavailable => {
                return Err(
                    invalid_input("a subset recall run requires local corpus vectors").into(),
                );
            }
            DatasetVectorSource::RawF32 => read_f32_rows(&raw_train_path, train_count, meta.dim)?,
            DatasetVectorSource::Parquet { train_files } => {
                read_parquet_vectors_from_files(train_files, train_count, meta.dim, "emb")?
            }
        };
        brute_force_ground_truth(&corpus, &queries_vec, &metric, RECALL_K)
    } else {
        shipped_ground_truth
    };
    let positions = permuted_positions(queries_vec.len(), config.query_seed);
    let queries = Arc::new(
        positions
            .iter()
            .map(|position| queries_vec[*position].clone())
            .collect(),
    );
    let ground_truth = positions
        .iter()
        .map(|position| ground_truth[*position].clone())
        .collect();
    let query_source_indices = Arc::new(positions);

    Ok(Dataset {
        meta,
        metric,
        train_count,
        source,
        queries,
        query_source_indices,
        ground_truth,
    })
}

fn dataset_metric(name: &str) -> BenchResult<VectorMetric> {
    match name {
        "cosine" => Ok(VectorMetric::Cosine),
        "euclidean" => Ok(VectorMetric::Euclidean),
        "hamming" => Ok(VectorMetric::Hamming),
        other => Err(invalid_input(&format!(
            "unsupported meta.json metric `{other}`; expected cosine, euclidean, or hamming"
        ))
        .into()),
    }
}

/// True when the vector's L2 norm is zero (all-zero, or within a tiny epsilon of
/// zero after summing squares). Cosine/angular distance to such a vector is the
/// metric maximum, so a zero-norm QUERY has no meaningful nearest neighbour and
/// is excluded from the recall average (the corpus keeps its zero vectors).
fn is_zero_norm(vector: &[f32]) -> bool {
    let sum_sq: f32 = vector.iter().map(|value| value * value).sum();
    sum_sq <= f32::EPSILON
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

fn read_logical_cell_catalog(
    path: &Path,
    expected_cells: usize,
    dimensions: usize,
) -> BenchResult<Vec<Vec<f32>>> {
    if expected_cells == 0 || dimensions == 0 {
        return Err(invalid_input("logical-cell catalog shape must be non-zero").into());
    }
    validate_file_size(path, expected_cells, dimensions, 4)?;
    let centroids = read_f32_rows(path, expected_cells, dimensions)?;
    if centroids.iter().flatten().any(|value| !value.is_finite()) {
        return Err(invalid_input("logical-cell catalog contains a non-finite value").into());
    }
    Ok(centroids)
}

fn update_vector_reservoir(
    reservoir: &mut Vec<Vec<f32>>,
    vector: Vec<f32>,
    row: usize,
    capacity: usize,
    seed: u64,
) {
    if reservoir.len() < capacity {
        reservoir.push(vector);
        return;
    }
    let mut mixed = seed ^ (row as u64).wrapping_add(0x9e37_79b9_7f4a_7c15);
    mixed = (mixed ^ (mixed >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    mixed = (mixed ^ (mixed >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    mixed ^= mixed >> 31;
    let slot = (mixed % (row as u64 + 1)) as usize;
    if slot < capacity {
        reservoir[slot] = vector;
    }
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

fn find_parquet_train_files(dataset_dir: &Path) -> BenchResult<Vec<PathBuf>> {
    let mut files = fs::read_dir(dataset_dir)?
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| {
                    (name == "train.parquet" || name.starts_with("train-"))
                        && name.ends_with(".parquet")
                })
        })
        .collect::<Vec<_>>();
    files.sort();
    if files.is_empty() {
        return Err(invalid_input(&format!(
            "{} contains neither train.f32 nor unshuffled train*.parquet files; \
             shuffled VectorDBBench files cannot preserve ground-truth ids",
            dataset_dir.display()
        ))
        .into());
    }
    Ok(files)
}

fn parquet_train_files_for_phase(
    dataset_dir: &Path,
    expected_rows: usize,
    allow_missing_corpus: bool,
) -> BenchResult<Option<Vec<PathBuf>>> {
    let train_files = match find_parquet_train_files(dataset_dir) {
        Ok(files) => files,
        Err(_) if allow_missing_corpus => return Ok(None),
        Err(error) => return Err(error),
    };
    validate_parquet_row_count(&train_files, expected_rows, "training")?;
    Ok(Some(train_files))
}

fn allow_missing_corpus_for_phase(build_index: bool, insert_only: bool, query_only: bool) -> bool {
    !build_index && !insert_only && query_only
}

fn validate_parquet_row_count(
    paths: &[PathBuf],
    expected_rows: usize,
    label: &str,
) -> BenchResult<()> {
    let mut actual_rows = 0_u64;
    for path in paths {
        let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(path)?)?;
        actual_rows = actual_rows
            .checked_add(u64::try_from(
                builder.metadata().file_metadata().num_rows(),
            )?)
            .ok_or_else(|| invalid_input("Parquet row count overflow"))?;
    }
    if actual_rows != u64::try_from(expected_rows)? {
        return Err(invalid_input(&format!(
            "{label} Parquet files contain {actual_rows} rows; expected {expected_rows} from meta.json"
        ))
        .into());
    }
    Ok(())
}

fn read_parquet_vectors(
    path: &Path,
    rows: usize,
    dimensions: usize,
    column: &str,
) -> BenchResult<Vec<Vec<f32>>> {
    read_parquet_vectors_from_files(&[path.to_path_buf()], rows, dimensions, column)
}

fn read_parquet_vectors_from_files(
    paths: &[PathBuf],
    rows: usize,
    dimensions: usize,
    column: &str,
) -> BenchResult<Vec<Vec<f32>>> {
    let mut result = Vec::with_capacity(rows);
    for path in paths {
        if result.len() == rows {
            break;
        }
        let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(path)?)?
            .with_batch_size(DEFAULT_WRITE_BATCH_SIZE)
            .build()?;
        for batch in reader {
            let batch = batch?;
            let vectors = batch.column_by_name(column).ok_or_else(|| {
                invalid_input(&format!(
                    "{} has no `{column}` vector column",
                    path.display()
                ))
            })?;
            for row in 0..batch.num_rows() {
                if result.len() == rows {
                    break;
                }
                result.push(vector_row(vectors.as_ref(), row, dimensions, column)?);
            }
        }
    }
    if result.len() != rows {
        return Err(invalid_input(&format!(
            "Parquet vector input ended after {} rows; expected {rows}",
            result.len()
        ))
        .into());
    }
    Ok(result)
}

fn vector_row(
    array: &dyn Array,
    row: usize,
    dimensions: usize,
    column: &str,
) -> BenchResult<Vec<f32>> {
    let values = if let Some(vectors) = array.as_any().downcast_ref::<FixedSizeListArray>() {
        vectors.value(row)
    } else if let Some(vectors) = array.as_any().downcast_ref::<ListArray>() {
        vectors.value(row)
    } else if let Some(vectors) = array.as_any().downcast_ref::<LargeListArray>() {
        vectors.value(row)
    } else {
        return Err(invalid_input(&format!(
            "Parquet `{column}` must be a fixed-size/list float32 vector, got {:?}",
            array.data_type()
        ))
        .into());
    };
    let floats = values
        .as_any()
        .downcast_ref::<Float32Array>()
        .ok_or_else(|| {
            invalid_input(&format!(
                "Parquet `{column}` values must be float32, got {:?}",
                values.data_type()
            ))
        })?;
    if floats.null_count() != 0 || floats.len() != dimensions {
        return Err(invalid_input(&format!(
            "Parquet `{column}` row has {} values/nulls; expected {dimensions} non-null float32 values",
            floats.len()
        ))
        .into());
    }
    Ok(floats.values().to_vec())
}

fn read_parquet_neighbors(
    path: &Path,
    rows: usize,
    neighbors_per_row: usize,
    n_train: usize,
    column: &str,
) -> BenchResult<Vec<Vec<String>>> {
    let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(path)?)?
        .with_batch_size(DEFAULT_WRITE_BATCH_SIZE)
        .build()?;
    let mut result = Vec::with_capacity(rows);
    for batch in reader {
        let batch = batch?;
        let neighbors = batch.column_by_name(column).ok_or_else(|| {
            invalid_input(&format!("{} has no `{column}` column", path.display()))
        })?;
        for row in 0..batch.num_rows() {
            if result.len() == rows {
                break;
            }
            result.push(neighbor_row(
                neighbors.as_ref(),
                row,
                neighbors_per_row,
                n_train,
                column,
            )?);
        }
    }
    if result.len() != rows {
        return Err(invalid_input(&format!(
            "{} contains {} ground-truth rows; expected {rows}",
            path.display(),
            result.len()
        ))
        .into());
    }
    Ok(result)
}

fn neighbor_row(
    array: &dyn Array,
    row: usize,
    neighbors_per_row: usize,
    n_train: usize,
    column: &str,
) -> BenchResult<Vec<String>> {
    let values = if let Some(neighbors) = array.as_any().downcast_ref::<FixedSizeListArray>() {
        neighbors.value(row)
    } else if let Some(neighbors) = array.as_any().downcast_ref::<ListArray>() {
        neighbors.value(row)
    } else if let Some(neighbors) = array.as_any().downcast_ref::<LargeListArray>() {
        neighbors.value(row)
    } else {
        return Err(invalid_input(&format!(
            "Parquet `{column}` must be a list of integer ids, got {:?}",
            array.data_type()
        ))
        .into());
    };
    if values.len() < neighbors_per_row {
        return Err(invalid_input(&format!(
            "Parquet `{column}` row has {} neighbors; expected at least {neighbors_per_row}",
            values.len()
        ))
        .into());
    }
    let mut result = Vec::with_capacity(RECALL_K);
    for position in 0..RECALL_K {
        let id = integer_value(values.as_ref(), position, column)?;
        if id < 0 || usize::try_from(id)? >= n_train {
            return Err(invalid_input(&format!(
                "Parquet `{column}` contains out-of-range id {id}"
            ))
            .into());
        }
        result.push(id.to_string());
    }
    Ok(result)
}

fn integer_value(array: &dyn Array, row: usize, column: &str) -> BenchResult<i64> {
    if array.is_null(row) {
        return Err(invalid_input(&format!("Parquet `{column}` contains a null id")).into());
    }
    if let Some(values) = array.as_any().downcast_ref::<Int64Array>() {
        return Ok(values.value(row));
    }
    if let Some(values) = array.as_any().downcast_ref::<Int32Array>() {
        return Ok(i64::from(values.value(row)));
    }
    if let Some(values) = array.as_any().downcast_ref::<UInt32Array>() {
        return Ok(i64::from(values.value(row)));
    }
    if let Some(values) = array.as_any().downcast_ref::<UInt64Array>() {
        return i64::try_from(values.value(row)).map_err(Into::into);
    }
    Err(invalid_input(&format!(
        "Parquet `{column}` ids must be int32/int64/uint32/uint64, got {:?}",
        array.data_type()
    ))
    .into())
}

/// Exact top-`k` neighbor ids for each query over the indexed subset, by the
/// dataset metric. Used for subset runs where the file's full-corpus ground
/// truth does not apply. O(queries * corpus * dim) — fine for the subset sizes
/// a laptop run uses; a full-corpus run reads the precomputed neighbors instead.
fn brute_force_ground_truth(
    corpus: &[Vec<f32>],
    queries: &[Vec<f32>],
    metric: &VectorMetric,
    k: usize,
) -> Vec<Vec<String>> {
    queries
        .iter()
        .map(|query| {
            let mut scored: Vec<(usize, f32)> = corpus
                .iter()
                .enumerate()
                .map(|(id, vector)| (id, ground_truth_distance(query, vector, metric)))
                .collect();
            scored.sort_by(|left, right| left.1.total_cmp(&right.1));
            scored
                .iter()
                .take(k)
                .map(|(id, _)| id.to_string())
                .collect()
        })
        .collect()
}

/// Rank key matching the dataset metric (smaller = nearer): squared L2 for
/// euclidean, cosine distance for cosine, and unequal-coordinate count for
/// binary Hamming fixtures.
fn ground_truth_distance(a: &[f32], b: &[f32], metric: &VectorMetric) -> f32 {
    match metric {
        VectorMetric::Cosine => {
            let dot: f32 = a.iter().zip(b).map(|(x, y)| x * y).sum();
            let norm_a = a.iter().map(|x| x * x).sum::<f32>().sqrt();
            let norm_b = b.iter().map(|x| x * x).sum::<f32>().sqrt();
            if norm_a == 0.0 || norm_b == 0.0 {
                1.0
            } else {
                1.0 - dot / (norm_a * norm_b)
            }
        }
        VectorMetric::Hamming => a
            .iter()
            .zip(b)
            .filter(|(left, right)| left != right)
            .count() as f32,
        _ => a.iter().zip(b).map(|(x, y)| (x - y) * (x - y)).sum(),
    }
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
    // Both source forms stream bounded batches and use monotonic generated ids.
    // VectorDBBench acquisition must use its unshuffled train files so row ids
    // remain identical to the shipped ground-truth neighbor ids.
    match &dataset.source {
        DatasetVectorSource::Unavailable => {
            return Err(invalid_input("index build requires local corpus vectors").into());
        }
        DatasetVectorSource::RawF32 => {
            let mut reader = BufReader::new(File::open(dataset_dir.join("train.f32"))?);
            let mut start = 0_usize;
            let batch_size = ingest_batch_size(dataset.meta.dim);
            while start < dataset.train_count {
                let end = start.saturating_add(batch_size).min(dataset.train_count);
                let mut vectors = Vec::with_capacity(end - start);
                for _ in start..end {
                    vectors.push(read_f32_vector(&mut reader, dataset.meta.dim)?);
                }
                ingest_generated_batch(index, start, vectors)?;
                start = end;
            }
        }
        DatasetVectorSource::Parquet { train_files } => {
            let mut start = 0_usize;
            let batch_size = ingest_batch_size(dataset.meta.dim);
            'files: for path in train_files {
                let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(path)?)?
                    .with_batch_size(batch_size)
                    .build()?;
                for batch in reader {
                    if start == dataset.train_count {
                        break 'files;
                    }
                    let batch = batch?;
                    let vectors = batch.column_by_name("emb").ok_or_else(|| {
                        invalid_input(&format!("{} has no `emb` vector column", path.display()))
                    })?;
                    let take = batch
                        .num_rows()
                        .min(dataset.train_count.saturating_sub(start));
                    let mut decoded = Vec::with_capacity(take);
                    for row in 0..take {
                        decoded.push(vector_row(vectors.as_ref(), row, dataset.meta.dim, "emb")?);
                    }
                    ingest_generated_batch(index, start, decoded)?;
                    start = start.saturating_add(take);
                }
            }
            if start != dataset.train_count {
                return Err(invalid_input(&format!(
                    "Parquet training stream ended after {start} rows; expected {}",
                    dataset.train_count
                ))
                .into());
            }
        }
    }
    Ok(())
}

fn sample_logical_cell_training_vectors(
    config: &ResolvedConfig,
    dataset: &Dataset,
    sample_rows: usize,
    seed: u64,
) -> BenchResult<Vec<Vec<f32>>> {
    let mut sample = Vec::with_capacity(sample_rows);
    stream_dataset_batches(config, dataset, dataset.train_count, |offset, vectors| {
        for (within_batch, vector) in vectors.into_iter().enumerate() {
            update_vector_reservoir(
                &mut sample,
                vector,
                offset.saturating_add(within_batch),
                sample_rows,
                seed,
            );
        }
        Ok(())
    })?;
    if sample.len() != sample_rows {
        return Err(invalid_input(&format!(
            "logical-cell sampling retained {} rows; expected {sample_rows}",
            sample.len()
        ))
        .into());
    }
    Ok(sample)
}

fn ingest_generated_batch(
    index: &mut BorsukIndex,
    start: usize,
    vectors: Vec<Vec<f32>>,
) -> BenchResult<()> {
    let end = start.saturating_add(vectors.len());
    // Ground-truth files address corpus rows by their numeric ordinal. Generated
    // library IDs are intentionally opaque, so the benchmark supplies the exact
    // stable row IDs instead of depending on an implementation detail.
    let ids = benchmark_row_ids(start, vectors.len());
    let inserted_ids = index.add_vectors_with_ids(vectors, ids)?;
    validate_generated_id_range(start, end, &inserted_ids)?;
    Ok(())
}

fn benchmark_row_ids(start: usize, count: usize) -> Vec<String> {
    (start..start.saturating_add(count))
        .map(|row| row.to_string())
        .collect()
}

fn ingest_batch_size(dimensions: usize) -> usize {
    let dense_bytes_per_vector = dimensions.saturating_mul(std::mem::size_of::<f32>()).max(1);
    (INGEST_DENSE_BATCH_BYTES / dense_bytes_per_vector).clamp(1, INGEST_BATCH_MAX_VECTORS)
}

fn validate_generated_id_range(start: usize, end: usize, ids: &[String]) -> BenchResult<()> {
    let expected_len = end.saturating_sub(start);
    let matches = ids.len() == expected_len
        && (expected_len == 0
            || (ids.first().map(String::as_str) == Some(start.to_string().as_str())
                && ids.last().map(String::as_str) == Some((end - 1).to_string().as_str())));
    if matches {
        Ok(())
    } else {
        Err(invalid_input(&format!(
            "generated ingest ids did not match dataset rows {start}..{end}"
        ))
        .into())
    }
}

fn warm_all_segments(index: &BorsukIndex) -> BenchResult<WarmReport> {
    // warm() attempts to decode every segment into the shared cache AND makes the routing
    // summaries resident — the latter is what activates the HNSW coarse
    // quantizer (it indexes all cell centroids, so it only runs on a resident
    // snapshot, never forcing a paged index to load its summaries).
    let report = index.warm()?;
    eprintln!(
        "warm segments_loaded={} segments_total={} segments_resident={} graphs_resident={} coverage_complete={} bytes_resident={}",
        report.segments_loaded,
        report.segments_total,
        report.segments_resident,
        report.graphs_resident,
        report.coverage_complete,
        report.bytes_resident,
    );
    Ok(report)
}

#[cfg(test)]
const fn preload_query_count() -> usize {
    0
}

fn write_startup_csv(
    config: &ResolvedConfig,
    open_ms: f64,
    preload_ms: f64,
    warm_report: Option<&WarmReport>,
) -> BenchResult<()> {
    let path = config.output_dir.join("bench_startup.csv");
    let mut writer = csv_writer(&path)?;
    writeln!(
        writer,
        "preload_requested,open_ms,preload_ms,segments_loaded,segments_total,segments_resident,graphs_resident,coverage_complete,bytes_resident"
    )?;
    let report = warm_report.copied().unwrap_or_default();
    writeln!(
        writer,
        "{},{open_ms:.3},{preload_ms:.3},{},{},{},{},{},{}",
        config.preload_serving,
        report.segments_loaded,
        report.segments_total,
        report.segments_resident,
        report.graphs_resident,
        report.coverage_complete,
        report.bytes_resident,
    )?;
    writer.flush()?;
    eprintln!("wrote {} rows=1", path.display());
    Ok(())
}

fn write_build_csv(config: &ResolvedConfig, build: &BuildMeasurement) -> BenchResult<()> {
    let path = config.output_dir.join("bench_build.csv");
    let mut writer = csv_writer(&path)?;
    writeln!(writer, "{BUILD_HEADER}")?;
    let total_active_index_bytes = build
        .logical_cell_catalog_bytes
        .saturating_add(build.segment_bytes)
        .saturating_add(build.vector_bytes)
        .saturating_add(build.graph_bytes)
        .saturating_add(build.global_scan_bytes);
    let bytes_per_vector = if build.records == 0 {
        0.0
    } else {
        total_active_index_bytes as f64 / build.records as f64
    };
    writeln!(
        writer,
        "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{bytes_per_vector:.6},{},{},{},{},{},{},{},{},{},{:.3},{:.3},{},{},{},{},{},{},{},{},{}",
        build.logical_cell_catalog_checksum,
        build.logical_cells,
        build.logical_cell_dimensions,
        build.logical_cell_catalog_bytes,
        config.vector_element_type,
        config.global_scan_codec,
        config.global_turboquant_bits,
        config.global_turboquant_qjl_bits,
        config.global_turboquant_shards,
        build.layout,
        config.leaf_capability,
        config.segment_max,
        build.records,
        build.segment_bytes,
        build.vector_bytes,
        build.graph_bytes,
        build.global_scan_bytes,
        total_active_index_bytes,
        build.resident_bytes_estimate,
        config.ram_budget_bytes.unwrap_or(0),
        build.collection_resident_bytes,
        build.retained_bytes,
        build.retained_capacity_bytes,
        build.retained_peak_bytes,
        build.transient_bytes,
        build.transient_capacity_bytes,
        build.transient_peak_bytes,
        build.ingest_ms,
        build.compaction_ms,
        build.compaction_bytes_read,
        build.compaction_bytes_written,
        build.storage_requests.gets,
        build.storage_requests.puts,
        build.storage_requests.deletes,
        build.storage_requests.heads,
        build.storage_requests.lists,
        build.storage_bytes_read,
        build.storage_bytes_written,
    )?;
    writer.flush()?;
    eprintln!("wrote {} rows=1", path.display());
    Ok(())
}

fn write_recall_latency_csv(
    config: &ResolvedConfig,
    dataset: &Dataset,
    index: &BorsukIndex,
    preload_complete: bool,
) -> BenchResult<()> {
    let path = config.output_dir.join("bench_recall_latency.csv");
    let mut writer = csv_writer(&path)?;
    writeln!(writer, "{RECALL_LATENCY_HEADER}")?;
    let samples_path = config.output_dir.join("bench_query_samples.csv");
    let mut samples_writer = csv_writer(&samples_path)?;
    writeln!(samples_writer, "{QUERY_SAMPLE_HEADER}")?;
    let query_source_indices = permuted_positions(dataset.queries.len(), config.query_seed);

    for &max_candidates in &config.recall_candidates {
        for &nprobe in &config.recall_nprobes {
            let options = approximate_options(
                config.recall_leaf_mode,
                HIGH_RECALL_ROUTING_OVERFETCH,
                max_candidates,
                nprobe,
                config.cache_execution,
                config.force_segment_path,
            );
            for (phase, summary) in
                run_recall_cache_phases(config, dataset, index, options, preload_complete)?
            {
                if !config.force_segment_path {
                    validate_bounded_v14_execution(&summary)?;
                }
                write_query_samples(
                    &mut samples_writer,
                    config,
                    QuerySampleContext {
                        phase,
                        mode: &config.recall_leaf_mode.to_string(),
                        nprobe,
                        max_candidates,
                        query_source_indices: &query_source_indices,
                    },
                    &summary,
                )?;
                write_recall_row(
                    &mut writer,
                    config,
                    phase,
                    &config.recall_leaf_mode.to_string(),
                    nprobe,
                    max_candidates,
                    &summary,
                )?;
            }
            writer.flush()?;
        }
    }

    if !config.skip_exact_recall {
        for (phase, summary) in run_recall_cache_phases(
            config,
            dataset,
            index,
            SearchOptions::exact(RECALL_K),
            preload_complete,
        )? {
            write_query_samples(
                &mut samples_writer,
                config,
                QuerySampleContext {
                    phase,
                    mode: "exact",
                    nprobe: 0,
                    max_candidates: 0,
                    query_source_indices: &query_source_indices,
                },
                &summary,
            )?;
            write_recall_row(&mut writer, config, phase, "exact", 0, 0, &summary)?;
        }
    }
    writer.flush()?;
    samples_writer.flush()?;
    eprintln!(
        "wrote {} rows={} dataset={}",
        path.display(),
        recall_row_count(
            config.recall_nprobes.len(),
            config.recall_candidates.len(),
            config.skip_exact_recall,
            uses_memory_preloaded_phase(
                config.preload_serving,
                config.cache_execution,
                preload_complete,
            ),
        ),
        dataset.meta.name
    );
    Ok(())
}

struct QuerySampleContext<'a> {
    phase: &'a str,
    mode: &'a str,
    nprobe: usize,
    max_candidates: usize,
    query_source_indices: &'a [usize],
}

fn write_query_samples(
    writer: &mut impl Write,
    config: &ResolvedConfig,
    context: QuerySampleContext<'_>,
    summary: &QuerySummary,
) -> io::Result<()> {
    let QuerySampleContext {
        phase,
        mode,
        nprobe,
        max_candidates,
        query_source_indices,
    } = context;
    for (sample_index, sample) in summary.samples.iter().enumerate() {
        let query_source_index = query_source_indices.get(sample_index).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "query sample has no source-index proof",
            )
        })?;
        writeln!(
            writer,
            "{PRODUCTION_BENCH_SCHEMA_VERSION},{},{},{phase},{mode},{nprobe},{max_candidates},{sample_index},{query_source_index},{:.6},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
            config.global_scan_codec,
            config.cache_execution,
            sample.latency_ms,
            sample
                .recall
                .map(|value| format!("{value:.6}"))
                .unwrap_or_default(),
            sample.execution_engine,
            sample.segments_searched,
            sample.global_leaf_directory_reads,
            sample.global_leaf_directory_bytes,
            sample.global_leaf_code_pages_read,
            sample.global_leaf_code_bytes,
            sample.global_leaf_pages_read,
            sample.global_leaf_page_bytes,
            sample.global_leaf_waves,
            sample.global_leaf_continuations,
            sample.global_leaf_exact_scores,
            sample.bytes_read,
            sample.decoded_cache_hits,
            sample.disk_cache_reads,
            sample.backing_reads,
            sample.disk_cache_bytes_read,
            sample.backing_bytes_read,
            sample.network_gets,
            config.query_seed,
            config.repetition_id,
            config.ram_budget_bytes.unwrap_or(0),
            sample.collection_resident_bytes,
            sample.retained_bytes,
            sample.retained_capacity_bytes,
            sample.retained_peak_bytes,
            sample.transient_bytes,
            sample.transient_capacity_bytes,
            sample.transient_peak_bytes,
            sample.global_leaf_code_requests,
            sample.global_leaf_exact_requests,
        )?;
    }
    Ok(())
}

fn recall_row_count(
    nprobes: usize,
    candidates: usize,
    skip_exact_recall: bool,
    memory_preloaded: bool,
) -> usize {
    (if memory_preloaded { 1 } else { 2 })
        * (nprobes * candidates + usize::from(!skip_exact_recall))
}

fn run_recall_cache_phases(
    config: &ResolvedConfig,
    dataset: &Dataset,
    index: &BorsukIndex,
    options: SearchOptions,
    preload_complete: bool,
) -> BenchResult<Vec<(&'static str, QuerySummary)>> {
    match config.cache_profile {
        BenchmarkCacheProfile::Uncached => {
            return Ok(vec![(
                "uncached",
                run_uncached_queries(config, dataset, index, options, dataset.queries.len())?,
            )]);
        }
        BenchmarkCacheProfile::DiskCached => {
            return Ok(vec![(
                "disk_cached",
                run_disk_cached_queries(config, dataset, index, options)?,
            )]);
        }
        BenchmarkCacheProfile::MixedCoverage => {
            return Err(invalid_input(
                "mixed cache coverage is measured by bench_cache_coverage.csv, not a homogeneous recall phase",
            )
            .into());
        }
        BenchmarkCacheProfile::All => {}
    }
    let memory_preloaded = uses_memory_preloaded_phase(
        config.preload_serving,
        config.cache_execution,
        preload_complete,
    );
    if memory_preloaded {
        let _ = run_queries(index, &dataset.queries[..1], None, options.clone())?;
        let memory_preloaded = run_queries(
            index,
            &dataset.queries,
            Some(&dataset.ground_truth),
            options,
        )?;
        return Ok(vec![("memory_preloaded", memory_preloaded)]);
    }
    if uses_bounded_decoded_cache_phases(
        memory_preloaded,
        options.mode.leaf_mode(),
        effective_segment_cache_budget(config),
    ) {
        reset_cache(&config.cache_dir)?;
        let fill = run_queries(
            index,
            &dataset.queries,
            Some(&dataset.ground_truth),
            options.clone(),
        )?;
        let steady = run_queries(
            index,
            &dataset.queries,
            Some(&dataset.ground_truth),
            options,
        )?;
        validate_disk_cached_network(&steady)?;
        return Ok(vec![
            (
                if config.preload_serving {
                    "partial_preload_mixed_first"
                } else {
                    "bounded_decoded_cache_fill"
                },
                fill,
            ),
            (
                if config.preload_serving {
                    "partial_preload_mixed_steady"
                } else {
                    "bounded_decoded_cache_steady"
                },
                steady,
            ),
        ]);
    }
    let mut uncached = QuerySummary::default();
    for query_index in 0..dataset.queries.len() {
        reset_cache(&config.cache_dir)?;
        uncached.absorb(run_queries(
            index,
            &dataset.queries[query_index..query_index + 1],
            Some(&dataset.ground_truth[query_index..query_index + 1]),
            options.clone(),
        )?);
    }

    reset_cache(&config.cache_dir)?;
    let _ = run_queries(index, &dataset.queries, None, options.clone())?;
    let disk_cached = run_queries(
        index,
        &dataset.queries,
        Some(&dataset.ground_truth),
        options,
    )?;
    validate_disk_cached_network(&disk_cached)?;
    Ok(vec![("uncached", uncached), ("disk_cached", disk_cached)])
}

fn run_uncached_queries(
    config: &ResolvedConfig,
    dataset: &Dataset,
    index: &BorsukIndex,
    options: SearchOptions,
    query_count: usize,
) -> BenchResult<QuerySummary> {
    let mut summary = QuerySummary::default();
    for query_index in 0..query_count.min(dataset.queries.len()) {
        reset_cache(&config.cache_dir)?;
        summary.absorb(run_queries(
            index,
            &dataset.queries[query_index..query_index + 1],
            Some(&dataset.ground_truth[query_index..query_index + 1]),
            options.clone(),
        )?);
    }
    Ok(summary)
}

fn run_disk_cached_queries(
    config: &ResolvedConfig,
    dataset: &Dataset,
    index: &BorsukIndex,
    options: SearchOptions,
) -> BenchResult<QuerySummary> {
    reset_cache(&config.cache_dir)?;
    let _ = run_queries(index, &dataset.queries, None, options.clone())?;
    let summary = run_queries(
        index,
        &dataset.queries,
        Some(&dataset.ground_truth),
        options,
    )?;
    validate_disk_cached_network(&summary)?;
    Ok(summary)
}

fn write_recall_row(
    writer: &mut impl Write,
    config: &ResolvedConfig,
    phase: &str,
    mode: &str,
    routing_page_overfetch: usize,
    max_candidates: usize,
    summary: &QuerySummary,
) -> io::Result<()> {
    writeln!(
        writer,
        "{PRODUCTION_BENCH_SCHEMA_VERSION},{},{},{},{},{},{},{phase},{mode},{routing_page_overfetch},{max_candidates},{:.3},{},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3}",
        config.global_scan_codec,
        config.global_turboquant_bits,
        config.global_turboquant_qjl_bits,
        config.global_turboquant_shards,
        config.cache_execution,
        summary.execution_engine(),
        summary.recall(),
        summary.count(),
        sample_mean(&summary.latencies_ms),
        sample_stddev(&summary.latencies_ms),
        percentile(&summary.latencies_ms, 0.50),
        percentile(&summary.latencies_ms, 0.95),
        percentile(&summary.latencies_ms, 0.99),
        maximum(&summary.latencies_ms),
        summary.average_global_leaf_directory_reads(),
        summary.average_global_leaf_directory_bytes(),
        summary.average_global_leaf_code_pages_read(),
        summary.average_global_leaf_code_bytes(),
        summary.average_global_leaf_pages_read(),
        summary.average_global_leaf_page_bytes(),
        summary.average_global_leaf_waves(),
        summary.average_global_leaf_continuations(),
        summary.average_global_leaf_exact_scores(),
        summary.average_backing_reads(),
        summary.average_backing_bytes_read(),
        summary.average_bytes(),
        summary.average_requests(),
        summary.dollars_per_million_queries()
    )
}

fn write_cold_warm_csv(
    config: &ResolvedConfig,
    dataset: &Dataset,
    index: &BorsukIndex,
    preload_complete: bool,
) -> BenchResult<()> {
    let path = config.output_dir.join("bench_cache_states.csv");
    let mut writer = csv_writer(&path)?;
    writeln!(writer, "{CACHE_STATE_HEADER}")?;
    let options = serving_options(config);
    match config.cache_profile {
        BenchmarkCacheProfile::Uncached => {
            let summary =
                run_uncached_queries(config, dataset, index, options, config.uncached_queries)?;
            write_cache_state_row(&mut writer, config, "uncached", &summary)?;
            writer.flush()?;
            eprintln!("wrote {} rows=1", path.display());
            return Ok(());
        }
        BenchmarkCacheProfile::DiskCached => {
            let summary = run_disk_cached_queries(config, dataset, index, options)?;
            write_cache_state_row(&mut writer, config, "disk_cached", &summary)?;
            writer.flush()?;
            eprintln!("wrote {} rows=1", path.display());
            return Ok(());
        }
        BenchmarkCacheProfile::MixedCoverage => {
            return Err(invalid_input(
                "mixed cache coverage must not be written as a homogeneous cache-state row",
            )
            .into());
        }
        BenchmarkCacheProfile::All => {}
    }
    let memory_preloaded = uses_memory_preloaded_phase(
        config.preload_serving,
        config.cache_execution,
        preload_complete,
    );
    let bounded_decoded = uses_bounded_decoded_cache_phases(
        memory_preloaded,
        options.mode.leaf_mode(),
        effective_segment_cache_budget(config),
    );
    let (first, cached) = if memory_preloaded {
        (
            run_queries(
                index,
                &dataset.queries[..1],
                Some(&dataset.ground_truth[..1]),
                options.clone(),
            )?,
            run_queries(
                index,
                &dataset.queries,
                Some(&dataset.ground_truth),
                options.clone(),
            )?,
        )
    } else if bounded_decoded {
        reset_cache(&config.cache_dir)?;
        (
            run_queries(
                index,
                &dataset.queries,
                Some(&dataset.ground_truth),
                options.clone(),
            )?,
            run_queries(
                index,
                &dataset.queries,
                Some(&dataset.ground_truth),
                options.clone(),
            )?,
        )
    } else {
        // Every uncached trial starts with a fresh DATA cache while retaining
        // the routing, IVF graph, and sidecar row-offset metadata prepared at
        // open. This matches a loaded serving process whose requested cell data
        // is absent locally, without charging initialization to the query.
        let mut uncached = QuerySummary::default();
        for query_index in 0..config.uncached_queries {
            reset_cache(&config.cache_dir)?;
            uncached.absorb(run_queries(
                index,
                &dataset.queries[query_index..query_index + 1],
                Some(&dataset.ground_truth[query_index..query_index + 1]),
                options.clone(),
            )?);
        }

        // Prime exactly the query set that will be measured. This pass is not a
        // result row: its only job is to populate the local disk cache. The
        // subsequent pass must report no storage bytes or underlying GETs.
        reset_cache(&config.cache_dir)?;
        let _ = run_queries(index, &dataset.queries, None, options.clone())?;
        let cached = run_queries(
            index,
            &dataset.queries,
            Some(&dataset.ground_truth),
            options.clone(),
        )?;
        (uncached, cached)
    };
    write_cache_state_row(
        &mut writer,
        config,
        if memory_preloaded {
            "memory_preloaded_first"
        } else if bounded_decoded && config.preload_serving {
            "partial_preload_mixed_first"
        } else if bounded_decoded {
            "bounded_decoded_cache_fill"
        } else {
            "uncached"
        },
        &first,
    )?;

    if !memory_preloaded {
        validate_disk_cached_network(&cached)?;
    }
    write_cache_state_row(
        &mut writer,
        config,
        if memory_preloaded {
            "memory_preloaded"
        } else if bounded_decoded && config.preload_serving {
            "partial_preload_mixed_steady"
        } else if bounded_decoded {
            "bounded_decoded_cache_steady"
        } else {
            "disk_cached"
        },
        &cached,
    )?;
    writer.flush()?;
    eprintln!("wrote {} rows=2", path.display());
    Ok(())
}

fn validate_disk_cached_network(summary: &QuerySummary) -> BenchResult<()> {
    // The benchmark's `bytes_read` is the sum of the query-scoped disk and
    // backing counters. The storage request counter is the authoritative
    // boundary for backing object-store traffic.
    if summary.billable_requests != 0 {
        return Err(invalid_input(&format!(
            "disk-cached phase performed network I/O: network_gets={}",
            summary.billable_requests
        ))
        .into());
    }
    Ok(())
}

fn write_cache_state_row(
    writer: &mut impl Write,
    config: &ResolvedConfig,
    phase: &str,
    summary: &QuerySummary,
) -> io::Result<()> {
    writeln!(
        writer,
        "{PRODUCTION_BENCH_SCHEMA_VERSION},{},{},{},{},{},{},{phase},{},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3}",
        config.global_scan_codec,
        config.global_turboquant_bits,
        config.global_turboquant_qjl_bits,
        config.global_turboquant_shards,
        config.cache_execution,
        summary.execution_engine(),
        summary.count(),
        summary.recall(),
        sample_mean(&summary.latencies_ms),
        sample_stddev(&summary.latencies_ms),
        percentile(&summary.latencies_ms, 0.50),
        percentile(&summary.latencies_ms, 0.95),
        percentile(&summary.latencies_ms, 0.99),
        maximum(&summary.latencies_ms),
        summary.average_global_leaf_directory_reads(),
        summary.average_global_leaf_directory_bytes(),
        summary.average_global_leaf_code_pages_read(),
        summary.average_global_leaf_code_bytes(),
        summary.average_global_leaf_pages_read(),
        summary.average_global_leaf_page_bytes(),
        summary.average_global_leaf_waves(),
        summary.average_global_leaf_continuations(),
        summary.average_global_leaf_exact_scores(),
        summary.average_backing_reads(),
        summary.average_backing_bytes_read(),
        summary.average_bytes(),
        summary.average_cache_misses(),
        summary.average_requests(),
        summary.dollars_per_million_queries()
    )
}

fn validate_phase_selection(recall_only: bool, skip_recall: bool) -> BenchResult<()> {
    if recall_only && skip_recall {
        return Err(invalid_input(
            "BORSUK_BENCH_RECALL_ONLY and BORSUK_BENCH_SKIP_RECALL cannot both be enabled",
        )
        .into());
    }
    Ok(())
}

fn validate_build_only(build_only: bool, build_index: bool) -> BenchResult<()> {
    if build_only && !build_index {
        return Err(
            invalid_input("BORSUK_BENCH_BUILD_ONLY requires BORSUK_BENCH_BUILD_INDEX=1").into(),
        );
    }
    Ok(())
}

fn validate_insert_only(insert_only: bool, build_only: bool, read_only: bool) -> BenchResult<()> {
    if insert_only && (build_only || read_only) {
        return Err(invalid_input(
            "BORSUK_BENCH_INSERT_ONLY cannot be combined with build-only or read-only",
        )
        .into());
    }
    Ok(())
}

fn write_concurrency_csv(
    config: &ResolvedConfig,
    dataset: &Dataset,
    index: &Arc<BorsukIndex>,
) -> BenchResult<()> {
    let path = config.output_dir.join("bench_concurrency.csv");
    let mut writer = csv_writer(&path)?;
    writeln!(writer, "{CONCURRENCY_HEADER}")?;
    let samples_path = config.output_dir.join("bench_concurrency_samples.csv");
    let mut samples_writer = csv_writer(&samples_path)?;
    writeln!(samples_writer, "{CONCURRENCY_SAMPLE_HEADER}")?;
    for &workers in &config.concurrency {
        let query_indices = prepare_concurrency_cache_state(config, dataset, index)?;
        let ground_truth = Arc::new(dataset.ground_truth.clone());
        let query_source_indices = Arc::clone(&dataset.query_source_indices);
        let hot_count = match config.cache_profile {
            BenchmarkCacheProfile::All | BenchmarkCacheProfile::DiskCached => dataset.queries.len(),
            BenchmarkCacheProfile::Uncached => 0,
            BenchmarkCacheProfile::MixedCoverage => {
                dataset.queries.len() * config.cache_coverage_percent / 100
            }
        };
        let started = Instant::now();
        let mut handles = Vec::with_capacity(workers);
        for worker in 0..workers {
            let worker_index = Arc::clone(index);
            let queries = Arc::clone(&dataset.queries);
            let query_indices = Arc::clone(&query_indices);
            let ground_truth = Arc::clone(&ground_truth);
            let query_source_indices = Arc::clone(&query_source_indices);
            let options = serving_options(config);
            handles.push(thread::spawn(
                move || -> Result<Vec<ConcurrencyMeasurement>, String> {
                    let mut measurements = Vec::new();
                    for position in (worker..query_indices.len()).step_by(workers) {
                        let query_index = query_indices[position];
                        let query_started = Instant::now();
                        let report = worker_index
                            .search_with_report(&queries[query_index], options.clone())
                            .map_err(|error| error.to_string())?;
                        let recall = if is_zero_norm(&queries[query_index]) {
                            f32::NAN
                        } else {
                            let ids = report
                                .hits
                                .iter()
                                .map(|hit| hit.id.to_utf8_string())
                                .collect::<borsuk::Result<Vec<_>>>()
                                .map_err(|error| error.to_string())?;
                            recall_at_k(&ground_truth[query_index], &ids, RECALL_K)
                                .map_err(|error| error.to_string())?
                        };
                        measurements.push(ConcurrencyMeasurement {
                            position,
                            query_source_index: query_source_indices[query_index],
                            target_hot_set_member: query_index < hot_count,
                            latency_ms: elapsed_ms(query_started),
                            recall,
                            bytes_read: report.bytes_read,
                            decoded_cache_hits: report.decoded_cache_hits,
                            disk_cache_reads: report.disk_cache_reads,
                            backing_reads: report.backing_reads,
                            decoded_cache_bytes_read: report.decoded_cache_bytes_read,
                            disk_cache_bytes_read: report.disk_cache_bytes_read,
                            backing_bytes_read: report.backing_bytes_read,
                            network_gets: report
                                .requests
                                .gets
                                .saturating_add(report.requests.heads),
                            global_leaf_directory_reads: report.global_leaf_directory_reads,
                            global_leaf_directory_bytes: report.global_leaf_directory_bytes,
                            global_leaf_code_pages_read: report.global_leaf_code_pages_read,
                            global_leaf_code_bytes: report.global_leaf_code_bytes,
                            global_leaf_pages_read: report.global_leaf_pages_read,
                            global_leaf_page_bytes: report.global_leaf_page_bytes,
                            global_leaf_waves: report.global_leaf_waves,
                            global_leaf_continuations: report.global_leaf_continuations,
                            global_leaf_exact_scores: report.global_leaf_exact_scores,
                            execution_engine: execution_engine_label(&report).to_string(),
                            collection_resident_bytes: report.collection_resident_bytes,
                            retained_bytes: report.retained_bytes,
                            retained_capacity_bytes: report.retained_capacity_bytes,
                            retained_peak_bytes: report.retained_peak_bytes,
                            transient_bytes: report.transient_bytes,
                            transient_capacity_bytes: report.transient_capacity_bytes,
                            transient_peak_bytes: report.transient_peak_bytes,
                        });
                    }
                    Ok(measurements)
                },
            ));
        }

        let mut measurements = Vec::with_capacity(query_indices.len());
        for handle in handles {
            measurements.extend(
                handle
                    .join()
                    .map_err(|_| invalid_input("concurrency benchmark worker panicked"))?
                    .map_err(|error| {
                        invalid_input(&format!("concurrency worker failed: {error}"))
                    })?,
            );
        }
        measurements.sort_by_key(|measurement| measurement.position);
        let latencies_ms = measurements
            .iter()
            .map(|measurement| measurement.latency_ms)
            .collect::<Vec<_>>();
        let mut bytes_read = 0_u128;
        let mut global_leaf_directory_reads = 0_u128;
        let mut global_leaf_directory_bytes = 0_u128;
        let mut global_leaf_code_pages_read = 0_u128;
        let mut global_leaf_code_bytes = 0_u128;
        let mut global_leaf_pages_read = 0_u128;
        let mut global_leaf_page_bytes = 0_u128;
        let mut global_leaf_waves = 0_u128;
        let mut global_leaf_continuations = 0_u128;
        let mut global_leaf_exact_scores = 0_u128;
        let mut backing_reads = 0_u128;
        let mut backing_bytes_read = 0_u128;
        let mut execution_engines = BTreeSet::new();
        for measurement in &measurements {
            bytes_read += u128::from(measurement.bytes_read);
            global_leaf_directory_reads += measurement.global_leaf_directory_reads as u128;
            global_leaf_directory_bytes += u128::from(measurement.global_leaf_directory_bytes);
            global_leaf_code_pages_read += measurement.global_leaf_code_pages_read as u128;
            global_leaf_code_bytes += u128::from(measurement.global_leaf_code_bytes);
            global_leaf_pages_read += measurement.global_leaf_pages_read as u128;
            global_leaf_page_bytes += u128::from(measurement.global_leaf_page_bytes);
            global_leaf_waves += measurement.global_leaf_waves as u128;
            global_leaf_continuations += measurement.global_leaf_continuations as u128;
            global_leaf_exact_scores += measurement.global_leaf_exact_scores as u128;
            backing_reads += u128::from(measurement.backing_reads);
            backing_bytes_read += u128::from(measurement.backing_bytes_read);
            execution_engines.insert(measurement.execution_engine.clone());
        }
        let wall_seconds = started.elapsed().as_secs_f64();
        let total_queries = latencies_ms.len();
        let qps = if wall_seconds == 0.0 {
            total_queries as f64
        } else {
            total_queries as f64 / wall_seconds
        };
        for (sample_index, measurement) in measurements.iter().enumerate() {
            writeln!(
                samples_writer,
                "{PRODUCTION_BENCH_SCHEMA_VERSION},{},{},{},{},{workers},{sample_index},{},{},{:.6},{:.6},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
                config.global_scan_codec,
                config.cache_execution,
                config.cache_profile.as_str(),
                config.cache_coverage_percent,
                measurement.query_source_index,
                u8::from(measurement.target_hot_set_member),
                measurement.latency_ms,
                measurement.recall,
                measurement.execution_engine,
                measurement.global_leaf_directory_reads,
                measurement.global_leaf_directory_bytes,
                measurement.global_leaf_code_pages_read,
                measurement.global_leaf_code_bytes,
                measurement.global_leaf_pages_read,
                measurement.global_leaf_page_bytes,
                measurement.global_leaf_waves,
                measurement.global_leaf_continuations,
                measurement.global_leaf_exact_scores,
                measurement.bytes_read,
                measurement.decoded_cache_hits,
                measurement.disk_cache_reads,
                measurement.backing_reads,
                measurement.decoded_cache_bytes_read,
                measurement.disk_cache_bytes_read,
                measurement.backing_bytes_read,
                measurement.network_gets,
                config.ram_budget_bytes.unwrap_or(0),
                measurement.collection_resident_bytes,
                measurement.retained_bytes,
                measurement.retained_capacity_bytes,
                measurement.retained_peak_bytes,
                measurement.transient_bytes,
                measurement.transient_capacity_bytes,
                measurement.transient_peak_bytes,
            )?;
        }
        writeln!(
            writer,
            "{PRODUCTION_BENCH_SCHEMA_VERSION},{},{},{},{},{},{},{},{},{workers},{total_queries},{qps:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3}",
            config.global_scan_codec,
            config.global_turboquant_bits,
            config.global_turboquant_qjl_bits,
            config.global_turboquant_shards,
            config.cache_execution,
            config.cache_profile.as_str(),
            config.cache_coverage_percent,
            if execution_engines.len() == 1 {
                execution_engines.first().map_or("unknown", String::as_str)
            } else if execution_engines.is_empty() {
                "unknown"
            } else {
                "mixed"
            },
            sample_mean(&latencies_ms),
            sample_stddev(&latencies_ms),
            percentile(&latencies_ms, 0.50),
            percentile(&latencies_ms, 0.95),
            percentile(&latencies_ms, 0.99),
            maximum(&latencies_ms),
            mean(global_leaf_directory_reads as f64, total_queries),
            mean(global_leaf_directory_bytes as f64, total_queries),
            mean(global_leaf_code_pages_read as f64, total_queries),
            mean(global_leaf_code_bytes as f64, total_queries),
            mean(global_leaf_pages_read as f64, total_queries),
            mean(global_leaf_page_bytes as f64, total_queries),
            mean(global_leaf_waves as f64, total_queries),
            mean(global_leaf_continuations as f64, total_queries),
            mean(global_leaf_exact_scores as f64, total_queries),
            mean(backing_reads as f64, total_queries),
            mean(backing_bytes_read as f64, total_queries),
            mean(bytes_read as f64, total_queries)
        )?;
    }
    writer.flush()?;
    samples_writer.flush()?;
    eprintln!("wrote {} rows={}", path.display(), config.concurrency.len());
    Ok(())
}

fn prepare_concurrency_cache_state(
    config: &ResolvedConfig,
    dataset: &Dataset,
    index: &BorsukIndex,
) -> BenchResult<Arc<Vec<usize>>> {
    reset_cache(&config.cache_dir)?;
    let all = || (0..dataset.queries.len()).collect::<Vec<_>>();
    match config.cache_profile {
        BenchmarkCacheProfile::All | BenchmarkCacheProfile::DiskCached => {
            let indices = all();
            for &query_index in &indices {
                let _ = index
                    .search_with_report(&dataset.queries[query_index], serving_options(config))?;
            }
            Ok(Arc::new(indices))
        }
        BenchmarkCacheProfile::Uncached => {
            // This is a concurrent cold-start workload: all workers begin with
            // an empty local data cache. Shared reads may populate it while the
            // wave is running, which is the production behavior being tested.
            Ok(Arc::new(all()))
        }
        BenchmarkCacheProfile::MixedCoverage => {
            let total = dataset.queries.len();
            if total < 2 {
                return Err(invalid_input(
                    "mixed concurrency measurement needs at least two distinct queries",
                )
                .into());
            }
            let hot_count = total * config.cache_coverage_percent / 100;
            for query_index in 0..hot_count {
                let _ = index
                    .search_with_report(&dataset.queries[query_index], serving_options(config))?;
            }
            Ok(Arc::new(mixed_concurrency_query_indices(
                total,
                config.cache_coverage_percent,
            )))
        }
    }
}

fn mixed_concurrency_query_indices(total: usize, target_hot_percent: usize) -> Vec<usize> {
    let hot_count = total * target_hot_percent / 100;
    let mut hot_cursor = 0_usize;
    let mut cold_cursor = 0_usize;
    let mut indices = Vec::with_capacity(total);
    for position in 0..total {
        if is_hot_workload_position(position, target_hot_percent) {
            indices.push(hot_cursor);
            hot_cursor += 1;
        } else {
            indices.push(hot_count + cold_cursor);
            cold_cursor += 1;
        }
    }
    debug_assert_eq!(hot_cursor, hot_count);
    debug_assert_eq!(cold_cursor, total - hot_count);
    indices
}

fn write_cache_coverage_csv(config: &ResolvedConfig, dataset: &Dataset) -> BenchResult<()> {
    if config.cache_profile != BenchmarkCacheProfile::MixedCoverage
        && config.cache_profile != BenchmarkCacheProfile::All
    {
        return Ok(());
    }
    if !cache_coverage_enabled(dataset.queries.len()) {
        return Ok(());
    }
    let path = config.output_dir.join("bench_cache_coverage.csv");
    let mut writer = csv_writer(&path)?;
    writeln!(writer, "{CACHE_COVERAGE_HEADER}")?;
    let cohort_queries = cache_coverage_cohort_size(dataset.queries.len());
    let hot_indices = (0..cohort_queries).collect::<Vec<_>>();
    let cold_indices = (cohort_queries..cohort_queries * 2).collect::<Vec<_>>();
    let options = serving_options(config);

    let coverage_points = if config.cache_profile == BenchmarkCacheProfile::MixedCoverage {
        vec![config.cache_coverage_percent]
    } else {
        vec![100_usize, 75, 50, 25, 0]
    };
    for target_hot_percent in coverage_points.iter().copied() {
        let hot_per_repetition = cohort_queries * target_hot_percent / 100;
        let cold_per_repetition = cohort_queries - hot_per_repetition;
        for repetition in 0..CACHE_COVERAGE_REPETITIONS {
            // Each repetition starts from the same explicit state: metadata is
            // resident, the data cache is empty, and then the complete hot
            // cohort is primed. Cold queries are unique inside a repetition,
            // so a preceding cold request cannot silently turn a later one hot.
            reset_cache(&config.cache_dir)?;
            let index = open_serving_index(config)?;
            for &query_index in &hot_indices {
                let _ = index.search_with_report(&dataset.queries[query_index], options.clone())?;
            }

            let mut hot_cursor = 0_usize;
            let mut cold_cursor = 0_usize;
            for position in 0..cohort_queries {
                let is_hot = is_hot_workload_position(position, target_hot_percent);
                let query_index = if is_hot {
                    let selected = hot_indices[rotated_workload_index(
                        hot_indices.len(),
                        repetition,
                        hot_per_repetition,
                        hot_cursor,
                    )];
                    hot_cursor += 1;
                    selected
                } else {
                    let selected = cold_indices[rotated_workload_index(
                        cold_indices.len(),
                        repetition,
                        cold_per_repetition,
                        cold_cursor,
                    )];
                    cold_cursor += 1;
                    selected
                };
                let started = Instant::now();
                let report =
                    index.search_with_report(&dataset.queries[query_index], options.clone())?;
                let latency_ms = elapsed_ms(started);
                let ids = report
                    .hits
                    .iter()
                    .map(|hit| hit.id.to_utf8_string())
                    .collect::<borsuk::Result<Vec<_>>>()?;
                let recall = if is_zero_norm(&dataset.queries[query_index]) {
                    f64::NAN
                } else {
                    f64::from(recall_at_k(
                        &dataset.ground_truth[query_index],
                        &ids,
                        RECALL_K,
                    )?)
                };
                let (decoded_fraction, disk_fraction, backing_fraction) =
                    cache_access_fractions(&report);
                writeln!(
                    writer,
                    "{PRODUCTION_BENCH_SCHEMA_VERSION},{},{},{:.2},{repetition},{position},{},{},{},{},{recall:.3},{latency_ms:.3},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{decoded_fraction:.3},{disk_fraction:.3},{backing_fraction:.3},{},{}",
                    config.global_scan_codec,
                    config.cache_execution,
                    target_hot_percent as f64 / 100.0,
                    if is_hot { "hot" } else { "outside_hot_set" },
                    query_index,
                    execution_engine_label(&report),
                    observed_cache_tier(&report),
                    report.segments_searched,
                    report.global_leaf_directory_reads,
                    report.global_leaf_directory_bytes,
                    report.global_leaf_code_pages_read,
                    report.global_leaf_code_bytes,
                    report.global_leaf_pages_read,
                    report.global_leaf_page_bytes,
                    report.global_leaf_waves,
                    report.global_leaf_continuations,
                    report.global_leaf_exact_scores,
                    report.decoded_cache_hits,
                    report.disk_cache_reads,
                    report.backing_reads,
                    report.decoded_cache_bytes_read,
                    report.disk_cache_bytes_read,
                    report.backing_bytes_read,
                    report.bytes_read,
                    report.requests.gets.saturating_add(report.requests.heads),
                )?;
            }
            writer.flush()?;
        }
    }
    eprintln!(
        "wrote {} rows={}",
        path.display(),
        cohort_queries * CACHE_COVERAGE_REPETITIONS * coverage_points.len()
    );
    Ok(())
}

fn cache_coverage_enabled(query_count: usize) -> bool {
    // The storage cache is part of every serving configuration, including a
    // scan-only index with no decoded-segment or graph cache. Always emit the
    // hot/outside-hot workload whenever there are enough distinct queries.
    query_count >= 2
}

fn cache_coverage_cohort_size(query_count: usize) -> usize {
    let available_per_class = (query_count / 2).min(CACHE_COVERAGE_COHORT_QUERIES);
    if available_per_class < 4 {
        available_per_class
    } else {
        available_per_class - available_per_class % 4
    }
}

fn rotated_workload_index(
    cohort_len: usize,
    repetition: usize,
    selected_per_repetition: usize,
    cursor: usize,
) -> usize {
    debug_assert!(cohort_len > 0);
    (repetition * selected_per_repetition + cursor) % cohort_len
}

fn cache_access_fractions(report: &SearchReport) -> (f64, f64, f64) {
    normalized_cache_access_fractions(
        report.decoded_cache_bytes_read,
        report.disk_cache_bytes_read,
        report.backing_bytes_read,
        report.requests.gets.saturating_add(report.requests.heads),
        report.bytes_read,
    )
}

fn normalized_cache_access_fractions(
    decoded_bytes: u64,
    disk_bytes: u64,
    backing_bytes: u64,
    network_requests: u64,
    bytes_read: u64,
) -> (f64, f64, f64) {
    let decoded = decoded_bytes as f64;
    let disk = disk_bytes as f64;
    let backing = backing_bytes as f64;
    let total = decoded + disk + backing;
    if total > 0.0 {
        return (decoded / total, disk / total, backing / total);
    }
    if network_requests > 0 {
        (0.0, 0.0, 1.0)
    } else if bytes_read > 0 {
        (0.0, 1.0, 0.0)
    } else {
        (1.0, 0.0, 0.0)
    }
}

fn is_hot_workload_position(position: usize, target_hot_percent: usize) -> bool {
    let hot_before = position * target_hot_percent / 100;
    let hot_after = (position + 1) * target_hot_percent / 100;
    hot_after > hot_before
}

fn observed_cache_tier(report: &SearchReport) -> &'static str {
    let (decoded, disk, backing) = cache_access_fractions(report);
    let tiers = usize::from(decoded > 0.0) + usize::from(disk > 0.0) + usize::from(backing > 0.0);
    if tiers > 1 {
        "mixed"
    } else if backing > 0.0 {
        "backing_storage"
    } else if disk > 0.0 {
        "disk_cache"
    } else {
        "decoded_memory"
    }
}

fn write_write_costs_csv(
    config: &ResolvedConfig,
    dataset: &Dataset,
    index: &mut BorsukIndex,
) -> BenchResult<()> {
    let write_ops = write_operation_count(dataset.train_count, config.write_ops)?;
    let update_ops = percentage_operation_count(write_ops, config.update_percent)?;
    let delete_ops = percentage_operation_count(write_ops, config.delete_percent)?;
    let mutation_queries = &dataset.queries[..dataset.queries.len().min(MUTATION_QUERY_SAMPLES)];
    let mut query_stages = vec![(
        "baseline",
        run_queries(index, mutation_queries, None, serving_options(config))?,
    )];
    let stats_before_insert = index.stats();
    let mut rows = Vec::with_capacity(5);
    let insert = measure_inserts(config, dataset, index, write_ops)?;
    let (searchable_samples, searchable_hits) =
        verify_insert_visibility(dataset, index, write_ops)?;
    let searchable_fraction = mean(searchable_hits as f64, searchable_samples);
    let insert_wall_ms = insert.row.wall_ms;
    let foreground_bytes_written = insert.row.bytes_written;
    let first_batch_publish_ms = insert.first_batch_publish_ms;
    rows.push(insert.row);
    query_stages.push((
        "after-insert-searchable",
        run_queries(index, mutation_queries, None, serving_options(config))?,
    ));

    // WAL publication makes rows durable/searchable. Flushing materializes
    // only the bounded tail into immutable segment-local indexes; it does not
    // rebuild the corpus-wide base.
    let delta_flush_started = Instant::now();
    index.flush()?;
    let delta_flush_ms = elapsed_ms(delta_flush_started);
    let stats_after_delta = index.stats();
    let indexed_delta_bytes = stats_after_delta
        .segment_bytes
        .saturating_sub(stats_before_insert.segment_bytes)
        .saturating_add(
            stats_after_delta
                .vector_bytes
                .saturating_sub(stats_before_insert.vector_bytes),
        )
        .saturating_add(
            stats_after_delta
                .graph_bytes
                .saturating_sub(stats_before_insert.graph_bytes),
        );
    query_stages.push((
        "after-fully-indexed-delta",
        run_queries(index, mutation_queries, None, serving_options(config))?,
    ));

    // Corpus-wide consolidation is a distinct maintenance metric. It must not
    // be mislabeled as time-to-indexed for the newly inserted rows.
    let consolidation_started = Instant::now();
    index.finish_bulk_load()?;
    let consolidation_ms = elapsed_ms(consolidation_started);
    let consolidated_global_bytes = index.stats().global_scan_bytes;
    query_stages.push((
        "after-global-consolidation",
        run_queries(index, mutation_queries, None, serving_options(config))?,
    ));
    let upsert = measure_upserts(config, dataset, index, update_ops)?;
    let (upsert_samples, upsert_correct) = verify_upsert_values(index, &upsert.expected_records)?;
    rows.push(upsert.row);
    query_stages.push((
        "after-upsert",
        run_queries(index, mutation_queries, None, serving_options(config))?,
    ));
    rows.push(measure_deletes(index, delete_ops, config.write_batch_size)?);
    let (delete_samples, delete_absent) = verify_delete_absence(index, delete_ops)?;
    query_stages.push((
        "after-delete",
        run_queries(index, mutation_queries, None, serving_options(config))?,
    ));

    let requests_before = index.request_counts();
    let compact_started = Instant::now();
    let compact = index.compact(CompactionOptions::default())?;
    let compact_wall_ms = elapsed_ms(compact_started);
    let compact_requests = index.request_counts().delta(&requests_before);
    rows.push(WriteRow {
        op: "compact",
        ops: 1,
        wall_ms: compact_wall_ms,
        latencies_ms: vec![compact_wall_ms],
        samples: vec![WriteSample {
            op: "compact",
            batch_index: 0,
            batch_records: compact.records_rewritten,
            batch_latency_ms: compact_wall_ms,
            requests: compact_requests,
        }],
        requests: compact_requests,
        bytes_read: compact.bytes_read,
        bytes_written: compact.bytes_written,
    });
    let (_, compact_delete_absent) = verify_delete_absence(index, delete_ops)?;
    query_stages.push((
        "after-compact",
        run_queries(index, mutation_queries, None, serving_options(config))?,
    ));

    let requests_before = index.request_counts();
    let purge_started = Instant::now();
    let purge = index.purge_with_report()?;
    let purge_wall_ms = elapsed_ms(purge_started);
    let purge_requests = index.request_counts().delta(&requests_before);
    // PurgeReport exposes request counts and row/segment counts, but no byte
    // counters. The closest honest representation for this CSV is zero bytes.
    rows.push(WriteRow {
        op: "purge",
        ops: 1,
        wall_ms: purge_wall_ms,
        latencies_ms: vec![purge_wall_ms],
        samples: vec![WriteSample {
            op: "purge",
            batch_index: 0,
            batch_records: purge.records_purged,
            batch_latency_ms: purge_wall_ms,
            requests: purge_requests,
        }],
        requests: purge_requests,
        bytes_read: 0,
        bytes_written: 0,
    });
    let (_, purge_delete_absent) = verify_delete_absence(index, delete_ops)?;
    query_stages.push((
        "after-purge",
        run_queries(index, mutation_queries, None, serving_options(config))?,
    ));

    write_lifecycle_csv(
        config,
        dataset,
        write_ops,
        insert_wall_ms,
        first_batch_publish_ms,
        searchable_samples,
        searchable_fraction,
        upsert_samples,
        mean(upsert_correct as f64, upsert_samples),
        delete_samples,
        mean(delete_absent as f64, delete_samples),
        mean(compact_delete_absent as f64, delete_samples),
        mean(purge_delete_absent as f64, delete_samples),
        delta_flush_ms,
        foreground_bytes_written,
        indexed_delta_bytes,
        consolidation_ms,
        consolidated_global_bytes,
    )?;

    write_cost_artifacts(config, &rows)?;
    write_mutation_query_artifacts(config, &query_stages)?;
    Ok(())
}

fn write_cost_artifacts(config: &ResolvedConfig, rows: &[WriteRow]) -> BenchResult<()> {
    let path = config.output_dir.join("bench_write_costs.csv");
    let mut writer = csv_writer(&path)?;
    writeln!(writer, "{WRITE_COST_HEADER}")?;
    for row in rows {
        let ops_per_second = if row.wall_ms == 0.0 {
            row.ops as f64
        } else {
            row.ops as f64 / (row.wall_ms / 1_000.0)
        };
        writeln!(
            writer,
            "{},{},{},{},{:.3},{ops_per_second:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.6},{},{},{},{},{},{},{}",
            row.op,
            config.write_batch_size,
            row.ops,
            row.samples.len(),
            row.wall_ms,
            sample_mean(&row.latencies_ms),
            sample_stddev(&row.latencies_ms),
            percentile(&row.latencies_ms, 0.50),
            percentile(&row.latencies_ms, 0.95),
            percentile(&row.latencies_ms, 0.99),
            maximum(&row.latencies_ms),
            mean_amortized_ms(row),
            row.requests.gets,
            row.requests.puts,
            row.requests.deletes,
            row.requests.heads,
            row.requests.lists,
            row.bytes_read,
            row.bytes_written
        )?;
    }
    writer.flush()?;
    let sample_path = config.output_dir.join("bench_write_samples.csv");
    let mut sample_writer = csv_writer(&sample_path)?;
    writeln!(sample_writer, "{WRITE_SAMPLE_HEADER}")?;
    for sample in rows.iter().flat_map(|row| &row.samples) {
        writeln!(
            sample_writer,
            "{},{},{},{:.3},{:.6},{},{},{},{},{}",
            sample.op,
            sample.batch_index,
            sample.batch_records,
            sample.batch_latency_ms,
            sample.batch_latency_ms / sample.batch_records.max(1) as f64,
            sample.requests.gets,
            sample.requests.puts,
            sample.requests.deletes,
            sample.requests.heads,
            sample.requests.lists,
        )?;
    }
    sample_writer.flush()?;
    eprintln!("wrote {} rows={}", path.display(), rows.len());
    eprintln!(
        "wrote {} rows={}",
        sample_path.display(),
        rows.iter().map(|row| row.samples.len()).sum::<usize>()
    );
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn write_lifecycle_csv(
    config: &ResolvedConfig,
    dataset: &Dataset,
    inserted_vectors: usize,
    insert_wall_ms: f64,
    first_batch_publish_ms: f64,
    searchable_samples: usize,
    searchable_fraction: f64,
    upsert_samples: usize,
    upsert_correct_fraction: f64,
    delete_samples: usize,
    delete_absent_fraction: f64,
    compact_delete_absent_fraction: f64,
    purge_delete_absent_fraction: f64,
    delta_flush_ms: f64,
    wal_publish_bytes: u64,
    indexed_delta_bytes: u64,
    consolidation_ms: f64,
    consolidated_global_bytes: u64,
) -> BenchResult<()> {
    let logical_vector_bytes = u64::try_from(inserted_vectors)?
        .saturating_mul(u64::try_from(dataset.meta.dim)?)
        .saturating_mul(u64::try_from(std::mem::size_of::<f32>())?);
    let total_indexing_bytes = wal_publish_bytes.saturating_add(indexed_delta_bytes);
    let write_amplification = if logical_vector_bytes == 0 {
        0.0
    } else {
        total_indexing_bytes as f64 / logical_vector_bytes as f64
    };
    let consolidation_amplification = if logical_vector_bytes == 0 {
        0.0
    } else {
        consolidated_global_bytes as f64 / logical_vector_bytes as f64
    };
    let insert_vectors_per_s = if insert_wall_ms == 0.0 {
        inserted_vectors as f64
    } else {
        inserted_vectors as f64 / (insert_wall_ms / 1_000.0)
    };
    let time_to_fully_indexed_ms = insert_wall_ms + delta_flush_ms;
    let time_to_consolidated_ms = time_to_fully_indexed_ms + consolidation_ms;
    let path = config.output_dir.join("bench_lifecycle.csv");
    let mut writer = csv_writer(&path)?;
    writeln!(writer, "{LIFECYCLE_HEADER}")?;
    writeln!(
        writer,
        "{},{inserted_vectors},{logical_vector_bytes},{insert_wall_ms:.3},{insert_vectors_per_s:.3},{first_batch_publish_ms:.3},{first_batch_publish_ms:.3},{searchable_samples},{searchable_fraction:.6},{upsert_samples},{upsert_correct_fraction:.6},{delete_samples},{delete_absent_fraction:.6},{compact_delete_absent_fraction:.6},{purge_delete_absent_fraction:.6},{delta_flush_ms:.3},{time_to_fully_indexed_ms:.3},{wal_publish_bytes},{indexed_delta_bytes},{total_indexing_bytes},{write_amplification:.6},true,{consolidation_ms:.3},{time_to_consolidated_ms:.3},{consolidated_global_bytes},{consolidation_amplification:.6}",
        config.write_batch_size,
    )?;
    writer.flush()?;
    eprintln!("wrote {} rows=1", path.display());
    Ok(())
}

fn write_mutation_query_artifacts(
    config: &ResolvedConfig,
    stages: &[(&str, QuerySummary)],
) -> BenchResult<()> {
    let summary_path = config.output_dir.join("bench_mutation_queries.csv");
    let mut summary_writer = csv_writer(&summary_path)?;
    writeln!(summary_writer, "{MUTATION_QUERY_HEADER}")?;
    for (stage, summary) in stages {
        writeln!(
            summary_writer,
            "{stage},{},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3}",
            summary.count(),
            sample_mean(&summary.latencies_ms),
            sample_stddev(&summary.latencies_ms),
            percentile(&summary.latencies_ms, 0.50),
            percentile(&summary.latencies_ms, 0.95),
            percentile(&summary.latencies_ms, 0.99),
            maximum(&summary.latencies_ms),
            summary.average_bytes(),
            summary.average_requests(),
        )?;
    }
    summary_writer.flush()?;

    let sample_path = config.output_dir.join("bench_mutation_query_samples.csv");
    let mut sample_writer = csv_writer(&sample_path)?;
    writeln!(sample_writer, "{MUTATION_QUERY_SAMPLE_HEADER}")?;
    for (stage, summary) in stages {
        for (sample_index, sample) in summary.samples.iter().enumerate() {
            writeln!(
                sample_writer,
                "{stage},{sample_index},{:.3},{},{},{}",
                sample.latency_ms, sample.execution_engine, sample.bytes_read, sample.network_gets,
            )?;
        }
    }
    sample_writer.flush()?;
    Ok(())
}

fn mean_amortized_ms(row: &WriteRow) -> f64 {
    mean(
        row.samples
            .iter()
            .map(|sample| sample.batch_latency_ms)
            .sum(),
        row.samples
            .iter()
            .map(|sample| sample.batch_records.max(1))
            .sum(),
    )
}

fn measure_inserts(
    config: &ResolvedConfig,
    dataset: &Dataset,
    index: &mut BorsukIndex,
    count: usize,
) -> BenchResult<InsertMeasurement> {
    let requests_before = index.request_counts();
    let started = Instant::now();
    let mut samples = Vec::new();
    let mut bytes_written = 0_u64;
    stream_dataset_batches(config, dataset, count, |offset, vectors| {
        let batch_records = vectors.len();
        let ids = (offset..offset.saturating_add(vectors.len()))
            .map(|id| format!("bench-insert-{}", dataset.train_count.saturating_add(id)))
            .collect::<Vec<_>>();
        let batch_requests_before = index.request_counts();
        let batch_started = Instant::now();
        let (_, report) = index.add_with_report(vectors, Some(ids))?;
        bytes_written = bytes_written.saturating_add(report.total_bytes_written);
        let batch_latency_ms = elapsed_ms(batch_started);
        samples.push(WriteSample {
            op: "insert",
            batch_index: samples.len(),
            batch_records,
            batch_latency_ms,
            requests: index.request_counts().delta(&batch_requests_before),
        });
        Ok(())
    })?;
    let first_batch_publish_ms = samples
        .first()
        .map_or(0.0, |sample| sample.batch_latency_ms);
    let mut row = write_row_from_samples(
        "insert",
        count,
        elapsed_ms(started),
        samples,
        index.request_counts().delta(&requests_before),
    );
    row.bytes_written = bytes_written;
    Ok(InsertMeasurement {
        row,
        first_batch_publish_ms,
    })
}

fn verify_insert_visibility(
    dataset: &Dataset,
    index: &BorsukIndex,
    count: usize,
) -> BenchResult<(usize, usize)> {
    let samples = count.min(16);
    let ids = (0..samples)
        .map(|sample| {
            let offset = if samples <= 1 {
                0
            } else {
                sample.saturating_mul(count.saturating_sub(1)) / samples.saturating_sub(1)
            };
            format!(
                "bench-insert-{}",
                dataset.train_count.saturating_add(offset)
            )
        })
        .collect::<Vec<_>>();
    let visible = index
        .get_records(&ids)?
        .into_iter()
        .filter(Option::is_some)
        .count();
    Ok((samples, visible))
}

fn measure_upserts(
    config: &ResolvedConfig,
    dataset: &Dataset,
    index: &mut BorsukIndex,
    count: usize,
) -> BenchResult<UpsertMeasurement> {
    // Re-upsert the first `count` train vectors (nudged so it is a real MVCC
    // upsert), streaming from the selected standard source. Zero-norm vectors
    // are accepted like any other.
    let requests_before = index.request_counts();
    let started = Instant::now();
    let mut samples = Vec::new();
    let mut expected_records = Vec::with_capacity(count.min(16));
    stream_dataset_batches(config, dataset, count, |offset, vectors| {
        let batch_records = vectors.len();
        let mut records = Vec::with_capacity(vectors.len());
        for (position, mut vector) in vectors.into_iter().enumerate() {
            vector[0] += 1.0e-4;
            let id = offset.saturating_add(position).to_string();
            if expected_records.len() < 16 {
                expected_records.push((id.clone(), vector.clone()));
            }
            records.push(VectorRecord::new(id, vector));
        }
        let batch_requests_before = index.request_counts();
        let batch_started = Instant::now();
        index.upsert(records)?;
        samples.push(WriteSample {
            op: "upsert",
            batch_index: samples.len(),
            batch_records,
            batch_latency_ms: elapsed_ms(batch_started),
            requests: index.request_counts().delta(&batch_requests_before),
        });
        Ok(())
    })?;
    Ok(UpsertMeasurement {
        row: write_row_from_samples(
            "upsert",
            count,
            elapsed_ms(started),
            samples,
            index.request_counts().delta(&requests_before),
        ),
        expected_records,
    })
}

fn verify_upsert_values(
    index: &BorsukIndex,
    expected: &[(String, Vec<f32>)],
) -> BenchResult<(usize, usize)> {
    let ids = expected
        .iter()
        .map(|(id, _)| id.as_str())
        .collect::<Vec<_>>();
    let records = index.get_records(&ids)?;
    let correct = records
        .iter()
        .zip(expected)
        .filter(|(record, (_, vector))| {
            record
                .as_ref()
                .is_some_and(|(observed, _)| observed == vector)
        })
        .count();
    Ok((expected.len(), correct))
}

fn verify_delete_absence(index: &BorsukIndex, count: usize) -> BenchResult<(usize, usize)> {
    let samples = count.min(16);
    let ids = (0..samples).map(|id| id.to_string()).collect::<Vec<_>>();
    let absent = index
        .get_records(&ids)?
        .into_iter()
        .filter(Option::is_none)
        .count();
    Ok((samples, absent))
}

fn write_batch_len(count: usize, offset: usize, batch_size: usize) -> usize {
    count.saturating_sub(offset).min(batch_size)
}

fn write_operation_count(train_count: usize, configured: Option<usize>) -> BenchResult<usize> {
    let count = configured.unwrap_or_else(|| (train_count / WRITE_FRACTION_DENOMINATOR).max(1));
    if count > train_count {
        return Err(invalid_input(&format!(
            "BORSUK_BENCH_WRITE_OPS={count} exceeds the {train_count}-row mutation source"
        ))
        .into());
    }
    Ok(count)
}

fn percentage_operation_count(base: usize, percent: usize) -> BenchResult<usize> {
    if base == 0 || !(1..=100).contains(&percent) {
        return Err(
            invalid_input("lifecycle mutation percentage must be between 1 and 100").into(),
        );
    }
    Ok(base.saturating_mul(percent).saturating_add(99) / 100)
}

fn stream_dataset_batches(
    config: &ResolvedConfig,
    dataset: &Dataset,
    count: usize,
    mut consume: impl FnMut(usize, Vec<Vec<f32>>) -> BenchResult<()>,
) -> BenchResult<()> {
    let mut offset = 0_usize;
    match &dataset.source {
        DatasetVectorSource::Unavailable => {
            while offset < count {
                let batch_rows = write_batch_len(count, offset, config.write_batch_size);
                let vectors = (offset..offset.saturating_add(batch_rows))
                    .map(|row| deterministic_mutation_vector(row, dataset.meta.dim))
                    .collect();
                consume(offset, vectors)?;
                offset = offset.saturating_add(batch_rows);
            }
        }
        DatasetVectorSource::RawF32 => {
            let mut reader = BufReader::new(File::open(config.dataset_dir.join("train.f32"))?);
            while offset < count {
                let batch_rows = write_batch_len(count, offset, config.write_batch_size);
                let mut vectors = Vec::with_capacity(batch_rows);
                for _ in 0..batch_rows {
                    vectors.push(read_f32_vector(&mut reader, dataset.meta.dim)?);
                }
                consume(offset, vectors)?;
                offset = offset.saturating_add(batch_rows);
            }
        }
        DatasetVectorSource::Parquet { train_files } => {
            'files: for path in train_files {
                let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(path)?)?
                    .with_batch_size(config.write_batch_size)
                    .build()?;
                for batch in reader {
                    if offset == count {
                        break 'files;
                    }
                    let batch = batch?;
                    let column = batch.column_by_name("emb").ok_or_else(|| {
                        invalid_input(&format!("{} has no `emb` vector column", path.display()))
                    })?;
                    let batch_rows = batch.num_rows().min(count.saturating_sub(offset));
                    let mut vectors = Vec::with_capacity(batch_rows);
                    for row in 0..batch_rows {
                        vectors.push(vector_row(column.as_ref(), row, dataset.meta.dim, "emb")?);
                    }
                    consume(offset, vectors)?;
                    offset = offset.saturating_add(batch_rows);
                }
            }
        }
    }
    if offset != count {
        return Err(invalid_input(&format!(
            "mutation source ended after {offset} vectors; expected {count}"
        ))
        .into());
    }
    Ok(())
}

fn deterministic_mutation_vector(row: usize, dimensions: usize) -> Vec<f32> {
    let mut state = (row as u64).wrapping_add(0x9e37_79b9_7f4a_7c15);
    let mut vector = Vec::with_capacity(dimensions);
    for _ in 0..dimensions {
        state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut mixed = state;
        mixed = (mixed ^ (mixed >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        mixed = (mixed ^ (mixed >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        mixed ^= mixed >> 31;
        vector.push((mixed as u32 as f32 / u32::MAX as f32) * 2.0 - 1.0);
    }
    vector
}

fn measure_deletes(
    index: &mut BorsukIndex,
    count: usize,
    write_batch_size: usize,
) -> BenchResult<WriteRow> {
    let requests_before = index.request_counts();
    let started = Instant::now();
    let mut samples = Vec::new();
    let mut offset = 0_usize;
    while offset < count {
        let end = offset.saturating_add(write_batch_size).min(count);
        let ids = (offset..end).map(|id| id.to_string()).collect::<Vec<_>>();
        let batch_requests_before = index.request_counts();
        let batch_started = Instant::now();
        let report = index.delete(ids)?;
        samples.push(WriteSample {
            op: "delete",
            batch_index: samples.len(),
            batch_records: report.ids_submitted,
            batch_latency_ms: elapsed_ms(batch_started),
            requests: index.request_counts().delta(&batch_requests_before),
        });
        offset = end;
    }
    Ok(write_row_from_samples(
        "delete",
        count,
        elapsed_ms(started),
        samples,
        index.request_counts().delta(&requests_before),
    ))
}

fn write_row_from_samples(
    op: &'static str,
    ops: usize,
    wall_ms: f64,
    samples: Vec<WriteSample>,
    requests: RequestCounts,
) -> WriteRow {
    WriteRow {
        op,
        ops,
        wall_ms,
        latencies_ms: samples
            .iter()
            .map(|sample| sample.batch_latency_ms)
            .collect(),
        samples,
        requests,
        bytes_read: 0,
        bytes_written: 0,
    }
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
        // A zero-norm query under a norm-dependent metric has no meaningful
        // nearest neighbour (its distance to everything is the metric max), so it
        // no longer aborts the run — it just contributes no recall figure. The
        // engine indexes every corpus vector (zeros rank last), so only the query
        // side needs this guard.
        let recall = if is_zero_norm(query) {
            None
        } else if let Some(truth) = ground_truth {
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

fn approximate_options(
    leaf_mode: LeafMode,
    routing_page_overfetch: usize,
    max_candidates: usize,
    nprobe: usize,
    cache_execution: CacheExecutionPolicy,
    force_segment_path: bool,
) -> SearchOptions {
    let mut options = SearchOptions::approx(RECALL_K, leaf_mode)
        .with_routing_page_overfetch(routing_page_overfetch)
        .with_cache_execution(cache_execution);
    if max_candidates > 0 {
        options = options.with_max_candidates_per_segment(max_candidates);
    }
    // Leaving nprobe unset selects the persisted v8 corpus-size default on the
    // global PQ path. Legacy/fallback cell scans retain their unbounded meaning.
    if nprobe > 0 {
        options = options.with_max_segments(nprobe);
    }
    if force_segment_path {
        options.without_coarse_quantizer()
    } else {
        options
    }
}

fn validate_v12_leaf_page_budgets(budgets: &[usize]) -> io::Result<()> {
    for &budget in budgets {
        if !matches!(budget, 4 | 8 | 16 | 32 | 64) {
            return Err(invalid_input(&format!(
                "V12 leaf-page budget must be 4, 8, 16, 32, or 64; received {budget}"
            )));
        }
    }
    Ok(())
}

fn validate_v12_candidate_budgets(budgets: &[usize]) -> io::Result<()> {
    if budgets != DEFAULT_RECALL_CANDIDATES {
        return Err(invalid_input(
            "BORSUK_BENCH_CANDIDATES must keep the preregistered V15 exact-rerank budget 512",
        ));
    }
    Ok(())
}

fn validate_v12_leaf_mode(
    name: &str,
    leaf_mode: LeafMode,
    global_scan_codec: GlobalScanCodec,
) -> io::Result<()> {
    let expected = global_scan_codec.leaf_mode();
    if leaf_mode != expected {
        return Err(invalid_input(&format!(
            "{name}={leaf_mode} cannot execute bounded V12; expected {expected} for BORSUK_BENCH_GLOBAL_SCAN_CODEC={global_scan_codec}"
        )));
    }
    Ok(())
}

fn validate_bounded_v14_execution(summary: &QuerySummary) -> io::Result<()> {
    if summary.execution_engine() != "bounded-cell-card-v15" {
        return Err(invalid_input(&format!(
            "production recall expected bounded-cell-card-v15 but observed {}",
            summary.execution_engine()
        )));
    }
    Ok(())
}

fn serving_options(config: &ResolvedConfig) -> SearchOptions {
    match config.serving_mode {
        ServingMode::Exact => SearchOptions::exact(RECALL_K),
        ServingMode::Hybrid => approximate_options(
            config.serving_leaf_mode,
            HIGH_RECALL_ROUTING_OVERFETCH,
            config.serving_candidates,
            config.serving_nprobe,
            config.cache_execution,
            config.force_segment_path,
        )
        .with_prefetch_depth(config.serving_prefetch_depth),
    }
}

fn reset_cache(path: &Path) -> io::Result<()> {
    fs::create_dir_all(path)?;
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            fs::remove_dir_all(entry.path())?;
        } else {
            fs::remove_file(entry.path())?;
        }
    }
    Ok(())
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

fn maximum(values: &[f64]) -> f64 {
    values.iter().copied().max_by(f64::total_cmp).unwrap_or(0.0)
}

fn sample_mean(values: &[f64]) -> f64 {
    mean(values.iter().sum(), values.len())
}

fn sample_stddev(values: &[f64]) -> f64 {
    if values.len() < 2 {
        return 0.0;
    }
    let average = sample_mean(values);
    let squared_deviations = values
        .iter()
        .map(|value| {
            let deviation = value - average;
            deviation * deviation
        })
        .sum::<f64>();
    (squared_deviations / (values.len() - 1) as f64).sqrt()
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

fn env_percentage(name: &str, default: usize) -> BenchResult<usize> {
    let value = env_usize(name, default)?;
    if !(1..=100).contains(&value) {
        return Err(invalid_input(&format!("{name} must be between 1 and 100")).into());
    }
    Ok(value)
}

fn env_optional_cap(name: &str, default: Option<usize>) -> BenchResult<Option<usize>> {
    match env::var(name) {
        Ok(value) => {
            let parsed = value.parse::<usize>().map_err(|error| {
                invalid_input(&format!("{name} must be an unsigned integer: {error}"))
            })?;
            Ok((parsed != 0).then_some(parsed))
        }
        Err(env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(error.into()),
    }
}

fn env_optional_byte_cap(name: &str, default: Option<u64>) -> BenchResult<Option<u64>> {
    match env::var(name) {
        Ok(value) => parse_optional_byte_cap(name, &value),
        Err(env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(error.into()),
    }
}

fn env_u64(name: &str, default: u64) -> BenchResult<u64> {
    match env::var(name) {
        Ok(value) => value.parse::<u64>().map_err(|error| {
            invalid_input(&format!("{name} must be an unsigned integer: {error}")).into()
        }),
        Err(env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(error.into()),
    }
}

fn parse_optional_byte_cap(name: &str, value: &str) -> BenchResult<Option<u64>> {
    let parsed = value
        .parse::<u64>()
        .map_err(|error| invalid_input(&format!("{name} must be an unsigned integer: {error}")))?;
    Ok((parsed != 0).then_some(parsed))
}

fn env_positive_list(name: &str, default: &[usize]) -> BenchResult<Vec<usize>> {
    match env::var(name) {
        Ok(value) => parse_positive_list(name, &value),
        Err(env::VarError::NotPresent) => Ok(default.to_vec()),
        Err(error) => Err(error.into()),
    }
}

fn parse_positive_list(name: &str, value: &str) -> BenchResult<Vec<usize>> {
    let values = value
        .split(',')
        .map(str::trim)
        .map(|item| {
            item.parse::<usize>().map_err(|error| {
                invalid_input(&format!("{name} contains invalid value `{item}`: {error}"))
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    if values.is_empty() || values.contains(&0) {
        return Err(invalid_input(&format!(
            "{name} must contain comma-separated positive integers"
        ))
        .into());
    }
    Ok(values)
}

fn env_flag(name: &str) -> BenchResult<bool> {
    match env::var(name) {
        Ok(value) => parse_flag_value(name, &value),
        Err(env::VarError::NotPresent) => Ok(false),
        Err(error) => Err(error.into()),
    }
}

fn env_flag_with_default(name: &str, default: bool) -> BenchResult<bool> {
    match env::var(name) {
        Ok(value) => parse_flag_value(name, &value),
        Err(env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(error.into()),
    }
}

fn parse_flag_value(name: &str, value: &str) -> BenchResult<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" => Ok(true),
        "0" | "false" | "no" => Ok(false),
        _ => Err(invalid_input(&format!(
            "{name} must be one of 1, 0, true, false, yes, or no"
        ))
        .into()),
    }
}

fn join_usizes(values: &[usize]) -> String {
    values
        .iter()
        .map(usize::to_string)
        .collect::<Vec<_>>()
        .join(",")
}

fn parse_serving_mode(value: &str) -> BenchResult<ServingMode> {
    match value.trim().to_ascii_lowercase().as_str() {
        "exact" => Ok(ServingMode::Exact),
        "hybrid" | "approx" | "approximate" => Ok(ServingMode::Hybrid),
        _ => Err(invalid_input("BORSUK_BENCH_SERVING_MODE must be `exact` or `hybrid`").into()),
    }
}

fn parse_leaf_mode(value: &str) -> BenchResult<LeafMode> {
    value
        .parse::<LeafMode>()
        .map_err(|error| invalid_input(&error.to_string()).into())
}

fn parse_leaf_capability(value: &str) -> BenchResult<LeafCapability> {
    value
        .parse::<LeafCapability>()
        .map_err(|error| invalid_input(&error.to_string()).into())
}

fn parse_global_pq_layout(value: &str) -> BenchResult<GlobalPqLayout> {
    let normalized = value.trim().to_ascii_lowercase().replace('_', "-");
    match normalized.as_str() {
        "adaptive" => Ok(GlobalPqLayout::Adaptive),
        "flat-256" | "flat256" => Ok(GlobalPqLayout::Flat256),
        "product-2x64" | "product2x64" => Ok(GlobalPqLayout::Product2x64),
        _ => {
            let Some(children) = normalized.strip_prefix("hierarchical-") else {
                return Err(invalid_input(
                    "BORSUK_BENCH_GLOBAL_PQ_LAYOUT must be adaptive, flat-256, product-2x64, or hierarchical-<1..256>",
                )
                .into());
            };
            let children_per_parent = children.parse::<usize>().map_err(|_| {
                invalid_input(
                    "BORSUK_BENCH_GLOBAL_PQ_LAYOUT hierarchical child count must be an integer",
                )
            })?;
            if !(1..=256).contains(&children_per_parent) {
                return Err(invalid_input(
                    "BORSUK_BENCH_GLOBAL_PQ_LAYOUT hierarchical child count must be in 1..=256",
                )
                .into());
            }
            Ok(GlobalPqLayout::Hierarchical {
                children_per_parent,
            })
        }
    }
}

fn default_build_leaf_capability() -> LeafCapability {
    LeafCapability::PqScanOnly
}

fn validate_leaf_capability_modes(
    leaf_capability: LeafCapability,
    recall_leaf_mode: LeafMode,
    serving_mode: ServingMode,
    serving_leaf_mode: LeafMode,
) -> BenchResult<()> {
    if !leaf_capability.allows_leaf_mode(recall_leaf_mode) {
        return Err(invalid_input(&format!(
            "BORSUK_BENCH_RECALL_LEAF_MODE={recall_leaf_mode} requires BORSUK_BENCH_LEAF_CAPABILITY=graph-enabled"
        ))
        .into());
    }
    if serving_mode == ServingMode::Hybrid && !leaf_capability.allows_leaf_mode(serving_leaf_mode) {
        return Err(invalid_input(&format!(
            "BORSUK_BENCH_SERVING_LEAF_MODE={serving_leaf_mode} requires BORSUK_BENCH_LEAF_CAPABILITY=graph-enabled"
        ))
        .into());
    }
    Ok(())
}

fn default_recall_leaf_mode() -> LeafMode {
    LeafMode::SrhtPqScan
}

fn default_serving_leaf_mode() -> LeafMode {
    LeafMode::SrhtPqScan
}

fn parse_concurrency(value: &str) -> BenchResult<Vec<usize>> {
    parse_positive_list("BORSUK_BENCH_CONCURRENCY", value).map_err(|_| {
        invalid_input(
            "BORSUK_BENCH_CONCURRENCY must contain comma-separated positive worker counts",
        )
        .into()
    })
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

fn permuted_positions(count: usize, seed: u64) -> Vec<usize> {
    let mut positions = (0..count).collect::<Vec<_>>();
    let mut state = seed ^ 0x9e37_79b9_7f4a_7c15;
    for upper in (1..count).rev() {
        state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut mixed = state;
        mixed = (mixed ^ (mixed >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        mixed = (mixed ^ (mixed >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        mixed ^= mixed >> 31;
        positions.swap(upper, mixed as usize % (upper + 1));
    }
    positions
}

#[cfg(test)]
mod tests {
    use super::{
        BUILD_HEADER, BenchmarkCacheProfile, BorsukIndex, CACHE_COVERAGE_HEADER,
        CACHE_STATE_HEADER, CONCURRENCY_HEADER, CONCURRENCY_SAMPLE_HEADER, CacheExecutionPolicy,
        DEFAULT_NPROBE_SWEEP, DEFAULT_PRODUCTION_RAM_BUDGET_BYTES, DEFAULT_RECALL_CANDIDATES,
        GlobalScanCodec, IndexConfig, LIFECYCLE_HEADER, LeafCapability, LeafMode,
        MUTATION_QUERY_HEADER, MUTATION_QUERY_SAMPLE_HEADER, QUERY_SAMPLE_HEADER, QuerySample,
        QuerySummary, RECALL_LATENCY_HEADER, SERVING_CANDIDATES, ServingMode, VectorMetric,
        WRITE_COST_HEADER, WRITE_SAMPLE_HEADER, allow_missing_corpus_for_phase,
        approximate_options, benchmark_row_ids, cache_coverage_cohort_size, cache_coverage_enabled,
        dataset_metric, default_build_leaf_capability, default_recall_leaf_mode,
        default_serving_leaf_mode, deterministic_mutation_vector, dollars_per_million_queries,
        ingest_batch_size, ingest_generated_batch, is_hot_workload_position,
        mixed_concurrency_query_indices, neighbor_row, normalized_cache_access_fractions,
        parquet_train_files_for_phase, parse_flag_value, parse_global_pq_layout,
        parse_leaf_capability, parse_leaf_mode, parse_optional_byte_cap, parse_positive_list,
        parse_serving_mode, percentage_operation_count, permuted_positions, preload_query_count,
        read_logical_cell_catalog, recall_preloads_local_snapshot, recall_row_count, reset_cache,
        rotated_workload_index, sample_mean, sample_stddev, update_vector_reservoir,
        uses_bounded_decoded_cache_phases, uses_memory_preloaded_phase,
        validate_bounded_v14_execution, validate_build_only, validate_disk_cached_network,
        validate_generated_id_range, validate_insert_only, validate_leaf_capability_modes,
        validate_phase_selection, validate_v12_candidate_budgets, validate_v12_leaf_mode,
        validate_v12_leaf_page_budgets, vector_row, write_batch_len, write_operation_count,
    };

    #[test]
    fn cache_reset_preserves_the_dedicated_mount_point() {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};

        let parent = tempfile::tempdir().unwrap();
        let cache = parent.path().join("cache");
        std::fs::create_dir(&cache).unwrap();
        std::fs::set_permissions(&cache, std::fs::Permissions::from_mode(0o750)).unwrap();
        std::fs::create_dir(cache.join("nested")).unwrap();
        std::fs::write(cache.join("nested/object"), b"cached").unwrap();
        let inode = std::fs::metadata(&cache).unwrap().ino();

        reset_cache(&cache).unwrap();

        assert_eq!(std::fs::metadata(&cache).unwrap().ino(), inode);
        assert_eq!(
            std::fs::metadata(&cache).unwrap().permissions().mode() & 0o777,
            0o750
        );
        assert_eq!(std::fs::read_dir(&cache).unwrap().count(), 0);
    }

    #[test]
    fn direct_query_permutation_is_seeded_and_membership_preserving() {
        let first = permuted_positions(20, 17);
        assert_eq!(first, permuted_positions(20, 17));
        assert_ne!(first, permuted_positions(20, 23));
        let mut sorted = first;
        sorted.sort_unstable();
        assert_eq!(sorted, (0..20).collect::<Vec<_>>());
        assert_eq!(permuted_positions(10, 17), [2, 6, 8, 9, 7, 1, 0, 5, 3, 4]);
    }

    #[test]
    fn benchmark_ingest_ids_are_explicit_deterministic_row_ids() {
        assert_eq!(benchmark_row_ids(5, 3), ["5", "6", "7"]);
    }

    #[test]
    fn frozen_corpus_ingest_does_not_create_mutation_tombstones() {
        let directory = tempfile::tempdir().unwrap();
        let mut index = BorsukIndex::create(IndexConfig {
            uri: directory.path().to_string_lossy().into_owned(),
            metric: VectorMetric::Euclidean,
            dimensions: 2,
            segment_max_vectors: 16,
            ram_budget_bytes: None,
            text: false,
            named_vectors: Default::default(),
        })
        .unwrap();

        ingest_generated_batch(&mut index, 0, vec![vec![1.0, 0.0], vec![0.0, 1.0]]).unwrap();
        index.flush().unwrap();

        assert_eq!(index.manifest().tombstone_delta_run_count(), 0);
        assert_eq!(index.manifest().tombstone_page_count(), 0);
        assert_eq!(index.get_vector("0").unwrap(), Some(vec![1.0, 0.0]));
        assert_eq!(index.get_vector("1").unwrap(), Some(vec![0.0, 1.0]));
    }

    #[test]
    fn logical_cell_catalog_file_is_exactly_sized_and_finite() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("catalog.f32");
        let values = [0.0_f32, 1.0, 2.0, 3.0, 4.0, 5.0];
        std::fs::write(
            &path,
            values
                .iter()
                .flat_map(|value| value.to_le_bytes())
                .collect::<Vec<_>>(),
        )
        .unwrap();

        assert_eq!(
            read_logical_cell_catalog(&path, 2, 3).unwrap(),
            [vec![0.0, 1.0, 2.0], vec![3.0, 4.0, 5.0]]
        );
        assert!(read_logical_cell_catalog(&path, 3, 3).is_err());

        std::fs::write(&path, f32::NAN.to_le_bytes()).unwrap();
        assert!(read_logical_cell_catalog(&path, 1, 1).is_err());
    }

    #[test]
    fn vector_reservoir_is_bounded_seeded_and_deterministic() {
        let sample = |seed| {
            let mut reservoir = Vec::new();
            for row in 0..100 {
                update_vector_reservoir(&mut reservoir, vec![row as f32], row, 8, seed);
            }
            reservoir
        };
        assert_eq!(sample(41), sample(41));
        assert_ne!(sample(41), sample(42));
        assert_eq!(sample(41).len(), 8);
    }

    use arrow_array::{
        LargeListArray,
        types::{Float32Type, Int64Type},
    };
    #[test]
    fn format_ab_can_force_the_normal_segment_query_path() {
        let options = approximate_options(
            LeafMode::SrhtPqScan,
            1,
            256,
            8,
            CacheExecutionPolicy::Scan,
            true,
        );

        assert!(options.disable_coarse_quantizer);
    }

    #[test]
    fn positive_list_parses_candidate_budget_sweep() {
        assert_eq!(
            parse_positive_list("BORSUK_BENCH_CANDIDATES", "32, 64,128").unwrap(),
            vec![32, 64, 128]
        );
    }

    #[test]
    fn positive_list_rejects_zero() {
        let error = parse_positive_list("BORSUK_BENCH_CANDIDATES", "32,0,128")
            .expect_err("zero is not a valid search budget");

        assert!(
            error
                .to_string()
                .contains("must contain comma-separated positive integers"),
            "{error}"
        );
    }

    #[test]
    fn production_v12_page_budget_sweep_is_bounded_and_rejects_legacy_nprobe_values() {
        assert_eq!(DEFAULT_NPROBE_SWEEP, [4, 8, 16, 32]);
        validate_v12_leaf_page_budgets(DEFAULT_NPROBE_SWEEP).unwrap();
        validate_v12_leaf_page_budgets(&[64]).unwrap();

        let error = validate_v12_leaf_page_budgets(&[128])
            .expect_err("legacy nprobe value silently selected the segment path");
        assert!(
            error.to_string().contains("V12 leaf-page budget")
                && error.to_string().contains("4, 8, 16, 32, or 64"),
            "{error}"
        );
    }

    #[test]
    fn production_v12_leaf_mode_mismatch_fails_before_build() {
        validate_v12_leaf_mode(
            "BORSUK_BENCH_RECALL_LEAF_MODE",
            LeafMode::SrhtPqScan,
            GlobalScanCodec::SrhtPq,
        )
        .unwrap();

        let error = validate_v12_leaf_mode(
            "BORSUK_BENCH_RECALL_LEAF_MODE",
            LeafMode::Graph,
            GlobalScanCodec::SrhtPq,
        )
        .expect_err("a non-V12 leaf mode survived the pre-build validation");
        assert!(
            error
                .to_string()
                .contains("BORSUK_BENCH_RECALL_LEAF_MODE=graph")
                && error.to_string().contains("srht-pq-scan"),
            "{error}"
        );
    }

    #[test]
    fn production_v15_pins_the_preregistered_exact_rerank_budget() {
        validate_v12_candidate_budgets(DEFAULT_RECALL_CANDIDATES).unwrap();
        assert_eq!(SERVING_CANDIDATES, DEFAULT_RECALL_CANDIDATES[0]);
        let error = validate_v12_candidate_budgets(&[128, 4_096])
            .expect_err("an unqualified V15 exact-rerank sweep was accepted");
        assert!(
            error.to_string().contains("BORSUK_BENCH_CANDIDATES")
                && error
                    .to_string()
                    .contains("preregistered V15 exact-rerank budget"),
            "{error}"
        );
    }

    #[test]
    fn production_recall_requires_the_frozen_v14_engine() {
        let mut fallback = QuerySummary::default();
        fallback.execution_engines.insert("srht-pq-scan".to_owned());
        let error = validate_bounded_v14_execution(&fallback)
            .expect_err("legacy segment execution was accepted as a V14 measurement");
        assert!(
            error.to_string().contains("bounded-cell-card-v15")
                && error.to_string().contains("srht-pq-scan"),
            "{error}"
        );

        let mut bounded = QuerySummary::default();
        bounded
            .execution_engines
            .insert("bounded-cell-card-v15".to_owned());
        validate_bounded_v14_execution(&bounded).unwrap();
    }

    #[test]
    fn serving_mode_defaults_can_select_exact_or_hybrid() {
        assert_eq!(parse_serving_mode("exact").unwrap(), ServingMode::Exact);
        assert_eq!(parse_serving_mode("hybrid").unwrap(), ServingMode::Hybrid);
        assert!(parse_serving_mode("slow-magic").is_err());
    }

    #[test]
    fn leaf_mode_controls_accept_public_leaf_names() {
        assert_eq!(parse_leaf_mode("sq-scan").unwrap(), LeafMode::SqScan);
        assert_eq!(parse_leaf_mode("pq-scan").unwrap(), LeafMode::PqScan);
        assert_eq!(
            parse_leaf_mode("srht-pq-scan").unwrap(),
            LeafMode::SrhtPqScan
        );
        assert_eq!(
            parse_leaf_mode("fast-turboquant-scan").unwrap(),
            LeafMode::FastTurboQuantProdScan
        );
        assert!(parse_leaf_mode("turboquant-mse-scan").is_err());
        assert!(parse_leaf_mode("turboquant-scan").is_err());
        assert_eq!(parse_leaf_mode("vamana-pq").unwrap(), LeafMode::VamanaPq);
        assert!(parse_leaf_mode("mystery").is_err());
    }

    #[test]
    fn leaf_capability_control_accepts_public_names() {
        assert_eq!(
            parse_leaf_capability("pq-scan-only").unwrap(),
            LeafCapability::PqScanOnly
        );
        assert_eq!(
            parse_leaf_capability("graph-enabled").unwrap(),
            LeafCapability::GraphEnabled
        );
        assert!(parse_leaf_capability("mystery").is_err());
    }

    #[test]
    fn global_pq_layout_control_accepts_public_ablation_names() {
        assert_eq!(
            parse_global_pq_layout("product-2x64").unwrap(),
            borsuk::GlobalPqLayout::Product2x64
        );
        assert_eq!(
            parse_global_pq_layout("hierarchical-16").unwrap(),
            borsuk::GlobalPqLayout::Hierarchical {
                children_per_parent: 16,
            }
        );
        assert!(parse_global_pq_layout("hierarchical-0").is_err());
    }

    #[test]
    fn default_build_capability_is_graph_free() {
        assert_eq!(default_build_leaf_capability(), LeafCapability::PqScanOnly);
    }

    #[test]
    fn graph_modes_require_graph_enabled_build_capability() {
        assert!(
            validate_leaf_capability_modes(
                LeafCapability::PqScanOnly,
                LeafMode::Graph,
                ServingMode::Exact,
                LeafMode::PqScan,
            )
            .is_err()
        );
        assert!(
            validate_leaf_capability_modes(
                LeafCapability::PqScanOnly,
                LeafMode::PqScan,
                ServingMode::Hybrid,
                LeafMode::Graph,
            )
            .is_err()
        );
        assert!(
            validate_leaf_capability_modes(
                LeafCapability::GraphEnabled,
                LeafMode::Graph,
                ServingMode::Hybrid,
                LeafMode::Graph,
            )
            .is_ok()
        );
    }

    #[test]
    fn default_recall_leaf_mode_is_graph_free() {
        assert_eq!(default_recall_leaf_mode(), LeafMode::SrhtPqScan);
    }

    #[test]
    fn default_serving_leaf_mode_is_graph_free() {
        assert_eq!(default_serving_leaf_mode(), LeafMode::SrhtPqScan);
    }

    #[test]
    fn preload_does_not_hide_query_work_inside_startup() {
        assert_eq!(preload_query_count(), 0);
    }

    #[test]
    fn cache_execution_matrix_preloads_only_explicit_snapshot_profiles() {
        assert!(recall_preloads_local_snapshot(true));
        assert!(!recall_preloads_local_snapshot(false));
    }

    #[test]
    fn preloading_segments_does_not_relabel_global_storage_scan_as_memory_preloaded() {
        assert!(!uses_memory_preloaded_phase(
            true,
            CacheExecutionPolicy::Scan,
            true,
        ));
        assert!(uses_memory_preloaded_phase(
            true,
            CacheExecutionPolicy::Graph,
            true,
        ));
        assert!(uses_memory_preloaded_phase(
            true,
            CacheExecutionPolicy::Auto,
            true,
        ));
        assert!(!uses_memory_preloaded_phase(
            true,
            CacheExecutionPolicy::Auto,
            false,
        ));
    }

    #[test]
    fn bounded_decoded_graph_cache_has_distinct_fill_and_steady_phases() {
        assert!(uses_bounded_decoded_cache_phases(
            false,
            LeafMode::Graph,
            Some(256 * 1024 * 1024)
        ));
        assert!(!uses_bounded_decoded_cache_phases(
            true,
            LeafMode::Graph,
            Some(256 * 1024 * 1024)
        ));
        assert!(!uses_bounded_decoded_cache_phases(
            false,
            LeafMode::Graph,
            None
        ));
        assert!(!uses_bounded_decoded_cache_phases(
            false,
            LeafMode::SrhtPqScan,
            Some(256 * 1024 * 1024)
        ));
    }

    #[test]
    fn latency_artifact_schemas_include_the_worst_query() {
        assert_eq!(RECALL_LATENCY_HEADER.split(',').count(), 33);
        assert_eq!(CACHE_STATE_HEADER.split(',').count(), 31);
        assert_eq!(CONCURRENCY_HEADER.split(',').count(), 30);
        assert_eq!(CACHE_COVERAGE_HEADER.split(',').count(), 33);
        assert_eq!(QUERY_SAMPLE_HEADER.split(',').count(), 41);
        assert_eq!(
            QUERY_SAMPLE_HEADER.split(',').skip(39).collect::<Vec<_>>(),
            vec!["global_leaf_code_requests", "global_leaf_exact_requests"]
        );
        assert_eq!(CONCURRENCY_SAMPLE_HEADER.split(',').count(), 37);
        for header in [
            RECALL_LATENCY_HEADER,
            CACHE_STATE_HEADER,
            CONCURRENCY_HEADER,
            CACHE_COVERAGE_HEADER,
            QUERY_SAMPLE_HEADER,
            CONCURRENCY_SAMPLE_HEADER,
        ] {
            assert!(header.starts_with("schema_version,"));
            assert!(!header.contains("graph"), "stale graph column in {header}");
            assert!(
                !header.contains("global_scan"),
                "stale scan column in {header}"
            );
        }
        for column in [
            "scan_codec",
            "turboquant_bits",
            "turboquant_qjl_bits",
            "turboquant_shards",
            "cache_execution",
            "execution_engine",
            "mean_ms",
            "stddev_ms",
            "avg_global_leaf_directory_reads",
            "avg_global_leaf_directory_bytes",
            "avg_global_leaf_code_pages_read",
            "avg_global_leaf_code_bytes",
            "avg_global_leaf_pages_read",
            "avg_global_leaf_page_bytes",
            "avg_global_leaf_waves",
            "avg_global_leaf_continuations",
            "avg_global_leaf_exact_scores",
            "avg_backing_reads",
            "avg_backing_bytes_read",
        ] {
            assert!(RECALL_LATENCY_HEADER.contains(column), "missing {column}");
            assert!(CACHE_STATE_HEADER.contains(column), "missing {column}");
            assert!(CONCURRENCY_HEADER.contains(column), "missing {column}");
        }
        assert!(RECALL_LATENCY_HEADER.contains("phase,mode,"));
        assert!(RECALL_LATENCY_HEADER.contains("max_ms"));
        assert!(CACHE_STATE_HEADER.contains("max_ms"));
        assert!(CONCURRENCY_HEADER.contains("max_ms"));
        for column in [
            "sample_index",
            "query_source_index",
            "latency_ms",
            "recall_at_10",
            "disk_cache_bytes_read",
            "backing_bytes_read",
            "global_leaf_directory_reads",
            "global_leaf_directory_bytes",
            "global_leaf_code_pages_read",
            "global_leaf_code_requests",
            "global_leaf_code_bytes",
            "global_leaf_pages_read",
            "global_leaf_exact_requests",
            "global_leaf_page_bytes",
            "global_leaf_waves",
            "global_leaf_continuations",
            "global_leaf_exact_scores",
            "network_gets",
            "query_seed",
            "repetition_id",
            "ram_budget_bytes",
            "collection_resident_bytes",
            "retained_bytes",
            "retained_capacity_bytes",
            "retained_peak_bytes",
            "transient_bytes",
            "transient_capacity_bytes",
            "transient_peak_bytes",
        ] {
            assert!(QUERY_SAMPLE_HEADER.contains(column), "missing {column}");
        }
        for column in [
            "query_source_index",
            "target_hot_set_member",
            "latency_ms",
            "recall_at_10",
            "execution_engine",
            "global_leaf_code_pages_read",
            "global_leaf_code_bytes",
            "decoded_cache_hits",
            "disk_cache_reads",
            "backing_reads",
            "decoded_cache_bytes_read",
            "disk_cache_bytes_read",
            "backing_bytes_read",
            "network_gets",
            "ram_budget_bytes",
            "collection_resident_bytes",
            "retained_bytes",
            "retained_capacity_bytes",
            "retained_peak_bytes",
            "transient_bytes",
            "transient_capacity_bytes",
            "transient_peak_bytes",
        ] {
            assert!(
                CONCURRENCY_SAMPLE_HEADER.contains(column),
                "missing {column}"
            );
        }
        for column in [
            "target_hot_query_fraction",
            "repetition",
            "cohort_position",
            "query_class",
            "observed_cache_tier",
            "global_leaf_directory_reads",
            "global_leaf_directory_bytes",
            "global_leaf_code_pages_read",
            "global_leaf_code_bytes",
            "global_leaf_pages_read",
            "global_leaf_page_bytes",
            "global_leaf_waves",
            "global_leaf_continuations",
            "global_leaf_exact_scores",
            "decoded_access_fraction",
            "disk_access_fraction",
            "backing_access_fraction",
            "decoded_bytes_read",
            "disk_bytes_read",
            "backing_bytes_read",
            "disk_cache_reads",
            "backing_reads",
        ] {
            assert!(CACHE_COVERAGE_HEADER.contains(column), "missing {column}");
        }
    }

    #[test]
    fn query_samples_carry_every_v12_leaf_counter() {
        let projection = |sample: &QuerySample| {
            (
                sample.global_leaf_directory_reads,
                sample.global_leaf_directory_bytes,
                sample.global_leaf_code_pages_read,
                sample.global_leaf_code_requests,
                sample.global_leaf_code_bytes,
                sample.global_leaf_pages_read,
                sample.global_leaf_exact_requests,
                sample.global_leaf_page_bytes,
                sample.global_leaf_waves,
                sample.global_leaf_continuations,
                sample.global_leaf_exact_scores,
                sample.backing_reads,
                sample.backing_bytes_read,
            )
        };
        let _ = projection;
    }

    #[test]
    fn latency_distribution_uses_sample_standard_deviation() {
        let values = [1.0, 2.0, 3.0, 4.0];
        assert!((sample_mean(&values) - 2.5).abs() < f64::EPSILON);
        assert!((sample_stddev(&values) - 1.290_994_448_735_805_6).abs() < 1.0e-12);
        assert_eq!(sample_stddev(&[]), 0.0);
        assert_eq!(sample_stddev(&[42.0]), 0.0);
    }

    #[test]
    fn production_dataset_metric_supports_packed_binary_hamming() {
        assert_eq!(dataset_metric("cosine").unwrap(), VectorMetric::Cosine);
        assert_eq!(
            dataset_metric("euclidean").unwrap(),
            VectorMetric::Euclidean
        );
        assert_eq!(dataset_metric("hamming").unwrap(), VectorMetric::Hamming);
        assert!(dataset_metric("implicit").is_err());
    }

    #[test]
    fn build_only_requires_a_fresh_build() {
        assert!(validate_build_only(true, true).is_ok());
        assert!(validate_build_only(false, false).is_ok());
        assert!(validate_build_only(true, false).is_err());
    }

    #[test]
    fn insert_only_is_a_distinct_mutation_phase() {
        assert!(validate_insert_only(true, false, false).is_ok());
        assert!(validate_insert_only(true, true, false).is_err());
        assert!(validate_insert_only(true, false, true).is_err());
        assert!(validate_insert_only(false, true, true).is_ok());
    }

    #[test]
    fn mixed_cache_workload_has_exact_hot_ratios_and_normalized_tier_fractions() {
        for target in [0, 25, 50, 75, 100] {
            let hot = (0..20)
                .filter(|position| is_hot_workload_position(*position, target))
                .count();
            assert_eq!(hot, target / 5);
        }
        assert_eq!(
            normalized_cache_access_fractions(200, 100, 100, 0, 0),
            (0.5, 0.25, 0.25)
        );
        assert_eq!(
            normalized_cache_access_fractions(0, 0, 0, 1, 0),
            (0.0, 0.0, 1.0)
        );
        assert_eq!(
            normalized_cache_access_fractions(0, 0, 0, 0, 1024),
            (0.0, 1.0, 0.0)
        );
    }

    #[test]
    fn mixed_concurrency_workload_uses_every_query_at_exact_target_ratio() {
        for total in [2_usize, 40, 503] {
            for target in [0_usize, 10, 25, 50, 75, 90, 100] {
                let indices = mixed_concurrency_query_indices(total, target);
                let hot_count = total * target / 100;
                assert_eq!(indices.len(), total);
                assert_eq!(
                    indices
                        .iter()
                        .filter(|query_index| **query_index < hot_count)
                        .count(),
                    hot_count
                );
                let mut sorted = indices;
                sorted.sort_unstable();
                assert_eq!(sorted, (0..total).collect::<Vec<_>>());
            }
        }
    }

    #[test]
    fn cache_coverage_is_emitted_for_scan_only_storage_cache_profiles() {
        assert!(!cache_coverage_enabled(0));
        assert!(!cache_coverage_enabled(1));
        assert!(cache_coverage_enabled(2));
        assert!(cache_coverage_enabled(100));
    }

    #[test]
    fn mixed_cache_repetitions_balance_every_hot_and_cold_query() {
        assert_eq!(cache_coverage_cohort_size(100), 40);
        assert_eq!(cache_coverage_cohort_size(80), 40);
        assert_eq!(cache_coverage_cohort_size(10), 4);

        for target_hot_percent in [0_usize, 25, 50, 75, 100] {
            let hot_per_repetition = 40 * target_hot_percent / 100;
            let cold_per_repetition = 40 - hot_per_repetition;
            let mut hot_seen = vec![0_usize; 40];
            let mut cold_seen = vec![0_usize; 40];
            for repetition in 0..4 {
                for cursor in 0..hot_per_repetition {
                    hot_seen[rotated_workload_index(40, repetition, hot_per_repetition, cursor)] +=
                        1;
                }
                for cursor in 0..cold_per_repetition {
                    cold_seen
                        [rotated_workload_index(40, repetition, cold_per_repetition, cursor)] += 1;
                }
            }
            assert!(
                hot_seen
                    .iter()
                    .all(|count| *count == target_hot_percent / 25)
            );
            assert!(
                cold_seen
                    .iter()
                    .all(|count| *count == (100 - target_hot_percent) / 25)
            );
        }
    }

    #[test]
    fn build_artifact_records_fresh_build_costs_and_footprint_inputs() {
        for column in [
            "logical_cell_catalog_checksum",
            "logical_cells",
            "logical_cell_dimensions",
            "logical_cell_catalog_bytes",
            "vector_element_type",
            "build_layout",
            "leaf_capability",
            "segment_max_vectors",
            "segment_bytes",
            "vector_sidecar_bytes",
            "global_scan_bytes",
            "bytes_per_vector",
            "resident_bytes_estimate",
            "ram_budget_bytes",
            "collection_resident_bytes",
            "retained_bytes",
            "retained_capacity_bytes",
            "retained_peak_bytes",
            "transient_bytes",
            "transient_capacity_bytes",
            "transient_peak_bytes",
            "ingest_ms",
            "compaction_ms",
            "compaction_bytes_read",
            "compaction_bytes_written",
            "storage_gets",
            "storage_puts",
            "storage_deletes",
            "storage_heads",
            "storage_lists",
            "storage_bytes_read",
            "storage_bytes_written",
        ] {
            assert!(BUILD_HEADER.contains(column), "missing {column}");
        }
    }

    #[test]
    fn write_artifacts_preserve_batch_distributions_and_request_counts() {
        for column in [
            "configured_batch_records",
            "time_to_searchable_ms",
            "searchable_fraction",
            "upsert_samples",
            "upsert_correct_fraction",
            "delete_samples",
            "delete_absent_fraction",
            "compact_delete_absent_fraction",
            "purge_delete_absent_fraction",
            "time_to_fully_indexed_ms",
            "indexed_delta_bytes",
            "write_amplification",
            "write_amplification_is_lower_bound",
            "consolidation_ms",
        ] {
            assert!(LIFECYCLE_HEADER.contains(column), "missing {column}");
        }
        for column in [
            "configured_batch_records",
            "batches",
            "stddev_batch_ms",
            "p95_batch_ms",
            "p99_batch_ms",
            "max_batch_ms",
            "mean_amortized_ms",
            "gets",
            "puts",
        ] {
            assert!(WRITE_COST_HEADER.contains(column), "missing {column}");
        }
        for column in [
            "batch_index",
            "batch_records",
            "batch_latency_ms",
            "amortized_ms",
            "gets",
            "puts",
        ] {
            assert!(WRITE_SAMPLE_HEADER.contains(column), "missing {column}");
        }
        for column in [
            "stage",
            "stddev_ms",
            "p95_ms",
            "p99_ms",
            "max_ms",
            "avg_network_gets",
        ] {
            assert!(MUTATION_QUERY_HEADER.contains(column), "missing {column}");
        }
        for column in [
            "stage",
            "sample_index",
            "latency_ms",
            "execution_engine",
            "network_gets",
        ] {
            assert!(
                MUTATION_QUERY_SAMPLE_HEADER.contains(column),
                "missing {column}"
            );
        }
    }

    #[test]
    fn optional_byte_caps_accept_bounded_values_and_explicit_disable() {
        assert_eq!(
            parse_optional_byte_cap("BORSUK_BENCH_SEGMENT_CACHE_MAX_BYTES", "536870912").unwrap(),
            Some(536_870_912)
        );
        assert_eq!(
            parse_optional_byte_cap("BORSUK_BENCH_SEGMENT_CACHE_MAX_BYTES", "0").unwrap(),
            None
        );
        assert!(parse_optional_byte_cap("BORSUK_BENCH_SEGMENT_CACHE_MAX_BYTES", "512MiB").is_err());
        assert_eq!(
            parse_optional_byte_cap("BORSUK_BENCH_DISK_CACHE_MAX_BYTES", "1073741824").unwrap(),
            Some(1_073_741_824)
        );
    }

    #[test]
    fn recall_only_runtime_dataset_does_not_require_local_corpus_shards() {
        let directory = tempfile::tempdir().unwrap();
        assert_eq!(
            parquet_train_files_for_phase(directory.path(), 100_000_000, true).unwrap(),
            None
        );
        assert!(parquet_train_files_for_phase(directory.path(), 100_000_000, false).is_err());
    }

    #[test]
    fn read_only_runtime_dataset_does_not_require_local_corpus_shards() {
        let directory = tempfile::tempdir().unwrap();
        assert_eq!(
            parquet_train_files_for_phase(
                directory.path(),
                100_000_000,
                allow_missing_corpus_for_phase(false, false, true),
            )
            .unwrap(),
            None
        );
        assert!(
            parquet_train_files_for_phase(
                directory.path(),
                100_000_000,
                allow_missing_corpus_for_phase(true, false, true),
            )
            .is_err()
        );
    }

    #[test]
    fn runtime_mutation_vectors_are_deterministic_without_local_corpus() {
        let first = deterministic_mutation_vector(42, 768);
        assert_eq!(first, deterministic_mutation_vector(42, 768));
        assert_ne!(first, deterministic_mutation_vector(43, 768));
        assert_eq!(first.len(), 768);
        assert!(
            first
                .iter()
                .all(|value| value.is_finite() && (-1.0..=1.0).contains(value))
        );
    }

    #[test]
    fn lifecycle_mutation_counts_follow_the_scheduled_percentages() {
        assert_eq!(percentage_operation_count(1_000, 10).unwrap(), 100);
        assert_eq!(percentage_operation_count(1_001, 10).unwrap(), 101);
        assert_eq!(percentage_operation_count(1, 1).unwrap(), 1);
        assert!(percentage_operation_count(1_000, 0).is_err());
        assert!(percentage_operation_count(1_000, 101).is_err());
    }

    #[test]
    fn production_profile_has_a_bounded_routing_memory_default() {
        assert_eq!(DEFAULT_PRODUCTION_RAM_BUDGET_BYTES, 536_870_912);
    }

    #[test]
    fn disk_cached_validation_allows_local_bytes_but_rejects_network_gets() {
        let local_disk = QuerySummary {
            bytes_read: 4_953_727,
            billable_requests: 0,
            ..QuerySummary::default()
        };
        assert!(validate_disk_cached_network(&local_disk).is_ok());

        let network = QuerySummary {
            bytes_read: 4_953_727,
            billable_requests: 1,
            ..QuerySummary::default()
        };
        assert!(validate_disk_cached_network(&network).is_err());
    }

    #[test]
    fn boolean_benchmark_flags_reject_ambiguous_values() {
        assert!(parse_flag_value("BORSUK_BENCH_READ_ONLY", "1").unwrap());
        assert!(!parse_flag_value("BORSUK_BENCH_READ_ONLY", "false").unwrap());
        assert!(parse_flag_value("BORSUK_BENCH_READ_ONLY", "sometimes").is_err());
    }

    #[test]
    fn benchmark_cache_profile_is_explicit_and_strict() {
        assert_eq!(
            "uncached".parse::<BenchmarkCacheProfile>().unwrap(),
            BenchmarkCacheProfile::Uncached
        );
        assert_eq!(
            "disk-cached".parse::<BenchmarkCacheProfile>().unwrap(),
            BenchmarkCacheProfile::DiskCached
        );
        assert_eq!(
            "mixed_coverage".parse::<BenchmarkCacheProfile>().unwrap(),
            BenchmarkCacheProfile::MixedCoverage
        );
        assert!("warmish".parse::<BenchmarkCacheProfile>().is_err());
    }

    #[test]
    fn recall_only_and_skip_recall_are_mutually_exclusive() {
        assert!(validate_phase_selection(true, true).is_err());
        assert!(validate_phase_selection(true, false).is_ok());
        assert!(validate_phase_selection(false, true).is_ok());
    }

    #[test]
    fn recall_row_count_can_omit_redundant_exact_scan() {
        assert_eq!(recall_row_count(6, 4, false, false), 50);
        assert_eq!(recall_row_count(6, 4, true, false), 48);
        assert_eq!(recall_row_count(6, 4, false, true), 25);
        assert_eq!(recall_row_count(6, 4, true, true), 24);
    }

    #[test]
    fn bulk_ingest_rejects_a_generated_id_frontier_mismatch() {
        assert!(validate_generated_id_range(5, 8, &["5".into(), "6".into(), "7".into()]).is_ok());
        assert!(validate_generated_id_range(5, 8, &["6".into(), "7".into(), "8".into()]).is_err());
    }

    #[test]
    fn bulk_ingest_batch_is_dimension_aware_and_positioned_append_bounded() {
        assert_eq!(ingest_batch_size(64), 16_384);
        assert_eq!(ingest_batch_size(100), 16_384);
        assert_eq!(ingest_batch_size(128), 16_384);
        assert_eq!(ingest_batch_size(960), 4_369);
        assert_eq!(ingest_batch_size(usize::MAX), 1);
        assert!(3 * ingest_batch_size(128) + 2 <= 65_536);
    }

    #[test]
    fn lifecycle_write_batch_size_is_an_explicit_experiment_factor() {
        assert_eq!(write_batch_len(5_000, 0, 1), 1);
        assert_eq!(write_batch_len(5_000, 128, 256), 256);
        assert_eq!(write_batch_len(5_000, 4_900, 1_024), 100);
    }

    #[test]
    fn lifecycle_write_count_is_explicit_and_never_silently_truncated() {
        assert_eq!(write_operation_count(1_000_000, None).unwrap(), 50_000);
        assert_eq!(
            write_operation_count(1_000_000, Some(3_200)).unwrap(),
            3_200
        );
        assert!(write_operation_count(100, Some(101)).is_err());
    }

    #[test]
    fn vectordbbench_large_list_float_vectors_decode_without_format_conversion() {
        let vectors = LargeListArray::from_iter_primitive::<Float32Type, _, _>([
            Some(vec![Some(1.0), Some(2.0), Some(3.0)]),
            Some(vec![Some(4.0), Some(5.0), Some(6.0)]),
        ]);

        assert_eq!(
            vector_row(&vectors, 1, 3, "emb").unwrap(),
            vec![4.0, 5.0, 6.0]
        );
        assert!(vector_row(&vectors, 0, 2, "emb").is_err());
    }

    #[test]
    fn vectordbbench_neighbor_lists_preserve_integer_ground_truth_ids() {
        let neighbors = LargeListArray::from_iter_primitive::<Int64Type, _, _>([Some(
            (0_i64..10).map(Some).collect::<Vec<_>>(),
        )]);

        assert_eq!(
            neighbor_row(&neighbors, 0, 10, 100, "neighbors_id").unwrap(),
            (0..10).map(|id| id.to_string()).collect::<Vec<_>>()
        );
        assert!(neighbor_row(&neighbors, 0, 10, 5, "neighbors_id").is_err());
    }

    #[test]
    fn benchmark_request_cost_uses_dated_frankfurt_get_price() {
        assert!((dollars_per_million_queries(200.0) - 86.0).abs() < f64::EPSILON);
    }
}
