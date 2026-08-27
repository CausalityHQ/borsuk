#![allow(missing_docs)]

use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    env,
    error::Error,
    fs::{self, File},
    io::{self, BufReader, BufWriter, Read, Write},
    path::{Path, PathBuf},
    str::FromStr,
    sync::{Arc, Barrier, OnceLock},
    thread,
    time::{Duration, Instant},
};

use arrow_array::{
    Array, FixedSizeListArray, Float32Array, Int32Array, Int64Array, LargeListArray, ListArray,
    UInt32Array, UInt64Array,
};
use borsuk::{
    BACKING_GET_CONCURRENCY_ENV, BULK_LOAD_SOURCE_SHARDS, BorsukIndex, BuildConfig,
    CPU_THREADS_ENV, CacheExecutionPolicy, CompactionOptions,
    DEFAULT_CELL_CARD_CODE_PLANE_CACHE_BYTES, DEFAULT_EXACT_READ_MAX_PHYSICAL_AMPLIFICATION,
    DEFAULT_LEAF_READ_WIDTH, DEFAULT_MAX_ACTIVE_SEARCHES, DEFAULT_MAX_INFLIGHT_LEAF_READS,
    DEFAULT_MAX_PARALLEL_DECODE_RANK_TASKS, DEFAULT_MAX_WAITING_SEARCHES, GarbageCollectionOptions,
    GarbageCollectionReport, GlobalPqLayout, GlobalScanCodec, IO_THREADS_ENV, IndexConfig,
    LeafCapability, LeafMode, MAX_GLOBAL_DELTA_ROWS, MAX_GLOBAL_DELTA_SEGMENTS,
    MAX_GLOBAL_DELTA_VECTOR_BYTES, OpenOptions, ProcessLimits, RequestCounts, SearchOptions,
    SearchReport, V21FeasibilityArm, V21FeasibilityReport, V21LimitingBound, V22LayoutKind,
    V22LayoutLimitingBound, V22StageLProjectedObject, V22StageLReport, VectorElementType,
    VectorMetric, VectorRecord, WalConfig, WarmReport, configure_process,
    configured_backing_get_concurrency, configured_cpu_threads, configured_io_threads, recall_at_k,
    recommended_segment_max_vectors,
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde::{Deserialize, Serialize};

#[cfg(test)]
use borsuk::V21FeasibilityQuerySample;

const DEFAULT_QUERIES: usize = 1_000;
const DEFAULT_CONCURRENCY: &str = "1,2,4,8,16";
// A positioned dense insert expands into record, ID-directory, and route-plan
// rows plus transaction metadata. Keep both its logical dense bytes and vector
// count comfortably below the immutable 64 MiB / 65,536-row append bounds.
const INGEST_DENSE_BATCH_BYTES: usize = 16 * 1024 * 1024;
const INGEST_BATCH_MAX_VECTORS: usize = 16_384;
const DEFAULT_BUILD_WRITERS: usize = 8;
const DEFAULT_WRITE_BATCH_SIZE: usize = 1_024;
// V12 persists the coarse-cell probe count in the authenticated codebook. The
// query-time sweep controls how many ranked leaf pages may be fetched. Keep the
// values aligned with the bounded V12 dispatcher so a benchmark can never
// silently measure the legacy segment path.
const DEFAULT_NPROBE_SWEEP: &[usize] = &[4, 8, 16, 32];
// V15 treats the public candidate knob as its whole-index exact-rerank row
// budget. Publication V3 pins the preregistered depth in
// scripts/run_publication_v3_cell.py; this CLI also permits bounded, explicitly
// labelled diagnostic sweeps.
const DEFAULT_RECALL_CANDIDATES: &[usize] = &[512];
const MAX_DIAGNOSTIC_RECALL_CANDIDATES: usize = 65_536;
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
const PRODUCTION_BENCH_SCHEMA_VERSION: &str = "borsuk-production-bench-v20";
const RECALL_LATENCY_HEADER: &str = "schema_version,scan_codec,turboquant_bits,turboquant_qjl_bits,turboquant_shards,cache_execution,execution_engine,phase,mode,nprobe,max_candidates,recall_at_10,samples,mean_ms,stddev_ms,p50_ms,p95_ms,p99_ms,max_ms,avg_global_leaf_directory_reads,avg_global_leaf_directory_bytes,avg_global_leaf_code_pages_read,avg_global_leaf_code_bytes,avg_global_leaf_pages_read,avg_global_leaf_page_bytes,avg_global_leaf_waves,avg_global_leaf_continuations,avg_global_leaf_exact_scores,avg_backing_reads,avg_backing_bytes_read,avg_bytes_read,avg_gets_per_query,dollars_per_million_queries";
const QUERY_SAMPLE_HEADER: &str = "schema_version,scan_codec,cache_execution,phase,mode,nprobe,max_candidates,sample_index,cache_cohort_index,cache_cohort_size,cache_cohort_count,query_source_index,latency_ms,recall_at_10,execution_engine,segments_searched,global_leaf_directory_reads,global_leaf_directory_bytes,global_leaf_code_pages_read,global_leaf_code_bytes,global_leaf_pages_read,global_leaf_page_bytes,global_leaf_waves,global_leaf_continuations,global_leaf_exact_scores,bytes_read,decoded_cache_hits,disk_cache_reads,backing_reads,decoded_cache_bytes_read,disk_cache_bytes_read,backing_bytes_read,network_gets,query_seed,repetition_id,ram_budget_bytes,collection_resident_bytes,retained_bytes,retained_capacity_bytes,retained_peak_bytes,transient_bytes,transient_capacity_bytes,transient_peak_bytes,global_leaf_code_requests,global_leaf_exact_requests,global_leaf_exact_cells,global_leaf_exact_cards,global_leaf_deepest_winning_card_rank,global_leaf_exact_groups,global_leaf_exact_selected_bytes,global_leaf_exact_speculative_bytes,global_base_approximate_us,global_base_head_admission_us,global_base_head_fetch_us,global_base_head_read_attempts,global_base_head_read_successes,global_base_head_read_response_bytes,global_base_head_read_us_max,global_base_head_read_us_sum,global_base_head_read_queue_us_max,global_base_head_read_queue_us_sum,global_base_head_reads_over_20ms,global_base_head_reads_over_30ms,global_base_head_reads_over_50ms,global_base_head_reads_over_100ms,global_base_head_decode_admission_us,global_base_head_decode_us,global_base_exact_admission_us,global_base_exact_fetch_us,global_base_exact_read_attempts,global_base_exact_read_successes,global_base_exact_read_response_bytes,global_base_exact_read_queue_us_max,global_base_exact_read_queue_us_sum,global_base_exact_read_us_max,global_base_exact_read_us_sum,global_base_exact_reads_over_20ms,global_base_exact_reads_over_30ms,global_base_exact_reads_over_50ms,global_base_exact_reads_over_100ms,global_base_exact_cpu_us,global_base_exact_rerank_us";
const CACHE_STATE_HEADER: &str = "schema_version,scan_codec,turboquant_bits,turboquant_qjl_bits,turboquant_shards,cache_execution,execution_engine,phase,queries,recall_at_10,mean_ms,stddev_ms,p50_ms,p95_ms,p99_ms,max_ms,avg_global_leaf_directory_reads,avg_global_leaf_directory_bytes,avg_global_leaf_code_pages_read,avg_global_leaf_code_bytes,avg_global_leaf_pages_read,avg_global_leaf_page_bytes,avg_global_leaf_waves,avg_global_leaf_continuations,avg_global_leaf_exact_scores,avg_backing_reads,avg_backing_bytes_read,avg_bytes_read,avg_object_cache_misses,avg_network_gets,dollars_per_million_queries";
const CONCURRENCY_HEADER: &str = "schema_version,scan_codec,turboquant_bits,turboquant_qjl_bits,turboquant_shards,cache_execution,cache_profile,target_cache_coverage_percent,execution_engine,nprobe,max_candidates,workers,total_queries,qps,mean_ms,stddev_ms,p50_ms,p95_ms,p99_ms,max_ms,avg_global_leaf_directory_reads,avg_global_leaf_directory_bytes,avg_global_leaf_code_pages_read,avg_global_leaf_code_bytes,avg_global_leaf_pages_read,avg_global_leaf_page_bytes,avg_global_leaf_waves,avg_global_leaf_continuations,avg_global_leaf_exact_scores,avg_backing_reads,avg_backing_bytes_read,avg_bytes_read";
const CONCURRENCY_SAMPLE_HEADER: &str = "schema_version,scan_codec,cache_execution,cache_profile,target_cache_coverage_percent,nprobe,max_candidates,workers,sample_index,cache_cohort_index,cache_cohort_size,cache_cohort_count,query_source_index,target_hot_set_member,latency_ms,recall_at_10,execution_engine,global_leaf_directory_reads,global_leaf_directory_bytes,global_leaf_code_pages_read,global_leaf_code_bytes,global_leaf_pages_read,global_leaf_page_bytes,global_leaf_waves,global_leaf_continuations,global_leaf_exact_scores,global_leaf_code_requests,global_leaf_exact_requests,global_leaf_exact_cells,global_leaf_exact_cards,global_leaf_deepest_winning_card_rank,global_leaf_exact_groups,global_leaf_exact_selected_bytes,global_leaf_exact_speculative_bytes,bytes_read,decoded_cache_hits,disk_cache_reads,backing_reads,decoded_cache_bytes_read,disk_cache_bytes_read,backing_bytes_read,network_gets,ram_budget_bytes,collection_resident_bytes,retained_bytes,retained_capacity_bytes,retained_peak_bytes,transient_bytes,transient_capacity_bytes,transient_peak_bytes,global_base_approximate_us,global_base_head_admission_us,global_base_head_fetch_us,global_base_head_read_attempts,global_base_head_read_successes,global_base_head_read_response_bytes,global_base_head_read_us_max,global_base_head_read_us_sum,global_base_head_read_queue_us_max,global_base_head_read_queue_us_sum,global_base_head_reads_over_20ms,global_base_head_reads_over_30ms,global_base_head_reads_over_50ms,global_base_head_reads_over_100ms,global_base_head_decode_admission_us,global_base_head_decode_us,global_base_exact_admission_us,global_base_exact_fetch_us,global_base_exact_read_attempts,global_base_exact_read_successes,global_base_exact_read_response_bytes,global_base_exact_read_queue_us_max,global_base_exact_read_queue_us_sum,global_base_exact_read_us_max,global_base_exact_read_us_sum,global_base_exact_reads_over_20ms,global_base_exact_reads_over_30ms,global_base_exact_reads_over_50ms,global_base_exact_reads_over_100ms,global_base_exact_cpu_us,global_base_exact_rerank_us";
const CACHE_COVERAGE_HEADER: &str = "schema_version,scan_codec,cache_execution,target_hot_query_fraction,repetition,cohort_position,query_class,query_index,execution_engine,observed_cache_tier,recall_at_10,latency_ms,segments_searched,global_leaf_directory_reads,global_leaf_directory_bytes,global_leaf_code_pages_read,global_leaf_code_bytes,global_leaf_pages_read,global_leaf_page_bytes,global_leaf_waves,global_leaf_continuations,global_leaf_exact_scores,decoded_cache_hits,disk_cache_reads,backing_reads,decoded_bytes_read,disk_bytes_read,backing_bytes_read,decoded_access_fraction,disk_access_fraction,backing_access_fraction,bytes_read,network_gets";
const BUILD_HEADER: &str = "logical_cell_catalog_checksum,logical_cells,logical_cell_dimensions,logical_cell_catalog_bytes,vector_element_type,scan_codec,turboquant_bits,turboquant_qjl_bits,turboquant_shards,build_layout,leaf_capability,segment_max_vectors,records,segment_bytes,vector_sidecar_bytes,graph_bytes,global_scan_bytes,total_active_index_bytes,bytes_per_vector,resident_bytes_estimate,ram_budget_bytes,collection_resident_bytes,retained_bytes,retained_capacity_bytes,retained_peak_bytes,transient_bytes,transient_capacity_bytes,transient_peak_bytes,ingest_ms,compaction_ms,compaction_bytes_read,compaction_bytes_written,gc_ms,gc_objects_scanned,gc_objects_deleted,gc_transaction_states_remaining,gc_bytes_read,gc_bytes_reclaimed,storage_gets,storage_puts,storage_deletes,storage_heads,storage_lists,storage_bytes_read,storage_bytes_written,configured_build_writers,ingest_batches,ingest_waves,ingest_vectors_per_s";
const WRITE_COST_HEADER: &str = "op,configured_writers,configured_batch_records,ops,batches,wall_ms,ops_per_s,mean_batch_ms,stddev_batch_ms,p50_batch_ms,p95_batch_ms,p99_batch_ms,max_batch_ms,mean_amortized_ms,gets,puts,deletes,heads,lists,bytes_read,bytes_written";
const WRITE_SAMPLE_HEADER: &str = "op,writer_index,wave_index,batch_index,batch_records,batch_latency_ms,amortized_ms,gets,puts,deletes,heads,lists";
const LIFECYCLE_HEADER: &str = "configured_writers,configured_batch_records,inserted_vectors,logical_vector_bytes,insert_wall_ms,insert_vectors_per_s,first_batch_publish_ms,searchability_refresh_ms,time_to_searchable_ms,searchable_samples,searchable_fraction,upsert_samples,upsert_correct_fraction,delete_samples,delete_absent_fraction,compact_delete_absent_fraction,purge_delete_absent_fraction,delta_flush_ms,time_to_fully_indexed_ms,wal_publish_bytes,indexed_delta_bytes,total_indexing_bytes,write_amplification,write_amplification_is_lower_bound,consolidation_ms,time_to_consolidated_ms,consolidated_global_bytes,consolidation_amplification";
const MUTATION_QUERY_HEADER: &str =
    "stage,queries,mean_ms,stddev_ms,p50_ms,p95_ms,p99_ms,max_ms,avg_bytes_read,avg_network_gets";
const MUTATION_QUERY_SAMPLE_HEADER: &str =
    "stage,sample_index,latency_ms,execution_engine,bytes_read,network_gets";
const MUTATION_QUERY_SAMPLES: usize = 100;
const V21_FEASIBILITY_SCHEMA: &str = "borsuk-v21-selector-feasibility-v1";
const V21_FEASIBILITY_ARMS_HEADER: &str = "schema,arm_index,bundle_row_limit,selector_span,hedge_delay_ms,bundle_count,region_count,projected_directory_bytes,replaced_v20_root_bytes,v20_root_checksum,baseline_rss_bytes,projected_query_transient_bytes,projected_peak_rss_bytes,gt_coverage,recall_at_10,maximum_actual_requests,maximum_physical_bytes,selector_within_frozen_cap,eligible,rows";
const V21_FEASIBILITY_SAMPLES_HEADER: &str = "schema,arm_index,query_index,query_source_index,routed_cells,selected_rows,selected_bundles,primary_requests,maximum_actual_requests,selected_bytes,physical_bytes,gt_hits,recall_hits,limiting_bound";
const V22_STAGE_L_SCHEMA: &str = "borsuk-v22-stage-l-layout-v1";
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
    build_writers: usize,
    lifecycle_writers: usize,
    lifecycle_insert_mode: LifecycleInsertMode,
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
    max_active_searches: usize,
    max_waiting_searches: usize,
    leaf_read_width: usize,
    max_inflight_leaf_reads: usize,
    max_parallel_decode_rank_tasks: usize,
    exact_read_max_physical_amplification: u64,
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
    lifecycle_only: bool,
    v21_feasibility: bool,
    v21_source_archive_sha256: Option<String>,
    v21_index_id: Option<String>,
    v21_dataset_id: Option<String>,
    v22_stage_l: bool,
    v22_source_archive_sha256: Option<String>,
    v22_index_id: Option<String>,
    v22_dataset_id: Option<String>,
    preload_serving: bool,
    _uri_temp: Option<tempfile::TempDir>,
    _cache_temp: Option<tempfile::TempDir>,
}

#[derive(Clone, Copy, Default)]
struct V21FeasibilityPhaseSelection {
    build_index: bool,
    build_only: bool,
    recall_only: bool,
    skip_recall: bool,
    read_only: bool,
    insert_only: bool,
    lifecycle_only: bool,
    ambient_nprobes: bool,
    ambient_candidates: bool,
    ambient_concurrency: bool,
    ambient_writes: bool,
    ambient_limit: bool,
}

fn v21_feasibility_arms() -> Vec<V21FeasibilityArm> {
    [128_u16, 256]
        .into_iter()
        .flat_map(|bundle_row_limit| {
            [32_u16, 64].into_iter().flat_map(move |selector_span| {
                [None, Some(20_u16), Some(35_u16)]
                    .into_iter()
                    .map(move |hedge_delay_ms| V21FeasibilityArm {
                        bundle_row_limit,
                        selector_span,
                        hedge_delay_ms,
                    })
            })
        })
        .collect()
}

fn validate_v21_feasibility_phase(
    enabled: bool,
    selection: V21FeasibilityPhaseSelection,
) -> io::Result<()> {
    if !enabled {
        return Ok(());
    }
    if selection.build_index
        || selection.build_only
        || selection.recall_only
        || !selection.skip_recall
        || !selection.read_only
        || selection.insert_only
        || selection.lifecycle_only
        || selection.ambient_nprobes
        || selection.ambient_candidates
        || selection.ambient_concurrency
        || selection.ambient_writes
        || selection.ambient_limit
    {
        return Err(invalid_input(
            "BORSUK_BENCH_V21_FEASIBILITY must be the sole phase selector for an existing immutable index",
        ));
    }
    Ok(())
}

fn validate_v21_evidence_identity_fields(
    source_archive_sha256: &str,
    index_id: &str,
    dataset_id: &str,
) -> io::Result<()> {
    let canonical_identifier = |value: &str| {
        !value.is_empty()
            && value.len() <= 128
            && value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"._-".contains(&byte))
    };
    if source_archive_sha256.len() != 64
        || !source_archive_sha256
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        || !canonical_identifier(index_id)
        || !canonical_identifier(dataset_id)
    {
        return Err(invalid_input(
            "V21 feasibility evidence identity is invalid",
        ));
    }
    Ok(())
}

#[derive(Serialize)]
struct EffectiveRuntimeFlowControl {
    schema_version: u8,
    disk_cache_max_bytes: u64,
    ram_budget_bytes: Option<u64>,
    max_active_searches: usize,
    max_waiting_searches: usize,
    leaf_read_width: usize,
    max_inflight_leaf_reads: usize,
    max_parallel_decode_rank_tasks: usize,
    exact_read_max_physical_amplification: u64,
    cpu_threads: usize,
    io_threads: usize,
    s3_get_concurrency: usize,
}

#[derive(Debug, Deserialize, PartialEq, Serialize)]
struct V21FeasibilitySummary {
    schema: String,
    claim_eligible: bool,
    dataset_name: String,
    dataset_id: String,
    index_id: String,
    source_archive_sha256: String,
    v20_root_checksum: String,
    dataset_rows: u64,
    dimensions: usize,
    query_seed: u64,
    query_source_indices: Vec<usize>,
    arm_count: usize,
    sample_count: usize,
    baseline_rss_bytes: u64,
    minimum_arm_gt_coverage: f64,
    minimum_arm_recall_at_10: f64,
    maximum_actual_requests: usize,
    maximum_physical_bytes: u64,
    eligible_arm_indexes: Vec<usize>,
    arms: Vec<V21FeasibilityArmSummary>,
}

#[derive(Debug, Deserialize, PartialEq, Serialize)]
struct V21FeasibilityArmSummary {
    arm_index: usize,
    bundle_row_limit: u16,
    selector_span: u16,
    hedge_delay_ms: Option<u16>,
    bundle_count: usize,
    region_count: usize,
    projected_directory_bytes: u64,
    replaced_v20_root_bytes: u64,
    selector_within_frozen_cap: bool,
    rows: u64,
    gt_coverage: f64,
    recall_at_10: f64,
    maximum_actual_requests: usize,
    maximum_physical_bytes: u64,
    projected_query_transient_bytes: u64,
    projected_peak_rss_bytes: u64,
    eligible: bool,
}

struct V21EvidenceIdentity<'a> {
    dataset_name: &'a str,
    dataset_id: &'a str,
    index_id: &'a str,
    source_archive_sha256: &'a str,
    dimensions: usize,
    dataset_rows: u64,
    query_seed: u64,
    query_source_indices: &'a [usize],
    baseline_rss_bytes: u64,
}

#[derive(Clone, Copy, Serialize)]
struct V22EvidenceIdentity<'a> {
    dataset_name: &'a str,
    dataset_id: &'a str,
    index_id: &'a str,
    source_archive_sha256: &'a str,
    dimensions: usize,
    dataset_rows: u64,
    query_seed: u64,
    query_source_indices: &'a [usize],
}

#[derive(Serialize)]
struct V22StageLEvidence<'a> {
    schema: &'static str,
    document_kind: &'static str,
    claim_eligible: bool,
    identity: V22EvidenceIdentity<'a>,
    report: &'a V22StageLReport,
}

#[derive(Debug, PartialEq, Serialize)]
struct V22StageLSummary {
    schema: &'static str,
    document_kind: &'static str,
    claim_eligible: bool,
    rows: u64,
    routing_cell_count: usize,
    queries: usize,
    arms: usize,
    eligible_arms: Vec<usize>,
    maximum_routed_rows: u64,
    maximum_primary_requests: usize,
    maximum_primary_physical_bytes: u64,
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
    disk_cache_reads: u128,
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
    global_leaf_exact_cells: usize,
    global_leaf_exact_cards: usize,
    global_leaf_deepest_winning_card_rank: usize,
    global_leaf_exact_groups: usize,
    global_leaf_exact_selected_bytes: u64,
    global_leaf_exact_speculative_bytes: u64,
    global_leaf_page_bytes: u64,
    global_leaf_waves: usize,
    global_leaf_continuations: usize,
    global_leaf_exact_scores: usize,
    bytes_read: u64,
    decoded_cache_hits: usize,
    disk_cache_reads: u64,
    backing_reads: u64,
    decoded_cache_bytes_read: u64,
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
    timings: QueryStageTimings,
}

impl ConcurrencyMeasurement {
    fn physical_exact_csv_fields(&self) -> String {
        format!(
            "{},{},{},{},{},{},{},{}",
            self.global_leaf_code_requests,
            self.global_leaf_exact_requests,
            self.global_leaf_exact_cells,
            self.global_leaf_exact_cards,
            self.global_leaf_deepest_winning_card_rank,
            self.global_leaf_exact_groups,
            self.global_leaf_exact_selected_bytes,
            self.global_leaf_exact_speculative_bytes,
        )
    }
}

#[derive(Clone, Copy, Default)]
struct QueryStageTimings {
    approximate_us: u64,
    head_admission_us: u64,
    head_fetch_us: u64,
    head_read_attempts: u64,
    head_read_successes: u64,
    head_read_response_bytes: u64,
    head_read_us_max: u64,
    head_read_us_sum: u64,
    head_read_queue_us_max: u64,
    head_read_queue_us_sum: u64,
    head_reads_over_20ms: u64,
    head_reads_over_30ms: u64,
    head_reads_over_50ms: u64,
    head_reads_over_100ms: u64,
    head_decode_admission_us: u64,
    head_decode_us: u64,
    exact_admission_us: u64,
    exact_fetch_us: u64,
    exact_read_attempts: u64,
    exact_read_successes: u64,
    exact_read_response_bytes: u64,
    exact_read_queue_us_max: u64,
    exact_read_queue_us_sum: u64,
    exact_read_us_max: u64,
    exact_read_us_sum: u64,
    exact_reads_over_20ms: u64,
    exact_reads_over_30ms: u64,
    exact_reads_over_50ms: u64,
    exact_reads_over_100ms: u64,
    exact_cpu_us: u64,
    exact_rerank_us: u64,
}

impl QueryStageTimings {
    fn from_report(report: &SearchReport) -> Self {
        Self {
            approximate_us: report.global_base_approximate_us,
            head_admission_us: report.global_base_head_admission_us,
            head_fetch_us: report.global_base_head_fetch_us,
            head_read_attempts: report.global_base_head_read_attempts,
            head_read_successes: report.global_base_head_read_successes,
            head_read_response_bytes: report.global_base_head_read_response_bytes,
            head_read_us_max: report.global_base_head_read_us_max,
            head_read_us_sum: report.global_base_head_read_us_sum,
            head_read_queue_us_max: report.global_base_head_read_queue_us_max,
            head_read_queue_us_sum: report.global_base_head_read_queue_us_sum,
            head_reads_over_20ms: report.global_base_head_reads_over_20ms,
            head_reads_over_30ms: report.global_base_head_reads_over_30ms,
            head_reads_over_50ms: report.global_base_head_reads_over_50ms,
            head_reads_over_100ms: report.global_base_head_reads_over_100ms,
            head_decode_admission_us: report.global_base_head_decode_admission_us,
            head_decode_us: report.global_base_head_decode_us,
            exact_admission_us: report.global_base_exact_admission_us,
            exact_fetch_us: report.global_base_exact_fetch_us,
            exact_read_attempts: report.global_base_exact_read_attempts,
            exact_read_successes: report.global_base_exact_read_successes,
            exact_read_response_bytes: report.global_base_exact_read_response_bytes,
            exact_read_queue_us_max: report.global_base_exact_read_queue_us_max,
            exact_read_queue_us_sum: report.global_base_exact_read_queue_us_sum,
            exact_read_us_max: report.global_base_exact_read_us_max,
            exact_read_us_sum: report.global_base_exact_read_us_sum,
            exact_reads_over_20ms: report.global_base_exact_reads_over_20ms,
            exact_reads_over_30ms: report.global_base_exact_reads_over_30ms,
            exact_reads_over_50ms: report.global_base_exact_reads_over_50ms,
            exact_reads_over_100ms: report.global_base_exact_reads_over_100ms,
            exact_cpu_us: report.global_base_exact_cpu_us,
            exact_rerank_us: report.global_base_exact_rerank_us,
        }
    }

    fn csv_fields(self) -> String {
        [
            self.approximate_us,
            self.head_admission_us,
            self.head_fetch_us,
            self.head_read_attempts,
            self.head_read_successes,
            self.head_read_response_bytes,
            self.head_read_us_max,
            self.head_read_us_sum,
            self.head_read_queue_us_max,
            self.head_read_queue_us_sum,
            self.head_reads_over_20ms,
            self.head_reads_over_30ms,
            self.head_reads_over_50ms,
            self.head_reads_over_100ms,
            self.head_decode_admission_us,
            self.head_decode_us,
            self.exact_admission_us,
            self.exact_fetch_us,
            self.exact_read_attempts,
            self.exact_read_successes,
            self.exact_read_response_bytes,
            self.exact_read_queue_us_max,
            self.exact_read_queue_us_sum,
            self.exact_read_us_max,
            self.exact_read_us_sum,
            self.exact_reads_over_20ms,
            self.exact_reads_over_30ms,
            self.exact_reads_over_50ms,
            self.exact_reads_over_100ms,
            self.exact_cpu_us,
            self.exact_rerank_us,
        ]
        .map(|value| value.to_string())
        .join(",")
    }
}

#[derive(Default)]
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
    global_leaf_code_requests: usize,
    global_leaf_exact_requests: usize,
    global_leaf_exact_cells: usize,
    global_leaf_exact_cards: usize,
    global_leaf_deepest_winning_card_rank: usize,
    global_leaf_exact_groups: usize,
    global_leaf_exact_selected_bytes: u64,
    global_leaf_exact_speculative_bytes: u64,
    execution_engine: String,
    collection_resident_bytes: u64,
    retained_bytes: u64,
    retained_capacity_bytes: u64,
    retained_peak_bytes: u64,
    transient_bytes: u64,
    transient_capacity_bytes: u64,
    transient_peak_bytes: u64,
    timings: QueryStageTimings,
}

impl QuerySummary {
    fn push(&mut self, elapsed_ms: f64, report: &SearchReport, recall: Option<f32>) {
        // Query-scoped tier counters are authoritative under parallel segment
        // reads. Summing per-segment logical byte totals can count overlapping
        // work more than once.
        let measured_bytes_read = query_scoped_physical_bytes_read(
            report.decoded_cache_bytes_read,
            report.disk_cache_bytes_read,
            report.backing_bytes_read,
        );
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
            global_leaf_exact_cells: report.global_leaf_exact_cells,
            global_leaf_exact_cards: report.global_leaf_exact_cards,
            global_leaf_deepest_winning_card_rank: report.global_leaf_deepest_winning_card_rank,
            global_leaf_exact_groups: report.global_leaf_exact_groups,
            global_leaf_exact_selected_bytes: report.global_leaf_exact_selected_bytes,
            global_leaf_exact_speculative_bytes: report.global_leaf_exact_speculative_bytes,
            global_leaf_page_bytes: report.global_leaf_page_bytes,
            global_leaf_waves: report.global_leaf_waves,
            global_leaf_continuations: report.global_leaf_continuations,
            global_leaf_exact_scores: report.global_leaf_exact_scores,
            bytes_read: measured_bytes_read,
            decoded_cache_hits: report.decoded_cache_hits,
            disk_cache_reads: report.disk_cache_reads,
            backing_reads: report.backing_reads,
            decoded_cache_bytes_read: report.decoded_cache_bytes_read,
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
            timings: QueryStageTimings::from_report(report),
        });
        if let Some(recall) = recall {
            self.recall_sum += f64::from(recall);
            self.recall_count += 1;
        }
        self.bytes_read += u128::from(measured_bytes_read);
        self.billable_requests +=
            u128::from(report.requests.gets.saturating_add(report.requests.heads));
        self.disk_cache_reads += u128::from(report.disk_cache_reads);
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
        self.disk_cache_reads += other.disk_cache_reads;
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

fn query_scoped_physical_bytes_read(decoded: u64, disk: u64, backing: u64) -> u64 {
    decoded.saturating_add(disk).saturating_add(backing)
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
    writer_index: usize,
    wave_index: usize,
    batch_index: usize,
    batch_records: usize,
    batch_latency_ms: f64,
    requests: RequestCounts,
}

struct PreparedRecordBatch {
    assignment: LifecycleBatchAssignment,
    records: Vec<VectorRecord>,
}

#[derive(Default)]
struct BuildIngestReport {
    batches: usize,
    rows: usize,
    waves: usize,
    materializations: usize,
    materializer_opens: usize,
    requests: RequestCounts,
    bytes_read: u64,
    bytes_written: u64,
}

struct BuildIngestCoordinator {
    writers: Vec<BorsukIndex>,
    uri: String,
    materializer_options: OpenOptions,
    pending: Vec<(u8, usize, Vec<Vec<f32>>)>,
    batches_since_materialization: usize,
    next_start: usize,
    report: BuildIngestReport,
}

impl BuildIngestCoordinator {
    fn open(uri: &str, writer_count: usize, ram_budget_bytes: Option<u64>) -> BenchResult<Self> {
        let writer_count = validate_build_writers(writer_count)?;
        let anchor =
            BorsukIndex::open_with_options(uri, lifecycle_writer_open_options(ram_budget_bytes))?;
        let open_requests = anchor.request_counts();
        let open_bytes_read = anchor.backing_bytes_read();
        let open_bytes_written = anchor.put_payload_bytes();
        // Build lanes have statically disjoint source shards, so sharing one
        // pinned appender is contention-neutral and avoids W resident copies.
        let writers = (0..writer_count)
            .map(|_| anchor.clone_for_coordinated_bulk_writer())
            .collect();
        Ok(Self {
            writers,
            uri: uri.to_owned(),
            materializer_options: lifecycle_writer_open_options(ram_budget_bytes),
            pending: Vec::with_capacity(writer_count),
            batches_since_materialization: 0,
            next_start: 0,
            report: BuildIngestReport {
                requests: open_requests,
                bytes_read: open_bytes_read,
                bytes_written: open_bytes_written,
                ..BuildIngestReport::default()
            },
        })
    }

    fn push(&mut self, start: usize, vectors: Vec<Vec<f32>>) -> BenchResult<()> {
        if vectors.is_empty() || start != self.next_start {
            return Err(
                invalid_input("bulk ingest batches must be nonempty and contiguous").into(),
            );
        }
        self.next_start = self.next_start.saturating_add(vectors.len());
        let source_shard = u8::try_from(
            self.batches_since_materialization
                .saturating_add(self.pending.len()),
        )
        .map_err(|_| invalid_input("bulk ingest source-shard window exceeds u8"))?;
        self.pending.push((source_shard, start, vectors));
        if self.pending.len() == self.writers.len()
            || self
                .batches_since_materialization
                .saturating_add(self.pending.len())
                == BULK_LOAD_SOURCE_SHARDS
        {
            self.flush_pending()?;
        }
        Ok(())
    }

    #[cfg(test)]
    fn pending_batches(&self) -> usize {
        self.pending.len()
    }

    fn finish(mut self) -> BenchResult<BuildIngestReport> {
        self.flush_pending()?;
        for writer in &self.writers {
            add_request_counts(&mut self.report.requests, writer.request_counts());
            self.report.bytes_read = self
                .report
                .bytes_read
                .saturating_add(writer.backing_bytes_read());
            self.report.bytes_written = self
                .report
                .bytes_written
                .saturating_add(writer.put_payload_bytes());
        }
        Ok(self.report)
    }

    fn flush_pending(&mut self) -> BenchResult<()> {
        if self.pending.is_empty() {
            return Ok(());
        }
        let completed =
            execute_bulk_add_wave(&mut self.writers, std::mem::take(&mut self.pending))?;
        self.report.waves = self.report.waves.saturating_add(1);
        for rows in completed {
            self.report.batches = self.report.batches.saturating_add(1);
            self.report.rows = self.report.rows.saturating_add(rows);
            self.batches_since_materialization =
                self.batches_since_materialization.saturating_add(1);
        }
        if self.batches_since_materialization == BULK_LOAD_SOURCE_SHARDS {
            // Open only after the joined positioned prefix exists. The read
            // runtime partitions its RAM budget from the resident authority
            // visible at open; retaining an empty-index handle would freeze
            // that partition below the later materialized manifest size.
            let mut materializer =
                BorsukIndex::open_with_options(&self.uri, self.materializer_options.clone())?;
            self.report.materializer_opens = self.report.materializer_opens.saturating_add(1);
            materializer.flush()?;
            add_request_counts(&mut self.report.requests, materializer.request_counts());
            self.report.bytes_read = self
                .report
                .bytes_read
                .saturating_add(materializer.backing_bytes_read());
            self.report.bytes_written = self
                .report
                .bytes_written
                .saturating_add(materializer.put_payload_bytes());
            self.report.materializations = self.report.materializations.saturating_add(1);
            self.batches_since_materialization = 0;
        }
        Ok(())
    }
}

fn lifecycle_progress_line(stage: &str, status: &str, elapsed_ms: u128) -> String {
    let valid_stage = !stage.is_empty()
        && stage.len() <= 64
        && stage
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'));
    if !valid_stage || !matches!(status, "start" | "complete") {
        return String::new();
    }
    format!("BORSUK_LIFECYCLE_PROGRESS stage={stage} status={status} elapsed_ms={elapsed_ms}")
}

struct LifecycleQueryProgress<'a> {
    sample: usize,
    elapsed_us: u128,
    engine: &'a str,
    termination: &'a str,
    backing_reads: u64,
    backing_bytes: u64,
    code_bytes: u64,
    exact_bytes: u64,
}

fn lifecycle_query_progress_line(progress: LifecycleQueryProgress<'_>) -> String {
    let LifecycleQueryProgress {
        sample,
        elapsed_us,
        engine,
        termination,
        backing_reads,
        backing_bytes,
        code_bytes,
        exact_bytes,
    } = progress;
    format!(
        "BORSUK_LIFECYCLE_QUERY sample={sample} elapsed_us={elapsed_us} engine={engine} termination={termination} backing_reads={backing_reads} backing_bytes={backing_bytes} code_bytes={code_bytes} exact_bytes={exact_bytes}",
    )
}

struct LifecycleProgress {
    stage: &'static str,
    started: Option<Instant>,
}

static LIFECYCLE_PROGRESS_ENABLED: OnceLock<bool> = OnceLock::new();

impl LifecycleProgress {
    fn start(stage: &'static str) -> Self {
        let enabled = *LIFECYCLE_PROGRESS_ENABLED.get_or_init(|| {
            env::var_os("BORSUK_LIFECYCLE_PROGRESS").is_some_and(|value| value == "1")
        });
        if enabled {
            eprintln!("{}", lifecycle_progress_line(stage, "start", 0));
        }
        Self {
            stage,
            started: enabled.then(Instant::now),
        }
    }

    fn complete(self) {
        if let Some(started) = self.started {
            eprintln!(
                "{}",
                lifecycle_progress_line(self.stage, "complete", started.elapsed().as_millis())
            );
        }
    }
}

fn lifecycle_phase<T>(
    stage: &'static str,
    operation: impl FnOnce() -> BenchResult<T>,
) -> BenchResult<T> {
    let progress = LifecycleProgress::start(stage);
    let result = operation()?;
    progress.complete();
    Ok(result)
}

fn open_lifecycle_writer_handles(
    uri: &str,
    writer_count: usize,
    ram_budget_bytes: Option<u64>,
) -> BenchResult<Vec<BorsukIndex>> {
    if writer_count == 0 {
        return Err(invalid_input("lifecycle writer count must be positive").into());
    }
    // The library RAM budget is per handle. Lifecycle scaling deliberately
    // opens W independent appenders so remote head contention remains part of
    // the measurement; the campaign cgroup separately attests process peak.
    (0..writer_count)
        .map(|_| {
            BorsukIndex::open_with_options(uri, lifecycle_writer_open_options(ram_budget_bytes))
                .map_err(Into::into)
        })
        .collect()
}

fn mutable_resident_metadata_budget(ram_budget_bytes: Option<u64>) -> Option<u64> {
    ram_budget_bytes.map(|bytes| bytes / 2)
}

fn serving_memory_partition(ram_budget_bytes: Option<u64>) -> (Option<u64>, Option<u64>) {
    const MIB: u64 = 1024 * 1024;
    const PREPARED_PLANE_BUDGET_THRESHOLD: u64 = 3 * 1024 * MIB;
    const PREPARED_PLANE_TRANSIENT_FLOOR: u64 = 768 * MIB;
    ram_budget_bytes.map_or((None, None), |bytes| {
        (
            Some(bytes / 4),
            (bytes >= PREPARED_PLANE_BUDGET_THRESHOLD).then_some(PREPARED_PLANE_TRANSIENT_FLOOR),
        )
    })
}

fn lifecycle_writer_open_options(ram_budget_bytes: Option<u64>) -> OpenOptions {
    OpenOptions {
        ram_budget_bytes,
        resident_metadata_max_bytes: mutable_resident_metadata_budget(ram_budget_bytes),
        // Writer handles are long-lived metadata/WAL clients, not serving
        // caches. Retaining an independent copy of every read cache per
        // concurrent writer would make process memory scale with W.
        routing_page_cache_max_bytes: 0,
        tombstone_page_cache_max_bytes: 0,
        bm25_stats_page_cache_max_bytes: 0,
        lexical_run_cache_max_bytes: 0,
        lexical_term_page_cache_max_bytes: 0,
        late_interaction_batch_cache_max_bytes: 0,
        wal_tail_cache_max_bytes: 0,
        ..OpenOptions::default()
    }
}

fn reopen_build_finalizer(
    index: &mut BorsukIndex,
    uri: &str,
    ram_budget_bytes: Option<u64>,
    report: &mut BuildIngestReport,
) -> BenchResult<()> {
    // The creator runtime was partitioned against the empty manifest. Preserve
    // its construction counters, then reopen from the newly materialized
    // authority so finalization uses the same explicit build partition as the
    // boundary materializer.
    add_request_counts(&mut report.requests, index.request_counts());
    report.bytes_read = report.bytes_read.saturating_add(index.backing_bytes_read());
    report.bytes_written = report
        .bytes_written
        .saturating_add(index.put_payload_bytes());
    *index = BorsukIndex::open_with_options(uri, lifecycle_writer_open_options(ram_budget_bytes))?;
    Ok(())
}

fn validate_claim_free_lifecycle_insert(index: &BorsukIndex) -> BenchResult<()> {
    let collection = &index.manifest().config;
    if collection.text || !collection.named_vectors.is_empty() {
        return Err(invalid_input(
            "claim-free lifecycle inserts require a primary-dense-only collection; multimodal inserts must use a separately labelled upsert profile",
        )
        .into());
    }
    Ok(())
}

#[derive(Clone, Copy)]
enum LifecycleRecordMutation {
    Put,
    Upsert,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LifecycleInsertMode {
    GeneralUpsert,
    ClaimFreePut,
}

impl LifecycleInsertMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::GeneralUpsert => "general-upsert",
            Self::ClaimFreePut => "claim-free-put",
        }
    }
}

fn execute_put_wave(
    op: &'static str,
    wave_index: usize,
    writers: &mut [BorsukIndex],
    batches: Vec<PreparedRecordBatch>,
) -> BenchResult<Vec<(WriteSample, u64)>> {
    for writer in writers.iter() {
        validate_claim_free_lifecycle_insert(writer)?;
    }
    execute_record_wave(
        op,
        wave_index,
        writers,
        batches,
        LifecycleRecordMutation::Put,
    )
}

fn execute_bulk_add_wave(
    writers: &mut [BorsukIndex],
    batches: Vec<(u8, usize, Vec<Vec<f32>>)>,
) -> BenchResult<Vec<usize>> {
    if batches.len() > writers.len() {
        return Err(invalid_input("bulk ingest wave exceeds its configured writer count").into());
    }
    std::thread::scope(|scope| -> BenchResult<Vec<usize>> {
        let mut joins = Vec::with_capacity(batches.len());
        for (writer, (source_shard, start, vectors)) in writers.iter_mut().zip(batches) {
            joins.push(scope.spawn(move || -> borsuk::Result<usize> {
                let rows = vectors.len();
                let ids = benchmark_row_ids(start, rows);
                let inserted_ids = writer.bulk_load_vectors_with_unique_ids_on_source_shard(
                    source_shard,
                    vectors,
                    ids,
                )?;
                validate_generated_id_range(start, start.saturating_add(rows), &inserted_ids)
                    .map_err(|error| borsuk::BorsukError::InvalidStorage(error.to_string()))?;
                Ok(rows)
            }));
        }
        joins
            .into_iter()
            .map(|join| {
                join.join()
                    .map_err(|_| io::Error::other("bulk ingest writer thread panicked"))?
                    .map_err(Into::into)
            })
            .collect()
    })
}

fn execute_upsert_wave(
    op: &'static str,
    wave_index: usize,
    writers: &mut [BorsukIndex],
    batches: Vec<PreparedRecordBatch>,
) -> BenchResult<Vec<(WriteSample, u64)>> {
    execute_record_wave(
        op,
        wave_index,
        writers,
        batches,
        LifecycleRecordMutation::Upsert,
    )
}

fn execute_record_wave(
    op: &'static str,
    wave_index: usize,
    writers: &mut [BorsukIndex],
    batches: Vec<PreparedRecordBatch>,
    mutation: LifecycleRecordMutation,
) -> BenchResult<Vec<(WriteSample, u64)>> {
    if batches.len() > writers.len()
        || batches
            .iter()
            .enumerate()
            .any(|(writer_index, batch)| batch.assignment.writer_index != writer_index)
    {
        return Err(invalid_input("lifecycle write wave is not canonically assigned").into());
    }
    std::thread::scope(|scope| -> BenchResult<Vec<(WriteSample, u64)>> {
        let mut joins = Vec::with_capacity(batches.len());
        for (writer, prepared) in writers.iter_mut().zip(batches) {
            joins.push(scope.spawn(move || -> borsuk::Result<(WriteSample, u64)> {
                let requests_before = writer.request_counts();
                let bytes_before = writer.put_payload_bytes();
                let batch_started = Instant::now();
                let _ = match mutation {
                    LifecycleRecordMutation::Put => writer.put_with_report(prepared.records),
                    LifecycleRecordMutation::Upsert => writer.upsert_with_report(prepared.records),
                }?;
                Ok((
                    WriteSample {
                        op,
                        writer_index: prepared.assignment.writer_index,
                        wave_index,
                        batch_index: prepared.assignment.batch_index,
                        batch_records: prepared.assignment.len,
                        batch_latency_ms: elapsed_ms(batch_started),
                        requests: writer.request_counts().delta(&requests_before),
                    },
                    writer.put_payload_bytes().saturating_sub(bytes_before),
                ))
            }));
        }
        joins
            .into_iter()
            .map(|join| {
                join.join()
                    .map_err(|_| io::Error::other("lifecycle writer thread panicked"))?
                    .map_err(Into::into)
            })
            .collect()
    })
}

fn execute_delete_wave(
    wave_index: usize,
    writers: &mut [BorsukIndex],
    batches: Vec<(LifecycleBatchAssignment, Vec<String>)>,
) -> BenchResult<Vec<(WriteSample, u64)>> {
    if batches.len() > writers.len()
        || batches
            .iter()
            .enumerate()
            .any(|(writer_index, (assignment, _))| assignment.writer_index != writer_index)
    {
        return Err(invalid_input("lifecycle delete wave is not canonically assigned").into());
    }
    std::thread::scope(|scope| -> BenchResult<Vec<(WriteSample, u64)>> {
        let mut joins = Vec::with_capacity(batches.len());
        for (writer, (assignment, ids)) in writers.iter_mut().zip(batches) {
            joins.push(scope.spawn(move || -> borsuk::Result<(WriteSample, u64)> {
                let requests_before = writer.request_counts();
                let bytes_before = writer.put_payload_bytes();
                let batch_started = Instant::now();
                let report = writer.delete(ids)?;
                Ok((
                    WriteSample {
                        op: "delete",
                        writer_index: assignment.writer_index,
                        wave_index,
                        batch_index: assignment.batch_index,
                        batch_records: report.ids_submitted,
                        batch_latency_ms: elapsed_ms(batch_started),
                        requests: writer.request_counts().delta(&requests_before),
                    },
                    writer.put_payload_bytes().saturating_sub(bytes_before),
                ))
            }));
        }
        joins
            .into_iter()
            .map(|join| {
                join.join()
                    .map_err(|_| io::Error::other("lifecycle writer thread panicked"))?
                    .map_err(Into::into)
            })
            .collect()
    })
}

fn request_counts_from_samples(samples: &[WriteSample]) -> RequestCounts {
    samples
        .iter()
        .fold(RequestCounts::default(), |mut total, sample| {
            total.gets = total.gets.saturating_add(sample.requests.gets);
            total.puts = total.puts.saturating_add(sample.requests.puts);
            total.deletes = total.deletes.saturating_add(sample.requests.deletes);
            total.heads = total.heads.saturating_add(sample.requests.heads);
            total.lists = total.lists.saturating_add(sample.requests.lists);
            total
        })
}

fn add_request_counts(total: &mut RequestCounts, addition: RequestCounts) {
    total.gets = total.gets.saturating_add(addition.gets);
    total.puts = total.puts.saturating_add(addition.puts);
    total.deletes = total.deletes.saturating_add(addition.deletes);
    total.heads = total.heads.saturating_add(addition.heads);
    total.lists = total.lists.saturating_add(addition.lists);
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
    ingest_batches: usize,
    ingest_waves: usize,
    layout: &'static str,
    ingest_ms: f64,
    compaction_ms: f64,
    compaction_bytes_read: u64,
    compaction_bytes_written: u64,
    gc_ms: f64,
    gc_objects_scanned: usize,
    gc_objects_deleted: usize,
    gc_transaction_states_remaining: usize,
    gc_bytes_read: u64,
    gc_bytes_reclaimed: u64,
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

struct BuildFinalization {
    layout: &'static str,
    compaction_ms: f64,
    compaction_bytes_read: u64,
    compaction_bytes_written: u64,
    gc_ms: f64,
    garbage_collection: GarbageCollectionReport,
}

fn finalize_fresh_build(
    index: &mut BorsukIndex,
    recluster_build: bool,
) -> BenchResult<BuildFinalization> {
    let compaction_started = Instant::now();
    let (layout, compaction_bytes_read, compaction_bytes_written) = if recluster_build {
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

    // The build prefix is quiescent and owned by this one builder. Reclaim
    // committed transaction controls and superseded immutable artifacts before
    // it becomes the frozen runtime base, so time-to-ready and S3 footprint are
    // both measured rather than hidden behind a looser evidence cap.
    let gc_started = Instant::now();
    let garbage_collection = index.gc_obsolete_segments_quiescent(GarbageCollectionOptions {
        dry_run: false,
        min_age: Duration::ZERO,
    })?;
    let gc_ms = elapsed_ms(gc_started);

    Ok(BuildFinalization {
        layout,
        compaction_ms,
        compaction_bytes_read,
        compaction_bytes_written,
        gc_ms,
        garbage_collection,
    })
}

fn main() {
    if let Err(error) = run() {
        eprintln!("production_bench: {error}");
        std::process::exit(1);
    }
}

fn run() -> BenchResult<()> {
    configure_benchmark_process()?;
    let config = resolve_config()?;
    if config.v21_feasibility {
        reject_existing_destinations(&v21_feasibility_destinations(&config.output_dir))?;
    }
    if config.v22_stage_l {
        reject_existing_destinations(&v22_stage_l_destinations(&config.output_dir))?;
    }
    print_config(&config);
    let dataset = load_dataset(&config)?;
    if config.v21_feasibility {
        write_v21_feasibility_artifacts(&config, &dataset)?;
        return Ok(());
    }
    if config.v22_stage_l {
        write_v22_stage_l_artifacts(&config, &dataset)?;
        return Ok(());
    }
    fs::create_dir_all(&config.output_dir)?;
    write_effective_runtime_flow_control(&config)?;

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
        let ingest = ingest_train(
            &mut index,
            &config.uri,
            config.build_writers,
            config.ram_budget_bytes,
            &config.dataset_dir,
            &dataset,
        )?;
        let ingest_ms = elapsed_ms(ingest_started);
        borsuk::report_build_timing("ingest")?;

        // Compare the low-memory ingest layout against an explicitly reclustered
        // layout. Both produce the same global product-PQ shortlist and recall;
        // reclustering may reduce exact-rerank GETs by colocating candidates.
        let finalization = finalize_fresh_build(&mut index, config.recluster_build)?;
        eprintln!(
            "build dataset={} records={} build_writers={} ingest_batches={} ingest_waves={} ingest_materializations={} ingest_ms={ingest_ms:.3} compaction_ms={:.3} compaction_bytes_read={} compaction_bytes_written={} gc_ms={:.3} gc_objects_scanned={} gc_objects_deleted={} gc_transaction_states_remaining={} gc_bytes_read={} gc_bytes_reclaimed={}",
            dataset.meta.name,
            dataset.train_count,
            config.build_writers,
            ingest.batches,
            ingest.waves,
            ingest.materializations,
            finalization.compaction_ms,
            finalization.compaction_bytes_read,
            finalization.compaction_bytes_written,
            finalization.gc_ms,
            finalization.garbage_collection.objects_scanned,
            finalization.garbage_collection.objects_deleted,
            finalization.garbage_collection.transaction_states_remaining,
            finalization.garbage_collection.bytes_read,
            finalization.garbage_collection.bytes_reclaimed,
        );
        let stats = index.stats();
        let mut storage_requests = index.request_counts();
        add_request_counts(&mut storage_requests, ingest.requests);
        let storage_bytes_read = index.backing_bytes_read().saturating_add(ingest.bytes_read);
        let storage_bytes_written = index
            .put_payload_bytes()
            .saturating_add(ingest.bytes_written);
        let build = BuildMeasurement {
            logical_cell_catalog_checksum: catalog_evidence.0,
            logical_cells: catalog_evidence.1,
            logical_cell_dimensions: catalog_evidence.2,
            logical_cell_catalog_bytes: catalog_evidence.3,
            ingest_batches: ingest.batches,
            ingest_waves: ingest.waves,
            layout: finalization.layout,
            ingest_ms,
            compaction_ms: finalization.compaction_ms,
            compaction_bytes_read: finalization.compaction_bytes_read,
            compaction_bytes_written: finalization.compaction_bytes_written,
            gc_ms: finalization.gc_ms,
            gc_objects_scanned: finalization.garbage_collection.objects_scanned,
            gc_objects_deleted: finalization.garbage_collection.objects_deleted,
            gc_transaction_states_remaining: finalization
                .garbage_collection
                .transaction_states_remaining,
            gc_bytes_read: finalization.garbage_collection.bytes_read,
            gc_bytes_reclaimed: finalization.garbage_collection.bytes_reclaimed,
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
        let write_ops = write_operation_count(dataset.train_count, config.write_ops)?;
        let mut writers = open_lifecycle_writer_handles(
            &config.uri,
            config.lifecycle_writers,
            config.ram_budget_bytes,
        )?;
        let insert = measure_inserts(&config, &dataset, &mut writers, write_ops)?;
        let observer = BorsukIndex::open_with_options(
            &config.uri,
            lifecycle_writer_open_options(config.ram_budget_bytes),
        )?;
        let (samples, visible) = verify_insert_visibility(&config, &dataset, &observer, write_ops)?;
        drop(observer);
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

    if config.lifecycle_only {
        reset_cache(&config.cache_dir)?;
        let mut write_index = open_mutable_index(&config)?;
        write_write_costs_csv(&config, &dataset, &mut write_index)?;
        return Ok(());
    }

    if !config.skip_recall && config.cache_profile != BenchmarkCacheProfile::MixedCoverage {
        // The recall writer owns every serving handle. A disk-cached cohort can
        // therefore reset only after the previous measurement handle is gone.
        write_recall_latency_csv(&config, &dataset)?;
    }
    if config.recall_only {
        return Ok(());
    }

    reset_cache(&config.cache_dir)?;
    let open_started = Instant::now();
    let reader = Arc::new(open_serving_index(
        &config,
        shared_serving_metadata_preparation(config.cache_profile),
    )?);
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
    // Startup/preload is a separate observation. Drop its full-budget handle
    // before cache-state or concurrency cohorts reset and reopen local cache.
    drop(reader);
    if cache_state_summary_enabled(config.skip_recall, config.cache_profile) {
        write_cold_warm_csv(&config, &dataset)?;
    }
    write_concurrency_csv(&config, &dataset)?;
    write_cache_coverage_csv(&config, &dataset)?;
    if config.read_only {
        return Ok(());
    }

    // Read measurements are complete. Open a new isolated mutable handle for
    // the write phase instead of carrying read-cache authority across phases.
    let mut write_index = open_mutable_index(&config)?;
    write_write_costs_csv(&config, &dataset, &mut write_index)?;
    Ok(())
}

fn configure_benchmark_process() -> BenchResult<()> {
    let defaults = ProcessLimits::default();
    configure_process(ProcessLimits {
        cpu_threads: env_usize(CPU_THREADS_ENV, defaults.cpu_threads)?,
        io_threads: env_usize(IO_THREADS_ENV, defaults.io_threads)?,
        s3_get_concurrency: env_usize(BACKING_GET_CONCURRENCY_ENV, defaults.s3_get_concurrency)?,
    })?;
    Ok(())
}

fn write_effective_runtime_flow_control(config: &ResolvedConfig) -> BenchResult<()> {
    let path = config.output_dir.join("bench_runtime_flow_control.json");
    write_runtime_flow_control_receipt(
        &path,
        &EffectiveRuntimeFlowControl {
            schema_version: 4,
            disk_cache_max_bytes: config.disk_cache_max_bytes.unwrap_or(0),
            ram_budget_bytes: config.ram_budget_bytes,
            max_active_searches: config.max_active_searches,
            max_waiting_searches: config.max_waiting_searches,
            leaf_read_width: config.leaf_read_width,
            max_inflight_leaf_reads: config.max_inflight_leaf_reads,
            max_parallel_decode_rank_tasks: config.max_parallel_decode_rank_tasks,
            exact_read_max_physical_amplification: config.exact_read_max_physical_amplification,
            cpu_threads: configured_cpu_threads(),
            io_threads: configured_io_threads(),
            s3_get_concurrency: configured_backing_get_concurrency(),
        },
    )
}

fn write_runtime_flow_control_receipt(
    path: &Path,
    value: &EffectiveRuntimeFlowControl,
) -> BenchResult<()> {
    let mut output = BufWriter::new(File::create(path)?);
    output.write_all(&effective_runtime_flow_control_json_line(value)?)?;
    output.flush()?;
    Ok(())
}

fn effective_runtime_flow_control_json_line(
    value: &EffectiveRuntimeFlowControl,
) -> BenchResult<Vec<u8>> {
    // Publication V3 deliberately limits this persisted receipt to integer and
    // null values, whose serde_json spelling is identical to Python's strict
    // canonical_json_bytes contract after lexicographic key ordering.
    Ok(canonical_json_line(value)?)
}

fn canonical_json_value(value: serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Array(values) => {
            serde_json::Value::Array(values.into_iter().map(canonical_json_value).collect())
        }
        serde_json::Value::Object(values) => {
            let ordered = values
                .into_iter()
                .map(|(key, value)| (key, canonical_json_value(value)))
                .collect::<BTreeMap<_, _>>();
            serde_json::Value::Object(ordered.into_iter().collect())
        }
        scalar => scalar,
    }
}

fn canonical_json_line(value: &impl Serialize) -> io::Result<Vec<u8>> {
    let value = canonical_json_value(serde_json::to_value(value)?);
    let mut bytes = serde_json::to_vec(&value)?;
    bytes.push(b'\n');
    Ok(bytes)
}

fn validate_exact_read_max_physical_amplification(value: usize) -> BenchResult<u64> {
    if !(1..=5).contains(&value) {
        return Err(invalid_input(
            "BORSUK_BENCH_EXACT_READ_MAX_PHYSICAL_AMPLIFICATION must be in 1..=5",
        )
        .into());
    }
    Ok(value as u64)
}

fn validate_max_parallel_decode_rank_tasks(value: usize) -> BenchResult<usize> {
    if value == 0 {
        return Err(invalid_input(
            "BORSUK_BENCH_MAX_PARALLEL_DECODE_RANK_TASKS must be greater than zero",
        )
        .into());
    }
    Ok(value)
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

fn recall_cache_profile_needs_outer_handle(
    cache_profile: BenchmarkCacheProfile,
    preload: bool,
    cache_execution: CacheExecutionPolicy,
    leaf_mode: LeafMode,
    segment_cache_max_bytes: Option<u64>,
) -> bool {
    cache_profile == BenchmarkCacheProfile::All
        && (uses_memory_preloaded_phase(preload, cache_execution, true)
            || uses_bounded_decoded_cache_phases(false, leaf_mode, segment_cache_max_bytes))
}

fn effective_segment_cache_budget(config: &ResolvedConfig) -> Option<u64> {
    config.segment_cache_max_bytes.or_else(|| {
        config
            .preload_serving
            .then_some(config.ram_budget_bytes.unwrap_or(u64::MAX))
    })
}

fn serving_cache_dir(cache_dir: &Path, disk_cache_max_bytes: Option<u64>) -> Option<PathBuf> {
    disk_cache_max_bytes.map(|_| cache_dir.to_path_buf())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ServingMetadataPreparation {
    DeferredCodePlanes,
    Complete,
}

impl ServingMetadataPreparation {
    fn complete_code_planes(self) -> bool {
        self == Self::Complete
    }

    fn code_plane_cache_max_bytes(self) -> u64 {
        match self {
            Self::DeferredCodePlanes => 0,
            Self::Complete => DEFAULT_CELL_CARD_CODE_PLANE_CACHE_BYTES,
        }
    }
}

fn shared_serving_metadata_preparation(
    cache_profile: BenchmarkCacheProfile,
) -> ServingMetadataPreparation {
    match cache_profile {
        BenchmarkCacheProfile::All | BenchmarkCacheProfile::DiskCached => {
            ServingMetadataPreparation::Complete
        }
        BenchmarkCacheProfile::Uncached | BenchmarkCacheProfile::MixedCoverage => {
            ServingMetadataPreparation::DeferredCodePlanes
        }
    }
}

fn open_serving_index(
    config: &ResolvedConfig,
    preparation: ServingMetadataPreparation,
) -> BenchResult<BorsukIndex> {
    let (resident_metadata_max_bytes, transient_ram_min_bytes) =
        serving_memory_partition(config.ram_budget_bytes);
    open_benchmark_index(
        config,
        resident_metadata_max_bytes,
        transient_ram_min_bytes,
        preparation,
    )
}

fn open_mutable_index(config: &ResolvedConfig) -> BenchResult<BorsukIndex> {
    open_benchmark_index(
        config,
        mutable_resident_metadata_budget(config.ram_budget_bytes),
        None,
        ServingMetadataPreparation::Complete,
    )
}

fn open_benchmark_index(
    config: &ResolvedConfig,
    resident_metadata_max_bytes: Option<u64>,
    transient_ram_min_bytes: Option<u64>,
    preparation: ServingMetadataPreparation,
) -> BenchResult<BorsukIndex> {
    let index = BorsukIndex::open_with_options(
        &config.uri,
        OpenOptions {
            cache_dir: serving_cache_dir(&config.cache_dir, config.disk_cache_max_bytes),
            cache_max_bytes: config.disk_cache_max_bytes,
            ram_budget_bytes: config.ram_budget_bytes,
            resident_metadata_max_bytes,
            transient_ram_min_bytes,
            segment_cache_max_bytes: effective_segment_cache_budget(config),
            cell_card_code_plane_cache_max_bytes: preparation.code_plane_cache_max_bytes(),
            // Routing summaries and the centroid graph are serving metadata.
            // Load/build them during open so neither cache-state measurement
            // charges one-time library initialization to the first query.
            resident_routing: true,
            max_active_searches: config.max_active_searches,
            max_waiting_searches: config.max_waiting_searches,
            leaf_read_width: config.leaf_read_width,
            max_inflight_leaf_reads: config.max_inflight_leaf_reads,
            max_parallel_decode_rank_tasks: config.max_parallel_decode_rank_tasks,
            exact_read_max_physical_amplification: config.exact_read_max_physical_amplification,
            ..OpenOptions::default()
        },
    )?;
    if config.serving_mode == ServingMode::Hybrid
        && config.serving_leaf_mode == config.global_scan_codec.leaf_mode()
    {
        if preparation.complete_code_planes() {
            let _ = index.prepare_serving_metadata()?;
        } else {
            let _ = index.prepare_serving_metadata_without_complete_code_planes()?;
        }
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
    let build_writers = validate_build_writers(env_usize(
        "BORSUK_BENCH_BUILD_WRITERS",
        DEFAULT_BUILD_WRITERS,
    )?)?;
    let lifecycle_writers =
        validate_lifecycle_writers(env_usize("BORSUK_BENCH_LIFECYCLE_WRITERS", 1)?)?;
    let lifecycle_insert_mode = parse_lifecycle_insert_mode(
        non_empty_env("BORSUK_BENCH_LIFECYCLE_INSERT_MODE")
            .as_deref()
            .unwrap_or("claim-free-put"),
    )?;
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
    let global_turboquant_bits = u8::try_from(env_usize("BORSUK_BENCH_TURBOQUANT_BITS", 8)?)
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
    // Preserve the benchmark's historical 1 GiB default when the variable is
    // absent, while making an explicit zero a real no-cache execution mode.
    let disk_cache_max_bytes = env_optional_byte_cap(
        "BORSUK_BENCH_DISK_CACHE_MAX_BYTES",
        Some(1024 * 1024 * 1024),
    )?;
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
    let max_active_searches = env_usize(
        "BORSUK_BENCH_MAX_ACTIVE_SEARCHES",
        DEFAULT_MAX_ACTIVE_SEARCHES,
    )?;
    let max_waiting_searches = env_usize(
        "BORSUK_BENCH_MAX_WAITING_SEARCHES",
        DEFAULT_MAX_WAITING_SEARCHES,
    )?;
    let leaf_read_width = env_usize("BORSUK_BENCH_LEAF_READ_WIDTH", DEFAULT_LEAF_READ_WIDTH)?;
    let max_inflight_leaf_reads = env_usize(
        "BORSUK_BENCH_MAX_INFLIGHT_LEAF_READS",
        DEFAULT_MAX_INFLIGHT_LEAF_READS,
    )?;
    let max_parallel_decode_rank_tasks = validate_max_parallel_decode_rank_tasks(env_usize(
        "BORSUK_BENCH_MAX_PARALLEL_DECODE_RANK_TASKS",
        DEFAULT_MAX_PARALLEL_DECODE_RANK_TASKS,
    )?)?;
    let exact_read_max_physical_amplification =
        validate_exact_read_max_physical_amplification(env_usize(
            "BORSUK_BENCH_EXACT_READ_MAX_PHYSICAL_AMPLIFICATION",
            DEFAULT_EXACT_READ_MAX_PHYSICAL_AMPLIFICATION as usize,
        )?)?;
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
    let v21_feasibility = env_flag("BORSUK_BENCH_V21_FEASIBILITY")?;
    let v21_source_archive_sha256 = v21_feasibility
        .then(|| non_empty_env("BORSUK_BENCH_V21_SOURCE_ARCHIVE_SHA256"))
        .flatten();
    let v21_index_id = v21_feasibility
        .then(|| non_empty_env("BORSUK_BENCH_V21_INDEX_ID"))
        .flatten();
    let v21_dataset_id = v21_feasibility
        .then(|| non_empty_env("BORSUK_BENCH_V21_DATASET_ID"))
        .flatten();
    let v22_stage_l = env_flag("BORSUK_BENCH_V22_STAGE_L")?;
    if v21_feasibility && v22_stage_l {
        return Err(invalid_input("V21 and V22 diagnostic modes are mutually exclusive").into());
    }
    let v22_source_archive_sha256 = v22_stage_l
        .then(|| non_empty_env("BORSUK_BENCH_V22_SOURCE_ARCHIVE_SHA256"))
        .flatten();
    let v22_index_id = v22_stage_l
        .then(|| non_empty_env("BORSUK_BENCH_V22_INDEX_ID"))
        .flatten();
    let v22_dataset_id = v22_stage_l
        .then(|| non_empty_env("BORSUK_BENCH_V22_DATASET_ID"))
        .flatten();
    if v21_feasibility
        && (v21_source_archive_sha256.is_none()
            || v21_index_id.is_none()
            || v21_dataset_id.is_none())
    {
        return Err(invalid_input(
            "BORSUK_BENCH_V21_FEASIBILITY requires exact source archive, index, and dataset identities",
        )
        .into());
    }
    if let (Some(source), Some(index), Some(dataset)) = (
        v21_source_archive_sha256.as_deref(),
        v21_index_id.as_deref(),
        v21_dataset_id.as_deref(),
    ) {
        validate_v21_evidence_identity_fields(source, index, dataset)?;
    }
    if v22_stage_l
        && (v22_source_archive_sha256.is_none()
            || v22_index_id.is_none()
            || v22_dataset_id.is_none())
    {
        return Err(invalid_input(
            "BORSUK_BENCH_V22_STAGE_L requires exact source archive, index, and dataset identities",
        )
        .into());
    }
    if let (Some(source), Some(index), Some(dataset)) = (
        v22_source_archive_sha256.as_deref(),
        v22_index_id.as_deref(),
        v22_dataset_id.as_deref(),
    ) {
        validate_v21_evidence_identity_fields(source, index, dataset)?;
    }
    let diagnostic_mode = v21_feasibility || v22_stage_l;
    let v21_forbidden_phase_env = [
        "BORSUK_BENCH_BUILD_INDEX",
        "BORSUK_BENCH_BUILD_ONLY",
        "BORSUK_BENCH_RECALL_ONLY",
        "BORSUK_BENCH_SKIP_RECALL",
        "BORSUK_BENCH_READ_ONLY",
        "BORSUK_BENCH_INSERT_ONLY",
        "BORSUK_BENCH_LIFECYCLE_ONLY",
        "BORSUK_BENCH_NPROBES",
        "BORSUK_BENCH_CANDIDATES",
        "BORSUK_BENCH_CONCURRENCY",
        "BORSUK_BENCH_WRITE_OPS",
        "BORSUK_BENCH_UPDATE_PERCENT",
        "BORSUK_BENCH_DELETE_PERCENT",
        "BORSUK_BENCH_LIFECYCLE_WRITERS",
        "BORSUK_BENCH_LIMIT",
    ];
    if diagnostic_mode
        && let Some(name) = v21_forbidden_phase_env
            .iter()
            .find(|name| env::var_os(name).is_some())
    {
        return Err(invalid_input(&format!(
            "BORSUK_BENCH_V21_FEASIBILITY is the only permitted phase selector; remove {name}"
        ))
        .into());
    }
    let build_index = if diagnostic_mode {
        false
    } else {
        env_flag_with_default("BORSUK_BENCH_BUILD_INDEX", true)?
    };
    let build_only = !diagnostic_mode && env_flag("BORSUK_BENCH_BUILD_ONLY")?;
    validate_build_only(build_only, build_index)?;
    let recall_only = !diagnostic_mode && env_flag("BORSUK_BENCH_RECALL_ONLY")?;
    let skip_recall = diagnostic_mode || env_flag("BORSUK_BENCH_SKIP_RECALL")?;
    let skip_exact_recall = env_flag("BORSUK_BENCH_SKIP_EXACT_RECALL")?;
    validate_phase_selection(recall_only, skip_recall)?;
    let read_only = diagnostic_mode || env_flag("BORSUK_BENCH_READ_ONLY")?;
    let insert_only = !diagnostic_mode && env_flag("BORSUK_BENCH_INSERT_ONLY")?;
    validate_insert_only(insert_only, build_only, read_only)?;
    let lifecycle_only = !diagnostic_mode && env_flag("BORSUK_BENCH_LIFECYCLE_ONLY")?;
    validate_lifecycle_only(
        lifecycle_only,
        build_index,
        build_only,
        recall_only,
        skip_recall,
        read_only,
        insert_only,
    )?;
    validate_v21_feasibility_phase(
        diagnostic_mode,
        V21FeasibilityPhaseSelection {
            build_index,
            build_only,
            recall_only,
            skip_recall,
            read_only,
            insert_only,
            lifecycle_only,
            ambient_nprobes: env::var_os("BORSUK_BENCH_NPROBES").is_some(),
            ambient_candidates: env::var_os("BORSUK_BENCH_CANDIDATES").is_some(),
            ambient_concurrency: env::var_os("BORSUK_BENCH_CONCURRENCY").is_some(),
            ambient_writes: [
                "BORSUK_BENCH_WRITE_OPS",
                "BORSUK_BENCH_UPDATE_PERCENT",
                "BORSUK_BENCH_DELETE_PERCENT",
                "BORSUK_BENCH_LIFECYCLE_WRITERS",
            ]
            .iter()
            .any(|name| env::var_os(name).is_some()),
            ambient_limit: env::var_os("BORSUK_BENCH_LIMIT").is_some(),
        },
    )?;
    let preload_serving = env_flag("BORSUK_BENCH_PRELOAD_SERVING")?;
    let recluster_build = env_flag("BORSUK_BENCH_RECLUSTER_BUILD")?;
    if lifecycle_only {
        let train_count = if limit == 0 {
            layout_meta.n_train
        } else {
            limit.min(layout_meta.n_train)
        };
        lifecycle_write_operation_count(
            train_count,
            LifecycleDeltaLayout {
                dimensions: layout_meta.dim,
                segment_max_vectors: segment_max,
            },
            write_ops,
            lifecycle_writers,
            write_batch_size,
            update_percent,
            delete_percent,
        )?;
    }

    Ok(ResolvedConfig {
        dataset_dir,
        uri,
        cache_dir,
        limit,
        queries,
        build_writers,
        lifecycle_writers,
        lifecycle_insert_mode,
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
        max_active_searches,
        max_waiting_searches,
        leaf_read_width,
        max_inflight_leaf_reads,
        max_parallel_decode_rank_tasks,
        exact_read_max_physical_amplification,
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
        lifecycle_only,
        v21_feasibility,
        v21_source_archive_sha256,
        v21_index_id,
        v21_dataset_id,
        v22_stage_l,
        v22_source_archive_sha256,
        v22_index_id,
        v22_dataset_id,
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
        "config dataset={} uri={} cache={} disk_cache_max_bytes={} limit={} queries={} build_writers={} lifecycle_writers={} lifecycle_insert_mode={} write_batch_size={} write_ops={} uncached_queries={} output_dir={} concurrency={} segment_max={} vector_element_type={} leaf_capability={} global_scan_codec={} global_pq_layout={:?} global_pq_code_bytes={} turboquant_bits={} turboquant_qjl_bits={} turboquant_shards={} cache_execution={} force_segment_path={} ram_budget_bytes={} segment_cache_max_bytes={} recall_nprobes={} recall_candidates={} recall_leaf_mode={} serving_mode={:?} serving_leaf_mode={} serving_nprobe={} serving_candidates={} serving_prefetch_depth={} max_active_searches={} max_waiting_searches={} leaf_read_width={} max_inflight_leaf_reads={} max_parallel_decode_rank_tasks={} exact_read_max_physical_amplification={} cache_profile={:?} cache_coverage_percent={} build_index={} build_only={} recall_only={} skip_recall={} skip_exact_recall={} recluster_build={} read_only={} insert_only={} lifecycle_only={} preload_serving={}",
        config.dataset_dir.display(),
        config.uri,
        config.cache_dir.display(),
        config.disk_cache_max_bytes.unwrap_or(0),
        config.limit,
        config.queries,
        config.build_writers,
        config.lifecycle_writers,
        config.lifecycle_insert_mode.as_str(),
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
        config.max_active_searches,
        config.max_waiting_searches,
        config.leaf_read_width,
        config.max_inflight_leaf_reads,
        config.max_parallel_decode_rank_tasks,
        config.exact_read_max_physical_amplification,
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
        config.lifecycle_only,
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
            allow_missing_corpus_for_phase(config.build_index, config.insert_only),
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

fn allow_missing_corpus_for_phase(build_index: bool, insert_only: bool) -> bool {
    !build_index && !insert_only
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

fn ingest_train(
    index: &mut BorsukIndex,
    uri: &str,
    build_writers: usize,
    ram_budget_bytes: Option<u64>,
    dataset_dir: &Path,
    dataset: &Dataset,
) -> BenchResult<BuildIngestReport> {
    // Both source forms stream bounded batches and use monotonic generated ids.
    // VectorDBBench acquisition must use its unshuffled train files so row ids
    // remain identical to the shipped ground-truth neighbor ids.
    let mut coordinator = BuildIngestCoordinator::open(uri, build_writers, ram_budget_bytes)?;
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
                coordinator.push(start, vectors)?;
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
                    coordinator.push(start, decoded)?;
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
    let mut report = coordinator.finish()?;
    if report.rows != dataset.train_count {
        return Err(invalid_input(&format!(
            "bulk ingest committed {} rows; expected {}",
            report.rows, dataset.train_count
        ))
        .into());
    }
    reopen_build_finalizer(index, uri, ram_budget_bytes, &mut report)?;
    Ok(report)
}

fn sample_logical_cell_training_vectors(
    config: &ResolvedConfig,
    dataset: &Dataset,
    sample_rows: usize,
    seed: u64,
) -> BenchResult<Vec<Vec<f32>>> {
    let mut sample = Vec::with_capacity(sample_rows);
    stream_dataset_batches(
        config,
        dataset,
        dataset.train_count,
        None,
        |offset, vectors| {
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
        },
    )?;
    if sample.len() != sample_rows {
        return Err(invalid_input(&format!(
            "logical-cell sampling retained {} rows; expected {sample_rows}",
            sample.len()
        ))
        .into());
    }
    Ok(sample)
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
    let ingest_vectors_per_s = if build.ingest_ms <= 0.0 {
        0.0
    } else {
        build.records as f64 * 1_000.0 / build.ingest_ms
    };
    writeln!(
        writer,
        "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{bytes_per_vector:.6},{},{},{},{},{},{},{},{},{},{:.3},{:.3},{},{},{:.3},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{ingest_vectors_per_s:.3}",
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
        build.gc_ms,
        build.gc_objects_scanned,
        build.gc_objects_deleted,
        build.gc_transaction_states_remaining,
        build.gc_bytes_read,
        build.gc_bytes_reclaimed,
        build.storage_requests.gets,
        build.storage_requests.puts,
        build.storage_requests.deletes,
        build.storage_requests.heads,
        build.storage_requests.lists,
        build.storage_bytes_read,
        build.storage_bytes_written,
        config.build_writers,
        build.ingest_batches,
        build.ingest_waves,
    )?;
    writer.flush()?;
    eprintln!("wrote {} rows=1", path.display());
    Ok(())
}

fn write_recall_latency_csv(config: &ResolvedConfig, dataset: &Dataset) -> BenchResult<()> {
    let path = config.output_dir.join("bench_recall_latency.csv");
    let mut writer = csv_writer(&path)?;
    writeln!(writer, "{RECALL_LATENCY_HEADER}")?;
    let samples_path = config.output_dir.join("bench_query_samples.csv");
    let mut samples_writer = csv_writer(&samples_path)?;
    writeln!(samples_writer, "{QUERY_SAMPLE_HEADER}")?;
    let mut rows_written = 0usize;

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
            // Each matrix cell owns its serving handle. Disk-cached execution
            // owns still narrower handles inside each cohort, so it deliberately
            // has no outer handle capable of retaining decoded/cache state.
            let cell_index = recall_cache_profile_needs_outer_handle(
                config.cache_profile,
                config.preload_serving,
                config.cache_execution,
                options.mode.leaf_mode(),
                effective_segment_cache_budget(config),
            )
            .then(|| open_serving_index(config, ServingMetadataPreparation::Complete))
            .transpose()?;
            let cell_preload_complete = if let Some(cell_index) = cell_index.as_ref() {
                if recall_preloads_local_snapshot(config.preload_serving) {
                    warm_all_segments(cell_index)?.coverage_complete
                } else {
                    let _ = cell_index.prepare_serving_metadata()?;
                    false
                }
            } else {
                false
            };
            for (phase, summary) in run_recall_cache_phases(
                config,
                dataset,
                cell_index,
                options,
                cell_preload_complete,
            )? {
                if !config.force_segment_path {
                    validate_bounded_v20_execution(&summary)?;
                }
                write_query_samples(
                    &mut samples_writer,
                    config,
                    QuerySampleContext {
                        phase,
                        mode: &config.recall_leaf_mode.to_string(),
                        nprobe,
                        max_candidates,
                        query_source_indices: dataset.query_source_indices.as_slice(),
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
                rows_written = rows_written.saturating_add(1);
            }
            writer.flush()?;
        }
    }

    if !config.skip_exact_recall {
        let exact_options = SearchOptions::exact(RECALL_K);
        let exact_index = recall_cache_profile_needs_outer_handle(
            config.cache_profile,
            config.preload_serving,
            config.cache_execution,
            exact_options.mode.leaf_mode(),
            effective_segment_cache_budget(config),
        )
        .then(|| open_serving_index(config, ServingMetadataPreparation::Complete))
        .transpose()?;
        let exact_preload_complete = if let Some(exact_index) = exact_index.as_ref() {
            if recall_preloads_local_snapshot(config.preload_serving) {
                warm_all_segments(exact_index)?.coverage_complete
            } else {
                let _ = exact_index.prepare_serving_metadata()?;
                false
            }
        } else {
            false
        };
        for (phase, summary) in run_recall_cache_phases(
            config,
            dataset,
            exact_index,
            exact_options,
            exact_preload_complete,
        )? {
            write_query_samples(
                &mut samples_writer,
                config,
                QuerySampleContext {
                    phase,
                    mode: "exact",
                    nprobe: 0,
                    max_candidates: 0,
                    query_source_indices: dataset.query_source_indices.as_slice(),
                },
                &summary,
            )?;
            write_recall_row(&mut writer, config, phase, "exact", 0, 0, &summary)?;
            rows_written = rows_written.saturating_add(1);
        }
    }
    writer.flush()?;
    samples_writer.flush()?;
    eprintln!(
        "wrote {} rows={} dataset={}",
        path.display(),
        rows_written,
        dataset.meta.name
    );
    Ok(())
}

fn write_v21_feasibility_artifacts(config: &ResolvedConfig, dataset: &Dataset) -> BenchResult<()> {
    let destinations = v21_feasibility_destinations(&config.output_dir);
    reject_existing_destinations(&destinations)?;
    if config.queries == 0
        || config.queries > dataset.queries.len()
        || config.queries > dataset.ground_truth.len()
        || config.queries > dataset.query_source_indices.len()
        || dataset.meta.name.is_empty()
        || dataset.meta.dim == 0
    {
        return Err(invalid_input(
            "V21 feasibility requires the exact configured dataset and query authority",
        )
        .into());
    }
    let query_count = config.queries;
    let index = BorsukIndex::open_with_options(
        &config.uri,
        OpenOptions {
            cache_dir: None,
            cache_max_bytes: Some(0),
            ram_budget_bytes: config.ram_budget_bytes,
            resident_routing: true,
            cell_card_code_plane_cache_max_bytes: 0,
            ..OpenOptions::default()
        },
    )?;
    let _ = index.prepare_serving_metadata_without_complete_code_planes()?;
    let baseline_rss_bytes = memory_stats::memory_stats()
        .ok_or_else(|| invalid_input("V21 feasibility could not observe process RSS"))?
        .physical_mem as u64;
    let queries = &dataset.queries[..query_count];
    let truth = &dataset.ground_truth[..query_count];
    let reports = index.diagnose_v21_selector_feasibility(
        queries,
        truth,
        &SearchOptions::approx(RECALL_K, config.recall_leaf_mode),
        &v21_feasibility_arms(),
    )?;
    let source_archive_sha256 = config
        .v21_source_archive_sha256
        .as_deref()
        .ok_or_else(|| invalid_input("V21 feasibility source identity is absent"))?;
    let index_id = config
        .v21_index_id
        .as_deref()
        .ok_or_else(|| invalid_input("V21 feasibility index identity is absent"))?;
    let dataset_id = config
        .v21_dataset_id
        .as_deref()
        .ok_or_else(|| invalid_input("V21 feasibility dataset identity is absent"))?;
    let identity = V21EvidenceIdentity {
        dataset_name: &dataset.meta.name,
        dataset_id,
        index_id,
        source_archive_sha256,
        dimensions: dataset.meta.dim,
        dataset_rows: u64::try_from(dataset.train_count)
            .map_err(|_| invalid_input("V21 dataset rows exceed u64"))?,
        query_seed: config.query_seed,
        query_source_indices: &dataset.query_source_indices[..query_count],
        baseline_rss_bytes,
    };
    write_v21_feasibility_evidence(&config.output_dir, &identity, &reports)
}

fn v21_feasibility_destinations(output_dir: &Path) -> [PathBuf; 3] {
    [
        output_dir.join("bench_v21_feasibility_arms.csv"),
        output_dir.join("bench_v21_feasibility_samples.csv"),
        output_dir.join("bench_v21_feasibility_summary.json"),
    ]
}

fn v22_stage_l_scratch_parent(output_dir: &Path) -> &Path {
    output_dir
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
}

fn write_v22_stage_l_artifacts(config: &ResolvedConfig, dataset: &Dataset) -> BenchResult<()> {
    let destinations = v22_stage_l_destinations(&config.output_dir);
    reject_existing_destinations(&destinations)?;
    if config.queries == 0
        || config.queries > dataset.queries.len()
        || config.queries > dataset.ground_truth.len()
        || config.queries > dataset.query_source_indices.len()
        || dataset.meta.name.is_empty()
        || dataset.meta.dim == 0
        || dataset.train_count != dataset.meta.n_train
    {
        return Err(invalid_input(
            "V22 Stage L requires the exact full dataset and configured query authority",
        )
        .into());
    }
    let query_count = config.queries;
    let index = BorsukIndex::open_with_options(
        &config.uri,
        OpenOptions {
            cache_dir: None,
            cache_max_bytes: Some(0),
            ram_budget_bytes: config.ram_budget_bytes,
            resident_routing: true,
            cell_card_code_plane_cache_max_bytes: 0,
            ..OpenOptions::default()
        },
    )?;
    let _ = index.prepare_serving_metadata_without_complete_code_planes()?;
    let scratch_parent = v22_stage_l_scratch_parent(&config.output_dir);
    fs::create_dir_all(scratch_parent)?;
    let scratch = tempfile::Builder::new()
        .prefix(".borsuk-v22-stage-l-")
        .tempdir_in(scratch_parent)?;
    let report = index.diagnose_v22_stage_l(
        &dataset.queries[..query_count],
        &dataset.ground_truth[..query_count],
        &SearchOptions::approx(RECALL_K, config.recall_leaf_mode),
        scratch.path(),
    )?;
    let source_archive_sha256 = config
        .v22_source_archive_sha256
        .as_deref()
        .ok_or_else(|| invalid_input("V22 Stage L source identity is absent"))?;
    let index_id = config
        .v22_index_id
        .as_deref()
        .ok_or_else(|| invalid_input("V22 Stage L index identity is absent"))?;
    let dataset_id = config
        .v22_dataset_id
        .as_deref()
        .ok_or_else(|| invalid_input("V22 Stage L dataset identity is absent"))?;
    let identity = V22EvidenceIdentity {
        dataset_name: &dataset.meta.name,
        dataset_id,
        index_id,
        source_archive_sha256,
        dimensions: dataset.meta.dim,
        dataset_rows: u64::try_from(dataset.train_count)
            .map_err(|_| invalid_input("V22 Stage L dataset rows exceed u64"))?,
        query_seed: config.query_seed,
        query_source_indices: &dataset.query_source_indices[..query_count],
    };
    write_v22_stage_l_evidence(&config.output_dir, &identity, &report)?;
    Ok(())
}

fn v22_stage_l_destinations(output_dir: &Path) -> [PathBuf; 2] {
    [
        output_dir.join("bench_v22_stage_l_report.json"),
        output_dir.join("bench_v22_stage_l_summary.json"),
    ]
}

fn valid_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn build_v22_stage_l_summary(
    identity: &V22EvidenceIdentity<'_>,
    report: &V22StageLReport,
) -> io::Result<V22StageLSummary> {
    if identity.dataset_name.is_empty()
        || identity.dataset_id.is_empty()
        || identity.index_id.is_empty()
        || !valid_sha256(identity.source_archive_sha256)
        || identity.dimensions == 0
        || identity.dataset_rows == 0
        || identity.dataset_rows != report.rows
        || identity.query_source_indices.is_empty()
        || identity
            .query_source_indices
            .iter()
            .copied()
            .collect::<BTreeSet<_>>()
            .len()
            != identity.query_source_indices.len()
        || !valid_sha256(&report.v20_root_checksum)
        || !valid_sha256(&report.v20_codebook_checksum)
        || report.routing_cell_count == 0
        || report.query_prefixes.len() != identity.query_source_indices.len()
        || report.layout_censuses.len() != 42
    {
        return Err(invalid_input("V22 Stage L identity authority differs"));
    }
    for (query_index, prefix) in report.query_prefixes.iter().enumerate() {
        if prefix.query_index != query_index
            || prefix.rows.len() != 2048
            || prefix.rows.iter().any(|row| {
                !row.distance.is_finite()
                    || row.canonical_record_id.is_empty()
                    || row.primary_cell_routing_rank == 0
                    || row.primary_cell_routing_rank > report.routing_cell_count
            })
            || prefix.rows.windows(2).any(|pair| {
                pair[0]
                    .distance
                    .total_cmp(&pair[1].distance)
                    .then_with(|| {
                        pair[0]
                            .canonical_record_id
                            .cmp(&pair[1].canonical_record_id)
                    })
                    .then_with(|| pair[0].record_id.cmp(&pair[1].record_id))
                    .is_gt()
            })
            || prefix
                .rows
                .iter()
                .map(|row| row.record_id)
                .collect::<BTreeSet<_>>()
                .len()
                != prefix.rows.len()
        {
            return Err(invalid_input("V22 Stage L exact-prefix authority differs"));
        }
    }
    let expected_arms = [
        (V22LayoutKind::V20Physical, None),
        (V22LayoutKind::V20TwoPivotRepacked, Some(32)),
        (V22LayoutKind::V20TwoPivotRepacked, Some(64)),
        (V22LayoutKind::SemanticWithinCell, Some(32)),
        (V22LayoutKind::SemanticWithinCell, Some(64)),
        (V22LayoutKind::SemanticCrossCell, Some(32)),
        (V22LayoutKind::SemanticCrossCell, Some(64)),
    ]
    .into_iter()
    .flat_map(|(layout, rows)| {
        [10_u16, 256, 512, 1024, 1536, 2048]
            .into_iter()
            .map(move |prefix| (layout, rows, prefix))
    });
    let mut maximum_routed_rows = 0_u64;
    let mut maximum_primary_requests = 0_usize;
    let mut maximum_primary_physical_bytes = 0_u64;
    let mut eligible_arms = Vec::new();
    let mut exact_row_bytes = None;
    let mut layout_object_authorities: [Option<&[V22StageLProjectedObject]>; 7] = [None; 7];
    for (arm_index, (arm, expected)) in report.layout_censuses.iter().zip(expected_arms).enumerate()
    {
        let projected_object_bytes = arm
            .projected_objects
            .iter()
            .map(|object| (object.path.as_str(), object.encoded_bytes))
            .collect::<BTreeMap<_, _>>();
        if (arm.layout, arm.microcluster_rows, arm.exact_prefix_rows) != expected
            || arm.query_samples.len() != report.query_prefixes.len()
            || arm.projected_objects.is_empty()
            || projected_object_bytes.len() != arm.projected_objects.len()
            || arm.projected_objects.iter().any(|object| {
                object.path.is_empty()
                    || !valid_sha256(&object.checksum)
                    || object.encoded_bytes == 0
            })
        {
            return Err(invalid_input("V22 Stage L arm authority differs"));
        }
        let layout_index = arm_index / 6;
        if layout_object_authorities[layout_index]
            .is_some_and(|objects| objects != arm.projected_objects.as_slice())
        {
            return Err(invalid_input("V22 Stage L layout object authority differs"));
        }
        layout_object_authorities[layout_index] = Some(&arm.projected_objects);
        for (query_index, sample) in arm.query_samples.iter().enumerate() {
            let physical_bytes = sample.ranges.iter().try_fold(0_u64, |total, range| {
                range
                    .end
                    .checked_sub(range.start)
                    .and_then(|bytes| total.checked_add(bytes))
            });
            let selected_bytes = sample.ranges.iter().try_fold(0_u64, |total, range| {
                total.checked_add(range.selected_bytes)
            });
            let selected_rows = sample
                .ranges
                .iter()
                .try_fold(0_u64, |total, range| total.checked_add(range.rows));
            let range_authority_differs = sample.ranges.iter().any(|range| {
                range.start >= range.end
                    || range.selected_bytes == 0
                    || range.selected_bytes > range.end.saturating_sub(range.start)
                    || range.rows == 0
                    || range.blocks == 0
                    || projected_object_bytes
                        .get(range.path.as_str())
                        .is_none_or(|encoded_bytes| range.end > *encoded_bytes)
            });
            let sample_exact_row_bytes = sample
                .useful_bytes
                .checked_div(u64::from(sample.exact_prefix_rows))
                .filter(|bytes| {
                    *bytes > 0
                        && bytes.checked_mul(u64::from(sample.exact_prefix_rows))
                            == Some(sample.useful_bytes)
                });
            let expected_packing_purity = sample
                .useful_bytes
                .checked_mul(1_000_000)
                .and_then(|value| value.checked_div(sample.physical_bytes));
            let expected_physical_amplification = sample
                .physical_bytes
                .checked_mul(1_000_000)
                .and_then(|value| value.checked_div(sample.selected_bytes));
            let expected_physical_bound = if sample.physical_bytes > 1_048_576 {
                V22LayoutLimitingBound::Bytes
            } else if sample.requests > 4 {
                V22LayoutLimitingBound::Requests
            } else {
                V22LayoutLimitingBound::Eligible
            };
            if sample.query_index != query_index
                || sample.exact_prefix_rows != arm.exact_prefix_rows
                || sample.gt_cell_hits != 10
                || sample.gt_cell_coverage_ppm != 1_000_000
                || sample.required_routing_cells == 0
                || sample.required_routing_cells > report.routing_cell_count
                || sample.routing_eligible != (sample.routed_rows <= 512_000)
                || sample.routed_rows > report.rows
                || sample.requests != sample.ranges.len()
                || sample.requests == 0
                || sample.requests > usize::from(sample.exact_prefix_rows)
                || range_authority_differs
                || physical_bytes != Some(sample.physical_bytes)
                || selected_bytes != Some(sample.selected_bytes)
                || selected_rows != Some(sample.selected_rows)
                || sample.selected_rows < u64::from(sample.exact_prefix_rows)
                || sample_exact_row_bytes.is_none()
                || sample.selected_bytes.checked_add(sample.speculative_bytes)
                    != Some(sample.physical_bytes)
                || expected_packing_purity != Some(sample.packing_purity_ppm)
                || expected_physical_amplification != Some(sample.physical_amplification_ppm)
                || sample.physical_limiting_bound != expected_physical_bound
                || sample.limiting_bound
                    != if sample.routing_eligible {
                        sample.physical_limiting_bound
                    } else {
                        V22LayoutLimitingBound::RoutingRows
                    }
                || sample.eligible
                    != (sample.routing_eligible
                        && sample.physical_limiting_bound == V22LayoutLimitingBound::Eligible)
            {
                return Err(invalid_input("V22 Stage L sample evidence differs"));
            }
            if exact_row_bytes.is_some_and(|bytes| Some(bytes) != sample_exact_row_bytes) {
                return Err(invalid_input("V22 Stage L exact-row width differs"));
            }
            exact_row_bytes = sample_exact_row_bytes;
            maximum_routed_rows = maximum_routed_rows.max(sample.routed_rows);
            maximum_primary_requests = maximum_primary_requests.max(sample.requests);
            maximum_primary_physical_bytes =
                maximum_primary_physical_bytes.max(sample.physical_bytes);
        }
        if arm.eligible != arm.query_samples.iter().all(|sample| sample.eligible) {
            return Err(invalid_input("V22 Stage L arm eligibility differs"));
        }
        if arm.eligible {
            eligible_arms.push(arm_index);
        }
    }
    Ok(V22StageLSummary {
        schema: V22_STAGE_L_SCHEMA,
        document_kind: "publication-v3-v22-stage-l-summary",
        claim_eligible: false,
        rows: report.rows,
        routing_cell_count: report.routing_cell_count,
        queries: report.query_prefixes.len(),
        arms: report.layout_censuses.len(),
        eligible_arms,
        maximum_routed_rows,
        maximum_primary_requests,
        maximum_primary_physical_bytes,
    })
}

fn serialize_v22_stage_l_evidence(
    identity: &V22EvidenceIdentity<'_>,
    report: &V22StageLReport,
) -> io::Result<[Vec<u8>; 2]> {
    let summary = build_v22_stage_l_summary(identity, report)?;
    let evidence = V22StageLEvidence {
        schema: V22_STAGE_L_SCHEMA,
        document_kind: "publication-v3-v22-stage-l-report",
        claim_eligible: false,
        identity: *identity,
        report,
    };
    let mut raw = serde_json::to_vec(&evidence)
        .map_err(|_| invalid_input("V22 Stage L report serialization failed"))?;
    raw.push(b'\n');
    let mut summary = serde_json::to_vec(&summary)
        .map_err(|_| invalid_input("V22 Stage L summary serialization failed"))?;
    summary.push(b'\n');
    Ok([raw, summary])
}

fn validate_v22_stage_l_evidence(
    output_dir: &Path,
    identity: &V22EvidenceIdentity<'_>,
    report: &V22StageLReport,
) -> io::Result<()> {
    let expected = serialize_v22_stage_l_evidence(identity, report)?;
    let observed = v22_stage_l_destinations(output_dir).map(fs::read);
    for (observed, expected) in observed.into_iter().zip(expected) {
        if observed? != expected {
            return Err(invalid_input("V22 Stage L persisted evidence differs"));
        }
    }
    Ok(())
}

fn write_v22_stage_l_evidence(
    output_dir: &Path,
    identity: &V22EvidenceIdentity<'_>,
    report: &V22StageLReport,
) -> io::Result<()> {
    let destinations = v22_stage_l_destinations(output_dir);
    let payloads = serialize_v22_stage_l_evidence(identity, report)?;
    publish_exclusive_file_set(output_dir, &destinations, &payloads)?;
    validate_v22_stage_l_evidence(output_dir, identity, report)
}

fn projected_v21_serving_rss(
    baseline_rss_bytes: u64,
    replaced_v20_root_bytes: u64,
    selector_bytes: u64,
    query_transient_bytes: u64,
) -> Option<u64> {
    baseline_rss_bytes
        .checked_sub(replaced_v20_root_bytes)?
        .checked_add(selector_bytes)?
        .checked_add(query_transient_bytes)
}

fn reject_existing_destinations(destinations: &[PathBuf]) -> io::Result<()> {
    if let Some(path) = destinations.iter().find(|path| path.exists()) {
        return Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            format!("refusing to overwrite `{}`", path.display()),
        ));
    }
    Ok(())
}

fn v21_limiting_bound(bound: V21LimitingBound) -> &'static str {
    match bound {
        V21LimitingBound::Exhausted => "exhausted",
        V21LimitingBound::Requests => "requests",
        V21LimitingBound::Bytes => "bytes",
        V21LimitingBound::Amplification => "amplification",
        V21LimitingBound::FirstBundle => "first_bundle",
    }
}

fn validate_v21_feasibility_reports(
    query_source_indices: &[usize],
    reports: &[V21FeasibilityReport],
) -> io::Result<()> {
    let expected_arms = v21_feasibility_arms();
    if reports.len() != expected_arms.len() || query_source_indices.is_empty() {
        return Err(invalid_input(
            "V21 feasibility evidence has incomplete arm or query authority",
        ));
    }
    let expected_root_bytes = reports[0].replaced_v20_root_bytes;
    let expected_root_checksum = &reports[0].v20_root_checksum;
    for (arm_index, (report, expected_arm)) in reports.iter().zip(expected_arms).enumerate() {
        if report.arm != expected_arm
            || report.samples.len() != query_source_indices.len()
            || report.bundle_count == 0
            || report.region_count == 0
            || report.rows == 0
            || report.projected_directory_bytes == 0
            || report.replaced_v20_root_bytes != expected_root_bytes
            || report.replaced_v20_root_bytes == 0
            || report.v20_root_checksum != *expected_root_checksum
            || report.v20_root_checksum.len() != 64
            || !report
                .v20_root_checksum
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            || report.selector_within_frozen_cap != (report.projected_directory_bytes <= 40_000_000)
        {
            return Err(invalid_input("V21 feasibility arm authority drifted"));
        }
        for (query_index, sample) in report.samples.iter().enumerate() {
            if sample.arm_index != arm_index
                || sample.query_index != query_index
                || sample.routed_cells == 0
                || u64::from(sample.selected_rows) > report.rows
                || sample.selected_bundles > report.bundle_count
                || sample.primary_requests > sample.maximum_actual_requests
                || sample.selected_bytes > sample.physical_bytes
                || sample.gt_hits > RECALL_K
                || sample.recall_hits > RECALL_K
            {
                return Err(invalid_input("V21 feasibility sample authority drifted"));
            }
        }
    }
    Ok(())
}

fn build_v21_feasibility_summary(
    identity: &V21EvidenceIdentity<'_>,
    reports: &[V21FeasibilityReport],
) -> io::Result<V21FeasibilitySummary> {
    validate_v21_feasibility_reports(identity.query_source_indices, reports)?;
    validate_v21_evidence_identity_fields(
        identity.source_archive_sha256,
        identity.index_id,
        identity.dataset_id,
    )?;
    if identity.dataset_name.is_empty()
        || identity.dimensions == 0
        || identity.dataset_rows == 0
        || identity.baseline_rss_bytes == 0
        || identity
            .query_source_indices
            .iter()
            .copied()
            .collect::<BTreeSet<_>>()
            .len()
            != identity.query_source_indices.len()
        || reports
            .iter()
            .any(|report| report.rows != identity.dataset_rows)
    {
        return Err(invalid_input(
            "V21 feasibility evidence identity is invalid",
        ));
    }
    let row_bytes = u64::try_from(identity.dimensions)
        .ok()
        .and_then(|dimensions| dimensions.checked_mul(4))
        .and_then(|bytes| bytes.checked_add(128))
        .ok_or_else(|| invalid_input("V21 feasibility decoded-row bytes overflow"))?;
    let mut arms = Vec::with_capacity(reports.len());
    for (arm_index, report) in reports.iter().enumerate() {
        let denominator = (report.samples.len() * RECALL_K) as f64;
        let gt_coverage = report
            .samples
            .iter()
            .map(|sample| sample.gt_hits)
            .sum::<usize>() as f64
            / denominator;
        let recall_at_10 = report
            .samples
            .iter()
            .map(|sample| sample.recall_hits)
            .sum::<usize>() as f64
            / denominator;
        let maximum_actual_requests = report
            .samples
            .iter()
            .map(|sample| sample.maximum_actual_requests)
            .max()
            .unwrap_or(0);
        let maximum_physical_bytes = report
            .samples
            .iter()
            .map(|sample| sample.physical_bytes)
            .max()
            .unwrap_or(0);
        let projected_query_transient_bytes = report
            .samples
            .iter()
            .map(|sample| {
                u64::from(sample.selected_rows)
                    .checked_mul(row_bytes)
                    .and_then(|decoded| decoded.checked_add(sample.physical_bytes))
            })
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| invalid_input("V21 feasibility query-transient bytes overflow"))?
            .into_iter()
            .max()
            .unwrap_or(0);
        let projected_peak_rss_bytes = projected_v21_serving_rss(
            identity.baseline_rss_bytes,
            report.replaced_v20_root_bytes,
            report.projected_directory_bytes,
            projected_query_transient_bytes,
        )
        .ok_or_else(|| invalid_input("V21 feasibility projected RSS overflows or underflows"))?;
        let eligible = report.selector_within_frozen_cap
            && projected_peak_rss_bytes <= 768 * 1024 * 1024
            && gt_coverage >= 0.990
            && recall_at_10 >= 0.975
            && maximum_actual_requests <= 4
            && maximum_physical_bytes <= 1_048_576;
        arms.push(V21FeasibilityArmSummary {
            arm_index,
            bundle_row_limit: report.arm.bundle_row_limit,
            selector_span: report.arm.selector_span,
            hedge_delay_ms: report.arm.hedge_delay_ms,
            bundle_count: report.bundle_count,
            region_count: report.region_count,
            projected_directory_bytes: report.projected_directory_bytes,
            replaced_v20_root_bytes: report.replaced_v20_root_bytes,
            selector_within_frozen_cap: report.selector_within_frozen_cap,
            rows: report.rows,
            gt_coverage,
            recall_at_10,
            maximum_actual_requests,
            maximum_physical_bytes,
            projected_query_transient_bytes,
            projected_peak_rss_bytes,
            eligible,
        });
    }
    let minimum_arm_gt_coverage = arms
        .iter()
        .map(|arm| arm.gt_coverage)
        .min_by(f64::total_cmp)
        .ok_or_else(|| invalid_input("V21 feasibility evidence contains no arms"))?;
    let minimum_arm_recall_at_10 = arms
        .iter()
        .map(|arm| arm.recall_at_10)
        .min_by(f64::total_cmp)
        .ok_or_else(|| invalid_input("V21 feasibility evidence contains no arms"))?;
    let maximum_actual_requests = arms
        .iter()
        .map(|arm| arm.maximum_actual_requests)
        .max()
        .unwrap_or(0);
    let maximum_physical_bytes = arms
        .iter()
        .map(|arm| arm.maximum_physical_bytes)
        .max()
        .unwrap_or(0);
    let eligible_arm_indexes = arms
        .iter()
        .filter_map(|arm| arm.eligible.then_some(arm.arm_index))
        .collect();
    Ok(V21FeasibilitySummary {
        schema: V21_FEASIBILITY_SCHEMA.to_string(),
        claim_eligible: false,
        dataset_name: identity.dataset_name.to_string(),
        dataset_id: identity.dataset_id.to_string(),
        index_id: identity.index_id.to_string(),
        source_archive_sha256: identity.source_archive_sha256.to_string(),
        v20_root_checksum: reports[0].v20_root_checksum.clone(),
        dataset_rows: identity.dataset_rows,
        dimensions: identity.dimensions,
        query_seed: identity.query_seed,
        query_source_indices: identity.query_source_indices.to_vec(),
        arm_count: reports.len(),
        sample_count: reports.iter().map(|report| report.samples.len()).sum(),
        baseline_rss_bytes: identity.baseline_rss_bytes,
        minimum_arm_gt_coverage,
        minimum_arm_recall_at_10,
        maximum_actual_requests,
        maximum_physical_bytes,
        eligible_arm_indexes,
        arms,
    })
}

fn write_v21_feasibility_evidence(
    output_dir: &Path,
    identity: &V21EvidenceIdentity<'_>,
    reports: &[V21FeasibilityReport],
) -> BenchResult<()> {
    let destinations = v21_feasibility_destinations(output_dir);
    reject_existing_destinations(&destinations)?;
    let summary = build_v21_feasibility_summary(identity, reports)?;
    let payloads = serialize_v21_feasibility_evidence(identity, reports, &summary)?;
    validate_serialized_v21_feasibility_evidence(identity, reports, &payloads)?;
    publish_exclusive_file_set(output_dir, &destinations, &payloads)?;
    Ok(())
}

fn serialize_v21_feasibility_evidence(
    identity: &V21EvidenceIdentity<'_>,
    reports: &[V21FeasibilityReport],
    summary: &V21FeasibilitySummary,
) -> io::Result<[Vec<u8>; 3]> {
    use std::fmt::Write as _;

    let mut arms = format!("{V21_FEASIBILITY_ARMS_HEADER}\n");
    let mut samples = format!("{V21_FEASIBILITY_SAMPLES_HEADER}\n");
    for (arm_index, report) in reports.iter().enumerate() {
        let arm_summary = summary
            .arms
            .get(arm_index)
            .ok_or_else(|| invalid_input("V21 feasibility summary arm is absent"))?;
        writeln!(
            arms,
            "{V21_FEASIBILITY_SCHEMA},{arm_index},{},{},{},{},{},{},{},{},{},{},{},{:.17},{:.17},{},{},{},{},{}",
            report.arm.bundle_row_limit,
            report.arm.selector_span,
            report
                .arm
                .hedge_delay_ms
                .map_or_else(|| "off".to_string(), |delay| delay.to_string()),
            report.bundle_count,
            report.region_count,
            report.projected_directory_bytes,
            report.replaced_v20_root_bytes,
            report.v20_root_checksum,
            identity.baseline_rss_bytes,
            arm_summary.projected_query_transient_bytes,
            arm_summary.projected_peak_rss_bytes,
            arm_summary.gt_coverage,
            arm_summary.recall_at_10,
            arm_summary.maximum_actual_requests,
            arm_summary.maximum_physical_bytes,
            report.selector_within_frozen_cap,
            arm_summary.eligible,
            report.rows,
        )
        .map_err(|_| invalid_input("V21 feasibility arm serialization failed"))?;
        for sample in &report.samples {
            writeln!(
                samples,
                "{V21_FEASIBILITY_SCHEMA},{arm_index},{},{},{},{},{},{},{},{},{},{},{},{}",
                sample.query_index,
                identity.query_source_indices[sample.query_index],
                sample.routed_cells,
                sample.selected_rows,
                sample.selected_bundles,
                sample.primary_requests,
                sample.maximum_actual_requests,
                sample.selected_bytes,
                sample.physical_bytes,
                sample.gt_hits,
                sample.recall_hits,
                v21_limiting_bound(sample.limiting_bound),
            )
            .map_err(|_| invalid_input("V21 feasibility sample serialization failed"))?;
        }
    }
    let summary_bytes = canonical_json_line(summary)?;
    Ok([arms.into_bytes(), samples.into_bytes(), summary_bytes])
}

fn validate_serialized_v21_feasibility_evidence(
    identity: &V21EvidenceIdentity<'_>,
    reports: &[V21FeasibilityReport],
    payloads: &[Vec<u8>; 3],
) -> io::Result<()> {
    let reparsed: V21FeasibilitySummary = serde_json::from_slice(&payloads[2])?;
    let recomputed = build_v21_feasibility_summary(identity, reports)?;
    if reparsed != recomputed {
        return Err(invalid_input(
            "V21 feasibility summary failed recomputation",
        ));
    }
    let expected = serialize_v21_feasibility_evidence(identity, reports, &recomputed)?;
    if payloads != &expected {
        return Err(invalid_input(
            "V21 feasibility CSV evidence failed canonical recomputation",
        ));
    }
    validate_v21_samples_csv(identity, reports, &reparsed, &payloads[1])?;
    validate_v21_arms_csv(identity, reports, &reparsed, &payloads[0])?;
    let arm_rows = payloads[0].split(|byte| *byte == b'\n').count() - 1;
    let sample_rows = payloads[1].split(|byte| *byte == b'\n').count() - 1;
    if arm_rows != reports.len() + 1 || sample_rows != recomputed.sample_count + 1 {
        return Err(invalid_input("V21 feasibility artifact row count drifted"));
    }
    Ok(())
}

fn validate_v21_arms_csv(
    identity: &V21EvidenceIdentity<'_>,
    reports: &[V21FeasibilityReport],
    summary: &V21FeasibilitySummary,
    bytes: &[u8],
) -> io::Result<()> {
    let text = std::str::from_utf8(bytes)
        .map_err(|_| invalid_input("V21 feasibility arms are not UTF-8"))?;
    let mut lines = text.lines();
    if lines.next() != Some(V21_FEASIBILITY_ARMS_HEADER) {
        return Err(invalid_input("V21 feasibility arm header drifted"));
    }
    let parse_bool = |field: &str| match field {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => Err(invalid_input("V21 feasibility boolean is invalid")),
    };
    let parse_usize = |field: &str| {
        field
            .parse::<usize>()
            .map_err(|_| invalid_input("V21 feasibility arm integer is invalid"))
    };
    let parse_u64 = |field: &str| {
        field
            .parse::<u64>()
            .map_err(|_| invalid_input("V21 feasibility arm integer is invalid"))
    };
    let parse_f64 = |field: &str| {
        field
            .parse::<f64>()
            .map_err(|_| invalid_input("V21 feasibility arm float is invalid"))
    };
    let mut rows = 0_usize;
    for (ordinal, line) in lines.enumerate() {
        let fields = line.split(',').collect::<Vec<_>>();
        if fields.len() != 20 || fields[0] != V21_FEASIBILITY_SCHEMA || ordinal >= reports.len() {
            return Err(invalid_input("V21 feasibility arm row shape drifted"));
        }
        let report = &reports[ordinal];
        let arm = &summary.arms[ordinal];
        let hedge_delay_ms = match fields[4] {
            "off" => None,
            value => Some(
                value
                    .parse::<u16>()
                    .map_err(|_| invalid_input("V21 feasibility hedge delay is invalid"))?,
            ),
        };
        if parse_usize(fields[1])? != ordinal
            || fields[2].parse::<u16>().ok() != Some(arm.bundle_row_limit)
            || fields[3].parse::<u16>().ok() != Some(arm.selector_span)
            || hedge_delay_ms != arm.hedge_delay_ms
            || parse_usize(fields[5])? != arm.bundle_count
            || parse_usize(fields[6])? != arm.region_count
            || parse_u64(fields[7])? != arm.projected_directory_bytes
            || parse_u64(fields[8])? != arm.replaced_v20_root_bytes
            || fields[9] != summary.v20_root_checksum
            || parse_u64(fields[10])? != identity.baseline_rss_bytes
            || parse_u64(fields[11])? != arm.projected_query_transient_bytes
            || parse_u64(fields[12])? != arm.projected_peak_rss_bytes
            || parse_f64(fields[13])? != arm.gt_coverage
            || parse_f64(fields[14])? != arm.recall_at_10
            || parse_usize(fields[15])? != arm.maximum_actual_requests
            || parse_u64(fields[16])? != arm.maximum_physical_bytes
            || parse_bool(fields[17])? != arm.selector_within_frozen_cap
            || parse_bool(fields[18])? != arm.eligible
            || parse_u64(fields[19])? != arm.rows
            || report.arm.bundle_row_limit != arm.bundle_row_limit
            || report.arm.selector_span != arm.selector_span
            || report.arm.hedge_delay_ms != arm.hedge_delay_ms
        {
            return Err(invalid_input(
                "V21 feasibility arm fields failed recomputation",
            ));
        }
        rows += 1;
    }
    if rows != reports.len() {
        return Err(invalid_input("V21 feasibility arm rows are incomplete"));
    }
    Ok(())
}

fn validate_v21_samples_csv(
    identity: &V21EvidenceIdentity<'_>,
    reports: &[V21FeasibilityReport],
    summary: &V21FeasibilitySummary,
    bytes: &[u8],
) -> io::Result<()> {
    let text = std::str::from_utf8(bytes)
        .map_err(|_| invalid_input("V21 feasibility samples are not UTF-8"))?;
    let mut lines = text.lines();
    if lines.next() != Some(V21_FEASIBILITY_SAMPLES_HEADER) {
        return Err(invalid_input("V21 feasibility sample header drifted"));
    }
    let query_count = identity.query_source_indices.len();
    let row_bytes = u64::try_from(identity.dimensions)
        .ok()
        .and_then(|dimensions| dimensions.checked_mul(4))
        .and_then(|bytes| bytes.checked_add(128))
        .ok_or_else(|| invalid_input("V21 feasibility decoded-row bytes overflow"))?;
    let mut counts = vec![0_usize; reports.len()];
    let mut gt_hits = vec![0_usize; reports.len()];
    let mut recall_hits = vec![0_usize; reports.len()];
    let mut maximum_requests = vec![0_usize; reports.len()];
    let mut maximum_physical = vec![0_u64; reports.len()];
    let mut maximum_transient = vec![0_u64; reports.len()];
    for (ordinal, line) in lines.enumerate() {
        let fields = line.split(',').collect::<Vec<_>>();
        if fields.len() != 14 || fields[0] != V21_FEASIBILITY_SCHEMA {
            return Err(invalid_input("V21 feasibility sample row shape drifted"));
        }
        let parse_usize = |field: &str| {
            field
                .parse::<usize>()
                .map_err(|_| invalid_input("V21 feasibility sample integer is invalid"))
        };
        let parse_u64 = |field: &str| {
            field
                .parse::<u64>()
                .map_err(|_| invalid_input("V21 feasibility sample integer is invalid"))
        };
        let arm_index = parse_usize(fields[1])?;
        let query_index = parse_usize(fields[2])?;
        if arm_index != ordinal / query_count
            || query_index != ordinal % query_count
            || arm_index >= reports.len()
            || parse_usize(fields[3])? != identity.query_source_indices[query_index]
        {
            return Err(invalid_input("V21 feasibility sample ordering drifted"));
        }
        let selected_rows = parse_u64(fields[5])?;
        let physical_bytes = parse_u64(fields[10])?;
        let expected_sample = &reports[arm_index].samples[query_index];
        if parse_usize(fields[4])? != expected_sample.routed_cells
            || selected_rows != u64::from(expected_sample.selected_rows)
            || parse_usize(fields[6])? != expected_sample.selected_bundles
            || parse_usize(fields[7])? != expected_sample.primary_requests
            || parse_usize(fields[8])? != expected_sample.maximum_actual_requests
            || parse_u64(fields[9])? != expected_sample.selected_bytes
            || physical_bytes != expected_sample.physical_bytes
            || parse_usize(fields[11])? != expected_sample.gt_hits
            || parse_usize(fields[12])? != expected_sample.recall_hits
            || fields[13] != v21_limiting_bound(expected_sample.limiting_bound)
        {
            return Err(invalid_input(
                "V21 feasibility sample fields failed recomputation",
            ));
        }
        let transient = selected_rows
            .checked_mul(row_bytes)
            .and_then(|decoded| decoded.checked_add(physical_bytes))
            .ok_or_else(|| invalid_input("V21 feasibility sample memory overflows"))?;
        counts[arm_index] += 1;
        gt_hits[arm_index] = gt_hits[arm_index]
            .checked_add(parse_usize(fields[11])?)
            .ok_or_else(|| invalid_input("V21 feasibility GT hits overflow"))?;
        recall_hits[arm_index] = recall_hits[arm_index]
            .checked_add(parse_usize(fields[12])?)
            .ok_or_else(|| invalid_input("V21 feasibility recall hits overflow"))?;
        maximum_requests[arm_index] = maximum_requests[arm_index].max(parse_usize(fields[8])?);
        maximum_physical[arm_index] = maximum_physical[arm_index].max(physical_bytes);
        maximum_transient[arm_index] = maximum_transient[arm_index].max(transient);
    }
    for (arm_index, arm_summary) in summary.arms.iter().enumerate() {
        let denominator = (query_count * RECALL_K) as f64;
        if counts[arm_index] != query_count
            || arm_summary.gt_coverage != gt_hits[arm_index] as f64 / denominator
            || arm_summary.recall_at_10 != recall_hits[arm_index] as f64 / denominator
            || arm_summary.maximum_actual_requests != maximum_requests[arm_index]
            || arm_summary.maximum_physical_bytes != maximum_physical[arm_index]
            || arm_summary.projected_query_transient_bytes != maximum_transient[arm_index]
        {
            return Err(invalid_input(
                "V21 feasibility sample aggregates failed recomputation",
            ));
        }
    }
    Ok(())
}

fn publish_exclusive_file_set<const N: usize>(
    output_dir: &Path,
    destinations: &[PathBuf; N],
    payloads: &[Vec<u8>; N],
) -> io::Result<()> {
    fs::create_dir_all(output_dir)?;
    reject_existing_destinations(destinations)?;
    let temporary = destinations
        .iter()
        .map(|path| {
            let file_name = path
                .file_name()
                .and_then(|name| name.to_str())
                .ok_or_else(|| invalid_input("evidence destination has no UTF-8 filename"))?;
            Ok(path.with_file_name(format!(".{file_name}.tmp-{}", std::process::id())))
        })
        .collect::<io::Result<Vec<_>>>()?;
    let cleanup_temporary = || {
        for path in &temporary {
            let _ = fs::remove_file(path);
        }
    };
    for (path, bytes) in temporary.iter().zip(payloads) {
        let result = (|| {
            let mut file = File::options().create_new(true).write(true).open(path)?;
            file.write_all(bytes)?;
            file.sync_all()
        })();
        if let Err(error) = result {
            cleanup_temporary();
            return Err(error);
        }
    }
    let reloaded = match temporary
        .iter()
        .map(fs::read)
        .collect::<io::Result<Vec<_>>>()
    {
        Ok(reloaded) => reloaded,
        Err(error) => {
            cleanup_temporary();
            return Err(error);
        }
    };
    if reloaded.as_slice() != payloads {
        cleanup_temporary();
        return Err(invalid_input(
            "temporary evidence changed before publication",
        ));
    }
    let mut linked = Vec::new();
    for (temporary, destination) in temporary.iter().zip(destinations) {
        if let Err(error) = fs::hard_link(temporary, destination) {
            for path in linked {
                let _ = fs::remove_file(path);
            }
            cleanup_temporary();
            return Err(error);
        }
        linked.push(destination);
    }
    cleanup_temporary();
    File::open(output_dir)?.sync_all()?;
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
    let cache_cohort_size = query_sample_cache_cohort_size(
        config.cache_profile,
        phase,
        config.disk_cache_max_bytes,
        summary.samples.len(),
    )
    .map_err(|error| io::Error::other(error.to_string()))?;
    let cache_cohort_count = if cache_cohort_size == 0 {
        0
    } else {
        summary.samples.len().div_ceil(cache_cohort_size)
    };
    for (sample_index, sample) in summary.samples.iter().enumerate() {
        let cache_cohort_index = sample_index.checked_div(cache_cohort_size).unwrap_or(0);
        let query_source_index = query_source_indices.get(sample_index).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "query sample has no source-index proof",
            )
        })?;
        writeln!(
            writer,
            "{PRODUCTION_BENCH_SCHEMA_VERSION},{},{},{phase},{mode},{nprobe},{max_candidates},{sample_index},{cache_cohort_index},{cache_cohort_size},{cache_cohort_count},{query_source_index},{:.6},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
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
            format_args!(
                "{},{}",
                sample.backing_reads, sample.decoded_cache_bytes_read
            ),
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
            sample.global_leaf_exact_cells,
            sample.global_leaf_exact_cards,
            sample.global_leaf_deepest_winning_card_rank,
            sample.global_leaf_exact_groups,
            sample.global_leaf_exact_selected_bytes,
            sample.global_leaf_exact_speculative_bytes,
            sample.timings.csv_fields(),
        )?;
    }
    Ok(())
}

fn run_recall_cache_phases(
    config: &ResolvedConfig,
    dataset: &Dataset,
    index: Option<BorsukIndex>,
    options: SearchOptions,
    preload_complete: bool,
) -> BenchResult<Vec<(&'static str, QuerySummary)>> {
    match config.cache_profile {
        BenchmarkCacheProfile::Uncached => {
            drop(index);
            return Ok(vec![(
                "uncached",
                run_uncached_queries(config, dataset, options, dataset.queries.len())?,
            )]);
        }
        BenchmarkCacheProfile::DiskCached => {
            drop(index);
            return Ok(vec![(
                "disk_cached",
                run_disk_cached_queries(config, dataset, options)?,
            )]);
        }
        BenchmarkCacheProfile::MixedCoverage => {
            drop(index);
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
    let bounded_decoded = uses_bounded_decoded_cache_phases(
        memory_preloaded,
        options.mode.leaf_mode(),
        effective_segment_cache_budget(config),
    );
    if !memory_preloaded && !bounded_decoded {
        let uncached_options = options.clone();
        return execute_isolated_recall_cache_phases(
            index,
            move || run_uncached_queries(config, dataset, uncached_options, dataset.queries.len()),
            move || run_disk_cached_queries(config, dataset, options),
        );
    }
    let index = index
        .as_ref()
        .ok_or_else(|| invalid_input("recall execution has no serving index"))?;
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
    if bounded_decoded {
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
    unreachable!("combined recall cache phase selection is exhaustive")
}

fn execute_isolated_recall_cache_phases<Handle, Summary>(
    outer: Option<Handle>,
    uncached: impl FnOnce() -> BenchResult<Summary>,
    disk_cached: impl FnOnce() -> BenchResult<Summary>,
) -> BenchResult<Vec<(&'static str, Summary)>> {
    drop(outer);
    Ok(vec![
        ("uncached", uncached()?),
        ("disk_cached", disk_cached()?),
    ])
}

fn run_uncached_queries(
    config: &ResolvedConfig,
    dataset: &Dataset,
    options: SearchOptions,
    query_count: usize,
) -> BenchResult<QuerySummary> {
    execute_uncached_query_sequence(
        query_count.min(dataset.queries.len()),
        || reset_cache(&config.cache_dir).map_err(Into::into),
        || open_serving_index(config, ServingMetadataPreparation::DeferredCodePlanes),
        |index, query_index| {
            run_queries(
                index,
                &dataset.queries[query_index..query_index + 1],
                Some(&dataset.ground_truth[query_index..query_index + 1]),
                options.clone(),
            )
        },
    )
}

fn execute_uncached_query_sequence<Handle>(
    query_count: usize,
    mut reset: impl FnMut() -> BenchResult<()>,
    mut open: impl FnMut() -> BenchResult<Handle>,
    mut measure: impl FnMut(&Handle, usize) -> BenchResult<QuerySummary>,
) -> BenchResult<QuerySummary> {
    let mut summary = QuerySummary::default();
    for query_index in 0..query_count {
        reset()?;
        {
            let measurement = open()?;
            summary.absorb(measure(&measurement, query_index)?);
        }
    }
    Ok(summary)
}

#[allow(clippy::too_many_arguments)] // Keep every ordered cache-lifecycle boundary testable.
fn execute_disk_cached_query_cohorts<Handle>(
    query_count: usize,
    cohort_size: usize,
    mut reset: impl FnMut() -> BenchResult<()>,
    mut open: impl FnMut() -> BenchResult<Handle>,
    mut clear_disk_cache_after_open: impl FnMut(&Handle) -> BenchResult<()>,
    mut prime: impl FnMut(&Handle, usize) -> BenchResult<()>,
    mut clear_query_retained_state: impl FnMut(&Handle) -> BenchResult<()>,
    mut measure: impl FnMut(&Handle, usize) -> BenchResult<QuerySummary>,
) -> BenchResult<QuerySummary> {
    if query_count == 0 || cohort_size != query_count {
        return Err(invalid_input(
            "disk-cached recall requires one complete nonempty query cohort",
        )
        .into());
    }
    let mut summary = QuerySummary::default();
    reset()?;
    {
        let handle = open()?;
        // Opening prepares serving authority through the read-through cache.
        // Discard the disk-resident product of that excluded setup while
        // deliberately retaining RAM-resident metadata on this handle.
        clear_disk_cache_after_open(&handle)?;
        for query_index in 0..query_count {
            // Every primer must exercise the same cold query-retained state as
            // its measured query. Otherwise an earlier primer's decoded plane
            // can hide disk keys that the later measurement will require.
            clear_query_retained_state(&handle)?;
            prime(&handle, query_index)?;
        }
        for query_index in 0..query_count {
            clear_query_retained_state(&handle)?;
            let measured = measure(&handle, query_index)?;
            validate_disk_cached_query(query_index, &measured)?;
            summary.absorb(measured);
        }
    }
    Ok(summary)
}

#[allow(clippy::too_many_arguments)] // Keep every ordered cache-lifecycle boundary testable.
fn execute_disk_cached_concurrency_profiles<Handle>(
    query_count: usize,
    worker_profiles: &[usize],
    mut reset: impl FnMut() -> BenchResult<()>,
    mut open: impl FnMut() -> BenchResult<Handle>,
    mut clear_disk_cache_after_open: impl FnMut(&Handle) -> BenchResult<()>,
    mut prime: impl FnMut(&Handle, usize) -> BenchResult<()>,
    mut clear_query_retained_state: impl FnMut(&Handle) -> BenchResult<()>,
    mut measure: impl FnMut(
        &Handle,
        &[usize],
        usize,
    ) -> BenchResult<(Vec<ConcurrencyMeasurement>, Duration)>,
) -> BenchResult<Vec<(usize, Vec<ConcurrencyMeasurement>, Duration)>> {
    if query_count == 0 || worker_profiles.is_empty() {
        return Err(invalid_input(
            "disk-cached concurrency requires one complete nonempty query cohort and worker schedule",
        )
        .into());
    }
    let cohort = (0..query_count).collect::<Vec<_>>();
    let mut profiles = Vec::with_capacity(worker_profiles.len());
    reset()?;
    {
        let handle = open()?;
        // Keep RAM-resident prepared metadata, but remove disk-resident setup
        // objects so they cannot evict any query in the complete primed cohort.
        clear_disk_cache_after_open(&handle)?;
        for &query_index in &cohort {
            // Populate disk authority from a cleared retained state for every
            // serial primer. The concurrent wave begins only after the final
            // clear below, while each public search owns isolated I/O counters.
            clear_query_retained_state(&handle)?;
            prime(&handle, query_index)?;
        }
        for &workers in worker_profiles {
            clear_query_retained_state(&handle)?;
            let (wave, elapsed) = measure(&handle, &cohort, workers)?;
            for sample in &wave {
                validate_disk_cached_observation(
                    sample.query_source_index,
                    sample.network_gets,
                    sample.disk_cache_reads,
                )?;
            }
            profiles.push((workers, wave, elapsed));
        }
    }
    Ok(profiles)
}

// Warm recall primes the complete registered query set once, clearing decoded
// query state before every primer so that no primer inherits a RAM-only path.
// Each measured query repeats that cleared state and must be served from the
// already-populated local disk tier without any backing-store read.
fn disk_cached_query_cohort_size(
    disk_cache_max_bytes: Option<u64>,
    expected_queries: usize,
) -> BenchResult<usize> {
    disk_cached_concurrency_cohort_size(disk_cache_max_bytes, expected_queries)
}

// Concurrent measurements prime the complete query set once, then measure one
// steady worker pipeline. Reserve a conservative 48 MiB per query: the 32 MiB
// physical read ceiling plus 50% for authenticated cache framing, index/control
// objects, and entry accounting. Only three quarters of the configured disk
// budget funds query entries; the remaining quarter is explicit headroom.
const DISK_CACHED_QUERY_CACHE_AUTHORITY_BYTES: u64 = 48 * 1024 * 1024;

fn disk_cached_concurrency_cohort_size(
    disk_cache_max_bytes: Option<u64>,
    expected_queries: usize,
) -> BenchResult<usize> {
    if expected_queries == 0 {
        return Err(invalid_input("disk-cached concurrency query set must not be empty").into());
    }
    let max_bytes = disk_cache_max_bytes
        .filter(|bytes| *bytes > 0)
        .ok_or_else(|| {
            invalid_input("disk-cached measurement requires a positive local disk-cache budget")
        })?;
    let cohort_bytes = max_bytes.saturating_mul(3) / 4;
    let safe_queries = usize::try_from(cohort_bytes / DISK_CACHED_QUERY_CACHE_AUTHORITY_BYTES)
        .unwrap_or(usize::MAX);
    if expected_queries > safe_queries {
        return Err(invalid_input(&format!(
            "disk-cached query count {expected_queries} exceeds the cache-safe complete-query size {safe_queries}"
        ))
        .into());
    }
    Ok(expected_queries)
}

fn query_sample_cache_cohort_size(
    cache_profile: BenchmarkCacheProfile,
    phase: &str,
    disk_cache_max_bytes: Option<u64>,
    expected_queries: usize,
) -> BenchResult<usize> {
    if cache_profile == BenchmarkCacheProfile::DiskCached && phase == "disk_cached" {
        disk_cached_query_cohort_size(disk_cache_max_bytes, expected_queries)
    } else {
        Ok(0)
    }
}

fn run_disk_cached_queries(
    config: &ResolvedConfig,
    dataset: &Dataset,
    options: SearchOptions,
) -> BenchResult<QuerySummary> {
    let cohort_size =
        disk_cached_query_cohort_size(config.disk_cache_max_bytes, dataset.queries.len())?;
    execute_disk_cached_query_cohorts(
        dataset.queries.len(),
        cohort_size,
        || reset_cache(&config.cache_dir).map_err(Into::into),
        || open_serving_index(config, ServingMetadataPreparation::Complete),
        |index| index.clear_local_read_through_cache().map_err(Into::into),
        |index, query_index| {
            let _ = run_queries(
                index,
                &dataset.queries[query_index..query_index + 1],
                None,
                options.clone(),
            )?;
            Ok(())
        },
        |index| index.clear_query_retained_state().map_err(Into::into),
        |index, query_index| {
            run_queries(
                index,
                &dataset.queries[query_index..query_index + 1],
                Some(&dataset.ground_truth[query_index..query_index + 1]),
                options.clone(),
            )
        },
    )
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

fn write_cold_warm_csv(config: &ResolvedConfig, dataset: &Dataset) -> BenchResult<()> {
    let path = config.output_dir.join("bench_cache_states.csv");
    let mut writer = csv_writer(&path)?;
    writeln!(writer, "{CACHE_STATE_HEADER}")?;
    let options = serving_options(config);
    let index = recall_cache_profile_needs_outer_handle(
        config.cache_profile,
        config.preload_serving,
        config.cache_execution,
        options.mode.leaf_mode(),
        effective_segment_cache_budget(config),
    )
    .then(|| open_serving_index(config, ServingMetadataPreparation::Complete))
    .transpose()?;
    let preload_complete = if let Some(index) = index.as_ref() {
        if config.preload_serving {
            warm_all_segments(index)?.coverage_complete
        } else {
            let _ = index.prepare_serving_metadata()?;
            false
        }
    } else {
        false
    };
    match config.cache_profile {
        BenchmarkCacheProfile::Uncached => {
            drop(index);
            let summary = run_uncached_queries(config, dataset, options, config.uncached_queries)?;
            write_cache_state_row(&mut writer, config, "uncached", &summary)?;
            writer.flush()?;
            eprintln!("wrote {} rows=1", path.display());
            return Ok(());
        }
        BenchmarkCacheProfile::DiskCached => {
            drop(index);
            let summary = run_disk_cached_queries(config, dataset, options)?;
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
    if !memory_preloaded && !bounded_decoded {
        let uncached_options = options.clone();
        let phases = execute_isolated_recall_cache_phases(
            index,
            move || {
                run_uncached_queries(config, dataset, uncached_options, config.uncached_queries)
            },
            move || run_disk_cached_queries(config, dataset, options),
        )?;
        for (phase, summary) in phases {
            write_cache_state_row(&mut writer, config, phase, &summary)?;
        }
        writer.flush()?;
        eprintln!("wrote {} rows=2", path.display());
        return Ok(());
    }
    let index = index
        .as_ref()
        .ok_or_else(|| invalid_input("cache-state execution has no serving index"))?;
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
        unreachable!("combined cache-state phase selection is exhaustive")
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

fn validate_disk_cached_query(query_index: usize, summary: &QuerySummary) -> BenchResult<()> {
    validate_disk_cached_observation(
        query_index,
        summary.billable_requests,
        summary.disk_cache_reads,
    )
}

fn validate_disk_cached_observation(
    query_identity: usize,
    network_gets: impl Into<u128>,
    disk_cache_reads: impl Into<u128>,
) -> BenchResult<()> {
    let network_gets = network_gets.into();
    let disk_cache_reads = disk_cache_reads.into();
    if network_gets != 0 {
        return Err(invalid_input(&format!(
            "disk-cached query {query_identity} performed network I/O: network_gets={network_gets}"
        ))
        .into());
    }
    if disk_cache_reads == 0 {
        return Err(invalid_input(&format!(
            "disk-cached query {query_identity} performed no local disk-cache reads"
        ))
        .into());
    }
    Ok(())
}

fn cache_state_summary_enabled(skip_recall: bool, cache_profile: BenchmarkCacheProfile) -> bool {
    !skip_recall && cache_profile != BenchmarkCacheProfile::MixedCoverage
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

fn measure_concurrency_wave(
    config: &ResolvedConfig,
    dataset: &Dataset,
    index: &Arc<BorsukIndex>,
    ground_truth: &Arc<Vec<Vec<String>>>,
    query_indices: &[usize],
    workers: usize,
    position_offset: usize,
) -> BenchResult<(Vec<ConcurrencyMeasurement>, Duration)> {
    let hot_count = match config.cache_profile {
        BenchmarkCacheProfile::All | BenchmarkCacheProfile::DiskCached => dataset.queries.len(),
        BenchmarkCacheProfile::Uncached => 0,
        BenchmarkCacheProfile::MixedCoverage => {
            dataset.queries.len() * config.cache_coverage_percent / 100
        }
    };
    let active_workers = workers.min(query_indices.len()).max(1);
    let query_indices = Arc::new(query_indices.to_vec());
    let query_source_indices = Arc::clone(&dataset.query_source_indices);
    let ready = Arc::new(Barrier::new(active_workers + 1));
    let mut handles = Vec::with_capacity(active_workers);
    for worker in 0..active_workers {
        let worker_index = Arc::clone(index);
        let queries = Arc::clone(&dataset.queries);
        let query_indices = Arc::clone(&query_indices);
        let ground_truth = Arc::clone(ground_truth);
        let query_source_indices = Arc::clone(&query_source_indices);
        let ready = Arc::clone(&ready);
        let options = serving_options(config);
        handles.push(thread::spawn(
            move || -> Result<Vec<ConcurrencyMeasurement>, String> {
                ready.wait();
                let mut measurements = Vec::new();
                for position in (worker..query_indices.len()).step_by(active_workers) {
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
                        position: position_offset + position,
                        query_source_index: query_source_indices[query_index],
                        target_hot_set_member: query_index < hot_count,
                        latency_ms: elapsed_ms(query_started),
                        recall,
                        bytes_read: query_scoped_physical_bytes_read(
                            report.decoded_cache_bytes_read,
                            report.disk_cache_bytes_read,
                            report.backing_bytes_read,
                        ),
                        decoded_cache_hits: report.decoded_cache_hits,
                        disk_cache_reads: report.disk_cache_reads,
                        backing_reads: report.backing_reads,
                        decoded_cache_bytes_read: report.decoded_cache_bytes_read,
                        disk_cache_bytes_read: report.disk_cache_bytes_read,
                        backing_bytes_read: report.backing_bytes_read,
                        network_gets: report.requests.gets.saturating_add(report.requests.heads),
                        global_leaf_directory_reads: report.global_leaf_directory_reads,
                        global_leaf_directory_bytes: report.global_leaf_directory_bytes,
                        global_leaf_code_pages_read: report.global_leaf_code_pages_read,
                        global_leaf_code_bytes: report.global_leaf_code_bytes,
                        global_leaf_pages_read: report.global_leaf_pages_read,
                        global_leaf_page_bytes: report.global_leaf_page_bytes,
                        global_leaf_waves: report.global_leaf_waves,
                        global_leaf_continuations: report.global_leaf_continuations,
                        global_leaf_exact_scores: report.global_leaf_exact_scores,
                        global_leaf_code_requests: report.global_leaf_code_requests,
                        global_leaf_exact_requests: report.global_leaf_exact_requests,
                        global_leaf_exact_cells: report.global_leaf_exact_cells,
                        global_leaf_exact_cards: report.global_leaf_exact_cards,
                        global_leaf_deepest_winning_card_rank: report
                            .global_leaf_deepest_winning_card_rank,
                        global_leaf_exact_groups: report.global_leaf_exact_groups,
                        global_leaf_exact_selected_bytes: report.global_leaf_exact_selected_bytes,
                        global_leaf_exact_speculative_bytes: report
                            .global_leaf_exact_speculative_bytes,
                        execution_engine: execution_engine_label(&report).to_string(),
                        collection_resident_bytes: report.collection_resident_bytes,
                        retained_bytes: report.retained_bytes,
                        retained_capacity_bytes: report.retained_capacity_bytes,
                        retained_peak_bytes: report.retained_peak_bytes,
                        transient_bytes: report.transient_bytes,
                        transient_capacity_bytes: report.transient_capacity_bytes,
                        transient_peak_bytes: report.transient_peak_bytes,
                        timings: QueryStageTimings::from_report(&report),
                    });
                }
                Ok(measurements)
            },
        ));
    }
    let started = Instant::now();
    ready.wait();
    let measurements = join_concurrency_workers(handles)?;
    Ok((measurements, started.elapsed()))
}

fn join_concurrency_workers(
    handles: Vec<thread::JoinHandle<Result<Vec<ConcurrencyMeasurement>, String>>>,
) -> BenchResult<Vec<ConcurrencyMeasurement>> {
    let mut measurements = Vec::new();
    let mut first_error = None;
    for handle in handles {
        match handle.join() {
            Ok(Ok(mut worker_measurements)) => measurements.append(&mut worker_measurements),
            Ok(Err(error)) if first_error.is_none() => {
                first_error = Some(invalid_input(&format!(
                    "concurrency worker failed: {error}"
                )));
            }
            Err(_) if first_error.is_none() => {
                first_error = Some(invalid_input("concurrency benchmark worker panicked"));
            }
            Ok(Err(_)) | Err(_) => {}
        }
    }
    if let Some(error) = first_error {
        return Err(error.into());
    }
    Ok(measurements)
}

fn validate_lifecycle_only(
    lifecycle_only: bool,
    build_index: bool,
    build_only: bool,
    recall_only: bool,
    skip_recall: bool,
    read_only: bool,
    insert_only: bool,
) -> BenchResult<()> {
    if lifecycle_only
        && (build_index || build_only || recall_only || !skip_recall || read_only || insert_only)
    {
        return Err(invalid_input(
            "BORSUK_BENCH_LIFECYCLE_ONLY requires an existing index and skip-recall, and cannot be combined with build-only, recall-only, read-only, or insert-only",
        )
        .into());
    }
    Ok(())
}

fn write_concurrency_csv(config: &ResolvedConfig, dataset: &Dataset) -> BenchResult<()> {
    let path = config.output_dir.join("bench_concurrency.csv");
    let mut writer = csv_writer(&path)?;
    writeln!(writer, "{CONCURRENCY_HEADER}")?;
    let samples_path = config.output_dir.join("bench_concurrency_samples.csv");
    let mut samples_writer = csv_writer(&samples_path)?;
    writeln!(samples_writer, "{CONCURRENCY_SAMPLE_HEADER}")?;
    let ground_truth = Arc::new(dataset.ground_truth.clone());
    let cache_cohort_size = if config.cache_profile == BenchmarkCacheProfile::DiskCached {
        disk_cached_concurrency_cohort_size(config.disk_cache_max_bytes, dataset.queries.len())?
    } else {
        0
    };
    let cache_cohort_count = usize::from(cache_cohort_size > 0);
    let serving_metadata_preparation = shared_serving_metadata_preparation(config.cache_profile);
    let mut disk_cached_profiles = if config.cache_profile == BenchmarkCacheProfile::DiskCached {
        VecDeque::from(execute_disk_cached_concurrency_profiles(
            dataset.queries.len(),
            &config.concurrency,
            || reset_cache(&config.cache_dir).map_err(Into::into),
            || {
                Ok(Arc::new(open_serving_index(
                    config,
                    serving_metadata_preparation,
                )?))
            },
            |index| index.clear_local_read_through_cache().map_err(Into::into),
            |index, query_index| {
                let _ = index
                    .search_with_report(&dataset.queries[query_index], serving_options(config))?;
                Ok(())
            },
            |index| index.clear_query_retained_state().map_err(Into::into),
            |index, cohort, workers| {
                measure_concurrency_wave(config, dataset, index, &ground_truth, cohort, workers, 0)
            },
        )?)
    } else {
        VecDeque::new()
    };
    for &workers in &config.concurrency {
        let (mut measurements, measured_wall) =
            if config.cache_profile == BenchmarkCacheProfile::DiskCached {
                let (profile_workers, measurements, elapsed) = disk_cached_profiles
                    .pop_front()
                    .ok_or_else(|| invalid_input("disk-cached concurrency profile is missing"))?;
                if profile_workers != workers {
                    return Err(invalid_input(
                        "disk-cached concurrency profile order differs from its authority",
                    )
                    .into());
                }
                (measurements, elapsed)
            } else {
                let (index, query_indices) = execute_concurrency_cache_setup(
                    || reset_cache(&config.cache_dir).map_err(Into::into),
                    || {
                        Ok(Arc::new(open_serving_index(
                            config,
                            serving_metadata_preparation,
                        )?))
                    },
                    |index| prepare_concurrency_cache_state(config, dataset, index),
                )?;
                let (wave, elapsed) = measure_concurrency_wave(
                    config,
                    dataset,
                    &index,
                    &ground_truth,
                    &query_indices,
                    workers,
                    0,
                )?;
                (wave, elapsed)
            };
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
        let wall_seconds = measured_wall.as_secs_f64();
        let total_queries = latencies_ms.len();
        let qps = if wall_seconds == 0.0 {
            total_queries as f64
        } else {
            total_queries as f64 / wall_seconds
        };
        for (sample_index, measurement) in measurements.iter().enumerate() {
            let cache_cohort_index = sample_index.checked_div(cache_cohort_size).unwrap_or(0);
            writeln!(
                samples_writer,
                "{PRODUCTION_BENCH_SCHEMA_VERSION},{},{},{},{},{},{},{workers},{sample_index},{cache_cohort_index},{cache_cohort_size},{cache_cohort_count},{},{},{:.6},{:.6},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
                config.global_scan_codec,
                config.cache_execution,
                config.cache_profile.as_str(),
                config.cache_coverage_percent,
                config.serving_nprobe,
                config.serving_candidates,
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
                measurement.physical_exact_csv_fields(),
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
                measurement.timings.csv_fields(),
            )?;
        }
        writeln!(
            writer,
            "{PRODUCTION_BENCH_SCHEMA_VERSION},{},{},{},{},{},{},{},{},{},{},{workers},{total_queries},{qps:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3}",
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
            config.serving_nprobe,
            config.serving_candidates,
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

fn execute_concurrency_cache_setup<Handle, State>(
    mut reset: impl FnMut() -> BenchResult<()>,
    mut open: impl FnMut() -> BenchResult<Handle>,
    mut prepare: impl FnMut(&Handle) -> BenchResult<State>,
) -> BenchResult<(Handle, State)> {
    reset()?;
    let handle = open()?;
    let state = prepare(&handle)?;
    Ok((handle, state))
}

fn prepare_concurrency_cache_state(
    config: &ResolvedConfig,
    dataset: &Dataset,
    index: &BorsukIndex,
) -> BenchResult<Arc<Vec<usize>>> {
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
            // Coverage is a per-handle hot/cold experiment even under the aggregate
            // `All` profile. Completing every plane here would erase its cold cohort.
            let index = open_serving_index(config, ServingMetadataPreparation::DeferredCodePlanes)?;
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
    let write_ops = lifecycle_write_operation_count(
        dataset.train_count,
        LifecycleDeltaLayout {
            dimensions: dataset.meta.dim,
            segment_max_vectors: config.segment_max,
        },
        config.write_ops,
        config.lifecycle_writers,
        config.write_batch_size,
        config.update_percent,
        config.delete_percent,
    )?;
    let update_ops = percentage_operation_count(write_ops, config.update_percent)?;
    let delete_ops = percentage_operation_count(write_ops, config.delete_percent)?;
    if update_ops.saturating_add(delete_ops) > dataset.train_count {
        return Err(
            invalid_input("lifecycle update and delete cohorts exceed the base corpus").into(),
        );
    }
    let mutation_queries = &dataset.queries[..dataset.queries.len().min(MUTATION_QUERY_SAMPLES)];
    let mut query_stages = vec![(
        "baseline",
        lifecycle_phase("baseline-query", || {
            run_queries(index, mutation_queries, None, serving_options(config))
        })?,
    )];
    let mut writers = lifecycle_phase("open-writers", || {
        open_lifecycle_writer_handles(
            &config.uri,
            config.lifecycle_writers,
            config.ram_budget_bytes,
        )
    })?;
    let stats_before_insert = index.stats();
    let mut rows = Vec::with_capacity(7);
    let insert = lifecycle_phase("insert", || {
        measure_inserts(config, dataset, &mut writers, write_ops)
    })?;
    let progress = LifecycleProgress::start("insert-refresh");
    let searchability_refresh_started = Instant::now();
    let _ = index.refresh()?;
    let searchability_refresh_ms = elapsed_ms(searchability_refresh_started);
    progress.complete();
    let time_to_searchable_ms = insert.row.wall_ms + searchability_refresh_ms;
    let (searchable_samples, searchable_hits) =
        lifecycle_phase("insert-verify", || -> BenchResult<(usize, usize)> {
            let observer = BorsukIndex::open_with_options(
                &config.uri,
                lifecycle_writer_open_options(config.ram_budget_bytes),
            )?;
            let result = verify_insert_visibility(config, dataset, &observer, write_ops)?;
            drop(observer);
            Ok(result)
        })?;
    let searchable_fraction = mean(searchable_hits as f64, searchable_samples);
    let insert_wall_ms = insert.row.wall_ms;
    let foreground_bytes_written = insert.row.bytes_written;
    let first_batch_publish_ms = insert.first_batch_publish_ms;
    rows.push(insert.row);
    query_stages.push((
        "after-insert-searchable",
        lifecycle_phase("after-insert-query", || {
            run_queries(index, mutation_queries, None, serving_options(config))
        })?,
    ));

    // WAL publication makes rows durable/searchable. Flushing materializes
    // only the bounded tail into immutable segment-local indexes; it does not
    // rebuild the corpus-wide base.
    let requests_before = index.request_counts();
    let bytes_read_before = index.backing_bytes_read();
    let bytes_written_before = index.put_payload_bytes();
    let progress = LifecycleProgress::start("flush");
    let delta_flush_started = Instant::now();
    index.flush()?;
    let delta_flush_ms = elapsed_ms(delta_flush_started);
    progress.complete();
    let flush_requests = index.request_counts().delta(&requests_before);
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
    rows.push(WriteRow {
        op: "flush",
        ops: 1,
        wall_ms: delta_flush_ms,
        latencies_ms: vec![delta_flush_ms],
        samples: vec![WriteSample {
            op: "flush",
            writer_index: 0,
            wave_index: 0,
            batch_index: 0,
            batch_records: write_ops,
            batch_latency_ms: delta_flush_ms,
            requests: flush_requests,
        }],
        requests: flush_requests,
        bytes_read: index.backing_bytes_read().saturating_sub(bytes_read_before),
        bytes_written: index
            .put_payload_bytes()
            .saturating_sub(bytes_written_before),
    });
    query_stages.push((
        "after-fully-indexed-delta",
        lifecycle_phase("after-flush-query", || {
            run_queries(index, mutation_queries, None, serving_options(config))
        })?,
    ));

    // Corpus-wide consolidation is a distinct maintenance metric. It must not
    // be mislabeled as time-to-indexed for the newly inserted rows.
    let requests_before = index.request_counts();
    let bytes_read_before = index.backing_bytes_read();
    let bytes_written_before = index.put_payload_bytes();
    let progress = LifecycleProgress::start("consolidate");
    let consolidation_started = Instant::now();
    index.finish_bulk_load()?;
    let consolidation_ms = elapsed_ms(consolidation_started);
    progress.complete();
    let consolidation_requests = index.request_counts().delta(&requests_before);
    let consolidated_global_bytes = index.stats().global_scan_bytes;
    rows.push(WriteRow {
        op: "consolidate",
        ops: 1,
        wall_ms: consolidation_ms,
        latencies_ms: vec![consolidation_ms],
        samples: vec![WriteSample {
            op: "consolidate",
            writer_index: 0,
            wave_index: 0,
            batch_index: 0,
            batch_records: index.stats().records,
            batch_latency_ms: consolidation_ms,
            requests: consolidation_requests,
        }],
        requests: consolidation_requests,
        bytes_read: index.backing_bytes_read().saturating_sub(bytes_read_before),
        bytes_written: index
            .put_payload_bytes()
            .saturating_sub(bytes_written_before),
    });
    query_stages.push((
        "after-global-consolidation",
        lifecycle_phase("after-consolidate-query", || {
            run_queries(index, mutation_queries, None, serving_options(config))
        })?,
    ));
    let upsert = lifecycle_phase("upsert", || {
        measure_upserts(config, dataset, &mut writers, update_ops)
    })?;
    lifecycle_phase("upsert-refresh", || -> BenchResult<()> {
        let _ = index.refresh()?;
        Ok(())
    })?;
    let (upsert_samples, upsert_correct) =
        lifecycle_phase("upsert-verify", || -> BenchResult<(usize, usize)> {
            let observer = BorsukIndex::open_with_options(
                &config.uri,
                lifecycle_writer_open_options(config.ram_budget_bytes),
            )?;
            let result = verify_upsert_values(&observer, &upsert.expected_records)?;
            drop(observer);
            Ok(result)
        })?;
    rows.push(upsert.row);
    query_stages.push((
        "after-upsert",
        lifecycle_phase("after-upsert-query", || {
            run_queries(index, mutation_queries, None, serving_options(config))
        })?,
    ));
    rows.push(lifecycle_phase("delete", || {
        measure_deletes(config, &mut writers, update_ops, delete_ops)
    })?);
    lifecycle_phase("delete-refresh", || -> BenchResult<()> {
        let _ = index.refresh()?;
        Ok(())
    })?;
    let (delete_samples, delete_absent) =
        lifecycle_phase("delete-verify", || -> BenchResult<(usize, usize)> {
            let observer = BorsukIndex::open_with_options(
                &config.uri,
                lifecycle_writer_open_options(config.ram_budget_bytes),
            )?;
            let result = verify_delete_absence(config, &observer, update_ops, delete_ops)?;
            drop(observer);
            Ok(result)
        })?;
    query_stages.push((
        "after-delete",
        lifecycle_phase("after-delete-query", || {
            run_queries(index, mutation_queries, None, serving_options(config))
        })?,
    ));

    let requests_before = index.request_counts();
    let bytes_before = index.put_payload_bytes();
    let progress = LifecycleProgress::start("compact");
    let compact_started = Instant::now();
    let compact = index.compact(CompactionOptions::default())?;
    let compact_wall_ms = elapsed_ms(compact_started);
    progress.complete();
    let compact_requests = index.request_counts().delta(&requests_before);
    rows.push(WriteRow {
        op: "compact",
        ops: 1,
        wall_ms: compact_wall_ms,
        latencies_ms: vec![compact_wall_ms],
        samples: vec![WriteSample {
            op: "compact",
            writer_index: 0,
            wave_index: 0,
            batch_index: 0,
            batch_records: compact.records_rewritten,
            batch_latency_ms: compact_wall_ms,
            requests: compact_requests,
        }],
        requests: compact_requests,
        bytes_read: compact.bytes_read,
        bytes_written: index.put_payload_bytes().saturating_sub(bytes_before),
    });
    let compact_delete_absent = lifecycle_phase("compact-verify", || -> BenchResult<usize> {
        let observer = BorsukIndex::open_with_options(
            &config.uri,
            lifecycle_writer_open_options(config.ram_budget_bytes),
        )?;
        let (_, absent) = verify_delete_absence(config, &observer, update_ops, delete_ops)?;
        require_surviving_mutations(
            "compaction",
            verify_insert_visibility(config, dataset, &observer, write_ops)?,
            verify_upsert_values(&observer, &upsert.expected_records)?,
        )?;
        drop(observer);
        Ok(absent)
    })?;
    query_stages.push((
        "after-compact",
        lifecycle_phase("after-compact-query", || {
            run_queries(index, mutation_queries, None, serving_options(config))
        })?,
    ));

    let requests_before = index.request_counts();
    let bytes_read_before = index.backing_bytes_read();
    let bytes_before = index.put_payload_bytes();
    let progress = LifecycleProgress::start("purge");
    let purge_started = Instant::now();
    let purge = index.purge_with_report()?;
    let purge_wall_ms = elapsed_ms(purge_started);
    progress.complete();
    let purge_requests = index.request_counts().delta(&requests_before);
    rows.push(WriteRow {
        op: "purge",
        ops: 1,
        wall_ms: purge_wall_ms,
        latencies_ms: vec![purge_wall_ms],
        samples: vec![WriteSample {
            op: "purge",
            writer_index: 0,
            wave_index: 0,
            batch_index: 0,
            batch_records: purge.records_purged,
            batch_latency_ms: purge_wall_ms,
            requests: purge_requests,
        }],
        requests: purge_requests,
        bytes_read: index.backing_bytes_read().saturating_sub(bytes_read_before),
        bytes_written: index.put_payload_bytes().saturating_sub(bytes_before),
    });
    let purge_delete_absent = lifecycle_phase("purge-verify", || -> BenchResult<usize> {
        let observer = BorsukIndex::open_with_options(
            &config.uri,
            lifecycle_writer_open_options(config.ram_budget_bytes),
        )?;
        let (_, absent) = verify_delete_absence(config, &observer, update_ops, delete_ops)?;
        require_surviving_mutations(
            "purge",
            verify_insert_visibility(config, dataset, &observer, write_ops)?,
            verify_upsert_values(&observer, &upsert.expected_records)?,
        )?;
        drop(observer);
        Ok(absent)
    })?;
    query_stages.push((
        "after-purge",
        lifecycle_phase("after-purge-query", || {
            run_queries(index, mutation_queries, None, serving_options(config))
        })?,
    ));

    write_lifecycle_csv(
        config,
        dataset,
        write_ops,
        insert_wall_ms,
        first_batch_publish_ms,
        searchability_refresh_ms,
        time_to_searchable_ms,
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
            "{},{},{},{},{},{:.3},{ops_per_second:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.6},{},{},{},{},{},{},{}",
            row.op,
            config.lifecycle_writers,
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
            "{},{},{},{},{},{:.3},{:.6},{},{},{},{},{}",
            sample.op,
            sample.writer_index,
            sample.wave_index,
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
    searchability_refresh_ms: f64,
    time_to_searchable_ms: f64,
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
    let time_to_fully_indexed_ms = time_to_searchable_ms + delta_flush_ms;
    let time_to_consolidated_ms = time_to_fully_indexed_ms + consolidation_ms;
    let path = config.output_dir.join("bench_lifecycle.csv");
    let mut writer = csv_writer(&path)?;
    writeln!(writer, "{LIFECYCLE_HEADER}")?;
    writeln!(
        writer,
        "{},{},{inserted_vectors},{logical_vector_bytes},{insert_wall_ms:.3},{insert_vectors_per_s:.3},{first_batch_publish_ms:.3},{searchability_refresh_ms:.3},{time_to_searchable_ms:.3},{searchable_samples},{searchable_fraction:.6},{upsert_samples},{upsert_correct_fraction:.6},{delete_samples},{delete_absent_fraction:.6},{compact_delete_absent_fraction:.6},{purge_delete_absent_fraction:.6},{delta_flush_ms:.3},{time_to_fully_indexed_ms:.3},{wal_publish_bytes},{indexed_delta_bytes},{total_indexing_bytes},{write_amplification:.6},true,{consolidation_ms:.3},{time_to_consolidated_ms:.3},{consolidated_global_bytes},{consolidation_amplification:.6}",
        config.lifecycle_writers, config.write_batch_size,
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
    writers: &mut [BorsukIndex],
    count: usize,
) -> BenchResult<InsertMeasurement> {
    // The paired lifecycle factor deliberately compares the general upsert
    // control with the claim-free last-write-wins insert path from one binary.
    // All generated ids are absent in the cloned base, so both modes perform
    // equivalent logical inserts.
    let started = Instant::now();
    let mut samples = Vec::new();
    let mut bytes_written = 0_u64;
    let assignments = lifecycle_write_waves(count, config.write_batch_size, writers.len())?
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    let assignment_batch_sizes = assignments
        .iter()
        .map(|assignment| assignment.len)
        .collect::<Vec<_>>();
    let mut assignment_cursor = 0_usize;
    let mut pending = Vec::with_capacity(writers.len());
    stream_dataset_batches(
        config,
        dataset,
        count,
        Some(&assignment_batch_sizes),
        |offset, vectors| {
            let assignment = assignments
                .get(assignment_cursor)
                .copied()
                .ok_or_else(|| invalid_input("lifecycle insert source exceeded its schedule"))?;
            if assignment.offset != offset || assignment.len != vectors.len() {
                return Err(
                    invalid_input("lifecycle insert source differs from its schedule").into(),
                );
            }
            let ids = (offset..offset.saturating_add(vectors.len()))
                .map(|id| format!("bench-insert-{}", dataset.train_count.saturating_add(id)))
                .collect::<Vec<_>>();
            pending.push(PreparedRecordBatch {
                assignment,
                records: ids
                    .into_iter()
                    .zip(vectors)
                    .map(|(id, vector)| VectorRecord::new(id, vector))
                    .collect(),
            });
            assignment_cursor = assignment_cursor.saturating_add(1);
            let wave_complete = assignment_cursor == assignments.len()
                || assignments[assignment_cursor].writer_index == 0;
            if wave_complete {
                let wave_index = assignment.batch_index / writers.len();
                let completed = match config.lifecycle_insert_mode {
                    LifecycleInsertMode::GeneralUpsert => execute_upsert_wave(
                        "insert",
                        wave_index,
                        writers,
                        std::mem::take(&mut pending),
                    )?,
                    LifecycleInsertMode::ClaimFreePut => execute_put_wave(
                        "insert",
                        wave_index,
                        writers,
                        std::mem::take(&mut pending),
                    )?,
                };
                for (sample, written) in completed {
                    bytes_written = bytes_written.saturating_add(written);
                    samples.push(sample);
                }
            }
            Ok(())
        },
    )?;
    if assignment_cursor != assignments.len() || !pending.is_empty() {
        return Err(invalid_input("lifecycle insert schedule is incomplete").into());
    }
    samples.sort_by_key(|sample| sample.batch_index);
    let first_batch_publish_ms = first_logical_batch_publish_ms(&samples);
    let requests = request_counts_from_samples(&samples);
    let mut row = write_row_from_samples("insert", count, elapsed_ms(started), samples, requests);
    row.bytes_written = bytes_written;
    Ok(InsertMeasurement {
        row,
        first_batch_publish_ms,
    })
}

fn first_logical_batch_publish_ms(samples: &[WriteSample]) -> f64 {
    samples
        .iter()
        .find(|sample| sample.batch_index == 0)
        .map_or(0.0, |sample| sample.batch_latency_ms)
}

fn verify_insert_visibility(
    config: &ResolvedConfig,
    dataset: &Dataset,
    index: &BorsukIndex,
    count: usize,
) -> BenchResult<(usize, usize)> {
    let offsets =
        verification_offsets(count, 16, config.lifecycle_writers, config.write_batch_size)?;
    let ids = offsets
        .iter()
        .map(|&offset| {
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
    Ok((offsets.len(), visible))
}

fn require_surviving_mutations(
    phase: &str,
    inserts: (usize, usize),
    upserts: (usize, usize),
) -> BenchResult<()> {
    if inserts.0 == 0 || upserts.0 == 0 || inserts.0 != inserts.1 || upserts.0 != upserts.1 {
        return Err(invalid_input(&format!(
            "lifecycle {phase} lost an inserted or updated record"
        ))
        .into());
    }
    Ok(())
}

fn measure_upserts(
    config: &ResolvedConfig,
    dataset: &Dataset,
    writers: &mut [BorsukIndex],
    count: usize,
) -> BenchResult<UpsertMeasurement> {
    // Re-upsert the first `count` train vectors (nudged so it is a real MVCC
    // upsert), streaming from the selected standard source. Zero-norm vectors
    // are accepted like any other.
    let started = Instant::now();
    let mut samples = Vec::new();
    let mut bytes_written = 0_u64;
    let verification_offsets =
        verification_offsets(count, 16, config.lifecycle_writers, config.write_batch_size)?
            .into_iter()
            .collect::<BTreeSet<_>>();
    let mut expected_records = Vec::with_capacity(verification_offsets.len());
    let assignments = lifecycle_write_waves(count, config.write_batch_size, writers.len())?
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    let assignment_batch_sizes = assignments
        .iter()
        .map(|assignment| assignment.len)
        .collect::<Vec<_>>();
    let mut assignment_cursor = 0_usize;
    let mut pending = Vec::with_capacity(writers.len());
    stream_dataset_batches(
        config,
        dataset,
        count,
        Some(&assignment_batch_sizes),
        |offset, vectors| {
            let assignment = assignments
                .get(assignment_cursor)
                .copied()
                .ok_or_else(|| invalid_input("lifecycle upsert source exceeded its schedule"))?;
            if assignment.offset != offset || assignment.len != vectors.len() {
                return Err(
                    invalid_input("lifecycle upsert source differs from its schedule").into(),
                );
            }
            let mut records = Vec::with_capacity(vectors.len());
            for (position, mut vector) in vectors.into_iter().enumerate() {
                vector[0] += 1.0e-4;
                let id = offset.saturating_add(position).to_string();
                if verification_offsets.contains(&offset.saturating_add(position)) {
                    expected_records.push((id.clone(), vector.clone()));
                }
                records.push(VectorRecord::new(id, vector));
            }
            pending.push(PreparedRecordBatch {
                assignment,
                records,
            });
            assignment_cursor = assignment_cursor.saturating_add(1);
            let wave_complete = assignment_cursor == assignments.len()
                || assignments[assignment_cursor].writer_index == 0;
            if wave_complete {
                let wave_index = assignment.batch_index / writers.len();
                for (sample, written) in execute_upsert_wave(
                    "upsert",
                    wave_index,
                    writers,
                    std::mem::take(&mut pending),
                )? {
                    bytes_written = bytes_written.saturating_add(written);
                    samples.push(sample);
                }
            }
            Ok(())
        },
    )?;
    if assignment_cursor != assignments.len() || !pending.is_empty() {
        return Err(invalid_input("lifecycle upsert schedule is incomplete").into());
    }
    samples.sort_by_key(|sample| sample.batch_index);
    let requests = request_counts_from_samples(&samples);
    let mut row = write_row_from_samples("upsert", count, elapsed_ms(started), samples, requests);
    row.bytes_written = bytes_written;
    Ok(UpsertMeasurement {
        row,
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

fn verify_delete_absence(
    config: &ResolvedConfig,
    index: &BorsukIndex,
    base_offset: usize,
    count: usize,
) -> BenchResult<(usize, usize)> {
    let offsets =
        verification_offsets(count, 16, config.lifecycle_writers, config.write_batch_size)?;
    let ids = offsets
        .iter()
        .map(|offset| base_offset.saturating_add(*offset).to_string())
        .collect::<Vec<_>>();
    let absent = index
        .get_records(&ids)?
        .into_iter()
        .filter(Option::is_none)
        .count();
    Ok((offsets.len(), absent))
}

fn verification_offsets(
    count: usize,
    maximum_samples: usize,
    writers: usize,
    batch_size: usize,
) -> io::Result<Vec<usize>> {
    let writers = validate_lifecycle_writers(writers)?;
    if batch_size == 0 {
        return Err(invalid_input("lifecycle verification batch size is zero"));
    }
    let samples = count.min(maximum_samples.max(writers.saturating_mul(2)));
    if samples == 0 {
        return Ok(Vec::new());
    }
    let mut offsets = BTreeSet::new();
    let batch_count = count.div_ceil(batch_size);
    for writer_index in 0..writers {
        let offset = writer_index.saturating_mul(batch_size);
        if offset < count {
            offsets.insert(offset);
            let last_batch = writer_index.saturating_add(
                batch_count.saturating_sub(1).saturating_sub(writer_index) / writers * writers,
            );
            offsets.insert(
                last_batch
                    .saturating_mul(batch_size)
                    .min(count.saturating_sub(1)),
            );
        }
    }
    for sample in 0..samples {
        let offset = if samples <= 1 {
            0
        } else {
            sample.saturating_mul(count.saturating_sub(1)) / samples.saturating_sub(1)
        };
        offsets.insert(offset);
        if offsets.len() == samples {
            break;
        }
    }
    let mut fallback = 0_usize;
    while offsets.len() < samples {
        offsets.insert(fallback);
        fallback = fallback.saturating_add(1);
    }
    Ok(offsets.into_iter().collect())
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

#[derive(Clone, Copy)]
struct LifecycleDeltaLayout {
    dimensions: usize,
    segment_max_vectors: usize,
}

fn lifecycle_write_operation_count(
    train_count: usize,
    layout: LifecycleDeltaLayout,
    configured: Option<usize>,
    writers: usize,
    batch_size: usize,
    update_percent: usize,
    delete_percent: usize,
) -> BenchResult<usize> {
    let LifecycleDeltaLayout {
        dimensions,
        segment_max_vectors,
    } = layout;
    let writers = validate_lifecycle_writers(writers)?;
    if batch_size == 0
        || segment_max_vectors == 0
        || !(1..=100).contains(&update_percent)
        || !(1..=100).contains(&delete_percent)
    {
        return Err(invalid_input("lifecycle concurrency inputs are invalid").into());
    }
    let minimum_percent = update_percent.min(delete_percent);
    let minimum_write_ops = writers.saturating_mul(100).div_ceil(minimum_percent);
    let vector_bytes_per_row = dimensions
        .checked_mul(std::mem::size_of::<f32>())
        .ok_or_else(|| invalid_input("lifecycle vector row size overflows"))?;
    if vector_bytes_per_row == 0 {
        return Err(invalid_input("lifecycle vector dimensions must be positive").into());
    }
    let max_delta_rows = MAX_GLOBAL_DELTA_ROWS.min(
        MAX_GLOBAL_DELTA_VECTOR_BYTES
            .checked_div(vector_bytes_per_row)
            .unwrap_or(0),
    );
    let segment_row_ceiling = segment_max_vectors.saturating_mul(MAX_GLOBAL_DELTA_SEGMENTS);
    let mut max_write_ops = max_delta_rows
        .min(segment_row_ceiling)
        .saturating_mul(100)
        .checked_div(100_usize.saturating_add(update_percent))
        .unwrap_or(0);
    while {
        let update_ops = max_write_ops
            .saturating_mul(update_percent)
            .saturating_add(99)
            / 100;
        max_write_ops.saturating_add(update_ops) > max_delta_rows
            || max_write_ops
                .div_ceil(segment_max_vectors)
                .saturating_add(update_ops.div_ceil(segment_max_vectors))
                > MAX_GLOBAL_DELTA_SEGMENTS
    } {
        max_write_ops = max_write_ops.saturating_sub(1);
    }
    if max_write_ops < minimum_write_ops {
        return Err(invalid_input(
            "bounded global ANN delta cannot exercise every lifecycle writer",
        )
        .into());
    }
    let count = configured.unwrap_or_else(|| {
        (train_count / WRITE_FRACTION_DENOMINATOR)
            .max(1)
            .max(minimum_write_ops)
            .min(max_write_ops)
    });
    if count < minimum_write_ops {
        return Err(invalid_input(&format!(
            "BORSUK_BENCH_WRITE_OPS={count} cannot exercise all {writers} lifecycle writers at {minimum_percent}% mutations; require at least {minimum_write_ops}"
        ))
        .into());
    }
    if count > max_write_ops {
        return Err(invalid_input(&format!(
            "BORSUK_BENCH_WRITE_OPS={count} plus {update_percent}% upserts exceeds the bounded global ANN delta capacity for {dimensions} dimensions and segment_max_vectors={segment_max_vectors}; maximum is {max_write_ops}"
        ))
        .into());
    }
    write_operation_count(train_count, Some(count))
}

fn percentage_operation_count(base: usize, percent: usize) -> BenchResult<usize> {
    if base == 0 || !(1..=100).contains(&percent) {
        return Err(
            invalid_input("lifecycle mutation percentage must be between 1 and 100").into(),
        );
    }
    Ok(base.saturating_mul(percent).saturating_add(99) / 100)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct LifecycleBatchAssignment {
    writer_index: usize,
    batch_index: usize,
    offset: usize,
    len: usize,
}

fn validate_lifecycle_writers(writers: usize) -> io::Result<usize> {
    if !(1..=64).contains(&writers) {
        return Err(invalid_input(
            "BORSUK_BENCH_LIFECYCLE_WRITERS must be in 1..=64",
        ));
    }
    Ok(writers)
}

fn validate_build_writers(writers: usize) -> io::Result<usize> {
    if !(1..=32).contains(&writers) {
        return Err(invalid_input(
            "BORSUK_BENCH_BUILD_WRITERS must be in 1..=32",
        ));
    }
    Ok(writers)
}

fn lifecycle_write_waves(
    count: usize,
    batch_size: usize,
    writers: usize,
) -> io::Result<Vec<Vec<LifecycleBatchAssignment>>> {
    let writers = validate_lifecycle_writers(writers)?;
    if batch_size == 0 {
        return Err(invalid_input(
            "lifecycle batch size must be greater than zero",
        ));
    }
    let natural_batch_count = count.div_ceil(batch_size);
    let participating_writers = writers.min(count);
    let batch_count = natural_batch_count.max(participating_writers);
    let balance_for_writers = natural_batch_count < participating_writers;
    let balanced_batch_size = count.checked_div(batch_count).unwrap_or(0);
    let larger_balanced_batches = count.checked_rem(batch_count).unwrap_or(0);
    let mut waves = Vec::with_capacity(batch_count.div_ceil(writers));
    for batch_index in 0..batch_count {
        let wave_index = batch_index / writers;
        if wave_index == waves.len() {
            waves.push(Vec::with_capacity(writers));
        }
        let (offset, len) = if balance_for_writers {
            let offset = batch_index
                .saturating_mul(balanced_batch_size)
                .saturating_add(batch_index.min(larger_balanced_batches));
            (
                offset,
                balanced_batch_size + usize::from(batch_index < larger_balanced_batches),
            )
        } else {
            let offset = batch_index.saturating_mul(batch_size);
            (offset, write_batch_len(count, offset, batch_size))
        };
        waves[wave_index].push(LifecycleBatchAssignment {
            writer_index: batch_index % writers,
            batch_index,
            offset,
            len,
        });
    }
    Ok(waves)
}

fn stream_dataset_batches(
    config: &ResolvedConfig,
    dataset: &Dataset,
    count: usize,
    scheduled_batch_sizes: Option<&[usize]>,
    mut consume: impl FnMut(usize, Vec<Vec<f32>>) -> BenchResult<()>,
) -> BenchResult<()> {
    let default_batch_sizes;
    let batch_sizes = if let Some(batch_sizes) = scheduled_batch_sizes {
        let scheduled_rows = batch_sizes
            .iter()
            .try_fold(0_usize, |total, &batch_size| total.checked_add(batch_size));
        if batch_sizes.contains(&0) || scheduled_rows != Some(count) {
            return Err(invalid_input("mutation source batch schedule is invalid").into());
        }
        batch_sizes
    } else {
        default_batch_sizes = (0..count.div_ceil(config.write_batch_size))
            .map(|batch_index| {
                let offset = batch_index.saturating_mul(config.write_batch_size);
                write_batch_len(count, offset, config.write_batch_size)
            })
            .collect::<Vec<_>>();
        &default_batch_sizes
    };
    let mut offset = 0_usize;
    match &dataset.source {
        DatasetVectorSource::Unavailable => {
            for &batch_rows in batch_sizes {
                let vectors = (offset..offset.saturating_add(batch_rows))
                    .map(|row| deterministic_mutation_vector(row, dataset.meta.dim))
                    .collect();
                consume(offset, vectors)?;
                offset = offset.saturating_add(batch_rows);
            }
        }
        DatasetVectorSource::RawF32 => {
            let mut reader = BufReader::new(File::open(config.dataset_dir.join("train.f32"))?);
            for &batch_rows in batch_sizes {
                let mut vectors = Vec::with_capacity(batch_rows);
                for _ in 0..batch_rows {
                    vectors.push(read_f32_vector(&mut reader, dataset.meta.dim)?);
                }
                consume(offset, vectors)?;
                offset = offset.saturating_add(batch_rows);
            }
        }
        DatasetVectorSource::Parquet { train_files } => {
            let mut decoded = 0_usize;
            let mut pending = VecDeque::with_capacity(config.write_batch_size);
            let mut batch_index = 0_usize;
            'files: for path in train_files {
                let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(path)?)?
                    .with_batch_size(config.write_batch_size)
                    .build()?;
                for batch in reader {
                    if decoded == count {
                        break 'files;
                    }
                    let batch = batch?;
                    let column = batch.column_by_name("emb").ok_or_else(|| {
                        invalid_input(&format!("{} has no `emb` vector column", path.display()))
                    })?;
                    let batch_rows = batch.num_rows().min(count.saturating_sub(decoded));
                    let mut vectors = Vec::with_capacity(batch_rows);
                    for row in 0..batch_rows {
                        vectors.push(vector_row(column.as_ref(), row, dataset.meta.dim, "emb")?);
                    }
                    decoded = decoded.saturating_add(batch_rows);
                    pending.extend(vectors);
                    while batch_index < batch_sizes.len()
                        && pending.len() >= batch_sizes[batch_index]
                    {
                        let batch_rows = batch_sizes[batch_index];
                        let vectors = pending.drain(..batch_rows).collect();
                        consume(offset, vectors)?;
                        offset = offset.saturating_add(batch_rows);
                        batch_index = batch_index.saturating_add(1);
                    }
                }
            }
            if decoded != count || batch_index != batch_sizes.len() || !pending.is_empty() {
                return Err(invalid_input(&format!(
                    "mutation source decoded {decoded} vectors into {batch_index} batches; expected {count} vectors in {} batches",
                    batch_sizes.len()
                ))
                .into());
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
    config: &ResolvedConfig,
    writers: &mut [BorsukIndex],
    base_offset: usize,
    count: usize,
) -> BenchResult<WriteRow> {
    let started = Instant::now();
    let mut samples = Vec::new();
    let mut bytes_written = 0_u64;
    for (wave_index, assignments) in
        lifecycle_write_waves(count, config.write_batch_size, writers.len())?
            .into_iter()
            .enumerate()
    {
        let batches = assignments
            .into_iter()
            .map(|assignment| {
                let ids = (assignment.offset..assignment.offset.saturating_add(assignment.len))
                    .map(|id| base_offset.saturating_add(id).to_string())
                    .collect();
                (assignment, ids)
            })
            .collect();
        for (sample, written) in execute_delete_wave(wave_index, writers, batches)? {
            samples.push(sample);
            bytes_written = bytes_written.saturating_add(written);
        }
    }
    samples.sort_by_key(|sample| sample.batch_index);
    let requests = request_counts_from_samples(&samples);
    let mut row = write_row_from_samples("delete", count, elapsed_ms(started), samples, requests);
    row.bytes_written = bytes_written;
    Ok(row)
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
        let query_elapsed = started.elapsed();
        if *LIFECYCLE_PROGRESS_ENABLED.get_or_init(|| {
            env::var_os("BORSUK_LIFECYCLE_PROGRESS").is_some_and(|value| value == "1")
        }) {
            eprintln!(
                "{}",
                lifecycle_query_progress_line(LifecycleQueryProgress {
                    sample: query_index,
                    elapsed_us: query_elapsed.as_micros(),
                    engine: &report.leaf_mode,
                    termination: report.termination_reason.as_str(),
                    backing_reads: report.backing_reads,
                    backing_bytes: report.backing_bytes_read,
                    code_bytes: report.global_leaf_code_bytes,
                    exact_bytes: report.global_leaf_page_bytes,
                })
            );
        }
        summary.push(query_elapsed.as_secs_f64() * 1_000.0, &report, recall);
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
    if budgets.is_empty() {
        return Err(invalid_input("BORSUK_BENCH_CANDIDATES cannot be empty"));
    }
    for &budget in budgets {
        if budget == 0 || budget > MAX_DIAGNOSTIC_RECALL_CANDIDATES {
            return Err(invalid_input(&format!(
                "BORSUK_BENCH_CANDIDATES entries must be within 1..={MAX_DIAGNOSTIC_RECALL_CANDIDATES}; received {budget}"
            )));
        }
    }
    if let Some(pair) = budgets.windows(2).find(|pair| pair[0] >= pair[1]) {
        return Err(invalid_input(&format!(
            "BORSUK_BENCH_CANDIDATES must be strictly increasing; received adjacent entries {} and {}",
            pair[0], pair[1]
        )));
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

fn validate_bounded_v20_execution(summary: &QuerySummary) -> io::Result<()> {
    if summary.execution_engine() != "bounded-cell-card-v20" {
        return Err(invalid_input(&format!(
            "production recall expected bounded-cell-card-v20 but observed {}",
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

fn parse_lifecycle_insert_mode(value: &str) -> BenchResult<LifecycleInsertMode> {
    match value {
        "general-upsert" => Ok(LifecycleInsertMode::GeneralUpsert),
        "claim-free-put" => Ok(LifecycleInsertMode::ClaimFreePut),
        _ => Err(invalid_input(
            "BORSUK_BENCH_LIFECYCLE_INSERT_MODE must be general-upsert or claim-free-put",
        )
        .into()),
    }
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
        BUILD_HEADER, BenchmarkCacheProfile, BorsukIndex, BuildIngestCoordinator,
        CACHE_COVERAGE_HEADER, CACHE_STATE_HEADER, CONCURRENCY_HEADER, CONCURRENCY_SAMPLE_HEADER,
        CacheExecutionPolicy, ConcurrencyMeasurement, DEFAULT_NPROBE_SWEEP,
        DEFAULT_PRODUCTION_RAM_BUDGET_BYTES, DEFAULT_RECALL_CANDIDATES,
        EffectiveRuntimeFlowControl, GlobalScanCodec, IndexConfig, LIFECYCLE_HEADER,
        LeafCapability, LeafMode, LifecycleBatchAssignment, LifecycleDeltaLayout,
        LifecycleInsertMode, LifecycleQueryProgress, MUTATION_QUERY_HEADER,
        MUTATION_QUERY_SAMPLE_HEADER, OpenOptions, PreparedRecordBatch, QUERY_SAMPLE_HEADER,
        QuerySample, QuerySummary, RECALL_LATENCY_HEADER, SERVING_CANDIDATES,
        ServingMetadataPreparation, ServingMode, V21EvidenceIdentity, V21FeasibilityPhaseSelection,
        V21FeasibilityQuerySample, V21FeasibilityReport, V21FeasibilitySummary, V21LimitingBound,
        V22EvidenceIdentity, VectorMetric, VectorRecord, WRITE_COST_HEADER, WRITE_SAMPLE_HEADER,
        allow_missing_corpus_for_phase, approximate_options, benchmark_row_ids,
        build_v21_feasibility_summary, build_v22_stage_l_summary, cache_coverage_cohort_size,
        cache_coverage_enabled, cache_state_summary_enabled, dataset_metric,
        default_build_leaf_capability, default_recall_leaf_mode, default_serving_leaf_mode,
        deterministic_mutation_vector, disk_cached_concurrency_cohort_size,
        disk_cached_query_cohort_size, dollars_per_million_queries, execute_bulk_add_wave,
        execute_concurrency_cache_setup, execute_disk_cached_concurrency_profiles,
        execute_disk_cached_query_cohorts, execute_isolated_recall_cache_phases, execute_put_wave,
        execute_uncached_query_sequence, finalize_fresh_build, first_logical_batch_publish_ms,
        ingest_batch_size, is_hot_workload_position, join_concurrency_workers,
        lifecycle_progress_line, lifecycle_query_progress_line, lifecycle_write_operation_count,
        lifecycle_write_waves, lifecycle_writer_open_options, mixed_concurrency_query_indices,
        mutable_resident_metadata_budget, neighbor_row, normalized_cache_access_fractions,
        open_lifecycle_writer_handles, parquet_train_files_for_phase, parse_flag_value,
        parse_global_pq_layout, parse_leaf_capability, parse_leaf_mode,
        parse_lifecycle_insert_mode, parse_optional_byte_cap, parse_positive_list,
        parse_serving_mode, percentage_operation_count, permuted_positions, preload_query_count,
        projected_v21_serving_rss, query_sample_cache_cohort_size,
        query_scoped_physical_bytes_read, read_logical_cell_catalog,
        recall_cache_profile_needs_outer_handle, recall_preloads_local_snapshot,
        reopen_build_finalizer, reset_cache, rotated_workload_index, sample_mean, sample_stddev,
        serialize_v21_feasibility_evidence, serving_cache_dir, serving_memory_partition,
        shared_serving_metadata_preparation, update_vector_reservoir,
        uses_bounded_decoded_cache_phases, uses_memory_preloaded_phase, v21_feasibility_arms,
        v22_stage_l_scratch_parent, validate_bounded_v20_execution, validate_build_only,
        validate_build_writers, validate_disk_cached_network,
        validate_exact_read_max_physical_amplification, validate_generated_id_range,
        validate_insert_only, validate_leaf_capability_modes, validate_lifecycle_only,
        validate_lifecycle_writers, validate_max_parallel_decode_rank_tasks,
        validate_phase_selection, validate_serialized_v21_feasibility_evidence,
        validate_v12_candidate_budgets, validate_v12_leaf_mode, validate_v12_leaf_page_budgets,
        validate_v21_feasibility_phase, validate_v21_feasibility_reports,
        validate_v22_stage_l_evidence, vector_row, verification_offsets, write_batch_len,
        write_operation_count, write_runtime_flow_control_receipt, write_v21_feasibility_evidence,
        write_v22_stage_l_evidence,
    };
    use borsuk::{
        V22LayoutKind, V22LayoutLimitingBound, V22StageLExactRow, V22StageLLayoutArmReport,
        V22StageLLayoutQuerySample, V22StageLProjectedObject, V22StageLQueryPrefix, V22StageLRange,
        V22StageLReport,
    };
    use std::{fs, path::Path};

    #[test]
    fn v21_feasibility_matrix_is_exact_and_canonical() {
        let observed = v21_feasibility_arms()
            .into_iter()
            .map(|arm| (arm.bundle_row_limit, arm.selector_span, arm.hedge_delay_ms))
            .collect::<Vec<_>>();
        let expected = [128_u16, 256]
            .into_iter()
            .flat_map(|bundle| {
                [32_u16, 64].into_iter().flat_map(move |span| {
                    [None, Some(20_u16), Some(35_u16)]
                        .into_iter()
                        .map(move |hedge| (bundle, span, hedge))
                })
            })
            .collect::<Vec<_>>();

        assert_eq!(observed, expected);
        assert_eq!(observed.len(), 12);
    }

    #[test]
    fn v21_feasibility_phase_rejects_every_ordinary_or_ambient_mode() {
        let valid = V21FeasibilityPhaseSelection {
            build_index: false,
            skip_recall: true,
            read_only: true,
            ..V21FeasibilityPhaseSelection::default()
        };
        validate_v21_feasibility_phase(true, valid).unwrap();

        for invalid in [
            V21FeasibilityPhaseSelection {
                build_index: true,
                ..valid
            },
            V21FeasibilityPhaseSelection {
                build_only: true,
                ..valid
            },
            V21FeasibilityPhaseSelection {
                recall_only: true,
                ..valid
            },
            V21FeasibilityPhaseSelection {
                skip_recall: false,
                ..valid
            },
            V21FeasibilityPhaseSelection {
                read_only: false,
                ..valid
            },
            V21FeasibilityPhaseSelection {
                insert_only: true,
                ..valid
            },
            V21FeasibilityPhaseSelection {
                lifecycle_only: true,
                ..valid
            },
            V21FeasibilityPhaseSelection {
                ambient_nprobes: true,
                ..valid
            },
            V21FeasibilityPhaseSelection {
                ambient_candidates: true,
                ..valid
            },
            V21FeasibilityPhaseSelection {
                ambient_concurrency: true,
                ..valid
            },
            V21FeasibilityPhaseSelection {
                ambient_writes: true,
                ..valid
            },
            V21FeasibilityPhaseSelection {
                ambient_limit: true,
                ..valid
            },
        ] {
            assert!(validate_v21_feasibility_phase(true, invalid).is_err());
        }
    }

    #[test]
    fn v21_feasibility_evidence_is_canonical_self_validating_and_no_clobber() {
        let output = tempfile::tempdir().unwrap();
        let query_source_indices = (0..40).collect::<Vec<_>>();
        let source_archive_sha256 = "a".repeat(64);
        let mut identity = V21EvidenceIdentity {
            dataset_name: "deep-image-10m",
            dataset_id: "deep-image-10m",
            index_id: "index-abc",
            source_archive_sha256: &source_archive_sha256,
            dimensions: 96,
            dataset_rows: 100,
            query_seed: 23_001,
            query_source_indices: &query_source_indices,
            baseline_rss_bytes: 700_000_000,
        };
        let reports = v21_feasibility_arms()
            .into_iter()
            .enumerate()
            .map(|(arm_index, arm)| V21FeasibilityReport {
                arm,
                bundle_count: 3,
                region_count: 5,
                projected_directory_bytes: 1_000 + arm_index as u64,
                replaced_v20_root_bytes: 100_000_000,
                v20_root_checksum: "b".repeat(64),
                selector_within_frozen_cap: true,
                rows: 100,
                samples: (0..40)
                    .map(|query_index| V21FeasibilityQuerySample {
                        arm_index,
                        query_index,
                        routed_cells: 4,
                        selected_rows: 100,
                        selected_bundles: 3,
                        primary_requests: 2,
                        maximum_actual_requests: 3,
                        selected_bytes: 4_096,
                        physical_bytes: 8_192,
                        gt_hits: if query_index == 0 { 9 } else { 10 },
                        recall_hits: if query_index == 0 { 9 } else { 10 },
                        limiting_bound: V21LimitingBound::Exhausted,
                    })
                    .collect(),
            })
            .collect::<Vec<_>>();

        write_v21_feasibility_evidence(output.path(), &identity, &reports).unwrap();

        for name in [
            "bench_v21_feasibility_arms.csv",
            "bench_v21_feasibility_samples.csv",
            "bench_v21_feasibility_summary.json",
        ] {
            assert!(output.path().join(name).is_file(), "missing {name}");
        }
        assert!(
            write_v21_feasibility_evidence(output.path(), &identity, &reports).is_err(),
            "evidence writer overwrote an immutable destination"
        );

        let mut reordered = reports.clone();
        reordered.swap(0, 1);
        assert!(validate_v21_feasibility_reports(&query_source_indices, &reordered).is_err());
        let mut reordered_samples = reports.clone();
        reordered_samples[0].samples.swap(0, 1);
        assert!(
            validate_v21_feasibility_reports(&query_source_indices, &reordered_samples).is_err()
        );
        let mut drifted_cap = reports.clone();
        drifted_cap[0].selector_within_frozen_cap = false;
        assert!(validate_v21_feasibility_reports(&query_source_indices, &drifted_cap).is_err());
        let mut invalid_requests = reports.clone();
        invalid_requests[0].samples[0].primary_requests = 4;
        invalid_requests[0].samples[0].maximum_actual_requests = 3;
        assert!(
            validate_v21_feasibility_reports(&query_source_indices, &invalid_requests).is_err()
        );
        let mut invalid_hits = reports.clone();
        invalid_hits[0].samples[0].gt_hits = 11;
        assert!(validate_v21_feasibility_reports(&query_source_indices, &invalid_hits).is_err());

        identity.dataset_rows = 99;
        assert!(build_v21_feasibility_summary(&identity, &reports).is_err());
        identity.dataset_rows = 100;

        let summary: V21FeasibilitySummary = serde_json::from_slice(
            &fs::read(output.path().join("bench_v21_feasibility_summary.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(summary.eligible_arm_indexes, (0..12).collect::<Vec<_>>());
        assert_eq!(summary.arms[0].recall_at_10, 399.0 / 400.0);
        assert_eq!(summary.dataset_rows, 100);
        assert_eq!(summary.arms[0].bundle_row_limit, 128);
        assert_eq!(summary.arms[0].selector_span, 32);
        assert_eq!(summary.arms[0].hedge_delay_ms, None);
        assert_eq!(summary.arms[0].projected_directory_bytes, 1_000);

        let mut one_slow_arm = reports.clone();
        one_slow_arm[0].samples[0].maximum_actual_requests = 5;
        let one_slow_summary = build_v21_feasibility_summary(&identity, &one_slow_arm).unwrap();
        assert!(!one_slow_summary.arms[0].eligible);
        assert!(one_slow_summary.arms[1..].iter().all(|arm| arm.eligible));

        let canonical_summary = build_v21_feasibility_summary(&identity, &reports).unwrap();
        let payloads =
            serialize_v21_feasibility_evidence(&identity, &reports, &canonical_summary).unwrap();
        assert!(
            payloads[2].starts_with(
                br#"{"arm_count":12,"arms":[{"arm_index":0,"bundle_count":3,"bundle_row_limit":128,"eligible":true,"gt_coverage":"#
            ),
            "V21 summary must use recursively lexicographic publication JSON: {}",
            String::from_utf8_lossy(&payloads[2][..payloads[2].len().min(256)])
        );
        for payload_index in 0..3 {
            let mut mutated = payloads.clone();
            let byte_index = mutated[payload_index]
                .iter()
                .position(|byte| byte.is_ascii_digit())
                .unwrap();
            mutated[payload_index][byte_index] = b'9';
            assert!(
                validate_serialized_v21_feasibility_evidence(&identity, &reports, &mutated)
                    .is_err(),
                "artifact {payload_index} accepted mutated evidence"
            );
        }
        let mut swapped_arm_columns = payloads.clone();
        let arms_text = String::from_utf8(swapped_arm_columns[0].clone()).unwrap();
        let mut arm_lines = arms_text.lines().map(str::to_string).collect::<Vec<_>>();
        let mut fields = arm_lines[1]
            .split(',')
            .map(str::to_string)
            .collect::<Vec<_>>();
        fields.swap(11, 12);
        arm_lines[1] = fields.join(",");
        swapped_arm_columns[0] = format!("{}\n", arm_lines.join("\n")).into_bytes();
        assert!(
            validate_serialized_v21_feasibility_evidence(
                &identity,
                &reports,
                &swapped_arm_columns,
            )
            .is_err()
        );
        let mut mutated_sample_field = payloads.clone();
        let samples_text = String::from_utf8(mutated_sample_field[1].clone()).unwrap();
        let mut sample_lines = samples_text.lines().map(str::to_string).collect::<Vec<_>>();
        let mut fields = sample_lines[1]
            .split(',')
            .map(str::to_string)
            .collect::<Vec<_>>();
        fields[4] = "999".to_string();
        sample_lines[1] = fields.join(",");
        mutated_sample_field[1] = format!("{}\n", sample_lines.join("\n")).into_bytes();
        assert!(
            validate_serialized_v21_feasibility_evidence(
                &identity,
                &reports,
                &mutated_sample_field,
            )
            .is_err()
        );

        let partial = tempfile::tempdir().unwrap();
        fs::write(
            partial.path().join("bench_v21_feasibility_samples.csv"),
            b"occupied\n",
        )
        .unwrap();
        assert!(write_v21_feasibility_evidence(partial.path(), &identity, &reports).is_err());
        assert!(
            !partial
                .path()
                .join("bench_v21_feasibility_arms.csv")
                .exists()
        );
        assert!(
            !partial
                .path()
                .join("bench_v21_feasibility_summary.json")
                .exists()
        );
    }

    #[test]
    fn v21_projected_rss_replaces_v20_root_instead_of_double_counting_it() {
        assert_eq!(
            projected_v21_serving_rss(700_000_000, 120_000_000, 35_000_000, 5_000_000),
            Some(620_000_000)
        );
    }

    #[test]
    fn lifecycle_progress_line_is_bounded_and_machine_readable() {
        assert_eq!(
            lifecycle_progress_line("after-insert-query", "complete", 12_345),
            "BORSUK_LIFECYCLE_PROGRESS stage=after-insert-query status=complete elapsed_ms=12345"
        );
        assert!(lifecycle_progress_line(&"x".repeat(65), "start", 0).is_empty());
        assert!(lifecycle_progress_line("after insert", "start", 0).is_empty());
        assert!(lifecycle_progress_line("insert", "unknown", 0).is_empty());
    }

    #[test]
    fn lifecycle_query_progress_exposes_the_selected_engine_and_io_boundary() {
        assert_eq!(
            lifecycle_query_progress_line(LifecycleQueryProgress {
                sample: 7,
                elapsed_us: 12_345,
                engine: "bounded-cell-card-v20",
                termination: "complete",
                backing_reads: 3,
                backing_bytes: 4_096,
                code_bytes: 1_024,
                exact_bytes: 2_048,
            }),
            "BORSUK_LIFECYCLE_QUERY sample=7 elapsed_us=12345 engine=bounded-cell-card-v20 termination=complete backing_reads=3 backing_bytes=4096 code_bytes=1024 exact_bytes=2048"
        );
    }

    #[test]
    fn lifecycle_writer_count_is_positive_and_bounded() {
        assert_eq!(validate_lifecycle_writers(1).unwrap(), 1);
        assert_eq!(validate_lifecycle_writers(4).unwrap(), 4);
        assert_eq!(validate_lifecycle_writers(16).unwrap(), 16);
        assert!(validate_lifecycle_writers(0).is_err());
        assert!(validate_lifecycle_writers(65).is_err());
    }

    #[test]
    fn lifecycle_insert_mode_pins_general_control_and_claim_free_candidate() {
        assert_eq!(
            parse_lifecycle_insert_mode("general-upsert").unwrap(),
            LifecycleInsertMode::GeneralUpsert
        );
        assert_eq!(
            parse_lifecycle_insert_mode("claim-free-put").unwrap(),
            LifecycleInsertMode::ClaimFreePut
        );
        assert!(parse_lifecycle_insert_mode("silent-fallback").is_err());
    }

    #[test]
    fn lifecycle_waves_assign_every_record_once_across_writers() {
        let waves = lifecycle_write_waves(19, 3, 4).unwrap();
        assert_eq!(waves.len(), 2);
        assert_eq!(
            waves
                .iter()
                .flatten()
                .map(|batch| (
                    batch.writer_index,
                    batch.batch_index,
                    batch.offset,
                    batch.len
                ))
                .collect::<Vec<_>>(),
            vec![
                (0, 0, 0, 3),
                (1, 1, 3, 3),
                (2, 2, 6, 3),
                (3, 3, 9, 3),
                (0, 4, 12, 3),
                (1, 5, 15, 3),
                (2, 6, 18, 1),
            ]
        );
        assert!(lifecycle_write_waves(1, 0, 1).is_err());
        assert!(lifecycle_write_waves(1, 1, 0).is_err());
    }

    #[test]
    fn lifecycle_default_write_count_exercises_every_writer_in_mutation_phases() {
        assert_eq!(
            lifecycle_write_operation_count(
                1_000_000,
                LifecycleDeltaLayout {
                    dimensions: 768,
                    segment_max_vectors: 8_192,
                },
                None,
                16,
                1024,
                10,
                10,
            )
            .unwrap(),
            19_859
        );
        assert!(
            lifecycle_write_operation_count(
                1_000_000,
                LifecycleDeltaLayout {
                    dimensions: 768,
                    segment_max_vectors: 8_192,
                },
                Some(50_000),
                16,
                1024,
                10,
                10,
            )
            .is_err()
        );
        assert_eq!(
            lifecycle_write_operation_count(
                1_000_000,
                LifecycleDeltaLayout {
                    dimensions: 768,
                    segment_max_vectors: 512,
                },
                None,
                16,
                1024,
                10,
                10,
            )
            .unwrap(),
            7_168
        );
    }

    #[test]
    fn partial_lifecycle_batches_exercise_every_configured_writer() {
        let waves = lifecycle_write_waves(1_800, 1_024, 16).unwrap();
        assert_eq!(waves.len(), 1);
        assert_eq!(waves[0].len(), 16);
        assert_eq!(
            waves[0]
                .iter()
                .map(|batch| batch.writer_index)
                .collect::<Vec<_>>(),
            (0..16).collect::<Vec<_>>()
        );
        assert_eq!(waves[0].iter().map(|batch| batch.len).sum::<usize>(), 1_800);
        assert!(waves[0].iter().all(|batch| batch.len <= 1_024));

        let uneven = lifecycle_write_waves(17, 1_024, 16).unwrap();
        assert_eq!(uneven.len(), 1);
        assert_eq!(uneven[0].len(), 16);
        assert_eq!(uneven[0][0].len, 2);
        assert!(uneven[0][1..].iter().all(|batch| batch.len == 1));
        assert_eq!(
            uneven[0]
                .iter()
                .map(|batch| batch.writer_index)
                .collect::<Vec<_>>(),
            (0..16).collect::<Vec<_>>()
        );
    }

    #[test]
    fn lifecycle_verification_samples_span_every_writer() {
        let offsets = verification_offsets(500_000, 16, 16, 1024).unwrap();
        assert_eq!(offsets.len(), 32);
        assert_eq!(
            offsets
                .iter()
                .map(|offset| (offset / 1024) % 16)
                .collect::<std::collections::BTreeSet<_>>(),
            (0..16).collect()
        );
        assert!(offsets.iter().any(|offset| *offset > 400_000));
    }

    #[test]
    fn lifecycle_samples_bind_writer_and_wave_identity() {
        assert!(WRITE_COST_HEADER.starts_with("op,configured_writers,configured_batch_records"));
        assert_eq!(
            WRITE_SAMPLE_HEADER,
            "op,writer_index,wave_index,batch_index,batch_records,batch_latency_ms,amortized_ms,gets,puts,deletes,heads,lists"
        );
        assert!(LIFECYCLE_HEADER.starts_with("configured_writers,configured_batch_records"));
    }

    #[test]
    fn lifecycle_writer_options_inherit_the_runtime_budget_without_retained_caches() {
        let options = lifecycle_writer_open_options(Some(96 * 1024 * 1024));
        assert_eq!(options.ram_budget_bytes, Some(96 * 1024 * 1024));
        assert_eq!(options.resident_metadata_max_bytes, Some(48 * 1024 * 1024));
        assert_eq!(options.cache_dir, None);
        assert_eq!(options.cache_max_bytes, None);
        assert!(!options.resident_routing);
        assert_eq!(options.segment_cache_max_bytes, None);
        assert_eq!(options.routing_page_cache_max_bytes, 0);
        assert_eq!(options.tombstone_page_cache_max_bytes, 0);
        assert_eq!(options.bm25_stats_page_cache_max_bytes, 0);
        assert_eq!(options.lexical_run_cache_max_bytes, 0);
        assert_eq!(options.lexical_term_page_cache_max_bytes, 0);
        assert_eq!(options.late_interaction_batch_cache_max_bytes, 0);
        assert_eq!(options.wal_tail_cache_max_bytes, 0);
    }

    #[test]
    fn lifecycle_runtime_reserves_half_of_one_total_budget_for_positioned_authority() {
        assert_eq!(
            mutable_resident_metadata_budget(Some(2 * 1024 * 1024 * 1024)),
            Some(1024 * 1024 * 1024)
        );
        assert_eq!(mutable_resident_metadata_budget(None), None);
    }

    #[test]
    fn serving_memory_partition_reserves_fixed_and_transient_shares() {
        const MIB: u64 = 1024 * 1024;
        assert_eq!(
            serving_memory_partition(Some(3 * 1024 * MIB)),
            (Some(768 * MIB), Some(768 * MIB))
        );
        assert_eq!(
            serving_memory_partition(Some(512 * MIB)),
            (Some(128 * MIB), None)
        );
        assert_eq!(serving_memory_partition(None), (None, None));
    }

    #[test]
    fn lifecycle_runtime_opens_a_positioned_tail_larger_than_the_default_resident_slice() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_string_lossy().to_string();
        let persisted_ram_budget_bytes = 8 * 1024 * 1024;
        let requested_ram_budget_bytes = 32 * 1024 * 1024;
        let dimensions = 128;
        let mut index = BorsukIndex::create(IndexConfig {
            uri: uri.clone(),
            metric: VectorMetric::Cosine,
            dimensions,
            segment_max_vectors: 8_192,
            ram_budget_bytes: Some(persisted_ram_budget_bytes),
            text: false,
            named_vectors: Default::default(),
        })
        .unwrap();
        let rows = 4_096;
        index
            .add_vectors_with_ids(
                vec![vec![1.0; dimensions]; rows],
                (0..rows).map(|row| format!("row-{row}")).collect(),
            )
            .unwrap();
        drop(index);

        let default_error = BorsukIndex::open_with_options(
            &uri,
            OpenOptions {
                ram_budget_bytes: Some(requested_ram_budget_bytes),
                ..OpenOptions::default()
            },
        )
        .unwrap_err();
        assert_eq!(default_error.code(), "ram_budget_exceeded");

        let opened = BorsukIndex::open_with_options(
            &uri,
            lifecycle_writer_open_options(Some(requested_ram_budget_bytes)),
        )
        .unwrap();
        let stats = opened.stats();
        assert!(stats.collection_resident_bytes > persisted_ram_budget_bytes / 4);
        assert!(stats.retained_capacity_bytes > 0);
        assert!(stats.transient_capacity_bytes > 0);
    }

    #[test]
    fn lifecycle_writers_use_independent_cache_free_accounting_scopes() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_string_lossy().to_string();
        let mut index = BorsukIndex::create(IndexConfig {
            uri: uri.clone(),
            metric: VectorMetric::Cosine,
            dimensions: 2,
            segment_max_vectors: 8,
            ram_budget_bytes: Some(96 * 1024 * 1024),
            text: false,
            named_vectors: Default::default(),
        })
        .unwrap();
        index
            .add_vectors_with_ids(vec![vec![1.0, 0.0]], vec!["base".to_owned()])
            .unwrap();

        let mut writers = open_lifecycle_writer_handles(&uri, 2, Some(96 * 1024 * 1024)).unwrap();
        writers[0]
            .add_vectors_with_ids(vec![vec![0.0, 1.0]], vec!["writer-0".to_owned()])
            .unwrap();
        let writer_0_bytes = writers[0].put_payload_bytes();
        writers[1]
            .add_vectors_with_ids(vec![vec![-1.0, 0.0]], vec!["writer-1".to_owned()])
            .unwrap();

        assert!(writer_0_bytes > 0);
        assert_eq!(writers[0].put_payload_bytes(), writer_0_bytes);
        assert!(writers[1].put_payload_bytes() > 0);

        index.refresh().unwrap();
        assert!(index.get_vector("writer-0").unwrap().is_some());
        assert!(index.get_vector("writer-1").unwrap().is_some());
    }

    #[test]
    fn first_batch_publish_is_batch_zero_not_the_fastest_concurrent_writer() {
        let samples = [
            super::WriteSample {
                op: "insert",
                writer_index: 0,
                wave_index: 0,
                batch_index: 0,
                batch_records: 64,
                batch_latency_ms: 12.0,
                requests: Default::default(),
            },
            super::WriteSample {
                op: "insert",
                writer_index: 1,
                wave_index: 0,
                batch_index: 1,
                batch_records: 64,
                batch_latency_ms: 1.0,
                requests: Default::default(),
            },
        ];
        assert_eq!(first_logical_batch_publish_ms(&samples), 12.0);
    }

    #[test]
    fn independent_lifecycle_writer_handles_publish_visible_waves_with_delta_bytes() {
        let directory = tempfile::tempdir().unwrap();
        let uri = directory.path().to_string_lossy().into_owned();
        BorsukIndex::create(IndexConfig {
            uri: uri.clone(),
            metric: VectorMetric::Euclidean,
            dimensions: 2,
            segment_max_vectors: 16,
            ram_budget_bytes: None,
            text: false,
            named_vectors: Default::default(),
        })
        .unwrap();
        let mut writers = (0..4)
            .map(|_| BorsukIndex::open(&uri).unwrap())
            .collect::<Vec<_>>();
        let batches = (0..4)
            .map(|writer_index| PreparedRecordBatch {
                assignment: LifecycleBatchAssignment {
                    writer_index,
                    batch_index: writer_index,
                    offset: writer_index,
                    len: 1,
                },
                records: vec![VectorRecord::new(
                    format!("writer-{writer_index}"),
                    vec![writer_index as f32, 1.0],
                )],
            })
            .collect();

        let completed = execute_put_wave("insert", 0, &mut writers, batches).unwrap();
        assert_eq!(completed.len(), 4);
        assert_eq!(
            completed
                .iter()
                .map(|(sample, _)| (sample.writer_index, sample.wave_index))
                .collect::<Vec<_>>(),
            vec![(0, 0), (1, 0), (2, 0), (3, 0)]
        );
        let bytes_before = writers
            .iter()
            .map(BorsukIndex::put_payload_bytes)
            .sum::<u64>();
        let second = (0..4)
            .map(|writer_index| PreparedRecordBatch {
                assignment: LifecycleBatchAssignment {
                    writer_index,
                    batch_index: writer_index + 4,
                    offset: writer_index + 4,
                    len: 1,
                },
                records: vec![VectorRecord::new(
                    format!("writer-{}", writer_index + 4),
                    vec![writer_index as f32, 2.0],
                )],
            })
            .collect();
        let completed = execute_put_wave("insert", 1, &mut writers, second).unwrap();
        assert_eq!(
            completed.iter().map(|(_, bytes)| bytes).sum::<u64>(),
            writers
                .iter()
                .map(BorsukIndex::put_payload_bytes)
                .sum::<u64>()
                .saturating_sub(bytes_before),
            "each batch must report its own physical PUT bytes, not the handle's cumulative total"
        );
        let observer = BorsukIndex::open(&uri).unwrap();
        let ids = (0..8)
            .map(|writer_index| format!("writer-{writer_index}"))
            .collect::<Vec<_>>();
        assert_eq!(
            observer
                .get_records(&ids)
                .unwrap()
                .into_iter()
                .filter(Option::is_some)
                .count(),
            8
        );
        drop(observer);

        let bytes_before = writers
            .iter()
            .map(BorsukIndex::put_payload_bytes)
            .sum::<u64>();
        let deletes = (0..4)
            .map(|writer_index| {
                (
                    LifecycleBatchAssignment {
                        writer_index,
                        batch_index: writer_index,
                        offset: writer_index,
                        len: 1,
                    },
                    vec![format!("writer-{writer_index}")],
                )
            })
            .collect();
        let completed = super::execute_delete_wave(0, &mut writers, deletes).unwrap();
        assert_eq!(
            completed.iter().map(|(_, bytes)| bytes).sum::<u64>(),
            writers
                .iter()
                .map(BorsukIndex::put_payload_bytes)
                .sum::<u64>()
                .saturating_sub(bytes_before),
            "delete samples must account physical PUT payload bytes"
        );
    }

    #[test]
    fn lifecycle_insert_profile_rejects_a_general_upsert_fallback() {
        let named = tempfile::tempdir().unwrap();
        let uri = named.path().to_string_lossy().into_owned();
        let mut named_vectors = std::collections::BTreeMap::new();
        named_vectors.insert(
            "image".to_string(),
            borsuk::VectorSpec {
                dimensions: 2,
                metric: VectorMetric::Euclidean,
                kind: Default::default(),
                element_type: Default::default(),
            },
        );
        BorsukIndex::create(IndexConfig {
            uri: uri.clone(),
            metric: VectorMetric::Euclidean,
            dimensions: 2,
            segment_max_vectors: 16,
            ram_budget_bytes: None,
            text: false,
            named_vectors,
        })
        .unwrap();
        let mut writers = vec![BorsukIndex::open(&uri).unwrap()];
        let error = match execute_put_wave(
            "insert",
            0,
            &mut writers,
            vec![PreparedRecordBatch {
                assignment: LifecycleBatchAssignment {
                    writer_index: 0,
                    batch_index: 0,
                    offset: 0,
                    len: 1,
                },
                records: vec![
                    VectorRecord::new("row", vec![1.0, 0.0])
                        .with_named_vector("image", vec![0.0, 1.0]),
                ],
            }],
        ) {
            Ok(_) => panic!("named lifecycle insert silently used the general upsert path"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("primary-dense-only"));
    }

    #[test]
    fn zero_disk_cache_cap_removes_the_cache_directory_from_serving_storage() {
        let cache = std::path::Path::new("/cache");
        assert_eq!(serving_cache_dir(cache, None), None);
        assert_eq!(
            serving_cache_dir(cache, Some(1024)),
            Some(cache.to_path_buf())
        );
    }

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
        let uri = directory.path().to_string_lossy().into_owned();
        BorsukIndex::create(IndexConfig {
            uri: uri.clone(),
            metric: VectorMetric::Euclidean,
            dimensions: 2,
            segment_max_vectors: 16,
            ram_budget_bytes: None,
            text: false,
            named_vectors: Default::default(),
        })
        .unwrap();
        let mut coordinator = BuildIngestCoordinator::open(&uri, 2, None).unwrap();
        coordinator.push(0, vec![vec![1.0, 0.0]]).unwrap();
        coordinator.push(1, vec![vec![0.0, 1.0]]).unwrap();
        coordinator.finish().unwrap();
        let mut index = BorsukIndex::open(&uri).unwrap();
        index.flush().unwrap();

        assert_eq!(index.manifest().tombstone_delta_run_count(), 0);
        assert!(!index.manifest().has_mutation_directory());
        assert_eq!(index.get_vector("0").unwrap(), Some(vec![1.0, 0.0]));
        assert_eq!(index.get_vector("1").unwrap(), Some(vec![0.0, 1.0]));
    }

    #[test]
    fn fresh_build_finalization_needs_no_online_claim_transaction_controls() {
        let directory = tempfile::tempdir().unwrap();
        let uri = directory.path().to_string_lossy().into_owned();
        BorsukIndex::create(IndexConfig {
            uri: uri.clone(),
            metric: VectorMetric::Euclidean,
            dimensions: 2,
            segment_max_vectors: 4,
            ram_budget_bytes: None,
            text: false,
            named_vectors: Default::default(),
        })
        .unwrap();
        let mut coordinator = BuildIngestCoordinator::open(&uri, 4, None).unwrap();
        for batch in 0..8 {
            let start = batch * 2;
            coordinator
                .push(
                    start,
                    vec![vec![start as f32, 0.0], vec![(start + 1) as f32, 0.0]],
                )
                .unwrap();
        }
        coordinator.finish().unwrap();
        let mut index = BorsukIndex::open(&uri).unwrap();
        let transaction_states = || {
            std::fs::read_dir(directory.path().join("transactions"))
                .into_iter()
                .flatten()
                .filter_map(Result::ok)
                .filter(|entry| entry.path().join("STATE").is_file())
                .count()
        };
        let before = transaction_states();
        assert_eq!(
            before, 0,
            "offline unique-ID build unexpectedly retained online claim transaction state"
        );

        let finalized = finalize_fresh_build(&mut index, false).unwrap();

        assert_eq!(finalized.layout, "ingest-preserving");
        assert_eq!(finalized.garbage_collection.transaction_states_remaining, 0);
        let after = transaction_states();
        assert_eq!(
            after, 0,
            "authorized prepared-state controls were not fully reclaimed: before={before} report={:?}",
            finalized.garbage_collection,
        );
        for id in 0..16 {
            assert!(index.get_vector(&id.to_string()).unwrap().is_some());
        }
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
    fn production_v20_defaults_to_the_preregistered_exact_rerank_budget() {
        validate_v12_candidate_budgets(DEFAULT_RECALL_CANDIDATES).unwrap();
        assert_eq!(SERVING_CANDIDATES, DEFAULT_RECALL_CANDIDATES[0]);
    }

    #[test]
    fn diagnostic_v20_candidate_sweep_accepts_bounded_positive_depths() {
        validate_v12_candidate_budgets(&[512, 1_024, 2_048, 4_096]).unwrap();
        assert!(validate_v12_candidate_budgets(&[]).is_err());
        assert!(validate_v12_candidate_budgets(&[0, 512]).is_err());
        assert!(validate_v12_candidate_budgets(&[65_537]).is_err());
        assert!(validate_v12_candidate_budgets(&[1_024, 512]).is_err());
        assert!(validate_v12_candidate_budgets(&[512, 512]).is_err());
    }

    #[test]
    fn production_recall_requires_the_frozen_v20_engine() {
        let mut fallback = QuerySummary::default();
        fallback.execution_engines.insert("srht-pq-scan".to_owned());
        let error = validate_bounded_v20_execution(&fallback)
            .expect_err("legacy segment execution was accepted as a V20 measurement");
        assert!(
            error.to_string().contains("bounded-cell-card-v20")
                && error.to_string().contains("srht-pq-scan"),
            "{error}"
        );

        let mut bounded = QuerySummary::default();
        bounded
            .execution_engines
            .insert("bounded-cell-card-v20".to_owned());
        validate_bounded_v20_execution(&bounded).unwrap();
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
    fn isolated_recall_profiles_do_not_open_or_preload_discarded_handles() {
        assert!(!recall_cache_profile_needs_outer_handle(
            BenchmarkCacheProfile::Uncached,
            true,
            CacheExecutionPolicy::Graph,
            LeafMode::Graph,
            Some(256 * 1024 * 1024),
        ));
        assert!(!recall_cache_profile_needs_outer_handle(
            BenchmarkCacheProfile::DiskCached,
            true,
            CacheExecutionPolicy::Graph,
            LeafMode::Graph,
            Some(256 * 1024 * 1024),
        ));
        assert!(!recall_cache_profile_needs_outer_handle(
            BenchmarkCacheProfile::MixedCoverage,
            true,
            CacheExecutionPolicy::Graph,
            LeafMode::Graph,
            Some(256 * 1024 * 1024),
        ));
        assert!(!recall_cache_profile_needs_outer_handle(
            BenchmarkCacheProfile::All,
            true,
            CacheExecutionPolicy::Scan,
            LeafMode::SrhtPqScan,
            Some(256 * 1024 * 1024),
        ));
        assert!(recall_cache_profile_needs_outer_handle(
            BenchmarkCacheProfile::All,
            true,
            CacheExecutionPolicy::Graph,
            LeafMode::Graph,
            Some(256 * 1024 * 1024),
        ));
        assert!(recall_cache_profile_needs_outer_handle(
            BenchmarkCacheProfile::All,
            true,
            CacheExecutionPolicy::Graph,
            LeafMode::SrhtPqScan,
            None,
        ));
        assert!(recall_cache_profile_needs_outer_handle(
            BenchmarkCacheProfile::All,
            false,
            CacheExecutionPolicy::Auto,
            LeafMode::Graph,
            Some(256 * 1024 * 1024),
        ));
    }

    #[test]
    fn serving_metadata_preparation_is_bound_to_each_handle_role() {
        assert!(!ServingMetadataPreparation::DeferredCodePlanes.complete_code_planes());
        assert!(ServingMetadataPreparation::Complete.complete_code_planes());
        assert_eq!(
            ServingMetadataPreparation::DeferredCodePlanes.code_plane_cache_max_bytes(),
            0,
            "a fresh cold handle must not retain or promote complete code planes"
        );
        assert!(
            ServingMetadataPreparation::Complete.code_plane_cache_max_bytes() > 0,
            "warm serving needs an explicitly bounded complete-plane cache"
        );
    }

    #[test]
    fn shared_serving_metadata_preparation_is_exhaustive_by_cache_profile() {
        assert_eq!(
            shared_serving_metadata_preparation(BenchmarkCacheProfile::All),
            ServingMetadataPreparation::Complete
        );
        assert_eq!(
            shared_serving_metadata_preparation(BenchmarkCacheProfile::DiskCached),
            ServingMetadataPreparation::Complete
        );
        assert_eq!(
            shared_serving_metadata_preparation(BenchmarkCacheProfile::Uncached),
            ServingMetadataPreparation::DeferredCodePlanes
        );
        assert_eq!(
            shared_serving_metadata_preparation(BenchmarkCacheProfile::MixedCoverage),
            ServingMetadataPreparation::DeferredCodePlanes
        );
    }

    #[test]
    fn concurrency_rows_use_query_scoped_physical_byte_tiers() {
        assert_eq!(query_scoped_physical_bytes_read(11, 22, 33), 66);
    }

    #[test]
    fn latency_artifact_schemas_include_the_worst_query() {
        assert_eq!(
            super::PRODUCTION_BENCH_SCHEMA_VERSION,
            "borsuk-production-bench-v20"
        );
        assert_eq!(RECALL_LATENCY_HEADER.split(',').count(), 33);
        assert_eq!(CACHE_STATE_HEADER.split(',').count(), 31);
        assert_eq!(CONCURRENCY_HEADER.split(',').count(), 32);
        assert_eq!(CACHE_COVERAGE_HEADER.split(',').count(), 33);
        assert_eq!(QUERY_SAMPLE_HEADER.split(',').count(), 82);
        assert_eq!(
            QUERY_SAMPLE_HEADER
                .split(',')
                .skip(8)
                .take(3)
                .collect::<Vec<_>>(),
            vec![
                "cache_cohort_index",
                "cache_cohort_size",
                "cache_cohort_count",
            ]
        );
        assert_eq!(
            QUERY_SAMPLE_HEADER
                .split(',')
                .skip(43)
                .take(8)
                .collect::<Vec<_>>(),
            vec![
                "global_leaf_code_requests",
                "global_leaf_exact_requests",
                "global_leaf_exact_cells",
                "global_leaf_exact_cards",
                "global_leaf_deepest_winning_card_rank",
                "global_leaf_exact_groups",
                "global_leaf_exact_selected_bytes",
                "global_leaf_exact_speculative_bytes",
            ]
        );
        assert_eq!(CONCURRENCY_SAMPLE_HEADER.split(',').count(), 81);
        assert_eq!(
            CONCURRENCY_SAMPLE_HEADER
                .split(',')
                .skip(9)
                .take(3)
                .collect::<Vec<_>>(),
            vec![
                "cache_cohort_index",
                "cache_cohort_size",
                "cache_cohort_count",
            ]
        );
        assert_eq!(
            CONCURRENCY_SAMPLE_HEADER
                .split(',')
                .skip(26)
                .take(8)
                .collect::<Vec<_>>(),
            vec![
                "global_leaf_code_requests",
                "global_leaf_exact_requests",
                "global_leaf_exact_cells",
                "global_leaf_exact_cards",
                "global_leaf_deepest_winning_card_rank",
                "global_leaf_exact_groups",
                "global_leaf_exact_selected_bytes",
                "global_leaf_exact_speculative_bytes",
            ]
        );
        let timing_columns = vec![
            "global_base_approximate_us",
            "global_base_head_admission_us",
            "global_base_head_fetch_us",
            "global_base_head_read_attempts",
            "global_base_head_read_successes",
            "global_base_head_read_response_bytes",
            "global_base_head_read_us_max",
            "global_base_head_read_us_sum",
            "global_base_head_read_queue_us_max",
            "global_base_head_read_queue_us_sum",
            "global_base_head_reads_over_20ms",
            "global_base_head_reads_over_30ms",
            "global_base_head_reads_over_50ms",
            "global_base_head_reads_over_100ms",
            "global_base_head_decode_admission_us",
            "global_base_head_decode_us",
            "global_base_exact_admission_us",
            "global_base_exact_fetch_us",
            "global_base_exact_read_attempts",
            "global_base_exact_read_successes",
            "global_base_exact_read_response_bytes",
            "global_base_exact_read_queue_us_max",
            "global_base_exact_read_queue_us_sum",
            "global_base_exact_read_us_max",
            "global_base_exact_read_us_sum",
            "global_base_exact_reads_over_20ms",
            "global_base_exact_reads_over_30ms",
            "global_base_exact_reads_over_50ms",
            "global_base_exact_reads_over_100ms",
            "global_base_exact_cpu_us",
            "global_base_exact_rerank_us",
        ];
        assert_eq!(
            QUERY_SAMPLE_HEADER.split(',').skip(51).collect::<Vec<_>>(),
            timing_columns
        );
        assert_eq!(
            CONCURRENCY_SAMPLE_HEADER
                .split(',')
                .skip(50)
                .collect::<Vec<_>>(),
            timing_columns
        );
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
            "global_leaf_exact_cells",
            "global_leaf_exact_cards",
            "global_leaf_deepest_winning_card_rank",
            "global_leaf_exact_groups",
            "global_leaf_exact_selected_bytes",
            "global_leaf_exact_speculative_bytes",
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
            "global_base_approximate_us",
            "global_base_head_admission_us",
            "global_base_head_fetch_us",
            "global_base_head_decode_admission_us",
            "global_base_head_decode_us",
            "global_base_exact_admission_us",
            "global_base_exact_fetch_us",
            "global_base_exact_read_us_max",
            "global_base_exact_read_us_sum",
            "global_base_exact_reads_over_20ms",
            "global_base_exact_reads_over_30ms",
            "global_base_exact_reads_over_50ms",
            "global_base_exact_reads_over_100ms",
            "global_base_exact_cpu_us",
            "global_base_exact_rerank_us",
        ] {
            assert!(QUERY_SAMPLE_HEADER.contains(column), "missing {column}");
        }
        for column in [
            "nprobe",
            "max_candidates",
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
            "global_base_approximate_us",
            "global_base_head_admission_us",
            "global_base_head_fetch_us",
            "global_base_head_decode_admission_us",
            "global_base_head_decode_us",
            "global_base_exact_admission_us",
            "global_base_exact_fetch_us",
            "global_base_exact_read_us_max",
            "global_base_exact_read_us_sum",
            "global_base_exact_reads_over_20ms",
            "global_base_exact_reads_over_30ms",
            "global_base_exact_reads_over_50ms",
            "global_base_exact_reads_over_100ms",
            "global_base_exact_cpu_us",
            "global_base_exact_rerank_us",
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
    fn query_samples_carry_every_v14_leaf_counter() {
        let projection = |sample: &QuerySample| {
            (
                sample.global_leaf_directory_reads,
                sample.global_leaf_directory_bytes,
                sample.global_leaf_code_pages_read,
                sample.global_leaf_code_requests,
                sample.global_leaf_code_bytes,
                sample.global_leaf_pages_read,
                sample.global_leaf_exact_requests,
                sample.global_leaf_exact_cells,
                sample.global_leaf_exact_cards,
                sample.global_leaf_deepest_winning_card_rank,
                sample.global_leaf_exact_groups,
                sample.global_leaf_exact_selected_bytes,
                sample.global_leaf_exact_speculative_bytes,
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
    fn concurrency_samples_carry_physical_exact_layout_counters() {
        let sample = ConcurrencyMeasurement {
            global_leaf_code_requests: 1,
            global_leaf_exact_requests: 2,
            global_leaf_exact_cells: 3,
            global_leaf_exact_cards: 4,
            global_leaf_deepest_winning_card_rank: 5,
            global_leaf_exact_groups: 6,
            global_leaf_exact_selected_bytes: 7,
            global_leaf_exact_speculative_bytes: 8,
            ..ConcurrencyMeasurement::default()
        };
        assert_eq!(sample.physical_exact_csv_fields(), "1,2,3,4,5,6,7,8");
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
            "configured_build_writers",
            "ingest_batches",
            "ingest_waves",
            "ingest_vectors_per_s",
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
            "gc_ms",
            "gc_objects_scanned",
            "gc_objects_deleted",
            "gc_transaction_states_remaining",
            "gc_bytes_read",
            "gc_bytes_reclaimed",
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
            "searchability_refresh_ms",
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
                allow_missing_corpus_for_phase(false, false),
            )
            .unwrap(),
            None
        );
        assert!(
            parquet_train_files_for_phase(
                directory.path(),
                100_000_000,
                allow_missing_corpus_for_phase(true, false),
            )
            .is_err()
        );
    }

    #[test]
    fn lifecycle_runtime_dataset_does_not_require_local_corpus_shards() {
        let directory = tempfile::tempdir().unwrap();
        assert_eq!(
            parquet_train_files_for_phase(
                directory.path(),
                100_000_000,
                allow_missing_corpus_for_phase(false, false),
            )
            .unwrap(),
            None
        );
    }

    #[test]
    fn concurrency_only_runtime_skips_the_serial_cache_state_summary() {
        assert!(!cache_state_summary_enabled(
            true,
            BenchmarkCacheProfile::DiskCached
        ));
        assert!(cache_state_summary_enabled(
            false,
            BenchmarkCacheProfile::DiskCached
        ));
        assert!(!cache_state_summary_enabled(
            false,
            BenchmarkCacheProfile::MixedCoverage
        ));
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
    fn runtime_flow_receipt_attests_exact_read_physical_amplification() {
        let value = serde_json::to_value(EffectiveRuntimeFlowControl {
            schema_version: 4,
            disk_cache_max_bytes: 0,
            ram_budget_bytes: Some(512 * 1024 * 1024),
            max_active_searches: 8,
            max_waiting_searches: 16,
            leaf_read_width: 32,
            max_inflight_leaf_reads: 48,
            max_parallel_decode_rank_tasks: 1,
            exact_read_max_physical_amplification: 2,
            cpu_threads: 4,
            io_threads: 4,
            s3_get_concurrency: 16,
        })
        .unwrap();

        assert_eq!(value["schema_version"], 4);
        assert_eq!(value["disk_cache_max_bytes"], 0);
        assert_eq!(value["max_parallel_decode_rank_tasks"], 1);
        assert_eq!(value["exact_read_max_physical_amplification"], 2);
    }

    #[test]
    fn runtime_flow_receipt_is_canonical_json_for_publication() {
        let receipt = EffectiveRuntimeFlowControl {
            schema_version: 4,
            disk_cache_max_bytes: 0,
            ram_budget_bytes: Some(536_870_912),
            max_active_searches: 8,
            max_waiting_searches: 16,
            leaf_read_width: 32,
            max_inflight_leaf_reads: 48,
            max_parallel_decode_rank_tasks: 1,
            exact_read_max_physical_amplification: 2,
            cpu_threads: 4,
            io_threads: 88,
            s3_get_concurrency: 64,
        };
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("bench_runtime_flow_control.json");

        write_runtime_flow_control_receipt(&path, &receipt).unwrap();
        let bytes = std::fs::read(path).unwrap();

        assert_eq!(
            bytes,
            br#"{"cpu_threads":4,"disk_cache_max_bytes":0,"exact_read_max_physical_amplification":2,"io_threads":88,"leaf_read_width":32,"max_active_searches":8,"max_inflight_leaf_reads":48,"max_parallel_decode_rank_tasks":1,"max_waiting_searches":16,"ram_budget_bytes":536870912,"s3_get_concurrency":64,"schema_version":4}
"#
        );
    }

    #[test]
    fn benchmark_rejects_invalid_exact_read_physical_amplification_before_receipt() {
        assert_eq!(
            validate_exact_read_max_physical_amplification(1).unwrap(),
            1
        );
        assert_eq!(
            validate_exact_read_max_physical_amplification(5).unwrap(),
            5
        );
        assert!(validate_exact_read_max_physical_amplification(0).is_err());
        assert!(validate_exact_read_max_physical_amplification(6).is_err());
    }

    #[test]
    fn benchmark_rejects_zero_decode_rank_capacity_before_receipt() {
        assert_eq!(validate_max_parallel_decode_rank_tasks(1).unwrap(), 1);
        assert!(validate_max_parallel_decode_rank_tasks(0).is_err());
    }

    #[test]
    fn disk_cached_validation_allows_local_bytes_but_rejects_network_gets() {
        let local_disk = QuerySummary {
            bytes_read: 4_953_727,
            disk_cache_reads: 1,
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
    fn query_sample_cohort_authority_is_bound_to_the_disk_cached_profile() {
        assert_eq!(
            query_sample_cache_cohort_size(
                BenchmarkCacheProfile::DiskCached,
                "disk_cached",
                Some(64 * 1024 * 1024 * 1024),
                1_000,
            )
            .unwrap(),
            1_000
        );
        assert_eq!(
            query_sample_cache_cohort_size(
                BenchmarkCacheProfile::All,
                "disk_cached",
                Some(1024 * 1024 * 1024),
                1_000,
            )
            .unwrap(),
            0,
            "legacy all-phase disk caching is not cohort-isolated"
        );
        assert_eq!(
            query_sample_cache_cohort_size(
                BenchmarkCacheProfile::Uncached,
                "uncached",
                Some(1024 * 1024 * 1024),
                1_000,
            )
            .unwrap(),
            0
        );
    }

    #[test]
    fn disk_cached_cohorts_discard_open_io_before_priming_one_handle() {
        use std::cell::{Cell, RefCell};
        use std::rc::Rc;

        assert_eq!(
            disk_cached_query_cohort_size(Some(64 * 1024 * 1024 * 1024), 1_000).unwrap(),
            1_000
        );
        assert_eq!(
            disk_cached_concurrency_cohort_size(Some(64 * 1024 * 1024 * 1024), 1_000).unwrap(),
            1_000
        );
        assert!(disk_cached_concurrency_cohort_size(Some(1024 * 1024 * 1024), 1_000).is_err());
        assert!(disk_cached_query_cohort_size(None, 1).is_err());
        assert!(disk_cached_concurrency_cohort_size(None, 1).is_err());

        struct TrackedHandle {
            id: usize,
            live: Rc<Cell<usize>>,
            events: Rc<RefCell<Vec<String>>>,
        }

        impl Drop for TrackedHandle {
            fn drop(&mut self) {
                assert_eq!(self.live.replace(0), 1);
                self.events.borrow_mut().push(format!("drop-{}", self.id));
            }
        }

        let live = Rc::new(Cell::new(0));
        let next_id = Rc::new(Cell::new(0));
        let events = Rc::new(RefCell::new(Vec::new()));
        let summary = execute_disk_cached_query_cohorts(
            3,
            3,
            {
                let live = Rc::clone(&live);
                let events = Rc::clone(&events);
                move || {
                    assert_eq!(live.get(), 0);
                    events.borrow_mut().push("reset".to_string());
                    Ok(())
                }
            },
            {
                let live = Rc::clone(&live);
                let next_id = Rc::clone(&next_id);
                let events = Rc::clone(&events);
                move || {
                    assert_eq!(live.replace(1), 0);
                    let id = next_id.get() + 1;
                    next_id.set(id);
                    events.borrow_mut().push(format!("open-{id}"));
                    Ok(TrackedHandle {
                        id,
                        live: Rc::clone(&live),
                        events: Rc::clone(&events),
                    })
                }
            },
            {
                let live = Rc::clone(&live);
                let events = Rc::clone(&events);
                move |handle: &TrackedHandle| {
                    assert_eq!(live.get(), 1);
                    events
                        .borrow_mut()
                        .push(format!("clear-disk-{}", handle.id));
                    Ok(())
                }
            },
            {
                let events = Rc::clone(&events);
                move |handle: &TrackedHandle, query_index| {
                    events
                        .borrow_mut()
                        .push(format!("prime-{}-{query_index}", handle.id));
                    Ok(())
                }
            },
            {
                let events = Rc::clone(&events);
                move |handle: &TrackedHandle| {
                    events.borrow_mut().push(format!("clear-{}", handle.id));
                    Ok(())
                }
            },
            {
                let events = Rc::clone(&events);
                move |handle: &TrackedHandle, query_index| {
                    events
                        .borrow_mut()
                        .push(format!("measure-{}-{query_index}", handle.id));
                    Ok(QuerySummary {
                        latencies_ms: vec![1.0],
                        disk_cache_reads: 1,
                        ..QuerySummary::default()
                    })
                }
            },
        )
        .unwrap();

        assert_eq!(live.get(), 0);
        assert_eq!(summary.count(), 3);
        assert_eq!(
            *events.borrow(),
            [
                "reset",
                "open-1",
                "clear-disk-1",
                "clear-1",
                "prime-1-0",
                "clear-1",
                "prime-1-1",
                "clear-1",
                "prime-1-2",
                "clear-1",
                "measure-1-0",
                "clear-1",
                "measure-1-1",
                "clear-1",
                "measure-1-2",
                "drop-1",
            ]
        );
    }

    #[test]
    fn disk_cached_recall_cohorts_cannot_evict_an_earlier_primed_query() {
        use std::cell::RefCell;
        use std::collections::BTreeSet;

        let cached_queries = RefCell::new(BTreeSet::new());
        let summary = execute_disk_cached_query_cohorts(
            3,
            disk_cached_query_cohort_size(Some(64 * 1024 * 1024 * 1024), 3).unwrap(),
            || {
                cached_queries.borrow_mut().clear();
                Ok(())
            },
            || Ok(()),
            |_| Ok(()),
            |_, query_index| {
                cached_queries.borrow_mut().insert(query_index);
                Ok(())
            },
            |_| Ok(()),
            |_, query_index| {
                let cache_hit = cached_queries.borrow().contains(&query_index);
                Ok(QuerySummary {
                    latencies_ms: vec![1.0],
                    billable_requests: usize::from(!cache_hit) as u128,
                    disk_cache_reads: usize::from(cache_hit) as u128,
                    ..QuerySummary::default()
                })
            },
        )
        .unwrap();

        assert_eq!(summary.count(), 3);
        assert_eq!(summary.billable_requests, 0);
        assert_eq!(summary.disk_cache_reads, 3);
    }

    #[test]
    fn disk_cached_recall_primes_every_query_from_cleared_retained_state() {
        use std::cell::{Cell, RefCell};
        use std::collections::BTreeSet;

        let retained_query = Cell::new(None);
        let disk_cached_queries = RefCell::new(BTreeSet::new());
        let summary = execute_disk_cached_query_cohorts(
            3,
            3,
            || {
                retained_query.set(None);
                disk_cached_queries.borrow_mut().clear();
                Ok(())
            },
            || Ok(()),
            |_| Ok(()),
            |_, query_index| {
                if retained_query.get().is_none() {
                    disk_cached_queries.borrow_mut().insert(query_index);
                }
                retained_query.set(Some(query_index));
                Ok(())
            },
            |_| {
                retained_query.set(None);
                Ok(())
            },
            |_, query_index| {
                let cache_hit = disk_cached_queries.borrow().contains(&query_index);
                retained_query.set(Some(query_index));
                Ok(QuerySummary {
                    latencies_ms: vec![1.0],
                    billable_requests: usize::from(!cache_hit) as u128,
                    disk_cache_reads: usize::from(cache_hit) as u128,
                    ..QuerySummary::default()
                })
            },
        )
        .unwrap();

        assert_eq!(summary.count(), 3);
        assert_eq!(summary.billable_requests, 0);
        assert_eq!(summary.disk_cache_reads, 3);
    }

    #[test]
    fn combined_recall_profile_drops_outer_handle_before_isolated_phases() {
        use std::cell::Cell;
        use std::rc::Rc;

        struct TrackedOuter(Rc<Cell<bool>>);

        impl Drop for TrackedOuter {
            fn drop(&mut self) {
                assert!(self.0.replace(false));
            }
        }

        let live = Rc::new(Cell::new(true));
        let phases = execute_isolated_recall_cache_phases(
            Some(TrackedOuter(Rc::clone(&live))),
            {
                let live = Rc::clone(&live);
                move || {
                    assert!(!live.get(), "uncached reset ran under the outer handle");
                    Ok(1usize)
                }
            },
            {
                let live = Rc::clone(&live);
                move || {
                    assert!(!live.get(), "disk-cache reset ran under the outer handle");
                    Ok(2usize)
                }
            },
        )
        .unwrap();

        assert_eq!(phases, vec![("uncached", 1), ("disk_cached", 2)]);
        assert!(!live.get());
    }

    #[test]
    fn uncached_queries_reset_before_each_fresh_handle_and_drop_after_measurement() {
        use std::cell::{Cell, RefCell};
        use std::rc::Rc;

        struct TrackedHandle {
            id: usize,
            live: Rc<Cell<usize>>,
            events: Rc<RefCell<Vec<String>>>,
        }

        impl Drop for TrackedHandle {
            fn drop(&mut self) {
                assert_eq!(self.live.replace(0), 1);
                self.events.borrow_mut().push(format!("drop-{}", self.id));
            }
        }

        let live = Rc::new(Cell::new(0));
        let next_id = Rc::new(Cell::new(0));
        let events = Rc::new(RefCell::new(Vec::new()));
        let summary = execute_uncached_query_sequence(
            2,
            {
                let live = Rc::clone(&live);
                let events = Rc::clone(&events);
                move || {
                    assert_eq!(live.get(), 0, "reset ran under a live serving handle");
                    events.borrow_mut().push("reset".to_string());
                    Ok(())
                }
            },
            {
                let live = Rc::clone(&live);
                let next_id = Rc::clone(&next_id);
                let events = Rc::clone(&events);
                move || {
                    assert_eq!(live.replace(1), 0);
                    let id = next_id.get() + 1;
                    next_id.set(id);
                    events.borrow_mut().push(format!("open-{id}"));
                    Ok(TrackedHandle {
                        id,
                        live: Rc::clone(&live),
                        events: Rc::clone(&events),
                    })
                }
            },
            {
                let events = Rc::clone(&events);
                move |handle: &TrackedHandle, query_index| {
                    events
                        .borrow_mut()
                        .push(format!("measure-{}-{query_index}", handle.id));
                    Ok(QuerySummary {
                        latencies_ms: vec![1.0],
                        ..QuerySummary::default()
                    })
                }
            },
        )
        .unwrap();

        assert_eq!(summary.count(), 2);
        assert_eq!(live.get(), 0);
        assert_eq!(
            *events.borrow(),
            [
                "reset",
                "open-1",
                "measure-1-0",
                "drop-1",
                "reset",
                "open-2",
                "measure-2-1",
                "drop-2",
            ]
        );
    }

    #[test]
    fn concurrency_cache_setup_resets_before_opening_the_serving_handle() {
        use std::cell::{Cell, RefCell};
        use std::rc::Rc;

        struct TrackedHandle {
            live: Rc<Cell<bool>>,
            events: Rc<RefCell<Vec<&'static str>>>,
        }

        impl Drop for TrackedHandle {
            fn drop(&mut self) {
                self.live.set(false);
                self.events.borrow_mut().push("drop");
            }
        }

        let live = Rc::new(Cell::new(false));
        let events = Rc::new(RefCell::new(Vec::new()));
        let (handle, state) = execute_concurrency_cache_setup(
            {
                let live = Rc::clone(&live);
                let events = Rc::clone(&events);
                move || {
                    assert!(!live.get(), "cache reset ran under a live serving handle");
                    events.borrow_mut().push("reset");
                    Ok(())
                }
            },
            {
                let live = Rc::clone(&live);
                let events = Rc::clone(&events);
                move || {
                    assert!(!live.replace(true));
                    events.borrow_mut().push("open");
                    Ok(TrackedHandle {
                        live: Rc::clone(&live),
                        events: Rc::clone(&events),
                    })
                }
            },
            {
                let events = Rc::clone(&events);
                move |_handle| {
                    events.borrow_mut().push("prepare");
                    Ok(vec![0usize, 1])
                }
            },
        )
        .unwrap();

        assert_eq!(state, [0, 1]);
        assert_eq!(*events.borrow(), ["reset", "open", "prepare"]);
        drop(handle);
        assert_eq!(*events.borrow(), ["reset", "open", "prepare", "drop"]);
    }

    #[test]
    fn concurrency_worker_error_still_joins_every_spawned_worker() {
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::sync::{Arc, Barrier};

        let second_started = Arc::new(Barrier::new(2));
        let release_second = Arc::new(Barrier::new(2));
        let second_finished = Arc::new(AtomicBool::new(false));
        let handles = vec![
            std::thread::spawn(|| Err("first worker failed".to_string())),
            {
                let second_started = Arc::clone(&second_started);
                let release_second = Arc::clone(&release_second);
                let second_finished = Arc::clone(&second_finished);
                std::thread::spawn(move || {
                    second_started.wait();
                    release_second.wait();
                    second_finished.store(true, Ordering::SeqCst);
                    Ok(Vec::new())
                })
            },
        ];
        second_started.wait();
        let releaser = {
            let release_second = Arc::clone(&release_second);
            std::thread::spawn(move || release_second.wait())
        };

        let error = match join_concurrency_workers(handles) {
            Ok(_) => panic!("concurrency worker failure was discarded"),
            Err(error) => error,
        };

        releaser.join().unwrap();
        assert!(error.to_string().contains("first worker failed"));
        assert!(
            second_finished.load(Ordering::SeqCst),
            "error return detached a still-running concurrency worker"
        );
    }

    #[test]
    fn disk_cached_concurrency_clears_retained_state_before_every_prime() {
        use std::cell::{Cell, RefCell};
        use std::rc::Rc;
        use std::time::Duration;

        struct TrackedHandle {
            id: usize,
            live: Rc<Cell<usize>>,
            events: Rc<RefCell<Vec<String>>>,
        }

        impl Drop for TrackedHandle {
            fn drop(&mut self) {
                assert_eq!(self.live.replace(0), 1);
                self.events.borrow_mut().push(format!("drop-{}", self.id));
            }
        }

        let live = Rc::new(Cell::new(0));
        let next_id = Rc::new(Cell::new(0));
        let events = Rc::new(RefCell::new(Vec::new()));
        let profiles = execute_disk_cached_concurrency_profiles(
            3,
            &[1, 2],
            {
                let live = Rc::clone(&live);
                let events = Rc::clone(&events);
                move || {
                    assert_eq!(live.get(), 0);
                    events.borrow_mut().push("reset".to_string());
                    Ok(())
                }
            },
            {
                let live = Rc::clone(&live);
                let next_id = Rc::clone(&next_id);
                let events = Rc::clone(&events);
                move || {
                    assert_eq!(live.replace(1), 0);
                    let id = next_id.get() + 1;
                    next_id.set(id);
                    events.borrow_mut().push(format!("open-{id}"));
                    Ok(TrackedHandle {
                        id,
                        live: Rc::clone(&live),
                        events: Rc::clone(&events),
                    })
                }
            },
            {
                let live = Rc::clone(&live);
                let events = Rc::clone(&events);
                move |handle: &TrackedHandle| {
                    assert_eq!(live.get(), 1);
                    events
                        .borrow_mut()
                        .push(format!("clear-disk-{}", handle.id));
                    Ok(())
                }
            },
            {
                let events = Rc::clone(&events);
                move |handle: &TrackedHandle, query_index| {
                    events
                        .borrow_mut()
                        .push(format!("prime-{}-{query_index}", handle.id));
                    Ok(())
                }
            },
            {
                let events = Rc::clone(&events);
                move |handle: &TrackedHandle| {
                    events.borrow_mut().push(format!("clear-{}", handle.id));
                    Ok(())
                }
            },
            {
                let events = Rc::clone(&events);
                move |handle: &TrackedHandle, cohort: &[usize], workers| {
                    events.borrow_mut().push(format!(
                        "measure-{}-{}-{workers}",
                        handle.id,
                        cohort.len()
                    ));
                    Ok((Vec::new(), Duration::from_millis(1)))
                }
            },
        )
        .unwrap();

        assert_eq!(live.get(), 0);
        assert_eq!(profiles.len(), 2);
        assert!(
            profiles
                .iter()
                .all(|(_, measurements, _)| measurements.is_empty())
        );
        assert!(
            profiles
                .iter()
                .all(|(_, _, elapsed)| *elapsed == Duration::from_millis(1))
        );
        assert_eq!(
            *events.borrow(),
            [
                "reset",
                "open-1",
                "clear-disk-1",
                "clear-1",
                "prime-1-0",
                "clear-1",
                "prime-1-1",
                "clear-1",
                "prime-1-2",
                "clear-1",
                "measure-1-3-1",
                "clear-1",
                "measure-1-3-2",
                "drop-1",
            ]
        );
    }

    #[test]
    fn disk_cached_query_guard_stops_at_the_first_nonlocal_measurement() {
        use std::cell::{Cell, RefCell};
        use std::rc::Rc;

        struct TrackedHandle {
            id: usize,
            live: Rc<Cell<usize>>,
            observed: Rc<RefCell<Vec<String>>>,
        }

        impl Drop for TrackedHandle {
            fn drop(&mut self) {
                assert_eq!(self.live.replace(0), 1);
                self.observed.borrow_mut().push(format!("drop-{}", self.id));
            }
        }

        let observed = Rc::new(RefCell::new(Vec::new()));
        let next_handle = Rc::new(Cell::new(0usize));
        let live = Rc::new(Cell::new(0usize));
        let result = execute_disk_cached_query_cohorts(
            2,
            2,
            {
                let observed = Rc::clone(&observed);
                let live = Rc::clone(&live);
                move || {
                    assert_eq!(live.get(), 0);
                    observed.borrow_mut().push("reset".to_owned());
                    Ok(())
                }
            },
            {
                let observed = Rc::clone(&observed);
                let next_handle = Rc::clone(&next_handle);
                let live = Rc::clone(&live);
                move || {
                    assert_eq!(live.replace(1), 0);
                    let id = next_handle.get() + 1;
                    next_handle.set(id);
                    observed.borrow_mut().push(format!("open-{id}"));
                    Ok(TrackedHandle {
                        id,
                        live: Rc::clone(&live),
                        observed: Rc::clone(&observed),
                    })
                }
            },
            {
                let observed = Rc::clone(&observed);
                let live = Rc::clone(&live);
                move |handle: &TrackedHandle| {
                    assert_eq!(live.get(), 1);
                    observed
                        .borrow_mut()
                        .push(format!("clear-disk-{}", handle.id));
                    Ok(())
                }
            },
            {
                let observed = Rc::clone(&observed);
                move |handle: &TrackedHandle, query_index| {
                    observed
                        .borrow_mut()
                        .push(format!("prime-{}-{query_index}", handle.id));
                    Ok(())
                }
            },
            {
                let observed = Rc::clone(&observed);
                move |handle: &TrackedHandle| {
                    observed.borrow_mut().push(format!("clear-{}", handle.id));
                    Ok(())
                }
            },
            {
                let observed = Rc::clone(&observed);
                move |handle: &TrackedHandle, query_index| {
                    observed
                        .borrow_mut()
                        .push(format!("measure-{}-{query_index}", handle.id));
                    Ok(match query_index {
                        0 => QuerySummary {
                            latencies_ms: vec![1.0],
                            disk_cache_reads: 1,
                            ..QuerySummary::default()
                        },
                        1 => QuerySummary {
                            latencies_ms: vec![1.0],
                            billable_requests: 1,
                            ..QuerySummary::default()
                        },
                        _ => QuerySummary::default(),
                    })
                }
            },
        );

        assert!(result.is_err());
        assert_eq!(live.get(), 0);
        assert_eq!(
            *observed.borrow(),
            vec![
                "reset",
                "open-1",
                "clear-disk-1",
                "clear-1",
                "prime-1-0",
                "clear-1",
                "prime-1-1",
                "clear-1",
                "measure-1-0",
                "clear-1",
                "measure-1-1",
                "drop-1",
            ]
        );
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
    fn lifecycle_only_is_a_distinct_mutation_profile() {
        assert!(validate_lifecycle_only(false, true, true, true, false, true, true).is_ok());
        assert!(validate_lifecycle_only(true, false, false, false, true, false, false).is_ok());
        assert!(validate_lifecycle_only(true, true, false, false, true, false, false).is_err());
        assert!(validate_lifecycle_only(true, false, true, false, true, false, false).is_err());
        assert!(validate_lifecycle_only(true, false, false, true, true, false, false).is_err());
        assert!(validate_lifecycle_only(true, false, false, false, false, false, false).is_err());
        assert!(validate_lifecycle_only(true, false, false, false, true, true, false).is_err());
        assert!(validate_lifecycle_only(true, false, false, false, true, false, true).is_err());
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
    fn build_writer_count_is_explicitly_bounded() {
        assert_eq!(validate_build_writers(1).unwrap(), 1);
        assert_eq!(validate_build_writers(8).unwrap(), 8);
        assert_eq!(validate_build_writers(32).unwrap(), 32);
        assert!(validate_build_writers(0).is_err());
        assert!(validate_build_writers(33).is_err());
    }

    #[test]
    fn bounded_bulk_ingest_writers_publish_disjoint_generated_id_ranges() {
        let directory = tempfile::tempdir().unwrap();
        let uri = directory.path().to_string_lossy().into_owned();
        BorsukIndex::create(IndexConfig {
            uri: uri.clone(),
            metric: VectorMetric::Euclidean,
            dimensions: 2,
            segment_max_vectors: 16,
            ram_budget_bytes: None,
            text: false,
            named_vectors: Default::default(),
        })
        .unwrap();
        let mut writers = (0..4)
            .map(|_| BorsukIndex::open(&uri).unwrap())
            .collect::<Vec<_>>();
        let batches = (0..4)
            .map(|batch| {
                let start = batch * 2;
                (
                    u8::try_from(batch).unwrap(),
                    start,
                    vec![vec![start as f32, 1.0], vec![start as f32 + 1.0, 1.0]],
                )
            })
            .collect();

        let completed = execute_bulk_add_wave(&mut writers, batches).unwrap();
        assert_eq!(completed, vec![2, 2, 2, 2]);

        fn regular_files(path: &std::path::Path) -> usize {
            let Ok(entries) = std::fs::read_dir(path) else {
                return 0;
            };
            entries
                .filter_map(Result::ok)
                .map(|entry| {
                    let path = entry.path();
                    if path.is_dir() {
                        regular_files(&path)
                    } else {
                        1
                    }
                })
                .sum()
        }
        assert_eq!(
            regular_files(&directory.path().join("id-directory/claim-pages")),
            0,
            "publication bulk ingestion must not run online duplicate-claim coordination"
        );

        let mut finalizer = BorsukIndex::open(&uri).unwrap();
        finalizer.finish_bulk_load().unwrap();
        assert_eq!(finalizer.stats().records, 8);
        let ids = (0..8).map(|row| row.to_string()).collect::<Vec<_>>();
        let records = finalizer.get_records(&ids).unwrap();
        for (row, record) in records.into_iter().enumerate() {
            assert_eq!(record.unwrap().0, vec![row as f32, 1.0]);
        }
    }

    #[test]
    fn bulk_ingest_coordinator_flushes_only_bounded_complete_waves() {
        let directory = tempfile::tempdir().unwrap();
        let uri = directory.path().to_string_lossy().into_owned();
        BorsukIndex::create(IndexConfig {
            uri: uri.clone(),
            metric: VectorMetric::Euclidean,
            dimensions: 2,
            segment_max_vectors: 16,
            ram_budget_bytes: None,
            text: false,
            named_vectors: Default::default(),
        })
        .unwrap();
        let mut coordinator = BuildIngestCoordinator::open(&uri, 3, None).unwrap();
        for batch in 0..5 {
            let start = batch * 2;
            coordinator
                .push(
                    start,
                    vec![vec![start as f32, 2.0], vec![start as f32 + 1.0, 2.0]],
                )
                .unwrap();
            assert!(coordinator.pending_batches() < 3);
        }
        let report = coordinator.finish().unwrap();
        assert_eq!(report.batches, 5);
        assert_eq!(report.rows, 10);
        assert_eq!(report.waves, 2);
        assert!(report.requests.puts > 0);
        assert!(report.bytes_written > 0);

        let mut finalizer = BorsukIndex::open(&uri).unwrap();
        finalizer.finish_bulk_load().unwrap();
        assert_eq!(finalizer.stats().records, 10);
    }

    #[test]
    fn bulk_ingest_materializes_before_reusing_the_sixty_four_source_shards() {
        let directory = tempfile::tempdir().unwrap();
        let uri = directory.path().to_string_lossy().into_owned();
        let mut finalizer = BorsukIndex::create(IndexConfig {
            uri: uri.clone(),
            metric: VectorMetric::Euclidean,
            dimensions: 2,
            segment_max_vectors: 16,
            ram_budget_bytes: None,
            text: false,
            named_vectors: Default::default(),
        })
        .unwrap();
        let mut coordinator = BuildIngestCoordinator::open(&uri, 3, None).unwrap();
        assert_eq!(coordinator.report.materializer_opens, 0);
        for row in 0..70 {
            coordinator.push(row, vec![vec![row as f32, 3.0]]).unwrap();
            if row == 62 {
                assert_eq!(coordinator.report.materializer_opens, 0);
            }
            if row == 63 {
                assert_eq!(coordinator.report.materializer_opens, 1);
            }
        }
        let mut report = coordinator.finish().unwrap();
        reopen_build_finalizer(&mut finalizer, &uri, None, &mut report).unwrap();
        assert_eq!(report.batches, 70);
        assert_eq!(report.rows, 70);
        assert_eq!(report.materializations, 1);
        assert_eq!(report.materializer_opens, 1);

        finalizer.finish_bulk_load().unwrap();
        assert_eq!(finalizer.stats().records, 70);
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

    #[test]
    fn v22_stage_l_evidence_is_canonical_validated_and_no_clobber() {
        let exact_rows = (0_u64..2048)
            .map(|record_id| V22StageLExactRow {
                distance: record_id as f32,
                record_id,
                canonical_record_id: format!("row-{record_id}").into_bytes().into(),
                primary_cell: (record_id % 4) as u32,
                primary_cell_routing_rank: (record_id % 4 + 1) as usize,
            })
            .collect::<Vec<_>>();
        let query_prefixes = (0..2)
            .map(|query_index| V22StageLQueryPrefix {
                query_index,
                rows: exact_rows.clone(),
            })
            .collect::<Vec<_>>();
        let mut layout_censuses = Vec::new();
        for (layout, microcluster_rows) in [
            (V22LayoutKind::V20Physical, None),
            (V22LayoutKind::V20TwoPivotRepacked, Some(32)),
            (V22LayoutKind::V20TwoPivotRepacked, Some(64)),
            (V22LayoutKind::SemanticWithinCell, Some(32)),
            (V22LayoutKind::SemanticWithinCell, Some(64)),
            (V22LayoutKind::SemanticCrossCell, Some(32)),
            (V22LayoutKind::SemanticCrossCell, Some(64)),
        ] {
            for exact_prefix_rows in [10_u16, 256, 512, 1024, 1536, 2048] {
                layout_censuses.push(V22StageLLayoutArmReport {
                    layout,
                    microcluster_rows,
                    exact_prefix_rows,
                    projected_objects: vec![V22StageLProjectedObject {
                        path: format!("{layout:?}-{microcluster_rows:?}.arrow"),
                        checksum: "11".repeat(32),
                        encoded_bytes: 65_536,
                    }],
                    query_samples: (0..2)
                        .map(|query_index| V22StageLLayoutQuerySample {
                            query_index,
                            exact_prefix_rows,
                            required_routing_cells: 4,
                            gt_cell_hits: 10,
                            gt_cell_coverage_ppm: 1_000_000,
                            routed_rows: 2048,
                            useful_bytes: u64::from(exact_prefix_rows) * 32,
                            selected_bytes: 65_536,
                            physical_bytes: 65_536,
                            speculative_bytes: 0,
                            requests: 1,
                            selected_rows: u64::from(exact_prefix_rows),
                            packing_purity_ppm: u64::from(exact_prefix_rows) * 32 * 1_000_000
                                / 65_536,
                            physical_amplification_ppm: 1_000_000,
                            physical_limiting_bound: V22LayoutLimitingBound::Eligible,
                            routing_eligible: true,
                            limiting_bound: V22LayoutLimitingBound::Eligible,
                            eligible: true,
                            ranges: vec![V22StageLRange {
                                path: format!("{layout:?}-{microcluster_rows:?}.arrow"),
                                start: 0,
                                end: 65_536,
                                selected_bytes: 65_536,
                                rows: u64::from(exact_prefix_rows),
                                blocks: 1,
                            }],
                        })
                        .collect(),
                    eligible: true,
                });
            }
        }
        let report = V22StageLReport {
            v20_root_checksum: "22".repeat(32),
            v20_codebook_checksum: "33".repeat(32),
            rows: 10_000_000,
            routing_cell_count: 4096,
            query_prefixes,
            layout_censuses,
        };
        let identity = V22EvidenceIdentity {
            dataset_name: "deep-image-96",
            dataset_id: "deep-image-96",
            index_id: "index-authority",
            source_archive_sha256: &"44".repeat(32),
            dimensions: 96,
            dataset_rows: 10_000_000,
            query_seed: 23_006,
            query_source_indices: &[11, 29],
        };
        let output = tempfile::tempdir().unwrap();
        write_v22_stage_l_evidence(output.path(), &identity, &report).unwrap();
        validate_v22_stage_l_evidence(output.path(), &identity, &report).unwrap();
        assert!(write_v22_stage_l_evidence(output.path(), &identity, &report).is_err());
        assert_eq!(output.path().read_dir().unwrap().count(), 2);

        let mut drifted = report.clone();
        drifted.layout_censuses[0].query_samples[0].packing_purity_ppm += 1;
        assert!(build_v22_stage_l_summary(&identity, &drifted).is_err());
        let mut drifted = report.clone();
        drifted.layout_censuses[0].query_samples[0].physical_amplification_ppm += 1;
        assert!(build_v22_stage_l_summary(&identity, &drifted).is_err());
        let mut drifted = report.clone();
        drifted.layout_censuses[0].query_samples[0].ranges[0].path =
            "unbound-object.arrow".to_string();
        assert!(build_v22_stage_l_summary(&identity, &drifted).is_err());
        let mut drifted = report.clone();
        drifted.layout_censuses[1].projected_objects[0].encoded_bytes += 1;
        assert!(build_v22_stage_l_summary(&identity, &drifted).is_err());
        let mut drifted = report.clone();
        let sample = &mut drifted.layout_censuses[0].query_samples[0];
        let range = sample.ranges[0].clone();
        let segment_ends = [13_107_u64, 26_214, 39_321, 52_428, 65_536];
        sample.ranges = segment_ends
            .into_iter()
            .scan(0_u64, |start, end| {
                let selected_bytes = end - *start;
                let result = V22StageLRange {
                    path: range.path.clone(),
                    start: *start,
                    end,
                    selected_bytes,
                    rows: 2,
                    blocks: 1,
                };
                *start = end;
                Some(result)
            })
            .collect();
        sample.requests = 5;
        assert!(build_v22_stage_l_summary(&identity, &drifted).is_err());
    }

    #[test]
    fn v22_stage_l_relative_output_uses_the_current_directory_for_scratch() {
        assert_eq!(
            v22_stage_l_scratch_parent(Path::new("runtime-output")),
            Path::new(".")
        );
        assert_eq!(
            v22_stage_l_scratch_parent(Path::new("/tmp/runtime-output")),
            Path::new("/tmp")
        );
    }
}
