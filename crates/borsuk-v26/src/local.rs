use std::{
    collections::BTreeSet,
    fs,
    io::Read,
    os::unix::fs::FileExt,
    path::{Path, PathBuf},
    sync::Arc,
};

use arrow_array::{
    Array, ArrayRef, FixedSizeBinaryArray, FixedSizeListArray, Float32Array, RecordBatch,
    StringArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow_ipc::{
    convert::fb_to_schema,
    reader::{FileReader, read_footer_length},
    root_as_footer, root_as_message,
    writer::FileWriter,
};
use arrow_schema::{DataType, Field, Schema};
use parquet::{
    arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder},
    basic::Compression,
    file::properties::{WriterProperties, WriterVersion},
};
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::tree::{build_v26_dual_tree_layout_with_workers, rank_v26_tree_page_prefix};
use crate::{
    Result, V26ConstructionRow, V26Disposition, V26ExactGlobalRankResult, V26ExactGlobalResult,
    V26ExactGlobalSample, V26ExternalQuery, V26ExternalTruth, V26LayoutAuthority, V26LayoutReceipt,
    V26LayoutResult, V26LayoutSample, V26Node, V26ObjectIdentity, V26PageModeSample,
    V26Pq16ServingSelection, V26PqRankedRow, V26QueryTruth, V26RowPages, V26Tree,
    build_v26_external_truth_rows, canonical_json_value, canonical_v26_exact_global_result_bytes,
    canonical_v26_layout_receipt_bytes, canonical_v26_layout_result_bytes_with_page_budget,
    canonical_v26_tree_router_result_bytes, diagnose_v26_global_centroid_candidate_widths,
    diagnose_v26_global_page_mode_candidate_widths, diagnose_v26_tree_router_candidate_widths,
    evaluate_v26_candidate_row_cover, evaluate_v26_centroid_router,
    evaluate_v26_exact_global_external_rows, evaluate_v26_page_mode_router,
    evaluate_v26_pq_width_ladder, evaluate_v26_pq8_candidate_cover,
    evaluate_v26_pq16_exact_rerank_ladder, evaluate_v26_tree_router, exact_lower_hex,
    exact_v26_layout_oracle_pages, invalid, projected_steps, projected_v26_pq8_resident_bytes,
    projected_v26_pq16_rerank_resident_bytes, rank_v26_pq16_packed_candidates, rank_v26_tree_pages,
    v26_squared_l2, validate_layout_authority, validate_v26_dual_tree_layout, validate_v26_vector,
};

fn vector_type() -> DataType {
    DataType::FixedSizeList(
        Arc::new(Field::new("element", DataType::Float32, false)),
        96,
    )
}

pub fn v26_construction_schema() -> Schema {
    Schema::new(vec![
        Field::new("source_ordinal", DataType::UInt64, false),
        Field::new("vector", vector_type(), false),
    ])
}

pub fn v26_query_schema() -> Schema {
    Schema::new(vec![Field::new("emb", vector_type(), false)])
}

pub fn v26_truth_schema() -> Schema {
    Schema::new(vec![
        Field::new("query_ordinal", DataType::UInt32, false),
        Field::new(
            "neighbor_source_ordinals",
            DataType::FixedSizeList(Arc::new(Field::new("element", DataType::UInt64, false)), 10),
            false,
        ),
        Field::new(
            "neighbor_distance_bits",
            DataType::FixedSizeList(Arc::new(Field::new("element", DataType::UInt32, false)), 10),
            false,
        ),
        Field::new("construction_sha256", DataType::Utf8, false),
        Field::new("external_queries_sha256", DataType::Utf8, false),
    ])
}

pub fn v26_tree_schema() -> Schema {
    Schema::new(vec![
        Field::new("node_ordinal", DataType::UInt32, false),
        Field::new("left", DataType::UInt32, true),
        Field::new("right", DataType::UInt32, true),
        Field::new("direction_ordinal", DataType::UInt8, false),
        Field::new("threshold", DataType::Float32, false),
        Field::new("split_gap", DataType::Float32, false),
        Field::new("leaf_page", DataType::UInt32, true),
    ])
}

pub fn v26_page_assignments_schema() -> Schema {
    Schema::new(vec![
        Field::new("source_ordinal", DataType::UInt64, false),
        Field::new("primary_page", DataType::UInt32, false),
        Field::new("replica_page", DataType::UInt32, false),
    ])
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct V26LocalObjectPath {
    pub identity: V26ObjectIdentity,
    pub path: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct V26LayoutBuildRequest {
    pub manifest: V26LocalObjectPath,
    pub construction_rows: V26LocalObjectPath,
    pub output_dir: PathBuf,
    pub output_uri_prefix: String,
    pub worker_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct V26LayoutEvaluationRequest {
    pub layout_terminal: V26LocalObjectPath,
    pub page_assignments: V26LocalObjectPath,
    pub external_queries: V26LocalObjectPath,
    pub truth: V26LocalObjectPath,
    pub expected_queries: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct V26ExactGlobalRequest {
    pub construction_rows: V26LocalObjectPath,
    pub layout: V26LayoutEvaluationRequest,
    pub ranked_row_limits: Vec<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct V26TreeRouterRequest {
    pub primary_tree: V26LocalObjectPath,
    pub replica_tree: V26LocalObjectPath,
    pub layout: V26LayoutEvaluationRequest,
    pub page_budget: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct V26CentroidRouterRequest {
    pub construction_rows: V26LocalObjectPath,
    pub router: V26TreeRouterRequest,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct V26PageModeRouterRequest {
    pub construction_rows: V26LocalObjectPath,
    pub router: V26TreeRouterRequest,
    pub evidence_output_path: PathBuf,
    pub evidence_output_uri: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct V26CandidateCoverRequest {
    pub construction_rows: V26LocalObjectPath,
    pub router: V26TreeRouterRequest,
    pub evidence_output_path: PathBuf,
    pub evidence_output_uri: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct V26Pq8CoverRequest {
    pub construction_rows: V26LocalObjectPath,
    pub router: V26TreeRouterRequest,
    pub evidence_output_path: PathBuf,
    pub evidence_output_uri: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct V26PqWidthLadderRequest {
    pub construction_rows: V26LocalObjectPath,
    pub router: V26TreeRouterRequest,
    pub evidence_output_path: PathBuf,
    pub evidence_output_uri: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct V26Pq16RerankRequest {
    pub construction_rows: V26LocalObjectPath,
    pub router: V26TreeRouterRequest,
    pub evidence_output_path: PathBuf,
    pub evidence_output_uri: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct V26ColdVectorManifest {
    pub row_count: u64,
    pub batch_rows: u32,
    pub encoded_bytes: u64,
    pub sha256: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct V26ColdVectorRead {
    pub vectors: Vec<[f32; 96]>,
    pub assignments: Vec<V26RowPages>,
    pub batches_read: u32,
    pub read_workers: u32,
}

pub struct V26ArrowColdVectors {
    file: fs::File,
    batches: Vec<V26ColdVectorBatch>,
    pool: rayon::ThreadPool,
    row_count: u64,
    batch_rows: u32,
}

#[derive(Debug, Clone, Copy)]
struct V26ColdVectorBatch {
    row_start: u64,
    row_count: u32,
    ordinal_values_offset: u64,
    vector_values_offset: u64,
    primary_values_offset: u64,
    replica_values_offset: u64,
}

struct V26ColdVectorSliceRead {
    vectors: Vec<[f32; 96]>,
    batches_read: u32,
    read_workers: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct V26ArrowFileIdentity {
    pub encoded_bytes: u64,
    pub sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct V26Pq16IndexManifest {
    pub row_count: u64,
    pub page_count: u32,
    pub occurrence_count: u64,
    pub projected_resident_bytes_100m: u64,
    pub codebook: V26ArrowFileIdentity,
    pub codes: V26ArrowFileIdentity,
    pub postings: V26ArrowFileIdentity,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct V26Pq4FastManifest {
    pub schema: String,
    pub construction_rows: V26ObjectIdentity,
    pub page_assignments: V26ObjectIdentity,
    pub layout_terminal: V26ObjectIdentity,
    pub codebook: V26ObjectIdentity,
    pub codes: V26ObjectIdentity,
    pub row_count: u64,
    pub block_count: u64,
    pub padding_rows: u32,
    pub dimension: u32,
    pub subquantizer_count: u32,
    pub subspace_dimensions: u32,
    pub centroid_count: u32,
    pub block_rows: u32,
    pub code_bytes_per_row: u32,
    pub byte_order: String,
    pub nibble_order: String,
    pub source_order: String,
    pub projected_resident_bytes_100m: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct V26SimHashPq16IndexManifest {
    pub row_count: u64,
    pub page_count: u32,
    pub bucket_count: u32,
    pub projected_resident_bytes_100m: u64,
    pub codebook: V26ArrowFileIdentity,
    pub buckets: V26ArrowFileIdentity,
    pub records: V26ArrowFileIdentity,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct V26DualPqKeyIndexManifest {
    pub row_count: u64,
    pub plane_count: u32,
    pub bucket_count: u32,
    pub projected_resident_bytes_100m: u64,
    pub offsets: V26ArrowFileIdentity,
    pub ordinals: V26ArrowFileIdentity,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct V26Pq16ServingBuildRequest {
    pub construction_rows: V26LocalObjectPath,
    pub page_assignments: V26LocalObjectPath,
    pub layout_terminal: V26LocalObjectPath,
    pub primary_tree: V26LocalObjectPath,
    pub replica_tree: V26LocalObjectPath,
    pub expected_rows: u64,
    pub output_dir: PathBuf,
    pub output_uri_prefix: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct V26Pq4FastBuildRequest {
    pub construction_rows: V26LocalObjectPath,
    pub page_assignments: V26LocalObjectPath,
    pub layout_terminal: V26LocalObjectPath,
    pub expected_rows: u64,
    pub output_dir: PathBuf,
    pub output_uri_prefix: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct V26Pq4QualityRequest {
    pub pq4_manifest: V26LocalObjectPath,
    pub pq4_dir: PathBuf,
    pub cold_vectors: V26LocalObjectPath,
    pub cold_vectors_manifest: V26ColdVectorManifest,
    pub layout_terminal: V26LocalObjectPath,
    pub external_queries: V26LocalObjectPath,
    pub truth: V26LocalObjectPath,
    pub evidence_output_path: PathBuf,
    pub evidence_output_uri: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct V26Pq16ServingBuildOutput {
    pub schema: String,
    pub inputs: Vec<V26ObjectIdentity>,
    pub outputs: Vec<V26ObjectIdentity>,
    pub row_count: u64,
    pub page_count: u32,
    pub projected_resident_bytes_100m: u64,
    pub index: V26Pq16IndexManifest,
    pub simhash_index: V26SimHashPq16IndexManifest,
    pub cold_vectors: V26ColdVectorManifest,
    pub query_role_opens: u32,
    pub page_body_reads: u32,
    pub claim_eligible: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct V26Pq16ServingRuntimeRequest {
    pub serving_manifest: V26LocalObjectPath,
    pub serving_dir: PathBuf,
    pub layout_terminal: V26LocalObjectPath,
    pub primary_tree: V26LocalObjectPath,
    pub replica_tree: V26LocalObjectPath,
    pub external_queries: V26LocalObjectPath,
    pub expected_queries: u32,
}

pub struct V26Pq16ServingRuntime {
    index: crate::V26PackedPq16Index,
    cold_vectors: V26ArrowColdVectors,
    primary: V26Tree,
    replica: V26Tree,
    queries: Vec<V26ExternalQuery>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct V26ServingLatencySample {
    pub sample_ordinal: u32,
    pub query_ordinal: u32,
    pub elapsed_ns: u64,
    pub cold_batches_read: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct V26Pq16ServingBenchmarkResult {
    pub schema: String,
    pub serving_manifest: V26ObjectIdentity,
    pub external_queries: V26ObjectIdentity,
    pub latency_evidence: V26ObjectIdentity,
    pub query_count: u32,
    pub candidate_page_limit: u32,
    pub ranked_row_limit: u32,
    pub selected_page_count: u32,
    pub warmup_count: u32,
    pub measurement_count: u32,
    pub p50_ns: u64,
    pub p95_ns: u64,
    pub p99_ns: u64,
    pub maximum_ns: u64,
    pub p99_gate_ns: u64,
    pub passed: bool,
    pub page_body_reads: u32,
    pub claim_eligible: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct V26Pq16GlobalPreflightResult {
    pub schema: String,
    pub serving_manifest: V26ObjectIdentity,
    pub external_queries: V26ObjectIdentity,
    pub latency_evidence: V26ObjectIdentity,
    pub query_count: u32,
    pub ranked_row_limit: u32,
    pub selected_page_count: u32,
    pub warmup_count: u32,
    pub measurement_count: u32,
    pub p50_ns: u64,
    pub p95_ns: u64,
    pub maximum_ns: u64,
    pub fail_fast_gate_ns: u64,
    pub passed: bool,
    pub page_body_reads: u32,
    pub claim_eligible: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct V26Pq16GlobalQualitySample {
    pub query_ordinal: u32,
    pub selected_pages: Vec<u32>,
    pub hits: u32,
    pub oracle_hits: u32,
    pub recall_ppm: u64,
    pub oracle_attainment_ppm: u64,
    pub elapsed_ns: u64,
    pub global_adc_elapsed_ns: u64,
    pub exact_rerank_elapsed_ns: u64,
    pub exact_rows_read: u32,
    pub cold_batches_read: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct V26Pq16GlobalQualityResult {
    pub schema: String,
    pub serving_manifest: V26ObjectIdentity,
    pub external_queries: V26ObjectIdentity,
    pub truth: V26ObjectIdentity,
    pub evidence: V26ObjectIdentity,
    pub query_count: u32,
    pub ranked_row_limit: u32,
    pub selected_page_count: u32,
    pub warmup_count: u32,
    pub measurement_count: u32,
    pub p50_ns: u64,
    pub p95_ns: u64,
    pub maximum_ns: u64,
    pub global_adc_p50_ns: u64,
    pub global_adc_p95_ns: u64,
    pub global_adc_maximum_ns: u64,
    pub exact_rerank_p50_ns: u64,
    pub exact_rerank_p95_ns: u64,
    pub exact_rerank_maximum_ns: u64,
    pub fail_fast_gate_ns: u64,
    pub aggregate_recall_ppm: u64,
    pub minimum_query_recall_ppm: u64,
    pub oracle_attainment_ppm: u64,
    pub aggregate_recall_gate_ppm: u64,
    pub minimum_query_recall_gate_ppm: u64,
    pub oracle_attainment_gate_ppm: u64,
    pub passed: bool,
    pub page_body_reads: u32,
    pub claim_eligible: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct V26Pq4QualitySample {
    pub ranked_row_limit: u32,
    pub query_ordinal: u32,
    pub selected_pages: Vec<u32>,
    pub hits: u32,
    pub oracle_hits: u32,
    pub recall_ppm: u64,
    pub oracle_attainment_ppm: u64,
    pub scan_elapsed_ns: u64,
    pub exact_rerank_elapsed_ns: u64,
    pub quantization_scale_bits: u32,
    pub saturation_count: u32,
    pub maximum_distance_error_bits: u32,
    pub page_body_reads: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct V26Pq4QualityArmResult {
    pub ranked_row_limit: u32,
    pub aggregate_recall_ppm: u64,
    pub minimum_query_recall_ppm: u64,
    pub oracle_attainment_ppm: u64,
    pub maximum_scan_elapsed_ns: u64,
    pub maximum_exact_rerank_elapsed_ns: u64,
    pub maximum_saturation_count: u32,
    pub maximum_distance_error_bits: u32,
    pub passed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct V26Pq4QualityResult {
    pub schema: String,
    pub pq4_manifest: V26ObjectIdentity,
    pub external_queries: V26ObjectIdentity,
    pub truth: V26ObjectIdentity,
    pub evidence: V26ObjectIdentity,
    pub backend: String,
    pub query_count: u32,
    pub candidate_depths: Vec<u32>,
    pub selected_page_count: u32,
    pub aggregate_recall_gate_ppm: u64,
    pub minimum_query_recall_gate_ppm: u64,
    pub oracle_attainment_gate_ppm: u64,
    pub arms: Vec<V26Pq4QualityArmResult>,
    pub smallest_passing_ranked_row_limit: Option<u32>,
    pub page_body_reads: u32,
    pub claim_eligible: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct V26SimHashPreflightSample {
    pub bucket_limit: u32,
    pub query_ordinal: u32,
    pub selected_pages: Vec<u32>,
    pub hits: u32,
    pub oracle_hits: u32,
    pub recall_ppm: u64,
    pub oracle_attainment_ppm: u64,
    pub elapsed_ns: u64,
    pub rows_scanned: u64,
    pub cold_batches_read: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct V26SimHashPreflightArmResult {
    pub bucket_limit: u32,
    pub aggregate_recall_ppm: u64,
    pub minimum_query_recall_ppm: u64,
    pub oracle_attainment_ppm: u64,
    pub maximum_latency_ns: u64,
    pub minimum_rows_scanned: u64,
    pub maximum_rows_scanned: u64,
    pub passed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct V26SimHashPreflightAuthority {
    pub serving_manifest: V26ObjectIdentity,
    pub external_queries: V26ObjectIdentity,
    pub truth: V26ObjectIdentity,
    pub evidence: V26ObjectIdentity,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct V26SimHashPreflightResult {
    pub schema: String,
    pub authority: V26SimHashPreflightAuthority,
    pub query_count: u32,
    pub ranked_row_limit: u32,
    pub selected_page_count: u32,
    pub aggregate_recall_gate_ppm: u64,
    pub minimum_query_recall_gate_ppm: u64,
    pub oracle_attainment_gate_ppm: u64,
    pub maximum_latency_gate_ns: u64,
    pub arms: Vec<V26SimHashPreflightArmResult>,
    pub page_body_reads: u32,
    pub claim_eligible: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct V26DualPqKeyPreflightSample {
    pub key_limit_per_plane: u32,
    pub ranked_row_limit: u32,
    pub query_ordinal: u32,
    pub selected_pages: Vec<u32>,
    pub hits: u32,
    pub oracle_hits: u32,
    pub recall_ppm: u64,
    pub oracle_attainment_ppm: u64,
    pub elapsed_ns: u64,
    pub unique_rows_scanned: u64,
    pub cold_batches_read: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct V26DualPqKeyPreflightArmResult {
    pub key_limit_per_plane: u32,
    pub ranked_row_limit: u32,
    pub aggregate_recall_ppm: u64,
    pub minimum_query_recall_ppm: u64,
    pub oracle_attainment_ppm: u64,
    pub maximum_latency_ns: u64,
    pub minimum_unique_rows_scanned: u64,
    pub maximum_unique_rows_scanned: u64,
    pub passed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct V26DualPqKeyPreflightAuthority {
    pub serving_manifest: V26ObjectIdentity,
    pub external_queries: V26ObjectIdentity,
    pub truth: V26ObjectIdentity,
    pub offsets: V26ObjectIdentity,
    pub ordinals: V26ObjectIdentity,
    pub evidence: V26ObjectIdentity,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct V26DualPqKeyPreflightResult {
    pub schema: String,
    pub authority: V26DualPqKeyPreflightAuthority,
    pub query_count: u32,
    pub ranked_row_limit: u32,
    pub selected_page_count: u32,
    pub aggregate_recall_gate_ppm: u64,
    pub minimum_query_recall_gate_ppm: u64,
    pub oracle_attainment_gate_ppm: u64,
    pub maximum_latency_gate_ns: u64,
    pub arms: Vec<V26DualPqKeyPreflightArmResult>,
    pub page_body_reads: u32,
    pub claim_eligible: bool,
}

fn summarize_v26_dual_pq_key_preflight(
    authority: V26DualPqKeyPreflightAuthority,
    samples: &[V26DualPqKeyPreflightSample],
) -> Result<V26DualPqKeyPreflightResult> {
    const KEY_LIMITS: [u32; 3] = [1_536, 4_096, 8_192];
    const RANKED_ROW_LIMIT: u32 = 512;
    const QUERY_COUNT: usize = 32;
    let roles = [
        "pq16-serving-manifest",
        "external-queries-parquet",
        "truth-parquet",
        "dual-pq-key-offsets-arrow",
        "dual-pq-key-ordinals-arrow",
        "dual-pq-key-preflight-evidence-parquet",
    ];
    let identities = [
        &authority.serving_manifest,
        &authority.external_queries,
        &authority.truth,
        &authority.offsets,
        &authority.ordinals,
        &authority.evidence,
    ];
    for (identity, role) in identities.iter().zip(roles) {
        validate_v26_benchmark_identity(identity, role)?;
    }
    let generation = &authority.serving_manifest.generation;
    let mut uris = BTreeSet::new();
    if identities
        .iter()
        .any(|identity| identity.generation != *generation || !uris.insert(&identity.uri))
        || samples.len() != KEY_LIMITS.len() * QUERY_COUNT
    {
        return Err(invalid(
            "V26 dual PQ-key preflight sample inventory differs",
        ));
    }
    let arms = KEY_LIMITS
        .into_iter()
        .enumerate()
        .map(|(arm_index, key_limit_per_plane)| {
            let arm = &samples[arm_index * QUERY_COUNT..(arm_index + 1) * QUERY_COUNT];
            let mut total_hits = 0_u64;
            let mut total_oracle_hits = 0_u64;
            for (query_index, sample) in arm.iter().enumerate() {
                if sample.key_limit_per_plane != key_limit_per_plane
                    || sample.ranked_row_limit != RANKED_ROW_LIMIT
                    || usize::try_from(sample.query_ordinal).ok() != Some(query_index)
                    || sample.selected_pages.len() != crate::V26_SERVING_PAGE_BUDGET
                    || sample
                        .selected_pages
                        .windows(2)
                        .any(|pair| pair[0] >= pair[1])
                    || sample.hits > sample.oracle_hits
                    || !(1..=10).contains(&sample.oracle_hits)
                    || sample.recall_ppm != u64::from(sample.hits) * 100_000
                    || sample.oracle_attainment_ppm
                        != u64::from(sample.hits) * 1_000_000 / u64::from(sample.oracle_hits)
                    || sample.elapsed_ns == 0
                    || sample.unique_rows_scanned < u64::from(RANKED_ROW_LIMIT)
                    || sample.cold_batches_read == 0
                {
                    return Err(invalid(
                        "V26 dual PQ-key preflight sample authority differs",
                    ));
                }
                total_hits += u64::from(sample.hits);
                total_oracle_hits += u64::from(sample.oracle_hits);
            }
            let aggregate_recall_ppm = total_hits * 1_000_000 / 320;
            let minimum_query_recall_ppm =
                arm.iter().map(|sample| sample.recall_ppm).min().unwrap();
            let oracle_attainment_ppm = total_hits * 1_000_000 / total_oracle_hits;
            let maximum_latency_ns = arm.iter().map(|sample| sample.elapsed_ns).max().unwrap();
            Ok(V26DualPqKeyPreflightArmResult {
                key_limit_per_plane,
                ranked_row_limit: RANKED_ROW_LIMIT,
                aggregate_recall_ppm,
                minimum_query_recall_ppm,
                oracle_attainment_ppm,
                maximum_latency_ns,
                minimum_unique_rows_scanned: arm
                    .iter()
                    .map(|sample| sample.unique_rows_scanned)
                    .min()
                    .unwrap(),
                maximum_unique_rows_scanned: arm
                    .iter()
                    .map(|sample| sample.unique_rows_scanned)
                    .max()
                    .unwrap(),
                passed: aggregate_recall_ppm >= 975_000
                    && minimum_query_recall_ppm >= 800_000
                    && oracle_attainment_ppm >= 995_000
                    && maximum_latency_ns <= 15_000_000
                    && arm
                        .iter()
                        .all(|sample| sample.unique_rows_scanned >= u64::from(RANKED_ROW_LIMIT)),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(V26DualPqKeyPreflightResult {
        schema: "borsuk-v26-dual-pq-key-preflight-result-v1".to_owned(),
        authority,
        query_count: 32,
        ranked_row_limit: RANKED_ROW_LIMIT,
        selected_page_count: u32::try_from(crate::V26_SERVING_PAGE_BUDGET).unwrap(),
        aggregate_recall_gate_ppm: 975_000,
        minimum_query_recall_gate_ppm: 800_000,
        oracle_attainment_gate_ppm: 995_000,
        maximum_latency_gate_ns: 15_000_000,
        arms,
        page_body_reads: 0,
        claim_eligible: false,
    })
}

pub fn canonical_v26_dual_pq_key_preflight_result_bytes(
    result: &V26DualPqKeyPreflightResult,
    samples: &[V26DualPqKeyPreflightSample],
) -> Result<Vec<u8>> {
    if result != &summarize_v26_dual_pq_key_preflight(result.authority.clone(), samples)? {
        return Err(invalid("V26 dual PQ-key preflight result differs"));
    }
    let value = serde_json::to_value(result)
        .map_err(|error| invalid(&format!("V26 dual PQ-key result failed: {error}")))?;
    let mut bytes = serde_json::to_vec(&canonical_json_value(value))
        .map_err(|error| invalid(&format!("V26 dual PQ-key result failed: {error}")))?;
    bytes.push(b'\n');
    Ok(bytes)
}

fn summarize_v26_simhash_preflight(
    authority: V26SimHashPreflightAuthority,
    samples: &[V26SimHashPreflightSample],
) -> Result<V26SimHashPreflightResult> {
    const BUCKET_LIMITS: [u32; 3] = [137, 697, 2_517];
    const QUERY_COUNT: usize = 32;
    validate_v26_benchmark_identity(&authority.serving_manifest, "pq16-serving-manifest")?;
    validate_v26_benchmark_identity(&authority.external_queries, "external-queries-parquet")?;
    validate_v26_benchmark_identity(&authority.truth, "truth-parquet")?;
    validate_v26_benchmark_identity(&authority.evidence, "simhash-preflight-evidence-parquet")?;
    let generation = &authority.serving_manifest.generation;
    let mut uris = BTreeSet::new();
    if [
        &authority.serving_manifest,
        &authority.external_queries,
        &authority.truth,
        &authority.evidence,
    ]
    .iter()
    .any(|identity| identity.generation != *generation || !uris.insert(&identity.uri))
        || samples.len() != BUCKET_LIMITS.len() * QUERY_COUNT
    {
        return Err(invalid("V26 SimHash preflight sample inventory differs"));
    }
    let arms = BUCKET_LIMITS
        .into_iter()
        .enumerate()
        .map(|(arm_index, bucket_limit)| {
            let arm = &samples[arm_index * QUERY_COUNT..(arm_index + 1) * QUERY_COUNT];
            let mut total_hits = 0_u64;
            let mut total_oracle_hits = 0_u64;
            for (query_index, sample) in arm.iter().enumerate() {
                if sample.bucket_limit != bucket_limit
                    || usize::try_from(sample.query_ordinal).ok() != Some(query_index)
                    || sample.selected_pages.len() != crate::V26_SERVING_PAGE_BUDGET
                    || sample
                        .selected_pages
                        .windows(2)
                        .any(|pair| pair[0] >= pair[1])
                    || sample.hits > sample.oracle_hits
                    || !(1..=10).contains(&sample.oracle_hits)
                    || sample.recall_ppm != u64::from(sample.hits) * 100_000
                    || sample.oracle_attainment_ppm
                        != u64::from(sample.hits) * 1_000_000 / u64::from(sample.oracle_hits)
                    || sample.elapsed_ns == 0
                    || sample.rows_scanned < 10
                    || sample.cold_batches_read == 0
                {
                    return Err(invalid("V26 SimHash preflight sample authority differs"));
                }
                total_hits = total_hits
                    .checked_add(u64::from(sample.hits))
                    .ok_or_else(|| invalid("V26 SimHash preflight metric overflows"))?;
                total_oracle_hits = total_oracle_hits
                    .checked_add(u64::from(sample.oracle_hits))
                    .ok_or_else(|| invalid("V26 SimHash preflight metric overflows"))?;
            }
            let aggregate_recall_ppm = total_hits * 1_000_000 / 320;
            let minimum_query_recall_ppm =
                arm.iter().map(|sample| sample.recall_ppm).min().unwrap();
            let oracle_attainment_ppm = total_hits * 1_000_000 / total_oracle_hits;
            let maximum_latency_ns = arm.iter().map(|sample| sample.elapsed_ns).max().unwrap();
            Ok(V26SimHashPreflightArmResult {
                bucket_limit,
                aggregate_recall_ppm,
                minimum_query_recall_ppm,
                oracle_attainment_ppm,
                maximum_latency_ns,
                minimum_rows_scanned: arm.iter().map(|sample| sample.rows_scanned).min().unwrap(),
                maximum_rows_scanned: arm.iter().map(|sample| sample.rows_scanned).max().unwrap(),
                passed: aggregate_recall_ppm >= 975_000
                    && minimum_query_recall_ppm >= 800_000
                    && oracle_attainment_ppm >= 995_000
                    && maximum_latency_ns <= 15_000_000
                    && arm.iter().all(|sample| sample.rows_scanned >= 2_048),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(V26SimHashPreflightResult {
        schema: "borsuk-v26-simhash-pq16-preflight-result-v1".to_owned(),
        authority,
        query_count: 32,
        ranked_row_limit: 2_048,
        selected_page_count: u32::try_from(crate::V26_SERVING_PAGE_BUDGET).unwrap(),
        aggregate_recall_gate_ppm: 975_000,
        minimum_query_recall_gate_ppm: 800_000,
        oracle_attainment_gate_ppm: 995_000,
        maximum_latency_gate_ns: 15_000_000,
        arms,
        page_body_reads: 0,
        claim_eligible: false,
    })
}

pub fn canonical_v26_simhash_preflight_result_bytes(
    result: &V26SimHashPreflightResult,
    samples: &[V26SimHashPreflightSample],
) -> Result<Vec<u8>> {
    if result != &summarize_v26_simhash_preflight(result.authority.clone(), samples)? {
        return Err(invalid("V26 SimHash preflight result differs"));
    }
    let value = serde_json::to_value(result)
        .map_err(|error| invalid(&format!("V26 SimHash preflight result failed: {error}")))?;
    let mut bytes = serde_json::to_vec(&canonical_json_value(value))
        .map_err(|error| invalid(&format!("V26 SimHash preflight result failed: {error}")))?;
    bytes.push(b'\n');
    Ok(bytes)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct V26Pq16ServingBenchmarkRequest {
    pub runtime: V26Pq16ServingRuntimeRequest,
    pub latency_output_path: PathBuf,
    pub latency_output_uri: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct V26Pq16GlobalPreflightRequest {
    pub runtime: V26Pq16ServingRuntimeRequest,
    pub latency_output_path: PathBuf,
    pub latency_output_uri: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct V26Pq16GlobalQualityRequest {
    pub serving_manifest: V26LocalObjectPath,
    pub serving_dir: PathBuf,
    pub layout_terminal: V26LocalObjectPath,
    pub external_queries: V26LocalObjectPath,
    pub truth: V26LocalObjectPath,
    pub evidence_output_path: PathBuf,
    pub evidence_output_uri: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct V26SimHashPreflightRequest {
    pub serving_manifest: V26LocalObjectPath,
    pub serving_dir: PathBuf,
    pub layout_terminal: V26LocalObjectPath,
    pub external_queries: V26LocalObjectPath,
    pub truth: V26LocalObjectPath,
    pub evidence_output_path: PathBuf,
    pub evidence_output_uri: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct V26DualPqKeyPreflightRequest {
    pub serving_manifest: V26LocalObjectPath,
    pub serving_dir: PathBuf,
    pub layout_terminal: V26LocalObjectPath,
    pub external_queries: V26LocalObjectPath,
    pub truth: V26LocalObjectPath,
    pub dual_index_dir: PathBuf,
    pub dual_index: V26DualPqKeyIndexManifest,
    pub offsets_uri: String,
    pub ordinals_uri: String,
    pub evidence_output_path: PathBuf,
    pub evidence_output_uri: String,
}

fn summarize_v26_pq16_global_preflight(
    serving_manifest: V26ObjectIdentity,
    external_queries: V26ObjectIdentity,
    latency_evidence: V26ObjectIdentity,
    samples: &[V26ServingLatencySample],
) -> Result<V26Pq16GlobalPreflightResult> {
    validate_v26_benchmark_identity(&serving_manifest, "pq16-serving-manifest")?;
    validate_v26_benchmark_identity(&external_queries, "external-queries-parquet")?;
    validate_v26_benchmark_identity(&latency_evidence, "pq16-global-preflight-latency-parquet")?;
    if serving_manifest.generation != external_queries.generation
        || serving_manifest.generation != latency_evidence.generation
        || samples.len() != 32
        || samples.iter().enumerate().any(|(ordinal, sample)| {
            usize::try_from(sample.sample_ordinal).ok() != Some(ordinal)
                || sample.query_ordinal != sample.sample_ordinal
                || sample.elapsed_ns == 0
                || sample.cold_batches_read == 0
                || sample.cold_batches_read > 2_048
        })
    {
        return Err(invalid("V26 global preflight sample authority differs"));
    }
    let mut timings = samples
        .iter()
        .map(|sample| sample.elapsed_ns)
        .collect::<Vec<_>>();
    timings.sort_unstable();
    let percentile = |percent: usize| timings[(timings.len() * percent).div_ceil(100) - 1];
    let maximum_ns = *timings.last().unwrap();
    Ok(V26Pq16GlobalPreflightResult {
        schema: "borsuk-v26-pq16-global-preflight-result-v1".to_owned(),
        serving_manifest,
        external_queries,
        latency_evidence,
        query_count: 32,
        ranked_row_limit: 2_048,
        selected_page_count: u32::try_from(crate::V26_SERVING_PAGE_BUDGET).unwrap(),
        warmup_count: 2,
        measurement_count: 32,
        p50_ns: percentile(50),
        p95_ns: percentile(95),
        maximum_ns,
        fail_fast_gate_ns: 15_000_000,
        passed: maximum_ns <= 15_000_000,
        page_body_reads: 0,
        claim_eligible: false,
    })
}

pub fn canonical_v26_pq16_global_preflight_result_bytes(
    result: &V26Pq16GlobalPreflightResult,
    samples: &[V26ServingLatencySample],
) -> Result<Vec<u8>> {
    let expected = summarize_v26_pq16_global_preflight(
        result.serving_manifest.clone(),
        result.external_queries.clone(),
        result.latency_evidence.clone(),
        samples,
    )?;
    if result != &expected {
        return Err(invalid("V26 global preflight result differs"));
    }
    let value = serde_json::to_value(result)
        .map_err(|error| invalid(&format!("V26 global preflight result failed: {error}")))?;
    let mut bytes = serde_json::to_vec(&canonical_json_value(value))
        .map_err(|error| invalid(&format!("V26 global preflight result failed: {error}")))?;
    bytes.push(b'\n');
    Ok(bytes)
}

fn summarize_v26_pq16_global_quality(
    serving_manifest: V26ObjectIdentity,
    external_queries: V26ObjectIdentity,
    truth: V26ObjectIdentity,
    evidence: V26ObjectIdentity,
    samples: &[V26Pq16GlobalQualitySample],
) -> Result<V26Pq16GlobalQualityResult> {
    let identities = [
        (&serving_manifest, "pq16-serving-manifest"),
        (&external_queries, "external-queries-parquet"),
        (&truth, "truth-parquet"),
        (&evidence, "pq16-global-preflight-evidence-parquet"),
    ];
    let mut uris = BTreeSet::new();
    for (identity, role) in identities {
        validate_v26_benchmark_identity(identity, role)?;
        if identity.generation != serving_manifest.generation || !uris.insert(&identity.uri) {
            return Err(invalid("V26 global quality authority differs"));
        }
    }
    if samples.len() != 32 {
        return Err(invalid("V26 global quality sample inventory differs"));
    }
    let mut total_hits = 0_u64;
    let mut total_oracle_hits = 0_u64;
    let mut timings = Vec::with_capacity(samples.len());
    let mut global_adc_timings = Vec::with_capacity(samples.len());
    let mut exact_rerank_timings = Vec::with_capacity(samples.len());
    for (query_index, sample) in samples.iter().enumerate() {
        if usize::try_from(sample.query_ordinal).ok() != Some(query_index)
            || sample.selected_pages.len() != crate::V26_SERVING_PAGE_BUDGET
            || sample
                .selected_pages
                .windows(2)
                .any(|pages| pages[0] >= pages[1])
            || sample.hits > sample.oracle_hits
            || !(1..=10).contains(&sample.oracle_hits)
            || sample.recall_ppm != u64::from(sample.hits) * 100_000
            || sample.oracle_attainment_ppm
                != u64::from(sample.hits) * 1_000_000 / u64::from(sample.oracle_hits)
            || sample.elapsed_ns == 0
            || sample.global_adc_elapsed_ns == 0
            || sample.exact_rerank_elapsed_ns == 0
            || sample
                .global_adc_elapsed_ns
                .checked_add(sample.exact_rerank_elapsed_ns)
                .is_none_or(|stages| stages > sample.elapsed_ns)
            || sample.exact_rows_read != 2_048
            || sample.cold_batches_read == 0
            || sample.cold_batches_read > 2_048
        {
            return Err(invalid("V26 global quality sample authority differs"));
        }
        total_hits += u64::from(sample.hits);
        total_oracle_hits += u64::from(sample.oracle_hits);
        timings.push(sample.elapsed_ns);
        global_adc_timings.push(sample.global_adc_elapsed_ns);
        exact_rerank_timings.push(sample.exact_rerank_elapsed_ns);
    }
    timings.sort_unstable();
    global_adc_timings.sort_unstable();
    exact_rerank_timings.sort_unstable();
    let percentile = |percent: usize| timings[(timings.len() * percent).div_ceil(100) - 1];
    let stage_percentile =
        |timings: &[u64], percent: usize| timings[(timings.len() * percent).div_ceil(100) - 1];
    let aggregate_recall_ppm = total_hits * 1_000_000 / 320;
    let minimum_query_recall_ppm = samples
        .iter()
        .map(|sample| sample.recall_ppm)
        .min()
        .unwrap();
    let oracle_attainment_ppm = total_hits * 1_000_000 / total_oracle_hits;
    let maximum_ns = *timings.last().unwrap();
    Ok(V26Pq16GlobalQualityResult {
        schema: "borsuk-v26-pq16-global-quality-result-v2".to_owned(),
        serving_manifest,
        external_queries,
        truth,
        evidence,
        query_count: 32,
        ranked_row_limit: 2_048,
        selected_page_count: u32::try_from(crate::V26_SERVING_PAGE_BUDGET).unwrap(),
        warmup_count: 2,
        measurement_count: 32,
        p50_ns: percentile(50),
        p95_ns: percentile(95),
        maximum_ns,
        global_adc_p50_ns: stage_percentile(&global_adc_timings, 50),
        global_adc_p95_ns: stage_percentile(&global_adc_timings, 95),
        global_adc_maximum_ns: *global_adc_timings.last().unwrap(),
        exact_rerank_p50_ns: stage_percentile(&exact_rerank_timings, 50),
        exact_rerank_p95_ns: stage_percentile(&exact_rerank_timings, 95),
        exact_rerank_maximum_ns: *exact_rerank_timings.last().unwrap(),
        fail_fast_gate_ns: 15_000_000,
        aggregate_recall_ppm,
        minimum_query_recall_ppm,
        oracle_attainment_ppm,
        aggregate_recall_gate_ppm: 975_000,
        minimum_query_recall_gate_ppm: 800_000,
        oracle_attainment_gate_ppm: 995_000,
        passed: aggregate_recall_ppm >= 975_000
            && minimum_query_recall_ppm >= 800_000
            && oracle_attainment_ppm >= 995_000
            && maximum_ns <= 15_000_000,
        page_body_reads: 0,
        claim_eligible: false,
    })
}

fn v26_pq16_global_quality_sample(
    query_ordinal: u32,
    selection: &V26Pq16ServingSelection,
    truth: &V26QueryTruth,
    elapsed_ns: u64,
    global_adc_elapsed_ns: u64,
    exact_rerank_elapsed_ns: u64,
) -> Result<V26Pq16GlobalQualitySample> {
    if truth.query_ordinal != query_ordinal
        || truth.neighbor_source_ordinals.len() != 10
        || truth.ground_truth_page_assignments.len() != 10
        || truth
            .ground_truth_page_assignments
            .iter()
            .any(|pages| pages.len() != 2 || pages[0] >= pages[1])
        || selection.selected_pages.len() != crate::V26_SERVING_PAGE_BUDGET
        || selection
            .selected_pages
            .windows(2)
            .any(|pages| pages[0] >= pages[1])
        || selection.exact_rows_read != 2_048
        || selection.cold_batches_read == 0
        || selection.cold_batches_read > 2_048
        || selection.cold_read_workers != 4
        || selection.page_body_reads != 0
        || elapsed_ns == 0
        || global_adc_elapsed_ns == 0
        || exact_rerank_elapsed_ns == 0
        || global_adc_elapsed_ns
            .checked_add(exact_rerank_elapsed_ns)
            .is_none_or(|stages| stages > elapsed_ns)
    {
        return Err(invalid("V26 global quality selection authority differs"));
    }
    let oracle_pages = exact_v26_layout_oracle_pages(
        &truth.ground_truth_page_assignments,
        crate::V26_SERVING_PAGE_BUDGET,
    )?;
    let hits = truth
        .ground_truth_page_assignments
        .iter()
        .filter(|pages| {
            pages
                .iter()
                .any(|page| selection.selected_pages.binary_search(page).is_ok())
        })
        .count() as u32;
    let oracle_hits = truth
        .ground_truth_page_assignments
        .iter()
        .filter(|pages| {
            pages
                .iter()
                .any(|page| oracle_pages.binary_search(page).is_ok())
        })
        .count() as u32;
    Ok(V26Pq16GlobalQualitySample {
        query_ordinal,
        selected_pages: selection.selected_pages.clone(),
        hits,
        oracle_hits,
        recall_ppm: u64::from(hits) * 100_000,
        oracle_attainment_ppm: u64::from(hits) * 1_000_000 / u64::from(oracle_hits),
        elapsed_ns,
        global_adc_elapsed_ns,
        exact_rerank_elapsed_ns,
        exact_rows_read: selection.exact_rows_read,
        cold_batches_read: selection.cold_batches_read,
    })
}

pub fn canonical_v26_pq16_global_quality_result_bytes(
    result: &V26Pq16GlobalQualityResult,
    samples: &[V26Pq16GlobalQualitySample],
) -> Result<Vec<u8>> {
    let expected = summarize_v26_pq16_global_quality(
        result.serving_manifest.clone(),
        result.external_queries.clone(),
        result.truth.clone(),
        result.evidence.clone(),
        samples,
    )?;
    if result != &expected {
        return Err(invalid("V26 global quality result differs"));
    }
    let value = serde_json::to_value(result)
        .map_err(|error| invalid(&format!("V26 global quality result failed: {error}")))?;
    let mut bytes = serde_json::to_vec(&canonical_json_value(value))
        .map_err(|error| invalid(&format!("V26 global quality result failed: {error}")))?;
    bytes.push(b'\n');
    Ok(bytes)
}

fn validate_v26_benchmark_identity(identity: &V26ObjectIdentity, role: &str) -> Result<()> {
    if identity.role != role
        || identity.digest_algorithm != "sha256"
        || !exact_lower_hex(&identity.digest, 64)
        || identity.encoded_bytes == 0
        || !identity.uri.starts_with("s3://")
    {
        return Err(invalid("V26 serving benchmark identity differs"));
    }
    Ok(())
}

fn summarize_v26_pq4_quality(
    pq4_manifest: V26ObjectIdentity,
    external_queries: V26ObjectIdentity,
    truth: V26ObjectIdentity,
    evidence: V26ObjectIdentity,
    samples: &[V26Pq4QualitySample],
) -> Result<V26Pq4QualityResult> {
    const DEPTHS: [u32; 4] = [512, 1_024, 2_048, 4_096];
    const QUERY_COUNT: usize = 32;
    let identities = [
        (&pq4_manifest, "pq4-fast-manifest"),
        (&external_queries, "external-queries-parquet"),
        (&truth, "truth-parquet"),
        (&evidence, "pq4-fast-quality-evidence-parquet"),
    ];
    let generation = &pq4_manifest.generation;
    let mut uris = BTreeSet::new();
    for (identity, role) in identities {
        validate_v26_benchmark_identity(identity, role)?;
        if identity.generation != *generation || !uris.insert(&identity.uri) {
            return Err(invalid("V26 PQ4 quality identity differs"));
        }
    }
    if samples.len() != DEPTHS.len() * QUERY_COUNT {
        return Err(invalid("V26 PQ4 quality sample inventory differs"));
    }
    let mut arms = Vec::with_capacity(DEPTHS.len());
    for (arm_index, depth) in DEPTHS.into_iter().enumerate() {
        let arm = &samples[arm_index * QUERY_COUNT..(arm_index + 1) * QUERY_COUNT];
        let mut total_hits = 0_u64;
        let mut total_oracle_hits = 0_u64;
        let mut minimum_recall = u64::MAX;
        let mut maximum_scan = 0_u64;
        let mut maximum_rerank = 0_u64;
        let mut maximum_saturation = 0_u32;
        let mut maximum_error = 0.0_f32;
        for (query_index, sample) in arm.iter().enumerate() {
            let scale = f32::from_bits(sample.quantization_scale_bits);
            let error = f32::from_bits(sample.maximum_distance_error_bits);
            if sample.ranked_row_limit != depth
                || sample.query_ordinal as usize != query_index
                || sample.selected_pages.len() != crate::V26_SERVING_PAGE_BUDGET
                || sample
                    .selected_pages
                    .windows(2)
                    .any(|pair| pair[0] >= pair[1])
                || sample.hits > sample.oracle_hits
                || !(1..=10).contains(&sample.oracle_hits)
                || sample.recall_ppm != u64::from(sample.hits) * 100_000
                || sample.oracle_attainment_ppm
                    != u64::from(sample.hits) * 1_000_000 / u64::from(sample.oracle_hits)
                || sample.scan_elapsed_ns == 0
                || sample.exact_rerank_elapsed_ns == 0
                || !scale.is_finite()
                || scale <= 0.0
                || !error.is_finite()
                || error < 0.0
                || sample.saturation_count > 512
                || sample.page_body_reads != 0
            {
                return Err(invalid("V26 PQ4 quality sample differs"));
            }
            total_hits += u64::from(sample.hits);
            total_oracle_hits += u64::from(sample.oracle_hits);
            minimum_recall = minimum_recall.min(sample.recall_ppm);
            maximum_scan = maximum_scan.max(sample.scan_elapsed_ns);
            maximum_rerank = maximum_rerank.max(sample.exact_rerank_elapsed_ns);
            maximum_saturation = maximum_saturation.max(sample.saturation_count);
            maximum_error = maximum_error.max(error);
        }
        let aggregate = total_hits * 1_000_000 / 320;
        let oracle = total_hits * 1_000_000 / total_oracle_hits;
        arms.push(V26Pq4QualityArmResult {
            ranked_row_limit: depth,
            aggregate_recall_ppm: aggregate,
            minimum_query_recall_ppm: minimum_recall,
            oracle_attainment_ppm: oracle,
            maximum_scan_elapsed_ns: maximum_scan,
            maximum_exact_rerank_elapsed_ns: maximum_rerank,
            maximum_saturation_count: maximum_saturation,
            maximum_distance_error_bits: maximum_error.to_bits(),
            passed: aggregate >= 975_000 && minimum_recall >= 800_000 && oracle >= 995_000,
        });
    }
    let smallest_passing_ranked_row_limit = arms
        .iter()
        .find(|arm| arm.passed)
        .map(|arm| arm.ranked_row_limit);
    Ok(V26Pq4QualityResult {
        schema: "borsuk-v26-pq4-fast-quality-result-v1".to_owned(),
        pq4_manifest,
        external_queries,
        truth,
        evidence,
        backend: "aarch64-neon-table".to_owned(),
        query_count: QUERY_COUNT as u32,
        candidate_depths: DEPTHS.to_vec(),
        selected_page_count: crate::V26_SERVING_PAGE_BUDGET as u32,
        aggregate_recall_gate_ppm: 975_000,
        minimum_query_recall_gate_ppm: 800_000,
        oracle_attainment_gate_ppm: 995_000,
        arms,
        smallest_passing_ranked_row_limit,
        page_body_reads: 0,
        claim_eligible: false,
    })
}

pub fn canonical_v26_pq4_quality_result_bytes(
    result: &V26Pq4QualityResult,
    samples: &[V26Pq4QualitySample],
) -> Result<Vec<u8>> {
    let expected = summarize_v26_pq4_quality(
        result.pq4_manifest.clone(),
        result.external_queries.clone(),
        result.truth.clone(),
        result.evidence.clone(),
        samples,
    )?;
    if result != &expected {
        return Err(invalid("V26 PQ4 quality result differs"));
    }
    let value = serde_json::to_value(result)
        .map_err(|error| invalid(&format!("V26 PQ4 quality result failed: {error}")))?;
    let mut bytes = serde_json::to_vec(&canonical_json_value(value))
        .map_err(|error| invalid(&format!("V26 PQ4 quality result failed: {error}")))?;
    bytes.push(b'\n');
    Ok(bytes)
}

fn v26_pq4_quality_schema() -> Schema {
    Schema::new(vec![
        Field::new("ranked_row_limit", DataType::UInt32, false),
        Field::new("query_ordinal", DataType::UInt32, false),
        Field::new(
            "selected_pages",
            DataType::FixedSizeList(Arc::new(Field::new("element", DataType::UInt32, false)), 10),
            false,
        ),
        Field::new("hits", DataType::UInt32, false),
        Field::new("oracle_hits", DataType::UInt32, false),
        Field::new("recall_ppm", DataType::UInt64, false),
        Field::new("oracle_attainment_ppm", DataType::UInt64, false),
        Field::new("scan_elapsed_ns", DataType::UInt64, false),
        Field::new("exact_rerank_elapsed_ns", DataType::UInt64, false),
        Field::new("quantization_scale_bits", DataType::UInt32, false),
        Field::new("saturation_count", DataType::UInt32, false),
        Field::new("maximum_distance_error_bits", DataType::UInt32, false),
        Field::new("page_body_reads", DataType::UInt32, false),
    ])
}

fn v26_pq4_quality_batch(samples: &[V26Pq4QualitySample]) -> Result<RecordBatch> {
    let selected_pages = samples
        .iter()
        .flat_map(|sample| sample.selected_pages.iter().copied())
        .collect::<Vec<_>>();
    let pages = FixedSizeListArray::try_new(
        Arc::new(Field::new("element", DataType::UInt32, false)),
        10,
        Arc::new(UInt32Array::from(selected_pages)),
        None,
    )
    .map_err(|error| invalid(&format!("V26 PQ4 quality pages failed: {error}")))?;
    let u32s = |values: Vec<u32>| Arc::new(UInt32Array::from(values)) as ArrayRef;
    let u64s = |values: Vec<u64>| Arc::new(UInt64Array::from(values)) as ArrayRef;
    RecordBatch::try_new(
        Arc::new(v26_pq4_quality_schema()),
        vec![
            u32s(samples.iter().map(|value| value.ranked_row_limit).collect()),
            u32s(samples.iter().map(|value| value.query_ordinal).collect()),
            Arc::new(pages),
            u32s(samples.iter().map(|value| value.hits).collect()),
            u32s(samples.iter().map(|value| value.oracle_hits).collect()),
            u64s(samples.iter().map(|value| value.recall_ppm).collect()),
            u64s(
                samples
                    .iter()
                    .map(|value| value.oracle_attainment_ppm)
                    .collect(),
            ),
            u64s(samples.iter().map(|value| value.scan_elapsed_ns).collect()),
            u64s(
                samples
                    .iter()
                    .map(|value| value.exact_rerank_elapsed_ns)
                    .collect(),
            ),
            u32s(
                samples
                    .iter()
                    .map(|value| value.quantization_scale_bits)
                    .collect(),
            ),
            u32s(samples.iter().map(|value| value.saturation_count).collect()),
            u32s(
                samples
                    .iter()
                    .map(|value| value.maximum_distance_error_bits)
                    .collect(),
            ),
            u32s(samples.iter().map(|value| value.page_body_reads).collect()),
        ],
    )
    .map_err(|error| invalid(&format!("V26 PQ4 quality batch failed: {error}")))
}

fn summarize_v26_pq16_serving_benchmark(
    serving_manifest: V26ObjectIdentity,
    external_queries: V26ObjectIdentity,
    latency_evidence: V26ObjectIdentity,
    samples: &[V26ServingLatencySample],
) -> Result<V26Pq16ServingBenchmarkResult> {
    validate_v26_benchmark_identity(&serving_manifest, "pq16-serving-manifest")?;
    validate_v26_benchmark_identity(&external_queries, "external-queries-parquet")?;
    validate_v26_benchmark_identity(&latency_evidence, "pq16-serving-latency-parquet")?;
    if serving_manifest.generation != external_queries.generation
        || serving_manifest.generation != latency_evidence.generation
        || samples.len() != 10_000
        || samples.iter().enumerate().any(|(ordinal, sample)| {
            usize::try_from(sample.sample_ordinal).ok() != Some(ordinal)
                || usize::try_from(sample.query_ordinal).ok() != Some(ordinal % 512)
                || sample.elapsed_ns == 0
                || !(1..=512).contains(&sample.cold_batches_read)
        })
    {
        return Err(invalid("V26 serving benchmark sample authority differs"));
    }
    let mut timings = samples
        .iter()
        .map(|sample| sample.elapsed_ns)
        .collect::<Vec<_>>();
    timings.sort_unstable();
    let percentile = |percent: usize| timings[timings.len() * percent / 100 - 1];
    let p99_ns = percentile(99);
    Ok(V26Pq16ServingBenchmarkResult {
        schema: "borsuk-v26-pq16-serving-benchmark-result-v1".to_owned(),
        serving_manifest,
        external_queries,
        latency_evidence,
        query_count: 512,
        candidate_page_limit: 128,
        ranked_row_limit: 512,
        selected_page_count: u32::try_from(crate::V26_SERVING_PAGE_BUDGET).unwrap(),
        warmup_count: 1_024,
        measurement_count: 10_000,
        p50_ns: percentile(50),
        p95_ns: percentile(95),
        p99_ns,
        maximum_ns: *timings.last().unwrap(),
        p99_gate_ns: 15_000_000,
        passed: p99_ns <= 15_000_000,
        page_body_reads: 0,
        claim_eligible: false,
    })
}

pub fn canonical_v26_pq16_serving_benchmark_result_bytes(
    result: &V26Pq16ServingBenchmarkResult,
    samples: &[V26ServingLatencySample],
) -> Result<Vec<u8>> {
    let expected = summarize_v26_pq16_serving_benchmark(
        result.serving_manifest.clone(),
        result.external_queries.clone(),
        result.latency_evidence.clone(),
        samples,
    )?;
    if result != &expected {
        return Err(invalid("V26 serving benchmark result differs"));
    }
    let value = serde_json::to_value(result)
        .map_err(|error| invalid(&format!("V26 serving benchmark result failed: {error}")))?;
    let mut bytes = serde_json::to_vec(&canonical_json_value(value))
        .map_err(|error| invalid(&format!("V26 serving benchmark result failed: {error}")))?;
    bytes.push(b'\n');
    Ok(bytes)
}

fn v26_serving_latency_schema() -> Schema {
    Schema::new(vec![
        Field::new("sample_ordinal", DataType::UInt32, false),
        Field::new("query_ordinal", DataType::UInt32, false),
        Field::new("elapsed_ns", DataType::UInt64, false),
        Field::new("cold_batches_read", DataType::UInt32, false),
    ])
}

fn v26_serving_latency_batch(samples: &[V26ServingLatencySample]) -> Result<RecordBatch> {
    if samples.len() != 10_000 {
        return Err(invalid("V26 serving latency inventory differs"));
    }
    RecordBatch::try_new(
        Arc::new(v26_serving_latency_schema()),
        vec![
            Arc::new(UInt32Array::from_iter_values(
                samples.iter().map(|sample| sample.sample_ordinal),
            )),
            Arc::new(UInt32Array::from_iter_values(
                samples.iter().map(|sample| sample.query_ordinal),
            )),
            Arc::new(UInt64Array::from_iter_values(
                samples.iter().map(|sample| sample.elapsed_ns),
            )),
            Arc::new(UInt32Array::from_iter_values(
                samples.iter().map(|sample| sample.cold_batches_read),
            )),
        ],
    )
    .map_err(|error| invalid(&format!("V26 serving latency batch failed: {error}")))
}

fn v26_global_preflight_latency_batch(samples: &[V26ServingLatencySample]) -> Result<RecordBatch> {
    if samples.len() != 32 {
        return Err(invalid("V26 global preflight latency inventory differs"));
    }
    RecordBatch::try_new(
        Arc::new(v26_serving_latency_schema()),
        vec![
            Arc::new(UInt32Array::from_iter_values(
                samples.iter().map(|sample| sample.sample_ordinal),
            )),
            Arc::new(UInt32Array::from_iter_values(
                samples.iter().map(|sample| sample.query_ordinal),
            )),
            Arc::new(UInt64Array::from_iter_values(
                samples.iter().map(|sample| sample.elapsed_ns),
            )),
            Arc::new(UInt32Array::from_iter_values(
                samples.iter().map(|sample| sample.cold_batches_read),
            )),
        ],
    )
    .map_err(|error| {
        invalid(&format!(
            "V26 global preflight latency batch failed: {error}"
        ))
    })
}

fn v26_pq16_global_quality_schema() -> Schema {
    Schema::new(vec![
        Field::new("query_ordinal", DataType::UInt32, false),
        Field::new(
            "selected_pages",
            DataType::FixedSizeList(Arc::new(Field::new("element", DataType::UInt32, false)), 10),
            false,
        ),
        Field::new("hits", DataType::UInt32, false),
        Field::new("oracle_hits", DataType::UInt32, false),
        Field::new("recall_ppm", DataType::UInt64, false),
        Field::new("oracle_attainment_ppm", DataType::UInt64, false),
        Field::new("elapsed_ns", DataType::UInt64, false),
        Field::new("global_adc_elapsed_ns", DataType::UInt64, false),
        Field::new("exact_rerank_elapsed_ns", DataType::UInt64, false),
        Field::new("exact_rows_read", DataType::UInt32, false),
        Field::new("cold_batches_read", DataType::UInt32, false),
    ])
}

fn v26_pq16_global_quality_batch(samples: &[V26Pq16GlobalQualitySample]) -> Result<RecordBatch> {
    if samples.len() != 32 {
        return Err(invalid("V26 global quality evidence inventory differs"));
    }
    let selected_pages = FixedSizeListArray::try_new(
        Arc::new(Field::new("element", DataType::UInt32, false)),
        10,
        Arc::new(UInt32Array::from_iter_values(
            samples
                .iter()
                .flat_map(|sample| sample.selected_pages.iter().copied()),
        )),
        None,
    )
    .map_err(|error| invalid(&format!("V26 global quality pages failed: {error}")))?;
    RecordBatch::try_new(
        Arc::new(v26_pq16_global_quality_schema()),
        vec![
            Arc::new(UInt32Array::from_iter_values(
                samples.iter().map(|sample| sample.query_ordinal),
            )),
            Arc::new(selected_pages),
            Arc::new(UInt32Array::from_iter_values(
                samples.iter().map(|sample| sample.hits),
            )),
            Arc::new(UInt32Array::from_iter_values(
                samples.iter().map(|sample| sample.oracle_hits),
            )),
            Arc::new(UInt64Array::from_iter_values(
                samples.iter().map(|sample| sample.recall_ppm),
            )),
            Arc::new(UInt64Array::from_iter_values(
                samples.iter().map(|sample| sample.oracle_attainment_ppm),
            )),
            Arc::new(UInt64Array::from_iter_values(
                samples.iter().map(|sample| sample.elapsed_ns),
            )),
            Arc::new(UInt64Array::from_iter_values(
                samples.iter().map(|sample| sample.global_adc_elapsed_ns),
            )),
            Arc::new(UInt64Array::from_iter_values(
                samples.iter().map(|sample| sample.exact_rerank_elapsed_ns),
            )),
            Arc::new(UInt32Array::from_iter_values(
                samples.iter().map(|sample| sample.exact_rows_read),
            )),
            Arc::new(UInt32Array::from_iter_values(
                samples.iter().map(|sample| sample.cold_batches_read),
            )),
        ],
    )
    .map_err(|error| invalid(&format!("V26 global quality batch failed: {error}")))
}

pub fn run_v26_pq16_global_preflight(request: &V26Pq16GlobalPreflightRequest) -> Result<Vec<u8>> {
    if request.latency_output_path.exists()
        || !request.latency_output_uri.starts_with("s3://")
        || !request.latency_output_uri.ends_with(".parquet")
        || request.latency_output_uri == request.runtime.serving_manifest.identity.uri
        || request.latency_output_uri == request.runtime.external_queries.identity.uri
    {
        return Err(invalid("V26 global preflight request differs"));
    }
    let runtime = open_v26_pq16_serving_runtime(&request.runtime)?;
    if runtime.query_count() != 512 {
        return Err(invalid("V26 global preflight query inventory differs"));
    }
    for query_ordinal in 0_u32..2 {
        let selection = runtime.select_global(query_ordinal)?;
        if selection.selected_pages.len() != crate::V26_SERVING_PAGE_BUDGET
            || selection.exact_rows_read != 2_048
            || selection.cold_batches_read == 0
            || selection.page_body_reads != 0
        {
            return Err(invalid("V26 global preflight warmup differs"));
        }
    }
    let mut samples = Vec::with_capacity(32);
    for query_ordinal in 0_u32..32 {
        let started = std::time::Instant::now();
        let selection = runtime.select_global(query_ordinal)?;
        let elapsed_ns = u64::try_from(started.elapsed().as_nanos())
            .map_err(|_| invalid("V26 global preflight latency overflows"))?
            .max(1);
        if selection.selected_pages.len() != crate::V26_SERVING_PAGE_BUDGET
            || selection.exact_rows_read != 2_048
            || selection.cold_batches_read == 0
            || selection.page_body_reads != 0
        {
            return Err(invalid("V26 global preflight selection differs"));
        }
        samples.push(V26ServingLatencySample {
            sample_ordinal: query_ordinal,
            query_ordinal,
            elapsed_ns,
            cold_batches_read: selection.cold_batches_read,
        });
    }
    let result = (|| {
        write_batch(
            &request.latency_output_path,
            v26_global_preflight_latency_batch(&samples)?,
        )?;
        let (encoded_bytes, digest) = sha256_file(&request.latency_output_path)?;
        let evidence = V26ObjectIdentity {
            role: "pq16-global-preflight-latency-parquet".to_owned(),
            uri: request.latency_output_uri.clone(),
            digest_algorithm: "sha256".to_owned(),
            digest,
            encoded_bytes,
            generation: request.runtime.serving_manifest.identity.generation.clone(),
        };
        let result = summarize_v26_pq16_global_preflight(
            request.runtime.serving_manifest.identity.clone(),
            request.runtime.external_queries.identity.clone(),
            evidence,
            &samples,
        )?;
        canonical_v26_pq16_global_preflight_result_bytes(&result, &samples)
    })();
    if result.is_err() {
        let _ = fs::remove_file(&request.latency_output_path);
    }
    result
}

pub fn run_v26_pq16_global_quality_preflight(
    request: &V26Pq16GlobalQualityRequest,
) -> Result<Vec<u8>> {
    if request.evidence_output_path.exists()
        || !request.evidence_output_uri.starts_with("s3://")
        || !request.evidence_output_uri.ends_with(".parquet")
    {
        return Err(invalid("V26 global quality request differs"));
    }
    let manifest = read_v26_pq16_serving_manifest(&request.serving_manifest)?;
    let terminal = read_layout_terminal(&request.layout_terminal)?;
    authenticate(&request.external_queries, "external-queries-parquet")?;
    authenticate(&request.truth, "truth-parquet")?;
    let generation = &terminal.authority.generation;
    let mut uris = BTreeSet::new();
    if manifest.inputs[0] != terminal.authority.construction_rows
        || manifest.inputs[2] != request.layout_terminal.identity
        || manifest.row_count != terminal.row_count
        || manifest.page_count != terminal.page_count
        || [
            &request.serving_manifest.identity,
            &request.layout_terminal.identity,
            &request.external_queries.identity,
            &request.truth.identity,
        ]
        .iter()
        .any(|identity| identity.generation != *generation || !uris.insert(&identity.uri))
        || !uris.insert(&request.evidence_output_uri)
    {
        return Err(invalid("V26 global quality authority differs"));
    }
    let expected_names = v26_pq16_serving_output_names()
        .into_iter()
        .chain(std::iter::once("serving-manifest.json"))
        .map(str::to_owned)
        .collect::<BTreeSet<_>>();
    let observed_names = fs::read_dir(&request.serving_dir)
        .map_err(|error| {
            invalid(&format!(
                "V26 global quality directory read failed: {error}"
            ))
        })?
        .map(|entry| {
            entry
                .map_err(|error| {
                    invalid(&format!(
                        "V26 global quality directory read failed: {error}"
                    ))
                })
                .and_then(|entry| {
                    entry
                        .file_name()
                        .into_string()
                        .map_err(|_| invalid("V26 global quality artifact name differs"))
                })
        })
        .collect::<Result<BTreeSet<_>>>()?;
    if observed_names != expected_names {
        return Err(invalid("V26 global quality artifact inventory differs"));
    }
    let index = read_v26_pq16_index_arrow(&request.serving_dir, &manifest.index)?;
    let cold_vectors = V26ArrowColdVectors::open(
        &request.serving_dir.join("cold-vectors.arrow"),
        &manifest.cold_vectors,
    )?;
    let mut queries = read_evaluation_queries(&request.external_queries.path, 512)?;
    let mut truths = read_evaluation_truth_with_assignment(
        &request.truth.path,
        512,
        &queries,
        &terminal.authority.construction_rows.digest,
        &request.external_queries.identity.digest,
        |neighbor| {
            let source = u32::try_from(neighbor)
                .map_err(|_| invalid("V26 global quality truth source differs"))?;
            cold_vectors.read_assignment(source)
        },
    )?;
    queries.truncate(32);
    truths.truncate(32);
    for query in queries.iter().take(2) {
        select_v26_pq16_global_pages_from_arrow(&index, &query.vector, &cold_vectors, 2_048)?;
    }
    let mut samples = Vec::with_capacity(32);
    for (query, truth) in queries.iter().zip(&truths) {
        let started = std::time::Instant::now();
        let timed = select_v26_pq16_global_pages_from_arrow_timed(
            &index,
            &query.vector,
            &cold_vectors,
            2_048,
        )?;
        let elapsed_ns = u64::try_from(started.elapsed().as_nanos())
            .map_err(|_| invalid("V26 global quality latency overflows"))?
            .max(1);
        samples.push(v26_pq16_global_quality_sample(
            query.query_ordinal,
            &timed.selection,
            truth,
            elapsed_ns,
            timed.global_adc_elapsed_ns,
            timed.exact_rerank_elapsed_ns,
        )?);
    }
    let result = (|| {
        write_batch(
            &request.evidence_output_path,
            v26_pq16_global_quality_batch(&samples)?,
        )?;
        let evidence = output_identity(
            "pq16-global-preflight-evidence-parquet",
            &request.evidence_output_path,
            &request.evidence_output_uri[..request.evidence_output_uri.rfind('/').unwrap() + 1],
            generation,
        )?;
        if evidence.uri != request.evidence_output_uri {
            return Err(invalid("V26 global quality evidence URI differs"));
        }
        let result = summarize_v26_pq16_global_quality(
            request.serving_manifest.identity.clone(),
            request.external_queries.identity.clone(),
            request.truth.identity.clone(),
            evidence,
            &samples,
        )?;
        canonical_v26_pq16_global_quality_result_bytes(&result, &samples)
    })();
    if result.is_err() {
        let _ = fs::remove_file(&request.evidence_output_path);
    }
    result
}

fn require_v26_serving_selection(selection: &V26Pq16ServingSelection) -> Result<()> {
    if selection.selected_pages.len() != crate::V26_SERVING_PAGE_BUDGET
        || selection
            .selected_pages
            .windows(2)
            .any(|pair| pair[0] >= pair[1])
        || selection.exact_rows_read != 512
        || selection.cold_batches_read == 0
        || selection.cold_batches_read > 512
        || selection.cold_read_workers != 4
        || selection.page_body_reads != 0
    {
        return Err(invalid("V26 serving selection contract differs"));
    }
    Ok(())
}

pub fn run_v26_pq16_serving_benchmark(request: &V26Pq16ServingBenchmarkRequest) -> Result<Vec<u8>> {
    if request.latency_output_path.exists()
        || !request.latency_output_uri.starts_with("s3://")
        || !request.latency_output_uri.ends_with(".parquet")
        || request.latency_output_uri == request.runtime.serving_manifest.identity.uri
        || request.latency_output_uri == request.runtime.external_queries.identity.uri
    {
        return Err(invalid("V26 serving benchmark request differs"));
    }
    let runtime = open_v26_pq16_serving_runtime(&request.runtime)?;
    if runtime.index.page_offsets.len() - 1 < 128 || runtime.query_count() != 512 {
        return Err(invalid("V26 serving benchmark inventory differs"));
    }
    for ordinal in 0_u32..1_024 {
        let selection = runtime.select(ordinal % 512)?;
        require_v26_serving_selection(&selection)?;
    }
    let mut samples = Vec::with_capacity(10_000);
    for sample_ordinal in 0_u32..10_000 {
        let query_ordinal = sample_ordinal % 512;
        let started = std::time::Instant::now();
        let selection = runtime.select(query_ordinal)?;
        let elapsed_ns = u64::try_from(started.elapsed().as_nanos())
            .map_err(|_| invalid("V26 serving latency overflows"))?
            .max(1);
        require_v26_serving_selection(&selection)?;
        samples.push(V26ServingLatencySample {
            sample_ordinal,
            query_ordinal,
            elapsed_ns,
            cold_batches_read: selection.cold_batches_read,
        });
    }
    let result = (|| {
        write_batch(
            &request.latency_output_path,
            v26_serving_latency_batch(&samples)?,
        )?;
        let (encoded_bytes, digest) = sha256_file(&request.latency_output_path)?;
        let evidence = V26ObjectIdentity {
            role: "pq16-serving-latency-parquet".to_owned(),
            uri: request.latency_output_uri.clone(),
            digest_algorithm: "sha256".to_owned(),
            digest,
            encoded_bytes,
            generation: request.runtime.serving_manifest.identity.generation.clone(),
        };
        let result = summarize_v26_pq16_serving_benchmark(
            request.runtime.serving_manifest.identity.clone(),
            request.runtime.external_queries.identity.clone(),
            evidence,
            &samples,
        )?;
        canonical_v26_pq16_serving_benchmark_result_bytes(&result, &samples)
    })();
    if result.is_err() {
        let _ = fs::remove_file(&request.latency_output_path);
    }
    result
}

impl V26Pq16ServingRuntime {
    pub fn query_count(&self) -> usize {
        self.queries.len()
    }

    pub fn page_body_reads(&self) -> u32 {
        0
    }

    pub fn select(&self, query_ordinal: u32) -> Result<V26Pq16ServingSelection> {
        let query = self
            .queries
            .get(
                usize::try_from(query_ordinal)
                    .map_err(|_| invalid("V26 serving query ordinal overflows"))?,
            )
            .ok_or_else(|| invalid("V26 serving query ordinal differs"))?;
        if query.query_ordinal != query_ordinal {
            return Err(invalid("V26 serving query inventory differs"));
        }
        let candidate_count = (self.index.page_offsets.len() - 1).min(128);
        let mut candidate_pages = rank_v26_tree_page_prefix(
            &self.primary,
            &self.replica,
            &query.vector,
            candidate_count,
        )?;
        candidate_pages.sort_unstable();
        select_v26_pq16_pages_from_arrow(
            &self.index,
            &candidate_pages,
            &query.vector,
            &self.cold_vectors,
        )
    }

    pub fn select_global(&self, query_ordinal: u32) -> Result<V26Pq16ServingSelection> {
        let query = self
            .queries
            .get(
                usize::try_from(query_ordinal)
                    .map_err(|_| invalid("V26 global preflight query ordinal overflows"))?,
            )
            .ok_or_else(|| invalid("V26 global preflight query ordinal differs"))?;
        if query.query_ordinal != query_ordinal {
            return Err(invalid("V26 global preflight query inventory differs"));
        }
        select_v26_pq16_global_pages_from_arrow(
            &self.index,
            &query.vector,
            &self.cold_vectors,
            2_048,
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct V26TruthBuildRequest {
    pub construction_rows: V26LocalObjectPath,
    pub external_queries: V26LocalObjectPath,
    pub expected_rows: u64,
    pub expected_queries: u32,
    pub output_path: PathBuf,
    pub output_uri: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct V26LayoutBuildOutput {
    pub authority: V26LayoutAuthority,
    pub inputs: Vec<V26ObjectIdentity>,
    pub outputs: Vec<V26ObjectIdentity>,
    pub row_count: u64,
    pub leaves_per_tree: u32,
    pub page_count: u32,
    pub projection_steps: u64,
    pub worker_count: u32,
}

fn sha256_file(path: &Path) -> Result<(u64, String)> {
    let mut file = fs::File::open(path)
        .map_err(|error| invalid(&format!("V26 local object open failed: {error}")))?;
    let encoded_bytes = file
        .metadata()
        .map_err(|error| invalid(&format!("V26 local object metadata failed: {error}")))?
        .len();
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file
            .read(&mut buffer)
            .map_err(|error| invalid(&format!("V26 local object hash failed: {error}")))?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok((encoded_bytes, format!("{:x}", hasher.finalize())))
}

fn authenticate(object: &V26LocalObjectPath, role: &str) -> Result<()> {
    if object.identity.role != role
        || object.identity.digest_algorithm != "sha256"
        || !exact_lower_hex(&object.identity.digest, 64)
        || object.identity.encoded_bytes == 0
        || !object.identity.uri.starts_with("s3://")
    {
        return Err(invalid("V26 local object identity differs"));
    }
    let (encoded_bytes, digest) = sha256_file(&object.path)?;
    if encoded_bytes != object.identity.encoded_bytes || digest != object.identity.digest {
        return Err(invalid("V26 local object bytes differ"));
    }
    Ok(())
}

fn read_manifest(object: &V26LocalObjectPath) -> Result<V26LayoutAuthority> {
    authenticate(object, "layout-manifest")?;
    let bytes = fs::read(&object.path)
        .map_err(|error| invalid(&format!("V26 layout manifest read failed: {error}")))?;
    if bytes.last() != Some(&b'\n') || bytes[..bytes.len() - 1].contains(&b'\n') {
        return Err(invalid("V26 layout manifest bytes differ"));
    }
    let authority: V26LayoutAuthority = serde_json::from_slice(&bytes)
        .map_err(|error| invalid(&format!("V26 layout manifest parse failed: {error}")))?;
    let mut expected = serde_json::to_vec(&canonical_json_value(
        serde_json::to_value(&authority)
            .map_err(|error| invalid(&format!("V26 layout manifest failed: {error}")))?,
    ))
    .map_err(|error| invalid(&format!("V26 layout manifest failed: {error}")))?;
    expected.push(b'\n');
    if bytes != expected || object.identity.generation != authority.generation {
        return Err(invalid("V26 layout manifest authority differs"));
    }
    validate_layout_authority(&authority)?;
    Ok(authority)
}

fn read_layout_terminal(object: &V26LocalObjectPath) -> Result<V26LayoutReceipt> {
    authenticate(object, "layout-terminal")?;
    let bytes = fs::read(&object.path)
        .map_err(|error| invalid(&format!("V26 layout terminal read failed: {error}")))?;
    let receipt: V26LayoutReceipt = serde_json::from_slice(&bytes)
        .map_err(|error| invalid(&format!("V26 layout terminal parse failed: {error}")))?;
    if canonical_v26_layout_receipt_bytes(&receipt)? != bytes
        || object.identity.generation != receipt.authority.generation
    {
        return Err(invalid("V26 layout terminal authority differs"));
    }
    Ok(receipt)
}

pub fn evaluate_v26_layout_oracle(
    request: &V26LayoutEvaluationRequest,
) -> Result<(Vec<V26QueryTruth>, Vec<V26LayoutSample>, V26LayoutResult)> {
    evaluate_v26_layout_oracle_with_page_budget(request, 8)
}

fn evaluate_v26_layout_oracle_with_page_budget(
    request: &V26LayoutEvaluationRequest,
    page_budget: usize,
) -> Result<(Vec<V26QueryTruth>, Vec<V26LayoutSample>, V26LayoutResult)> {
    if page_budget == 0 {
        return Err(invalid("V26 layout page budget differs"));
    }
    let terminal = read_layout_terminal(&request.layout_terminal)?;
    if request.expected_queries != 512
        || !terminal
            .outputs
            .iter()
            .any(|identity| identity == &request.page_assignments.identity)
    {
        return Err(invalid("V26 layout evaluation authority differs"));
    }
    authenticate(&request.page_assignments, "page-assignments-parquet")?;
    authenticate(&request.external_queries, "external-queries-parquet")?;
    authenticate(&request.truth, "truth-parquet")?;
    if [
        &request.page_assignments.identity,
        &request.external_queries.identity,
        &request.truth.identity,
    ]
    .iter()
    .any(|identity| identity.generation != terminal.authority.generation)
    {
        return Err(invalid("V26 layout evaluation generation differs"));
    }
    let assignment_rows = i64::try_from(terminal.row_count)
        .map_err(|_| invalid("V26 assignment row count overflows"))?;
    let assignments = read_assignments(&request.page_assignments.path, assignment_rows)?;
    if assignments
        .iter()
        .enumerate()
        .any(|(ordinal, row)| usize::try_from(row.source_ordinal).ok() != Some(ordinal))
    {
        return Err(invalid("V26 assignment inventory differs"));
    }
    let queries =
        read_evaluation_queries(&request.external_queries.path, request.expected_queries)?;
    let truths = read_evaluation_truth(
        &request.truth.path,
        request.expected_queries,
        &queries,
        &assignments,
        &terminal.authority.construction_rows.digest,
        &request.external_queries.identity.digest,
    )?;
    let samples = truths
        .iter()
        .map(|truth| {
            let selected_pages =
                exact_v26_layout_oracle_pages(&truth.ground_truth_page_assignments, page_budget)?;
            let hits = truth
                .ground_truth_page_assignments
                .iter()
                .filter(|pages| {
                    pages
                        .iter()
                        .any(|page| selected_pages.binary_search(page).is_ok())
                })
                .count() as u32;
            Ok(V26LayoutSample {
                query_ordinal: truth.query_ordinal,
                selected_pages,
                hits,
                recall_ppm: u64::from(hits) * 100_000,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let total_hits = samples
        .iter()
        .try_fold(0_u64, |sum, sample| sum.checked_add(u64::from(sample.hits)))
        .ok_or_else(|| invalid("V26 metric arithmetic differs"))?;
    let aggregate_recall_ppm = total_hits
        .checked_mul(1_000_000)
        .and_then(|value| value.checked_div(u64::from(request.expected_queries) * 10))
        .ok_or_else(|| invalid("V26 metric arithmetic differs"))?;
    let minimum_query_recall_ppm = samples
        .iter()
        .map(|sample| sample.recall_ppm)
        .min()
        .ok_or_else(|| invalid("V26 layout samples are absent"))?;
    let result = V26LayoutResult {
        schema: "borsuk-v26-layout-result-v1".to_owned(),
        query_count: request.expected_queries,
        aggregate_recall_ppm,
        minimum_query_recall_ppm,
        disposition: if aggregate_recall_ppm >= 995_000 && minimum_query_recall_ppm >= 800_000 {
            V26Disposition::BoundedLayoutCandidate
        } else {
            V26Disposition::LayoutRejected
        },
        page_body_reads: 0,
        claim_eligible: false,
    };
    canonical_v26_layout_result_bytes_with_page_budget(&result, &truths, &samples, page_budget)?;
    Ok((truths, samples, result))
}

fn read_evaluation_queries(path: &Path, expected_queries: u32) -> Result<Vec<V26ExternalQuery>> {
    let reader = open_reader(path)?;
    if reader.schema().as_ref() != &v26_query_schema()
        || reader.metadata().file_metadata().num_rows() != 10_000
        || expected_queries != 512
    {
        return Err(invalid("V26 query Parquet authority differs"));
    }
    let mut queries = Vec::with_capacity(expected_queries as usize);
    for batch in reader
        .build()
        .map_err(|error| invalid(&format!("V26 query reader failed: {error}")))?
    {
        let batch = batch.map_err(|error| invalid(&format!("V26 query batch failed: {error}")))?;
        if batch
            .columns()
            .iter()
            .any(|column| column.null_count() != 0)
        {
            return Err(invalid("V26 query nullability differs"));
        }
        let vectors = batch
            .column(0)
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .ok_or_else(|| invalid("V26 query vector differs"))?;
        for row in 0..batch.num_rows() {
            if queries.len() == expected_queries as usize {
                break;
            }
            let vector = vectors.value(row);
            let values = vector
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| invalid("V26 query vector child differs"))?;
            let norm = values
                .values()
                .iter()
                .map(|value| value * value)
                .sum::<f32>();
            let query_ordinal =
                u32::try_from(queries.len()).map_err(|_| invalid("V26 query ordinal overflows"))?;
            if usize::try_from(query_ordinal).ok() != Some(queries.len())
                || values.len() != 96
                || values.null_count() != 0
                || values.values().iter().any(|value| !value.is_finite())
                || !norm.is_finite()
                || (norm - 1.0).abs() > 1.0e-4
            {
                return Err(invalid("V26 query authority differs"));
            }
            let vector: [f32; 96] = values
                .values()
                .as_ref()
                .try_into()
                .map_err(|_| invalid("V26 query vector width differs"))?;
            queries.push(V26ExternalQuery {
                query_ordinal,
                vector,
            });
        }
    }
    if queries.len() != expected_queries as usize {
        return Err(invalid("V26 query inventory differs"));
    }
    Ok(queries)
}

fn read_evaluation_truth(
    path: &Path,
    expected_queries: u32,
    queries: &[V26ExternalQuery],
    assignments: &[V26RowPages],
    construction_sha256: &str,
    external_queries_sha256: &str,
) -> Result<Vec<V26QueryTruth>> {
    read_evaluation_truth_with_assignment(
        path,
        expected_queries,
        queries,
        construction_sha256,
        external_queries_sha256,
        |neighbor| {
            assignments
                .get(usize::try_from(neighbor).map_err(|_| invalid("V26 truth source differs"))?)
                .copied()
                .ok_or_else(|| invalid("V26 truth source differs"))
        },
    )
}

fn read_evaluation_truth_with_assignment<F>(
    path: &Path,
    expected_queries: u32,
    queries: &[V26ExternalQuery],
    construction_sha256: &str,
    external_queries_sha256: &str,
    mut assignment_for: F,
) -> Result<Vec<V26QueryTruth>>
where
    F: FnMut(u64) -> Result<V26RowPages>,
{
    let reader = open_reader(path)?;
    if reader.schema().as_ref() != &v26_truth_schema()
        || u32::try_from(reader.metadata().file_metadata().num_rows()).ok()
            != Some(expected_queries)
    {
        return Err(invalid("V26 truth Parquet authority differs"));
    }
    let mut truths = Vec::with_capacity(expected_queries as usize);
    for batch in reader
        .build()
        .map_err(|error| invalid(&format!("V26 truth reader failed: {error}")))?
    {
        let batch = batch.map_err(|error| invalid(&format!("V26 truth batch failed: {error}")))?;
        if batch
            .columns()
            .iter()
            .any(|column| column.null_count() != 0)
        {
            return Err(invalid("V26 truth nullability differs"));
        }
        let ordinals = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or_else(|| invalid("V26 truth ordinal differs"))?;
        let neighbors = batch
            .column(1)
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .ok_or_else(|| invalid("V26 truth neighbors differ"))?;
        let distances = batch
            .column(2)
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .ok_or_else(|| invalid("V26 truth distances differ"))?;
        let construction_digests = batch
            .column(3)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| invalid("V26 truth construction binding differs"))?;
        let query_digests = batch
            .column(4)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| invalid("V26 truth query binding differs"))?;
        for row in 0..batch.num_rows() {
            let query_index = truths.len();
            let neighbor_value = neighbors.value(row);
            let neighbor_values = neighbor_value
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(|| invalid("V26 truth neighbor child differs"))?;
            let distance_value = distances.value(row);
            let distance_values = distance_value
                .as_any()
                .downcast_ref::<UInt32Array>()
                .ok_or_else(|| invalid("V26 truth distance child differs"))?;
            let mut ground_truth_page_assignments = Vec::with_capacity(10);
            if construction_digests.value(row) != construction_sha256
                || query_digests.value(row) != external_queries_sha256
                || neighbor_values.len() != 10
                || neighbor_values.null_count() != 0
                || distance_values.len() != 10
                || distance_values.null_count() != 0
            {
                return Err(invalid("V26 truth neighbor width differs"));
            }
            let mut prior = None;
            for (neighbor, distance_bits) in neighbor_values
                .values()
                .iter()
                .zip(distance_values.values())
            {
                let distance = f32::from_bits(*distance_bits);
                if !distance.is_finite()
                    || prior.is_some_and(|(prior_distance, prior_source): (f32, u64)| {
                        distance.total_cmp(&prior_distance).is_lt()
                            || distance.total_cmp(&prior_distance).is_eq()
                                && *neighbor <= prior_source
                    })
                {
                    return Err(invalid("V26 truth rank order differs"));
                }
                prior = Some((distance, *neighbor));
                let assignment = assignment_for(*neighbor)?;
                let mut pages = vec![assignment.primary_page, assignment.replica_page];
                pages.sort_unstable();
                ground_truth_page_assignments.push(pages);
            }
            let truth = V26QueryTruth {
                query_ordinal: ordinals.value(row),
                neighbor_source_ordinals: neighbor_values.values().to_vec(),
                ground_truth_page_assignments,
            };
            if queries.get(query_index).map(|query| query.query_ordinal)
                != Some(truth.query_ordinal)
            {
                return Err(invalid("V26 truth oracle authority differs"));
            }
            truths.push(truth);
        }
    }
    if truths.len() != expected_queries as usize {
        return Err(invalid("V26 truth inventory differs"));
    }
    Ok(truths)
}

fn read_exact_global_construction(
    path: &Path,
    expected_rows: u64,
) -> Result<Vec<V26ConstructionRow>> {
    let reader = open_reader(path)?;
    if reader.schema().as_ref() != &v26_construction_schema()
        || u64::try_from(reader.metadata().file_metadata().num_rows()).ok() != Some(expected_rows)
    {
        return Err(invalid(
            "V26 exact-global construction Parquet authority differs",
        ));
    }
    let mut rows = Vec::with_capacity(
        usize::try_from(expected_rows)
            .map_err(|_| invalid("V26 exact-global construction row count overflows"))?,
    );
    for batch in reader
        .build()
        .map_err(|error| invalid(&format!("V26 exact-global reader failed: {error}")))?
    {
        let batch =
            batch.map_err(|error| invalid(&format!("V26 exact-global batch failed: {error}")))?;
        if batch
            .columns()
            .iter()
            .any(|column| column.null_count() != 0)
        {
            return Err(invalid("V26 exact-global construction nullability differs"));
        }
        let ordinals = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| invalid("V26 exact-global source ordinal differs"))?;
        let vectors = batch
            .column(1)
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .ok_or_else(|| invalid("V26 exact-global vector differs"))?;
        for index in 0..batch.num_rows() {
            let vector = vectors.value(index);
            let values = vector
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| invalid("V26 exact-global vector child differs"))?;
            if values.len() != 96 || values.null_count() != 0 {
                return Err(invalid("V26 exact-global vector width differs"));
            }
            let source_ordinal = ordinals.value(index);
            if usize::try_from(source_ordinal).ok() != Some(rows.len()) {
                return Err(invalid("V26 exact-global source inventory differs"));
            }
            rows.push(V26ConstructionRow {
                source_ordinal,
                vector: values
                    .values()
                    .as_ref()
                    .try_into()
                    .map_err(|_| invalid("V26 exact-global vector width differs"))?,
            });
        }
    }
    if u64::try_from(rows.len()).ok() != Some(expected_rows) {
        return Err(invalid("V26 exact-global construction inventory differs"));
    }
    Ok(rows)
}

fn external_truth_batch(
    rows: &[V26ExternalTruth],
    construction_sha256: &str,
    external_queries_sha256: &str,
) -> Result<RecordBatch> {
    let mut neighbors = Vec::with_capacity(rows.len() * 10);
    let mut distances = Vec::with_capacity(rows.len() * 10);
    for row in rows {
        if row.neighbor_source_ordinals.len() != 10 || row.neighbor_distance_bits.len() != 10 {
            return Err(invalid("V26 external truth row width differs"));
        }
        neighbors.extend_from_slice(&row.neighbor_source_ordinals);
        distances.extend_from_slice(&row.neighbor_distance_bits);
    }
    RecordBatch::try_new(
        Arc::new(v26_truth_schema()),
        vec![
            Arc::new(UInt32Array::from_iter_values(
                rows.iter().map(|row| row.query_ordinal),
            )) as ArrayRef,
            Arc::new(
                FixedSizeListArray::try_new(
                    Arc::new(Field::new("element", DataType::UInt64, false)),
                    10,
                    Arc::new(UInt64Array::from(neighbors)),
                    None,
                )
                .map_err(|error| invalid(&format!("V26 truth neighbor batch failed: {error}")))?,
            ),
            Arc::new(
                FixedSizeListArray::try_new(
                    Arc::new(Field::new("element", DataType::UInt32, false)),
                    10,
                    Arc::new(UInt32Array::from(distances)),
                    None,
                )
                .map_err(|error| invalid(&format!("V26 truth distance batch failed: {error}")))?,
            ),
            Arc::new(StringArray::from(vec![construction_sha256; rows.len()])),
            Arc::new(StringArray::from(vec![external_queries_sha256; rows.len()])),
        ],
    )
    .map_err(|error| invalid(&format!("V26 truth batch failed: {error}")))
}

pub fn run_v26_truth_build(request: &V26TruthBuildRequest) -> Result<V26LocalObjectPath> {
    if request.expected_rows < 10
        || request.expected_queries != 512
        || request.output_path.exists()
        || !request.output_uri.starts_with("s3://")
        || request.construction_rows.identity.generation
            != request.external_queries.identity.generation
    {
        return Err(invalid("V26 truth build request differs"));
    }
    authenticate(&request.construction_rows, "construction-parquet")?;
    authenticate(&request.external_queries, "external-queries-parquet")?;
    let rows =
        read_exact_global_construction(&request.construction_rows.path, request.expected_rows)?;
    let queries =
        read_evaluation_queries(&request.external_queries.path, request.expected_queries)?;
    let truth = build_v26_external_truth_rows(&rows, &queries)?;
    let result = (|| {
        write_batch(
            &request.output_path,
            external_truth_batch(
                &truth,
                &request.construction_rows.identity.digest,
                &request.external_queries.identity.digest,
            )?,
        )?;
        let reader = open_reader(&request.output_path)?;
        if reader.schema().as_ref() != &v26_truth_schema()
            || u32::try_from(reader.metadata().file_metadata().num_rows()).ok()
                != Some(request.expected_queries)
        {
            return Err(invalid("V26 truth output authority differs"));
        }
        let (encoded_bytes, digest) = sha256_file(&request.output_path)?;
        Ok(V26LocalObjectPath {
            identity: V26ObjectIdentity {
                role: "external-truth-parquet".to_owned(),
                uri: request.output_uri.clone(),
                digest_algorithm: "sha256".to_owned(),
                digest,
                encoded_bytes,
                generation: request.construction_rows.identity.generation.clone(),
            },
            path: request.output_path.clone(),
        })
    })();
    if result.is_err() {
        let _ = fs::remove_file(&request.output_path);
    }
    result
}

struct V26LoadedExactGlobal {
    rows: Vec<V26ConstructionRow>,
    assignments: Vec<V26RowPages>,
    queries: Vec<V26ExternalQuery>,
    truths: Vec<V26QueryTruth>,
}

fn load_v26_exact_global(request: &V26ExactGlobalRequest) -> Result<V26LoadedExactGlobal> {
    load_v26_exact_global_with_page_budget(request, 8)
}

fn load_v26_exact_global_with_page_budget(
    request: &V26ExactGlobalRequest,
    page_budget: usize,
) -> Result<V26LoadedExactGlobal> {
    let (_, _, layout_result) =
        evaluate_v26_layout_oracle_with_page_budget(&request.layout, page_budget)?;
    if layout_result.disposition != V26Disposition::BoundedLayoutCandidate {
        return Err(invalid("V26 exact-global layout gate is closed"));
    }
    let terminal = read_layout_terminal(&request.layout.layout_terminal)?;
    if request.construction_rows.identity != terminal.authority.construction_rows
        || !terminal
            .outputs
            .iter()
            .any(|identity| identity == &request.layout.page_assignments.identity)
    {
        return Err(invalid("V26 exact-global input authority differs"));
    }
    authenticate(&request.construction_rows, "construction-parquet")?;
    authenticate(&request.layout.page_assignments, "page-assignments-parquet")?;
    authenticate(&request.layout.external_queries, "external-queries-parquet")?;
    authenticate(&request.layout.truth, "truth-parquet")?;
    let expected_rows = terminal.authority.expected_rows;
    let rows = read_exact_global_construction(&request.construction_rows.path, expected_rows)?;
    let assignments = read_assignments(
        &request.layout.page_assignments.path,
        i64::try_from(expected_rows)
            .map_err(|_| invalid("V26 exact-global row count overflows"))?,
    )?;
    let queries = read_evaluation_queries(
        &request.layout.external_queries.path,
        request.layout.expected_queries,
    )?;
    let truths = read_evaluation_truth(
        &request.layout.truth.path,
        request.layout.expected_queries,
        &queries,
        &assignments,
        &terminal.authority.construction_rows.digest,
        &request.layout.external_queries.identity.digest,
    )?;
    Ok(V26LoadedExactGlobal {
        rows,
        assignments,
        queries,
        truths,
    })
}

pub fn evaluate_v26_exact_global(
    request: &V26ExactGlobalRequest,
) -> Result<Vec<V26ExactGlobalSample>> {
    let loaded = load_v26_exact_global(request)?;
    evaluate_v26_exact_global_external_rows(
        &loaded.rows,
        &loaded.assignments,
        &loaded.queries,
        &loaded.truths,
        &request.ranked_row_limits,
        8,
    )
}

fn summarize_v26_exact_global(
    samples: &[V26ExactGlobalSample],
    query_count: u32,
    ranked_row_limits: &[u32],
) -> Result<V26ExactGlobalResult> {
    let query_count_usize = usize::try_from(query_count)
        .map_err(|_| invalid("V26 exact-global query count overflows"))?;
    if query_count != 512
        || ranked_row_limits != [10, 32, 128, 512, 2_048, 4_096]
        || samples.len() != query_count_usize * ranked_row_limits.len()
    {
        return Err(invalid("V26 exact-global summary authority differs"));
    }
    let mut rank_results = Vec::with_capacity(ranked_row_limits.len());
    for (limit_index, limit) in ranked_row_limits.iter().enumerate() {
        let mut total_hits = 0_u64;
        let mut total_oracle_hits = 0_u64;
        let mut minimum_query_recall_ppm = 1_000_000_u64;
        for query_index in 0..query_count_usize {
            let sample = &samples[query_index * ranked_row_limits.len() + limit_index];
            if sample.query_ordinal != u32::try_from(query_index).unwrap()
                || sample.ranked_row_limit != *limit
            {
                return Err(invalid("V26 exact-global summary sample order differs"));
            }
            total_hits = total_hits
                .checked_add(u64::from(sample.hits))
                .ok_or_else(|| invalid("V26 exact-global summary overflows"))?;
            total_oracle_hits = total_oracle_hits
                .checked_add(u64::from(sample.oracle_hits))
                .ok_or_else(|| invalid("V26 exact-global summary overflows"))?;
            minimum_query_recall_ppm = minimum_query_recall_ppm.min(sample.recall_ppm);
        }
        let aggregate_recall_ppm = total_hits * 1_000_000 / (u64::from(query_count) * 10);
        let oracle_attainment_ppm = total_hits * 1_000_000 / total_oracle_hits;
        rank_results.push(V26ExactGlobalRankResult {
            ranked_row_limit: *limit,
            aggregate_recall_ppm,
            minimum_query_recall_ppm,
            oracle_attainment_ppm,
            passed: aggregate_recall_ppm >= 975_000 && oracle_attainment_ppm >= 995_000,
        });
    }
    let disposition = if rank_results.iter().any(|result| result.passed) {
        V26Disposition::BoundedLayoutCandidate
    } else {
        V26Disposition::RankReducerRejected
    };
    Ok(V26ExactGlobalResult {
        schema: "borsuk-v26-cumulative-exact-global-result-v1".to_owned(),
        query_count,
        rank_results,
        disposition,
        page_body_reads: 0,
        claim_eligible: false,
    })
}

pub fn run_v26_exact_global(request: &V26ExactGlobalRequest) -> Result<Vec<u8>> {
    let loaded = load_v26_exact_global(request)?;
    let samples = evaluate_v26_exact_global_external_rows(
        &loaded.rows,
        &loaded.assignments,
        &loaded.queries,
        &loaded.truths,
        &request.ranked_row_limits,
        8,
    )?;
    let result = summarize_v26_exact_global(
        &samples,
        request.layout.expected_queries,
        &request.ranked_row_limits,
    )?;
    canonical_v26_exact_global_result_bytes(
        &result,
        &loaded.rows,
        &loaded.assignments,
        &loaded.queries,
        &loaded.truths,
        &samples,
    )
}

fn load_v26_tree_router(
    request: &V26TreeRouterRequest,
) -> Result<(V26Tree, V26Tree, Vec<V26ExternalQuery>, Vec<V26QueryTruth>)> {
    load_v26_tree_router_with_page_budget(request, 8)
}

fn load_v26_tree_router_with_page_budget(
    request: &V26TreeRouterRequest,
    page_budget: usize,
) -> Result<(V26Tree, V26Tree, Vec<V26ExternalQuery>, Vec<V26QueryTruth>)> {
    let (truths, _, layout_result) =
        evaluate_v26_layout_oracle_with_page_budget(&request.layout, page_budget)?;
    if usize::try_from(request.page_budget).ok() != Some(page_budget)
        || layout_result.disposition != V26Disposition::BoundedLayoutCandidate
    {
        return Err(invalid("V26 tree router layout gate is closed"));
    }
    let (primary, replica) = load_v26_router_trees(request, request.page_budget)?;
    let queries = read_evaluation_queries(
        &request.layout.external_queries.path,
        request.layout.expected_queries,
    )?;
    Ok((primary, replica, queries, truths))
}

fn load_v26_router_trees(
    request: &V26TreeRouterRequest,
    expected_page_budget: u32,
) -> Result<(V26Tree, V26Tree)> {
    if request.page_budget != expected_page_budget {
        return Err(invalid("V26 tree router page budget differs"));
    }
    let terminal = read_layout_terminal(&request.layout.layout_terminal)?;
    if [
        &request.primary_tree.identity,
        &request.replica_tree.identity,
    ]
    .iter()
    .any(|identity| {
        identity.generation != terminal.authority.generation
            || !terminal.outputs.iter().any(|output| output == *identity)
    }) {
        return Err(invalid("V26 tree router input authority differs"));
    }
    authenticate(&request.primary_tree, "primary-tree-parquet")?;
    authenticate(&request.replica_tree, "replica-tree-parquet")?;
    let node_count = i64::from(terminal.leaves_per_tree)
        .checked_mul(2)
        .and_then(|value| value.checked_sub(1))
        .ok_or_else(|| invalid("V26 tree router node count overflows"))?;
    let primary = read_tree(
        &request.primary_tree.path,
        node_count,
        terminal.authority.primary_seed,
    )?;
    let replica = read_tree(
        &request.replica_tree.path,
        node_count,
        terminal.authority.replica_seed,
    )?;
    let assignment_rows = i64::try_from(terminal.row_count)
        .map_err(|_| invalid("V26 tree router assignment count overflows"))?;
    let assignments = read_assignments(&request.layout.page_assignments.path, assignment_rows)?;
    validate_v26_dual_tree_layout(&terminal.authority, &primary, &replica, &assignments)?;
    Ok((primary, replica))
}

pub fn run_v26_tree_router(request: &V26TreeRouterRequest) -> Result<Vec<u8>> {
    let (primary, replica, queries, truths) = load_v26_tree_router(request)?;
    let (samples, result) = evaluate_v26_tree_router(
        &primary,
        &replica,
        &queries,
        &truths,
        usize::try_from(request.page_budget)
            .map_err(|_| invalid("V26 tree router page budget overflows"))?,
    )?;
    canonical_v26_tree_router_result_bytes(&result, &primary, &replica, &queries, &truths, &samples)
}

pub fn run_v26_tree_router_diagnostic(request: &V26TreeRouterRequest) -> Result<Vec<u8>> {
    let (primary, replica, queries, truths) = load_v26_tree_router_with_page_budget(request, 10)?;
    let (samples, widths) =
        diagnose_v26_tree_router_candidate_widths(&primary, &replica, &queries, &truths)?;
    let value = serde_json::json!({
        "schema": "borsuk-v26-tree-router-diagnostic-result-v1",
        "samples": samples,
        "widths": widths,
        "page_body_reads": 0,
        "claim_eligible": false,
    });
    let mut bytes = serde_json::to_vec(&canonical_json_value(value)).map_err(|error| {
        invalid(&format!(
            "V26 tree router diagnostic serialization failed: {error}"
        ))
    })?;
    bytes.push(b'\n');
    Ok(bytes)
}

pub fn run_v26_centroid_router(request: &V26CentroidRouterRequest) -> Result<Vec<u8>> {
    let exact = V26ExactGlobalRequest {
        construction_rows: request.construction_rows.clone(),
        layout: request.router.layout.clone(),
        ranked_row_limits: vec![10, 32, 128, 512, 2_048, 4_096],
    };
    let loaded = load_v26_exact_global(&exact)?;
    let (primary, replica, queries, truths) = load_v26_tree_router(&request.router)?;
    if queries != loaded.queries || truths != loaded.truths || request.router.page_budget != 8 {
        return Err(invalid("V26 centroid router authority differs"));
    }
    let page_count = rank_v26_tree_pages(&primary, &replica, &queries[0].vector)?.len();
    let candidate_page_limit = 128.min(page_count);
    let (samples, result) = evaluate_v26_centroid_router(
        &primary,
        &replica,
        &loaded.rows,
        &loaded.assignments,
        &queries,
        &truths,
        candidate_page_limit,
    )?;
    let value = serde_json::json!({
        "candidate_page_limit": candidate_page_limit,
        "result": result,
        "samples": samples,
    });
    let mut bytes = serde_json::to_vec(&canonical_json_value(value)).map_err(|error| {
        invalid(&format!(
            "V26 centroid router serialization failed: {error}"
        ))
    })?;
    bytes.push(b'\n');
    Ok(bytes)
}

pub fn run_v26_global_centroid_frontier_diagnostic(
    request: &V26CentroidRouterRequest,
) -> Result<Vec<u8>> {
    let exact = V26ExactGlobalRequest {
        construction_rows: request.construction_rows.clone(),
        layout: request.router.layout.clone(),
        ranked_row_limits: vec![10, 32, 128, 512, 2_048, 4_096],
    };
    let loaded = load_v26_exact_global_with_page_budget(&exact, 10)?;
    let (primary, replica, queries, truths) =
        load_v26_tree_router_with_page_budget(&request.router, 10)?;
    if queries != loaded.queries || truths != loaded.truths || request.router.page_budget != 10 {
        return Err(invalid("V26 global centroid authority differs"));
    }
    let (samples, widths) = diagnose_v26_global_centroid_candidate_widths(
        &primary,
        &replica,
        &loaded.rows,
        &loaded.assignments,
        &queries,
        &truths,
    )?;
    let value = serde_json::json!({
        "claim_eligible": false,
        "page_body_reads": 0,
        "samples": samples,
        "schema": "borsuk-v26-global-centroid-frontier-result-v1",
        "widths": widths,
    });
    let mut bytes = serde_json::to_vec(&canonical_json_value(value)).map_err(|error| {
        invalid(&format!(
            "V26 global centroid serialization failed: {error}"
        ))
    })?;
    bytes.push(b'\n');
    Ok(bytes)
}

fn v26_global_page_mode_evidence_schema() -> Schema {
    let mut fields = vec![
        Field::new("query_ordinal", DataType::UInt32, false),
        Field::new("mode_count", DataType::UInt32, false),
        Field::new("candidate_page_limit", DataType::UInt32, false),
    ];
    fields.extend(
        (0..10).map(|index| Field::new(format!("selected_page_{index}"), DataType::UInt32, true)),
    );
    fields.extend([
        Field::new("hits", DataType::UInt32, false),
        Field::new("oracle_hits", DataType::UInt32, false),
        Field::new("recall_ppm", DataType::UInt64, false),
        Field::new("oracle_attainment_ppm", DataType::UInt64, false),
    ]);
    Schema::new(fields)
}

fn v26_global_page_mode_evidence_batch(samples: &[V26PageModeSample]) -> Result<RecordBatch> {
    if samples.is_empty()
        || samples
            .iter()
            .any(|sample| sample.selected_pages.len() > 10)
    {
        return Err(invalid("V26 global page mode evidence inventory differs"));
    }
    let mut columns: Vec<ArrayRef> = vec![
        Arc::new(UInt32Array::from_iter_values(
            samples.iter().map(|sample| sample.query_ordinal),
        )),
        Arc::new(UInt32Array::from_iter_values(
            samples.iter().map(|sample| sample.mode_count),
        )),
        Arc::new(UInt32Array::from_iter_values(
            samples.iter().map(|sample| sample.candidate_page_limit),
        )),
    ];
    columns.extend((0..10).map(|index| {
        Arc::new(UInt32Array::from_iter(
            samples
                .iter()
                .map(|sample| sample.selected_pages.get(index).copied()),
        )) as ArrayRef
    }));
    columns.extend([
        Arc::new(UInt32Array::from_iter_values(
            samples.iter().map(|sample| sample.hits),
        )) as ArrayRef,
        Arc::new(UInt32Array::from_iter_values(
            samples.iter().map(|sample| sample.oracle_hits),
        )) as ArrayRef,
        Arc::new(UInt64Array::from_iter_values(
            samples.iter().map(|sample| sample.recall_ppm),
        )) as ArrayRef,
        Arc::new(UInt64Array::from_iter_values(
            samples.iter().map(|sample| sample.oracle_attainment_ppm),
        )) as ArrayRef,
    ]);
    RecordBatch::try_new(Arc::new(v26_global_page_mode_evidence_schema()), columns)
        .map_err(|error| invalid(&format!("V26 global page mode evidence failed: {error}")))
}

pub fn run_v26_global_page_mode_frontier_diagnostic(
    request: &V26PageModeRouterRequest,
) -> Result<Vec<u8>> {
    if request.evidence_output_path.exists()
        || !request.evidence_output_uri.starts_with("s3://")
        || !request.evidence_output_uri.ends_with(".parquet")
    {
        return Err(invalid("V26 global page mode output authority differs"));
    }
    let exact = V26ExactGlobalRequest {
        construction_rows: request.construction_rows.clone(),
        layout: request.router.layout.clone(),
        ranked_row_limits: vec![10, 32, 128, 512, 2_048, 4_096],
    };
    let loaded = load_v26_exact_global_with_page_budget(&exact, 10)?;
    let (_, _, queries, truths) = load_v26_tree_router_with_page_budget(&request.router, 10)?;
    if queries != loaded.queries || truths != loaded.truths || request.router.page_budget != 10 {
        return Err(invalid("V26 global page mode authority differs"));
    }
    let (samples, mode_results) = diagnose_v26_global_page_mode_candidate_widths(
        &loaded.rows,
        &loaded.assignments,
        &queries,
        &truths,
    )?;
    let output = (|| {
        write_batch(
            &request.evidence_output_path,
            v26_global_page_mode_evidence_batch(&samples)?,
        )?;
        let (encoded_bytes, digest) = sha256_file(&request.evidence_output_path)?;
        let evidence = V26ObjectIdentity {
            role: "global-page-mode-evidence-parquet".to_owned(),
            uri: request.evidence_output_uri.clone(),
            digest_algorithm: "sha256".to_owned(),
            digest,
            encoded_bytes,
            generation: request.construction_rows.identity.generation.clone(),
        };
        let value = serde_json::json!({
            "claim_eligible": false,
            "evidence": evidence,
            "mode_results": mode_results,
            "page_body_reads": 0,
            "schema": "borsuk-v26-global-page-mode-frontier-result-v1",
        });
        let mut bytes = serde_json::to_vec(&canonical_json_value(value)).map_err(|error| {
            invalid(&format!(
                "V26 global page mode serialization failed: {error}"
            ))
        })?;
        bytes.push(b'\n');
        Ok(bytes)
    })();
    if output.is_err() {
        let _ = fs::remove_file(&request.evidence_output_path);
    }
    output
}

fn v26_page_mode_evidence_schema() -> Schema {
    Schema::new(vec![
        Field::new("query_ordinal", DataType::UInt32, false),
        Field::new("mode_count", DataType::UInt32, false),
        Field::new("candidate_page_limit", DataType::UInt32, false),
        Field::new(
            "selected_pages",
            DataType::FixedSizeList(Arc::new(Field::new("element", DataType::UInt32, false)), 8),
            false,
        ),
        Field::new("hits", DataType::UInt32, false),
        Field::new("oracle_hits", DataType::UInt32, false),
        Field::new("recall_ppm", DataType::UInt64, false),
        Field::new("oracle_attainment_ppm", DataType::UInt64, false),
    ])
}

fn v26_page_mode_evidence_batch(samples: &[V26PageModeSample]) -> Result<RecordBatch> {
    let pages = samples
        .iter()
        .flat_map(|sample| sample.selected_pages.iter().copied())
        .collect::<Vec<_>>();
    RecordBatch::try_new(
        Arc::new(v26_page_mode_evidence_schema()),
        vec![
            Arc::new(UInt32Array::from_iter_values(
                samples.iter().map(|sample| sample.query_ordinal),
            )),
            Arc::new(UInt32Array::from_iter_values(
                samples.iter().map(|sample| sample.mode_count),
            )),
            Arc::new(UInt32Array::from_iter_values(
                samples.iter().map(|sample| sample.candidate_page_limit),
            )),
            Arc::new(
                FixedSizeListArray::try_new(
                    Arc::new(Field::new("element", DataType::UInt32, false)),
                    8,
                    Arc::new(UInt32Array::from(pages)),
                    None,
                )
                .map_err(|error| invalid(&format!("V26 page mode evidence failed: {error}")))?,
            ),
            Arc::new(UInt32Array::from_iter_values(
                samples.iter().map(|sample| sample.hits),
            )),
            Arc::new(UInt32Array::from_iter_values(
                samples.iter().map(|sample| sample.oracle_hits),
            )),
            Arc::new(UInt64Array::from_iter_values(
                samples.iter().map(|sample| sample.recall_ppm),
            )),
            Arc::new(UInt64Array::from_iter_values(
                samples.iter().map(|sample| sample.oracle_attainment_ppm),
            )),
        ],
    )
    .map_err(|error| invalid(&format!("V26 page mode evidence failed: {error}")))
}

pub fn run_v26_page_mode_router(request: &V26PageModeRouterRequest) -> Result<Vec<u8>> {
    if request.evidence_output_path.exists()
        || !request.evidence_output_uri.starts_with("s3://")
        || !request.evidence_output_uri.ends_with(".parquet")
    {
        return Err(invalid("V26 page mode output authority differs"));
    }
    let exact = V26ExactGlobalRequest {
        construction_rows: request.construction_rows.clone(),
        layout: request.router.layout.clone(),
        ranked_row_limits: vec![10, 32, 128, 512, 2_048, 4_096],
    };
    let loaded = load_v26_exact_global(&exact)?;
    let (primary, replica, queries, truths) = load_v26_tree_router(&request.router)?;
    if queries != loaded.queries || truths != loaded.truths || request.router.page_budget != 8 {
        return Err(invalid("V26 page mode authority differs"));
    }
    let page_count = rank_v26_tree_pages(&primary, &replica, &queries[0].vector)?.len();
    let candidate_page_limit = 128.min(page_count);
    let (samples, mode_results) = evaluate_v26_page_mode_router(
        &primary,
        &replica,
        &loaded.rows,
        &loaded.assignments,
        &queries,
        &truths,
        candidate_page_limit,
    )?;
    let result = (|| {
        write_batch(
            &request.evidence_output_path,
            v26_page_mode_evidence_batch(&samples)?,
        )?;
        let (encoded_bytes, digest) = sha256_file(&request.evidence_output_path)?;
        let evidence = V26ObjectIdentity {
            role: "page-mode-evidence-parquet".to_owned(),
            uri: request.evidence_output_uri.clone(),
            digest_algorithm: "sha256".to_owned(),
            digest,
            encoded_bytes,
            generation: request.construction_rows.identity.generation.clone(),
        };
        let value = serde_json::json!({
            "schema": "borsuk-v26-page-mode-router-result-v1",
            "candidate_page_limit": candidate_page_limit,
            "mode_results": mode_results,
            "evidence": evidence,
            "page_body_reads": 0,
            "claim_eligible": false,
        });
        let mut bytes = serde_json::to_vec(&canonical_json_value(value))
            .map_err(|error| invalid(&format!("V26 page mode serialization failed: {error}")))?;
        bytes.push(b'\n');
        Ok(bytes)
    })();
    if result.is_err() {
        let _ = fs::remove_file(&request.evidence_output_path);
    }
    result
}

fn v26_candidate_cover_evidence_schema(page_budget: i32) -> Schema {
    Schema::new(vec![
        Field::new("query_ordinal", DataType::UInt32, false),
        Field::new("candidate_page_limit", DataType::UInt32, false),
        Field::new("ranked_row_limit", DataType::UInt32, false),
        Field::new(
            "selected_pages",
            DataType::FixedSizeList(
                Arc::new(Field::new("element", DataType::UInt32, false)),
                page_budget,
            ),
            false,
        ),
        Field::new("hits", DataType::UInt32, false),
        Field::new("oracle_hits", DataType::UInt32, false),
        Field::new("recall_ppm", DataType::UInt64, false),
        Field::new("oracle_attainment_ppm", DataType::UInt64, false),
    ])
}

fn v26_candidate_cover_evidence_batch(
    samples: &[crate::V26TreeRouterSample],
    candidate_page_limit: u32,
    page_budget: usize,
) -> Result<RecordBatch> {
    if samples.len() != 512
        || samples
            .iter()
            .any(|sample| sample.selected_pages.len() != page_budget)
    {
        return Err(invalid("V26 candidate cover evidence inventory differs"));
    }
    let pages = samples
        .iter()
        .flat_map(|sample| sample.selected_pages.iter().copied())
        .collect::<Vec<_>>();
    RecordBatch::try_new(
        Arc::new(v26_candidate_cover_evidence_schema(
            i32::try_from(page_budget)
                .map_err(|_| invalid("V26 candidate cover page budget overflows"))?,
        )),
        vec![
            Arc::new(UInt32Array::from_iter_values(
                samples.iter().map(|sample| sample.query_ordinal),
            )),
            Arc::new(UInt32Array::from_value(candidate_page_limit, samples.len())),
            Arc::new(UInt32Array::from_value(10, samples.len())),
            Arc::new(
                FixedSizeListArray::try_new(
                    Arc::new(Field::new("element", DataType::UInt32, false)),
                    i32::try_from(page_budget)
                        .map_err(|_| invalid("V26 candidate cover page budget overflows"))?,
                    Arc::new(UInt32Array::from(pages)),
                    None,
                )
                .map_err(|error| {
                    invalid(&format!("V26 candidate cover evidence failed: {error}"))
                })?,
            ),
            Arc::new(UInt32Array::from_iter_values(
                samples.iter().map(|sample| sample.hits),
            )),
            Arc::new(UInt32Array::from_iter_values(
                samples.iter().map(|sample| sample.oracle_hits),
            )),
            Arc::new(UInt64Array::from_iter_values(
                samples.iter().map(|sample| sample.recall_ppm),
            )),
            Arc::new(UInt64Array::from_iter_values(
                samples.iter().map(|sample| sample.oracle_attainment_ppm),
            )),
        ],
    )
    .map_err(|error| invalid(&format!("V26 candidate cover evidence failed: {error}")))
}

pub fn run_v26_candidate_row_cover(request: &V26CandidateCoverRequest) -> Result<Vec<u8>> {
    if request.evidence_output_path.exists()
        || !request.evidence_output_uri.starts_with("s3://")
        || !request.evidence_output_uri.ends_with(".parquet")
    {
        return Err(invalid("V26 candidate cover output authority differs"));
    }
    let exact = V26ExactGlobalRequest {
        construction_rows: request.construction_rows.clone(),
        layout: request.router.layout.clone(),
        ranked_row_limits: vec![10],
    };
    let loaded = load_v26_exact_global_with_page_budget(&exact, 10)?;
    let (primary, replica) = load_v26_router_trees(&request.router, 10)?;
    if request.router.page_budget != 10 {
        return Err(invalid("V26 candidate cover authority differs"));
    }
    let queries = &loaded.queries;
    let truths = &loaded.truths;
    let page_count = rank_v26_tree_pages(&primary, &replica, &queries[0].vector)?.len();
    let candidate_page_limit = 128.min(page_count);
    let (samples, result) = evaluate_v26_candidate_row_cover(
        &primary,
        &replica,
        &loaded.rows,
        &loaded.assignments,
        queries,
        truths,
        (candidate_page_limit, 10),
    )?;
    let output = (|| {
        write_batch(
            &request.evidence_output_path,
            v26_candidate_cover_evidence_batch(
                &samples,
                u32::try_from(candidate_page_limit)
                    .map_err(|_| invalid("V26 candidate cover width overflows"))?,
                10,
            )?,
        )?;
        let (encoded_bytes, digest) = sha256_file(&request.evidence_output_path)?;
        let evidence = V26ObjectIdentity {
            role: "candidate-cover-evidence-parquet".to_owned(),
            uri: request.evidence_output_uri.clone(),
            digest_algorithm: "sha256".to_owned(),
            digest,
            encoded_bytes,
            generation: request.construction_rows.identity.generation.clone(),
        };
        let value = serde_json::json!({
            "schema": "borsuk-v26-candidate-row-cover-output-v1",
            "candidate_page_limit": candidate_page_limit,
            "ranked_row_limit": 10,
            "result": result,
            "evidence": evidence,
            "page_body_reads": 0,
            "claim_eligible": false,
        });
        let mut bytes = serde_json::to_vec(&canonical_json_value(value)).map_err(|error| {
            invalid(&format!(
                "V26 candidate cover serialization failed: {error}"
            ))
        })?;
        bytes.push(b'\n');
        Ok(bytes)
    })();
    if output.is_err() {
        let _ = fs::remove_file(&request.evidence_output_path);
    }
    output
}

pub fn run_v26_pq8_candidate_cover(request: &V26Pq8CoverRequest) -> Result<Vec<u8>> {
    if request.evidence_output_path.exists()
        || !request.evidence_output_uri.starts_with("s3://")
        || !request.evidence_output_uri.ends_with(".parquet")
    {
        return Err(invalid("V26 PQ8 cover output authority differs"));
    }
    let exact = V26ExactGlobalRequest {
        construction_rows: request.construction_rows.clone(),
        layout: request.router.layout.clone(),
        ranked_row_limits: vec![10],
    };
    let loaded = load_v26_exact_global(&exact)?;
    let (primary, replica, queries, truths) = load_v26_tree_router(&request.router)?;
    if queries != loaded.queries || truths != loaded.truths || request.router.page_budget != 8 {
        return Err(invalid("V26 PQ8 cover authority differs"));
    }
    let page_count = rank_v26_tree_pages(&primary, &replica, &queries[0].vector)?.len();
    let candidate_page_limit = 128.min(page_count);
    let (samples, result) = evaluate_v26_pq8_candidate_cover(
        &primary,
        &replica,
        &loaded.rows,
        &loaded.assignments,
        &queries,
        &truths,
        candidate_page_limit,
    )?;
    let output = (|| {
        write_batch(
            &request.evidence_output_path,
            v26_candidate_cover_evidence_batch(
                &samples,
                u32::try_from(candidate_page_limit)
                    .map_err(|_| invalid("V26 PQ8 cover width overflows"))?,
                8,
            )?,
        )?;
        let (encoded_bytes, digest) = sha256_file(&request.evidence_output_path)?;
        let evidence = V26ObjectIdentity {
            role: "pq8-cover-evidence-parquet".to_owned(),
            uri: request.evidence_output_uri.clone(),
            digest_algorithm: "sha256".to_owned(),
            digest,
            encoded_bytes,
            generation: request.construction_rows.identity.generation.clone(),
        };
        let value = serde_json::json!({
            "schema": "borsuk-v26-pq8-candidate-cover-output-v1",
            "candidate_page_limit": candidate_page_limit,
            "ranked_row_limit": 10,
            "projected_resident_bytes_100m": projected_v26_pq8_resident_bytes(100_000_000, 2_816)?,
            "result": result,
            "evidence": evidence,
            "page_body_reads": 0,
            "claim_eligible": false,
        });
        let mut bytes = serde_json::to_vec(&canonical_json_value(value))
            .map_err(|error| invalid(&format!("V26 PQ8 cover serialization failed: {error}")))?;
        bytes.push(b'\n');
        Ok(bytes)
    })();
    if output.is_err() {
        let _ = fs::remove_file(&request.evidence_output_path);
    }
    output
}

fn v26_pq_width_ladder_evidence_schema() -> Schema {
    Schema::new(vec![
        Field::new("code_width", DataType::UInt32, false),
        Field::new("projected_resident_bytes_100m", DataType::UInt64, false),
        Field::new("query_ordinal", DataType::UInt32, false),
        Field::new("candidate_page_limit", DataType::UInt32, false),
        Field::new("ranked_row_limit", DataType::UInt32, false),
        Field::new(
            "selected_pages",
            DataType::FixedSizeList(Arc::new(Field::new("element", DataType::UInt32, false)), 8),
            false,
        ),
        Field::new("hits", DataType::UInt32, false),
        Field::new("oracle_hits", DataType::UInt32, false),
        Field::new("recall_ppm", DataType::UInt64, false),
        Field::new("oracle_attainment_ppm", DataType::UInt64, false),
    ])
}

fn v26_pq_width_ladder_evidence_batch(
    arms: &[crate::V26PqWidthEvaluation],
    candidate_page_limit: u32,
) -> Result<RecordBatch> {
    if arms.len() != 4
        || arms.iter().any(|arm| {
            arm.samples.len() != 512
                || arm
                    .samples
                    .iter()
                    .any(|sample| sample.selected_pages.len() != 8)
        })
    {
        return Err(invalid("V26 PQ width evidence inventory differs"));
    }
    let row_count = arms.len() * 512;
    let pages = arms
        .iter()
        .flat_map(|arm| &arm.samples)
        .flat_map(|sample| sample.selected_pages.iter().copied())
        .collect::<Vec<_>>();
    RecordBatch::try_new(
        Arc::new(v26_pq_width_ladder_evidence_schema()),
        vec![
            Arc::new(UInt32Array::from_iter_values(arms.iter().flat_map(|arm| {
                std::iter::repeat_n(u32::try_from(arm.code_width).unwrap(), 512)
            }))),
            Arc::new(UInt64Array::from_iter_values(arms.iter().flat_map(|arm| {
                std::iter::repeat_n(arm.projected_resident_bytes_100m, 512)
            }))),
            Arc::new(UInt32Array::from_iter_values(
                arms.iter()
                    .flat_map(|arm| &arm.samples)
                    .map(|sample| sample.query_ordinal),
            )),
            Arc::new(UInt32Array::from_value(candidate_page_limit, row_count)),
            Arc::new(UInt32Array::from_value(10, row_count)),
            Arc::new(
                FixedSizeListArray::try_new(
                    Arc::new(Field::new("element", DataType::UInt32, false)),
                    8,
                    Arc::new(UInt32Array::from(pages)),
                    None,
                )
                .map_err(|error| invalid(&format!("V26 PQ width evidence failed: {error}")))?,
            ),
            Arc::new(UInt32Array::from_iter_values(
                arms.iter()
                    .flat_map(|arm| &arm.samples)
                    .map(|sample| sample.hits),
            )),
            Arc::new(UInt32Array::from_iter_values(
                arms.iter()
                    .flat_map(|arm| &arm.samples)
                    .map(|sample| sample.oracle_hits),
            )),
            Arc::new(UInt64Array::from_iter_values(
                arms.iter()
                    .flat_map(|arm| &arm.samples)
                    .map(|sample| sample.recall_ppm),
            )),
            Arc::new(UInt64Array::from_iter_values(
                arms.iter()
                    .flat_map(|arm| &arm.samples)
                    .map(|sample| sample.oracle_attainment_ppm),
            )),
        ],
    )
    .map_err(|error| invalid(&format!("V26 PQ width evidence failed: {error}")))
}

pub fn run_v26_pq_width_ladder(request: &V26PqWidthLadderRequest) -> Result<Vec<u8>> {
    if request.evidence_output_path.exists()
        || !request.evidence_output_uri.starts_with("s3://")
        || !request.evidence_output_uri.ends_with(".parquet")
    {
        return Err(invalid("V26 PQ width output authority differs"));
    }
    let exact = V26ExactGlobalRequest {
        construction_rows: request.construction_rows.clone(),
        layout: request.router.layout.clone(),
        ranked_row_limits: vec![10],
    };
    let loaded = load_v26_exact_global(&exact)?;
    let (primary, replica, queries, truths) = load_v26_tree_router(&request.router)?;
    if queries != loaded.queries || truths != loaded.truths || request.router.page_budget != 8 {
        return Err(invalid("V26 PQ width authority differs"));
    }
    let page_count = rank_v26_tree_pages(&primary, &replica, &queries[0].vector)?.len();
    let candidate_page_limit = 128.min(page_count);
    let arms = evaluate_v26_pq_width_ladder(
        &primary,
        &replica,
        &loaded.rows,
        &loaded.assignments,
        &queries,
        &truths,
        candidate_page_limit,
    )?;
    let output = (|| {
        write_batch(
            &request.evidence_output_path,
            v26_pq_width_ladder_evidence_batch(
                &arms,
                u32::try_from(candidate_page_limit)
                    .map_err(|_| invalid("V26 PQ width candidate count overflows"))?,
            )?,
        )?;
        let (encoded_bytes, digest) = sha256_file(&request.evidence_output_path)?;
        let evidence = V26ObjectIdentity {
            role: "pq-width-ladder-evidence-parquet".to_owned(),
            uri: request.evidence_output_uri.clone(),
            digest_algorithm: "sha256".to_owned(),
            digest,
            encoded_bytes,
            generation: request.construction_rows.identity.generation.clone(),
        };
        let summaries = arms
            .iter()
            .map(|arm| {
                serde_json::json!({
                    "code_width": arm.code_width,
                    "projected_resident_bytes_100m": arm.projected_resident_bytes_100m,
                    "result": arm.result,
                })
            })
            .collect::<Vec<_>>();
        let value = serde_json::json!({
            "schema": "borsuk-v26-pq-width-ladder-output-v1",
            "candidate_page_limit": candidate_page_limit,
            "ranked_row_limit": 10,
            "arms": summaries,
            "evidence": evidence,
            "page_body_reads": 0,
            "claim_eligible": false,
        });
        let mut bytes = serde_json::to_vec(&canonical_json_value(value))
            .map_err(|error| invalid(&format!("V26 PQ width serialization failed: {error}")))?;
        bytes.push(b'\n');
        Ok(bytes)
    })();
    if output.is_err() {
        let _ = fs::remove_file(&request.evidence_output_path);
    }
    output
}

fn v26_pq16_rerank_evidence_schema() -> Schema {
    Schema::new(vec![
        Field::new("ranked_row_limit", DataType::UInt32, false),
        Field::new("query_ordinal", DataType::UInt32, false),
        Field::new(
            "selected_pages",
            DataType::FixedSizeList(Arc::new(Field::new("element", DataType::UInt32, false)), 10),
            false,
        ),
        Field::new("hits", DataType::UInt32, false),
        Field::new("oracle_hits", DataType::UInt32, false),
        Field::new("recall_ppm", DataType::UInt64, false),
        Field::new("oracle_attainment_ppm", DataType::UInt64, false),
    ])
}

fn v26_pq16_rerank_evidence_batch(arms: &[crate::V26Pq16RerankEvaluation]) -> Result<RecordBatch> {
    if arms.len() != 5
        || arms.iter().any(|arm| {
            arm.samples.len() != 512
                || arm
                    .samples
                    .iter()
                    .any(|sample| sample.selected_pages.len() != 10)
        })
    {
        return Err(invalid("V26 PQ16 rerank evidence inventory differs"));
    }
    let pages = arms
        .iter()
        .flat_map(|arm| &arm.samples)
        .flat_map(|sample| sample.selected_pages.iter().copied())
        .collect::<Vec<_>>();
    RecordBatch::try_new(
        Arc::new(v26_pq16_rerank_evidence_schema()),
        vec![
            Arc::new(UInt32Array::from_iter_values(arms.iter().flat_map(|arm| {
                std::iter::repeat_n(u32::try_from(arm.ranked_row_limit).unwrap(), 512)
            }))),
            Arc::new(UInt32Array::from_iter_values(
                arms.iter()
                    .flat_map(|arm| &arm.samples)
                    .map(|sample| sample.query_ordinal),
            )),
            Arc::new(
                FixedSizeListArray::try_new(
                    Arc::new(Field::new("element", DataType::UInt32, false)),
                    10,
                    Arc::new(UInt32Array::from(pages)),
                    None,
                )
                .map_err(|error| invalid(&format!("V26 PQ16 rerank evidence failed: {error}")))?,
            ),
            Arc::new(UInt32Array::from_iter_values(
                arms.iter()
                    .flat_map(|arm| &arm.samples)
                    .map(|sample| sample.hits),
            )),
            Arc::new(UInt32Array::from_iter_values(
                arms.iter()
                    .flat_map(|arm| &arm.samples)
                    .map(|sample| sample.oracle_hits),
            )),
            Arc::new(UInt64Array::from_iter_values(
                arms.iter()
                    .flat_map(|arm| &arm.samples)
                    .map(|sample| sample.recall_ppm),
            )),
            Arc::new(UInt64Array::from_iter_values(
                arms.iter()
                    .flat_map(|arm| &arm.samples)
                    .map(|sample| sample.oracle_attainment_ppm),
            )),
        ],
    )
    .map_err(|error| invalid(&format!("V26 PQ16 rerank evidence failed: {error}")))
}

pub fn run_v26_pq16_exact_rerank(request: &V26Pq16RerankRequest) -> Result<Vec<u8>> {
    if request.evidence_output_path.exists()
        || !request.evidence_output_uri.starts_with("s3://")
        || !request.evidence_output_uri.ends_with(".parquet")
    {
        return Err(invalid("V26 PQ16 rerank output authority differs"));
    }
    let exact = V26ExactGlobalRequest {
        construction_rows: request.construction_rows.clone(),
        layout: request.router.layout.clone(),
        ranked_row_limits: vec![10],
    };
    let loaded = load_v26_exact_global_with_page_budget(&exact, 10)?;
    let (primary, replica) = load_v26_router_trees(&request.router, 10)?;
    let queries = loaded.queries;
    let truths = loaded.truths;
    let page_count = rank_v26_tree_pages(&primary, &replica, &queries[0].vector)?.len();
    let candidate_page_limit = 128.min(page_count);
    let arms = evaluate_v26_pq16_exact_rerank_ladder(
        &primary,
        &replica,
        &loaded.rows,
        &loaded.assignments,
        &queries,
        &truths,
        candidate_page_limit,
    )?;
    let output = (|| {
        write_batch(
            &request.evidence_output_path,
            v26_pq16_rerank_evidence_batch(&arms)?,
        )?;
        let (encoded_bytes, digest) = sha256_file(&request.evidence_output_path)?;
        let evidence = V26ObjectIdentity {
            role: "pq16-exact-rerank-evidence-parquet".to_owned(),
            uri: request.evidence_output_uri.clone(),
            digest_algorithm: "sha256".to_owned(),
            digest,
            encoded_bytes,
            generation: request.construction_rows.identity.generation.clone(),
        };
        let summaries = arms
            .iter()
            .map(|arm| {
                serde_json::json!({
                    "ranked_row_limit": arm.ranked_row_limit,
                    "projected_resident_bytes_100m": arm.projected_resident_bytes_100m,
                    "result": arm.result,
                })
            })
            .collect::<Vec<_>>();
        let value = serde_json::json!({
            "schema": "borsuk-v26-pq16-exact-rerank-output-v1",
            "candidate_page_limit": candidate_page_limit,
            "arms": summaries,
            "evidence": evidence,
            "page_body_reads": 0,
            "claim_eligible": false,
        });
        let mut bytes = serde_json::to_vec(&canonical_json_value(value))
            .map_err(|error| invalid(&format!("V26 PQ16 rerank serialization failed: {error}")))?;
        bytes.push(b'\n');
        Ok(bytes)
    })();
    if output.is_err() {
        let _ = fs::remove_file(&request.evidence_output_path);
    }
    output
}

fn v26_cold_vector_schema() -> Schema {
    Schema::new(vec![
        Field::new("source_ordinal", DataType::UInt64, false),
        Field::new("vector", vector_type(), false),
        Field::new("primary_page", DataType::UInt32, false),
        Field::new("replica_page", DataType::UInt32, false),
    ])
}

const V26_COLD_VECTOR_BATCH_ROWS: u32 = 65_536;

pub fn write_v26_cold_vectors_arrow(
    path: &Path,
    rows: &[V26ConstructionRow],
    assignments: &[V26RowPages],
    batch_rows: u32,
) -> Result<V26ColdVectorManifest> {
    if path.exists()
        || rows.is_empty()
        || rows.len() != assignments.len()
        || batch_rows != V26_COLD_VECTOR_BATCH_ROWS
        || rows.len() > usize::try_from(u32::MAX).unwrap()
    {
        return Err(invalid("V26 cold-vector write request differs"));
    }
    let result = (|| {
        for (index, (row, assignment)) in rows.iter().zip(assignments).enumerate() {
            if usize::try_from(row.source_ordinal).ok() != Some(index)
                || assignment.source_ordinal != row.source_ordinal
                || assignment.primary_page == assignment.replica_page
                || validate_v26_vector(&row.vector).is_err()
            {
                return Err(invalid("V26 cold-vector row authority differs"));
            }
        }
        let file = fs::File::create(path)
            .map_err(|error| invalid(&format!("V26 cold-vector create failed: {error}")))?;
        let mut writer = FileWriter::try_new(file, &v26_cold_vector_schema())
            .map_err(|error| invalid(&format!("V26 cold-vector writer failed: {error}")))?;
        for (batch_index, chunk) in rows
            .chunks(usize::try_from(batch_rows).unwrap())
            .enumerate()
        {
            let first = batch_index * usize::try_from(batch_rows).unwrap();
            let chunk_assignments = &assignments[first..first + chunk.len()];
            let values = chunk.iter().flat_map(|row| row.vector).collect::<Vec<_>>();
            let vectors = FixedSizeListArray::try_new(
                Arc::new(Field::new("element", DataType::Float32, false)),
                96,
                Arc::new(Float32Array::from(values)),
                None,
            )
            .map_err(|error| invalid(&format!("V26 cold-vector array failed: {error}")))?;
            let batch = RecordBatch::try_new(
                Arc::new(v26_cold_vector_schema()),
                vec![
                    Arc::new(UInt64Array::from_iter_values(
                        chunk.iter().map(|row| row.source_ordinal),
                    )),
                    Arc::new(vectors),
                    Arc::new(UInt32Array::from_iter_values(
                        chunk_assignments
                            .iter()
                            .map(|assignment| assignment.primary_page),
                    )),
                    Arc::new(UInt32Array::from_iter_values(
                        chunk_assignments
                            .iter()
                            .map(|assignment| assignment.replica_page),
                    )),
                ],
            )
            .map_err(|error| invalid(&format!("V26 cold-vector batch failed: {error}")))?;
            writer
                .write(&batch)
                .map_err(|error| invalid(&format!("V26 cold-vector write failed: {error}")))?;
        }
        writer
            .finish()
            .map_err(|error| invalid(&format!("V26 cold-vector finish failed: {error}")))?;
        let (encoded_bytes, sha256) = sha256_file(path)?;
        Ok(V26ColdVectorManifest {
            row_count: u64::try_from(rows.len()).unwrap(),
            batch_rows,
            encoded_bytes,
            sha256,
        })
    })();
    if result.is_err() {
        let _ = fs::remove_file(path);
    }
    result
}

impl V26ArrowColdVectors {
    pub fn open(path: &Path, manifest: &V26ColdVectorManifest) -> Result<Self> {
        if manifest.row_count == 0
            || manifest.batch_rows != V26_COLD_VECTOR_BATCH_ROWS
            || manifest.encoded_bytes == 0
            || !exact_lower_hex(&manifest.sha256, 64)
        {
            return Err(invalid("V26 cold-vector manifest differs"));
        }
        let (encoded_bytes, sha256) = sha256_file(path)?;
        if encoded_bytes != manifest.encoded_bytes || sha256 != manifest.sha256 {
            return Err(invalid("V26 cold-vector identity differs"));
        }
        let file = fs::File::open(path)
            .map_err(|error| invalid(&format!("V26 cold-vector open failed: {error}")))?;
        let file_len = file
            .metadata()
            .map_err(|error| invalid(&format!("V26 cold-vector metadata failed: {error}")))?
            .len();
        let mut trailer = [0_u8; 10];
        file.read_exact_at(
            &mut trailer,
            file_len
                .checked_sub(10)
                .ok_or_else(|| invalid("V26 cold-vector footer length differs"))?,
        )
        .map_err(|error| invalid(&format!("V26 cold-vector footer read failed: {error}")))?;
        let footer_len = read_footer_length(trailer)
            .map_err(|error| invalid(&format!("V26 cold-vector footer failed: {error}")))?;
        let mut footer_bytes = vec![0_u8; footer_len];
        file.read_exact_at(
            &mut footer_bytes,
            file_len
                .checked_sub(10 + u64::try_from(footer_len).unwrap())
                .ok_or_else(|| invalid("V26 cold-vector footer length differs"))?,
        )
        .map_err(|error| invalid(&format!("V26 cold-vector footer read failed: {error}")))?;
        let footer = root_as_footer(&footer_bytes)
            .map_err(|error| invalid(&format!("V26 cold-vector footer parse failed: {error}")))?;
        if footer
            .dictionaries()
            .is_some_and(|values| !values.is_empty())
        {
            return Err(invalid("V26 cold-vector dictionaries differ"));
        }
        let ipc_schema = footer
            .schema()
            .ok_or_else(|| invalid("V26 cold-vector schema is absent"))?;
        if !ipc_schema.endianness().equals_to_target_endianness() {
            return Err(invalid("V26 cold-vector endianness differs"));
        }
        let schema = Arc::new(fb_to_schema(ipc_schema));
        let blocks = footer
            .recordBatches()
            .ok_or_else(|| invalid("V26 cold-vector batches are absent"))?
            .iter()
            .copied()
            .collect::<Vec<_>>();
        let expected_batches = manifest.row_count.div_ceil(u64::from(manifest.batch_rows));
        if schema.as_ref() != &v26_cold_vector_schema()
            || u64::try_from(blocks.len()).unwrap() != expected_batches
        {
            return Err(invalid("V26 cold-vector schema differs"));
        }
        let mut batches = Vec::with_capacity(blocks.len());
        for (batch_index, block) in blocks.iter().enumerate() {
            let metadata_len = usize::try_from(block.metaDataLength())
                .map_err(|_| invalid("V26 cold-vector metadata length differs"))?;
            if metadata_len < 8 {
                return Err(invalid("V26 cold-vector metadata length differs"));
            }
            let block_offset = u64::try_from(block.offset())
                .map_err(|_| invalid("V26 cold-vector block offset differs"))?;
            let mut metadata = vec![0_u8; metadata_len];
            file.read_exact_at(&mut metadata, block_offset)
                .map_err(|error| {
                    invalid(&format!("V26 cold-vector metadata read failed: {error}"))
                })?;
            let message_start = if metadata[..4] == [0xff; 4] { 8 } else { 4 };
            let message = root_as_message(&metadata[message_start..]).map_err(|error| {
                invalid(&format!("V26 cold-vector metadata parse failed: {error}"))
            })?;
            let record = message
                .header_as_record_batch()
                .ok_or_else(|| invalid("V26 cold-vector record batch is absent"))?;
            let buffers = record
                .buffers()
                .ok_or_else(|| invalid("V26 cold-vector buffers are absent"))?;
            let row_start = u64::try_from(batch_index).unwrap() * u64::from(manifest.batch_rows);
            let row_count = manifest
                .row_count
                .saturating_sub(row_start)
                .min(u64::from(manifest.batch_rows));
            let validity_bytes = row_count.div_ceil(8);
            let vector_validity_bytes = (row_count * 96).div_ceil(8);
            let lengths = [
                validity_bytes,
                row_count * 8,
                validity_bytes,
                vector_validity_bytes,
                row_count * 96 * 4,
                validity_bytes,
                row_count * 4,
                validity_bytes,
                row_count * 4,
            ];
            if record.length() != i64::try_from(row_count).unwrap()
                || record.compression().is_some()
                || buffers.len() != lengths.len()
                || buffers.iter().zip(lengths).any(|(buffer, length)| {
                    u64::try_from(buffer.length()).ok() != Some(length) || buffer.offset() < 0
                })
            {
                let observed = buffers
                    .iter()
                    .map(|buffer| (buffer.offset(), buffer.length()))
                    .collect::<Vec<_>>();
                return Err(invalid(&format!(
                    "V26 cold-vector buffer layout differs: rows={} buffers={observed:?}",
                    record.length()
                )));
            }
            let body_start = block_offset
                .checked_add(u64::try_from(metadata_len).unwrap())
                .ok_or_else(|| invalid("V26 cold-vector body offset overflows"))?;
            let absolute = |index: usize| -> Result<u64> {
                body_start
                    .checked_add(u64::try_from(buffers.get(index).offset()).unwrap())
                    .ok_or_else(|| invalid("V26 cold-vector buffer offset overflows"))
            };
            batches.push(V26ColdVectorBatch {
                row_start,
                row_count: u32::try_from(row_count).unwrap(),
                ordinal_values_offset: absolute(1)?,
                vector_values_offset: absolute(4)?,
                primary_values_offset: absolute(6)?,
                replica_values_offset: absolute(8)?,
            });
        }
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(4)
            .thread_name(|index| format!("v26-cold-{index}"))
            .build()
            .map_err(|error| invalid(&format!("V26 cold-vector pool failed: {error}")))?;
        Ok(Self {
            file,
            batches,
            pool,
            row_count: manifest.row_count,
            batch_rows: manifest.batch_rows,
        })
    }

    fn batch_and_local(&self, row_id: u32) -> Result<(&V26ColdVectorBatch, u64)> {
        let batch_index = row_id / self.batch_rows;
        let batch = self
            .batches
            .get(usize::try_from(batch_index).unwrap())
            .ok_or_else(|| invalid("V26 cold-vector batch is absent"))?;
        let local = u64::from(row_id) - batch.row_start;
        if local >= u64::from(batch.row_count) {
            return Err(invalid("V26 cold-vector row is absent"));
        }
        Ok((batch, local))
    }

    fn read_vector(&self, row_id: u32) -> Result<[f32; 96]> {
        let (batch, local) = self.batch_and_local(row_id)?;
        let ordinal_bytes = self.read_bytes::<8>(batch.ordinal_values_offset + local * 8)?;
        if u64::from_le_bytes(ordinal_bytes) != u64::from(row_id) {
            return Err(invalid("V26 cold-vector ordinal binding differs"));
        }
        let bytes = self.read_bytes::<{ 96 * 4 }>(batch.vector_values_offset + local * 96 * 4)?;
        let mut vector = [0_f32; 96];
        for (value, encoded) in vector.iter_mut().zip(bytes.as_chunks::<4>().0) {
            *value = f32::from_le_bytes(*encoded);
        }
        validate_v26_vector(&vector)?;
        Ok(vector)
    }

    fn read_assignment(&self, row_id: u32) -> Result<V26RowPages> {
        let (batch, local) = self.batch_and_local(row_id)?;
        let read_page = |offset: u64| -> Result<u32> {
            Ok(u32::from_le_bytes(
                self.read_bytes::<4>(offset + local * 4)?,
            ))
        };
        let assignment = V26RowPages {
            source_ordinal: u64::from(row_id),
            primary_page: read_page(batch.primary_values_offset)?,
            replica_page: read_page(batch.replica_values_offset)?,
        };
        if assignment.primary_page == assignment.replica_page {
            return Err(invalid("V26 cold-vector assignment differs"));
        }
        Ok(assignment)
    }

    fn read_bytes<const N: usize>(&self, offset: u64) -> Result<[u8; N]> {
        let mut bytes = [0_u8; N];
        self.file
            .read_exact_at(&mut bytes, offset)
            .map_err(|error| invalid(&format!("V26 cold-vector sparse read failed: {error}")))?;
        Ok(bytes)
    }

    fn read_vectors(&self, row_ids: &[u32]) -> Result<V26ColdVectorSliceRead> {
        self.validate_row_ids(row_ids)?;
        let vectors = self.pool.install(|| {
            row_ids
                .par_iter()
                .map(|row_id| self.read_vector(*row_id))
                .collect::<Result<Vec<_>>>()
        })?;
        let batches_read = 1 + row_ids
            .windows(2)
            .filter(|pair| pair[0] / self.batch_rows != pair[1] / self.batch_rows)
            .count();
        Ok(V26ColdVectorSliceRead {
            vectors,
            batches_read: u32::try_from(batches_read).unwrap(),
            read_workers: u32::try_from(row_ids.len().min(4)).unwrap(),
        })
    }

    fn read_assignments(&self, row_ids: &[u32]) -> Result<Vec<V26RowPages>> {
        self.validate_row_ids(row_ids)?;
        self.pool.install(|| {
            row_ids
                .par_iter()
                .map(|row_id| self.read_assignment(*row_id))
                .collect::<Result<Vec<_>>>()
        })
    }

    fn validate_row_ids(&self, row_ids: &[u32]) -> Result<()> {
        if row_ids.is_empty()
            || row_ids.windows(2).any(|pair| pair[0] >= pair[1])
            || row_ids.iter().any(|row| u64::from(*row) >= self.row_count)
        {
            return Err(invalid("V26 cold-vector read request differs"));
        }
        Ok(())
    }

    pub fn read_rows(&self, row_ids: &[u32]) -> Result<V26ColdVectorRead> {
        let selected = self.read_vectors(row_ids)?;
        let assignments = self.read_assignments(row_ids)?;
        Ok(V26ColdVectorRead {
            vectors: selected.vectors,
            assignments,
            batches_read: selected.batches_read,
            read_workers: selected.read_workers,
        })
    }

    #[cfg(test)]
    fn decoded_batch_count(&self) -> u32 {
        0
    }

    #[cfg(test)]
    fn is_memory_mapped(&self) -> bool {
        false
    }
}

pub fn select_v26_pq16_pages_from_arrow(
    index: &crate::V26PackedPq16Index,
    candidate_pages: &[u32],
    query: &[f32; 96],
    cold_vectors: &V26ArrowColdVectors,
) -> Result<V26Pq16ServingSelection> {
    if u64::try_from(index.codes.len() / 16).unwrap() != cold_vectors.row_count {
        return Err(invalid("V26 PQ16 Arrow serving authority differs"));
    }
    let approximate = rank_v26_pq16_packed_candidates(index, candidate_pages, query, 512)?;
    let mut source_ordinals = approximate
        .iter()
        .map(|candidate| u32::try_from(candidate.source_ordinal))
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|_| invalid("V26 PQ16 Arrow source ordinal differs"))?;
    source_ordinals.sort_unstable();
    let cold = cold_vectors.read_vectors(&source_ordinals)?;
    let mut exact = approximate
        .iter()
        .map(|candidate| {
            let source_ordinal = u32::try_from(candidate.source_ordinal)
                .map_err(|_| invalid("V26 PQ16 Arrow source ordinal differs"))?;
            let position = source_ordinals
                .binary_search(&source_ordinal)
                .map_err(|_| invalid("V26 PQ16 Arrow cold-vector binding differs"))?;
            let distance = v26_squared_l2(&cold.vectors[position], query);
            if !distance.is_finite() {
                return Err(invalid("V26 PQ16 Arrow exact distance differs"));
            }
            Ok(V26PqRankedRow {
                source_ordinal: candidate.source_ordinal,
                distance,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    exact.sort_unstable();
    let mut exact_source_ordinals = exact[..10]
        .iter()
        .map(|row| u32::try_from(row.source_ordinal).unwrap())
        .collect::<Vec<_>>();
    exact_source_ordinals.sort_unstable();
    let exact_assignments = cold_vectors.read_assignments(&exact_source_ordinals)?;
    let ranked_assignments = exact[..10]
        .iter()
        .map(|row| {
            let source_ordinal = u32::try_from(row.source_ordinal).unwrap();
            let position = exact_source_ordinals
                .binary_search(&source_ordinal)
                .unwrap();
            let assignment = exact_assignments[position];
            if assignment.source_ordinal != row.source_ordinal {
                return Err(invalid("V26 PQ16 Arrow assignment binding differs"));
            }
            Ok(vec![assignment.primary_page, assignment.replica_page])
        })
        .collect::<Result<Vec<_>>>()?;
    let mut selected_pages =
        exact_v26_layout_oracle_pages(&ranked_assignments, crate::V26_SERVING_PAGE_BUDGET)?;
    for page in candidate_pages {
        if selected_pages.len() == crate::V26_SERVING_PAGE_BUDGET {
            break;
        }
        if !selected_pages.contains(page) {
            selected_pages.push(*page);
        }
    }
    if selected_pages.len() != crate::V26_SERVING_PAGE_BUDGET {
        return Err(invalid("V26 PQ16 Arrow serving page inventory differs"));
    }
    selected_pages.sort_unstable();
    Ok(V26Pq16ServingSelection {
        selected_pages,
        exact_rows_read: 512,
        cold_batches_read: cold.batches_read,
        cold_read_workers: cold.read_workers,
        page_body_reads: 0,
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct V26Pq16GlobalTimedSelection {
    selection: V26Pq16ServingSelection,
    global_adc_elapsed_ns: u64,
    exact_rerank_elapsed_ns: u64,
}

fn select_v26_pq16_global_pages_from_arrow_timed(
    index: &crate::V26PackedPq16Index,
    query: &[f32; 96],
    cold_vectors: &V26ArrowColdVectors,
    ranked_row_limit: usize,
) -> Result<V26Pq16GlobalTimedSelection> {
    if u64::try_from(index.codes.len() / 16).unwrap() != cold_vectors.row_count {
        return Err(invalid("V26 global PQ16 Arrow authority differs"));
    }
    let global_adc_started = std::time::Instant::now();
    let approximate = crate::rank_v26_pq16_global_candidates(index, query, ranked_row_limit)?;
    let global_adc_elapsed_ns = u64::try_from(global_adc_started.elapsed().as_nanos())
        .map_err(|_| invalid("V26 global PQ16 ADC latency overflows"))?
        .max(1);
    let exact_rerank_started = std::time::Instant::now();
    let mut source_ordinals = approximate
        .iter()
        .map(|candidate| u32::try_from(candidate.source_ordinal))
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|_| invalid("V26 global PQ16 Arrow source ordinal differs"))?;
    source_ordinals.sort_unstable();
    let cold = cold_vectors.read_vectors(&source_ordinals)?;
    let mut exact = approximate
        .iter()
        .map(|candidate| {
            let source_ordinal = u32::try_from(candidate.source_ordinal)
                .map_err(|_| invalid("V26 global PQ16 Arrow source ordinal differs"))?;
            let position = source_ordinals
                .binary_search(&source_ordinal)
                .map_err(|_| invalid("V26 global PQ16 Arrow binding differs"))?;
            let distance = v26_squared_l2(&cold.vectors[position], query);
            if !distance.is_finite() {
                return Err(invalid("V26 global PQ16 Arrow exact distance differs"));
            }
            Ok(V26PqRankedRow {
                source_ordinal: candidate.source_ordinal,
                distance,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    exact.sort_unstable();
    let assignment_limit = exact.len();
    let mut assignment_ordinals = exact[..assignment_limit]
        .iter()
        .map(|row| u32::try_from(row.source_ordinal).unwrap())
        .collect::<Vec<_>>();
    assignment_ordinals.sort_unstable();
    let assignments = cold_vectors.read_assignments(&assignment_ordinals)?;
    let ranked_assignments = exact[..assignment_limit]
        .iter()
        .map(|row| {
            let source_ordinal = u32::try_from(row.source_ordinal).unwrap();
            let position = assignment_ordinals.binary_search(&source_ordinal).unwrap();
            let assignment = assignments[position];
            if assignment.source_ordinal != row.source_ordinal {
                return Err(invalid("V26 global PQ16 Arrow assignment differs"));
            }
            Ok([assignment.primary_page, assignment.replica_page])
        })
        .collect::<Result<Vec<_>>>()?;
    let top_assignments = ranked_assignments[..10]
        .iter()
        .map(|pages| pages.to_vec())
        .collect::<Vec<_>>();
    let mut selected_pages =
        exact_v26_layout_oracle_pages(&top_assignments, crate::V26_SERVING_PAGE_BUDGET)?;
    for pages in &ranked_assignments {
        for page in pages {
            if selected_pages.len() == crate::V26_SERVING_PAGE_BUDGET {
                break;
            }
            if !selected_pages.contains(page) {
                selected_pages.push(*page);
            }
        }
    }
    if selected_pages.len() != crate::V26_SERVING_PAGE_BUDGET {
        return Err(invalid("V26 global PQ16 Arrow page inventory differs"));
    }
    selected_pages.sort_unstable();
    let exact_rerank_elapsed_ns = u64::try_from(exact_rerank_started.elapsed().as_nanos())
        .map_err(|_| invalid("V26 global PQ16 exact-rerank latency overflows"))?
        .max(1);
    Ok(V26Pq16GlobalTimedSelection {
        selection: V26Pq16ServingSelection {
            selected_pages,
            exact_rows_read: u32::try_from(ranked_row_limit)
                .map_err(|_| invalid("V26 global PQ16 Arrow ranked-row limit overflows"))?,
            cold_batches_read: cold.batches_read,
            cold_read_workers: cold.read_workers,
            page_body_reads: 0,
        },
        global_adc_elapsed_ns,
        exact_rerank_elapsed_ns,
    })
}

pub fn select_v26_pq16_global_pages_from_arrow(
    index: &crate::V26PackedPq16Index,
    query: &[f32; 96],
    cold_vectors: &V26ArrowColdVectors,
    ranked_row_limit: usize,
) -> Result<V26Pq16ServingSelection> {
    Ok(
        select_v26_pq16_global_pages_from_arrow_timed(
            index,
            query,
            cold_vectors,
            ranked_row_limit,
        )?
        .selection,
    )
}

/// Builds the deterministic SimHash/PQ16 multi-index by streaming authenticated Arrow vectors.
pub fn build_v26_simhash_pq16_multi_index_from_arrow(
    packed: &crate::V26PackedPq16Index,
    cold_vectors: &V26ArrowColdVectors,
) -> Result<crate::V26SimHashPq16MultiIndex> {
    const VECTOR_BYTES: usize = 96 * 4;
    if u64::try_from(packed.codes.len() / 16).unwrap() != cold_vectors.row_count
        || !packed.codes.len().is_multiple_of(16)
    {
        return Err(invalid("V26 SimHash PQ16 Arrow build authority differs"));
    }
    let row_count = usize::try_from(cold_vectors.row_count)
        .map_err(|_| invalid("V26 SimHash PQ16 Arrow row count overflows"))?;
    let mut signatures = Vec::with_capacity(row_count);
    for batch in &cold_vectors.batches {
        let batch_rows = usize::try_from(batch.row_count).unwrap();
        let mut ordinal_bytes = vec![0_u8; batch_rows * 8];
        cold_vectors
            .file
            .read_exact_at(&mut ordinal_bytes, batch.ordinal_values_offset)
            .map_err(|error| invalid(&format!("V26 SimHash ordinal read failed: {error}")))?;
        let mut vector_bytes = vec![0_u8; batch_rows * VECTOR_BYTES];
        cold_vectors
            .file
            .read_exact_at(&mut vector_bytes, batch.vector_values_offset)
            .map_err(|error| invalid(&format!("V26 SimHash vector read failed: {error}")))?;
        for row_index in 0..batch_rows {
            let ordinal_start = row_index * 8;
            let ordinal = u64::from_le_bytes(
                ordinal_bytes[ordinal_start..ordinal_start + 8]
                    .try_into()
                    .unwrap(),
            );
            if ordinal != batch.row_start + u64::try_from(row_index).unwrap() {
                return Err(invalid("V26 SimHash Arrow ordinal binding differs"));
            }
            let vector_start = row_index * VECTOR_BYTES;
            let vector_slice = &vector_bytes[vector_start..vector_start + VECTOR_BYTES];
            let mut vector = [0_f32; 96];
            for (coordinate, bytes) in vector.iter_mut().zip(vector_slice.as_chunks::<4>().0) {
                *coordinate = f32::from_le_bytes(*bytes);
            }
            signatures.push(crate::v26_simhash_signature(&vector)?);
        }
    }
    if signatures.len() != row_count {
        return Err(invalid("V26 SimHash Arrow vector inventory differs"));
    }
    crate::build_v26_simhash_pq16_multi_index_from_signatures(packed, &signatures)
}

/// Selects ten pages from bounded SimHash buckets with sparse exact Arrow reranking.
pub fn select_v26_simhash_pq16_pages_from_arrow(
    index: &crate::V26SimHashPq16MultiIndex,
    query: &[f32; 96],
    cold_vectors: &V26ArrowColdVectors,
    bucket_limit: usize,
    ranked_row_limit: usize,
) -> Result<V26Pq16ServingSelection> {
    if u64::try_from(index.source_ordinals.len()).unwrap() != cold_vectors.row_count {
        return Err(invalid("V26 SimHash PQ16 Arrow authority differs"));
    }
    let approximate =
        crate::rank_v26_simhash_pq16_candidates(index, query, bucket_limit, ranked_row_limit)?;
    let mut source_ordinals = approximate
        .iter()
        .map(|candidate| u32::try_from(candidate.source_ordinal))
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|_| invalid("V26 SimHash PQ16 Arrow source ordinal differs"))?;
    source_ordinals.sort_unstable();
    let cold = cold_vectors.read_vectors(&source_ordinals)?;
    let mut exact = approximate
        .iter()
        .map(|candidate| {
            let source_ordinal = u32::try_from(candidate.source_ordinal)
                .map_err(|_| invalid("V26 SimHash PQ16 Arrow source ordinal differs"))?;
            let position = source_ordinals
                .binary_search(&source_ordinal)
                .map_err(|_| invalid("V26 SimHash PQ16 Arrow vector binding differs"))?;
            let distance = v26_squared_l2(&cold.vectors[position], query);
            if !distance.is_finite() {
                return Err(invalid("V26 SimHash PQ16 Arrow exact distance differs"));
            }
            Ok(V26PqRankedRow {
                source_ordinal: candidate.source_ordinal,
                distance,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    exact.sort_unstable();
    let mut assignment_ordinals = exact
        .iter()
        .map(|row| u32::try_from(row.source_ordinal).unwrap())
        .collect::<Vec<_>>();
    assignment_ordinals.sort_unstable();
    let assignments = cold_vectors.read_assignments(&assignment_ordinals)?;
    let ranked_assignments = exact
        .iter()
        .map(|row| {
            let source_ordinal = u32::try_from(row.source_ordinal).unwrap();
            let position = assignment_ordinals.binary_search(&source_ordinal).unwrap();
            let assignment = assignments[position];
            if assignment.source_ordinal != row.source_ordinal {
                return Err(invalid("V26 SimHash PQ16 Arrow assignment differs"));
            }
            Ok([assignment.primary_page, assignment.replica_page])
        })
        .collect::<Result<Vec<_>>>()?;
    let top_assignments = ranked_assignments[..10]
        .iter()
        .map(|pages| pages.to_vec())
        .collect::<Vec<_>>();
    let mut selected_pages =
        exact_v26_layout_oracle_pages(&top_assignments, crate::V26_SERVING_PAGE_BUDGET)?;
    for pages in &ranked_assignments {
        for page in pages {
            if selected_pages.len() == crate::V26_SERVING_PAGE_BUDGET {
                break;
            }
            if !selected_pages.contains(page) {
                selected_pages.push(*page);
            }
        }
    }
    for page in 0..index.page_count {
        if selected_pages.len() == crate::V26_SERVING_PAGE_BUDGET {
            break;
        }
        if !selected_pages.contains(&page) {
            selected_pages.push(page);
        }
    }
    if selected_pages.len() != crate::V26_SERVING_PAGE_BUDGET {
        return Err(invalid("V26 SimHash PQ16 Arrow page inventory differs"));
    }
    selected_pages.sort_unstable();
    Ok(V26Pq16ServingSelection {
        selected_pages,
        exact_rows_read: u32::try_from(ranked_row_limit)
            .map_err(|_| invalid("V26 SimHash PQ16 Arrow ranked-row limit overflows"))?,
        cold_batches_read: cold.batches_read,
        cold_read_workers: cold.read_workers,
        page_body_reads: 0,
    })
}

/// Selects ten pages from two bounded PQ-key planes with sparse exact Arrow reranking.
pub fn select_v26_dual_pq_key_pages_from_arrow(
    index: &crate::V26DualPqKeyIndex,
    query: &[f32; 96],
    cold_vectors: &V26ArrowColdVectors,
    key_limit_per_plane: usize,
    ranked_row_limit: usize,
) -> Result<V26Pq16ServingSelection> {
    Ok(select_v26_dual_pq_key_pages_from_arrow_with_count(
        index,
        query,
        cold_vectors,
        key_limit_per_plane,
        ranked_row_limit,
    )?
    .0)
}

fn select_v26_dual_pq_key_pages_from_arrow_with_count(
    index: &crate::V26DualPqKeyIndex,
    query: &[f32; 96],
    cold_vectors: &V26ArrowColdVectors,
    key_limit_per_plane: usize,
    ranked_row_limit: usize,
) -> Result<(V26Pq16ServingSelection, u64)> {
    if u64::try_from(index.codes.len() / 16).unwrap() != cold_vectors.row_count {
        return Err(invalid("V26 dual PQ-key Arrow authority differs"));
    }
    let (approximate, unique_rows_scanned) = crate::rank_v26_dual_pq_key_candidates_with_count(
        index,
        query,
        key_limit_per_plane,
        ranked_row_limit,
    )?;
    let mut source_ordinals = approximate
        .iter()
        .map(|candidate| u32::try_from(candidate.source_ordinal))
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|_| invalid("V26 dual PQ-key Arrow source ordinal differs"))?;
    source_ordinals.sort_unstable();
    let cold = cold_vectors.read_vectors(&source_ordinals)?;
    let mut exact = approximate
        .iter()
        .map(|candidate| {
            let source_ordinal = u32::try_from(candidate.source_ordinal)
                .map_err(|_| invalid("V26 dual PQ-key Arrow source ordinal differs"))?;
            let position = source_ordinals
                .binary_search(&source_ordinal)
                .map_err(|_| invalid("V26 dual PQ-key Arrow vector binding differs"))?;
            let distance = v26_squared_l2(&cold.vectors[position], query);
            if !distance.is_finite() {
                return Err(invalid("V26 dual PQ-key Arrow exact distance differs"));
            }
            Ok(V26PqRankedRow {
                source_ordinal: candidate.source_ordinal,
                distance,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    exact.sort_unstable();
    let mut assignment_ordinals = exact
        .iter()
        .map(|row| u32::try_from(row.source_ordinal).unwrap())
        .collect::<Vec<_>>();
    assignment_ordinals.sort_unstable();
    let assignments = cold_vectors.read_assignments(&assignment_ordinals)?;
    let ranked_assignments = exact
        .iter()
        .map(|row| {
            let source_ordinal = u32::try_from(row.source_ordinal).unwrap();
            let position = assignment_ordinals.binary_search(&source_ordinal).unwrap();
            let assignment = assignments[position];
            if assignment.source_ordinal != row.source_ordinal {
                return Err(invalid("V26 dual PQ-key Arrow assignment differs"));
            }
            Ok([assignment.primary_page, assignment.replica_page])
        })
        .collect::<Result<Vec<_>>>()?;
    let top_assignments = ranked_assignments[..10]
        .iter()
        .map(|pages| pages.to_vec())
        .collect::<Vec<_>>();
    let mut selected_pages =
        exact_v26_layout_oracle_pages(&top_assignments, crate::V26_SERVING_PAGE_BUDGET)?;
    for pages in &ranked_assignments {
        for page in pages {
            if selected_pages.len() == crate::V26_SERVING_PAGE_BUDGET {
                break;
            }
            if !selected_pages.contains(page) {
                selected_pages.push(*page);
            }
        }
    }
    for page in 0..index.page_count {
        if selected_pages.len() == crate::V26_SERVING_PAGE_BUDGET {
            break;
        }
        if !selected_pages.contains(&page) {
            selected_pages.push(page);
        }
    }
    if selected_pages.len() != crate::V26_SERVING_PAGE_BUDGET {
        return Err(invalid("V26 dual PQ-key Arrow page inventory differs"));
    }
    selected_pages.sort_unstable();
    Ok((
        V26Pq16ServingSelection {
            selected_pages,
            exact_rows_read: u32::try_from(ranked_row_limit)
                .map_err(|_| invalid("V26 dual PQ-key Arrow ranked-row limit overflows"))?,
            cold_batches_read: cold.batches_read,
            cold_read_workers: cold.read_workers,
            page_body_reads: 0,
        },
        unique_rows_scanned,
    ))
}

fn execute_v26_simhash_preflight_samples(
    index: &crate::V26SimHashPq16MultiIndex,
    cold_vectors: &V26ArrowColdVectors,
    queries: &[V26ExternalQuery],
    truths: &[V26QueryTruth],
) -> Result<Vec<V26SimHashPreflightSample>> {
    const BUCKET_LIMITS: [usize; 3] = [137, 697, 2_517];
    if queries.len() != 32 || truths.len() != queries.len() {
        return Err(invalid("V26 SimHash preflight query inventory differs"));
    }
    let mut samples = Vec::with_capacity(BUCKET_LIMITS.len() * queries.len());
    for bucket_limit in BUCKET_LIMITS {
        for (query_index, (query, truth)) in queries.iter().zip(truths).enumerate() {
            if usize::try_from(query.query_ordinal).ok() != Some(query_index)
                || truth.query_ordinal != query.query_ordinal
                || truth.neighbor_source_ordinals.len() != 10
                || truth.ground_truth_page_assignments.len() != 10
                || truth
                    .ground_truth_page_assignments
                    .iter()
                    .any(|pages| pages.len() != 2 || pages[0] >= pages[1])
            {
                return Err(invalid("V26 SimHash preflight truth authority differs"));
            }
            let rows_scanned = crate::v26_simhash_rows_scanned(index, &query.vector, bucket_limit)?;
            let ranked_row_limit = usize::try_from(rows_scanned.min(2_048)).unwrap();
            if ranked_row_limit < 10 {
                return Err(invalid("V26 SimHash preflight candidate inventory differs"));
            }
            let started = std::time::Instant::now();
            let selection = select_v26_simhash_pq16_pages_from_arrow(
                index,
                &query.vector,
                cold_vectors,
                bucket_limit,
                ranked_row_limit,
            )?;
            let elapsed_ns = u64::try_from(started.elapsed().as_nanos())
                .map_err(|_| invalid("V26 SimHash preflight latency overflows"))?
                .max(1);
            let oracle_pages = exact_v26_layout_oracle_pages(
                &truth.ground_truth_page_assignments,
                crate::V26_SERVING_PAGE_BUDGET,
            )?;
            let hits = truth
                .ground_truth_page_assignments
                .iter()
                .filter(|pages| {
                    pages
                        .iter()
                        .any(|page| selection.selected_pages.binary_search(page).is_ok())
                })
                .count() as u32;
            let oracle_hits = truth
                .ground_truth_page_assignments
                .iter()
                .filter(|pages| {
                    pages
                        .iter()
                        .any(|page| oracle_pages.binary_search(page).is_ok())
                })
                .count() as u32;
            samples.push(V26SimHashPreflightSample {
                bucket_limit: u32::try_from(bucket_limit).unwrap(),
                query_ordinal: query.query_ordinal,
                selected_pages: selection.selected_pages,
                hits,
                oracle_hits,
                recall_ppm: u64::from(hits) * 100_000,
                oracle_attainment_ppm: u64::from(hits) * 1_000_000 / u64::from(oracle_hits),
                elapsed_ns,
                rows_scanned,
                cold_batches_read: selection.cold_batches_read,
            });
        }
    }
    Ok(samples)
}

/// Executes the fixed three-arm, 32-query SimHash/PQ16 truth-bound preflight.
pub fn evaluate_v26_simhash_preflight(
    index: &crate::V26SimHashPq16MultiIndex,
    cold_vectors: &V26ArrowColdVectors,
    queries: &[V26ExternalQuery],
    truths: &[V26QueryTruth],
    authority: V26SimHashPreflightAuthority,
) -> Result<(Vec<V26SimHashPreflightSample>, V26SimHashPreflightResult)> {
    let samples = execute_v26_simhash_preflight_samples(index, cold_vectors, queries, truths)?;
    let result = summarize_v26_simhash_preflight(authority, &samples)?;
    Ok((samples, result))
}

fn execute_v26_dual_pq_key_preflight_samples(
    index: &crate::V26DualPqKeyIndex,
    cold_vectors: &V26ArrowColdVectors,
    queries: &[V26ExternalQuery],
    truths: &[V26QueryTruth],
) -> Result<Vec<V26DualPqKeyPreflightSample>> {
    const KEY_LIMITS: [usize; 3] = [1_536, 4_096, 8_192];
    const RANKED_ROW_LIMIT: usize = 512;
    if queries.len() != 32 || truths.len() != queries.len() {
        return Err(invalid("V26 dual PQ-key preflight query inventory differs"));
    }
    let mut samples = Vec::with_capacity(KEY_LIMITS.len() * queries.len());
    for key_limit_per_plane in KEY_LIMITS {
        for (query_index, (query, truth)) in queries.iter().zip(truths).enumerate() {
            if usize::try_from(query.query_ordinal).ok() != Some(query_index)
                || truth.query_ordinal != query.query_ordinal
                || truth.neighbor_source_ordinals.len() != 10
                || truth.ground_truth_page_assignments.len() != 10
                || truth
                    .ground_truth_page_assignments
                    .iter()
                    .any(|pages| pages.len() != 2 || pages[0] >= pages[1])
            {
                return Err(invalid("V26 dual PQ-key preflight truth authority differs"));
            }
            let started = std::time::Instant::now();
            let (selection, unique_rows_scanned) =
                select_v26_dual_pq_key_pages_from_arrow_with_count(
                    index,
                    &query.vector,
                    cold_vectors,
                    key_limit_per_plane,
                    RANKED_ROW_LIMIT,
                )?;
            let elapsed_ns = u64::try_from(started.elapsed().as_nanos())
                .map_err(|_| invalid("V26 dual PQ-key latency overflows"))?
                .max(1);
            let oracle_pages = exact_v26_layout_oracle_pages(
                &truth.ground_truth_page_assignments,
                crate::V26_SERVING_PAGE_BUDGET,
            )?;
            let hits = truth
                .ground_truth_page_assignments
                .iter()
                .filter(|pages| {
                    pages
                        .iter()
                        .any(|page| selection.selected_pages.binary_search(page).is_ok())
                })
                .count() as u32;
            let oracle_hits = truth
                .ground_truth_page_assignments
                .iter()
                .filter(|pages| {
                    pages
                        .iter()
                        .any(|page| oracle_pages.binary_search(page).is_ok())
                })
                .count() as u32;
            samples.push(V26DualPqKeyPreflightSample {
                key_limit_per_plane: u32::try_from(key_limit_per_plane).unwrap(),
                ranked_row_limit: u32::try_from(RANKED_ROW_LIMIT).unwrap(),
                query_ordinal: query.query_ordinal,
                selected_pages: selection.selected_pages,
                hits,
                oracle_hits,
                recall_ppm: u64::from(hits) * 100_000,
                oracle_attainment_ppm: u64::from(hits) * 1_000_000 / u64::from(oracle_hits),
                elapsed_ns,
                unique_rows_scanned,
                cold_batches_read: selection.cold_batches_read,
            });
        }
    }
    Ok(samples)
}

pub fn evaluate_v26_dual_pq_key_preflight(
    index: &crate::V26DualPqKeyIndex,
    cold_vectors: &V26ArrowColdVectors,
    queries: &[V26ExternalQuery],
    truths: &[V26QueryTruth],
    authority: V26DualPqKeyPreflightAuthority,
) -> Result<(
    Vec<V26DualPqKeyPreflightSample>,
    V26DualPqKeyPreflightResult,
)> {
    let samples = execute_v26_dual_pq_key_preflight_samples(index, cold_vectors, queries, truths)?;
    let result = summarize_v26_dual_pq_key_preflight(authority, &samples)?;
    Ok((samples, result))
}

fn v26_simhash_preflight_schema() -> Schema {
    Schema::new(vec![
        Field::new("bucket_limit", DataType::UInt32, false),
        Field::new("query_ordinal", DataType::UInt32, false),
        Field::new(
            "selected_pages",
            DataType::FixedSizeList(Arc::new(Field::new("element", DataType::UInt32, false)), 10),
            false,
        ),
        Field::new("hits", DataType::UInt32, false),
        Field::new("oracle_hits", DataType::UInt32, false),
        Field::new("recall_ppm", DataType::UInt64, false),
        Field::new("oracle_attainment_ppm", DataType::UInt64, false),
        Field::new("elapsed_ns", DataType::UInt64, false),
        Field::new("rows_scanned", DataType::UInt64, false),
        Field::new("cold_batches_read", DataType::UInt32, false),
    ])
}

fn v26_simhash_preflight_batch(samples: &[V26SimHashPreflightSample]) -> Result<RecordBatch> {
    if samples.len() != 96 {
        return Err(invalid("V26 SimHash preflight evidence inventory differs"));
    }
    let selected_pages = FixedSizeListArray::try_new(
        Arc::new(Field::new("element", DataType::UInt32, false)),
        10,
        Arc::new(UInt32Array::from_iter_values(
            samples
                .iter()
                .flat_map(|sample| sample.selected_pages.iter().copied()),
        )),
        None,
    )
    .map_err(|error| invalid(&format!("V26 SimHash preflight pages failed: {error}")))?;
    RecordBatch::try_new(
        Arc::new(v26_simhash_preflight_schema()),
        vec![
            Arc::new(UInt32Array::from_iter_values(
                samples.iter().map(|sample| sample.bucket_limit),
            )),
            Arc::new(UInt32Array::from_iter_values(
                samples.iter().map(|sample| sample.query_ordinal),
            )),
            Arc::new(selected_pages),
            Arc::new(UInt32Array::from_iter_values(
                samples.iter().map(|sample| sample.hits),
            )),
            Arc::new(UInt32Array::from_iter_values(
                samples.iter().map(|sample| sample.oracle_hits),
            )),
            Arc::new(UInt64Array::from_iter_values(
                samples.iter().map(|sample| sample.recall_ppm),
            )),
            Arc::new(UInt64Array::from_iter_values(
                samples.iter().map(|sample| sample.oracle_attainment_ppm),
            )),
            Arc::new(UInt64Array::from_iter_values(
                samples.iter().map(|sample| sample.elapsed_ns),
            )),
            Arc::new(UInt64Array::from_iter_values(
                samples.iter().map(|sample| sample.rows_scanned),
            )),
            Arc::new(UInt32Array::from_iter_values(
                samples.iter().map(|sample| sample.cold_batches_read),
            )),
        ],
    )
    .map_err(|error| invalid(&format!("V26 SimHash preflight batch failed: {error}")))
}

fn v26_dual_pq_key_preflight_schema() -> Schema {
    Schema::new(vec![
        Field::new("key_limit_per_plane", DataType::UInt32, false),
        Field::new("ranked_row_limit", DataType::UInt32, false),
        Field::new("query_ordinal", DataType::UInt32, false),
        Field::new(
            "selected_pages",
            DataType::FixedSizeList(Arc::new(Field::new("element", DataType::UInt32, false)), 10),
            false,
        ),
        Field::new("hits", DataType::UInt32, false),
        Field::new("oracle_hits", DataType::UInt32, false),
        Field::new("recall_ppm", DataType::UInt64, false),
        Field::new("oracle_attainment_ppm", DataType::UInt64, false),
        Field::new("elapsed_ns", DataType::UInt64, false),
        Field::new("unique_rows_scanned", DataType::UInt64, false),
        Field::new("cold_batches_read", DataType::UInt32, false),
    ])
}

fn v26_dual_pq_key_preflight_batch(samples: &[V26DualPqKeyPreflightSample]) -> Result<RecordBatch> {
    if samples.len() != 96 {
        return Err(invalid("V26 dual PQ-key evidence inventory differs"));
    }
    let selected_pages = FixedSizeListArray::try_new(
        Arc::new(Field::new("element", DataType::UInt32, false)),
        10,
        Arc::new(UInt32Array::from_iter_values(
            samples
                .iter()
                .flat_map(|sample| sample.selected_pages.iter().copied()),
        )),
        None,
    )
    .map_err(|error| invalid(&format!("V26 dual PQ-key pages failed: {error}")))?;
    RecordBatch::try_new(
        Arc::new(v26_dual_pq_key_preflight_schema()),
        vec![
            Arc::new(UInt32Array::from_iter_values(
                samples.iter().map(|sample| sample.key_limit_per_plane),
            )),
            Arc::new(UInt32Array::from_iter_values(
                samples.iter().map(|sample| sample.ranked_row_limit),
            )),
            Arc::new(UInt32Array::from_iter_values(
                samples.iter().map(|sample| sample.query_ordinal),
            )),
            Arc::new(selected_pages),
            Arc::new(UInt32Array::from_iter_values(
                samples.iter().map(|sample| sample.hits),
            )),
            Arc::new(UInt32Array::from_iter_values(
                samples.iter().map(|sample| sample.oracle_hits),
            )),
            Arc::new(UInt64Array::from_iter_values(
                samples.iter().map(|sample| sample.recall_ppm),
            )),
            Arc::new(UInt64Array::from_iter_values(
                samples.iter().map(|sample| sample.oracle_attainment_ppm),
            )),
            Arc::new(UInt64Array::from_iter_values(
                samples.iter().map(|sample| sample.elapsed_ns),
            )),
            Arc::new(UInt64Array::from_iter_values(
                samples.iter().map(|sample| sample.unique_rows_scanned),
            )),
            Arc::new(UInt32Array::from_iter_values(
                samples.iter().map(|sample| sample.cold_batches_read),
            )),
        ],
    )
    .map_err(|error| invalid(&format!("V26 dual PQ-key batch failed: {error}")))
}

pub fn run_v26_simhash_preflight(request: &V26SimHashPreflightRequest) -> Result<Vec<u8>> {
    if request.evidence_output_path.exists()
        || !request.evidence_output_uri.starts_with("s3://")
        || !request.evidence_output_uri.ends_with(".parquet")
    {
        return Err(invalid("V26 SimHash preflight request differs"));
    }
    let manifest = read_v26_pq16_serving_manifest(&request.serving_manifest)?;
    let terminal = read_layout_terminal(&request.layout_terminal)?;
    authenticate(&request.external_queries, "external-queries-parquet")?;
    authenticate(&request.truth, "truth-parquet")?;
    let generation = &terminal.authority.generation;
    let mut uris = BTreeSet::new();
    if manifest.inputs[0] != terminal.authority.construction_rows
        || manifest.inputs[2] != request.layout_terminal.identity
        || manifest.row_count != terminal.row_count
        || manifest.page_count != terminal.page_count
        || [
            &request.serving_manifest.identity,
            &request.layout_terminal.identity,
            &request.external_queries.identity,
            &request.truth.identity,
        ]
        .iter()
        .any(|identity| identity.generation != *generation || !uris.insert(&identity.uri))
        || !uris.insert(&request.evidence_output_uri)
    {
        return Err(invalid("V26 SimHash preflight authority differs"));
    }
    let expected_names = v26_pq16_serving_output_names()
        .into_iter()
        .chain(std::iter::once("serving-manifest.json"))
        .map(str::to_owned)
        .collect::<BTreeSet<_>>();
    let observed_names = fs::read_dir(&request.serving_dir)
        .map_err(|error| invalid(&format!("V26 SimHash directory read failed: {error}")))?
        .map(|entry| {
            entry
                .map_err(|error| invalid(&format!("V26 SimHash directory read failed: {error}")))
                .and_then(|entry| {
                    entry
                        .file_name()
                        .into_string()
                        .map_err(|_| invalid("V26 SimHash artifact name differs"))
                })
        })
        .collect::<Result<BTreeSet<_>>>()?;
    if observed_names != expected_names {
        return Err(invalid("V26 SimHash artifact inventory differs"));
    }
    let index = read_v26_simhash_pq16_index_arrow(&request.serving_dir, &manifest.simhash_index)?;
    let cold_vectors = V26ArrowColdVectors::open(
        &request.serving_dir.join("cold-vectors.arrow"),
        &manifest.cold_vectors,
    )?;
    let mut queries = read_evaluation_queries(&request.external_queries.path, 512)?;
    let mut truths = read_evaluation_truth_with_assignment(
        &request.truth.path,
        512,
        &queries,
        &terminal.authority.construction_rows.digest,
        &request.external_queries.identity.digest,
        |neighbor| {
            let neighbor =
                u32::try_from(neighbor).map_err(|_| invalid("V26 SimHash truth source differs"))?;
            cold_vectors.read_assignment(neighbor)
        },
    )?;
    queries.truncate(32);
    truths.truncate(32);
    let samples = execute_v26_simhash_preflight_samples(&index, &cold_vectors, &queries, &truths)?;
    let result = (|| {
        write_batch(
            &request.evidence_output_path,
            v26_simhash_preflight_batch(&samples)?,
        )?;
        let evidence = output_identity(
            "simhash-preflight-evidence-parquet",
            &request.evidence_output_path,
            &request.evidence_output_uri[..request.evidence_output_uri.rfind('/').unwrap() + 1],
            generation,
        )?;
        if evidence.uri != request.evidence_output_uri {
            return Err(invalid("V26 SimHash evidence URI differs"));
        }
        let authority = V26SimHashPreflightAuthority {
            serving_manifest: request.serving_manifest.identity.clone(),
            external_queries: request.external_queries.identity.clone(),
            truth: request.truth.identity.clone(),
            evidence,
        };
        let result = summarize_v26_simhash_preflight(authority, &samples)?;
        canonical_v26_simhash_preflight_result_bytes(&result, &samples)
    })();
    if result.is_err() {
        let _ = fs::remove_file(&request.evidence_output_path);
    }
    result
}

/// Runs the fixed, offline dual-PQ-key truth preflight over authenticated Arrow artifacts.
pub fn run_v26_dual_pq_key_preflight(request: &V26DualPqKeyPreflightRequest) -> Result<Vec<u8>> {
    if request.evidence_output_path.exists()
        || !request.offsets_uri.starts_with("s3://")
        || !request.offsets_uri.ends_with("pq16-dual-key-offsets.arrow")
        || !request.ordinals_uri.starts_with("s3://")
        || !request
            .ordinals_uri
            .ends_with("pq16-dual-key-ordinals.arrow")
        || !request.evidence_output_uri.starts_with("s3://")
        || !request.evidence_output_uri.ends_with(".parquet")
    {
        return Err(invalid("V26 dual PQ-key preflight request differs"));
    }
    let manifest = read_v26_pq16_serving_manifest(&request.serving_manifest)?;
    let terminal = read_layout_terminal(&request.layout_terminal)?;
    authenticate(&request.external_queries, "external-queries-parquet")?;
    authenticate(&request.truth, "truth-parquet")?;
    let generation = &terminal.authority.generation;
    let mut uris = BTreeSet::new();
    if manifest.inputs[0] != terminal.authority.construction_rows
        || manifest.inputs[2] != request.layout_terminal.identity
        || manifest.row_count != terminal.row_count
        || manifest.page_count != terminal.page_count
        || request.dual_index.row_count != manifest.row_count
        || [
            &request.serving_manifest.identity,
            &request.layout_terminal.identity,
            &request.external_queries.identity,
            &request.truth.identity,
        ]
        .iter()
        .any(|identity| identity.generation != *generation || !uris.insert(&identity.uri))
        || !uris.insert(&request.offsets_uri)
        || !uris.insert(&request.ordinals_uri)
        || !uris.insert(&request.evidence_output_uri)
    {
        return Err(invalid("V26 dual PQ-key preflight authority differs"));
    }
    let expected_names = v26_pq16_serving_output_names()
        .into_iter()
        .chain(std::iter::once("serving-manifest.json"))
        .map(str::to_owned)
        .collect::<BTreeSet<_>>();
    let observed_names = fs::read_dir(&request.serving_dir)
        .map_err(|error| invalid(&format!("V26 dual PQ-key directory read failed: {error}")))?
        .map(|entry| {
            entry
                .map_err(|error| {
                    invalid(&format!("V26 dual PQ-key directory read failed: {error}"))
                })
                .and_then(|entry| {
                    entry
                        .file_name()
                        .into_string()
                        .map_err(|_| invalid("V26 dual PQ-key artifact name differs"))
                })
        })
        .collect::<Result<BTreeSet<_>>>()?;
    if observed_names != expected_names {
        return Err(invalid("V26 dual PQ-key artifact inventory differs"));
    }
    let packed = read_v26_pq16_index_arrow(&request.serving_dir, &manifest.index)?;
    let index =
        read_v26_dual_pq_key_index_arrow(&request.dual_index_dir, &request.dual_index, &packed)?;
    let cold_vectors = V26ArrowColdVectors::open(
        &request.serving_dir.join("cold-vectors.arrow"),
        &manifest.cold_vectors,
    )?;
    let mut queries = read_evaluation_queries(&request.external_queries.path, 512)?;
    let mut truths = read_evaluation_truth_with_assignment(
        &request.truth.path,
        512,
        &queries,
        &terminal.authority.construction_rows.digest,
        &request.external_queries.identity.digest,
        |neighbor| {
            let neighbor = u32::try_from(neighbor)
                .map_err(|_| invalid("V26 dual PQ-key truth source differs"))?;
            cold_vectors.read_assignment(neighbor)
        },
    )?;
    queries.truncate(32);
    truths.truncate(32);
    let samples =
        execute_v26_dual_pq_key_preflight_samples(&index, &cold_vectors, &queries, &truths)?;
    let result = (|| {
        write_batch(
            &request.evidence_output_path,
            v26_dual_pq_key_preflight_batch(&samples)?,
        )?;
        let evidence = output_identity(
            "dual-pq-key-preflight-evidence-parquet",
            &request.evidence_output_path,
            &request.evidence_output_uri[..request.evidence_output_uri.rfind('/').unwrap() + 1],
            generation,
        )?;
        if evidence.uri != request.evidence_output_uri {
            return Err(invalid("V26 dual PQ-key evidence URI differs"));
        }
        let arrow_identity =
            |role: &str, uri: &str, identity: &V26ArrowFileIdentity| -> V26ObjectIdentity {
                V26ObjectIdentity {
                    role: role.to_owned(),
                    uri: uri.to_owned(),
                    digest_algorithm: "sha256".to_owned(),
                    digest: identity.sha256.clone(),
                    encoded_bytes: identity.encoded_bytes,
                    generation: generation.clone(),
                }
            };
        let authority = V26DualPqKeyPreflightAuthority {
            serving_manifest: request.serving_manifest.identity.clone(),
            external_queries: request.external_queries.identity.clone(),
            truth: request.truth.identity.clone(),
            offsets: arrow_identity(
                "dual-pq-key-offsets-arrow",
                &request.offsets_uri,
                &request.dual_index.offsets,
            ),
            ordinals: arrow_identity(
                "dual-pq-key-ordinals-arrow",
                &request.ordinals_uri,
                &request.dual_index.ordinals,
            ),
            evidence,
        };
        let result = summarize_v26_dual_pq_key_preflight(authority, &samples)?;
        canonical_v26_dual_pq_key_preflight_result_bytes(&result, &samples)
    })();
    if result.is_err() {
        let _ = fs::remove_file(&request.evidence_output_path);
    }
    result
}

fn v26_pq16_codebook_schema() -> Schema {
    Schema::new(vec![
        Field::new("subspace", DataType::UInt8, false),
        Field::new("centroid", DataType::UInt16, false),
        Field::new(
            "values",
            DataType::FixedSizeList(Arc::new(Field::new("element", DataType::Float32, false)), 6),
            false,
        ),
    ])
}

fn v26_pq4_fast_codebook_schema() -> Schema {
    Schema::new(vec![Field::new(
        "centroids",
        DataType::FixedSizeList(
            Arc::new(Field::new("element", DataType::Float32, false)),
            1_536,
        ),
        false,
    )])
}

fn v26_pq4_fast_codes_schema() -> Schema {
    Schema::new(vec![
        Field::new("block_ordinal", DataType::UInt64, false),
        Field::new("packed_codes", DataType::FixedSizeBinary(512), false),
    ])
}

fn validate_v26_pq4_fast_manifest(manifest: &V26Pq4FastManifest) -> Result<()> {
    let identities = [
        (&manifest.construction_rows, "construction-parquet"),
        (&manifest.page_assignments, "page-assignments-parquet"),
        (&manifest.layout_terminal, "layout-terminal"),
        (&manifest.codebook, "pq4-fast-codebook-arrow"),
        (&manifest.codes, "pq4-fast-codes-arrow"),
    ];
    let generation = &manifest.construction_rows.generation;
    let mut uris = BTreeSet::new();
    for (identity, role) in identities {
        validate_v26_benchmark_identity(identity, role)?;
        if identity.generation != *generation || !uris.insert(&identity.uri) {
            return Err(invalid("V26 PQ4 manifest identity differs"));
        }
    }
    let expected_blocks = manifest.row_count.div_ceil(32);
    let expected_padding = u32::try_from(expected_blocks * 32 - manifest.row_count).unwrap();
    if generation.is_empty()
        || !manifest.codebook.uri.ends_with("/pq4-fast-codebook.arrow")
        || !manifest.codes.uri.ends_with("/pq4-fast-codes.arrow")
        || manifest.schema != "borsuk-v26-pq4-fast-manifest-v1"
        || manifest.row_count == 0
        || manifest.row_count > u64::from(u32::MAX)
        || manifest.block_count != expected_blocks
        || manifest.padding_rows != expected_padding
        || manifest.dimension != 96
        || manifest.subquantizer_count != 32
        || manifest.subspace_dimensions != 3
        || manifest.centroid_count != 16
        || manifest.block_rows != 32
        || manifest.code_bytes_per_row != 16
        || manifest.byte_order != "subquantizer-major"
        || manifest.nibble_order != "even-low-odd-high"
        || manifest.source_order != "ascending-source-ordinal"
        || manifest.projected_resident_bytes_100m
            != crate::projected_v26_pq4_fast_resident_bytes(100_000_000)?
    {
        return Err(invalid("V26 PQ4 manifest differs"));
    }
    Ok(())
}

pub fn canonical_v26_pq4_fast_manifest_bytes(manifest: &V26Pq4FastManifest) -> Result<Vec<u8>> {
    validate_v26_pq4_fast_manifest(manifest)?;
    let value = serde_json::to_value(manifest)
        .map_err(|error| invalid(&format!("V26 PQ4 manifest failed: {error}")))?;
    let mut bytes = serde_json::to_vec(&canonical_json_value(value))
        .map_err(|error| invalid(&format!("V26 PQ4 manifest failed: {error}")))?;
    bytes.push(b'\n');
    Ok(bytes)
}

pub fn write_v26_pq4_fast_index_arrow(
    directory: &Path,
    index: &crate::V26Pq4FastIndex,
    construction_rows: &V26ObjectIdentity,
    page_assignments: &V26ObjectIdentity,
    layout_terminal: &V26ObjectIdentity,
    output_uri_prefix: &str,
) -> Result<V26Pq4FastManifest> {
    let codebook_path = directory.join("pq4-fast-codebook.arrow");
    let codes_path = directory.join("pq4-fast-codes.arrow");
    let generation = &construction_rows.generation;
    if !directory.is_dir()
        || codebook_path.exists()
        || codes_path.exists()
        || !output_uri_prefix.starts_with("s3://")
        || !output_uri_prefix.ends_with('/')
        || index.row_count == 0
        || index.blocks.len() != usize::try_from(index.row_count).unwrap().div_ceil(32)
        || index.codebook.centroids.len() != 32
        || [construction_rows, page_assignments, layout_terminal]
            .iter()
            .any(|identity| identity.generation != *generation)
    {
        return Err(invalid("V26 PQ4 Arrow write request differs"));
    }
    for (identity, role) in [
        (construction_rows, "construction-parquet"),
        (page_assignments, "page-assignments-parquet"),
        (layout_terminal, "layout-terminal"),
    ] {
        validate_v26_benchmark_identity(identity, role)?;
    }
    let result = (|| {
        let values = index
            .codebook
            .centroids
            .iter()
            .flat_map(|centroids| centroids.iter().copied())
            .collect::<Vec<_>>();
        let centroids = FixedSizeListArray::try_new(
            Arc::new(Field::new("element", DataType::Float32, false)),
            1_536,
            Arc::new(Float32Array::from(values)),
            None,
        )
        .map_err(|error| invalid(&format!("V26 PQ4 codebook values failed: {error}")))?;
        let file = fs::File::create(&codebook_path)
            .map_err(|error| invalid(&format!("V26 PQ4 codebook create failed: {error}")))?;
        let mut writer = FileWriter::try_new(file, &v26_pq4_fast_codebook_schema())
            .map_err(|error| invalid(&format!("V26 PQ4 codebook writer failed: {error}")))?;
        writer
            .write(
                &RecordBatch::try_new(
                    Arc::new(v26_pq4_fast_codebook_schema()),
                    vec![Arc::new(centroids)],
                )
                .map_err(|error| invalid(&format!("V26 PQ4 codebook batch failed: {error}")))?,
            )
            .map_err(|error| invalid(&format!("V26 PQ4 codebook write failed: {error}")))?;
        writer
            .finish()
            .map_err(|error| invalid(&format!("V26 PQ4 codebook finish failed: {error}")))?;

        let file = fs::File::create(&codes_path)
            .map_err(|error| invalid(&format!("V26 PQ4 codes create failed: {error}")))?;
        let mut writer = FileWriter::try_new(file, &v26_pq4_fast_codes_schema())
            .map_err(|error| invalid(&format!("V26 PQ4 codes writer failed: {error}")))?;
        for (batch_index, blocks) in index.blocks.chunks(65_536).enumerate() {
            let first = batch_index * 65_536;
            let packed = FixedSizeBinaryArray::try_from_iter(blocks.iter())
                .map_err(|error| invalid(&format!("V26 PQ4 codes array failed: {error}")))?;
            writer
                .write(
                    &RecordBatch::try_new(
                        Arc::new(v26_pq4_fast_codes_schema()),
                        vec![
                            Arc::new(UInt64Array::from_iter_values(
                                (first..first + blocks.len()).map(|value| value as u64),
                            )),
                            Arc::new(packed),
                        ],
                    )
                    .map_err(|error| invalid(&format!("V26 PQ4 codes batch failed: {error}")))?,
                )
                .map_err(|error| invalid(&format!("V26 PQ4 codes write failed: {error}")))?;
        }
        writer
            .finish()
            .map_err(|error| invalid(&format!("V26 PQ4 codes finish failed: {error}")))?;

        let manifest = V26Pq4FastManifest {
            schema: "borsuk-v26-pq4-fast-manifest-v1".to_owned(),
            construction_rows: construction_rows.clone(),
            page_assignments: page_assignments.clone(),
            layout_terminal: layout_terminal.clone(),
            codebook: output_identity(
                "pq4-fast-codebook-arrow",
                &codebook_path,
                output_uri_prefix,
                generation,
            )?,
            codes: output_identity(
                "pq4-fast-codes-arrow",
                &codes_path,
                output_uri_prefix,
                generation,
            )?,
            row_count: index.row_count,
            block_count: index.blocks.len() as u64,
            padding_rows: u32::try_from(index.blocks.len() * 32).unwrap()
                - u32::try_from(index.row_count).unwrap(),
            dimension: 96,
            subquantizer_count: 32,
            subspace_dimensions: 3,
            centroid_count: 16,
            block_rows: 32,
            code_bytes_per_row: 16,
            byte_order: "subquantizer-major".to_owned(),
            nibble_order: "even-low-odd-high".to_owned(),
            source_order: "ascending-source-ordinal".to_owned(),
            projected_resident_bytes_100m: index.projected_resident_bytes_100m,
        };
        canonical_v26_pq4_fast_manifest_bytes(&manifest)?;
        Ok(manifest)
    })();
    if result.is_err() {
        let _ = fs::remove_file(&codebook_path);
        let _ = fs::remove_file(&codes_path);
    }
    result
}

pub fn read_v26_pq4_fast_index_arrow(
    directory: &Path,
    manifest: &V26Pq4FastManifest,
) -> Result<crate::V26Pq4FastIndex> {
    validate_v26_pq4_fast_manifest(manifest)?;
    let mut codebook_reader = authenticate_arrow_file(
        &directory.join("pq4-fast-codebook.arrow"),
        &V26ArrowFileIdentity {
            encoded_bytes: manifest.codebook.encoded_bytes,
            sha256: manifest.codebook.digest.clone(),
        },
    )?;
    if codebook_reader.schema().as_ref() != &v26_pq4_fast_codebook_schema() {
        return Err(invalid("V26 PQ4 codebook schema differs"));
    }
    let batch = codebook_reader
        .next()
        .ok_or_else(|| invalid("V26 PQ4 codebook inventory differs"))?
        .map_err(|error| invalid(&format!("V26 PQ4 codebook read failed: {error}")))?;
    if batch.num_rows() != 1 || codebook_reader.next().is_some() {
        return Err(invalid("V26 PQ4 codebook inventory differs"));
    }
    let list = batch
        .column(0)
        .as_any()
        .downcast_ref::<FixedSizeListArray>()
        .ok_or_else(|| invalid("V26 PQ4 codebook type differs"))?;
    let values = list
        .values()
        .as_any()
        .downcast_ref::<Float32Array>()
        .ok_or_else(|| invalid("V26 PQ4 codebook value type differs"))?;
    let centroids = values
        .values()
        .chunks_exact(48)
        .map(|chunk| {
            let mut value = [0.0_f32; 48];
            value.copy_from_slice(chunk);
            value
        })
        .collect::<Vec<_>>();
    if centroids.len() != 32 || centroids.iter().flatten().any(|value| !value.is_finite()) {
        return Err(invalid("V26 PQ4 codebook values differ"));
    }

    let mut codes_reader = authenticate_arrow_file(
        &directory.join("pq4-fast-codes.arrow"),
        &V26ArrowFileIdentity {
            encoded_bytes: manifest.codes.encoded_bytes,
            sha256: manifest.codes.digest.clone(),
        },
    )?;
    if codes_reader.schema().as_ref() != &v26_pq4_fast_codes_schema() {
        return Err(invalid("V26 PQ4 codes schema differs"));
    }
    let mut blocks = Vec::with_capacity(usize::try_from(manifest.block_count).unwrap());
    for batch in &mut codes_reader {
        let batch =
            batch.map_err(|error| invalid(&format!("V26 PQ4 codes read failed: {error}")))?;
        let ordinals = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| invalid("V26 PQ4 block ordinal type differs"))?;
        let packed = batch
            .column(1)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .ok_or_else(|| invalid("V26 PQ4 block type differs"))?;
        for row in 0..batch.num_rows() {
            if ordinals.value(row) != blocks.len() as u64 {
                return Err(invalid("V26 PQ4 block order differs"));
            }
            let mut block = [0_u8; 512];
            block.copy_from_slice(packed.value(row));
            blocks.push(block);
        }
    }
    if blocks.len() as u64 != manifest.block_count {
        return Err(invalid("V26 PQ4 block inventory differs"));
    }
    let used_in_last = usize::try_from(manifest.row_count).unwrap() % 32;
    if used_in_last != 0 {
        let last = blocks.last().unwrap();
        for row in used_in_last..32 {
            if (0..32).any(|subspace| {
                let value = last[subspace * 16 + row / 2];
                if row.is_multiple_of(2) {
                    value & 15 != 0
                } else {
                    value >> 4 != 0
                }
            }) {
                return Err(invalid("V26 PQ4 padding differs"));
            }
        }
    }
    Ok(crate::V26Pq4FastIndex {
        codebook: crate::V26Pq4FastCodebook { centroids },
        blocks,
        row_count: manifest.row_count,
        projected_resident_bytes_100m: manifest.projected_resident_bytes_100m,
    })
}

fn v26_pq16_codes_schema() -> Schema {
    Schema::new(vec![
        Field::new("source_ordinal", DataType::UInt32, false),
        Field::new("code", DataType::FixedSizeBinary(16), false),
    ])
}

fn v26_pq16_postings_schema() -> Schema {
    Schema::new(vec![
        Field::new("page_ordinal", DataType::UInt32, false),
        Field::new("source_ordinal", DataType::UInt32, false),
    ])
}

fn v26_simhash_buckets_schema() -> Schema {
    Schema::new(vec![
        Field::new("bucket_ordinal", DataType::UInt32, false),
        Field::new("row_start", DataType::UInt64, false),
        Field::new("row_end", DataType::UInt64, false),
    ])
}

fn v26_simhash_records_schema() -> Schema {
    Schema::new(vec![
        Field::new("source_ordinal", DataType::UInt32, false),
        Field::new("pq16_code", DataType::FixedSizeBinary(16), false),
    ])
}

fn v26_dual_pq_key_offsets_schema() -> Schema {
    Schema::new(vec![
        Field::new("plane_ordinal", DataType::UInt8, false),
        Field::new("bucket_ordinal", DataType::UInt32, false),
        Field::new("row_start", DataType::UInt64, false),
        Field::new("row_end", DataType::UInt64, false),
    ])
}

fn v26_dual_pq_key_ordinals_schema() -> Schema {
    Schema::new(vec![
        Field::new("plane_ordinal", DataType::UInt8, false),
        Field::new("position", DataType::UInt64, false),
        Field::new("source_ordinal", DataType::UInt32, false),
    ])
}

fn arrow_file_identity(path: &Path) -> Result<V26ArrowFileIdentity> {
    let (encoded_bytes, sha256) = sha256_file(path)?;
    Ok(V26ArrowFileIdentity {
        encoded_bytes,
        sha256,
    })
}

fn authenticate_arrow_file(
    path: &Path,
    identity: &V26ArrowFileIdentity,
) -> Result<FileReader<fs::File>> {
    if identity.encoded_bytes == 0 || !exact_lower_hex(&identity.sha256, 64) {
        return Err(invalid("V26 Arrow identity differs"));
    }
    let (encoded_bytes, sha256) = sha256_file(path)?;
    if encoded_bytes != identity.encoded_bytes || sha256 != identity.sha256 {
        return Err(invalid("V26 Arrow file identity differs"));
    }
    FileReader::try_new(
        fs::File::open(path)
            .map_err(|error| invalid(&format!("V26 Arrow file open failed: {error}")))?,
        None,
    )
    .map_err(|error| invalid(&format!("V26 Arrow metadata failed: {error}")))
}

pub fn write_v26_pq16_index_arrow(
    directory: &Path,
    index: &crate::V26PackedPq16Index,
    assignments: &[V26RowPages],
) -> Result<V26Pq16IndexManifest> {
    let codebook_path = directory.join("pq16-codebook.arrow");
    let codes_path = directory.join("pq16-codes.arrow");
    let postings_path = directory.join("pq16-postings.arrow");
    if !directory.is_dir()
        || [&codebook_path, &codes_path, &postings_path]
            .iter()
            .any(|path| path.exists())
        || index.codebook.width != 16
        || index.codebook.subspace_width != 6
        || index.codes.len() != assignments.len() * 16
        || index.posting_rows.len() != assignments.len() * 2
    {
        return Err(invalid("V26 PQ16 Arrow write request differs"));
    }
    let result = (|| {
        let codebook_file = fs::File::create(&codebook_path)
            .map_err(|error| invalid(&format!("V26 codebook create failed: {error}")))?;
        let mut codebook_writer =
            FileWriter::try_new(codebook_file, &v26_pq16_codebook_schema())
                .map_err(|error| invalid(&format!("V26 codebook writer failed: {error}")))?;
        let values = index
            .codebook
            .centroids
            .iter()
            .flat_map(|centroids| centroids.iter().copied())
            .collect::<Vec<_>>();
        let codebook_values = FixedSizeListArray::try_new(
            Arc::new(Field::new("element", DataType::Float32, false)),
            6,
            Arc::new(Float32Array::from(values)),
            None,
        )
        .map_err(|error| invalid(&format!("V26 codebook values failed: {error}")))?;
        codebook_writer
            .write(
                &RecordBatch::try_new(
                    Arc::new(v26_pq16_codebook_schema()),
                    vec![
                        Arc::new(UInt8Array::from_iter_values(
                            (0_u8..16).flat_map(|subspace| std::iter::repeat_n(subspace, 256)),
                        )),
                        Arc::new(UInt16Array::from_iter_values(
                            (0_u8..16).flat_map(|_| 0_u16..256),
                        )),
                        Arc::new(codebook_values),
                    ],
                )
                .map_err(|error| invalid(&format!("V26 codebook batch failed: {error}")))?,
            )
            .map_err(|error| invalid(&format!("V26 codebook write failed: {error}")))?;
        codebook_writer
            .finish()
            .map_err(|error| invalid(&format!("V26 codebook finish failed: {error}")))?;

        let codes_file = fs::File::create(&codes_path)
            .map_err(|error| invalid(&format!("V26 codes create failed: {error}")))?;
        let mut codes_writer = FileWriter::try_new(codes_file, &v26_pq16_codes_schema())
            .map_err(|error| invalid(&format!("V26 codes writer failed: {error}")))?;
        for (batch_index, chunk) in index.codes.chunks(65_536 * 16).enumerate() {
            let row_start = batch_index * 65_536;
            let row_count = chunk.len() / 16;
            let codes = FixedSizeBinaryArray::try_from_iter(chunk.as_chunks::<16>().0.iter())
                .map_err(|error| invalid(&format!("V26 codes array failed: {error}")))?;
            let batch = RecordBatch::try_new(
                Arc::new(v26_pq16_codes_schema()),
                vec![
                    Arc::new(UInt32Array::from_iter_values(
                        (row_start..row_start + row_count).map(|row| u32::try_from(row).unwrap()),
                    )),
                    Arc::new(codes),
                ],
            )
            .map_err(|error| invalid(&format!("V26 codes batch failed: {error}")))?;
            codes_writer
                .write(&batch)
                .map_err(|error| invalid(&format!("V26 codes write failed: {error}")))?;
        }
        codes_writer
            .finish()
            .map_err(|error| invalid(&format!("V26 codes finish failed: {error}")))?;

        let postings_file = fs::File::create(&postings_path)
            .map_err(|error| invalid(&format!("V26 postings create failed: {error}")))?;
        let mut postings_writer =
            FileWriter::try_new(postings_file, &v26_pq16_postings_schema())
                .map_err(|error| invalid(&format!("V26 postings writer failed: {error}")))?;
        for page in 0..index.page_offsets.len() - 1 {
            let start = usize::try_from(index.page_offsets[page]).unwrap();
            let end = usize::try_from(index.page_offsets[page + 1]).unwrap();
            let rows = &index.posting_rows[start..end];
            let batch = RecordBatch::try_new(
                Arc::new(v26_pq16_postings_schema()),
                vec![
                    Arc::new(UInt32Array::from_iter_values(std::iter::repeat_n(
                        u32::try_from(page).unwrap(),
                        rows.len(),
                    ))),
                    Arc::new(UInt32Array::from_iter_values(rows.iter().copied())),
                ],
            )
            .map_err(|error| invalid(&format!("V26 postings batch failed: {error}")))?;
            postings_writer
                .write(&batch)
                .map_err(|error| invalid(&format!("V26 postings write failed: {error}")))?;
        }
        postings_writer
            .finish()
            .map_err(|error| invalid(&format!("V26 postings finish failed: {error}")))?;

        Ok(V26Pq16IndexManifest {
            row_count: u64::try_from(assignments.len()).unwrap(),
            page_count: u32::try_from(index.page_offsets.len() - 1).unwrap(),
            occurrence_count: u64::try_from(index.posting_rows.len()).unwrap(),
            projected_resident_bytes_100m: index.projected_resident_bytes_100m,
            codebook: arrow_file_identity(&codebook_path)?,
            codes: arrow_file_identity(&codes_path)?,
            postings: arrow_file_identity(&postings_path)?,
        })
    })();
    if result.is_err() {
        for path in [&codebook_path, &codes_path, &postings_path] {
            let _ = fs::remove_file(path);
        }
    }
    result
}

pub fn read_v26_pq16_index_arrow(
    directory: &Path,
    manifest: &V26Pq16IndexManifest,
) -> Result<crate::V26PackedPq16Index> {
    if manifest.row_count == 0
        || manifest.row_count > u64::from(u32::MAX)
        || manifest.page_count == 0
        || manifest.occurrence_count != manifest.row_count.checked_mul(2).unwrap_or(0)
        || manifest.projected_resident_bytes_100m
            != projected_v26_pq16_rerank_resident_bytes(100_000_000, 2_816)?
    {
        return Err(invalid("V26 PQ16 Arrow manifest differs"));
    }
    let mut codebook_reader =
        authenticate_arrow_file(&directory.join("pq16-codebook.arrow"), &manifest.codebook)?;
    if codebook_reader.schema().as_ref() != &v26_pq16_codebook_schema() {
        return Err(invalid("V26 PQ16 codebook schema differs"));
    }
    let mut centroids = vec![Vec::<f32>::new(); 16];
    let mut codebook_row = 0_usize;
    for batch in &mut codebook_reader {
        let batch =
            batch.map_err(|error| invalid(&format!("V26 codebook read failed: {error}")))?;
        if batch
            .columns()
            .iter()
            .any(|column| column.null_count() != 0)
        {
            return Err(invalid("V26 PQ16 codebook nullability differs"));
        }
        let subspaces = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt8Array>()
            .unwrap();
        let centroid_ids = batch
            .column(1)
            .as_any()
            .downcast_ref::<UInt16Array>()
            .unwrap();
        let lists = batch
            .column(2)
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .unwrap();
        let values = lists
            .values()
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            if usize::from(subspaces.value(row)) != codebook_row / 256
                || usize::from(centroid_ids.value(row)) != codebook_row % 256
            {
                return Err(invalid("V26 PQ16 codebook order differs"));
            }
            let start = row * 6;
            let value = &values.values()[start..start + 6];
            if value.iter().any(|value| !value.is_finite()) {
                return Err(invalid("V26 PQ16 codebook value differs"));
            }
            centroids[codebook_row / 256].extend_from_slice(value);
            codebook_row += 1;
        }
    }
    if codebook_row != 16 * 256 {
        return Err(invalid("V26 PQ16 codebook inventory differs"));
    }

    let mut codes_reader =
        authenticate_arrow_file(&directory.join("pq16-codes.arrow"), &manifest.codes)?;
    if codes_reader.schema().as_ref() != &v26_pq16_codes_schema() {
        return Err(invalid("V26 PQ16 codes schema differs"));
    }
    let mut codes = Vec::with_capacity(usize::try_from(manifest.row_count).unwrap() * 16);
    let mut expected_row = 0_u32;
    for batch in &mut codes_reader {
        let batch = batch.map_err(|error| invalid(&format!("V26 codes read failed: {error}")))?;
        if batch
            .columns()
            .iter()
            .any(|column| column.null_count() != 0)
        {
            return Err(invalid("V26 PQ16 codes nullability differs"));
        }
        let ordinals = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap();
        let encoded = batch
            .column(1)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        for row in 0..batch.num_rows() {
            if ordinals.value(row) != expected_row {
                return Err(invalid("V26 PQ16 code order differs"));
            }
            codes.extend_from_slice(encoded.value(row));
            expected_row += 1;
        }
    }
    if u64::from(expected_row) != manifest.row_count {
        return Err(invalid("V26 PQ16 code inventory differs"));
    }

    let mut postings_reader =
        authenticate_arrow_file(&directory.join("pq16-postings.arrow"), &manifest.postings)?;
    if postings_reader.schema().as_ref() != &v26_pq16_postings_schema() {
        return Err(invalid("V26 PQ16 postings schema differs"));
    }
    let mut posting_rows = Vec::with_capacity(usize::try_from(manifest.occurrence_count).unwrap());
    let mut page_offsets = vec![0_u64];
    let mut current_page = 0_u32;
    let mut prior_row = None;
    for batch in &mut postings_reader {
        let batch =
            batch.map_err(|error| invalid(&format!("V26 postings read failed: {error}")))?;
        if batch
            .columns()
            .iter()
            .any(|column| column.null_count() != 0)
        {
            return Err(invalid("V26 PQ16 postings nullability differs"));
        }
        let pages = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap();
        let rows = batch
            .column(1)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            let page = pages.value(row);
            let source = rows.value(row);
            if page >= manifest.page_count || u64::from(source) >= manifest.row_count {
                return Err(invalid("V26 PQ16 posting value differs"));
            }
            while current_page < page {
                if page_offsets.last().copied() == Some(posting_rows.len() as u64) {
                    return Err(invalid("V26 PQ16 empty posting page differs"));
                }
                page_offsets.push(posting_rows.len() as u64);
                current_page += 1;
                prior_row = None;
            }
            if page != current_page || prior_row.is_some_and(|prior| prior >= source) {
                return Err(invalid("V26 PQ16 posting order differs"));
            }
            posting_rows.push(source);
            prior_row = Some(source);
        }
    }
    page_offsets.push(posting_rows.len() as u64);
    if page_offsets.len() != usize::try_from(manifest.page_count).unwrap() + 1
        || u64::try_from(posting_rows.len()).unwrap() != manifest.occurrence_count
    {
        return Err(invalid("V26 PQ16 posting inventory differs"));
    }
    Ok(crate::V26PackedPq16Index {
        codebook: crate::V26PqCodebook {
            width: 16,
            subspace_width: 6,
            centroids,
        },
        codes,
        page_offsets,
        posting_rows,
        projected_resident_bytes_100m: manifest.projected_resident_bytes_100m,
    })
}

pub fn write_v26_simhash_pq16_index_arrow(
    directory: &Path,
    index: &crate::V26SimHashPq16MultiIndex,
) -> Result<V26SimHashPq16IndexManifest> {
    let codebook_path = directory.join("simhash-pq16-codebook.arrow");
    let buckets_path = directory.join("simhash-pq16-buckets.arrow");
    let records_path = directory.join("simhash-pq16-records.arrow");
    if !directory.is_dir()
        || [&codebook_path, &buckets_path, &records_path]
            .iter()
            .any(|path| path.exists())
        || index.codebook.width != 16
        || index.codebook.subspace_width != 6
        || index.bucket_offsets.len() != 65_537
        || index.page_count == 0
        || index.source_ordinals.len() * 16 != index.codes.len()
    {
        return Err(invalid("V26 SimHash Arrow write request differs"));
    }
    let result = (|| {
        let codebook_file = fs::File::create(&codebook_path)
            .map_err(|error| invalid(&format!("V26 SimHash codebook create failed: {error}")))?;
        let mut codebook_writer = FileWriter::try_new(codebook_file, &v26_pq16_codebook_schema())
            .map_err(|error| {
            invalid(&format!("V26 SimHash codebook writer failed: {error}"))
        })?;
        let values = index
            .codebook
            .centroids
            .iter()
            .flat_map(|centroids| centroids.iter().copied())
            .collect::<Vec<_>>();
        let centroid_values = FixedSizeListArray::try_new(
            Arc::new(Field::new("element", DataType::Float32, false)),
            6,
            Arc::new(Float32Array::from(values)),
            None,
        )
        .map_err(|error| invalid(&format!("V26 SimHash codebook values failed: {error}")))?;
        codebook_writer
            .write(
                &RecordBatch::try_new(
                    Arc::new(v26_pq16_codebook_schema()),
                    vec![
                        Arc::new(UInt8Array::from_iter_values(
                            (0_u8..16).flat_map(|subspace| std::iter::repeat_n(subspace, 256)),
                        )),
                        Arc::new(UInt16Array::from_iter_values(
                            (0_u8..16).flat_map(|_| 0_u16..256),
                        )),
                        Arc::new(centroid_values),
                    ],
                )
                .map_err(|error| invalid(&format!("V26 SimHash codebook batch failed: {error}")))?,
            )
            .map_err(|error| invalid(&format!("V26 SimHash codebook write failed: {error}")))?;
        codebook_writer
            .finish()
            .map_err(|error| invalid(&format!("V26 SimHash codebook finish failed: {error}")))?;

        let buckets_file = fs::File::create(&buckets_path)
            .map_err(|error| invalid(&format!("V26 SimHash buckets create failed: {error}")))?;
        let mut buckets_writer =
            FileWriter::try_new(buckets_file, &v26_simhash_buckets_schema())
                .map_err(|error| invalid(&format!("V26 SimHash buckets writer failed: {error}")))?;
        buckets_writer
            .write(
                &RecordBatch::try_new(
                    Arc::new(v26_simhash_buckets_schema()),
                    vec![
                        Arc::new(UInt32Array::from_iter_values(0_u32..65_536)),
                        Arc::new(UInt64Array::from_iter_values(
                            index.bucket_offsets[..65_536].iter().copied(),
                        )),
                        Arc::new(UInt64Array::from_iter_values(
                            index.bucket_offsets[1..].iter().copied(),
                        )),
                    ],
                )
                .map_err(|error| invalid(&format!("V26 SimHash buckets batch failed: {error}")))?,
            )
            .map_err(|error| invalid(&format!("V26 SimHash buckets write failed: {error}")))?;
        buckets_writer
            .finish()
            .map_err(|error| invalid(&format!("V26 SimHash buckets finish failed: {error}")))?;

        let records_file = fs::File::create(&records_path)
            .map_err(|error| invalid(&format!("V26 SimHash records create failed: {error}")))?;
        let mut records_writer =
            FileWriter::try_new(records_file, &v26_simhash_records_schema())
                .map_err(|error| invalid(&format!("V26 SimHash records writer failed: {error}")))?;
        for (batch_index, ordinals) in index.source_ordinals.chunks(65_536).enumerate() {
            let start = batch_index * 65_536;
            let end = start + ordinals.len();
            let codes = FixedSizeBinaryArray::try_from_iter(
                index.codes[start * 16..end * 16].as_chunks::<16>().0.iter(),
            )
            .map_err(|error| invalid(&format!("V26 SimHash records array failed: {error}")))?;
            records_writer
                .write(
                    &RecordBatch::try_new(
                        Arc::new(v26_simhash_records_schema()),
                        vec![
                            Arc::new(UInt32Array::from_iter_values(ordinals.iter().copied())),
                            Arc::new(codes),
                        ],
                    )
                    .map_err(|error| {
                        invalid(&format!("V26 SimHash records batch failed: {error}"))
                    })?,
                )
                .map_err(|error| invalid(&format!("V26 SimHash records write failed: {error}")))?;
        }
        records_writer
            .finish()
            .map_err(|error| invalid(&format!("V26 SimHash records finish failed: {error}")))?;
        Ok(V26SimHashPq16IndexManifest {
            row_count: u64::try_from(index.source_ordinals.len()).unwrap(),
            page_count: index.page_count,
            bucket_count: 65_536,
            projected_resident_bytes_100m: index.projected_resident_bytes_100m,
            codebook: arrow_file_identity(&codebook_path)?,
            buckets: arrow_file_identity(&buckets_path)?,
            records: arrow_file_identity(&records_path)?,
        })
    })();
    if result.is_err() {
        for path in [&codebook_path, &buckets_path, &records_path] {
            let _ = fs::remove_file(path);
        }
    }
    result
}

pub fn read_v26_simhash_pq16_index_arrow(
    directory: &Path,
    manifest: &V26SimHashPq16IndexManifest,
) -> Result<crate::V26SimHashPq16MultiIndex> {
    if manifest.row_count == 0
        || manifest.row_count > u64::from(u32::MAX)
        || manifest.bucket_count != 65_536
        || manifest.page_count == 0
        || manifest.projected_resident_bytes_100m != 2_537_493_520
    {
        return Err(invalid("V26 SimHash Arrow manifest differs"));
    }
    let mut codebook_reader = authenticate_arrow_file(
        &directory.join("simhash-pq16-codebook.arrow"),
        &manifest.codebook,
    )?;
    if codebook_reader.schema().as_ref() != &v26_pq16_codebook_schema() {
        return Err(invalid("V26 SimHash codebook schema differs"));
    }
    let mut centroids = vec![Vec::<f32>::new(); 16];
    let mut codebook_row = 0_usize;
    for batch in &mut codebook_reader {
        let batch = batch
            .map_err(|error| invalid(&format!("V26 SimHash codebook read failed: {error}")))?;
        if batch
            .columns()
            .iter()
            .any(|column| column.null_count() != 0)
        {
            return Err(invalid("V26 SimHash codebook nullability differs"));
        }
        let subspaces = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt8Array>()
            .unwrap();
        let centroid_ids = batch
            .column(1)
            .as_any()
            .downcast_ref::<UInt16Array>()
            .unwrap();
        let lists = batch
            .column(2)
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .unwrap();
        let values = lists
            .values()
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            if usize::from(subspaces.value(row)) != codebook_row / 256
                || usize::from(centroid_ids.value(row)) != codebook_row % 256
            {
                return Err(invalid("V26 SimHash codebook order differs"));
            }
            let start = row * 6;
            let value = &values.values()[start..start + 6];
            if value.iter().any(|value| !value.is_finite()) {
                return Err(invalid("V26 SimHash codebook value differs"));
            }
            centroids[codebook_row / 256].extend_from_slice(value);
            codebook_row += 1;
        }
    }
    if codebook_row != 16 * 256 {
        return Err(invalid("V26 SimHash codebook inventory differs"));
    }

    let mut buckets_reader = authenticate_arrow_file(
        &directory.join("simhash-pq16-buckets.arrow"),
        &manifest.buckets,
    )?;
    if buckets_reader.schema().as_ref() != &v26_simhash_buckets_schema() {
        return Err(invalid("V26 SimHash bucket schema differs"));
    }
    let mut bucket_offsets = Vec::with_capacity(65_537);
    let mut expected_bucket = 0_u32;
    for batch in &mut buckets_reader {
        let batch =
            batch.map_err(|error| invalid(&format!("V26 SimHash bucket read failed: {error}")))?;
        if batch
            .columns()
            .iter()
            .any(|column| column.null_count() != 0)
        {
            return Err(invalid("V26 SimHash bucket nullability differs"));
        }
        let ordinals = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap();
        let starts = batch
            .column(1)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let ends = batch
            .column(2)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            let start = starts.value(row);
            let end = ends.value(row);
            if ordinals.value(row) != expected_bucket
                || start > end
                || bucket_offsets.last().is_some_and(|prior| *prior != start)
            {
                return Err(invalid("V26 SimHash bucket order differs"));
            }
            if bucket_offsets.is_empty() {
                bucket_offsets.push(start);
            }
            bucket_offsets.push(end);
            expected_bucket += 1;
        }
    }
    if expected_bucket != 65_536
        || bucket_offsets.first() != Some(&0)
        || bucket_offsets.last().copied() != Some(manifest.row_count)
    {
        return Err(invalid("V26 SimHash bucket inventory differs"));
    }

    let mut records_reader = authenticate_arrow_file(
        &directory.join("simhash-pq16-records.arrow"),
        &manifest.records,
    )?;
    if records_reader.schema().as_ref() != &v26_simhash_records_schema() {
        return Err(invalid("V26 SimHash records schema differs"));
    }
    let mut source_ordinals = Vec::with_capacity(usize::try_from(manifest.row_count).unwrap());
    let mut codes = Vec::with_capacity(source_ordinals.capacity() * 16);
    for batch in &mut records_reader {
        let batch =
            batch.map_err(|error| invalid(&format!("V26 SimHash records read failed: {error}")))?;
        if batch
            .columns()
            .iter()
            .any(|column| column.null_count() != 0)
        {
            return Err(invalid("V26 SimHash records nullability differs"));
        }
        let ordinals = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap();
        let encoded = batch
            .column(1)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        for row in 0..batch.num_rows() {
            source_ordinals.push(ordinals.value(row));
            codes.extend_from_slice(encoded.value(row));
        }
    }
    if u64::try_from(source_ordinals.len()).unwrap() != manifest.row_count
        || source_ordinals
            .iter()
            .copied()
            .collect::<BTreeSet<_>>()
            .len()
            != source_ordinals.len()
        || source_ordinals
            .iter()
            .any(|ordinal| u64::from(*ordinal) >= manifest.row_count)
    {
        return Err(invalid("V26 SimHash record inventory differs"));
    }
    Ok(crate::V26SimHashPq16MultiIndex {
        codebook: crate::V26PqCodebook {
            width: 16,
            subspace_width: 6,
            centroids,
        },
        page_count: manifest.page_count,
        bucket_offsets,
        source_ordinals,
        codes,
        projected_resident_bytes_100m: manifest.projected_resident_bytes_100m,
    })
}

pub fn write_v26_dual_pq_key_index_arrow(
    directory: &Path,
    index: &crate::V26DualPqKeyIndex,
) -> Result<V26DualPqKeyIndexManifest> {
    let offsets_path = directory.join("pq16-dual-key-offsets.arrow");
    let ordinals_path = directory.join("pq16-dual-key-ordinals.arrow");
    let row_count = index.source_ordinals[0].len();
    if !directory.is_dir()
        || offsets_path.exists()
        || ordinals_path.exists()
        || row_count == 0
        || index.source_ordinals[1].len() != row_count
        || index.codes.len() != row_count * 16
        || index.bucket_offsets.iter().any(|offsets| {
            offsets.len() != 65_537
                || offsets.first() != Some(&0)
                || offsets.last().copied() != Some(u64::try_from(row_count).unwrap())
                || offsets.windows(2).any(|pair| pair[0] > pair[1])
        })
        || index.projected_resident_bytes_100m != 2_938_017_816
    {
        return Err(invalid("V26 dual PQ-key Arrow write request differs"));
    }
    let result = (|| {
        let file = fs::File::create(&offsets_path)
            .map_err(|error| invalid(&format!("V26 dual PQ-key offsets create failed: {error}")))?;
        let mut writer = FileWriter::try_new(file, &v26_dual_pq_key_offsets_schema())
            .map_err(|error| invalid(&format!("V26 dual PQ-key offsets writer failed: {error}")))?;
        for plane in 0..2 {
            writer
                .write(
                    &RecordBatch::try_new(
                        Arc::new(v26_dual_pq_key_offsets_schema()),
                        vec![
                            Arc::new(UInt8Array::from_iter_values(std::iter::repeat_n(
                                u8::try_from(plane).unwrap(),
                                65_536,
                            ))),
                            Arc::new(UInt32Array::from_iter_values(0_u32..65_536)),
                            Arc::new(UInt64Array::from_iter_values(
                                index.bucket_offsets[plane][..65_536].iter().copied(),
                            )),
                            Arc::new(UInt64Array::from_iter_values(
                                index.bucket_offsets[plane][1..].iter().copied(),
                            )),
                        ],
                    )
                    .map_err(|error| {
                        invalid(&format!("V26 dual PQ-key offsets batch failed: {error}"))
                    })?,
                )
                .map_err(|error| {
                    invalid(&format!("V26 dual PQ-key offsets write failed: {error}"))
                })?;
        }
        writer
            .finish()
            .map_err(|error| invalid(&format!("V26 dual PQ-key offsets finish failed: {error}")))?;

        let file = fs::File::create(&ordinals_path).map_err(|error| {
            invalid(&format!("V26 dual PQ-key ordinals create failed: {error}"))
        })?;
        let mut writer =
            FileWriter::try_new(file, &v26_dual_pq_key_ordinals_schema()).map_err(|error| {
                invalid(&format!("V26 dual PQ-key ordinals writer failed: {error}"))
            })?;
        for plane in 0..2 {
            for (batch_index, ordinals) in index.source_ordinals[plane].chunks(65_536).enumerate() {
                let first = batch_index * 65_536;
                writer
                    .write(
                        &RecordBatch::try_new(
                            Arc::new(v26_dual_pq_key_ordinals_schema()),
                            vec![
                                Arc::new(UInt8Array::from_iter_values(std::iter::repeat_n(
                                    u8::try_from(plane).unwrap(),
                                    ordinals.len(),
                                ))),
                                Arc::new(UInt64Array::from_iter_values(
                                    (first..first + ordinals.len())
                                        .map(|position| u64::try_from(position).unwrap()),
                                )),
                                Arc::new(UInt32Array::from_iter_values(ordinals.iter().copied())),
                            ],
                        )
                        .map_err(|error| {
                            invalid(&format!("V26 dual PQ-key ordinals batch failed: {error}"))
                        })?,
                    )
                    .map_err(|error| {
                        invalid(&format!("V26 dual PQ-key ordinals write failed: {error}"))
                    })?;
            }
        }
        writer.finish().map_err(|error| {
            invalid(&format!("V26 dual PQ-key ordinals finish failed: {error}"))
        })?;
        Ok(V26DualPqKeyIndexManifest {
            row_count: u64::try_from(row_count).unwrap(),
            plane_count: 2,
            bucket_count: 65_536,
            projected_resident_bytes_100m: index.projected_resident_bytes_100m,
            offsets: arrow_file_identity(&offsets_path)?,
            ordinals: arrow_file_identity(&ordinals_path)?,
        })
    })();
    if result.is_err() {
        let _ = fs::remove_file(offsets_path);
        let _ = fs::remove_file(ordinals_path);
    }
    result
}

/// Derives the two dual-PQ-key Arrow planes from an authenticated serving bundle.
pub fn build_v26_dual_pq_key_index_from_serving(
    serving_manifest: &V26LocalObjectPath,
    serving_dir: &Path,
    dual_index_dir: &Path,
) -> Result<V26DualPqKeyIndexManifest> {
    let manifest = read_v26_pq16_serving_manifest(serving_manifest)?;
    let expected_names = v26_pq16_serving_output_names()
        .into_iter()
        .chain(std::iter::once("serving-manifest.json"))
        .map(str::to_owned)
        .collect::<BTreeSet<_>>();
    let observed_names = fs::read_dir(serving_dir)
        .map_err(|error| invalid(&format!("V26 dual PQ-key directory read failed: {error}")))?
        .map(|entry| {
            entry
                .map_err(|error| {
                    invalid(&format!("V26 dual PQ-key directory read failed: {error}"))
                })
                .and_then(|entry| {
                    entry
                        .file_name()
                        .into_string()
                        .map_err(|_| invalid("V26 dual PQ-key artifact name differs"))
                })
        })
        .collect::<Result<BTreeSet<_>>>()?;
    if observed_names != expected_names || !dual_index_dir.is_dir() {
        return Err(invalid("V26 dual PQ-key artifact inventory differs"));
    }
    let packed = read_v26_pq16_index_arrow(serving_dir, &manifest.index)?;
    let index = crate::build_v26_dual_pq_key_index(&packed)?;
    write_v26_dual_pq_key_index_arrow(dual_index_dir, &index)
}

pub fn read_v26_dual_pq_key_index_arrow(
    directory: &Path,
    manifest: &V26DualPqKeyIndexManifest,
    packed: &crate::V26PackedPq16Index,
) -> Result<crate::V26DualPqKeyIndex> {
    if manifest.row_count == 0
        || manifest.row_count > u64::from(u32::MAX)
        || manifest.plane_count != 2
        || manifest.bucket_count != 65_536
        || manifest.projected_resident_bytes_100m != 2_938_017_816
        || u64::try_from(packed.codes.len() / 16).ok() != Some(manifest.row_count)
        || !packed.codes.len().is_multiple_of(16)
    {
        return Err(invalid("V26 dual PQ-key Arrow manifest differs"));
    }
    let mut offsets_reader = authenticate_arrow_file(
        &directory.join("pq16-dual-key-offsets.arrow"),
        &manifest.offsets,
    )?;
    if offsets_reader.schema().as_ref() != &v26_dual_pq_key_offsets_schema() {
        return Err(invalid("V26 dual PQ-key offsets schema differs"));
    }
    let mut bucket_offsets = [vec![0_u64], vec![0_u64]];
    let mut expected = [(0_u32, 0_u64), (0_u32, 0_u64)];
    for batch in &mut offsets_reader {
        let batch = batch
            .map_err(|error| invalid(&format!("V26 dual PQ-key offsets read failed: {error}")))?;
        if batch
            .columns()
            .iter()
            .any(|column| column.null_count() != 0)
        {
            return Err(invalid("V26 dual PQ-key offsets nullability differs"));
        }
        let planes = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt8Array>()
            .unwrap();
        let buckets = batch
            .column(1)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap();
        let starts = batch
            .column(2)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let ends = batch
            .column(3)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            let plane = usize::from(planes.value(row));
            if plane >= 2
                || buckets.value(row) != expected[plane].0
                || starts.value(row) != expected[plane].1
                || starts.value(row) > ends.value(row)
            {
                return Err(invalid("V26 dual PQ-key offsets order differs"));
            }
            bucket_offsets[plane].push(ends.value(row));
            expected[plane] = (expected[plane].0 + 1, ends.value(row));
        }
    }
    if expected != [(65_536, manifest.row_count), (65_536, manifest.row_count)] {
        return Err(invalid("V26 dual PQ-key offsets inventory differs"));
    }

    let mut ordinals_reader = authenticate_arrow_file(
        &directory.join("pq16-dual-key-ordinals.arrow"),
        &manifest.ordinals,
    )?;
    if ordinals_reader.schema().as_ref() != &v26_dual_pq_key_ordinals_schema() {
        return Err(invalid("V26 dual PQ-key ordinals schema differs"));
    }
    let row_count = usize::try_from(manifest.row_count).unwrap();
    let mut source_ordinals = [Vec::with_capacity(row_count), Vec::with_capacity(row_count)];
    for batch in &mut ordinals_reader {
        let batch = batch
            .map_err(|error| invalid(&format!("V26 dual PQ-key ordinals read failed: {error}")))?;
        if batch
            .columns()
            .iter()
            .any(|column| column.null_count() != 0)
        {
            return Err(invalid("V26 dual PQ-key ordinals nullability differs"));
        }
        let planes = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt8Array>()
            .unwrap();
        let positions = batch
            .column(1)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let ordinals = batch
            .column(2)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            let plane = usize::from(planes.value(row));
            if plane >= 2
                || usize::try_from(positions.value(row)).ok() != Some(source_ordinals[plane].len())
                || u64::from(ordinals.value(row)) >= manifest.row_count
            {
                return Err(invalid("V26 dual PQ-key ordinal order differs"));
            }
            source_ordinals[plane].push(ordinals.value(row));
        }
    }
    for plane in 0..2 {
        let mut seen = vec![false; row_count];
        for (bucket, bounds) in bucket_offsets[plane].windows(2).enumerate() {
            let start = usize::try_from(bounds[0]).unwrap();
            let end = usize::try_from(bounds[1]).unwrap();
            let rows = source_ordinals[plane]
                .get(start..end)
                .ok_or_else(|| invalid("V26 dual PQ-key ordinal inventory differs"))?;
            if rows.windows(2).any(|pair| pair[0] >= pair[1]) {
                return Err(invalid("V26 dual PQ-key ordinal stability differs"));
            }
            for source_ordinal in rows {
                let ordinal = usize::try_from(*source_ordinal).unwrap();
                let code: &[u8; 16] = packed.codes[ordinal * 16..ordinal * 16 + 16]
                    .try_into()
                    .unwrap();
                if seen[ordinal] || usize::from(crate::v26_dual_pq_key(code, plane)) != bucket {
                    return Err(invalid("V26 dual PQ-key ordinal binding differs"));
                }
                seen[ordinal] = true;
            }
        }
        if source_ordinals[plane].len() != row_count || seen.iter().any(|value| !value) {
            return Err(invalid("V26 dual PQ-key ordinal inventory differs"));
        }
    }
    Ok(crate::V26DualPqKeyIndex {
        codebook: packed.codebook.clone(),
        page_count: u32::try_from(packed.page_offsets.len() - 1).unwrap(),
        bucket_offsets,
        source_ordinals,
        codes: packed.codes.clone(),
        projected_resident_bytes_100m: manifest.projected_resident_bytes_100m,
    })
}

fn open_reader(path: &Path) -> Result<ParquetRecordBatchReaderBuilder<fs::File>> {
    let file = fs::File::open(path)
        .map_err(|error| invalid(&format!("V26 Parquet open failed: {error}")))?;
    ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|error| invalid(&format!("V26 Parquet metadata failed: {error}")))
}

fn read_inputs(
    request: &V26LayoutBuildRequest,
    authority: &V26LayoutAuthority,
) -> Result<Vec<V26ConstructionRow>> {
    if request.construction_rows.identity != authority.construction_rows {
        return Err(invalid("V26 construction input authority differs"));
    }
    authenticate(&request.construction_rows, "construction-parquet")?;
    if request.construction_rows.identity.generation != authority.generation {
        return Err(invalid("V26 input generation differs"));
    }
    let expected_rows_i64 = i64::try_from(authority.expected_rows)
        .map_err(|_| invalid("V26 input row count overflows"))?;
    let expected_rows_usize = usize::try_from(authority.expected_rows)
        .map_err(|_| invalid("V26 input row count overflows"))?;
    let construction = open_reader(&request.construction_rows.path)?;
    let construction_rows = construction.metadata().file_metadata().num_rows();
    if construction.schema().as_ref() != &v26_construction_schema()
        || construction_rows < expected_rows_i64
    {
        return Err(invalid("V26 input Parquet authority differs"));
    }
    let mut rows = Vec::with_capacity(expected_rows_usize);
    'construction: for batch in construction
        .build()
        .map_err(|error| invalid(&format!("V26 construction reader failed: {error}")))?
    {
        let batch =
            batch.map_err(|error| invalid(&format!("V26 construction batch failed: {error}")))?;
        let ordinals = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| invalid("V26 construction ordinal differs"))?;
        let vectors = batch
            .column(1)
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .ok_or_else(|| invalid("V26 construction vector differs"))?;
        if batch
            .columns()
            .iter()
            .any(|column| column.null_count() != 0)
            || vectors.values().null_count() != 0
        {
            return Err(invalid("V26 construction nullability differs"));
        }
        let flat = vectors
            .values()
            .as_any()
            .downcast_ref::<Float32Array>()
            .ok_or_else(|| invalid("V26 construction vector child differs"))?;
        let value_offset = vectors
            .offset()
            .checked_mul(96)
            .ok_or_else(|| invalid("V26 construction vector offset overflows"))?;
        for index in 0..batch.num_rows() {
            if rows.len() == expected_rows_usize {
                break 'construction;
            }
            let source_ordinal = ordinals.value(index);
            if source_ordinal != rows.len() as u64 {
                return Err(invalid("V26 construction inventory differs"));
            }
            let start = value_offset
                .checked_add(
                    index
                        .checked_mul(96)
                        .ok_or_else(|| invalid("V26 construction vector offset overflows"))?,
                )
                .ok_or_else(|| invalid("V26 construction vector offset overflows"))?;
            let vector: [f32; 96] = flat.values()[start..start + 96].try_into().unwrap();
            let norm = vector.iter().map(|value| value * value).sum::<f32>();
            if vector.iter().any(|value| !value.is_finite())
                || !norm.is_finite()
                || (norm - 1.0).abs() > 1.0e-4
            {
                return Err(invalid("V26 construction vector authority differs"));
            }
            rows.push(V26ConstructionRow {
                source_ordinal,
                vector,
            });
        }
    }
    if rows.len() as u64 != authority.expected_rows {
        return Err(invalid("V26 input row count differs"));
    }
    Ok(rows)
}

fn writer_properties() -> WriterProperties {
    WriterProperties::builder()
        .set_compression(Compression::ZSTD(Default::default()))
        .set_writer_version(WriterVersion::PARQUET_2_0)
        .set_dictionary_enabled(false)
        .set_statistics_enabled(parquet::file::properties::EnabledStatistics::Page)
        .build()
}

fn write_batch(path: &Path, batch: RecordBatch) -> Result<()> {
    let file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|error| invalid(&format!("V26 output create failed: {error}")))?;
    let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(writer_properties()))
        .map_err(|error| invalid(&format!("V26 output writer failed: {error}")))?;
    writer
        .write(&batch)
        .map_err(|error| invalid(&format!("V26 output write failed: {error}")))?;
    writer
        .close()
        .map_err(|error| invalid(&format!("V26 output close failed: {error}")))?;
    fs::OpenOptions::new()
        .write(true)
        .open(path)
        .and_then(|file| file.sync_all())
        .map_err(|error| invalid(&format!("V26 output sync failed: {error}")))
}

fn tree_batch(tree: &V26Tree) -> Result<RecordBatch> {
    RecordBatch::try_new(
        Arc::new(v26_tree_schema()),
        vec![
            Arc::new(UInt32Array::from_iter_values(
                tree.nodes.iter().map(|node| node.node_ordinal),
            )) as ArrayRef,
            Arc::new(UInt32Array::from(
                tree.nodes.iter().map(|node| node.left).collect::<Vec<_>>(),
            )),
            Arc::new(UInt32Array::from(
                tree.nodes.iter().map(|node| node.right).collect::<Vec<_>>(),
            )),
            Arc::new(UInt8Array::from_iter_values(
                tree.nodes.iter().map(|node| node.direction_ordinal),
            )),
            Arc::new(Float32Array::from_iter_values(
                tree.nodes.iter().map(|node| node.threshold),
            )),
            Arc::new(Float32Array::from_iter_values(
                tree.nodes.iter().map(|node| node.split_gap),
            )),
            Arc::new(UInt32Array::from(
                tree.nodes
                    .iter()
                    .map(|node| node.leaf_page)
                    .collect::<Vec<_>>(),
            )),
        ],
    )
    .map_err(|error| invalid(&format!("V26 tree batch failed: {error}")))
}

fn assignments_batch(rows: &[V26RowPages]) -> Result<RecordBatch> {
    RecordBatch::try_new(
        Arc::new(v26_page_assignments_schema()),
        vec![
            Arc::new(UInt64Array::from_iter_values(
                rows.iter().map(|row| row.source_ordinal),
            )) as ArrayRef,
            Arc::new(UInt32Array::from_iter_values(
                rows.iter().map(|row| row.primary_page),
            )),
            Arc::new(UInt32Array::from_iter_values(
                rows.iter().map(|row| row.replica_page),
            )),
        ],
    )
    .map_err(|error| invalid(&format!("V26 assignment batch failed: {error}")))
}

fn output_identity(
    role: &str,
    path: &Path,
    prefix: &str,
    generation: &str,
) -> Result<V26ObjectIdentity> {
    let (encoded_bytes, digest) = sha256_file(path)?;
    Ok(V26ObjectIdentity {
        role: role.to_owned(),
        uri: format!("{prefix}{}", path.file_name().unwrap().to_string_lossy()),
        digest_algorithm: "sha256".to_owned(),
        digest,
        encoded_bytes,
        generation: generation.to_owned(),
    })
}

fn validate_uri_inventory(request: &V26LayoutBuildRequest) -> Result<()> {
    let mut uris = BTreeSet::new();
    for uri in [
        request.manifest.identity.uri.clone(),
        request.construction_rows.identity.uri.clone(),
        format!("{}page-assignments.parquet", request.output_uri_prefix),
        format!("{}primary-tree.parquet", request.output_uri_prefix),
        format!("{}replica-tree.parquet", request.output_uri_prefix),
    ] {
        if !uris.insert(uri) {
            return Err(invalid("V26 object URI roles overlap"));
        }
    }
    Ok(())
}

pub fn run_v26_layout_build(request: &V26LayoutBuildRequest) -> Result<V26LayoutBuildOutput> {
    if request.worker_count == 0
        || !request.output_uri_prefix.starts_with("s3://")
        || !request.output_uri_prefix.ends_with('/')
        || request.output_dir.exists()
    {
        return Err(invalid("V26 layout build request differs"));
    }
    validate_uri_inventory(request)?;
    let authority = read_manifest(&request.manifest)?;
    let rows = read_inputs(request, &authority)?;
    let (primary, replica, assignments) =
        build_v26_dual_tree_layout_with_workers(&authority, &rows, request.worker_count)?;
    fs::create_dir(&request.output_dir)
        .map_err(|error| invalid(&format!("V26 output directory failed: {error}")))?;
    let result = (|| -> Result<V26LayoutBuildOutput> {
        write_batch(
            &request.output_dir.join("primary-tree.parquet"),
            tree_batch(&primary)?,
        )?;
        write_batch(
            &request.output_dir.join("replica-tree.parquet"),
            tree_batch(&replica)?,
        )?;
        write_batch(
            &request.output_dir.join("page-assignments.parquet"),
            assignments_batch(&assignments)?,
        )?;
        let outputs = vec![
            output_identity(
                "page-assignments-parquet",
                &request.output_dir.join("page-assignments.parquet"),
                &request.output_uri_prefix,
                &authority.generation,
            )?,
            output_identity(
                "primary-tree-parquet",
                &request.output_dir.join("primary-tree.parquet"),
                &request.output_uri_prefix,
                &authority.generation,
            )?,
            output_identity(
                "replica-tree-parquet",
                &request.output_dir.join("replica-tree.parquet"),
                &request.output_uri_prefix,
                &authority.generation,
            )?,
        ];
        let leaves = authority
            .expected_rows
            .div_ceil(u64::from(authority.page_capacity));
        let output = V26LayoutBuildOutput {
            authority: authority.clone(),
            inputs: vec![
                request.construction_rows.identity.clone(),
                request.manifest.identity.clone(),
            ],
            outputs,
            row_count: authority.expected_rows,
            leaves_per_tree: u32::try_from(leaves)
                .map_err(|_| invalid("V26 leaf count overflows"))?,
            page_count: u32::try_from(
                leaves
                    .checked_mul(2)
                    .ok_or_else(|| invalid("V26 page count overflows"))?,
            )
            .map_err(|_| invalid("V26 page count overflows"))?,
            projection_steps: projected_steps(
                authority.expected_rows,
                leaves,
                authority.page_capacity,
            )?
            .checked_mul(2)
            .ok_or_else(|| invalid("V26 projection work overflows"))?,
            worker_count: u32::try_from(request.worker_count)
                .map_err(|_| invalid("V26 worker count overflows"))?,
        };
        validate_v26_layout_build_output(request, &output)?;
        Ok(output)
    })();
    if result.is_err() {
        let _ = fs::remove_file(request.output_dir.join("primary-tree.parquet"));
        let _ = fs::remove_file(request.output_dir.join("replica-tree.parquet"));
        let _ = fs::remove_file(request.output_dir.join("page-assignments.parquet"));
        let _ = fs::remove_dir(&request.output_dir);
    }
    result
}

pub fn run_v26_layout_build_directory(
    manifest: V26LocalObjectPath,
    input_dir: &Path,
    output_dir: PathBuf,
    output_uri_prefix: String,
    worker_count: usize,
) -> Result<(V26LayoutBuildRequest, V26LayoutBuildOutput)> {
    let authority = read_manifest(&manifest)?;
    let request = V26LayoutBuildRequest {
        construction_rows: V26LocalObjectPath {
            identity: authority.construction_rows.clone(),
            path: input_dir.join("construction.parquet"),
        },
        manifest,
        output_dir,
        output_uri_prefix,
        worker_count,
    };
    let output = run_v26_layout_build(&request)?;
    Ok((request, output))
}

pub fn canonical_v26_layout_build_output_bytes(
    request: &V26LayoutBuildRequest,
    output: &V26LayoutBuildOutput,
) -> Result<Vec<u8>> {
    validate_v26_layout_build_output(request, output)?;
    let value = serde_json::to_value(output)
        .map_err(|error| invalid(&format!("V26 layout build output failed: {error}")))?;
    let mut bytes = serde_json::to_vec(&canonical_json_value(value))
        .map_err(|error| invalid(&format!("V26 layout build output failed: {error}")))?;
    bytes.push(b'\n');
    Ok(bytes)
}

fn read_tree(path: &Path, expected_rows: i64, seed: u64) -> Result<V26Tree> {
    let reader = open_reader(path)?;
    if reader.schema().as_ref() != &v26_tree_schema()
        || reader.metadata().file_metadata().num_rows() != expected_rows
    {
        return Err(invalid("V26 tree Parquet authority differs"));
    }
    let mut nodes = Vec::new();
    for batch in reader
        .build()
        .map_err(|error| invalid(&format!("V26 tree reader failed: {error}")))?
    {
        let batch = batch.map_err(|error| invalid(&format!("V26 tree batch failed: {error}")))?;
        let u32s = |column| {
            batch
                .column(column)
                .as_any()
                .downcast_ref::<UInt32Array>()
                .ok_or_else(|| invalid("V26 tree u32 differs"))
        };
        let ordinals = u32s(0)?;
        let left = u32s(1)?;
        let right = u32s(2)?;
        let directions = batch
            .column(3)
            .as_any()
            .downcast_ref::<UInt8Array>()
            .ok_or_else(|| invalid("V26 tree direction differs"))?;
        let thresholds = batch
            .column(4)
            .as_any()
            .downcast_ref::<Float32Array>()
            .ok_or_else(|| invalid("V26 tree threshold differs"))?;
        let gaps = batch
            .column(5)
            .as_any()
            .downcast_ref::<Float32Array>()
            .ok_or_else(|| invalid("V26 tree gap differs"))?;
        let pages = u32s(6)?;
        for row in 0..batch.num_rows() {
            nodes.push(V26Node {
                node_ordinal: ordinals.value(row),
                left: (!left.is_null(row)).then(|| left.value(row)),
                right: (!right.is_null(row)).then(|| right.value(row)),
                direction_ordinal: directions.value(row),
                threshold: thresholds.value(row),
                split_gap: gaps.value(row),
                leaf_page: (!pages.is_null(row)).then(|| pages.value(row)),
            });
        }
    }
    Ok(V26Tree {
        seed,
        root: 0,
        nodes,
    })
}

fn read_assignments(path: &Path, expected_rows: i64) -> Result<Vec<V26RowPages>> {
    let reader = open_reader(path)?;
    if reader.schema().as_ref() != &v26_page_assignments_schema()
        || reader.metadata().file_metadata().num_rows() != expected_rows
    {
        return Err(invalid("V26 assignment Parquet authority differs"));
    }
    let mut rows = Vec::new();
    for batch in reader
        .build()
        .map_err(|error| invalid(&format!("V26 assignment reader failed: {error}")))?
    {
        let batch =
            batch.map_err(|error| invalid(&format!("V26 assignment batch failed: {error}")))?;
        let sources = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| invalid("V26 assignment ordinal differs"))?;
        let primary = batch
            .column(1)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or_else(|| invalid("V26 primary page differs"))?;
        let replica = batch
            .column(2)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or_else(|| invalid("V26 replica page differs"))?;
        if batch
            .columns()
            .iter()
            .any(|column| column.null_count() != 0)
        {
            return Err(invalid("V26 assignment nullability differs"));
        }
        for row in 0..batch.num_rows() {
            rows.push(V26RowPages {
                source_ordinal: sources.value(row),
                primary_page: primary.value(row),
                replica_page: replica.value(row),
            });
        }
    }
    Ok(rows)
}

fn read_construction_rows(path: &Path, expected_rows: u64) -> Result<Vec<V26ConstructionRow>> {
    let expected_rows_i64 = i64::try_from(expected_rows)
        .map_err(|_| invalid("V26 construction row count overflows"))?;
    let reader = open_reader(path)?;
    if reader.schema().as_ref() != &v26_construction_schema()
        || reader.metadata().file_metadata().num_rows() != expected_rows_i64
    {
        return Err(invalid("V26 construction Parquet authority differs"));
    }
    let mut rows = Vec::with_capacity(
        usize::try_from(expected_rows)
            .map_err(|_| invalid("V26 construction row count overflows"))?,
    );
    for batch in reader
        .build()
        .map_err(|error| invalid(&format!("V26 construction reader failed: {error}")))?
    {
        let batch =
            batch.map_err(|error| invalid(&format!("V26 construction batch failed: {error}")))?;
        if batch
            .columns()
            .iter()
            .any(|column| column.null_count() != 0)
        {
            return Err(invalid("V26 construction nullability differs"));
        }
        let ordinals = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| invalid("V26 construction ordinal differs"))?;
        let vectors = batch
            .column(1)
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .ok_or_else(|| invalid("V26 construction vector differs"))?;
        if vectors.values().null_count() != 0 {
            return Err(invalid("V26 construction vector nullability differs"));
        }
        let flat = vectors
            .values()
            .as_any()
            .downcast_ref::<Float32Array>()
            .ok_or_else(|| invalid("V26 construction vector child differs"))?;
        let value_offset = vectors
            .offset()
            .checked_mul(96)
            .ok_or_else(|| invalid("V26 construction vector offset overflows"))?;
        for index in 0..batch.num_rows() {
            let source_ordinal = ordinals.value(index);
            if usize::try_from(source_ordinal).ok() != Some(rows.len()) {
                return Err(invalid("V26 construction inventory differs"));
            }
            let start = value_offset
                .checked_add(
                    index
                        .checked_mul(96)
                        .ok_or_else(|| invalid("V26 construction vector offset overflows"))?,
                )
                .ok_or_else(|| invalid("V26 construction vector offset overflows"))?;
            let vector: [f32; 96] = flat.values()[start..start + 96].try_into().unwrap();
            validate_v26_vector(&vector)?;
            rows.push(V26ConstructionRow {
                source_ordinal,
                vector,
            });
        }
    }
    if u64::try_from(rows.len()).ok() != Some(expected_rows) {
        return Err(invalid("V26 construction inventory differs"));
    }
    Ok(rows)
}

pub fn run_v26_pq4_fast_build(request: &V26Pq4FastBuildRequest) -> Result<V26Pq4FastManifest> {
    if request.expected_rows == 0
        || request.expected_rows > u64::from(u32::MAX)
        || request.output_dir.exists()
        || !request.output_uri_prefix.starts_with("s3://")
        || !request.output_uri_prefix.ends_with('/')
    {
        return Err(invalid("V26 PQ4 build request differs"));
    }
    authenticate(&request.construction_rows, "construction-parquet")?;
    authenticate(&request.page_assignments, "page-assignments-parquet")?;
    let terminal = read_layout_terminal(&request.layout_terminal)?;
    let generation = &terminal.authority.generation;
    if terminal.row_count != request.expected_rows
        || terminal.authority.expected_rows != request.expected_rows
        || terminal.authority.construction_rows != request.construction_rows.identity
        || !terminal
            .outputs
            .iter()
            .any(|identity| identity == &request.page_assignments.identity)
        || [
            &request.construction_rows.identity,
            &request.page_assignments.identity,
            &request.layout_terminal.identity,
        ]
        .iter()
        .any(|identity| identity.generation != *generation)
    {
        return Err(invalid("V26 PQ4 build authority differs"));
    }
    let rows = read_construction_rows(&request.construction_rows.path, request.expected_rows)?;
    let assignments = read_assignments(
        &request.page_assignments.path,
        i64::try_from(request.expected_rows)
            .map_err(|_| invalid("V26 PQ4 assignment count overflows"))?,
    )?;
    if rows
        .iter()
        .zip(&assignments)
        .enumerate()
        .any(|(ordinal, (row, assignment))| {
            row.source_ordinal != ordinal as u64
                || assignment.source_ordinal != ordinal as u64
                || assignment.primary_page == assignment.replica_page
                || assignment.primary_page >= terminal.page_count
                || assignment.replica_page >= terminal.page_count
        })
    {
        return Err(invalid("V26 PQ4 source order differs"));
    }
    let vectors = rows.iter().map(|row| row.vector).collect::<Vec<_>>();
    let index = crate::build_v26_pq4_fast_index(&vectors)?;
    fs::create_dir(&request.output_dir)
        .map_err(|error| invalid(&format!("V26 PQ4 output directory failed: {error}")))?;
    let result = (|| {
        let manifest = write_v26_pq4_fast_index_arrow(
            &request.output_dir,
            &index,
            &request.construction_rows.identity,
            &request.page_assignments.identity,
            &request.layout_terminal.identity,
            &request.output_uri_prefix,
        )?;
        fs::write(
            request.output_dir.join("pq4-fast-manifest.json"),
            canonical_v26_pq4_fast_manifest_bytes(&manifest)?,
        )
        .map_err(|error| invalid(&format!("V26 PQ4 manifest write failed: {error}")))?;
        Ok(manifest)
    })();
    if result.is_err() {
        for name in [
            "pq4-fast-codebook.arrow",
            "pq4-fast-codes.arrow",
            "pq4-fast-manifest.json",
        ] {
            let _ = fs::remove_file(request.output_dir.join(name));
        }
        let _ = fs::remove_dir(&request.output_dir);
    }
    result
}

fn read_v26_pq4_fast_manifest(object: &V26LocalObjectPath) -> Result<V26Pq4FastManifest> {
    authenticate(object, "pq4-fast-manifest")?;
    let bytes = fs::read(&object.path)
        .map_err(|error| invalid(&format!("V26 PQ4 manifest read failed: {error}")))?;
    let manifest: V26Pq4FastManifest = serde_json::from_slice(&bytes)
        .map_err(|error| invalid(&format!("V26 PQ4 manifest parse failed: {error}")))?;
    if canonical_v26_pq4_fast_manifest_bytes(&manifest)? != bytes
        || manifest.construction_rows.generation != object.identity.generation
    {
        return Err(invalid("V26 PQ4 manifest bytes differ"));
    }
    Ok(manifest)
}

fn select_v26_pq4_quality_arms(
    index: &crate::V26Pq4FastIndex,
    query: &[f32; 96],
    cold_vectors: &V26ArrowColdVectors,
) -> Result<(Vec<(V26Pq16ServingSelection, u64)>, u64, u32, u32, u32)> {
    const DEPTHS: [usize; 4] = [512, 1_024, 2_048, 4_096];
    if index.row_count != cold_vectors.row_count {
        return Err(invalid("V26 PQ4 cold-vector authority differs"));
    }
    let tables = crate::prepare_v26_pq4_query_tables(&index.codebook, query)?;
    let scan_started = std::time::Instant::now();
    let approximate = crate::rank_v26_pq4_fast_candidates_parallel(
        index,
        query,
        4_096,
        crate::V26Pq4Backend::Aarch64NeonTable,
    )?;
    let scan_elapsed_ns = u64::try_from(scan_started.elapsed().as_nanos())
        .map_err(|_| invalid("V26 PQ4 scan latency overflows"))?
        .max(1);
    let cold_started = std::time::Instant::now();
    let mut source_ordinals = approximate
        .iter()
        .map(|candidate| u32::try_from(candidate.source_ordinal))
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|_| invalid("V26 PQ4 source ordinal differs"))?;
    source_ordinals.sort_unstable();
    let cold = cold_vectors.read_rows(&source_ordinals)?;
    let cold_elapsed_ns = u64::try_from(cold_started.elapsed().as_nanos())
        .map_err(|_| invalid("V26 PQ4 cold-read latency overflows"))?
        .max(1);
    let mut maximum_error = 0.0_f32;
    for candidate in &approximate {
        let ordinal = usize::try_from(candidate.source_ordinal).unwrap();
        let row_in_block = ordinal % 32;
        let block = &index.blocks[ordinal / 32];
        let mut floating = 0.0_f32;
        for subspace in 0..32 {
            let packed = block[subspace * 16 + row_in_block / 2];
            let code = if row_in_block.is_multiple_of(2) {
                packed & 15
            } else {
                packed >> 4
            };
            let start = subspace * 3;
            let centroid = usize::from(code) * 3;
            floating += (0..3)
                .map(|dimension| {
                    let delta = query[start + dimension]
                        - index.codebook.centroids[subspace][centroid + dimension];
                    delta * delta
                })
                .sum::<f32>();
        }
        let quantized = tables.minima_sum + f32::from(candidate.score) * tables.scale;
        maximum_error = maximum_error.max((floating - quantized).abs());
    }
    if !maximum_error.is_finite() {
        return Err(invalid("V26 PQ4 quantization error differs"));
    }
    let selections = DEPTHS
        .into_iter()
        .map(|depth| {
            let rerank_started = std::time::Instant::now();
            let mut exact = approximate[..depth]
                .iter()
                .map(|candidate| {
                    let source_ordinal = u32::try_from(candidate.source_ordinal).unwrap();
                    let position = source_ordinals
                        .binary_search(&source_ordinal)
                        .map_err(|_| invalid("V26 PQ4 cold binding differs"))?;
                    let distance = v26_squared_l2(&cold.vectors[position], query);
                    let assignment = cold.assignments[position];
                    if assignment.source_ordinal != candidate.source_ordinal
                        || !distance.is_finite()
                    {
                        return Err(invalid("V26 PQ4 exact-row binding differs"));
                    }
                    Ok((
                        V26PqRankedRow {
                            source_ordinal: candidate.source_ordinal,
                            distance,
                        },
                        [assignment.primary_page, assignment.replica_page],
                    ))
                })
                .collect::<Result<Vec<_>>>()?;
            exact.sort_by_key(|entry| entry.0);
            let top = exact[..10]
                .iter()
                .map(|(_, pages)| pages.to_vec())
                .collect::<Vec<_>>();
            let mut selected_pages =
                exact_v26_layout_oracle_pages(&top, crate::V26_SERVING_PAGE_BUDGET)?;
            for (_, pages) in &exact {
                for page in pages {
                    if selected_pages.len() == crate::V26_SERVING_PAGE_BUDGET {
                        break;
                    }
                    if !selected_pages.contains(page) {
                        selected_pages.push(*page);
                    }
                }
            }
            if selected_pages.len() != crate::V26_SERVING_PAGE_BUDGET {
                return Err(invalid("V26 PQ4 selected-page inventory differs"));
            }
            selected_pages.sort_unstable();
            let rerank_elapsed_ns = u64::try_from(rerank_started.elapsed().as_nanos())
                .map_err(|_| invalid("V26 PQ4 rerank latency overflows"))?
                .checked_add(cold_elapsed_ns)
                .ok_or_else(|| invalid("V26 PQ4 rerank latency overflows"))?;
            Ok((
                V26Pq16ServingSelection {
                    selected_pages,
                    exact_rows_read: depth as u32,
                    cold_batches_read: cold.batches_read,
                    cold_read_workers: cold.read_workers,
                    page_body_reads: 0,
                },
                rerank_elapsed_ns,
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok((
        selections,
        scan_elapsed_ns,
        tables.scale.to_bits(),
        tables.saturation_count,
        maximum_error.to_bits(),
    ))
}

fn v26_pq4_quality_sample(
    ranked_row_limit: u32,
    query_ordinal: u32,
    selection: &V26Pq16ServingSelection,
    truth: &V26QueryTruth,
    scan_elapsed_ns: u64,
    exact_rerank_elapsed_ns: u64,
    quantization_scale_bits: u32,
    saturation_count: u32,
    maximum_distance_error_bits: u32,
) -> Result<V26Pq4QualitySample> {
    if truth.query_ordinal != query_ordinal
        || truth.neighbor_source_ordinals.len() != 10
        || truth.ground_truth_page_assignments.len() != 10
        || selection.exact_rows_read != ranked_row_limit
        || selection.selected_pages.len() != crate::V26_SERVING_PAGE_BUDGET
        || selection.page_body_reads != 0
    {
        return Err(invalid("V26 PQ4 quality selection differs"));
    }
    let oracle_pages = exact_v26_layout_oracle_pages(
        &truth.ground_truth_page_assignments,
        crate::V26_SERVING_PAGE_BUDGET,
    )?;
    let hits = truth
        .ground_truth_page_assignments
        .iter()
        .filter(|pages| {
            pages
                .iter()
                .any(|page| selection.selected_pages.binary_search(page).is_ok())
        })
        .count() as u32;
    let oracle_hits = truth
        .ground_truth_page_assignments
        .iter()
        .filter(|pages| {
            pages
                .iter()
                .any(|page| oracle_pages.binary_search(page).is_ok())
        })
        .count() as u32;
    Ok(V26Pq4QualitySample {
        ranked_row_limit,
        query_ordinal,
        selected_pages: selection.selected_pages.clone(),
        hits,
        oracle_hits,
        recall_ppm: u64::from(hits) * 100_000,
        oracle_attainment_ppm: u64::from(hits) * 1_000_000 / u64::from(oracle_hits),
        scan_elapsed_ns,
        exact_rerank_elapsed_ns,
        quantization_scale_bits,
        saturation_count,
        maximum_distance_error_bits,
        page_body_reads: 0,
    })
}

pub fn run_v26_pq4_quality_frontier(request: &V26Pq4QualityRequest) -> Result<Vec<u8>> {
    if request.evidence_output_path.exists()
        || !request.evidence_output_uri.starts_with("s3://")
        || !request.evidence_output_uri.ends_with(".parquet")
    {
        return Err(invalid("V26 PQ4 quality request differs"));
    }
    let manifest = read_v26_pq4_fast_manifest(&request.pq4_manifest)?;
    let terminal = read_layout_terminal(&request.layout_terminal)?;
    authenticate(&request.cold_vectors, "cold-vectors-arrow")?;
    authenticate(&request.external_queries, "external-queries-parquet")?;
    authenticate(&request.truth, "truth-parquet")?;
    let generation = &manifest.construction_rows.generation;
    let mut uris = BTreeSet::new();
    if manifest.layout_terminal != request.layout_terminal.identity
        || terminal.row_count != manifest.row_count
        || request.cold_vectors_manifest.row_count != manifest.row_count
        || request.cold_vectors_manifest.encoded_bytes
            != request.cold_vectors.identity.encoded_bytes
        || request.cold_vectors_manifest.sha256 != request.cold_vectors.identity.digest
        || [
            &request.pq4_manifest.identity,
            &request.cold_vectors.identity,
            &request.layout_terminal.identity,
            &request.external_queries.identity,
            &request.truth.identity,
        ]
        .iter()
        .any(|identity| identity.generation != *generation || !uris.insert(&identity.uri))
        || !uris.insert(&request.evidence_output_uri)
    {
        return Err(invalid("V26 PQ4 quality authority differs"));
    }
    let expected_names = [
        "pq4-fast-codebook.arrow",
        "pq4-fast-codes.arrow",
        "pq4-fast-manifest.json",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect::<BTreeSet<_>>();
    let observed_names = fs::read_dir(&request.pq4_dir)
        .map_err(|error| invalid(&format!("V26 PQ4 directory failed: {error}")))?
        .map(|entry| {
            entry
                .map_err(|error| invalid(&format!("V26 PQ4 directory failed: {error}")))?
                .file_name()
                .into_string()
                .map_err(|_| invalid("V26 PQ4 artifact name differs"))
        })
        .collect::<Result<BTreeSet<_>>>()?;
    if observed_names != expected_names {
        return Err(invalid("V26 PQ4 artifact inventory differs"));
    }
    let index = read_v26_pq4_fast_index_arrow(&request.pq4_dir, &manifest)?;
    let cold_vectors =
        V26ArrowColdVectors::open(&request.cold_vectors.path, &request.cold_vectors_manifest)?;
    let mut queries = read_evaluation_queries(&request.external_queries.path, 512)?;
    let mut truths = read_evaluation_truth_with_assignment(
        &request.truth.path,
        512,
        &queries,
        &manifest.construction_rows.digest,
        &request.external_queries.identity.digest,
        |neighbor| {
            let ordinal =
                u32::try_from(neighbor).map_err(|_| invalid("V26 PQ4 truth source differs"))?;
            cold_vectors.read_assignment(ordinal)
        },
    )?;
    queries.truncate(32);
    truths.truncate(32);
    let mut by_depth = vec![Vec::with_capacity(32); 4];
    for (query, truth) in queries.iter().zip(&truths) {
        let (selections, scan_ns, scale_bits, saturation_count, maximum_error_bits) =
            select_v26_pq4_quality_arms(&index, &query.vector, &cold_vectors)?;
        for (arm_index, (selection, rerank_ns)) in selections.iter().enumerate() {
            by_depth[arm_index].push(v26_pq4_quality_sample(
                selection.exact_rows_read,
                query.query_ordinal,
                selection,
                truth,
                scan_ns,
                *rerank_ns,
                scale_bits,
                saturation_count,
                maximum_error_bits,
            )?);
        }
    }
    let samples = by_depth.into_iter().flatten().collect::<Vec<_>>();
    let result = (|| {
        write_batch(
            &request.evidence_output_path,
            v26_pq4_quality_batch(&samples)?,
        )?;
        let slash = request
            .evidence_output_uri
            .rfind('/')
            .ok_or_else(|| invalid("V26 PQ4 evidence URI differs"))?;
        let evidence = output_identity(
            "pq4-fast-quality-evidence-parquet",
            &request.evidence_output_path,
            &request.evidence_output_uri[..=slash],
            generation,
        )?;
        if evidence.uri != request.evidence_output_uri {
            return Err(invalid("V26 PQ4 evidence URI differs"));
        }
        let result = summarize_v26_pq4_quality(
            request.pq4_manifest.identity.clone(),
            request.external_queries.identity.clone(),
            request.truth.identity.clone(),
            evidence,
            &samples,
        )?;
        canonical_v26_pq4_quality_result_bytes(&result, &samples)
    })();
    if result.is_err() {
        let _ = fs::remove_file(&request.evidence_output_path);
    }
    result
}

fn v26_pq16_serving_output_names() -> [&'static str; 7] {
    [
        "pq16-codebook.arrow",
        "pq16-codes.arrow",
        "pq16-postings.arrow",
        "cold-vectors.arrow",
        "simhash-pq16-codebook.arrow",
        "simhash-pq16-buckets.arrow",
        "simhash-pq16-records.arrow",
    ]
}

fn validate_v26_pq16_serving_build_output(
    request: &V26Pq16ServingBuildRequest,
    output: &V26Pq16ServingBuildOutput,
) -> Result<()> {
    if output.schema != "borsuk-v26-pq16-serving-manifest-v2"
        || output.inputs
            != [
                request.construction_rows.identity.clone(),
                request.page_assignments.identity.clone(),
                request.layout_terminal.identity.clone(),
                request.primary_tree.identity.clone(),
                request.replica_tree.identity.clone(),
            ]
        || output.row_count != request.expected_rows
        || output.index.row_count != output.row_count
        || output.cold_vectors.row_count != output.row_count
        || output.page_count != output.index.page_count
        || output.projected_resident_bytes_100m
            != output.simhash_index.projected_resident_bytes_100m
        || output.simhash_index.row_count != output.row_count
        || output.simhash_index.bucket_count != 65_536
        || output.outputs.len() != 7
        || output.query_role_opens != 0
        || output.page_body_reads != 0
        || output.claim_eligible
    {
        return Err(invalid("V26 PQ16 serving build output differs"));
    }
    let generation = &request.construction_rows.identity.generation;
    let expected_roles = [
        "pq16-codebook-arrow",
        "pq16-codes-arrow",
        "pq16-postings-arrow",
        "cold-vectors-arrow",
        "simhash-pq16-codebook-arrow",
        "simhash-pq16-buckets-arrow",
        "simhash-pq16-records-arrow",
    ];
    for ((identity, name), role) in output
        .outputs
        .iter()
        .zip(v26_pq16_serving_output_names())
        .zip(expected_roles)
    {
        let expected = output_identity(
            role,
            &request.output_dir.join(name),
            &request.output_uri_prefix,
            generation,
        )?;
        if identity != &expected {
            return Err(invalid("V26 PQ16 serving output identity differs"));
        }
    }
    if output.outputs[0].encoded_bytes != output.index.codebook.encoded_bytes
        || output.outputs[0].digest != output.index.codebook.sha256
        || output.outputs[1].encoded_bytes != output.index.codes.encoded_bytes
        || output.outputs[1].digest != output.index.codes.sha256
        || output.outputs[2].encoded_bytes != output.index.postings.encoded_bytes
        || output.outputs[2].digest != output.index.postings.sha256
        || output.outputs[3].encoded_bytes != output.cold_vectors.encoded_bytes
        || output.outputs[3].digest != output.cold_vectors.sha256
        || output.outputs[4].encoded_bytes != output.simhash_index.codebook.encoded_bytes
        || output.outputs[4].digest != output.simhash_index.codebook.sha256
        || output.outputs[5].encoded_bytes != output.simhash_index.buckets.encoded_bytes
        || output.outputs[5].digest != output.simhash_index.buckets.sha256
        || output.outputs[6].encoded_bytes != output.simhash_index.records.encoded_bytes
        || output.outputs[6].digest != output.simhash_index.records.sha256
    {
        return Err(invalid("V26 PQ16 serving manifest binding differs"));
    }
    Ok(())
}

pub fn canonical_v26_pq16_serving_build_output_bytes(
    request: &V26Pq16ServingBuildRequest,
    output: &V26Pq16ServingBuildOutput,
) -> Result<Vec<u8>> {
    validate_v26_pq16_serving_build_output(request, output)?;
    let value = serde_json::to_value(output)
        .map_err(|error| invalid(&format!("V26 PQ16 serving manifest failed: {error}")))?;
    let mut bytes = serde_json::to_vec(&canonical_json_value(value))
        .map_err(|error| invalid(&format!("V26 PQ16 serving manifest failed: {error}")))?;
    bytes.push(b'\n');
    Ok(bytes)
}

pub fn run_v26_pq16_serving_build(
    request: &V26Pq16ServingBuildRequest,
) -> Result<V26Pq16ServingBuildOutput> {
    let mut uris = BTreeSet::new();
    let uri_inventory_is_unique = [
        request.construction_rows.identity.uri.clone(),
        request.page_assignments.identity.uri.clone(),
        request.layout_terminal.identity.uri.clone(),
        request.primary_tree.identity.uri.clone(),
        request.replica_tree.identity.uri.clone(),
    ]
    .into_iter()
    .chain(
        v26_pq16_serving_output_names().map(|name| format!("{}{name}", request.output_uri_prefix)),
    )
    .all(|uri| uris.insert(uri));
    if request.expected_rows == 0
        || request.expected_rows > u64::from(u32::MAX)
        || request.output_dir.exists()
        || !request.output_uri_prefix.starts_with("s3://")
        || !request.output_uri_prefix.ends_with('/')
        || request.construction_rows.identity.generation
            != request.page_assignments.identity.generation
        || request.construction_rows.identity.generation
            != request.layout_terminal.identity.generation
        || request.construction_rows.identity.generation != request.primary_tree.identity.generation
        || request.construction_rows.identity.generation != request.replica_tree.identity.generation
        || !uri_inventory_is_unique
    {
        return Err(invalid("V26 PQ16 serving build request differs"));
    }
    authenticate(&request.construction_rows, "construction-parquet")?;
    authenticate(&request.page_assignments, "page-assignments-parquet")?;
    let terminal = read_layout_terminal(&request.layout_terminal)?;
    authenticate(&request.primary_tree, "primary-tree-parquet")?;
    authenticate(&request.replica_tree, "replica-tree-parquet")?;
    if terminal.authority.construction_rows != request.construction_rows.identity
        || !terminal
            .outputs
            .iter()
            .any(|identity| identity == &request.page_assignments.identity)
        || !terminal
            .outputs
            .iter()
            .any(|identity| identity == &request.primary_tree.identity)
        || !terminal
            .outputs
            .iter()
            .any(|identity| identity == &request.replica_tree.identity)
        || terminal.row_count != request.expected_rows
    {
        return Err(invalid("V26 PQ16 serving layout authority differs"));
    }
    let rows = read_construction_rows(&request.construction_rows.path, request.expected_rows)?;
    let assignments = read_assignments(
        &request.page_assignments.path,
        i64::try_from(request.expected_rows)
            .map_err(|_| invalid("V26 assignment row count overflows"))?,
    )?;
    if assignments.iter().enumerate().any(|(ordinal, assignment)| {
        usize::try_from(assignment.source_ordinal).ok() != Some(ordinal)
            || assignment.primary_page == assignment.replica_page
    }) {
        return Err(invalid("V26 assignment inventory differs"));
    }
    let node_count = i64::from(terminal.leaves_per_tree)
        .checked_mul(2)
        .and_then(|value| value.checked_sub(1))
        .ok_or_else(|| invalid("V26 serving tree node count overflows"))?;
    let primary = read_tree(
        &request.primary_tree.path,
        node_count,
        terminal.authority.primary_seed,
    )?;
    let replica = read_tree(
        &request.replica_tree.path,
        node_count,
        terminal.authority.replica_seed,
    )?;
    validate_v26_dual_tree_layout(&terminal.authority, &primary, &replica, &assignments)?;
    let index = crate::build_v26_pq16_packed_index(&rows, &assignments)?;
    let simhash_index = crate::build_v26_simhash_pq16_multi_index(&index, &rows)?;
    fs::create_dir(&request.output_dir)
        .map_err(|error| invalid(&format!("V26 serving output directory failed: {error}")))?;
    let result = (|| {
        let index_manifest = write_v26_pq16_index_arrow(&request.output_dir, &index, &assignments)?;
        let cold_vectors = write_v26_cold_vectors_arrow(
            &request.output_dir.join("cold-vectors.arrow"),
            &rows,
            &assignments,
            V26_COLD_VECTOR_BATCH_ROWS,
        )?;
        let simhash_manifest =
            write_v26_simhash_pq16_index_arrow(&request.output_dir, &simhash_index)?;
        let roles = [
            "pq16-codebook-arrow",
            "pq16-codes-arrow",
            "pq16-postings-arrow",
            "cold-vectors-arrow",
            "simhash-pq16-codebook-arrow",
            "simhash-pq16-buckets-arrow",
            "simhash-pq16-records-arrow",
        ];
        let outputs = v26_pq16_serving_output_names()
            .into_iter()
            .zip(roles)
            .map(|(name, role)| {
                output_identity(
                    role,
                    &request.output_dir.join(name),
                    &request.output_uri_prefix,
                    &request.construction_rows.identity.generation,
                )
            })
            .collect::<Result<Vec<_>>>()?;
        let output = V26Pq16ServingBuildOutput {
            schema: "borsuk-v26-pq16-serving-manifest-v2".to_owned(),
            inputs: vec![
                request.construction_rows.identity.clone(),
                request.page_assignments.identity.clone(),
                request.layout_terminal.identity.clone(),
                request.primary_tree.identity.clone(),
                request.replica_tree.identity.clone(),
            ],
            outputs,
            row_count: request.expected_rows,
            page_count: index_manifest.page_count,
            projected_resident_bytes_100m: simhash_manifest.projected_resident_bytes_100m,
            index: index_manifest,
            simhash_index: simhash_manifest,
            cold_vectors,
            query_role_opens: 0,
            page_body_reads: 0,
            claim_eligible: false,
        };
        let bytes = canonical_v26_pq16_serving_build_output_bytes(request, &output)?;
        fs::write(request.output_dir.join("serving-manifest.json"), bytes)
            .map_err(|error| invalid(&format!("V26 serving manifest write failed: {error}")))?;
        Ok(output)
    })();
    if result.is_err() {
        for name in v26_pq16_serving_output_names() {
            let _ = fs::remove_file(request.output_dir.join(name));
        }
        let _ = fs::remove_file(request.output_dir.join("serving-manifest.json"));
        let _ = fs::remove_dir(&request.output_dir);
    }
    result
}

fn read_v26_pq16_serving_manifest(
    object: &V26LocalObjectPath,
) -> Result<V26Pq16ServingBuildOutput> {
    authenticate(object, "pq16-serving-manifest")?;
    let bytes = fs::read(&object.path)
        .map_err(|error| invalid(&format!("V26 serving manifest read failed: {error}")))?;
    if bytes.last() != Some(&b'\n') || bytes[..bytes.len() - 1].contains(&b'\n') {
        return Err(invalid("V26 serving manifest bytes differ"));
    }
    let output: V26Pq16ServingBuildOutput = serde_json::from_slice(&bytes)
        .map_err(|error| invalid(&format!("V26 serving manifest parse failed: {error}")))?;
    let mut expected = serde_json::to_vec(&canonical_json_value(
        serde_json::to_value(&output)
            .map_err(|error| invalid(&format!("V26 serving manifest failed: {error}")))?,
    ))
    .map_err(|error| invalid(&format!("V26 serving manifest failed: {error}")))?;
    expected.push(b'\n');
    let roles = [
        "pq16-codebook-arrow",
        "pq16-codes-arrow",
        "pq16-postings-arrow",
        "cold-vectors-arrow",
        "simhash-pq16-codebook-arrow",
        "simhash-pq16-buckets-arrow",
        "simhash-pq16-records-arrow",
    ];
    let generation = output
        .inputs
        .first()
        .map(|identity| identity.generation.as_str())
        .unwrap_or_default();
    let mut uris = BTreeSet::new();
    if bytes != expected
        || output.schema != "borsuk-v26-pq16-serving-manifest-v2"
        || output.inputs.len() != 5
        || output.inputs[0].role != "construction-parquet"
        || output.inputs[1].role != "page-assignments-parquet"
        || output.inputs[2].role != "layout-terminal"
        || output.inputs[3].role != "primary-tree-parquet"
        || output.inputs[4].role != "replica-tree-parquet"
        || output.inputs.iter().any(|identity| {
            identity.generation != generation
                || identity.digest_algorithm != "sha256"
                || !exact_lower_hex(&identity.digest, 64)
                || identity.encoded_bytes == 0
                || !identity.uri.starts_with("s3://")
                || !uris.insert(identity.uri.clone())
        })
        || output.outputs.len() != roles.len()
        || output.outputs.iter().zip(roles).any(|(identity, role)| {
            identity.role != role
                || identity.generation != generation
                || identity.digest_algorithm != "sha256"
                || !exact_lower_hex(&identity.digest, 64)
                || identity.encoded_bytes == 0
                || !identity.uri.starts_with("s3://")
                || !uris.insert(identity.uri.clone())
        })
        || object.identity.generation != generation
        || output.row_count == 0
        || output.row_count != output.index.row_count
        || output.row_count != output.cold_vectors.row_count
        || output.page_count == 0
        || output.page_count != output.index.page_count
        || output.projected_resident_bytes_100m
            != output.simhash_index.projected_resident_bytes_100m
        || output.simhash_index.row_count != output.row_count
        || output.simhash_index.bucket_count != 65_536
        || output.outputs[0].encoded_bytes != output.index.codebook.encoded_bytes
        || output.outputs[0].digest != output.index.codebook.sha256
        || output.outputs[1].encoded_bytes != output.index.codes.encoded_bytes
        || output.outputs[1].digest != output.index.codes.sha256
        || output.outputs[2].encoded_bytes != output.index.postings.encoded_bytes
        || output.outputs[2].digest != output.index.postings.sha256
        || output.outputs[3].encoded_bytes != output.cold_vectors.encoded_bytes
        || output.outputs[3].digest != output.cold_vectors.sha256
        || output.outputs[4].encoded_bytes != output.simhash_index.codebook.encoded_bytes
        || output.outputs[4].digest != output.simhash_index.codebook.sha256
        || output.outputs[5].encoded_bytes != output.simhash_index.buckets.encoded_bytes
        || output.outputs[5].digest != output.simhash_index.buckets.sha256
        || output.outputs[6].encoded_bytes != output.simhash_index.records.encoded_bytes
        || output.outputs[6].digest != output.simhash_index.records.sha256
        || output.query_role_opens != 0
        || output.page_body_reads != 0
        || output.claim_eligible
        || !uris.insert(object.identity.uri.clone())
    {
        return Err(invalid("V26 serving manifest authority differs"));
    }
    Ok(output)
}

pub fn open_v26_pq16_serving_runtime(
    request: &V26Pq16ServingRuntimeRequest,
) -> Result<V26Pq16ServingRuntime> {
    let manifest = read_v26_pq16_serving_manifest(&request.serving_manifest)?;
    let terminal = read_layout_terminal(&request.layout_terminal)?;
    if manifest.inputs[0] != terminal.authority.construction_rows
        || !terminal
            .outputs
            .iter()
            .any(|identity| identity == &manifest.inputs[1])
        || manifest.inputs[2] != request.layout_terminal.identity
        || manifest.inputs[3] != request.primary_tree.identity
        || manifest.inputs[4] != request.replica_tree.identity
        || manifest.row_count != terminal.row_count
        || manifest.page_count != terminal.page_count
        || request.expected_queries != 512
        || request.external_queries.identity.generation != terminal.authority.generation
    {
        return Err(invalid("V26 serving router binding differs"));
    }
    let expected_names = v26_pq16_serving_output_names()
        .into_iter()
        .chain(std::iter::once("serving-manifest.json"))
        .map(str::to_owned)
        .collect::<BTreeSet<_>>();
    let observed_names = fs::read_dir(&request.serving_dir)
        .map_err(|error| invalid(&format!("V26 serving directory read failed: {error}")))?
        .map(|entry| {
            entry
                .map_err(|error| invalid(&format!("V26 serving directory read failed: {error}")))
                .and_then(|entry| {
                    entry
                        .file_name()
                        .into_string()
                        .map_err(|_| invalid("V26 serving artifact name differs"))
                })
        })
        .collect::<Result<BTreeSet<_>>>()?;
    if observed_names != expected_names {
        return Err(invalid("V26 serving artifact inventory differs"));
    }
    let index = read_v26_pq16_index_arrow(&request.serving_dir, &manifest.index)?;
    let cold_vectors = V26ArrowColdVectors::open(
        &request.serving_dir.join("cold-vectors.arrow"),
        &manifest.cold_vectors,
    )?;
    authenticate(&request.primary_tree, "primary-tree-parquet")?;
    authenticate(&request.replica_tree, "replica-tree-parquet")?;
    authenticate(&request.external_queries, "external-queries-parquet")?;
    let node_count = i64::from(terminal.leaves_per_tree)
        .checked_mul(2)
        .and_then(|value| value.checked_sub(1))
        .ok_or_else(|| invalid("V26 serving tree node count overflows"))?;
    let primary = read_tree(
        &request.primary_tree.path,
        node_count,
        terminal.authority.primary_seed,
    )?;
    let replica = read_tree(
        &request.replica_tree.path,
        node_count,
        terminal.authority.replica_seed,
    )?;
    let queries =
        read_evaluation_queries(&request.external_queries.path, request.expected_queries)?;
    if index.page_offsets.len() != usize::try_from(manifest.page_count).unwrap() + 1 {
        return Err(invalid("V26 serving page inventory differs"));
    }
    Ok(V26Pq16ServingRuntime {
        index,
        cold_vectors,
        primary,
        replica,
        queries,
    })
}

pub fn validate_v26_layout_build_output(
    request: &V26LayoutBuildRequest,
    output: &V26LayoutBuildOutput,
) -> Result<()> {
    validate_uri_inventory(request)?;
    let observed_authority = read_manifest(&request.manifest)?;
    authenticate(&request.construction_rows, "construction-parquet")?;
    if output.authority != observed_authority
        || output.authority.generation != request.manifest.identity.generation
        || output.inputs
            != vec![
                request.construction_rows.identity.clone(),
                request.manifest.identity.clone(),
            ]
        || output.row_count != output.authority.expected_rows
        || output.worker_count as usize != request.worker_count
        || output.outputs.len() != 3
    {
        return Err(invalid("V26 layout build output differs"));
    }
    for (identity, role, name) in [
        (
            &output.outputs[0],
            "page-assignments-parquet",
            "page-assignments.parquet",
        ),
        (
            &output.outputs[1],
            "primary-tree-parquet",
            "primary-tree.parquet",
        ),
        (
            &output.outputs[2],
            "replica-tree-parquet",
            "replica-tree.parquet",
        ),
    ] {
        let observed = V26LocalObjectPath {
            identity: identity.clone(),
            path: request.output_dir.join(name),
        };
        authenticate(&observed, role)?;
        if identity.generation != output.authority.generation
            || identity.uri != format!("{}{name}", request.output_uri_prefix)
        {
            return Err(invalid("V26 output identity differs"));
        }
    }
    let node_count = i64::from(output.leaves_per_tree) * 2 - 1;
    let primary = read_tree(
        &request.output_dir.join("primary-tree.parquet"),
        node_count,
        output.authority.primary_seed,
    )?;
    let replica = read_tree(
        &request.output_dir.join("replica-tree.parquet"),
        node_count,
        output.authority.replica_seed,
    )?;
    let assignment_rows = i64::try_from(output.row_count)
        .map_err(|_| invalid("V26 assignment row count overflows"))?;
    let assignments = read_assignments(
        &request.output_dir.join("page-assignments.parquet"),
        assignment_rows,
    )?;
    validate_v26_dual_tree_layout(&output.authority, &primary, &replica, &assignments)?;
    let leaves = output
        .row_count
        .div_ceil(u64::from(output.authority.page_capacity));
    if output.leaves_per_tree as u64 != leaves
        || output.page_count as u64 != leaves * 2
        || output.projection_steps
            != projected_steps(output.row_count, leaves, output.authority.page_capacity)?
                .checked_mul(2)
                .ok_or_else(|| invalid("V26 projection work overflows"))?
    {
        return Err(invalid("V26 layout build counts differ"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, BTreeSet},
        fs,
        io::Write,
        sync::Arc,
    };

    use arrow_array::{
        ArrayRef, FixedSizeListArray, Float32Array, RecordBatch, StringArray, UInt32Array,
        UInt64Array,
    };
    use arrow_schema::{DataType, Field};
    use parquet::{
        arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder},
        basic::Compression,
        file::properties::{WriterProperties, WriterVersion},
    };
    use sha2::{Digest, Sha256};
    use tempfile::TempDir;

    use super::{
        V26ArrowColdVectors, V26CandidateCoverRequest, V26CentroidRouterRequest,
        V26ExactGlobalRequest, V26LayoutBuildRequest, V26LayoutEvaluationRequest,
        V26LocalObjectPath, V26PageModeRouterRequest, V26Pq4FastBuildRequest, V26Pq4FastManifest,
        V26Pq4QualityRequest, V26Pq4QualitySample, V26Pq8CoverRequest, V26Pq16GlobalQualityResult,
        V26Pq16GlobalQualitySample, V26Pq16RerankRequest, V26Pq16ServingBuildRequest,
        V26Pq16ServingRuntimeRequest, V26PqWidthLadderRequest, V26ServingLatencySample,
        V26TreeRouterRequest, V26TruthBuildRequest, assignments_batch,
        canonical_v26_pq4_fast_manifest_bytes, canonical_v26_pq4_quality_result_bytes,
        canonical_v26_pq16_serving_benchmark_result_bytes, evaluate_v26_exact_global,
        evaluate_v26_layout_oracle, evaluate_v26_layout_oracle_with_page_budget, open_reader,
        open_v26_pq16_serving_runtime, output_identity, read_assignments, read_construction_rows,
        read_evaluation_queries, read_evaluation_truth, read_layout_terminal,
        read_v26_pq4_fast_index_arrow, read_v26_pq16_index_arrow, run_v26_candidate_row_cover,
        run_v26_centroid_router, run_v26_global_centroid_frontier_diagnostic,
        run_v26_global_page_mode_frontier_diagnostic, run_v26_layout_build,
        run_v26_page_mode_router, run_v26_pq_width_ladder, run_v26_pq4_fast_build,
        run_v26_pq4_quality_frontier, run_v26_pq8_candidate_cover, run_v26_pq16_exact_rerank,
        run_v26_pq16_serving_build, run_v26_tree_router, run_v26_tree_router_diagnostic,
        run_v26_truth_build, select_v26_pq16_global_pages_from_arrow,
        select_v26_pq16_pages_from_arrow, summarize_v26_pq4_quality, v26_construction_schema,
        v26_page_assignments_schema, v26_query_schema, v26_tree_schema, v26_truth_schema,
        validate_v26_layout_build_output, write_v26_cold_vectors_arrow,
        write_v26_pq4_fast_index_arrow, write_v26_pq16_index_arrow,
    };
    use crate::{
        V26Disposition, V26LayoutAuthority, V26LayoutReceipt, V26ObjectIdentity,
        canonical_json_value, canonical_v26_layout_receipt_bytes,
        canonical_v26_layout_result_bytes,
    };

    fn write_parquet(path: &std::path::Path, batch: &RecordBatch) {
        let properties = WriterProperties::builder()
            .set_compression(Compression::ZSTD(Default::default()))
            .set_writer_version(WriterVersion::PARQUET_2_0)
            .build();
        let file = fs::File::create(path).unwrap();
        let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(properties)).unwrap();
        writer.write(batch).unwrap();
        writer.close().unwrap();
    }

    fn identity(role: &str, path: &std::path::Path) -> V26LocalObjectPath {
        let bytes = fs::read(path).unwrap();
        V26LocalObjectPath {
            identity: V26ObjectIdentity {
                role: role.to_owned(),
                uri: format!("s3://v26-input/{role}"),
                digest_algorithm: "sha256".to_owned(),
                digest: format!("{:x}", Sha256::digest(&bytes)),
                encoded_bytes: bytes.len() as u64,
                generation: "v26-local-test".to_owned(),
            },
            path: path.to_owned(),
        }
    }

    fn fixture_with_rows(expected_rows: u64) -> (TempDir, V26LocalObjectPath, V26LocalObjectPath) {
        let temp = TempDir::new().unwrap();
        let ordinals = UInt64Array::from_iter_values(0..expected_rows);
        let mut flat = Vec::with_capacity(expected_rows as usize * 96);
        for ordinal in 0..expected_rows as usize {
            for dimension in 0..96 {
                flat.push(if dimension == ordinal % 96 { 1.0 } else { 0.0 });
            }
        }
        let vectors = FixedSizeListArray::try_new(
            Arc::new(Field::new("element", DataType::Float32, false)),
            96,
            Arc::new(Float32Array::from(flat)),
            None,
        )
        .unwrap();
        let construction = RecordBatch::try_new(
            Arc::new(v26_construction_schema()),
            vec![Arc::new(ordinals.clone()) as ArrayRef, Arc::new(vectors)],
        )
        .unwrap();
        let construction_path = temp.path().join("construction.parquet");
        write_parquet(&construction_path, &construction);

        let construction = identity("construction-parquet", &construction_path);
        let authority = V26LayoutAuthority {
            schema: "borsuk-v26-dual-tree-layout-v2".to_owned(),
            generation: "v26-local-test".to_owned(),
            source_commit: "1".repeat(40),
            source_archive_sha256: "2".repeat(64),
            binary: V26ObjectIdentity {
                role: "v26-layout-binary".to_owned(),
                uri: "s3://v26-input/v26-layout-binary".to_owned(),
                digest_algorithm: "sha256".to_owned(),
                digest: "9".repeat(64),
                encoded_bytes: 4096,
                generation: "v26-local-test".to_owned(),
            },
            construction_rows: construction.identity.clone(),
            primary_seed: 0x5632_362d_5452_4545,
            replica_seed: 0x5632_362d_5245_504c,
            page_capacity: 704,
            expected_rows,
        };
        let manifest_path = temp.path().join("manifest.json");
        let mut manifest_bytes = serde_json::to_vec(&canonical_json_value(
            serde_json::to_value(&authority).unwrap(),
        ))
        .unwrap();
        manifest_bytes.push(b'\n');
        fs::write(&manifest_path, manifest_bytes).unwrap();
        (
            temp,
            identity("layout-manifest", &manifest_path),
            construction,
        )
    }

    fn fixture() -> (TempDir, V26LocalObjectPath, V26LocalObjectPath) {
        fixture_with_rows(1_409)
    }

    fn request(
        manifest: V26LocalObjectPath,
        construction_rows: V26LocalObjectPath,
        output_dir: std::path::PathBuf,
        worker_count: usize,
    ) -> V26LayoutBuildRequest {
        V26LayoutBuildRequest {
            manifest,
            construction_rows,
            output_dir,
            output_uri_prefix: "s3://v26-output/layout-a/".to_owned(),
            worker_count,
        }
    }

    fn fixed_u32(values: Vec<u32>, width: i32) -> FixedSizeListArray {
        FixedSizeListArray::try_new(
            Arc::new(Field::new("element", DataType::UInt32, false)),
            width,
            Arc::new(UInt32Array::from(values)),
            None,
        )
        .unwrap()
    }

    fn evaluation_fixture_with_rows(expected_rows: u64) -> (TempDir, V26LayoutEvaluationRequest) {
        let (temp, manifest, construction) = fixture_with_rows(expected_rows);
        let output_dir = temp.path().join("layout");
        let build_request = request(manifest, construction, output_dir.clone(), 2);
        let build = run_v26_layout_build(&build_request).unwrap();
        let receipt = V26LayoutReceipt {
            authority: build.authority.clone(),
            inputs: build.inputs.clone(),
            outputs: build.outputs.clone(),
            row_count: build.row_count,
            leaves_per_tree: build.leaves_per_tree,
            page_count: build.page_count,
            projection_steps: build.projection_steps,
            worker_count: build.worker_count,
            elapsed_ns: 1,
            cpu_ns: 1,
            peak_rss_bytes: 1,
            peak_psi_full_avg10_milli_percent: 0,
            swap_start_bytes: 0,
            swap_end_bytes: 0,
            query_role_opens: 0,
            page_body_reads: 0,
            claim_eligible: false,
        };
        let terminal_path = temp.path().join("layout-terminal.json");
        fs::write(
            &terminal_path,
            canonical_v26_layout_receipt_bytes(&receipt).unwrap(),
        )
        .unwrap();

        let query_ordinals = UInt32Array::from_iter_values(0..512_u32);
        let mut query_values = Vec::with_capacity(10_000 * 96);
        for query in 0..10_000 {
            for dimension in 0..96 {
                query_values.push(if dimension == query % 96 { 1.0 } else { 0.0 });
            }
        }
        let query_vectors = FixedSizeListArray::try_new(
            Arc::new(Field::new("element", DataType::Float32, false)),
            96,
            Arc::new(Float32Array::from(query_values)),
            None,
        )
        .unwrap();
        let query_batch =
            RecordBatch::try_new(Arc::new(v26_query_schema()), vec![Arc::new(query_vectors)])
                .unwrap();
        let query_path = temp.path().join("queries.parquet");
        write_parquet(&query_path, &query_batch);
        let external_queries = identity("external-queries-parquet", &query_path);

        let neighbors = (512_u64..522).collect::<Vec<_>>();
        let mut neighbor_values = Vec::with_capacity(512 * 10);
        let mut distance_values = Vec::with_capacity(512 * 10);
        for _ in 0..512 {
            neighbor_values.extend_from_slice(&neighbors);
            for rank in 0..10 {
                distance_values.push((rank as f32 / 10.0).to_bits());
            }
        }
        let truth_batch = RecordBatch::try_new(
            Arc::new(v26_truth_schema()),
            vec![
                Arc::new(query_ordinals),
                Arc::new(
                    FixedSizeListArray::try_new(
                        Arc::new(Field::new("element", DataType::UInt64, false)),
                        10,
                        Arc::new(UInt64Array::from(neighbor_values)),
                        None,
                    )
                    .unwrap(),
                ),
                Arc::new(fixed_u32(distance_values, 10)),
                Arc::new(StringArray::from(vec![
                    build
                        .authority
                        .construction_rows
                        .digest
                        .as_str();
                    512
                ])),
                Arc::new(StringArray::from(vec![
                    external_queries
                        .identity
                        .digest
                        .as_str();
                    512
                ])),
            ],
        )
        .unwrap();
        let truth_path = temp.path().join("truth.parquet");
        write_parquet(&truth_path, &truth_batch);

        let page_assignments = V26LocalObjectPath {
            identity: build.outputs[0].clone(),
            path: output_dir.join("page-assignments.parquet"),
        };
        let request = V26LayoutEvaluationRequest {
            layout_terminal: identity("layout-terminal", &terminal_path),
            page_assignments,
            external_queries,
            truth: identity("truth-parquet", &truth_path),
            expected_queries: 512,
        };
        (temp, request)
    }

    fn evaluation_fixture() -> (TempDir, V26LayoutEvaluationRequest) {
        evaluation_fixture_with_rows(1_409)
    }

    fn rewrite_evaluation_truth_neighbors(
        request: &mut V26LayoutEvaluationRequest,
        neighbors: &[u64; 10],
    ) {
        let terminal = read_layout_terminal(&request.layout_terminal).unwrap();
        let query_ordinals = UInt32Array::from_iter_values(0..512_u32);
        let mut neighbor_values = Vec::with_capacity(512 * 10);
        let mut distance_values = Vec::with_capacity(512 * 10);
        for _ in 0..512 {
            neighbor_values.extend_from_slice(neighbors);
            for rank in 0..10 {
                distance_values.push((rank as f32 / 10.0).to_bits());
            }
        }
        let truth_batch = RecordBatch::try_new(
            Arc::new(v26_truth_schema()),
            vec![
                Arc::new(query_ordinals),
                Arc::new(
                    FixedSizeListArray::try_new(
                        Arc::new(Field::new("element", DataType::UInt64, false)),
                        10,
                        Arc::new(UInt64Array::from(neighbor_values)),
                        None,
                    )
                    .unwrap(),
                ),
                Arc::new(fixed_u32(distance_values, 10)),
                Arc::new(StringArray::from(vec![
                    terminal
                        .authority
                        .construction_rows
                        .digest
                        .as_str();
                    512
                ])),
                Arc::new(StringArray::from(vec![
                    request
                        .external_queries
                        .identity
                        .digest
                        .as_str();
                    512
                ])),
            ],
        )
        .unwrap();
        write_parquet(&request.truth.path, &truth_batch);
        request.truth.identity = identity("truth-parquet", &request.truth.path).identity;
    }

    #[test]
    fn v26_layout_local_authenticates_construction_only_and_emits_parquet() {
        // Break caught: parsing before authentication or emitting a nondeterministic layout.
        let (temp, manifest, construction) = fixture();
        let first_dir = temp.path().join("out-one");
        let second_dir = temp.path().join("out-four");
        let first = run_v26_layout_build(&request(
            manifest.clone(),
            construction.clone(),
            first_dir.clone(),
            1,
        ))
        .unwrap();
        let second_request = request(manifest, construction, second_dir.clone(), 4);
        let second = run_v26_layout_build(&second_request).unwrap();
        assert_eq!(first.row_count, 1_409);
        assert_eq!(first.leaves_per_tree, 3);
        assert_eq!(first.page_count, 6);
        assert_eq!(first.projection_steps, 6_494_208);
        assert_eq!(first.outputs, second.outputs);
        validate_v26_layout_build_output(&second_request, &second).unwrap();

        for (name, schema, rows) in [
            (
                "page-assignments.parquet",
                v26_page_assignments_schema(),
                1_409,
            ),
            ("primary-tree.parquet", v26_tree_schema(), 5),
            ("replica-tree.parquet", v26_tree_schema(), 5),
        ] {
            let reader = ParquetRecordBatchReaderBuilder::try_new(
                fs::File::open(second_dir.join(name)).unwrap(),
            )
            .unwrap();
            assert_eq!(reader.schema().as_ref(), &schema);
            assert_eq!(reader.metadata().file_metadata().num_rows(), rows);
            assert_eq!(
                fs::read(first_dir.join(name)).unwrap(),
                fs::read(second_dir.join(name)).unwrap()
            );
        }
    }

    #[test]
    fn v26_external_query_truth_local_writes_only_ranked_parquet_evidence() {
        // Break caught: the truth phase requires a layout/page role or emits JSON bulk data.
        let (temp, evaluation) = evaluation_fixture();
        let output_path = temp.path().join("external-truth.parquet");
        let external_queries = evaluation.external_queries;
        let request = V26TruthBuildRequest {
            construction_rows: identity(
                "construction-parquet",
                &temp.path().join("construction.parquet"),
            ),
            external_queries,
            expected_rows: 1_409,
            expected_queries: 512,
            output_path: output_path.clone(),
            output_uri: "s3://v26-output/external-truth.parquet".to_owned(),
        };

        let output = run_v26_truth_build(&request).unwrap();

        assert_eq!(output.identity.role, "external-truth-parquet");
        assert_eq!(output.identity.uri, request.output_uri);
        assert_eq!(output.path, output_path);
        let reader =
            ParquetRecordBatchReaderBuilder::try_new(fs::File::open(&output.path).unwrap())
                .unwrap();
        assert_eq!(reader.schema().as_ref(), &v26_truth_schema());
        assert_eq!(reader.metadata().file_metadata().num_rows(), 512);
        assert_eq!(reader.schema().field(3).name(), "construction_sha256");
        assert_eq!(reader.schema().field(4).name(), "external_queries_sha256");
    }

    #[test]
    fn v26_layout_local_rejects_query_truth_and_result_roles() {
        // Break caught: construction gains a query/evaluation capability.
        for forbidden in ["external-queries-parquet", "truth-parquet", "prior-result"] {
            let (temp, manifest, mut construction) = fixture();
            construction.identity.role = forbidden.to_owned();
            let output = temp.path().join("forbidden-output");
            assert!(
                run_v26_layout_build(&request(manifest, construction, output.clone(), 1)).is_err()
            );
            assert!(!output.exists());
        }
    }

    #[test]
    fn v26_layout_local_rejects_input_output_uri_role_overlap() {
        // Break caught: one immutable URI is assigned both an input and output role.
        let (temp, manifest, mut construction) = fixture();
        construction.identity.uri = "s3://v26-output/layout-a/page-assignments.parquet".to_owned();
        let output_dir = temp.path().join("overlap-output");
        assert!(
            run_v26_layout_build(&request(manifest, construction, output_dir.clone(), 1,)).is_err()
        );
        assert!(!output_dir.exists());
    }

    #[test]
    fn v26_layout_local_manifest_rejects_coherent_input_substitution() {
        // Break caught: a different URI with identical valid bytes replaces a frozen input.
        let (temp, manifest, construction) = fixture();
        let alternate_path = temp.path().join("alternate-construction.parquet");
        fs::copy(&construction.path, &alternate_path).unwrap();
        let mut alternate = identity("construction-parquet", &alternate_path);
        alternate.identity.uri = "s3://v26-input/alternate-construction-parquet".to_owned();
        let output_dir = temp.path().join("substituted-output");
        assert!(
            run_v26_layout_build(&request(manifest, alternate, output_dir.clone(), 1,)).is_err()
        );
        assert!(!output_dir.exists());
    }

    #[test]
    fn v26_layout_local_smoke_uses_exact_registered_prefix_without_conversion() {
        // Break caught: the structural smoke requires a separately materialized corpus.
        let (temp, manifest, construction) = fixture();
        let mut authority: V26LayoutAuthority =
            serde_json::from_slice(&fs::read(&manifest.path).unwrap()).unwrap();
        authority.expected_rows = 705;
        let mut bytes = serde_json::to_vec(&canonical_json_value(
            serde_json::to_value(&authority).unwrap(),
        ))
        .unwrap();
        bytes.push(b'\n');
        fs::write(&manifest.path, bytes).unwrap();
        let manifest = identity("layout-manifest", &manifest.path);
        let output = run_v26_layout_build(&request(
            manifest,
            construction,
            temp.path().join("prefix-output"),
            2,
        ))
        .unwrap();
        assert_eq!(output.row_count, 705);
        assert_eq!(output.leaves_per_tree, 2);
        assert_eq!(output.page_count, 4);
    }

    #[test]
    fn v26_layout_local_rejects_output_schema_topology_and_identity_drift() {
        // Break caught: a validated output is modified before receipt sealing.
        let (temp, manifest, construction) = fixture();
        let output_dir = temp.path().join("outputs");
        let request = request(manifest, construction, output_dir.clone(), 1);
        let output = run_v26_layout_build(&request).unwrap();
        let assignments = output_dir.join("page-assignments.parquet");
        fs::OpenOptions::new()
            .append(true)
            .open(assignments)
            .unwrap()
            .write_all(b"drift")
            .unwrap();
        assert!(validate_v26_layout_build_output(&request, &output).is_err());
    }

    #[test]
    fn v26_layout_local_reauthenticates_inputs_before_sealing_output() {
        // Break caught: an authenticated construction input changes while the layout is built.
        let (temp, manifest, construction) = fixture();
        let output_dir = temp.path().join("outputs");
        let request = request(manifest, construction, output_dir, 1);
        let output = run_v26_layout_build(&request).unwrap();
        fs::OpenOptions::new()
            .append(true)
            .open(&request.construction_rows.path)
            .unwrap()
            .write_all(b"drift")
            .unwrap();
        assert!(validate_v26_layout_build_output(&request, &output).is_err());
    }

    #[test]
    fn v26_layout_local_rejects_rehashed_semantic_parquet_drift() {
        // Break caught: byte authority is refreshed around a duplicate assignment ordinal.
        let (temp, manifest, construction) = fixture();
        let output_dir = temp.path().join("outputs");
        let request = request(manifest, construction, output_dir.clone(), 1);
        let mut output = run_v26_layout_build(&request).unwrap();
        let assignment_path = output_dir.join("page-assignments.parquet");
        let mut assignments = read_assignments(&assignment_path, 1_409).unwrap();
        assignments[1].source_ordinal = assignments[0].source_ordinal;
        fs::remove_file(&assignment_path).unwrap();
        write_parquet(&assignment_path, &assignments_batch(&assignments).unwrap());
        output.outputs[0] = output_identity(
            "page-assignments-parquet",
            &assignment_path,
            &request.output_uri_prefix,
            &output.authority.generation,
        )
        .unwrap();
        assert!(validate_v26_layout_build_output(&request, &output).is_err());
    }

    #[test]
    fn v26_layout_oracle_evaluation_opens_truth_only_after_layout_terminal() {
        // Break caught: evaluation inputs are opened before the construction terminal closes.
        let temp = TempDir::new().unwrap();
        let terminal_path = temp.path().join("layout-terminal.json");
        fs::write(&terminal_path, b"{}\n").unwrap();
        let terminal = identity("layout-terminal", &terminal_path);
        let missing = |role: &str| V26LocalObjectPath {
            identity: V26ObjectIdentity {
                role: role.to_owned(),
                uri: format!("s3://v26-evaluation/{role}"),
                digest_algorithm: "sha256".to_owned(),
                digest: "0".repeat(64),
                encoded_bytes: 1,
                generation: "v26-local-test".to_owned(),
            },
            path: temp.path().join(format!("missing-{role}")),
        };
        let request = V26LayoutEvaluationRequest {
            layout_terminal: terminal,
            page_assignments: missing("page-assignments-parquet"),
            external_queries: missing("external-queries-parquet"),
            truth: missing("truth-parquet"),
            expected_queries: 512,
        };
        let error = evaluate_v26_layout_oracle(&request).unwrap_err();
        assert!(error.to_string().contains("layout terminal"));
    }

    #[test]
    fn v26_layout_oracle_evaluates_closed_parquet_without_page_reads() {
        // Break caught: layout evaluation scores vectors or trusts stored oracle metrics.
        let (_temp, request) = evaluation_fixture();
        let terminal = read_layout_terminal(&request.layout_terminal).unwrap();
        let assignments = read_assignments(
            &request.page_assignments.path,
            i64::try_from(terminal.row_count).unwrap(),
        )
        .unwrap();
        let queries = read_evaluation_queries(&request.external_queries.path, 512).unwrap();
        assert!(
            read_evaluation_truth(
                &request.truth.path,
                512,
                &queries,
                &assignments,
                &"0".repeat(64),
                &request.external_queries.identity.digest,
            )
            .is_err()
        );
        let (truths, samples, result) = evaluate_v26_layout_oracle(&request).unwrap();
        assert_eq!(truths.len(), 512);
        assert_eq!(samples.len(), 512);
        assert_eq!(result.aggregate_recall_ppm, 1_000_000);
        assert_eq!(result.minimum_query_recall_ppm, 1_000_000);
        assert_eq!(result.disposition, V26Disposition::BoundedLayoutCandidate);
        assert_eq!(result.page_body_reads, 0);
        canonical_v26_layout_result_bytes(&result, &truths, &samples).unwrap();
    }

    #[test]
    fn v26_exact_global_local_authenticates_parquet_and_keeps_only_ranked_heads() {
        // Break caught: exact-global bypasses the closed layout authority or materializes the
        // full query-by-construction distance matrix instead of bounded ranked heads.
        let (temp, layout) = evaluation_fixture();
        let request = V26ExactGlobalRequest {
            construction_rows: identity(
                "construction-parquet",
                &temp.path().join("construction.parquet"),
            ),
            layout,
            ranked_row_limits: vec![10, 32, 128, 512, 2_048, 4_096],
        };

        let samples = evaluate_v26_exact_global(&request).unwrap();

        assert_eq!(samples.len(), 512 * 6);
        assert!(samples.iter().all(|sample| {
            sample.candidate_rows == 1_409
                && sample.first_ten_ranked_rows.len() == 10
                && sample.selected_pages.len() <= 8
        }));
    }

    #[test]
    fn v26_tree_router_local_authenticates_closed_trees_and_emits_canonical_result() {
        // Break caught: the router bypasses the closed tree identities, opens construction/page
        // data, or trusts emitted metrics instead of the pure router contract.
        let (temp, layout) = evaluation_fixture_with_rows(2_113);
        let terminal = read_layout_terminal(&layout.layout_terminal).unwrap();
        let tree = |role: &str, name: &str| V26LocalObjectPath {
            identity: terminal
                .outputs
                .iter()
                .find(|identity| identity.role == role)
                .unwrap()
                .clone(),
            path: temp.path().join("layout").join(name),
        };
        let request = V26TreeRouterRequest {
            primary_tree: tree("primary-tree-parquet", "primary-tree.parquet"),
            replica_tree: tree("replica-tree-parquet", "replica-tree.parquet"),
            layout,
            page_budget: 8,
        };

        let bytes = run_v26_tree_router(&request).unwrap();
        let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();

        assert_eq!(bytes.last(), Some(&b'\n'));
        assert_eq!(
            value["result"]["schema"],
            "borsuk-v26-tree-router-result-v1"
        );
        assert_eq!(value["result"]["page_body_reads"], 0);
        assert_eq!(value["result"]["claim_eligible"], false);
        assert_eq!(value["samples"].as_array().unwrap().len(), 512);
    }

    #[test]
    fn v26_tree_router_diagnostic_local_reuses_closed_authority_without_page_reads() {
        // Break caught: the diagnostic bypasses the authenticated layout/tree inputs or opens
        // construction/page bodies instead of reusing the closed router boundary.
        let (temp, layout) = evaluation_fixture_with_rows(2_113);
        let terminal = read_layout_terminal(&layout.layout_terminal).unwrap();
        let tree = |role: &str, name: &str| V26LocalObjectPath {
            identity: terminal
                .outputs
                .iter()
                .find(|identity| identity.role == role)
                .unwrap()
                .clone(),
            path: temp.path().join("layout").join(name),
        };
        let request = V26TreeRouterRequest {
            primary_tree: tree("primary-tree-parquet", "primary-tree.parquet"),
            replica_tree: tree("replica-tree-parquet", "replica-tree.parquet"),
            layout,
            page_budget: 8,
        };

        let bytes = run_v26_tree_router_diagnostic(&request).unwrap();
        let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();

        assert_eq!(bytes.last(), Some(&b'\n'));
        assert_eq!(
            value["schema"],
            "borsuk-v26-tree-router-diagnostic-result-v1"
        );
        assert_eq!(value["page_body_reads"], 0);
        assert_eq!(value["claim_eligible"], false);
        assert_eq!(value["widths"][0]["candidate_page_limit"], 8);
        assert_eq!(value["samples"].as_array().unwrap().len(), 512);
    }

    #[test]
    fn v26_centroid_router_local_authenticates_construction_and_emits_no_page_reads() {
        // Break caught: centroid construction bypasses the closed construction identity, gains
        // a page-body capability, or exposes a tunable serving frontier.
        let (temp, layout) = evaluation_fixture_with_rows(2_113);
        let terminal = read_layout_terminal(&layout.layout_terminal).unwrap();
        let tree = |role: &str, name: &str| V26LocalObjectPath {
            identity: terminal
                .outputs
                .iter()
                .find(|identity| identity.role == role)
                .unwrap()
                .clone(),
            path: temp.path().join("layout").join(name),
        };
        let request = V26CentroidRouterRequest {
            construction_rows: V26LocalObjectPath {
                identity: terminal.authority.construction_rows.clone(),
                path: temp.path().join("construction.parquet"),
            },
            router: V26TreeRouterRequest {
                primary_tree: tree("primary-tree-parquet", "primary-tree.parquet"),
                replica_tree: tree("replica-tree-parquet", "replica-tree.parquet"),
                layout,
                page_budget: 8,
            },
        };

        let bytes = run_v26_centroid_router(&request).unwrap();
        let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();

        assert_eq!(bytes.last(), Some(&b'\n'));
        assert_eq!(
            value["result"]["schema"],
            "borsuk-v26-centroid-router-result-v1"
        );
        assert_eq!(value["candidate_page_limit"], 8);
        assert_eq!(value["result"]["page_body_reads"], 0);
        assert_eq!(value["result"]["claim_eligible"], false);
        assert_eq!(value["samples"].as_array().unwrap().len(), 512);

        let mut forged = request.clone();
        forged.construction_rows.identity.digest = "f".repeat(64);
        assert!(run_v26_centroid_router(&forged).is_err());
    }

    #[test]
    fn v26_fast_global_centroid_frontier_uses_its_registered_ten_page_layout_gate() {
        // Break caught: the ten-page diagnostic reuses the eight-page exact-global loader and
        // closes before authenticating or scoring an otherwise valid ten-page layout.
        let (temp, mut layout) = evaluation_fixture_with_rows(6_000);
        let assignments = read_assignments(&layout.page_assignments.path, 6_000).unwrap();
        let mut used_pages = BTreeSet::new();
        let mut neighbors = Vec::new();
        for assignment in &assignments {
            if !used_pages.contains(&assignment.primary_page)
                && !used_pages.contains(&assignment.replica_page)
            {
                used_pages.insert(assignment.primary_page);
                used_pages.insert(assignment.replica_page);
                neighbors.push(u64::from(assignment.source_ordinal));
                if neighbors.len() == 9 {
                    break;
                }
            }
        }
        let extra = assignments
            .iter()
            .find(|assignment| !neighbors.contains(&u64::from(assignment.source_ordinal)))
            .unwrap();
        neighbors.push(u64::from(extra.source_ordinal));
        let neighbors: [u64; 10] = neighbors.try_into().unwrap();
        rewrite_evaluation_truth_neighbors(&mut layout, &neighbors);
        let (_, _, narrow) = evaluate_v26_layout_oracle_with_page_budget(&layout, 8).unwrap();
        let (_, _, broad) = evaluate_v26_layout_oracle_with_page_budget(&layout, 10).unwrap();
        assert_eq!(narrow.disposition, V26Disposition::LayoutRejected);
        assert_eq!(broad.disposition, V26Disposition::BoundedLayoutCandidate);

        let terminal = read_layout_terminal(&layout.layout_terminal).unwrap();
        let tree = |role: &str, name: &str| V26LocalObjectPath {
            identity: terminal
                .outputs
                .iter()
                .find(|identity| identity.role == role)
                .unwrap()
                .clone(),
            path: temp.path().join("layout").join(name),
        };
        let request = V26CentroidRouterRequest {
            construction_rows: V26LocalObjectPath {
                identity: terminal.authority.construction_rows.clone(),
                path: temp.path().join("construction.parquet"),
            },
            router: V26TreeRouterRequest {
                primary_tree: tree("primary-tree-parquet", "primary-tree.parquet"),
                replica_tree: tree("replica-tree-parquet", "replica-tree.parquet"),
                layout,
                page_budget: 10,
            },
        };

        let bytes = run_v26_global_centroid_frontier_diagnostic(&request).unwrap();
        let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(
            value["schema"],
            "borsuk-v26-global-centroid-frontier-result-v1"
        );
        assert_eq!(value["page_body_reads"], 0);
        assert_eq!(value["claim_eligible"], false);
    }

    #[test]
    fn v26_fast_global_page_mode_frontier_persists_parquet_without_page_reads() {
        // Break caught: the multimodal diagnostic stays trapped inside the tree frontier, emits
        // bulk JSON, or gains page-body access instead of persisting bounded Arrow evidence.
        let (temp, layout) = evaluation_fixture_with_rows(6_000);
        let terminal = read_layout_terminal(&layout.layout_terminal).unwrap();
        let tree = |role: &str, name: &str| V26LocalObjectPath {
            identity: terminal
                .outputs
                .iter()
                .find(|identity| identity.role == role)
                .unwrap()
                .clone(),
            path: temp.path().join("layout").join(name),
        };
        let evidence_path = temp.path().join("global-page-mode-frontier.parquet");
        let request = V26PageModeRouterRequest {
            construction_rows: V26LocalObjectPath {
                identity: terminal.authority.construction_rows.clone(),
                path: temp.path().join("construction.parquet"),
            },
            router: V26TreeRouterRequest {
                primary_tree: tree("primary-tree-parquet", "primary-tree.parquet"),
                replica_tree: tree("replica-tree-parquet", "replica-tree.parquet"),
                layout,
                page_budget: 10,
            },
            evidence_output_path: evidence_path.clone(),
            evidence_output_uri: "s3://frozen/v26/global-page-mode-frontier.parquet".to_owned(),
        };

        let bytes = run_v26_global_page_mode_frontier_diagnostic(&request).unwrap();
        let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(
            value["schema"],
            "borsuk-v26-global-page-mode-frontier-result-v1"
        );
        assert_eq!(value["page_body_reads"], 0);
        assert_eq!(value["claim_eligible"], false);
        assert_eq!(value["mode_results"].as_array().unwrap().len(), 6 * 3);
        assert_eq!(
            value["evidence"]["role"],
            "global-page-mode-evidence-parquet"
        );
        let reader = open_reader(&evidence_path).unwrap();
        assert_eq!(reader.metadata().file_metadata().num_rows(), 6 * 3 * 512);
    }

    #[test]
    fn v26_page_mode_router_local_writes_parquet_evidence_without_page_reads() {
        // Break caught: the diagnostic emits bulk samples as JSON, gains a page/storage client,
        // or allows the fixed mode/frontier ladder to be supplied by the caller.
        let (temp, layout) = evaluation_fixture_with_rows(2_113);
        let terminal = read_layout_terminal(&layout.layout_terminal).unwrap();
        let tree = |role: &str, name: &str| V26LocalObjectPath {
            identity: terminal
                .outputs
                .iter()
                .find(|identity| identity.role == role)
                .unwrap()
                .clone(),
            path: temp.path().join("layout").join(name),
        };
        let evidence_path = temp.path().join("page-mode-evidence.parquet");
        let assignments = read_assignments(
            &layout.page_assignments.path,
            i64::try_from(terminal.row_count).unwrap(),
        )
        .unwrap();
        let mut rows_per_page = BTreeMap::<u32, usize>::new();
        for assignment in &assignments {
            *rows_per_page.entry(assignment.primary_page).or_default() += 1;
            *rows_per_page.entry(assignment.replica_page).or_default() += 1;
        }
        assert!(rows_per_page.values().any(|count| *count < 16));
        let request = V26PageModeRouterRequest {
            construction_rows: V26LocalObjectPath {
                identity: terminal.authority.construction_rows.clone(),
                path: temp.path().join("construction.parquet"),
            },
            router: V26TreeRouterRequest {
                primary_tree: tree("primary-tree-parquet", "primary-tree.parquet"),
                replica_tree: tree("replica-tree-parquet", "replica-tree.parquet"),
                layout,
                page_budget: 8,
            },
            evidence_output_path: evidence_path.clone(),
            evidence_output_uri: "s3://frozen/v26/page-mode-evidence.parquet".to_owned(),
        };

        let bytes = run_v26_page_mode_router(&request).unwrap();
        let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(bytes.last(), Some(&b'\n'));
        assert_eq!(value["schema"], "borsuk-v26-page-mode-router-result-v1");
        assert_eq!(value["candidate_page_limit"], 8);
        assert_eq!(value["mode_results"].as_array().unwrap().len(), 6);
        assert_eq!(value["evidence"]["role"], "page-mode-evidence-parquet");
        assert_eq!(value["evidence"]["uri"], request.evidence_output_uri);
        assert_eq!(value["page_body_reads"], 0);
        assert_eq!(value["claim_eligible"], false);
        assert!(evidence_path.is_file());
        let reader = open_reader(&evidence_path).unwrap();
        assert_eq!(reader.metadata().file_metadata().num_rows(), 6 * 512);
    }

    #[test]
    fn v26_candidate_cover_local_persists_only_bounded_parquet_evidence() {
        // Break caught: exact candidate scoring retains a full ranking, emits bulk JSON, or
        // gains page/network access instead of using the authenticated local construction.
        let (temp, layout) = evaluation_fixture_with_rows(2_113);
        let terminal = read_layout_terminal(&layout.layout_terminal).unwrap();
        let tree = |role: &str, name: &str| V26LocalObjectPath {
            identity: terminal
                .outputs
                .iter()
                .find(|identity| identity.role == role)
                .unwrap()
                .clone(),
            path: temp.path().join("layout").join(name),
        };
        let evidence_path = temp.path().join("candidate-cover-evidence.parquet");
        let request = V26CandidateCoverRequest {
            construction_rows: V26LocalObjectPath {
                identity: terminal.authority.construction_rows.clone(),
                path: temp.path().join("construction.parquet"),
            },
            router: V26TreeRouterRequest {
                primary_tree: tree("primary-tree-parquet", "primary-tree.parquet"),
                replica_tree: tree("replica-tree-parquet", "replica-tree.parquet"),
                layout,
                page_budget: 8,
            },
            evidence_output_path: evidence_path.clone(),
            evidence_output_uri: "s3://frozen/v26/candidate-cover-evidence.parquet".to_owned(),
        };

        let bytes = run_v26_candidate_row_cover(&request).unwrap();
        let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(value["schema"], "borsuk-v26-candidate-row-cover-output-v1");
        assert_eq!(value["candidate_page_limit"], 8);
        assert_eq!(value["ranked_row_limit"], 10);
        assert_eq!(
            value["evidence"]["role"],
            "candidate-cover-evidence-parquet"
        );
        assert_eq!(value["page_body_reads"], 0);
        assert_eq!(value["claim_eligible"], false);
        let reader = open_reader(&evidence_path).unwrap();
        assert_eq!(reader.metadata().file_metadata().num_rows(), 512);
    }

    #[test]
    fn v26_pq8_cover_local_authenticates_inputs_and_persists_bounded_evidence() {
        // Break caught: PQ8 fitting discovers inputs, persists row IDs/bulk JSON, or omits the
        // complete 100M resident-memory projection from its claim-ineligible result.
        let (temp, layout) = evaluation_fixture_with_rows(2_113);
        let terminal = read_layout_terminal(&layout.layout_terminal).unwrap();
        let tree = |role: &str, name: &str| V26LocalObjectPath {
            identity: terminal
                .outputs
                .iter()
                .find(|identity| identity.role == role)
                .unwrap()
                .clone(),
            path: temp.path().join("layout").join(name),
        };
        let evidence_path = temp.path().join("pq8-cover-evidence.parquet");
        let request = V26Pq8CoverRequest {
            construction_rows: V26LocalObjectPath {
                identity: terminal.authority.construction_rows.clone(),
                path: temp.path().join("construction.parquet"),
            },
            router: V26TreeRouterRequest {
                primary_tree: tree("primary-tree-parquet", "primary-tree.parquet"),
                replica_tree: tree("replica-tree-parquet", "replica-tree.parquet"),
                layout,
                page_budget: 8,
            },
            evidence_output_path: evidence_path.clone(),
            evidence_output_uri: "s3://frozen/v26/pq8-cover-evidence.parquet".to_owned(),
        };

        let bytes = run_v26_pq8_candidate_cover(&request).unwrap();
        let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(value["schema"], "borsuk-v26-pq8-candidate-cover-output-v1");
        assert_eq!(value["candidate_page_limit"], 8);
        assert_eq!(value["ranked_row_limit"], 10);
        assert_eq!(value["projected_resident_bytes_100m"], 2_937_537_416_u64);
        assert_eq!(value["evidence"]["role"], "pq8-cover-evidence-parquet");
        assert_eq!(value["page_body_reads"], 0);
        assert_eq!(value["claim_eligible"], false);
        let reader = open_reader(&evidence_path).unwrap();
        assert_eq!(reader.metadata().file_metadata().num_rows(), 512);
    }

    #[test]
    fn v26_pq_width_ladder_local_persists_one_bounded_parquet_evidence() {
        // Break caught: the fidelity diagnostic tunes widths independently, emits bulk JSON,
        // omits the serving projection, or gains page/network access.
        let (temp, layout) = evaluation_fixture_with_rows(2_113);
        let terminal = read_layout_terminal(&layout.layout_terminal).unwrap();
        let tree = |role: &str, name: &str| V26LocalObjectPath {
            identity: terminal
                .outputs
                .iter()
                .find(|identity| identity.role == role)
                .unwrap()
                .clone(),
            path: temp.path().join("layout").join(name),
        };
        let evidence_path = temp.path().join("pq-width-ladder-evidence.parquet");
        let request = V26PqWidthLadderRequest {
            construction_rows: V26LocalObjectPath {
                identity: terminal.authority.construction_rows.clone(),
                path: temp.path().join("construction.parquet"),
            },
            router: V26TreeRouterRequest {
                primary_tree: tree("primary-tree-parquet", "primary-tree.parquet"),
                replica_tree: tree("replica-tree-parquet", "replica-tree.parquet"),
                layout,
                page_budget: 8,
            },
            evidence_output_path: evidence_path.clone(),
            evidence_output_uri: "s3://frozen/v26/pq-width-ladder-evidence.parquet".to_owned(),
        };

        let bytes = run_v26_pq_width_ladder(&request).unwrap();
        let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(value["schema"], "borsuk-v26-pq-width-ladder-output-v1");
        assert_eq!(value["candidate_page_limit"], 8);
        assert_eq!(value["ranked_row_limit"], 10);
        assert_eq!(value["page_body_reads"], 0);
        assert_eq!(value["claim_eligible"], false);
        assert_eq!(
            value["arms"]
                .as_array()
                .unwrap()
                .iter()
                .map(|arm| arm["code_width"].as_u64().unwrap())
                .collect::<Vec<_>>(),
            [8, 16, 24, 32]
        );
        assert_eq!(
            value["arms"]
                .as_array()
                .unwrap()
                .iter()
                .map(|arm| arm["projected_resident_bytes_100m"].as_u64().unwrap())
                .collect::<Vec<_>>(),
            [
                2_937_537_416_u64,
                4_537_537_416,
                6_137_537_416,
                7_737_537_416,
            ]
        );
        assert_eq!(
            value["evidence"]["role"],
            "pq-width-ladder-evidence-parquet"
        );
        let reader = open_reader(&evidence_path).unwrap();
        assert_eq!(reader.metadata().file_metadata().num_rows(), 4 * 512);
        assert_eq!(reader.schema().field(0).name(), "code_width");
    }

    #[test]
    fn v26_pq16_exact_rerank_local_persists_fixed_depth_evidence() {
        // Break caught: the hybrid runner discovers cold vectors, tunes rank depth, emits bulk
        // JSON, or fails to bind its single resident projection to every arm.
        let (temp, layout) = evaluation_fixture_with_rows(3_521);
        let terminal = read_layout_terminal(&layout.layout_terminal).unwrap();
        let tree = |role: &str, name: &str| V26LocalObjectPath {
            identity: terminal
                .outputs
                .iter()
                .find(|identity| identity.role == role)
                .unwrap()
                .clone(),
            path: temp.path().join("layout").join(name),
        };
        let evidence_path = temp.path().join("pq16-rerank-evidence.parquet");
        let request = V26Pq16RerankRequest {
            construction_rows: V26LocalObjectPath {
                identity: terminal.authority.construction_rows.clone(),
                path: temp.path().join("construction.parquet"),
            },
            router: V26TreeRouterRequest {
                primary_tree: tree("primary-tree-parquet", "primary-tree.parquet"),
                replica_tree: tree("replica-tree-parquet", "replica-tree.parquet"),
                layout,
                page_budget: 10,
            },
            evidence_output_path: evidence_path.clone(),
            evidence_output_uri: "s3://frozen/v26/pq16-rerank-evidence.parquet".to_owned(),
        };

        let (_, layout_samples, layout_result) =
            evaluate_v26_layout_oracle_with_page_budget(&request.router.layout, 10).unwrap();
        assert_eq!(
            layout_result.disposition,
            V26Disposition::BoundedLayoutCandidate
        );
        assert!(
            layout_samples
                .iter()
                .all(|sample| sample.selected_pages.len() <= 10)
        );

        let bytes = run_v26_pq16_exact_rerank(&request).unwrap();
        let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(value["schema"], "borsuk-v26-pq16-exact-rerank-output-v1");
        assert_eq!(value["candidate_page_limit"], 12);
        assert_eq!(value["page_body_reads"], 0);
        assert_eq!(value["claim_eligible"], false);
        assert_eq!(
            value["arms"]
                .as_array()
                .unwrap()
                .iter()
                .map(|arm| arm["ranked_row_limit"].as_u64().unwrap())
                .collect::<Vec<_>>(),
            [10, 32, 128, 512, 2_048]
        );
        assert!(
            value["arms"]
                .as_array()
                .unwrap()
                .iter()
                .all(|arm| arm["projected_resident_bytes_100m"] == 2_937_537_416_u64)
        );
        assert_eq!(
            value["evidence"]["role"],
            "pq16-exact-rerank-evidence-parquet"
        );
        let reader = open_reader(&evidence_path).unwrap();
        assert_eq!(reader.metadata().file_metadata().num_rows(), 5 * 512);
        assert_eq!(reader.schema().field(0).name(), "ranked_row_limit");
    }

    #[test]
    fn v26_arrow_cold_vectors_read_only_requested_fixed_batches() {
        // Break caught: exact vectors use a private binary format, map/load the cold corpus into
        // process RSS, lose assignment authority, or decode complete Arrow batches per query.
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("cold-vectors.arrow");
        let rows = (0_u64..1_024)
            .map(|source_ordinal| {
                let mut vector = [0.0_f32; 96];
                vector[usize::try_from(source_ordinal % 96).unwrap()] = 1.0;
                crate::V26ConstructionRow {
                    source_ordinal,
                    vector,
                }
            })
            .collect::<Vec<_>>();
        let assignments = (0_u64..1_024)
            .map(|source_ordinal| crate::V26RowPages {
                source_ordinal,
                primary_page: u32::try_from(source_ordinal % 8).unwrap(),
                replica_page: 8 + u32::try_from(source_ordinal % 8).unwrap(),
            })
            .collect::<Vec<_>>();
        let manifest = write_v26_cold_vectors_arrow(
            &path,
            &rows,
            &assignments,
            super::V26_COLD_VECTOR_BATCH_ROWS,
        )
        .unwrap();
        assert_eq!(manifest.row_count, 1_024);
        assert_eq!(manifest.batch_rows, super::V26_COLD_VECTOR_BATCH_ROWS);
        let bytes = fs::read(&path).unwrap();
        assert_eq!(&bytes[..6], b"ARROW1");
        assert_eq!(&bytes[bytes.len() - 6..], b"ARROW1");

        let reader = V26ArrowColdVectors::open(&path, &manifest).unwrap();
        assert!(!reader.is_memory_mapped());
        let selected = reader.read_rows(&[0, 1, 63, 64, 511, 512, 1_023]).unwrap();
        assert_eq!(selected.vectors.len(), 7);
        assert_eq!(selected.batches_read, 1);
        assert_eq!(selected.read_workers, 4);
        assert_eq!(reader.decoded_batch_count(), 0);
        assert_eq!(selected.vectors[0], rows[0].vector);
        assert_eq!(selected.vectors[6], rows[1_023].vector);
        assert_eq!(selected.assignments[0], assignments[0]);
        assert_eq!(selected.assignments[6], assignments[1_023]);
        assert!(reader.read_rows(&[64, 63]).is_err());
        assert!(reader.read_rows(&[1_024]).is_err());
    }

    #[test]
    fn v26_fast_pq16_arrow_serving_matches_reference_with_bounded_cold_reads() {
        // Break caught: serving loads the cold corpus, changes top-512 exact-rerank semantics,
        // reads page bodies, or loses the Arrow batch-read bound.
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("cold-vectors.arrow");
        let rows = (0_u64..2_113)
            .map(|source_ordinal| {
                let angle = source_ordinal as f32 / 2_048.0;
                let mut vector = [0.0_f32; 96];
                vector[0] = angle.cos();
                vector[1] = angle.sin();
                crate::V26ConstructionRow {
                    source_ordinal,
                    vector,
                }
            })
            .collect::<Vec<_>>();
        let assignments = (0_u64..2_113)
            .map(|source_ordinal| {
                let (primary_page, replica_page) = if source_ordinal < 256 {
                    (0, 1)
                } else {
                    (
                        2 + u32::try_from(source_ordinal % 8).unwrap(),
                        10 + u32::try_from(source_ordinal % 8).unwrap(),
                    )
                };
                crate::V26RowPages {
                    source_ordinal,
                    primary_page,
                    replica_page,
                }
            })
            .collect::<Vec<_>>();
        let index = crate::build_v26_pq16_packed_index(&rows, &assignments).unwrap();
        let candidate_pages = (0_u32..16).collect::<Vec<_>>();
        let query = rows[42].vector;
        let reference = crate::select_v26_pq16_packed_pages(
            &index,
            &candidate_pages,
            &query,
            &rows,
            &assignments,
        )
        .unwrap();

        let manifest = write_v26_cold_vectors_arrow(
            &path,
            &rows,
            &assignments,
            super::V26_COLD_VECTOR_BATCH_ROWS,
        )
        .unwrap();
        let reader = V26ArrowColdVectors::open(&path, &manifest).unwrap();
        let in_memory_multi = crate::build_v26_simhash_pq16_multi_index(&index, &rows).unwrap();
        let arrow_multi =
            super::build_v26_simhash_pq16_multi_index_from_arrow(&index, &reader).unwrap();
        assert_eq!(arrow_multi, in_memory_multi);
        let multi_dir = temp.path().join("simhash-pq16");
        fs::create_dir(&multi_dir).unwrap();
        let multi_manifest =
            super::write_v26_simhash_pq16_index_arrow(&multi_dir, &arrow_multi).unwrap();
        assert_eq!(multi_manifest.row_count, rows.len() as u64);
        assert_eq!(multi_manifest.bucket_count, 65_536);
        assert_eq!(multi_manifest.projected_resident_bytes_100m, 2_537_493_520);
        let restored =
            super::read_v26_simhash_pq16_index_arrow(&multi_dir, &multi_manifest).unwrap();
        assert_eq!(restored, arrow_multi);
        let result =
            select_v26_pq16_pages_from_arrow(&index, &candidate_pages, &query, &reader).unwrap();

        assert_eq!(result.selected_pages, reference.selected_pages);
        assert_eq!(result.selected_pages.len(), 10);
        assert_eq!(result.exact_rows_read, 512);
        assert!(result.cold_batches_read > 0 && result.cold_batches_read <= 34);
        assert_eq!(result.cold_read_workers, 4);
        assert_eq!(result.page_body_reads, 0);

        let global_reference =
            crate::select_v26_pq16_global_packed_pages(&index, &query, &rows, &assignments, 2_048)
                .unwrap();
        let global =
            select_v26_pq16_global_pages_from_arrow(&index, &query, &reader, 2_048).unwrap();
        assert_eq!(global.selected_pages, global_reference.selected_pages);
        assert_eq!(global.exact_rows_read, 2_048);
        assert!(global.cold_batches_read > 0 && global.cold_batches_read <= 34);
        assert_eq!(global.cold_read_workers, 4);
        assert_eq!(global.page_body_reads, 0);
        let timed =
            super::select_v26_pq16_global_pages_from_arrow_timed(&index, &query, &reader, 2_048)
                .unwrap();
        assert_eq!(timed.selection, global);
        assert!(timed.global_adc_elapsed_ns > 0);
        assert!(timed.exact_rerank_elapsed_ns > 0);

        let simhash = super::select_v26_simhash_pq16_pages_from_arrow(
            &arrow_multi,
            &query,
            &reader,
            65_536,
            2_048,
        )
        .unwrap();
        assert_eq!(simhash, global);

        // Break caught: the two distance-aligned ordinal planes are serialized with an
        // ambiguous schema, lose stable source order, or exact-rerank a different row set.
        let dual = crate::build_v26_dual_pq_key_index(&index).unwrap();
        let dual_dir = temp.path().join("dual-pq-key");
        fs::create_dir(&dual_dir).unwrap();
        let dual_manifest = super::write_v26_dual_pq_key_index_arrow(&dual_dir, &dual).unwrap();
        assert_eq!(dual_manifest.row_count, rows.len() as u64);
        assert_eq!(dual_manifest.plane_count, 2);
        assert_eq!(dual_manifest.bucket_count, 65_536);
        assert_eq!(dual_manifest.projected_resident_bytes_100m, 2_938_017_816);
        let restored =
            super::read_v26_dual_pq_key_index_arrow(&dual_dir, &dual_manifest, &index).unwrap();
        assert_eq!(restored, dual);
        let dual_selection = super::select_v26_dual_pq_key_pages_from_arrow(
            &restored, &query, &reader, 65_536, 2_048,
        )
        .unwrap();
        assert_eq!(dual_selection, global);

        if !cfg!(debug_assertions) {
            let mut latency_ns = Vec::with_capacity(128);
            for sample in 0..144 {
                let query = rows[(42 + sample * 13) % rows.len()].vector;
                let started = std::time::Instant::now();
                let selection =
                    select_v26_pq16_pages_from_arrow(&index, &candidate_pages, &query, &reader)
                        .unwrap();
                let elapsed = started.elapsed().as_nanos();
                assert_eq!(selection.exact_rows_read, 512);
                assert!(selection.cold_batches_read > 0 && selection.cold_batches_read <= 34);
                assert_eq!(selection.cold_read_workers, 4);
                assert_eq!(selection.page_body_reads, 0);
                if sample >= 16 {
                    latency_ns.push(elapsed);
                }
            }
            latency_ns.sort_unstable();
            let p99_ns = latency_ns[(latency_ns.len() * 99).div_ceil(100) - 1];
            eprintln!("v26-pq16-arrow-reduced-shape-p99-ns={p99_ns}");
            assert!(p99_ns < 15_000_000);
        }
    }

    #[test]
    fn v26_simhash_preflight_executes_32_truth_bound_queries_without_page_reads() {
        // Break caught: the preflight reports stored quality without executing every fixed arm
        // against independently supplied truth, or opens page bodies while selecting pages.
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("simhash-preflight-cold.arrow");
        let rows = (0_u64..2_113)
            .map(|source_ordinal| {
                let mut vector = [0.0_f32; 96];
                vector[0] = 1.0;
                crate::V26ConstructionRow {
                    source_ordinal,
                    vector,
                }
            })
            .collect::<Vec<_>>();
        let assignments = (0_u64..2_113)
            .map(|source_ordinal| crate::V26RowPages {
                source_ordinal,
                primary_page: u32::try_from(source_ordinal % 5).unwrap(),
                replica_page: 5 + u32::try_from(source_ordinal % 5).unwrap(),
            })
            .collect::<Vec<_>>();
        let packed = crate::build_v26_pq16_packed_index(&rows, &assignments).unwrap();
        let manifest = write_v26_cold_vectors_arrow(
            &path,
            &rows,
            &assignments,
            super::V26_COLD_VECTOR_BATCH_ROWS,
        )
        .unwrap();
        let cold = V26ArrowColdVectors::open(&path, &manifest).unwrap();
        let index = super::build_v26_simhash_pq16_multi_index_from_arrow(&packed, &cold).unwrap();
        let queries = rows[..32]
            .iter()
            .enumerate()
            .map(|(query_ordinal, row)| crate::V26ExternalQuery {
                query_ordinal: u32::try_from(query_ordinal).unwrap(),
                vector: row.vector,
            })
            .collect::<Vec<_>>();
        let truths = queries
            .iter()
            .map(|query| {
                let neighbors = (0_u64..10)
                    .map(|offset| u64::from(query.query_ordinal) + offset)
                    .collect::<Vec<_>>();
                let ground_truth_page_assignments = neighbors
                    .iter()
                    .map(|neighbor| {
                        let assignment = assignments[usize::try_from(*neighbor).unwrap()];
                        let mut pages = vec![assignment.primary_page, assignment.replica_page];
                        pages.sort_unstable();
                        pages
                    })
                    .collect();
                crate::V26QueryTruth {
                    query_ordinal: query.query_ordinal,
                    neighbor_source_ordinals: neighbors,
                    ground_truth_page_assignments,
                }
            })
            .collect::<Vec<_>>();

        let object = |role: &str, fill: char| V26ObjectIdentity {
            role: role.to_owned(),
            uri: format!("s3://v26/{role}"),
            digest_algorithm: "sha256".to_owned(),
            digest: fill.to_string().repeat(64),
            encoded_bytes: 1_024,
            generation: "v26-local-test".to_owned(),
        };
        let authority = super::V26SimHashPreflightAuthority {
            serving_manifest: object("pq16-serving-manifest", 'a'),
            external_queries: object("external-queries-parquet", 'b'),
            truth: object("truth-parquet", 'c'),
            evidence: object("simhash-preflight-evidence-parquet", 'd'),
        };
        let (samples, result) =
            super::evaluate_v26_simhash_preflight(&index, &cold, &queries, &truths, authority)
                .unwrap();

        assert_eq!(samples.len(), 96);
        assert_eq!(result.arms.len(), 3);
        assert_eq!(result.page_body_reads, 0);
        assert!(samples.iter().all(|sample| {
            sample.selected_pages.len() == 10
                && sample.hits <= sample.oracle_hits
                && sample.elapsed_ns > 0
                && sample.rows_scanned >= 2_048
        }));
        assert!(super::canonical_v26_simhash_preflight_result_bytes(&result, &samples).is_ok());

        // Break caught: the distance-aligned preflight skips a fixed arm/query, reports stored
        // metrics, loses its derived Arrow identities, or gains a page-body surface.
        let dual = crate::build_v26_dual_pq_key_index(&packed).unwrap();
        let dual_authority = super::V26DualPqKeyPreflightAuthority {
            serving_manifest: object("pq16-serving-manifest", 'a'),
            external_queries: object("external-queries-parquet", 'b'),
            truth: object("truth-parquet", 'c'),
            offsets: object("dual-pq-key-offsets-arrow", 'd'),
            ordinals: object("dual-pq-key-ordinals-arrow", 'e'),
            evidence: object("dual-pq-key-preflight-evidence-parquet", 'f'),
        };
        let (dual_samples, dual_result) = super::evaluate_v26_dual_pq_key_preflight(
            &dual,
            &cold,
            &queries,
            &truths,
            dual_authority,
        )
        .unwrap();
        assert_eq!(dual_samples.len(), 96);
        assert_eq!(dual_result.arms.len(), 3);
        assert_eq!(
            dual_result
                .arms
                .iter()
                .map(|arm| arm.key_limit_per_plane)
                .collect::<Vec<_>>(),
            [1_536, 4_096, 8_192]
        );
        assert_eq!(dual_result.ranked_row_limit, 512);
        assert!(
            dual_samples
                .iter()
                .all(|sample| sample.ranked_row_limit == 512)
        );
        assert_eq!(dual_result.page_body_reads, 0);
        assert!(dual_samples.iter().all(|sample| {
            sample.selected_pages.len() == 10
                && sample.hits <= sample.oracle_hits
                && sample.elapsed_ns > 0
                && sample.unique_rows_scanned >= 2_048
        }));
        assert!(
            super::canonical_v26_dual_pq_key_preflight_result_bytes(&dual_result, &dual_samples)
                .is_ok()
        );
    }

    fn pq4_authority(role: &str, marker: char) -> V26ObjectIdentity {
        let name = match role {
            "pq4-fast-codebook-arrow" => "output/pq4-fast-codebook.arrow",
            "pq4-fast-codes-arrow" => "output/pq4-fast-codes.arrow",
            _ => role,
        };
        V26ObjectIdentity {
            role: role.to_owned(),
            uri: format!("s3://v26-pq4-test/{name}"),
            digest_algorithm: "sha256".to_owned(),
            digest: marker.to_string().repeat(64),
            encoded_bytes: 1,
            generation: "v26-pq4-test-a0001".to_owned(),
        }
    }

    #[test]
    fn v26_pq4_arrow_roundtrips_exact_transposed_blocks_and_authority() {
        // Break caught: PQ4 persistence uses a private blob, changes nibble/source order, or
        // accepts coherent Arrow bytes under a different registered identity.
        let temp = TempDir::new().unwrap();
        let rows = (0_usize..1_024)
            .map(|source_ordinal| {
                let mut vector = [0.0_f32; 96];
                vector[source_ordinal % 96] = 1.0;
                vector
            })
            .collect::<Vec<_>>();
        let expected = crate::build_v26_pq4_fast_index(&rows).unwrap();
        let construction = pq4_authority("construction-parquet", 'a');
        let assignments = pq4_authority("page-assignments-parquet", 'b');
        let terminal = pq4_authority("layout-terminal", 'c');

        let manifest = write_v26_pq4_fast_index_arrow(
            temp.path(),
            &expected,
            &construction,
            &assignments,
            &terminal,
            "s3://v26-pq4-test/output/",
        )
        .unwrap();
        assert_eq!(manifest.schema, "borsuk-v26-pq4-fast-manifest-v1");
        assert_eq!(manifest.row_count, 1_024);
        assert_eq!(manifest.block_count, 32);
        assert_eq!(manifest.padding_rows, 0);
        assert_eq!(manifest.codebook.role, "pq4-fast-codebook-arrow");
        assert_eq!(manifest.codes.role, "pq4-fast-codes-arrow");
        for name in ["pq4-fast-codebook.arrow", "pq4-fast-codes.arrow"] {
            let bytes = fs::read(temp.path().join(name)).unwrap();
            assert_eq!(&bytes[..6], b"ARROW1");
            assert_eq!(&bytes[bytes.len() - 6..], b"ARROW1");
        }
        let reopened = read_v26_pq4_fast_index_arrow(temp.path(), &manifest).unwrap();
        assert_eq!(reopened, expected);

        let mut drifted = manifest.clone();
        drifted.codes.digest.replace_range(0..1, "e");
        assert!(read_v26_pq4_fast_index_arrow(temp.path(), &drifted).is_err());
    }

    #[test]
    fn v26_pq4_arrow_manifest_rejects_every_layout_and_identity_drift() {
        // Break caught: the manifest ceases to be the complete cross-language authority for
        // dimensions, packing, padding, memory, source inputs, or its two Arrow outputs.
        let baseline = V26Pq4FastManifest {
            schema: "borsuk-v26-pq4-fast-manifest-v1".to_owned(),
            construction_rows: pq4_authority("construction-parquet", 'a'),
            page_assignments: pq4_authority("page-assignments-parquet", 'b'),
            layout_terminal: pq4_authority("layout-terminal", 'c'),
            codebook: pq4_authority("pq4-fast-codebook-arrow", 'd'),
            codes: pq4_authority("pq4-fast-codes-arrow", 'e'),
            row_count: 1_025,
            block_count: 33,
            padding_rows: 31,
            dimension: 96,
            subquantizer_count: 32,
            subspace_dimensions: 3,
            centroid_count: 16,
            block_rows: 32,
            code_bytes_per_row: 16,
            byte_order: "subquantizer-major".to_owned(),
            nibble_order: "even-low-odd-high".to_owned(),
            source_order: "ascending-source-ordinal".to_owned(),
            projected_resident_bytes_100m: 2_336_975_744,
        };
        let bytes = canonical_v26_pq4_fast_manifest_bytes(&baseline).unwrap();
        assert_eq!(bytes.last(), Some(&b'\n'));
        assert!(!bytes.windows(2).any(|pair| pair == b" \n"));

        let mutations: Vec<Box<dyn Fn(&mut V26Pq4FastManifest)>> = vec![
            Box::new(|value| value.dimension = 95),
            Box::new(|value| value.block_count = 32),
            Box::new(|value| value.padding_rows = 30),
            Box::new(|value| value.nibble_order = "odd-low-even-high".to_owned()),
            Box::new(|value| value.projected_resident_bytes_100m += 1),
            Box::new(|value| value.construction_rows.role = "query-parquet".to_owned()),
            Box::new(|value| value.codes.uri = value.codebook.uri.clone()),
            Box::new(|value| value.layout_terminal.digest_algorithm = "blake3".to_owned()),
        ];
        for mutate in mutations {
            let mut drifted = baseline.clone();
            mutate(&mut drifted);
            assert!(canonical_v26_pq4_fast_manifest_bytes(&drifted).is_err());
        }
    }

    #[test]
    fn v26_pq4_arrow_builder_binds_only_query_independent_inputs() {
        // Break caught: PQ4 construction admits query/truth/page data, loses the layout receipt
        // binding, rereads source order inconsistently, or emits an unauthenticated manifest.
        let (temp, evaluation) = evaluation_fixture_with_rows(1_024);
        let output_dir = temp.path().join("pq4-fast");
        let request = V26Pq4FastBuildRequest {
            construction_rows: identity(
                "construction-parquet",
                &temp.path().join("construction.parquet"),
            ),
            page_assignments: evaluation.page_assignments.clone(),
            layout_terminal: evaluation.layout_terminal.clone(),
            expected_rows: 1_024,
            output_dir: output_dir.clone(),
            output_uri_prefix: "s3://v26-output/pq4-fast-a0001/".to_owned(),
        };

        let output = run_v26_pq4_fast_build(&request).unwrap();
        assert_eq!(output.row_count, 1_024);
        assert_eq!(output.construction_rows, request.construction_rows.identity);
        assert_eq!(output.page_assignments, request.page_assignments.identity);
        assert_eq!(output.layout_terminal, request.layout_terminal.identity);
        assert_eq!(
            fs::read(output_dir.join("pq4-fast-manifest.json")).unwrap(),
            canonical_v26_pq4_fast_manifest_bytes(&output).unwrap()
        );
        assert_eq!(
            read_v26_pq4_fast_index_arrow(&output_dir, &output)
                .unwrap()
                .row_count,
            1_024
        );

        let mut drifted = request.clone();
        drifted.page_assignments.identity.digest = "f".repeat(64);
        drifted.output_dir = temp.path().join("rejected-pq4-fast");
        assert!(run_v26_pq4_fast_build(&drifted).is_err());
        assert!(!drifted.output_dir.exists());
    }

    #[test]
    fn v26_fast_smoke_pq4_quality_result_recomputes_the_fixed_four_arm_frontier() {
        // Break caught: the result trusts reported aggregates, tunes the candidate ladder, loses
        // quantization telemetry, or becomes claim-eligible on the burned development queries.
        let depths = [512_u32, 1_024, 2_048, 4_096];
        let samples = depths
            .into_iter()
            .flat_map(|ranked_row_limit| {
                (0_u32..32).map(move |query_ordinal| {
                    let hits = if ranked_row_limit == 512 { 9 } else { 10 };
                    V26Pq4QualitySample {
                        ranked_row_limit,
                        query_ordinal,
                        selected_pages: (0_u32..10).collect(),
                        hits,
                        oracle_hits: 10,
                        recall_ppm: u64::from(hits) * 100_000,
                        oracle_attainment_ppm: u64::from(hits) * 100_000,
                        scan_elapsed_ns: 100,
                        exact_rerank_elapsed_ns: 20 + u64::from(ranked_row_limit),
                        quantization_scale_bits: 1.0_f32.to_bits(),
                        saturation_count: 32,
                        maximum_distance_error_bits: 0.01_f32.to_bits(),
                        page_body_reads: 0,
                    }
                })
            })
            .collect::<Vec<_>>();
        let manifest = pq4_authority("pq4-fast-manifest", 'a');
        let queries = pq4_authority("external-queries-parquet", 'b');
        let truth = pq4_authority("truth-parquet", 'c');
        let evidence = pq4_authority("pq4-fast-quality-evidence-parquet", 'd');
        let result = summarize_v26_pq4_quality(
            manifest.clone(),
            queries.clone(),
            truth.clone(),
            evidence.clone(),
            &samples,
        )
        .unwrap();
        assert_eq!(result.schema, "borsuk-v26-pq4-fast-quality-result-v1");
        assert_eq!(result.arms.len(), 4);
        assert_eq!(result.smallest_passing_ranked_row_limit, Some(1_024));
        assert_eq!(result.backend, "aarch64-neon-table");
        assert_eq!(result.page_body_reads, 0);
        assert!(!result.claim_eligible);
        let bytes = canonical_v26_pq4_quality_result_bytes(&result, &samples).unwrap();
        assert_eq!(bytes.last(), Some(&b'\n'));

        let mut drifted_result = result.clone();
        drifted_result.arms[1].aggregate_recall_ppm -= 1;
        assert!(canonical_v26_pq4_quality_result_bytes(&drifted_result, &samples).is_err());
        let mut drifted_samples = samples.clone();
        drifted_samples[32].hits = 9;
        assert!(canonical_v26_pq4_quality_result_bytes(&result, &drifted_samples).is_err());
    }

    #[test]
    fn v26_pq4_quality_runner_authenticates_full_inputs_and_writes_128_samples() {
        // Break caught: the frontier truncates before authenticating all queries/truth, rebuilds
        // PQ, opens page bodies, or writes a result not bound to its typed Parquet evidence.
        let row_count = 8_193_u64;
        let (temp, evaluation) = evaluation_fixture_with_rows(row_count);
        let rows =
            read_construction_rows(&temp.path().join("construction.parquet"), row_count).unwrap();
        let assignments = read_assignments(
            &evaluation.page_assignments.path,
            i64::try_from(row_count).unwrap(),
        )
        .unwrap();
        let cold_path = temp.path().join("pq4-fast-cold-vectors.arrow");
        let cold_manifest = write_v26_cold_vectors_arrow(
            &cold_path,
            &rows,
            &assignments,
            super::V26_COLD_VECTOR_BATCH_ROWS,
        )
        .unwrap();
        let pq4_dir = temp.path().join("pq4-fast");
        let build_request = V26Pq4FastBuildRequest {
            construction_rows: identity(
                "construction-parquet",
                &temp.path().join("construction.parquet"),
            ),
            page_assignments: evaluation.page_assignments.clone(),
            layout_terminal: evaluation.layout_terminal.clone(),
            expected_rows: row_count,
            output_dir: pq4_dir.clone(),
            output_uri_prefix: "s3://v26-output/pq4-quality/index/".to_owned(),
        };
        run_v26_pq4_fast_build(&build_request).unwrap();
        let evidence_path = temp.path().join("evidence.parquet");
        let request = V26Pq4QualityRequest {
            pq4_manifest: identity("pq4-fast-manifest", &pq4_dir.join("pq4-fast-manifest.json")),
            pq4_dir,
            cold_vectors: identity("cold-vectors-arrow", &cold_path),
            cold_vectors_manifest: cold_manifest,
            layout_terminal: evaluation.layout_terminal,
            external_queries: evaluation.external_queries,
            truth: evaluation.truth,
            evidence_output_path: evidence_path.clone(),
            evidence_output_uri: "s3://v26-output/pq4-quality/evidence.parquet".to_owned(),
        };

        let bytes = run_v26_pq4_quality_frontier(&request).unwrap();
        let result: super::V26Pq4QualityResult = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(result.query_count, 32);
        assert_eq!(result.arms.len(), 4);
        assert_eq!(result.candidate_depths, [512, 1_024, 2_048, 4_096]);
        assert_eq!(result.page_body_reads, 0);
        assert!(!result.claim_eligible);
        let reader = open_reader(&evidence_path).unwrap();
        assert_eq!(reader.metadata().file_metadata().num_rows(), 128);

        let mut drifted = request.clone();
        drifted.external_queries.identity.digest = "f".repeat(64);
        drifted.evidence_output_path = temp.path().join("rejected-quality.parquet");
        assert!(run_v26_pq4_quality_frontier(&drifted).is_err());
        assert!(!drifted.evidence_output_path.exists());
    }

    #[test]
    fn v26_fast_pq16_index_arrow_roundtrips_the_exact_resident_representation() {
        // Break caught: the deployable index uses a private format, loses row/page order,
        // rebuilds PQ at open, or admits coherent bytes under a different registered digest.
        let temp = TempDir::new().unwrap();
        let rows = (0_u64..1_024)
            .map(|source_ordinal| {
                let angle = source_ordinal as f32 / 1_024.0;
                let mut vector = [0.0_f32; 96];
                vector[0] = angle.cos();
                vector[1] = angle.sin();
                crate::V26ConstructionRow {
                    source_ordinal,
                    vector,
                }
            })
            .collect::<Vec<_>>();
        let assignments = (0_u64..1_024)
            .map(|source_ordinal| crate::V26RowPages {
                source_ordinal,
                primary_page: u32::try_from(source_ordinal % 8).unwrap(),
                replica_page: 8 + u32::try_from(source_ordinal % 8).unwrap(),
            })
            .collect::<Vec<_>>();
        let expected = crate::build_v26_pq16_packed_index(&rows, &assignments).unwrap();

        let manifest = write_v26_pq16_index_arrow(temp.path(), &expected, &assignments).unwrap();
        assert_eq!(manifest.row_count, 1_024);
        assert_eq!(manifest.page_count, 16);
        assert_eq!(manifest.occurrence_count, 2_048);
        for name in [
            "pq16-codebook.arrow",
            "pq16-codes.arrow",
            "pq16-postings.arrow",
        ] {
            let bytes = fs::read(temp.path().join(name)).unwrap();
            assert_eq!(&bytes[..6], b"ARROW1");
            assert_eq!(&bytes[bytes.len() - 6..], b"ARROW1");
        }
        let reopened = read_v26_pq16_index_arrow(temp.path(), &manifest).unwrap();
        let candidate_pages = (0_u32..16).collect::<Vec<_>>();
        let expected_ranked = crate::rank_v26_pq16_packed_candidates(
            &expected,
            &candidate_pages,
            &rows[37].vector,
            512,
        )
        .unwrap();
        let actual_ranked = crate::rank_v26_pq16_packed_candidates(
            &reopened,
            &candidate_pages,
            &rows[37].vector,
            512,
        )
        .unwrap();
        assert_eq!(
            actual_ranked
                .iter()
                .map(|row| (row.source_ordinal, row.distance.to_bits()))
                .collect::<Vec<_>>(),
            expected_ranked
                .iter()
                .map(|row| (row.source_ordinal, row.distance.to_bits()))
                .collect::<Vec<_>>()
        );

        let mut drifted = manifest.clone();
        let replacement = if drifted.codes.sha256.starts_with('f') {
            "e"
        } else {
            "f"
        };
        drifted.codes.sha256.replace_range(0..1, replacement);
        assert!(read_v26_pq16_index_arrow(temp.path(), &drifted).is_err());
    }

    #[test]
    fn v26_arrow_cold_vectors_use_sparse_fixed_width_reads_without_batch_decode() {
        // Break caught: the depth-512 reranker decodes complete Arrow batches or silently
        // reads a broader vector inventory than the exact requested ordinals.
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("cold-vectors.arrow");
        let rows = (0_u64..32_768)
            .map(|source_ordinal| {
                let mut vector = [0.0_f32; 96];
                vector[usize::try_from(source_ordinal % 96).unwrap()] = 1.0;
                crate::V26ConstructionRow {
                    source_ordinal,
                    vector,
                }
            })
            .collect::<Vec<_>>();
        let assignments = (0_u64..32_768)
            .map(|source_ordinal| crate::V26RowPages {
                source_ordinal,
                primary_page: u32::try_from(source_ordinal % 64).unwrap(),
                replica_page: 64 + u32::try_from(source_ordinal % 64).unwrap(),
            })
            .collect::<Vec<_>>();
        let requested = (0_u32..512).map(|batch| batch * 64).collect::<Vec<_>>();
        let manifest = write_v26_cold_vectors_arrow(
            &path,
            &rows,
            &assignments,
            super::V26_COLD_VECTOR_BATCH_ROWS,
        )
        .unwrap();
        let reader = V26ArrowColdVectors::open(&path, &manifest).unwrap();
        let selected = reader.read_rows(&requested).unwrap();
        assert_eq!(selected.vectors.len(), 512);
        assert_eq!(selected.assignments.len(), 512);
        assert_eq!(selected.batches_read, 1);
        assert_eq!(selected.read_workers, 4);
        assert_eq!(reader.decoded_batch_count(), 0);

        if !cfg!(debug_assertions) {
            let mut latency_ns = Vec::with_capacity(128);
            for sample in 0..144 {
                let started = std::time::Instant::now();
                let selected = reader.read_rows(&requested).unwrap();
                let elapsed = started.elapsed().as_nanos();
                assert_eq!(selected.batches_read, 1);
                if sample >= 16 {
                    latency_ns.push(elapsed);
                }
            }
            latency_ns.sort_unstable();
            let p99_ns = latency_ns[(latency_ns.len() * 99).div_ceil(100) - 1];
            eprintln!("v26-arrow-512-sparse-rows-p99-ns={p99_ns}");
            assert!(p99_ns < 15_000_000);
        }
    }

    #[test]
    fn v26_pq16_serving_build_emits_authenticated_arrow_without_query_access() {
        // Break caught: construction and serving share a process, a query artifact leaks into
        // construction, or the deployable Arrow files are not bound by one canonical manifest.
        let row_count = 1_024_u64;
        let (temp, evaluation) = evaluation_fixture_with_rows(row_count);
        let receipt = read_layout_terminal(&evaluation.layout_terminal).unwrap();
        let output_dir = temp.path().join("serving");
        let request = V26Pq16ServingBuildRequest {
            construction_rows: identity(
                "construction-parquet",
                &temp.path().join("construction.parquet"),
            ),
            page_assignments: evaluation.page_assignments.clone(),
            layout_terminal: evaluation.layout_terminal.clone(),
            primary_tree: V26LocalObjectPath {
                identity: receipt.outputs[1].clone(),
                path: temp.path().join("layout/primary-tree.parquet"),
            },
            replica_tree: V26LocalObjectPath {
                identity: receipt.outputs[2].clone(),
                path: temp.path().join("layout/replica-tree.parquet"),
            },
            expected_rows: row_count,
            output_dir: output_dir.clone(),
            output_uri_prefix: "s3://v26-output/serving-a/".to_owned(),
        };

        let output = run_v26_pq16_serving_build(&request).unwrap();

        assert_eq!(output.schema, "borsuk-v26-pq16-serving-manifest-v2");
        assert_eq!(output.inputs.len(), 5);
        assert_eq!(output.outputs.len(), 7);
        assert_eq!(output.row_count, row_count);
        assert_eq!(output.page_count, 4);
        assert_eq!(output.page_body_reads, 0);
        assert_eq!(output.query_role_opens, 0);
        assert_eq!(
            fs::read(output_dir.join("serving-manifest.json")).unwrap(),
            super::canonical_v26_pq16_serving_build_output_bytes(&request, &output).unwrap()
        );
        for name in [
            "pq16-codebook.arrow",
            "pq16-codes.arrow",
            "pq16-postings.arrow",
            "cold-vectors.arrow",
            "simhash-pq16-codebook.arrow",
            "simhash-pq16-buckets.arrow",
            "simhash-pq16-records.arrow",
        ] {
            let bytes = fs::read(output_dir.join(name)).unwrap();
            assert_eq!(&bytes[..6], b"ARROW1");
            assert_eq!(&bytes[bytes.len() - 6..], b"ARROW1");
        }
        let reopened = read_v26_pq16_index_arrow(&output_dir, &output.index).unwrap();
        let simhash =
            super::read_v26_simhash_pq16_index_arrow(&output_dir, &output.simhash_index).unwrap();
        let cold =
            V26ArrowColdVectors::open(&output_dir.join("cold-vectors.arrow"), &output.cold_vectors)
                .unwrap();
        assert_eq!(
            reopened.codes.len(),
            usize::try_from(row_count).unwrap() * 16
        );
        assert_eq!(simhash.source_ordinals.len(), row_count as usize);
        assert_eq!(simhash.projected_resident_bytes_100m, 2_537_493_520);
        assert_eq!(
            cold.read_rows(&[0, 63, 64, 1_023]).unwrap().vectors.len(),
            4
        );

        let mut drifted = request.clone();
        let replacement = if drifted.construction_rows.identity.digest.starts_with('f') {
            "e"
        } else {
            "f"
        };
        drifted
            .construction_rows
            .identity
            .digest
            .replace_range(0..1, replacement);
        drifted.output_dir = temp.path().join("rejected");
        assert!(run_v26_pq16_serving_build(&drifted).is_err());
        assert!(!drifted.output_dir.exists());
    }

    #[test]
    fn v26_pq16_serving_runtime_opens_fresh_artifacts_without_construction_state() {
        // Break caught: the measured process rebuilds PQ, retains construction vectors, opens
        // page bodies, or accepts a router that is not bound to the serving assignments.
        let (temp, evaluation) = evaluation_fixture_with_rows(1_024);
        let construction = identity(
            "construction-parquet",
            &temp.path().join("construction.parquet"),
        );
        let receipt = read_layout_terminal(&evaluation.layout_terminal).unwrap();
        let serving_dir = temp.path().join("serving");
        let build_request = V26Pq16ServingBuildRequest {
            construction_rows: construction,
            page_assignments: evaluation.page_assignments.clone(),
            layout_terminal: evaluation.layout_terminal.clone(),
            primary_tree: V26LocalObjectPath {
                identity: receipt.outputs[1].clone(),
                path: temp.path().join("layout/primary-tree.parquet"),
            },
            replica_tree: V26LocalObjectPath {
                identity: receipt.outputs[2].clone(),
                path: temp.path().join("layout/replica-tree.parquet"),
            },
            expected_rows: 1_024,
            output_dir: serving_dir.clone(),
            output_uri_prefix: "s3://v26-output/serving-runtime/".to_owned(),
        };
        run_v26_pq16_serving_build(&build_request).unwrap();
        let request = V26Pq16ServingRuntimeRequest {
            serving_manifest: identity(
                "pq16-serving-manifest",
                &serving_dir.join("serving-manifest.json"),
            ),
            serving_dir,
            layout_terminal: evaluation.layout_terminal,
            primary_tree: V26LocalObjectPath {
                identity: receipt.outputs[1].clone(),
                path: temp.path().join("layout/primary-tree.parquet"),
            },
            replica_tree: V26LocalObjectPath {
                identity: receipt.outputs[2].clone(),
                path: temp.path().join("layout/replica-tree.parquet"),
            },
            external_queries: evaluation.external_queries,
            expected_queries: 512,
        };

        let runtime = open_v26_pq16_serving_runtime(&request).unwrap();
        assert_eq!(runtime.query_count(), 512);
        assert_eq!(runtime.page_body_reads(), 0);
        // This deliberately small layout has fewer than eight pages. Opening succeeds and the
        // serving call fails closed; the separate kernel contract proves exact eight-page output.
        assert!(runtime.select(0).is_err());
        assert!(runtime.select_global(0).is_err());

        let mut drifted = request.clone();
        drifted.primary_tree.identity.digest = "f".repeat(64);
        assert!(open_v26_pq16_serving_runtime(&drifted).is_err());
    }

    #[test]
    fn v26_simhash_preflight_runner_binds_truth_and_writes_96_parquet_samples() {
        // Break caught: the offline preflight skips immutable truth, rebuilds from construction,
        // emits JSON-only samples, or can become claim eligible without a later full run.
        let (temp, mut evaluation) = evaluation_fixture_with_rows(3_521);
        let mut truth_reader = open_reader(&evaluation.truth.path)
            .unwrap()
            .build()
            .unwrap();
        let truth_batch = truth_reader.next().unwrap().unwrap();
        let preflight_truth_path = temp.path().join("preflight-truth.parquet");
        // The preflight must authenticate the complete frozen truth artifact before selecting
        // its fixed 32-query evaluation prefix. A physically truncated truth file is not the
        // published authority.
        write_parquet(&preflight_truth_path, &truth_batch);
        evaluation.truth = identity("truth-parquet", &preflight_truth_path);
        let receipt = read_layout_terminal(&evaluation.layout_terminal).unwrap();
        let serving_dir = temp.path().join("simhash-serving");
        let build_request = V26Pq16ServingBuildRequest {
            construction_rows: identity(
                "construction-parquet",
                &temp.path().join("construction.parquet"),
            ),
            page_assignments: evaluation.page_assignments,
            layout_terminal: evaluation.layout_terminal.clone(),
            primary_tree: V26LocalObjectPath {
                identity: receipt.outputs[1].clone(),
                path: temp.path().join("layout/primary-tree.parquet"),
            },
            replica_tree: V26LocalObjectPath {
                identity: receipt.outputs[2].clone(),
                path: temp.path().join("layout/replica-tree.parquet"),
            },
            expected_rows: 3_521,
            output_dir: serving_dir.clone(),
            output_uri_prefix: "s3://v26-output/simhash-serving/".to_owned(),
        };
        run_v26_pq16_serving_build(&build_request).unwrap();
        let evidence_path = temp.path().join("simhash-preflight.parquet");
        let request = super::V26SimHashPreflightRequest {
            serving_manifest: identity(
                "pq16-serving-manifest",
                &serving_dir.join("serving-manifest.json"),
            ),
            serving_dir,
            layout_terminal: evaluation.layout_terminal,
            external_queries: evaluation.external_queries,
            truth: evaluation.truth,
            evidence_output_path: evidence_path.clone(),
            evidence_output_uri: "s3://v26-output/simhash-preflight.parquet".to_owned(),
        };

        let bytes = super::run_v26_simhash_preflight(&request).unwrap();

        let result: super::V26SimHashPreflightResult = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(result.schema, "borsuk-v26-simhash-pq16-preflight-result-v1");
        assert_eq!(result.query_count, 32);
        assert_eq!(result.arms.len(), 3);
        assert_eq!(result.page_body_reads, 0);
        assert!(!result.claim_eligible);
        let reader = open_reader(&evidence_path).unwrap();
        assert_eq!(reader.metadata().file_metadata().num_rows(), 96);
        assert_eq!(result.authority.truth, request.truth.identity);
        assert_eq!(
            result.authority.evidence.encoded_bytes,
            fs::metadata(evidence_path).unwrap().len()
        );
    }

    #[test]
    fn v26_pq16_global_quality_runner_binds_truth_and_writes_32_parquet_samples() {
        // Break caught: native global PQ16 is timed without authenticating truth and persisting
        // independently recomputable per-query quality evidence before a full-scale run.
        let (temp, evaluation) = evaluation_fixture_with_rows(3_521);
        let receipt = read_layout_terminal(&evaluation.layout_terminal).unwrap();
        let serving_dir = temp.path().join("global-quality-serving");
        run_v26_pq16_serving_build(&V26Pq16ServingBuildRequest {
            construction_rows: identity(
                "construction-parquet",
                &temp.path().join("construction.parquet"),
            ),
            page_assignments: evaluation.page_assignments,
            layout_terminal: evaluation.layout_terminal.clone(),
            primary_tree: V26LocalObjectPath {
                identity: receipt.outputs[1].clone(),
                path: temp.path().join("layout/primary-tree.parquet"),
            },
            replica_tree: V26LocalObjectPath {
                identity: receipt.outputs[2].clone(),
                path: temp.path().join("layout/replica-tree.parquet"),
            },
            expected_rows: 3_521,
            output_dir: serving_dir.clone(),
            output_uri_prefix: "s3://v26-output/global-quality-serving/".to_owned(),
        })
        .unwrap();
        let evidence_path = temp.path().join("global-quality.parquet");
        let request = super::V26Pq16GlobalQualityRequest {
            serving_manifest: identity(
                "pq16-serving-manifest",
                &serving_dir.join("serving-manifest.json"),
            ),
            serving_dir,
            layout_terminal: evaluation.layout_terminal,
            external_queries: evaluation.external_queries,
            truth: evaluation.truth,
            evidence_output_path: evidence_path.clone(),
            evidence_output_uri: "s3://v26-output/global-quality.parquet".to_owned(),
        };

        let bytes = super::run_v26_pq16_global_quality_preflight(&request).unwrap();

        let result: super::V26Pq16GlobalQualityResult = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(result.schema, "borsuk-v26-pq16-global-quality-result-v1");
        assert_eq!(result.query_count, 32);
        assert_eq!(result.measurement_count, 32);
        assert_eq!(result.truth, request.truth.identity);
        assert_eq!(result.page_body_reads, 0);
        assert!(!result.claim_eligible);
        let reader = open_reader(&evidence_path).unwrap();
        assert_eq!(reader.metadata().file_metadata().num_rows(), 32);
        assert_eq!(
            reader.schema().as_ref(),
            &super::v26_pq16_global_quality_schema()
        );
        assert_eq!(
            result.evidence.encoded_bytes,
            fs::metadata(evidence_path).unwrap().len()
        );
    }

    #[test]
    fn v26_dual_pq_key_preflight_runner_binds_arrow_truth_and_writes_96_samples() {
        // Break caught: the dual-key runner tunes on truth, skips an arm/query, loses either
        // Arrow-plane identity, emits non-Parquet evidence, or gains a page-body read surface.
        let (temp, mut evaluation) = evaluation_fixture_with_rows(3_521);
        let mut truth_reader = open_reader(&evaluation.truth.path)
            .unwrap()
            .build()
            .unwrap();
        let truth_batch = truth_reader.next().unwrap().unwrap();
        let truth_path = temp.path().join("dual-preflight-truth.parquet");
        write_parquet(&truth_path, &truth_batch);
        evaluation.truth = identity("truth-parquet", &truth_path);
        let receipt = read_layout_terminal(&evaluation.layout_terminal).unwrap();
        let serving_dir = temp.path().join("dual-serving");
        run_v26_pq16_serving_build(&V26Pq16ServingBuildRequest {
            construction_rows: identity(
                "construction-parquet",
                &temp.path().join("construction.parquet"),
            ),
            page_assignments: evaluation.page_assignments,
            layout_terminal: evaluation.layout_terminal.clone(),
            primary_tree: V26LocalObjectPath {
                identity: receipt.outputs[1].clone(),
                path: temp.path().join("layout/primary-tree.parquet"),
            },
            replica_tree: V26LocalObjectPath {
                identity: receipt.outputs[2].clone(),
                path: temp.path().join("layout/replica-tree.parquet"),
            },
            expected_rows: 3_521,
            output_dir: serving_dir.clone(),
            output_uri_prefix: "s3://v26-output/dual-serving/".to_owned(),
        })
        .unwrap();
        let serving_manifest_path = serving_dir.join("serving-manifest.json");
        let dual_dir = temp.path().join("dual-index");
        fs::create_dir(&dual_dir).unwrap();
        let serving_manifest_identity = identity("pq16-serving-manifest", &serving_manifest_path);
        let dual_manifest = super::build_v26_dual_pq_key_index_from_serving(
            &serving_manifest_identity,
            &serving_dir,
            &dual_dir,
        )
        .unwrap();
        let evidence_path = temp.path().join("dual-preflight.parquet");
        let request = super::V26DualPqKeyPreflightRequest {
            serving_manifest: serving_manifest_identity,
            serving_dir,
            layout_terminal: evaluation.layout_terminal,
            external_queries: evaluation.external_queries,
            truth: evaluation.truth,
            dual_index_dir: dual_dir,
            dual_index: dual_manifest,
            offsets_uri: "s3://v26-output/dual-index/pq16-dual-key-offsets.arrow".to_owned(),
            ordinals_uri: "s3://v26-output/dual-index/pq16-dual-key-ordinals.arrow".to_owned(),
            evidence_output_path: evidence_path.clone(),
            evidence_output_uri: "s3://v26-output/dual-preflight.parquet".to_owned(),
        };

        let bytes = super::run_v26_dual_pq_key_preflight(&request).unwrap();

        let result: super::V26DualPqKeyPreflightResult = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(result.schema, "borsuk-v26-dual-pq-key-preflight-result-v1");
        assert_eq!(result.query_count, 32);
        assert_eq!(result.arms.len(), 3);
        assert_eq!(result.page_body_reads, 0);
        assert!(!result.claim_eligible);
        assert_eq!(
            result
                .arms
                .iter()
                .map(|arm| arm.key_limit_per_plane)
                .collect::<Vec<_>>(),
            [1_536, 4_096, 8_192]
        );
        assert_eq!(result.ranked_row_limit, 512);
        let reader = open_reader(&evidence_path).unwrap();
        assert_eq!(reader.metadata().file_metadata().num_rows(), 96);
        assert_eq!(
            reader.schema().as_ref(),
            &super::v26_dual_pq_key_preflight_schema()
        );
        assert_eq!(result.authority.truth, request.truth.identity);
        assert_eq!(
            result.authority.offsets.digest,
            request.dual_index.offsets.sha256
        );
        assert_eq!(
            result.authority.ordinals.digest,
            request.dual_index.ordinals.sha256
        );
        assert_eq!(
            result.authority.evidence.encoded_bytes,
            fs::metadata(evidence_path).unwrap().len()
        );
    }

    #[test]
    fn v26_fast_dual_pq_key_preflight_summary_recomputes_authority_and_gates() {
        // Break caught: the canonical result trusts stored arm metrics, accepts a malformed Arrow
        // identity, or permits incomplete/non-deterministically ordered arm/query evidence.
        let object = |role: &str, fill: char| V26ObjectIdentity {
            role: role.to_owned(),
            uri: format!("s3://v26-fast/{role}"),
            digest_algorithm: "sha256".to_owned(),
            digest: fill.to_string().repeat(64),
            encoded_bytes: 1_024,
            generation: "v26-fast-dual".to_owned(),
        };
        let authority = super::V26DualPqKeyPreflightAuthority {
            serving_manifest: object("pq16-serving-manifest", 'a'),
            external_queries: object("external-queries-parquet", 'b'),
            truth: object("truth-parquet", 'c'),
            offsets: object("dual-pq-key-offsets-arrow", 'd'),
            ordinals: object("dual-pq-key-ordinals-arrow", 'e'),
            evidence: object("dual-pq-key-preflight-evidence-parquet", 'f'),
        };
        let samples = [1_536_u32, 4_096, 8_192]
            .into_iter()
            .flat_map(|key_limit_per_plane| {
                (0_u32..32).map(move |query_ordinal| super::V26DualPqKeyPreflightSample {
                    key_limit_per_plane,
                    ranked_row_limit: 512,
                    query_ordinal,
                    selected_pages: (0_u32..10).collect(),
                    hits: 10,
                    oracle_hits: 10,
                    recall_ppm: 1_000_000,
                    oracle_attainment_ppm: 1_000_000,
                    elapsed_ns: 1,
                    unique_rows_scanned: 2_048,
                    cold_batches_read: 1,
                })
            })
            .collect::<Vec<_>>();
        let result = super::summarize_v26_dual_pq_key_preflight(authority, &samples).unwrap();
        assert!(result.arms.iter().all(|arm| arm.passed));
        assert!(super::canonical_v26_dual_pq_key_preflight_result_bytes(&result, &samples).is_ok());

        let mut drifted_samples = samples.clone();
        drifted_samples[32].recall_ppm -= 1;
        assert!(
            super::canonical_v26_dual_pq_key_preflight_result_bytes(&result, &drifted_samples)
                .is_err()
        );
        let mut drifted_result = result.clone();
        drifted_result.authority.offsets.digest = "0".repeat(63);
        assert!(
            super::canonical_v26_dual_pq_key_preflight_result_bytes(&drifted_result, &samples)
                .is_err()
        );
        let mut drifted_result = result;
        drifted_result.arms[2].maximum_latency_ns += 1;
        assert!(
            super::canonical_v26_dual_pq_key_preflight_result_bytes(&drifted_result, &samples)
                .is_err()
        );
    }

    #[test]
    fn v26_fast_pq16_serving_benchmark_summary_recomputes_latency_gate() {
        // Break caught: p99 is averaged/rounded down, raw timings disappear into JSON, or the
        // reported pass can drift from the fixed 15 ms serving gate.
        let samples = (0_u32..10_000)
            .map(|sample_ordinal| V26ServingLatencySample {
                sample_ordinal,
                query_ordinal: sample_ordinal % 512,
                elapsed_ns: u64::from(sample_ordinal + 1),
                cold_batches_read: 8,
            })
            .collect::<Vec<_>>();
        let serving_manifest = V26ObjectIdentity {
            role: "pq16-serving-manifest".to_owned(),
            uri: "s3://v26/serving-manifest.json".to_owned(),
            digest_algorithm: "sha256".to_owned(),
            digest: "a".repeat(64),
            encoded_bytes: 1_024,
            generation: "v26-local-test".to_owned(),
        };
        let external_queries = V26ObjectIdentity {
            role: "external-queries-parquet".to_owned(),
            uri: "s3://v26/queries.parquet".to_owned(),
            digest_algorithm: "sha256".to_owned(),
            digest: "b".repeat(64),
            encoded_bytes: 2_048,
            generation: "v26-local-test".to_owned(),
        };
        let evidence = V26ObjectIdentity {
            role: "pq16-serving-latency-parquet".to_owned(),
            uri: "s3://v26/latency.parquet".to_owned(),
            digest_algorithm: "sha256".to_owned(),
            digest: "c".repeat(64),
            encoded_bytes: 4_096,
            generation: "v26-local-test".to_owned(),
        };
        let result = super::summarize_v26_pq16_serving_benchmark(
            serving_manifest,
            external_queries,
            evidence,
            &samples,
        )
        .unwrap();
        assert_eq!(result.p50_ns, 5_000);
        assert_eq!(result.p95_ns, 9_500);
        assert_eq!(result.p99_ns, 9_900);
        assert_eq!(result.maximum_ns, 10_000);
        assert_eq!(result.selected_page_count, 10);
        assert!(result.passed);
        let batch = super::v26_serving_latency_batch(&samples).unwrap();
        assert_eq!(batch.num_rows(), 10_000);
        assert_eq!(batch.schema().field(0).name(), "sample_ordinal");
        assert_eq!(batch.schema().field(2).name(), "elapsed_ns");
        let bytes = canonical_v26_pq16_serving_benchmark_result_bytes(&result, &samples).unwrap();
        assert_eq!(bytes.last(), Some(&b'\n'));

        let mut drifted = result.clone();
        drifted.p99_ns += 1;
        assert!(canonical_v26_pq16_serving_benchmark_result_bytes(&drifted, &samples).is_err());
        let mut drifted = result;
        drifted.passed = false;
        assert!(canonical_v26_pq16_serving_benchmark_result_bytes(&drifted, &samples).is_err());
    }

    #[test]
    fn v26_fast_smoke_global_quality_recomputes_the_fail_fast_truth_gate() {
        // Break caught: the short global-scan preflight can pass latency while silently
        // missing truth neighbors, trusting stored quality aggregates, or changing its depth.
        let samples = (0_u32..32)
            .map(|query_ordinal| V26Pq16GlobalQualitySample {
                elapsed_ns: u64::from(query_ordinal + 1) * 100_000,
                global_adc_elapsed_ns: u64::from(query_ordinal + 1) * 100_000 - 20_000,
                exact_rerank_elapsed_ns: 10_000,
                query_ordinal,
                selected_pages: (0_u32..10).collect(),
                hits: if query_ordinal == 0 { 8 } else { 10 },
                oracle_hits: 10,
                recall_ppm: if query_ordinal == 0 {
                    800_000
                } else {
                    1_000_000
                },
                oracle_attainment_ppm: if query_ordinal == 0 {
                    800_000
                } else {
                    1_000_000
                },
                exact_rows_read: 2_048,
                cold_batches_read: 153,
            })
            .collect::<Vec<_>>();
        let identity = |role: &str, fill: char| V26ObjectIdentity {
            role: role.to_owned(),
            uri: format!("s3://v26/{role}"),
            digest_algorithm: "sha256".to_owned(),
            digest: fill.to_string().repeat(64),
            encoded_bytes: 1_024,
            generation: "v26-local-test".to_owned(),
        };
        let result = V26Pq16GlobalQualityResult {
            schema: "borsuk-v26-pq16-global-quality-result-v2".to_owned(),
            serving_manifest: identity("pq16-serving-manifest", 'a'),
            external_queries: identity("external-queries-parquet", 'b'),
            truth: identity("truth-parquet", 'c'),
            evidence: identity("pq16-global-preflight-evidence-parquet", 'd'),
            query_count: 32,
            ranked_row_limit: 2_048,
            selected_page_count: 10,
            warmup_count: 2,
            measurement_count: 32,
            p50_ns: 1_600_000,
            p95_ns: 3_100_000,
            maximum_ns: 3_200_000,
            global_adc_p50_ns: 1_580_000,
            global_adc_p95_ns: 3_080_000,
            global_adc_maximum_ns: 3_180_000,
            exact_rerank_p50_ns: 10_000,
            exact_rerank_p95_ns: 10_000,
            exact_rerank_maximum_ns: 10_000,
            fail_fast_gate_ns: 15_000_000,
            aggregate_recall_ppm: 993_750,
            minimum_query_recall_ppm: 800_000,
            oracle_attainment_ppm: 993_750,
            aggregate_recall_gate_ppm: 975_000,
            minimum_query_recall_gate_ppm: 800_000,
            oracle_attainment_gate_ppm: 995_000,
            passed: false,
            page_body_reads: 0,
            claim_eligible: false,
        };
        assert!(super::canonical_v26_pq16_global_quality_result_bytes(&result, &samples).is_ok());

        let mut drifted = result.clone();
        drifted.maximum_ns += 1;
        assert!(super::canonical_v26_pq16_global_quality_result_bytes(&drifted, &samples).is_err());

        let mut drifted_samples = samples;
        drifted_samples[0].hits = 10;
        assert!(
            super::canonical_v26_pq16_global_quality_result_bytes(&result, &drifted_samples)
                .is_err()
        );
        let mut drifted_samples = drifted_samples;
        drifted_samples[0].hits = 8;
        drifted_samples[31].global_adc_elapsed_ns += 1;
        assert!(
            super::canonical_v26_pq16_global_quality_result_bytes(&result, &drifted_samples)
                .is_err()
        );
    }

    #[test]
    fn v26_fast_smoke_global_quality_sample_joins_selection_to_truth_without_page_reads() {
        // Break caught: the fast global screen measures latency but never joins the selected
        // pages to the independently authenticated truth assignments.
        let selection = crate::V26Pq16ServingSelection {
            selected_pages: (0_u32..10).collect(),
            exact_rows_read: 2_048,
            cold_batches_read: 2,
            cold_read_workers: 4,
            page_body_reads: 0,
        };
        let mut assignments = (0_u32..8)
            .map(|page| vec![page, 200 + page])
            .collect::<Vec<_>>();
        assignments.extend([vec![100, 101], vec![102, 103]]);
        let truth = crate::V26QueryTruth {
            query_ordinal: 7,
            neighbor_source_ordinals: (0_u64..10).collect(),
            ground_truth_page_assignments: assignments,
        };

        let sample =
            super::v26_pq16_global_quality_sample(7, &selection, &truth, 1_234, 700, 400).unwrap();
        assert_eq!(sample.query_ordinal, 7);
        assert_eq!(sample.selected_pages, (0_u32..10).collect::<Vec<_>>());
        assert_eq!(sample.hits, 8);
        assert_eq!(sample.oracle_hits, 10);
        assert_eq!(sample.recall_ppm, 800_000);
        assert_eq!(sample.oracle_attainment_ppm, 800_000);
        assert_eq!(sample.elapsed_ns, 1_234);
        assert_eq!(sample.global_adc_elapsed_ns, 700);
        assert_eq!(sample.exact_rerank_elapsed_ns, 400);
        assert_eq!(sample.exact_rows_read, 2_048);
        assert_eq!(sample.cold_batches_read, 2);
    }

    #[test]
    fn v26_fast_simhash_preflight_recomputes_truth_quality_and_latency_gates() {
        // Break caught: the SimHash preflight trusts stored recall, latency, or arm pass fields.
        let identity = |role: &str, fill: char| V26ObjectIdentity {
            role: role.to_owned(),
            uri: format!("s3://v26/{role}"),
            digest_algorithm: "sha256".to_owned(),
            digest: fill.to_string().repeat(64),
            encoded_bytes: 1_024,
            generation: "v26-local-test".to_owned(),
        };
        let authority = super::V26SimHashPreflightAuthority {
            serving_manifest: identity("pq16-serving-manifest", 'a'),
            external_queries: identity("external-queries-parquet", 'b'),
            truth: identity("truth-parquet", 'c'),
            evidence: identity("simhash-preflight-evidence-parquet", 'd'),
        };
        let samples = [137_u32, 697, 2_517]
            .into_iter()
            .flat_map(|bucket_limit| {
                (0_u32..32).map(move |query_ordinal| super::V26SimHashPreflightSample {
                    bucket_limit,
                    query_ordinal,
                    selected_pages: (0_u32..10).collect(),
                    hits: 10,
                    oracle_hits: 10,
                    recall_ppm: 1_000_000,
                    oracle_attainment_ppm: 1_000_000,
                    elapsed_ns: 1_000_000 + u64::from(query_ordinal),
                    rows_scanned: 8_192,
                    cold_batches_read: 2,
                })
            })
            .collect::<Vec<_>>();
        let result = super::summarize_v26_simhash_preflight(authority.clone(), &samples).unwrap();
        assert_eq!(result.authority, authority);
        assert_eq!(result.query_count, 32);
        assert_eq!(result.arms.len(), 3);
        assert!(result.arms.iter().all(|arm| arm.passed));
        assert!(!result.claim_eligible);
        assert_eq!(result.page_body_reads, 0);
        let bytes = super::canonical_v26_simhash_preflight_result_bytes(&result, &samples).unwrap();
        assert_eq!(bytes.last(), Some(&b'\n'));

        let mut drifted = result.clone();
        drifted.arms[0].aggregate_recall_ppm -= 1;
        assert!(super::canonical_v26_simhash_preflight_result_bytes(&drifted, &samples).is_err());
        let mut drifted = result;
        drifted.arms[0].passed = false;
        assert!(super::canonical_v26_simhash_preflight_result_bytes(&drifted, &samples).is_err());
    }
}
