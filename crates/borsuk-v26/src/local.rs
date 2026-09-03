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
    UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow_buffer::Buffer;
use arrow_ipc::{
    Block, MetadataVersion,
    convert::fb_to_schema,
    reader::{FileDecoder, FileReader, read_footer_length},
    root_as_footer,
    writer::FileWriter,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use parquet::{
    arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder},
    basic::Compression,
    file::properties::{WriterProperties, WriterVersion},
};
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::tree::build_v26_dual_tree_layout_with_workers;
use crate::{
    Result, V26ConstructionRow, V26Disposition, V26ExactGlobalRankResult, V26ExactGlobalResult,
    V26ExactGlobalSample, V26ExternalQuery, V26ExternalTruth, V26LayoutAuthority, V26LayoutReceipt,
    V26LayoutResult, V26LayoutSample, V26Node, V26ObjectIdentity, V26PageModeSample,
    V26Pq16ServingSelection, V26PqRankedRow, V26QueryTruth, V26RowPages, V26Tree,
    build_v26_external_truth_rows, canonical_json_value, canonical_v26_exact_global_result_bytes,
    canonical_v26_layout_receipt_bytes, canonical_v26_layout_result_bytes,
    canonical_v26_tree_router_result_bytes, diagnose_v26_tree_router_candidate_widths,
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

pub fn v26_source_map_schema() -> Schema {
    Schema::new(vec![
        Field::new("source_ordinal", DataType::UInt64, false),
        Field::new("dataset_ordinal", DataType::UInt64, false),
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
    pub source_map: V26LocalObjectPath,
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

#[derive(Debug, Clone, PartialEq, Eq)]
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
    blocks: Vec<Block>,
    schema: SchemaRef,
    metadata_version: MetadataVersion,
    pool: rayon::ThreadPool,
    row_count: u64,
    batch_rows: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct V26ArrowFileIdentity {
    pub encoded_bytes: u64,
    pub sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct V26Pq16IndexManifest {
    pub row_count: u64,
    pub page_count: u32,
    pub occurrence_count: u64,
    pub projected_resident_bytes_100m: u64,
    pub codebook: V26ArrowFileIdentity,
    pub codes: V26ArrowFileIdentity,
    pub postings: V26ArrowFileIdentity,
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
    )?;
    let samples = truths
        .iter()
        .map(|truth| {
            let selected_pages =
                exact_v26_layout_oracle_pages(&truth.ground_truth_page_assignments, 8)?;
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
    canonical_v26_layout_result_bytes(&result, &truths, &samples)?;
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
) -> Result<Vec<V26QueryTruth>> {
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
            if neighbor_values.len() != 10
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
                let assignment = assignments
                    .get(
                        usize::try_from(*neighbor)
                            .map_err(|_| invalid("V26 truth source differs"))?,
                    )
                    .ok_or_else(|| invalid("V26 truth source differs"))?;
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

fn external_truth_batch(rows: &[V26ExternalTruth]) -> Result<RecordBatch> {
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
        write_batch(&request.output_path, external_truth_batch(&truth)?)?;
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
    let (_, _, layout_result) = evaluate_v26_layout_oracle(&request.layout)?;
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
    let (truths, _, layout_result) = evaluate_v26_layout_oracle(&request.layout)?;
    if request.page_budget != 8
        || layout_result.disposition != V26Disposition::BoundedLayoutCandidate
    {
        return Err(invalid("V26 tree router layout gate is closed"));
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
    let queries = read_evaluation_queries(
        &request.layout.external_queries.path,
        request.layout.expected_queries,
    )?;
    Ok((primary, replica, queries, truths))
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
    let (primary, replica, queries, truths) = load_v26_tree_router(request)?;
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

fn v26_candidate_cover_evidence_schema() -> Schema {
    Schema::new(vec![
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

fn v26_candidate_cover_evidence_batch(
    samples: &[crate::V26TreeRouterSample],
    candidate_page_limit: u32,
) -> Result<RecordBatch> {
    if samples.len() != 512
        || samples
            .iter()
            .any(|sample| sample.selected_pages.len() != 8)
    {
        return Err(invalid("V26 candidate cover evidence inventory differs"));
    }
    let pages = samples
        .iter()
        .flat_map(|sample| sample.selected_pages.iter().copied())
        .collect::<Vec<_>>();
    RecordBatch::try_new(
        Arc::new(v26_candidate_cover_evidence_schema()),
        vec![
            Arc::new(UInt32Array::from_iter_values(
                samples.iter().map(|sample| sample.query_ordinal),
            )),
            Arc::new(UInt32Array::from_value(candidate_page_limit, samples.len())),
            Arc::new(UInt32Array::from_value(10, samples.len())),
            Arc::new(
                FixedSizeListArray::try_new(
                    Arc::new(Field::new("element", DataType::UInt32, false)),
                    8,
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
    let loaded = load_v26_exact_global(&exact)?;
    let (primary, replica, queries, truths) = load_v26_tree_router(&request.router)?;
    if queries != loaded.queries || truths != loaded.truths || request.router.page_budget != 8 {
        return Err(invalid("V26 candidate cover authority differs"));
    }
    let page_count = rank_v26_tree_pages(&primary, &replica, &queries[0].vector)?.len();
    let candidate_page_limit = 128.min(page_count);
    let (samples, result) = evaluate_v26_candidate_row_cover(
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
                    .map_err(|_| invalid("V26 candidate cover width overflows"))?,
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
            DataType::FixedSizeList(Arc::new(Field::new("element", DataType::UInt32, false)), 8),
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
                    .any(|sample| sample.selected_pages.len() != 8)
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
                    8,
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
    let loaded = load_v26_exact_global(&exact)?;
    let (primary, replica, queries, truths) = load_v26_tree_router(&request.router)?;
    if queries != loaded.queries || truths != loaded.truths || request.router.page_budget != 8 {
        return Err(invalid("V26 PQ16 rerank authority differs"));
    }
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

pub fn write_v26_cold_vectors_arrow(
    path: &Path,
    rows: &[V26ConstructionRow],
    assignments: &[V26RowPages],
    batch_rows: u32,
) -> Result<V26ColdVectorManifest> {
    if path.exists()
        || rows.is_empty()
        || rows.len() != assignments.len()
        || batch_rows != 64
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
            || manifest.batch_rows != 64
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
        let metadata_version = footer.version();
        let expected_batches = manifest.row_count.div_ceil(u64::from(manifest.batch_rows));
        if schema.as_ref() != &v26_cold_vector_schema()
            || u64::try_from(blocks.len()).unwrap() != expected_batches
        {
            return Err(invalid("V26 cold-vector schema differs"));
        }
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(4)
            .thread_name(|index| format!("v26-cold-{index}"))
            .build()
            .map_err(|error| invalid(&format!("V26 cold-vector pool failed: {error}")))?;
        Ok(Self {
            file,
            blocks,
            schema,
            metadata_version,
            pool,
            row_count: manifest.row_count,
            batch_rows: manifest.batch_rows,
        })
    }

    fn read_batch(
        &self,
        batch_index: u32,
        row_ids: &[u32],
    ) -> Result<Vec<([f32; 96], V26RowPages)>> {
        let block = *self
            .blocks
            .get(usize::try_from(batch_index).unwrap())
            .ok_or_else(|| invalid("V26 cold-vector batch is absent"))?;
        let length = usize::try_from(block.metaDataLength())
            .ok()
            .and_then(|metadata| {
                usize::try_from(block.bodyLength())
                    .ok()
                    .and_then(|body| metadata.checked_add(body))
            })
            .ok_or_else(|| invalid("V26 cold-vector block length differs"))?;
        let offset = u64::try_from(block.offset())
            .map_err(|_| invalid("V26 cold-vector block offset differs"))?;
        let mut bytes = vec![0_u8; length];
        self.file
            .read_exact_at(&mut bytes, offset)
            .map_err(|error| invalid(&format!("V26 cold-vector batch read failed: {error}")))?;
        let batch = FileDecoder::new(self.schema.clone(), self.metadata_version)
            .read_record_batch(&block, &Buffer::from(bytes))
            .map_err(|error| invalid(&format!("V26 cold-vector decode failed: {error}")))?
            .ok_or_else(|| invalid("V26 cold-vector batch is absent"))?;
        let first = u64::from(batch_index) * u64::from(self.batch_rows);
        let expected_rows = usize::try_from(
            self.row_count
                .saturating_sub(first)
                .min(u64::from(self.batch_rows)),
        )
        .unwrap();
        if batch.num_rows() != expected_rows
            || batch
                .columns()
                .iter()
                .any(|column| column.null_count() != 0)
        {
            return Err(invalid("V26 cold-vector batch shape differs"));
        }
        let ordinals = batch.columns()[0]
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| invalid("V26 cold-vector ordinal array differs"))?;
        if (0..expected_rows).any(|row| ordinals.value(row) != first + row as u64) {
            return Err(invalid("V26 cold-vector ordinal binding differs"));
        }
        let list = batch.columns()[1]
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .ok_or_else(|| invalid("V26 cold-vector list differs"))?;
        let values = list
            .values()
            .as_any()
            .downcast_ref::<Float32Array>()
            .ok_or_else(|| invalid("V26 cold-vector values differ"))?;
        let primary_pages = batch.columns()[2]
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or_else(|| invalid("V26 cold-vector primary pages differ"))?;
        let replica_pages = batch.columns()[3]
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or_else(|| invalid("V26 cold-vector replica pages differ"))?;
        row_ids
            .iter()
            .map(|row_id| {
                let local = usize::try_from(u64::from(*row_id) - first).unwrap();
                let assignment = V26RowPages {
                    source_ordinal: u64::from(*row_id),
                    primary_page: primary_pages.value(local),
                    replica_page: replica_pages.value(local),
                };
                if assignment.primary_page == assignment.replica_page {
                    return Err(invalid("V26 cold-vector assignment differs"));
                }
                let start = local * 96;
                let vector: [f32; 96] = values.values()[start..start + 96]
                    .try_into()
                    .map_err(|_| invalid("V26 cold-vector width differs"))?;
                validate_v26_vector(&vector)?;
                Ok((vector, assignment))
            })
            .collect()
    }

    pub fn read_rows(&self, row_ids: &[u32]) -> Result<V26ColdVectorRead> {
        if row_ids.is_empty()
            || row_ids.windows(2).any(|pair| pair[0] >= pair[1])
            || row_ids.iter().any(|row| u64::from(*row) >= self.row_count)
        {
            return Err(invalid("V26 cold-vector read request differs"));
        }
        let mut groups = Vec::<(u32, &[u32])>::new();
        let mut cursor = 0;
        while cursor < row_ids.len() {
            let batch_index = row_ids[cursor] / self.batch_rows;
            let start = cursor;
            while cursor < row_ids.len() && row_ids[cursor] / self.batch_rows == batch_index {
                cursor += 1;
            }
            groups.push((batch_index, &row_ids[start..cursor]));
        }
        let selected = self.pool.install(|| {
            groups
                .par_iter()
                .map(|(batch, rows)| self.read_batch(*batch, rows))
                .collect::<Result<Vec<_>>>()
        })?;
        let selected = selected.into_iter().flatten().collect::<Vec<_>>();
        Ok(V26ColdVectorRead {
            vectors: selected.iter().map(|(vector, _)| *vector).collect(),
            assignments: selected.iter().map(|(_, assignment)| *assignment).collect(),
            batches_read: u32::try_from(groups.len()).unwrap(),
            read_workers: u32::try_from(groups.len().min(4)).unwrap(),
        })
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
    let cold = cold_vectors.read_rows(&source_ordinals)?;
    let mut exact = approximate
        .iter()
        .map(|candidate| {
            let source_ordinal = u32::try_from(candidate.source_ordinal)
                .map_err(|_| invalid("V26 PQ16 Arrow source ordinal differs"))?;
            let position = source_ordinals
                .binary_search(&source_ordinal)
                .map_err(|_| invalid("V26 PQ16 Arrow cold-vector binding differs"))?;
            let assignment = cold.assignments[position];
            if assignment.source_ordinal != candidate.source_ordinal {
                return Err(invalid("V26 PQ16 Arrow assignment binding differs"));
            }
            let distance = v26_squared_l2(&cold.vectors[position], query);
            if !distance.is_finite() {
                return Err(invalid("V26 PQ16 Arrow exact distance differs"));
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
    let ranked_assignments = exact[..10]
        .iter()
        .map(|(_, pages)| pages.to_vec())
        .collect::<Vec<_>>();
    let mut selected_pages = exact_v26_layout_oracle_pages(&ranked_assignments, 8)?;
    for page in candidate_pages {
        if selected_pages.len() == 8 {
            break;
        }
        if !selected_pages.contains(page) {
            selected_pages.push(*page);
        }
    }
    if selected_pages.len() != 8 {
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
    if request.construction_rows.identity != authority.construction_rows
        || request.source_map.identity != authority.source_map
    {
        return Err(invalid("V26 construction input authority differs"));
    }
    authenticate(&request.construction_rows, "construction-parquet")?;
    authenticate(&request.source_map, "source-map-parquet")?;
    if request.construction_rows.identity.generation != authority.generation
        || request.source_map.identity.generation != authority.generation
    {
        return Err(invalid("V26 input generation differs"));
    }
    let expected_rows_i64 = i64::try_from(authority.expected_rows)
        .map_err(|_| invalid("V26 input row count overflows"))?;
    let expected_rows_usize = usize::try_from(authority.expected_rows)
        .map_err(|_| invalid("V26 input row count overflows"))?;
    let construction = open_reader(&request.construction_rows.path)?;
    let source = open_reader(&request.source_map.path)?;
    let construction_rows = construction.metadata().file_metadata().num_rows();
    let source_rows = source.metadata().file_metadata().num_rows();
    if construction.schema().as_ref() != &v26_construction_schema()
        || source.schema().as_ref() != &v26_source_map_schema()
        || construction_rows != source_rows
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
    let mut observed_source = Vec::with_capacity(rows.len());
    let mut datasets = BTreeSet::new();
    'source: for batch in source
        .build()
        .map_err(|error| invalid(&format!("V26 source-map reader failed: {error}")))?
    {
        let batch =
            batch.map_err(|error| invalid(&format!("V26 source-map batch failed: {error}")))?;
        let sources = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| invalid("V26 source-map ordinal differs"))?;
        let dataset = batch
            .column(1)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| invalid("V26 dataset ordinal differs"))?;
        if batch
            .columns()
            .iter()
            .any(|column| column.null_count() != 0)
        {
            return Err(invalid("V26 source-map nullability differs"));
        }
        for index in 0..batch.num_rows() {
            if observed_source.len() == expected_rows_usize {
                break 'source;
            }
            let source_ordinal = sources.value(index);
            if source_ordinal != observed_source.len() as u64
                || !datasets.insert(dataset.value(index))
            {
                return Err(invalid("V26 source-map inventory differs"));
            }
            observed_source.push(source_ordinal);
        }
    }
    if rows.len() as u64 != authority.expected_rows || observed_source.len() != rows.len() {
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
        request.source_map.identity.uri.clone(),
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
                request.source_map.identity.clone(),
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
        source_map: V26LocalObjectPath {
            identity: authority.source_map.clone(),
            path: input_dir.join("source-map.parquet"),
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

pub fn validate_v26_layout_build_output(
    request: &V26LayoutBuildRequest,
    output: &V26LayoutBuildOutput,
) -> Result<()> {
    validate_uri_inventory(request)?;
    let observed_authority = read_manifest(&request.manifest)?;
    authenticate(&request.construction_rows, "construction-parquet")?;
    authenticate(&request.source_map, "source-map-parquet")?;
    if output.authority != observed_authority
        || output.authority.generation != request.manifest.identity.generation
        || output.inputs
            != vec![
                request.construction_rows.identity.clone(),
                request.manifest.identity.clone(),
                request.source_map.identity.clone(),
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
    use std::{collections::BTreeMap, fs, io::Write, sync::Arc};

    use arrow_array::{
        ArrayRef, FixedSizeListArray, Float32Array, RecordBatch, UInt32Array, UInt64Array,
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
        V26LocalObjectPath, V26PageModeRouterRequest, V26Pq8CoverRequest, V26Pq16RerankRequest,
        V26PqWidthLadderRequest, V26TreeRouterRequest, V26TruthBuildRequest, assignments_batch,
        evaluate_v26_exact_global, evaluate_v26_layout_oracle, open_reader, output_identity,
        read_assignments, read_layout_terminal, read_v26_pq16_index_arrow,
        run_v26_candidate_row_cover, run_v26_centroid_router, run_v26_layout_build,
        run_v26_page_mode_router, run_v26_pq_width_ladder, run_v26_pq8_candidate_cover,
        run_v26_pq16_exact_rerank, run_v26_tree_router, run_v26_tree_router_diagnostic,
        run_v26_truth_build, select_v26_pq16_pages_from_arrow, v26_construction_schema,
        v26_page_assignments_schema, v26_query_schema, v26_source_map_schema, v26_tree_schema,
        v26_truth_schema, validate_v26_layout_build_output, write_v26_cold_vectors_arrow,
        write_v26_pq16_index_arrow,
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

    fn fixture_with_rows(
        expected_rows: u64,
    ) -> (
        TempDir,
        V26LocalObjectPath,
        V26LocalObjectPath,
        V26LocalObjectPath,
    ) {
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

        let source_map = RecordBatch::try_new(
            Arc::new(v26_source_map_schema()),
            vec![
                Arc::new(ordinals) as ArrayRef,
                Arc::new(UInt64Array::from_iter_values(
                    10_000..10_000 + expected_rows,
                )),
            ],
        )
        .unwrap();
        let source_map_path = temp.path().join("source-map.parquet");
        write_parquet(&source_map_path, &source_map);
        let construction = identity("construction-parquet", &construction_path);
        let source_map = identity("source-map-parquet", &source_map_path);
        let authority = V26LayoutAuthority {
            schema: "borsuk-v26-dual-tree-layout-v1".to_owned(),
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
            source_map: source_map.identity.clone(),
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
            source_map,
        )
    }

    fn fixture() -> (
        TempDir,
        V26LocalObjectPath,
        V26LocalObjectPath,
        V26LocalObjectPath,
    ) {
        fixture_with_rows(1_409)
    }

    fn request(
        manifest: V26LocalObjectPath,
        construction_rows: V26LocalObjectPath,
        source_map: V26LocalObjectPath,
        output_dir: std::path::PathBuf,
        worker_count: usize,
    ) -> V26LayoutBuildRequest {
        V26LayoutBuildRequest {
            manifest,
            construction_rows,
            source_map,
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
        let (temp, manifest, construction, source_map) = fixture_with_rows(expected_rows);
        let output_dir = temp.path().join("layout");
        let build_request = request(manifest, construction, source_map, output_dir.clone(), 2);
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
            external_queries: identity("external-queries-parquet", &query_path),
            truth: identity("truth-parquet", &truth_path),
            expected_queries: 512,
        };
        (temp, request)
    }

    fn evaluation_fixture() -> (TempDir, V26LayoutEvaluationRequest) {
        evaluation_fixture_with_rows(1_409)
    }

    #[test]
    fn v26_layout_local_authenticates_construction_only_and_emits_parquet() {
        // Break caught: parsing before authentication or emitting a nondeterministic layout.
        let (temp, manifest, construction, source_map) = fixture();
        let first_dir = temp.path().join("out-one");
        let second_dir = temp.path().join("out-four");
        let first = run_v26_layout_build(&request(
            manifest.clone(),
            construction.clone(),
            source_map.clone(),
            first_dir.clone(),
            1,
        ))
        .unwrap();
        let second_request = request(manifest, construction, source_map, second_dir.clone(), 4);
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
    }

    #[test]
    fn v26_layout_local_rejects_query_truth_and_result_roles() {
        // Break caught: construction gains a query/evaluation capability.
        for forbidden in ["external-queries-parquet", "truth-parquet", "prior-result"] {
            let (temp, manifest, mut construction, source_map) = fixture();
            construction.identity.role = forbidden.to_owned();
            let output = temp.path().join("forbidden-output");
            assert!(
                run_v26_layout_build(&request(
                    manifest,
                    construction,
                    source_map,
                    output.clone(),
                    1
                ))
                .is_err()
            );
            assert!(!output.exists());
        }
    }

    #[test]
    fn v26_layout_local_rejects_input_output_uri_role_overlap() {
        // Break caught: one immutable URI is assigned both an input and output role.
        let (temp, manifest, mut construction, source_map) = fixture();
        construction.identity.uri = "s3://v26-output/layout-a/page-assignments.parquet".to_owned();
        let output_dir = temp.path().join("overlap-output");
        assert!(
            run_v26_layout_build(&request(
                manifest,
                construction,
                source_map,
                output_dir.clone(),
                1,
            ))
            .is_err()
        );
        assert!(!output_dir.exists());
    }

    #[test]
    fn v26_layout_local_manifest_rejects_coherent_input_substitution() {
        // Break caught: a different URI with identical valid bytes replaces a frozen input.
        let (temp, manifest, construction, source_map) = fixture();
        let alternate_path = temp.path().join("alternate-construction.parquet");
        fs::copy(&construction.path, &alternate_path).unwrap();
        let mut alternate = identity("construction-parquet", &alternate_path);
        alternate.identity.uri = "s3://v26-input/alternate-construction-parquet".to_owned();
        let output_dir = temp.path().join("substituted-output");
        assert!(
            run_v26_layout_build(&request(
                manifest,
                alternate,
                source_map,
                output_dir.clone(),
                1,
            ))
            .is_err()
        );
        assert!(!output_dir.exists());
    }

    #[test]
    fn v26_layout_local_smoke_uses_exact_registered_prefix_without_conversion() {
        // Break caught: the structural smoke requires a separately materialized corpus.
        let (temp, manifest, construction, source_map) = fixture();
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
            source_map,
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
        let (temp, manifest, construction, source_map) = fixture();
        let output_dir = temp.path().join("outputs");
        let request = request(manifest, construction, source_map, output_dir.clone(), 1);
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
        let (temp, manifest, construction, source_map) = fixture();
        let output_dir = temp.path().join("outputs");
        let request = request(manifest, construction, source_map, output_dir, 1);
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
        let (temp, manifest, construction, source_map) = fixture();
        let output_dir = temp.path().join("outputs");
        let request = request(manifest, construction, source_map, output_dir.clone(), 1);
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
        assert_eq!(value["mode_results"].as_array().unwrap().len(), 4);
        assert_eq!(value["evidence"]["role"], "page-mode-evidence-parquet");
        assert_eq!(value["evidence"]["uri"], request.evidence_output_uri);
        assert_eq!(value["page_body_reads"], 0);
        assert_eq!(value["claim_eligible"], false);
        assert!(evidence_path.is_file());
        let reader = open_reader(&evidence_path).unwrap();
        assert_eq!(reader.metadata().file_metadata().num_rows(), 4 * 512);
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
                page_budget: 8,
            },
            evidence_output_path: evidence_path.clone(),
            evidence_output_uri: "s3://frozen/v26/pq16-rerank-evidence.parquet".to_owned(),
        };

        let bytes = run_v26_pq16_exact_rerank(&request).unwrap();
        let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(value["schema"], "borsuk-v26-pq16-exact-rerank-output-v1");
        assert_eq!(value["candidate_page_limit"], 8);
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
        // Break caught: exact vectors use a private binary format, load the full corpus into RAM,
        // lose assignment authority, or seek once per reranked row instead of once per page.
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
        let manifest = write_v26_cold_vectors_arrow(&path, &rows, &assignments, 64).unwrap();
        assert_eq!(manifest.row_count, 1_024);
        assert_eq!(manifest.batch_rows, 64);
        let bytes = fs::read(&path).unwrap();
        assert_eq!(&bytes[..6], b"ARROW1");
        assert_eq!(&bytes[bytes.len() - 6..], b"ARROW1");

        let reader = V26ArrowColdVectors::open(&path, &manifest).unwrap();
        let selected = reader.read_rows(&[0, 1, 63, 64, 511, 512, 1_023]).unwrap();
        assert_eq!(selected.vectors.len(), 7);
        assert_eq!(selected.batches_read, 5);
        assert_eq!(selected.read_workers, 4);
        assert_eq!(selected.vectors[0], rows[0].vector);
        assert_eq!(selected.vectors[6], rows[1_023].vector);
        assert_eq!(selected.assignments[0], assignments[0]);
        assert_eq!(selected.assignments[6], assignments[1_023]);
        assert!(reader.read_rows(&[64, 63]).is_err());
        assert!(reader.read_rows(&[1_024]).is_err());
    }

    #[test]
    fn v26_pq16_arrow_serving_matches_reference_with_bounded_cold_reads() {
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
            .map(|source_ordinal| crate::V26RowPages {
                source_ordinal,
                primary_page: u32::try_from(source_ordinal % 8).unwrap(),
                replica_page: 8 + u32::try_from(source_ordinal % 8).unwrap(),
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

        let manifest = write_v26_cold_vectors_arrow(&path, &rows, &assignments, 64).unwrap();
        let reader = V26ArrowColdVectors::open(&path, &manifest).unwrap();
        let result =
            select_v26_pq16_pages_from_arrow(&index, &candidate_pages, &query, &reader).unwrap();

        assert_eq!(result.selected_pages, reference.selected_pages);
        assert_eq!(result.exact_rows_read, 512);
        assert!(result.cold_batches_read > 0 && result.cold_batches_read <= 34);
        assert_eq!(result.cold_read_workers, 4);
        assert_eq!(result.page_body_reads, 0);

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
    fn v26_pq16_index_arrow_roundtrips_the_exact_resident_representation() {
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
    fn v26_arrow_cold_vectors_parallelize_512_sparse_batches() {
        // Break caught: the depth-512 reranker serializes independent Arrow batch reads or
        // silently reads a broader vector inventory than the exact requested ordinals.
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
        let manifest = write_v26_cold_vectors_arrow(&path, &rows, &assignments, 64).unwrap();
        let reader = V26ArrowColdVectors::open(&path, &manifest).unwrap();
        let selected = reader.read_rows(&requested).unwrap();
        assert_eq!(selected.vectors.len(), 512);
        assert_eq!(selected.assignments.len(), 512);
        assert_eq!(selected.batches_read, 512);
        assert_eq!(selected.read_workers, 4);

        if !cfg!(debug_assertions) {
            let mut latency_ns = Vec::with_capacity(128);
            for sample in 0..144 {
                let started = std::time::Instant::now();
                let selected = reader.read_rows(&requested).unwrap();
                let elapsed = started.elapsed().as_nanos();
                assert_eq!(selected.batches_read, 512);
                if sample >= 16 {
                    latency_ns.push(elapsed);
                }
            }
            latency_ns.sort_unstable();
            let p99_ns = latency_ns[(latency_ns.len() * 99).div_ceil(100) - 1];
            eprintln!("v26-arrow-512-sparse-batches-p99-ns={p99_ns}");
            assert!(p99_ns < 15_000_000);
        }
    }
}
