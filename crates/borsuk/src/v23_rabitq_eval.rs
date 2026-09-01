use std::{cmp::Ordering, collections::BinaryHeap, time::Instant};

use borsuk_fma::{FmaBackend, fused_dot_8x12};
use half::f16;
use serde::{Deserialize, Serialize};

use crate::{
    BorsukError, Result,
    v23_diagnostic::v23_reciprocal_rank_page_cover,
    v23_incidence_eval::V23IncidenceQueryTruth,
    v23_incidence_tree::{
        V23IncidenceTree, normalize_v23_incidence_vector, rank_v23_incidence_tree_beam,
    },
    v23_rabitq::{V23RaBitQObjectIdentity, project_v23_rabitq_serving_bytes},
    v23_rabitq_arrow::{V23RaBitQGeometry, V23RaBitQRowPlanes},
    v23_rabitq_quantizer::{
        V23RaBitQCode, V23RaBitQEstimate, V23RaBitQPreparedQuery, estimate_v23_rabitq_from_dot,
        prepare_v23_rabitq_query_with_validated_rotation, scalar_v23_rabitq_sign_dot,
        score_v23_rabitq_prepared_scalar, v23_rabitq_sign_dot_lut, validate_v23_rabitq_rotation,
    },
};

const MAX_SCORED_ROWS: usize = 262_144;
const MAX_RETAINED_ROWS: usize = 4_096;
const MAX_PAGE_ASSIGNMENTS: usize = 8_192;
const TARGET_INDEXED_ROWS: usize = 100_000_000;
const SELECTED_PAGES: usize = 8;
const INVERSE_SQRT_DIMENSIONS: f32 = 0.102_062_07;
const SCREEN_INPUT_ROLES: [&str; 9] = [
    "construction-receipt",
    "incidence-tree",
    "row-codes",
    "leaf-offsets",
    "centroids",
    "rotation",
    "f16-control",
    "d2-report",
    "query-parquet",
];

#[cfg(test)]
thread_local! {
    static V23_RABITQ_SELECT_CALLS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    static V23_RABITQ_SCALAR_SCORE_CALLS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct V23RaBitQQueryLimits {
    pub(crate) scored_rows: usize,
    pub(crate) retained_rows: usize,
    pub(crate) page_assignments: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct V23RaBitQLeafPrefix {
    pub(crate) requested_leaf_count: u16,
    pub(crate) leaf_ordinals: Vec<u16>,
    pub(crate) scored_rows: usize,
}

pub(crate) fn v23_rabitq_query_limits(indexed_rows: usize) -> Result<V23RaBitQQueryLimits> {
    if indexed_rows == 0 || indexed_rows > TARGET_INDEXED_ROWS {
        return Err(invalid("V23 RaBitQ indexed-row count differs"));
    }
    let scaled_ceiling = |limit: usize| {
        indexed_rows
            .checked_mul(limit)
            .and_then(|value| value.checked_add(TARGET_INDEXED_ROWS - 1))
            .map(|value| value / TARGET_INDEXED_ROWS)
            .ok_or_else(|| invalid("V23 RaBitQ query-limit projection overflows"))
    };
    let scored_rows = scaled_ceiling(MAX_SCORED_ROWS)?;
    let retained_rows = MAX_RETAINED_ROWS;
    let page_assignments = MAX_PAGE_ASSIGNMENTS;
    Ok(V23RaBitQQueryLimits {
        scored_rows,
        retained_rows,
        page_assignments,
    })
}

pub(crate) fn v23_rabitq_ranked_leaf_prefix(
    ranked_leaf_ordinals: &[u16],
    leaf_offsets: &[u64],
) -> Result<V23RaBitQLeafPrefix> {
    let indexed_rows = leaf_offsets
        .last()
        .copied()
        .and_then(|value| usize::try_from(value).ok())
        .ok_or_else(|| invalid("V23 RaBitQ leaf offsets are absent"))?;
    let limits = v23_rabitq_query_limits(indexed_rows)?;
    if ranked_leaf_ordinals.is_empty() || ranked_leaf_ordinals.len() > 128 {
        return Err(invalid("V23 RaBitQ ranked-leaf count differs"));
    }
    if leaf_offsets.first() != Some(&0) || leaf_offsets.windows(2).any(|pair| pair[0] > pair[1]) {
        return Err(invalid("V23 RaBitQ leaf offsets differ"));
    }
    let mut seen = Vec::with_capacity(ranked_leaf_ordinals.len());
    let mut selected = Vec::with_capacity(ranked_leaf_ordinals.len());
    let mut scored_rows = 0usize;
    for &leaf_ordinal in ranked_leaf_ordinals {
        let leaf = usize::from(leaf_ordinal);
        if leaf + 1 >= leaf_offsets.len() || seen.contains(&leaf_ordinal) {
            return Err(invalid("V23 RaBitQ ranked leaves differ"));
        }
        seen.push(leaf_ordinal);
        let start = usize::try_from(leaf_offsets[leaf])
            .map_err(|_| invalid("V23 RaBitQ leaf offset exceeds usize"))?;
        let end = usize::try_from(leaf_offsets[leaf + 1])
            .map_err(|_| invalid("V23 RaBitQ leaf offset exceeds usize"))?;
        let next = scored_rows
            .checked_add(end - start)
            .ok_or_else(|| invalid("V23 RaBitQ scored rows overflow"))?;
        if next > limits.scored_rows {
            break;
        }
        selected.push(leaf_ordinal);
        scored_rows = next;
    }
    if selected.is_empty() || scored_rows == 0 {
        return Err(invalid("V23 RaBitQ ranked-leaf prefix is empty"));
    }
    Ok(V23RaBitQLeafPrefix {
        requested_leaf_count: u16::try_from(ranked_leaf_ordinals.len())
            .map_err(|_| invalid("V23 RaBitQ ranked-leaf count exceeds u16"))?,
        leaf_ordinals: selected,
        scored_rows,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum V23RaBitQBackend {
    QueryLut,
    Aarch64Neon,
    X86Avx2Fma,
    ScalarControl,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct V23RaBitQEvalRequest<'a> {
    pub(crate) query_ordinal: u32,
    pub(crate) query: &'a [f32; 96],
    pub(crate) ranked_leaf_ordinals: &'a [u16],
    pub(crate) geometry: &'a V23RaBitQGeometry,
    pub(crate) rows: &'a V23RaBitQRowPlanes,
    pub(crate) backend: V23RaBitQBackend,
}

pub(crate) struct V23RaBitQDevelopmentRequest<'a> {
    pub(crate) source_commit: String,
    pub(crate) source_archive_sha256: String,
    pub(crate) index_id: String,
    pub(crate) inputs: &'a [V23RaBitQObjectIdentity],
    pub(crate) tree: &'a V23IncidenceTree,
    pub(crate) geometry: &'a V23RaBitQGeometry,
    pub(crate) rows: &'a V23RaBitQRowPlanes,
    pub(crate) exact_rows: &'a [[f16; 96]],
    pub(crate) queries: &'a [[f32; 96]],
    pub(crate) truth: &'a [V23IncidenceQueryTruth],
    pub(crate) backend: V23RaBitQBackend,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct V23RaBitQQueryEvidence {
    pub(crate) query_ordinal: u32,
    pub(crate) requested_leaf_count: u32,
    pub(crate) scanned_leaf_count: u32,
    pub(crate) scored_rows: u32,
    pub(crate) retained_rows: u16,
    pub(crate) page_assignments: u16,
    pub(crate) page_ordinals: Vec<u32>,
    pub(crate) max_estimator_error_ppm: u64,
    pub(crate) max_scalar_simd_error_ppm: Option<u32>,
    pub(crate) max_exact_fused_ulp: u8,
    pub(crate) scalar_pages_equal: Option<bool>,
    pub(crate) backend: V23RaBitQBackend,
    pub(crate) kernel_elapsed_ns: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum V23RaBitQControl {
    ExactExhaustive,
    ExactTree,
    RaBitQExhaustive,
    RaBitQTree,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum V23RaBitQClassification {
    AuthorityStop,
    TreePruningRejected,
    RaBitQEstimatorRejected,
    DevelopmentCandidateAccepted,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct V23RaBitQQuerySample {
    pub(crate) query_ordinal: u32,
    pub(crate) page_ordinals: Vec<u32>,
    pub(crate) hits: u16,
    pub(crate) oracle_hits: u16,
    pub(crate) recall_ppm: u32,
    pub(crate) requested_leaf_count: u32,
    pub(crate) scanned_leaf_count: u32,
    pub(crate) scored_rows: u32,
    pub(crate) retained_rows: u16,
    pub(crate) page_assignments: u16,
    pub(crate) max_estimator_error_ppm: u64,
    pub(crate) max_scalar_simd_error_ppm: u32,
    pub(crate) max_exact_fused_ulp: u8,
    pub(crate) kernel_elapsed_ns: u64,
    pub(crate) backend: V23RaBitQBackend,
    pub(crate) scalar_pages_equal: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct V23RaBitQCellResult {
    pub(crate) control: V23RaBitQControl,
    pub(crate) probe_count: u32,
    pub(crate) samples: Vec<V23RaBitQQuerySample>,
    pub(crate) total_hits: u16,
    pub(crate) total_oracle_hits: u16,
    pub(crate) aggregate_recall_ppm: u32,
    pub(crate) minimum_recall_ppm: u32,
    pub(crate) oracle_attainment_ppm: u32,
    pub(crate) passed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct V23RaBitQScreenResult {
    pub(crate) schema: String,
    pub(crate) source_commit: String,
    pub(crate) source_archive_sha256: String,
    pub(crate) index_id: String,
    pub(crate) indexed_rows: u64,
    pub(crate) leaf_count: u32,
    pub(crate) projected_serving_bytes: u64,
    pub(crate) inputs: Vec<V23RaBitQObjectIdentity>,
    pub(crate) development_truth: Vec<V23IncidenceQueryTruth>,
    pub(crate) cells: Vec<V23RaBitQCellResult>,
    pub(crate) classification: V23RaBitQClassification,
    pub(crate) claim_eligible: bool,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct V23RaBitQRankedRow {
    pub(crate) distance: f32,
    pub(crate) row_ordinal: u32,
    pub(crate) absolute_error_bound: f32,
}

struct V23RaBitQRankOutcome {
    ranked: Vec<V23RaBitQRankedRow>,
    maximum_differential_error_ppm: u32,
    scored_rows: usize,
    scanned_leaf_count: usize,
}

#[derive(Clone, Copy)]
struct V23RaBitQRankOptions {
    backend: V23RaBitQBackend,
    differential_backend: Option<V23RaBitQBackend>,
    maximum_leaves: usize,
    maximum_rows: usize,
}

impl PartialEq for V23RaBitQRankedRow {
    fn eq(&self, other: &Self) -> bool {
        self.distance.to_bits() == other.distance.to_bits() && self.row_ordinal == other.row_ordinal
    }
}

impl Eq for V23RaBitQRankedRow {}

impl PartialOrd for V23RaBitQRankedRow {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for V23RaBitQRankedRow {
    fn cmp(&self, other: &Self) -> Ordering {
        self.distance
            .total_cmp(&other.distance)
            .then_with(|| self.row_ordinal.cmp(&other.row_ordinal))
    }
}

fn invalid(message: &str) -> BorsukError {
    BorsukError::InvalidStorage(message.to_string())
}

fn valid_lower_hex(value: &str, width: usize) -> bool {
    value.len() == width
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn expected_cell_shape(ordinal: usize, leaf_count: u32) -> (V23RaBitQControl, u32) {
    match ordinal {
        0 => (V23RaBitQControl::ExactExhaustive, leaf_count),
        1 => (V23RaBitQControl::ExactTree, 32),
        2 => (V23RaBitQControl::ExactTree, 64),
        3 => (V23RaBitQControl::ExactTree, 128),
        4 => (V23RaBitQControl::RaBitQExhaustive, leaf_count),
        5 => (V23RaBitQControl::RaBitQTree, 32),
        6 => (V23RaBitQControl::RaBitQTree, 64),
        _ => (V23RaBitQControl::RaBitQTree, 128),
    }
}

fn validate_cell_shape(cells: &[V23RaBitQCellResult], leaf_count: u32) -> Result<()> {
    if leaf_count == 0
        || leaf_count > 65_536
        || cells.len() != 8
        || cells.iter().enumerate().any(|(ordinal, cell)| {
            let expected = expected_cell_shape(ordinal, leaf_count);
            (cell.control, cell.probe_count) != expected
        })
    {
        return Err(invalid("V23 RaBitQ screen cell shape differs"));
    }
    Ok(())
}

pub(crate) fn classify_v23_rabitq_controls(
    cells: &[V23RaBitQCellResult],
    leaf_count: u32,
) -> Result<V23RaBitQClassification> {
    validate_cell_shape(cells, leaf_count)?;
    if cells[0].total_hits != 318 || cells[0].total_oracle_hits != 318 || !cells[0].passed {
        return Ok(V23RaBitQClassification::AuthorityStop);
    }
    if !cells[1..4].iter().any(|cell| cell.passed) {
        return Ok(V23RaBitQClassification::TreePruningRejected);
    }
    if (0..3).any(|ordinal| cells[1 + ordinal].passed && cells[5 + ordinal].passed) {
        return Ok(V23RaBitQClassification::DevelopmentCandidateAccepted);
    }
    Ok(V23RaBitQClassification::RaBitQEstimatorRejected)
}

fn validate_cell(
    cell: &V23RaBitQCellResult,
    indexed_rows: usize,
    leaf_count: u32,
    truth: &[V23IncidenceQueryTruth],
) -> Result<()> {
    if cell.samples.len() != 32 || truth.len() != 32 {
        return Err(invalid("V23 RaBitQ query sample count differs"));
    }
    let limits = v23_rabitq_query_limits(indexed_rows)?;
    let mut total_hits = 0u16;
    let mut total_oracle_hits = 0u16;
    let mut minimum_recall_ppm = u32::MAX;
    for (ordinal, (sample, query_truth)) in cell.samples.iter().zip(truth).enumerate() {
        let hits = query_truth
            .ground_truth_page_assignments
            .iter()
            .filter(|assignments| {
                assignments
                    .iter()
                    .any(|page| sample.page_ordinals.binary_search(page).is_ok())
            })
            .count();
        let oracle_hits = query_truth
            .ground_truth_page_assignments
            .iter()
            .filter(|assignments| {
                assignments
                    .iter()
                    .any(|page| query_truth.oracle_pages.binary_search(page).is_ok())
            })
            .count();
        if sample.query_ordinal != ordinal as u32
            || query_truth.query_ordinal != ordinal as u32
            || query_truth.ground_truth_page_assignments.len() != 10
            || query_truth.oracle_pages.is_empty()
            || query_truth.oracle_pages.len() > SELECTED_PAGES
            || query_truth
                .oracle_pages
                .windows(2)
                .any(|pair| pair[0] >= pair[1])
            || query_truth
                .ground_truth_page_assignments
                .iter()
                .any(|assignments| {
                    assignments.is_empty()
                        || assignments.len() > 2
                        || assignments.windows(2).any(|pair| pair[0] >= pair[1])
                })
            || query_truth
                .ground_truth_page_assignments
                .iter()
                .flatten()
                .chain(&query_truth.oracle_pages)
                .any(|page| *page >= 28_282)
            || sample.page_ordinals.is_empty()
            || sample.page_ordinals.len() > SELECTED_PAGES
            || sample
                .page_ordinals
                .windows(2)
                .any(|pair| pair[0] >= pair[1])
            || sample.page_ordinals.iter().any(|page| *page >= 28_282)
            || usize::from(sample.hits) != hits
            || usize::from(sample.oracle_hits) != oracle_hits
            || sample.hits > sample.oracle_hits
            || sample.recall_ppm != u32::from(sample.hits) * 100_000
            || sample.requested_leaf_count != cell.probe_count
            || sample.scanned_leaf_count == 0
            || sample.scanned_leaf_count > sample.requested_leaf_count
            || (matches!(
                cell.control,
                V23RaBitQControl::ExactExhaustive | V23RaBitQControl::RaBitQExhaustive
            ) && (sample.requested_leaf_count != leaf_count
                || sample.scanned_leaf_count != leaf_count
                || sample.scored_rows != indexed_rows as u32))
            || sample.scored_rows == 0
            || (matches!(
                cell.control,
                V23RaBitQControl::ExactTree | V23RaBitQControl::RaBitQTree
            ) && sample.scored_rows > limits.scored_rows as u32)
            || sample.retained_rows == 0
            || usize::from(sample.retained_rows)
                != usize::try_from(sample.scored_rows)
                    .unwrap()
                    .min(limits.retained_rows)
            || usize::from(sample.page_assignments) < usize::from(sample.retained_rows)
            || usize::from(sample.page_assignments) > usize::from(sample.retained_rows) * 2
            || usize::from(sample.retained_rows) > limits.retained_rows
            || usize::from(sample.page_assignments) > limits.page_assignments
            || (matches!(
                cell.control,
                V23RaBitQControl::RaBitQExhaustive | V23RaBitQControl::RaBitQTree
            ) && (sample.backend != V23RaBitQBackend::QueryLut
                || sample.max_scalar_simd_error_ppm > 1
                || sample.max_exact_fused_ulp != 0))
            || (matches!(
                cell.control,
                V23RaBitQControl::ExactExhaustive | V23RaBitQControl::ExactTree
            ) && (!matches!(
                sample.backend,
                V23RaBitQBackend::Aarch64Neon | V23RaBitQBackend::X86Avx2Fma
            ) || sample.max_scalar_simd_error_ppm != 0
                || sample.max_exact_fused_ulp > 8
                || sample.max_estimator_error_ppm != 0))
            || !sample.scalar_pages_equal
            || sample.kernel_elapsed_ns == 0
        {
            return Err(invalid("V23 RaBitQ query sample evidence differs"));
        }
        total_hits = total_hits
            .checked_add(sample.hits)
            .ok_or_else(|| invalid("V23 RaBitQ hit count overflows"))?;
        total_oracle_hits = total_oracle_hits
            .checked_add(sample.oracle_hits)
            .ok_or_else(|| invalid("V23 RaBitQ oracle hit count overflows"))?;
        minimum_recall_ppm = minimum_recall_ppm.min(sample.recall_ppm);
    }
    let aggregate_recall_ppm = u32::from(total_hits) * 1_000_000 / 320;
    let oracle_attainment_ppm = if total_oracle_hits == 0 {
        0
    } else {
        u32::from(total_hits) * 1_000_000 / u32::from(total_oracle_hits)
    };
    let passed = total_hits == 318
        && aggregate_recall_ppm == 993_750
        && minimum_recall_ppm >= 900_000
        && oracle_attainment_ppm == 1_000_000;
    if cell.total_hits != total_hits
        || cell.total_oracle_hits != total_oracle_hits
        || cell.aggregate_recall_ppm != aggregate_recall_ppm
        || cell.minimum_recall_ppm != minimum_recall_ppm
        || cell.oracle_attainment_ppm != oracle_attainment_ppm
        || cell.passed != passed
    {
        return Err(invalid("V23 RaBitQ cell aggregate differs"));
    }
    Ok(())
}

pub(crate) fn canonical_v23_rabitq_screen_result_bytes(
    result: &V23RaBitQScreenResult,
    expected_inputs: &[V23RaBitQObjectIdentity],
    expected_leaf_count: u32,
) -> Result<Vec<u8>> {
    if result.schema != "borsuk-v23-rabitq-screen-v3"
        || !valid_lower_hex(&result.source_commit, 40)
        || !valid_lower_hex(&result.source_archive_sha256, 64)
        || result.index_id.is_empty()
        || result.leaf_count != expected_leaf_count
        || result.projected_serving_bytes
            != project_v23_rabitq_serving_bytes(result.indexed_rows)?.total_bytes
        || result.inputs != expected_inputs
        || result.inputs.len() != SCREEN_INPUT_ROLES.len()
        || result.claim_eligible
    {
        return Err(invalid("V23 RaBitQ screen authority differs"));
    }
    let mut roles = std::collections::BTreeSet::new();
    let mut uris = std::collections::BTreeSet::new();
    for (identity, expected_role) in result.inputs.iter().zip(SCREEN_INPUT_ROLES) {
        if identity.role.is_empty()
            || identity.role != expected_role
            || !roles.insert(identity.role.as_str())
            || !identity.uri.starts_with("s3://")
            || !uris.insert(identity.uri.as_str())
            || !valid_lower_hex(&identity.sha256, 64)
            || identity.blake3.is_some()
            || identity.encoded_bytes == 0
        {
            return Err(invalid("V23 RaBitQ screen input identity differs"));
        }
    }
    validate_cell_shape(&result.cells, result.leaf_count)?;
    let indexed_rows = usize::try_from(result.indexed_rows)
        .map_err(|_| invalid("V23 RaBitQ indexed rows exceed usize"))?;
    result.cells.iter().try_for_each(|cell| {
        validate_cell(
            cell,
            indexed_rows,
            result.leaf_count,
            &result.development_truth,
        )
    })?;
    if result.classification != classify_v23_rabitq_controls(&result.cells, result.leaf_count)? {
        return Err(invalid("V23 RaBitQ screen classification differs"));
    }
    let value = serde_json::to_value(result)
        .map_err(|error| invalid(&format!("V23 RaBitQ screen JSON failed: {error}")))?;
    let value = crate::v23_incidence::canonical_json_value(value);
    let mut bytes = serde_json::to_vec(&value)
        .map_err(|error| invalid(&format!("V23 RaBitQ screen JSON failed: {error}")))?;
    bytes.push(b'\n');
    Ok(bytes)
}

fn map_backend(value: FmaBackend) -> V23RaBitQBackend {
    match value {
        FmaBackend::Aarch64NeonFma => V23RaBitQBackend::Aarch64Neon,
        FmaBackend::X86AvxFma => V23RaBitQBackend::X86Avx2Fma,
    }
}

fn detected_v23_rabitq_exact_backend() -> Result<V23RaBitQBackend> {
    let (_, backend) = fused_dot_8x12(&[0.0; 96], &[0.0; 96])
        .map_err(|_| invalid("V23 RaBitQ fused backend is unavailable"))?;
    let backend = map_backend(backend);
    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    if backend == V23RaBitQBackend::X86Avx2Fma && !std::arch::is_x86_feature_detected!("avx2") {
        return Err(invalid("V23 RaBitQ AVX2 backend is unavailable"));
    }
    Ok(backend)
}

pub(crate) fn detected_v23_rabitq_backend() -> Result<V23RaBitQBackend> {
    Ok(V23RaBitQBackend::QueryLut)
}

fn sign_vector(code: &V23RaBitQCode) -> [f32; 96] {
    std::array::from_fn(|ordinal| {
        if code.sign_code[ordinal / 8] & (1 << (ordinal % 8)) == 0 {
            -INVERSE_SQRT_DIMENSIONS
        } else {
            INVERSE_SQRT_DIMENSIONS
        }
    })
}

pub(crate) fn score_v23_rabitq_code(
    prepared: &V23RaBitQPreparedQuery,
    code: &V23RaBitQCode,
    backend: V23RaBitQBackend,
) -> Result<V23RaBitQEstimate> {
    Ok(score_v23_rabitq_code_with_dot(prepared, code, backend)?.0)
}

fn score_v23_rabitq_code_with_dot(
    prepared: &V23RaBitQPreparedQuery,
    code: &V23RaBitQCode,
    backend: V23RaBitQBackend,
) -> Result<(V23RaBitQEstimate, f32)> {
    if backend == V23RaBitQBackend::ScalarControl {
        #[cfg(test)]
        V23_RABITQ_SCALAR_SCORE_CALLS.with(|calls| calls.set(calls.get() + 1));
        let dot = scalar_v23_rabitq_sign_dot(prepared, code);
        return Ok((score_v23_rabitq_prepared_scalar(prepared, code)?, dot));
    }
    if backend == V23RaBitQBackend::QueryLut {
        let dot = v23_rabitq_sign_dot_lut(prepared, code);
        return Ok((estimate_v23_rabitq_from_dot(prepared, code, dot)?, dot));
    }
    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    if backend == V23RaBitQBackend::X86Avx2Fma && !std::arch::is_x86_feature_detected!("avx2") {
        return Err(invalid("V23 RaBitQ AVX2 backend is unavailable"));
    }
    let (dot, actual) = fused_dot_8x12(&sign_vector(code), &prepared.reconstructed)
        .map_err(|_| invalid("V23 RaBitQ fused backend is unavailable"))?;
    if map_backend(actual) != backend {
        return Err(invalid("V23 RaBitQ fused backend authority differs"));
    }
    Ok((estimate_v23_rabitq_from_dot(prepared, code, dot)?, dot))
}

fn float_ulp_distance(left: f32, right: f32) -> u32 {
    fn ordered(value: f32) -> i32 {
        let bits = value.to_bits() as i32;
        if bits < 0 { i32::MIN - bits } else { bits }
    }
    ordered(left).abs_diff(ordered(right))
}

fn code_at(rows: &V23RaBitQRowPlanes, ordinal: usize) -> Result<V23RaBitQCode> {
    Ok(V23RaBitQCode {
        sign_code: *rows
            .sign_codes
            .get(ordinal)
            .ok_or_else(|| invalid("V23 RaBitQ sign code is absent"))?,
        residual_norm: *rows
            .residual_norms
            .get(ordinal)
            .ok_or_else(|| invalid("V23 RaBitQ residual norm is absent"))?,
        alignment: *rows
            .alignments
            .get(ordinal)
            .ok_or_else(|| invalid("V23 RaBitQ alignment is absent"))?,
    })
}

fn validate_shapes(
    ranked_leaf_ordinals: &[u16],
    geometry: &V23RaBitQGeometry,
    rows: &V23RaBitQRowPlanes,
    maximum_leaves: usize,
    maximum_rows: usize,
) -> Result<(Vec<u16>, usize)> {
    let row_count = rows.sign_codes.len();
    if ranked_leaf_ordinals.is_empty()
        || ranked_leaf_ordinals.len() > maximum_leaves
        || row_count == 0
        || rows.residual_norms.len() != row_count
        || rows.alignments.len() != row_count
        || rows.primary_pages.len() != row_count
        || rows.replica_pages.len() != row_count
        || geometry.leaf_offsets.len() != geometry.centroids.len() + 1
        || geometry.leaf_offsets.first() != Some(&0)
        || geometry.leaf_offsets.last().copied() != Some(row_count as u64)
        || geometry
            .leaf_offsets
            .windows(2)
            .any(|pair| pair[0] > pair[1])
    {
        return Err(invalid("V23 RaBitQ evaluation shape differs"));
    }
    validate_v23_rabitq_rotation(&geometry.rotation)?;
    let leaves = ranked_leaf_ordinals.to_vec();
    let mut uniqueness = leaves.clone();
    uniqueness.sort_unstable();
    if uniqueness.windows(2).any(|pair| pair[0] == pair[1])
        || uniqueness
            .last()
            .is_some_and(|leaf| usize::from(*leaf) >= geometry.centroids.len())
    {
        return Err(invalid("V23 RaBitQ ranked leaves differ"));
    }
    let mut scored_rows = 0usize;
    let mut selected = Vec::with_capacity(leaves.len());
    for &leaf_ordinal in &leaves {
        let leaf = usize::from(leaf_ordinal);
        let start = usize::try_from(geometry.leaf_offsets[leaf])
            .map_err(|_| invalid("V23 RaBitQ leaf offset exceeds usize"))?;
        let end = usize::try_from(geometry.leaf_offsets[leaf + 1])
            .map_err(|_| invalid("V23 RaBitQ leaf offset exceeds usize"))?;
        let next = scored_rows
            .checked_add(end - start)
            .ok_or_else(|| invalid("V23 RaBitQ scored rows overflow"))?;
        if next > maximum_rows {
            break;
        }
        scored_rows = next;
        selected.push(leaf_ordinal);
    }
    if scored_rows == 0 {
        return Err(invalid("V23 RaBitQ scored-row cap differs"));
    }
    Ok((selected, scored_rows))
}

fn rank_rows_internal(
    query: &[f32; 96],
    ranked_leaf_ordinals: &[u16],
    geometry: &V23RaBitQGeometry,
    rows: &V23RaBitQRowPlanes,
    options: V23RaBitQRankOptions,
) -> Result<V23RaBitQRankOutcome> {
    if query.iter().any(|value| !value.is_finite()) {
        return Err(invalid("V23 RaBitQ query is nonfinite"));
    }
    let (leaves, scored_rows) = validate_shapes(
        ranked_leaf_ordinals,
        geometry,
        rows,
        options.maximum_leaves,
        options.maximum_rows,
    )?;
    let limits = v23_rabitq_query_limits(rows.sign_codes.len())?;
    let normalized_query = normalize_v23_incidence_vector(query)?;
    let mut heap = BinaryHeap::with_capacity(limits.retained_rows + 1);
    let mut maximum_error_ppm = 0u32;
    let scanned_leaf_count = leaves.len();
    for leaf in leaves {
        let leaf = usize::from(leaf);
        let centroid = geometry.centroids[leaf].map(f16::to_f32);
        if centroid.iter().any(|value| !value.is_finite()) {
            return Err(invalid("V23 RaBitQ centroid is nonfinite"));
        }
        let query_residual =
            std::array::from_fn(|dimension| normalized_query[dimension] - centroid[dimension]);
        let prepared =
            prepare_v23_rabitq_query_with_validated_rotation(&query_residual, &geometry.rotation)?;
        let start = usize::try_from(geometry.leaf_offsets[leaf]).unwrap();
        let end = usize::try_from(geometry.leaf_offsets[leaf + 1]).unwrap();
        for row_ordinal in start..end {
            let code = code_at(rows, row_ordinal)?;
            let (estimate, primary_dot) =
                score_v23_rabitq_code_with_dot(&prepared, &code, options.backend)?;
            if let Some(differential_backend) = options.differential_backend {
                let (_, differential_dot) =
                    score_v23_rabitq_code_with_dot(&prepared, &code, differential_backend)?;
                let scale = INVERSE_SQRT_DIMENSIONS
                    * prepared
                        .reconstructed
                        .iter()
                        .map(|value| value.abs())
                        .sum::<f32>();
                let absolute_error = (primary_dot - differential_dot).abs();
                let permitted_error = 8.0 * f32::EPSILON * scale.max(f32::MIN_POSITIVE);
                if absolute_error > permitted_error {
                    return Err(invalid(&format!(
                        "V23 RaBitQ scalar/SIMD dot differs at row {row_ordinal}"
                    )));
                }
                let error_ppm = (absolute_error / scale.max(f32::MIN_POSITIVE) * 1_000_000.0)
                    .ceil()
                    .min(u32::MAX as f32) as u32;
                maximum_error_ppm = maximum_error_ppm.max(error_ppm);
            }
            let candidate = V23RaBitQRankedRow {
                distance: estimate.distance_squared,
                row_ordinal: u32::try_from(row_ordinal)
                    .map_err(|_| invalid("V23 RaBitQ row ordinal exceeds u32"))?,
                absolute_error_bound: estimate.absolute_error_bound,
            };
            if heap.len() < limits.retained_rows {
                heap.push(candidate);
            } else if heap.peek().is_some_and(|worst| candidate < *worst) {
                heap.pop();
                heap.push(candidate);
            }
        }
    }
    let mut ranked = heap.into_vec();
    ranked.sort_unstable();
    Ok(V23RaBitQRankOutcome {
        ranked,
        maximum_differential_error_ppm: maximum_error_ppm,
        scored_rows,
        scanned_leaf_count,
    })
}

pub(crate) fn rank_v23_rabitq_rows(
    query: &[f32; 96],
    ranked_leaf_ordinals: &[u16],
    geometry: &V23RaBitQGeometry,
    rows: &V23RaBitQRowPlanes,
    backend: V23RaBitQBackend,
) -> Result<Vec<V23RaBitQRankedRow>> {
    let limits = v23_rabitq_query_limits(rows.sign_codes.len())?;
    Ok(rank_rows_internal(
        query,
        ranked_leaf_ordinals,
        geometry,
        rows,
        V23RaBitQRankOptions {
            backend,
            differential_backend: None,
            maximum_leaves: 128,
            maximum_rows: limits.scored_rows,
        },
    )?
    .ranked)
}

fn ranked_page_assignments(
    ranked: &[V23RaBitQRankedRow],
    rows: &V23RaBitQRowPlanes,
) -> Result<Vec<(u32, Option<u32>)>> {
    let limits = v23_rabitq_query_limits(rows.sign_codes.len())?;
    if ranked.len() > limits.retained_rows {
        return Err(invalid("V23 RaBitQ retained-row cap differs"));
    }
    let mut assignments = Vec::with_capacity(ranked.len());
    let mut assignment_count = 0usize;
    for candidate in ranked {
        let row = usize::try_from(candidate.row_ordinal).unwrap();
        let primary = *rows
            .primary_pages
            .get(row)
            .ok_or_else(|| invalid("V23 RaBitQ primary page is absent"))?;
        let replica = *rows
            .replica_pages
            .get(row)
            .ok_or_else(|| invalid("V23 RaBitQ replica page is absent"))?;
        if primary == u32::MAX || replica == primary {
            return Err(invalid("V23 RaBitQ page assignment differs"));
        }
        let replica = (replica != u32::MAX).then_some(replica);
        assignment_count += 1 + usize::from(replica.is_some());
        assignments.push((primary, replica));
    }
    if assignment_count > limits.page_assignments {
        return Err(invalid("V23 RaBitQ page-assignment cap differs"));
    }
    Ok(assignments)
}

pub(crate) fn select_v23_rabitq_pages(
    request: V23RaBitQEvalRequest<'_>,
) -> Result<V23RaBitQQueryEvidence> {
    if request.backend != V23RaBitQBackend::QueryLut {
        return Err(invalid("V23 RaBitQ production backend is not query-lut"));
    }
    #[cfg(test)]
    V23_RABITQ_SELECT_CALLS.with(|calls| calls.set(calls.get() + 1));
    let limits = v23_rabitq_query_limits(request.rows.sign_codes.len())?;
    let started = Instant::now();
    let production = rank_rows_internal(
        request.query,
        request.ranked_leaf_ordinals,
        request.geometry,
        request.rows,
        V23RaBitQRankOptions {
            backend: request.backend,
            differential_backend: None,
            maximum_leaves: 128,
            maximum_rows: limits.scored_rows,
        },
    )?;
    let kernel_elapsed_ns = u64::try_from(started.elapsed().as_nanos())
        .unwrap_or(u64::MAX)
        .max(1);
    let assignments = ranked_page_assignments(&production.ranked, request.rows)?;
    let pages = v23_reciprocal_rank_page_cover(&assignments, SELECTED_PAGES)?;
    if pages.is_empty() || pages.len() > SELECTED_PAGES {
        return Err(invalid("V23 RaBitQ selected-page width differs"));
    }

    let assignment_count = assignments
        .iter()
        .map(|(_, replica)| 1 + usize::from(replica.is_some()))
        .sum::<usize>();
    let max_estimator_error_ppm = production
        .ranked
        .iter()
        .map(|row| {
            let denominator = f64::from(row.distance.abs()).max(f64::from(f32::MIN_POSITIVE));
            (f64::from(row.absolute_error_bound) / denominator * 1_000_000.0)
                .ceil()
                .min(u64::MAX as f64) as u64
        })
        .max()
        .unwrap_or(0);
    Ok(V23RaBitQQueryEvidence {
        query_ordinal: request.query_ordinal,
        requested_leaf_count: u32::try_from(request.ranked_leaf_ordinals.len()).unwrap(),
        scanned_leaf_count: u32::try_from(production.scanned_leaf_count).unwrap(),
        scored_rows: u32::try_from(production.scored_rows).unwrap(),
        retained_rows: u16::try_from(production.ranked.len()).unwrap(),
        page_assignments: u16::try_from(assignment_count).unwrap(),
        page_ordinals: pages,
        max_estimator_error_ppm,
        max_scalar_simd_error_ppm: None,
        max_exact_fused_ulp: 0,
        scalar_pages_equal: None,
        backend: request.backend,
        kernel_elapsed_ns,
    })
}

fn scalar_dot_8x12(left: &[f32; 96], right: &[f32; 96]) -> f32 {
    let mut lanes = [0.0f32; 8];
    for (lane, accumulator) in lanes.iter_mut().enumerate() {
        for step in 0..12 {
            let ordinal = lane * 12 + step;
            *accumulator = left[ordinal].mul_add(right[ordinal], *accumulator);
        }
    }
    lanes.into_iter().sum()
}

fn rank_exact_rows(
    query: &[f32; 96],
    leaves: &[u16],
    geometry: &V23RaBitQGeometry,
    exact_rows: &[[f16; 96]],
    backend: V23RaBitQBackend,
    use_fused: bool,
) -> Result<(Vec<V23RaBitQRankedRow>, u8, usize)> {
    if backend == V23RaBitQBackend::ScalarControl
        || exact_rows.len() != geometry.leaf_offsets.last().copied().unwrap_or(0) as usize
    {
        return Err(invalid("V23 RaBitQ exact control authority differs"));
    }
    let limits = v23_rabitq_query_limits(exact_rows.len())?;
    let expected_fused_backend = use_fused
        .then(detected_v23_rabitq_exact_backend)
        .transpose()?;
    let normalized = normalize_v23_incidence_vector(query)?;
    let mut heap = BinaryHeap::with_capacity(limits.retained_rows + 1);
    let mut maximum_ulp = 0u32;
    let mut scored_rows = 0usize;
    for &leaf in leaves {
        let leaf = usize::from(leaf);
        if leaf + 1 >= geometry.leaf_offsets.len() {
            return Err(invalid("V23 RaBitQ exact control leaf differs"));
        }
        let start = usize::try_from(geometry.leaf_offsets[leaf])
            .map_err(|_| invalid("V23 RaBitQ exact offset exceeds usize"))?;
        let end = usize::try_from(geometry.leaf_offsets[leaf + 1])
            .map_err(|_| invalid("V23 RaBitQ exact offset exceeds usize"))?;
        scored_rows = scored_rows
            .checked_add(end - start)
            .ok_or_else(|| invalid("V23 RaBitQ exact scanned rows overflow"))?;
        for (row_ordinal, exact_row) in exact_rows.iter().enumerate().take(end).skip(start) {
            let row = exact_row.map(f16::to_f32);
            if row.iter().any(|value| !value.is_finite()) {
                return Err(invalid("V23 RaBitQ exact row is nonfinite"));
            }
            let difference =
                std::array::from_fn(|dimension| normalized[dimension] - row[dimension]);
            let scalar = scalar_dot_8x12(&difference, &difference);
            let distance = if use_fused {
                let (distance, actual) = fused_dot_8x12(&difference, &difference)
                    .map_err(|_| invalid("V23 RaBitQ exact fused backend is unavailable"))?;
                if Some(map_backend(actual)) != expected_fused_backend {
                    return Err(invalid("V23 RaBitQ exact fused backend authority differs"));
                }
                distance
            } else {
                scalar
            };
            let ulp = float_ulp_distance(distance, scalar);
            if ulp > 8 || !distance.is_finite() {
                return Err(invalid("V23 RaBitQ exact scalar/SIMD distance differs"));
            }
            maximum_ulp = maximum_ulp.max(ulp);
            let candidate = V23RaBitQRankedRow {
                distance,
                row_ordinal: u32::try_from(row_ordinal)
                    .map_err(|_| invalid("V23 RaBitQ exact row ordinal exceeds u32"))?,
                absolute_error_bound: 0.0,
            };
            if heap.len() < limits.retained_rows {
                heap.push(candidate);
            } else if heap.peek().is_some_and(|worst| candidate < *worst) {
                heap.pop();
                heap.push(candidate);
            }
        }
    }
    let mut ranked = heap.into_vec();
    ranked.sort_unstable();
    Ok((
        ranked,
        u8::try_from(maximum_ulp).unwrap_or(u8::MAX),
        scored_rows,
    ))
}

fn pages_from_ranked(
    ranked: &[V23RaBitQRankedRow],
    rows: &V23RaBitQRowPlanes,
) -> Result<(Vec<u32>, usize)> {
    let assignments = ranked_page_assignments(ranked, rows)?;
    let count = assignments
        .iter()
        .map(|(_, replica)| 1 + usize::from(replica.is_some()))
        .sum();
    let pages = v23_reciprocal_rank_page_cover(&assignments, SELECTED_PAGES)?;
    if pages.is_empty() || pages.len() > SELECTED_PAGES {
        return Err(invalid("V23 RaBitQ control page width differs"));
    }
    Ok((pages, count))
}

fn sample_from_pages(
    query_ordinal: u32,
    pages: &[u32],
    truth: &V23IncidenceQueryTruth,
    evidence: &V23RaBitQQueryEvidence,
) -> Result<V23RaBitQQuerySample> {
    if truth.query_ordinal != query_ordinal
        || truth.ground_truth_page_assignments.len() != 10
        || truth.oracle_pages.is_empty()
        || truth.oracle_pages.len() > SELECTED_PAGES
        || truth.oracle_pages.windows(2).any(|pair| pair[0] >= pair[1])
        || truth
            .ground_truth_page_assignments
            .iter()
            .any(|assignments| {
                assignments.is_empty()
                    || assignments.len() > 2
                    || assignments.windows(2).any(|pair| pair[0] >= pair[1])
            })
    {
        return Err(invalid("V23 RaBitQ development truth differs"));
    }
    let hits = truth
        .ground_truth_page_assignments
        .iter()
        .filter(|assignments| {
            assignments
                .iter()
                .any(|page| pages.binary_search(page).is_ok())
        })
        .count();
    let oracle_hits = truth
        .ground_truth_page_assignments
        .iter()
        .filter(|assignments| {
            assignments
                .iter()
                .any(|page| truth.oracle_pages.binary_search(page).is_ok())
        })
        .count();
    Ok(V23RaBitQQuerySample {
        query_ordinal,
        page_ordinals: pages.to_vec(),
        hits: u16::try_from(hits).unwrap(),
        oracle_hits: u16::try_from(oracle_hits).unwrap(),
        recall_ppm: u32::try_from(hits).unwrap() * 100_000,
        requested_leaf_count: evidence.requested_leaf_count,
        scanned_leaf_count: evidence.scanned_leaf_count,
        scored_rows: evidence.scored_rows,
        retained_rows: evidence.retained_rows,
        page_assignments: evidence.page_assignments,
        max_estimator_error_ppm: evidence.max_estimator_error_ppm,
        max_scalar_simd_error_ppm: evidence
            .max_scalar_simd_error_ppm
            .ok_or_else(|| invalid("V23 RaBitQ scalar differential evidence is absent"))?,
        max_exact_fused_ulp: evidence.max_exact_fused_ulp,
        kernel_elapsed_ns: evidence.kernel_elapsed_ns,
        backend: evidence.backend,
        scalar_pages_equal: evidence
            .scalar_pages_equal
            .ok_or_else(|| invalid("V23 RaBitQ scalar page evidence is absent"))?,
    })
}

fn evaluate_rabitq_tree_sample(
    query_ordinal: u32,
    query: &[f32; 96],
    truth: &V23IncidenceQueryTruth,
    ranked_leaf_ordinals: &[u16],
    geometry: &V23RaBitQGeometry,
    rows: &V23RaBitQRowPlanes,
    backend: V23RaBitQBackend,
) -> Result<V23RaBitQQuerySample> {
    let mut evidence = select_v23_rabitq_pages(V23RaBitQEvalRequest {
        query_ordinal,
        query,
        ranked_leaf_ordinals,
        geometry,
        rows,
        backend,
    })?;
    let limits = v23_rabitq_query_limits(rows.sign_codes.len())?;
    let scalar = rank_rows_internal(
        query,
        ranked_leaf_ordinals,
        geometry,
        rows,
        V23RaBitQRankOptions {
            backend: V23RaBitQBackend::ScalarControl,
            differential_backend: Some(V23RaBitQBackend::QueryLut),
            maximum_leaves: 128,
            maximum_rows: limits.scored_rows,
        },
    )?;
    let scalar_pages = pages_from_ranked(&scalar.ranked, rows)?.0;
    if scalar_pages.as_slice() != evidence.page_ordinals.as_slice() {
        return Err(invalid("V23 RaBitQ scalar/SIMD selected pages differ"));
    }
    evidence.max_scalar_simd_error_ppm = Some(scalar.maximum_differential_error_ppm);
    evidence.scalar_pages_equal = Some(true);
    sample_from_pages(query_ordinal, &evidence.page_ordinals, truth, &evidence)
}

fn cell_from_samples(
    control: V23RaBitQControl,
    probe_count: u32,
    samples: Vec<V23RaBitQQuerySample>,
) -> Result<V23RaBitQCellResult> {
    let total_hits = samples
        .iter()
        .map(|sample| u32::from(sample.hits))
        .sum::<u32>();
    let total_oracle_hits = samples
        .iter()
        .map(|sample| u32::from(sample.oracle_hits))
        .sum::<u32>();
    let aggregate_recall_ppm = total_hits * 1_000_000 / 320;
    let minimum_recall_ppm = samples
        .iter()
        .map(|sample| sample.recall_ppm)
        .min()
        .unwrap_or(0);
    let oracle_attainment_ppm = total_hits
        .checked_mul(1_000_000)
        .and_then(|value| value.checked_div(total_oracle_hits))
        .unwrap_or(0);
    let passed = total_hits == 318
        && aggregate_recall_ppm == 993_750
        && minimum_recall_ppm >= 900_000
        && oracle_attainment_ppm == 1_000_000;
    Ok(V23RaBitQCellResult {
        control,
        probe_count,
        samples,
        total_hits: u16::try_from(total_hits).unwrap(),
        total_oracle_hits: u16::try_from(total_oracle_hits).unwrap(),
        aggregate_recall_ppm,
        minimum_recall_ppm,
        oracle_attainment_ppm,
        passed,
    })
}

fn evaluate_control(
    request: &V23RaBitQDevelopmentRequest<'_>,
    control: V23RaBitQControl,
    probe_count: u32,
    all_leaves: &[u16],
) -> Result<V23RaBitQCellResult> {
    let mut samples = Vec::with_capacity(32);
    for (ordinal, (query, truth)) in request.queries.iter().zip(request.truth).enumerate() {
        let leaves = match control {
            V23RaBitQControl::ExactExhaustive | V23RaBitQControl::RaBitQExhaustive => {
                all_leaves.to_vec()
            }
            V23RaBitQControl::ExactTree | V23RaBitQControl::RaBitQTree => {
                rank_v23_incidence_tree_beam(request.tree, query, probe_count as usize)?
            }
        };
        if control == V23RaBitQControl::RaBitQTree {
            samples.push(evaluate_rabitq_tree_sample(
                ordinal as u32,
                query,
                truth,
                &leaves,
                request.geometry,
                request.rows,
                request.backend,
            )?);
            continue;
        }
        let evidence = match control {
            V23RaBitQControl::ExactExhaustive | V23RaBitQControl::ExactTree => {
                let requested_leaf_count = leaves.len();
                let scanned_leaves = if control == V23RaBitQControl::ExactTree {
                    v23_rabitq_ranked_leaf_prefix(&leaves, &request.geometry.leaf_offsets)?
                        .leaf_ordinals
                } else {
                    leaves.clone()
                };
                let started = Instant::now();
                let (ranked, maximum_ulp, scored_rows) = rank_exact_rows(
                    query,
                    &scanned_leaves,
                    request.geometry,
                    request.exact_rows,
                    request.backend,
                    true,
                )?;
                let kernel_elapsed_ns = u64::try_from(started.elapsed().as_nanos())
                    .unwrap_or(u64::MAX)
                    .max(1);
                let (scalar, _, _) = rank_exact_rows(
                    query,
                    &scanned_leaves,
                    request.geometry,
                    request.exact_rows,
                    request.backend,
                    false,
                )?;
                if pages_from_ranked(&ranked, request.rows)?.0
                    != pages_from_ranked(&scalar, request.rows)?.0
                {
                    return Err(invalid("V23 exact control scalar/SIMD pages differ"));
                }
                let (pages, page_assignments) = pages_from_ranked(&ranked, request.rows)?;
                V23RaBitQQueryEvidence {
                    query_ordinal: ordinal as u32,
                    requested_leaf_count: u32::try_from(requested_leaf_count).unwrap(),
                    scanned_leaf_count: u32::try_from(scanned_leaves.len()).unwrap(),
                    scored_rows: u32::try_from(scored_rows).unwrap(),
                    retained_rows: u16::try_from(ranked.len()).unwrap(),
                    page_assignments: u16::try_from(page_assignments).unwrap(),
                    page_ordinals: pages,
                    max_estimator_error_ppm: 0,
                    max_scalar_simd_error_ppm: Some(0),
                    max_exact_fused_ulp: maximum_ulp,
                    scalar_pages_equal: Some(true),
                    backend: detected_v23_rabitq_exact_backend()?,
                    kernel_elapsed_ns,
                }
            }
            V23RaBitQControl::RaBitQTree => unreachable!("RaBitQ tree handled above"),
            V23RaBitQControl::RaBitQExhaustive => {
                let started = Instant::now();
                let production = rank_rows_internal(
                    query,
                    &leaves,
                    request.geometry,
                    request.rows,
                    V23RaBitQRankOptions {
                        backend: request.backend,
                        differential_backend: None,
                        maximum_leaves: request.geometry.centroids.len(),
                        maximum_rows: request.rows.sign_codes.len(),
                    },
                )?;
                let kernel_elapsed_ns = u64::try_from(started.elapsed().as_nanos())
                    .unwrap_or(u64::MAX)
                    .max(1);
                let scalar = rank_rows_internal(
                    query,
                    &leaves,
                    request.geometry,
                    request.rows,
                    V23RaBitQRankOptions {
                        backend: V23RaBitQBackend::ScalarControl,
                        differential_backend: Some(V23RaBitQBackend::QueryLut),
                        maximum_leaves: request.geometry.centroids.len(),
                        maximum_rows: request.rows.sign_codes.len(),
                    },
                )?;
                let (pages, page_assignments) =
                    pages_from_ranked(&production.ranked, request.rows)?;
                if pages != pages_from_ranked(&scalar.ranked, request.rows)?.0 {
                    return Err(invalid("V23 RaBitQ control scalar/SIMD pages differ"));
                }
                let max_estimator_error_ppm = production
                    .ranked
                    .iter()
                    .map(|row| {
                        let denominator =
                            f64::from(row.distance.abs()).max(f64::from(f32::MIN_POSITIVE));
                        (f64::from(row.absolute_error_bound) / denominator * 1_000_000.0)
                            .ceil()
                            .min(u64::MAX as f64) as u64
                    })
                    .max()
                    .unwrap_or(0);
                V23RaBitQQueryEvidence {
                    query_ordinal: ordinal as u32,
                    requested_leaf_count: u32::try_from(leaves.len()).unwrap(),
                    scanned_leaf_count: u32::try_from(production.scanned_leaf_count).unwrap(),
                    scored_rows: u32::try_from(production.scored_rows).unwrap(),
                    retained_rows: u16::try_from(production.ranked.len()).unwrap(),
                    page_assignments: u16::try_from(page_assignments).unwrap(),
                    page_ordinals: pages,
                    max_estimator_error_ppm,
                    max_scalar_simd_error_ppm: Some(scalar.maximum_differential_error_ppm),
                    max_exact_fused_ulp: 0,
                    scalar_pages_equal: Some(true),
                    backend: request.backend,
                    kernel_elapsed_ns,
                }
            }
        };
        samples.push(sample_from_pages(
            ordinal as u32,
            &evidence.page_ordinals,
            truth,
            &evidence,
        )?);
    }
    cell_from_samples(control, probe_count, samples)
}

pub(crate) fn evaluate_v23_rabitq_development(
    request: V23RaBitQDevelopmentRequest<'_>,
) -> Result<V23RaBitQScreenResult> {
    if request.queries.len() != 32
        || request.truth.len() != 32
        || request.rows.sign_codes.len() != request.exact_rows.len()
        || request.geometry.centroids.len() != request.tree.leaves.len()
        || request
            .geometry
            .centroids
            .iter()
            .zip(&request.tree.leaves)
            .any(|(centroid, leaf)| centroid != &leaf.centroid)
        || request.geometry.centroids.is_empty()
        || request.geometry.centroids.len() > u16::MAX as usize + 1
        || request.backend == V23RaBitQBackend::ScalarControl
    {
        return Err(invalid("V23 RaBitQ development request differs"));
    }
    let all_leaves = (0..request.geometry.centroids.len())
        .map(|leaf| u16::try_from(leaf).unwrap())
        .collect::<Vec<_>>();
    let mut cells = Vec::with_capacity(8);
    for ordinal in 0..8 {
        let (control, probe_count) = expected_cell_shape(
            ordinal,
            u32::try_from(request.geometry.centroids.len()).unwrap(),
        );
        cells.push(evaluate_control(
            &request,
            control,
            probe_count,
            &all_leaves,
        )?);
    }
    let leaf_count = u32::try_from(request.geometry.centroids.len()).unwrap();
    let classification = classify_v23_rabitq_controls(&cells, leaf_count)?;
    let result = V23RaBitQScreenResult {
        schema: "borsuk-v23-rabitq-screen-v3".to_string(),
        source_commit: request.source_commit,
        source_archive_sha256: request.source_archive_sha256,
        index_id: request.index_id,
        indexed_rows: request.rows.sign_codes.len() as u64,
        leaf_count,
        projected_serving_bytes: project_v23_rabitq_serving_bytes(
            request.rows.sign_codes.len() as u64
        )?
        .total_bytes,
        inputs: request.inputs.to_vec(),
        development_truth: request.truth.to_vec(),
        cells,
        classification,
        claim_eligible: false,
    };
    canonical_v23_rabitq_screen_result_bytes(&result, request.inputs, leaf_count)?;
    Ok(result)
}

#[cfg(test)]
mod tests {
    use half::f16;

    use super::{
        SELECTED_PAGES, V23_RABITQ_SCALAR_SCORE_CALLS, V23_RABITQ_SELECT_CALLS, V23RaBitQBackend,
        V23RaBitQCellResult, V23RaBitQClassification, V23RaBitQControl,
        V23RaBitQDevelopmentRequest, V23RaBitQEvalRequest, V23RaBitQQuerySample,
        V23RaBitQScreenResult, canonical_v23_rabitq_screen_result_bytes,
        classify_v23_rabitq_controls, detected_v23_rabitq_backend, evaluate_rabitq_tree_sample,
        evaluate_v23_rabitq_development, rank_v23_rabitq_rows, score_v23_rabitq_code,
        select_v23_rabitq_pages, v23_rabitq_query_limits, v23_rabitq_ranked_leaf_prefix,
    };
    use crate::{
        v23_incidence_eval::V23IncidenceQueryTruth,
        v23_incidence_tree::{
            V23IncidenceTrainingShape, V23TrainingRow, assign_one_leaf,
            normalize_v23_incidence_vector, train_incidence_tree_test_shape,
        },
        v23_rabitq::V23RaBitQObjectIdentity,
        v23_rabitq_arrow::{V23RaBitQGeometry, V23RaBitQRowPlanes},
        v23_rabitq_quantizer::{
            build_v23_rabitq_rotation, encode_v23_rabitq_residual, prepare_v23_rabitq_query,
        },
    };
    use sha2::{Digest, Sha256};

    fn identity_rotation() -> [[f32; 96]; 96] {
        let mut value = [[0.0; 96]; 96];
        for (ordinal, row) in value.iter_mut().enumerate() {
            row[ordinal] = 1.0;
        }
        value
    }

    fn ulp_distance(left: f32, right: f32) -> u32 {
        fn ordered(value: f32) -> i32 {
            let bits = value.to_bits() as i32;
            if bits < 0 { i32::MIN - bits } else { bits }
        }
        ordered(left).abs_diff(ordered(right))
    }

    fn fixture(rows: usize, pages: u32) -> (V23RaBitQGeometry, V23RaBitQRowPlanes) {
        let rotation = identity_rotation();
        (
            V23RaBitQGeometry {
                leaf_offsets: vec![0, rows as u64],
                centroids: vec![[f16::ZERO; 96]],
                rotation,
            },
            V23RaBitQRowPlanes {
                sign_codes: (0..rows)
                    .map(|ordinal| [u8::try_from(ordinal % 251).unwrap(); 12])
                    .collect(),
                residual_norms: (0..rows)
                    .map(|ordinal| 1.0 + (ordinal % 31) as f32 / 32.0)
                    .collect(),
                alignments: vec![0.8; rows],
                primary_pages: (0..rows).map(|row| row as u32 % pages).collect(),
                replica_pages: (0..rows).map(|row| (row as u32 + 1) % pages).collect(),
            },
        )
    }

    fn scaled_limit_fixture() -> (V23RaBitQGeometry, V23RaBitQRowPlanes, Vec<u16>) {
        let (mut geometry, mut rows) = fixture(200_000, 16);
        geometry.centroids = vec![[f16::ZERO; 96]; 65_536];
        geometry.leaf_offsets = (0..=65_536)
            .map(|ordinal| {
                if ordinal <= 128 {
                    u64::try_from(ordinal * 4).unwrap()
                } else if ordinal < 65_536 {
                    512
                } else {
                    200_000
                }
            })
            .collect();
        rows.primary_pages = (0..200_000).map(|ordinal| ordinal as u32).collect();
        rows.replica_pages.fill(u32::MAX);
        let ranked_leaves = (0..128).collect();
        (geometry, rows, ranked_leaves)
    }

    fn identity(role: &str) -> V23RaBitQObjectIdentity {
        let bytes = role.as_bytes();
        V23RaBitQObjectIdentity {
            role: role.to_string(),
            uri: format!("s3://borsuk-v23-rabitq/development/{role}"),
            sha256: format!("{:x}", Sha256::digest(bytes)),
            blake3: None,
            encoded_bytes: bytes.len() as u64,
        }
    }

    fn screen_inputs() -> Vec<V23RaBitQObjectIdentity> {
        [
            "construction-receipt",
            "incidence-tree",
            "row-codes",
            "leaf-offsets",
            "centroids",
            "rotation",
            "f16-control",
            "d2-report",
            "query-parquet",
        ]
        .into_iter()
        .map(identity)
        .collect()
    }

    fn samples(
        probe_count: u32,
        backend: V23RaBitQBackend,
        exact_control: bool,
        exhaustive: bool,
    ) -> Vec<V23RaBitQQuerySample> {
        (0..32)
            .map(|query_ordinal| {
                let hits = if query_ordinal < 2 { 9 } else { 10 };
                V23RaBitQQuerySample {
                    query_ordinal,
                    page_ordinals: (0..SELECTED_PAGES)
                        .map(|page| query_ordinal * 8 + page as u32)
                        .collect(),
                    hits,
                    oracle_hits: hits,
                    recall_ppm: u32::from(hits) * 100_000,
                    requested_leaf_count: probe_count,
                    scanned_leaf_count: probe_count,
                    scored_rows: if exhaustive { 100_000_000 } else { 65_536 },
                    retained_rows: 4_096,
                    page_assignments: 8_192,
                    max_estimator_error_ppm: u64::from(!exact_control),
                    max_scalar_simd_error_ppm: u32::from(!exact_control),
                    max_exact_fused_ulp: u8::from(exact_control),
                    kernel_elapsed_ns: 1,
                    backend,
                    scalar_pages_equal: true,
                }
            })
            .collect()
    }

    fn cell(control: V23RaBitQControl, probe_count: u32, passed: bool) -> V23RaBitQCellResult {
        let exact_control = matches!(
            control,
            V23RaBitQControl::ExactExhaustive | V23RaBitQControl::ExactTree
        );
        let backend = if exact_control {
            super::detected_v23_rabitq_exact_backend().unwrap()
        } else {
            V23RaBitQBackend::QueryLut
        };
        let exhaustive = matches!(
            control,
            V23RaBitQControl::ExactExhaustive | V23RaBitQControl::RaBitQExhaustive
        );
        V23RaBitQCellResult {
            control,
            probe_count,
            samples: samples(probe_count, backend, exact_control, exhaustive),
            total_hits: 318,
            total_oracle_hits: 318,
            aggregate_recall_ppm: 993_750,
            minimum_recall_ppm: 900_000,
            oracle_attainment_ppm: 1_000_000,
            passed,
        }
    }

    fn screen_truth() -> Vec<V23IncidenceQueryTruth> {
        (0..32_u32)
            .map(|query_ordinal| {
                let hits = if query_ordinal < 2 { 9 } else { 10 };
                let selected =
                    std::array::from_fn::<_, 8, _>(|page| query_ordinal * 8 + page as u32);
                V23IncidenceQueryTruth {
                    query_ordinal,
                    ground_truth_page_assignments: (0..10)
                        .map(|neighbor| {
                            if neighbor < hits {
                                vec![selected[neighbor as usize % selected.len()]]
                            } else {
                                vec![10_000 + query_ordinal * 10 + neighbor]
                            }
                        })
                        .collect(),
                    oracle_pages: selected.to_vec(),
                }
            })
            .collect()
    }

    fn screen() -> V23RaBitQScreenResult {
        let cells = vec![
            cell(V23RaBitQControl::ExactExhaustive, 65_536, true),
            cell(V23RaBitQControl::ExactTree, 32, true),
            cell(V23RaBitQControl::ExactTree, 64, true),
            cell(V23RaBitQControl::ExactTree, 128, true),
            cell(V23RaBitQControl::RaBitQExhaustive, 65_536, true),
            cell(V23RaBitQControl::RaBitQTree, 32, true),
            cell(V23RaBitQControl::RaBitQTree, 64, true),
            cell(V23RaBitQControl::RaBitQTree, 128, true),
        ];
        V23RaBitQScreenResult {
            schema: "borsuk-v23-rabitq-screen-v3".to_string(),
            source_commit: "1".repeat(40),
            source_archive_sha256: "2".repeat(64),
            index_id: "deep-image-96-v23-rabitq".to_string(),
            indexed_rows: 100_000_000,
            leaf_count: 65_536,
            projected_serving_bytes: 2_920_622_772,
            inputs: screen_inputs(),
            development_truth: screen_truth(),
            cells,
            classification: V23RaBitQClassification::DevelopmentCandidateAccepted,
            claim_eligible: false,
        }
    }

    fn reduced_tree() -> crate::v23_incidence_tree::V23IncidenceTree {
        let rows = (0..32)
            .map(|source_ordinal| V23TrainingRow {
                source_ordinal,
                vector: std::array::from_fn(|dimension| {
                    (((source_ordinal as usize + 1) * (dimension + 3) % 211) as f32 + 1.0) / 212.0
                }),
            })
            .collect::<Vec<_>>();
        train_incidence_tree_test_shape(
            &rows,
            V23IncidenceTrainingShape {
                dimensions: 96,
                reservoir_rows: 32,
                depth: 3,
                lloyd_iterations: 4,
            },
            1,
            16,
        )
        .unwrap()
    }

    type EvaluationFixture = (
        crate::v23_incidence_tree::V23IncidenceTree,
        V23RaBitQGeometry,
        V23RaBitQRowPlanes,
        Vec<[f16; 96]>,
        Vec<[f32; 96]>,
        Vec<V23IncidenceQueryTruth>,
    );

    fn evaluation_fixture() -> EvaluationFixture {
        let source = (0..2_048)
            .map(|source_ordinal| V23TrainingRow {
                source_ordinal,
                vector: std::array::from_fn(|dimension| {
                    let mut word = source_ordinal
                        ^ ((dimension as u64 + 1).wrapping_mul(0x9e37_79b9_7f4a_7c15));
                    word = (word ^ (word >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
                    word = (word ^ (word >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
                    word ^= word >> 31;
                    ((word >> 40) as f32 + 1.0) / 16_777_217.0
                }),
            })
            .collect::<Vec<_>>();
        let tree = train_incidence_tree_test_shape(
            &source,
            V23IncidenceTrainingShape {
                dimensions: 96,
                reservoir_rows: 2_048,
                depth: 11,
                lloyd_iterations: 4,
            },
            1,
            64,
        )
        .unwrap();
        let rotation = build_v23_rabitq_rotation([0x42; 32]).unwrap();
        let mut grouped = (0..tree.leaves.len())
            .map(|_| Vec::new())
            .collect::<Vec<Vec<_>>>();
        for row in &source {
            let leaf =
                usize::from(assign_one_leaf(&tree, &row.vector, row.source_ordinal).unwrap());
            grouped[leaf].push(row);
        }
        let mut offsets = Vec::with_capacity(129);
        let mut sign_codes = Vec::new();
        let mut residual_norms = Vec::new();
        let mut alignments = Vec::new();
        let mut primary_pages = Vec::new();
        let mut replica_pages = Vec::new();
        let mut exact_rows = Vec::new();
        offsets.push(0);
        for (leaf, rows) in grouped.iter().enumerate() {
            let centroid = tree.leaves[leaf].centroid.map(f16::to_f32);
            for row in rows {
                let normalized = normalize_v23_incidence_vector(&row.vector).unwrap();
                let residual =
                    std::array::from_fn(|dimension| normalized[dimension] - centroid[dimension]);
                let code = encode_v23_rabitq_residual(&residual, &rotation).unwrap();
                sign_codes.push(code.sign_code);
                residual_norms.push(code.residual_norm);
                alignments.push(code.alignment);
                primary_pages.push(row.source_ordinal as u32 % 16);
                replica_pages.push((row.source_ordinal as u32 + 1) % 16);
                exact_rows.push(normalized.map(f16::from_f32));
            }
            offsets.push(sign_codes.len() as u64);
        }
        let geometry = V23RaBitQGeometry {
            leaf_offsets: offsets,
            centroids: tree.leaves.iter().map(|leaf| leaf.centroid).collect(),
            rotation,
        };
        let rows = V23RaBitQRowPlanes {
            sign_codes,
            residual_norms,
            alignments,
            primary_pages,
            replica_pages,
        };
        let queries = source[..32]
            .iter()
            .map(|row| row.vector)
            .collect::<Vec<_>>();
        let truth = (0..32)
            .map(|query_ordinal| V23IncidenceQueryTruth {
                query_ordinal,
                ground_truth_page_assignments: (0..10).map(|page| vec![page]).collect(),
                oracle_pages: (0..8).collect(),
            })
            .collect();
        (tree, geometry, rows, exact_rows, queries, truth)
    }

    #[test]
    fn v23_rabitq_eval_scales_scan_but_keeps_production_heap_and_assignment_caps() {
        let cases = [
            (1, 1, 4_096, 8_192),
            (9_990_000, 26_189, 4_096, 8_192),
            (99_999_999, 262_144, 4_096, 8_192),
            (100_000_000, 262_144, 4_096, 8_192),
        ];
        for (indexed_rows, scored_rows, retained_rows, page_assignments) in cases {
            let limits = v23_rabitq_query_limits(indexed_rows).unwrap();
            assert_eq!(limits.scored_rows, scored_rows);
            assert_eq!(limits.retained_rows, retained_rows);
            assert_eq!(limits.page_assignments, page_assignments);
        }
        assert!(v23_rabitq_query_limits(0).is_err());
        assert!(v23_rabitq_query_limits(100_000_001).is_err());
    }

    #[test]
    fn v23_rabitq_eval_truncates_the_lowest_ranked_whole_leaf_before_scan_overflow() {
        let offsets = [0, 10_000, 20_000, 30_000, 9_990_000];
        let selected = v23_rabitq_ranked_leaf_prefix(&[1, 0, 2, 3], &offsets).unwrap();
        assert_eq!(selected.requested_leaf_count, 4);
        assert_eq!(selected.leaf_ordinals, vec![1, 0]);
        assert_eq!(selected.scored_rows, 20_000);

        let exact = v23_rabitq_ranked_leaf_prefix(&[2, 0], &offsets).unwrap();
        assert_eq!(exact.leaf_ordinals, vec![2, 0]);
        assert_eq!(exact.scored_rows, 20_000);
    }

    #[test]
    fn v23_rabitq_eval_fused_matches_registered_scalar_on_adversarial_vectors() {
        let backend = detected_v23_rabitq_backend().unwrap();
        let rotation = build_v23_rabitq_rotation([13; 32]).unwrap();
        let cases = [
            [0.0; 96],
            [f32::from_bits(1); 96],
            std::array::from_fn(|ordinal| (ordinal as f32 - 47.0) / 53.0),
            std::array::from_fn(|ordinal| (95 - ordinal) as f32 / 97.0),
        ];
        for (ordinal, row) in cases.iter().enumerate() {
            let query = cases[(ordinal + 1) % cases.len()];
            let code = encode_v23_rabitq_residual(row, &rotation).unwrap();
            let prepared = prepare_v23_rabitq_query(&query, &rotation).unwrap();
            let scalar =
                score_v23_rabitq_code(&prepared, &code, V23RaBitQBackend::ScalarControl).unwrap();
            let fused = score_v23_rabitq_code(&prepared, &code, backend).unwrap();
            assert!(ulp_distance(scalar.distance_squared, fused.distance_squared) <= 8);
        }
    }

    #[test]
    fn v23_rabitq_eval_query_lut_is_the_production_scoring_backend() {
        assert_eq!(
            detected_v23_rabitq_backend().unwrap(),
            V23RaBitQBackend::QueryLut
        );
        let rotation = build_v23_rabitq_rotation([29; 32]).unwrap();
        let row = std::array::from_fn(|ordinal| (ordinal as f32 - 41.0) / 59.0);
        let query = std::array::from_fn(|ordinal| (83.0 - ordinal as f32) / 71.0);
        let code = encode_v23_rabitq_residual(&row, &rotation).unwrap();
        let prepared = prepare_v23_rabitq_query(&query, &rotation).unwrap();
        let scalar =
            score_v23_rabitq_code(&prepared, &code, V23RaBitQBackend::ScalarControl).unwrap();
        let production =
            score_v23_rabitq_code(&prepared, &code, V23RaBitQBackend::QueryLut).unwrap();
        let scale = 96.0f32.sqrt().recip()
            * prepared
                .reconstructed
                .iter()
                .map(|value| value.abs())
                .sum::<f32>();
        assert!(
            (production.estimated_cosine - scalar.estimated_cosine).abs()
                <= 8.0 * f32::EPSILON * scale.max(f32::MIN_POSITIVE)
        );
    }

    #[test]
    fn v23_rabitq_eval_bounds_scan_with_fixed_heap_and_assignment_caps() {
        let (geometry, rows, ranked_leaves) = scaled_limit_fixture();
        let query = std::array::from_fn(|ordinal| (ordinal as f32 - 31.0) / 37.0);
        let backend = detected_v23_rabitq_backend().unwrap();
        V23_RABITQ_SCALAR_SCORE_CALLS.with(|calls| calls.set(0));
        let ranked =
            rank_v23_rabitq_rows(&query, &ranked_leaves, &geometry, &rows, backend).unwrap();
        assert_eq!(ranked.len(), 512);

        let evidence = select_v23_rabitq_pages(V23RaBitQEvalRequest {
            query_ordinal: 7,
            query: &query,
            ranked_leaf_ordinals: &ranked_leaves,
            geometry: &geometry,
            rows: &rows,
            backend,
        })
        .unwrap();
        assert_eq!(evidence.query_ordinal, 7);
        assert_eq!(evidence.requested_leaf_count, 128);
        assert_eq!(evidence.scanned_leaf_count, 128);
        assert_eq!(evidence.scored_rows, 512);
        assert_eq!(evidence.retained_rows, 512);
        assert_eq!(evidence.page_assignments, 512);
        assert_eq!(evidence.page_ordinals.len(), 8);
        assert!(
            evidence
                .page_ordinals
                .windows(2)
                .all(|pair| pair[0] < pair[1])
        );
        assert_eq!(evidence.scalar_pages_equal, None);
        assert!(evidence.kernel_elapsed_ns > 0);
        V23_RABITQ_SCALAR_SCORE_CALLS.with(|calls| assert_eq!(calls.get(), 0));
    }

    #[test]
    fn v23_rabitq_eval_tree_sample_uses_the_shared_serving_selector_once() {
        let (geometry, rows, ranked_leaves) = scaled_limit_fixture();
        let query = std::array::from_fn(|ordinal| (ordinal as f32 - 31.0) / 37.0);
        let truth = V23IncidenceQueryTruth {
            query_ordinal: 0,
            ground_truth_page_assignments: (0..10).map(|page| vec![page]).collect(),
            oracle_pages: (0..8).collect(),
        };
        V23_RABITQ_SELECT_CALLS.with(|calls| calls.set(0));
        let sample = evaluate_rabitq_tree_sample(
            0,
            &query,
            &truth,
            &ranked_leaves,
            &geometry,
            &rows,
            detected_v23_rabitq_backend().unwrap(),
        )
        .unwrap();
        assert_eq!(sample.requested_leaf_count, 128);
        V23_RABITQ_SELECT_CALLS.with(|calls| assert_eq!(calls.get(), 1));
    }

    #[test]
    fn v23_rabitq_eval_rejects_nonfinite_queries_and_shape_drift() {
        let (mut geometry, rows, ranked_leaves) = scaled_limit_fixture();
        let query = std::array::from_fn(|ordinal| (ordinal as f32 + 1.0) / 101.0);
        let backend = detected_v23_rabitq_backend().unwrap();

        let mut invalid = query;
        invalid[0] = f32::NAN;
        assert!(rank_v23_rabitq_rows(&invalid, &ranked_leaves, &geometry, &rows, backend).is_err());
        geometry.leaf_offsets[65_536] = 199_999;
        assert!(rank_v23_rabitq_rows(&query, &ranked_leaves, &geometry, &rows, backend).is_err());
    }

    #[test]
    fn v23_rabitq_eval_production_rejects_scalar_and_wrong_architecture_backends() {
        let (geometry, rows) = fixture(16, 16);
        let query = [1.0; 96];
        assert!(
            select_v23_rabitq_pages(V23RaBitQEvalRequest {
                query_ordinal: 0,
                query: &query,
                ranked_leaf_ordinals: &[0],
                geometry: &geometry,
                rows: &rows,
                backend: V23RaBitQBackend::ScalarControl,
            })
            .is_err()
        );

        #[cfg(target_arch = "aarch64")]
        assert!(
            rank_v23_rabitq_rows(&query, &[0], &geometry, &rows, V23RaBitQBackend::X86Avx2Fma,)
                .is_err()
        );
        #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
        assert!(
            rank_v23_rabitq_rows(
                &query,
                &[0],
                &geometry,
                &rows,
                V23RaBitQBackend::Aarch64Neon,
            )
            .is_err()
        );
    }

    #[test]
    fn v23_rabitq_screen_classification_precedence_is_outcome_blind() {
        let accepted = screen().cells;
        assert_eq!(
            classify_v23_rabitq_controls(&accepted, 65_536).unwrap(),
            V23RaBitQClassification::DevelopmentCandidateAccepted
        );

        let mut cells = accepted.clone();
        cells[4].passed = false;
        assert_eq!(
            classify_v23_rabitq_controls(&cells, 65_536).unwrap(),
            V23RaBitQClassification::DevelopmentCandidateAccepted
        );

        let mut cells = accepted.clone();
        cells[0].total_hits = 317;
        assert_eq!(
            classify_v23_rabitq_controls(&cells, 65_536).unwrap(),
            V23RaBitQClassification::AuthorityStop
        );
        let mut cells = accepted.clone();
        cells[1..4].iter_mut().for_each(|cell| cell.passed = false);
        assert_eq!(
            classify_v23_rabitq_controls(&cells, 65_536).unwrap(),
            V23RaBitQClassification::TreePruningRejected
        );
        let mut cells = accepted.clone();
        cells[2].passed = false;
        cells[3].passed = false;
        cells[5].passed = false;
        cells[7].passed = false;
        assert_eq!(
            classify_v23_rabitq_controls(&cells, 65_536).unwrap(),
            V23RaBitQClassification::RaBitQEstimatorRejected
        );
        let mut cells = accepted;
        cells[5..].iter_mut().for_each(|cell| cell.passed = false);
        assert_eq!(
            classify_v23_rabitq_controls(&cells, 65_536).unwrap(),
            V23RaBitQClassification::RaBitQEstimatorRejected
        );
    }

    #[test]
    fn v23_rabitq_screen_canonical_result_recomputes_every_quality_and_authority_field() {
        let expected = screen();
        let bytes = canonical_v23_rabitq_screen_result_bytes(
            &expected,
            &expected.inputs,
            expected.leaf_count,
        )
        .unwrap();
        assert_eq!(bytes.last(), Some(&b'\n'));
        assert_eq!(
            bytes,
            canonical_v23_rabitq_screen_result_bytes(
                &expected,
                &expected.inputs,
                expected.leaf_count,
            )
            .unwrap()
        );

        let mut mutations = Vec::new();
        let mut changed = expected.clone();
        changed.leaf_count = 65_535;
        for cell in changed.cells.iter_mut().filter(|cell| {
            matches!(
                cell.control,
                V23RaBitQControl::ExactExhaustive | V23RaBitQControl::RaBitQExhaustive
            )
        }) {
            cell.probe_count = 65_535;
            for sample in &mut cell.samples {
                sample.requested_leaf_count = 65_535;
                sample.scanned_leaf_count = 65_535;
            }
        }
        mutations.push(changed);
        let mut changed = expected.clone();
        changed.development_truth[0].ground_truth_page_assignments[0][0] = 20_000;
        mutations.push(changed);
        let mut changed = expected.clone();
        changed.cells[0].samples[0].page_ordinals.swap(0, 1);
        mutations.push(changed);
        let mut changed = expected.clone();
        changed.cells[0].samples[0].page_ordinals = Vec::new();
        mutations.push(changed);
        let mut changed = expected.clone();
        changed.cells[0].samples[0].page_ordinals = (0..=SELECTED_PAGES as u32).collect();
        mutations.push(changed);
        let mut changed = expected.clone();
        changed.cells[0].samples[0].hits = 8;
        mutations.push(changed);
        let mut changed = expected.clone();
        changed.cells[0].samples[0].recall_ppm = 800_000;
        mutations.push(changed);
        let mut changed = expected.clone();
        changed.cells[0].aggregate_recall_ppm = 993_749;
        mutations.push(changed);
        let mut changed = expected.clone();
        changed.cells[7].samples[0].requested_leaf_count = 64;
        mutations.push(changed);
        let mut changed = expected.clone();
        changed.cells[7].samples[0].scanned_leaf_count = 129;
        mutations.push(changed);
        let mut changed = expected.clone();
        changed.cells[7].samples[0].scored_rows = 262_145;
        mutations.push(changed);
        let mut changed = expected.clone();
        changed.cells[7].samples[0].retained_rows = 4_097;
        mutations.push(changed);
        let mut changed = expected.clone();
        changed.cells[7].samples[0].page_assignments = 8_193;
        mutations.push(changed);
        let mut changed = expected.clone();
        changed.cells[7].samples[0].scalar_pages_equal = false;
        mutations.push(changed);
        let mut changed = expected.clone();
        changed.cells[7].samples[0].kernel_elapsed_ns = 0;
        mutations.push(changed);
        let mut changed = expected.clone();
        changed.projected_serving_bytes -= 1;
        mutations.push(changed);
        let mut changed = expected.clone();
        changed.classification = V23RaBitQClassification::AuthorityStop;
        mutations.push(changed);
        let mut changed = expected.clone();
        changed.inputs[0].sha256 = "3".repeat(64);
        mutations.push(changed);
        let mut changed = expected.clone();
        changed.claim_eligible = true;
        mutations.push(changed);
        for mutation in mutations {
            assert!(
                canonical_v23_rabitq_screen_result_bytes(
                    &mutation,
                    &expected.inputs,
                    expected.leaf_count,
                )
                .is_err()
            );
        }
    }

    #[test]
    fn v23_rabitq_screen_evaluator_rejects_incomplete_development_authority() {
        let tree = reduced_tree();
        let (geometry, rows) = fixture(32, 16);
        let exact_rows = vec![[f16::ZERO; 96]; 32];
        let queries = Vec::<[f32; 96]>::new();
        let truth = Vec::<V23IncidenceQueryTruth>::new();
        let inputs = screen_inputs();
        assert!(
            evaluate_v23_rabitq_development(V23RaBitQDevelopmentRequest {
                source_commit: "1".repeat(40),
                source_archive_sha256: "2".repeat(64),
                index_id: "deep-image-96-v23-rabitq".to_string(),
                inputs: &inputs,
                tree: &tree,
                geometry: &geometry,
                rows: &rows,
                exact_rows: &exact_rows,
                queries: &queries,
                truth: &truth,
                backend: detected_v23_rabitq_backend().unwrap(),
            })
            .is_err()
        );
    }

    #[test]
    fn v23_rabitq_screen_evaluator_records_saturated_cover_without_padding_or_abort() {
        let (tree, geometry, rows, exact_rows, queries, truth) = evaluation_fixture();
        let inputs = screen_inputs();
        let result = evaluate_v23_rabitq_development(V23RaBitQDevelopmentRequest {
            source_commit: "1".repeat(40),
            source_archive_sha256: "2".repeat(64),
            index_id: "deep-image-96-v23-rabitq".to_string(),
            inputs: &inputs,
            tree: &tree,
            geometry: &geometry,
            rows: &rows,
            exact_rows: &exact_rows,
            queries: &queries,
            truth: &truth,
            backend: detected_v23_rabitq_backend().unwrap(),
        })
        .unwrap();
        assert_eq!(result.cells.len(), 8);
        assert!(result.cells.iter().all(|cell| cell.samples.len() == 32));
        assert!(result.cells.iter().any(|cell| {
            cell.samples
                .iter()
                .any(|sample| sample.page_ordinals.len() < SELECTED_PAGES)
        }));
        assert!(
            result
                .cells
                .iter()
                .flat_map(|cell| &cell.samples)
                .all(|sample| {
                    !sample.page_ordinals.is_empty()
                        && sample.page_ordinals.len() <= SELECTED_PAGES
                        && sample
                            .page_ordinals
                            .windows(2)
                            .all(|pair| pair[0] < pair[1])
                })
        );
    }
}
