use std::{cmp::Ordering, collections::BinaryHeap};

use borsuk_fma::{FmaBackend, fused_dot_8x12};
use half::f16;
use serde::{Deserialize, Serialize};

use crate::{
    BorsukError, Result,
    v23_diagnostic::v23_reciprocal_rank_page_cover,
    v23_incidence_tree::normalize_v23_incidence_vector,
    v23_rabitq::V23RaBitQObjectIdentity,
    v23_rabitq_arrow::{V23RaBitQGeometry, V23RaBitQRowPlanes},
    v23_rabitq_quantizer::{
        V23RaBitQCode, V23RaBitQEstimate, V23RaBitQPreparedQuery, estimate_v23_rabitq_from_dot,
        prepare_v23_rabitq_query_with_validated_rotation, score_v23_rabitq_prepared_scalar,
        validate_v23_rabitq_rotation,
    },
};

const MAX_SCORED_ROWS: usize = 262_144;
const MAX_RETAINED_ROWS: usize = 4_096;
const MAX_PAGE_ASSIGNMENTS: usize = 8_192;
const SELECTED_PAGES: usize = 8;
const INVERSE_SQRT_DIMENSIONS: f32 = 0.102_062_07;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum V23RaBitQBackend {
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct V23RaBitQQueryEvidence {
    pub(crate) query_ordinal: u32,
    pub(crate) probe_count: u16,
    pub(crate) scored_rows: u32,
    pub(crate) retained_rows: u16,
    pub(crate) page_assignments: u16,
    pub(crate) page_ordinals: [u32; SELECTED_PAGES],
    pub(crate) max_estimator_error_ppm: u64,
    pub(crate) max_scalar_simd_ulp: u8,
    pub(crate) scalar_pages_equal: bool,
    pub(crate) backend: V23RaBitQBackend,
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
    RaBitQRepresentationRejected,
    TreeRaBitQCompositionRejected,
    DevelopmentCandidateAccepted,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct V23RaBitQQuerySample {
    pub(crate) query_ordinal: u32,
    pub(crate) page_ordinals: [u32; SELECTED_PAGES],
    pub(crate) hits: u16,
    pub(crate) oracle_hits: u16,
    pub(crate) recall_ppm: u32,
    pub(crate) scored_rows: u32,
    pub(crate) retained_rows: u16,
    pub(crate) page_assignments: u16,
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
    pub(crate) inputs: Vec<V23RaBitQObjectIdentity>,
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

fn expected_cell_shape(ordinal: usize) -> (V23RaBitQControl, u32) {
    match ordinal {
        0 => (V23RaBitQControl::ExactExhaustive, 65_536),
        1 => (V23RaBitQControl::ExactTree, 32),
        2 => (V23RaBitQControl::ExactTree, 64),
        3 => (V23RaBitQControl::ExactTree, 128),
        4 => (V23RaBitQControl::RaBitQExhaustive, 65_536),
        5 => (V23RaBitQControl::RaBitQTree, 32),
        6 => (V23RaBitQControl::RaBitQTree, 64),
        _ => (V23RaBitQControl::RaBitQTree, 128),
    }
}

fn validate_cell_shape(cells: &[V23RaBitQCellResult]) -> Result<()> {
    if cells.len() != 8
        || cells.iter().enumerate().any(|(ordinal, cell)| {
            let expected = expected_cell_shape(ordinal);
            (cell.control, cell.probe_count) != expected
        })
    {
        return Err(invalid("V23 RaBitQ screen cell shape differs"));
    }
    Ok(())
}

pub(crate) fn classify_v23_rabitq_controls(
    cells: &[V23RaBitQCellResult],
) -> Result<V23RaBitQClassification> {
    validate_cell_shape(cells)?;
    if cells[0].total_hits != 318 || cells[0].total_oracle_hits != 318 || !cells[0].passed {
        return Ok(V23RaBitQClassification::AuthorityStop);
    }
    if !cells[1..4].iter().any(|cell| cell.passed) {
        return Ok(V23RaBitQClassification::TreePruningRejected);
    }
    if !cells[4].passed {
        return Ok(V23RaBitQClassification::RaBitQRepresentationRejected);
    }
    if !cells[5..].iter().any(|cell| cell.passed) {
        return Ok(V23RaBitQClassification::TreeRaBitQCompositionRejected);
    }
    Ok(V23RaBitQClassification::DevelopmentCandidateAccepted)
}

fn validate_cell(cell: &V23RaBitQCellResult) -> Result<()> {
    if cell.samples.len() != 32 {
        return Err(invalid("V23 RaBitQ query sample count differs"));
    }
    let mut total_hits = 0u16;
    let mut total_oracle_hits = 0u16;
    let mut minimum_recall_ppm = u32::MAX;
    for (ordinal, sample) in cell.samples.iter().enumerate() {
        if sample.query_ordinal != ordinal as u32
            || sample
                .page_ordinals
                .windows(2)
                .any(|pair| pair[0] >= pair[1])
            || sample.hits > 10
            || sample.oracle_hits > 10
            || sample.hits > sample.oracle_hits
            || sample.recall_ppm != u32::from(sample.hits) * 100_000
            || sample.scored_rows == 0
            || sample.scored_rows > MAX_SCORED_ROWS as u32
            || sample.retained_rows == 0
            || usize::from(sample.retained_rows) > MAX_RETAINED_ROWS
            || usize::from(sample.page_assignments) > MAX_PAGE_ASSIGNMENTS
            || sample.backend == V23RaBitQBackend::ScalarControl
            || !sample.scalar_pages_equal
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
) -> Result<Vec<u8>> {
    if result.schema != "borsuk-v23-rabitq-screen-v1"
        || !valid_lower_hex(&result.source_commit, 40)
        || !valid_lower_hex(&result.source_archive_sha256, 64)
        || result.index_id.is_empty()
        || result.inputs != expected_inputs
        || result.inputs.is_empty()
        || result.claim_eligible
    {
        return Err(invalid("V23 RaBitQ screen authority differs"));
    }
    let mut roles = std::collections::BTreeSet::new();
    let mut uris = std::collections::BTreeSet::new();
    for identity in &result.inputs {
        if identity.role.is_empty()
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
    validate_cell_shape(&result.cells)?;
    result.cells.iter().try_for_each(validate_cell)?;
    if result.classification != classify_v23_rabitq_controls(&result.cells)? {
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

pub(crate) fn detected_v23_rabitq_backend() -> Result<V23RaBitQBackend> {
    let (_, backend) = fused_dot_8x12(&[0.0; 96], &[0.0; 96])
        .map_err(|_| invalid("V23 RaBitQ fused backend is unavailable"))?;
    let backend = map_backend(backend);
    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    if backend == V23RaBitQBackend::X86Avx2Fma && !std::arch::is_x86_feature_detected!("avx2") {
        return Err(invalid("V23 RaBitQ AVX2 backend is unavailable"));
    }
    Ok(backend)
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
    if backend == V23RaBitQBackend::ScalarControl {
        return score_v23_rabitq_prepared_scalar(prepared, code);
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
    estimate_v23_rabitq_from_dot(prepared, code, dot)
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
) -> Result<(Vec<u16>, usize)> {
    let row_count = rows.sign_codes.len();
    if ranked_leaf_ordinals.is_empty()
        || ranked_leaf_ordinals.len() > 128
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
    let mut leaves = ranked_leaf_ordinals.to_vec();
    leaves.sort_unstable();
    if leaves.windows(2).any(|pair| pair[0] == pair[1])
        || leaves
            .last()
            .is_some_and(|leaf| usize::from(*leaf) >= geometry.centroids.len())
    {
        return Err(invalid("V23 RaBitQ ranked leaves differ"));
    }
    let mut scored_rows = 0usize;
    for leaf in &leaves {
        let leaf = usize::from(*leaf);
        let start = usize::try_from(geometry.leaf_offsets[leaf])
            .map_err(|_| invalid("V23 RaBitQ leaf offset exceeds usize"))?;
        let end = usize::try_from(geometry.leaf_offsets[leaf + 1])
            .map_err(|_| invalid("V23 RaBitQ leaf offset exceeds usize"))?;
        scored_rows = scored_rows
            .checked_add(end - start)
            .ok_or_else(|| invalid("V23 RaBitQ scored rows overflow"))?;
    }
    if scored_rows == 0 || scored_rows > MAX_SCORED_ROWS {
        return Err(invalid("V23 RaBitQ scored-row cap differs"));
    }
    Ok((leaves, scored_rows))
}

fn rank_rows_internal(
    query: &[f32; 96],
    ranked_leaf_ordinals: &[u16],
    geometry: &V23RaBitQGeometry,
    rows: &V23RaBitQRowPlanes,
    backend: V23RaBitQBackend,
) -> Result<(Vec<V23RaBitQRankedRow>, u8, usize)> {
    if query.iter().any(|value| !value.is_finite()) {
        return Err(invalid("V23 RaBitQ query is nonfinite"));
    }
    let (leaves, scored_rows) = validate_shapes(ranked_leaf_ordinals, geometry, rows)?;
    let normalized_query = normalize_v23_incidence_vector(query)?;
    let mut heap = BinaryHeap::with_capacity(MAX_RETAINED_ROWS + 1);
    let mut maximum_ulp = 0u32;
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
            let estimate = score_v23_rabitq_code(&prepared, &code, backend)?;
            if backend != V23RaBitQBackend::ScalarControl {
                let scalar = score_v23_rabitq_prepared_scalar(&prepared, &code)?;
                let ulp = float_ulp_distance(estimate.distance_squared, scalar.distance_squared);
                if ulp > 8 {
                    return Err(invalid("V23 RaBitQ scalar/SIMD distance differs"));
                }
                maximum_ulp = maximum_ulp.max(ulp);
            }
            let candidate = V23RaBitQRankedRow {
                distance: estimate.distance_squared,
                row_ordinal: u32::try_from(row_ordinal)
                    .map_err(|_| invalid("V23 RaBitQ row ordinal exceeds u32"))?,
                absolute_error_bound: estimate.absolute_error_bound,
            };
            if heap.len() < MAX_RETAINED_ROWS {
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

pub(crate) fn rank_v23_rabitq_rows(
    query: &[f32; 96],
    ranked_leaf_ordinals: &[u16],
    geometry: &V23RaBitQGeometry,
    rows: &V23RaBitQRowPlanes,
    backend: V23RaBitQBackend,
) -> Result<Vec<V23RaBitQRankedRow>> {
    Ok(rank_rows_internal(query, ranked_leaf_ordinals, geometry, rows, backend)?.0)
}

fn ranked_page_assignments(
    ranked: &[V23RaBitQRankedRow],
    rows: &V23RaBitQRowPlanes,
) -> Result<Vec<(u32, Option<u32>)>> {
    if ranked.len() > MAX_RETAINED_ROWS {
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
    if assignment_count > MAX_PAGE_ASSIGNMENTS {
        return Err(invalid("V23 RaBitQ page-assignment cap differs"));
    }
    Ok(assignments)
}

pub(crate) fn select_v23_rabitq_pages(
    request: V23RaBitQEvalRequest<'_>,
) -> Result<V23RaBitQQueryEvidence> {
    if request.backend == V23RaBitQBackend::ScalarControl {
        return Err(invalid("V23 RaBitQ production backend is not fused"));
    }
    let (ranked, max_ulp, scored_rows) = rank_rows_internal(
        request.query,
        request.ranked_leaf_ordinals,
        request.geometry,
        request.rows,
        request.backend,
    )?;
    let assignments = ranked_page_assignments(&ranked, request.rows)?;
    let pages = v23_reciprocal_rank_page_cover(&assignments, SELECTED_PAGES)?;
    if pages.len() != SELECTED_PAGES {
        return Err(invalid("V23 RaBitQ cannot select exactly eight pages"));
    }

    let (scalar_ranked, _, _) = rank_rows_internal(
        request.query,
        request.ranked_leaf_ordinals,
        request.geometry,
        request.rows,
        V23RaBitQBackend::ScalarControl,
    )?;
    let scalar_assignments = ranked_page_assignments(&scalar_ranked, request.rows)?;
    let scalar_pages = v23_reciprocal_rank_page_cover(&scalar_assignments, SELECTED_PAGES)?;
    if scalar_pages != pages {
        return Err(invalid("V23 RaBitQ scalar/SIMD selected pages differ"));
    }
    let assignment_count = assignments
        .iter()
        .map(|(_, replica)| 1 + usize::from(replica.is_some()))
        .sum::<usize>();
    let max_estimator_error_ppm = ranked
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
        probe_count: u16::try_from(request.ranked_leaf_ordinals.len()).unwrap(),
        scored_rows: u32::try_from(scored_rows).unwrap(),
        retained_rows: u16::try_from(ranked.len()).unwrap(),
        page_assignments: u16::try_from(assignment_count).unwrap(),
        page_ordinals: pages
            .try_into()
            .map_err(|_| invalid("V23 RaBitQ selected-page width differs"))?,
        max_estimator_error_ppm,
        max_scalar_simd_ulp: max_ulp,
        scalar_pages_equal: true,
        backend: request.backend,
    })
}

#[cfg(test)]
mod tests {
    use half::f16;

    use super::{
        V23RaBitQBackend, V23RaBitQCellResult, V23RaBitQClassification, V23RaBitQControl,
        V23RaBitQEvalRequest, V23RaBitQQuerySample, V23RaBitQScreenResult,
        canonical_v23_rabitq_screen_result_bytes, classify_v23_rabitq_controls,
        detected_v23_rabitq_backend, rank_v23_rabitq_rows, score_v23_rabitq_code,
        select_v23_rabitq_pages,
    };
    use crate::{
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

    fn samples() -> Vec<V23RaBitQQuerySample> {
        (0..32)
            .map(|query_ordinal| {
                let hits = if query_ordinal < 2 { 9 } else { 10 };
                V23RaBitQQuerySample {
                    query_ordinal,
                    page_ordinals: std::array::from_fn(|page| query_ordinal * 8 + page as u32),
                    hits,
                    oracle_hits: hits,
                    recall_ppm: u32::from(hits) * 100_000,
                    scored_rows: 65_536,
                    retained_rows: 4_096,
                    page_assignments: 8_192,
                    backend: V23RaBitQBackend::Aarch64Neon,
                    scalar_pages_equal: true,
                }
            })
            .collect()
    }

    fn cell(control: V23RaBitQControl, probe_count: u32, passed: bool) -> V23RaBitQCellResult {
        V23RaBitQCellResult {
            control,
            probe_count,
            samples: samples(),
            total_hits: 318,
            total_oracle_hits: 318,
            aggregate_recall_ppm: 993_750,
            minimum_recall_ppm: 900_000,
            oracle_attainment_ppm: 1_000_000,
            passed,
        }
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
            schema: "borsuk-v23-rabitq-screen-v1".to_string(),
            source_commit: "1".repeat(40),
            source_archive_sha256: "2".repeat(64),
            index_id: "deep-image-96-v23-rabitq".to_string(),
            inputs: vec![identity("construction-receipt"), identity("query-parquet")],
            cells,
            classification: V23RaBitQClassification::DevelopmentCandidateAccepted,
            claim_eligible: false,
        }
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
    fn v23_rabitq_eval_bounds_scan_heap_assignments_and_selects_exactly_eight_pages() {
        let (geometry, rows) = fixture(5_000, 16);
        let query = std::array::from_fn(|ordinal| (ordinal as f32 - 31.0) / 37.0);
        let backend = detected_v23_rabitq_backend().unwrap();
        let ranked = rank_v23_rabitq_rows(&query, &[0], &geometry, &rows, backend).unwrap();
        assert_eq!(ranked.len(), 4_096);

        let evidence = select_v23_rabitq_pages(V23RaBitQEvalRequest {
            query_ordinal: 7,
            query: &query,
            ranked_leaf_ordinals: &[0],
            geometry: &geometry,
            rows: &rows,
            backend,
        })
        .unwrap();
        assert_eq!(evidence.query_ordinal, 7);
        assert_eq!(evidence.scored_rows, 5_000);
        assert_eq!(evidence.retained_rows, 4_096);
        assert_eq!(evidence.page_assignments, 8_192);
        assert_eq!(evidence.page_ordinals.len(), 8);
        assert!(
            evidence
                .page_ordinals
                .windows(2)
                .all(|pair| pair[0] < pair[1])
        );
        assert!(evidence.scalar_pages_equal);

        let (too_large_geometry, too_large_rows) = fixture(262_145, 16);
        assert!(
            rank_v23_rabitq_rows(&query, &[0], &too_large_geometry, &too_large_rows, backend,)
                .is_err()
        );
    }

    #[test]
    fn v23_rabitq_eval_cover_is_permutation_independent_and_rejects_nonfinite() {
        let rotation = identity_rotation();
        let mut geometry = V23RaBitQGeometry {
            leaf_offsets: (0..=16).map(|value| value * 2).collect(),
            centroids: vec![[f16::ZERO; 96]; 16],
            rotation,
        };
        let (_, rows) = fixture(32, 16);
        let query = std::array::from_fn(|ordinal| (ordinal as f32 + 1.0) / 101.0);
        let backend = detected_v23_rabitq_backend().unwrap();
        let forward = (0..16).collect::<Vec<u16>>();
        let mut reverse = forward.clone();
        reverse.reverse();
        let left = select_v23_rabitq_pages(V23RaBitQEvalRequest {
            query_ordinal: 0,
            query: &query,
            ranked_leaf_ordinals: &forward,
            geometry: &geometry,
            rows: &rows,
            backend,
        })
        .unwrap();
        let right = select_v23_rabitq_pages(V23RaBitQEvalRequest {
            ranked_leaf_ordinals: &reverse,
            ..V23RaBitQEvalRequest {
                query_ordinal: 0,
                query: &query,
                ranked_leaf_ordinals: &forward,
                geometry: &geometry,
                rows: &rows,
                backend,
            }
        })
        .unwrap();
        assert_eq!(left.page_ordinals, right.page_ordinals);

        let mut invalid = query;
        invalid[0] = f32::NAN;
        assert!(
            select_v23_rabitq_pages(V23RaBitQEvalRequest {
                query_ordinal: 0,
                query: &invalid,
                ranked_leaf_ordinals: &forward,
                geometry: &geometry,
                rows: &rows,
                backend,
            })
            .is_err()
        );
        geometry.leaf_offsets[16] = 31;
        assert!(rank_v23_rabitq_rows(&query, &forward, &geometry, &rows, backend).is_err());
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
            classify_v23_rabitq_controls(&accepted).unwrap(),
            V23RaBitQClassification::DevelopmentCandidateAccepted
        );

        let mut cells = accepted.clone();
        cells[0].total_hits = 317;
        assert_eq!(
            classify_v23_rabitq_controls(&cells).unwrap(),
            V23RaBitQClassification::AuthorityStop
        );
        let mut cells = accepted.clone();
        cells[1..4].iter_mut().for_each(|cell| cell.passed = false);
        assert_eq!(
            classify_v23_rabitq_controls(&cells).unwrap(),
            V23RaBitQClassification::TreePruningRejected
        );
        let mut cells = accepted.clone();
        cells[4].passed = false;
        assert_eq!(
            classify_v23_rabitq_controls(&cells).unwrap(),
            V23RaBitQClassification::RaBitQRepresentationRejected
        );
        let mut cells = accepted;
        cells[5..].iter_mut().for_each(|cell| cell.passed = false);
        assert_eq!(
            classify_v23_rabitq_controls(&cells).unwrap(),
            V23RaBitQClassification::TreeRaBitQCompositionRejected
        );
    }

    #[test]
    fn v23_rabitq_screen_canonical_result_recomputes_every_quality_and_authority_field() {
        let expected = screen();
        let bytes = canonical_v23_rabitq_screen_result_bytes(&expected, &expected.inputs).unwrap();
        assert_eq!(bytes.last(), Some(&b'\n'));
        assert_eq!(
            bytes,
            canonical_v23_rabitq_screen_result_bytes(&expected, &expected.inputs).unwrap()
        );

        let mut mutations = Vec::new();
        let mut changed = expected.clone();
        changed.cells[0].samples[0].page_ordinals.swap(0, 1);
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
        changed.classification = V23RaBitQClassification::AuthorityStop;
        mutations.push(changed);
        let mut changed = expected.clone();
        changed.inputs[0].sha256 = "3".repeat(64);
        mutations.push(changed);
        let mut changed = expected.clone();
        changed.claim_eligible = true;
        mutations.push(changed);
        for mutation in mutations {
            assert!(canonical_v23_rabitq_screen_result_bytes(&mutation, &expected.inputs).is_err());
        }
    }
}
