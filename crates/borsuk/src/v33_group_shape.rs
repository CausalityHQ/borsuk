use std::{collections::BTreeSet, io::Cursor, sync::Arc};

#[cfg(test)]
use std::collections::BTreeMap;

use arrow_array::{
    ArrayRef, BooleanArray, FixedSizeListArray, Float32Array, RecordBatch, UInt8Array, UInt32Array,
    UInt64Array,
};
use arrow_ipc::{
    MetadataVersion,
    reader::FileReader,
    writer::{FileWriter, IpcWriteOptions},
};
use arrow_schema::{DataType, Field, Schema};
use nalgebra::{DMatrix, SymmetricEigen};
use rayon::prelude::*;
use sha2::{Digest, Sha256};

use crate::{
    BorsukError, Result, V27HierarchyArtifacts, decode_v27_hierarchy,
    v30_s3_layout::{V30LayoutArtifacts, decode_v30_layout_artifacts},
    v30_s3_pq::{
        V30CodePlanes, V30PqArtifacts, V30PqCodebook, V30PqReconstructor, V30PqWidth,
        decode_v30_pq_artifacts,
    },
};

const DIMENSIONS: usize = 96;

fn invalid(message: &str) -> BorsukError {
    BorsukError::InvalidStorage(message.to_owned())
}

#[derive(Debug, Clone, PartialEq)]
struct V33LeafPopulation {
    routing_leaf_ordinal: u32,
    group_ordinal: u32,
    rows: Vec<(u64, [f32; DIMENSIONS])>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct V33RoutingRange {
    routing_leaf_ordinal: u32,
    code_parent_leaf_ordinal: u32,
    logical_start: u64,
    row_count: u64,
}

fn reconstruct_v33_leaf_populations(
    base_codebook: &V30PqCodebook,
    high_codebook: &V30PqCodebook,
    codes: &V30CodePlanes,
    code_parent_centers: &[[f32; DIMENSIONS]],
    ranges: &[V33RoutingRange],
    group_of_code_parent: &[u32],
) -> Result<Vec<V33LeafPopulation>> {
    if base_codebook.width() != V30PqWidth::Base24
        || high_codebook.width() != V30PqWidth::High48
        || ranges.is_empty()
        || code_parent_centers.is_empty()
        || code_parent_centers.len() != group_of_code_parent.len()
        || code_parent_centers
            .iter()
            .flatten()
            .any(|value| !value.is_finite())
    {
        return Err(invalid("V33 PQ reconstruction authority differs"));
    }
    let base = V30PqReconstructor::new(base_codebook)?;
    let high = V30PqReconstructor::new(high_codebook)?;
    let mut logical_start = 0_u64;
    let mut populations = Vec::with_capacity(ranges.len());
    for (expected_ordinal, range) in ranges.iter().enumerate() {
        if range.routing_leaf_ordinal != expected_ordinal as u32
            || range.logical_start != logical_start
            || range.row_count == 0
        {
            return Err(invalid("V33 routing range authority differs"));
        }
        let parent = usize::try_from(range.code_parent_leaf_ordinal)
            .map_err(|_| invalid("V33 code parent ordinal overflows"))?;
        let center = code_parent_centers
            .get(parent)
            .ok_or_else(|| invalid("V33 code parent ordinal differs"))?;
        let group_ordinal = *group_of_code_parent
            .get(parent)
            .ok_or_else(|| invalid("V33 code parent group differs"))?;
        let end = range
            .logical_start
            .checked_add(range.row_count)
            .ok_or_else(|| invalid("V33 routing range overflows"))?;
        let mut rows = Vec::with_capacity(
            usize::try_from(range.row_count)
                .map_err(|_| invalid("V33 routing population overflows"))?,
        );
        for logical in range.logical_start..end {
            let logical_index =
                usize::try_from(logical).map_err(|_| invalid("V33 logical ordinal overflows"))?;
            let (width, code) = codes.code(logical_index)?;
            let residual = match width {
                V30PqWidth::Base24 => base.reconstruct(code)?,
                V30PqWidth::High48 => high.reconstruct(code)?,
            };
            let mut reconstructed = [0.0_f32; DIMENSIONS];
            for dimension in 0..DIMENSIONS {
                reconstructed[dimension] = center[dimension] + residual[dimension];
            }
            if reconstructed.iter().any(|value| !value.is_finite()) {
                return Err(invalid("V33 reconstructed row is nonfinite"));
            }
            rows.push((logical, reconstructed));
        }
        populations.push(V33LeafPopulation {
            routing_leaf_ordinal: range.routing_leaf_ordinal,
            group_ordinal,
            rows,
        });
        logical_start = end;
    }
    if logical_start != codes.logical_rows() as u64
        || codes.materialized_rows() != codes.logical_rows()
    {
        return Err(invalid("V33 reconstructed logical coverage differs"));
    }
    Ok(populations)
}

#[derive(Debug, Clone, PartialEq)]
struct V33LeafShape {
    routing_leaf_ordinal: u32,
    group_ordinal: u32,
    logical_start: u64,
    population: u64,
    mean: [f32; DIMENSIONS],
    diagonal_variance: [f32; DIMENSIONS],
    scalar_moment: f32,
    maximum_radius: f32,
    split_dimension: usize,
    split_centers: [[f32; DIMENSIONS]; 2],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg(test)]
enum V33ShapeArm {
    Centroid,
    ScalarMoment,
    DiagonalMoment,
    SplitCentroid,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg(test)]
struct V33GroupPopulation {
    ordinal: u32,
    rows: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg(test)]
struct V33ShapeControlBytes {
    scalar_summary_bytes: usize,
    scalar_extra_centers: usize,
    scalar_padding_bytes: usize,
    diagonal_summary_bytes: usize,
    diagonal_control_bytes: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Authenticated Arrow IPC output of the query-independent V33 shape builder.
pub struct V33LeafShapeArtifact {
    /// Stable semantic role.
    pub role: &'static str,
    /// SHA-256 of `arrow`.
    pub sha256: String,
    /// Complete Arrow IPC length.
    pub encoded_bytes: u64,
    /// Number of routing-leaf summaries.
    pub row_count: u64,
    /// Complete deterministic Arrow IPC payload.
    pub arrow: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Complete local authenticated input for the query-independent V33 shape builder.
pub struct V33GroupShapeBuildRequest {
    /// Frozen V27 hierarchy artifacts.
    pub hierarchy: V27HierarchyArtifacts,
    /// Frozen V30 logical layout artifacts.
    pub layout: V30LayoutArtifacts,
    /// Frozen five-role V30 PQ artifacts.
    pub pq: V30PqArtifacts,
    /// Storage-group ordinal for every code-parent ordinal.
    pub group_of_code_parent: Vec<u32>,
    /// Number of largest non-singleton leaves receiving a matched-byte second center.
    pub scalar_split_count: usize,
}

#[derive(Debug, Clone, PartialEq)]
struct V33ReconstructedGroup {
    ordinal: u32,
    rows: Vec<(u64, [f32; DIMENSIONS])>,
}

#[derive(Debug, Clone, PartialEq)]
#[doc(hidden)]
/// Immutable query-free reconstructed-row oracle for mechanism diagnostics.
pub struct V33ReconstructedGroupOracle {
    groups: Vec<V33ReconstructedGroup>,
}

#[derive(Debug, Clone, PartialEq)]
#[doc(hidden)]
/// Bound input for the canonical reconstructed-row oracle receipt.
pub struct V33ReconstructedOracleRequest {
    /// Registered SHA-256 of the immutable V33 frontier.
    pub frontier_sha256: String,
    /// Registered byte length of the immutable V33 frontier.
    pub frontier_bytes: u64,
    /// Frozen query ordinal.
    pub query_ordinal: u64,
    /// Frozen normalized query vector.
    pub query: [f32; DIMENSIONS],
    /// Ten frozen truth logical ordinals, retaining duplicates by owner group.
    pub truth_logicals: Vec<u64>,
    /// Exact row population indexed by dense group ordinal.
    pub group_rows: Vec<u64>,
    /// Longest-prefix row ceiling.
    pub row_limit: u64,
    /// Longest-prefix group ceiling.
    pub group_limit: usize,
}

#[derive(Debug, Clone, PartialEq)]
struct V33FullCovarianceSummary {
    ordinal: u32,
    population: u64,
    mean: [f64; DIMENSIONS],
    covariance: Box<[f64]>,
    trace: f64,
    trace_square: f64,
}

#[derive(Debug, Clone, PartialEq)]
struct V33FullCovarianceGroup {
    ordinal: u32,
    population: u64,
    leaves: Vec<V33FullCovarianceSummary>,
}

#[derive(Debug, Clone, PartialEq)]
#[doc(hidden)]
/// Query-independent complete-covariance ceiling over reconstructed groups.
pub struct V33FullCovarianceCeiling {
    groups: Vec<V33FullCovarianceGroup>,
    logical_groups: Vec<u32>,
}

#[derive(Debug, Clone, PartialEq)]
#[doc(hidden)]
/// Bound inputs for the canonical complete-covariance ceiling receipt.
pub struct V33FullCovarianceCeilingRequest {
    /// Registered SHA-256 of the immutable V33 frontier.
    pub frontier_sha256: String,
    /// Registered byte length of the immutable V33 frontier.
    pub frontier_bytes: u64,
    /// Strictly increasing frozen query ordinals.
    pub query_ordinals: Vec<u64>,
    /// Frozen normalized query vectors in query-ordinal order.
    pub queries: Vec<[f32; DIMENSIONS]>,
    /// Ten frozen truth logical ordinals for every query.
    pub truth_logicals: Vec<Vec<u64>>,
    /// Exact row population indexed by dense group ordinal.
    pub group_rows: Vec<u64>,
    /// Longest-prefix row ceiling.
    pub row_limit: u64,
    /// Longest-prefix group ceiling.
    pub group_limit: usize,
}

#[derive(Debug, Clone, PartialEq)]
struct V33LowRankCovarianceSummary {
    ordinal: u32,
    group_ordinal: u32,
    logical_start: u64,
    population: u64,
    mean: [f32; DIMENSIONS],
    diagonal: [f32; DIMENSIONS],
    directions: [[f32; DIMENSIONS]; 4],
    eigenvalues: [f32; 4],
    residuals: [[f32; DIMENSIONS]; 3],
    ranks: [usize; 3],
}

#[derive(Debug, Clone, PartialEq)]
struct V33LowRankCovarianceGroup {
    ordinal: u32,
    population: u64,
    leaves: Vec<V33LowRankCovarianceSummary>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct V33LowRankCovarianceArtifact {
    sha256: String,
    encoded_bytes: u64,
    row_count: u64,
    arrow: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct V33Rank4LeafSnapshot {
    pub(crate) ordinal: u32,
    pub(crate) group_ordinal: u32,
    pub(crate) logical_start: u64,
    pub(crate) population: u64,
    pub(crate) mean: [f32; DIMENSIONS],
    pub(crate) residual: [f32; DIMENSIONS],
    pub(crate) eigenvalues: [f32; 4],
    pub(crate) directions: [[f32; DIMENSIONS]; 4],
}

#[derive(Debug, Clone, PartialEq)]
#[doc(hidden)]
/// Nested f32 rank-one/two/four covariance diagnostics over reconstructed leaves.
pub struct V33LowRankCovarianceLadder {
    groups: Vec<V33LowRankCovarianceGroup>,
    logical_groups: Vec<u32>,
    artifact_arrow: Vec<u8>,
    artifact_sha256: String,
    artifact_encoded_bytes: u64,
}

impl V33LowRankCovarianceLadder {
    /// Return the authenticated uncompressed Arrow IPC summary artifact.
    pub fn artifact_arrow(&self) -> &[u8] {
        &self.artifact_arrow
    }

    /// Return the SHA-256 authority for the Arrow IPC summary artifact.
    pub fn artifact_sha256(&self) -> &str {
        &self.artifact_sha256
    }

    /// Return the exact byte length of the Arrow IPC summary artifact.
    pub fn artifact_encoded_bytes(&self) -> u64 {
        self.artifact_encoded_bytes
    }
}

#[derive(Debug, Clone, PartialEq)]
#[doc(hidden)]
/// Bound inputs for the canonical low-rank covariance ladder receipt.
pub struct V33LowRankCovarianceLadderRequest {
    /// Registered SHA-256 of the immutable V33 frontier.
    pub frontier_sha256: String,
    /// Registered byte length of the immutable V33 frontier.
    pub frontier_bytes: u64,
    /// Strictly increasing frozen query ordinals.
    pub query_ordinals: Vec<u64>,
    /// Frozen normalized query vectors in query-ordinal order.
    pub queries: Vec<[f32; DIMENSIONS]>,
    /// Ten frozen truth logical ordinals for every query.
    pub truth_logicals: Vec<Vec<u64>>,
    /// Exact row population indexed by dense group ordinal.
    pub group_rows: Vec<u64>,
    /// Longest-prefix row ceiling.
    pub row_limit: u64,
    /// Longest-prefix group ceiling.
    pub group_limit: usize,
}

fn reconstruct_v33_request(request: &V33GroupShapeBuildRequest) -> Result<Vec<V33LeafPopulation>> {
    let hierarchy = decode_v27_hierarchy(
        &request.hierarchy.roots,
        &request.hierarchy.roots_bytes,
        &request.hierarchy.leaves,
        &request.hierarchy.leaves_bytes,
    )?;
    let layout = decode_v30_layout_artifacts(&request.layout)?;
    let (base_codebook, high_codebook, codes) = decode_v30_pq_artifacts(&request.pq)?.into_parts();
    if request.group_of_code_parent.len() != hierarchy.leaves.len()
        || request.scalar_split_count == 0
        || layout.source_rows() != codes.logical_rows() as u64
    {
        return Err(invalid("V33 shape build authority differs"));
    }
    let code_parent_centers = hierarchy
        .leaves
        .iter()
        .map(|center| center.map(f32::from))
        .collect::<Vec<_>>();
    let ranges = layout
        .leaves()
        .iter()
        .map(|range| V33RoutingRange {
            routing_leaf_ordinal: range.leaf_ordinal,
            code_parent_leaf_ordinal: range.code_parent_leaf_ordinal,
            logical_start: range.logical_start,
            row_count: range.row_count,
        })
        .collect::<Vec<_>>();
    reconstruct_v33_leaf_populations(
        &base_codebook,
        &high_codebook,
        &codes,
        &code_parent_centers,
        &ranges,
        &request.group_of_code_parent,
    )
}

/// Authenticate the inputs, reconstruct rows, and encode frozen V33 leaf summaries.
pub fn build_v33_group_shape_artifact(
    request: &V33GroupShapeBuildRequest,
) -> Result<V33LeafShapeArtifact> {
    let populations = reconstruct_v33_request(request)?;
    let mut scalar_split_leaves =
        select_v33_scalar_split_leaves(&populations, request.scalar_split_count)?;
    scalar_split_leaves.sort_unstable();
    let summaries = populations
        .iter()
        .map(summarize_v33_leaf)
        .collect::<Result<Vec<_>>>()?;
    encode_v33_leaf_shape_artifact(&summaries, &scalar_split_leaves)
}

/// Build the exact reconstructed-row diagnostic before any query is available.
#[doc(hidden)]
pub fn build_v33_reconstructed_group_oracle(
    request: &V33GroupShapeBuildRequest,
) -> Result<V33ReconstructedGroupOracle> {
    let populations = reconstruct_v33_request(request)?;
    let group_ordinals = populations
        .iter()
        .map(|population| population.group_ordinal)
        .collect::<BTreeSet<_>>();
    let group_count = group_ordinals
        .last()
        .and_then(|ordinal| usize::try_from(*ordinal).ok())
        .and_then(|ordinal| ordinal.checked_add(1))
        .ok_or_else(|| invalid("V33 reconstructed oracle group authority differs"))?;
    let group_count_u32 = u32::try_from(group_count)
        .map_err(|_| invalid("V33 reconstructed oracle group authority differs"))?;
    if group_ordinals.iter().copied().ne(0..group_count_u32) {
        return Err(invalid("V33 reconstructed oracle group authority differs"));
    }
    let mut groups = (0..group_count)
        .map(|ordinal| V33ReconstructedGroup {
            ordinal: ordinal as u32,
            rows: Vec::new(),
        })
        .collect::<Vec<_>>();
    for population in populations {
        let group_index = usize::try_from(population.group_ordinal)
            .map_err(|_| invalid("V33 reconstructed oracle group authority differs"))?;
        groups[group_index].rows.extend(population.rows);
    }
    if groups.iter().any(|group| group.rows.is_empty()) {
        return Err(invalid("V33 reconstructed oracle group population differs"));
    }
    Ok(V33ReconstructedGroupOracle { groups })
}

fn summarize_full_covariance_rows(
    ordinal: u32,
    rows: &[(u64, [f32; DIMENSIONS])],
) -> Result<V33FullCovarianceSummary> {
    if rows.is_empty()
        || rows
            .iter()
            .any(|(_, row)| row.iter().any(|value| !value.is_finite()))
        || rows
            .iter()
            .map(|(logical, _)| *logical)
            .collect::<BTreeSet<_>>()
            .len()
            != rows.len()
    {
        return Err(invalid("V33 full covariance population differs"));
    }
    let population = rows.len() as u64;
    let count = population as f64;
    let mut mean = [0.0_f64; DIMENSIONS];
    for (_, row) in rows {
        for dimension in 0..DIMENSIONS {
            mean[dimension] += f64::from(row[dimension]);
        }
    }
    for value in &mut mean {
        *value /= count;
    }

    let mut covariance = vec![0.0_f64; DIMENSIONS * DIMENSIONS];
    for (_, row) in rows {
        let delta = std::array::from_fn::<_, DIMENSIONS, _>(|dimension| {
            f64::from(row[dimension]) - mean[dimension]
        });
        for left in 0..DIMENSIONS {
            for right in left..DIMENSIONS {
                covariance[left * DIMENSIONS + right] += delta[left] * delta[right] / count;
            }
        }
    }
    for left in 0..DIMENSIONS {
        for right in 0..left {
            covariance[left * DIMENSIONS + right] = covariance[right * DIMENSIONS + left];
        }
    }
    if mean
        .iter()
        .chain(&covariance)
        .any(|value| !value.is_finite())
    {
        return Err(invalid("V33 full covariance is nonfinite"));
    }
    let trace = (0..DIMENSIONS)
        .map(|dimension| covariance[dimension * DIMENSIONS + dimension])
        .sum::<f64>();
    let trace_square = covariance.iter().map(|value| value * value).sum::<f64>();
    if !trace.is_finite() || trace < 0.0 || !trace_square.is_finite() || trace_square < 0.0 {
        return Err(invalid("V33 full covariance moments differ"));
    }
    Ok(V33FullCovarianceSummary {
        ordinal,
        population,
        mean,
        covariance: covariance.into_boxed_slice(),
        trace,
        trace_square,
    })
}

#[cfg(test)]
fn summarize_full_covariance(population: &V33LeafPopulation) -> Result<V33FullCovarianceSummary> {
    summarize_full_covariance_rows(population.group_ordinal, &population.rows)
}

fn full_covariance_moment_score(
    summary: &V33FullCovarianceSummary,
    query: &[f32; DIMENSIONS],
) -> Result<f64> {
    if summary.population == 0
        || summary.covariance.len() != DIMENSIONS * DIMENSIONS
        || query.iter().any(|value| !value.is_finite())
    {
        return Err(invalid("V33 full covariance score authority differs"));
    }
    let delta = std::array::from_fn::<_, DIMENSIONS, _>(|dimension| {
        f64::from(query[dimension]) - summary.mean[dimension]
    });
    let squared_mean_distance = delta.iter().map(|value| value * value).sum::<f64>();
    let mut delta_covariance_delta = 0.0_f64;
    for left in 0..DIMENSIONS {
        let projected = (0..DIMENSIONS)
            .map(|right| summary.covariance[left * DIMENSIONS + right] * delta[right])
            .sum::<f64>();
        delta_covariance_delta += delta[left] * projected;
    }
    let variance = 2.0 * summary.trace_square + 4.0 * delta_covariance_delta;
    let tolerance = (summary.trace_square * 1.0e-12).max(1.0e-15);
    if !squared_mean_distance.is_finite()
        || !delta_covariance_delta.is_finite()
        || !variance.is_finite()
        || variance < -tolerance
    {
        return Err(invalid("V33 full covariance score is nonfinite"));
    }
    let extreme = (2.0 * (summary.population as f64).ln()).sqrt();
    let score = squared_mean_distance + summary.trace - extreme * variance.max(0.0).sqrt();
    if !score.is_finite() {
        return Err(invalid("V33 full covariance score is nonfinite"));
    }
    Ok(score)
}

/// Build complete reconstructed-group covariance before opening any query.
#[doc(hidden)]
pub fn build_v33_full_covariance_ceiling(
    request: &V33GroupShapeBuildRequest,
) -> Result<V33FullCovarianceCeiling> {
    build_full_covariance_ceiling_from_populations(reconstruct_v33_request(request)?)
}

fn build_full_covariance_ceiling_from_populations(
    populations: Vec<V33LeafPopulation>,
) -> Result<V33FullCovarianceCeiling> {
    if populations.is_empty()
        || populations
            .iter()
            .enumerate()
            .any(|(ordinal, population)| population.routing_leaf_ordinal != ordinal as u32)
    {
        return Err(invalid("V33 full covariance leaf authority differs"));
    }
    let logical_count = populations
        .iter()
        .try_fold(0_usize, |count, leaf| count.checked_add(leaf.rows.len()))
        .ok_or_else(|| invalid("V33 full covariance logical coverage overflows"))?;
    let mut logical_groups = vec![u32::MAX; logical_count];
    for leaf in &populations {
        for (logical, _) in &leaf.rows {
            let logical = usize::try_from(*logical)
                .map_err(|_| invalid("V33 full covariance logical ordinal overflows"))?;
            let owner = logical_groups
                .get_mut(logical)
                .ok_or_else(|| invalid("V33 full covariance logical coverage differs"))?;
            if *owner != u32::MAX {
                return Err(invalid("V33 full covariance logical ownership differs"));
            }
            *owner = leaf.group_ordinal;
        }
    }
    if logical_groups.contains(&u32::MAX) {
        return Err(invalid("V33 full covariance logical coverage differs"));
    }
    let summaries = populations
        .par_iter()
        .map(|leaf| summarize_full_covariance_rows(leaf.routing_leaf_ordinal, &leaf.rows))
        .collect::<Result<Vec<_>>>()?;
    let group_count = populations
        .iter()
        .map(|leaf| leaf.group_ordinal)
        .max()
        .and_then(|ordinal| usize::try_from(ordinal).ok())
        .and_then(|ordinal| ordinal.checked_add(1))
        .ok_or_else(|| invalid("V33 full covariance group authority differs"))?;
    let mut groups = (0..group_count)
        .map(|ordinal| V33FullCovarianceGroup {
            ordinal: ordinal as u32,
            population: 0,
            leaves: Vec::new(),
        })
        .collect::<Vec<_>>();
    for (leaf, summary) in populations.iter().zip(summaries) {
        let group = groups
            .get_mut(
                usize::try_from(leaf.group_ordinal)
                    .map_err(|_| invalid("V33 full covariance group ordinal overflows"))?,
            )
            .ok_or_else(|| invalid("V33 full covariance group authority differs"))?;
        group.population = group
            .population
            .checked_add(summary.population)
            .ok_or_else(|| invalid("V33 full covariance group population overflows"))?;
        group.leaves.push(summary);
    }
    if groups
        .iter()
        .enumerate()
        .any(|(ordinal, group)| group.ordinal != ordinal as u32 || group.leaves.is_empty())
    {
        return Err(invalid("V33 full covariance group coverage differs"));
    }
    Ok(V33FullCovarianceCeiling {
        groups,
        logical_groups,
    })
}

fn full_covariance_group_score(
    group: &V33FullCovarianceGroup,
    query: &[f32; DIMENSIONS],
) -> Result<f64> {
    let mut score = f64::INFINITY;
    for leaf in &group.leaves {
        score = score.min(full_covariance_moment_score(leaf, query)?);
    }
    if !score.is_finite() {
        return Err(invalid("V33 full covariance group score differs"));
    }
    Ok(score)
}

/// Rank complete-covariance group summaries with ordinal tie breaking.
#[doc(hidden)]
pub fn rank_v33_full_covariance_groups(
    ceiling: &V33FullCovarianceCeiling,
    query: &[f32; DIMENSIONS],
) -> Result<Vec<u32>> {
    if ceiling.groups.is_empty() || query.iter().any(|value| !value.is_finite()) {
        return Err(invalid("V33 full covariance query differs"));
    }
    let mut ranked = ceiling
        .groups
        .iter()
        .map(|group| Ok((full_covariance_group_score(group, query)?, group.ordinal)))
        .collect::<Result<Vec<_>>>()?;
    ranked.sort_by(|left, right| {
        left.0
            .total_cmp(&right.0)
            .then_with(|| left.1.cmp(&right.1))
    });
    Ok(ranked.into_iter().map(|(_, ordinal)| ordinal).collect())
}

fn summarize_low_rank_covariance(
    population: &V33LeafPopulation,
) -> Result<V33LowRankCovarianceSummary> {
    if population.rows.is_empty() {
        return Err(invalid("V33 low-rank population differs"));
    }
    let count = population.rows.len() as f64;
    let mut mean64 = [0.0_f64; DIMENSIONS];
    for (_, row) in &population.rows {
        for dimension in 0..DIMENSIONS {
            mean64[dimension] += f64::from(row[dimension]);
        }
    }
    for value in &mut mean64 {
        *value /= count;
    }
    let mut diagonal64 = [0.0_f64; DIMENSIONS];
    for (_, row) in &population.rows {
        for dimension in 0..DIMENSIONS {
            let delta = f64::from(row[dimension]) - mean64[dimension];
            diagonal64[dimension] += delta * delta / count;
        }
    }
    let mean = mean64.map(|value| value as f32);
    let diagonal = diagonal64.map(|value| value as f32);
    let (directions, eigenvalues, _) = principal_covariance_components(population, 4)?;
    let directions: [[f32; DIMENSIONS]; 4] = directions
        .try_into()
        .map_err(|_| invalid("V33 low-rank direction count differs"))?;
    let eigenvalues: [f32; 4] = eigenvalues
        .try_into()
        .map_err(|_| invalid("V33 low-rank eigenvalue count differs"))?;
    let ranks = [1_usize, 2, 4];
    let mut residuals = [[0.0_f32; DIMENSIONS]; 3];
    for (arm, rank) in ranks.into_iter().enumerate() {
        for dimension in 0..DIMENSIONS {
            let explained = (0..rank)
                .map(|component| {
                    f64::from(eigenvalues[component])
                        * f64::from(directions[component][dimension]).powi(2)
                })
                .sum::<f64>();
            let residual = f64::from(diagonal[dimension]) - explained;
            let tolerance = (f64::from(diagonal[dimension]).abs() * 1.0e-5).max(1.0e-7);
            if !residual.is_finite() || residual < -tolerance {
                return Err(invalid("V33 low-rank residual diagonal differs"));
            }
            residuals[arm][dimension] = residual.max(0.0) as f32;
        }
    }
    Ok(V33LowRankCovarianceSummary {
        ordinal: population.routing_leaf_ordinal,
        group_ordinal: population.group_ordinal,
        logical_start: population
            .rows
            .iter()
            .map(|(ordinal, _)| *ordinal)
            .min()
            .unwrap(),
        population: population.rows.len() as u64,
        mean,
        diagonal,
        directions,
        eigenvalues,
        residuals,
        ranks,
    })
}

fn v33_low_rank_covariance_schema() -> Schema {
    let vector = || {
        DataType::FixedSizeList(
            Arc::new(Field::new("element", DataType::Float32, false)),
            DIMENSIONS as i32,
        )
    };
    let eigenvalues =
        DataType::FixedSizeList(Arc::new(Field::new("element", DataType::Float32, false)), 4);
    Schema::new(vec![
        Field::new("routing_leaf_ordinal", DataType::UInt32, false),
        Field::new("group_ordinal", DataType::UInt32, false),
        Field::new("logical_start", DataType::UInt64, false),
        Field::new("population", DataType::UInt64, false),
        Field::new("mean", vector(), false),
        Field::new("diagonal", vector(), false),
        Field::new("direction_1", vector(), false),
        Field::new("direction_2", vector(), false),
        Field::new("direction_3", vector(), false),
        Field::new("direction_4", vector(), false),
        Field::new("eigenvalues", eigenvalues, false),
        Field::new("residual_rank_1", vector(), false),
        Field::new("residual_rank_2", vector(), false),
        Field::new("residual_rank_4", vector(), false),
    ])
}

fn encode_v33_low_rank_covariance_artifact(
    groups: &[V33LowRankCovarianceGroup],
) -> Result<V33LowRankCovarianceArtifact> {
    let mut leaves = groups
        .iter()
        .flat_map(|group| group.leaves.iter())
        .collect::<Vec<_>>();
    leaves.sort_by_key(|leaf| leaf.ordinal);
    if leaves.is_empty()
        || leaves
            .iter()
            .enumerate()
            .any(|(ordinal, leaf)| leaf.ordinal != ordinal as u32)
    {
        return Err(invalid(
            "V33 low-rank covariance artifact authority differs",
        ));
    }
    let schema = v33_low_rank_covariance_schema();
    let eigenvalue_values = Arc::new(Float32Array::from_iter_values(
        leaves
            .iter()
            .flat_map(|leaf| leaf.eigenvalues.iter().copied()),
    ));
    let eigenvalues = Arc::new(FixedSizeListArray::try_new(
        Arc::new(Field::new("element", DataType::Float32, false)),
        4,
        eigenvalue_values,
        None,
    )?);
    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(UInt32Array::from_iter_values(
                leaves.iter().map(|leaf| leaf.ordinal),
            )),
            Arc::new(UInt32Array::from_iter_values(
                leaves.iter().map(|leaf| leaf.group_ordinal),
            )),
            Arc::new(UInt64Array::from_iter_values(
                leaves.iter().map(|leaf| leaf.logical_start),
            )),
            Arc::new(UInt64Array::from_iter_values(
                leaves.iter().map(|leaf| leaf.population),
            )),
            v33_vector_array(leaves.iter().map(|leaf| &leaf.mean))?,
            v33_vector_array(leaves.iter().map(|leaf| &leaf.diagonal))?,
            v33_vector_array(leaves.iter().map(|leaf| &leaf.directions[0]))?,
            v33_vector_array(leaves.iter().map(|leaf| &leaf.directions[1]))?,
            v33_vector_array(leaves.iter().map(|leaf| &leaf.directions[2]))?,
            v33_vector_array(leaves.iter().map(|leaf| &leaf.directions[3]))?,
            eigenvalues,
            v33_vector_array(leaves.iter().map(|leaf| &leaf.residuals[0]))?,
            v33_vector_array(leaves.iter().map(|leaf| &leaf.residuals[1]))?,
            v33_vector_array(leaves.iter().map(|leaf| &leaf.residuals[2]))?,
        ],
    )?;
    let mut arrow = Vec::new();
    let options = IpcWriteOptions::try_new(8, false, MetadataVersion::V5)?;
    let mut writer = FileWriter::try_new_with_options(&mut arrow, &schema, options)?;
    writer.write(&batch)?;
    writer.finish()?;
    drop(writer);
    Ok(V33LowRankCovarianceArtifact {
        sha256: format!("{:x}", Sha256::digest(&arrow)),
        encoded_bytes: arrow.len() as u64,
        row_count: leaves.len() as u64,
        arrow,
    })
}

fn v33_low_rank_vector(
    batch: &RecordBatch,
    column: usize,
    row: usize,
) -> Result<[f32; DIMENSIONS]> {
    let list = batch
        .column(column)
        .as_any()
        .downcast_ref::<FixedSizeListArray>()
        .ok_or_else(|| invalid("V33 low-rank covariance vector differs"))?;
    let values = list
        .value(row)
        .as_any()
        .downcast_ref::<Float32Array>()
        .ok_or_else(|| invalid("V33 low-rank covariance vector differs"))?
        .values()
        .to_vec();
    let vector: [f32; DIMENSIONS] = values
        .try_into()
        .map_err(|_| invalid("V33 low-rank covariance dimension differs"))?;
    if vector.iter().any(|value| !value.is_finite()) {
        return Err(invalid("V33 low-rank covariance vector is nonfinite"));
    }
    Ok(vector)
}

fn decode_v33_low_rank_covariance_artifact(
    artifact: &V33LowRankCovarianceArtifact,
) -> Result<V33LowRankCovarianceLadder> {
    if artifact.sha256.len() != 64
        || artifact.encoded_bytes != artifact.arrow.len() as u64
        || artifact.row_count == 0
        || format!("{:x}", Sha256::digest(&artifact.arrow)) != artifact.sha256
    {
        return Err(invalid("V33 low-rank covariance artifact identity differs"));
    }
    let mut reader = FileReader::try_new(Cursor::new(&artifact.arrow), None)?;
    if reader.schema().as_ref() != &v33_low_rank_covariance_schema() {
        return Err(invalid("V33 low-rank covariance artifact schema differs"));
    }
    let batch = reader
        .next()
        .transpose()?
        .ok_or_else(|| invalid("V33 low-rank covariance artifact batch differs"))?;
    if reader.next().is_some()
        || batch.num_rows() as u64 != artifact.row_count
        || batch
            .columns()
            .iter()
            .any(|column| column.null_count() != 0)
    {
        return Err(invalid("V33 low-rank covariance artifact rows differ"));
    }
    let u32_column = |column: usize| {
        batch
            .column(column)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or_else(|| invalid("V33 low-rank covariance integer differs"))
    };
    let u64_column = |column: usize| {
        batch
            .column(column)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| invalid("V33 low-rank covariance integer differs"))
    };
    let ordinals = u32_column(0)?;
    let group_ordinals = u32_column(1)?;
    let logical_starts = u64_column(2)?;
    let populations = u64_column(3)?;
    let eigenvalue_lists = batch
        .column(10)
        .as_any()
        .downcast_ref::<FixedSizeListArray>()
        .ok_or_else(|| invalid("V33 low-rank covariance eigenvalues differ"))?;
    let mut summaries = Vec::with_capacity(batch.num_rows());
    let mut logical_start = 0_u64;
    let mut logical_groups = Vec::new();
    for row in 0..batch.num_rows() {
        let ordinal = ordinals.value(row);
        let group_ordinal = group_ordinals.value(row);
        let population = populations.value(row);
        if ordinal != row as u32 || logical_starts.value(row) != logical_start || population == 0 {
            return Err(invalid("V33 low-rank covariance artifact ordering differs"));
        }
        let eigenvalue_values = eigenvalue_lists.value(row);
        let eigenvalue_values = eigenvalue_values
            .as_any()
            .downcast_ref::<Float32Array>()
            .ok_or_else(|| invalid("V33 low-rank covariance eigenvalues differ"))?
            .values()
            .to_vec();
        let eigenvalues: [f32; 4] = eigenvalue_values
            .try_into()
            .map_err(|_| invalid("V33 low-rank covariance eigenvalue count differs"))?;
        let directions = [
            v33_low_rank_vector(&batch, 6, row)?,
            v33_low_rank_vector(&batch, 7, row)?,
            v33_low_rank_vector(&batch, 8, row)?,
            v33_low_rank_vector(&batch, 9, row)?,
        ];
        let residuals = [
            v33_low_rank_vector(&batch, 11, row)?,
            v33_low_rank_vector(&batch, 12, row)?,
            v33_low_rank_vector(&batch, 13, row)?,
        ];
        summaries.push(V33LowRankCovarianceSummary {
            ordinal,
            group_ordinal,
            logical_start,
            population,
            mean: v33_low_rank_vector(&batch, 4, row)?,
            diagonal: v33_low_rank_vector(&batch, 5, row)?,
            directions,
            eigenvalues,
            residuals,
            ranks: [1, 2, 4],
        });
        logical_groups.extend(std::iter::repeat_n(group_ordinal, population as usize));
        logical_start = logical_start
            .checked_add(population)
            .ok_or_else(|| invalid("V33 low-rank covariance logical coverage overflows"))?;
    }
    let group_count = summaries
        .iter()
        .map(|leaf| leaf.group_ordinal)
        .max()
        .and_then(|ordinal| usize::try_from(ordinal).ok())
        .and_then(|ordinal| ordinal.checked_add(1))
        .ok_or_else(|| invalid("V33 low-rank covariance group authority differs"))?;
    let mut groups = (0..group_count)
        .map(|ordinal| V33LowRankCovarianceGroup {
            ordinal: ordinal as u32,
            population: 0,
            leaves: Vec::new(),
        })
        .collect::<Vec<_>>();
    for summary in summaries {
        let group = groups
            .get_mut(summary.group_ordinal as usize)
            .ok_or_else(|| invalid("V33 low-rank covariance group authority differs"))?;
        group.population = group
            .population
            .checked_add(summary.population)
            .ok_or_else(|| invalid("V33 low-rank covariance group population overflows"))?;
        group.leaves.push(summary);
    }
    let ladder = V33LowRankCovarianceLadder {
        groups,
        logical_groups,
        artifact_arrow: artifact.arrow.clone(),
        artifact_sha256: artifact.sha256.clone(),
        artifact_encoded_bytes: artifact.encoded_bytes,
    };
    validate_v33_low_rank_covariance_ladder(&ladder)?;
    Ok(ladder)
}

fn build_low_rank_covariance_ladder_from_populations(
    populations: Vec<V33LeafPopulation>,
) -> Result<V33LowRankCovarianceLadder> {
    if populations.is_empty()
        || populations
            .iter()
            .enumerate()
            .any(|(ordinal, population)| population.routing_leaf_ordinal != ordinal as u32)
    {
        return Err(invalid("V33 low-rank leaf authority differs"));
    }
    let logical_count = populations
        .iter()
        .try_fold(0_usize, |count, leaf| count.checked_add(leaf.rows.len()))
        .ok_or_else(|| invalid("V33 low-rank logical coverage overflows"))?;
    let mut logical_groups = vec![u32::MAX; logical_count];
    for leaf in &populations {
        for (logical, _) in &leaf.rows {
            let logical = usize::try_from(*logical)
                .map_err(|_| invalid("V33 low-rank logical ordinal overflows"))?;
            let owner = logical_groups
                .get_mut(logical)
                .ok_or_else(|| invalid("V33 low-rank logical coverage differs"))?;
            if *owner != u32::MAX {
                return Err(invalid("V33 low-rank logical ownership differs"));
            }
            *owner = leaf.group_ordinal;
        }
    }
    if logical_groups.contains(&u32::MAX) {
        return Err(invalid("V33 low-rank logical coverage differs"));
    }
    let summaries = populations
        .par_iter()
        .map(summarize_low_rank_covariance)
        .collect::<Result<Vec<_>>>()?;
    let group_count = populations
        .iter()
        .map(|leaf| leaf.group_ordinal)
        .max()
        .and_then(|ordinal| usize::try_from(ordinal).ok())
        .and_then(|ordinal| ordinal.checked_add(1))
        .ok_or_else(|| invalid("V33 low-rank group authority differs"))?;
    let mut groups = (0..group_count)
        .map(|ordinal| V33LowRankCovarianceGroup {
            ordinal: ordinal as u32,
            population: 0,
            leaves: Vec::new(),
        })
        .collect::<Vec<_>>();
    for (leaf, summary) in populations.iter().zip(summaries) {
        let group = groups
            .get_mut(
                usize::try_from(leaf.group_ordinal)
                    .map_err(|_| invalid("V33 low-rank group ordinal overflows"))?,
            )
            .ok_or_else(|| invalid("V33 low-rank group authority differs"))?;
        group.population = group
            .population
            .checked_add(summary.population)
            .ok_or_else(|| invalid("V33 low-rank group population overflows"))?;
        group.leaves.push(summary);
    }
    if groups
        .iter()
        .enumerate()
        .any(|(ordinal, group)| group.ordinal != ordinal as u32 || group.leaves.is_empty())
    {
        return Err(invalid("V33 low-rank group coverage differs"));
    }
    let artifact = encode_v33_low_rank_covariance_artifact(&groups)?;
    let ladder = decode_v33_low_rank_covariance_artifact(&artifact)?;
    if ladder.logical_groups != logical_groups {
        return Err(invalid(
            "V33 low-rank covariance persisted ownership differs",
        ));
    }
    Ok(ladder)
}

pub(crate) fn build_v33_rank4_leaf_snapshots(
    request: &V33GroupShapeBuildRequest,
) -> Result<Vec<V33Rank4LeafSnapshot>> {
    let ladder = build_v33_low_rank_covariance_ladder(request)?;
    let mut leaves = ladder
        .groups
        .iter()
        .flat_map(|group| group.leaves.iter())
        .map(|leaf| V33Rank4LeafSnapshot {
            ordinal: leaf.ordinal,
            group_ordinal: leaf.group_ordinal,
            logical_start: leaf.logical_start,
            population: leaf.population,
            mean: leaf.mean,
            residual: leaf.residuals[2],
            eigenvalues: leaf.eigenvalues,
            directions: leaf.directions,
        })
        .collect::<Vec<_>>();
    leaves.sort_by_key(|leaf| leaf.ordinal);
    Ok(leaves)
}

fn validate_v33_low_rank_covariance_ladder(ladder: &V33LowRankCovarianceLadder) -> Result<()> {
    if ladder.groups.is_empty()
        || ladder.logical_groups.is_empty()
        || ladder.artifact_arrow.is_empty()
        || ladder.artifact_sha256.len() != 64
        || ladder.artifact_encoded_bytes != ladder.artifact_arrow.len() as u64
        || format!("{:x}", Sha256::digest(&ladder.artifact_arrow)) != ladder.artifact_sha256
    {
        return Err(invalid("V33 low-rank covariance authority differs"));
    }
    let mut leaf_ordinals = BTreeSet::new();
    for (group_ordinal, group) in ladder.groups.iter().enumerate() {
        if group.ordinal != group_ordinal as u32
            || group.population == 0
            || group.leaves.is_empty()
            || group
                .leaves
                .windows(2)
                .any(|pair| pair[0].ordinal >= pair[1].ordinal)
            || group
                .leaves
                .iter()
                .try_fold(0_u64, |total, leaf| total.checked_add(leaf.population))
                != Some(group.population)
        {
            return Err(invalid("V33 low-rank covariance group authority differs"));
        }
        for leaf in &group.leaves {
            if !leaf_ordinals.insert(leaf.ordinal)
                || leaf.group_ordinal != group.ordinal
                || leaf.population == 0
                || leaf.ranks != [1, 2, 4]
                || leaf.mean.iter().any(|value| !value.is_finite())
                || leaf
                    .diagonal
                    .iter()
                    .any(|value| !value.is_finite() || *value < 0.0)
                || leaf
                    .directions
                    .iter()
                    .flatten()
                    .any(|value| !value.is_finite())
                || leaf
                    .eigenvalues
                    .iter()
                    .any(|value| !value.is_finite() || *value < 0.0)
                || leaf
                    .residuals
                    .iter()
                    .flatten()
                    .any(|value| !value.is_finite() || *value < 0.0)
            {
                return Err(invalid("V33 low-rank covariance leaf authority differs"));
            }
            if leaf.eigenvalues.windows(2).any(|pair| pair[0] < pair[1]) {
                return Err(invalid("V33 low-rank covariance eigenvalue order differs"));
            }
            for component in 0..4 {
                let norm = leaf.directions[component]
                    .iter()
                    .map(|value| f64::from(*value).powi(2))
                    .sum::<f64>();
                let mut sign_dimension = 0_usize;
                let mut sign_magnitude = 0.0_f32;
                for (dimension, value) in leaf.directions[component].iter().enumerate() {
                    if value.abs() > sign_magnitude {
                        sign_dimension = dimension;
                        sign_magnitude = value.abs();
                    }
                }
                let zero = leaf.eigenvalues[component] == 0.0;
                if (zero && (norm != 0.0 || sign_magnitude != 0.0))
                    || (!zero && (norm - 1.0).abs() > 2.0e-5)
                    || (sign_magnitude > 0.0
                        && leaf.directions[component][sign_dimension].is_sign_negative())
                {
                    return Err(invalid(
                        "V33 low-rank covariance component authority differs",
                    ));
                }
            }
            for (arm, rank) in leaf.ranks.into_iter().enumerate() {
                for dimension in 0..DIMENSIONS {
                    let reconstructed = f64::from(leaf.residuals[arm][dimension])
                        + (0..rank)
                            .map(|component| {
                                f64::from(leaf.eigenvalues[component])
                                    * f64::from(leaf.directions[component][dimension]).powi(2)
                            })
                            .sum::<f64>();
                    let diagonal = f64::from(leaf.diagonal[dimension]);
                    let tolerance = (diagonal.abs() * 2.0e-5).max(2.0e-7);
                    if (reconstructed - diagonal).abs() > tolerance {
                        return Err(invalid(
                            "V33 low-rank covariance diagonal authority differs",
                        ));
                    }
                }
            }
        }
    }
    if leaf_ordinals
        .iter()
        .copied()
        .ne((0..leaf_ordinals.len()).map(|ordinal| ordinal as u32))
        || ladder
            .logical_groups
            .iter()
            .any(|group| usize::try_from(*group).map_or(true, |group| group >= ladder.groups.len()))
    {
        return Err(invalid("V33 low-rank covariance coverage differs"));
    }
    let mut leaves = ladder
        .groups
        .iter()
        .flat_map(|group| group.leaves.iter())
        .collect::<Vec<_>>();
    leaves.sort_by_key(|leaf| leaf.ordinal);
    let mut logical_start = 0_u64;
    for leaf in leaves {
        let end = logical_start
            .checked_add(leaf.population)
            .ok_or_else(|| invalid("V33 low-rank covariance logical coverage overflows"))?;
        if leaf.logical_start != logical_start
            || ladder
                .logical_groups
                .get(logical_start as usize..end as usize)
                .is_none_or(|groups| groups.iter().any(|group| *group != leaf.group_ordinal))
        {
            return Err(invalid("V33 low-rank covariance logical authority differs"));
        }
        logical_start = end;
    }
    if logical_start as usize != ladder.logical_groups.len() {
        return Err(invalid("V33 low-rank covariance logical coverage differs"));
    }
    Ok(())
}

/// Build nested rank-one/two/four summaries before opening any query.
#[doc(hidden)]
pub fn build_v33_low_rank_covariance_ladder(
    request: &V33GroupShapeBuildRequest,
) -> Result<V33LowRankCovarianceLadder> {
    build_low_rank_covariance_ladder_from_populations(reconstruct_v33_request(request)?)
}

/// Rank low-rank covariance groups for one fixed diagnostic rank.
#[doc(hidden)]
pub fn rank_v33_low_rank_covariance_groups(
    ladder: &V33LowRankCovarianceLadder,
    rank: usize,
    query: &[f32; DIMENSIONS],
) -> Result<Vec<u32>> {
    validate_v33_low_rank_covariance_ladder(ladder)?;
    let arm = [1_usize, 2, 4]
        .iter()
        .position(|candidate| *candidate == rank)
        .ok_or_else(|| invalid("V33 low-rank diagnostic rank differs"))?;
    if ladder.groups.is_empty() || query.iter().any(|value| !value.is_finite()) {
        return Err(invalid("V33 low-rank query differs"));
    }
    let mut ranked = ladder
        .groups
        .iter()
        .map(|group| {
            let mut score = f64::INFINITY;
            for leaf in &group.leaves {
                score = score.min(low_rank_moment_score(
                    &leaf.mean,
                    &leaf.residuals[arm],
                    &leaf.directions[..rank],
                    &leaf.eigenvalues[..rank],
                    leaf.population,
                    query,
                )?);
            }
            score
                .is_finite()
                .then_some((score, group.ordinal))
                .ok_or_else(|| invalid("V33 low-rank group score differs"))
        })
        .collect::<Result<Vec<_>>>()?;
    ranked.sort_by(|left, right| {
        left.0
            .total_cmp(&right.0)
            .then_with(|| left.1.cmp(&right.1))
    });
    Ok(ranked.into_iter().map(|(_, ordinal)| ordinal).collect())
}

/// Rank groups by their minimum exact reconstructed-row squared distance.
#[doc(hidden)]
pub fn rank_v33_reconstructed_groups(
    oracle: &V33ReconstructedGroupOracle,
    query: &[f32; DIMENSIONS],
) -> Result<Vec<u32>> {
    if oracle.groups.is_empty() || query.iter().any(|value| !value.is_finite()) {
        return Err(invalid("V33 reconstructed oracle query differs"));
    }
    let mut ranked = Vec::with_capacity(oracle.groups.len());
    for group in &oracle.groups {
        let mut score = f64::INFINITY;
        for (_, row) in &group.rows {
            score = score.min(squared_distance(row, query)?);
        }
        if !score.is_finite() {
            return Err(invalid("V33 reconstructed oracle group population differs"));
        }
        ranked.push((score, group.ordinal));
    }
    ranked.sort_by(|left, right| {
        left.0
            .total_cmp(&right.0)
            .then_with(|| left.1.cmp(&right.1))
    });
    Ok(ranked.into_iter().map(|(_, ordinal)| ordinal).collect())
}

/// Resolve an authenticated logical row to its reconstructed storage group.
#[doc(hidden)]
pub fn v33_reconstructed_group_for_logical(
    oracle: &V33ReconstructedGroupOracle,
    logical_ordinal: u64,
) -> Result<u32> {
    let mut owner = None;
    for group in &oracle.groups {
        if group
            .rows
            .iter()
            .any(|(logical, _)| *logical == logical_ordinal)
            && owner.replace(group.ordinal).is_some()
        {
            return Err(invalid("V33 reconstructed logical ownership differs"));
        }
    }
    owner.ok_or_else(|| invalid("V33 reconstructed logical ownership differs"))
}

/// Recompute and serialize the query-6160 reconstructed-row oracle bracket.
#[doc(hidden)]
pub fn canonical_v33_reconstructed_oracle_result_bytes(
    oracle: &V33ReconstructedGroupOracle,
    request: &V33ReconstructedOracleRequest,
) -> Result<Vec<u8>> {
    if request.frontier_sha256.len() != 64
        || request
            .frontier_sha256
            .bytes()
            .any(|byte| !byte.is_ascii_digit() && !(b'a'..=b'f').contains(&byte))
        || request.frontier_bytes == 0
        || request.query_ordinal != 6_160
        || request.query.iter().any(|value| !value.is_finite())
        || request.truth_logicals.len() != 10
        || request.group_rows.len() != oracle.groups.len()
        || request.group_rows.contains(&0)
        || request.row_limit == 0
        || request.group_limit == 0
    {
        return Err(invalid("V33 reconstructed oracle request differs"));
    }
    for (ordinal, group) in oracle.groups.iter().enumerate() {
        if group.ordinal != ordinal as u32 || request.group_rows[ordinal] != group.rows.len() as u64
        {
            return Err(invalid("V33 reconstructed oracle population differs"));
        }
    }

    let ranked = rank_v33_reconstructed_groups(oracle, &request.query)?;
    let required_groups = request
        .truth_logicals
        .iter()
        .map(|logical| v33_reconstructed_group_for_logical(oracle, *logical))
        .collect::<Result<Vec<_>>>()?;
    let required_group_ranks = required_groups
        .iter()
        .map(|owner| {
            ranked
                .iter()
                .position(|group| group == owner)
                .and_then(|rank| rank.checked_add(1))
                .ok_or_else(|| invalid("V33 reconstructed oracle owner rank differs"))
        })
        .collect::<Result<Vec<_>>>()?;
    let mut selected_groups = Vec::new();
    let mut selected_rows = 0_u64;
    for group in ranked.iter().copied() {
        let rows = request.group_rows[usize::try_from(group)
            .map_err(|_| invalid("V33 reconstructed oracle rank differs"))?];
        let next = selected_rows
            .checked_add(rows)
            .ok_or_else(|| invalid("V33 reconstructed oracle selected rows overflow"))?;
        if selected_groups.len() == request.group_limit || next > request.row_limit {
            break;
        }
        selected_groups.push(group);
        selected_rows = next;
    }
    if selected_groups.is_empty() {
        return Err(invalid("V33 reconstructed oracle prefix differs"));
    }
    let selected = selected_groups.iter().copied().collect::<BTreeSet<_>>();
    let all_required_selected = required_groups.iter().all(|group| selected.contains(group));
    let value = serde_json::json!({
        "all_required_selected": all_required_selected,
        "claim_eligible": false,
        "frontier": {
            "encoded_bytes": request.frontier_bytes,
            "role": "v33-group-proxy-result-json",
            "sha256": request.frontier_sha256,
        },
        "group_limit": request.group_limit,
        "query_ordinal": request.query_ordinal,
        "required_group_ranks": required_group_ranks,
        "required_groups": required_groups,
        "row_limit": request.row_limit,
        "schema": "borsuk-v33-reconstructed-row-oracle-result-v1",
        "selected_groups": selected_groups,
        "selected_rows": selected_rows,
    });
    let mut bytes = serde_json::to_vec(&value)
        .map_err(|_| invalid("V33 reconstructed oracle serialization differs"))?;
    bytes.push(b'\n');
    Ok(bytes)
}

/// Recompute every burned-cohort owner and serialize the complete-covariance ceiling.
#[doc(hidden)]
pub fn canonical_v33_full_covariance_ceiling_result_bytes(
    ceiling: &V33FullCovarianceCeiling,
    request: &V33FullCovarianceCeilingRequest,
) -> Result<Vec<u8>> {
    let query_count = request.query_ordinals.len();
    if request.frontier_sha256.len() != 64
        || request
            .frontier_sha256
            .bytes()
            .any(|byte| !byte.is_ascii_digit() && !(b'a'..=b'f').contains(&byte))
        || request.frontier_bytes == 0
        || query_count == 0
        || request.queries.len() != query_count
        || request.truth_logicals.len() != query_count
        || request
            .query_ordinals
            .windows(2)
            .any(|pair| pair[0] >= pair[1])
        || request
            .queries
            .iter()
            .flatten()
            .any(|value| !value.is_finite())
        || request.truth_logicals.iter().any(|truth| truth.len() != 10)
        || request.group_rows.len() != ceiling.groups.len()
        || request.group_rows.contains(&0)
        || request.row_limit == 0
        || request.group_limit == 0
    {
        return Err(invalid("V33 full covariance ceiling request differs"));
    }
    for (ordinal, group) in ceiling.groups.iter().enumerate() {
        if group.ordinal != ordinal as u32 || group.population != request.group_rows[ordinal] {
            return Err(invalid("V33 full covariance ceiling population differs"));
        }
    }

    let mut records = Vec::with_capacity(query_count);
    let mut included_owners = 0_usize;
    let mut perfect_queries = 0_usize;
    let mut minimum_selected_rows = u64::MAX;
    let mut maximum_selected_rows = 0_u64;
    for index in 0..query_count {
        let ranked = rank_v33_full_covariance_groups(ceiling, &request.queries[index])?;
        let mut selected_groups = Vec::new();
        let mut selected_rows = 0_u64;
        for group in ranked.iter().copied() {
            let group_index = usize::try_from(group)
                .map_err(|_| invalid("V33 full covariance rank overflows"))?;
            let rows = *request
                .group_rows
                .get(group_index)
                .ok_or_else(|| invalid("V33 full covariance rank differs"))?;
            let next = selected_rows
                .checked_add(rows)
                .ok_or_else(|| invalid("V33 full covariance selected rows overflow"))?;
            if selected_groups.len() == request.group_limit || next > request.row_limit {
                break;
            }
            selected_groups.push(group);
            selected_rows = next;
        }
        if selected_groups.is_empty() {
            return Err(invalid("V33 full covariance prefix differs"));
        }
        minimum_selected_rows = minimum_selected_rows.min(selected_rows);
        maximum_selected_rows = maximum_selected_rows.max(selected_rows);
        let selected = selected_groups.iter().copied().collect::<BTreeSet<_>>();
        let truth_groups = request.truth_logicals[index]
            .iter()
            .map(|logical| {
                let logical = usize::try_from(*logical)
                    .map_err(|_| invalid("V33 full covariance truth ordinal overflows"))?;
                ceiling
                    .logical_groups
                    .get(logical)
                    .copied()
                    .ok_or_else(|| invalid("V33 full covariance truth ordinal differs"))
            })
            .collect::<Result<Vec<_>>>()?;
        let truth_owner_ranks = truth_groups
            .iter()
            .map(|owner| {
                ranked
                    .iter()
                    .position(|group| group == owner)
                    .and_then(|rank| rank.checked_add(1))
                    .ok_or_else(|| invalid("V33 full covariance owner rank differs"))
            })
            .collect::<Result<Vec<_>>>()?;
        let hits = truth_groups
            .iter()
            .filter(|group| selected.contains(group))
            .count();
        included_owners += hits;
        if hits == 10 {
            perfect_queries += 1;
        }
        records.push(serde_json::json!({
            "hits": hits,
            "query_ordinal": request.query_ordinals[index],
            "selected_groups": selected_groups,
            "selected_rows": selected_rows,
            "truth_owner_ranks": truth_owner_ranks,
        }));
    }
    let total_owners = query_count
        .checked_mul(10)
        .ok_or_else(|| invalid("V33 full covariance owner count overflows"))?;
    let passed = included_owners == total_owners && perfect_queries == query_count;
    let value = serde_json::json!({
        "claim_eligible": false,
        "frontier": {
            "encoded_bytes": request.frontier_bytes,
            "role": "v33-group-proxy-result-json",
            "sha256": request.frontier_sha256,
        },
        "group_limit": request.group_limit,
        "included_owners": included_owners,
        "maximum_selected_rows": maximum_selected_rows,
        "minimum_selected_rows": minimum_selected_rows,
        "passed": passed,
        "perfect_queries": perfect_queries,
        "query_count": query_count,
        "records": records,
        "row_limit": request.row_limit,
        "schema": "borsuk-v33-full-covariance-ceiling-result-v1",
        "total_owners": total_owners,
    });
    let mut bytes = serde_json::to_vec(&value)
        .map_err(|_| invalid("V33 full covariance ceiling serialization differs"))?;
    bytes.push(b'\n');
    Ok(bytes)
}

fn rank_v33_fine_leaf_centroid_groups(
    ladder: &V33LowRankCovarianceLadder,
    query: &[f32; DIMENSIONS],
) -> Result<Vec<u32>> {
    validate_v33_low_rank_covariance_ladder(ladder)?;
    if query.iter().any(|value| !value.is_finite()) {
        return Err(invalid("V33 fine-leaf centroid query differs"));
    }
    let mut ranked = ladder
        .groups
        .iter()
        .map(|group| {
            let score = group
                .leaves
                .iter()
                .map(|leaf| squared_distance(&leaf.mean, query))
                .collect::<Result<Vec<_>>>()?
                .into_iter()
                .min_by(f64::total_cmp)
                .ok_or_else(|| invalid("V33 fine-leaf centroid group differs"))?;
            Ok((score, group.ordinal))
        })
        .collect::<Result<Vec<_>>>()?;
    ranked.sort_by(|left, right| {
        left.0
            .total_cmp(&right.0)
            .then_with(|| left.1.cmp(&right.1))
    });
    Ok(ranked.into_iter().map(|(_, ordinal)| ordinal).collect())
}

fn v33_nearest_rank(values: &[u64], numerator: usize) -> Result<u64> {
    if values.is_empty() || numerator == 0 || numerator > 100 {
        return Err(invalid("V33 covariance frontier percentile differs"));
    }
    let mut ordered = values.to_vec();
    ordered.sort_unstable();
    let rank = ordered
        .len()
        .checked_mul(numerator)
        .and_then(|value| value.checked_add(99))
        .map(|value| value / 100)
        .and_then(|value| value.checked_sub(1))
        .ok_or_else(|| invalid("V33 covariance frontier percentile overflows"))?;
    Ok(ordered[rank])
}

fn v33_covariance_arm_value(
    ladder: &V33LowRankCovarianceLadder,
    request: &V33LowRankCovarianceLadderRequest,
    rank: Option<usize>,
) -> Result<serde_json::Value> {
    let query_count = request.query_ordinals.len();
    let mut records = Vec::with_capacity(query_count);
    let mut included_owners = 0_usize;
    let mut perfect_queries = 0_usize;
    let mut minimum_selected_rows = u64::MAX;
    let mut maximum_selected_rows = 0_u64;
    let mut required_rows = Vec::with_capacity(query_count);
    let mut required_groups = Vec::with_capacity(query_count);
    for index in 0..query_count {
        let ranked = if let Some(rank) = rank {
            rank_v33_low_rank_covariance_groups(ladder, rank, &request.queries[index])?
        } else {
            rank_v33_fine_leaf_centroid_groups(ladder, &request.queries[index])?
        };
        let mut selected_groups = Vec::new();
        let mut selected_rows = 0_u64;
        for group in ranked.iter().copied() {
            let group_index =
                usize::try_from(group).map_err(|_| invalid("V33 low-rank rank overflows"))?;
            let rows = *request
                .group_rows
                .get(group_index)
                .ok_or_else(|| invalid("V33 low-rank rank differs"))?;
            let next = selected_rows
                .checked_add(rows)
                .ok_or_else(|| invalid("V33 low-rank selected rows overflow"))?;
            if selected_groups.len() == request.group_limit || next > request.row_limit {
                break;
            }
            selected_groups.push(group);
            selected_rows = next;
        }
        if selected_groups.is_empty() {
            return Err(invalid("V33 low-rank prefix differs"));
        }
        minimum_selected_rows = minimum_selected_rows.min(selected_rows);
        maximum_selected_rows = maximum_selected_rows.max(selected_rows);
        let selected = selected_groups.iter().copied().collect::<BTreeSet<_>>();
        let truth_groups = request.truth_logicals[index]
            .iter()
            .map(|logical| {
                let logical = usize::try_from(*logical)
                    .map_err(|_| invalid("V33 low-rank truth ordinal overflows"))?;
                ladder
                    .logical_groups
                    .get(logical)
                    .copied()
                    .ok_or_else(|| invalid("V33 low-rank truth ordinal differs"))
            })
            .collect::<Result<Vec<_>>>()?;
        let truth_owner_ranks = truth_groups
            .iter()
            .map(|owner| {
                ranked
                    .iter()
                    .position(|group| group == owner)
                    .and_then(|position| position.checked_add(1))
                    .ok_or_else(|| invalid("V33 low-rank owner rank differs"))
            })
            .collect::<Result<Vec<_>>>()?;
        let required_group_count = *truth_owner_ranks
            .iter()
            .max()
            .ok_or_else(|| invalid("V33 covariance truth frontier differs"))?;
        let required_row_count =
            ranked
                .iter()
                .take(required_group_count)
                .try_fold(0_u64, |rows, group| {
                    rows.checked_add(request.group_rows[*group as usize])
                        .ok_or_else(|| invalid("V33 covariance truth frontier overflows"))
                })?;
        required_groups.push(required_group_count as u64);
        required_rows.push(required_row_count);
        let hits = truth_groups
            .iter()
            .filter(|group| selected.contains(group))
            .count();
        included_owners += hits;
        if hits == 10 {
            perfect_queries += 1;
        }
        records.push(serde_json::json!({
            "hits": hits,
            "query_ordinal": request.query_ordinals[index],
            "required_groups": required_group_count,
            "required_rows": required_row_count,
            "selected_groups": selected_groups,
            "selected_rows": selected_rows,
            "truth_owner_ranks": truth_owner_ranks,
        }));
    }
    let total_owners = query_count
        .checked_mul(10)
        .ok_or_else(|| invalid("V33 low-rank owner count overflows"))?;
    let coverage_passed = included_owners == total_owners && perfect_queries == query_count;
    Ok(serde_json::json!({
        "arm": rank.map_or("fine-leaf-centroid".to_owned(), |rank| format!("low-rank-{rank}")),
        "coverage_passed": coverage_passed,
        "included_owners": included_owners,
        "maximum_selected_rows": maximum_selected_rows,
        "minimum_selected_rows": minimum_selected_rows,
        "passed": coverage_passed,
        "perfect_queries": perfect_queries,
        "query_count": query_count,
        "rank": rank,
        "records": records,
        "required_groups_max": required_groups.iter().copied().max().unwrap(),
        "required_groups_p50": v33_nearest_rank(&required_groups, 50)?,
        "required_groups_p95": v33_nearest_rank(&required_groups, 95)?,
        "required_rows_max": required_rows.iter().copied().max().unwrap(),
        "required_rows_p50": v33_nearest_rank(&required_rows, 50)?,
        "required_rows_p95": v33_nearest_rank(&required_rows, 95)?,
        "total_owners": total_owners,
    }))
}

/// Recompute ranks one/two/four and serialize the fixed rank-two-primary receipt.
#[doc(hidden)]
pub fn canonical_v33_low_rank_covariance_ladder_result_bytes(
    ladder: &V33LowRankCovarianceLadder,
    request: &V33LowRankCovarianceLadderRequest,
) -> Result<Vec<u8>> {
    validate_v33_low_rank_covariance_ladder(ladder)?;
    let query_count = request.query_ordinals.len();
    if request.frontier_sha256.len() != 64
        || request
            .frontier_sha256
            .bytes()
            .any(|byte| !byte.is_ascii_digit() && !(b'a'..=b'f').contains(&byte))
        || request.frontier_bytes == 0
        || query_count == 0
        || request.queries.len() != query_count
        || request.truth_logicals.len() != query_count
        || request
            .query_ordinals
            .windows(2)
            .any(|pair| pair[0] >= pair[1])
        || request
            .queries
            .iter()
            .flatten()
            .any(|value| !value.is_finite())
        || request.truth_logicals.iter().any(|truth| truth.len() != 10)
        || request.group_rows.len() != ladder.groups.len()
        || request.group_rows.contains(&0)
        || request.row_limit == 0
        || request.group_limit == 0
    {
        return Err(invalid("V33 low-rank covariance ladder request differs"));
    }
    for (ordinal, group) in ladder.groups.iter().enumerate() {
        if group.ordinal != ordinal as u32 || group.population != request.group_rows[ordinal] {
            return Err(invalid("V33 low-rank covariance population differs"));
        }
    }
    let fine_leaf_centroid_control = v33_covariance_arm_value(ladder, request, None)?;
    let mut arms = [1_usize, 2, 4]
        .into_iter()
        .map(|rank| v33_covariance_arm_value(ladder, request, Some(rank)))
        .collect::<Result<Vec<_>>>()?;
    for arm in &mut arms {
        let non_worse = [
            "required_groups_p50",
            "required_groups_p95",
            "required_groups_max",
            "required_rows_p50",
            "required_rows_p95",
            "required_rows_max",
        ]
        .into_iter()
        .all(|field| {
            arm[field]
                .as_u64()
                .zip(fine_leaf_centroid_control[field].as_u64())
                .is_some_and(|(candidate, control)| candidate <= control)
        });
        let coverage_passed = arm["coverage_passed"]
            .as_bool()
            .ok_or_else(|| invalid("V33 low-rank coverage result differs"))?;
        let object = arm
            .as_object_mut()
            .ok_or_else(|| invalid("V33 low-rank arm result differs"))?;
        object.insert("frontier_non_worse".to_owned(), non_worse.into());
        object.insert("passed".to_owned(), (coverage_passed && non_worse).into());
    }
    let passed = arms[1]
        .get("passed")
        .and_then(serde_json::Value::as_bool)
        .ok_or_else(|| invalid("V33 low-rank primary result differs"))?;
    let value = serde_json::json!({
        "arms": arms,
        "claim_eligible": false,
        "frontier": {
            "encoded_bytes": request.frontier_bytes,
            "role": "v33-group-proxy-result-json",
            "sha256": request.frontier_sha256,
        },
        "fine_leaf_centroid_control": fine_leaf_centroid_control,
        "group_limit": request.group_limit,
        "low_rank_summary": {
            "encoded_bytes": ladder.artifact_encoded_bytes,
            "role": "v33-low-rank-covariance-arrow",
            "row_count": ladder.groups.iter().map(|group| group.leaves.len()).sum::<usize>(),
            "sha256": ladder.artifact_sha256,
        },
        "passed": passed,
        "primary_rank": 2,
        "row_limit": request.row_limit,
        "schema": "borsuk-v33-low-rank-covariance-ladder-result-v1",
    });
    let mut bytes = serde_json::to_vec(&value)
        .map_err(|_| invalid("V33 low-rank covariance serialization differs"))?;
    bytes.push(b'\n');
    Ok(bytes)
}

#[cfg(test)]
fn v33_shape_control_bytes(leaf_count: usize) -> Result<V33ShapeControlBytes> {
    if leaf_count == 0 {
        return Err(invalid("V33 shape leaf count differs"));
    }
    let center_bytes = DIMENSIONS * size_of::<f32>();
    let scalar_extra_bytes = leaf_count
        .checked_mul(size_of::<f32>())
        .ok_or_else(|| invalid("V33 scalar control bytes overflow"))?;
    let scalar_extra_centers = scalar_extra_bytes / center_bytes;
    let scalar_padding_bytes = scalar_extra_bytes % center_bytes;
    let scalar_summary_bytes = leaf_count
        .checked_mul(center_bytes + size_of::<f32>())
        .ok_or_else(|| invalid("V33 scalar summary bytes overflow"))?;
    let diagonal_summary_bytes = leaf_count
        .checked_mul(center_bytes * 2)
        .ok_or_else(|| invalid("V33 diagonal summary bytes overflow"))?;
    Ok(V33ShapeControlBytes {
        scalar_summary_bytes,
        scalar_extra_centers,
        scalar_padding_bytes,
        diagonal_summary_bytes,
        diagonal_control_bytes: diagonal_summary_bytes,
    })
}

fn summarize_v33_leaf(population: &V33LeafPopulation) -> Result<V33LeafShape> {
    if population.rows.is_empty()
        || population
            .rows
            .iter()
            .any(|(_, row)| row.iter().any(|value| !value.is_finite()))
        || population
            .rows
            .iter()
            .map(|(ordinal, _)| *ordinal)
            .collect::<BTreeSet<_>>()
            .len()
            != population.rows.len()
    {
        return Err(invalid("V33 leaf population differs"));
    }
    let count = population.rows.len() as f64;
    let mut mean64 = [0.0_f64; DIMENSIONS];
    for (_, row) in &population.rows {
        for dimension in 0..DIMENSIONS {
            mean64[dimension] += f64::from(row[dimension]);
        }
    }
    for value in &mut mean64 {
        *value /= count;
    }
    let mut variance64 = [0.0_f64; DIMENSIONS];
    for (_, row) in &population.rows {
        for dimension in 0..DIMENSIONS {
            let delta = f64::from(row[dimension]) - mean64[dimension];
            variance64[dimension] += delta * delta;
        }
    }
    for value in &mut variance64 {
        *value /= count;
    }
    let scalar64 = variance64.iter().sum::<f64>();
    let mut maximum_radius64 = 0.0_f64;
    for (_, row) in &population.rows {
        let mut squared = 0.0_f64;
        for dimension in 0..DIMENSIONS {
            let delta = f64::from(row[dimension]) - mean64[dimension];
            squared += delta * delta;
        }
        maximum_radius64 = maximum_radius64.max(squared.sqrt());
    }
    if mean64
        .iter()
        .chain(variance64.iter())
        .chain(std::iter::once(&scalar64))
        .chain(std::iter::once(&maximum_radius64))
        .any(|value| !value.is_finite())
    {
        return Err(invalid("V33 leaf moment is nonfinite"));
    }
    let split_dimension = variance64
        .iter()
        .enumerate()
        .max_by(|left, right| left.1.total_cmp(right.1).then_with(|| right.0.cmp(&left.0)))
        .unwrap()
        .0;
    let mut ordered = population.rows.iter().collect::<Vec<_>>();
    ordered.sort_by(|left, right| {
        left.1[split_dimension]
            .total_cmp(&right.1[split_dimension])
            .then_with(|| left.0.cmp(&right.0))
    });
    let mean = mean64.map(|value| value as f32);
    let diagonal_variance = variance64.map(|value| value as f32);
    let mut split_centers = [mean; 2];
    if ordered.len() > 1 {
        let cut = ordered.len() / 2;
        for (slot, rows) in [&ordered[..cut], &ordered[cut..]].into_iter().enumerate() {
            let mut center = [0.0_f64; DIMENSIONS];
            for (_, row) in rows {
                for dimension in 0..DIMENSIONS {
                    center[dimension] += f64::from(row[dimension]);
                }
            }
            for dimension in 0..DIMENSIONS {
                split_centers[slot][dimension] = (center[dimension] / rows.len() as f64) as f32;
            }
        }
    }
    Ok(V33LeafShape {
        routing_leaf_ordinal: population.routing_leaf_ordinal,
        group_ordinal: population.group_ordinal,
        logical_start: population
            .rows
            .iter()
            .map(|(ordinal, _)| *ordinal)
            .min()
            .unwrap(),
        population: population.rows.len() as u64,
        mean,
        diagonal_variance,
        scalar_moment: scalar64 as f32,
        maximum_radius: maximum_radius64 as f32,
        split_dimension,
        split_centers,
    })
}

fn v33_vector_array<'a>(vectors: impl Iterator<Item = &'a [f32; DIMENSIONS]>) -> Result<ArrayRef> {
    let values = Arc::new(Float32Array::from_iter_values(
        vectors.flat_map(|vector| vector.iter().copied()),
    ));
    Ok(Arc::new(FixedSizeListArray::try_new(
        Arc::new(Field::new("element", DataType::Float32, false)),
        DIMENSIONS as i32,
        values,
        None,
    )?))
}

fn encode_v33_leaf_shape_artifact(
    leaves: &[V33LeafShape],
    scalar_split_leaves: &[u32],
) -> Result<V33LeafShapeArtifact> {
    if leaves.is_empty()
        || leaves
            .iter()
            .enumerate()
            .any(|(ordinal, leaf)| leaf.routing_leaf_ordinal != ordinal as u32)
        || scalar_split_leaves.is_empty()
        || scalar_split_leaves
            .windows(2)
            .any(|pair| pair[0] >= pair[1])
        || scalar_split_leaves
            .iter()
            .any(|ordinal| usize::try_from(*ordinal).map_or(true, |index| index >= leaves.len()))
    {
        return Err(invalid("V33 leaf shape artifact authority differs"));
    }
    let selected = scalar_split_leaves.iter().copied().collect::<BTreeSet<_>>();
    let vector_field = || {
        DataType::FixedSizeList(
            Arc::new(Field::new("element", DataType::Float32, false)),
            DIMENSIONS as i32,
        )
    };
    let schema = Schema::new(vec![
        Field::new("routing_leaf_ordinal", DataType::UInt32, false),
        Field::new("group_ordinal", DataType::UInt32, false),
        Field::new("logical_start", DataType::UInt64, false),
        Field::new("population", DataType::UInt64, false),
        Field::new("mean", vector_field(), false),
        Field::new("diagonal_variance", vector_field(), false),
        Field::new("scalar_moment", DataType::Float32, false),
        Field::new("maximum_radius", DataType::Float32, false),
        Field::new("split_dimension", DataType::UInt8, false),
        Field::new("split_center_left", vector_field(), false),
        Field::new("split_center_right", vector_field(), false),
        Field::new("scalar_split_selected", DataType::Boolean, false),
    ]);
    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(UInt32Array::from_iter_values(
                leaves.iter().map(|leaf| leaf.routing_leaf_ordinal),
            )),
            Arc::new(UInt32Array::from_iter_values(
                leaves.iter().map(|leaf| leaf.group_ordinal),
            )),
            Arc::new(UInt64Array::from_iter_values(
                leaves.iter().map(|leaf| leaf.logical_start),
            )),
            Arc::new(UInt64Array::from_iter_values(
                leaves.iter().map(|leaf| leaf.population),
            )),
            v33_vector_array(leaves.iter().map(|leaf| &leaf.mean))?,
            v33_vector_array(leaves.iter().map(|leaf| &leaf.diagonal_variance))?,
            Arc::new(Float32Array::from_iter_values(
                leaves.iter().map(|leaf| leaf.scalar_moment),
            )),
            Arc::new(Float32Array::from_iter_values(
                leaves.iter().map(|leaf| leaf.maximum_radius),
            )),
            Arc::new(UInt8Array::from_iter_values(leaves.iter().map(|leaf| {
                u8::try_from(leaf.split_dimension).expect("V33 split dimension fits u8")
            }))),
            v33_vector_array(leaves.iter().map(|leaf| &leaf.split_centers[0]))?,
            v33_vector_array(leaves.iter().map(|leaf| &leaf.split_centers[1]))?,
            Arc::new(BooleanArray::from(
                leaves
                    .iter()
                    .map(|leaf| selected.contains(&leaf.routing_leaf_ordinal))
                    .collect::<Vec<_>>(),
            )),
        ],
    )?;
    let mut arrow = Vec::new();
    let options = IpcWriteOptions::try_new(8, false, MetadataVersion::V5)?;
    let mut writer = FileWriter::try_new_with_options(&mut arrow, &schema, options)?;
    writer.write(&batch)?;
    writer.finish()?;
    drop(writer);
    Ok(V33LeafShapeArtifact {
        role: "v33-leaf-shapes-arrow",
        sha256: format!("{:x}", Sha256::digest(&arrow)),
        encoded_bytes: arrow.len() as u64,
        row_count: leaves.len() as u64,
        arrow,
    })
}

fn select_v33_scalar_split_leaves(
    populations: &[V33LeafPopulation],
    additional_centers: usize,
) -> Result<Vec<u32>> {
    if additional_centers == 0
        || populations.is_empty()
        || populations
            .iter()
            .map(|leaf| leaf.routing_leaf_ordinal)
            .collect::<BTreeSet<_>>()
            .len()
            != populations.len()
    {
        return Err(invalid("V33 scalar split authority differs"));
    }
    let mut splittable = populations
        .iter()
        .filter(|leaf| leaf.rows.len() > 1)
        .map(|leaf| (leaf.rows.len(), leaf.routing_leaf_ordinal))
        .collect::<Vec<_>>();
    if splittable.len() < additional_centers {
        return Err(invalid("V33 scalar split population differs"));
    }
    splittable.sort_by(|left, right| right.0.cmp(&left.0).then_with(|| left.1.cmp(&right.1)));
    Ok(splittable
        .into_iter()
        .take(additional_centers)
        .map(|(_, ordinal)| ordinal)
        .collect())
}

type V33PrincipalComponents = (Vec<[f32; DIMENSIONS]>, Vec<f32>, [f32; DIMENSIONS]);

fn principal_covariance_components(
    population: &V33LeafPopulation,
    rank: usize,
) -> Result<V33PrincipalComponents> {
    if population.rows.is_empty()
        || rank == 0
        || rank > DIMENSIONS
        || population
            .rows
            .iter()
            .any(|(_, row)| row.iter().any(|value| !value.is_finite()))
        || population
            .rows
            .iter()
            .map(|(ordinal, _)| *ordinal)
            .collect::<BTreeSet<_>>()
            .len()
            != population.rows.len()
    {
        return Err(invalid("V33 low-rank population differs"));
    }

    let count = population.rows.len() as f64;
    let mut mean = [0.0_f64; DIMENSIONS];
    for (_, row) in &population.rows {
        for dimension in 0..DIMENSIONS {
            mean[dimension] += f64::from(row[dimension]);
        }
    }
    for value in &mut mean {
        *value /= count;
    }

    let mut covariance = vec![0.0_f64; DIMENSIONS * DIMENSIONS];
    for (_, row) in &population.rows {
        let delta = std::array::from_fn::<_, DIMENSIONS, _>(|dimension| {
            f64::from(row[dimension]) - mean[dimension]
        });
        for left in 0..DIMENSIONS {
            for right in 0..DIMENSIONS {
                covariance[left * DIMENSIONS + right] += delta[left] * delta[right] / count;
            }
        }
    }
    if covariance.iter().any(|value| !value.is_finite()) {
        return Err(invalid("V33 low-rank covariance is nonfinite"));
    }

    let trace = (0..DIMENSIONS)
        .map(|dimension| covariance[dimension * DIMENSIONS + dimension])
        .sum::<f64>();
    let tolerance = (trace * 1.0e-12).max(1.0e-15);
    let decomposition =
        SymmetricEigen::new(DMatrix::from_row_slice(DIMENSIONS, DIMENSIONS, &covariance));
    let mut order = (0..DIMENSIONS).collect::<Vec<_>>();
    order.sort_by(|left, right| {
        decomposition.eigenvalues[*right]
            .total_cmp(&decomposition.eigenvalues[*left])
            .then_with(|| left.cmp(right))
    });
    let mut ordered_values = Vec::with_capacity(DIMENSIONS);
    let mut ordered_vectors = Vec::<[f64; DIMENSIONS]>::with_capacity(DIMENSIONS);
    for index in order {
        let value = decomposition.eigenvalues[index];
        if !value.is_finite() || value < -tolerance {
            return Err(invalid("V33 low-rank covariance eigensystem differs"));
        }
        ordered_values.push(value.max(0.0));
        ordered_vectors.push(std::array::from_fn(|dimension| {
            decomposition.eigenvectors[(dimension, index)]
        }));
    }

    let mut directions64 = Vec::<[f64; DIMENSIONS]>::with_capacity(rank);
    let mut eigenvalues64 = Vec::<f64>::with_capacity(rank);
    let mut start = 0_usize;
    while directions64.len() < rank {
        if start == DIMENSIONS || ordered_values[start] <= tolerance {
            directions64.push([0.0; DIMENSIONS]);
            eigenvalues64.push(0.0);
            continue;
        }
        let mut end = start + 1;
        while end < DIMENSIONS && (ordered_values[end] - ordered_values[start]).abs() <= tolerance {
            end += 1;
        }
        let cluster_size = end - start;
        let cluster_value = ordered_values[start..end].iter().sum::<f64>() / cluster_size as f64;
        let mut canonical = Vec::<[f64; DIMENSIONS]>::with_capacity(cluster_size);
        for axis in 0..DIMENSIONS {
            let mut projected = [0.0_f64; DIMENSIONS];
            for basis in &ordered_vectors[start..end] {
                let coefficient = basis[axis];
                for dimension in 0..DIMENSIONS {
                    projected[dimension] += coefficient * basis[dimension];
                }
            }
            for previous in &canonical {
                let coefficient = projected
                    .iter()
                    .zip(previous)
                    .map(|(left, right)| left * right)
                    .sum::<f64>();
                for dimension in 0..DIMENSIONS {
                    projected[dimension] -= coefficient * previous[dimension];
                }
            }
            let norm = projected
                .iter()
                .map(|value| value * value)
                .sum::<f64>()
                .sqrt();
            if norm <= tolerance {
                continue;
            }
            for value in &mut projected {
                *value /= norm;
            }
            let mut sign_dimension = 0_usize;
            let mut sign_magnitude = 0.0_f64;
            for (dimension, value) in projected.iter().enumerate() {
                if value.abs() > sign_magnitude {
                    sign_dimension = dimension;
                    sign_magnitude = value.abs();
                }
            }
            if projected[sign_dimension].is_sign_negative() {
                for value in &mut projected {
                    *value = -*value;
                }
            }
            canonical.push(projected);
            if canonical.len() == cluster_size {
                break;
            }
        }
        if canonical.len() != cluster_size {
            return Err(invalid("V33 low-rank repeated eigenspace differs"));
        }
        for direction in canonical {
            if directions64.len() == rank {
                break;
            }
            directions64.push(direction);
            eigenvalues64.push(cluster_value);
        }
        start = end;
    }

    for dimension in 0..DIMENSIONS {
        let explained = directions64
            .iter()
            .zip(&eigenvalues64)
            .map(|(direction, eigenvalue)| eigenvalue * direction[dimension] * direction[dimension])
            .sum::<f64>();
        let diagonal = covariance[dimension * DIMENSIONS + dimension];
        let residual = diagonal - explained;
        let diagonal_tolerance = 1.0e-12 * diagonal.abs().max(1.0);
        if !residual.is_finite() || residual < -diagonal_tolerance {
            return Err(invalid("V33 low-rank covariance over-explains diagonal"));
        }
    }

    let directions = directions64
        .iter()
        .map(|direction| direction.map(|value| value as f32))
        .collect::<Vec<_>>();
    let eigenvalues = eigenvalues64
        .iter()
        .map(|value| *value as f32)
        .collect::<Vec<_>>();
    let residual = std::array::from_fn(|dimension| {
        let explained = directions
            .iter()
            .zip(&eigenvalues)
            .map(|(direction, eigenvalue)| {
                f64::from(*eigenvalue)
                    * f64::from(direction[dimension])
                    * f64::from(direction[dimension])
            })
            .sum::<f64>();
        (covariance[dimension * DIMENSIONS + dimension] - explained).max(0.0) as f32
    });
    Ok((directions, eigenvalues, residual))
}

/// Query-independent Gaussian distance-moment ranking heuristic for a
/// diagonal-plus-low-rank covariance. This is not a hard membership test or a
/// lower bound on exact vector distance.
fn low_rank_moment_score(
    mean: &[f32; DIMENSIONS],
    residual: &[f32; DIMENSIONS],
    directions: &[[f32; DIMENSIONS]],
    eigenvalues: &[f32],
    population: u64,
    query: &[f32; DIMENSIONS],
) -> Result<f64> {
    if population == 0
        || directions.len() != eigenvalues.len()
        || directions.len() > DIMENSIONS
        || mean.iter().chain(query).any(|value| !value.is_finite())
        || residual
            .iter()
            .any(|value| !value.is_finite() || *value < 0.0)
        || eigenvalues
            .iter()
            .any(|value| !value.is_finite() || *value < 0.0)
        || directions.iter().flatten().any(|value| !value.is_finite())
    {
        return Err(invalid("V33 low-rank score authority differs"));
    }

    let delta = std::array::from_fn::<_, DIMENSIONS, _>(|dimension| {
        f64::from(query[dimension]) - f64::from(mean[dimension])
    });
    let distance = delta.iter().map(|value| value * value).sum::<f64>();
    let mut trace = residual.iter().map(|value| f64::from(*value)).sum::<f64>();
    let mut trace_square = residual
        .iter()
        .map(|value| f64::from(*value).powi(2))
        .sum::<f64>();
    let mut directional = delta
        .iter()
        .zip(residual)
        .map(|(delta, variance)| delta * delta * f64::from(*variance))
        .sum::<f64>();

    for (index, (direction, eigenvalue)) in directions.iter().zip(eigenvalues).enumerate() {
        let eigenvalue = f64::from(*eigenvalue);
        let norm_square = direction
            .iter()
            .map(|value| f64::from(*value).powi(2))
            .sum::<f64>();
        let projected_delta = direction
            .iter()
            .zip(&delta)
            .map(|(left, right)| f64::from(*left) * right)
            .sum::<f64>();
        trace += eigenvalue * norm_square;
        directional += eigenvalue * projected_delta * projected_delta;
        trace_square += 2.0
            * eigenvalue
            * direction
                .iter()
                .zip(residual)
                .map(|(component, variance)| f64::from(*variance) * f64::from(*component).powi(2))
                .sum::<f64>();
        for (other_direction, other_eigenvalue) in
            directions.iter().zip(eigenvalues).take(index + 1)
        {
            let dot = direction
                .iter()
                .zip(other_direction)
                .map(|(left, right)| f64::from(*left) * f64::from(*right))
                .sum::<f64>();
            let product = eigenvalue * f64::from(*other_eigenvalue) * dot * dot;
            trace_square += if std::ptr::eq(direction, other_direction) {
                product
            } else {
                2.0 * product
            };
        }
    }
    let variance = 2.0 * trace_square + 4.0 * directional;
    let score = distance + trace - extreme_factor(population) * variance.max(0.0).sqrt();
    if !score.is_finite() {
        return Err(invalid("V33 low-rank score is nonfinite"));
    }
    Ok(if score == 0.0 { 0.0 } else { score })
}

fn squared_distance(left: &[f32; DIMENSIONS], right: &[f32; DIMENSIONS]) -> Result<f64> {
    let mut distance = 0.0_f64;
    for dimension in 0..DIMENSIONS {
        let delta = f64::from(left[dimension]) - f64::from(right[dimension]);
        distance += delta * delta;
    }
    if !distance.is_finite() {
        return Err(invalid("V33 shape distance is nonfinite"));
    }
    Ok(if distance == 0.0 { 0.0 } else { distance })
}

#[cfg(test)]
fn score_v33_leaf(leaf: &V33LeafShape, query: &[f32; DIMENSIONS], arm: V33ShapeArm) -> Result<f64> {
    if leaf.population == 0
        || query.iter().any(|value| !value.is_finite())
        || leaf.mean.iter().any(|value| !value.is_finite())
        || leaf
            .diagonal_variance
            .iter()
            .any(|value| !value.is_finite() || *value < 0.0)
        || !leaf.scalar_moment.is_finite()
        || leaf.scalar_moment < 0.0
    {
        return Err(invalid("V33 shape score authority differs"));
    }
    let distance = squared_distance(&leaf.mean, query)?;
    let score = match arm {
        V33ShapeArm::Centroid => distance,
        V33ShapeArm::SplitCentroid => squared_distance(&leaf.split_centers[0], query)?
            .min(squared_distance(&leaf.split_centers[1], query)?),
        V33ShapeArm::ScalarMoment => {
            let moment = f64::from(leaf.scalar_moment);
            let variance = 2.0 * moment * moment / DIMENSIONS as f64
                + 4.0 * moment * distance / DIMENSIONS as f64;
            distance + moment - extreme_factor(leaf.population) * variance.sqrt()
        }
        V33ShapeArm::DiagonalMoment => {
            let mut moment = 0.0_f64;
            let mut variance_square = 0.0_f64;
            let mut directional = 0.0_f64;
            for ((query_value, mean), variance) in
                query.iter().zip(&leaf.mean).zip(&leaf.diagonal_variance)
            {
                let variance = f64::from(*variance);
                let delta = f64::from(*query_value) - f64::from(*mean);
                moment += variance;
                variance_square += variance * variance;
                directional += delta * delta * variance;
            }
            distance + moment
                - extreme_factor(leaf.population)
                    * (2.0 * variance_square + 4.0 * directional).sqrt()
        }
    };
    if !score.is_finite() {
        return Err(invalid("V33 shape score is nonfinite"));
    }
    Ok(if score == 0.0 { 0.0 } else { score })
}

fn extreme_factor(population: u64) -> f64 {
    if population <= 1 {
        0.0
    } else {
        (2.0 * (population as f64).ln()).sqrt()
    }
}

#[cfg(test)]
fn rank_v33_groups(
    leaves: &[V33LeafShape],
    query: &[f32; DIMENSIONS],
    arm: V33ShapeArm,
) -> Result<Vec<u32>> {
    if leaves.is_empty() {
        return Err(invalid("V33 shape leaf summaries differ"));
    }
    let mut scores = BTreeMap::<u32, f64>::new();
    for leaf in leaves {
        let score = score_v33_leaf(leaf, query, arm)?;
        scores
            .entry(leaf.group_ordinal)
            .and_modify(|current| *current = current.min(score))
            .or_insert(score);
    }
    let mut ranked = scores.into_iter().collect::<Vec<_>>();
    ranked.sort_by(|left, right| {
        left.1
            .total_cmp(&right.1)
            .then_with(|| left.0.cmp(&right.0))
    });
    Ok(ranked.into_iter().map(|(ordinal, _)| ordinal).collect())
}

#[cfg(test)]
fn select_v33_group_prefix(
    groups: &[V33GroupPopulation],
    ranked: &[u32],
    row_limit: u64,
    group_limit: usize,
) -> Result<Vec<u32>> {
    if groups.is_empty() || row_limit == 0 || group_limit == 0 {
        return Err(invalid("V33 group prefix bounds differ"));
    }
    let by_ordinal = groups
        .iter()
        .map(|group| (group.ordinal, group.rows))
        .collect::<BTreeMap<_, _>>();
    if by_ordinal.len() != groups.len() || groups.iter().any(|group| group.rows == 0) {
        return Err(invalid("V33 group population authority differs"));
    }
    let mut seen = BTreeSet::new();
    let mut selected = Vec::new();
    let mut rows = 0_u64;
    for ordinal in ranked.iter().copied() {
        if !seen.insert(ordinal) {
            return Err(invalid("V33 ranked group authority differs"));
        }
        let population = *by_ordinal
            .get(&ordinal)
            .ok_or_else(|| invalid("V33 ranked group authority differs"))?;
        let next = rows
            .checked_add(population)
            .ok_or_else(|| invalid("V33 selected rows overflow"))?;
        if selected.len() == group_limit || next > row_limit {
            break;
        }
        selected.push(ordinal);
        rows = next;
    }
    Ok(selected)
}

#[cfg(test)]
mod tests {
    use super::{
        V33FullCovarianceCeilingRequest, V33GroupPopulation, V33GroupShapeBuildRequest,
        V33LeafPopulation, V33LowRankCovarianceLadderRequest, V33ReconstructedOracleRequest,
        V33RoutingRange, V33ShapeArm, build_full_covariance_ceiling_from_populations,
        build_v33_full_covariance_ceiling, build_v33_group_shape_artifact,
        build_v33_low_rank_covariance_ladder, build_v33_reconstructed_group_oracle,
        canonical_v33_full_covariance_ceiling_result_bytes,
        canonical_v33_low_rank_covariance_ladder_result_bytes,
        canonical_v33_reconstructed_oracle_result_bytes, encode_v33_leaf_shape_artifact,
        full_covariance_group_score, full_covariance_moment_score, low_rank_moment_score,
        principal_covariance_components, rank_v33_full_covariance_groups, rank_v33_groups,
        rank_v33_low_rank_covariance_groups, rank_v33_reconstructed_groups,
        reconstruct_v33_leaf_populations, score_v33_leaf, select_v33_group_prefix,
        select_v33_scalar_split_leaves, summarize_full_covariance, summarize_full_covariance_rows,
        summarize_v33_leaf, v33_reconstructed_group_for_logical, v33_shape_control_bytes,
    };
    use crate::{
        V27Hierarchy, build_v34_rank4_generation_from_v33, decode_v34_rank4_arrow,
        encode_v27_hierarchy, encode_v34_rank4_arrow, score_v34_rank4_leaf,
        v30_s3_layout::{
            V30Layout, V30PageIdentity, V30PageRange, V32RoutingRange, encode_v30_layout_artifacts,
        },
        v30_s3_pq::{V30CodePlanes, V30PqCodebook, V30PqWidth, encode_v30_pq_artifacts},
    };
    use arrow_array::{Array, FixedSizeListArray, Float32Array, UInt32Array};
    use arrow_ipc::reader::FileReader;
    use arrow_schema::{DataType, Field};
    use sha2::{Digest, Sha256};
    use std::io::Cursor;

    fn row(logical_ordinal: u64, first: f32, second: f32) -> (u64, [f32; 96]) {
        let mut values = [0.0; 96];
        values[0] = first;
        values[1] = second;
        (logical_ordinal, values)
    }

    fn codebook(width: V30PqWidth, label: u8, value: f32) -> V30PqCodebook {
        let dimensions = width.dimensions();
        let mut values = vec![0.0; width.subquantizers() * width.centroids() * dimensions];
        for subquantizer in 0..width.subquantizers() {
            let start = (subquantizer * width.centroids() + usize::from(label)) * dimensions;
            values[start..start + dimensions].fill(value);
        }
        V30PqCodebook::new(width, values).unwrap()
    }

    fn authenticated_shape_request() -> V33GroupShapeBuildRequest {
        let mut centroid = [half::f16::ZERO; 96];
        centroid[0] = half::f16::ONE;
        let mut leaves = vec![centroid; 4_096];
        leaves[1][0] = half::f16::from_f32(10.0);
        let hierarchy = encode_v27_hierarchy(&V27Hierarchy {
            roots: vec![centroid; 256],
            leaves,
            leaf_roots: (0..4_096).map(|leaf| (leaf / 16) as u16).collect(),
        })
        .unwrap();
        let layout = V30Layout::new(
            20,
            vec![
                V32RoutingRange {
                    leaf_ordinal: 0,
                    code_parent_leaf_ordinal: 0,
                    routing_centroid: centroid,
                    logical_start: 0,
                    row_count: 9,
                    page_start: 0,
                    page_count: 1,
                },
                V32RoutingRange {
                    leaf_ordinal: 1,
                    code_parent_leaf_ordinal: 1,
                    routing_centroid: centroid,
                    logical_start: 9,
                    row_count: 11,
                    page_start: 0,
                    page_count: 1,
                },
            ],
            vec![V30PageRange {
                logical_start: 0,
                row_count: 20,
                identity: V30PageIdentity {
                    ordinal: 0,
                    sha256: [1; 32],
                    encoded_bytes: 1,
                    primary_rows: 20,
                    replica_rows: 0,
                },
            }],
        )
        .unwrap();
        V33GroupShapeBuildRequest {
            hierarchy,
            layout: encode_v30_layout_artifacts(&layout).unwrap(),
            pq: encode_v30_pq_artifacts(
                &codebook(V30PqWidth::Base24, 1, 0.5),
                &codebook(V30PqWidth::High48, 2, 1.5),
                &V30CodePlanes::from_packed(20, vec![1, 0, 0, 0], vec![1; 19 * 24], vec![2; 48])
                    .unwrap(),
            )
            .unwrap(),
            group_of_code_parent: (0..4_096).collect(),
            scalar_split_count: 2,
        }
    }

    #[test]
    fn v33_group_shape_reconstruction_uses_fidelity_width_and_code_parent() {
        let base = codebook(V30PqWidth::Base24, 1, 0.5);
        let high = codebook(V30PqWidth::High48, 2, 1.5);
        let codes =
            V30CodePlanes::from_packed(2, vec![0b10, 0, 0, 0], vec![1; 24], vec![2; 48]).unwrap();
        let mut parent_centers = vec![[0.0; 96]; 2];
        parent_centers[0].fill(2.0);
        parent_centers[1].fill(4.0);
        let ranges = [
            V33RoutingRange {
                routing_leaf_ordinal: 0,
                code_parent_leaf_ordinal: 0,
                logical_start: 0,
                row_count: 1,
            },
            V33RoutingRange {
                routing_leaf_ordinal: 1,
                code_parent_leaf_ordinal: 1,
                logical_start: 1,
                row_count: 1,
            },
        ];
        let reconstructed = reconstruct_v33_leaf_populations(
            &base,
            &high,
            &codes,
            &parent_centers,
            &ranges,
            &[7, 9],
        )
        .unwrap();
        assert_eq!(reconstructed.len(), 2);
        assert_eq!(reconstructed[0].group_ordinal, 7);
        assert_eq!(reconstructed[0].rows[0].1, [2.5; 96]);
        assert_eq!(reconstructed[1].group_ordinal, 9);
        assert_eq!(reconstructed[1].rows[0].1, [5.5; 96]);
        assert!(reconstructed[1].rows[0].1.iter().sum::<f32>() > 1.0);

        let mut overlapping = ranges;
        overlapping[1].logical_start = 0;
        assert!(
            reconstruct_v33_leaf_populations(
                &base,
                &high,
                &codes,
                &parent_centers,
                &overlapping,
                &[7, 9],
            )
            .is_err()
        );
    }

    #[test]
    fn v33_group_shape_moments_use_complete_gaussian_variance_without_clamp() {
        let leaf = V33LeafPopulation {
            routing_leaf_ordinal: 7,
            group_ordinal: 3,
            rows: vec![row(10, 1.0, 0.0), row(11, 3.0, 0.0)],
        };
        let summary = summarize_v33_leaf(&leaf).unwrap();
        assert_eq!(summary.population, 2);
        assert_eq!(summary.mean[0], 2.0);
        assert_eq!(summary.diagonal_variance[0], 1.0);
        assert_eq!(summary.scalar_moment, 1.0);
        assert_eq!(summary.split_centers[0][0], 1.0);
        assert_eq!(summary.split_centers[1][0], 3.0);

        let query = row(0, 4.0, 0.0).1;
        let a = (2.0_f64 * 2.0_f64.ln()).sqrt();
        let scalar_expected = 5.0 - a * (18.0_f64 / 96.0).sqrt();
        let diagonal_expected = 5.0 - a * 18.0_f64.sqrt();
        assert_eq!(
            score_v33_leaf(&summary, &query, V33ShapeArm::ScalarMoment).unwrap(),
            scalar_expected
        );
        assert_eq!(
            score_v33_leaf(&summary, &query, V33ShapeArm::DiagonalMoment).unwrap(),
            diagonal_expected
        );
        assert_eq!(
            score_v33_leaf(&summary, &query, V33ShapeArm::SplitCentroid).unwrap(),
            1.0
        );

        let far_spread = V33LeafPopulation {
            routing_leaf_ordinal: 8,
            group_ordinal: 4,
            rows: vec![row(12, -100.0, 0.0), row(13, 100.0, 0.0)],
        };
        let signed = score_v33_leaf(
            &summarize_v33_leaf(&far_spread).unwrap(),
            &[0.0; 96],
            V33ShapeArm::DiagonalMoment,
        )
        .unwrap();
        assert!(
            signed < 0.0,
            "negative ranking evidence must not be clamped"
        );
    }

    #[test]
    fn v33_group_shape_low_rank_covariance_couples_dimensions() {
        // Break caught: a rotated correlated population is reduced to the
        // axis-aligned diagonal and cannot distinguish along/across directions.
        let population = V33LeafPopulation {
            routing_leaf_ordinal: 0,
            group_ordinal: 0,
            rows: vec![row(0, -1.0, -1.0), row(1, 1.0, 1.0)],
        };
        let (directions, eigenvalues, residual) =
            principal_covariance_components(&population, 2).unwrap();
        let root_half = 0.5_f64.sqrt();
        assert!((f64::from(directions[0][0]) - root_half).abs() < 1e-6);
        assert!((f64::from(directions[0][1]) - root_half).abs() < 1e-6);
        assert!((f64::from(eigenvalues[0]) - 2.0).abs() < 1e-6);
        assert!(eigenvalues[1].abs() < 1e-6);
        assert!(residual[0].abs() < 1e-6 && residual[1].abs() < 1e-6);

        let mut query = [0.0_f32; 96];
        query[0] = 1.0;
        query[1] = -1.0;
        let score =
            low_rank_moment_score(&[0.0; 96], &residual, &directions, &eigenvalues, 2, &query)
                .unwrap();
        let expected = 4.0 - (2.0_f64 * 2.0_f64.ln()).sqrt() * 8.0_f64.sqrt();
        assert!(
            (score - expected).abs() < 1e-6,
            "score={score:.12} expected={expected:.12} delta={:.12}",
            score - expected
        );
    }

    #[test]
    fn v33_group_shape_low_rank_covariance_is_deterministic_when_degenerate() {
        let population = V33LeafPopulation {
            routing_leaf_ordinal: 0,
            group_ordinal: 0,
            rows: vec![row(0, 2.0, 3.0), row(1, 2.0, 3.0)],
        };
        let first = principal_covariance_components(&population, 2).unwrap();
        let second = principal_covariance_components(&population, 2).unwrap();
        assert_eq!(first, second);
        assert_eq!(first.1, [0.0, 0.0]);
        assert_eq!(first.2, [0.0; 96]);
    }

    #[test]
    fn v33_group_shape_low_rank_direction_sign_uses_largest_coordinate() {
        // Break caught: sign depends on the first tiny nonzero coordinate,
        // rather than the registered largest-magnitude coordinate tie-break.
        let population = V33LeafPopulation {
            routing_leaf_ordinal: 0,
            group_ordinal: 0,
            rows: vec![row(0, -1.0, 2.0), row(1, 1.0, -2.0)],
        };
        let (directions, _, _) = principal_covariance_components(&population, 1).unwrap();
        let direction = directions[0];
        let largest = direction
            .iter()
            .enumerate()
            .max_by(|left, right| {
                left.1
                    .abs()
                    .total_cmp(&right.1.abs())
                    .then_with(|| right.0.cmp(&left.0))
            })
            .unwrap();
        assert_eq!(largest.0, 1);
        assert!(largest.1.is_sign_positive());
    }

    #[test]
    fn v33_group_shape_low_rank_repeated_eigenspace_uses_coordinate_order() {
        // Break caught: equal eigenvalues inherit row order or thread order.
        let rows = vec![
            row(0, -1.0, 0.0),
            row(1, 1.0, 0.0),
            row(2, 0.0, -1.0),
            row(3, 0.0, 1.0),
        ];
        let population = V33LeafPopulation {
            routing_leaf_ordinal: 0,
            group_ordinal: 0,
            rows: rows.clone(),
        };
        let reversed = V33LeafPopulation {
            routing_leaf_ordinal: 0,
            group_ordinal: 0,
            rows: rows.into_iter().rev().collect(),
        };
        let first = principal_covariance_components(&population, 2).unwrap();
        let second = principal_covariance_components(&reversed, 2).unwrap();
        assert_eq!(first, second);
        assert_eq!(first.0[0][0], 1.0);
        assert_eq!(first.0[1][1], 1.0);
    }

    #[test]
    fn v33_group_shape_low_rank_components_are_globally_ordered() {
        // Break caught: a power seed chosen from the largest diagonal can be
        // orthogonal to a larger correlated eigenspace.
        let mut rows = Vec::new();
        for (ordinal, (start, end, magnitude)) in [(0, 1, 2.5_f32), (1, 5, 2.0), (5, 9, 1.5)]
            .into_iter()
            .enumerate()
        {
            for sign in [-1.0_f32, 1.0] {
                let mut value = [0.0_f32; 96];
                for coordinate in &mut value[start..end] {
                    *coordinate = sign * magnitude;
                }
                rows.push(((ordinal * 2 + usize::from(sign > 0.0)) as u64, value));
            }
        }
        rows.sort_by_key(|(ordinal, _)| *ordinal);
        let population = V33LeafPopulation {
            routing_leaf_ordinal: 0,
            group_ordinal: 0,
            rows,
        };
        let (_, eigenvalues, _) = principal_covariance_components(&population, 4).unwrap();
        assert!((f64::from(eigenvalues[0]) - 16.0 / 3.0).abs() < 1.0e-5);
        assert!((f64::from(eigenvalues[1]) - 3.0).abs() < 1.0e-5);
        assert!((f64::from(eigenvalues[2]) - 25.0 / 12.0).abs() < 1.0e-5);
        assert!(eigenvalues.windows(2).all(|pair| pair[0] >= pair[1]));
    }

    #[test]
    fn v33_group_shape_full_covariance_ceiling_couples_rotated_dimensions() {
        // Break caught: the full-covariance ceiling silently drops off-diagonal
        // covariance and reproduces the already-failed diagonal arm.
        let population = V33LeafPopulation {
            routing_leaf_ordinal: 0,
            group_ordinal: 0,
            rows: vec![row(0, -1.0, -1.0), row(1, 1.0, 1.0)],
        };
        let summary = summarize_full_covariance(&population).unwrap();
        let mut query = [0.0_f32; 96];
        query[0] = 1.0;
        query[1] = -1.0;
        let score = full_covariance_moment_score(&summary, &query).unwrap();
        let expected = 4.0 - (2.0_f64 * 2.0_f64.ln()).sqrt() * 8.0_f64.sqrt();
        assert!((score - expected).abs() < 1e-12);
    }

    #[test]
    fn v33_group_shape_full_covariance_reduces_leaf_scores_without_pooling() {
        // Break caught: rows from distinct routing leaves are pooled into one
        // storage-group covariance instead of reducing independent leaf scores.
        let left = V33LeafPopulation {
            routing_leaf_ordinal: 0,
            group_ordinal: 0,
            rows: vec![row(0, -3.0, -3.0), row(1, -1.0, -1.0)],
        };
        let right = V33LeafPopulation {
            routing_leaf_ordinal: 1,
            group_ordinal: 0,
            rows: vec![row(2, 1.0, 1.0), row(3, 3.0, 3.0)],
        };
        let other = V33LeafPopulation {
            routing_leaf_ordinal: 2,
            group_ordinal: 1,
            rows: vec![row(4, 8.0, 0.0), row(5, 9.0, 0.0)],
        };
        let query = row(99, -2.0, -2.0).1;
        let expected =
            full_covariance_moment_score(&summarize_full_covariance(&left).unwrap(), &query)
                .unwrap()
                .min(
                    full_covariance_moment_score(
                        &summarize_full_covariance(&right).unwrap(),
                        &query,
                    )
                    .unwrap(),
                );
        let ceiling = build_full_covariance_ceiling_from_populations(vec![
            left.clone(),
            right.clone(),
            other,
        ])
        .unwrap();
        assert_eq!(
            full_covariance_group_score(&ceiling.groups[0], &query).unwrap(),
            expected
        );
        assert_ne!(
            full_covariance_moment_score(
                &summarize_full_covariance_rows(0, &[left.rows, right.rows].concat()).unwrap(),
                &query,
            )
            .unwrap(),
            expected
        );
    }

    #[test]
    fn v33_group_shape_full_covariance_ceiling_receipt_recomputes_every_owner() {
        // Break caught: the dense ceiling trusts frontier hit/rank fields or
        // serializes a pass without recomputing the longest complete prefix.
        let ceiling = build_v33_full_covariance_ceiling(&authenticated_shape_request()).unwrap();
        let mut query = [0.5_f32; 96];
        query[0] = 1.5;
        assert_eq!(
            rank_v33_full_covariance_groups(&ceiling, &query).unwrap(),
            [0, 1]
        );
        let request = V33FullCovarianceCeilingRequest {
            frontier_sha256: "1".repeat(64),
            frontier_bytes: 123,
            query_ordinals: vec![4_096],
            queries: vec![query],
            truth_logicals: vec![vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9]],
            group_rows: vec![9, 11],
            row_limit: 9,
            group_limit: 2,
        };
        let bytes = canonical_v33_full_covariance_ceiling_result_bytes(&ceiling, &request).unwrap();
        assert_eq!(bytes.last(), Some(&b'\n'));
        let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(value["included_owners"], 9);
        assert_eq!(value["total_owners"], 10);
        assert_eq!(value["perfect_queries"], 0);
        assert_eq!(value["query_count"], 1);
        assert_eq!(value["passed"], false);
        assert_eq!(
            value["records"][0]["selected_groups"],
            serde_json::json!([0])
        );
        assert_eq!(
            value["records"][0]["truth_owner_ranks"],
            serde_json::json!([1, 1, 1, 1, 1, 1, 1, 1, 1, 2])
        );

        let mut invalid = request;
        invalid.queries[0][0] = f32::NAN;
        assert!(canonical_v33_full_covariance_ceiling_result_bytes(&ceiling, &invalid).is_err());
    }

    #[test]
    fn v33_group_shape_low_rank_ladder_is_nested_and_preserves_f32_diagonal() {
        // Break caught: ranks one/two/four are decomposed independently or the
        // residual diagonal double-counts principal variance after f32 storage.
        let mut rows = Vec::new();
        for (ordinal, (x, y, z)) in [
            (-3.0, -3.0, 1.0),
            (-1.0, -1.0, -1.0),
            (1.0, 1.0, -1.0),
            (3.0, 3.0, 1.0),
        ]
        .into_iter()
        .enumerate()
        {
            let mut value = [0.0_f32; 96];
            value[0] = x;
            value[1] = y;
            value[2] = z;
            rows.push((ordinal as u64, value));
        }
        let population = V33LeafPopulation {
            routing_leaf_ordinal: 0,
            group_ordinal: 0,
            rows,
        };
        let first =
            super::build_low_rank_covariance_ladder_from_populations(vec![population.clone()])
                .unwrap();
        let second =
            super::build_low_rank_covariance_ladder_from_populations(vec![population]).unwrap();
        assert_eq!(first, second);
        assert!(!first.artifact_arrow().is_empty());
        assert_eq!(
            first.artifact_sha256(),
            format!("{:x}", Sha256::digest(first.artifact_arrow()))
        );
        assert_eq!(
            first.artifact_encoded_bytes(),
            first.artifact_arrow().len() as u64
        );
        let mut corrupted = super::V33LowRankCovarianceArtifact {
            sha256: first.artifact_sha256().to_owned(),
            encoded_bytes: first.artifact_encoded_bytes(),
            row_count: 1,
            arrow: first.artifact_arrow().to_vec(),
        };
        corrupted.arrow[0] ^= 1;
        assert!(super::decode_v33_low_rank_covariance_artifact(&corrupted).is_err());
        let leaf = &first.groups[0].leaves[0];
        assert_eq!(leaf.ranks, [1, 2, 4]);
        for (arm, rank) in [1_usize, 2, 4].into_iter().enumerate() {
            for dimension in 0..96 {
                let reconstructed = f64::from(leaf.residuals[arm][dimension])
                    + (0..rank)
                        .map(|component| {
                            f64::from(leaf.eigenvalues[component])
                                * f64::from(leaf.directions[component][dimension]).powi(2)
                        })
                        .sum::<f64>();
                assert!(
                    (reconstructed - f64::from(leaf.diagonal[dimension])).abs() <= 2.0e-6,
                    "rank={rank} dimension={dimension} reconstructed={reconstructed} diagonal={}",
                    leaf.diagonal[dimension]
                );
            }
        }
    }

    #[test]
    fn v34_rank4_from_v33_binds_authenticated_reconstruction_and_rank_four() {
        // Break caught: V34 accepts caller-invented leaves instead of deriving
        // the exact rank-four arm from authenticated V27/V30 artifacts.
        let request = authenticated_shape_request();
        let v33 = build_v33_low_rank_covariance_ladder(&request).unwrap();
        let v34 = build_v34_rank4_generation_from_v33(&request).unwrap();
        assert_eq!(v34.leaves().len(), 2);
        assert_eq!(v34.logical_rows(), 20);
        assert_eq!(v34.group_count(), 2);
        let expected = v33
            .groups
            .iter()
            .flat_map(|group| group.leaves.iter())
            .collect::<Vec<_>>();
        for (actual, expected) in v34.leaves().iter().zip(expected) {
            assert_eq!(actual.leaf_ordinal(), expected.ordinal);
            assert_eq!(actual.group_ordinal(), expected.group_ordinal);
            assert_eq!(actual.logical_start(), expected.logical_start);
            assert_eq!(u64::from(actual.population()), expected.population);
            assert_eq!(actual.mean(), &expected.mean);
            assert_eq!(actual.residual_diagonal(), &expected.residuals[2]);
            assert_eq!(actual.eigenvalues(), &expected.eigenvalues);
            for component in 0..4 {
                if expected.eigenvalues[component] == 0.0 {
                    assert_eq!(actual.directions()[component], [0.0; 96]);
                } else {
                    assert_eq!(
                        actual.directions()[component],
                        expected.directions[component]
                    );
                }
            }
        }

        let (arrow, identity) = encode_v34_rank4_arrow(
            &v34,
            "s3://borsuk-v34-test/generations/authenticated-rank4.arrow",
            &"11".repeat(32),
            &"22".repeat(32),
            &"33".repeat(32),
        )
        .unwrap();
        let decoded = decode_v34_rank4_arrow(&arrow, &identity).unwrap();
        let query = std::array::from_fn(|dimension| dimension as f32 / 97.0);
        for (actual, expected) in decoded
            .leaves()
            .iter()
            .zip(v33.groups.iter().flat_map(|group| group.leaves.iter()))
        {
            let expected_score = low_rank_moment_score(
                &expected.mean,
                &expected.residuals[2],
                &expected.directions,
                &expected.eigenvalues,
                expected.population,
                &query,
            )
            .unwrap();
            let actual_score = score_v34_rank4_leaf(actual, &query).unwrap();
            assert!(
                (actual_score - expected_score).abs() <= 1.0e-12 * expected_score.abs().max(1.0)
            );
        }
        let expected_order = rank_v33_low_rank_covariance_groups(&v33, 4, &query).unwrap();
        let mut group_scores = std::collections::BTreeMap::<u32, f64>::new();
        for leaf in decoded.leaves() {
            let score = score_v34_rank4_leaf(leaf, &query).unwrap();
            group_scores
                .entry(leaf.group_ordinal())
                .and_modify(|current| *current = current.min(score))
                .or_insert(score);
        }
        let mut actual_order = group_scores.into_iter().collect::<Vec<_>>();
        actual_order.sort_by(|left, right| {
            left.1
                .total_cmp(&right.1)
                .then_with(|| left.0.cmp(&right.0))
        });
        assert_eq!(
            actual_order
                .into_iter()
                .map(|(group, _)| group)
                .collect::<Vec<_>>(),
            expected_order
        );
    }

    #[test]
    fn v33_group_shape_low_rank_ladder_receipt_uses_rank_two_as_frozen_primary() {
        // Break caught: the outcome selects the best observed rank, or the
        // receipt trusts arm summaries instead of recomputing every owner.
        let ladder = build_v33_low_rank_covariance_ladder(&authenticated_shape_request()).unwrap();
        let mut query = [0.5_f32; 96];
        query[0] = 1.5;
        assert_eq!(
            rank_v33_low_rank_covariance_groups(&ladder, 2, &query).unwrap(),
            [0, 1]
        );
        let request = V33LowRankCovarianceLadderRequest {
            frontier_sha256: "1".repeat(64),
            frontier_bytes: 123,
            query_ordinals: vec![4_096],
            queries: vec![query],
            truth_logicals: vec![vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9]],
            group_rows: vec![9, 11],
            row_limit: 9,
            group_limit: 2,
        };
        let bytes =
            canonical_v33_low_rank_covariance_ladder_result_bytes(&ladder, &request).unwrap();
        assert_eq!(bytes.last(), Some(&b'\n'));
        let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(
            value["schema"],
            "borsuk-v33-low-rank-covariance-ladder-result-v1"
        );
        assert_eq!(value["primary_rank"], 2);
        assert_eq!(
            value["low_rank_summary"]["sha256"],
            ladder.artifact_sha256()
        );
        assert_eq!(value["arms"].as_array().unwrap().len(), 3);
        assert_eq!(value["arms"][0]["rank"], 1);
        assert_eq!(value["arms"][1]["rank"], 2);
        assert_eq!(value["arms"][2]["rank"], 4);
        assert_eq!(value["passed"], value["arms"][1]["passed"]);
        assert_eq!(value["arms"][1]["included_owners"], 9);
        assert_eq!(value["arms"][1]["perfect_queries"], 0);
        assert_eq!(
            value["fine_leaf_centroid_control"]["arm"],
            "fine-leaf-centroid"
        );
        assert!(value["arms"][1]["required_rows_p50"].is_u64());
        assert!(value["arms"][1]["required_rows_p95"].is_u64());
        assert!(value["arms"][1]["required_rows_max"].is_u64());
        assert!(value["arms"][1]["required_groups_p50"].is_u64());
        assert!(value["arms"][1]["required_groups_p95"].is_u64());
        assert!(value["arms"][1]["required_groups_max"].is_u64());
        assert!(value["arms"][1]["frontier_non_worse"].is_boolean());
        assert_eq!(
            value["arms"][1]["passed"],
            serde_json::Value::Bool(
                value["arms"][1]["coverage_passed"].as_bool().unwrap()
                    && value["arms"][1]["frontier_non_worse"].as_bool().unwrap()
            )
        );
        assert_eq!(
            value["arms"][1]["records"][0]["truth_owner_ranks"],
            serde_json::json!([1, 1, 1, 1, 1, 1, 1, 1, 1, 2])
        );

        let mut drifted = ladder.clone();
        drifted.groups[0].leaves[0].residuals[1][0] += 1.0;
        assert!(canonical_v33_low_rank_covariance_ladder_result_bytes(&drifted, &request).is_err());
        let mut reordered = ladder.clone();
        reordered.groups[0].leaves[0].ordinal = u32::MAX;
        assert!(
            canonical_v33_low_rank_covariance_ladder_result_bytes(&reordered, &request).is_err()
        );

        let mut invalid = request;
        invalid.queries[0][0] = f32::NAN;
        assert!(canonical_v33_low_rank_covariance_ladder_result_bytes(&ladder, &invalid).is_err());
    }

    #[test]
    fn v33_group_shape_reconstructed_oracle_is_frozen_before_query_ranking() {
        // Break caught: an approximate shape is promoted without first proving
        // that the same immutable PQ reconstruction can rank the missed owner.
        let request = authenticated_shape_request();
        let oracle = build_v33_reconstructed_group_oracle(&request).unwrap();
        assert_eq!(v33_reconstructed_group_for_logical(&oracle, 0).unwrap(), 0);
        assert_eq!(v33_reconstructed_group_for_logical(&oracle, 9).unwrap(), 1);
        assert!(v33_reconstructed_group_for_logical(&oracle, 20).is_err());
        let mut query = [0.5_f32; 96];
        query[0] = 1.5;
        assert_eq!(
            rank_v33_reconstructed_groups(&oracle, &query).unwrap(),
            [0, 1]
        );

        query[0] = f32::NAN;
        assert!(rank_v33_reconstructed_groups(&oracle, &query).is_err());
    }

    #[test]
    fn v33_group_shape_reconstructed_oracle_receipt_recomputes_prefix_and_bindings() {
        let oracle = build_v33_reconstructed_group_oracle(&authenticated_shape_request()).unwrap();
        let mut query = [0.5_f32; 96];
        query[0] = 1.5;
        let bytes = canonical_v33_reconstructed_oracle_result_bytes(
            &oracle,
            &V33ReconstructedOracleRequest {
                frontier_sha256: "1111111111111111111111111111111111111111111111111111111111111111"
                    .to_owned(),
                frontier_bytes: 123,
                query_ordinal: 6_160,
                query,
                truth_logicals: vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
                group_rows: vec![9, 11],
                row_limit: 9,
                group_limit: 2,
            },
        )
        .unwrap();
        assert_eq!(bytes.last(), Some(&b'\n'));
        let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(value["all_required_selected"], false);
        assert_eq!(
            value["required_group_ranks"],
            serde_json::json!([1, 1, 1, 1, 1, 1, 1, 1, 1, 2])
        );
        assert_eq!(value["selected_groups"], serde_json::json!([0]));
        assert_eq!(value["selected_rows"], 9);

        let mut invalid = V33ReconstructedOracleRequest {
            frontier_sha256: "1".repeat(64),
            frontier_bytes: 123,
            query_ordinal: 6_160,
            query,
            truth_logicals: vec![0; 10],
            group_rows: vec![9, 11],
            row_limit: 9,
            group_limit: 2,
        };
        invalid.group_rows.pop();
        assert!(canonical_v33_reconstructed_oracle_result_bytes(&oracle, &invalid).is_err());
    }

    #[test]
    fn v33_group_shape_equal_byte_controls_are_exact_and_deterministic() {
        let bytes = v33_shape_control_bytes(4_141).unwrap();
        assert_eq!(bytes.scalar_summary_bytes, 4_141 * 388);
        assert_eq!(bytes.scalar_extra_centers, 43);
        assert_eq!(bytes.scalar_padding_bytes, 52);
        assert_eq!(bytes.diagonal_summary_bytes, 4_141 * 768);
        assert_eq!(bytes.diagonal_control_bytes, bytes.diagonal_summary_bytes);

        let leaf = V33LeafPopulation {
            routing_leaf_ordinal: 2,
            group_ordinal: 1,
            rows: vec![
                row(9, 2.0, 0.0),
                row(4, -2.0, 0.0),
                row(7, 1.0, 0.0),
                row(5, -1.0, 0.0),
            ],
        };
        let summary = summarize_v33_leaf(&leaf).unwrap();
        assert_eq!(summary.split_dimension, 0);
        assert_eq!(summary.split_centers[0][0], -1.5);
        assert_eq!(summary.split_centers[1][0], 1.5);
        assert_eq!(summary.maximum_radius, 2.0);

        let populations = (0..50)
            .map(|ordinal| V33LeafPopulation {
                routing_leaf_ordinal: ordinal,
                group_ordinal: 0,
                rows: (0..(ordinal + 2))
                    .map(|row_ordinal| row(u64::from(row_ordinal), row_ordinal as f32, 0.0))
                    .collect(),
            })
            .collect::<Vec<_>>();
        assert_eq!(
            select_v33_scalar_split_leaves(&populations, 43).unwrap(),
            (7_u32..50).rev().collect::<Vec<_>>()
        );
    }

    #[test]
    fn v33_group_shape_group_min_ties_overflow_and_duplicate_truth_are_preserved() {
        let leaves = vec![
            summarize_v33_leaf(&V33LeafPopulation {
                routing_leaf_ordinal: 2,
                group_ordinal: 1,
                rows: vec![row(0, 1.0, 0.0)],
            })
            .unwrap(),
            summarize_v33_leaf(&V33LeafPopulation {
                routing_leaf_ordinal: 0,
                group_ordinal: 0,
                rows: vec![row(1, 1.0, 0.0)],
            })
            .unwrap(),
            summarize_v33_leaf(&V33LeafPopulation {
                routing_leaf_ordinal: 1,
                group_ordinal: 0,
                rows: vec![row(2, 4.0, 0.0)],
            })
            .unwrap(),
        ];
        let ranked = rank_v33_groups(&leaves, &[0.0; 96], V33ShapeArm::Centroid).unwrap();
        assert_eq!(ranked, vec![0, 1]);

        let groups = vec![
            V33GroupPopulation {
                ordinal: 0,
                rows: 7,
            },
            V33GroupPopulation {
                ordinal: 1,
                rows: 6,
            },
            V33GroupPopulation {
                ordinal: 2,
                rows: 1,
            },
        ];
        assert_eq!(
            select_v33_group_prefix(&groups, &[0, 1, 2], 12, 3).unwrap(),
            vec![0]
        );

        let truth_groups = [0_u32, 0, 1, 0, 1, 1, 0, 1, 0, 1];
        let selected = [0_u32];
        assert_eq!(
            truth_groups
                .iter()
                .filter(|group| selected.contains(group))
                .count(),
            5
        );
    }

    #[test]
    fn v33_group_shape_arrow_artifact_binds_exact_f32_shape_and_split_set() {
        let populations = [
            V33LeafPopulation {
                routing_leaf_ordinal: 0,
                group_ordinal: 4,
                rows: vec![row(0, -1.0, 0.0), row(1, 1.0, 0.0)],
            },
            V33LeafPopulation {
                routing_leaf_ordinal: 1,
                group_ordinal: 5,
                rows: vec![row(2, 2.0, 3.0)],
            },
        ];
        let summaries = populations
            .iter()
            .map(summarize_v33_leaf)
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        let artifact = encode_v33_leaf_shape_artifact(&summaries, &[0]).unwrap();
        assert_eq!(artifact.role, "v33-leaf-shapes-arrow");
        assert_eq!(artifact.row_count, 2);
        assert_eq!(artifact.encoded_bytes, artifact.arrow.len() as u64);
        assert_eq!(
            artifact.sha256,
            format!("{:x}", Sha256::digest(&artifact.arrow))
        );
        let mut reader = FileReader::try_new(Cursor::new(&artifact.arrow), None).unwrap();
        let schema = reader.schema();
        assert_eq!(
            schema.field(0),
            &Field::new("routing_leaf_ordinal", DataType::UInt32, false)
        );
        assert_eq!(
            schema.field(4),
            &Field::new(
                "mean",
                DataType::FixedSizeList(
                    std::sync::Arc::new(Field::new("element", DataType::Float32, false)),
                    96,
                ),
                false,
            )
        );
        assert_eq!(schema.field(5).name(), "diagonal_variance");
        assert_eq!(schema.field(6).name(), "scalar_moment");
        assert_eq!(schema.field(7).name(), "maximum_radius");
        assert_eq!(schema.field(8).name(), "split_dimension");
        assert_eq!(schema.field(9).name(), "split_center_left");
        assert_eq!(schema.field(10).name(), "split_center_right");
        assert_eq!(schema.field(11).name(), "scalar_split_selected");
        let batch = reader.next().unwrap().unwrap();
        assert!(reader.next().is_none());
        assert!(
            batch
                .columns()
                .iter()
                .all(|column| column.null_count() == 0)
        );
        assert_eq!(
            batch
                .column(0)
                .as_any()
                .downcast_ref::<UInt32Array>()
                .unwrap()
                .values(),
            &[0, 1]
        );
        let means = batch
            .column(4)
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .unwrap();
        assert_eq!(means.len(), 2);
        assert_eq!(
            means
                .values()
                .as_any()
                .downcast_ref::<Float32Array>()
                .unwrap()
                .value(96),
            2.0
        );
    }

    #[test]
    fn v33_group_shape_authenticated_artifacts_build_query_independent_arrow() {
        let request = authenticated_shape_request();
        let artifact = build_v33_group_shape_artifact(&request).unwrap();
        assert_eq!(artifact.role, "v33-leaf-shapes-arrow");
        assert_eq!(artifact.row_count, 2);

        let mut changed = request.clone();
        changed.pq.bytes[2][0] ^= 1;
        assert!(build_v33_group_shape_artifact(&changed).is_err());
    }
}
