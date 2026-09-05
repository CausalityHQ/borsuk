use std::{collections::BTreeMap, io::Cursor, sync::Arc};

use arrow_array::{
    Array, FixedSizeListArray, Float32Array, Float64Array, Int8Array, RecordBatch, UInt8Array,
    UInt32Array, UInt64Array,
};
use arrow_ipc::{
    MetadataVersion,
    reader::FileReader,
    writer::{FileWriter, IpcWriteOptions},
};
use arrow_schema::{DataType, Field, Schema};
use nalgebra::{DMatrix, SymmetricEigen};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{
    BorsukError, Result, V35ArtifactIdentity, V35Dimensions, V35Projection, V35ProjectionArm,
};

const COMPONENTS: usize = 6;
const MAX_LEAF_ROWS: usize = 256;
const GENERATION_FORMAT: &str = "borsuk-v35-routing-arrow-v1";
const GENERATION_MANIFEST_KEY: &str = "borsuk.v35.routing.manifest";

fn invalid(message: &str) -> BorsukError {
    BorsukError::InvalidStorage(message.to_owned())
}

/// Exact packed-upper-triangular f64 co-moment bytes for one worker.
pub fn project_v35_leaf_moment_bytes(routing: u16) -> Result<u64> {
    if !matches!(routing, 64 | 128 | 192) {
        return Err(invalid("V35 patch moment dimension differs"));
    }
    let width = u64::from(routing);
    width
        .checked_mul(width + 1)
        .and_then(|values| values.checked_div(2))
        .and_then(|values| values.checked_mul(8))
        .ok_or_else(|| invalid("V35 patch moment bytes overflow"))
}

/// Conservative eigensolver workspace after the packed accumulator is sealed.
pub fn project_v35_leaf_seal_workspace_bytes(routing: u16) -> Result<u64> {
    if !matches!(routing, 64 | 128 | 192) {
        return Err(invalid("V35 patch seal dimension differs"));
    }
    let width = u64::from(routing);
    width
        .checked_mul(width)
        .and_then(|values| values.checked_mul(16))
        .and_then(|matrices| {
            width
                .checked_mul(4 * std::mem::size_of::<f64>() as u64)
                .and_then(|vectors| matrices.checked_add(vectors))
        })
        .ok_or_else(|| invalid("V35 patch seal workspace overflows"))
}

fn packed_upper_index(width: usize, left: usize, right: usize) -> usize {
    debug_assert!(left <= right && right < width);
    left * width - left * left.saturating_sub(1) / 2 + (right - left)
}

#[derive(Debug, Clone, PartialEq)]
/// One bounded projected leaf used to construct a compact routing patch.
pub struct V35LeafPatchBuildRequest {
    /// Greatest source assignment covered by the leaf.
    pub assignment_max: u64,
    /// Smallest source assignment covered by the leaf.
    pub assignment_min: u64,
    /// Source and projected routing dimensions.
    pub dimensions: V35Dimensions,
    /// Dense storage-group ordinal.
    pub group_ordinal: u32,
    /// Dense generation-local leaf ordinal.
    pub leaf_ordinal: u32,
    /// First logical row in this leaf.
    pub logical_start: u64,
    /// Per-row clamped source-space energy omitted by the projection.
    pub omitted_energies: Vec<f64>,
    /// At-most-256 decoded-f32 projected rows in source ordinal order.
    pub projected_rows: Vec<Vec<f32>>,
}

#[derive(Debug, Clone, PartialEq)]
/// Compact quantized V35 routing patch; decoded planes are never retained.
pub struct V35LeafPatch {
    assignment_max: u64,
    assignment_min: u64,
    dimensions: V35Dimensions,
    group_ordinal: u32,
    leaf_ordinal: u32,
    logical_start: u64,
    population: u32,
    mean_codes: Vec<i8>,
    mean_scale: f32,
    residual_codes: Vec<u8>,
    residual_scale: f32,
    direction_codes: Vec<i8>,
    direction_scales: [f32; COMPONENTS],
    weights: [f32; COMPONENTS],
    trace: f64,
    trace_square: f64,
    population_factor: f64,
    spectral_bound: f64,
    omitted_energy: f32,
}

#[derive(Debug, Clone, PartialEq)]
/// One- or two-patch routing arm under an exact resident byte envelope.
pub struct V35LeafPatchArm {
    patches: Vec<V35LeafPatch>,
    encoded_bytes: usize,
}

impl V35LeafPatchArm {
    /// Number of independently scored patches.
    pub fn patch_count(&self) -> usize {
        self.patches.len()
    }

    /// Exact admitted resident bytes including every fixed envelope.
    pub fn encoded_bytes(&self) -> usize {
        self.encoded_bytes
    }

    /// Sum of patch populations; equal to the parent leaf population.
    pub fn total_population(&self) -> u32 {
        self.patches.iter().map(V35LeafPatch::population).sum()
    }
}

#[derive(Debug, Clone, PartialEq)]
/// Equal-byte f32 extra-centroid control for a patch arm.
pub struct V35EqualByteCentroidControl {
    dimensions: V35Dimensions,
    centers: Vec<Vec<f32>>,
    encoded_bytes: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Allocation limits authenticated before a routing generation is decoded.
pub struct V35RoutingGenerationLimits {
    /// Maximum complete Arrow object length.
    pub maximum_encoded_bytes: u64,
    /// Maximum logical leaf count.
    pub maximum_leaf_count: u64,
    /// Maximum compact numeric resident bytes.
    pub maximum_numeric_bytes: usize,
    /// Conservative maximum of encoded bytes plus one complete Arrow copy.
    pub maximum_peak_codec_bytes: u64,
}

#[derive(Debug, Clone)]
/// Authenticated compact routing generation; row codes and vectors are absent.
pub struct V35RoutingGeneration {
    dimensions: V35Dimensions,
    projection_arm: V35ProjectionArm,
    projection_checksum: [u8; 32],
    projection_algorithm: String,
    projection_sample_sha256: Option<String>,
    projection_training_descriptor: String,
    source_id: String,
    source_archive_sha256: String,
    patches_per_leaf: u8,
    leaf_count: usize,
    resident_numeric_bytes: usize,
    storage: V35RoutingGenerationStorage,
}

#[derive(Debug, Clone)]
enum V35RoutingGenerationStorage {
    Construction(Vec<V35LeafPatchArm>),
    Serving(RecordBatch),
}

impl V35RoutingGeneration {
    /// Number of logical leaves.
    pub fn leaf_count(&self) -> usize {
        self.leaf_count
    }

    /// Patches stored for every logical leaf.
    pub fn patches_per_leaf(&self) -> u8 {
        self.patches_per_leaf
    }

    /// Source and resident routing dimensions.
    pub fn dimensions(&self) -> V35Dimensions {
        self.dimensions
    }

    /// Logical checksum of the exact projection representation.
    pub fn projection_checksum(&self) -> [u8; 32] {
        self.projection_checksum
    }

    /// Immutable source identifier.
    pub fn source_id(&self) -> &str {
        &self.source_id
    }

    /// Immutable source archive SHA-256.
    pub fn source_archive_sha256(&self) -> &str {
        &self.source_archive_sha256
    }

    /// Exact compact numeric leaf bytes, excluding Arrow framing.
    pub fn resident_numeric_bytes(&self) -> usize {
        self.resident_numeric_bytes
    }

    /// Whether decoded serving state retains one columnar Arrow batch.
    pub fn is_columnar_serving(&self) -> bool {
        match &self.storage {
            V35RoutingGenerationStorage::Serving(batch) => {
                batch.num_rows() == self.leaf_count * usize::from(self.patches_per_leaf)
            }
            V35RoutingGenerationStorage::Construction(_) => false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct V35RoutingGenerationManifest {
    dimensions: V35Dimensions,
    format: String,
    leaf_count: u64,
    patches_per_leaf: u8,
    projection_algorithm: String,
    projection_arm: V35ProjectionArm,
    projection_checksum_sha256: String,
    projection_sample_sha256: Option<String>,
    projection_training_descriptor: String,
    role: String,
    source_archive_sha256: String,
    source_id: String,
    storage_order: String,
    uri: String,
}

impl V35EqualByteCentroidControl {
    /// Number of f32 centers admitted by the matched patch budget.
    pub fn centroid_count(&self) -> usize {
        self.centers.len()
    }

    /// Canonically ordered f32 centers.
    pub fn centers(&self) -> &[Vec<f32>] {
        &self.centers
    }

    /// Exact matched resident byte envelope.
    pub fn encoded_bytes(&self) -> usize {
        self.encoded_bytes
    }
}

impl V35LeafPatch {
    /// Dense generation-local leaf ordinal.
    pub fn leaf_ordinal(&self) -> u32 {
        self.leaf_ordinal
    }

    /// Dense storage-group ordinal.
    pub fn group_ordinal(&self) -> u32 {
        self.group_ordinal
    }

    /// First logical row in this leaf.
    pub fn logical_start(&self) -> u64 {
        self.logical_start
    }

    /// Number of rows summarized by this patch.
    pub fn population(&self) -> u32 {
        self.population
    }

    /// Inclusive source-assignment bounds.
    pub fn assignment_bounds(&self) -> (u64, u64) {
        (self.assignment_min, self.assignment_max)
    }

    /// Exact admitted encoded bytes for the patch planes and fixed envelope.
    pub fn encoded_numeric_bytes(&self) -> usize {
        8 * usize::from(self.dimensions.routing) + 128
    }

    /// Signed quantized projected-mean plane.
    pub fn mean_codes(&self) -> &[i8] {
        &self.mean_codes
    }

    /// Shared f32 scale for the signed projected-mean plane.
    pub fn mean_scale(&self) -> f32 {
        self.mean_scale
    }

    /// Unsigned quantized residual-diagonal plane.
    pub fn residual_codes(&self) -> &[u8] {
        &self.residual_codes
    }

    /// Shared f32 scale for the unsigned residual-diagonal plane.
    pub fn residual_scale(&self) -> f32 {
        self.residual_scale
    }

    /// Six concatenated signed quantized residual directions.
    pub fn direction_codes(&self) -> &[i8] {
        &self.direction_codes
    }

    /// Per-direction decoded f32 scales.
    pub fn direction_scales(&self) -> &[f32; COMPONENTS] {
        &self.direction_scales
    }

    /// Nonnegative per-direction covariance weights.
    pub fn weights(&self) -> &[f32; COMPONENTS] {
        &self.weights
    }

    /// Trace recomputed from decoded planes.
    pub fn trace(&self) -> f64 {
        self.trace
    }

    /// Deterministic `sqrt(2*ln(population))` score factor.
    pub fn population_factor(&self) -> f64 {
        self.population_factor
    }

    /// Squared covariance trace recomputed from decoded planes.
    pub fn trace_square(&self) -> f64 {
        self.trace_square
    }

    /// Conservative decoded covariance spectral bound.
    pub fn spectral_bound(&self) -> f64 {
        self.spectral_bound
    }

    /// Mean clamped source-space energy omitted by projection.
    pub fn omitted_energy(&self) -> f32 {
        self.omitted_energy
    }
}

fn quantize_i8(values: &[f64]) -> Result<(f32, Vec<i8>)> {
    let maximum = values.iter().try_fold(0.0_f64, |maximum, value| {
        if value.is_finite() {
            Ok(maximum.max(value.abs()))
        } else {
            Err(invalid("V35 patch signed plane is nonfinite"))
        }
    })?;
    if maximum == 0.0 {
        return Ok((0.0, vec![0; values.len()]));
    }
    let scale = (maximum / 127.0) as f32;
    if scale == 0.0 {
        return Ok((0.0, vec![0; values.len()]));
    }
    if !scale.is_finite() || scale < 0.0 {
        return Err(invalid("V35 patch signed scale differs"));
    }
    let codes = values
        .iter()
        .map(|value| {
            (value / f64::from(scale))
                .round_ties_even()
                .clamp(-127.0, 127.0) as i8
        })
        .collect();
    Ok((scale, codes))
}

fn quantize_u8(values: &[f64]) -> Result<(f32, Vec<u8>)> {
    let maximum = values.iter().try_fold(0.0_f64, |maximum, value| {
        if value.is_finite() && *value >= 0.0 {
            Ok(maximum.max(*value))
        } else {
            Err(invalid("V35 patch unsigned plane differs"))
        }
    })?;
    if maximum == 0.0 {
        return Ok((0.0, vec![0; values.len()]));
    }
    let scale = (maximum / 255.0) as f32;
    if scale == 0.0 {
        return Ok((0.0, vec![0; values.len()]));
    }
    if !scale.is_finite() || scale < 0.0 {
        return Err(invalid("V35 patch unsigned scale differs"));
    }
    let codes = values
        .iter()
        .map(|value| {
            (value / f64::from(scale))
                .round_ties_even()
                .clamp(0.0, 255.0) as u8
        })
        .collect();
    Ok((scale, codes))
}

fn decode_i8(codes: &[i8], scale: f32) -> Vec<f64> {
    codes
        .iter()
        .map(|code| f64::from(*code) * f64::from(scale))
        .collect()
}

fn decode_u8(codes: &[u8], scale: f32) -> Vec<f64> {
    codes
        .iter()
        .map(|code| f64::from(*code) * f64::from(scale))
        .collect()
}

fn canonicalize_direction(direction: &mut [f64]) -> Result<()> {
    let (_, pivot) = direction
        .iter()
        .enumerate()
        .max_by(|(left_index, left), (right_index, right)| {
            left.abs()
                .total_cmp(&right.abs())
                .then_with(|| right_index.cmp(left_index))
        })
        .ok_or_else(|| invalid("V35 patch direction is empty"))?;
    if *pivot < 0.0 {
        direction.iter_mut().for_each(|value| *value = -*value);
    }
    Ok(())
}

fn same_eigenvalue_cluster(left: f64, right: f64, scale: f64) -> bool {
    (left - right).abs() <= scale.max(1.0) * 256.0 * f64::EPSILON
}

fn canonical_cluster_directions(
    eigenvectors: &DMatrix<f64>,
    cluster: &[usize],
) -> Result<Vec<Vec<f64>>> {
    let width = eigenvectors.nrows();
    let mut accepted = Vec::<Vec<f64>>::with_capacity(cluster.len());
    for axis in 0..width {
        let mut candidate = (0..width)
            .map(|row| {
                cluster.iter().fold(0.0_f64, |sum, column| {
                    eigenvectors[(row, *column)].mul_add(eigenvectors[(axis, *column)], sum)
                })
            })
            .collect::<Vec<_>>();
        let original_norm = candidate
            .iter()
            .fold(0.0_f64, |sum, value| value.mul_add(*value, sum))
            .sqrt();
        for _ in 0..2 {
            for prior in &accepted {
                let dot = candidate
                    .iter()
                    .zip(prior)
                    .fold(0.0_f64, |sum, (left, right)| left.mul_add(*right, sum));
                for (value, prior_value) in candidate.iter_mut().zip(prior) {
                    *value = (-dot).mul_add(*prior_value, *value);
                }
            }
        }
        let norm = candidate
            .iter()
            .fold(0.0_f64, |sum, value| value.mul_add(*value, sum))
            .sqrt();
        if norm.is_finite() && original_norm.is_finite() && norm > original_norm * 1e-12 {
            let inverse = norm.recip();
            candidate.iter_mut().for_each(|value| *value *= inverse);
            canonicalize_direction(&mut candidate)?;
            accepted.push(candidate);
            if accepted.len() == cluster.len() {
                return Ok(accepted);
            }
        }
    }
    Err(invalid("V35 patch eigenspace canonicalization failed"))
}

fn next_up(value: f64) -> f64 {
    if value.is_infinite() || value.is_nan() {
        return value;
    }
    if value == 0.0 {
        return f64::from_bits(1);
    }
    if value > 0.0 {
        f64::from_bits(value.to_bits() + 1)
    } else {
        f64::from_bits(value.to_bits() - 1)
    }
}

fn multiply_nonnegative_up(left: f64, right: f64) -> f64 {
    if left == 0.0 || right == 0.0 {
        0.0
    } else {
        next_up(left * right)
    }
}

fn add_nonnegative_up(left: f64, right: f64) -> f64 {
    if right == 0.0 {
        left
    } else {
        next_up(left + right)
    }
}

fn deterministic_ln_u32(value: u32) -> f64 {
    if value == 1 {
        return 0.0;
    }
    const LN_2: f64 = f64::from_bits(0x3fe6_2e42_fefa_39ef);
    let exponent = 31 - value.leading_zeros();
    let scale = (1_u64 << exponent) as f64;
    let mantissa = f64::from(value) / scale;
    let z = (mantissa - 1.0) / (mantissa + 1.0);
    let z_square = z * z;
    let mut power = z;
    let mut series = 0.0_f64;
    for term in 0..32_u32 {
        series += power / f64::from(2 * term + 1);
        power *= z_square;
    }
    f64::from(exponent) * LN_2 + 2.0 * series
}

fn recompute_moments(
    residual: &[f64],
    directions: &[Vec<f64>],
    weights: &[f32; COMPONENTS],
) -> Result<(f64, f64, f64)> {
    let mut trace = residual.iter().sum::<f64>();
    let mut trace_square = residual.iter().map(|value| value * value).sum::<f64>();
    let mut spectral_bound = residual.iter().copied().fold(0.0_f64, f64::max);
    for component in 0..COMPONENTS {
        let weight = f64::from(weights[component]);
        let norm_squared = directions[component]
            .iter()
            .fold(0.0_f64, |sum, value| value.mul_add(*value, sum));
        let diagonal_cross = residual
            .iter()
            .zip(&directions[component])
            .fold(0.0_f64, |sum, (diagonal, value)| {
                (diagonal * value).mul_add(*value, sum)
            });
        trace += weight * norm_squared;
        trace_square += 2.0 * weight * diagonal_cross;
        let mut norm_squared_up = 0.0_f64;
        for value in &directions[component] {
            norm_squared_up = add_nonnegative_up(
                norm_squared_up,
                multiply_nonnegative_up(value.abs(), value.abs()),
            );
        }
        spectral_bound = add_nonnegative_up(
            spectral_bound,
            multiply_nonnegative_up(weight, norm_squared_up),
        );
    }
    for left in 0..COMPONENTS {
        for right in 0..COMPONENTS {
            let dot = directions[left]
                .iter()
                .zip(&directions[right])
                .fold(0.0_f64, |sum, (a, b)| a.mul_add(*b, sum));
            trace_square += f64::from(weights[left]) * f64::from(weights[right]) * dot * dot;
        }
    }
    if [trace, trace_square, spectral_bound]
        .iter()
        .any(|value| !value.is_finite() || *value < 0.0)
    {
        return Err(invalid("V35 patch decoded moments differ"));
    }
    Ok((trace, trace_square, spectral_bound))
}

fn validate_patch_build_request(request: &V35LeafPatchBuildRequest) -> Result<(usize, usize)> {
    let routing = usize::from(request.dimensions.routing);
    let population = request.projected_rows.len();
    if request.dimensions.source == 0
        || !matches!(request.dimensions.routing, 64 | 128 | 192)
        || u32::from(request.dimensions.routing) > request.dimensions.source
        || population == 0
        || population > MAX_LEAF_ROWS
        || request.omitted_energies.len() != population
        || request.assignment_min > request.assignment_max
        || request
            .logical_start
            .checked_add(u64::try_from(population).unwrap_or(u64::MAX))
            .is_none()
        || request
            .projected_rows
            .iter()
            .any(|row| row.len() != routing || row.iter().any(|value| !value.is_finite()))
        || request
            .omitted_energies
            .iter()
            .any(|value| !value.is_finite() || *value < 0.0)
    {
        return Err(invalid("V35 patch build request differs"));
    }
    Ok((routing, population))
}

/// Seal one at-most-256-row projected leaf into a compact quantized patch.
pub fn build_v35_leaf_patch(request: &V35LeafPatchBuildRequest) -> Result<V35LeafPatch> {
    let (routing, population) = validate_patch_build_request(request)?;
    let population_f64 = population as f64;
    let mut mean = vec![0.0_f64; routing];
    let mut omitted_sum = 0.0_f64;
    for (row, omitted) in request.projected_rows.iter().zip(&request.omitted_energies) {
        for (sum, value) in mean.iter_mut().zip(row) {
            *sum += f64::from(*value);
        }
        omitted_sum += *omitted;
    }
    mean.iter_mut().for_each(|value| *value /= population_f64);
    let packed_len = routing
        .checked_mul(routing + 1)
        .and_then(|values| values.checked_div(2))
        .ok_or_else(|| invalid("V35 patch moment allocation overflows"))?;
    let mut packed_covariance = vec![0.0_f64; packed_len];
    for row in &request.projected_rows {
        for left in 0..routing {
            let left_delta = f64::from(row[left]) - mean[left];
            for right in left..routing {
                let index = packed_upper_index(routing, left, right);
                packed_covariance[index] = left_delta.mul_add(
                    f64::from(row[right]) - mean[right],
                    packed_covariance[index],
                );
            }
        }
    }
    packed_covariance
        .iter_mut()
        .for_each(|value| *value /= population_f64);
    let covariance_diagonal = (0..routing)
        .map(|dimension| packed_covariance[packed_upper_index(routing, dimension, dimension)])
        .collect::<Vec<_>>();
    let dense_len = routing
        .checked_mul(routing)
        .ok_or_else(|| invalid("V35 patch eigensolver allocation overflows"))?;
    packed_covariance.resize(dense_len, 0.0);
    for left in (0..routing).rev() {
        for right in (left..routing).rev() {
            let source = packed_upper_index(routing, left, right);
            let target = left + right * routing;
            packed_covariance[target] = packed_covariance[source];
        }
    }
    for left in 0..routing {
        for right in 0..left {
            packed_covariance[left + right * routing] = packed_covariance[right + left * routing];
        }
    }
    let covariance = DMatrix::<f64>::from_vec(routing, routing, packed_covariance);
    let eigen = SymmetricEigen::new(covariance);
    let mut order = (0..routing).collect::<Vec<_>>();
    order.sort_by(|left, right| {
        eigen.eigenvalues[*right]
            .total_cmp(&eigen.eigenvalues[*left])
            .then_with(|| left.cmp(right))
    });
    let spectrum_scale = eigen
        .eigenvalues
        .iter()
        .fold(0.0_f64, |scale, value| scale.max(value.abs()));
    let mut raw_directions = Vec::with_capacity(COMPONENTS);
    let mut weights = [0.0_f32; COMPONENTS];
    let mut start = 0;
    while start < order.len() && raw_directions.len() < COMPONENTS {
        let reference = eigen.eigenvalues[order[start]];
        let mut end = start + 1;
        while end < order.len()
            && same_eigenvalue_cluster(reference, eigen.eigenvalues[order[end]], spectrum_scale)
        {
            end += 1;
        }
        let cluster_weight = order[start..end].iter().fold(0.0_f64, |sum, index| {
            sum + eigen.eigenvalues[*index].max(0.0)
        }) / (end - start) as f64;
        for direction in canonical_cluster_directions(&eigen.eigenvectors, &order[start..end])? {
            if raw_directions.len() == COMPONENTS {
                break;
            }
            let component = raw_directions.len();
            weights[component] = cluster_weight as f32;
            raw_directions.push(if weights[component] == 0.0 {
                vec![0.0; routing]
            } else {
                direction
            });
        }
        start = end;
    }
    if raw_directions.len() != COMPONENTS {
        return Err(invalid("V35 patch retained eigenspace differs"));
    }
    let (mean_scale, mean_codes) = quantize_i8(&mean)?;
    let mut direction_codes = Vec::with_capacity(COMPONENTS * routing);
    let mut direction_scales = [0.0_f32; COMPONENTS];
    let mut decoded_directions = Vec::with_capacity(COMPONENTS);
    for (component, direction) in raw_directions.iter().enumerate() {
        let (scale, codes) = quantize_i8(direction)?;
        direction_scales[component] = scale;
        decoded_directions.push(decode_i8(&codes, scale));
        direction_codes.extend(codes);
    }
    let mut residual = (0..routing)
        .map(|dimension| {
            let explained = (0..COMPONENTS).fold(0.0_f64, |sum, component| {
                let value = decoded_directions[component][dimension];
                f64::from(weights[component]).mul_add(value * value, sum)
            });
            (covariance_diagonal[dimension] - explained).max(0.0)
        })
        .collect::<Vec<_>>();
    let (residual_scale, residual_codes) = quantize_u8(&residual)?;
    residual = decode_u8(&residual_codes, residual_scale);
    let (trace, trace_square, spectral_bound) =
        recompute_moments(&residual, &decoded_directions, &weights)?;
    let omitted_energy = (omitted_sum / population_f64) as f32;
    if !omitted_energy.is_finite() || omitted_energy < 0.0 {
        return Err(invalid("V35 patch omitted energy differs"));
    }
    Ok(V35LeafPatch {
        assignment_max: request.assignment_max,
        assignment_min: request.assignment_min,
        dimensions: request.dimensions,
        group_ordinal: request.group_ordinal,
        leaf_ordinal: request.leaf_ordinal,
        logical_start: request.logical_start,
        population: u32::try_from(population)
            .map_err(|_| invalid("V35 patch population overflows"))?,
        mean_codes,
        mean_scale,
        residual_codes,
        residual_scale,
        direction_codes,
        direction_scales,
        weights,
        trace,
        trace_square,
        population_factor: (2.0
            * deterministic_ln_u32(
                u32::try_from(population).map_err(|_| invalid("V35 patch population overflows"))?,
            ))
        .sqrt(),
        spectral_bound,
        omitted_energy,
    })
}

fn validate_patch_count(patch_count: u8) -> Result<usize> {
    if !matches!(patch_count, 1 | 2) {
        return Err(invalid("V35 patch arm count differs"));
    }
    Ok(usize::from(patch_count))
}

fn split_projected_rows(
    request: &V35LeafPatchBuildRequest,
    parts: usize,
) -> Result<Vec<Vec<usize>>> {
    let (routing, _) = validate_patch_build_request(request)?;
    if parts == 0 || parts > request.projected_rows.len() {
        return Err(invalid("V35 patch control split differs"));
    }
    let mut groups = vec![(0..request.projected_rows.len()).collect::<Vec<_>>()];
    while groups.len() < parts {
        let mut choice = None::<(f64, usize, usize)>;
        for (group_ordinal, group) in groups.iter().enumerate() {
            if group.len() < 2 {
                continue;
            }
            for dimension in 0..routing {
                let mean = group.iter().fold(0.0_f64, |sum, row| {
                    sum + f64::from(request.projected_rows[*row][dimension])
                }) / group.len() as f64;
                let weighted_variance = group.iter().fold(0.0_f64, |sum, row| {
                    let delta = f64::from(request.projected_rows[*row][dimension]) - mean;
                    delta.mul_add(delta, sum)
                });
                let candidate = (weighted_variance, group_ordinal, dimension);
                if choice.is_none_or(|current| {
                    candidate.0.total_cmp(&current.0).is_gt()
                        || (candidate.0.to_bits() == current.0.to_bits()
                            && (candidate.1, candidate.2) < (current.1, current.2))
                }) {
                    choice = Some(candidate);
                }
            }
        }
        let (_, group_ordinal, dimension) =
            choice.ok_or_else(|| invalid("V35 patch control cannot split population"))?;
        let mut ordered = groups.remove(group_ordinal);
        ordered.sort_by(|left, right| {
            request.projected_rows[*left][dimension]
                .total_cmp(&request.projected_rows[*right][dimension])
                .then_with(|| left.cmp(right))
        });
        let mut right = ordered.split_off(ordered.len() / 2);
        ordered.sort_unstable();
        right.sort_unstable();
        groups.insert(group_ordinal, right);
        groups.insert(group_ordinal, ordered);
    }
    Ok(groups)
}

fn subgroup_request(
    request: &V35LeafPatchBuildRequest,
    rows: &[usize],
) -> V35LeafPatchBuildRequest {
    V35LeafPatchBuildRequest {
        assignment_max: request.assignment_max,
        assignment_min: request.assignment_min,
        dimensions: request.dimensions,
        group_ordinal: request.group_ordinal,
        leaf_ordinal: request.leaf_ordinal,
        logical_start: request.logical_start,
        omitted_energies: rows
            .iter()
            .map(|row| request.omitted_energies[*row])
            .collect(),
        projected_rows: rows
            .iter()
            .map(|row| request.projected_rows[*row].clone())
            .collect(),
    }
}

/// Build one or two patches using the registered deterministic variance split.
pub fn build_v35_leaf_patch_arm(
    request: &V35LeafPatchBuildRequest,
    patch_count: u8,
) -> Result<V35LeafPatchArm> {
    let patch_count = validate_patch_count(patch_count)?;
    let groups = split_projected_rows(request, patch_count)?;
    let patches = groups
        .iter()
        .map(|rows| build_v35_leaf_patch(&subgroup_request(request, rows)))
        .collect::<Result<Vec<_>>>()?;
    let encoded_bytes = patches
        .iter()
        .try_fold(0_usize, |sum, patch| {
            sum.checked_add(patch.encoded_numeric_bytes())
        })
        .ok_or_else(|| invalid("V35 patch arm bytes overflow"))?;
    Ok(V35LeafPatchArm {
        patches,
        encoded_bytes,
    })
}

/// Score a patch arm by its minimum exact decoded scalar patch score.
pub fn score_v35_leaf_patch_arm(
    arm: &V35LeafPatchArm,
    projected_query: &[f64],
    query_omitted_energy: f64,
) -> Result<f64> {
    arm.patches.iter().try_fold(f64::INFINITY, |best, patch| {
        Ok(best.min(score_v35_leaf_patch(
            patch,
            projected_query,
            query_omitted_energy,
        )?))
    })
}

/// Build the exact-byte-matched f32 extra-centroid control.
pub fn build_v35_equal_byte_centroid_control(
    request: &V35LeafPatchBuildRequest,
    patch_count: u8,
) -> Result<V35EqualByteCentroidControl> {
    let patch_count = validate_patch_count(patch_count)?;
    let groups = split_projected_rows(request, patch_count * 2)?;
    let routing = usize::from(request.dimensions.routing);
    let mut centers = groups
        .iter()
        .map(|rows| {
            (0..routing)
                .map(|dimension| {
                    (rows.iter().fold(0.0_f64, |sum, row| {
                        sum + f64::from(request.projected_rows[*row][dimension])
                    }) / rows.len() as f64) as f32
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    centers.sort_by(|left, right| {
        left.iter()
            .zip(right)
            .find_map(|(a, b)| {
                let ordering = a.total_cmp(b);
                (!ordering.is_eq()).then_some(ordering)
            })
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    let encoded_bytes = patch_count
        .checked_mul(
            routing
                .checked_mul(8)
                .and_then(|value| value.checked_add(128))
                .ok_or_else(|| invalid("V35 centroid control bytes overflow"))?,
        )
        .ok_or_else(|| invalid("V35 centroid control bytes overflow"))?;
    Ok(V35EqualByteCentroidControl {
        dimensions: request.dimensions,
        centers,
        encoded_bytes,
    })
}

/// Score the equal-byte centroid control by minimum projected squared L2.
pub fn score_v35_equal_byte_centroid_control(
    control: &V35EqualByteCentroidControl,
    projected_query: &[f64],
) -> Result<f64> {
    if projected_query.len() != usize::from(control.dimensions.routing)
        || projected_query.iter().any(|value| !value.is_finite())
    {
        return Err(invalid("V35 centroid control query differs"));
    }
    control
        .centers
        .iter()
        .try_fold(f64::INFINITY, |best, center| {
            let distance =
                projected_query
                    .iter()
                    .zip(center)
                    .fold(0.0_f64, |sum, (query, value)| {
                        let delta = *query - f64::from(*value);
                        delta.mul_add(delta, sum)
                    });
            if distance.is_finite() {
                Ok(best.min(distance))
            } else {
                Err(invalid("V35 centroid control score differs"))
            }
        })
}

/// Evaluate the exact increasing-dimension f64 stored-patch heuristic.
pub fn score_v35_leaf_patch(
    patch: &V35LeafPatch,
    projected_query: &[f64],
    query_omitted_energy: f64,
) -> Result<f64> {
    let routing = usize::from(patch.dimensions.routing);
    if projected_query.len() != routing
        || projected_query.iter().any(|value| !value.is_finite())
        || !query_omitted_energy.is_finite()
        || query_omitted_energy < 0.0
    {
        return Err(invalid("V35 patch query differs"));
    }
    let mean = decode_i8(&patch.mean_codes, patch.mean_scale);
    let residual = decode_u8(&patch.residual_codes, patch.residual_scale);
    let directions = patch
        .direction_codes
        .chunks_exact(routing)
        .zip(patch.direction_scales)
        .map(|(codes, scale)| decode_i8(codes, scale))
        .collect::<Vec<_>>();
    let mut delta = vec![0.0_f64; routing];
    let mut distance = 0.0_f64;
    let mut covariance_projection = 0.0_f64;
    for dimension in 0..routing {
        delta[dimension] = projected_query[dimension] - mean[dimension];
        distance = delta[dimension].mul_add(delta[dimension], distance);
        covariance_projection = (residual[dimension] * delta[dimension])
            .mul_add(delta[dimension], covariance_projection);
    }
    for (component, direction) in directions.iter().enumerate() {
        let projection = direction
            .iter()
            .zip(&delta)
            .fold(0.0_f64, |sum, (direction, value)| {
                direction.mul_add(*value, sum)
            });
        covariance_projection = f64::from(patch.weights[component])
            .mul_add(projection * projection, covariance_projection);
    }
    let radicand = 2.0 * patch.trace_square + 4.0 * covariance_projection;
    if !radicand.is_finite() || radicand < 0.0 {
        return Err(invalid("V35 patch score radicand differs"));
    }
    let complement = (query_omitted_energy.sqrt() - f64::from(patch.omitted_energy).sqrt()).powi(2);
    let score = distance + patch.trace - patch.population_factor * radicand.sqrt() + complement;
    if !score.is_finite() {
        return Err(invalid("V35 patch score differs"));
    }
    Ok(score)
}

fn valid_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn checksum_hex(checksum: [u8; 32]) -> String {
    checksum.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn validate_patch_payload(patch: &V35LeafPatch) -> Result<()> {
    let routing = usize::from(patch.dimensions.routing);
    if !matches!(patch.dimensions.routing, 64 | 128 | 192)
        || patch.dimensions.source == 0
        || u32::from(patch.dimensions.routing) > patch.dimensions.source
        || patch.population == 0
        || patch.population as usize > MAX_LEAF_ROWS
        || patch.assignment_min > patch.assignment_max
        || patch.mean_codes.len() != routing
        || patch.residual_codes.len() != routing
        || patch.direction_codes.len() != COMPONENTS * routing
        || !patch.mean_scale.is_finite()
        || patch.mean_scale < 0.0
        || (patch.mean_scale == 0.0 && patch.mean_scale.to_bits() != 0)
        || !patch.residual_scale.is_finite()
        || patch.residual_scale < 0.0
        || (patch.residual_scale == 0.0 && patch.residual_scale.to_bits() != 0)
        || patch
            .direction_scales
            .iter()
            .chain(&patch.weights)
            .any(|value| {
                !value.is_finite() || *value < 0.0 || (*value == 0.0 && value.to_bits() != 0)
            })
        || !patch.omitted_energy.is_finite()
        || patch.omitted_energy < 0.0
        || (patch.omitted_energy == 0.0 && patch.omitted_energy.to_bits() != 0)
        || (patch.mean_scale == 0.0 && patch.mean_codes.iter().any(|code| *code != 0))
        || (patch.residual_scale == 0.0 && patch.residual_codes.iter().any(|code| *code != 0))
        || (0..COMPONENTS).any(|component| {
            (patch.direction_scales[component] == 0.0 || patch.weights[component] == 0.0)
                && patch.direction_codes[component * routing..(component + 1) * routing]
                    .iter()
                    .any(|code| *code != 0)
        })
    {
        return Err(invalid("V35 routing patch payload differs"));
    }
    let residual = decode_u8(&patch.residual_codes, patch.residual_scale);
    let directions = patch
        .direction_codes
        .chunks_exact(routing)
        .zip(patch.direction_scales)
        .map(|(codes, scale)| decode_i8(codes, scale))
        .collect::<Vec<_>>();
    let (trace, trace_square, spectral_bound) =
        recompute_moments(&residual, &directions, &patch.weights)?;
    let population_factor = (2.0 * deterministic_ln_u32(patch.population)).sqrt();
    if patch.trace.to_bits() != trace.to_bits()
        || patch.trace_square.to_bits() != trace_square.to_bits()
        || patch.spectral_bound.to_bits() != spectral_bound.to_bits()
        || patch.population_factor.to_bits() != population_factor.to_bits()
    {
        return Err(invalid("V35 routing patch cached moments differ"));
    }
    Ok(())
}

fn validate_generation(generation: &V35RoutingGeneration) -> Result<()> {
    if generation.source_id.is_empty()
        || !valid_sha256(&generation.source_archive_sha256)
        || generation.leaf_count == 0
        || !matches!(generation.patches_per_leaf, 1 | 2)
    {
        return Err(invalid("V35 routing generation authority differs"));
    }
    let V35RoutingGenerationStorage::Construction(leaves) = &generation.storage else {
        return Ok(());
    };
    if leaves.len() != generation.leaf_count {
        return Err(invalid("V35 routing generation leaf count differs"));
    }
    let mut previous_group = None::<u32>;
    let mut expected_logical_start = 0_u64;
    for (leaf_ordinal, arm) in leaves.iter().enumerate() {
        if arm.patch_count() != usize::from(generation.patches_per_leaf) || arm.patches.is_empty() {
            return Err(invalid("V35 routing generation patch count differs"));
        }
        let first = &arm.patches[0];
        for patch in &arm.patches {
            validate_patch_payload(patch)?;
        }
        if first.leaf_ordinal != u32::try_from(leaf_ordinal).unwrap_or(u32::MAX)
            || first.dimensions != generation.dimensions
            || arm.patches.iter().any(|patch| {
                patch.leaf_ordinal != first.leaf_ordinal
                    || patch.group_ordinal != first.group_ordinal
                    || patch.logical_start != first.logical_start
                    || patch.assignment_min != first.assignment_min
                    || patch.assignment_max != first.assignment_max
                    || patch.dimensions != first.dimensions
            })
            || previous_group.is_some_and(|group| first.group_ordinal < group)
            || first.logical_start != expected_logical_start
        {
            return Err(invalid("V35 routing generation logical order differs"));
        }
        expected_logical_start = expected_logical_start
            .checked_add(u64::from(arm.total_population()))
            .ok_or_else(|| invalid("V35 routing generation logical interval overflows"))?;
        previous_group = Some(first.group_ordinal);
    }
    Ok(())
}

/// Bind compact leaf arms to one exact projection and immutable source.
pub fn build_v35_routing_generation(
    projection: &V35Projection,
    source_id: &str,
    source_archive_sha256: &str,
    leaves: Vec<V35LeafPatchArm>,
) -> Result<V35RoutingGeneration> {
    let patches_per_leaf = leaves
        .first()
        .and_then(|arm| u8::try_from(arm.patch_count()).ok())
        .ok_or_else(|| invalid("V35 routing generation is empty"))?;
    let generation = V35RoutingGeneration {
        dimensions: projection.dimensions(),
        projection_arm: projection.arm(),
        projection_checksum: projection.checksum(),
        projection_algorithm: projection.algorithm().to_owned(),
        projection_sample_sha256: projection.sample_sha256().map(ToOwned::to_owned),
        projection_training_descriptor: projection.training_descriptor().to_owned(),
        source_id: source_id.to_owned(),
        source_archive_sha256: source_archive_sha256.to_owned(),
        patches_per_leaf,
        leaf_count: leaves.len(),
        resident_numeric_bytes: leaves.iter().map(V35LeafPatchArm::encoded_bytes).sum(),
        storage: V35RoutingGenerationStorage::Construction(leaves),
    };
    validate_generation(&generation)?;
    Ok(generation)
}

fn canonical_json(value: serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Array(values) => {
            serde_json::Value::Array(values.into_iter().map(canonical_json).collect())
        }
        serde_json::Value::Object(values) => serde_json::Value::Object(
            values
                .into_iter()
                .map(|(key, value)| (key, canonical_json(value)))
                .collect::<BTreeMap<_, _>>()
                .into_iter()
                .collect(),
        ),
        scalar => scalar,
    }
}

fn generation_manifest(
    generation: &V35RoutingGeneration,
    role: &str,
    uri: &str,
) -> Result<V35RoutingGenerationManifest> {
    validate_generation(generation)?;
    if !matches!(role, "active-routing" | "retiring-routing") || uri.is_empty() {
        return Err(invalid("V35 routing generation artifact identity differs"));
    }
    Ok(V35RoutingGenerationManifest {
        dimensions: generation.dimensions,
        format: GENERATION_FORMAT.to_owned(),
        leaf_count: u64::try_from(generation.leaf_count)
            .map_err(|_| invalid("V35 routing generation leaf count overflows"))?,
        patches_per_leaf: generation.patches_per_leaf,
        projection_algorithm: generation.projection_algorithm.clone(),
        projection_arm: generation.projection_arm,
        projection_checksum_sha256: checksum_hex(generation.projection_checksum),
        projection_sample_sha256: generation.projection_sample_sha256.clone(),
        projection_training_descriptor: generation.projection_training_descriptor.clone(),
        role: role.to_owned(),
        source_archive_sha256: generation.source_archive_sha256.clone(),
        source_id: generation.source_id.clone(),
        storage_order: "logical-leaf-patch-major".to_owned(),
        uri: uri.to_owned(),
    })
}

fn canonical_manifest(manifest: &V35RoutingGenerationManifest) -> Result<String> {
    let value = serde_json::to_value(manifest)
        .map_err(|_| invalid("V35 routing generation manifest differs"))?;
    serde_json::to_string(&canonical_json(value))
        .map_err(|_| invalid("V35 routing generation manifest differs"))
}

fn fixed_list_type(name: &str, data_type: DataType, width: usize) -> Result<DataType> {
    Ok(DataType::FixedSizeList(
        Arc::new(Field::new(name, data_type, false)),
        i32::try_from(width).map_err(|_| invalid("V35 routing list width overflows"))?,
    ))
}

fn generation_schema(manifest: &V35RoutingGenerationManifest) -> Result<Arc<Schema>> {
    let routing = usize::from(manifest.dimensions.routing);
    let fields = vec![
        Field::new("leaf_ordinal", DataType::UInt32, false),
        Field::new("group_ordinal", DataType::UInt32, false),
        Field::new("logical_start", DataType::UInt64, false),
        Field::new("population", DataType::UInt32, false),
        Field::new("assignment_min", DataType::UInt64, false),
        Field::new("assignment_max", DataType::UInt64, false),
        Field::new(
            "mean_codes",
            fixed_list_type("item", DataType::Int8, routing)?,
            false,
        ),
        Field::new("mean_scale", DataType::Float32, false),
        Field::new(
            "residual_codes",
            fixed_list_type("item", DataType::UInt8, routing)?,
            false,
        ),
        Field::new("residual_scale", DataType::Float32, false),
        Field::new(
            "direction_codes",
            fixed_list_type("item", DataType::Int8, COMPONENTS * routing)?,
            false,
        ),
        Field::new(
            "direction_scales",
            fixed_list_type("item", DataType::Float32, COMPONENTS)?,
            false,
        ),
        Field::new(
            "weights",
            fixed_list_type("item", DataType::Float32, COMPONENTS)?,
            false,
        ),
        Field::new("trace", DataType::Float64, false),
        Field::new("trace_square", DataType::Float64, false),
        Field::new("population_factor", DataType::Float64, false),
        Field::new("spectral_bound", DataType::Float64, false),
        Field::new("omitted_energy", DataType::Float32, false),
    ];
    Ok(Arc::new(Schema::new_with_metadata(
        fields,
        std::collections::HashMap::from([(
            GENERATION_MANIFEST_KEY.to_owned(),
            canonical_manifest(manifest)?,
        )]),
    )))
}

fn fixed_i8(values: Vec<i8>, width: usize) -> Result<Arc<FixedSizeListArray>> {
    Ok(Arc::new(FixedSizeListArray::try_new(
        Arc::new(Field::new("item", DataType::Int8, false)),
        i32::try_from(width).map_err(|_| invalid("V35 routing list width overflows"))?,
        Arc::new(Int8Array::from(values)),
        None,
    )?))
}

fn fixed_u8(values: Vec<u8>, width: usize) -> Result<Arc<FixedSizeListArray>> {
    Ok(Arc::new(FixedSizeListArray::try_new(
        Arc::new(Field::new("item", DataType::UInt8, false)),
        i32::try_from(width).map_err(|_| invalid("V35 routing list width overflows"))?,
        Arc::new(UInt8Array::from(values)),
        None,
    )?))
}

fn fixed_f32(values: Vec<f32>, width: usize) -> Result<Arc<FixedSizeListArray>> {
    Ok(Arc::new(FixedSizeListArray::try_new(
        Arc::new(Field::new("item", DataType::Float32, false)),
        i32::try_from(width).map_err(|_| invalid("V35 routing list width overflows"))?,
        Arc::new(Float32Array::from(values)),
        None,
    )?))
}

/// Encode one compact routing generation as one strict Arrow IPC batch.
pub fn encode_v35_generation_arrow(
    generation: &V35RoutingGeneration,
    role: &str,
    uri: &str,
) -> Result<(Vec<u8>, V35ArtifactIdentity)> {
    let manifest = generation_manifest(generation, role, uri)?;
    let schema = generation_schema(&manifest)?;
    let routing = usize::from(generation.dimensions.routing);
    let V35RoutingGenerationStorage::Construction(leaves) = &generation.storage else {
        return Err(invalid("V35 serving generation cannot be re-encoded"));
    };
    let rows = leaves
        .iter()
        .flat_map(|arm| arm.patches.iter())
        .collect::<Vec<_>>();
    let mut leaf_ordinals = Vec::with_capacity(rows.len());
    for arm in leaves {
        for patch in &arm.patches {
            leaf_ordinals.push(patch.leaf_ordinal);
        }
    }
    let columns: Vec<Arc<dyn Array>> = vec![
        Arc::new(UInt32Array::from(leaf_ordinals)),
        Arc::new(UInt32Array::from_iter_values(
            rows.iter().map(|patch| patch.group_ordinal),
        )),
        Arc::new(UInt64Array::from_iter_values(
            rows.iter().map(|patch| patch.logical_start),
        )),
        Arc::new(UInt32Array::from_iter_values(
            rows.iter().map(|patch| patch.population),
        )),
        Arc::new(UInt64Array::from_iter_values(
            rows.iter().map(|patch| patch.assignment_min),
        )),
        Arc::new(UInt64Array::from_iter_values(
            rows.iter().map(|patch| patch.assignment_max),
        )),
        fixed_i8(
            rows.iter()
                .flat_map(|patch| patch.mean_codes.iter().copied())
                .collect(),
            routing,
        )?,
        Arc::new(Float32Array::from_iter_values(
            rows.iter().map(|patch| patch.mean_scale),
        )),
        fixed_u8(
            rows.iter()
                .flat_map(|patch| patch.residual_codes.iter().copied())
                .collect(),
            routing,
        )?,
        Arc::new(Float32Array::from_iter_values(
            rows.iter().map(|patch| patch.residual_scale),
        )),
        fixed_i8(
            rows.iter()
                .flat_map(|patch| patch.direction_codes.iter().copied())
                .collect(),
            COMPONENTS * routing,
        )?,
        fixed_f32(
            rows.iter()
                .flat_map(|patch| patch.direction_scales)
                .collect(),
            COMPONENTS,
        )?,
        fixed_f32(
            rows.iter().flat_map(|patch| patch.weights).collect(),
            COMPONENTS,
        )?,
        Arc::new(Float64Array::from_iter_values(
            rows.iter().map(|patch| patch.trace),
        )),
        Arc::new(Float64Array::from_iter_values(
            rows.iter().map(|patch| patch.trace_square),
        )),
        Arc::new(Float64Array::from_iter_values(
            rows.iter().map(|patch| patch.population_factor),
        )),
        Arc::new(Float64Array::from_iter_values(
            rows.iter().map(|patch| patch.spectral_bound),
        )),
        Arc::new(Float32Array::from_iter_values(
            rows.iter().map(|patch| patch.omitted_energy),
        )),
    ];
    let batch = RecordBatch::try_new(schema.clone(), columns)?;
    let mut bytes = Vec::new();
    let options = IpcWriteOptions::try_new(8, false, MetadataVersion::V5)?;
    let mut writer = FileWriter::try_new_with_options(&mut bytes, schema.as_ref(), options)?;
    writer.write(&batch)?;
    writer.finish()?;
    drop(writer);
    let identity = V35ArtifactIdentity {
        digest: format!("{:x}", Sha256::digest(&bytes)),
        digest_algorithm: "sha256".to_owned(),
        length: u64::try_from(bytes.len())
            .map_err(|_| invalid("V35 routing generation length overflows"))?,
        role: role.to_owned(),
        uri: uri.to_owned(),
    };
    Ok((bytes, identity))
}

fn list_i8(list: &FixedSizeListArray, row: usize, width: usize) -> Result<Vec<i8>> {
    let values = list.value(row);
    let array = values
        .as_any()
        .downcast_ref::<Int8Array>()
        .ok_or_else(|| invalid("V35 routing signed list differs"))?;
    if array.null_count() != 0 || array.len() != width {
        return Err(invalid("V35 routing signed list differs"));
    }
    Ok(array.values().to_vec())
}

fn list_u8(list: &FixedSizeListArray, row: usize, width: usize) -> Result<Vec<u8>> {
    let values = list.value(row);
    let array = values
        .as_any()
        .downcast_ref::<UInt8Array>()
        .ok_or_else(|| invalid("V35 routing unsigned list differs"))?;
    if array.null_count() != 0 || array.len() != width {
        return Err(invalid("V35 routing unsigned list differs"));
    }
    Ok(array.values().to_vec())
}

fn list_f32<const N: usize>(list: &FixedSizeListArray, row: usize) -> Result<[f32; N]> {
    let values = list.value(row);
    let array = values
        .as_any()
        .downcast_ref::<Float32Array>()
        .ok_or_else(|| invalid("V35 routing float list differs"))?;
    if array.null_count() != 0 || array.len() != N {
        return Err(invalid("V35 routing float list differs"));
    }
    array
        .values()
        .to_vec()
        .try_into()
        .map_err(|_| invalid("V35 routing float list differs"))
}

fn validate_generation_ipc_field(field: arrow_ipc::Field<'_>, expected: &Field) -> Result<()> {
    if field.name() != Some(expected.name().as_str())
        || field.nullable()
        || field.dictionary().is_some()
        || field
            .custom_metadata()
            .is_some_and(|values| !values.is_empty())
    {
        return Err(invalid("V35 routing IPC field differs"));
    }
    let children = match expected.data_type() {
        DataType::Int8 | DataType::UInt8 | DataType::UInt32 | DataType::UInt64 => {
            let integer = field
                .type_as_int()
                .ok_or_else(|| invalid("V35 routing IPC integer differs"))?;
            let (width, signed) = match expected.data_type() {
                DataType::Int8 => (8, true),
                DataType::UInt8 => (8, false),
                DataType::UInt32 => (32, false),
                DataType::UInt64 => (64, false),
                _ => unreachable!(),
            };
            if integer.bitWidth() != width || integer.is_signed() != signed {
                return Err(invalid("V35 routing IPC integer differs"));
            }
            Vec::new()
        }
        DataType::Float32 | DataType::Float64 => {
            let floating = field
                .type_as_floating_point()
                .ok_or_else(|| invalid("V35 routing IPC float differs"))?;
            let precision = if expected.data_type() == &DataType::Float32 {
                arrow_ipc::Precision::SINGLE
            } else {
                arrow_ipc::Precision::DOUBLE
            };
            if floating.precision() != precision {
                return Err(invalid("V35 routing IPC float differs"));
            }
            Vec::new()
        }
        DataType::FixedSizeList(child, width) => {
            if field
                .type_as_fixed_size_list()
                .is_none_or(|list| list.listSize() != *width)
            {
                return Err(invalid("V35 routing IPC fixed list differs"));
            }
            vec![child.as_ref()]
        }
        _ => return Err(invalid("V35 routing IPC type differs")),
    };
    let actual_children = field.children();
    if actual_children.map_or(0, |values| values.len()) != children.len() {
        return Err(invalid("V35 routing IPC children differ"));
    }
    for (index, child) in children.iter().enumerate() {
        validate_generation_ipc_field(
            actual_children
                .ok_or_else(|| invalid("V35 routing IPC child is missing"))?
                .get(index),
            child,
        )?;
    }
    Ok(())
}

fn validate_generation_ipc_schema(schema: arrow_ipc::Schema<'_>, expected: &Schema) -> Result<()> {
    if schema.endianness() != arrow_ipc::Endianness::Little
        || schema.features().is_some_and(|values| !values.is_empty())
    {
        return Err(invalid("V35 routing IPC schema features differ"));
    }
    let metadata = schema
        .custom_metadata()
        .ok_or_else(|| invalid("V35 routing IPC manifest is missing"))?;
    let expected_manifest = expected
        .metadata()
        .get(GENERATION_MANIFEST_KEY)
        .ok_or_else(|| invalid("V35 routing IPC manifest differs"))?;
    if metadata.len() != 1
        || metadata.get(0).key() != Some(GENERATION_MANIFEST_KEY)
        || metadata.get(0).value() != Some(expected_manifest.as_str())
    {
        return Err(invalid("V35 routing IPC manifest differs"));
    }
    let fields = schema
        .fields()
        .ok_or_else(|| invalid("V35 routing IPC fields are missing"))?;
    if fields.len() != expected.fields().len() {
        return Err(invalid("V35 routing IPC field count differs"));
    }
    for (index, expected_field) in expected.fields().iter().enumerate() {
        validate_generation_ipc_field(fields.get(index), expected_field)?;
    }
    Ok(())
}

fn validate_generation_ipc_envelope(
    bytes: &[u8],
    expected_schema: &Schema,
    rows: usize,
    routing: usize,
) -> Result<()> {
    if bytes.len() < 18 || !bytes.starts_with(b"ARROW1\0\0") || !bytes.ends_with(b"ARROW1") {
        return Err(invalid("V35 routing IPC magic differs"));
    }
    let trailer = bytes.len() - 10;
    let footer_len = u32::from_le_bytes(
        bytes[trailer..trailer + 4]
            .try_into()
            .map_err(|_| invalid("V35 routing IPC footer length differs"))?,
    ) as usize;
    let footer_start = trailer
        .checked_sub(footer_len)
        .filter(|offset| *offset >= 8)
        .ok_or_else(|| invalid("V35 routing IPC footer extent differs"))?;
    let footer = arrow_ipc::root_as_footer(&bytes[footer_start..trailer])
        .map_err(|_| invalid("V35 routing IPC footer differs"))?;
    validate_generation_ipc_schema(
        footer
            .schema()
            .ok_or_else(|| invalid("V35 routing IPC footer schema is missing"))?,
        expected_schema,
    )?;
    if footer
        .dictionaries()
        .is_some_and(|values| !values.is_empty())
    {
        return Err(invalid("V35 routing IPC dictionaries are forbidden"));
    }
    let blocks = footer
        .recordBatches()
        .ok_or_else(|| invalid("V35 routing IPC batch is missing"))?;
    if blocks.len() != 1 {
        return Err(invalid("V35 routing IPC batch count differs"));
    }
    let block = blocks.get(0);
    let block_offset = usize::try_from(block.offset())
        .map_err(|_| invalid("V35 routing IPC batch offset differs"))?;
    let metadata_len = usize::try_from(block.metaDataLength())
        .map_err(|_| invalid("V35 routing IPC batch metadata differs"))?;
    let body_len = usize::try_from(block.bodyLength())
        .map_err(|_| invalid("V35 routing IPC batch body differs"))?;
    let body_start = block_offset
        .checked_add(metadata_len)
        .ok_or_else(|| invalid("V35 routing IPC batch extent overflows"))?;
    let body_end = body_start
        .checked_add(body_len)
        .ok_or_else(|| invalid("V35 routing IPC batch extent overflows"))?;
    if block_offset < 8 || metadata_len < 8 || body_end > footer_start {
        return Err(invalid("V35 routing IPC batch extent differs"));
    }
    let parse_message = |start: usize, end: usize| {
        let metadata = bytes
            .get(start..end)
            .ok_or_else(|| invalid("V35 routing IPC message extent differs"))?;
        if metadata.len() < 4 {
            return Err(invalid("V35 routing IPC message is truncated"));
        }
        let prefix = if metadata.starts_with(&[255; 4]) {
            8
        } else {
            4
        };
        if metadata.len() < prefix {
            return Err(invalid("V35 routing IPC message prefix is truncated"));
        }
        let message_len = u32::from_le_bytes(
            metadata[prefix - 4..prefix]
                .try_into()
                .map_err(|_| invalid("V35 routing IPC message length differs"))?,
        ) as usize;
        let message_end = prefix
            .checked_add(message_len)
            .filter(|value| *value <= metadata.len())
            .ok_or_else(|| invalid("V35 routing IPC message extent differs"))?;
        arrow_ipc::root_as_message(&metadata[prefix..message_end])
            .map_err(|_| invalid("V35 routing IPC message differs"))
    };
    let leading = parse_message(8, block_offset)?;
    if leading.bodyLength() != 0 {
        return Err(invalid("V35 routing IPC leading schema body differs"));
    }
    validate_generation_ipc_schema(
        leading
            .header_as_schema()
            .ok_or_else(|| invalid("V35 routing IPC leading schema is missing"))?,
        expected_schema,
    )?;
    let record_message = parse_message(block_offset, body_start)?;
    let record = record_message
        .header_as_record_batch()
        .ok_or_else(|| invalid("V35 routing IPC record differs"))?;
    if usize::try_from(record.length()).ok() != Some(rows)
        || record.compression().is_some()
        || usize::try_from(record_message.bodyLength()).ok() != Some(body_len)
    {
        return Err(invalid("V35 routing IPC record authority differs"));
    }
    let expected_nodes = [
        rows,
        rows,
        rows,
        rows,
        rows,
        rows,
        rows,
        rows * routing,
        rows,
        rows,
        rows * routing,
        rows,
        rows,
        rows * COMPONENTS * routing,
        rows,
        rows * COMPONENTS,
        rows,
        rows * COMPONENTS,
        rows,
        rows,
        rows,
        rows,
        rows,
    ];
    let nodes = record
        .nodes()
        .ok_or_else(|| invalid("V35 routing IPC nodes are missing"))?;
    if nodes.len() != expected_nodes.len()
        || nodes.iter().zip(expected_nodes).any(|(node, expected)| {
            usize::try_from(node.length()).ok() != Some(expected) || node.null_count() != 0
        })
    {
        return Err(invalid("V35 routing IPC node shape differs"));
    }
    let buffers = record
        .buffers()
        .ok_or_else(|| invalid("V35 routing IPC buffers are missing"))?;
    if buffers.len() != 41 {
        return Err(invalid("V35 routing IPC buffer count differs"));
    }
    let body = &bytes[body_start..body_end];
    let mut slices = Vec::with_capacity(buffers.len());
    let mut previous_end = 0_usize;
    for buffer in buffers {
        let start = usize::try_from(buffer.offset())
            .map_err(|_| invalid("V35 routing IPC buffer offset differs"))?;
        let length = usize::try_from(buffer.length())
            .map_err(|_| invalid("V35 routing IPC buffer length differs"))?;
        let end = start
            .checked_add(length)
            .ok_or_else(|| invalid("V35 routing IPC buffer extent overflows"))?;
        if start < previous_end || start % 8 != 0 {
            return Err(invalid("V35 routing IPC buffers overlap"));
        }
        slices.push(
            body.get(start..end)
                .ok_or_else(|| invalid("V35 routing IPC buffer extent differs"))?,
        );
        previous_end = end;
    }
    let value_lengths = [
        (1, rows * 4),
        (3, rows * 4),
        (5, rows * 8),
        (7, rows * 4),
        (9, rows * 8),
        (11, rows * 8),
        (14, rows * routing),
        (16, rows * 4),
        (19, rows * routing),
        (21, rows * 4),
        (24, rows * COMPONENTS * routing),
        (27, rows * COMPONENTS * 4),
        (30, rows * COMPONENTS * 4),
        (32, rows * 8),
        (34, rows * 8),
        (36, rows * 8),
        (38, rows * 8),
        (40, rows * 4),
    ];
    if value_lengths
        .iter()
        .any(|(index, length)| slices[*index].len() != *length)
    {
        return Err(invalid("V35 routing IPC value length differs"));
    }
    for (index, count) in [
        (0, rows),
        (2, rows),
        (4, rows),
        (6, rows),
        (8, rows),
        (10, rows),
        (12, rows),
        (13, rows * routing),
        (15, rows),
        (17, rows),
        (18, rows * routing),
        (20, rows),
        (22, rows),
        (23, rows * COMPONENTS * routing),
        (25, rows),
        (26, rows * COMPONENTS),
        (28, rows),
        (29, rows * COMPONENTS),
        (31, rows),
        (33, rows),
        (35, rows),
        (37, rows),
        (39, rows),
    ] {
        if !slices[index].is_empty()
            && (slices[index].len() != count.div_ceil(8)
                || (0..count).any(|bit| slices[index][bit / 8] & (1 << (bit % 8)) == 0))
        {
            return Err(invalid("V35 routing IPC null bitmap differs"));
        }
    }
    Ok(())
}

fn read_generation_manifest_text(bytes: &[u8]) -> Result<String> {
    if bytes.len() < 18 || !bytes.starts_with(b"ARROW1\0\0") || !bytes.ends_with(b"ARROW1") {
        return Err(invalid("V35 routing IPC magic differs"));
    }
    let trailer = bytes.len() - 10;
    let footer_len = u32::from_le_bytes(
        bytes[trailer..trailer + 4]
            .try_into()
            .map_err(|_| invalid("V35 routing IPC footer length differs"))?,
    ) as usize;
    let footer_start = trailer
        .checked_sub(footer_len)
        .filter(|offset| *offset >= 8)
        .ok_or_else(|| invalid("V35 routing IPC footer extent differs"))?;
    let footer = arrow_ipc::root_as_footer(&bytes[footer_start..trailer])
        .map_err(|_| invalid("V35 routing IPC footer differs"))?;
    let schema = footer
        .schema()
        .ok_or_else(|| invalid("V35 routing IPC footer schema is missing"))?;
    let metadata = schema
        .custom_metadata()
        .ok_or_else(|| invalid("V35 routing IPC manifest is missing"))?;
    if metadata.len() != 1 || metadata.get(0).key() != Some(GENERATION_MANIFEST_KEY) {
        return Err(invalid("V35 routing IPC manifest differs"));
    }
    metadata
        .get(0)
        .value()
        .map(ToOwned::to_owned)
        .ok_or_else(|| invalid("V35 routing IPC manifest differs"))
}

/// Authenticate and decode one compact Arrow routing generation.
pub fn decode_v35_generation_arrow(
    bytes: &[u8],
    identity: &V35ArtifactIdentity,
    projection: &V35Projection,
    source_id: &str,
    source_archive_sha256: &str,
    limits: V35RoutingGenerationLimits,
) -> Result<V35RoutingGeneration> {
    if identity.digest_algorithm != "sha256"
        || !valid_sha256(&identity.digest)
        || !matches!(
            identity.role.as_str(),
            "active-routing" | "retiring-routing"
        )
        || identity.uri.is_empty()
        || identity.length != u64::try_from(bytes.len()).unwrap_or(u64::MAX)
        || identity.digest != format!("{:x}", Sha256::digest(bytes))
        || identity.length > limits.maximum_encoded_bytes
    {
        return Err(invalid("V35 routing generation artifact bytes differ"));
    }
    let manifest_text = read_generation_manifest_text(bytes)?;
    let manifest: V35RoutingGenerationManifest = serde_json::from_str(&manifest_text)
        .map_err(|_| invalid("V35 routing generation manifest differs"))?;
    if canonical_manifest(&manifest)? != manifest_text
        || manifest.format != GENERATION_FORMAT
        || manifest.storage_order != "logical-leaf-patch-major"
        || manifest.role != identity.role
        || manifest.uri != identity.uri
        || manifest.dimensions != projection.dimensions()
        || manifest.projection_arm != projection.arm()
        || manifest.projection_checksum_sha256 != checksum_hex(projection.checksum())
        || manifest.projection_algorithm != projection.algorithm()
        || manifest.projection_sample_sha256.as_deref() != projection.sample_sha256()
        || manifest.projection_training_descriptor != projection.training_descriptor()
        || manifest.source_id != source_id
        || manifest.source_archive_sha256 != source_archive_sha256
        || !valid_sha256(&manifest.source_archive_sha256)
        || manifest.leaf_count == 0
        || manifest.leaf_count > limits.maximum_leaf_count
        || !matches!(manifest.patches_per_leaf, 1 | 2)
    {
        return Err(invalid("V35 routing generation manifest authority differs"));
    }
    let bytes_per_patch = usize::from(manifest.dimensions.routing)
        .checked_mul(8)
        .and_then(|value| value.checked_add(128))
        .ok_or_else(|| invalid("V35 routing generation numeric bytes overflow"))?;
    let numeric_bytes = usize::try_from(manifest.leaf_count)
        .ok()
        .and_then(|leaves| leaves.checked_mul(usize::from(manifest.patches_per_leaf)))
        .and_then(|patches| patches.checked_mul(bytes_per_patch))
        .ok_or_else(|| invalid("V35 routing generation numeric bytes overflow"))?;
    let peak_codec_bytes = identity
        .length
        .checked_mul(2)
        .ok_or_else(|| invalid("V35 routing generation codec peak overflows"))?;
    let expected_schema = generation_schema(&manifest)?;
    let expected_rows = usize::try_from(manifest.leaf_count)
        .ok()
        .and_then(|leaves| leaves.checked_mul(usize::from(manifest.patches_per_leaf)))
        .ok_or_else(|| invalid("V35 routing generation row count overflows"))?;
    if numeric_bytes > limits.maximum_numeric_bytes
        || peak_codec_bytes > limits.maximum_peak_codec_bytes
    {
        return Err(invalid("V35 routing generation allocation differs"));
    }
    validate_generation_ipc_envelope(
        bytes,
        expected_schema.as_ref(),
        expected_rows,
        usize::from(manifest.dimensions.routing),
    )?;
    let mut reader = FileReader::try_new(Cursor::new(bytes), None)?;
    if reader.num_batches() != 1 || reader.schema().as_ref() != expected_schema.as_ref() {
        return Err(invalid("V35 routing generation Arrow schema differs"));
    }
    let batch = reader
        .next()
        .transpose()?
        .ok_or_else(|| invalid("V35 routing generation batch is missing"))?;
    if reader.next().is_some()
        || batch.num_rows() != expected_rows
        || batch
            .columns()
            .iter()
            .any(|column| column.null_count() != 0)
    {
        return Err(invalid("V35 routing generation batch differs"));
    }
    let u32_column = |index: usize| {
        batch
            .column(index)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or_else(|| invalid("V35 routing generation integer differs"))
    };
    let u64_column = |index: usize| {
        batch
            .column(index)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| invalid("V35 routing generation integer differs"))
    };
    let f32_column = |index: usize| {
        batch
            .column(index)
            .as_any()
            .downcast_ref::<Float32Array>()
            .ok_or_else(|| invalid("V35 routing generation scalar differs"))
    };
    let f64_column = |index: usize| {
        batch
            .column(index)
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| invalid("V35 routing generation scalar differs"))
    };
    let list_column = |index: usize| {
        batch
            .column(index)
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .ok_or_else(|| invalid("V35 routing generation list differs"))
    };
    let leaf_ordinals = u32_column(0)?;
    let group_ordinals = u32_column(1)?;
    let logical_starts = u64_column(2)?;
    let populations = u32_column(3)?;
    let assignment_mins = u64_column(4)?;
    let assignment_maxes = u64_column(5)?;
    let mean_codes = list_column(6)?;
    let mean_scales = f32_column(7)?;
    let residual_codes = list_column(8)?;
    let residual_scales = f32_column(9)?;
    let direction_codes = list_column(10)?;
    let direction_scales = list_column(11)?;
    let weights = list_column(12)?;
    let traces = f64_column(13)?;
    let trace_squares = f64_column(14)?;
    let population_factors = f64_column(15)?;
    let spectral_bounds = f64_column(16)?;
    let omitted_energies = f32_column(17)?;
    let routing = usize::from(manifest.dimensions.routing);
    let mut previous_group = None::<u32>;
    let mut expected_logical_start = 0_u64;
    for row in 0..batch.num_rows() {
        let patches_per_leaf = usize::from(manifest.patches_per_leaf);
        let expected_leaf = row / patches_per_leaf;
        let patch_ordinal = row % patches_per_leaf;
        if leaf_ordinals.value(row) != u32::try_from(expected_leaf).unwrap_or(u32::MAX) {
            return Err(invalid("V35 routing generation row order differs"));
        }
        let first_row = row - patch_ordinal;
        if patch_ordinal > 0
            && (group_ordinals.value(row) != group_ordinals.value(first_row)
                || logical_starts.value(row) != logical_starts.value(first_row)
                || assignment_mins.value(row) != assignment_mins.value(first_row)
                || assignment_maxes.value(row) != assignment_maxes.value(first_row))
        {
            return Err(invalid("V35 routing generation patch binding differs"));
        }
        if patch_ordinal == 0
            && (previous_group.is_some_and(|group| group_ordinals.value(row) < group)
                || logical_starts.value(row) != expected_logical_start)
        {
            return Err(invalid("V35 routing generation logical order differs"));
        }
        if patch_ordinal + 1 == patches_per_leaf {
            let population = (first_row..=row).try_fold(0_u32, |sum, index| {
                sum.checked_add(populations.value(index))
            });
            let population = population
                .filter(|value| *value > 0 && *value as usize <= MAX_LEAF_ROWS)
                .ok_or_else(|| invalid("V35 routing generation leaf population differs"))?;
            expected_logical_start = expected_logical_start
                .checked_add(u64::from(population))
                .ok_or_else(|| invalid("V35 routing generation logical interval overflows"))?;
            previous_group = Some(group_ordinals.value(row));
            if logical_starts
                .value(row)
                .checked_add(u64::from(population))
                .is_none()
            {
                return Err(invalid("V35 routing generation leaf population differs"));
            }
        }
        let patch = V35LeafPatch {
            assignment_max: assignment_maxes.value(row),
            assignment_min: assignment_mins.value(row),
            dimensions: manifest.dimensions,
            group_ordinal: group_ordinals.value(row),
            leaf_ordinal: leaf_ordinals.value(row),
            logical_start: logical_starts.value(row),
            population: populations.value(row),
            mean_codes: list_i8(mean_codes, row, routing)?,
            mean_scale: mean_scales.value(row),
            residual_codes: list_u8(residual_codes, row, routing)?,
            residual_scale: residual_scales.value(row),
            direction_codes: list_i8(direction_codes, row, COMPONENTS * routing)?,
            direction_scales: list_f32(direction_scales, row)?,
            weights: list_f32(weights, row)?,
            trace: traces.value(row),
            trace_square: trace_squares.value(row),
            population_factor: population_factors.value(row),
            spectral_bound: spectral_bounds.value(row),
            omitted_energy: omitted_energies.value(row),
        };
        validate_patch_payload(&patch)?;
    }
    let generation = V35RoutingGeneration {
        dimensions: manifest.dimensions,
        projection_arm: manifest.projection_arm,
        projection_checksum: projection.checksum(),
        projection_algorithm: manifest.projection_algorithm,
        projection_sample_sha256: manifest.projection_sample_sha256,
        projection_training_descriptor: manifest.projection_training_descriptor,
        source_id: manifest.source_id,
        source_archive_sha256: manifest.source_archive_sha256,
        patches_per_leaf: manifest.patches_per_leaf,
        leaf_count: usize::try_from(manifest.leaf_count)
            .map_err(|_| invalid("V35 routing generation leaf count overflows"))?,
        resident_numeric_bytes: numeric_bytes,
        storage: V35RoutingGenerationStorage::Serving(batch),
    };
    validate_generation(&generation)?;
    Ok(generation)
}
