use nalgebra::{DMatrix, SymmetricEigen};

use crate::{BorsukError, Result, V35Dimensions};

const COMPONENTS: usize = 6;
const MAX_LEAF_ROWS: usize = 256;

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

/// Seal one at-most-256-row projected leaf into a compact quantized patch.
pub fn build_v35_leaf_patch(request: &V35LeafPatchBuildRequest) -> Result<V35LeafPatch> {
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
    let routing = usize::from(request.dimensions.routing);
    if request.projected_rows.is_empty() || parts == 0 || parts > request.projected_rows.len() {
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
                .then_with(|| {
                    request.projected_rows[*left]
                        .iter()
                        .zip(&request.projected_rows[*right])
                        .find_map(|(a, b)| {
                            let ordering = a.total_cmp(b);
                            (!ordering.is_eq()).then_some(ordering)
                        })
                        .unwrap_or_else(|| left.cmp(right))
                })
        });
        let right = ordered.split_off(ordered.len() / 2);
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
