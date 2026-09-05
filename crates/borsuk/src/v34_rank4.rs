use std::collections::BTreeSet;

use crate::{
    BorsukError, Result, V33GroupShapeBuildRequest, v33_group_shape::build_v33_rank4_leaf_snapshots,
};

const V34_DIMENSIONS: usize = 96;
const V34_COMPONENTS: usize = 4;
const MIB: u64 = 1_048_576;

fn invalid(message: &str) -> BorsukError {
    BorsukError::InvalidStorage(message.to_owned())
}

#[derive(Debug, Clone, PartialEq)]
/// Query-independent raw fields used to build one authenticated rank-four leaf.
pub struct V34Rank4LeafInput {
    /// Dense deterministic leaf ordinal.
    pub leaf_ordinal: u32,
    /// Dense storage-group ordinal.
    pub group_ordinal: u32,
    /// First logical row in this leaf.
    pub logical_start: u64,
    /// Number of logical rows in this leaf.
    pub population: u32,
    /// Reconstructed-row mean.
    pub mean: [f32; V34_DIMENSIONS],
    /// Nonnegative covariance diagonal left after the rank-four factors.
    pub residual_diagonal: [f32; V34_DIMENSIONS],
    /// Descending nonnegative factor eigenvalues.
    pub eigenvalues: [f32; V34_COMPONENTS],
    /// Persisted factor directions; decoded vectors need not remain orthogonal.
    pub directions: [[f32; V34_DIMENSIONS]; V34_COMPONENTS],
}

#[derive(Debug, Clone, PartialEq)]
/// One immutable decoded rank-four routing leaf.
pub struct V34Rank4Leaf {
    /// Dense deterministic leaf ordinal.
    pub(crate) leaf_ordinal: u32,
    /// Dense storage-group ordinal.
    pub(crate) group_ordinal: u32,
    /// First logical row in this leaf.
    pub(crate) logical_start: u64,
    /// Number of logical rows in this leaf.
    pub(crate) population: u32,
    /// Reconstructed-row mean.
    pub(crate) mean: [f32; V34_DIMENSIONS],
    /// Nonnegative covariance diagonal left after the rank-four factors.
    pub(crate) residual_diagonal: [f32; V34_DIMENSIONS],
    /// Descending nonnegative factor eigenvalues.
    pub(crate) eigenvalues: [f32; V34_COMPONENTS],
    /// Persisted factor directions; decoded vectors need not remain orthogonal.
    pub(crate) directions: [[f32; V34_DIMENSIONS]; V34_COMPONENTS],
    /// `sqrt(2*ln(population))` cached for scoring.
    pub(crate) population_factor: f64,
    /// Trace of the decoded covariance approximation.
    pub(crate) trace: f64,
    /// Trace of the square of the decoded covariance approximation.
    pub(crate) trace_square: f64,
    /// Conservative decoded covariance spectral-norm bound.
    pub(crate) spectral_bound: f64,
}

impl V34Rank4Leaf {
    /// Dense deterministic leaf ordinal.
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

    /// Number of logical rows in this leaf.
    pub fn population(&self) -> u32 {
        self.population
    }

    /// Reconstructed-row mean.
    pub fn mean(&self) -> &[f32; V34_DIMENSIONS] {
        &self.mean
    }

    /// Nonnegative residual diagonal.
    pub fn residual_diagonal(&self) -> &[f32; V34_DIMENSIONS] {
        &self.residual_diagonal
    }

    /// Descending nonnegative eigenvalues.
    pub fn eigenvalues(&self) -> &[f32; V34_COMPONENTS] {
        &self.eigenvalues
    }

    /// Four persisted covariance directions.
    pub fn directions(&self) -> &[[f32; V34_DIMENSIONS]; V34_COMPONENTS] {
        &self.directions
    }

    /// Deterministic population factor.
    pub fn population_factor(&self) -> f64 {
        self.population_factor
    }

    /// Decoded covariance trace.
    pub fn trace(&self) -> f64 {
        self.trace
    }

    /// Trace of the square of the decoded covariance.
    pub fn trace_square(&self) -> f64 {
        self.trace_square
    }

    /// Outward-rounded spectral bound.
    pub fn spectral_bound(&self) -> f64 {
        self.spectral_bound
    }
}

#[derive(Debug, Clone, PartialEq)]
/// Authenticated in-memory rank-four generation.
pub struct V34Rank4Generation {
    leaves: Vec<V34Rank4Leaf>,
    logical_rows: u64,
    group_count: u32,
}

impl V34Rank4Generation {
    /// Leaves in dense logical order.
    pub fn leaves(&self) -> &[V34Rank4Leaf] {
        &self.leaves
    }

    /// Total logical rows covered exactly once.
    pub fn logical_rows(&self) -> u64 {
        self.logical_rows
    }

    /// Number of dense storage groups.
    pub fn group_count(&self) -> u32 {
        self.group_count
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Checked 100M serving-memory and directional-work projection.
pub struct V34ServingMemoryProjection {
    /// Rank-four numeric bytes at 2,320 bytes per leaf.
    pub rank_four_numeric_bytes: u64,
    /// Logical interval and group identity bytes at 24 bytes per leaf.
    pub leaf_identity_bytes: u64,
    /// Four cached f64 scalar bytes per leaf.
    pub cached_scalar_bytes: u64,
    /// Provisional tree bytes at 512 bytes per node.
    pub tree_bytes: u64,
    /// Maximum bytes admitted for the active generation.
    pub active_generation_cap_bytes: u64,
    /// Maximum bytes admitted for the retiring generation.
    pub retiring_generation_cap_bytes: u64,
    /// Shared page and routing-cache admission.
    pub shared_cache_cap_bytes: u64,
    /// Runtime, allocator, and thread-stack admission.
    pub runtime_cap_bytes: u64,
    /// Concurrent query-workspace admission.
    pub query_workspace_cap_bytes: u64,
    /// Deliberately unallocated process headroom.
    pub unallocated_headroom_bytes: u64,
    /// Sum of every preregistered process admission bucket.
    pub admission_budget_bytes: u64,
    /// Strict process hard limit.
    pub hard_limit_bytes: u64,
    /// Rank-four directional multiply-accumulate count for one exhaustive query.
    pub exhaustive_directional_macs: u64,
}

#[derive(Debug, Clone, Copy)]
struct V34DecodedMoments {
    population_factor: f64,
    trace: f64,
    trace_square: f64,
    spectral_bound: f64,
}

fn recompute_v34_moments(leaf: &V34Rank4Leaf) -> Result<V34DecodedMoments> {
    if leaf.population == 0
        || leaf.mean.iter().any(|value| !value.is_finite())
        || leaf
            .residual_diagonal
            .iter()
            .any(|value| !value.is_finite() || *value < 0.0)
        || leaf
            .eigenvalues
            .iter()
            .any(|value| !value.is_finite() || *value < 0.0)
        || leaf.eigenvalues.windows(2).any(|pair| pair[0] < pair[1])
        || leaf
            .directions
            .iter()
            .flatten()
            .any(|value| !value.is_finite())
    {
        return Err(invalid("V34 rank-four leaf authority differs"));
    }

    for component in 0..V34_COMPONENTS {
        let direction = &leaf.directions[component];
        if leaf.eigenvalues[component] == 0.0 {
            if direction.iter().any(|value| *value != 0.0) {
                return Err(invalid("V34 zero component authority differs"));
            }
            continue;
        }
        let pivot = direction
            .iter()
            .enumerate()
            .max_by(|left, right| {
                left.1
                    .abs()
                    .total_cmp(&right.1.abs())
                    .then_with(|| right.0.cmp(&left.0))
            })
            .map(|(_, value)| *value)
            .ok_or_else(|| invalid("V34 rank-four direction authority differs"))?;
        if pivot <= 0.0 {
            return Err(invalid("V34 rank-four direction sign differs"));
        }
    }

    let population_factor = (2.0 * deterministic_ln_u32(leaf.population)).sqrt();
    let mut trace = 0.0_f64;
    let mut trace_square = 0.0_f64;
    let mut maximum_diagonal = 0.0_f64;
    for dimension in 0..V34_DIMENSIONS {
        let diagonal = f64::from(leaf.residual_diagonal[dimension]);
        trace += diagonal;
        trace_square += diagonal * diagonal;
        maximum_diagonal = maximum_diagonal.max(diagonal);
    }

    let mut direction_norms = [0.0_f64; V34_COMPONENTS];
    for (component, direction_norm) in direction_norms.iter_mut().enumerate() {
        for dimension in 0..V34_DIMENSIONS {
            let value = f64::from(leaf.directions[component][dimension]);
            *direction_norm += value * value;
        }
        let eigenvalue = f64::from(leaf.eigenvalues[component]);
        trace += eigenvalue * *direction_norm;
        for dimension in 0..V34_DIMENSIONS {
            let value = f64::from(leaf.directions[component][dimension]);
            trace_square +=
                2.0 * eigenvalue * f64::from(leaf.residual_diagonal[dimension]) * value * value;
        }
    }
    for left in 0..V34_COMPONENTS {
        for right in 0..V34_COMPONENTS {
            let mut dot = 0.0_f64;
            for dimension in 0..V34_DIMENSIONS {
                dot += f64::from(leaf.directions[left][dimension])
                    * f64::from(leaf.directions[right][dimension]);
            }
            trace_square +=
                f64::from(leaf.eigenvalues[left]) * f64::from(leaf.eigenvalues[right]) * dot * dot;
        }
    }
    let mut spectral_bound = maximum_diagonal;
    for component in 0..V34_COMPONENTS {
        let mut norm_upper = 0.0_f64;
        for dimension in 0..V34_DIMENSIONS {
            let value = f64::from(leaf.directions[component][dimension]).abs();
            norm_upper = add_nonnegative_up(norm_upper, multiply_nonnegative_up(value, value));
        }
        spectral_bound = add_nonnegative_up(
            spectral_bound,
            multiply_nonnegative_up(f64::from(leaf.eigenvalues[component]), norm_upper),
        );
    }
    if !population_factor.is_finite()
        || !trace.is_finite()
        || !trace_square.is_finite()
        || !spectral_bound.is_finite()
    {
        return Err(invalid("V34 rank-four moments overflow"));
    }
    Ok(V34DecodedMoments {
        population_factor,
        trace,
        trace_square,
        spectral_bound,
    })
}

fn validate_v34_leaf_payload(leaf: &V34Rank4Leaf) -> Result<()> {
    let moments = recompute_v34_moments(leaf)?;
    if leaf.population_factor != moments.population_factor
        || leaf.trace != moments.trace
        || leaf.trace_square != moments.trace_square
        || leaf.spectral_bound != moments.spectral_bound
    {
        return Err(invalid("V34 rank-four cached moments differ"));
    }
    Ok(())
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

fn canonicalize_v34_leaf_zeroes(leaf: &mut V34Rank4LeafInput) {
    for value in leaf
        .mean
        .iter_mut()
        .chain(leaf.residual_diagonal.iter_mut())
        .chain(leaf.eigenvalues.iter_mut())
        .chain(leaf.directions.iter_mut().flatten())
    {
        if *value == 0.0 {
            *value = 0.0;
        }
    }
    for component in 0..V34_COMPONENTS {
        if leaf.eigenvalues[component] == 0.0 {
            leaf.directions[component].fill(0.0);
        }
    }
}

/// Validate compact leaves and bind their dense logical/group coverage.
pub fn build_v34_rank4_generation(
    mut inputs: Vec<V34Rank4LeafInput>,
) -> Result<V34Rank4Generation> {
    if inputs.is_empty() {
        return Err(invalid("V34 rank-four generation is empty"));
    }
    let mut leaves = Vec::with_capacity(inputs.len());
    let mut logical_rows = 0_u64;
    let mut groups = BTreeSet::new();
    for (ordinal, input) in inputs.iter_mut().enumerate() {
        canonicalize_v34_leaf_zeroes(input);
        if input.leaf_ordinal != ordinal as u32 || input.logical_start != logical_rows {
            return Err(invalid("V34 rank-four leaf ordering differs"));
        }
        let mut leaf = V34Rank4Leaf {
            leaf_ordinal: input.leaf_ordinal,
            group_ordinal: input.group_ordinal,
            logical_start: input.logical_start,
            population: input.population,
            mean: input.mean,
            residual_diagonal: input.residual_diagonal,
            eigenvalues: input.eigenvalues,
            directions: input.directions,
            population_factor: 0.0,
            trace: 0.0,
            trace_square: 0.0,
            spectral_bound: 0.0,
        };
        let moments = recompute_v34_moments(&leaf)?;
        leaf.population_factor = moments.population_factor;
        leaf.trace = moments.trace;
        leaf.trace_square = moments.trace_square;
        leaf.spectral_bound = moments.spectral_bound;
        validate_v34_leaf_payload(&leaf)?;
        logical_rows = logical_rows
            .checked_add(u64::from(leaf.population))
            .ok_or_else(|| invalid("V34 rank-four logical coverage overflows"))?;
        groups.insert(leaf.group_ordinal);
        leaves.push(leaf);
    }
    let group_count = groups
        .last()
        .copied()
        .and_then(|ordinal| ordinal.checked_add(1))
        .ok_or_else(|| invalid("V34 rank-four group coverage differs"))?;
    if groups.len() != group_count as usize || groups.iter().copied().ne(0..group_count) {
        return Err(invalid("V34 rank-four group coverage differs"));
    }
    Ok(V34Rank4Generation {
        leaves,
        logical_rows,
        group_count,
    })
}

/// Derive rank-four leaves from the authenticated V33 reconstruction boundary.
pub fn build_v34_rank4_generation_from_v33(
    request: &V33GroupShapeBuildRequest,
) -> Result<V34Rank4Generation> {
    let inputs = build_v33_rank4_leaf_snapshots(request)?
        .into_iter()
        .map(|leaf| {
            Ok(V34Rank4LeafInput {
                leaf_ordinal: leaf.ordinal,
                group_ordinal: leaf.group_ordinal,
                logical_start: leaf.logical_start,
                population: u32::try_from(leaf.population)
                    .map_err(|_| invalid("V34 rank-four population overflows"))?,
                mean: leaf.mean,
                residual_diagonal: leaf.residual,
                eigenvalues: leaf.eigenvalues,
                directions: leaf.directions,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    build_v34_rank4_generation(inputs)
}

/// Evaluate the frozen lower-tail heuristic over one compact leaf.
pub fn score_v34_rank4_leaf(leaf: &V34Rank4Leaf, query: &[f32; V34_DIMENSIONS]) -> Result<f64> {
    if query.iter().any(|value| !value.is_finite()) {
        return Err(invalid("V34 rank-four query differs"));
    }
    let mut delta = [0.0_f64; V34_DIMENSIONS];
    let mut distance = 0.0_f64;
    let mut covariance_projection = 0.0_f64;
    for dimension in 0..V34_DIMENSIONS {
        delta[dimension] = f64::from(query[dimension]) - f64::from(leaf.mean[dimension]);
        distance += delta[dimension] * delta[dimension];
        covariance_projection +=
            f64::from(leaf.residual_diagonal[dimension]) * delta[dimension] * delta[dimension];
    }
    for component in 0..V34_COMPONENTS {
        let mut projection = 0.0_f64;
        for (dimension, delta_value) in delta.iter().enumerate() {
            projection += f64::from(leaf.directions[component][dimension]) * delta_value;
        }
        covariance_projection += f64::from(leaf.eigenvalues[component]) * projection * projection;
    }
    let radicand = 2.0 * leaf.trace_square + 4.0 * covariance_projection;
    if !radicand.is_finite() || radicand < 0.0 {
        return Err(invalid("V34 rank-four score radicand differs"));
    }
    let score = distance + leaf.trace - leaf.population_factor * radicand.sqrt();
    if !score.is_finite() {
        return Err(invalid("V34 rank-four score differs"));
    }
    Ok(score)
}

/// Project fixed 100M serving memory and exhaustive directional work.
pub fn project_v34_serving_memory(
    leaf_count: u64,
    tree_node_count: u64,
) -> Result<V34ServingMemoryProjection> {
    if leaf_count == 0 || tree_node_count == 0 {
        return Err(invalid("V34 serving projection authority differs"));
    }
    let checked = |count: u64, width: u64| {
        count
            .checked_mul(width)
            .ok_or_else(|| invalid("V34 serving projection overflows"))
    };
    let rank_four_numeric_bytes = checked(leaf_count, 2_320)?;
    let leaf_identity_bytes = checked(leaf_count, 24)?;
    let cached_scalar_bytes = checked(leaf_count, 32)?;
    let tree_bytes = checked(tree_node_count, 512)?;
    let active_bytes = rank_four_numeric_bytes
        .checked_add(leaf_identity_bytes)
        .and_then(|value| value.checked_add(cached_scalar_bytes))
        .and_then(|value| value.checked_add(tree_bytes))
        .ok_or_else(|| invalid("V34 active generation projection overflows"))?;
    let active_generation_cap_bytes = 1_040 * MIB;
    if active_bytes > active_generation_cap_bytes {
        return Err(invalid("V34 active generation memory cap exceeded"));
    }
    let retiring_generation_cap_bytes = 1_040 * MIB;
    let shared_cache_cap_bytes = 128 * MIB;
    let runtime_cap_bytes = 160 * MIB;
    let query_workspace_cap_bytes = 512 * MIB;
    let unallocated_headroom_bytes = 96 * MIB;
    let admission_budget_bytes = active_generation_cap_bytes
        .checked_add(retiring_generation_cap_bytes)
        .and_then(|value| value.checked_add(shared_cache_cap_bytes))
        .and_then(|value| value.checked_add(runtime_cap_bytes))
        .and_then(|value| value.checked_add(query_workspace_cap_bytes))
        .and_then(|value| value.checked_add(unallocated_headroom_bytes))
        .ok_or_else(|| invalid("V34 process memory admission overflows"))?;
    let hard_limit_bytes = 3_072 * MIB;
    if admission_budget_bytes >= hard_limit_bytes {
        return Err(invalid("V34 process memory admission differs"));
    }
    let exhaustive_directional_macs = leaf_count
        .checked_mul(V34_COMPONENTS as u64)
        .and_then(|value| value.checked_mul(V34_DIMENSIONS as u64))
        .ok_or_else(|| invalid("V34 exhaustive work projection overflows"))?;
    Ok(V34ServingMemoryProjection {
        rank_four_numeric_bytes,
        leaf_identity_bytes,
        cached_scalar_bytes,
        tree_bytes,
        active_generation_cap_bytes,
        retiring_generation_cap_bytes,
        shared_cache_cap_bytes,
        runtime_cap_bytes,
        query_workspace_cap_bytes,
        unallocated_headroom_bytes,
        admission_budget_bytes,
        hard_limit_bytes,
        exhaustive_directional_macs,
    })
}
