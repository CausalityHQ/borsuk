use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    io::Cursor,
    sync::Arc,
};

use arrow_array::{
    Array, FixedSizeListArray, Float32Array, Float64Array, RecordBatch, UInt32Array, UInt64Array,
};
use arrow_ipc::{
    MetadataVersion,
    reader::FileReader,
    writer::{FileWriter, IpcWriteOptions},
};
use arrow_schema::{DataType, Field, Schema};
use sha2::{Digest, Sha256};

use crate::{
    BorsukError, Result, V33GroupShapeBuildRequest, v33_group_shape::build_v33_rank4_leaf_snapshots,
};

const V34_DIMENSIONS: usize = 96;
const V34_COMPONENTS: usize = 4;
const MIB: u64 = 1_048_576;
const V34_METRIC: &str = "squared-l2";
const V34_NORMALIZATION: &str = "none";
const V34_SCORER_VERSION: &str = "v34-rank4-gaussian-lower-tail-v1";
const V34_MANIFEST_KEY: &str = "borsuk.v34.rank4.manifest";
const V34_MAX_LEAVES: usize = 414_100;
const V34_MAX_ARROW_BYTES: usize = 1_040 * 1_048_576;

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

#[derive(Debug, Clone, PartialEq, Eq)]
/// Complete immutable identity and upstream bindings for one V34 Arrow generation.
pub struct V34Rank4ArtifactIdentity {
    /// Immutable object URI.
    pub uri: String,
    /// SHA-256 of the complete Arrow IPC bytes.
    pub sha256: String,
    /// Complete Arrow IPC byte length.
    pub length: u64,
    /// Source-archive SHA-256.
    pub source_archive_sha256: String,
    /// Reconstruction-authority SHA-256.
    pub reconstruction_sha256: String,
    /// Codebook-authority SHA-256.
    pub codebooks_sha256: String,
    /// Exact final metric.
    pub metric: String,
    /// Exact vector dimensions.
    pub dimensions: u32,
    /// Exact normalization policy.
    pub normalization: String,
    /// Frozen scorer version.
    pub scorer_version: String,
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
        if input
            .mean
            .iter()
            .chain(input.residual_diagonal.iter())
            .chain(input.eigenvalues.iter())
            .chain(input.directions.iter().flatten())
            .any(|value| !value.is_finite())
        {
            return Err(invalid("V34 rank-four raw leaf is nonfinite"));
        }
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

fn valid_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn v34_rank4_manifest(identity: &V34Rank4ArtifactIdentity) -> Result<String> {
    let values = BTreeMap::from([
        (
            "codebooks_sha256",
            serde_json::Value::String(identity.codebooks_sha256.clone()),
        ),
        ("dimensions", serde_json::Value::from(identity.dimensions)),
        ("metric", serde_json::Value::String(identity.metric.clone())),
        (
            "normalization",
            serde_json::Value::String(identity.normalization.clone()),
        ),
        (
            "reconstruction_sha256",
            serde_json::Value::String(identity.reconstruction_sha256.clone()),
        ),
        (
            "scorer_version",
            serde_json::Value::String(identity.scorer_version.clone()),
        ),
        (
            "source_archive_sha256",
            serde_json::Value::String(identity.source_archive_sha256.clone()),
        ),
        ("uri", serde_json::Value::String(identity.uri.clone())),
    ]);
    serde_json::to_string(&values).map_err(|_| invalid("V34 rank-four artifact manifest differs"))
}

fn v34_vector_type() -> DataType {
    DataType::FixedSizeList(
        Arc::new(Field::new("element", DataType::Float32, false)),
        V34_DIMENSIONS as i32,
    )
}

fn v34_directions_type() -> DataType {
    DataType::FixedSizeList(
        Arc::new(Field::new("direction", v34_vector_type(), false)),
        V34_COMPONENTS as i32,
    )
}

fn v34_rank4_arrow_schema(identity: &V34Rank4ArtifactIdentity) -> Result<Schema> {
    let fields = vec![
        Field::new("leaf_ordinal", DataType::UInt32, false),
        Field::new("group_ordinal", DataType::UInt32, false),
        Field::new("logical_start", DataType::UInt64, false),
        Field::new("population", DataType::UInt32, false),
        Field::new("mean", v34_vector_type(), false),
        Field::new("residual_diagonal", v34_vector_type(), false),
        Field::new(
            "eigenvalues",
            DataType::FixedSizeList(
                Arc::new(Field::new("element", DataType::Float32, false)),
                V34_COMPONENTS as i32,
            ),
            false,
        ),
        Field::new("directions", v34_directions_type(), false),
        Field::new("population_factor", DataType::Float64, false),
        Field::new("trace", DataType::Float64, false),
        Field::new("trace_square", DataType::Float64, false),
        Field::new("spectral_bound", DataType::Float64, false),
    ];
    Ok(Schema::new_with_metadata(
        fields,
        HashMap::from([(V34_MANIFEST_KEY.to_owned(), v34_rank4_manifest(identity)?)]),
    ))
}

fn v34_vector_array<'a>(
    values: impl Iterator<Item = &'a [f32; V34_DIMENSIONS]>,
) -> Result<Arc<FixedSizeListArray>> {
    let flat = values
        .flat_map(|value| value.iter().copied())
        .collect::<Vec<_>>();
    Ok(Arc::new(FixedSizeListArray::try_new(
        Arc::new(Field::new("element", DataType::Float32, false)),
        V34_DIMENSIONS as i32,
        Arc::new(Float32Array::from(flat)),
        None,
    )?))
}

fn validate_v34_artifact_identity(identity: &V34Rank4ArtifactIdentity) -> Result<()> {
    if !identity.uri.starts_with("s3://")
        || !valid_sha256(&identity.sha256)
        || !valid_sha256(&identity.source_archive_sha256)
        || !valid_sha256(&identity.reconstruction_sha256)
        || !valid_sha256(&identity.codebooks_sha256)
        || identity.length == 0
        || identity.metric != V34_METRIC
        || identity.dimensions != V34_DIMENSIONS as u32
        || identity.normalization != V34_NORMALIZATION
        || identity.scorer_version != V34_SCORER_VERSION
    {
        return Err(invalid("V34 rank-four artifact identity differs"));
    }
    Ok(())
}

fn validate_v34_ipc_field(field: arrow_ipc::Field<'_>, expected: &Field) -> Result<()> {
    if field.name() != Some(expected.name().as_str())
        || field.nullable()
        || field.dictionary().is_some()
        || field
            .custom_metadata()
            .is_some_and(|values| !values.is_empty())
    {
        return Err(invalid("V34 rank-four IPC field differs"));
    }
    let children = match expected.data_type() {
        DataType::UInt32 | DataType::UInt64 => {
            let integer = field
                .type_as_int()
                .ok_or_else(|| invalid("V34 rank-four IPC integer differs"))?;
            let width = if expected.data_type() == &DataType::UInt32 {
                32
            } else {
                64
            };
            if integer.bitWidth() != width || integer.is_signed() {
                return Err(invalid("V34 rank-four IPC integer differs"));
            }
            Vec::new()
        }
        DataType::Float32 | DataType::Float64 => {
            let floating = field
                .type_as_floating_point()
                .ok_or_else(|| invalid("V34 rank-four IPC float differs"))?;
            let precision = if expected.data_type() == &DataType::Float32 {
                arrow_ipc::Precision::SINGLE
            } else {
                arrow_ipc::Precision::DOUBLE
            };
            if floating.precision() != precision {
                return Err(invalid("V34 rank-four IPC float differs"));
            }
            Vec::new()
        }
        DataType::FixedSizeList(child, width) => {
            if field
                .type_as_fixed_size_list()
                .is_none_or(|list| list.listSize() != *width)
            {
                return Err(invalid("V34 rank-four IPC fixed list differs"));
            }
            vec![child.as_ref()]
        }
        _ => return Err(invalid("V34 rank-four IPC type differs")),
    };
    let actual_children = field.children();
    if actual_children.map_or(0, |values| values.len()) != children.len() {
        return Err(invalid("V34 rank-four IPC children differ"));
    }
    for (index, child) in children.iter().enumerate() {
        validate_v34_ipc_field(
            actual_children
                .ok_or_else(|| invalid("V34 rank-four IPC child is missing"))?
                .get(index),
            child,
        )?;
    }
    Ok(())
}

fn validate_v34_ipc_schema(schema: arrow_ipc::Schema<'_>, expected: &Schema) -> Result<()> {
    if schema.endianness() != arrow_ipc::Endianness::Little
        || schema.features().is_some_and(|values| !values.is_empty())
    {
        return Err(invalid("V34 rank-four IPC schema features differ"));
    }
    let metadata = schema
        .custom_metadata()
        .ok_or_else(|| invalid("V34 rank-four IPC manifest is missing"))?;
    let expected_manifest = expected
        .metadata()
        .get(V34_MANIFEST_KEY)
        .ok_or_else(|| invalid("V34 rank-four IPC manifest differs"))?;
    if metadata.len() != 1
        || metadata.get(0).key() != Some(V34_MANIFEST_KEY)
        || metadata.get(0).value() != Some(expected_manifest.as_str())
    {
        return Err(invalid("V34 rank-four IPC manifest differs"));
    }
    let fields = schema
        .fields()
        .ok_or_else(|| invalid("V34 rank-four IPC fields are missing"))?;
    if fields.len() != expected.fields().len() {
        return Err(invalid("V34 rank-four IPC field count differs"));
    }
    for (index, expected_field) in expected.fields().iter().enumerate() {
        validate_v34_ipc_field(fields.get(index), expected_field)?;
    }
    Ok(())
}

fn validate_v34_ipc_envelope(bytes: &[u8], expected_schema: &Schema) -> Result<()> {
    if bytes.len() < 18
        || bytes.len() > V34_MAX_ARROW_BYTES
        || !bytes.starts_with(b"ARROW1\0\0")
        || !bytes.ends_with(b"ARROW1")
    {
        return Err(invalid("V34 rank-four IPC magic or length differs"));
    }
    let trailer = bytes.len() - 10;
    let footer_len = u32::from_le_bytes(
        bytes[trailer..trailer + 4]
            .try_into()
            .map_err(|_| invalid("V34 rank-four IPC footer length differs"))?,
    ) as usize;
    let footer_start = trailer
        .checked_sub(footer_len)
        .filter(|offset| *offset >= 8)
        .ok_or_else(|| invalid("V34 rank-four IPC footer extent differs"))?;
    let footer = arrow_ipc::root_as_footer(&bytes[footer_start..trailer])
        .map_err(|_| invalid("V34 rank-four IPC footer differs"))?;
    validate_v34_ipc_schema(
        footer
            .schema()
            .ok_or_else(|| invalid("V34 rank-four IPC footer schema is missing"))?,
        expected_schema,
    )?;
    if footer
        .dictionaries()
        .is_some_and(|values| !values.is_empty())
    {
        return Err(invalid("V34 rank-four IPC dictionaries are forbidden"));
    }
    let blocks = footer
        .recordBatches()
        .ok_or_else(|| invalid("V34 rank-four IPC batch is missing"))?;
    if blocks.len() != 1 {
        return Err(invalid("V34 rank-four IPC batch count differs"));
    }
    let block = blocks.get(0);
    let block_offset = usize::try_from(block.offset())
        .map_err(|_| invalid("V34 rank-four IPC batch offset differs"))?;
    let metadata_len = usize::try_from(block.metaDataLength())
        .map_err(|_| invalid("V34 rank-four IPC batch metadata differs"))?;
    let body_len = usize::try_from(block.bodyLength())
        .map_err(|_| invalid("V34 rank-four IPC batch body differs"))?;
    let body_start = block_offset
        .checked_add(metadata_len)
        .ok_or_else(|| invalid("V34 rank-four IPC batch extent overflows"))?;
    let body_end = body_start
        .checked_add(body_len)
        .ok_or_else(|| invalid("V34 rank-four IPC batch extent overflows"))?;
    if block_offset < 8 || metadata_len < 8 || body_end > footer_start {
        return Err(invalid("V34 rank-four IPC batch extent differs"));
    }

    let parse_message = |start: usize, end: usize| {
        let metadata = bytes
            .get(start..end)
            .ok_or_else(|| invalid("V34 rank-four IPC message extent differs"))?;
        if metadata.len() < 4 {
            return Err(invalid("V34 rank-four IPC message is truncated"));
        }
        let prefix = if metadata.starts_with(&[255; 4]) {
            8
        } else {
            4
        };
        let length_bytes = metadata
            .get(prefix - 4..prefix)
            .ok_or_else(|| invalid("V34 rank-four IPC message length differs"))?;
        let message_len = u32::from_le_bytes(
            length_bytes
                .try_into()
                .map_err(|_| invalid("V34 rank-four IPC message length differs"))?,
        ) as usize;
        let message_end = prefix
            .checked_add(message_len)
            .filter(|value| *value <= metadata.len())
            .ok_or_else(|| invalid("V34 rank-four IPC message extent differs"))?;
        arrow_ipc::root_as_message(&metadata[prefix..message_end])
            .map_err(|_| invalid("V34 rank-four IPC message differs"))
    };

    let leading = parse_message(8, block_offset)?;
    if leading.bodyLength() != 0 {
        return Err(invalid("V34 rank-four IPC leading schema body differs"));
    }
    validate_v34_ipc_schema(
        leading
            .header_as_schema()
            .ok_or_else(|| invalid("V34 rank-four IPC leading schema is missing"))?,
        expected_schema,
    )?;

    let record_message = parse_message(block_offset, body_start)?;
    let record = record_message
        .header_as_record_batch()
        .ok_or_else(|| invalid("V34 rank-four IPC record differs"))?;
    let rows = usize::try_from(record.length())
        .map_err(|_| invalid("V34 rank-four IPC row count differs"))?;
    if !(1..=V34_MAX_LEAVES).contains(&rows)
        || record.compression().is_some()
        || usize::try_from(record_message.bodyLength()).ok() != Some(body_len)
    {
        return Err(invalid("V34 rank-four IPC record authority differs"));
    }
    let nodes = record
        .nodes()
        .ok_or_else(|| invalid("V34 rank-four IPC nodes are missing"))?;
    let expected_nodes = [
        rows,
        rows,
        rows,
        rows,
        rows,
        rows * V34_DIMENSIONS,
        rows,
        rows * V34_DIMENSIONS,
        rows,
        rows * V34_COMPONENTS,
        rows,
        rows * V34_COMPONENTS,
        rows * V34_COMPONENTS * V34_DIMENSIONS,
        rows,
        rows,
        rows,
        rows,
    ];
    if nodes.len() != expected_nodes.len() {
        return Err(invalid("V34 rank-four IPC node count differs"));
    }
    for (node, expected) in nodes.iter().zip(expected_nodes) {
        if usize::try_from(node.length()).ok() != Some(expected) || node.null_count() != 0 {
            return Err(invalid("V34 rank-four IPC node shape differs"));
        }
    }
    let buffers = record
        .buffers()
        .ok_or_else(|| invalid("V34 rank-four IPC buffers are missing"))?;
    if buffers.len() != 29 {
        return Err(invalid("V34 rank-four IPC buffer count differs"));
    }
    let body = &bytes[body_start..body_end];
    let mut slices = Vec::with_capacity(buffers.len());
    let mut previous_end = 0_usize;
    for buffer in buffers {
        let start = usize::try_from(buffer.offset())
            .map_err(|_| invalid("V34 rank-four IPC buffer offset differs"))?;
        let length = usize::try_from(buffer.length())
            .map_err(|_| invalid("V34 rank-four IPC buffer length differs"))?;
        let end = start
            .checked_add(length)
            .ok_or_else(|| invalid("V34 rank-four IPC buffer extent overflows"))?;
        if start < previous_end {
            return Err(invalid("V34 rank-four IPC buffers overlap"));
        }
        slices.push(
            body.get(start..end)
                .ok_or_else(|| invalid("V34 rank-four IPC buffer extent differs"))?,
        );
        previous_end = end;
    }
    for (index, count) in [
        (0, rows),
        (2, rows),
        (4, rows),
        (6, rows),
        (8, rows),
        (9, rows * V34_DIMENSIONS),
        (11, rows),
        (12, rows * V34_DIMENSIONS),
        (14, rows),
        (15, rows * V34_COMPONENTS),
        (17, rows),
        (18, rows * V34_COMPONENTS),
        (19, rows * V34_COMPONENTS * V34_DIMENSIONS),
        (21, rows),
        (23, rows),
        (25, rows),
        (27, rows),
    ] {
        if !slices[index].is_empty()
            && (slices[index].len() != count.div_ceil(8)
                || (0..count).any(|bit| slices[index][bit / 8] & (1 << (bit % 8)) == 0))
        {
            return Err(invalid("V34 rank-four IPC null bitmap differs"));
        }
    }
    for (index, length) in [
        (1, rows * 4),
        (3, rows * 4),
        (5, rows * 8),
        (7, rows * 4),
        (10, rows * V34_DIMENSIONS * 4),
        (13, rows * V34_DIMENSIONS * 4),
        (16, rows * V34_COMPONENTS * 4),
        (20, rows * V34_COMPONENTS * V34_DIMENSIONS * 4),
        (22, rows * 8),
        (24, rows * 8),
        (26, rows * 8),
        (28, rows * 8),
    ] {
        if slices[index].len() != length {
            return Err(invalid("V34 rank-four IPC value length differs"));
        }
    }
    Ok(())
}

/// Encode one immutable rank-four-only Arrow IPC generation.
pub fn encode_v34_rank4_arrow(
    generation: &V34Rank4Generation,
    uri: &str,
    source_archive_sha256: &str,
    reconstruction_sha256: &str,
    codebooks_sha256: &str,
) -> Result<(Vec<u8>, V34Rank4ArtifactIdentity)> {
    let mut identity = V34Rank4ArtifactIdentity {
        uri: uri.to_owned(),
        sha256: "0".repeat(64),
        length: 1,
        source_archive_sha256: source_archive_sha256.to_owned(),
        reconstruction_sha256: reconstruction_sha256.to_owned(),
        codebooks_sha256: codebooks_sha256.to_owned(),
        metric: V34_METRIC.to_owned(),
        dimensions: V34_DIMENSIONS as u32,
        normalization: V34_NORMALIZATION.to_owned(),
        scorer_version: V34_SCORER_VERSION.to_owned(),
    };
    validate_v34_artifact_identity(&identity)?;
    if generation.leaves.is_empty() {
        return Err(invalid("V34 rank-four artifact generation is empty"));
    }
    let schema = v34_rank4_arrow_schema(&identity)?;
    let eigenvalues = Arc::new(FixedSizeListArray::try_new(
        Arc::new(Field::new("element", DataType::Float32, false)),
        V34_COMPONENTS as i32,
        Arc::new(Float32Array::from(
            generation
                .leaves
                .iter()
                .flat_map(|leaf| leaf.eigenvalues)
                .collect::<Vec<_>>(),
        )),
        None,
    )?);
    let direction_vectors = FixedSizeListArray::try_new(
        Arc::new(Field::new("element", DataType::Float32, false)),
        V34_DIMENSIONS as i32,
        Arc::new(Float32Array::from(
            generation
                .leaves
                .iter()
                .flat_map(|leaf| leaf.directions.iter().flatten().copied())
                .collect::<Vec<_>>(),
        )),
        None,
    )?;
    let directions = Arc::new(FixedSizeListArray::try_new(
        Arc::new(Field::new("direction", v34_vector_type(), false)),
        V34_COMPONENTS as i32,
        Arc::new(direction_vectors),
        None,
    )?);
    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(UInt32Array::from_iter_values(
                generation.leaves.iter().map(|leaf| leaf.leaf_ordinal),
            )),
            Arc::new(UInt32Array::from_iter_values(
                generation.leaves.iter().map(|leaf| leaf.group_ordinal),
            )),
            Arc::new(UInt64Array::from_iter_values(
                generation.leaves.iter().map(|leaf| leaf.logical_start),
            )),
            Arc::new(UInt32Array::from_iter_values(
                generation.leaves.iter().map(|leaf| leaf.population),
            )),
            v34_vector_array(generation.leaves.iter().map(|leaf| &leaf.mean))?,
            v34_vector_array(generation.leaves.iter().map(|leaf| &leaf.residual_diagonal))?,
            eigenvalues,
            directions,
            Arc::new(Float64Array::from_iter_values(
                generation.leaves.iter().map(|leaf| leaf.population_factor),
            )),
            Arc::new(Float64Array::from_iter_values(
                generation.leaves.iter().map(|leaf| leaf.trace),
            )),
            Arc::new(Float64Array::from_iter_values(
                generation.leaves.iter().map(|leaf| leaf.trace_square),
            )),
            Arc::new(Float64Array::from_iter_values(
                generation.leaves.iter().map(|leaf| leaf.spectral_bound),
            )),
        ],
    )?;
    let mut bytes = Vec::new();
    let options = IpcWriteOptions::try_new(8, false, MetadataVersion::V5)?;
    let mut writer = FileWriter::try_new_with_options(&mut bytes, &schema, options)?;
    writer.write(&batch)?;
    writer.finish()?;
    drop(writer);
    identity.length = u64::try_from(bytes.len())
        .map_err(|_| invalid("V34 rank-four artifact length overflows"))?;
    identity.sha256 = format!("{:x}", Sha256::digest(&bytes));
    Ok((bytes, identity))
}

fn v34_fixed_vector(list: &FixedSizeListArray, row: usize) -> Result<[f32; V34_DIMENSIONS]> {
    let child_values = list.value(row);
    let child = child_values
        .as_any()
        .downcast_ref::<Float32Array>()
        .ok_or_else(|| invalid("V34 rank-four Arrow vector differs"))?;
    if child.null_count() != 0 {
        return Err(invalid("V34 rank-four Arrow vector null differs"));
    }
    child
        .values()
        .to_vec()
        .try_into()
        .map_err(|_| invalid("V34 rank-four Arrow vector width differs"))
}

/// Authenticate and decode one rank-four-only Arrow IPC generation.
pub fn decode_v34_rank4_arrow(
    bytes: &[u8],
    identity: &V34Rank4ArtifactIdentity,
) -> Result<V34Rank4Generation> {
    validate_v34_artifact_identity(identity)?;
    if identity.length != bytes.len() as u64
        || format!("{:x}", Sha256::digest(bytes)) != identity.sha256
    {
        return Err(invalid("V34 rank-four artifact bytes differ"));
    }
    let expected_schema = v34_rank4_arrow_schema(identity)?;
    validate_v34_ipc_envelope(bytes, &expected_schema)?;
    let mut reader = FileReader::try_new(Cursor::new(bytes), None)?;
    if reader.schema().as_ref() != &expected_schema {
        return Err(invalid("V34 rank-four Arrow schema differs"));
    }
    let batch = reader
        .next()
        .transpose()?
        .ok_or_else(|| invalid("V34 rank-four Arrow batch is missing"))?;
    if reader.next().is_some()
        || batch.num_rows() == 0
        || batch
            .columns()
            .iter()
            .any(|column| column.null_count() != 0)
    {
        return Err(invalid("V34 rank-four Arrow batch differs"));
    }
    let u32_column = |index: usize| {
        batch
            .column(index)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or_else(|| invalid("V34 rank-four Arrow integer differs"))
    };
    let u64_column = |index: usize| {
        batch
            .column(index)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| invalid("V34 rank-four Arrow integer differs"))
    };
    let list_column = |index: usize| {
        batch
            .column(index)
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .ok_or_else(|| invalid("V34 rank-four Arrow list differs"))
    };
    let float64_column = |index: usize| {
        batch
            .column(index)
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| invalid("V34 rank-four Arrow scalar differs"))
    };
    let leaf_ordinals = u32_column(0)?;
    let group_ordinals = u32_column(1)?;
    let logical_starts = u64_column(2)?;
    let populations = u32_column(3)?;
    let means = list_column(4)?;
    let residuals = list_column(5)?;
    let eigenvalue_lists = list_column(6)?;
    let direction_lists = list_column(7)?;
    let population_factors = float64_column(8)?;
    let traces = float64_column(9)?;
    let trace_squares = float64_column(10)?;
    let spectral_bounds = float64_column(11)?;
    let mut inputs = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        let eigenvalue_values = eigenvalue_lists.value(row);
        let eigenvalue_array = eigenvalue_values
            .as_any()
            .downcast_ref::<Float32Array>()
            .ok_or_else(|| invalid("V34 rank-four Arrow eigenvalues differ"))?;
        if eigenvalue_array.null_count() != 0 {
            return Err(invalid("V34 rank-four Arrow eigenvalue null differs"));
        }
        let eigenvalues = eigenvalue_array
            .values()
            .to_vec()
            .try_into()
            .map_err(|_| invalid("V34 rank-four Arrow eigenvalue width differs"))?;
        let row_direction_values = direction_lists.value(row);
        let row_directions = row_direction_values
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .ok_or_else(|| invalid("V34 rank-four Arrow directions differ"))?;
        if row_directions.null_count() != 0 {
            return Err(invalid("V34 rank-four Arrow direction null differs"));
        }
        let mut directions = [[0.0_f32; V34_DIMENSIONS]; V34_COMPONENTS];
        for (component, direction) in directions.iter_mut().enumerate() {
            *direction = v34_fixed_vector(row_directions, component)?;
        }
        let input = V34Rank4LeafInput {
            leaf_ordinal: leaf_ordinals.value(row),
            group_ordinal: group_ordinals.value(row),
            logical_start: logical_starts.value(row),
            population: populations.value(row),
            mean: v34_fixed_vector(means, row)?,
            residual_diagonal: v34_fixed_vector(residuals, row)?,
            eigenvalues,
            directions,
        };
        if input
            .mean
            .iter()
            .chain(input.residual_diagonal.iter())
            .chain(input.eigenvalues.iter())
            .chain(input.directions.iter().flatten())
            .any(|value| *value == 0.0 && value.to_bits() != 0)
            || (0..V34_COMPONENTS).any(|component| {
                input.eigenvalues[component] == 0.0
                    && input.directions[component]
                        .iter()
                        .any(|value| value.to_bits() != 0)
            })
        {
            return Err(invalid("V34 rank-four persisted zero authority differs"));
        }
        inputs.push(input);
    }
    let generation = build_v34_rank4_generation(inputs)?;
    for (row, leaf) in generation.leaves.iter().enumerate() {
        if population_factors.value(row).to_bits() != leaf.population_factor.to_bits()
            || traces.value(row).to_bits() != leaf.trace.to_bits()
            || trace_squares.value(row).to_bits() != leaf.trace_square.to_bits()
            || spectral_bounds.value(row).to_bits() != leaf.spectral_bound.to_bits()
        {
            return Err(invalid("V34 rank-four Arrow cached moments differ"));
        }
    }
    Ok(generation)
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
