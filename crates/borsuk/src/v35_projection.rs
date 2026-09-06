use std::{
    collections::{BTreeMap, HashMap},
    io::Cursor,
    sync::Arc,
};

use crate::{BorsukError, Result, V35ArtifactIdentity, V35Dimensions, V35ProjectionArm};
use arrow_array::{Array, FixedSizeListArray, Float32Array, RecordBatch};
use arrow_ipc::{
    MetadataVersion,
    reader::FileReader,
    writer::{FileWriter, IpcWriteOptions},
};
use arrow_schema::{DataType, Field, Schema};
use borsuk_fma::{FmaBackend, FusedProjection4};
use nalgebra::{DMatrix, SymmetricEigen};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

const SRHT_ALGORITHM: &str = "srht-prefix-orthonormalized-v1";
const PCA_ALGORITHM: &str = "uncentered-subspace-iteration-rr-v1";
const DEPENDENCE_THRESHOLD: f64 = 1e-12;
const GRAM_TOLERANCE: f64 = 5e-4;
const EIGEN_CLUSTER_RELATIVE_TOLERANCE: f64 = 1e-10;
const TRAINING_WORKSPACE_LIMIT_BYTES: u64 = 64 * 1_048_576;
const SIGN_SEED_DOMAIN: u64 = 0x7633_352d_7369_676e;
const ROW_SEED_DOMAIN: u64 = 0x7633_352d_726f_7773;
const PROJECTION_MANIFEST_KEY: &str = "borsuk.v35.projection.manifest";

fn invalid(message: &str) -> BorsukError {
    BorsukError::InvalidStorage(message.to_owned())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Arithmetic backend used by one V35 query projection.
pub enum V35ProjectionBackend {
    /// AArch64 NEON-capable fused arithmetic.
    Aarch64NeonFma,
    /// x86 AVX/FMA-capable fused arithmetic.
    X86AvxFma,
    /// Scalar fused control, never accepted as the serving SIMD backend.
    ScalarControl,
}

impl V35ProjectionBackend {
    /// Whether the backend preserves the registered fused-lane arithmetic.
    pub fn is_fused(self) -> bool {
        !matches!(self, Self::ScalarControl)
    }
}

#[derive(Debug, Clone, PartialEq)]
/// Immutable decoded f32 projection used by every downstream V35 consumer.
pub struct V35Projection {
    dimensions: V35Dimensions,
    arm: V35ProjectionArm,
    seed: u64,
    algorithm: String,
    sample_sha256: Option<String>,
    training_descriptor: String,
    basis_source_major: Vec<f32>,
    checksum: [u8; 32],
}

impl V35Projection {
    /// Authenticated source and routing dimensions.
    pub fn dimensions(&self) -> V35Dimensions {
        self.dimensions
    }

    /// Projection arm.
    pub fn arm(&self) -> V35ProjectionArm {
        self.arm
    }

    /// Query-independent construction seed.
    pub fn seed(&self) -> u64 {
        self.seed
    }

    /// Frozen algorithm identifier.
    pub fn algorithm(&self) -> &str {
        &self.algorithm
    }

    /// PCA sample identity, absent for the query-independent control.
    pub fn sample_sha256(&self) -> Option<&str> {
        self.sample_sha256.as_deref()
    }

    /// Canonical complete trainer descriptor bound by the logical checksum.
    pub fn training_descriptor(&self) -> &str {
        &self.training_descriptor
    }

    /// Source-major decoded f32 coefficient storage.
    pub fn basis_source_major(&self) -> &[f32] {
        &self.basis_source_major
    }

    /// Logical projection checksum independent of Arrow framing.
    pub fn checksum(&self) -> [u8; 32] {
        self.checksum
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct V35ProjectionManifest {
    algorithm: String,
    arm: V35ProjectionArm,
    checksum_sha256: String,
    dimensions: V35Dimensions,
    sample_sha256: Option<String>,
    seed: u64,
    storage_order: String,
    training_descriptor: String,
    role: String,
    uri: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PcaTrainingDescriptor {
    algorithm: String,
    arithmetic: String,
    eigen_cluster_relative_tolerance: String,
    maximum_block_rows: u64,
    oversampling: u64,
    sample_passes: u64,
    sample_role: String,
    sample_rows: u64,
    selection_policy: String,
    source_archive_sha256: String,
    subspace_iterations: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SrhtTrainingDescriptor {
    algorithm: String,
    dependence_threshold: String,
    modified_gram_schmidt_passes: u64,
    padded_dimension: u64,
    row_permutation: String,
    skipped_rows: Vec<u64>,
}

#[derive(Debug, Clone, PartialEq)]
/// One projected query plus its clamped complement-energy heuristic.
pub struct V35ProjectedQuery {
    coordinates: Vec<f64>,
    complement_energy: f64,
    backend: V35ProjectionBackend,
    source_query: Vec<f32>,
    source_digest: [u8; 32],
    projection_checksum: [u8; 32],
}

impl V35ProjectedQuery {
    /// Routing coordinates in logical output order.
    pub fn coordinates(&self) -> &[f64] {
        &self.coordinates
    }

    /// `max(0, ||q||²-||Pq||²)` over the decoded f32 basis.
    pub fn complement_energy(&self) -> f64 {
        self.complement_energy
    }

    /// Registered arithmetic backend.
    pub fn backend(&self) -> V35ProjectionBackend {
        self.backend
    }

    /// Canonical source query retained for exact reranking.
    pub fn source_query(&self) -> &[f32] {
        &self.source_query
    }

    /// Domain-separated digest of source query, dimensions, and projection.
    pub fn source_digest(&self) -> [u8; 32] {
        self.source_digest
    }

    /// Exact projection basis used to derive routing coordinates.
    pub fn projection_checksum(&self) -> [u8; 32] {
        self.projection_checksum
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Frozen authority for one bounded, query-independent PCA training sample.
pub struct V35ProjectionTrainingSpec {
    /// Original and resident routing dimensions.
    pub dimensions: V35Dimensions,
    /// Exact number of sample rows; ordinal membership is digest-streamed.
    pub sample_rows: u64,
    /// SHA-256 of ordinal plus source-major f32 sample bytes.
    pub sample_sha256: String,
    /// Capability-isolated construction role; query/truth roles are forbidden.
    pub sample_role: String,
    /// Domain-separated deterministic trainer seed.
    pub seed: u64,
    /// Query-independent deterministic membership rule.
    pub selection_policy: String,
    /// Immutable source archive SHA-256.
    pub source_archive_sha256: String,
    /// Exact trainer algorithm.
    pub trainer: String,
    /// Hard per-callback row cap, at most 2,048.
    pub maximum_block_rows: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Explicit encoded/decoded peak-memory admission for one projection codec.
pub struct V35ProjectionLimits {
    /// Maximum complete Arrow IPC bytes.
    pub maximum_encoded_bytes: u64,
    /// Maximum decoded f32 coefficient bytes.
    pub maximum_decoded_numeric_bytes: u64,
    /// Maximum simultaneous encoded, Arrow-decoded, and owned decoded bytes.
    pub maximum_peak_codec_bytes: u64,
    /// Checked trainer matrices plus one maximum-size source block.
    pub projected_training_workspace_bytes: u64,
    /// Hard construction workspace limit, excluding the remote source corpus.
    pub maximum_training_workspace_bytes: u64,
}

impl V35ProjectionLimits {
    /// Construct the exact one-basis codec admission for these dimensions.
    pub fn for_dimensions(dimensions: V35Dimensions) -> Result<Self> {
        let (source, routing) = validate_dimensions(dimensions)?;
        let numeric = source
            .checked_mul(routing)
            .and_then(|value| value.checked_mul(std::mem::size_of::<f32>()))
            .and_then(|value| u64::try_from(value).ok())
            .ok_or_else(|| invalid("V35 projection numeric admission overflows"))?;
        let maximum_encoded_bytes = numeric
            .checked_add(1_048_576)
            .ok_or_else(|| invalid("V35 projection encoded admission overflows"))?;
        let maximum_peak_codec_bytes = maximum_encoded_bytes
            .checked_add(
                numeric
                    .checked_mul(2)
                    .ok_or_else(|| invalid("V35 projection peak admission overflows"))?,
            )
            .ok_or_else(|| invalid("V35 projection peak admission overflows"))?;
        let projected_training_workspace_bytes = projected_training_workspace(dimensions, 2_048)?;
        Ok(Self {
            maximum_encoded_bytes,
            maximum_decoded_numeric_bytes: numeric,
            maximum_peak_codec_bytes,
            projected_training_workspace_bytes,
            maximum_training_workspace_bytes: TRAINING_WORKSPACE_LIMIT_BYTES,
        })
    }
}

fn projected_training_workspace(dimensions: V35Dimensions, block_rows: usize) -> Result<u64> {
    let (source, routing) = validate_dimensions(dimensions)?;
    let columns = source.min(routing + 16);
    let matrix_bytes = u64::try_from(source)
        .ok()
        .and_then(|rows| rows.checked_mul(u64::try_from(columns).ok()?))
        .and_then(|values| values.checked_mul(8))
        .ok_or_else(|| invalid("V35 projection training admission overflows"))?;
    let selected_bytes = u64::try_from(source)
        .ok()
        .and_then(|rows| rows.checked_mul(u64::try_from(routing).ok()?))
        .and_then(|values| values.checked_mul(8))
        .ok_or_else(|| invalid("V35 projection training admission overflows"))?;
    let small_matrix_bytes = u64::try_from(columns)
        .ok()
        .and_then(|width| width.checked_mul(width))
        .and_then(|values| values.checked_mul(8))
        .and_then(|bytes| bytes.checked_mul(3))
        .ok_or_else(|| invalid("V35 projection training admission overflows"))?;
    let block_bytes = u64::try_from(source)
        .ok()
        .and_then(|width| width.checked_mul(u64::try_from(block_rows).ok()?))
        .and_then(|values| values.checked_mul(4))
        .ok_or_else(|| invalid("V35 projection training admission overflows"))?;
    matrix_bytes
        .checked_mul(3)
        .and_then(|bytes| bytes.checked_add(selected_bytes))
        .and_then(|bytes| bytes.checked_add(small_matrix_bytes))
        .and_then(|bytes| bytes.checked_add(block_bytes))
        .ok_or_else(|| invalid("V35 projection training admission overflows"))
}

/// Callback used to consume one bounded source-ordinal sample block.
pub type V35ProjectionBlockVisitor<'a> = dyn FnMut(&[u64], &[f32]) -> Result<()> + 'a;

/// Restartable four-pass source for one registered PCA sample.
pub trait V35ProjectionSampleSource {
    /// Scan the same authenticated sample once in increasing ordinal order.
    fn scan(&mut self, visitor: &mut V35ProjectionBlockVisitor<'_>) -> Result<()>;
}

fn splitmix64(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut value = *state;
    value = (value ^ (value >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    value ^ (value >> 31)
}

fn validate_dimensions(dimensions: V35Dimensions) -> Result<(usize, usize)> {
    if dimensions.source == 0
        || !matches!(dimensions.routing, 64 | 128 | 192)
        || u32::from(dimensions.routing) > dimensions.source
    {
        return Err(invalid("V35 projection dimensions differ"));
    }
    let source = usize::try_from(dimensions.source)
        .map_err(|_| invalid("V35 projection source dimension overflows"))?;
    let routing = usize::from(dimensions.routing);
    source
        .checked_mul(routing)
        .ok_or_else(|| invalid("V35 projection allocation overflows"))?;
    Ok((source, routing))
}

fn canonicalize_row(row: &mut [f64]) -> Result<()> {
    let pivot = row
        .iter()
        .enumerate()
        .max_by(|(left_index, left), (right_index, right)| {
            left.abs()
                .total_cmp(&right.abs())
                .then_with(|| right_index.cmp(left_index))
        })
        .map(|(index, value)| (index, *value))
        .ok_or_else(|| invalid("V35 projection row is empty"))?;
    if pivot.1 == 0.0 || !pivot.1.is_finite() {
        return Err(invalid("V35 projection row has no canonical sign"));
    }
    if pivot.1.is_sign_negative() {
        row.iter_mut().for_each(|value| *value = -*value);
    }
    for value in row {
        if !value.is_finite() {
            return Err(invalid("V35 projection row is nonfinite"));
        }
        if *value == 0.0 {
            *value = 0.0;
        }
    }
    Ok(())
}

fn projection_checksum(
    arm: V35ProjectionArm,
    dimensions: V35Dimensions,
    seed: u64,
    algorithm: &str,
    sample_sha256: Option<&str>,
    training_descriptor: &str,
    basis: &[f32],
) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(b"borsuk-v35-projection-v1\n");
    digest.update([match arm {
        V35ProjectionArm::Pca => 0,
        V35ProjectionArm::Srht => 1,
    }]);
    digest.update(dimensions.source.to_le_bytes());
    digest.update(dimensions.routing.to_le_bytes());
    digest.update(seed.to_le_bytes());
    let sample = sample_sha256.unwrap_or("");
    digest.update((sample.len() as u64).to_le_bytes());
    digest.update(sample.as_bytes());
    digest.update((algorithm.len() as u64).to_le_bytes());
    digest.update(algorithm.as_bytes());
    digest.update((training_descriptor.len() as u64).to_le_bytes());
    digest.update(training_descriptor.as_bytes());
    for value in basis {
        digest.update(value.to_bits().to_le_bytes());
    }
    digest.finalize().into()
}

fn validate_decoded_basis(dimensions: V35Dimensions, basis: &[f32]) -> Result<()> {
    let (source, routing) = validate_dimensions(dimensions)?;
    if basis.len() != source * routing
        || basis
            .iter()
            .any(|value| !value.is_finite() || (*value == 0.0 && value.is_sign_negative()))
    {
        return Err(invalid("V35 decoded projection basis differs"));
    }
    for left in 0..routing {
        for right in 0..routing {
            let dot = basis.chunks_exact(routing).fold(0.0_f64, |sum, row| {
                f64::from(row[left]).mul_add(f64::from(row[right]), sum)
            });
            let expected = if left == right { 1.0 } else { 0.0 };
            if (dot - expected).abs() > GRAM_TOLERANCE {
                return Err(invalid("V35 decoded projection Gram matrix differs"));
            }
        }
    }
    Ok(())
}

fn is_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn validate_training_spec(spec: &V35ProjectionTrainingSpec) -> Result<(usize, usize, usize)> {
    let (source, routing) = validate_dimensions(spec.dimensions)?;
    let workspace = projected_training_workspace(spec.dimensions, spec.maximum_block_rows)?;
    if spec.trainer != PCA_ALGORITHM
        || !is_sha256(&spec.sample_sha256)
        || !is_sha256(&spec.source_archive_sha256)
        || spec.sample_rows == 0
        || spec.sample_role != "construction-projection-sample"
        || spec.selection_policy != "source-ordinal-hash-bottom-k-v1"
        || spec.maximum_block_rows == 0
        || spec.maximum_block_rows > 2_048
        || workspace > TRAINING_WORKSPACE_LIMIT_BYTES
    {
        return Err(invalid("V35 PCA training authority differs"));
    }
    Ok((source, routing, source.min(routing + 16)))
}

fn scan_covariance_application(
    source_rows: &mut dyn V35ProjectionSampleSource,
    spec: &V35ProjectionTrainingSpec,
    input: &DMatrix<f64>,
) -> Result<DMatrix<f64>> {
    let (source, _, columns) = validate_training_spec(spec)?;
    if input.nrows() != source || input.ncols() != columns {
        return Err(invalid("V35 PCA covariance input differs"));
    }
    let mut output = DMatrix::<f64>::zeros(source, columns);
    let mut seen = 0_u64;
    let mut previous = None;
    let mut digest = Sha256::new();
    digest.update(b"borsuk-v35-projection-sample-v1\n");
    source_rows.scan(&mut |ordinals, values| {
        if ordinals.is_empty()
            || ordinals.len() > spec.maximum_block_rows
            || values.len() != ordinals.len().saturating_mul(source)
            || seen
                .checked_add(u64::try_from(ordinals.len()).unwrap_or(u64::MAX))
                .is_none_or(|end| end > spec.sample_rows)
        {
            return Err(invalid("V35 PCA sample block differs"));
        }
        if previous.is_some_and(|prior| ordinals.first().is_some_and(|first| *first <= prior))
            || ordinals.windows(2).any(|pair| pair[0] >= pair[1])
            || values.iter().any(|value| !value.is_finite())
        {
            return Err(invalid("V35 PCA sample authority differs"));
        }
        for (row_index, ordinal) in ordinals.iter().enumerate() {
            let row = &values[row_index * source..(row_index + 1) * source];
            digest.update(ordinal.to_le_bytes());
            for value in row {
                digest.update(value.to_bits().to_le_bytes());
            }
            previous = Some(*ordinal);
            for column in 0..columns {
                let dot = (0..source).fold(0.0_f64, |sum, dimension| {
                    f64::from(row[dimension]).mul_add(input[(dimension, column)], sum)
                });
                for dimension in 0..source {
                    output[(dimension, column)] =
                        f64::from(row[dimension]).mul_add(dot, output[(dimension, column)]);
                }
            }
        }
        seen += u64::try_from(ordinals.len())
            .map_err(|_| invalid("V35 PCA sample row count overflows"))?;
        Ok(())
    })?;
    if seen != spec.sample_rows || format!("{:x}", digest.finalize()) != spec.sample_sha256 {
        return Err(invalid("V35 PCA sample digest differs"));
    }
    Ok(output)
}

fn orthonormalize_columns(mut candidates: DMatrix<f64>) -> Result<DMatrix<f64>> {
    let rows = candidates.nrows();
    let columns = candidates.ncols();
    let mut accepted = DMatrix::<f64>::zeros(rows, columns);
    for column in 0..columns {
        let original = candidates
            .column(column)
            .iter()
            .copied()
            .collect::<Vec<_>>();
        let mut accepted_candidate = false;
        for completion in 0..=rows {
            let mut candidate = if completion == 0 {
                original.clone()
            } else {
                vec![0.0; rows]
            };
            if completion > 0 {
                candidate[completion - 1] = 1.0;
            }
            let original_norm = candidate
                .iter()
                .fold(0.0_f64, |sum, value| value.mul_add(*value, sum))
                .sqrt();
            for _ in 0..2 {
                for prior in 0..column {
                    let dot = (0..rows).fold(0.0_f64, |sum, row| {
                        candidate[row].mul_add(accepted[(row, prior)], sum)
                    });
                    for row in 0..rows {
                        candidate[row] = (-dot).mul_add(accepted[(row, prior)], candidate[row]);
                    }
                }
            }
            let norm_squared = candidate
                .iter()
                .fold(0.0_f64, |sum, value| value.mul_add(*value, sum));
            if norm_squared.is_finite()
                && original_norm.is_finite()
                && norm_squared.sqrt() > original_norm * DEPENDENCE_THRESHOLD
            {
                let inverse = norm_squared.sqrt().recip();
                candidate.iter_mut().for_each(|value| *value *= inverse);
                canonicalize_row(&mut candidate)?;
                for row in 0..rows {
                    accepted[(row, column)] = candidate[row];
                }
                accepted_candidate = true;
                break;
            }
        }
        if !accepted_candidate {
            return Err(invalid("V35 PCA rank completion failed"));
        }
    }
    candidates.fill(0.0);
    Ok(accepted)
}

fn same_eigenvalue_cluster(left: f64, right: f64, spectrum_scale: f64) -> bool {
    (left - right).abs() <= EIGEN_CLUSTER_RELATIVE_TOLERANCE * spectrum_scale
}

fn canonical_cluster_vectors(
    basis: &DMatrix<f64>,
    eigenvectors: &DMatrix<f64>,
    cluster: &[usize],
) -> Result<Vec<Vec<f64>>> {
    let source = basis.nrows();
    let columns = basis.ncols();
    let mut span = DMatrix::<f64>::zeros(source, cluster.len());
    for (span_column, eigen_column) in cluster.iter().copied().enumerate() {
        for row in 0..source {
            span[(row, span_column)] = (0..columns).fold(0.0_f64, |sum, column| {
                basis[(row, column)].mul_add(eigenvectors[(column, eigen_column)], sum)
            });
        }
    }
    let mut accepted = Vec::<Vec<f64>>::with_capacity(cluster.len());
    for axis in 0..source {
        let mut candidate = (0..source)
            .map(|row| {
                (0..cluster.len()).fold(0.0_f64, |sum, column| {
                    span[(row, column)].mul_add(span[(axis, column)], sum)
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
        if norm.is_finite()
            && original_norm.is_finite()
            && norm > original_norm * EIGEN_CLUSTER_RELATIVE_TOLERANCE
        {
            let inverse = norm.recip();
            candidate.iter_mut().for_each(|value| *value *= inverse);
            canonicalize_row(&mut candidate)?;
            accepted.push(candidate);
            if accepted.len() == cluster.len() {
                return Ok(accepted);
            }
        }
    }
    Err(invalid("V35 PCA eigenspace canonicalization failed"))
}

/// Train the registered uncentered PCA basis with exactly four sample passes.
pub fn train_v35_pca(
    spec: &V35ProjectionTrainingSpec,
    sample: &mut dyn V35ProjectionSampleSource,
) -> Result<V35Projection> {
    let (source, routing, columns) = validate_training_spec(spec)?;
    let mut state = spec.seed ^ 0x7633_352d_7063_612d;
    let mut sketch = DMatrix::<f64>::from_fn(source, columns, |_, _| {
        if splitmix64(&mut state) & 1 == 0 {
            1.0
        } else {
            -1.0
        }
    });
    let mut basis = orthonormalize_columns(scan_covariance_application(sample, spec, &sketch)?)?;
    sketch.fill(0.0);
    for _ in 0..2 {
        basis = orthonormalize_columns(scan_covariance_application(sample, spec, &basis)?)?;
    }
    let covariance_basis = scan_covariance_application(sample, spec, &basis)?;
    let mut rayleigh = DMatrix::<f64>::zeros(columns, columns);
    for left in 0..columns {
        for right in left..columns {
            let value = (0..source).fold(0.0_f64, |sum, row| {
                basis[(row, left)].mul_add(covariance_basis[(row, right)], sum)
            });
            rayleigh[(left, right)] = value;
            rayleigh[(right, left)] = value;
        }
    }
    let eigen = SymmetricEigen::new(rayleigh);
    let mut order = (0..columns).collect::<Vec<_>>();
    order.sort_by(|left, right| {
        eigen.eigenvalues[*right]
            .total_cmp(&eigen.eigenvalues[*left])
            .then_with(|| left.cmp(right))
    });
    let spectrum_scale = eigen
        .eigenvalues
        .iter()
        .fold(0.0_f64, |scale, value| scale.max(value.abs()));
    if !spectrum_scale.is_finite() || spectrum_scale == 0.0 {
        return Err(invalid("V35 PCA eigenspectrum has no finite signal"));
    }
    let mut canonical = Vec::<Vec<f64>>::with_capacity(routing);
    let mut start = 0;
    while start < order.len() && canonical.len() < routing {
        let reference = eigen.eigenvalues[order[start]];
        let mut end = start + 1;
        while end < order.len()
            && same_eigenvalue_cluster(reference, eigen.eigenvalues[order[end]], spectrum_scale)
        {
            end += 1;
        }
        for vector in canonical_cluster_vectors(&basis, &eigen.eigenvectors, &order[start..end])? {
            if canonical.len() == routing {
                break;
            }
            canonical.push(vector);
        }
        start = end;
    }
    if canonical.len() != routing {
        return Err(invalid("V35 PCA retained eigenspace differs"));
    }
    let mut selected = DMatrix::<f64>::zeros(source, routing);
    for (column, vector) in canonical.into_iter().enumerate() {
        for row in 0..source {
            selected[(row, column)] = vector[row];
        }
    }
    selected = orthonormalize_columns(selected)?;
    let mut basis_source_major = Vec::with_capacity(source * routing);
    for row in 0..source {
        for column in 0..routing {
            let rounded = selected[(row, column)] as f32;
            basis_source_major.push(if rounded == 0.0 { 0.0 } else { rounded });
        }
    }
    drop(selected);
    drop(basis);
    validate_decoded_basis(spec.dimensions, &basis_source_major)?;
    let training_descriptor = pca_training_descriptor(spec)?;
    let checksum = projection_checksum(
        V35ProjectionArm::Pca,
        spec.dimensions,
        spec.seed,
        PCA_ALGORITHM,
        Some(&spec.sample_sha256),
        &training_descriptor,
        &basis_source_major,
    );
    Ok(V35Projection {
        dimensions: spec.dimensions,
        arm: V35ProjectionArm::Pca,
        seed: spec.seed,
        algorithm: PCA_ALGORITHM.to_owned(),
        sample_sha256: Some(spec.sample_sha256.clone()),
        training_descriptor,
        basis_source_major,
        checksum,
    })
}

/// Build the equal-byte orthonormalized restricted-Hadamard control.
pub fn build_v35_srht(dimensions: V35Dimensions, seed: u64) -> Result<V35Projection> {
    let (source, routing) = validate_dimensions(dimensions)?;
    let padded = source
        .checked_next_power_of_two()
        .ok_or_else(|| invalid("V35 SRHT padded dimension overflows"))?;

    let mut sign_state = seed ^ SIGN_SEED_DOMAIN;
    let signs = (0..source)
        .map(|_| {
            if splitmix64(&mut sign_state) & 1 == 0 {
                1.0
            } else {
                -1.0
            }
        })
        .collect::<Vec<f64>>();
    let mut rows = (0..padded).collect::<Vec<_>>();
    let mut row_state = seed ^ ROW_SEED_DOMAIN;
    for index in 0..rows.len() {
        let remaining = rows.len() - index;
        let offset = usize::try_from(splitmix64(&mut row_state) % remaining as u64)
            .map_err(|_| invalid("V35 SRHT row selection overflows"))?;
        rows.swap(index, index + offset);
    }

    let mut accepted = Vec::<Vec<f64>>::with_capacity(routing);
    let mut skipped_rows = Vec::new();
    for hadamard_row in rows {
        let mut candidate = (0..source)
            .map(|dimension| {
                let sign = if (hadamard_row & dimension).count_ones() & 1 == 0 {
                    1.0
                } else {
                    -1.0
                };
                signs[dimension] * sign
            })
            .collect::<Vec<_>>();
        for _ in 0..2 {
            for basis_row in &accepted {
                let dot = candidate
                    .iter()
                    .zip(basis_row)
                    .fold(0.0_f64, |sum, (left, right)| left.mul_add(*right, sum));
                for (value, basis) in candidate.iter_mut().zip(basis_row) {
                    *value = (-dot).mul_add(*basis, *value);
                }
            }
        }
        let norm_squared = candidate
            .iter()
            .fold(0.0_f64, |sum, value| value.mul_add(*value, sum));
        if !norm_squared.is_finite() || norm_squared.sqrt() <= DEPENDENCE_THRESHOLD {
            skipped_rows.push(
                u64::try_from(hadamard_row)
                    .map_err(|_| invalid("V35 SRHT skipped row overflows"))?,
            );
            continue;
        }
        let inverse = norm_squared.sqrt().recip();
        candidate.iter_mut().for_each(|value| *value *= inverse);
        canonicalize_row(&mut candidate)?;
        accepted.push(candidate);
        if accepted.len() == routing {
            break;
        }
    }
    if accepted.len() != routing {
        return Err(invalid("V35 SRHT cannot produce the registered rank"));
    }

    let mut basis_source_major = Vec::with_capacity(source * routing);
    for dimension in 0..source {
        for row in &accepted {
            let rounded = row[dimension] as f32;
            basis_source_major.push(if rounded == 0.0 { 0.0 } else { rounded });
        }
    }
    drop(accepted);
    validate_decoded_basis(dimensions, &basis_source_major)?;
    skipped_rows.sort_unstable();
    let training_descriptor = canonical_struct(&SrhtTrainingDescriptor {
        algorithm: SRHT_ALGORITHM.to_owned(),
        dependence_threshold: "1e-12".to_owned(),
        modified_gram_schmidt_passes: 2,
        padded_dimension: u64::try_from(padded)
            .map_err(|_| invalid("V35 SRHT padded dimension overflows"))?,
        row_permutation: "fisher-yates-splitmix64-v1".to_owned(),
        skipped_rows,
    })?;
    let checksum = projection_checksum(
        V35ProjectionArm::Srht,
        dimensions,
        seed,
        SRHT_ALGORITHM,
        None,
        &training_descriptor,
        &basis_source_major,
    );
    Ok(V35Projection {
        dimensions,
        arm: V35ProjectionArm::Srht,
        seed,
        algorithm: SRHT_ALGORITHM.to_owned(),
        sample_sha256: None,
        training_descriptor,
        basis_source_major,
        checksum,
    })
}

fn canonical_manifest(manifest: &V35ProjectionManifest) -> Result<String> {
    canonical_struct(manifest)
}

fn canonical_struct<T: Serialize>(value: &T) -> Result<String> {
    let value = serde_json::to_value(value)
        .map_err(|_| invalid("V35 projection manifest cannot be serialized"))?;
    let object = value
        .as_object()
        .ok_or_else(|| invalid("V35 projection manifest differs"))?;
    let sorted = object
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect::<BTreeMap<_, _>>();
    serde_json::to_string(&sorted)
        .map_err(|_| invalid("V35 projection manifest cannot be serialized"))
}

fn pca_training_descriptor(spec: &V35ProjectionTrainingSpec) -> Result<String> {
    canonical_struct(&PcaTrainingDescriptor {
        algorithm: PCA_ALGORITHM.to_owned(),
        arithmetic: "increasing-ordinal-source-major-f64-fma-v1".to_owned(),
        eigen_cluster_relative_tolerance: "1e-10".to_owned(),
        maximum_block_rows: u64::try_from(spec.maximum_block_rows)
            .map_err(|_| invalid("V35 PCA block limit overflows"))?,
        oversampling: 16,
        sample_passes: 4,
        sample_role: spec.sample_role.clone(),
        sample_rows: spec.sample_rows,
        selection_policy: spec.selection_policy.clone(),
        source_archive_sha256: spec.source_archive_sha256.clone(),
        subspace_iterations: 2,
    })
}

fn validate_pca_training_descriptor(text: &str) -> Result<()> {
    let descriptor: PcaTrainingDescriptor =
        serde_json::from_str(text).map_err(|_| invalid("V35 PCA training descriptor differs"))?;
    if canonical_struct(&descriptor)? != text
        || descriptor.algorithm != PCA_ALGORITHM
        || descriptor.arithmetic != "increasing-ordinal-source-major-f64-fma-v1"
        || descriptor.eigen_cluster_relative_tolerance != "1e-10"
        || descriptor.maximum_block_rows == 0
        || descriptor.maximum_block_rows > 2_048
        || descriptor.oversampling != 16
        || descriptor.sample_passes != 4
        || descriptor.sample_role != "construction-projection-sample"
        || descriptor.sample_rows == 0
        || descriptor.selection_policy != "source-ordinal-hash-bottom-k-v1"
        || !is_sha256(&descriptor.source_archive_sha256)
        || descriptor.subspace_iterations != 2
    {
        return Err(invalid("V35 PCA training descriptor authority differs"));
    }
    Ok(())
}

fn validate_srht_training_descriptor(text: &str, dimensions: V35Dimensions) -> Result<()> {
    let descriptor: SrhtTrainingDescriptor =
        serde_json::from_str(text).map_err(|_| invalid("V35 SRHT training descriptor differs"))?;
    let padded = u64::from(dimensions.source)
        .checked_next_power_of_two()
        .ok_or_else(|| invalid("V35 SRHT padded dimension overflows"))?;
    if canonical_struct(&descriptor)? != text
        || descriptor.algorithm != SRHT_ALGORITHM
        || descriptor.dependence_threshold != "1e-12"
        || descriptor.modified_gram_schmidt_passes != 2
        || descriptor.padded_dimension != padded
        || descriptor.row_permutation != "fisher-yates-splitmix64-v1"
        || descriptor
            .skipped_rows
            .windows(2)
            .any(|pair| pair[0] >= pair[1])
        || descriptor
            .skipped_rows
            .iter()
            .any(|row| *row >= descriptor.padded_dimension)
    {
        return Err(invalid("V35 SRHT training descriptor authority differs"));
    }
    Ok(())
}

fn projection_manifest(projection: &V35Projection, role: &str, uri: &str) -> V35ProjectionManifest {
    V35ProjectionManifest {
        algorithm: projection.algorithm.clone(),
        arm: projection.arm,
        checksum_sha256: projection
            .checksum
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect(),
        dimensions: projection.dimensions,
        sample_sha256: projection.sample_sha256.clone(),
        seed: projection.seed,
        storage_order: "source-major-f32-le".to_owned(),
        training_descriptor: projection.training_descriptor.clone(),
        role: role.to_owned(),
        uri: uri.to_owned(),
    }
}

fn validate_projection_identity(identity: &V35ArtifactIdentity, bytes: &[u8]) -> Result<()> {
    if !matches!(
        identity.role.as_str(),
        "active-projection-basis" | "retiring-projection-basis"
    ) || identity.digest_algorithm != "sha256"
        || identity.uri.is_empty()
        || identity.length != u64::try_from(bytes.len()).unwrap_or(u64::MAX)
        || identity.digest != format!("{:x}", Sha256::digest(bytes))
    {
        return Err(invalid("V35 projection artifact identity differs"));
    }
    Ok(())
}

/// Encode one strict source-major decoded-f32 projection basis as Arrow IPC.
pub fn encode_v35_projection_arrow(
    projection: &V35Projection,
    role: &str,
    uri: &str,
) -> Result<(Vec<u8>, V35ArtifactIdentity)> {
    validate_decoded_basis(projection.dimensions, &projection.basis_source_major)?;
    if !matches!(
        role,
        "active-projection-basis" | "retiring-projection-basis"
    ) || uri.is_empty()
    {
        return Err(invalid("V35 projection artifact identity differs"));
    }
    let (_, routing) = validate_dimensions(projection.dimensions)?;
    let list_size = i32::try_from(routing)
        .map_err(|_| invalid("V35 projection routing dimension overflows"))?;
    let child = Arc::new(Field::new("item", DataType::Float32, false));
    let values = Arc::new(Float32Array::new(
        projection.basis_source_major.clone().into(),
        None,
    ));
    let coefficients = Arc::new(FixedSizeListArray::try_new(
        child.clone(),
        list_size,
        values,
        None,
    )?);
    let mut metadata = HashMap::new();
    metadata.insert(
        PROJECTION_MANIFEST_KEY.to_owned(),
        canonical_manifest(&projection_manifest(projection, role, uri))?,
    );
    let schema = Arc::new(Schema::new_with_metadata(
        vec![Field::new(
            "coefficients",
            DataType::FixedSizeList(child, list_size),
            false,
        )],
        metadata,
    ));
    let batch = RecordBatch::try_new(schema.clone(), vec![coefficients])?;
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
            .map_err(|_| invalid("V35 projection artifact length overflows"))?,
        role: role.to_owned(),
        uri: uri.to_owned(),
    };
    Ok((bytes, identity))
}

/// Authenticate and decode one strict source-major f32 projection basis.
pub fn decode_v35_projection_arrow(
    bytes: &[u8],
    identity: &V35ArtifactIdentity,
    limits: V35ProjectionLimits,
) -> Result<V35Projection> {
    validate_projection_identity(identity, bytes)?;
    if u64::try_from(bytes.len()).unwrap_or(u64::MAX) > limits.maximum_encoded_bytes {
        return Err(invalid(
            "V35 projection encoded allocation exceeds admission",
        ));
    }
    let mut reader = FileReader::try_new(Cursor::new(bytes), None)?;
    let schema = reader.schema();
    if schema.fields().len() != 1 || schema.metadata().len() != 1 {
        return Err(invalid("V35 projection Arrow schema differs"));
    }
    let manifest_text = schema
        .metadata()
        .get(PROJECTION_MANIFEST_KEY)
        .ok_or_else(|| invalid("V35 projection manifest is missing"))?;
    let manifest: V35ProjectionManifest = serde_json::from_str(manifest_text)
        .map_err(|_| invalid("V35 projection manifest differs"))?;
    let arm_authority_valid = match manifest.arm {
        V35ProjectionArm::Pca => {
            manifest.algorithm == PCA_ALGORITHM
                && manifest.sample_sha256.as_deref().is_some_and(is_sha256)
                && validate_pca_training_descriptor(&manifest.training_descriptor).is_ok()
        }
        V35ProjectionArm::Srht => {
            manifest.algorithm == SRHT_ALGORITHM
                && manifest.sample_sha256.is_none()
                && validate_srht_training_descriptor(
                    &manifest.training_descriptor,
                    manifest.dimensions,
                )
                .is_ok()
        }
    };
    if canonical_manifest(&manifest)? != *manifest_text
        || manifest.storage_order != "source-major-f32-le"
        || !arm_authority_valid
        || manifest.role != identity.role
        || manifest.uri != identity.uri
    {
        return Err(invalid("V35 projection manifest authority differs"));
    }
    let (source, routing) = validate_dimensions(manifest.dimensions)?;
    let numeric_bytes = u64::try_from(
        source
            .checked_mul(routing)
            .and_then(|value| value.checked_mul(std::mem::size_of::<f32>()))
            .ok_or_else(|| invalid("V35 projection decoded allocation overflows"))?,
    )
    .map_err(|_| invalid("V35 projection decoded allocation overflows"))?;
    let peak_bytes = u64::try_from(bytes.len())
        .ok()
        .and_then(|encoded| {
            numeric_bytes
                .checked_mul(2)
                .and_then(|decoded| encoded.checked_add(decoded))
        })
        .ok_or_else(|| invalid("V35 projection peak allocation overflows"))?;
    if numeric_bytes > limits.maximum_decoded_numeric_bytes
        || peak_bytes > limits.maximum_peak_codec_bytes
        || reader.num_batches() != 1
    {
        return Err(invalid(
            "V35 projection decoded allocation exceeds admission",
        ));
    }
    let list_size = i32::try_from(routing)
        .map_err(|_| invalid("V35 projection routing dimension overflows"))?;
    let expected_type = DataType::FixedSizeList(
        Arc::new(Field::new("item", DataType::Float32, false)),
        list_size,
    );
    let field = &schema.fields()[0];
    if field.name() != "coefficients" || field.is_nullable() || field.data_type() != &expected_type
    {
        return Err(invalid("V35 projection Arrow schema differs"));
    }
    let batch = reader
        .next()
        .ok_or_else(|| invalid("V35 projection Arrow batch is missing"))??;
    if batch.num_rows() != source || batch.num_columns() != 1 {
        return Err(invalid("V35 projection Arrow batches differ"));
    }
    let list = batch
        .column(0)
        .as_any()
        .downcast_ref::<FixedSizeListArray>()
        .ok_or_else(|| invalid("V35 projection Arrow column differs"))?;
    if list.null_count() != 0 {
        return Err(invalid("V35 projection Arrow nullability differs"));
    }
    let values = list
        .values()
        .as_any()
        .downcast_ref::<Float32Array>()
        .ok_or_else(|| invalid("V35 projection Arrow values differ"))?;
    if values.null_count() != 0 || values.len() != source * routing {
        return Err(invalid("V35 projection Arrow values differ"));
    }
    let basis_source_major = values.values().to_vec();
    validate_decoded_basis(manifest.dimensions, &basis_source_major)?;
    let checksum = projection_checksum(
        manifest.arm,
        manifest.dimensions,
        manifest.seed,
        &manifest.algorithm,
        manifest.sample_sha256.as_deref(),
        &manifest.training_descriptor,
        &basis_source_major,
    );
    let checksum_hex: String = checksum.iter().map(|byte| format!("{byte:02x}")).collect();
    if checksum_hex != manifest.checksum_sha256 {
        return Err(invalid("V35 projection logical checksum differs"));
    }
    Ok(V35Projection {
        dimensions: manifest.dimensions,
        arm: manifest.arm,
        seed: manifest.seed,
        algorithm: manifest.algorithm,
        sample_sha256: manifest.sample_sha256,
        training_descriptor: manifest.training_descriptor,
        basis_source_major,
        checksum,
    })
}

fn validate_query<'a>(projection: &V35Projection, query: &'a [f32]) -> Result<&'a [f32]> {
    if query.len() != usize::try_from(projection.dimensions.source).unwrap_or(usize::MAX)
        || query.iter().any(|value| !value.is_finite())
    {
        return Err(invalid("V35 projection query differs"));
    }
    Ok(query)
}

fn complement_energy(query: &[f32], coordinates: &[f64]) -> f64 {
    let query_energy = query.iter().fold(0.0_f64, |sum, value| {
        f64::from(*value).mul_add(f64::from(*value), sum)
    });
    let projected_energy = coordinates
        .iter()
        .fold(0.0_f64, |sum, value| value.mul_add(*value, sum));
    let energy = (query_energy - projected_energy).max(0.0);
    if energy == 0.0 { 0.0 } else { energy }
}

fn source_query_digest(projection: &V35Projection, query: &[f32]) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(b"borsuk-v35-source-query-v1\n");
    digest.update(projection.dimensions.source.to_le_bytes());
    digest.update(projection.dimensions.routing.to_le_bytes());
    digest.update(projection.checksum);
    for value in query {
        digest.update(value.to_bits().to_le_bytes());
    }
    digest.finalize().into()
}

/// Project with the increasing-source-dimension scalar f64 authority.
pub fn project_v35_query_scalar(
    projection: &V35Projection,
    query: &[f32],
) -> Result<V35ProjectedQuery> {
    let query = validate_query(projection, query)?;
    let routing = usize::from(projection.dimensions.routing);
    let mut coordinates = vec![0.0_f64; routing];
    for (dimension, value) in query.iter().enumerate() {
        let start = dimension * routing;
        for (output, coefficient) in coordinates
            .iter_mut()
            .zip(&projection.basis_source_major[start..start + routing])
        {
            *output = f64::from(*coefficient).mul_add(f64::from(*value), *output);
        }
    }
    coordinates.iter_mut().for_each(|value| {
        if *value == 0.0 {
            *value = 0.0;
        }
    });
    Ok(V35ProjectedQuery {
        complement_energy: complement_energy(query, &coordinates),
        coordinates,
        backend: V35ProjectionBackend::ScalarControl,
        source_query: query.to_vec(),
        source_digest: source_query_digest(projection, query),
        projection_checksum: projection.checksum,
    })
}

/// Project with four independent f64 output lanes and ordered source updates.
pub fn project_v35_query_simd(
    projection: &V35Projection,
    query: &[f32],
) -> Result<V35ProjectedQuery> {
    let query = validate_query(projection, query)?;
    let (coordinates, backend) = project_v35_source_row_simd(projection, query)?;
    Ok(V35ProjectedQuery {
        complement_energy: complement_energy(query, &coordinates),
        coordinates,
        backend,
        source_query: query.to_vec(),
        source_digest: source_query_digest(projection, query),
        projection_checksum: projection.checksum,
    })
}

pub(crate) fn project_v35_source_row_simd(
    projection: &V35Projection,
    source: &[f32],
) -> Result<(Vec<f64>, V35ProjectionBackend)> {
    let query = validate_query(projection, source)?;
    let routing = usize::from(projection.dimensions.routing);
    let mut coordinates = vec![0.0_f64; routing];
    let kernel =
        FusedProjection4::detect().map_err(|_| invalid("V35 fused SIMD backend is unavailable"))?;
    for output in (0..routing).step_by(4) {
        let block = kernel
            .project_source_major(&projection.basis_source_major, query, routing, output)
            .ok_or_else(|| invalid("V35 fused SIMD projection layout differs"))?;
        coordinates[output..output + 4].copy_from_slice(&block);
    }
    let backend = match kernel.backend() {
        FmaBackend::Aarch64NeonFma => V35ProjectionBackend::Aarch64NeonFma,
        FmaBackend::X86AvxFma => V35ProjectionBackend::X86AvxFma,
    };
    coordinates.iter_mut().for_each(|value| {
        if *value == 0.0 {
            *value = 0.0;
        }
    });
    Ok((coordinates, backend))
}
