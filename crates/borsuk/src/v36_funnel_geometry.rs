//! Deterministic posting geometry for the V36 qualification funnel.

use crate::{
    BorsukError, Result, V35Dimensions,
    v35_projection::{V35Projection, V35ProjectionBackend, build_v35_srht},
};
use borsuk_fma::{FmaBackend, FusedProjection4};
use nalgebra::{DMatrix, SymmetricEigen};
use sha2::{Digest, Sha256};

const CENTERED_EIGEN_MAX_ITERATIONS_PER_DIMENSION: usize = 30;
const CENTERED_AXIS_DEPENDENCE_THRESHOLD: f64 = 1e-12;
const CENTERED_EIGEN_CLUSTER_RELATIVE_TOLERANCE: f64 = 1e-10;
const CENTERED_TRAINING_WORKSPACE_LIMIT_BYTES: u64 = 64 * 1_048_576;
const CENTERED_MATRIX_WORKSPACE_COPIES: u64 = 7;

#[derive(Debug, Clone, PartialEq)]
struct V36CenteredCovarianceAnalysis {
    mean: Vec<f64>,
    covariance: Vec<f64>,
    eigenvalues: Vec<f64>,
    basis_source_major: Vec<f64>,
    retained_energy_ppm: Vec<u32>,
    max_eigenpair_relative_residual: f64,
    reconstruction_relative_error: f64,
}

/// One bounded source-major block consumed by the V36 centered trainer.
pub type V36CenteredProjectionBlockVisitor<'a> = dyn FnMut(&[u64], &[f32]) -> Result<()> + 'a;

/// Authenticated input role scanned by the V36 centered trainer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum V36CenteredSampleRole {
    /// Complete corpus population used only for the binary64 mean.
    CorpusMean,
    /// Query-independent geometry reservoir used for centered covariance.
    GeometryReservoir,
}

/// Restartable bounded source for the two distinct centered-training roles.
pub trait V36CenteredProjectionSource {
    /// Scan one role once in strictly increasing source-ordinal order.
    fn scan(
        &mut self,
        role: V36CenteredSampleRole,
        visitor: &mut V36CenteredProjectionBlockVisitor<'_>,
    ) -> Result<()>;
}

/// Complete authority for one bounded centered principal-subspace training.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct V36CenteredProjectionTrainingSpec {
    /// Source vector dimensions.
    pub source_dimensions: usize,
    /// Leading eigenvectors retained in source-major order.
    pub retained_dimensions: usize,
    /// Complete corpus population used for the mean.
    pub corpus_rows: u64,
    /// Domain-separated SHA-256 of corpus ordinals and f32 row bytes.
    pub corpus_sha256: String,
    /// Complete geometry-reservoir population.
    pub reservoir_rows: u64,
    /// Domain-separated SHA-256 of reservoir ordinals and f32 row bytes.
    pub reservoir_sha256: String,
    /// Hard callback block-row cap.
    pub maximum_block_rows: usize,
    /// Strictly increasing full-spectrum prefix dimensions to report.
    pub energy_dimensions: Vec<usize>,
}

/// Deterministic centered principal-subspace training result.
#[derive(Debug, Clone, PartialEq)]
pub struct V36CenteredProjection {
    retained_dimensions: usize,
    mean: Vec<f64>,
    mean_sha256: String,
    covariance_sha256: String,
    eigenvalues: Vec<f64>,
    basis_source_major: Vec<f32>,
    retained_energy_ppm: Vec<u32>,
    max_eigenpair_relative_residual: f64,
    reconstruction_relative_error: f64,
}

impl V36CenteredProjection {
    /// Explicit retained routing width bound by the trained basis.
    pub fn retained_dimensions(&self) -> usize {
        self.retained_dimensions
    }

    /// Corpus-only binary64 mean used to form centered covariance.
    pub fn mean(&self) -> &[f64] {
        &self.mean
    }

    /// Domain-separated digest of the binary64 corpus mean.
    pub fn mean_sha256(&self) -> &str {
        &self.mean_sha256
    }

    /// Domain-separated digest of the source-major binary64 covariance.
    pub fn covariance_sha256(&self) -> &str {
        &self.covariance_sha256
    }

    /// Complete descending, nonnegative centered eigenspectrum.
    pub fn eigenvalues(&self) -> &[f64] {
        &self.eigenvalues
    }

    /// Retained eigenvectors rounded to source-major f32 coefficients.
    pub fn basis_source_major(&self) -> &[f32] {
        &self.basis_source_major
    }

    /// Full-spectrum retained-energy fractions in registered dimension order.
    pub fn retained_energy_ppm(&self) -> &[u32] {
        &self.retained_energy_ppm
    }

    /// Greatest admitted eigenpair residual relative to covariance Frobenius norm.
    pub fn max_eigenpair_relative_residual(&self) -> f64 {
        self.max_eigenpair_relative_residual
    }

    /// Full covariance reconstruction error relative to covariance Frobenius norm.
    pub fn reconstruction_relative_error(&self) -> f64 {
        self.reconstruction_relative_error
    }
}

/// One centered projection result with explicit arithmetic-backend evidence.
#[derive(Debug, Clone, PartialEq)]
pub struct V36CenteredProjectedRow {
    coordinates: Vec<f64>,
    backend: V35ProjectionBackend,
}

impl V36CenteredProjectedRow {
    /// Centered routing coordinates in retained-eigenvector order.
    pub fn coordinates(&self) -> &[f64] {
        &self.coordinates
    }

    /// Arithmetic backend used for this projection.
    pub fn backend(&self) -> V35ProjectionBackend {
        self.backend
    }
}

fn validate_v36_centered_source(
    projection: &V36CenteredProjection,
    source: &[f32],
) -> Result<(Vec<f32>, usize)> {
    let dimensions = projection.mean.len();
    let routing = projection.retained_dimensions;
    if dimensions == 0
        || routing == 0
        || routing > dimensions
        || projection.eigenvalues.len() != dimensions
        || source.len() != dimensions
        || dimensions
            .checked_mul(routing)
            .is_none_or(|coefficients| projection.basis_source_major.len() != coefficients)
        || source.iter().any(|value| !value.is_finite())
        || projection.mean.iter().any(|value| !value.is_finite())
        || projection
            .basis_source_major
            .iter()
            .any(|value| !value.is_finite())
    {
        return Err(invalid("V36 centered projection source differs"));
    }
    let centered = source
        .iter()
        .zip(&projection.mean)
        .map(|(value, mean)| {
            let centered = (f64::from(*value) - mean) as f32;
            centered
                .is_finite()
                .then_some(if centered == 0.0 { 0.0 } else { centered })
                .ok_or_else(|| invalid("V36 centered projection source is nonfinite"))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok((centered, routing))
}

/// Project one row with the ordered scalar-fused authority.
pub fn project_v36_centered_row_scalar(
    projection: &V36CenteredProjection,
    source: &[f32],
) -> Result<V36CenteredProjectedRow> {
    let (centered, routing) = validate_v36_centered_source(projection, source)?;
    let mut coordinates = vec![0.0_f64; routing];
    for (dimension, value) in centered.iter().enumerate() {
        let start = dimension * routing;
        for (coordinate, coefficient) in coordinates
            .iter_mut()
            .zip(&projection.basis_source_major[start..start + routing])
        {
            *coordinate = f64::from(*coefficient).mul_add(f64::from(*value), *coordinate);
        }
    }
    coordinates.iter_mut().for_each(|value| {
        if *value == 0.0 {
            *value = 0.0;
        }
    });
    Ok(V36CenteredProjectedRow {
        coordinates,
        backend: V35ProjectionBackend::ScalarControl,
    })
}

/// Project one row with the registered four-lane fused serving kernel.
pub fn project_v36_centered_row_simd(
    projection: &V36CenteredProjection,
    source: &[f32],
) -> Result<V36CenteredProjectedRow> {
    let (centered, routing) = validate_v36_centered_source(projection, source)?;
    if !routing.is_multiple_of(4) {
        return Err(invalid("V36 centered projection SIMD width differs"));
    }
    let kernel =
        FusedProjection4::detect().map_err(|_| invalid("V36 fused SIMD backend is unavailable"))?;
    let mut coordinates = vec![0.0_f64; routing];
    for output in (0..routing).step_by(4) {
        let block = kernel
            .project_source_major(&projection.basis_source_major, &centered, routing, output)
            .ok_or_else(|| invalid("V36 fused SIMD projection layout differs"))?;
        coordinates[output..output + 4].copy_from_slice(&block);
    }
    coordinates.iter_mut().for_each(|value| {
        if *value == 0.0 {
            *value = 0.0;
        }
    });
    let backend = match kernel.backend() {
        FmaBackend::Aarch64NeonFma => V35ProjectionBackend::Aarch64NeonFma,
        FmaBackend::X86AvxFma => V35ProjectionBackend::X86AvxFma,
    };
    Ok(V36CenteredProjectedRow {
        coordinates,
        backend,
    })
}

fn invalid(message: &str) -> BorsukError {
    BorsukError::InvalidStorage(message.to_owned())
}

fn valid_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn v36_centered_f64_digest(domain: &[u8], dimensions: usize, values: &[f64]) -> Result<String> {
    let mut digest = Sha256::new();
    digest.update(domain);
    digest.update(
        u64::try_from(dimensions)
            .map_err(|_| invalid("V36 centered dimensions overflow"))?
            .to_le_bytes(),
    );
    for value in values {
        digest.update(value.to_bits().to_le_bytes());
    }
    Ok(format!("{:x}", digest.finalize()))
}

fn validate_v36_centered_workspace(spec: &V36CenteredProjectionTrainingSpec) -> Result<()> {
    let dimensions = u64::try_from(spec.source_dimensions)
        .map_err(|_| invalid("V36 centered dimensions overflow"))?;
    let retained = u64::try_from(spec.retained_dimensions)
        .map_err(|_| invalid("V36 centered dimensions overflow"))?;
    let block_rows = u64::try_from(spec.maximum_block_rows)
        .map_err(|_| invalid("V36 centered block rows overflow"))?;
    let matrix_bytes = dimensions
        .checked_mul(dimensions)
        .and_then(|entries| entries.checked_mul(8))
        .and_then(|bytes| bytes.checked_mul(CENTERED_MATRIX_WORKSPACE_COPIES))
        .ok_or_else(|| invalid("V36 centered workspace overflows"))?;
    let vector_bytes = dimensions
        .checked_mul(8 * 4)
        .and_then(|bytes| bytes.checked_add(dimensions.checked_mul(retained)?.checked_mul(8)?))
        .and_then(|bytes| bytes.checked_add(dimensions.checked_mul(block_rows)?.checked_mul(4)?))
        .ok_or_else(|| invalid("V36 centered workspace overflows"))?;
    if matrix_bytes
        .checked_add(vector_bytes)
        .is_none_or(|bytes| bytes > CENTERED_TRAINING_WORKSPACE_LIMIT_BYTES)
    {
        return Err(invalid("V36 centered workspace exceeds admission"));
    }
    Ok(())
}

fn scan_v36_centered_role(
    source: &mut dyn V36CenteredProjectionSource,
    role: V36CenteredSampleRole,
    source_dimensions: usize,
    expected_rows: u64,
    expected_sha256: &str,
    maximum_block_rows: usize,
    consume: &mut dyn FnMut(&[f32]) -> Result<()>,
) -> Result<()> {
    let domain = match role {
        V36CenteredSampleRole::CorpusMean => b"borsuk-v36-centered-corpus-v1\n".as_slice(),
        V36CenteredSampleRole::GeometryReservoir => {
            b"borsuk-v36-centered-reservoir-v1\n".as_slice()
        }
    };
    let mut digest = Sha256::new();
    digest.update(domain);
    let mut seen = 0_u64;
    let mut previous_ordinal = None;
    source.scan(role, &mut |ordinals, values| {
        if ordinals.is_empty()
            || ordinals.len() > maximum_block_rows
            || values.len() != ordinals.len().saturating_mul(source_dimensions)
            || seen
                .checked_add(u64::try_from(ordinals.len()).unwrap_or(u64::MAX))
                .is_none_or(|end| end > expected_rows)
            || previous_ordinal
                .is_some_and(|previous| ordinals.first().is_some_and(|first| *first <= previous))
            || ordinals.windows(2).any(|pair| pair[0] >= pair[1])
            || values.iter().any(|value| !value.is_finite())
        {
            return Err(invalid("V36 centered sample block differs"));
        }
        for (row_index, ordinal) in ordinals.iter().enumerate() {
            let row = &values[row_index * source_dimensions..(row_index + 1) * source_dimensions];
            digest.update(ordinal.to_le_bytes());
            for value in row {
                digest.update(value.to_bits().to_le_bytes());
            }
            consume(row)?;
            previous_ordinal = Some(*ordinal);
        }
        seen = seen
            .checked_add(
                u64::try_from(ordinals.len())
                    .map_err(|_| invalid("V36 centered sample row count overflows"))?,
            )
            .ok_or_else(|| invalid("V36 centered sample row count overflows"))?;
        Ok(())
    })?;
    if seen != expected_rows || format!("{:x}", digest.finalize()) != expected_sha256 {
        return Err(invalid("V36 centered sample digest differs"));
    }
    Ok(())
}

fn canonicalize_v36_eigenspace(
    eigenvectors: &DMatrix<f64>,
    cluster: &[usize],
) -> Result<Vec<Vec<f64>>> {
    let dimensions = eigenvectors.nrows();
    let mut accepted = Vec::<Vec<f64>>::with_capacity(cluster.len());
    for axis in 0..dimensions {
        let mut candidate = (0..dimensions)
            .map(|row| {
                cluster.iter().fold(0.0_f64, |sum, column| {
                    eigenvectors[(row, *column)].mul_add(eigenvectors[(axis, *column)], sum)
                })
            })
            .collect::<Vec<_>>();
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
        if norm.is_finite() && norm > CENTERED_AXIS_DEPENDENCE_THRESHOLD {
            let inverse = norm.recip();
            candidate.iter_mut().for_each(|value| *value *= inverse);
            let sign_axis = candidate
                .iter()
                .enumerate()
                .max_by(|left, right| {
                    left.1
                        .abs()
                        .total_cmp(&right.1.abs())
                        .then_with(|| right.0.cmp(&left.0))
                })
                .map(|(ordinal, _)| ordinal)
                .ok_or_else(|| invalid("V36 centered eigenvector is empty"))?;
            if candidate[sign_axis].is_sign_negative() {
                candidate.iter_mut().for_each(|value| *value = -*value);
            }
            candidate
                .iter_mut()
                .filter(|value| **value == 0.0)
                .for_each(|value| *value = 0.0);
            accepted.push(candidate);
            if accepted.len() == cluster.len() {
                return Ok(accepted);
            }
        }
    }
    Err(invalid("V36 centered eigenspace canonicalization failed"))
}

fn analyze_v36_centered_covariance_matrix(
    mean: Vec<f64>,
    covariance_values: Vec<f64>,
    source_dimensions: usize,
    retained_dimensions: usize,
    energy_dimensions: &[usize],
) -> Result<V36CenteredCovarianceAnalysis> {
    if source_dimensions == 0
        || retained_dimensions == 0
        || retained_dimensions > source_dimensions
        || mean.len() != source_dimensions
        || mean.iter().any(|value| !value.is_finite())
        || covariance_values.len()
            != source_dimensions
                .checked_mul(source_dimensions)
                .ok_or_else(|| invalid("V36 centered covariance allocation overflows"))?
        || covariance_values.iter().any(|value| !value.is_finite())
        || energy_dimensions.is_empty()
        || energy_dimensions.windows(2).any(|pair| pair[0] >= pair[1])
        || energy_dimensions
            .iter()
            .any(|dimension| *dimension == 0 || *dimension > source_dimensions)
    {
        return Err(invalid("V36 centered covariance authority differs"));
    }
    let covariance =
        DMatrix::<f64>::from_column_slice(source_dimensions, source_dimensions, &covariance_values);
    for left in 0..source_dimensions {
        for right in 0..left {
            if covariance[(left, right)].to_bits() != covariance[(right, left)].to_bits() {
                return Err(invalid("V36 centered covariance is not exactly symmetric"));
            }
        }
    }
    let covariance_frobenius = covariance.norm();
    if !covariance_frobenius.is_finite() || covariance_frobenius == 0.0 {
        return Err(invalid("V36 centered covariance has no finite signal"));
    }
    let max_iterations = source_dimensions
        .checked_mul(CENTERED_EIGEN_MAX_ITERATIONS_PER_DIMENSION)
        .ok_or_else(|| invalid("V36 centered eigensolver iteration cap overflows"))?;
    let eigen = SymmetricEigen::try_new(covariance.clone(), f64::EPSILON, max_iterations)
        .ok_or_else(|| invalid("V36 centered eigensolver did not converge"))?;
    if eigen.eigenvalues.iter().any(|value| !value.is_finite())
        || eigen.eigenvectors.iter().any(|value| !value.is_finite())
    {
        return Err(invalid("V36 centered eigensystem is nonfinite"));
    }

    let trace = covariance.trace();
    let negative_tolerance = (trace.abs() * 1e-12).max(1e-15);
    let mut eigenvalues = eigen.eigenvalues.as_slice().to_vec();
    for value in &mut eigenvalues {
        if *value < -negative_tolerance {
            return Err(invalid("V36 centered covariance has a negative eigenvalue"));
        }
        if *value < 0.0 {
            *value = 0.0;
        }
    }
    let mut order = (0..source_dimensions).collect::<Vec<_>>();
    order.sort_by(|left, right| {
        eigenvalues[*right]
            .total_cmp(&eigenvalues[*left])
            .then_with(|| left.cmp(right))
    });
    let spectrum_scale = eigenvalues
        .iter()
        .fold(0.0_f64, |scale, value| scale.max(value.abs()));

    let mut ordered_values = Vec::with_capacity(source_dimensions);
    let mut ordered_vectors = Vec::<Vec<f64>>::with_capacity(source_dimensions);
    let mut start = 0;
    while start < order.len() {
        let reference = eigenvalues[order[start]];
        let mut end = start + 1;
        while end < order.len()
            && (eigenvalues[order[end]] - reference).abs()
                <= CENTERED_EIGEN_CLUSTER_RELATIVE_TOLERANCE * spectrum_scale
        {
            end += 1;
        }
        let cluster = &order[start..end];
        let cluster_value = cluster
            .iter()
            .fold(0.0_f64, |sum, ordinal| sum + eigenvalues[*ordinal])
            / cluster.len() as f64;
        for vector in canonicalize_v36_eigenspace(&eigen.eigenvectors, cluster)? {
            ordered_values.push(cluster_value);
            ordered_vectors.push(vector);
        }
        start = end;
    }

    let mut max_eigenpair_relative_residual = 0.0_f64;
    for (value, vector) in ordered_values.iter().zip(&ordered_vectors) {
        let mut residual_squared = 0.0_f64;
        for row in 0..source_dimensions {
            let product = (0..source_dimensions).fold(0.0_f64, |sum, column| {
                covariance[(row, column)].mul_add(vector[column], sum)
            });
            let residual = product - *value * vector[row];
            residual_squared = residual.mul_add(residual, residual_squared);
        }
        max_eigenpair_relative_residual =
            max_eigenpair_relative_residual.max(residual_squared.sqrt() / covariance_frobenius);
    }

    let mut reconstruction_error_squared = 0.0_f64;
    for row in 0..source_dimensions {
        for column in 0..source_dimensions {
            let reconstructed = ordered_values
                .iter()
                .zip(&ordered_vectors)
                .fold(0.0_f64, |sum, (value, vector)| {
                    (value * vector[row]).mul_add(vector[column], sum)
                });
            let error = covariance[(row, column)] - reconstructed;
            reconstruction_error_squared = error.mul_add(error, reconstruction_error_squared);
        }
    }
    let reconstruction_relative_error = reconstruction_error_squared.sqrt() / covariance_frobenius;
    if max_eigenpair_relative_residual > 1e-10 || reconstruction_relative_error > 1e-10 {
        return Err(invalid(
            "V36 centered eigensystem exceeds numerical admission",
        ));
    }

    let total_energy = ordered_values.iter().sum::<f64>();
    if !total_energy.is_finite() || total_energy <= 0.0 {
        return Err(invalid("V36 centered spectrum has no finite energy"));
    }
    let retained_energy_ppm = energy_dimensions
        .iter()
        .map(|dimension| {
            let retained = ordered_values[..*dimension].iter().sum::<f64>();
            let ppm = (retained / total_energy * 1_000_000.0).round();
            if !ppm.is_finite() || !(0.0..=1_000_000.0).contains(&ppm) {
                return Err(invalid("V36 centered retained energy differs"));
            }
            Ok(ppm as u32)
        })
        .collect::<Result<Vec<_>>>()?;
    let basis_source_major = (0..source_dimensions)
        .flat_map(|row| {
            ordered_vectors[..retained_dimensions]
                .iter()
                .map(move |vector| vector[row])
        })
        .collect::<Vec<_>>();
    Ok(V36CenteredCovarianceAnalysis {
        mean,
        covariance: covariance.as_slice().to_vec(),
        eigenvalues: ordered_values,
        basis_source_major,
        retained_energy_ppm,
        max_eigenpair_relative_residual,
        reconstruction_relative_error,
    })
}

/// Train a deterministic centered principal subspace with two authenticated,
/// strictly ordered scans and O(source_dimensions squared) resident memory.
pub fn train_v36_centered_subspace(
    spec: &V36CenteredProjectionTrainingSpec,
    source: &mut dyn V36CenteredProjectionSource,
) -> Result<V36CenteredProjection> {
    if spec.source_dimensions == 0
        || spec.retained_dimensions == 0
        || spec.retained_dimensions > spec.source_dimensions
        || spec.corpus_rows == 0
        || spec.reservoir_rows == 0
        || !valid_sha256(&spec.corpus_sha256)
        || !valid_sha256(&spec.reservoir_sha256)
        || spec.maximum_block_rows == 0
        || spec.maximum_block_rows > 2_048
        || spec.energy_dimensions.is_empty()
        || spec
            .energy_dimensions
            .windows(2)
            .any(|pair| pair[0] >= pair[1])
        || spec
            .energy_dimensions
            .iter()
            .any(|dimension| *dimension == 0 || *dimension > spec.source_dimensions)
    {
        return Err(invalid("V36 centered training authority differs"));
    }
    validate_v36_centered_workspace(spec)?;
    let matrix_entries = spec
        .source_dimensions
        .checked_mul(spec.source_dimensions)
        .ok_or_else(|| invalid("V36 centered covariance allocation overflows"))?;
    let mut mean = vec![0.0_f64; spec.source_dimensions];
    scan_v36_centered_role(
        source,
        V36CenteredSampleRole::CorpusMean,
        spec.source_dimensions,
        spec.corpus_rows,
        &spec.corpus_sha256,
        spec.maximum_block_rows,
        &mut |row| {
            for (sum, value) in mean.iter_mut().zip(row) {
                *sum += f64::from(*value);
            }
            Ok(())
        },
    )?;
    let corpus_divisor = spec.corpus_rows as f64;
    mean.iter_mut().for_each(|value| *value /= corpus_divisor);
    if mean.iter().any(|value| !value.is_finite()) {
        return Err(invalid("V36 centered corpus mean is nonfinite"));
    }

    let mut covariance = vec![0.0_f64; matrix_entries];
    let mut centered = vec![0.0_f64; spec.source_dimensions];
    scan_v36_centered_role(
        source,
        V36CenteredSampleRole::GeometryReservoir,
        spec.source_dimensions,
        spec.reservoir_rows,
        &spec.reservoir_sha256,
        spec.maximum_block_rows,
        &mut |row| {
            for dimension in 0..spec.source_dimensions {
                centered[dimension] = f64::from(row[dimension]) - mean[dimension];
            }
            for left in 0..spec.source_dimensions {
                for right in 0..=left {
                    let index = right * spec.source_dimensions + left;
                    covariance[index] = centered[left].mul_add(centered[right], covariance[index]);
                }
            }
            Ok(())
        },
    )?;
    let reservoir_divisor = spec.reservoir_rows as f64;
    for left in 0..spec.source_dimensions {
        for right in 0..=left {
            let lower = right * spec.source_dimensions + left;
            let upper = left * spec.source_dimensions + right;
            let value = covariance[lower] / reservoir_divisor;
            covariance[lower] = value;
            covariance[upper] = value;
        }
    }
    let mean_sha256 = v36_centered_f64_digest(
        b"borsuk-v36-centered-mean-v1\n",
        spec.source_dimensions,
        &mean,
    )?;
    let covariance_sha256 = v36_centered_f64_digest(
        b"borsuk-v36-centered-covariance-v1\n",
        spec.source_dimensions,
        &covariance,
    )?;
    let analysis = analyze_v36_centered_covariance_matrix(
        mean,
        covariance,
        spec.source_dimensions,
        spec.retained_dimensions,
        &spec.energy_dimensions,
    )?;
    let basis_source_major = analysis
        .basis_source_major
        .iter()
        .map(|value| {
            let rounded = *value as f32;
            if rounded == 0.0 { 0.0 } else { rounded }
        })
        .collect::<Vec<_>>();
    Ok(V36CenteredProjection {
        retained_dimensions: spec.retained_dimensions,
        mean: analysis.mean,
        mean_sha256,
        covariance_sha256,
        eigenvalues: analysis.eigenvalues,
        basis_source_major,
        retained_energy_ppm: analysis.retained_energy_ppm,
        max_eigenpair_relative_residual: analysis.max_eigenpair_relative_residual,
        reconstruction_relative_error: analysis.reconstruction_relative_error,
    })
}

/// Build the registered 768-to-192 SRHT control with seed 36 by reusing the
/// generic authenticated projection implementation.
pub fn build_v36_srht192_control() -> Result<V35Projection> {
    build_v35_srht(
        V35Dimensions {
            source: 768,
            routing: 192,
        },
        36,
    )
}

/// Allocate a fixed posting budget across non-empty runs with a one-posting
/// lower bound and Hamilton largest-remainder apportionment.
pub fn allocate_v36_hamilton_postings(run_rows: &[u64], total_postings: u32) -> Result<Vec<u32>> {
    let active: Vec<usize> = run_rows
        .iter()
        .enumerate()
        .filter_map(|(ordinal, rows)| (*rows != 0).then_some(ordinal))
        .collect();
    if active.is_empty() {
        return Err(invalid("V36 posting population is empty"));
    }
    let active_count =
        u32::try_from(active.len()).map_err(|_| invalid("V36 posting run count overflows"))?;
    if total_postings < active_count {
        return Err(invalid("V36 posting budget cannot cover every run"));
    }

    let total_rows = active.iter().try_fold(0_u128, |sum, ordinal| {
        sum.checked_add(u128::from(run_rows[*ordinal]))
            .ok_or_else(|| invalid("V36 posting population overflows"))
    })?;
    let remaining = u128::from(total_postings - active_count);
    let mut allocation = vec![0_u32; run_rows.len()];
    let mut floor_sum = 0_u32;
    let mut remainders = Vec::with_capacity(active.len());

    for ordinal in active {
        let numerator = remaining
            .checked_mul(u128::from(run_rows[ordinal]))
            .ok_or_else(|| invalid("V36 posting apportionment overflows"))?;
        let floor = u32::try_from(numerator / total_rows)
            .map_err(|_| invalid("V36 posting apportionment overflows"))?;
        allocation[ordinal] = 1_u32
            .checked_add(floor)
            .ok_or_else(|| invalid("V36 posting allocation overflows"))?;
        floor_sum = floor_sum
            .checked_add(floor)
            .ok_or_else(|| invalid("V36 posting allocation overflows"))?;
        remainders.push((numerator % total_rows, ordinal));
    }

    remainders
        .sort_unstable_by(|left, right| right.0.cmp(&left.0).then_with(|| left.1.cmp(&right.1)));
    let leftover = u32::try_from(remaining)
        .map_err(|_| invalid("V36 posting allocation overflows"))?
        .checked_sub(floor_sum)
        .ok_or_else(|| invalid("V36 posting allocation differs"))?;
    for (_, ordinal) in remainders
        .into_iter()
        .take(usize::try_from(leftover).map_err(|_| invalid("V36 posting allocation overflows"))?)
    {
        allocation[ordinal] = allocation[ordinal]
            .checked_add(1)
            .ok_or_else(|| invalid("V36 posting allocation overflows"))?;
    }

    if allocation.iter().try_fold(0_u32, |sum, value| {
        sum.checked_add(*value)
            .ok_or_else(|| invalid("V36 posting allocation overflows"))
    })? != total_postings
    {
        return Err(invalid("V36 posting allocation differs"));
    }
    Ok(allocation)
}

fn squared_l2(left: &[f32], right: &[f32]) -> Result<f64> {
    if left.len() != right.len() || left.is_empty() {
        return Err(invalid("V36 posting vector shape differs"));
    }
    left.iter().zip(right).try_fold(0.0, |sum, (left, right)| {
        if !left.is_finite() || !right.is_finite() {
            return Err(invalid("V36 posting vector is nonfinite"));
        }
        let delta = f64::from(*left) - f64::from(*right);
        let next = sum + delta * delta;
        next.is_finite()
            .then_some(next)
            .ok_or_else(|| invalid("V36 posting distance is nonfinite"))
    })
}

/// Select deterministic primary and closure owners for one projected row.
pub fn select_v36_closure_owners(
    row: &[f32],
    centroids: &[Vec<f32>],
    epsilon: f64,
    max_owners: u8,
) -> Result<Vec<u32>> {
    if centroids.is_empty() || max_owners == 0 || max_owners > 8 {
        return Err(invalid("V36 closure authority differs"));
    }
    if !matches!(epsilon, 0.05 | 0.15 | 0.30) {
        return Err(invalid("V36 closure epsilon differs"));
    }

    let mut ranked = centroids
        .iter()
        .enumerate()
        .map(|(ordinal, centroid)| Ok((squared_l2(row, centroid)?, ordinal)))
        .collect::<Result<Vec<_>>>()?;
    ranked.sort_unstable_by(|left, right| {
        left.0
            .total_cmp(&right.0)
            .then_with(|| left.1.cmp(&right.1))
    });
    let (primary_distance, primary) = ranked[0];
    let scale = 1.0 + epsilon;
    let threshold = primary_distance * scale * scale;
    if !threshold.is_finite() {
        return Err(invalid("V36 closure threshold is nonfinite"));
    }

    let mut retained = vec![primary];
    for (distance, candidate) in ranked.into_iter().skip(1) {
        if distance > threshold || retained.len() == usize::from(max_owners) {
            break;
        }
        let redundant = retained.iter().try_fold(false, |redundant, owner| {
            Ok::<_, BorsukError>(
                redundant || squared_l2(&centroids[*owner], &centroids[candidate])? < distance,
            )
        })?;
        if !redundant {
            retained.push(candidate);
        }
    }
    retained
        .into_iter()
        .map(|ordinal| u32::try_from(ordinal).map_err(|_| invalid("V36 posting ordinal overflows")))
        .collect()
}

/// First registered construction-side reason a V36 geometry is rejected.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum V36GeometryStop {
    /// Mean stored assignments exceed three per primary row.
    MeanReplication,
    /// Primary posting p99 exceeds twice the target occupancy.
    PrimaryP99,
    /// A primary posting exceeds four times the target occupancy.
    PrimaryMaximum,
    /// Stored-assignment p99 exceeds six times the target occupancy.
    StoredP99,
    /// A stored posting exceeds eight times the target occupancy.
    StoredMaximum,
}

/// Exact construction statistics and first-stop disposition for one geometry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct V36GeometryAdmission {
    /// Mean stored assignments per primary row in parts per million.
    pub mean_replication_ppm: u64,
    /// Nearest-rank 99th percentile of primary posting occupancy.
    pub primary_p99: u64,
    /// Maximum primary posting occupancy.
    pub primary_maximum: u64,
    /// Nearest-rank 99th percentile of stored posting occupancy.
    pub stored_p99: u64,
    /// Maximum stored posting occupancy.
    pub stored_maximum: u64,
    /// First registered rejection reason, or `None` when admitted.
    pub stop: Option<V36GeometryStop>,
}

fn percentile_99(values: &[u64]) -> Result<u64> {
    let mut ordered = values.to_vec();
    ordered.sort_unstable();
    let numerator = ordered
        .len()
        .checked_mul(99)
        .ok_or_else(|| invalid("V36 geometry percentile overflows"))?;
    let rank = numerator
        .checked_add(99)
        .ok_or_else(|| invalid("V36 geometry percentile overflows"))?
        / 100;
    ordered
        .get(rank.saturating_sub(1))
        .copied()
        .ok_or_else(|| invalid("V36 geometry occupancy is empty"))
}

/// Compute exact occupancy evidence and reject at the first registered gate.
pub fn admit_v36_geometry(
    primary_occupancy: &[u64],
    stored_occupancy: &[u64],
    target_primary_rows: u64,
) -> Result<V36GeometryAdmission> {
    if primary_occupancy.is_empty()
        || primary_occupancy.len() != stored_occupancy.len()
        || target_primary_rows == 0
        || primary_occupancy
            .iter()
            .zip(stored_occupancy)
            .any(|(primary, stored)| *primary == 0 || stored < primary)
    {
        return Err(invalid("V36 geometry occupancy authority differs"));
    }
    let primary_rows = primary_occupancy.iter().try_fold(0_u64, |sum, rows| {
        sum.checked_add(*rows)
            .ok_or_else(|| invalid("V36 geometry primary rows overflow"))
    })?;
    let stored_rows = stored_occupancy.iter().try_fold(0_u64, |sum, rows| {
        sum.checked_add(*rows)
            .ok_or_else(|| invalid("V36 geometry stored rows overflow"))
    })?;
    let replication_numerator = u128::from(stored_rows)
        .checked_mul(1_000_000)
        .ok_or_else(|| invalid("V36 geometry replication overflows"))?;
    let mean_replication_ppm = u64::try_from(
        replication_numerator
            .checked_add(u128::from(primary_rows - 1))
            .ok_or_else(|| invalid("V36 geometry replication overflows"))?
            / u128::from(primary_rows),
    )
    .map_err(|_| invalid("V36 geometry replication overflows"))?;
    let primary_p99 = percentile_99(primary_occupancy)?;
    let stored_p99 = percentile_99(stored_occupancy)?;
    let primary_maximum = *primary_occupancy
        .iter()
        .max()
        .ok_or_else(|| invalid("V36 geometry occupancy is empty"))?;
    let stored_maximum = *stored_occupancy
        .iter()
        .max()
        .ok_or_else(|| invalid("V36 geometry occupancy is empty"))?;
    let primary_p99_limit = target_primary_rows
        .checked_mul(2)
        .ok_or_else(|| invalid("V36 geometry threshold overflows"))?;
    let primary_maximum_limit = target_primary_rows
        .checked_mul(4)
        .ok_or_else(|| invalid("V36 geometry threshold overflows"))?;
    let stored_p99_limit = target_primary_rows
        .checked_mul(6)
        .ok_or_else(|| invalid("V36 geometry threshold overflows"))?;
    let stored_maximum_limit = target_primary_rows
        .checked_mul(8)
        .ok_or_else(|| invalid("V36 geometry threshold overflows"))?;
    let stop = if u128::from(stored_rows) > u128::from(primary_rows) * 3 {
        Some(V36GeometryStop::MeanReplication)
    } else if primary_p99 > primary_p99_limit {
        Some(V36GeometryStop::PrimaryP99)
    } else if primary_maximum > primary_maximum_limit {
        Some(V36GeometryStop::PrimaryMaximum)
    } else if stored_p99 > stored_p99_limit {
        Some(V36GeometryStop::StoredP99)
    } else if stored_maximum > stored_maximum_limit {
        Some(V36GeometryStop::StoredMaximum)
    } else {
        None
    };
    Ok(V36GeometryAdmission {
        mean_replication_ppm,
        primary_p99,
        primary_maximum,
        stored_p99,
        stored_maximum,
        stop,
    })
}
