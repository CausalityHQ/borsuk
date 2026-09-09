//! Deterministic posting geometry for the V36 qualification funnel.

use std::{
    collections::{BinaryHeap, HashMap},
    io::Cursor,
    sync::Arc,
};

use crate::{
    BorsukError, Result, V35Dimensions, V36ArtifactIdentity,
    v35_patch::deterministic_ln_u32,
    v35_projection::{V35Projection, V35ProjectionBackend, build_v35_srht},
    v36_funnel::POSTING_SUMMARY_SLOT_BYTES,
};
use arrow_array::{Array, FixedSizeListArray, Float32Array, Float64Array, RecordBatch};
use arrow_ipc::{
    MetadataVersion,
    reader::FileReader,
    writer::{FileWriter, IpcWriteOptions},
};
use arrow_schema::{DataType, Field, Schema};
use borsuk_fma::{FmaBackend, FusedProjection4};
use nalgebra::{DMatrix, SymmetricEigen};
use rand_chacha::ChaCha8Rng;
use rand_core::{RngCore, SeedableRng};
use rayon::{ThreadPoolBuilder, prelude::*};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

const CENTERED_EIGEN_MAX_ITERATIONS_PER_DIMENSION: usize = 30;
const CENTERED_AXIS_DEPENDENCE_THRESHOLD: f64 = 1e-12;
const CENTERED_EIGEN_CLUSTER_RELATIVE_TOLERANCE: f64 = 1e-10;
const CENTERED_TRAINING_WORKSPACE_LIMIT_BYTES: u64 = 64 * 1_048_576;
const CENTERED_MATRIX_WORKSPACE_COPIES: u64 = 7;
const CENTERED_PROJECTION_FORMAT: &str = "borsuk-v36-centered-projection-arrow-v1";
const CENTERED_PROJECTION_ALGORITHM: &str = "centered-exact-covariance-symmetric-eigen-v1";
const CENTERED_PROJECTION_SOLVER: &str = "nalgebra-0.33-symmetric-eigen-try-new-epsilon-30d-v1";
const CENTERED_PROJECTION_STORAGE_ORDER: &str = "source-major-f32-v1";
const CENTERED_PROJECTION_MANIFEST_KEY: &str = "borsuk.v36.centered_projection.manifest";
const CENTERED_PROJECTION_ROLE: &str = "centered-projection-basis";
const CENTERED_PROJECTION_MAXIMUM_ENCODED_BYTES: u64 = 3 * 1_048_576;

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
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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
    training_spec: V36CenteredProjectionTrainingSpec,
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
    /// Exact query-independent training population and bounded scan authority.
    pub fn training_spec(&self) -> &V36CenteredProjectionTrainingSpec {
        &self.training_spec
    }

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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct V36CenteredProjectionManifest {
    algorithm: String,
    basis_sha256: String,
    covariance_sha256: String,
    energy_dimensions: Vec<usize>,
    eigenvalues_sha256: String,
    format: String,
    max_eigenpair_relative_residual_bits: String,
    mean_sha256: String,
    reconstruction_relative_error_bits: String,
    retained_energy_ppm: Vec<u32>,
    role: String,
    solver: String,
    storage_order: String,
    training: V36CenteredProjectionTrainingSpec,
    uri: String,
}

fn v36_canonical_json_value(value: serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Array(values) => {
            serde_json::Value::Array(values.into_iter().map(v36_canonical_json_value).collect())
        }
        serde_json::Value::Object(values) => {
            let mut entries = values.into_iter().collect::<Vec<_>>();
            entries.sort_unstable_by(|left, right| left.0.cmp(&right.0));
            let mut sorted = serde_json::Map::new();
            for (key, value) in entries {
                sorted.insert(key, v36_canonical_json_value(value));
            }
            serde_json::Value::Object(sorted)
        }
        scalar => scalar,
    }
}

fn canonical_v36_projection_manifest(manifest: &V36CenteredProjectionManifest) -> Result<String> {
    let value = serde_json::to_value(manifest)
        .map_err(|_| invalid("V36 centered projection manifest differs"))?;
    serde_json::to_string(&v36_canonical_json_value(value))
        .map_err(|_| invalid("V36 centered projection manifest differs"))
}

fn v36_centered_f32_digest(
    domain: &[u8],
    source_dimensions: usize,
    retained_dimensions: usize,
    values: &[f32],
) -> Result<String> {
    let mut digest = Sha256::new();
    digest.update(domain);
    digest.update(
        u64::try_from(source_dimensions)
            .map_err(|_| invalid("V36 centered dimensions overflow"))?
            .to_le_bytes(),
    );
    digest.update(
        u64::try_from(retained_dimensions)
            .map_err(|_| invalid("V36 centered dimensions overflow"))?
            .to_le_bytes(),
    );
    for value in values {
        digest.update(value.to_bits().to_le_bytes());
    }
    Ok(format!("{:x}", digest.finalize()))
}

fn v36_centered_projection_manifest(
    projection: &V36CenteredProjection,
    role: &str,
    uri: &str,
) -> Result<V36CenteredProjectionManifest> {
    Ok(V36CenteredProjectionManifest {
        algorithm: CENTERED_PROJECTION_ALGORITHM.to_owned(),
        basis_sha256: v36_centered_f32_digest(
            b"borsuk-v36-centered-basis-v1\n",
            projection.mean.len(),
            projection.retained_dimensions,
            &projection.basis_source_major,
        )?,
        covariance_sha256: projection.covariance_sha256.clone(),
        energy_dimensions: projection.training_spec.energy_dimensions.clone(),
        eigenvalues_sha256: v36_centered_f64_digest(
            b"borsuk-v36-centered-eigenvalues-v1\n",
            projection.mean.len(),
            &projection.eigenvalues,
        )?,
        format: CENTERED_PROJECTION_FORMAT.to_owned(),
        max_eigenpair_relative_residual_bits: format!(
            "{:016x}",
            projection.max_eigenpair_relative_residual.to_bits()
        ),
        mean_sha256: projection.mean_sha256.clone(),
        reconstruction_relative_error_bits: format!(
            "{:016x}",
            projection.reconstruction_relative_error.to_bits()
        ),
        retained_energy_ppm: projection.retained_energy_ppm.clone(),
        role: role.to_owned(),
        solver: CENTERED_PROJECTION_SOLVER.to_owned(),
        storage_order: CENTERED_PROJECTION_STORAGE_ORDER.to_owned(),
        training: projection.training_spec.clone(),
        uri: uri.to_owned(),
    })
}

fn validate_v36_projection_artifact_identity(
    identity: &V36ArtifactIdentity,
    bytes: &[u8],
) -> Result<()> {
    let valid_digest = |value: &str| valid_sha256(value) && value.bytes().any(|byte| byte != b'0');
    if identity.role != CENTERED_PROJECTION_ROLE
        || identity.uri.is_empty()
        || identity.encoded_bytes != u64::try_from(bytes.len()).unwrap_or(u64::MAX)
        || identity.encoded_bytes > CENTERED_PROJECTION_MAXIMUM_ENCODED_BYTES
        || !valid_digest(&identity.sha256)
        || !valid_digest(&identity.blake3)
        || identity.sha256 != format!("{:x}", Sha256::digest(bytes))
        || identity.blake3 != blake3::hash(bytes).to_hex().as_str()
    {
        return Err(invalid("V36 centered projection artifact identity differs"));
    }
    Ok(())
}

fn v36_projection_untrusted_manifest(bytes: &[u8]) -> Result<String> {
    if bytes.len() < 18
        || bytes.len() > usize::try_from(CENTERED_PROJECTION_MAXIMUM_ENCODED_BYTES).unwrap()
        || !bytes.starts_with(b"ARROW1\0\0")
        || !bytes.ends_with(b"ARROW1")
    {
        return Err(invalid("V36 centered projection IPC envelope differs"));
    }
    let trailer = bytes.len() - 10;
    let footer_len = u32::from_le_bytes(
        bytes[trailer..trailer + 4]
            .try_into()
            .map_err(|_| invalid("V36 centered projection footer length differs"))?,
    ) as usize;
    let footer_start = trailer
        .checked_sub(footer_len)
        .filter(|offset| *offset >= 8)
        .ok_or_else(|| invalid("V36 centered projection footer extent differs"))?;
    let footer = arrow_ipc::root_as_footer(&bytes[footer_start..trailer])
        .map_err(|_| invalid("V36 centered projection footer differs"))?;
    if footer.version() != MetadataVersion::V5
        || footer
            .custom_metadata()
            .is_some_and(|values| !values.is_empty())
        || footer
            .dictionaries()
            .is_some_and(|values| !values.is_empty())
        || footer.recordBatches().map_or(0, |values| values.len()) != 1
    {
        return Err(invalid("V36 centered projection footer authority differs"));
    }
    let metadata = footer
        .schema()
        .and_then(|schema| schema.custom_metadata())
        .ok_or_else(|| invalid("V36 centered projection manifest is missing"))?;
    if metadata.len() != 1 || metadata.get(0).key() != Some(CENTERED_PROJECTION_MANIFEST_KEY) {
        return Err(invalid("V36 centered projection manifest differs"));
    }
    metadata
        .get(0)
        .value()
        .map(str::to_owned)
        .ok_or_else(|| invalid("V36 centered projection manifest differs"))
}

fn validate_v36_projection_ipc_field(field: arrow_ipc::Field<'_>, expected: &Field) -> Result<()> {
    if field.name() != Some(expected.name().as_str())
        || field.nullable()
        || field.dictionary().is_some()
        || field
            .custom_metadata()
            .is_some_and(|values| !values.is_empty())
    {
        return Err(invalid("V36 centered projection IPC field differs"));
    }
    let expected_children = match expected.data_type() {
        DataType::Float32 | DataType::Float64 => {
            let floating = field
                .type_as_floating_point()
                .ok_or_else(|| invalid("V36 centered projection IPC float differs"))?;
            let precision = if expected.data_type() == &DataType::Float32 {
                arrow_ipc::Precision::SINGLE
            } else {
                arrow_ipc::Precision::DOUBLE
            };
            if floating.precision() != precision {
                return Err(invalid("V36 centered projection IPC float differs"));
            }
            Vec::new()
        }
        DataType::FixedSizeList(child, width) => {
            if field
                .type_as_fixed_size_list()
                .is_none_or(|list| list.listSize() != *width)
            {
                return Err(invalid("V36 centered projection IPC list differs"));
            }
            vec![child.as_ref()]
        }
        _ => return Err(invalid("V36 centered projection IPC type differs")),
    };
    let actual_children = field.children();
    if actual_children.map_or(0, |values| values.len()) != expected_children.len() {
        return Err(invalid("V36 centered projection IPC children differ"));
    }
    for (index, child) in expected_children.iter().enumerate() {
        validate_v36_projection_ipc_field(
            actual_children
                .ok_or_else(|| invalid("V36 centered projection IPC child is missing"))?
                .get(index),
            child,
        )?;
    }
    Ok(())
}

fn validate_v36_projection_ipc_schema(
    schema: arrow_ipc::Schema<'_>,
    expected: &Schema,
) -> Result<()> {
    if schema.endianness() != arrow_ipc::Endianness::Little
        || schema.features().is_some_and(|values| !values.is_empty())
    {
        return Err(invalid("V36 centered projection IPC schema differs"));
    }
    let metadata = schema
        .custom_metadata()
        .ok_or_else(|| invalid("V36 centered projection manifest is missing"))?;
    let expected_manifest = expected
        .metadata()
        .get(CENTERED_PROJECTION_MANIFEST_KEY)
        .ok_or_else(|| invalid("V36 centered projection manifest differs"))?;
    if metadata.len() != 1
        || metadata.get(0).key() != Some(CENTERED_PROJECTION_MANIFEST_KEY)
        || metadata.get(0).value() != Some(expected_manifest.as_str())
    {
        return Err(invalid("V36 centered projection manifest differs"));
    }
    let fields = schema
        .fields()
        .ok_or_else(|| invalid("V36 centered projection IPC fields are missing"))?;
    if fields.len() != expected.fields().len() {
        return Err(invalid("V36 centered projection IPC field count differs"));
    }
    for (index, expected_field) in expected.fields().iter().enumerate() {
        validate_v36_projection_ipc_field(fields.get(index), expected_field)?;
    }
    Ok(())
}

fn validate_v36_projection_ipc_envelope(
    bytes: &[u8],
    expected_schema: &Schema,
    dimensions: usize,
    retained: usize,
) -> Result<()> {
    if bytes.len() < 18 {
        return Err(invalid("V36 centered projection IPC envelope differs"));
    }
    let trailer = bytes.len() - 10;
    let footer_len = u32::from_le_bytes(
        bytes
            .get(trailer..trailer + 4)
            .and_then(|value| value.try_into().ok())
            .ok_or_else(|| invalid("V36 centered projection footer length differs"))?,
    ) as usize;
    let footer_start = trailer
        .checked_sub(footer_len)
        .filter(|offset| *offset >= 8)
        .ok_or_else(|| invalid("V36 centered projection footer extent differs"))?;
    let footer = arrow_ipc::root_as_footer(&bytes[footer_start..trailer])
        .map_err(|_| invalid("V36 centered projection footer differs"))?;
    validate_v36_projection_ipc_schema(
        footer
            .schema()
            .ok_or_else(|| invalid("V36 centered projection footer schema is missing"))?,
        expected_schema,
    )?;
    let block = footer
        .recordBatches()
        .ok_or_else(|| invalid("V36 centered projection batch is missing"))?
        .get(0);
    let block_offset = usize::try_from(block.offset())
        .map_err(|_| invalid("V36 centered projection batch offset differs"))?;
    let metadata_len = usize::try_from(block.metaDataLength())
        .map_err(|_| invalid("V36 centered projection batch metadata differs"))?;
    let body_len = usize::try_from(block.bodyLength())
        .map_err(|_| invalid("V36 centered projection batch body differs"))?;
    let body_start = block_offset
        .checked_add(metadata_len)
        .ok_or_else(|| invalid("V36 centered projection batch extent overflows"))?;
    let body_end = body_start
        .checked_add(body_len)
        .ok_or_else(|| invalid("V36 centered projection batch extent overflows"))?;
    if block_offset < 8 || metadata_len < 8 || body_end > footer_start {
        return Err(invalid("V36 centered projection batch extent differs"));
    }
    let parse_message = |start: usize, end: usize| {
        let metadata = bytes
            .get(start..end)
            .ok_or_else(|| invalid("V36 centered projection message extent differs"))?;
        if metadata.len() < 4 {
            return Err(invalid("V36 centered projection message is truncated"));
        }
        let prefix = if metadata.starts_with(&[255; 4]) {
            8
        } else {
            4
        };
        let length = u32::from_le_bytes(
            metadata
                .get(prefix - 4..prefix)
                .and_then(|value| value.try_into().ok())
                .ok_or_else(|| invalid("V36 centered projection message length differs"))?,
        ) as usize;
        let message_end = prefix
            .checked_add(length)
            .filter(|value| *value <= metadata.len())
            .ok_or_else(|| invalid("V36 centered projection message extent differs"))?;
        arrow_ipc::root_as_message(&metadata[prefix..message_end])
            .map_err(|_| invalid("V36 centered projection message differs"))
    };
    let leading = parse_message(8, block_offset)?;
    if leading.version() != MetadataVersion::V5 || leading.bodyLength() != 0 {
        return Err(invalid("V36 centered projection leading schema differs"));
    }
    validate_v36_projection_ipc_schema(
        leading
            .header_as_schema()
            .ok_or_else(|| invalid("V36 centered projection leading schema is missing"))?,
        expected_schema,
    )?;
    let record_message = parse_message(block_offset, body_start)?;
    let record = record_message
        .header_as_record_batch()
        .ok_or_else(|| invalid("V36 centered projection record differs"))?;
    if record_message.version() != MetadataVersion::V5
        || record.compression().is_some()
        || record
            .variadicBufferCounts()
            .is_some_and(|values| !values.is_empty())
        || usize::try_from(record.length()).ok() != Some(dimensions)
        || usize::try_from(record_message.bodyLength()).ok() != Some(body_len)
    {
        return Err(invalid("V36 centered projection record authority differs"));
    }
    let nodes = record
        .nodes()
        .ok_or_else(|| invalid("V36 centered projection nodes are missing"))?;
    let basis_values = dimensions
        .checked_mul(retained)
        .ok_or_else(|| invalid("V36 centered projection node length overflows"))?;
    let expected_nodes = [dimensions, dimensions, dimensions, basis_values];
    if nodes.len() != expected_nodes.len()
        || nodes.iter().zip(expected_nodes).any(|(node, expected)| {
            usize::try_from(node.length()).ok() != Some(expected) || node.null_count() != 0
        })
    {
        return Err(invalid("V36 centered projection node shape differs"));
    }
    let buffers = record
        .buffers()
        .ok_or_else(|| invalid("V36 centered projection buffers are missing"))?;
    let f64_bytes = dimensions
        .checked_mul(8)
        .ok_or_else(|| invalid("V36 centered projection buffer length overflows"))?;
    let validity_bytes = |rows: usize| {
        rows.checked_add(7)
            .map(|bits| bits / 8)
            .ok_or_else(|| invalid("V36 centered projection validity length overflows"))
    };
    let expected_lengths = [
        validity_bytes(dimensions)?,
        f64_bytes,
        validity_bytes(dimensions)?,
        f64_bytes,
        validity_bytes(dimensions)?,
        validity_bytes(basis_values)?,
        dimensions
            .checked_mul(retained)
            .and_then(|values| values.checked_mul(4))
            .ok_or_else(|| invalid("V36 centered projection buffer length overflows"))?,
    ];
    if buffers.len() != expected_lengths.len() {
        return Err(invalid("V36 centered projection buffer count differs"));
    }
    let mut previous_end = 0_usize;
    for (index, (buffer, expected_length)) in buffers.iter().zip(expected_lengths).enumerate() {
        let start = usize::try_from(buffer.offset())
            .map_err(|_| invalid("V36 centered projection buffer offset differs"))?;
        let length = usize::try_from(buffer.length())
            .map_err(|_| invalid("V36 centered projection buffer length differs"))?;
        let end = start
            .checked_add(length)
            .ok_or_else(|| invalid("V36 centered projection buffer extent overflows"))?;
        let is_validity = matches!(index, 0 | 2 | 4 | 5);
        if start < previous_end
            || (length != expected_length && !(is_validity && length == 0))
            || end > body_len
        {
            return Err(invalid(&format!(
                "V36 centered projection buffer extent differs: index={index} start={start} \
                 length={length} expected={expected_length} previous_end={previous_end} \
                 body_length={body_len}"
            )));
        }
        previous_end = end;
    }
    Ok(())
}

fn parse_v36_f64_bits(value: &str) -> Result<f64> {
    if value.len() != 16
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(invalid(
            "V36 centered projection numerical evidence differs",
        ));
    }
    u64::from_str_radix(value, 16)
        .map(f64::from_bits)
        .map_err(|_| invalid("V36 centered projection numerical evidence differs"))
}

fn validate_v36_projection_values(projection: &V36CenteredProjection) -> Result<()> {
    validate_v36_centered_training_spec(&projection.training_spec)?;
    let dimensions = projection.mean.len();
    let retained = projection.retained_dimensions;
    if dimensions == 0
        || retained == 0
        || retained > dimensions
        || projection.training_spec.source_dimensions != dimensions
        || projection.training_spec.retained_dimensions != retained
        || projection.eigenvalues.len() != dimensions
        || dimensions
            .checked_mul(retained)
            .is_none_or(|length| projection.basis_source_major.len() != length)
        || projection.mean.iter().any(|value| !value.is_finite())
        || projection
            .eigenvalues
            .iter()
            .any(|value| !value.is_finite() || *value < 0.0)
        || projection
            .eigenvalues
            .windows(2)
            .any(|pair| pair[0] < pair[1])
        || projection
            .basis_source_major
            .iter()
            .any(|value| !value.is_finite())
        || !projection.max_eigenpair_relative_residual.is_finite()
        || projection.max_eigenpair_relative_residual < 0.0
        || projection.max_eigenpair_relative_residual > 1e-10
        || !projection.reconstruction_relative_error.is_finite()
        || projection.reconstruction_relative_error < 0.0
        || projection.reconstruction_relative_error > 1e-10
        || !valid_sha256(&projection.mean_sha256)
        || !valid_sha256(&projection.covariance_sha256)
    {
        return Err(invalid("V36 centered projection values differ"));
    }
    let total = projection.eigenvalues.iter().sum::<f64>();
    if !total.is_finite() || total <= 0.0 {
        return Err(invalid("V36 centered projection spectrum differs"));
    }
    let energy = projection
        .training_spec
        .energy_dimensions
        .iter()
        .map(|dimension| {
            let retained_energy = projection.eigenvalues[..*dimension].iter().sum::<f64>();
            Ok((retained_energy / total * 1_000_000.0).round() as u32)
        })
        .collect::<Result<Vec<_>>>()?;
    if energy != projection.retained_energy_ppm
        || v36_centered_f64_digest(
            b"borsuk-v36-centered-mean-v1\n",
            dimensions,
            &projection.mean,
        )? != projection.mean_sha256
    {
        return Err(invalid("V36 centered projection evidence differs"));
    }
    for left in 0..retained {
        for right in 0..retained {
            let dot = (0..dimensions).fold(0.0_f64, |sum, dimension| {
                let left_value =
                    f64::from(projection.basis_source_major[dimension * retained + left]);
                let right_value =
                    f64::from(projection.basis_source_major[dimension * retained + right]);
                left_value.mul_add(right_value, sum)
            });
            let expected = if left == right { 1.0 } else { 0.0 };
            if (dot - expected).abs() > 5e-6 {
                return Err(invalid("V36 centered projection basis differs"));
            }
        }
    }
    Ok(())
}

/// Encode one V36 centered projection as a strict cross-language Arrow IPC file.
pub fn encode_v36_centered_projection_arrow(
    projection: &V36CenteredProjection,
    role: &str,
    uri: &str,
) -> Result<(Vec<u8>, V36ArtifactIdentity)> {
    validate_v36_projection_values(projection)?;
    if role != CENTERED_PROJECTION_ROLE || uri.is_empty() {
        return Err(invalid("V36 centered projection artifact identity differs"));
    }
    let retained = projection.retained_dimensions;
    let list_size = i32::try_from(retained)
        .map_err(|_| invalid("V36 centered projection dimensions overflow"))?;
    let child = Arc::new(Field::new("element", DataType::Float32, false));
    let basis = Arc::new(FixedSizeListArray::try_new(
        child.clone(),
        list_size,
        Arc::new(Float32Array::new(
            projection.basis_source_major.clone().into(),
            None,
        )),
        None,
    )?);
    let manifest = v36_centered_projection_manifest(projection, role, uri)?;
    let mut metadata = HashMap::new();
    metadata.insert(
        CENTERED_PROJECTION_MANIFEST_KEY.to_owned(),
        canonical_v36_projection_manifest(&manifest)?,
    );
    let schema = Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("mean", DataType::Float64, false),
            Field::new("eigenvalue", DataType::Float64, false),
            Field::new("basis", DataType::FixedSizeList(child, list_size), false),
        ],
        metadata,
    ));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Float64Array::new(projection.mean.clone().into(), None)),
            Arc::new(Float64Array::new(
                projection.eigenvalues.clone().into(),
                None,
            )),
            basis,
        ],
    )?;
    let mut bytes = Vec::new();
    let options = IpcWriteOptions::try_new(8, false, MetadataVersion::V5)?;
    let mut writer = FileWriter::try_new_with_options(&mut bytes, schema.as_ref(), options)?;
    writer.write(&batch)?;
    writer.finish()?;
    drop(writer);
    if u64::try_from(bytes.len()).unwrap_or(u64::MAX) > CENTERED_PROJECTION_MAXIMUM_ENCODED_BYTES {
        return Err(invalid(
            "V36 centered projection encoded bytes exceed admission",
        ));
    }
    let identity = V36ArtifactIdentity {
        blake3: blake3::hash(&bytes).to_hex().to_string(),
        encoded_bytes: u64::try_from(bytes.len())
            .map_err(|_| invalid("V36 centered projection artifact length overflows"))?,
        role: role.to_owned(),
        sha256: format!("{:x}", Sha256::digest(&bytes)),
        uri: uri.to_owned(),
    };
    Ok((bytes, identity))
}

/// Authenticate and decode one strict V36 centered projection Arrow IPC file.
pub fn decode_v36_centered_projection_arrow(
    bytes: &[u8],
    identity: &V36ArtifactIdentity,
    expected_training: &V36CenteredProjectionTrainingSpec,
) -> Result<V36CenteredProjection> {
    validate_v36_projection_artifact_identity(identity, bytes)?;
    validate_v36_centered_training_spec(expected_training)?;
    let manifest_text = v36_projection_untrusted_manifest(bytes)?;
    let manifest: V36CenteredProjectionManifest = serde_json::from_str(&manifest_text)
        .map_err(|_| invalid("V36 centered projection manifest differs"))?;
    if canonical_v36_projection_manifest(&manifest)? != manifest_text
        || manifest.format != CENTERED_PROJECTION_FORMAT
        || manifest.algorithm != CENTERED_PROJECTION_ALGORITHM
        || manifest.solver != CENTERED_PROJECTION_SOLVER
        || manifest.storage_order != CENTERED_PROJECTION_STORAGE_ORDER
        || manifest.role != identity.role
        || manifest.uri != identity.uri
        || &manifest.training != expected_training
        || manifest.energy_dimensions != expected_training.energy_dimensions
        || !valid_sha256(&manifest.basis_sha256)
        || !valid_sha256(&manifest.eigenvalues_sha256)
        || !valid_sha256(&manifest.mean_sha256)
        || !valid_sha256(&manifest.covariance_sha256)
    {
        return Err(invalid(
            "V36 centered projection manifest authority differs",
        ));
    }
    let dimensions = expected_training.source_dimensions;
    let retained = expected_training.retained_dimensions;
    let list_size = i32::try_from(retained)
        .map_err(|_| invalid("V36 centered projection dimensions overflow"))?;
    let child = Arc::new(Field::new("element", DataType::Float32, false));
    let expected_fields = vec![
        Field::new("mean", DataType::Float64, false),
        Field::new("eigenvalue", DataType::Float64, false),
        Field::new("basis", DataType::FixedSizeList(child, list_size), false),
    ];
    let mut metadata = HashMap::new();
    metadata.insert(CENTERED_PROJECTION_MANIFEST_KEY.to_owned(), manifest_text);
    let expected_schema = Schema::new_with_metadata(expected_fields.clone(), metadata);
    validate_v36_projection_ipc_envelope(bytes, &expected_schema, dimensions, retained)?;
    let mut reader = FileReader::try_new(Cursor::new(bytes), None)?;
    let schema = reader.schema();
    if reader.num_batches() != 1 || schema.as_ref() != &expected_schema {
        return Err(invalid("V36 centered projection Arrow schema differs"));
    }
    if schema
        .fields()
        .iter()
        .zip(&expected_fields)
        .any(|(actual, expected)| actual.as_ref() != expected)
    {
        return Err(invalid("V36 centered projection Arrow schema differs"));
    }
    let batch = reader
        .next()
        .ok_or_else(|| invalid("V36 centered projection Arrow batch is missing"))??;
    if batch.num_rows() != dimensions || batch.num_columns() != 3 {
        return Err(invalid("V36 centered projection Arrow batch differs"));
    }
    let mean = batch
        .column(0)
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| invalid("V36 centered projection mean differs"))?;
    let eigenvalues = batch
        .column(1)
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| invalid("V36 centered projection eigenvalues differ"))?;
    let basis = batch
        .column(2)
        .as_any()
        .downcast_ref::<FixedSizeListArray>()
        .ok_or_else(|| invalid("V36 centered projection basis differs"))?;
    let basis_values = basis
        .values()
        .as_any()
        .downcast_ref::<Float32Array>()
        .ok_or_else(|| invalid("V36 centered projection basis differs"))?;
    if mean.null_count() != 0
        || eigenvalues.null_count() != 0
        || basis.null_count() != 0
        || basis_values.null_count() != 0
        || basis_values.len() != dimensions.saturating_mul(retained)
    {
        return Err(invalid("V36 centered projection Arrow nullability differs"));
    }
    let projection = V36CenteredProjection {
        training_spec: manifest.training,
        retained_dimensions: retained,
        mean: mean.values().to_vec(),
        mean_sha256: manifest.mean_sha256,
        covariance_sha256: manifest.covariance_sha256,
        eigenvalues: eigenvalues.values().to_vec(),
        basis_source_major: basis_values.values().to_vec(),
        retained_energy_ppm: manifest.retained_energy_ppm,
        max_eigenpair_relative_residual: parse_v36_f64_bits(
            &manifest.max_eigenpair_relative_residual_bits,
        )?,
        reconstruction_relative_error: parse_v36_f64_bits(
            &manifest.reconstruction_relative_error_bits,
        )?,
    };
    validate_v36_projection_values(&projection)?;
    let expected_manifest =
        v36_centered_projection_manifest(&projection, CENTERED_PROJECTION_ROLE, &identity.uri)?;
    if expected_manifest.basis_sha256 != manifest.basis_sha256
        || expected_manifest.eigenvalues_sha256 != manifest.eigenvalues_sha256
    {
        return Err(invalid("V36 centered projection logical digest differs"));
    }
    Ok(projection)
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

fn validate_v36_centered_training_spec(spec: &V36CenteredProjectionTrainingSpec) -> Result<()> {
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
    Ok(())
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
    validate_v36_centered_training_spec(spec)?;
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
        training_spec: spec.clone(),
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

// Give every empty centroid one deterministic row without emptying its donor.
fn repair_v36_empty_posting_assignments(
    ordered: &[&(u64, Vec<f32>)],
    assignments: &mut [usize],
    assigned_distances: &[f64],
    counts: &mut [usize],
) -> Result<()> {
    for empty in 0..counts.len() {
        if counts[empty] != 0 {
            continue;
        }
        let candidate = (0..ordered.len())
            .filter(|row_index| counts[assignments[*row_index]] > 1)
            .max_by(|left, right| {
                assigned_distances[*left]
                    .total_cmp(&assigned_distances[*right])
                    .then_with(|| ordered[*right].0.cmp(&ordered[*left].0))
            })
            .ok_or_else(|| invalid("V36 posting empty-centroid repair differs"))?;
        let donor = assignments[candidate];
        counts[donor] -= 1;
        counts[empty] = 1;
        assignments[candidate] = empty;
    }
    Ok(())
}

/// Train local posting centroids with deterministic farthest-first seeding and
/// exactly ten source-ordinal-ordered Lloyd iterations.
pub fn train_v36_posting_centroids(
    rows: &[(u64, Vec<f32>)],
    posting_count: u32,
) -> Result<Vec<Vec<f32>>> {
    let posting_count =
        usize::try_from(posting_count).map_err(|_| invalid("V36 posting count overflows"))?;
    if rows.is_empty() || posting_count == 0 || posting_count > rows.len() {
        return Err(invalid("V36 posting centroid authority differs"));
    }
    let mut ordered = rows.iter().collect::<Vec<_>>();
    ordered.sort_unstable_by_key(|(ordinal, _)| *ordinal);
    if ordered.windows(2).any(|pair| pair[0].0 == pair[1].0)
        || ordered.iter().any(|(_, vector)| {
            vector.len() != 192
                || vector
                    .iter()
                    .any(|value| !value.is_finite() || (*value == 0.0 && value.to_bits() != 0))
        })
    {
        return Err(invalid("V36 posting centroid training row differs"));
    }

    let mut selected = vec![false; ordered.len()];
    selected[0] = true;
    let mut centroids = vec![ordered[0].1.clone()];
    let mut nearest_distances = vec![f64::INFINITY; ordered.len()];
    while centroids.len() < posting_count {
        let mut best: Option<(f64, u64, usize)> = None;
        let newest = centroids
            .last()
            .ok_or_else(|| invalid("V36 posting centroid initialization differs"))?;
        for (row_index, (ordinal, vector)) in ordered.iter().enumerate() {
            if selected[row_index] {
                continue;
            }
            nearest_distances[row_index] =
                nearest_distances[row_index].min(squared_l2(vector, newest)?);
            let nearest = nearest_distances[row_index];
            if best.as_ref().is_none_or(|(distance, best_ordinal, _)| {
                nearest > *distance || (nearest == *distance && *ordinal < *best_ordinal)
            }) {
                best = Some((nearest, *ordinal, row_index));
            }
        }
        let (_, _, row_index) =
            best.ok_or_else(|| invalid("V36 posting centroid initialization differs"))?;
        selected[row_index] = true;
        centroids.push(ordered[row_index].1.clone());
    }

    let mut assignments = vec![0_usize; ordered.len()];
    let mut assigned_distances = vec![0.0_f64; ordered.len()];
    for _ in 0..10 {
        let mut counts = vec![0_usize; posting_count];
        for (row_index, (_, vector)) in ordered.iter().enumerate() {
            let mut best = (squared_l2(vector, &centroids[0])?, 0_usize);
            for (centroid_index, centroid) in centroids.iter().enumerate().skip(1) {
                let distance = squared_l2(vector, centroid)?;
                if distance < best.0 {
                    best = (distance, centroid_index);
                }
            }
            assignments[row_index] = best.1;
            assigned_distances[row_index] = best.0;
            counts[best.1] = counts[best.1]
                .checked_add(1)
                .ok_or_else(|| invalid("V36 posting assignment count overflows"))?;
        }

        repair_v36_empty_posting_assignments(
            &ordered,
            &mut assignments,
            &assigned_distances,
            &mut counts,
        )?;

        let mut sums = vec![vec![0.0_f64; 192]; posting_count];
        for (row_index, (_, vector)) in ordered.iter().enumerate() {
            let centroid = assignments[row_index];
            for (sum, value) in sums[centroid].iter_mut().zip(vector) {
                *sum += f64::from(*value);
            }
        }
        for centroid in 0..posting_count {
            let divisor = counts[centroid] as f64;
            for dimension in 0..192 {
                let rounded = (sums[centroid][dimension] / divisor) as f32;
                if !rounded.is_finite() {
                    return Err(invalid("V36 posting centroid is nonfinite"));
                }
                centroids[centroid][dimension] = if rounded == 0.0 { 0.0 } else { rounded };
            }
        }
    }
    Ok(centroids)
}

fn invalid_v36_vector(vector: &[f32]) -> bool {
    vector.len() != 192
        || vector
            .iter()
            .any(|value| !value.is_finite() || (*value == 0.0 && value.to_bits() != 0))
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

/// Authenticated rank-four Gaussian posting summary for the V36 screen.
#[derive(Debug, Clone, PartialEq)]
pub struct V36PostingGaussianSummary {
    rank: u8,
    population: u32,
    mean: [f32; 192],
    residual_diagonal: [f32; 192],
    eigenvalues: [f32; 4],
    directions: [[f32; 192]; 4],
    trace: f64,
    trace_square: f64,
    population_factor: f64,
}

impl V36PostingGaussianSummary {
    /// Validate one complete 192-dimensional rank-four summary.
    ///
    /// Direction signs must be canonicalized after rounding to binary32.
    pub fn try_new(
        rank: u8,
        population: u32,
        mean: Vec<f32>,
        residual_diagonal: Vec<f32>,
        eigenvalues: [f32; 4],
        directions: [Vec<f32>; 4],
    ) -> Result<Self> {
        if !matches!(rank, 0 | 2 | 4)
            || population == 0
            || mean.len() != 192
            || residual_diagonal.len() != 192
            || directions.iter().any(|direction| direction.len() != 192)
            || mean.iter().any(|value| !value.is_finite())
            || residual_diagonal
                .iter()
                .any(|value| !value.is_finite() || *value < 0.0)
            || eigenvalues
                .iter()
                .any(|value| !value.is_finite() || *value < 0.0)
            || eigenvalues.windows(2).any(|pair| pair[0] < pair[1])
            || directions.iter().flatten().any(|value| !value.is_finite())
            || mean
                .iter()
                .chain(&residual_diagonal)
                .chain(&eigenvalues)
                .chain(directions.iter().flatten())
                .any(|value| value.to_bits() == (-0.0_f32).to_bits())
        {
            return Err(invalid("V36 posting Gaussian summary differs"));
        }
        for component in 0..4 {
            let direction = &directions[component];
            if component >= usize::from(rank)
                && (eigenvalues[component] != 0.0 || direction.iter().any(|value| *value != 0.0))
            {
                return Err(invalid("V36 inactive posting component differs"));
            }
            if eigenvalues[component] == 0.0 {
                if direction.iter().any(|value| *value != 0.0) {
                    return Err(invalid("V36 zero posting component differs"));
                }
                continue;
            }
            let norm = direction.iter().fold(0.0_f64, |sum, value| {
                f64::from(*value).mul_add(f64::from(*value), sum)
            });
            if (norm - 1.0).abs() > 1e-5 {
                return Err(invalid("V36 posting direction norm differs"));
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
                .ok_or_else(|| invalid("V36 posting direction authority differs"))?;
            if pivot <= 0.0 {
                return Err(invalid("V36 posting direction sign differs"));
            }
            for prior in &directions[..component] {
                let dot = direction
                    .iter()
                    .zip(prior)
                    .fold(0.0_f64, |sum, (left, right)| {
                        f64::from(*left).mul_add(f64::from(*right), sum)
                    });
                if dot.abs() > 1e-5 {
                    return Err(invalid("V36 posting directions are not orthogonal"));
                }
            }
        }
        let mut trace = residual_diagonal
            .iter()
            .map(|value| f64::from(*value))
            .sum::<f64>();
        let mut trace_square = residual_diagonal
            .iter()
            .map(|value| f64::from(*value).powi(2))
            .sum::<f64>();
        let active_components = usize::from(rank);
        for component in 0..active_components {
            let eigenvalue = f64::from(eigenvalues[component]);
            let direction = &directions[component];
            let norm = direction.iter().fold(0.0_f64, |sum, value| {
                f64::from(*value).mul_add(f64::from(*value), sum)
            });
            trace += eigenvalue * norm;
            for (basis, diagonal) in direction.iter().zip(&residual_diagonal) {
                let basis = f64::from(*basis);
                trace_square += 2.0 * eigenvalue * f64::from(*diagonal) * basis * basis;
            }
            for other in 0..active_components {
                let dot = direction
                    .iter()
                    .zip(&directions[other])
                    .fold(0.0_f64, |sum, (left, right)| {
                        f64::from(*left).mul_add(f64::from(*right), sum)
                    });
                trace_square += eigenvalue * f64::from(eigenvalues[other]) * dot * dot;
            }
        }
        if !trace.is_finite() || !trace_square.is_finite() {
            return Err(invalid("V36 posting Gaussian moments are nonfinite"));
        }
        let population_factor = (2.0 * deterministic_ln_u32(population)).sqrt();
        if !population_factor.is_finite() {
            return Err(invalid("V36 posting population factor is nonfinite"));
        }
        let [direction_0, direction_1, direction_2, direction_3] = directions;
        Ok(Self {
            rank,
            population,
            mean: mean
                .try_into()
                .map_err(|_| invalid("V36 posting mean shape differs"))?,
            residual_diagonal: residual_diagonal
                .try_into()
                .map_err(|_| invalid("V36 posting residual shape differs"))?,
            eigenvalues,
            directions: [
                direction_0
                    .try_into()
                    .map_err(|_| invalid("V36 posting direction shape differs"))?,
                direction_1
                    .try_into()
                    .map_err(|_| invalid("V36 posting direction shape differs"))?,
                direction_2
                    .try_into()
                    .map_err(|_| invalid("V36 posting direction shape differs"))?,
                direction_3
                    .try_into()
                    .map_err(|_| invalid("V36 posting direction shape differs"))?,
            ],
            trace,
            trace_square,
            population_factor,
        })
    }

    /// Posting mean shared by the centroid and Gaussian arms.
    pub fn mean(&self) -> &[f32] {
        &self.mean
    }

    /// Active covariance rank, restricted to zero, two, or four.
    pub const fn rank(&self) -> u8 {
        self.rank
    }

    /// Rank-specific residual diagonal after removing active components.
    pub fn residual_diagonal(&self) -> &[f32] {
        &self.residual_diagonal
    }

    /// Rank-ordered binary32 covariance eigenvalues.
    pub const fn eigenvalues(&self) -> &[f32; 4] {
        &self.eigenvalues
    }

    /// Canonical rank-ordered binary32 covariance directions.
    pub const fn directions(&self) -> &[[f32; 192]; 4] {
        &self.directions
    }

    /// Number of unique primary rows summarized by this posting.
    pub const fn population(&self) -> u32 {
        self.population
    }

    /// Raw persisted bytes for this rank-specific summary.
    pub const fn raw_bytes(&self) -> u64 {
        1_540 + self.rank as u64 * 772
    }

    /// Resident bytes including three cached binary64 scoring moments.
    pub const fn used_bytes(&self) -> u64 {
        self.raw_bytes() + 24
    }

    /// Equal resident charge applied to every posting-score arm.
    pub const fn resident_slot_bytes(&self) -> u64 {
        POSTING_SUMMARY_SLOT_BYTES
    }
}

/// Train one rank-specific V36 Gaussian summary from unique primary rows.
pub fn train_v36_posting_gaussian(
    rows: &[(u64, Vec<f32>)],
    rank: u8,
) -> Result<V36PostingGaussianSummary> {
    if rows.is_empty() || rows.len() > u32::MAX as usize || !matches!(rank, 0 | 2 | 4) {
        return Err(invalid("V36 posting Gaussian training authority differs"));
    }
    let mut ordered = rows.iter().collect::<Vec<_>>();
    ordered.sort_unstable_by_key(|(ordinal, _)| *ordinal);
    if ordered.windows(2).any(|pair| pair[0].0 == pair[1].0)
        || ordered.iter().any(|(_, vector)| {
            vector.len() != 192
                || vector
                    .iter()
                    .any(|value| !value.is_finite() || (*value == 0.0 && value.to_bits() != 0))
        })
    {
        return Err(invalid("V36 posting Gaussian training row differs"));
    }
    let population = u32::try_from(ordered.len())
        .map_err(|_| invalid("V36 posting Gaussian population overflows"))?;
    let divisor = f64::from(population);
    let mut mean_f64 = vec![0.0_f64; 192];
    for (_, row) in &ordered {
        for (sum, value) in mean_f64.iter_mut().zip(row.iter()) {
            *sum += f64::from(*value);
        }
    }
    mean_f64.iter_mut().for_each(|value| *value /= divisor);
    let mean = mean_f64
        .iter()
        .map(|value| {
            let rounded = *value as f32;
            if rounded == 0.0 { 0.0 } else { rounded }
        })
        .collect::<Vec<_>>();

    let mut covariance = vec![0.0_f64; 192 * 192];
    for (_, row) in &ordered {
        let mut delta = [0.0_f64; 192];
        for dimension in 0..192 {
            delta[dimension] = f64::from(row[dimension]) - mean_f64[dimension];
        }
        for column in 0..192 {
            for row_index in 0..=column {
                let index = row_index + column * 192;
                covariance[index] = delta[row_index].mul_add(delta[column], covariance[index]);
            }
        }
    }
    for column in 0..192 {
        for row_index in 0..=column {
            let value = covariance[row_index + column * 192] / divisor;
            covariance[row_index + column * 192] = value;
            covariance[column + row_index * 192] = value;
        }
    }
    let covariance_diagonal = (0..192)
        .map(|dimension| covariance[dimension + dimension * 192])
        .collect::<Vec<_>>();

    let mut eigenvalues = [0.0_f32; 4];
    let mut directions = [(); 4].map(|_| vec![0.0_f32; 192]);
    if rank > 0 && covariance.iter().any(|value| *value != 0.0) {
        let matrix = DMatrix::<f64>::from_vec(192, 192, covariance);
        let eigen = SymmetricEigen::try_new(
            matrix,
            f64::EPSILON,
            192 * CENTERED_EIGEN_MAX_ITERATIONS_PER_DIMENSION,
        )
        .ok_or_else(|| invalid("V36 posting Gaussian eigensolver did not converge"))?;
        let trace = eigen.eigenvalues.iter().sum::<f64>();
        let negative_tolerance = (trace.abs() * 1e-12).max(1e-15);
        if eigen
            .eigenvalues
            .iter()
            .any(|value| !value.is_finite() || *value < -negative_tolerance)
            || eigen.eigenvectors.iter().any(|value| !value.is_finite())
        {
            return Err(invalid("V36 posting Gaussian eigensystem differs"));
        }
        let values = eigen
            .eigenvalues
            .iter()
            .map(|value| value.max(0.0))
            .collect::<Vec<_>>();
        let mut order = (0..192).collect::<Vec<_>>();
        order.sort_by(|left, right| {
            values[*right]
                .total_cmp(&values[*left])
                .then_with(|| left.cmp(right))
        });
        let mut retained = 0_usize;
        let mut start = 0_usize;
        while start < order.len() && retained < usize::from(rank) {
            let reference = values[order[start]];
            let mut end = start + 1;
            while end < order.len() {
                let candidate = values[order[end]];
                let local_scale = reference.abs().max(candidate.abs());
                if (candidate - reference).abs()
                    > CENTERED_EIGEN_CLUSTER_RELATIVE_TOLERANCE * local_scale
                {
                    break;
                }
                end += 1;
            }
            let cluster_value = order[start..end]
                .iter()
                .fold(0.0_f64, |sum, ordinal| sum + values[*ordinal])
                / (end - start) as f64;
            if cluster_value > 0.0 {
                for vector in canonicalize_v36_eigenspace(&eigen.eigenvectors, &order[start..end])?
                {
                    if retained == usize::from(rank) {
                        break;
                    }
                    let rounded_value = cluster_value as f32;
                    if !rounded_value.is_finite() || rounded_value < 0.0 {
                        return Err(invalid("V36 posting Gaussian eigenvalue differs"));
                    }
                    let mut rounded = vector
                        .iter()
                        .map(|value| {
                            let value = *value as f32;
                            if value == 0.0 { 0.0 } else { value }
                        })
                        .collect::<Vec<_>>();
                    let pivot = rounded
                        .iter()
                        .enumerate()
                        .max_by(|left, right| {
                            left.1
                                .abs()
                                .total_cmp(&right.1.abs())
                                .then_with(|| right.0.cmp(&left.0))
                        })
                        .map(|(dimension, _)| dimension)
                        .ok_or_else(|| invalid("V36 posting Gaussian direction is empty"))?;
                    if rounded[pivot].is_sign_negative() {
                        rounded.iter_mut().for_each(|value| *value = -*value);
                    }
                    rounded
                        .iter_mut()
                        .filter(|value| **value == 0.0)
                        .for_each(|value| *value = 0.0);
                    eigenvalues[retained] = rounded_value;
                    directions[retained] = rounded;
                    retained += 1;
                }
            }
            start = end;
        }
    }

    let mut residual = Vec::with_capacity(192);
    for dimension in 0..192 {
        let retained = (0..usize::from(rank)).fold(0.0_f64, |sum, component| {
            let direction = f64::from(directions[component][dimension]);
            f64::from(eigenvalues[component]).mul_add(direction * direction, sum)
        });
        let raw = covariance_diagonal[dimension] - retained;
        let tolerance = (covariance_diagonal[dimension].abs() * 1e-5).max(1e-7);
        if raw < -tolerance {
            return Err(invalid("V36 posting Gaussian residual differs"));
        }
        let rounded = raw.max(0.0) as f32;
        residual.push(if rounded == 0.0 { 0.0 } else { rounded });
    }
    V36PostingGaussianSummary::try_new(rank, population, mean, residual, eigenvalues, directions)
}

/// Score one posting by centroid squared-L2.
pub fn score_v36_posting_centroid(mean: &[f32], query: &[f32]) -> Result<f64> {
    if mean.len() != 192 || query.len() != 192 {
        return Err(invalid("V36 centroid score shape differs"));
    }
    squared_l2(mean, query)
}

/// Score one posting with the frozen diagonal, rank-two, or rank-four lower tail.
pub fn score_v36_posting_gaussian(
    summary: &V36PostingGaussianSummary,
    query: &[f32],
) -> Result<f64> {
    if query.len() != 192 || query.iter().any(|v| !v.is_finite()) {
        return Err(invalid("V36 Gaussian score authority differs"));
    }
    let mut delta = [0.0_f64; 192];
    let mut distance = 0.0_f64;
    let mut covariance_projection = 0.0_f64;
    for dimension in 0..192 {
        let value = f64::from(query[dimension]) - f64::from(summary.mean[dimension]);
        delta[dimension] = value;
        distance = value.mul_add(value, distance);
        let diagonal = f64::from(summary.residual_diagonal[dimension]);
        covariance_projection = diagonal.mul_add(value * value, covariance_projection);
    }
    for component in 0..usize::from(summary.rank) {
        let eigenvalue = f64::from(summary.eigenvalues[component]);
        let direction = &summary.directions[component];
        let projection = direction
            .iter()
            .zip(&delta)
            .fold(0.0_f64, |sum, (basis, value)| {
                f64::from(*basis).mul_add(*value, sum)
            });
        covariance_projection += eigenvalue * projection * projection;
    }
    let radicand = 2.0 * summary.trace_square + 4.0 * covariance_projection;
    let score = distance + summary.trace - summary.population_factor * radicand.max(0.0).sqrt();
    score
        .is_finite()
        .then_some(score)
        .ok_or_else(|| invalid("V36 Gaussian score is nonfinite"))
}

/// Equal-byte six-vector posting summary: one centroid plus five sub-prototypes.
#[derive(Debug, Clone, PartialEq)]
pub struct V36PostingPrototypeSummary {
    population: u32,
    active_subprototypes: u8,
    prototypes: [[f32; 192]; 6],
}

impl V36PostingPrototypeSummary {
    /// Unique primary-row population represented by this summary.
    pub const fn population(&self) -> u32 {
        self.population
    }

    /// Number of active sub-prototypes after the posting centroid.
    pub const fn active_subprototypes(&self) -> u8 {
        self.active_subprototypes
    }

    /// Posting centroid in slot zero.
    pub const fn centroid(&self) -> &[f32; 192] {
        &self.prototypes[0]
    }

    /// All six slots, including centroid padding in inactive slots.
    pub const fn prototypes(&self) -> &[[f32; 192]; 6] {
        &self.prototypes
    }

    /// Raw persisted bytes for exactly six projected vectors.
    pub const fn raw_bytes(&self) -> u64 {
        6 * 192 * 4
    }

    /// Resident bytes including fixed population and activity metadata.
    pub const fn used_bytes(&self) -> u64 {
        self.raw_bytes() + 8
    }

    /// Equal resident charge applied to every posting-score arm.
    pub const fn resident_slot_bytes(&self) -> u64 {
        POSTING_SUMMARY_SLOT_BYTES
    }
}

fn v36_prototype_unit_interval(rng: &mut ChaCha8Rng) -> f64 {
    const TWO_POW_NEG_53: f64 = 1.0 / 9_007_199_254_740_992.0;
    (rng.next_u64() >> 11) as f64 * TWO_POW_NEG_53
}

/// Train the frozen seed-36 prototype-six control over unique primary rows.
pub fn train_v36_posting_prototype_six(
    rows: &[(u64, Vec<f32>)],
) -> Result<V36PostingPrototypeSummary> {
    if rows.is_empty() || rows.len() > u32::MAX as usize {
        return Err(invalid("V36 prototype-six population differs"));
    }
    let mut ordered = rows.iter().collect::<Vec<_>>();
    ordered.sort_unstable_by_key(|(ordinal, _)| *ordinal);
    if ordered.windows(2).any(|pair| pair[0].0 == pair[1].0)
        || ordered.iter().any(|(_, vector)| {
            vector.len() != 192
                || vector
                    .iter()
                    .any(|value| !value.is_finite() || (*value == 0.0 && value.to_bits() != 0))
        })
    {
        return Err(invalid("V36 prototype-six row authority differs"));
    }

    let population = u32::try_from(ordered.len())
        .map_err(|_| invalid("V36 prototype-six population overflows"))?;
    let mut centroid = [0.0_f32; 192];
    for dimension in 0..192 {
        let sum = ordered
            .iter()
            .fold(0.0_f64, |sum, (_, row)| sum + f64::from(row[dimension]));
        let value = (sum / f64::from(population)) as f32;
        centroid[dimension] = if value == 0.0 { 0.0 } else { value };
    }

    let active = ordered.len().min(5);
    let mut selected = Vec::with_capacity(active);
    selected.push(0_usize);
    let mut nearest = ordered
        .iter()
        .map(|(_, row)| squared_l2(row, &ordered[0].1))
        .collect::<Result<Vec<_>>>()?;
    let mut rng = ChaCha8Rng::seed_from_u64(36);
    while selected.len() < active {
        let total = nearest.iter().sum::<f64>();
        let next = if total == 0.0 {
            (0..ordered.len())
                .find(|candidate| !selected.contains(candidate))
                .ok_or_else(|| invalid("V36 prototype-six seed authority differs"))?
        } else {
            let target = v36_prototype_unit_interval(&mut rng) * total;
            let mut cumulative = 0.0_f64;
            nearest
                .iter()
                .enumerate()
                .find_map(|(candidate, distance)| {
                    cumulative += *distance;
                    (cumulative > target && !selected.contains(&candidate)).then_some(candidate)
                })
                .or_else(|| {
                    (0..ordered.len()).rev().find(|candidate| {
                        nearest[*candidate] > 0.0 && !selected.contains(candidate)
                    })
                })
                .ok_or_else(|| invalid("V36 prototype-six draw authority differs"))?
        };
        selected.push(next);
        for (row, nearest_distance) in ordered.iter().zip(&mut nearest) {
            *nearest_distance = nearest_distance.min(squared_l2(&row.1, &ordered[next].1)?);
        }
    }

    let mut subprototypes = selected
        .iter()
        .map(|index| {
            ordered[*index]
                .1
                .clone()
                .try_into()
                .map_err(|_| invalid("V36 prototype-six shape differs"))
        })
        .collect::<Result<Vec<[f32; 192]>>>()?;
    for _ in 0..10 {
        let mut sums = vec![[0.0_f64; 192]; active];
        let mut counts = vec![0_u32; active];
        for (_, row) in &ordered {
            let owner = subprototypes
                .iter()
                .enumerate()
                .map(|(index, prototype)| Ok((squared_l2(row, prototype)?, index)))
                .collect::<Result<Vec<_>>>()?
                .into_iter()
                .min_by(|left, right| {
                    left.0
                        .total_cmp(&right.0)
                        .then_with(|| left.1.cmp(&right.1))
                })
                .map(|(_, index)| index)
                .ok_or_else(|| invalid("V36 prototype-six assignment differs"))?;
            counts[owner] += 1;
            for dimension in 0..192 {
                sums[owner][dimension] += f64::from(row[dimension]);
            }
        }
        for prototype in 0..active {
            if counts[prototype] == 0 {
                continue;
            }
            for dimension in 0..192 {
                let value = (sums[prototype][dimension] / f64::from(counts[prototype])) as f32;
                subprototypes[prototype][dimension] = if value == 0.0 { 0.0 } else { value };
            }
        }
    }

    let mut prototypes = [centroid; 6];
    prototypes[1..=active].copy_from_slice(&subprototypes);
    Ok(V36PostingPrototypeSummary {
        population,
        active_subprototypes: active as u8,
        prototypes,
    })
}

/// Score a query by the nearest active vector in the prototype-six summary.
pub fn score_v36_posting_prototype_six(
    summary: &V36PostingPrototypeSummary,
    query: &[f32],
) -> Result<f64> {
    let mut best = f64::INFINITY;
    for prototype in &summary.prototypes[..=usize::from(summary.active_subprototypes)] {
        best = best.min(squared_l2(prototype, query)?);
    }
    best.is_finite()
        .then_some(best)
        .ok_or_else(|| invalid("V36 prototype-six score differs"))
}

/// Frozen candidate-count ladder for posting-summary accelerator qualification.
pub const V36_POSTING_ACCELERATOR_EF_LADDER: [u32; 5] = [64, 128, 256, 512, 1_024];

/// Candidate generator measured by one posting-summary accelerator observation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum V36PostingAcceleratorKind {
    /// Bounded flat centroid squared-L2 scan followed by exact selected-score rerank.
    SimdFlatCentroid,
    /// HNSW traversed with centroid squared-L2 followed by exact selected-score rerank.
    HnswCentroid,
    /// HNSW traversed with the selected scorer followed by an exact rerank.
    HnswSelectedScore,
}

/// One exactly recomputable accelerator qualification observation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct V36PostingAcceleratorObservation {
    /// Candidate-generation strategy that produced this observation.
    pub kind: V36PostingAcceleratorKind,
    /// Authenticated number of posting summaries in the measured population.
    pub posting_count: u32,
    /// Exact selected-score prefix length requested by the causal boundary.
    pub requested_prefix_length: u32,
    /// Effective candidate/search width used for this ladder rung.
    pub ef_search: u32,
    /// Query-prefix pairs whose ordered prefix exactly matched the authority.
    pub prefix_matches: u64,
    /// Complete registered query-prefix-pair denominator.
    pub query_prefix_pairs: u64,
    /// Floor-rounded exact-prefix agreement in parts per million.
    pub parity_ppm: u32,
    /// Graph or flat-scan nodes visited by candidate generation.
    pub visited_nodes: u64,
    /// Distance evaluations performed only to generate candidates.
    pub candidate_generation_score_evaluations: u64,
    /// Exact selected-score evaluations used to rerank candidates.
    pub exact_rerank_score_evaluations: u64,
    /// Selected-score evaluations used by the exhaustive authority.
    pub exhaustive_score_evaluations: u64,
    /// Exact allocated capacity charged to this accelerator.
    pub allocated_bytes: u64,
    /// Decoded-hot p99 of candidate generation plus exact reranking.
    pub accelerated_decoded_hot_p99_ns: u64,
    /// Decoded-hot p99 of exhaustive exact selected scoring.
    pub exhaustive_decoded_hot_p99_ns: u64,
    /// Stored decision, which validation independently recomputes.
    pub qualified: bool,
}

/// One posting with its canonical exact selected score.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct V36RankedPosting {
    /// Dense ordinal of the posting summary.
    pub posting_ordinal: u32,
    /// Canonical finite binary64 selected score.
    pub score: f64,
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct V36FlatCentroidCandidate {
    distance: f32,
    posting_ordinal: u32,
}

impl Eq for V36FlatCentroidCandidate {}

impl Ord for V36FlatCentroidCandidate {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.distance
            .total_cmp(&other.distance)
            .then_with(|| self.posting_ordinal.cmp(&other.posting_ordinal))
    }
}

impl PartialOrd for V36FlatCentroidCandidate {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// Whether the posting count requires an accelerator qualification result.
pub fn v36_posting_acceleration_required(posting_count: u32) -> Result<bool> {
    if posting_count == 0 {
        return Err(invalid("V36 posting accelerator population differs"));
    }
    Ok(posting_count > 1_024)
}

/// Resolve one frozen `efSearch=max(L,e)` ladder rung.
pub fn v36_effective_ef_search(prefix_length: u32, ladder_rung: u32) -> Result<u32> {
    if prefix_length == 0 || !V36_POSTING_ACCELERATOR_EF_LADDER.contains(&ladder_rung) {
        return Err(invalid("V36 posting accelerator search authority differs"));
    }
    Ok(prefix_length.max(ladder_rung))
}

/// Select a bounded centroid-L2 candidate set without a population-sized rank buffer.
pub fn select_v36_flat_centroid_candidates(
    centroids: &[Vec<f32>],
    query: &[f32],
    candidate_count: u32,
) -> Result<Vec<u32>> {
    let candidate_count = usize::try_from(candidate_count)
        .map_err(|_| invalid("V36 flat centroid candidate count overflows"))?;
    if centroids.is_empty()
        || candidate_count == 0
        || candidate_count > centroids.len()
        || invalid_v36_vector(query)
        || centroids
            .iter()
            .any(|centroid| invalid_v36_vector(centroid))
    {
        return Err(invalid("V36 flat centroid candidate authority differs"));
    }

    let mut best = BinaryHeap::with_capacity(candidate_count);
    for (posting_ordinal, centroid) in centroids.iter().enumerate() {
        let distance = crate::metric::squared_euclidean_simd(centroid, query);
        if !distance.is_finite() {
            return Err(invalid("V36 flat centroid distance is nonfinite"));
        }
        let candidate = V36FlatCentroidCandidate {
            distance: if distance == 0.0 { 0.0 } else { distance },
            posting_ordinal: u32::try_from(posting_ordinal)
                .map_err(|_| invalid("V36 flat centroid ordinal overflows"))?,
        };
        if best.len() < candidate_count {
            best.push(candidate);
        } else if best.peek().is_some_and(|farthest| candidate < *farthest) {
            best.pop();
            best.push(candidate);
        }
    }
    let mut ranked = best.into_vec();
    ranked.sort_unstable();
    Ok(ranked
        .into_iter()
        .map(|candidate| candidate.posting_ordinal)
        .collect())
}

/// Rescore and order a bounded candidate set with the exact selected scorer.
pub fn rank_v36_selected_posting_candidates<F>(
    candidates: &[u32],
    posting_count: u32,
    prefix_length: u32,
    mut selected_score: F,
) -> Result<Vec<V36RankedPosting>>
where
    F: FnMut(u32) -> Result<f64>,
{
    let prefix_length = usize::try_from(prefix_length)
        .map_err(|_| invalid("V36 posting accelerator prefix overflows"))?;
    if posting_count == 0 || prefix_length == 0 || prefix_length > candidates.len() {
        return Err(invalid(
            "V36 posting accelerator candidate authority differs",
        ));
    }
    let mut unique = candidates.to_vec();
    unique.sort_unstable();
    if unique.windows(2).any(|pair| pair[0] == pair[1])
        || unique.iter().any(|ordinal| *ordinal >= posting_count)
    {
        return Err(invalid(
            "V36 posting accelerator candidate authority differs",
        ));
    }

    let mut ranked = candidates
        .iter()
        .map(|posting_ordinal| {
            let score = selected_score(*posting_ordinal)?;
            if !score.is_finite() {
                return Err(invalid("V36 posting accelerator score is nonfinite"));
            }
            Ok(V36RankedPosting {
                posting_ordinal: *posting_ordinal,
                score: if score == 0.0 { 0.0 } else { score },
            })
        })
        .collect::<Result<Vec<_>>>()?;
    ranked.sort_unstable_by(|left, right| {
        left.score
            .total_cmp(&right.score)
            .then_with(|| left.posting_ordinal.cmp(&right.posting_ordinal))
    });
    ranked.truncate(prefix_length);
    Ok(ranked)
}

/// Validate and recompute one accelerator qualification decision.
pub fn validate_v36_posting_accelerator_observation(
    observation: &V36PostingAcceleratorObservation,
) -> Result<bool> {
    if !v36_posting_acceleration_required(observation.posting_count)?
        || observation.requested_prefix_length == 0
        || observation.requested_prefix_length > observation.posting_count
        || !V36_POSTING_ACCELERATOR_EF_LADDER
            .iter()
            .any(|rung| observation.requested_prefix_length.max(*rung) == observation.ef_search)
        || observation.query_prefix_pairs == 0
        || observation.prefix_matches > observation.query_prefix_pairs
        || observation.visited_nodes == 0
        || observation.candidate_generation_score_evaluations == 0
        || observation.exact_rerank_score_evaluations == 0
        || observation.exhaustive_score_evaluations == 0
        || observation.allocated_bytes == 0
        || observation.accelerated_decoded_hot_p99_ns == 0
        || observation.exhaustive_decoded_hot_p99_ns == 0
    {
        return Err(invalid("V36 posting accelerator observation differs"));
    }
    let parity_ppm = observation
        .prefix_matches
        .checked_mul(1_000_000)
        .ok_or_else(|| invalid("V36 posting accelerator parity overflows"))?
        / observation.query_prefix_pairs;
    if parity_ppm != u64::from(observation.parity_ppm) {
        return Err(invalid("V36 posting accelerator parity differs"));
    }
    let score_evaluations = observation
        .candidate_generation_score_evaluations
        .checked_add(observation.exact_rerank_score_evaluations)
        .ok_or_else(|| invalid("V36 posting accelerator work overflows"))?;
    let memory_passes = observation.kind == V36PostingAcceleratorKind::SimdFlatCentroid
        || observation.allocated_bytes <= 128 * 1024 * 1024;
    let qualified = observation.parity_ppm >= 999_000
        && score_evaluations < observation.exhaustive_score_evaluations
        && observation.accelerated_decoded_hot_p99_ns < observation.exhaustive_decoded_hot_p99_ns
        && memory_passes;
    if observation.qualified != qualified {
        return Err(invalid("V36 posting accelerator decision differs"));
    }
    Ok(qualified)
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

/// Flat, source-ordinal-ordered ownership and occupancy evidence for one V36
/// posting geometry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct V36PostingAssignments {
    source_ordinals: Vec<u64>,
    owner_offsets: Vec<u64>,
    owners: Vec<u32>,
    primary_occupancy: Vec<u64>,
    stored_occupancy: Vec<u64>,
    admission: V36GeometryAdmission,
}

impl V36PostingAssignments {
    /// Unique source ordinals in canonical row order.
    pub fn source_ordinals(&self) -> &[u64] {
        &self.source_ordinals
    }

    /// CSR offsets into [`Self::owners`], including one terminal offset.
    pub fn owner_offsets(&self) -> &[u64] {
        &self.owner_offsets
    }

    /// Primary-first posting owners, with at most eight entries per row.
    pub fn owners(&self) -> &[u32] {
        &self.owners
    }

    /// Unique-primary occupancy for every posting.
    pub fn primary_occupancy(&self) -> &[u64] {
        &self.primary_occupancy
    }

    /// Primary-plus-closure occupancy for every posting.
    pub fn stored_occupancy(&self) -> &[u64] {
        &self.stored_occupancy
    }

    /// Exact construction-side admission evidence.
    pub const fn admission(&self) -> &V36GeometryAdmission {
        &self.admission
    }
}

#[derive(Clone, Copy)]
struct V36RowOwners {
    len: u8,
    owners: [u32; 8],
}

/// Assign a projected population to exact primary and optional closure owners
/// with byte-identical output across worker and block counts.
pub fn assign_v36_postings(
    rows: &[(u64, Vec<f32>)],
    centroids: &[Vec<f32>],
    closure_epsilon: Option<f64>,
    worker_threads: usize,
    block_rows: usize,
    target_primary_rows: u64,
) -> Result<V36PostingAssignments> {
    if rows.is_empty()
        || centroids.is_empty()
        || centroids.len() > u32::MAX as usize
        || worker_threads == 0
        || worker_threads > 64
        || block_rows == 0
        || block_rows > 65_536
        || target_primary_rows == 0
        || closure_epsilon.is_some_and(|epsilon| !matches!(epsilon, 0.05 | 0.15 | 0.30))
    {
        return Err(invalid("V36 population assignment authority differs"));
    }
    let row_count = u64::try_from(rows.len())
        .map_err(|_| invalid("V36 population assignment row count overflows"))?;
    let centroid_count = u64::try_from(centroids.len())
        .map_err(|_| invalid("V36 population assignment posting count overflows"))?;
    if row_count.div_ceil(target_primary_rows) != centroid_count {
        return Err(invalid("V36 population assignment geometry differs"));
    }
    let mut ordered = rows.iter().collect::<Vec<_>>();
    ordered.sort_unstable_by_key(|(ordinal, _)| *ordinal);
    if ordered.windows(2).any(|pair| pair[0].0 == pair[1].0)
        || ordered.iter().any(|(_, vector)| invalid_v36_vector(vector))
        || centroids
            .iter()
            .any(|centroid| invalid_v36_vector(centroid))
    {
        return Err(invalid("V36 population assignment vector differs"));
    }

    let pool = ThreadPoolBuilder::new()
        .num_threads(worker_threads)
        .build()
        .map_err(|_| invalid("V36 population assignment workers differ"))?;
    let mut row_owners = vec![
        V36RowOwners {
            len: 0,
            owners: [0; 8]
        };
        ordered.len()
    ];
    pool.install(|| {
        ordered
            .par_chunks(block_rows)
            .zip(row_owners.par_chunks_mut(block_rows))
            .try_for_each(|(input, output)| {
                for ((_, row), output) in input.iter().zip(output) {
                    let selected = if let Some(epsilon) = closure_epsilon {
                        select_v36_closure_owners(row, centroids, epsilon, 8)?
                    } else {
                        let mut best = (squared_l2(row, &centroids[0])?, 0_usize);
                        for (ordinal, centroid) in centroids.iter().enumerate().skip(1) {
                            let distance = squared_l2(row, centroid)?;
                            if distance < best.0 {
                                best = (distance, ordinal);
                            }
                        }
                        vec![
                            u32::try_from(best.1)
                                .map_err(|_| invalid("V36 posting ordinal overflows"))?,
                        ]
                    };
                    let len = u8::try_from(selected.len())
                        .map_err(|_| invalid("V36 owner count overflows"))?;
                    output.len = len;
                    output.owners[..usize::from(len)].copy_from_slice(&selected);
                }
                Ok::<(), BorsukError>(())
            })
    })?;

    let source_ordinals = ordered.iter().map(|row| row.0).collect::<Vec<_>>();
    let mut owner_offsets = Vec::with_capacity(ordered.len() + 1);
    let owner_count = row_owners.iter().try_fold(0_usize, |sum, row| {
        sum.checked_add(usize::from(row.len))
            .ok_or_else(|| invalid("V36 owner count overflows"))
    })?;
    let mut owners = Vec::with_capacity(owner_count);
    owner_offsets.push(0);
    for row in row_owners {
        owners.extend_from_slice(&row.owners[..usize::from(row.len)]);
        owner_offsets
            .push(u64::try_from(owners.len()).map_err(|_| invalid("V36 owner count overflows"))?);
    }

    let mut primary_occupancy = vec![0_u64; centroids.len()];
    let mut stored_occupancy = vec![0_u64; centroids.len()];
    for offsets in owner_offsets.windows(2) {
        let start =
            usize::try_from(offsets[0]).map_err(|_| invalid("V36 owner offset overflows"))?;
        let end = usize::try_from(offsets[1]).map_err(|_| invalid("V36 owner offset overflows"))?;
        let row_owners = owners
            .get(start..end)
            .filter(|row_owners| !row_owners.is_empty())
            .ok_or_else(|| invalid("V36 owner offsets differ"))?;
        let primary =
            usize::try_from(row_owners[0]).map_err(|_| invalid("V36 posting ordinal overflows"))?;
        primary_occupancy[primary] = primary_occupancy[primary]
            .checked_add(1)
            .ok_or_else(|| invalid("V36 primary occupancy overflows"))?;
        for owner in row_owners {
            let owner =
                usize::try_from(*owner).map_err(|_| invalid("V36 posting ordinal overflows"))?;
            stored_occupancy[owner] = stored_occupancy[owner]
                .checked_add(1)
                .ok_or_else(|| invalid("V36 stored occupancy overflows"))?;
        }
    }
    let admission = admit_v36_geometry(&primary_occupancy, &stored_occupancy, target_primary_rows)?;
    Ok(V36PostingAssignments {
        source_ordinals,
        owner_offsets,
        owners,
        primary_occupancy,
        stored_occupancy,
        admission,
    })
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
            .any(|(primary, stored)| stored < primary)
    {
        return Err(invalid("V36 geometry occupancy authority differs"));
    }
    let primary_rows = primary_occupancy.iter().try_fold(0_u64, |sum, rows| {
        sum.checked_add(*rows)
            .ok_or_else(|| invalid("V36 geometry primary rows overflow"))
    })?;
    if primary_rows == 0 {
        return Err(invalid("V36 geometry primary population is empty"));
    }
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

#[cfg(test)]
mod tests {
    use super::{V36RowOwners, repair_v36_empty_posting_assignments};

    #[test]
    fn v36_row_owner_scratch_is_one_fixed_inline_record() {
        assert!(std::mem::size_of::<V36RowOwners>() <= 36);
    }

    #[test]
    fn v36_empty_posting_repair_uses_farthest_donor_then_source_ordinal() {
        let vector = vec![0.0_f32; 192];
        let rows = [
            (10_u64, vector.clone()),
            (20_u64, vector.clone()),
            (30_u64, vector.clone()),
            (40_u64, vector),
        ];
        let ordered = rows.iter().collect::<Vec<_>>();
        let mut assignments = vec![0_usize, 0, 0, 1];
        let distances = [9.0_f64, 9.0, 1.0, 100.0];
        let mut counts = vec![3_usize, 1, 0, 0];

        repair_v36_empty_posting_assignments(&ordered, &mut assignments, &distances, &mut counts)
            .unwrap();

        assert_eq!(assignments, [2, 3, 0, 1]);
        assert_eq!(counts, [1, 1, 1, 1]);
    }
}
