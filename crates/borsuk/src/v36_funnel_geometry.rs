//! Deterministic posting geometry for the V36 qualification funnel.

use std::{
    collections::{BinaryHeap, HashMap, HashSet},
    io::Cursor,
    sync::Arc,
};

use crate::{
    BorsukError, Result, V35Dimensions, V36ArtifactIdentity,
    v35_patch::deterministic_ln_u32,
    v35_projection::{V35Projection, V35ProjectionBackend, build_v35_srht},
    v36_funnel::POSTING_SUMMARY_SLOT_BYTES,
};
use arrow_array::{
    Array, FixedSizeListArray, Float32Array, Float64Array, ListArray, RecordBatch, UInt32Array,
    UInt64Array,
};
use arrow_buffer::OffsetBuffer;
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
const SUPERCELL_MODEL_FORMAT: &str = "borsuk-v36-supercell-model-arrow-v1";
const SUPERCELL_MODEL_ALGORITHM: &str = "sha-reservoir-farthest-first-lloyd25-v1";
const SUPERCELL_MODEL_ROLE: &str = "supercell-model";
const SUPERCELL_MODEL_MANIFEST_KEY: &str = "borsuk.v36.supercell_model.manifest";
const SUPERCELL_MODEL_MAXIMUM_ENCODED_BYTES: u64 = 16 * 1_048_576;

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

/// One bounded projected-corpus block consumed during V36 geometry construction.
pub type V36ProjectedCorpusBlockVisitor<'a> = dyn FnMut(&[u64], &[f32]) -> Result<()> + 'a;

/// Restartable query-blind source of projected corpus rows.
pub trait V36ProjectedCorpusSource {
    /// Scan every projected row once in strictly increasing source-ordinal order.
    fn scan(&mut self, visitor: &mut V36ProjectedCorpusBlockVisitor<'_>) -> Result<()>;
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
/// Exact bounded authority for deterministic V36 super-cell training.
pub struct V36SupercellTrainingSpec {
    /// Exact query-excluded corpus population.
    pub corpus_rows: u64,
    /// Projected vector dimensions; V36 fixes this to 192.
    pub dimensions: usize,
    /// Hard row cap for every source callback.
    pub maximum_block_rows: usize,
    /// Registered digest of exact projected `(ordinal, f32 bits)` replay bytes.
    pub projected_corpus_sha256: String,
    /// Query-independent SHA-ranked reservoir population.
    pub reservoir_rows: u64,
    /// Number of construction-only super cells.
    pub super_cell_count: u32,
}

/// Bind the registered dimension-independent V36 super-cell training shape.
pub fn bind_v36_registered_supercell_training_spec(
    corpus_rows: u64,
    maximum_block_rows: usize,
    projected_corpus_sha256: &str,
) -> Result<V36SupercellTrainingSpec> {
    if corpus_rows == 0
        || maximum_block_rows == 0
        || maximum_block_rows > 65_536
        || !valid_sha256(projected_corpus_sha256)
    {
        return Err(invalid("V36 registered super-cell training spec differs"));
    }
    let super_cells = corpus_rows
        .div_ceil(262_144)
        .checked_next_power_of_two()
        .ok_or_else(|| invalid("V36 registered super-cell count overflows"))?
        .min(4_096);
    Ok(V36SupercellTrainingSpec {
        corpus_rows,
        dimensions: 192,
        maximum_block_rows,
        projected_corpus_sha256: projected_corpus_sha256.to_owned(),
        reservoir_rows: corpus_rows.min(1_048_576),
        super_cell_count: u32::try_from(super_cells)
            .map_err(|_| invalid("V36 registered super-cell count overflows"))?,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Checked work and trainer-owned memory projection for V36 super-cell training.
pub struct V36SupercellTrainingPreflight {
    /// Exact scalar component terms across initialization and 25 Lloyd passes.
    pub component_terms: u128,
    /// Ceiling-scaled active time from the registered measured kernel sample.
    pub projected_active_ns: u128,
    /// Maximum bytes simultaneously owned by the trainer's largest phase.
    pub trainer_owned_peak_bytes: u64,
    /// Whether the projected active time is inside the registered wall cap.
    pub within_active_wall_cap: bool,
}

/// Project complete V36 super-cell work from a bounded measured kernel sample.
pub fn project_v36_supercell_training_preflight(
    spec: &V36SupercellTrainingSpec,
    measured_component_terms: u128,
    measured_elapsed_ns: u64,
    maximum_active_wall_seconds: u64,
) -> Result<V36SupercellTrainingPreflight> {
    if spec.corpus_rows == 0
        || spec.dimensions != 192
        || spec.maximum_block_rows == 0
        || spec.maximum_block_rows > 65_536
        || !valid_sha256(&spec.projected_corpus_sha256)
        || spec.reservoir_rows == 0
        || spec.reservoir_rows > spec.corpus_rows
        || spec.reservoir_rows > 1_048_576
        || spec.super_cell_count == 0
        || !spec.super_cell_count.is_power_of_two()
        || spec.super_cell_count > 4_096
        || u64::from(spec.super_cell_count) > spec.reservoir_rows
        || measured_component_terms == 0
        || measured_elapsed_ns == 0
        || maximum_active_wall_seconds == 0
    {
        return Err(invalid("V36 super-cell preflight authority differs"));
    }
    let rows = u128::from(spec.reservoir_rows);
    let cells = u128::from(spec.super_cell_count);
    let initialization_distances = cells
        .checked_sub(1)
        .and_then(|iterations| iterations.checked_mul(rows))
        .and_then(|value| {
            cells
                .checked_mul(cells - 1)
                .and_then(|selected| value.checked_sub(selected / 2))
        })
        .ok_or_else(|| invalid("V36 super-cell initialization work overflows"))?;
    let lloyd_distances = 25_u128
        .checked_mul(rows)
        .and_then(|value| value.checked_mul(cells))
        .ok_or_else(|| invalid("V36 super-cell Lloyd work overflows"))?;
    let component_terms = initialization_distances
        .checked_add(lloyd_distances)
        .and_then(|value| value.checked_mul(192))
        .ok_or_else(|| invalid("V36 super-cell training work overflows"))?;
    let projected_active_ns = component_terms
        .checked_mul(u128::from(measured_elapsed_ns))
        .ok_or_else(|| invalid("V36 super-cell projected time overflows"))?
        .div_ceil(measured_component_terms);
    let maximum_active_ns = u128::from(maximum_active_wall_seconds)
        .checked_mul(1_000_000_000)
        .ok_or_else(|| invalid("V36 super-cell active wall cap overflows"))?;

    let rows = u64::try_from(rows).map_err(|_| invalid("V36 super-cell resident rows overflow"))?;
    let cells = u64::from(spec.super_cell_count);
    let block_rows = u64::try_from(spec.maximum_block_rows)
        .map_err(|_| invalid("V36 super-cell block rows overflow"))?;
    let block_bytes = block_rows
        .checked_mul(192 * 4 + 8)
        .ok_or_else(|| invalid("V36 super-cell block bytes overflow"))?;
    let selection_phase = rows
        .checked_mul(32 + 8)
        .and_then(|value| value.checked_add(block_bytes))
        .ok_or_else(|| invalid("V36 super-cell selection memory overflows"))?;
    let centroid_bytes = cells
        .checked_mul(8 + 192 * 4 + 8 + 192 * 8)
        .ok_or_else(|| invalid("V36 super-cell centroid memory overflows"))?;
    let training_phase = rows
        .checked_mul(8 + 192 * 4 + 1 + 8 + 8 + 8)
        .and_then(|value| value.checked_add(centroid_bytes))
        .and_then(|value| value.checked_add(block_bytes))
        .ok_or_else(|| invalid("V36 super-cell training memory overflows"))?;
    Ok(V36SupercellTrainingPreflight {
        component_terms,
        projected_active_ns,
        trainer_owned_peak_bytes: selection_phase.max(training_phase),
        within_active_wall_cap: projected_active_ns <= maximum_active_ns,
    })
}

const V36_EXTERNAL_ASSIGNMENT_SHARD_ROWS: u64 = 65_536;
const V36_EXTERNAL_ASSIGNMENT_ROW_BYTES: u64 = 4 + 8 + 192 * 4;
const V36_EXTERNAL_ASSIGNMENT_SHARD_ENVELOPE_BYTES: u64 = 2 * 1_048_576;
const V36_EXTERNAL_ASSIGNMENT_FORMAT: &str = "borsuk-v36-supercell-assignment-arrow-v1";
const V36_EXTERNAL_ASSIGNMENT_ROLE: &str = "supercell-assignment-shard";
const V36_EXTERNAL_ASSIGNMENT_MANIFEST_KEY: &str = "borsuk.v36.supercell_assignment.manifest";
const V36_EXTERNAL_ASSIGNMENT_MAXIMUM_ENCODED_BYTES: u64 = 64 * 1_048_576;
const V36_EXTERNAL_ASSIGNMENT_ROOT_FORMAT: &str = "borsuk-v36-supercell-assignment-root-v1";
const V36_EXTERNAL_ASSIGNMENT_ROOT_ROLE: &str = "supercell-assignment-root";
const V36_EXTERNAL_ASSIGNMENT_ROOT_MAXIMUM_ENCODED_BYTES: u64 = 16 * 1_048_576;
const V36_INITIAL_ASSIGNMENT_MERGE_CHUNK_FORMAT: &str =
    "borsuk-v36-initial-assignment-merge-chunk-arrow-v1";
const V36_INITIAL_ASSIGNMENT_MERGE_CHUNK_ROLE: &str = "initial-assignment-merge-chunk";
const V36_INITIAL_ASSIGNMENT_MERGE_CHUNK_MANIFEST_KEY: &str =
    "borsuk.v36.initial_assignment_merge_chunk.manifest";
const V36_INITIAL_ASSIGNMENT_MERGE_ROOT_FORMAT: &str =
    "borsuk-v36-initial-assignment-merge-root-v1";
const V36_INITIAL_ASSIGNMENT_MERGE_ROOT_ROLE: &str = "initial-assignment-merge-root";
const V36_FOLLOWUP_ASSIGNMENT_MERGE_ROOT_FORMAT: &str =
    "borsuk-v36-followup-assignment-merge-root-v1";
const V36_FOLLOWUP_ASSIGNMENT_MERGE_ROOT_ROLE: &str = "followup-assignment-merge-root";
// Covers the maximum admitted 64-input/64-chunk manifest even when every URI
// reaches the shared 4,096-byte URI ceiling and every byte needs six-byte JSON
// escaping, while retaining a finite bound before untrusted parsing.
const V36_INITIAL_ASSIGNMENT_MERGE_ROOT_MAXIMUM_ENCODED_BYTES: u64 = 16 * 1_048_576;
const V36_INITIAL_ASSIGNMENT_MERGE_GENERATION_ROOT_FORMAT: &str =
    "borsuk-v36-initial-assignment-merge-generation-root-v1";
const V36_INITIAL_ASSIGNMENT_MERGE_GENERATION_ROOT_ROLE: &str =
    "initial-assignment-merge-generation-root";
const V36_INITIAL_ASSIGNMENT_MERGE_GENERATION_ROOT_MAXIMUM_ENCODED_BYTES: u64 = 1_048_576;
const V36_INITIAL_ASSIGNMENT_MERGE_GENERATION_RUN_INVENTORY_DOMAIN: &[u8] =
    b"borsuk-v36-initial-assignment-merge-generation-run-inventory-v1\0";
const V36_SUPERCELL_RUN_CHUNK_FORMAT: &str = "borsuk-v36-supercell-run-chunk-arrow-v1";
const V36_SUPERCELL_RUN_CHUNK_ROLE: &str = "supercell-run-chunk";
const V36_SUPERCELL_RUN_CHUNK_MANIFEST_KEY: &str = "borsuk.v36.supercell_run_chunk.manifest";
const V36_SUPERCELL_RUN_ROOT_FORMAT: &str = "borsuk-v36-supercell-run-root-v1";
const V36_SUPERCELL_RUN_ROOT_ROLE: &str = "supercell-run-root";
const V36_SUPERCELL_RUN_ROOT_MAXIMUM_ENCODED_BYTES: u64 = 1_048_576;

#[derive(Debug, Clone, PartialEq)]
/// One provisional external-assignment row sorted by super-cell then source.
pub struct V36SupercellAssignmentRow {
    supercell_ordinal: u32,
    source_ordinal: u64,
    projected: [f32; 192],
}

impl V36SupercellAssignmentRow {
    /// Construct one finite projected assignment row.
    pub fn new(supercell_ordinal: u32, source_ordinal: u64, projected: [f32; 192]) -> Result<Self> {
        if projected
            .iter()
            .any(|value| !value.is_finite() || (*value == 0.0 && value.is_sign_negative()))
        {
            return Err(invalid("V36 assignment row is nonfinite"));
        }
        Ok(Self {
            supercell_ordinal,
            source_ordinal,
            projected,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Complete authority embedded in one provisional assignment shard.
pub struct V36SupercellAssignmentShardContext {
    /// Exact authenticated super-cell model object.
    model_identity: V36ArtifactIdentity,
    /// Exact projected corpus replay digest.
    projected_corpus_sha256: String,
    /// Consecutive logical source shard ordinal.
    shard_ordinal: u64,
    /// Exact model training and corpus authority.
    training_spec: V36SupercellTrainingSpec,
    /// Immutable assignment shard URI.
    uri: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct V36SupercellAssignmentShardContextWire {
    model_identity: V36ArtifactIdentity,
    projected_corpus_sha256: String,
    shard_ordinal: u64,
    training_spec: V36SupercellTrainingSpec,
    uri: String,
}

impl From<&V36SupercellAssignmentShardContext> for V36SupercellAssignmentShardContextWire {
    fn from(context: &V36SupercellAssignmentShardContext) -> Self {
        Self {
            model_identity: context.model_identity.clone(),
            projected_corpus_sha256: context.projected_corpus_sha256.clone(),
            shard_ordinal: context.shard_ordinal,
            training_spec: context.training_spec.clone(),
            uri: context.uri.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Complete identity and authority for one assignment shard.
pub struct V36SupercellAssignmentShardArtifact {
    /// Complete-object BLAKE3.
    pub blake3: String,
    /// Embedded corpus/model/shard authority.
    pub context: V36SupercellAssignmentShardContext,
    /// Complete encoded length.
    pub encoded_bytes: u64,
    /// Complete logical row count.
    pub row_count: u32,
    /// Complete-object SHA-256.
    pub sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct V36SupercellAssignmentShardManifest {
    context: V36SupercellAssignmentShardContextWire,
    format: String,
    role: String,
    row_count: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct V36SupercellAssignmentShardArtifactWire {
    blake3: String,
    context: V36SupercellAssignmentShardContextWire,
    encoded_bytes: u64,
    row_count: u32,
    sha256: String,
}

impl From<&V36SupercellAssignmentShardArtifact> for V36SupercellAssignmentShardArtifactWire {
    fn from(artifact: &V36SupercellAssignmentShardArtifact) -> Self {
        Self {
            blake3: artifact.blake3.clone(),
            context: (&artifact.context).into(),
            encoded_bytes: artifact.encoded_bytes,
            row_count: artifact.row_count,
            sha256: artifact.sha256.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct V36CommittedSupercellAssignmentsManifest {
    artifacts: Vec<V36SupercellAssignmentShardArtifactWire>,
    format: String,
    merge_fan_in: u8,
    merge_schedule: Vec<V36ExternalMergeGenerationProjection>,
    model_identity: V36ArtifactIdentity,
    projected_corpus_sha256: String,
    role: String,
    training_spec: V36SupercellTrainingSpec,
    uri_prefix: String,
}

/// Bind one assignment shard to a previously authenticated model handle.
pub fn bind_v36_supercell_assignment_shard_context(
    model: &V36AuthenticatedSupercellModel,
    shard_ordinal: u64,
    uri: &str,
) -> Result<V36SupercellAssignmentShardContext> {
    let context = V36SupercellAssignmentShardContext {
        model_identity: model.identity().clone(),
        projected_corpus_sha256: model.training_spec().projected_corpus_sha256.clone(),
        shard_ordinal,
        training_spec: model.training_spec().clone(),
        uri: uri.to_owned(),
    };
    validate_v36_assignment_context(&context)?;
    Ok(context)
}

fn validate_v36_assignment_context(context: &V36SupercellAssignmentShardContext) -> Result<()> {
    let identity = &context.model_identity;
    let shard_count = context
        .training_spec
        .corpus_rows
        .div_ceil(V36_EXTERNAL_ASSIGNMENT_SHARD_ROWS);
    if context.projected_corpus_sha256 != context.training_spec.projected_corpus_sha256
        || !valid_sha256(&context.projected_corpus_sha256)
        || context.shard_ordinal >= shard_count
        || identity.role != SUPERCELL_MODEL_ROLE
        || identity.encoded_bytes == 0
        || identity.encoded_bytes > SUPERCELL_MODEL_MAXIMUM_ENCODED_BYTES
        || !valid_sha256(&identity.sha256)
        || !valid_sha256(&identity.blake3)
        || !valid_v36_supercell_model_uri(&identity.uri)
        || !valid_v36_supercell_model_uri(&context.uri)
    {
        return Err(invalid("V36 assignment shard context differs"));
    }
    Ok(())
}

fn validate_v36_assignment_rows(
    context: &V36SupercellAssignmentShardContext,
    rows: &[V36SupercellAssignmentRow],
) -> Result<()> {
    validate_v36_assignment_context(context)?;
    let start = context
        .shard_ordinal
        .checked_mul(V36_EXTERNAL_ASSIGNMENT_SHARD_ROWS)
        .ok_or_else(|| invalid("V36 assignment shard range overflows"))?;
    let end = start
        .checked_add(V36_EXTERNAL_ASSIGNMENT_SHARD_ROWS)
        .map(|end| end.min(context.training_spec.corpus_rows))
        .ok_or_else(|| invalid("V36 assignment shard range overflows"))?;
    let expected_rows = usize::try_from(end - start)
        .map_err(|_| invalid("V36 assignment shard row count overflows"))?;
    if rows.len() != expected_rows {
        return Err(invalid("V36 assignment shard row count differs"));
    }
    let mut seen = vec![false; expected_rows];
    let mut previous = None;
    for row in rows {
        let key = (row.supercell_ordinal, row.source_ordinal);
        if previous.is_some_and(|prior| prior >= key)
            || row.supercell_ordinal >= context.training_spec.super_cell_count
            || row.source_ordinal < start
            || row.source_ordinal >= end
            || row
                .projected
                .iter()
                .any(|value| !value.is_finite() || (*value == 0.0 && value.is_sign_negative()))
        {
            return Err(invalid("V36 assignment shard rows differ"));
        }
        let offset = usize::try_from(row.source_ordinal - start)
            .map_err(|_| invalid("V36 assignment shard source offset overflows"))?;
        if std::mem::replace(&mut seen[offset], true) {
            return Err(invalid("V36 assignment shard source coverage differs"));
        }
        previous = Some(key);
    }
    if seen.iter().any(|seen| !seen) {
        return Err(invalid("V36 assignment shard source coverage differs"));
    }
    Ok(())
}

fn v36_assignment_schema(manifest_key: &str, manifest: String) -> Schema {
    let mut metadata = HashMap::new();
    metadata.insert(manifest_key.to_owned(), manifest);
    Schema::new_with_metadata(
        vec![
            Field::new("supercell_ordinal", DataType::UInt32, false),
            Field::new("source_ordinal", DataType::UInt64, false),
            Field::new(
                "projected",
                DataType::FixedSizeList(
                    Arc::new(Field::new("element", DataType::Float32, false)),
                    192,
                ),
                false,
            ),
        ],
        metadata,
    )
}

fn validate_v36_assignment_ipc_field(field: arrow_ipc::Field<'_>, expected: &Field) -> Result<()> {
    if field.name() != Some(expected.name().as_str())
        || field.nullable()
        || field.dictionary().is_some()
        || field
            .custom_metadata()
            .is_some_and(|values| !values.is_empty())
    {
        return Err(invalid("V36 assignment shard IPC field differs"));
    }
    let expected_children = match expected.data_type() {
        DataType::UInt32 => {
            if field
                .type_as_int()
                .is_none_or(|value| value.bitWidth() != 32 || value.is_signed())
            {
                return Err(invalid("V36 assignment shard IPC integer differs"));
            }
            Vec::new()
        }
        DataType::UInt64 => {
            if field
                .type_as_int()
                .is_none_or(|value| value.bitWidth() != 64 || value.is_signed())
            {
                return Err(invalid("V36 assignment shard IPC integer differs"));
            }
            Vec::new()
        }
        DataType::Float32 => {
            if field
                .type_as_floating_point()
                .is_none_or(|value| value.precision() != arrow_ipc::Precision::SINGLE)
            {
                return Err(invalid("V36 assignment shard IPC float differs"));
            }
            Vec::new()
        }
        DataType::FixedSizeList(child, width) => {
            if field
                .type_as_fixed_size_list()
                .is_none_or(|value| value.listSize() != *width)
            {
                return Err(invalid("V36 assignment shard IPC fixed list differs"));
            }
            vec![child.as_ref()]
        }
        _ => return Err(invalid("V36 assignment shard IPC type differs")),
    };
    let actual_children = field.children();
    if actual_children.map_or(0, |values| values.len()) != expected_children.len() {
        return Err(invalid("V36 assignment shard IPC children differ"));
    }
    for (index, child) in expected_children.iter().enumerate() {
        validate_v36_assignment_ipc_field(
            actual_children
                .ok_or_else(|| invalid("V36 assignment shard IPC child is missing"))?
                .get(index),
            child,
        )?;
    }
    Ok(())
}

fn validate_v36_assignment_ipc_schema(
    schema: arrow_ipc::Schema<'_>,
    expected: &Schema,
    manifest_key: &str,
) -> Result<()> {
    if schema.endianness() != arrow_ipc::Endianness::Little
        || schema.features().is_some_and(|values| !values.is_empty())
    {
        return Err(invalid("V36 assignment shard IPC schema differs"));
    }
    let metadata = schema
        .custom_metadata()
        .ok_or_else(|| invalid("V36 assignment shard manifest is missing"))?;
    let expected_manifest = expected
        .metadata()
        .get(manifest_key)
        .ok_or_else(|| invalid("V36 assignment shard manifest differs"))?;
    if metadata.len() != 1
        || metadata.get(0).key() != Some(manifest_key)
        || metadata.get(0).value() != Some(expected_manifest.as_str())
    {
        return Err(invalid("V36 assignment shard manifest differs"));
    }
    let fields = schema
        .fields()
        .ok_or_else(|| invalid("V36 assignment shard IPC fields are missing"))?;
    if fields.len() != expected.fields().len() {
        return Err(invalid("V36 assignment shard IPC field count differs"));
    }
    for (index, expected_field) in expected.fields().iter().enumerate() {
        validate_v36_assignment_ipc_field(fields.get(index), expected_field)?;
    }
    Ok(())
}

fn validate_v36_assignment_ipc_envelope(
    bytes: &[u8],
    expected: &Schema,
    row_count: usize,
    manifest_key: &str,
) -> Result<()> {
    if bytes.len() < 18
        || bytes.len() > V36_EXTERNAL_ASSIGNMENT_MAXIMUM_ENCODED_BYTES as usize
        || !bytes.starts_with(b"ARROW1")
        || !bytes.ends_with(b"ARROW1")
    {
        return Err(invalid("V36 assignment shard IPC envelope differs"));
    }
    let trailer = bytes.len() - 10;
    let footer_len = u32::from_le_bytes(
        bytes[trailer..trailer + 4]
            .try_into()
            .map_err(|_| invalid("V36 assignment shard footer length differs"))?,
    ) as usize;
    let footer_start = trailer
        .checked_sub(footer_len)
        .filter(|offset| *offset >= 8)
        .ok_or_else(|| invalid("V36 assignment shard footer extent differs"))?;
    let footer = arrow_ipc::root_as_footer(&bytes[footer_start..trailer])
        .map_err(|_| invalid("V36 assignment shard footer differs"))?;
    if footer.version() != MetadataVersion::V5
        || footer
            .custom_metadata()
            .is_some_and(|values| !values.is_empty())
        || footer
            .dictionaries()
            .is_some_and(|values| !values.is_empty())
    {
        return Err(invalid("V36 assignment shard footer authority differs"));
    }
    validate_v36_assignment_ipc_schema(
        footer
            .schema()
            .ok_or_else(|| invalid("V36 assignment shard footer schema is missing"))?,
        expected,
        manifest_key,
    )?;
    let blocks = footer
        .recordBatches()
        .ok_or_else(|| invalid("V36 assignment shard batch is missing"))?;
    if blocks.len() != 1 {
        return Err(invalid("V36 assignment shard batch count differs"));
    }
    let block = blocks.get(0);
    let block_offset = usize::try_from(block.offset())
        .map_err(|_| invalid("V36 assignment shard batch offset differs"))?;
    let metadata_len = usize::try_from(block.metaDataLength())
        .map_err(|_| invalid("V36 assignment shard batch metadata differs"))?;
    let body_len = usize::try_from(block.bodyLength())
        .map_err(|_| invalid("V36 assignment shard batch body differs"))?;
    let body_start = block_offset
        .checked_add(metadata_len)
        .ok_or_else(|| invalid("V36 assignment shard batch extent overflows"))?;
    let body_end = body_start
        .checked_add(body_len)
        .ok_or_else(|| invalid("V36 assignment shard batch extent overflows"))?;
    if block_offset < 8 || metadata_len < 8 || body_end > footer_start {
        return Err(invalid("V36 assignment shard batch extent differs"));
    }
    let parse_message = |start: usize, end: usize| {
        let metadata = bytes
            .get(start..end)
            .ok_or_else(|| invalid("V36 assignment shard message extent differs"))?;
        if metadata.len() < 4 {
            return Err(invalid("V36 assignment shard message is truncated"));
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
                .ok_or_else(|| invalid("V36 assignment shard message length differs"))?,
        ) as usize;
        let message_end = prefix
            .checked_add(length)
            .filter(|value| *value <= metadata.len())
            .ok_or_else(|| invalid("V36 assignment shard message extent differs"))?;
        arrow_ipc::root_as_message(&metadata[prefix..message_end])
            .map_err(|_| invalid("V36 assignment shard message differs"))
    };
    let leading = parse_message(8, block_offset)?;
    if leading.version() != MetadataVersion::V5 || leading.bodyLength() != 0 {
        return Err(invalid("V36 assignment shard leading schema differs"));
    }
    validate_v36_assignment_ipc_schema(
        leading
            .header_as_schema()
            .ok_or_else(|| invalid("V36 assignment shard leading schema is missing"))?,
        expected,
        manifest_key,
    )?;
    let record_message = parse_message(block_offset, body_start)?;
    let record = record_message
        .header_as_record_batch()
        .ok_or_else(|| invalid("V36 assignment shard record differs"))?;
    if record_message.version() != MetadataVersion::V5
        || record.compression().is_some()
        || record
            .variadicBufferCounts()
            .is_some_and(|values| !values.is_empty())
        || usize::try_from(record.length()).ok() != Some(row_count)
        || usize::try_from(record_message.bodyLength()).ok() != Some(body_len)
    {
        return Err(invalid("V36 assignment shard record authority differs"));
    }
    let nodes = record
        .nodes()
        .ok_or_else(|| invalid("V36 assignment shard nodes are missing"))?;
    let projected_values = row_count
        .checked_mul(192)
        .ok_or_else(|| invalid("V36 assignment shard node length overflows"))?;
    let expected_nodes = [row_count, row_count, row_count, projected_values];
    if nodes.len() != expected_nodes.len()
        || nodes.iter().zip(expected_nodes).any(|(node, length)| {
            usize::try_from(node.length()).ok() != Some(length) || node.null_count() != 0
        })
    {
        return Err(invalid("V36 assignment shard node shape differs"));
    }
    let buffers = record
        .buffers()
        .ok_or_else(|| invalid("V36 assignment shard buffers are missing"))?;
    if buffers.len() != 7 {
        return Err(invalid("V36 assignment shard buffer count differs"));
    }
    let body = &bytes[body_start..body_end];
    let mut slices = Vec::new();
    slices
        .try_reserve_exact(7)
        .map_err(|_| invalid("V36 assignment shard buffer allocation exceeds capacity"))?;
    let mut previous_end = 0_usize;
    for buffer in buffers {
        let start = usize::try_from(buffer.offset())
            .map_err(|_| invalid("V36 assignment shard buffer offset differs"))?;
        let length = usize::try_from(buffer.length())
            .map_err(|_| invalid("V36 assignment shard buffer length differs"))?;
        let end = start
            .checked_add(length)
            .ok_or_else(|| invalid("V36 assignment shard buffer extent overflows"))?;
        if start < previous_end {
            return Err(invalid("V36 assignment shard buffers overlap"));
        }
        slices.push(
            body.get(start..end)
                .ok_or_else(|| invalid("V36 assignment shard buffer extent differs"))?,
        );
        previous_end = end;
    }
    for (index, count) in [
        (0, row_count),
        (2, row_count),
        (4, row_count),
        (5, projected_values),
    ] {
        if !slices[index].is_empty() && slices[index].len() != count.div_ceil(8) {
            return Err(invalid("V36 assignment shard validity length differs"));
        }
    }
    let supercell_bytes = row_count
        .checked_mul(4)
        .ok_or_else(|| invalid("V36 assignment shard supercell bytes overflow"))?;
    let source_bytes = row_count
        .checked_mul(8)
        .ok_or_else(|| invalid("V36 assignment shard source bytes overflow"))?;
    let projected_bytes = projected_values
        .checked_mul(4)
        .ok_or_else(|| invalid("V36 assignment shard projected bytes overflow"))?;
    for (index, length) in [
        (1, supercell_bytes),
        (3, source_bytes),
        (6, projected_bytes),
    ] {
        if slices[index].len() != length {
            return Err(invalid("V36 assignment shard value length differs"));
        }
    }
    Ok(())
}

fn encode_v36_assignment_rows_arrow(
    schema: Arc<Schema>,
    rows: &[V36SupercellAssignmentRow],
) -> Result<Vec<u8>> {
    let projected_values = rows
        .len()
        .checked_mul(192)
        .ok_or_else(|| invalid("V36 assignment projected allocation overflows"))?;
    let mut supercells = Vec::new();
    supercells
        .try_reserve_exact(rows.len())
        .map_err(|_| invalid("V36 assignment supercell allocation exceeds capacity"))?;
    supercells.extend(rows.iter().map(|row| row.supercell_ordinal));
    let mut sources = Vec::new();
    sources
        .try_reserve_exact(rows.len())
        .map_err(|_| invalid("V36 assignment source allocation exceeds capacity"))?;
    sources.extend(rows.iter().map(|row| row.source_ordinal));
    let mut projected_flat = Vec::new();
    projected_flat
        .try_reserve_exact(projected_values)
        .map_err(|_| invalid("V36 assignment projected allocation exceeds capacity"))?;
    projected_flat.extend(rows.iter().flat_map(|row| row.projected));
    let projected = FixedSizeListArray::try_new(
        Arc::new(Field::new("element", DataType::Float32, false)),
        192,
        Arc::new(Float32Array::from(projected_flat)),
        None,
    )?;
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(UInt32Array::from(supercells)),
            Arc::new(UInt64Array::from(sources)),
            Arc::new(projected),
        ],
    )?;
    let options = IpcWriteOptions::try_new(8, false, MetadataVersion::V5)?;
    let maximum_encoded_bytes = rows
        .len()
        .checked_mul(
            usize::try_from(V36_EXTERNAL_ASSIGNMENT_ROW_BYTES)
                .map_err(|_| invalid("V36 assignment encoded bytes overflow"))?,
        )
        .and_then(|bytes| {
            bytes.checked_add(usize::try_from(V36_EXTERNAL_ASSIGNMENT_SHARD_ENVELOPE_BYTES).ok()?)
        })
        .ok_or_else(|| invalid("V36 assignment encoded bytes overflow"))?;
    let mut output = Cursor::new(Vec::new());
    output
        .get_mut()
        .try_reserve_exact(maximum_encoded_bytes)
        .map_err(|_| invalid("V36 assignment encoding allocation exceeds capacity"))?;
    {
        let mut writer = FileWriter::try_new_with_options(&mut output, schema.as_ref(), options)?;
        writer.write(&batch)?;
        writer.finish()?;
    }
    let bytes = output.into_inner();
    if bytes.len() > maximum_encoded_bytes {
        return Err(invalid(&format!(
            "V36 assignment encoded envelope differs: {} > {maximum_encoded_bytes}",
            bytes.len()
        )));
    }
    Ok(bytes)
}

/// Encode one canonical, uncompressed, complete assignment shard.
pub fn encode_v36_supercell_assignment_shard_arrow(
    context: &V36SupercellAssignmentShardContext,
    rows: &[V36SupercellAssignmentRow],
) -> Result<(Vec<u8>, V36SupercellAssignmentShardArtifact)> {
    validate_v36_assignment_rows(context, rows)?;
    let row_count = u32::try_from(rows.len())
        .map_err(|_| invalid("V36 assignment shard row count overflows"))?;
    let manifest = V36SupercellAssignmentShardManifest {
        context: context.into(),
        format: V36_EXTERNAL_ASSIGNMENT_FORMAT.to_owned(),
        role: V36_EXTERNAL_ASSIGNMENT_ROLE.to_owned(),
        row_count,
    };
    let manifest = serde_json::to_string(&v36_canonical_json_value(
        serde_json::to_value(&manifest)
            .map_err(|_| invalid("V36 assignment shard manifest differs"))?,
    ))
    .map_err(|_| invalid("V36 assignment shard manifest differs"))?;
    let schema = Arc::new(v36_assignment_schema(
        V36_EXTERNAL_ASSIGNMENT_MANIFEST_KEY,
        manifest,
    ));
    let bytes = encode_v36_assignment_rows_arrow(schema, rows)?;
    let encoded_bytes = u64::try_from(bytes.len())
        .map_err(|_| invalid("V36 assignment shard encoded bytes overflow"))?;
    if encoded_bytes > V36_EXTERNAL_ASSIGNMENT_MAXIMUM_ENCODED_BYTES {
        return Err(invalid("V36 assignment shard exceeds encoded admission"));
    }
    let artifact = V36SupercellAssignmentShardArtifact {
        blake3: blake3::hash(&bytes).to_hex().to_string(),
        context: context.clone(),
        encoded_bytes,
        row_count,
        sha256: format!("{:x}", Sha256::digest(&bytes)),
    };
    Ok((bytes, artifact))
}

fn decode_v36_assignment_rows_batch(
    reader: &mut FileReader<Cursor<&[u8]>>,
    row_count: usize,
) -> Result<Vec<V36SupercellAssignmentRow>> {
    let batch = reader
        .next()
        .ok_or_else(|| invalid("V36 assignment shard batch is missing"))??;
    if batch.num_rows() != row_count || batch.num_columns() != 3 || reader.next().is_some() {
        return Err(invalid("V36 assignment shard batch differs"));
    }
    let supercells = batch
        .column(0)
        .as_any()
        .downcast_ref::<UInt32Array>()
        .filter(|array| array.null_count() == 0)
        .ok_or_else(|| invalid("V36 assignment shard supercells differ"))?;
    let sources = batch
        .column(1)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .filter(|array| array.null_count() == 0)
        .ok_or_else(|| invalid("V36 assignment shard sources differ"))?;
    let projected = batch
        .column(2)
        .as_any()
        .downcast_ref::<FixedSizeListArray>()
        .filter(|array| array.null_count() == 0 && array.value_length() == 192)
        .ok_or_else(|| invalid("V36 assignment shard projected rows differ"))?;
    let projected = projected
        .values()
        .as_any()
        .downcast_ref::<Float32Array>()
        .filter(|array| array.null_count() == 0)
        .ok_or_else(|| invalid("V36 assignment shard projected rows differ"))?;
    let (projected, remainder) = projected.values().as_chunks::<192>();
    if !remainder.is_empty()
        || projected.len() != supercells.len()
        || sources.len() != supercells.len()
    {
        return Err(invalid("V36 assignment shard projected rows differ"));
    }
    let mut rows = Vec::new();
    rows.try_reserve_exact(row_count)
        .map_err(|_| invalid("V36 assignment decoded rows exceed capacity"))?;
    for ((&supercell_ordinal, &source_ordinal), projected) in supercells
        .values()
        .iter()
        .zip(sources.values())
        .zip(projected)
    {
        rows.push(V36SupercellAssignmentRow {
            supercell_ordinal,
            source_ordinal,
            projected: *projected,
        });
    }
    Ok(rows)
}

/// Authenticate and decode one complete assignment shard.
pub fn decode_v36_supercell_assignment_shard_arrow(
    bytes: &[u8],
    artifact: &V36SupercellAssignmentShardArtifact,
) -> Result<Vec<V36SupercellAssignmentRow>> {
    validate_v36_assignment_context(&artifact.context)?;
    let encoded_bytes = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
    if artifact.row_count == 0
        || artifact.row_count > 65_536
        || artifact.encoded_bytes != encoded_bytes
        || encoded_bytes > V36_EXTERNAL_ASSIGNMENT_MAXIMUM_ENCODED_BYTES
        || !valid_sha256(&artifact.sha256)
        || !valid_sha256(&artifact.blake3)
        || artifact.sha256 != format!("{:x}", Sha256::digest(bytes))
        || artifact.blake3 != blake3::hash(bytes).to_hex().as_str()
    {
        return Err(invalid("V36 assignment shard artifact identity differs"));
    }
    let row_count = usize::try_from(artifact.row_count)
        .map_err(|_| invalid("V36 assignment shard row count overflows"))?;
    let manifest = V36SupercellAssignmentShardManifest {
        context: (&artifact.context).into(),
        format: V36_EXTERNAL_ASSIGNMENT_FORMAT.to_owned(),
        role: V36_EXTERNAL_ASSIGNMENT_ROLE.to_owned(),
        row_count: artifact.row_count,
    };
    let manifest = serde_json::to_string(&v36_canonical_json_value(
        serde_json::to_value(&manifest)
            .map_err(|_| invalid("V36 assignment shard manifest differs"))?,
    ))
    .map_err(|_| invalid("V36 assignment shard manifest differs"))?;
    validate_v36_assignment_ipc_envelope(
        bytes,
        &v36_assignment_schema(V36_EXTERNAL_ASSIGNMENT_MANIFEST_KEY, manifest),
        row_count,
        V36_EXTERNAL_ASSIGNMENT_MANIFEST_KEY,
    )?;
    let mut reader = FileReader::try_new(Cursor::new(bytes), None)?;
    if reader.num_batches() != 1 {
        return Err(invalid("V36 assignment shard batch count differs"));
    }
    let manifest_text = reader
        .schema()
        .metadata()
        .get(V36_EXTERNAL_ASSIGNMENT_MANIFEST_KEY)
        .cloned()
        .ok_or_else(|| invalid("V36 assignment shard manifest is missing"))?;
    let manifest: V36SupercellAssignmentShardManifest = serde_json::from_str(&manifest_text)
        .map_err(|_| invalid("V36 assignment shard manifest differs"))?;
    let canonical = serde_json::to_string(&v36_canonical_json_value(
        serde_json::to_value(&manifest)
            .map_err(|_| invalid("V36 assignment shard manifest differs"))?,
    ))
    .map_err(|_| invalid("V36 assignment shard manifest differs"))?;
    if canonical != manifest_text
        || manifest.context != V36SupercellAssignmentShardContextWire::from(&artifact.context)
        || manifest.format != V36_EXTERNAL_ASSIGNMENT_FORMAT
        || manifest.role != V36_EXTERNAL_ASSIGNMENT_ROLE
        || manifest.row_count != artifact.row_count
        || reader.schema().as_ref()
            != &v36_assignment_schema(V36_EXTERNAL_ASSIGNMENT_MANIFEST_KEY, manifest_text)
    {
        return Err(invalid("V36 assignment shard manifest differs"));
    }
    let rows = decode_v36_assignment_rows_batch(&mut reader, row_count)?;
    validate_v36_assignment_rows(&artifact.context, &rows)?;
    Ok(rows)
}

#[derive(Debug, PartialEq)]
/// One assignment shard authenticated against its exact Arrow bytes and identity.
pub struct V36AuthenticatedSupercellAssignmentShard {
    artifact: V36SupercellAssignmentShardArtifact,
    rows: Vec<V36SupercellAssignmentRow>,
}

impl V36AuthenticatedSupercellAssignmentShard {
    /// Return the exact authenticated assignment-shard identity.
    pub fn artifact(&self) -> &V36SupercellAssignmentShardArtifact {
        &self.artifact
    }

    /// Return the authenticated assignment rows.
    pub fn rows(&self) -> &[V36SupercellAssignmentRow] {
        &self.rows
    }
}

/// Authenticate one complete assignment shard into an opaque merge input.
pub fn authenticate_v36_supercell_assignment_shard_arrow(
    bytes: &[u8],
    artifact: &V36SupercellAssignmentShardArtifact,
) -> Result<V36AuthenticatedSupercellAssignmentShard> {
    let rows = decode_v36_supercell_assignment_shard_arrow(bytes, artifact)?;
    Ok(V36AuthenticatedSupercellAssignmentShard {
        artifact: artifact.clone(),
        rows,
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Complete authority embedded in one per-supercell merge-run chunk.
pub struct V36SupercellRunChunkContext {
    model_identity: V36ArtifactIdentity,
    projected_corpus_sha256: String,
    supercell_ordinal: u32,
    chunk_ordinal: u64,
    training_spec: V36SupercellTrainingSpec,
    uri: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct V36SupercellRunChunkContextWire {
    model_identity: V36ArtifactIdentity,
    projected_corpus_sha256: String,
    supercell_ordinal: u32,
    chunk_ordinal: u64,
    training_spec: V36SupercellTrainingSpec,
    uri: String,
}

impl From<&V36SupercellRunChunkContext> for V36SupercellRunChunkContextWire {
    fn from(context: &V36SupercellRunChunkContext) -> Self {
        Self {
            model_identity: context.model_identity.clone(),
            projected_corpus_sha256: context.projected_corpus_sha256.clone(),
            supercell_ordinal: context.supercell_ordinal,
            chunk_ordinal: context.chunk_ordinal,
            training_spec: context.training_spec.clone(),
            uri: context.uri.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Complete identity and authority for one per-supercell merge-run chunk.
pub struct V36SupercellRunChunkArtifact {
    /// Complete-object BLAKE3.
    pub blake3: String,
    /// Embedded corpus/model/cell/chunk authority.
    pub context: V36SupercellRunChunkContext,
    /// Complete encoded length.
    pub encoded_bytes: u64,
    /// Complete logical row count.
    pub row_count: u32,
    /// Complete-object SHA-256.
    pub sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct V36SupercellRunChunkManifest {
    context: V36SupercellRunChunkContextWire,
    format: String,
    role: String,
    row_count: u32,
}

/// Bind one merge-run chunk to an authenticated model, corpus, and supercell.
pub fn bind_v36_supercell_run_chunk_context(
    model: &V36AuthenticatedSupercellModel,
    supercell_ordinal: u32,
    chunk_ordinal: u64,
    uri: &str,
) -> Result<V36SupercellRunChunkContext> {
    let context = V36SupercellRunChunkContext {
        model_identity: model.identity().clone(),
        projected_corpus_sha256: model.training_spec().projected_corpus_sha256.clone(),
        supercell_ordinal,
        chunk_ordinal,
        training_spec: model.training_spec().clone(),
        uri: uri.to_owned(),
    };
    validate_v36_supercell_run_chunk_context(&context)?;
    Ok(context)
}

fn validate_v36_supercell_run_chunk_context(context: &V36SupercellRunChunkContext) -> Result<()> {
    let identity = &context.model_identity;
    if context.projected_corpus_sha256 != context.training_spec.projected_corpus_sha256
        || !valid_sha256(&context.projected_corpus_sha256)
        || context.supercell_ordinal >= context.training_spec.super_cell_count
        || identity.role != SUPERCELL_MODEL_ROLE
        || identity.encoded_bytes == 0
        || identity.encoded_bytes > SUPERCELL_MODEL_MAXIMUM_ENCODED_BYTES
        || !valid_sha256(&identity.sha256)
        || !valid_sha256(&identity.blake3)
        || !valid_v36_supercell_model_uri(&identity.uri)
        || !valid_v36_supercell_model_uri(&context.uri)
    {
        return Err(invalid("V36 supercell run chunk context differs"));
    }
    Ok(())
}

fn validate_v36_supercell_run_chunk_rows(
    context: &V36SupercellRunChunkContext,
    rows: &[V36SupercellAssignmentRow],
) -> Result<()> {
    validate_v36_supercell_run_chunk_context(context)?;
    if rows.is_empty() || rows.len() > 65_536 {
        return Err(invalid("V36 supercell run chunk row count differs"));
    }
    let mut previous = None;
    for row in rows {
        if row.supercell_ordinal != context.supercell_ordinal
            || row.source_ordinal >= context.training_spec.corpus_rows
            || previous.is_some_and(|source_ordinal| source_ordinal >= row.source_ordinal)
            || row
                .projected
                .iter()
                .any(|value| !value.is_finite() || (*value == 0.0 && value.is_sign_negative()))
        {
            return Err(invalid("V36 supercell run chunk rows differ"));
        }
        previous = Some(row.source_ordinal);
    }
    Ok(())
}

fn v36_supercell_run_chunk_manifest(
    context: &V36SupercellRunChunkContext,
    row_count: u32,
) -> Result<String> {
    let manifest = V36SupercellRunChunkManifest {
        context: context.into(),
        format: V36_SUPERCELL_RUN_CHUNK_FORMAT.to_owned(),
        role: V36_SUPERCELL_RUN_CHUNK_ROLE.to_owned(),
        row_count,
    };
    serde_json::to_string(&v36_canonical_json_value(
        serde_json::to_value(&manifest)
            .map_err(|_| invalid("V36 supercell run chunk manifest differs"))?,
    ))
    .map_err(|_| invalid("V36 supercell run chunk manifest differs"))
}

/// Encode one canonical, uncompressed, per-supercell merge-run chunk.
pub fn encode_v36_supercell_run_chunk_arrow(
    context: &V36SupercellRunChunkContext,
    rows: &[V36SupercellAssignmentRow],
) -> Result<(Vec<u8>, V36SupercellRunChunkArtifact)> {
    validate_v36_supercell_run_chunk_rows(context, rows)?;
    let row_count = u32::try_from(rows.len())
        .map_err(|_| invalid("V36 supercell run chunk row count overflows"))?;
    let manifest = v36_supercell_run_chunk_manifest(context, row_count)?;
    let schema = Arc::new(v36_assignment_schema(
        V36_SUPERCELL_RUN_CHUNK_MANIFEST_KEY,
        manifest,
    ));
    let bytes = encode_v36_assignment_rows_arrow(schema, rows)?;
    let encoded_bytes = u64::try_from(bytes.len())
        .map_err(|_| invalid("V36 supercell run chunk encoded bytes overflow"))?;
    if encoded_bytes > V36_EXTERNAL_ASSIGNMENT_MAXIMUM_ENCODED_BYTES {
        return Err(invalid("V36 supercell run chunk exceeds encoded admission"));
    }
    let artifact = V36SupercellRunChunkArtifact {
        blake3: blake3::hash(&bytes).to_hex().to_string(),
        context: context.clone(),
        encoded_bytes,
        row_count,
        sha256: format!("{:x}", Sha256::digest(&bytes)),
    };
    Ok((bytes, artifact))
}

/// Authenticate and decode one complete per-supercell merge-run chunk.
pub fn decode_v36_supercell_run_chunk_arrow(
    bytes: &[u8],
    artifact: &V36SupercellRunChunkArtifact,
) -> Result<Vec<V36SupercellAssignmentRow>> {
    validate_v36_supercell_run_chunk_context(&artifact.context)?;
    let encoded_bytes = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
    if artifact.row_count == 0
        || artifact.row_count > 65_536
        || artifact.encoded_bytes != encoded_bytes
        || encoded_bytes > V36_EXTERNAL_ASSIGNMENT_MAXIMUM_ENCODED_BYTES
        || !valid_sha256(&artifact.sha256)
        || !valid_sha256(&artifact.blake3)
        || artifact.sha256 != format!("{:x}", Sha256::digest(bytes))
        || artifact.blake3 != blake3::hash(bytes).to_hex().as_str()
    {
        return Err(invalid("V36 supercell run chunk artifact identity differs"));
    }
    let row_count = usize::try_from(artifact.row_count)
        .map_err(|_| invalid("V36 supercell run chunk row count overflows"))?;
    let manifest = v36_supercell_run_chunk_manifest(&artifact.context, artifact.row_count)?;
    validate_v36_assignment_ipc_envelope(
        bytes,
        &v36_assignment_schema(V36_SUPERCELL_RUN_CHUNK_MANIFEST_KEY, manifest),
        row_count,
        V36_SUPERCELL_RUN_CHUNK_MANIFEST_KEY,
    )?;
    let mut reader = FileReader::try_new(Cursor::new(bytes), None)?;
    if reader.num_batches() != 1 {
        return Err(invalid("V36 supercell run chunk batch count differs"));
    }
    let manifest_text = reader
        .schema()
        .metadata()
        .get(V36_SUPERCELL_RUN_CHUNK_MANIFEST_KEY)
        .cloned()
        .ok_or_else(|| invalid("V36 supercell run chunk manifest is missing"))?;
    let manifest: V36SupercellRunChunkManifest = serde_json::from_str(&manifest_text)
        .map_err(|_| invalid("V36 supercell run chunk manifest differs"))?;
    let canonical = serde_json::to_string(&v36_canonical_json_value(
        serde_json::to_value(&manifest)
            .map_err(|_| invalid("V36 supercell run chunk manifest differs"))?,
    ))
    .map_err(|_| invalid("V36 supercell run chunk manifest differs"))?;
    if canonical != manifest_text
        || manifest.context != V36SupercellRunChunkContextWire::from(&artifact.context)
        || manifest.format != V36_SUPERCELL_RUN_CHUNK_FORMAT
        || manifest.role != V36_SUPERCELL_RUN_CHUNK_ROLE
        || manifest.row_count != artifact.row_count
        || reader.schema().as_ref()
            != &v36_assignment_schema(V36_SUPERCELL_RUN_CHUNK_MANIFEST_KEY, manifest_text)
    {
        return Err(invalid("V36 supercell run chunk manifest differs"));
    }
    let rows = decode_v36_assignment_rows_batch(&mut reader, row_count)?;
    validate_v36_supercell_run_chunk_rows(&artifact.context, &rows)?;
    Ok(rows)
}

/// Exact-object source for terminal assignment publication.
pub trait V36TerminalAssignmentSource {
    /// Read the complete bytes of the sole assignment shard when no merge is needed.
    fn read_assignment_shard(
        &mut self,
        artifact: &V36SupercellAssignmentShardArtifact,
    ) -> Result<Vec<u8>>;

    /// Read the complete bytes of one chunk from the exact terminal merge run.
    fn read_merge_chunk(
        &mut self,
        run_root: &V36ArtifactIdentity,
        artifact: &V36InitialAssignmentMergeChunkArtifact,
    ) -> Result<Vec<u8>>;
}

/// Transactional destination for canonical per-supercell chunks.
pub trait V36SupercellRunPublisherSink {
    /// Persist one canonical chunk as attempt-private provisional state.
    fn write_chunk_provisional(
        &mut self,
        bytes: &[u8],
        artifact: &V36SupercellRunChunkArtifact,
    ) -> Result<()>;

    /// Publish the canonical inventory root after all chunks and coverage checks succeed.
    fn commit_root(
        &mut self,
        root_bytes: &[u8],
        root_identity: &V36ArtifactIdentity,
    ) -> Result<V36PublicationCommitStatus>;

    /// Remove only provisional outputs owned by this publication attempt.
    fn abort(&mut self) -> Result<()>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Unambiguous outcome of conditional canonical-root installation.
pub enum V36PublicationCommitStatus {
    /// The exact root is durably visible and owns its provisional chunks.
    Committed,
    /// The root is known not to be visible, so attempt-private chunks may be removed.
    NotCommitted,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct V36SupercellRunRootChunkEntry {
    blake3: String,
    chunk_ordinal: u64,
    encoded_bytes: u64,
    row_count: u32,
    sha256: String,
    supercell_ordinal: u32,
    uri: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct V36SupercellRunRootManifest {
    assignment_root_identity: V36ArtifactIdentity,
    chunks: Vec<V36SupercellRunRootChunkEntry>,
    format: String,
    model_identity: V36ArtifactIdentity,
    projected_corpus_sha256: String,
    role: String,
    row_count: u64,
    training_spec: V36SupercellTrainingSpec,
    terminal_root_identity: V36ArtifactIdentity,
    uri: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Canonical per-supercell inventory returned only after root-last commit.
pub struct V36CommittedSupercellRuns {
    assignment_root_identity: V36ArtifactIdentity,
    chunks: Vec<V36SupercellRunChunkArtifact>,
    root_identity: V36ArtifactIdentity,
    row_count: u64,
    terminal_root_identity: V36ArtifactIdentity,
}

impl V36CommittedSupercellRuns {
    /// Ordered canonical chunk inventory.
    pub fn chunks(&self) -> &[V36SupercellRunChunkArtifact] {
        &self.chunks
    }

    /// Authenticated assignment root that supplied every published row.
    pub const fn assignment_root_identity(&self) -> &V36ArtifactIdentity {
        &self.assignment_root_identity
    }

    /// Exact sole-shard or terminal-generation root consumed by publication.
    pub const fn terminal_root_identity(&self) -> &V36ArtifactIdentity {
        &self.terminal_root_identity
    }

    /// Canonical root identity published after every chunk.
    pub const fn root_identity(&self) -> &V36ArtifactIdentity {
        &self.root_identity
    }

    /// Complete uniquely covered source population.
    pub const fn row_count(&self) -> u64 {
        self.row_count
    }
}

fn v36_supercell_run_root(
    committed: &V36CommittedSupercellAssignments,
    terminal_root_identity: &V36ArtifactIdentity,
    chunks: &[V36SupercellRunChunkArtifact],
) -> Result<(Vec<u8>, V36ArtifactIdentity)> {
    let uri = format!("{}/supercells/root.json", committed.uri_prefix);
    let manifest = V36SupercellRunRootManifest {
        assignment_root_identity: committed.root_identity.clone(),
        chunks: chunks
            .iter()
            .map(|artifact| V36SupercellRunRootChunkEntry {
                blake3: artifact.blake3.clone(),
                chunk_ordinal: artifact.context.chunk_ordinal,
                encoded_bytes: artifact.encoded_bytes,
                row_count: artifact.row_count,
                sha256: artifact.sha256.clone(),
                supercell_ordinal: artifact.context.supercell_ordinal,
                uri: artifact.context.uri.clone(),
            })
            .collect(),
        format: V36_SUPERCELL_RUN_ROOT_FORMAT.to_owned(),
        model_identity: committed.admission.model_identity.clone(),
        projected_corpus_sha256: committed
            .admission
            .training_spec
            .projected_corpus_sha256
            .clone(),
        role: V36_SUPERCELL_RUN_ROOT_ROLE.to_owned(),
        row_count: committed.admission.training_spec.corpus_rows,
        training_spec: committed.admission.training_spec.clone(),
        terminal_root_identity: terminal_root_identity.clone(),
        uri: uri.clone(),
    };
    let mut bytes = serde_json::to_vec(&v36_canonical_json_value(
        serde_json::to_value(manifest).map_err(|_| invalid("V36 supercell run root differs"))?,
    ))
    .map_err(|_| invalid("V36 supercell run root differs"))?;
    bytes
        .try_reserve_exact(1)
        .map_err(|_| invalid("V36 supercell run root exceeds capacity"))?;
    bytes.push(b'\n');
    let encoded_bytes = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
    if encoded_bytes > V36_SUPERCELL_RUN_ROOT_MAXIMUM_ENCODED_BYTES
        || !valid_v36_supercell_model_uri(&uri)
    {
        return Err(invalid("V36 supercell run root exceeds admission"));
    }
    let identity = V36ArtifactIdentity {
        blake3: blake3::hash(&bytes).to_hex().to_string(),
        encoded_bytes,
        role: V36_SUPERCELL_RUN_ROOT_ROLE.to_owned(),
        sha256: format!("{:x}", Sha256::digest(&bytes)),
        uri,
    };
    Ok((bytes, identity))
}

/// Publish the exact terminal assignment as canonical per-supercell chunks.
pub fn publish_v36_supercell_run_chunks(
    committed: &V36CommittedSupercellAssignments,
    terminal_generation: Option<&V36CommittedInitialAssignmentMergeGeneration>,
    source: &mut dyn V36TerminalAssignmentSource,
    sink: &mut dyn V36SupercellRunPublisherSink,
) -> Result<V36CommittedSupercellRuns> {
    let prepared = (|| {
        let schedule = committed.admission.projection.merge_schedule()?;
        let terminal_run = if schedule.is_empty() {
            if terminal_generation.is_some() || committed.artifacts.len() != 1 {
                return Err(invalid("V36 terminal assignment source differs"));
            }
            None
        } else {
            let generation = terminal_generation
                .ok_or_else(|| invalid("V36 terminal assignment generation is missing"))?;
            validate_v36_committed_assignment_merge_generation(committed, generation)?;
            if schedule.last() != Some(&generation.generation)
                || generation.generation.output_run_count != 1
                || generation.runs.len() != 1
            {
                return Err(invalid("V36 terminal assignment generation differs"));
            }
            Some(&generation.runs[0])
        };
        let terminal_root_identity = terminal_generation.map_or_else(
            || committed.root_identity.clone(),
            |generation| generation.root_identity.clone(),
        );
        let bitmap_len = usize::try_from(committed.admission.projection.coverage_bitmap_bytes)
            .map_err(|_| invalid("V36 supercell coverage bitmap overflows"))?;
        let mut coverage = Vec::new();
        coverage
            .try_reserve_exact(bitmap_len)
            .map_err(|_| invalid("V36 supercell coverage bitmap exceeds capacity"))?;
        coverage.resize(bitmap_len, 0_u8);
        let mut chunks = Vec::new();
        let mut buffered = Vec::new();
        buffered
            .try_reserve_exact(usize::try_from(V36_EXTERNAL_ASSIGNMENT_SHARD_ROWS).unwrap())
            .map_err(|_| invalid("V36 supercell publication buffer exceeds capacity"))?;
        let mut current_cell = None;
        let mut chunk_ordinal = 0_u64;
        let flush = |rows: &mut Vec<V36SupercellAssignmentRow>,
                     cell: u32,
                     ordinal: u64,
                     chunks: &mut Vec<V36SupercellRunChunkArtifact>,
                     sink: &mut dyn V36SupercellRunPublisherSink|
         -> Result<()> {
            if rows.is_empty() {
                return Ok(());
            }
            let uri = format!(
                "{}/supercells/cell-{cell:06}/chunk-{ordinal:06}.arrow",
                committed.uri_prefix
            );
            let context = V36SupercellRunChunkContext {
                model_identity: committed.admission.model_identity.clone(),
                projected_corpus_sha256: committed
                    .admission
                    .training_spec
                    .projected_corpus_sha256
                    .clone(),
                supercell_ordinal: cell,
                chunk_ordinal: ordinal,
                training_spec: committed.admission.training_spec.clone(),
                uri,
            };
            let (bytes, output) = encode_v36_supercell_run_chunk_arrow(&context, rows)?;
            sink.write_chunk_provisional(&bytes, &output)?;
            chunks.push(output);
            rows.clear();
            Ok(())
        };
        let mut process_rows = |rows: &[V36SupercellAssignmentRow]| -> Result<()> {
            for row in rows {
                if current_cell.is_some_and(|cell| cell != row.supercell_ordinal) {
                    flush(
                        &mut buffered,
                        current_cell.unwrap(),
                        chunk_ordinal,
                        &mut chunks,
                        sink,
                    )?;
                    current_cell = Some(row.supercell_ordinal);
                    chunk_ordinal = 0;
                } else if current_cell.is_none() {
                    current_cell = Some(row.supercell_ordinal);
                }
                let source_ordinal = usize::try_from(row.source_ordinal)
                    .map_err(|_| invalid("V36 supercell coverage ordinal overflows"))?;
                let byte = coverage
                    .get_mut(source_ordinal / 8)
                    .ok_or_else(|| invalid("V36 supercell coverage ordinal differs"))?;
                let mask = 1_u8 << (source_ordinal % 8);
                if *byte & mask != 0 {
                    return Err(invalid("V36 supercell source coverage differs"));
                }
                *byte |= mask;
                buffered.push(row.clone());
                if buffered.len() == usize::try_from(V36_EXTERNAL_ASSIGNMENT_SHARD_ROWS).unwrap() {
                    flush(
                        &mut buffered,
                        current_cell.unwrap(),
                        chunk_ordinal,
                        &mut chunks,
                        sink,
                    )?;
                    chunk_ordinal = chunk_ordinal
                        .checked_add(1)
                        .ok_or_else(|| invalid("V36 supercell chunk ordinal overflows"))?;
                }
            }
            Ok(())
        };
        if let Some(run) = terminal_run {
            for artifact in &run.chunks {
                let bytes = source.read_merge_chunk(&run.root_identity, artifact)?;
                let rows =
                    decode_v36_initial_assignment_merge_chunk_against_artifact(&bytes, artifact)?;
                process_rows(&rows)?;
            }
        } else {
            let artifact = &committed.artifacts[0];
            let bytes = source.read_assignment_shard(artifact)?;
            let authenticated =
                authenticate_v36_supercell_assignment_shard_arrow(&bytes, artifact)?;
            process_rows(authenticated.rows())?;
        }
        if let Some(cell) = current_cell {
            flush(&mut buffered, cell, chunk_ordinal, &mut chunks, sink)?;
        }
        let rows = committed.admission.training_spec.corpus_rows;
        for ordinal in 0..rows {
            let ordinal = usize::try_from(ordinal)
                .map_err(|_| invalid("V36 supercell coverage ordinal overflows"))?;
            if coverage[ordinal / 8] & (1_u8 << (ordinal % 8)) == 0 {
                return Err(invalid("V36 supercell source coverage differs"));
            }
        }
        let published_rows = chunks.iter().try_fold(0_u64, |sum, chunk| {
            sum.checked_add(u64::from(chunk.row_count))
                .ok_or_else(|| invalid("V36 supercell publication rows overflow"))
        })?;
        if published_rows != rows || chunks.is_empty() {
            return Err(invalid("V36 supercell publication rows differ"));
        }
        let (root_bytes, root_identity) =
            v36_supercell_run_root(committed, &terminal_root_identity, &chunks)?;
        Ok((
            root_bytes,
            V36CommittedSupercellRuns {
                assignment_root_identity: committed.root_identity.clone(),
                chunks,
                root_identity,
                row_count: rows,
                terminal_root_identity,
            },
        ))
    })();
    let (root_bytes, published) = match prepared {
        Ok(prepared) => prepared,
        Err(error) => {
            sink.abort()?;
            return Err(error);
        }
    };
    match sink.commit_root(&root_bytes, &published.root_identity) {
        Ok(V36PublicationCommitStatus::Committed) => Ok(published),
        Ok(V36PublicationCommitStatus::NotCommitted) => {
            sink.abort()?;
            Err(invalid("V36 supercell run root was not committed"))
        }
        // A transport error is an ambiguous commit outcome. Preserve the
        // attempt until the storage implementation reconciles root visibility.
        Err(error) => Err(error),
    }
}

/// Transactional destination for provisional external-assignment shards.
///
/// Implementations must keep provisional bytes unpublished until `commit` and
/// delete them when `abort` is called.
pub trait V36SupercellAssignmentShardSink {
    /// Persist one authenticated shard as attempt-private provisional state.
    fn write_provisional(
        &mut self,
        bytes: &[u8],
        artifact: &V36SupercellAssignmentShardArtifact,
    ) -> Result<()>;

    /// Publish the canonical root last after complete corpus replay succeeds.
    fn commit(
        &mut self,
        artifacts: &[V36SupercellAssignmentShardArtifact],
        root_bytes: &[u8],
        root_identity: &V36ArtifactIdentity,
    ) -> Result<()>;

    /// Remove every provisional shard written by this transaction.
    fn abort(&mut self) -> Result<()>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Complete assignment inventory returned only after transactional commit.
pub struct V36CommittedSupercellAssignments {
    admission: V36AdmittedSupercellAssignmentPreflight,
    artifacts: Vec<V36SupercellAssignmentShardArtifact>,
    root_identity: V36ArtifactIdentity,
    uri_prefix: String,
}

impl V36CommittedSupercellAssignments {
    /// Ordered authenticated assignment shards committed by the writer.
    pub fn artifacts(&self) -> &[V36SupercellAssignmentShardArtifact] {
        &self.artifacts
    }

    /// Exact preflight authority consumed by assignment execution.
    pub const fn admission(&self) -> &V36AdmittedSupercellAssignmentPreflight {
        &self.admission
    }

    /// Canonical root identity published after every provisional shard.
    pub const fn root_identity(&self) -> &V36ArtifactIdentity {
        &self.root_identity
    }

    /// Stable logical URI prefix shared by the committed shards.
    pub fn uri_prefix(&self) -> &str {
        &self.uri_prefix
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// One deterministic fixed-fan-in group in the first external merge generation.
pub struct V36InitialAssignmentMergeGroup {
    group_ordinal: u64,
    input_range: std::ops::Range<usize>,
    output_chunk_count: u64,
    output_root_uri: String,
    output_row_count: u64,
}

impl V36InitialAssignmentMergeGroup {
    /// Consecutive group ordinal within generation zero.
    pub const fn group_ordinal(&self) -> u64 {
        self.group_ordinal
    }

    /// Exact half-open range into the committed assignment inventory.
    pub fn input_range(&self) -> std::ops::Range<usize> {
        self.input_range.clone()
    }

    /// Number of fixed-size Arrow chunks in this output run.
    pub const fn output_chunk_count(&self) -> u64 {
        self.output_chunk_count
    }

    /// Attempt-private canonical root URI reserved for this output run.
    pub fn output_root_uri(&self) -> &str {
        &self.output_root_uri
    }

    /// Exact number of globally sorted rows in this output run.
    pub const fn output_row_count(&self) -> u64 {
        self.output_row_count
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Root-bound deterministic plan for generation zero of external assignment merge.
pub struct V36InitialAssignmentMergeGeneration {
    generation: V36ExternalMergeGenerationProjection,
    groups: Vec<V36InitialAssignmentMergeGroup>,
    model_identity: V36ArtifactIdentity,
    predecessor_root_identity: V36ArtifactIdentity,
    training_spec: V36SupercellTrainingSpec,
}

impl V36InitialAssignmentMergeGeneration {
    /// Exact generation projection admitted before assignment execution.
    pub const fn generation(&self) -> V36ExternalMergeGenerationProjection {
        self.generation
    }

    /// Ordered fixed-fan-in merge groups.
    pub fn groups(&self) -> &[V36InitialAssignmentMergeGroup] {
        &self.groups
    }

    /// Canonical committed assignment root from which the groups were derived.
    pub const fn predecessor_root_identity(&self) -> &V36ArtifactIdentity {
        &self.predecessor_root_identity
    }
}

/// Plan the first global fixed-fan-in merge generation from committed authority.
///
/// A single-shard inventory needs no merge and returns `None`. The opaque
/// committed handle is re-bound to its canonical root before any group is
/// exposed, preventing detached artifact inventories from scheduling work.
pub fn plan_v36_initial_assignment_merge_generation(
    committed: &V36CommittedSupercellAssignments,
) -> Result<Option<V36InitialAssignmentMergeGeneration>> {
    let (_, expected_root_identity) = v36_committed_assignment_root(
        &committed.admission,
        &committed.artifacts,
        &committed.uri_prefix,
    )?;
    if expected_root_identity != committed.root_identity {
        return Err(invalid("V36 assignment merge predecessor root differs"));
    }
    let Some(generation) = committed
        .admission
        .projection
        .merge_schedule()?
        .into_iter()
        .next()
    else {
        return Ok(None);
    };
    if generation.generation_ordinal != 0
        || generation.input_run_count
            != u64::try_from(committed.artifacts.len())
                .map_err(|_| invalid("V36 assignment merge input count overflows"))?
    {
        return Err(invalid("V36 assignment merge generation differs"));
    }
    let output_count = usize::try_from(generation.output_run_count)
        .map_err(|_| invalid("V36 assignment merge output count overflows"))?;
    let fan_in = usize::from(committed.admission.projection.merge_fan_in);
    let mut groups = Vec::new();
    groups
        .try_reserve_exact(output_count)
        .map_err(|_| invalid("V36 assignment merge groups exceed capacity"))?;
    for group_ordinal in 0..output_count {
        let input_start = group_ordinal
            .checked_mul(fan_in)
            .ok_or_else(|| invalid("V36 assignment merge input range overflows"))?;
        let input_end = input_start
            .checked_add(fan_in)
            .ok_or_else(|| invalid("V36 assignment merge input range overflows"))?
            .min(committed.artifacts.len());
        let group_ordinal_u64 = u64::try_from(group_ordinal)
            .map_err(|_| invalid("V36 assignment merge group ordinal overflows"))?;
        let output_row_count = committed.artifacts[input_start..input_end]
            .iter()
            .try_fold(0_u64, |rows, artifact| {
                rows.checked_add(u64::from(artifact.row_count))
                    .ok_or_else(|| invalid("V36 assignment merge output rows overflow"))
            })?;
        if output_row_count == 0 {
            return Err(invalid("V36 assignment merge output rows differ"));
        }
        let output_chunk_count = output_row_count.div_ceil(V36_EXTERNAL_ASSIGNMENT_SHARD_ROWS);
        let output_run_prefix = format!(
            "{}/merge/generation-{:06}/run-{group_ordinal_u64:06}",
            committed.uri_prefix, generation.generation_ordinal
        );
        let output_root_uri = format!("{output_run_prefix}/root.json");
        let last_chunk_uri = format!(
            "{output_run_prefix}/chunk-{:06}.arrow",
            output_chunk_count - 1
        );
        if !valid_v36_supercell_model_uri(&output_root_uri)
            || !valid_v36_supercell_model_uri(&last_chunk_uri)
        {
            return Err(invalid("V36 assignment merge output URI differs"));
        }
        groups.push(V36InitialAssignmentMergeGroup {
            group_ordinal: group_ordinal_u64,
            input_range: input_start..input_end,
            output_chunk_count,
            output_root_uri,
            output_row_count,
        });
    }
    Ok(Some(V36InitialAssignmentMergeGeneration {
        generation,
        groups,
        model_identity: committed.admission.model_identity.clone(),
        predecessor_root_identity: committed.root_identity.clone(),
        training_spec: committed.admission.training_spec.clone(),
    }))
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Authority embedded in one bounded chunk of a globally sorted merge run.
pub struct V36InitialAssignmentMergeChunkContext {
    chunk_ordinal: u64,
    generation_ordinal: u32,
    group_ordinal: u64,
    model_identity: V36ArtifactIdentity,
    predecessor_root_identity: V36ArtifactIdentity,
    training_spec: V36SupercellTrainingSpec,
    uri: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct V36InitialAssignmentMergeChunkContextWire {
    chunk_ordinal: u64,
    generation_ordinal: u32,
    group_ordinal: u64,
    model_identity: V36ArtifactIdentity,
    predecessor_root_identity: V36ArtifactIdentity,
    training_spec: V36SupercellTrainingSpec,
    uri: String,
}

impl From<&V36InitialAssignmentMergeChunkContext> for V36InitialAssignmentMergeChunkContextWire {
    fn from(context: &V36InitialAssignmentMergeChunkContext) -> Self {
        Self {
            chunk_ordinal: context.chunk_ordinal,
            generation_ordinal: context.generation_ordinal,
            group_ordinal: context.group_ordinal,
            model_identity: context.model_identity.clone(),
            predecessor_root_identity: context.predecessor_root_identity.clone(),
            training_spec: context.training_spec.clone(),
            uri: context.uri.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Complete identity for one bounded globally sorted initial-merge chunk.
pub struct V36InitialAssignmentMergeChunkArtifact {
    /// Complete-object BLAKE3.
    pub blake3: String,
    /// Root-bound generation/group/chunk authority.
    pub context: V36InitialAssignmentMergeChunkContext,
    /// Complete encoded length.
    pub encoded_bytes: u64,
    /// First globally ordered `(supercell, source)` key.
    pub first_key: (u32, u64),
    /// Last globally ordered `(supercell, source)` key.
    pub last_key: (u32, u64),
    /// Complete logical row count.
    pub row_count: u32,
    /// Complete-object SHA-256.
    pub sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct V36InitialAssignmentMergeChunkManifest {
    context: V36InitialAssignmentMergeChunkContextWire,
    first_key: (u32, u64),
    format: String,
    last_key: (u32, u64),
    role: String,
    row_count: u32,
}

fn v36_initial_assignment_merge_group(
    plan: &V36InitialAssignmentMergeGeneration,
    group_ordinal: u64,
) -> Result<&V36InitialAssignmentMergeGroup> {
    let group = plan
        .groups
        .get(
            usize::try_from(group_ordinal)
                .map_err(|_| invalid("V36 initial merge group ordinal overflows"))?,
        )
        .ok_or_else(|| invalid("V36 initial merge group ordinal differs"))?;
    if group.group_ordinal != group_ordinal {
        return Err(invalid("V36 initial merge group ordinal differs"));
    }
    Ok(group)
}

fn v36_initial_assignment_merge_chunk_context(
    plan: &V36InitialAssignmentMergeGeneration,
    group_ordinal: u64,
    chunk_ordinal: u64,
) -> Result<(V36InitialAssignmentMergeChunkContext, u32)> {
    let group = v36_initial_assignment_merge_group(plan, group_ordinal)?;
    if chunk_ordinal >= group.output_chunk_count {
        return Err(invalid("V36 initial merge chunk ordinal differs"));
    }
    let preceding_rows = chunk_ordinal
        .checked_mul(V36_EXTERNAL_ASSIGNMENT_SHARD_ROWS)
        .ok_or_else(|| invalid("V36 initial merge chunk rows overflow"))?;
    let remaining_rows = group
        .output_row_count
        .checked_sub(preceding_rows)
        .ok_or_else(|| invalid("V36 initial merge chunk rows differ"))?;
    let row_count = u32::try_from(remaining_rows.min(V36_EXTERNAL_ASSIGNMENT_SHARD_ROWS))
        .map_err(|_| invalid("V36 initial merge chunk rows overflow"))?;
    let run_prefix = group
        .output_root_uri
        .strip_suffix("/root.json")
        .ok_or_else(|| invalid("V36 initial merge root URI differs"))?;
    let uri = format!("{run_prefix}/chunk-{chunk_ordinal:06}.arrow");
    if row_count == 0 || !valid_v36_supercell_model_uri(&uri) {
        return Err(invalid("V36 initial merge chunk authority differs"));
    }
    Ok((
        V36InitialAssignmentMergeChunkContext {
            chunk_ordinal,
            generation_ordinal: plan.generation.generation_ordinal,
            group_ordinal,
            model_identity: plan.model_identity.clone(),
            predecessor_root_identity: plan.predecessor_root_identity.clone(),
            training_spec: plan.training_spec.clone(),
            uri,
        },
        row_count,
    ))
}

fn validate_v36_initial_assignment_merge_rows(
    spec: &V36SupercellTrainingSpec,
    rows: &[V36SupercellAssignmentRow],
    expected_row_count: u32,
) -> Result<()> {
    if rows.len()
        != usize::try_from(expected_row_count)
            .map_err(|_| invalid("V36 initial merge row count overflows"))?
    {
        return Err(invalid("V36 initial merge row count differs"));
    }
    let mut previous = None;
    let mut source_ordinals = HashSet::new();
    source_ordinals
        .try_reserve(rows.len())
        .map_err(|_| invalid("V36 initial merge source set exceeds capacity"))?;
    for row in rows {
        let key = (row.supercell_ordinal, row.source_ordinal);
        if row.supercell_ordinal >= spec.super_cell_count
            || row.source_ordinal >= spec.corpus_rows
            || previous.is_some_and(|previous| previous >= key)
            || !source_ordinals.insert(row.source_ordinal)
            || row
                .projected
                .iter()
                .any(|value| !value.is_finite() || (*value == 0.0 && value.is_sign_negative()))
        {
            return Err(invalid("V36 initial merge rows differ"));
        }
        previous = Some(key);
    }
    Ok(())
}

fn v36_initial_assignment_merge_chunk_manifest(
    context: &V36InitialAssignmentMergeChunkContext,
    row_count: u32,
    first_key: (u32, u64),
    last_key: (u32, u64),
) -> Result<String> {
    let manifest = V36InitialAssignmentMergeChunkManifest {
        context: context.into(),
        first_key,
        format: V36_INITIAL_ASSIGNMENT_MERGE_CHUNK_FORMAT.to_owned(),
        last_key,
        role: V36_INITIAL_ASSIGNMENT_MERGE_CHUNK_ROLE.to_owned(),
        row_count,
    };
    serde_json::to_string(&v36_canonical_json_value(
        serde_json::to_value(&manifest)
            .map_err(|_| invalid("V36 initial merge chunk manifest differs"))?,
    ))
    .map_err(|_| invalid("V36 initial merge chunk manifest differs"))
}

/// Encode one canonical, bounded chunk of an initial globally sorted merge run.
pub fn encode_v36_initial_assignment_merge_chunk_arrow(
    plan: &V36InitialAssignmentMergeGeneration,
    group_ordinal: u64,
    chunk_ordinal: u64,
    rows: &[V36SupercellAssignmentRow],
) -> Result<(Vec<u8>, V36InitialAssignmentMergeChunkArtifact)> {
    let (context, row_count) =
        v36_initial_assignment_merge_chunk_context(plan, group_ordinal, chunk_ordinal)?;
    validate_v36_initial_assignment_merge_rows(&context.training_spec, rows, row_count)?;
    let first_key = rows
        .first()
        .map(|row| (row.supercell_ordinal, row.source_ordinal))
        .ok_or_else(|| invalid("V36 initial merge chunk is empty"))?;
    let last_key = rows
        .last()
        .map(|row| (row.supercell_ordinal, row.source_ordinal))
        .ok_or_else(|| invalid("V36 initial merge chunk is empty"))?;
    let manifest =
        v36_initial_assignment_merge_chunk_manifest(&context, row_count, first_key, last_key)?;
    let schema = Arc::new(v36_assignment_schema(
        V36_INITIAL_ASSIGNMENT_MERGE_CHUNK_MANIFEST_KEY,
        manifest,
    ));
    let bytes = encode_v36_assignment_rows_arrow(schema, rows)?;
    let encoded_bytes = u64::try_from(bytes.len())
        .map_err(|_| invalid("V36 initial merge chunk encoded bytes overflow"))?;
    if encoded_bytes > V36_EXTERNAL_ASSIGNMENT_MAXIMUM_ENCODED_BYTES {
        return Err(invalid("V36 initial merge chunk exceeds encoded admission"));
    }
    let artifact = V36InitialAssignmentMergeChunkArtifact {
        blake3: blake3::hash(&bytes).to_hex().to_string(),
        context,
        encoded_bytes,
        first_key,
        last_key,
        row_count,
        sha256: format!("{:x}", Sha256::digest(&bytes)),
    };
    Ok((bytes, artifact))
}

/// Authenticate and decode one plan-bound initial globally sorted merge chunk.
pub fn decode_v36_initial_assignment_merge_chunk_arrow(
    plan: &V36InitialAssignmentMergeGeneration,
    bytes: &[u8],
    artifact: &V36InitialAssignmentMergeChunkArtifact,
) -> Result<Vec<V36SupercellAssignmentRow>> {
    let (expected_context, expected_row_count) = v36_initial_assignment_merge_chunk_context(
        plan,
        artifact.context.group_ordinal,
        artifact.context.chunk_ordinal,
    )?;
    if artifact.context != expected_context || artifact.row_count != expected_row_count {
        return Err(invalid("V36 initial merge chunk identity differs"));
    }
    decode_v36_initial_assignment_merge_chunk_against_artifact(bytes, artifact)
}

fn decode_v36_initial_assignment_merge_chunk_against_artifact(
    bytes: &[u8],
    artifact: &V36InitialAssignmentMergeChunkArtifact,
) -> Result<Vec<V36SupercellAssignmentRow>> {
    let encoded_bytes = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
    if artifact.encoded_bytes != encoded_bytes
        || encoded_bytes > V36_EXTERNAL_ASSIGNMENT_MAXIMUM_ENCODED_BYTES
        || !valid_sha256(&artifact.sha256)
        || !valid_sha256(&artifact.blake3)
        || artifact.sha256 != format!("{:x}", Sha256::digest(bytes))
        || artifact.blake3 != blake3::hash(bytes).to_hex().as_str()
    {
        return Err(invalid("V36 initial merge chunk identity differs"));
    }
    let row_count = usize::try_from(artifact.row_count)
        .map_err(|_| invalid("V36 initial merge chunk row count overflows"))?;
    let manifest = v36_initial_assignment_merge_chunk_manifest(
        &artifact.context,
        artifact.row_count,
        artifact.first_key,
        artifact.last_key,
    )?;
    validate_v36_assignment_ipc_envelope(
        bytes,
        &v36_assignment_schema(V36_INITIAL_ASSIGNMENT_MERGE_CHUNK_MANIFEST_KEY, manifest),
        row_count,
        V36_INITIAL_ASSIGNMENT_MERGE_CHUNK_MANIFEST_KEY,
    )?;
    let mut reader = FileReader::try_new(Cursor::new(bytes), None)?;
    if reader.num_batches() != 1 {
        return Err(invalid("V36 initial merge chunk batch count differs"));
    }
    let manifest_text = reader
        .schema()
        .metadata()
        .get(V36_INITIAL_ASSIGNMENT_MERGE_CHUNK_MANIFEST_KEY)
        .cloned()
        .ok_or_else(|| invalid("V36 initial merge chunk manifest is missing"))?;
    let decoded_manifest: V36InitialAssignmentMergeChunkManifest =
        serde_json::from_str(&manifest_text)
            .map_err(|_| invalid("V36 initial merge chunk manifest differs"))?;
    let canonical = serde_json::to_string(&v36_canonical_json_value(
        serde_json::to_value(&decoded_manifest)
            .map_err(|_| invalid("V36 initial merge chunk manifest differs"))?,
    ))
    .map_err(|_| invalid("V36 initial merge chunk manifest differs"))?;
    if canonical != manifest_text
        || decoded_manifest.context
            != V36InitialAssignmentMergeChunkContextWire::from(&artifact.context)
        || decoded_manifest.first_key != artifact.first_key
        || decoded_manifest.format != V36_INITIAL_ASSIGNMENT_MERGE_CHUNK_FORMAT
        || decoded_manifest.last_key != artifact.last_key
        || decoded_manifest.role != V36_INITIAL_ASSIGNMENT_MERGE_CHUNK_ROLE
        || decoded_manifest.row_count != artifact.row_count
        || reader.schema().as_ref()
            != &v36_assignment_schema(
                V36_INITIAL_ASSIGNMENT_MERGE_CHUNK_MANIFEST_KEY,
                manifest_text,
            )
    {
        return Err(invalid("V36 initial merge chunk manifest differs"));
    }
    let rows = decode_v36_assignment_rows_batch(&mut reader, row_count)?;
    validate_v36_initial_assignment_merge_rows(
        &artifact.context.training_spec,
        &rows,
        artifact.row_count,
    )?;
    if rows
        .first()
        .map(|row| (row.supercell_ordinal, row.source_ordinal))
        != Some(artifact.first_key)
        || rows
            .last()
            .map(|row| (row.supercell_ordinal, row.source_ordinal))
            != Some(artifact.last_key)
    {
        return Err(invalid("V36 initial merge chunk boundary keys differ"));
    }
    Ok(rows)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct V36InitialAssignmentMergeChunkArtifactWire {
    blake3: String,
    context: V36InitialAssignmentMergeChunkContextWire,
    encoded_bytes: u64,
    first_key: (u32, u64),
    last_key: (u32, u64),
    row_count: u32,
    sha256: String,
}

impl From<&V36InitialAssignmentMergeChunkArtifact> for V36InitialAssignmentMergeChunkArtifactWire {
    fn from(artifact: &V36InitialAssignmentMergeChunkArtifact) -> Self {
        Self {
            blake3: artifact.blake3.clone(),
            context: (&artifact.context).into(),
            encoded_bytes: artifact.encoded_bytes,
            first_key: artifact.first_key,
            last_key: artifact.last_key,
            row_count: artifact.row_count,
            sha256: artifact.sha256.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct V36InitialAssignmentMergeRootManifest {
    chunks: Vec<V36InitialAssignmentMergeChunkArtifactWire>,
    format: String,
    generation_ordinal: u32,
    group_ordinal: u64,
    inputs: Vec<V36SupercellAssignmentShardArtifactWire>,
    model_identity: V36ArtifactIdentity,
    predecessor_root_identity: V36ArtifactIdentity,
    role: String,
    row_count: u64,
    training_spec: V36SupercellTrainingSpec,
    uri: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct V36FollowupAssignmentMergeChunkInventoryEntry {
    blake3: String,
    encoded_bytes: u64,
    first_key: (u32, u64),
    last_key: (u32, u64),
    row_count: u32,
    sha256: String,
}

impl From<&V36InitialAssignmentMergeChunkArtifact>
    for V36FollowupAssignmentMergeChunkInventoryEntry
{
    fn from(artifact: &V36InitialAssignmentMergeChunkArtifact) -> Self {
        Self {
            blake3: artifact.blake3.clone(),
            encoded_bytes: artifact.encoded_bytes,
            first_key: artifact.first_key,
            last_key: artifact.last_key,
            row_count: artifact.row_count,
            sha256: artifact.sha256.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct V36FollowupAssignmentMergeRootManifest {
    chunks: Vec<V36FollowupAssignmentMergeChunkInventoryEntry>,
    format: String,
    generation_ordinal: u32,
    group_ordinal: u64,
    input_run_roots: Vec<V36ArtifactIdentity>,
    model_identity: V36ArtifactIdentity,
    predecessor_root_identity: V36ArtifactIdentity,
    role: String,
    row_count: u64,
    training_spec: V36SupercellTrainingSpec,
    uri: String,
}

#[derive(Debug)]
/// Authenticated decoded inputs for one initial merge group.
///
/// Fields remain private so only the authority loader can mint this handle.
pub struct V36AuthenticatedInitialAssignmentMergeGroup {
    group_ordinal: u64,
    input_artifacts: Vec<V36SupercellAssignmentShardArtifact>,
    inputs: Vec<Vec<V36SupercellAssignmentRow>>,
    plan: V36InitialAssignmentMergeGeneration,
}

/// Bind one planned merge group to its exact authenticated assignment shards.
pub fn load_v36_initial_assignment_merge_group(
    committed: &V36CommittedSupercellAssignments,
    group_ordinal: u64,
    authenticated_shards: Vec<V36AuthenticatedSupercellAssignmentShard>,
) -> Result<V36AuthenticatedInitialAssignmentMergeGroup> {
    let plan = plan_v36_initial_assignment_merge_generation(committed)?
        .ok_or_else(|| invalid("V36 initial merge generation is absent"))?;
    let group = v36_initial_assignment_merge_group(&plan, group_ordinal)?;
    let expected_artifacts = &committed.artifacts[group.input_range.clone()];
    if authenticated_shards.len() != expected_artifacts.len() {
        return Err(invalid("V36 initial merge input inventory differs"));
    }
    let mut input_artifacts = Vec::new();
    input_artifacts
        .try_reserve_exact(expected_artifacts.len())
        .map_err(|_| invalid("V36 initial merge artifact inventory exceeds capacity"))?;
    let mut inputs = Vec::new();
    inputs
        .try_reserve_exact(expected_artifacts.len())
        .map_err(|_| invalid("V36 initial merge input inventory exceeds capacity"))?;
    for (expected, authenticated) in expected_artifacts.iter().zip(authenticated_shards) {
        if &authenticated.artifact != expected {
            return Err(invalid("V36 initial merge input artifact differs"));
        }
        input_artifacts.push(authenticated.artifact);
        inputs.push(authenticated.rows);
    }
    let authenticated = V36AuthenticatedInitialAssignmentMergeGroup {
        group_ordinal,
        input_artifacts,
        inputs,
        plan,
    };
    validate_v36_initial_assignment_merge_inputs(&authenticated)?;
    Ok(authenticated)
}

/// Transactional destination for one initial globally sorted merge run.
pub trait V36InitialAssignmentMergeRunSink {
    /// Persist one authenticated chunk as attempt-private provisional state.
    fn write_chunk_provisional(
        &mut self,
        bytes: &[u8],
        artifact: &V36InitialAssignmentMergeChunkArtifact,
    ) -> Result<()>;

    /// Publish the canonical run root after every chunk succeeds.
    fn commit_root(
        &mut self,
        chunks: &[V36InitialAssignmentMergeChunkArtifact],
        root_bytes: &[u8],
        root_identity: &V36ArtifactIdentity,
    ) -> Result<()>;

    /// Remove every provisional chunk written by this transaction.
    fn abort(&mut self) -> Result<()>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// One merge run returned only after its canonical root commits.
pub struct V36CommittedInitialAssignmentMergeRun {
    chunks: Vec<V36InitialAssignmentMergeChunkArtifact>,
    root_identity: V36ArtifactIdentity,
}

impl V36CommittedInitialAssignmentMergeRun {
    /// Ordered authenticated chunks covering the complete merge group.
    pub fn chunks(&self) -> &[V36InitialAssignmentMergeChunkArtifact] {
        &self.chunks
    }

    /// Canonical root identity published after all chunks.
    pub const fn root_identity(&self) -> &V36ArtifactIdentity {
        &self.root_identity
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct V36InitialAssignmentMergeGenerationRootManifest {
    format: String,
    generation: V36ExternalMergeGenerationProjection,
    merge_fan_in: u8,
    model_identity: V36ArtifactIdentity,
    predecessor_root_identity: V36ArtifactIdentity,
    role: String,
    run_count: u64,
    run_inventory_blake3: String,
    run_inventory_sha256: String,
    training_spec: V36SupercellTrainingSpec,
    uri: String,
}

fn v36_initial_assignment_merge_run_inventory_digests(
    runs: &[V36CommittedInitialAssignmentMergeRun],
) -> Result<(String, String)> {
    let mut sha256 = Sha256::new();
    let mut blake3 = blake3::Hasher::new();
    sha256.update(V36_INITIAL_ASSIGNMENT_MERGE_GENERATION_RUN_INVENTORY_DOMAIN);
    blake3.update(V36_INITIAL_ASSIGNMENT_MERGE_GENERATION_RUN_INVENTORY_DOMAIN);
    for run in runs {
        let mut identity = serde_json::to_vec(&v36_canonical_json_value(
            serde_json::to_value(&run.root_identity)
                .map_err(|_| invalid("V36 initial merge generation run identity differs"))?,
        ))
        .map_err(|_| invalid("V36 initial merge generation run identity differs"))?;
        identity
            .try_reserve_exact(1)
            .map_err(|_| invalid("V36 initial merge generation run identity exceeds capacity"))?;
        identity.push(b'\n');
        sha256.update(&identity);
        blake3.update(&identity);
    }
    Ok((
        format!("{:x}", sha256.finalize()),
        blake3.finalize().to_hex().to_string(),
    ))
}

/// Transactional publisher for one complete initial merge-generation root.
pub trait V36InitialAssignmentMergeGenerationSink {
    /// Publish the canonical generation root after every planned run committed.
    fn commit_root(
        &mut self,
        runs: &[V36CommittedInitialAssignmentMergeRun],
        root_bytes: &[u8],
        root_identity: &V36ArtifactIdentity,
    ) -> Result<()>;
}

#[derive(Debug)]
/// Complete generation-zero inventory returned only after its root commits.
pub struct V36CommittedInitialAssignmentMergeGeneration {
    generation: V36ExternalMergeGenerationProjection,
    merge_fan_in: u8,
    predecessor_root_identity: V36ArtifactIdentity,
    root_identity: V36ArtifactIdentity,
    runs: Vec<V36CommittedInitialAssignmentMergeRun>,
}

impl V36CommittedInitialAssignmentMergeGeneration {
    /// Exact fixed-fan-in generation projection.
    pub const fn generation(&self) -> V36ExternalMergeGenerationProjection {
        self.generation
    }

    /// Exact fixed fan-in shared by every external merge generation.
    pub const fn merge_fan_in(&self) -> u8 {
        self.merge_fan_in
    }

    /// Assignment root consumed by every run in this generation.
    pub const fn predecessor_root_identity(&self) -> &V36ArtifactIdentity {
        &self.predecessor_root_identity
    }

    /// Canonical generation root published after every run.
    pub const fn root_identity(&self) -> &V36ArtifactIdentity {
        &self.root_identity
    }

    /// Ordered complete committed run inventory.
    pub fn runs(&self) -> &[V36CommittedInitialAssignmentMergeRun] {
        &self.runs
    }
}

fn validate_v36_committed_initial_assignment_merge_run(
    plan: &V36InitialAssignmentMergeGeneration,
    group_ordinal: u64,
    run: &V36CommittedInitialAssignmentMergeRun,
) -> Result<()> {
    let group = v36_initial_assignment_merge_group(plan, group_ordinal)?;
    if run.root_identity.role != V36_INITIAL_ASSIGNMENT_MERGE_ROOT_ROLE
        || run.root_identity.uri != group.output_root_uri
        || run.root_identity.encoded_bytes == 0
        || run.root_identity.encoded_bytes > V36_INITIAL_ASSIGNMENT_MERGE_ROOT_MAXIMUM_ENCODED_BYTES
        || !valid_sha256(&run.root_identity.sha256)
        || !valid_sha256(&run.root_identity.blake3)
        || run.chunks.len()
            != usize::try_from(group.output_chunk_count)
                .map_err(|_| invalid("V36 initial merge chunk count overflows"))?
    {
        return Err(invalid(
            "V36 initial merge generation run inventory differs",
        ));
    }
    let mut row_count = 0_u64;
    let mut previous_last = None;
    for (chunk_ordinal, chunk) in run.chunks.iter().enumerate() {
        let chunk_ordinal = u64::try_from(chunk_ordinal)
            .map_err(|_| invalid("V36 initial merge chunk ordinal overflows"))?;
        let (expected_context, expected_rows) =
            v36_initial_assignment_merge_chunk_context(plan, group_ordinal, chunk_ordinal)?;
        if chunk.context != expected_context
            || chunk.row_count != expected_rows
            || chunk.encoded_bytes == 0
            || chunk.encoded_bytes > V36_EXTERNAL_ASSIGNMENT_MAXIMUM_ENCODED_BYTES
            || !valid_sha256(&chunk.sha256)
            || !valid_sha256(&chunk.blake3)
            || chunk.first_key > chunk.last_key
            || previous_last.is_some_and(|last| last >= chunk.first_key)
        {
            return Err(invalid(
                "V36 initial merge generation chunk inventory differs",
            ));
        }
        row_count = row_count
            .checked_add(u64::from(chunk.row_count))
            .ok_or_else(|| invalid("V36 initial merge generation rows overflow"))?;
        previous_last = Some(chunk.last_key);
    }
    if row_count != group.output_row_count {
        return Err(invalid("V36 initial merge generation rows differ"));
    }
    Ok(())
}

/// Commit the exact ordered inventory of every generation-zero merge run.
pub fn commit_v36_initial_assignment_merge_generation(
    committed: &V36CommittedSupercellAssignments,
    runs: Vec<V36CommittedInitialAssignmentMergeRun>,
    sink: &mut dyn V36InitialAssignmentMergeGenerationSink,
) -> Result<V36CommittedInitialAssignmentMergeGeneration> {
    let plan = plan_v36_initial_assignment_merge_generation(committed)?
        .ok_or_else(|| invalid("V36 initial merge generation is absent"))?;
    if runs.len()
        != usize::try_from(plan.generation.output_run_count)
            .map_err(|_| invalid("V36 initial merge generation run count overflows"))?
    {
        return Err(invalid(
            "V36 initial merge generation run inventory differs",
        ));
    }
    for (group_ordinal, run) in runs.iter().enumerate() {
        validate_v36_committed_initial_assignment_merge_run(
            &plan,
            u64::try_from(group_ordinal)
                .map_err(|_| invalid("V36 initial merge group ordinal overflows"))?,
            run,
        )?;
    }
    let uri = format!(
        "{}/merge/generation-{:06}/root.json",
        committed.uri_prefix, plan.generation.generation_ordinal
    );
    if !valid_v36_supercell_model_uri(&uri) {
        return Err(invalid("V36 initial merge generation root URI differs"));
    }
    let (run_inventory_sha256, run_inventory_blake3) =
        v36_initial_assignment_merge_run_inventory_digests(&runs)?;
    let manifest = V36InitialAssignmentMergeGenerationRootManifest {
        format: V36_INITIAL_ASSIGNMENT_MERGE_GENERATION_ROOT_FORMAT.to_owned(),
        generation: plan.generation,
        merge_fan_in: committed.admission.projection.merge_fan_in,
        model_identity: plan.model_identity,
        predecessor_root_identity: plan.predecessor_root_identity.clone(),
        role: V36_INITIAL_ASSIGNMENT_MERGE_GENERATION_ROOT_ROLE.to_owned(),
        run_count: u64::try_from(runs.len())
            .map_err(|_| invalid("V36 initial merge generation run count overflows"))?,
        run_inventory_blake3,
        run_inventory_sha256,
        training_spec: plan.training_spec,
        uri: uri.clone(),
    };
    let mut root_bytes = serde_json::to_vec(&v36_canonical_json_value(
        serde_json::to_value(manifest)
            .map_err(|_| invalid("V36 initial merge generation root differs"))?,
    ))
    .map_err(|_| invalid("V36 initial merge generation root differs"))?;
    root_bytes
        .try_reserve_exact(1)
        .map_err(|_| invalid("V36 initial merge generation root exceeds capacity"))?;
    root_bytes.push(b'\n');
    let encoded_bytes = u64::try_from(root_bytes.len()).unwrap_or(u64::MAX);
    if encoded_bytes > V36_INITIAL_ASSIGNMENT_MERGE_GENERATION_ROOT_MAXIMUM_ENCODED_BYTES {
        return Err(invalid(
            "V36 initial merge generation root exceeds encoded admission",
        ));
    }
    let root_identity = V36ArtifactIdentity {
        blake3: blake3::hash(&root_bytes).to_hex().to_string(),
        encoded_bytes,
        role: V36_INITIAL_ASSIGNMENT_MERGE_GENERATION_ROOT_ROLE.to_owned(),
        sha256: format!("{:x}", Sha256::digest(&root_bytes)),
        uri,
    };
    sink.commit_root(&runs, &root_bytes, &root_identity)?;
    Ok(V36CommittedInitialAssignmentMergeGeneration {
        generation: plan.generation,
        merge_fan_in: committed.admission.projection.merge_fan_in,
        predecessor_root_identity: plan.predecessor_root_identity,
        root_identity,
        runs,
    })
}

#[derive(Debug)]
/// Root-bound plan for the first merge generation consuming committed runs.
pub struct V36FollowupAssignmentMergeGeneration {
    generation: V36ExternalMergeGenerationProjection,
    groups: Vec<V36InitialAssignmentMergeGroup>,
    predecessor: V36CommittedInitialAssignmentMergeGeneration,
}

impl V36FollowupAssignmentMergeGeneration {
    /// Exact fixed-fan-in generation projection.
    pub const fn generation(&self) -> V36ExternalMergeGenerationProjection {
        self.generation
    }

    /// Ordered deterministic groups over the committed predecessor runs.
    pub fn groups(&self) -> &[V36InitialAssignmentMergeGroup] {
        &self.groups
    }

    /// Canonical generation root from which all input groups were derived.
    pub const fn predecessor_root_identity(&self) -> &V36ArtifactIdentity {
        self.predecessor.root_identity()
    }
}

fn v36_followup_assignment_merge_authority(
    plan: &V36FollowupAssignmentMergeGeneration,
) -> Result<(&V36ArtifactIdentity, &V36SupercellTrainingSpec)> {
    let first = plan
        .predecessor
        .runs
        .first()
        .and_then(|run| run.chunks.first())
        .ok_or_else(|| invalid("V36 followup merge predecessor is empty"))?;
    let model_identity = &first.context.model_identity;
    let training_spec = &first.context.training_spec;
    for run in &plan.predecessor.runs {
        if run.chunks.is_empty()
            || run.chunks.iter().any(|chunk| {
                chunk.context.model_identity != *model_identity
                    || chunk.context.training_spec != *training_spec
            })
        {
            return Err(invalid("V36 followup merge predecessor authority differs"));
        }
    }
    Ok((model_identity, training_spec))
}

fn v36_followup_assignment_merge_chunk_context(
    plan: &V36FollowupAssignmentMergeGeneration,
    group_ordinal: u64,
    chunk_ordinal: u64,
) -> Result<(V36InitialAssignmentMergeChunkContext, u32)> {
    let group = plan
        .groups
        .get(
            usize::try_from(group_ordinal)
                .map_err(|_| invalid("V36 followup merge group ordinal overflows"))?,
        )
        .ok_or_else(|| invalid("V36 followup merge group ordinal differs"))?;
    if group.group_ordinal != group_ordinal || chunk_ordinal >= group.output_chunk_count {
        return Err(invalid("V36 followup merge chunk ordinal differs"));
    }
    let preceding_rows = chunk_ordinal
        .checked_mul(V36_EXTERNAL_ASSIGNMENT_SHARD_ROWS)
        .ok_or_else(|| invalid("V36 followup merge chunk rows overflow"))?;
    let remaining_rows = group
        .output_row_count
        .checked_sub(preceding_rows)
        .ok_or_else(|| invalid("V36 followup merge chunk rows differ"))?;
    let row_count = u32::try_from(remaining_rows.min(V36_EXTERNAL_ASSIGNMENT_SHARD_ROWS))
        .map_err(|_| invalid("V36 followup merge chunk rows overflow"))?;
    let run_prefix = group
        .output_root_uri
        .strip_suffix("/root.json")
        .ok_or_else(|| invalid("V36 followup merge root URI differs"))?;
    let uri = format!("{run_prefix}/chunk-{chunk_ordinal:06}.arrow");
    let (model_identity, training_spec) = v36_followup_assignment_merge_authority(plan)?;
    Ok((
        V36InitialAssignmentMergeChunkContext {
            chunk_ordinal,
            generation_ordinal: plan.generation.generation_ordinal,
            group_ordinal,
            model_identity: model_identity.clone(),
            predecessor_root_identity: plan.predecessor.root_identity.clone(),
            training_spec: training_spec.clone(),
            uri,
        },
        row_count,
    ))
}

fn encode_v36_followup_assignment_merge_chunk_arrow(
    plan: &V36FollowupAssignmentMergeGeneration,
    group_ordinal: u64,
    chunk_ordinal: u64,
    rows: &[V36SupercellAssignmentRow],
) -> Result<(Vec<u8>, V36InitialAssignmentMergeChunkArtifact)> {
    let (context, row_count) =
        v36_followup_assignment_merge_chunk_context(plan, group_ordinal, chunk_ordinal)?;
    validate_v36_initial_assignment_merge_rows(&context.training_spec, rows, row_count)?;
    let first_key = rows
        .first()
        .map(|row| (row.supercell_ordinal, row.source_ordinal))
        .ok_or_else(|| invalid("V36 followup merge chunk is empty"))?;
    let last_key = rows
        .last()
        .map(|row| (row.supercell_ordinal, row.source_ordinal))
        .ok_or_else(|| invalid("V36 followup merge chunk is empty"))?;
    let manifest =
        v36_initial_assignment_merge_chunk_manifest(&context, row_count, first_key, last_key)?;
    let schema = Arc::new(v36_assignment_schema(
        V36_INITIAL_ASSIGNMENT_MERGE_CHUNK_MANIFEST_KEY,
        manifest,
    ));
    let bytes = encode_v36_assignment_rows_arrow(schema, rows)?;
    let encoded_bytes = u64::try_from(bytes.len())
        .map_err(|_| invalid("V36 followup merge chunk encoded bytes overflow"))?;
    if encoded_bytes > V36_EXTERNAL_ASSIGNMENT_MAXIMUM_ENCODED_BYTES {
        return Err(invalid(
            "V36 followup merge chunk exceeds encoded admission",
        ));
    }
    let artifact = V36InitialAssignmentMergeChunkArtifact {
        blake3: blake3::hash(&bytes).to_hex().to_string(),
        context,
        encoded_bytes,
        first_key,
        last_key,
        row_count,
        sha256: format!("{:x}", Sha256::digest(&bytes)),
    };
    Ok((bytes, artifact))
}

/// Exact-object reader for one bounded committed merge chunk.
pub trait V36AssignmentMergeChunkSource {
    /// Read the complete bytes for one root-bound chunk artifact.
    fn read_chunk(
        &mut self,
        run_root: &V36ArtifactIdentity,
        artifact: &V36InitialAssignmentMergeChunkArtifact,
    ) -> Result<Vec<u8>>;
}

/// Transactional destination for one follow-up globally sorted merge run.
pub trait V36FollowupAssignmentMergeRunSink {
    /// Persist one authenticated output chunk as attempt-private provisional state.
    fn write_chunk_provisional(
        &mut self,
        bytes: &[u8],
        artifact: &V36InitialAssignmentMergeChunkArtifact,
    ) -> Result<()>;

    /// Publish the canonical run root after every output chunk succeeds.
    fn commit_root(
        &mut self,
        chunks: &[V36InitialAssignmentMergeChunkArtifact],
        root_bytes: &[u8],
        root_identity: &V36ArtifactIdentity,
    ) -> Result<()>;

    /// Remove every provisional output written by this transaction.
    fn abort(&mut self) -> Result<()>;
}

struct V36FollowupAssignmentMergeCursor {
    chunk_ordinal: usize,
    row_ordinal: usize,
    rows: Vec<V36SupercellAssignmentRow>,
    run_ordinal: usize,
}

fn load_v36_followup_assignment_merge_chunk(
    run: &V36CommittedInitialAssignmentMergeRun,
    run_ordinal: usize,
    chunk_ordinal: usize,
    source: &mut dyn V36AssignmentMergeChunkSource,
) -> Result<V36FollowupAssignmentMergeCursor> {
    let artifact = run
        .chunks
        .get(chunk_ordinal)
        .ok_or_else(|| invalid("V36 followup merge chunk ordinal differs"))?;
    let bytes = source.read_chunk(&run.root_identity, artifact)?;
    let rows = decode_v36_initial_assignment_merge_chunk_against_artifact(&bytes, artifact)?;
    if rows.is_empty() {
        return Err(invalid("V36 followup merge chunk is empty"));
    }
    Ok(V36FollowupAssignmentMergeCursor {
        chunk_ordinal,
        row_ordinal: 0,
        rows,
        run_ordinal,
    })
}

/// Stream one deterministic follow-up merge group with one decoded chunk resident per input run.
pub fn stream_v36_followup_assignment_merge_group(
    plan: &V36FollowupAssignmentMergeGeneration,
    group_ordinal: u64,
    source: &mut dyn V36AssignmentMergeChunkSource,
    visitor: &mut dyn FnMut(&[V36SupercellAssignmentRow]) -> Result<()>,
) -> Result<()> {
    stream_v36_followup_assignment_merge_group_with_output_capacity(
        plan,
        group_ordinal,
        source,
        usize::try_from(V36_EXTERNAL_ASSIGNMENT_SHARD_ROWS).unwrap(),
        visitor,
    )
}

fn stream_v36_followup_assignment_merge_group_with_output_capacity(
    plan: &V36FollowupAssignmentMergeGeneration,
    group_ordinal: u64,
    source: &mut dyn V36AssignmentMergeChunkSource,
    output_capacity: usize,
    visitor: &mut dyn FnMut(&[V36SupercellAssignmentRow]) -> Result<()>,
) -> Result<()> {
    if output_capacity == 0
        || output_capacity > usize::try_from(V36_EXTERNAL_ASSIGNMENT_SHARD_ROWS).unwrap()
    {
        return Err(invalid("V36 followup merge output capacity differs"));
    }
    let group = plan
        .groups
        .get(
            usize::try_from(group_ordinal)
                .map_err(|_| invalid("V36 followup merge group ordinal overflows"))?,
        )
        .ok_or_else(|| invalid("V36 followup merge group ordinal differs"))?;
    if group.group_ordinal != group_ordinal || group.input_range.is_empty() {
        return Err(invalid("V36 followup merge group differs"));
    }

    let runs = plan
        .predecessor
        .runs
        .get(group.input_range.clone())
        .ok_or_else(|| invalid("V36 followup merge input range differs"))?;
    let mut cursors = Vec::new();
    cursors
        .try_reserve_exact(runs.len())
        .map_err(|_| invalid("V36 followup merge cursors exceed capacity"))?;
    let mut heap = BinaryHeap::new();
    heap.try_reserve(runs.len())
        .map_err(|_| invalid("V36 followup merge heap exceeds capacity"))?;
    for (run_ordinal, run) in runs.iter().enumerate() {
        let cursor = load_v36_followup_assignment_merge_chunk(run, run_ordinal, 0, source)?;
        let row = &cursor.rows[0];
        heap.push(std::cmp::Reverse((
            row.supercell_ordinal,
            row.source_ordinal,
            run_ordinal,
        )));
        cursors.push(cursor);
    }

    let mut output = Vec::new();
    output
        .try_reserve_exact(
            output_capacity.min(
                usize::try_from(group.output_row_count)
                    .map_err(|_| invalid("V36 followup merge row count overflows"))?,
            ),
        )
        .map_err(|_| invalid("V36 followup merge output exceeds capacity"))?;
    let mut output_rows = 0_u64;
    let mut previous_key = None;
    while let Some(std::cmp::Reverse((cell, source_ordinal, run_ordinal))) = heap.pop() {
        let key = (cell, source_ordinal);
        if previous_key.is_some_and(|previous| previous >= key) {
            return Err(invalid("V36 followup merge global order differs"));
        }
        let cursor = cursors
            .get_mut(run_ordinal)
            .ok_or_else(|| invalid("V36 followup merge cursor differs"))?;
        let row = cursor
            .rows
            .get(cursor.row_ordinal)
            .ok_or_else(|| invalid("V36 followup merge cursor row differs"))?;
        if (row.supercell_ordinal, row.source_ordinal) != key || cursor.run_ordinal != run_ordinal {
            return Err(invalid("V36 followup merge cursor differs"));
        }
        output.push(row.clone());
        output_rows = output_rows
            .checked_add(1)
            .ok_or_else(|| invalid("V36 followup merge rows overflow"))?;
        previous_key = Some(key);
        cursor.row_ordinal += 1;

        if cursor.row_ordinal == cursor.rows.len() {
            let next_chunk_ordinal = cursor
                .chunk_ordinal
                .checked_add(1)
                .ok_or_else(|| invalid("V36 followup merge chunk ordinal overflows"))?;
            let run = &runs[run_ordinal];
            if next_chunk_ordinal < run.chunks.len() {
                drop(std::mem::take(&mut cursor.rows));
                *cursor = load_v36_followup_assignment_merge_chunk(
                    run,
                    run_ordinal,
                    next_chunk_ordinal,
                    source,
                )?;
            }
        }
        if let Some(next) = cursor.rows.get(cursor.row_ordinal) {
            heap.push(std::cmp::Reverse((
                next.supercell_ordinal,
                next.source_ordinal,
                run_ordinal,
            )));
        }
        if output.len() == output_capacity {
            visitor(&output)?;
            output.clear();
        }
    }
    if !output.is_empty() {
        visitor(&output)?;
    }
    if output_rows != group.output_row_count {
        return Err(invalid("V36 followup merge row count differs"));
    }
    Ok(())
}

fn v36_followup_assignment_merge_root(
    plan: &V36FollowupAssignmentMergeGeneration,
    group_ordinal: u64,
    chunks: &[V36InitialAssignmentMergeChunkArtifact],
) -> Result<(Vec<u8>, V36ArtifactIdentity)> {
    let group = plan
        .groups
        .get(
            usize::try_from(group_ordinal)
                .map_err(|_| invalid("V36 followup merge group ordinal overflows"))?,
        )
        .ok_or_else(|| invalid("V36 followup merge group ordinal differs"))?;
    if group.group_ordinal != group_ordinal
        || chunks.len()
            != usize::try_from(group.output_chunk_count)
                .map_err(|_| invalid("V36 followup merge chunk count overflows"))?
    {
        return Err(invalid("V36 followup merge chunk inventory differs"));
    }
    let mut row_count = 0_u64;
    let mut previous_last = None;
    for (chunk_ordinal, chunk) in chunks.iter().enumerate() {
        let (expected_context, expected_rows) = v36_followup_assignment_merge_chunk_context(
            plan,
            group_ordinal,
            u64::try_from(chunk_ordinal)
                .map_err(|_| invalid("V36 followup merge chunk ordinal overflows"))?,
        )?;
        if chunk.context != expected_context
            || chunk.row_count != expected_rows
            || chunk.first_key > chunk.last_key
            || previous_last.is_some_and(|last| last >= chunk.first_key)
        {
            return Err(invalid("V36 followup merge chunk inventory differs"));
        }
        row_count = row_count
            .checked_add(u64::from(chunk.row_count))
            .ok_or_else(|| invalid("V36 followup merge rows overflow"))?;
        previous_last = Some(chunk.last_key);
    }
    if row_count != group.output_row_count {
        return Err(invalid("V36 followup merge row count differs"));
    }
    let runs = plan
        .predecessor
        .runs
        .get(group.input_range.clone())
        .ok_or_else(|| invalid("V36 followup merge input range differs"))?;
    let (model_identity, training_spec) = v36_followup_assignment_merge_authority(plan)?;
    let manifest = V36FollowupAssignmentMergeRootManifest {
        chunks: chunks
            .iter()
            .map(V36FollowupAssignmentMergeChunkInventoryEntry::from)
            .collect(),
        format: V36_FOLLOWUP_ASSIGNMENT_MERGE_ROOT_FORMAT.to_owned(),
        generation_ordinal: plan.generation.generation_ordinal,
        group_ordinal,
        input_run_roots: runs.iter().map(|run| run.root_identity.clone()).collect(),
        model_identity: model_identity.clone(),
        predecessor_root_identity: plan.predecessor.root_identity.clone(),
        role: V36_FOLLOWUP_ASSIGNMENT_MERGE_ROOT_ROLE.to_owned(),
        row_count,
        training_spec: training_spec.clone(),
        uri: group.output_root_uri.clone(),
    };
    let mut bytes = serde_json::to_vec(&v36_canonical_json_value(
        serde_json::to_value(manifest)
            .map_err(|_| invalid("V36 followup merge root manifest differs"))?,
    ))
    .map_err(|_| invalid("V36 followup merge root manifest differs"))?;
    bytes
        .try_reserve_exact(1)
        .map_err(|_| invalid("V36 followup merge root exceeds capacity"))?;
    bytes.push(b'\n');
    let encoded_bytes = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
    if encoded_bytes > V36_INITIAL_ASSIGNMENT_MERGE_ROOT_MAXIMUM_ENCODED_BYTES {
        return Err(invalid("V36 followup merge root exceeds encoded admission"));
    }
    let identity = V36ArtifactIdentity {
        blake3: blake3::hash(&bytes).to_hex().to_string(),
        encoded_bytes,
        role: V36_FOLLOWUP_ASSIGNMENT_MERGE_ROOT_ROLE.to_owned(),
        sha256: format!("{:x}", Sha256::digest(&bytes)),
        uri: group.output_root_uri.clone(),
    };
    Ok((bytes, identity))
}

/// Authenticate one compact follow-up merge root and reconstruct its chunk inventory.
pub fn authenticate_v36_followup_assignment_merge_run_root(
    plan: &V36FollowupAssignmentMergeGeneration,
    group_ordinal: u64,
    bytes: &[u8],
    root_identity: &V36ArtifactIdentity,
) -> Result<V36CommittedInitialAssignmentMergeRun> {
    let group = plan
        .groups
        .get(
            usize::try_from(group_ordinal)
                .map_err(|_| invalid("V36 followup merge group ordinal overflows"))?,
        )
        .ok_or_else(|| invalid("V36 followup merge group ordinal differs"))?;
    let encoded_bytes = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
    if group.group_ordinal != group_ordinal
        || bytes.last() != Some(&b'\n')
        || root_identity.encoded_bytes != encoded_bytes
        || encoded_bytes > V36_INITIAL_ASSIGNMENT_MERGE_ROOT_MAXIMUM_ENCODED_BYTES
        || root_identity.role != V36_FOLLOWUP_ASSIGNMENT_MERGE_ROOT_ROLE
        || root_identity.uri != group.output_root_uri
        || !valid_sha256(&root_identity.sha256)
        || !valid_sha256(&root_identity.blake3)
        || root_identity.sha256 != format!("{:x}", Sha256::digest(bytes))
        || root_identity.blake3 != blake3::hash(bytes).to_hex().as_str()
    {
        return Err(invalid("V36 followup merge root identity differs"));
    }
    let manifest: V36FollowupAssignmentMergeRootManifest = serde_json::from_slice(bytes)
        .map_err(|_| invalid("V36 followup merge root manifest differs"))?;
    let mut canonical = serde_json::to_vec(&v36_canonical_json_value(
        serde_json::to_value(&manifest)
            .map_err(|_| invalid("V36 followup merge root manifest differs"))?,
    ))
    .map_err(|_| invalid("V36 followup merge root manifest differs"))?;
    canonical
        .try_reserve_exact(1)
        .map_err(|_| invalid("V36 followup merge root manifest exceeds capacity"))?;
    canonical.push(b'\n');
    let predecessor_runs = plan
        .predecessor
        .runs
        .get(group.input_range.clone())
        .ok_or_else(|| invalid("V36 followup merge input range differs"))?;
    let expected_input_roots: Vec<_> = predecessor_runs
        .iter()
        .map(|run| run.root_identity.clone())
        .collect();
    let (model_identity, training_spec) = v36_followup_assignment_merge_authority(plan)?;
    if canonical != bytes
        || manifest.format != V36_FOLLOWUP_ASSIGNMENT_MERGE_ROOT_FORMAT
        || manifest.generation_ordinal != plan.generation.generation_ordinal
        || manifest.group_ordinal != group_ordinal
        || manifest.input_run_roots != expected_input_roots
        || manifest.model_identity != *model_identity
        || manifest.predecessor_root_identity != plan.predecessor.root_identity
        || manifest.role != V36_FOLLOWUP_ASSIGNMENT_MERGE_ROOT_ROLE
        || manifest.row_count != group.output_row_count
        || manifest.training_spec != *training_spec
        || manifest.uri != group.output_root_uri
        || manifest.chunks.len()
            != usize::try_from(group.output_chunk_count)
                .map_err(|_| invalid("V36 followup merge chunk count overflows"))?
    {
        return Err(invalid("V36 followup merge root manifest differs"));
    }

    let mut chunks = Vec::new();
    chunks
        .try_reserve_exact(manifest.chunks.len())
        .map_err(|_| invalid("V36 followup merge chunk inventory exceeds capacity"))?;
    let mut row_count = 0_u64;
    let mut previous_last = None;
    for (chunk_ordinal, chunk) in manifest.chunks.into_iter().enumerate() {
        let chunk_ordinal = u64::try_from(chunk_ordinal)
            .map_err(|_| invalid("V36 followup merge chunk ordinal overflows"))?;
        let (context, expected_rows) =
            v36_followup_assignment_merge_chunk_context(plan, group_ordinal, chunk_ordinal)?;
        if chunk.row_count != expected_rows
            || chunk.encoded_bytes == 0
            || chunk.encoded_bytes > V36_EXTERNAL_ASSIGNMENT_MAXIMUM_ENCODED_BYTES
            || !valid_sha256(&chunk.sha256)
            || !valid_sha256(&chunk.blake3)
            || chunk.first_key > chunk.last_key
            || previous_last.is_some_and(|last| last >= chunk.first_key)
        {
            return Err(invalid("V36 followup merge root chunk inventory differs"));
        }
        row_count = row_count
            .checked_add(u64::from(chunk.row_count))
            .ok_or_else(|| invalid("V36 followup merge chunk rows overflow"))?;
        previous_last = Some(chunk.last_key);
        chunks.push(V36InitialAssignmentMergeChunkArtifact {
            blake3: chunk.blake3,
            context,
            encoded_bytes: chunk.encoded_bytes,
            first_key: chunk.first_key,
            last_key: chunk.last_key,
            row_count: chunk.row_count,
            sha256: chunk.sha256,
        });
    }
    if row_count != group.output_row_count {
        return Err(invalid("V36 followup merge root chunk rows differ"));
    }
    Ok(V36CommittedInitialAssignmentMergeRun {
        chunks,
        root_identity: root_identity.clone(),
    })
}

/// Stream one follow-up merge run transaction and publish its canonical root last.
pub fn write_v36_followup_assignment_merge_group(
    plan: &V36FollowupAssignmentMergeGeneration,
    group_ordinal: u64,
    source: &mut dyn V36AssignmentMergeChunkSource,
    sink: &mut dyn V36FollowupAssignmentMergeRunSink,
) -> Result<V36CommittedInitialAssignmentMergeRun> {
    let execute = (|| {
        let group = plan
            .groups
            .get(
                usize::try_from(group_ordinal)
                    .map_err(|_| invalid("V36 followup merge group ordinal overflows"))?,
            )
            .ok_or_else(|| invalid("V36 followup merge group ordinal differs"))?;
        if group.group_ordinal != group_ordinal {
            return Err(invalid("V36 followup merge group ordinal differs"));
        }
        let mut chunks = Vec::new();
        chunks
            .try_reserve_exact(
                usize::try_from(group.output_chunk_count)
                    .map_err(|_| invalid("V36 followup merge chunk count overflows"))?,
            )
            .map_err(|_| invalid("V36 followup merge chunks exceed capacity"))?;
        stream_v36_followup_assignment_merge_group(plan, group_ordinal, source, &mut |rows| {
            let chunk_ordinal = u64::try_from(chunks.len())
                .map_err(|_| invalid("V36 followup merge chunk ordinal overflows"))?;
            let (bytes, artifact) = encode_v36_followup_assignment_merge_chunk_arrow(
                plan,
                group_ordinal,
                chunk_ordinal,
                rows,
            )?;
            sink.write_chunk_provisional(&bytes, &artifact)?;
            chunks.push(artifact);
            Ok(())
        })?;
        let (root_bytes, root_identity) =
            v36_followup_assignment_merge_root(plan, group_ordinal, &chunks)?;
        sink.commit_root(&chunks, &root_bytes, &root_identity)?;
        Ok(V36CommittedInitialAssignmentMergeRun {
            chunks,
            root_identity,
        })
    })();
    match execute {
        Ok(committed) => Ok(committed),
        Err(source_error) => match sink.abort() {
            Ok(()) => Err(source_error),
            Err(cleanup_error) => Err(cleanup_error),
        },
    }
}

fn validate_v36_committed_initial_assignment_merge_generation(
    committed: &V36CommittedSupercellAssignments,
    generation: &V36CommittedInitialAssignmentMergeGeneration,
) -> Result<()> {
    let plan = plan_v36_initial_assignment_merge_generation(committed)?
        .ok_or_else(|| invalid("V36 initial merge generation is absent"))?;
    if generation.generation != plan.generation
        || generation.merge_fan_in != committed.admission.projection.merge_fan_in
        || generation.predecessor_root_identity != committed.root_identity
        || generation.runs.len()
            != usize::try_from(plan.generation.output_run_count)
                .map_err(|_| invalid("V36 initial merge generation run count overflows"))?
    {
        return Err(invalid("V36 committed initial merge generation differs"));
    }
    for (group_ordinal, run) in generation.runs.iter().enumerate() {
        validate_v36_committed_initial_assignment_merge_run(
            &plan,
            u64::try_from(group_ordinal)
                .map_err(|_| invalid("V36 initial merge group ordinal overflows"))?,
            run,
        )?;
    }
    let uri = format!(
        "{}/merge/generation-{:06}/root.json",
        committed.uri_prefix, plan.generation.generation_ordinal
    );
    let (run_inventory_sha256, run_inventory_blake3) =
        v36_initial_assignment_merge_run_inventory_digests(&generation.runs)?;
    let manifest = V36InitialAssignmentMergeGenerationRootManifest {
        format: V36_INITIAL_ASSIGNMENT_MERGE_GENERATION_ROOT_FORMAT.to_owned(),
        generation: plan.generation,
        merge_fan_in: committed.admission.projection.merge_fan_in,
        model_identity: plan.model_identity,
        predecessor_root_identity: plan.predecessor_root_identity,
        role: V36_INITIAL_ASSIGNMENT_MERGE_GENERATION_ROOT_ROLE.to_owned(),
        run_count: u64::try_from(generation.runs.len())
            .map_err(|_| invalid("V36 initial merge generation run count overflows"))?,
        run_inventory_blake3,
        run_inventory_sha256,
        training_spec: plan.training_spec,
        uri: uri.clone(),
    };
    let mut bytes = serde_json::to_vec(&v36_canonical_json_value(
        serde_json::to_value(manifest)
            .map_err(|_| invalid("V36 initial merge generation root differs"))?,
    ))
    .map_err(|_| invalid("V36 initial merge generation root differs"))?;
    bytes
        .try_reserve_exact(1)
        .map_err(|_| invalid("V36 initial merge generation root exceeds capacity"))?;
    bytes.push(b'\n');
    let expected = V36ArtifactIdentity {
        blake3: blake3::hash(&bytes).to_hex().to_string(),
        encoded_bytes: u64::try_from(bytes.len()).unwrap_or(u64::MAX),
        role: V36_INITIAL_ASSIGNMENT_MERGE_GENERATION_ROOT_ROLE.to_owned(),
        sha256: format!("{:x}", Sha256::digest(&bytes)),
        uri,
    };
    if generation.root_identity != expected {
        return Err(invalid(
            "V36 committed initial merge generation root differs",
        ));
    }
    Ok(())
}

fn v36_followup_assignment_merge_expected_group_rows(
    committed: &V36CommittedSupercellAssignments,
    generation_ordinal: u32,
    group_ordinal: u64,
) -> Result<u64> {
    let mut shard_span = 1_u64;
    for _ in 0..=generation_ordinal {
        shard_span = shard_span
            .checked_mul(u64::from(committed.admission.projection.merge_fan_in))
            .ok_or_else(|| invalid("V36 followup merge shard span overflows"))?;
    }
    let start_shard = group_ordinal
        .checked_mul(shard_span)
        .ok_or_else(|| invalid("V36 followup merge shard range overflows"))?;
    let start_row = start_shard
        .checked_mul(V36_EXTERNAL_ASSIGNMENT_SHARD_ROWS)
        .ok_or_else(|| invalid("V36 followup merge row range overflows"))?;
    let span_rows = shard_span
        .checked_mul(V36_EXTERNAL_ASSIGNMENT_SHARD_ROWS)
        .ok_or_else(|| invalid("V36 followup merge row range overflows"))?;
    Ok(committed
        .admission
        .training_spec
        .corpus_rows
        .saturating_sub(start_row)
        .min(span_rows))
}

fn validate_v36_committed_followup_assignment_merge_run(
    committed: &V36CommittedSupercellAssignments,
    generation: &V36CommittedInitialAssignmentMergeGeneration,
    group_ordinal: u64,
    run: &V36CommittedInitialAssignmentMergeRun,
) -> Result<()> {
    let expected_rows = v36_followup_assignment_merge_expected_group_rows(
        committed,
        generation.generation.generation_ordinal,
        group_ordinal,
    )?;
    let run_prefix = format!(
        "{}/merge/generation-{:06}/run-{group_ordinal:06}",
        committed.uri_prefix, generation.generation.generation_ordinal
    );
    let expected_root_uri = format!("{run_prefix}/root.json");
    if expected_rows == 0
        || run.root_identity.role != V36_FOLLOWUP_ASSIGNMENT_MERGE_ROOT_ROLE
        || run.root_identity.uri != expected_root_uri
        || run.root_identity.encoded_bytes == 0
        || run.root_identity.encoded_bytes > V36_INITIAL_ASSIGNMENT_MERGE_ROOT_MAXIMUM_ENCODED_BYTES
        || !valid_sha256(&run.root_identity.sha256)
        || !valid_sha256(&run.root_identity.blake3)
        || run.chunks.len()
            != usize::try_from(expected_rows.div_ceil(V36_EXTERNAL_ASSIGNMENT_SHARD_ROWS))
                .map_err(|_| invalid("V36 followup merge chunk count overflows"))?
    {
        return Err(invalid("V36 followup merge run inventory differs"));
    }
    let mut row_count = 0_u64;
    let mut previous_last = None;
    for (chunk_ordinal, chunk) in run.chunks.iter().enumerate() {
        let chunk_ordinal = u64::try_from(chunk_ordinal)
            .map_err(|_| invalid("V36 followup merge chunk ordinal overflows"))?;
        let preceding_rows = chunk_ordinal
            .checked_mul(V36_EXTERNAL_ASSIGNMENT_SHARD_ROWS)
            .ok_or_else(|| invalid("V36 followup merge chunk rows overflow"))?;
        let expected_chunk_rows = u32::try_from(
            expected_rows
                .checked_sub(preceding_rows)
                .ok_or_else(|| invalid("V36 followup merge chunk rows differ"))?
                .min(V36_EXTERNAL_ASSIGNMENT_SHARD_ROWS),
        )
        .map_err(|_| invalid("V36 followup merge chunk rows overflow"))?;
        let expected_uri = format!("{run_prefix}/chunk-{chunk_ordinal:06}.arrow");
        if chunk.context.chunk_ordinal != chunk_ordinal
            || chunk.context.generation_ordinal != generation.generation.generation_ordinal
            || chunk.context.group_ordinal != group_ordinal
            || chunk.context.model_identity != committed.admission.model_identity
            || chunk.context.predecessor_root_identity != generation.predecessor_root_identity
            || chunk.context.training_spec != committed.admission.training_spec
            || chunk.context.uri != expected_uri
            || chunk.row_count != expected_chunk_rows
            || chunk.encoded_bytes == 0
            || chunk.encoded_bytes > V36_EXTERNAL_ASSIGNMENT_MAXIMUM_ENCODED_BYTES
            || !valid_sha256(&chunk.sha256)
            || !valid_sha256(&chunk.blake3)
            || chunk.first_key > chunk.last_key
            || previous_last.is_some_and(|last| last >= chunk.first_key)
        {
            return Err(invalid("V36 followup merge chunk inventory differs"));
        }
        row_count = row_count
            .checked_add(u64::from(chunk.row_count))
            .ok_or_else(|| invalid("V36 followup merge rows overflow"))?;
        previous_last = Some(chunk.last_key);
    }
    if row_count != expected_rows {
        return Err(invalid("V36 followup merge row count differs"));
    }
    Ok(())
}

fn v36_assignment_merge_generation_root(
    committed: &V36CommittedSupercellAssignments,
    generation: V36ExternalMergeGenerationProjection,
    predecessor_root_identity: &V36ArtifactIdentity,
    runs: &[V36CommittedInitialAssignmentMergeRun],
) -> Result<(Vec<u8>, V36ArtifactIdentity)> {
    let uri = format!(
        "{}/merge/generation-{:06}/root.json",
        committed.uri_prefix, generation.generation_ordinal
    );
    let (run_inventory_sha256, run_inventory_blake3) =
        v36_initial_assignment_merge_run_inventory_digests(runs)?;
    let manifest = V36InitialAssignmentMergeGenerationRootManifest {
        format: V36_INITIAL_ASSIGNMENT_MERGE_GENERATION_ROOT_FORMAT.to_owned(),
        generation,
        merge_fan_in: committed.admission.projection.merge_fan_in,
        model_identity: committed.admission.model_identity.clone(),
        predecessor_root_identity: predecessor_root_identity.clone(),
        role: V36_INITIAL_ASSIGNMENT_MERGE_GENERATION_ROOT_ROLE.to_owned(),
        run_count: u64::try_from(runs.len())
            .map_err(|_| invalid("V36 merge generation run count overflows"))?,
        run_inventory_blake3,
        run_inventory_sha256,
        training_spec: committed.admission.training_spec.clone(),
        uri: uri.clone(),
    };
    let mut bytes = serde_json::to_vec(&v36_canonical_json_value(
        serde_json::to_value(manifest).map_err(|_| invalid("V36 merge generation root differs"))?,
    ))
    .map_err(|_| invalid("V36 merge generation root differs"))?;
    bytes
        .try_reserve_exact(1)
        .map_err(|_| invalid("V36 merge generation root exceeds capacity"))?;
    bytes.push(b'\n');
    let encoded_bytes = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
    if encoded_bytes > V36_INITIAL_ASSIGNMENT_MERGE_GENERATION_ROOT_MAXIMUM_ENCODED_BYTES {
        return Err(invalid(
            "V36 merge generation root exceeds encoded admission",
        ));
    }
    let identity = V36ArtifactIdentity {
        blake3: blake3::hash(&bytes).to_hex().to_string(),
        encoded_bytes,
        role: V36_INITIAL_ASSIGNMENT_MERGE_GENERATION_ROOT_ROLE.to_owned(),
        sha256: format!("{:x}", Sha256::digest(&bytes)),
        uri,
    };
    Ok((bytes, identity))
}

fn validate_v36_committed_assignment_merge_generation(
    committed: &V36CommittedSupercellAssignments,
    generation: &V36CommittedInitialAssignmentMergeGeneration,
) -> Result<()> {
    if generation.generation.generation_ordinal == 0 {
        return validate_v36_committed_initial_assignment_merge_generation(committed, generation);
    }
    let schedule = committed.admission.projection.merge_schedule()?;
    let expected = schedule
        .get(
            usize::try_from(generation.generation.generation_ordinal)
                .map_err(|_| invalid("V36 merge generation ordinal overflows"))?,
        )
        .ok_or_else(|| invalid("V36 merge generation differs"))?;
    if &generation.generation != expected
        || generation.merge_fan_in != committed.admission.projection.merge_fan_in
        || generation.runs.len()
            != usize::try_from(expected.output_run_count)
                .map_err(|_| invalid("V36 merge generation run count overflows"))?
    {
        return Err(invalid("V36 committed followup merge generation differs"));
    }
    for (group_ordinal, run) in generation.runs.iter().enumerate() {
        validate_v36_committed_followup_assignment_merge_run(
            committed,
            generation,
            u64::try_from(group_ordinal)
                .map_err(|_| invalid("V36 followup merge group ordinal overflows"))?,
            run,
        )?;
    }
    let (_, expected_root) = v36_assignment_merge_generation_root(
        committed,
        generation.generation,
        &generation.predecessor_root_identity,
        &generation.runs,
    )?;
    if generation.root_identity != expected_root {
        return Err(invalid(
            "V36 committed followup merge generation root differs",
        ));
    }
    Ok(())
}

/// Authenticate one persisted merge-generation root over exact committed runs.
pub fn authenticate_v36_assignment_merge_generation_root(
    committed: &V36CommittedSupercellAssignments,
    predecessor: Option<&V36CommittedInitialAssignmentMergeGeneration>,
    runs: Vec<V36CommittedInitialAssignmentMergeRun>,
    bytes: &[u8],
    root_identity: &V36ArtifactIdentity,
) -> Result<V36CommittedInitialAssignmentMergeGeneration> {
    let schedule = committed.admission.projection.merge_schedule()?;
    let (generation_ordinal, predecessor_root_identity) = match predecessor {
        Some(predecessor) => {
            validate_v36_committed_assignment_merge_generation(committed, predecessor)?;
            (
                predecessor
                    .generation
                    .generation_ordinal
                    .checked_add(1)
                    .ok_or_else(|| invalid("V36 merge generation ordinal overflows"))?,
                predecessor.root_identity.clone(),
            )
        }
        None => (0, committed.root_identity.clone()),
    };
    let generation = *schedule
        .get(
            usize::try_from(generation_ordinal)
                .map_err(|_| invalid("V36 merge generation ordinal overflows"))?,
        )
        .ok_or_else(|| invalid("V36 merge generation differs"))?;
    let uri = format!(
        "{}/merge/generation-{generation_ordinal:06}/root.json",
        committed.uri_prefix
    );
    let encoded_bytes = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
    if bytes.last() != Some(&b'\n')
        || root_identity.encoded_bytes != encoded_bytes
        || encoded_bytes > V36_INITIAL_ASSIGNMENT_MERGE_GENERATION_ROOT_MAXIMUM_ENCODED_BYTES
        || root_identity.role != V36_INITIAL_ASSIGNMENT_MERGE_GENERATION_ROOT_ROLE
        || root_identity.uri != uri
        || !valid_sha256(&root_identity.sha256)
        || !valid_sha256(&root_identity.blake3)
        || root_identity.sha256 != format!("{:x}", Sha256::digest(bytes))
        || root_identity.blake3 != blake3::hash(bytes).to_hex().as_str()
    {
        return Err(invalid("V36 merge generation root identity differs"));
    }
    let manifest: V36InitialAssignmentMergeGenerationRootManifest =
        serde_json::from_slice(bytes)
            .map_err(|_| invalid("V36 merge generation root manifest differs"))?;
    let mut canonical = serde_json::to_vec(&v36_canonical_json_value(
        serde_json::to_value(&manifest)
            .map_err(|_| invalid("V36 merge generation root manifest differs"))?,
    ))
    .map_err(|_| invalid("V36 merge generation root manifest differs"))?;
    canonical
        .try_reserve_exact(1)
        .map_err(|_| invalid("V36 merge generation root manifest exceeds capacity"))?;
    canonical.push(b'\n');
    let (run_inventory_sha256, run_inventory_blake3) =
        v36_initial_assignment_merge_run_inventory_digests(&runs)?;
    if canonical != bytes
        || manifest.format != V36_INITIAL_ASSIGNMENT_MERGE_GENERATION_ROOT_FORMAT
        || manifest.generation != generation
        || manifest.merge_fan_in != committed.admission.projection.merge_fan_in
        || manifest.model_identity != committed.admission.model_identity
        || manifest.predecessor_root_identity != predecessor_root_identity
        || manifest.role != V36_INITIAL_ASSIGNMENT_MERGE_GENERATION_ROOT_ROLE
        || manifest.run_count
            != u64::try_from(runs.len())
                .map_err(|_| invalid("V36 merge generation run count overflows"))?
        || manifest.run_inventory_blake3 != run_inventory_blake3
        || manifest.run_inventory_sha256 != run_inventory_sha256
        || manifest.training_spec != committed.admission.training_spec
        || manifest.uri != uri
    {
        return Err(invalid("V36 merge generation root manifest differs"));
    }
    let authenticated = V36CommittedInitialAssignmentMergeGeneration {
        generation,
        merge_fan_in: manifest.merge_fan_in,
        predecessor_root_identity,
        root_identity: root_identity.clone(),
        runs,
    };
    validate_v36_committed_assignment_merge_generation(committed, &authenticated)?;
    Ok(authenticated)
}

/// Commit one complete follow-up merge generation after all of its runs commit.
pub fn commit_v36_followup_assignment_merge_generation(
    committed: &V36CommittedSupercellAssignments,
    plan: V36FollowupAssignmentMergeGeneration,
    runs: Vec<V36CommittedInitialAssignmentMergeRun>,
    sink: &mut dyn V36InitialAssignmentMergeGenerationSink,
) -> Result<V36CommittedInitialAssignmentMergeGeneration> {
    validate_v36_committed_assignment_merge_generation(committed, &plan.predecessor)?;
    if plan.predecessor.merge_fan_in != committed.admission.projection.merge_fan_in {
        return Err(invalid("V36 followup merge generation fan-in differs"));
    }
    if runs.len()
        != usize::try_from(plan.generation.output_run_count)
            .map_err(|_| invalid("V36 followup merge generation run count overflows"))?
    {
        return Err(invalid(
            "V36 followup merge generation run inventory differs",
        ));
    }
    let predecessor_root_identity = plan.predecessor.root_identity.clone();
    let generation = V36CommittedInitialAssignmentMergeGeneration {
        generation: plan.generation,
        merge_fan_in: plan.predecessor.merge_fan_in,
        predecessor_root_identity,
        root_identity: V36ArtifactIdentity {
            blake3: String::new(),
            encoded_bytes: 0,
            role: String::new(),
            sha256: String::new(),
            uri: String::new(),
        },
        runs,
    };
    for (group_ordinal, run) in generation.runs.iter().enumerate() {
        validate_v36_committed_followup_assignment_merge_run(
            committed,
            &generation,
            u64::try_from(group_ordinal)
                .map_err(|_| invalid("V36 followup merge group ordinal overflows"))?,
            run,
        )?;
    }
    let (root_bytes, root_identity) = v36_assignment_merge_generation_root(
        committed,
        generation.generation,
        &generation.predecessor_root_identity,
        &generation.runs,
    )?;
    let committed_generation = V36CommittedInitialAssignmentMergeGeneration {
        root_identity,
        ..generation
    };
    validate_v36_committed_assignment_merge_generation(committed, &committed_generation)?;
    sink.commit_root(
        &committed_generation.runs,
        &root_bytes,
        &committed_generation.root_identity,
    )?;
    Ok(committed_generation)
}

/// Plan generation one from the exact committed generation-zero inventory.
pub fn plan_v36_followup_assignment_merge_generation(
    committed: &V36CommittedSupercellAssignments,
    predecessor: V36CommittedInitialAssignmentMergeGeneration,
) -> Result<Option<V36FollowupAssignmentMergeGeneration>> {
    validate_v36_committed_assignment_merge_generation(committed, &predecessor)?;
    let schedule = committed.admission.projection.merge_schedule()?;
    let next_generation = predecessor
        .generation
        .generation_ordinal
        .checked_add(1)
        .ok_or_else(|| invalid("V36 followup merge generation overflows"))?;
    let Some(generation) = schedule
        .get(
            usize::try_from(next_generation)
                .map_err(|_| invalid("V36 followup merge generation overflows"))?,
        )
        .copied()
    else {
        return Ok(None);
    };
    if generation.generation_ordinal != next_generation
        || generation.input_run_count
            != u64::try_from(predecessor.runs.len())
                .map_err(|_| invalid("V36 followup merge input count overflows"))?
    {
        return Err(invalid("V36 followup merge generation differs"));
    }
    let output_count = usize::try_from(generation.output_run_count)
        .map_err(|_| invalid("V36 followup merge output count overflows"))?;
    let fan_in = usize::from(predecessor.merge_fan_in);
    let mut groups = Vec::new();
    groups
        .try_reserve_exact(output_count)
        .map_err(|_| invalid("V36 followup merge groups exceed capacity"))?;
    for group_ordinal in 0..output_count {
        let input_start = group_ordinal
            .checked_mul(fan_in)
            .ok_or_else(|| invalid("V36 followup merge input range overflows"))?;
        let input_end = input_start
            .checked_add(fan_in)
            .ok_or_else(|| invalid("V36 followup merge input range overflows"))?
            .min(predecessor.runs.len());
        let output_row_count = predecessor.runs[input_start..input_end]
            .iter()
            .flat_map(|run| run.chunks.iter())
            .try_fold(0_u64, |rows, chunk| {
                rows.checked_add(u64::from(chunk.row_count))
                    .ok_or_else(|| invalid("V36 followup merge output rows overflow"))
            })?;
        if output_row_count == 0 {
            return Err(invalid("V36 followup merge output rows differ"));
        }
        let group_ordinal = u64::try_from(group_ordinal)
            .map_err(|_| invalid("V36 followup merge group ordinal overflows"))?;
        let run_prefix = format!(
            "{}/merge/generation-{:06}/run-{group_ordinal:06}",
            committed.uri_prefix, generation.generation_ordinal
        );
        let output_root_uri = format!("{run_prefix}/root.json");
        let output_chunk_count = output_row_count.div_ceil(V36_EXTERNAL_ASSIGNMENT_SHARD_ROWS);
        let last_chunk_uri = format!("{run_prefix}/chunk-{:06}.arrow", output_chunk_count - 1);
        if !valid_v36_supercell_model_uri(&output_root_uri)
            || !valid_v36_supercell_model_uri(&last_chunk_uri)
        {
            return Err(invalid("V36 followup merge output URI differs"));
        }
        groups.push(V36InitialAssignmentMergeGroup {
            group_ordinal,
            input_range: input_start..input_end,
            output_chunk_count,
            output_root_uri,
            output_row_count,
        });
    }
    Ok(Some(V36FollowupAssignmentMergeGeneration {
        generation,
        groups,
        predecessor,
    }))
}

/// Authenticate one committed initial-merge root and its exact chunk inventory.
pub fn authenticate_v36_initial_assignment_merge_run_root(
    committed: &V36CommittedSupercellAssignments,
    group_ordinal: u64,
    bytes: &[u8],
    root_identity: &V36ArtifactIdentity,
) -> Result<V36CommittedInitialAssignmentMergeRun> {
    let plan = plan_v36_initial_assignment_merge_generation(committed)?
        .ok_or_else(|| invalid("V36 initial merge generation is absent"))?;
    let group = v36_initial_assignment_merge_group(&plan, group_ordinal)?;
    let expected_inputs = &committed.artifacts[group.input_range.clone()];
    let encoded_bytes = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
    if bytes.last() != Some(&b'\n')
        || root_identity.encoded_bytes != encoded_bytes
        || encoded_bytes > V36_INITIAL_ASSIGNMENT_MERGE_ROOT_MAXIMUM_ENCODED_BYTES
        || root_identity.role != V36_INITIAL_ASSIGNMENT_MERGE_ROOT_ROLE
        || root_identity.uri != group.output_root_uri
        || !valid_sha256(&root_identity.sha256)
        || !valid_sha256(&root_identity.blake3)
        || root_identity.sha256 != format!("{:x}", Sha256::digest(bytes))
        || root_identity.blake3 != blake3::hash(bytes).to_hex().as_str()
    {
        return Err(invalid("V36 initial merge root identity differs"));
    }
    let manifest: V36InitialAssignmentMergeRootManifest = serde_json::from_slice(bytes)
        .map_err(|_| invalid("V36 initial merge root manifest differs"))?;
    let mut canonical = serde_json::to_vec(&v36_canonical_json_value(
        serde_json::to_value(&manifest)
            .map_err(|_| invalid("V36 initial merge root manifest differs"))?,
    ))
    .map_err(|_| invalid("V36 initial merge root manifest differs"))?;
    canonical
        .try_reserve_exact(1)
        .map_err(|_| invalid("V36 initial merge root manifest exceeds capacity"))?;
    canonical.push(b'\n');
    if canonical != bytes
        || manifest.format != V36_INITIAL_ASSIGNMENT_MERGE_ROOT_FORMAT
        || manifest.generation_ordinal != plan.generation.generation_ordinal
        || manifest.group_ordinal != group_ordinal
        || manifest.model_identity != plan.model_identity
        || manifest.predecessor_root_identity != plan.predecessor_root_identity
        || manifest.role != V36_INITIAL_ASSIGNMENT_MERGE_ROOT_ROLE
        || manifest.row_count != group.output_row_count
        || manifest.training_spec != plan.training_spec
        || manifest.uri != group.output_root_uri
        || manifest.inputs.len() != group.input_range.end - group.input_range.start
        || manifest.chunks.len()
            != usize::try_from(group.output_chunk_count)
                .map_err(|_| invalid("V36 initial merge chunk count overflows"))?
    {
        return Err(invalid("V36 initial merge root manifest differs"));
    }

    let input_prefix = plan
        .predecessor_root_identity
        .uri
        .strip_suffix("/assignment-root.json")
        .ok_or_else(|| invalid("V36 initial merge predecessor URI differs"))?;
    let mut input_rows = 0_u64;
    for (offset, (input, expected_input)) in manifest.inputs.iter().zip(expected_inputs).enumerate()
    {
        let shard_ordinal = u64::try_from(group.input_range.start + offset)
            .map_err(|_| invalid("V36 initial merge input ordinal overflows"))?;
        let expected_uri = format!("{input_prefix}/shard-{shard_ordinal:06}.arrow");
        let context = V36SupercellAssignmentShardContext {
            model_identity: input.context.model_identity.clone(),
            projected_corpus_sha256: input.context.projected_corpus_sha256.clone(),
            shard_ordinal: input.context.shard_ordinal,
            training_spec: input.context.training_spec.clone(),
            uri: input.context.uri.clone(),
        };
        validate_v36_assignment_context(&context)?;
        let start = shard_ordinal
            .checked_mul(V36_EXTERNAL_ASSIGNMENT_SHARD_ROWS)
            .ok_or_else(|| invalid("V36 initial merge input range overflows"))?;
        let expected_rows = plan
            .training_spec
            .corpus_rows
            .min(
                start
                    .checked_add(V36_EXTERNAL_ASSIGNMENT_SHARD_ROWS)
                    .ok_or_else(|| invalid("V36 initial merge input range overflows"))?,
            )
            .checked_sub(start)
            .ok_or_else(|| invalid("V36 initial merge input range differs"))?;
        if input != &V36SupercellAssignmentShardArtifactWire::from(expected_input)
            || context.model_identity != plan.model_identity
            || context.training_spec != plan.training_spec
            || context.projected_corpus_sha256 != plan.training_spec.projected_corpus_sha256
            || context.shard_ordinal != shard_ordinal
            || context.uri != expected_uri
            || u64::from(input.row_count) != expected_rows
            || input.encoded_bytes == 0
            || input.encoded_bytes > V36_EXTERNAL_ASSIGNMENT_MAXIMUM_ENCODED_BYTES
            || !valid_sha256(&input.sha256)
            || !valid_sha256(&input.blake3)
        {
            return Err(invalid("V36 initial merge root input inventory differs"));
        }
        input_rows = input_rows
            .checked_add(u64::from(input.row_count))
            .ok_or_else(|| invalid("V36 initial merge input rows overflow"))?;
    }
    if input_rows != group.output_row_count {
        return Err(invalid("V36 initial merge root input rows differ"));
    }

    let mut chunks = Vec::new();
    chunks
        .try_reserve_exact(manifest.chunks.len())
        .map_err(|_| invalid("V36 initial merge chunk inventory exceeds capacity"))?;
    let mut chunk_rows = 0_u64;
    let mut previous_last = None;
    for (chunk_ordinal, chunk) in manifest.chunks.into_iter().enumerate() {
        let chunk_ordinal = u64::try_from(chunk_ordinal)
            .map_err(|_| invalid("V36 initial merge chunk ordinal overflows"))?;
        let (expected_context, expected_rows) =
            v36_initial_assignment_merge_chunk_context(&plan, group_ordinal, chunk_ordinal)?;
        let context = V36InitialAssignmentMergeChunkContext {
            chunk_ordinal: chunk.context.chunk_ordinal,
            generation_ordinal: chunk.context.generation_ordinal,
            group_ordinal: chunk.context.group_ordinal,
            model_identity: chunk.context.model_identity,
            predecessor_root_identity: chunk.context.predecessor_root_identity,
            training_spec: chunk.context.training_spec,
            uri: chunk.context.uri,
        };
        if context != expected_context
            || chunk.row_count != expected_rows
            || chunk.encoded_bytes == 0
            || chunk.encoded_bytes > V36_EXTERNAL_ASSIGNMENT_MAXIMUM_ENCODED_BYTES
            || !valid_sha256(&chunk.sha256)
            || !valid_sha256(&chunk.blake3)
            || chunk.first_key > chunk.last_key
            || previous_last.is_some_and(|last| last >= chunk.first_key)
        {
            return Err(invalid("V36 initial merge root chunk inventory differs"));
        }
        chunk_rows = chunk_rows
            .checked_add(u64::from(chunk.row_count))
            .ok_or_else(|| invalid("V36 initial merge chunk rows overflow"))?;
        previous_last = Some(chunk.last_key);
        chunks.push(V36InitialAssignmentMergeChunkArtifact {
            blake3: chunk.blake3,
            context,
            encoded_bytes: chunk.encoded_bytes,
            first_key: chunk.first_key,
            last_key: chunk.last_key,
            row_count: chunk.row_count,
            sha256: chunk.sha256,
        });
    }
    if chunk_rows != group.output_row_count {
        return Err(invalid("V36 initial merge root chunk rows differ"));
    }
    Ok(V36CommittedInitialAssignmentMergeRun {
        chunks,
        root_identity: root_identity.clone(),
    })
}

fn validate_v36_initial_assignment_merge_inputs(
    authenticated: &V36AuthenticatedInitialAssignmentMergeGroup,
) -> Result<&V36InitialAssignmentMergeGroup> {
    let group =
        v36_initial_assignment_merge_group(&authenticated.plan, authenticated.group_ordinal)?;
    let expected_inputs = group.input_range.end - group.input_range.start;
    if authenticated.inputs.len() != expected_inputs
        || authenticated.input_artifacts.len() != expected_inputs
    {
        return Err(invalid("V36 initial merge input inventory differs"));
    }
    let mut row_count = 0_u64;
    for (offset, (artifact, rows)) in authenticated
        .input_artifacts
        .iter()
        .zip(&authenticated.inputs)
        .enumerate()
    {
        let expected_shard_ordinal = u64::try_from(group.input_range.start + offset)
            .map_err(|_| invalid("V36 initial merge shard ordinal overflows"))?;
        if rows.is_empty()
            || rows.len() > usize::try_from(V36_EXTERNAL_ASSIGNMENT_SHARD_ROWS).unwrap()
            || artifact.context.shard_ordinal != expected_shard_ordinal
            || artifact.context.model_identity != authenticated.plan.model_identity
            || artifact.context.training_spec != authenticated.plan.training_spec
            || artifact.context.projected_corpus_sha256
                != authenticated.plan.training_spec.projected_corpus_sha256
            || usize::try_from(artifact.row_count).ok() != Some(rows.len())
            || artifact.encoded_bytes == 0
            || !valid_sha256(&artifact.sha256)
            || !valid_sha256(&artifact.blake3)
            || !valid_v36_supercell_model_uri(&artifact.context.uri)
        {
            return Err(invalid("V36 initial merge input authority differs"));
        }
        validate_v36_initial_assignment_merge_rows(
            &authenticated.plan.training_spec,
            rows,
            artifact.row_count,
        )?;
        row_count = row_count
            .checked_add(u64::from(artifact.row_count))
            .ok_or_else(|| invalid("V36 initial merge input rows overflow"))?;
    }
    if row_count != group.output_row_count {
        return Err(invalid("V36 initial merge input row total differs"));
    }
    Ok(group)
}

fn v36_initial_assignment_merge_root(
    authenticated: &V36AuthenticatedInitialAssignmentMergeGroup,
    chunks: &[V36InitialAssignmentMergeChunkArtifact],
) -> Result<(Vec<u8>, V36ArtifactIdentity)> {
    let group = validate_v36_initial_assignment_merge_inputs(authenticated)?;
    if chunks.len()
        != usize::try_from(group.output_chunk_count)
            .map_err(|_| invalid("V36 initial merge chunk count overflows"))?
    {
        return Err(invalid("V36 initial merge chunk inventory differs"));
    }
    let mut row_count = 0_u64;
    let mut previous_last = None;
    for (chunk_ordinal, chunk) in chunks.iter().enumerate() {
        if chunk.context.group_ordinal != authenticated.group_ordinal
            || chunk.context.chunk_ordinal
                != u64::try_from(chunk_ordinal)
                    .map_err(|_| invalid("V36 initial merge chunk ordinal overflows"))?
            || chunk.context.predecessor_root_identity
                != authenticated.plan.predecessor_root_identity
            || previous_last.is_some_and(|last| last >= chunk.first_key)
        {
            return Err(invalid("V36 initial merge chunk inventory differs"));
        }
        row_count = row_count
            .checked_add(u64::from(chunk.row_count))
            .ok_or_else(|| invalid("V36 initial merge chunk rows overflow"))?;
        previous_last = Some(chunk.last_key);
    }
    if row_count != group.output_row_count {
        return Err(invalid("V36 initial merge chunk row total differs"));
    }
    let manifest = V36InitialAssignmentMergeRootManifest {
        chunks: chunks
            .iter()
            .map(V36InitialAssignmentMergeChunkArtifactWire::from)
            .collect(),
        format: V36_INITIAL_ASSIGNMENT_MERGE_ROOT_FORMAT.to_owned(),
        generation_ordinal: authenticated.plan.generation.generation_ordinal,
        group_ordinal: authenticated.group_ordinal,
        inputs: authenticated
            .input_artifacts
            .iter()
            .map(V36SupercellAssignmentShardArtifactWire::from)
            .collect(),
        model_identity: authenticated.plan.model_identity.clone(),
        predecessor_root_identity: authenticated.plan.predecessor_root_identity.clone(),
        role: V36_INITIAL_ASSIGNMENT_MERGE_ROOT_ROLE.to_owned(),
        row_count,
        training_spec: authenticated.plan.training_spec.clone(),
        uri: group.output_root_uri.clone(),
    };
    let mut bytes = serde_json::to_vec(&v36_canonical_json_value(
        serde_json::to_value(manifest)
            .map_err(|_| invalid("V36 initial merge root manifest differs"))?,
    ))
    .map_err(|_| invalid("V36 initial merge root manifest differs"))?;
    bytes
        .try_reserve_exact(1)
        .map_err(|_| invalid("V36 initial merge root exceeds capacity"))?;
    bytes.push(b'\n');
    if u64::try_from(bytes.len()).unwrap_or(u64::MAX)
        > V36_INITIAL_ASSIGNMENT_MERGE_ROOT_MAXIMUM_ENCODED_BYTES
    {
        return Err(invalid("V36 initial merge root exceeds encoded admission"));
    }
    let identity = V36ArtifactIdentity {
        blake3: blake3::hash(&bytes).to_hex().to_string(),
        encoded_bytes: u64::try_from(bytes.len())
            .map_err(|_| invalid("V36 initial merge root length overflows"))?,
        role: V36_INITIAL_ASSIGNMENT_MERGE_ROOT_ROLE.to_owned(),
        sha256: format!("{:x}", Sha256::digest(&bytes)),
        uri: group.output_root_uri.clone(),
    };
    Ok((bytes, identity))
}

/// Deterministically merge one authenticated initial group and commit its root last.
pub fn write_v36_initial_assignment_merge_group(
    authenticated: &V36AuthenticatedInitialAssignmentMergeGroup,
    sink: &mut dyn V36InitialAssignmentMergeRunSink,
) -> Result<V36CommittedInitialAssignmentMergeRun> {
    let execute = (|| {
        let group = validate_v36_initial_assignment_merge_inputs(authenticated)?;
        let total_rows = usize::try_from(group.output_row_count)
            .map_err(|_| invalid("V36 initial merge total rows overflow"))?;
        let mut heap = BinaryHeap::new();
        heap.try_reserve(authenticated.inputs.len())
            .map_err(|_| invalid("V36 initial merge heap exceeds capacity"))?;
        for (input_ordinal, input) in authenticated.inputs.iter().enumerate() {
            let row = &input[0];
            heap.push(std::cmp::Reverse((
                row.supercell_ordinal,
                row.source_ordinal,
                input_ordinal,
                0_usize,
            )));
        }
        let output_capacity = usize::try_from(V36_EXTERNAL_ASSIGNMENT_SHARD_ROWS).unwrap();
        let mut output = Vec::new();
        output
            .try_reserve_exact(output_capacity.min(total_rows))
            .map_err(|_| invalid("V36 initial merge output exceeds capacity"))?;
        let mut chunks = Vec::new();
        chunks
            .try_reserve_exact(
                usize::try_from(group.output_chunk_count)
                    .map_err(|_| invalid("V36 initial merge chunk count overflows"))?,
            )
            .map_err(|_| invalid("V36 initial merge chunk inventory exceeds capacity"))?;
        let mut source_ordinals = HashSet::new();
        source_ordinals
            .try_reserve(total_rows)
            .map_err(|_| invalid("V36 initial merge source set exceeds capacity"))?;
        let mut previous_key = None;
        while let Some(std::cmp::Reverse((cell, source, input_ordinal, row_ordinal))) = heap.pop() {
            let key = (cell, source);
            if previous_key.is_some_and(|previous| previous >= key)
                || !source_ordinals.insert(source)
            {
                return Err(invalid("V36 initial merge global order differs"));
            }
            let row = &authenticated.inputs[input_ordinal][row_ordinal];
            output.push(row.clone());
            previous_key = Some(key);
            let next_ordinal = row_ordinal + 1;
            if let Some(next) = authenticated.inputs[input_ordinal].get(next_ordinal) {
                heap.push(std::cmp::Reverse((
                    next.supercell_ordinal,
                    next.source_ordinal,
                    input_ordinal,
                    next_ordinal,
                )));
            }
            if output.len() == output_capacity {
                let chunk_ordinal = u64::try_from(chunks.len())
                    .map_err(|_| invalid("V36 initial merge chunk ordinal overflows"))?;
                let (bytes, artifact) = encode_v36_initial_assignment_merge_chunk_arrow(
                    &authenticated.plan,
                    authenticated.group_ordinal,
                    chunk_ordinal,
                    &output,
                )?;
                sink.write_chunk_provisional(&bytes, &artifact)?;
                chunks.push(artifact);
                output.clear();
            }
        }
        if !output.is_empty() {
            let chunk_ordinal = u64::try_from(chunks.len())
                .map_err(|_| invalid("V36 initial merge chunk ordinal overflows"))?;
            let (bytes, artifact) = encode_v36_initial_assignment_merge_chunk_arrow(
                &authenticated.plan,
                authenticated.group_ordinal,
                chunk_ordinal,
                &output,
            )?;
            sink.write_chunk_provisional(&bytes, &artifact)?;
            chunks.push(artifact);
        }
        let (root_bytes, root_identity) =
            v36_initial_assignment_merge_root(authenticated, &chunks)?;
        sink.commit_root(&chunks, &root_bytes, &root_identity)?;
        Ok(V36CommittedInitialAssignmentMergeRun {
            chunks,
            root_identity,
        })
    })();
    match execute {
        Ok(committed) => Ok(committed),
        Err(source) => match sink.abort() {
            Ok(()) => Err(source),
            Err(cleanup) => Err(cleanup),
        },
    }
}

fn v36_committed_assignment_root(
    admission: &V36AdmittedSupercellAssignmentPreflight,
    artifacts: &[V36SupercellAssignmentShardArtifact],
    uri_prefix: &str,
) -> Result<(Vec<u8>, V36ArtifactIdentity)> {
    let mut artifact_wires = Vec::new();
    artifact_wires
        .try_reserve_exact(artifacts.len())
        .map_err(|_| invalid("V36 assignment root inventory exceeds capacity"))?;
    artifact_wires.extend(
        artifacts
            .iter()
            .map(V36SupercellAssignmentShardArtifactWire::from),
    );
    let manifest = V36CommittedSupercellAssignmentsManifest {
        artifacts: artifact_wires,
        format: V36_EXTERNAL_ASSIGNMENT_ROOT_FORMAT.to_owned(),
        merge_fan_in: admission.projection.merge_fan_in,
        merge_schedule: admission.projection.merge_schedule()?,
        model_identity: admission.model_identity.clone(),
        projected_corpus_sha256: admission.training_spec.projected_corpus_sha256.clone(),
        role: V36_EXTERNAL_ASSIGNMENT_ROOT_ROLE.to_owned(),
        training_spec: admission.training_spec.clone(),
        uri_prefix: uri_prefix.to_owned(),
    };
    let mut bytes = serde_json::to_vec(&v36_canonical_json_value(
        serde_json::to_value(manifest)
            .map_err(|_| invalid("V36 assignment root manifest differs"))?,
    ))
    .map_err(|_| invalid("V36 assignment root manifest differs"))?;
    bytes
        .try_reserve_exact(1)
        .map_err(|_| invalid("V36 assignment root manifest exceeds capacity"))?;
    bytes.push(b'\n');
    if u64::try_from(bytes.len()).unwrap_or(u64::MAX)
        > V36_EXTERNAL_ASSIGNMENT_ROOT_MAXIMUM_ENCODED_BYTES
    {
        return Err(invalid("V36 assignment root exceeds encoded admission"));
    }
    let uri = format!("{uri_prefix}/assignment-root.json");
    let identity = V36ArtifactIdentity {
        blake3: blake3::hash(&bytes).to_hex().to_string(),
        encoded_bytes: u64::try_from(bytes.len())
            .map_err(|_| invalid("V36 assignment root length overflows"))?,
        role: V36_EXTERNAL_ASSIGNMENT_ROOT_ROLE.to_owned(),
        sha256: format!("{:x}", Sha256::digest(&bytes)),
        uri,
    };
    if !valid_v36_supercell_model_uri(&identity.uri) {
        return Err(invalid("V36 assignment root URI differs"));
    }
    Ok((bytes, identity))
}

/// Authenticate one persisted assignment root against the admitted execution.
pub fn authenticate_v36_supercell_assignment_root(
    admission: &V36AdmittedSupercellAssignmentPreflight,
    bytes: &[u8],
    root_identity: &V36ArtifactIdentity,
) -> Result<V36CommittedSupercellAssignments> {
    let encoded_bytes = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
    if bytes.last() != Some(&b'\n')
        || root_identity.encoded_bytes != encoded_bytes
        || encoded_bytes > V36_EXTERNAL_ASSIGNMENT_ROOT_MAXIMUM_ENCODED_BYTES
        || root_identity.role != V36_EXTERNAL_ASSIGNMENT_ROOT_ROLE
        || !valid_v36_supercell_model_uri(&root_identity.uri)
        || !valid_sha256(&root_identity.sha256)
        || !valid_sha256(&root_identity.blake3)
        || root_identity.sha256 != format!("{:x}", Sha256::digest(bytes))
        || root_identity.blake3 != blake3::hash(bytes).to_hex().as_str()
    {
        return Err(invalid("V36 assignment root identity differs"));
    }
    let manifest: V36CommittedSupercellAssignmentsManifest = serde_json::from_slice(bytes)
        .map_err(|_| invalid("V36 assignment root manifest differs"))?;
    let mut canonical = serde_json::to_vec(&v36_canonical_json_value(
        serde_json::to_value(&manifest)
            .map_err(|_| invalid("V36 assignment root manifest differs"))?,
    ))
    .map_err(|_| invalid("V36 assignment root manifest differs"))?;
    canonical
        .try_reserve_exact(1)
        .map_err(|_| invalid("V36 assignment root manifest exceeds capacity"))?;
    canonical.push(b'\n');
    let expected_shards = usize::try_from(admission.projection.logical_shards)
        .map_err(|_| invalid("V36 assignment shard count overflows"))?;
    if canonical != bytes
        || manifest.format != V36_EXTERNAL_ASSIGNMENT_ROOT_FORMAT
        || manifest.merge_fan_in != admission.projection.merge_fan_in
        || manifest.merge_schedule != admission.projection.merge_schedule()?
        || manifest.model_identity != admission.model_identity
        || manifest.projected_corpus_sha256 != admission.training_spec.projected_corpus_sha256
        || manifest.role != V36_EXTERNAL_ASSIGNMENT_ROOT_ROLE
        || manifest.training_spec != admission.training_spec
        || manifest.artifacts.len() != expected_shards
        || !valid_v36_supercell_model_uri(&manifest.uri_prefix)
        || root_identity.uri != format!("{}/assignment-root.json", manifest.uri_prefix)
    {
        return Err(invalid("V36 assignment root manifest differs"));
    }

    let mut artifacts = Vec::new();
    artifacts
        .try_reserve_exact(expected_shards)
        .map_err(|_| invalid("V36 assignment root inventory exceeds capacity"))?;
    let mut row_count = 0_u64;
    for (shard_ordinal, artifact) in manifest.artifacts.iter().enumerate() {
        let shard_ordinal = u64::try_from(shard_ordinal)
            .map_err(|_| invalid("V36 assignment shard ordinal overflows"))?;
        let start = shard_ordinal
            .checked_mul(V36_EXTERNAL_ASSIGNMENT_SHARD_ROWS)
            .ok_or_else(|| invalid("V36 assignment shard range overflows"))?;
        let expected_rows = u32::try_from(
            admission
                .training_spec
                .corpus_rows
                .min(
                    start
                        .checked_add(V36_EXTERNAL_ASSIGNMENT_SHARD_ROWS)
                        .ok_or_else(|| invalid("V36 assignment shard range overflows"))?,
                )
                .checked_sub(start)
                .ok_or_else(|| invalid("V36 assignment shard range differs"))?,
        )
        .map_err(|_| invalid("V36 assignment shard row count overflows"))?;
        let context = V36SupercellAssignmentShardContext {
            model_identity: artifact.context.model_identity.clone(),
            projected_corpus_sha256: artifact.context.projected_corpus_sha256.clone(),
            shard_ordinal: artifact.context.shard_ordinal,
            training_spec: artifact.context.training_spec.clone(),
            uri: artifact.context.uri.clone(),
        };
        validate_v36_assignment_context(&context)?;
        if context.model_identity != admission.model_identity
            || context.projected_corpus_sha256 != admission.training_spec.projected_corpus_sha256
            || context.shard_ordinal != shard_ordinal
            || context.training_spec != admission.training_spec
            || context.uri != format!("{}/shard-{shard_ordinal:06}.arrow", manifest.uri_prefix)
            || artifact.row_count != expected_rows
            || artifact.encoded_bytes == 0
            || artifact.encoded_bytes > V36_EXTERNAL_ASSIGNMENT_MAXIMUM_ENCODED_BYTES
            || !valid_sha256(&artifact.sha256)
            || !valid_sha256(&artifact.blake3)
        {
            return Err(invalid("V36 assignment root shard inventory differs"));
        }
        row_count = row_count
            .checked_add(u64::from(artifact.row_count))
            .ok_or_else(|| invalid("V36 assignment root rows overflow"))?;
        artifacts.push(V36SupercellAssignmentShardArtifact {
            blake3: artifact.blake3.clone(),
            context,
            encoded_bytes: artifact.encoded_bytes,
            row_count: artifact.row_count,
            sha256: artifact.sha256.clone(),
        });
    }
    if row_count != admission.training_spec.corpus_rows {
        return Err(invalid("V36 assignment root rows differ"));
    }
    let (expected_bytes, expected_identity) =
        v36_committed_assignment_root(admission, &artifacts, &manifest.uri_prefix)?;
    if expected_bytes != bytes || expected_identity != *root_identity {
        return Err(invalid("V36 assignment root authority differs"));
    }
    Ok(V36CommittedSupercellAssignments {
        admission: admission.clone(),
        artifacts,
        root_identity: root_identity.clone(),
        uri_prefix: manifest.uri_prefix,
    })
}

fn write_v36_assignment_buffer(
    model: &V36AuthenticatedSupercellModel,
    pool: &rayon::ThreadPool,
    uri_prefix: &str,
    sink: &mut dyn V36SupercellAssignmentShardSink,
    shard_ordinal: u64,
    buffered: &mut Vec<(u64, [f32; 192])>,
    artifacts: &mut Vec<V36SupercellAssignmentShardArtifact>,
) -> Result<()> {
    if buffered.is_empty() {
        return Ok(());
    }
    let centroids = model.model().centroids();
    let mut assigned = Vec::new();
    assigned
        .try_reserve_exact(buffered.len())
        .map_err(|_| invalid("V36 assignment result allocation exceeds capacity"))?;
    assigned.resize(
        buffered.len(),
        V36SupercellAssignmentRow {
            supercell_ordinal: 0,
            source_ordinal: 0,
            projected: [0.0; 192],
        },
    );
    pool.install(|| {
        buffered
            .par_iter()
            .zip(assigned.par_iter_mut())
            .try_for_each(|((source_ordinal, projected), assigned)| {
                let mut best = (squared_l2(projected, &centroids[0])?, 0_u32);
                for (ordinal, centroid) in centroids.iter().enumerate().skip(1) {
                    let distance = squared_l2(projected, centroid)?;
                    if distance < best.0 {
                        best = (
                            distance,
                            u32::try_from(ordinal)
                                .map_err(|_| invalid("V36 assignment supercell overflows"))?,
                        );
                    }
                }
                *assigned = V36SupercellAssignmentRow {
                    supercell_ordinal: best.1,
                    source_ordinal: *source_ordinal,
                    projected: *projected,
                };
                Ok::<(), BorsukError>(())
            })
    })?;
    assigned.sort_unstable_by_key(|row| (row.supercell_ordinal, row.source_ordinal));
    let uri = format!("{uri_prefix}/shard-{shard_ordinal:06}.arrow");
    let context = bind_v36_supercell_assignment_shard_context(model, shard_ordinal, &uri)?;
    let (bytes, artifact) = encode_v36_supercell_assignment_shard_arrow(&context, &assigned)?;
    sink.write_provisional(&bytes, &artifact)?;
    artifacts.push(artifact);
    buffered.clear();
    Ok(())
}

/// Assign one complete projected corpus into fixed, schedule-invariant shards.
///
/// The sink remains provisional until complete row ordering and replay digest
/// authority succeed. Any failure requests transactional cleanup through
/// `V36SupercellAssignmentShardSink::abort`.
pub fn write_v36_supercell_assignment_shards(
    model: &V36AuthenticatedSupercellModel,
    admission: &V36AdmittedSupercellAssignmentPreflight,
    source: &mut dyn V36ProjectedCorpusSource,
    uri_prefix: &str,
    sink: &mut dyn V36SupercellAssignmentShardSink,
) -> Result<V36CommittedSupercellAssignments> {
    let maximum_shard_rows = admission
        .training_spec
        .corpus_rows
        .min(V36_EXTERNAL_ASSIGNMENT_SHARD_ROWS);
    if admission.model_identity != *model.identity()
        || admission.training_spec != *model.training_spec()
        || admission.request.sort_rows_per_worker < maximum_shard_rows
        || !valid_v36_supercell_model_uri(uri_prefix)
    {
        return Err(invalid("V36 assignment execution authority differs"));
    }
    let execute = (|| {
        let worker_count = usize::from(admission.request.worker_count);
        let pool = ThreadPoolBuilder::new()
            .num_threads(worker_count)
            .build()
            .map_err(|_| invalid("V36 assignment workers differ"))?;
        let buffer_rows = usize::try_from(maximum_shard_rows)
            .map_err(|_| invalid("V36 assignment shard rows overflow"))?;
        let mut buffered = Vec::new();
        buffered
            .try_reserve_exact(buffer_rows)
            .map_err(|_| invalid("V36 assignment buffer allocation exceeds capacity"))?;
        let artifact_count = usize::try_from(admission.projection.logical_shards)
            .map_err(|_| invalid("V36 assignment shard count overflows"))?;
        let mut artifacts = Vec::new();
        artifacts
            .try_reserve_exact(artifact_count)
            .map_err(|_| invalid("V36 assignment artifact allocation exceeds capacity"))?;
        let mut shard_ordinal = 0_u64;
        let replay_sha256 = scan_v36_projected_corpus(
            &admission.training_spec,
            source,
            |source_ordinal, projected| {
                buffered.push((
                    source_ordinal,
                    projected
                        .try_into()
                        .map_err(|_| invalid("V36 assignment projected row differs"))?,
                ));
                if buffered.len() == buffer_rows {
                    write_v36_assignment_buffer(
                        model,
                        &pool,
                        uri_prefix,
                        sink,
                        shard_ordinal,
                        &mut buffered,
                        &mut artifacts,
                    )?;
                    shard_ordinal = shard_ordinal
                        .checked_add(1)
                        .ok_or_else(|| invalid("V36 assignment shard ordinal overflows"))?;
                }
                Ok(())
            },
        )?;
        if !buffered.is_empty() {
            write_v36_assignment_buffer(
                model,
                &pool,
                uri_prefix,
                sink,
                shard_ordinal,
                &mut buffered,
                &mut artifacts,
            )?;
        }
        if replay_sha256 != admission.training_spec.projected_corpus_sha256
            || artifacts.len() != artifact_count
        {
            return Err(invalid("V36 assignment corpus authority differs"));
        }
        let (root_bytes, root_identity) =
            v36_committed_assignment_root(admission, &artifacts, uri_prefix)?;
        sink.commit(&artifacts, &root_bytes, &root_identity)?;
        Ok(V36CommittedSupercellAssignments {
            admission: admission.clone(),
            artifacts,
            root_identity,
            uri_prefix: uri_prefix.to_owned(),
        })
    })();
    match execute {
        Ok(committed) => Ok(committed),
        Err(source) => match sink.abort() {
            Ok(()) => Err(source),
            Err(cleanup) => Err(cleanup),
        },
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Outcome-blind limits and calibration for external super-cell assignment.
pub struct V36SupercellAssignmentAdmissionRequest {
    /// Fixed construction workers; V36 registers only one, two, or four.
    pub worker_count: u8,
    /// Maximum queued projected rows owned by each worker.
    pub queue_rows_per_worker: u64,
    /// Maximum rows in each worker's in-memory sort buffer.
    pub sort_rows_per_worker: u64,
    /// Fixed external merge fan-in.
    pub merge_fan_in: u8,
    /// Component terms in the bounded calibration measurement.
    pub measured_component_terms: u128,
    /// Active nanoseconds in the bounded calibration measurement.
    pub measured_elapsed_ns: u64,
    /// Cost of the bounded calibration measurement in micro-US-dollars.
    pub measured_cost_microusd: u64,
    /// External sort/merge work units in the bounded I/O calibration.
    pub measured_external_work_units: u128,
    /// Active nanoseconds in the bounded external-work calibration.
    pub measured_external_elapsed_ns: u64,
    /// Cost of the external-work calibration in micro-US-dollars.
    pub measured_external_cost_microusd: u64,
    /// Hard active execution wall limit.
    pub maximum_active_wall_seconds: u64,
    /// Hard construction cost limit in micro-US-dollars.
    pub maximum_cost_microusd: u64,
    /// Hard aggregate process live-byte limit.
    pub maximum_peak_live_bytes: u64,
    /// Hard attempt-owned scratch-byte limit.
    pub maximum_scratch_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
/// One exact fixed-fan-in external merge generation.
pub struct V36ExternalMergeGenerationProjection {
    /// Zero-based generation ordinal after provisional assignment shards.
    pub generation_ordinal: u32,
    /// Authenticated predecessor runs consumed by this generation.
    pub input_run_count: u64,
    /// Deterministic runs produced by this generation.
    pub output_run_count: u64,
    /// Complete groups containing exactly the admitted fan-in.
    pub full_group_count: u64,
    /// Final partial group size, or zero when every group is full.
    pub tail_group_size: u8,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Checked conservative projection made before any corpus row is scanned.
pub struct V36SupercellAssignmentProjection {
    /// Fixed logical shard count including the final tail shard.
    pub logical_shards: u64,
    /// Fixed fan-in merge generations in the worst all-shards run.
    pub merge_generations: u32,
    /// Exact admitted merge fan-in used to derive every generation.
    pub merge_fan_in: u8,
    /// Maximum uncompressed assignment bytes including per-shard envelopes.
    pub uncompressed_assignment_bytes: u64,
    /// Scratch required for input/output overlap and bounded worker buffers.
    pub required_scratch_bytes: u64,
    /// Aggregate live bytes charged to all workers plus the model envelope.
    pub required_peak_live_bytes: u64,
    /// Packed one-bit-per-source exact-coverage proof used by final publication.
    pub coverage_bitmap_bytes: u64,
    /// Exact assignment distance component terms.
    pub component_terms: u128,
    /// Conservative sort comparisons plus assignment/merge row visits.
    pub external_work_units: u128,
    /// Complete terminal-run rows decoded and republished into per-cell chunks.
    pub publication_row_visits: u128,
    /// Ceiling-scaled active assignment time.
    pub projected_active_ns: u128,
    /// Ceiling-scaled assignment cost in micro-US-dollars.
    pub projected_cost_microusd: u64,
}

impl V36SupercellAssignmentProjection {
    /// Derive the only legal fixed-fan-in generation schedule.
    pub fn merge_schedule(&self) -> Result<Vec<V36ExternalMergeGenerationProjection>> {
        if self.logical_shards == 0 || !(2..=64).contains(&self.merge_fan_in) {
            return Err(invalid("V36 external assignment merge schedule differs"));
        }
        let fan_in = u64::from(self.merge_fan_in);
        let mut schedule = Vec::new();
        schedule
            .try_reserve_exact(
                usize::try_from(self.merge_generations)
                    .map_err(|_| invalid("V36 external assignment merge schedule overflows"))?,
            )
            .map_err(|_| invalid("V36 external assignment merge schedule exceeds capacity"))?;
        let mut input_run_count = self.logical_shards;
        let mut generation_ordinal = 0_u32;
        while input_run_count > 1 {
            let full_group_count = input_run_count / fan_in;
            let remainder = input_run_count % fan_in;
            let tail_group_size = u8::try_from(remainder)
                .map_err(|_| invalid("V36 external assignment merge schedule overflows"))?;
            let output_run_count = input_run_count.div_ceil(fan_in);
            schedule.push(V36ExternalMergeGenerationProjection {
                generation_ordinal,
                input_run_count,
                output_run_count,
                full_group_count,
                tail_group_size,
            });
            input_run_count = output_run_count;
            generation_ordinal = generation_ordinal
                .checked_add(1)
                .ok_or_else(|| invalid("V36 external assignment merge schedule overflows"))?;
        }
        if schedule.len()
            != usize::try_from(self.merge_generations)
                .map_err(|_| invalid("V36 external assignment merge schedule overflows"))?
        {
            return Err(invalid("V36 external assignment merge schedule differs"));
        }
        Ok(schedule)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Pre-assignment authority bound to one authenticated model and exact request.
///
/// This does not authorize post-count Hamilton allocation or local Lloyd work.
pub struct V36AdmittedSupercellAssignmentPreflight {
    model_identity: V36ArtifactIdentity,
    training_spec: V36SupercellTrainingSpec,
    request: V36SupercellAssignmentAdmissionRequest,
    projection: V36SupercellAssignmentProjection,
}

impl V36AdmittedSupercellAssignmentPreflight {
    /// Exact authenticated model identity inseparable from this admission.
    pub fn model_identity(&self) -> &V36ArtifactIdentity {
        &self.model_identity
    }

    /// Exact query-excluded corpus and model-training authority.
    pub fn training_spec(&self) -> &V36SupercellTrainingSpec {
        &self.training_spec
    }

    /// Complete admitted worker, buffering, calibration, and limit request.
    pub fn request(&self) -> &V36SupercellAssignmentAdmissionRequest {
        &self.request
    }

    /// Checked resource, work, wall, and cost projection.
    pub const fn projection(&self) -> &V36SupercellAssignmentProjection {
        &self.projection
    }
}

/// Project external assignment without allocating the projected population.
pub fn project_v36_supercell_assignment_admission(
    spec: &V36SupercellTrainingSpec,
    model_encoded_bytes: u64,
    request: &V36SupercellAssignmentAdmissionRequest,
) -> Result<V36SupercellAssignmentProjection> {
    if spec.corpus_rows == 0
        || spec.dimensions != 192
        || spec.maximum_block_rows == 0
        || spec.maximum_block_rows > 65_536
        || !valid_sha256(&spec.projected_corpus_sha256)
        || spec.reservoir_rows == 0
        || spec.reservoir_rows > spec.corpus_rows
        || spec.reservoir_rows > 1_048_576
        || spec.super_cell_count == 0
        || !spec.super_cell_count.is_power_of_two()
        || spec.super_cell_count > 4_096
        || model_encoded_bytes == 0
        || model_encoded_bytes > SUPERCELL_MODEL_MAXIMUM_ENCODED_BYTES
        || !matches!(request.worker_count, 1 | 2 | 4)
        || request.queue_rows_per_worker == 0
        || request.queue_rows_per_worker > V36_EXTERNAL_ASSIGNMENT_SHARD_ROWS
        || request.sort_rows_per_worker == 0
        || request.sort_rows_per_worker > V36_EXTERNAL_ASSIGNMENT_SHARD_ROWS
        || !(2..=64).contains(&request.merge_fan_in)
        || request.measured_component_terms == 0
        || request.measured_elapsed_ns == 0
        || request.measured_cost_microusd == 0
        || request.measured_external_work_units == 0
        || request.measured_external_elapsed_ns == 0
        || request.measured_external_cost_microusd == 0
        || request.maximum_active_wall_seconds == 0
        || request.maximum_cost_microusd == 0
        || request.maximum_peak_live_bytes == 0
        || request.maximum_scratch_bytes == 0
    {
        return Err(invalid("V36 external assignment admission differs"));
    }
    let logical_shards = spec
        .corpus_rows
        .div_ceil(V36_EXTERNAL_ASSIGNMENT_SHARD_ROWS);
    let mut remaining_shards = logical_shards;
    let mut merge_generations = 0_u32;
    while remaining_shards > 1 {
        remaining_shards = remaining_shards.div_ceil(u64::from(request.merge_fan_in));
        merge_generations = merge_generations
            .checked_add(1)
            .ok_or_else(|| invalid("V36 external assignment merge generations overflow"))?;
    }
    let uncompressed_assignment_bytes = spec
        .corpus_rows
        .checked_mul(V36_EXTERNAL_ASSIGNMENT_ROW_BYTES)
        .and_then(|bytes| {
            logical_shards
                .checked_mul(V36_EXTERNAL_ASSIGNMENT_SHARD_ENVELOPE_BYTES)
                .and_then(|envelopes| bytes.checked_add(envelopes))
        })
        .ok_or_else(|| invalid("V36 external assignment bytes overflow"))?;
    let worker_rows = request
        .queue_rows_per_worker
        .checked_add(request.sort_rows_per_worker)
        .ok_or_else(|| invalid("V36 external assignment worker rows overflow"))?;
    let aggregate_worker_bytes = worker_rows
        .checked_mul(V36_EXTERNAL_ASSIGNMENT_ROW_BYTES)
        .and_then(|bytes| bytes.checked_mul(u64::from(request.worker_count)))
        .ok_or_else(|| invalid("V36 external assignment worker bytes overflow"))?;
    let maximum_shard_rows = spec.corpus_rows.min(V36_EXTERNAL_ASSIGNMENT_SHARD_ROWS);
    let canonicalization_bytes = maximum_shard_rows
        .checked_mul(V36_EXTERNAL_ASSIGNMENT_ROW_BYTES)
        .and_then(|bytes| bytes.checked_mul(4))
        .and_then(|bytes| bytes.checked_add(V36_EXTERNAL_ASSIGNMENT_SHARD_ENVELOPE_BYTES))
        .ok_or_else(|| invalid("V36 external assignment canonicalization bytes overflow"))?;
    let merge_reader_count = if logical_shards > 1 {
        logical_shards.min(u64::from(request.merge_fan_in))
    } else {
        0
    };
    let merge_phase_bytes = maximum_shard_rows
        .checked_mul(V36_EXTERNAL_ASSIGNMENT_ROW_BYTES)
        .and_then(|bytes| bytes.checked_add(V36_EXTERNAL_ASSIGNMENT_SHARD_ENVELOPE_BYTES))
        .and_then(|bytes| bytes.checked_mul(merge_reader_count))
        .and_then(|bytes| {
            maximum_shard_rows
                .checked_mul(u64::try_from(std::mem::size_of::<V36SupercellAssignmentRow>()).ok()?)
                .and_then(|decoded| decoded.checked_mul(merge_reader_count))
                .and_then(|decoded| bytes.checked_add(decoded))
        })
        .and_then(|bytes| bytes.checked_add(canonicalization_bytes))
        .ok_or_else(|| invalid("V36 external assignment merge memory overflows"))?;
    let maximum_final_chunks = logical_shards
        .checked_add(u64::from(spec.super_cell_count).saturating_sub(1))
        .ok_or_else(|| invalid("V36 external assignment final chunk count overflows"))?;
    let maximum_final_bytes = spec
        .corpus_rows
        .checked_mul(V36_EXTERNAL_ASSIGNMENT_ROW_BYTES)
        .and_then(|bytes| {
            maximum_final_chunks
                .checked_mul(V36_EXTERNAL_ASSIGNMENT_SHARD_ENVELOPE_BYTES)
                .and_then(|envelopes| bytes.checked_add(envelopes))
        })
        .ok_or_else(|| invalid("V36 external assignment final bytes overflow"))?;
    let coverage_bitmap_bytes = spec.corpus_rows.div_ceil(8);
    let decoded_publication_chunk_bytes = maximum_shard_rows
        .checked_mul(
            u64::try_from(std::mem::size_of::<V36SupercellAssignmentRow>())
                .map_err(|_| invalid("V36 external assignment row size overflows"))?,
        )
        .ok_or_else(|| invalid("V36 external assignment publication memory overflows"))?;
    let publication_phase_bytes = canonicalization_bytes
        .checked_add(decoded_publication_chunk_bytes)
        .and_then(|bytes| bytes.checked_add(coverage_bitmap_bytes))
        .ok_or_else(|| invalid("V36 external assignment publication memory overflows"))?;
    let required_scratch_bytes = uncompressed_assignment_bytes
        .checked_add(maximum_final_bytes)
        .and_then(|bytes| bytes.checked_add(aggregate_worker_bytes))
        .ok_or_else(|| invalid("V36 external assignment scratch bytes overflow"))?;
    let required_peak_live_bytes = SUPERCELL_MODEL_MAXIMUM_ENCODED_BYTES
        .checked_add(
            aggregate_worker_bytes
                .max(canonicalization_bytes)
                .max(merge_phase_bytes)
                .max(publication_phase_bytes),
        )
        .ok_or_else(|| invalid("V36 external assignment live bytes overflow"))?;
    let component_terms = u128::from(spec.corpus_rows)
        .checked_mul(u128::from(spec.super_cell_count))
        .and_then(|terms| terms.checked_mul(192))
        .ok_or_else(|| invalid("V36 external assignment work overflows"))?;
    let sort_comparison_units = u128::from(spec.corpus_rows)
        .checked_mul(16)
        .ok_or_else(|| invalid("V36 external assignment sort work overflows"))?;
    let publication_row_visits = u128::from(spec.corpus_rows);
    let external_row_visits = u128::from(spec.corpus_rows)
        .checked_mul(2 + 2 * u128::from(merge_generations))
        .ok_or_else(|| invalid("V36 external assignment merge work overflows"))?;
    let external_work_units = sort_comparison_units
        .checked_add(external_row_visits)
        .ok_or_else(|| invalid("V36 external assignment external work overflows"))?;
    if request.measured_component_terms > component_terms
        || request.measured_external_work_units > external_work_units
    {
        return Err(invalid(
            "V36 external assignment calibration exceeds projected work",
        ));
    }
    let projected_compute_ns = component_terms
        .checked_mul(u128::from(request.measured_elapsed_ns))
        .ok_or_else(|| invalid("V36 external assignment time overflows"))?
        .div_ceil(request.measured_component_terms);
    let projected_external_ns = external_work_units
        .checked_mul(u128::from(request.measured_external_elapsed_ns))
        .ok_or_else(|| invalid("V36 external assignment time overflows"))?
        .div_ceil(request.measured_external_work_units);
    let projected_active_ns = projected_compute_ns
        .checked_add(projected_external_ns)
        .ok_or_else(|| invalid("V36 external assignment time overflows"))?;
    let projected_compute_cost = component_terms
        .checked_mul(u128::from(request.measured_cost_microusd))
        .ok_or_else(|| invalid("V36 external assignment cost overflows"))?
        .div_ceil(request.measured_component_terms);
    let projected_external_cost = external_work_units
        .checked_mul(u128::from(request.measured_external_cost_microusd))
        .ok_or_else(|| invalid("V36 external assignment cost overflows"))?
        .div_ceil(request.measured_external_work_units);
    let projected_cost_microusd = projected_compute_cost
        .checked_add(projected_external_cost)
        .ok_or_else(|| invalid("V36 external assignment cost overflows"))?;
    let projected_cost_microusd = u64::try_from(projected_cost_microusd)
        .map_err(|_| invalid("V36 external assignment cost overflows"))?;
    Ok(V36SupercellAssignmentProjection {
        logical_shards,
        merge_generations,
        merge_fan_in: request.merge_fan_in,
        uncompressed_assignment_bytes,
        required_scratch_bytes,
        required_peak_live_bytes,
        coverage_bitmap_bytes,
        component_terms,
        external_work_units,
        publication_row_visits,
        projected_active_ns,
        projected_cost_microusd,
    })
}

/// Admit pre-assignment work bound to an authenticated model and exact request.
pub fn admit_v36_supercell_assignment_preflight(
    model: &V36AuthenticatedSupercellModel,
    request: &V36SupercellAssignmentAdmissionRequest,
) -> Result<V36AdmittedSupercellAssignmentPreflight> {
    let projection = project_v36_supercell_assignment_admission(
        model.training_spec(),
        model.identity().encoded_bytes,
        request,
    )?;
    let maximum_active_ns = u128::from(request.maximum_active_wall_seconds)
        .checked_mul(1_000_000_000)
        .ok_or_else(|| invalid("V36 external assignment wall cap overflows"))?;
    if projection.projected_active_ns > maximum_active_ns
        || projection.projected_cost_microusd > request.maximum_cost_microusd
        || projection.required_peak_live_bytes > request.maximum_peak_live_bytes
        || projection.required_scratch_bytes > request.maximum_scratch_bytes
    {
        return Err(invalid("V36 external assignment admission exceeds limits"));
    }
    Ok(V36AdmittedSupercellAssignmentPreflight {
        model_identity: model.identity().clone(),
        training_spec: model.training_spec().clone(),
        request: request.clone(),
        projection,
    })
}

const V36_LOCAL_POSTING_SIDECAR_ROW_BYTES: u64 = 8 + 8 + 4;
const V36_LOCAL_POSTING_WORKER_ROW_BYTES: u64 =
    V36_EXTERNAL_ASSIGNMENT_ROW_BYTES + V36_LOCAL_POSTING_SIDECAR_ROW_BYTES;

#[derive(Debug, Clone, PartialEq, Eq)]
/// Calibration and hard limits for exact post-count local training admission.
pub struct V36SupercellPostCountAdmissionRequest {
    /// Local-training component terms in the bounded calibration.
    pub measured_component_terms: u128,
    /// Active nanoseconds in the bounded local-training calibration.
    pub measured_elapsed_ns: u64,
    /// Cost of the bounded local-training calibration in micro-US-dollars.
    pub measured_cost_microusd: u64,
    /// Hard total active wall limit including assignment.
    pub maximum_active_wall_seconds: u64,
    /// Hard total construction cost limit in micro-US-dollars.
    pub maximum_cost_microusd: u64,
    /// Hard aggregate process live-byte limit.
    pub maximum_peak_live_bytes: u64,
    /// Hard attempt-owned scratch-byte limit.
    pub maximum_scratch_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Exact post-count projection including Hamilton allocation and local work.
pub struct V36SupercellPostCountProjection {
    /// Exact global posting count derived from target primary rows.
    pub posting_count: u64,
    /// Hamilton allocation in super-cell ordinal order.
    pub postings_per_supercell: Vec<u32>,
    /// Exact farthest-first distance evaluations across all super cells.
    pub initialization_distance_evaluations: u128,
    /// Exact distance evaluations across ten local Lloyd passes.
    pub lloyd_distance_evaluations: u128,
    /// Conservative worst-case empty-repair distance evaluations.
    pub repair_distance_evaluations: u128,
    /// Exact source-ordered binary64 centroid reduction component terms.
    pub source_reduction_terms: u128,
    /// Farthest-first, ten Lloyd, repair, and replay component terms.
    pub local_component_terms: u128,
    /// Total scratch including assignment overlap and two sidecar generations.
    pub required_scratch_bytes: u64,
    /// Maximum of assignment and bounded local-worker live memory.
    pub required_peak_live_bytes: u64,
    /// Total ceiling-scaled active time including assignment.
    pub projected_active_ns: u128,
    /// Total ceiling-scaled cost including assignment.
    pub projected_cost_microusd: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Final construction admission consuming one authenticated assignment preflight.
pub struct V36AdmittedSupercellPostCountPlan {
    assignment_preflight: V36AdmittedSupercellAssignmentPreflight,
    run_rows: Vec<u64>,
    target_primary_rows: u64,
    request: V36SupercellPostCountAdmissionRequest,
    projection: V36SupercellPostCountProjection,
}

impl V36AdmittedSupercellPostCountPlan {
    /// Authenticated assignment preflight consumed by this final admission.
    pub fn assignment_preflight(&self) -> &V36AdmittedSupercellAssignmentPreflight {
        &self.assignment_preflight
    }

    /// Committed super-cell populations in exact ordinal order.
    pub fn run_rows(&self) -> &[u64] {
        &self.run_rows
    }

    /// Exact target primary rows from which the posting count was derived.
    pub const fn target_primary_rows(&self) -> u64 {
        self.target_primary_rows
    }

    /// Exact local calibration and total construction limits.
    pub fn request(&self) -> &V36SupercellPostCountAdmissionRequest {
        &self.request
    }

    /// Exact checked Hamilton, work, scratch, live-memory, time, and cost projection.
    pub fn projection(&self) -> &V36SupercellPostCountProjection {
        &self.projection
    }
}

/// Project exact post-count local training without allocating the population.
pub fn project_v36_supercell_post_count_admission(
    spec: &V36SupercellTrainingSpec,
    assignment: &V36SupercellAssignmentProjection,
    run_rows: &[u64],
    target_primary_rows: u64,
    worker_count: u8,
    request: &V36SupercellPostCountAdmissionRequest,
) -> Result<V36SupercellPostCountProjection> {
    if run_rows.len()
        != usize::try_from(spec.super_cell_count)
            .map_err(|_| invalid("V36 post-count super-cell count overflows"))?
        || target_primary_rows == 0
        || !matches!(worker_count, 1 | 2 | 4)
        || request.measured_component_terms == 0
        || request.measured_elapsed_ns == 0
        || request.measured_cost_microusd == 0
        || request.maximum_active_wall_seconds == 0
        || request.maximum_cost_microusd == 0
        || request.maximum_peak_live_bytes == 0
        || request.maximum_scratch_bytes == 0
    {
        return Err(invalid("V36 post-count admission differs"));
    }
    let population = run_rows.iter().try_fold(0_u64, |sum, rows| {
        sum.checked_add(*rows)
            .ok_or_else(|| invalid("V36 post-count population overflows"))
    })?;
    if population != spec.corpus_rows {
        return Err(invalid("V36 post-count population differs"));
    }
    let posting_count = population.div_ceil(target_primary_rows);
    let posting_count_u32 = u32::try_from(posting_count)
        .map_err(|_| invalid("V36 post-count posting count overflows"))?;
    let postings_per_supercell = allocate_v36_hamilton_postings(run_rows, posting_count_u32)?;

    let mut initialization_distance_evaluations = 0_u128;
    let mut lloyd_distance_evaluations = 0_u128;
    let mut repair_distance_evaluations = 0_u128;
    let mut source_reduction_terms = 0_u128;
    for (&rows, &postings) in run_rows.iter().zip(&postings_per_supercell) {
        if u64::from(postings) > rows {
            return Err(invalid("V36 post-count postings exceed run population"));
        }
        let rows = u128::from(rows);
        let postings = u128::from(postings);
        let initialization_distances = if postings == 0 {
            0
        } else {
            (postings - 1)
                .checked_mul(rows)
                .and_then(|value| value.checked_sub(postings * (postings - 1) / 2))
                .ok_or_else(|| invalid("V36 post-count initialization work overflows"))?
        };
        let lloyd_distances = 10_u128
            .checked_mul(rows)
            .and_then(|value| value.checked_mul(postings))
            .ok_or_else(|| invalid("V36 post-count Lloyd work overflows"))?;
        let repair_distances = 10_u128
            .checked_mul(rows)
            .and_then(|value| value.checked_mul(postings))
            .ok_or_else(|| invalid("V36 post-count repair work overflows"))?;
        let reduction_terms = 10_u128
            .checked_mul(rows)
            .and_then(|value| value.checked_mul(192))
            .ok_or_else(|| invalid("V36 post-count reduction work overflows"))?;
        initialization_distance_evaluations = initialization_distance_evaluations
            .checked_add(initialization_distances)
            .ok_or_else(|| invalid("V36 post-count initialization work overflows"))?;
        lloyd_distance_evaluations = lloyd_distance_evaluations
            .checked_add(lloyd_distances)
            .ok_or_else(|| invalid("V36 post-count Lloyd work overflows"))?;
        repair_distance_evaluations = repair_distance_evaluations
            .checked_add(repair_distances)
            .ok_or_else(|| invalid("V36 post-count repair work overflows"))?;
        source_reduction_terms = source_reduction_terms
            .checked_add(reduction_terms)
            .ok_or_else(|| invalid("V36 post-count reduction work overflows"))?;
    }
    let local_component_terms = initialization_distance_evaluations
        .checked_add(lloyd_distance_evaluations)
        .and_then(|value| value.checked_add(repair_distance_evaluations))
        .and_then(|value| value.checked_mul(192))
        .and_then(|value| value.checked_add(source_reduction_terms))
        .ok_or_else(|| invalid("V36 post-count work overflows"))?;
    if request.measured_component_terms > local_component_terms {
        return Err(invalid("V36 post-count calibration exceeds projected work"));
    }
    let sidecar_overlap_bytes = population
        .checked_mul(V36_LOCAL_POSTING_SIDECAR_ROW_BYTES)
        .and_then(|value| value.checked_mul(2))
        .ok_or_else(|| invalid("V36 post-count sidecar bytes overflow"))?;
    let required_scratch_bytes = assignment
        .required_scratch_bytes
        .checked_add(sidecar_overlap_bytes)
        .ok_or_else(|| invalid("V36 post-count scratch bytes overflow"))?;
    let local_worker_bytes = V36_EXTERNAL_ASSIGNMENT_SHARD_ROWS
        .checked_mul(V36_LOCAL_POSTING_WORKER_ROW_BYTES)
        .and_then(|value| value.checked_mul(u64::from(worker_count)))
        .and_then(|value| value.checked_add(posting_count.checked_mul(192 * 4 + 192 * 8 + 8)?))
        .ok_or_else(|| invalid("V36 post-count live bytes overflow"))?;
    let required_peak_live_bytes = assignment.required_peak_live_bytes.max(local_worker_bytes);
    let local_active_ns = local_component_terms
        .checked_mul(u128::from(request.measured_elapsed_ns))
        .ok_or_else(|| invalid("V36 post-count time overflows"))?
        .div_ceil(request.measured_component_terms);
    let projected_active_ns = assignment
        .projected_active_ns
        .checked_add(local_active_ns)
        .ok_or_else(|| invalid("V36 post-count time overflows"))?;
    let local_cost = local_component_terms
        .checked_mul(u128::from(request.measured_cost_microusd))
        .ok_or_else(|| invalid("V36 post-count cost overflows"))?
        .div_ceil(request.measured_component_terms);
    let local_cost =
        u64::try_from(local_cost).map_err(|_| invalid("V36 post-count cost overflows"))?;
    let projected_cost_microusd = assignment
        .projected_cost_microusd
        .checked_add(local_cost)
        .ok_or_else(|| invalid("V36 post-count cost overflows"))?;
    Ok(V36SupercellPostCountProjection {
        posting_count,
        postings_per_supercell,
        initialization_distance_evaluations,
        lloyd_distance_evaluations,
        repair_distance_evaluations,
        source_reduction_terms,
        local_component_terms,
        required_scratch_bytes,
        required_peak_live_bytes,
        projected_active_ns,
        projected_cost_microusd,
    })
}

/// Admit final local training against one authenticated assignment preflight.
pub fn admit_v36_supercell_post_count(
    assignment: &V36AdmittedSupercellAssignmentPreflight,
    run_rows: &[u64],
    target_primary_rows: u64,
    request: &V36SupercellPostCountAdmissionRequest,
) -> Result<V36AdmittedSupercellPostCountPlan> {
    let projection = project_v36_supercell_post_count_admission(
        assignment.training_spec(),
        assignment.projection(),
        run_rows,
        target_primary_rows,
        assignment.request().worker_count,
        request,
    )?;
    let maximum_active_ns = u128::from(request.maximum_active_wall_seconds)
        .checked_mul(1_000_000_000)
        .ok_or_else(|| invalid("V36 post-count wall cap overflows"))?;
    if projection.projected_active_ns > maximum_active_ns
        || projection.projected_cost_microusd > request.maximum_cost_microusd
        || projection.required_peak_live_bytes > request.maximum_peak_live_bytes
        || projection.required_scratch_bytes > request.maximum_scratch_bytes
    {
        return Err(invalid("V36 post-count admission exceeds limits"));
    }
    Ok(V36AdmittedSupercellPostCountPlan {
        assignment_preflight: assignment.clone(),
        run_rows: run_rows.to_vec(),
        target_primary_rows,
        request: request.clone(),
        projection,
    })
}

#[derive(Debug, Clone, PartialEq)]
/// Deterministic V36 super-cell centroids and reservoir evidence.
pub struct V36SupercellModel {
    centroids: Vec<[f32; 192]>,
    empty_repairs: u64,
    initialization_source_ordinals: Vec<u64>,
    projected_corpus_sha256: String,
    reservoir_source_ordinals: Vec<u64>,
}

#[derive(Debug, Clone, PartialEq)]
/// Super-cell model inseparably bound to its authenticated artifact and training authority.
pub struct V36AuthenticatedSupercellModel {
    identity: V36ArtifactIdentity,
    model: V36SupercellModel,
    training_spec: V36SupercellTrainingSpec,
}

impl V36AuthenticatedSupercellModel {
    /// Complete immutable Arrow artifact identity authenticated at load time.
    pub fn identity(&self) -> &V36ArtifactIdentity {
        &self.identity
    }

    /// Decoded model retained behind the authenticated handle.
    pub fn model(&self) -> &V36SupercellModel {
        &self.model
    }

    /// Exact training authority authenticated from the artifact manifest.
    pub fn training_spec(&self) -> &V36SupercellTrainingSpec {
        &self.training_spec
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct V36SupercellModelManifest {
    algorithm: String,
    empty_repairs: u64,
    format: String,
    role: String,
    training: V36SupercellTrainingSpec,
    uri: String,
}

impl V36SupercellModel {
    /// Trained super-cell centroids in canonical ordinal order.
    pub fn centroids(&self) -> &[[f32; 192]] {
        &self.centroids
    }

    /// Total deterministic empty-centroid repairs across all Lloyd iterations.
    pub const fn empty_repairs(&self) -> u64 {
        self.empty_repairs
    }

    /// Reservoir source ordinals selected by deterministic farthest-first initialization.
    pub fn initialization_source_ordinals(&self) -> &[u64] {
        &self.initialization_source_ordinals
    }

    /// Exact fixed Lloyd iteration count.
    pub const fn iterations(&self) -> u8 {
        25
    }

    /// Authenticated projected-corpus replay consumed by both bounded passes.
    pub fn projected_corpus_sha256(&self) -> &str {
        &self.projected_corpus_sha256
    }

    /// Selected query-independent reservoir rows in source-ordinal order.
    pub fn reservoir_source_ordinals(&self) -> &[u64] {
        &self.reservoir_source_ordinals
    }
}

fn validate_v36_supercell_model(
    model: &V36SupercellModel,
    spec: &V36SupercellTrainingSpec,
) -> Result<()> {
    let centroid_count = usize::try_from(spec.super_cell_count)
        .map_err(|_| invalid("V36 super-cell count overflows"))?;
    let reservoir_rows = usize::try_from(spec.reservoir_rows)
        .map_err(|_| invalid("V36 super-cell reservoir row count overflows"))?;
    if spec.corpus_rows == 0
        || spec.dimensions != 192
        || spec.maximum_block_rows == 0
        || spec.maximum_block_rows > 65_536
        || !valid_sha256(&spec.projected_corpus_sha256)
        || spec.reservoir_rows == 0
        || spec.reservoir_rows > spec.corpus_rows
        || spec.reservoir_rows > 1_048_576
        || spec.super_cell_count == 0
        || !spec.super_cell_count.is_power_of_two()
        || spec.super_cell_count > 4_096
        || u64::from(spec.super_cell_count) > spec.reservoir_rows
        || model.centroids.len() != centroid_count
        || model.initialization_source_ordinals.len() != centroid_count
        || model.reservoir_source_ordinals.len() != reservoir_rows
        || model.projected_corpus_sha256 != spec.projected_corpus_sha256
        || model.empty_repairs > 25_u64.saturating_mul(u64::from(spec.super_cell_count))
        || model
            .centroids
            .iter()
            .flatten()
            .any(|value| !value.is_finite() || (*value == 0.0 && value.is_sign_negative()))
        || model
            .reservoir_source_ordinals
            .windows(2)
            .any(|pair| pair[0] >= pair[1])
        || model
            .reservoir_source_ordinals
            .last()
            .is_some_and(|ordinal| *ordinal >= spec.corpus_rows)
        || model
            .initialization_source_ordinals
            .iter()
            .enumerate()
            .any(|(index, ordinal)| model.initialization_source_ordinals[..index].contains(ordinal))
        || model.initialization_source_ordinals.first() != model.reservoir_source_ordinals.first()
        || model.initialization_source_ordinals.iter().any(|ordinal| {
            model
                .reservoir_source_ordinals
                .binary_search(ordinal)
                .is_err()
        })
    {
        return Err(invalid("V36 super-cell model differs"));
    }
    Ok(())
}

fn canonical_v36_supercell_manifest(manifest: &V36SupercellModelManifest) -> Result<String> {
    let value = serde_json::to_value(manifest)
        .map_err(|_| invalid("V36 super-cell model manifest differs"))?;
    serde_json::to_string(&v36_canonical_json_value(value))
        .map_err(|_| invalid("V36 super-cell model manifest differs"))
}

fn v36_supercell_list_offsets(length: usize) -> Result<OffsetBuffer<i32>> {
    let length =
        i32::try_from(length).map_err(|_| invalid("V36 super-cell model list length overflows"))?;
    Ok(OffsetBuffer::new(vec![0_i32, length].into()))
}

fn v36_supercell_model_schema(manifest: String) -> Schema {
    let vector_child = Arc::new(Field::new("element", DataType::Float32, false));
    let centroid_child = Arc::new(Field::new(
        "element",
        DataType::FixedSizeList(vector_child, 192),
        false,
    ));
    let ordinal_child = Arc::new(Field::new("element", DataType::UInt64, false));
    Schema::new_with_metadata(
        vec![
            Field::new("centroids", DataType::List(centroid_child), false),
            Field::new(
                "initialization_source_ordinals",
                DataType::List(ordinal_child.clone()),
                false,
            ),
            Field::new(
                "reservoir_source_ordinals",
                DataType::List(ordinal_child),
                false,
            ),
        ],
        HashMap::from([(SUPERCELL_MODEL_MANIFEST_KEY.to_owned(), manifest)]),
    )
}

fn valid_v36_supercell_model_uri(uri: &str) -> bool {
    if uri.len() > 4_096 {
        return false;
    }
    let Ok(parsed) = url::Url::parse(uri) else {
        return false;
    };
    let path = parsed.path();
    parsed.scheme() == "s3"
        && parsed.host_str().is_some_and(|host| !host.is_empty())
        && path.len() > 1
        && !path.ends_with('/')
        && !path.split('/').any(|part| part == "..")
        && parsed.query().is_none()
        && parsed.fragment().is_none()
}

fn v36_supercell_untrusted_manifest(bytes: &[u8]) -> Result<String> {
    if bytes.len() < 18
        || bytes.len() > SUPERCELL_MODEL_MAXIMUM_ENCODED_BYTES as usize
        || !bytes.starts_with(b"ARROW1")
        || !bytes.ends_with(b"ARROW1")
    {
        return Err(invalid("V36 super-cell model IPC envelope differs"));
    }
    let trailer = bytes.len() - 10;
    let footer_len = u32::from_le_bytes(
        bytes[trailer..trailer + 4]
            .try_into()
            .map_err(|_| invalid("V36 super-cell model footer length differs"))?,
    ) as usize;
    let footer_start = trailer
        .checked_sub(footer_len)
        .filter(|offset| *offset >= 8)
        .ok_or_else(|| invalid("V36 super-cell model footer extent differs"))?;
    let footer = arrow_ipc::root_as_footer(&bytes[footer_start..trailer])
        .map_err(|_| invalid("V36 super-cell model footer differs"))?;
    if footer.version() != MetadataVersion::V5
        || footer
            .custom_metadata()
            .is_some_and(|values| !values.is_empty())
        || footer
            .dictionaries()
            .is_some_and(|values| !values.is_empty())
        || footer.recordBatches().map_or(0, |values| values.len()) != 1
    {
        return Err(invalid("V36 super-cell model footer authority differs"));
    }
    let metadata = footer
        .schema()
        .and_then(|schema| schema.custom_metadata())
        .ok_or_else(|| invalid("V36 super-cell model manifest is missing"))?;
    if metadata.len() != 1 || metadata.get(0).key() != Some(SUPERCELL_MODEL_MANIFEST_KEY) {
        return Err(invalid("V36 super-cell model manifest differs"));
    }
    metadata
        .get(0)
        .value()
        .map(str::to_owned)
        .ok_or_else(|| invalid("V36 super-cell model manifest differs"))
}

fn validate_v36_supercell_ipc_field(field: arrow_ipc::Field<'_>, expected: &Field) -> Result<()> {
    if field.name() != Some(expected.name().as_str())
        || field.nullable()
        || field.dictionary().is_some()
        || field
            .custom_metadata()
            .is_some_and(|values| !values.is_empty())
    {
        return Err(invalid("V36 super-cell model IPC field differs"));
    }
    let children = match expected.data_type() {
        DataType::Float32 => {
            if field
                .type_as_floating_point()
                .is_none_or(|value| value.precision() != arrow_ipc::Precision::SINGLE)
            {
                return Err(invalid("V36 super-cell model IPC float differs"));
            }
            Vec::new()
        }
        DataType::UInt64 => {
            if field
                .type_as_int()
                .is_none_or(|value| value.bitWidth() != 64 || value.is_signed())
            {
                return Err(invalid("V36 super-cell model IPC integer differs"));
            }
            Vec::new()
        }
        DataType::FixedSizeList(child, width) => {
            if field
                .type_as_fixed_size_list()
                .is_none_or(|value| value.listSize() != *width)
            {
                return Err(invalid("V36 super-cell model IPC fixed list differs"));
            }
            vec![child.as_ref()]
        }
        DataType::List(child) => {
            if field.type_as_list().is_none() {
                return Err(invalid("V36 super-cell model IPC list differs"));
            }
            vec![child.as_ref()]
        }
        _ => return Err(invalid("V36 super-cell model IPC type differs")),
    };
    let actual_children = field.children();
    if actual_children.map_or(0, |values| values.len()) != children.len() {
        return Err(invalid("V36 super-cell model IPC children differ"));
    }
    for (index, child) in children.iter().enumerate() {
        validate_v36_supercell_ipc_field(
            actual_children
                .ok_or_else(|| invalid("V36 super-cell model IPC child is missing"))?
                .get(index),
            child,
        )?;
    }
    Ok(())
}

fn validate_v36_supercell_ipc_schema(
    schema: arrow_ipc::Schema<'_>,
    expected: &Schema,
) -> Result<()> {
    if schema.endianness() != arrow_ipc::Endianness::Little
        || schema.features().is_some_and(|values| !values.is_empty())
    {
        return Err(invalid("V36 super-cell model IPC schema differs"));
    }
    let metadata = schema
        .custom_metadata()
        .ok_or_else(|| invalid("V36 super-cell model manifest is missing"))?;
    let expected_manifest = expected
        .metadata()
        .get(SUPERCELL_MODEL_MANIFEST_KEY)
        .ok_or_else(|| invalid("V36 super-cell model manifest differs"))?;
    if metadata.len() != 1
        || metadata.get(0).key() != Some(SUPERCELL_MODEL_MANIFEST_KEY)
        || metadata.get(0).value() != Some(expected_manifest.as_str())
    {
        return Err(invalid("V36 super-cell model manifest differs"));
    }
    let fields = schema
        .fields()
        .ok_or_else(|| invalid("V36 super-cell model IPC fields are missing"))?;
    if fields.len() != expected.fields().len() {
        return Err(invalid("V36 super-cell model IPC field count differs"));
    }
    for (index, expected_field) in expected.fields().iter().enumerate() {
        validate_v36_supercell_ipc_field(fields.get(index), expected_field)?;
    }
    Ok(())
}

fn validate_v36_supercell_ipc_envelope(
    bytes: &[u8],
    expected: &Schema,
    centroid_count: usize,
    seed_count: usize,
    reservoir_rows: usize,
) -> Result<()> {
    if bytes.len() < 18
        || bytes.len() > SUPERCELL_MODEL_MAXIMUM_ENCODED_BYTES as usize
        || !bytes.starts_with(b"ARROW1")
        || !bytes.ends_with(b"ARROW1")
    {
        return Err(invalid("V36 super-cell model IPC envelope differs"));
    }
    let trailer = bytes.len() - 10;
    let footer_len = u32::from_le_bytes(
        bytes[trailer..trailer + 4]
            .try_into()
            .map_err(|_| invalid("V36 super-cell model footer length differs"))?,
    ) as usize;
    let footer_start = trailer
        .checked_sub(footer_len)
        .filter(|offset| *offset >= 8)
        .ok_or_else(|| invalid("V36 super-cell model footer extent differs"))?;
    let footer = arrow_ipc::root_as_footer(&bytes[footer_start..trailer])
        .map_err(|_| invalid("V36 super-cell model footer differs"))?;
    validate_v36_supercell_ipc_schema(
        footer
            .schema()
            .ok_or_else(|| invalid("V36 super-cell model footer schema is missing"))?,
        expected,
    )?;
    if footer
        .dictionaries()
        .is_some_and(|values| !values.is_empty())
    {
        return Err(invalid("V36 super-cell model dictionaries are forbidden"));
    }
    let blocks = footer
        .recordBatches()
        .ok_or_else(|| invalid("V36 super-cell model batch is missing"))?;
    if blocks.len() != 1 {
        return Err(invalid("V36 super-cell model batch count differs"));
    }
    let block = blocks.get(0);
    let block_offset = usize::try_from(block.offset())
        .map_err(|_| invalid("V36 super-cell model batch offset differs"))?;
    let metadata_len = usize::try_from(block.metaDataLength())
        .map_err(|_| invalid("V36 super-cell model batch metadata differs"))?;
    let body_len = usize::try_from(block.bodyLength())
        .map_err(|_| invalid("V36 super-cell model batch body differs"))?;
    let body_start = block_offset
        .checked_add(metadata_len)
        .ok_or_else(|| invalid("V36 super-cell model batch extent overflows"))?;
    let body_end = body_start
        .checked_add(body_len)
        .ok_or_else(|| invalid("V36 super-cell model batch extent overflows"))?;
    if block_offset < 8 || metadata_len < 8 || body_end > footer_start {
        return Err(invalid("V36 super-cell model batch extent differs"));
    }
    let parse_message = |start: usize, end: usize| {
        let metadata = bytes
            .get(start..end)
            .ok_or_else(|| invalid("V36 super-cell model message extent differs"))?;
        if metadata.len() < 4 {
            return Err(invalid("V36 super-cell model message is truncated"));
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
                .ok_or_else(|| invalid("V36 super-cell model message length differs"))?,
        ) as usize;
        let message_end = prefix
            .checked_add(length)
            .filter(|value| *value <= metadata.len())
            .ok_or_else(|| invalid("V36 super-cell model message extent differs"))?;
        arrow_ipc::root_as_message(&metadata[prefix..message_end])
            .map_err(|_| invalid("V36 super-cell model message differs"))
    };
    let leading = parse_message(8, block_offset)?;
    if leading.version() != MetadataVersion::V5 || leading.bodyLength() != 0 {
        return Err(invalid("V36 super-cell model leading schema differs"));
    }
    validate_v36_supercell_ipc_schema(
        leading
            .header_as_schema()
            .ok_or_else(|| invalid("V36 super-cell model leading schema is missing"))?,
        expected,
    )?;
    let record_message = parse_message(block_offset, body_start)?;
    let record = record_message
        .header_as_record_batch()
        .ok_or_else(|| invalid("V36 super-cell model record differs"))?;
    if record_message.version() != MetadataVersion::V5
        || record.compression().is_some()
        || record
            .variadicBufferCounts()
            .is_some_and(|values| !values.is_empty())
        || record.length() != 1
        || usize::try_from(record_message.bodyLength()).ok() != Some(body_len)
    {
        return Err(invalid("V36 super-cell model record authority differs"));
    }
    let nodes = record
        .nodes()
        .ok_or_else(|| invalid("V36 super-cell model nodes are missing"))?;
    let centroid_values = centroid_count
        .checked_mul(192)
        .ok_or_else(|| invalid("V36 super-cell model node length overflows"))?;
    let expected_nodes = [
        1,
        centroid_count,
        centroid_values,
        1,
        seed_count,
        1,
        reservoir_rows,
    ];
    if nodes.len() != expected_nodes.len()
        || nodes.iter().zip(expected_nodes).any(|(node, length)| {
            usize::try_from(node.length()).ok() != Some(length) || node.null_count() != 0
        })
    {
        return Err(invalid("V36 super-cell model node shape differs"));
    }
    let buffers = record
        .buffers()
        .ok_or_else(|| invalid("V36 super-cell model buffers are missing"))?;
    if buffers.len() != 13 {
        return Err(invalid("V36 super-cell model buffer count differs"));
    }
    let mut slices = Vec::new();
    slices
        .try_reserve_exact(13)
        .map_err(|_| invalid("V36 super-cell model buffer allocation exceeds capacity"))?;
    let body = &bytes[body_start..body_end];
    let mut previous_end = 0_usize;
    for buffer in buffers {
        let start = usize::try_from(buffer.offset())
            .map_err(|_| invalid("V36 super-cell model buffer offset differs"))?;
        let length = usize::try_from(buffer.length())
            .map_err(|_| invalid("V36 super-cell model buffer length differs"))?;
        let end = start
            .checked_add(length)
            .ok_or_else(|| invalid("V36 super-cell model buffer extent overflows"))?;
        if start < previous_end {
            return Err(invalid("V36 super-cell model buffers overlap"));
        }
        slices.push(
            body.get(start..end)
                .ok_or_else(|| invalid("V36 super-cell model buffer extent differs"))?,
        );
        previous_end = end;
    }
    for (index, count) in [
        (0, 1),
        (2, centroid_count),
        (3, centroid_values),
        (5, 1),
        (7, seed_count),
        (9, 1),
        (11, reservoir_rows),
    ] {
        if !slices[index].is_empty() && slices[index].len() != count.div_ceil(8) {
            return Err(invalid("V36 super-cell model validity length differs"));
        }
    }
    let centroid_bytes = centroid_values
        .checked_mul(4)
        .ok_or_else(|| invalid("V36 super-cell model centroid bytes overflow"))?;
    let seed_bytes = seed_count
        .checked_mul(8)
        .ok_or_else(|| invalid("V36 super-cell model seed bytes overflow"))?;
    let reservoir_bytes = reservoir_rows
        .checked_mul(8)
        .ok_or_else(|| invalid("V36 super-cell model reservoir bytes overflow"))?;
    for (index, length) in [
        (1, 8),
        (4, centroid_bytes),
        (6, 8),
        (8, seed_bytes),
        (10, 8),
        (12, reservoir_bytes),
    ] {
        if slices[index].len() != length {
            return Err(invalid("V36 super-cell model value length differs"));
        }
    }
    for (index, terminal) in [(1, centroid_count), (6, seed_count), (10, reservoir_rows)] {
        let offsets = slices[index]
            .as_chunks::<4>()
            .0
            .iter()
            .map(|value| i32::from_le_bytes(*value))
            .collect::<Vec<_>>();
        if offsets != [0, i32::try_from(terminal).unwrap_or(-1)] {
            return Err(invalid("V36 super-cell model list offsets differ"));
        }
    }
    Ok(())
}

/// Encode one trained V36 super-cell model as strict cross-language Arrow IPC.
pub fn encode_v36_supercell_model_arrow(
    model: &V36SupercellModel,
    spec: &V36SupercellTrainingSpec,
    role: &str,
    uri: &str,
) -> Result<(Vec<u8>, V36ArtifactIdentity)> {
    validate_v36_supercell_model(model, spec)?;
    if role != SUPERCELL_MODEL_ROLE || !valid_v36_supercell_model_uri(uri) {
        return Err(invalid("V36 super-cell model artifact identity differs"));
    }
    let vector_child = Arc::new(Field::new("element", DataType::Float32, false));
    let centroid_values = Arc::new(FixedSizeListArray::try_new(
        vector_child.clone(),
        192,
        Arc::new(Float32Array::from_iter_values(
            model.centroids.iter().flatten().copied(),
        )),
        None,
    )?);
    let centroid_child = Arc::new(Field::new(
        "element",
        DataType::FixedSizeList(vector_child, 192),
        false,
    ));
    let centroids = Arc::new(ListArray::new(
        centroid_child.clone(),
        v36_supercell_list_offsets(model.centroids.len())?,
        centroid_values,
        None,
    ));
    let ordinal_child = Arc::new(Field::new("element", DataType::UInt64, false));
    let initialization = Arc::new(ListArray::new(
        ordinal_child.clone(),
        v36_supercell_list_offsets(model.initialization_source_ordinals.len())?,
        Arc::new(UInt64Array::from(
            model.initialization_source_ordinals.clone(),
        )),
        None,
    ));
    let reservoir = Arc::new(ListArray::new(
        ordinal_child.clone(),
        v36_supercell_list_offsets(model.reservoir_source_ordinals.len())?,
        Arc::new(UInt64Array::from(model.reservoir_source_ordinals.clone())),
        None,
    ));
    let manifest = V36SupercellModelManifest {
        algorithm: SUPERCELL_MODEL_ALGORITHM.to_owned(),
        empty_repairs: model.empty_repairs,
        format: SUPERCELL_MODEL_FORMAT.to_owned(),
        role: role.to_owned(),
        training: spec.clone(),
        uri: uri.to_owned(),
    };
    let metadata = HashMap::from([(
        SUPERCELL_MODEL_MANIFEST_KEY.to_owned(),
        canonical_v36_supercell_manifest(&manifest)?,
    )]);
    let schema = Arc::new(v36_supercell_model_schema(
        metadata
            .get(SUPERCELL_MODEL_MANIFEST_KEY)
            .expect("manifest was inserted")
            .clone(),
    ));
    let batch = RecordBatch::try_new(schema.clone(), vec![centroids, initialization, reservoir])?;
    let options = IpcWriteOptions::try_new(8, false, MetadataVersion::V5)?;
    let mut bytes = Vec::new();
    let mut writer = FileWriter::try_new_with_options(&mut bytes, schema.as_ref(), options)?;
    writer.write(&batch)?;
    writer.finish()?;
    drop(writer);
    let encoded_bytes = u64::try_from(bytes.len())
        .map_err(|_| invalid("V36 super-cell model artifact length overflows"))?;
    if encoded_bytes > SUPERCELL_MODEL_MAXIMUM_ENCODED_BYTES {
        return Err(invalid("V36 super-cell model exceeds encoded admission"));
    }
    let identity = V36ArtifactIdentity {
        blake3: blake3::hash(&bytes).to_hex().to_string(),
        encoded_bytes,
        role: role.to_owned(),
        sha256: format!("{:x}", Sha256::digest(&bytes)),
        uri: uri.to_owned(),
    };
    Ok((bytes, identity))
}

/// Authenticate and decode one exact V36 super-cell Arrow model.
pub fn decode_v36_supercell_model_arrow(
    bytes: &[u8],
    identity: &V36ArtifactIdentity,
    expected_training: &V36SupercellTrainingSpec,
) -> Result<V36AuthenticatedSupercellModel> {
    let encoded_bytes = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
    if identity.role != SUPERCELL_MODEL_ROLE
        || !valid_v36_supercell_model_uri(&identity.uri)
        || identity.encoded_bytes != encoded_bytes
        || encoded_bytes > SUPERCELL_MODEL_MAXIMUM_ENCODED_BYTES
        || !valid_sha256(&identity.sha256)
        || !valid_sha256(&identity.blake3)
        || identity.sha256 != format!("{:x}", Sha256::digest(bytes))
        || identity.blake3 != blake3::hash(bytes).to_hex().as_str()
    {
        return Err(invalid("V36 super-cell model artifact identity differs"));
    }
    let manifest_text = v36_supercell_untrusted_manifest(bytes)?;
    let manifest: V36SupercellModelManifest = serde_json::from_str(&manifest_text)
        .map_err(|_| invalid("V36 super-cell model manifest differs"))?;
    let expected_schema = v36_supercell_model_schema(manifest_text.clone());
    if canonical_v36_supercell_manifest(&manifest)? != manifest_text
        || manifest.algorithm != SUPERCELL_MODEL_ALGORITHM
        || manifest.format != SUPERCELL_MODEL_FORMAT
        || manifest.role != identity.role
        || manifest.uri != identity.uri
        || manifest.training != *expected_training
    {
        return Err(invalid("V36 super-cell model manifest differs"));
    }
    validate_v36_supercell_ipc_envelope(
        bytes,
        &expected_schema,
        usize::try_from(expected_training.super_cell_count)
            .map_err(|_| invalid("V36 super-cell count overflows"))?,
        usize::try_from(expected_training.super_cell_count)
            .map_err(|_| invalid("V36 super-cell count overflows"))?,
        usize::try_from(expected_training.reservoir_rows)
            .map_err(|_| invalid("V36 super-cell reservoir row count overflows"))?,
    )?;
    let mut reader = FileReader::try_new(Cursor::new(bytes), None)?;
    let schema = reader.schema();
    if reader.num_batches() != 1 || schema.as_ref() != &expected_schema {
        return Err(invalid("V36 super-cell model Arrow schema differs"));
    }
    let batch = reader
        .next()
        .ok_or_else(|| invalid("V36 super-cell model batch is missing"))??;
    if batch.num_rows() != 1 || batch.num_columns() != 3 || reader.next().is_some() {
        return Err(invalid("V36 super-cell model batch differs"));
    }
    let list = |column: usize| -> Result<&ListArray> {
        batch
            .column(column)
            .as_any()
            .downcast_ref::<ListArray>()
            .filter(|array| array.null_count() == 0)
            .ok_or_else(|| invalid("V36 super-cell model list differs"))
    };
    let centroid_list = list(0)?;
    let centroid_array = centroid_list.value(0);
    let centroid_array = centroid_array
        .as_any()
        .downcast_ref::<FixedSizeListArray>()
        .filter(|array| array.null_count() == 0 && array.value_length() == 192)
        .ok_or_else(|| invalid("V36 super-cell model centroids differ"))?;
    let centroid_values = centroid_array
        .values()
        .as_any()
        .downcast_ref::<Float32Array>()
        .filter(|array| array.null_count() == 0)
        .ok_or_else(|| invalid("V36 super-cell model centroids differ"))?;
    let (centroids, remainder) = centroid_values.values().as_chunks::<192>();
    if !remainder.is_empty() {
        return Err(invalid("V36 super-cell model centroids differ"));
    }
    let centroids = centroids.to_vec();
    let ordinal_values = |column: usize| -> Result<Vec<u64>> {
        let values = list(column)?.value(0);
        let values = values
            .as_any()
            .downcast_ref::<UInt64Array>()
            .filter(|array| array.null_count() == 0)
            .ok_or_else(|| invalid("V36 super-cell model ordinals differ"))?;
        Ok(values.values().to_vec())
    };
    let model = V36SupercellModel {
        centroids,
        empty_repairs: manifest.empty_repairs,
        initialization_source_ordinals: ordinal_values(1)?,
        projected_corpus_sha256: manifest.training.projected_corpus_sha256.clone(),
        reservoir_source_ordinals: ordinal_values(2)?,
    };
    validate_v36_supercell_model(&model, expected_training)?;
    Ok(V36AuthenticatedSupercellModel {
        identity: identity.clone(),
        model,
        training_spec: expected_training.clone(),
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct V36ReservoirKey {
    digest: [u8; 32],
    source_ordinal: u64,
}

impl Ord for V36ReservoirKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.digest
            .cmp(&other.digest)
            .then_with(|| self.source_ordinal.cmp(&other.source_ordinal))
    }
}

impl PartialOrd for V36ReservoirKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

fn scan_v36_projected_corpus(
    spec: &V36SupercellTrainingSpec,
    source: &mut dyn V36ProjectedCorpusSource,
    mut consume: impl FnMut(u64, &[f32]) -> Result<()>,
) -> Result<String> {
    let mut expected_ordinal = 0_u64;
    let mut replay_sha256 = Sha256::new();
    replay_sha256.update(b"borsuk-v36-projected-corpus-replay-v1");
    source.scan(&mut |ordinals, vectors| {
        let expected_values = ordinals
            .len()
            .checked_mul(spec.dimensions)
            .ok_or_else(|| invalid("V36 projected corpus block shape overflows"))?;
        if ordinals.is_empty()
            || ordinals.len() > spec.maximum_block_rows
            || vectors.len() != expected_values
        {
            return Err(invalid("V36 projected corpus block differs"));
        }
        for (row, ordinal) in ordinals.iter().copied().enumerate() {
            if ordinal != expected_ordinal {
                return Err(invalid("V36 projected corpus ordering differs"));
            }
            let start = row
                .checked_mul(spec.dimensions)
                .ok_or_else(|| invalid("V36 projected corpus block shape overflows"))?;
            let vector = &vectors[start..start + spec.dimensions];
            if vector
                .iter()
                .any(|value| !value.is_finite() || (*value == 0.0 && value.is_sign_negative()))
            {
                return Err(invalid("V36 projected corpus vector differs"));
            }
            replay_sha256.update(ordinal.to_le_bytes());
            for value in vector {
                replay_sha256.update(value.to_bits().to_le_bytes());
            }
            consume(ordinal, vector)?;
            expected_ordinal = expected_ordinal
                .checked_add(1)
                .ok_or_else(|| invalid("V36 projected corpus row count overflows"))?;
        }
        Ok(())
    })?;
    if expected_ordinal != spec.corpus_rows {
        return Err(invalid("V36 projected corpus row count differs"));
    }
    Ok(format!("{:x}", replay_sha256.finalize()))
}

fn try_filled_v36_training_vec<T: Clone>(
    len: usize,
    value: T,
    allocation: &'static str,
) -> Result<Vec<T>> {
    let mut output = Vec::new();
    output
        .try_reserve_exact(len)
        .map_err(|_| invalid(allocation))?;
    output.resize(len, value);
    Ok(output)
}

/// Train the query-independent V36 super cells from a bounded SHA-ranked reservoir.
pub fn train_v36_supercells(
    spec: &V36SupercellTrainingSpec,
    source: &mut dyn V36ProjectedCorpusSource,
) -> Result<V36SupercellModel> {
    if spec.corpus_rows == 0
        || spec.dimensions != 192
        || spec.maximum_block_rows == 0
        || spec.maximum_block_rows > 65_536
        || !valid_sha256(&spec.projected_corpus_sha256)
        || spec.reservoir_rows == 0
        || spec.reservoir_rows > spec.corpus_rows
        || spec.reservoir_rows > 1_048_576
        || spec.super_cell_count == 0
        || !spec.super_cell_count.is_power_of_two()
        || spec.super_cell_count > 4_096
        || u64::from(spec.super_cell_count) > spec.reservoir_rows
    {
        return Err(invalid("V36 super-cell training authority differs"));
    }
    let reservoir_rows = usize::try_from(spec.reservoir_rows)
        .map_err(|_| invalid("V36 super-cell reservoir row count overflows"))?;
    let mut selected = BinaryHeap::new();
    selected
        .try_reserve_exact(reservoir_rows)
        .map_err(|_| invalid("V36 super-cell reservoir allocation exceeds capacity"))?;
    let replay_sha256 = scan_v36_projected_corpus(spec, source, |source_ordinal, _| {
        let mut digest = Sha256::new();
        digest.update(b"borsuk-v36-geometry-reservoir-v1");
        digest.update(source_ordinal.to_le_bytes());
        let key = V36ReservoirKey {
            digest: digest.finalize().into(),
            source_ordinal,
        };
        if selected.len() < reservoir_rows {
            selected.push(key);
        } else if selected.peek().is_some_and(|largest| key < *largest) {
            selected.pop();
            selected.push(key);
        }
        Ok(())
    })?;
    let mut reservoir_source_ordinals = Vec::new();
    reservoir_source_ordinals
        .try_reserve_exact(reservoir_rows)
        .map_err(|_| invalid("V36 super-cell reservoir allocation exceeds capacity"))?;
    reservoir_source_ordinals.extend(selected.into_iter().map(|key| key.source_ordinal));
    reservoir_source_ordinals.sort_unstable();

    if replay_sha256 != spec.projected_corpus_sha256 {
        return Err(invalid("V36 projected corpus authority differs"));
    }
    let mut rows = Vec::<[f32; 192]>::new();
    rows.try_reserve_exact(reservoir_rows)
        .map_err(|_| invalid("V36 super-cell reservoir allocation exceeds capacity"))?;
    let mut selected_index = 0_usize;
    let second_replay_sha256 =
        scan_v36_projected_corpus(spec, source, |source_ordinal, vector| {
            if reservoir_source_ordinals.get(selected_index) == Some(&source_ordinal) {
                rows.push(
                    vector
                        .try_into()
                        .map_err(|_| invalid("V36 super-cell reservoir vector differs"))?,
                );
                selected_index += 1;
            }
            Ok(())
        })?;
    if second_replay_sha256 != replay_sha256
        || rows.len() != reservoir_rows
        || selected_index != reservoir_rows
    {
        return Err(invalid("V36 super-cell reservoir differs"));
    }

    let centroid_count = usize::try_from(spec.super_cell_count)
        .map_err(|_| invalid("V36 super-cell count overflows"))?;
    let mut selected_rows = try_filled_v36_training_vec(
        rows.len(),
        false,
        "V36 super-cell selection allocation exceeds capacity",
    )?;
    selected_rows[0] = true;
    let mut centroids = Vec::new();
    centroids
        .try_reserve_exact(centroid_count)
        .map_err(|_| invalid("V36 super-cell centroid allocation exceeds capacity"))?;
    centroids.push(rows[0]);
    let mut initialization_source_ordinals = Vec::new();
    initialization_source_ordinals
        .try_reserve_exact(centroid_count)
        .map_err(|_| invalid("V36 super-cell seed allocation exceeds capacity"))?;
    initialization_source_ordinals.push(reservoir_source_ordinals[0]);
    let mut nearest_distances = try_filled_v36_training_vec(
        rows.len(),
        f64::INFINITY,
        "V36 super-cell distance allocation exceeds capacity",
    )?;
    while centroids.len() < centroid_count {
        let newest = centroids
            .last()
            .ok_or_else(|| invalid("V36 super-cell initialization differs"))?;
        let mut best = None::<(f64, u64, usize)>;
        for (row_index, vector) in rows.iter().enumerate() {
            if selected_rows[row_index] {
                continue;
            }
            nearest_distances[row_index] =
                nearest_distances[row_index].min(squared_l2(vector.as_slice(), newest.as_slice())?);
            let candidate = (
                nearest_distances[row_index],
                reservoir_source_ordinals[row_index],
                row_index,
            );
            if best.as_ref().is_none_or(|current| {
                candidate.0 > current.0 || (candidate.0 == current.0 && candidate.1 < current.1)
            }) {
                best = Some(candidate);
            }
        }
        let row_index = best
            .map(|candidate| candidate.2)
            .ok_or_else(|| invalid("V36 super-cell initialization differs"))?;
        selected_rows[row_index] = true;
        centroids.push(rows[row_index]);
        initialization_source_ordinals.push(reservoir_source_ordinals[row_index]);
    }

    let mut assignments = try_filled_v36_training_vec(
        rows.len(),
        0_usize,
        "V36 super-cell assignment allocation exceeds capacity",
    )?;
    let mut assigned_distances = try_filled_v36_training_vec(
        rows.len(),
        0.0_f64,
        "V36 super-cell assigned-distance allocation exceeds capacity",
    )?;
    let mut empty_repairs = 0_u64;
    for _ in 0..25 {
        let mut counts = try_filled_v36_training_vec(
            centroid_count,
            0_usize,
            "V36 super-cell count allocation exceeds capacity",
        )?;
        for (row_index, vector) in rows.iter().enumerate() {
            let mut best = (squared_l2(vector, &centroids[0])?, 0_usize);
            for (centroid, candidate) in centroids.iter().enumerate().skip(1) {
                let distance = squared_l2(vector, candidate)?;
                if distance < best.0 {
                    best = (distance, centroid);
                }
            }
            assignments[row_index] = best.1;
            assigned_distances[row_index] = best.0;
            counts[best.1] = counts[best.1]
                .checked_add(1)
                .ok_or_else(|| invalid("V36 super-cell assignment count overflows"))?;
        }
        for empty in 0..centroid_count {
            if counts[empty] != 0 {
                continue;
            }
            let candidate = (0..rows.len())
                .filter(|row| counts[assignments[*row]] > 1)
                .max_by(|left, right| {
                    assigned_distances[*left]
                        .total_cmp(&assigned_distances[*right])
                        .then_with(|| {
                            reservoir_source_ordinals[*right].cmp(&reservoir_source_ordinals[*left])
                        })
                })
                .ok_or_else(|| invalid("V36 super-cell empty repair differs"))?;
            counts[assignments[candidate]] -= 1;
            assignments[candidate] = empty;
            counts[empty] = 1;
            empty_repairs = empty_repairs
                .checked_add(1)
                .ok_or_else(|| invalid("V36 super-cell empty repair count overflows"))?;
        }
        let mut sums = try_filled_v36_training_vec(
            centroid_count,
            [0.0_f64; 192],
            "V36 super-cell sum allocation exceeds capacity",
        )?;
        for (row, vector) in rows.iter().enumerate() {
            for (sum, value) in sums[assignments[row]].iter_mut().zip(vector) {
                *sum += f64::from(*value);
            }
        }
        for centroid in 0..centroid_count {
            let divisor = counts[centroid] as f64;
            for dimension in 0..192 {
                let value = (sums[centroid][dimension] / divisor) as f32;
                if !value.is_finite() {
                    return Err(invalid("V36 super-cell centroid is nonfinite"));
                }
                centroids[centroid][dimension] = if value == 0.0 { 0.0 } else { value };
            }
        }
    }
    Ok(V36SupercellModel {
        centroids,
        empty_repairs,
        initialization_source_ordinals,
        projected_corpus_sha256: replay_sha256,
        reservoir_source_ordinals,
    })
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

/// Exact-object reader used by the bounded 1M resident geometry diagnostic.
pub trait V36SupercellRunChunkSource {
    /// Read one complete registered per-supercell Arrow chunk.
    fn read_chunk(&mut self, artifact: &V36SupercellRunChunkArtifact) -> Result<Vec<u8>>;
}

#[derive(Debug, Clone, PartialEq)]
/// Claim-ineligible 1M bridge result used to fail fast before scalable training.
pub struct V36ResidentPostingDiagnostic {
    assignments: V36PostingAssignments,
    centroids: Vec<Vec<f32>>,
    postings_per_supercell: Vec<u32>,
}

impl V36ResidentPostingDiagnostic {
    /// Globally numbered posting centroids, grouped by supercell ordinal.
    pub fn centroids(&self) -> &[Vec<f32>] {
        &self.centroids
    }

    /// Exact Hamilton posting allocation in supercell ordinal order.
    pub fn postings_per_supercell(&self) -> &[u32] {
        &self.postings_per_supercell
    }

    /// Final ownership recomputed against every posting, never the training partition only.
    pub const fn assignments(&self) -> &V36PostingAssignments {
        &self.assignments
    }
}

/// Train one resident supercell at a time, then assign the complete 1M population globally.
///
/// This is deliberately a claim-ineligible fail-fast bridge. It does not replace the
/// external-sidecar trainer required for 10M/100M construction qualification.
pub fn run_v36_resident_posting_diagnostic(
    runs: &V36CommittedSupercellRuns,
    admitted: &V36AdmittedSupercellPostCountPlan,
    closure_epsilon: Option<f64>,
    worker_threads: usize,
    block_rows: usize,
    source: &mut dyn V36SupercellRunChunkSource,
) -> Result<V36ResidentPostingDiagnostic> {
    const MAXIMUM_DIAGNOSTIC_ROWS: u64 = 1_000_000;

    let spec = admitted.assignment_preflight.training_spec();
    let allocation = &admitted.projection.postings_per_supercell;
    if spec.corpus_rows == 0
        || spec.corpus_rows > MAXIMUM_DIAGNOSTIC_ROWS
        || runs.row_count != spec.corpus_rows
        || runs.assignment_root_identity.role != V36_EXTERNAL_ASSIGNMENT_ROOT_ROLE
        || admitted.run_rows.len() != usize::try_from(spec.super_cell_count).unwrap_or(usize::MAX)
        || allocation.len() != admitted.run_rows.len()
        || allocate_v36_hamilton_postings(
            &admitted.run_rows,
            u32::try_from(spec.corpus_rows.div_ceil(admitted.target_primary_rows))
                .map_err(|_| invalid("V36 resident diagnostic posting count overflows"))?,
        )? != *allocation
    {
        return Err(invalid("V36 resident diagnostic authority differs"));
    }

    let row_count = usize::try_from(spec.corpus_rows)
        .map_err(|_| invalid("V36 resident diagnostic row count overflows"))?;
    let cell_count = usize::try_from(spec.super_cell_count)
        .map_err(|_| invalid("V36 resident diagnostic supercell count overflows"))?;
    let mut rows = Vec::new();
    rows.try_reserve_exact(row_count)
        .map_err(|_| invalid("V36 resident diagnostic rows exceed capacity"))?;
    let mut row_cells = Vec::new();
    row_cells
        .try_reserve_exact(row_count)
        .map_err(|_| invalid("V36 resident diagnostic cells exceed capacity"))?;
    let mut observed_run_rows = vec![0_u64; cell_count];
    let mut coverage = vec![0_u8; row_count.div_ceil(8)];
    let mut previous_chunk = None::<(u32, u64)>;

    for artifact in &runs.chunks {
        let context = &artifact.context;
        let key = (context.supercell_ordinal, context.chunk_ordinal);
        let expected_ordinal = previous_chunk.map_or(0, |(cell, ordinal)| {
            if cell == context.supercell_ordinal {
                ordinal + 1
            } else {
                0
            }
        });
        if previous_chunk.is_some_and(|previous| key <= previous)
            || context.chunk_ordinal != expected_ordinal
            || context.training_spec != *spec
            || context.model_identity != admitted.assignment_preflight.model_identity
            || context.projected_corpus_sha256 != spec.projected_corpus_sha256
        {
            return Err(invalid("V36 resident diagnostic chunk inventory differs"));
        }
        let bytes = source.read_chunk(artifact)?;
        let decoded = decode_v36_supercell_run_chunk_arrow(&bytes, artifact)?;
        let cell = usize::try_from(context.supercell_ordinal)
            .map_err(|_| invalid("V36 resident diagnostic supercell overflows"))?;
        for row in decoded {
            let ordinal = usize::try_from(row.source_ordinal)
                .map_err(|_| invalid("V36 resident diagnostic source ordinal overflows"))?;
            let byte = coverage
                .get_mut(ordinal / 8)
                .ok_or_else(|| invalid("V36 resident diagnostic source coverage differs"))?;
            let mask = 1_u8 << (ordinal % 8);
            if *byte & mask != 0 {
                return Err(invalid("V36 resident diagnostic source coverage differs"));
            }
            *byte |= mask;
            observed_run_rows[cell] = observed_run_rows[cell]
                .checked_add(1)
                .ok_or_else(|| invalid("V36 resident diagnostic run count overflows"))?;
            rows.push((row.source_ordinal, row.projected.to_vec()));
            row_cells.push(cell);
        }
        previous_chunk = Some(key);
    }
    if rows.len() != row_count
        || observed_run_rows != admitted.run_rows
        || coverage.iter().enumerate().any(|(byte_index, byte)| {
            let expected = if byte_index + 1 == coverage.len() && row_count % 8 != 0 {
                (1_u8 << (row_count % 8)) - 1
            } else {
                u8::MAX
            };
            *byte != expected
        })
    {
        return Err(invalid("V36 resident diagnostic source coverage differs"));
    }

    let mut centroids = Vec::new();
    centroids
        .try_reserve_exact(
            allocation
                .iter()
                .try_fold(0_usize, |sum, postings| {
                    sum.checked_add(usize::try_from(*postings).ok()?)
                })
                .ok_or_else(|| invalid("V36 resident diagnostic posting count overflows"))?,
        )
        .map_err(|_| invalid("V36 resident diagnostic centroids exceed capacity"))?;
    for (cell, posting_count) in allocation.iter().copied().enumerate() {
        if posting_count == 0 {
            continue;
        }
        let local = rows
            .iter()
            .zip(&row_cells)
            .filter(|(_, row_cell)| **row_cell == cell)
            .map(|(row, _)| row.clone())
            .collect::<Vec<_>>();
        centroids.extend(train_v36_posting_centroids(&local, posting_count)?);
    }
    let assignments = assign_v36_postings(
        &rows,
        &centroids,
        closure_epsilon,
        worker_threads,
        block_rows,
        admitted.target_primary_rows,
    )?;
    Ok(V36ResidentPostingDiagnostic {
        assignments,
        centroids,
        postings_per_supercell: allocation.clone(),
    })
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

/// Frozen deterministic construction recipe for the V36 posting HNSW candidate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct V36PostingHnswRecipe {
    /// Maximum neighbors on layers above zero.
    pub m: u32,
    /// Maximum neighbors on layer zero.
    pub m0: u32,
    /// Construction candidate width.
    pub ef_construction: u32,
    /// Seed for the canonical ChaCha8 level stream.
    pub seed: u64,
    /// Maximum accepted node level.
    pub maximum_level: u8,
    /// Exact level-generation algorithm identity.
    pub level_algorithm: String,
    /// Exact neighbor-selection algorithm identity.
    pub neighbor_algorithm: String,
}

impl V36PostingHnswRecipe {
    /// Return the only recipe admitted by the V36 qualification.
    pub fn frozen() -> Self {
        Self {
            m: 32,
            m0: 64,
            ef_construction: 200,
            seed: 36,
            maximum_level: 63,
            level_algorithm: "chacha8-u53-ln32-v1".to_owned(),
            neighbor_algorithm: "hnsw-diversity-fill-v1".to_owned(),
        }
    }
}

/// Derive the frozen deterministic node-level stream in posting-ordinal order.
pub fn derive_v36_posting_hnsw_levels(
    posting_count: u32,
    recipe: &V36PostingHnswRecipe,
) -> Result<Vec<u8>> {
    if posting_count == 0 || recipe != &V36PostingHnswRecipe::frozen() {
        return Err(invalid("V36 posting HNSW recipe differs"));
    }
    let mut rng = ChaCha8Rng::seed_from_u64(recipe.seed);
    let inverse_denominator = 1.0 / 9_007_199_254_740_992.0;
    let inverse_log_m = 1.0 / f64::from(recipe.m).ln();
    Ok((0..posting_count)
        .map(|_| {
            let unit = ((rng.next_u64() >> 11) + 1) as f64 * inverse_denominator;
            (-unit.ln() * inverse_log_m)
                .floor()
                .min(f64::from(recipe.maximum_level)) as u8
        })
        .collect())
}

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
    /// Decoded-hot p50 of candidate generation plus exact reranking.
    pub accelerated_decoded_hot_p50_ns: u64,
    /// Decoded-hot p95 of candidate generation plus exact reranking.
    pub accelerated_decoded_hot_p95_ns: u64,
    /// Decoded-hot p99 of candidate generation plus exact reranking.
    pub accelerated_decoded_hot_p99_ns: u64,
    /// Decoded-hot p50 of exhaustive exact selected scoring.
    pub exhaustive_decoded_hot_p50_ns: u64,
    /// Decoded-hot p95 of exhaustive exact selected scoring.
    pub exhaustive_decoded_hot_p95_ns: u64,
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

/// Complete comparison for one registered query and causal prefix boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct V36PostingPrefixComparison {
    /// Registered query ordinal.
    pub query_ordinal: u32,
    /// Requested final selected-score prefix length.
    pub requested_prefix_length: u32,
    /// Exhaustive selected-score authority prefix.
    pub exhaustive_prefix: Vec<u32>,
    /// Complete bounded candidate set produced by the accelerator.
    pub accelerated_candidates: Vec<u32>,
    /// Exact selected-score rerank of the accelerated candidate set.
    pub accelerated_prefix: Vec<u32>,
}

/// First positional difference between exhaustive and accelerated prefixes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct V36PostingOrderedDisagreement {
    /// Registered query ordinal.
    pub query_ordinal: u32,
    /// Requested prefix length at this boundary.
    pub requested_prefix_length: u32,
    /// Zero-based position of the first difference.
    pub position: u32,
    /// Exhaustive posting ordinal at the differing position.
    pub exhaustive_posting_ordinal: u32,
    /// Accelerated posting ordinal at the differing position.
    pub accelerated_posting_ordinal: u32,
}

/// First exhaustive-prefix posting omitted from the accelerated candidate set.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct V36PostingMissingCandidate {
    /// Registered query ordinal.
    pub query_ordinal: u32,
    /// Requested prefix length at this boundary.
    pub requested_prefix_length: u32,
    /// Exhaustive posting ordinal absent from the candidate set.
    pub exhaustive_posting_ordinal: u32,
}

/// Independently derived accelerator parity and containment evidence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct V36PostingParityEvidence {
    /// Number of registered query-prefix comparisons.
    pub query_prefix_pairs: u64,
    /// Comparisons with exactly equal ordered prefixes.
    pub ordered_prefix_matches: u64,
    /// `floor(1_000_000 * ordered_prefix_matches / query_prefix_pairs)`.
    pub parity_ppm: u32,
    /// Comparisons whose candidate set contains the complete exhaustive prefix.
    pub candidate_containment_matches: u64,
    /// Candidate-containment fraction in parts per million.
    pub candidate_containment_ppm: u32,
    /// First positional disagreement in registered comparison order.
    pub first_ordered_disagreement: Option<V36PostingOrderedDisagreement>,
    /// First exhaustive prefix item absent from the candidate set.
    pub first_missing_candidate: Option<V36PostingMissingCandidate>,
}

/// Scalar-control versus SIMD evidence for one bounded flat-centroid query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct V36PostingCentroidDifferentialEvidence {
    /// Registered query ordinal.
    pub query_ordinal: u32,
    /// Authenticated posting-centroid population.
    pub posting_count: u32,
    /// Candidate prefix length compared by both kernels.
    pub candidate_count: u32,
    /// Scalar-control candidate order.
    pub scalar_candidates: Vec<u32>,
    /// Serving SIMD candidate order.
    pub simd_candidates: Vec<u32>,
    /// Whether both kernels produced the same ordered candidate prefix.
    pub ordered_candidates_equal: bool,
    /// Maximum ceil-rounded relative distance error in parts per million.
    pub maximum_relative_error_ppm: u64,
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct V36FlatCentroidCandidate {
    distance: f32,
    posting_ordinal: u32,
}

/// Once-authenticated contiguous posting centroids shared by query accelerators.
#[derive(Debug, Clone)]
pub struct V36AuthenticatedPostingCentroids {
    centroids: Arc<[[f32; 192]]>,
}

impl V36AuthenticatedPostingCentroids {
    /// Number of authenticated posting centroids.
    pub fn posting_count(&self) -> u32 {
        self.centroids.len() as u32
    }

    pub(crate) fn centroids(&self) -> &[[f32; 192]] {
        &self.centroids
    }
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

/// Authenticate one contiguous posting-centroid population before query execution.
pub fn authenticate_v36_posting_centroids(
    centroids: Vec<[f32; 192]>,
) -> Result<V36AuthenticatedPostingCentroids> {
    if centroids.is_empty()
        || centroids.len() > u32::MAX as usize
        || centroids.iter().flatten().any(|value| {
            !value.is_finite() || (*value == 0.0 && value.to_bits() != 0.0_f32.to_bits())
        })
    {
        return Err(invalid("V36 posting centroid authority differs"));
    }
    Ok(V36AuthenticatedPostingCentroids {
        centroids: Arc::from(centroids),
    })
}

/// Select a bounded centroid-L2 candidate set without a population-sized rank buffer.
pub fn select_v36_flat_centroid_candidates(
    centroids: &V36AuthenticatedPostingCentroids,
    query: &[f32],
    candidate_count: u32,
) -> Result<Vec<u32>> {
    let candidate_count = usize::try_from(candidate_count)
        .map_err(|_| invalid("V36 flat centroid candidate count overflows"))?;
    if candidate_count == 0
        || candidate_count > centroids.centroids.len()
        || invalid_v36_vector(query)
    {
        return Err(invalid("V36 flat centroid candidate authority differs"));
    }

    let mut best = BinaryHeap::with_capacity(candidate_count);
    for (posting_ordinal, centroid) in centroids.centroids.iter().enumerate() {
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

/// Compare the SIMD flat-centroid kernel with its scalar control in one bounded scan.
pub fn diagnose_v36_flat_centroid_scalar_simd(
    query_ordinal: u32,
    centroids: &V36AuthenticatedPostingCentroids,
    query: &[f32],
    candidate_count: u32,
) -> Result<V36PostingCentroidDifferentialEvidence> {
    let candidate_capacity = usize::try_from(candidate_count)
        .map_err(|_| invalid("V36 flat centroid candidate count overflows"))?;
    if candidate_capacity == 0
        || candidate_capacity > centroids.centroids.len()
        || invalid_v36_vector(query)
    {
        return Err(invalid("V36 flat centroid differential authority differs"));
    }
    let mut scalar_best = BinaryHeap::with_capacity(candidate_capacity);
    let mut simd_best = BinaryHeap::with_capacity(candidate_capacity);
    let mut maximum_relative_error_ppm = 0_u64;
    for (posting_ordinal, centroid) in centroids.centroids.iter().enumerate() {
        let scalar = crate::metric::squared_euclidean_scalar(centroid, query);
        let simd = crate::metric::squared_euclidean_simd(centroid, query);
        if !scalar.is_finite() || !simd.is_finite() || scalar < 0.0 || simd < 0.0 {
            return Err(invalid("V36 flat centroid differential is nonfinite"));
        }
        let relative_error =
            f64::from((scalar - simd).abs()) / f64::from(scalar.abs().max(f32::MIN_POSITIVE));
        let error_ppm = (relative_error * 1_000_000.0).ceil();
        if !error_ppm.is_finite() || error_ppm > u64::MAX as f64 {
            return Err(invalid("V36 flat centroid differential error overflows"));
        }
        maximum_relative_error_ppm = maximum_relative_error_ppm.max(error_ppm as u64);
        let posting_ordinal = u32::try_from(posting_ordinal)
            .map_err(|_| invalid("V36 flat centroid ordinal overflows"))?;
        for (best, distance) in [(&mut scalar_best, scalar), (&mut simd_best, simd)] {
            let candidate = V36FlatCentroidCandidate {
                distance: if distance == 0.0 { 0.0 } else { distance },
                posting_ordinal,
            };
            if best.len() < candidate_capacity {
                best.push(candidate);
            } else if best.peek().is_some_and(|farthest| candidate < *farthest) {
                best.pop();
                best.push(candidate);
            }
        }
    }
    let ordered = |heap: BinaryHeap<V36FlatCentroidCandidate>| {
        let mut candidates = heap.into_vec();
        candidates.sort_unstable();
        candidates
            .into_iter()
            .map(|candidate| candidate.posting_ordinal)
            .collect::<Vec<_>>()
    };
    let scalar_candidates = ordered(scalar_best);
    let simd_candidates = ordered(simd_best);
    Ok(V36PostingCentroidDifferentialEvidence {
        query_ordinal,
        posting_count: centroids.posting_count(),
        candidate_count,
        ordered_candidates_equal: scalar_candidates == simd_candidates,
        scalar_candidates,
        simd_candidates,
        maximum_relative_error_ppm,
    })
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

fn sorted_v36_posting_ordinals(ordinals: &[u32], posting_count: u32) -> Option<Vec<u32>> {
    if ordinals.iter().any(|ordinal| *ordinal >= posting_count) {
        return None;
    }
    let mut sorted = ordinals.to_vec();
    sorted.sort_unstable();
    (!sorted.windows(2).any(|pair| pair[0] == pair[1])).then_some(sorted)
}

/// Compare complete registered accelerator prefixes against exhaustive authority.
pub fn compare_v36_posting_prefixes(
    posting_count: u32,
    comparisons: &[V36PostingPrefixComparison],
) -> Result<V36PostingParityEvidence> {
    if posting_count == 0 || comparisons.is_empty() {
        return Err(invalid("V36 posting accelerator parity population differs"));
    }
    let mut previous_key = None;
    let mut ordered_prefix_matches = 0_u64;
    let mut candidate_containment_matches = 0_u64;
    let mut first_ordered_disagreement = None;
    let mut first_missing_candidate = None;
    for comparison in comparisons {
        let key = (comparison.query_ordinal, comparison.requested_prefix_length);
        if previous_key.is_some_and(|previous| previous >= key) {
            return Err(invalid("V36 posting accelerator parity order differs"));
        }
        previous_key = Some(key);
        let prefix_length = usize::try_from(comparison.requested_prefix_length)
            .map_err(|_| invalid("V36 posting accelerator prefix overflows"))?;
        let exhaustive_ordinals =
            sorted_v36_posting_ordinals(&comparison.exhaustive_prefix, posting_count);
        let accelerated_prefix_ordinals =
            sorted_v36_posting_ordinals(&comparison.accelerated_prefix, posting_count);
        let accelerated_candidates =
            sorted_v36_posting_ordinals(&comparison.accelerated_candidates, posting_count);
        if prefix_length == 0
            || comparison.exhaustive_prefix.len() != prefix_length
            || comparison.accelerated_prefix.len() != prefix_length
            || comparison.accelerated_candidates.len() < prefix_length
            || exhaustive_ordinals.is_none()
            || accelerated_prefix_ordinals.is_none()
            || accelerated_candidates.is_none()
        {
            return Err(invalid("V36 posting accelerator parity authority differs"));
        }
        let accelerated_candidates = accelerated_candidates
            .ok_or_else(|| invalid("V36 posting accelerator parity authority differs"))?;
        if comparison
            .accelerated_prefix
            .iter()
            .any(|ordinal| accelerated_candidates.binary_search(ordinal).is_err())
        {
            return Err(invalid("V36 posting accelerator parity authority differs"));
        }

        if comparison.exhaustive_prefix == comparison.accelerated_prefix {
            ordered_prefix_matches = ordered_prefix_matches
                .checked_add(1)
                .ok_or_else(|| invalid("V36 posting accelerator parity count overflows"))?;
        } else if first_ordered_disagreement.is_none() {
            let position = comparison
                .exhaustive_prefix
                .iter()
                .zip(&comparison.accelerated_prefix)
                .position(|(exhaustive, accelerated)| exhaustive != accelerated)
                .ok_or_else(|| invalid("V36 posting accelerator disagreement differs"))?;
            first_ordered_disagreement = Some(V36PostingOrderedDisagreement {
                query_ordinal: comparison.query_ordinal,
                requested_prefix_length: comparison.requested_prefix_length,
                position: u32::try_from(position)
                    .map_err(|_| invalid("V36 posting accelerator position overflows"))?,
                exhaustive_posting_ordinal: comparison.exhaustive_prefix[position],
                accelerated_posting_ordinal: comparison.accelerated_prefix[position],
            });
        }

        if let Some(exhaustive_posting_ordinal) = comparison
            .exhaustive_prefix
            .iter()
            .find(|ordinal| accelerated_candidates.binary_search(ordinal).is_err())
        {
            if first_missing_candidate.is_none() {
                first_missing_candidate = Some(V36PostingMissingCandidate {
                    query_ordinal: comparison.query_ordinal,
                    requested_prefix_length: comparison.requested_prefix_length,
                    exhaustive_posting_ordinal: *exhaustive_posting_ordinal,
                });
            }
        } else {
            candidate_containment_matches = candidate_containment_matches
                .checked_add(1)
                .ok_or_else(|| invalid("V36 posting accelerator containment count overflows"))?;
        }
    }

    let query_prefix_pairs = u64::try_from(comparisons.len())
        .map_err(|_| invalid("V36 posting accelerator pair count overflows"))?;
    let ppm = |matches: u64| -> Result<u32> {
        let value = matches
            .checked_mul(1_000_000)
            .ok_or_else(|| invalid("V36 posting accelerator ppm overflows"))?
            / query_prefix_pairs;
        u32::try_from(value).map_err(|_| invalid("V36 posting accelerator ppm overflows"))
    };
    Ok(V36PostingParityEvidence {
        query_prefix_pairs,
        ordered_prefix_matches,
        parity_ppm: ppm(ordered_prefix_matches)?,
        candidate_containment_matches,
        candidate_containment_ppm: ppm(candidate_containment_matches)?,
        first_ordered_disagreement,
        first_missing_candidate,
    })
}

/// Validate and recompute one accelerator qualification decision.
pub(crate) fn recompute_v36_posting_accelerator_qualification(
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
        || observation.accelerated_decoded_hot_p50_ns == 0
        || observation.accelerated_decoded_hot_p50_ns > observation.accelerated_decoded_hot_p95_ns
        || observation.accelerated_decoded_hot_p95_ns > observation.accelerated_decoded_hot_p99_ns
        || observation.accelerated_decoded_hot_p99_ns == 0
        || observation.exhaustive_decoded_hot_p50_ns == 0
        || observation.exhaustive_decoded_hot_p50_ns > observation.exhaustive_decoded_hot_p95_ns
        || observation.exhaustive_decoded_hot_p95_ns > observation.exhaustive_decoded_hot_p99_ns
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
    Ok(qualified)
}

/// Validate and recompute one accelerator qualification decision.
pub fn validate_v36_posting_accelerator_observation(
    observation: &V36PostingAcceleratorObservation,
) -> Result<bool> {
    let qualified = recompute_v36_posting_accelerator_qualification(observation)?;
    if observation.qualified != qualified {
        return Err(invalid("V36 posting accelerator decision differs"));
    }
    Ok(qualified)
}

/// Validate complete ordered strategy/rung coverage for every registered prefix boundary.
pub fn validate_v36_posting_accelerator_matrix(
    posting_count: u32,
    expected_query_prefix_pairs: u64,
    prefix_lengths: &[u32],
    observations: &[V36PostingAcceleratorObservation],
) -> Result<()> {
    if !v36_posting_acceleration_required(posting_count)?
        || expected_query_prefix_pairs == 0
        || prefix_lengths.is_empty()
        || prefix_lengths
            .iter()
            .any(|prefix| *prefix == 0 || *prefix > 1_024 || *prefix > posting_count)
        || prefix_lengths.windows(2).any(|pair| pair[0] >= pair[1])
    {
        return Err(invalid("V36 posting accelerator matrix authority differs"));
    }
    let mut observed = observations.iter();
    for prefix_length in prefix_lengths {
        for kind in [
            V36PostingAcceleratorKind::SimdFlatCentroid,
            V36PostingAcceleratorKind::HnswCentroid,
            V36PostingAcceleratorKind::HnswSelectedScore,
        ] {
            let mut previous_ef_search = None;
            for rung in V36_POSTING_ACCELERATOR_EF_LADDER {
                let ef_search = v36_effective_ef_search(*prefix_length, rung)?;
                if previous_ef_search == Some(ef_search) {
                    continue;
                }
                previous_ef_search = Some(ef_search);
                let observation = observed
                    .next()
                    .ok_or_else(|| invalid("V36 posting accelerator matrix is incomplete"))?;
                if observation.kind != kind
                    || observation.posting_count != posting_count
                    || observation.requested_prefix_length != *prefix_length
                    || observation.ef_search != ef_search
                    || observation.query_prefix_pairs != expected_query_prefix_pairs
                {
                    return Err(invalid("V36 posting accelerator matrix cell differs"));
                }
                validate_v36_posting_accelerator_observation(observation)?;
            }
        }
    }
    if observed.next().is_some() {
        return Err(invalid("V36 posting accelerator matrix has extra cells"));
    }
    Ok(())
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

/// Checked exact-assignment work and measured-throughput projection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct V36ExactAssignmentPreflight {
    /// Exact number of posting distances evaluated over the population.
    pub distance_evaluations: u128,
    /// Exact scalar component terms in those posting distances.
    pub component_terms: u128,
    /// Exact primary posting count implied by the target occupancy.
    pub posting_count: u64,
    /// Ceiling-scaled active time from the registered measured kernel sample.
    pub projected_active_ns: u128,
    /// Whether the projected active time is inside the registered wall cap.
    pub within_active_wall_cap: bool,
}

/// Project exact global-assignment work from one bounded measured kernel sample.
///
/// This is an outcome-blind construction preflight: it can stop an infeasible
/// corpus run before loading scientific vectors, without making a quality claim.
pub fn project_v36_exact_assignment_preflight(
    rows: u64,
    dimensions: u32,
    target_primary_rows: u64,
    measured_rows: u64,
    measured_postings: u32,
    measured_elapsed_ns: u64,
    maximum_active_wall_seconds: u64,
) -> Result<V36ExactAssignmentPreflight> {
    if rows == 0
        || dimensions == 0
        || target_primary_rows == 0
        || measured_rows == 0
        || measured_postings == 0
        || measured_elapsed_ns == 0
        || maximum_active_wall_seconds == 0
    {
        return Err(invalid("V36 exact-assignment preflight authority differs"));
    }
    let posting_count = rows.div_ceil(target_primary_rows);
    let distance_evaluations = u128::from(rows)
        .checked_mul(u128::from(posting_count))
        .ok_or_else(|| invalid("V36 exact-assignment work overflows"))?;
    let component_terms = distance_evaluations
        .checked_mul(u128::from(dimensions))
        .ok_or_else(|| invalid("V36 exact-assignment work overflows"))?;
    let measured_component_terms = u128::from(measured_rows)
        .checked_mul(u128::from(measured_postings))
        .and_then(|value| value.checked_mul(u128::from(dimensions)))
        .ok_or_else(|| invalid("V36 measured assignment work overflows"))?;
    let projected_active_ns = component_terms
        .checked_mul(u128::from(measured_elapsed_ns))
        .ok_or_else(|| invalid("V36 projected assignment time overflows"))?
        .div_ceil(measured_component_terms);
    let maximum_active_ns = u128::from(maximum_active_wall_seconds)
        .checked_mul(1_000_000_000)
        .ok_or_else(|| invalid("V36 active wall cap overflows"))?;
    Ok(V36ExactAssignmentPreflight {
        distance_evaluations,
        component_terms,
        posting_count,
        projected_active_ns,
        within_active_wall_cap: projected_active_ns <= maximum_active_ns,
    })
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
    use super::{
        Result, V36_EXTERNAL_ASSIGNMENT_ROOT_ROLE, V36AdmittedSupercellAssignmentPreflight,
        V36AdmittedSupercellPostCountPlan, V36ArtifactIdentity, V36AssignmentMergeChunkSource,
        V36AuthenticatedInitialAssignmentMergeGroup, V36AuthenticatedSupercellAssignmentShard,
        V36CommittedSupercellAssignments, V36CommittedSupercellRuns,
        V36ExternalMergeGenerationProjection, V36FollowupAssignmentMergeRunSink,
        V36InitialAssignmentMergeChunkArtifact, V36InitialAssignmentMergeGeneration,
        V36InitialAssignmentMergeGenerationSink, V36InitialAssignmentMergeGroup,
        V36InitialAssignmentMergeRunSink, V36PublicationCommitStatus, V36RowOwners,
        V36SupercellAssignmentAdmissionRequest, V36SupercellAssignmentProjection,
        V36SupercellAssignmentRow, V36SupercellAssignmentShardArtifact,
        V36SupercellAssignmentShardContext, V36SupercellRunChunkContext,
        V36SupercellRunChunkSource, V36SupercellRunPublisherSink, V36SupercellTrainingSpec,
        V36TerminalAssignmentSource, authenticate_v36_assignment_merge_generation_root,
        authenticate_v36_followup_assignment_merge_run_root,
        authenticate_v36_initial_assignment_merge_run_root,
        authenticate_v36_supercell_assignment_root,
        authenticate_v36_supercell_assignment_shard_arrow,
        commit_v36_followup_assignment_merge_generation,
        commit_v36_initial_assignment_merge_generation,
        decode_v36_initial_assignment_merge_chunk_arrow,
        encode_v36_initial_assignment_merge_chunk_arrow,
        encode_v36_supercell_assignment_shard_arrow, encode_v36_supercell_run_chunk_arrow, invalid,
        load_v36_initial_assignment_merge_group, plan_v36_followup_assignment_merge_generation,
        plan_v36_initial_assignment_merge_generation, publish_v36_supercell_run_chunks,
        repair_v36_empty_posting_assignments, run_v36_resident_posting_diagnostic,
        stream_v36_followup_assignment_merge_group, v36_canonical_json_value,
        v36_committed_assignment_root, write_v36_followup_assignment_merge_group,
        write_v36_initial_assignment_merge_group,
    };
    use sha2::{Digest, Sha256};

    #[derive(Default)]
    struct TerminalSource {
        merge_chunks: Vec<(Vec<u8>, V36InitialAssignmentMergeChunkArtifact)>,
        shard: Option<(Vec<u8>, V36SupercellAssignmentShardArtifact)>,
    }

    impl V36TerminalAssignmentSource for TerminalSource {
        fn read_assignment_shard(
            &mut self,
            artifact: &V36SupercellAssignmentShardArtifact,
        ) -> Result<Vec<u8>> {
            let (bytes, expected) = self
                .shard
                .as_ref()
                .ok_or_else(|| invalid("missing terminal shard"))?;
            if expected != artifact {
                return Err(invalid("terminal shard differs"));
            }
            Ok(bytes.clone())
        }

        fn read_merge_chunk(
            &mut self,
            _run_root: &V36ArtifactIdentity,
            artifact: &V36InitialAssignmentMergeChunkArtifact,
        ) -> Result<Vec<u8>> {
            self.merge_chunks
                .iter()
                .find(|(_, expected)| expected == artifact)
                .map(|(bytes, _)| bytes.clone())
                .ok_or_else(|| invalid("missing terminal merge chunk"))
        }
    }

    struct TerminalSink {
        aborted: bool,
        chunks: Vec<(Vec<u8>, super::V36SupercellRunChunkArtifact)>,
        commit_error: bool,
        commit_status: V36PublicationCommitStatus,
        events: Vec<&'static str>,
        root: Option<(Vec<u8>, V36ArtifactIdentity)>,
    }

    impl Default for TerminalSink {
        fn default() -> Self {
            Self {
                aborted: false,
                chunks: Vec::new(),
                commit_error: false,
                commit_status: V36PublicationCommitStatus::Committed,
                events: Vec::new(),
                root: None,
            }
        }
    }

    impl V36SupercellRunPublisherSink for TerminalSink {
        fn write_chunk_provisional(
            &mut self,
            bytes: &[u8],
            artifact: &super::V36SupercellRunChunkArtifact,
        ) -> Result<()> {
            self.events.push("chunk");
            self.chunks.push((bytes.to_vec(), artifact.clone()));
            Ok(())
        }

        fn commit_root(
            &mut self,
            root_bytes: &[u8],
            root_identity: &V36ArtifactIdentity,
        ) -> Result<V36PublicationCommitStatus> {
            self.events.push("root");
            self.root = Some((root_bytes.to_vec(), root_identity.clone()));
            if self.commit_error {
                return Err(invalid("ambiguous root acknowledgement"));
            }
            Ok(self.commit_status)
        }

        fn abort(&mut self) -> Result<()> {
            self.events.push("abort");
            self.aborted = true;
            self.chunks.clear();
            self.root = None;
            Ok(())
        }
    }

    #[test]
    fn v36_terminal_supercell_publisher_handles_single_shard_and_commits_root_last() {
        let training_spec = V36SupercellTrainingSpec {
            corpus_rows: 4,
            dimensions: 192,
            maximum_block_rows: 4,
            projected_corpus_sha256: "1".repeat(64),
            reservoir_rows: 4,
            super_cell_count: 3,
        };
        let model_identity = V36ArtifactIdentity {
            blake3: "2".repeat(64),
            encoded_bytes: 1,
            role: "supercell-model".to_owned(),
            sha256: "3".repeat(64),
            uri: "s3://borsuk-v36-test/geometry/supercells.arrow".to_owned(),
        };
        let admission = V36AdmittedSupercellAssignmentPreflight {
            model_identity: model_identity.clone(),
            training_spec: training_spec.clone(),
            request: V36SupercellAssignmentAdmissionRequest {
                worker_count: 1,
                queue_rows_per_worker: 4,
                sort_rows_per_worker: 4,
                merge_fan_in: 8,
                measured_component_terms: 1,
                measured_elapsed_ns: 1,
                measured_cost_microusd: 1,
                measured_external_work_units: 1,
                measured_external_elapsed_ns: 1,
                measured_external_cost_microusd: 1,
                maximum_active_wall_seconds: 1,
                maximum_cost_microusd: 1,
                maximum_peak_live_bytes: 1,
                maximum_scratch_bytes: 1,
            },
            projection: V36SupercellAssignmentProjection {
                logical_shards: 1,
                merge_generations: 0,
                merge_fan_in: 8,
                uncompressed_assignment_bytes: 1,
                required_scratch_bytes: 1,
                required_peak_live_bytes: 1,
                coverage_bitmap_bytes: 1,
                component_terms: 1,
                external_work_units: 1,
                publication_row_visits: 4,
                projected_active_ns: 1,
                projected_cost_microusd: 1,
            },
        };
        let mut projected = [0.0_f32; 192];
        projected[0] = 1.0;
        let rows = vec![
            V36SupercellAssignmentRow::new(0, 0, projected).unwrap(),
            V36SupercellAssignmentRow::new(0, 2, projected).unwrap(),
            V36SupercellAssignmentRow::new(2, 1, projected).unwrap(),
            V36SupercellAssignmentRow::new(2, 3, projected).unwrap(),
        ];
        let context = V36SupercellAssignmentShardContext {
            model_identity,
            projected_corpus_sha256: training_spec.projected_corpus_sha256.clone(),
            shard_ordinal: 0,
            training_spec,
            uri: "s3://borsuk-v36-test/geometry/assignments/shard-000000.arrow".to_owned(),
        };
        let (shard_bytes, shard_artifact) =
            encode_v36_supercell_assignment_shard_arrow(&context, &rows).unwrap();
        let artifacts = vec![shard_artifact.clone()];
        let uri_prefix = "s3://borsuk-v36-test/geometry/assignments";
        let (_, root_identity) =
            v36_committed_assignment_root(&admission, &artifacts, uri_prefix).unwrap();
        let committed = V36CommittedSupercellAssignments {
            admission,
            artifacts,
            root_identity,
            uri_prefix: uri_prefix.to_owned(),
        };
        let mut source = TerminalSource {
            shard: Some((shard_bytes, shard_artifact)),
            ..TerminalSource::default()
        };
        let mut sink = TerminalSink::default();

        let published =
            publish_v36_supercell_run_chunks(&committed, None, &mut source, &mut sink).unwrap();

        assert_eq!(sink.events, ["chunk", "chunk", "root"]);
        assert!(!sink.aborted);
        assert_eq!(published.chunks().len(), 2);
        assert_eq!(published.row_count(), 4);
        assert_eq!(
            published.assignment_root_identity(),
            committed.root_identity()
        );
        assert_eq!(
            published.terminal_root_identity(),
            committed.root_identity()
        );
        assert_eq!(published.root_identity(), &sink.root.as_ref().unwrap().1);
        assert_eq!(
            sink.chunks
                .iter()
                .map(|(_, artifact)| (artifact.context.supercell_ordinal, artifact.row_count))
                .collect::<Vec<_>>(),
            [(0, 2), (2, 2)]
        );

        let mut ambiguous_sink = TerminalSink {
            commit_error: true,
            ..TerminalSink::default()
        };
        assert!(
            publish_v36_supercell_run_chunks(&committed, None, &mut source, &mut ambiguous_sink,)
                .is_err()
        );
        assert!(!ambiguous_sink.aborted);
        assert!(!ambiguous_sink.chunks.is_empty());

        let mut rejected_sink = TerminalSink {
            commit_status: V36PublicationCommitStatus::NotCommitted,
            ..TerminalSink::default()
        };
        assert!(
            publish_v36_supercell_run_chunks(&committed, None, &mut source, &mut rejected_sink)
                .is_err()
        );
        assert!(rejected_sink.aborted);
        assert!(rejected_sink.chunks.is_empty());
    }

    #[test]
    fn v36_terminal_supercell_publisher_consumes_exact_final_merge_generation() {
        let training_spec = V36SupercellTrainingSpec {
            corpus_rows: 65_537,
            dimensions: 192,
            maximum_block_rows: 65_536,
            projected_corpus_sha256: "1".repeat(64),
            reservoir_rows: 4,
            super_cell_count: 2,
        };
        let model_identity = V36ArtifactIdentity {
            blake3: "2".repeat(64),
            encoded_bytes: 1,
            role: "supercell-model".to_owned(),
            sha256: "3".repeat(64),
            uri: "s3://borsuk-v36-test/geometry/supercells.arrow".to_owned(),
        };
        let admission = V36AdmittedSupercellAssignmentPreflight {
            model_identity: model_identity.clone(),
            training_spec: training_spec.clone(),
            request: V36SupercellAssignmentAdmissionRequest {
                worker_count: 1,
                queue_rows_per_worker: 65_536,
                sort_rows_per_worker: 65_536,
                merge_fan_in: 2,
                measured_component_terms: 1,
                measured_elapsed_ns: 1,
                measured_cost_microusd: 1,
                measured_external_work_units: 1,
                measured_external_elapsed_ns: 1,
                measured_external_cost_microusd: 1,
                maximum_active_wall_seconds: 1,
                maximum_cost_microusd: 1,
                maximum_peak_live_bytes: 1,
                maximum_scratch_bytes: 1,
            },
            projection: V36SupercellAssignmentProjection {
                logical_shards: 2,
                merge_generations: 1,
                merge_fan_in: 2,
                uncompressed_assignment_bytes: 1,
                required_scratch_bytes: 1,
                required_peak_live_bytes: 1,
                coverage_bitmap_bytes: 8_193,
                component_terms: 1,
                external_work_units: 1,
                publication_row_visits: 65_537,
                projected_active_ns: 1,
                projected_cost_microusd: 1,
            },
        };
        let mut projected = [0.0_f32; 192];
        projected[0] = 1.0;
        let first_shard = (0..65_536_u64)
            .map(|source| {
                V36SupercellAssignmentRow::new(
                    u32::try_from(source % 2).unwrap(),
                    source,
                    projected,
                )
                .unwrap()
            })
            .collect::<Vec<_>>();
        let mut first_shard = first_shard;
        first_shard.sort_unstable_by_key(|row| (row.supercell_ordinal, row.source_ordinal));
        let shard_rows = [
            first_shard,
            vec![V36SupercellAssignmentRow::new(0, 65_536, projected).unwrap()],
        ];
        let uri_prefix = "s3://borsuk-v36-test/geometry/assignments";
        let mut shard_bytes = Vec::new();
        let mut artifacts = Vec::new();
        for (shard_ordinal, rows) in shard_rows.iter().enumerate() {
            let shard_ordinal = u64::try_from(shard_ordinal).unwrap();
            let context = V36SupercellAssignmentShardContext {
                model_identity: model_identity.clone(),
                projected_corpus_sha256: training_spec.projected_corpus_sha256.clone(),
                shard_ordinal,
                training_spec: training_spec.clone(),
                uri: format!("{uri_prefix}/shard-{shard_ordinal:06}.arrow"),
            };
            let (bytes, artifact) =
                encode_v36_supercell_assignment_shard_arrow(&context, rows).unwrap();
            shard_bytes.push(bytes);
            artifacts.push(artifact);
        }
        let (_, assignment_root) =
            v36_committed_assignment_root(&admission, &artifacts, uri_prefix).unwrap();
        let committed = V36CommittedSupercellAssignments {
            admission,
            artifacts: artifacts.clone(),
            root_identity: assignment_root,
            uri_prefix: uri_prefix.to_owned(),
        };
        let authenticated = artifacts
            .iter()
            .zip(&shard_bytes)
            .map(|(artifact, bytes)| {
                authenticate_v36_supercell_assignment_shard_arrow(bytes, artifact).unwrap()
            })
            .collect();
        let group = load_v36_initial_assignment_merge_group(&committed, 0, authenticated).unwrap();
        let mut merge_sink = InitialMergeSink::default();
        let run = write_v36_initial_assignment_merge_group(&group, &mut merge_sink).unwrap();
        let mut generation_sink = InitialMergeGenerationSink::default();
        let generation = commit_v36_initial_assignment_merge_generation(
            &committed,
            vec![run],
            &mut generation_sink,
        )
        .unwrap();
        let mut source = TerminalSource {
            merge_chunks: merge_sink.chunks.clone(),
            shard: None,
        };
        let mut sink = TerminalSink::default();

        let published =
            publish_v36_supercell_run_chunks(&committed, Some(&generation), &mut source, &mut sink)
                .unwrap();

        assert_eq!(sink.events, ["chunk", "chunk", "root"]);
        assert_eq!(published.row_count(), 65_537);
        assert_eq!(
            published.terminal_root_identity(),
            generation.root_identity()
        );
        assert_eq!(
            published.assignment_root_identity(),
            committed.root_identity()
        );
        assert_eq!(
            sink.chunks
                .iter()
                .map(|(_, artifact)| (artifact.context.supercell_ordinal, artifact.row_count))
                .collect::<Vec<_>>(),
            [(0, 32_769), (1, 32_768)]
        );
    }

    #[derive(Default)]
    struct ResidentChunkSource {
        chunks: Vec<(Vec<u8>, super::V36SupercellRunChunkArtifact)>,
        reads: usize,
    }

    impl V36SupercellRunChunkSource for ResidentChunkSource {
        fn read_chunk(
            &mut self,
            artifact: &super::V36SupercellRunChunkArtifact,
        ) -> Result<Vec<u8>> {
            self.reads += 1;
            self.chunks
                .iter()
                .find(|(_, expected)| expected == artifact)
                .map(|(bytes, _)| bytes.clone())
                .ok_or_else(|| invalid("missing resident diagnostic chunk"))
        }
    }

    #[test]
    fn v36_resident_posting_diagnostic_trains_authenticated_runs_but_assigns_globally() {
        // Break caught: the 1M fail-fast bridge trusts unauthenticated row
        // bytes or incorrectly treats the training supercell as final posting
        // ownership instead of comparing every row with every posting.
        let training_spec = V36SupercellTrainingSpec {
            corpus_rows: 4,
            dimensions: 192,
            maximum_block_rows: 4,
            projected_corpus_sha256: "1".repeat(64),
            reservoir_rows: 4,
            super_cell_count: 2,
        };
        let model_identity = V36ArtifactIdentity {
            blake3: "2".repeat(64),
            encoded_bytes: 1,
            role: "supercell-model".to_owned(),
            sha256: "3".repeat(64),
            uri: "s3://borsuk-v36-test/geometry/supercells.arrow".to_owned(),
        };
        let assignment_preflight = V36AdmittedSupercellAssignmentPreflight {
            model_identity: model_identity.clone(),
            training_spec: training_spec.clone(),
            request: V36SupercellAssignmentAdmissionRequest {
                worker_count: 1,
                queue_rows_per_worker: 4,
                sort_rows_per_worker: 4,
                merge_fan_in: 2,
                measured_component_terms: 1,
                measured_elapsed_ns: 1,
                measured_cost_microusd: 1,
                measured_external_work_units: 1,
                measured_external_elapsed_ns: 1,
                measured_external_cost_microusd: 1,
                maximum_active_wall_seconds: 1,
                maximum_cost_microusd: 1,
                maximum_peak_live_bytes: 1,
                maximum_scratch_bytes: 1,
            },
            projection: V36SupercellAssignmentProjection {
                logical_shards: 1,
                merge_generations: 0,
                merge_fan_in: 2,
                uncompressed_assignment_bytes: 1,
                required_scratch_bytes: 1,
                required_peak_live_bytes: 1,
                coverage_bitmap_bytes: 1,
                component_terms: 1,
                external_work_units: 1,
                publication_row_visits: 4,
                projected_active_ns: 1,
                projected_cost_microusd: 1,
            },
        };
        let admitted = V36AdmittedSupercellPostCountPlan {
            assignment_preflight,
            run_rows: vec![2, 2],
            target_primary_rows: 2,
            request: super::V36SupercellPostCountAdmissionRequest {
                measured_component_terms: 1,
                measured_elapsed_ns: 1,
                measured_cost_microusd: 1,
                maximum_active_wall_seconds: 1,
                maximum_cost_microusd: 1,
                maximum_peak_live_bytes: 1,
                maximum_scratch_bytes: 1,
            },
            projection: super::V36SupercellPostCountProjection {
                posting_count: 2,
                postings_per_supercell: vec![1, 1],
                initialization_distance_evaluations: 0,
                lloyd_distance_evaluations: 40,
                repair_distance_evaluations: 40,
                source_reduction_terms: 7_680,
                local_component_terms: 23_040,
                required_scratch_bytes: 1,
                required_peak_live_bytes: 1,
                projected_active_ns: 1,
                projected_cost_microusd: 1,
            },
        };
        let vector = |x: f32| {
            let mut value = [0.0_f32; 192];
            value[0] = x;
            value
        };
        let rows = [
            vec![
                V36SupercellAssignmentRow::new(0, 0, vector(0.0)).unwrap(),
                V36SupercellAssignmentRow::new(0, 1, vector(100.0)).unwrap(),
            ],
            vec![
                V36SupercellAssignmentRow::new(1, 2, vector(51.0)).unwrap(),
                V36SupercellAssignmentRow::new(1, 3, vector(52.0)).unwrap(),
            ],
        ];
        let mut source = ResidentChunkSource::default();
        let mut artifacts = Vec::new();
        for (cell, rows) in rows.iter().enumerate() {
            let context = V36SupercellRunChunkContext {
                model_identity: model_identity.clone(),
                projected_corpus_sha256: training_spec.projected_corpus_sha256.clone(),
                supercell_ordinal: u32::try_from(cell).unwrap(),
                chunk_ordinal: 0,
                training_spec: training_spec.clone(),
                uri: format!(
                    "s3://borsuk-v36-test/geometry/assignments/supercells/cell-{cell:06}/chunk-000000.arrow"
                ),
            };
            let (bytes, artifact) = encode_v36_supercell_run_chunk_arrow(&context, rows).unwrap();
            source.chunks.push((bytes, artifact.clone()));
            artifacts.push(artifact);
        }
        let published = V36CommittedSupercellRuns {
            assignment_root_identity: V36ArtifactIdentity {
                blake3: "4".repeat(64),
                encoded_bytes: 1,
                role: V36_EXTERNAL_ASSIGNMENT_ROOT_ROLE.to_owned(),
                sha256: "5".repeat(64),
                uri: "s3://borsuk-v36-test/geometry/assignments/assignment-root.json".to_owned(),
            },
            chunks: artifacts,
            root_identity: V36ArtifactIdentity {
                blake3: "6".repeat(64),
                encoded_bytes: 1,
                role: super::V36_SUPERCELL_RUN_ROOT_ROLE.to_owned(),
                sha256: "7".repeat(64),
                uri: "s3://borsuk-v36-test/geometry/assignments/supercells/root.json".to_owned(),
            },
            row_count: 4,
            terminal_root_identity: V36ArtifactIdentity {
                blake3: "8".repeat(64),
                encoded_bytes: 1,
                role: V36_EXTERNAL_ASSIGNMENT_ROOT_ROLE.to_owned(),
                sha256: "9".repeat(64),
                uri: "s3://borsuk-v36-test/geometry/assignments/assignment-root.json".to_owned(),
            },
        };

        let diagnostic =
            run_v36_resident_posting_diagnostic(&published, &admitted, None, 1, 4, &mut source)
                .unwrap();

        assert_eq!(source.reads, 2);
        assert_eq!(diagnostic.postings_per_supercell(), &[1, 1]);
        assert_eq!(diagnostic.centroids()[0][0].to_bits(), 50.0_f32.to_bits());
        assert_eq!(diagnostic.centroids()[1][0].to_bits(), 51.5_f32.to_bits());
        assert_eq!(diagnostic.assignments().source_ordinals(), &[0, 1, 2, 3]);
        assert_eq!(diagnostic.assignments().primary_occupancy(), &[1, 3]);

        source.chunks[0].0[0] ^= 1;
        assert!(
            run_v36_resident_posting_diagnostic(&published, &admitted, None, 1, 4, &mut source,)
                .is_err()
        );
    }

    #[test]
    fn v36_row_owner_scratch_is_one_fixed_inline_record() {
        assert!(std::mem::size_of::<V36RowOwners>() <= 36);
    }

    #[test]
    fn v36_initial_merge_root_admits_maximum_fan_in_and_uri_lengths() {
        let maximum_uri = format!("s3://b/{}", "x".repeat(4_096 - "s3://b/".len()));
        let training_spec = V36SupercellTrainingSpec {
            corpus_rows: 64 * 65_536,
            dimensions: 192,
            maximum_block_rows: 1,
            projected_corpus_sha256: "1".repeat(64),
            reservoir_rows: 1,
            super_cell_count: 1,
        };
        let model_identity = V36ArtifactIdentity {
            blake3: "2".repeat(64),
            encoded_bytes: 1,
            role: "supercell-model".to_owned(),
            sha256: "3".repeat(64),
            uri: maximum_uri.clone(),
        };
        let predecessor_root_identity = V36ArtifactIdentity {
            blake3: "4".repeat(64),
            encoded_bytes: 1,
            role: V36_EXTERNAL_ASSIGNMENT_ROOT_ROLE.to_owned(),
            sha256: "5".repeat(64),
            uri: maximum_uri.clone(),
        };
        let inputs = (0..64_u64)
            .map(|shard_ordinal| {
                super::V36SupercellAssignmentShardArtifactWire::from(
                    &V36SupercellAssignmentShardArtifact {
                        blake3: "6".repeat(64),
                        context: V36SupercellAssignmentShardContext {
                            model_identity: model_identity.clone(),
                            projected_corpus_sha256: training_spec.projected_corpus_sha256.clone(),
                            shard_ordinal,
                            training_spec: training_spec.clone(),
                            uri: maximum_uri.clone(),
                        },
                        encoded_bytes: 1,
                        row_count: 65_536,
                        sha256: "7".repeat(64),
                    },
                )
            })
            .collect();
        let chunks = (0..64_u64)
            .map(|chunk_ordinal| {
                super::V36InitialAssignmentMergeChunkArtifactWire::from(
                    &V36InitialAssignmentMergeChunkArtifact {
                        blake3: "8".repeat(64),
                        context: super::V36InitialAssignmentMergeChunkContext {
                            chunk_ordinal,
                            generation_ordinal: 0,
                            group_ordinal: 0,
                            model_identity: model_identity.clone(),
                            predecessor_root_identity: predecessor_root_identity.clone(),
                            training_spec: training_spec.clone(),
                            uri: maximum_uri.clone(),
                        },
                        encoded_bytes: 1,
                        first_key: (0, chunk_ordinal * 65_536),
                        last_key: (0, (chunk_ordinal + 1) * 65_536 - 1),
                        row_count: 65_536,
                        sha256: "9".repeat(64),
                    },
                )
            })
            .collect();
        let manifest = super::V36InitialAssignmentMergeRootManifest {
            chunks,
            format: super::V36_INITIAL_ASSIGNMENT_MERGE_ROOT_FORMAT.to_owned(),
            generation_ordinal: 0,
            group_ordinal: 0,
            inputs,
            model_identity,
            predecessor_root_identity,
            role: super::V36_INITIAL_ASSIGNMENT_MERGE_ROOT_ROLE.to_owned(),
            row_count: training_spec.corpus_rows,
            training_spec,
            uri: maximum_uri,
        };
        let mut bytes = serde_json::to_vec(&v36_canonical_json_value(
            serde_json::to_value(manifest).unwrap(),
        ))
        .unwrap();
        bytes.push(b'\n');
        assert!(
            u64::try_from(bytes.len()).unwrap()
                <= super::V36_INITIAL_ASSIGNMENT_MERGE_ROOT_MAXIMUM_ENCODED_BYTES,
            "maximum admitted merge root is {} bytes but cap is {}",
            bytes.len(),
            super::V36_INITIAL_ASSIGNMENT_MERGE_ROOT_MAXIMUM_ENCODED_BYTES,
        );
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

    #[test]
    fn v36_initial_assignment_merge_generation_is_root_bound_and_fixed_fan_in() {
        // Break caught: the external builder invents groups from callback or
        // worker scheduling instead of the authenticated assignment root.
        let training_spec = V36SupercellTrainingSpec {
            corpus_rows: 1_048_577,
            dimensions: 192,
            maximum_block_rows: 65_536,
            projected_corpus_sha256: "1".repeat(64),
            reservoir_rows: 65_536,
            super_cell_count: 4,
        };
        let model_identity = V36ArtifactIdentity {
            blake3: "2".repeat(64),
            encoded_bytes: 1,
            role: "supercell-model".to_owned(),
            sha256: "3".repeat(64),
            uri: "s3://borsuk-v36-test/geometry/supercells.arrow".to_owned(),
        };
        let request = V36SupercellAssignmentAdmissionRequest {
            worker_count: 1,
            queue_rows_per_worker: 1,
            sort_rows_per_worker: 1,
            merge_fan_in: 8,
            measured_component_terms: 1,
            measured_elapsed_ns: 1,
            measured_cost_microusd: 1,
            measured_external_work_units: 1,
            measured_external_elapsed_ns: 1,
            measured_external_cost_microusd: 1,
            maximum_active_wall_seconds: 1,
            maximum_cost_microusd: 1,
            maximum_peak_live_bytes: 1,
            maximum_scratch_bytes: 1,
        };
        let projection = V36SupercellAssignmentProjection {
            logical_shards: 17,
            merge_generations: 2,
            merge_fan_in: 8,
            uncompressed_assignment_bytes: 1,
            required_scratch_bytes: 1,
            required_peak_live_bytes: 1,
            coverage_bitmap_bytes: 1,
            component_terms: 1,
            external_work_units: 1,
            publication_row_visits: 1,
            projected_active_ns: 1,
            projected_cost_microusd: 1,
        };
        let admission = V36AdmittedSupercellAssignmentPreflight {
            model_identity: model_identity.clone(),
            training_spec: training_spec.clone(),
            request,
            projection,
        };
        let artifacts = (0..17)
            .map(|shard_ordinal| V36SupercellAssignmentShardArtifact {
                blake3: format!("{shard_ordinal:064x}"),
                context: V36SupercellAssignmentShardContext {
                    model_identity: model_identity.clone(),
                    projected_corpus_sha256: training_spec.projected_corpus_sha256.clone(),
                    shard_ordinal,
                    training_spec: training_spec.clone(),
                    uri: format!(
                        "s3://borsuk-v36-test/geometry/assignments/shard-{shard_ordinal:06}.arrow"
                    ),
                },
                encoded_bytes: 1,
                row_count: if shard_ordinal == 16 { 1 } else { 65_536 },
                sha256: format!("{:064x}", shard_ordinal + 17),
            })
            .collect::<Vec<_>>();
        let uri_prefix = "s3://borsuk-v36-test/geometry/assignments";
        let (root_bytes, root_identity) =
            v36_committed_assignment_root(&admission, &artifacts, uri_prefix).unwrap();
        assert_eq!(root_identity.role, V36_EXTERNAL_ASSIGNMENT_ROOT_ROLE);
        let committed = V36CommittedSupercellAssignments {
            admission,
            artifacts,
            root_identity: root_identity.clone(),
            uri_prefix: uri_prefix.to_owned(),
        };
        let resumed = authenticate_v36_supercell_assignment_root(
            committed.admission(),
            &root_bytes,
            &root_identity,
        )
        .unwrap();
        assert_eq!(resumed.artifacts(), committed.artifacts());
        assert_eq!(resumed.root_identity(), committed.root_identity());
        assert_eq!(resumed.uri_prefix(), committed.uri_prefix());
        let mut corrupt_root = root_bytes;
        corrupt_root[0] ^= 1;
        assert!(
            authenticate_v36_supercell_assignment_root(
                committed.admission(),
                &corrupt_root,
                &root_identity,
            )
            .is_err()
        );

        let plan = plan_v36_initial_assignment_merge_generation(&committed)
            .unwrap()
            .unwrap();
        assert_eq!(plan.predecessor_root_identity(), &root_identity);
        assert_eq!(
            plan.generation(),
            V36ExternalMergeGenerationProjection {
                generation_ordinal: 0,
                input_run_count: 17,
                output_run_count: 3,
                full_group_count: 2,
                tail_group_size: 1,
            }
        );
        assert_eq!(
            plan.groups()
                .iter()
                .map(|group| (
                    group.group_ordinal(),
                    group.input_range(),
                    group.output_row_count(),
                    group.output_chunk_count(),
                    group.output_root_uri(),
                ))
                .collect::<Vec<_>>(),
            vec![
                (
                    0,
                    0..8,
                    524_288,
                    8,
                    "s3://borsuk-v36-test/geometry/assignments/merge/generation-000000/run-000000/root.json",
                ),
                (
                    1,
                    8..16,
                    524_288,
                    8,
                    "s3://borsuk-v36-test/geometry/assignments/merge/generation-000000/run-000001/root.json",
                ),
                (
                    2,
                    16..17,
                    1,
                    1,
                    "s3://borsuk-v36-test/geometry/assignments/merge/generation-000000/run-000002/root.json",
                ),
            ]
        );

        let mut detached = committed.clone();
        detached.root_identity.sha256 = "6".repeat(64);
        assert!(plan_v36_initial_assignment_merge_generation(&detached).is_err());

        let mut overlong = committed;
        overlong.uri_prefix = format!("s3://b/{}", "a".repeat(4_063));
        let (_, overlong_root_identity) = v36_committed_assignment_root(
            &overlong.admission,
            &overlong.artifacts,
            &overlong.uri_prefix,
        )
        .unwrap();
        overlong.root_identity = overlong_root_identity;
        assert!(plan_v36_initial_assignment_merge_generation(&overlong).is_err());
    }

    #[test]
    fn v36_initial_assignment_merge_chunk_is_global_root_bound_and_canonical() {
        // Break caught: an intermediate run is incorrectly cell-bound, accepts
        // reordered rows, or is detached from its predecessor assignment root.
        let training_spec = V36SupercellTrainingSpec {
            corpus_rows: 4,
            dimensions: 192,
            maximum_block_rows: 4,
            projected_corpus_sha256: "1".repeat(64),
            reservoir_rows: 4,
            super_cell_count: 4,
        };
        let model_identity = V36ArtifactIdentity {
            blake3: "2".repeat(64),
            encoded_bytes: 1,
            role: "supercell-model".to_owned(),
            sha256: "3".repeat(64),
            uri: "s3://borsuk-v36-test/geometry/supercells.arrow".to_owned(),
        };
        let predecessor_root_identity = V36ArtifactIdentity {
            blake3: "4".repeat(64),
            encoded_bytes: 1,
            role: V36_EXTERNAL_ASSIGNMENT_ROOT_ROLE.to_owned(),
            sha256: "5".repeat(64),
            uri: "s3://borsuk-v36-test/geometry/assignments/assignment-root.json".to_owned(),
        };
        let plan = V36InitialAssignmentMergeGeneration {
            generation: V36ExternalMergeGenerationProjection {
                generation_ordinal: 0,
                input_run_count: 2,
                output_run_count: 1,
                full_group_count: 1,
                tail_group_size: 0,
            },
            groups: vec![V36InitialAssignmentMergeGroup {
                group_ordinal: 0,
                input_range: 0..2,
                output_chunk_count: 1,
                output_root_uri:
                    "s3://borsuk-v36-test/geometry/assignments/merge/generation-000000/run-000000/root.json"
                        .to_owned(),
                output_row_count: 2,
            }],
            model_identity,
            predecessor_root_identity: predecessor_root_identity.clone(),
            training_spec,
        };
        let mut first = [0.0_f32; 192];
        first[0] = 1.0;
        let mut second = [0.0_f32; 192];
        second[1] = 1.0;
        let rows = vec![
            V36SupercellAssignmentRow::new(0, 3, first).unwrap(),
            V36SupercellAssignmentRow::new(1, 0, second).unwrap(),
        ];

        let (bytes, artifact) =
            encode_v36_initial_assignment_merge_chunk_arrow(&plan, 0, 0, &rows).unwrap();
        assert_eq!(
            artifact.context.predecessor_root_identity,
            predecessor_root_identity
        );
        assert_eq!(artifact.context.generation_ordinal, 0);
        assert_eq!(artifact.context.group_ordinal, 0);
        assert_eq!(artifact.context.chunk_ordinal, 0);
        assert_eq!(artifact.row_count, 2);
        assert_eq!(artifact.first_key, (0, 3));
        assert_eq!(artifact.last_key, (1, 0));
        assert_eq!(
            artifact.context.uri,
            "s3://borsuk-v36-test/geometry/assignments/merge/generation-000000/run-000000/chunk-000000.arrow"
        );
        assert_eq!(
            decode_v36_initial_assignment_merge_chunk_arrow(&plan, &bytes, &artifact).unwrap(),
            rows
        );

        let mut reordered = rows.clone();
        reordered.reverse();
        assert!(encode_v36_initial_assignment_merge_chunk_arrow(&plan, 0, 0, &reordered).is_err());
        assert!(encode_v36_initial_assignment_merge_chunk_arrow(&plan, 0, 1, &rows).is_err());
        assert!(encode_v36_initial_assignment_merge_chunk_arrow(&plan, 0, 0, &rows[..1]).is_err());
        let duplicate_source = vec![
            V36SupercellAssignmentRow::new(0, 3, first).unwrap(),
            V36SupercellAssignmentRow::new(1, 3, second).unwrap(),
        ];
        assert!(
            encode_v36_initial_assignment_merge_chunk_arrow(&plan, 0, 0, &duplicate_source,)
                .is_err()
        );
        let mut detached = artifact;
        detached.context.predecessor_root_identity.sha256 = "6".repeat(64);
        assert!(decode_v36_initial_assignment_merge_chunk_arrow(&plan, &bytes, &detached).is_err());
    }

    #[derive(Default)]
    struct InitialMergeSink {
        aborted: bool,
        chunks: Vec<(Vec<u8>, V36InitialAssignmentMergeChunkArtifact)>,
        committed_root: Option<(Vec<u8>, V36ArtifactIdentity)>,
        events: Vec<&'static str>,
        fail_write: bool,
    }

    impl V36InitialAssignmentMergeRunSink for InitialMergeSink {
        fn write_chunk_provisional(
            &mut self,
            bytes: &[u8],
            artifact: &V36InitialAssignmentMergeChunkArtifact,
        ) -> Result<()> {
            self.events.push("chunk");
            if self.fail_write {
                return Err(invalid("injected merge write failure"));
            }
            self.chunks.push((bytes.to_vec(), artifact.clone()));
            Ok(())
        }

        fn commit_root(
            &mut self,
            chunks: &[V36InitialAssignmentMergeChunkArtifact],
            root_bytes: &[u8],
            root_identity: &V36ArtifactIdentity,
        ) -> Result<()> {
            assert_eq!(
                chunks,
                self.chunks
                    .iter()
                    .map(|(_, artifact)| artifact.clone())
                    .collect::<Vec<_>>()
            );
            self.events.push("root");
            self.committed_root = Some((root_bytes.to_vec(), root_identity.clone()));
            Ok(())
        }

        fn abort(&mut self) -> Result<()> {
            self.events.push("abort");
            self.aborted = true;
            self.chunks.clear();
            self.committed_root = None;
            Ok(())
        }
    }

    #[derive(Default)]
    struct InitialMergeGenerationSink {
        committed_root: Option<(Vec<u8>, V36ArtifactIdentity)>,
        run_count: usize,
    }

    impl V36InitialAssignmentMergeGenerationSink for InitialMergeGenerationSink {
        fn commit_root(
            &mut self,
            runs: &[super::V36CommittedInitialAssignmentMergeRun],
            root_bytes: &[u8],
            root_identity: &V36ArtifactIdentity,
        ) -> Result<()> {
            self.run_count = runs.len();
            self.committed_root = Some((root_bytes.to_vec(), root_identity.clone()));
            Ok(())
        }
    }

    #[test]
    fn v36_initial_merge_generation_root_binds_complete_run_inventory() {
        let training_spec = V36SupercellTrainingSpec {
            corpus_rows: 4 * 65_536 + 1,
            dimensions: 192,
            maximum_block_rows: 1,
            projected_corpus_sha256: "1".repeat(64),
            reservoir_rows: 1,
            super_cell_count: 4,
        };
        let model_identity = V36ArtifactIdentity {
            blake3: "2".repeat(64),
            encoded_bytes: 1,
            role: "supercell-model".to_owned(),
            sha256: "3".repeat(64),
            uri: "s3://borsuk-v36-test/geometry/supercells.arrow".to_owned(),
        };
        let admission = V36AdmittedSupercellAssignmentPreflight {
            model_identity: model_identity.clone(),
            training_spec: training_spec.clone(),
            request: V36SupercellAssignmentAdmissionRequest {
                worker_count: 1,
                queue_rows_per_worker: 1,
                sort_rows_per_worker: 1,
                merge_fan_in: 2,
                measured_component_terms: 1,
                measured_elapsed_ns: 1,
                measured_cost_microusd: 1,
                measured_external_work_units: 1,
                measured_external_elapsed_ns: 1,
                measured_external_cost_microusd: 1,
                maximum_active_wall_seconds: 1,
                maximum_cost_microusd: 1,
                maximum_peak_live_bytes: 1,
                maximum_scratch_bytes: 1,
            },
            projection: V36SupercellAssignmentProjection {
                logical_shards: 5,
                merge_generations: 3,
                merge_fan_in: 2,
                uncompressed_assignment_bytes: 1,
                required_scratch_bytes: 1,
                required_peak_live_bytes: 1,
                coverage_bitmap_bytes: 1,
                component_terms: 1,
                external_work_units: 1,
                publication_row_visits: 1,
                projected_active_ns: 1,
                projected_cost_microusd: 1,
            },
        };
        let artifacts = (0..5_u64)
            .map(|shard_ordinal| V36SupercellAssignmentShardArtifact {
                blake3: format!("{:064x}", shard_ordinal + 10),
                context: V36SupercellAssignmentShardContext {
                    model_identity: model_identity.clone(),
                    projected_corpus_sha256: training_spec.projected_corpus_sha256.clone(),
                    shard_ordinal,
                    training_spec: training_spec.clone(),
                    uri: format!(
                        "s3://borsuk-v36-test/geometry/assignments/shard-{shard_ordinal:06}.arrow"
                    ),
                },
                encoded_bytes: 1,
                row_count: if shard_ordinal == 4 { 1 } else { 65_536 },
                sha256: format!("{:064x}", shard_ordinal + 20),
            })
            .collect::<Vec<_>>();
        let uri_prefix = "s3://borsuk-v36-test/geometry/assignments";
        let (_, root_identity) =
            v36_committed_assignment_root(&admission, &artifacts, uri_prefix).unwrap();
        let committed = V36CommittedSupercellAssignments {
            admission,
            artifacts,
            root_identity,
            uri_prefix: uri_prefix.to_owned(),
        };
        let plan = plan_v36_initial_assignment_merge_generation(&committed)
            .unwrap()
            .unwrap();
        let runs = plan
            .groups()
            .iter()
            .map(|group| {
                let chunks = (0..group.output_chunk_count())
                    .map(|chunk_ordinal| {
                        let (context, row_count) =
                            super::v36_initial_assignment_merge_chunk_context(
                                &plan,
                                group.group_ordinal(),
                                chunk_ordinal,
                            )
                            .unwrap();
                        V36InitialAssignmentMergeChunkArtifact {
                            blake3: format!(
                                "{:064x}",
                                100 + group.group_ordinal() * 10 + chunk_ordinal
                            ),
                            context,
                            encoded_bytes: 1,
                            first_key: (0, chunk_ordinal * 65_536),
                            last_key: (0, chunk_ordinal * 65_536 + u64::from(row_count) - 1),
                            row_count,
                            sha256: format!(
                                "{:064x}",
                                200 + group.group_ordinal() * 10 + chunk_ordinal
                            ),
                        }
                    })
                    .collect();
                super::V36CommittedInitialAssignmentMergeRun {
                    chunks,
                    root_identity: V36ArtifactIdentity {
                        blake3: format!("{:064x}", 300 + group.group_ordinal()),
                        encoded_bytes: 1,
                        role: super::V36_INITIAL_ASSIGNMENT_MERGE_ROOT_ROLE.to_owned(),
                        sha256: format!("{:064x}", 400 + group.group_ordinal()),
                        uri: group.output_root_uri().to_owned(),
                    },
                }
            })
            .collect::<Vec<_>>();
        let mut sink = InitialMergeGenerationSink::default();
        let sealed =
            commit_v36_initial_assignment_merge_generation(&committed, runs.clone(), &mut sink)
                .unwrap();
        assert_eq!(sink.run_count, 3);
        assert_eq!(sealed.runs().len(), 3);
        assert_eq!(sealed.generation().generation_ordinal, 0);
        assert_eq!(
            sealed.predecessor_root_identity(),
            committed.root_identity()
        );
        let (generation_root_bytes, generation_root_identity) = sink.committed_root.unwrap();
        assert_eq!(sealed.root_identity(), &generation_root_identity);
        let resumed = authenticate_v36_assignment_merge_generation_root(
            &committed,
            None,
            runs.clone(),
            &generation_root_bytes,
            &generation_root_identity,
        )
        .unwrap();
        assert_eq!(resumed.generation(), sealed.generation());
        assert_eq!(resumed.runs(), sealed.runs());
        assert_eq!(resumed.root_identity(), sealed.root_identity());
        let mut corrupt_generation_root = generation_root_bytes.clone();
        corrupt_generation_root[0] ^= 1;
        assert!(
            authenticate_v36_assignment_merge_generation_root(
                &committed,
                None,
                runs.clone(),
                &corrupt_generation_root,
                &generation_root_identity,
            )
            .is_err()
        );
        let generation_root: serde_json::Value =
            serde_json::from_slice(&generation_root_bytes).unwrap();
        assert_eq!(generation_root["run_count"], 3);
        assert_eq!(generation_root["merge_fan_in"], 2);
        assert!(generation_root.get("runs").is_none());
        assert_eq!(
            generation_root["run_inventory_sha256"]
                .as_str()
                .unwrap()
                .len(),
            64
        );
        assert_eq!(
            generation_root["run_inventory_blake3"]
                .as_str()
                .unwrap()
                .len(),
            64
        );

        let mut reordered = runs.clone();
        reordered.swap(0, 1);
        assert!(
            commit_v36_initial_assignment_merge_generation(
                &committed,
                reordered,
                &mut InitialMergeGenerationSink::default(),
            )
            .is_err()
        );
        assert!(
            commit_v36_initial_assignment_merge_generation(
                &committed,
                runs[..2].to_vec(),
                &mut InitialMergeGenerationSink::default(),
            )
            .is_err()
        );

        let followup = plan_v36_followup_assignment_merge_generation(&committed, sealed)
            .unwrap()
            .unwrap();
        assert_eq!(
            followup.generation(),
            V36ExternalMergeGenerationProjection {
                generation_ordinal: 1,
                input_run_count: 3,
                output_run_count: 2,
                full_group_count: 1,
                tail_group_size: 1,
            }
        );
        assert_eq!(
            followup.predecessor_root_identity(),
            &generation_root_identity
        );
        assert_eq!(
            followup
                .groups()
                .iter()
                .map(|group| (
                    group.group_ordinal(),
                    group.input_range(),
                    group.output_row_count(),
                    group.output_chunk_count(),
                    group.output_root_uri(),
                ))
                .collect::<Vec<_>>(),
            [
                (
                    0,
                    0..2,
                    4 * 65_536,
                    4,
                    "s3://borsuk-v36-test/geometry/assignments/merge/generation-000001/run-000000/root.json",
                ),
                (
                    1,
                    2..3,
                    1,
                    1,
                    "s3://borsuk-v36-test/geometry/assignments/merge/generation-000001/run-000001/root.json",
                ),
            ]
        );

        let followup_runs = followup
            .groups()
            .iter()
            .map(|group| {
                let chunks = (0..group.output_chunk_count())
                    .map(|chunk_ordinal| {
                        let (context, row_count) =
                            super::v36_followup_assignment_merge_chunk_context(
                                &followup,
                                group.group_ordinal(),
                                chunk_ordinal,
                            )
                            .unwrap();
                        V36InitialAssignmentMergeChunkArtifact {
                            blake3: format!(
                                "{:064x}",
                                500 + group.group_ordinal() * 10 + chunk_ordinal
                            ),
                            context,
                            encoded_bytes: 1,
                            first_key: (0, chunk_ordinal * 65_536),
                            last_key: (0, chunk_ordinal * 65_536 + u64::from(row_count) - 1),
                            row_count,
                            sha256: format!(
                                "{:064x}",
                                600 + group.group_ordinal() * 10 + chunk_ordinal
                            ),
                        }
                    })
                    .collect();
                super::V36CommittedInitialAssignmentMergeRun {
                    chunks,
                    root_identity: V36ArtifactIdentity {
                        blake3: format!("{:064x}", 700 + group.group_ordinal()),
                        encoded_bytes: 1,
                        role: super::V36_FOLLOWUP_ASSIGNMENT_MERGE_ROOT_ROLE.to_owned(),
                        sha256: format!("{:064x}", 800 + group.group_ordinal()),
                        uri: group.output_root_uri().to_owned(),
                    },
                }
            })
            .collect::<Vec<_>>();
        let mut followup_sink = InitialMergeGenerationSink::default();
        let sealed_followup = commit_v36_followup_assignment_merge_generation(
            &committed,
            followup,
            followup_runs.clone(),
            &mut followup_sink,
        )
        .unwrap();
        assert_eq!(sealed_followup.generation().generation_ordinal, 1);
        assert_eq!(sealed_followup.runs().len(), 2);
        let (followup_root_bytes, followup_root_identity) =
            followup_sink.committed_root.as_ref().unwrap();
        let resumed_followup = authenticate_v36_assignment_merge_generation_root(
            &committed,
            Some(&resumed),
            followup_runs,
            followup_root_bytes,
            followup_root_identity,
        )
        .unwrap();
        assert_eq!(resumed_followup.generation(), sealed_followup.generation());
        assert_eq!(resumed_followup.runs(), sealed_followup.runs());
        assert_eq!(
            resumed_followup.root_identity(),
            sealed_followup.root_identity()
        );
        let terminal = plan_v36_followup_assignment_merge_generation(&committed, resumed_followup)
            .unwrap()
            .unwrap();
        assert_eq!(terminal.generation().generation_ordinal, 2);
        assert_eq!(terminal.generation().input_run_count, 2);
        assert_eq!(terminal.generation().output_run_count, 1);
        assert_eq!(terminal.groups().len(), 1);
        assert_eq!(terminal.groups()[0].output_row_count(), 4 * 65_536 + 1);

        let terminal_runs = terminal
            .groups()
            .iter()
            .map(|group| {
                let chunks = (0..group.output_chunk_count())
                    .map(|chunk_ordinal| {
                        let (context, row_count) =
                            super::v36_followup_assignment_merge_chunk_context(
                                &terminal,
                                group.group_ordinal(),
                                chunk_ordinal,
                            )
                            .unwrap();
                        V36InitialAssignmentMergeChunkArtifact {
                            blake3: format!("{:064x}", 900 + chunk_ordinal),
                            context,
                            encoded_bytes: 1,
                            first_key: (0, chunk_ordinal * 65_536),
                            last_key: (0, chunk_ordinal * 65_536 + u64::from(row_count) - 1),
                            row_count,
                            sha256: format!("{:064x}", 1_000 + chunk_ordinal),
                        }
                    })
                    .collect();
                super::V36CommittedInitialAssignmentMergeRun {
                    chunks,
                    root_identity: V36ArtifactIdentity {
                        blake3: "c".repeat(64),
                        encoded_bytes: 1,
                        role: super::V36_FOLLOWUP_ASSIGNMENT_MERGE_ROOT_ROLE.to_owned(),
                        sha256: "d".repeat(64),
                        uri: group.output_root_uri().to_owned(),
                    },
                }
            })
            .collect();
        let mut mismatched = committed.clone();
        mismatched.admission.request.merge_fan_in = 3;
        mismatched.admission.projection.merge_fan_in = 3;
        let mut mismatched_sink = InitialMergeGenerationSink::default();
        assert!(
            commit_v36_followup_assignment_merge_generation(
                &mismatched,
                terminal,
                terminal_runs,
                &mut mismatched_sink,
            )
            .is_err()
        );
        assert!(mismatched_sink.committed_root.is_none());
    }

    struct FollowupChunkSource {
        bytes: std::collections::HashMap<String, Vec<u8>>,
        calls: Vec<String>,
    }

    impl V36AssignmentMergeChunkSource for FollowupChunkSource {
        fn read_chunk(
            &mut self,
            _run_root: &V36ArtifactIdentity,
            artifact: &V36InitialAssignmentMergeChunkArtifact,
        ) -> Result<Vec<u8>> {
            self.calls.push(artifact.context.uri.clone());
            self.bytes
                .get(&artifact.context.uri)
                .cloned()
                .ok_or_else(|| invalid("test merge chunk is missing"))
        }
    }

    #[derive(Default)]
    struct FollowupRunSink {
        committed_root: Option<(Vec<u8>, V36ArtifactIdentity)>,
        events: Vec<&'static str>,
        fail_chunk: bool,
    }

    impl V36FollowupAssignmentMergeRunSink for FollowupRunSink {
        fn write_chunk_provisional(
            &mut self,
            _bytes: &[u8],
            _artifact: &V36InitialAssignmentMergeChunkArtifact,
        ) -> Result<()> {
            self.events.push("chunk");
            if self.fail_chunk {
                return Err(invalid("test followup chunk write failed"));
            }
            Ok(())
        }

        fn commit_root(
            &mut self,
            _chunks: &[V36InitialAssignmentMergeChunkArtifact],
            root_bytes: &[u8],
            root_identity: &V36ArtifactIdentity,
        ) -> Result<()> {
            self.events.push("root");
            self.committed_root = Some((root_bytes.to_vec(), root_identity.clone()));
            Ok(())
        }

        fn abort(&mut self) -> Result<()> {
            self.events.push("abort");
            Ok(())
        }
    }

    fn followup_test_chunk(
        context: super::V36InitialAssignmentMergeChunkContext,
        rows: &[V36SupercellAssignmentRow],
    ) -> (Vec<u8>, V36InitialAssignmentMergeChunkArtifact) {
        let row_count = u32::try_from(rows.len()).unwrap();
        let first_key = rows
            .first()
            .map(|row| (row.supercell_ordinal, row.source_ordinal))
            .unwrap();
        let last_key = rows
            .last()
            .map(|row| (row.supercell_ordinal, row.source_ordinal))
            .unwrap();
        let manifest = super::v36_initial_assignment_merge_chunk_manifest(
            &context, row_count, first_key, last_key,
        )
        .unwrap();
        let schema = std::sync::Arc::new(super::v36_assignment_schema(
            super::V36_INITIAL_ASSIGNMENT_MERGE_CHUNK_MANIFEST_KEY,
            manifest,
        ));
        let bytes = super::encode_v36_assignment_rows_arrow(schema, rows).unwrap();
        let artifact = V36InitialAssignmentMergeChunkArtifact {
            blake3: blake3::hash(&bytes).to_hex().to_string(),
            context,
            encoded_bytes: u64::try_from(bytes.len()).unwrap(),
            first_key,
            last_key,
            row_count,
            sha256: format!("{:x}", Sha256::digest(&bytes)),
        };
        (bytes, artifact)
    }

    #[test]
    fn v36_followup_merge_streams_one_authenticated_chunk_per_run() {
        let training_spec = V36SupercellTrainingSpec {
            corpus_rows: 4,
            dimensions: 192,
            maximum_block_rows: 4,
            projected_corpus_sha256: "1".repeat(64),
            reservoir_rows: 4,
            super_cell_count: 2,
        };
        let model_identity = V36ArtifactIdentity {
            blake3: "2".repeat(64),
            encoded_bytes: 1,
            role: "supercell-model".to_owned(),
            sha256: "3".repeat(64),
            uri: "s3://borsuk-v36-test/geometry/supercells.arrow".to_owned(),
        };
        let assignment_root = V36ArtifactIdentity {
            blake3: "4".repeat(64),
            encoded_bytes: 1,
            role: V36_EXTERNAL_ASSIGNMENT_ROOT_ROLE.to_owned(),
            sha256: "5".repeat(64),
            uri: "s3://borsuk-v36-test/geometry/assignments/assignment-root.json".to_owned(),
        };
        let initial_plan = V36InitialAssignmentMergeGeneration {
            generation: V36ExternalMergeGenerationProjection {
                generation_ordinal: 0,
                input_run_count: 4,
                output_run_count: 2,
                full_group_count: 2,
                tail_group_size: 0,
            },
            groups: (0..2_u64)
                .map(|group_ordinal| V36InitialAssignmentMergeGroup {
                    group_ordinal,
                    input_range: usize::try_from(group_ordinal * 2).unwrap()
                        ..usize::try_from(group_ordinal * 2 + 2).unwrap(),
                    output_chunk_count: 1,
                    output_root_uri: format!(
                        "s3://borsuk-v36-test/geometry/assignments/merge/generation-000000/run-{group_ordinal:06}/root.json"
                    ),
                    output_row_count: 2,
                })
                .collect(),
            model_identity: model_identity.clone(),
            predecessor_root_identity: assignment_root.clone(),
            training_spec: training_spec.clone(),
        };
        let mut first = [0.0_f32; 192];
        first[0] = 1.0;
        let mut second = [0.0_f32; 192];
        second[0] = 2.0;
        let run_rows = [
            vec![
                V36SupercellAssignmentRow::new(0, 0, first).unwrap(),
                V36SupercellAssignmentRow::new(1, 2, second).unwrap(),
            ],
            vec![
                V36SupercellAssignmentRow::new(0, 1, first).unwrap(),
                V36SupercellAssignmentRow::new(1, 3, second).unwrap(),
            ],
        ];
        let mut chunk_bytes = std::collections::HashMap::new();
        let runs = run_rows
            .iter()
            .enumerate()
            .map(|(group_ordinal, rows)| {
                let group_ordinal = u64::try_from(group_ordinal).unwrap();
                let (bytes, artifact) = encode_v36_initial_assignment_merge_chunk_arrow(
                    &initial_plan,
                    group_ordinal,
                    0,
                    rows,
                )
                .unwrap();
                chunk_bytes.insert(artifact.context.uri.clone(), bytes);
                super::V36CommittedInitialAssignmentMergeRun {
                    chunks: vec![artifact],
                    root_identity: V36ArtifactIdentity {
                        blake3: format!("{:064x}", 10 + group_ordinal),
                        encoded_bytes: 1,
                        role: super::V36_INITIAL_ASSIGNMENT_MERGE_ROOT_ROLE.to_owned(),
                        sha256: format!("{:064x}", 20 + group_ordinal),
                        uri: initial_plan.groups[usize::try_from(group_ordinal).unwrap()]
                            .output_root_uri
                            .clone(),
                    },
                }
            })
            .collect();
        let predecessor = super::V36CommittedInitialAssignmentMergeGeneration {
            generation: initial_plan.generation,
            merge_fan_in: 2,
            predecessor_root_identity: assignment_root,
            root_identity: V36ArtifactIdentity {
                blake3: "6".repeat(64),
                encoded_bytes: 1,
                role: super::V36_INITIAL_ASSIGNMENT_MERGE_GENERATION_ROOT_ROLE.to_owned(),
                sha256: "7".repeat(64),
                uri: "s3://borsuk-v36-test/geometry/assignments/merge/generation-000000/root.json"
                    .to_owned(),
            },
            runs,
        };
        let followup = super::V36FollowupAssignmentMergeGeneration {
            generation: V36ExternalMergeGenerationProjection {
                generation_ordinal: 1,
                input_run_count: 2,
                output_run_count: 1,
                full_group_count: 1,
                tail_group_size: 0,
            },
            groups: vec![V36InitialAssignmentMergeGroup {
                group_ordinal: 0,
                input_range: 0..2,
                output_chunk_count: 1,
                output_root_uri:
                    "s3://borsuk-v36-test/geometry/assignments/merge/generation-000001/run-000000/root.json"
                        .to_owned(),
                output_row_count: 4,
            }],
            predecessor,
        };
        let mut source = FollowupChunkSource {
            bytes: chunk_bytes.clone(),
            calls: Vec::new(),
        };
        let mut output = Vec::new();
        stream_v36_followup_assignment_merge_group(&followup, 0, &mut source, &mut |rows| {
            output.extend_from_slice(rows);
            Ok(())
        })
        .unwrap();
        assert_eq!(source.calls.len(), 2);
        assert_eq!(
            output
                .iter()
                .map(|row| (row.supercell_ordinal, row.source_ordinal))
                .collect::<Vec<_>>(),
            [(0, 0), (0, 1), (1, 2), (1, 3)]
        );

        let writer_bytes = chunk_bytes.clone();
        let mut writer_source = FollowupChunkSource {
            bytes: writer_bytes.clone(),
            calls: Vec::new(),
        };
        let mut writer_sink = FollowupRunSink::default();
        let committed = write_v36_followup_assignment_merge_group(
            &followup,
            0,
            &mut writer_source,
            &mut writer_sink,
        )
        .unwrap();
        assert_eq!(writer_source.calls.len(), 2);
        assert_eq!(writer_sink.events, ["chunk", "root"]);
        assert_eq!(committed.chunks().len(), 1);
        assert_eq!(committed.chunks()[0].row_count, 4);
        let (root_bytes, root_identity) = writer_sink.committed_root.unwrap();
        let resumed = authenticate_v36_followup_assignment_merge_run_root(
            &followup,
            0,
            &root_bytes,
            &root_identity,
        )
        .unwrap();
        assert_eq!(resumed.chunks(), committed.chunks());
        assert_eq!(resumed.root_identity(), committed.root_identity());
        let mut corrupt_root = root_bytes;
        corrupt_root[0] ^= 1;
        assert!(
            authenticate_v36_followup_assignment_merge_run_root(
                &followup,
                0,
                &corrupt_root,
                &root_identity,
            )
            .is_err()
        );

        let mut failing_source = FollowupChunkSource {
            bytes: writer_bytes,
            calls: Vec::new(),
        };
        let mut failing_sink = FollowupRunSink {
            fail_chunk: true,
            ..FollowupRunSink::default()
        };
        assert!(
            write_v36_followup_assignment_merge_group(
                &followup,
                0,
                &mut failing_source,
                &mut failing_sink,
            )
            .is_err()
        );
        assert_eq!(failing_sink.events, ["chunk", "abort"]);

        let corrupt_uri = followup.predecessor.runs[1].chunks[0].context.uri.clone();
        chunk_bytes.get_mut(&corrupt_uri).unwrap()[0] ^= 1;
        let mut corrupt_source = FollowupChunkSource {
            bytes: chunk_bytes,
            calls: Vec::new(),
        };
        assert!(
            stream_v36_followup_assignment_merge_group(
                &followup,
                0,
                &mut corrupt_source,
                &mut |_| Ok(()),
            )
            .is_err()
        );

        let base_context = followup.predecessor.runs[0].chunks[0].context.clone();
        let rollover_rows = [
            vec![
                V36SupercellAssignmentRow::new(0, 0, first).unwrap(),
                V36SupercellAssignmentRow::new(0, 1, first).unwrap(),
            ],
            vec![
                V36SupercellAssignmentRow::new(1, 2, second).unwrap(),
                V36SupercellAssignmentRow::new(1, 3, second).unwrap(),
            ],
        ];
        let rollover_chunks = rollover_rows
            .iter()
            .enumerate()
            .map(|(chunk_ordinal, rows)| {
                let mut context = base_context.clone();
                context.chunk_ordinal = u64::try_from(chunk_ordinal).unwrap();
                context.uri = format!(
                    "s3://borsuk-v36-test/geometry/assignments/merge/generation-000000/run-000000/chunk-{chunk_ordinal:06}.arrow"
                );
                followup_test_chunk(context, rows)
            })
            .collect::<Vec<_>>();
        let rollover_bytes = rollover_chunks
            .iter()
            .map(|(bytes, artifact)| (artifact.context.uri.clone(), bytes.clone()))
            .collect();
        let rollover_plan = super::V36FollowupAssignmentMergeGeneration {
            generation: V36ExternalMergeGenerationProjection {
                generation_ordinal: 1,
                input_run_count: 1,
                output_run_count: 1,
                full_group_count: 0,
                tail_group_size: 1,
            },
            groups: vec![V36InitialAssignmentMergeGroup {
                group_ordinal: 0,
                input_range: 0..1,
                output_chunk_count: 1,
                output_root_uri:
                    "s3://borsuk-v36-test/geometry/assignments/merge/generation-000001/run-000000/root.json"
                        .to_owned(),
                output_row_count: 4,
            }],
            predecessor: super::V36CommittedInitialAssignmentMergeGeneration {
                generation: initial_plan.generation,
                merge_fan_in: 2,
                predecessor_root_identity: initial_plan.predecessor_root_identity.clone(),
                root_identity: followup.predecessor.root_identity.clone(),
                runs: vec![super::V36CommittedInitialAssignmentMergeRun {
                    chunks: rollover_chunks
                        .iter()
                        .map(|(_, artifact)| artifact.clone())
                        .collect(),
                    root_identity: followup.predecessor.runs[0].root_identity.clone(),
                }],
            },
        };
        let mut rollover_source = FollowupChunkSource {
            bytes: rollover_bytes,
            calls: Vec::new(),
        };
        let mut blocks = Vec::new();
        super::stream_v36_followup_assignment_merge_group_with_output_capacity(
            &rollover_plan,
            0,
            &mut rollover_source,
            2,
            &mut |rows| {
                blocks.push(
                    rows.iter()
                        .map(|row| (row.supercell_ordinal, row.source_ordinal))
                        .collect::<Vec<_>>(),
                );
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(rollover_source.calls.len(), 2);
        assert_eq!(blocks, [vec![(0, 0), (0, 1)], vec![(1, 2), (1, 3)]]);

        let mut visitor_error_source = FollowupChunkSource {
            bytes: rollover_chunks
                .iter()
                .map(|(bytes, artifact)| (artifact.context.uri.clone(), bytes.clone()))
                .collect(),
            calls: Vec::new(),
        };
        assert!(
            super::stream_v36_followup_assignment_merge_group_with_output_capacity(
                &rollover_plan,
                0,
                &mut visitor_error_source,
                1,
                &mut |_| Err(invalid("test visitor failed")),
            )
            .is_err()
        );
        assert_eq!(visitor_error_source.calls.len(), 1);
    }

    #[test]
    fn v36_followup_merge_root_is_compact_at_the_100m_chunk_frontier() {
        let corpus_rows = 100_000_000_u64;
        let chunk_count = corpus_rows.div_ceil(super::V36_EXTERNAL_ASSIGNMENT_SHARD_ROWS);
        assert_eq!(chunk_count, 1_526);
        let long_component = "x".repeat(3_900);
        let model_identity = V36ArtifactIdentity {
            blake3: "1".repeat(64),
            encoded_bytes: 1,
            role: "supercell-model".to_owned(),
            sha256: "2".repeat(64),
            uri: format!("s3://borsuk-v36-test/{long_component}/model.arrow"),
        };
        let training_spec = V36SupercellTrainingSpec {
            corpus_rows,
            dimensions: 192,
            maximum_block_rows: 65_536,
            projected_corpus_sha256: "3".repeat(64),
            reservoir_rows: 1_048_576,
            super_cell_count: 512,
        };
        let predecessor_root = V36ArtifactIdentity {
            blake3: "4".repeat(64),
            encoded_bytes: 1,
            role: super::V36_INITIAL_ASSIGNMENT_MERGE_GENERATION_ROOT_ROLE.to_owned(),
            sha256: "5".repeat(64),
            uri: format!("s3://borsuk-v36-test/{long_component}/predecessor.json"),
        };
        let predecessor_chunk = V36InitialAssignmentMergeChunkArtifact {
            blake3: "6".repeat(64),
            context: super::V36InitialAssignmentMergeChunkContext {
                chunk_ordinal: 0,
                generation_ordinal: 0,
                group_ordinal: 0,
                model_identity: model_identity.clone(),
                predecessor_root_identity: predecessor_root.clone(),
                training_spec: training_spec.clone(),
                uri: format!("s3://borsuk-v36-test/{long_component}/input.arrow"),
            },
            encoded_bytes: 1,
            first_key: (0, 0),
            last_key: (511, corpus_rows - 1),
            row_count: u32::MAX,
            sha256: "7".repeat(64),
        };
        let plan = super::V36FollowupAssignmentMergeGeneration {
            generation: V36ExternalMergeGenerationProjection {
                generation_ordinal: 1,
                input_run_count: 1,
                output_run_count: 1,
                full_group_count: 0,
                tail_group_size: 1,
            },
            groups: vec![V36InitialAssignmentMergeGroup {
                group_ordinal: 0,
                input_range: 0..1,
                output_chunk_count: chunk_count,
                output_root_uri: format!(
                    "s3://borsuk-v36-test/{long_component}/generation-000001/run-000000/root.json"
                ),
                output_row_count: corpus_rows,
            }],
            predecessor: super::V36CommittedInitialAssignmentMergeGeneration {
                generation: V36ExternalMergeGenerationProjection {
                    generation_ordinal: 0,
                    input_run_count: 2,
                    output_run_count: 1,
                    full_group_count: 0,
                    tail_group_size: 2,
                },
                merge_fan_in: 64,
                predecessor_root_identity: predecessor_root,
                root_identity: V36ArtifactIdentity {
                    blake3: "8".repeat(64),
                    encoded_bytes: 1,
                    role: super::V36_INITIAL_ASSIGNMENT_MERGE_GENERATION_ROOT_ROLE.to_owned(),
                    sha256: "9".repeat(64),
                    uri: format!(
                        "s3://borsuk-v36-test/{long_component}/generation-000000/root.json"
                    ),
                },
                runs: vec![super::V36CommittedInitialAssignmentMergeRun {
                    chunks: vec![predecessor_chunk],
                    root_identity: V36ArtifactIdentity {
                        blake3: "a".repeat(64),
                        encoded_bytes: 1,
                        role: super::V36_INITIAL_ASSIGNMENT_MERGE_ROOT_ROLE.to_owned(),
                        sha256: "b".repeat(64),
                        uri: format!("s3://borsuk-v36-test/{long_component}/input-root.json"),
                    },
                }],
            },
        };
        let mut chunks = Vec::with_capacity(usize::try_from(chunk_count).unwrap());
        for chunk_ordinal in 0..chunk_count {
            let (context, row_count) =
                super::v36_followup_assignment_merge_chunk_context(&plan, 0, chunk_ordinal)
                    .unwrap();
            let first_source = chunk_ordinal * super::V36_EXTERNAL_ASSIGNMENT_SHARD_ROWS;
            let last_source = first_source + u64::from(row_count) - 1;
            chunks.push(V36InitialAssignmentMergeChunkArtifact {
                blake3: format!("{chunk_ordinal:064x}"),
                context,
                encoded_bytes: 1,
                first_key: (0, first_source),
                last_key: (0, last_source),
                row_count,
                sha256: format!("{:064x}", chunk_ordinal + 1),
            });
        }
        let (bytes, _) = super::v36_followup_assignment_merge_root(&plan, 0, &chunks).unwrap();
        assert!(bytes.len() < 1_048_576);
    }

    #[test]
    fn v36_initial_assignment_merge_writer_is_transactional_and_root_last() {
        // Break caught: globally merged chunks publish before complete unique
        // source coverage or a failed transaction leaves provisional output.
        let training_spec = V36SupercellTrainingSpec {
            corpus_rows: 4,
            dimensions: 192,
            maximum_block_rows: 4,
            projected_corpus_sha256: "1".repeat(64),
            reservoir_rows: 4,
            super_cell_count: 2,
        };
        let model_identity = V36ArtifactIdentity {
            blake3: "2".repeat(64),
            encoded_bytes: 1,
            role: "supercell-model".to_owned(),
            sha256: "3".repeat(64),
            uri: "s3://borsuk-v36-test/geometry/supercells.arrow".to_owned(),
        };
        let predecessor_root_identity = V36ArtifactIdentity {
            blake3: "4".repeat(64),
            encoded_bytes: 1,
            role: V36_EXTERNAL_ASSIGNMENT_ROOT_ROLE.to_owned(),
            sha256: "5".repeat(64),
            uri: "s3://borsuk-v36-test/geometry/assignments/assignment-root.json".to_owned(),
        };
        let plan = V36InitialAssignmentMergeGeneration {
            generation: V36ExternalMergeGenerationProjection {
                generation_ordinal: 0,
                input_run_count: 2,
                output_run_count: 1,
                full_group_count: 1,
                tail_group_size: 0,
            },
            groups: vec![V36InitialAssignmentMergeGroup {
                group_ordinal: 0,
                input_range: 0..2,
                output_chunk_count: 1,
                output_root_uri:
                    "s3://borsuk-v36-test/geometry/assignments/merge/generation-000000/run-000000/root.json"
                        .to_owned(),
                output_row_count: 4,
            }],
            model_identity: model_identity.clone(),
            predecessor_root_identity,
            training_spec: training_spec.clone(),
        };
        let input_artifacts = (0..2)
            .map(|shard_ordinal| V36SupercellAssignmentShardArtifact {
                blake3: format!("{:064x}", shard_ordinal + 10),
                context: V36SupercellAssignmentShardContext {
                    model_identity: model_identity.clone(),
                    projected_corpus_sha256: training_spec.projected_corpus_sha256.clone(),
                    shard_ordinal,
                    training_spec: training_spec.clone(),
                    uri: format!(
                        "s3://borsuk-v36-test/geometry/assignments/input-{shard_ordinal}.arrow"
                    ),
                },
                encoded_bytes: 1,
                row_count: 2,
                sha256: format!("{:064x}", shard_ordinal + 20),
            })
            .collect::<Vec<_>>();
        let mut vector = [0.0_f32; 192];
        vector[0] = 1.0;
        let inputs = vec![
            vec![
                V36SupercellAssignmentRow::new(0, 0, vector).unwrap(),
                V36SupercellAssignmentRow::new(1, 1, vector).unwrap(),
            ],
            vec![
                V36SupercellAssignmentRow::new(0, 2, vector).unwrap(),
                V36SupercellAssignmentRow::new(1, 3, vector).unwrap(),
            ],
        ];
        let authenticated = V36AuthenticatedInitialAssignmentMergeGroup {
            group_ordinal: 0,
            input_artifacts,
            inputs,
            plan: plan.clone(),
        };
        let mut sink = InitialMergeSink::default();
        let committed =
            write_v36_initial_assignment_merge_group(&authenticated, &mut sink).unwrap();
        assert_eq!(sink.events, ["chunk", "root"]);
        assert!(!sink.aborted);
        assert_eq!(
            committed.chunks(),
            sink.chunks
                .iter()
                .map(|(_, a)| a.clone())
                .collect::<Vec<_>>()
        );
        let (root_bytes, root_identity) = sink.committed_root.as_ref().unwrap();
        assert_eq!(root_bytes.last(), Some(&b'\n'));
        assert_eq!(committed.root_identity(), root_identity);
        assert_eq!(root_identity.uri, plan.groups[0].output_root_uri);
        assert_eq!(
            root_identity.sha256,
            format!("{:x}", Sha256::digest(root_bytes))
        );
        assert_eq!(
            decode_v36_initial_assignment_merge_chunk_arrow(
                &plan,
                &sink.chunks[0].0,
                &sink.chunks[0].1,
            )
            .unwrap()
            .iter()
            .map(|row| (row.supercell_ordinal, row.source_ordinal))
            .collect::<Vec<_>>(),
            [(0, 0), (0, 2), (1, 1), (1, 3)]
        );

        let mut failing_sink = InitialMergeSink {
            fail_write: true,
            ..InitialMergeSink::default()
        };
        assert!(
            write_v36_initial_assignment_merge_group(&authenticated, &mut failing_sink).is_err()
        );
        assert!(failing_sink.aborted);
        assert_eq!(failing_sink.events, ["chunk", "abort"]);
        assert!(failing_sink.chunks.is_empty());
    }

    #[test]
    fn v36_authenticated_assignment_shard_handle_is_byte_bound_and_opaque() {
        // Break caught: the merge loader accepts decoded rows independently
        // from the exact assignment shard bytes and registered identity.
        let training_spec = V36SupercellTrainingSpec {
            corpus_rows: 4,
            dimensions: 192,
            maximum_block_rows: 4,
            projected_corpus_sha256: "1".repeat(64),
            reservoir_rows: 4,
            super_cell_count: 2,
        };
        let model_identity = V36ArtifactIdentity {
            blake3: "2".repeat(64),
            encoded_bytes: 1,
            role: "supercell-model".to_owned(),
            sha256: "3".repeat(64),
            uri: "s3://borsuk-v36-test/geometry/supercells.arrow".to_owned(),
        };
        let context = V36SupercellAssignmentShardContext {
            model_identity,
            projected_corpus_sha256: training_spec.projected_corpus_sha256.clone(),
            shard_ordinal: 0,
            training_spec,
            uri: "s3://borsuk-v36-test/geometry/assignments/shard-000000.arrow".to_owned(),
        };
        let mut vector = [0.0_f32; 192];
        vector[0] = 1.0;
        let rows = vec![
            V36SupercellAssignmentRow::new(0, 0, vector).unwrap(),
            V36SupercellAssignmentRow::new(0, 2, vector).unwrap(),
            V36SupercellAssignmentRow::new(1, 1, vector).unwrap(),
            V36SupercellAssignmentRow::new(1, 3, vector).unwrap(),
        ];
        let (bytes, artifact) =
            encode_v36_supercell_assignment_shard_arrow(&context, &rows).unwrap();
        let authenticated =
            authenticate_v36_supercell_assignment_shard_arrow(&bytes, &artifact).unwrap();
        assert_eq!(authenticated.artifact(), &artifact);
        assert_eq!(authenticated.rows(), rows);

        let mut corrupted = bytes;
        let last = corrupted.len() - 1;
        corrupted[last] ^= 1;
        assert!(authenticate_v36_supercell_assignment_shard_arrow(&corrupted, &artifact).is_err());
        let _: &V36AuthenticatedSupercellAssignmentShard = &authenticated;
    }

    #[test]
    fn v36_initial_merge_loader_binds_committed_inventory_to_authenticated_shards() {
        // Break caught: a merge group can be assembled from a detached plan or
        // from a valid shard whose exact artifact is absent from the committed inventory.
        let training_spec = V36SupercellTrainingSpec {
            corpus_rows: 16 * 65_536 + 1,
            dimensions: 192,
            maximum_block_rows: 65_536,
            projected_corpus_sha256: "1".repeat(64),
            reservoir_rows: 65_536,
            super_cell_count: 4,
        };
        let model_identity = V36ArtifactIdentity {
            blake3: "2".repeat(64),
            encoded_bytes: 1,
            role: "supercell-model".to_owned(),
            sha256: "3".repeat(64),
            uri: "s3://borsuk-v36-test/geometry/supercells.arrow".to_owned(),
        };
        let context = V36SupercellAssignmentShardContext {
            model_identity: model_identity.clone(),
            projected_corpus_sha256: training_spec.projected_corpus_sha256.clone(),
            shard_ordinal: 16,
            training_spec: training_spec.clone(),
            uri: "s3://borsuk-v36-test/geometry/assignments/shard-000016.arrow".to_owned(),
        };
        let mut vector = [0.0_f32; 192];
        vector[0] = 1.0;
        let tail_rows = vec![V36SupercellAssignmentRow::new(3, 16 * 65_536, vector).unwrap()];
        let (tail_bytes, tail_artifact) =
            encode_v36_supercell_assignment_shard_arrow(&context, &tail_rows).unwrap();
        let tail =
            authenticate_v36_supercell_assignment_shard_arrow(&tail_bytes, &tail_artifact).unwrap();
        let mut artifacts = (0..16)
            .map(|shard_ordinal| V36SupercellAssignmentShardArtifact {
                blake3: format!("{shard_ordinal:064x}"),
                context: V36SupercellAssignmentShardContext {
                    model_identity: model_identity.clone(),
                    projected_corpus_sha256: training_spec.projected_corpus_sha256.clone(),
                    shard_ordinal,
                    training_spec: training_spec.clone(),
                    uri: format!(
                        "s3://borsuk-v36-test/geometry/assignments/shard-{shard_ordinal:06}.arrow"
                    ),
                },
                encoded_bytes: 1,
                row_count: 65_536,
                sha256: format!("{:064x}", shard_ordinal + 17),
            })
            .collect::<Vec<_>>();
        artifacts.push(tail_artifact.clone());
        let admission = V36AdmittedSupercellAssignmentPreflight {
            model_identity,
            training_spec,
            request: V36SupercellAssignmentAdmissionRequest {
                worker_count: 1,
                queue_rows_per_worker: 1,
                sort_rows_per_worker: 1,
                merge_fan_in: 8,
                measured_component_terms: 1,
                measured_elapsed_ns: 1,
                measured_cost_microusd: 1,
                measured_external_work_units: 1,
                measured_external_elapsed_ns: 1,
                measured_external_cost_microusd: 1,
                maximum_active_wall_seconds: 1,
                maximum_cost_microusd: 1,
                maximum_peak_live_bytes: 1,
                maximum_scratch_bytes: 1,
            },
            projection: V36SupercellAssignmentProjection {
                logical_shards: 17,
                merge_generations: 2,
                merge_fan_in: 8,
                uncompressed_assignment_bytes: 1,
                required_scratch_bytes: 1,
                required_peak_live_bytes: 1,
                coverage_bitmap_bytes: 1,
                component_terms: 1,
                external_work_units: 1,
                publication_row_visits: 1,
                projected_active_ns: 1,
                projected_cost_microusd: 1,
            },
        };
        let uri_prefix = "s3://borsuk-v36-test/geometry/assignments";
        let (_, root_identity) =
            v36_committed_assignment_root(&admission, &artifacts, uri_prefix).unwrap();
        let committed = V36CommittedSupercellAssignments {
            admission,
            artifacts,
            root_identity,
            uri_prefix: uri_prefix.to_owned(),
        };
        let mut detached = committed.clone();
        detached.root_identity.sha256 = "9".repeat(64);
        let detached_tail =
            authenticate_v36_supercell_assignment_shard_arrow(&tail_bytes, &tail_artifact).unwrap();
        assert!(
            load_v36_initial_assignment_merge_group(&detached, 2, vec![detached_tail]).is_err()
        );

        let mut unregistered_context = context;
        unregistered_context.uri =
            "s3://borsuk-v36-test/geometry/assignments/unregistered.arrow".to_owned();
        let (unregistered_bytes, unregistered_artifact) =
            encode_v36_supercell_assignment_shard_arrow(&unregistered_context, &tail_rows).unwrap();
        let unregistered = authenticate_v36_supercell_assignment_shard_arrow(
            &unregistered_bytes,
            &unregistered_artifact,
        )
        .unwrap();
        assert!(
            load_v36_initial_assignment_merge_group(&committed, 2, vec![unregistered]).is_err()
        );

        let loaded = load_v36_initial_assignment_merge_group(&committed, 2, vec![tail]).unwrap();
        let mut sink = InitialMergeSink::default();
        let written = write_v36_initial_assignment_merge_group(&loaded, &mut sink).unwrap();
        assert_eq!(written.chunks().len(), 1);
        assert_eq!(written.chunks()[0].row_count, 1);
        let (root_bytes, root_identity) = sink.committed_root.as_ref().unwrap();
        let authenticated_root = authenticate_v36_initial_assignment_merge_run_root(
            &committed,
            2,
            root_bytes,
            root_identity,
        )
        .unwrap();
        assert_eq!(authenticated_root, written);

        let mut forged_manifest: serde_json::Value = serde_json::from_slice(root_bytes).unwrap();
        forged_manifest["inputs"][0]["sha256"] = serde_json::Value::String("8".repeat(64));
        let mut forged_bytes =
            serde_json::to_vec(&v36_canonical_json_value(forged_manifest)).unwrap();
        forged_bytes.push(b'\n');
        let mut forged_identity = root_identity.clone();
        forged_identity.encoded_bytes = u64::try_from(forged_bytes.len()).unwrap();
        forged_identity.sha256 = format!("{:x}", Sha256::digest(&forged_bytes));
        forged_identity.blake3 = blake3::hash(&forged_bytes).to_hex().to_string();
        assert!(
            authenticate_v36_initial_assignment_merge_run_root(
                &committed,
                2,
                &forged_bytes,
                &forged_identity,
            )
            .is_err()
        );

        let mut corrupted_root = root_bytes.clone();
        corrupted_root[0] ^= 1;
        assert!(
            authenticate_v36_initial_assignment_merge_run_root(
                &committed,
                2,
                &corrupted_root,
                root_identity,
            )
            .is_err()
        );
    }
}
