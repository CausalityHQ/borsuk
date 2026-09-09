//! Bounded coarse-code identities, controls, and unique-live admission for V36.

use std::{
    cmp::Ordering,
    collections::{BTreeSet, HashMap},
    io::Cursor,
    sync::Arc,
};

use arrow_array::{Array, FixedSizeBinaryArray, Float32Array, RecordBatch, UInt64Array};
use arrow_ipc::{
    MetadataVersion,
    reader::FileReader,
    writer::{FileWriter, IpcWriteOptions},
};
use arrow_schema::{DataType, Field, Schema};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{BorsukError, Result};

const PROJECTED_DIMENSIONS: usize = 192;
const PQ4_CODEWORDS: usize = 16;
const COARSE_FRAGMENT_LIMIT_BYTES: usize = 1_024 * 1_024;
const COARSE_FRAGMENT_DECODE_LIMIT_BYTES: u64 = 8 * 1_024 * 1_024;
const COARSE_FRAGMENT_MANIFEST_KEY: &str = "borsuk.v36.coarse_fragment_manifest";
const COARSE_FRAGMENT_FORMAT: &str = "borsuk-v36-coarse-fragment-v1";

fn invalid(message: &'static str) -> BorsukError {
    BorsukError::InvalidStorage(message.to_owned())
}

fn valid_vector(vector: &[f32]) -> bool {
    vector.len() == PROJECTED_DIMENSIONS
        && vector
            .iter()
            .all(|value| value.is_finite() && !(value.to_bits() == (-0.0_f32).to_bits()))
}

/// Visitor for one canonical block of owner-relative assignment inputs.
pub type V36ResidualAssignmentBlockVisitor<'a> =
    dyn FnMut(&[V36CoarseAssignmentIdentity], &[f32]) -> Result<()> + 'a;

/// Replayable, bounded source used by residual-PQ training passes.
pub trait V36ResidualAssignmentSource {
    /// Scan canonical identities and row-major projected f32[192] blocks.
    fn scan(&mut self, visitor: &mut V36ResidualAssignmentBlockVisitor<'_>) -> Result<()>;
}

/// Distinct construction and persisted identities for one owner-relative
/// coarse assignment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct V36CoarseAssignmentIdentity {
    source_ordinal: u64,
    dense_ordinal: u64,
    source_feature_id: u64,
    owner_posting_ordinal: u32,
}

impl V36CoarseAssignmentIdentity {
    /// Construct an identity without aliasing source, dense, and feature roles.
    pub const fn new(
        source_ordinal: u64,
        dense_ordinal: u64,
        source_feature_id: u64,
        owner_posting_ordinal: u32,
    ) -> Self {
        Self {
            source_ordinal,
            dense_ordinal,
            source_feature_id,
            owner_posting_ordinal,
        }
    }

    /// Source position used by deterministic construction reductions.
    pub const fn source_ordinal(self) -> u64 {
        self.source_ordinal
    }

    /// Final primary-plane physical identity.
    pub const fn dense_ordinal(self) -> u64 {
        self.dense_ordinal
    }

    /// External result identity.
    pub const fn source_feature_id(self) -> u64 {
        self.source_feature_id
    }

    /// Posting whose centroid defines this replica's residual.
    pub const fn owner_posting_ordinal(self) -> u32 {
        self.owner_posting_ordinal
    }
}

/// Exact 44-byte sign24 coarse control record.
#[derive(Debug, Clone, PartialEq)]
pub struct V36Sign24Record {
    code: [u8; 24],
    residual_norm: f32,
    dense_ordinal: u64,
    source_feature_id: u64,
}

impl V36Sign24Record {
    /// Dimension-ordered, least-significant-bit-first signs.
    pub const fn code(&self) -> &[u8; 24] {
        &self.code
    }

    /// Binary32-rounded residual L2 norm.
    pub const fn residual_norm(&self) -> f32 {
        self.residual_norm
    }

    /// Final primary-plane physical identity.
    pub const fn dense_ordinal(&self) -> u64 {
        self.dense_ordinal
    }

    /// External feature identity.
    pub const fn source_feature_id(&self) -> u64 {
        self.source_feature_id
    }

    /// Encode the exact persisted 44-byte little-endian record.
    pub fn to_bytes(&self) -> [u8; 44] {
        let mut bytes = [0_u8; 44];
        bytes[..24].copy_from_slice(&self.code);
        bytes[24..28].copy_from_slice(&self.residual_norm.to_le_bytes());
        bytes[28..36].copy_from_slice(&self.dense_ordinal.to_le_bytes());
        bytes[36..44].copy_from_slice(&self.source_feature_id.to_le_bytes());
        bytes
    }
}

/// Encode one projected row relative to the centroid of its containing owner.
pub fn encode_v36_sign24_record(
    identity: &V36CoarseAssignmentIdentity,
    projected_row: &[f32],
    owner_centroid: &[f32],
) -> Result<V36Sign24Record> {
    if !valid_vector(projected_row) || !valid_vector(owner_centroid) {
        return Err(invalid("V36 sign24 vector authority differs"));
    }
    let mut residual = [0.0_f64; PROJECTED_DIMENSIONS];
    let mut squared_norm = 0.0_f64;
    for dimension in 0..PROJECTED_DIMENSIONS {
        let value = f64::from(projected_row[dimension]) - f64::from(owner_centroid[dimension]);
        residual[dimension] = value;
        squared_norm += value * value;
    }
    let residual_norm = squared_norm.sqrt() as f32;
    if !residual_norm.is_finite() {
        return Err(invalid("V36 sign24 residual norm differs"));
    }
    let mut code = [0_u8; 24];
    if residual_norm != 0.0 {
        for (dimension, value) in residual.into_iter().enumerate() {
            if value >= 0.0 {
                code[dimension / 8] |= 1 << (dimension % 8);
            }
        }
    }
    Ok(V36Sign24Record {
        code,
        residual_norm,
        dense_ordinal: identity.dense_ordinal,
        source_feature_id: identity.source_feature_id,
    })
}

/// Score one sign24 record with fixed-order unfused binary64 squared L2.
pub fn score_v36_sign24_record(
    record: &V36Sign24Record,
    projected_query: &[f32],
    owner_centroid: &[f32],
) -> Result<f64> {
    if !valid_vector(projected_query)
        || !valid_vector(owner_centroid)
        || !record.residual_norm.is_finite()
        || record.residual_norm < 0.0
    {
        return Err(invalid("V36 sign24 score authority differs"));
    }
    let scale = f64::from(record.residual_norm) / (PROJECTED_DIMENSIONS as f64).sqrt();
    let mut distance = 0.0_f64;
    for dimension in 0..PROJECTED_DIMENSIONS {
        let query_residual =
            f64::from(projected_query[dimension]) - f64::from(owner_centroid[dimension]);
        let reconstruction = if record.residual_norm == 0.0 {
            0.0
        } else if record.code[dimension / 8] & (1 << (dimension % 8)) != 0 {
            scale
        } else {
            -scale
        };
        let delta = query_residual - reconstruction;
        distance += delta * delta;
    }
    Ok(distance)
}

/// Frozen residual PQ4 code width.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum V36ResidualPq4Width {
    /// 64 contiguous three-dimensional subquantizers.
    Code32,
    /// 96 contiguous two-dimensional subquantizers.
    Code48,
}

impl V36ResidualPq4Width {
    /// Number of four-bit subquantizers.
    pub const fn subquantizers(self) -> usize {
        match self {
            Self::Code32 => 64,
            Self::Code48 => 96,
        }
    }

    /// Persisted PQ bytes per assignment.
    pub const fn code_bytes(self) -> usize {
        self.subquantizers() / 2
    }

    /// Persisted bytes including both u64 identities.
    pub const fn record_bytes(self) -> usize {
        self.code_bytes() + 16
    }

    const fn subvector_dimensions(self) -> usize {
        PROJECTED_DIMENSIONS / self.subquantizers()
    }
}

/// One global residual PQ4 model with 16 binary32 codewords per subquantizer.
#[derive(Debug, Clone, PartialEq)]
pub struct V36ResidualPq4Codebook {
    width: V36ResidualPq4Width,
    centroids: Vec<f32>,
}

impl V36ResidualPq4Codebook {
    /// Validate a subquantizer-major, codeword-major, component-major model.
    pub fn try_new(width: V36ResidualPq4Width, centroids: Vec<f32>) -> Result<Self> {
        if centroids.len() != PQ4_CODEWORDS * PROJECTED_DIMENSIONS
            || centroids
                .iter()
                .any(|value| !value.is_finite() || value.to_bits() == (-0.0_f32).to_bits())
        {
            return Err(invalid("V36 residual PQ4 codebook differs"));
        }
        Ok(Self { width, centroids })
    }

    /// Exact raw binary32 model bytes.
    pub const fn raw_bytes(&self) -> usize {
        PQ4_CODEWORDS * PROJECTED_DIMENSIONS * size_of::<f32>()
    }

    /// Frozen arm width.
    pub const fn width(&self) -> V36ResidualPq4Width {
        self.width
    }

    /// One validated codeword in component order.
    pub fn codeword(&self, subquantizer: usize, codeword: usize) -> Result<&[f32]> {
        if subquantizer >= self.width.subquantizers() || codeword >= PQ4_CODEWORDS {
            return Err(invalid("V36 residual PQ4 codeword ordinal differs"));
        }
        let dimensions = self.width.subvector_dimensions();
        let start = (subquantizer * PQ4_CODEWORDS + codeword) * dimensions;
        Ok(&self.centroids[start..start + dimensions])
    }

    /// Complete subquantizer-major model for authority and serialization.
    pub fn centroids(&self) -> &[f32] {
        &self.centroids
    }
}

/// One fixed-capacity owner-relative residual PQ4 coarse record.
#[derive(Debug, Clone, PartialEq)]
pub struct V36ResidualPq4Record {
    width: V36ResidualPq4Width,
    code: [u8; 48],
    dense_ordinal: u64,
    source_feature_id: u64,
}

impl V36ResidualPq4Record {
    /// Row-major PQ bytes with the even subquantizer in the low nibble.
    pub fn code(&self) -> &[u8] {
        &self.code[..self.width.code_bytes()]
    }

    /// Frozen residual-code width.
    pub const fn width(&self) -> V36ResidualPq4Width {
        self.width
    }

    /// Final primary-plane physical identity.
    pub const fn dense_ordinal(&self) -> u64 {
        self.dense_ordinal
    }

    /// External feature identity.
    pub const fn source_feature_id(&self) -> u64 {
        self.source_feature_id
    }

    /// Encode the exact variable-width record bytes.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(self.width.record_bytes());
        bytes.extend_from_slice(self.code());
        bytes.extend_from_slice(&self.dense_ordinal.to_le_bytes());
        bytes.extend_from_slice(&self.source_feature_id.to_le_bytes());
        bytes
    }
}

fn pq4_subvector_distance(residual: &[f64], codeword: &[f32]) -> f64 {
    let mut distance = 0.0_f64;
    for (value, center) in residual.iter().zip(codeword) {
        let delta = *value - f64::from(*center);
        distance += delta * delta;
    }
    distance
}

/// Encode one row against the centroid of the owner containing this replica.
pub fn encode_v36_residual_pq4_record(
    identity: &V36CoarseAssignmentIdentity,
    projected_row: &[f32],
    owner_centroid: &[f32],
    codebook: &V36ResidualPq4Codebook,
) -> Result<V36ResidualPq4Record> {
    if !valid_vector(projected_row) || !valid_vector(owner_centroid) {
        return Err(invalid("V36 residual PQ4 vector authority differs"));
    }
    let dimensions = codebook.width.subvector_dimensions();
    let mut code = [0_u8; 48];
    let mut residual = [0.0_f64; 3];
    for subquantizer in 0..codebook.width.subquantizers() {
        let start = subquantizer * dimensions;
        for component in 0..dimensions {
            residual[component] = f64::from(projected_row[start + component])
                - f64::from(owner_centroid[start + component]);
        }
        let residual = &residual[..dimensions];
        let mut best = (
            pq4_subvector_distance(residual, codebook.codeword(subquantizer, 0)?),
            0,
        );
        for codeword in 1..16 {
            let distance =
                pq4_subvector_distance(residual, codebook.codeword(subquantizer, codeword)?);
            if distance < best.0 {
                best = (distance, codeword);
            }
        }
        let nibble = u8::try_from(best.1).map_err(|_| invalid("V36 PQ4 codeword overflows"))?;
        if subquantizer % 2 == 0 {
            code[subquantizer / 2] = nibble;
        } else {
            code[subquantizer / 2] |= nibble << 4;
        }
    }
    Ok(V36ResidualPq4Record {
        width: codebook.width,
        code,
        dense_ordinal: identity.dense_ordinal,
        source_feature_id: identity.source_feature_id,
    })
}

/// Score one residual PQ4 record by exact increasing-subquantizer ADC.
pub fn score_v36_residual_pq4_record(
    record: &V36ResidualPq4Record,
    projected_query: &[f32],
    owner_centroid: &[f32],
    codebook: &V36ResidualPq4Codebook,
) -> Result<f64> {
    if record.width != codebook.width
        || !valid_vector(projected_query)
        || !valid_vector(owner_centroid)
    {
        return Err(invalid("V36 residual PQ4 score authority differs"));
    }
    let dimensions = codebook.width.subvector_dimensions();
    let mut distance = 0.0_f64;
    let mut residual = [0.0_f64; 3];
    for subquantizer in 0..codebook.width.subquantizers() {
        let start = subquantizer * dimensions;
        for component in 0..dimensions {
            residual[component] = f64::from(projected_query[start + component])
                - f64::from(owner_centroid[start + component]);
        }
        let byte = record.code[subquantizer / 2];
        let codeword = if subquantizer % 2 == 0 {
            byte & 0x0f
        } else {
            byte >> 4
        };
        distance += pq4_subvector_distance(
            &residual[..dimensions],
            codebook.codeword(subquantizer, usize::from(codeword))?,
        );
    }
    Ok(distance)
}

/// Serving coarse fragment arm. Projected-f32 is deliberately absent.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum V36CoarseFragmentArm {
    /// Sign control with an explicit residual norm.
    Sign24,
    /// Residual PQ with 32 persisted code bytes.
    ResidualPq4Code32,
    /// Residual PQ with 48 persisted code bytes.
    ResidualPq4Code48,
}

impl V36CoarseFragmentArm {
    fn code_width(self) -> i32 {
        match self {
            Self::Sign24 => 24,
            Self::ResidualPq4Code32 => 32,
            Self::ResidualPq4Code48 => 48,
        }
    }

    fn pq_width(self) -> Option<V36ResidualPq4Width> {
        match self {
            Self::Sign24 => None,
            Self::ResidualPq4Code32 => Some(V36ResidualPq4Width::Code32),
            Self::ResidualPq4Code48 => Some(V36ResidualPq4Width::Code48),
        }
    }
}

/// Immutable generation and model bindings for one coarse fragment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct V36CoarseFragmentContext {
    /// Serving arm.
    pub arm: V36CoarseFragmentArm,
    /// PQ codebook identity; absent only for sign24.
    pub codebook_sha256: Option<String>,
    /// Consecutive fragment ordinal inside the owner posting.
    pub fragment_ordinal: u32,
    /// Active generation manifest identity.
    pub generation_manifest_sha256: String,
    /// Owner centroid set identity.
    pub owner_centroids_sha256: String,
    /// Owner posting ordinal.
    pub posting_ordinal: u32,
    /// Centered projection identity.
    pub projection_sha256: String,
    /// Immutable object URI.
    pub uri: String,
}

/// Typed rows held by one serving coarse fragment.
#[derive(Debug, Clone, PartialEq)]
pub enum V36CoarseFragmentRows {
    /// Sign records with explicit norm values.
    Sign24(Vec<V36Sign24Record>),
    /// Residual PQ records whose widths must match the fragment arm.
    ResidualPq4(Vec<V36ResidualPq4Record>),
}

/// Registered complete-object identity and decoded bounds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct V36CoarseFragmentArtifact {
    /// Complete-object BLAKE3.
    pub blake3: String,
    /// Generation/model/owner bindings embedded in the Arrow schema.
    pub context: V36CoarseFragmentContext,
    /// Conservative peak decode workspace derived from object length, row
    /// count, arm, retained records, and validation scratch.
    pub decoded_capacity_bytes: u64,
    /// Complete encoded object length.
    pub encoded_bytes: u64,
    /// First sparse primary-plane dense ordinal.
    pub first_dense_ordinal: u64,
    /// Last sparse primary-plane dense ordinal.
    pub last_dense_ordinal: u64,
    /// Complete fragment row count.
    pub row_count: u32,
    /// Complete-object SHA-256.
    pub sha256: String,
}

fn validate_v36_coarse_artifact(artifact: &V36CoarseFragmentArtifact) -> Result<()> {
    validate_v36_coarse_context(&artifact.context)?;
    if artifact.row_count == 0
        || artifact.first_dense_ordinal > artifact.last_dense_ordinal
        || artifact.encoded_bytes == 0
        || artifact.encoded_bytes > COARSE_FRAGMENT_LIMIT_BYTES as u64
        || !valid_sha256(&artifact.sha256)
        || !valid_sha256(&artifact.blake3)
    {
        return Err(invalid("V36 coarse fragment artifact differs"));
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct V36CoarseFragmentManifest {
    arm: V36CoarseFragmentArm,
    codebook_sha256: Option<String>,
    first_dense_ordinal: u64,
    format: String,
    fragment_ordinal: u32,
    generation_manifest_sha256: String,
    last_dense_ordinal: u64,
    owner_centroids_sha256: String,
    posting_ordinal: u32,
    projection_sha256: String,
    row_count: u32,
}

fn valid_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn validate_v36_coarse_context(context: &V36CoarseFragmentContext) -> Result<()> {
    if context.uri.is_empty()
        || !valid_sha256(&context.generation_manifest_sha256)
        || !valid_sha256(&context.owner_centroids_sha256)
        || !valid_sha256(&context.projection_sha256)
        || context.arm.pq_width().is_some() != context.codebook_sha256.is_some()
        || context
            .codebook_sha256
            .as_deref()
            .is_some_and(|digest| !valid_sha256(digest))
    {
        return Err(invalid("V36 coarse fragment context differs"));
    }
    Ok(())
}

fn coarse_row_identities(rows: &V36CoarseFragmentRows) -> Result<Vec<(u64, u64)>> {
    let identities = match rows {
        V36CoarseFragmentRows::Sign24(rows) => rows
            .iter()
            .map(|row| (row.dense_ordinal(), row.source_feature_id()))
            .collect::<Vec<_>>(),
        V36CoarseFragmentRows::ResidualPq4(rows) => rows
            .iter()
            .map(|row| (row.dense_ordinal(), row.source_feature_id()))
            .collect::<Vec<_>>(),
    };
    let mut source_ids = BTreeSet::new();
    if identities.is_empty()
        || identities.windows(2).any(|pair| pair[0].0 >= pair[1].0)
        || identities
            .iter()
            .any(|(_, source_id)| !source_ids.insert(*source_id))
    {
        return Err(invalid("V36 coarse fragment row identity differs"));
    }
    Ok(identities)
}

fn validate_v36_coarse_rows(
    context: &V36CoarseFragmentContext,
    rows: &V36CoarseFragmentRows,
) -> Result<Vec<(u64, u64)>> {
    match rows {
        V36CoarseFragmentRows::Sign24(rows) => {
            if context.arm != V36CoarseFragmentArm::Sign24
                || rows.iter().any(|row| {
                    !row.residual_norm().is_finite()
                        || row.residual_norm() < 0.0
                        || row.residual_norm().to_bits() == (-0.0_f32).to_bits()
                        || (row.residual_norm() == 0.0 && row.code().iter().any(|byte| *byte != 0))
                })
            {
                return Err(invalid("V36 sign24 fragment rows differ"));
            }
        }
        V36CoarseFragmentRows::ResidualPq4(rows) => {
            let expected = context
                .arm
                .pq_width()
                .ok_or_else(|| invalid("V36 PQ fragment arm differs"))?;
            if rows.iter().any(|row| row.width() != expected) {
                return Err(invalid("V36 PQ fragment width differs"));
            }
        }
    }
    coarse_row_identities(rows)
}

fn v36_coarse_manifest(
    context: &V36CoarseFragmentContext,
    identities: &[(u64, u64)],
) -> Result<V36CoarseFragmentManifest> {
    let row_count = u32::try_from(identities.len())
        .map_err(|_| invalid("V36 coarse fragment row count overflows"))?;
    Ok(V36CoarseFragmentManifest {
        arm: context.arm,
        codebook_sha256: context.codebook_sha256.clone(),
        first_dense_ordinal: identities[0].0,
        format: COARSE_FRAGMENT_FORMAT.to_owned(),
        fragment_ordinal: context.fragment_ordinal,
        generation_manifest_sha256: context.generation_manifest_sha256.clone(),
        last_dense_ordinal: identities[identities.len() - 1].0,
        owner_centroids_sha256: context.owner_centroids_sha256.clone(),
        posting_ordinal: context.posting_ordinal,
        projection_sha256: context.projection_sha256.clone(),
        row_count,
    })
}

fn v36_coarse_schema(manifest: &V36CoarseFragmentManifest) -> Result<Arc<Schema>> {
    let mut fields = vec![
        Field::new("dense_ordinal", DataType::UInt64, false),
        Field::new("source_feature_id", DataType::UInt64, false),
        Field::new(
            "code",
            DataType::FixedSizeBinary(manifest.arm.code_width()),
            false,
        ),
    ];
    if manifest.arm == V36CoarseFragmentArm::Sign24 {
        fields.push(Field::new("residual_norm", DataType::Float32, false));
    }
    let manifest_json = serde_json::to_string(manifest)
        .map_err(|_| invalid("V36 coarse fragment manifest cannot be serialized"))?;
    Ok(Arc::new(Schema::new_with_metadata(
        fields,
        HashMap::from([(COARSE_FRAGMENT_MANIFEST_KEY.to_owned(), manifest_json)]),
    )))
}

fn coarse_decoded_capacity(
    arm: V36CoarseFragmentArm,
    rows: usize,
    encoded_bytes: usize,
) -> Result<u64> {
    let row_bytes = usize::try_from(arm.code_width())
        .map_err(|_| invalid("V36 coarse fragment code width overflows"))?
        .checked_add(16)
        .and_then(|bytes| {
            bytes.checked_add(if arm == V36CoarseFragmentArm::Sign24 {
                4
            } else {
                0
            })
        })
        .ok_or_else(|| invalid("V36 coarse fragment row width overflows"))?;
    let retained_row_bytes = match arm {
        V36CoarseFragmentArm::Sign24 => size_of::<V36Sign24Record>(),
        V36CoarseFragmentArm::ResidualPq4Code32 | V36CoarseFragmentArm::ResidualPq4Code48 => {
            size_of::<V36ResidualPq4Record>()
        }
    };
    let bytes = encoded_bytes
        .checked_add(
            rows.checked_mul(row_bytes)
                .ok_or_else(|| invalid("V36 coarse fragment logical bytes overflow"))?,
        )
        .and_then(|bytes| bytes.checked_add(rows.checked_mul(retained_row_bytes)?))
        .and_then(|bytes| bytes.checked_add(rows.checked_mul(size_of::<(u64, u64)>())?))
        // Conservative BTreeSet node/allocation charge for uniqueness validation.
        .and_then(|bytes| bytes.checked_add(rows.checked_mul(96)?))
        .and_then(|bytes| bytes.checked_add(64 * 1_024))
        .ok_or_else(|| invalid("V36 coarse fragment decoded bytes overflow"))?;
    let bytes =
        u64::try_from(bytes).map_err(|_| invalid("V36 coarse fragment decoded bytes overflow"))?;
    if bytes > COARSE_FRAGMENT_DECODE_LIMIT_BYTES {
        return Err(invalid("V36 coarse fragment decoded admission differs"));
    }
    Ok(bytes)
}

fn validate_v36_coarse_ipc_field(field: arrow_ipc::Field<'_>, expected: &Field) -> Result<()> {
    if field.name() != Some(expected.name().as_str())
        || field.nullable()
        || field.dictionary().is_some()
        || field
            .custom_metadata()
            .is_some_and(|metadata| !metadata.is_empty())
        || field
            .children()
            .is_some_and(|children| !children.is_empty())
    {
        return Err(invalid("V36 coarse fragment IPC field differs"));
    }
    match expected.data_type() {
        DataType::UInt64 => {
            let integer = field
                .type_as_int()
                .ok_or_else(|| invalid("V36 coarse fragment IPC integer differs"))?;
            if integer.bitWidth() != 64 || integer.is_signed() {
                return Err(invalid("V36 coarse fragment IPC integer differs"));
            }
        }
        DataType::FixedSizeBinary(width) => {
            if field
                .type_as_fixed_size_binary()
                .is_none_or(|binary| binary.byteWidth() != *width)
            {
                return Err(invalid("V36 coarse fragment IPC binary differs"));
            }
        }
        DataType::Float32 => {
            if field
                .type_as_floating_point()
                .is_none_or(|float| float.precision() != arrow_ipc::Precision::SINGLE)
            {
                return Err(invalid("V36 coarse fragment IPC float differs"));
            }
        }
        _ => return Err(invalid("V36 coarse fragment IPC type differs")),
    }
    Ok(())
}

fn validate_v36_coarse_ipc_schema(schema: arrow_ipc::Schema<'_>, expected: &Schema) -> Result<()> {
    if schema.endianness() != arrow_ipc::Endianness::Little
        || schema
            .features()
            .is_some_and(|features| !features.is_empty())
    {
        return Err(invalid("V36 coarse fragment IPC schema differs"));
    }
    let metadata = schema
        .custom_metadata()
        .ok_or_else(|| invalid("V36 coarse fragment IPC manifest is missing"))?;
    let expected_manifest = expected
        .metadata()
        .get(COARSE_FRAGMENT_MANIFEST_KEY)
        .ok_or_else(|| invalid("V36 coarse fragment IPC manifest differs"))?;
    if metadata.len() != 1
        || metadata.get(0).key() != Some(COARSE_FRAGMENT_MANIFEST_KEY)
        || metadata.get(0).value() != Some(expected_manifest.as_str())
    {
        return Err(invalid("V36 coarse fragment IPC manifest differs"));
    }
    let fields = schema
        .fields()
        .ok_or_else(|| invalid("V36 coarse fragment IPC fields are missing"))?;
    if fields.len() != expected.fields().len() {
        return Err(invalid("V36 coarse fragment IPC field count differs"));
    }
    for (index, expected_field) in expected.fields().iter().enumerate() {
        validate_v36_coarse_ipc_field(fields.get(index), expected_field)?;
    }
    Ok(())
}

fn preflight_v36_coarse_ipc(
    bytes: &[u8],
    expected_schema: &Schema,
    arm: V36CoarseFragmentArm,
    row_count: u32,
) -> Result<()> {
    if bytes.len() < 18 || !bytes.starts_with(b"ARROW1") || !bytes.ends_with(b"ARROW1") {
        return Err(invalid("V36 coarse fragment IPC envelope differs"));
    }
    let trailer = bytes.len() - 10;
    let footer_len = u32::from_le_bytes(
        bytes
            .get(trailer..trailer + 4)
            .and_then(|value| value.try_into().ok())
            .ok_or_else(|| invalid("V36 coarse fragment footer length differs"))?,
    ) as usize;
    let footer_start = trailer
        .checked_sub(footer_len)
        .filter(|offset| *offset >= 8)
        .ok_or_else(|| invalid("V36 coarse fragment footer extent differs"))?;
    let footer = arrow_ipc::root_as_footer(&bytes[footer_start..trailer])
        .map_err(|_| invalid("V36 coarse fragment footer differs"))?;
    if footer.version() != MetadataVersion::V5
        || footer
            .custom_metadata()
            .is_some_and(|metadata| !metadata.is_empty())
        || footer
            .dictionaries()
            .is_some_and(|dictionaries| !dictionaries.is_empty())
    {
        return Err(invalid("V36 coarse fragment footer authority differs"));
    }
    validate_v36_coarse_ipc_schema(
        footer
            .schema()
            .ok_or_else(|| invalid("V36 coarse fragment footer schema is missing"))?,
        expected_schema,
    )?;
    let batches = footer
        .recordBatches()
        .ok_or_else(|| invalid("V36 coarse fragment batch is missing"))?;
    if batches.len() != 1 {
        return Err(invalid("V36 coarse fragment batch count differs"));
    }
    let block = batches.get(0);
    let block_offset = usize::try_from(block.offset())
        .map_err(|_| invalid("V36 coarse fragment batch offset differs"))?;
    let metadata_len = usize::try_from(block.metaDataLength())
        .map_err(|_| invalid("V36 coarse fragment batch metadata differs"))?;
    let body_len = usize::try_from(block.bodyLength())
        .map_err(|_| invalid("V36 coarse fragment batch body differs"))?;
    let body_start = block_offset
        .checked_add(metadata_len)
        .ok_or_else(|| invalid("V36 coarse fragment batch extent overflows"))?;
    let body_end = body_start
        .checked_add(body_len)
        .ok_or_else(|| invalid("V36 coarse fragment batch extent overflows"))?;
    if block_offset < 8 || metadata_len < 8 || body_end > footer_start {
        return Err(invalid("V36 coarse fragment batch extent differs"));
    }
    let parse_message = |start: usize, end: usize| {
        let metadata = bytes
            .get(start..end)
            .ok_or_else(|| invalid("V36 coarse fragment message extent differs"))?;
        let prefix = if metadata.starts_with(&[255; 4]) {
            8
        } else {
            4
        };
        let message_len = u32::from_le_bytes(
            metadata
                .get(prefix - 4..prefix)
                .and_then(|value| value.try_into().ok())
                .ok_or_else(|| invalid("V36 coarse fragment message length differs"))?,
        ) as usize;
        let message_end = prefix
            .checked_add(message_len)
            .filter(|end| *end <= metadata.len())
            .ok_or_else(|| invalid("V36 coarse fragment message extent differs"))?;
        arrow_ipc::root_as_message(&metadata[prefix..message_end])
            .map_err(|_| invalid("V36 coarse fragment message differs"))
    };
    let leading = parse_message(8, block_offset)?;
    if leading.version() != MetadataVersion::V5 || leading.bodyLength() != 0 {
        return Err(invalid("V36 coarse fragment leading schema differs"));
    }
    validate_v36_coarse_ipc_schema(
        leading
            .header_as_schema()
            .ok_or_else(|| invalid("V36 coarse fragment leading schema is missing"))?,
        expected_schema,
    )?;
    let message = parse_message(block_offset, body_start)?;
    let record = message
        .header_as_record_batch()
        .ok_or_else(|| invalid("V36 coarse fragment record differs"))?;
    if message.version() != MetadataVersion::V5
        || record.compression().is_some()
        || record
            .variadicBufferCounts()
            .is_some_and(|counts| !counts.is_empty())
        || u32::try_from(record.length()).ok() != Some(row_count)
        || usize::try_from(message.bodyLength()).ok() != Some(body_len)
    {
        return Err(invalid("V36 coarse fragment record authority differs"));
    }
    let nodes = record
        .nodes()
        .ok_or_else(|| invalid("V36 coarse fragment nodes are missing"))?;
    let data_widths = if arm == V36CoarseFragmentArm::Sign24 {
        vec![8_usize, 8, 24, 4]
    } else {
        vec![
            8_usize,
            8,
            usize::try_from(arm.code_width())
                .map_err(|_| invalid("V36 coarse fragment code width overflows"))?,
        ]
    };
    if nodes.len() != data_widths.len()
        || nodes.iter().any(|node| {
            u32::try_from(node.length()).ok() != Some(row_count) || node.null_count() != 0
        })
    {
        return Err(invalid("V36 coarse fragment node shape differs"));
    }
    let buffers = record
        .buffers()
        .ok_or_else(|| invalid("V36 coarse fragment buffers are missing"))?;
    if buffers.len() != data_widths.len() * 2 {
        return Err(invalid("V36 coarse fragment buffer count differs"));
    }
    let validity_bytes = usize::try_from(row_count)
        .ok()
        .and_then(|rows| rows.checked_add(7))
        .map(|bits| bits / 8)
        .ok_or_else(|| invalid("V36 coarse fragment validity length overflows"))?;
    let mut previous_end = 0_usize;
    for (column, width) in data_widths.into_iter().enumerate() {
        for (part, expected) in [
            validity_bytes,
            usize::try_from(row_count)
                .ok()
                .and_then(|rows| rows.checked_mul(width))
                .ok_or_else(|| invalid("V36 coarse fragment buffer length overflows"))?,
        ]
        .into_iter()
        .enumerate()
        {
            let buffer = buffers.get(column * 2 + part);
            let offset = usize::try_from(buffer.offset())
                .map_err(|_| invalid("V36 coarse fragment buffer offset differs"))?;
            let length = usize::try_from(buffer.length())
                .map_err(|_| invalid("V36 coarse fragment buffer length differs"))?;
            let end = offset
                .checked_add(length)
                .ok_or_else(|| invalid("V36 coarse fragment buffer extent overflows"))?;
            if offset < previous_end
                || (part == 0 && length != 0 && length != expected)
                || (part == 1 && length != expected)
                || end > body_len
            {
                return Err(invalid("V36 coarse fragment buffer extent differs"));
            }
            previous_end = end;
        }
    }
    Ok(())
}

/// Encode one canonical, uncompressed, complete V36 serving fragment.
pub fn encode_v36_coarse_fragment_arrow(
    context: &V36CoarseFragmentContext,
    rows: &V36CoarseFragmentRows,
) -> Result<(Vec<u8>, V36CoarseFragmentArtifact)> {
    validate_v36_coarse_context(context)?;
    let identities = validate_v36_coarse_rows(context, rows)?;
    let manifest = v36_coarse_manifest(context, &identities)?;
    let schema = v36_coarse_schema(&manifest)?;
    let codes = match rows {
        V36CoarseFragmentRows::Sign24(rows) => {
            FixedSizeBinaryArray::try_from_iter(rows.iter().map(|row| row.code().as_slice()))?
        }
        V36CoarseFragmentRows::ResidualPq4(rows) => {
            FixedSizeBinaryArray::try_from_iter(rows.iter().map(V36ResidualPq4Record::code))?
        }
    };
    let mut columns: Vec<Arc<dyn Array>> = vec![
        Arc::new(UInt64Array::from_iter_values(
            identities.iter().map(|(dense, _)| *dense),
        )),
        Arc::new(UInt64Array::from_iter_values(
            identities.iter().map(|(_, source)| *source),
        )),
        Arc::new(codes),
    ];
    if let V36CoarseFragmentRows::Sign24(rows) = rows {
        columns.push(Arc::new(Float32Array::from_iter_values(
            rows.iter().map(V36Sign24Record::residual_norm),
        )));
    }
    let batch = RecordBatch::try_new(Arc::clone(&schema), columns)?;
    let options = IpcWriteOptions::try_new(8, false, MetadataVersion::V5)?;
    let mut bytes = Vec::new();
    let mut writer = FileWriter::try_new_with_options(&mut bytes, schema.as_ref(), options)?;
    writer.write(&batch)?;
    writer.finish()?;
    drop(writer);
    if bytes.len() > COARSE_FRAGMENT_LIMIT_BYTES {
        return Err(invalid("V36 coarse fragment exceeds encoded admission"));
    }
    let sha256 = format!("{:x}", Sha256::digest(&bytes));
    let blake3 = blake3::hash(&bytes).to_hex().to_string();
    let artifact = V36CoarseFragmentArtifact {
        blake3,
        context: context.clone(),
        decoded_capacity_bytes: coarse_decoded_capacity(
            context.arm,
            identities.len(),
            bytes.len(),
        )?,
        encoded_bytes: u64::try_from(bytes.len())
            .map_err(|_| invalid("V36 coarse fragment encoded bytes overflow"))?,
        first_dense_ordinal: manifest.first_dense_ordinal,
        last_dense_ordinal: manifest.last_dense_ordinal,
        row_count: manifest.row_count,
        sha256,
    };
    Ok((bytes, artifact))
}

/// Audit a complete fragment's publication SHA-256 at generation admission.
///
/// Query cache admission uses the fragment's BLAKE3 instead, so the fetched
/// body is not dual-hashed again on every query.
pub fn audit_v36_coarse_fragment_sha256(
    bytes: &[u8],
    artifact: &V36CoarseFragmentArtifact,
) -> Result<()> {
    validate_v36_coarse_artifact(artifact)?;
    if u64::try_from(bytes.len()).ok() != Some(artifact.encoded_bytes)
        || format!("{:x}", Sha256::digest(bytes)) != artifact.sha256
    {
        return Err(invalid("V36 coarse fragment publication identity differs"));
    }
    Ok(())
}

/// BLAKE3-authenticate and decode one complete V36 serving fragment at bounded
/// query-cache admission.
pub fn decode_v36_coarse_fragment_arrow(
    bytes: &[u8],
    artifact: &V36CoarseFragmentArtifact,
) -> Result<V36CoarseFragmentRows> {
    validate_v36_coarse_artifact(artifact)?;
    if bytes.is_empty()
        || bytes.len() > COARSE_FRAGMENT_LIMIT_BYTES
        || u64::try_from(bytes.len()).ok() != Some(artifact.encoded_bytes)
        || blake3::hash(bytes).to_hex().as_str() != artifact.blake3
    {
        return Err(invalid("V36 coarse fragment object identity differs"));
    }
    let row_count = usize::try_from(artifact.row_count)
        .map_err(|_| invalid("V36 coarse fragment row count overflows"))?;
    if coarse_decoded_capacity(artifact.context.arm, row_count, bytes.len())?
        != artifact.decoded_capacity_bytes
    {
        return Err(invalid("V36 coarse fragment decoded authority differs"));
    }
    let expected_manifest = V36CoarseFragmentManifest {
        arm: artifact.context.arm,
        codebook_sha256: artifact.context.codebook_sha256.clone(),
        first_dense_ordinal: artifact.first_dense_ordinal,
        format: COARSE_FRAGMENT_FORMAT.to_owned(),
        fragment_ordinal: artifact.context.fragment_ordinal,
        generation_manifest_sha256: artifact.context.generation_manifest_sha256.clone(),
        last_dense_ordinal: artifact.last_dense_ordinal,
        owner_centroids_sha256: artifact.context.owner_centroids_sha256.clone(),
        posting_ordinal: artifact.context.posting_ordinal,
        projection_sha256: artifact.context.projection_sha256.clone(),
        row_count: artifact.row_count,
    };
    let expected_schema = v36_coarse_schema(&expected_manifest)?;
    preflight_v36_coarse_ipc(
        bytes,
        expected_schema.as_ref(),
        artifact.context.arm,
        artifact.row_count,
    )?;
    let mut reader = FileReader::try_new(Cursor::new(bytes), None)?;
    if reader.num_batches() != 1 || reader.schema() != expected_schema {
        return Err(invalid("V36 coarse fragment Arrow schema differs"));
    }
    let batch = reader
        .next()
        .transpose()?
        .ok_or_else(|| invalid("V36 coarse fragment batch is missing"))?;
    if reader.next().is_some()
        || batch.num_rows() != usize::try_from(artifact.row_count).unwrap_or(usize::MAX)
        || batch
            .columns()
            .iter()
            .any(|column| column.null_count() != 0)
    {
        return Err(invalid("V36 coarse fragment batch differs"));
    }
    let dense = batch
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| invalid("V36 coarse fragment dense ordinals differ"))?;
    let source = batch
        .column(1)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| invalid("V36 coarse fragment source IDs differ"))?;
    let codes = batch
        .column(2)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .ok_or_else(|| invalid("V36 coarse fragment codes differ"))?;
    let rows = match artifact.context.arm {
        V36CoarseFragmentArm::Sign24 => {
            let norms = batch
                .column(3)
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| invalid("V36 coarse fragment norms differ"))?;
            let mut rows = Vec::with_capacity(batch.num_rows());
            for row in 0..batch.num_rows() {
                let code: [u8; 24] = codes
                    .value(row)
                    .try_into()
                    .map_err(|_| invalid("V36 sign24 fragment code differs"))?;
                rows.push(V36Sign24Record {
                    code,
                    residual_norm: norms.value(row),
                    dense_ordinal: dense.value(row),
                    source_feature_id: source.value(row),
                });
            }
            V36CoarseFragmentRows::Sign24(rows)
        }
        arm => {
            let width = arm
                .pq_width()
                .ok_or_else(|| invalid("V36 PQ fragment arm differs"))?;
            let mut rows = Vec::with_capacity(batch.num_rows());
            for row in 0..batch.num_rows() {
                let mut code = [0_u8; 48];
                code[..width.code_bytes()].copy_from_slice(codes.value(row));
                rows.push(V36ResidualPq4Record {
                    width,
                    code,
                    dense_ordinal: dense.value(row),
                    source_feature_id: source.value(row),
                });
            }
            V36CoarseFragmentRows::ResidualPq4(rows)
        }
    };
    let identities = validate_v36_coarse_rows(&artifact.context, &rows)?;
    if identities[0].0 != artifact.first_dense_ordinal
        || identities[identities.len() - 1].0 != artifact.last_dense_ordinal
        || identities.len() != artifact.row_count as usize
    {
        return Err(invalid("V36 coarse fragment authority differs"));
    }
    Ok(rows)
}

fn canonical_f32(value: f64) -> Result<f32> {
    let value = value as f32;
    if !value.is_finite() {
        return Err(invalid("V36 residual PQ4 centroid differs"));
    }
    Ok(if value == 0.0 { 0.0 } else { value })
}

fn scan_v36_residual_assignments<S, F>(
    source: &mut S,
    owner_centroids: &[Vec<f32>],
    expected_digest: Option<[u8; 32]>,
    mut visit: F,
) -> Result<(usize, [u8; 32])>
where
    S: V36ResidualAssignmentSource,
    F: FnMut(V36CoarseAssignmentIdentity, &[f64; PROJECTED_DIMENSIONS]) -> Result<()>,
{
    let mut count = 0_usize;
    let mut previous = None;
    let mut digest = blake3::Hasher::new();
    source.scan(&mut |identities, values| {
        if identities.is_empty()
            || values.len()
                != identities
                    .len()
                    .checked_mul(PROJECTED_DIMENSIONS)
                    .ok_or_else(|| invalid("V36 residual assignment block overflows"))?
        {
            return Err(invalid("V36 residual assignment block differs"));
        }
        let (rows, remainder) = values.as_chunks::<PROJECTED_DIMENSIONS>();
        if !remainder.is_empty() {
            return Err(invalid("V36 residual assignment block differs"));
        }
        for (identity, row) in identities.iter().zip(rows) {
            let key = (identity.source_ordinal, identity.owner_posting_ordinal);
            if previous.is_some_and(|prior| prior >= key) || !valid_vector(row) {
                return Err(invalid("V36 residual assignment order differs"));
            }
            let owner = owner_centroids
                .get(
                    usize::try_from(identity.owner_posting_ordinal)
                        .map_err(|_| invalid("V36 posting ordinal overflows"))?,
                )
                .filter(|owner| valid_vector(owner))
                .ok_or_else(|| invalid("V36 residual assignment owner differs"))?;
            digest.update(&identity.source_ordinal.to_le_bytes());
            digest.update(&identity.dense_ordinal.to_le_bytes());
            digest.update(&identity.source_feature_id.to_le_bytes());
            digest.update(&identity.owner_posting_ordinal.to_le_bytes());
            let mut residual = [0.0_f64; PROJECTED_DIMENSIONS];
            for dimension in 0..PROJECTED_DIMENSIONS {
                digest.update(&row[dimension].to_bits().to_le_bytes());
                residual[dimension] = f64::from(row[dimension]) - f64::from(owner[dimension]);
            }
            visit(*identity, &residual)?;
            previous = Some(key);
            count = count
                .checked_add(1)
                .ok_or_else(|| invalid("V36 residual assignment count overflows"))?;
        }
        Ok(())
    })?;
    let actual = *digest.finalize().as_bytes();
    if count == 0 || expected_digest.is_some_and(|expected| expected != actual) {
        return Err(invalid("V36 residual assignment replay differs"));
    }
    Ok((count, actual))
}

#[derive(Clone, Copy)]
struct RepairCandidate {
    distance: f64,
    key: (u64, u32),
    values: [f64; 3],
}

fn repair_precedes(left: &RepairCandidate, right: &RepairCandidate) -> bool {
    left.distance > right.distance || (left.distance == right.distance && left.key < right.key)
}

fn retain_repair_candidate(candidates: &mut Vec<RepairCandidate>, candidate: RepairCandidate) {
    let index = candidates.partition_point(|existing| repair_precedes(existing, &candidate));
    if index < PQ4_CODEWORDS - 1 {
        candidates.insert(index, candidate);
        if candidates.len() == PQ4_CODEWORDS {
            candidates.pop();
        }
    }
}

fn model_distance(
    centroids: &[f32],
    width: V36ResidualPq4Width,
    subquantizer: usize,
    codeword: usize,
    values: &[f64],
) -> f64 {
    let dimensions = width.subvector_dimensions();
    let start = (subquantizer * PQ4_CODEWORDS + codeword) * dimensions;
    pq4_subvector_distance(values, &centroids[start..start + dimensions])
}

fn repair_v36_pq4_empty_clusters(
    subquantizer: usize,
    dimensions: usize,
    sums: &mut [f64],
    counts: &mut [u64],
    repairs: &[Vec<RepairCandidate>],
) -> Result<()> {
    let mut moved = Vec::with_capacity(PQ4_CODEWORDS - 1);
    for empty in 0..PQ4_CODEWORDS {
        let empty_cluster = subquantizer * PQ4_CODEWORDS + empty;
        if counts[empty_cluster] != 0 {
            continue;
        }
        let mut donor = None::<(usize, RepairCandidate)>;
        for codeword in 0..PQ4_CODEWORDS {
            let cluster = subquantizer * PQ4_CODEWORDS + codeword;
            if counts[cluster] <= 1 {
                continue;
            }
            if let Some(candidate) = repairs[cluster]
                .iter()
                .copied()
                .find(|candidate| !moved.contains(&candidate.key))
                && donor.is_none_or(|(_, prior)| repair_precedes(&candidate, &prior))
            {
                donor = Some((cluster, candidate));
            }
        }
        let (donor_cluster, candidate) =
            donor.ok_or_else(|| invalid("V36 residual PQ4 empty repair differs"))?;
        let donor_start = donor_cluster * dimensions;
        let empty_start = empty_cluster * dimensions;
        for component in 0..dimensions {
            sums[donor_start + component] -= candidate.values[component];
        }
        sums[empty_start..empty_start + dimensions]
            .copy_from_slice(&candidate.values[..dimensions]);
        counts[donor_cluster] -= 1;
        counts[empty_cluster] = 1;
        moved.push(candidate.key);
    }
    Ok(())
}

/// Train one residual PQ4 model through replayable bounded assignment scans.
pub fn train_v36_residual_pq4<S: V36ResidualAssignmentSource>(
    source: &mut S,
    owner_centroids: &[Vec<f32>],
    width: V36ResidualPq4Width,
) -> Result<V36ResidualPq4Codebook> {
    if owner_centroids.is_empty() || owner_centroids.iter().any(|owner| !valid_vector(owner)) {
        return Err(invalid("V36 residual PQ4 owner authority differs"));
    }
    let subquantizers = width.subquantizers();
    let dimensions = width.subvector_dimensions();
    let mut centroids = vec![0.0_f32; PQ4_CODEWORDS * PROJECTED_DIMENSIONS];
    let mut selected_keys = vec![Vec::<(u64, u32)>::new(); subquantizers];
    let mut first = None;
    let (assignment_count, replay_digest) =
        scan_v36_residual_assignments(source, owner_centroids, None, |identity, residual| {
            if first.is_none() {
                first = Some((
                    (identity.source_ordinal, identity.owner_posting_ordinal),
                    *residual,
                ));
            }
            Ok(())
        })?;
    if assignment_count < PQ4_CODEWORDS {
        return Err(invalid("V36 residual PQ4 training population differs"));
    }
    let (first_key, first_residual) = first
        .take()
        .ok_or_else(|| invalid("V36 residual PQ4 training population differs"))?;
    for (subquantizer, keys) in selected_keys.iter_mut().enumerate() {
        let start = subquantizer * dimensions;
        let model_start = subquantizer * PQ4_CODEWORDS * dimensions;
        for component in 0..dimensions {
            centroids[model_start + component] = canonical_f32(first_residual[start + component])?;
        }
        keys.push(first_key);
    }

    for codeword in 1..PQ4_CODEWORDS {
        let mut best = vec![None::<RepairCandidate>; subquantizers];
        scan_v36_residual_assignments(
            source,
            owner_centroids,
            Some(replay_digest),
            |identity, residual| {
                let key = (identity.source_ordinal, identity.owner_posting_ordinal);
                for subquantizer in 0..subquantizers {
                    if selected_keys[subquantizer].contains(&key) {
                        continue;
                    }
                    let start = subquantizer * dimensions;
                    let values = &residual[start..start + dimensions];
                    let mut distance = model_distance(&centroids, width, subquantizer, 0, values);
                    for existing in 1..codeword {
                        distance = distance.min(model_distance(
                            &centroids,
                            width,
                            subquantizer,
                            existing,
                            values,
                        ));
                    }
                    let mut candidate_values = [0.0_f64; 3];
                    candidate_values[..dimensions].copy_from_slice(values);
                    let candidate = RepairCandidate {
                        distance,
                        key,
                        values: candidate_values,
                    };
                    if best[subquantizer].is_none_or(|prior| repair_precedes(&candidate, &prior)) {
                        best[subquantizer] = Some(candidate);
                    }
                }
                Ok(())
            },
        )?;
        for subquantizer in 0..subquantizers {
            let selected = best[subquantizer]
                .take()
                .ok_or_else(|| invalid("V36 residual PQ4 seed authority differs"))?;
            let model_start = (subquantizer * PQ4_CODEWORDS + codeword) * dimensions;
            for component in 0..dimensions {
                centroids[model_start + component] = canonical_f32(selected.values[component])?;
            }
            selected_keys[subquantizer].push(selected.key);
        }
    }

    for _ in 0..20 {
        let mut sums = vec![0.0_f64; centroids.len()];
        let mut counts = vec![0_u64; subquantizers * PQ4_CODEWORDS];
        let mut repairs = vec![Vec::<RepairCandidate>::new(); subquantizers * PQ4_CODEWORDS];
        scan_v36_residual_assignments(
            source,
            owner_centroids,
            Some(replay_digest),
            |identity, residual| {
                let key = (identity.source_ordinal, identity.owner_posting_ordinal);
                for subquantizer in 0..subquantizers {
                    let start = subquantizer * dimensions;
                    let values = &residual[start..start + dimensions];
                    let mut best = (
                        model_distance(&centroids, width, subquantizer, 0, values),
                        0,
                    );
                    for codeword in 1..PQ4_CODEWORDS {
                        let distance =
                            model_distance(&centroids, width, subquantizer, codeword, values);
                        if distance < best.0 {
                            best = (distance, codeword);
                        }
                    }
                    let cluster = subquantizer * PQ4_CODEWORDS + best.1;
                    counts[cluster] = counts[cluster]
                        .checked_add(1)
                        .ok_or_else(|| invalid("V36 residual PQ4 cluster count overflows"))?;
                    let sum_start = cluster * dimensions;
                    for component in 0..dimensions {
                        sums[sum_start + component] += values[component];
                    }
                    let mut candidate_values = [0.0_f64; 3];
                    candidate_values[..dimensions].copy_from_slice(values);
                    retain_repair_candidate(
                        &mut repairs[cluster],
                        RepairCandidate {
                            distance: best.0,
                            key,
                            values: candidate_values,
                        },
                    );
                }
                Ok(())
            },
        )?;

        for subquantizer in 0..subquantizers {
            repair_v36_pq4_empty_clusters(
                subquantizer,
                dimensions,
                &mut sums,
                &mut counts,
                &repairs,
            )?;
        }

        for (cluster, count) in counts.iter().copied().enumerate() {
            let start = cluster * dimensions;
            for component in 0..dimensions {
                centroids[start + component] =
                    canonical_f32(sums[start + component] / count as f64)?;
            }
        }
    }
    V36ResidualPq4Codebook::try_new(width, centroids)
}

#[derive(Debug, Clone, Copy)]
struct Candidate {
    distance: f64,
    dense_ordinal: u64,
    source_feature_id: u64,
}

impl PartialEq for Candidate {
    fn eq(&self, other: &Self) -> bool {
        self.distance.to_bits() == other.distance.to_bits()
            && self.dense_ordinal == other.dense_ordinal
            && self.source_feature_id == other.source_feature_id
    }
}

impl Eq for Candidate {}

impl PartialOrd for Candidate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Candidate {
    fn cmp(&self, other: &Self) -> Ordering {
        self.distance
            .total_cmp(&other.distance)
            .then(self.dense_ordinal.cmp(&other.dense_ordinal))
            .then(self.source_feature_id.cmp(&other.source_feature_id))
    }
}

/// O(K) unique-live coarse candidate admission with replica-minimum updates.
pub struct V36UniqueLiveTopK {
    capacity: usize,
    ranked: BTreeSet<Candidate>,
    by_feature: HashMap<u64, Candidate>,
}

impl V36UniqueLiveTopK {
    /// Construct one bounded heap.
    pub fn try_new(capacity: usize) -> Result<Self> {
        if capacity == 0 || capacity > 2_048 {
            return Err(invalid("V36 unique-live K differs"));
        }
        Ok(Self {
            capacity,
            ranked: BTreeSet::new(),
            by_feature: HashMap::with_capacity(capacity),
        })
    }

    /// Offer one scored replica under the frozen liveness snapshot.
    pub fn offer(
        &mut self,
        distance: f64,
        dense_ordinal: u64,
        source_feature_id: u64,
        live: bool,
    ) -> Result<()> {
        if !distance.is_finite() || distance < 0.0 || distance.to_bits() == (-0.0_f64).to_bits() {
            return Err(invalid("V36 unique-live distance differs"));
        }
        if !live {
            return Ok(());
        }
        let candidate = Candidate {
            distance,
            dense_ordinal,
            source_feature_id,
        };
        if let Some(existing) = self.by_feature.get(&source_feature_id).copied() {
            if existing.dense_ordinal != dense_ordinal {
                return Err(invalid("V36 unique-live identity binding differs"));
            }
            if candidate < existing {
                self.ranked.remove(&existing);
                self.ranked.insert(candidate);
                self.by_feature.insert(source_feature_id, candidate);
            }
            return Ok(());
        }
        if self.ranked.len() == self.capacity {
            let worst = *self
                .ranked
                .last()
                .ok_or_else(|| invalid("V36 unique-live heap differs"))?;
            if candidate >= worst {
                return Ok(());
            }
            self.ranked.remove(&worst);
            self.by_feature.remove(&worst.source_feature_id);
        }
        self.ranked.insert(candidate);
        self.by_feature.insert(source_feature_id, candidate);
        Ok(())
    }

    /// Return retained rows in exact ascending coarse rank.
    pub fn ranked(&self) -> Vec<(f64, u64, u64)> {
        self.ranked
            .iter()
            .map(|candidate| {
                (
                    candidate.distance,
                    candidate.dense_ordinal,
                    candidate.source_feature_id,
                )
            })
            .collect()
    }

    /// Number of retained unique live rows.
    pub fn retained_len(&self) -> usize {
        self.ranked.len()
    }

    /// Number of live membership records retained; bounded by K.
    pub fn membership_len(&self) -> usize {
        self.by_feature.len()
    }
}

#[cfg(test)]
mod tests {
    use super::{PQ4_CODEWORDS, RepairCandidate, repair_v36_pq4_empty_clusters};

    #[test]
    fn v36_pq4_empty_repair_moves_farthest_rows_and_updates_donor_sums() {
        let dimensions = 2;
        let mut counts = vec![1_u64; PQ4_CODEWORDS];
        counts[..4].copy_from_slice(&[3, 1, 0, 0]);
        let mut sums = vec![0.0_f64; PQ4_CODEWORDS * dimensions];
        sums[..4].copy_from_slice(&[6.0, 60.0, 5.0, 50.0]);
        let mut repairs = vec![Vec::new(); PQ4_CODEWORDS];
        repairs[0] = vec![
            RepairCandidate {
                distance: 9.0,
                key: (1, 0),
                values: [3.0, 30.0, 0.0],
            },
            RepairCandidate {
                distance: 4.0,
                key: (2, 0),
                values: [2.0, 20.0, 0.0],
            },
            RepairCandidate {
                distance: 1.0,
                key: (3, 0),
                values: [1.0, 10.0, 0.0],
            },
        ];

        repair_v36_pq4_empty_clusters(0, dimensions, &mut sums, &mut counts, &repairs).unwrap();

        assert_eq!(&counts[..4], &[1, 1, 1, 1]);
        assert_eq!(&sums[..8], &[1.0, 10.0, 5.0, 50.0, 3.0, 30.0, 2.0, 20.0]);
    }
}
