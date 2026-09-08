//! Bounded coarse-code identities, controls, and unique-live admission for V36.

use std::{
    cmp::Ordering,
    collections::{BTreeSet, HashMap},
};

use crate::{BorsukError, Result};

const PROJECTED_DIMENSIONS: usize = 192;

fn invalid(message: &'static str) -> BorsukError {
    BorsukError::InvalidStorage(message.to_owned())
}

fn valid_vector(vector: &[f32]) -> bool {
    vector.len() == PROJECTED_DIMENSIONS
        && vector
            .iter()
            .all(|value| value.is_finite() && !(value.to_bits() == (-0.0_f32).to_bits()))
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
        if centroids.len() != 16 * PROJECTED_DIMENSIONS
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
        16 * PROJECTED_DIMENSIONS * size_of::<f32>()
    }

    /// Frozen arm width.
    pub const fn width(&self) -> V36ResidualPq4Width {
        self.width
    }

    fn codeword(&self, subquantizer: usize, codeword: usize) -> &[f32] {
        let dimensions = self.width.subvector_dimensions();
        let start = (subquantizer * 16 + codeword) * dimensions;
        &self.centroids[start..start + dimensions]
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
            pq4_subvector_distance(residual, codebook.codeword(subquantizer, 0)),
            0,
        );
        for codeword in 1..16 {
            let distance =
                pq4_subvector_distance(residual, codebook.codeword(subquantizer, codeword));
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
            codebook.codeword(subquantizer, usize::from(codeword)),
        );
    }
    Ok(distance)
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
