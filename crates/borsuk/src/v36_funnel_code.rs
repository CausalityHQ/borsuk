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
