//! Bounded coarse-code identities, controls, and unique-live admission for V36.

use std::{
    cmp::Ordering,
    collections::{BTreeSet, HashMap},
};

use crate::{BorsukError, Result};

const PROJECTED_DIMENSIONS: usize = 192;
const PQ4_CODEWORDS: usize = 16;

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
