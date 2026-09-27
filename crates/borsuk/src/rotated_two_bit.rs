//! Source-only rotated two-bit codes for precise page nomination.
//!
//! Version 1 records contain packed two-bit levels, a little-endian f32 scale,
//! and the inverse reconstructed vector norm as f32. A generation must bind
//! dimensions, source mean, rotation seed and record bytes in its trusted root.
//! Historical research records stored a different final scalar and are not
//! accepted as this format. These codes nominate pages; SQ8 ranks fetched rows.

use sha2::{Digest, Sha256};

/// Invalid codec geometry, source/query vector or encoded record.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TwoBitError {
    /// Empty or overflowing dimension geometry.
    Geometry,
    /// Wrong width, nonfinite vector or zero reconstructed norm.
    Vector,
    /// Wrong record width or nonfinite/invalid scalar.
    Record,
    /// The admitted query scratch cap is too small or allocation failed.
    MemoryBudget,
}

impl std::fmt::Display for TwoBitError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "rotated two-bit codec: {self:?}")
    }
}

impl std::error::Error for TwoBitError {}

/// Immutable source mean and SHA-derived block rotation; no row cache.
pub struct RotatedTwoBitCodec {
    dimensions: usize,
    mean: Vec<f64>,
    signs: Vec<f64>,
    blocks: Vec<(usize, usize)>,
    packed_bytes: usize,
}

/// One query's byte lookup table. Scoring visits only caller-selected records.
pub struct PreparedTwoBit {
    table: Vec<f64>,
    packed_bytes: usize,
    mean_dot: f64,
    inverse_query_norm: f64,
}

fn hadamard(block: &mut [f64]) {
    let mut width = 1;
    while width < block.len() {
        for first in (0..block.len()).step_by(2 * width) {
            for offset in 0..width {
                let left = block[first + offset];
                let right = block[first + width + offset];
                block[first + offset] = left + right;
                block[first + width + offset] = left - right;
            }
        }
        width *= 2;
    }
    let inverse = 1.0 / (block.len() as f64).sqrt();
    for value in block {
        *value *= inverse;
    }
}

impl RotatedTwoBitCodec {
    /// Bind a source-only mean and seed. Rotate 256-coordinate blocks; pad
    /// only the final block to a power of two. D768 preserves the qualified
    /// three-block transform without dataset-specific routing parameters.
    pub fn new(mean: &[f32], seed: u32) -> Result<Self, TwoBitError> {
        if mean.is_empty() {
            return Err(TwoBitError::Geometry);
        }
        if mean.iter().any(|x| !x.is_finite()) {
            return Err(TwoBitError::Vector);
        }
        let complete = mean.len() / 256 * 256;
        let tail = mean.len() % 256;
        let padded = complete
            .checked_add(if tail == 0 {
                0
            } else {
                tail.next_power_of_two()
            })
            .filter(|&n| n <= u32::MAX as usize)
            .ok_or(TwoBitError::Geometry)?;
        let mut padded_mean = vec![0.0; padded];
        for (target, &source) in padded_mean.iter_mut().zip(mean) {
            *target = f64::from(source);
        }
        let mut signs = Vec::with_capacity(padded);
        for coordinate in 0..padded {
            let mut hash = Sha256::new();
            hash.update(b"borsuk-sq2-sign-v1");
            hash.update(seed.to_le_bytes());
            hash.update((coordinate as u32).to_le_bytes());
            signs.push(if hash.finalize()[0] & 1 == 0 {
                1.0
            } else {
                -1.0
            });
        }
        let mut blocks = (0..complete)
            .step_by(256)
            .map(|first| (first, first + 256))
            .collect::<Vec<_>>();
        if padded > complete {
            blocks.push((complete, padded));
        }
        Ok(Self {
            dimensions: mean.len(),
            mean: padded_mean,
            signs,
            blocks,
            packed_bytes: padded.div_ceil(4),
        })
    }

    /// Original source coordinate width, excluding block padding.
    pub fn dimensions(&self) -> usize {
        self.dimensions
    }

    /// Fixed encoded bytes per row, including both scalar fields.
    pub fn record_bytes(&self) -> usize {
        self.packed_bytes + 8
    }

    fn rotate(&self, values: &mut [f64], inverse: bool) {
        if !inverse {
            for (value, sign) in values.iter_mut().zip(&self.signs) {
                *value *= sign;
            }
        }
        for &(first, end) in &self.blocks {
            hadamard(&mut values[first..end]);
        }
        if inverse {
            for (value, sign) in values.iter_mut().zip(&self.signs) {
                *value *= sign;
            }
        }
    }

    fn validate_vector(&self, vector: &[f32]) -> Result<f64, TwoBitError> {
        if vector.len() != self.dimensions || vector.iter().any(|x| !x.is_finite()) {
            return Err(TwoBitError::Vector);
        }
        let norm2 = vector
            .iter()
            .map(|&x| f64::from(x) * f64::from(x))
            .sum::<f64>();
        if norm2 <= 0.0 || !norm2.is_finite() {
            return Err(TwoBitError::Vector);
        }
        Ok(norm2)
    }

    /// Encode one finite nonzero source row using the fixed eight-step scalar
    /// fit. Scratch is O(dimensions), never proportional to collection size.
    pub fn encode(&self, row: &[f32]) -> Result<Vec<u8>, TwoBitError> {
        self.validate_vector(row)?;
        let mut rotated = vec![0.0; self.mean.len()];
        for coordinate in 0..self.dimensions {
            rotated[coordinate] = f64::from(row[coordinate]) - self.mean[coordinate];
        }
        self.rotate(&mut rotated, false);
        let mut scale = rotated.iter().map(|x| x.abs()).sum::<f64>() / rotated.len() as f64 / 1.6;
        let mut levels = vec![0_i8; rotated.len()];
        for _ in 0..8 {
            let mut numerator = 0.0;
            let mut denominator = 0.0;
            for (level, &value) in levels.iter_mut().zip(&rotated) {
                *level = if value >= 0.0 { 1 } else { -1 }
                    * if value.abs() > 2.0 * scale { 3 } else { 1 };
                numerator += f64::from(*level) * value;
                denominator += f64::from(*level) * f64::from(*level);
            }
            scale = numerator / denominator;
        }
        let scale = scale as f32;
        if !scale.is_finite() || scale < 0.0 {
            return Err(TwoBitError::Vector);
        }
        let mut record = vec![0_u8; self.record_bytes()];
        for (coordinate, &level) in levels.iter().enumerate() {
            record[coordinate / 4] |= ((i16::from(level) + 3) as u8 / 2) << (2 * (coordinate % 4));
        }
        // The final scalar excludes padded coordinates after inverse rotation,
        // so arbitrary dimension tails retain the intended cosine metric.
        for (value, &level) in rotated.iter_mut().zip(&levels) {
            *value = f64::from(level) * f64::from(scale);
        }
        self.rotate(&mut rotated, true);
        let norm2 = rotated[..self.dimensions]
            .iter()
            .zip(&self.mean)
            .map(|(&value, &mean)| (value + mean) * (value + mean))
            .sum::<f64>();
        let inverse_norm = (1.0 / norm2.sqrt()) as f32;
        if !inverse_norm.is_finite() || inverse_norm <= 0.0 {
            return Err(TwoBitError::Vector);
        }
        record[self.packed_bytes..self.packed_bytes + 4].copy_from_slice(&scale.to_le_bytes());
        record[self.packed_bytes + 4..].copy_from_slice(&inverse_norm.to_le_bytes());
        Ok(record)
    }

    /// Rotate a query once and build one four-coordinate lookup per packed
    /// byte. D768 uses 192 lookups per row instead of vector reconstruction.
    /// `max_scratch_bytes` admits the lookup and temporary rotated query;
    /// allocator overhead and immutable codec metadata are charged separately.
    pub fn prepare_query(
        &self,
        query: &[f32],
        max_scratch_bytes: usize,
    ) -> Result<PreparedTwoBit, TwoBitError> {
        let query_norm2 = self.validate_vector(query)?;
        let table_len = self
            .packed_bytes
            .checked_mul(256)
            .ok_or(TwoBitError::Geometry)?;
        let peak_scratch = table_len
            .checked_add(self.mean.len())
            .and_then(|values| values.checked_mul(std::mem::size_of::<f64>()))
            .ok_or(TwoBitError::Geometry)?;
        if peak_scratch > max_scratch_bytes {
            return Err(TwoBitError::MemoryBudget);
        }
        let mean_dot = query
            .iter()
            .zip(&self.mean)
            .map(|(&q, &m)| f64::from(q) * m)
            .sum();
        let mut rotated = Vec::new();
        rotated
            .try_reserve_exact(self.mean.len())
            .map_err(|_| TwoBitError::MemoryBudget)?;
        rotated.resize(self.mean.len(), 0.0);
        for (target, &source) in rotated.iter_mut().zip(query) {
            *target = f64::from(source);
        }
        self.rotate(&mut rotated, false);
        let mut table = Vec::new();
        table
            .try_reserve_exact(table_len)
            .map_err(|_| TwoBitError::MemoryBudget)?;
        table.resize(table_len, 0.0);
        for byte in 0..self.packed_bytes {
            for word in 0..256 {
                let mut dot = 0.0;
                for slot in 0..4 {
                    let coordinate = byte * 4 + slot;
                    if coordinate < rotated.len() {
                        let level = ((word >> (2 * slot)) & 3) as i8 * 2 - 3;
                        dot += f64::from(level) * rotated[coordinate];
                    }
                }
                table[byte * 256 + word] = dot;
            }
        }
        Ok(PreparedTwoBit {
            table,
            packed_bytes: self.packed_bytes,
            mean_dot,
            inverse_query_norm: 1.0 / query_norm2.sqrt(),
        })
    }
}

impl PreparedTwoBit {
    /// Allocated query lookup bytes; charge this per admitted active query.
    pub fn scratch_bytes(&self) -> usize {
        self.table.capacity() * std::mem::size_of::<f64>()
    }

    /// Cosine to one reconstructed row. The caller authenticates the record's
    /// generation and chooses a bounded roster; this method performs no scan.
    pub fn score(&self, record: &[u8]) -> Result<f64, TwoBitError> {
        if record.len() != self.packed_bytes + 8 {
            return Err(TwoBitError::Record);
        }
        let scale = f32::from_le_bytes(
            record[self.packed_bytes..self.packed_bytes + 4]
                .try_into()
                .unwrap(),
        );
        let inverse_norm = f32::from_le_bytes(record[self.packed_bytes + 4..].try_into().unwrap());
        if !scale.is_finite() || scale < 0.0 || !inverse_norm.is_finite() || inverse_norm <= 0.0 {
            return Err(TwoBitError::Record);
        }
        let dot = record[..self.packed_bytes]
            .iter()
            .enumerate()
            .map(|(byte, &word)| self.table[byte * 256 + usize::from(word)])
            .sum::<f64>();
        let score = (self.mean_dot + f64::from(scale) * dot)
            * self.inverse_query_norm
            * f64::from(inverse_norm);
        if !score.is_finite() {
            return Err(TwoBitError::Record);
        }
        Ok(score)
    }
}
