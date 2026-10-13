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

pub(crate) const SCORE_BATCH_ROWS: usize = 8;
const SCORE_BATCH_SCRATCH_BYTES: usize =
    SCORE_BATCH_ROWS * (std::mem::size_of::<&[u8]>() + 4 * std::mem::size_of::<f64>());

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
        let padded = Self::padded_dimensions(mean.len())?;
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

    pub(crate) fn padded_dimensions(dimensions: usize) -> Result<usize, TwoBitError> {
        if dimensions == 0 {
            return Err(TwoBitError::Geometry);
        }
        let complete = dimensions / 256 * 256;
        let tail = dimensions % 256;
        complete
            .checked_add(if tail == 0 {
                0
            } else {
                tail.next_power_of_two()
            })
            .filter(|&n| n <= u32::MAX as usize)
            .ok_or(TwoBitError::Geometry)
    }

    /// Required peak query scratch bytes for a source dimension, without allocating.
    /// Includes the lookup table plus the larger of temporary rotation and
    /// eight-row scoring buffers; preparation and scoring do not overlap.
    /// Uses the constructor's block padding; rejects invalid geometry or overflow.
    /// Allocator overhead and immutable codec metadata are charged separately.
    pub fn required_query_scratch_bytes(dimensions: usize) -> Result<usize, TwoBitError> {
        let padded = Self::padded_dimensions(dimensions)?;
        let table_bytes = padded
            .div_ceil(4)
            .checked_mul(256)
            .and_then(|values| values.checked_mul(std::mem::size_of::<f64>()))
            .ok_or(TwoBitError::Geometry)?;
        let rotated_bytes = padded
            .checked_mul(std::mem::size_of::<f64>())
            .ok_or(TwoBitError::Geometry)?;
        table_bytes
            .checked_add(rotated_bytes.max(SCORE_BATCH_SCRATCH_BYTES))
            .ok_or(TwoBitError::Geometry)
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
        let peak_scratch = Self::required_query_scratch_bytes(self.dimensions)?;
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
        let (scale, inverse_norm) = self.record_scalars(record)?;
        let dot = record[..self.packed_bytes]
            .iter()
            .enumerate()
            .map(|(byte, &word)| self.table[byte * 256 + usize::from(word)])
            .sum::<f64>();
        let score = (self.mean_dot + scale * dot) * self.inverse_query_norm * inverse_norm;
        if !score.is_finite() {
            return Err(TwoBitError::Record);
        }
        Ok(score)
    }

    pub(crate) fn record_scalars(&self, record: &[u8]) -> Result<(f64, f64), TwoBitError> {
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
        Ok((f64::from(scale), f64::from(inverse_norm)))
    }

    /// Score zero to eight caller-selected, authenticated records without allocation.
    /// Output width must match input; errors leave output untouched. Each row keeps
    /// scalar byte-order accumulation while neighboring rows reuse a LUT bank.
    /// Query preparation admits the fixed batch buffers in its scratch bound.
    pub fn score_batch(&self, records: &[&[u8]], scores: &mut [f64]) -> Result<(), TwoBitError> {
        if records.len() > SCORE_BATCH_ROWS {
            return Err(TwoBitError::MemoryBudget);
        }
        if records.len() != scores.len() {
            return Err(TwoBitError::Geometry);
        }
        if records.is_empty() {
            return Ok(());
        }
        let mut scales = [0.0; SCORE_BATCH_ROWS];
        let mut inverse_norms = [0.0; SCORE_BATCH_ROWS];
        for (lane, record) in records.iter().enumerate() {
            (scales[lane], inverse_norms[lane]) = self.record_scalars(record)?;
        }
        // Match f64::Sum's identity, including an all-negative-zero row.
        let mut dots = [-0.0; SCORE_BATCH_ROWS];
        for byte in 0..self.packed_bytes {
            let bank: &[f64; 256] = self.table[byte * 256..(byte + 1) * 256].try_into().unwrap();
            for (lane, record) in records.iter().enumerate() {
                dots[lane] += bank[usize::from(record[byte])];
            }
        }
        for lane in 0..records.len() {
            dots[lane] = (self.mean_dot + scales[lane] * dots[lane])
                * self.inverse_query_norm
                * inverse_norms[lane];
            if !dots[lane].is_finite() {
                return Err(TwoBitError::Record);
            }
        }
        scores.copy_from_slice(&dots[..records.len()]);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn score_record(words: &[u8], scale: f32, inverse_norm: f32) -> Vec<u8> {
        let mut record = words.to_vec();
        record.extend_from_slice(&scale.to_le_bytes());
        record.extend_from_slice(&inverse_norm.to_le_bytes());
        record
    }

    #[test]
    fn batched_scores_match_scalar_bits_for_generic_geometry_and_partial_widths() {
        for dimensions in [
            1, 2, 3, 4, 5, 31, 32, 63, 64, 255, 256, 257, 511, 512, 768, 1024, 1536,
        ] {
            let codec = RotatedTwoBitCodec::new(&vec![0.0; dimensions], 7).unwrap();
            let query = (0..dimensions)
                .map(|i| (i % 11 + 1) as f32 / 11.0)
                .collect::<Vec<_>>();
            let prepared = codec.prepare_query(&query, usize::MAX).unwrap();
            let records = (0..8)
                .map(|row| {
                    let vector = (0..dimensions)
                        .map(|i| ((i * 13 + row * 17) % 31) as f32 / 16.0 - 15.0 / 16.0)
                        .collect::<Vec<_>>();
                    codec.encode(&vector).unwrap()
                })
                .collect::<Vec<_>>();
            for count in 0..=8 {
                let refs = records[..count]
                    .iter()
                    .map(Vec::as_slice)
                    .collect::<Vec<_>>();
                let mut scores = vec![123.0; count];
                prepared.score_batch(&refs, &mut scores).unwrap();
                for (record, score) in refs.iter().zip(&scores) {
                    assert_eq!(
                        score.to_bits(),
                        prepared.score(record).unwrap().to_bits(),
                        "D={dimensions}, count={count}"
                    );
                }
            }
        }
    }

    #[test]
    fn batched_scores_preserve_ordered_cancellation_and_signed_zero() {
        let mut table = vec![0.0; 4 * 256];
        for (bank, value) in [1e16, 1.0, -1e16, 1.0].into_iter().enumerate() {
            table[bank * 256..(bank + 1) * 256].fill(value);
        }
        let prepared = PreparedTwoBit {
            table,
            packed_bytes: 4,
            mean_dot: 0.0,
            inverse_query_norm: 1.0,
        };
        let record = score_record(&[0; 4], 1.0, 1.0);
        let mut scores = [0.0; 8];
        prepared
            .score_batch(&[record.as_slice(); 8], &mut scores)
            .unwrap();
        assert!(
            scores
                .iter()
                .all(|score| score.to_bits() == 1.0_f64.to_bits())
        );
        let prepared = PreparedTwoBit {
            table: vec![-0.0; 256],
            packed_bytes: 1,
            mean_dot: -0.0,
            inverse_query_norm: 1.0,
        };
        let record = score_record(&[0], 1.0, 1.0);
        prepared
            .score_batch(&[record.as_slice(); 8], &mut scores)
            .unwrap();
        assert!(
            scores
                .iter()
                .all(|score| score.to_bits() == 0x8000_0000_0000_0000)
        );

        // Independently hand-computed footer/word results, not a common parser oracle.
        let mut table = vec![0.0; 256];
        table[0] = 1.0;
        table[1] = -2.0;
        let prepared = PreparedTwoBit {
            table,
            packed_bytes: 1,
            mean_dot: 0.25,
            inverse_query_norm: 0.5,
        };
        let a = score_record(&[0], 2.0, 0.5);
        let b = score_record(&[1], 2.0, 0.5);
        prepared.score_batch(&[&a, &b], &mut scores[..2]).unwrap();
        assert_eq!(scores[0].to_bits(), 0.5625_f64.to_bits());
        assert_eq!(scores[1].to_bits(), (-0.9375_f64).to_bits());
    }

    #[test]
    fn batched_scores_refuse_shape_and_invalid_records_without_output_mutation() {
        let codec = RotatedTwoBitCodec::new(&[0.0], 7).unwrap();
        let prepared = codec.prepare_query(&[1.0], usize::MAX).unwrap();
        let good = codec.encode(&[2.0]).unwrap();
        let mut out = [123.0; 9];
        assert!(matches!(
            prepared.score_batch(&[good.as_slice(); 9], &mut out),
            Err(TwoBitError::MemoryBudget)
        ));
        assert!(out.iter().all(|value| *value == 123.0));
        assert!(matches!(
            prepared.score_batch(&[&good], &mut out[..0]),
            Err(TwoBitError::Geometry)
        ));
        for slot in 0..8 {
            let mut invalid = vec![good[..good.len() - 1].to_vec(), {
                let mut v = good.clone();
                v.push(0);
                v
            }];
            for (scale, inverse_norm) in [
                (f32::NAN, 1.0),
                (f32::INFINITY, 1.0),
                (-1.0, 1.0),
                (1.0, 0.0),
                (1.0, -1.0),
                (1.0, f32::NAN),
                (1.0, f32::INFINITY),
            ] {
                invalid.push(score_record(&good[..1], scale, inverse_norm));
            }
            for bad in &invalid {
                let mut refs = [good.as_slice(); 8];
                refs[slot] = bad;
                let mut scores = [123.0; 8];
                assert!(matches!(
                    prepared.score_batch(&refs, &mut scores),
                    Err(TwoBitError::Record)
                ));
                assert_eq!(scores, [123.0; 8]);
            }
        }
    }

    #[test]
    fn batch_query_scratch_refuses_unmodeled_small_dimension_buffers() {
        // One-byte LUT =2048B; eight borrowed slices and four f64 lane arrays
        // need384B on64-bit, exceeding the old8B temporary rotation buffer.
        let codec = RotatedTwoBitCodec::new(&[0.0], 7).unwrap();
        let query = [1.0];
        if usize::BITS == 64 {
            assert!(matches!(
                codec.prepare_query(&query, 2056),
                Err(TwoBitError::MemoryBudget)
            ));
            assert!(codec.prepare_query(&query, 2431).is_err());
            assert!(codec.prepare_query(&query, 2432).is_ok());
        }
    }

    #[test]
    fn required_query_scratch_matches_exact_prepare_query_budget() {
        let (small, padded_small) = if usize::BITS == 64 {
            (2432, 4480)
        } else {
            (2368, 4416)
        };
        for (dimensions, expected) in [
            (1, small),
            (5, padded_small),
            (768, 399_360),
            (1024, 532_480),
        ] {
            let required = RotatedTwoBitCodec::required_query_scratch_bytes(dimensions).unwrap();
            assert_eq!(required, expected);
            let codec = RotatedTwoBitCodec::new(&vec![0.0; dimensions], 7).unwrap();
            let query = vec![1.0; dimensions];
            assert!(codec.prepare_query(&query, required).is_ok());
            assert!(matches!(
                codec.prepare_query(&query, required - 1),
                Err(TwoBitError::MemoryBudget)
            ));
        }
    }

    #[test]
    fn required_query_scratch_rejects_invalid_or_overflowing_geometry() {
        for dimensions in [0, u32::MAX as usize, usize::MAX] {
            assert_eq!(
                RotatedTwoBitCodec::required_query_scratch_bytes(dimensions),
                Err(TwoBitError::Geometry)
            );
        }
        if let Some(dimensions) = (u32::MAX as usize).checked_add(1) {
            assert_eq!(
                RotatedTwoBitCodec::required_query_scratch_bytes(dimensions),
                Err(TwoBitError::Geometry)
            );
        }
        // Valid padding can still overflow the scratch calculation on 32-bit hosts.
        if usize::BITS == 32 {
            assert_eq!(
                RotatedTwoBitCodec::required_query_scratch_bytes(u32::MAX as usize - 255),
                Err(TwoBitError::Geometry)
            );
        }
    }
}
