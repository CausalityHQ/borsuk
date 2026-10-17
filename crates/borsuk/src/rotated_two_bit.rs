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

#[derive(Clone, Copy)]
struct ValidatedTwoBitRow<'a> {
    packed: &'a [u8],
    scale: f32,
    inverse_norm: f32,
}

/// One SOURCE unit; output reuses the accumulation lanes without a caller buffer.
pub(crate) struct TwoBitScoreBlock<'q, 'r> {
    prepared: &'q PreparedTwoBit,
    rows: [ValidatedTwoBitRow<'r>; 32],
    scores: [f64; 32],
    len: usize,
}

impl<'q, 'r> TwoBitScoreBlock<'q, 'r> {
    pub(crate) fn new(prepared: &'q PreparedTwoBit) -> Self {
        Self {
            prepared,
            rows: [ValidatedTwoBitRow {
                packed: &[],
                scale: 0.0,
                inverse_norm: 1.0,
            }; 32],
            scores: [-0.0; 32],
            len: 0,
        }
    }

    /// Checked simultaneous payload, including pending validation and caller controls.
    pub(crate) fn scratch_bytes() -> Option<usize> {
        // Block: row slices/scalars, shared dot/output lanes, prepared borrow and len.
        // Then: pending row/scalars, finalization, six indices, two record slices,
        // returned score slice and caller maximum; compiler/RSS overhead is separate.
        std::mem::size_of::<Self>()
            .checked_add(std::mem::size_of::<ValidatedTwoBitRow<'_>>())?
            .checked_add(std::mem::size_of::<[f32; 2]>())?
            .checked_add(std::mem::size_of::<[f64; 3]>())?
            .checked_add(std::mem::size_of::<[usize; 6]>())?
            .checked_add(std::mem::size_of::<[&[u8]; 2]>())?
            .checked_add(std::mem::size_of::<&[f64]>())?
            .checked_add(std::mem::size_of::<f64>())
    }

    /// Finish width/scalar validation before the caller requests another record.
    pub(crate) fn push(&mut self, record: &'r [u8]) -> Result<(), TwoBitError> {
        if self.len == self.rows.len() {
            return Err(TwoBitError::Geometry);
        }
        let packed_bytes = self.prepared.packed_bytes;
        if record.len() != packed_bytes + 8 {
            return Err(TwoBitError::Record);
        }
        let scale = f32::from_le_bytes(record[packed_bytes..packed_bytes + 4].try_into().unwrap());
        let inverse_norm = f32::from_le_bytes(record[packed_bytes + 4..].try_into().unwrap());
        if !scale.is_finite() || scale < 0.0 || !inverse_norm.is_finite() || inverse_norm <= 0.0 {
            return Err(TwoBitError::Record);
        }
        self.rows[self.len] = ValidatedTwoBitRow {
            packed: &record[..packed_bytes],
            scale,
            inverse_norm,
        };
        self.len += 1;
        Ok(())
    }

    pub(crate) fn score(&mut self) -> &[f64] {
        let rows = &self.rows[..self.len];
        let scores = &mut self.scores[..self.len];
        // Match f64::Sum's signed identity and each row's original byte order.
        scores.fill(-0.0);
        for byte in 0..self.prepared.packed_bytes {
            let table = &self.prepared.table[byte * 256..(byte + 1) * 256];
            for (row, dot) in rows.iter().zip(scores.iter_mut()) {
                *dot += table[usize::from(row.packed[byte])];
            }
        }
        for (row, dot) in rows.iter().zip(scores.iter_mut()) {
            *dot = (self.prepared.mean_dot + f64::from(row.scale) * *dot)
                * self.prepared.inverse_query_norm
                * f64::from(row.inverse_norm);
            // Checked D<=2^32, finite f32 inputs and <=256-coordinate rotations
            // bound |dot|<2^172, |mean_dot|<2^290, inverse query norm<2^150,
            // and every final score<2^580: validation leaves no f64 failure.
            debug_assert!(dot.is_finite());
        }
        scores
    }
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
    /// Includes the packed-byte lookup table and temporary rotated query:
    /// `(ceil(padded_dimensions / 4) * 256 + padded_dimensions) * size_of::<f64>()`.
    /// Uses the constructor's block padding; rejects invalid geometry or overflow.
    /// Allocator overhead and immutable codec metadata are charged separately.
    pub fn required_query_scratch_bytes(dimensions: usize) -> Result<usize, TwoBitError> {
        let padded = Self::padded_dimensions(dimensions)?;
        padded
            .div_ceil(4)
            .checked_mul(256)
            .and_then(|values| values.checked_add(padded))
            .and_then(|values| values.checked_mul(std::mem::size_of::<f64>()))
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

#[cfg(test)]
mod tests {
    use super::*;

    fn block_fixture(dimensions: usize, domain: usize) -> (PreparedTwoBit, Vec<Vec<u8>>) {
        let mean = (0..dimensions)
            .map(|i| match domain {
                1 => {
                    if i % 2 == 0 {
                        f32::MAX
                    } else {
                        -f32::MAX
                    }
                }
                _ => 0.0,
            })
            .collect::<Vec<_>>();
        let query = (0..dimensions)
            .map(|i| match domain {
                1 => {
                    if i % 2 == 0 {
                        f32::MAX
                    } else {
                        -f32::MAX
                    }
                }
                2 => {
                    if i == 0 {
                        f32::from_bits(1)
                    } else {
                        -0.0
                    }
                }
                _ => ((i * 13 % 29) as f32 - 14.0) / 7.0,
            })
            .collect::<Vec<_>>();
        let codec = RotatedTwoBitCodec::new(&mean, 17).unwrap();
        let prepared = codec
            .prepare_query(
                &query,
                RotatedTwoBitCodec::required_query_scratch_bytes(dimensions).unwrap(),
            )
            .unwrap();
        let scales = [
            0.0_f32,
            -0.0,
            f32::from_bits(1),
            f32::MIN_POSITIVE,
            1.0,
            f32::MAX,
        ];
        let norms = [f32::from_bits(1), f32::MIN_POSITIVE, 1.0, f32::MAX];
        let records = (0..32)
            .map(|lane| {
                let mut record = vec![0; codec.record_bytes()];
                for (byte, word) in record[..prepared.packed_bytes].iter_mut().enumerate() {
                    *word = (lane * 73 + byte * 29) as u8;
                }
                record[prepared.packed_bytes..prepared.packed_bytes + 4]
                    .copy_from_slice(&scales[lane % scales.len()].to_le_bytes());
                record[prepared.packed_bytes + 4..]
                    .copy_from_slice(&norms[lane % norms.len()].to_le_bytes());
                record
            })
            .collect();
        (prepared, records)
    }

    #[test]
    fn source_block32_scalar_bit_parity_dimensions_batches_and_extreme_scalars() {
        for dimensions in [1, 5, 257, 768, 1000, 1024] {
            for domain in 0..3 {
                let (prepared, records) = block_fixture(dimensions, domain);
                for batch in 1..=32 {
                    for chunk in records.chunks(batch) {
                        let mut block = TwoBitScoreBlock::new(&prepared);
                        for record in chunk {
                            block.push(record).unwrap();
                        }
                        let actual = block.score();
                        assert_eq!(actual.len(), chunk.len());
                        for (lane, (record, &score)) in chunk.iter().zip(actual).enumerate() {
                            assert_eq!(
                                score.to_bits(),
                                prepared.score(record).unwrap().to_bits(),
                                "D{dimensions} domain{domain} batch{batch} lane{lane}"
                            );
                        }
                        // Reusing the block must reset every accumulator.
                        let again = block
                            .score()
                            .iter()
                            .map(|x| x.to_bits())
                            .collect::<Vec<_>>();
                        let expected = chunk
                            .iter()
                            .map(|r| prepared.score(r).unwrap().to_bits())
                            .collect::<Vec<_>>();
                        assert_eq!(again, expected);
                    }
                }
            }
        }
    }

    #[test]
    fn source_block32_preserves_sum_signed_zero_and_cancellation_order() {
        for entries in [vec![-0.0], vec![1e16, 1.0, -1e16, 1.0]] {
            let prepared = PreparedTwoBit {
                table: entries.iter().flat_map(|&value| [value; 256]).collect(),
                packed_bytes: entries.len(),
                mean_dot: -0.0,
                inverse_query_norm: 1.0,
            };
            let mut record = vec![0; entries.len()];
            record.extend_from_slice(&1.0_f32.to_le_bytes());
            record.extend_from_slice(&1.0_f32.to_le_bytes());
            let expected = prepared.score(&record).unwrap();
            assert_eq!(
                expected.to_bits(),
                if entries.len() == 1 {
                    (-0.0_f64).to_bits()
                } else {
                    1.0_f64.to_bits()
                }
            );
            let mut block = TwoBitScoreBlock::new(&prepared);
            for _ in 0..32 {
                block.push(&record).unwrap();
            }
            assert!(
                block
                    .score()
                    .iter()
                    .all(|x| x.to_bits() == expected.to_bits())
            );
            assert_eq!(block.push(&record), Err(TwoBitError::Geometry));
        }
    }

    #[test]
    fn source_block32_checked_scratch_matches_simultaneous_payload() {
        let expected = std::mem::size_of::<TwoBitScoreBlock<'_, '_>>()
            + std::mem::size_of::<ValidatedTwoBitRow<'_>>()
            + std::mem::size_of::<[f32; 2]>()
            + std::mem::size_of::<[f64; 3]>()
            + std::mem::size_of::<[usize; 6]>()
            + std::mem::size_of::<[&[u8]; 2]>()
            + std::mem::size_of::<&[f64]>()
            + std::mem::size_of::<f64>();
        assert_eq!(TwoBitScoreBlock::scratch_bytes(), Some(expected));
        if usize::BITS == 64 {
            assert_eq!(expected, 1200);
        }
    }

    #[test]
    #[ignore = "bounded release primitive; root runs explicitly with --release --ignored --nocapture"]
    fn source_block32_release_primitive() {
        use std::{hint::black_box, time::Instant};
        assert!(!cfg!(debug_assertions), "run this primitive with --release");
        const UNIT_ROSTER: usize = 256;
        const MIN_ROWS_PER_REPETITION: usize = 524_288;
        const REPETITIONS: usize = 8;
        for dimensions in [257, 768, 1000, 1024] {
            let (prepared, records) = block_fixture(dimensions, 0);
            let roster = (0..UNIT_ROSTER)
                .map(|unit| {
                    records
                        .iter()
                        .enumerate()
                        .map(|(lane, record)| {
                            let mut record = record.clone();
                            for (byte, word) in
                                record[..prepared.packed_bytes].iter_mut().enumerate()
                            {
                                *word = (unit + lane * 73 + byte * 29) as u8;
                            }
                            record
                        })
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>();
            for byte in 0..prepared.packed_bytes {
                let mut seen = [false; 256];
                for unit in &roster {
                    seen[usize::from(unit[0][byte])] = true;
                }
                assert!(seen.into_iter().all(|present| present));
            }
            for count in [1, 7, 32] {
                let iterations = MIN_ROWS_PER_REPETITION.div_ceil(UNIT_ROSTER * count);
                for unit in &roster {
                    let records = &unit[..count];
                    let mut block = TwoBitScoreBlock::new(&prepared);
                    for record in records {
                        block.push(record).unwrap();
                    }
                    for (record, actual) in records.iter().zip(block.score()) {
                        assert_eq!(prepared.score(record).unwrap().to_bits(), actual.to_bits());
                    }
                }
                let scalar = || {
                    let started = Instant::now();
                    for _ in 0..iterations {
                        for unit in &roster {
                            for record in &unit[..count] {
                                black_box(black_box(&prepared).score(black_box(record)).unwrap());
                            }
                        }
                    }
                    started.elapsed().as_nanos()
                };
                let blocked = || {
                    let started = Instant::now();
                    for _ in 0..iterations {
                        for unit in &roster {
                            let mut block = TwoBitScoreBlock::new(black_box(&prepared));
                            for record in &unit[..count] {
                                block.push(black_box(record)).unwrap();
                            }
                            black_box(block.score());
                        }
                    }
                    started.elapsed().as_nanos()
                };
                let mut scalar_ns = Vec::new();
                let mut block_ns = Vec::new();
                for repetition in 0..REPETITIONS {
                    if repetition % 2 == 0 {
                        scalar_ns.push(scalar());
                        block_ns.push(blocked());
                    } else {
                        block_ns.push(blocked());
                        scalar_ns.push(scalar());
                    }
                }
                println!(
                    "{}",
                    serde_json::json!({
                        "kind": "source_block32_primitive",
                        "production_uses_block": count == 32,
                        "dimensions": dimensions,
                        "rows": count,
                        "iterations": iterations,
                        "units_per_iteration": UNIT_ROSTER,
                        "rows_per_repetition": iterations * UNIT_ROSTER * count,
                        "packed_values_per_byte": 256,
                        "repetitions": REPETITIONS,
                        "scalar_ns": scalar_ns,
                        "block_ns": block_ns
                    })
                );
            }
        }
    }

    #[test]
    fn required_query_scratch_matches_exact_prepare_query_budget() {
        for (dimensions, expected) in [(1, 2056), (5, 4160), (768, 399_360), (1024, 532_480)] {
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
