//! Versioned f16 unit centroids for score-first physical page selection.

use half::f16;
use half::slice::{HalfBitsSliceExt, HalfFloatSliceExt};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::io::Read;

const MAGIC: &[u8; 8] = b"BORSUCP1";
const HEADER_BYTES: usize = 32;

fn centroid_half(value: f64) -> f16 {
    // half 2.7's portable converter discards the low 32 mantissa bits. Retain
    // their sticky bit so values just beyond a tie still round correctly.
    let bits = value.to_bits();
    let sticky = u64::from(bits as u32 != 0) << 32;
    f16::from_f64_const(f64::from_bits(bits | sticky))
}

#[derive(Debug)]
pub enum UnitCentroidError {
    InvalidGeometry,
    InvalidCoefficients,
    InvalidPayload,
    InvalidQuery,
    ArithmeticOverflow,
    Io(std::io::Error),
}

impl std::fmt::Display for UnitCentroidError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "unit centroid I/O error: {error}"),
            other => write!(formatter, "unit centroid error: {other:?}"),
        }
    }
}

impl std::error::Error for UnitCentroidError {}

impl From<std::io::Error> for UnitCentroidError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

#[derive(Clone, Debug)]
pub struct UnitCentroidPages {
    rows: usize,
    dimensions: usize,
    unit_rows: usize,
    page_rows: usize,
    centers: Vec<f32>,
    center_norms: Vec<f32>,
    blob_sha256: [u8; 32],
}

fn geometry(
    rows: usize,
    dimensions: usize,
    unit_rows: usize,
    page_rows: usize,
) -> Result<(usize, usize, usize), UnitCentroidError> {
    if rows == 0
        || dimensions == 0
        || unit_rows == 0
        || page_rows == 0
        || page_rows % unit_rows != 0
    {
        return Err(UnitCentroidError::InvalidGeometry);
    }
    let unit_count = rows.div_ceil(unit_rows);
    let page_count = rows.div_ceil(page_rows);
    let payload_bytes = unit_count
        .checked_mul(dimensions)
        .and_then(|count| count.checked_mul(2))
        .ok_or(UnitCentroidError::ArithmeticOverflow)?;
    let row_bytes = dimensions
        .checked_add(12)
        .ok_or(UnitCentroidError::ArithmeticOverflow)?;
    rows.checked_mul(row_bytes)
        .ok_or(UnitCentroidError::ArithmeticOverflow)?;
    payload_bytes
        .checked_add(HEADER_BYTES)
        .ok_or(UnitCentroidError::ArithmeticOverflow)?;
    Ok((unit_count, page_count, payload_bytes))
}

impl UnitCentroidPages {
    /// Build a versioned centroid plane from the source-order SQ8 stream.
    /// Each row is LE i64 ID, LE f32 norm, followed by `dimensions` codes.
    /// Build-time I/O is sequential and does not retain the SQ8 plane.
    pub fn build_from_sq8_reader<R: Read>(
        source: &mut R,
        rows: usize,
        dimensions: usize,
        unit_rows: usize,
        page_rows: usize,
        low: &[f32],
        step: &[f32],
    ) -> Result<Vec<u8>, UnitCentroidError> {
        let (unit_count, _, payload_bytes) = geometry(rows, dimensions, unit_rows, page_rows)?;
        if low.len() != dimensions
            || step.len() != dimensions
            || low.iter().any(|value| !value.is_finite())
            || step.iter().any(|value| !value.is_finite() || *value <= 0.0)
        {
            return Err(UnitCentroidError::InvalidCoefficients);
        }
        let mut output = Vec::with_capacity(HEADER_BYTES + payload_bytes);
        output.extend_from_slice(MAGIC);
        output.extend_from_slice(
            &u64::try_from(rows)
                .map_err(|_| UnitCentroidError::ArithmeticOverflow)?
                .to_le_bytes(),
        );
        for value in [dimensions, unit_rows, page_rows] {
            output.extend_from_slice(
                &u32::try_from(value)
                    .map_err(|_| UnitCentroidError::ArithmeticOverflow)?
                    .to_le_bytes(),
            );
        }
        output.extend_from_slice(&0u32.to_le_bytes());
        let mut row = vec![0u8; dimensions + 12];
        let mut sums = vec![0.0f64; dimensions];
        for unit in 0..unit_count {
            sums.fill(0.0);
            let count = (rows - unit * unit_rows).min(unit_rows);
            for _ in 0..count {
                source.read_exact(&mut row)?;
                for dimension in 0..dimensions {
                    let restored =
                        low[dimension] + f32::from(row[12 + dimension]) * step[dimension];
                    if !restored.is_finite() {
                        return Err(UnitCentroidError::InvalidCoefficients);
                    }
                    sums[dimension] += f64::from(restored);
                }
            }
            for &sum in &sums {
                let half = centroid_half(sum / count as f64);
                if !half.is_finite() {
                    return Err(UnitCentroidError::InvalidCoefficients);
                }
                output.extend_from_slice(&half.to_bits().to_le_bytes());
            }
        }
        let mut extra = [0u8; 1];
        if source.read(&mut extra)? != 0 {
            return Err(UnitCentroidError::InvalidPayload);
        }
        Ok(output)
    }

    /// Decode the current format only. A generation manifest must authenticate
    /// these bytes before use; this local parser checks format and geometry.
    pub fn decode(bytes: &[u8]) -> Result<Self, UnitCentroidError> {
        if bytes.len() < HEADER_BYTES || &bytes[..8] != MAGIC || bytes[28..32] != [0; 4] {
            return Err(UnitCentroidError::InvalidPayload);
        }
        let rows = usize::try_from(u64::from_le_bytes(bytes[8..16].try_into().unwrap()))
            .map_err(|_| UnitCentroidError::ArithmeticOverflow)?;
        let dimensions = u32::from_le_bytes(bytes[16..20].try_into().unwrap()) as usize;
        let unit_rows = u32::from_le_bytes(bytes[20..24].try_into().unwrap()) as usize;
        let page_rows = u32::from_le_bytes(bytes[24..28].try_into().unwrap()) as usize;
        let (unit_count, _, payload_bytes) = geometry(rows, dimensions, unit_rows, page_rows)?;
        if bytes.len() != HEADER_BYTES + payload_bytes {
            return Err(UnitCentroidError::InvalidPayload);
        }
        let mut centers = vec![0.0; payload_bytes / 2];
        let mut bits = [0u16; 1024];
        for (input, output) in bytes[HEADER_BYTES..]
            .chunks(bits.len() * 2)
            .zip(centers.chunks_mut(bits.len()))
        {
            for (pair, value) in input.chunks_exact(2).zip(&mut bits) {
                *value = u16::from_le_bytes(pair.try_into().unwrap());
            }
            bits[..output.len()]
                .reinterpret_cast::<f16>()
                .convert_to_f32_slice(output);
        }
        if centers.iter().any(|value| !value.is_finite()) {
            return Err(UnitCentroidError::InvalidPayload);
        }
        let mut center_norms = Vec::with_capacity(unit_count);
        for center in centers.chunks_exact(dimensions) {
            let norm = center.iter().map(|value| value * value).sum::<f32>();
            if !norm.is_finite() {
                return Err(UnitCentroidError::InvalidPayload);
            }
            center_norms.push(norm);
        }
        Ok(Self {
            rows,
            dimensions,
            unit_rows,
            page_rows,
            centers,
            center_norms,
            blob_sha256: Sha256::digest(bytes).into(),
        })
    }

    pub fn rows(&self) -> usize {
        self.rows
    }

    pub fn blob_sha256(&self) -> &[u8; 32] {
        &self.blob_sha256
    }

    pub fn dimensions(&self) -> usize {
        self.dimensions
    }

    pub fn unit_rows(&self) -> usize {
        self.unit_rows
    }

    pub fn page_rows(&self) -> usize {
        self.page_rows
    }

    /// Logical resident array bytes, excluding allocator overhead and cache.
    pub fn resident_array_bytes(&self) -> usize {
        self.centers.len() * size_of::<f32>() + self.center_norms.len() * size_of::<f32>()
    }

    pub fn unit_count(&self) -> usize {
        self.center_norms.len()
    }

    pub fn unit_centroid(&self, unit: usize) -> Option<&[f32]> {
        if unit >= self.unit_count() {
            return None;
        }
        let start = unit * self.dimensions;
        Some(&self.centers[start..start + self.dimensions])
    }

    fn query_norm(&self, query: &[f32]) -> Result<f32, UnitCentroidError> {
        if query.len() != self.dimensions || query.iter().any(|value| !value.is_finite()) {
            return Err(UnitCentroidError::InvalidQuery);
        }
        let norm = query.iter().map(|value| value * value).sum::<f32>();
        if !norm.is_finite() {
            return Err(UnitCentroidError::InvalidQuery);
        }
        Ok(norm)
    }

    fn unit_distance(&self, query: &[f32], query_norm: f32, unit: usize) -> f32 {
        let center = self.unit_centroid(unit).expect("valid centroid unit");
        let mut lanes = [0.0f32; 8];
        let mut chunks = query.chunks_exact(8).zip(center.chunks_exact(8));
        for (query_chunk, center_chunk) in &mut chunks {
            for lane in 0..8 {
                lanes[lane] += query_chunk[lane] * center_chunk[lane];
            }
        }
        let consumed = self.dimensions - query.len() % 8;
        let mut dot = lanes.into_iter().sum::<f32>();
        for dimension in consumed..self.dimensions {
            dot += query[dimension] * center[dimension];
        }
        (query_norm + self.center_norms[unit] - 2.0 * dot)
            .max(0.0)
            .sqrt()
    }

    /// Score one physical page exactly from its resident unit centroids.
    pub fn score_page(&self, query: &[f32], page: usize) -> Result<f32, UnitCentroidError> {
        let query_norm = self.query_norm(query)?;
        if page >= self.rows.div_ceil(self.page_rows) {
            return Err(UnitCentroidError::InvalidGeometry);
        }
        let units_per_page = self.page_rows / self.unit_rows;
        let first = page * units_per_page;
        let last = (first + units_per_page).min(self.unit_count());
        let mut best = f32::INFINITY;
        for unit in first..last {
            best = best.min(self.unit_distance(query, query_norm, unit));
        }
        if !best.is_finite() {
            return Err(UnitCentroidError::InvalidQuery);
        }
        Ok(best)
    }

    /// Minimum Euclidean distance from the query to each page's unit means.
    /// This is a score for ranking, not an exact point-distance bound.
    pub fn score_pages(&self, query: &[f32]) -> Result<Vec<f32>, UnitCentroidError> {
        let query_norm = self.query_norm(query)?;
        let mut scores = vec![f32::INFINITY; self.rows.div_ceil(self.page_rows)];
        for unit in 0..self.unit_count() {
            let score = self.unit_distance(query, query_norm, unit);
            if !score.is_finite() {
                return Err(UnitCentroidError::InvalidQuery);
            }
            let page = unit / (self.page_rows / self.unit_rows);
            scores[page] = scores[page].min(score);
        }
        Ok(scores)
    }

    /// Score sorted candidate pages, reusing squared distances from a graph walk.
    /// Returns the page minima and the number of newly computed unit distances.
    pub fn score_pages_sparse_cached(
        &self,
        query: &[f32],
        pages: &[usize],
        cached_squared: &HashMap<u32, f32>,
    ) -> Result<(Vec<(usize, f32)>, usize), UnitCentroidError> {
        if query.len() != self.dimensions
            || query.iter().any(|value| !value.is_finite())
            || pages.is_empty()
            || pages
                .iter()
                .any(|&page| page >= self.rows.div_ceil(self.page_rows))
            || pages.windows(2).any(|pair| pair[0] >= pair[1])
        {
            return Err(UnitCentroidError::InvalidGeometry);
        }
        let units_per_page = self.page_rows / self.unit_rows;
        let mut missing = 0;
        let mut result = Vec::with_capacity(pages.len());
        for &page in pages {
            let first = page * units_per_page;
            let last = (first + units_per_page).min(self.unit_count());
            let mut best = f32::INFINITY;
            for unit in first..last {
                let squared = match cached_squared.get(&(unit as u32)).copied() {
                    Some(distance) if distance.is_finite() && distance >= 0.0 => distance,
                    Some(_) => return Err(UnitCentroidError::InvalidPayload),
                    None => {
                        missing += 1;
                        let center = self.unit_centroid(unit).expect("valid centroid unit");
                        query
                            .iter()
                            .zip(center)
                            .map(|(&left, &right)| {
                                let difference = left - right;
                                difference * difference
                            })
                            .sum::<f32>()
                    }
                };
                if !squared.is_finite() {
                    return Err(UnitCentroidError::InvalidQuery);
                }
                best = best.min(squared);
            }
            result.push((page, best.sqrt()));
        }
        Ok((result, missing))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn row(code: [u8; 2]) -> Vec<u8> {
        let mut bytes = vec![0; 12];
        bytes.extend_from_slice(&code);
        bytes
    }

    #[test]
    fn streamed_sq8_centroids_round_f64_means_directly_to_half() {
        // Adjacent half values are exact f64 values; their midpoint chooses the
        // even encoding. Its immediate f64 neighbors choose the nearer value.
        for lower in 0..0x7bffu16 {
            let midpoint =
                (f16::from_bits(lower).to_f64() + f16::from_bits(lower + 1).to_f64()) / 2.0;
            for (value, expected) in [
                (midpoint.next_down(), lower),
                (midpoint, lower + (lower & 1)),
                (midpoint.next_up(), lower + 1),
            ] {
                for sign in [0, 0x8000] {
                    let signed = if sign == 0 { value } else { -value };
                    assert_eq!(centroid_half(signed).to_bits(), expected | sign);
                }
            }
        }
        for (value, expected) in [
            (0.0, 0),
            (f64::MIN_POSITIVE, 0),
            (f16::from_bits(1).to_f64(), 1),
            (f16::from_bits(0x0400).to_f64(), 0x0400),
            (0.011753082508221269, 0x2205),
            (65504.0, 0x7bff),
            (65520.0f64.next_down(), 0x7bff),
            (65520.0, 0x7c00),
            (65520.0f64.next_up(), 0x7c00),
            (f64::MAX, 0x7c00),
            (f64::INFINITY, 0x7c00),
        ] {
            assert_eq!(centroid_half(value).to_bits(), expected);
            assert_eq!(centroid_half(-value).to_bits(), expected | 0x8000);
        }
        assert!(centroid_half(f64::NAN).is_nan());

        let step = f32::EPSILON;
        // One of 32 rows moves the mean just below/above a half tie. An f32
        // intermediate loses that offset; exercise both tie parities and signs.
        for (midpoint_bits, expected) in [
            (0x3f801000, [0x3c00u16, 0x3c00, 0x3c01]),
            (0x3f803000, [0x3c01u16, 0x3c02, 0x3c02]),
        ] {
            let midpoint = f32::from_bits(midpoint_bits);
            for (code, bits) in expected.into_iter().enumerate() {
                let mut sq8 = row([code as u8, 2 - code as u8]);
                for _ in 1..32 {
                    sq8.extend(row([1, 1]));
                }
                let blob = UnitCentroidPages::build_from_sq8_reader(
                    &mut Cursor::new(sq8),
                    32,
                    2,
                    32,
                    256,
                    &[midpoint - step, -midpoint - step],
                    &[step, step],
                )
                .unwrap();
                assert_eq!(
                    &blob[HEADER_BYTES..],
                    [bits.to_le_bytes(), (bits | 0x8000).to_le_bytes()].concat(),
                    "midpoint={midpoint_bits:#x}, code={code}"
                );
            }
        }
        // Reproduce the reported mean exactly: eight of 32 restored values
        // are one f32 ULP above the half tie, for each sign.
        let sq8 = (0..32)
            .flat_map(|index| row(if index < 8 { [1, 0] } else { [0, 1] }))
            .collect::<Vec<_>>();
        let midpoint = f32::from_bits(0x3c409000);
        let step = f32::from_bits(0x30800000);
        let blob = UnitCentroidPages::build_from_sq8_reader(
            &mut Cursor::new(sq8),
            32,
            2,
            32,
            256,
            &[midpoint, -midpoint - step],
            &[step, step],
        )
        .unwrap();
        assert_eq!(&blob[HEADER_BYTES..], &[0x05, 0x22, 0x05, 0xa2]);

        for low in [
            f32::NAN,
            f32::INFINITY,
            f32::NEG_INFINITY,
            65520.0,
            -65520.0,
        ] {
            assert!(matches!(
                UnitCentroidPages::build_from_sq8_reader(
                    &mut Cursor::new(row([0, 0])),
                    1,
                    2,
                    32,
                    256,
                    &[low, 0.0],
                    &[1.0, 1.0],
                ),
                Err(UnitCentroidError::InvalidCoefficients)
            ));
        }
    }

    #[test]
    fn decoded_half_values_match_scalar_bits_and_reject_nonfinite() {
        let finite = (0..=u16::MAX)
            .filter(|bits| f16::from_bits(*bits).is_finite())
            .collect::<Vec<_>>();
        // Include a partial conversion block after exercising every finite encoding.
        for count in [1, 1023, 1024, 1025, finite.len()] {
            let mut blob = MAGIC.to_vec();
            blob.extend_from_slice(&(count as u64).to_le_bytes());
            for field in [1u32, 1, 1, 0] {
                blob.extend_from_slice(&field.to_le_bytes());
            }
            for bits in &finite[..count] {
                blob.extend_from_slice(&bits.to_le_bytes());
            }
            let decoded = UnitCentroidPages::decode(&blob).unwrap();
            for (index, bits) in finite[..count].iter().enumerate() {
                let expected = f16::from_bits(*bits).to_f32();
                assert_eq!(decoded.centers[index].to_bits(), expected.to_bits());
                assert_eq!(
                    decoded.center_norms[index].to_bits(),
                    (expected * expected).to_bits()
                );
            }
            assert_eq!(decoded.blob_sha256, <[u8; 32]>::from(Sha256::digest(&blob)));
        }
        let mut blob = MAGIC.to_vec();
        blob.extend_from_slice(&1u64.to_le_bytes());
        for field in [1u32, 1, 1, 0] {
            blob.extend_from_slice(&field.to_le_bytes());
        }
        blob.extend_from_slice(&[0; 2]);
        for bits in (0..=u16::MAX).filter(|bits| !f16::from_bits(*bits).is_finite()) {
            blob[HEADER_BYTES..].copy_from_slice(&bits.to_le_bytes());
            assert!(matches!(
                UnitCentroidPages::decode(&blob),
                Err(UnitCentroidError::InvalidPayload)
            ));
        }
    }

    #[test]
    fn streamed_sq8_centroids_roundtrip_and_rank_partial_last_page() {
        let rows = [[1, 0], [3, 0], [0, 1], [0, 3], [5, 5]];
        let sq8 = rows.into_iter().flat_map(row).collect::<Vec<_>>();
        let bytes = UnitCentroidPages::build_from_sq8_reader(
            &mut Cursor::new(sq8),
            5,
            2,
            2,
            4,
            &[0.0, 0.0],
            &[1.0, 1.0],
        )
        .unwrap();
        let scorer = UnitCentroidPages::decode(&bytes).unwrap();
        assert_eq!(scorer.rows(), 5);
        assert_eq!(scorer.dimensions(), 2);
        assert_eq!(scorer.resident_array_bytes(), 36);
        let scores = scorer.score_pages(&[2.0, 0.0]).unwrap();
        assert_eq!(scores.len(), 2);
        assert_eq!(scores[0], 0.0);
        assert!((scores[1] - 34.0_f32.sqrt()).abs() < 1e-5);
        assert_eq!(scorer.score_page(&[2.0, 0.0], 0).unwrap(), scores[0]);
        assert_eq!(scorer.score_page(&[2.0, 0.0], 1).unwrap(), scores[1]);
        let (cached, missing) = scorer
            .score_pages_sparse_cached(&[2.0, 0.0], &[0, 1], &HashMap::from([(0, 0.0)]))
            .unwrap();
        assert_eq!(missing, 2);
        assert_eq!(cached.len(), 2);
        assert!((cached[0].1 - scores[0]).abs() < 1e-5);
        assert!((cached[1].1 - scores[1]).abs() < 1e-5);
        assert!(
            scorer
                .score_pages_sparse_cached(&[2.0, 0.0], &[1, 0], &HashMap::new())
                .is_err()
        );
        assert!(matches!(
            scorer.score_pages_sparse_cached(&[2.0, 0.0], &[0], &HashMap::from([(0, f32::NAN)])),
            Err(UnitCentroidError::InvalidPayload)
        ));
    }

    #[test]
    fn cached_squared_pages_match_flat_d96_scores() {
        let mut sq8 = Vec::new();
        for row in 0..512usize {
            sq8.extend_from_slice(&[0u8; 12]);
            for dim in 0..96usize {
                sq8.push(((row * 17 + dim * 23) % 256) as u8);
            }
        }
        let low = vec![-1.0f32; 96];
        let step = vec![0.01f32; 96];
        let blob = UnitCentroidPages::build_from_sq8_reader(
            &mut Cursor::new(sq8),
            512,
            96,
            32,
            256,
            &low,
            &step,
        )
        .unwrap();
        let scorer = UnitCentroidPages::decode(&blob).unwrap();
        let query = (0..96)
            .map(|dim| (dim % 13) as f32 / 13.0)
            .collect::<Vec<_>>();
        let flat = scorer.score_pages(&query).unwrap();
        let (cached, missing) = scorer
            .score_pages_sparse_cached(&query, &[0, 1], &HashMap::new())
            .unwrap();
        assert_eq!(missing, 16);
        for (page, score) in cached {
            assert!((score - flat[page]).abs() <= 0.0001);
        }
    }

    #[test]
    fn version_and_payload_length_are_checked_before_scoring() {
        let sq8 = row([1, 1]);
        let bytes = UnitCentroidPages::build_from_sq8_reader(
            &mut Cursor::new(sq8),
            1,
            2,
            2,
            4,
            &[0.0, 0.0],
            &[1.0, 1.0],
        )
        .unwrap();
        let mut wrong_version = bytes.clone();
        wrong_version[7] ^= 1;
        assert!(UnitCentroidPages::decode(&wrong_version).is_err());
        assert!(UnitCentroidPages::decode(&bytes[..bytes.len() - 1]).is_err());
        let mut invalid_centroid = bytes.clone();
        invalid_centroid[32..34].copy_from_slice(&f16::NAN.to_bits().to_le_bytes());
        assert!(UnitCentroidPages::decode(&invalid_centroid).is_err());
        let scorer = UnitCentroidPages::decode(&bytes).unwrap();
        assert!(scorer.score_pages(&[f32::NAN, 0.0]).is_err());
        assert!(scorer.score_pages(&[0.0]).is_err());
        let mut trailing_sq8 = row([1, 1]);
        trailing_sq8.push(0);
        assert!(
            UnitCentroidPages::build_from_sq8_reader(
                &mut Cursor::new(trailing_sq8),
                1,
                2,
                2,
                4,
                &[0.0, 0.0],
                &[1.0, 1.0],
            )
            .is_err()
        );
    }
}
