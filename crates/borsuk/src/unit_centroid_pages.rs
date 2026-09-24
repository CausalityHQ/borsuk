//! Versioned f16 unit centroids for score-first physical page selection.

use half::f16;
use std::io::Read;

const MAGIC: &[u8; 8] = b"BORSUCP1";
const HEADER_BYTES: usize = 32;

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
                let half = f16::from_f64(sum / count as f64);
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
        let centers = bytes[HEADER_BYTES..]
            .chunks_exact(2)
            .map(|pair| f16::from_bits(u16::from_le_bytes(pair.try_into().unwrap())).to_f32())
            .collect::<Vec<_>>();
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
        })
    }

    pub fn rows(&self) -> usize {
        self.rows
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
