//! Exact local SQ8 nominee scoring over the authenticated D+12 row layout.

use std::collections::HashSet;
#[cfg(test)]
use std::fs::File;
#[cfg(test)]
use std::os::unix::fs::FileExt;

/// Physical SQ8 row geometry supplied by a generation manifest.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Sq8Geometry {
    /// Number of physical SQ8 records.
    pub rows: usize,
    /// Number of code bytes after each ID and norm.
    pub dimensions: usize,
}

/// A nominee score and its stable physical row identity.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ScoredNominee {
    /// Physical record index in the generation object.
    pub ordinal: usize,
    /// Stable vector identifier stored with the record.
    pub id: i64,
    /// Squared-L2 score in the generation's SQ8 scale.
    pub score: f32,
}

/// Rejected exact-score input or local read.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Sq8ScoreError {
    /// Row geometry cannot be represented or is empty.
    InvalidGeometry,
    /// Object bytes or record fields violate the geometry.
    InvalidPlane,
    /// Query or quantization coefficients are malformed.
    InvalidQuery,
    /// Candidate ordinals or IDs are duplicated or out of bounds.
    InvalidRoster,
    /// A positioned local read failed or ended short.
    IoFailure,
}

/// Score nominated rows from a previously authenticated SQ8 object.
///
/// The bytes have one little-endian i64 ID, one little-endian f32 norm,
/// then D unsigned code bytes per row. Authentication is the caller's
/// responsibility; this pure core validates geometry and values.
pub fn score_nominees(
    object: &[u8],
    geometry: Sq8Geometry,
    ordinals: &[usize],
    query: &[f32],
    low: &[f32],
    step: &[f32],
) -> Result<Vec<ScoredNominee>, Sq8ScoreError> {
    let row_bytes = geometry
        .dimensions
        .checked_add(12)
        .ok_or(Sq8ScoreError::InvalidGeometry)?;
    let total = geometry
        .rows
        .checked_mul(row_bytes)
        .ok_or(Sq8ScoreError::InvalidGeometry)?;
    if geometry.rows == 0 || geometry.dimensions == 0 {
        return Err(Sq8ScoreError::InvalidGeometry);
    }
    if object.len() != total {
        return Err(Sq8ScoreError::InvalidPlane);
    }
    if query.len() != geometry.dimensions
        || low.len() != geometry.dimensions
        || step.len() != geometry.dimensions
        || query.iter().any(|value| !value.is_finite())
        || low.iter().any(|value| !value.is_finite())
        || step.iter().any(|value| !value.is_finite() || *value <= 0.0)
    {
        return Err(Sq8ScoreError::InvalidQuery);
    }
    let mut seen = HashSet::with_capacity(ordinals.len());
    if ordinals
        .iter()
        .any(|ordinal| *ordinal >= geometry.rows || !seen.insert(*ordinal))
    {
        return Err(Sq8ScoreError::InvalidRoster);
    }
    let mut shift = 0.0f32;
    let mut qnorm = 0.0f32;
    let mut weights = Vec::with_capacity(geometry.dimensions);
    for coordinate in 0..geometry.dimensions {
        shift += query[coordinate] * low[coordinate];
        qnorm += query[coordinate] * query[coordinate];
        weights.push(query[coordinate] * step[coordinate]);
    }
    shift -= qnorm / 2.0;
    // Validate every roster row (finite norm, unique ID) before scoring any block.
    let mut ids = HashSet::with_capacity(ordinals.len());
    let mut rows = Vec::with_capacity(ordinals.len());
    for &ordinal in ordinals {
        let offset = ordinal * row_bytes;
        let id = i64::from_le_bytes(object[offset..offset + 8].try_into().unwrap());
        let norm = f32::from_le_bytes(object[offset + 8..offset + 12].try_into().unwrap());
        if !norm.is_finite() || !ids.insert(id) {
            return Err(Sq8ScoreError::InvalidPlane);
        }
        rows.push(RosterRow { ordinal, id, norm });
    }
    let mut result = Vec::with_capacity(rows.len());
    let mut start = 0;
    while start < rows.len() {
        let remaining = &rows[start..];
        // Independent rows are scored together in 8, then 4, 2 and 1-row blocks.
        let lanes = match remaining.len() {
            8.. => {
                score_block::<8>(object, row_bytes, &weights, shift, remaining, &mut result)?;
                8
            }
            4..=7 => {
                score_block::<4>(object, row_bytes, &weights, shift, remaining, &mut result)?;
                4
            }
            2..=3 => {
                score_block::<2>(object, row_bytes, &weights, shift, remaining, &mut result)?;
                2
            }
            _ => {
                score_block::<1>(object, row_bytes, &weights, shift, remaining, &mut result)?;
                1
            }
        };
        start += lanes;
    }
    Ok(result)
}

/// One validated roster row: physical position, stable ID and stored norm.
#[derive(Clone, Copy)]
struct RosterRow {
    ordinal: usize,
    id: i64,
    norm: f32,
}

/// Score the first `N` rows of `rows` together, SIMD-across-rows style.
///
/// Every lane keeps its own strictly sequential f32 coordinate accumulation
/// (`inner += code * weight`, a separate multiply and add, no FMA, no horizontal
/// sum, no reassociation), so each score is bit-identical to scoring that row
/// alone. The `N` independent accumulators remove the single-dependency-chain
/// latency bound and let the compiler emit vector code where the target has it.
fn score_block<const N: usize>(
    object: &[u8],
    row_bytes: usize,
    weights: &[f32],
    shift: f32,
    rows: &[RosterRow],
    result: &mut Vec<ScoredNominee>,
) -> Result<(), Sq8ScoreError> {
    let dimensions = weights.len();
    let codes: [&[u8]; N] = std::array::from_fn(|lane| {
        let start = rows[lane].ordinal * row_bytes + 12;
        &object[start..start + dimensions]
    });
    let mut inner = [0.0f32; N];
    for (coordinate, &weight) in weights.iter().enumerate() {
        for lane in 0..N {
            inner[lane] += f32::from(codes[lane][coordinate]) * weight;
        }
    }
    for lane in 0..N {
        let row = rows[lane];
        let score = row.norm - 2.0 * (inner[lane] + shift);
        if !score.is_finite() {
            return Err(Sq8ScoreError::InvalidPlane);
        }
        result.push(ScoredNominee {
            ordinal: row.ordinal,
            id: row.id,
            score,
        });
    }
    Ok(())
}

/// Read exactly the nominated local rows; the caller must authenticate the
/// immutable file before making it visible to serving queries.
#[cfg(test)]
pub(crate) fn score_nominees_file(
    file: &File,
    geometry: Sq8Geometry,
    ordinals: &[usize],
    query: &[f32],
    low: &[f32],
    step: &[f32],
) -> Result<Vec<ScoredNominee>, Sq8ScoreError> {
    let row_bytes = geometry
        .dimensions
        .checked_add(12)
        .ok_or(Sq8ScoreError::InvalidGeometry)?;
    let total = geometry
        .rows
        .checked_mul(row_bytes)
        .ok_or(Sq8ScoreError::InvalidGeometry)?;
    if geometry.rows == 0 || geometry.dimensions == 0 {
        return Err(Sq8ScoreError::InvalidGeometry);
    }
    let size = file.metadata().map_err(|_| Sq8ScoreError::IoFailure)?.len();
    if u64::try_from(total).map_err(|_| Sq8ScoreError::InvalidGeometry)? != size {
        return Err(Sq8ScoreError::InvalidPlane);
    }
    let mut seen = HashSet::with_capacity(ordinals.len());
    if ordinals
        .iter()
        .any(|ordinal| *ordinal >= geometry.rows || !seen.insert(*ordinal))
    {
        return Err(Sq8ScoreError::InvalidRoster);
    }
    if ordinals.is_empty() {
        return Ok(Vec::new());
    }
    let byte_count = ordinals
        .len()
        .checked_mul(row_bytes)
        .ok_or(Sq8ScoreError::InvalidGeometry)?;
    let mut selected = vec![0u8; byte_count];
    for (index, &ordinal) in ordinals.iter().enumerate() {
        let destination = &mut selected[index * row_bytes..(index + 1) * row_bytes];
        let offset = ordinal
            .checked_mul(row_bytes)
            .and_then(|value| u64::try_from(value).ok())
            .ok_or(Sq8ScoreError::InvalidGeometry)?;
        let mut read = 0;
        while read < row_bytes {
            let amount = file
                .read_at(&mut destination[read..], offset + read as u64)
                .map_err(|_| Sq8ScoreError::IoFailure)?;
            if amount == 0 {
                return Err(Sq8ScoreError::IoFailure);
            }
            read += amount;
        }
    }
    let selected_ordinals = (0..ordinals.len()).collect::<Vec<_>>();
    let mut scored = score_nominees(
        &selected,
        Sq8Geometry {
            rows: ordinals.len(),
            dimensions: geometry.dimensions,
        },
        &selected_ordinals,
        query,
        low,
        step,
    )?;
    for entry in &mut scored {
        entry.ordinal = ordinals[entry.ordinal];
    }
    Ok(scored)
}

/// Select physical ordinals by ascending `(score, ID)`.
pub fn primary_ordinals(
    scores: &[ScoredNominee],
    count: usize,
) -> Result<Vec<usize>, Sq8ScoreError> {
    if count == 0 || count > scores.len() {
        return Err(Sq8ScoreError::InvalidRoster);
    }
    let mut seen_ordinals = HashSet::with_capacity(scores.len());
    let mut seen_ids = HashSet::with_capacity(scores.len());
    if scores.iter().any(|entry| {
        !entry.score.is_finite()
            || !seen_ordinals.insert(entry.ordinal)
            || !seen_ids.insert(entry.id)
    }) {
        return Err(Sq8ScoreError::InvalidRoster);
    }
    let mut sorted = scores.to_vec();
    sorted.sort_by(|left, right| {
        left.score
            .total_cmp(&right.score)
            .then(left.id.cmp(&right.id))
    });
    Ok(sorted[..count].iter().map(|entry| entry.ordinal).collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::{self, File};
    use std::io::Write;

    fn row(id: i64, norm: f32, code: [u8; 4]) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&id.to_le_bytes());
        bytes.extend_from_slice(&norm.to_le_bytes());
        bytes.extend_from_slice(&code);
        bytes
    }

    #[test]
    fn exact_rows_score_and_tie_by_id() {
        let mut object = row(12, 1.0, [1, 0, 0, 0]);
        object.extend_from_slice(&row(5, 1.0, [1, 0, 0, 0]));
        object.extend_from_slice(&row(8, 4.0, [2, 0, 0, 0]));
        let geometry = Sq8Geometry {
            rows: 3,
            dimensions: 4,
        };
        let scores = score_nominees(
            &object,
            geometry,
            &[0, 1, 2],
            &[1.0, 0.0, 0.0, 0.0],
            &[0.0; 4],
            &[1.0; 4],
        )
        .unwrap();
        assert_eq!(
            scores.iter().map(|entry| entry.id).collect::<Vec<_>>(),
            vec![12, 5, 8]
        );
        assert_eq!(
            scores.iter().map(|entry| entry.score).collect::<Vec<_>>(),
            vec![0.0, 0.0, 1.0]
        );
        assert_eq!(primary_ordinals(&scores, 2).unwrap(), vec![1, 0]);
    }

    #[test]
    fn rejects_short_final_row_invalid_ordinal_and_nonfinite_query() {
        let mut object = row(1, 0.0, [0; 4]);
        object.extend_from_slice(&row(2, 0.0, [0; 4])[..15]);
        let geometry = Sq8Geometry {
            rows: 2,
            dimensions: 4,
        };
        assert!(score_nominees(&object, geometry, &[1], &[0.0; 4], &[0.0; 4], &[1.0; 4]).is_err());
        assert!(score_nominees(&object, geometry, &[2], &[0.0; 4], &[0.0; 4], &[1.0; 4]).is_err());
        assert!(
            score_nominees(
                &object,
                geometry,
                &[0],
                &[f32::NAN; 4],
                &[0.0; 4],
                &[1.0; 4]
            )
            .is_err()
        );
    }

    #[test]
    fn file_and_ram_placements_score_the_same_bytes() {
        let mut object = row(4, 1.0, [1, 0, 0, 0]);
        object.extend_from_slice(&row(9, 4.0, [2, 0, 0, 0]));
        let path =
            std::env::temp_dir().join(format!("borsuk-v114-{}-plane.bin", std::process::id()));
        let mut output = File::create(&path).unwrap();
        output.write_all(&object).unwrap();
        drop(output);
        let file = File::open(&path).unwrap();
        let geometry = Sq8Geometry {
            rows: 2,
            dimensions: 4,
        };
        let query = [1.0, 0.0, 0.0, 0.0];
        let ram = score_nominees(&object, geometry, &[1, 0], &query, &[0.0; 4], &[1.0; 4]).unwrap();
        let disk =
            score_nominees_file(&file, geometry, &[1, 0], &query, &[0.0; 4], &[1.0; 4]).unwrap();
        assert_eq!(ram, disk);
        fs::remove_file(path).unwrap();
    }

    struct Rng(u64);

    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 >> 12;
            self.0 ^= self.0 << 25;
            self.0 ^= self.0 >> 27;
            self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
        }

        fn signed(&mut self) -> f32 {
            (self.next() >> 40) as f32 / 8_388_608.0 - 1.0
        }
    }

    struct Plane {
        object: Vec<u8>,
        rows: usize,
        dimensions: usize,
        query: Vec<f32>,
        low: Vec<f32>,
        step: Vec<f32>,
    }

    impl Plane {
        fn new(rows: usize, dimensions: usize, seed: u64) -> Self {
            let mut rng = Rng(seed | 1);
            let mut object = Vec::with_capacity(rows * (dimensions + 12));
            for row in 0..rows {
                object.extend_from_slice(&(1000 + 7 * row as i64 - 3).to_le_bytes());
                object.extend_from_slice(&(2.0 + 2.0 * rng.signed()).to_le_bytes());
                for _ in 0..dimensions {
                    // Include the extreme codes 0 and 255 as well as arbitrary bytes.
                    object.push(match rng.next() % 11 {
                        0 => 0,
                        1 => 255,
                        _ => (rng.next() >> 33) as u8,
                    });
                }
            }
            let mut query = (0..dimensions)
                .map(|_| 3.0 * rng.signed())
                .collect::<Vec<_>>();
            let mut low = (0..dimensions).map(|_| rng.signed()).collect::<Vec<_>>();
            let step = (0..dimensions)
                .map(|_| 0.003 + 0.4 * (rng.signed().abs()))
                .collect::<Vec<_>>();
            query[0] = -0.0;
            low[0] = -0.0;
            if dimensions > 2 {
                query[2] = 0.0;
                low[2] = 0.0;
            }
            Self {
                object,
                rows,
                dimensions,
                query,
                low,
                step,
            }
        }

        fn score(&self, ordinals: &[usize]) -> Result<Vec<ScoredNominee>, Sq8ScoreError> {
            score_nominees(
                &self.object,
                Sq8Geometry {
                    rows: self.rows,
                    dimensions: self.dimensions,
                },
                ordinals,
                &self.query,
                &self.low,
                &self.step,
            )
        }

        fn set_norm(&mut self, row: usize, norm: f32) {
            let offset = row * (self.dimensions + 12) + 8;
            self.object[offset..offset + 4].copy_from_slice(&norm.to_le_bytes());
        }

        fn set_id(&mut self, row: usize, id: i64) {
            let offset = row * (self.dimensions + 12);
            self.object[offset..offset + 8].copy_from_slice(&id.to_le_bytes());
        }
    }

    /// Independent single-chain scalar arithmetic: no blocks, no shared helpers.
    fn oracle(plane: &Plane, ordinals: &[usize]) -> Vec<(usize, i64, u32)> {
        let row_bytes = plane.dimensions + 12;
        let mut shift = 0.0f32;
        let mut qnorm = 0.0f32;
        for c in 0..plane.dimensions {
            shift += plane.query[c] * plane.low[c];
            qnorm += plane.query[c] * plane.query[c];
        }
        shift -= qnorm / 2.0;
        ordinals
            .iter()
            .map(|&ordinal| {
                let offset = ordinal * row_bytes;
                let id = i64::from_le_bytes(plane.object[offset..offset + 8].try_into().unwrap());
                let norm =
                    f32::from_le_bytes(plane.object[offset + 8..offset + 12].try_into().unwrap());
                let mut inner = 0.0f32;
                for c in 0..plane.dimensions {
                    let weight = plane.query[c] * plane.step[c];
                    inner += f32::from(plane.object[offset + 12 + c]) * weight;
                }
                (ordinal, id, (norm - 2.0 * (inner + shift)).to_bits())
            })
            .collect()
    }

    fn bits(scores: &[ScoredNominee]) -> Vec<(usize, i64, u32)> {
        scores
            .iter()
            .map(|entry| (entry.ordinal, entry.id, entry.score.to_bits()))
            .collect()
    }

    #[test]
    fn blocked_scores_equal_independent_scalar_oracle_bits() {
        for dimensions in [1, 2, 3, 4, 5, 7, 8, 9, 15, 16, 17, 31, 33, 127, 129, 300] {
            for rows in [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 15, 16, 17, 23, 24, 25, 257] {
                let plane = Plane::new(rows, dimensions, (dimensions * 1009 + rows) as u64);
                let ordinals = (0..rows).collect::<Vec<_>>();
                let scored = plane.score(&ordinals).unwrap();
                assert_eq!(
                    bits(&scored),
                    oracle(&plane, &ordinals),
                    "D{dimensions} rows{rows}"
                );
            }
        }
    }

    #[test]
    fn rosters_keep_requested_order_ids_and_ordinals_across_blocks() {
        let plane = Plane::new(40, 13, 77);
        for count in [0, 1, 7, 8, 9, 17, 29, 40] {
            // 7 is coprime with 40, so every prefix is a unique shuffled roster.
            let ordinals = (0..count).map(|i| (i * 7) % 40).collect::<Vec<_>>();
            let scored = plane.score(&ordinals).unwrap();
            assert_eq!(bits(&scored), oracle(&plane, &ordinals), "count {count}");
            assert_eq!(
                scored.iter().map(|entry| entry.ordinal).collect::<Vec<_>>(),
                ordinals
            );
        }
    }

    #[test]
    fn signed_zero_negative_nonunit_and_near_ties_keep_bits_and_id_order() {
        let mut plane = Plane::new(18, 5, 5);
        // Rows 0..12 share one code vector and norm: exact ties, ordered by ID.
        let template = plane.object[12..12 + 5].to_vec();
        for row in 0..12 {
            let offset = row * 17 + 12;
            plane.object[offset..offset + 5].copy_from_slice(&template);
            plane.set_norm(row, 3.0);
        }
        // Zero norms (+0 and -0) with zero codes exercise signed-zero results.
        for (row, norm) in [(12, 0.0f32), (13, -0.0f32)] {
            plane.set_norm(row, norm);
            let offset = row * 17 + 12;
            plane.object[offset..offset + 5].fill(0);
        }
        plane.query = vec![0.0, -0.0, 0.0, -0.0, 0.0];
        plane.low = vec![-0.0, 0.0, -0.0, 0.0, -0.0];
        let ordinals = (0..18).collect::<Vec<_>>();
        let scored = plane.score(&ordinals).unwrap();
        assert_eq!(bits(&scored), oracle(&plane, &ordinals));
        let first = scored[0].score.to_bits();
        assert!(
            scored[..12]
                .iter()
                .all(|entry| entry.score.to_bits() == first)
        );
        let ties = primary_ordinals(&scored[..12], 5).unwrap();
        assert_eq!(
            ties,
            vec![0, 1, 2, 3, 4],
            "equal scores order by ascending ID"
        );
        // Nonunit negative coefficients through the ordinary random plane as well.
        let mut negative = Plane::new(33, 9, 12345);
        negative.query.iter_mut().for_each(|value| *value = -*value);
        negative.step.iter_mut().for_each(|value| *value *= 7.5);
        let all = (0..33).collect::<Vec<_>>();
        assert_eq!(
            bits(&negative.score(&all).unwrap()),
            oracle(&negative, &all)
        );
    }

    #[test]
    fn corrupt_inputs_keep_error_classes_in_every_block_position() {
        let ordinals = (0..17).collect::<Vec<_>>();
        for position in [0, 1, 3, 4, 7, 8, 9, 15, 16] {
            let mut norm = Plane::new(17, 6, 9);
            norm.set_norm(position, f32::NAN);
            assert_eq!(
                norm.score(&ordinals).unwrap_err(),
                Sq8ScoreError::InvalidPlane
            );
            let mut infinite = Plane::new(17, 6, 9);
            infinite.set_norm(position, f32::INFINITY);
            assert_eq!(
                infinite.score(&ordinals).unwrap_err(),
                Sq8ScoreError::InvalidPlane
            );
            let mut duplicate = Plane::new(17, 6, 9);
            let other = (position + 1) % 17;
            let id = i64::from_le_bytes(
                duplicate.object[other * 18..other * 18 + 8]
                    .try_into()
                    .unwrap(),
            );
            duplicate.set_id(position, id);
            assert_eq!(
                duplicate.score(&ordinals).unwrap_err(),
                Sq8ScoreError::InvalidPlane
            );
            // The same duplicate outside the roster is ignored.
            let outside = ordinals
                .iter()
                .copied()
                .filter(|ordinal| *ordinal != position)
                .collect::<Vec<_>>();
            assert!(duplicate.score(&outside).is_ok());
            // One row alone overflows to an infinite score; no partial result.
            let mut overflow = Plane::new(17, 6, 9);
            overflow.query = vec![1e18; 6];
            overflow.low = vec![0.0; 6];
            overflow.step = vec![1e18; 6];
            for row in 0..17 {
                let offset = row * 18 + 12;
                overflow.object[offset..offset + 6].fill(if row == position { 255 } else { 0 });
            }
            assert_eq!(
                overflow.score(&ordinals).unwrap_err(),
                Sq8ScoreError::InvalidPlane
            );
        }
        let plane = Plane::new(9, 4, 3);
        let geometry = Sq8Geometry {
            rows: 9,
            dimensions: 4,
        };
        let score = |geometry, object: &[u8], ordinals: &[usize], query: &[f32], step: &[f32]| {
            score_nominees(object, geometry, ordinals, query, &plane.low, step)
        };
        let all = (0..9).collect::<Vec<_>>();
        let good = score(geometry, &plane.object, &all, &plane.query, &plane.step);
        assert!(good.is_ok());
        assert_eq!(
            score(
                Sq8Geometry {
                    rows: 0,
                    ..geometry
                },
                &plane.object,
                &all,
                &plane.query,
                &plane.step
            ),
            Err(Sq8ScoreError::InvalidGeometry)
        );
        assert_eq!(
            score(
                Sq8Geometry {
                    dimensions: 0,
                    ..geometry
                },
                &plane.object,
                &all,
                &plane.query,
                &plane.step
            ),
            Err(Sq8ScoreError::InvalidGeometry)
        );
        assert_eq!(
            score(
                geometry,
                &plane.object[1..],
                &all,
                &plane.query,
                &plane.step
            ),
            Err(Sq8ScoreError::InvalidPlane)
        );
        // Plane length is checked before the query, the query before the roster.
        assert_eq!(
            score(
                geometry,
                &plane.object[1..],
                &[99],
                &[f32::NAN; 4],
                &plane.step
            ),
            Err(Sq8ScoreError::InvalidPlane)
        );
        assert_eq!(
            score(geometry, &plane.object, &[99], &[f32::NAN; 4], &plane.step),
            Err(Sq8ScoreError::InvalidQuery)
        );
        for step in [[0.0f32; 4], [-1.0; 4], [f32::INFINITY; 4], [f32::NAN; 4]] {
            assert_eq!(
                score(geometry, &plane.object, &all, &plane.query, &step),
                Err(Sq8ScoreError::InvalidQuery)
            );
        }
        assert_eq!(
            score(
                geometry,
                &plane.object,
                &all,
                &plane.query[..3],
                &plane.step
            ),
            Err(Sq8ScoreError::InvalidQuery)
        );
        assert_eq!(
            score(geometry, &plane.object, &[0, 9], &plane.query, &plane.step),
            Err(Sq8ScoreError::InvalidRoster)
        );
        assert_eq!(
            score(
                geometry,
                &plane.object,
                &[1, 2, 1],
                &plane.query,
                &plane.step
            ),
            Err(Sq8ScoreError::InvalidRoster)
        );
        assert_eq!(
            score(geometry, &plane.object, &[], &plane.query, &plane.step),
            Ok(Vec::new())
        );
    }
}
