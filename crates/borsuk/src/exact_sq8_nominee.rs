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
    let mut result = Vec::with_capacity(ordinals.len());
    let mut ids = HashSet::with_capacity(ordinals.len());
    for &ordinal in ordinals {
        let offset = ordinal * row_bytes;
        let id = i64::from_le_bytes(object[offset..offset + 8].try_into().unwrap());
        let norm = f32::from_le_bytes(object[offset + 8..offset + 12].try_into().unwrap());
        if !norm.is_finite() || !ids.insert(id) {
            return Err(Sq8ScoreError::InvalidPlane);
        }
        let mut inner = 0.0f32;
        for coordinate in 0..geometry.dimensions {
            inner += f32::from(object[offset + 12 + coordinate]) * weights[coordinate];
        }
        let score = norm - 2.0 * (inner + shift);
        if !score.is_finite() {
            return Err(Sq8ScoreError::InvalidPlane);
        }
        result.push(ScoredNominee { ordinal, id, score });
    }
    Ok(result)
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
}
