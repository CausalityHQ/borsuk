//! Exact SQ8 ranking over authenticated physical ranges from one generation.

use crate::exact_sq8_nominee::{ScoredNominee, Sq8Geometry, Sq8ScoreError, score_nominees};
use std::collections::HashSet;

/// One half-open physical byte range and its already authenticated payload.
/// The caller must validate S3 status, byte range, generation, and page hashes.
pub struct ReturnedRange<'a> {
    pub start: usize,
    pub bytes: &'a [u8],
}

/// Rank all returned rows by exact SQ8 `(score, ID)` and keep the best `top_k`.
/// Ranges must be nonempty, ordered, disjoint, row-aligned and within the object.
/// `max_bytes` enforces the physical response budget after authentication.
pub fn rank_returned_ranges(
    geometry: Sq8Geometry,
    ranges: &[ReturnedRange<'_>],
    query: &[f32],
    low: &[f32],
    step: &[f32],
    top_k: usize,
    max_bytes: usize,
) -> Result<Vec<ScoredNominee>, Sq8ScoreError> {
    let row_bytes = geometry.dimensions.checked_add(12)
        .ok_or(Sq8ScoreError::InvalidGeometry)?;
    let object_bytes = geometry.rows.checked_mul(row_bytes)
        .ok_or(Sq8ScoreError::InvalidGeometry)?;
    if geometry.rows == 0 || geometry.dimensions == 0 || top_k == 0 {
        return Err(Sq8ScoreError::InvalidGeometry);
    }
    let mut previous_end = 0usize;
    let mut total_bytes = 0usize;
    let mut scores = Vec::new();
    let mut ids = HashSet::new();
    for range in ranges {
        let end = range.start.checked_add(range.bytes.len())
            .ok_or(Sq8ScoreError::InvalidPlane)?;
        total_bytes = total_bytes.checked_add(range.bytes.len())
            .ok_or(Sq8ScoreError::InvalidPlane)?;
        if range.bytes.is_empty() || range.start < previous_end
            || range.start % row_bytes != 0 || end % row_bytes != 0
            || end > object_bytes || total_bytes > max_bytes
        {
            return Err(Sq8ScoreError::InvalidPlane);
        }
        let first_row = range.start / row_bytes;
        let count = range.bytes.len() / row_bytes;
        let local_ordinals = (0..count).collect::<Vec<_>>();
        let local = score_nominees(
            range.bytes,
            Sq8Geometry { rows: count, dimensions: geometry.dimensions },
            &local_ordinals, query, low, step,
        )?;
        for mut score in local {
            if !ids.insert(score.id) {
                return Err(Sq8ScoreError::InvalidPlane);
            }
            score.ordinal += first_row;
            scores.push(score);
        }
        previous_end = end;
    }
    if scores.len() < top_k {
        return Err(Sq8ScoreError::InvalidRoster);
    }
    scores.sort_by(|left, right| left.score.total_cmp(&right.score)
        .then(left.id.cmp(&right.id)));
    scores.truncate(top_k);
    Ok(scores)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(id: i64, norm: f32, code: u8) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&id.to_le_bytes());
        bytes.extend_from_slice(&norm.to_le_bytes());
        bytes.push(code);
        bytes
    }

    #[test]
    fn ranks_sparse_ranges_and_ties_by_id() {
        let first = row(30, 1.0, 1);
        let mut second = row(20, 1.0, 1);
        second.extend_from_slice(&row(10, 1.0, 1));
        let result = rank_returned_ranges(
            Sq8Geometry { rows: 5, dimensions: 1 },
            &[ReturnedRange { start: 0, bytes: &first },
              ReturnedRange { start: 3 * 13, bytes: &second }],
            &[0.0], &[0.0], &[1.0], 2, 39,
        ).unwrap();
        assert_eq!(result.iter().map(|entry| (entry.ordinal, entry.id))
            .collect::<Vec<_>>(), vec![(4, 10), (3, 20)]);
    }

    #[test]
    fn rejects_overlap_unaligned_budget_and_duplicate_ids() {
        let row = row(7, 1.0, 1);
        let geometry = Sq8Geometry { rows: 3, dimensions: 1 };
        let score = |ranges: &[ReturnedRange<'_>], cap| rank_returned_ranges(
            geometry, ranges, &[0.0], &[0.0], &[1.0], 1, cap,
        );
        assert_eq!(score(&[ReturnedRange { start: 0, bytes: &row },
                           ReturnedRange { start: 0, bytes: &row }], 26),
                   Err(Sq8ScoreError::InvalidPlane));
        assert_eq!(score(&[ReturnedRange { start: 1, bytes: &row }], 13),
                   Err(Sq8ScoreError::InvalidPlane));
        assert_eq!(score(&[ReturnedRange { start: 0, bytes: &row }], 12),
                   Err(Sq8ScoreError::InvalidPlane));
        assert_eq!(score(&[ReturnedRange { start: 0, bytes: &row },
                           ReturnedRange { start: 26, bytes: &row }], 26),
                   Err(Sq8ScoreError::InvalidPlane));
    }
}
