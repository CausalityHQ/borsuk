//! Exact SQ8 ranking over authenticated physical ranges from one generation.

use crate::exact_sq8_nominee::{ScoredNominee, Sq8Geometry, Sq8ScoreError, score_nominees};
use std::collections::HashSet;

/// Conservative concurrent response, ranking and planner payload admission.
/// Allocator/runtime/transport overhead must be charged separately.
pub(crate) fn query_payload_bytes(
    geometry: Sq8Geometry,
    max_bytes: u64,
    planner_bytes: u64,
    max_active_queries: u64,
) -> Result<u64, Sq8ScoreError> {
    let bad = Sq8ScoreError::InvalidGeometry;
    let row_bytes = geometry.dimensions.checked_add(12).ok_or(bad)? as u64;
    if geometry.rows == 0 || geometry.dimensions == 0 || max_bytes == 0 || max_active_queries == 0 {
        return Err(bad);
    }
    let rows = (geometry.rows as u64).min(max_bytes / row_bytes);
    // Four score slots cover Vec growth, local results and sorting; remaining
    // allowance covers ordinals and three membership/duplicate-ID sets.
    let ranking = rows
        .checked_mul(256)
        .and_then(|n| n.checked_add((geometry.dimensions as u64).checked_mul(4)?))
        .ok_or(bad)?;
    max_bytes
        .checked_mul(3)
        .and_then(|n| n.checked_add(planner_bytes))
        .and_then(|n| n.checked_add(ranking))
        .and_then(|n| n.checked_mul(max_active_queries))
        .ok_or(bad)
}

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
    let scores =
        rank_returned_ranges_excluding(geometry, ranges, query, low, step, top_k, max_bytes, &[])?;
    if scores.len() < top_k {
        return Err(Sq8ScoreError::InvalidRoster);
    }
    Ok(scores)
}

/// Rank up to `top_k` visible rows. Exclusions are immutable, sorted, unique
/// logical IDs from a separately authenticated and admitted mutation snapshot.
/// All fetched rows are still validated, including excluded rows. No refetch
/// occurs if visibility leaves fewer than k rows; callers must report underfill.
#[allow(clippy::too_many_arguments)]
pub fn rank_returned_ranges_excluding(
    geometry: Sq8Geometry,
    ranges: &[ReturnedRange<'_>],
    query: &[f32],
    low: &[f32],
    step: &[f32],
    top_k: usize,
    max_bytes: usize,
    excluded_ids: &[i64],
) -> Result<Vec<ScoredNominee>, Sq8ScoreError> {
    if ranges.is_empty() || excluded_ids.windows(2).any(|ids| ids[0] >= ids[1]) {
        return Err(Sq8ScoreError::InvalidRoster);
    }
    let row_bytes = geometry
        .dimensions
        .checked_add(12)
        .ok_or(Sq8ScoreError::InvalidGeometry)?;
    let object_bytes = geometry
        .rows
        .checked_mul(row_bytes)
        .ok_or(Sq8ScoreError::InvalidGeometry)?;
    if geometry.rows == 0 || geometry.dimensions == 0 || top_k == 0 {
        return Err(Sq8ScoreError::InvalidGeometry);
    }
    let mut previous_end = 0usize;
    let mut total_bytes = 0usize;
    let mut scores = Vec::new();
    let mut ids = HashSet::new();
    for range in ranges {
        let end = range
            .start
            .checked_add(range.bytes.len())
            .ok_or(Sq8ScoreError::InvalidPlane)?;
        total_bytes = total_bytes
            .checked_add(range.bytes.len())
            .ok_or(Sq8ScoreError::InvalidPlane)?;
        if range.bytes.is_empty()
            || range.start < previous_end
            || range.start % row_bytes != 0
            || end % row_bytes != 0
            || end > object_bytes
            || total_bytes > max_bytes
        {
            return Err(Sq8ScoreError::InvalidPlane);
        }
        let first_row = range.start / row_bytes;
        let count = range.bytes.len() / row_bytes;
        let local_ordinals = (0..count).collect::<Vec<_>>();
        let local = score_nominees(
            range.bytes,
            Sq8Geometry {
                rows: count,
                dimensions: geometry.dimensions,
            },
            &local_ordinals,
            query,
            low,
            step,
        )?;
        for mut score in local {
            if !ids.insert(score.id) {
                return Err(Sq8ScoreError::InvalidPlane);
            }
            if excluded_ids.binary_search(&score.id).is_err() {
                score.ordinal += first_row;
                scores.push(score);
            }
        }
        previous_end = end;
    }
    scores.sort_by(|left, right| {
        left.score
            .total_cmp(&right.score)
            .then(left.id.cmp(&right.id))
    });
    // Release fetched-row capacity before returning caller-owned top-k results.
    Ok(scores[..top_k.min(scores.len())].to_vec())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query_admission_accounts_for_narrow_rows_concurrent_planners_and_overflow() {
        let geometry = Sq8Geometry {
            rows: 1_000_000,
            dimensions: 1,
        };
        let wire = 13_000_000;
        let admitted = query_payload_bytes(geometry, wire, 0, 1).unwrap();
        assert!(
            admitted
                >= 3 * wire + geometry.rows as u64 * std::mem::size_of::<ScoredNominee>() as u64
        );
        let tiny = query_payload_bytes(
            Sq8Geometry {
                rows: 1,
                dimensions: 1,
            },
            wire,
            0,
            1,
        )
        .unwrap();
        assert!(admitted > tiny);
        assert_eq!(
            query_payload_bytes(geometry, wire, 123, 3).unwrap(),
            (admitted + 123) * 3
        );
        assert!(
            query_payload_bytes(
                Sq8Geometry {
                    rows: 0,
                    dimensions: 1
                },
                wire,
                0,
                1
            )
            .is_err()
        );
        assert!(
            query_payload_bytes(
                Sq8Geometry {
                    rows: 1,
                    dimensions: 0
                },
                wire,
                0,
                1
            )
            .is_err()
        );
        assert!(
            query_payload_bytes(
                Sq8Geometry {
                    rows: 1,
                    dimensions: usize::MAX
                },
                wire,
                0,
                1
            )
            .is_err()
        );
        assert!(query_payload_bytes(geometry, u64::MAX, 0, 1).is_err());
        assert!(query_payload_bytes(geometry, wire, 0, 0).is_err());
        assert!(query_payload_bytes(geometry, wire, u64::MAX, 1).is_err());
    }

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
            Sq8Geometry {
                rows: 5,
                dimensions: 1,
            },
            &[
                ReturnedRange {
                    start: 0,
                    bytes: &first,
                },
                ReturnedRange {
                    start: 3 * 13,
                    bytes: &second,
                },
            ],
            &[0.0],
            &[0.0],
            &[1.0],
            2,
            39,
        )
        .unwrap();
        assert_eq!(
            result
                .iter()
                .map(|entry| (entry.ordinal, entry.id))
                .collect::<Vec<_>>(),
            vec![(4, 10), (3, 20)]
        );
        assert_eq!(
            result.capacity(),
            2,
            "fetched-row scratch must not escape in top-k results"
        );
    }

    #[test]
    fn mutation_visibility_precedes_top_k_and_validates_masked_rows() {
        let mut bytes = row(i64::MIN, 1.0, 1);
        bytes.extend_from_slice(&row(10, 2.0, 1));
        bytes.extend_from_slice(&row(20, 3.0, 1));
        let geometry = Sq8Geometry {
            rows: 3,
            dimensions: 1,
        };
        let run = |data: &[u8], excluded: &[i64], k| {
            rank_returned_ranges_excluding(
                geometry,
                &[ReturnedRange {
                    start: 0,
                    bytes: data,
                }],
                &[0.0],
                &[0.0],
                &[1.0],
                k,
                data.len(),
                excluded,
            )
        };
        assert_eq!(
            run(&bytes, &[i64::MIN], 2)
                .unwrap()
                .iter()
                .map(|r| r.id)
                .collect::<Vec<_>>(),
            vec![10, 20]
        );
        assert_eq!(run(&bytes, &[i64::MIN, 10], 2).unwrap()[0].id, 20);
        assert!(run(&bytes, &[i64::MIN, 10, 20], 2).unwrap().is_empty());
        assert_eq!(
            run(&bytes, &[10, i64::MIN], 1),
            Err(Sq8ScoreError::InvalidRoster)
        );
        assert_eq!(run(&bytes, &[10, 10], 1), Err(Sq8ScoreError::InvalidRoster));
        let legacy = rank_returned_ranges(
            geometry,
            &[ReturnedRange {
                start: 0,
                bytes: &bytes,
            }],
            &[0.0],
            &[0.0],
            &[1.0],
            2,
            bytes.len(),
        )
        .unwrap();
        assert_eq!(run(&bytes, &[], 2).unwrap(), legacy);
        assert_eq!(
            rank_returned_ranges_excluding(geometry, &[], &[0.0], &[0.0], &[1.0], 1, 39, &[]),
            Err(Sq8ScoreError::InvalidRoster),
        );
        let mut corrupt = bytes.clone();
        corrupt[8..12].copy_from_slice(&f32::NAN.to_le_bytes());
        assert_eq!(
            run(&corrupt, &[i64::MIN], 2),
            Err(Sq8ScoreError::InvalidPlane)
        );
        let mut duplicate = bytes.clone();
        duplicate[13..21].copy_from_slice(&i64::MIN.to_le_bytes());
        assert_eq!(
            run(&duplicate, &[i64::MIN], 2),
            Err(Sq8ScoreError::InvalidPlane)
        );
    }

    #[test]
    fn rejects_overlap_unaligned_budget_and_duplicate_ids() {
        let row = row(7, 1.0, 1);
        let geometry = Sq8Geometry {
            rows: 3,
            dimensions: 1,
        };
        let score = |ranges: &[ReturnedRange<'_>], cap| {
            rank_returned_ranges(geometry, ranges, &[0.0], &[0.0], &[1.0], 1, cap)
        };
        assert_eq!(
            score(
                &[
                    ReturnedRange {
                        start: 0,
                        bytes: &row
                    },
                    ReturnedRange {
                        start: 0,
                        bytes: &row
                    }
                ],
                26
            ),
            Err(Sq8ScoreError::InvalidPlane)
        );
        assert_eq!(
            score(
                &[ReturnedRange {
                    start: 1,
                    bytes: &row
                }],
                13
            ),
            Err(Sq8ScoreError::InvalidPlane)
        );
        assert_eq!(
            score(
                &[ReturnedRange {
                    start: 0,
                    bytes: &row
                }],
                12
            ),
            Err(Sq8ScoreError::InvalidPlane)
        );
        assert_eq!(
            score(
                &[
                    ReturnedRange {
                        start: 0,
                        bytes: &row
                    },
                    ReturnedRange {
                        start: 26,
                        bytes: &row
                    }
                ],
                26
            ),
            Err(Sq8ScoreError::InvalidPlane)
        );
    }
}
