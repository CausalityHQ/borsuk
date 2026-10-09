//! Exact SQ8 ranking over authenticated physical ranges from one generation.

use crate::exact_sq8_nominee::{ScoredNominee, Sq8Geometry, Sq8ScoreError, score_nominees};
use std::collections::{BTreeMap, HashSet};

/// Validated headerless records from a framed extent. File offsets are not
/// physical ordinals. Authentication and complete frame charging belong to
/// the caller; this additive interface does not alter ReturnedRange.
pub struct RecordSlice<'a> {
    pub first_physical_ordinal: usize,
    pub bytes: &'a [u8],
}

/// Validate every complete duplicate body, choose its smallest physical
/// ordinal, then apply visibility and top-k using the unchanged SQ8 kernel.
/// Logical count and encoded physical geometry are deliberately separate.
#[allow(clippy::too_many_arguments)]
pub fn rank_unique_sq8(
    logical_rows: usize,
    physical: Sq8Geometry,
    slices: &[RecordSlice<'_>],
    query: &[f32],
    low: &[f32],
    step: &[f32],
    top_k: usize,
    max_record_bytes: usize,
    excluded_ids: &[i64],
) -> Result<Vec<ScoredNominee>, Sq8ScoreError> {
    if logical_rows == 0 || logical_rows > physical.rows || top_k == 0
        || physical.dimensions == 0 {
        return Err(Sq8ScoreError::InvalidGeometry);
    }
    if slices.is_empty() || excluded_ids.windows(2).any(|v| v[0] >= v[1]) {
        return Err(Sq8ScoreError::InvalidRoster);
    }
    let width = physical.dimensions.checked_add(12).ok_or(Sq8ScoreError::InvalidGeometry)?;
    let mut ordered = slices.iter().collect::<Vec<_>>();
    ordered.sort_by_key(|s| s.first_physical_ordinal);
    let mut previous = 0;
    let mut bytes = 0_usize;
    let mut unique: BTreeMap<i64, (&[u8], usize)> = BTreeMap::new();
    for slice in ordered {
        let count = slice.bytes.len() / width;
        let end = slice.first_physical_ordinal.checked_add(count).ok_or(Sq8ScoreError::InvalidPlane)?;
        bytes = bytes.checked_add(slice.bytes.len()).ok_or(Sq8ScoreError::InvalidPlane)?;
        if count == 0 || slice.bytes.len() % width != 0 || slice.first_physical_ordinal < previous
            || end > physical.rows || bytes > max_record_bytes {
            return Err(Sq8ScoreError::InvalidPlane);
        }
        for (slot, body) in slice.bytes.chunks_exact(width).enumerate() {
            let id = i64::from_le_bytes(body[..8].try_into().unwrap());
            let ordinal = slice.first_physical_ordinal + slot;
            if let Some((old, _)) = unique.get(&id) {
                if *old != body { return Err(Sq8ScoreError::InvalidPlane); }
            } else {
                unique.insert(id, (body, ordinal));
            }
        }
        previous = end;
    }
    if unique.len() > logical_rows { return Err(Sq8ScoreError::InvalidRoster); }
    let mut plane = Vec::new();
    plane.try_reserve_exact(unique.len().checked_mul(width).ok_or(Sq8ScoreError::InvalidGeometry)?)
        .map_err(|_| Sq8ScoreError::InvalidGeometry)?;
    let mut representatives = Vec::with_capacity(unique.len());
    for (body, ordinal) in unique.values() {
        plane.extend_from_slice(body);
        representatives.push(*ordinal);
    }
    let ordinals = (0..unique.len()).collect::<Vec<_>>();
    let mut scores = score_nominees(&plane, Sq8Geometry { rows: unique.len(), dimensions: physical.dimensions },
        &ordinals, query, low, step)?;
    for s in &mut scores { s.ordinal = representatives[s.ordinal]; }
    scores.retain(|s| excluded_ids.binary_search(&s.id).is_err());
    scores.sort_by(|a, b| a.score.total_cmp(&b.score).then(a.id.cmp(&b.id)));
    Ok(scores[..top_k.min(scores.len())].to_vec())
}

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
    rank_returned_ranges_excluding_traced(
        geometry,
        ranges,
        query,
        low,
        step,
        top_k,
        max_bytes,
        excluded_ids,
        None,
    )
}

/// Opt-in rank phase durations. Present only when the caller supplies a sink:
/// the ordinary path reads no clock. Phases never overlap, so they may be summed,
/// but they are not the rank interval: validation and range bookkeeping between
/// them are left unattributed on purpose.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize)]
pub struct RankPhases {
    pub ranges: u32,
    pub rows: u64,
    /// Sum of ordinal-index construction and shared SQ8 kernel calls over all ranges.
    pub score_ns: u64,
    /// Sum of duplicate-ID insertion and exclusion filtering over all ranges.
    pub roster_ns: u64,
    /// The one `(score, id)` sort over every retained row.
    pub sort_ns: u64,
    /// Copying the top-k out and releasing the scoring scratch (duplicate-ID set, scores).
    pub finish_ns: u64,
}

fn elapsed_ns(since: std::time::Instant) -> u64 {
    u64::try_from(since.elapsed().as_nanos()).unwrap_or(u64::MAX)
}

/// The same ranking as `rank_returned_ranges_excluding`; `phases` only observes it.
#[allow(clippy::too_many_arguments)]
pub fn rank_returned_ranges_excluding_traced(
    geometry: Sq8Geometry,
    ranges: &[ReturnedRange<'_>],
    query: &[f32],
    low: &[f32],
    step: &[f32],
    top_k: usize,
    max_bytes: usize,
    excluded_ids: &[i64],
    mut phases: Option<&mut RankPhases>,
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
        let scored_at = phases.is_some().then(std::time::Instant::now);
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
        if let (Some(phases), Some(since)) = (phases.as_deref_mut(), scored_at) {
            phases.score_ns = phases.score_ns.saturating_add(elapsed_ns(since));
            phases.ranges = phases.ranges.saturating_add(1);
            phases.rows = phases.rows.saturating_add(count as u64);
        }
        let rostered_at = phases.is_some().then(std::time::Instant::now);
        for mut score in local {
            if !ids.insert(score.id) {
                return Err(Sq8ScoreError::InvalidPlane);
            }
            if excluded_ids.binary_search(&score.id).is_err() {
                score.ordinal += first_row;
                scores.push(score);
            }
        }
        if let (Some(phases), Some(since)) = (phases.as_deref_mut(), rostered_at) {
            phases.roster_ns = phases.roster_ns.saturating_add(elapsed_ns(since));
        }
        previous_end = end;
    }
    let sorted_at = phases.is_some().then(std::time::Instant::now);
    scores.sort_by(|left, right| {
        left.score
            .total_cmp(&right.score)
            .then(left.id.cmp(&right.id))
    });
    if let (Some(phases), Some(since)) = (phases.as_deref_mut(), sorted_at) {
        phases.sort_ns = elapsed_ns(since);
    }
    // Release fetched-row capacity before returning caller-owned top-k results.
    let finished_at = phases.is_some().then(std::time::Instant::now);
    let top = scores[..top_k.min(scores.len())].to_vec();
    // The duplicate-ID set and the score scratch are freed here, in their usual order (set
    // first), so the finish phase covers their release instead of leaving it unattributed.
    drop(ids);
    drop(scores);
    if let (Some(phases), Some(since)) = (phases.as_deref_mut(), finished_at) {
        phases.finish_ns = elapsed_ns(since);
    }
    Ok(top)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn overlap_nonunit_sq8_and_duplicate_truncation() {
        let a = row(10, 1., 1);
        let mut b = a.clone();
        b.extend_from_slice(&row(20, 2., 0));
        let slices = [RecordSlice { first_physical_ordinal: 0, bytes: &a },
            RecordSlice { first_physical_ordinal: 3, bytes: &b }];
        let scores = rank_unique_sq8(2, Sq8Geometry { rows: 5, dimensions: 1 },
            &slices, &[3.], &[0.], &[1.], 2, 39, &[]).unwrap();
        assert_eq!(scores.iter().map(|s| (s.id, s.ordinal)).collect::<Vec<_>>(), vec![(10, 0), (20, 4)]);
        let reference = score_nominees(&b, Sq8Geometry { rows: 2, dimensions: 1 },
            &[0, 1], &[3.], &[0.], &[1.]).unwrap();
        assert_eq!(scores[0].score.to_bits(), reference[0].score.to_bits());
        assert_eq!(scores[1].score.to_bits(), reference[1].score.to_bits());
        // Equal query scores do not establish duplicate byte identity.
        let mut conflict = a.clone();
        conflict[12] = 2;
        assert!(rank_unique_sq8(2, Sq8Geometry { rows: 5, dimensions: 1 },
            &[RecordSlice { first_physical_ordinal: 0, bytes: &a },
              RecordSlice { first_physical_ordinal: 3, bytes: &conflict }],
            &[0.], &[0.], &[1.], 1, 26, &[10]).is_err());
        conflict = a.clone();
        conflict[8..12].copy_from_slice(&2_f32.to_le_bytes());
        assert!(rank_unique_sq8(2, Sq8Geometry { rows: 5, dimensions: 1 },
            &[RecordSlice { first_physical_ordinal: 0, bytes: &a },
              RecordSlice { first_physical_ordinal: 3, bytes: &conflict }],
            &[0.], &[0.], &[1.], 1, 26, &[10]).is_err());
    }

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
    fn rank_phases_observe_without_changing_ties_exclusions_or_errors() {
        let first = row(30, 1.0, 1);
        let mut second = row(20, 1.0, 1);
        second.extend_from_slice(&row(10, 1.0, 1));
        let geometry = Sq8Geometry {
            rows: 5,
            dimensions: 1,
        };
        let ranges = [
            ReturnedRange {
                start: 0,
                bytes: &first,
            },
            ReturnedRange {
                start: 3 * 13,
                bytes: &second,
            },
        ];
        let rank =
            |ranges: &[ReturnedRange<'_>], excluded: &[i64], phases: Option<&mut RankPhases>| {
                rank_returned_ranges_excluding_traced(
                    geometry,
                    ranges,
                    &[0.0],
                    &[0.0],
                    &[1.0],
                    3,
                    39,
                    excluded,
                    phases,
                )
            };
        for excluded in [&[][..], &[20][..], &[10, 20, 30][..]] {
            let mut phases = RankPhases::default();
            let traced = rank(&ranges, excluded, Some(&mut phases)).unwrap();
            let plain = rank(&ranges, excluded, None).unwrap();
            assert_eq!(traced, plain);
            assert_eq!(
                traced
                    .iter()
                    .map(|hit| (hit.id, hit.score.to_bits()))
                    .collect::<Vec<_>>(),
                plain
                    .iter()
                    .map(|hit| (hit.id, hit.score.to_bits()))
                    .collect::<Vec<_>>()
            );
            assert_eq!((phases.ranges, phases.rows), (2, 3));
        }
        // A failure keeps the phases reached so far and the exact untraced error.
        let overlap = [
            ReturnedRange {
                start: 0,
                bytes: &first,
            },
            ReturnedRange {
                start: 0,
                bytes: &first,
            },
        ];
        let mut phases = RankPhases::default();
        assert_eq!(
            rank(&overlap, &[], Some(&mut phases)),
            Err(Sq8ScoreError::InvalidPlane)
        );
        assert_eq!(rank(&overlap, &[], None), Err(Sq8ScoreError::InvalidPlane));
        assert_eq!((phases.ranges, phases.rows, phases.sort_ns), (1, 1, 0));
        let mut untouched = RankPhases::default();
        assert_eq!(
            rank(&[], &[], Some(&mut untouched)),
            Err(Sq8ScoreError::InvalidRoster)
        );
        assert_eq!(untouched, RankPhases::default());
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
