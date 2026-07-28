use std::ops::Range;

use crate::{BorsukError, Result};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ByteRangePlan {
    pub(crate) ranges: Vec<Range<usize>>,
    pub(crate) requested_bytes: usize,
    pub(crate) gap_bytes: usize,
    pub(crate) predicted_gets: usize,
}

/// Plan sorted, non-overlapping logical slices into physical object-store GETs.
/// A gap is folded into the current GET only when it is below both the hard
/// transfer cap and the configured byte-equivalent cost of another request.
pub(crate) fn plan_byte_ranges(
    requested: &[Range<usize>],
    hard_gap_cap: usize,
    request_weight_bytes: usize,
) -> Result<ByteRangePlan> {
    let mut ranges = Vec::<Range<usize>>::new();
    let mut requested_bytes = 0usize;
    for range in requested {
        if range.start > range.end {
            return Err(BorsukError::InvalidStorage(
                "object-store byte range ends before it starts".into(),
            ));
        }
        if ranges
            .last()
            .is_some_and(|previous| range.start < previous.end)
        {
            return Err(BorsukError::InvalidStorage(
                "object-store byte ranges must be sorted and non-overlapping".into(),
            ));
        }
        requested_bytes = requested_bytes
            .checked_add(range.end - range.start)
            .ok_or_else(|| BorsukError::InvalidStorage("byte range total overflows".into()))?;
        if let Some(current) = ranges.last_mut() {
            let gap = range.start - current.end;
            if gap <= hard_gap_cap && gap <= request_weight_bytes {
                current.end = range.end;
                continue;
            }
        }
        ranges.push(range.clone());
    }
    let physical_bytes = ranges
        .iter()
        .try_fold(0usize, |total, range| {
            total.checked_add(range.end - range.start)
        })
        .ok_or_else(|| BorsukError::InvalidStorage("physical byte range total overflows".into()))?;
    Ok(ByteRangePlan {
        predicted_gets: ranges.len(),
        ranges,
        requested_bytes,
        gap_bytes: physical_bytes.saturating_sub(requested_bytes),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn planner_merges_a_gap_cheaper_than_an_extra_request() {
        let plan = plan_byte_ranges(&[0..100, 120..220], 64 * 1024, 8 * 1024).unwrap();
        assert_eq!(plan.ranges, vec![0..220]);
        assert_eq!(plan.requested_bytes, 200);
        assert_eq!(plan.gap_bytes, 20);
        assert_eq!(plan.predicted_gets, 1);
    }

    #[test]
    fn planner_keeps_a_gap_more_expensive_than_an_extra_request() {
        let plan = plan_byte_ranges(&[0..100, 5_000..5_100], 64 * 1024, 1_000).unwrap();
        assert_eq!(plan.ranges, vec![0..100, 5_000..5_100]);
        assert_eq!(plan.requested_bytes, 200);
        assert_eq!(plan.gap_bytes, 0);
        assert_eq!(plan.predicted_gets, 2);
    }

    #[test]
    fn planner_obeys_the_hard_gap_cap_and_rejects_bad_ranges() {
        let plan = plan_byte_ranges(&[0..100, 220..320], 100, 8 * 1024).unwrap();
        assert_eq!(plan.ranges, vec![0..100, 220..320]);
        let invalid = std::ops::Range { start: 10, end: 9 };
        assert!(plan_byte_ranges(std::slice::from_ref(&invalid), 100, 100).is_err());
        assert!(plan_byte_ranges(&[10..20, 5..9], 100, 100).is_err());
    }
}
