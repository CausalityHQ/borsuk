//! Exact, bounded contiguous page intervals for a one-object dense read.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::ops::Range;

/// Physical units for pages of one contiguous object.
///
/// Full pages are exact multiples of `unit_bytes`. The final page may be
/// shorter than a unit; its units are rounded up for budget accounting while
/// its physical range ends at the exact last byte.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct IntervalGeometry {
    /// Number of pages, including the possibly short final page.
    pub page_count: usize,
    /// Units charged for each page except the final page.
    pub full_page_units: usize,
    /// Exact bytes in the final page.
    pub last_page_bytes: usize,
    /// Exact bytes in one physical unit.
    pub unit_bytes: usize,
    /// Maximum number of contiguous object ranges.
    pub max_gets: usize,
    /// Maximum physical units across all ranges, including bridged pages.
    pub max_units: usize,
}

/// One exact weighted physical read plan.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IntervalPlan {
    /// Sum of weights on every covered page.
    pub score: u64,
    /// Nonoverlapping, ascending, half-open byte ranges into one object.
    pub ranges: Vec<Range<usize>>,
    /// Exact sum of range lengths.
    pub bytes: usize,
}

/// Exact minimum cover of mandatory whole pages under one GET cap.
/// The byte floor counts a short final page by its actual physical length;
/// the unit floor charges that page rounded up to a physical unit.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PrimaryCoverFloor {
    /// Number of distinct mandatory physical pages.
    pub primary_pages: usize,
    /// Number of contiguous mandatory runs before bridging gaps.
    pub disconnected_runs: usize,
    /// Number of previously empty pages added by the cheapest bridges.
    pub bridged_pages: usize,
    /// Rounded physical units charged by the minimum cover.
    pub minimum_units: usize,
    /// Smallest caller byte cap accepted by the rounded-unit budget lattice.
    pub minimum_budget_bytes: usize,
    /// Exact physical bytes read by the minimum cover; may be smaller.
    pub minimum_bytes: usize,
}

/// A malformed or unrepresentable physical planning request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PlanError {
    /// The page or budget geometry is invalid.
    InvalidGeometry,
    /// Weights are zero, out of order, duplicated or outside the object.
    InvalidWeights,
    /// Byte or score arithmetic exceeded the machine representation.
    ArithmeticOverflow,
    /// The requested state space is too large to plan safely.
    BudgetTooLarge,
    /// Reconstructing an optimum did not match its dynamic-program state.
    InconsistentWitness,
    /// Mandatory primary pages cannot fit the caller's GET/unit/byte caps.
    InsufficientBudget,
}

/// Calculate the minimum mandatory cover by joining the shortest run gaps.
/// This uses only physical geometry and the caller's GET cap, so it may be
/// checked before optional utility scores or ground truth are available.
pub fn minimum_primary_cover(
    geometry: IntervalGeometry,
    primary_pages: &[usize],
) -> Result<PrimaryCoverFloor, PlanError> {
    let full_page_bytes = geometry
        .full_page_units
        .checked_mul(geometry.unit_bytes)
        .ok_or(PlanError::ArithmeticOverflow)?;
    if (geometry.page_count == 0
        || geometry.full_page_units == 0
        || geometry.unit_bytes == 0
        || geometry.last_page_bytes == 0
        || geometry.last_page_bytes > full_page_bytes
        || geometry.max_gets == 0
        || primary_pages.is_empty())
    {
        return Err(PlanError::InvalidGeometry);
    }
    let primary = primary_pages.iter().copied().collect::<BTreeSet<_>>();
    if primary.iter().any(|&page| page >= geometry.page_count) {
        return Err(PlanError::InvalidWeights);
    }
    let final_page = geometry.page_count - 1;
    let final_units = geometry.last_page_bytes.div_ceil(geometry.unit_bytes);
    let mut minimum_units = primary
        .len()
        .checked_mul(geometry.full_page_units)
        .ok_or(PlanError::ArithmeticOverflow)?;
    let mut minimum_bytes = primary
        .len()
        .checked_mul(full_page_bytes)
        .ok_or(PlanError::ArithmeticOverflow)?;
    if primary.contains(&final_page) {
        minimum_units -= geometry.full_page_units - final_units;
        minimum_bytes -= full_page_bytes - geometry.last_page_bytes;
    }
    let mut gaps = Vec::new();
    let mut previous = None;
    for &page in &primary {
        if let Some(before) = previous
            && page > before + 1
        {
            gaps.push(page - before - 1);
        }
        previous = Some(page);
    }
    let disconnected_runs = gaps.len() + 1;
    let bridges = disconnected_runs.saturating_sub(geometry.max_gets);
    gaps.sort_unstable();
    let bridged_pages = gaps
        .into_iter()
        .take(bridges)
        .try_fold(0usize, |sum, gap| {
            sum.checked_add(gap).ok_or(PlanError::ArithmeticOverflow)
        })?;
    minimum_units = minimum_units
        .checked_add(
            bridged_pages
                .checked_mul(geometry.full_page_units)
                .ok_or(PlanError::ArithmeticOverflow)?,
        )
        .ok_or(PlanError::ArithmeticOverflow)?;
    minimum_bytes = minimum_bytes
        .checked_add(
            bridged_pages
                .checked_mul(full_page_bytes)
                .ok_or(PlanError::ArithmeticOverflow)?,
        )
        .ok_or(PlanError::ArithmeticOverflow)?;
    let minimum_budget_bytes = minimum_units
        .checked_mul(geometry.unit_bytes)
        .ok_or(PlanError::ArithmeticOverflow)?;
    Ok(PrimaryCoverFloor {
        primary_pages: primary.len(),
        disconnected_runs,
        bridged_pages,
        minimum_units,
        minimum_budget_bytes,
        minimum_bytes,
    })
}

/// Build the source-only primary/secondary page vote plan from physical SQ8
/// row ordinals. The primary weight is derived from the nominee roster size,
/// so one primary vote dominates every secondary vote at any vector count.
/// Page and byte budgets are explicit policy inputs; no corpus-size branch is
/// hidden in the planner.
pub fn plan_weighted_nominee_pages(
    rows: usize,
    dimensions: usize,
    page_rows: usize,
    primary: &[usize],
    nominees: &[usize],
    max_gets: usize,
    max_bytes: usize,
) -> Result<(Vec<(usize, u32)>, IntervalPlan), PlanError> {
    const UNIT_ROWS: usize = 32;
    if rows == 0
        || dimensions == 0
        || page_rows == 0
        || page_rows % UNIT_ROWS != 0
        || primary.is_empty()
        || nominees.is_empty()
        || max_gets == 0
    {
        return Err(PlanError::InvalidGeometry);
    }
    let row_bytes = dimensions
        .checked_add(12)
        .ok_or(PlanError::ArithmeticOverflow)?;
    let unit_bytes = UNIT_ROWS
        .checked_mul(row_bytes)
        .ok_or(PlanError::ArithmeticOverflow)?;
    let primary_set = primary.iter().copied().collect::<HashSet<_>>();
    let nominee_set = nominees.iter().copied().collect::<HashSet<_>>();
    if primary_set.len() != primary.len()
        || nominee_set.len() != nominees.len()
        || !primary_set.is_subset(&nominee_set)
        || nominees.iter().any(|&ordinal| ordinal >= rows)
    {
        return Err(PlanError::InvalidWeights);
    }
    let primary_weight = nominees
        .len()
        .checked_add(1)
        .and_then(|weight| u32::try_from(weight).ok())
        .ok_or(PlanError::ArithmeticOverflow)?;
    let mut weights = BTreeMap::<usize, u32>::new();
    for &ordinal in nominees {
        let weight = weights.entry(ordinal / page_rows).or_default();
        *weight = weight
            .checked_add(if primary_set.contains(&ordinal) {
                primary_weight
            } else {
                1
            })
            .ok_or(PlanError::ArithmeticOverflow)?;
    }
    let page_count = rows.div_ceil(page_rows);
    let final_rows = rows - (page_count - 1) * page_rows;
    let votes = weights.into_iter().collect::<Vec<_>>();
    let geometry = IntervalGeometry {
        page_count,
        full_page_units: page_rows / UNIT_ROWS,
        last_page_bytes: final_rows
            .checked_mul(row_bytes)
            .ok_or(PlanError::ArithmeticOverflow)?,
        unit_bytes,
        max_gets,
        max_units: max_bytes / unit_bytes,
    };
    let primary_pages = primary
        .iter()
        .map(|row| row / page_rows)
        .collect::<Vec<_>>();
    let floor = minimum_primary_cover(geometry, &primary_pages)?;
    if floor.minimum_budget_bytes > max_bytes {
        return Err(PlanError::InsufficientBudget);
    }
    let plan = plan_weighted_intervals(normalize_budget_lattice(geometry)?, &votes)?;
    if plan.bytes > max_bytes || plan.ranges.len() > max_gets || plan.ranges.is_empty() {
        return Err(PlanError::InconsistentWitness);
    }
    let full_page_bytes = page_rows
        .checked_mul(row_bytes)
        .ok_or(PlanError::ArithmeticOverflow)?;
    for page in primary_pages {
        let offset = page
            .checked_mul(full_page_bytes)
            .ok_or(PlanError::ArithmeticOverflow)?;
        if !plan
            .ranges
            .iter()
            .any(|range| range.start <= offset && offset < range.end)
        {
            return Err(PlanError::InconsistentWitness);
        }
    }
    Ok((votes, plan))
}

impl std::fmt::Display for PlanError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "physical interval plan error: {self:?}")
    }
}

impl std::error::Error for PlanError {}

/// Divide the budget lattice by every page charge's common divisor.
///
/// Full pages cost `full_page_units`; the short final page costs
/// `ceil(last_page_bytes / unit_bytes)`. Every feasible plan uses a multiple
/// of their gcd, so dividing all charges and the budget by it preserves the
/// exact feasible plans and physical byte ranges while shrinking DP state.
pub fn normalize_budget_lattice(geometry: IntervalGeometry) -> Result<IntervalGeometry, PlanError> {
    let full_page_bytes = geometry
        .full_page_units
        .checked_mul(geometry.unit_bytes)
        .ok_or(PlanError::ArithmeticOverflow)?;
    if geometry.full_page_units == 0
        || geometry.unit_bytes == 0
        || geometry.last_page_bytes == 0
        || geometry.last_page_bytes > full_page_bytes
    {
        return Err(PlanError::InvalidGeometry);
    }
    let mut left = geometry.full_page_units;
    let mut right = geometry.last_page_bytes.div_ceil(geometry.unit_bytes);
    while right != 0 {
        (left, right) = (right, left % right);
    }
    Ok(IntervalGeometry {
        full_page_units: geometry.full_page_units / left,
        unit_bytes: geometry
            .unit_bytes
            .checked_mul(left)
            .ok_or(PlanError::ArithmeticOverflow)?,
        max_units: geometry.max_units / left,
        ..geometry
    })
}

struct Step {
    page: usize,
    page_units: usize,
    continuation_units: usize,
    closed_from_open: Vec<bool>,
    continued: Vec<bool>,
}

/// Maximize positive page weights under GET and byte budgets.
///
/// `weights` must be strictly sorted by page. The planner may bridge pages
/// without weight, and charges their bytes. Ground truth is not part of this
/// interface; the caller supplies only serving-time page weights. Ties choose
/// a deterministic state preferring fewer GETs, then fewer charged bytes.
pub fn plan_weighted_intervals(
    geometry: IntervalGeometry,
    weights: &[(usize, u32)],
) -> Result<IntervalPlan, PlanError> {
    if geometry.page_count == 0
        || geometry.full_page_units == 0
        || geometry.last_page_bytes == 0
        || geometry.unit_bytes == 0
        || geometry.max_gets == 0
    {
        return Err(PlanError::InvalidGeometry);
    }
    let full_page_bytes = geometry
        .full_page_units
        .checked_mul(geometry.unit_bytes)
        .ok_or(PlanError::ArithmeticOverflow)?;
    if geometry.last_page_bytes > full_page_bytes {
        return Err(PlanError::InvalidGeometry);
    }
    let last_page_units = geometry.last_page_bytes.div_ceil(geometry.unit_bytes);
    let geometry = IntervalGeometry {
        max_gets: geometry.max_gets.min(geometry.page_count),
        ..geometry
    };
    if weights
        .iter()
        .any(|(page, weight)| *page >= geometry.page_count || *weight == 0)
        || weights.windows(2).any(|pair| pair[0].0 >= pair[1].0)
    {
        return Err(PlanError::InvalidWeights);
    }
    // If the smallest interval spanning every positive page fits, it already
    // has the maximum possible score. Any full-score plan needs at least one
    // range; among one-range plans this hull charges the fewest units. This
    // is the DP's exact optimum under its score/GET/byte tie order.
    if let (Some(&(first, _)), Some(&(last, _))) = (weights.first(), weights.last()) {
        let span_pages = last - first + 1;
        let charged_units = if last == geometry.page_count - 1 {
            (span_pages - 1)
                .checked_mul(geometry.full_page_units)
                .and_then(|units| units.checked_add(last_page_units))
        } else {
            span_pages.checked_mul(geometry.full_page_units)
        }
        .ok_or(PlanError::ArithmeticOverflow)?;
        if charged_units <= geometry.max_units {
            let start = first
                .checked_mul(full_page_bytes)
                .ok_or(PlanError::ArithmeticOverflow)?;
            let end = if last == geometry.page_count - 1 {
                (geometry.page_count - 1)
                    .checked_mul(full_page_bytes)
                    .and_then(|prefix| prefix.checked_add(geometry.last_page_bytes))
            } else {
                (last + 1).checked_mul(full_page_bytes)
            }
            .ok_or(PlanError::ArithmeticOverflow)?;
            let score = weights.iter().try_fold(0u64, |sum, &(_, weight)| {
                sum.checked_add(u64::from(weight))
                    .ok_or(PlanError::ArithmeticOverflow)
            })?;
            return Ok(IntervalPlan {
                score,
                ranges: vec![start..end],
                bytes: end - start,
            });
        }
    }
    let gets_width = geometry
        .max_gets
        .checked_add(1)
        .ok_or(PlanError::ArithmeticOverflow)?;
    let units_width = geometry
        .max_units
        .checked_add(1)
        .ok_or(PlanError::ArithmeticOverflow)?;
    let states = gets_width
        .checked_mul(units_width)
        .ok_or(PlanError::ArithmeticOverflow)?;
    if states > 4_000_000 {
        return Err(PlanError::BudgetTooLarge);
    }
    if states
        .checked_mul(weights.len())
        .ok_or(PlanError::ArithmeticOverflow)?
        > 32_000_000
    {
        return Err(PlanError::BudgetTooLarge);
    }
    let index = |gets: usize, units: usize| gets * units_width + units;
    let mut closed = vec![None::<u64>; states];
    let mut opened = vec![None::<u64>; states];
    closed[0] = Some(0);
    let mut history = Vec::with_capacity(weights.len());
    let mut previous_page: Option<usize> = None;
    for &(page, weight) in weights {
        let page_units = if page == geometry.page_count - 1 {
            last_page_units
        } else {
            geometry.full_page_units
        };
        let gap_units = previous_page
            .map(|previous| {
                (page - previous - 1)
                    .checked_mul(geometry.full_page_units)
                    .ok_or(PlanError::ArithmeticOverflow)
            })
            .transpose()?
            .unwrap_or(0);
        let continuation_units = gap_units
            .checked_add(page_units)
            .ok_or(PlanError::ArithmeticOverflow)?;
        let mut base = vec![None; states];
        let mut closed_from_open = vec![false; states];
        for slot in 0..states {
            closed_from_open[slot] = opened[slot] > closed[slot];
            base[slot] = opened[slot].max(closed[slot]);
        }
        let mut next_open = vec![None; states];
        let mut continued = vec![false; states];
        if page_units <= geometry.max_units {
            for gets in 0..geometry.max_gets {
                for units in 0..=geometry.max_units - page_units {
                    if let Some(score) = base[index(gets, units)] {
                        let proposed = score
                            .checked_add(u64::from(weight))
                            .ok_or(PlanError::ArithmeticOverflow)?;
                        next_open[index(gets + 1, units + page_units)] = Some(proposed);
                    }
                }
            }
        }
        if continuation_units <= geometry.max_units {
            for gets in 0..=geometry.max_gets {
                for units in 0..=geometry.max_units - continuation_units {
                    if let Some(score) = opened[index(gets, units)] {
                        let proposed = score
                            .checked_add(u64::from(weight))
                            .ok_or(PlanError::ArithmeticOverflow)?;
                        let target = index(gets, units + continuation_units);
                        if Some(proposed) > next_open[target] {
                            next_open[target] = Some(proposed);
                            continued[target] = true;
                        }
                    }
                }
            }
        }
        history.push(Step {
            page,
            page_units,
            continuation_units,
            closed_from_open,
            continued,
        });
        closed = base;
        opened = next_open;
        previous_page = Some(page);
    }

    let mut best_score = 0u64;
    let (mut best_gets, mut best_units, mut best_open) = (0, 0, false);
    for gets in 0..=geometry.max_gets {
        for units in 0..=geometry.max_units {
            for (is_open, state) in [(false, &closed), (true, &opened)] {
                if let Some(score) = state[index(gets, units)] {
                    if score > best_score {
                        best_score = score;
                        (best_gets, best_units, best_open) = (gets, units, is_open);
                    }
                }
            }
        }
    }
    let selected_units = best_units;
    let mut page_intervals = Vec::new();
    let mut pending_end = None;
    for step in history.iter().rev() {
        if !best_open {
            best_open = step.closed_from_open[index(best_gets, best_units)];
            continue;
        }
        pending_end.get_or_insert(step.page);
        if step.continued[index(best_gets, best_units)] {
            best_units -= step.continuation_units;
        } else {
            page_intervals.push((step.page, pending_end.take().unwrap()));
            best_gets -= 1;
            best_units -= step.page_units;
            best_open = step.closed_from_open[index(best_gets, best_units)];
        }
    }
    if pending_end.is_some() || best_gets != 0 || best_units != 0 || best_open {
        return Err(PlanError::InconsistentWitness);
    }
    page_intervals.reverse();
    let object_bytes = (geometry.page_count - 1)
        .checked_mul(full_page_bytes)
        .and_then(|value| value.checked_add(geometry.last_page_bytes))
        .ok_or(PlanError::ArithmeticOverflow)?;
    let mut ranges = Vec::with_capacity(page_intervals.len());
    let mut bytes = 0usize;
    for (start_page, end_page) in page_intervals {
        let start = start_page
            .checked_mul(full_page_bytes)
            .ok_or(PlanError::ArithmeticOverflow)?;
        let end = if end_page == geometry.page_count - 1 {
            object_bytes
        } else {
            (end_page + 1)
                .checked_mul(full_page_bytes)
                .ok_or(PlanError::ArithmeticOverflow)?
        };
        bytes = bytes
            .checked_add(end - start)
            .ok_or(PlanError::ArithmeticOverflow)?;
        ranges.push(start..end);
    }
    let cap_bytes = geometry
        .max_units
        .checked_mul(geometry.unit_bytes)
        .ok_or(PlanError::ArithmeticOverflow)?;
    let charged_bytes = selected_units
        .checked_mul(geometry.unit_bytes)
        .ok_or(PlanError::ArithmeticOverflow)?;
    let selected_final_page = ranges.iter().any(|range| range.end == object_bytes);
    let final_page_slack = last_page_units * geometry.unit_bytes - geometry.last_page_bytes;
    let selected_bytes = charged_bytes
        - if selected_final_page {
            final_page_slack
        } else {
            0
        };
    let witnessed_score = weights.iter().try_fold(0u64, |score, &(page, weight)| {
        let offset = page
            .checked_mul(full_page_bytes)
            .ok_or(PlanError::ArithmeticOverflow)?;
        if ranges
            .iter()
            .any(|range| range.start <= offset && offset < range.end)
        {
            score
                .checked_add(u64::from(weight))
                .ok_or(PlanError::ArithmeticOverflow)
        } else {
            Ok(score)
        }
    })?;
    if ranges.len() > geometry.max_gets
        || bytes > cap_bytes
        || bytes != selected_bytes
        || witnessed_score != best_score
        || ranges.windows(2).any(|pair| pair[0].end >= pair[1].start)
    {
        return Err(PlanError::InconsistentWitness);
    }
    Ok(IntervalPlan {
        score: best_score,
        ranges,
        bytes,
    })
}

#[cfg(test)]
mod tests {
    use super::{
        IntervalGeometry, PlanError, minimum_primary_cover, normalize_budget_lattice,
        plan_weighted_intervals, plan_weighted_nominee_pages,
    };

    fn geometry(page_count: usize, max_gets: usize, max_units: usize) -> IntervalGeometry {
        IntervalGeometry {
            page_count,
            full_page_units: 3,
            last_page_bytes: 7,
            unit_bytes: 7,
            max_gets,
            max_units,
        }
    }

    #[test]
    fn normalized_lattice_preserves_exact_plan_with_short_tail() {
        let original = IntervalGeometry {
            page_count: 8,
            full_page_units: 8,
            last_page_bytes: 28,
            unit_bytes: 7,
            max_gets: 3,
            max_units: 37,
        };
        let normalized = normalize_budget_lattice(original).unwrap();
        assert_eq!(normalized.full_page_units, 2);
        assert_eq!(normalized.unit_bytes, 28);
        assert_eq!(normalized.max_units, 9);
        for mask in 0..256usize {
            let weights = (0..8)
                .filter(|page| mask & (1 << page) != 0)
                .map(|page| (page, (page + 1) as u32))
                .collect::<Vec<_>>();
            assert_eq!(
                plan_weighted_intervals(original, &weights),
                plan_weighted_intervals(normalized, &weights),
                "mask={mask}",
            );
        }
    }

    #[test]
    fn normalized_d96_ten_million_lattice_stays_within_work_guard() {
        let geometry = normalize_budget_lattice(IntervalGeometry {
            page_count: 39_024,
            full_page_units: 8,
            last_page_bytes: 112 * 108,
            unit_bytes: 32 * 108,
            max_gets: 32,
            max_units: 16_777_216 / (32 * 108),
        })
        .unwrap();
        assert_eq!(geometry.full_page_units, 2);
        assert_eq!(geometry.max_units, 1_213);
        assert_eq!(
            (geometry.max_gets + 1) * (geometry.max_units + 1) * 368,
            14_742_816
        );
    }

    fn brute_force(weights: &[(usize, u32)], pages: usize, gets: usize, units: usize) -> u64 {
        let mut best = 0;
        for mask in 0..(1usize << pages) {
            let requests = (0..pages)
                .filter(|&page| {
                    mask & (1 << page) != 0 && (page == 0 || mask & (1 << (page - 1)) == 0)
                })
                .count();
            let paid = (0..pages)
                .filter(|&page| mask & (1 << page) != 0)
                .map(|page| if page == pages - 1 { 1 } else { 3 })
                .sum::<usize>();
            if requests <= gets && paid <= units {
                best = best.max(
                    weights
                        .iter()
                        .filter(|(page, _)| mask & (1 << page) != 0)
                        .map(|(_, weight)| u64::from(*weight))
                        .sum(),
                );
            }
        }
        best
    }

    #[test]
    fn witness_matches_exhaustive_seven_page_plans() {
        let mut seed = 11101u64;
        for _ in 0..100 {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
            let mut weights = Vec::new();
            for page in 0..7 {
                seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
                if seed >> 17 & 1 == 1 {
                    weights.push((page, ((seed >> 8) % 8 + 1) as u32));
                }
            }
            let gets = (seed as usize % 3) + 1;
            let units = ((seed >> 16) as usize % 12) + 1;
            let plan = plan_weighted_intervals(geometry(7, gets, units), &weights).unwrap();
            assert_eq!(plan.score, brute_force(&weights, 7, gets, units));
            assert!(plan.ranges.len() <= gets);
            assert!(plan.bytes <= units * 7);
            let witnessed = weights
                .iter()
                .filter(|(page, _)| {
                    let start = page * 3 * 7;
                    plan.ranges
                        .iter()
                        .any(|range| range.start <= start && start < range.end)
                })
                .map(|(_, weight)| u64::from(*weight))
                .sum::<u64>();
            assert_eq!(plan.score, witnessed);
        }
    }

    #[test]
    fn final_short_page_costs_one_unit() {
        let plan = plan_weighted_intervals(
            IntervalGeometry {
                page_count: 3907,
                full_page_units: 4,
                last_page_bytes: 49_920,
                unit_bytes: 49_920,
                max_gets: 32,
                max_units: 336,
            },
            &[(3906, 1)],
        )
        .unwrap();
        assert_eq!(plan.score, 1);
        assert_eq!(plan.bytes, 49_920);
        assert_eq!(plan.ranges, vec![779_950_080..780_000_000]);
    }

    #[test]
    fn hundred_thousand_row_final_page_uses_five_units() {
        let plan = plan_weighted_intervals(
            IntervalGeometry {
                page_count: 391,
                full_page_units: 8,
                last_page_bytes: 124_800,
                unit_bytes: 24_960,
                max_gets: 32,
                max_units: 672,
            },
            &[(390, 1)],
        )
        .unwrap();
        assert_eq!(plan.ranges, vec![77_875_200..78_000_000]);
        assert_eq!(plan.bytes, 124_800);
    }

    #[test]
    fn partial_unit_tail_emits_exact_range_and_stays_within_cap() {
        let plan = plan_weighted_intervals(
            IntervalGeometry {
                page_count: 2,
                full_page_units: 8,
                last_page_bytes: 17 * 780,
                unit_bytes: 32 * 780,
                max_gets: 1,
                max_units: 1,
            },
            &[(1, 1)],
        )
        .unwrap();
        assert_eq!(plan.ranges, vec![256 * 780..273 * 780]);
        assert_eq!(plan.bytes, 17 * 780);
    }

    #[test]
    fn high_primary_vote_beats_all_secondary_votes() {
        let plan = plan_weighted_intervals(geometry(3, 1, 3), &[(0, 412), (1, 513)]).unwrap();
        assert_eq!(plan.score, 513);
        assert_eq!(plan.ranges, vec![21..42]);
    }

    #[test]
    fn small_object_accepts_larger_get_budget_and_bridges_pages() {
        let plan = plan_weighted_intervals(geometry(3, 32, 7), &[(0, 1), (2, 1)]).unwrap();
        assert_eq!(plan.score, 2);
        assert_eq!(plan.ranges, vec![0..49]);
        assert_eq!(plan.bytes, 49);
    }

    #[test]
    fn affordable_positive_weight_hull_is_exact_one_range_optimum() {
        let weights = [(1, 513), (6, 1)];
        let shortcut = plan_weighted_intervals(geometry(7, 2, 16), &weights).unwrap();
        assert_eq!(shortcut.score, 514);
        assert_eq!(shortcut.ranges, vec![21..133]);
        assert_eq!(shortcut.bytes, 112);

        let constrained = plan_weighted_intervals(geometry(7, 2, 15), &weights).unwrap();
        assert_eq!(constrained.score, 514);
        assert_eq!(constrained.ranges, vec![21..42, 126..133]);
        assert_eq!(constrained.bytes, 28);
    }

    #[test]
    fn invalid_weights_fail_closed() {
        assert_eq!(
            plan_weighted_intervals(geometry(7, 2, 12), &[(2, 1), (2, 2)]),
            Err(PlanError::InvalidWeights)
        );
        assert_eq!(
            plan_weighted_intervals(geometry(7, 2, 12), &[(3, 1), (2, 2)]),
            Err(PlanError::InvalidWeights)
        );
        assert_eq!(
            plan_weighted_intervals(geometry(7, 2, 12), &[(2, 0)]),
            Err(PlanError::InvalidWeights)
        );
    }

    #[test]
    fn nominee_votes_preserve_primary_priority_and_short_tail() {
        let (votes, plan) = plan_weighted_nominee_pages(
            416,
            768,
            256,
            &[0, 256, 300],
            &[0, 1, 256, 257, 300],
            32,
            16_777_216,
        )
        .unwrap();
        assert_eq!(votes, vec![(0, 7), (1, 13)]);
        assert_eq!(plan.score, 20);
        assert_eq!(plan.ranges, vec![0..416 * 780]);
        assert_eq!(plan.bytes, 416 * 780);

        assert_eq!(
            plan_weighted_nominee_pages(
                416,
                768,
                256,
                &[0, 256],
                &[0, 1, 256, 256],
                32,
                16_777_216
            ),
            Err(PlanError::InvalidWeights),
        );
        assert_eq!(
            plan_weighted_nominee_pages(416, 768, 256, &[0, 300], &[0, 1, 256], 32, 16_777_216),
            Err(PlanError::InvalidWeights),
        );
    }

    #[test]
    fn mandatory_primary_floor_bridges_shortest_gap_and_rejects_small_cap() {
        let geometry = IntervalGeometry {
            page_count: 10,
            full_page_units: 1,
            last_page_bytes: 416,
            unit_bytes: 416,
            max_gets: 2,
            max_units: 3,
        };
        let floor = minimum_primary_cover(geometry, &[0, 2, 5]).unwrap();
        assert_eq!(floor.primary_pages, 3);
        assert_eq!(floor.disconnected_runs, 3);
        assert_eq!(floor.bridged_pages, 1);
        assert_eq!(floor.minimum_units, 4);
        assert_eq!(floor.minimum_budget_bytes, 1_664);
        assert_eq!(floor.minimum_bytes, 1_664);
        assert_eq!(
            plan_weighted_nominee_pages(320, 1, 32, &[0, 64, 160], &[0, 64, 160], 2, 1_248),
            Err(PlanError::InsufficientBudget),
        );
        let (_, plan) =
            plan_weighted_nominee_pages(320, 1, 32, &[0, 64, 160], &[0, 64, 160], 2, 1_664)
                .unwrap();
        assert_eq!(plan.ranges, vec![0..1_248, 2_080..2_496]);
    }

    #[test]
    fn mandatory_floor_charges_exact_short_final_page_bytes() {
        let floor = minimum_primary_cover(
            IntervalGeometry {
                page_count: 10,
                full_page_units: 1,
                last_page_bytes: 130,
                unit_bytes: 416,
                max_gets: 2,
                max_units: 2,
            },
            &[0, 9],
        )
        .unwrap();
        assert_eq!(floor.minimum_units, 2);
        assert_eq!(floor.minimum_budget_bytes, 832);
        assert_eq!(floor.minimum_bytes, 546);
        assert_eq!(
            plan_weighted_nominee_pages(298, 1, 32, &[0, 288], &[0, 288], 2, 546),
            Err(PlanError::InsufficientBudget),
        );
        let (_, plan) =
            plan_weighted_nominee_pages(298, 1, 32, &[0, 288], &[0, 288], 2, 832).unwrap();
        assert_eq!(plan.bytes, 546);
    }

    #[test]
    fn mandatory_floor_matches_exhaustive_six_page_covers() {
        for primary_mask in 1..64usize {
            let primary = (0..6)
                .filter(|&page| primary_mask & (1 << page) != 0)
                .collect::<Vec<_>>();
            for max_gets in 1..=3 {
                let floor = minimum_primary_cover(
                    IntervalGeometry {
                        page_count: 6,
                        full_page_units: 2,
                        last_page_bytes: 7,
                        unit_bytes: 7,
                        max_gets,
                        max_units: 12,
                    },
                    &primary,
                )
                .unwrap();
                let mut best = (usize::MAX, usize::MAX);
                for selected_mask in 1..64usize {
                    if selected_mask & primary_mask != primary_mask {
                        continue;
                    }
                    let gets = (0..6)
                        .filter(|&page| {
                            selected_mask & (1 << page) != 0
                                && (page == 0 || selected_mask & (1 << (page - 1)) == 0)
                        })
                        .count();
                    if gets > max_gets {
                        continue;
                    }
                    let units = (0..6)
                        .filter(|&page| selected_mask & (1 << page) != 0)
                        .map(|page| if page == 5 { 1 } else { 2 })
                        .sum();
                    let bytes = (0..6)
                        .filter(|&page| selected_mask & (1 << page) != 0)
                        .map(|page| if page == 5 { 7 } else { 14 })
                        .sum();
                    best = best.min((units, bytes));
                }
                assert_eq!(
                    (floor.minimum_units, floor.minimum_bytes),
                    best,
                    "primary_mask={primary_mask}, max_gets={max_gets}"
                );
                assert_eq!(floor.minimum_budget_bytes, best.0 * 7);
            }
        }
    }
}
