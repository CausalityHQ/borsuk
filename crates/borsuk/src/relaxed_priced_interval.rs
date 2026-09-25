//! Exact one-cap priced interval relaxation with checked physical witness.
//!
//! This solver enforces either units or GETs, then prices the other resource.
//! Its result may serve a two-cap request only after the caller admits it
//! against the other hard cap. Otherwise a stricter solver is required.

use std::collections::{BTreeMap, BTreeSet};

use crate::unconstrained_priced_interval::{CoverError, UnconstrainedCover};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FixedCap {
    Units,
    Gets,
}

#[derive(Clone, Copy)]
struct State {
    mass: i64,
    units: usize,
    gets: usize,
}

struct Trace {
    site: usize,
    continuation: usize,
    required: bool,
    closed_from_open: Vec<bool>,
    continued: Vec<bool>,
}

fn key(state: State, unit_price: i64, get_price: i64) -> (i64, i64, i64, i64) {
    let units = state.units as i64;
    let gets = state.gets as i64;
    (
        state.mass - unit_price * units - get_price * gets,
        -units,
        -gets,
        state.mass,
    )
}

fn prefer(
    left: Option<State>,
    right: Option<State>,
    unit_price: i64,
    get_price: i64,
) -> Option<State> {
    match (left, right) {
        (None, other) | (other, None) => other,
        (Some(a), Some(b)) => Some(
            if key(b, unit_price, get_price) > key(a, unit_price, get_price) {
                b
            } else {
                a
            },
        ),
    }
}

/// Maximize priced mass subject to one hard resource cap. A result that
/// independently meets the other cap is exact for the two-cap problem.
pub fn relaxed_priced_cover(
    weights: &BTreeMap<usize, i64>,
    mandatory: &[usize],
    page_count: usize,
    fixed_cap: FixedCap,
    cap: usize,
    unit_price: i64,
    get_price: i64,
    trace_budget_bytes: usize,
) -> Result<UnconstrainedCover, CoverError> {
    if page_count == 0
        || page_count > i64::MAX as usize
        || cap == 0
        || cap > page_count
        || trace_budget_bytes == 0
        || unit_price < 0
        || get_price < 0
        || unit_price
            .checked_add(get_price)
            .and_then(|sum| sum.checked_mul(page_count as i64))
            .is_none_or(|charge| charge > i64::MAX - (1 << 30))
    {
        return Err(CoverError::InvalidGeometry);
    }
    let required = mandatory.iter().copied().collect::<BTreeSet<_>>();
    if required.len() != mandatory.len()
        || required.iter().any(|site| *site >= page_count)
        || weights
            .iter()
            .any(|(&site, &weight)| site >= page_count || weight <= 0)
    {
        return Err(CoverError::InvalidGeometry);
    }
    let mut total_mass = 0_i64;
    for &weight in weights.values() {
        total_mass = total_mass
            .checked_add(weight)
            .ok_or(CoverError::ArithmeticOverflow)?;
    }
    if total_mass >= 1 << 30 {
        return Err(CoverError::InvalidGeometry);
    }
    let sites = required
        .iter()
        .copied()
        .chain(weights.keys().copied())
        .collect::<BTreeSet<_>>();
    if sites.is_empty() {
        return Ok(UnconstrainedCover {
            intervals: Vec::new(),
            mass: 0,
            units: 0,
            gets: 0,
            objective: 0,
        });
    }
    let trace_bytes = sites
        .len()
        .checked_mul(cap + 1)
        .and_then(|n| n.checked_mul(2))
        .ok_or(CoverError::ArithmeticOverflow)?;
    if trace_bytes > trace_budget_bytes {
        return Err(CoverError::TraceBudgetExceeded);
    }
    let mut closed = vec![None; cap + 1];
    let mut opened = vec![None; cap + 1];
    closed[0] = Some(State {
        mass: 0,
        units: 0,
        gets: 0,
    });
    let mut history = Vec::with_capacity(sites.len());
    let mut previous = None;
    for site in sites {
        let weight = *weights.get(&site).unwrap_or(&0);
        let gap = previous.map_or(1, |prev| site - prev);
        let continuation = if fixed_cap == FixedCap::Units { gap } else { 0 };
        let mut next_closed = vec![None; cap + 1];
        let mut next_open = vec![None; cap + 1];
        let mut closed_from_open = vec![false; cap + 1];
        let mut continued = vec![false; cap + 1];
        for slot in 0..=cap {
            let take_open = match (closed[slot], opened[slot]) {
                (Some(a), Some(b)) => key(b, unit_price, get_price) > key(a, unit_price, get_price),
                (None, Some(_)) => true,
                _ => false,
            };
            closed_from_open[slot] = take_open;
            let base = prefer(closed[slot], opened[slot], unit_price, get_price);
            if !required.contains(&site) {
                next_closed[slot] = base;
            }
            if slot < cap {
                if let Some(state) = base {
                    let candidate = State {
                        mass: state.mass + weight,
                        units: state.units + 1,
                        gets: state.gets + 1,
                    };
                    let target = slot + 1;
                    if next_open[target].is_none_or(|old| {
                        key(candidate, unit_price, get_price) > key(old, unit_price, get_price)
                    }) {
                        next_open[target] = Some(candidate);
                        continued[target] = false;
                    }
                }
            }
            if continuation <= cap && slot <= cap - continuation {
                if let Some(state) = opened[slot] {
                    let candidate = State {
                        mass: state.mass + weight,
                        units: state.units + gap,
                        gets: state.gets,
                    };
                    let target = slot + continuation;
                    if next_open[target].is_none_or(|old| {
                        key(candidate, unit_price, get_price) > key(old, unit_price, get_price)
                    }) {
                        next_open[target] = Some(candidate);
                        continued[target] = true;
                    }
                }
            }
        }
        history.push(Trace {
            site,
            continuation,
            required: required.contains(&site),
            closed_from_open,
            continued,
        });
        closed = next_closed;
        opened = next_open;
        previous = Some(site);
    }
    let mut best: Option<State> = None;
    let mut best_slot = 0_usize;
    let mut best_open = false;
    for slot in 0..=cap {
        let is_open = match (closed[slot], opened[slot]) {
            (Some(a), Some(b)) => key(b, unit_price, get_price) > key(a, unit_price, get_price),
            (None, Some(_)) => true,
            _ => false,
        };
        let candidate = prefer(closed[slot], opened[slot], unit_price, get_price);
        if let Some(candidate) = candidate {
            if best.is_none_or(|state| {
                key(candidate, unit_price, get_price) > key(state, unit_price, get_price)
            }) {
                best = Some(candidate);
                best_slot = slot;
                best_open = is_open;
            }
        }
    }
    let winner = best.ok_or(CoverError::NoFeasibleCover)?;
    let mut slot = best_slot;
    let mut pending_end = None;
    let mut intervals = Vec::new();
    for trace in history.into_iter().rev() {
        if !best_open {
            if trace.required {
                return Err(CoverError::InvalidWitness);
            }
            best_open = trace.closed_from_open[slot];
        } else {
            if pending_end.is_none() {
                pending_end = Some(trace.site);
            }
            if trace.continued[slot] {
                slot = slot
                    .checked_sub(trace.continuation)
                    .ok_or(CoverError::InvalidWitness)?;
            } else {
                intervals.push((
                    trace.site,
                    pending_end.take().ok_or(CoverError::InvalidWitness)?,
                ));
                slot = slot.checked_sub(1).ok_or(CoverError::InvalidWitness)?;
                best_open = trace.closed_from_open[slot];
            }
        }
    }
    if slot != 0 || best_open || pending_end.is_some() {
        return Err(CoverError::InvalidWitness);
    }
    intervals.reverse();
    let mut units = 0_usize;
    for &(start, end) in &intervals {
        units = units
            .checked_add(end - start + 1)
            .ok_or(CoverError::ArithmeticOverflow)?;
    }
    let gets = intervals.len();
    let mut mass = 0_i64;
    let mut interval = 0_usize;
    for (&site, &weight) in weights {
        while interval < intervals.len() && intervals[interval].1 < site {
            interval += 1;
        }
        if interval < intervals.len() && intervals[interval].0 <= site {
            mass += weight;
        }
    }
    let objective = mass - unit_price * units as i64 - get_price * gets as i64;
    if (mass, units, gets, objective)
        != (
            winner.mass,
            winner.units,
            winner.gets,
            key(winner, unit_price, get_price).0,
        )
        || required
            .iter()
            .any(|site| !intervals.iter().any(|(a, b)| a <= site && site <= b))
        || (fixed_cap == FixedCap::Units && units > cap)
        || (fixed_cap == FixedCap::Gets && gets > cap)
    {
        return Err(CoverError::InvalidWitness);
    }
    Ok(UnconstrainedCover {
        intervals,
        mass,
        units,
        gets,
        objective,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn one_cap_relaxations_cover_mandatory_units() {
        let weights = BTreeMap::from([(0, 10), (2, 8)]);
        let units =
            relaxed_priced_cover(&weights, &[0, 2], 3, FixedCap::Units, 2, 1, 3, 1024).unwrap();
        assert_eq!(units.intervals, vec![(0, 0), (2, 2)]);
        let gets =
            relaxed_priced_cover(&weights, &[0, 2], 3, FixedCap::Gets, 1, 1, 3, 1024).unwrap();
        assert_eq!(gets.intervals, vec![(0, 2)]);
    }

    #[test]
    fn later_new_interval_replaces_earlier_continuation_trace() {
        let weights = BTreeMap::from([(0, 10), (1, 1), (3, 10)]);
        let cover =
            relaxed_priced_cover(&weights, &[3], 4, FixedCap::Units, 3, 1, 3, 1024).unwrap();
        assert_eq!(cover.intervals, vec![(0, 1), (3, 3)]);
        assert_eq!((cover.mass, cover.units, cover.gets), (21, 3, 2));
    }
}
