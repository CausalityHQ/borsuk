//! Exact priced interval cover under simultaneous hard GET and unit caps.
//!
//! The two-dimensional fallback is used only when cheaper admitted optima
//! cannot certify the two-cap optimum. Its weights are caller predictions.

use std::collections::{BTreeMap, BTreeSet};

use crate::unconstrained_priced_interval::{CoverError, UnconstrainedCover};

struct Trace {
    site: usize,
    gap: usize,
    required: bool,
    closed_from_open: Vec<bool>,
    continued: Vec<bool>,
}

fn index(gets: usize, units: usize, stride: usize) -> usize {
    gets * stride + units
}

/// Maximize modeled mass minus fixed unit and GET prices under both caps.
///
/// Runtime is `O(sites * get_cap * unit_cap)` and the retained trace has
/// `2 * sites * (get_cap + 1) * (unit_cap + 1)` Boolean entries.
pub fn hard_priced_cover(
    weights: &BTreeMap<usize, i64>,
    mandatory: &[usize],
    page_count: usize,
    get_cap: usize,
    unit_cap: usize,
    unit_price: i64,
    get_price: i64,
    trace_budget_bytes: usize,
) -> Result<UnconstrainedCover, CoverError> {
    if page_count == 0
        || page_count > i64::MAX as usize
        || get_cap == 0
        || unit_cap == 0
        || get_cap > page_count
        || unit_cap > page_count
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
    let stride = unit_cap
        .checked_add(1)
        .ok_or(CoverError::ArithmeticOverflow)?;
    let slots = get_cap
        .checked_add(1)
        .and_then(|value| value.checked_mul(stride))
        .ok_or(CoverError::ArithmeticOverflow)?;
    let trace_bytes = sites
        .len()
        .checked_mul(slots)
        .and_then(|value| value.checked_mul(2))
        .ok_or(CoverError::ArithmeticOverflow)?;
    if trace_bytes > trace_budget_bytes {
        return Err(CoverError::TraceBudgetExceeded);
    }
    // All reachable modeled masses are nonnegative, so -1 is unreachable.
    let mut closed = vec![-1_i64; slots];
    let mut opened = vec![-1_i64; slots];
    closed[0] = 0;
    let mut history = Vec::with_capacity(sites.len());
    let mut previous = None;
    for site in sites {
        let weight = *weights.get(&site).unwrap_or(&0);
        let gap = previous.map_or(1, |old| site - old);
        let required_here = required.contains(&site);
        let mut next_closed = vec![-1_i64; slots];
        let mut next_open = vec![-1_i64; slots];
        let mut closed_from_open = vec![false; slots];
        let mut continued = vec![false; slots];
        for gets in 0..=get_cap {
            for units in 0..=unit_cap {
                let slot = index(gets, units, stride);
                let from_open = opened[slot] > closed[slot];
                closed_from_open[slot] = from_open;
                let base = closed[slot].max(opened[slot]);
                if !required_here {
                    next_closed[slot] = base;
                }
                if base >= 0 && gets < get_cap && units < unit_cap {
                    let target = index(gets + 1, units + 1, stride);
                    let candidate = base + weight;
                    if candidate > next_open[target] {
                        next_open[target] = candidate;
                        continued[target] = false;
                    }
                }
            }
        }
        // Match the reference's strict tie rule: all new intervals are
        // installed before continuations, so an equal-mass continuation
        // preserves the new-interval trace.
        if gap <= unit_cap {
            for gets in 0..=get_cap {
                for units in 0..=unit_cap - gap {
                    let slot = index(gets, units, stride);
                    if opened[slot] < 0 {
                        continue;
                    }
                    let target = index(gets, units + gap, stride);
                    let candidate = opened[slot] + weight;
                    if candidate > next_open[target] {
                        next_open[target] = candidate;
                        continued[target] = true;
                    }
                }
            }
        }
        history.push(Trace {
            site,
            gap,
            required: required_here,
            closed_from_open,
            continued,
        });
        closed = next_closed;
        opened = next_open;
        previous = Some(site);
    }
    let mut best: Option<(i64, i64, i64, i64)> = None;
    let mut best_gets = 0;
    let mut best_units = 0;
    let mut best_open = false;
    for gets in 0..=get_cap {
        for units in 0..=unit_cap {
            let slot = index(gets, units, stride);
            let mass = closed[slot].max(opened[slot]);
            if mass < 0 {
                continue;
            }
            let key = (
                mass - unit_price * units as i64 - get_price * gets as i64,
                -(units as i64),
                -(gets as i64),
                mass,
            );
            if best.is_none_or(|prior| key > prior) {
                best = Some(key);
                best_gets = gets;
                best_units = units;
                best_open = opened[slot] > closed[slot];
            }
        }
    }
    let winner = best.ok_or(CoverError::NoFeasibleCover)?;
    let mut gets = best_gets;
    let mut units = best_units;
    let mut pending_end = None;
    let mut intervals = Vec::new();
    for trace in history.into_iter().rev() {
        let slot = index(gets, units, stride);
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
                units = units
                    .checked_sub(trace.gap)
                    .ok_or(CoverError::InvalidWitness)?;
            } else {
                intervals.push((
                    trace.site,
                    pending_end.take().ok_or(CoverError::InvalidWitness)?,
                ));
                gets = gets.checked_sub(1).ok_or(CoverError::InvalidWitness)?;
                units = units.checked_sub(1).ok_or(CoverError::InvalidWitness)?;
                best_open = trace.closed_from_open[index(gets, units, stride)];
            }
        }
    }
    if gets != 0 || units != 0 || best_open || pending_end.is_some() {
        return Err(CoverError::InvalidWitness);
    }
    intervals.reverse();
    let actual_units = intervals.iter().try_fold(0_usize, |sum, &(start, end)| {
        sum.checked_add(end - start + 1)
            .ok_or(CoverError::ArithmeticOverflow)
    })?;
    let actual_gets = intervals.len();
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
    let objective = mass - unit_price * actual_units as i64 - get_price * actual_gets as i64;
    if (
        objective,
        -(actual_units as i64),
        -(actual_gets as i64),
        mass,
    ) != winner
        || actual_units > unit_cap
        || actual_gets > get_cap
        || required
            .iter()
            .any(|site| !intervals.iter().any(|(a, b)| a <= site && site <= b))
    {
        return Err(CoverError::InvalidWitness);
    }
    Ok(UnconstrainedCover {
        intervals,
        mass,
        units: actual_units,
        gets: actual_gets,
        objective,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hard_caps_force_bridge_of_mandatory_sites() {
        let weights = BTreeMap::from([(0, 10), (2, 8)]);
        let cover = hard_priced_cover(&weights, &[0, 2], 3, 1, 3, 1, 3, 1024).unwrap();
        assert_eq!(cover.intervals, vec![(0, 2)]);
        assert_eq!((cover.mass, cover.units, cover.gets), (18, 3, 1));
    }

    #[test]
    fn infeasible_hard_caps_are_rejected() {
        let cover = hard_priced_cover(&BTreeMap::new(), &[0, 2], 3, 1, 2, 1, 3, 1024);
        assert_eq!(cover, Err(CoverError::NoFeasibleCover));
    }
}
