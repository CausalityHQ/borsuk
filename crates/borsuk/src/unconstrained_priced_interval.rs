//! Linear two-state priced interval cover, before hard resource admission.
//!
//! A caller may accept this optimum only when its witness meets both hard
//! caps. Otherwise it must use a capped solver. Weights are predicted mass,
//! not ground truth, and the unit/GET prices are supplied by the caller.

use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UnconstrainedCover {
    pub intervals: Vec<(usize, usize)>,
    pub mass: i64,
    pub units: usize,
    pub gets: usize,
    pub objective: i64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CoverError {
    InvalidGeometry,
    ArithmeticOverflow,
    InvalidWitness,
}

#[derive(Clone, Copy)]
struct State {
    mass: i64,
    units: usize,
    gets: usize,
    prev_open: bool,
    continued: bool,
}

#[derive(Clone, Copy)]
struct Trace {
    site: usize,
    closed_prev_open: Option<bool>,
    open_prev_open: Option<bool>,
    open_continued: Option<bool>,
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

/// Maximize modeled mass minus priced physical units and GETs over all covers.
/// This is `O(weighted sites + mandatory sites)` time and trace space, aside
/// from ordered input construction. A feasible result is a hard-cap optimum.
pub fn unconstrained_priced_cover(
    weights: &BTreeMap<usize, i64>,
    mandatory: &[usize],
    page_count: usize,
    unit_price: i64,
    get_price: i64,
) -> Result<UnconstrainedCover, CoverError> {
    if page_count == 0
        || page_count > i64::MAX as usize
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
    let mut closed = Some(State {
        mass: 0,
        units: 0,
        gets: 0,
        prev_open: false,
        continued: false,
    });
    let mut opened: Option<State> = None;
    let mut history = Vec::with_capacity(sites.len());
    let mut previous = None;
    for site in sites {
        let weight = *weights.get(&site).unwrap_or(&0);
        let gap = previous.map_or(1, |prev| site - prev);
        let (next_closed, closed_prev_open) = if required.contains(&site) {
            (None, None)
        } else {
            let from_open = match (closed, opened) {
                (Some(a), Some(b)) => key(b, unit_price, get_price) > key(a, unit_price, get_price),
                (None, Some(_)) => true,
                _ => false,
            };
            (
                prefer(closed, opened, unit_price, get_price),
                Some(from_open),
            )
        };
        let start_closed = closed.map(|state| State {
            mass: state.mass + weight,
            units: state.units + 1,
            gets: state.gets + 1,
            prev_open: false,
            continued: false,
        });
        let start_open = opened.map(|state| State {
            mass: state.mass + weight,
            units: state.units + 1,
            gets: state.gets + 1,
            prev_open: true,
            continued: false,
        });
        let new_interval = prefer(start_closed, start_open, unit_price, get_price);
        let continue_open = opened.map(|state| State {
            mass: state.mass + weight,
            units: state.units + gap,
            gets: state.gets,
            prev_open: true,
            continued: true,
        });
        let next_open = prefer(new_interval, continue_open, unit_price, get_price);
        history.push(Trace {
            site,
            closed_prev_open,
            open_prev_open: next_open.map(|state| state.prev_open),
            open_continued: next_open.map(|state| state.continued),
        });
        closed = next_closed;
        opened = next_open;
        previous = Some(site);
    }
    let winner = prefer(closed, opened, unit_price, get_price).ok_or(CoverError::InvalidWitness)?;
    let mut best_open = match (closed, opened) {
        (Some(a), Some(b)) => key(b, unit_price, get_price) > key(a, unit_price, get_price),
        (None, Some(_)) => true,
        _ => false,
    };
    let mut pending_end = None;
    let mut intervals = Vec::new();
    for trace in history.into_iter().rev() {
        if best_open {
            if pending_end.is_none() {
                pending_end = Some(trace.site);
            }
            let continued = trace.open_continued.ok_or(CoverError::InvalidWitness)?;
            if !continued {
                intervals.push((
                    trace.site,
                    pending_end.take().ok_or(CoverError::InvalidWitness)?,
                ));
            }
            best_open = trace.open_prev_open.ok_or(CoverError::InvalidWitness)?;
        } else {
            best_open = trace.closed_prev_open.ok_or(CoverError::InvalidWitness)?;
        }
    }
    if best_open || pending_end.is_some() {
        return Err(CoverError::InvalidWitness);
    }
    intervals.reverse();
    let mut units = 0_usize;
    for &(start, end) in &intervals {
        units = units
            .checked_add(end - start + 1)
            .ok_or(CoverError::ArithmeticOverflow)?;
    }
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
    let gets = intervals.len();
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
    fn exact_small_witness_and_mandatory_bridge() {
        let weights = BTreeMap::from([(0, 10), (2, 8)]);
        let result = unconstrained_priced_cover(&weights, &[0], 3, 1, 3).unwrap();
        assert_eq!(result.intervals, vec![(0, 2)]);
        assert_eq!((result.mass, result.units, result.gets), (18, 3, 1));
        let strict = unconstrained_priced_cover(&weights, &[0, 2], 3, 1, 20).unwrap();
        assert_eq!(strict.intervals, vec![(0, 2)]);
    }
}
