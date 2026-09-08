//! Deterministic posting geometry for the V36 qualification funnel.

use crate::{BorsukError, Result};

fn invalid(message: &str) -> BorsukError {
    BorsukError::InvalidStorage(message.to_owned())
}

/// Allocate a fixed posting budget across non-empty runs with a one-posting
/// lower bound and Hamilton largest-remainder apportionment.
pub fn allocate_v36_hamilton_postings(run_rows: &[u64], total_postings: u32) -> Result<Vec<u32>> {
    let active: Vec<usize> = run_rows
        .iter()
        .enumerate()
        .filter_map(|(ordinal, rows)| (*rows != 0).then_some(ordinal))
        .collect();
    if active.is_empty() {
        return Err(invalid("V36 posting population is empty"));
    }
    let active_count =
        u32::try_from(active.len()).map_err(|_| invalid("V36 posting run count overflows"))?;
    if total_postings < active_count {
        return Err(invalid("V36 posting budget cannot cover every run"));
    }

    let total_rows = active.iter().try_fold(0_u128, |sum, ordinal| {
        sum.checked_add(u128::from(run_rows[*ordinal]))
            .ok_or_else(|| invalid("V36 posting population overflows"))
    })?;
    let remaining = u128::from(total_postings - active_count);
    let mut allocation = vec![0_u32; run_rows.len()];
    let mut floor_sum = 0_u32;
    let mut remainders = Vec::with_capacity(active.len());

    for ordinal in active {
        let numerator = remaining
            .checked_mul(u128::from(run_rows[ordinal]))
            .ok_or_else(|| invalid("V36 posting apportionment overflows"))?;
        let floor = u32::try_from(numerator / total_rows)
            .map_err(|_| invalid("V36 posting apportionment overflows"))?;
        allocation[ordinal] = 1_u32
            .checked_add(floor)
            .ok_or_else(|| invalid("V36 posting allocation overflows"))?;
        floor_sum = floor_sum
            .checked_add(floor)
            .ok_or_else(|| invalid("V36 posting allocation overflows"))?;
        remainders.push((numerator % total_rows, ordinal));
    }

    remainders
        .sort_unstable_by(|left, right| right.0.cmp(&left.0).then_with(|| left.1.cmp(&right.1)));
    let leftover = u32::try_from(remaining)
        .map_err(|_| invalid("V36 posting allocation overflows"))?
        .checked_sub(floor_sum)
        .ok_or_else(|| invalid("V36 posting allocation differs"))?;
    for (_, ordinal) in remainders
        .into_iter()
        .take(usize::try_from(leftover).map_err(|_| invalid("V36 posting allocation overflows"))?)
    {
        allocation[ordinal] = allocation[ordinal]
            .checked_add(1)
            .ok_or_else(|| invalid("V36 posting allocation overflows"))?;
    }

    if allocation.iter().try_fold(0_u32, |sum, value| {
        sum.checked_add(*value)
            .ok_or_else(|| invalid("V36 posting allocation overflows"))
    })? != total_postings
    {
        return Err(invalid("V36 posting allocation differs"));
    }
    Ok(allocation)
}

fn squared_l2(left: &[f32], right: &[f32]) -> Result<f64> {
    if left.len() != right.len() || left.is_empty() {
        return Err(invalid("V36 posting vector shape differs"));
    }
    left.iter().zip(right).try_fold(0.0, |sum, (left, right)| {
        if !left.is_finite() || !right.is_finite() {
            return Err(invalid("V36 posting vector is nonfinite"));
        }
        let delta = f64::from(*left) - f64::from(*right);
        let next = sum + delta * delta;
        next.is_finite()
            .then_some(next)
            .ok_or_else(|| invalid("V36 posting distance is nonfinite"))
    })
}

/// Select deterministic primary and closure owners for one projected row.
pub fn select_v36_closure_owners(
    row: &[f32],
    centroids: &[Vec<f32>],
    epsilon: f64,
    max_owners: u8,
) -> Result<Vec<u32>> {
    if centroids.is_empty() || max_owners == 0 || max_owners > 8 {
        return Err(invalid("V36 closure authority differs"));
    }
    if !matches!(epsilon, 0.05 | 0.15 | 0.30) {
        return Err(invalid("V36 closure epsilon differs"));
    }

    let mut ranked = centroids
        .iter()
        .enumerate()
        .map(|(ordinal, centroid)| Ok((squared_l2(row, centroid)?, ordinal)))
        .collect::<Result<Vec<_>>>()?;
    ranked.sort_unstable_by(|left, right| {
        left.0
            .total_cmp(&right.0)
            .then_with(|| left.1.cmp(&right.1))
    });
    let (primary_distance, primary) = ranked[0];
    let scale = 1.0 + epsilon;
    let threshold = primary_distance * scale * scale;
    if !threshold.is_finite() {
        return Err(invalid("V36 closure threshold is nonfinite"));
    }

    let mut retained = vec![primary];
    for (distance, candidate) in ranked.into_iter().skip(1) {
        if distance > threshold || retained.len() == usize::from(max_owners) {
            break;
        }
        let redundant = retained.iter().try_fold(false, |redundant, owner| {
            Ok::<_, BorsukError>(
                redundant || squared_l2(&centroids[*owner], &centroids[candidate])? < distance,
            )
        })?;
        if !redundant {
            retained.push(candidate);
        }
    }
    retained
        .into_iter()
        .map(|ordinal| u32::try_from(ordinal).map_err(|_| invalid("V36 posting ordinal overflows")))
        .collect()
}
