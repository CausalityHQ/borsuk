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
