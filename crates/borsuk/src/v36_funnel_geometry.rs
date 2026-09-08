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

/// First registered construction-side reason a V36 geometry is rejected.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum V36GeometryStop {
    /// Mean stored assignments exceed three per primary row.
    MeanReplication,
    /// Primary posting p99 exceeds twice the target occupancy.
    PrimaryP99,
    /// A primary posting exceeds four times the target occupancy.
    PrimaryMaximum,
    /// Stored-assignment p99 exceeds six times the target occupancy.
    StoredP99,
    /// A stored posting exceeds eight times the target occupancy.
    StoredMaximum,
}

/// Exact construction statistics and first-stop disposition for one geometry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct V36GeometryAdmission {
    /// Mean stored assignments per primary row in parts per million.
    pub mean_replication_ppm: u64,
    /// Nearest-rank 99th percentile of primary posting occupancy.
    pub primary_p99: u64,
    /// Maximum primary posting occupancy.
    pub primary_maximum: u64,
    /// Nearest-rank 99th percentile of stored posting occupancy.
    pub stored_p99: u64,
    /// Maximum stored posting occupancy.
    pub stored_maximum: u64,
    /// First registered rejection reason, or `None` when admitted.
    pub stop: Option<V36GeometryStop>,
}

fn percentile_99(values: &[u64]) -> Result<u64> {
    let mut ordered = values.to_vec();
    ordered.sort_unstable();
    let numerator = ordered
        .len()
        .checked_mul(99)
        .ok_or_else(|| invalid("V36 geometry percentile overflows"))?;
    let rank = numerator
        .checked_add(99)
        .ok_or_else(|| invalid("V36 geometry percentile overflows"))?
        / 100;
    ordered
        .get(rank.saturating_sub(1))
        .copied()
        .ok_or_else(|| invalid("V36 geometry occupancy is empty"))
}

/// Compute exact occupancy evidence and reject at the first registered gate.
pub fn admit_v36_geometry(
    primary_occupancy: &[u64],
    stored_occupancy: &[u64],
    target_primary_rows: u64,
) -> Result<V36GeometryAdmission> {
    if primary_occupancy.is_empty()
        || primary_occupancy.len() != stored_occupancy.len()
        || target_primary_rows == 0
        || primary_occupancy
            .iter()
            .zip(stored_occupancy)
            .any(|(primary, stored)| *primary == 0 || stored < primary)
    {
        return Err(invalid("V36 geometry occupancy authority differs"));
    }
    let primary_rows = primary_occupancy.iter().try_fold(0_u64, |sum, rows| {
        sum.checked_add(*rows)
            .ok_or_else(|| invalid("V36 geometry primary rows overflow"))
    })?;
    let stored_rows = stored_occupancy.iter().try_fold(0_u64, |sum, rows| {
        sum.checked_add(*rows)
            .ok_or_else(|| invalid("V36 geometry stored rows overflow"))
    })?;
    let replication_numerator = u128::from(stored_rows)
        .checked_mul(1_000_000)
        .ok_or_else(|| invalid("V36 geometry replication overflows"))?;
    let mean_replication_ppm = u64::try_from(
        replication_numerator
            .checked_add(u128::from(primary_rows - 1))
            .ok_or_else(|| invalid("V36 geometry replication overflows"))?
            / u128::from(primary_rows),
    )
    .map_err(|_| invalid("V36 geometry replication overflows"))?;
    let primary_p99 = percentile_99(primary_occupancy)?;
    let stored_p99 = percentile_99(stored_occupancy)?;
    let primary_maximum = *primary_occupancy
        .iter()
        .max()
        .ok_or_else(|| invalid("V36 geometry occupancy is empty"))?;
    let stored_maximum = *stored_occupancy
        .iter()
        .max()
        .ok_or_else(|| invalid("V36 geometry occupancy is empty"))?;
    let primary_p99_limit = target_primary_rows
        .checked_mul(2)
        .ok_or_else(|| invalid("V36 geometry threshold overflows"))?;
    let primary_maximum_limit = target_primary_rows
        .checked_mul(4)
        .ok_or_else(|| invalid("V36 geometry threshold overflows"))?;
    let stored_p99_limit = target_primary_rows
        .checked_mul(6)
        .ok_or_else(|| invalid("V36 geometry threshold overflows"))?;
    let stored_maximum_limit = target_primary_rows
        .checked_mul(8)
        .ok_or_else(|| invalid("V36 geometry threshold overflows"))?;
    let stop = if u128::from(stored_rows) > u128::from(primary_rows) * 3 {
        Some(V36GeometryStop::MeanReplication)
    } else if primary_p99 > primary_p99_limit {
        Some(V36GeometryStop::PrimaryP99)
    } else if primary_maximum > primary_maximum_limit {
        Some(V36GeometryStop::PrimaryMaximum)
    } else if stored_p99 > stored_p99_limit {
        Some(V36GeometryStop::StoredP99)
    } else if stored_maximum > stored_maximum_limit {
        Some(V36GeometryStop::StoredMaximum)
    } else {
        None
    };
    Ok(V36GeometryAdmission {
        mean_replication_ppm,
        primary_p99,
        primary_maximum,
        stored_p99,
        stored_maximum,
        stop,
    })
}
