"""Minimum charged-unit frontier for the V165 513/1 vote planner.

The caller supplies GT-blind integer votes and the same unit/GET caps as the
full-cap plan. This layer changes only the stopping objective.
"""

from __future__ import annotations

from dataclasses import dataclass

from scripts.v114_weighted_interval_plan import optimal_weighted_intervals


@dataclass(frozen=True)
class VoteFrontier:
    units: int
    intervals: tuple[tuple[int, int], ...]
    target_score: int
    achieved_score: int
    full_score: int


def minimum_vote_frontier(
    page_weights: dict[int, int], *, primary_units: tuple[int, ...],
    primary_vote_total: int, secondary_vote_total: int,
    fraction: tuple[int, int], page_count: int, max_gets: int,
    max_units: int,
) -> VoteFrontier:
    """Find the least charged units preserving primary and a secondary fraction.

    `fraction` is exact and rounded upward against achievable secondary votes.
    V165 primary votes must dominate all secondary votes individually.
    """
    numerator, denominator = fraction
    if (not 0 < numerator <= denominator or primary_vote_total <= 0
        or secondary_vote_total < 0 or not primary_units
        or len(set(primary_units)) != len(primary_units)
        or any(unit not in page_weights or
               page_weights[unit] <= secondary_vote_total
               for unit in primary_units)
        or sum(page_weights.values()) != primary_vote_total + secondary_vote_total):
        raise ValueError("vote geometry or primary dominance differs")

    def plan(units: int, gets: int = max_gets) -> tuple[int, tuple[tuple[int, int], ...]]:
        return optimal_weighted_intervals(
            page_weights, page_count=page_count, max_gets=gets,
            max_units=units, full_page_units=1, last_page_units=1,
        )

    full_score, full_intervals = plan(max_units)
    full_covered = {unit for start, end in full_intervals
                    for unit in range(start, end + 1)}
    if not set(primary_units).issubset(full_covered):
        raise ValueError("primary units infeasible within full cap")
    achievable_secondary = full_score - primary_vote_total
    target = primary_vote_total + (
        numerator * achievable_secondary + denominator - 1
    ) // denominator

    low, high = 0, max_units
    while low < high:
        mid = (low + high) // 2
        score, _ = plan(mid)
        if score >= target:
            high = mid
        else:
            low = mid + 1
    get_low, get_high = 1, max_gets
    while get_low < get_high:
        mid = (get_low + get_high) // 2
        score, _ = plan(low, mid)
        if score >= target:
            get_high = mid
        else:
            get_low = mid + 1
    score, intervals = plan(low, get_low)
    covered = {unit for start, end in intervals
               for unit in range(start, end + 1)}
    if score < target or not set(primary_units).issubset(covered):
        raise AssertionError("vote frontier witness lost primary coverage")
    charged = sum(end - start + 1 for start, end in intervals)
    if charged != low or len(intervals) != get_low:
        raise AssertionError("vote frontier did not minimize charged units or GETs")
    return VoteFrontier(charged, intervals, target, score, full_score)
