"""Exact sparse truth-aware physical interval oracle at fixed dual prices.

This is an offline upper-bound/witness tool. It must never be used for a
serving plan because its page masses come from exact ground truth. The DP
optimizes hits × scale - unit_price × units - get_price × GETs while covering
every mandatory physical unit and using at most the caller GET cap.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping, Sequence


@dataclass(frozen=True)
class PricedCover:
    intervals: tuple[tuple[int, int], ...]
    hits: int
    units: int
    gets: int
    objective: int


@dataclass(frozen=True)
class _State:
    objective: int
    units: int
    hits: int


@dataclass(frozen=True)
class _Start:
    rank: int
    base_objective: int
    base_units: int
    prior: _State


def _better(candidate: _State, incumbent: _State | None) -> bool:
    return (incumbent is None or
            (candidate.objective, -candidate.units) >
            (incumbent.objective, -incumbent.units))


def priced_cover(
    truth_by_unit: Mapping[int, int], mandatory_units: Sequence[int], *,
    page_count: int, max_gets: int, hit_scale: int,
    unit_price: int, get_price: int,
) -> PricedCover:
    """Return an exact optimum at one nonnegative unit/GET price pair.

    Only truth-bearing or mandatory units need to be DP sites: an optimum
    can shift every interval endpoint inward to the first/last such site.
    The affine interval score gives O(sites × max_gets) time and state.
    """
    mandatory = tuple(mandatory_units)
    if (type(page_count) is not int or page_count <= 0
            or type(max_gets) is not int or max_gets <= 0
            or type(hit_scale) is not int or hit_scale <= 0
            or type(unit_price) is not int or unit_price < 0
            or type(get_price) is not int or get_price < 0
            or not mandatory
            or any(type(unit) is not int or not 0 <= unit < page_count
                   for unit in mandatory)
            or any(type(unit) is not int or not 0 <= unit < page_count
                   or type(hits) is not int or hits < 0
                   for unit, hits in truth_by_unit.items())):
        raise ValueError("priced interval oracle geometry differs")
    sites = sorted(set(mandatory) | set(truth_by_unit))
    required = set(mandatory)
    masses = [truth_by_unit.get(site, 0) for site in sites]
    prefix = [0]
    for mass in masses:
        prefix.append(prefix[-1] + mass)
    gets_cap = min(max_gets, len(sites))
    dp: list[list[_State | None]] = [
        [None] * (gets_cap + 1) for _ in range(len(sites) + 1)]
    trace: list[list[tuple[str, int] | None]] = [
        [None] * (gets_cap + 1) for _ in range(len(sites) + 1)]
    dp[0][0] = _State(0, 0, 0)
    best_start: list[_Start | None] = [None] * gets_cap

    for end_rank, end_unit in enumerate(sites):
        for gets in range(gets_cap + 1):
            state = dp[end_rank][gets]
            if state is not None and end_unit not in required:
                if _better(state, dp[end_rank + 1][gets]):
                    dp[end_rank + 1][gets] = state
                    trace[end_rank + 1][gets] = ("skip", end_rank)
            if gets == gets_cap:
                continue
            if state is not None:
                start = _Start(
                    end_rank,
                    state.objective - hit_scale * prefix[end_rank]
                    + unit_price * end_unit,
                    state.units - end_unit,
                    state,
                )
                previous = best_start[gets]
                if (previous is None or
                        (start.base_objective, -start.base_units) >
                        (previous.base_objective, -previous.base_units)):
                    best_start[gets] = start
            chosen = best_start[gets]
            if chosen is None:
                continue
            interval_hits = prefix[end_rank + 1] - prefix[chosen.rank]
            interval_units = end_unit - sites[chosen.rank] + 1
            candidate = _State(
                chosen.prior.objective + hit_scale * interval_hits
                - unit_price * interval_units - get_price,
                chosen.prior.units + interval_units,
                chosen.prior.hits + interval_hits,
            )
            if _better(candidate, dp[end_rank + 1][gets + 1]):
                dp[end_rank + 1][gets + 1] = candidate
                trace[end_rank + 1][gets + 1] = ("interval", chosen.rank)

    final = len(sites)
    winner = max(
        ((state.objective, -state.units, -gets, gets, state)
         for gets, state in enumerate(dp[final]) if state is not None),
        default=None,
    )
    if winner is None:
        raise AssertionError("mandatory interval oracle has no feasible cover")
    _, _, _, gets, state = winner
    cursor = final
    intervals = []
    while cursor:
        choice = trace[cursor][gets]
        if choice is None:
            raise AssertionError("priced interval witness is incomplete")
        kind, start_rank = choice
        if kind == "skip":
            cursor -= 1
        else:
            intervals.append((sites[start_rank], sites[cursor - 1]))
            cursor = start_rank
            gets -= 1
    if gets != 0:
        raise AssertionError("priced interval GET reconstruction differs")
    intervals.reverse()
    if (len(intervals) != winner[3]
            or sum(end - start + 1 for start, end in intervals) != state.units
            or sum(count for unit, count in truth_by_unit.items()
                   if any(start <= unit <= end for start, end in intervals))
                   != state.hits
            or any(not any(start <= unit <= end for start, end in intervals)
                   for unit in mandatory)):
        raise AssertionError("priced interval witness accounting differs")
    return PricedCover(tuple(intervals), state.hits, state.units,
                       len(intervals), state.objective)
