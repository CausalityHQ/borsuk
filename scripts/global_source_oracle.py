"""Global truth-aware allocation under hit and query-tail constraints.

At a nonnegative GET price, each query's exact unit frontier supplies its
cheapest choice for each miss count. A small DP allocates misses and allowed
tail queries. Subtracting price × aggregate GET budget from its optimum is
a valid lower bound on the units any resource-feasible plan must read.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

import numpy as np

from scripts.sparse_cover_frontier import INF


@dataclass(frozen=True)
class QueryChoice:
    hits: int
    misses: int
    units: int
    gets: int


@dataclass(frozen=True)
class AggregateChoice:
    choices: tuple[QueryChoice, ...]
    hits: int
    misses: int
    tail_queries: int
    units: int
    gets: int
    priced_units: int


@dataclass(frozen=True)
class PriceSearch:
    evaluations: tuple[tuple[int, AggregateChoice], ...]
    strongest_unit_lower_bound: int
    witness_price: int | None
    decision: str


def allocate_priced(
    frontiers: Sequence[np.ndarray], *, truth_per_query: int,
    max_misses: int, max_tail_queries: int, tail_min_hits: int,
    get_price: int,
) -> AggregateChoice:
    if (not frontiers or type(truth_per_query) is not int or truth_per_query <= 0
            or type(max_misses) is not int or max_misses < 0
            or type(max_tail_queries) is not int or max_tail_queries < 0
            or type(tail_min_hits) is not int
            or not 0 <= tail_min_hits <= truth_per_query
            or type(get_price) is not int or get_price < 0
            or any(frontier.ndim != 2 or frontier.shape[0] <= 1
                   or frontier.shape[1] != truth_per_query + 1
                   or frontier.dtype != np.int64
                   for frontier in frontiers)):
        raise ValueError("global oracle geometry differs")
    shape = (max_misses + 1, max_tail_queries + 1)
    cost = np.full(shape, INF, dtype=np.int64)
    gets_used = np.full(shape, INF, dtype=np.int64)
    units_used = np.full(shape, INF, dtype=np.int64)
    cost[0, 0] = gets_used[0, 0] = units_used[0, 0] = 0
    traces: list[np.ndarray] = []
    options: list[dict[int, QueryChoice]] = []

    for frontier in frontiers:
        query_options: dict[int, QueryChoice] = {}
        get_values = np.arange(frontier.shape[0], dtype=np.int64)
        for misses in range(min(max_misses, truth_per_query) + 1):
            hits = truth_per_query - misses
            priced = frontier[:, hits] + get_price * get_values
            best_gets = int(np.argmin(priced[1:])) + 1
            units = int(frontier[best_gets, hits])
            if units < int(INF) // 2:
                query_options[misses] = QueryChoice(
                    hits, misses, units, best_gets)
        if not query_options:
            raise ValueError("no per-query oracle choice meets miss allowance")
        options.append(query_options)
        next_cost = np.full(shape, INF, dtype=np.int64)
        next_gets = np.full(shape, INF, dtype=np.int64)
        next_units = np.full(shape, INF, dtype=np.int64)
        chosen_miss = np.full(shape, -1, dtype=np.int16)
        for misses, choice in query_options.items():
            tail = int(choice.hits < tail_min_hits)
            if tail > max_tail_queries:
                continue
            src = (slice(0, max_misses + 1 - misses),
                   slice(0, max_tail_queries + 1 - tail))
            dst = (slice(misses, max_misses + 1),
                   slice(tail, max_tail_queries + 1))
            candidate_cost = cost[src] + choice.units + get_price * choice.gets
            candidate_gets = gets_used[src] + choice.gets
            candidate_units = units_used[src] + choice.units
            target_cost = next_cost[dst]
            target_gets = next_gets[dst]
            target_units = next_units[dst]
            better = ((candidate_cost < target_cost)
                      | ((candidate_cost == target_cost)
                         & (candidate_gets < target_gets))
                      | ((candidate_cost == target_cost)
                         & (candidate_gets == target_gets)
                         & (candidate_units < target_units)))
            target_cost[better] = candidate_cost[better]
            target_gets[better] = candidate_gets[better]
            target_units[better] = candidate_units[better]
            chosen_miss[dst][better] = misses
        cost, gets_used, units_used = next_cost, next_gets, next_units
        traces.append(chosen_miss)

    possible = [(int(cost[misses, tail]), int(gets_used[misses, tail]),
                 int(units_used[misses, tail]), misses, tail)
                for misses in range(shape[0]) for tail in range(shape[1])
                if cost[misses, tail] < int(INF) // 2]
    if not possible:
        raise ValueError("global oracle miss/tail target is infeasible")
    priced, gets, units, misses, tail = min(possible)
    cursor_miss, cursor_tail = misses, tail
    chosen = []
    for index in range(len(frontiers) - 1, -1, -1):
        delta = int(traces[index][cursor_miss, cursor_tail])
        if delta < 0:
            raise AssertionError("global oracle witness trace differs")
        choice = options[index][delta]
        chosen.append(choice)
        cursor_miss -= delta
        cursor_tail -= int(choice.hits < tail_min_hits)
    chosen.reverse()
    if (cursor_miss != 0 or cursor_tail != 0
            or sum(choice.units for choice in chosen) != units
            or sum(choice.gets for choice in chosen) != gets
            or sum(choice.misses for choice in chosen) != misses
            or units + get_price * gets != priced):
        raise AssertionError("global oracle aggregate accounting differs")
    return AggregateChoice(tuple(chosen),
                           len(frontiers) * truth_per_query - misses,
                           misses, tail, units, gets, priced)


def search_get_prices(
    frontiers: Sequence[np.ndarray], *, truth_per_query: int,
    max_misses: int, max_tail_queries: int, tail_min_hits: int,
    aggregate_get_budget: int, aggregate_unit_budget: int,
) -> PriceSearch:
    """Find a certified unit lower bound or a priced feasible witness."""
    if (type(aggregate_get_budget) is not int or aggregate_get_budget <= 0
            or type(aggregate_unit_budget) is not int
            or aggregate_unit_budget < 0):
        raise ValueError("aggregate oracle budget differs")
    seen: dict[int, AggregateChoice] = {}

    def evaluate(price: int) -> AggregateChoice:
        if price not in seen:
            seen[price] = allocate_priced(
                frontiers, truth_per_query=truth_per_query,
                max_misses=max_misses, max_tail_queries=max_tail_queries,
                tail_min_hits=tail_min_hits, get_price=price)
        return seen[price]

    first = evaluate(0)
    if first.gets > aggregate_get_budget:
        low, high = 0, 1
        while evaluate(high).gets > aggregate_get_budget:
            low, high = high, high * 2
            if high > 1 << 30:
                raise ValueError("GET price cannot bracket aggregate budget")
        while high - low > 1:
            midpoint = (high + low) // 2
            if evaluate(midpoint).gets <= aggregate_get_budget:
                high = midpoint
            else:
                low = midpoint
        for price in range(max(0, low - 2), high + 3):
            evaluate(price)
    lower_bound = max(choice.priced_units - price * aggregate_get_budget
                      for price, choice in seen.items())
    witnesses = [(choice.units, choice.gets, price)
                 for price, choice in seen.items()
                 if choice.gets <= aggregate_get_budget
                 and choice.units <= aggregate_unit_budget]
    witness_price = min(witnesses)[2] if witnesses else None
    if witness_price is not None:
        decision = "priced-feasibility-witness"
    elif lower_bound > aggregate_unit_budget:
        decision = "certified-layout-infeasible"
    else:
        decision = "unresolved-exact-global-needed"
    return PriceSearch(tuple(sorted(seen.items())), lower_bound,
                       witness_price, decision)
