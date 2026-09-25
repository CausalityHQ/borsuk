"""Select shared interval prices from modeled utility and physical caps.

The caller supplies a fit-trained, sealed GT-blind utility for each query.
This offline selector uses no query labels. It rejects a price pair if any
query or aggregate plan exceeds its supplied resource envelope; it never
silently clips an optimum from the underlying priced interval DP.
"""

from __future__ import annotations

from dataclasses import dataclass
from numbers import Integral
from typing import Mapping, Sequence

from scripts.priced_interval_oracle import PricedCover, priced_cover


@dataclass(frozen=True)
class QueryUtility:
    mandatory_units: tuple[int, ...]
    weight_by_unit: Mapping[int, int]
    page_count: int


@dataclass(frozen=True)
class PriceSelection:
    unit_price: int
    get_price: int
    plans: tuple[PricedCover, ...]
    predicted_mass: int
    total_units: int
    total_gets: int


def select_priced_plans(
    queries: Sequence[QueryUtility], *, prices: Sequence[tuple[int, int]],
    max_gets_per_query: int, max_units_per_query: int,
    max_total_units: int, max_total_gets: int,
) -> PriceSelection:
    """Choose highest modeled covered mass among feasible fixed prices.

    Price search is deterministic. The returned budget certificate is checked
    from physical intervals, independently of the DP's objective value.
    """
    if (not queries or not prices
            or any(type(value) is not int or value <= 0 for value in
                   (max_gets_per_query, max_units_per_query,
                    max_total_units, max_total_gets))
            or any(type(unit_price) is not int or unit_price < 0
                   or type(get_price) is not int or get_price < 0
                   for unit_price, get_price in prices)
            or any(type(query.page_count) is not int or query.page_count <= 0
                   or not query.mandatory_units
                   or any(not isinstance(unit, Integral) or unit < 0
                          or unit >= query.page_count
                          for unit in query.mandatory_units)
                   or any(not isinstance(unit, Integral) or unit < 0
                          or unit >= query.page_count
                          or not isinstance(weight, Integral) or weight < 0
                          for unit, weight in query.weight_by_unit.items())
                   for query in queries)):
        raise ValueError("predicted interval price inputs differ")
    winner: PriceSelection | None = None
    for unit_price, get_price in prices:
        plans = tuple(priced_cover(
            query.weight_by_unit, query.mandatory_units,
            page_count=query.page_count, max_gets=max_gets_per_query,
            hit_scale=1, unit_price=unit_price, get_price=get_price)
            for query in queries)
        if any(plan.units > max_units_per_query
               or plan.gets > max_gets_per_query for plan in plans):
            continue
        units = sum(plan.units for plan in plans)
        gets = sum(plan.gets for plan in plans)
        if units > max_total_units or gets > max_total_gets:
            continue
        for query, plan in zip(queries, plans, strict=True):
            if (plan.units != sum(end - start + 1
                                  for start, end in plan.intervals)
                    or plan.gets != len(plan.intervals)
                    or any(not any(start <= unit <= end
                                   for start, end in plan.intervals)
                           for unit in query.mandatory_units)):
                raise AssertionError("predicted plan witness differs")
        candidate = PriceSelection(unit_price, get_price, plans,
                                   sum(plan.hits for plan in plans),
                                   units, gets)
        if (winner is None or
                (candidate.predicted_mass, -candidate.total_units,
                 -candidate.total_gets, -candidate.unit_price,
                 -candidate.get_price) >
                (winner.predicted_mass, -winner.total_units,
                 -winner.total_gets, -winner.unit_price,
                 -winner.get_price)):
            winner = candidate
    if winner is None:
        raise ValueError("no feasible price under physical caps")
    return winner
