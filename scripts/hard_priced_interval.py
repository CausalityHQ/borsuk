"""Exact offline priced interval reference with hard GET and unit caps.

This reference retains the best nonnegative integer modeled mass for each
exact (GET, unit, open/closed) state. Mandatory units cannot be skipped.
The implementation is O(sites × max_gets × max_units) in time and stores
two trace bitmaps per site; a low-latency Rust refinement is separate work.
The weights are caller-supplied predictions, never ground truth here.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping, Sequence

import numpy as np


@dataclass(frozen=True)
class HardPricedCover:
    intervals: tuple[tuple[int, int], ...]
    mass: int
    units: int
    gets: int
    objective: int


def hard_priced_cover(
    weight_by_unit: Mapping[int, int], mandatory_units: Sequence[int], *,
    page_count: int, max_gets: int, max_units: int,
    unit_price: int, get_price: int,
) -> HardPricedCover:
    """Maximize modeled mass minus prices among physically feasible plans."""
    mandatory = tuple(mandatory_units)
    if (type(page_count) is not int or page_count <= 0
            or type(max_gets) is not int or max_gets <= 0
            or type(max_units) is not int or max_units <= 0
            or type(unit_price) is not int or unit_price < 0
            or type(get_price) is not int or get_price < 0
            or len(set(mandatory)) != len(mandatory)
            or any(type(unit) is not int or not 0 <= unit < page_count
                   for unit in mandatory)
            or any(type(unit) is not int or not 0 <= unit < page_count
                   or type(weight) is not int or weight <= 0
                   for unit, weight in weight_by_unit.items())
            or sum(weight_by_unit.values()) >= 2**30):
        raise ValueError("hard priced interval geometry differs")
    sites = sorted(set(mandatory) | set(weight_by_unit))
    if not sites:
        return HardPricedCover((), 0, 0, 0, 0)
    gets_cap = min(max_gets, len(sites))
    shape = (gets_cap + 1, max_units + 1)
    negative = -(2**40)
    closed = np.full(shape, negative, dtype=np.int64)
    opened = np.full(shape, negative, dtype=np.int64)
    closed[0, 0] = 0
    history: list[tuple[int, int, np.ndarray, np.ndarray, bool]] = []
    required = set(mandatory)
    previous: int | None = None
    for site in sites:
        weight = weight_by_unit.get(site, 0)
        continuation = 1 if previous is None else site - previous
        closed_from_open = opened > closed
        base = np.maximum(closed, opened)
        next_closed = (np.full(shape, negative, dtype=np.int64)
                       if site in required else base)
        next_open = np.full(shape, negative, dtype=np.int64)
        continued = np.zeros(shape, dtype=bool)
        next_open[1:, 1:] = base[:-1, :-1] + weight
        if continuation <= max_units:
            candidate = opened[:, :max_units + 1 - continuation] + weight
            section = next_open[:, continuation:]
            continued[:, continuation:] = candidate > section
            np.maximum(section, candidate, out=section)
        history.append((site, continuation, closed_from_open,
                        continued, site in required))
        closed, opened = next_closed, next_open
        previous = site

    best: tuple[int, int, int, int] | None = None
    best_open = False
    for gets, units in np.argwhere(np.maximum(closed, opened) >= 0):
        gets, units = int(gets), int(units)
        is_open = bool(opened[gets, units] > closed[gets, units])
        mass = int(max(closed[gets, units], opened[gets, units]))
        key = (mass - unit_price * units - get_price * gets,
               -units, -gets, mass)
        if best is None or key > best:
            best, best_open = key, is_open
    if best is None:
        raise ValueError("mandatory cover infeasible within hard caps")

    objective, neg_units, neg_gets, expected_mass = best
    cursor_units, cursor_gets = -neg_units, -neg_gets
    intervals: list[tuple[int, int]] = []
    pending_end: int | None = None
    for site, continuation, closed_from_open, continued, required_site in reversed(history):
        if not best_open:
            if required_site:
                raise AssertionError("hard priced mandatory site was skipped")
            best_open = bool(closed_from_open[cursor_gets, cursor_units])
            continue
        if pending_end is None:
            pending_end = site
        if continued[cursor_gets, cursor_units]:
            cursor_units -= continuation
        else:
            intervals.append((site, pending_end))
            pending_end = None
            cursor_gets -= 1
            cursor_units -= 1
            best_open = bool(closed_from_open[cursor_gets, cursor_units])
    if pending_end is not None or cursor_units or cursor_gets or best_open:
        raise AssertionError("hard priced interval witness reconstruction differs")
    intervals.reverse()
    witness = tuple(intervals)
    gets = len(witness)
    units = sum(end - start + 1 for start, end in witness)
    mass = sum(weight for site, weight in weight_by_unit.items()
               if any(start <= site <= end for start, end in witness))
    if (units > max_units or gets > max_gets
            or any(not any(start <= site <= end for start, end in witness)
                   for site in mandatory)
            or any(left[1] >= right[0]
                   for left, right in zip(witness, witness[1:]))
            or mass != expected_mass
            or objective != mass - unit_price * units - get_price * gets):
        raise AssertionError("hard priced interval witness accounting differs")
    return HardPricedCover(witness, mass, units, gets, objective)
