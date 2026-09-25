"""Linear exact fast path when the unconstrained priced optimum meets hard caps.

This solver considers all interval covers of supplied weighted/mandatory
sites without resource caps. A feasible unconstrained optimum is also a
hard-cap optimum. Callers must check both caps before using its witness.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping, Sequence


@dataclass(frozen=True)
class UnconstrainedCover:
    intervals: tuple[tuple[int, int], ...]
    mass: int
    units: int
    gets: int
    objective: int


@dataclass(frozen=True)
class _State:
    mass: int
    units: int
    gets: int
    prev_open: bool
    continued: bool


def _key(state: _State, unit_price: int, get_price: int) -> tuple[int, int, int, int]:
    return (state.mass - unit_price * state.units - get_price * state.gets,
            -state.units, -state.gets, state.mass)


def _prefer(left: _State | None, right: _State | None,
            unit_price: int, get_price: int) -> _State | None:
    if left is None:
        return right
    if right is None:
        return left
    return right if _key(right, unit_price, get_price) > _key(left, unit_price, get_price) else left


def unconstrained_priced_cover(
    weight_by_unit: Mapping[int, int], mandatory_units: Sequence[int], *,
    page_count: int, unit_price: int, get_price: int,
) -> UnconstrainedCover:
    """Optimize modeled mass minus nonnegative linear unit/GET prices."""
    mandatory = tuple(mandatory_units)
    if (type(page_count) is not int or page_count <= 0
            or type(unit_price) is not int or unit_price < 0
            or type(get_price) is not int or get_price < 0
            or len(set(mandatory)) != len(mandatory)
            or any(type(unit) is not int or not 0 <= unit < page_count
                   for unit in mandatory)
            or any(type(unit) is not int or not 0 <= unit < page_count
                   or type(weight) is not int or weight <= 0
                   for unit, weight in weight_by_unit.items())
            or sum(weight_by_unit.values()) >= 2**30):
        raise ValueError("unconstrained priced interval geometry differs")
    sites = sorted(set(mandatory) | set(weight_by_unit))
    if not sites:
        return UnconstrainedCover((), 0, 0, 0, 0)
    required = set(mandatory)
    closed: _State | None = _State(0, 0, 0, False, False)
    opened: _State | None = None
    history: list[tuple[int, bool | None, bool | None, bool | None]] = []
    previous: int | None = None
    for site in sites:
        weight = weight_by_unit.get(site, 0)
        gap = 1 if previous is None else site - previous
        if site in required:
            next_closed = None
        else:
            next_closed = _prefer(closed, opened, unit_price, get_price)
        closed_prev = None if next_closed is None else next_closed is opened
        starts = []
        if closed is not None:
            starts.append(_State(closed.mass + weight, closed.units + 1,
                                 closed.gets + 1, False, False))
        if opened is not None:
            starts.append(_State(opened.mass + weight, opened.units + 1,
                                 opened.gets + 1, True, False))
        new_open: _State | None = None
        for start in starts:
            new_open = _prefer(new_open, start, unit_price, get_price)
        continued = (None if opened is None else
                     _State(opened.mass + weight, opened.units + gap,
                            opened.gets, True, True))
        next_open = _prefer(new_open, continued, unit_price, get_price)
        history.append((site, closed_prev,
                        None if next_open is None else next_open.prev_open,
                        None if next_open is None else next_open.continued))
        closed, opened, previous = next_closed, next_open, site
    winner = _prefer(closed, opened, unit_price, get_price)
    assert winner is not None
    best_open = winner is opened
    pending_end: int | None = None
    intervals: list[tuple[int, int]] = []
    for site, closed_prev, open_prev, continued in reversed(history):
        if not best_open:
            if closed_prev is None:
                raise AssertionError("unconstrained closed backtrace differs")
            best_open = closed_prev
        else:
            if pending_end is None:
                pending_end = site
            if continued is None or open_prev is None:
                raise AssertionError("unconstrained open backtrace differs")
            if not continued:
                intervals.append((site, pending_end))
                pending_end = None
            best_open = open_prev
    if best_open or pending_end is not None:
        raise AssertionError("unconstrained backtrace origin differs")
    intervals.reverse()
    witness = tuple(intervals)
    mass = sum(weight for site, weight in weight_by_unit.items()
               if any(start <= site <= end for start, end in witness))
    units = sum(end - start + 1 for start, end in witness)
    gets = len(witness)
    objective = mass - unit_price * units - get_price * gets
    if (mass, units, gets, objective) != (
            winner.mass, winner.units, winner.gets,
            _key(winner, unit_price, get_price)[0]):
        raise AssertionError("unconstrained priced witness differs")
    if any(not any(start <= unit <= end for start, end in witness)
           for unit in mandatory):
        raise AssertionError("unconstrained mandatory witness differs")
    return UnconstrainedCover(witness, mass, units, gets, objective)
