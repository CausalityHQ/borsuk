"""Exact minimum GET-plus-unit cost on a bounded physical interval frontier.

Utilities are authenticated nonnegative integers supplied by the caller.
This solves only the physical interval problem; utility calibration and
latency conversion are separate measured obligations.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np


@dataclass(frozen=True)
class CostedPlan:
    intervals: tuple[tuple[int, int], ...]
    score: int
    gets: int
    units: int
    cost: int


def plan_min_cost_intervals(
    page_weights: dict[int, int], *, mandatory: tuple[int, ...],
    target: int, page_count: int, max_gets: int, max_units: int,
    get_cost: int, unit_cost: int,
) -> CostedPlan:
    """Choose minimum positive linear cost meeting utility and hard caps.

    A state tracks completed GETs, charged physical units and whether
    the last weighted/mandatory page belongs to an open interval. The
    maximum utility retained at each state dominates lower-utility plans
    with the same exact charges. Skipping a mandatory page is forbidden.
    """
    if (type(page_count) is not int or page_count <= 0
            or any(type(value) is not int or value < 0 for value in
                   (target, max_gets, max_units))
            or max_gets == 0 or max_units == 0
            or type(get_cost) is not int or get_cost <= 0
            or type(unit_cost) is not int or unit_cost <= 0
            or len(set(mandatory)) != len(mandatory)
            or any(type(page) is not int or not 0 <= page < page_count
                   for page in mandatory)
            or any(type(page) is not int or not 0 <= page < page_count
                   or type(weight) is not int or weight <= 0
                   for page, weight in page_weights.items())
            or sum(page_weights.values()) >= 2**30):
        raise ValueError("V169 physical frontier geometry differs")

    negative = -(2**30)
    shape = (max_gets + 1, max_units + 1)
    closed = np.full(shape, negative, dtype=np.int32)
    opened = np.full(shape, negative, dtype=np.int32)
    closed[0, 0] = 0
    history: list[tuple[int, int, np.ndarray, np.ndarray, bool]] = []
    mandatory_set = set(mandatory)
    previous_page: int | None = None

    for page in sorted(set(page_weights) | mandatory_set):
        weight = page_weights.get(page, 0)
        continuation = (1 if previous_page is None
                        else page - previous_page)
        closed_from_open = opened > closed
        base = np.maximum(closed, opened)
        next_closed = (np.full(shape, negative, dtype=np.int32)
                       if page in mandatory_set else base)
        next_open = np.full(shape, negative, dtype=np.int32)
        continued = np.zeros(shape, dtype=bool)
        next_open[1:, 1:] = base[:-1, :-1] + weight
        if continuation <= max_units:
            candidate = opened[:, :max_units + 1 - continuation] + weight
            section = next_open[:, continuation:]
            continued[:, continuation:] = candidate > section
            np.maximum(section, candidate, out=section)
        history.append((page, continuation, closed_from_open,
                        continued, page in mandatory_set))
        closed, opened = next_closed, next_open
        previous_page = page

    best: tuple[int, int, int, int] | None = None
    best_open = False
    for gets, units in np.argwhere(np.maximum(closed, opened) >= target):
        gets, units = int(gets), int(units)
        mode_open = bool(opened[gets, units] > closed[gets, units])
        score = int(max(closed[gets, units], opened[gets, units]))
        key = (get_cost * gets + unit_cost * units, gets, units, -score)
        if best is None or key < best:
            best, best_open = key, mode_open
    if best is None:
        raise ValueError("V169 mandatory or target infeasible within caps")

    cost, gets, units, neg_score = best
    cursor_gets, cursor_units = gets, units
    ranges: list[tuple[int, int]] = []
    pending_end: int | None = None
    for page, continuation, closed_from_open, continued, required in reversed(history):
        if not best_open:
            if required:
                raise AssertionError("V169 mandatory page was skipped")
            best_open = bool(closed_from_open[cursor_gets, cursor_units])
            continue
        if pending_end is None:
            pending_end = page
        if continued[cursor_gets, cursor_units]:
            cursor_units -= continuation
        else:
            ranges.append((page, pending_end))
            pending_end = None
            cursor_gets -= 1
            cursor_units -= 1
            best_open = bool(closed_from_open[cursor_gets, cursor_units])
    if (pending_end is not None or cursor_gets != 0 or cursor_units != 0
            or best_open):
        raise AssertionError("V169 interval witness reconstruction differs")
    ranges.reverse()
    intervals = tuple(ranges)
    covered = {page for start, end in intervals
               for page in range(start, end + 1)}
    actual_gets = len(intervals)
    actual_units = len(covered)
    actual_score = sum(weight for page, weight in page_weights.items()
                       if page in covered)
    if (actual_gets != gets or actual_units != units
            or actual_score != -neg_score
            or not mandatory_set.issubset(covered)
            or actual_gets > max_gets or actual_units > max_units
            or cost != get_cost * actual_gets + unit_cost * actual_units):
        raise AssertionError("V169 physical witness fails independent recount")
    return CostedPlan(intervals, actual_score, actual_gets, actual_units, cost)
