"""Exact minimum-unit frontier for sparse truth and mandatory physical units.

This offline oracle knows ground truth. It computes a cost frontier for
validation and impossibility certificates, never a serving-time query plan.
"""

from __future__ import annotations

from typing import Mapping, Sequence

import numpy as np

INF = np.int64(1 << 60)


def min_units_frontier(
    truth_by_unit: Mapping[int, int], mandatory_units: Sequence[int], *,
    page_count: int, max_gets: int,
) -> np.ndarray:
    """Minimum complete units for exact truth hits and at most each GET cap.

    Rows index GET allowance 0..max_gets; columns index exact captured truth
    mass. Unreachable entries are INF. Every mandatory unit is covered.
    Only truth-bearing and mandatory positions are visited; an optimum can
    trim all interval endpoints to this sparse set.
    """
    mandatory = tuple(mandatory_units)
    if (type(page_count) is not int or page_count <= 0
            or type(max_gets) is not int or max_gets <= 0
            or not mandatory
            or any(type(unit) is not int or not 0 <= unit < page_count
                   for unit in mandatory)
            or any(type(unit) is not int or not 0 <= unit < page_count
                   or type(hits) is not int or hits < 0
                   for unit, hits in truth_by_unit.items())):
        raise ValueError("sparse truth cover geometry differs")
    sites = sorted(set(mandatory) | set(truth_by_unit))
    required = set(mandatory)
    total_hits = sum(truth_by_unit.values())
    shape = (max_gets + 1, total_hits + 1)
    closed = np.full(shape, INF, dtype=np.int64)
    opened = np.full(shape, INF, dtype=np.int64)
    closed[0, 0] = 0
    previous = None
    for unit in sites:
        hits = truth_by_unit.get(unit, 0)
        base = np.minimum(closed, opened)
        next_closed = (np.full(shape, INF, dtype=np.int64)
                       if unit in required else base)
        next_open = np.full(shape, INF, dtype=np.int64)
        next_open[1:, hits:] = np.minimum(
            next_open[1:, hits:], base[:-1, :total_hits + 1 - hits] + 1)
        if previous is not None:
            next_open[:, hits:] = np.minimum(
                next_open[:, hits:],
                opened[:, :total_hits + 1 - hits] + unit - previous)
        closed, opened = next_closed, next_open
        previous = unit
    exact = np.minimum(closed, opened)
    return np.minimum.accumulate(exact, axis=0)


def reconstruct_min_cover(
    truth_by_unit: Mapping[int, int], mandatory_units: Sequence[int], *,
    page_count: int, max_gets: int, target_hits: int,
) -> tuple[tuple[int, int], ...]:
    """Reconstruct one minimum-unit witness at exact target truth hits."""
    frontier = min_units_frontier(truth_by_unit, mandatory_units,
                                  page_count=page_count, max_gets=max_gets)
    if (type(target_hits) is not int or not 0 <= target_hits < frontier.shape[1]
            or frontier[max_gets, target_hits] >= INF):
        raise ValueError("target truth mass is infeasible")
    sites = sorted(set(mandatory_units) | set(truth_by_unit))
    required = set(mandatory_units)
    total_hits = frontier.shape[1] - 1
    shape = frontier.shape
    closed = np.full(shape, INF, dtype=np.int64)
    opened = np.full(shape, INF, dtype=np.int64)
    closed[0, 0] = 0
    closed_traces = []
    open_traces = []
    previous = None
    for unit in sites:
        hits = truth_by_unit.get(unit, 0)
        base_from_open = opened < closed
        base = np.minimum(closed, opened)
        next_closed = (np.full(shape, INF, dtype=np.int64)
                       if unit in required else base)
        closed_trace = np.full(shape, 255, dtype=np.uint8)
        if unit not in required:
            closed_trace[base < INF] = base_from_open[base < INF]
        next_open = np.full(shape, INF, dtype=np.int64)
        open_trace = np.full(shape, 255, dtype=np.uint8)
        width = total_hits + 1 - hits
        started = base[:-1, :width] + 1
        next_open[1:, hits:] = started
        open_trace[1:, hits:] = np.where(
            base_from_open[:-1, :width], 3, 2)
        open_trace[1:, hits:][started >= INF] = 255
        if previous is not None:
            extended = opened[:, :width] + unit - previous
            better = extended < next_open[:, hits:]
            next_open[:, hits:][better] = extended[better]
            open_trace[:, hits:][better] = 4
        closed_traces.append(closed_trace)
        open_traces.append(open_trace)
        closed, opened = next_closed, next_open
        previous = unit
    choices = []
    for gets in range(max_gets + 1):
        choices.append((int(closed[gets, target_hits]), gets, 0))
        choices.append((int(opened[gets, target_hits]), gets, 1))
    units, gets, mode = min(choices)
    if units != int(frontier[max_gets, target_hits]):
        raise AssertionError("frontier reconstruction cost differs")
    intervals = []
    end_unit = None
    hits_left = target_hits
    for index in range(len(sites) - 1, -1, -1):
        if mode == 0:
            action = int(closed_traces[index][gets, hits_left])
            if action not in (0, 1):
                raise AssertionError("closed interval trace differs")
            mode = action
            continue
        if end_unit is None:
            end_unit = sites[index]
        action = int(open_traces[index][gets, hits_left])
        hits_left -= truth_by_unit.get(sites[index], 0)
        if action == 4:
            continue
        if action not in (2, 3):
            raise AssertionError("open interval trace differs")
        intervals.append((sites[index], end_unit))
        end_unit = None
        gets -= 1
        mode = action - 2
    intervals.reverse()
    if (gets != 0 or mode != 0 or hits_left != 0 or end_unit is not None
            or sum(end - start + 1 for start, end in intervals) != units
            or sum(count for unit, count in truth_by_unit.items()
                   if any(start <= unit <= end for start, end in intervals))
                   != target_hits
            or any(not any(start <= unit <= end for start, end in intervals)
                   for unit in mandatory_units)):
        raise AssertionError("minimum truth cover witness differs")
    return tuple(intervals)
