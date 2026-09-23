"""Exact budgeted physical intervals for truth-free page weights.

The caller supplies weights derived only from query-time router scores. The
optimizer sees no ground truth or SQ8 result and returns inclusive page ranges.
"""

from __future__ import annotations

import numpy as np


def optimal_weighted_intervals(
    page_weights: dict[int, int], *, page_count: int, max_gets: int,
    max_units: int, full_page_units: int, last_page_units: int,
) -> tuple[int, tuple[tuple[int, int], ...]]:
    """Maximize covered weight over whole-page contiguous GET intervals.

    A weighted page can be skipped, started as a new GET, or included by
    extending the preceding open GET across its intervening zero-weight pages.
    States retain completed GET count, charged units, and open/closed status.
    """
    if (page_count <= 0 or max_gets <= 0 or max_units < 0
        or full_page_units <= 0 or last_page_units <= 0
        or any(page < 0 or page >= page_count or weight <= 0
               for page, weight in page_weights.items())
        or sum(page_weights.values()) >= 2**30):
        raise ValueError("weighted interval geometry or weights differ")
    if not page_weights:
        return 0, ()

    negative = -2**30
    shape = (max_gets + 1, max_units + 1)
    closed = np.full(shape, negative, dtype=np.int32)
    opened = np.full(shape, negative, dtype=np.int32)
    closed[0, 0] = 0
    history: list[tuple[int, int, int, np.ndarray, np.ndarray]] = []
    previous_page: int | None = None

    for page, weight in sorted(page_weights.items()):
        page_units = last_page_units if page == page_count - 1 else full_page_units
        gap_units = 0 if previous_page is None else (page - previous_page - 1) * full_page_units
        closed_from_open = opened > closed
        base = np.maximum(closed, opened)
        next_open = np.full(shape, negative, dtype=np.int32)
        continued = np.zeros(shape, dtype=bool)
        if page_units <= max_units:
            next_open[1:, page_units:] = base[:-1, :max_units + 1 - page_units] + weight
        continuation_cost = gap_units + page_units
        if continuation_cost <= max_units:
            candidate = opened[:, :max_units + 1 - continuation_cost] + weight
            better = candidate > next_open[:, continuation_cost:]
            next_open[:, continuation_cost:] = np.maximum(
                next_open[:, continuation_cost:], candidate,
            )
            continued[:, continuation_cost:] = better
        history.append((page, page_units, continuation_cost,
                        closed_from_open, continued))
        closed, opened = base, next_open
        previous_page = page

    best_closed = int(closed.max())
    best_open = int(opened.max())
    mode_open = best_open > best_closed
    final = opened if mode_open else closed
    gets, units = map(int, np.unravel_index(int(final.argmax()), shape))
    score = int(final[gets, units])
    ranges: list[tuple[int, int]] = []
    pending_end: int | None = None
    for page, page_units, continuation_cost, closed_from_open, continued in reversed(history):
        if not mode_open:
            mode_open = bool(closed_from_open[gets, units])
            continue
        if pending_end is None:
            pending_end = page
        if continued[gets, units]:
            units -= continuation_cost
        else:
            ranges.append((page, pending_end))
            pending_end = None
            gets -= 1
            units -= page_units
            mode_open = bool(closed_from_open[gets, units])
    if pending_end is not None or gets != 0 or units != 0 or mode_open:
        raise AssertionError("weighted interval witness reconstruction differs")
    ranges.reverse()
    return score, tuple(ranges)
