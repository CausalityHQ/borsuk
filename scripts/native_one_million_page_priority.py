"""Turn sealed OPQ8 row scores into deterministic physical-page priority."""

from __future__ import annotations

from collections.abc import Sequence

import numpy as np


def rank_selected_pages(
    scores: np.ndarray,
    row_pages: np.ndarray,
    row_groups: np.ndarray,
    selected_groups: Sequence[int],
    page_count: int,
    *,
    top_rows: int = 100,
) -> tuple[int, ...]:
    """Rank top-row owner pages, then remaining pages, with stable ties."""
    selected = tuple(selected_groups)
    if (
        scores.ndim != 1
        or row_pages.shape != scores.shape
        or row_groups.shape != scores.shape
        or scores.dtype not in (np.dtype("float32"), np.dtype("float64"))
        or not np.isfinite(scores).all()
        or not selected
        or len(set(selected)) != len(selected)
        or any(type(group) is not int or group < 0 for group in selected)
        or type(page_count) is not int
        or page_count <= 0
        or type(top_rows) is not int
        or top_rows <= 0
        or np.any(row_pages >= page_count)
    ):
        raise ValueError("page priority inputs differ")
    available = np.flatnonzero(np.isin(row_groups, selected))
    if len(available) == 0:
        raise ValueError("selected group membership differs")
    present = np.bincount(row_groups[available].astype(np.intp), minlength=max(selected) + 1)
    if any(present[group] == 0 for group in selected):
        raise ValueError("selected group membership differs")
    values = scores[available]
    take = min(top_rows, len(available))
    threshold = np.partition(values, take - 1)[take - 1]
    contenders = available[values <= threshold]
    best = contenders[np.lexsort((contenders, scores[contenders]))[:take]]
    page_ids = row_pages[available].astype(np.intp, copy=False)
    pages = np.unique(page_ids)
    counts = np.bincount(row_pages[best].astype(np.intp, copy=False), minlength=page_count)
    minima = np.full(page_count, np.inf, dtype=scores.dtype)
    np.minimum.at(minima, page_ids, values)
    priority = sorted(
        (int(page) for page in pages),
        key=lambda page: (
            -int(counts[page] > 0),
            -int(counts[page]),
            float(minima[page]),
            page,
        ),
    )
    return tuple(priority)
