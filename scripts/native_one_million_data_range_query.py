"""One query's truth-free OPQ8 score-to-data-range plan."""

from __future__ import annotations

from collections.abc import Mapping, Sequence

import numpy as np

from scripts.native_one_million_data_ranges import admit_ranked_pages
from scripts.native_one_million_page_priority import rank_selected_pages


def plan_ranked_pages(
    ranked_global: Sequence[int], page_lengths: Mapping[str, Sequence[int]],
    *, maximum_gets: int = 32, maximum_bytes: int = 16_777_216,
) -> dict[str, object]:
    """Apply the frozen interval rule to a truth-free physical page order."""
    if set(page_lengths) not in ({"base"}, {"base", "delta"}):
        raise ValueError("query data-range layout differs")
    base_pages = len(page_lengths["base"])
    page_count = base_pages + len(page_lengths.get("delta", ()))
    if any(type(page) is not int or not 0 <= page < page_count for page in ranked_global):
        raise ValueError("query data-range priority differs")
    ranked = tuple(
        ("base", page) if page < base_pages else ("delta", page - base_pages)
        for page in ranked_global
    )
    admitted = admit_ranked_pages(page_lengths, ranked, maximum_gets, maximum_bytes)
    return {
        "priority_pages": [list(page) for page in ranked],
        "target_pages": [list(page) for page in admitted.targets],
        "ranges": [list(interval) for interval in admitted.cover.intervals],
        "included_pages": [list(page) for page in admitted.cover.included_pages],
        "gets": len(admitted.cover.intervals),
        "encoded_bytes": admitted.cover.bytes,
    }


def plan_query(
    scores: np.ndarray,
    row_pages: np.ndarray,
    page_groups: np.ndarray,
    page_lengths: Mapping[str, Sequence[int]],
    selected_groups: Sequence[int],
    *,
    maximum_gets: int = 32,
    maximum_bytes: int = 16_777_216,
    top_rows: int = 100,
) -> dict[str, object]:
    """Use only authenticated code scores, physical layout and group plan."""
    if (
        set(page_lengths) not in ({"base"}, {"base", "delta"})
        or page_groups.ndim != 1
        or len(page_groups) != sum(len(values) for values in page_lengths.values())
        or len(row_pages) != len(scores)
        or np.any(row_pages >= len(page_groups))
    ):
        raise ValueError("query data-range layout differs")
    row_groups = page_groups[row_pages.astype(np.intp)]
    ranked_global = rank_selected_pages(
        scores, row_pages, row_groups, selected_groups, len(page_groups), top_rows=top_rows
    )
    return plan_ranked_pages(
        ranked_global, page_lengths, maximum_gets=maximum_gets,
        maximum_bytes=maximum_bytes,
    )
