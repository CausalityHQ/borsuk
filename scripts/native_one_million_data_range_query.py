"""One query's truth-free OPQ8 score-to-data-range plan."""

from __future__ import annotations

from collections.abc import Mapping, Sequence

import numpy as np

from scripts.native_one_million_data_ranges import admit_ranked_pages
from scripts.native_one_million_page_priority import rank_selected_pages


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
        set(page_lengths) != {"base", "delta"}
        or page_groups.ndim != 1
        or len(page_groups) != sum(len(page_lengths[role]) for role in ("base", "delta"))
        or len(row_pages) != len(scores)
        or np.any(row_pages >= len(page_groups))
    ):
        raise ValueError("query data-range layout differs")
    base_pages = len(page_lengths["base"])
    row_groups = page_groups[row_pages.astype(np.intp)]
    ranked_global = rank_selected_pages(
        scores, row_pages, row_groups, selected_groups, len(page_groups), top_rows=top_rows
    )
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
