"""Necessary final-page containment bound for sealed 1M group plans."""

from __future__ import annotations

from collections import Counter
from collections.abc import Sequence


def oracle_page_hit_ceiling(
    truth_pages: Sequence[int],
    selected_groups: Sequence[int],
    page_groups: Sequence[int],
    *,
    maximum_pages: int = 32,
) -> int:
    """Best truth hit count under a page-count cap, ignoring byte size.

    This truth-aware bound is for final evaluation only. It says nothing
    about which pages a query-only scorer can find.
    """
    pages = oracle_page_selection(
        truth_pages, selected_groups, page_groups, maximum_pages=maximum_pages
    )
    chosen = set(pages)
    return sum(page in chosen for page in truth_pages)


def oracle_page_selection(
    truth_pages: Sequence[int],
    selected_groups: Sequence[int],
    page_groups: Sequence[int],
    *,
    maximum_pages: int = 32,
) -> tuple[int, ...]:
    """A deterministic witness attaining the optimistic count ceiling."""
    if (
        not truth_pages
        or not page_groups
        or type(maximum_pages) is not int
        or maximum_pages <= 0
        or any(type(group) is not int or group < 0 for group in page_groups)
        or any(type(page) is not int or not 0 <= page < len(page_groups) for page in truth_pages)
        or any(type(group) is not int or group not in page_groups for group in selected_groups)
        or len(set(selected_groups)) != len(selected_groups)
    ):
        raise ValueError("page ceiling authority differs")
    chosen_groups = set(selected_groups)
    counts = Counter(page for page in truth_pages if page_groups[page] in chosen_groups)
    return tuple(sorted(counts, key=lambda page: (-counts[page], page))[:maximum_pages])
