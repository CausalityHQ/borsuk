#!/usr/bin/env python3
"""Exact SQ8 byte intervals for the V63/V70 single-layout replay.

Ranges are half-open, matching Rust object_store's bounded range convention.
The gaps between selected pages are charged because their rows are fetched and
rescored. This module does not choose pages or silently trim an over-cap plan.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class RangePlan:
    ranges: tuple[tuple[int, int], ...]
    gets: int
    bytes: int

    def within(self, *, gets: int, bytes_limit: int) -> bool:
        return self.gets <= gets and self.bytes <= bytes_limit


def plan_page_ranges(
    pages: list[int] | tuple[int, ...],
    *,
    gap_pages: int,
    rows: int,
    page_rows: int,
    row_bytes: int,
) -> RangePlan:
    if rows <= 0 or page_rows <= 0 or row_bytes <= 0 or gap_pages < 0:
        raise ValueError("invalid physical geometry")
    total_pages = (rows + page_rows - 1) // page_rows
    if any(page < 0 or page >= total_pages for page in pages):
        raise ValueError("page outside object")
    if any(a >= b for a, b in zip(pages, pages[1:])):
        raise ValueError("pages must be strictly increasing")
    if not pages:
        return RangePlan((), 0, 0)

    spans: list[tuple[int, int]] = []
    first = last = pages[0]
    for page in pages[1:]:
        if page <= last + gap_pages + 1:
            last = page
        else:
            spans.append((first * page_rows * row_bytes,
                          min((last + 1) * page_rows, rows) * row_bytes))
            first = last = page
    spans.append((first * page_rows * row_bytes,
                  min((last + 1) * page_rows, rows) * row_bytes))
    return RangePlan(tuple(spans), len(spans), sum(end - start for start, end in spans))
