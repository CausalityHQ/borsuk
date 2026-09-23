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


@dataclass(frozen=True)
class AdmittedPlan:
    nominated_pages: tuple[int, ...]
    rejected_pages: tuple[int, ...]
    plan: RangePlan


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


def _range_plan_from_intervals(
    intervals: list[tuple[int, int]], *, rows: int, page_rows: int, row_bytes: int
) -> RangePlan:
    ranges = tuple(
        (first * page_rows * row_bytes,
         min((last + 1) * page_rows, rows) * row_bytes)
        for first, last in intervals
    )
    return RangePlan(ranges, len(ranges), sum(end - start for start, end in ranges))


def admit_ranked_pages(
    ranked_pages: list[int] | tuple[int, ...],
    *,
    rows: int,
    page_rows: int,
    row_bytes: int,
    max_gets: int,
    max_bytes: int,
) -> AdmittedPlan:
    """Admit pages in score order, merging cheapest gaps to satisfy both caps.

    All accepted pages remain covered. Every bridged page is charged, and a
    rejected page does not change the current physical plan. Query truth is
    absent from the interface by construction.
    """
    if rows <= 0 or page_rows <= 0 or row_bytes <= 0 or max_gets <= 0 or max_bytes <= 0:
        raise ValueError("invalid geometry or range budget")
    total_pages = (rows + page_rows - 1) // page_rows
    if any(page < 0 or page >= total_pages for page in ranked_pages):
        raise ValueError("page outside object")

    intervals: list[tuple[int, int]] = []
    accepted: list[int] = []
    rejected: list[int] = []
    seen: set[int] = set()
    for page in ranked_pages:
        if page in seen:
            continue
        seen.add(page)
        if any(first <= page <= last for first, last in intervals):
            accepted.append(page)
            continue
        proposal = sorted((*intervals, (page, page)))
        merged: list[tuple[int, int]] = []
        for first, last in proposal:
            if merged and first <= merged[-1][1] + 1:
                merged[-1] = (merged[-1][0], max(merged[-1][1], last))
            else:
                merged.append((first, last))
        while len(merged) > max_gets:
            index = min(
                range(len(merged) - 1),
                key=lambda i: merged[i + 1][0] - merged[i][1] - 1,
            )
            merged[index:index + 2] = [(merged[index][0], merged[index + 1][1])]
        candidate = _range_plan_from_intervals(
            merged, rows=rows, page_rows=page_rows, row_bytes=row_bytes
        )
        if candidate.bytes <= max_bytes:
            intervals = merged
            accepted.append(page)
        else:
            rejected.append(page)
    return AdmittedPlan(
        tuple(accepted), tuple(rejected),
        _range_plan_from_intervals(
            intervals, rows=rows, page_rows=page_rows, row_bytes=row_bytes
        ),
    )
