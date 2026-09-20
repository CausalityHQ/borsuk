#!/usr/bin/env python3
"""Typed ranked-gap range planning for the V99 G1 screen."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Mapping

from scripts.v97_row_width_screen import PageKey, RoutedPage


@dataclass(frozen=True, slots=True, order=True)
class PageRange:
    """One contiguous byte-range GET within one immutable run object."""

    object_role: str
    first_page: int
    last_page: int
    offset: int
    bytes: int

    def __post_init__(self) -> None:
        if (
            self.object_role not in ("base", "delta")
            or self.first_page < 0
            or self.last_page < self.first_page
            or self.offset < 0
            or self.bytes <= 0
        ):
            raise ValueError("page range differs")


@dataclass(frozen=True, slots=True)
class RangeSelection:
    """Canonical nonoverlapping range selection and its complete page union."""

    ranges: tuple[PageRange, ...]
    pages: tuple[PageKey, ...]
    gets: int
    bytes: int

    def __post_init__(self) -> None:
        if not self.ranges or tuple(sorted(self.ranges)) != self.ranges:
            raise ValueError("range order differs")
        for left, right in zip(self.ranges, self.ranges[1:], strict=False):
            if (
                left.object_role == right.object_role
                and left.last_page >= right.first_page
            ):
                raise ValueError("range overlap differs")
        expected_pages = tuple(
            PageKey(item.object_role, ordinal)
            for item in self.ranges
            for ordinal in range(item.first_page, item.last_page + 1)
        )
        if self.pages != expected_pages:
            raise ValueError("page union differs")
        if self.gets != len(self.ranges) or self.bytes != sum(
            item.bytes for item in self.ranges
        ):
            raise ValueError("range accounting differs")


def _registered_page(
    key: PageKey, pages: Mapping[PageKey, RoutedPage]
) -> RoutedPage:
    page = pages.get(key)
    if (
        page is None
        or page.key != key
        or page.offset < 0
        or page.encoded_bytes <= 0
    ):
        raise ValueError("registered page differs")
    return page


def _page_range(
    object_role: str,
    first_page: int,
    last_page: int,
    pages: Mapping[PageKey, RoutedPage],
) -> PageRange:
    registered = tuple(
        _registered_page(PageKey(object_role, ordinal), pages)
        for ordinal in range(first_page, last_page + 1)
    )
    for left, right in zip(registered, registered[1:], strict=False):
        if left.offset + left.encoded_bytes != right.offset:
            raise ValueError("page ranges are not contiguous")
    first, last = registered[0], registered[-1]
    return PageRange(
        object_role=object_role,
        first_page=first_page,
        last_page=last_page,
        offset=first.offset,
        bytes=last.offset + last.encoded_bytes - first.offset,
    )


def _selection(
    intervals: list[tuple[str, int, int]],
    pages: Mapping[PageKey, RoutedPage],
) -> RangeSelection:
    ranges = tuple(
        _page_range(role, first, last, pages)
        for role, first, last in sorted(intervals)
    )
    selected_pages = tuple(
        PageKey(item.object_role, ordinal)
        for item in ranges
        for ordinal in range(item.first_page, item.last_page + 1)
    )
    return RangeSelection(
        ranges=ranges,
        pages=selected_pages,
        gets=len(ranges),
        bytes=sum(item.bytes for item in ranges),
    )


def select_ranked_gap_ranges(
    ranked_pages: Iterable[PageKey],
    pages: Mapping[PageKey, RoutedPage],
    *,
    max_gets: int,
    max_bytes: int,
) -> RangeSelection:
    """Select ranked pages while greedily coalescing the cheapest byte gaps."""

    if max_gets <= 0 or max_bytes <= 0 or not pages:
        raise ValueError("range budget differs")
    intervals: list[tuple[str, int, int]] = []
    accepted: RangeSelection | None = None
    for key in ranked_pages:
        _registered_page(key, pages)
        if any(
            role == key.object_role and first <= key.ordinal <= last
            for role, first, last in intervals
        ):
            continue
        proposal = sorted((*intervals, (key.object_role, key.ordinal, key.ordinal)))
        valid = True
        while len(proposal) > max_gets:
            candidates: list[tuple[int, str, int, int, int]] = []
            for index, (left, right) in enumerate(
                zip(proposal, proposal[1:], strict=False)
            ):
                left_role, left_first, left_last = left
                right_role, right_first, right_last = right
                if left_role != right_role:
                    continue
                merged = _page_range(
                    left_role, left_first, right_last, pages
                )
                left_range = _page_range(
                    left_role, left_first, left_last, pages
                )
                right_range = _page_range(
                    right_role, right_first, right_last, pages
                )
                candidates.append(
                    (
                        merged.bytes - left_range.bytes - right_range.bytes,
                        left_role,
                        left_first,
                        right_last,
                        index,
                    )
                )
            if not candidates:
                valid = False
                break
            _, role, first, last, index = min(candidates)
            proposal[index : index + 2] = [(role, first, last)]
        if not valid:
            continue
        candidate = _selection(proposal, pages)
        if candidate.bytes > max_bytes:
            continue
        intervals = proposal
        accepted = candidate
    if accepted is None:
        raise ValueError("ranked pages do not fit range budget")
    return accepted
