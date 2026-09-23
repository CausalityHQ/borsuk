"""Exact byte accounting for role-preserving data-page range selection."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass

Page = tuple[str, int]
Interval = tuple[str, int, int]


@dataclass(frozen=True, slots=True)
class Cover:
    intervals: tuple[Interval, ...]
    included_pages: tuple[Page, ...]
    bytes: int


@dataclass(frozen=True, slots=True)
class Admission:
    targets: tuple[Page, ...]
    cover: Cover


def _validated(
    page_lengths: Mapping[str, Sequence[int]], target_pages: Sequence[Page],
    maximum_gets: int,
) -> tuple[dict[str, tuple[int, ...]], tuple[Page, ...]]:
    if (
        not page_lengths
        or any(role not in ("base", "delta") for role in page_lengths)
        or not isinstance(maximum_gets, int)
        or maximum_gets < 1
    ):
        raise ValueError("data-range contract differs")
    lengths = {role: tuple(values) for role, values in page_lengths.items()}
    if any(not values or any(type(value) is not int or value <= 0 for value in values) for values in lengths.values()):
        raise ValueError("data-range page length differs")
    targets = tuple(target_pages)
    if len(set(targets)) != len(targets):
        raise ValueError("data-range target repeats")
    for role, page in targets:
        if role not in lengths or type(page) is not int or not 0 <= page < len(lengths[role]):
            raise ValueError("data-range target differs")
    return lengths, targets


def minimum_cover(
    page_lengths: Mapping[str, Sequence[int]], target_pages: Sequence[Page],
    maximum_gets: int,
) -> Cover | None:
    """Minimize encoded bytes while covering targets in at most `maximum_gets` GETs."""
    lengths, targets = _validated(page_lengths, target_pages, maximum_gets)
    if not targets:
        return Cover((), (), 0)
    prefixes: dict[str, list[int]] = {}
    for role, values in lengths.items():
        prefix = [0]
        for value in values:
            prefix.append(prefix[-1] + value)
        prefixes[role] = prefix
    runs: list[Interval] = []
    for role, page in sorted(targets):
        if runs and runs[-1][0] == role and runs[-1][2] == page:
            previous = runs[-1]
            runs[-1] = role, previous[1], page + 1
        else:
            runs.append((role, page, page + 1))
    minimum_roles = len({role for role, _, _ in runs})
    if minimum_roles > maximum_gets:
        return None
    gaps = []
    for index, (left, right) in enumerate(zip(runs, runs[1:], strict=False)):
        if left[0] == right[0]:
            role = left[0]
            cost = prefixes[role][right[1]] - prefixes[role][left[2]]
            gaps.append((cost, role, left[2], index))
    bridge_count = max(0, len(runs) - maximum_gets)
    bridged = {item[3] for item in sorted(gaps)[:bridge_count]}
    intervals: list[Interval] = []
    for index, (role, start, end) in enumerate(runs):
        if index and index - 1 in bridged:
            previous = intervals[-1]
            intervals[-1] = role, previous[1], end
        else:
            intervals.append((role, start, end))
    included = tuple(
        (role, page)
        for role, start, end in intervals
        for page in range(start, end)
    )
    total = sum(prefixes[role][end] - prefixes[role][start] for role, start, end in intervals)
    return Cover(tuple(intervals), included, total)


def admit_ranked_pages(
    page_lengths: Mapping[str, Sequence[int]], ranked_pages: Sequence[Page],
    maximum_gets: int, maximum_bytes: int,
) -> Admission:
    """Admit each query-ranked page only if its optimal cover fits both caps."""
    if type(maximum_bytes) is not int or maximum_bytes < 0:
        raise ValueError("data-range byte cap differs")
    _validated(page_lengths, ranked_pages, maximum_gets)
    targets: list[Page] = []
    cover = minimum_cover(page_lengths, targets, maximum_gets)
    assert cover is not None
    for page in ranked_pages:
        next_cover = minimum_cover(page_lengths, (*targets, page), maximum_gets)
        if next_cover is not None and next_cover.bytes <= maximum_bytes:
            targets.append(page)
            cover = next_cover
    return Admission(tuple(targets), cover)
