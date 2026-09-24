"""GT-blind physical-unit admission under exact mandatory gap floors.

The frontier keeps a sorted physical unit set and a sorted multiset of gaps.
It admits optional units in caller rank order only when the *exact minimum*
GET-limited cover remains under the caller unit cap. Selection is greedy in
utility rank; no optimal utility claim is made. Ground truth is absent.
"""

from __future__ import annotations

from bisect import bisect_left, insort
from collections.abc import Iterable


class CoverFrontier:
    def __init__(self, page_count: int, max_gets: int,
                 max_units: int, mandatory: Iterable[int]):
        primary = tuple(mandatory)
        if (type(page_count) is not int or page_count <= 0
                or type(max_gets) is not int or max_gets <= 0
                or type(max_units) is not int or max_units < 0
                or not primary
                or any(type(page) is not int or not 0 <= page < page_count
                       for page in primary)):
            raise ValueError("physical cover geometry differs")
        self.page_count = page_count
        self.max_gets = max_gets
        self.max_units = max_units
        self._selected = sorted(set(primary))
        self._gaps = sorted(right - left - 1
                            for left, right in zip(self._selected,
                                                   self._selected[1:])
                            if right > left + 1)
        if self.minimum_units > max_units:
            raise ValueError("mandatory physical cover exceeds caller cap")

    @property
    def selected(self) -> tuple[int, ...]:
        return tuple(self._selected)

    @property
    def minimum_units(self) -> int:
        joins = max(0, len(self._gaps) + 1 - self.max_gets)
        return len(self._selected) + sum(self._gaps[:joins])

    def _remove_gap(self, gap: int) -> None:
        if gap <= 0:
            return
        position = bisect_left(self._gaps, gap)
        if position == len(self._gaps) or self._gaps[position] != gap:
            raise AssertionError("physical gap accounting differs")
        self._gaps.pop(position)

    def _add_gap(self, gap: int) -> None:
        if gap > 0:
            insort(self._gaps, gap)

    def try_add(self, unit: int) -> bool:
        """Admit one optional unit if a GET-limited cover still fits."""
        if type(unit) is not int or not 0 <= unit < self.page_count:
            raise ValueError("optional physical unit differs")
        index = bisect_left(self._selected, unit)
        if index < len(self._selected) and self._selected[index] == unit:
            return False
        left = self._selected[index - 1] if index else None
        right = self._selected[index] if index < len(self._selected) else None
        old_gap = right - left - 1 if left is not None and right is not None else 0
        left_gap = unit - left - 1 if left is not None else 0
        right_gap = right - unit - 1 if right is not None else 0
        self._remove_gap(old_gap)
        self._add_gap(left_gap)
        self._add_gap(right_gap)
        self._selected.insert(index, unit)
        if self.minimum_units <= self.max_units:
            return True
        self._selected.pop(index)
        self._remove_gap(left_gap)
        self._remove_gap(right_gap)
        self._add_gap(old_gap)
        return False

    def admit_ranked(self, units: Iterable[int]) -> tuple[int, ...]:
        accepted = []
        seen = set()
        for unit in units:
            if unit in seen:
                raise ValueError("optional unit rank has duplicates")
            seen.add(unit)
            if self.try_add(unit):
                accepted.append(unit)
        return tuple(accepted)

    def intervals(self) -> tuple[tuple[int, int], ...]:
        """Construct the exact cheapest cover, tying bridges by position."""
        gaps = [(right - left - 1, index)
                for index, (left, right) in enumerate(
                    zip(self._selected, self._selected[1:]))
                if right > left + 1]
        joins = max(0, len(gaps) + 1 - self.max_gets)
        bridged = {index for _, index in sorted(gaps)[:joins]}
        intervals = []
        start = self._selected[0]
        for index, (left, right) in enumerate(zip(self._selected,
                                                 self._selected[1:])):
            if right > left + 1 and index not in bridged:
                intervals.append((start, left))
                start = right
        intervals.append((start, self._selected[-1]))
        if (len(intervals) > self.max_gets
                or sum(end - start + 1 for start, end in intervals)
                   != self.minimum_units):
            raise AssertionError("minimum physical cover witness differs")
        return tuple(intervals)
