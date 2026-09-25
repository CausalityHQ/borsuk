"""Fit query-conditioned expected gain from optional physical units.

Mandatory units are already paid for, so their source truth must not train
optional weights. Only fit labels enter this module's fitting function.
Serving weights use rank and mandatory geometry, never query IDs or truth.
"""

from __future__ import annotations

from bisect import bisect_left
from dataclasses import dataclass
from itertools import groupby
from math import ceil, isfinite
from numbers import Integral
from typing import Iterable, Mapping, Sequence

from scripts.source_rank_utility import RankUtility, fit_rank_utility


def _optional(ranked_units: Sequence[int],
              mandatory_units: Sequence[int]) -> tuple[int, ...]:
    ranked, mandatory = tuple(ranked_units), tuple(mandatory_units)
    if (not ranked or len(set(ranked)) != len(ranked)
            or len(set(mandatory)) != len(mandatory)
            or any(not isinstance(unit, Integral) or isinstance(unit, bool)
                   or unit < 0 for unit in ranked + mandatory)
            or not set(mandatory).issubset(ranked)):
        raise ValueError("optional rank geometry differs")
    required = set(mandatory)
    return tuple(unit for unit in ranked if unit not in required)


@dataclass(frozen=True)
class OptionalRankUtility:
    rank_curve: RankUtility
    risk_edges: tuple[int, ...]
    risk_expected_hits: tuple[float, ...]
    fit_queries: int
    result_count: int

    def weights(self, ranked_units: Sequence[int],
                mandatory_units: Sequence[int], *,
                units_per_hit: int) -> dict[int, int]:
        """Integer expected-hit weights summing to a query risk estimate."""
        optional = _optional(ranked_units, mandatory_units)
        if (type(units_per_hit) is not int or units_per_hit <= 0
                or not self.risk_edges
                or len(self.risk_edges) != len(self.risk_expected_hits)
                or self.fit_queries <= 0
                or self.result_count <= 0
                or tuple(sorted(set(self.risk_edges))) != self.risk_edges
                or any(not isfinite(value) or value < 0
                       for value in self.risk_expected_hits)):
            raise ValueError("optional rank prediction geometry differs")
        if not optional:
            return {}
        risk_index = min(bisect_left(self.risk_edges, len(mandatory_units)),
                         len(self.risk_edges) - 1)
        target = round(self.risk_expected_hits[risk_index] * units_per_hit)
        if target <= 0:
            return {}
        means = self.rank_curve.expected_hits
        if any(not isfinite(value) or value < 0 for value in means):
            raise ValueError("optional rank curve differs")
        ratios = [
            means[index].as_integer_ratio() if index < len(means) else (0, 1)
            for index in range(len(optional))
        ]
        denominator = max(part for _, part in ratios)
        raw = [numerator * (denominator // part)
               for numerator, part in ratios]
        total = sum(raw)
        if total == 0:
            raise ValueError("positive optional risk lacks a rank distribution")
        shares = [(unit, ordinal, *divmod(target * weight, total))
                  for ordinal, (unit, weight) in enumerate(zip(optional, raw))]
        result = {unit: whole for unit, _, whole, _ in shares if whole > 0}
        remainder = target - sum(result.values())
        for unit, _, _, _ in sorted(shares, key=lambda item: (-item[3],
                                                              item[1]))[:remainder]:
            result[unit] = result.get(unit, 0) + 1
        if sum(result.values()) != target:
            raise AssertionError("optional rank mass normalization differs")
        return result


def fit_optional_rank_utility(
    examples: Iterable[tuple[Sequence[int], Sequence[int],
                             Mapping[int, int]]], *, result_count: int,
    bins: int = 4,
) -> OptionalRankUtility:
    """Fit optional rank order and monotone risk by mandatory-page count."""
    if (type(bins) is not int or bins <= 0
            or type(result_count) is not int or result_count <= 0):
        raise ValueError("optional risk bin count differs")
    rows: list[tuple[int, tuple[int, ...], dict[int, int]]] = []
    for ranked, mandatory, truth in examples:
        optional = _optional(ranked, mandatory)
        if (any(not isinstance(unit, Integral)
                or isinstance(unit, bool) or unit < 0
                or not isinstance(hits, Integral)
                or isinstance(hits, bool)
                or not 0 <= hits <= result_count
                for unit, hits in truth.items())
                or sum(truth.values()) > result_count):
            raise ValueError("optional risk fit geometry differs")
        optional_truth = {unit: int(truth[unit]) for unit in optional
                          if unit in truth}
        rows.append((len(mandatory), optional, optional_truth))
    if not rows:
        raise ValueError("optional risk fit panel is empty")
    rank_examples = [
        ({unit: float(index) for index, unit in enumerate(optional)}, truth)
        for _, optional, truth in rows if optional]
    rank_curve = (fit_rank_utility(rank_examples) if rank_examples
                  else RankUtility((), ()))
    target = ceil(len(rows) / bins)
    raw: list[tuple[int, int, int]] = []
    count = hits = 0
    for mandatory_count, group in groupby(sorted(rows, key=lambda row: row[0]),
                                          key=lambda row: row[0]):
        batch = list(group)
        count += len(batch)
        hits += sum(sum(truth.values()) for _, _, truth in batch)
        if count >= target and len(raw) < bins - 1:
            raw.append((mandatory_count, hits, count))
            count = hits = 0
    if count:
        raw.append((max(row[0] for row in rows), hits, count))
    # Pool non-monotone risk means. More scattered mandatory pages may
    # increase the fitted expected optional gain, never decrease it.
    blocks: list[tuple[int, int, int, int]] = []
    for index, (_, mass, samples) in enumerate(raw):
        blocks.append((index, index, mass, samples))
        while len(blocks) > 1:
            left, right = blocks[-2:]
            if left[2] * right[3] <= right[2] * left[3]:
                break
            blocks[-2:] = [(left[0], right[1], left[2] + right[2],
                            left[3] + right[3])]
    expected = [0.0] * len(raw)
    for start, end, mass, samples in blocks:
        expected[start:end + 1] = [mass / samples] * (end - start + 1)
    return OptionalRankUtility(rank_curve,
                               tuple(edge for edge, _, _ in raw),
                               tuple(expected), len(rows), result_count)
