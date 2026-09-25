"""Fit per-unit source utility from query-normalized PQ cosine scores.

The feature and prediction path use only resident PQ scores. Source truth
enters `fit_margin_utility` on a disjoint fit panel; a caller must seal the
model and holdout plans before opening holdout labels. Weights are expected
truth counts, not a recall guarantee.
"""

from __future__ import annotations

from bisect import bisect_left
from dataclasses import dataclass
from itertools import groupby
from math import ceil, isfinite
from numbers import Integral, Real
from typing import Iterable, Mapping, Sequence

SCALE = 1_000_000


def normalized_pq_margins(
    minimum_by_unit: Mapping[int, float], nominee_scores: Sequence[float],
) -> dict[int, float]:
    """Use the 100th PQ nominee score and top-100 PQ spread as one metric.

    Lower reconstructed-cosine scores are better. The caller excludes the
    source query row from both the nominee roster and unit minima.
    """
    if (not minimum_by_unit or len(nominee_scores) < 100
            or any(not isinstance(unit, Integral) or isinstance(unit, bool)
                   or unit < 0 or not isinstance(score, Real)
                   or not isfinite(score)
                   for unit, score in minimum_by_unit.items())
            or any(not isinstance(score, Real) or not isfinite(score)
                   for score in nominee_scores)):
        raise ValueError("PQ cosine margin geometry differs")
    sorted_scores = sorted(float(score) for score in nominee_scores)
    cutoff = sorted_scores[99]
    spread = max(sorted_scores[89] - sorted_scores[9], 1e-6)
    return {int(unit): (float(score) - cutoff) / spread
            for unit, score in minimum_by_unit.items()}


@dataclass(frozen=True)
class MarginUtility:
    upper_margins: tuple[float, ...]
    expected_hits: tuple[float, ...]
    samples: tuple[int, ...]

    def weights(self, margin_by_unit: Mapping[int, float]) -> dict[int, int]:
        """Predict nonnegative integer unit utility with no truth input."""
        if (not margin_by_unit or any(
            not isinstance(unit, Integral) or isinstance(unit, bool)
            or unit < 0 or not isinstance(margin, Real)
            or not isfinite(margin)
            for unit, margin in margin_by_unit.items()
        )):
            raise ValueError("PQ margin prediction geometry differs")
        result = {}
        for unit, margin in margin_by_unit.items():
            index = min(bisect_left(self.upper_margins, margin),
                        len(self.upper_margins) - 1)
            weight = round(self.expected_hits[index] * SCALE)
            if weight > 0:
                result[int(unit)] = weight
        return result


def fit_margin_utility(
    examples: Iterable[tuple[Mapping[int, float], Mapping[int, int]]], *,
    bins: int = 64,
) -> MarginUtility:
    """Equal-count score bins, then weighted nonincreasing isotonic means."""
    if type(bins) is not int or bins <= 0:
        raise ValueError("margin bin count differs")
    observations: list[tuple[float, int]] = []
    query_count = 0
    for margins, truth_by_unit in examples:
        if (not margins or any(not isinstance(unit, Integral)
                               or isinstance(unit, bool) or unit < 0
                               or not isinstance(margin, Real)
                               or not isfinite(margin)
                               for unit, margin in margins.items())
                or any(not isinstance(unit, Integral)
                       or isinstance(unit, bool) or unit < 0
                       or not isinstance(hits, Integral)
                       or isinstance(hits, bool) or not 0 <= hits <= 32
                       for unit, hits in truth_by_unit.items())):
            raise ValueError("margin fit geometry differs")
        query_count += 1
        observations.extend((float(margin), int(truth_by_unit.get(unit, 0)))
                            for unit, margin in margins.items())
    if not query_count:
        raise ValueError("margin fit panel is empty")
    observations.sort(key=lambda item: item[0])
    target = ceil(len(observations) / bins)
    raw: list[tuple[float, int, int]] = []
    count = total = 0
    for margin, group in groupby(observations, key=lambda item: item[0]):
        grouped = list(group)
        count += len(grouped)
        total += sum(hits for _, hits in grouped)
        if count >= target and len(raw) < bins - 1:
            raw.append((margin, total, count))
            count = total = 0
    if count:
        raw.append((observations[-1][0], total, count))
    if not raw:
        raise AssertionError("margin binning lost observations")

    # Pool adjacent violations of nonincreasing expected truth mass.
    blocks: list[tuple[int, int, int, int]] = []
    for index, (_, hits, samples) in enumerate(raw):
        blocks.append((index, index, hits, samples))
        while len(blocks) > 1:
            left, right = blocks[-2:]
            if left[2] * right[3] >= right[2] * left[3]:
                break
            blocks[-2:] = [(left[0], right[1], left[2] + right[2],
                            left[3] + right[3])]
    expected = [0.0] * len(raw)
    for start, end, hits, samples in blocks:
        expected[start:end + 1] = [hits / samples] * (end - start + 1)
    return MarginUtility(tuple(edge for edge, _, _ in raw),
                         tuple(expected), tuple(count for _, _, count in raw))
