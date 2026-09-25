"""Source-fit, monotone PQ rank utility for optional physical units.

Only the fit operation sees truth counts. Ranking and prediction need PQ
scores, physical unit IDs, and the frozen curve; they never need ground truth.
The model has no dataset identity, vector-count knee, or fixed memory policy.
"""

from __future__ import annotations

from dataclasses import dataclass
from math import isfinite
from numbers import Integral, Real
from typing import Iterable, Mapping

def rank_units(score_by_unit: Mapping[int, float]) -> tuple[int, ...]:
    """Rank candidate physical units by lowest PQ row score, then unit ID."""
    if (not score_by_unit or any(
        not isinstance(unit, Integral) or isinstance(unit, bool) or unit < 0
        or not isinstance(score, Real) or not isfinite(score)
        for unit, score in score_by_unit.items()
    )):
        raise ValueError("PQ unit score geometry differs")
    return tuple(sorted(score_by_unit, key=lambda unit: (score_by_unit[unit], unit)))


@dataclass(frozen=True)
class RankUtility:
    """Expected exact source hits per PQ rank after weighted isotonic fit."""

    expected_hits: tuple[float, ...]
    samples: tuple[int, ...]

    def weights(self, ranked_units: Iterable[int], *,
                units_per_hit: int) -> dict[int, int]:
        """Positive integer weights in the caller's explicit price units."""
        ranked = tuple(ranked_units)
        if (type(units_per_hit) is not int or units_per_hit <= 0
                or len(set(ranked)) != len(ranked)
                or any(not isinstance(unit, Integral) or unit < 0
                       for unit in ranked)):
            raise ValueError("PQ ranked unit geometry differs")
        return {
            unit: weight
            for rank, unit in enumerate(ranked[:len(self.expected_hits)])
            if (weight := round(self.expected_hits[rank] * units_per_hit)) > 0
        }


def fit_rank_utility(
    examples: Iterable[tuple[Mapping[int, float], Mapping[int, int]]],
) -> RankUtility:
    """Pool adjacent rank means until expected hits decrease with rank.

    Every fit query contributes one observation at every candidate rank. The
    caller supplies source truth counts only after sealing fit features. Truth
    outside the candidate universe is accounted for by the experiment gate.
    """
    total: list[int] = []
    samples: list[int] = []
    query_count = 0
    for score_by_unit, truth_by_unit in examples:
        ranked = rank_units(score_by_unit)
        if (any(not isinstance(unit, Integral) or unit not in score_by_unit
                or not isinstance(hits, Integral) or isinstance(hits, bool)
                or hits < 0 for unit, hits in truth_by_unit.items())):
            raise ValueError("source truth unit counts differ")
        query_count += 1
        for rank, unit in enumerate(ranked):
            if rank == len(total):
                total.append(0)
                samples.append(0)
            total[rank] += int(truth_by_unit.get(unit, 0))
            samples[rank] += 1
    if not query_count:
        raise ValueError("source fit panel is empty")

    # A block is [start rank, end rank, total hits, observed units]. Its mean
    # must be at least the next block's mean. Cross products avoid float
    # comparison drift in the merge decision.
    blocks: list[tuple[int, int, int, int]] = []
    for rank, (hits, count) in enumerate(zip(total, samples, strict=True)):
        blocks.append((rank, rank, hits, count))
        while len(blocks) >= 2:
            left, right = blocks[-2:]
            if left[2] * right[3] >= right[2] * left[3]:
                break
            blocks[-2:] = [(left[0], right[1], left[2] + right[2],
                            left[3] + right[3])]
    expected = [0.0] * len(total)
    for start, end, hits, count in blocks:
        expected[start:end + 1] = [hits / count] * (end - start + 1)
    return RankUtility(tuple(expected), tuple(samples))
