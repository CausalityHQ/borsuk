"""V166 GT-blind source-unit geometry and matched interval planning.

These functions support a frozen offline experiment. They do not infer
Recall@100 from source pseudoqueries.
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterable

import numpy as np

from scripts.v114_weighted_interval_plan import optimal_weighted_intervals
from scripts.v166_surrogate_core import primary_weight

PSEUDOQUERY_DOMAIN = b"borsuk-v166-pseudoquery-v1:"


def select_pseudoqueries(stable_ids: Iterable[int], count: int = 256) -> tuple[int, ...]:
    """Select source IDs by a corpus-independent stable digest order."""
    identifiers = tuple(int(value) for value in stable_ids)
    if count <= 0 or len(identifiers) < count or len(set(identifiers)) != len(identifiers):
        raise ValueError("V166 pseudoquery source IDs differ")
    return tuple(sorted(identifiers,
                        key=lambda value: (hashlib.sha256(
                            PSEUDOQUERY_DOMAIN + str(value).encode()).digest(), value))[:count])


def captured_exceedances(actual_by_unit: dict[int, int],
                         intervals: Iterable[tuple[int, int]]) -> int:
    """Count GT-blind above-threshold non-nominees in fetched units."""
    ranges = tuple(intervals)
    if (any(start < 0 or end < start for start, end in ranges)
            or any(left[1] >= right[0] for left, right in zip(ranges, ranges[1:]))
            or any(unit < 0 or actual < 0
                   for unit, actual in actual_by_unit.items())):
        raise ValueError("V166 capture intervals or counts differ")
    return sum(actual for unit, actual in actual_by_unit.items()
               if any(start <= unit <= end for start, end in ranges))


def _validated_source(vectors: np.ndarray) -> np.ndarray:
    values = np.asarray(vectors, dtype=np.float64)
    if values.ndim != 2 or values.shape[0] == 0 or not np.isfinite(values).all():
        raise ValueError("V166 source vector geometry differs")
    norms = np.linalg.norm(values, axis=1)
    if not np.isfinite(norms).all() or (norms <= 0).any():
        raise ValueError("V166 source vector norm differs")
    return values


def _moment(rows: np.ndarray) -> tuple[np.ndarray, float]:
    source = _validated_source(rows)
    mean = source.mean(axis=0)
    residual = float(np.mean(np.sum((source - mean) ** 2, axis=1)))
    if not np.isfinite(residual) or residual < 0:
        raise ValueError("V166 source residual differs")
    return mean, residual


def source_unit_moments(
    vectors: np.ndarray, order: np.ndarray, unit_rows: int,
) -> tuple[np.ndarray, np.ndarray]:
    """Build f16 means and f32 residual energy from ordered source rows."""
    rows, dimensions = vectors.shape
    if (unit_rows <= 0 or rows == 0 or dimensions == 0
            or order.shape != (rows,) or not np.issubdtype(order.dtype, np.integer)
            or not np.array_equal(np.sort(order), np.arange(rows))):
        raise ValueError("V166 source order differs")
    units = (rows + unit_rows - 1) // unit_rows
    means = np.empty((units, dimensions), dtype=np.float16)
    residuals = np.empty(units, dtype=np.float32)
    for unit in range(units):
        start = unit * unit_rows
        stop = min(start + unit_rows, rows)
        mean, residual = _moment(vectors[order[start:stop]])
        means[unit] = mean.astype(np.float16)
        residuals[unit] = np.float32(residual)
    if not np.isfinite(means).all() or not np.isfinite(residuals).all():
        raise ValueError("V166 source moment storage overflow")
    return means, residuals


def unit_moment_without(
    vectors: np.ndarray, order: np.ndarray, unit: int,
    source_row: int, unit_rows: int,
) -> tuple[np.ndarray, float]:
    """Recompute one pseudoquery's own unit without its source row."""
    rows = len(order)
    if unit_rows <= 0 or not 0 <= source_row < rows or not 0 <= unit * unit_rows < rows:
        raise ValueError("V166 own-unit geometry differs")
    subset = order[unit * unit_rows:min((unit + 1) * unit_rows, rows)]
    if np.count_nonzero(subset == source_row) != 1 or len(subset) <= 1:
        raise ValueError("V166 own source row is absent or unit becomes empty")
    return _moment(vectors[subset[subset != source_row]])


def candidate_units(
    nominees: Iterable[int], old_order: np.ndarray,
    inverse_order: np.ndarray, rows: int, unit_rows: int,
) -> tuple[int, ...]:
    """Nominee units and immediate physical neighbors under V164 order."""
    roster = tuple(nominees)
    if (rows <= 0 or unit_rows <= 0 or old_order.shape != (rows,)
            or inverse_order.shape != (rows,) or not roster
            or len(set(roster)) != len(roster)
            or any(type(value) not in (int, np.int32, np.int64)
                   or not 0 <= value < rows for value in roster)):
        raise ValueError("V166 candidate roster differs")
    unit_count = (rows + unit_rows - 1) // unit_rows
    answer: set[int] = set()
    for old_physical in roster:
        source_row = int(old_order[old_physical])
        unit = int(inverse_order[source_row]) // unit_rows
        answer.update(neighbor for neighbor in (unit - 1, unit, unit + 1)
                      if 0 <= neighbor < unit_count)
    return tuple(sorted(answer))


def modeled_plan(
    masses: dict[int, int], primary_units: Iterable[int], *,
    page_count: int, max_gets: int, max_units: int,
    nominee_count: int, unit_rows: int,
) -> tuple[tuple[int, int], ...]:
    """Maximize integer modeled mass while including exact-primary units."""
    primary = set(primary_units)
    if (not primary or not 0 < len(primary) <= nominee_count
            or any(not 0 <= unit < page_count for unit in primary)
            or any(not 0 <= unit < page_count or not 0 <= mass <= unit_rows * 100
                   for unit, mass in masses.items())):
        raise ValueError("V166 modeled planner inputs differ")
    force = primary_weight(nominee_count, unit_rows, len(primary))
    weights = {unit: mass for unit, mass in masses.items() if mass > 0}
    for unit in primary:
        weights[unit] = weights.get(unit, 0) + force
    _, intervals = optimal_weighted_intervals(
        weights, page_count=page_count, max_gets=max_gets,
        max_units=max_units, full_page_units=1, last_page_units=1,
    )
    if not all(any(start <= unit <= end for start, end in intervals)
               for unit in primary):
        raise ValueError("V166 modeled plan misses an exact-primary unit")
    return intervals
