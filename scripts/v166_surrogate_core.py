"""Preregistered GT-blind V166 isotropic unit-score surrogate.

This predicts a count within an explicitly enumerated candidate universe.
It is neither a recall estimator nor a score bound.
"""

from __future__ import annotations

import math
from collections.abc import Iterable

ALPHAS = (0.25, 0.5, 1.0, 2.0, 4.0, 8.0)
MASS_SCALE = 100
PLANNER_SCORE_LIMIT = 2**30


def predict_count(
    center_distance: float, residual2: float, dimensions: int,
    threshold: float, alpha: float, eligible_rows: int,
) -> float:
    """Predict eligible rows whose SQ8 distance is at most ``threshold``.

    The center distance is ``||q-m||²``; the modeled mean is that distance
    plus residual energy. Mean and variance come from unit-normalized
    *source* vectors. The
    caller must count only non-nominee rows and omit a source pseudoquery
    from its own unit. SQ8 quantization mismatch is an observed error.
    """
    if (not all(map(math.isfinite, (center_distance, residual2, threshold, alpha)))
            or center_distance < 0 or residual2 < 0 or dimensions <= 0
            or alpha <= 0 or eligible_rows < 0):
        raise ValueError("V166 surrogate inputs differ")
    mean_distance = center_distance + residual2
    spread = max(1e-6, 2.0 * math.sqrt(residual2 * center_distance / dimensions))
    z = (threshold - mean_distance) / (alpha * spread)
    probability = 0.5 * math.erfc(-z / math.sqrt(2.0))
    return eligible_rows * probability


def fit_alpha(cases: Iterable[tuple[float, float, int, float, int, int]]) -> float:
    """Choose one global variance scale by fit-only unit-fraction MSE.

    Each case is (center distance, residual energy, D, SQ8 threshold,
    eligible non-nominee rows, actual SQ8 exceedances). Exact ties use
    the first (smaller) preregistered alpha.
    """
    records = tuple(cases)
    if not records:
        raise ValueError("V166 fit partition is empty")
    for *_, eligible, actual in records:
        if eligible <= 0 or not 0 <= actual <= eligible:
            raise ValueError("V166 fit count differs")
    losses = []
    for alpha in ALPHAS:
        error = 0.0
        for center, residual2, dimensions, threshold, eligible, actual in records:
            predicted_fraction = predict_count(
                center, residual2, dimensions, threshold, alpha, eligible,
            ) / eligible
            error += (predicted_fraction - actual / eligible) ** 2
        losses.append(error / len(records))
    return ALPHAS[min(range(len(ALPHAS)), key=lambda index: losses[index])]


def primary_weight(nominees: int, unit_rows: int, primary_units: int = 100) -> int:
    """One primary outweighs all modeled mass without int32 DP overflow."""
    if nominees <= 0 or unit_rows <= 0 or not 0 < primary_units <= nominees:
        raise ValueError("V166 primary geometry differs")
    maximum_mass = 3 * nominees * unit_rows * MASS_SCALE
    weight = maximum_mass + 1
    if primary_units * weight + maximum_mass >= PLANNER_SCORE_LIMIT:
        raise ValueError("V166 int32 planner score cannot represent geometry")
    return weight
