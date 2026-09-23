"""Bounded matrix scoring for the rejoined rotated two-bit records."""

from __future__ import annotations

import numpy as np

from scripts.native_rotated_two_bit_codes import decode_levels, rotate_rows
from scripts.native_rotated_two_bit_evaluation import _read_record_scalars

MAXIMUM_ROWS = 4096
MAXIMUM_QUERIES = 1000


def coded_distance_batch(
    queries: np.ndarray, mean: np.ndarray, records: np.ndarray,
    *, rotation_seed: int,
) -> np.ndarray:
    """Return float32 squared-L2 primary scores for one bounded row batch."""
    if (
        type(queries) is not np.ndarray or queries.dtype != np.float32
        or queries.ndim != 2 or queries.shape[1] != 768
        or not 1 <= len(queries) <= MAXIMUM_QUERIES
        or not np.isfinite(queries).all()
        or type(mean) is not np.ndarray or mean.dtype != np.float32
        or mean.shape != (768,) or not np.isfinite(mean).all()
        or not isinstance(records, np.ndarray) or records.dtype != np.uint8
        or records.ndim != 2 or records.shape[1] != 200
        or not 1 <= len(records) <= MAXIMUM_ROWS
    ):
        raise ValueError("progressive score authority differs")
    centered = queries.astype(np.float64) - mean.astype(np.float64)
    rotated = rotate_rows(centered, rotation_seed=rotation_seed)
    levels = decode_levels(np.asarray(records[:, :192])).astype(np.float64)
    scales, _ = _read_record_scalars(records)
    coded = levels * scales[:, None]
    dots = rotated @ coded.T
    query_norms = np.sum(centered * centered, axis=1, dtype=np.float64)
    coded_norms = np.sum(coded * coded, axis=1, dtype=np.float64)
    scores = (query_norms[:, None] + coded_norms[None, :] - 2 * dots).astype(np.float32)
    if not np.isfinite(scores).all():
        raise ValueError("progressive score nonfinite")
    return scores
