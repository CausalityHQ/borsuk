"""Bounded PQ64 score field for nominated physical units and neighbors.

The old/new source-order maps are authenticated once by the caller. This
function touches only resident PQ codes for rows in the declared candidate
units; it does not read SQ8 data or ground truth.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np

from scripts.v166_surrogate_probe import candidate_units


@dataclass(frozen=True)
class ScoredNeighborField:
    units: tuple[int, ...]
    old_rows: np.ndarray
    scores: np.ndarray


def score_neighbor_field(
    query: np.ndarray, *, nominees: tuple[int, ...],
    old_order: np.ndarray, inverse_old: np.ndarray,
    new_order: np.ndarray, inverse_new: np.ndarray,
    books: np.ndarray, codes: np.ndarray, unit_rows: int,
    radius: int = 1,
    metric: str = "squared_l2",
) -> ScoredNeighborField:
    """Score candidate rows by PQ64 L2 or reconstructed cosine."""
    rows = codes.shape[0]
    dimensions = query.size
    if (query.ndim != 1 or query.dtype != np.float32
            or dimensions <= 0 or not np.isfinite(query).all()
            or codes.ndim != 2 or codes.shape != (rows, 64)
            or codes.dtype != np.uint8
            or books.ndim != 3 or books.shape[:2] != (64, 256)
            or books.dtype != np.float32 or not np.isfinite(books).all()
            or not 0 <= books.shape[2] * 64 - dimensions < 64
            or old_order.shape != (rows,)
            or old_order.dtype not in (np.dtype("int32"), np.dtype("int64"))
            or any(mapping.shape != (rows,) or mapping.dtype != np.int64
                   for mapping in (inverse_old, new_order, inverse_new))):
        raise ValueError("V168 PQ field geometry differs")
    if metric not in ("squared_l2", "cosine"):
        raise ValueError("PQ score metric differs")
    if metric == "cosine" and not np.any(query):
        raise ValueError("cosine query has zero norm")
    units = candidate_units(nominees, old_order, inverse_new, rows,
                            unit_rows, radius=radius)
    max_units = (2 * radius + 1) * len(nominees)
    if len(units) > max_units:
        raise AssertionError("V168 candidate-unit bound differs")
    new_physical = np.concatenate([
        np.arange(unit * unit_rows, min((unit + 1) * unit_rows, rows),
                  dtype=np.int64) for unit in units])
    if new_physical.size > max_units * unit_rows:
        raise AssertionError("V168 candidate-row bound differs")
    old_rows = inverse_old[new_order[new_physical]]
    width = books.shape[2]
    padded = np.zeros(width * 64, dtype=np.float32)
    for subspace in range(64):
        first = subspace * dimensions // 64
        last = (subspace + 1) * dimensions // 64
        padded[subspace * width:subspace * width + last - first] = query[first:last]
    if metric == "squared_l2":
        delta = books - padded.reshape(64, 1, width)
        table = np.einsum("ijk,ijk->ij", delta, delta)
        scores = np.zeros(old_rows.size, dtype=np.float32)
        for subspace in range(64):
            scores += table[subspace, codes[old_rows, subspace]]
    else:
        dot_table = np.einsum("ijk,ik->ij", books,
                              padded.reshape(64, width))
        norm_table = np.einsum("ijk,ijk->ij", books, books)
        dot = np.zeros(old_rows.size, dtype=np.float32)
        norm_sq = np.zeros(old_rows.size, dtype=np.float32)
        for subspace in range(64):
            selected = codes[old_rows, subspace]
            dot += dot_table[subspace, selected]
            norm_sq += norm_table[subspace, selected]
        if not np.isfinite(norm_sq).all() or (norm_sq <= 0).any():
            raise ValueError("PQ reconstruction has zero or invalid norm")
        query_norm = np.float32(np.linalg.norm(query.astype(np.float64)))
        scores = -dot / (np.sqrt(norm_sq) * query_norm)
        if not np.isfinite(scores).all():
            raise ValueError("PQ reconstructed cosine score is nonfinite")
    return ScoredNeighborField(units, old_rows, scores)
