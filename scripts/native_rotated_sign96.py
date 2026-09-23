"""Bounded 96-byte rotated-sign row-score records, without corpus artifacts."""

from __future__ import annotations

import numpy as np

from scripts.native_rotated_two_bit_codes import rotate_rows

DIMENSIONS = 768
KEPT_COORDINATES = np.asarray(
    [index for index in range(DIMENSIONS) if index % 48 != 47], dtype=np.intp
)
KEPT_COORDINATES.setflags(write=False)
SIGN_BYTES = 94
RECORD_BYTES = 96
BATCH_ROWS = 2048
MAX_ENCODE_ROWS = 4096


def _checked_mean(mean: np.ndarray) -> None:
    if (
        type(mean) is not np.ndarray
        or mean.dtype != np.float32
        or mean.shape != (DIMENSIONS,)
        or not np.isfinite(mean).all()
    ):
        raise ValueError("sign96 mean differs")


def encode_records(
    vectors: np.ndarray, mean: np.ndarray, *, rotation_seed: int
) -> np.ndarray:
    """Encode physical-order source rows with the rounded scale used at query time."""
    _checked_mean(mean)
    if (
        type(vectors) is not np.ndarray
        or vectors.dtype != np.float32
        or vectors.ndim != 2
        or vectors.shape[1] != DIMENSIONS
        or len(vectors) > MAX_ENCODE_ROWS
        or not np.isfinite(vectors).all()
    ):
        raise ValueError("sign96 source differs")
    records = np.empty((len(vectors), RECORD_BYTES), dtype=np.uint8)
    mean64 = mean.astype(np.float64)
    for first in range(0, len(vectors), BATCH_ROWS):
        last = min(first + BATCH_ROWS, len(vectors))
        centered = vectors[first:last].astype(np.float64) - mean64
        rotated = rotate_rows(centered, rotation_seed=rotation_seed)
        kept = rotated[:, KEPT_COORDINATES]
        scales = np.mean(np.abs(kept), axis=1, dtype=np.float64)
        with np.errstate(over="ignore", invalid="ignore"):
            stored = scales.astype("<f2")
        if not np.isfinite(stored).all() or np.any(stored < 0):
            raise ValueError("sign96 scale cannot be stored")
        records[first:last, :SIGN_BYTES] = np.packbits(kept >= 0, axis=1, bitorder="little")
        records[first:last, SIGN_BYTES:] = np.frombuffer(
            stored.tobytes(order="C"), dtype=np.uint8
        ).reshape(-1, 2)
    return records


def score_records(
    query: np.ndarray, mean: np.ndarray, records: np.ndarray, *, rotation_seed: int
) -> np.ndarray:
    """Score reconstructed rows, including the full query-only norm."""
    _checked_mean(mean)
    if (
        type(query) is not np.ndarray
        or query.dtype != np.float32
        or query.shape != (DIMENSIONS,)
        or not np.isfinite(query).all()
        or type(records) is not np.ndarray
        or records.dtype != np.uint8
        or records.ndim != 2
        or records.shape[1] != RECORD_BYTES
    ):
        raise ValueError("sign96 score authority differs")
    centered_q = query.astype(np.float64) - mean.astype(np.float64)
    rotated_q = rotate_rows(centered_q[None, :], rotation_seed=rotation_seed)[0]
    kept_q = rotated_q[KEPT_COORDINATES]
    query_norm = float(np.sum(centered_q * centered_q, dtype=np.float64))
    scores = np.empty(len(records), dtype=np.float32)
    for first in range(0, len(records), BATCH_ROWS):
        last = min(first + BATCH_ROWS, len(records))
        part = records[first:last]
        scales = np.frombuffer(part[:, SIGN_BYTES:].tobytes(), dtype="<f2").astype(np.float64)
        if not np.isfinite(scales).all() or np.any(scales < 0):
            raise ValueError("sign96 record scale differs")
        signs = np.unpackbits(part[:, :SIGN_BYTES], axis=1, bitorder="little")
        dot = np.sum(np.where(signs != 0, kept_q, -kept_q), axis=1, dtype=np.float64)
        scores[first:last] = (query_norm + 752 * scales * scales - 2 * scales * dot).astype(
            np.float32
        )
    if not np.isfinite(scores).all():
        raise ValueError("sign96 score is nonfinite")
    return scores
