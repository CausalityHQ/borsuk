"""Bit-exact sign and magnitude planes for rotated two-bit row records."""

from __future__ import annotations

import numpy as np

SOURCE_BYTES = 200
SIGN_BYTES = 104
MAGNITUDE_BYTES = 96
PACKED_BYTES = 192
SYMBOLS = 768


def _records(value: np.ndarray, width: int) -> bool:
    return (
        isinstance(value, np.ndarray)
        and value.dtype == np.uint8
        and value.ndim == 2
        and value.shape[1] == width
    )


def split_records(records: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """Separate four two-bit symbols per byte into sign and magnitude bits."""
    if not _records(records, SOURCE_BYTES):
        raise ValueError("two-bit source records differ")
    symbols = np.empty((len(records), SYMBOLS), dtype=np.uint8)
    for position in range(4):
        symbols[:, position::4] = (records[:, :PACKED_BYTES] >> (2 * position)) & 3
    sign = np.empty((len(records), SIGN_BYTES), dtype=np.uint8)
    sign[:, :MAGNITUDE_BYTES] = np.packbits(symbols >> 1, axis=1, bitorder="little")
    sign[:, MAGNITUDE_BYTES:] = records[:, PACKED_BYTES:]
    magnitude = np.packbits(symbols & 1, axis=1, bitorder="little")
    return sign, magnitude


def join_planes(sign: np.ndarray, magnitude: np.ndarray) -> np.ndarray:
    """Recreate the original bytes before calling the qualified two-bit scorer."""
    if (
        not _records(sign, SIGN_BYTES)
        or not _records(magnitude, MAGNITUDE_BYTES)
        or len(sign) != len(magnitude)
    ):
        raise ValueError("two-bit split planes differ")
    signs = np.unpackbits(sign[:, :MAGNITUDE_BYTES], axis=1, bitorder="little")
    magnitudes = np.unpackbits(magnitude, axis=1, bitorder="little")
    symbols = (signs << 1) | magnitudes
    records = np.empty((len(sign), SOURCE_BYTES), dtype=np.uint8)
    records[:, :PACKED_BYTES] = 0
    for position in range(4):
        records[:, :PACKED_BYTES] |= symbols[:, position::4] << (2 * position)
    records[:, PACKED_BYTES:] = sign[:, MAGNITUDE_BYTES:]
    return records
