"""Bounded two-pass source build of mirrored rotated two-bit page objects."""

from __future__ import annotations

import hashlib
from collections.abc import Callable, Iterable, Mapping
from pathlib import Path

import numpy as np

from scripts.native_progressive_page_objects import write_page_objects
from scripts.native_rotated_two_bit_codes import _fit_records

Batch = tuple[np.ndarray, np.ndarray]
BatchFactory = Callable[[], Iterable[Batch]]


def _checked_batch(
    ids: np.ndarray, vectors: np.ndarray, physical: Mapping[int, int], seen: np.ndarray,
) -> np.ndarray:
    if (
        type(ids) is not np.ndarray or ids.dtype != np.int64 or ids.ndim != 1
        or type(vectors) is not np.ndarray or vectors.dtype != np.float32
        or vectors.shape != (len(ids), 768) or not 1 <= len(ids) <= 4096
        or not np.isfinite(vectors).all() or len(np.unique(ids)) != len(ids)
    ):
        raise ValueError("progressive source batch differs")
    positions = np.fromiter((physical.get(int(row_id), -1) for row_id in ids),
                            dtype=np.int64, count=len(ids))
    if np.any(positions < 0) or np.any(positions >= len(seen)) or np.any(seen[positions]):
        raise ValueError("progressive source physical order differs")
    seen[positions] = True
    return positions


def build_from_source_batches(
    root: Path, batches: BatchFactory, physical: Mapping[int, int],
    page_row_counts: tuple[int, ...], *, base_pages: int, source_sha256: str,
    layout_sha256: str, rotation_seed: int,
) -> dict[str, object]:
    """Fit the global mean, encode physical rows, then seal split page objects."""
    rows = len(physical)
    if (
        rows <= 0 or sum(page_row_counts) != rows
        or len(set(physical.values())) != rows
        or any(type(position) is not int or not 0 <= position < rows
               for position in physical.values())
        or any((root / name).exists() for name in (
            "mean.bin", "records.bin", "progressive-code-seal.json",
        ))
    ):
        raise ValueError("progressive source physical population differs")
    root.mkdir(parents=True, exist_ok=True)
    seen = np.zeros(rows, dtype=np.bool_)
    vector_sum = np.zeros(768, dtype=np.float64)
    first_digest = hashlib.sha256()
    observed = 0
    for ids, vectors in batches():
        _checked_batch(ids, vectors, physical, seen)
        first_digest.update(ids.astype("<i8", copy=False).tobytes(order="C"))
        first_digest.update(vectors.tobytes(order="C"))
        vector_sum += np.sum(vectors, axis=0, dtype=np.float64)
        observed += len(ids)
    if observed != rows or not np.all(seen):
        raise ValueError("progressive first source pass incomplete")
    mean = (vector_sum / rows).astype("<f4")
    mean_body = mean.tobytes(order="C")
    (root / "mean.bin").write_bytes(mean_body)
    mean_sha = hashlib.sha256(mean_body).hexdigest()
    records = np.memmap(root / "records.bin", mode="w+", dtype=np.uint8,
                        shape=(rows, 200))
    seen.fill(False)
    second_digest = hashlib.sha256()
    observed = 0
    for ids, vectors in batches():
        positions = _checked_batch(ids, vectors, physical, seen)
        second_digest.update(ids.astype("<i8", copy=False).tobytes(order="C"))
        second_digest.update(vectors.tobytes(order="C"))
        centered = vectors.astype(np.float64) - mean.astype(np.float64)
        records[positions] = _fit_records(centered, rotation_seed=rotation_seed)
        observed += len(ids)
    if (
        observed != rows or not np.all(seen)
        or second_digest.digest() != first_digest.digest()
    ):
        raise ValueError("progressive second source pass differs")
    records.flush()
    seal = write_page_objects(
        root, records, page_row_counts, base_pages=base_pages,
        source_sha256=source_sha256, layout_sha256=layout_sha256,
        mean_sha256=mean_sha, rotation_seed=rotation_seed,
    )
    del records
    return {
        "rows": rows, "source_stream_sha256": first_digest.hexdigest(),
        "mean_sha256": mean_sha, "seal": seal,
    }
