#!/usr/bin/env python3
"""Eight-byte source-trained row routing codes for the frozen 100k screen."""

from __future__ import annotations

import hashlib
import struct
from dataclasses import dataclass
from pathlib import Path

import numpy as np
from scipy.linalg import svd

from scripts.native_rotated_two_bit_codes import DIMENSIONS

SCHEMA = "borsuk-hundred-thousand-opq8-row-router-v1"
MAGIC = b"BRSOPQ81"
HEADER = struct.Struct("<8sIII")
SUBSPACES = 8
CENTROIDS = 256
TRAINING_ROWS = 8192
SEED = 20260923
OUTER_STEPS = 8
LLOYD_STEPS = 4
DOMAIN = b"borsuk-route-opq8-v1"


@dataclass(frozen=True, slots=True)
class Opq8Model:
    mean: np.ndarray
    rotation: np.ndarray
    books: np.ndarray


def training_ordinals(row_count: int, *, take: int = TRAINING_ROWS) -> np.ndarray:
    if row_count < take or take < CENTROIDS:
        raise ValueError("OPQ8 training population differs")
    prefix = DOMAIN + struct.pack("<I", SEED)
    keyed = [
        (hashlib.sha256(prefix + struct.pack("<Q", ordinal)).digest(), ordinal)
        for ordinal in range(row_count)
    ]
    keyed.sort()
    return np.asarray([ordinal for _, ordinal in keyed[:take]], dtype=np.int64)


def _assign(values: np.ndarray, centers: np.ndarray) -> np.ndarray:
    if values.ndim != 2 or centers.ndim != 2 or values.shape[1] != centers.shape[1]:
        raise ValueError("OPQ8 assignment shape differs")
    center_norms = np.sum(centers * centers, axis=1, dtype=np.float64)
    result = np.empty(len(values), dtype=np.uint8)
    for first in range(0, len(values), 4096):
        last = min(first + 4096, len(values))
        rows = values[first:last]
        distances = center_norms[None, :] - 2 * (rows @ centers.T)
        result[first:last] = np.argmin(distances, axis=1).astype(np.uint8)
    return result


def _farthest_first(values: np.ndarray, source_ordinals: np.ndarray) -> np.ndarray:
    if len(values) < CENTROIDS or len(source_ordinals) != len(values):
        raise ValueError("OPQ8 initialization population differs")
    centers = np.empty((CENTROIDS, values.shape[1]), dtype=np.float64)
    centers[0] = values[0]
    nearest = np.sum((values - centers[0]) ** 2, axis=1, dtype=np.float64)
    selected = np.zeros(len(values), dtype=bool)
    selected[0] = True
    for index in range(1, CENTROIDS):
        eligible = np.where(~selected)[0]
        best_distance = np.max(nearest[eligible])
        tied = eligible[nearest[eligible] == best_distance]
        chosen = int(tied[np.argmin(source_ordinals[tied])])
        centers[index] = values[chosen]
        selected[chosen] = True
        candidate = np.sum((values - centers[index]) ** 2, axis=1, dtype=np.float64)
        np.minimum(nearest, candidate, out=nearest)
    return centers


def _lloyd(values: np.ndarray, centers: np.ndarray, *, iterations: int = LLOYD_STEPS) -> np.ndarray:
    output = centers.copy()
    for _ in range(iterations):
        assignment = _assign(values, output)
        counts = np.bincount(assignment, minlength=CENTROIDS)
        sums = np.zeros_like(output)
        np.add.at(sums, assignment, values)
        occupied = counts > 0
        output[occupied] = sums[occupied] / counts[occupied, None]
    return output


def train_opq8(
    source_vectors: np.ndarray, *, training_rows: int = TRAINING_ROWS,
    outer_steps: int = OUTER_STEPS,
) -> tuple[Opq8Model, np.ndarray]:
    if (
        type(source_vectors) is not np.ndarray
        or source_vectors.dtype != np.float32
        or source_vectors.ndim != 2
        or source_vectors.shape[1] != DIMENSIONS
        or not np.isfinite(source_vectors).all()
        or outer_steps <= 0
    ):
        raise ValueError("OPQ8 source vectors differ")
    mean = np.mean(source_vectors, axis=0, dtype=np.float64).astype("<f4")
    ordinals = training_ordinals(len(source_vectors), take=training_rows)
    training = source_vectors[ordinals].astype(np.float64) - mean.astype(np.float64)
    transform = np.eye(DIMENSIONS, dtype=np.float64)
    width = DIMENSIONS // SUBSPACES
    books = np.empty((SUBSPACES, CENTROIDS, width), dtype=np.float64)
    for subspace in range(SUBSPACES):
        lo = subspace * width
        books[subspace] = _farthest_first(training[:, lo:lo + width], ordinals)
    for _ in range(outer_steps):
        rotated = training @ transform
        reconstructed = np.empty_like(rotated)
        for subspace in range(SUBSPACES):
            lo = subspace * width
            values = rotated[:, lo:lo + width]
            books[subspace] = _lloyd(values, books[subspace])
            codes = _assign(values, books[subspace])
            reconstructed[:, lo:lo + width] = books[subspace][codes]
        u, _, vt = svd(training.T @ reconstructed, full_matrices=False, lapack_driver="gesvd")
        transform = u @ vt
    stored_transform = transform.astype("<f4")
    rotated = training @ stored_transform.astype(np.float64)
    for subspace in range(SUBSPACES):
        lo = subspace * width
        books[subspace] = _lloyd(rotated[:, lo:lo + width], books[subspace])
    model = Opq8Model(mean, stored_transform, books.astype("<f4"))
    return model, ordinals


def encode_opq8(
    source_vectors: np.ndarray, physical_ordinals: np.ndarray, model: Opq8Model,
) -> np.ndarray:
    if (
        source_vectors.shape != (len(physical_ordinals), DIMENSIONS)
        or source_vectors.dtype != np.float32
        or set(int(i) for i in physical_ordinals) != set(range(len(physical_ordinals)))
    ):
        raise ValueError("OPQ8 physical source order differs")
    codes = np.empty((len(source_vectors), SUBSPACES), dtype=np.uint8)
    width = DIMENSIONS // SUBSPACES
    for first in range(0, len(codes), 4096):
        last = min(first + 4096, len(codes))
        centered = source_vectors[physical_ordinals[first:last]].astype(np.float64) - model.mean.astype(np.float64)
        rotated = centered @ model.rotation.astype(np.float64)
        for subspace in range(SUBSPACES):
            lo = subspace * width
            codes[first:last, subspace] = _assign(
                rotated[:, lo:lo + width], model.books[subspace].astype(np.float64),
            )
    return codes


def write_opq8_model(path: Path, model: Opq8Model) -> None:
    if (
        model.mean.shape != (DIMENSIONS,)
        or model.rotation.shape != (DIMENSIONS, DIMENSIONS)
        or model.books.shape != (SUBSPACES, CENTROIDS, DIMENSIONS // SUBSPACES)
        or not np.isfinite(model.mean).all()
        or not np.isfinite(model.rotation).all()
        or not np.isfinite(model.books).all()
    ):
        raise ValueError("OPQ8 model shape differs")
    with path.open("wb") as stream:
        stream.write(HEADER.pack(MAGIC, DIMENSIONS, SUBSPACES, CENTROIDS))
        stream.write(np.asarray(model.mean, dtype="<f4").tobytes())
        stream.write(np.asarray(model.rotation, dtype="<f4").tobytes())
        stream.write(np.asarray(model.books, dtype="<f4").tobytes())


def read_opq8_model(path: Path) -> Opq8Model:
    body = path.read_bytes()
    expected = HEADER.size + 4 * (DIMENSIONS + DIMENSIONS**2 + DIMENSIONS * CENTROIDS)
    if len(body) != expected:
        raise ValueError("OPQ8 model length differs")
    magic, dimensions, subspaces, centers = HEADER.unpack_from(body)
    if (magic, dimensions, subspaces, centers) != (MAGIC, DIMENSIONS, SUBSPACES, CENTROIDS):
        raise ValueError("OPQ8 model header differs")
    floats = np.frombuffer(body, dtype="<f4", count=(len(body) - HEADER.size) // 4, offset=HEADER.size)
    mean = floats[:DIMENSIONS]
    rotation = floats[DIMENSIONS:DIMENSIONS + DIMENSIONS**2].reshape(DIMENSIONS, DIMENSIONS)
    books = floats[DIMENSIONS + DIMENSIONS**2:].reshape(SUBSPACES, CENTROIDS, DIMENSIONS // SUBSPACES)
    if not np.isfinite(floats).all() or not np.allclose(
        rotation.T @ rotation, np.eye(DIMENSIONS), atol=2e-5,
    ):
        raise ValueError("OPQ8 model arrays differ")
    return Opq8Model(mean, rotation, books)


def row_adc_scores(query: np.ndarray, model: Opq8Model, codes: np.ndarray) -> np.ndarray:
    if query.shape != (DIMENSIONS,) or not np.isfinite(query).all() or codes.ndim != 2 or codes.shape[1] != SUBSPACES or codes.dtype != np.uint8:
        raise ValueError("OPQ8 query or code shape differs")
    rotated = (query.astype(np.float64) - model.mean.astype(np.float64)) @ model.rotation.astype(np.float64)
    scores = np.zeros(len(codes), dtype=np.float64)
    width = DIMENSIONS // SUBSPACES
    for subspace in range(SUBSPACES):
        lo = subspace * width
        residual = model.books[subspace].astype(np.float64) - rotated[lo:lo + width]
        table = np.sum(residual * residual, axis=1, dtype=np.float64)
        scores += table[codes[:, subspace]]
    return scores.astype(np.float32)


def rank_opq8_groups(
    scores: np.ndarray, page_row_counts: tuple[int, ...], *, group_pages: int = 4,
) -> tuple[int, ...]:
    if scores.dtype != np.float32 or sum(page_row_counts) != len(scores) or group_pages != 4:
        raise ValueError("OPQ8 group score shape differs")
    offsets = np.concatenate(([0], np.cumsum(page_row_counts)))
    ranked: list[tuple[float, float, int]] = []
    for group in range((len(page_row_counts) + group_pages - 1) // group_pages):
        first = int(offsets[group * group_pages])
        last = int(offsets[min((group + 1) * group_pages, len(page_row_counts))])
        candidates = scores[first:last]
        selected = np.lexsort((np.arange(first, last), candidates))[:min(4, len(candidates))]
        values = candidates[selected].astype(np.float64)
        ranked.append((float(np.mean(values, dtype=np.float64)), float(values[0]), group))
    return tuple(item[2] for item in sorted(ranked))
