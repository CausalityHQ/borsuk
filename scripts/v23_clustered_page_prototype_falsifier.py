#!/usr/bin/env python3
"""Bounded evidence-only falsifier for clustered historical V23 posting pages."""

from __future__ import annotations

import os

for _thread_variable in (
    "OPENBLAS_NUM_THREADS",
    "OMP_NUM_THREADS",
    "MKL_NUM_THREADS",
    "NUMEXPR_NUM_THREADS",
):
    os.environ[_thread_variable] = "1"

import dataclasses
import re
import struct

from blake3 import blake3
import numpy

_PAGE_HEADER_BYTES = 96
_PAGE_MAX_ENCODED_BYTES = 245_760
_HEX_DIGEST = re.compile(r"[0-9a-f]{64}\Z")
_U64_MASK = (1 << 64) - 1


@dataclasses.dataclass(frozen=True, slots=True)
class PageRef:
    """Complete immutable authority for one historical BVP2 page."""

    generation_checksum: bytes
    page_ordinal: int
    metric: str
    dimensions: int
    family: str
    code_width: int
    path: str
    checksum: str
    encoded_bytes: int
    primary_rows: int
    replicated_rows: int


class SplitMix64:
    """The registered deterministic 64-bit stream used for initialization."""

    def __init__(self, state: int) -> None:
        if type(state) is not int:
            raise TypeError("SplitMix64 state must be an integer")
        self.state = state & _U64_MASK

    def next_u64(self) -> int:
        self.state = (self.state + 0x9E37_79B9_7F4A_7C15) & _U64_MASK
        value = self.state
        value = ((value ^ (value >> 30)) * 0xBF58_476D_1CE4_E5B9) & _U64_MASK
        value = ((value ^ (value >> 27)) * 0x94D0_49BB_1331_11EB) & _U64_MASK
        return (value ^ (value >> 31)) & _U64_MASK


def _concrete_nonnegative_int(value: object, role: str, *, positive: bool = False) -> int:
    if type(value) is not int or value < int(positive):
        qualifier = "positive" if positive else "nonnegative"
        raise ValueError(f"{role} must be a concrete {qualifier} integer")
    return value


def _validate_page_reference(reference: PageRef) -> None:
    if type(reference) is not PageRef:
        raise ValueError("page reference has the wrong concrete type")
    _concrete_nonnegative_int(reference.page_ordinal, "page ordinal")
    dimensions = _concrete_nonnegative_int(reference.dimensions, "page dimensions", positive=True)
    code_width = _concrete_nonnegative_int(reference.code_width, "page code width", positive=True)
    encoded_bytes = _concrete_nonnegative_int(
        reference.encoded_bytes, "page encoded bytes", positive=True
    )
    primary_rows = _concrete_nonnegative_int(
        reference.primary_rows, "page primary rows", positive=True
    )
    _concrete_nonnegative_int(reference.replicated_rows, "page replicated rows")
    if (
        type(reference.generation_checksum) is not bytes
        or len(reference.generation_checksum) != 32
        or reference.generation_checksum == bytes(32)
        or type(reference.metric) is not str
        or reference.metric != "cosine"
        or type(reference.family) is not str
        or reference.family != "f16-flat"
        or code_width != dimensions * 2
        or encoded_bytes > _PAGE_MAX_ENCODED_BYTES
        or type(reference.checksum) is not str
        or _HEX_DIGEST.fullmatch(reference.checksum) is None
        or type(reference.path) is not str
        or reference.path != f"pages/{reference.checksum}"
        or primary_rows + reference.replicated_rows <= 0
    ):
        raise ValueError("page reference authority differs")


def _strictly_increasing(values: tuple[bytes, ...]) -> bool:
    return all(left < right for left, right in zip(values, values[1:], strict=False))


def decode_bvp2_page(reference: PageRef, body: bytes) -> numpy.ndarray:
    """Authenticate and decode one immutable historical f16-flat BVP2 page."""

    _validate_page_reference(reference)
    if type(body) is not bytes:
        raise ValueError("page body must be immutable bytes")
    if (
        len(body) != reference.encoded_bytes
        or len(body) < _PAGE_HEADER_BYTES
        or blake3(body).hexdigest() != reference.checksum
    ):
        raise ValueError("page envelope authority differs")
    if (
        body[:4] != b"BVP2"
        or body[4] != 2
        or body[5] != 3
        or body[6] != 4
        or body[7] != 0
        or body[66:96] != bytes(30)
    ):
        raise ValueError("page header authority differs")

    dimensions, page_ordinal, primary_rows, replicated_rows = struct.unpack_from(
        "<IIII", body, 8
    )
    id_section_bytes, code_section_bytes = struct.unpack_from("<II", body, 24)
    generation_checksum = body[32:64]
    (code_width,) = struct.unpack_from("<H", body, 64)
    if (
        dimensions != reference.dimensions
        or page_ordinal != reference.page_ordinal
        or primary_rows != reference.primary_rows
        or replicated_rows != reference.replicated_rows
        or generation_checksum != reference.generation_checksum
        or code_width != reference.code_width
    ):
        raise ValueError("page header differs from reference")

    row_count = primary_rows + replicated_rows
    offset_bytes = (row_count + 1) * 4
    if (
        primary_rows == 0
        or id_section_bytes < offset_bytes
        or code_section_bytes != row_count * code_width
        or _PAGE_HEADER_BYTES + id_section_bytes + code_section_bytes != len(body)
    ):
        raise ValueError("page section authority differs")

    offsets = struct.unpack_from(f"<{row_count + 1}I", body, _PAGE_HEADER_BYTES)
    id_bytes = id_section_bytes - offset_bytes
    if (
        offsets[0] != 0
        or offsets[-1] != id_bytes
        or any(left >= right for left, right in zip(offsets, offsets[1:], strict=False))
    ):
        raise ValueError("page offsets differ")

    id_start = _PAGE_HEADER_BYTES + offset_bytes
    identifiers = tuple(
        body[id_start + start : id_start + end]
        for start, end in zip(offsets, offsets[1:], strict=False)
    )
    primary_identifiers = identifiers[:primary_rows]
    replica_identifiers = identifiers[primary_rows:]
    if (
        not _strictly_increasing(primary_identifiers)
        or not _strictly_increasing(replica_identifiers)
        or len(set(identifiers)) != len(identifiers)
    ):
        raise ValueError("page record-ID authority differs")

    code_start = _PAGE_HEADER_BYTES + id_section_bytes
    encoded = numpy.frombuffer(body, dtype="<f2", count=row_count * dimensions, offset=code_start)
    if encoded.size != row_count * dimensions or not numpy.isfinite(encoded).all():
        raise ValueError("page f16 code authority differs")
    vectors = encoded.reshape(row_count, dimensions).astype(numpy.float32)
    norms = numpy.linalg.norm(vectors.astype(numpy.float64), axis=1)
    if not numpy.isfinite(norms).all() or numpy.any(norms <= 0.0):
        raise ValueError("page vector norm differs")
    normalized = vectors.astype(numpy.float64) / norms[:, None]
    return normalized.astype(numpy.float32)


def _validated_unit_matrix(value: numpy.ndarray, role: str) -> numpy.ndarray:
    matrix = numpy.asarray(value)
    if matrix.ndim != 2 or matrix.shape[0] == 0 or matrix.shape[1] == 0:
        raise ValueError(f"{role} must be a nonempty matrix")
    if not numpy.issubdtype(matrix.dtype, numpy.floating):
        raise ValueError(f"{role} must contain floating-point values")
    matrix = matrix.astype(numpy.float64)
    norms = numpy.linalg.norm(matrix, axis=1)
    if (
        not numpy.isfinite(matrix).all()
        or not numpy.isfinite(norms).all()
        or numpy.any(norms <= 0.0)
    ):
        raise ValueError(f"{role} contains an invalid vector")
    return matrix / norms[:, None]


def _initial_centers(vectors: numpy.ndarray, count: int, generator: SplitMix64) -> numpy.ndarray:
    row_count = vectors.shape[0]
    first = (generator.next_u64() * row_count) >> 64
    selected = [first]
    centers = [vectors[first].copy()]
    while len(centers) < count:
        similarities = vectors @ numpy.asarray(centers, dtype=numpy.float64).T
        nearest_cosine_distance = numpy.maximum(0.0, 1.0 - similarities.max(axis=1))
        masses = nearest_cosine_distance * nearest_cosine_distance
        masses[numpy.asarray(selected, dtype=numpy.intp)] = 0.0
        total = float(masses.sum(dtype=numpy.float64))
        if total == 0.0:
            choice = next(index for index in range(row_count) if index not in selected)
        else:
            boundary = (generator.next_u64() / float(1 << 64)) * total
            cumulative = 0.0
            choice = -1
            for index, mass in enumerate(masses):
                cumulative += float(mass)
                if cumulative > boundary:
                    choice = index
                    break
            if choice < 0:
                choice = max(index for index, mass in enumerate(masses) if mass > 0.0)
        if choice in selected:
            raise ValueError("k-means++ selected a duplicate input position")
        selected.append(choice)
        centers.append(vectors[choice].copy())
    return numpy.asarray(centers, dtype=numpy.float64)


def _repair_empty_assignments(
    vectors: numpy.ndarray,
    centers: numpy.ndarray,
    assignments: numpy.ndarray,
    count: int,
) -> numpy.ndarray:
    repaired = assignments.copy()
    counts = numpy.bincount(repaired, minlength=count)
    for empty in numpy.flatnonzero(counts == 0):
        donor_index = -1
        donor_distance = -1.0
        for index, assigned in enumerate(repaired):
            if counts[assigned] < 2:
                continue
            distance = 1.0 - float(vectors[index] @ centers[assigned])
            if distance > donor_distance:
                donor_distance = distance
                donor_index = index
        if donor_index < 0:
            raise ValueError("empty cluster has no valid donor")
        old_cluster = int(repaired[donor_index])
        repaired[donor_index] = int(empty)
        counts[old_cluster] -= 1
        counts[empty] += 1
    return repaired


def spherical_kmeans(
    vectors: numpy.ndarray,
    body_checksum: str,
    clusters: int = 32,
    iterations: int = 8,
) -> numpy.ndarray:
    """Fit the registered deterministic spherical page means."""

    matrix = _validated_unit_matrix(vectors, "page vectors")
    if (
        type(body_checksum) is not str
        or _HEX_DIGEST.fullmatch(body_checksum) is None
        or type(clusters) is not int
        or clusters <= 0
        or type(iterations) is not int
        or iterations <= 0
    ):
        raise ValueError("clustering authority differs")
    count = min(clusters, matrix.shape[0])
    seed = int.from_bytes(bytes.fromhex(body_checksum)[:8], "little")
    generator = SplitMix64(seed)
    centers = _initial_centers(matrix, count, generator)

    for _ in range(iterations):
        assignments = numpy.argmax(matrix @ centers.T, axis=1)
        assignments = _repair_empty_assignments(matrix, centers, assignments, count)
        sums = numpy.zeros((count, matrix.shape[1]), dtype=numpy.float64)
        for index, cluster in enumerate(assignments):
            sums[int(cluster)] += matrix[index]
        norms = numpy.linalg.norm(sums, axis=1)
        if not numpy.isfinite(norms).all() or numpy.any(norms <= 0.0):
            raise ValueError("cluster mean authority differs")
        centers = sums / norms[:, None]

    encoded = centers.astype("<f2")
    if not numpy.isfinite(encoded).all():
        raise ValueError("cluster f16 encoding is non-finite")
    return encoded.astype(numpy.float32)


def score_page_means(queries: numpy.ndarray, means: numpy.ndarray) -> numpy.ndarray:
    """Return one exact minimum squared-Euclidean score per query."""

    query_matrix = _validated_unit_matrix(queries, "queries").astype(numpy.float32)
    mean_matrix = numpy.asarray(means)
    if (
        mean_matrix.ndim != 2
        or mean_matrix.shape[0] == 0
        or mean_matrix.shape[1] != query_matrix.shape[1]
        or not numpy.issubdtype(mean_matrix.dtype, numpy.floating)
        or not numpy.isfinite(mean_matrix).all()
    ):
        raise ValueError("page means differ")
    distances = numpy.sum(
        (query_matrix[:, None, :] - mean_matrix.astype(numpy.float32)[None, :, :]) ** 2,
        axis=2,
        dtype=numpy.float32,
    )
    if not numpy.isfinite(distances).all():
        raise ValueError("page scores are non-finite")
    return distances.min(axis=1).astype(numpy.float32)

