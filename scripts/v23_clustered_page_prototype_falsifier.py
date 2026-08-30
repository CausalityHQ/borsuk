#!/usr/bin/env python3
"""Bounded evidence-only falsifier for clustered historical V23 posting pages."""

from __future__ import annotations

import dataclasses
import hashlib
import itertools
import json
import math
import os
import re
import resource
import struct
import sys
import time
from collections.abc import Callable, Iterator, Sequence
from concurrent.futures import Future, ThreadPoolExecutor
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import cast
from urllib.parse import urlparse

if __package__:
    from scripts.publication_v3_protocol import canonical_json_bytes
else:  # Direct ``python scripts/...`` execution.
    from publication_v3_protocol import canonical_json_bytes

for _thread_variable in (
    "OPENBLAS_NUM_THREADS",
    "OMP_NUM_THREADS",
    "MKL_NUM_THREADS",
    "NUMEXPR_NUM_THREADS",
):
    os.environ[_thread_variable] = "1"

import numpy  # noqa: E402
from blake3 import blake3  # noqa: E402

_PAGE_HEADER_BYTES = 96
_PAGE_MAX_ENCODED_BYTES = 245_760
_HEX_DIGEST = re.compile(r"[0-9a-f]{64}\Z")
_U64_MASK = (1 << 64) - 1
_RSS_LIMIT_BYTES = 768 * 1024**2
_PSI_LIMIT_PPM = 500_000
_SWAP_GROWTH_LIMIT_BYTES = 128 * 1024**2
_PROGRESS_LIMIT_NS = 300 * 1_000_000_000

_HISTORICAL_TERMINAL_FIELD_ORDER = (
    "schema_version",
        "status",
        "role",
        "attempt",
        "attempt_id",
        "instance_id",
        "source_archive_sha256",
        "manifest_sha256",
        "protocol_sha256",
        "binary_sha256",
        "purchase_option",
        "runtime_profile",
        "arm_index",
        "max_active_searches",
        "max_waiting_searches",
        "leaf_read_width",
        "max_inflight_leaf_reads",
        "max_parallel_decode_rank_tasks",
        "cpu_threads",
        "io_threads",
        "s3_get_concurrency",
        "ram_budget_bytes",
        "disk_cache_max_bytes",
        "exact_read_max_physical_amplification",
        "execution_contract_sha256",
        "artifact_upload_reconciliations",
        "claim_eligible",
        "v23_stage",
        "v23_passed",
        "v23_result_sha256",
        "v23_page_prefix",
        "v23_d2_report_sha256",
        "v23_pages_sha256",
        "v23_summary_sha256",
        "v23_d1_receipt_sha256",
        "v23_d1_report_sha256",
        "v23_prerequisite_binary_sha256",
        "base_build_terminal_sha256",
        "base_manifest_sha256",
        "base_protocol_sha256",
        "base_source_archive_sha256",
        "base_index_receipt_sha256",
        "base_object_roster_sha256",
        "base_inventory_sha256",
        "base_index_id",
        "base_index_uri",
        "diagnostic_source_archive_sha256",
        "memory_max_bytes",
        "memory_swap_max_bytes",
    "memory_peak_bytes",
)
_HISTORICAL_TERMINAL_FIELDS = frozenset(_HISTORICAL_TERMINAL_FIELD_ORDER)
_HISTORICAL_TERMINAL_DIGEST_FIELDS = frozenset(
    {
        "source_archive_sha256",
        "manifest_sha256",
        "protocol_sha256",
        "binary_sha256",
        "execution_contract_sha256",
        "v23_result_sha256",
        "v23_d2_report_sha256",
        "v23_pages_sha256",
        "v23_summary_sha256",
        "v23_d1_receipt_sha256",
        "v23_d1_report_sha256",
        "v23_prerequisite_binary_sha256",
        "base_build_terminal_sha256",
        "base_manifest_sha256",
        "base_protocol_sha256",
        "base_source_archive_sha256",
        "base_index_receipt_sha256",
        "base_object_roster_sha256",
        "base_inventory_sha256",
        "diagnostic_source_archive_sha256",
    }
)
_HISTORICAL_TERMINAL_INT_FIELDS = frozenset(
    {
        "attempt",
        "arm_index",
        "max_active_searches",
        "max_waiting_searches",
        "leaf_read_width",
        "max_inflight_leaf_reads",
        "max_parallel_decode_rank_tasks",
        "cpu_threads",
        "io_threads",
        "s3_get_concurrency",
        "ram_budget_bytes",
        "disk_cache_max_bytes",
        "exact_read_max_physical_amplification",
        "artifact_upload_reconciliations",
        "memory_max_bytes",
        "memory_swap_max_bytes",
        "memory_peak_bytes",
    }
)
_RESULT_FIELDS = frozenset(
    {
        "schema",
        "source_commit",
        "attempt_prefix",
        "terminal_sha256",
        "result_sha256",
        "report_sha256",
        "roster_sha256",
        "query_uri",
        "query_sha256",
        "page_count",
        "query_count",
        "dimensions",
        "recall_k",
        "selection_width",
        "authenticated_pages",
        "authenticated_primary_rows",
        "authenticated_replica_rows",
        "total_bytes_read",
        "algorithm",
        "query_ordinals",
        "selected_pages",
        "query_hits",
        "oracle_hits",
        "aggregate_recall_ppm",
        "minimum_query_recall_ppm",
        "oracle_attainment_ppm",
        "projected_serving_bytes",
        "elapsed_ns",
        "cpu_ns",
        "peak_rss_bytes",
        "peak_psi_full_avg10_ppm",
        "swap_delta_bytes",
        "passed",
    }
)
_ALGORITHM = {
    "name": "spherical-kmeans-page-prototypes-v1",
    "max_centers": 32,
    "lloyd_iterations": 8,
    "prng": "splitmix64",
    "input_dtype": "f32",
    "accumulator_dtype": "f64",
    "stored_dtype": "f16-le",
    "tie_breaks": "lowest-input-and-center-position",
}
_STOP_REASONS = frozenset(
    {"rss-limit", "psi-limit", "swap-growth-limit", "progress-limit"}
)

_REPORT_ARTIFACT_FIELDS = frozenset(
    {
        "schema",
        "document_kind",
        "claim_eligible",
        "stage",
        "source_archive_sha256",
        "index_id",
        "dataset_id",
        "d1_report_sha256",
        "page_uri",
        "report",
    }
)
_ROSTER_ARTIFACT_FIELDS = _REPORT_ARTIFACT_FIELDS - {"report"} | {"pages"}
_REPORT_FIELDS = frozenset(
    {"schema", "d1_report_checksum", "query_ordinals", "rows", "arms"}
)
_ARM_FIELDS = frozenset(
    {
        "d1_key",
        "selector_key",
        "selector",
        "selector_routing_cells",
        "selector_ranked_anchor_cap",
        "primary_target_rows",
        "maximum_assignments_per_row",
        "maximum_query_pages",
        "maximum_record_id_bytes",
        "pages",
        "unique_rows",
        "total_assignments",
        "storage_amplification_ppm",
        "projected_root_bytes",
        "projected_ram_bytes",
        "projected_build_bytes",
        "query_samples",
        "aggregate_recall_ppm",
        "minimum_query_recall_ppm",
        "coverage_oracle_recall_ppm",
        "coverage_oracle_minimum_query_recall_ppm",
        "selector_regret_ppm",
        "cpu_p99_ns",
        "passed",
    }
)
_PAGE_FIELDS = frozenset(
    {
        "generation_checksum",
        "page_ordinal",
        "metric",
        "dimensions",
        "family",
        "code_width",
        "path",
        "checksum",
        "encoded_bytes",
        "primary_rows",
        "replicated_rows",
    }
)
_SELECTOR_FIELDS = frozenset(
    {
        "generation_checksum",
        "metric",
        "dimensions",
        "coarse_cells",
        "page_count",
        "anchors_per_page",
        "code_width",
        "anchor_count",
        "path",
        "checksum",
        "encoded_bytes",
    }
)
_QUERY_SAMPLE_FIELDS = frozenset(
    {
        "query_index",
        "page_ordinals",
        "oracle_page_ordinals",
        "ground_truth_page_assignments",
        "encoded_bytes",
        "candidate_rows",
        "selector_candidate_anchors",
        "selector_routed_cells",
        "selector_ranked_anchors",
        "ground_truth_ids",
        "ranked",
        "gt_page_hits",
        "oracle_gt_page_hits",
        "hits",
        "recall_ppm",
        "cpu_ns",
    }
)
_HISTORICAL_RESULT_FIELDS = frozenset(
    {
        "schema",
        "document_kind",
        "claim_eligible",
        "stage",
        "source_archive_sha256",
        "index_id",
        "dataset_id",
        "d1_report_sha256",
        "rows",
        "queries",
        "arms",
        "passing_arm_indexes",
        "pages",
        "passed",
        "publishable",
        "artifact_sha256",
        "pages_sha256",
        "cell_id",
        "diagnostic_cell_id",
        "attempt_id",
        "instance_identity",
        "dataset_materialization_sha256",
        "elapsed_ns",
        "resources",
        "runtime_attestation",
    }
)
_HISTORICAL_RESOURCE_FIELDS = frozenset(
    {"cpu_ns", "peak_rss_bytes", "disk_read_bytes", "disk_write_bytes"}
)
_HISTORICAL_ATTESTATION_FIELDS = frozenset(
    {
        "schema_version",
        "cell_id",
        "attempt_id",
        "instance_id",
        "instance_type",
        "purchase_option",
        "architecture",
        "vcpus",
        "memory_max_bytes",
        "memory_peak_bytes",
        "swap_max_bytes",
        "swap_current_bytes",
        "swap_peak_bytes",
        "oom_events",
        "oom_kill_events",
        "cache_capacity_bytes",
        "effective_disk_cache_max_bytes",
        "cache_filesystem_bytes",
        "cache_device",
        "root_device",
        "cache_is_mount",
        "source_revision",
    }
)


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


@dataclasses.dataclass(frozen=True, slots=True)
class ScientificShape:
    """Registered scientific cardinalities, injectable only for pure tests."""

    page_count: int
    query_count: int
    dimensions: int
    recall_k: int
    selection_width: int


@dataclasses.dataclass(frozen=True, slots=True)
class RegisteredAuthority:
    """Precommitted immutable identities for the historical evidence bundle."""

    source_commit: str
    attempt_prefix: str
    terminal_sha256: str
    result_sha256: str
    report_sha256: str
    roster_sha256: str
    query_uri: str
    query_sha256: str
    dataset_materialization_sha256: str
    base_cell_id: str
    diagnostic_cell_id: str


@dataclasses.dataclass(frozen=True, slots=True)
class Revision4Bvs2Paths:
    """The five exact local prerequisites for the historical adapter."""

    terminal: Path
    result: Path
    report: Path
    roster: Path
    query: Path


@dataclasses.dataclass(frozen=True, slots=True)
class Authority:
    """Validated scientific inputs retained by the bounded stream."""

    registered: RegisteredAuthority
    shape: ScientificShape
    pages: tuple[PageRef, ...]
    queries: numpy.ndarray
    query_ordinals: tuple[int, ...]
    ground_truth_page_assignments: tuple[tuple[tuple[int, ...], ...], ...]
    oracle_hits: tuple[int, ...]


@dataclasses.dataclass(frozen=True, slots=True)
class PressureSample:
    """One concrete cgroup/process pressure observation."""

    rss_bytes: int
    psi_full_avg10_ppm: int
    swap_bytes: int
    monotonic_ns: int


class StreamStopped(RuntimeError):
    """Fail-closed stop carrying only the last authenticated page identity."""

    def __init__(
        self,
        reason: str,
        last_authenticated_page: int,
        last_authenticated_checksum: str | None,
    ) -> None:
        super().__init__(reason)
        self.reason = reason
        self.last_authenticated_page = last_authenticated_page
        self.last_authenticated_checksum = last_authenticated_checksum


REGISTERED_SHAPE = ScientificShape(
    page_count=28_282,
    query_count=32,
    dimensions=96,
    recall_k=10,
    selection_width=8,
)
REGISTERED_AUTHORITY = RegisteredAuthority(
    source_commit="c59128ee68eb28beaa7f5eef7e0570dc7c787b88",
    attempt_prefix=(
        "s3://borsuk-bench-453182569524-euc1/publication/v3/20260812/results/"
        "r01-f7a6e06a6a40c1165b6cb889/runtime-v23-d2/arms/0000/attempts/0001/"
    ),
    terminal_sha256="db12dd670ae5121fa4d90147fba7816d6a20878764a28d089be45be1138579ef",
    result_sha256="41ec2b4eb9e0506f4732c2e0ff34d92e1493b24953669c486fc5714a38002a00",
    report_sha256="665dc206d04073b8cbc0b8bab9e5645760440d2336ddf4bfebea81d176b4779d",
    roster_sha256="dfa5759c06663655b4a963a7687b40c8bd8020bebf805d7c825a88c6d0df53e1",
    query_uri=(
        "s3://borsuk-bench-453182569524-euc1/publication/v3/20260812/datasets/"
        "deep-image-96/attempts/0001/materialized/test.parquet"
    ),
    query_sha256="296d45828020c1c0b88c6a1d5c822f6283280513b8c58d01cfa961f3a139a5d4",
    dataset_materialization_sha256=(
        "02eec37494dce759433fef0983c871856b4bf445b4c93e35abceb430b7109408"
    ),
    base_cell_id="r01-46d286fd1e2290c1cb8b8645",
    diagnostic_cell_id="r01-f7a6e06a6a40c1165b6cb889",
)


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
        or primary_rows > 65_535
        or reference.replicated_rows > 65_535
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


def _exact_dict(value: object, fields: frozenset[str], role: str) -> dict[str, object]:
    if type(value) is not dict or frozenset(value) != fields:
        raise ValueError(f"{role} fields differ")
    return value


def _same_concrete(left: object, right: object) -> bool:
    if type(left) is not type(right):
        return False
    if type(left) is dict:
        left_dict = cast(dict[object, object], left)
        right_dict = cast(dict[object, object], right)
        return left_dict.keys() == right_dict.keys() and all(
            _same_concrete(left_dict[key], right_dict[key]) for key in left_dict
        )
    if type(left) is list:
        left_list = cast(list[object], left)
        right_list = cast(list[object], right)
        return len(left_list) == len(right_list) and all(
            _same_concrete(a, b) for a, b in zip(left_list, right_list, strict=True)
        )
    return left == right


def _digest_is_valid(value: object, *, length: int = 64) -> bool:
    return (
        type(value) is str
        and len(value) == length
        and all(character in "0123456789abcdef" for character in value)
    )


def _validate_shape(shape: ScientificShape) -> None:
    if type(shape) is not ScientificShape:
        raise ValueError("scientific shape has the wrong concrete type")
    for field in dataclasses.fields(shape):
        _concrete_nonnegative_int(getattr(shape, field.name), field.name, positive=True)
    if shape.selection_width > shape.page_count:
        raise ValueError("selection width exceeds page count")


def _validate_registered(registered: RegisteredAuthority) -> None:
    if type(registered) is not RegisteredAuthority:
        raise ValueError("registered authority has the wrong concrete type")
    if (
        not _digest_is_valid(registered.source_commit, length=40)
        or type(registered.attempt_prefix) is not str
        or not registered.attempt_prefix.startswith("s3://")
        or not registered.attempt_prefix.endswith("/")
        or type(registered.query_uri) is not str
        or not registered.query_uri.startswith("s3://")
        or type(registered.base_cell_id) is not str
        or not registered.base_cell_id
        or type(registered.diagnostic_cell_id) is not str
        or not registered.diagnostic_cell_id
        or registered.base_cell_id == registered.diagnostic_cell_id
        or any(
            not _digest_is_valid(value)
            for value in (
                registered.terminal_sha256,
                registered.result_sha256,
                registered.report_sha256,
                registered.roster_sha256,
                registered.query_sha256,
                registered.dataset_materialization_sha256,
            )
        )
    ):
        raise ValueError("registered authority differs")


def _read_canonical_json(path: Path, expected_sha256: str, role: str) -> dict[str, object]:
    if type(path) is not Path:
        path = Path(path)
    payload = path.read_bytes()
    if hashlib.sha256(payload).hexdigest() != expected_sha256:
        raise ValueError(f"{role} SHA-256 differs")
    try:
        value = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError(f"{role} is not canonical JSON") from error
    if type(value) is not dict or payload != canonical_json_bytes(value) + b"\n":
        raise ValueError(f"{role} bytes are not canonical")
    return value


def _read_historical_runtime_terminal(
    path: Path, registered: RegisteredAuthority
) -> dict[str, object]:
    """Authenticate the shell-writer's compact insertion-order terminal."""

    if type(path) is not Path:
        path = Path(path)
    payload = path.read_bytes()
    if hashlib.sha256(payload).hexdigest() != registered.terminal_sha256:
        raise ValueError("terminal marker SHA-256 differs")
    try:
        value = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("terminal marker is not JSON") from error
    expected_payload = json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8") + b"\n"
    terminal = _exact_dict(value, _HISTORICAL_TERMINAL_FIELDS, "terminal marker")
    if (
        payload != expected_payload
        or tuple(terminal) != _HISTORICAL_TERMINAL_FIELD_ORDER
    ):
        raise ValueError("terminal marker bytes are not canonical")
    if any(not _digest_is_valid(terminal[field]) for field in _HISTORICAL_TERMINAL_DIGEST_FIELDS):
        raise ValueError("terminal marker digest differs")
    for field in _HISTORICAL_TERMINAL_INT_FIELDS:
        _concrete_nonnegative_int(terminal[field], f"terminal marker {field}")
    fixed = {
        "schema_version": 5,
        "status": "complete",
        "role": "runtime",
        "attempt": 1,
        "attempt_id": "runtime-v23-d2-r01-f7a6e06a6a40c1165b6cb889-arm-0000-a0001",
        "purchase_option": "spot",
        "runtime_profile": "recall",
        "arm_index": 0,
        "max_active_searches": 4,
        "max_waiting_searches": 16,
        "leaf_read_width": 32,
        "max_inflight_leaf_reads": 48,
        "max_parallel_decode_rank_tasks": 2,
        "cpu_threads": 3,
        "io_threads": 88,
        "s3_get_concurrency": 64,
        "ram_budget_bytes": 3 * 1024**3,
        "disk_cache_max_bytes": 0,
        "exact_read_max_physical_amplification": 2,
        "artifact_upload_reconciliations": 0,
        "claim_eligible": False,
        "v23_stage": "d2",
        "v23_passed": False,
        "memory_max_bytes": 32 * 1024**3,
        "memory_swap_max_bytes": 0,
    }
    if any(type(terminal[field]) is not type(expected) or terminal[field] != expected for field, expected in fixed.items()):
        raise ValueError("terminal marker frozen authority differs")
    if (
        type(terminal["instance_id"]) is not str
        or re.fullmatch(r"i-[0-9a-f]+", terminal["instance_id"]) is None
        or type(terminal["base_index_id"]) is not str
        or not terminal["base_index_id"]
        or type(terminal["base_index_uri"]) is not str
        or not terminal["base_index_uri"].startswith("s3://")
        or terminal["v23_result_sha256"] != registered.result_sha256
        or terminal["v23_d2_report_sha256"] != registered.report_sha256
        or terminal["v23_pages_sha256"] != registered.roster_sha256
        or terminal["v23_page_prefix"] != f"{registered.attempt_prefix}pages"
        or terminal["v23_prerequisite_binary_sha256"] != terminal["binary_sha256"]
        or terminal["diagnostic_source_archive_sha256"]
        != terminal["source_archive_sha256"]
        or terminal["memory_peak_bytes"] <= 0
        or terminal["memory_peak_bytes"] > terminal["memory_max_bytes"]
    ):
        raise ValueError("terminal marker authority differs")
    return terminal


def _page_from_json(value: object) -> PageRef:
    item = _exact_dict(value, _PAGE_FIELDS, "page reference")
    generation = item["generation_checksum"]
    if (
        type(generation) is not list
        or len(generation) != 32
        or any(type(byte) is not int or not 0 <= byte <= 255 for byte in generation)
    ):
        raise ValueError("page generation checksum differs")
    reference = PageRef(
        generation_checksum=bytes(generation),
        page_ordinal=item["page_ordinal"],
        metric=item["metric"],
        dimensions=item["dimensions"],
        family=item["family"],
        code_width=item["code_width"],
        path=item["path"],
        checksum=item["checksum"],
        encoded_bytes=item["encoded_bytes"],
        primary_rows=item["primary_rows"],
        replicated_rows=item["replicated_rows"],
    )
    _validate_page_reference(reference)
    return reference


def _concrete_int_list(
    value: object,
    role: str,
    *,
    exact_length: int | None = None,
    upper_bound: int | None = None,
) -> tuple[int, ...]:
    if type(value) is not list or (
        exact_length is not None and len(value) != exact_length
    ):
        raise ValueError(f"{role} shape differs")
    result = []
    for item in value:
        concrete = _concrete_nonnegative_int(item, role)
        if upper_bound is not None and concrete >= upper_bound:
            raise ValueError(f"{role} is out of range")
        result.append(concrete)
    return tuple(result)


def _validate_outer_artifacts(
    report_artifact: dict[str, object],
    roster_artifact: dict[str, object],
    registered: RegisteredAuthority,
) -> None:
    _exact_dict(report_artifact, _REPORT_ARTIFACT_FIELDS, "D2 report artifact")
    _exact_dict(roster_artifact, _ROSTER_ARTIFACT_FIELDS, "D2 roster artifact")
    expected = {
        "claim_eligible": False,
        "stage": "d2",
        "dataset_id": "deep-image-96",
        "page_uri": registered.attempt_prefix + "pages",
    }
    if (
        report_artifact["schema"] != "borsuk-v23-d2-artifact-v1"
        or report_artifact["document_kind"] != "publication-v3-v23-d2-report"
        or roster_artifact["schema"] != "borsuk-v23-pages-v1"
        or roster_artifact["document_kind"] != "publication-v3-v23-page-roster"
        or any(
            not _same_concrete(report_artifact[key], value)
            for key, value in expected.items()
        )
        or any(
            not _same_concrete(roster_artifact[key], value)
            for key, value in expected.items()
        )
        or any(
            not _same_concrete(report_artifact[key], roster_artifact[key])
            for key in (
                "source_archive_sha256",
                "index_id",
                "dataset_id",
                "d1_report_sha256",
                "page_uri",
            )
        )
        or not _digest_is_valid(report_artifact["source_archive_sha256"])
        or not _digest_is_valid(report_artifact["d1_report_sha256"])
        or type(report_artifact["index_id"]) is not str
        or not report_artifact["index_id"]
    ):
        raise ValueError("D2 artifact authority differs")


def _projected_arm_memory(
    unique_rows: int,
    page_count: int,
    dimensions: int,
    coarse_cells: int,
    anchors_per_page: int,
) -> tuple[int, int]:
    projected_pages = max(
        (page_count * 100_000_000 + unique_rows - 1) // unique_rows,
        page_count,
    )
    projected_root = 96 + projected_pages * 320
    projected_coarse = max(coarse_cells, 4_096)
    centroid_bytes = projected_coarse * dimensions * 4
    offset_bytes = (projected_coarse + 1) * 4
    selector_bytes = (
        96
        + centroid_bytes
        + offset_bytes
        + projected_pages * anchors_per_page * (12 + 192)
    )
    projected_ram = (
        projected_root
        + selector_bytes
        + centroid_bytes
        + offset_bytes
        + 512 * 1024**2
        + 2 * 1_966_080
    )
    return projected_root, projected_ram


def _projected_build_memory(
    rows: int,
    pages: tuple[PageRef, ...],
    dimensions: int,
    maximum_record_id_bytes: int,
    code_width: int,
    maximum_assignments_per_row: int,
) -> int:
    decoded_row_bytes = 64 + maximum_record_id_bytes + dimensions * 4 + code_width
    replica_candidate_bytes = 32 * min(
        max(maximum_assignments_per_row - 1, 0),
        1,
    )
    planner_index_bytes = 8 * 7
    decoded_and_planner = rows * (
        decoded_row_bytes + 192 + 32 + replica_candidate_bytes + planner_index_bytes
    )
    page_authority = len(pages) * (dimensions * 4 + 4_096 + 512)
    lightweight_evidence = 32 * (4_096 + maximum_record_id_bytes * 20)
    return (
        decoded_and_planner
        + sum(page.encoded_bytes for page in pages)
        + 4 * page_authority
        + lightweight_evidence
        + 2 * 1_966_080
    )


def _validate_arm_shape(
    arm: dict[str, object], shape: ScientificShape, pages: tuple[PageRef, ...]
) -> None:
    _exact_dict(arm, _ARM_FIELDS, "D2 arm")
    for key in (
        "selector_routing_cells",
        "selector_ranked_anchor_cap",
        "primary_target_rows",
        "maximum_assignments_per_row",
        "maximum_query_pages",
        "maximum_record_id_bytes",
        "unique_rows",
        "total_assignments",
        "storage_amplification_ppm",
        "projected_root_bytes",
        "projected_ram_bytes",
        "projected_build_bytes",
        "aggregate_recall_ppm",
        "minimum_query_recall_ppm",
        "coverage_oracle_recall_ppm",
        "coverage_oracle_minimum_query_recall_ppm",
        "selector_regret_ppm",
        "cpu_p99_ns",
    ):
        _concrete_nonnegative_int(arm[key], key)
    expected_key = {"family": "f16-flat", "code_width_bytes": shape.dimensions * 2}
    if (
        arm["maximum_query_pages"] != shape.selection_width
        or type(arm["passed"]) is not bool
        or type(arm["d1_key"]) is not dict
        or not _same_concrete(arm["d1_key"], expected_key)
        or type(arm["selector_key"]) is not dict
        or not _same_concrete(arm["selector_key"], expected_key)
        or type(arm["selector"]) is not dict
    ):
        raise ValueError("D2 arm authority differs")
    selector = _exact_dict(arm["selector"], _SELECTOR_FIELDS, "D2 selector")
    generation = selector["generation_checksum"]
    for role in (
        "dimensions",
        "coarse_cells",
        "page_count",
        "anchors_per_page",
        "code_width",
        "anchor_count",
        "encoded_bytes",
    ):
        _concrete_nonnegative_int(selector[role], f"selector {role}", positive=True)
    if (
        type(generation) is not list
        or len(generation) != 32
        or any(type(byte) is not int or not 0 <= byte <= 255 for byte in generation)
        or generation == [0] * 32
        or selector["metric"] != "cosine"
        or type(selector["metric"]) is not str
        or selector["dimensions"] != shape.dimensions
        or selector["page_count"] != shape.page_count
        or selector["code_width"] != shape.dimensions * 2
        or selector["anchors_per_page"] != 16
        or not selector["page_count"]
        <= selector["anchor_count"]
        <= selector["page_count"] * 16
        or selector["encoded_bytes"]
        != 96
        + selector["coarse_cells"] * shape.dimensions * 4
        + (selector["coarse_cells"] + 1) * 4
        + selector["anchor_count"] * (12 + shape.dimensions * 2)
        or not _digest_is_valid(selector["checksum"])
        or type(selector["path"]) is not str
        or selector["path"] != f"selectors/{selector['checksum']}"
    ):
        raise ValueError("D2 selector authority differs")
    primary_rows = sum(page.primary_rows for page in pages)
    total_assignments = sum(page.primary_rows + page.replicated_rows for page in pages)
    projected_root, projected_ram = _projected_arm_memory(
        arm["unique_rows"],
        len(pages),
        shape.dimensions,
        selector["coarse_cells"],
        selector["anchors_per_page"],
    )
    projected_build = _projected_build_memory(
        arm["unique_rows"],
        pages,
        shape.dimensions,
        arm["maximum_record_id_bytes"],
        arm["d1_key"]["code_width_bytes"],
        arm["maximum_assignments_per_row"],
    )
    if (
        arm["selector_routing_cells"] != min(320, selector["coarse_cells"])
        or arm["selector_ranked_anchor_cap"] != 8_192
        or arm["primary_target_rows"] != 384
        or arm["maximum_assignments_per_row"] != 2
        or arm["maximum_query_pages"] != shape.selection_width
        or arm["maximum_record_id_bytes"] <= 0
        or arm["unique_rows"] != primary_rows
        or arm["total_assignments"] != total_assignments
        or arm["storage_amplification_ppm"]
        != total_assignments * 1_000_000 // primary_rows
        or arm["projected_root_bytes"] != projected_root
        or arm["projected_ram_bytes"] != projected_ram
        or arm["projected_build_bytes"] != projected_build
    ):
        raise ValueError("D2 arm derived authority differs")


def _identifier(value: object, role: str, maximum_bytes: int) -> tuple[int, ...]:
    result = _concrete_int_list(value, role)
    if (
        not result
        or len(result) > maximum_bytes
        or any(byte > 255 for byte in result)
    ):
        raise ValueError(f"{role} differs")
    return result


def _coverage_oracle(
    assignments: tuple[tuple[int, ...], ...], width: int
) -> tuple[tuple[int, ...], int]:
    candidates = tuple(sorted({page for pages in assignments for page in pages}))
    best_pages: tuple[int, ...] = ()
    best_hits = -1
    for size in range(1, min(width, len(candidates)) + 1):
        for pages in itertools.combinations(candidates, size):
            hits = sum(
                bool(set(assignment).intersection(pages))
                for assignment in assignments
            )
            if hits > best_hits or (hits == best_hits and pages < best_pages):
                best_pages = pages
                best_hits = hits
    return best_pages, best_hits


def _query_evidence(
    arm: dict[str, object],
    shape: ScientificShape,
    pages: tuple[PageRef, ...],
) -> tuple[
    tuple[tuple[tuple[int, ...], ...], ...],
    tuple[int, ...],
]:
    samples = arm["query_samples"]
    if type(samples) is not list or len(samples) != shape.query_count:
        raise ValueError("query sample shape differs")
    all_assignments: list[tuple[tuple[int, ...], ...]] = []
    oracle_hits: list[int] = []
    total_hits = 0
    total_page_hits = 0
    total_oracle_hits = 0
    minimum_recall = 1_000_000
    minimum_oracle_recall = 1_000_000
    cpu_values: list[int] = []
    for expected_query, raw_sample in enumerate(samples):
        sample = _exact_dict(raw_sample, _QUERY_SAMPLE_FIELDS, "D2 query sample")
        if (
            type(sample["query_index"]) is not int
            or sample["query_index"] != expected_query
            or type(sample["ranked"]) is not dict
            or frozenset(sample["ranked"]) != {"ids", "distances"}
            or type(sample["ground_truth_ids"]) is not list
            or len(sample["ground_truth_ids"]) != shape.recall_k
        ):
            raise ValueError("query sample authority differs")
        for key in (
            "encoded_bytes",
            "candidate_rows",
            "selector_candidate_anchors",
            "selector_routed_cells",
            "selector_ranked_anchors",
            "gt_page_hits",
            "oracle_gt_page_hits",
            "hits",
            "recall_ppm",
            "cpu_ns",
        ):
            _concrete_nonnegative_int(sample[key], key)
        selected = _concrete_int_list(
            sample["page_ordinals"],
            "selected page ordinal",
            exact_length=shape.selection_width,
            upper_bound=shape.page_count,
        )
        oracle = _concrete_int_list(
            sample["oracle_page_ordinals"],
            "oracle page ordinal",
            upper_bound=shape.page_count,
        )
        if (
            selected != tuple(sorted(set(selected)))
            or not oracle
            or len(oracle) > shape.selection_width
            or oracle != tuple(sorted(set(oracle)))
        ):
            raise ValueError("query page ordering differs")
        raw_assignments = sample["ground_truth_page_assignments"]
        if type(raw_assignments) is not list or len(raw_assignments) != shape.recall_k:
            raise ValueError("ground-truth assignment shape differs")
        assignments = tuple(
            _concrete_int_list(
                pages,
                "ground-truth page assignment",
                upper_bound=shape.page_count,
            )
            for pages in raw_assignments
        )
        if any(
            not assigned
            or len(assigned) > arm["maximum_assignments_per_row"]
            or assigned != tuple(sorted(set(assigned)))
            for assigned in assignments
        ):
            raise ValueError("ground-truth page assignments differ")
        truth = tuple(
            _identifier(
                value,
                "ground-truth identifier",
                arm["maximum_record_id_bytes"],
            )
            for value in sample["ground_truth_ids"]
        )
        ranked = _exact_dict(
            sample["ranked"],
            frozenset({"ids", "distances"}),
            "ranked result",
        )
        if (
            type(ranked["ids"]) is not list
            or type(ranked["distances"]) is not list
            or not 1 <= len(ranked["ids"]) <= shape.recall_k
            or len(ranked["ids"]) != len(ranked["distances"])
        ):
            raise ValueError("ranked result shape differs")
        ranked_ids = tuple(
            _identifier(value, "ranked identifier", arm["maximum_record_id_bytes"])
            for value in ranked["ids"]
        )
        distances = tuple(ranked["distances"])
        if (
            len(set(truth)) != shape.recall_k
            or len(set(ranked_ids)) != len(ranked_ids)
            or any(
                type(distance) is not float or not math.isfinite(distance)
                for distance in distances
            )
            or tuple(zip(distances, ranked_ids, strict=True))
            != tuple(sorted(zip(distances, ranked_ids, strict=True)))
        ):
            raise ValueError("ranked result authority differs")
        selected_hits = sum(
            bool(set(assigned).intersection(selected)) for assigned in assignments
        )
        recomputed_oracle, recomputed_oracle_hits = _coverage_oracle(
            assignments, arm["maximum_query_pages"]
        )
        expected_hits = sum(identifier in set(truth) for identifier in ranked_ids)
        expected_bytes = sum(pages[page].encoded_bytes for page in selected)
        expected_candidates = sum(
            pages[page].primary_rows + pages[page].replicated_rows for page in selected
        )
        expected_recall = expected_hits * 1_000_000 // shape.recall_k
        if (
            oracle != recomputed_oracle
            or sample["gt_page_hits"] != selected_hits
            or sample["oracle_gt_page_hits"] != recomputed_oracle_hits
            or sample["encoded_bytes"] != expected_bytes
            or sample["encoded_bytes"] > 1_966_080
            or sample["candidate_rows"] != expected_candidates
            or sample["selector_candidate_anchors"] <= 0
            or sample["selector_routed_cells"] != arm["selector_routing_cells"]
            or sample["selector_ranked_anchors"]
            != min(
                sample["selector_candidate_anchors"],
                arm["selector_ranked_anchor_cap"],
            )
            or sample["gt_page_hits"] > shape.recall_k
            or sample["oracle_gt_page_hits"] > shape.recall_k
            or sample["oracle_gt_page_hits"] < sample["gt_page_hits"]
            or sample["gt_page_hits"] < sample["hits"]
            or sample["hits"] != expected_hits
            or sample["recall_ppm"] != expected_recall
            or sample["cpu_ns"] <= 0
        ):
            raise ValueError("query evidence differs")
        all_assignments.append(assignments)
        oracle_hits.append(recomputed_oracle_hits)
        total_hits += expected_hits
        total_page_hits += selected_hits
        total_oracle_hits += recomputed_oracle_hits
        minimum_recall = min(minimum_recall, expected_recall)
        minimum_oracle_recall = min(
            minimum_oracle_recall,
            recomputed_oracle_hits * 1_000_000 // shape.recall_k,
        )
        cpu_values.append(sample["cpu_ns"])
    aggregate = total_hits * 1_000_000 // (shape.query_count * shape.recall_k)
    oracle_aggregate = total_oracle_hits * 1_000_000 // (
        shape.query_count * shape.recall_k
    )
    regret = total_page_hits * 1_000_000 // max(total_oracle_hits, 1)
    cpu_p99 = sorted(cpu_values)[math.ceil(len(cpu_values) * 0.99) - 1]
    passed = (
        aggregate >= 975_000
        and minimum_recall >= 800_000
        and oracle_aggregate >= 985_000
        and minimum_oracle_recall >= 900_000
        and regret >= 995_000
        and arm["storage_amplification_ppm"] <= 2_000_000
        and arm["projected_ram_bytes"] <= 3 * 1024**3
        and cpu_p99 <= 15_000_000
    )
    if (
        arm["aggregate_recall_ppm"] != aggregate
        or arm["minimum_query_recall_ppm"] != minimum_recall
        or arm["coverage_oracle_recall_ppm"] != oracle_aggregate
        or arm["coverage_oracle_minimum_query_recall_ppm"] != minimum_oracle_recall
        or arm["selector_regret_ppm"] != regret
        or arm["cpu_p99_ns"] != cpu_p99
        or arm["passed"] is not passed
    ):
        raise ValueError("D2 aggregate authority differs")
    return tuple(all_assignments), tuple(oracle_hits)


def _load_query_vectors(
    query_path: Path,
    expected_sha256: str,
    expected_uri: str,
    query_ordinals: tuple[int, ...],
    shape: ScientificShape,
) -> numpy.ndarray:
    payload = Path(query_path).read_bytes()
    if hashlib.sha256(payload).hexdigest() != expected_sha256:
        raise ValueError("query object SHA-256 differs")
    import pyarrow as pa
    import pyarrow.parquet as pq

    parquet = pq.ParquetFile(query_path)
    table = parquet.read()
    if (
        urlparse(expected_uri).path.rsplit("/", 1)[-1] != Path(query_path).name
        or parquet.metadata.num_rows != 10_000
        or table.num_rows != 10_000
        or table.schema.names != ["emb"]
    ):
        raise ValueError("query Parquet fields differ")
    field = table.schema.field("emb")
    if (
        field.nullable
        or not pa.types.is_fixed_size_list(field.type)
        or field.type.list_size != shape.dimensions
        or not pa.types.is_float32(field.type.value_type)
        or field.type.value_field.nullable
        or not query_ordinals
        or query_ordinals[-1] >= table.num_rows
    ):
        raise ValueError("query Parquet authority differs")
    vectors = numpy.asarray(table.column("emb").to_pylist(), dtype=numpy.float32)
    selected = vectors[numpy.asarray(query_ordinals, dtype=numpy.intp)]
    norms = numpy.linalg.norm(selected.astype(numpy.float64), axis=1)
    if (
        selected.shape != (shape.query_count, shape.dimensions)
        or not numpy.isfinite(selected).all()
        or not numpy.isfinite(norms).all()
        or numpy.any(norms <= 0.0)
    ):
        raise ValueError("query vectors differ")
    return (selected.astype(numpy.float64) / norms[:, None]).astype(numpy.float32)


def _validate_result_receipt(
    result: dict[str, object],
    terminal: dict[str, object],
    report_artifact: dict[str, object],
    roster_artifact: dict[str, object],
    arm: dict[str, object],
    registered: RegisteredAuthority,
    shape: ScientificShape,
) -> None:
    _exact_dict(result, _HISTORICAL_RESULT_FIELDS, "result receipt")
    summary = {
        "schema": "borsuk-v23-summary-v1",
        "document_kind": "publication-v3-v23-d2-summary",
        "claim_eligible": False,
        "stage": "d2",
        "source_archive_sha256": report_artifact["source_archive_sha256"],
        "index_id": report_artifact["index_id"],
        "dataset_id": "deep-image-96",
        "d1_report_sha256": report_artifact["d1_report_sha256"],
        "rows": report_artifact["report"]["rows"],
        "queries": shape.query_count,
        "arms": 1,
        "passing_arm_indexes": [0] if arm["passed"] else [],
        "pages": shape.page_count,
        "passed": arm["passed"],
    }
    if any(
        not _same_concrete(result[key], expected)
        for key, expected in summary.items()
    ):
        raise ValueError("result summary differs")
    diagnostic_cell = result["diagnostic_cell_id"]
    attempt_id = result["attempt_id"]
    if (
        result["publishable"] is not False
        or result["artifact_sha256"] != registered.report_sha256
        or result["pages_sha256"] != registered.roster_sha256
        or result["cell_id"] != registered.base_cell_id
        or type(diagnostic_cell) is not str
        or diagnostic_cell != registered.diagnostic_cell_id
        or type(attempt_id) is not str
        or attempt_id != f"runtime-v23-d2-{diagnostic_cell}-arm-0000-a0001"
        or attempt_id != terminal["attempt_id"]
        or result["instance_identity"] != terminal["instance_id"]
        or result["dataset_materialization_sha256"]
        != registered.dataset_materialization_sha256
        or result["source_archive_sha256"] != terminal["source_archive_sha256"]
        or result["index_id"] != terminal["base_index_id"]
        or result["d1_report_sha256"] != terminal["v23_d1_report_sha256"]
    ):
        raise ValueError("result binding differs")
    _concrete_nonnegative_int(result["elapsed_ns"], "result elapsed", positive=True)
    resources = _exact_dict(
        result["resources"],
        _HISTORICAL_RESOURCE_FIELDS,
        "result resources",
    )
    for key in _HISTORICAL_RESOURCE_FIELDS:
        _concrete_nonnegative_int(resources[key], f"result resource {key}")
    attestation = _exact_dict(
        result["runtime_attestation"],
        _HISTORICAL_ATTESTATION_FIELDS,
        "runtime attestation",
    )
    integer_fields = (
        "schema_version",
        "vcpus",
        "memory_max_bytes",
        "memory_peak_bytes",
        "swap_max_bytes",
        "swap_current_bytes",
        "swap_peak_bytes",
        "oom_events",
        "oom_kill_events",
        "cache_capacity_bytes",
        "effective_disk_cache_max_bytes",
        "cache_filesystem_bytes",
    )
    for key in integer_fields:
        _concrete_nonnegative_int(attestation[key], f"runtime attestation {key}")
    if (
        attestation["schema_version"] != 2
        or attestation["cell_id"] != diagnostic_cell
        or attestation["attempt_id"] != attempt_id
        or attestation["instance_id"] != result["instance_identity"]
        or attestation["purchase_option"] != terminal["purchase_option"]
        or attestation["memory_max_bytes"] != terminal["memory_max_bytes"]
        or attestation["memory_peak_bytes"] != terminal["memory_peak_bytes"]
        or not 0 < resources["peak_rss_bytes"] <= attestation["memory_peak_bytes"]
        or attestation["swap_max_bytes"] != terminal["memory_swap_max_bytes"]
        or any(
            attestation[key] != 0
            for key in (
                "swap_current_bytes",
                "swap_peak_bytes",
                "oom_events",
                "oom_kill_events",
            )
        )
        or attestation["effective_disk_cache_max_bytes"]
        != terminal["disk_cache_max_bytes"]
        or attestation["source_revision"] != registered.source_commit
        or type(attestation["instance_type"]) is not str
        or not attestation["instance_type"]
        or type(attestation["architecture"]) is not str
        or not attestation["architecture"]
        or type(attestation["cache_device"]) is not str
        or type(attestation["root_device"]) is not str
        or type(attestation["cache_is_mount"]) is not bool
    ):
        raise ValueError("runtime attestation differs")


@dataclasses.dataclass(frozen=True, slots=True)
class Revision4Bvs2Authority:
    """Evidence-only adapter for the frozen Revision-4 BVS2 writer contract."""

    registered: RegisteredAuthority
    shape: ScientificShape

    def _read_terminal(self, path: Path) -> dict[str, object]:
        return _read_historical_runtime_terminal(path, self.registered)

    def _read_sorted_json(
        self, path: Path, expected_sha256: str, role: str
    ) -> dict[str, object]:
        return _read_canonical_json(path, expected_sha256, role)

    def _validate_bundle(
        self,
        terminal: dict[str, object],
        result: dict[str, object],
        report_artifact: dict[str, object],
        roster_artifact: dict[str, object],
    ) -> tuple[dict[str, object], tuple[PageRef, ...], tuple[int, ...]]:
        _validate_outer_artifacts(report_artifact, roster_artifact, self.registered)
        report = _exact_dict(report_artifact["report"], _REPORT_FIELDS, "D2 report")
        if (
            report["schema"] != "borsuk-v23-d2-v8"
            or not _digest_is_valid(report["d1_report_checksum"])
            or type(report["rows"]) is not int
            or report["rows"] <= 0
            or type(report["arms"]) is not list
            or len(report["arms"]) != 1
            or terminal["source_archive_sha256"]
            != report_artifact["source_archive_sha256"]
            or terminal["base_index_id"] != report_artifact["index_id"]
            or terminal["v23_d1_report_sha256"]
            != report_artifact["d1_report_sha256"]
            or terminal["v23_passed"] is not report["arms"][0]["passed"]
        ):
            raise ValueError("D2 report authority differs")
        query_ordinals = _concrete_int_list(
            report["query_ordinals"],
            "query ordinal",
            exact_length=self.shape.query_count,
        )
        if query_ordinals != tuple(sorted(set(query_ordinals))):
            raise ValueError("query ordinals differ")
        arm = _exact_dict(report["arms"][0], _ARM_FIELDS, "D2 arm")
        raw_pages = roster_artifact["pages"]
        raw_arm_pages = arm["pages"]
        if (
            type(raw_pages) is not list
            or len(raw_pages) != self.shape.page_count
            or type(raw_arm_pages) is not list
            or len(raw_arm_pages) != self.shape.page_count
        ):
            raise ValueError("page roster shape differs")
        pages = tuple(_page_from_json(page) for page in raw_pages)
        arm_pages = tuple(_page_from_json(page) for page in raw_arm_pages)
        selector_generation = bytes(arm["selector"]["generation_checksum"])
        if (
            pages != arm_pages
            or tuple(page.page_ordinal for page in pages)
            != tuple(range(self.shape.page_count))
            or len({page.path for page in pages}) != self.shape.page_count
            or len({page.checksum for page in pages}) != self.shape.page_count
            or any(page.dimensions != self.shape.dimensions for page in pages)
            or any(page.generation_checksum != selector_generation for page in pages)
        ):
            raise ValueError("page roster authority differs")
        _validate_arm_shape(arm, self.shape, pages)
        _validate_result_receipt(
            result,
            terminal,
            report_artifact,
            roster_artifact,
            arm,
            self.registered,
            self.shape,
        )
        return arm, pages, query_ordinals

    def _validate_arm_evidence(
        self, arm: dict[str, object], pages: tuple[PageRef, ...]
    ) -> tuple[tuple[tuple[tuple[int, ...], ...], ...], tuple[int, ...]]:
        return _query_evidence(arm, self.shape, pages)

    def _read_query(
        self,
        path: Path,
        result: dict[str, object],
        query_ordinals: tuple[int, ...],
    ) -> numpy.ndarray:
        if (
            result["dataset_materialization_sha256"]
            != self.registered.dataset_materialization_sha256
        ):
            raise ValueError("query materialization differs")
        return _load_query_vectors(
            path,
            self.registered.query_sha256,
            self.registered.query_uri,
            query_ordinals,
            self.shape,
        )

    def load(self, paths: Revision4Bvs2Paths) -> Authority:
        _validate_registered(self.registered)
        _validate_shape(self.shape)
        if type(paths) is not Revision4Bvs2Paths:
            raise ValueError("Revision-4 BVS2 paths have the wrong concrete type")
        terminal = self._read_terminal(paths.terminal)
        result = self._read_sorted_json(
            paths.result,
            self.registered.result_sha256,
            "result receipt",
        )
        report = self._read_sorted_json(
            paths.report,
            self.registered.report_sha256,
            "D2 report",
        )
        roster = self._read_sorted_json(
            paths.roster,
            self.registered.roster_sha256,
            "D2 page roster",
        )
        arm, pages, query_ordinals = self._validate_bundle(
            terminal,
            result,
            report,
            roster,
        )
        assignments, oracle_hits = self._validate_arm_evidence(arm, pages)
        queries = self._read_query(paths.query, result, query_ordinals)
        return Authority(
            registered=self.registered,
            shape=self.shape,
            pages=pages,
            queries=queries,
            query_ordinals=query_ordinals,
            ground_truth_page_assignments=assignments,
            oracle_hits=oracle_hits,
        )


def load_authority(
    terminal_path: Path,
    result_path: Path,
    report_path: Path,
    roster_path: Path,
    query_path: Path,
    registered: RegisteredAuthority = REGISTERED_AUTHORITY,
    shape: ScientificShape = REGISTERED_SHAPE,
) -> Authority:
    """Authenticate the complete immutable evidence bundle before page I/O."""
    return Revision4Bvs2Authority(registered=registered, shape=shape).load(
        Revision4Bvs2Paths(
            terminal=Path(terminal_path),
            result=Path(result_path),
            report=Path(report_path),
            roster=Path(roster_path),
            query=Path(query_path),
        )
    )


def select_pages(score_matrix: numpy.ndarray, width: int = 8) -> numpy.ndarray:
    """Select pages using the registered (distance, page ordinal) ordering."""

    scores = numpy.asarray(score_matrix)
    if (
        scores.ndim != 2
        or scores.shape[0] == 0
        or scores.shape[1] == 0
        or not numpy.issubdtype(scores.dtype, numpy.floating)
        or not numpy.isfinite(scores).all()
        or type(width) is not int
        or width <= 0
        or width > scores.shape[1]
    ):
        raise ValueError("page score matrix differs")
    ordinals = numpy.arange(scores.shape[1], dtype=numpy.uint32)
    selected = numpy.empty((scores.shape[0], width), dtype=numpy.uint32)
    for query_index, row in enumerate(scores):
        selected[query_index] = numpy.lexsort((ordinals, row))[:width]
    return selected


def quality_metrics(authority: Authority, selections: numpy.ndarray) -> dict[str, object]:
    """Independently recompute recall and oracle attainment from page assignments."""

    if type(authority) is not Authority:
        raise ValueError("authority has the wrong concrete type")
    selected = numpy.asarray(selections)
    expected_shape = (authority.shape.query_count, authority.shape.selection_width)
    if (
        selected.shape != expected_shape
        or not numpy.issubdtype(selected.dtype, numpy.integer)
        or numpy.any(selected < 0)
        or numpy.any(selected >= authority.shape.page_count)
    ):
        raise ValueError("selected page shape differs")
    query_hits = []
    for query_index, row in enumerate(selected):
        concrete = tuple(int(page) for page in row)
        if len(set(concrete)) != authority.shape.selection_width:
            raise ValueError("selected pages duplicate")
        selected_set = set(concrete)
        query_hits.append(
            sum(
                bool(selected_set.intersection(assignments))
                for assignments in authority.ground_truth_page_assignments[query_index]
            )
        )
    denominator = authority.shape.query_count * authority.shape.recall_k
    total_hits = sum(query_hits)
    oracle_total = sum(authority.oracle_hits)
    if oracle_total <= 0:
        raise ValueError("oracle hit denominator differs")
    per_query_ppm = [
        hits * 1_000_000 // authority.shape.recall_k for hits in query_hits
    ]
    return {
        "aggregate_recall_ppm": total_hits * 1_000_000 // denominator,
        "minimum_query_recall_ppm": min(per_query_ppm),
        "oracle_attainment_ppm": total_hits * 1_000_000 // oracle_total,
        "query_hits": query_hits,
        "oracle_hits": list(authority.oracle_hits),
    }


def projected_serving_bytes() -> int:
    """Return the exact conservative K=32 serving-memory projection."""

    terms = (
        9_050_240 * 196,
        282_820 * 320,
        65_536 * 96 * 2,
        (65_536 + 1) * 4,
        65_536 * 4_096,
        512 * 1024**2,
        2 * 1_966_080,
    )
    if any(type(term) is not int or term < 0 for term in terms):
        raise ValueError("serving-memory term differs")
    total = sum(terms)
    if total != 2_686_433_028 or total > 3 * 1024**3:
        raise ValueError("serving-memory projection differs")
    return total


def _read_page_body(client: object, bucket: str, key: str, expected_bytes: int) -> bytes:
    response = client.get_object(Bucket=bucket, Key=key)  # type: ignore[attr-defined]
    if type(response) is not dict or "Body" not in response:
        raise ValueError("S3 response differs")
    stream = response["Body"]
    read = getattr(stream, "read", None)
    close = getattr(stream, "close", None)
    if not callable(read) or not callable(close):
        raise ValueError("S3 streaming body differs")
    chunks: list[bytes] = []
    remaining = expected_bytes
    try:
        while remaining:
            chunk = read(remaining)
            if type(chunk) is not bytes or not chunk:
                raise ValueError("S3 page body ended early")
            if len(chunk) > remaining:
                raise ValueError("S3 page read exceeded requested bytes")
            chunks.append(chunk)
            remaining -= len(chunk)
        extra = read(1)
        if type(extra) is not bytes or extra:
            raise ValueError("S3 page body has trailing bytes")
        return b"".join(chunks)
    finally:
        close()


def ordered_page_bodies(
    client: object,
    bucket: str,
    prefix: str,
    pages: tuple[PageRef, ...],
    max_inflight: int = 4,
) -> Iterator[tuple[PageRef, bytes]]:
    """Fetch immutable pages concurrently while yielding canonical ordinal order."""

    if (
        not hasattr(client, "get_object")
        or type(bucket) is not str
        or not bucket
        or type(prefix) is not str
        or type(pages) is not tuple
        or type(max_inflight) is not int
        or not 1 <= max_inflight <= 4
    ):
        raise ValueError("bounded page stream arguments differ")
    for expected, reference in enumerate(pages):
        _validate_page_reference(reference)
        if reference.page_ordinal != expected:
            raise ValueError("page stream order differs")

    executor = ThreadPoolExecutor(max_workers=max_inflight, thread_name_prefix="v23-page")
    futures: dict[int, Future[bytes]] = {}
    next_submit = 0

    def submit(index: int) -> None:
        reference = pages[index]
        futures[index] = executor.submit(
            _read_page_body,
            client,
            bucket,
            f"{prefix}{reference.path}",
            reference.encoded_bytes,
        )

    try:
        while next_submit < min(len(pages), max_inflight):
            submit(next_submit)
            next_submit += 1
        for index, reference in enumerate(pages):
            body = futures.pop(index).result()
            yield reference, body
            if next_submit < len(pages):
                submit(next_submit)
                next_submit += 1
    except BaseException:
        for future in futures.values():
            future.cancel()
        raise
    finally:
        executor.shutdown(wait=True, cancel_futures=True)


def _validated_pressure(sample: PressureSample) -> PressureSample:
    if type(sample) is not PressureSample:
        raise ValueError("pressure sample has the wrong concrete type")
    for field in dataclasses.fields(PressureSample):
        _concrete_nonnegative_int(getattr(sample, field.name), f"pressure {field.name}")
    return sample


def _pressure_stop_reason(
    sample: PressureSample,
    *,
    baseline_swap: int,
    previous_monotonic_ns: int,
) -> str | None:
    if sample.rss_bytes >= _RSS_LIMIT_BYTES:
        return "rss-limit"
    if sample.psi_full_avg10_ppm >= _PSI_LIMIT_PPM:
        return "psi-limit"
    if sample.swap_bytes - baseline_swap >= _SWAP_GROWTH_LIMIT_BYTES:
        return "swap-growth-limit"
    if sample.monotonic_ns - previous_monotonic_ns >= _PROGRESS_LIMIT_NS:
        return "progress-limit"
    return None


def _attempt_location(attempt_prefix: str) -> tuple[str, str]:
    parsed = urlparse(attempt_prefix)
    if parsed.scheme != "s3" or not parsed.netloc or not parsed.path.startswith("/"):
        raise ValueError("attempt prefix is not an S3 URI")
    prefix = parsed.path[1:]
    if not prefix.endswith("/"):
        raise ValueError("attempt prefix lacks trailing slash")
    return parsed.netloc, prefix


def run_falsifier(
    authority: Authority,
    client: object,
    pressure_probe: Callable[[], PressureSample],
    execute_complete_stream: bool,
) -> dict[str, object]:
    """Authenticate and score every page without retaining a page corpus."""

    if type(authority) is not Authority or not callable(pressure_probe):
        raise ValueError("falsifier authority differs")
    if type(execute_complete_stream) is not bool or not execute_complete_stream:
        raise ValueError("complete stream requires explicit execution authority")
    _validate_shape(authority.shape)
    _validate_registered(authority.registered)
    if (
        type(authority.pages) is not tuple
        or len(authority.pages) != authority.shape.page_count
        or type(authority.query_ordinals) is not tuple
        or len(authority.query_ordinals) != authority.shape.query_count
        or authority.queries.shape
        != (authority.shape.query_count, authority.shape.dimensions)
    ):
        raise ValueError("falsifier scientific shape differs")

    bucket, prefix = _attempt_location(authority.registered.attempt_prefix)
    initial = _validated_pressure(pressure_probe())
    initial_reason = _pressure_stop_reason(
        initial,
        baseline_swap=initial.swap_bytes,
        previous_monotonic_ns=initial.monotonic_ns,
    )
    if initial_reason is not None:
        raise StreamStopped(initial_reason, -1, None)

    scores = numpy.empty(
        (authority.shape.query_count, authority.shape.page_count), dtype="<f4"
    )
    started_ns = time.monotonic_ns()
    started_cpu_ns = time.process_time_ns()
    peak_rss = initial.rss_bytes
    peak_psi = initial.psi_full_avg10_ppm
    previous_pressure_ns = initial.monotonic_ns
    last_pressure = initial
    total_bytes = 0
    primary_rows = 0
    replica_rows = 0
    last_page = -1
    last_checksum: str | None = None

    for reference, body in ordered_page_bodies(
        client, bucket, prefix, authority.pages, max_inflight=4
    ):
        vectors = decode_bvp2_page(reference, body)
        means = spherical_kmeans(vectors, reference.checksum, clusters=32, iterations=8)
        page_scores = score_page_means(authority.queries, means)
        if page_scores.shape != (authority.shape.query_count,) or not numpy.isfinite(
            page_scores
        ).all():
            raise ValueError("page scores differ")
        scores[:, reference.page_ordinal] = page_scores.astype("<f4")
        total_bytes += len(body)
        primary_rows += reference.primary_rows
        replica_rows += reference.replicated_rows
        last_page = reference.page_ordinal
        last_checksum = reference.checksum
        del body, vectors, means, page_scores

        sample = _validated_pressure(pressure_probe())
        last_pressure = sample
        peak_rss = max(peak_rss, sample.rss_bytes)
        peak_psi = max(peak_psi, sample.psi_full_avg10_ppm)
        reason = _pressure_stop_reason(
            sample,
            baseline_swap=initial.swap_bytes,
            previous_monotonic_ns=previous_pressure_ns,
        )
        previous_pressure_ns = sample.monotonic_ns
        if reason is not None:
            raise StreamStopped(reason, last_page, last_checksum)

    selected = select_pages(scores, authority.shape.selection_width)
    metrics = quality_metrics(authority, selected)
    elapsed_ns = time.monotonic_ns() - started_ns
    cpu_ns = time.process_time_ns() - started_cpu_ns
    final_swap_delta = max(0, last_pressure.swap_bytes - initial.swap_bytes)
    passed = (
        metrics["aggregate_recall_ppm"] >= 975_000
        and metrics["minimum_query_recall_ppm"] >= 800_000
        and metrics["oracle_attainment_ppm"] >= 995_000
        and projected_serving_bytes() <= 3 * 1024**3
        and final_swap_delta == 0
    )
    result: dict[str, object] = {
        "schema": "borsuk-v23-clustered-page-falsifier-v1",
        "source_commit": authority.registered.source_commit,
        "attempt_prefix": authority.registered.attempt_prefix,
        "terminal_sha256": authority.registered.terminal_sha256,
        "result_sha256": authority.registered.result_sha256,
        "report_sha256": authority.registered.report_sha256,
        "roster_sha256": authority.registered.roster_sha256,
        "query_uri": authority.registered.query_uri,
        "query_sha256": authority.registered.query_sha256,
        "page_count": authority.shape.page_count,
        "query_count": authority.shape.query_count,
        "dimensions": authority.shape.dimensions,
        "recall_k": authority.shape.recall_k,
        "selection_width": authority.shape.selection_width,
        "authenticated_pages": last_page + 1,
        "authenticated_primary_rows": primary_rows,
        "authenticated_replica_rows": replica_rows,
        "total_bytes_read": total_bytes,
        "algorithm": dict(_ALGORITHM),
        "query_ordinals": list(authority.query_ordinals),
        "selected_pages": selected.astype(numpy.uint32).tolist(),
        "query_hits": metrics["query_hits"],
        "oracle_hits": metrics["oracle_hits"],
        "aggregate_recall_ppm": metrics["aggregate_recall_ppm"],
        "minimum_query_recall_ppm": metrics["minimum_query_recall_ppm"],
        "oracle_attainment_ppm": metrics["oracle_attainment_ppm"],
        "projected_serving_bytes": projected_serving_bytes(),
        "elapsed_ns": elapsed_ns,
        "cpu_ns": cpu_ns,
        "peak_rss_bytes": peak_rss,
        "peak_psi_full_avg10_ppm": peak_psi,
        "swap_delta_bytes": final_swap_delta,
        "passed": passed,
    }
    return validate_result(result)


def _concrete_int_vector(value: object, length: int, role: str) -> list[int]:
    if type(value) is not list or len(value) != length:
        raise ValueError(f"{role} cardinality differs")
    for item in value:
        _concrete_nonnegative_int(item, role)
    return value


def validate_result(value: object) -> dict[str, object]:
    """Validate the complete terminal result using concrete types and exact keys."""

    result = _exact_dict(value, _RESULT_FIELDS, "falsifier result")
    if (
        result["schema"] != "borsuk-v23-clustered-page-falsifier-v1"
        or type(result["schema"]) is not str
        or type(result["source_commit"]) is not str
        or re.fullmatch(r"[0-9a-f]{40}", result["source_commit"]) is None
        or type(result["attempt_prefix"]) is not str
        or type(result["query_uri"]) is not str
    ):
        raise ValueError("falsifier result authority differs")
    _attempt_location(result["attempt_prefix"])
    query_location = urlparse(result["query_uri"])
    if query_location.scheme != "s3" or not query_location.netloc:
        raise ValueError("falsifier query URI differs")
    for role in (
        "terminal_sha256",
        "result_sha256",
        "report_sha256",
        "roster_sha256",
        "query_sha256",
    ):
        if not _digest_is_valid(result[role]):
            raise ValueError(f"falsifier {role} differs")

    integer_fields = (
        "page_count",
        "query_count",
        "dimensions",
        "recall_k",
        "selection_width",
        "authenticated_pages",
        "authenticated_primary_rows",
        "authenticated_replica_rows",
        "total_bytes_read",
        "aggregate_recall_ppm",
        "minimum_query_recall_ppm",
        "oracle_attainment_ppm",
        "projected_serving_bytes",
        "elapsed_ns",
        "cpu_ns",
        "peak_rss_bytes",
        "peak_psi_full_avg10_ppm",
        "swap_delta_bytes",
    )
    for role in integer_fields:
        _concrete_nonnegative_int(result[role], role)
    page_count = result["page_count"]
    query_count = result["query_count"]
    recall_k = result["recall_k"]
    selection_width = result["selection_width"]
    if (
        page_count <= 0
        or query_count <= 0
        or result["dimensions"] <= 0
        or recall_k <= 0
        or selection_width <= 0
        or selection_width > page_count
        or result["authenticated_pages"] != page_count
        or result["authenticated_primary_rows"] <= 0
        or result["total_bytes_read"] <= 0
        or result["projected_serving_bytes"] != projected_serving_bytes()
        or result["peak_rss_bytes"] >= _RSS_LIMIT_BYTES
        or result["peak_psi_full_avg10_ppm"] >= _PSI_LIMIT_PPM
        or result["swap_delta_bytes"] >= _SWAP_GROWTH_LIMIT_BYTES
    ):
        raise ValueError("falsifier result scientific bounds differ")
    if type(result["algorithm"]) is not dict or not _same_concrete(
        result["algorithm"], _ALGORITHM
    ):
        raise ValueError("falsifier algorithm authority differs")

    query_ordinals = _concrete_int_vector(result["query_ordinals"], query_count, "query ordinal")
    if len(set(query_ordinals)) != query_count:
        raise ValueError("falsifier query ordinals duplicate")
    selected_pages = result["selected_pages"]
    if type(selected_pages) is not list or len(selected_pages) != query_count:
        raise ValueError("falsifier selected-page cardinality differs")
    for row in selected_pages:
        selected_row = _concrete_int_vector(row, selection_width, "selected page")
        if len(set(selected_row)) != selection_width or any(page >= page_count for page in selected_row):
            raise ValueError("falsifier selected pages differ")
    query_hits = _concrete_int_vector(result["query_hits"], query_count, "query hit")
    oracle_hits = _concrete_int_vector(result["oracle_hits"], query_count, "oracle hit")
    if any(hit > recall_k for hit in query_hits + oracle_hits) or sum(oracle_hits) <= 0:
        raise ValueError("falsifier hit evidence differs")
    expected_aggregate = sum(query_hits) * 1_000_000 // (query_count * recall_k)
    expected_minimum = min(hit * 1_000_000 // recall_k for hit in query_hits)
    expected_attainment = sum(query_hits) * 1_000_000 // sum(oracle_hits)
    if (
        result["aggregate_recall_ppm"] != expected_aggregate
        or result["minimum_query_recall_ppm"] != expected_minimum
        or result["oracle_attainment_ppm"] != expected_attainment
    ):
        raise ValueError("falsifier quality arithmetic differs")
    expected_passed = (
        expected_aggregate >= 975_000
        and expected_minimum >= 800_000
        and expected_attainment >= 995_000
        and result["projected_serving_bytes"] <= 3 * 1024**3
        and result["swap_delta_bytes"] == 0
    )
    if type(result["passed"]) is not bool or result["passed"] is not expected_passed:
        raise ValueError("falsifier pass decision differs")
    return result


def canonical_result_bytes(value: dict[str, object]) -> bytes:
    """Return one newline-terminated canonical result document."""

    return canonical_json_bytes(validate_result(value)) + b"\n"


def canonical_stop_bytes(error: StreamStopped) -> bytes:
    """Return an outcome-blind canonical stop receipt with no quality fields."""

    if (
        type(error) is not StreamStopped
        or type(error.reason) is not str
        or error.reason not in _STOP_REASONS
        or type(error.last_authenticated_page) is not int
        or error.last_authenticated_page < -1
        or (
            error.last_authenticated_page == -1
            and error.last_authenticated_checksum is not None
        )
        or (
            error.last_authenticated_page >= 0
            and not _digest_is_valid(error.last_authenticated_checksum)
        )
    ):
        raise ValueError("stream stop receipt differs")
    return canonical_json_bytes(
        {
            "schema": "borsuk-v23-clustered-page-falsifier-stop-v1",
            "status": "stopped",
            "reason": error.reason,
            "last_authenticated_page": error.last_authenticated_page,
            "last_authenticated_checksum": error.last_authenticated_checksum,
        }
    ) + b"\n"


def _s3_config() -> object:
    """Pin each request/retry envelope comfortably below the wedge threshold."""

    from botocore.config import Config

    return Config(
        connect_timeout=10,
        read_timeout=60,
        retries={"total_max_attempts": 3, "mode": "standard"},
        max_pool_connections=4,
    )


def _default_pressure_probe() -> PressureSample:
    peak_rss_bytes = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024
    psi_ppm = 0
    try:
        for line in Path("/sys/fs/cgroup/memory.pressure").read_text().splitlines():
            if line.startswith("full "):
                values = dict(field.split("=", 1) for field in line.split()[1:])
                psi_ppm = int(Decimal(values["avg10"]) * 1_000_000)
                break
    except (FileNotFoundError, KeyError, InvalidOperation, ValueError):
        psi_ppm = 0
    try:
        swap_bytes = int(Path("/sys/fs/cgroup/memory.swap.current").read_text().strip())
    except (FileNotFoundError, ValueError):
        swap_bytes = 0
    return PressureSample(peak_rss_bytes, psi_ppm, swap_bytes, time.monotonic_ns())


def main(argv: Sequence[str] | None = None) -> int:
    """Authenticate local evidence and run only under the explicit stream flag."""

    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    for role in ("terminal", "result", "report", "roster", "query"):
        parser.add_argument(f"--{role}", required=True, type=Path)
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--prefix", required=True)
    parser.add_argument("--aws-profile", required=True)
    parser.add_argument("--region", required=True)
    parser.add_argument("--execute-complete-stream", action="store_true")
    arguments = parser.parse_args(argv)
    registered_bucket, registered_prefix = _attempt_location(REGISTERED_AUTHORITY.attempt_prefix)
    if (
        arguments.bucket != registered_bucket
        or arguments.prefix != registered_prefix
        or arguments.aws_profile != "causality"
        or arguments.region != "eu-central-1"
        or not arguments.execute_complete_stream
    ):
        parser.error("execution authority differs from the registered complete stream")
    authority = load_authority(
        arguments.terminal,
        arguments.result,
        arguments.report,
        arguments.roster,
        arguments.query,
    )
    import boto3

    client = boto3.Session(profile_name=arguments.aws_profile).client(
        "s3", region_name=arguments.region, config=_s3_config()
    )
    try:
        result = run_falsifier(authority, client, _default_pressure_probe, True)
    except StreamStopped as error:
        payload = canonical_stop_bytes(error)
        digest = hashlib.sha256(payload).hexdigest().encode()
        sys.stderr.buffer.write(payload + b"sha256=" + digest + b"\n")
        return 3
    payload = canonical_result_bytes(result)
    sys.stdout.buffer.write(payload)
    sys.stderr.write(f"sha256={hashlib.sha256(payload).hexdigest()}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
