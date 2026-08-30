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
import hashlib
import json
from pathlib import Path
import re
import struct

from blake3 import blake3
import numpy

_PAGE_HEADER_BYTES = 96
_PAGE_MAX_ENCODED_BYTES = 245_760
_HEX_DIGEST = re.compile(r"[0-9a-f]{64}\Z")
_U64_MASK = (1 << 64) - 1

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
        or any(
            not _digest_is_valid(value)
            for value in (
                registered.terminal_sha256,
                registered.result_sha256,
                registered.report_sha256,
                registered.roster_sha256,
                registered.query_sha256,
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
    from scripts.publication_v3_protocol import canonical_json_bytes

    if type(value) is not dict or payload != canonical_json_bytes(value) + b"\n":
        raise ValueError(f"{role} bytes are not canonical")
    return value


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
        or any(report_artifact[key] != value for key, value in expected.items())
        or any(roster_artifact[key] != value for key, value in expected.items())
        or any(
            report_artifact[key] != roster_artifact[key]
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


def _validate_arm_shape(arm: dict[str, object], shape: ScientificShape) -> None:
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
    if (
        arm["maximum_query_pages"] != shape.selection_width
        or type(arm["passed"]) is not bool
        or type(arm["d1_key"]) is not dict
        or frozenset(arm["d1_key"]) != {"family", "code_width_bytes"}
        or type(arm["selector_key"]) is not dict
        or frozenset(arm["selector_key"]) != {"family", "code_width_bytes"}
        or type(arm["selector"]) is not dict
    ):
        raise ValueError("D2 arm authority differs")


def _query_evidence(
    arm: dict[str, object],
    shape: ScientificShape,
) -> tuple[
    tuple[tuple[tuple[int, ...], ...], ...],
    tuple[int, ...],
]:
    samples = arm["query_samples"]
    if type(samples) is not list or len(samples) != shape.query_count:
        raise ValueError("query sample shape differs")
    all_assignments = []
    oracle_hits = []
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
        if any(not pages or pages != tuple(sorted(set(pages))) for pages in assignments):
            raise ValueError("ground-truth page assignments differ")
        selected_hits = sum(bool(set(pages).intersection(selected)) for pages in assignments)
        recomputed_oracle_hits = sum(bool(set(pages).intersection(oracle)) for pages in assignments)
        if (
            sample["gt_page_hits"] != selected_hits
            or sample["oracle_gt_page_hits"] != recomputed_oracle_hits
        ):
            raise ValueError("query page-hit evidence differs")
        all_assignments.append(assignments)
        oracle_hits.append(recomputed_oracle_hits)
    return tuple(all_assignments), tuple(oracle_hits)


def _load_query_vectors(
    query_path: Path,
    expected_sha256: str,
    query_ordinals: tuple[int, ...],
    shape: ScientificShape,
) -> numpy.ndarray:
    payload = Path(query_path).read_bytes()
    if hashlib.sha256(payload).hexdigest() != expected_sha256:
        raise ValueError("query object SHA-256 differs")
    import pyarrow as pa
    import pyarrow.parquet as pq

    table = pq.read_table(query_path, columns=["emb"])
    if table.schema.names != ["emb"]:
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

    _validate_registered(registered)
    _validate_shape(shape)
    _read_canonical_json(Path(terminal_path), registered.terminal_sha256, "terminal marker")
    _read_canonical_json(Path(result_path), registered.result_sha256, "result receipt")
    report_artifact = _read_canonical_json(
        Path(report_path), registered.report_sha256, "D2 report"
    )
    roster_artifact = _read_canonical_json(
        Path(roster_path), registered.roster_sha256, "D2 page roster"
    )
    _validate_outer_artifacts(report_artifact, roster_artifact, registered)
    report = _exact_dict(report_artifact["report"], _REPORT_FIELDS, "D2 report")
    if (
        report["schema"] != "borsuk-v23-d2-v8"
        or not _digest_is_valid(report["d1_report_checksum"])
        or type(report["rows"]) is not int
        or report["rows"] <= 0
        or type(report["arms"]) is not list
        or len(report["arms"]) != 1
    ):
        raise ValueError("D2 report authority differs")
    query_ordinals = _concrete_int_list(
        report["query_ordinals"],
        "query ordinal",
        exact_length=shape.query_count,
    )
    if query_ordinals != tuple(sorted(set(query_ordinals))):
        raise ValueError("query ordinals differ")
    arm = _exact_dict(report["arms"][0], _ARM_FIELDS, "D2 arm")
    _validate_arm_shape(arm, shape)
    raw_pages = roster_artifact["pages"]
    raw_arm_pages = arm["pages"]
    if (
        type(raw_pages) is not list
        or len(raw_pages) != shape.page_count
        or type(raw_arm_pages) is not list
        or len(raw_arm_pages) != shape.page_count
    ):
        raise ValueError("page roster shape differs")
    pages = tuple(_page_from_json(page) for page in raw_pages)
    arm_pages = tuple(_page_from_json(page) for page in raw_arm_pages)
    if (
        pages != arm_pages
        or tuple(page.page_ordinal for page in pages) != tuple(range(shape.page_count))
        or len({page.path for page in pages}) != shape.page_count
        or len({page.checksum for page in pages}) != shape.page_count
        or any(page.dimensions != shape.dimensions for page in pages)
    ):
        raise ValueError("page roster authority differs")
    assignments, oracle_hits = _query_evidence(arm, shape)
    queries = _load_query_vectors(
        Path(query_path), registered.query_sha256, query_ordinals, shape
    )
    return Authority(
        registered=registered,
        shape=shape,
        pages=pages,
        queries=queries,
        query_ordinals=query_ordinals,
        ground_truth_page_assignments=assignments,
        oracle_hits=oracle_hits,
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
