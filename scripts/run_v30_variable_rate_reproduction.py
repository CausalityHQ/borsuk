#!/usr/bin/env python3
"""Authority and fail-fast contracts for the V30 variable-rate reproduction."""

from __future__ import annotations

import hashlib
import json
import math
import sys
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

import numpy as np

SOURCE_ROWS = 100_000
QUERY_COUNT = 32
RECALL_K = 10
TRUTH_MEMBERSHIPS = QUERY_COUNT * RECALL_K
FIDELITY_FRACTIONS_PPM = (0, 50_000, 100_000, 200_000)
MAX_CANDIDATE_DEPTH = 12_288
PAGE_COUNT = 10
MAX_ENCODED_BYTES = 4_587_520
MAX_SCANNED_CODES = 1_000_000
ARTIFACT_ROLES = (
    "pages-manifest",
    "leaf-postings",
    "leaf-centroids",
    "query-parquet",
)


@dataclass(frozen=True)
class ArtifactAuthority:
    """Exact immutable identity for one reproduction input."""

    role: str
    uri: str
    sha256: str
    encoded_bytes: int


@dataclass(frozen=True)
class V30ConstructionInputs:
    """Construction-only capabilities; evaluation inputs are deliberately absent."""

    pages_manifest: ArtifactAuthority
    leaf_postings: ArtifactAuthority
    leaf_centroids: ArtifactAuthority
    output_uri: str


@dataclass(frozen=True)
class V30ArmObservation:
    """Raw per-query evidence for one fixed fidelity fraction."""

    fidelity_fraction_ppm: int
    hits: tuple[int, ...]
    selected_page_counts: tuple[int, ...]
    maximum_encoded_bytes: int
    maximum_scanned_codes: int


@dataclass(frozen=True)
class Pq8Model:
    """One deterministic fixed-partition eight-bit residual quantizer."""

    width_bytes: int
    dimensions_per_subquantizer: int
    centroids: np.ndarray

    def score(self, codes: np.ndarray, query_residual: np.ndarray) -> np.ndarray:
        """Compute ADC squared distances with one fixed f32 reduction order."""

        if (
            not isinstance(codes, np.ndarray)
            or codes.dtype != np.uint8
            or codes.ndim != 2
            or codes.shape[1] != self.width_bytes
            or not isinstance(query_residual, np.ndarray)
            or query_residual.dtype != np.float32
            or query_residual.shape
            != (self.width_bytes * self.dimensions_per_subquantizer,)
            or not np.isfinite(query_residual).all()
        ):
            raise ValueError("V30 PQ8 score input differs")
        scores = np.zeros(len(codes), dtype=np.float32)
        for subquantizer in range(self.width_bytes):
            start = subquantizer * self.dimensions_per_subquantizer
            query_part = query_residual[
                start : start + self.dimensions_per_subquantizer
            ]
            table = np.sum(
                (self.centroids[subquantizer] - query_part) ** 2,
                axis=1,
                dtype=np.float32,
            )
            scores += table[codes[:, subquantizer]]
        return scores


@dataclass(frozen=True)
class LoadedReproduction:
    """Authenticated bounded corpus/query state resident only in a worker process."""

    primary: np.ndarray
    primary_leaf: np.ndarray
    leaf_centroids: np.ndarray
    queries: np.ndarray
    construction_bytes_streamed: int


@dataclass(frozen=True)
class V30ReproductionPlan:
    """One explicit no-discovery reproduction invocation."""

    artifacts: tuple[ArtifactAuthority, ...]
    page_prefix: str
    evidence_parquet: Path


def _exact_digest(value: object) -> bool:
    return (
        type(value) is str
        and len(value) == 64
        and all(character in "0123456789abcdef" for character in value)
    )


def parse_args(arguments: list[str]) -> V30ReproductionPlan:
    """Parse the closed one-shot interface; no local corpus path exists."""

    iterator = iter(arguments)
    if next(iterator, None) is None:
        raise ValueError("V30 reproduction program is missing")
    execute = False
    values: dict[str, str] = {}
    for flag in iterator:
        if flag == "--execute":
            if execute:
                raise ValueError("V30 reproduction duplicate --execute")
            execute = True
            continue
        if not flag.startswith("--"):
            raise ValueError("V30 reproduction flag differs")
        name = flag.removeprefix("--")
        value = next(iterator, None)
        if value is None:
            raise ValueError(f"V30 reproduction --{name} value is missing")
        if name in values:
            raise ValueError(f"V30 reproduction duplicate --{name}")
        values[name] = value
    if not execute:
        raise ValueError("V30 reproduction --execute is required")

    def take(name: str) -> str:
        try:
            return values.pop(name)
        except KeyError as error:
            raise ValueError(f"V30 reproduction missing --{name}") from error

    artifacts: list[ArtifactAuthority] = []
    for role in ARTIFACT_ROLES:
        try:
            encoded_bytes = int(take(f"{role}-bytes"))
        except ValueError as error:
            raise ValueError(f"V30 reproduction --{role}-bytes type differs") from error
        artifacts.append(
            ArtifactAuthority(
                role=role,
                uri=take(f"{role}-uri"),
                sha256=take(f"{role}-sha256"),
                encoded_bytes=encoded_bytes,
            )
        )
    page_prefix = take("page-prefix")
    evidence = Path(take("evidence-parquet"))
    if values:
        raise ValueError("V30 reproduction unknown flag")
    validate_reproduction_authority(
        tuple(artifacts),
        source_rows=SOURCE_ROWS,
        query_count=QUERY_COUNT,
        truth_memberships=TRUTH_MEMBERSHIPS,
    )
    if (
        not page_prefix.startswith("s3://")
        or page_prefix.endswith("/")
        or not evidence.is_absolute()
    ):
        raise ValueError("V30 reproduction output boundary differs")
    return V30ReproductionPlan(tuple(artifacts), page_prefix, evidence)


def validate_reproduction_authority(
    artifacts: tuple[ArtifactAuthority, ...],
    *,
    source_rows: int,
    query_count: int,
    truth_memberships: int,
) -> None:
    """Fail closed on input identity or frozen-shape drift."""

    if (
        type(artifacts) is not tuple
        or len(artifacts) != len(ARTIFACT_ROLES)
        or tuple(artifact.role for artifact in artifacts) != ARTIFACT_ROLES
    ):
        raise ValueError("V30 reproduction artifact roles differ")
    for artifact in artifacts:
        if type(artifact) is not ArtifactAuthority:
            raise ValueError("V30 reproduction artifact type differs")
        if not artifact.uri.startswith("s3://") or artifact.uri.endswith("/"):
            raise ValueError("V30 reproduction artifact URI differs")
        if not _exact_digest(artifact.sha256):
            raise ValueError("V30 reproduction artifact digest differs")
        if type(artifact.encoded_bytes) is not int or artifact.encoded_bytes <= 0:
            raise ValueError("V30 reproduction artifact length differs")
    if (
        type(source_rows) is not int
        or source_rows != SOURCE_ROWS
        or type(query_count) is not int
        or query_count != QUERY_COUNT
        or type(truth_memberships) is not int
        or truth_memberships != TRUTH_MEMBERSHIPS
    ):
        raise ValueError("V30 reproduction frozen shape differs")


def _authenticated_object(
    authority: ArtifactAuthority, get_object: Callable[[str], bytes]
) -> bytes:
    body = get_object(authority.uri)
    if (
        type(body) is not bytes
        or len(body) != authority.encoded_bytes
        or hashlib.sha256(body).hexdigest() != authority.sha256
    ):
        raise ValueError(f"V30 {authority.role} byte authority differs")
    return body


def _normalize_rows(rows: np.ndarray, role: str) -> np.ndarray:
    if rows.dtype != np.float32:
        rows = rows.astype(np.float32)
    if rows.ndim != 2 or rows.shape[1] != 96 or not np.isfinite(rows).all():
        raise ValueError(f"V30 {role} values differ")
    norms = np.linalg.norm(rows, axis=1)
    if np.any(~np.isfinite(norms)) or np.any(norms <= 0):
        raise ValueError(f"V30 {role} norms differ")
    return rows / norms[:, None]


def load_frozen_reproduction(
    artifacts: tuple[ArtifactAuthority, ...],
    *,
    page_prefix: str,
    get_object: Callable[[str], bytes],
    expected_source_rows: int = SOURCE_ROWS,
    expected_query_rows: int = 10_000,
) -> LoadedReproduction:
    """Stream and authenticate the frozen reduced corpus without local persistence."""

    import pyarrow as pa
    import pyarrow.ipc as ipc
    import pyarrow.parquet as pq

    if (
        type(artifacts) is not tuple
        or tuple(artifact.role for artifact in artifacts) != ARTIFACT_ROLES
        or type(page_prefix) is not str
        or not page_prefix.startswith("s3://")
        or page_prefix.endswith("/")
        or not callable(get_object)
        or type(expected_source_rows) is not int
        or expected_source_rows <= 0
        or type(expected_query_rows) is not int
        or expected_query_rows < QUERY_COUNT
    ):
        raise ValueError("V30 frozen reproduction request differs")
    bodies = {
        artifact.role: _authenticated_object(artifact, get_object)
        for artifact in artifacts
    }
    manifest_bytes = bodies["pages-manifest"]
    if not manifest_bytes.endswith(b"\n") or manifest_bytes.endswith(b"\n\n"):
        raise ValueError("V30 page manifest newline differs")
    try:
        manifest = json.loads(manifest_bytes)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("V30 page manifest JSON differs") from error
    canonical = json.dumps(
        manifest, allow_nan=False, separators=(",", ":"), sort_keys=True
    ).encode() + b"\n"
    if canonical != manifest_bytes or set(manifest) != {
        "pages",
        "primary_rows",
        "replica_rows",
        "schema_version",
        "source_rows",
        "stored_rows",
    }:
        raise ValueError("V30 page manifest authority differs")
    pages = manifest["pages"]
    if (
        type(manifest["schema_version"]) is not int
        or manifest["schema_version"] != 1
        or type(manifest["source_rows"]) is not int
        or manifest["source_rows"] != expected_source_rows
        or type(manifest["primary_rows"]) is not int
        or manifest["primary_rows"] != expected_source_rows
        or type(manifest["replica_rows"]) is not int
        or manifest["replica_rows"] < 0
        or type(manifest["stored_rows"]) is not int
        or manifest["stored_rows"]
        != manifest["primary_rows"] + manifest["replica_rows"]
        or type(pages) is not list
        or not pages
    ):
        raise ValueError("V30 page manifest shape differs")
    for ordinal, page in enumerate(pages):
        if (
            type(page) is not dict
            or set(page)
            != {
                "encoded_bytes",
                "ordinal",
                "primary_rows",
                "replica_rows",
                "sha256",
            }
            or type(page["ordinal"]) is not int
            or page["ordinal"] != ordinal
            or not _exact_digest(page["sha256"])
            or type(page["encoded_bytes"]) is not int
            or page["encoded_bytes"] <= 0
            or type(page["primary_rows"]) is not int
            or page["primary_rows"] <= 0
            or type(page["replica_rows"]) is not int
            or page["replica_rows"] < 0
            or page["primary_rows"] + page["replica_rows"] > 1_024
        ):
            raise ValueError("V30 page reference differs")
    posting_schema = pa.schema(
        [
            pa.field("leaf_ordinal", pa.uint32(), nullable=False),
            pa.field("page_ordinal", pa.uint32(), nullable=False),
            pa.field("page_sha256", pa.string(), nullable=False),
            pa.field("encoded_bytes", pa.uint64(), nullable=False),
            pa.field("primary_rows", pa.uint16(), nullable=False),
            pa.field("replica_rows", pa.uint16(), nullable=False),
        ]
    )
    postings_file = pq.ParquetFile(pa.BufferReader(bodies["leaf-postings"]))
    if postings_file.schema_arrow != posting_schema or postings_file.metadata.num_rows != len(pages):
        raise ValueError("V30 leaf postings schema differs")
    postings = postings_file.read()
    if any(column.null_count for column in postings.columns):
        raise ValueError("V30 leaf postings nullability differs")
    page_ordinals = postings["page_ordinal"].to_numpy()
    leaf_ordinals = postings["leaf_ordinal"].to_numpy()
    if sorted(int(value) for value in page_ordinals) != list(range(len(pages))):
        raise ValueError("V30 leaf postings ordinals differ")
    page_leaf = np.empty(len(pages), dtype=np.int32)
    for row in range(len(pages)):
        page = int(page_ordinals[row])
        reference = pages[page]
        if (
            postings["page_sha256"][row].as_py() != reference["sha256"]
            or int(postings["encoded_bytes"][row].as_py()) != reference["encoded_bytes"]
            or int(postings["primary_rows"][row].as_py()) != reference["primary_rows"]
            or int(postings["replica_rows"][row].as_py()) != reference["replica_rows"]
        ):
            raise ValueError("V30 leaf postings binding differs")
        page_leaf[page] = int(leaf_ordinals[row])
    leaf_child = pa.field("element", pa.float16(), nullable=False)
    leaf_schema = pa.schema(
        [
            pa.field("root_ordinal", pa.uint16(), nullable=False),
            pa.field("centroid", pa.list_(leaf_child, 96), nullable=False),
        ]
    )
    leaf_reader = ipc.open_file(pa.BufferReader(bodies["leaf-centroids"]))
    if leaf_reader.schema != leaf_schema or leaf_reader.num_record_batches != 1:
        raise ValueError("V30 leaf centroid schema differs")
    leaf_table = leaf_reader.read_all()
    if any(column.null_count for column in leaf_table.columns):
        raise ValueError("V30 leaf centroid nullability differs")
    leaf_values = (
        leaf_table["centroid"].combine_chunks().values.to_numpy(zero_copy_only=False)
    )
    leaf_centroids = _normalize_rows(
        leaf_values.reshape(-1, 96).astype(np.float32), "leaf centroid"
    )
    if np.any(page_leaf < 0) or np.any(page_leaf >= len(leaf_centroids)):
        raise ValueError("V30 leaf postings range differs")
    query_child = pa.field("element", pa.float32(), nullable=False)
    query_schema = pa.schema(
        [pa.field("emb", pa.list_(query_child, 96), nullable=False)]
    )
    query_file = pq.ParquetFile(pa.BufferReader(bodies["query-parquet"]))
    if query_file.schema_arrow != query_schema or query_file.metadata.num_rows != expected_query_rows:
        raise ValueError("V30 query Parquet schema differs")
    query_table = query_file.read(columns=["emb"])
    if query_table["emb"].null_count:
        raise ValueError("V30 query Parquet nullability differs")
    query_values = (
        query_table["emb"].combine_chunks().values.to_numpy(zero_copy_only=False)
    )
    queries = _normalize_rows(
        query_values.reshape(-1, 96)[:QUERY_COUNT].astype(np.float32), "query"
    )
    page_child = pa.field("element", pa.float32(), nullable=False)
    page_schema = pa.schema(
        [
            pa.field("id", pa.binary(8), nullable=False),
            pa.field("vector", pa.list_(page_child, 96), nullable=False),
        ]
    )
    primary = np.empty((expected_source_rows, 96), dtype=np.float32)
    primary_leaf = np.empty(expected_source_rows, dtype=np.int32)
    seen = np.zeros(expected_source_rows, dtype=np.bool_)
    construction_bytes = 0
    for reference in pages:
        body = get_object(f"{page_prefix}/{reference['sha256']}.arrow")
        if (
            type(body) is not bytes
            or len(body) != reference["encoded_bytes"]
            or hashlib.sha256(body).hexdigest() != reference["sha256"]
        ):
            raise ValueError("V30 page byte authority differs")
        construction_bytes += len(body)
        reader = ipc.open_file(pa.BufferReader(body))
        if reader.schema != page_schema or reader.num_record_batches != 1:
            raise ValueError("V30 page Arrow schema differs")
        table = reader.read_all()
        if (
            table.num_rows != reference["primary_rows"] + reference["replica_rows"]
            or any(column.null_count for column in table.columns)
        ):
            raise ValueError("V30 page Arrow rows differ")
        ids = np.array(
            [int.from_bytes(value.as_py(), "little") for value in table["id"]],
            dtype=np.int64,
        )
        vectors = _normalize_rows(
            table["vector"]
            .combine_chunks()
            .values.to_numpy(zero_copy_only=False)
            .reshape(-1, 96)
            .astype(np.float32),
            "page",
        )
        count = reference["primary_rows"]
        primary_ids = ids[:count]
        if (
            np.any(primary_ids < 0)
            or np.any(primary_ids >= expected_source_rows)
            or seen[primary_ids].any()
        ):
            raise ValueError("V30 primary page union differs")
        primary[primary_ids] = vectors[:count]
        primary_leaf[primary_ids] = page_leaf[reference["ordinal"]]
        seen[primary_ids] = True
    if not seen.all():
        raise ValueError("V30 primary page union differs")
    return LoadedReproduction(
        primary=primary,
        primary_leaf=primary_leaf,
        leaf_centroids=leaf_centroids,
        queries=queries,
        construction_bytes_streamed=construction_bytes,
    )


def pq8_replacement_geometry() -> dict[str, int]:
    """Return the single preregistered replacement interpretation."""

    return {
        "base_centroids": 256,
        "base_dimensions": 4,
        "base_subquantizers": 24,
        "base_width_bytes": 24,
        "high_centroids": 256,
        "high_dimensions": 2,
        "high_subquantizers": 48,
        "high_width_bytes": 48,
    }


def _validate_pq8_matrix(rows: np.ndarray) -> None:
    if (
        not isinstance(rows, np.ndarray)
        or rows.dtype != np.float32
        or rows.ndim != 2
        or not rows.size
        or not np.isfinite(rows).all()
    ):
        raise ValueError("V30 PQ8 matrix differs")


def _nearest_centroids(
    values: np.ndarray, centroids: np.ndarray, *, batch_rows: int
) -> np.ndarray:
    assignments = np.empty(len(values), dtype=np.int64)
    for start in range(0, len(values), batch_rows):
        batch = values[start : start + batch_rows]
        distances = np.sum(
            (batch[:, None, :] - centroids[None, :, :]) ** 2,
            axis=2,
            dtype=np.float32,
        )
        assignments[start : start + len(batch)] = np.argmin(distances, axis=1)
    return assignments


def fit_pq8(
    residuals: np.ndarray,
    *,
    width_bytes: int,
    centroid_count: int = 256,
    sample_size: int = 8_192,
    iterations: int = 4,
    batch_rows: int = 4_096,
) -> Pq8Model:
    """Fit deterministic evenly sampled PQ8 codebooks with bounded batches."""

    _validate_pq8_matrix(residuals)
    if (
        type(width_bytes) is not int
        or width_bytes <= 0
        or residuals.shape[1] % width_bytes
        or type(centroid_count) is not int
        or not 1 <= centroid_count <= 256
        or type(sample_size) is not int
        or sample_size < centroid_count
        or type(iterations) is not int
        or iterations <= 0
        or type(batch_rows) is not int
        or batch_rows <= 0
    ):
        raise ValueError("V30 PQ8 geometry differs")
    dimensions = residuals.shape[1] // width_bytes
    count = min(sample_size, len(residuals))
    if count < centroid_count:
        raise ValueError("V30 PQ8 training sample differs")
    sample_ordinals = np.arange(count, dtype=np.int64) * len(residuals) // count
    sample = residuals[sample_ordinals]
    books = np.empty((width_bytes, centroid_count, dimensions), dtype=np.float32)
    for subquantizer in range(width_bytes):
        start = subquantizer * dimensions
        values = sample[:, start : start + dimensions]
        centers = values[
            np.arange(centroid_count, dtype=np.int64) * count // centroid_count
        ].copy()
        for _ in range(iterations):
            assignments = _nearest_centroids(values, centers, batch_rows=batch_rows)
            for centroid in range(centroid_count):
                members = values[assignments == centroid]
                if len(members):
                    centers[centroid] = np.mean(members, axis=0, dtype=np.float32)
        books[subquantizer] = centers
    return Pq8Model(
        width_bytes=width_bytes,
        dimensions_per_subquantizer=dimensions,
        centroids=books,
    )


def encode_pq8(
    model: Pq8Model, residuals: np.ndarray, *, batch_rows: int = 4_096
) -> tuple[np.ndarray, np.ndarray]:
    """Encode residuals and return exact base reconstruction error."""

    _validate_pq8_matrix(residuals)
    if (
        type(model) is not Pq8Model
        or residuals.shape[1]
        != model.width_bytes * model.dimensions_per_subquantizer
        or type(batch_rows) is not int
        or batch_rows <= 0
    ):
        raise ValueError("V30 PQ8 encode input differs")
    codes = np.empty((len(residuals), model.width_bytes), dtype=np.uint8)
    errors = np.zeros(len(residuals), dtype=np.float32)
    for subquantizer in range(model.width_bytes):
        start = subquantizer * model.dimensions_per_subquantizer
        values = residuals[:, start : start + model.dimensions_per_subquantizer]
        assignments = _nearest_centroids(
            values, model.centroids[subquantizer], batch_rows=batch_rows
        )
        codes[:, subquantizer] = assignments.astype(np.uint8)
        reconstructed = model.centroids[subquantizer, assignments]
        errors += np.sum(
            (values - reconstructed) ** 2, axis=1, dtype=np.float32
        )
    return codes, errors


def build_base_page_layout(
    primary_leaf: np.ndarray,
    base_codes: np.ndarray,
    *,
    leaf_count: int,
    page_rows: int,
) -> tuple[tuple[np.ndarray, ...], np.ndarray, dict[int, np.ndarray]]:
    """Build one-owner pages once from leaf and transient base-code order."""

    if (
        not isinstance(primary_leaf, np.ndarray)
        or primary_leaf.ndim != 1
        or not isinstance(base_codes, np.ndarray)
        or base_codes.dtype != np.uint8
        or base_codes.ndim != 2
        or len(primary_leaf) != len(base_codes)
        or type(leaf_count) is not int
        or leaf_count <= 0
        or np.any(primary_leaf < 0)
        or np.any(primary_leaf >= leaf_count)
        or type(page_rows) is not int
        or not 1 <= page_rows <= 512
    ):
        raise ValueError("V30 base page layout input differs")
    ordinals = np.arange(len(primary_leaf), dtype=np.int64)
    keys: list[np.ndarray] = [ordinals]
    keys.extend(base_codes[:, index] for index in range(base_codes.shape[1] - 1, -1, -1))
    keys.append(primary_leaf)
    order = np.lexsort(tuple(keys))
    pages: list[np.ndarray] = []
    row_page = np.full(len(primary_leaf), -1, dtype=np.int32)
    leaf_rows: dict[int, np.ndarray] = {}
    for leaf in range(leaf_count):
        rows = order[primary_leaf[order] == leaf]
        leaf_rows[leaf] = rows
        for start in range(0, len(rows), page_rows):
            chunk = rows[start : start + page_rows]
            row_page[chunk] = len(pages)
            pages.append(chunk)
    if np.any(row_page < 0) or sorted(np.concatenate(pages).tolist()) != list(range(len(primary_leaf))):
        raise ValueError("V30 base page ownership differs")
    return tuple(pages), row_page, leaf_rows


def exact_truth(
    primary: np.ndarray, queries: np.ndarray, *, recall_k: int = RECALL_K
) -> tuple[tuple[int, ...], ...]:
    """Compute deterministic exact squared-L2 truth over the bounded corpus."""

    _validate_pq8_matrix(primary)
    _validate_pq8_matrix(queries)
    if (
        primary.shape[1] != queries.shape[1]
        or type(recall_k) is not int
        or not 1 <= recall_k <= len(primary)
    ):
        raise ValueError("V30 truth input differs")
    ordinals = np.arange(len(primary), dtype=np.int64)
    truth: list[tuple[int, ...]] = []
    for query in queries:
        distance = np.float32(2.0) - np.float32(2.0) * (primary @ query)
        picked = np.argpartition(distance, recall_k - 1)[:recall_k]
        ordered = picked[np.lexsort((ordinals[picked], distance[picked]))]
        truth.append(tuple(int(value) for value in ordered))
    return tuple(truth)


def evaluate_pq8_replacement_arms(
    primary: np.ndarray,
    primary_leaf: np.ndarray,
    leaf_centroids: np.ndarray,
    queries: np.ndarray,
    truth: tuple[tuple[int, ...], ...],
    base_model: Pq8Model,
    high_model: Pq8Model,
    *,
    page_rows: int,
    leaf_beam: int,
    candidate_depth: int,
    page_encoded_bytes: tuple[int, ...],
) -> tuple[V30ArmObservation, ...]:
    """Evaluate fixed replacement fractions over one immutable base-code page layout."""

    _validate_pq8_matrix(primary)
    _validate_pq8_matrix(leaf_centroids)
    _validate_pq8_matrix(queries)
    if (
        queries.shape != (QUERY_COUNT, primary.shape[1])
        or leaf_centroids.shape[1] != primary.shape[1]
        or primary_leaf.shape != (len(primary),)
        or type(truth) is not tuple
        or len(truth) != QUERY_COUNT
        or any(
            type(neighbors) is not tuple
            or len(neighbors) != RECALL_K
            or len(set(neighbors)) != RECALL_K
            or any(type(row) is not int or not 0 <= row < len(primary) for row in neighbors)
            for neighbors in truth
        )
        or type(base_model) is not Pq8Model
        or type(high_model) is not Pq8Model
        or base_model.width_bytes * base_model.dimensions_per_subquantizer
        != primary.shape[1]
        or high_model.width_bytes * high_model.dimensions_per_subquantizer
        != primary.shape[1]
        or type(leaf_beam) is not int
        or not 1 <= leaf_beam <= len(leaf_centroids)
        or type(candidate_depth) is not int
        or not 1 <= candidate_depth <= MAX_CANDIDATE_DEPTH
    ):
        raise ValueError("V30 replacement evaluation input differs")
    residuals = primary - leaf_centroids[primary_leaf]
    base_codes, base_errors = encode_pq8(base_model, residuals)
    high_codes, _high_errors = encode_pq8(high_model, residuals)
    pages, row_page, leaf_rows = build_base_page_layout(
        primary_leaf,
        base_codes,
        leaf_count=len(leaf_centroids),
        page_rows=page_rows,
    )
    if (
        type(page_encoded_bytes) is not tuple
        or len(page_encoded_bytes) != len(pages)
        or any(type(value) is not int or value <= 0 for value in page_encoded_bytes)
    ):
        raise ValueError("V30 page byte authority differs")
    leaf_ordinals = np.arange(len(leaf_centroids), dtype=np.int64)
    observations: list[V30ArmObservation] = []
    for fraction_ppm in FIDELITY_FRACTIONS_PPM:
        high_mask = np.zeros(len(primary), dtype=np.bool_)
        high_mask[list(select_high_fidelity(base_errors.tolist(), fraction_ppm))] = True
        hits: list[int] = []
        selected_page_counts: list[int] = []
        maximum_bytes = 0
        maximum_scanned = 0
        for query_index, query in enumerate(queries):
            leaf_distance = np.sum(
                (leaf_centroids - query) ** 2, axis=1, dtype=np.float32
            )
            selected_leaves = np.lexsort((leaf_ordinals, leaf_distance))[:leaf_beam]
            ranked_rows: list[tuple[float, int]] = []
            scanned = 0
            for leaf in selected_leaves:
                rows = leaf_rows[int(leaf)]
                if not len(rows):
                    continue
                scanned += len(rows)
                query_residual = query - leaf_centroids[leaf]
                base_rows = rows[~high_mask[rows]]
                high_rows = rows[high_mask[rows]]
                if len(base_rows):
                    scores = base_model.score(base_codes[base_rows], query_residual)
                    ranked_rows.extend(
                        (float(score), int(row))
                        for score, row in zip(scores, base_rows, strict=True)
                    )
                if len(high_rows):
                    scores = high_model.score(high_codes[high_rows], query_residual)
                    ranked_rows.extend(
                        (float(score), int(row))
                        for score, row in zip(scores, high_rows, strict=True)
                    )
            depth = min(candidate_depth, len(ranked_rows))
            selected_pages = reduce_page_candidates(
                ranked_rows,
                row_page,
                candidate_depth=depth,
                page_count=PAGE_COUNT,
            )
            exact_rows = np.concatenate([pages[page] for page in selected_pages])
            distances = (
                np.float32(2.0)
                - np.float32(2.0) * (primary[exact_rows] @ query)
            )
            take = min(RECALL_K, len(exact_rows))
            local = np.argpartition(distances, take - 1)[:take]
            ordered = local[
                np.lexsort((exact_rows[local], distances[local]))
            ]
            matches = set(int(exact_rows[item]) for item in ordered)
            hits.append(len(matches & set(truth[query_index])))
            selected_page_counts.append(len(selected_pages))
            maximum_bytes = max(
                maximum_bytes,
                sum(page_encoded_bytes[page] for page in selected_pages),
            )
            maximum_scanned = max(maximum_scanned, scanned)
        observations.append(
            V30ArmObservation(
                fidelity_fraction_ppm=fraction_ppm,
                hits=tuple(hits),
                selected_page_counts=tuple(selected_page_counts),
                maximum_encoded_bytes=maximum_bytes,
                maximum_scanned_codes=maximum_scanned,
            )
        )
    return tuple(observations)


def select_high_fidelity(errors: list[float], fraction_ppm: int) -> tuple[int, ...]:
    """Select the exact query-independent reconstruction-error tail."""

    if (
        type(errors) is not list
        or not errors
        or any(type(value) not in {int, float} or not math.isfinite(value) or value < 0 for value in errors)
    ):
        raise ValueError("V30 reconstruction errors differ")
    if type(fraction_ppm) is not int or fraction_ppm not in FIDELITY_FRACTIONS_PPM:
        raise ValueError("V30 fidelity fraction differs")
    count = len(errors) * fraction_ppm // 1_000_000
    ranked = sorted(range(len(errors)), key=lambda ordinal: (-errors[ordinal], ordinal))
    return tuple(sorted(ranked[:count]))


def reduce_page_candidates(
    ranked_rows: list[tuple[float, int]],
    row_pages: tuple[int, ...],
    *,
    candidate_depth: int,
    page_count: int,
) -> tuple[int, ...]:
    """Reduce a bounded row frontier to deterministic unique page ordinals."""

    if (
        type(candidate_depth) is not int
        or candidate_depth <= 0
        or candidate_depth > MAX_CANDIDATE_DEPTH
        or candidate_depth > len(ranked_rows)
    ):
        raise ValueError("V30 candidate depth differs")
    if type(page_count) is not int or page_count != PAGE_COUNT:
        raise ValueError("V30 page count differs")
    if isinstance(row_pages, np.ndarray):
        if row_pages.ndim != 1 or not np.issubdtype(row_pages.dtype, np.integer) or np.any(row_pages < 0):
            raise ValueError("V30 row page authority differs")
    elif type(row_pages) is not tuple or any(
        type(page) is not int or page < 0 for page in row_pages
    ):
        raise ValueError("V30 row page authority differs")
    checked: list[tuple[float, int]] = []
    for score, row in ranked_rows:
        if (
            type(score) not in {int, float}
            or not math.isfinite(score)
            or type(row) is not int
            or not 0 <= row < len(row_pages)
        ):
            raise ValueError("V30 ranked row differs")
        checked.append((float(score), row))
    selected: list[int] = []
    seen: set[int] = set()
    for _score, row in sorted(checked, key=lambda item: (item[0], item[1]))[:candidate_depth]:
        page = int(row_pages[row])
        if page not in seen:
            seen.add(page)
            selected.append(page)
            if len(selected) == page_count:
                return tuple(selected)
    raise ValueError("V30 page count cannot be satisfied")


def _arm_result(observation: V30ArmObservation) -> dict[str, object]:
    if (
        type(observation) is not V30ArmObservation
        or observation.fidelity_fraction_ppm not in FIDELITY_FRACTIONS_PPM
        or type(observation.hits) is not tuple
        or len(observation.hits) != QUERY_COUNT
        or any(type(hit) is not int or not 0 <= hit <= RECALL_K for hit in observation.hits)
        or type(observation.selected_page_counts) is not tuple
        or observation.selected_page_counts != (PAGE_COUNT,) * QUERY_COUNT
        or type(observation.maximum_encoded_bytes) is not int
        or not 0 < observation.maximum_encoded_bytes <= MAX_ENCODED_BYTES
        or type(observation.maximum_scanned_codes) is not int
        or not 0 < observation.maximum_scanned_codes <= MAX_SCANNED_CODES
    ):
        raise ValueError("V30 arm evidence differs")
    hit_sum = sum(observation.hits)
    return {
        "aggregate_recall_ppm": hit_sum * 1_000_000 // TRUTH_MEMBERSHIPS,
        "fidelity_fraction_ppm": observation.fidelity_fraction_ppm,
        "maximum_encoded_bytes": observation.maximum_encoded_bytes,
        "maximum_scanned_codes": observation.maximum_scanned_codes,
        "minimum_recall_ppm": min(observation.hits) * 1_000_000 // RECALL_K,
        "perfect_queries": sum(hit == RECALL_K for hit in observation.hits),
        "query_count": QUERY_COUNT,
        "selected_pages": PAGE_COUNT,
    }


def build_reproduction_result(observations: tuple[V30ArmObservation, ...]) -> bytes:
    """Independently reduce raw arm evidence to one canonical claim-ineligible result."""

    if (
        type(observations) is not tuple
        or tuple(item.fidelity_fraction_ppm for item in observations) != FIDELITY_FRACTIONS_PPM
    ):
        raise ValueError("V30 arm ordering differs")
    arms = [_arm_result(observation) for observation in observations]
    passing = [
        arm
        for arm in arms
        if arm["aggregate_recall_ppm"] >= 996_875
        and arm["minimum_recall_ppm"] >= 900_000
        and arm["perfect_queries"] >= 31
    ]
    selected = min((int(arm["fidelity_fraction_ppm"]) for arm in passing), default=None)
    value = {
        "arms": arms,
        "claim_eligible": False,
        "geometry": pq8_replacement_geometry(),
        "schema": "borsuk-v30-variable-rate-reproduction-v1",
        "selected_fraction_ppm": selected,
        "status": "reproduced" if selected == 50_000 else "rejected",
    }
    return json.dumps(value, allow_nan=False, separators=(",", ":"), sort_keys=True).encode() + b"\n"


def encode_reproduction_evidence(
    observations: tuple[V30ArmObservation, ...],
) -> bytes:
    """Encode raw per-query arm evidence as strict cross-language Parquet."""

    import pyarrow as pa
    import pyarrow.parquet as pq

    if (
        type(observations) is not tuple
        or tuple(item.fidelity_fraction_ppm for item in observations)
        != FIDELITY_FRACTIONS_PPM
    ):
        raise ValueError("V30 evidence arm ordering differs")
    rows: list[dict[str, int]] = []
    for observation in observations:
        _arm_result(observation)
        for query_ordinal, (hits, selected_pages) in enumerate(
            zip(observation.hits, observation.selected_page_counts, strict=True)
        ):
            rows.append(
                {
                    "fidelity_fraction_ppm": observation.fidelity_fraction_ppm,
                    "query_ordinal": query_ordinal,
                    "hits": hits,
                    "selected_pages": selected_pages,
                    "maximum_encoded_bytes": observation.maximum_encoded_bytes,
                    "maximum_scanned_codes": observation.maximum_scanned_codes,
                }
            )
    schema = pa.schema(
        [
            pa.field("fidelity_fraction_ppm", pa.uint32(), nullable=False),
            pa.field("query_ordinal", pa.uint16(), nullable=False),
            pa.field("hits", pa.uint8(), nullable=False),
            pa.field("selected_pages", pa.uint8(), nullable=False),
            pa.field("maximum_encoded_bytes", pa.uint64(), nullable=False),
            pa.field("maximum_scanned_codes", pa.uint64(), nullable=False),
        ]
    )
    table = pa.Table.from_pylist(rows, schema=schema)
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink, compression="zstd", use_dictionary=False)
    return sink.getvalue().to_pybytes()


def finalize_reproduction_result(
    observations: tuple[V30ArmObservation, ...],
    artifacts: tuple[ArtifactAuthority, ...],
    *,
    construction_bytes_streamed: int,
    evidence_parquet: bytes,
) -> bytes:
    """Bind canonical summary evidence to every immutable input and Parquet bytes."""

    validate_reproduction_authority(
        artifacts,
        source_rows=SOURCE_ROWS,
        query_count=QUERY_COUNT,
        truth_memberships=TRUTH_MEMBERSHIPS,
    )
    expected_evidence = encode_reproduction_evidence(observations)
    if type(evidence_parquet) is not bytes or evidence_parquet != expected_evidence:
        raise ValueError("V30 evidence Parquet authority differs")
    if type(construction_bytes_streamed) is not int or construction_bytes_streamed <= 0:
        raise ValueError("V30 construction byte count differs")
    value = json.loads(build_reproduction_result(observations))
    value.update(
        {
            "artifacts": [
                {
                    "encoded_bytes": artifact.encoded_bytes,
                    "role": artifact.role,
                    "sha256": artifact.sha256,
                    "uri": artifact.uri,
                }
                for artifact in artifacts
            ],
            "construction_bytes_streamed": construction_bytes_streamed,
            "evidence_parquet_bytes": len(evidence_parquet),
            "evidence_parquet_sha256": hashlib.sha256(evidence_parquet).hexdigest(),
        }
    )
    return json.dumps(value, allow_nan=False, separators=(",", ":"), sort_keys=True).encode() + b"\n"


def _nearest_rank(values: list[int], numerator: int, denominator: int) -> int:
    index = max(0, (len(values) * numerator + denominator - 1) // denominator - 1)
    return sorted(values)[index]


def simulate_concurrent_get_latency_ns(waves: tuple[tuple[int, ...], ...]) -> dict[str, object]:
    """Project concurrent-wave latency from injected request observations without sleeping."""

    if (
        type(waves) is not tuple
        or len(waves) != QUERY_COUNT
        or any(
            type(wave) is not tuple
            or len(wave) != PAGE_COUNT
            or any(type(value) is not int or value <= 0 for value in wave)
            for wave in waves
        )
    ):
        raise ValueError("V30 S3 latency samples differ")
    wave_maxima = [max(wave) for wave in waves]
    return {
        "maximum_ns": max(wave_maxima),
        "model": "concurrent-max-no-sleep",
        "p50_ns": _nearest_rank(wave_maxima, 50, 100),
        "p95_ns": _nearest_rank(wave_maxima, 95, 100),
        "p99_ns": _nearest_rank(wave_maxima, 99, 100),
        "request_count": QUERY_COUNT * PAGE_COUNT,
        "wave_count": QUERY_COUNT,
    }


def _encoded_page_sizes(
    primary: np.ndarray, pages: tuple[np.ndarray, ...]
) -> tuple[int, ...]:
    """Materialize only bounded output pages to measure the real Arrow byte envelope."""

    import pyarrow as pa
    import pyarrow.ipc as ipc

    child = pa.field("element", pa.float32(), nullable=False)
    schema = pa.schema(
        [
            pa.field("id", pa.binary(8), nullable=False),
            pa.field("vector", pa.list_(child, 96), nullable=False),
        ]
    )
    sizes: list[int] = []
    for rows in pages:
        ids = pa.array(
            [int(row).to_bytes(8, "little") for row in rows], type=pa.binary(8)
        )
        vectors = pa.FixedSizeListArray.from_arrays(
            pa.array(primary[rows].ravel(), type=pa.float32()), 96
        )
        table = pa.Table.from_arrays([ids, vectors], schema=schema)
        sink = pa.BufferOutputStream()
        with ipc.new_file(sink, schema) as writer:
            writer.write_table(table)
        sizes.append(sink.tell())
    return tuple(sizes)


def run_reproduction(
    plan: V30ReproductionPlan, get_object: Callable[[str], bytes]
) -> tuple[bytes, bytes]:
    """Execute the frozen 100K scientific reproduction entirely in worker memory."""

    if type(plan) is not V30ReproductionPlan:
        raise ValueError("V30 reproduction plan type differs")
    loaded = load_frozen_reproduction(
        plan.artifacts,
        page_prefix=plan.page_prefix,
        get_object=get_object,
    )
    truth = exact_truth(loaded.primary, loaded.queries)
    residuals = loaded.primary - loaded.leaf_centroids[loaded.primary_leaf]
    base_model = fit_pq8(residuals, width_bytes=24)
    high_model = fit_pq8(residuals, width_bytes=48)
    base_codes, _base_errors = encode_pq8(base_model, residuals)
    pages, _row_page, _leaf_rows = build_base_page_layout(
        loaded.primary_leaf,
        base_codes,
        leaf_count=len(loaded.leaf_centroids),
        page_rows=512,
    )
    page_sizes = _encoded_page_sizes(loaded.primary, pages)
    observations = evaluate_pq8_replacement_arms(
        loaded.primary,
        loaded.primary_leaf,
        loaded.leaf_centroids,
        loaded.queries,
        truth,
        base_model,
        high_model,
        page_rows=512,
        leaf_beam=64,
        candidate_depth=12_288,
        page_encoded_bytes=page_sizes,
    )
    evidence = encode_reproduction_evidence(observations)
    result = finalize_reproduction_result(
        observations,
        plan.artifacts,
        construction_bytes_streamed=loaded.construction_bytes_streamed,
        evidence_parquet=evidence,
    )
    return result, evidence


def _s3_getter() -> Callable[[str], bytes]:
    import boto3
    from botocore.config import Config

    client = boto3.client(
        "s3",
        region_name="eu-central-1",
        config=Config(
            connect_timeout=10,
            read_timeout=30,
            retries={"max_attempts": 3, "mode": "standard"},
        ),
    )

    def get_object(uri: str) -> bytes:
        if not uri.startswith("s3://"):
            raise ValueError("V30 S3 URI differs")
        bucket, separator, key = uri.removeprefix("s3://").partition("/")
        if not bucket or not separator or not key:
            raise ValueError("V30 S3 URI differs")
        return client.get_object(Bucket=bucket, Key=key)["Body"].read()

    return get_object


def main(arguments: list[str]) -> int:
    """Run one explicit reproduction and emit only canonical result bytes to stdout."""

    try:
        plan = parse_args(arguments)
        result, evidence = run_reproduction(plan, _s3_getter())
        plan.evidence_parquet.write_bytes(evidence)
        sys.stdout.buffer.write(result)
    except (OSError, RuntimeError, ValueError) as error:
        print(str(error), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
