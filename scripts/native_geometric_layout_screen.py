#!/usr/bin/env python3
"""Query-blind geometric page-layout screen for the native ANN redesign."""

from __future__ import annotations

import argparse
import dataclasses
import enum
import hashlib
import json
import math
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

AUTHORITY_SCHEMA = "borsuk-native-geometric-layout-authority-v1"
AUTHORITY_KEYS = frozenset(
    {
        "schema",
        "source",
        "rows",
        "dimensions",
        "metric",
        "seed",
        "method",
        "maximum_page_rows",
        "maximum_page_bytes",
    }
)
IDENTITY_KEYS = frozenset({"role", "uri", "sha256", "encoded_bytes"})


class LayoutMethod(str, enum.Enum):
    ID_ORDER_256 = "id-order-256"
    RANDOM_PROJECTION_256 = "balanced-random-projection-256"
    TWO_MEANS_256 = "balanced-two-means-256"
    TWO_MEANS_480K = "balanced-two-means-480k"


def _concrete_int(value: object, label: str, *, maximum: int) -> int:
    if type(value) is not int or not 0 < value <= maximum:
        raise ValueError(f"{label} differs")
    return value


def _digest(value: object, label: str) -> str:
    if (
        type(value) is not str
        or len(value) != 64
        or value.lower() != value
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise ValueError(f"{label} differs")
    return value


@dataclasses.dataclass(frozen=True, slots=True)
class ArtifactIdentity:
    role: str
    uri: str
    sha256: str
    encoded_bytes: int

    def __post_init__(self) -> None:
        if type(self.role) is not str or not self.role:
            raise ValueError("artifact role differs")
        if type(self.uri) is not str or not self.uri.startswith(("s3://", "file://")):
            raise ValueError("artifact URI differs")
        _digest(self.sha256, "artifact SHA-256")
        _concrete_int(self.encoded_bytes, "artifact encoded bytes", maximum=(1 << 63) - 1)


@dataclasses.dataclass(frozen=True, slots=True)
class LayoutAuthority:
    schema: str
    source: ArtifactIdentity
    rows: int
    dimensions: int
    metric: str
    seed: int
    method: LayoutMethod
    maximum_page_rows: int
    maximum_page_bytes: int

    def __post_init__(self) -> None:
        if self.schema != AUTHORITY_SCHEMA:
            raise ValueError("layout authority schema differs")
        if self.source.role != "source":
            raise ValueError("layout source role differs")
        _concrete_int(self.rows, "layout rows", maximum=(1 << 32) - 1)
        _concrete_int(self.dimensions, "layout dimensions", maximum=(1 << 16) - 1)
        if self.metric not in {"l2", "cosine"}:
            raise ValueError("layout metric differs")
        _concrete_int(self.seed, "layout seed", maximum=(1 << 64) - 1)
        if type(self.method) is not LayoutMethod:
            raise ValueError("layout method differs")
        _concrete_int(
            self.maximum_page_rows,
            "maximum page rows",
            maximum=(1 << 16) - 1,
        )
        _concrete_int(
            self.maximum_page_bytes,
            "maximum page bytes",
            maximum=(1 << 32) - 1,
        )


@dataclasses.dataclass(frozen=True, slots=True)
class MembershipRow:
    stable_id: bytes
    source_ordinal: int
    page_ordinal: int
    in_page_ordinal: int
    page_rows: int
    encoded_page_bytes: int
    method: LayoutMethod
    source_sha256: bytes
    seed: int
    construction_sha256: bytes


@dataclasses.dataclass(frozen=True, slots=True)
class EvaluationLimits:
    maximum_pages: int
    maximum_bytes: int

    def __post_init__(self) -> None:
        _concrete_int(self.maximum_pages, "maximum evaluation pages", maximum=(1 << 16) - 1)
        _concrete_int(self.maximum_bytes, "maximum evaluation bytes", maximum=(1 << 63) - 1)


@dataclasses.dataclass(frozen=True, slots=True)
class QueryCoverageSample:
    query_ordinal: int
    selected_page_ordinals: tuple[int, ...]
    hits_at_10: int
    hits_at_100: int
    encoded_bytes: int
    recall_at_10_ppm: int
    recall_at_100_ppm: int


@dataclasses.dataclass(frozen=True, slots=True)
class LayoutEvaluation:
    method: LayoutMethod
    samples: tuple[QueryCoverageSample, ...]
    recall_at_10_ppm: int
    mean_recall_at_100_ppm: int
    p05_recall_at_100_ppm: int
    worst_recall_at_100_ppm: int
    decision: str


def layout_authority_from_dict(payload: Mapping[str, Any]) -> LayoutAuthority:
    if type(payload) is not dict or set(payload) != AUTHORITY_KEYS:
        raise ValueError("layout authority keys differ")
    source_payload = payload["source"]
    if type(source_payload) is not dict or set(source_payload) != IDENTITY_KEYS:
        raise ValueError("layout source identity keys differ")
    try:
        method = LayoutMethod(payload["method"])
    except (TypeError, ValueError) as error:
        raise ValueError("layout method differs") from error
    return LayoutAuthority(
        schema=payload["schema"],
        source=ArtifactIdentity(
            role=source_payload["role"],
            uri=source_payload["uri"],
            sha256=source_payload["sha256"],
            encoded_bytes=source_payload["encoded_bytes"],
        ),
        rows=payload["rows"],
        dimensions=payload["dimensions"],
        metric=payload["metric"],
        seed=payload["seed"],
        method=method,
        maximum_page_rows=payload["maximum_page_rows"],
        maximum_page_bytes=payload["maximum_page_bytes"],
    )


def membership_schema() -> pa.Schema:
    return pa.schema(
        [
            pa.field("stable_id", pa.binary(), nullable=False),
            pa.field("source_ordinal", pa.uint32(), nullable=False),
            pa.field("page_ordinal", pa.uint32(), nullable=False),
            pa.field("in_page_ordinal", pa.uint16(), nullable=False),
            pa.field("page_rows", pa.uint16(), nullable=False),
            pa.field("encoded_page_bytes", pa.uint32(), nullable=False),
            pa.field("method", pa.string(), nullable=False),
            pa.field("source_sha256", pa.binary(32), nullable=False),
            pa.field("seed", pa.uint64(), nullable=False),
            pa.field("construction_sha256", pa.binary(32), nullable=False),
        ]
    )


def _validate_membership(
    authority: LayoutAuthority,
    rows: Sequence[MembershipRow],
    source_ids: Sequence[bytes] | None = None,
) -> list[MembershipRow]:
    concrete = list(rows)
    if len(concrete) != authority.rows:
        raise ValueError("membership row count differs")
    if source_ids is not None and len(source_ids) != authority.rows:
        raise ValueError("membership source identity count differs")
    if {row.source_ordinal for row in concrete} != set(range(authority.rows)):
        raise ValueError("membership source ordinals differ")
    if len({row.stable_id for row in concrete}) != authority.rows:
        raise ValueError("membership stable IDs differ")
    if source_ids is not None and any(
        row.stable_id != source_ids[row.source_ordinal] for row in concrete
    ):
        raise ValueError("membership source binding differs")

    expected_source_sha = bytes.fromhex(authority.source.sha256)
    construction_digests = {row.construction_sha256 for row in concrete}
    if len(construction_digests) != 1 or any(
        len(value) != 32 or value == bytes(32) for value in construction_digests
    ):
        raise ValueError("membership construction identity differs")

    pages: dict[int, list[MembershipRow]] = {}
    for row in concrete:
        if type(row.stable_id) is not bytes or not row.stable_id:
            raise ValueError("membership stable ID differs")
        if (
            row.method is not authority.method
            or row.source_sha256 != expected_source_sha
            or row.seed != authority.seed
        ):
            raise ValueError("membership authority binding differs")
        pages.setdefault(row.page_ordinal, []).append(row)
    if set(pages) != set(range(len(pages))):
        raise ValueError("membership page ordinals differ")

    for page_ordinal, page in pages.items():
        page.sort(key=lambda row: row.in_page_ordinal)
        if [row.in_page_ordinal for row in page] != list(range(len(page))):
            raise ValueError("membership in-page ordinals differ")
        if len(page) > authority.maximum_page_rows:
            raise ValueError("membership page row cap differs")
        if any(row.page_ordinal != page_ordinal or row.page_rows != len(page) for row in page):
            raise ValueError("membership page row authority differs")
        encoded_lengths = {row.encoded_page_bytes for row in page}
        if (
            len(encoded_lengths) != 1
            or next(iter(encoded_lengths)) <= 0
            or next(iter(encoded_lengths)) > authority.maximum_page_bytes
        ):
            raise ValueError("membership page byte authority differs")

    ordered = sorted(concrete, key=lambda row: (row.page_ordinal, row.in_page_ordinal))
    if concrete != ordered:
        raise ValueError("membership row order differs")
    return concrete


def write_membership_parquet(
    path: Path,
    authority: LayoutAuthority,
    rows: Sequence[MembershipRow],
) -> ArtifactIdentity:
    concrete = _validate_membership(authority, rows)
    schema = membership_schema()
    table = pa.Table.from_arrays(
        [
            pa.array([row.stable_id for row in concrete], type=pa.binary()),
            pa.array([row.source_ordinal for row in concrete], type=pa.uint32()),
            pa.array([row.page_ordinal for row in concrete], type=pa.uint32()),
            pa.array([row.in_page_ordinal for row in concrete], type=pa.uint16()),
            pa.array([row.page_rows for row in concrete], type=pa.uint16()),
            pa.array([row.encoded_page_bytes for row in concrete], type=pa.uint32()),
            pa.array([row.method.value for row in concrete], type=pa.string()),
            pa.array([row.source_sha256 for row in concrete], type=pa.binary(32)),
            pa.array([row.seed for row in concrete], type=pa.uint64()),
            pa.array(
                [row.construction_sha256 for row in concrete],
                type=pa.binary(32),
            ),
        ],
        schema=schema,
    )
    pq.write_table(
        table,
        path,
        version="2.6",
        compression="zstd",
        use_dictionary=False,
        write_statistics=True,
    )
    payload = path.read_bytes()
    return ArtifactIdentity(
        role="layout-membership",
        uri=path.resolve().as_uri(),
        sha256=hashlib.sha256(payload).hexdigest(),
        encoded_bytes=len(payload),
    )


def _column(table: pa.Table, name: str) -> list[Any]:
    return table[name].combine_chunks().to_pylist()


def read_membership_parquet(
    path: Path,
    authority: LayoutAuthority,
    source_ids: Sequence[bytes],
) -> list[MembershipRow]:
    if pq.read_schema(path) != membership_schema():
        raise ValueError("membership physical schema differs")
    table = pq.read_table(path)
    columns = {name: _column(table, name) for name in table.column_names}
    try:
        rows = [
            MembershipRow(
                stable_id=columns["stable_id"][index],
                source_ordinal=columns["source_ordinal"][index],
                page_ordinal=columns["page_ordinal"][index],
                in_page_ordinal=columns["in_page_ordinal"][index],
                page_rows=columns["page_rows"][index],
                encoded_page_bytes=columns["encoded_page_bytes"][index],
                method=LayoutMethod(columns["method"][index]),
                source_sha256=columns["source_sha256"][index],
                seed=columns["seed"][index],
                construction_sha256=columns["construction_sha256"][index],
            )
            for index in range(table.num_rows)
        ]
    except (KeyError, TypeError, ValueError) as error:
        raise ValueError("membership payload differs") from error
    return _validate_membership(authority, rows, source_ids)


def _projection_token(seed: int, index: int) -> bytes:
    return hashlib.sha256(
        seed.to_bytes(8, "little") + index.to_bytes(4, "little")
    ).digest()


def _srht_projection(vectors: np.ndarray, seed: int) -> np.ndarray:
    dimensions = vectors.shape[1]
    padded_dimensions = 1 << (dimensions - 1).bit_length()
    projected = np.zeros((vectors.shape[0], padded_dimensions), dtype=np.float32)
    projected[:, :dimensions] = vectors
    signs = np.asarray(
        [1.0 if _projection_token(seed, index)[0] & 1 == 0 else -1.0 for index in range(padded_dimensions)],
        dtype=np.float32,
    )
    projected *= signs
    width = 1
    while width < padded_dimensions:
        for start in range(0, padded_dimensions, width * 2):
            left = projected[:, start : start + width].copy()
            right = projected[:, start + width : start + width * 2].copy()
            projected[:, start : start + width] = left + right
            projected[:, start + width : start + width * 2] = left - right
        width *= 2
    projected *= np.float32(1.0 / math.sqrt(padded_dimensions))
    permutation = sorted(
        range(padded_dimensions),
        key=lambda index: (int.from_bytes(_projection_token(seed, index)[1:9], "little"), index),
    )
    return np.ascontiguousarray(projected[:, permutation[: min(dimensions, 192)]])


def encoded_sq8_page_bytes(
    source_ordinals: np.ndarray,
    stable_ids: Sequence[bytes],
    vectors: np.ndarray,
) -> int:
    selected = np.ascontiguousarray(vectors[source_ordinals], dtype=np.float32)
    low = selected.min(axis=0)
    high = selected.max(axis=0)
    step = np.asarray((high - low) / np.float32(255.0), dtype=np.float32)
    safe_step = np.where(step == 0.0, np.float32(1.0), step)
    codes = np.asarray(
        np.clip(np.rint((selected - low) / safe_step), 0, 255), dtype=np.uint8
    )
    embeddings = pa.FixedSizeListArray.from_arrays(
        pa.array(codes.reshape(-1), type=pa.uint8()), vectors.shape[1]
    )
    schema = pa.schema(
        [
            pa.field("stable_id", pa.binary(), nullable=False),
            pa.field("mutation_sequence", pa.uint64(), nullable=False),
            pa.field("row_state", pa.uint8(), nullable=False),
            pa.field(
                "sq8_embedding",
                pa.list_(
                    pa.field("element", pa.uint8(), nullable=False),
                    vectors.shape[1],
                ),
                nullable=False,
            ),
        ],
        metadata={
            b"sq8_low_f32_le": low.astype("<f4", copy=False).tobytes(),
            b"sq8_step_f32_le": step.astype("<f4", copy=False).tobytes(),
        },
    )
    table = pa.Table.from_arrays(
        [
            pa.array([stable_ids[int(index)] for index in source_ordinals], type=pa.binary()),
            pa.array(np.zeros(len(source_ordinals), dtype=np.uint64), type=pa.uint64()),
            pa.array(np.zeros(len(source_ordinals), dtype=np.uint8), type=pa.uint8()),
            embeddings,
        ],
        schema=schema,
    )
    sink = pa.BufferOutputStream()
    with pa.ipc.new_file(sink, table.schema) as writer:
        writer.write_table(table)
    return sink.getvalue().size


def _split_ordered_pages(
    order: np.ndarray,
    authority: LayoutAuthority,
    stable_ids: Sequence[bytes],
    vectors: np.ndarray,
) -> list[np.ndarray]:
    pending = [order[start : start + authority.maximum_page_rows] for start in range(0, len(order), authority.maximum_page_rows)]
    pages: list[np.ndarray] = []
    while pending:
        page = pending.pop(0)
        if encoded_sq8_page_bytes(page, stable_ids, vectors) <= authority.maximum_page_bytes:
            pages.append(page)
            continue
        if len(page) == 1:
            raise ValueError("one layout row exceeds the page byte cap")
        cut = len(page) // 2
        pending[0:0] = [page[:cut], page[cut:]]
    return pages


def _two_means_order(
    row_ordinals: np.ndarray,
    projected: np.ndarray,
    stable_ids: Sequence[bytes],
    cut: int,
) -> tuple[np.ndarray, np.ndarray]:
    ids = np.asarray([stable_ids[int(index)] for index in row_ordinals])
    seed_rank = int(np.argmin(ids))
    first = projected[int(row_ordinals[seed_rank])].astype(np.float64)
    seed_distances = np.sum(
        (projected[row_ordinals].astype(np.float64) - first) ** 2,
        axis=1,
    )
    second_rank = int(np.argmax(seed_distances))
    if not seed_distances[second_rank] > 0.0:
        raise ValueError("two-means geometry is unsplittable")
    centroids = np.stack(
        (first, projected[int(row_ordinals[second_rank])].astype(np.float64))
    )
    for _ in range(8):
        rows = projected[row_ordinals].astype(np.float64)
        distances = np.sum((rows[:, None, :] - centroids[None, :, :]) ** 2, axis=2)
        assignments = np.argmin(distances, axis=1)
        if not np.any(assignments == 0) or not np.any(assignments == 1):
            raise ValueError("two-means produced an empty cluster")
        centroids = np.stack(
            (
                rows[assignments == 0].mean(axis=0),
                rows[assignments == 1].mean(axis=0),
            )
        )
    rows = projected[row_ordinals].astype(np.float64)
    scores = np.sum((rows - centroids[1]) ** 2, axis=1) - np.sum(
        (rows - centroids[0]) ** 2, axis=1
    )
    if not np.isfinite(scores).all() or float(np.ptp(scores)) == 0.0:
        raise ValueError("two-means split scores differ")
    ranked = np.lexsort((ids, scores))
    if cut == 0 or cut == len(row_ordinals):
        raise ValueError("two-means split capacity differs")
    return row_ordinals[ranked[:cut]], row_ordinals[ranked[cut:]]


def _page_row_capacity(
    authority: LayoutAuthority,
    stable_ids: Sequence[bytes],
    vectors: np.ndarray,
) -> int:
    candidates = np.asarray(
        sorted(
            range(len(stable_ids)),
            key=lambda index: (-len(stable_ids[index]), stable_ids[index]),
        ),
        dtype=np.int64,
    )
    low = 0
    high = min(authority.maximum_page_rows, len(candidates))
    while low < high:
        middle = (low + high + 1) // 2
        if (
            encoded_sq8_page_bytes(candidates[:middle], stable_ids, vectors)
            <= authority.maximum_page_bytes
        ):
            low = middle
        else:
            high = middle - 1
    if low == 0:
        raise ValueError("one layout row exceeds the page byte cap")
    return low


def _two_means_pages(
    row_ordinals: np.ndarray,
    authority: LayoutAuthority,
    stable_ids: Sequence[bytes],
    vectors: np.ndarray,
    projected: np.ndarray,
    page_row_capacity: int,
) -> list[np.ndarray]:
    if (
        len(row_ordinals) <= page_row_capacity
        and encoded_sq8_page_bytes(row_ordinals, stable_ids, vectors)
        <= authority.maximum_page_bytes
    ):
        inside = sorted(row_ordinals, key=lambda index: stable_ids[int(index)])
        return [np.asarray(inside, dtype=np.int64)]
    page_count = math.ceil(len(row_ordinals) / page_row_capacity)
    left_page_count = page_count // 2
    cut = len(row_ordinals) * left_page_count // page_count
    left, right = _two_means_order(row_ordinals, projected, stable_ids, cut)
    return _two_means_pages(
        left, authority, stable_ids, vectors, projected, page_row_capacity
    ) + _two_means_pages(
        right, authority, stable_ids, vectors, projected, page_row_capacity
    )


def _construction_digest(
    authority: LayoutAuthority,
    rows: Sequence[MembershipRow],
) -> bytes:
    digest = hashlib.sha256()
    digest.update(authority.schema.encode())
    digest.update(authority.source.sha256.encode())
    digest.update(authority.method.value.encode())
    digest.update(authority.seed.to_bytes(8, "little"))
    for row in rows:
        digest.update(len(row.stable_id).to_bytes(4, "little"))
        digest.update(row.stable_id)
        digest.update(row.source_ordinal.to_bytes(4, "little"))
        digest.update(row.page_ordinal.to_bytes(4, "little"))
        digest.update(row.in_page_ordinal.to_bytes(2, "little"))
        digest.update(row.page_rows.to_bytes(2, "little"))
        digest.update(row.encoded_page_bytes.to_bytes(4, "little"))
    return digest.digest()


def construct_layout(
    authority: LayoutAuthority,
    stable_ids: Sequence[bytes],
    vectors: np.ndarray,
) -> list[MembershipRow]:
    if (
        len(stable_ids) != authority.rows
        or len(set(stable_ids)) != authority.rows
        or any(type(stable_id) is not bytes or not stable_id for stable_id in stable_ids)
    ):
        raise ValueError("layout source stable IDs differ")
    if (
        type(vectors) is not np.ndarray
        or vectors.dtype != np.float32
        or vectors.shape != (authority.rows, authority.dimensions)
        or not np.isfinite(vectors).all()
    ):
        raise ValueError("layout source vectors differ")
    geometry = vectors
    if authority.metric == "cosine":
        norms = np.linalg.norm(vectors.astype(np.float64), axis=1)
        if np.any(norms == 0.0) or not np.isfinite(norms).all():
            raise ValueError("layout cosine vector norms differ")
        geometry = np.asarray(vectors / norms[:, None], dtype=np.float32)

    source_ordinals = np.arange(authority.rows, dtype=np.int64)
    if authority.method is LayoutMethod.ID_ORDER_256:
        order = np.asarray(sorted(source_ordinals, key=lambda index: stable_ids[int(index)]))
        pages = _split_ordered_pages(order, authority, stable_ids, vectors)
    else:
        projected = _srht_projection(geometry, authority.seed)
        if authority.method is LayoutMethod.RANDOM_PROJECTION_256:
            order = np.asarray(
                sorted(
                    source_ordinals,
                    key=lambda index: (float(projected[int(index), 0]), stable_ids[int(index)]),
                )
            )
            pages = _split_ordered_pages(order, authority, stable_ids, vectors)
        elif authority.method in {LayoutMethod.TWO_MEANS_256, LayoutMethod.TWO_MEANS_480K}:
            page_row_capacity = _page_row_capacity(authority, stable_ids, vectors)
            pages = _two_means_pages(
                source_ordinals,
                authority,
                stable_ids,
                vectors,
                projected,
                page_row_capacity,
            )
        else:
            raise ValueError("layout method differs")

    source_sha = bytes.fromhex(authority.source.sha256)
    rows: list[MembershipRow] = []
    for page_ordinal, page in enumerate(pages):
        encoded_bytes = encoded_sq8_page_bytes(page, stable_ids, vectors)
        if encoded_bytes > authority.maximum_page_bytes:
            raise ValueError("layout page exceeds byte cap")
        for in_page_ordinal, source_ordinal in enumerate(page):
            rows.append(
                MembershipRow(
                    stable_id=stable_ids[int(source_ordinal)],
                    source_ordinal=int(source_ordinal),
                    page_ordinal=page_ordinal,
                    in_page_ordinal=in_page_ordinal,
                    page_rows=len(page),
                    encoded_page_bytes=encoded_bytes,
                    method=authority.method,
                    source_sha256=source_sha,
                    seed=authority.seed,
                    construction_sha256=bytes(32),
                )
            )
    construction_sha = _construction_digest(authority, rows)
    bound = [dataclasses.replace(row, construction_sha256=construction_sha) for row in rows]
    return _validate_membership(authority, bound, stable_ids)


def _source_arrays(path: Path, authority: LayoutAuthority) -> tuple[tuple[bytes, ...], np.ndarray]:
    payload = path.read_bytes()
    if (
        len(payload) != authority.source.encoded_bytes
        or hashlib.sha256(payload).hexdigest() != authority.source.sha256
    ):
        raise ValueError("layout source artifact identity differs")
    table = pq.read_table(path, columns=["feature_row_id", "embedding"])
    id_values = table["feature_row_id"].combine_chunks().to_pylist()
    stable_ids = tuple(_stable_id(value) for value in id_values)
    embedding = table["embedding"].combine_chunks()
    vectors = np.asarray(
        embedding.values.to_numpy(zero_copy_only=False), dtype=np.float32
    ).reshape(table.num_rows, authority.dimensions)
    return stable_ids, vectors


def _stable_id(value: object) -> bytes:
    if type(value) is bytes and value:
        return value
    if type(value) is int and 0 <= value < 1 << 128:
        return value.to_bytes(16, "big")
    raise ValueError("stable ID differs")


def _ground_truth(path: Path) -> tuple[tuple[bytes, ...], ...]:
    table = pq.read_table(path, columns=["query_ordinal", "rank", "feature_row_id"])
    query_ordinals = table["query_ordinal"].combine_chunks().to_pylist()
    ranks = table["rank"].combine_chunks().to_pylist()
    ids = table["feature_row_id"].combine_chunks().to_pylist()
    if table.num_rows == 0 or table.num_rows % 100 != 0:
        raise ValueError("layout GT100 row count differs")
    query_count = table.num_rows // 100
    if query_ordinals != [query for query in range(query_count) for _ in range(100)]:
        raise ValueError("layout GT100 query order differs")
    if ranks != list(range(100)) * query_count:
        raise ValueError("layout GT100 rank order differs")
    return tuple(
        tuple(_stable_id(value) for value in ids[start : start + 100])
        for start in range(0, len(ids), 100)
    )


def exact_page_coverage(
    page_hits: Mapping[int, int],
    page_bytes: Mapping[int, int],
    limits: EvaluationLimits,
    *,
    page_hits_at_10: Mapping[int, int] | None = None,
    query_ordinal: int = 0,
) -> QueryCoverageSample:
    if set(page_hits) != set(page_bytes) or (
        page_hits_at_10 is not None and set(page_hits_at_10) != set(page_hits)
    ):
        raise ValueError("coverage page authority differs")
    for page, hits in page_hits.items():
        if type(page) is not int or page < 0 or type(hits) is not int or not 0 <= hits <= 100:
            raise ValueError("coverage page hits differ")
        encoded = page_bytes[page]
        if type(encoded) is not int or encoded <= 0:
            raise ValueError("coverage page bytes differ")
        if page_hits_at_10 is not None:
            top_hits = page_hits_at_10[page]
            if type(top_hits) is not int or not 0 <= top_hits <= min(hits, 10):
                raise ValueError("coverage top-10 hits differ")

    states: dict[tuple[int, int], tuple[int, tuple[int, ...]]] = {(0, 0): (0, ())}
    for page in sorted(page_hits):
        additions: dict[tuple[int, int], tuple[int, tuple[int, ...]]] = {}
        for (count, hits), (encoded, selected) in states.items():
            if count == limits.maximum_pages:
                continue
            next_encoded = encoded + page_bytes[page]
            if next_encoded > limits.maximum_bytes:
                continue
            key = (count + 1, min(100, hits + page_hits[page]))
            value = (next_encoded, selected + (page,))
            current = states.get(key)
            pending = additions.get(key)
            best = pending if pending is not None and (current is None or pending < current) else current
            if best is None or value < best:
                additions[key] = value
        for key, value in additions.items():
            current = states.get(key)
            if current is None or value < current:
                states[key] = value

    candidates = [
        (hits, -encoded, tuple(-page for page in selected), encoded, selected)
        for (_, hits), (encoded, selected) in states.items()
    ]
    hits_at_100, _, _, encoded_bytes, selected_pages = max(candidates)
    hits_at_10 = (
        sum(page_hits_at_10[page] for page in selected_pages)
        if page_hits_at_10 is not None
        else 0
    )
    return QueryCoverageSample(
        query_ordinal=query_ordinal,
        selected_page_ordinals=selected_pages,
        hits_at_10=hits_at_10,
        hits_at_100=hits_at_100,
        encoded_bytes=encoded_bytes,
        recall_at_10_ppm=hits_at_10 * 100_000,
        recall_at_100_ppm=hits_at_100 * 10_000,
    )


def evaluate_layout(
    method: LayoutMethod,
    owner_by_id: Mapping[bytes, int],
    page_bytes: Mapping[int, int],
    ground_truth: Sequence[Sequence[bytes]],
    limits: EvaluationLimits,
) -> LayoutEvaluation:
    if not ground_truth:
        raise ValueError("layout ground truth is empty")
    samples: list[QueryCoverageSample] = []
    for query_ordinal, neighbors in enumerate(ground_truth):
        if len(neighbors) != 100 or len(set(neighbors)) != 100:
            raise ValueError("layout GT100 order differs")
        page_hits = {page: 0 for page in page_bytes}
        page_hits_at_10 = {page: 0 for page in page_bytes}
        for rank, stable_id in enumerate(neighbors):
            try:
                page = owner_by_id[stable_id]
            except KeyError as error:
                raise ValueError("layout ground truth references an unknown ID") from error
            if page not in page_bytes:
                raise ValueError("layout ground truth page differs")
            page_hits[page] += 1
            if rank < 10:
                page_hits_at_10[page] += 1
        samples.append(
            exact_page_coverage(
                page_hits,
                page_bytes,
                limits,
                page_hits_at_10=page_hits_at_10,
                query_ordinal=query_ordinal,
            )
        )

    query_count = len(samples)
    recall_at_10_ppm = sum(sample.hits_at_10 for sample in samples) * 1_000_000 // (
        query_count * 10
    )
    mean_recall_at_100_ppm = sum(sample.hits_at_100 for sample in samples) * 1_000_000 // (
        query_count * 100
    )
    ordered_recall = sorted(sample.recall_at_100_ppm for sample in samples)
    p05_index = math.ceil(0.05 * query_count) - 1
    p05_recall_at_100_ppm = ordered_recall[p05_index]
    worst_recall_at_100_ppm = ordered_recall[0]
    if method is LayoutMethod.ID_ORDER_256:
        decision = "control"
    elif mean_recall_at_100_ppm < 975_000 or p05_recall_at_100_ppm < 900_000:
        decision = "killed"
    elif mean_recall_at_100_ppm >= 990_000 and p05_recall_at_100_ppm >= 950_000:
        decision = "advance"
    else:
        decision = "insufficient"
    return LayoutEvaluation(
        method=method,
        samples=tuple(samples),
        recall_at_10_ppm=recall_at_10_ppm,
        mean_recall_at_100_ppm=mean_recall_at_100_ppm,
        p05_recall_at_100_ppm=p05_recall_at_100_ppm,
        worst_recall_at_100_ppm=worst_recall_at_100_ppm,
        decision=decision,
    )


def finalize_layout_screen(
    evaluations: Sequence[LayoutEvaluation],
) -> tuple[LayoutEvaluation, ...]:
    concrete = tuple(evaluations)
    expected_methods = set(LayoutMethod)
    if len(concrete) != len(expected_methods) or {item.method for item in concrete} != expected_methods:
        raise ValueError("layout screen arm roster differs")
    control = next(item for item in concrete if item.method is LayoutMethod.ID_ORDER_256)
    if (
        control.decision != "control"
        or abs(control.mean_recall_at_100_ppm - 613_770) > 10_000
        or abs(control.p05_recall_at_100_ppm - 470_000) > 10_000
        or abs(control.worst_recall_at_100_ppm - 410_000) > 10_000
    ):
        raise ValueError("layout ID-order reproduction control differs")
    for evaluation in concrete:
        if evaluation.method is LayoutMethod.ID_ORDER_256:
            continue
        if (
            evaluation.mean_recall_at_100_ppm < 975_000
            or evaluation.p05_recall_at_100_ppm < 900_000
        ):
            expected_decision = "killed"
        elif (
            evaluation.mean_recall_at_100_ppm >= 990_000
            and evaluation.p05_recall_at_100_ppm >= 950_000
        ):
            expected_decision = "advance"
        else:
            expected_decision = "insufficient"
        if evaluation.decision != expected_decision:
            raise ValueError("layout screen decision differs")
    return concrete


def coverage_schema() -> pa.Schema:
    return pa.schema(
        [
            pa.field("query_ordinal", pa.uint32(), nullable=False),
            pa.field(
                "selected_page_ordinals",
                pa.list_(pa.field("element", pa.uint32(), nullable=False)),
                nullable=False,
            ),
            pa.field("hits_at_10", pa.uint8(), nullable=False),
            pa.field("hits_at_100", pa.uint8(), nullable=False),
            pa.field("encoded_bytes", pa.uint32(), nullable=False),
            pa.field("recall_at_10_ppm", pa.uint32(), nullable=False),
            pa.field("recall_at_100_ppm", pa.uint32(), nullable=False),
        ]
    )


def write_coverage_parquet(path: Path, evaluation: LayoutEvaluation) -> ArtifactIdentity:
    schema = coverage_schema()
    samples = evaluation.samples
    table = pa.Table.from_arrays(
        [
            pa.array([sample.query_ordinal for sample in samples], type=pa.uint32()),
            pa.array(
                [list(sample.selected_page_ordinals) for sample in samples],
                type=schema.field("selected_page_ordinals").type,
            ),
            pa.array([sample.hits_at_10 for sample in samples], type=pa.uint8()),
            pa.array([sample.hits_at_100 for sample in samples], type=pa.uint8()),
            pa.array([sample.encoded_bytes for sample in samples], type=pa.uint32()),
            pa.array([sample.recall_at_10_ppm for sample in samples], type=pa.uint32()),
            pa.array([sample.recall_at_100_ppm for sample in samples], type=pa.uint32()),
        ],
        schema=schema,
    )
    pq.write_table(
        table,
        path,
        version="2.6",
        compression="zstd",
        use_dictionary=False,
        write_statistics=True,
    )
    payload = path.read_bytes()
    return ArtifactIdentity(
        role="layout-coverage-evidence",
        uri=path.resolve().as_uri(),
        sha256=hashlib.sha256(payload).hexdigest(),
        encoded_bytes=len(payload),
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subcommands = parser.add_subparsers(dest="command", required=True)
    construct = subcommands.add_parser(
        "construct",
        help="construct a query-blind page-layout membership artifact",
    )
    construct.add_argument("--authority", type=Path, required=True)
    construct.add_argument("--source", type=Path, required=True)
    construct.add_argument("--output", type=Path, required=True)
    evaluate = subcommands.add_parser(
        "evaluate",
        help="evaluate sealed membership against frozen GT100",
    )
    evaluate.add_argument("--authority", type=Path, required=True)
    evaluate.add_argument("--source", type=Path, required=True)
    evaluate.add_argument("--membership", type=Path, required=True)
    evaluate.add_argument("--truth", type=Path, required=True)
    evaluate.add_argument("--evidence", type=Path, required=True)
    evaluate.add_argument("--result", type=Path, required=True)
    return parser


def main() -> None:
    args = _parser().parse_args()
    authority_payload = json.loads(args.authority.read_text())
    authority = layout_authority_from_dict(authority_payload)
    stable_ids, vectors = _source_arrays(args.source, authority)
    if args.command == "construct":
        rows = construct_layout(authority, stable_ids, vectors)
        identity = write_membership_parquet(args.output, authority, rows)
    elif args.command == "evaluate":
        rows = read_membership_parquet(args.membership, authority, stable_ids)
        owner_by_id = {row.stable_id: row.page_ordinal for row in rows}
        page_bytes = {row.page_ordinal: row.encoded_page_bytes for row in rows}
        evaluation = evaluate_layout(
            authority.method,
            owner_by_id,
            page_bytes,
            _ground_truth(args.truth),
            EvaluationLimits(maximum_pages=32, maximum_bytes=16_777_216),
        )
        identity = write_coverage_parquet(args.evidence, evaluation)
        result = {
            "schema": "borsuk-native-geometric-layout-result-v1",
            "claim_eligible": False,
            "method": evaluation.method.value,
            "evidence": dataclasses.asdict(identity),
            "recall_at_10_ppm": evaluation.recall_at_10_ppm,
            "mean_recall_at_100_ppm": evaluation.mean_recall_at_100_ppm,
            "p05_recall_at_100_ppm": evaluation.p05_recall_at_100_ppm,
            "worst_recall_at_100_ppm": evaluation.worst_recall_at_100_ppm,
            "decision": evaluation.decision,
        }
        args.result.write_text(
            json.dumps(result, sort_keys=True, separators=(",", ":")) + "\n"
        )
    else:
        raise ValueError("layout command differs")
    print(
        json.dumps(dataclasses.asdict(identity), sort_keys=True, separators=(",", ":"))
    )


if __name__ == "__main__":
    main()
