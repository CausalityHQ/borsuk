#!/usr/bin/env python3
"""Query-blind geometric page-layout screen for the native ANN redesign."""

from __future__ import annotations

import dataclasses
import enum
import hashlib
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

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
