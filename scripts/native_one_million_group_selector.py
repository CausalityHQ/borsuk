#!/usr/bin/env python3
"""Source-only adjacent-page centroids for one fixed ReLAION-1M selector gate."""

from __future__ import annotations

import dataclasses
import hashlib
import json
import struct
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from scripts.v97_row_width_screen import (
    ObjectIdentity,
    PageKey,
    _authenticate_object,
    _canonical_json_bytes,
    _read_page_run,
)

DIMENSIONS = 768
GROUP_PAGES = 8
ROW_BYTES = 200
SCHEMA = "borsuk-one-million-group-selector-v1"
MEMBERSHIP_DTYPE = np.dtype([("id", "<i8"), ("group", "<u4")])
SOURCE_ROLES = frozenset({"source", "generation", "base", "delta", "router"})


@dataclass(frozen=True, slots=True)
class Group:
    role: str
    ordinal: int
    first_page: int
    end_page: int
    row_count: int
    code_bytes: int


@dataclass(frozen=True, slots=True)
class SelectorArtifact:
    groups: tuple[Group, ...]
    centroids: np.ndarray
    membership_ids: np.ndarray
    membership_groups: np.ndarray
    seal: dict[str, object]


def _identity(path: Path) -> dict[str, object]:
    body = path.read_bytes()
    return {"bytes": len(body), "sha256": hashlib.sha256(body).hexdigest()}


def _source_schema() -> pa.Schema:
    return pa.schema([
        pa.field("feature_row_id", pa.uint64(), nullable=False),
        pa.field("embedding", pa.list_(pa.field("item", pa.float32(), nullable=False), DIMENSIONS), nullable=False),
    ])


def _page_map(root: Path, identities: Mapping[str, ObjectIdentity]) -> tuple[tuple[Group, ...], dict[int, int], str]:
    body = (root / "generation.json").read_bytes()
    try:
        generation = json.loads(body)
    except json.JSONDecodeError as error:
        raise ValueError("generation authority differs") from error
    if (
        body != _canonical_json_bytes(generation)
        or generation.get("dimensions") != DIMENSIONS
        or type(generation.get("runs")) is not list
        or len(generation["runs"]) != 2
    ):
        raise ValueError("generation authority differs")
    groups: list[Group] = []
    id_to_group: dict[int, int] = {}
    ordered = hashlib.sha256()
    for role_index, role in enumerate(("base", "delta")):
        run = generation["runs"][role_index]
        if type(run) is not dict or run.get("kind") != role:
            raise ValueError("generation run order differs")
        page_map, pages, row_order = _read_page_run(
            root / (role + ".arrow"), run, role=role,
            dimensions=DIMENSIONS, identity=identities[role],
        )
        if len(page_map) != sum(len(ids) for ids in row_order.values()):
            raise ValueError("page rows differ")
        page_count = len(pages)
        for group_ordinal, first_page in enumerate(range(0, page_count, GROUP_PAGES)):
            end_page = min(first_page + GROUP_PAGES, page_count)
            group_index = len(groups)
            row_count = 0
            for page_ordinal in range(first_page, end_page):
                for row_id in row_order[PageKey(role, page_ordinal)]:
                    if row_id < 0 or row_id in id_to_group:
                        raise ValueError("page rows overlap")
                    id_to_group[row_id] = group_index
                    ordered.update(struct.pack("<BIQ", role_index, page_ordinal, row_id))
                    row_count += 1
            groups.append(Group(
                role, group_ordinal, first_page, end_page, row_count,
                4 + 4 * (end_page - first_page) + ROW_BYTES * row_count,
            ))
    if not groups or not id_to_group:
        raise ValueError("page rows differ")
    return tuple(groups), id_to_group, ordered.hexdigest()


def build_selector(
    root: Path,
    out: Path,
    identities: Mapping[str, ObjectIdentity],
    *,
    batch_rows: int = 4096,
) -> SelectorArtifact:
    """Authenticate V85 pages and stream source vectors into group centroids."""
    if set(identities) != SOURCE_ROLES or not 1 <= batch_rows <= 4096:
        raise ValueError("selector source contract differs")
    for role, filename in (
        ("source", "source.parquet"), ("generation", "generation.json"),
        ("base", "base.arrow"), ("delta", "delta.arrow"), ("router", "router.arrow"),
    ):
        _authenticate_object(role, root / filename, identities[role])
    groups, id_to_group, page_order_sha = _page_map(root, identities)
    parquet = pq.ParquetFile(root / "source.parquet")
    if parquet.schema_arrow != _source_schema():
        raise ValueError("source schema differs")
    sums = np.zeros((len(groups), DIMENSIONS), dtype=np.float64)
    counts = np.zeros(len(groups), dtype=np.int64)
    seen: set[int] = set()
    for batch in parquet.iter_batches(batch_size=batch_rows):
        table = pa.Table.from_batches([batch])
        ids = table.column("feature_row_id").combine_chunks().to_numpy()
        vectors_array = table.column("embedding").combine_chunks()
        vectors = np.asarray(vectors_array.values.to_numpy(), dtype=np.float32).reshape(-1, DIMENSIONS)
        if vectors_array.null_count or not np.isfinite(vectors).all():
            raise ValueError("source vectors differ")
        batch_groups = np.empty(len(ids), dtype=np.int64)
        for index, value in enumerate(ids):
            row_id = int(value)
            if row_id not in id_to_group or row_id in seen:
                raise ValueError("source rows differ")
            seen.add(row_id)
            batch_groups[index] = id_to_group[row_id]
        np.add.at(sums, batch_groups, vectors.astype(np.float64))
        np.add.at(counts, batch_groups, 1)
    if len(seen) != len(id_to_group) or any(counts[index] != group.row_count for index, group in enumerate(groups)):
        raise ValueError("source rows differ")
    centroids = np.asarray(sums / counts[:, None], dtype="<f2")
    if not np.isfinite(centroids).all():
        raise ValueError("centroids differ")
    membership = np.empty(len(id_to_group), dtype=MEMBERSHIP_DTYPE)
    for index, row_id in enumerate(sorted(id_to_group)):
        membership[index] = (row_id, id_to_group[row_id])
    out.mkdir(parents=True, exist_ok=True)
    (out / "centroids.bin").write_bytes(centroids.tobytes(order="C"))
    (out / "membership.bin").write_bytes(membership.tobytes(order="C"))
    seal: dict[str, object] = {
        "schema": SCHEMA,
        "dimensions": DIMENSIONS,
        "group_pages": GROUP_PAGES,
        "row_bytes": ROW_BYTES,
        "source_identities": {role: dataclasses.asdict(identities[role]) for role in sorted(SOURCE_ROLES)},
        "page_order_sha256": page_order_sha,
        "groups": [dataclasses.asdict(group) for group in groups],
        "centroids": _identity(out / "centroids.bin"),
        "membership": _identity(out / "membership.bin"),
    }
    (out / "seal.json").write_bytes(_canonical_json_bytes(seal))
    return read_selector(out, identities)


def read_selector(out: Path, identities: Mapping[str, ObjectIdentity]) -> SelectorArtifact:
    """Read only the canonical source seal and its exact fixed-width arrays."""
    body = (out / "seal.json").read_bytes()
    try:
        seal = json.loads(body)
        groups = tuple(Group(**group) for group in seal["groups"])
    except (KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
        raise ValueError("selector seal differs") from error
    if (
        body != _canonical_json_bytes(seal)
        or set(seal) != {"schema", "dimensions", "group_pages", "row_bytes", "source_identities", "page_order_sha256", "groups", "centroids", "membership"}
        or seal["schema"] != SCHEMA
        or seal["dimensions"] != DIMENSIONS
        or seal["group_pages"] != GROUP_PAGES
        or seal["row_bytes"] != ROW_BYTES
        or seal["source_identities"] != {role: dataclasses.asdict(identities[role]) for role in sorted(SOURCE_ROLES)}
        or type(seal["page_order_sha256"]) is not str
        or len(seal["page_order_sha256"]) != 64
        or not groups
    ):
        raise ValueError("selector seal authority differs")
    for role, name in (("centroids", "centroids.bin"), ("membership", "membership.bin")):
        if seal[role] != _identity(out / name):
            raise ValueError(role + " identity differs")
    centroids = np.frombuffer((out / "centroids.bin").read_bytes(), dtype="<f2")
    membership = np.frombuffer((out / "membership.bin").read_bytes(), dtype=MEMBERSHIP_DTYPE)
    if (
        centroids.size != len(groups) * DIMENSIONS
        or not np.isfinite(centroids).all()
        or membership.size != sum(group.row_count for group in groups)
        or any(group.role not in ("base", "delta") or group.row_count <= 0 or group.end_page <= group.first_page or group.end_page - group.first_page > GROUP_PAGES or group.code_bytes != 4 + 4 * (group.end_page - group.first_page) + ROW_BYTES * group.row_count for group in groups)
        or (membership.size and not np.all(membership["id"][1:] > membership["id"][:-1]))
        or (membership.size and int(np.max(membership["group"])) >= len(groups))
    ):
        raise ValueError("selector array authority differs")
    return SelectorArtifact(
        groups, centroids.reshape(len(groups), DIMENSIONS),
        membership["id"], membership["group"], seal,
    )
