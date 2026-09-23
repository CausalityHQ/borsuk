#!/usr/bin/env python3
"""Source-only V85 page representatives for one fixed 1M routing screen."""

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

from scripts.native_one_million_group_selector import (
    DIMENSIONS,
    GROUP_PAGES,
    MEMBERSHIP_DTYPE,
    ROW_BYTES,
    SOURCE_ROLES,
    Group,
    _identity,
    _source_schema,
)
from scripts.v97_row_width_screen import (
    ObjectIdentity,
    PageKey,
    _authenticate_object,
    _canonical_json_bytes,
    _read_page_run,
)

SCHEMA = "borsuk-one-million-page-selector-v1"


@dataclass(frozen=True, slots=True)
class PageSelectorArtifact:
    groups: tuple[Group, ...]
    page_centroids: np.ndarray
    page_groups: np.ndarray
    membership_ids: np.ndarray
    membership_groups: np.ndarray
    seal: dict[str, object]


def _page_membership(
    root: Path, identities: Mapping[str, ObjectIdentity]
) -> tuple[tuple[Group, ...], dict[int, int], np.ndarray, str]:
    body = (root / "generation.json").read_bytes()
    try:
        generation = json.loads(body)
    except json.JSONDecodeError as error:
        raise ValueError("page-selector generation differs") from error
    if (
        body != _canonical_json_bytes(generation)
        or generation.get("dimensions") != DIMENSIONS
        or type(generation.get("runs")) is not list
        or len(generation["runs"]) != 2
    ):
        raise ValueError("page-selector generation differs")
    groups: list[Group] = []
    id_to_page: dict[int, int] = {}
    page_groups: list[int] = []
    order = hashlib.sha256()
    for role_index, role in enumerate(("base", "delta")):
        run = generation["runs"][role_index]
        if type(run) is not dict or run.get("kind") != role:
            raise ValueError("page-selector run differs")
        page_map, pages, row_order = _read_page_run(
            root / (role + ".arrow"), run, role=role,
            dimensions=DIMENSIONS, identity=identities[role],
        )
        if len(page_map) != sum(len(ids) for ids in row_order.values()):
            raise ValueError("page-selector page rows differ")
        for ordinal in range(len(pages)):
            group_index = len(groups) + ordinal // GROUP_PAGES
            page_index = len(page_groups)
            page_groups.append(group_index)
            for row_id in row_order[PageKey(role, ordinal)]:
                if row_id < 0 or row_id in id_to_page:
                    raise ValueError("page-selector rows overlap")
                id_to_page[row_id] = page_index
                order.update(struct.pack("<BIQ", role_index, ordinal, row_id))
        for group_ordinal, first_page in enumerate(range(0, len(pages), GROUP_PAGES)):
            end_page = min(first_page + GROUP_PAGES, len(pages))
            row_count = sum(
                len(row_order[PageKey(role, ordinal)])
                for ordinal in range(first_page, end_page)
            )
            groups.append(Group(
                role, group_ordinal, first_page, end_page, row_count,
                4 + 4 * (end_page - first_page) + ROW_BYTES * row_count,
            ))
    if not groups or not id_to_page:
        raise ValueError("page-selector page rows differ")
    return tuple(groups), id_to_page, np.asarray(page_groups, dtype="<u4"), order.hexdigest()


def build_page_selector(
    root: Path, out: Path, identities: Mapping[str, ObjectIdentity],
    *, batch_rows: int = 4096,
) -> PageSelectorArtifact:
    if set(identities) != SOURCE_ROLES or not 1 <= batch_rows <= 4096:
        raise ValueError("page-selector source contract differs")
    for role, filename in (("source", "source.parquet"), ("generation", "generation.json"),
                           ("base", "base.arrow"), ("delta", "delta.arrow"),
                           ("router", "router.arrow")):
        _authenticate_object(role, root / filename, identities[role])
    groups, id_to_page, page_groups, order_sha = _page_membership(root, identities)
    source = pq.ParquetFile(root / "source.parquet")
    if source.schema_arrow != _source_schema():
        raise ValueError("page-selector source schema differs")
    sums = np.zeros((len(page_groups), DIMENSIONS), dtype=np.float64)
    counts = np.zeros(len(page_groups), dtype=np.int64)
    seen: set[int] = set()
    for batch in source.iter_batches(batch_size=batch_rows):
        table = pa.Table.from_batches([batch])
        ids = table.column("feature_row_id").combine_chunks().to_numpy()
        column = table.column("embedding").combine_chunks()
        vectors = np.asarray(column.values.to_numpy(), dtype=np.float32).reshape(-1, DIMENSIONS)
        if column.null_count or not np.isfinite(vectors).all():
            raise ValueError("page-selector source vectors differ")
        pages = np.empty(len(ids), dtype=np.int64)
        for index, raw in enumerate(ids):
            row_id = int(raw)
            if row_id not in id_to_page or row_id in seen:
                raise ValueError("page-selector source rows differ")
            seen.add(row_id)
            pages[index] = id_to_page[row_id]
        np.add.at(sums, pages, vectors.astype(np.float64))
        np.add.at(counts, pages, 1)
    if len(seen) != len(id_to_page) or np.any(counts <= 0):
        raise ValueError("page-selector source rows differ")
    centroids = np.asarray(sums / counts[:, None], dtype="<f2")
    if not np.isfinite(centroids).all():
        raise ValueError("page-selector centroids differ")
    membership = np.empty(len(id_to_page), dtype=MEMBERSHIP_DTYPE)
    for index, row_id in enumerate(sorted(id_to_page)):
        membership[index] = row_id, page_groups[id_to_page[row_id]]
    out.mkdir(parents=True, exist_ok=True)
    (out / "centroids.bin").write_bytes(centroids.tobytes(order="C"))
    (out / "membership.bin").write_bytes(membership.tobytes(order="C"))
    seal: dict[str, object] = {
        "schema": SCHEMA, "dimensions": DIMENSIONS, "group_pages": GROUP_PAGES,
        "row_bytes": ROW_BYTES,
        "source_identities": {role: dataclasses.asdict(identities[role]) for role in sorted(SOURCE_ROLES)},
        "page_order_sha256": order_sha,
        "groups": [dataclasses.asdict(group) for group in groups],
        "page_groups": page_groups.tolist(),
        "centroids": _identity(out / "centroids.bin"),
        "membership": _identity(out / "membership.bin"),
    }
    (out / "seal.json").write_bytes(_canonical_json_bytes(seal))
    return read_page_selector(out, identities)


def read_page_selector(out: Path, identities: Mapping[str, ObjectIdentity]) -> PageSelectorArtifact:
    body = (out / "seal.json").read_bytes()
    try:
        seal = json.loads(body)
        groups = tuple(Group(**entry) for entry in seal["groups"])
        page_groups = np.asarray(seal["page_groups"], dtype="<u4")
    except (KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
        raise ValueError("page-selector seal differs") from error
    page_counts = [group.end_page - group.first_page for group in groups]
    expected_page_groups = np.repeat(np.arange(len(groups), dtype="<u4"), page_counts)
    if (
        body != _canonical_json_bytes(seal)
        or set(seal) != {"schema", "dimensions", "group_pages", "row_bytes", "source_identities", "page_order_sha256", "groups", "page_groups", "centroids", "membership"}
        or seal["schema"] != SCHEMA
        or seal["dimensions"] != DIMENSIONS
        or seal["group_pages"] != GROUP_PAGES
        or seal["row_bytes"] != ROW_BYTES
        or seal["source_identities"] != {role: dataclasses.asdict(identities[role]) for role in sorted(SOURCE_ROLES)}
        or not groups
        or any(
            group.role not in ("base", "delta")
            or group.row_count <= 0
            or not 1 <= page_count <= GROUP_PAGES
            or group.code_bytes != 4 + 4 * page_count + ROW_BYTES * group.row_count
            for group, page_count in zip(groups, page_counts, strict=True)
        )
        or not np.array_equal(page_groups, expected_page_groups)
    ):
        raise ValueError("page-selector seal authority differs")
    for role, filename in (("centroids", "centroids.bin"), ("membership", "membership.bin")):
        if seal[role] != _identity(out / filename):
            raise ValueError(role + " identity differs")
    centroids = np.frombuffer((out / "centroids.bin").read_bytes(), dtype="<f2")
    membership = np.frombuffer((out / "membership.bin").read_bytes(), dtype=MEMBERSHIP_DTYPE)
    if (
        centroids.size != len(page_groups) * DIMENSIONS
        or not np.isfinite(centroids).all()
        or membership.size != sum(group.row_count for group in groups)
        or (membership.size and not np.all(membership["id"][1:] > membership["id"][:-1]))
        or (membership.size and int(np.max(membership["group"])) >= len(groups))
    ):
        raise ValueError("page-selector arrays differ")
    return PageSelectorArtifact(
        groups, centroids.reshape(len(page_groups), DIMENSIONS), page_groups,
        membership["id"], membership["group"], seal,
    )
