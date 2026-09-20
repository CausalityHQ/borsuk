#!/usr/bin/env python3
"""Bounded hierarchical row-score router for the authenticated G1 screen."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Literal

import numpy as np
import pyarrow as pa
import pyarrow.ipc as ipc

from scripts.v97_row_width_screen import (
    PQ16X8,
    PageKey,
    ScreenInputs,
    encode_pq,
    fit_pq,
    page_block_means,
)

_ROOT_KIND = 0
_PAGE_KIND = 1
_BASE_ROLE = 0
_DELTA_ROLE = 1
_NO_PARENT = (1 << 32) - 1


@dataclass(frozen=True, slots=True)
class HierarchyConfig:
    """The complete fixed-work contract for the V98 hierarchy."""

    pages_per_root: int
    maximum_root_groups: int
    maximum_exposed_pages: int
    retained_pages: int
    maximum_scanned_rows: int
    shortlist_rows: int
    maximum_gets: int
    maximum_bytes: int

    def __post_init__(self) -> None:
        values = (
            self.pages_per_root,
            self.maximum_root_groups,
            self.maximum_exposed_pages,
            self.retained_pages,
            self.maximum_scanned_rows,
            self.shortlist_rows,
            self.maximum_gets,
            self.maximum_bytes,
        )
        if (
            any(type(value) is not int or value <= 0 for value in values)
            or self.pages_per_root != 8
            or self.maximum_root_groups != 65_536
            or self.maximum_exposed_pages != 4_096
            or self.retained_pages != 1_024
            or self.maximum_scanned_rows != 262_144
            or self.shortlist_rows != 2_048
            or self.maximum_gets != 32
            or self.maximum_bytes != 16 * 1024**2
        ):
            raise ValueError("hierarchy configuration differs")


@dataclass(frozen=True, slots=True)
class ArrayIdentity:
    """Canonical identity of one dense hierarchy array."""

    bytes: int
    dtype: str
    shape: tuple[int, ...]
    sha256: str


@dataclass(frozen=True, slots=True)
class RootGroup:
    """One same-role contiguous group of physical pages."""

    role: Literal["base", "delta"]
    ordinal: int
    pages: tuple[PageKey, ...]
    child_offset: int
    child_count: int


@dataclass(frozen=True, slots=True)
class HierarchyRecord:
    """One decoded cross-language summary record."""

    kind: Literal["root", "page"]
    role: Literal["base", "delta"]
    entity_ordinal: int
    summary_slot: int
    parent_root: int | None
    child_offset: int
    child_count: int
    code: bytes


@dataclass(frozen=True, slots=True)
class HierarchyArtifact:
    """Authenticated in-memory hierarchy plus its Arrow IPC authority."""

    config: HierarchyConfig
    roots: tuple[RootGroup, ...]
    page_keys: tuple[PageKey, ...]
    summary_books: np.ndarray
    page_summary_codes: np.ndarray
    root_summary_codes: np.ndarray
    summary_books_identity: ArrayIdentity
    page_summary_codes_identity: ArrayIdentity
    root_summary_codes_identity: ArrayIdentity
    ipc_schema: pa.Schema
    ipc_bytes: bytes
    ipc_sha256: str


def hierarchy_ipc_schema() -> pa.Schema:
    """Return the exact non-null V98 hierarchy record schema."""

    return pa.schema(
        [
            pa.field("kind", pa.uint8(), nullable=False),
            pa.field("role", pa.uint8(), nullable=False),
            pa.field("entity_ordinal", pa.uint32(), nullable=False),
            pa.field("summary_slot", pa.uint8(), nullable=False),
            pa.field("parent_root", pa.uint32(), nullable=False),
            pa.field("child_offset", pa.uint32(), nullable=False),
            pa.field("child_count", pa.uint16(), nullable=False),
            pa.field(
                "code",
                pa.list_(pa.field("element", pa.uint8(), nullable=False), 16),
                nullable=False,
            ),
        ]
    )


def _array_identity(value: np.ndarray) -> ArrayIdentity:
    array = np.ascontiguousarray(value)
    body = array.tobytes(order="C")
    return ArrayIdentity(
        bytes=len(body),
        dtype=array.dtype.name,
        shape=tuple(int(size) for size in array.shape),
        sha256=hashlib.sha256(body).hexdigest(),
    )


def _validated_visible_rows(
    inputs: ScreenInputs,
) -> tuple[tuple[PageKey, ...], dict[int, int]]:
    source_ids = np.asarray(inputs.source_ids)
    vectors = np.asarray(inputs.vectors)
    page_keys = tuple(sorted(inputs.pages))
    if (
        source_ids.ndim != 1
        or not np.issubdtype(source_ids.dtype, np.integer)
        or vectors.ndim != 2
        or vectors.dtype != np.float32
        or vectors.shape[0] != source_ids.size
        or vectors.shape[1] != 16
        or not np.isfinite(vectors).all()
        or len(set(int(row_id) for row_id in source_ids)) != source_ids.size
        or set(page_keys) != set(inputs.row_order_by_page)
        or set(int(row_id) for row_id in source_ids) != set(inputs.page_by_id)
    ):
        raise ValueError("visible row roster differs")
    position_by_id = {
        int(row_id): position for position, row_id in enumerate(source_ids)
    }
    ordered_ids: list[int] = []
    for key in page_keys:
        page = inputs.pages[key]
        rows = tuple(int(row_id) for row_id in inputs.row_order_by_page[key])
        if (
            page.key != key
            or page.offset < 0
            or page.encoded_bytes <= 0
            or not rows
            or any(row_id not in position_by_id for row_id in rows)
            or any(inputs.page_by_id[row_id] != key for row_id in rows)
        ):
            raise ValueError("visible row roster differs")
        ordered_ids.extend(rows)
    if len(ordered_ids) != len(set(ordered_ids)) or set(ordered_ids) != set(
        position_by_id
    ):
        raise ValueError("visible row roster differs")
    return page_keys, position_by_id


def _vectors_by_page(
    inputs: ScreenInputs,
    page_keys: tuple[PageKey, ...],
    position_by_id: dict[int, int],
) -> dict[PageKey, np.ndarray]:
    vectors = np.asarray(inputs.vectors)
    return {
        key: np.ascontiguousarray(
            vectors[
                [position_by_id[int(row_id)] for row_id in inputs.row_order_by_page[key]]
            ]
        )
        for key in page_keys
    }


def _root_layout(
    page_keys: tuple[PageKey, ...], config: HierarchyConfig
) -> tuple[RootGroup, ...]:
    page_position = {key: position for position, key in enumerate(page_keys)}
    roots: list[RootGroup] = []
    for role in ("base", "delta"):
        role_pages = tuple(key for key in page_keys if key.object_role == role)
        for ordinal, start in enumerate(range(0, len(role_pages), config.pages_per_root)):
            pages = role_pages[start : start + config.pages_per_root]
            roots.append(
                RootGroup(
                    role=role,
                    ordinal=ordinal,
                    pages=pages,
                    child_offset=page_position[pages[0]],
                    child_count=len(pages),
                )
            )
    if not roots or len(roots) > config.maximum_root_groups:
        raise ValueError("root group differs")
    return tuple(roots)


def _root_means(
    roots: tuple[RootGroup, ...], vectors_by_page: dict[PageKey, np.ndarray]
) -> np.ndarray:
    means: list[np.ndarray] = []
    for root in roots:
        split = (len(root.pages) + 1) // 2
        first_pages = root.pages[:split]
        second_pages = root.pages[split:] if split < len(root.pages) else root.pages[:1]
        for half in (first_pages, second_pages):
            rows = np.concatenate([vectors_by_page[key] for key in half], axis=0)
            means.append(rows.mean(axis=0, dtype=np.float32))
    return np.ascontiguousarray(np.stack(means), dtype=np.float32)


def _ipc_bytes(
    roots: tuple[RootGroup, ...],
    page_keys: tuple[PageKey, ...],
    root_codes: np.ndarray,
    page_codes: np.ndarray,
) -> bytes:
    parent_by_page = {
        key: root_index
        for root_index, root in enumerate(roots)
        for key in root.pages
    }
    kind: list[int] = []
    role: list[int] = []
    entity_ordinal: list[int] = []
    summary_slot: list[int] = []
    parent_root: list[int] = []
    child_offset: list[int] = []
    child_count: list[int] = []
    codes: list[np.ndarray] = []
    for root_index, root in enumerate(roots):
        for slot in range(2):
            kind.append(_ROOT_KIND)
            role.append(_BASE_ROLE if root.role == "base" else _DELTA_ROLE)
            entity_ordinal.append(root.ordinal)
            summary_slot.append(slot)
            parent_root.append(_NO_PARENT)
            child_offset.append(root.child_offset)
            child_count.append(root.child_count)
            codes.append(root_codes[root_index * 2 + slot])
    for page_index, key in enumerate(page_keys):
        for slot in range(2):
            kind.append(_PAGE_KIND)
            role.append(_BASE_ROLE if key.object_role == "base" else _DELTA_ROLE)
            entity_ordinal.append(key.ordinal)
            summary_slot.append(slot)
            parent_root.append(parent_by_page[key])
            child_offset.append(0)
            child_count.append(0)
            codes.append(page_codes[page_index * 2 + slot])
    schema = hierarchy_ipc_schema()
    flat_codes = np.ascontiguousarray(np.stack(codes), dtype=np.uint8).reshape(-1)
    table = pa.Table.from_arrays(
        [
            pa.array(kind, type=pa.uint8()),
            pa.array(role, type=pa.uint8()),
            pa.array(entity_ordinal, type=pa.uint32()),
            pa.array(summary_slot, type=pa.uint8()),
            pa.array(parent_root, type=pa.uint32()),
            pa.array(child_offset, type=pa.uint32()),
            pa.array(child_count, type=pa.uint16()),
            pa.FixedSizeListArray.from_arrays(
                pa.array(flat_codes, type=pa.uint8()),
                16,
            ),
        ],
        schema=schema,
    )
    sink = pa.BufferOutputStream()
    with ipc.new_stream(sink, schema) as writer:
        writer.write_table(table)
    return sink.getvalue().to_pybytes()


def read_hierarchy_ipc(body: bytes) -> tuple[HierarchyRecord, ...]:
    """Strictly decode hierarchy records from their Arrow IPC authority."""

    if not isinstance(body, bytes) or not body:
        raise ValueError("hierarchy IPC bytes differ")
    try:
        reader = ipc.open_stream(pa.py_buffer(body))
        if reader.schema != hierarchy_ipc_schema():
            raise ValueError("hierarchy IPC schema differs")
        table = reader.read_all().combine_chunks()
    except ValueError:
        raise
    except (pa.ArrowException, OSError) as error:
        raise ValueError("hierarchy IPC bytes differ") from error
    columns = {
        name: table[name].combine_chunks().to_pylist() for name in table.column_names[:-1]
    }
    code_array = table["code"].combine_chunks()
    flat_codes = code_array.values.to_numpy(zero_copy_only=False).reshape(-1, 16)
    records: list[HierarchyRecord] = []
    for index in range(table.num_rows):
        kind_raw = columns["kind"][index]
        role_raw = columns["role"][index]
        slot = columns["summary_slot"][index]
        parent = columns["parent_root"][index]
        if kind_raw not in (_ROOT_KIND, _PAGE_KIND) or role_raw not in (
            _BASE_ROLE,
            _DELTA_ROLE,
        ) or slot not in (0, 1):
            raise ValueError("hierarchy IPC record differs")
        records.append(
            HierarchyRecord(
                kind="root" if kind_raw == _ROOT_KIND else "page",
                role="base" if role_raw == _BASE_ROLE else "delta",
                entity_ordinal=int(columns["entity_ordinal"][index]),
                summary_slot=int(slot),
                parent_root=None if parent == _NO_PARENT else int(parent),
                child_offset=int(columns["child_offset"][index]),
                child_count=int(columns["child_count"][index]),
                code=np.ascontiguousarray(flat_codes[index], dtype=np.uint8).tobytes(),
            )
        )
    return tuple(records)


def build_hierarchy(inputs: ScreenInputs, config: HierarchyConfig) -> HierarchyArtifact:
    """Build the deterministic query-blind V98 hierarchy."""

    page_keys, position_by_id = _validated_visible_rows(inputs)
    vectors_by_page = _vectors_by_page(inputs, page_keys, position_by_id)
    summary_keys, page_means = page_block_means(vectors_by_page)
    if summary_keys != tuple(key for key in page_keys for _ in range(2)):
        raise ValueError("page summary order differs")
    base_positions = [
        position
        for position, key in enumerate(summary_keys)
        if key.object_role == "base"
    ]
    summary_books = fit_pq(
        np.ascontiguousarray(page_means[base_positions]),
        PQ16X8,
        seed=inputs.seed ^ 0x53554D4D,
        sample_rows=inputs.training_rows,
        iterations=inputs.training_iterations,
    )
    page_codes = np.ascontiguousarray(encode_pq(page_means, summary_books, PQ16X8))
    roots = _root_layout(page_keys, config)
    root_means = _root_means(roots, vectors_by_page)
    root_codes = np.ascontiguousarray(encode_pq(root_means, summary_books, PQ16X8))
    ipc_body = _ipc_bytes(roots, page_keys, root_codes, page_codes)
    artifact = HierarchyArtifact(
        config=config,
        roots=roots,
        page_keys=page_keys,
        summary_books=summary_books,
        page_summary_codes=page_codes,
        root_summary_codes=root_codes,
        summary_books_identity=_array_identity(summary_books),
        page_summary_codes_identity=_array_identity(page_codes),
        root_summary_codes_identity=_array_identity(root_codes),
        ipc_schema=hierarchy_ipc_schema(),
        ipc_bytes=ipc_body,
        ipc_sha256=hashlib.sha256(ipc_body).hexdigest(),
    )
    validate_hierarchy(artifact, inputs, config)
    return artifact


def validate_hierarchy(
    artifact: HierarchyArtifact,
    inputs: ScreenInputs,
    config: HierarchyConfig,
) -> None:
    """Recompute and validate the hierarchy's complete Task-1 authority."""

    if artifact.config != config:
        raise ValueError("hierarchy configuration differs")
    page_keys, position_by_id = _validated_visible_rows(inputs)
    expected_roots = _root_layout(page_keys, config)
    if artifact.roots != expected_roots or artifact.page_keys != page_keys:
        raise ValueError("root group differs")
    expected_page_shape = (len(page_keys) * 2, 16)
    expected_root_shape = (len(expected_roots) * 2, 16)
    if (
        artifact.page_summary_codes.shape != expected_page_shape
        or artifact.page_summary_codes.dtype != np.uint8
    ):
        raise ValueError("page summary codes differ")
    if (
        artifact.root_summary_codes.shape != expected_root_shape
        or artifact.root_summary_codes.dtype != np.uint8
    ):
        raise ValueError("root summary codes differ")
    if artifact.summary_books.shape != (16, 256, 1) or artifact.summary_books.dtype != np.float32:
        raise ValueError("summary codebook differs")
    if artifact.page_summary_codes_identity != _array_identity(
        artifact.page_summary_codes
    ):
        raise ValueError("page summary identity differs")
    if artifact.root_summary_codes_identity != _array_identity(
        artifact.root_summary_codes
    ):
        raise ValueError("root summary identity differs")
    if artifact.summary_books_identity != _array_identity(artifact.summary_books):
        raise ValueError("summary codebook identity differs")

    vectors_by_page = _vectors_by_page(inputs, page_keys, position_by_id)
    summary_keys, page_means = page_block_means(vectors_by_page)
    base_positions = [
        position
        for position, key in enumerate(summary_keys)
        if key.object_role == "base"
    ]
    expected_books = fit_pq(
        np.ascontiguousarray(page_means[base_positions]),
        PQ16X8,
        seed=inputs.seed ^ 0x53554D4D,
        sample_rows=inputs.training_rows,
        iterations=inputs.training_iterations,
    )
    if not np.array_equal(artifact.summary_books, expected_books):
        raise ValueError("summary codebook differs")
    expected_page_codes = encode_pq(page_means, expected_books, PQ16X8)
    expected_root_codes = encode_pq(
        _root_means(expected_roots, vectors_by_page), expected_books, PQ16X8
    )
    if not np.array_equal(artifact.page_summary_codes, expected_page_codes):
        raise ValueError("page summary codes differ")
    if not np.array_equal(artifact.root_summary_codes, expected_root_codes):
        raise ValueError("root summary codes differ")
    if artifact.ipc_schema != hierarchy_ipc_schema():
        raise ValueError("hierarchy IPC schema differs")
    records = read_hierarchy_ipc(artifact.ipc_bytes)
    expected_ipc = _ipc_bytes(
        expected_roots,
        page_keys,
        artifact.root_summary_codes,
        artifact.page_summary_codes,
    )
    if (
        artifact.ipc_bytes != expected_ipc
        or artifact.ipc_sha256 != hashlib.sha256(expected_ipc).hexdigest()
        or len(records) != 2 * (len(expected_roots) + len(page_keys))
    ):
        raise ValueError("hierarchy IPC identity differs")
