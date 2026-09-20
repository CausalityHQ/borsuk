#!/usr/bin/env python3
"""Bounded hierarchical row-score router for the authenticated G1 screen."""

from __future__ import annotations

import hashlib
import heapq
from dataclasses import dataclass
from typing import Literal

import numpy as np
import pyarrow as pa
import pyarrow.ipc as ipc

from scripts.v97_row_width_screen import (
    PQ16X8,
    PageKey,
    PqSpec,
    ScreenInputs,
    adc_scores,
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
    page_row_count: int
    code: bytes


@dataclass(frozen=True, slots=True)
class HierarchyArtifact:
    """Authenticated in-memory hierarchy plus its Arrow IPC authority."""

    config: HierarchyConfig
    roots: tuple[RootGroup, ...]
    page_keys: tuple[PageKey, ...]
    page_row_counts: tuple[int, ...]
    summary_books: np.ndarray
    page_summary_codes: np.ndarray
    root_summary_codes: np.ndarray
    summary_books_identity: ArrayIdentity
    page_summary_codes_identity: ArrayIdentity
    root_summary_codes_identity: ArrayIdentity
    page_row_counts_identity: ArrayIdentity
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
            pa.field("page_row_count", pa.uint16(), nullable=False),
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
    page_row_counts: tuple[int, ...],
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
    page_row_count: list[int] = []
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
            page_row_count.append(0)
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
            page_row_count.append(page_row_counts[page_index])
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
            pa.array(page_row_count, type=pa.uint16()),
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
                page_row_count=int(columns["page_row_count"][index]),
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
    page_row_counts = tuple(
        len(inputs.row_order_by_page[key]) for key in page_keys
    )
    root_means = _root_means(roots, vectors_by_page)
    root_codes = np.ascontiguousarray(encode_pq(root_means, summary_books, PQ16X8))
    ipc_body = _ipc_bytes(
        roots, page_keys, page_row_counts, root_codes, page_codes
    )
    artifact = HierarchyArtifact(
        config=config,
        roots=roots,
        page_keys=page_keys,
        page_row_counts=page_row_counts,
        summary_books=summary_books,
        page_summary_codes=page_codes,
        root_summary_codes=root_codes,
        summary_books_identity=_array_identity(summary_books),
        page_summary_codes_identity=_array_identity(page_codes),
        root_summary_codes_identity=_array_identity(root_codes),
        page_row_counts_identity=_array_identity(
            np.asarray(page_row_counts, dtype=np.uint16)
        ),
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
    expected_page_row_counts = tuple(
        len(inputs.row_order_by_page[key]) for key in page_keys
    )
    if artifact.roots != expected_roots or artifact.page_keys != page_keys:
        raise ValueError("root group differs")
    if artifact.page_row_counts != expected_page_row_counts:
        raise ValueError("visible row roster differs")
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
    if artifact.page_row_counts_identity != _array_identity(
        np.asarray(artifact.page_row_counts, dtype=np.uint16)
    ):
        raise ValueError("page row-count identity differs")

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
        expected_page_row_counts,
        artifact.root_summary_codes,
        artifact.page_summary_codes,
    )
    if (
        artifact.ipc_bytes != expected_ipc
        or artifact.ipc_sha256 != hashlib.sha256(expected_ipc).hexdigest()
        or len(records) != 2 * (len(expected_roots) + len(page_keys))
    ):
        raise ValueError("hierarchy IPC identity differs")


@dataclass(frozen=True, slots=True)
class HierarchyFence:
    """Bounded page hierarchy selected for one query."""

    exposed_pages: tuple[PageKey, ...]
    retained_pages: tuple[PageKey, ...]
    root_evaluations: int
    page_evaluations: int
    scanned_rows: int


def _bounded_total_order(
    scores: np.ndarray,
    keys: tuple[PageKey, ...] | tuple[tuple[int, int], ...],
    count: int,
) -> tuple[int, ...]:
    values = np.asarray(scores)
    if (
        values.ndim != 1
        or values.size != len(keys)
        or not np.issubdtype(values.dtype, np.floating)
        or not np.isfinite(values).all()
        or type(count) is not int
        or count <= 0
        or count > values.size
    ):
        raise ValueError("bounded total-order input differs")
    if count == values.size:
        candidates = np.arange(values.size, dtype=np.int64)
    else:
        threshold = np.partition(values, count - 1)[count - 1]
        lower = np.flatnonzero(values < threshold)
        equal = np.flatnonzero(values == threshold)
        remaining = count - lower.size
        ordered_equal = sorted(equal.tolist(), key=lambda index: keys[index])
        candidates = np.concatenate(
            (lower, np.asarray(ordered_equal[:remaining], dtype=np.int64))
        )
    return tuple(
        sorted(
            (int(index) for index in candidates),
            key=lambda index: (float(values[index]), keys[index]),
        )
    )


def route_hierarchy(
    query: np.ndarray,
    artifact: HierarchyArtifact,
    config: HierarchyConfig,
) -> HierarchyFence:
    """Select a deterministic bounded set of pages through both levels."""

    vector = np.asarray(query)
    if (
        artifact.config != config
        or vector.ndim != 1
        or vector.dtype != np.float32
        or not np.isfinite(vector).all()
        or len(artifact.roots) == 0
        or len(artifact.roots) > config.maximum_root_groups
        or len(artifact.page_keys) != len(artifact.page_row_counts)
        or artifact.root_summary_codes.shape != (len(artifact.roots) * 2, 16)
        or artifact.page_summary_codes.shape != (len(artifact.page_keys) * 2, 16)
    ):
        raise ValueError("hierarchy routing input differs")
    root_scores = adc_scores(
        vector, artifact.summary_books, artifact.root_summary_codes, PQ16X8
    ).reshape(-1, 2).min(axis=1)
    root_keys = tuple(
        (0 if root.role == "base" else 1, root.ordinal)
        for root in artifact.roots
    )
    root_candidate_count = min(len(artifact.roots), config.maximum_exposed_pages)
    root_order = _bounded_total_order(
        root_scores, root_keys, root_candidate_count
    )
    exposed: list[PageKey] = []
    for root_index in root_order:
        pages = artifact.roots[root_index].pages
        if len(exposed) + len(pages) > config.maximum_exposed_pages:
            break
        exposed.extend(pages)
    if not exposed:
        raise ValueError("hierarchy exposed no pages")
    page_position = {key: index for index, key in enumerate(artifact.page_keys)}
    try:
        exposed_positions = np.asarray(
            [page_position[key] for key in exposed], dtype=np.int64
        )
    except KeyError as error:
        raise ValueError("hierarchy root child differs") from error
    code_positions = np.stack(
        (exposed_positions * 2, exposed_positions * 2 + 1), axis=1
    ).reshape(-1)
    page_scores = adc_scores(
        vector,
        artifact.summary_books,
        np.ascontiguousarray(artifact.page_summary_codes[code_positions]),
        PQ16X8,
    ).reshape(-1, 2).min(axis=1)
    retained_count = min(config.retained_pages, len(exposed))
    page_order = _bounded_total_order(page_scores, tuple(exposed), retained_count)
    retained = tuple(exposed[index] for index in page_order)
    scanned_rows = sum(
        artifact.page_row_counts[page_position[key]] for key in retained
    )
    if scanned_rows > config.maximum_scanned_rows:
        raise ValueError("hierarchy scanned-row cap differs")
    return HierarchyFence(
        exposed_pages=tuple(exposed),
        retained_pages=retained,
        root_evaluations=len(artifact.roots),
        page_evaluations=len(exposed),
        scanned_rows=scanned_rows,
    )


def _push_best(
    heap: list[tuple[float, int, int]],
    score: float,
    row_id: int,
    count: int,
) -> None:
    item = (-score, -row_id, row_id)
    if len(heap) < count:
        heapq.heappush(heap, item)
        return
    worst = (-heap[0][0], -heap[0][1])
    if (score, row_id) < worst:
        heapq.heapreplace(heap, item)


def blockwise_top_rows(
    scores: np.ndarray,
    row_ids: np.ndarray,
    count: int,
    *,
    block_rows: int = 8_192,
) -> tuple[int, ...]:
    """Return row IDs under exact `(score,row_id)` order with bounded storage."""

    values = np.asarray(scores)
    ids = np.asarray(row_ids)
    if (
        values.ndim != 1
        or ids.ndim != 1
        or values.shape != ids.shape
        or not np.issubdtype(values.dtype, np.floating)
        or not np.issubdtype(ids.dtype, np.integer)
        or not np.isfinite(values).all()
        or len(set(int(row_id) for row_id in ids)) != ids.size
        or type(count) is not int
        or not 0 < count <= ids.size
        or type(block_rows) is not int
        or block_rows <= 0
    ):
        raise ValueError("bounded row-score input differs")
    heap: list[tuple[float, int, int]] = []
    for start in range(0, ids.size, block_rows):
        stop = min(start + block_rows, ids.size)
        for score, row_id in zip(
            values[start:stop].tolist(), ids[start:stop].tolist(), strict=True
        ):
            _push_best(heap, float(score), int(row_id), count)
    return tuple(
        row_id
        for _, row_id in sorted(
            ((-negative_score, stored_id) for negative_score, _, stored_id in heap)
        )
    )


def score_retained_rows(
    query: np.ndarray,
    row_ids: np.ndarray,
    row_codes: np.ndarray,
    books: np.ndarray,
    spec: PqSpec,
    *,
    maximum_rows: int,
    shortlist_rows: int,
    block_rows: int = 8_192,
) -> tuple[int, ...]:
    """ADC-score retained rows without materializing all `(score,id)` pairs."""

    vector = np.asarray(query)
    ids = np.asarray(row_ids)
    codes = np.asarray(row_codes)
    if (
        vector.ndim != 1
        or vector.dtype != np.float32
        or not np.isfinite(vector).all()
        or ids.ndim != 1
        or not np.issubdtype(ids.dtype, np.integer)
        or ids.size == 0
        or ids.size > maximum_rows
        or len(set(int(row_id) for row_id in ids)) != ids.size
        or codes.ndim != 2
        or codes.shape[0] != ids.size
        or type(shortlist_rows) is not int
        or not 0 < shortlist_rows <= ids.size
        or type(block_rows) is not int
        or block_rows <= 0
    ):
        raise ValueError("retained row-score input differs")
    heap: list[tuple[float, int, int]] = []
    try:
        for start in range(0, ids.size, block_rows):
            stop = min(start + block_rows, ids.size)
            block_scores = adc_scores(
                vector,
                books,
                np.ascontiguousarray(codes[start:stop]),
                spec,
            )
            for score, row_id in zip(
                block_scores.tolist(), ids[start:stop].tolist(), strict=True
            ):
                _push_best(heap, float(score), int(row_id), shortlist_rows)
    except ValueError as error:
        raise ValueError("retained row-score input differs") from error
    return tuple(
        row_id
        for _, row_id in sorted(
            ((-negative_score, stored_id) for negative_score, _, stored_id in heap)
        )
    )
