#!/usr/bin/env python3
"""Claim-ineligible PQ16 page-nomination fail-fast screen for V85.

The screen reads authenticated local artifacts only.  It trains PQ16 from the
base corpus without queries or truth, ranks base rows with ADC, maps the fixed
top-2,048 rows to the existing physical pages, and treats the authenticated
delta tier as resident.  It measures page containment and planned S3 range
work; it does not claim serving latency or page-SQ8 rerank quality.
"""

from __future__ import annotations

import argparse
import dataclasses
import hashlib
import json
import pathlib
from typing import Any

import numpy as np
import pyarrow as pa
import pyarrow.ipc as ipc
import pyarrow.parquet as pq


@dataclasses.dataclass(frozen=True)
class PageEntry:
    offset: int
    encoded_bytes: int


@dataclasses.dataclass(frozen=True)
class SparseResidualPq8:
    selected_indices: np.ndarray
    books: np.ndarray
    codes: np.ndarray
    combined_norms: np.ndarray


def _sha256_file(path: pathlib.Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(value, separators=(",", ":"), sort_keys=True).encode() + b"\n"


def _coalesced_work(
    selected_pages: list[int], page_entries: dict[int, PageEntry], gap_pages: int
) -> tuple[int, int]:
    if not selected_pages:
        return 0, 0
    groups: list[list[int]] = [[selected_pages[0]]]
    for page in selected_pages[1:]:
        if page - groups[-1][-1] <= gap_pages + 1:
            groups[-1].append(page)
        else:
            groups.append([page])
    encoded_bytes = 0
    for group in groups:
        first = page_entries[group[0]]
        last = page_entries[group[-1]]
        encoded_bytes += last.offset + last.encoded_bytes - first.offset
    return len(groups), encoded_bytes


def order_pages_by_pq_cooccurrence(
    *,
    ranked_base_ids: np.ndarray,
    base_page_by_id: dict[int, int],
    page_count: int,
    unique_pages_per_query: int,
) -> list[int]:
    """Order pages by query-independent PQ shortlist co-occurrence."""

    if (
        ranked_base_ids.ndim != 2
        or ranked_base_ids.shape[0] == 0
        or ranked_base_ids.shape[1] == 0
        or page_count <= 0
        or unique_pages_per_query <= 1
    ):
        raise ValueError("PQ cooccurrence layout shape differs")
    adjacency: list[dict[int, int]] = [dict() for _ in range(page_count)]
    for ranked_ids in ranked_base_ids:
        pages = []
        seen = set()
        for row_id in ranked_ids:
            page = base_page_by_id.get(int(row_id))
            if page is None or not 0 <= page < page_count:
                raise ValueError("PQ cooccurrence row page differs")
            if page not in seen:
                seen.add(page)
                pages.append(page)
                if len(pages) == unique_pages_per_query:
                    break
        for left_rank, left in enumerate(pages):
            for right_rank in range(left_rank + 1, len(pages)):
                right = pages[right_rank]
                weight = 1_000_000 // (right_rank + 1)
                adjacency[left][right] = adjacency[left].get(right, 0) + weight
                adjacency[right][left] = adjacency[right].get(left, 0) + weight

    degree = [sum(neighbors.values()) for neighbors in adjacency]
    frontier = [0] * page_count
    unvisited = set(range(page_count))
    order = []
    while unvisited:
        connected = [page for page in unvisited if frontier[page] > 0]
        if connected:
            page = min(
                connected, key=lambda value: (-frontier[value], -degree[value], value)
            )
        else:
            page = min(unvisited, key=lambda value: (-degree[value], value))
        order.append(page)
        unvisited.remove(page)
        for neighbor, weight in adjacency[page].items():
            if neighbor in unvisited:
                frontier[neighbor] += weight
    return order


def remap_page_layout(
    *,
    base_page_by_id: dict[int, int],
    page_entries: dict[int, PageEntry],
    old_pages_in_new_order: list[int],
) -> tuple[dict[int, int], dict[int, PageEntry]]:
    """Apply a page permutation without changing page membership or bytes."""

    page_count = len(page_entries)
    if sorted(page_entries) != list(range(page_count)) or sorted(
        old_pages_in_new_order
    ) != list(range(page_count)):
        raise ValueError("PQ cooccurrence page permutation differs")
    old_to_new = {
        old_page: new_page for new_page, old_page in enumerate(old_pages_in_new_order)
    }
    remapped_ids = {}
    for row_id, old_page in base_page_by_id.items():
        if old_page not in old_to_new:
            raise ValueError("PQ cooccurrence row membership differs")
        remapped_ids[row_id] = old_to_new[old_page]
    remapped_entries = {}
    offset = 0
    for new_page, old_page in enumerate(old_pages_in_new_order):
        encoded_bytes = page_entries[old_page].encoded_bytes
        remapped_entries[new_page] = PageEntry(
            offset=offset, encoded_bytes=encoded_bytes
        )
        offset += encoded_bytes
    return remapped_ids, remapped_entries


def plan_rank_weighted_ranges(
    *,
    ranked_base_ids: np.ndarray,
    base_page_by_id: dict[int, int],
    page_count: int,
    max_span_pages: int,
    max_ranges: int,
) -> list[tuple[int, int]]:
    """Select the exact maximum reciprocal-rank page evidence under a budget."""

    if (
        ranked_base_ids.ndim != 1
        or ranked_base_ids.size == 0
        or page_count <= 0
        or max_span_pages <= 0
        or max_ranges <= 0
    ):
        raise ValueError("rank-weighted page plan differs")
    weights = np.zeros(page_count, dtype=np.float64)
    for rank, row_id in enumerate(ranked_base_ids):
        page = base_page_by_id.get(int(row_id))
        if page is None or not 0 <= page < page_count:
            raise ValueError("rank-weighted row page differs")
        weights[page] += 1_000_000_000 // (rank + 1)

    unreachable = np.float64(-np.inf)
    shape = (max_ranges + 1, max_span_pages + 1)
    off = np.full(shape, unreachable, dtype=np.float64)
    on = np.full(shape, unreachable, dtype=np.float64)
    off[0, 0] = 0
    off_choices = np.zeros((page_count, *shape), dtype=np.uint8)
    on_choices = np.zeros((page_count, *shape), dtype=np.uint8)
    for page, weight in enumerate(weights):
        next_off = np.maximum(off, on)
        off_choices[page] = on > off
        next_on = np.full(shape, unreachable, dtype=np.float64)
        continuing = on[:, :-1]
        next_on[:, 1:] = continuing
        on_choices[page, :, 1:][continuing != unreachable] = 1
        starting = off[:-1, :-1]
        replace = starting > next_on[1:, 1:]
        next_on[1:, 1:][replace] = starting[replace]
        on_choices[page, 1:, 1:][replace] = 2
        reachable = next_on != unreachable
        next_on[reachable] += weight
        off, on = next_off, next_on

    best_value = unreachable
    best = (0, 0, 0)
    for pages in range(max_span_pages + 1):
        for ranges in range(max_ranges + 1):
            for state, values in enumerate((off, on)):
                value = values[ranges, pages]
                if value > best_value:
                    best_value = value
                    best = (ranges, pages, state)
    ranges, pages, state = best
    selected = np.zeros(page_count, dtype=np.bool_)
    for page in range(page_count - 1, -1, -1):
        if state == 0:
            state = int(off_choices[page, ranges, pages])
            continue
        selected[page] = True
        choice = int(on_choices[page, ranges, pages])
        pages -= 1
        if choice == 2:
            ranges -= 1
            state = 0
        elif choice == 1:
            state = 1
        else:
            raise AssertionError("rank-weighted page backtrack differs")
    selected_pages = np.flatnonzero(selected)
    if selected_pages.size == 0:
        return []
    breaks = np.flatnonzero(np.diff(selected_pages) > 1)
    starts = np.concatenate(([selected_pages[0]], selected_pages[breaks + 1]))
    ends = np.concatenate((selected_pages[breaks], [selected_pages[-1]]))
    return [(int(starts[index]), int(ends[index])) for index in range(starts.size)]


def evaluate_page_nominations(
    *,
    ranked_base_ids: np.ndarray,
    truth_ids: np.ndarray,
    base_page_by_id: dict[int, int],
    resident_delta_ids: set[int],
    page_entries: dict[int, PageEntry],
    neighbors: int,
    gap_pages: int,
    max_gets: int,
    max_bytes: int,
    min_average_recall10_ppm: int,
    min_average_recall100_ppm: int,
    min_p05_recall100_ppm: int,
    max_span_pages: int | None = None,
) -> dict[str, Any]:
    if (
        ranked_base_ids.ndim != 2
        or truth_ids.ndim != 2
        or ranked_base_ids.shape[0] != truth_ids.shape[0]
        or truth_ids.shape[1] != neighbors
        or neighbors <= 0
        or gap_pages < 0
    ):
        raise ValueError("PQ16 page-nomination shape differs")
    samples = []
    all_pages = sorted(page_entries)
    page_count = all_pages[-1] + 1
    for query in range(truth_ids.shape[0]):
        planned_ranges = None
        if max_span_pages is not None:
            planned_ranges = plan_rank_weighted_ranges(
                ranked_base_ids=ranked_base_ids[query],
                base_page_by_id=base_page_by_id,
                page_count=page_count,
                max_span_pages=max_span_pages,
                max_ranges=max_gets,
            )
            selected_pages = [
                page
                for page in all_pages
                if any(start <= page <= end for start, end in planned_ranges)
            ]
        else:
            selected_pages = sorted(
                {
                    base_page_by_id[int(row_id)]
                    for row_id in ranked_base_ids[query]
                    if int(row_id) in base_page_by_id
                }
            )
        if any(page not in page_entries for page in selected_pages):
            raise ValueError("PQ16 page nomination references an unknown page")
        if planned_ranges is None:
            gets, encoded_bytes = _coalesced_work(
                selected_pages, page_entries, gap_pages
            )
        else:
            gets = len(planned_ranges)
            encoded_bytes = 0
            for start, end in planned_ranges:
                present = [page for page in all_pages if start <= page <= end]
                first = page_entries[present[0]]
                last = page_entries[present[-1]]
                encoded_bytes += last.offset + last.encoded_bytes - first.offset
        selected = set(selected_pages)
        hit_ids = [
            int(row_id)
            for row_id in truth_ids[query]
            if int(row_id) in resident_delta_ids
            or base_page_by_id.get(int(row_id)) in selected
        ]
        recall10_cutoff = min(10, neighbors)
        hit10_ids = [
            int(row_id)
            for row_id in truth_ids[query, :recall10_cutoff]
            if int(row_id) in resident_delta_ids
            or base_page_by_id.get(int(row_id)) in selected
        ]
        samples.append(
            {
                "bytes": encoded_bytes,
                "gets": gets,
                "hit10_ids": hit10_ids,
                "hit_ids": hit_ids,
                "hits10": len(hit10_ids),
                "hits": len(hit_ids),
                "query": query,
                "selected_pages": selected_pages,
                "truth_ids": [int(value) for value in truth_ids[query]],
            }
        )
    total_hits10 = sum(sample["hits10"] for sample in samples)
    total_hits100 = sum(sample["hits"] for sample in samples)
    recall10_cutoff = min(10, neighbors)
    average_recall10_ppm = total_hits10 * 1_000_000 // (len(samples) * recall10_cutoff)
    average_recall100_ppm = total_hits100 * 1_000_000 // (len(samples) * neighbors)
    recalls100 = sorted(sample["hits"] * 1_000_000 // neighbors for sample in samples)
    p05_recall100_ppm = recalls100[max(0, (len(samples) * 5 + 99) // 100 - 1)]
    worst_recall_ppm = recalls100[0]
    max_gets_per_query = max(sample["gets"] for sample in samples)
    max_bytes_per_query = max(sample["bytes"] for sample in samples)
    return {
        "average_recall10_ppm": average_recall10_ppm,
        "average_recall100_ppm": average_recall100_ppm,
        "gate_passed": average_recall10_ppm >= min_average_recall10_ppm
        and average_recall100_ppm >= min_average_recall100_ppm
        and p05_recall100_ppm >= min_p05_recall100_ppm
        and max_gets_per_query <= max_gets
        and max_bytes_per_query <= max_bytes,
        "max_bytes_per_query": max_bytes_per_query,
        "max_gets_per_query": max_gets_per_query,
        "p05_recall100_ppm": p05_recall100_ppm,
        "samples": samples,
        "worst_recall_ppm": worst_recall_ppm,
    }


def _train_pq(
    vectors: np.ndarray, subspaces: int, seed: int
) -> tuple[np.ndarray, np.ndarray]:
    rows, dimensions = vectors.shape
    if subspaces <= 0 or dimensions % subspaces != 0 or rows < 256:
        raise ValueError("PQ training shape differs")
    width = dimensions // subspaces
    generator = np.random.default_rng(seed)
    sample = vectors[generator.choice(rows, min(rows, 100_000), replace=False)]
    books = np.empty((subspaces, 256, width), dtype=np.float32)
    codes = np.empty((rows, subspaces), dtype=np.uint8)
    for subspace in range(subspaces):
        lo, hi = subspace * width, (subspace + 1) * width
        training = np.ascontiguousarray(sample[:, lo:hi])
        centroids = training[
            generator.choice(training.shape[0], 256, replace=False)
        ].copy()
        for _ in range(10):
            norms = np.einsum("ij,ij->i", centroids, centroids)
            assignment = np.empty(training.shape[0], dtype=np.int32)
            for start in range(0, training.shape[0], 8192):
                stop = min(start + 8192, training.shape[0])
                block = training[start:stop]
                assignment[start:stop] = np.argmin(
                    norms[None, :] - 2.0 * (block @ centroids.T), axis=1
                )
            counts = np.bincount(assignment, minlength=256)
            order = np.argsort(assignment, kind="stable")
            starts = np.concatenate(([0], np.cumsum(counts)[:-1]))
            occupied = counts > 0
            centroids[occupied] = (
                np.add.reduceat(training[order], starts[occupied], axis=0)
                / counts[occupied, None]
            )
        books[subspace] = centroids
        norms = np.einsum("ij,ij->i", centroids, centroids)
        for start in range(0, rows, 8192):
            stop = min(start + 8192, rows)
            block = vectors[start:stop, lo:hi]
            codes[start:stop, subspace] = np.argmin(
                norms[None, :] - 2.0 * (block @ centroids.T), axis=1
            )
    return books, codes


def _train_pq16(vectors: np.ndarray, seed: int) -> tuple[np.ndarray, np.ndarray]:
    return _train_pq(vectors, 16, seed)


def _train_sparse_residual_pq8(
    vectors: np.ndarray,
    base_ids: np.ndarray,
    books: np.ndarray,
    codes: np.ndarray,
    *,
    fraction_ppm: int,
    seed: int,
) -> SparseResidualPq8:
    rows, dimensions = vectors.shape
    if (
        rows != base_ids.size
        or books.shape[0] != 16
        or books.shape[1] != 256
        or books.shape[2] * 16 != dimensions
        or codes.shape != (rows, 16)
        or not 0 < fraction_ppm <= 1_000_000
    ):
        raise ValueError("sparse residual training shape differs")
    selected_count = (rows * fraction_ppm + 999_999) // 1_000_000
    if selected_count < 256:
        raise ValueError("sparse residual training population differs")
    base_width = dimensions // 16
    errors = np.zeros(rows, dtype=np.float32)
    for subspace in range(16):
        lo, hi = subspace * base_width, (subspace + 1) * base_width
        delta = vectors[:, lo:hi] - books[subspace, codes[:, subspace]]
        errors += np.einsum("ij,ij->i", delta, delta)
    selected_indices = np.lexsort((base_ids, -errors))[:selected_count].astype(
        np.int64, copy=False
    )
    residual_vectors = vectors[selected_indices].copy()
    for subspace in range(16):
        lo, hi = subspace * base_width, (subspace + 1) * base_width
        residual_vectors[:, lo:hi] -= books[
            subspace, codes[selected_indices, subspace]
        ]
    residual_books, residual_codes = _train_pq(residual_vectors, 8, seed)
    combined = np.empty_like(residual_vectors)
    for subspace in range(16):
        lo, hi = subspace * base_width, (subspace + 1) * base_width
        combined[:, lo:hi] = books[subspace, codes[selected_indices, subspace]]
    residual_width = dimensions // 8
    for subspace in range(8):
        lo, hi = subspace * residual_width, (subspace + 1) * residual_width
        combined[:, lo:hi] += residual_books[
            subspace, residual_codes[:, subspace]
        ]
    return SparseResidualPq8(
        selected_indices=selected_indices,
        books=residual_books,
        codes=residual_codes,
        combined_norms=np.einsum("ij,ij->i", combined, combined),
    )


def _srht_rotate(vectors: np.ndarray, seed: int) -> np.ndarray:
    """Apply one deterministic orthonormal sign-Hadamard rotation."""

    if (
        vectors.ndim != 2
        or vectors.shape[0] == 0
        or vectors.shape[1] == 0
        or not np.isfinite(vectors).all()
    ):
        raise ValueError("SRHT rotation input differs")
    dimensions = vectors.shape[1]
    padded = 1 << (dimensions - 1).bit_length()
    rotated = np.zeros((vectors.shape[0], padded), dtype=np.float32)
    rotated[:, :dimensions] = vectors
    generator = np.random.default_rng(seed)
    signs = generator.choice(
        np.asarray([-1.0, 1.0], dtype=np.float32), size=padded
    )
    rotated *= signs[None, :]
    width = 1
    while width < padded:
        for start in range(0, padded, 2 * width):
            left = rotated[:, start : start + width].copy()
            right = rotated[:, start + width : start + 2 * width].copy()
            rotated[:, start : start + width] = left + right
            rotated[:, start + width : start + 2 * width] = left - right
        width *= 2
    rotated *= np.float32(1.0 / np.sqrt(padded))
    return rotated


def _rank_pq16(
    queries: np.ndarray,
    base_ids: np.ndarray,
    books: np.ndarray,
    codes: np.ndarray,
    shortlist_rows: int,
) -> np.ndarray:
    subspaces, _, width = books.shape
    ranked = np.empty((queries.shape[0], shortlist_rows), dtype=np.int64)
    offsets = np.arange(subspaces, dtype=np.int32) * 256
    code_offsets = codes.astype(np.int32) + offsets[None, :]
    for query_ordinal, query in enumerate(queries):
        table = np.empty((subspaces, 256), dtype=np.float32)
        for subspace in range(subspaces):
            lo, hi = subspace * width, (subspace + 1) * width
            delta = books[subspace] - query[lo:hi]
            table[subspace] = np.einsum("ij,ij->i", delta, delta)
        scores = table.reshape(-1)[code_offsets].sum(axis=1)
        head = np.argpartition(scores, shortlist_rows - 1)[:shortlist_rows]
        ordered = head[np.lexsort((base_ids[head], scores[head]))]
        ranked[query_ordinal] = base_ids[ordered]
    return ranked


def _sparse_residual_resident_bytes(rows: int, fraction_ppm: int) -> int:
    if (
        type(rows) is not int
        or rows <= 0
        or type(fraction_ppm) is not int
        or not 0 < fraction_ppm <= 1_000_000
    ):
        raise ValueError("sparse residual projection differs")
    selected = (rows * fraction_ppm + 999_999) // 1_000_000
    return rows * 16 + selected * (8 + 4) + (rows + 7) // 8


def _rank_pq16_sparse_residual(
    queries: np.ndarray,
    base_ids: np.ndarray,
    books: np.ndarray,
    codes: np.ndarray,
    residual: SparseResidualPq8,
    shortlist_rows: int,
) -> np.ndarray:
    """Rank PQ16 rows, replacing selected scores with PQ8 residual scores."""

    subspaces, _, width = books.shape
    residual_subspaces, _, residual_width = residual.books.shape
    if (
        subspaces != 16
        or residual_subspaces != 8
        or queries.ndim != 2
        or queries.shape[1] != subspaces * width
        or residual_width * residual_subspaces != queries.shape[1]
        or codes.shape != (base_ids.size, subspaces)
        or residual.selected_indices.ndim != 1
        or residual.codes.shape
        != (residual.selected_indices.size, residual_subspaces)
        or residual.combined_norms.shape != (residual.selected_indices.size,)
        or shortlist_rows <= 0
        or shortlist_rows > base_ids.size
        or np.any(residual.selected_indices < 0)
        or np.any(residual.selected_indices >= base_ids.size)
        or len(set(residual.selected_indices.tolist()))
        != residual.selected_indices.size
    ):
        raise ValueError("sparse residual ranking shape differs")
    offsets = np.arange(subspaces, dtype=np.int32) * 256
    code_offsets = codes.astype(np.int32) + offsets[None, :]
    residual_offsets = np.arange(residual_subspaces, dtype=np.int32) * 256
    residual_code_offsets = (
        residual.codes.astype(np.int32) + residual_offsets[None, :]
    )
    ranked = np.empty((queries.shape[0], shortlist_rows), dtype=np.int64)
    for query_ordinal, query in enumerate(queries):
        base_distance_table = np.empty((subspaces, 256), dtype=np.float32)
        base_dot_table = np.empty((subspaces, 256), dtype=np.float32)
        for subspace in range(subspaces):
            lo, hi = subspace * width, (subspace + 1) * width
            delta = books[subspace] - query[lo:hi]
            base_distance_table[subspace] = np.einsum("ij,ij->i", delta, delta)
            base_dot_table[subspace] = books[subspace] @ query[lo:hi]
        scores = base_distance_table.reshape(-1)[code_offsets].sum(axis=1)
        residual_dot_table = np.empty(
            (residual_subspaces, 256), dtype=np.float32
        )
        for subspace in range(residual_subspaces):
            lo, hi = subspace * residual_width, (subspace + 1) * residual_width
            residual_dot_table[subspace] = residual.books[subspace] @ query[lo:hi]
        selected_base_dot = (
            base_dot_table.reshape(-1)[code_offsets[residual.selected_indices]].sum(
                axis=1
            )
        )
        selected_residual_dot = (
            residual_dot_table.reshape(-1)[residual_code_offsets].sum(axis=1)
        )
        scores[residual.selected_indices] = (
            residual.combined_norms
            + np.dot(query, query)
            - 2.0 * (selected_base_dot + selected_residual_dot)
        )
        head = np.argpartition(scores, shortlist_rows - 1)[:shortlist_rows]
        ordered = head[np.lexsort((base_ids[head], scores[head]))]
        ranked[query_ordinal] = base_ids[ordered]
    return ranked


def _splitmix64(values: np.ndarray, seed: int) -> np.ndarray:
    mixed = values.astype(np.uint64, copy=True) ^ np.uint64(seed)
    mixed += np.uint64(0x9E3779B97F4A7C15)
    mixed = (mixed ^ (mixed >> np.uint64(30))) * np.uint64(0xBF58476D1CE4E5B9)
    mixed = (mixed ^ (mixed >> np.uint64(27))) * np.uint64(0x94D049BB133111EB)
    return mixed ^ (mixed >> np.uint64(31))


def _pseudoquery_indices(base_ids: np.ndarray, count: int, seed: int) -> np.ndarray:
    if base_ids.ndim != 1 or count <= 0 or count > base_ids.size:
        raise ValueError("PQ cooccurrence pseudoquery count differs")
    keys = _splitmix64(base_ids, seed ^ 0xC001C0DE)
    return np.lexsort((base_ids, keys))[:count]


def _read_fixed_list(path: pathlib.Path, field: str, dimensions: int) -> np.ndarray:
    table = pq.read_table(path)
    column = table.column(field).combine_chunks()
    values = np.asarray(column.values.to_numpy(), dtype=np.float32).reshape(
        -1, dimensions
    )
    if column.null_count or not np.isfinite(values).all():
        raise ValueError(f"{field} authority differs")
    return values


def _read_page_run(
    path: pathlib.Path, run: dict[str, Any], dimensions: int
) -> tuple[dict[int, int], dict[int, PageEntry]]:
    identity = run["object"]
    if (
        path.stat().st_size != identity["bytes"]
        or _sha256_file(path) != identity["sha256"]
    ):
        raise ValueError("PQ16 run identity differs")
    body = path.read_bytes()
    page_by_id: dict[int, int] = {}
    page_entries: dict[int, PageEntry] = {}
    expected_schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("sequence", pa.uint64(), nullable=False),
            pa.field("state", pa.uint8(), nullable=False),
            pa.field(
                "code",
                pa.list_(pa.field("element", pa.uint8(), nullable=False), dimensions),
                nullable=False,
            ),
        ]
    )
    for page in run["pages"]:
        start, stop = page["offset"], page["offset"] + page["bytes"]
        table = ipc.open_stream(pa.py_buffer(body[start:stop])).read_all()
        if table.schema != expected_schema or table.num_rows != page["rows"]:
            raise ValueError("PQ16 page authority differs")
        ids = table.column("id").to_pylist()
        states = table.column("state").to_pylist()
        if any(state != 0 for state in states):
            raise ValueError("PQ16 page contains a non-live row")
        for row_id in ids:
            if int(row_id) in page_by_id:
                raise ValueError("PQ16 run contains a duplicate row")
            page_by_id[int(row_id)] = int(page["page"])
        page_entries[int(page["page"])] = PageEntry(
            offset=int(page["offset"]), encoded_bytes=int(page["bytes"])
        )
    return page_by_id, page_entries


def main() -> None:
    parser = argparse.ArgumentParser()
    for name in ("source", "queries", "truth", "generation", "base", "delta", "output"):
        parser.add_argument(f"--{name}", type=pathlib.Path, required=True)
    for name in ("source", "queries", "truth", "generation"):
        parser.add_argument(f"--{name}-uri", required=True)
        parser.add_argument(f"--{name}-sha256", required=True)
    parser.add_argument("--dimensions", type=int, default=768)
    parser.add_argument("--neighbors", type=int, default=100)
    parser.add_argument("--queries-count", type=int, default=32)
    parser.add_argument("--shortlist-rows", type=int, default=2048)
    parser.add_argument("--gap-pages", type=int, default=0)
    parser.add_argument("--seed", type=int, default=7216)
    parser.add_argument("--rotation", choices=("identity", "srht"), default="identity")
    parser.add_argument("--sparse-residual-fraction-ppm", type=int, default=0)
    parser.add_argument("--cooccurrence-pseudoqueries", type=int, default=0)
    parser.add_argument("--cooccurrence-shortlist-rows", type=int, default=512)
    parser.add_argument("--cooccurrence-unique-pages", type=int, default=32)
    args = parser.parse_args()
    if (
        args.sparse_residual_fraction_ppm < 0
        or args.sparse_residual_fraction_ppm > 1_000_000
        or (
            args.sparse_residual_fraction_ppm
            and (
                args.rotation != "identity" or args.cooccurrence_pseudoqueries != 0
            )
        )
    ):
        raise ValueError("sparse residual configuration differs")

    identities = {}
    for name in ("source", "queries", "truth", "generation"):
        path = getattr(args, name)
        digest = getattr(args, f"{name}_sha256")
        uri = getattr(args, f"{name}_uri")
        if not uri.startswith("s3://") or _sha256_file(path) != digest:
            raise ValueError(f"{name} identity differs")
        identities[name] = {
            "bytes": path.stat().st_size,
            "sha256": digest,
            "uri": uri,
        }

    generation_body = args.generation.read_bytes()
    generation = json.loads(generation_body)
    if _canonical_bytes(generation) != generation_body:
        raise ValueError("generation canonical bytes differ")
    if generation.get("dimensions") != args.dimensions:
        raise ValueError("generation dimensions differ")
    base_runs = [run for run in generation["runs"] if run["kind"] == "base"]
    delta_runs = [run for run in generation["runs"] if run["kind"] == "delta"]
    if len(base_runs) != 1 or len(delta_runs) != 1:
        raise ValueError("screen requires exactly one base and one delta run")
    base_page_by_id, page_entries = _read_page_run(
        args.base, base_runs[0], args.dimensions
    )
    delta_page_by_id, _ = _read_page_run(args.delta, delta_runs[0], args.dimensions)

    source = pq.read_table(args.source)
    source_ids = np.asarray(
        source.column("feature_row_id").combine_chunks().to_numpy(), dtype=np.int64
    )
    vectors = np.asarray(
        source.column("embedding").combine_chunks().values.to_numpy(),
        dtype=np.float32,
    ).reshape(-1, args.dimensions)
    if (
        len(set(source_ids.tolist())) != len(source_ids)
        or not np.isfinite(vectors).all()
    ):
        raise ValueError("source authority differs")
    row_by_id = {int(row_id): row for row, row_id in enumerate(source_ids)}
    if set(base_page_by_id) | set(delta_page_by_id) != set(row_by_id):
        raise ValueError("generation rows differ from source")
    base_ids = np.asarray(sorted(base_page_by_id), dtype=np.int64)
    base_vectors = np.ascontiguousarray(
        vectors[[row_by_id[int(row_id)] for row_id in base_ids]]
    )
    queries = _read_fixed_list(args.queries, "vector", args.dimensions)
    truth_table = pq.read_table(args.truth)
    truth_ids = np.asarray(
        truth_table.column("neighbors").combine_chunks().values.to_numpy(),
        dtype=np.int64,
    ).reshape(-1, args.neighbors)
    if (
        queries.shape[0] != args.queries_count
        or truth_ids.shape[0] != args.queries_count
    ):
        raise ValueError("evaluation query count differs")

    rotation_seed = args.seed ^ 0x53524854
    if args.rotation == "srht":
        scoring_base = _srht_rotate(base_vectors, rotation_seed)
        scoring_queries = _srht_rotate(queries, rotation_seed)
    else:
        scoring_base = base_vectors
        scoring_queries = queries
    books, codes = _train_pq16(scoring_base, args.seed)
    residual = None
    if args.sparse_residual_fraction_ppm:
        residual = _train_sparse_residual_pq8(
            scoring_base,
            base_ids,
            books,
            codes,
            fraction_ppm=args.sparse_residual_fraction_ppm,
            seed=args.seed ^ 0x52535138,
        )
        ranked = _rank_pq16_sparse_residual(
            scoring_queries,
            base_ids,
            books,
            codes,
            residual,
            args.shortlist_rows,
        )
    else:
        ranked = _rank_pq16(
            scoring_queries, base_ids, books, codes, args.shortlist_rows
        )
    maximum_page_bytes = max(entry.encoded_bytes for entry in page_entries.values())
    max_span_pages = (16 * 1024 * 1024) // maximum_page_bytes
    evaluation_args = dict(
        ranked_base_ids=ranked,
        truth_ids=truth_ids,
        base_page_by_id=base_page_by_id,
        resident_delta_ids=set(delta_page_by_id),
        page_entries=page_entries,
        neighbors=args.neighbors,
        gap_pages=args.gap_pages,
        max_gets=32,
        max_bytes=16 * 1024 * 1024,
        min_average_recall10_ppm=960_000,
        min_average_recall100_ppm=975_000,
        min_p05_recall100_ppm=900_000,
        max_span_pages=max_span_pages,
    )
    evaluation = evaluate_page_nominations(**evaluation_args)
    common_result = {
        "claim_eligible": False,
        "code_row_bytes": 16,
        "codes_built_without_queries_or_truth": True,
        "dimensions": args.dimensions,
        "evidence_kind": "offline-page-containment-not-serving-quality-or-latency",
        "gap_pages": args.gap_pages,
        "inputs": identities,
        "neighbors": args.neighbors,
        "page_planner": "exact-reciprocal-rank",
        "queries": args.queries_count,
        "representation": (
            "sparse-residual-pq8"
            if residual is not None
            else f"pq16-{args.rotation}"
        ),
        "resident_bytes_at_100m": (
            _sparse_residual_resident_bytes(
                100_000_000, args.sparse_residual_fraction_ppm
            )
            if residual is not None
            else 1_600_000_000
        ),
        "residual_code_row_bytes": 8 if residual is not None else 0,
        "residual_fraction_ppm": args.sparse_residual_fraction_ppm,
        "residual_norm_bytes": 4 if residual is not None else 0,
        "residual_seed": args.seed ^ 0x52535138 if residual is not None else None,
        "residual_selected_rows": (
            residual.selected_indices.size if residual is not None else 0
        ),
        "rotated_dimensions": scoring_base.shape[1],
        "rotation": args.rotation,
        "rotation_seed": rotation_seed if args.rotation == "srht" else None,
        "rows": len(source_ids),
        "shortlist_rows": args.shortlist_rows,
        "span_page_budget": max_span_pages,
    }
    if args.cooccurrence_pseudoqueries:
        pseudoquery_indices = _pseudoquery_indices(
            base_ids, args.cooccurrence_pseudoqueries, args.seed
        )
        pseudoquery_ids = base_ids[pseudoquery_indices]
        pseudoquery_ranked = _rank_pq16(
            scoring_base[pseudoquery_indices],
            base_ids,
            books,
            codes,
            args.cooccurrence_shortlist_rows,
        )
        page_order = order_pages_by_pq_cooccurrence(
            ranked_base_ids=pseudoquery_ranked,
            base_page_by_id=base_page_by_id,
            page_count=len(page_entries),
            unique_pages_per_query=args.cooccurrence_unique_pages,
        )
        remapped_ids, remapped_entries = remap_page_layout(
            base_page_by_id=base_page_by_id,
            page_entries=page_entries,
            old_pages_in_new_order=page_order,
        )
        cooccurrence_args = dict(evaluation_args)
        cooccurrence_args["base_page_by_id"] = remapped_ids
        cooccurrence_args["page_entries"] = remapped_entries
        cooccurrence = evaluate_page_nominations(**cooccurrence_args)
        non_regression = all(
            cooccurrence[field] >= evaluation[field]
            for field in (
                "average_recall10_ppm",
                "average_recall100_ppm",
                "p05_recall100_ppm",
            )
        )
        page_order_bytes = np.asarray(page_order, dtype="<u4").tobytes()
        pseudoquery_id_bytes = np.asarray(pseudoquery_ids, dtype="<i8").tobytes()
        result = {
            **common_result,
            "control": evaluation,
            "cooccurrence": cooccurrence,
            "gate_passed": cooccurrence["gate_passed"] and non_regression,
            "layout": {
                "algorithm": "maximum-adjacency-pq-cooccurrence-v1",
                "constructed_without_evaluation_queries_or_truth": True,
                "page_order": page_order,
                "page_order_sha256": hashlib.sha256(page_order_bytes).hexdigest(),
                "pseudoquery_count": args.cooccurrence_pseudoqueries,
                "pseudoquery_ids_sha256": hashlib.sha256(
                    pseudoquery_id_bytes
                ).hexdigest(),
                "shortlist_rows": args.cooccurrence_shortlist_rows,
                "unique_pages_per_query": args.cooccurrence_unique_pages,
            },
            "non_regression": non_regression,
            "schema": "borsuk-v85-pq16-cooccurrence-layout-result-v1",
        }
    else:
        result = {
            **evaluation,
            **common_result,
            "schema": (
                "borsuk-v85-sparse-residual-page-nomination-result-v1"
                if residual is not None
                else "borsuk-v85-pq16-page-nomination-result-v2"
            ),
        }
    body = _canonical_bytes(result)
    args.output.write_bytes(body)
    print(
        json.dumps(
            {
                "gate_passed": result["gate_passed"],
                "result_sha256": hashlib.sha256(body).hexdigest(),
                "schema": result["schema"],
            },
            separators=(",", ":"),
            sort_keys=True,
        ),
        flush=True,
    )


if __name__ == "__main__":
    main()
