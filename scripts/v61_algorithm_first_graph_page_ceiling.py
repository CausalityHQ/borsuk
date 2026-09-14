#!/usr/bin/env python3
"""Disposable exact-score locality ceiling for page-packed DiskANN graphs."""

from __future__ import annotations

import argparse
import hashlib
import heapq
import json
import os
import struct
import tempfile
import time
from pathlib import Path

import diskannpy
import numpy as np
import pyarrow.parquet as pq
from scipy.sparse import csr_matrix
from scipy.sparse.csgraph import breadth_first_order


ROWS = 1_000_000
QUERIES = 1_000
DIMENSIONS = 768
NEIGHBORS = 100
GRAPH_DEGREE = 64
BUILD_COMPLEXITY = 200
TRACE_EXPANSIONS = 512
HARD_PAGE_CAP = 64
PAGE_ROWS = (32, 64, 128)
EXPANSION_LIMITS = (128, 256, 512)
BEAMS = (4, 8, 16)
PACKINGS = ("original", "bfs", "random")
SCREEN_INDICES = np.arange(0, QUERIES, 4)
SEED = 20260914
PAGE_HEADER_BYTES = 64
NODE_METADATA_BYTES = 16
NODE_BYTES = NODE_METADATA_BYTES + DIMENSIONS * 4 + GRAPH_DEGREE * 4


def smoke_graph_layout() -> None:
    rng = np.random.default_rng(SEED)
    vectors = rng.standard_normal((1_024, DIMENSIONS), dtype=np.float32)
    with tempfile.TemporaryDirectory() as directory:
        diskannpy.build_disk_index(
            data=vectors,
            distance_metric="l2",
            index_directory=directory,
            complexity=64,
            graph_degree=GRAPH_DEGREE,
            search_memory_maximum=1.0,
            build_memory_maximum=1.0,
            num_threads=2,
            pq_disk_bytes=0,
            vector_dtype=np.float32,
            index_prefix="smoke",
        )
        path = Path(directory) / "smoke_disk.index"
        raw = np.memmap(path, mode="r", dtype=np.uint8)
        if struct.unpack_from("<II", raw, 0) != (9, 1):
            raise ValueError("DiskANN smoke metadata envelope differs")
        metadata = struct.unpack_from("<9Q", raw, 8)
        expected_node_length = DIMENSIONS * 4 + (GRAPH_DEGREE + 1) * 4
        expected_bytes = (1_024 + 1) * 4096
        if (
            metadata[0] != 1_024
            or metadata[1] != DIMENSIONS
            or metadata[3] != expected_node_length
            or metadata[4] != 1
            or metadata[7] != 0
            or metadata[8] != expected_bytes
            or raw.size != expected_bytes
        ):
            raise ValueError("DiskANN smoke graph layout differs")
        degree = struct.unpack_from("<I", raw, 4096 + DIMENSIONS * 4)[0]
        if degree == 0 or degree > GRAPH_DEGREE:
            raise ValueError("DiskANN smoke graph degree differs")


def fixed_list(table, name: str, width: int, rows: int) -> np.ndarray:
    column = table[name].combine_chunks()
    values = column.values.to_numpy(zero_copy_only=False)
    result = np.array(values, dtype=np.float32, copy=True).reshape(rows, width)
    if result.shape != (rows, width) or not np.isfinite(result).all():
        raise ValueError(f"{name} differs")
    return result


def scalar(table, name: str, dtype) -> np.ndarray:
    return np.asarray(
        table[name].combine_chunks().to_numpy(zero_copy_only=False), dtype=dtype
    )


def load_inputs(source_path: Path, query_path: Path, truth_path: Path):
    source = pq.read_table(source_path)
    query = pq.read_table(query_path)
    truth = pq.read_table(truth_path)
    if (
        source.num_rows != ROWS
        or query.num_rows != QUERIES
        or truth.num_rows != QUERIES * NEIGHBORS
    ):
        raise ValueError("row count differs")
    vectors = fixed_list(source, "embedding", DIMENSIONS, ROWS)
    feature_ids = scalar(source, "feature_row_id", np.uint64)
    queries = fixed_list(query, "embedding", DIMENSIONS, QUERIES)
    truth_queries = scalar(truth, "query_ordinal", np.int64)
    truth_ranks = scalar(truth, "rank", np.int64)
    truth_ids = scalar(truth, "feature_row_id", np.uint64).reshape(
        QUERIES, NEIGHBORS
    )
    if not np.array_equal(
        truth_queries, np.repeat(np.arange(QUERIES), NEIGHBORS)
    ) or not np.array_equal(truth_ranks, np.tile(np.arange(NEIGHBORS), QUERIES)):
        raise ValueError("ground-truth order differs")
    order = np.argsort(feature_ids)
    sorted_ids = feature_ids[order]
    positions = np.searchsorted(sorted_ids, truth_ids)
    if np.any(positions == ROWS) or not np.array_equal(
        sorted_ids[positions], truth_ids
    ):
        raise ValueError("ground-truth feature id differs")
    truth_rows = order[positions].astype(np.int32)
    return vectors, feature_ids, queries, truth_rows


def build_graph(vectors: np.ndarray, directory: Path) -> tuple[float, int]:
    started = time.perf_counter()
    diskannpy.build_disk_index(
        data=vectors,
        distance_metric="l2",
        index_directory=str(directory),
        complexity=BUILD_COMPLEXITY,
        graph_degree=GRAPH_DEGREE,
        search_memory_maximum=16.0,
        build_memory_maximum=128.0,
        num_threads=min(os.cpu_count() or 1, 64),
        pq_disk_bytes=0,
        vector_dtype=np.float32,
        index_prefix="ann",
    )
    elapsed = time.perf_counter() - started
    size = sum(path.stat().st_size for path in directory.rglob("*") if path.is_file())
    return elapsed, size


def parse_graph(path: Path) -> tuple[int, np.ndarray, np.ndarray, dict]:
    raw = np.memmap(path, mode="r", dtype=np.uint8)
    points, fields = struct.unpack_from("<II", raw, 0)
    if (points, fields) != (9, 1):
        raise ValueError("DiskANN graph metadata envelope differs")
    metadata = struct.unpack_from("<9Q", raw, 8)
    (
        rows,
        dimensions,
        medoid,
        node_length,
        nodes_per_sector,
        frozen_points,
        frozen_location,
        append_reorder_data,
        disk_index_bytes,
    ) = metadata
    expected_node_length = DIMENSIONS * 4 + (GRAPH_DEGREE + 1) * 4
    if (
        rows != ROWS
        or dimensions != DIMENSIONS
        or node_length != expected_node_length
        or nodes_per_sector != 1
        or frozen_points != 0
        or frozen_location != 0
        or append_reorder_data != 0
        or disk_index_bytes != raw.size
        or raw.size != (ROWS + 1) * 4096
        or medoid >= ROWS
    ):
        raise ValueError("DiskANN graph metadata differs")
    degrees = np.ndarray(
        (ROWS,),
        dtype="<u4",
        buffer=raw,
        offset=4096 + DIMENSIONS * 4,
        strides=(4096,),
    ).copy()
    neighbors = np.ndarray(
        (ROWS, GRAPH_DEGREE),
        dtype="<u4",
        buffer=raw,
        offset=4096 + DIMENSIONS * 4 + 4,
        strides=(4096, 4),
    ).copy()
    if np.any(degrees == 0) or np.any(degrees > GRAPH_DEGREE):
        raise ValueError("DiskANN graph degree differs")
    slots = np.arange(GRAPH_DEGREE)[None, :] < degrees[:, None]
    if np.any(neighbors[slots] >= ROWS):
        raise ValueError("DiskANN graph neighbor differs")
    return int(medoid), degrees, neighbors, {
        "node_length": int(node_length),
        "nodes_per_sector": int(nodes_per_sector),
        "disk_index_bytes": int(disk_index_bytes),
        "minimum_degree": int(degrees.min()),
        "maximum_degree": int(degrees.max()),
        "mean_degree": float(degrees.mean()),
    }


def splitmix64(values: np.ndarray) -> np.ndarray:
    values = values.astype(np.uint64, copy=True) + np.uint64(0x9E3779B97F4A7C15)
    values = (values ^ (values >> np.uint64(30))) * np.uint64(0xBF58476D1CE4E5B9)
    values = (values ^ (values >> np.uint64(27))) * np.uint64(0x94D049BB133111EB)
    return values ^ (values >> np.uint64(31))


def packing_orders(
    medoid: int, degrees: np.ndarray, neighbors: np.ndarray
) -> dict[str, np.ndarray]:
    indptr = np.empty(ROWS + 1, dtype=np.int64)
    indptr[0] = 0
    np.cumsum(degrees, dtype=np.int64, out=indptr[1:])
    slots = np.arange(GRAPH_DEGREE)[None, :] < degrees[:, None]
    indices = neighbors[slots].astype(np.int32, copy=False)
    graph = csr_matrix(
        (np.ones(len(indices), dtype=np.uint8), indices, indptr),
        shape=(ROWS, ROWS),
    )
    bfs = breadth_first_order(
        graph, medoid, directed=True, return_predecessors=False
    ).astype(np.int32, copy=False)
    if len(bfs) != ROWS:
        seen = np.zeros(ROWS, dtype=bool)
        seen[bfs] = True
        bfs = np.concatenate((bfs, np.flatnonzero(~seen).astype(np.int32)))
    random_keys = splitmix64(np.arange(ROWS, dtype=np.uint64) ^ np.uint64(SEED))
    random_order = np.argsort(random_keys, kind="stable").astype(np.int32)
    original = np.arange(ROWS, dtype=np.int32)
    for name, order in (("bfs", bfs), ("random", random_order)):
        if len(order) != ROWS or len(np.unique(order)) != ROWS:
            raise ValueError(f"{name} packing differs")
    return {"original": original, "bfs": bfs, "random": random_order}


def exact_trace(
    query: np.ndarray,
    vectors: np.ndarray,
    medoid: int,
    degrees: np.ndarray,
    neighbors: np.ndarray,
) -> np.ndarray:
    initial_delta = vectors[medoid] - query
    heap = [(float(initial_delta @ initial_delta), medoid)]
    discovered = {medoid}
    expanded: list[int] = []
    while heap and len(expanded) < TRACE_EXPANSIONS:
        _, node = heapq.heappop(heap)
        expanded.append(node)
        adjacent = neighbors[node, : degrees[node]]
        fresh = [int(value) for value in adjacent if int(value) not in discovered]
        if not fresh:
            continue
        discovered.update(fresh)
        fresh_array = np.asarray(fresh, dtype=np.int32)
        delta = vectors[fresh_array] - query
        distances = np.einsum("ij,ij->i", delta, delta)
        for distance, row in zip(distances, fresh):
            heapq.heappush(heap, (float(distance), int(row)))
    if len(expanded) != TRACE_EXPANSIONS:
        raise ValueError("exact graph trace exhausted")
    return np.asarray(expanded, dtype=np.int32)


def traces_for(
    query_indices: np.ndarray,
    queries: np.ndarray,
    vectors: np.ndarray,
    medoid: int,
    degrees: np.ndarray,
    neighbors: np.ndarray,
) -> tuple[np.ndarray, int]:
    started = time.perf_counter_ns()
    traces = np.vstack(
        [
            exact_trace(queries[index], vectors, medoid, degrees, neighbors)
            for index in query_indices
        ]
    )
    return traces, (time.perf_counter_ns() - started) // 1_000


def percentile(values: list[int], numerator: int) -> int:
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, len(ordered) * numerator // 100)]


def evaluate_cell(
    query_indices: np.ndarray,
    traces: np.ndarray,
    truth_rows: np.ndarray,
    order: np.ndarray,
    page_rows: int,
    expansion_limit: int,
) -> dict:
    page_of = np.empty(ROWS, dtype=np.int32)
    page_of[order] = np.arange(ROWS, dtype=np.int32) // page_rows
    hits: list[int] = []
    pages_per_query: list[int] = []
    selected_pages: list[np.ndarray] = []
    for position, query_index in enumerate(query_indices):
        pages: list[int] = []
        seen: set[int] = set()
        for row in traces[position, :expansion_limit]:
            page = int(page_of[row])
            if page not in seen:
                seen.add(page)
                pages.append(page)
                if len(pages) == HARD_PAGE_CAP:
                    break
        page_array = np.asarray(pages, dtype=np.int32)
        selected_pages.append(page_array)
        pages_per_query.append(len(pages))
        hits.append(int(np.isin(page_of[truth_rows[query_index]], page_array).sum()))
    aggregate = int(sum(hits) * 1_000_000 // (len(hits) * NEIGHBORS))
    minimum = int(min(hits) * 10_000)
    page_bytes = PAGE_HEADER_BYTES + page_rows * NODE_BYTES
    p95_pages = percentile(pages_per_query, 95)
    maximum_pages = max(pages_per_query)
    beam_projections = [
        {
            "beam": beam,
            "optimistic_p95_round_lower_bound": (p95_pages + beam - 1) // beam,
            "optimistic_maximum_round_lower_bound": (maximum_pages + beam - 1)
            // beam,
        }
        for beam in BEAMS
    ]
    return {
        "queries": len(hits),
        "page_rows": page_rows,
        "expansion_limit": expansion_limit,
        "page_bytes": page_bytes,
        "aggregate_exact_rerank_recall_ppm": aggregate,
        "minimum_exact_rerank_recall_ppm": minimum,
        "p50_pages": percentile(pages_per_query, 50),
        "p95_pages": p95_pages,
        "maximum_pages": maximum_pages,
        "p95_payload_bytes": p95_pages * page_bytes,
        "maximum_payload_bytes": maximum_pages * page_bytes,
        "beam_round_lower_bounds": beam_projections,
        "quality_passed": aggregate >= 990_000 and minimum >= 700_000,
        "io_ceiling_passed": p95_pages <= 32
        and p95_pages * page_bytes <= 16 * 1024 * 1024,
        "hard_caps_passed": maximum_pages <= HARD_PAGE_CAP
        and maximum_pages * page_bytes <= 32 * 1024 * 1024,
        "selected_pages": selected_pages,
    }


def verify_exact_rerank(
    indices: np.ndarray,
    queries: np.ndarray,
    vectors: np.ndarray,
    truth_rows: np.ndarray,
    order: np.ndarray,
    cell: dict,
) -> None:
    page_rows = int(cell["page_rows"])
    for position, query_index in enumerate(indices[:8]):
        pages = cell["selected_pages"][position]
        candidate_parts = [
            order[page * page_rows : min((page + 1) * page_rows, ROWS)]
            for page in pages
        ]
        candidates = np.concatenate(candidate_parts)
        delta = vectors[candidates] - queries[query_index]
        distances = np.einsum("ij,ij->i", delta, delta)
        count = min(NEIGHBORS, len(candidates))
        partition = np.argpartition(distances, count - 1)[:count]
        ranked = candidates[
            partition[np.lexsort((candidates[partition], distances[partition]))]
        ]
        exact_hits = int(np.isin(truth_rows[query_index], ranked).sum())
        page_hits = int(
            np.isin(
                truth_rows[query_index],
                candidates,
            ).sum()
        )
        if exact_hits != page_hits:
            raise ValueError("exact rerank/page-containment equivalence differs")


def strip_pages(cell: dict) -> dict:
    return {key: value for key, value in cell.items() if key != "selected_pages"}


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while block := handle.read(8 * 1024 * 1024):
            digest.update(block)
    return digest.hexdigest()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--development-query", type=Path, required=True)
    parser.add_argument("--development-ground-truth", type=Path, required=True)
    parser.add_argument("--index-directory", type=Path, required=True)
    parser.add_argument("--artifact-directory", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    smoke_graph_layout()
    vectors, feature_ids, queries, truth_rows = load_inputs(
        args.source, args.development_query, args.development_ground_truth
    )
    args.index_directory.mkdir(parents=True, exist_ok=False)
    args.artifact_directory.mkdir(parents=True, exist_ok=False)
    build_seconds, build_directory_bytes = build_graph(vectors, args.index_directory)
    medoid, degrees, neighbors, graph_metadata = parse_graph(
        args.index_directory / "ann_disk.index"
    )
    orders = packing_orders(medoid, degrees, neighbors)
    screen_traces, screen_trace_us = traces_for(
        SCREEN_INDICES, queries, vectors, medoid, degrees, neighbors
    )
    cells = []
    for packing in PACKINGS:
        for page_rows in PAGE_ROWS:
            for expansion_limit in EXPANSION_LIMITS:
                cell = evaluate_cell(
                    SCREEN_INDICES,
                    screen_traces,
                    truth_rows,
                    orders[packing],
                    page_rows,
                    expansion_limit,
                )
                cell["packing"] = packing
                cells.append(cell)
    eligible = [
        cell
        for cell in cells
        if cell["quality_passed"]
        and cell["io_ceiling_passed"]
        and cell["hard_caps_passed"]
    ]
    eligible.sort(
        key=lambda cell: (
            cell["p95_payload_bytes"],
            cell["p95_pages"],
            cell["expansion_limit"],
            PACKINGS.index(cell["packing"]),
        )
    )
    chosen = eligible[0] if eligible else None
    development = None
    development_trace_us = None
    if chosen is not None:
        full_indices = np.arange(QUERIES)
        full_traces, development_trace_us = traces_for(
            full_indices, queries, vectors, medoid, degrees, neighbors
        )
        development_cell = evaluate_cell(
            full_indices,
            full_traces,
            truth_rows,
            orders[str(chosen["packing"])],
            int(chosen["page_rows"]),
            int(chosen["expansion_limit"]),
        )
        verify_exact_rerank(
            full_indices,
            queries,
            vectors,
            truth_rows,
            orders[str(chosen["packing"])],
            development_cell,
        )
        development_cell["packing"] = chosen["packing"]
        development_cell["quality_passed"] = (
            development_cell["aggregate_exact_rerank_recall_ppm"] >= 995_000
            and development_cell["minimum_exact_rerank_recall_ppm"] >= 800_000
        )
        development = strip_pages(development_cell)
    artifacts = {
        "degrees": args.artifact_directory / "degrees.npy",
        "neighbors": args.artifact_directory / "neighbors.npy",
        "bfs_order": args.artifact_directory / "bfs-order.npy",
        "random_order": args.artifact_directory / "random-order.npy",
    }
    np.save(artifacts["degrees"], degrees, allow_pickle=False)
    np.save(artifacts["neighbors"], neighbors, allow_pickle=False)
    np.save(artifacts["bfs_order"], orders["bfs"], allow_pickle=False)
    np.save(artifacts["random_order"], orders["random"], allow_pickle=False)
    artifact_receipts = {
        role: {
            "file": path.name,
            "bytes": path.stat().st_size,
            "sha256": file_sha256(path),
        }
        for role, path in artifacts.items()
    }
    result = {
        "schema": "borsuk-v61-algorithm-first-graph-page-ceiling-result-v1",
        "claim_eligible": False,
        "evidence_kind": "optimistic-exact-score-page-locality-ceiling",
        "source_rows": ROWS,
        "dimensions": DIMENSIONS,
        "returned_neighbors": NEIGHBORS,
        "graph_degree": GRAPH_DEGREE,
        "build_complexity": BUILD_COMPLEXITY,
        "trace_expansions": TRACE_EXPANSIONS,
        "hard_page_cap": HARD_PAGE_CAP,
        "build_elapsed_ms": int(build_seconds * 1_000),
        "build_directory_bytes": build_directory_bytes,
        "graph_metadata": graph_metadata,
        "medoid": medoid,
        "screen_trace_total_us": screen_trace_us,
        "development_trace_total_us": development_trace_us,
        "screen_cells": [strip_pages(cell) for cell in cells],
        "chosen_screen_cell": strip_pages(chosen) if chosen is not None else None,
        "development": development,
        "artifact_receipts": artifact_receipts,
        "projected_100m_f32_graph_payload_bytes": 100_000_000 * NODE_BYTES,
        "projected_100m_resident_pq16_bytes_for_next_stage": 1_600_000_000,
        "dependent_rounds_are_optimistic_lower_bounds": True,
        "global_exact_neighbor_scoring_is_not_serving_qualified": True,
        "s3_latency_unmeasured": True,
        "validation_opened": False,
    }
    payload = (json.dumps(result, sort_keys=True, separators=(",", ":")) + "\n").encode()
    args.output.write_bytes(payload)
    print(json.dumps({"result_sha256": hashlib.sha256(payload).hexdigest(), **result}))


if __name__ == "__main__":
    main()
