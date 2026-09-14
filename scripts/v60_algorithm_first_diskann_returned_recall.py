#!/usr/bin/env python3
"""Disposable actual-returned-recall DiskANN calibration on ReLAION2B 1M."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import tempfile
import time
from pathlib import Path

import diskannpy
import numpy as np
import pyarrow.parquet as pq


ROWS = 1_000_000
QUERIES = 1_000
DIMENSIONS = 768
NEIGHBORS = 100
SCREEN_INDICES = np.arange(0, QUERIES, 4)
BUILD_COMPLEXITY = 200
GRAPH_DEGREE = 64
SEARCH_LADDER = ((128, 2), (256, 2), (256, 4), (512, 4), (512, 8), (1024, 8))


def smoke_diskann_api() -> None:
    rng = np.random.default_rng(20260914)
    vectors = rng.standard_normal((1_024, 32), dtype=np.float32)
    with tempfile.TemporaryDirectory() as directory:
        diskannpy.build_disk_index(
            data=vectors,
            distance_metric="l2",
            index_directory=directory,
            complexity=32,
            graph_degree=16,
            search_memory_maximum=1.0,
            build_memory_maximum=1.0,
            num_threads=2,
            pq_disk_bytes=0,
            vector_dtype=np.float32,
            index_prefix="smoke",
        )
        index = diskannpy.StaticDiskIndex(
            index_directory=directory,
            num_threads=2,
            num_nodes_to_cache=0,
            distance_metric="l2",
            vector_dtype=np.float32,
            dimensions=32,
            index_prefix="smoke",
        )
        response = index.search(vectors[0], 10, 32, beam_width=2)
        if len(response.identifiers) != 10:
            raise ValueError("DiskANN smoke response differs")


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
    if len(np.unique(feature_ids)) != ROWS:
        raise ValueError("source feature ids differ")
    return vectors, feature_ids, queries, truth_ids


def evaluate(
    index,
    query_indices: np.ndarray,
    queries: np.ndarray,
    truth_ids: np.ndarray,
    feature_ids: np.ndarray,
    complexity: int,
    beam_width: int,
    promotion: bool,
) -> dict:
    hits = []
    timings = []
    for query_index in query_indices:
        started = time.perf_counter_ns()
        response = index.search(
            queries[query_index],
            NEIGHBORS,
            complexity,
            beam_width=beam_width,
        )
        timings.append((time.perf_counter_ns() - started) // 1_000)
        returned = feature_ids[np.asarray(response.identifiers, dtype=np.int64)]
        hits.append(int(np.isin(truth_ids[query_index], returned).sum()))
    hit_array = np.asarray(hits)
    timings.sort()
    aggregate = int(hit_array.sum() * 1_000_000 // (len(hits) * NEIGHBORS))
    minimum = int(hit_array.min() * 10_000)
    aggregate_gate, minimum_gate = (995_000, 800_000) if promotion else (990_000, 700_000)
    return {
        "queries": len(hits),
        "search_complexity": complexity,
        "beam_width": beam_width,
        "aggregate_returned_recall_ppm": aggregate,
        "minimum_returned_recall_ppm": minimum,
        "p50_local_disk_query_us": int(timings[len(timings) // 2]),
        "p99_local_disk_query_us": int(
            timings[min(len(timings) - 1, len(timings) * 99 // 100)]
        ),
        "passed": aggregate >= aggregate_gate and minimum >= minimum_gate,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--development-query", type=Path, required=True)
    parser.add_argument("--development-ground-truth", type=Path, required=True)
    parser.add_argument("--index-directory", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    smoke_diskann_api()
    vectors, feature_ids, queries, truth_ids = load_inputs(
        args.source, args.development_query, args.development_ground_truth
    )
    args.index_directory.mkdir(parents=True, exist_ok=False)
    workers = min(os.cpu_count() or 1, 64)
    started = time.perf_counter()
    diskannpy.build_disk_index(
        data=vectors,
        distance_metric="l2",
        index_directory=str(args.index_directory),
        complexity=BUILD_COMPLEXITY,
        graph_degree=GRAPH_DEGREE,
        search_memory_maximum=16.0,
        build_memory_maximum=128.0,
        num_threads=workers,
        pq_disk_bytes=0,
        vector_dtype=np.float32,
        index_prefix="ann",
    )
    build_seconds = time.perf_counter() - started
    del vectors
    index_bytes = sum(
        path.stat().st_size for path in args.index_directory.rglob("*") if path.is_file()
    )
    index = diskannpy.StaticDiskIndex(
        index_directory=str(args.index_directory),
        num_threads=workers,
        num_nodes_to_cache=0,
        distance_metric="l2",
        vector_dtype=np.float32,
        dimensions=DIMENSIONS,
        index_prefix="ann",
    )
    screens = [
        evaluate(
            index,
            SCREEN_INDICES,
            queries,
            truth_ids,
            feature_ids,
            complexity,
            beam_width,
            False,
        )
        for complexity, beam_width in SEARCH_LADDER
    ]
    chosen = next((cell for cell in screens if cell["passed"]), None)
    development = None
    if chosen is not None:
        development = evaluate(
            index,
            np.arange(QUERIES),
            queries,
            truth_ids,
            feature_ids,
            int(chosen["search_complexity"]),
            int(chosen["beam_width"]),
            True,
        )
    result = {
        "schema": "borsuk-v60-algorithm-first-diskann-returned-recall-result-v1",
        "claim_eligible": False,
        "source_rows": ROWS,
        "dimensions": DIMENSIONS,
        "returned_neighbors": NEIGHBORS,
        "distance_metric": "squared-l2",
        "diskannpy_version": "0.7.0",
        "build_complexity": BUILD_COMPLEXITY,
        "graph_degree": GRAPH_DEGREE,
        "workers": workers,
        "build_elapsed_ms": int(build_seconds * 1_000),
        "serialized_index_bytes": index_bytes,
        "serialized_index_bytes_per_row": index_bytes / ROWS,
        "search_ladder": SEARCH_LADDER,
        "screen_cells": screens,
        "chosen_screen_cell": chosen,
        "development": development,
        "validation_opened": False,
        "storage": "local-ebs-calibration-not-s3",
        "s3_gets_and_payload_unmeasured": True,
    }
    payload = (json.dumps(result, sort_keys=True, separators=(",", ":")) + "\n").encode()
    args.output.write_bytes(payload)
    print(json.dumps({"result_sha256": hashlib.sha256(payload).hexdigest(), **result}), flush=True)


if __name__ == "__main__":
    main()
