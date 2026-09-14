#!/usr/bin/env python3
"""Disposable co-designed prefix-page layout/router probe on ReLAION2B 1M."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import time
from pathlib import Path

import numpy as np
import pyarrow.parquet as pq
import torch

import v53_algorithm_first_page_mlp as common
import v54_algorithm_first_binary_scan as binary
import v57_algorithm_first_leaf_incidence as leaf


PARTITION_LADDER = (1, 2, 4)
PAGE_DEPTH_LADDER = (11, 9, 7)


def load_source(path: Path):
    source = pq.read_table(path)
    if source.num_rows != common.ROWS:
        raise ValueError("source row count differs")
    feature_ids = common.scalar(source, "feature_row_id", np.uint64)
    vectors = common.fixed_list(source, "embedding", common.DIMENSIONS, common.ROWS)
    return vectors, feature_ids


def page_assignments(assignments: np.ndarray, page_depth: int) -> np.ndarray:
    return assignments.astype(np.uint32) >> np.uint32(leaf.DEPTH - page_depth)


def selected_pages(
    leaves_by_partition: list[np.ndarray], partition_count: int, page_depth: int
) -> np.ndarray:
    pages_per_partition = 1 << page_depth
    scores = np.zeros(partition_count * pages_per_partition, dtype=np.float64)
    weights = 1.0 / (60.0 + np.arange(leaf.TOP_LEAVES, dtype=np.float64))
    for partition in range(partition_count):
        prefixes = leaves_by_partition[partition] >> (leaf.DEPTH - page_depth)
        np.add.at(scores, partition * pages_per_partition + prefixes, weights)
    page_ids = np.arange(len(scores))
    return np.lexsort((page_ids, -scores))[: common.SELECTED_PAGES]


def evaluate_cell(
    query_indices: np.ndarray,
    queries: np.ndarray,
    truth_rows: np.ndarray,
    assignments: list[np.ndarray],
    centroids: list[np.ndarray],
    partition_count: int,
    page_depth: int,
    promotion: bool,
) -> dict:
    pages_per_partition = 1 << page_depth
    row_pages = [page_assignments(current, page_depth) for current in assignments[:partition_count]]
    populations = [
        np.bincount(current, minlength=pages_per_partition) for current in row_pages
    ]
    hits = []
    transferred_rows = []
    timings = []
    for query_index in query_indices:
        started = time.perf_counter_ns()
        leaves_by_partition = [
            leaf.top_leaves(queries[query_index], current)
            for current in centroids[:partition_count]
        ]
        pages = selected_pages(leaves_by_partition, partition_count, page_depth)
        truth = truth_rows[query_index]
        covered = np.zeros(len(truth), dtype=bool)
        rows = 0
        for page in pages:
            partition = int(page) // pages_per_partition
            ordinal = int(page) % pages_per_partition
            covered |= row_pages[partition][truth] == ordinal
            rows += int(populations[partition][ordinal])
        hits.append(int(covered.sum()))
        transferred_rows.append(rows)
        timings.append((time.perf_counter_ns() - started) // 1_000)
    hits_array = np.asarray(hits)
    timings.sort()
    transferred_rows.sort()
    aggregate = int(hits_array.sum() * 1_000_000 // (len(hits) * 100))
    minimum = int(hits_array.min() * 10_000)
    aggregate_gate, minimum_gate = (995_000, 800_000) if promotion else (990_000, 700_000)
    median_rows = int(transferred_rows[len(transferred_rows) // 2])
    p99_rows = int(
        transferred_rows[min(len(transferred_rows) - 1, len(transferred_rows) * 99 // 100)]
    )
    return {
        "partitions": partition_count,
        "page_depth": page_depth,
        "pages_per_partition": pages_per_partition,
        "mean_rows_per_page": common.ROWS // pages_per_partition,
        "selected_pages": common.SELECTED_PAGES,
        "queries": len(hits),
        "aggregate_recall_ppm": aggregate,
        "minimum_recall_ppm": minimum,
        "median_rows_transferred": median_rows,
        "p99_rows_transferred": p99_rows,
        "median_f32_payload_bytes": median_rows * (common.DIMENSIONS * 4 + 16),
        "median_f16_payload_bytes": median_rows * (common.DIMENSIONS * 2 + 16),
        "p50_query_us": int(timings[len(timings) // 2]),
        "p99_query_us": int(timings[min(len(timings) - 1, len(timings) * 99 // 100)]),
        "passed": aggregate >= aggregate_gate and minimum >= minimum_gate,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--development-query", type=Path, required=True)
    parser.add_argument("--development-ground-truth", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    torch.set_num_threads(os.cpu_count() or 1)
    vectors, feature_ids = load_source(args.source)
    assignments = []
    centroids = []
    populations = []
    started = time.perf_counter()
    for seed in leaf.SEEDS:
        current_assignments, current_centroids, current_populations = leaf.build_partition(
            vectors, seed
        )
        assignments.append(current_assignments)
        centroids.append(current_centroids)
        populations.append(current_populations)
    construction_seconds = time.perf_counter() - started
    queries, truth = common.load_queries(
        args.development_query, args.development_ground_truth
    )
    truth_rows = binary.map_truth(feature_ids, truth)
    screen_indices = np.arange(0, common.QUERIES, 4)
    screens = []
    for partition_count in PARTITION_LADDER:
        for page_depth in PAGE_DEPTH_LADDER:
            screens.append(
                evaluate_cell(
                    screen_indices,
                    queries,
                    truth_rows,
                    assignments,
                    centroids,
                    partition_count,
                    page_depth,
                    False,
                )
            )
    passing = [cell for cell in screens if cell["passed"]]
    chosen = min(
        passing,
        key=lambda cell: (
            cell["partitions"],
            cell["median_f32_payload_bytes"],
            -cell["page_depth"],
        ),
        default=None,
    )
    development = None
    if chosen is not None:
        development = evaluate_cell(
            np.arange(common.QUERIES),
            queries,
            truth_rows,
            assignments,
            centroids,
            int(chosen["partitions"]),
            int(chosen["page_depth"]),
            True,
        )
    result = {
        "schema": "borsuk-v58-algorithm-first-prefix-pages-result-v1",
        "claim_eligible": False,
        "source_rows": common.ROWS,
        "dimensions": common.DIMENSIONS,
        "partition_ladder": PARTITION_LADDER,
        "page_depth_ladder": PAGE_DEPTH_LADDER,
        "leaf_depth": leaf.DEPTH,
        "leaves_per_partition": leaf.LEAVES,
        "top_leaves_per_partition": leaf.TOP_LEAVES,
        "selected_pages": common.SELECTED_PAGES,
        "seeds": leaf.SEEDS,
        "construction_elapsed_ms": int(construction_seconds * 1_000),
        "screen_cells": screens,
        "chosen_screen_cell": chosen,
        "development": development,
        "validation_opened": False,
        "routing_gets_per_query": 0,
        "logical_router_write_bytes_per_row_at_four_partitions": 8,
        "payload_replication_equals_partition_count": True,
    }
    payload = (json.dumps(result, sort_keys=True, separators=(",", ":")) + "\n").encode()
    args.output.write_bytes(payload)
    print(json.dumps({"result_sha256": hashlib.sha256(payload).hexdigest(), **result}), flush=True)


if __name__ == "__main__":
    main()
