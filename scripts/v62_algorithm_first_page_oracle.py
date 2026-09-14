#!/usr/bin/env python3
"""Query-independent page-layout oracle coverage for the ReLAION2B screen."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import numpy as np
import pyarrow.parquet as pq


ROWS = 1_000_000
QUERIES = 1_000
NEIGHBORS = 100
DIMENSIONS = 768
GRAPH_DEGREE = 64
SCREEN_INDICES = np.arange(0, QUERIES, 4, dtype=np.int64)
PAGE_ROWS = (32, 64, 128, 256)
SOFT_PAGES = 32
SOFT_BYTES = 16 * 1024 * 1024
HARD_PAGES = 64
HARD_BYTES = 32 * 1024 * 1024


def scalar_column(path: Path, name: str, dtype: np.dtype) -> np.ndarray:
    table = pq.read_table(path, columns=[name])
    return np.asarray(
        table[name].combine_chunks().to_numpy(zero_copy_only=False), dtype=dtype
    )


def load_truth(path: Path) -> np.ndarray:
    table = pq.read_table(
        path, columns=["query_ordinal", "rank", "feature_row_id"]
    )
    if table.num_rows != QUERIES * NEIGHBORS:
        raise ValueError("ground-truth row count differs")
    query_ordinals = np.asarray(
        table["query_ordinal"].combine_chunks().to_numpy(zero_copy_only=False),
        dtype=np.int64,
    )
    ranks = np.asarray(
        table["rank"].combine_chunks().to_numpy(zero_copy_only=False),
        dtype=np.int64,
    )
    if not np.array_equal(
        query_ordinals, np.repeat(np.arange(QUERIES), NEIGHBORS)
    ) or not np.array_equal(ranks, np.tile(np.arange(NEIGHBORS), QUERIES)):
        raise ValueError("ground-truth order differs")
    return np.asarray(
        table["feature_row_id"].combine_chunks().to_numpy(zero_copy_only=False),
        dtype=np.uint64,
    ).reshape(QUERIES, NEIGHBORS)


def feature_ids_to_rows(feature_ids: np.ndarray, truth_ids: np.ndarray) -> np.ndarray:
    if feature_ids.shape != (ROWS,) or len(np.unique(feature_ids)) != ROWS:
        raise ValueError("source feature IDs differ")
    ordering = np.argsort(feature_ids, kind="stable")
    sorted_ids = feature_ids[ordering]
    positions = np.searchsorted(sorted_ids, truth_ids)
    if np.any(positions == ROWS) or not np.array_equal(
        sorted_ids[positions], truth_ids
    ):
        raise ValueError("ground truth references an unknown feature ID")
    return ordering[positions].astype(np.int32, copy=False)


def validate_order(path: Path | None) -> np.ndarray:
    if path is None:
        return np.arange(ROWS, dtype=np.int32)
    order = np.asarray(np.load(path, mmap_mode="r"), dtype=np.int32)
    if order.shape != (ROWS,) or not np.array_equal(
        np.sort(order), np.arange(ROWS, dtype=np.int32)
    ):
        raise ValueError(f"{path} is not a row permutation")
    return order


def nearest_rank(values: list[int], numerator: int, denominator: int) -> int:
    ordered = sorted(values)
    index = max(0, min(len(ordered) - 1, (len(ordered) * numerator - 1) // denominator))
    return ordered[index]


def evaluate_layout(
    packing: str, order: np.ndarray, truth_rows: np.ndarray, page_rows: int
) -> dict[str, object]:
    row_to_position = np.empty(ROWS, dtype=np.int32)
    row_to_position[order] = np.arange(ROWS, dtype=np.int32)
    page_bytes = 64 + page_rows * (16 + DIMENSIONS * 4 + GRAPH_DEGREE * 4)
    soft_page_cap = min(SOFT_PAGES, SOFT_BYTES // page_bytes)
    hard_page_cap = min(HARD_PAGES, HARD_BYTES // page_bytes)
    if soft_page_cap < 1 or hard_page_cap < 1:
        raise ValueError("encoded page exceeds the registered byte budget")

    soft_hits: list[int] = []
    hard_hits: list[int] = []
    distinct_pages: list[int] = []
    for query in SCREEN_INDICES:
        pages = row_to_position[truth_rows[query]] // page_rows
        unique_pages, counts = np.unique(pages, return_counts=True)
        ranked = sorted(
            zip(counts.tolist(), unique_pages.tolist()),
            key=lambda item: (-item[0], item[1]),
        )
        soft_hits.append(sum(count for count, _ in ranked[:soft_page_cap]))
        hard_hits.append(sum(count for count, _ in ranked[:hard_page_cap]))
        distinct_pages.append(len(ranked))

    aggregate_soft = sum(soft_hits) * 1_000_000 // (
        len(soft_hits) * NEIGHBORS
    )
    minimum_soft = min(soft_hits) * 10_000
    aggregate_hard = sum(hard_hits) * 1_000_000 // (
        len(hard_hits) * NEIGHBORS
    )
    minimum_hard = min(hard_hits) * 10_000
    used_pages = [min(count, soft_page_cap) for count in distinct_pages]
    return {
        "packing": packing,
        "page_rows": page_rows,
        "page_bytes": page_bytes,
        "soft_page_cap_after_bytes": soft_page_cap,
        "hard_page_cap_after_bytes": hard_page_cap,
        "aggregate_soft_oracle_recall_ppm": aggregate_soft,
        "minimum_soft_oracle_recall_ppm": minimum_soft,
        "aggregate_hard_oracle_recall_ppm": aggregate_hard,
        "minimum_hard_oracle_recall_ppm": minimum_hard,
        "p50_soft_pages": nearest_rank(used_pages, 50, 100),
        "p95_soft_pages": nearest_rank(used_pages, 95, 100),
        "maximum_distinct_gt_pages": max(distinct_pages),
        "soft_oracle_passed": aggregate_soft >= 998_000
        and minimum_soft >= 700_000,
        "hard_oracle_passed": minimum_hard == 1_000_000,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--ground-truth", type=Path, required=True)
    parser.add_argument("--bfs-order", type=Path, required=True)
    parser.add_argument("--random-order", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    feature_ids = scalar_column(args.source, "feature_row_id", np.uint64)
    truth_ids = load_truth(args.ground_truth)
    truth_rows = feature_ids_to_rows(feature_ids, truth_ids)
    layouts = {
        "original": validate_order(None),
        "bfs": validate_order(args.bfs_order),
        "random": validate_order(args.random_order),
    }
    cells = [
        evaluate_layout(name, order, truth_rows, page_rows)
        for name, order in layouts.items()
        for page_rows in PAGE_ROWS
    ]
    promoted = [
        cell
        for cell in cells
        if cell["soft_oracle_passed"] and cell["hard_oracle_passed"]
    ]
    result = {
        "schema": "borsuk-v62-algorithm-first-page-oracle-result-v1",
        "claim_eligible": False,
        "evidence_kind": "oracle-page-containment-not-query-routing",
        "source_rows": ROWS,
        "dimensions": DIMENSIONS,
        "queries": len(SCREEN_INDICES),
        "neighbors": NEIGHBORS,
        "soft_page_cap": SOFT_PAGES,
        "soft_byte_cap": SOFT_BYTES,
        "hard_page_cap": HARD_PAGES,
        "hard_byte_cap": HARD_BYTES,
        "query_or_ground_truth_not_used_to_construct_layouts": True,
        "cells": cells,
        "promoted_cells": promoted,
        "next_action": (
            "test-query-only-page-discovery"
            if promoted
            else "test-balanced-graph-and-geometric-partitioning"
        ),
        "validation_opened": False,
    }
    payload = (json.dumps(result, sort_keys=True, separators=(",", ":")) + "\n").encode()
    args.output.write_bytes(payload)
    print(
        json.dumps(
            {"result_sha256": hashlib.sha256(payload).hexdigest(), **result},
            sort_keys=True,
        ),
        flush=True,
    )


if __name__ == "__main__":
    main()
