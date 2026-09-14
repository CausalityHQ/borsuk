#!/usr/bin/env python3
"""Disposable four-partition leaf-to-page incidence router on ReLAION2B 1M."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import time
from pathlib import Path

import numpy as np
import torch

import v53_algorithm_first_page_mlp as common
import v54_algorithm_first_binary_scan as binary


PARTITIONS = 4
DEPTH = 13
LEAVES = 1 << DEPTH
TOP_LEAVES = 128
SELECTED_PAGES = 21
SEEDS = (20260914, 20260915, 20260916, 20260917)


def splitmix64(values: np.ndarray) -> np.ndarray:
    values = values.astype(np.uint64, copy=True) + np.uint64(0x9E3779B97F4A7C15)
    values = (values ^ (values >> np.uint64(30))) * np.uint64(0xBF58476D1CE4E5B9)
    values = (values ^ (values >> np.uint64(27))) * np.uint64(0x94D049BB133111EB)
    return values ^ (values >> np.uint64(31))


def build_partition(vectors: np.ndarray, seed: int):
    assignments = np.empty(common.ROWS, dtype=np.uint16)
    nodes = [np.arange(common.ROWS, dtype=np.uint32)]
    for depth in range(DEPTH):
        children = []
        for ordinal, rows in enumerate(nodes):
            node_seed = np.uint64(seed) ^ np.uint64((depth + 1) * 0x9E37 + ordinal)
            keys = splitmix64(rows.astype(np.uint64) ^ node_seed)
            left_seed = (keys & np.uint64(1)) == 0
            if np.all(left_seed) or not np.any(left_seed):
                left_seed = np.arange(len(rows)) < len(rows) // 2
            direction = (
                vectors[rows[left_seed]].mean(axis=0, dtype=np.float32)
                - vectors[rows[~left_seed]].mean(axis=0, dtype=np.float32)
            )
            if not np.isfinite(direction).all() or float(direction @ direction) == 0.0:
                coordinate = int(splitmix64(np.array([node_seed]))[0] % common.DIMENSIONS)
                direction = np.zeros(common.DIMENSIONS, dtype=np.float32)
                direction[coordinate] = 1.0
            scores = vectors[rows] @ direction
            order = np.lexsort((rows, scores))
            middle = len(rows) // 2
            children.append(rows[order[:middle]])
            children.append(rows[order[middle:]])
        nodes = children
    centroids = np.empty((LEAVES, common.DIMENSIONS), dtype=np.float32)
    populations = np.empty(LEAVES, dtype=np.uint16)
    for leaf, rows in enumerate(nodes):
        assignments[rows] = leaf
        populations[leaf] = len(rows)
        centroids[leaf] = vectors[rows].mean(axis=0, dtype=np.float32)
    if np.any(populations == 0) or int(populations.sum()) != common.ROWS:
        raise ValueError("leaf populations differ")
    return assignments, centroids, populations


def page_incidence(
    assignments: np.ndarray,
    populations: np.ndarray,
    primary: np.ndarray,
    alternate: np.ndarray,
) -> np.ndarray:
    counts = np.zeros((LEAVES, common.PAGES), dtype=np.uint16)
    np.add.at(counts, (assignments, primary), 1)
    valid = alternate >= 0
    np.add.at(counts, (assignments[valid], alternate[valid]), 1)
    if np.any(counts.sum(axis=1) < populations):
        raise ValueError("leaf incidence differs")
    return counts


def top_leaves(query: np.ndarray, centroids: np.ndarray) -> np.ndarray:
    distances = (
        np.einsum("ij,ij->i", centroids, centroids)
        - 2.0 * (centroids @ query)
        + float(query @ query)
    )
    candidates = np.argpartition(distances, TOP_LEAVES)[:TOP_LEAVES]
    return candidates[np.lexsort((candidates, distances[candidates]))]


def select_pages(
    leaves_by_partition: list[np.ndarray],
    populations: list[np.ndarray],
    incidence: list[np.ndarray],
) -> np.ndarray:
    scores = np.zeros(common.PAGES, dtype=np.float64)
    for leaves, current_populations, current_incidence in zip(
        leaves_by_partition, populations, incidence, strict=True
    ):
        weights = 1.0 / (60.0 + np.arange(TOP_LEAVES, dtype=np.float64))
        normalized = current_incidence[leaves].astype(np.float64) / current_populations[
            leaves, None
        ]
        scores += (normalized * weights[:, None]).sum(axis=0)
    pages = np.arange(common.PAGES)
    return np.lexsort((pages, -scores))[:SELECTED_PAGES]


def evaluate(
    indices: np.ndarray,
    queries: np.ndarray,
    truth_rows: np.ndarray,
    primary: np.ndarray,
    alternate: np.ndarray,
    assignments: list[np.ndarray],
    centroids: list[np.ndarray],
    populations: list[np.ndarray],
    incidence: list[np.ndarray],
    promotion: bool,
) -> dict:
    hits = []
    leaf_truth_hits = 0
    timings = []
    for query_index in indices:
        started = time.perf_counter_ns()
        leaves_by_partition = [
            top_leaves(queries[query_index], current) for current in centroids
        ]
        truth = truth_rows[query_index]
        leaf_truth_hits += int(
            np.logical_or.reduce(
                [np.isin(current[truth], leaves) for current, leaves in zip(assignments, leaves_by_partition, strict=True)]
            ).sum()
        )
        pages = select_pages(leaves_by_partition, populations, incidence)
        covered = np.isin(primary[truth], pages) | np.isin(alternate[truth], pages)
        hits.append(int(covered.sum()))
        timings.append((time.perf_counter_ns() - started) // 1_000)
    hits_array = np.asarray(hits)
    timings.sort()
    aggregate = int(hits_array.sum() * 1_000_000 // (len(hits) * 100))
    minimum = int(hits_array.min() * 10_000)
    aggregate_gate, minimum_gate = (995_000, 800_000) if promotion else (990_000, 700_000)
    return {
        "queries": len(hits),
        "aggregate_recall_ppm": aggregate,
        "minimum_recall_ppm": minimum,
        "top_leaf_truth_recall_ppm": int(
            leaf_truth_hits * 1_000_000 // (len(hits) * 100)
        ),
        "p50_query_us": int(timings[len(timings) // 2]),
        "p99_query_us": int(timings[min(len(timings) - 1, len(timings) * 99 // 100)]),
        "passed": aggregate >= aggregate_gate and minimum >= minimum_gate,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--development-query", type=Path, required=True)
    parser.add_argument("--development-ground-truth", type=Path, required=True)
    parser.add_argument("--spill-relation", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    torch.set_num_threads(os.cpu_count() or 1)
    vectors, feature_ids, primary, alternate = common.load_corpus(
        args.source, args.spill_relation
    )
    built_assignments = []
    built_centroids = []
    built_populations = []
    built_incidence = []
    started = time.perf_counter()
    for seed in SEEDS:
        current_assignments, current_centroids, current_populations = build_partition(
            vectors, seed
        )
        built_assignments.append(current_assignments)
        built_centroids.append(current_centroids)
        built_populations.append(current_populations)
        built_incidence.append(
            page_incidence(current_assignments, current_populations, primary, alternate)
        )
    construction_seconds = time.perf_counter() - started
    queries, truth = common.load_queries(
        args.development_query, args.development_ground_truth
    )
    truth_rows = binary.map_truth(feature_ids, truth)
    screen = evaluate(
        np.arange(0, common.QUERIES, 4),
        queries,
        truth_rows,
        primary,
        alternate,
        built_assignments,
        built_centroids,
        built_populations,
        built_incidence,
        False,
    )
    development = None
    if screen["passed"]:
        development = evaluate(
            np.arange(common.QUERIES),
            queries,
            truth_rows,
            primary,
            alternate,
            built_assignments,
            built_centroids,
            built_populations,
            built_incidence,
            True,
        )
    projected_leaf_centroid_bytes = PARTITIONS * LEAVES * common.DIMENSIONS * 2
    projected_dense_incidence_bytes = PARTITIONS * LEAVES * common.PAGES * 2
    result = {
        "schema": "borsuk-v57-algorithm-first-leaf-incidence-result-v1",
        "claim_eligible": False,
        "source_rows": common.ROWS,
        "dimensions": common.DIMENSIONS,
        "partitions": PARTITIONS,
        "depth": DEPTH,
        "leaves_per_partition": LEAVES,
        "top_leaves_per_partition": TOP_LEAVES,
        "selected_pages": SELECTED_PAGES,
        "seeds": SEEDS,
        "construction_elapsed_ms": int(construction_seconds * 1_000),
        "resident_leaf_centroid_bytes_f16_projection": projected_leaf_centroid_bytes,
        "resident_dense_incidence_bytes_u16_projection": projected_dense_incidence_bytes,
        "resident_row_state_bytes_at_100m": 100_000_000 * PARTITIONS * 2,
        "logical_router_write_bytes_per_row": PARTITIONS * 2,
        "routing_gets_per_query": 0,
        "page_gets_per_query": SELECTED_PAGES,
        "screen": screen,
        "development": development,
        "validation_opened": False,
    }
    payload = (json.dumps(result, sort_keys=True, separators=(",", ":")) + "\n").encode()
    args.output.write_bytes(payload)
    print(json.dumps({"result_sha256": hashlib.sha256(payload).hexdigest(), **result}), flush=True)


if __name__ == "__main__":
    main()
