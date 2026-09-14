#!/usr/bin/env python3
"""Disposable cross-polytope distributional page-sketch probe on ReLAION2B 1M."""

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
import v55_algorithm_first_cross_polytope as cross


TABLE_LADDER = (4, 8, 16)
COORDINATE_LADDER = (1, 2, 4)
SCORING_MODES = ("additive", "paired-naive-bayes")


def build_sketch(
    vectors: np.ndarray,
    primary: np.ndarray,
    alternate: np.ndarray,
    all_signs: torch.Tensor,
) -> tuple[np.ndarray, np.ndarray, float]:
    counts = np.zeros(
        (cross.TABLES, cross.ROTATIONS, cross.BUCKETS, common.PAGES),
        dtype=np.uint16,
    )
    populations = np.zeros(
        (cross.TABLES, cross.ROTATIONS, cross.BUCKETS), dtype=np.uint32
    )
    started = time.perf_counter()
    with torch.no_grad():
        for start in range(0, common.ROWS, cross.BUILD_BATCH):
            rows = torch.from_numpy(vectors[start : start + cross.BUILD_BATCH])
            row_primary = primary[start : start + len(rows)]
            row_alternate = alternate[start : start + len(rows)]
            for table in range(cross.TABLES):
                for rotation in range(cross.ROTATIONS):
                    buckets = cross.signed_top(
                        cross.rotate(rows, all_signs[table, rotation]), 1
                    )[:, 0]
                    np.add.at(populations[table, rotation], buckets, 1)
                    np.add.at(counts[table, rotation], (buckets, row_primary), 1)
                    valid = row_alternate >= 0
                    np.add.at(
                        counts[table, rotation],
                        (buckets[valid], row_alternate[valid]),
                        1,
                    )
    return counts, populations, time.perf_counter() - started


def query_coordinates(
    vectors: np.ndarray, all_signs: torch.Tensor
) -> tuple[np.ndarray, float]:
    values = torch.from_numpy(vectors)
    coordinates = np.empty(
        (
            len(vectors),
            cross.TABLES,
            cross.ROTATIONS,
            max(COORDINATE_LADDER),
        ),
        dtype=np.uint16,
    )
    started = time.perf_counter()
    with torch.no_grad():
        for table in range(cross.TABLES):
            for rotation in range(cross.ROTATIONS):
                coordinates[:, table, rotation] = cross.signed_top(
                    cross.rotate(values, all_signs[table, rotation]),
                    max(COORDINATE_LADDER),
                )
    return coordinates, time.perf_counter() - started


def score_pages(
    query_coordinates: np.ndarray,
    counts: np.ndarray,
    populations: np.ndarray,
    tables: int,
    coordinate_count: int,
    mode: str,
) -> np.ndarray:
    scores = np.zeros(common.PAGES, dtype=np.float64)
    weights = 1.0 / (1.0 + np.arange(coordinate_count, dtype=np.float64))
    for table in range(tables):
        left_buckets = query_coordinates[table, 0, :coordinate_count]
        right_buckets = query_coordinates[table, 1, :coordinate_count]
        left = counts[table, 0, left_buckets].astype(np.float64)
        right = counts[table, 1, right_buckets].astype(np.float64)
        left_denominator = populations[table, 0, left_buckets, None]
        right_denominator = populations[table, 1, right_buckets, None]
        np.divide(left, left_denominator, out=left, where=left_denominator != 0)
        np.divide(right, right_denominator, out=right, where=right_denominator != 0)
        if mode == "additive":
            scores += (left * weights[:, None]).sum(axis=0)
            scores += (right * weights[:, None]).sum(axis=0)
        elif mode == "paired-naive-bayes":
            for first in range(coordinate_count):
                for second in range(coordinate_count):
                    scores += (
                        weights[first]
                        * weights[second]
                        * left[first]
                        * right[second]
                    )
        else:
            raise ValueError("scoring mode differs")
    pages = np.arange(common.PAGES)
    return np.lexsort((pages, -scores))[: common.SELECTED_PAGES]


def evaluate(
    indices: np.ndarray,
    coordinates: np.ndarray,
    truth_rows: np.ndarray,
    primary: np.ndarray,
    alternate: np.ndarray,
    counts: np.ndarray,
    populations: np.ndarray,
    tables: int,
    coordinate_count: int,
    mode: str,
    promotion: bool,
) -> dict:
    hits = []
    timings = []
    for query in indices:
        started = time.perf_counter_ns()
        pages = score_pages(
            coordinates[query], counts, populations, tables, coordinate_count, mode
        )
        truth = truth_rows[query]
        covered = np.isin(primary[truth], pages) | np.isin(alternate[truth], pages)
        hits.append(int(covered.sum()))
        timings.append((time.perf_counter_ns() - started) // 1_000)
    hit_array = np.asarray(hits)
    timings.sort()
    aggregate = int(hit_array.sum() * 1_000_000 // (len(hits) * 100))
    minimum = int(hit_array.min() * 10_000)
    aggregate_gate, minimum_gate = (995_000, 800_000) if promotion else (990_000, 700_000)
    return {
        "tables": tables,
        "query_coordinates_per_rotation": coordinate_count,
        "scoring_mode": mode,
        "queries": len(hits),
        "aggregate_recall_ppm": aggregate,
        "minimum_recall_ppm": minimum,
        "p50_query_us": int(timings[len(timings) // 2]),
        "p99_query_us": int(
            timings[min(len(timings) - 1, len(timings) * 99 // 100)]
        ),
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
    all_signs = cross.signs()
    counts, populations, construction_seconds = build_sketch(
        vectors, primary, alternate, all_signs
    )
    queries, truth = common.load_queries(
        args.development_query, args.development_ground_truth
    )
    coordinates, query_hash_seconds = query_coordinates(queries, all_signs)
    truth_rows = binary.map_truth(feature_ids, truth)
    screen_indices = np.arange(0, common.QUERIES, 4)
    screens = []
    for tables in TABLE_LADDER:
        for coordinate_count in COORDINATE_LADDER:
            for mode in SCORING_MODES:
                screens.append(
                    evaluate(
                        screen_indices,
                        coordinates,
                        truth_rows,
                        primary,
                        alternate,
                        counts,
                        populations,
                        tables,
                        coordinate_count,
                        mode,
                        False,
                    )
                )
    passing = [cell for cell in screens if cell["passed"]]
    chosen = min(
        passing,
        key=lambda cell: (
            cell["tables"],
            cell["query_coordinates_per_rotation"],
            SCORING_MODES.index(cell["scoring_mode"]),
        ),
        default=None,
    )
    development = None
    if chosen is not None:
        development = evaluate(
            np.arange(common.QUERIES),
            coordinates,
            truth_rows,
            primary,
            alternate,
            counts,
            populations,
            int(chosen["tables"]),
            int(chosen["query_coordinates_per_rotation"]),
            str(chosen["scoring_mode"]),
            True,
        )
    pages_at_100m = common.PAGES * 100
    maximum_resident_bytes = (
        cross.TABLES
        * cross.ROTATIONS
        * cross.BUCKETS
        * pages_at_100m
        * 2
    )
    result = {
        "schema": "borsuk-v59-algorithm-first-cross-polytope-page-sketch-result-v1",
        "claim_eligible": False,
        "source_rows": common.ROWS,
        "dimensions": common.DIMENSIONS,
        "table_ladder": TABLE_LADDER,
        "coordinate_ladder": COORDINATE_LADDER,
        "scoring_modes": SCORING_MODES,
        "selected_pages": common.SELECTED_PAGES,
        "construction_elapsed_ms": int(construction_seconds * 1_000),
        "query_hash_elapsed_us_per_query": int(
            query_hash_seconds * 1_000_000 / common.QUERIES
        ),
        "screen_cells": screens,
        "chosen_screen_cell": chosen,
        "development": development,
        "validation_opened": False,
        "routing_gets_per_query": 0,
        "maximum_logical_counter_updates_per_write": cross.TABLES * cross.ROTATIONS,
        "projected_100m_pages": pages_at_100m,
        "projected_100m_dense_sketch_bytes_u16": maximum_resident_bytes,
        "exact_row_scan_or_full_vector_rerank": False,
        "page_layout": "frozen-v38-diagnostic-only",
    }
    payload = (json.dumps(result, sort_keys=True, separators=(",", ":")) + "\n").encode()
    args.output.write_bytes(payload)
    print(json.dumps({"result_sha256": hashlib.sha256(payload).hexdigest(), **result}), flush=True)


if __name__ == "__main__":
    main()
