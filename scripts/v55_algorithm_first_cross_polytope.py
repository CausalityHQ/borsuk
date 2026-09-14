#!/usr/bin/env python3
"""Disposable paired cross-polytope row router on frozen ReLAION2B 1M."""

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


PAD = 1024
TABLES = 16
ROTATIONS = 2
ROUNDS = 3
QUERY_COORDINATES = 4
BUCKETS = PAD * 2
BUILD_BATCH = 65_536
SEED = 20260914


def signs() -> torch.Tensor:
    rng = np.random.default_rng(SEED)
    values = np.where(
        rng.integers(0, 2, size=(TABLES, ROTATIONS, ROUNDS, PAD), dtype=np.uint8) == 0,
        -1.0,
        1.0,
    ).astype(np.float32)
    return torch.from_numpy(values)


def hadamard(values: torch.Tensor) -> torch.Tensor:
    width = 1
    rows = len(values)
    while width < PAD:
        shaped = values.reshape(rows, -1, width * 2)
        left = shaped[:, :, :width]
        right = shaped[:, :, width:]
        values = torch.cat((left + right, left - right), dim=2).reshape(rows, PAD)
        width *= 2
    return values * (1.0 / 32.0)


def rotate(values: torch.Tensor, round_signs: torch.Tensor) -> torch.Tensor:
    padded = torch.zeros((len(values), PAD), dtype=torch.float32)
    padded[:, : common.DIMENSIONS] = values
    for current in round_signs:
        padded = hadamard(padded * current)
    return padded


def signed_top(rotated: torch.Tensor, count: int) -> np.ndarray:
    coordinates = torch.topk(rotated.abs(), count, dim=1, largest=True, sorted=True).indices
    selected = torch.gather(rotated, 1, coordinates)
    return (coordinates * 2 + (selected < 0).to(torch.int64)).numpy().astype(np.uint16)


def build_signatures(vectors: np.ndarray, all_signs: torch.Tensor):
    signatures = np.empty((common.ROWS, TABLES, ROTATIONS), dtype=np.uint16)
    started = time.perf_counter()
    with torch.no_grad():
        for start in range(0, common.ROWS, BUILD_BATCH):
            rows = torch.from_numpy(vectors[start : start + BUILD_BATCH])
            for table in range(TABLES):
                for rotation in range(ROTATIONS):
                    signatures[start : start + len(rows), table, rotation] = signed_top(
                        rotate(rows, all_signs[table, rotation]), 1
                    )[:, 0]
    indexes = []
    for table in range(TABLES):
        keys = signatures[:, table, 0].astype(np.uint32) * BUCKETS + signatures[:, table, 1]
        order = np.argsort(keys, kind="stable").astype(np.uint32)
        indexes.append((keys[order], order))
    return signatures, indexes, time.perf_counter() - started


def query_probes(vectors: np.ndarray, all_signs: torch.Tensor):
    values = torch.from_numpy(vectors)
    probes = np.empty((len(vectors), TABLES, ROTATIONS, QUERY_COORDINATES), dtype=np.uint16)
    started = time.perf_counter()
    with torch.no_grad():
        for table in range(TABLES):
            for rotation in range(ROTATIONS):
                probes[:, table, rotation] = signed_top(
                    rotate(values, all_signs[table, rotation]), QUERY_COORDINATES
                )
    return probes, time.perf_counter() - started


def candidates_for_query(probes: np.ndarray, indexes) -> np.ndarray:
    pieces = []
    for table, (keys, order) in enumerate(indexes):
        for first in probes[table, 0]:
            for second in probes[table, 1]:
                key = np.uint32(first) * BUCKETS + np.uint32(second)
                lower = np.searchsorted(keys, key, side="left")
                upper = np.searchsorted(keys, key, side="right")
                pieces.append(order[lower:upper])
    if not pieces:
        return np.empty(0, dtype=np.uint32)
    return np.unique(np.concatenate(pieces))


def exact_rank(query: np.ndarray, vectors: np.ndarray, candidates: np.ndarray) -> np.ndarray:
    if len(candidates) == 0:
        return candidates
    delta = vectors[candidates] - query
    distances = np.einsum("ij,ij->i", delta, delta)
    return candidates[np.lexsort((candidates, distances))]


def evaluate(indices, queries, probes, truth_rows, vectors, primary, alternate, indexes, promotion):
    hits = []
    candidate_hits = 0
    ranked_hits = 0
    counts = []
    timings = []
    for query in indices:
        started = time.perf_counter_ns()
        candidates = candidates_for_query(probes[query], indexes)
        ranked = exact_rank(queries[query], vectors, candidates)
        candidate_hits += int(np.isin(truth_rows[query], candidates).sum())
        ranked_hits += int(np.isin(truth_rows[query], ranked[:100]).sum())
        selected = binary.select_pages(ranked, primary, alternate)
        final = np.isin(primary[truth_rows[query]], selected) | np.isin(
            alternate[truth_rows[query]], selected
        )
        hits.append(int(final.sum()))
        counts.append(len(candidates))
        timings.append((time.perf_counter_ns() - started) // 1000)
    hits_array = np.asarray(hits)
    timings.sort()
    counts.sort()
    aggregate = int(hits_array.sum() * 1_000_000 // (len(hits) * 100))
    minimum = int(hits_array.min() * 10_000)
    aggregate_gate, minimum_gate = (995_000, 800_000) if promotion else (990_000, 700_000)
    return {
        "queries": len(hits),
        "aggregate_recall_ppm": aggregate,
        "minimum_recall_ppm": minimum,
        "candidate_truth_recall_ppm": int(candidate_hits * 1_000_000 // (len(hits) * 100)),
        "ranked_truth_recall_ppm": int(ranked_hits * 1_000_000 // (len(hits) * 100)),
        "median_candidate_rows": int(counts[len(counts) // 2]),
        "p99_candidate_rows": int(counts[min(len(counts) - 1, len(counts) * 99 // 100)]),
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
    vectors, feature_ids, primary, alternate = common.load_corpus(args.source, args.spill_relation)
    all_signs = signs()
    _, indexes, construction_seconds = build_signatures(vectors, all_signs)
    queries, truth = common.load_queries(args.development_query, args.development_ground_truth)
    probes, query_hash_seconds = query_probes(queries, all_signs)
    truth_rows = binary.map_truth(feature_ids, truth)
    screen = evaluate(
        np.arange(0, common.QUERIES, 4), queries, probes, truth_rows, vectors, primary, alternate, indexes, False
    )
    development = None
    if screen["passed"]:
        development = evaluate(
            np.arange(common.QUERIES), queries, probes, truth_rows, vectors, primary, alternate, indexes, True
        )
    result = {
        "schema": "borsuk-v55-algorithm-first-cross-polytope-result-v1",
        "claim_eligible": False,
        "source_rows": common.ROWS,
        "dimensions": common.DIMENSIONS,
        "tables": TABLES,
        "paired_rotations_per_table": ROTATIONS,
        "hadamard_rounds_per_rotation": ROUNDS,
        "query_coordinates_per_rotation": QUERY_COORDINATES,
        "logical_bucket_probes": TABLES * QUERY_COORDINATES * QUERY_COORDINATES,
        "packed_primary_bucket_gets": TABLES * QUERY_COORDINATES,
        "selected_pages": common.SELECTED_PAGES,
        "seed": SEED,
        "construction_elapsed_ms": int(construction_seconds * 1000),
        "query_hash_elapsed_us_per_query": int(query_hash_seconds * 1_000_000 / common.QUERIES),
        "projected_100m_posting_bytes": 100_000_000 * TABLES * 6,
        "projected_100m_resident_pq24_and_owners_bytes": 2_800_000_000,
        "logical_write_bytes_per_row": TABLES * 6 + 28,
        "screen": screen,
        "development": development,
        "validation_opened": False,
    }
    payload = (json.dumps(result, sort_keys=True, separators=(",", ":")) + "\n").encode()
    args.output.write_bytes(payload)
    print(json.dumps({"result_sha256": hashlib.sha256(payload).hexdigest(), **result}), flush=True)


if __name__ == "__main__":
    main()
