#!/usr/bin/env python3
"""Disposable resident binary-signature router falsifier for ReLAION2B 1M."""

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


BITS = 192
BYTES = BITS // 8
RANKED_ROWS = 4096
SEED = 20260914
BUILD_BATCH = 8192
POPCOUNT = np.array([int(value).bit_count() for value in range(256)], dtype=np.uint8)


def hyperplanes() -> np.ndarray:
    rng = np.random.default_rng(SEED)
    return np.where(
        rng.integers(0, 2, size=(BITS, common.DIMENSIONS), dtype=np.uint8) == 0,
        -1.0,
        1.0,
    ).astype(np.float32)


def encode(vectors: np.ndarray, planes: np.ndarray) -> tuple[np.ndarray, float]:
    started = time.perf_counter()
    weights = torch.from_numpy(planes)
    codes = np.empty((len(vectors), BYTES), dtype=np.uint8)
    with torch.no_grad():
        for start in range(0, len(vectors), BUILD_BATCH):
            scores = torch.from_numpy(vectors[start : start + BUILD_BATCH]) @ weights.T
            codes[start : start + len(scores)] = np.packbits(
                scores.numpy() >= 0.0, axis=1, bitorder="little"
            )
    return codes, time.perf_counter() - started


def closest_rows(query_code: np.ndarray, codes: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    distances = POPCOUNT[np.bitwise_xor(codes, query_code)].sum(axis=1, dtype=np.uint16)
    candidate = np.argpartition(distances, RANKED_ROWS)[:RANKED_ROWS]
    order = np.lexsort((candidate, distances[candidate]))
    rows = candidate[order]
    return rows, distances[rows]


def select_pages(rows: np.ndarray, primary: np.ndarray, alternate: np.ndarray) -> np.ndarray:
    evidence = rows[:100]
    pairs = np.stack((primary[evidence], alternate[evidence]), axis=1)
    candidates = sorted(set(pairs[:, 0]) | set(pairs[pairs[:, 1] >= 0, 1]))
    selected: set[int] = set()
    while len(selected) < common.SELECTED_PAGES:
        best = None
        for page in candidates:
            if page in selected:
                continue
            gain = 0
            for first, second in pairs:
                if first in selected or (second >= 0 and second in selected):
                    continue
                if page == first or page == second:
                    gain += 1
            score = (gain, -int(page))
            if best is None or score > best[0]:
                best = (score, int(page))
        if best is None:
            break
        selected.add(best[1])
    for page in range(common.PAGES):
        if len(selected) == common.SELECTED_PAGES:
            break
        selected.add(page)
    return np.array(sorted(selected), dtype=np.int64)


def map_truth(feature_ids: np.ndarray, truth: np.ndarray) -> np.ndarray:
    order = np.argsort(feature_ids)
    ordered = feature_ids[order]
    positions = np.searchsorted(ordered, truth)
    if np.any(positions >= common.ROWS) or np.any(ordered[positions] != truth):
        raise ValueError("ground truth/source binding differs")
    return order[positions]


def evaluate(
    query_indices: np.ndarray,
    query_codes: np.ndarray,
    truth_rows: np.ndarray,
    codes: np.ndarray,
    primary: np.ndarray,
    alternate: np.ndarray,
    aggregate_gate: int,
    minimum_gate: int,
) -> dict:
    final_hits = []
    candidate_hits = 0
    ranked_hits = 0
    timings = []
    maximum_hamming = []
    for query in query_indices:
        started = time.perf_counter_ns()
        rows, distances = closest_rows(query_codes[query], codes)
        candidate_truth = np.isin(truth_rows[query], rows)
        ranked_truth = np.isin(truth_rows[query], rows[:100])
        candidate_hits += int(candidate_truth.sum())
        ranked_hits += int(ranked_truth.sum())
        selected = select_pages(rows, primary, alternate)
        hit = np.isin(primary[truth_rows[query]], selected) | np.isin(
            alternate[truth_rows[query]], selected
        )
        final_hits.append(int(hit.sum()))
        maximum_hamming.append(int(distances[-1]))
        timings.append((time.perf_counter_ns() - started) // 1000)
    timings.sort()
    final = np.asarray(final_hits)
    aggregate = int(final.sum() * 1_000_000 // (len(final) * 100))
    minimum = int(final.min() * 10_000)
    return {
        "queries": len(final_hits),
        "aggregate_recall_ppm": aggregate,
        "minimum_recall_ppm": minimum,
        "candidate_truth_recall_ppm": int(candidate_hits * 1_000_000 // (len(final) * 100)),
        "ranked_truth_recall_ppm": int(ranked_hits * 1_000_000 // (len(final) * 100)),
        "p50_query_us": int(timings[len(timings) // 2]),
        "p99_query_us": int(timings[min(len(timings) - 1, len(timings) * 99 // 100)]),
        "median_ranked_threshold_hamming": int(np.median(maximum_hamming)),
        "passed": aggregate >= aggregate_gate and minimum >= minimum_gate,
    }


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--development-query", type=Path, required=True)
    parser.add_argument("--development-ground-truth", type=Path, required=True)
    parser.add_argument("--spill-relation", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    torch.set_num_threads(os.cpu_count() or 1)
    vectors, feature_ids, primary, alternate = common.load_corpus(args.source, args.spill_relation)
    planes = hyperplanes()
    codes, construction_seconds = encode(vectors, planes)
    query_vectors, truth = common.load_queries(
        args.development_query, args.development_ground_truth
    )
    query_codes, query_encoding_seconds = encode(query_vectors, planes)
    truth_rows = map_truth(feature_ids, truth)
    screen = evaluate(
        np.arange(0, common.QUERIES, 4),
        query_codes,
        truth_rows,
        codes,
        primary,
        alternate,
        990_000,
        700_000,
    )
    development = None
    if screen["passed"]:
        development = evaluate(
            np.arange(common.QUERIES),
            query_codes,
            truth_rows,
            codes,
            primary,
            alternate,
            995_000,
            800_000,
        )
    projected_signature_bytes = 100_000_000 * BYTES
    projected_owner_bytes = 100_000_000 * 4
    result = {
        "schema": "borsuk-v54-algorithm-first-binary-scan-result-v1",
        "claim_eligible": False,
        "source_rows": common.ROWS,
        "dimensions": common.DIMENSIONS,
        "signature_bits": BITS,
        "ranked_rows": RANKED_ROWS,
        "reducer_evidence_rows": 100,
        "selected_pages": common.SELECTED_PAGES,
        "seed": SEED,
        "construction_elapsed_ms": int(construction_seconds * 1000),
        "query_encoding_elapsed_us_per_query": int(query_encoding_seconds * 1_000_000 / common.QUERIES),
        "projected_100m_signature_bytes": projected_signature_bytes,
        "projected_100m_owner_bytes": projected_owner_bytes,
        "projected_100m_resident_bytes": projected_signature_bytes + projected_owner_bytes,
        "projected_routing_gets_per_query": 0,
        "logical_write_bytes_per_row": BYTES + 4,
        "screen": screen,
        "development": development,
        "validation_opened": False,
    }
    payload = (json.dumps(result, sort_keys=True, separators=(",", ":")) + "\n").encode()
    args.output.write_bytes(payload)
    print(json.dumps({"result_sha256": hashlib.sha256(payload).hexdigest(), **result}), flush=True)


if __name__ == "__main__":
    main()
