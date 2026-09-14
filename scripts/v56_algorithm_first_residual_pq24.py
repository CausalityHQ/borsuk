#!/usr/bin/env python3
"""Disposable original-space residual-PQ24 quality falsifier on ReLAION2B 1M."""

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


SUBQUANTIZERS = 24
SUBDIMENSIONS = common.DIMENSIONS // SUBQUANTIZERS
CENTROIDS = 256
CODE_BYTES = SUBQUANTIZERS
TRAINING_ROWS = 65_536
LLOYD_ITERATIONS = 10
ENCODE_BATCH = 8_192
RANKED_ROWS = 4_096
SEED = 20260914


def splitmix64(values: np.ndarray) -> np.ndarray:
    values = values.astype(np.uint64, copy=True) + np.uint64(0x9E3779B97F4A7C15)
    values = (values ^ (values >> np.uint64(30))) * np.uint64(0xBF58476D1CE4E5B9)
    values = (values ^ (values >> np.uint64(27))) * np.uint64(0x94D049BB133111EB)
    return values ^ (values >> np.uint64(31))


def training_indices() -> np.ndarray:
    ordinals = np.arange(common.ROWS, dtype=np.uint64)
    keys = splitmix64(ordinals ^ np.uint64(SEED))
    selected = np.argpartition(keys, TRAINING_ROWS)[:TRAINING_ROWS]
    return selected[np.lexsort((selected, keys[selected]))]


def primary_anchors(vectors: np.ndarray, primary: np.ndarray) -> np.ndarray:
    sums = np.zeros((common.PAGES, common.DIMENSIONS), dtype=np.float64)
    counts = np.bincount(primary, minlength=common.PAGES)
    if np.any(counts == 0):
        raise ValueError("empty primary anchor")
    np.add.at(sums, primary, vectors)
    anchors = (sums / counts[:, None]).astype(np.float32)
    if not np.isfinite(anchors).all():
        raise ValueError("primary anchor differs")
    return anchors


def train_codebooks(sample: np.ndarray) -> tuple[np.ndarray, list[float]]:
    codebooks = np.empty(
        (SUBQUANTIZERS, CENTROIDS, SUBDIMENSIONS), dtype=np.float32
    )
    losses = []
    with torch.no_grad():
        for subquantizer in range(SUBQUANTIZERS):
            values = torch.from_numpy(
                sample[
                    :,
                    subquantizer * SUBDIMENSIONS : (subquantizer + 1) * SUBDIMENSIONS,
                ]
            )
            centers = values[:CENTROIDS].clone()
            labels = torch.empty(len(values), dtype=torch.int64)
            for _ in range(LLOYD_ITERATIONS):
                distances = (
                    values.square().sum(1, keepdim=True)
                    + centers.square().sum(1).unsqueeze(0)
                    - 2.0 * values @ centers.T
                )
                labels = distances.argmin(1)
                sums = torch.zeros_like(centers)
                sums.index_add_(0, labels, values)
                counts = torch.bincount(labels, minlength=CENTROIDS)
                populated = counts > 0
                centers[populated] = sums[populated] / counts[populated, None]
            residual = values - centers[labels]
            losses.append(float(residual.square().mean()))
            codebooks[subquantizer] = centers.numpy()
    return codebooks, losses


def encode(
    vectors: np.ndarray, primary: np.ndarray, anchors: np.ndarray, codebooks: np.ndarray
) -> tuple[np.ndarray, float]:
    codes = np.empty((common.ROWS, SUBQUANTIZERS), dtype=np.uint8)
    started = time.perf_counter()
    with torch.no_grad():
        centers = torch.from_numpy(codebooks)
        for start in range(0, common.ROWS, ENCODE_BATCH):
            end = min(start + ENCODE_BATCH, common.ROWS)
            residual = torch.from_numpy(vectors[start:end] - anchors[primary[start:end]])
            for subquantizer in range(SUBQUANTIZERS):
                values = residual[
                    :,
                    subquantizer * SUBDIMENSIONS : (subquantizer + 1) * SUBDIMENSIONS,
                ]
                current = centers[subquantizer]
                distances = (
                    values.square().sum(1, keepdim=True)
                    + current.square().sum(1).unsqueeze(0)
                    - 2.0 * values @ current.T
                )
                codes[start:end, subquantizer] = distances.argmin(1).numpy().astype(
                    np.uint8
                )
    return codes, time.perf_counter() - started


def adc_scores(
    query: np.ndarray,
    primary: np.ndarray,
    anchors: np.ndarray,
    codebooks: np.ndarray,
    codes: np.ndarray,
) -> np.ndarray:
    query_residuals = (query[None, :] - anchors).reshape(
        common.PAGES, SUBQUANTIZERS, SUBDIMENSIONS
    )
    tables = np.empty((common.PAGES, SUBQUANTIZERS, CENTROIDS), dtype=np.float32)
    for subquantizer in range(SUBQUANTIZERS):
        delta = (
            query_residuals[:, subquantizer, None, :]
            - codebooks[subquantizer][None, :, :]
        )
        tables[:, subquantizer] = np.einsum("pkd,pkd->pk", delta, delta)
    scores = np.zeros(common.ROWS, dtype=np.float32)
    for subquantizer in range(SUBQUANTIZERS):
        scores += tables[primary, subquantizer, codes[:, subquantizer]]
    if not np.isfinite(scores).all():
        raise ValueError("ADC score differs")
    return scores


def ranked_rows(scores: np.ndarray) -> np.ndarray:
    candidates = np.argpartition(scores, RANKED_ROWS)[:RANKED_ROWS]
    return candidates[np.lexsort((candidates, scores[candidates]))]


def evaluate(
    indices: np.ndarray,
    queries: np.ndarray,
    truth_rows: np.ndarray,
    primary: np.ndarray,
    alternate: np.ndarray,
    anchors: np.ndarray,
    codebooks: np.ndarray,
    codes: np.ndarray,
    promotion: bool,
) -> dict:
    final_hits = []
    candidate_hits = 0
    ranked_hits = 0
    timings = []
    for query_index in indices:
        started = time.perf_counter_ns()
        rows = ranked_rows(
            adc_scores(queries[query_index], primary, anchors, codebooks, codes)
        )
        candidate_hits += int(np.isin(truth_rows[query_index], rows).sum())
        ranked_hits += int(np.isin(truth_rows[query_index], rows[:100]).sum())
        selected = binary.select_pages(rows, primary, alternate)
        hit = np.isin(primary[truth_rows[query_index]], selected) | np.isin(
            alternate[truth_rows[query_index]], selected
        )
        final_hits.append(int(hit.sum()))
        timings.append((time.perf_counter_ns() - started) // 1_000)
    final = np.asarray(final_hits)
    timings.sort()
    aggregate = int(final.sum() * 1_000_000 // (len(final) * 100))
    minimum = int(final.min() * 10_000)
    aggregate_gate, minimum_gate = (995_000, 800_000) if promotion else (990_000, 700_000)
    return {
        "queries": len(final_hits),
        "aggregate_recall_ppm": aggregate,
        "minimum_recall_ppm": minimum,
        "candidate_truth_recall_ppm": int(
            candidate_hits * 1_000_000 // (len(final) * 100)
        ),
        "ranked_truth_recall_ppm": int(
            ranked_hits * 1_000_000 // (len(final) * 100)
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
    anchors = primary_anchors(vectors, primary)
    sample_rows = training_indices()
    sample = vectors[sample_rows] - anchors[primary[sample_rows]]
    training_started = time.perf_counter()
    codebooks, training_losses = train_codebooks(sample)
    training_seconds = time.perf_counter() - training_started
    codes, encoding_seconds = encode(vectors, primary, anchors, codebooks)
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
        anchors,
        codebooks,
        codes,
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
            anchors,
            codebooks,
            codes,
            True,
        )
    result = {
        "schema": "borsuk-v56-algorithm-first-residual-pq24-result-v1",
        "claim_eligible": False,
        "source_rows": common.ROWS,
        "dimensions": common.DIMENSIONS,
        "primary_anchor_count": common.PAGES,
        "subquantizers": SUBQUANTIZERS,
        "subdimensions": SUBDIMENSIONS,
        "centroids_per_subquantizer": CENTROIDS,
        "code_bytes_per_row": CODE_BYTES,
        "training_rows": TRAINING_ROWS,
        "lloyd_iterations": LLOYD_ITERATIONS,
        "ranked_rows": RANKED_ROWS,
        "selected_pages": common.SELECTED_PAGES,
        "seed": SEED,
        "training_elapsed_ms": int(training_seconds * 1_000),
        "encoding_elapsed_ms": int(encoding_seconds * 1_000),
        "training_mse_by_subquantizer": training_losses,
        "projected_100m_code_bytes": 100_000_000 * CODE_BYTES,
        "projected_100m_owner_bytes": 100_000_000 * 4,
        "projected_100m_resident_bytes": 100_000_000 * (CODE_BYTES + 4),
        "logical_write_bytes_per_row": CODE_BYTES + 4,
        "routing_gets_per_query": 0,
        "page_gets_per_query": common.SELECTED_PAGES,
        "global_scan_serving_qualified": False,
        "screen": screen,
        "development": development,
        "validation_opened": False,
    }
    payload = (json.dumps(result, sort_keys=True, separators=(",", ":")) + "\n").encode()
    args.output.write_bytes(payload)
    print(json.dumps({"result_sha256": hashlib.sha256(payload).hexdigest(), **result}), flush=True)


if __name__ == "__main__":
    main()
