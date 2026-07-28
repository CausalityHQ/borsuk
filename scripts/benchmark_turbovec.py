#!/usr/bin/env python3
"""Fresh-build TurboVec control with optional exact candidate reranking."""

from __future__ import annotations

import argparse
import csv
import importlib.metadata
import json
import statistics
import time
from pathlib import Path

import numpy as np


def percentile(values: list[float], fraction: float) -> float:
    ordered = sorted(values)
    rank = int((len(ordered) - 1) * fraction + 0.999999)
    return ordered[min(len(ordered) - 1, max(0, rank))]


def sample_stddev(values: list[float]) -> float:
    return statistics.stdev(values) if len(values) > 1 else 0.0


def recall_at_k(actual: list[list[int]], expected: list[list[int]], k: int) -> float:
    return sum(
        len(set(got[:k]) & set(want[:k])) / k
        for got, want in zip(actual, expected, strict=True)
    ) / len(actual)


def normalized_rows(values: np.ndarray) -> np.ndarray:
    norms = np.linalg.norm(values, axis=1, keepdims=True)
    return values / np.where(norms > 0.0, norms, 1.0)


def exact_rerank(
    metric: str,
    train: np.ndarray,
    queries: np.ndarray,
    candidates: list[list[int]],
    k: int,
) -> list[list[int]]:
    query_rows = np.asarray(queries, dtype=np.float32)
    if metric in {"angular", "cosine"}:
        query_rows = normalized_rows(query_rows)
    results: list[list[int]] = []
    for query, ids in zip(query_rows, candidates, strict=True):
        candidate_ids = np.asarray(ids, dtype=np.int64)
        vectors = np.asarray(train[candidate_ids], dtype=np.float32)
        if metric in {"angular", "cosine"}:
            vectors = normalized_rows(vectors)
        scores = vectors @ query
        order = np.argsort(-scores, kind="stable")[:k]
        results.append([int(candidate_ids[index]) for index in order])
    return results


def padded(values: np.ndarray, dimensions: int) -> np.ndarray:
    if values.shape[1] == dimensions:
        return np.ascontiguousarray(values, dtype=np.float32)
    output = np.zeros((values.shape[0], dimensions), dtype=np.float32)
    output[:, : values.shape[1]] = values
    return output


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--queries", type=int, default=100)
    parser.add_argument("--bit-width", type=int, choices=(2, 3, 4), default=4)
    parser.add_argument("--candidates", default="16,32,64,128,256,512")
    args = parser.parse_args()

    from turbovec import TurboQuantIndex

    meta = json.loads((args.dataset / "meta.json").read_text())
    metric = str(meta["metric"]).lower()
    if metric not in {"angular", "cosine", "inner-product", "dot"}:
        raise ValueError(
            "TurboVec is an inner-product/cosine control; do not relabel it as Euclidean"
        )
    rows = int(meta["n_train"])
    dimensions = int(meta["dim"])
    stored_dimensions = ((dimensions + 7) // 8) * 8
    query_count = min(args.queries, int(meta["n_test"]))
    truth_width = int(meta["k"])
    candidate_budgets = [int(value) for value in args.candidates.split(",")]
    if not candidate_budgets or any(value < 10 for value in candidate_budgets):
        raise ValueError("candidate budgets must all be at least 10")

    train = np.memmap(
        args.dataset / "train.f32",
        dtype="<f4",
        mode="r",
        shape=(rows, dimensions),
    )
    queries = np.memmap(
        args.dataset / "test.f32",
        dtype="<f4",
        mode="r",
        shape=(int(meta["n_test"]), dimensions),
    )[:query_count]
    truth = np.memmap(
        args.dataset / "neighbors.i32",
        dtype="<i4",
        mode="r",
        shape=(int(meta["n_test"]), truth_width),
    )[:query_count, :10]

    build_started = time.perf_counter()
    index = TurboQuantIndex(dim=stored_dimensions, bit_width=args.bit_width)
    index.add(padded(np.asarray(train), stored_dimensions))
    add_ms = (time.perf_counter() - build_started) * 1000.0
    prepare_started = time.perf_counter()
    index.prepare()
    prepare_ms = (time.perf_counter() - prepare_started) * 1000.0

    args.output_dir.mkdir(parents=True, exist_ok=True)
    index_path = args.output_dir / "index.tv"
    index.write(index_path)
    package_release = importlib.metadata.version("turbovec")
    with (args.output_dir / "build.csv").open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "dataset",
                "engine",
                "package_release",
                "vectors",
                "dimensions",
                "stored_dimensions",
                "bit_width",
                "add_ms",
                "prepare_ms",
                "index_bytes",
                "rotation",
                "calibration",
                "score_correction",
                "latency_role",
            ]
        )
        writer.writerow(
            [
                meta["name"],
                "turbovec",
                package_release,
                rows,
                dimensions,
                stored_dimensions,
                args.bit_width,
                f"{add_ms:.3f}",
                f"{prepare_ms:.3f}",
                index_path.stat().st_size,
                "dense-random-orthogonal",
                "tq-plus-first-add",
                "length-renormalized",
                "optimized-derivative-local-resident-control",
            ]
        )

    padded_queries = padded(np.asarray(queries), stored_dimensions)
    with (args.output_dir / "query.csv").open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "dataset",
                "engine",
                "package_release",
                "bit_width",
                "candidate_budget",
                "queries",
                "quantized_recall_at_10",
                "reranked_recall_at_10",
                "mean_ms",
                "stddev_ms",
                "p50_ms",
                "p95_ms",
                "p99_ms",
                "max_ms",
                "search_p95_ms",
                "rerank_p95_ms",
                "search_mean_ms",
                "search_stddev_ms",
                "rerank_mean_ms",
                "rerank_stddev_ms",
                "cache_state",
                "latency_scope",
            ]
        )
        for budget in candidate_budgets:
            latencies: list[float] = []
            search_latencies: list[float] = []
            rerank_latencies: list[float] = []
            quantized: list[list[int]] = []
            reranked: list[list[int]] = []
            for query_index, query in enumerate(padded_queries):
                started = time.perf_counter()
                _, ids = index.search(query.reshape(1, -1), k=budget)
                search_finished = time.perf_counter()
                candidates = [int(value) for value in ids[0]]
                quantized.append(candidates[:10])
                reranked.extend(
                    exact_rerank(
                        metric,
                        train,
                        np.asarray(queries[query_index : query_index + 1]),
                        [candidates],
                        10,
                    )
                )
                finished = time.perf_counter()
                search_latencies.append((search_finished - started) * 1000.0)
                rerank_latencies.append((finished - search_finished) * 1000.0)
                latencies.append((finished - started) * 1000.0)
            writer.writerow(
                [
                    meta["name"],
                    "turbovec",
                    package_release,
                    args.bit_width,
                    budget,
                    query_count,
                    f"{recall_at_k(quantized, truth.tolist(), 10):.6f}",
                    f"{recall_at_k(reranked, truth.tolist(), 10):.6f}",
                    f"{statistics.fmean(latencies):.6f}",
                    f"{sample_stddev(latencies):.6f}",
                    f"{percentile(latencies, 0.50):.6f}",
                    f"{percentile(latencies, 0.95):.6f}",
                    f"{percentile(latencies, 0.99):.6f}",
                    f"{max(latencies):.6f}",
                    f"{percentile(search_latencies, 0.95):.6f}",
                    f"{percentile(rerank_latencies, 0.95):.6f}",
                    f"{statistics.fmean(search_latencies):.6f}",
                    f"{sample_stddev(search_latencies):.6f}",
                    f"{statistics.fmean(rerank_latencies):.6f}",
                    f"{sample_stddev(rerank_latencies):.6f}",
                    "memory-resident-index",
                    "optimized-library-search-plus-exact-rerank",
                ]
            )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
