#!/usr/bin/env python3
"""Fresh-build FAISS controls on a BORSUK/ANN-Benchmarks dataset."""

from __future__ import annotations

import argparse
import csv
import importlib.metadata
import json
import statistics
import time
from pathlib import Path


def effective_pq_subspaces(
    dimensions: int, bits_per_dimension: int, explicit: int
) -> int:
    subspaces = explicit or dimensions * bits_per_dimension // 8
    if subspaces <= 0 or dimensions % subspaces != 0:
        raise ValueError("dimensions must be divisible by the effective pq-subspaces")
    return subspaces


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


def timed_queries(
    index: object, queries: object, k: int
) -> tuple[list[float], list[list[int]]]:
    latencies: list[float] = []
    results: list[list[int]] = []
    for query in queries:
        started = time.perf_counter()
        _, ids = index.search(query.reshape(1, -1), k)
        latencies.append((time.perf_counter() - started) * 1000.0)
        results.append([int(value) for value in ids[0]])
    return latencies, results


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument(
        "--method",
        choices=("exact", "hnsw-flat", "hnsw-pq", "ivf-pq", "ivf-pq-refine"),
        required=True,
    )
    parser.add_argument("--queries", type=int, default=100)
    parser.add_argument("--threads", type=int, default=4)
    parser.add_argument("--hnsw-m", type=int, default=64)
    parser.add_argument("--ivf-lists", type=int, default=256)
    parser.add_argument("--bits-per-dimension", type=int, choices=(2, 4), default=4)
    parser.add_argument("--pq-subspaces", type=int, default=0)
    parser.add_argument("--training-rows", type=int, default=262_144)
    parser.add_argument("--add-batch-rows", type=int, default=65_536)
    parser.add_argument("--refine-factor", type=float, default=8.0)
    args = parser.parse_args()

    import faiss
    import numpy as np

    meta = json.loads((args.dataset / "meta.json").read_text())
    rows = int(meta["n_train"])
    dimensions = int(meta["dim"])
    query_count = min(args.queries, int(meta["n_test"]))
    neighbors = int(meta["k"])
    metric_name = str(meta["metric"])
    if metric_name not in {"euclidean", "cosine", "angular"}:
        raise ValueError(f"unsupported FAISS control metric {metric_name}")
    pq_subspaces = effective_pq_subspaces(
        dimensions, args.bits_per_dimension, args.pq_subspaces
    )

    faiss.omp_set_num_threads(args.threads)
    train = np.memmap(
        args.dataset / "train.f32", dtype="<f4", mode="r", shape=(rows, dimensions)
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
        shape=(int(meta["n_test"]), neighbors),
    )[:query_count, :10]
    metric = (
        faiss.METRIC_L2 if metric_name == "euclidean" else faiss.METRIC_INNER_PRODUCT
    )
    normalized_metric = metric_name in {"cosine", "angular"}
    if normalized_metric:
        queries = np.asarray(queries).copy()
        faiss.normalize_L2(queries)

    def prepared_rows(row_ids: object) -> object:
        values = np.asarray(train[row_ids], dtype=np.float32).copy()
        if normalized_metric:
            faiss.normalize_L2(values)
        return values

    build_started = time.perf_counter()
    ivf_base = None
    if args.method == "exact":
        index = faiss.IndexFlat(dimensions, metric)
        parameters = [("exact", 0)]
    elif args.method == "hnsw-flat":
        index = faiss.IndexHNSWFlat(dimensions, args.hnsw_m, metric)
        index.hnsw.efConstruction = 200
        parameters = [("ef_search", value) for value in (16, 32, 64, 128, 256, 512)]
    elif args.method == "hnsw-pq":
        index = faiss.index_factory(
            dimensions, f"HNSW{args.hnsw_m},PQ{pq_subspaces}", metric
        )
        index.hnsw.efConstruction = 200
        training_count = min(rows, args.training_rows)
        training_ids = np.linspace(0, rows - 1, training_count, dtype=np.int64)
        index.train(prepared_rows(training_ids))
        parameters = [("ef_search", value) for value in (16, 32, 64, 128, 256, 512)]
    elif args.method == "ivf-pq":
        coarse = faiss.IndexFlat(dimensions, metric)
        index = faiss.IndexIVFPQ(
            coarse, dimensions, args.ivf_lists, pq_subspaces, 8, metric
        )
        training_count = min(rows, args.training_rows)
        training_ids = np.linspace(0, rows - 1, training_count, dtype=np.int64)
        index.train(prepared_rows(training_ids))
        ivf_base = index
        parameters = [("nprobe", value) for value in (1, 2, 4, 8, 16, 32, 64, 128)]
    else:
        index = faiss.index_factory(
            dimensions,
            f"IVF{args.ivf_lists},PQ{pq_subspaces},RFlat",
            metric,
        )
        training_count = min(rows, args.training_rows)
        training_ids = np.linspace(0, rows - 1, training_count, dtype=np.int64)
        index.train(prepared_rows(training_ids))
        index.k_factor = args.refine_factor
        ivf_base = faiss.downcast_index(index.base_index)
        parameters = [("nprobe", value) for value in (1, 2, 4, 8, 16, 32, 64, 128)]
    for start in range(0, rows, args.add_batch_rows):
        index.add(prepared_rows(slice(start, min(rows, start + args.add_batch_rows))))
    build_ms = (time.perf_counter() - build_started) * 1000.0

    args.output_dir.mkdir(parents=True, exist_ok=True)
    index_path = args.output_dir / "index.faiss"
    faiss.write_index(index, str(index_path))
    with (args.output_dir / "build.csv").open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "dataset",
                "method",
                "package_release",
                "vectors",
                "dimensions",
                "threads",
                "bits_per_dimension",
                "pq_subspaces",
                "build_ms",
                "index_bytes",
                "cache_state",
                "latency_role",
            ]
        )
        writer.writerow(
            [
                meta["name"],
                args.method,
                importlib.metadata.version("faiss-cpu"),
                rows,
                dimensions,
                args.threads,
                args.bits_per_dimension,
                pq_subspaces,
                f"{build_ms:.3f}",
                index_path.stat().st_size,
                "memory-resident-index",
                "optimized-local-library-control",
            ]
        )

    with (args.output_dir / "query.csv").open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "dataset",
                "method",
                "parameter",
                "value",
                "queries",
                "recall_at_10",
                "mean_ms",
                "stddev_ms",
                "p50_ms",
                "p95_ms",
                "p99_ms",
                "max_ms",
                "cache_state",
                "latency_scope",
            ]
        )
        for parameter, value in parameters:
            if parameter == "ef_search":
                index.hnsw.efSearch = value
            elif parameter == "nprobe":
                ivf_base.nprobe = value
            latencies, actual = timed_queries(index, queries, 10)
            writer.writerow(
                [
                    meta["name"],
                    args.method,
                    parameter,
                    value,
                    query_count,
                    f"{recall_at_k(actual, truth.tolist(), 10):.6f}",
                    f"{statistics.fmean(latencies):.6f}",
                    f"{sample_stddev(latencies):.6f}",
                    f"{percentile(latencies, 0.50):.6f}",
                    f"{percentile(latencies, 0.95):.6f}",
                    f"{percentile(latencies, 0.99):.6f}",
                    f"{max(latencies):.6f}",
                    "memory-resident-index",
                    "optimized-library-search",
                ]
            )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
