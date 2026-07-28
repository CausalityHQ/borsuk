#!/usr/bin/env python3
"""Dense-rotation TurboQuant paper reference, rebuilt from raw vectors.

This is a correctness/recall control for the original dense Haar rotation and,
for ``prod``, the dense Gaussian QJL residual stage. It is intentionally labeled
as a NumPy reference rather than a production SIMD competitor.
"""

from __future__ import annotations

import argparse
import csv
import json
import statistics
import time
from pathlib import Path

import numpy as np

SQRT_PI_OVER_2 = np.float32(1.2533141373155001)


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


def haar_rotation(dimensions: int, seed: int) -> np.ndarray:
    rng = np.random.default_rng(seed)
    gaussian = rng.standard_normal((dimensions, dimensions))
    orthogonal, upper = np.linalg.qr(gaussian)
    diagonal = np.diag(upper)
    signs = np.where(diagonal < 0.0, -1.0, 1.0)
    return np.asarray(orthogonal * signs, dtype=np.float32)


def lloyd_max_sphere_codebook(
    dimensions: int, bits: int, grid_points: int = 16_384, iterations: int = 64
) -> tuple[np.ndarray, np.ndarray]:
    levels = 1 << bits
    step = 2.0 / grid_points
    points = -1.0 + (np.arange(grid_points, dtype=np.float64) + 0.5) * step
    exponent = (dimensions - 3.0) * 0.5
    if dimensions <= 2:
        weights = np.ones_like(points)
    else:
        log_weights = exponent * np.log(np.maximum(1.0 - points * points, 1e-300))
        weights = np.exp(log_weights - np.max(log_weights))
    cumulative = np.cumsum(weights)
    targets = (np.arange(levels, dtype=np.float64) + 0.5) / levels * cumulative[-1]
    centroids = points[np.searchsorted(cumulative, targets)].copy()
    for _ in range(iterations):
        boundaries = (centroids[:-1] + centroids[1:]) * 0.5
        buckets = np.searchsorted(boundaries, points)
        weighted = np.bincount(buckets, weights=weights * points, minlength=levels)
        mass = np.bincount(buckets, weights=weights, minlength=levels)
        updated = np.divide(weighted, mass, out=centroids.copy(), where=mass > 0.0)
        if np.max(np.abs(updated - centroids)) < 1e-12:
            centroids = updated
            break
        centroids = updated
    boundaries = (centroids[:-1] + centroids[1:]) * 0.5
    return boundaries.astype(np.float32), centroids.astype(np.float32)


def metric_distances(metric: str, query: np.ndarray, vectors: np.ndarray) -> np.ndarray:
    if metric in {"angular", "cosine"}:
        qnorm = np.linalg.norm(query)
        norms = np.linalg.norm(vectors, axis=1)
        denominator = np.where(norms * qnorm > 0.0, norms * qnorm, 1.0)
        return 1.0 - (vectors @ query) / denominator
    if metric in {"inner-product", "dot"}:
        return -(vectors @ query)
    if metric in {"euclidean", "l2"}:
        delta = vectors - query
        return np.einsum("ij,ij->i", delta, delta)
    raise ValueError(f"unsupported metric {metric}")


def approximate_distances(
    metric: str,
    query_norm: float,
    vector_norms: np.ndarray,
    approximate_unit_dots: np.ndarray,
) -> np.ndarray:
    if metric in {"angular", "cosine"}:
        return 1.0 - approximate_unit_dots
    scaled_dot = query_norm * vector_norms * approximate_unit_dots
    if metric in {"inner-product", "dot"}:
        return -scaled_dot
    if metric in {"euclidean", "l2"}:
        return query_norm * query_norm + vector_norms * vector_norms - 2.0 * scaled_dot
    raise ValueError(f"unsupported metric {metric}")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--variant", choices=("mse", "prod"), required=True)
    parser.add_argument("--bit-width", type=int, choices=range(2, 9), default=4)
    parser.add_argument("--queries", type=int, default=100)
    parser.add_argument("--candidates", default="16,32,64,128,256,512")
    parser.add_argument("--seed", type=int, default=794060427680530654)
    parser.add_argument("--batch-rows", type=int, default=16_384)
    args = parser.parse_args()

    meta = json.loads((args.dataset / "meta.json").read_text())
    rows = int(meta["n_train"])
    dimensions = int(meta["dim"])
    query_count = min(args.queries, int(meta["n_test"]))
    truth_width = int(meta["k"])
    metric = str(meta["metric"]).lower()
    normalized_geometry = metric in {"angular", "cosine"}
    candidates = [int(value) for value in args.candidates.split(",")]
    if not candidates or any(value < 10 or value > rows for value in candidates):
        raise ValueError("candidate budgets must be in 10..=dataset rows")

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
        shape=(int(meta["n_test"]), truth_width),
    )[:query_count, :10]

    args.output_dir.mkdir(parents=True, exist_ok=True)
    scalar_bits = args.bit_width if args.variant == "mse" else args.bit_width - 1
    rotation_started = time.perf_counter()
    rotation = haar_rotation(dimensions, args.seed)
    qjl = (
        np.random.default_rng(args.seed ^ 0x51574A4C5F32D1CE)
        .standard_normal((dimensions, dimensions))
        .astype(np.float32)
        if args.variant == "prod"
        else None
    )
    boundaries, centroids = lloyd_max_sphere_codebook(dimensions, scalar_bits)
    setup_ms = (time.perf_counter() - rotation_started) * 1000.0

    codes_path = args.output_dir / "scalar-codes.u8"
    norms_path = args.output_dir / "vector-norms.f32"
    codes = np.memmap(codes_path, dtype=np.uint8, mode="w+", shape=(rows, dimensions))
    norms = np.memmap(norms_path, dtype="<f4", mode="w+", shape=(rows,))
    sign_width = (dimensions + 7) // 8
    signs_path = args.output_dir / "qjl-signs.u1" if args.variant == "prod" else None
    residual_path = (
        args.output_dir / "residual-norms.f32" if args.variant == "prod" else None
    )
    signs = (
        np.memmap(signs_path, dtype=np.uint8, mode="w+", shape=(rows, sign_width))
        if signs_path is not None
        else None
    )
    residual_norms = (
        np.memmap(residual_path, dtype="<f4", mode="w+", shape=(rows,))
        if residual_path is not None
        else None
    )

    encode_started = time.perf_counter()
    for start in range(0, rows, args.batch_rows):
        end = min(rows, start + args.batch_rows)
        batch = np.asarray(train[start:end], dtype=np.float32)
        batch_norms = np.linalg.norm(batch, axis=1)
        unit = batch / np.where(batch_norms[:, None] > 0.0, batch_norms[:, None], 1.0)
        rotated = unit @ rotation.T
        batch_codes = np.searchsorted(boundaries, rotated).astype(np.uint8)
        codes[start:end] = batch_codes
        norms[start:end] = 1.0 if normalized_geometry else batch_norms
        if signs is not None and residual_norms is not None and qjl is not None:
            residual = rotated - centroids[batch_codes]
            residual_norms[start:end] = np.linalg.norm(residual, axis=1)
            projected = residual @ qjl.T
            signs[start:end] = np.packbits(projected < 0.0, axis=1, bitorder="little")
    codes.flush()
    norms.flush()
    if signs is not None and residual_norms is not None:
        signs.flush()
        residual_norms.flush()
    encode_ms = (time.perf_counter() - encode_started) * 1000.0

    theoretical_code_bytes = rows * (
        (dimensions * scalar_bits + 7) // 8
        + 4
        + (sign_width + 4 if args.variant == "prod" else 0)
    )
    actual_reference_bytes = codes_path.stat().st_size + norms_path.stat().st_size
    if signs_path is not None and residual_path is not None:
        actual_reference_bytes += (
            signs_path.stat().st_size + residual_path.stat().st_size
        )
    with (args.output_dir / "build.csv").open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "dataset",
                "engine",
                "variant",
                "vectors",
                "dimensions",
                "bit_width",
                "seed",
                "setup_ms",
                "encode_ms",
                "theoretical_packed_bytes",
                "reference_working_bytes",
                "rotation_state_bytes",
                "qjl_state_bytes",
                "rotation",
                "qjl_projection",
                "latency_role",
            ]
        )
        writer.writerow(
            [
                meta["name"],
                "dense-turboquant-numpy-reference",
                args.variant,
                rows,
                dimensions,
                args.bit_width,
                args.seed,
                f"{setup_ms:.3f}",
                f"{encode_ms:.3f}",
                theoretical_code_bytes,
                actual_reference_bytes,
                rotation.nbytes,
                0 if qjl is None else qjl.nbytes,
                "dense-haar-qr",
                "none" if qjl is None else "dense-iid-gaussian",
                "correctness-reference-not-production-kernel",
            ]
        )

    with (args.output_dir / "query.csv").open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "dataset",
                "engine",
                "variant",
                "bit_width",
                "seed",
                "candidate_budget",
                "queries",
                "recall_at_10",
                "mean_ms",
                "stddev_ms",
                "p50_ms",
                "p95_ms",
                "p99_ms",
                "max_ms",
                "scan_p95_ms",
                "rerank_p95_ms",
                "scan_mean_ms",
                "scan_stddev_ms",
                "rerank_mean_ms",
                "rerank_stddev_ms",
                "cache_state",
                "latency_scope",
            ]
        )
        actual = {budget: [] for budget in candidates}
        latencies = {budget: [] for budget in candidates}
        scan_latencies: list[float] = []
        rerank_latencies = {budget: [] for budget in candidates}
        for query in queries:
            scan_started = time.perf_counter()
            query_norm = float(np.linalg.norm(query))
            unit_query = (
                query / query_norm if query_norm > 0.0 else np.zeros_like(query)
            )
            if normalized_geometry:
                query_norm = 1.0
            rotated_query = rotation @ unit_query
            qjl_query = qjl @ rotated_query if qjl is not None else None
            scores = np.empty(rows, dtype=np.float32)
            for start in range(0, rows, args.batch_rows):
                end = min(rows, start + args.batch_rows)
                decoded = centroids[np.asarray(codes[start:end])]
                dot = decoded @ rotated_query
                if (
                    qjl_query is not None
                    and signs is not None
                    and residual_norms is not None
                ):
                    sign_values = np.unpackbits(
                        np.asarray(signs[start:end]),
                        axis=1,
                        count=dimensions,
                        bitorder="little",
                    )
                    signed = 1.0 - 2.0 * sign_values.astype(np.float32)
                    dot += (
                        SQRT_PI_OVER_2
                        * np.asarray(residual_norms[start:end])
                        / dimensions
                        * (signed @ qjl_query)
                    )
                vector_norms = np.asarray(norms[start:end])
                scores[start:end] = approximate_distances(
                    metric,
                    query_norm,
                    vector_norms,
                    dot,
                )
            scan_ms = (time.perf_counter() - scan_started) * 1000.0
            scan_latencies.append(scan_ms)
            for budget in candidates:
                rerank_started = time.perf_counter()
                shortlist = np.argpartition(scores, budget - 1)[:budget]
                exact = metric_distances(
                    metric, np.asarray(query), np.asarray(train[shortlist])
                )
                order = np.argsort(exact, kind="stable")[:10]
                actual[budget].append([int(shortlist[index]) for index in order])
                rerank_ms = (time.perf_counter() - rerank_started) * 1000.0
                rerank_latencies[budget].append(rerank_ms)
                latencies[budget].append(scan_ms + rerank_ms)
        for budget in candidates:
            writer.writerow(
                [
                    meta["name"],
                    "dense-turboquant-numpy-reference",
                    args.variant,
                    args.bit_width,
                    args.seed,
                    budget,
                    query_count,
                    f"{recall_at_k(actual[budget], truth.tolist(), 10):.6f}",
                    f"{statistics.fmean(latencies[budget]):.6f}",
                    f"{sample_stddev(latencies[budget]):.6f}",
                    f"{percentile(latencies[budget], 0.50):.6f}",
                    f"{percentile(latencies[budget], 0.95):.6f}",
                    f"{percentile(latencies[budget], 0.99):.6f}",
                    f"{max(latencies[budget]):.6f}",
                    f"{percentile(scan_latencies, 0.95):.6f}",
                    f"{percentile(rerank_latencies[budget], 0.95):.6f}",
                    f"{statistics.fmean(scan_latencies):.6f}",
                    f"{sample_stddev(scan_latencies):.6f}",
                    f"{statistics.fmean(rerank_latencies[budget]):.6f}",
                    f"{sample_stddev(rerank_latencies[budget]):.6f}",
                    "local-mmap-os-cache-unspecified",
                    "python-numpy-reference-including-exact-rerank",
                ]
            )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
