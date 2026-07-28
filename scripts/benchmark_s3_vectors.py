#!/usr/bin/env python3
"""Benchmark Amazon S3 Vectors on an ANN-Benchmarks dataset.

The script deliberately keeps ingestion and query output machine-readable so
the comparison can state dataset, recall, cache order, and client placement
instead of comparing unrelated vendor headline numbers.
"""

from __future__ import annotations

import argparse
import csv
import json
import statistics
import struct
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path


def read_meta(dataset: Path) -> dict[str, object]:
    return json.loads((dataset / "meta.json").read_text())


def normalize_metric(metric: str) -> str:
    normalized = metric.lower()
    if normalized == "angular":
        return "cosine"
    if normalized in {"cosine", "euclidean"}:
        return normalized
    raise ValueError(f"unsupported S3 Vectors metric {metric}")


def read_f32_rows(path: Path, rows: int, dimensions: int) -> list[list[float]]:
    row_bytes = dimensions * 4
    result: list[list[float]] = []
    with path.open("rb") as handle:
        for _ in range(rows):
            payload = handle.read(row_bytes)
            if len(payload) != row_bytes:
                raise ValueError(f"{path} ended before {rows} rows")
            result.append(list(struct.unpack(f"<{dimensions}f", payload)))
    return result


def read_ground_truth(path: Path, rows: int, neighbors: int, k: int) -> list[set[str]]:
    row_bytes = neighbors * 4
    result: list[set[str]] = []
    with path.open("rb") as handle:
        for _ in range(rows):
            payload = handle.read(row_bytes)
            if len(payload) != row_bytes:
                raise ValueError(f"{path} ended before {rows} rows")
            ids = struct.unpack(f"<{neighbors}i", payload)
            result.append({str(value) for value in ids[:k]})
    return result


def percentile(values: list[float], fraction: float) -> float:
    ordered = sorted(values)
    position = max(
        0, min(len(ordered) - 1, int(len(ordered) * fraction + 0.999999) - 1)
    )
    return ordered[position]


def sample_stddev(values: list[float]) -> float:
    return statistics.stdev(values) if len(values) > 1 else 0.0


def permuted_positions(count: int, seed: int) -> list[int]:
    positions = list(range(count))
    mask = (1 << 64) - 1
    state = (seed ^ 0x9E3779B97F4A7C15) & mask
    for upper in range(count - 1, 0, -1):
        state = (state + 0x9E3779B97F4A7C15) & mask
        mixed = state
        mixed = ((mixed ^ (mixed >> 30)) * 0xBF58476D1CE4E5B9) & mask
        mixed = ((mixed ^ (mixed >> 27)) * 0x94D049BB133111EB) & mask
        mixed ^= mixed >> 31
        other = mixed % (upper + 1)
        positions[upper], positions[other] = positions[other], positions[upper]
    return positions


def wait_for_index(
    client: object, bucket: str, index: str, timeout_seconds: int = 300
) -> None:
    deadline = time.monotonic() + timeout_seconds
    while True:
        try:
            client.get_index(vectorBucketName=bucket, indexName=index)
            return
        except client.exceptions.NotFoundException as error:
            if time.monotonic() >= deadline:
                raise TimeoutError(
                    f"S3 Vectors index {bucket}/{index} was not ready"
                ) from error
            time.sleep(2)


def ingest(
    client: object,
    dataset: Path,
    rows: int,
    dimensions: int,
    bucket: str,
    index: str,
    workers: int,
) -> float:
    row_bytes = dimensions * 4
    batches: list[list[dict[str, object]]] = []
    started = time.monotonic()
    with dataset.joinpath("train.f32").open("rb") as handle:
        row = 0
        while row < rows:
            batch: list[dict[str, object]] = []
            for _ in range(min(500, rows - row)):
                payload = handle.read(row_bytes)
                if len(payload) != row_bytes:
                    raise ValueError("train.f32 ended before meta.json n_train")
                batch.append(
                    {
                        "key": str(row),
                        "data": {
                            "float32": list(struct.unpack(f"<{dimensions}f", payload))
                        },
                    }
                )
                row += 1
            batches.append(batch)
            if len(batches) >= workers:
                with ThreadPoolExecutor(max_workers=workers) as executor:
                    list(
                        executor.map(
                            lambda vectors: client.put_vectors(
                                vectorBucketName=bucket,
                                indexName=index,
                                vectors=vectors,
                            ),
                            batches,
                        )
                    )
                batches.clear()
    if batches:
        with ThreadPoolExecutor(max_workers=workers) as executor:
            list(
                executor.map(
                    lambda vectors: client.put_vectors(
                        vectorBucketName=bucket,
                        indexName=index,
                        vectors=vectors,
                    ),
                    batches,
                )
            )
    return time.monotonic() - started


def query_pass(
    client: object,
    queries: list[list[float]],
    truth: list[set[str]],
    bucket: str,
    index: str,
    k: int,
    positions: list[int],
    label: str,
    repetition_id: str,
    query_seed: int,
) -> list[dict[str, object]]:
    if len(queries) != len(truth):
        raise ValueError(
            f"query/truth row mismatch: {len(queries)} queries, {len(truth)} truth rows"
        )
    if sorted(positions) != list(range(len(queries))):
        raise ValueError("query positions must be a complete permutation")
    samples: list[dict[str, object]] = []
    for query_position, source_index in enumerate(positions):
        query = queries[source_index]
        expected = truth[source_index]
        started = time.monotonic()
        response = client.query_vectors(
            vectorBucketName=bucket,
            indexName=index,
            queryVector={"float32": query},
            topK=k,
            returnDistance=True,
        )
        latency_ms = (time.monotonic() - started) * 1000
        actual = {row["key"] for row in response["vectors"]}
        samples.append(
            {
                "pass": label,
                "repetition_id": repetition_id,
                "query_seed": query_seed,
                "query_position": query_position,
                "query_source_index": source_index,
                "latency_ms": latency_ms,
                "recall_at_10": len(expected & actual) / k,
            }
        )
    return samples


def main() -> int:
    import boto3
    from botocore.config import Config

    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", type=Path, required=True)
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--index", required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--profile", default=None)
    parser.add_argument("--region", default="eu-central-1")
    parser.add_argument("--queries", type=int, default=100)
    parser.add_argument("--workers", type=int, default=5)
    parser.add_argument("--query-seed", type=int, required=True)
    parser.add_argument("--repetition-id", required=True)
    args = parser.parse_args()

    meta = read_meta(args.dataset)
    dimensions = int(meta["dim"])
    rows = int(meta["n_train"])
    neighbors = int(meta["k"])
    metric = normalize_metric(str(meta["metric"]))
    query_count = min(args.queries, int(meta["n_test"]))
    session = boto3.Session(profile_name=args.profile, region_name=args.region)
    client = session.client(
        "s3vectors",
        config=Config(retries={"mode": "adaptive", "max_attempts": 10}),
    )

    args.output_dir.mkdir(parents=True, exist_ok=True)
    created_bucket = False
    created_index = False
    try:
        client.create_vector_bucket(vectorBucketName=args.bucket)
        created_bucket = True
        client.create_index(
            vectorBucketName=args.bucket,
            indexName=args.index,
            dataType="float32",
            dimension=dimensions,
            distanceMetric=metric,
        )
        created_index = True
        wait_for_index(client, args.bucket, args.index)
        ingest_seconds = ingest(
            client,
            args.dataset,
            rows,
            dimensions,
            args.bucket,
            args.index,
            args.workers,
        )

        queries = read_f32_rows(args.dataset / "test.f32", query_count, dimensions)
        truth = read_ground_truth(
            args.dataset / "neighbors.i32", query_count, neighbors, 10
        )
        positions = permuted_positions(query_count, args.query_seed)
        with (args.output_dir / "build.csv").open("w", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow(
                [
                    "dataset",
                    "engine",
                    "vectors",
                    "dimensions",
                    "metric",
                    "seconds",
                    "vectors_per_second",
                    "resource_scope",
                ]
            )
            writer.writerow(
                [
                    meta["name"],
                    "amazon-s3-vectors",
                    rows,
                    dimensions,
                    metric,
                    f"{ingest_seconds:.3f}",
                    f"{rows / ingest_seconds:.3f}",
                    "managed-service-plus-measured-client",
                ]
            )

        all_samples: list[dict[str, object]] = []
        for label in ("first_pass", "repeated_pass"):
            all_samples.extend(
                query_pass(
                    client,
                    queries,
                    truth,
                    args.bucket,
                    args.index,
                    10,
                    positions,
                    label,
                    args.repetition_id,
                    args.query_seed,
                )
            )
        with (args.output_dir / "query_samples.csv").open("w", newline="") as handle:
            fieldnames = [
                "dataset",
                "engine",
                "pass",
                "repetition_id",
                "query_seed",
                "query_position",
                "query_source_index",
                "latency_ms",
                "recall_at_10",
                "status",
                "cache_state",
                "resource_scope",
            ]
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            for sample in all_samples:
                writer.writerow(
                    {
                        "dataset": meta["name"],
                        "engine": "amazon-s3-vectors",
                        **sample,
                        "latency_ms": f"{float(sample['latency_ms']):.6f}",
                        "recall_at_10": f"{float(sample['recall_at_10']):.6f}",
                        "status": "ok",
                        "cache_state": "vendor-managed-opaque",
                        "resource_scope": "managed-service-e2e-client-latency",
                    }
                )

        with (args.output_dir / "query.csv").open("w", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow(
                [
                    "dataset",
                    "engine",
                    "pass",
                    "repetition_id",
                    "query_seed",
                    "queries",
                    "recall_at_10",
                    "mean_ms",
                    "stddev_ms",
                    "p50_ms",
                    "p95_ms",
                    "p99_ms",
                    "max_ms",
                    "cache_state",
                    "resource_scope",
                ]
            )
            for label in ("first_pass", "repeated_pass"):
                selected = [row for row in all_samples if row["pass"] == label]
                latencies = [float(row["latency_ms"]) for row in selected]
                recalls = [float(row["recall_at_10"]) for row in selected]
                writer.writerow(
                    [
                        meta["name"],
                        "amazon-s3-vectors",
                        label,
                        args.repetition_id,
                        args.query_seed,
                        len(latencies),
                        f"{statistics.fmean(recalls):.6f}",
                        f"{statistics.fmean(latencies):.3f}",
                        f"{sample_stddev(latencies):.3f}",
                        f"{percentile(latencies, 0.50):.3f}",
                        f"{percentile(latencies, 0.95):.3f}",
                        f"{percentile(latencies, 0.99):.3f}",
                        f"{max(latencies):.3f}",
                        "vendor-managed-opaque",
                        "managed-service-e2e-client-latency",
                    ]
                )
    finally:
        if created_index:
            client.delete_index(vectorBucketName=args.bucket, indexName=args.index)
        if created_bucket:
            deadline = time.monotonic() + 300
            while True:
                try:
                    client.delete_vector_bucket(vectorBucketName=args.bucket)
                    break
                except client.exceptions.ConflictException:
                    if time.monotonic() >= deadline:
                        raise
                    time.sleep(2)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
