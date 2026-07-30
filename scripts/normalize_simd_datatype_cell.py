#!/usr/bin/env python3
"""Normalize one measured SIMD datatype cell into the frozen evidence schema."""

from __future__ import annotations

import argparse
import csv
import math
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Iterable

RAW_FIELDS = [
    "architecture",
    "instance_type",
    "source_sha256",
    "manifest_sha256",
    "dataset_identity_sha256",
    "build",
    "binary_sha256",
    "path",
    "element_type",
    "repetition",
    "cache_state",
    "target_cache_coverage_percent",
    "observed_cache_coverage_percent",
    "client_concurrency",
    "query_seed",
    "query_ordinal",
    "query_id",
    "latency_ms",
    "cpu_seconds",
    "recall_or_exact_agreement",
    "rss_bytes",
    "logical_bytes",
    "disk_cache_bytes",
    "backing_bytes",
    "disk_cache_requests",
    "backing_requests",
]
SUMMARY_FIELDS = [
    "architecture",
    "build",
    "path",
    "element_type",
    "repetition",
    "cache_state",
    "target_cache_coverage_percent",
    "client_concurrency",
    "query_seed",
    "samples",
    "mean_ms",
    "stddev_ms",
    "p50_ms",
    "p90_ms",
    "p95_ms",
    "p99_ms",
    "max_ms",
    "qps",
    "cpu_seconds_per_query",
    "recall_or_exact_agreement",
    "peak_rss_bytes",
    "mean_logical_bytes",
    "mean_disk_cache_bytes",
    "mean_backing_bytes",
    "mean_disk_cache_requests",
    "mean_backing_requests",
]


class NormalizationError(ValueError):
    """The native benchmark artifacts cannot support the frozen cell schema."""


@dataclass(frozen=True)
class CellIdentity:
    architecture: str
    instance_type: str
    source_sha256: str
    manifest_sha256: str
    dataset_identity_sha256: str
    build: str
    binary_sha256: str
    path: str
    element_type: str
    repetition: int
    cache_state: str
    target_cache_coverage_percent: int
    client_concurrency: int
    query_seed: int

    def schedule_fields(self) -> dict[str, object]:
        return {
            "architecture": self.architecture,
            "build": self.build,
            "path": self.path,
            "element_type": self.element_type,
            "repetition": self.repetition,
            "cache_state": self.cache_state,
            "target_cache_coverage_percent": self.target_cache_coverage_percent,
            "client_concurrency": self.client_concurrency,
            "query_seed": self.query_seed,
        }


@dataclass(frozen=True)
class NativeQuery:
    ordinal: int
    query_id: str
    hot: bool
    latency_ms: float
    recall: float
    logical_bytes: float
    disk_cache_bytes: float
    backing_bytes: float
    disk_cache_requests: float
    backing_requests: float


def read_rows(path: Path) -> list[dict[str, str]]:
    if not path.is_file() or path.stat().st_size == 0:
        raise NormalizationError(f"missing or empty CSV: {path}")
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None:
            raise NormalizationError(f"CSV has no header: {path}")
        return list(reader)


def number(row: dict[str, str], field: str, path: Path) -> float:
    try:
        value = float(row[field])
    except (KeyError, TypeError, ValueError) as error:
        raise NormalizationError(f"{path}: invalid {field}") from error
    if not math.isfinite(value):
        raise NormalizationError(f"{path}: non-finite {field}")
    return value


def integer(row: dict[str, str], field: str, path: Path) -> int:
    try:
        return int(row[field])
    except (KeyError, TypeError, ValueError) as error:
        raise NormalizationError(f"{path}: invalid {field}") from error


def resource_totals(directory: Path) -> tuple[float, int, float]:
    path = directory / "resources.csv"
    rows = read_rows(path)
    elapsed_ms = max(number(row, "elapsed_ms", path) for row in rows)
    rss_values = [integer(row, "rss_bytes", path) for row in rows]
    exact_rows = [row for row in rows if row.get("child_cpu_seconds", "").strip()]
    if len(exact_rows) != 1:
        raise NormalizationError(
            f"{path}: expected exactly one exact child CPU accounting row"
        )
    exact = exact_rows[0]
    child_cpu_seconds = number(exact, "child_cpu_seconds", path)
    child_max_rss = integer(exact, "child_max_rss_bytes", path)
    if elapsed_ms <= 0 or child_cpu_seconds < 0:
        raise NormalizationError(f"{path}: invalid process wall time or CPU")
    return child_cpu_seconds, max([child_max_rss, *rss_values]), elapsed_ms


def production_queries(
    directory: Path, identity: CellIdentity, expected_queries: int
) -> list[NativeQuery]:
    path = directory / "bench_concurrency_samples.csv"
    rows = [
        row
        for row in read_rows(path)
        if integer(row, "workers", path) == identity.client_concurrency
    ]
    rows.sort(key=lambda row: integer(row, "sample_index", path))
    return [
        NativeQuery(
            ordinal=ordinal,
            query_id=f"source-{integer(row, 'query_source_index', path)}",
            hot=integer(row, "target_hot_set_member", path) == 1,
            latency_ms=number(row, "latency_ms", path),
            recall=number(row, "recall_at_10", path),
            logical_bytes=number(row, "bytes_read", path),
            disk_cache_bytes=number(row, "disk_cache_bytes_read", path),
            backing_bytes=number(row, "backing_bytes_read", path),
            disk_cache_requests=number(row, "disk_cache_reads", path),
            backing_requests=number(row, "backing_reads", path),
        )
        for ordinal, row in enumerate(rows)
    ]


def hybrid_queries(directory: Path) -> list[NativeQuery]:
    path = directory / "hybrid_queries.csv"
    rows = read_rows(path)
    rows.sort(key=lambda row: integer(row, "query_position", path))
    return [
        NativeQuery(
            ordinal=ordinal,
            query_id=row["query_id"],
            hot=row.get("query_class") == "target-hot",
            latency_ms=number(row, "latency_ms", path),
            recall=number(row, "recall_at_10", path),
            logical_bytes=number(row, "bytes_read", path),
            disk_cache_bytes=number(row, "disk_cache_bytes_read", path),
            backing_bytes=number(row, "backing_bytes_read", path),
            disk_cache_requests=number(row, "disk_cache_reads", path),
            backing_requests=number(row, "backing_reads", path),
        )
        for ordinal, row in enumerate(rows)
    ]


def late_queries(directory: Path, frontier: int) -> list[NativeQuery]:
    path = directory / "late_interaction_samples.csv"
    rows = [
        row for row in read_rows(path) if integer(row, "frontier", path) == frontier
    ]
    rows.sort(key=lambda row: integer(row, "sample_index", path))
    return [
        NativeQuery(
            ordinal=ordinal,
            query_id=row["query_id"],
            hot=False,
            latency_ms=number(row, "latency_ms", path),
            recall=number(row, "recall_at_50", path),
            logical_bytes=number(row, "bytes_read", path),
            disk_cache_bytes=number(row, "disk_bytes", path),
            backing_bytes=number(row, "backing_bytes", path),
            disk_cache_requests=number(row, "disk_cache_reads", path),
            backing_requests=number(row, "backing_reads", path),
        )
        for ordinal, row in enumerate(rows)
    ]


def mean(values: list[float]) -> float:
    return sum(values) / len(values)


def sample_stddev(values: list[float]) -> float:
    if len(values) < 2:
        return 0.0
    average = mean(values)
    return math.sqrt(
        sum((value - average) ** 2 for value in values) / (len(values) - 1)
    )


def percentile(values: list[float], quantile: float) -> float:
    ordered = sorted(values)
    index = round((len(ordered) - 1) * quantile)
    return ordered[min(index, len(ordered) - 1)]


def write_csv(path: Path, fields: list[str], rows: list[dict[str, object]]) -> None:
    with path.open("x", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def normalize_cell(
    *,
    kind: str,
    directory: Path,
    identity: CellIdentity,
    expected_queries: int,
    late_frontier: int = 128,
) -> None:
    if not 0 <= identity.target_cache_coverage_percent <= 100:
        raise NormalizationError("target cache coverage must be in [0, 100]")
    if (directory / "queries.csv").exists() or (directory / "summary.csv").exists():
        raise NormalizationError("refusing to overwrite normalized cell evidence")
    if kind in {"primary-dense", "primary-binary"}:
        queries = production_queries(directory, identity, expected_queries)
    elif kind in {"named-sparse", "text-bm25"}:
        queries = hybrid_queries(directory)
    elif kind == "late-interaction":
        queries = late_queries(directory, late_frontier)
        hot_count = len(queries) * identity.target_cache_coverage_percent // 100
        queries = [replace(query, hot=query.ordinal < hot_count) for query in queries]
    else:
        raise NormalizationError(f"unsupported SIMD path kind: {kind}")
    if len(queries) != expected_queries:
        raise NormalizationError(
            f"native query evidence has {len(queries)} rows; "
            f"expected {expected_queries}"
        )
    if [query.ordinal for query in queries] != list(range(expected_queries)):
        raise NormalizationError("native query ordinals are incomplete")
    observed = sum(query.hot for query in queries) * 100 / len(queries)
    if abs(observed - identity.target_cache_coverage_percent) > 1.0e-9:
        raise NormalizationError(
            f"hot-set coverage drift: {observed} versus "
            f"{identity.target_cache_coverage_percent}"
        )

    child_cpu_seconds, peak_rss_bytes, elapsed_ms = resource_totals(directory)
    cpu_per_query = child_cpu_seconds / expected_queries
    raw_rows = []
    fixed = {
        **identity.schedule_fields(),
        "instance_type": identity.instance_type,
        "source_sha256": identity.source_sha256,
        "manifest_sha256": identity.manifest_sha256,
        "dataset_identity_sha256": identity.dataset_identity_sha256,
        "binary_sha256": identity.binary_sha256,
    }
    for query in queries:
        raw_rows.append(
            {
                **fixed,
                "observed_cache_coverage_percent": (
                    "100.000000" if query.hot else "0.000000"
                ),
                "query_ordinal": query.ordinal,
                "query_id": query.query_id,
                "latency_ms": f"{query.latency_ms:.6f}",
                "cpu_seconds": f"{cpu_per_query:.9f}",
                "recall_or_exact_agreement": f"{query.recall:.9f}",
                "rss_bytes": peak_rss_bytes,
                "logical_bytes": f"{query.logical_bytes:.6f}",
                "disk_cache_bytes": f"{query.disk_cache_bytes:.6f}",
                "backing_bytes": f"{query.backing_bytes:.6f}",
                "disk_cache_requests": f"{query.disk_cache_requests:.6f}",
                "backing_requests": f"{query.backing_requests:.6f}",
            }
        )
    write_csv(directory / "queries.csv", RAW_FIELDS, raw_rows)

    latencies = [query.latency_ms for query in queries]
    summary = {
        **identity.schedule_fields(),
        "samples": expected_queries,
        "mean_ms": f"{mean(latencies):.6f}",
        "stddev_ms": f"{sample_stddev(latencies):.6f}",
        "p50_ms": f"{percentile(latencies, 0.50):.6f}",
        "p90_ms": f"{percentile(latencies, 0.90):.6f}",
        "p95_ms": f"{percentile(latencies, 0.95):.6f}",
        "p99_ms": f"{percentile(latencies, 0.99):.6f}",
        "max_ms": f"{max(latencies):.6f}",
        "qps": f"{expected_queries / (elapsed_ms / 1000.0):.6f}",
        "cpu_seconds_per_query": f"{cpu_per_query:.9f}",
        "recall_or_exact_agreement": f"{mean([query.recall for query in queries]):.9f}",
        "peak_rss_bytes": peak_rss_bytes,
        "mean_logical_bytes": f"{mean([query.logical_bytes for query in queries]):.6f}",
        "mean_disk_cache_bytes": f"{mean([query.disk_cache_bytes for query in queries]):.6f}",
        "mean_backing_bytes": f"{mean([query.backing_bytes for query in queries]):.6f}",
        "mean_disk_cache_requests": f"{mean([query.disk_cache_requests for query in queries]):.6f}",
        "mean_backing_requests": f"{mean([query.backing_requests for query in queries]):.6f}",
    }
    write_csv(directory / "summary.csv", SUMMARY_FIELDS, [summary])


def parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--kind", required=True)
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--architecture", required=True)
    parser.add_argument("--instance-type", required=True)
    parser.add_argument("--source-sha256", required=True)
    parser.add_argument("--manifest-sha256", required=True)
    parser.add_argument("--dataset-identity-sha256", required=True)
    parser.add_argument("--build", required=True)
    parser.add_argument("--binary-sha256", required=True)
    parser.add_argument("--path", required=True)
    parser.add_argument("--element-type", required=True)
    parser.add_argument("--repetition", type=int, required=True)
    parser.add_argument("--cache-state", required=True)
    parser.add_argument("--target-cache-coverage-percent", type=int, required=True)
    parser.add_argument("--client-concurrency", type=int, required=True)
    parser.add_argument("--query-seed", type=int, required=True)
    parser.add_argument("--expected-queries", type=int, required=True)
    parser.add_argument("--late-frontier", type=int, default=128)
    return parser.parse_args(argv)


def main(argv: Iterable[str] | None = None) -> int:
    args = parse_args(argv)
    identity = CellIdentity(
        architecture=args.architecture,
        instance_type=args.instance_type,
        source_sha256=args.source_sha256,
        manifest_sha256=args.manifest_sha256,
        dataset_identity_sha256=args.dataset_identity_sha256,
        build=args.build,
        binary_sha256=args.binary_sha256,
        path=args.path,
        element_type=args.element_type,
        repetition=args.repetition,
        cache_state=args.cache_state,
        target_cache_coverage_percent=args.target_cache_coverage_percent,
        client_concurrency=args.client_concurrency,
        query_seed=args.query_seed,
    )
    try:
        normalize_cell(
            kind=args.kind,
            directory=args.directory,
            identity=identity,
            expected_queries=args.expected_queries,
            late_frontier=args.late_frontier,
        )
    except (OSError, KeyError, NormalizationError) as error:
        print(f"SIMD cell normalization failed: {error}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
