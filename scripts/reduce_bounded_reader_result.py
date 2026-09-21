#!/usr/bin/env python3
"""Independently validate and reduce bounded-reader Parquet evidence."""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

PASS_LABELS = ("first_connection_pass", "connection_reuse_pass")
RESULT_KEYS = {
    "schema",
    "claim_eligible",
    "evidence_kind",
    "storage",
    "cpu_path",
    "in_query_cpu",
    "source_commit",
    "manifest_sha256",
    "sq8_sha256",
    "rows",
    "dimensions",
    "page_rows",
    "shortlist_rows",
    "coarse_regions",
    "gap_pages",
    "concurrency",
    "queries_per_pass",
    "passes",
    "throughput",
    "samples_sha256",
    "samples_bytes",
}


@dataclass(frozen=True)
class PassReduction:
    label: str
    queries: int
    average_recall10_ppm: int
    average_recall100_ppm: int
    p05_recall100_ppm: int
    worst_recall100_ppm: int
    latency_p50_ns: int
    latency_p95_ns: int
    latency_p99_ns: int
    requests_total: int
    requests_mean_milli: int
    requests_p50: int
    requests_p95: int
    requests_p99: int
    requests_max: int
    bytes_total: int
    bytes_mean: int
    bytes_p50: int
    bytes_p95: int
    bytes_p99: int
    bytes_max: int


@dataclass(frozen=True)
class ReductionReceipt:
    schema: str
    claim_eligible: bool
    result_sha256: str
    samples_sha256: str
    samples_bytes: int
    truth_sha256: str
    truth_bytes: int
    queries_per_pass: int
    passes: tuple[PassReduction, ...]


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while block := handle.read(1 << 20):
            digest.update(block)
    return digest.hexdigest()


def _load_result(path: Path) -> dict[str, Any]:
    body = path.read_bytes()
    if not body.endswith(b"\n") or body.endswith(b"\n\n"):
        raise ValueError("bounded reader result canonical bytes differ")
    try:
        value = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("bounded reader result JSON differs") from error
    if type(value) is not dict or set(value) != RESULT_KEYS:
        raise ValueError("bounded reader result schema differs")
    if (
        value["schema"] != "borsuk-bounded-reader-result-v1"
        or value["claim_eligible"] is not False
        or type(value["queries_per_pass"]) is not int
        or value["queries_per_pass"] <= 0
        or type(value["passes"]) is not list
        or type(value["throughput"]) is not list
        or type(value["samples_sha256"]) is not str
        or len(value["samples_sha256"]) != 64
        or type(value["samples_bytes"]) is not int
        or value["samples_bytes"] <= 0
    ):
        raise ValueError("bounded reader result authority differs")
    return value


def _sample_schema() -> pa.Schema:
    return pa.schema(
        [
            pa.field("pass_label", pa.string(), nullable=False),
            pa.field("query_ordinal", pa.uint32(), nullable=False),
            pa.field("latency_ns", pa.uint64(), nullable=False),
            pa.field("recall10_ppm", pa.uint32(), nullable=False),
            pa.field("recall100_ppm", pa.uint32(), nullable=False),
            pa.field("requests", pa.uint32(), nullable=False),
            pa.field("bytes", pa.uint64(), nullable=False),
            pa.field(
                "returned_feature_row_ids",
                pa.list_(pa.field("item", pa.int64(), nullable=False), 100),
                nullable=False,
            ),
        ]
    )


def _truth_schema() -> pa.Schema:
    return pa.schema(
        [
            pa.field("query_ordinal", pa.uint32(), nullable=False),
            pa.field("rank", pa.uint16(), nullable=False),
            pa.field("feature_row_id", pa.uint64(), nullable=False),
            pa.field("squared_distance", pa.float64(), nullable=False),
        ]
    )


def _percentile(values: list[int], quantile_ppm: int) -> int:
    ordered = sorted(values)
    index = ((len(ordered) - 1) * quantile_ppm + 500_000) // 1_000_000
    return ordered[index]


def _aggregate(label: str, rows: list[dict[str, Any]]) -> PassReduction:
    recall10 = [row["recall10_ppm"] for row in rows]
    recall100 = [row["recall100_ppm"] for row in rows]
    latencies = [row["latency_ns"] for row in rows]
    requests = [row["requests"] for row in rows]
    sizes = [row["bytes"] for row in rows]
    count = len(rows)
    return PassReduction(
        label=label,
        queries=count,
        average_recall10_ppm=sum(recall10) // count,
        average_recall100_ppm=sum(recall100) // count,
        p05_recall100_ppm=_percentile(recall100, 50_000),
        worst_recall100_ppm=min(recall100),
        latency_p50_ns=_percentile(latencies, 500_000),
        latency_p95_ns=_percentile(latencies, 950_000),
        latency_p99_ns=_percentile(latencies, 990_000),
        requests_total=sum(requests),
        requests_mean_milli=sum(requests) * 1_000 // count,
        requests_p50=_percentile(requests, 500_000),
        requests_p95=_percentile(requests, 950_000),
        requests_p99=_percentile(requests, 990_000),
        requests_max=max(requests),
        bytes_total=sum(sizes),
        bytes_mean=sum(sizes) // count,
        bytes_p50=_percentile(sizes, 500_000),
        bytes_p95=_percentile(sizes, 950_000),
        bytes_p99=_percentile(sizes, 990_000),
        bytes_max=max(sizes),
    )


def _read_truth(path: Path, query_count: int) -> list[list[int]]:
    table = pq.read_table(path)
    if table.schema != _truth_schema() or table.num_rows != query_count * 100:
        raise ValueError("ground-truth Parquet schema or cardinality differs")
    rows = table.to_pylist()
    truth: list[list[int]] = [[] for _ in range(query_count)]
    for position, row in enumerate(rows):
        query_ordinal = position // 100
        rank = position % 100
        if (
            row["query_ordinal"] != query_ordinal
            or row["rank"] != rank
            or type(row["feature_row_id"]) is not int
            or not math.isfinite(row["squared_distance"])
            or row["squared_distance"] < 0.0
        ):
            raise ValueError("ground-truth row authority differs")
        truth[query_ordinal].append(row["feature_row_id"])
    return truth


def reduce_bounded_reader_result(
    result_path: Path, samples_path: Path, truth_path: Path
) -> ReductionReceipt:
    result = _load_result(result_path)
    samples_sha256 = _sha256(samples_path)
    samples_bytes = samples_path.stat().st_size
    if (
        result["samples_sha256"] != samples_sha256
        or result["samples_bytes"] != samples_bytes
    ):
        raise ValueError("query-sample identity differs")
    query_count = result["queries_per_pass"]
    truth = _read_truth(truth_path, query_count)
    table = pq.read_table(samples_path)
    if table.schema != _sample_schema() or table.num_rows != query_count * 2:
        raise ValueError("query-sample Parquet schema or cardinality differs")

    all_rows = table.to_pylist()
    reductions: list[PassReduction] = []
    for pass_index, label in enumerate(PASS_LABELS):
        rows = all_rows[pass_index * query_count : (pass_index + 1) * query_count]
        for query_ordinal, row in enumerate(rows):
            returned = row["returned_feature_row_ids"]
            expected_truth = truth[query_ordinal]
            recall10 = len(set(returned[:10]).intersection(expected_truth[:10])) * 100_000
            recall100 = len(set(returned).intersection(expected_truth)) * 10_000
            if (
                row["pass_label"] != label
                or row["query_ordinal"] != query_ordinal
                or row["latency_ns"] <= 0
                or row["requests"] <= 0
                or row["bytes"] <= 0
                or len(returned) != 100
                or len(set(returned)) != 100
                or row["recall10_ppm"] != recall10
                or row["recall100_ppm"] != recall100
            ):
                raise ValueError("query-sample semantic evidence differs")
        reductions.append(_aggregate(label, rows))

    if result["passes"] != [asdict(value) for value in reductions]:
        raise ValueError("producer aggregate differs from independent reduction")
    return ReductionReceipt(
        schema="borsuk-bounded-reader-reduction-v1",
        claim_eligible=False,
        result_sha256=_sha256(result_path),
        samples_sha256=samples_sha256,
        samples_bytes=samples_bytes,
        truth_sha256=_sha256(truth_path),
        truth_bytes=truth_path.stat().st_size,
        queries_per_pass=query_count,
        passes=tuple(reductions),
    )


def canonical_reduction_bytes(receipt: ReductionReceipt) -> bytes:
    return (
        json.dumps(asdict(receipt), sort_keys=True, separators=(",", ":")) + "\n"
    ).encode()
