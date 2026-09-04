#!/usr/bin/env python3
"""Independent fail-fast recall and cold-S3 projection for V30 query results."""

from __future__ import annotations

import hashlib
import json
import math

import pyarrow as pa
import pyarrow.parquet as pq

from scripts.run_v30_variable_rate_reproduction import (
    V30S3LatencyProfile,
    V30ServingEvidence,
    project_v30_s3_latency,
)

QUERY_COUNT = 32
RECALL_K = 10
MAX_CODES = 1_000_000
MAX_CANDIDATES = 12_288
MAX_PAGES = 10
MAX_BYTES = 4_587_520


def _truth(truth_parquet: bytes, expected_sha256: str) -> tuple[frozenset[int], ...]:
    if (
        type(truth_parquet) is not bytes
        or len(expected_sha256) != 64
        or hashlib.sha256(truth_parquet).hexdigest() != expected_sha256
    ):
        raise ValueError("V30 truth byte authority differs")
    table = pq.read_table(pa.BufferReader(truth_parquet))
    expected = pa.schema(
        [
            pa.field("query_ordinal", pa.uint16(), nullable=False),
            pa.field("source_ordinal", pa.uint64(), nullable=False),
        ]
    )
    if table.schema != expected or table.num_rows != QUERY_COUNT * RECALL_K:
        raise ValueError("V30 truth Parquet schema or cardinality differs")
    ordinals = table.column("query_ordinal").to_pylist()
    sources = table.column("source_ordinal").to_pylist()
    truth: list[frozenset[int]] = []
    for query in range(QUERY_COUNT):
        values = [source for ordinal, source in zip(ordinals, sources, strict=True) if ordinal == query]
        if len(values) != RECALL_K or len(set(values)) != RECALL_K:
            raise ValueError("V30 truth membership differs")
        truth.append(frozenset(values))
    if ordinals != [query for query in range(QUERY_COUNT) for _ in range(RECALL_K)]:
        raise ValueError("V30 truth order differs")
    return tuple(truth)


def _query_result(payload: bytes) -> tuple[frozenset[int], dict[str, int]]:
    if type(payload) is not bytes or not payload.endswith(b"\n") or b"\n" in payload[:-1]:
        raise ValueError("V30 query result canonical bytes differ")
    value = json.loads(payload)
    expected = json.dumps(value, allow_nan=False, separators=(",", ":"), sort_keys=True).encode() + b"\n"
    if payload != expected or set(value) != {"claim_eligible", "matches", "schema_version", "work"}:
        raise ValueError("V30 query result canonical bytes differ")
    if value["claim_eligible"] is not False or type(value["schema_version"]) is not int or value["schema_version"] != 1:
        raise ValueError("V30 query result constants differ")
    matches = value["matches"]
    if type(matches) is not list or len(matches) != RECALL_K:
        raise ValueError("V30 query result cardinality differs")
    sources: list[int] = []
    previous: tuple[float, int] | None = None
    for match in matches:
        if set(match) != {"source_ordinal", "squared_distance"}:
            raise ValueError("V30 match schema differs")
        source = match["source_ordinal"]
        distance = match["squared_distance"]
        if type(source) is not int or source < 0 or type(distance) not in (int, float) or not math.isfinite(distance) or distance < 0:
            raise ValueError("V30 match value differs")
        key = (float(distance), source)
        if previous is not None and key < previous:
            raise ValueError("V30 match order differs")
        previous = key
        sources.append(source)
    if len(set(sources)) != RECALL_K:
        raise ValueError("V30 match identity differs")
    work = value["work"]
    routing = work.get("routing") if type(work) is dict else None
    if (
        set(work) != {"decoded_rows", "encoded_bytes", "get_count", "routing", "unique_rows"}
        or type(routing) is not dict
        or set(routing)
        != {
            "candidates_retained",
            "codes_scanned",
            "leaves_scored",
            "pages_considered",
            "roots_scored",
            "selected_pages",
        }
    ):
        raise ValueError("V30 query work schema differs")
    numeric = [work[key] for key in ("decoded_rows", "encoded_bytes", "get_count", "unique_rows")]
    numeric.extend(routing.values())
    if any(type(item) is not int or item < 0 for item in numeric):
        raise ValueError("V30 query work value differs")
    if (
        work["get_count"] != MAX_PAGES
        or work["encoded_bytes"] > MAX_BYTES
        or routing["selected_pages"] != MAX_PAGES
        or routing["codes_scanned"] > MAX_CODES
        or routing["candidates_retained"] > MAX_CANDIDATES
    ):
        raise ValueError("V30 query work gates differ")
    return frozenset(sources), {
        "encoded_bytes": work["encoded_bytes"],
        "get_count": work["get_count"],
        "codes_scanned": routing["codes_scanned"],
    }


def reduce_v30_quality(
    query_results: tuple[bytes, ...],
    truth_parquet: bytes,
    *,
    truth_sha256: str,
    cpu_p99_ms: float,
    request_p50_ms: float,
    request_p95_ms: float,
    request_p99_ms: float,
    aggregate_bytes_per_second: int,
) -> bytes:
    """Recompute reduced recall/work and a no-sleep cold-S3 latency projection."""

    if type(query_results) is not tuple or len(query_results) != QUERY_COUNT:
        raise ValueError("V30 query cohort differs")
    truth = _truth(truth_parquet, truth_sha256)
    parsed = tuple(_query_result(payload) for payload in query_results)
    hits = tuple(len(matches & truth[index]) for index, (matches, _) in enumerate(parsed))
    maximum_bytes = max(work["encoded_bytes"] for _, work in parsed)
    maximum_gets = max(work["get_count"] for _, work in parsed)
    maximum_codes = max(work["codes_scanned"] for _, work in parsed)
    aggregate = sum(hits) * 1_000_000 // (QUERY_COUNT * RECALL_K)
    minimum = min(hits) * 1_000_000 // RECALL_K
    perfect = sum(hit == RECALL_K for hit in hits)
    if aggregate < 996_875 or minimum < 900_000 or perfect < 31:
        raise ValueError("V30 reduced quality gates failed")
    if not 0 <= request_p50_ms <= request_p95_ms <= request_p99_ms:
        raise ValueError("V30 reduced latency profile differs")
    projections = {
        percentile: project_v30_s3_latency(
            V30ServingEvidence(cpu_p99_ms, maximum_gets, maximum_bytes),
            V30S3LatencyProfile(request_ms, aggregate_bytes_per_second),
        )
        for percentile, request_ms in (
            ("p50", request_p50_ms),
            ("p95", request_p95_ms),
            ("p99", request_p99_ms),
        )
    }
    if cpu_p99_ms > 15.0 or projections["p99"]["projected_p99_ms"] > 100.0:
        raise ValueError("V30 reduced latency gate failed")
    value = {
        "aggregate_recall_ppm": aggregate,
        "claim_eligible": False,
        "maximum_codes_scanned": maximum_codes,
        "maximum_encoded_bytes": maximum_bytes,
        "maximum_get_count": maximum_gets,
        "minimum_recall_ppm": minimum,
        "perfect_queries": perfect,
        "projected_cold_s3_p50_ms": projections["p50"]["projected_p99_ms"],
        "projected_cold_s3_p95_ms": projections["p95"]["projected_p99_ms"],
        "projected_cold_s3_p99_ms": projections["p99"]["projected_p99_ms"],
        "projection_model": projections["p99"]["model"],
        "query_count": QUERY_COUNT,
        "schema_version": 1,
        "status": "pass",
        "truth_sha256": truth_sha256,
    }
    return json.dumps(value, allow_nan=False, separators=(",", ":"), sort_keys=True).encode() + b"\n"
