#!/usr/bin/env python3
"""Independent fail-fast recall and cold-S3 projection for V30 query results."""

from __future__ import annotations

import hashlib
import json
import math

import pyarrow as pa
import pyarrow.parquet as pq

if __package__:
    from scripts.run_v30_variable_rate_reproduction import (
        V30S3LatencyProfile,
        V30ServingEvidence,
        project_v30_s3_latency,
    )
else:
    from run_v30_variable_rate_reproduction import (
        V30S3LatencyProfile,
        V30ServingEvidence,
        project_v30_s3_latency,
    )

QUERY_COUNT = 32
RECALL_K = 10
MAX_CODES = 1_000_000
MAX_CANDIDATES = 12_288
DEFAULT_PAGES = 10
MAX_PAGES = 16
MAX_BYTES = 4_587_520


def _truth(
    truth_parquet: bytes,
    expected_sha256: str,
    *,
    query_start: int,
    source_rows: int,
) -> tuple[frozenset[int], ...]:
    if (
        type(truth_parquet) is not bytes
        or len(expected_sha256) != 64
        or hashlib.sha256(truth_parquet).hexdigest() != expected_sha256
    ):
        raise ValueError("V30 truth byte authority differs")
    table = pq.read_table(pa.BufferReader(truth_parquet))
    if (
        type(query_start) is not int
        or query_start < 0
        or type(source_rows) is not int
        or source_rows <= 0
        or table.schema.names != ["neighbors_id"]
        or table.num_rows < query_start + QUERY_COUNT
    ):
        raise ValueError("V30 truth Parquet schema or cardinality differs")
    field = table.schema.field("neighbors_id")
    data_type = field.type
    if not (
        not field.nullable
        and (pa.types.is_list(data_type) or pa.types.is_fixed_size_list(data_type))
        and not data_type.value_field.nullable
        and (pa.types.is_int32(data_type.value_type) or pa.types.is_int64(data_type.value_type))
    ):
        raise ValueError("V30 truth Parquet schema or cardinality differs")
    rows = table.column("neighbors_id").slice(query_start, QUERY_COUNT).to_pylist()
    truth: list[frozenset[int]] = []
    for values in rows:
        if (
            type(values) is not list
            or len(values) < RECALL_K
            or any(type(source) is not int or source < 0 or source >= source_rows for source in values)
        ):
            raise ValueError("V30 truth membership differs")
        top = values[:RECALL_K]
        if len(set(top)) != RECALL_K:
            raise ValueError("V30 truth membership differs")
        truth.append(frozenset(top))
    return tuple(truth)


def _query_result(
    payload: bytes, *, expected_pages: int = DEFAULT_PAGES
) -> tuple[tuple[int, ...], dict[str, int]]:
    if type(expected_pages) is not int or not 1 <= expected_pages <= MAX_PAGES:
        raise ValueError("V30 expected page count differs")
    if type(payload) is not bytes or not payload.endswith(b"\n") or b"\n" in payload[:-1]:
        raise ValueError("V30 query result canonical bytes differ")
    value = json.loads(payload)
    expected = json.dumps(value, allow_nan=False, separators=(",", ":"), sort_keys=True).encode() + b"\n"
    if payload != expected or set(value) != {
        "claim_eligible",
        "matches",
        "schema_version",
        "timing",
        "work",
    }:
        raise ValueError("V30 query result canonical bytes differ")
    if value["claim_eligible"] is not False or type(value["schema_version"]) is not int or value["schema_version"] != 2:
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
    timing = value["timing"]
    phase_cpu_keys = (
        "routing_cpu_ns",
        "page_read_cpu_ns",
        "exact_rerank_cpu_ns",
    )
    phase_elapsed_keys = (
        "routing_elapsed_ns",
        "page_read_elapsed_ns",
        "exact_rerank_elapsed_ns",
    )
    if (
        type(timing) is not dict
        or set(timing)
        != {
            "elapsed_ns",
            "exact_rerank_cpu_ns",
            "exact_rerank_elapsed_ns",
            "page_read_cpu_ns",
            "page_read_elapsed_ns",
            "peak_rss_bytes",
            "process_cpu_ns",
            "routing_cpu_ns",
            "routing_elapsed_ns",
        }
        or any(type(timing[key]) is not int or timing[key] < 0 for key in timing)
        or sum(timing[key] for key in phase_cpu_keys) > timing["process_cpu_ns"]
        or sum(timing[key] for key in phase_elapsed_keys) > timing["elapsed_ns"]
    ):
        raise ValueError("V30 query timing differs")
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
        work["get_count"] != expected_pages
        or work["encoded_bytes"] > MAX_BYTES
        or routing["selected_pages"] != expected_pages
        or routing["codes_scanned"] > MAX_CODES
        or routing["candidates_retained"] > MAX_CANDIDATES
    ):
        raise ValueError("V30 query work gates differ")
    return tuple(sources), {
        "encoded_bytes": work["encoded_bytes"],
        "get_count": work["get_count"],
        "codes_scanned": routing["codes_scanned"],
        "candidates_retained": routing["candidates_retained"],
        "roots_scored": routing["roots_scored"],
        "leaves_scored": routing["leaves_scored"],
        "pages_considered": routing["pages_considered"],
        "selected_pages": routing["selected_pages"],
        "elapsed_ns": timing["elapsed_ns"],
        "peak_rss_bytes": timing["peak_rss_bytes"],
        "process_cpu_ns": timing["process_cpu_ns"],
        **{key: timing[key] for key in phase_cpu_keys + phase_elapsed_keys},
    }


def reduce_v30_quality(
    query_results: tuple[bytes, ...],
    truth_parquet: bytes,
    *,
    truth_sha256: str,
    truth_query_start: int,
    source_rows: int,
    request_p50_ms: float,
    request_p95_ms: float,
    request_p99_ms: float,
    aggregate_bytes_per_second: int,
) -> bytes:
    """Recompute reduced recall/work and a no-sleep cold-S3 latency projection."""

    if type(query_results) is not tuple or len(query_results) != QUERY_COUNT:
        raise ValueError("V30 query cohort differs")
    truth = _truth(
        truth_parquet,
        truth_sha256,
        query_start=truth_query_start,
        source_rows=source_rows,
    )
    parsed = tuple(_query_result(payload) for payload in query_results)
    hits = tuple(
        len(frozenset(matches) & truth[index])
        for index, (matches, _) in enumerate(parsed)
    )
    maximum_bytes = max(work["encoded_bytes"] for _, work in parsed)
    maximum_gets = max(work["get_count"] for _, work in parsed)
    maximum_codes = max(work["codes_scanned"] for _, work in parsed)
    cpu_p99_ns = max(work["process_cpu_ns"] for _, work in parsed)
    cold_p99_ns = max(work["elapsed_ns"] for _, work in parsed)
    aggregate = sum(hits) * 1_000_000 // (QUERY_COUNT * RECALL_K)
    minimum = min(hits) * 1_000_000 // RECALL_K
    perfect = sum(hit == RECALL_K for hit in hits)
    if aggregate < 996_875 or minimum < 900_000 or perfect < 31:
        raise ValueError("V30 reduced quality gates failed")
    if not 0 <= request_p50_ms <= request_p95_ms <= request_p99_ms:
        raise ValueError("V30 reduced latency profile differs")
    projections = {
        percentile: project_v30_s3_latency(
            V30ServingEvidence(cpu_p99_ns / 1_000_000, maximum_gets, maximum_bytes),
            V30S3LatencyProfile(request_ms, aggregate_bytes_per_second),
        )
        for percentile, request_ms in (
            ("p50", request_p50_ms),
            ("p95", request_p95_ms),
            ("p99", request_p99_ms),
        )
    }
    if (
        cpu_p99_ns > 15_000_000
        or cold_p99_ns > 100_000_000
        or max(work["elapsed_ns"] for _, work in parsed) > 150_000_000
        or projections["p99"]["projected_p99_ms"] > 100.0
    ):
        raise ValueError("V30 reduced latency gate failed")
    value = {
        "aggregate_recall_ppm": aggregate,
        "claim_eligible": False,
        "measured_cold_p99_ns": cold_p99_ns,
        "measured_process_cpu_p99_ns": cpu_p99_ns,
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
