#!/usr/bin/env python3
"""Independently validate one bounded-native qualification result."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

RESULT_KEYS = {
    "schema",
    "claim_eligible",
    "source_commit",
    "rows",
    "dimensions",
    "query_count",
    "neighbors",
    "average_recall_at_10_gate_ppm",
    "average_recall_at_100_gate_ppm",
    "p05_recall_at_100_gate_ppm",
    "inputs",
    "samples",
    "build_wall_ns",
    "query_wall_ns",
    "peak_rss_bytes",
    "index_stats",
    "summary",
    "equivalence",
}
SUMMARY_KEYS = {
    "average_recall_at_10_ppm",
    "average_recall_at_100_ppm",
    "p05_recall_at_100_ppm",
    "worst_recall_at_100_ppm",
    "p50_latency_ns",
    "p95_latency_ns",
    "p99_latency_ns",
    "total_gets",
    "total_pages_read",
    "total_bytes",
    "total_records_scored",
    "passed",
}
EQUIVALENCE_KEYS = {
    "one_run",
    "ten_run",
    "hundred_run",
    "pending_put",
    "pending_delete",
    "reopen",
    "compaction",
}


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _identity(value: Any, role: str, path: Path) -> None:
    _require(type(value) is dict, f"{role} identity differs")
    _require(set(value) == {"role", "sha256", "encoded_bytes"}, f"{role} identity schema differs")
    _require(value["role"] == role, f"{role} identity role differs")
    _require(type(value["sha256"]) is str and len(value["sha256"]) == 64, f"{role} digest differs")
    _require(type(value["encoded_bytes"]) is int and value["encoded_bytes"] > 0, f"{role} length differs")
    _require((_sha256(path), path.stat().st_size) == (value["sha256"], value["encoded_bytes"]), f"{role} bytes differ")


def _input_identities(value: Any, truth_path: Path) -> None:
    _require(type(value) is list and len(value) == 3, "input identities differ")
    keys = {"role", "path", "uri", "sha256", "encoded_bytes"}
    for authority, role in zip(value, ["source", "queries", "truth"], strict=True):
        _require(type(authority) is dict and set(authority) == keys, f"{role} input schema differs")
        _require(authority["role"] == role, f"{role} input role differs")
        _require(type(authority["path"]) is str and authority["path"], f"{role} input path differs")
        _require(type(authority["uri"]) is str and "://" in authority["uri"], f"{role} input URI differs")
        _require(type(authority["sha256"]) is str and len(authority["sha256"]) == 64, f"{role} input digest differs")
        _require(type(authority["encoded_bytes"]) is int and authority["encoded_bytes"] > 0, f"{role} input length differs")
    truth = value[2]
    _require((_sha256(truth_path), truth_path.stat().st_size) == (truth["sha256"], truth["encoded_bytes"]), "truth bytes differ")


def _fixed_list(name: str, value_type: pa.DataType, width: int) -> pa.Field:
    return pa.field(name, pa.list_(pa.field("element", value_type, nullable=False), width), nullable=False)


def _truth_schema(neighbors: int) -> pa.Schema:
    return pa.schema(
        [
            pa.field("query", pa.uint32(), nullable=False),
            _fixed_list("neighbors", pa.int64(), neighbors),
        ]
    )


def _samples_schema(neighbors: int) -> pa.Schema:
    return pa.schema(
        [
            pa.field("query_ordinal", pa.uint32(), nullable=False),
            pa.field("hits_at_10", pa.uint32(), nullable=False),
            pa.field("hits_at_100", pa.uint32(), nullable=False),
            pa.field("recall_at_10_ppm", pa.uint32(), nullable=False),
            pa.field("recall_at_100_ppm", pa.uint32(), nullable=False),
            pa.field("physical_gets", pa.uint64(), nullable=False),
            pa.field("pages_read", pa.uint64(), nullable=False),
            pa.field("bytes_read", pa.uint64(), nullable=False),
            pa.field("records_scored", pa.uint64(), nullable=False),
            pa.field("latency_ns", pa.uint64(), nullable=False),
            _fixed_list("returned_ids", pa.uint64(), neighbors),
        ]
    )


def _rounded_ppm(hits: int, denominator: int) -> int:
    return (hits * 1_000_000 + denominator // 2) // denominator


def _percentile(sorted_values: list[int], percentile: int) -> int:
    return sorted_values[max(0, math.ceil(len(sorted_values) * percentile / 100) - 1)]


def _read_table(path: Path, schema: pa.Schema, rows: int, role: str) -> pa.Table:
    parquet = pq.ParquetFile(path)
    _require(parquet.schema_arrow == schema, f"{role} physical schema differs")
    _require(parquet.metadata.num_rows == rows, f"{role} row count differs")
    table = parquet.read()
    _require(table.schema == schema and table.num_rows == rows, f"{role} materialization differs")
    _require(
        all(chunk.null_count == 0 for column in table.columns for chunk in column.chunks),
        f"{role} nullability differs",
    )
    return table


def validate_bounded_native_result(
    result_path: Path, samples_path: Path, truth_path: Path
) -> dict[str, int | bool]:
    """Validate exact evidence bytes and recompute all quality/latency aggregates."""
    result_bytes = result_path.read_bytes()
    _require(result_bytes.endswith(b"\n") and result_bytes.count(b"\n") == 1, "result newline differs")
    result = json.loads(result_bytes)
    _require(type(result) is dict and set(result) == RESULT_KEYS, "result schema differs")
    canonical = (json.dumps(result, sort_keys=True, separators=(",", ":")) + "\n").encode()
    _require(result_bytes == canonical, "result canonical bytes differ")
    _require(result["schema"] == "borsuk-bounded-native-100k-qualification-v1", "result version differs")
    _require(result["claim_eligible"] is False, "result claim eligibility differs")
    _require(type(result["source_commit"]) is str and len(result["source_commit"]) == 40, "source commit differs")
    for field in ["rows", "dimensions", "query_count", "neighbors"]:
        _require(type(result[field]) is int and result[field] > 0, f"{field} differs")
    for field in [
        "average_recall_at_10_gate_ppm",
        "average_recall_at_100_gate_ppm",
        "p05_recall_at_100_gate_ppm",
    ]:
        _require(type(result[field]) is int and 0 <= result[field] <= 1_000_000, f"{field} differs")

    query_count = result["query_count"]
    neighbors = result["neighbors"]
    _input_identities(result["inputs"], truth_path)
    _identity(result["samples"], "per-query-samples", samples_path)
    for field in ["build_wall_ns", "query_wall_ns", "peak_rss_bytes"]:
        _require(type(result[field]) is int and result[field] > 0, f"{field} differs")
    _require(type(result["index_stats"]) is dict and result["index_stats"], "index stats differ")
    truth = _read_table(truth_path, _truth_schema(neighbors), query_count, "truth")
    samples = _read_table(samples_path, _samples_schema(neighbors), query_count, "samples")

    truth_rows = truth.to_pylist()
    sample_rows = samples.to_pylist()
    recall10: list[int] = []
    recall100: list[int] = []
    latencies: list[int] = []
    totals = {"total_gets": 0, "total_pages_read": 0, "total_bytes": 0, "total_records_scored": 0}
    at10 = min(10, neighbors)
    for ordinal, (truth_row, sample) in enumerate(zip(truth_rows, sample_rows, strict=True)):
        _require(truth_row["query"] == ordinal and sample["query_ordinal"] == ordinal, "query ordinal differs")
        returned = sample["returned_ids"]
        expected = truth_row["neighbors"]
        _require(len(returned) == neighbors and len(set(returned)) == neighbors, "returned ID cardinality differs")
        _require(len(expected) == neighbors and len(set(expected)) == neighbors, "truth ID cardinality differs")
        hits10 = len(set(returned[:at10]).intersection(expected[:at10]))
        hits100 = len(set(returned).intersection(expected))
        ppm10 = _rounded_ppm(hits10, at10)
        ppm100 = _rounded_ppm(hits100, neighbors)
        _require(sample["hits_at_10"] == hits10 and sample["hits_at_100"] == hits100, "sample hit arithmetic differs")
        _require(sample["recall_at_10_ppm"] == ppm10 and sample["recall_at_100_ppm"] == ppm100, "sample recall arithmetic differs")
        for field in ["physical_gets", "pages_read", "bytes_read", "records_scored", "latency_ns"]:
            _require(type(sample[field]) is int and sample[field] > 0, f"sample {field} differs")
        recall10.append(ppm10)
        recall100.append(ppm100)
        latencies.append(sample["latency_ns"])
        totals["total_gets"] += sample["physical_gets"]
        totals["total_pages_read"] += sample["pages_read"]
        totals["total_bytes"] += sample["bytes_read"]
        totals["total_records_scored"] += sample["records_scored"]

    recall100.sort()
    latencies.sort()
    recomputed: dict[str, int | bool] = {
        "average_recall_at_10_ppm": _rounded_ppm(sum(recall10), query_count * 1_000_000),
        "average_recall_at_100_ppm": _rounded_ppm(sum(recall100), query_count * 1_000_000),
        "p05_recall_at_100_ppm": recall100[max(0, math.ceil(query_count * 0.05) - 1)],
        "worst_recall_at_100_ppm": recall100[0],
        "p50_latency_ns": _percentile(latencies, 50),
        "p95_latency_ns": _percentile(latencies, 95),
        "p99_latency_ns": _percentile(latencies, 99),
        **totals,
    }
    recomputed["passed"] = (
        recomputed["average_recall_at_10_ppm"] >= result["average_recall_at_10_gate_ppm"]
        and recomputed["average_recall_at_100_ppm"] >= result["average_recall_at_100_gate_ppm"]
        and recomputed["p05_recall_at_100_ppm"] >= result["p05_recall_at_100_gate_ppm"]
    )
    _require(type(result["summary"]) is dict and set(result["summary"]) == SUMMARY_KEYS, "summary schema differs")
    _require(result["summary"] == recomputed, "summary recomputation differs")
    _require(type(result["equivalence"]) is dict and set(result["equivalence"]) == EQUIVALENCE_KEYS, "equivalence schema differs")
    _require(all(result["equivalence"].get(role) is True for role in EQUIVALENCE_KEYS), "semantic equivalence failed")
    return recomputed


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("result", type=Path)
    parser.add_argument("samples", type=Path)
    parser.add_argument("truth", type=Path)
    arguments = parser.parse_args()
    receipt = validate_bounded_native_result(arguments.result, arguments.samples, arguments.truth)
    print(json.dumps(receipt, sort_keys=True, separators=(",", ":")))


if __name__ == "__main__":
    main()
