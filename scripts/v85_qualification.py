#!/usr/bin/env python3
"""Frozen V85 1M qualification matrix and fail-closed receipt validators."""

from __future__ import annotations

import argparse
import json
from typing import Any


def frozen_matrix() -> dict[str, Any]:
    """Return the literal preregistered matrix consumed by the remote runner."""

    return {
        "base_rows": 900_000,
        "cpu_parallelism": "sequential-per-query",
        "delta_rows": 100_000,
        "max_bytes_per_query": 16 * 1024 * 1024,
        "max_gets_per_query": 32,
        "max_peak_rss_bytes": 3 * 1024**3,
        "min_aggregate_recall_ppm": 990_000,
        "offered_load_ppm": 700_000,
        "preflight_page_budgets": [8, 16],
        "range_concurrency": 16,
        "replacement_rows": 500,
        "run_counts": [1, 10, 100],
        "schema": "borsuk-v85-qualification-matrix-v1",
        "source_objects": [
            {
                "role": "source",
                "sha256": "2796b579f37afe99ca4aff57e282335a6a79ad30596645957d26326a0560cf86",
                "uri": "s3://borsuk-bench-453182569524-euc1/research/v36-prefix-screen/runs/v36-prefix-screen-20260908T174540Z-31445a91/attempt-0000/source.parquet",
            },
            {
                "role": "queries",
                "sha256": "310bb54f79f2e79d09fe63aa4f6b5c6e9e7ffb31101964f816be978dadb2db54",
                "uri": "s3://borsuk-bench-453182569524-euc1/research/v36-prefix-screen/runs/v36-prefix-screen-20260908T174540Z-31445a91/attempt-0000/development-query.parquet",
            },
            {
                "role": "truth",
                "sha256": "fed7524fd675087f42b48b2f7fa9192b4661aaa4b665600de8378b8b6c696e11",
                "uri": "s3://borsuk-bench-453182569524-euc1/research/v36-prefix-screen/runs/v36-prefix-screen-20260908T174540Z-31445a91/attempt-0000/development-gt100.parquet",
            },
        ],
        "spot_interruption": "discard-cell-and-restart",
        "terminal_schema": "borsuk-v85-terminal-v1",
        "tombstone_rows": 500,
    }


def _exact_keys(value: Any, keys: set[str], label: str) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != keys:
        raise ValueError(f"{label} schema differs")
    return value


def _positive_int(value: Any) -> bool:
    return type(value) is int and value > 0


def validate_preflight_receipt(receipt: Any, matrix: Any) -> None:
    """Reject a 10k preflight that cannot safely promote to the paid 1M cell."""

    matrix = _exact_keys(matrix, set(frozen_matrix()), "matrix")
    receipt = _exact_keys(
        receipt,
        {
            "authenticated_inputs",
            "aggregate_recall_ppm",
            "binary_authenticated",
            "binary_sha256",
            "built_rows",
            "cas_conflict_observed",
            "failed_queries",
            "manifest_drift",
            "max_bytes_per_query",
            "max_gets_per_query",
            "peak_rss_bytes",
            "query_count",
            "result_sha256",
            "schema",
            "selected_page_budget",
            "source_archive_sha256",
            "source_commit",
            "worst_recall_ppm",
        },
        "preflight",
    )
    if (
        receipt["schema"] != "borsuk-v85-preflight-receipt-v1"
        or type(receipt["binary_authenticated"]) is not bool
        or not receipt["binary_authenticated"]
        or type(receipt["cas_conflict_observed"]) is not bool
        or not receipt["cas_conflict_observed"]
        or type(receipt["manifest_drift"]) is not bool
        or receipt["manifest_drift"]
        or receipt["authenticated_inputs"] != 4
        or not isinstance(receipt["source_commit"], str)
        or len(receipt["source_commit"]) != 40
        or any(
            not isinstance(receipt[field], str)
            or len(receipt[field]) != 64
            or any(character not in "0123456789abcdef" for character in receipt[field])
            for field in (
                "binary_sha256",
                "result_sha256",
                "source_archive_sha256",
            )
        )
        or receipt["built_rows"] != 10_000
        or receipt["query_count"] != 1
        or receipt["failed_queries"] != 0
        or receipt["selected_page_budget"] not in matrix["preflight_page_budgets"]
        or receipt["aggregate_recall_ppm"] < matrix["min_aggregate_recall_ppm"]
        or receipt["worst_recall_ppm"] < matrix["min_aggregate_recall_ppm"]
        or not _positive_int(receipt["max_gets_per_query"])
        or receipt["max_gets_per_query"] > matrix["max_gets_per_query"]
        or not _positive_int(receipt["max_bytes_per_query"])
        or receipt["max_bytes_per_query"] > matrix["max_bytes_per_query"]
        or not _positive_int(receipt["peak_rss_bytes"])
        or receipt["peak_rss_bytes"] > matrix["max_peak_rss_bytes"]
    ):
        raise ValueError("preflight gate failed")


def validate_qualification_receipt(receipt: Any, matrix: Any) -> None:
    """Independently recompute the frozen 1M promotion gates."""

    matrix = _exact_keys(matrix, set(frozen_matrix()), "matrix")
    receipt = _exact_keys(
        receipt,
        {
            "base_capacity_qps_milli",
            "cells",
            "compaction_amplification_ppm",
            "fresh_recall_ppm",
            "mutation_checks",
            "offered_qps_milli",
            "post_compaction_result_ids",
            "schema",
            "visibility_p95_ns",
        },
        "qualification",
    )
    cell_keys = {
        "bytes_per_query",
        "failed_queries",
        "gets_per_query",
        "p50_ns",
        "p95_ns",
        "p99_ns",
        "peak_rss_bytes",
        "recall_ppm",
        "result_ids",
        "runs",
        "successful_qps_milli",
        "worst_recall_ppm",
    }
    cells = receipt["cells"]
    mutation_checks = receipt["mutation_checks"]
    if (
        receipt["schema"] != "borsuk-v85-qualification-receipt-v1"
        or not isinstance(cells, list)
        or len(cells) != 3
        or not isinstance(mutation_checks, dict)
        or set(mutation_checks) != {"newest", "replacement", "tombstone"}
        or not all(value is True for value in mutation_checks.values())
        or not _positive_int(receipt["base_capacity_qps_milli"])
        or receipt["offered_qps_milli"]
        != receipt["base_capacity_qps_milli"] * matrix["offered_load_ppm"] // 1_000_000
        or receipt["visibility_p95_ns"] > 1_000_000_000
        or receipt["compaction_amplification_ppm"] > 5_000_000
        or receipt["fresh_recall_ppm"] < matrix["min_aggregate_recall_ppm"]
    ):
        raise ValueError("qualification gate failed")

    for expected_runs, cell in zip(matrix["run_counts"], cells, strict=True):
        cell = _exact_keys(cell, cell_keys, "qualification cell")
        if (
            cell["runs"] != expected_runs
            or cell["failed_queries"] != 0
            or cell["successful_qps_milli"] < receipt["offered_qps_milli"]
            or cell["gets_per_query"] > matrix["max_gets_per_query"]
            or cell["bytes_per_query"] > matrix["max_bytes_per_query"]
            or cell["peak_rss_bytes"] > matrix["max_peak_rss_bytes"]
            or cell["recall_ppm"] < matrix["min_aggregate_recall_ppm"]
            or abs(cell["recall_ppm"] - receipt["fresh_recall_ppm"]) > 2_000
            or not isinstance(cell["result_ids"], list)
            or len(cell["result_ids"]) != 100
            or any(type(value) is not int for value in cell["result_ids"])
        ):
            raise ValueError("qualification cell gate failed")

    one, _ten, hundred = cells
    if (
        hundred["p95_ns"] * 1_000_000 > one["p95_ns"] * 1_250_000
        or hundred["p99_ns"] * 1_000_000 > one["p99_ns"] * 1_500_000
        or receipt["post_compaction_result_ids"] != hundred["result_ids"]
    ):
        raise ValueError("qualification fragmentation gate failed")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--print-matrix", action="store_true")
    args = parser.parse_args()
    if not args.print_matrix:
        parser.error("--print-matrix is required")
    print(json.dumps(frozen_matrix(), separators=(",", ":"), sort_keys=True))


if __name__ == "__main__":
    main()
