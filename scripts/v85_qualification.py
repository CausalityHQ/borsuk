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
        "neighbors": 100,
        "offered_load_ppm": 700_000,
        "preflight_page_budgets": [8, 16],
        "range_concurrency": 16,
        "replacement_rows": 500,
        "query_count": 1_000,
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


def _nearest_percentile(values: list[int], quantile: float) -> int:
    ordered = sorted(values)
    return ordered[round((len(ordered) - 1) * quantile)]


def _qualification_samples(
    samples: Any, matrix: dict[str, Any], label: str
) -> dict[str, Any]:
    if not isinstance(samples, list) or len(samples) != matrix["query_count"]:
        raise ValueError(f"{label} query count differs")
    sample_keys = {
        "bytes",
        "latency_ns",
        "neighbors",
        "query",
        "requests",
        "result_ids",
        "truth_ids",
    }
    total_hits = 0
    latencies = []
    result_ids = []
    all_truth_ids = []
    worst_recall_ppm = 1_000_000
    max_requests = 0
    max_bytes = 0
    for query, candidate in enumerate(samples):
        sample = _exact_keys(candidate, sample_keys, f"{label} sample")
        ids = sample["result_ids"]
        truth_ids = sample["truth_ids"]
        if (
            sample["query"] != query
            or sample["neighbors"] != matrix["neighbors"]
            or not _positive_int(sample["requests"])
            or sample["requests"] > matrix["max_gets_per_query"]
            or not _positive_int(sample["bytes"])
            or sample["bytes"] > matrix["max_bytes_per_query"]
            or not _positive_int(sample["latency_ns"])
            or not isinstance(ids, list)
            or len(ids) != matrix["neighbors"]
            or any(type(identifier) is not int for identifier in ids)
            or len(set(ids)) != len(ids)
            or not isinstance(truth_ids, list)
            or len(truth_ids) != matrix["neighbors"]
            or any(type(identifier) is not int for identifier in truth_ids)
            or len(set(truth_ids)) != len(truth_ids)
        ):
            raise ValueError(f"{label} sample gate failed")
        hits = len(set(ids).intersection(truth_ids))
        total_hits += hits
        recall_ppm = hits * 1_000_000 // sample["neighbors"]
        worst_recall_ppm = min(worst_recall_ppm, recall_ppm)
        max_requests = max(max_requests, sample["requests"])
        max_bytes = max(max_bytes, sample["bytes"])
        latencies.append(sample["latency_ns"])
        result_ids.append(ids)
        all_truth_ids.append(truth_ids)
    return {
        "bytes_per_query": max_bytes,
        "gets_per_query": max_requests,
        "p50_ns": _nearest_percentile(latencies, 0.50),
        "p95_ns": _nearest_percentile(latencies, 0.95),
        "p99_ns": _nearest_percentile(latencies, 0.99),
        "recall_ppm": total_hits * 1_000_000
        // (matrix["query_count"] * matrix["neighbors"]),
        "result_ids": result_ids,
        "truth_ids": all_truth_ids,
        "worst_recall_ppm": worst_recall_ppm,
    }


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


def validate_qualification_receipt(
    receipt: Any, matrix: Any
) -> dict[str, Any]:
    """Independently recompute the frozen 1M promotion gates."""

    matrix = _exact_keys(matrix, set(frozen_matrix()), "matrix")
    receipt = _exact_keys(
        receipt,
        {
            "base_capacity",
            "cells",
            "compaction",
            "fresh_samples",
            "mutation_cases",
            "schema",
        },
        "qualification",
    )
    if receipt["schema"] != "borsuk-v85-qualification-receipt-v2":
        raise ValueError("qualification schema differs")

    capacity = _exact_keys(
        receipt["base_capacity"],
        {"attempted_queries", "elapsed_ns", "successful_queries"},
        "qualification capacity",
    )
    if (
        not _positive_int(capacity["attempted_queries"])
        or capacity["successful_queries"] != capacity["attempted_queries"]
        or not _positive_int(capacity["elapsed_ns"])
    ):
        raise ValueError("qualification capacity gate failed")
    base_capacity_qps_milli = (
        capacity["successful_queries"] * 1_000_000_000_000
        // capacity["elapsed_ns"]
    )
    offered_qps_milli = (
        base_capacity_qps_milli * matrix["offered_load_ppm"] // 1_000_000
    )

    fresh = _qualification_samples(
        receipt["fresh_samples"], matrix, "qualification fresh"
    )
    if fresh["recall_ppm"] < matrix["min_aggregate_recall_ppm"]:
        raise ValueError("qualification fresh quality gate failed")

    cells = receipt["cells"]
    if not isinstance(cells, list) or len(cells) != len(matrix["run_counts"]):
        raise ValueError("qualification cells differ")
    derived_cells = []
    cell_keys = {"elapsed_ns", "peak_rss_bytes", "runs", "samples"}
    for expected_runs, candidate in zip(matrix["run_counts"], cells, strict=True):
        cell = _exact_keys(candidate, cell_keys, "qualification cell")
        sample_summary = _qualification_samples(
            cell["samples"], matrix, "qualification cell"
        )
        if not _positive_int(cell["elapsed_ns"]):
            raise ValueError("qualification cell elapsed time differs")
        successful_qps_milli = (
            matrix["query_count"] * 1_000_000_000_000 // cell["elapsed_ns"]
        )
        if (
            cell["runs"] != expected_runs
            or successful_qps_milli < offered_qps_milli
            or not _positive_int(cell["peak_rss_bytes"])
            or cell["peak_rss_bytes"] > matrix["max_peak_rss_bytes"]
            or sample_summary["recall_ppm"] < matrix["min_aggregate_recall_ppm"]
            or abs(sample_summary["recall_ppm"] - fresh["recall_ppm"]) > 2_000
            or sample_summary["truth_ids"] != fresh["truth_ids"]
        ):
            raise ValueError("qualification cell gate failed")
        derived_cells.append(
            {
                key: value
                for key, value in sample_summary.items()
                if key not in {"result_ids", "truth_ids"}
            }
            | {
                "peak_rss_bytes": cell["peak_rss_bytes"],
                "runs": cell["runs"],
                "successful_qps_milli": successful_qps_milli,
            }
        )

    one, _ten, hundred = derived_cells
    if (
        hundred["p95_ns"] * 1_000_000 > one["p95_ns"] * 1_250_000
        or hundred["p99_ns"] * 1_000_000 > one["p99_ns"] * 1_500_000
    ):
        raise ValueError("qualification fragmentation gate failed")

    mutation_keys = {
        "id",
        "kind",
        "observed_sequence",
        "observed_state",
        "visibility_latency_ns",
        "writes",
    }
    write_keys = {"sequence", "state"}
    cases = receipt["mutation_cases"]
    if not isinstance(cases, list) or len(cases) != 3:
        raise ValueError("qualification mutation cases differ")
    seen_kinds = set()
    seen_ids = set()
    visibility_latencies = []
    for candidate in cases:
        case = _exact_keys(candidate, mutation_keys, "qualification mutation")
        writes = case["writes"]
        if (
            case["kind"] not in {"newest", "replacement", "tombstone"}
            or case["kind"] in seen_kinds
            or type(case["id"]) is not int
            or case["id"] in seen_ids
            or not isinstance(writes, list)
            or len(writes) < 2
            or not _positive_int(case["visibility_latency_ns"])
        ):
            raise ValueError("qualification mutation gate failed")
        seen_kinds.add(case["kind"])
        seen_ids.add(case["id"])
        parsed_writes = [
            _exact_keys(write, write_keys, "qualification mutation write")
            for write in writes
        ]
        if any(
            not _positive_int(write["sequence"])
            or write["state"] not in {"live", "tombstone"}
            for write in parsed_writes
        ):
            raise ValueError("qualification mutation write gate failed")
        sequences = [write["sequence"] for write in parsed_writes]
        if len(set(sequences)) != len(sequences):
            raise ValueError("qualification mutation sequence tie")
        latest = max(parsed_writes, key=lambda write: write["sequence"])
        expected_state = "tombstone" if case["kind"] == "tombstone" else "live"
        if (
            latest["state"] != expected_state
            or case["observed_sequence"] != latest["sequence"]
            or case["observed_state"] != latest["state"]
        ):
            raise ValueError("qualification mutation visibility differs")
        visibility_latencies.append(case["visibility_latency_ns"])
    visibility_p95_ns = _nearest_percentile(visibility_latencies, 0.95)
    if visibility_p95_ns > 1_000_000_000:
        raise ValueError("qualification visibility gate failed")

    compaction = _exact_keys(
        receipt["compaction"],
        {
            "amplification_ppm",
            "logical_live_bytes",
            "post_result_ids",
            "read_bytes",
            "write_bytes",
        },
        "qualification compaction",
    )
    if (
        not _positive_int(compaction["logical_live_bytes"])
        or not _positive_int(compaction["read_bytes"])
        or not _positive_int(compaction["write_bytes"])
    ):
        raise ValueError("qualification compaction counters differ")
    amplification_ppm = (
        (compaction["read_bytes"] + compaction["write_bytes"])
        * 1_000_000
        // compaction["logical_live_bytes"]
    )
    if (
        compaction["amplification_ppm"] != amplification_ppm
        or amplification_ppm > 5_000_000
        or compaction["post_result_ids"]
        != _qualification_samples(
            cells[2]["samples"], matrix, "qualification pre-compaction"
        )["result_ids"]
    ):
        raise ValueError("qualification compaction gate failed")

    return {
        "base_capacity_qps_milli": base_capacity_qps_milli,
        "cells": derived_cells,
        "compaction_amplification_ppm": amplification_ppm,
        "fresh_recall_ppm": fresh["recall_ppm"],
        "offered_qps_milli": offered_qps_milli,
        "schema": "borsuk-v85-qualification-summary-v1",
        "visibility_p95_ns": visibility_p95_ns,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--print-matrix", action="store_true")
    args = parser.parse_args()
    if not args.print_matrix:
        parser.error("--print-matrix is required")
    print(json.dumps(frozen_matrix(), separators=(",", ":"), sort_keys=True))


if __name__ == "__main__":
    main()
