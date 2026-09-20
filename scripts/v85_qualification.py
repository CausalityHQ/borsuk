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
        "min_average_recall10_ppm": 960_000,
        "min_average_recall100_ppm": 975_000,
        "min_p05_recall100_ppm": 900_000,
        "neighbors": 100,
        "offered_load_ppm": 700_000,
        "preflight_page_budgets": [16, 32],
        "range_concurrency": 16,
        "replacement_rows": 500,
        "query_count": 1_000,
        "run_counts": [1, 10, 100],
        "schema": "borsuk-v85-qualification-matrix-v2",
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
    samples: Any,
    matrix: dict[str, Any],
    label: str,
    expected_query_count: int | None = None,
) -> dict[str, Any]:
    query_count = (
        matrix["query_count"] if expected_query_count is None else expected_query_count
    )
    if not isinstance(samples, list) or len(samples) != query_count:
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
    total_hits10 = 0
    total_hits100 = 0
    recall100_values = []
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
        hits10 = len(set(ids[:10]).intersection(truth_ids[:10]))
        hits100 = len(set(ids).intersection(truth_ids))
        total_hits10 += hits10
        total_hits100 += hits100
        recall100_ppm = hits100 * 1_000_000 // sample["neighbors"]
        recall100_values.append(recall100_ppm)
        worst_recall_ppm = min(worst_recall_ppm, recall100_ppm)
        max_requests = max(max_requests, sample["requests"])
        max_bytes = max(max_bytes, sample["bytes"])
        latencies.append(sample["latency_ns"])
        result_ids.append(ids)
        all_truth_ids.append(truth_ids)
    return {
        "average_recall10_ppm": total_hits10 * 1_000_000 // (query_count * 10),
        "average_recall100_ppm": total_hits100
        * 1_000_000
        // (query_count * matrix["neighbors"]),
        "bytes_per_query": max_bytes,
        "gets_per_query": max_requests,
        "p50_ns": _nearest_percentile(latencies, 0.50),
        "p95_ns": _nearest_percentile(latencies, 0.95),
        "p99_ns": _nearest_percentile(latencies, 0.99),
        "p05_recall100_ppm": sorted(recall100_values)[
            max(0, (query_count * 5 + 99) // 100 - 1)
        ],
        "result_ids": result_ids,
        "truth_ids": all_truth_ids,
        "worst_recall_ppm": worst_recall_ppm,
    }


def _quality_gate_passes(summary: dict[str, Any], matrix: dict[str, Any]) -> bool:
    return (
        summary["average_recall10_ppm"] >= matrix["min_average_recall10_ppm"]
        and summary["average_recall100_ppm"] >= matrix["min_average_recall100_ppm"]
        and summary["p05_recall100_ppm"] >= matrix["min_p05_recall100_ppm"]
    )


def _semantic_screen_result(value: Any, label: str) -> dict[str, Any]:
    result = _exact_keys(
        value,
        {
            "aggregate_recall_ppm",
            "generation",
            "samples",
            "total_bytes",
            "total_requests",
            "worst_recall_ppm",
        },
        label,
    )
    if not _positive_int(result["generation"]):
        raise ValueError(f"{label} generation differs")
    samples = result["samples"]
    if not isinstance(samples, list) or not samples:
        raise ValueError(f"{label} samples differ")
    total_hits = 0
    total_neighbors = 0
    total_bytes = 0
    total_requests = 0
    worst_recall = 1_000_000
    ordered_ids = []
    ordered_truth = []
    recalls = []
    sample_keys = {
        "bytes",
        "hits",
        "latency_ns",
        "neighbors",
        "query",
        "recall_ppm",
        "requests",
        "result_ids",
        "truth_ids",
    }
    for expected_query, candidate in enumerate(samples):
        sample = _exact_keys(candidate, sample_keys, f"{label} sample")
        result_ids = sample["result_ids"]
        truth_ids = sample["truth_ids"]
        if (
            sample["query"] != expected_query
            or not _positive_int(sample["neighbors"])
            or not _positive_int(sample["bytes"])
            or not _positive_int(sample["requests"])
            or not _positive_int(sample["latency_ns"])
            or not isinstance(result_ids, list)
            or not isinstance(truth_ids, list)
            or len(result_ids) != sample["neighbors"]
            or len(truth_ids) != sample["neighbors"]
            or any(type(row_id) is not int for row_id in result_ids + truth_ids)
            or len(set(result_ids)) != len(result_ids)
            or len(set(truth_ids)) != len(truth_ids)
        ):
            raise ValueError(f"{label} sample differs")
        hits = len(set(result_ids).intersection(truth_ids))
        recall_ppm = hits * 1_000_000 // sample["neighbors"]
        if sample["hits"] != hits or sample["recall_ppm"] != recall_ppm:
            raise ValueError(f"{label} recall differs")
        total_hits += hits
        total_neighbors += sample["neighbors"]
        total_bytes += sample["bytes"]
        total_requests += sample["requests"]
        worst_recall = min(worst_recall, recall_ppm)
        ordered_ids.append(result_ids)
        ordered_truth.append(truth_ids)
        recalls.append(recall_ppm)
    aggregate_recall = total_hits * 1_000_000 // total_neighbors
    if (
        result["aggregate_recall_ppm"] != aggregate_recall
        or result["worst_recall_ppm"] != worst_recall
        or result["total_bytes"] != total_bytes
        or result["total_requests"] != total_requests
    ):
        raise ValueError(f"{label} aggregate differs")
    return {
        "aggregate_recall_ppm": aggregate_recall,
        "generation": result["generation"],
        "ordered_ids": ordered_ids,
        "ordered_truth": ordered_truth,
        "p05_recall_ppm": _nearest_percentile(recalls, 0.05),
        "query_count": len(samples),
        "worst_recall_ppm": worst_recall,
    }


def validate_delta_compaction_screen(receipt: Any) -> dict[str, Any]:
    """Validate the real-data run-fragmentation and compaction fail-fast screen."""

    label = "compaction screen"
    receipt = _exact_keys(
        receipt,
        {
            "base_rows",
            "claim_eligible",
            "compaction",
            "delta_rows",
            "evidence_kind",
            "page_budget",
            "query_count",
            "results",
            "rows",
            "schema",
        },
        label,
    )
    if (
        receipt["schema"] != "borsuk-v85-delta-compaction-screen-v1"
        or receipt["evidence_kind"] != "semantic-local-artifact-screen"
        or receipt["claim_eligible"] is not False
        or receipt["rows"] != 100_000
        or receipt["base_rows"] != 90_000
        or receipt["delta_rows"] != 10_000
        or receipt["query_count"] != 32
        or receipt["page_budget"] != 512
    ):
        raise ValueError(f"{label} authority differs")
    cells = receipt["results"]
    if not isinstance(cells, list) or len(cells) != 3:
        raise ValueError(f"{label} cells differ")
    baseline_ids = None
    aggregate_recall = None
    worst_recall = None
    baseline_truth = None
    baseline_generation = None
    for index, expected_runs in enumerate((1, 10, 100)):
        candidate = cells[index]
        cell = _exact_keys(candidate, {"result", "runs"}, f"{label} cell")
        if cell["runs"] != expected_runs:
            raise ValueError(f"{label} run count differs")
        summary = _semantic_screen_result(cell["result"], label)
        if summary["query_count"] != receipt["query_count"]:
            raise ValueError(f"{label} query count differs")
        if baseline_ids is None:
            baseline_ids = summary["ordered_ids"]
            aggregate_recall = summary["aggregate_recall_ppm"]
            worst_recall = summary["worst_recall_ppm"]
            baseline_truth = summary["ordered_truth"]
            baseline_generation = cell["result"]["generation"]
        elif cell["result"]["generation"] != baseline_generation:
            raise ValueError(f"{label} generation identities differ")
        elif (
            summary["ordered_ids"] != baseline_ids
            or summary["ordered_truth"] != baseline_truth
        ):
            raise ValueError(f"{label} fragmented results differ")

    compaction = _exact_keys(
        receipt["compaction"],
        {
            "amplification_ppm",
            "input_runs",
            "logical_live_bytes",
            "read_bytes",
            "result",
            "write_bytes",
        },
        f"{label} compaction",
    )
    if (
        compaction["input_runs"] != 100
        or not _positive_int(compaction["logical_live_bytes"])
        or not _positive_int(compaction["read_bytes"])
        or not _positive_int(compaction["write_bytes"])
    ):
        raise ValueError(f"{label} compaction counters differ")
    amplification_ppm = (
        (compaction["read_bytes"] + compaction["write_bytes"])
        * 1_000_000
        // compaction["logical_live_bytes"]
    )
    compacted = _semantic_screen_result(compaction["result"], label)
    if (
        compaction["amplification_ppm"] != amplification_ppm
        or amplification_ppm > 5_000_000
        or compacted["generation"] <= baseline_generation
        or compacted["query_count"] != receipt["query_count"]
        or compacted["ordered_ids"] != baseline_ids
        or compacted["ordered_truth"] != baseline_truth
        or aggregate_recall < 975_000
        or compacted["p05_recall_ppm"] < 900_000
    ):
        raise ValueError(f"{label} compaction differs")
    return {
        "aggregate_recall_ppm": aggregate_recall,
        "compaction_amplification_ppm": amplification_ppm,
        "query_count": receipt["query_count"],
        "schema": "borsuk-v85-delta-compaction-screen-summary-v1",
        "worst_recall_ppm": worst_recall,
    }


def validate_preflight_receipt(receipt: Any, matrix: Any) -> None:
    """Reject a 10k preflight that cannot safely promote to the paid 1M cell."""

    matrix = _exact_keys(matrix, set(frozen_matrix()), "matrix")
    receipt = _exact_keys(
        receipt,
        {
            "authenticated_inputs",
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
            "samples",
            "result_sha256",
            "schema",
            "selected_page_budget",
            "source_archive_sha256",
            "source_commit",
        },
        "preflight",
    )
    if (
        receipt["schema"] != "borsuk-v85-preflight-receipt-v2"
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
        or not _positive_int(receipt["max_gets_per_query"])
        or receipt["max_gets_per_query"] > matrix["max_gets_per_query"]
        or not _positive_int(receipt["max_bytes_per_query"])
        or receipt["max_bytes_per_query"] > matrix["max_bytes_per_query"]
        or not _positive_int(receipt["peak_rss_bytes"])
        or receipt["peak_rss_bytes"] > matrix["max_peak_rss_bytes"]
    ):
        raise ValueError("preflight gate failed")
    samples = _qualification_samples(
        receipt["samples"], matrix, "preflight", expected_query_count=1
    )
    if (
        not _quality_gate_passes(samples, matrix)
        or receipt["max_gets_per_query"] != samples["gets_per_query"]
        or receipt["max_bytes_per_query"] != samples["bytes_per_query"]
    ):
        raise ValueError("preflight quality gate failed")


def validate_qualification_receipt(receipt: Any, matrix: Any) -> dict[str, Any]:
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
    if receipt["schema"] != "borsuk-v85-qualification-receipt-v4":
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
        capacity["successful_queries"] * 1_000_000_000_000 // capacity["elapsed_ns"]
    )
    offered_qps_milli = (
        base_capacity_qps_milli * matrix["offered_load_ppm"] // 1_000_000
    )

    fresh = _qualification_samples(
        receipt["fresh_samples"], matrix, "qualification fresh"
    )
    if not _quality_gate_passes(fresh, matrix):
        raise ValueError("qualification fresh quality gate failed")

    cells = receipt["cells"]
    if not isinstance(cells, list) or len(cells) != len(matrix["run_counts"]):
        raise ValueError("qualification cells differ")
    derived_cells = []
    cell_keys = {"elapsed_ns", "peak_rss_bytes", "runs", "samples"}
    for index, expected_runs in enumerate(matrix["run_counts"]):
        candidate = cells[index]
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
            or not _quality_gate_passes(sample_summary, matrix)
            or abs(
                sample_summary["average_recall10_ppm"] - fresh["average_recall10_ppm"]
            )
            > 2_000
            or abs(
                sample_summary["average_recall100_ppm"] - fresh["average_recall100_ppm"]
            )
            > 2_000
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
    expected_mutations = matrix["replacement_rows"] + matrix["tombstone_rows"]
    if not isinstance(cases, list) or len(cases) != expected_mutations:
        raise ValueError("qualification mutation cases differ")
    seen_ids = set()
    kind_counts = {"replacement": 0, "tombstone": 0}
    visibility_latencies = []
    for candidate in cases:
        case = _exact_keys(candidate, mutation_keys, "qualification mutation")
        writes = case["writes"]
        if (
            case["kind"] not in kind_counts
            or type(case["id"]) is not int
            or case["id"] in seen_ids
            or not isinstance(writes, list)
            or len(writes) < 2
            or not _positive_int(case["visibility_latency_ns"])
        ):
            raise ValueError("qualification mutation gate failed")
        seen_ids.add(case["id"])
        kind_counts[case["kind"]] += 1
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
    if kind_counts != {
        "replacement": matrix["replacement_rows"],
        "tombstone": matrix["tombstone_rows"],
    }:
        raise ValueError("qualification mutation mix differs")
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
        "fresh_average_recall10_ppm": fresh["average_recall10_ppm"],
        "fresh_average_recall100_ppm": fresh["average_recall100_ppm"],
        "fresh_p05_recall100_ppm": fresh["p05_recall100_ppm"],
        "offered_qps_milli": offered_qps_milli,
        "schema": "borsuk-v85-qualification-summary-v2",
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
