#!/usr/bin/env python3
"""Fail-closed validator for the uncached global-range hedge campaign."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import pathlib
import sys
from collections.abc import Iterable

LEGACY_CAMPAIGN_ID = "global-range-hedge-qualification-v1"
EXACT_RERANK_CAMPAIGN_ID = "global-exact-rerank-hedge-qualification-v1"
EXACT_RERANK_35MS_CAMPAIGN_ID = "global-exact-rerank-hedge-qualification-v2"
EXACT_RERANK_CAMPAIGN_IDS = {
    EXACT_RERANK_CAMPAIGN_ID,
    EXACT_RERANK_35MS_CAMPAIGN_ID,
}
ARM_NAMES = ("control", "candidate")


class ValidationError(RuntimeError):
    pass


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ValidationError(message)


def read_key_values(path: pathlib.Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for line in path.read_text().splitlines():
        key, separator, value = line.partition("=")
        require(bool(separator) and bool(key), f"invalid environment line in {path}")
        require(key not in values, f"duplicate environment key {key} in {path}")
        values[key] = value
    return values


def read_csv(path: pathlib.Path) -> list[dict[str, str]]:
    with path.open(newline="") as handle:
        return list(csv.DictReader(handle))


def read_one_csv(path: pathlib.Path) -> dict[str, str]:
    rows = read_csv(path)
    require(len(rows) == 1, f"{path} must contain exactly one summary row")
    return rows[0]


def integer(row: dict[str, str], key: str, path: pathlib.Path) -> int:
    try:
        return int(row[key])
    except (KeyError, ValueError) as error:
        raise ValidationError(f"invalid integer {key} in {path}") from error


def floating(row: dict[str, str], key: str, path: pathlib.Path) -> float:
    try:
        return float(row[key])
    except (KeyError, ValueError) as error:
        raise ValidationError(f"invalid number {key} in {path}") from error


def percentile(values: Iterable[float], quantile: float) -> float:
    ordered = sorted(values)
    require(bool(ordered), "cannot compute a percentile of no values")
    return ordered[int((len(ordered) - 1) * quantile + 0.5)]


def validate_manifest(manifest: dict[str, object]) -> str:
    campaign_id = manifest.get("campaign_id")
    common = {
        "protocol": "read-hedge-qualification",
        "status": "preregistered-not-launched",
        "architecture": "aarch64",
        "instance_type": "c7g.8xlarge",
        "base_cell": "c2000/r01/l1/w8",
        "base_manifest_sha256": "81c849548d9ef7300cffd88a0a13aca2023645ae0af40e66f0da5a60ad37408a",
        "dataset": "cohere-medium-1M",
        "dataset_sha256": "54c733e39adfcaa9ee10f3ed8bd8e66ada9f8f9a1a73e9753f5c5c2044b79254",
        "dimensions": 768,
        "writers": 8,
        "operations_per_writer": 1000,
        "records_per_operation": 16,
        "read_writer": 0,
        "queries_per_arm": 500,
        "max_read_segments": 4,
        "stripe_bytes": 1048576,
        "repetitions": 5,
        "arm_orders": [
            ["control", "candidate"],
            ["candidate", "control"],
            ["control", "candidate"],
            ["candidate", "control"],
            ["control", "candidate"],
        ],
        "fresh_process_per_arm": True,
        "disk_cache_enabled": False,
        "resource_sample_interval_ms": 100,
        "arm_timeout_seconds": 1800,
        "required_recall_at_10": 1.0,
        "max_pooled_p95_ms": 200.0,
        "max_worst_repetition_p95_ms": 200.0,
        "maximum_pooled_p50_regression_fraction": 0.05,
        "maximum_get_amplification_fraction": 0.20,
        "maximum_backing_byte_amplification_fraction": 0.20,
    }
    profiles = {
        LEGACY_CAMPAIGN_ID: {
            "campaign_id": LEGACY_CAMPAIGN_ID,
            "hedge_after_ms": {"control": "none", "candidate": "75"},
            "base_run_id": "20260808T091300Z-v67-40911df",
            "base_index_uri": "s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260808T091300Z-v67-40911df/index/cells/c2000/r01/l1/w8",
            "base_samples_uri": "s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260808T091300Z-v67-40911df/results/cells/c2000/r01/l1/w8/samples.csv",
            "base_source_sha256": "4ea819fbb9cb4e203811410e40f7c158dca5fc18a3644012d96341155aa52423",
            "base_samples_sha256": "7ec84babc5dc24bdc6275898155d362bf7e4c487c39491d1e136e2ba9906f578",
            "required_nonworse_paired_repetitions": 4,
            "minimum_pooled_p95_improvement_fraction": 0.10,
            "root_complete_marker": "GLOBAL_RANGE_HEDGE_QUALIFICATION_COMPLETE",
            "root_failure_marker": "GLOBAL_RANGE_HEDGE_QUALIFICATION_FAILED",
            "comparison_contract": "Five paired repetitions replay the same 500 deterministic writer-zero inserted-vector queries through public k=10 SrhtPqScan against one immutable terminal v67 S3 index. Every arm starts a fresh process with disk cache disabled. The control uses one 1 MiB S3 range GET per stripe; the candidate may issue exactly one duplicate after 75 ms. Arm order alternates. Every terminal arm must retain inserted-ID recall@10 1.0, issue zero PUT/DELETE requests, record zero disk-cache bytes, preserve raw/resource/storage telemetry, and reconcile logical and physical bytes. Promotion additionally requires pooled and worst-repeat p95 below 200 ms, at least four of five non-worse paired p95s, at least 10% pooled-p95 improvement, at most 5% pooled-p50 regression, identical logical bytes, and at most 20% GET and backing-byte amplification. Incomplete measurement CSV files are never eligible for inspection.",
        },
        EXACT_RERANK_CAMPAIGN_ID: {
            "campaign_id": EXACT_RERANK_CAMPAIGN_ID,
            "hedge_after_ms": {"control": "none", "candidate": "75"},
            "base_run_id": "20260809T034709Z-v35-8e09070",
            "base_index_uri": "s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260809T034709Z-v35-8e09070/index/cells/c2000/r01/l1/w8",
            "base_samples_uri": "s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260809T034709Z-v35-8e09070/results/cells/c2000/r01/l1/w8/samples.csv",
            "base_source_sha256": "e3e71fe81283277148f6bc6a47cd9072890ea0652136d2680dde1ebc879fa594",
            "base_samples_sha256": "43828b0cba9db2fa915e131377b286e95d8079033535cc5fca3907160f8446e7",
            "required_better_paired_repetitions": 4,
            "minimum_pooled_p95_improvement_ms": 5.0,
            "root_complete_marker": "GLOBAL_EXACT_RERANK_HEDGE_QUALIFICATION_COMPLETE",
            "root_failure_marker": "GLOBAL_EXACT_RERANK_HEDGE_QUALIFICATION_FAILED",
            "comparison_contract": "Five paired repetitions replay the same 500 deterministic writer-zero inserted-vector queries through public k=10 SrhtPqScan against one immutable terminal v35 S3 index. Every arm starts a fresh process with disk cache disabled. Control disables slow-read hedging; candidate permits exactly one duplicate immutable range GET after 75 ms in unbounded query-stage stripe and exact-rerank reads. Arm order alternates. Every terminal arm must retain inserted-ID recall@10 1.0, issue zero PUT/DELETE requests, record zero disk-cache bytes, preserve raw/resource/storage telemetry, and reconcile logical and physical bytes. Paired arms must preserve query IDs, ordered top-10 hit IDs, and per-query logical bytes; query-phase storage traces must reconcile backing GETs and bytes. Promotion requires candidate pooled and worst-repeat p95 below 200 ms, strictly better paired p95 in at least four of five repetitions, at least 5 ms pooled-p95 improvement, at most 5% pooled-p50 regression, and at most 20% GET and backing-byte amplification. Incomplete measurement CSV files are never eligible for inspection.",
        },
        EXACT_RERANK_35MS_CAMPAIGN_ID: {
            "campaign_id": EXACT_RERANK_35MS_CAMPAIGN_ID,
            "hedge_after_ms": {"control": "none", "candidate": "35"},
            "base_run_id": "20260809T034709Z-v35-8e09070",
            "base_index_uri": "s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260809T034709Z-v35-8e09070/index/cells/c2000/r01/l1/w8",
            "base_samples_uri": "s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260809T034709Z-v35-8e09070/results/cells/c2000/r01/l1/w8/samples.csv",
            "base_source_sha256": "e3e71fe81283277148f6bc6a47cd9072890ea0652136d2680dde1ebc879fa594",
            "base_samples_sha256": "43828b0cba9db2fa915e131377b286e95d8079033535cc5fca3907160f8446e7",
            "required_better_paired_repetitions": 4,
            "minimum_pooled_p95_improvement_ms": 5.0,
            "root_complete_marker": "GLOBAL_EXACT_RERANK_HEDGE_35MS_QUALIFICATION_COMPLETE",
            "root_failure_marker": "GLOBAL_EXACT_RERANK_HEDGE_35MS_QUALIFICATION_FAILED",
            "comparison_contract": "Five paired repetitions replay the same 500 deterministic writer-zero inserted-vector queries through public k=10 SrhtPqScan against one immutable terminal v35 S3 index. Every arm starts a fresh process with disk cache disabled. Control disables slow-read hedging; candidate permits exactly one duplicate immutable range GET after 35 ms in unbounded query-stage stripe and exact-rerank reads. Arm order alternates. Every terminal arm must retain inserted-ID recall@10 1.0, issue zero PUT/DELETE requests, record zero disk-cache bytes, preserve raw/resource/storage telemetry, and reconcile logical and physical bytes. Paired arms must preserve query IDs, ordered top-10 hit IDs, and per-query logical bytes; query-phase storage traces must reconcile backing GETs and bytes. Promotion requires candidate pooled and worst-repeat p95 below 200 ms, strictly better paired p95 in at least four of five repetitions, at least 5 ms pooled-p95 improvement, at most 5% pooled-p50 regression, and at most 20% GET and backing-byte amplification. Incomplete measurement CSV files are never eligible for inspection.",
        },
    }
    require(campaign_id in profiles, "manifest campaign_id changed")
    frozen = {
        **common,
        **profiles[str(campaign_id)],
        "required_artifacts": [
            "summary.csv",
            "reads.csv",
            "resources.csv",
            "storage-access.csv",
            "environment.txt",
            "process_exit.txt",
            "READ_HEDGE_QUALIFICATION_COMPLETE",
            "CELL_COMPLETE",
        ],
    }
    require(manifest == frozen, "manifest fields changed")
    return str(campaign_id)


def validate(
    manifest_path: pathlib.Path,
    root: pathlib.Path,
    recover_terminal_validator_failure: bool = False,
) -> dict[str, object]:
    manifest_bytes = manifest_path.read_bytes()
    manifest = json.loads(manifest_bytes)
    campaign_id = validate_manifest(manifest)
    complete_marker = str(manifest["root_complete_marker"])
    failure_marker = str(manifest["root_failure_marker"])

    # Never open a measurement CSV before root terminality is established.
    if recover_terminal_validator_failure:
        require(
            not (root / complete_marker).exists(),
            "recovery requires no completion marker",
        )
        require(
            (root / failure_marker).is_file(),
            "recovery requires the terminal failure marker",
        )
        terminal_mode = "validator-failure-recovery"
    else:
        require((root / complete_marker).is_file(), "campaign is incomplete")
        require(not (root / failure_marker).exists(), "campaign has a failure marker")
        terminal_mode = "complete"
    preserved_manifest = root / "manifest.json"
    require(preserved_manifest.is_file(), "missing preserved manifest.json")
    require(
        preserved_manifest.read_bytes() == manifest_bytes, "preserved manifest differs"
    )
    manifest_sha = hashlib.sha256(manifest_bytes).hexdigest()

    query_count = int(manifest["queries_per_arm"])
    expected_query_ids = tuple(
        f"group-o{query * 2 * 16:08}" for query in range(query_count)
    )
    source_sha: str | None = None
    latencies: dict[str, list[float]] = {name: [] for name in ARM_NAMES}
    repetition_p95: dict[str, list[float]] = {name: [] for name in ARM_NAMES}
    repetition_hit_ids: dict[tuple[int, str], tuple[str, ...]] = {}
    repetition_ordered_top_10: dict[tuple[int, str], tuple[str, ...]] = {}
    repetition_logical_bytes: dict[tuple[int, str], tuple[int, ...]] = {}
    totals = {
        name: {"gets": 0, "logical_bytes": 0, "backing_bytes": 0, "hits": 0}
        for name in ARM_NAMES
    }

    for repetition, order in enumerate(manifest["arm_orders"], 1):
        require(
            sorted(order) == sorted(ARM_NAMES), f"r{repetition:02} arm order changed"
        )
        for order_position, arm_name in enumerate(order):
            arm = root / "repetitions" / f"r{repetition:02}" / arm_name
            for artifact in manifest["required_artifacts"]:
                require((arm / artifact).is_file(), f"missing {artifact} in {arm}")
            require(not (arm / "CELL_FAILED").exists(), f"failure marker in {arm}")
            require(
                (arm / "process_exit.txt").read_text().strip() == "0",
                f"nonzero process exit in {arm}",
            )
            require(
                bool(read_csv(arm / "resources.csv")),
                f"resources.csv has no samples in {arm}",
            )
            storage_trace_path = arm / "storage-access.csv"
            storage_events = read_csv(storage_trace_path)
            require(
                bool(storage_events),
                f"storage-access.csv has no events in {arm}",
            )

            hedge_after = manifest["hedge_after_ms"][arm_name]
            environment = read_key_values(arm / "environment.txt")
            expected_environment = {
                "manifest_sha256": manifest_sha,
                "base_source_sha256": manifest["base_source_sha256"],
                "base_manifest_sha256": manifest["base_manifest_sha256"],
                "base_samples_sha256": manifest["base_samples_sha256"],
                "dataset_sha256": manifest["dataset_sha256"],
                "base_cell": manifest["base_cell"],
                "index_uri": manifest["base_index_uri"],
                "cache_dir": "none",
                "cache_enabled": "false",
                "repetition": repetition,
                "writers": manifest["writers"],
                "operations_per_writer": manifest["operations_per_writer"],
                "records_per_operation": manifest["records_per_operation"],
                "dimensions": manifest["dimensions"],
                "read_writer": manifest["read_writer"],
                "queries": query_count,
                "max_read_segments": manifest["max_read_segments"],
                "stripe_bytes": manifest["stripe_bytes"],
                "hedge_after_ms": hedge_after,
                "order_position": order_position,
            }
            for key, expected in expected_environment.items():
                require(
                    environment.get(key) == str(expected), f"{key} mismatch in {arm}"
                )
            current_source = environment.get("source_sha256", "")
            require(len(current_source) == 64, f"invalid source_sha256 in {arm}")
            if source_sha is None:
                source_sha = current_source
            require(current_source == source_sha, f"source_sha256 mismatch in {arm}")

            summary_path = arm / "summary.csv"
            summary = read_one_csv(summary_path)
            expected_summary = {
                "protocol_kind": f"range-hedge-{arm_name}",
                "source_sha256": source_sha,
                **expected_environment,
            }
            expected_summary.pop("cache_dir")
            for key, expected in expected_summary.items():
                require(
                    summary.get(key) == str(expected),
                    f"{key} mismatch in {summary_path}",
                )
            require(
                floating(summary, "inserted_id_recall_at_10", summary_path)
                == float(manifest["required_recall_at_10"]),
                f"recall gate failed in {arm}",
            )
            require(
                integer(summary, "read_storage_puts", summary_path) == 0,
                f"PUT in {arm}",
            )
            require(
                integer(summary, "read_storage_deletes", summary_path) == 0,
                f"DELETE in {arm}",
            )
            require(
                integer(summary, "read_disk_cache_bytes", summary_path) == 0,
                f"disk-cache bytes in {arm}",
            )

            reads_path = arm / "reads.csv"
            reads = read_csv(reads_path)
            require(len(reads) == query_count, f"{reads_path} has {len(reads)} queries")
            arm_latencies: list[float] = []
            query_ids: list[str] = []
            hit_ids: list[str] = []
            ordered_top_10: list[str] = []
            logical_bytes_by_query: list[int] = []
            aggregate = {
                "requests": 0,
                "gets": 0,
                "puts": 0,
                "deletes": 0,
                "heads": 0,
                "lists": 0,
                "logical_bytes": 0,
                "disk_bytes": 0,
                "backing_bytes": 0,
                "segments": 0,
            }
            for query, row in enumerate(reads):
                require(
                    integer(row, "query", reads_path) == query,
                    f"query order mismatch in {arm}",
                )
                record_id = row.get("record_id", "")
                require(
                    record_id == expected_query_ids[query],
                    f"writer-zero query cohort changed in {arm}",
                )
                query_ids.append(record_id)
                hit_ids.append(row.get("hit_id", ""))
                ordered_hits = row.get("hit_ids", "")
                if campaign_id in EXACT_RERANK_CAMPAIGN_IDS:
                    require(
                        len(ordered_hits.split("|")) == 10,
                        f"ordered top-10 hit IDs missing in {arm}",
                    )
                ordered_top_10.append(ordered_hits)
                require(
                    row.get("contains_record_id", "").lower() == "true",
                    f"recall miss in {arm}",
                )
                row_counts = {
                    key: integer(row, key, reads_path)
                    for key in ("gets", "puts", "deletes", "heads", "lists")
                }
                request_count = integer(row, "requests", reads_path)
                require(
                    request_count == sum(row_counts.values()),
                    f"request total mismatch in {arm}",
                )
                require(
                    row_counts["puts"] == 0 and row_counts["deletes"] == 0,
                    f"write request in {arm}",
                )
                disk_bytes = integer(row, "disk_cache_bytes_read", reads_path)
                require(disk_bytes == 0, f"disk-cache row in {arm}")
                arm_latencies.append(floating(row, "latency_ms", reads_path))
                aggregate["requests"] += request_count
                for key in ("gets", "puts", "deletes", "heads", "lists"):
                    aggregate[key] += row_counts[key]
                logical_bytes = integer(row, "bytes_read", reads_path)
                logical_bytes_by_query.append(logical_bytes)
                aggregate["logical_bytes"] += logical_bytes
                aggregate["disk_bytes"] += disk_bytes
                aggregate["backing_bytes"] += integer(
                    row, "backing_bytes_read", reads_path
                )
                aggregate["segments"] += integer(row, "segments_searched", reads_path)
            require(
                tuple(query_ids) == expected_query_ids,
                f"paired query IDs differ in {arm}",
            )
            repetition_hit_ids[(repetition, arm_name)] = tuple(hit_ids)
            repetition_ordered_top_10[(repetition, arm_name)] = tuple(ordered_top_10)
            repetition_logical_bytes[(repetition, arm_name)] = tuple(
                logical_bytes_by_query
            )
            computed_p50 = percentile(arm_latencies, 0.50)
            computed_p95 = percentile(arm_latencies, 0.95)
            require(
                abs(computed_p50 - floating(summary, "read_p50_ms", summary_path))
                <= 1e-6,
                f"p50 mismatch in {arm}",
            )
            require(
                abs(computed_p95 - floating(summary, "read_p95_ms", summary_path))
                <= 1e-6,
                f"p95 mismatch in {arm}",
            )
            summary_fields = {
                "read_storage_requests": "requests",
                "read_storage_gets": "gets",
                "read_storage_puts": "puts",
                "read_storage_deletes": "deletes",
                "read_storage_heads": "heads",
                "read_storage_lists": "lists",
                "read_bytes": "logical_bytes",
                "read_disk_cache_bytes": "disk_bytes",
                "read_backing_bytes": "backing_bytes",
                "read_segments_searched": "segments",
            }
            for summary_key, aggregate_key in summary_fields.items():
                require(
                    integer(summary, summary_key, summary_path)
                    == aggregate[aggregate_key],
                    f"{summary_key} does not reconcile in {arm}",
                )
            if campaign_id in EXACT_RERANK_CAMPAIGN_IDS:
                required_trace_fields = {
                    "operation",
                    "object_role",
                    "path",
                    "physical_format",
                    "object_bytes",
                    "request_count",
                    "bytes_fetched",
                    "cache_state",
                    "status",
                }
                require(
                    all(
                        required_trace_fields <= event.keys()
                        for event in storage_events
                    ),
                    f"storage trace schema mismatch in {arm}",
                )
                trace_requests = 0
                trace_backing_bytes = 0
                for event in storage_events:
                    require(event["status"] == "ok", f"storage trace failure in {arm}")
                    require(
                        event["operation"] in {"read", "decode"},
                        f"write-like storage trace event in {arm}",
                    )
                    request_count = integer(event, "request_count", storage_trace_path)
                    bytes_fetched = integer(event, "bytes_fetched", storage_trace_path)
                    require(
                        request_count >= 0 and bytes_fetched >= 0,
                        f"negative storage trace evidence in {arm}",
                    )
                    if event["operation"] == "read":
                        require(
                            event["cache_state"] == "backing",
                            f"non-backing read in uncached storage trace {arm}",
                        )
                        trace_requests += request_count
                        trace_backing_bytes += bytes_fetched
                require(
                    aggregate["heads"] == 0 and aggregate["lists"] == 0,
                    f"non-GET read request in {arm}",
                )
                require(
                    trace_requests == aggregate["gets"],
                    f"storage trace request reconciliation failed in {arm}",
                )
                require(
                    trace_backing_bytes == aggregate["backing_bytes"],
                    f"storage trace byte reconciliation failed in {arm}",
                )
            latencies[arm_name].extend(arm_latencies)
            repetition_p95[arm_name].append(computed_p95)
            totals[arm_name]["gets"] += aggregate["gets"]
            totals[arm_name]["logical_bytes"] += aggregate["logical_bytes"]
            totals[arm_name]["backing_bytes"] += aggregate["backing_bytes"]
            totals[arm_name]["hits"] += query_count

    if campaign_id in EXACT_RERANK_CAMPAIGN_IDS:
        for repetition in range(1, int(manifest["repetitions"]) + 1):
            require(
                repetition_hit_ids[(repetition, "candidate")]
                == repetition_hit_ids[(repetition, "control")],
                f"paired hit IDs differ in r{repetition:02}",
            )
            require(
                repetition_ordered_top_10[(repetition, "candidate")]
                == repetition_ordered_top_10[(repetition, "control")],
                f"paired ordered top-10 hit IDs differ in r{repetition:02}",
            )
            require(
                repetition_logical_bytes[(repetition, "candidate")]
                == repetition_logical_bytes[(repetition, "control")],
                f"paired logical bytes differ in r{repetition:02}",
            )

    arms: dict[str, dict[str, object]] = {}
    pooled_queries = int(manifest["repetitions"]) * query_count
    for arm_name in ARM_NAMES:
        require(
            len(latencies[arm_name]) == pooled_queries,
            f"{arm_name} pooled query count mismatch",
        )
        arms[arm_name] = {
            "queries": pooled_queries,
            "recall_at_10": totals[arm_name]["hits"] / pooled_queries,
            "pooled_p50_ms": percentile(latencies[arm_name], 0.50),
            "pooled_p95_ms": percentile(latencies[arm_name], 0.95),
            "worst_repetition_p95_ms": max(repetition_p95[arm_name]),
            "gets_per_query": totals[arm_name]["gets"] / pooled_queries,
            "logical_bytes_per_query": totals[arm_name]["logical_bytes"]
            / pooled_queries,
            "backing_bytes_per_query": totals[arm_name]["backing_bytes"]
            / pooled_queries,
            "repetition_p95_ms": repetition_p95[arm_name],
        }

    control = arms["control"]
    candidate = arms["candidate"]
    control_p95 = float(control["pooled_p95_ms"])
    require(control_p95 > 0.0, "control pooled p95 must be positive")
    control_gets = float(control["gets_per_query"])
    control_backing = float(control["backing_bytes_per_query"])
    require(control_gets > 0.0, "control GET count must be positive")
    require(control_backing > 0.0, "control backing bytes must be positive")
    paired_nonworse = sum(
        candidate_p95 <= control_repeat_p95
        for candidate_p95, control_repeat_p95 in zip(
            repetition_p95["candidate"], repetition_p95["control"], strict=True
        )
    )
    paired_better = sum(
        candidate_p95 < control_repeat_p95
        for candidate_p95, control_repeat_p95 in zip(
            repetition_p95["candidate"], repetition_p95["control"], strict=True
        )
    )
    p95_improvement = (control_p95 - float(candidate["pooled_p95_ms"])) / control_p95
    p95_improvement_ms = control_p95 - float(candidate["pooled_p95_ms"])
    get_amplification = float(candidate["gets_per_query"]) / control_gets - 1.0
    backing_amplification = (
        float(candidate["backing_bytes_per_query"]) / control_backing - 1.0
    )
    paired_criterion = (
        paired_better >= int(manifest["required_better_paired_repetitions"])
        if campaign_id in EXACT_RERANK_CAMPAIGN_IDS
        else paired_nonworse >= int(manifest["required_nonworse_paired_repetitions"])
    )
    improvement_criterion = (
        p95_improvement_ms >= float(manifest["minimum_pooled_p95_improvement_ms"])
        if campaign_id in EXACT_RERANK_CAMPAIGN_IDS
        else p95_improvement
        >= float(manifest["minimum_pooled_p95_improvement_fraction"])
    )
    criteria = {
        "pooled_p95_below_limit": float(candidate["pooled_p95_ms"])
        < float(manifest["max_pooled_p95_ms"]),
        "worst_repetition_p95_below_limit": float(candidate["worst_repetition_p95_ms"])
        < float(manifest["max_worst_repetition_p95_ms"]),
        (
            "paired_better_repetitions"
            if campaign_id in EXACT_RERANK_CAMPAIGN_IDS
            else "paired_nonworse_repetitions"
        ): paired_criterion,
        "minimum_pooled_p95_improvement": improvement_criterion,
        "maximum_pooled_p50_regression": float(candidate["pooled_p50_ms"])
        <= float(control["pooled_p50_ms"])
        * (1.0 + float(manifest["maximum_pooled_p50_regression_fraction"])),
        "identical_logical_bytes": candidate["logical_bytes_per_query"]
        == control["logical_bytes_per_query"],
        "maximum_get_amplification": get_amplification
        <= float(manifest["maximum_get_amplification_fraction"]),
        "maximum_backing_byte_amplification": backing_amplification
        <= float(manifest["maximum_backing_byte_amplification_fraction"]),
    }
    winner = "candidate" if all(criteria.values()) else None
    return {
        "campaign_id": manifest["campaign_id"],
        "terminal_mode": terminal_mode,
        "source_sha256": source_sha,
        "manifest_sha256": manifest_sha,
        "arms": arms,
        "paired_nonworse_repetitions": paired_nonworse,
        "paired_better_repetitions": paired_better,
        "pooled_p95_improvement_fraction": p95_improvement,
        "pooled_p95_improvement_ms": p95_improvement_ms,
        "get_amplification_fraction": get_amplification,
        "backing_byte_amplification_fraction": backing_amplification,
        "selection_criteria": criteria,
        "winner": winner,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("root", nargs="?", type=pathlib.Path)
    parser.add_argument("--manifest", required=True, type=pathlib.Path)
    parser.add_argument("--validate-manifest-only", action="store_true")
    parser.add_argument("--recover-terminal-validator-failure", action="store_true")
    args = parser.parse_args()
    try:
        if args.validate_manifest_only:
            require(
                args.root is None, "manifest-only validation does not accept a root"
            )
            require(
                not args.recover_terminal_validator_failure,
                "manifest-only validation cannot recover a campaign",
            )
            validate_manifest(json.loads(args.manifest.read_bytes()))
            return 0
        require(args.root is not None, "campaign root is required")
        report = validate(
            args.manifest,
            args.root,
            recover_terminal_validator_failure=args.recover_terminal_validator_failure,
        )
    except (
        OSError,
        KeyError,
        TypeError,
        json.JSONDecodeError,
        ValidationError,
    ) as error:
        print(str(error), file=sys.stderr)
        return 1
    json.dump(report, sys.stdout, indent=2, sort_keys=True)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
