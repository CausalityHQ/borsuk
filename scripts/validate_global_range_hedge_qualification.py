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


CAMPAIGN_ID = "global-range-hedge-qualification-v1"
COMPLETE_MARKER = "GLOBAL_RANGE_HEDGE_QUALIFICATION_COMPLETE"
FAILURE_MARKER = "GLOBAL_RANGE_HEDGE_QUALIFICATION_FAILED"
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


def validate_manifest(manifest: dict[str, object]) -> None:
    frozen = {
        "campaign_id": CAMPAIGN_ID,
        "protocol": "read-hedge-qualification",
        "architecture": "aarch64",
        "instance_type": "c7g.8xlarge",
        "base_run_id": "20260808T091300Z-v67-40911df",
        "base_cell": "c2000/r01/l1/w8",
        "base_index_uri": "s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260808T091300Z-v67-40911df/index/cells/c2000/r01/l1/w8",
        "base_samples_uri": "s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260808T091300Z-v67-40911df/results/cells/c2000/r01/l1/w8/samples.csv",
        "base_source_sha256": "4ea819fbb9cb4e203811410e40f7c158dca5fc18a3644012d96341155aa52423",
        "base_manifest_sha256": "81c849548d9ef7300cffd88a0a13aca2023645ae0af40e66f0da5a60ad37408a",
        "base_samples_sha256": "7ec84babc5dc24bdc6275898155d362bf7e4c487c39491d1e136e2ba9906f578",
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
        "hedge_after_ms": {"control": "none", "candidate": "75"},
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
        "required_nonworse_paired_repetitions": 4,
        "minimum_pooled_p95_improvement_fraction": 0.10,
        "maximum_pooled_p50_regression_fraction": 0.05,
        "maximum_get_amplification_fraction": 0.20,
        "maximum_backing_byte_amplification_fraction": 0.20,
        "root_complete_marker": COMPLETE_MARKER,
        "root_failure_marker": FAILURE_MARKER,
    }
    for key, expected in frozen.items():
        require(manifest.get(key) == expected, f"manifest {key} changed")
    require(
        manifest.get("required_artifacts")
        == [
            "summary.csv",
            "reads.csv",
            "resources.csv",
            "storage-access.csv",
            "environment.txt",
            "process_exit.txt",
            "READ_HEDGE_QUALIFICATION_COMPLETE",
            "CELL_COMPLETE",
        ],
        "required artifact set changed",
    )


def validate(
    manifest_path: pathlib.Path,
    root: pathlib.Path,
    recover_terminal_validator_failure: bool = False,
) -> dict[str, object]:
    manifest_bytes = manifest_path.read_bytes()
    manifest = json.loads(manifest_bytes)
    validate_manifest(manifest)

    # Never open a measurement CSV before root terminality is established.
    if recover_terminal_validator_failure:
        require(not (root / COMPLETE_MARKER).exists(), "recovery requires no completion marker")
        require((root / FAILURE_MARKER).is_file(), "recovery requires the terminal failure marker")
        terminal_mode = "validator-failure-recovery"
    else:
        require((root / COMPLETE_MARKER).is_file(), "campaign is incomplete")
        require(not (root / FAILURE_MARKER).exists(), "campaign has a failure marker")
        terminal_mode = "complete"
    preserved_manifest = root / "manifest.json"
    require(preserved_manifest.is_file(), "missing preserved manifest.json")
    require(preserved_manifest.read_bytes() == manifest_bytes, "preserved manifest differs")
    manifest_sha = hashlib.sha256(manifest_bytes).hexdigest()

    query_count = int(manifest["queries_per_arm"])
    expected_query_ids = tuple(f"group-o{query * 2 * 16:08}" for query in range(query_count))
    source_sha: str | None = None
    latencies: dict[str, list[float]] = {name: [] for name in ARM_NAMES}
    repetition_p95: dict[str, list[float]] = {name: [] for name in ARM_NAMES}
    totals = {
        name: {"gets": 0, "logical_bytes": 0, "backing_bytes": 0, "hits": 0}
        for name in ARM_NAMES
    }

    for repetition, order in enumerate(manifest["arm_orders"], 1):
        require(sorted(order) == sorted(ARM_NAMES), f"r{repetition:02} arm order changed")
        for order_position, arm_name in enumerate(order):
            arm = root / "repetitions" / f"r{repetition:02}" / arm_name
            for artifact in manifest["required_artifacts"]:
                require((arm / artifact).is_file(), f"missing {artifact} in {arm}")
            require(not (arm / "CELL_FAILED").exists(), f"failure marker in {arm}")
            require((arm / "process_exit.txt").read_text().strip() == "0", f"nonzero process exit in {arm}")
            require(bool(read_csv(arm / "resources.csv")), f"resources.csv has no samples in {arm}")
            require(bool(read_csv(arm / "storage-access.csv")), f"storage-access.csv has no events in {arm}")

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
                require(environment.get(key) == str(expected), f"{key} mismatch in {arm}")
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
                require(summary.get(key) == str(expected), f"{key} mismatch in {summary_path}")
            require(
                floating(summary, "inserted_id_recall_at_10", summary_path)
                == float(manifest["required_recall_at_10"]),
                f"recall gate failed in {arm}",
            )
            require(integer(summary, "read_storage_puts", summary_path) == 0, f"PUT in {arm}")
            require(integer(summary, "read_storage_deletes", summary_path) == 0, f"DELETE in {arm}")
            require(integer(summary, "read_disk_cache_bytes", summary_path) == 0, f"disk-cache bytes in {arm}")

            reads_path = arm / "reads.csv"
            reads = read_csv(reads_path)
            require(len(reads) == query_count, f"{reads_path} has {len(reads)} queries")
            arm_latencies: list[float] = []
            query_ids: list[str] = []
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
                require(integer(row, "query", reads_path) == query, f"query order mismatch in {arm}")
                record_id = row.get("record_id", "")
                require(record_id == expected_query_ids[query], f"writer-zero query cohort changed in {arm}")
                query_ids.append(record_id)
                require(row.get("contains_record_id", "").lower() == "true", f"recall miss in {arm}")
                row_counts = {
                    key: integer(row, key, reads_path)
                    for key in ("gets", "puts", "deletes", "heads", "lists")
                }
                request_count = integer(row, "requests", reads_path)
                require(request_count == sum(row_counts.values()), f"request total mismatch in {arm}")
                require(row_counts["puts"] == 0 and row_counts["deletes"] == 0, f"write request in {arm}")
                disk_bytes = integer(row, "disk_cache_bytes_read", reads_path)
                require(disk_bytes == 0, f"disk-cache row in {arm}")
                arm_latencies.append(floating(row, "latency_ms", reads_path))
                aggregate["requests"] += request_count
                for key in ("gets", "puts", "deletes", "heads", "lists"):
                    aggregate[key] += row_counts[key]
                aggregate["logical_bytes"] += integer(row, "bytes_read", reads_path)
                aggregate["disk_bytes"] += disk_bytes
                aggregate["backing_bytes"] += integer(row, "backing_bytes_read", reads_path)
                aggregate["segments"] += integer(row, "segments_searched", reads_path)
            require(tuple(query_ids) == expected_query_ids, f"paired query IDs differ in {arm}")
            computed_p50 = percentile(arm_latencies, 0.50)
            computed_p95 = percentile(arm_latencies, 0.95)
            require(abs(computed_p50 - floating(summary, "read_p50_ms", summary_path)) <= 1e-6, f"p50 mismatch in {arm}")
            require(abs(computed_p95 - floating(summary, "read_p95_ms", summary_path)) <= 1e-6, f"p95 mismatch in {arm}")
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
                require(integer(summary, summary_key, summary_path) == aggregate[aggregate_key], f"{summary_key} does not reconcile in {arm}")
            latencies[arm_name].extend(arm_latencies)
            repetition_p95[arm_name].append(computed_p95)
            totals[arm_name]["gets"] += aggregate["gets"]
            totals[arm_name]["logical_bytes"] += aggregate["logical_bytes"]
            totals[arm_name]["backing_bytes"] += aggregate["backing_bytes"]
            totals[arm_name]["hits"] += query_count

    arms: dict[str, dict[str, object]] = {}
    pooled_queries = int(manifest["repetitions"]) * query_count
    for arm_name in ARM_NAMES:
        require(len(latencies[arm_name]) == pooled_queries, f"{arm_name} pooled query count mismatch")
        arms[arm_name] = {
            "queries": pooled_queries,
            "recall_at_10": totals[arm_name]["hits"] / pooled_queries,
            "pooled_p50_ms": percentile(latencies[arm_name], 0.50),
            "pooled_p95_ms": percentile(latencies[arm_name], 0.95),
            "worst_repetition_p95_ms": max(repetition_p95[arm_name]),
            "gets_per_query": totals[arm_name]["gets"] / pooled_queries,
            "logical_bytes_per_query": totals[arm_name]["logical_bytes"] / pooled_queries,
            "backing_bytes_per_query": totals[arm_name]["backing_bytes"] / pooled_queries,
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
            repetition_p95["candidate"], repetition_p95["control"]
        )
    )
    p95_improvement = (control_p95 - float(candidate["pooled_p95_ms"])) / control_p95
    get_amplification = float(candidate["gets_per_query"]) / control_gets - 1.0
    backing_amplification = (
        float(candidate["backing_bytes_per_query"]) / control_backing - 1.0
    )
    criteria = {
        "pooled_p95_below_limit": float(candidate["pooled_p95_ms"])
        < float(manifest["max_pooled_p95_ms"]),
        "worst_repetition_p95_below_limit": float(candidate["worst_repetition_p95_ms"])
        < float(manifest["max_worst_repetition_p95_ms"]),
        "paired_nonworse_repetitions": paired_nonworse
        >= int(manifest["required_nonworse_paired_repetitions"]),
        "minimum_pooled_p95_improvement": p95_improvement
        >= float(manifest["minimum_pooled_p95_improvement_fraction"]),
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
        "pooled_p95_improvement_fraction": p95_improvement,
        "get_amplification_fraction": get_amplification,
        "backing_byte_amplification_fraction": backing_amplification,
        "selection_criteria": criteria,
        "winner": winner,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("root", type=pathlib.Path)
    parser.add_argument("--manifest", required=True, type=pathlib.Path)
    parser.add_argument("--recover-terminal-validator-failure", action="store_true")
    args = parser.parse_args()
    try:
        report = validate(
            args.manifest,
            args.root,
            recover_terminal_validator_failure=args.recover_terminal_validator_failure,
        )
    except (OSError, KeyError, TypeError, json.JSONDecodeError, ValidationError) as error:
        print(str(error), file=sys.stderr)
        return 1
    json.dump(report, sys.stdout, indent=2, sort_keys=True)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
