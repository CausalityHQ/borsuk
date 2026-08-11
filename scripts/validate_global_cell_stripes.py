#!/usr/bin/env python3
"""Fail-closed validator for the paired global-cell stripe qualification."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import pathlib
import sys
from collections.abc import Iterable

QUALIFICATION_CAMPAIGN = "global-cell-stripe-qualification-v1"
CONFIRMATION_CAMPAIGN = "global-cell-stripe-confirmation-v1"
QUALIFICATION_COMPLETE = "GLOBAL_CELL_STRIPE_QUALIFICATION_COMPLETE"
QUALIFICATION_FAILED = "GLOBAL_CELL_STRIPE_QUALIFICATION_FAILED"
CONFIRMATION_COMPLETE = "GLOBAL_CELL_STRIPE_CONFIRMATION_COMPLETE"
CONFIRMATION_FAILED = "GLOBAL_CELL_STRIPE_CONFIRMATION_FAILED"


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


def read_one_csv(path: pathlib.Path) -> dict[str, str]:
    with path.open(newline="") as handle:
        rows = list(csv.DictReader(handle))
    require(len(rows) == 1, f"{path} must contain exactly one summary row")
    return rows[0]


def read_csv(path: pathlib.Path) -> list[dict[str, str]]:
    with path.open(newline="") as handle:
        return list(csv.DictReader(handle))


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
    index = int((len(ordered) - 1) * quantile + 0.5)
    return ordered[index]


def stripe_name(stripe_bytes: int) -> str:
    require(stripe_bytes % (1024 * 1024) == 0, "stripe width is not MiB-aligned")
    return f"s{stripe_bytes // (1024 * 1024)}m"


def campaign_schema(manifest: dict[str, object]) -> tuple[str, str, list[int]]:
    campaign_id = manifest.get("campaign_id")
    mib = 1024 * 1024
    if campaign_id == QUALIFICATION_CAMPAIGN:
        complete = QUALIFICATION_COMPLETE
        failed = QUALIFICATION_FAILED
        stripes = [mib, 2 * mib, 4 * mib]
        expected_orders = [
            [mib, 2 * mib, 4 * mib],
            [2 * mib, 4 * mib, mib],
            [4 * mib, mib, 2 * mib],
            [mib, 2 * mib, 4 * mib],
            [2 * mib, 4 * mib, mib],
        ]
        require(
            manifest.get("protocol_kind") == "production-diagnostic",
            "qualification protocol changed",
        )
        require(
            manifest.get("queries_per_arm") == 100, "qualification query count changed"
        )
    elif campaign_id == CONFIRMATION_CAMPAIGN:
        complete = CONFIRMATION_COMPLETE
        failed = CONFIRMATION_FAILED
        stripes = [mib, 4 * mib]
        expected_orders = [
            [mib, 4 * mib],
            [4 * mib, mib],
            [mib, 4 * mib],
            [4 * mib, mib],
            [mib, 4 * mib],
        ]
        require(
            manifest.get("protocol_kind") == "stripe-confirmation",
            "confirmation protocol changed",
        )
        require(
            manifest.get("queries_per_arm") == 500, "confirmation query count changed"
        )
        require(
            manifest.get("max_pooled_p95_ms") == 200.0,
            "confirmation pooled p95 limit changed",
        )
        require(
            manifest.get("max_worst_repetition_p95_ms") == 200.0,
            "confirmation worst-repeat p95 limit changed",
        )
        require(
            manifest.get("minimum_pooled_p95_improvement_fraction") == 0.10,
            "confirmation p95 effect floor changed",
        )
        require(
            manifest.get("maximum_pooled_p50_regression_fraction") == 0.05,
            "confirmation p50 regression guard changed",
        )
    else:
        raise ValidationError(f"unknown campaign_id {campaign_id!r}")
    frozen_common = {
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
        "max_read_segments": 4,
        "resource_sample_interval_ms": 100,
    }
    for key, expected in frozen_common.items():
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
            "READ_QUALIFICATION_COMPLETE",
            "CELL_COMPLETE",
        ],
        "required artifact set changed",
    )
    require(
        manifest.get("root_complete_marker") == complete,
        "manifest completion marker changed",
    )
    require(
        manifest.get("root_failure_marker") == failed, "manifest failure marker changed"
    )
    require(manifest.get("stripe_bytes") == stripes, "manifest stripe arms changed")
    require(manifest.get("repetitions") == 5, "manifest repetition count changed")
    require(
        manifest.get("arm_orders") == expected_orders, "manifest arm orders changed"
    )
    require(
        manifest.get("fresh_process_per_arm") is True,
        "fresh-process requirement changed",
    )
    require(
        manifest.get("fresh_cache_per_arm") is True, "fresh-cache requirement changed"
    )
    require(manifest.get("required_recall_at_10") == 1.0, "recall requirement changed")
    require(
        manifest.get("required_nonworse_paired_repetitions") == 4,
        "paired repetition requirement changed",
    )
    return complete, failed, stripes


def validate(
    manifest_path: pathlib.Path,
    root: pathlib.Path,
    recover_terminal_validator_failure: bool = False,
) -> dict[str, object]:
    # The immutable manifest selects exact marker names. Terminality is checked
    # before any campaign measurement CSV is opened.
    manifest_bytes = manifest_path.read_bytes()
    manifest = json.loads(manifest_bytes)
    complete, failed, expected_stripes = campaign_schema(manifest)
    if recover_terminal_validator_failure:
        require(
            not (root / complete).exists(), "recovery requires no completion marker"
        )
        require(
            (root / failed).is_file(), "recovery requires the terminal failure marker"
        )
        terminal_mode = "validator-failure-recovery"
    else:
        require((root / complete).is_file(), "campaign is incomplete")
        require(not (root / failed).exists(), "campaign has a failure marker")
        terminal_mode = "complete"

    preserved_manifest = root / "manifest.json"
    require(preserved_manifest.is_file(), "missing preserved manifest.json")
    require(
        preserved_manifest.read_bytes() == manifest_bytes, "preserved manifest differs"
    )
    manifest_sha = hashlib.sha256(manifest_bytes).hexdigest()

    required_artifacts = list(manifest["required_artifacts"])
    query_count = int(manifest["queries_per_arm"])
    repetitions = int(manifest["repetitions"])
    orders = manifest["arm_orders"]
    require(
        len(orders) == repetitions, "manifest arm-order count differs from repetitions"
    )
    require(
        manifest["stripe_bytes"] == expected_stripes, "manifest stripe arms changed"
    )

    seen_caches: set[str] = set()
    source_sha: str | None = None
    paired_query_ids: tuple[str, ...] | None = None
    arm_latencies: dict[str, list[float]] = {
        stripe_name(value): [] for value in manifest["stripe_bytes"]
    }
    arm_gets: dict[str, int] = {name: 0 for name in arm_latencies}
    arm_bytes: dict[str, int] = {name: 0 for name in arm_latencies}
    arm_hits: dict[str, int] = {name: 0 for name in arm_latencies}
    repetition_p95: dict[str, list[float]] = {name: [] for name in arm_latencies}

    for repetition, order in enumerate(orders, 1):
        require(
            sorted(order) == sorted(manifest["stripe_bytes"]),
            f"r{repetition:02} arm order changed",
        )
        for order_position, stripe_bytes in enumerate(order):
            name = stripe_name(stripe_bytes)
            arm = root / "repetitions" / f"r{repetition:02}" / name
            for artifact in required_artifacts:
                require((arm / artifact).is_file(), f"missing {artifact} in {arm}")
            require(not (arm / "CELL_FAILED").exists(), f"failure marker in {arm}")
            require(
                (arm / "process_exit.txt").read_text().strip() == "0",
                f"nonzero process exit in {arm}",
            )
            require(
                (arm / "resources.csv").stat().st_size > 0,
                f"empty resources.csv in {arm}",
            )
            require(
                (arm / "storage-access.csv").stat().st_size > 0,
                f"empty storage-access.csv in {arm}",
            )
            require(
                bool(read_csv(arm / "resources.csv")),
                f"resources.csv has no samples in {arm}",
            )
            require(
                bool(read_csv(arm / "storage-access.csv")),
                f"storage-access.csv has no events in {arm}",
            )

            environment = read_key_values(arm / "environment.txt")
            expected_environment = {
                "manifest_sha256": manifest_sha,
                "base_source_sha256": manifest["base_source_sha256"],
                "base_manifest_sha256": manifest["base_manifest_sha256"],
                "base_samples_sha256": manifest["base_samples_sha256"],
                "dataset_sha256": manifest["dataset_sha256"],
                "base_cell": manifest["base_cell"],
                "index_uri": manifest["base_index_uri"],
                "repetition": str(repetition),
                "stripe_bytes": str(stripe_bytes),
                "order_position": str(order_position),
            }
            for key, expected in expected_environment.items():
                require(
                    environment.get(key) == str(expected), f"{key} mismatch in {arm}"
                )
            cache_dir = environment.get("cache_dir", "")
            require(bool(cache_dir), f"missing cache_dir in {arm}")
            require(cache_dir not in seen_caches, f"reused cache_dir in {arm}")
            seen_caches.add(cache_dir)
            current_source = environment.get("source_sha256", "")
            require(len(current_source) == 64, f"invalid source_sha256 in {arm}")
            if source_sha is None:
                source_sha = current_source
            require(current_source == source_sha, f"source_sha256 mismatch in {arm}")

            summary = read_one_csv(arm / "summary.csv")
            expected_summary = {
                "protocol_kind": manifest["protocol_kind"],
                "source_sha256": source_sha,
                **expected_environment,
                "queries": str(query_count),
            }
            expected_summary.pop("order_position", None)
            # Current benchmark summaries also carry order_position; require it
            # separately so an older binary cannot pass by omitting the field.
            expected_summary["order_position"] = str(order_position)
            for key, expected in expected_summary.items():
                require(
                    summary.get(key) == str(expected),
                    f"{key} mismatch in {arm / 'summary.csv'}",
                )
            require(
                floating(summary, "inserted_id_recall_at_10", arm / "summary.csv")
                == float(manifest["required_recall_at_10"]),
                f"recall gate failed in {arm}",
            )
            require(
                integer(summary, "read_storage_puts", arm / "summary.csv") == 0,
                f"PUT in {arm}",
            )
            require(
                integer(summary, "read_storage_deletes", arm / "summary.csv") == 0,
                f"DELETE in {arm}",
            )

            reads = read_csv(arm / "reads.csv")
            require(
                len(reads) == query_count,
                f"{arm / 'reads.csv'} has {len(reads)} queries",
            )
            latencies: list[float] = []
            query_ids: list[str] = []
            gets = 0
            requests = 0
            puts = 0
            deletes = 0
            heads = 0
            lists = 0
            bytes_read = 0
            hits = 0
            for query, row in enumerate(reads):
                require(
                    integer(row, "query", arm / "reads.csv") == query,
                    f"query order mismatch in {arm}",
                )
                contains = row.get("contains_record_id", "").lower() == "true"
                require(contains, f"inserted ID recall miss in {arm} query {query}")
                row_gets = integer(row, "gets", arm / "reads.csv")
                row_puts = integer(row, "puts", arm / "reads.csv")
                row_deletes = integer(row, "deletes", arm / "reads.csv")
                row_heads = integer(row, "heads", arm / "reads.csv")
                row_lists = integer(row, "lists", arm / "reads.csv")
                row_requests = integer(row, "requests", arm / "reads.csv")
                require(row_puts == 0, f"query PUT in {arm}")
                require(row_deletes == 0, f"query DELETE in {arm}")
                require(
                    row_requests
                    == row_gets + row_puts + row_deletes + row_heads + row_lists,
                    f"query request total does not reconcile in {arm}",
                )
                query_ids.append(row.get("record_id", ""))
                latencies.append(floating(row, "latency_ms", arm / "reads.csv"))
                requests += row_requests
                gets += row_gets
                puts += row_puts
                deletes += row_deletes
                heads += row_heads
                lists += row_lists
                bytes_read += integer(row, "bytes_read", arm / "reads.csv")
                hits += 1
            current_query_ids = tuple(query_ids)
            if paired_query_ids is None:
                paired_query_ids = current_query_ids
            require(
                current_query_ids == paired_query_ids,
                f"paired query IDs differ in {arm}",
            )
            computed_p95 = percentile(latencies, 0.95)
            computed_p50 = percentile(latencies, 0.50)
            reported_p95 = floating(summary, "read_p95_ms", arm / "summary.csv")
            reported_p50 = floating(summary, "read_p50_ms", arm / "summary.csv")
            require(
                abs(computed_p95 - reported_p95) <= 1e-6,
                f"read_p95_ms does not reconcile in {arm}",
            )
            require(
                abs(computed_p50 - reported_p50) <= 1e-6,
                f"read_p50_ms does not reconcile in {arm}",
            )
            require(
                integer(summary, "read_storage_gets", arm / "summary.csv") == gets,
                f"GET total mismatch in {arm}",
            )
            require(
                integer(summary, "read_storage_requests", arm / "summary.csv")
                == requests,
                f"request total mismatch in {arm}",
            )
            require(
                integer(summary, "read_storage_puts", arm / "summary.csv") == puts,
                f"PUT total mismatch in {arm}",
            )
            require(
                integer(summary, "read_storage_deletes", arm / "summary.csv")
                == deletes,
                f"DELETE total mismatch in {arm}",
            )
            require(
                integer(summary, "read_storage_heads", arm / "summary.csv") == heads,
                f"HEAD total mismatch in {arm}",
            )
            require(
                integer(summary, "read_storage_lists", arm / "summary.csv") == lists,
                f"LIST total mismatch in {arm}",
            )
            require(
                integer(summary, "read_bytes", arm / "summary.csv") == bytes_read,
                f"byte total mismatch in {arm}",
            )
            arm_latencies[name].extend(latencies)
            arm_gets[name] += gets
            arm_bytes[name] += bytes_read
            arm_hits[name] += hits
            repetition_p95[name].append(computed_p95)

    arms: dict[str, dict[str, object]] = {}
    for name in sorted(arm_latencies):
        values = arm_latencies[name]
        queries = len(values)
        require(
            queries == query_count * repetitions, f"{name} pooled query count mismatch"
        )
        arms[name] = {
            "queries": queries,
            "recall_at_10": arm_hits[name] / queries,
            "pooled_p50_ms": percentile(values, 0.50),
            "pooled_p95_ms": percentile(values, 0.95),
            "worst_repetition_p95_ms": max(repetition_p95[name]),
            "gets_per_query": arm_gets[name] / queries,
            "bytes_per_query": arm_bytes[name] / queries,
            "repetition_p95_ms": repetition_p95[name],
        }

    control = repetition_p95["s1m"]
    promotable: list[str] = []
    paired_nonworse: dict[str, int] = {}
    selection_criteria: dict[str, bool] = {}
    winner = None
    if manifest["campaign_id"] == QUALIFICATION_CAMPAIGN:
        for name in ("s2m", "s4m"):
            require(
                len(repetition_p95[name]) == len(control),
                f"{name} paired repetition count differs from the control",
            )
            count = sum(
                candidate <= baseline
                for candidate, baseline in zip(
                    repetition_p95[name], control, strict=True
                )
            )
            paired_nonworse[name] = count
            if arms[name]["pooled_p95_ms"] < float(
                manifest["max_pooled_p95_ms"]
            ) and count >= int(manifest["required_nonworse_paired_repetitions"]):
                promotable.append(name)
        if promotable:
            winner = min(
                promotable,
                key=lambda name: (
                    arms[name]["worst_repetition_p95_ms"],
                    arms[name]["gets_per_query"],
                ),
            )
    else:
        name = "s4m"
        require(
            len(repetition_p95[name]) == len(control),
            f"{name} paired repetition count differs from the control",
        )
        count = sum(
            candidate <= baseline
            for candidate, baseline in zip(repetition_p95[name], control, strict=True)
        )
        paired_nonworse[name] = count
        baseline_p95 = float(arms["s1m"]["pooled_p95_ms"])
        candidate_p95 = float(arms[name]["pooled_p95_ms"])
        require(baseline_p95 > 0.0, "control pooled p95 must be positive")
        improvement = (baseline_p95 - candidate_p95) / baseline_p95
        selection_criteria = {
            "pooled_p95_below_limit": candidate_p95
            < float(manifest["max_pooled_p95_ms"]),
            "worst_repetition_p95_below_limit": float(
                arms[name]["worst_repetition_p95_ms"]
            )
            < float(manifest["max_worst_repetition_p95_ms"]),
            "paired_nonworse_repetitions": count
            >= int(manifest["required_nonworse_paired_repetitions"]),
            "minimum_pooled_p95_improvement": improvement
            >= float(manifest["minimum_pooled_p95_improvement_fraction"]),
            "maximum_pooled_p50_regression": float(arms[name]["pooled_p50_ms"])
            <= float(arms["s1m"]["pooled_p50_ms"])
            * (1.0 + float(manifest["maximum_pooled_p50_regression_fraction"])),
            "identical_logical_bytes": arms[name]["bytes_per_query"]
            == arms["s1m"]["bytes_per_query"],
        }
        if all(selection_criteria.values()):
            promotable.append(name)
            winner = name
    return {
        "campaign_id": manifest["campaign_id"],
        "terminal_mode": terminal_mode,
        "source_sha256": source_sha,
        "manifest_sha256": manifest_sha,
        "arms": arms,
        "paired_nonworse_repetitions": paired_nonworse,
        "selection_criteria": selection_criteria,
        "promotable": promotable,
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
    except (OSError, KeyError, json.JSONDecodeError, ValidationError) as error:
        print(str(error), file=sys.stderr)
        return 1
    json.dump(report, sys.stdout, indent=2, sort_keys=True)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
