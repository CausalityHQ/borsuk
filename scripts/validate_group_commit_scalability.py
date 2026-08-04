#!/usr/bin/env python3
"""Fail-closed validator for terminal group-commit scalability campaigns."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import re
from pathlib import Path


class ValidationError(RuntimeError):
    pass


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ValidationError(message)


def rows(path: Path) -> list[dict[str, str]]:
    require(path.is_file(), f"missing {path}")
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        require(reader.fieldnames is not None, f"missing CSV header in {path}")
        return list(reader)


def finite(value: str, label: str) -> float:
    try:
        parsed = float(value)
    except ValueError as error:
        raise ValidationError(f"invalid {label}: {value!r}") from error
    require(math.isfinite(parsed), f"non-finite {label}")
    return parsed


def integer(value: str, label: str) -> int:
    try:
        return int(value)
    except ValueError as error:
        raise ValidationError(f"invalid {label}: {value!r}") from error


def environment(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        key, separator, value = line.partition("=")
        require(bool(separator) and key not in values, f"invalid environment line {line!r}")
        values[key] = value
    return values


def validate(root: Path, manifest_path: Path) -> None:
    require((root / "GROUP_COMMIT_SCALABILITY_COMPLETE").is_file(), "campaign is incomplete")
    require(not (root / "GROUP_COMMIT_SCALABILITY_FAILED").exists(), "campaign has a failure marker")
    require(manifest_path.is_file(), f"missing manifest {manifest_path}")
    frozen = manifest_path.read_bytes()
    copied = (root / "manifest.json").read_bytes()
    require(copied == frozen, "copied manifest differs from the frozen manifest")
    manifest = json.loads(frozen)
    manifest_sha = hashlib.sha256(frozen).hexdigest()
    identity = environment(root / "environment.txt")
    require(identity.get("manifest_sha256") == manifest_sha, "manifest SHA-256 mismatch")
    source_sha = identity.get("source_sha256", "")
    require(re.fullmatch(r"[0-9a-f]{64}", source_sha) is not None, "invalid source SHA-256")
    require(identity.get("architecture") == manifest["architecture"], "architecture mismatch")
    require(identity.get("instance_type") == manifest["instance_type"], "instance type mismatch")

    expected_cells = {
        (int(cell_count), repetition, int(writers))
        for cell_count in manifest["cell_counts"]
        for repetition in range(1, int(manifest["repetitions"]) + 1)
        for writers in manifest["writers"]
    }
    observed_cells: set[tuple[int, int, int]] = set()
    expected_sample_total = 0
    for cell_count, repetition, writers in sorted(expected_cells):
        cell = root / "cells" / f"c{cell_count}" / f"r{repetition:02d}" / f"w{writers}"
        summary_rows = rows(cell / "summary.csv")
        require(len(summary_rows) == 1, f"{cell} must contain one summary row")
        summary = summary_rows[0]
        require(summary["source_sha256"] == source_sha, f"source identity drift in {cell}")
        require(summary["manifest_sha256"] == manifest_sha, f"manifest identity drift in {cell}")
        require(integer(summary["writers"], "writers") == writers, f"writer drift in {cell}")
        operations = int(manifest["operations_per_writer"])
        expected_records = writers * operations
        require(integer(summary["operations"], "operations") == operations, f"operation drift in {cell}")
        require(
            integer(summary["pipeline_depth"], "pipeline depth")
            == int(manifest.get("pipeline_depth_per_writer", 1)),
            f"pipeline depth drift in {cell}",
        )
        require(
            integer(summary["worker_lanes"], "worker lanes")
            == int(manifest.get("worker_lanes", 1)),
            f"worker lane drift in {cell}",
        )
        require(integer(summary["records"], "records") == expected_records, f"record drift in {cell}")
        require(integer(summary["visible_records"], "visible records") == expected_records, f"visibility failure in {cell}")
        require(
            integer(summary["recall_queries"], "recall queries")
            == int(manifest["read_queries_per_cell"]),
            f"recall query count drift in {cell}",
        )
        require(
            integer(summary["max_read_segments"], "max read segments")
            == int(manifest["max_read_segments"]),
            f"read segment budget drift in {cell}",
        )
        require(
            finite(summary["recall_at_1"], "recall at 1")
            >= float(manifest["min_recall_at_1"]),
            f"recall failure in {cell}",
        )
        for field in (
            "elapsed_ms",
            "drain_ms",
            "p50_ms",
            "p95_ms",
            "records_per_second",
            "requests_per_record",
            "read_p50_ms",
            "read_p95_ms",
        ):
            require(finite(summary[field], field) >= 0.0, f"negative {field} in {cell}")
        if manifest.get("protocol_kind") == "production":
            require(
                finite(summary["p95_ms"], "p95_ms") < float(manifest["max_p95_ms"]),
                f"production p95 gate failed in {cell}",
            )
            require(
                finite(summary["records_per_second"], "records_per_second")
                >= float(manifest["min_records_per_second_per_writer"]) * writers,
                f"production throughput gate failed in {cell}",
            )
            require(
                finite(summary["read_p95_ms"], "read_p95_ms")
                < float(manifest["max_read_p95_ms"]),
                f"production read p95 gate failed in {cell}",
            )
            require(
                (cell / "PRODUCTION_PERFORMANCE_GATE_COMPLETE").is_file(),
                f"missing production performance gate marker in {cell}",
            )
            require(
                not (cell / "PRODUCTION_PERFORMANCE_GATE_FAILED").exists(),
                f"production performance failure marker in {cell}",
            )

        samples = rows(cell / "samples.csv")
        require(len(samples) == expected_records, f"raw sample count mismatch in {cell}")
        ids: set[str] = set()
        groups: dict[tuple[int, int], tuple[int, int, int, int, int]] = {}
        writer_operations: set[tuple[int, int]] = set()
        for sample in samples:
            writer = integer(sample["writer"], "sample writer")
            operation = integer(sample["operation"], "sample operation")
            require(0 <= writer < writers and 0 <= operation < operations, f"sample coordinate out of range in {cell}")
            require((writer, operation) not in writer_operations, f"duplicate sample coordinate in {cell}")
            writer_operations.add((writer, operation))
            require(sample["record_id"] not in ids, f"duplicate record id in {cell}")
            ids.add(sample["record_id"])
            require(finite(sample["latency_ms"], "sample latency") >= 0.0, f"negative sample latency in {cell}")
            group = (
                integer(sample["commit_lane"], "commit lane"),
                integer(sample["commit_sequence"], "commit sequence"),
            )
            evidence = tuple(
                integer(sample[field], field)
                for field in ("committed_records", "group_requests", "group_gets", "group_puts", "group_heads")
            )
            require(group not in groups or groups[group] == evidence, f"inconsistent shared group evidence in {cell}")
            groups[group] = evidence
        require(sum(evidence[0] for evidence in groups.values()) == expected_records, f"group record reconciliation failed in {cell}")
        total_requests = sum(evidence[1] for evidence in groups.values())
        require(total_requests == integer(summary["storage_requests"], "storage requests"), f"request reconciliation failed in {cell}")
        require(len(groups) == integer(summary["groups"], "groups"), f"group count mismatch in {cell}")

        resource = Path(f"{cell}.resources.txt")
        require(resource.is_file(), f"missing resource telemetry {resource}")
        require("Exit status: 0" in resource.read_text(encoding="utf-8"), f"nonzero resource exit in {cell}")
        observed_cells.add((cell_count, repetition, writers))
        expected_sample_total += expected_records

    require(observed_cells == expected_cells, "matrix coverage mismatch")
    aggregate_summary = rows(root / "summary.csv")
    aggregate_samples = rows(root / "samples.csv")
    require(len(aggregate_summary) == len(expected_cells), "aggregate summary count mismatch")
    require(len(aggregate_samples) == expected_sample_total, "aggregate sample count mismatch")
    aggregate_keys = {
        (integer(row["cell_count"], "aggregate cell count"), integer(row["repetition"], "aggregate repetition"), integer(row["writers"], "aggregate writers"))
        for row in aggregate_summary
    }
    require(aggregate_keys == expected_cells, "aggregate matrix coverage mismatch")

    correctness = rows(root / "correctness.csv")
    expected_gates = set(manifest["correctness_gates"])
    require({row["gate"] for row in correctness} == expected_gates, "correctness gate coverage mismatch")
    require(all(row["status"] == "pass" for row in correctness), "correctness gate failure")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("root", type=Path)
    parser.add_argument("--manifest", required=True, type=Path)
    args = parser.parse_args()
    try:
        validate(args.root, args.manifest)
    except (OSError, KeyError, TypeError, json.JSONDecodeError, ValidationError) as error:
        print(f"group-commit scalability validation failed: {error}")
        return 1
    print("group-commit scalability artifacts are structurally valid")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
