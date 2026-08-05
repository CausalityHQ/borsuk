#!/usr/bin/env python3
"""Fail-closed validator for terminal realistic durable-write campaigns."""

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


def read_rows(path: Path) -> list[dict[str, str]]:
    require(path.is_file(), f"missing {path}")
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        require(reader.fieldnames is not None, f"missing CSV header in {path}")
        return list(reader)


def finite(value: str, label: str) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError) as error:
        raise ValidationError(f"invalid {label}: {value!r}") from error
    require(math.isfinite(parsed), f"non-finite {label}")
    return parsed


def integer(value: str, label: str) -> int:
    try:
        return int(value)
    except (TypeError, ValueError) as error:
        raise ValidationError(f"invalid {label}: {value!r}") from error


def read_environment(path: Path) -> dict[str, str]:
    require(path.is_file(), f"missing {path}")
    values: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        key, separator, value = line.partition("=")
        require(bool(separator) and bool(key) and key not in values, f"invalid environment line {line!r}")
        values[key] = value
    return values


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def validate(
    root: Path,
    manifest_path: Path,
    *,
    expected_dataset_sha: str | None = None,
    terminal_cell: tuple[str, int, int] | None = None,
) -> None:
    # The root marker gate deliberately precedes every measurement read.
    if terminal_cell is None:
        require(
            (root / "REALISTIC_DURABLE_WRITE_COMPLETE").is_file(),
            "campaign is incomplete",
        )
    require(
        not (root / "REALISTIC_DURABLE_WRITE_FAILED").exists(),
        "campaign has a failure marker",
    )
    require(manifest_path.is_file(), f"missing manifest {manifest_path}")
    frozen = manifest_path.read_bytes()
    copied_path = root / "manifest.json"
    require(copied_path.is_file(), "missing copied manifest")
    require(copied_path.read_bytes() == frozen, "copied manifest differs from frozen manifest")
    manifest = json.loads(frozen)
    identity = read_environment(root / "environment.txt")
    require(identity.get("manifest_sha256") == hashlib.sha256(frozen).hexdigest(), "manifest SHA-256 mismatch")
    for name in ("source_sha256", "dataset_sha256"):
        require(re.fullmatch(r"[0-9a-f]{64}", identity.get(name, "")) is not None, f"invalid {name.replace('_', ' ')}")
    descriptor_path = root / "dataset.json"
    require(descriptor_path.is_file(), "missing copied dataset descriptor")
    descriptor_sha = sha256(descriptor_path)
    require(
        identity["dataset_sha256"] == descriptor_sha,
        "dataset descriptor SHA-256 mismatch",
    )
    if "dataset_sha256" in manifest:
        require(
            descriptor_sha == manifest["dataset_sha256"],
            "dataset descriptor differs from frozen manifest",
        )
    descriptor = json.loads(descriptor_path.read_bytes())
    if "dataset_source_sha256" in manifest:
        require(
            descriptor.get("source_sha256") == manifest["dataset_source_sha256"],
            "dataset source SHA-256 mismatch",
        )
    if expected_dataset_sha is not None:
        require(identity["dataset_sha256"] == expected_dataset_sha, "dataset SHA-256 mismatch")
    require(identity.get("architecture") == manifest["architecture"], "architecture mismatch")
    require(identity.get("instance_type") == manifest["instance_type"], "instance type mismatch")

    minimum_batches = int(manifest["minimum_raw_batches"])
    max_p95_ms = float(manifest["max_write_p95_ms"])
    frozen_cells = {
        (dataset, repetition, int(batch_records))
        for dataset in manifest["datasets"]
        for repetition in range(1, int(manifest["repetitions"]) + 1)
        for batch_records in manifest["batch_records"]
    }
    if terminal_cell is not None:
        require(terminal_cell in frozen_cells, "terminal cell is outside the frozen matrix")
        cells = [terminal_cell]
    else:
        cells = sorted(frozen_cells)
    for dataset, repetition, batch_records in cells:
        require(re.fullmatch(r"[A-Za-z0-9._-]+", dataset) is not None, "unsafe dataset name")
        batch_records = int(batch_records)
        expected_ops = int(
            manifest.get("write_operations", batch_records * minimum_batches)
        )
        cell = root / "cells" / dataset / f"r{repetition:02d}" / f"b{batch_records}"
        # A cell is eligible only after all terminal markers exist.
        require((cell / "CELL_COMPLETE").is_file(), f"cell is incomplete: {cell}")
        require(not (cell / "CELL_FAILED").exists(), f"cell has a failure marker: {cell}")
        require((cell / "INSERT_VISIBILITY_COMPLETE").is_file(), f"missing visibility marker in {cell}")
        exit_path = cell / "process_exit.txt"
        require(exit_path.is_file() and exit_path.read_text().strip() == "0", f"nonzero or missing process exit in {cell}")

        costs = read_rows(cell / "bench_write_costs.csv")
        require(len(costs) == 1 and costs[0].get("op") == "insert", f"expected exactly one insert aggregate in {cell}")
        cost = costs[0]
        require(integer(cost["configured_batch_records"], "configured batch") == batch_records, f"configured batch drift in {cell}")
        require(integer(cost["ops"], "operations") == expected_ops, f"operation count drift in {cell}")
        aggregate_batches = integer(cost["batches"], "batches")
        require(aggregate_batches >= minimum_batches, f"raw batch count below minimum in {cell}")
        p95 = finite(cost["p95_batch_ms"], "write p95")
        require(p95 < max_p95_ms, f"write p95 gate failed in {cell}")
        wall_ms = finite(cost["wall_ms"], "write wall time")
        require(wall_ms >= 0.0, f"negative write wall time in {cell}")

        samples = read_rows(cell / "bench_write_samples.csv")
        require(len(samples) == aggregate_batches and len(samples) >= minimum_batches, f"raw batch count mismatch in {cell}")
        observed_records = 0
        request_totals = {name: 0 for name in ("gets", "puts", "deletes", "heads", "lists")}
        latencies: list[float] = []
        for batch_index, sample in enumerate(samples):
            require(sample.get("op") == "insert", f"non-insert sample in {cell}")
            require(integer(sample["batch_index"], "batch index") == batch_index, f"batch index drift in {cell}")
            records = integer(sample["batch_records"], "batch records")
            require(0 < records <= batch_records, f"sample exceeds configured batch in {cell}")
            observed_records += records
            latency = finite(sample["batch_latency_ms"], "batch latency")
            require(latency >= 0.0, f"negative batch latency in {cell}")
            latencies.append(latency)
            for name in request_totals:
                value = integer(sample[name], name)
                require(value >= 0, f"negative {name} in {cell}")
                request_totals[name] += value
        require(observed_records == expected_ops, f"record reconciliation failed in {cell}")
        for name, total in request_totals.items():
            require(total == integer(cost[name], f"aggregate {name}"), f"request reconciliation failed for {name} in {cell}")
        raw_p95 = sorted(latencies)[max(math.ceil(len(latencies) * 0.95) - 1, 0)]
        require(abs(raw_p95 - p95) <= 0.001, f"raw write p95 differs from aggregate in {cell}")

        resources = read_rows(cell / "resources.csv")
        require(len(resources) >= 2, f"resource telemetry is incomplete in {cell}")
        elapsed = [finite(row["elapsed_ms"], "resource elapsed time") for row in resources]
        require(elapsed == sorted(elapsed), f"resource timeline is not monotonic in {cell}")
        require(elapsed[0] <= wall_ms and elapsed[-1] >= wall_ms, f"resource timeline does not cover measured wall time in {cell}")
        require(resources[-1].get("child_cpu_seconds", "").strip() != "", f"resource telemetry lacks exact process totals in {cell}")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("root", type=Path)
    parser.add_argument("--manifest", required=True, type=Path)
    parser.add_argument("--expected-dataset-sha")
    parser.add_argument(
        "--terminal-cell",
        metavar="DATASET/rREPETITION/bBATCH",
        help="validate one completed cell without reading an incomplete campaign",
    )
    args = parser.parse_args()
    terminal_cell = None
    if args.terminal_cell is not None:
        match = re.fullmatch(r"([A-Za-z0-9._-]+)/r(\d+)/b(\d+)", args.terminal_cell)
        if match is None:
            parser.error("--terminal-cell must match DATASET/rREPETITION/bBATCH")
        terminal_cell = (match.group(1), int(match.group(2)), int(match.group(3)))
    try:
        validate(
            args.root,
            args.manifest,
            expected_dataset_sha=args.expected_dataset_sha,
            terminal_cell=terminal_cell,
        )
    except (KeyError, OSError, TypeError, json.JSONDecodeError, ValidationError) as error:
        parser.error(str(error))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
