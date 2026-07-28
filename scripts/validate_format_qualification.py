#!/usr/bin/env python3
"""Validate one Parquet/Arrow/Vortex physical-format evidence directory."""

from __future__ import annotations

import argparse
import csv
from collections import Counter
from pathlib import Path

REQUIRED_FILES = (
    "build.csv",
    "open.csv",
    "samples.csv",
    "summary.csv",
    "status.csv",
    "resources.csv",
)
RESOURCE_COLUMNS = {
    "elapsed_ms",
    "cpu_percent",
    "rss_bytes",
    "process_read_bytes",
    "process_write_bytes",
    "network_receive_bytes",
    "network_transmit_bytes",
}


def read_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        raise ValueError(f"missing required artifact {path}")
    with path.open(newline="") as handle:
        rows = list(csv.DictReader(handle))
    if not rows:
        raise ValueError(f"{path} has no data rows")
    return rows


def distribution_key(row: dict[str, str]) -> tuple[str, ...]:
    fields = ("format", "workload", "pattern", "selected_rows")
    return tuple(row.get(field, "") for field in fields)


def validate_run(root: Path, expected_samples: int) -> None:
    if expected_samples <= 0:
        raise ValueError("expected_samples must be positive")
    for name in REQUIRED_FILES:
        if not (root / name).exists():
            raise ValueError(f"missing required artifact {root / name}")

    status_rows = read_rows(root / "status.csv")
    for row in status_rows:
        if row.get("status") != "complete":
            raise ValueError(
                f"{root}: format {row.get('format', '?')} is not complete: "
                f"{row.get('blocker', '')}"
            )

    build_rows = read_rows(root / "build.csv")
    for row in build_rows:
        if float(row["elapsed_ms"]) < 0 or int(row["file_bytes"]) <= 0:
            raise ValueError(f"{root}: invalid build timing or file size")

    read_rows(root / "open.csv")
    sample_rows = read_rows(root / "samples.csv")
    counts = Counter(distribution_key(row) for row in sample_rows)
    summary_rows = read_rows(root / "summary.csv")
    for row in summary_rows:
        samples = int(row["samples"])
        if samples != expected_samples:
            raise ValueError(
                f"{root}: summary has {samples} samples, expected {expected_samples}"
            )
        if counts[distribution_key(row)] != expected_samples:
            raise ValueError(f"{root}: raw samples do not reconcile with summary")
        mean = float(row["mean_ms"])
        stddev = float(row["stddev_ms"])
        p50 = float(row["p50_ms"])
        p95 = float(row["p95_ms"])
        p99 = float(row["p99_ms"])
        if min(mean, stddev, p50, p95, p99) < 0 or not (p50 <= p95 <= p99):
            raise ValueError(f"{root}: invalid latency distribution")

    resource_rows = read_rows(root / "resources.csv")
    missing = RESOURCE_COLUMNS.difference(resource_rows[0])
    if missing:
        raise ValueError(f"{root}: resources.csv is missing {sorted(missing)}")
    for row in resource_rows:
        if any(float(row[column]) < 0 for column in RESOURCE_COLUMNS):
            raise ValueError(f"{root}: negative resource counter")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--expected-samples", type=int, default=30)
    args = parser.parse_args()
    validate_run(args.directory, args.expected_samples)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
