#!/usr/bin/env python3
"""Fail-closed validation and assembly for WAL layout qualification cases."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
from pathlib import Path
from typing import Any

REQUIRED_RESULT_FIELDS = {
    "repetition",
    "policy",
    "element_type",
    "metric",
    "rows",
    "dimensions",
    "batch_rows",
    "batches",
    "wal_objects",
    "wal_bytes",
    "parquet_objects",
    "parquet_bytes",
    "vortex_objects",
    "vortex_bytes",
    "ingest_ms",
    "batch_p95_ms",
    "ingest_bytes_written",
    "ingest_gets",
    "ingest_puts",
    "ingest_heads",
    "ingest_lists",
    "open_ms",
    "first_query_ms",
    "first_query_gets",
    "first_query_backing_bytes",
    "warm_query_p95_ms",
    "warm_query_p99_ms",
    "warm_query_gets_p95",
    "warm_query_backing_bytes_p95",
    "flush_ms",
    "status",
}


def read_single_case(path: Path) -> dict[str, str]:
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None:
            raise ValueError(f"{path}: missing CSV header")
        missing = REQUIRED_RESULT_FIELDS.difference(reader.fieldnames)
        if missing:
            raise ValueError(f"{path}: missing fields {sorted(missing)}")
        rows = list(reader)
    if len(rows) != 1:
        raise ValueError(f"{path}: expected exactly one result row, got {len(rows)}")
    row = rows[0]
    if row["status"] != "complete":
        raise ValueError(f"{path}: status is {row['status']!r}, expected complete")
    return row


def _integer(row: dict[str, str], field: str) -> int:
    try:
        value = int(row[field])
    except (KeyError, ValueError) as error:
        raise ValueError(f"invalid integer {field}={row.get(field)!r}") from error
    if value < 0:
        raise ValueError(f"{field} must be non-negative")
    return value


def _number(row: dict[str, str], field: str) -> float:
    try:
        value = float(row[field])
    except (KeyError, ValueError) as error:
        raise ValueError(f"invalid number {field}={row.get(field)!r}") from error
    if value < 0 or value != value:
        raise ValueError(f"{field} must be finite and non-negative")
    return value


def validate_case(
    row: dict[str, str], arm: str, expected_candidate_format: str
) -> None:
    if arm == "fixed-parquet":
        if row["policy"] != "parquet":
            raise ValueError("fixed-parquet case did not report policy=parquet")
        expected_format = "parquet"
    elif arm == "adaptive-candidate":
        if row["policy"] != "adaptive":
            raise ValueError("adaptive case did not report policy=adaptive")
        expected_format = expected_candidate_format
    else:
        raise ValueError(f"unknown arm {arm!r}")
    if expected_format not in {"parquet", "vortex"}:
        raise ValueError(f"unknown expected candidate format {expected_format!r}")

    objects = _integer(row, "wal_objects")
    bytes_total = _integer(row, "wal_bytes")
    parquet_objects = _integer(row, "parquet_objects")
    parquet_bytes = _integer(row, "parquet_bytes")
    vortex_objects = _integer(row, "vortex_objects")
    vortex_bytes = _integer(row, "vortex_bytes")
    if objects == 0 or bytes_total == 0:
        raise ValueError("qualification case emitted no WAL record objects")
    if objects != parquet_objects + vortex_objects:
        raise ValueError("WAL object count does not equal its format subtotals")
    if bytes_total != parquet_bytes + vortex_bytes:
        raise ValueError("WAL bytes do not equal their format subtotals")
    if expected_format == "parquet":
        if parquet_objects == 0 or vortex_objects != 0 or vortex_bytes != 0:
            raise ValueError("case expected only Parquet WAL record objects")
    else:
        if vortex_objects == 0 or parquet_objects != 0 or parquet_bytes != 0:
            raise ValueError("case expected only Vortex WAL record objects")

    for field in (
        "rows",
        "dimensions",
        "batch_rows",
        "batches",
        "ingest_puts",
    ):
        if _integer(row, field) == 0:
            raise ValueError(f"{field} must be positive")
    for field in (
        "ingest_ms",
        "batch_p95_ms",
        "open_ms",
        "first_query_ms",
        "warm_query_p95_ms",
        "warm_query_p99_ms",
        "flush_ms",
    ):
        if _number(row, field) <= 0:
            raise ValueError(f"{field} must be positive")
    for field in (
        "first_query_gets",
        "first_query_backing_bytes",
        "warm_query_gets_p95",
        "warm_query_backing_bytes_p95",
    ):
        _number(row, field)


def read_protocol_file(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line:
            continue
        key, separator, value = line.partition("=")
        if not separator or not key:
            raise ValueError(f"{path}: invalid protocol line {line!r}")
        if key in values:
            raise ValueError(f"{path}: duplicate protocol field {key!r}")
        values[key] = value
    return values


def frozen_schedule(protocol: dict[str, Any]) -> list[dict[str, str]]:
    arms = [str(protocol["baseline_arm"]), str(protocol["candidate_arm"])]
    rows: list[dict[str, str]] = []
    for repetition in range(1, int(protocol["repetitions"]) + 1):
        repetition_id = f"r{repetition:02d}"
        for workload_index, workload in enumerate(protocol["workloads"]):
            for backend_index, backend in enumerate(protocol["backends"]):
                offset = (repetition - 1 + workload_index + backend_index) % len(arms)
                for position in range(len(arms)):
                    arm = arms[(position + offset) % len(arms)]
                    case_id = "/".join(
                        [repetition_id, str(workload["name"]), str(backend), arm]
                    )
                    rows.append(
                        {
                            "repetition_id": repetition_id,
                            "workload": str(workload["name"]),
                            "backend": str(backend),
                            "arm": arm,
                            "arm_position": str(position),
                            "rows": str(workload["rows"]),
                            "dimensions": str(workload["dimensions"]),
                            "batch_rows": str(workload["batch_rows"]),
                            "element_type": str(workload["element_type"]),
                            "metric": str(workload["metric"]),
                            "dataset": str(workload.get("dataset", "")),
                            "expected_candidate_format": str(
                                workload["expected_candidate_format"]
                            ),
                            "case_id": case_id,
                        }
                    )
    return rows


def read_resource_summary(path: Path) -> dict[str, float | int]:
    with path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    if len(rows) < 2:
        raise ValueError(f"{path}: expected at least two resource samples")
    required = {"elapsed_ms", "cpu_percent", "rss_bytes"}
    if not required.issubset(rows[0]):
        raise ValueError(
            f"{path}: missing resource fields {sorted(required.difference(rows[0]))}"
        )
    peak_rss = 0
    cpu_core_ms = 0.0
    exact_child_cpu_seconds = None
    exact_child_max_rss_bytes = None
    previous_elapsed = None
    for row in rows:
        elapsed = _number(row, "elapsed_ms")
        cpu_percent = _number(row, "cpu_percent")
        rss_bytes = _integer(row, "rss_bytes")
        if previous_elapsed is not None:
            if elapsed < previous_elapsed:
                raise ValueError(f"{path}: resource elapsed_ms is not monotonic")
            cpu_core_ms += (elapsed - previous_elapsed) * cpu_percent / 100.0
        previous_elapsed = elapsed
        peak_rss = max(peak_rss, rss_bytes)
        child_cpu = row.get("child_cpu_seconds", "").strip()
        if child_cpu:
            exact_child_cpu_seconds = _number(row, "child_cpu_seconds")
        child_max_rss = row.get("child_max_rss_bytes", "").strip()
        if child_max_rss:
            exact_child_max_rss_bytes = _integer(row, "child_max_rss_bytes")
    if exact_child_cpu_seconds is not None:
        cpu_core_ms = exact_child_cpu_seconds * 1_000.0
    if exact_child_max_rss_bytes is not None:
        peak_rss = exact_child_max_rss_bytes
    if peak_rss <= 0 or cpu_core_ms <= 0:
        raise ValueError(f"{path}: resource samples did not observe CPU and RSS")
    return {
        "peak_rss_bytes": peak_rss,
        "cpu_core_ms": cpu_core_ms,
    }


def assemble(root: Path, protocol_path: Path, output: Path) -> int:
    protocol = json.loads(protocol_path.read_text(encoding="utf-8"))
    schedule_path = root / "schedule.csv"
    with schedule_path.open(newline="", encoding="utf-8") as handle:
        schedule = list(csv.DictReader(handle))
    expected_schedule = frozen_schedule(protocol)
    if schedule != expected_schedule:
        raise ValueError("schedule does not exactly match the frozen protocol")
    expected_cases = int(protocol["promotion_gates"]["required_complete_cases"])
    if len(schedule) != expected_cases:
        raise ValueError(
            f"schedule has {len(schedule)} cases; protocol requires {expected_cases}"
        )
    case_ids = [row["case_id"] for row in schedule]
    if len(set(case_ids)) != len(case_ids):
        raise ValueError("schedule contains duplicate case ids")

    environment = read_protocol_file(root / "environment.txt")
    source_sha256 = environment.get("source_sha256", "")
    dataset_identity_sha256 = environment.get("dataset_identity_sha256", "")
    for name, digest in (
        ("source_sha256", source_sha256),
        ("dataset_identity_sha256", dataset_identity_sha256),
    ):
        if re.fullmatch(r"[0-9a-f]{64}", digest) is None:
            raise ValueError(f"environment has invalid {name}")
    actual_dataset_identity = hashlib.sha256(
        (root / "dataset-identities.json").read_bytes()
    ).hexdigest()
    if actual_dataset_identity != dataset_identity_sha256:
        raise ValueError("dataset identity manifest checksum mismatch")

    assembled: list[dict[str, Any]] = []
    for scheduled in schedule:
        case_root = root / scheduled["case_id"]
        if not (case_root / "CASE_COMPLETE").is_file():
            raise ValueError(f"{scheduled['case_id']}: missing CASE_COMPLETE")
        case_protocol = read_protocol_file(case_root / "protocol.txt")
        for field, expected in (
            ("source_sha256", source_sha256),
            ("dataset_identity_sha256", dataset_identity_sha256),
            ("queries_per_case", str(protocol["queries_per_case"])),
        ):
            if case_protocol.get(field) != expected:
                raise ValueError(f"{scheduled['case_id']}: protocol {field} mismatch")
        for field in (
            "repetition_id",
            "workload",
            "backend",
            "arm",
            "arm_position",
            "rows",
            "dimensions",
            "batch_rows",
            "element_type",
            "metric",
            "dataset",
            "expected_candidate_format",
        ):
            if case_protocol.get(field) != scheduled[field]:
                raise ValueError(
                    f"{scheduled['case_id']}: protocol {field} mismatch "
                    f"{case_protocol.get(field)!r} != {scheduled[field]!r}"
                )
        result = read_single_case(case_root / "result.csv")
        validate_case(
            result,
            scheduled["arm"],
            scheduled["expected_candidate_format"],
        )
        resources = read_resource_summary(case_root / "resources.csv")
        for scheduled_field, result_field in (
            ("repetition_id", "repetition"),
            ("rows", "rows"),
            ("dimensions", "dimensions"),
            ("batch_rows", "batch_rows"),
            ("element_type", "element_type"),
            ("metric", "metric"),
        ):
            if scheduled[scheduled_field] != result[result_field]:
                raise ValueError(
                    f"{scheduled['case_id']}: result {result_field} mismatch"
                )
        assembled.append({**scheduled, **result, **resources})

    fieldnames = list(assembled[0])
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("x", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(assembled)
    return len(assembled)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--case", type=Path)
    parser.add_argument("--arm")
    parser.add_argument("--expected-candidate-format")
    parser.add_argument("--root", type=Path)
    parser.add_argument("--protocol", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.case is not None:
        if not args.arm or not args.expected_candidate_format:
            parser.error("--case requires --arm and --expected-candidate-format")
        row = read_single_case(args.case)
        validate_case(row, args.arm, args.expected_candidate_format)
        print(f"valid WAL layout case: {args.case}")
        return
    if args.root is None or args.protocol is None or args.output is None:
        parser.error("assembly requires --root, --protocol, and --output")
    count = assemble(args.root, args.protocol, args.output)
    print(f"valid WAL layout qualification: {count} cases")


if __name__ == "__main__":
    main()
