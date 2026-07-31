#!/usr/bin/env python3
"""Fail-closed validator for the paired logical-cell write-routing campaign."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import re
from collections import defaultdict
from pathlib import Path

HEX_SHA256 = re.compile(r"^[0-9a-f]{64}$")
MODES = {"flat", "quantizer"}
CORRECTNESS_GATES = {"duplicate_race", "prepare_failure", "crash_recovery"}


class ValidationError(ValueError):
    """The result tree is incomplete or violates its frozen protocol."""


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def read_csv(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    if not path.is_file() or path.stat().st_size == 0:
        raise ValidationError(f"missing or empty CSV: {path}")
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None:
            raise ValidationError(f"CSV has no header: {path}")
        return list(reader.fieldnames), list(reader)


def integer(value: str, field: str, path: Path) -> int:
    try:
        return int(value)
    except (TypeError, ValueError) as error:
        raise ValidationError(f"{path}: {field} is not an integer: {value!r}") from error


def finite(value: str, field: str, path: Path) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError) as error:
        raise ValidationError(f"{path}: {field} is not numeric: {value!r}") from error
    if not math.isfinite(parsed):
        raise ValidationError(f"{path}: {field} is non-finite: {value!r}")
    return parsed


def require_fields(actual: list[str], required: set[str], path: Path) -> None:
    missing = required.difference(actual)
    if missing:
        raise ValidationError(f"{path}: missing fields: {sorted(missing)}")


def key(row: dict[str, str], path: Path) -> tuple[int, int, int, str]:
    return (
        integer(row["cell_count"], "cell_count", path),
        integer(row["writers"], "writers", path),
        integer(row["repetition"], "repetition", path),
        row["routing_mode"],
    )


def expected_keys(manifest: dict) -> set[tuple[int, int, int, str]]:
    return {
        (cells, writers, repetition, mode)
        for cells in manifest["cell_counts"]
        for writers in manifest["writers"]
        for repetition in range(1, int(manifest["repetitions"]) + 1)
        for mode in manifest["routing_modes"]
    }


def validate_results(manifest_path: Path, root: Path) -> None:
    # This ordering is deliberate: never parse an in-progress campaign CSV.
    if (root / "LOGICAL_CELL_ROUTING_FAILED").exists():
        raise ValidationError("campaign contains failure marker")
    if not (root / "LOGICAL_CELL_ROUTING_COMPLETE").is_file():
        raise ValidationError("campaign completion marker is absent")

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    protocol_kind = manifest.get("protocol_kind", "production")
    if protocol_kind not in {"production", "local-smoke"}:
        raise ValidationError("manifest protocol_kind is unsupported")
    if protocol_kind == "production" and manifest.get("cell_counts") != [2000, 16000]:
        raise ValidationError("production manifest must freeze cell_counts to [2000, 16000]")
    if set(manifest.get("routing_modes", [])) != MODES:
        raise ValidationError("manifest must freeze flat and quantizer modes")
    if protocol_kind == "production" and manifest.get("writers") != [1, 8, 32]:
        raise ValidationError("production manifest must freeze writers to [1, 8, 32]")
    repetitions = int(manifest.get("repetitions", 0))
    operations_per_writer = int(manifest.get("operations_per_writer", 0))
    minimum_repetitions = 2 if protocol_kind == "production" else 1
    if repetitions < minimum_repetitions or operations_per_writer < 1:
        raise ValidationError("manifest has too few repetitions or operations")
    manifest_sha = sha256_file(manifest_path)

    summary_path = root / "summary.csv"
    summary_fields, summaries = read_csv(summary_path)
    require_fields(
        summary_fields,
        {
            "source_sha256", "manifest_sha256", "architecture", "instance_type",
            "routing_mode", "cell_count", "writers", "repetition", "cohort_sha256",
            "operations", "elapsed_ms", "cpu_seconds", "p50_ms", "p95_ms",
            "throughput_ops_per_second", "storage_requests", "distinct_cells",
        },
        summary_path,
    )
    expected = expected_keys(manifest)
    observed: dict[tuple[int, int, int, str], dict[str, str]] = {}
    source_hashes = set()
    for row in summaries:
        identity = key(row, summary_path)
        if identity in observed:
            raise ValidationError(f"duplicate summary cell: {identity}")
        if identity not in expected:
            raise ValidationError(f"unexpected summary cell: {identity}")
        if row["routing_mode"] not in MODES:
            raise ValidationError(f"unknown routing mode: {row['routing_mode']!r}")
        if row["manifest_sha256"] != manifest_sha:
            raise ValidationError("summary manifest SHA-256 mismatch")
        if not HEX_SHA256.fullmatch(row["source_sha256"]):
            raise ValidationError("invalid source SHA-256")
        if not HEX_SHA256.fullmatch(row["cohort_sha256"]):
            raise ValidationError("invalid cohort SHA-256")
        source_hashes.add(row["source_sha256"])
        expected_operations = identity[1] * operations_per_writer
        if integer(row["operations"], "operations", summary_path) != expected_operations:
            raise ValidationError(f"{identity}: operation count mismatch")
        for field in (
            "elapsed_ms", "cpu_seconds", "p50_ms", "p95_ms",
            "throughput_ops_per_second",
        ):
            if finite(row[field], field, summary_path) < 0:
                raise ValidationError(f"{summary_path}: {field} must be non-negative")
        for field in ("storage_requests", "distinct_cells"):
            if integer(row[field], field, summary_path) < 0:
                raise ValidationError(f"{summary_path}: {field} must be non-negative")
        observed[identity] = row
    if set(observed) != expected:
        raise ValidationError(f"summary matrix mismatch: missing {sorted(expected - set(observed))}")
    if len(source_hashes) != 1:
        raise ValidationError("summary rows do not share one source SHA-256")

    for cells, writers, repetition, mode in expected:
        resource_path = (
            root
            / "cells"
            / f"c{cells}"
            / f"r{repetition:02d}"
            / f"w{writers}"
            / f"{mode}.resources.txt"
        )
        if not resource_path.is_file() or resource_path.stat().st_size == 0:
            raise ValidationError(f"missing resource telemetry: {resource_path}")
        telemetry = resource_path.read_text(encoding="utf-8")
        for label in (
            "User time (seconds)",
            "System time (seconds)",
            "Maximum resident set size (kbytes)",
        ):
            match = re.search(
                rf"^\s*{re.escape(label)}:\s*(\S+)\s*$", telemetry, re.MULTILINE
            )
            if match is None or finite(match.group(1), label, resource_path) < 0:
                raise ValidationError(f"invalid resource telemetry: {resource_path} {label}")

    for cells in manifest["cell_counts"]:
        for writers in manifest["writers"]:
            for repetition in range(1, repetitions + 1):
                flat = observed[(cells, writers, repetition, "flat")]
                quantizer = observed[(cells, writers, repetition, "quantizer")]
                if flat["cohort_sha256"] != quantizer["cohort_sha256"]:
                    raise ValidationError(
                        f"paired cohort mismatch for {(cells, writers, repetition)}"
                    )
                for field in (
                    "source_sha256",
                    "manifest_sha256",
                    "architecture",
                    "instance_type",
                    "operations",
                ):
                    if flat[field] != quantizer[field]:
                        raise ValidationError(
                            f"paired {field} mismatch for {(cells, writers, repetition)}"
                        )

    sample_path = root / "samples.csv"
    sample_fields, samples = read_csv(sample_path)
    require_fields(
        sample_fields,
        {
            "source_sha256", "manifest_sha256", "architecture", "instance_type",
            "routing_mode", "cell_count", "writers", "repetition", "cohort_sha256",
            "writer", "operation", "record_id", "latency_ms", "selected_cell",
        },
        sample_path,
    )
    counts: defaultdict[tuple[int, int, int, str], int] = defaultdict(int)
    identities: defaultdict[tuple[int, int, int, str], set[tuple[int, int, str]]] = defaultdict(set)
    for row in samples:
        identity = key(row, sample_path)
        summary = observed.get(identity)
        if summary is None:
            raise ValidationError(f"sample references unexpected cell: {identity}")
        for field in ("source_sha256", "manifest_sha256", "architecture", "instance_type", "cohort_sha256"):
            if row[field] != summary[field]:
                raise ValidationError(f"{sample_path}: {field} differs from summary")
        writer = integer(row["writer"], "writer", sample_path)
        operation = integer(row["operation"], "operation", sample_path)
        if not 0 <= writer < identity[1] or not 0 <= operation < operations_per_writer:
            raise ValidationError(f"sample ordinal outside frozen cohort: {identity}")
        if finite(row["latency_ms"], "latency_ms", sample_path) < 0:
            raise ValidationError("sample latency must be non-negative")
        integer(row["selected_cell"], "selected_cell", sample_path)
        sample_identity = (writer, operation, row["record_id"])
        if sample_identity in identities[identity]:
            raise ValidationError(f"duplicate raw sample: {identity} {sample_identity}")
        identities[identity].add(sample_identity)
        counts[identity] += 1
    for identity in expected:
        expected_count = identity[1] * operations_per_writer
        if counts[identity] != expected_count:
            raise ValidationError(
                f"raw sample count mismatch for {identity}: {counts[identity]} != {expected_count}"
            )
    for cells in manifest["cell_counts"]:
        for writers in manifest["writers"]:
            for repetition in range(1, repetitions + 1):
                flat_ids = identities[(cells, writers, repetition, "flat")]
                quantizer_ids = identities[(cells, writers, repetition, "quantizer")]
                if flat_ids != quantizer_ids:
                    raise ValidationError(
                        f"paired raw cohort mismatch for {(cells, writers, repetition)}"
                    )

    correctness_path = root / "correctness.csv"
    fields, rows = read_csv(correctness_path)
    require_fields(fields, {"gate", "status"}, correctness_path)
    passed = {row["gate"] for row in rows if row["status"] == "pass"}
    if (
        len(rows) != len(CORRECTNESS_GATES)
        or passed != CORRECTNESS_GATES
        or any(row["status"] != "pass" for row in rows)
    ):
        raise ValidationError("correctness gates are missing, duplicated, or failed")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", required=True, type=Path)
    parser.add_argument("--root", required=True, type=Path)
    args = parser.parse_args()
    try:
        validate_results(args.manifest, args.root)
    except (OSError, json.JSONDecodeError, ValidationError) as error:
        parser.error(str(error))
    print("logical-cell routing campaign is complete and valid")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
