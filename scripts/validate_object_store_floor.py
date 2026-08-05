#!/usr/bin/env python3
"""Fail-closed validation for the terminal object-store latency floor."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
from pathlib import Path


class ValidationError(RuntimeError):
    pass


EXPECTED_MANIFEST_SHA256 = "f03dddd082d825743d4761f32114c6aaefdeb7cfc628cac50a19e7597e3efec4"


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
        result = float(value)
    except ValueError as error:
        raise ValidationError(f"invalid {label}: {value!r}") from error
    require(math.isfinite(result) and result >= 0.0, f"invalid {label}")
    return result


def percentile(values: list[float], quantile: float) -> float:
    ordered = sorted(values)
    return ordered[math.floor((len(ordered) - 1) * quantile + 0.5)]


def validate(root: Path, manifest_path: Path) -> str:
    require((root / "OBJECT_STORE_FLOOR_COMPLETE").is_file(), "campaign is incomplete")
    require(not (root / "OBJECT_STORE_FLOOR_FAILED").exists(), "campaign has a failure marker")
    frozen = manifest_path.read_bytes()
    manifest_sha256 = hashlib.sha256(frozen).hexdigest()
    require(manifest_sha256 == EXPECTED_MANIFEST_SHA256, "unrecognized frozen manifest")
    require((root / "manifest.json").read_bytes() == frozen, "manifest copy differs")
    manifest = json.loads(frozen)
    identity = dict(
        line.split("=", 1)
        for line in (root / "environment.txt").read_text(encoding="utf-8").splitlines()
    )
    require(
        identity.get("manifest_sha256") == manifest_sha256,
        "manifest SHA-256 mismatch",
    )
    require(len(identity.get("source_sha256", "")) == 64, "invalid source SHA-256")
    require(identity.get("architecture") == manifest["architecture"], "architecture mismatch")
    require(identity.get("instance_type") == manifest["instance_type"], "instance mismatch")

    protocol = dict(
        line.split("=", 1)
        for line in (root / "measurement" / "protocol.txt")
        .read_text(encoding="utf-8")
        .splitlines()
    )
    for field in ("samples", "warmups", "payload_bytes", "head_bytes"):
        require(int(protocol[field]) == int(manifest[field]), f"{field} drift")
    require(protocol.get("region") == manifest["region"], "region drift")
    require(protocol.get("uri", "").startswith("s3://"), "production URI is not S3")
    require(protocol.get("conditional_create_rejected") == "true", "create control failed")
    require(protocol.get("stale_update_rejected") == "true", "update control failed")

    samples = read_rows(root / "measurement" / "samples.csv")
    expected_operations = list(manifest["operations"])
    expected_samples = int(manifest["samples"])
    require(
        len(samples) == expected_samples * len(expected_operations),
        "raw sample count mismatch",
    )
    by_operation: dict[str, list[float]] = {name: [] for name in expected_operations}
    coordinates: set[tuple[str, int]] = set()
    for row in samples:
        operation = row["operation"]
        require(operation in by_operation, f"unexpected operation {operation!r}")
        sample = int(row["sample"])
        require(0 <= sample < expected_samples, "sample ordinal out of range")
        require((operation, sample) not in coordinates, "duplicate raw sample")
        coordinates.add((operation, sample))
        by_operation[operation].append(finite(row["latency_ms"], "latency_ms"))

    summaries = read_rows(root / "measurement" / "summary.csv")
    require(len(summaries) == len(expected_operations), "summary row count mismatch")
    summary_by_operation = {row["operation"]: row for row in summaries}
    require(set(summary_by_operation) == set(expected_operations), "summary operations drift")
    for operation, values in by_operation.items():
        row = summary_by_operation[operation]
        require(int(row["samples"]) == expected_samples, "summary sample count mismatch")
        for field, quantile in (("p50_ms", 0.50), ("p95_ms", 0.95), ("p99_ms", 0.99)):
            require(
                abs(finite(row[field], field) - percentile(values, quantile)) <= 0.0015,
                f"{operation} {field} does not reconcile",
            )
        require(abs(finite(row["max_ms"], "max_ms") - max(values)) <= 0.0015, "max drift")

    chained_p95 = finite(
        summary_by_operation["put_then_conditional_update"]["p95_ms"], "chained p95"
    )
    if chained_p95 < float(manifest["proceed_max_chained_p95_ms"]):
        return "proceed"
    if chained_p95 >= float(manifest["unreachable_min_chained_p95_ms"]):
        return "unreachable"
    return "inconclusive"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("root", type=Path)
    args = parser.parse_args()
    print(validate(args.root, args.manifest))


if __name__ == "__main__":
    main()
