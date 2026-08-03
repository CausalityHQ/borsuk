#!/usr/bin/env python3
"""Fail-closed validation for terminal group-commit diagnostic artifacts."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
from pathlib import Path


class ValidationError(RuntimeError):
    pass


def rows(path: Path) -> list[dict[str, str]]:
    if not path.is_file():
        raise ValidationError(f"missing {path}")
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def integer(value: str, name: str) -> int:
    try:
        parsed = int(value)
    except ValueError as error:
        raise ValidationError(f"invalid integer {name}={value!r}") from error
    if parsed < 0:
        raise ValidationError(f"negative {name}")
    return parsed


def finite(value: str, name: str) -> float:
    try:
        parsed = float(value)
    except ValueError as error:
        raise ValidationError(f"invalid float {name}={value!r}") from error
    if not math.isfinite(parsed) or parsed < 0:
        raise ValidationError(f"invalid finite non-negative {name}")
    return parsed


def validate(root: Path, canonical_manifest: Path) -> None:
    if not (root / "GROUP_COMMIT_DIAGNOSTIC_COMPLETE").is_file():
        raise ValidationError("terminal success marker is absent")
    if (root / "GROUP_COMMIT_DIAGNOSTIC_FAILED").exists():
        raise ValidationError("failure marker is present")
    manifest_bytes = canonical_manifest.read_bytes()
    if (root / "manifest.json").read_bytes() != manifest_bytes:
        raise ValidationError("artifact manifest differs from the canonical manifest")
    manifest = json.loads(manifest_bytes)
    expected_manifest_sha = hashlib.sha256(manifest_bytes).hexdigest()
    environment = dict(
        line.split("=", 1)
        for line in (root / "environment.txt").read_text(encoding="utf-8").splitlines()
        if "=" in line
    )
    source_sha = environment.get("source_sha256", "")
    if len(source_sha) != 64 or any(c not in "0123456789abcdef" for c in source_sha):
        raise ValidationError("invalid source_sha256")
    if environment.get("manifest_sha256") != expected_manifest_sha:
        raise ValidationError("manifest_sha256 mismatch")

    summary_rows = rows(root / "cell" / "summary.csv")
    if len(summary_rows) != 1:
        raise ValidationError("summary must contain exactly one row")
    summary = summary_rows[0]
    expected_records = manifest["writers"] * manifest["operations_per_writer"]
    identities = {
        "source_sha256": source_sha,
        "manifest_sha256": expected_manifest_sha,
        "writers": str(manifest["writers"]),
        "operations": str(manifest["operations_per_writer"]),
        "records": str(expected_records),
        "visible_records": str(expected_records),
    }
    for field, expected in identities.items():
        if summary.get(field) != expected:
            raise ValidationError(f"summary {field} mismatch")
    for field in ("groups", "storage_requests", "storage_gets", "storage_puts", "storage_heads"):
        integer(summary[field], field)
    for field in ("mean_group_records", "elapsed_ms", "p50_ms", "p95_ms", "records_per_second", "requests_per_record", "exact_recall"):
        finite(summary[field], field)
    if finite(summary["exact_recall"], "exact_recall") != 1.0:
        raise ValidationError("exact recall gate failed")

    samples = rows(root / "cell" / "samples.csv")
    if len(samples) != expected_records:
        raise ValidationError("raw sample count mismatch")
    expected_pairs = {
        (writer, operation)
        for writer in range(manifest["writers"])
        for operation in range(manifest["operations_per_writer"])
    }
    actual_pairs: set[tuple[int, int]] = set()
    groups: dict[int, tuple[int, int, int, int, int]] = {}
    for sample in samples:
        writer = integer(sample["writer"], "writer")
        operation = integer(sample["operation"], "operation")
        actual_pairs.add((writer, operation))
        expected_id = f"group-w{writer:02}-o{operation:03}"
        if sample["record_id"] != expected_id:
            raise ValidationError("record identity mismatch")
        finite(sample["latency_ms"], "latency_ms")
        sequence = integer(sample["commit_sequence"], "commit_sequence")
        evidence = tuple(
            integer(sample[field], field)
            for field in ("committed_records", "group_requests", "group_gets", "group_puts", "group_heads")
        )
        if evidence[1] != sum(evidence[2:]):
            raise ValidationError("group request components do not reconcile")
        if sequence in groups and groups[sequence] != evidence:
            raise ValidationError("callers disagree about shared group evidence")
        groups[sequence] = evidence
    if actual_pairs != expected_pairs:
        raise ValidationError("writer/operation matrix is incomplete or duplicated")
    if len(groups) != integer(summary["groups"], "groups"):
        raise ValidationError("group count mismatch")
    if sum(group[0] for group in groups.values()) != expected_records:
        raise ValidationError("committed group records do not reconcile")
    totals = [sum(group[index] for group in groups.values()) for index in range(1, 5)]
    summary_totals = [integer(summary[field], field) for field in ("storage_requests", "storage_gets", "storage_puts", "storage_heads")]
    if totals != summary_totals:
        raise ValidationError("raw and summary request totals differ")
    if integer(summary["storage_requests"], "storage_requests") != sum(summary_totals[1:]):
        raise ValidationError("summary request components do not reconcile")
    expected_rpr = summary_totals[0] / expected_records
    if not math.isclose(finite(summary["requests_per_record"], "requests_per_record"), expected_rpr, rel_tol=1e-9):
        raise ValidationError("requests_per_record does not reconcile")
    resources = (root / "resources.txt").read_text(encoding="utf-8")
    if "Exit status: 0" not in resources:
        raise ValidationError("resource telemetry does not report exit status 0")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("root", type=Path)
    parser.add_argument("--manifest", type=Path, default=Path("docs/research/group-commit-diagnostic.json"))
    args = parser.parse_args()
    try:
        validate(args.root, args.manifest)
    except (OSError, KeyError, json.JSONDecodeError, ValidationError) as error:
        raise SystemExit(f"invalid group-commit diagnostic: {error}") from error
    print("group-commit diagnostic artifacts are structurally valid")


if __name__ == "__main__":
    main()
