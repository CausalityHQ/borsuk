#!/usr/bin/env python3
"""Fail-closed validator for one architecture's SIMD datatype campaign."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import re
from collections import defaultdict
from pathlib import Path
from typing import Iterable


HEX_SHA256 = re.compile(r"^[0-9a-f]{64}$")
IDENTITY_FIELDS = {
    "architecture",
    "build",
    "path",
    "element_type",
    "repetition",
    "cache_state",
    "target_cache_coverage_percent",
    "client_concurrency",
    "query_seed",
}
RESOURCE_FIELDS = {"cpu_percent", "rss_bytes"}


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


def finite(value: str, *, field: str, path: Path) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError) as error:
        raise ValidationError(f"{path}: {field} is not numeric: {value!r}") from error
    if not math.isfinite(parsed):
        raise ValidationError(f"{path}: {field} is non-finite: {value!r}")
    return parsed


def integer(value: str, *, field: str, path: Path) -> int:
    try:
        return int(value)
    except (TypeError, ValueError) as error:
        raise ValidationError(f"{path}: {field} is not an integer: {value!r}") from error


def cell_directory(root: Path, row: dict[str, str]) -> Path:
    return (
        root
        / "cells"
        / row["build"]
        / row["path"]
        / f"r{int(row['repetition']):02d}"
        / row["cache_state"]
        / f"c{row['client_concurrency']}"
    )


def expected_schedule_rows(
    manifest: dict, architecture: str
) -> set[tuple[str, ...]]:
    rows: set[tuple[str, ...]] = set()
    for build in manifest["builds"]:
        for path in manifest["paths"]:
            for repetition in range(1, int(manifest["repetitions"]) + 1):
                query_seed = int(manifest["query_cohort"]["master_seed"]) + repetition
                for cache_state in manifest["cache_states"]:
                    if (
                        cache_state["name"] == "memory-preloaded"
                        and not path.get("memory_preloaded_valid", False)
                    ):
                        continue
                    for concurrency in manifest["client_concurrency"]:
                        rows.add(
                            (
                                architecture,
                                build["name"],
                                path["name"],
                                path["kind"],
                                path["element_type"],
                                path["dataset"],
                                str(repetition),
                                cache_state["name"],
                                str(cache_state["coverage_percent"]),
                                str(concurrency),
                                str(query_seed),
                            )
                        )
    return rows


def schedule_identity(row: dict[str, str]) -> tuple[str, ...]:
    return (
        row["architecture"],
        row["build"],
        row["path"],
        row["kind"],
        row["element_type"],
        row["dataset"],
        row["repetition"],
        row["cache_state"],
        row["target_cache_coverage_percent"],
        row["client_concurrency"],
        row["query_seed"],
    )


def load_build_hashes(path: Path) -> dict[tuple[str, str], str]:
    fields, rows = read_csv(path)
    if not {"build", "binary", "sha256"}.issubset(fields):
        raise ValidationError("builds.csv is missing build identity fields")
    hashes: dict[tuple[str, str], str] = {}
    for row in rows:
        digest = row["sha256"]
        if not HEX_SHA256.fullmatch(digest):
            raise ValidationError(f"invalid binary SHA-256: {digest!r}")
        key = (row["build"], row["binary"])
        if key in hashes:
            raise ValidationError(f"duplicate binary build identity: {key}")
        hashes[key] = digest
    for binary in (
        "production_bench",
        "hybrid_retrieval_bench",
        "market_workload_bench",
    ):
        simd = hashes.get(("simd", binary))
        scalar = hashes.get(("scalar-control", binary))
        if simd is None or scalar is None or simd == scalar:
            raise ValidationError(f"missing or equal SIMD/scalar hash for {binary}")
    return hashes


def binary_for_kind(kind: str) -> str:
    if kind in {"primary-dense", "primary-binary"}:
        return "production_bench"
    if kind in {"named-sparse", "text-bm25"}:
        return "hybrid_retrieval_bench"
    if kind == "late-interaction":
        return "market_workload_bench"
    raise ValidationError(f"unknown path kind: {kind!r}")


def check_identity(
    artifact: dict[str, str],
    schedule: dict[str, str],
    *,
    path: Path,
) -> None:
    for field in IDENTITY_FIELDS:
        if artifact.get(field) != schedule[field]:
            raise ValidationError(
                f"{path}: {field} mismatch "
                f"{artifact.get(field)!r} != {schedule[field]!r}"
            )


def validate_results(
    *,
    manifest_path: Path,
    schedule_path: Path,
    root: Path,
    architecture: str,
    source_sha256: str,
    manifest_sha256: str,
) -> dict:
    if not HEX_SHA256.fullmatch(source_sha256):
        raise ValidationError("source SHA-256 is invalid")
    if not HEX_SHA256.fullmatch(manifest_sha256):
        raise ValidationError("manifest SHA-256 is invalid")
    if sha256_file(manifest_path) != manifest_sha256:
        raise ValidationError("manifest SHA-256 does not match manifest bytes")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    architecture_rows = [
        row for row in manifest["architectures"] if row["name"] == architecture
    ]
    if len(architecture_rows) != 1:
        raise ValidationError(f"manifest does not identify architecture {architecture!r}")

    schedule_fields, schedule = read_csv(schedule_path)
    required_schedule_fields = {
        "architecture",
        "build",
        "path",
        "kind",
        "element_type",
        "dataset",
        "repetition",
        "cache_state",
        "target_cache_coverage_percent",
        "client_concurrency",
        "query_seed",
        "index_key",
        "status",
    }
    if not required_schedule_fields.issubset(schedule_fields):
        raise ValidationError("schedule is missing required identity fields")
    actual_schedule = [schedule_identity(row) for row in schedule]
    if len(actual_schedule) != len(set(actual_schedule)):
        raise ValidationError("schedule contains duplicate cells")
    expected_schedule = expected_schedule_rows(manifest, architecture)
    if set(actual_schedule) != expected_schedule:
        missing = len(expected_schedule.difference(actual_schedule))
        extra = len(set(actual_schedule).difference(expected_schedule))
        raise ValidationError(
            f"schedule matrix mismatch: missing={missing} extra={extra}"
        )
    if any(row["status"] != "planned" for row in schedule):
        raise ValidationError("frozen schedule contains a non-planned row")

    build_hashes = load_build_hashes(root / "builds.csv")
    expected_queries = int(manifest["query_cohort"]["queries_per_cell"])
    required_raw_fields = set(manifest["required_raw_query_fields"])
    required_summary_fields = set(manifest["required_summary_fields"])
    cohorts: dict[tuple[str, ...], dict[str, tuple[str, ...]]] = defaultdict(dict)
    recalls: dict[tuple[str, ...], dict[str, float]] = defaultdict(dict)
    validated_queries = 0

    for schedule_row in schedule:
        directory = cell_directory(root, schedule_row)
        if not (directory / "CELL_COMPLETE").is_file():
            raise ValidationError(f"cell has no completion marker: {directory}")
        raw_fields, raw_rows = read_csv(directory / "queries.csv")
        if not required_raw_fields.issubset(raw_fields):
            missing = sorted(required_raw_fields.difference(raw_fields))
            raise ValidationError(f"{directory}/queries.csv missing fields: {missing}")
        if len(raw_rows) != expected_queries:
            raise ValidationError(
                f"{directory}/queries.csv has {len(raw_rows)} rows, "
                f"expected {expected_queries}"
            )
        expected_binary = build_hashes[
            (schedule_row["build"], binary_for_kind(schedule_row["kind"]))
        ]
        query_ids: list[str] = []
        for ordinal, row in enumerate(raw_rows):
            check_identity(row, schedule_row, path=directory / "queries.csv")
            if row["source_sha256"] != source_sha256:
                raise ValidationError(f"{directory}: source SHA-256 mismatch")
            if row["manifest_sha256"] != manifest_sha256:
                raise ValidationError(f"{directory}: manifest SHA-256 mismatch")
            if row["binary_sha256"] != expected_binary:
                raise ValidationError(f"{directory}: binary SHA-256 mismatch")
            if integer(
                row["query_ordinal"],
                field="query_ordinal",
                path=directory / "queries.csv",
            ) != ordinal:
                raise ValidationError(f"{directory}: query ordinal drift")
            if not row["query_id"]:
                raise ValidationError(f"{directory}: empty query id")
            query_ids.append(row["query_id"])
            latency = finite(
                row["latency_ms"], field="latency_ms", path=directory / "queries.csv"
            )
            cpu = finite(
                row["cpu_seconds"],
                field="cpu_seconds",
                path=directory / "queries.csv",
            )
            recall = finite(
                row["recall_or_exact_agreement"],
                field="recall_or_exact_agreement",
                path=directory / "queries.csv",
            )
            if latency < 0 or cpu < 0 or not 0.0 <= recall <= 1.0:
                raise ValidationError(f"{directory}: invalid timing or correctness value")
            observed_coverage = finite(
                row["observed_cache_coverage_percent"],
                field="observed_cache_coverage_percent",
                path=directory / "queries.csv",
            )
            target_coverage = float(schedule_row["target_cache_coverage_percent"])
            if abs(observed_coverage - target_coverage) > 5.0:
                raise ValidationError(
                    f"{directory}: cache coverage drift "
                    f"{observed_coverage} versus {target_coverage}"
                )
            for field in (
                "rss_bytes",
                "logical_bytes",
                "disk_cache_bytes",
                "backing_bytes",
                "disk_cache_requests",
                "backing_requests",
            ):
                if finite(
                    row[field], field=field, path=directory / "queries.csv"
                ) < 0:
                    raise ValidationError(f"{directory}: {field} is negative")

        summary_fields, summary_rows = read_csv(directory / "summary.csv")
        if not required_summary_fields.issubset(summary_fields):
            missing = sorted(required_summary_fields.difference(summary_fields))
            raise ValidationError(f"{directory}/summary.csv missing fields: {missing}")
        if len(summary_rows) != 1:
            raise ValidationError(f"{directory}/summary.csv must contain one row")
        summary = summary_rows[0]
        check_identity(summary, schedule_row, path=directory / "summary.csv")
        if integer(
            summary["samples"], field="samples", path=directory / "summary.csv"
        ) != expected_queries:
            raise ValidationError(f"{directory}: summary sample count mismatch")
        for field in required_summary_fields.difference({"samples"}):
            if finite(
                summary[field], field=field, path=directory / "summary.csv"
            ) < 0:
                raise ValidationError(f"{directory}: negative summary field {field}")
        summary_recall = finite(
            summary["recall_or_exact_agreement"],
            field="recall_or_exact_agreement",
            path=directory / "summary.csv",
        )
        if not 0.0 <= summary_recall <= 1.0:
            raise ValidationError(f"{directory}: summary correctness is out of range")

        resource_fields, resource_rows = read_csv(directory / "resources.csv")
        if not RESOURCE_FIELDS.issubset(resource_fields) or not resource_rows:
            raise ValidationError(f"{directory}: resource telemetry is incomplete")
        if max(
            finite(row["rss_bytes"], field="rss_bytes", path=directory / "resources.csv")
            for row in resource_rows
        ) <= 0:
            raise ValidationError(f"{directory}: resource telemetry observed no RSS")
        cpu_values = [
            finite(
                row["cpu_percent"],
                field="cpu_percent",
                path=directory / "resources.csv",
            )
            for row in resource_rows
        ]
        if any(value < 0 for value in cpu_values) or max(cpu_values) <= 0:
            raise ValidationError(f"{directory}: invalid or empty CPU telemetry")

        cohort_key = (
            schedule_row["architecture"],
            schedule_row["path"],
            schedule_row["repetition"],
            schedule_row["cache_state"],
            schedule_row["client_concurrency"],
            schedule_row["query_seed"],
        )
        cohorts[cohort_key][schedule_row["build"]] = tuple(query_ids)
        recalls[cohort_key][schedule_row["build"]] = summary_recall
        validated_queries += len(raw_rows)

    for key, builds in cohorts.items():
        if set(builds) != {"simd", "scalar-control"}:
            raise ValidationError(f"cohort is missing a build: {key}")
        if builds["simd"] != builds["scalar-control"]:
            raise ValidationError(f"query cohort membership or order drift: {key}")
        if recalls[key]["simd"] + 1.0e-9 < recalls[key]["scalar-control"]:
            raise ValidationError(f"SIMD correctness or recall regressed: {key}")

    decision = {
        "schema_version": 1,
        "status": "validated",
        "architecture": architecture,
        "source_sha256": source_sha256,
        "manifest_sha256": manifest_sha256,
        "schedule_cells": len(schedule),
        "raw_query_rows": validated_queries,
        "claim_decision": "architecture-complete-no-cross-architecture-claim",
    }
    (root / "simd-validation.json").write_text(
        json.dumps(decision, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return decision


def parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--schedule", type=Path, required=True)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--architecture", required=True)
    parser.add_argument("--source-sha256", required=True)
    parser.add_argument("--manifest-sha256", required=True)
    return parser.parse_args(argv)


def main(argv: Iterable[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        decision = validate_results(
            manifest_path=args.manifest,
            schedule_path=args.schedule,
            root=args.root,
            architecture=args.architecture,
            source_sha256=args.source_sha256,
            manifest_sha256=args.manifest_sha256,
        )
    except (OSError, KeyError, TypeError, json.JSONDecodeError, ValidationError) as error:
        print(f"SIMD validation failed: {error}")
        return 1
    print(json.dumps(decision, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
