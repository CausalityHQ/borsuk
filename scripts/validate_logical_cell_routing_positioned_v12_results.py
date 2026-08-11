#!/usr/bin/env python3
"""Fail-closed validator for positioned V12 logical-cell routing evidence."""

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
INSTANCE_ID = re.compile(r"^i-[0-9a-f]{17}$")
MODES = {"flat", "quantizer"}


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


def require_fields(actual: list[str], required: set[str], path: Path) -> None:
    missing = required.difference(actual)
    if missing:
        raise ValidationError(f"{path}: missing fields: {sorted(missing)}")


def integer(value: object, field: str, path: Path) -> int:
    try:
        return int(value)
    except (TypeError, ValueError) as error:
        raise ValidationError(
            f"{path}: {field} is not an integer: {value!r}"
        ) from error


def finite(value: object, field: str, path: Path) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError) as error:
        raise ValidationError(f"{path}: {field} is not numeric: {value!r}") from error
    if not math.isfinite(parsed):
        raise ValidationError(f"{path}: {field} is non-finite: {value!r}")
    return parsed


def environment(path: Path) -> dict[str, str]:
    if not path.is_file():
        raise ValidationError(f"missing environment identity: {path}")
    values: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line or "=" not in line:
            raise ValidationError(f"invalid environment identity line: {line!r}")
        name, value = line.split("=", 1)
        if not name or name in values:
            raise ValidationError(f"duplicate or empty environment identity: {name!r}")
        values[name] = value
    return values


def identity(row: dict[str, str], path: Path) -> tuple[int, int, int, str]:
    return (
        integer(row["cell_count"], "cell_count", path),
        integer(row["writers"], "writers", path),
        integer(row["repetition"], "repetition", path),
        row["routing_mode"],
    )


def expected_keys(manifest: dict[str, object]) -> set[tuple[int, int, int, str]]:
    return {
        (cells, writers, repetition, mode)
        for cells in manifest["cell_counts"]
        for writers in manifest["writers"]
        for repetition in range(1, int(manifest["repetitions"]) + 1)
        for mode in manifest["routing_modes"]
    }


def validate_manifest(manifest: dict[str, object]) -> None:
    protocol_kind = manifest.get("protocol_kind")
    if protocol_kind not in {"production", "architecture-qualification", "local-smoke"}:
        raise ValidationError("manifest protocol_kind is not positioned qualification")
    if manifest.get("mutation_protocol") != "positioned-v12":
        raise ValidationError("manifest mutation protocol must be positioned-v12")
    if manifest.get("dimensions") != 768:
        raise ValidationError("production manifest dimensions must be 768")
    if manifest.get("metric") != "cosine":
        raise ValidationError("production manifest metric must be cosine")
    if protocol_kind != "local-smoke" and manifest.get("purchase_option") != "spot":
        raise ValidationError("production execution must use EC2 Spot")
    if protocol_kind != "local-smoke" and manifest.get("cell_counts") != [
        2_000,
        16_000,
    ]:
        raise ValidationError(
            "production manifest must freeze cell_counts to [2000, 16000]"
        )
    if protocol_kind != "local-smoke" and manifest.get("writers") != [1, 8, 32]:
        raise ValidationError("production manifest must freeze writers to [1, 8, 32]")
    if protocol_kind == "local-smoke" and (
        manifest.get("cell_counts") != [2_000] or manifest.get("writers") != [1]
    ):
        raise ValidationError("local smoke must use one realistic 2000-cell writer arm")
    if set(manifest.get("routing_modes", [])) != MODES:
        raise ValidationError("manifest must freeze flat and quantizer modes")
    for field in (
        "repetitions",
        "operations_per_writer",
        "cell_timeout_seconds",
        "clone_timeout_seconds",
        "campaign_timeout_seconds",
        "setup_timeout_seconds",
    ):
        if field not in manifest:
            label = field.replace("_", " ")
            raise ValidationError(f"manifest {label} is absent")
        if integer(manifest.get(field), field, Path("manifest")) <= 0:
            raise ValidationError(f"manifest {field} must be positive")
    timeout = integer(
        manifest["cell_timeout_seconds"], "cell_timeout_seconds", Path("manifest")
    )
    if not 60 <= timeout <= 14_400:
        raise ValidationError("manifest cell timeout must be from 60 to 14400 seconds")
    clone_timeout = integer(
        manifest["clone_timeout_seconds"], "clone_timeout_seconds", Path("manifest")
    )
    if not 60 <= clone_timeout <= 900:
        raise ValidationError("manifest clone timeout must be from 60 to 900 seconds")
    campaign_timeout = integer(
        manifest["campaign_timeout_seconds"],
        "campaign_timeout_seconds",
        Path("manifest"),
    )
    if not 300 <= campaign_timeout <= 21_600:
        raise ValidationError(
            "manifest campaign timeout must be from 300 to 21600 seconds"
        )
    setup_timeout = integer(
        manifest["setup_timeout_seconds"], "setup_timeout_seconds", Path("manifest")
    )
    worst_case_seconds = setup_timeout + len(expected_keys(manifest)) * (
        clone_timeout + timeout
    )
    if worst_case_seconds > campaign_timeout:
        raise ValidationError(
            "manifest worst-case matrix duration exceeds campaign timeout: "
            f"{worst_case_seconds} > {campaign_timeout}"
        )
    hard_cap = finite(
        manifest.get("max_write_p95_ms"), "max_write_p95_ms", Path("manifest")
    )
    if hard_cap <= 0:
        raise ValidationError("manifest write p95 hard cap must be positive")
    gates = manifest.get("correctness_gates")
    if not isinstance(gates, list) or not gates or len(gates) != len(set(gates)):
        raise ValidationError("manifest correctness gates must be unique and nonempty")


def validate_resource_csv(path: Path) -> None:
    if not path.is_file() or path.stat().st_size == 0:
        raise ValidationError(f"missing resource telemetry: {path}")
    fields, rows = read_csv(path)
    required = {
        "elapsed_ms",
        "cpu_percent",
        "rss_bytes",
        "vms_bytes",
        "process_read_bytes",
        "process_write_bytes",
        "cache_disk_bytes",
        "scratch_disk_bytes",
        "network_receive_bytes",
        "network_transmit_bytes",
        "child_cpu_seconds",
        "child_max_rss_bytes",
    }
    require_fields(fields, required, path)
    if not rows:
        raise ValidationError(f"missing resource telemetry samples: {path}")
    for field in required - {"child_cpu_seconds", "child_max_rss_bytes"}:
        if finite(rows[-1][field], field, path) < 0:
            raise ValidationError(f"negative resource telemetry: {path} {field}")
    for field in ("child_cpu_seconds", "child_max_rss_bytes"):
        if finite(rows[-1][field], field, path) < 0:
            raise ValidationError(
                f"invalid terminal resource telemetry: {path} {field}"
            )


def validate_results(manifest_path: Path, root: Path) -> None:
    # This ordering is a safety contract: never parse an in-progress campaign CSV.
    if (root / "LOGICAL_CELL_ROUTING_FAILED").exists():
        raise ValidationError("campaign contains failure marker")
    if not (root / "LOGICAL_CELL_ROUTING_COMPLETE").is_file():
        raise ValidationError("campaign completion marker is absent")

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    validate_manifest(manifest)
    copied_manifest = root / "manifest.json"
    if (
        not copied_manifest.is_file()
        or copied_manifest.read_bytes() != manifest_path.read_bytes()
    ):
        raise ValidationError("copied manifest differs from the frozen input")
    manifest_sha = sha256_file(manifest_path)
    env = environment(root / "environment.txt")
    expected_environment = {
        "manifest_sha256": manifest_sha,
        "architecture": str(manifest["architecture"]),
        "instance_type": str(manifest["instance_type"]),
        "purchase_option": str(manifest["purchase_option"]),
        "mutation_protocol": "positioned-v12",
        "dimensions": "768",
        "metric": "cosine",
    }
    for field, expected_value in expected_environment.items():
        if env.get(field) != expected_value:
            raise ValidationError(f"environment {field} mismatch")
    if not HEX_SHA256.fullmatch(env.get("source_sha256", "")):
        raise ValidationError("environment source SHA-256 is invalid")
    if manifest["protocol_kind"] != "local-smoke" and not INSTANCE_ID.fullmatch(
        env.get("instance_id", "")
    ):
        raise ValidationError("environment EC2 instance identity is invalid")
    if manifest["protocol_kind"] == "local-smoke" and env.get("instance_id") != "local":
        raise ValidationError("local smoke instance identity is invalid")
    if not env.get("availability_zone", ""):
        raise ValidationError("environment availability zone is absent")

    summary_path = root / "summary.csv"
    summary_fields, summaries = read_csv(summary_path)
    identity_fields = {
        "source_sha256",
        "manifest_sha256",
        "architecture",
        "instance_type",
        "instance_id",
        "availability_zone",
        "purchase_option",
        "mutation_protocol",
        "dimensions",
        "metric",
        "routing_mode",
        "routing_path",
        "cell_count",
        "writers",
        "repetition",
        "cohort_blake3",
    }
    require_fields(
        summary_fields,
        identity_fields
        | {
            "operations",
            "elapsed_ms",
            "cpu_seconds",
            "p50_ms",
            "p95_ms",
            "throughput_ops_per_second",
            "storage_requests",
            "distinct_cells",
        },
        summary_path,
    )
    expected = expected_keys(manifest)
    observed: dict[tuple[int, int, int, str], dict[str, str]] = {}
    operations_per_writer = integer(
        manifest["operations_per_writer"], "operations_per_writer", manifest_path
    )
    hard_cap = finite(manifest["max_write_p95_ms"], "max_write_p95_ms", manifest_path)
    for row in summaries:
        key = identity(row, summary_path)
        if key in observed or key not in expected:
            raise ValidationError(f"duplicate or unexpected summary cell: {key}")
        for field in (
            "source_sha256",
            "manifest_sha256",
            "architecture",
            "instance_type",
            "instance_id",
            "availability_zone",
            "purchase_option",
            "mutation_protocol",
            "dimensions",
            "metric",
        ):
            if row[field] != env[field]:
                raise ValidationError(f"summary {field} differs from environment")
        if not HEX_SHA256.fullmatch(row["cohort_blake3"]):
            raise ValidationError("invalid cohort BLAKE3")
        if row["routing_path"] != row["routing_mode"]:
            raise ValidationError(f"{key}: routing path differs from the labeled arm")
        if (
            integer(row["operations"], "operations", summary_path)
            != key[1] * operations_per_writer
        ):
            raise ValidationError(f"{key}: operation count mismatch")
        for field in (
            "elapsed_ms",
            "cpu_seconds",
            "p50_ms",
            "p95_ms",
            "throughput_ops_per_second",
        ):
            if finite(row[field], field, summary_path) < 0:
                raise ValidationError(f"{summary_path}: {field} must be non-negative")
        if finite(row["p95_ms"], "p95_ms", summary_path) > hard_cap:
            raise ValidationError(f"{key}: write p95 hard cap exceeded")
        for field in ("storage_requests", "distinct_cells"):
            if integer(row[field], field, summary_path) < 0:
                raise ValidationError(f"{summary_path}: {field} must be non-negative")
        if integer(row["distinct_cells"], "distinct_cells", summary_path) <= 1:
            raise ValidationError(f"{key}: routing catalog collapsed to one cell")
        observed[key] = row
    if set(observed) != expected:
        raise ValidationError(
            f"summary matrix mismatch: missing {sorted(expected - set(observed))}"
        )

    for cells, writers, repetition, mode in expected:
        cell = (
            root / "cells" / f"c{cells}" / f"r{repetition:02d}" / f"w{writers}" / mode
        )
        if not (cell / "CELL_COMPLETE").is_file():
            raise ValidationError(f"nonterminal cell: {cell}")
        if (cell / "process_exit.txt").read_text(encoding="utf-8").strip() != "0":
            raise ValidationError(f"cell process did not exit successfully: {cell}")
        validate_resource_csv(cell / "resources.csv")
        for name in ("storage-access.csv",):
            path = cell / name
            if not path.is_file() or path.stat().st_size == 0:
                raise ValidationError(f"missing raw cell artifact: {path}")
        for name in ("benchmark.stdout.log", "benchmark.stderr.log"):
            path = cell / name
            if not path.is_file():
                raise ValidationError(f"missing raw cell log: {path}")

    for cells in manifest["cell_counts"]:
        for writers in manifest["writers"]:
            for repetition in range(1, int(manifest["repetitions"]) + 1):
                flat = observed[(cells, writers, repetition, "flat")]
                quantizer = observed[(cells, writers, repetition, "quantizer")]
                if flat["cohort_blake3"] != quantizer["cohort_blake3"]:
                    raise ValidationError(
                        f"paired cohort mismatch for {(cells, writers, repetition)}"
                    )
                for field in identity_fields - {"routing_mode", "routing_path"}:
                    if flat[field] != quantizer[field]:
                        raise ValidationError(
                            f"paired {field} mismatch for {(cells, writers, repetition)}"
                        )

    sample_path = root / "samples.csv"
    sample_fields, samples = read_csv(sample_path)
    require_fields(
        sample_fields,
        identity_fields
        | {
            "writer",
            "operation",
            "record_id",
            "vector_blake3",
            "latency_ms",
            "selected_cell",
            "storage_requests",
        },
        sample_path,
    )
    counts: defaultdict[tuple[int, int, int, str], int] = defaultdict(int)
    identities: defaultdict[
        tuple[int, int, int, str], set[tuple[int, int, str, str]]
    ] = defaultdict(set)
    selected_cells: defaultdict[tuple[int, int, int, str], set[int]] = defaultdict(set)
    for row in samples:
        key = identity(row, sample_path)
        summary = observed.get(key)
        if summary is None:
            raise ValidationError(f"sample references unexpected cell: {key}")
        for field in identity_fields:
            if row[field] != summary[field]:
                raise ValidationError(f"sample {field} differs from summary")
        writer = integer(row["writer"], "writer", sample_path)
        operation = integer(row["operation"], "operation", sample_path)
        if not 0 <= writer < key[1] or not 0 <= operation < operations_per_writer:
            raise ValidationError(f"sample ordinal outside frozen cohort: {key}")
        if finite(row["latency_ms"], "latency_ms", sample_path) < 0:
            raise ValidationError("sample latency must be non-negative")
        selected = integer(row["selected_cell"], "selected_cell", sample_path)
        if not 0 <= selected < key[0]:
            raise ValidationError("sample selected cell is outside the catalog")
        if integer(row["storage_requests"], "storage_requests", sample_path) < 0:
            raise ValidationError("sample storage requests must be non-negative")
        if not HEX_SHA256.fullmatch(row["vector_blake3"]):
            raise ValidationError("invalid sample vector BLAKE3")
        sample_id = (writer, operation, row["record_id"], row["vector_blake3"])
        if sample_id in identities[key]:
            raise ValidationError(f"duplicate raw sample: {key} {sample_id}")
        identities[key].add(sample_id)
        selected_cells[key].add(selected)
        counts[key] += 1
    for key in expected:
        if counts[key] != key[1] * operations_per_writer:
            raise ValidationError(f"raw sample count mismatch for {key}")
        if len(selected_cells[key]) != integer(
            observed[key]["distinct_cells"], "distinct_cells", summary_path
        ):
            raise ValidationError(
                f"summary distinct cell count is not backed by samples: {key}"
            )
    for cells in manifest["cell_counts"]:
        for writers in manifest["writers"]:
            for repetition in range(1, int(manifest["repetitions"]) + 1):
                if (
                    identities[(cells, writers, repetition, "flat")]
                    != identities[(cells, writers, repetition, "quantizer")]
                ):
                    raise ValidationError(
                        f"paired raw cohort mismatch for {(cells, writers, repetition)}"
                    )

    correctness_path = root / "correctness.csv"
    fields, rows = read_csv(correctness_path)
    require_fields(fields, {"gate", "status"}, correctness_path)
    expected_gates = set(manifest["correctness_gates"])
    passed = {row["gate"] for row in rows if row["status"] == "pass"}
    if (
        len(rows) != len(expected_gates)
        or passed != expected_gates
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
    print("positioned V12 logical-cell routing campaign is complete and valid")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
