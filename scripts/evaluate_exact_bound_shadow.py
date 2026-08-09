#!/usr/bin/env python3
"""Evaluate the preregistered exact-rerank shadow decision after validation."""

import argparse
import csv
import json
import math
from pathlib import Path
from typing import Any

try:
    from .validate_group_commit_scalability import validate
except ImportError:
    from validate_group_commit_scalability import validate


def percentile(values: list[float], quantile: float) -> float:
    if not values:
        raise ValueError("cannot evaluate an empty exact-bound shadow")
    ordered = sorted(values)
    index = math.floor((len(ordered) - 1) * quantile + 0.5)
    return ordered[index]


def evaluate(rows: list[dict[str, str]], gate: dict[str, Any]) -> dict[str, Any]:
    if not rows:
        raise ValueError("cannot evaluate an empty exact-bound shadow")

    survivors = [int(row["global_exact_bound_survivors"]) for row in rows]
    containment_failures = sum(
        int(row["global_exact_bound_containment_failures"]) for row in rows
    )
    baseline_reads = sum(int(row["global_exact_bound_baseline_reads"]) for row in rows)
    predicted_reads = sum(
        int(row["global_exact_bound_predicted_reads"]) for row in rows
    )
    baseline_bytes = sum(int(row["global_exact_bound_baseline_bytes"]) for row in rows)
    predicted_bytes = sum(
        int(row["global_exact_bound_predicted_bytes"]) for row in rows
    )
    if baseline_reads <= 0 or baseline_bytes <= 0:
        raise ValueError("exact-bound shadow baseline must be positive")

    survivor_p95 = int(percentile([float(value) for value in survivors], 0.95))
    cpu_p95_us = int(
        percentile(
            [float(row["global_exact_bound_cpu_us"]) for row in rows], 0.95
        )
    )
    read_p95_ms = percentile([float(row["latency_ms"]) for row in rows], 0.95)
    read_reduction = 1.0 - predicted_reads / baseline_reads
    byte_reduction = 1.0 - predicted_bytes / baseline_bytes
    cpu_limit_us = max(
        float(gate["max_cpu_p95_us"]),
        read_p95_ms
        * 1_000.0
        * float(gate["max_cpu_fraction_of_read_p95"]),
    )

    failures: list[str] = []
    if bool(gate["require_zero_containment_failures"]) and containment_failures != 0:
        failures.append("containment")
    if survivor_p95 > int(gate["max_survivor_p95"]):
        failures.append("survivors")
    if read_reduction + 1e-12 < float(gate["min_read_reduction_fraction"]):
        failures.append("reads")
    if byte_reduction + 1e-12 < float(gate["min_byte_reduction_fraction"]):
        failures.append("bytes")
    if cpu_p95_us > cpu_limit_us:
        failures.append("cpu")

    return {
        "accepted": not failures,
        "queries": len(rows),
        "containment_failures": containment_failures,
        "survivor_p95": survivor_p95,
        "baseline_reads": baseline_reads,
        "predicted_reads": predicted_reads,
        "read_reduction_fraction": read_reduction,
        "baseline_bytes": baseline_bytes,
        "predicted_bytes": predicted_bytes,
        "byte_reduction_fraction": byte_reduction,
        "cpu_p95_us": cpu_p95_us,
        "cpu_limit_us": cpu_limit_us,
        "read_p95_ms": read_p95_ms,
        "failures": failures,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("root", type=Path)
    parser.add_argument("--manifest", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()

    validate(args.root, args.manifest)
    manifest = json.loads(args.manifest.read_text(encoding="utf-8"))
    gate = manifest.get("exact_bound_shadow")
    if not isinstance(gate, dict) or not gate.get("required"):
        raise ValueError("manifest does not require exact-bound shadow evidence")
    with (args.root / "reads.csv").open(newline="", encoding="utf-8") as handle:
        result = evaluate(list(csv.DictReader(handle)), gate)
    args.output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(result, sort_keys=True))
    return 0 if result["accepted"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
