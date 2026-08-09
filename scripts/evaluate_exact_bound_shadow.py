#!/usr/bin/env python3
"""Evaluate the preregistered exact-rerank shadow decision after validation."""

import argparse
import csv
import json
import math
import sys
from pathlib import Path
from typing import Any

try:
    from .validate_group_commit_scalability import ValidationError, validate
except ImportError:
    from validate_group_commit_scalability import ValidationError, validate


def percentile(values: list[float], quantile: float) -> float:
    if not values:
        raise ValueError("cannot evaluate an empty exact-bound shadow")
    ordered = sorted(values)
    index = math.floor((len(ordered) - 1) * quantile + 0.5)
    return ordered[index]


def evaluate(
    rows: list[dict[str, str]],
    gate: dict[str, Any],
    optimization: dict[str, Any] | None = None,
) -> dict[str, Any]:
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
    scan_waves = [int(row["global_exact_bound_predicted_waves"]) for row in rows]
    scratch_allocations = [
        int(row["global_exact_bound_certificate_scratch_allocations"])
        for row in rows
    ]
    residual_bytes = sum(
        int(row["global_exact_bound_residual_bytes"]) for row in rows
    )
    exact_backing_bytes = sum(
        int(row["global_exact_bound_exact_backing_bytes"]) for row in rows
    )
    total_backing_bytes = sum(int(row["backing_bytes_read"]) for row in rows)
    if baseline_reads <= 0 or baseline_bytes <= 0:
        raise ValueError("exact-bound shadow baseline must be positive")
    if exact_backing_bytes <= 0:
        raise ValueError("exact-bound shadow backing-byte baseline must be positive")

    survivor_p95 = int(percentile([float(value) for value in survivors], 0.95))
    cpu_values = [float(row["global_exact_bound_cpu_us"]) for row in rows]
    read_values = [float(row["latency_ms"]) for row in rows]
    cpu_p50_us = int(percentile(cpu_values, 0.50))
    cpu_p95_us = int(percentile(cpu_values, 0.95))
    cpu_p99_us = int(percentile(cpu_values, 0.99))
    read_p50_ms = percentile(read_values, 0.50)
    read_p95_ms = percentile(read_values, 0.95)
    read_p99_ms = percentile(read_values, 0.99)
    read_reduction = 1.0 - predicted_reads / baseline_reads
    byte_reduction = 1.0 - predicted_bytes / baseline_bytes
    candidate_count = sum(int(row["global_exact_bound_candidates"]) for row in rows)
    if candidate_count <= 0:
        raise ValueError("exact-bound shadow candidate count must be positive")
    residual_bytes_per_vector = residual_bytes / candidate_count
    total_backing_byte_ratio = total_backing_bytes / exact_backing_bytes
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
    if residual_bytes_per_vector > float(gate["max_residual_bytes_per_vector"]):
        failures.append("residual_bytes")
    if total_backing_byte_ratio > float(gate["max_total_backing_byte_ratio"]):
        failures.append("total_backing_bytes")

    drain_regression_fraction = 0.0
    physical_write_amplification_regression_fraction = 0.0
    if gate.get("non_read_regression_control") != "same-cell-shared-ingest-and-drain":
        raise ValueError("unsupported non-read regression control")
    if drain_regression_fraction > float(gate["max_drain_regression_fraction"]):
        failures.append("drain")
    if physical_write_amplification_regression_fraction > float(
        gate["max_physical_write_amplification_regression_fraction"]
    ):
        failures.append("physical_write_amplification")

    if optimization is None:
        optimization = {}
    hard_read_p95_ms = float(optimization.get("hard_read_p95_ms", math.inf))
    if read_p95_ms > hard_read_p95_ms:
        failures.append("read_latency_hard_cap")

    exact_vector_bytes = int(gate["exact_vector_bytes"])
    survivor_total = sum(survivors)
    exact_range_floor = sum(value > 0 for value in survivors)
    exact_byte_floor = survivor_total * exact_vector_bytes
    scan_waves_total = sum(scan_waves)
    if predicted_reads < exact_range_floor or scan_waves_total < exact_range_floor:
        raise ValueError("exact physical plan is below its structural request floor")
    if predicted_bytes < exact_byte_floor:
        raise ValueError("exact physical plan is below its structural byte floor")
    floor_gaps = {
        "exact_ranges_above_one_range_per_nonempty_query": predicted_reads
        - exact_range_floor,
        "scan_waves_above_one_wave_per_nonempty_query": scan_waves_total
        - exact_range_floor,
        "exact_bytes_above_lossless_survivor_payload": predicted_bytes
        - exact_byte_floor,
        "read_p95_ms_above_observed_minimum": read_p95_ms - min(read_values),
        "certificate_cpu_p95_us_above_observed_minimum": cpu_p95_us
        - min(cpu_values),
    }

    return {
        "accepted": not failures,
        "provisional_only": True,
        "production_default_eligible": False,
        "queries": len(rows),
        "containment_failures": containment_failures,
        "survivor_p95": survivor_p95,
        "baseline_reads": baseline_reads,
        "predicted_reads": predicted_reads,
        "dynamic_program_minimum_exact_ranges": predicted_reads,
        "read_reduction_fraction": read_reduction,
        "baseline_bytes": baseline_bytes,
        "predicted_bytes": predicted_bytes,
        "dynamic_program_minimum_exact_bytes": predicted_bytes,
        "byte_reduction_fraction": byte_reduction,
        "cpu_p50_us": cpu_p50_us,
        "cpu_p95_us": cpu_p95_us,
        "cpu_p99_us": cpu_p99_us,
        "cpu_limit_us": cpu_limit_us,
        "certificate_scratch_allocations_p95": int(
            percentile([float(value) for value in scratch_allocations], 0.95)
        ),
        "scan_waves_total": scan_waves_total,
        "scan_waves_p95": int(
            percentile([float(value) for value in scan_waves], 0.95)
        ),
        "residual_bytes_per_vector": residual_bytes_per_vector,
        "total_backing_bytes": total_backing_bytes,
        "exact_backing_bytes": exact_backing_bytes,
        "total_backing_byte_ratio": total_backing_byte_ratio,
        "drain_regression_fraction": drain_regression_fraction,
        "physical_write_amplification_regression_fraction": (
            physical_write_amplification_regression_fraction
        ),
        "non_read_regression_control": gate["non_read_regression_control"],
        "read_p50_ms": read_p50_ms,
        "read_p95_ms": read_p95_ms,
        "read_p99_ms": read_p99_ms,
        "hard_read_p95_ms": hard_read_p95_ms,
        "optimization_selection_rule": optimization.get("selection_rule"),
        "structural_and_empirical_floor_gaps": floor_gaps,
        "failures": failures,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("root", type=Path)
    parser.add_argument("--manifest", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument(
        "--preterminal-root",
        action="store_true",
        help="evaluate a root that has passed validation but is not terminally marked yet",
    )
    args = parser.parse_args()

    validate(args.root, args.manifest, preterminal_root=args.preterminal_root)
    manifest = json.loads(args.manifest.read_text(encoding="utf-8"))
    gate = manifest.get("exact_bound_shadow")
    if not isinstance(gate, dict) or not gate.get("required"):
        raise ValueError("manifest does not require exact-bound shadow evidence")
    optimization = manifest.get("optimization_contract")
    if not isinstance(optimization, dict):
        raise ValueError("manifest does not define an optimization contract")
    with (args.root / "reads.csv").open(newline="", encoding="utf-8") as handle:
        result = evaluate(list(csv.DictReader(handle)), gate, optimization)
    args.output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(result, sort_keys=True))
    return 0 if result["accepted"] else 1


def entrypoint() -> int:
    try:
        return main()
    except (
        OSError,
        KeyError,
        TypeError,
        ValueError,
        json.JSONDecodeError,
        ValidationError,
    ) as error:
        print(f"exact-bound shadow evaluation failed: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(entrypoint())
