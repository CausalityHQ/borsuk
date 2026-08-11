#!/usr/bin/env python3
"""Validate and expand the positioned V12 logical-cell routing protocol."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


class ProtocolError(ValueError):
    """The requested execution differs from the preregistered protocol."""


def comma_separated_integers(value: str) -> list[int]:
    try:
        parsed = [int(item) for item in value.split(",")]
    except ValueError as error:
        raise argparse.ArgumentTypeError("expected comma-separated integers") from error
    if (
        not parsed
        or any(item <= 0 for item in parsed)
        or len(set(parsed)) != len(parsed)
    ):
        raise argparse.ArgumentTypeError("values must be unique positive integers")
    return parsed


def validate_manifest(manifest: dict[str, object]) -> None:
    if manifest.get("protocol_kind") == "local-smoke":
        expected = {
            "mutation_protocol": "positioned-v12",
            "architecture": "local",
            "instance_type": "local",
            "purchase_option": "local",
            "dimensions": 768,
            "metric": "cosine",
            "cell_counts": [2_000, 16_000],
            "routing_modes": ["flat", "quantizer"],
            "writers": [1],
            "repetitions": 1,
        }
    else:
        expected = {
            "protocol_kind": "architecture-qualification",
            "mutation_protocol": "positioned-v12",
            "architecture": "aarch64",
            "instance_type": "c7g.8xlarge",
            "purchase_option": "spot",
            "dimensions": 768,
            "metric": "cosine",
            "cell_counts": [2_000, 16_000],
            "routing_modes": ["flat", "quantizer"],
            "writers": [1, 8, 32],
            "repetitions": 5,
        }
    for field, value in expected.items():
        if manifest.get(field) != value:
            raise ProtocolError(f"manifest must freeze {field} to {value!r}")
    for field in (
        "operations_per_writer",
        "warmup_operations_per_writer",
        "cell_timeout_seconds",
        "clone_timeout_seconds",
        "campaign_timeout_seconds",
        "setup_timeout_seconds",
        "resource_sample_interval_ms",
        "master_seed",
    ):
        value = manifest.get(field)
        if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
            raise ProtocolError(f"manifest field {field} must be a positive integer")
    if manifest["cell_timeout_seconds"] > 14_400:
        raise ProtocolError(
            "cell timeout exceeds the preregistered four-hour hard bound"
        )
    if manifest["campaign_timeout_seconds"] > 21_600:
        raise ProtocolError("campaign timeout exceeds the six-hour hard bound")
    gates = manifest.get("correctness_gates")
    if not isinstance(gates, list) or not gates or len(set(gates)) != len(gates):
        raise ProtocolError("correctness gates must be a nonempty unique list")


def expand_plan(
    manifest: dict[str, object],
    *,
    structural_smoke: bool,
    writers: list[int] | None,
    logical_cells: list[int] | None,
) -> dict[str, object]:
    validate_manifest(manifest)
    frozen_writers = list(manifest["writers"])
    frozen_cells = list(manifest["cell_counts"])
    requested_writers = writers if writers is not None else frozen_writers
    requested_cells = logical_cells if logical_cells is not None else frozen_cells
    if requested_writers != frozen_writers or requested_cells != frozen_cells:
        raise ProtocolError(
            "structural planning must preserve the full preregistered factor shape"
        )
    repetitions = 1 if structural_smoke else int(manifest["repetitions"])
    arms: list[dict[str, object]] = []
    for cells in requested_cells:
        for repetition in range(1, repetitions + 1):
            modes = (
                ["flat", "quantizer"] if repetition % 2 == 1 else ["quantizer", "flat"]
            )
            for writer_count in requested_writers:
                for mode in modes:
                    arms.append(
                        {
                            "cell_count": cells,
                            "writers": writer_count,
                            "repetition": repetition,
                            "routing_mode": mode,
                        }
                    )
    worst_case_seconds = int(manifest["setup_timeout_seconds"]) + len(arms) * (
        int(manifest["clone_timeout_seconds"]) + int(manifest["cell_timeout_seconds"])
    )
    if worst_case_seconds > int(manifest["campaign_timeout_seconds"]):
        raise ProtocolError(
            "worst-case matrix duration exceeds campaign_timeout_seconds: "
            f"{worst_case_seconds} > {manifest['campaign_timeout_seconds']}"
        )
    return {
        "campaign_id": manifest["campaign_id"],
        "claim_eligibility": (
            "ineligible-structural-smoke"
            if structural_smoke or manifest["protocol_kind"] == "local-smoke"
            else "architecture-qualification-only"
        ),
        "mutation_protocol": manifest["mutation_protocol"],
        "purchase_option": manifest["purchase_option"],
        "dimensions": manifest["dimensions"],
        "metric": manifest["metric"],
        "clone_timeout_seconds": manifest["clone_timeout_seconds"],
        "campaign_timeout_seconds": manifest["campaign_timeout_seconds"],
        "setup_timeout_seconds": manifest["setup_timeout_seconds"],
        "cell_timeout_seconds": manifest["cell_timeout_seconds"],
        "worst_case_seconds": worst_case_seconds,
        "cell_counts": requested_cells,
        "writers": requested_writers,
        "repetitions": repetitions,
        "arm_count": len(arms),
        "arms": arms,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", required=True, type=Path)
    parser.add_argument("--structural-smoke", action="store_true")
    parser.add_argument("--writers", type=comma_separated_integers)
    parser.add_argument("--logical-cells", type=comma_separated_integers)
    args = parser.parse_args()
    try:
        manifest = json.loads(args.manifest.read_text(encoding="utf-8"))
        plan = expand_plan(
            manifest,
            structural_smoke=args.structural_smoke,
            writers=args.writers,
            logical_cells=args.logical_cells,
        )
    except (OSError, json.JSONDecodeError, ProtocolError) as error:
        parser.error(str(error))
    json.dump(plan, sys.stdout, indent=2, sort_keys=True)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
