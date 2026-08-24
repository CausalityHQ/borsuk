#!/usr/bin/env python3
"""Run one frozen Publication V3 read workload serially and resumably."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
from pathlib import Path

try:
    from scripts.publication_v3_protocol import validate_manifest
except ModuleNotFoundError:
    from publication_v3_protocol import validate_manifest


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MANIFEST = ROOT / "docs/research/publication-v3-manifest.json"
DEFAULT_LAUNCHER = ROOT / "scripts/launch_aws_publication_v3.sh"


def campaign_commands(
    manifest: dict[str, object], workload_id: str
) -> list[tuple[str, ...]]:
    """Return the exact serial build/runtime command suffixes for one workload."""

    normalized = validate_manifest(manifest)
    matches = [
        workload
        for workload in normalized["workloads"]
        if workload["id"] == workload_id and workload["kind"] == "read-recall"
    ]
    if len(matches) != 1:
        raise ValueError("campaign requires one scheduled read-recall workload")
    workload = matches[0]
    datasets = {dataset["id"]: dataset for dataset in normalized["datasets"]}
    if any(
        datasets[dataset_id]["source"]["state"] not in {"staged", "staged-generated"}
        for dataset_id in workload["dataset_ids"]
    ):
        raise ValueError("campaign requires a durable generated dataset handoff")
    factors = workload["factors"]
    arm_count = len(factors["leaf_page_budgets"]) * len(factors["cache_states"])
    repetitions = int(normalized["repetitions"])
    commands: list[tuple[str, ...]] = []
    for dataset_id in workload["dataset_ids"]:
        commands.append(("--build-read", workload_id, dataset_id))
        for repetition in range(1, repetitions + 1):
            repetition_id = f"r{repetition:02d}"
            for arm_index in range(arm_count):
                commands.append(
                    (
                        "--run-read",
                        workload_id,
                        dataset_id,
                        repetition_id,
                        str(arm_index),
                    )
                )
    return commands


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("workload")
    parser.add_argument("--launcher", type=Path, default=DEFAULT_LAUNCHER)
    parser.add_argument("--profile", default="causality")
    parser.add_argument("--max-attempts", type=int, default=6)
    parser.add_argument("--plan-only", action="store_true")
    args = parser.parse_args()

    manifest = json.loads(DEFAULT_MANIFEST.read_text(encoding="utf-8"))
    commands = campaign_commands(manifest, args.workload)
    if args.plan_only:
        print(
            json.dumps([list(command) for command in commands], separators=(",", ":"))
        )
        return 0
    environment = {
        **os.environ,
        "AWS_PROFILE": args.profile,
        "BORSUK_PUBLICATION_V3_BUILD_ATTEMPT": "0",
        "BORSUK_PUBLICATION_V3_RUNTIME_ATTEMPT": "0",
        "BORSUK_PUBLICATION_V3_MAX_ATTEMPTS": str(args.max_attempts),
    }
    for command in commands:
        subprocess.run(
            ["bash", str(args.launcher), *command],
            cwd=ROOT,
            env=environment,
            check=True,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
