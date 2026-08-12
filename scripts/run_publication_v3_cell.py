#!/usr/bin/env python3
"""Fail-closed execution adapter for one canonical Publication V3 cell."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import os
import subprocess
from pathlib import Path

try:
    from scripts.publication_v3_protocol import canonical_json_bytes, read_protocol
except ModuleNotFoundError:
    from publication_v3_protocol import canonical_json_bytes, read_protocol


SUPPORTED_LOCAL_KINDS = frozenset({"read-recall"})


def plan_arms(cell: dict[str, object]) -> list[dict[str, object]]:
    workload = cell.get("workload")
    if not isinstance(workload, dict) or workload.get("kind") != "read-recall":
        raise ValueError("only read-recall arms are currently executable")
    factors = workload.get("factors")
    if not isinstance(factors, dict):
        raise ValueError("read-recall factors are invalid")
    k_values = factors.get("k")
    if k_values != [10]:
        unsupported = next((value for value in k_values or [] if value != 10), "missing")
        raise ValueError(f"k={unsupported} is not executable by the current benchmark")
    candidates = factors.get("candidate_budgets")
    cache_states = factors.get("cache_states")
    routing_budget = factors.get("routing_cell_budget")
    if (
        not isinstance(candidates, list)
        or not candidates
        or not isinstance(cache_states, list)
        or not cache_states
        or isinstance(routing_budget, bool)
        or not isinstance(routing_budget, int)
        or routing_budget <= 0
    ):
        raise ValueError("read-recall arm factors are incomplete")
    if any(state not in {"cold", "warm"} for state in cache_states):
        raise ValueError("read-recall cache state is unsupported")
    return [
        {
            "k": 10,
            "candidate_budget": candidate,
            "routing_cell_budget": routing_budget,
            "cache_state": state,
        }
        for candidate in candidates
        for state in cache_states
    ]


def build_execution_plan(
    cell: dict[str, object],
    *,
    arm: dict[str, object],
    workspace: Path,
    generator: Path,
    borsuk_bench: Path,
    mode: str,
) -> dict[str, object]:
    if mode not in {"publication", "smoke"}:
        raise ValueError("execution mode must be publication or smoke")
    if cell.get("system") != "borsuk":
        raise ValueError(f"system {cell.get('system')!r} is not available in local execution")
    workload = cell.get("workload")
    dataset = cell.get("dataset")
    source = cell.get("source")
    if not isinstance(workload, dict) or workload.get("kind") not in SUPPORTED_LOCAL_KINDS:
        raise ValueError("workload is not supported by the local read runner")
    if not isinstance(dataset, dict) or not isinstance(dataset.get("source"), dict):
        raise ValueError("cell dataset is invalid")
    if mode == "publication" and (not isinstance(source, dict) or source.get("state") != "frozen"):
        raise ValueError("publication execution requires a frozen source archive")
    if mode == "publication":
        raise ValueError("publication execution awaits exact index-profile binding")

    factors = workload.get("factors")
    scale = dataset.get("scale")
    if not isinstance(factors, dict) or not isinstance(scale, dict):
        raise ValueError("cell workload factors or dataset scale are invalid")
    scheduled_rows = scale.get("rows")
    dimensions = dataset.get("dimensions")
    if (
        isinstance(scheduled_rows, bool)
        or not isinstance(scheduled_rows, int)
        or scheduled_rows <= 0
        or isinstance(dimensions, bool)
        or not isinstance(dimensions, int)
        or dimensions <= 0
    ):
        raise ValueError("cell rows and dimensions must be positive integers")
    queries_per_repetition = cell.get("queries_per_repetition")
    if isinstance(queries_per_repetition, bool) or not isinstance(queries_per_repetition, int):
        raise ValueError("cell source query count is invalid")

    effective_rows = scheduled_rows if mode == "publication" else min(scheduled_rows, 1_000)
    effective_queries = queries_per_repetition if mode == "publication" else min(queries_per_repetition, 10)
    dataset_dir = workspace / "dataset"
    output_dir = workspace / "output"
    index_dir = workspace / "index"
    cache_dir = workspace / "cache"
    if arm not in plan_arms(cell):
        raise ValueError("execution arm is not authorized by the scheduled cell")

    steps: list[dict[str, object]] = []
    dataset_source = dataset["source"]
    if dataset_source.get("state") == "generated":
        if dataset_source.get("generator") != "synthetic-clustered-v1":
            raise ValueError("scheduled synthetic generator is not implemented")
        if dataset.get("metric") != "cosine":
            raise ValueError("the deterministic dense generator supports cosine cells only")
        steps.append(
            {
                "argv": [str(generator)],
                "env": {
                    "BORSUK_SYNTHETIC_OUTPUT": str(dataset_dir),
                    "BORSUK_SYNTHETIC_TRAIN": str(effective_rows),
                    "BORSUK_SYNTHETIC_DIMENSIONS": str(dimensions),
                    "BORSUK_SYNTHETIC_QUERIES": str(effective_queries),
                    "BORSUK_SYNTHETIC_GROUP_SIZE": "100",
                    "BORSUK_SYNTHETIC_SEED": str(dataset_source.get("seed")),
                },
            }
        )
    elif dataset_source.get("state") != "staged":
        raise ValueError("dataset must be generated or staged before execution")

    benchmark_env = {
        "BORSUK_BENCH_DATASET": str(dataset_dir),
        "BORSUK_BENCH_URI": str(index_dir),
        "BORSUK_BENCH_CACHE": str(cache_dir),
        "BORSUK_BENCH_OUTPUT_DIR": str(output_dir),
        "BORSUK_BENCH_QUERIES": str(effective_queries),
        "BORSUK_BENCH_QUERY_SEED": str(cell.get("query_seed")),
        "BORSUK_BENCH_REPETITION_ID": str(cell.get("repetition_id")),
        "BORSUK_BENCH_NPROBES": str(arm["routing_cell_budget"]),
        "BORSUK_BENCH_CANDIDATES": str(arm["candidate_budget"]),
        "BORSUK_BENCH_READ_ONLY": "1",
        "BORSUK_BENCH_CONCURRENCY": "1",
        "BORSUK_BENCH_SKIP_EXACT_RECALL": "1",
        "BORSUK_BENCH_CACHE_PROFILE": (
            "uncached" if arm["cache_state"] == "cold" else "disk_cached"
        ),
    }
    if mode == "smoke":
        benchmark_env["BORSUK_BENCH_LIMIT"] = str(effective_rows)
    steps.append(
        {
            "argv": [str(borsuk_bench)],
            "env": benchmark_env,
        }
    )
    return {
        "schema_version": 1,
        "cell_id": cell.get("cell_id"),
        "mode": mode,
        "publishable": mode == "publication",
        "effective_rows": effective_rows,
        "effective_queries": effective_queries,
        "workspace": str(workspace),
        "output_dir": str(output_dir),
        "steps": steps,
    }


def execute_plan(plan: dict[str, object]) -> Path:
    steps = plan.get("steps")
    if not isinstance(steps, list) or not steps:
        raise ValueError("execution plan has no steps")
    workspace = Path(str(plan.get("workspace")))
    output_dir = Path(str(plan.get("output_dir")))
    workspace.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)
    for index, step in enumerate(steps):
        if not isinstance(step, dict) or not isinstance(step.get("argv"), list):
            raise ValueError("execution step is invalid")
        argv = [str(value) for value in step["argv"]]
        if not argv or any(not value for value in argv):
            raise ValueError("execution argv is invalid")
        environment = step.get("env")
        if not isinstance(environment, dict):
            raise ValueError("execution environment is invalid")
        child_environment = {
            "HOME": os.environ.get("HOME", "/tmp"),
            "LANG": "C.UTF-8",
            "PATH": "/usr/bin:/bin",
        }
        child_environment.update({str(key): str(value) for key, value in environment.items()})
        log_path = workspace / f"step-{index:02d}.log"
        with log_path.open("wb") as log:
            subprocess.run(
                argv,
                check=True,
                cwd=workspace,
                env=child_environment,
                stdout=log,
                stderr=subprocess.STDOUT,
            )
    samples = output_dir / "bench_query_samples.csv"
    if not samples.is_file() or samples.stat().st_size == 0:
        raise ValueError("execution completed without a real query sample artifact")
    return samples


def _nearest_rank(values: list[int], quantile: float) -> int:
    return sorted(values)[max(0, math.ceil(quantile * len(values)) - 1)]


def summarize_query_samples(
    rows: list[dict[str, str]],
    *,
    cell: dict[str, object],
    arm: dict[str, object],
    expected_queries: int,
    enforce_quality: bool = True,
) -> dict[str, int]:
    if len(rows) != expected_queries:
        raise ValueError("query sample artifact is incomplete for its arm")
    latencies_us: list[int] = []
    recalls_ppm: list[int] = []
    sample_indices: set[int] = set()
    storage_gets = 0
    storage_bytes_read = 0
    for row in rows:
        if row.get("schema_version") != "borsuk-production-bench-v10":
            raise ValueError("query sample schema differs")
        expected_phase = "uncached" if arm["cache_state"] == "cold" else "disk_cached"
        if (
            row.get("phase") != expected_phase
            or row.get("mode") != "srht-pq-scan"
            or int(row.get("nprobe", "-1")) != arm["routing_cell_budget"]
            or int(row.get("max_candidates", "-1")) != arm["candidate_budget"]
        ):
            raise ValueError("query sample belongs to a different factor arm")
        sample_index = int(row["sample_index"])
        if sample_index < 0 or sample_index in sample_indices:
            raise ValueError("query sample indices must be unique and nonnegative")
        sample_indices.add(sample_index)
        latency = float(row["latency_ms"])
        recall = float(row["recall_at_10"])
        if not math.isfinite(latency) or latency < 0 or not math.isfinite(recall) or not 0 <= recall <= 1:
            raise ValueError("query sample latency or recall is invalid")
        latencies_us.append(round(latency * 1_000))
        recalls_ppm.append(round(recall * 1_000_000))
        network_gets = int(row["network_gets"])
        bytes_read = int(row["bytes_read"])
        if network_gets < 0 or bytes_read < 0:
            raise ValueError("query sample storage telemetry is invalid")
        storage_gets += network_gets
        storage_bytes_read += bytes_read
    correctness_ppm = round(sum(recalls_ppm) / len(recalls_ppm))
    factors = cell.get("workload", {}).get("factors", {})
    floor = factors.get("minimum_recall_ppm")
    if not isinstance(floor, int) or (enforce_quality and correctness_ppm < floor):
        raise ValueError("query sample quality floor is not met")
    return {
        "queries": len(rows),
        "correctness_ppm": correctness_ppm,
        "latency_p50_us": _nearest_rank(latencies_us, 0.50),
        "latency_p95_us": _nearest_rank(latencies_us, 0.95),
        "latency_p99_us": _nearest_rank(latencies_us, 0.99),
        "storage_gets": storage_gets,
        "storage_bytes_read": storage_bytes_read,
    }


def build_smoke_report(
    *,
    cell: dict[str, object],
    arm: dict[str, object],
    effective_rows: int,
    effective_queries: int,
    metrics: dict[str, int],
    protocol_sha256: str,
) -> dict[str, object]:
    if len(protocol_sha256) != 64 or any(
        character not in "0123456789abcdef" for character in protocol_sha256
    ):
        raise ValueError("smoke protocol checksum is invalid")
    return {
        "schema_version": 1,
        "document_kind": "publication-v3-smoke",
        "publishable": False,
        "cell_id": cell["cell_id"],
        "protocol_sha256": protocol_sha256,
        "arm": arm,
        "effective_rows": effective_rows,
        "effective_queries": effective_queries,
        "metrics": metrics,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("protocol", type=Path)
    parser.add_argument("workspace", type=Path)
    parser.add_argument("--generator", type=Path, required=True)
    parser.add_argument("--borsuk-bench", type=Path, required=True)
    parser.add_argument("--arm-index", type=int, default=0)
    args = parser.parse_args()

    cell = read_protocol(args.protocol)
    protocol_bytes = args.protocol.read_bytes()
    arms = plan_arms(cell)
    if args.arm_index < 0 or args.arm_index >= len(arms):
        raise ValueError("arm index is outside the scheduled factor matrix")
    arm = arms[args.arm_index]
    plan = build_execution_plan(
        cell,
        arm=arm,
        workspace=args.workspace,
        generator=args.generator,
        borsuk_bench=args.borsuk_bench,
        mode="smoke",
    )
    samples = execute_plan(plan)
    with samples.open(newline="") as source:
        rows = list(csv.DictReader(source))
    metrics = summarize_query_samples(
        rows,
        cell=cell,
        arm=arm,
        expected_queries=int(plan["effective_queries"]),
        enforce_quality=False,
    )
    report = build_smoke_report(
        cell=cell,
        arm=arm,
        effective_rows=int(plan["effective_rows"]),
        effective_queries=int(plan["effective_queries"]),
        metrics=metrics,
        protocol_sha256=hashlib.sha256(protocol_bytes).hexdigest(),
    )
    destination = args.workspace / "SMOKE_COMPLETE.json"
    destination.write_bytes(canonical_json_bytes(report) + b"\n")
    print(json.dumps(report, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
