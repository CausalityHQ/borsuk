#!/usr/bin/env python3
"""Plan and execute fresh, publication-grade market benchmark experiments.

The planner is intentionally strict: a dataset is runnable only when its local
descriptor identifies the source, license, adapter, scale/dimensions, and every
prepared file by SHA-256. Missing data becomes an explicit blocked row; it is
never silently replaced by a synthetic workload with the same label.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import re
import subprocess
import sys
from pathlib import Path, PurePosixPath
from typing import Iterable

from validate_benchmark_artifacts import validate_directory

MIXED_COVERAGE = (0, 25, 50, 75, 100)
CONCURRENCY_PROFILES = ("production", "research_ceiling")
SPECIALIZED_ARTIFACTS = {
    "borsuk_filter": {
        "build": ("filter_build.csv", "resources.csv"),
        "query": ("filter_summary.csv", "filter_samples.csv", "resources.csv"),
    },
    "borsuk_namespace": {
        "build": ("namespace_build.csv", "resources.csv"),
        "query": ("namespace_summary.csv", "namespace_samples.csv", "resources.csv"),
    },
    "borsuk_late_interaction": {
        "build": ("late_interaction_build.csv", "resources.csv"),
        "query": (
            "late_interaction_summary.csv",
            "late_interaction_samples.csv",
            "resources.csv",
        ),
    },
}
PLAN_COLUMNS = (
    "run_id",
    "goal",
    "dataset",
    "workload",
    "adapter",
    "repetition",
    "cache_profile",
    "cache_coverage_percent",
    "traffic_class",
    "concurrency_profile",
    "status",
    "blocker",
    "dataset_dir",
    "index_uri",
    "output_dir",
)
REQUIRED_DESCRIPTOR_KEYS = {
    "schema_version",
    "dataset",
    "workload",
    "dimensions",
    "scale",
    "source",
    "source_sha256",
    "license",
    "adapter",
    "files",
}
KNOWN_ADAPTERS = {
    "borsuk_dense_ann",
    "borsuk_filter",
    "borsuk_lifecycle",
    "borsuk_namespace",
    "borsuk_hybrid",
    "borsuk_late_interaction",
}
HEX_SHA256 = re.compile(r"^[0-9a-f]{64}$")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def load_descriptor(dataset_dir: Path) -> dict:
    descriptor_path = dataset_dir / "dataset.json"
    if not descriptor_path.is_file():
        raise ValueError(f"missing descriptor {descriptor_path}")
    try:
        descriptor = json.loads(descriptor_path.read_text())
    except (OSError, json.JSONDecodeError) as error:
        raise ValueError(f"invalid descriptor {descriptor_path}: {error}") from error
    if not isinstance(descriptor, dict):
        raise ValueError(f"{descriptor_path} must contain a JSON object")
    missing = sorted(REQUIRED_DESCRIPTOR_KEYS.difference(descriptor))
    if missing:
        raise ValueError(f"{descriptor_path} is missing keys: {', '.join(missing)}")
    return descriptor


def validate_dataset(
    dataset_dir: Path,
    expected_dataset: str,
    expected_workload: str,
    expected_dimensions: int | str,
    expected_scale: str,
) -> str:
    """Return an empty string when valid, otherwise a publication blocker."""
    try:
        descriptor = load_descriptor(dataset_dir)
        if descriptor["schema_version"] != 1:
            raise ValueError("dataset descriptor schema_version must be 1")
        if descriptor["dataset"] != expected_dataset:
            raise ValueError(
                f"dataset name mismatch: {descriptor['dataset']!r} != {expected_dataset!r}"
            )
        if descriptor["workload"] != expected_workload:
            raise ValueError(
                f"workload mismatch: {descriptor['workload']!r} != {expected_workload!r}"
            )
        dimensions = str(descriptor["dimensions"])
        if str(expected_dimensions) != "variable" and dimensions != str(
            expected_dimensions
        ):
            raise ValueError(
                f"dimensions mismatch: {dimensions} != {expected_dimensions}"
            )
        if str(expected_scale) != "variable" and str(descriptor["scale"]) != str(
            expected_scale
        ):
            raise ValueError(
                f"scale mismatch: {descriptor['scale']} != {expected_scale}"
            )
        if not isinstance(descriptor["source"], str) or not descriptor["source"]:
            raise ValueError("dataset source must be non-empty")
        if not HEX_SHA256.fullmatch(str(descriptor["source_sha256"])):
            raise ValueError("source_sha256 must contain 64 lowercase hex characters")
        if not isinstance(descriptor["license"], str) or not descriptor["license"]:
            raise ValueError("dataset license must be non-empty")
        if descriptor["adapter"] not in KNOWN_ADAPTERS:
            raise ValueError(f"unknown benchmark adapter {descriptor['adapter']!r}")
        files = descriptor["files"]
        if not isinstance(files, list) or not files:
            raise ValueError("dataset descriptor files must be a non-empty list")
        for ordinal, item in enumerate(files):
            if not isinstance(item, dict):
                raise ValueError(f"files[{ordinal}] must be an object")
            relative = PurePosixPath(str(item.get("path", "")))
            if relative.is_absolute() or ".." in relative.parts or not relative.parts:
                raise ValueError(f"files[{ordinal}] has an unsafe relative path")
            path = dataset_dir.joinpath(*relative.parts)
            if not path.is_file():
                raise ValueError(f"missing prepared dataset file {path}")
            expected_bytes = item.get("bytes")
            if not isinstance(expected_bytes, int) or expected_bytes < 0:
                raise ValueError(f"files[{ordinal}].bytes must be nonnegative integer")
            if path.stat().st_size != expected_bytes:
                raise ValueError(
                    f"byte-size mismatch for {path}: {path.stat().st_size} != {expected_bytes}"
                )
            expected_hash = str(item.get("sha256", ""))
            if not HEX_SHA256.fullmatch(expected_hash):
                raise ValueError(f"files[{ordinal}].sha256 is invalid")
            actual_hash = sha256_file(path)
            if actual_hash != expected_hash:
                raise ValueError(
                    f"sha256 mismatch for {path}: {actual_hash} != {expected_hash}"
                )
    except (OSError, ValueError) as error:
        return str(error)
    return ""


def _cache_points(cache_profiles: str) -> Iterable[tuple[str, int, str]]:
    profiles = cache_profiles.split(";")
    if "uncached" in profiles:
        yield ("uncached", 0, "out_of_cache")
    if "disk_cached" in profiles:
        yield ("disk_cached", 100, "in_cache")
    if "mixed_coverage" in profiles:
        for coverage in MIXED_COVERAGE:
            traffic = (
                "out_of_cache"
                if coverage == 0
                else "in_cache"
                if coverage == 100
                else "mixed"
            )
            yield ("mixed_coverage", coverage, traffic)


def _join_uri(base: str, *parts: str) -> str:
    return "/".join([base.rstrip("/"), *(part.strip("/") for part in parts)])


def build_plan(
    *,
    matrix_path: Path,
    dataset_root: Path,
    output_root: Path,
    run_id: str,
    repetitions: int,
    selected_datasets: set[str] | None,
    bucket: str,
) -> list[dict[str, str]]:
    if repetitions < 1:
        raise ValueError("repetitions must be positive")
    if not run_id or "/" in run_id or "\\" in run_id:
        raise ValueError("run_id must be one non-empty path component")
    if not bucket:
        raise ValueError("bucket/prefix must be non-empty")
    with matrix_path.open(newline="") as handle:
        matrix = list(csv.DictReader(handle))
    rows: list[dict[str, str]] = []
    for contract in matrix:
        dataset = contract["dataset"]
        if dataset == "all_object_store_datasets":
            continue
        if selected_datasets is not None and dataset not in selected_datasets:
            continue
        dataset_dir = dataset_root / dataset
        blocker = validate_dataset(
            dataset_dir,
            dataset,
            contract["workload"],
            contract["dimensions"],
            contract["scale"],
        )
        adapter = ""
        if not blocker:
            adapter = str(load_descriptor(dataset_dir)["adapter"])
        for repetition in range(1, repetitions + 1):
            build_key = f"r{repetition:02d}"
            index_uri = _join_uri(bucket, run_id, dataset, build_key)
            for cache_profile, coverage, traffic_class in _cache_points(
                contract["cache_profiles"]
            ):
                for concurrency in CONCURRENCY_PROFILES:
                    relative = Path(
                        run_id,
                        dataset,
                        build_key,
                        cache_profile,
                        f"coverage-{coverage:03d}",
                        concurrency,
                    )
                    rows.append(
                        {
                            "run_id": run_id,
                            "goal": contract["goal"],
                            "dataset": dataset,
                            "workload": contract["workload"],
                            "adapter": adapter,
                            "repetition": str(repetition),
                            "cache_profile": cache_profile,
                            "cache_coverage_percent": str(coverage),
                            "traffic_class": traffic_class,
                            "concurrency_profile": concurrency,
                            "status": "blocked" if blocker else "ready",
                            "blocker": blocker,
                            "dataset_dir": str(dataset_dir),
                            "index_uri": index_uri,
                            "output_dir": str(output_root / relative),
                        }
                    )
            if adapter == "borsuk_lifecycle" or (
                not adapter and contract["goal"] == "write_to_serve"
            ):
                relative = Path(
                    run_id,
                    dataset,
                    build_key,
                    "lifecycle",
                    "coverage-000",
                    "production",
                )
                rows.append(
                    {
                        "run_id": run_id,
                        "goal": contract["goal"],
                        "dataset": dataset,
                        "workload": contract["workload"],
                        "adapter": adapter,
                        "repetition": str(repetition),
                        "cache_profile": "lifecycle",
                        "cache_coverage_percent": "0",
                        "traffic_class": "writes",
                        "concurrency_profile": "production",
                        "status": "blocked" if blocker else "ready",
                        "blocker": blocker,
                        "dataset_dir": str(dataset_dir),
                        "index_uri": index_uri,
                        "output_dir": str(output_root / relative),
                    }
                )
    return rows


def write_plan(path: Path, rows: list[dict[str, str]]) -> None:
    if path.exists():
        raise FileExistsError(f"refusing to overwrite existing plan {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("x", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=PLAN_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)


def read_plan(path: Path) -> list[dict[str, str]]:
    with path.open(newline="") as handle:
        rows = list(csv.DictReader(handle))
    if rows and tuple(rows[0].keys()) != PLAN_COLUMNS:
        raise ValueError(f"{path} has an invalid plan header")
    return rows


def dense_ann_command(
    row: dict[str, str], repo_root: Path
) -> tuple[list[str], dict[str, str]]:
    output = Path(row["output_dir"])
    cache = output / "cache"
    scratch = output / "scratch"
    resources = output / "resources.csv"
    concurrency = (
        "1,2,4" if row["concurrency_profile"] == "production" else "1,2,4,8,16,32"
    )
    environment = {
        "BORSUK_BENCH_DATASET": row["dataset_dir"],
        "BORSUK_BENCH_URI": row["index_uri"],
        "BORSUK_BENCH_CACHE": str(cache),
        "BORSUK_BENCH_OUTPUT_DIR": str(output),
        "BORSUK_BENCH_CONCURRENCY": concurrency,
        "BORSUK_BENCH_GLOBAL_SCAN_CODEC": "srht-pq-scan",
        "BORSUK_BENCH_CACHE_EXECUTION": "scan",
        "BORSUK_BENCH_MAX_ACTIVE_SEARCHES": "4",
        "BORSUK_BENCH_MAX_INFLIGHT_LEAF_READS": "24",
        "BORSUK_BENCH_RAM_BUDGET_BYTES": str(512 * 1024 * 1024),
        "BORSUK_BENCH_CACHE_PROFILE": (
            "uncached" if row["cache_profile"] == "lifecycle" else row["cache_profile"]
        ),
        "BORSUK_BENCH_CACHE_COVERAGE_PERCENT": row["cache_coverage_percent"],
    }
    command = [
        sys.executable,
        str(repo_root / "scripts/benchmark_with_resources.py"),
        "--output",
        str(resources),
        "--cache-dir",
        str(cache),
        "--scratch-dir",
        str(scratch),
        "--",
        str(repo_root / "target/release/examples/production_bench"),
    ]
    return command, environment


def hybrid_mode(workload: str) -> str:
    try:
        return {
            "dense_sparse": "dense+sparse",
            "dense_bm25": "dense+text",
            "sparse_bm25": "sparse+text",
        }[workload]
    except KeyError as error:
        raise ValueError(f"unsupported hybrid workload {workload!r}") from error


def hybrid_command(
    row: dict[str, str],
    repo_root: Path,
    *,
    action: str,
    output: Path,
) -> tuple[list[str], dict[str, str]]:
    cache = output / "cache"
    scratch = output / "scratch"
    resources = output / "resources.csv"
    coverage = int(row["cache_coverage_percent"])
    if coverage < 0 or coverage > 100:
        raise ValueError("cache coverage must be between 0 and 100")
    concurrency = 4 if row["concurrency_profile"] == "production" else 32
    environment = {
        "BORSUK_HYBRID_DATASET": row["dataset_dir"],
        "BORSUK_HYBRID_INDEX_URI": row["index_uri"],
        "BORSUK_HYBRID_OUTPUT": str(output),
        "BORSUK_HYBRID_SCAN_CODEC": "srht-pq-scan",
        "BORSUK_HYBRID_RAM_BUDGET_BYTES": str(512 * 1024 * 1024),
    }
    if action == "query":
        environment.update(
            {
                "BORSUK_HYBRID_MODES": hybrid_mode(row["workload"]),
                "BORSUK_HYBRID_REPETITIONS": "1",
                "BORSUK_HYBRID_CLIENT_CONCURRENCY": str(concurrency),
                "BORSUK_HYBRID_MAX_ACTIVE_SEARCHES": "4",
                "BORSUK_HYBRID_MAX_INFLIGHT_LEAF_READS": "24",
                "BORSUK_HYBRID_CACHE_DIR": str(cache),
                "BORSUK_HYBRID_CACHE_PROFILE": row["cache_profile"],
                "BORSUK_HYBRID_TARGET_HOT_FRACTION": f"{coverage / 100:.2f}",
                "BORSUK_HYBRID_PRIME_TARGET_HOT_SET": "1" if coverage else "0",
            }
        )
    command = [
        sys.executable,
        str(repo_root / "scripts/benchmark_with_resources.py"),
        "--output",
        str(resources),
        "--cache-dir",
        str(cache),
        "--scratch-dir",
        str(scratch),
        "--",
        str(repo_root / "target/release/examples/hybrid_retrieval_bench"),
        action,
    ]
    return command, environment


def specialized_workload_name(adapter: str) -> str:
    try:
        return {
            "borsuk_filter": "filter",
            "borsuk_namespace": "namespace",
            "borsuk_late_interaction": "late-interaction",
        }[adapter]
    except KeyError as error:
        raise ValueError(f"unsupported specialized adapter {adapter!r}") from error


def specialized_command(
    row: dict[str, str],
    repo_root: Path,
    *,
    action: str,
    output: Path,
) -> tuple[list[str], dict[str, str]]:
    cache = output / "cache"
    scratch = output / "scratch"
    resources = output / "resources.csv"
    coverage = int(row["cache_coverage_percent"])
    if coverage < 0 or coverage > 100:
        raise ValueError("cache coverage must be between 0 and 100")
    concurrency = 4 if row["concurrency_profile"] == "production" else 32
    environment = {
        "BORSUK_MARKET_DATASET": row["dataset_dir"],
        "BORSUK_MARKET_INDEX_URI": row["index_uri"],
        "BORSUK_MARKET_OUTPUT": str(output),
        "BORSUK_MARKET_CACHE_DIR": str(cache),
        "BORSUK_MARKET_CACHE_PROFILE": row["cache_profile"],
        "BORSUK_MARKET_CACHE_COVERAGE_PERCENT": str(coverage),
        "BORSUK_MARKET_CLIENT_CONCURRENCY": str(concurrency),
        "BORSUK_MARKET_MAX_ACTIVE_SEARCHES": "4",
        "BORSUK_MARKET_MAX_INFLIGHT_LEAF_READS": "24",
        "BORSUK_MARKET_RAM_BUDGET_BYTES": str(512 * 1024 * 1024),
    }
    command = [
        sys.executable,
        str(repo_root / "scripts/benchmark_with_resources.py"),
        "--output",
        str(resources),
        "--cache-dir",
        str(cache),
        "--scratch-dir",
        str(scratch),
        "--",
        str(repo_root / "target/release/examples/market_workload_bench"),
        specialized_workload_name(row["adapter"]),
        action,
    ]
    return command, environment


def run_measured(
    command: list[str],
    environment: dict[str, str],
    *,
    repo_root: Path,
) -> None:
    subprocess.run(
        command,
        cwd=repo_root,
        env={**os.environ, **environment},
        check=True,
    )


def validate_specialized_output(adapter: str, action: str, output: Path) -> None:
    try:
        required = SPECIALIZED_ARTIFACTS[adapter][action]
    except KeyError as error:
        raise ValueError(
            f"no artifact contract for specialized adapter={adapter!r}, action={action!r}"
        ) from error
    validate_directory(output, None, required)


def execute_plan(
    plan_path: Path,
    *,
    repo_root: Path,
    allow_paid_execution: bool,
    selected_status: str = "ready",
) -> None:
    if not allow_paid_execution:
        raise PermissionError("execution requires --allow-paid-execution")
    rows = read_plan(plan_path)
    built_indexes: set[str] = set()
    for row in rows:
        if row["status"] != selected_status:
            continue
        output = Path(row["output_dir"])
        if output.exists():
            raise FileExistsError(f"refusing to overwrite run output {output}")
        build_index = row["index_uri"] not in built_indexes
        if row["adapter"] in {"borsuk_dense_ann", "borsuk_lifecycle"}:
            output.mkdir(parents=True)
            command, environment = dense_ann_command(row, repo_root)
            environment["BORSUK_BENCH_READ_ONLY"] = (
                "0"
                if row["adapter"] == "borsuk_lifecycle"
                and row["cache_profile"] == "lifecycle"
                else "1"
            )
            environment["BORSUK_BENCH_BUILD_INDEX"] = "1" if build_index else "0"
            run_measured(command, environment, repo_root=repo_root)
        elif row["adapter"] == "borsuk_hybrid":
            if build_index:
                build_output = output.parents[2] / "build"
                if build_output.exists():
                    raise FileExistsError(
                        f"refusing to overwrite hybrid build output {build_output}"
                    )
                build_output.mkdir(parents=True)
                command, environment = hybrid_command(
                    row,
                    repo_root,
                    action="build",
                    output=build_output,
                )
                run_measured(command, environment, repo_root=repo_root)
            output.mkdir(parents=True)
            command, environment = hybrid_command(
                row,
                repo_root,
                action="query",
                output=output,
            )
            run_measured(command, environment, repo_root=repo_root)
        elif row["adapter"] in {
            "borsuk_filter",
            "borsuk_namespace",
            "borsuk_late_interaction",
        }:
            if build_index:
                build_output = output.parents[2] / "build"
                if build_output.exists():
                    raise FileExistsError(
                        f"refusing to overwrite specialized build output {build_output}"
                    )
                build_output.mkdir(parents=True)
                command, environment = specialized_command(
                    row,
                    repo_root,
                    action="build",
                    output=build_output,
                )
                run_measured(command, environment, repo_root=repo_root)
                validate_specialized_output(row["adapter"], "build", build_output)
            output.mkdir(parents=True)
            command, environment = specialized_command(
                row,
                repo_root,
                action="query",
                output=output,
            )
            run_measured(command, environment, repo_root=repo_root)
            validate_specialized_output(row["adapter"], "query", output)
        else:
            raise NotImplementedError(
                f"adapter {row['adapter']!r} must be implemented before executing {row['dataset']}"
            )
        built_indexes.add(row["index_uri"])


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)
    plan = subparsers.add_parser("plan")
    plan.add_argument(
        "--matrix",
        type=Path,
        default=Path("docs/research/market-benchmark-matrix.csv"),
    )
    plan.add_argument("--dataset-root", type=Path, required=True)
    plan.add_argument("--output-root", type=Path, required=True)
    plan.add_argument("--plan", type=Path, required=True)
    plan.add_argument("--run-id", required=True)
    plan.add_argument("--repetitions", type=int, default=3)
    plan.add_argument("--datasets", nargs="*")
    plan.add_argument("--bucket", required=True)
    execute = subparsers.add_parser("execute")
    execute.add_argument("--plan", type=Path, required=True)
    execute.add_argument("--repo-root", type=Path, default=Path.cwd())
    execute.add_argument("--allow-paid-execution", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.command == "plan":
        rows = build_plan(
            matrix_path=args.matrix,
            dataset_root=args.dataset_root,
            output_root=args.output_root,
            run_id=args.run_id,
            repetitions=args.repetitions,
            selected_datasets=set(args.datasets) if args.datasets else None,
            bucket=args.bucket,
        )
        write_plan(args.plan, rows)
        ready = sum(row["status"] == "ready" for row in rows)
        blocked = len(rows) - ready
        print(f"wrote {args.plan}: ready={ready} blocked={blocked}")
        return 0 if blocked == 0 else 2
    execute_plan(
        args.plan,
        repo_root=args.repo_root.resolve(),
        allow_paid_execution=args.allow_paid_execution,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
