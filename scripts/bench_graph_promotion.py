#!/usr/bin/env python3
"""Plan and execute the evidence-gated graph-default benchmark matrix."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import subprocess
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterable

PUBLIC_QUERY_COUNT = 100
PRODUCTION_CONCURRENCY = "1,2,4,8,16"
PRODUCTION_QUERY_CAP = 4
PRODUCTION_DECODE_CAP = 24
PRODUCTION_RAM_BUDGET_BYTES = 8 * 1024**3
# Keep decoded cells out of the cache-state benchmark. A non-zero decoded
# segment cache turns graph-free PQ search into full-cell decode and also makes
# the repeated phase partly an in-process memory-cache measurement instead of
# the promised local-disk measurement. Overlapping callers are still
# single-flighted and globally decode-capped by the engine. The separate
# memory-preloaded profile exercises decoded retention explicitly.
PRODUCTION_SEGMENT_CACHE_BYTES = 0
AWS_REGION = "eu-central-1"


@dataclass(frozen=True)
class DatasetSpec:
    name: str
    dimensions: int
    metric: str
    segment_rows: int
    probes: tuple[int, ...]
    candidates: tuple[int, ...]
    recall_target: float


@dataclass(frozen=True)
class MatrixRow:
    dataset_kind: str
    dataset: str
    method: str
    index_capability: str
    index_variant: str
    repetitions: int
    index_uri: str
    segment_rows: int
    probes: tuple[int, ...]
    candidates: tuple[int, ...]
    recall_target: float


@dataclass(frozen=True)
class SelectedPoint:
    nprobe: int
    max_candidates: int
    recall_at_10: float
    p50_ms: float
    p95_ms: float
    p99_ms: float
    meets_target: bool
    full_cell_scan_excluded: bool


PUBLIC_DATASETS = (
    DatasetSpec(
        "fashion-mnist-784",
        784,
        "euclidean",
        512,
        (4, 6, 8, 16, 32, 64),
        (8, 11, 16, 32, 64, 128, 256, 512, 1024, 2048, 2560, 4096),
        0.989,
    ),
    DatasetSpec(
        "glove-100",
        100,
        "cosine",
        4096,
        (32, 64, 80, 96, 128, 160, 256),
        (8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096),
        0.951,
    ),
    DatasetSpec(
        "sift-128",
        128,
        "euclidean",
        4096,
        (8, 16, 32, 64, 128),
        (8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096),
        0.969,
    ),
    DatasetSpec(
        "nytimes-256",
        256,
        "cosine",
        2048,
        (32, 64, 72, 96, 128, 160),
        (8, 16, 32, 64, 128, 256, 512, 1024, 2048),
        0.959,
    ),
    DatasetSpec(
        "gist-960",
        960,
        "euclidean",
        512,
        (64, 128, 144, 160, 192, 256),
        (8, 16, 32, 64, 128, 256, 384, 512),
        0.967,
    ),
    DatasetSpec(
        "deep-image-96",
        96,
        "cosine",
        4096,
        (32, 64, 96, 128, 256, 512),
        (8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096),
        0.956,
    ),
)

CONTROLLED_DATASETS = (
    "sklearn-digits",
    "synthetic-uniform",
    "synthetic-clustered",
    "synthetic-adversarial",
)
SUCCESS_MARKER = "_SUCCESS"


def plan_matrix(bucket: str, repetitions: int) -> list[MatrixRow]:
    prefix = bucket.rstrip("/") + "/graph-default-promotion-2026-07-21"
    rows: list[MatrixRow] = []
    for spec in PUBLIC_DATASETS:
        for method, capability, variant in (
            ("pq-scan", "pq-scan-only", "pq-scan-only"),
            ("flat-scan", "pq-scan-only", "pq-scan-only"),
            ("pq-scan", "graph-enabled", "graph-enabled"),
            ("graph", "graph-enabled", "graph-enabled"),
        ):
            rows.append(
                MatrixRow(
                    dataset_kind="public",
                    dataset=spec.name,
                    method=method,
                    index_capability=capability,
                    index_variant=variant,
                    repetitions=repetitions,
                    index_uri=f"{prefix}/{spec.name}/{variant}",
                    segment_rows=spec.segment_rows,
                    probes=spec.probes,
                    candidates=spec.candidates,
                    recall_target=spec.recall_target,
                )
            )
    for dataset in CONTROLLED_DATASETS:
        rows.append(
            MatrixRow(
                dataset_kind="controlled",
                dataset=dataset,
                method="all",
                index_capability="graph-enabled",
                index_variant="controlled-local",
                repetitions=repetitions,
                index_uri="local-temporary-index",
                segment_rows=256,
                probes=(8,),
                candidates=(64,),
                recall_target=0.0,
            )
        )
    return rows


def validate_execution(bucket: str, execute: bool) -> None:
    if execute and (not bucket.startswith("s3://") or bucket == "s3://dry-run"):
        raise ValueError("paid execution requires a real s3 bucket")


def select_recall_matched(
    path: Path,
    recall_target: float,
    max_candidates_exclusive: int | None = None,
    full_cell_scan_excluded: bool | None = None,
) -> SelectedPoint:
    with path.open(newline="") as handle:
        rows = list(csv.DictReader(handle))
    # New v8 sweeps emit both honest cache states. Configuration selection uses
    # one explicit state instead of accidentally preferring whichever row
    # inherited a warm cache from sweep order. Legacy files have no phase and
    # retain their historical behavior.
    approximate = [
        row
        for row in rows
        if row.get("mode") != "exact" and row.get("phase", "") in ("", "disk_cached")
    ]
    if max_candidates_exclusive is not None:
        approximate = [
            row
            for row in approximate
            if int(row["max_candidates"]) < max_candidates_exclusive
        ]
    if not approximate:
        raise ValueError(f"{path}: contains no eligible approximate result rows")
    qualified = [
        row for row in approximate if float(row["recall_at_10"]) >= recall_target
    ]
    meets_target = bool(qualified)
    if qualified:
        row = min(
            qualified,
            key=lambda item: (
                float(item["p95_ms"]),
                int(item["nprobe"]),
                int(item["max_candidates"]),
            ),
        )
    else:
        row = min(
            approximate,
            key=lambda item: (
                -float(item["recall_at_10"]),
                float(item["p95_ms"]),
                int(item["nprobe"]),
                int(item["max_candidates"]),
            ),
        )
    return SelectedPoint(
        nprobe=int(row["nprobe"]),
        max_candidates=int(row["max_candidates"]),
        recall_at_10=float(row["recall_at_10"]),
        p50_ms=float(row["p50_ms"]),
        p95_ms=float(row["p95_ms"]),
        p99_ms=float(row["p99_ms"]),
        meets_target=meets_target,
        full_cell_scan_excluded=(
            max_candidates_exclusive is not None
            if full_cell_scan_excluded is None
            else full_cell_scan_excluded
        ),
    )


def public_environment(
    row: MatrixRow,
    dataset_dir: Path,
    output_dir: Path,
    cache_dir: Path,
) -> dict[str, str]:
    run_key = hashlib.sha256(str(output_dir).encode()).hexdigest()[:16]
    return {
        "AWS_REGION": AWS_REGION,
        "AWS_DEFAULT_REGION": AWS_REGION,
        "BORSUK_BENCH_DATASET": str(dataset_dir),
        "BORSUK_BENCH_URI": f"{row.index_uri.rstrip('/')}/fresh/{run_key}",
        "BORSUK_BENCH_CACHE": str(cache_dir),
        "BORSUK_BENCH_OUTPUT_DIR": str(output_dir),
        "BORSUK_BENCH_SEGMENT_MAX": str(row.segment_rows),
        "BORSUK_BENCH_LEAF_CAPABILITY": row.index_capability,
        "BORSUK_BENCH_RECALL_LEAF_MODE": row.method,
        "BORSUK_BENCH_SERVING_MODE": "hybrid",
        "BORSUK_BENCH_SERVING_LEAF_MODE": row.method,
        "BORSUK_BENCH_NPROBES": ",".join(map(str, row.probes)),
        "BORSUK_BENCH_CANDIDATES": ",".join(map(str, row.candidates)),
        "BORSUK_BENCH_QUERIES": str(PUBLIC_QUERY_COUNT),
        "BORSUK_BENCH_UNCACHED_QUERIES": str(PUBLIC_QUERY_COUNT),
        "BORSUK_BENCH_CONCURRENCY": PRODUCTION_CONCURRENCY,
        "BORSUK_BENCH_MAX_CONCURRENT_SEARCHES": str(PRODUCTION_QUERY_CAP),
        "BORSUK_BENCH_MAX_CONCURRENT_CELL_DECODES": str(PRODUCTION_DECODE_CAP),
        "BORSUK_BENCH_RAM_BUDGET_BYTES": str(PRODUCTION_RAM_BUDGET_BYTES),
        "BORSUK_BENCH_SEGMENT_CACHE_MAX_BYTES": str(PRODUCTION_SEGMENT_CACHE_BYTES),
        "BORSUK_BENCH_READ_ONLY": "1",
        "BORSUK_BENCH_SKIP_EXACT_RECALL": "1",
    }


def write_manifest(
    rows: Iterable[MatrixRow], output_root: Path, source_sha: str
) -> Path:
    materialized = list(rows)
    if not materialized:
        raise ValueError("promotion matrix must contain at least one row")
    output_root.mkdir(parents=True, exist_ok=True)
    path = output_root / "matrix.csv"
    columns = tuple(asdict(materialized[0]).keys())
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=(*columns, "source_sha", "status"))
        writer.writeheader()
        for row in materialized:
            values = asdict(row)
            values["probes"] = ";".join(map(str, row.probes))
            values["candidates"] = ";".join(map(str, row.candidates))
            writer.writerow({**values, "source_sha": source_sha, "status": "planned"})
    return path


def run_logged(command: list[str], env: dict[str, str], output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    marker = output_dir / SUCCESS_MARKER
    marker.unlink(missing_ok=True)
    (output_dir / "command.json").write_text(
        json.dumps({"command": command, "environment": env}, indent=2, sort_keys=True)
        + "\n"
    )
    with (
        (output_dir / "stdout.log").open("w") as stdout,
        (output_dir / "stderr.log").open("w") as stderr,
    ):
        completed = subprocess.run(
            command,
            env={**os.environ, **env},
            text=True,
            stdout=stdout,
            stderr=stderr,
        )
    if completed.returncode:
        raise RuntimeError(f"benchmark failed ({completed.returncode}): {output_dir}")
    marker.write_text("complete\n")


def is_complete(output_dir: Path) -> bool:
    return (output_dir / SUCCESS_MARKER).is_file()


def sampled_benchmark(env: dict[str, str], output_dir: Path) -> None:
    run_logged(
        [
            sys.executable,
            "scripts/benchmark_with_resources.py",
            "--output",
            str(output_dir / "resources.csv"),
            "--cache-dir",
            env["BORSUK_BENCH_CACHE"],
            "--scratch-dir",
            str(output_dir / "scratch"),
            "--",
            "target/release/examples/production_bench",
        ],
        env,
        output_dir,
    )


def run_public_row(
    row: MatrixRow,
    datasets_root: Path,
    output_root: Path,
    _build_index: bool,
    resume: bool,
    source_sha: str,
) -> None:
    row_root = output_root / "public" / row.dataset / row.index_variant / row.method
    sweep_dir = row_root / "sweep"
    sweep_csv = sweep_dir / "bench_recall_latency.csv"
    if not (resume and is_complete(sweep_dir) and sweep_csv.is_file()):
        cache_dir = sweep_dir / "cache"
        env = public_environment(
            row,
            datasets_root / row.dataset,
            sweep_dir,
            cache_dir,
        )
        env["BORSUK_BENCH_RECALL_ONLY"] = "1"
        sampled_benchmark(env, sweep_dir)
    point = select_recall_matched(
        sweep_csv,
        row.recall_target,
        max_candidates_exclusive=(
            row.segment_rows if row.method == "graph" else row.segment_rows + 1
        ),
        full_cell_scan_excluded=row.method == "graph",
    )
    (row_root / "selected.json").write_text(
        json.dumps(
            {**asdict(point), "source_sha": source_sha}, indent=2, sort_keys=True
        )
        + "\n"
    )

    for profile, repetitions in (
        ("production", row.repetitions),
        ("memory-preloaded", row.repetitions),
        ("research-uncapped", 1),
    ):
        for repetition in range(1, repetitions + 1):
            run_dir = row_root / profile / f"run-{repetition}"
            if (
                resume
                and is_complete(run_dir)
                and (run_dir / "bench_cache_states.csv").is_file()
            ):
                continue
            env = public_environment(
                row,
                datasets_root / row.dataset,
                run_dir,
                run_dir / "cache",
            )
            env.update(
                {
                    "BORSUK_BENCH_SKIP_RECALL": "1",
                    "BORSUK_BENCH_SERVING_NPROBE": str(point.nprobe),
                    "BORSUK_BENCH_SERVING_CANDIDATES": str(point.max_candidates),
                }
            )
            if profile == "memory-preloaded":
                env["BORSUK_BENCH_PRELOAD_SERVING"] = "1"
            if profile == "research-uncapped":
                env["BORSUK_BENCH_MAX_CONCURRENT_SEARCHES"] = "0"
                env["BORSUK_BENCH_MAX_CONCURRENT_CELL_DECODES"] = "0"
            sampled_benchmark(env, run_dir)


def run_controlled(output_root: Path, resume: bool) -> None:
    profiles = (
        (64, "10000,100000,1000000"),
        (96, "10000"),
        (256, "10000"),
        (784, "10000"),
        (960, "10000"),
    )
    for dimensions, record_counts in profiles:
        run_dir = output_root / "controlled" / f"d{dimensions}"
        if resume and is_complete(run_dir) and (run_dir / "sequential.csv").is_file():
            continue
        run_logged(
            [
                sys.executable,
                "scripts/benchmark_with_resources.py",
                "--output",
                str(run_dir / "resources.csv"),
                "--",
                "target/release/examples/benchmark_report",
                "--synthetic-records-list",
                record_counts,
                "--dimensions",
                str(dimensions),
                "--segment-max-vectors",
                "256",
                "--max-segments",
                "8",
                "--routing-page-overfetch",
                "8",
                "--max-candidates-per-segment",
                "64",
                "--queries",
                "100",
                "--parallelism",
                "1,2,4,8,16",
                "--artifacts-dir",
                str(run_dir),
            ],
            {},
            run_dir,
        )


PUBLIC_RESULT_COLUMNS = (
    "dataset",
    "method",
    "index_capability",
    "index_variant",
    "profile",
    "cache_state",
    "repetition",
    "queries",
    "recall_at_10",
    "meets_target",
    "nprobe",
    "max_candidates",
    "segment_max_vectors",
    "p50_ms",
    "p95_ms",
    "p99_ms",
    "max_ms",
    "qps",
    "peak_cpu_percent",
    "peak_rss_bytes",
    "ram_budget_bytes",
    "max_concurrent_searches",
    "max_concurrent_cell_decodes",
    "network_gets",
    "network_bytes",
    "logical_bytes_read",
    "cache_disk_bytes",
    "process_read_bytes",
    "process_write_bytes",
    "source_sha",
    "index_uri",
)


def _csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="") as handle:
        return list(csv.DictReader(handle))


def consolidate_public_results(
    output_root: Path,
    matrix: Iterable[MatrixRow],
    source_sha: str,
    output: Path,
) -> None:
    consolidated: list[dict[str, str]] = []
    for row in matrix:
        if row.dataset_kind != "public":
            continue
        row_root = output_root / "public" / row.dataset / row.index_variant / row.method
        selected_path = row_root / "selected.json"
        if not selected_path.is_file():
            continue
        selected = json.loads(selected_path.read_text())
        for profile, repetitions in (
            ("production", row.repetitions),
            ("memory-preloaded", row.repetitions),
            ("research-uncapped", 1),
        ):
            for repetition in range(1, repetitions + 1):
                run_dir = row_root / profile / f"run-{repetition}"
                required = (
                    run_dir / "command.json",
                    run_dir / "bench_cache_states.csv",
                    run_dir / "bench_concurrency.csv",
                    run_dir / "resources.csv",
                )
                if not all(path.is_file() for path in required):
                    continue
                command = json.loads(required[0].read_text())
                environment = command["environment"]
                cache_rows = _csv_rows(required[1])
                concurrency_rows = _csv_rows(required[2])
                concurrency = next(
                    (item for item in concurrency_rows if int(item["workers"]) == 4),
                    max(concurrency_rows, key=lambda item: int(item["workers"])),
                )
                resources = _csv_rows(required[3])
                peak_cpu = max(
                    (float(item["cpu_percent"]) for item in resources), default=0.0
                )
                peak_rss = max(
                    (int(item["rss_bytes"]) for item in resources), default=0
                )
                cache_disk = max(
                    (int(item["cache_disk_bytes"]) for item in resources), default=0
                )
                process_read = max(
                    (int(item["process_read_bytes"]) for item in resources), default=0
                )
                process_write = max(
                    (int(item["process_write_bytes"]) for item in resources), default=0
                )
                for cache in cache_rows:
                    queries = int(cache["queries"])
                    avg_network_gets = float(cache["avg_network_gets"])
                    logical_bytes_read = float(cache["avg_bytes_read"])
                    # Cache-state trials are deliberately binary: strict
                    # uncached starts from an empty data cache, while
                    # disk_cached must issue zero backing GETs. `bytes_read` is
                    # logical I/O and therefore includes local-disk bytes; it is
                    # network traffic only in a phase that performed GETs.
                    avg_network_bytes = (
                        logical_bytes_read if avg_network_gets > 0 else 0.0
                    )
                    consolidated.append(
                        {
                            "dataset": row.dataset,
                            "method": row.method,
                            "index_capability": row.index_capability,
                            "index_variant": row.index_variant,
                            "profile": profile,
                            "cache_state": cache["phase"],
                            "repetition": str(repetition),
                            "queries": str(queries),
                            "recall_at_10": f"{float(selected['recall_at_10']):.6f}",
                            "meets_target": str(bool(selected["meets_target"])).lower(),
                            "nprobe": str(int(selected["nprobe"])),
                            "max_candidates": str(int(selected["max_candidates"])),
                            "segment_max_vectors": str(row.segment_rows),
                            "p50_ms": f"{float(cache['p50_ms']):.3f}",
                            "p95_ms": f"{float(cache['p95_ms']):.3f}",
                            "p99_ms": f"{float(cache['p99_ms']):.3f}",
                            "max_ms": f"{float(cache['max_ms']):.3f}",
                            "qps": f"{float(concurrency['qps']):.3f}",
                            "peak_cpu_percent": f"{peak_cpu:.3f}",
                            "peak_rss_bytes": str(peak_rss),
                            "ram_budget_bytes": environment[
                                "BORSUK_BENCH_RAM_BUDGET_BYTES"
                            ],
                            "max_concurrent_searches": environment[
                                "BORSUK_BENCH_MAX_CONCURRENT_SEARCHES"
                            ],
                            "max_concurrent_cell_decodes": environment[
                                "BORSUK_BENCH_MAX_CONCURRENT_CELL_DECODES"
                            ],
                            "network_gets": f"{avg_network_gets:.3f}",
                            "network_bytes": f"{avg_network_bytes:.3f}",
                            "logical_bytes_read": f"{logical_bytes_read:.3f}",
                            "cache_disk_bytes": str(cache_disk),
                            "process_read_bytes": str(process_read),
                            "process_write_bytes": str(process_write),
                            "source_sha": source_sha,
                            "index_uri": row.index_uri,
                        }
                    )
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=PUBLIC_RESULT_COLUMNS)
        writer.writeheader()
        writer.writerows(consolidated)


def execute(args: argparse.Namespace, rows: list[MatrixRow]) -> None:
    built_variants: set[tuple[str, str]] = set()
    for row in rows:
        if row.dataset_kind != "public":
            continue
        variant = (row.dataset, row.index_variant)
        build = args.build_indexes and variant not in built_variants
        run_public_row(
            row,
            args.datasets_root,
            args.output_root,
            build,
            args.resume,
            args.source_sha,
        )
        built_variants.add(variant)
    run_controlled(args.output_root, args.resume)
    consolidate_public_results(
        args.output_root,
        rows,
        args.source_sha,
        args.output_root / "public-results.csv",
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--datasets-root", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--source-sha", default="unrecorded")
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--build-indexes", action="store_true")
    parser.add_argument("--resume", action="store_true")
    args = parser.parse_args()
    if args.repetitions < 1:
        parser.error("--repetitions must be positive")
    return args


def main() -> int:
    args = parse_args()
    try:
        validate_execution(args.bucket, args.execute)
    except ValueError as error:
        print(error, file=sys.stderr)
        return 2
    rows = plan_matrix(args.bucket, args.repetitions)
    manifest = write_manifest(rows, args.output_root, args.source_sha)
    print(f"wrote {manifest} rows={len(rows)}")
    if not args.execute:
        print("dry run only; pass --execute and --build-indexes for paid AWS execution")
        return 0
    execute(args, rows)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
