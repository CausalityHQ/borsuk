#!/usr/bin/env python3
"""Fail-closed decision validator for the terminal Cohere GET-admission campaign."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import statistics
import sys
from collections.abc import Iterable
from pathlib import Path

from validate_benchmark_artifacts import validate_directory


class ValidationError(RuntimeError):
    """The artifact set is incomplete, inconsistent, or not the frozen campaign."""


FROZEN_PROTOCOL: dict[str, object] = {
    "protocol": "physical-get-admission-cohere-1m-aws-v1",
    "campaign_id": "cohere1m-ac4a68d-v1",
    "source_commit": "ac4a68da5a19ead15f896d7225244cea457d73a4",
    "source_archive_sha256": "78e62074a7868302cb8bd1fe6ae74814419be784c594fe2baea8bf71cd4b99c2",
    "dataset_descriptor_sha256": "54c733e39adfcaa9ee10f3ed8bd8e66ada9f8f9a1a73e9753f5c5c2044b79254",
    "dataset": "cohere-medium-1M",
    "corpus_vectors": 1_000_000,
    "dimensions": 768,
    "metric": "cosine",
    "queries_per_cell": 1000,
    "query_seed": 918273645,
    "repetitions": 5,
    "workers": [1, 8, 32],
    "arms": [
        {"name": "production-cap-128", "backing_get_concurrency": 128},
        {"name": "high-cap-control-1024", "backing_get_concurrency": 1024},
    ],
    "arm_order": "alternating by repetition",
    "scan_codec": "srht-pq-scan",
    "nprobe": 32,
    "exact_candidates": 128,
    "cache_profile": "concurrent cold-start per worker point; empty BORSUK disk cache before each 1/8/32 wave, with ordinary shared population during that wave",
    "max_active_searches": 32,
    "max_inflight_leaf_reads": 128,
    "serving_prefetch_depth": 16,
    "ram_budget_bytes": 536_870_912,
    "segment_cache_max_bytes": 0,
    "global_graph_cache_max_bytes": 0,
    "quality_gates": {"mean_recall_at_10": 0.95, "query_p05_recall_at_10": 0.80},
    "latency_rejection_ceiling_p95_ms": 200.0,
    "selection_rule": "Admission is overload protection, not a latency claim. Compare paired arm/repetition cells at identical worker counts. Reject any cell failing quality, p95 ceiling, structural integrity, truthful request/byte accounting, or bounded resources. Among valid cells prefer the lowest latency and highest QPS without hiding reads behind cache.",
    "evidence_boundary": "V9 Cohere concurrency qualification only. It cannot promote the read architecture, establish 100M scale, modality parity, write performance, or competitor parity. V10 bounded Arrow leaf routing remains required.",
}

EXPECTED_ENVIRONMENT = {
    "campaign_id": "cohere1m-ac4a68d-v1",
    "source_commit": "ac4a68da5a19ead15f896d7225244cea457d73a4",
    "source_archive_sha256": "78e62074a7868302cb8bd1fe6ae74814419be784c594fe2baea8bf71cd4b99c2",
    "dataset_descriptor_sha256": "54c733e39adfcaa9ee10f3ed8bd8e66ada9f8f9a1a73e9753f5c5c2044b79254",
    "instance_id": "i-0641c0333f007a30f",
    "instance_type": "c7g.8xlarge",
    "region": "eu-central-1",
    "architecture": "aarch64",
    "runner_sha256": "b66406d223c88d75cfdf0848713bb84438292a20b1ecdb77855b300470d3d5c2",
    "binary_sha256": "5397c07417a0230bca52e67cc4799ea2b4f3714e57c09b883f35b3cae3e2fdd3",
}

CASE_REQUIRED = (
    "bench_startup.csv",
    "bench_cache_states.csv",
    "bench_concurrency.csv",
    "bench_concurrency_samples.csv",
    "resources.csv",
)


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ValidationError(message)


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="") as handle:
        return list(csv.DictReader(handle))


def read_key_values(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for line in path.read_text().splitlines():
        key, separator, value = line.partition("=")
        require(bool(separator) and bool(key), f"invalid environment line in {path}")
        require(key not in values, f"duplicate environment key {key} in {path}")
        values[key] = value
    return values


def integer(row: dict[str, str], key: str, path: Path) -> int:
    try:
        value = int(row[key])
    except (KeyError, ValueError) as error:
        raise ValidationError(f"invalid integer {key} in {path}") from error
    require(value >= 0, f"negative {key} in {path}")
    return value


def floating(row: dict[str, str], key: str, path: Path) -> float:
    try:
        value = float(row[key])
    except (KeyError, ValueError) as error:
        raise ValidationError(f"invalid number {key} in {path}") from error
    require(math.isfinite(value), f"non-finite {key} in {path}")
    return value


def nearest_rank(values: Iterable[float], quantile: float) -> float:
    ordered = sorted(values)
    require(bool(ordered), "cannot compute percentile of no samples")
    index = max(0, math.ceil(len(ordered) * quantile) - 1)
    return ordered[min(index, len(ordered) - 1)]


def expected_schedule() -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for repetition_number in range(1, 6):
        repetition = f"r{repetition_number:02d}"
        arms = (
            ("production-cap-128", "high-cap-control-1024")
            if repetition_number % 2
            else ("high-cap-control-1024", "production-cap-128")
        )
        for position, arm in enumerate(arms):
            rows.append(
                {
                    "repetition": repetition,
                    "position": str(position),
                    "arm": arm,
                    "backing_get_concurrency": "128"
                    if arm.startswith("production")
                    else "1024",
                    "workers": "1;8;32",
                }
            )
    return rows


def validate_terminal_markers(root: Path) -> None:
    require(
        (root / "RAW_MEASUREMENTS_COMPLETE").is_file(),
        "root terminal marker RAW_MEASUREMENTS_COMPLETE is missing",
    )
    require(
        not (root / "CAMPAIGN_FAILED").exists(), "campaign failure marker is present"
    )
    failure_markers = sorted(root.glob("results/r*/*/CASE_FAILED"))
    require(
        not failure_markers,
        f"case failure marker is present: {failure_markers[0] if failure_markers else ''}",
    )


def validate_identity(root: Path, verify_payload_hashes: bool) -> None:
    protocol_path = root / "protocol.json"
    require(protocol_path.is_file(), "protocol.json is missing")
    try:
        protocol = json.loads(protocol_path.read_text())
    except (OSError, json.JSONDecodeError) as error:
        raise ValidationError("protocol.json is invalid") from error
    require(protocol == FROZEN_PROTOCOL, "protocol differs from the frozen campaign")
    schedule_path = root / "schedule.csv"
    require(schedule_path.is_file(), "schedule.csv is missing")
    require(
        read_csv(schedule_path) == expected_schedule(),
        "campaign schedule differs from the frozen alternating order",
    )

    environment_path = root / "environment.txt"
    require(environment_path.is_file(), "environment.txt is missing")
    environment = read_key_values(environment_path)
    for key, expected in EXPECTED_ENVIRONMENT.items():
        require(
            environment.get(key) == expected, f"environment identity mismatch for {key}"
        )

    if verify_payload_hashes:
        for name, expected in (
            ("source.tar", EXPECTED_ENVIRONMENT["source_archive_sha256"]),
            ("campaign.sh", EXPECTED_ENVIRONMENT["runner_sha256"]),
        ):
            path = root / name
            require(path.is_file(), f"identity payload {name} is missing")
            require(
                sha256(path) == expected,
                f"identity payload checksum mismatch for {name}",
            )


def validate_summary_against_samples(
    summary: dict[str, str], samples: list[dict[str, str]], path: Path
) -> None:
    latencies = [floating(row, "latency_ms", path) for row in samples]
    observed = {
        "mean_ms": statistics.fmean(latencies),
        "p50_ms": nearest_rank(latencies, 0.50),
        "p95_ms": nearest_rank(latencies, 0.95),
        "p99_ms": nearest_rank(latencies, 0.99),
        "max_ms": max(latencies),
    }
    for key, expected in observed.items():
        reported = floating(summary, key, path)
        require(
            math.isclose(reported, expected, abs_tol=0.0011),
            f"{path} {key} does not reconcile with raw samples: reported={reported}, raw={expected}",
        )


def validate_historical_benchmark_schema(
    rows: list[dict[str, str]], path: Path
) -> None:
    require(bool(rows), f"{path} contains no benchmark rows")
    require(
        all("schema_version" not in row for row in rows),
        f"{path} uses a versioned production benchmark schema; the frozen unversioned V9 "
        "campaign cannot be compared with V10 output",
    )


def validate(root: Path, *, verify_payload_hashes: bool = True) -> dict[str, object]:
    # This guard must remain before every measurement CSV read.
    validate_terminal_markers(root)
    validate_identity(root, verify_payload_hashes)

    build_root = root / "build"
    require(
        (build_root / "BUILD_COMPLETE").is_file(), "build completion marker is missing"
    )
    try:
        validate_directory(
            build_root, "srht-pq-scan", ("bench_build.csv", "resources.csv")
        )
    except ValueError as error:
        raise ValidationError(str(error)) from error
    build_rows = read_csv(build_root / "bench_build.csv")
    require(len(build_rows) == 1, "bench_build.csv must contain exactly one row")
    require(
        integer(build_rows[0], "records", build_root / "bench_build.csv") == 1_000_000,
        "build record count changed",
    )
    require(
        build_rows[0].get("vector_element_type") == "float32",
        "build vector element type changed",
    )
    build = {
        "records": integer(build_rows[0], "records", build_root / "bench_build.csv"),
        "vector_element_type": build_rows[0]["vector_element_type"],
        "total_active_index_bytes": integer(
            build_rows[0], "total_active_index_bytes", build_root / "bench_build.csv"
        ),
        "bytes_per_vector": floating(
            build_rows[0], "bytes_per_vector", build_root / "bench_build.csv"
        ),
        "ingest_ms": floating(
            build_rows[0], "ingest_ms", build_root / "bench_build.csv"
        ),
    }
    if "compaction_ms" in build_rows[0]:
        build["compaction_ms"] = floating(
            build_rows[0], "compaction_ms", build_root / "bench_build.csv"
        )

    workers = (1, 8, 32)
    arms = ("production-cap-128", "high-cap-control-1024")
    repetitions = tuple(f"r{number:02d}" for number in range(1, 6))
    by_cell: dict[tuple[str, str, int], dict[str, object]] = {}
    paired_rows: dict[tuple[str, int], dict[str, list[dict[str, str]]]] = {}
    failures: list[str] = []
    query_samples = 0
    arm_peak_rss: dict[str, int] = {arm: 0 for arm in arms}

    for repetition in repetitions:
        repetition_root = root / "results" / repetition
        require(
            (repetition_root / "REPETITION_COMPLETE").is_file(),
            f"{repetition} completion marker is missing",
        )
        for arm in arms:
            case_root = repetition_root / arm
            require(
                (case_root / "CASE_COMPLETE").is_file(),
                f"{repetition}/{arm} completion marker is missing",
            )
            try:
                validate_directory(
                    case_root,
                    "srht-pq-scan",
                    CASE_REQUIRED,
                    historical_unversioned=True,
                )
            except ValueError as error:
                raise ValidationError(str(error)) from error
            summaries = read_csv(case_root / "bench_concurrency.csv")
            samples = read_csv(case_root / "bench_concurrency_samples.csv")
            validate_historical_benchmark_schema(
                summaries, case_root / "bench_concurrency.csv"
            )
            validate_historical_benchmark_schema(
                samples, case_root / "bench_concurrency_samples.csv"
            )
            resources = read_csv(case_root / "resources.csv")
            arm_peak_rss[arm] = max(
                arm_peak_rss[arm],
                *(
                    integer(row, "rss_bytes", case_root / "resources.csv")
                    for row in resources
                ),
            )
            require(
                len(summaries) == len(workers),
                f"{repetition}/{arm} must have three concurrency summaries",
            )
            summary_by_worker = {
                integer(row, "workers", case_root / "bench_concurrency.csv"): row
                for row in summaries
            }
            require(
                set(summary_by_worker) == set(workers),
                f"{repetition}/{arm} worker summaries changed",
            )
            for worker in workers:
                cell_samples = [
                    row
                    for row in samples
                    if integer(
                        row, "workers", case_root / "bench_concurrency_samples.csv"
                    )
                    == worker
                ]
                require(
                    len(cell_samples) == 1000,
                    f"{repetition}/{arm}/w{worker} must contain 1000 query samples",
                )
                require(
                    [
                        integer(
                            row,
                            "sample_index",
                            case_root / "bench_concurrency_samples.csv",
                        )
                        for row in cell_samples
                    ]
                    == list(range(1000)),
                    f"{repetition}/{arm}/w{worker} sample indexes are incomplete or reordered",
                )
                require(
                    len(
                        {
                            integer(
                                row,
                                "query_source_index",
                                case_root / "bench_concurrency_samples.csv",
                            )
                            for row in cell_samples
                        }
                    )
                    == 1000,
                    f"{repetition}/{arm}/w{worker} query identities are not unique",
                )
                for row in cell_samples:
                    require(
                        row.get("cache_execution") == "scan",
                        f"{repetition}/{arm}/w{worker} cache execution changed",
                    )
                    require(
                        row.get("cache_profile") == "uncached",
                        f"{repetition}/{arm}/w{worker} cache profile changed",
                    )
                    require(
                        row.get("execution_engine") == "srht-pq-scan",
                        f"{repetition}/{arm}/w{worker} execution engine changed",
                    )
                    require(
                        integer(
                            row,
                            "ram_budget_bytes",
                            case_root / "bench_concurrency_samples.csv",
                        )
                        == 536_870_912,
                        f"{repetition}/{arm}/w{worker} RAM budget changed",
                    )
                    require(
                        integer(
                            row,
                            "network_gets",
                            case_root / "bench_concurrency_samples.csv",
                        )
                        == integer(
                            row,
                            "backing_reads",
                            case_root / "bench_concurrency_samples.csv",
                        ),
                        f"{repetition}/{arm}/w{worker} network_gets do not equal backing_reads",
                    )
                    require(
                        integer(
                            row,
                            "decoded_cache_hits",
                            case_root / "bench_concurrency_samples.csv",
                        )
                        == 0,
                        f"{repetition}/{arm}/w{worker} has decoded cache hits",
                    )
                summary = summary_by_worker[worker]
                validate_summary_against_samples(
                    summary, cell_samples, case_root / "bench_concurrency_samples.csv"
                )
                recalls = [
                    floating(
                        row, "recall_at_10", case_root / "bench_concurrency_samples.csv"
                    )
                    for row in cell_samples
                ]
                mean_recall = statistics.fmean(recalls)
                p05_recall = nearest_rank(recalls, 0.05)
                p95_ms = floating(
                    summary, "p95_ms", case_root / "bench_concurrency.csv"
                )
                qps = floating(summary, "qps", case_root / "bench_concurrency.csv")
                cell_failures: list[str] = []
                if mean_recall < 0.95:
                    cell_failures.append(f"mean recall {mean_recall:.6f} < 0.95")
                if p05_recall < 0.80:
                    cell_failures.append(f"query-p05 recall {p05_recall:.6f} < 0.80")
                if p95_ms >= 200.0:
                    cell_failures.append(f"p95 {p95_ms:.3f} ms is not below 200 ms")
                failures.extend(
                    f"{repetition}/{arm}/w{worker}: {reason}"
                    for reason in cell_failures
                )
                by_cell[(repetition, arm, worker)] = {
                    "p95_ms": p95_ms,
                    "p50_ms": floating(
                        summary, "p50_ms", case_root / "bench_concurrency.csv"
                    ),
                    "qps": qps,
                    "mean_recall_at_10": mean_recall,
                    "query_p05_recall_at_10": p05_recall,
                    "mean_backing_reads": statistics.fmean(
                        integer(
                            row,
                            "backing_reads",
                            case_root / "bench_concurrency_samples.csv",
                        )
                        for row in cell_samples
                    ),
                    "mean_backing_bytes": statistics.fmean(
                        integer(
                            row,
                            "backing_bytes_read",
                            case_root / "bench_concurrency_samples.csv",
                        )
                        for row in cell_samples
                    ),
                    "mean_disk_cache_reads": statistics.fmean(
                        integer(
                            row,
                            "disk_cache_reads",
                            case_root / "bench_concurrency_samples.csv",
                        )
                        for row in cell_samples
                    ),
                    "failures": cell_failures,
                }
                paired_rows.setdefault((repetition, worker), {})[arm] = cell_samples
                query_samples += len(cell_samples)

    for (repetition, worker), pairs in paired_rows.items():
        require(
            set(pairs) == set(arms),
            f"{repetition}/w{worker} paired arms are incomplete",
        )
        production = pairs["production-cap-128"]
        control = pairs["high-cap-control-1024"]
        for position, (production_row, control_row) in enumerate(
            zip(production, control, strict=True)
        ):
            require(
                production_row["query_source_index"]
                == control_row["query_source_index"],
                f"{repetition}/w{worker} paired query identity divergence at sample {position}",
            )
            require(
                production_row["recall_at_10"] == control_row["recall_at_10"],
                f"{repetition}/w{worker} paired recall divergence at sample {position}",
            )

    summaries: list[dict[str, object]] = []
    for arm in arms:
        for worker in workers:
            cells = [by_cell[(repetition, arm, worker)] for repetition in repetitions]
            summaries.append(
                {
                    "arm": arm,
                    "workers": worker,
                    "median_p50_ms": round(
                        statistics.median(float(cell["p50_ms"]) for cell in cells), 6
                    ),
                    "median_p95_ms": round(
                        statistics.median(float(cell["p95_ms"]) for cell in cells), 6
                    ),
                    "median_qps": round(
                        statistics.median(float(cell["qps"]) for cell in cells), 6
                    ),
                    "worst_repetition_p95_ms": round(
                        max(float(cell["p95_ms"]) for cell in cells), 6
                    ),
                    "mean_recall_at_10": round(
                        statistics.fmean(
                            float(cell["mean_recall_at_10"]) for cell in cells
                        ),
                        6,
                    ),
                    "query_p05_recall_at_10": round(
                        min(float(cell["query_p05_recall_at_10"]) for cell in cells), 6
                    ),
                    "mean_backing_reads": round(
                        statistics.fmean(
                            float(cell["mean_backing_reads"]) for cell in cells
                        ),
                        6,
                    ),
                    "mean_backing_bytes": round(
                        statistics.fmean(
                            float(cell["mean_backing_bytes"]) for cell in cells
                        ),
                        6,
                    ),
                    "mean_disk_cache_reads": round(
                        statistics.fmean(
                            float(cell["mean_disk_cache_reads"]) for cell in cells
                        ),
                        6,
                    ),
                }
            )

    paired_differences = []
    for worker in workers:
        production_cells = [
            by_cell[(repetition, "production-cap-128", worker)]
            for repetition in repetitions
        ]
        control_cells = [
            by_cell[(repetition, "high-cap-control-1024", worker)]
            for repetition in repetitions
        ]
        paired_differences.append(
            {
                "workers": worker,
                "production_minus_control_p95_ms": [
                    round(float(production["p95_ms"]) - float(control["p95_ms"]), 6)
                    for production, control in zip(
                        production_cells, control_cells, strict=True
                    )
                ],
                "production_minus_control_qps": [
                    round(float(production["qps"]) - float(control["qps"]), 6)
                    for production, control in zip(
                        production_cells, control_cells, strict=True
                    )
                ],
            }
        )

    accepted = not failures
    return {
        "status": "accepted" if accepted else "valid-rejected",
        "accepted": accepted,
        "campaign_id": FROZEN_PROTOCOL["campaign_id"],
        "source_commit": FROZEN_PROTOCOL["source_commit"],
        "dataset_descriptor_sha256": FROZEN_PROTOCOL["dataset_descriptor_sha256"],
        "cells": len(by_cell),
        "query_samples": query_samples,
        "build": build,
        "arm_peak_rss_bytes": arm_peak_rss,
        "summaries": summaries,
        "paired_differences": paired_differences,
        "failures": failures,
        "evidence_boundary": FROZEN_PROTOCOL["evidence_boundary"],
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--decision", type=Path)
    args = parser.parse_args()
    try:
        decision = validate(args.root)
    except (OSError, ValidationError) as error:
        print(f"invalid physical GET admission campaign: {error}", file=sys.stderr)
        return 2
    encoded = json.dumps(decision, indent=2, sort_keys=True) + "\n"
    if args.decision:
        args.decision.write_text(encoded)
    print(encoded, end="")
    return 0 if decision["accepted"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
