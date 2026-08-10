#!/usr/bin/env python3
"""Assemble query, build, storage, CPU, and RSS layout evidence."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import re
from pathlib import Path
from typing import Any, Sequence

try:
    from scripts.freeze_layout_dataset_identity import validate_manifest
except ModuleNotFoundError:
    from freeze_layout_dataset_identity import validate_manifest

FIELDS = [
    "dataset",
    "backend",
    "arm",
    "repetition_id",
    "query_position",
    "query_source_index",
    "latency_ms",
    "recall_at_10",
    "physical_requests",
    "bytes_read",
    "build_ms",
    "segment_bytes",
    "total_active_index_bytes",
    "peak_rss_bytes",
    "cpu_core_ms",
    "status",
]


def _read_rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="") as handle:
        return list(csv.DictReader(handle))


def _read_key_values(path: Path) -> dict[str, str]:
    try:
        lines = path.read_text().splitlines()
    except FileNotFoundError as error:
        raise ValueError(f"missing evidence file: {path}") from error
    values: dict[str, str] = {}
    for line in lines:
        if not line or "=" not in line:
            raise ValueError(f"invalid key-value evidence in {path}")
        key, value = line.split("=", 1)
        if not key or key in values:
            raise ValueError(f"duplicate or empty evidence key in {path}")
        values[key] = value
    return values


def _source_sha(root: Path) -> str:
    source_sha = _read_key_values(root / "environment.txt").get("source_sha256", "")
    if not re.fullmatch(r"[0-9a-fA-F]{64}", source_sha):
        raise ValueError("environment.txt has no valid source_sha256")
    return source_sha.lower()


def _dataset_identity_sha(root: Path, protocol: dict[str, Any]) -> str | None:
    if not protocol.get("dataset_contracts"):
        return None
    path = root / "dataset-identities.json"
    try:
        payload = path.read_bytes()
        manifest = json.loads(payload)
    except (FileNotFoundError, json.JSONDecodeError) as error:
        raise ValueError("missing or invalid dataset identity manifest") from error
    validate_manifest(manifest, protocol)
    digest = hashlib.sha256(payload).hexdigest()
    environment_digest = _read_key_values(root / "environment.txt").get(
        "dataset_identity_sha256", ""
    )
    if environment_digest.lower() != digest:
        raise ValueError("dataset identity manifest does not match environment.txt")
    return digest


def _validate_schedule_contract(
    schedule: Sequence[dict[str, str]], protocol: dict[str, Any]
) -> None:
    datasets = [str(item) for item in protocol["datasets"]]
    backends = [str(item) for item in protocol["backends"]]
    arms = [
        str(protocol["baseline_arm"]),
        *(str(item) for item in protocol["candidate_arms"]),
    ]
    repetitions = int(protocol["repetitions"])
    query_seeds = [int(item) for item in protocol["query_seeds"]]
    if repetitions <= 0 or len(query_seeds) != repetitions:
        raise ValueError(
            "qualification protocol has an invalid repetition/seed contract"
        )

    expected: dict[tuple[str, str, str, str], tuple[str, str, str]] = {}
    for repetition in range(1, repetitions + 1):
        repetition_id = f"r{repetition:02d}"
        query_seed = str(query_seeds[repetition - 1])
        for dataset in datasets:
            for backend in backends:
                for position, arm_index in enumerate(
                    (position + repetition - 1) % len(arms)
                    for position in range(len(arms))
                ):
                    arm = arms[arm_index]
                    key = (repetition_id, dataset, backend, arm)
                    expected[key] = (
                        query_seed,
                        str(position),
                        "/".join(key),
                    )

    actual: dict[tuple[str, str, str, str], tuple[str, str, str]] = {}
    for case in schedule:
        key = tuple(
            case[field] for field in ("repetition_id", "dataset", "backend", "arm")
        )
        if key in actual:
            raise ValueError(
                f"qualification schedule has a duplicate case: {'/'.join(key)}"
            )
        actual[key] = (
            case["query_seed"],
            case["arm_position"],
            case["case_id"],
        )
    if actual != expected:
        raise ValueError(
            "qualification schedule does not exactly match the frozen protocol"
        )


def _validate_case_proof(
    root: Path,
    case: dict[str, str],
    expected_source_sha: str,
    expected_dataset_identity: str | None = None,
) -> None:
    expected_case_id = "/".join(
        case[field] for field in ("repetition_id", "dataset", "backend", "arm")
    )
    if case["case_id"] != expected_case_id:
        raise ValueError(
            f"case_id does not match its schedule dimensions: {case['case_id']}"
        )
    case_root = root / case["case_id"]
    complete = case_root / "CASE_COMPLETE"
    if not complete.is_file() or complete.read_text().strip() != "complete":
        raise ValueError(f"missing or invalid CASE_COMPLETE for {case['case_id']}")

    protocol = _read_key_values(case_root / "protocol.txt")
    for field in (
        "repetition_id",
        "query_seed",
        "dataset",
        "backend",
        "arm",
        "arm_position",
    ):
        if protocol.get(field) != case[field]:
            raise ValueError(
                f"{case['case_id']}: protocol {field} does not match schedule"
            )
    if protocol.get("source_sha256", "").lower() != expected_source_sha:
        raise ValueError(f"{case['case_id']}: source identity mismatch")
    if (
        expected_dataset_identity is not None
        and protocol.get("dataset_identity_sha256", "").lower()
        != expected_dataset_identity
    ):
        raise ValueError(f"{case['case_id']}: dataset identity mismatch")

    layout = _read_key_values(case_root / "segment-layout.txt")
    try:
        parquet = int(layout["segment_parquet_objects"])
        vortex = int(layout["segment_vortex_objects"])
    except (KeyError, ValueError) as error:
        raise ValueError(f"{case['case_id']}: invalid segment-layout proof") from error
    if parquet < 0 or vortex < 0:
        raise ValueError(f"{case['case_id']}: negative segment object count")
    for key, value in (
        ("segment_parquet_objects", parquet),
        ("segment_vortex_objects", vortex),
    ):
        if protocol.get(key) != str(value):
            raise ValueError(
                f"{case['case_id']}: protocol and segment-layout {key} disagree"
            )
    arm = case["arm"]
    if arm == "fixed-parquet" and not (parquet > 0 and vortex == 0):
        raise ValueError(f"{case['case_id']}: fixed Parquet layout proof failed")
    if arm.startswith("fixed-vortex-") and not (parquet == 0 and vortex > 0):
        raise ValueError(f"{case['case_id']}: fixed Vortex layout proof failed")
    if arm.startswith("mixed-vortex-") and not (parquet > 0 and vortex > 0):
        raise ValueError(f"{case['case_id']}: mixed layout proof failed")


def _case_metrics(result_root: Path, case_id: str) -> dict[str, Any]:
    build_rows = _read_rows(result_root / "bench_build.csv")
    if len(build_rows) != 1:
        raise ValueError(f"expected one build row for {case_id}")
    build = build_rows[0]

    resources = _read_rows(result_root / "resources.csv")
    if len(resources) < 2:
        raise ValueError(f"under-sampled resources for {case_id}")
    peak_rss_bytes = max(int(row["rss_bytes"]) for row in resources)
    cpu_core_ms = 0.0
    previous_elapsed_ms = float(resources[0]["elapsed_ms"])
    for resource in resources[1:]:
        elapsed_ms = float(resource["elapsed_ms"])
        cpu_core_ms += (
            max(elapsed_ms - previous_elapsed_ms, 0.0)
            * float(resource["cpu_percent"])
            / 100.0
        )
        previous_elapsed_ms = elapsed_ms

    return {
        "build_ms": f"{float(build['ingest_ms']) + float(build['compaction_ms']):.6f}",
        "segment_bytes": int(build["segment_bytes"]),
        "total_active_index_bytes": int(build["total_active_index_bytes"]),
        "peak_rss_bytes": peak_rss_bytes,
        "cpu_core_ms": f"{cpu_core_ms:.6f}",
    }


def _validate_segment_path(row: dict[str, str], case_id: str) -> None:
    if row.get("schema_version") != "borsuk-production-bench-v10":
        raise ValueError(f"unsupported production benchmark schema for {case_id}")
    try:
        segments_searched = int(row["segments_searched"])
        global_leaf_counters = {
            key: int(row[key])
            for key in (
                "global_leaf_directory_reads",
                "global_leaf_directory_bytes",
                "global_leaf_pages_read",
                "global_leaf_page_bytes",
                "global_leaf_waves",
                "global_leaf_continuations",
                "global_leaf_exact_scores",
            )
        }
    except (KeyError, ValueError) as error:
        raise ValueError(f"missing segment-path proof for {case_id}") from error
    if any(global_leaf_counters.values()):
        raise ValueError(
            f"global-leaf sample cannot qualify normal segments for {case_id}"
        )
    if segments_searched <= 0:
        raise ValueError(f"normal-segment sample searched no segments for {case_id}")


def _validate_sample_identity(
    row: dict[str, str], case: dict[str, str], case_id: str
) -> None:
    expected = {
        "scan_codec": "srht-pq-scan",
        "cache_execution": "scan",
        "execution_engine": "srht-pq-scan",
        "query_seed": case["query_seed"],
        "repetition_id": case["repetition_id"],
    }
    for field, value in expected.items():
        if row.get(field) != value:
            raise ValueError(
                f"{case_id}: raw sample {field}={row.get(field)!r}; expected {value!r}"
            )


def _validate_sample_values(row: dict[str, str], case_id: str) -> None:
    try:
        latency_ms = float(row["latency_ms"])
        recall_at_10 = float(row["recall_at_10"])
    except (KeyError, ValueError) as error:
        raise ValueError(f"{case_id}: invalid query measurement") from error
    if not math.isfinite(latency_ms) or latency_ms < 0:
        raise ValueError(f"{case_id}: invalid latency_ms")
    if not math.isfinite(recall_at_10) or not 0 <= recall_at_10 <= 1:
        raise ValueError(f"{case_id}: invalid recall_at_10")


def _physical_requests(row: dict[str, str], backend: str) -> str:
    """Use the external request boundary appropriate to each backend."""
    field = "network_gets" if backend == "s3" else "backing_reads"
    try:
        value = int(row[field])
    except (KeyError, ValueError) as error:
        raise ValueError(
            f"missing physical request count `{field}` for {backend}"
        ) from error
    if value < 0:
        raise ValueError(f"negative physical request count `{field}` for {backend}")
    return str(value)


def assemble_rows(root: Path, *, minimum_samples: int) -> list[dict[str, Any]]:
    if minimum_samples <= 0:
        raise ValueError("minimum_samples must be positive")
    schedule = _read_rows(root / "schedule.csv")
    if not schedule:
        raise ValueError("qualification schedule is empty")
    try:
        protocol = json.loads((root / "qualification-protocol.json").read_text())
    except (FileNotFoundError, json.JSONDecodeError) as error:
        raise ValueError("missing or invalid frozen qualification protocol") from error
    _validate_schedule_contract(schedule, protocol)
    expected_samples = int(protocol["queries_per_repetition"])
    if minimum_samples != expected_samples:
        raise ValueError(
            f"assembler sample count must equal the frozen protocol: {expected_samples}"
        )
    source_sha = _source_sha(root)
    dataset_identity_sha = _dataset_identity_sha(root, protocol)
    output: list[dict[str, Any]] = []
    for case in schedule:
        case_id = case["case_id"]
        _validate_case_proof(
            root,
            case,
            source_sha,
            dataset_identity_sha,
        )
        result_root = root / case_id / "results"
        metrics = _case_metrics(result_root, case_id)
        rows = []
        for row in _read_rows(result_root / "bench_query_samples.csv"):
            if (
                row["phase"] == "uncached"
                and row["mode"] == "srht-pq-scan"
                and row["nprobe"] == "8"
                and row["max_candidates"] == "320"
            ):
                _validate_sample_identity(row, case, case_id)
                _validate_sample_values(row, case_id)
                _validate_segment_path(row, case_id)
                rows.append(
                    {
                        "dataset": case["dataset"],
                        "backend": case["backend"],
                        "arm": case["arm"],
                        "repetition_id": case["repetition_id"],
                        "query_position": row["sample_index"],
                        "query_source_index": row["query_source_index"],
                        "latency_ms": row["latency_ms"],
                        "recall_at_10": row["recall_at_10"],
                        "physical_requests": _physical_requests(row, case["backend"]),
                        # The query-scoped backing counter is authoritative.
                        # `bytes_read` is a logical accumulator and concurrent
                        # segment scopes can observe overlapping work.
                        "bytes_read": row["backing_bytes_read"],
                        **metrics,
                        "status": "ok",
                    }
                )
        try:
            positions = [int(row["query_position"]) for row in rows]
            source_indices = [int(row["query_source_index"]) for row in rows]
        except (KeyError, ValueError) as error:
            raise ValueError(f"{case_id}: invalid query identity") from error
        if len(positions) != expected_samples or set(positions) != set(
            range(expected_samples)
        ):
            raise ValueError(
                f"{case_id}: query positions do not exactly cover "
                f"0..{expected_samples - 1}"
            )
        if len(source_indices) != len(set(source_indices)):
            raise ValueError(f"{case_id}: duplicate query source indices")
        if len(rows) != expected_samples:
            raise ValueError(
                f"case {case_id} has {len(rows)} qualifying samples; "
                f"protocol requires exactly {expected_samples}"
            )
        output.extend(rows)
    return output


def write_rows(path: Path, rows: Sequence[dict[str, Any]]) -> None:
    if path.exists():
        raise FileExistsError(f"refusing to overwrite {path}")
    with path.open("x", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--minimum-samples", type=int, default=30)
    args = parser.parse_args()
    write_rows(
        args.output,
        assemble_rows(args.root, minimum_samples=args.minimum_samples),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
