#!/usr/bin/env python3
"""Fail-closed audit of a completed publication-v2 result tree."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import re
import tarfile
from dataclasses import asdict
from pathlib import Path
from typing import Any

if __package__:
    from scripts.analyze_publication_claims import compare_direct
    from scripts.publication_protocol import (
        SCHEDULE_FIELDS,
        build_schedule,
        validate_manifest,
    )
else:
    from analyze_publication_claims import compare_direct
    from publication_protocol import (
        SCHEDULE_FIELDS,
        build_schedule,
        validate_manifest,
    )

SHA256 = re.compile(r"[0-9a-f]{64}")
EMBEDDED_MANIFEST = "docs/research/publication-v2-manifest.json"
HYBRID_COVERAGE_KEY_FIELDS = (
    "stage",
    "dataset",
    "profile",
    "status",
    "scan_codec",
    "mode",
    "candidate_depth",
    "max_segments",
    "fusion",
    "rrf_k",
    "target_hot_query_fraction",
    "cache_profile",
    "campaign_repetition",
    "query_seed",
)
BORSUK_MEASURED_ARTIFACTS = (
    "bench_build.csv",
    "bench_recall_latency.csv",
    "bench_query_samples.csv",
    "resources.csv",
)
S3_VECTORS_MEASURED_ARTIFACTS = (
    "build.csv",
    "query.csv",
    "query_samples.csv",
    "resources.csv",
)
HYBRID_BUILD_ARTIFACTS = ("hybrid_build.csv", "resources.csv")
HYBRID_QUERY_ARTIFACTS = (
    "hybrid_queries.csv",
    "hybrid_summary.csv",
    "hybrid_startup.csv",
    "resources.csv",
)
HYBRID_DIRECT_DEPENDENCIES = {
    "boto3": "1.42.97",
    "numpy": "1.26.4",
    "sentence-transformers": "3.4.1",
    "torch": "2.5.1",
    "transformers": "4.48.3",
}
EXPECTED_INSTANCE_TYPE = "c7g.8xlarge"
EXPECTED_LOGICAL_CPUS = 32
EXPECTED_LOCAL_DISK_CLASS = "ebs-gp3-500-3000-125"


def _read_csv(path: Path) -> list[dict[str, str]]:
    if not path.is_file():
        raise ValueError(f"missing required CSV: {path}")
    with path.open(encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None:
            raise ValueError(f"CSV has no header: {path}")
        rows = list(reader)
    if not rows:
        raise ValueError(f"CSV has no data rows: {path}")
    return rows


def _read_key_values(path: Path) -> dict[str, str]:
    if not path.is_file():
        raise ValueError(f"missing required protocol file: {path}")
    result: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line or "=" not in line:
            raise ValueError(f"invalid key=value line in {path}: {line!r}")
        key, value = line.split("=", 1)
        if not key or key in result:
            raise ValueError(f"duplicate or empty protocol key in {path}: {key!r}")
        result[key] = value
    return result


def _read_json_object(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise ValueError(f"missing required JSON: {path}")
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as error:
        raise ValueError(f"invalid JSON: {path}") from error
    if not isinstance(value, dict):
        raise ValueError(f"required JSON must contain an object: {path}")
    return value


def _require_marker(path: Path) -> None:
    if not path.is_file() or path.read_text(encoding="utf-8") != "complete\n":
        raise ValueError(f"missing or invalid completion marker: {path}")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _read_embedded_manifest(path: Path) -> bytes:
    try:
        with tarfile.open(path, "r:gz") as archive:
            members = [
                member
                for member in archive.getmembers()
                if member.name == EMBEDDED_MANIFEST
            ]
            if len(members) != 1 or not members[0].isfile():
                raise ValueError(
                    "source archive must contain exactly one regular embedded manifest"
                )
            if members[0].size > 1024 * 1024:
                raise ValueError("source archive embedded manifest is unreasonably large")
            handle = archive.extractfile(members[0])
            if handle is None:
                raise ValueError("source archive embedded manifest is unreadable")
            return handle.read()
    except (tarfile.TarError, OSError) as error:
        raise ValueError("source archive is not a readable gzip tar") from error


def _require_measured_coverage(
    path: Path,
    expected_datasets: set[str],
    label: str,
    *,
    rows_per_dataset: int = 1,
) -> list[dict[str, str]]:
    rows = _read_csv(path)
    expected_row_count = len(expected_datasets) * rows_per_dataset
    if len(rows) != expected_row_count:
        raise ValueError(
            f"{label} coverage row count differs: "
            f"{len(rows)} != {expected_row_count}"
        )
    actual_datasets = {row.get("dataset", "") for row in rows}
    if actual_datasets != expected_datasets:
        raise ValueError(
            f"{label} coverage datasets differ: {actual_datasets} != {expected_datasets}"
        )
    if any(row.get("status") != "measured" for row in rows):
        raise ValueError(f"{label} coverage contains a non-measured row")
    return rows


def _require_hybrid_coverage(
    path: Path,
    expected_datasets: set[str],
    query_seed: int,
    label: str,
    search_config: dict[str, object],
) -> list[dict[str, str]]:
    rows = _read_csv(path)
    actual = {
        tuple(row.get(field, "") for field in HYBRID_COVERAGE_KEY_FIELDS)
        for row in rows
    }
    expected: set[tuple[str, ...]] = set()
    modes = tuple(str(value) for value in search_config["modes"])
    candidate_depth = str(search_config["candidate_depth"])
    max_segments = str(search_config["max_segments"])
    hot_fractions = tuple(str(value) for value in search_config["hot_fractions"])
    rrf_k = str(search_config["rrf_k"])
    campaign_repetition = str(
        search_config["repetitions_per_campaign_repetition"]
    )
    for dataset in expected_datasets:
        expected.add(
            (
                "build",
                dataset,
                "srht",
                "measured",
                "srht-pq-scan",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
            )
        )
        for mode in modes:
            mode_rrf_k = rrf_k if "+" in mode else "1"
            for hot_fraction in hot_fractions:
                expected.add(
                    (
                        "query",
                        dataset,
                        "srht",
                        "measured",
                        "srht-pq-scan",
                        mode,
                        candidate_depth,
                        max_segments,
                        "rrf",
                        mode_rrf_k,
                        hot_fraction,
                        f"mixed-cache-{hot_fraction}",
                        campaign_repetition,
                        str(query_seed),
                    )
                )
    if len(rows) != len(expected):
        raise ValueError(
            f"{label} coverage row count differs: {len(rows)} != {len(expected)}"
        )
    if actual != expected:
        missing = len(expected - actual)
        unexpected = len(actual - expected)
        raise ValueError(
            f"{label} coverage differs from the frozen hybrid matrix: "
            f"missing={missing} unexpected={unexpected}"
        )
    return rows


def _require_nonempty_artifacts(directory: Path, names: tuple[str, ...]) -> None:
    for name in names:
        path = directory / name
        if not path.is_file() or path.stat().st_size == 0:
            raise ValueError(f"missing measured artifact: {path}")


def _require_hybrid_artifacts(
    repetition_root: Path,
    datasets: set[str],
    coverage_rows: list[dict[str, str]],
) -> None:
    for dataset in datasets:
        profile_root = repetition_root / f"hybrid/{dataset}/srht"
        _require_nonempty_artifacts(profile_root, ("dataset-validation.json",))
        _require_nonempty_artifacts(profile_root / "build", HYBRID_BUILD_ARTIFACTS)
    for row in coverage_rows:
        if row.get("stage") != "query":
            continue
        query_root = (
            repetition_root
            / "hybrid"
            / row["dataset"]
            / "srht"
            / "query"
            / row["mode"]
            / f"c{row['candidate_depth']}-p{row['max_segments']}"
            / f"rrf-k{row['rrf_k']}"
            / f"hot-{row['target_hot_query_fraction']}"
            / "repetition-1"
        )
        _require_nonempty_artifacts(query_root, HYBRID_QUERY_ARTIFACTS)


def _validate_hybrid_inputs(
    root: Path,
    datasets: set[str],
    dense_config: dict[str, object],
) -> None:
    packages_path = root / "hybrid-python-packages.txt"
    if not packages_path.is_file():
        raise ValueError("missing hybrid dependency freeze")
    packages: dict[str, str] = {}
    for line in packages_path.read_text(encoding="utf-8").splitlines():
        if "==" in line:
            name, version = line.split("==", 1)
            packages[name.casefold()] = version
    for name, version in HYBRID_DIRECT_DEPENDENCIES.items():
        if packages.get(name) != version:
            raise ValueError(
                f"hybrid dependency drift for {name}: "
                f"{packages.get(name)!r} != {version!r}"
            )

    inputs = root / "hybrid-inputs"
    for dataset in datasets:
        prepared = _read_json_object(inputs / f"{dataset}-manifest.json")
        if prepared.get("dataset") != dataset:
            raise ValueError(f"{dataset} hybrid input manifest dataset drift")
        dense = prepared.get("dense")
        if not isinstance(dense, dict):
            raise ValueError(f"{dataset} hybrid input manifest lacks dense config")
        for field, expected in dense_config.items():
            if dense.get(field) != expected:
                raise ValueError(
                    f"{dataset} hybrid input dense {field} drift: "
                    f"{dense.get(field)!r} != {expected!r}"
                )
        if dense.get("publication_valid") is not True:
            raise ValueError(f"{dataset} hybrid input is not publication-valid")

        validation = _read_json_object(inputs / f"{dataset}-validation.json")
        if validation.get("dataset") != dataset or validation.get("status") != "valid":
            raise ValueError(f"{dataset} hybrid input validation is not valid")

        source = _read_json_object(inputs / f"{dataset}-source.json")
        if source.get("dataset") != dataset:
            raise ValueError(f"{dataset} hybrid source dataset drift")
        source_sha = source.get("archive_sha256")
        if not isinstance(source_sha, str) or not SHA256.fullmatch(source_sha):
            raise ValueError(f"{dataset} hybrid source SHA-256 is invalid")
        source_url = source.get("url")
        if not isinstance(source_url, str) or not source_url.startswith("https://"):
            raise ValueError(f"{dataset} hybrid source URL is invalid")


def _direct_rows(
    repetition_root: Path,
    repetition_id: str,
    query_seed: int,
    query_count: int,
) -> tuple[dict[str, list[dict[str, Any]]], dict[str, list[dict[str, Any]]]]:
    borsuk_path = (
        repetition_root / "borsuk-direct/fashion-mnist-784/bench_query_samples.csv"
    )
    s3_path = (
        repetition_root
        / f"amazon-s3-vectors/fashion-mnist-784/{repetition_id}/query_samples.csv"
    )
    borsuk_source = _read_csv(borsuk_path)
    s3_source = _read_csv(s3_path)
    borsuk: dict[str, list[dict[str, Any]]] = {}
    s3: dict[str, list[dict[str, Any]]] = {}

    for phase in ("uncached", "disk_cached"):
        selected = [
            row
            for row in borsuk_source
            if row.get("phase") == phase
            and row.get("mode") == "srht-pq-scan"
            and row.get("nprobe") == "8"
            and row.get("max_candidates") == "320"
        ]
        if len(selected) != query_count:
            raise ValueError(
                f"{repetition_id} BORSUK {phase} has {len(selected)} rows, "
                f"expected {query_count}"
            )
        normalized = []
        for row in selected:
            if row.get("repetition_id") != repetition_id:
                raise ValueError(f"{repetition_id} BORSUK repetition id drift")
            if int(row.get("query_seed", "-1")) != query_seed:
                raise ValueError(f"{repetition_id} BORSUK query seed drift")
            normalized.append(
                {
                    **row,
                    "query_position": int(row["sample_index"]),
                    "query_source_index": int(row["query_source_index"]),
                    "latency_ms": float(row["latency_ms"]),
                    "recall_at_10": float(row["recall_at_10"]),
                    "status": "ok",
                }
            )
        borsuk[phase] = normalized

    for query_pass in ("first_pass", "repeated_pass"):
        selected = [row for row in s3_source if row.get("pass") == query_pass]
        if len(selected) != query_count:
            raise ValueError(
                f"{repetition_id} S3 Vectors {query_pass} has {len(selected)} rows, "
                f"expected {query_count}"
            )
        normalized = []
        for row in selected:
            if row.get("status") != "ok":
                raise ValueError(f"{repetition_id} S3 Vectors contains failed requests")
            if row.get("repetition_id") != repetition_id:
                raise ValueError(f"{repetition_id} S3 Vectors repetition id drift")
            if int(row.get("query_seed", "-1")) != query_seed:
                raise ValueError(f"{repetition_id} S3 Vectors query seed drift")
            normalized.append(
                {
                    **row,
                    "query_position": int(row["query_position"]),
                    "query_source_index": int(row["query_source_index"]),
                    "latency_ms": float(row["latency_ms"]),
                    "recall_at_10": float(row["recall_at_10"]),
                }
            )
        s3[query_pass] = normalized

    expected_positions = set(range(query_count))
    for label, rows in [*borsuk.items(), *s3.items()]:
        positions = {int(row["query_position"]) for row in rows}
        sources = {int(row["query_source_index"]) for row in rows}
        if positions != expected_positions:
            raise ValueError(f"{repetition_id} {label} query positions are incomplete")
        if sources != expected_positions:
            raise ValueError(f"{repetition_id} {label} query source set is incomplete")
        for row in rows:
            latency = float(row["latency_ms"])
            recall = float(row["recall_at_10"])
            if not math.isfinite(latency) or latency <= 0:
                raise ValueError(f"{repetition_id} {label} has invalid latency")
            if not math.isfinite(recall) or not 0.0 <= recall <= 1.0:
                raise ValueError(f"{repetition_id} {label} has invalid recall")

    for borsuk_phase, s3_pass in (
        ("uncached", "first_pass"),
        ("disk_cached", "repeated_pass"),
    ):
        borsuk_pairs = {
            int(row["query_position"]): int(row["query_source_index"])
            for row in borsuk[borsuk_phase]
        }
        s3_pairs = {
            int(row["query_position"]): int(row["query_source_index"])
            for row in s3[s3_pass]
        }
        if borsuk_pairs != s3_pairs:
            raise ValueError(
                f"{repetition_id} {borsuk_phase}/{s3_pass} query source pairing differs"
            )
    return borsuk, s3


def _verify_claim(
    path: Path,
    dataset: str,
    cache_pair: str,
    decision: object,
) -> None:
    rows = _read_csv(path)
    if len(rows) != 1:
        raise ValueError(f"claim CSV must contain exactly one row: {path}")
    actual = rows[0]
    expected = {"dataset": dataset, "cache_pair": cache_pair, **asdict(decision)}
    for field, expected_value in expected.items():
        if isinstance(expected_value, float):
            try:
                actual_value = float(actual[field])
            except (KeyError, ValueError) as error:
                raise ValueError(f"{path} has invalid numeric field {field}") from error
            if not math.isclose(
                actual_value, expected_value, rel_tol=1e-12, abs_tol=1e-12
            ):
                raise ValueError(f"{path} does not match recomputed field {field}")
        elif actual.get(field) != str(expected_value):
            raise ValueError(f"{path} does not match recomputed field {field}")


def validate_result_tree(
    root: Path,
    *,
    expected_source_sha256: str | None = None,
    bootstrap_samples: int = 10_000,
) -> dict[str, object]:
    root = root.resolve()
    if (root / "PUBLICATION_V2_FAILED").exists():
        raise ValueError("publication result tree contains PUBLICATION_V2_FAILED")
    _require_marker(root / "PUBLICATION_V2_COMPLETE")

    manifest_path = root / "manifest.json"
    manifest = validate_manifest(json.loads(manifest_path.read_text(encoding="utf-8")))
    schedule_path = root / "schedule.csv"
    if b"\r\n" in schedule_path.read_bytes():
        raise ValueError("schedule.csv must use LF line endings")
    actual_schedule = _read_csv(schedule_path)
    expected_schedule = [
        {field: str(row[field]) for field in SCHEDULE_FIELDS}
        for row in build_schedule(manifest)
    ]
    if actual_schedule != expected_schedule:
        raise ValueError("schedule.csv does not match the frozen manifest")

    environment = _read_key_values(root / "environment.txt")
    required_environment = {
        "logical_cpus",
        "ram_bytes",
        "instance_type",
        "local_disk_class",
        "accelerator",
        "index_storage_class",
        "hybrid_python_version",
        "client_compute_boundary",
        "managed_service_compute",
        "source_sha256",
        "manifest_sha256",
        "rustc_version",
    }
    missing_environment = required_environment.difference(environment)
    if missing_environment:
        raise ValueError(f"environment identity is incomplete: {missing_environment}")
    if int(environment["logical_cpus"]) <= 0 or int(environment["ram_bytes"]) <= 0:
        raise ValueError("environment CPU and RAM must be positive")
    if environment.get("run_id") != manifest["campaign_id"]:
        raise ValueError("environment run_id does not match the frozen campaign")
    if environment.get("execute") != "1":
        raise ValueError("environment must record paid execution")
    if int(environment["logical_cpus"]) != EXPECTED_LOGICAL_CPUS:
        raise ValueError("environment logical CPU count differs from the frozen client")
    if environment["instance_type"] != EXPECTED_INSTANCE_TYPE:
        raise ValueError("environment instance type differs from the frozen client")
    if environment["local_disk_class"] != EXPECTED_LOCAL_DISK_CLASS:
        raise ValueError("environment local disk differs from the frozen client")
    if environment["accelerator"] != "none":
        raise ValueError("publication client must record accelerator=none")
    if environment["index_storage_class"] != "amazon-s3-standard":
        raise ValueError("BORSUK index storage class must be Amazon S3 Standard")
    if environment["client_compute_boundary"] != (
        "same-instance-for-borsuk-and-amazon-s3-vectors-client"
    ):
        raise ValueError("client_compute_boundary does not match the frozen protocol")
    if environment["managed_service_compute"] != "undisclosed":
        raise ValueError("managed_service_compute must remain undisclosed")
    if environment["hybrid_python_version"] != "3.12":
        raise ValueError("hybrid_python_version must remain 3.12")
    if not environment["rustc_version"].startswith("rustc "):
        raise ValueError("environment must record the measured Rust toolchain")
    source_sha = environment["source_sha256"]
    if not SHA256.fullmatch(source_sha):
        raise ValueError("source_sha256 is not a lowercase SHA-256 digest")
    if expected_source_sha256 is not None and source_sha != expected_source_sha256:
        raise ValueError("source_sha256 does not match the expected frozen source")
    if _sha256(root / "source-archive.tar.gz") != source_sha:
        raise ValueError("source archive digest does not match environment.txt")
    if _read_embedded_manifest(root / "source-archive.tar.gz") != manifest_path.read_bytes():
        raise ValueError("source archive embedded manifest differs from manifest.json")
    if _sha256(manifest_path) != environment["manifest_sha256"]:
        raise ValueError("manifest digest does not match environment.txt")

    all_borsuk: dict[str, list[dict[str, Any]]] = {
        "uncached": [],
        "disk_cached": [],
    }
    all_s3: dict[str, list[dict[str, Any]]] = {
        "first_pass": [],
        "repeated_pass": [],
    }
    direct_dataset = str(manifest["direct_dataset"])
    dense_remaining = set(manifest["dense_datasets"]) - {direct_dataset}
    expected_hybrid = set(manifest["hybrid_datasets"])
    dense_config = manifest["hybrid_dense_config"]
    if not isinstance(dense_config, dict):
        raise ValueError("manifest hybrid_dense_config must be an object")
    _validate_hybrid_inputs(root, expected_hybrid, dense_config)
    hybrid_search_config = manifest["hybrid_search_config"]
    if not isinstance(hybrid_search_config, dict):
        raise ValueError("manifest hybrid_search_config must be an object")
    query_count = int(manifest["queries_per_repetition"])

    for schedule_row in expected_schedule:
        repetition_id = schedule_row["repetition_id"]
        repetition_root = root / repetition_id
        _require_marker(repetition_root / "REPETITION_COMPLETE")
        protocol = _read_key_values(repetition_root / "protocol.txt")
        if protocol != schedule_row:
            raise ValueError(f"{repetition_id} protocol.txt differs from schedule")

        _require_measured_coverage(
            repetition_root / "borsuk-direct/coverage.csv",
            {direct_dataset},
            f"{repetition_id} BORSUK direct",
        )
        _require_nonempty_artifacts(
            repetition_root / f"borsuk-direct/{direct_dataset}",
            BORSUK_MEASURED_ARTIFACTS,
        )
        _require_measured_coverage(
            repetition_root / "borsuk-dense/coverage.csv",
            dense_remaining,
            f"{repetition_id} BORSUK dense",
        )
        for dataset in dense_remaining:
            _require_nonempty_artifacts(
                repetition_root / f"borsuk-dense/{dataset}",
                BORSUK_MEASURED_ARTIFACTS,
            )
        s3_coverage = _require_measured_coverage(
            repetition_root / "amazon-s3-vectors/coverage.csv",
            {direct_dataset},
            f"{repetition_id} S3 Vectors",
        )
        if any(
            row.get("repetition_id") != repetition_id
            or row.get("query_seed") != schedule_row["query_seed"]
            for row in s3_coverage
        ):
            raise ValueError(f"{repetition_id} S3 Vectors coverage protocol drift")
        _require_nonempty_artifacts(
            repetition_root
            / f"amazon-s3-vectors/{direct_dataset}/{repetition_id}",
            S3_VECTORS_MEASURED_ARTIFACTS,
        )

        hybrid_coverage = _require_hybrid_coverage(
            repetition_root / "hybrid/coverage.csv",
            expected_hybrid,
            int(schedule_row["query_seed"]),
            f"{repetition_id} hybrid",
            hybrid_search_config,
        )
        _require_hybrid_artifacts(
            repetition_root,
            expected_hybrid,
            hybrid_coverage,
        )
        for dataset in expected_hybrid:
            dataset_rows = [
                row for row in hybrid_coverage if row.get("dataset") == dataset
            ]
            stages = {row.get("stage") for row in dataset_rows}
            if stages != {"build", "query"}:
                raise ValueError(
                    f"{repetition_id} hybrid {dataset} lacks build/query coverage"
                )
            if any(
                row.get("profile") != "srht" or row.get("scan_codec") != "srht-pq-scan"
                for row in dataset_rows
            ):
                raise ValueError(f"{repetition_id} hybrid {dataset} profile drift")

        borsuk, s3 = _direct_rows(
            repetition_root,
            repetition_id,
            int(schedule_row["query_seed"]),
            query_count,
        )
        for phase, rows in borsuk.items():
            all_borsuk[phase].extend(rows)
        for query_pass, rows in s3.items():
            all_s3[query_pass].extend(rows)

    for borsuk_phase, s3_pass, suffix, cache_pair in (
        ("uncached", "first_pass", "first", "uncached-vs-first-pass"),
        (
            "disk_cached",
            "repeated_pass",
            "repeated",
            "disk-cached-vs-repeated-pass",
        ),
    ):
        decision = compare_direct(
            all_borsuk[borsuk_phase],
            all_s3[s3_pass],
            bootstrap_samples=bootstrap_samples,
            expected_repetitions=int(manifest["repetitions"]),
            expected_queries_per_repetition=query_count,
        )
        _verify_claim(
            root / f"direct-claim-{suffix}.csv",
            direct_dataset,
            cache_pair,
            decision,
        )

    return {
        "campaign_id": manifest["campaign_id"],
        "source_sha256": source_sha,
        "repetitions": int(manifest["repetitions"]),
        "paired_queries_per_phase": int(manifest["repetitions"]) * query_count,
        "status": "valid",
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("root", type=Path)
    parser.add_argument("--expected-source-sha256")
    parser.add_argument("--bootstrap-samples", type=int, default=10_000)
    args = parser.parse_args()
    summary = validate_result_tree(
        args.root,
        expected_source_sha256=args.expected_source_sha256,
        bootstrap_samples=args.bootstrap_samples,
    )
    print(json.dumps(summary, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
