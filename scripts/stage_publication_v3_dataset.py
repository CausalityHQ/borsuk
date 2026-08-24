#!/usr/bin/env python3
"""Materialize one Publication V3 external dataset into immutable local bytes.

AWS promotion is intentionally separate: this worker first produces and fully
validates a stock-Parquet descriptor.  A resumed attempt validates an existing
directory byte-for-byte and never rewrites it.
"""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import shutil
import subprocess
import sys
from pathlib import Path

if __package__:
    from scripts.check_publication_disk import require_free_disk
    from scripts.publication_v3_aws import staging_jobs
    from scripts.publication_v3_beir import (
        BEIR_DENSE_MODEL,
        BEIR_DENSE_REVISION,
        BEIR_DOCUMENT_PREFIX,
        BEIR_QUERY_PREFIX,
        BEIR_SPARSE_MAX_FEATURES,
    )
    from scripts.publication_v3_datasets import (
        build_dataset_descriptor,
        dataset_materialization_sha256,
        validate_dataset_descriptor,
    )
    from scripts.publication_v3_protocol import canonical_json_bytes, validate_manifest
else:
    from check_publication_disk import require_free_disk
    from publication_v3_aws import staging_jobs
    from publication_v3_beir import (
        BEIR_DENSE_MODEL,
        BEIR_DENSE_REVISION,
        BEIR_DOCUMENT_PREFIX,
        BEIR_QUERY_PREFIX,
        BEIR_SPARSE_MAX_FEATURES,
    )
    from publication_v3_datasets import (
        build_dataset_descriptor,
        dataset_materialization_sha256,
        validate_dataset_descriptor,
    )
    from publication_v3_protocol import canonical_json_bytes, validate_manifest


ROOT = Path(__file__).resolve().parents[1]
VDB_DATASET_NAMES = {
    "cohere-medium-1m-768": "cohere-medium-1M",
    "cohere-large-10m-768": "cohere-large-10M",
    "laion-100m-768": "laion-100M",
}
MINIMUM_STAGING_RESERVE_BYTES = 1024**3
MAXIMUM_STAGING_RESERVE_BYTES = 64 * 1024**3


def _dataset(manifest: dict[str, object], dataset_id: str) -> dict[str, object]:
    matches = [item for item in manifest["datasets"] if item["id"] == dataset_id]
    if len(matches) != 1:
        raise ValueError(f"manifest has no unique dataset {dataset_id}")
    dataset = matches[0]
    if dataset["source"]["state"] not in {"unstaged", "generated"}:
        raise ValueError("dataset worker requires an unresolved source")
    return dataset


def adapter_command(
    manifest: dict[str, object],
    dataset_id: str,
    output: Path,
    source_cache: Path | None,
) -> tuple[str, ...]:
    normalized = validate_manifest(manifest)
    if normalized["source"]["state"] != "frozen":
        raise ValueError("dataset staging requires a frozen source archive")
    dataset = _dataset(normalized, dataset_id)
    kind = dataset["kind"]
    if dataset["source"]["state"] == "generated":
        if source_cache is not None:
            raise ValueError("synthetic generation does not accept a source cache")
        source = dataset["source"]
        return (
            "env",
            f"BORSUK_SYNTHETIC_OUTPUT={output}",
            f"BORSUK_SYNTHETIC_GENERATOR={source['generator']}",
            f"BORSUK_SYNTHETIC_DATASET_ID={dataset_id}",
            f"BORSUK_SYNTHETIC_TRAIN={dataset['scale']['rows']}",
            f"BORSUK_SYNTHETIC_DIMENSIONS={dataset['dimensions']}",
            f"BORSUK_SYNTHETIC_QUERIES={normalized['queries_per_repetition']}",
            "BORSUK_SYNTHETIC_GROUP_SIZE=100",
            f"BORSUK_SYNTHETIC_SEED={source['seed']}",
            str(ROOT / "target/release/examples/generate_synthetic_dataset"),
        )
    if kind == "standard-ann":
        expected_source = str(dataset["source"]["expected_source"])
        filename = expected_source.rsplit("/", 1)[-1]
        if not filename.endswith(".hdf5"):
            raise ValueError("ANN source must be an HDF5 URL")
        command = [
            sys.executable,
            str(ROOT / "scripts/fetch_ann_dataset.py"),
            "--dataset",
            filename.removesuffix(".hdf5"),
            "--publication-id",
            dataset_id,
            "--out",
            str(output),
        ]
        if source_cache is not None:
            command.extend(("--source-cache", str(source_cache)))
        return tuple(command)
    if kind == "realistic-dense":
        if source_cache is not None:
            raise ValueError(
                "VDBBench acquisition does not accept a local source cache"
            )
        try:
            upstream_name = VDB_DATASET_NAMES[dataset_id]
        except KeyError as error:
            raise ValueError(
                f"VDBBench dataset has no exact adapter: {dataset_id}"
            ) from error
        return (
            sys.executable,
            str(ROOT / "scripts/fetch_vdbbench_dataset.py"),
            "--dataset",
            upstream_name,
            "--output-root",
            str(output.parent / "acquired"),
            "--execute-download",
            "--publication-output-root",
            str(output.parent / ".publication"),
        )
    if kind == "beir-hybrid":
        command = [
            sys.executable,
            str(ROOT / "scripts/prepare_beir_publication_dataset.py"),
            "--dataset",
            dataset_id,
            "--out",
            str(output),
            "--expected-source",
            str(dataset["source"]["expected_source"]),
            "--dense-model",
            BEIR_DENSE_MODEL,
            "--dense-revision",
            BEIR_DENSE_REVISION,
            "--dense-query-prefix",
            BEIR_QUERY_PREFIX,
            "--dense-document-prefix",
            BEIR_DOCUMENT_PREFIX,
            "--sparse-max-features",
            str(BEIR_SPARSE_MAX_FEATURES),
            "--publication",
        ]
        if source_cache is not None:
            command.extend(("--source", str(source_cache)))
        return tuple(command)
    raise ValueError(f"dataset kind has no external staging adapter: {kind}")


def _staged_uri(manifest: dict[str, object], dataset_id: str, attempt: int) -> str:
    matches = [
        job.output_uri
        for job in staging_jobs(manifest, attempt=attempt)
        if job.dataset_id == dataset_id
    ]
    if len(matches) != 1:
        raise ValueError("dataset has no unique staging job")
    return matches[0]


def _load_provenance(dataset: dict[str, object], path: Path) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError(
            "materialized dataset provenance is missing or invalid"
        ) from error
    if dataset["source"]["state"] == "generated":
        required_generated = {
            "schema_version",
            "dataset",
            "source",
            "source_sha256",
            "materialization_sha256",
            "generator",
            "seed",
            "kind",
            "rows",
            "dimensions",
            "metric",
            "generator_source_archive_sha256",
        }
        source = dataset["source"]
        if (
            not isinstance(value, dict)
            or set(value) != required_generated
            or value["schema_version"] != 1
            or value["dataset"] != dataset["id"]
            or value["source"] != "generated"
            or value["generator"] != source["generator"]
            or value["seed"] != source["seed"]
            or value["kind"] != dataset["kind"]
            or value["rows"] != dataset["scale"]["rows"]
            or value["dimensions"] != dataset["dimensions"]
            or value["metric"] != dataset["metric"]
            or any(
                not isinstance(value[field], str)
                or len(value[field]) != 64
                or any(
                    character not in "0123456789abcdef" for character in value[field]
                )
                for field in (
                    "source_sha256",
                    "materialization_sha256",
                    "generator_source_archive_sha256",
                )
            )
        ):
            raise ValueError("generated dataset provenance differs from the manifest")
        return value
    required = {
        "schema_version",
        "dataset",
        "source",
        "source_sha256",
        "materialization_sha256",
    }
    allowed = required | {"source_descriptor_sha256"}
    digest_fields = {"source_sha256", "materialization_sha256"}
    if "source_descriptor_sha256" in value:
        digest_fields.add("source_descriptor_sha256")
    if (
        not isinstance(value, dict)
        or not required.issubset(value)
        or not set(value).issubset(allowed)
        or value["schema_version"] != 1
        or value["dataset"] != dataset["id"]
        or value["source"] != dataset["source"]["expected_source"]
        or any(
            not isinstance(value[field], str)
            or len(value[field]) != 64
            or any(character not in "0123456789abcdef" for character in value[field])
            for field in digest_fields
        )
    ):
        raise ValueError("materialized dataset provenance differs from the manifest")
    return value


def _descriptor(
    manifest: dict[str, object],
    dataset: dict[str, object],
    output: Path,
    provenance_path: Path,
    attempt: int,
) -> dict[str, object]:
    provenance = _load_provenance(dataset, provenance_path)
    if (
        dataset["source"]["state"] == "generated"
        and provenance["generator_source_archive_sha256"]
        != manifest["source"]["archive_sha256"]
    ):
        raise ValueError("generated dataset provenance uses a different source archive")
    inspected = copy.deepcopy(dataset)
    if dataset["source"]["state"] == "generated":
        metadata = json.loads((output / "meta.json").read_text(encoding="utf-8"))
        if (
            metadata.get("generator") != dataset["source"]["generator"]
            or metadata.get("seed") != dataset["source"]["seed"]
        ):
            raise ValueError("generated metadata differs from its recipe")
    inspected["source"] = {
        "state": "staged",
        "url": output.resolve().as_uri(),
        "sha256": provenance["materialization_sha256"],
        "license": dataset["source"].get("license", "borsuk-generated"),
    }
    try:
        descriptor = build_dataset_descriptor(inspected)
    except ValueError as error:
        raise ValueError(
            f"materialized dataset differs from provenance: {error}"
        ) from error
    staged = copy.deepcopy(dataset)
    staged["source"] = {
        "state": "staged",
        "url": _staged_uri(manifest, str(dataset["id"]), attempt),
        "sha256": descriptor["content_sha256"],
        "license": dataset["source"].get("license", "borsuk-generated"),
    }
    descriptor["source"] = staged["source"]
    return validate_dataset_descriptor(descriptor, staged)


def required_staging_bytes(dataset: dict[str, object]) -> int:
    vector_bytes = int(dataset["scale"]["rows"]) * int(dataset["dimensions"]) * 4
    reserve = min(
        MAXIMUM_STAGING_RESERVE_BYTES,
        max(MINIMUM_STAGING_RESERVE_BYTES, vector_bytes // 5),
    )
    return 2 * vector_bytes + reserve


def materialize_dataset(
    manifest: dict[str, object],
    *,
    dataset_id: str,
    attempt: int,
    work_root: Path,
    source_cache: Path | None = None,
    source_archive_sha256: str | None = None,
) -> dict[str, object]:
    normalized = validate_manifest(manifest)
    if normalized["source"]["state"] != "frozen":
        raise ValueError("dataset staging requires a frozen source archive")
    if attempt <= 0 or attempt > 9_999:
        raise ValueError("dataset staging attempt must be in 1..=9999")
    dataset = _dataset(normalized, dataset_id)
    work_root.mkdir(parents=True, exist_ok=True)
    attempts_root = work_root / "attempts"
    attempt_root = attempts_root / f"{attempt:04d}"
    output = attempt_root / "materialized"
    provenance_path = attempt_root / "materialized.provenance.json"
    if output.exists():
        descriptor = _descriptor(normalized, dataset, output, provenance_path, attempt)
        if dataset["kind"] in {"realistic-dense", "beir-hybrid"}:
            shutil.rmtree(attempt_root / "acquired", ignore_errors=True)
        return descriptor
    if attempt_root.exists():
        raise ValueError("dataset attempt exists without a sealed materialization")
    attempts_root.mkdir(parents=True, exist_ok=True)
    scratch_root = attempts_root / f".{attempt:04d}-{dataset_id}.partial"
    for stale_scratch in attempts_root.glob(f".*-{dataset_id}.partial"):
        shutil.rmtree(stale_scratch)
    require_free_disk(
        work_root,
        required_staging_bytes(dataset),
        f"{dataset_id} source acquisition and materialization",
    )
    scratch_root.mkdir()
    try:
        scratch_output = scratch_root / "materialized"
        effective_source_cache = source_cache
        if dataset["kind"] == "standard-ann" and effective_source_cache is None:
            filename = str(dataset["source"]["expected_source"]).rsplit("/", 1)[-1]
            effective_source_cache = work_root / "source-cache" / filename
        command = adapter_command(
            normalized, dataset_id, scratch_output, effective_source_cache
        )
        subprocess.run(command, cwd=ROOT, check=True, stdout=subprocess.PIPE, text=True)
        if dataset["kind"] == "realistic-dense":
            candidate = scratch_root / ".publication" / dataset_id
            candidate_provenance = (
                scratch_root / ".publication" / f"{dataset_id}.provenance.json"
            )
            if scratch_output.exists() or not candidate.is_dir():
                raise ValueError(
                    "VDBBench adapter did not produce the exact publication directory"
                )
            candidate.rename(scratch_output)
            candidate_provenance.rename(scratch_root / "materialized.provenance.json")
            shutil.rmtree(scratch_root / ".publication")
        if not scratch_output.is_dir():
            raise ValueError(
                "dataset adapter did not produce a materialization directory"
            )
        if dataset["source"]["state"] == "generated":
            if (
                source_archive_sha256 is None
                or len(source_archive_sha256) != 64
                or any(
                    character not in "0123456789abcdef"
                    for character in source_archive_sha256
                )
            ):
                raise ValueError(
                    "generated dataset requires its generator source archive checksum"
                )
            recipe = {
                "dataset": dataset["id"],
                "generator": dataset["source"]["generator"],
                "seed": dataset["source"]["seed"],
                "kind": dataset["kind"],
                "rows": dataset["scale"]["rows"],
                "dimensions": dataset["dimensions"],
                "metric": dataset["metric"],
            }
            content_sha256 = dataset_materialization_sha256(
                scratch_output, kind=str(dataset["kind"])
            )
            provenance = {
                "schema_version": 1,
                **recipe,
                "source": "generated",
                "source_sha256": hashlib.sha256(
                    canonical_json_bytes(recipe)
                ).hexdigest(),
                "materialization_sha256": content_sha256,
                "generator_source_archive_sha256": source_archive_sha256,
            }
            (scratch_root / "materialized.provenance.json").write_bytes(
                canonical_json_bytes(provenance) + b"\n"
            )
        descriptor = _descriptor(
            normalized,
            dataset,
            scratch_output,
            scratch_root / "materialized.provenance.json",
            attempt,
        )
        scratch_root.rename(attempt_root)
        if dataset["kind"] in {"realistic-dense", "beir-hybrid"}:
            shutil.rmtree(attempt_root / "acquired", ignore_errors=True)
        return descriptor
    except BaseException:
        shutil.rmtree(scratch_root, ignore_errors=True)
        raise


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", required=True, type=Path)
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--attempt", required=True, type=int)
    parser.add_argument("--work-root", required=True, type=Path)
    parser.add_argument("--source-cache", type=Path)
    args = parser.parse_args()
    manifest = json.loads(args.manifest.read_text(encoding="utf-8"))
    descriptor = materialize_dataset(
        manifest,
        dataset_id=args.dataset,
        attempt=args.attempt,
        work_root=args.work_root,
        source_cache=args.source_cache,
    )
    print(canonical_json_bytes(descriptor).decode("utf-8"))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (
        OSError,
        ValueError,
        subprocess.CalledProcessError,
        json.JSONDecodeError,
    ) as error:
        print(f"publication-v3 dataset staging failed: {error}", file=sys.stderr)
        raise SystemExit(2) from None
