#!/usr/bin/env python3
"""Stream authenticated historical BVP2 rows into the local RaBitQ constructor."""

from __future__ import annotations

import argparse
import dataclasses
import hashlib
import json
import os
import pathlib
import re
import signal
import struct
import subprocess
import tempfile
from collections.abc import Iterable, Sequence
from typing import Any, BinaryIO
from urllib.parse import urlsplit

import numpy
import pyarrow as pa

from scripts import v23_clustered_page_prototype_falsifier as historical

REGION = "eu-central-1"
INPUT_ROLES = ("tree-receipt", "incidence-tree", "page-roster")
OUTPUT_ROLES = ("row-codes", "leaf-offsets", "centroids", "rotation", "f16-control")
INPUT_BASENAMES = {
    "tree-receipt": "tree-receipt.json",
    "incidence-tree": "incidence-tree.bin",
    "page-roster": "page-roster.json",
}
OUTPUT_BASENAMES = {
    "row-codes": "row-codes.arrow",
    "leaf-offsets": "leaf-offsets.arrow",
    "centroids": "centroids.arrow",
    "rotation": "rotation.arrow",
    "f16-control": "f16-control.arrow",
}
LOWER_DIGEST = re.compile(r"[0-9a-f]{64}\Z")
LOWER_COMMIT = re.compile(r"[0-9a-f]{40}\Z")


@dataclasses.dataclass(frozen=True)
class FrozenArtifact:
    role: str
    uri: str
    sha256: str
    encoded_bytes: int
    basename: str


@dataclasses.dataclass(frozen=True)
class Occurrence:
    canonical_record_id: bytes
    vector: numpy.ndarray
    page_ordinal: int
    is_primary: bool


def _canonical_bytes(value: object) -> bytes:
    return (
        json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
        + b"\n"
    )


def _split_s3_uri(uri: str) -> tuple[str, str]:
    parsed = urlsplit(uri)
    if parsed.scheme != "s3" or not parsed.netloc or not parsed.path.lstrip("/"):
        raise ValueError("artifact URI differs")
    return parsed.netloc, parsed.path.lstrip("/")


def _default_s3_client() -> Any:
    import boto3
    from botocore.config import Config

    return boto3.Session(region_name=REGION).client(
        "s3",
        config=Config(
            connect_timeout=10,
            read_timeout=300,
            retries={"max_attempts": 3, "mode": "standard"},
        ),
    )


def _digest_file(path: pathlib.Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _validate_artifact(artifact: FrozenArtifact, role: str) -> None:
    if (
        type(artifact) is not FrozenArtifact
        or artifact.role != role
        or not artifact.basename
        or "/" in artifact.basename
        or LOWER_DIGEST.fullmatch(artifact.sha256) is None
        or type(artifact.encoded_bytes) is not int
        or artifact.encoded_bytes <= 0
    ):
        raise ValueError(f"{role} identity differs")
    _split_s3_uri(artifact.uri)


def _manifest_inputs(raw: bytes) -> tuple[dict[str, object], tuple[FrozenArtifact, ...]]:
    try:
        value = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("construction manifest JSON differs") from error
    keys = {
        "dataset_id",
        "expected_pages",
        "expected_source_occurrences",
        "expected_unique_rows",
        "index_id",
        "output_roles",
        "output_uri_prefix",
        "page_namespace_uri_prefix",
        "registered_inputs",
        "rotation_seed_hex",
        "run_mode",
        "schema",
        "source_archive_sha256",
        "source_commit",
    }
    if (
        type(value) is not dict
        or set(value) != keys
        or raw != _canonical_bytes(value)
        or value["schema"] != "borsuk-v23-rabitq-manifest-v1"
        or value["run_mode"] != {"execute": "construction"}
        or value["output_roles"] != list(OUTPUT_ROLES)
        or type(value["registered_inputs"]) is not list
        or type(value["output_uri_prefix"]) is not str
        or not value["output_uri_prefix"].startswith("s3://")
        or not value["output_uri_prefix"].endswith("/")
        or type(value["page_namespace_uri_prefix"]) is not str
        or not value["page_namespace_uri_prefix"].startswith("s3://")
        or not value["page_namespace_uri_prefix"].endswith("/")
        or type(value["expected_pages"]) is not int
        or value["expected_pages"] < 8
        or type(value["expected_unique_rows"]) is not int
        or value["expected_unique_rows"] <= 0
        or type(value["expected_source_occurrences"]) is not int
        or not value["expected_unique_rows"]
        <= value["expected_source_occurrences"]
        <= 2 * value["expected_unique_rows"]
        or type(value["rotation_seed_hex"]) is not str
        or LOWER_DIGEST.fullmatch(value["rotation_seed_hex"]) is None
        or type(value["source_commit"]) is not str
        or LOWER_COMMIT.fullmatch(value["source_commit"]) is None
        or type(value["source_archive_sha256"]) is not str
        or LOWER_DIGEST.fullmatch(value["source_archive_sha256"]) is None
    ):
        raise ValueError("construction manifest authority differs")
    identities = []
    identity_keys = {"blake3", "encoded_bytes", "role", "sha256", "uri"}
    for item, role in zip(value["registered_inputs"], INPUT_ROLES, strict=True):
        if (
            type(item) is not dict
            or set(item) != identity_keys
            or item["role"] != role
            or item["blake3"] is not None
            or type(item["uri"]) is not str
            or type(item["sha256"]) is not str
            or type(item["encoded_bytes"]) is not int
        ):
            raise ValueError(f"{role} manifest identity differs")
        artifact = FrozenArtifact(
            role=role,
            uri=item["uri"],
            sha256=item["sha256"],
            encoded_bytes=item["encoded_bytes"],
            basename=INPUT_BASENAMES[role],
        )
        _validate_artifact(artifact, role)
        identities.append(artifact)
    if len(identities) != len(INPUT_ROLES):
        raise ValueError("construction manifest input count differs")
    return value, tuple(identities)


def _read_roster(
    raw: bytes, manifest: dict[str, object]
) -> tuple[historical.PageRef, ...]:
    try:
        value = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("page roster JSON differs") from error
    keys = {
        "claim_eligible",
        "d1_report_sha256",
        "dataset_id",
        "document_kind",
        "index_id",
        "page_uri",
        "pages",
        "schema",
        "source_archive_sha256",
        "stage",
    }
    if (
        type(value) is not dict
        or set(value) != keys
        or raw != _canonical_bytes(value)
        or value["schema"] != "borsuk-v23-pages-v1"
        or value["document_kind"] != "publication-v3-v23-page-roster"
        or value["claim_eligible"] is not False
        or value["stage"] != "d2"
        or value["dataset_id"] != manifest["dataset_id"]
        or value["index_id"] != manifest["index_id"]
        or value["source_archive_sha256"] != manifest["source_archive_sha256"]
        or value["page_uri"] + "/" != manifest["page_namespace_uri_prefix"]
        or type(value["pages"]) is not list
        or len(value["pages"]) != manifest["expected_pages"]
    ):
        raise ValueError("page roster authority differs")
    pages = tuple(historical._page_from_json(item) for item in value["pages"])
    if tuple(page.page_ordinal for page in pages) != tuple(range(len(pages))):
        raise ValueError("page roster ordering differs")
    if sum(page.primary_rows + page.replicated_rows for page in pages) != manifest[
        "expected_source_occurrences"
    ] or sum(page.primary_rows for page in pages) != manifest["expected_unique_rows"]:
        raise ValueError("page roster row counts differ")
    return pages


def decode_bvp2_occurrences(
    reference: dict[str, object] | historical.PageRef, body: bytes
) -> tuple[Occurrence, ...]:
    page = (
        historical._page_from_json(reference)
        if type(reference) is dict
        else reference
    )
    if type(page) is not historical.PageRef:
        raise ValueError("page reference differs")
    vectors = historical.decode_bvp2_page(page, body)
    row_count = page.primary_rows + page.replicated_rows
    offset_bytes = (row_count + 1) * 4
    offsets = struct.unpack_from(f"<{row_count + 1}I", body, 96)
    id_start = 96 + offset_bytes
    identifiers = tuple(
        body[id_start + start : id_start + end]
        for start, end in zip(offsets, offsets[1:], strict=False)
    )
    return tuple(
        Occurrence(
            canonical_record_id=identifier,
            vector=vectors[row],
            page_ordinal=page.page_ordinal,
            is_primary=row < page.primary_rows,
        )
        for row, identifier in enumerate(identifiers)
    )


def _occurrence_schema() -> pa.Schema:
    return pa.schema(
        [
            pa.field("canonical_record_id", pa.binary(), nullable=False),
            pa.field(
                "vector",
                pa.list_(pa.field("element", pa.float32(), nullable=False), 96),
                nullable=False,
            ),
            pa.field("page_ordinal", pa.uint32(), nullable=False),
            pa.field("is_primary", pa.bool_(), nullable=False),
        ]
    )


def _occurrence_batch(rows: Sequence[Occurrence]) -> pa.RecordBatch:
    if not rows:
        raise ValueError("empty occurrence batch")
    vectors = numpy.stack([row.vector for row in rows]).astype(numpy.float32, copy=False)
    if vectors.shape != (len(rows), 96) or not numpy.isfinite(vectors).all():
        raise ValueError("occurrence vector differs")
    values = pa.array(vectors.reshape(-1), type=pa.float32())
    vector_array = pa.FixedSizeListArray.from_arrays(
        values,
        type=pa.list_(pa.field("element", pa.float32(), nullable=False), 96),
    )
    return pa.RecordBatch.from_arrays(
        [
            pa.array([row.canonical_record_id for row in rows], type=pa.binary()),
            vector_array,
            pa.array([row.page_ordinal for row in rows], type=pa.uint32()),
            pa.array([row.is_primary for row in rows], type=pa.bool_()),
        ],
        schema=_occurrence_schema(),
    )


def write_occurrence_stream(
    sink: BinaryIO, batches: Iterable[Sequence[Occurrence]]
) -> None:
    with pa.ipc.new_stream(sink, _occurrence_schema()) as writer:
        for rows in batches:
            writer.write_batch(_occurrence_batch(rows))


def _identity_from_file(role: str, uri: str, path: pathlib.Path) -> dict[str, object]:
    return {
        "blake3": None,
        "encoded_bytes": path.stat().st_size,
        "role": role,
        "sha256": _digest_file(path),
        "uri": uri,
    }


def _validate_receipt(
    raw: bytes, manifest: dict[str, object], output_directory: pathlib.Path
) -> dict[str, object]:
    try:
        value = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise RuntimeError("construction receipt JSON differs") from error
    keys = {
        "claim_eligible",
        "dataset_id",
        "index_id",
        "inputs",
        "manifest",
        "manifest_sha256",
        "outputs",
        "run_mode",
        "schema",
        "source_archive_sha256",
        "source_commit",
        "stop_reason",
        "terminal_status",
    }
    manifest_bytes = _canonical_bytes(manifest)
    if (
        type(value) is not dict
        or set(value) != keys
        or raw != _canonical_bytes(value)
        or value["schema"] != "borsuk-v23-rabitq-receipt-v1"
        or value["manifest_sha256"] != hashlib.sha256(manifest_bytes).hexdigest()
        or value["manifest"] != manifest
        or value["source_commit"] != manifest["source_commit"]
        or value["source_archive_sha256"] != manifest["source_archive_sha256"]
        or value["index_id"] != manifest["index_id"]
        or value["dataset_id"] != manifest["dataset_id"]
        or value["run_mode"] != manifest["run_mode"]
        or value["inputs"] != manifest["registered_inputs"]
        or value["terminal_status"] != "complete"
        or value["stop_reason"] is not None
        or value["claim_eligible"] is not False
        or type(value["outputs"]) is not list
        or len(value["outputs"]) != len(OUTPUT_ROLES)
    ):
        raise RuntimeError("construction receipt authority differs")
    for item, role in zip(value["outputs"], OUTPUT_ROLES, strict=True):
        path = output_directory / OUTPUT_BASENAMES[role]
        expected = _identity_from_file(
            role, manifest["output_uri_prefix"] + OUTPUT_BASENAMES[role], path
        )
        if item != expected:
            raise RuntimeError(f"{role} construction output authority differs")
    return value


def _development_manifest(
    construction: dict[str, object],
    receipt_identity: dict[str, object],
    d2_report: FrozenArtifact,
    query_parquet: FrozenArtifact,
    output_prefix: str,
) -> bytes:
    for artifact, role in ((d2_report, "d2-report"), (query_parquet, "query-parquet")):
        _validate_artifact(artifact, role)
    _split_s3_uri(output_prefix)
    if not output_prefix.endswith("/"):
        raise ValueError("development output prefix differs")
    construction_inputs = construction["manifest"]["registered_inputs"]
    inputs = [
        receipt_identity,
        construction_inputs[1],
        *construction["outputs"],
        dataclasses.asdict(d2_report) | {"blake3": None},
        dataclasses.asdict(query_parquet) | {"blake3": None},
    ]
    for item in inputs[-2:]:
        item.pop("basename")
    return _canonical_bytes(
        {
            "dataset_id": construction["dataset_id"],
            "expected_pages": construction["manifest"]["expected_pages"],
            "expected_source_occurrences": construction["manifest"][
                "expected_source_occurrences"
            ],
            "expected_unique_rows": construction["manifest"]["expected_unique_rows"],
            "index_id": construction["index_id"],
            "output_roles": ["screen-result"],
            "output_uri_prefix": output_prefix,
            "page_namespace_uri_prefix": None,
            "registered_inputs": inputs,
            "rotation_seed_hex": construction["manifest"]["rotation_seed_hex"],
            "run_mode": {"execute": "development"},
            "schema": "borsuk-v23-rabitq-manifest-v1",
            "source_archive_sha256": construction["source_archive_sha256"],
            "source_commit": construction["source_commit"],
        }
    )


def run_construction(
    *,
    binary: pathlib.Path,
    manifest: FrozenArtifact,
    d2_report: FrozenArtifact,
    query_parquet: FrozenArtifact,
    development_output_prefix: str,
    s3_client: Any | None = None,
    scratch_parent: pathlib.Path | None = None,
) -> dict[str, object]:
    binary = binary.resolve()
    _validate_artifact(manifest, "manifest")
    if not binary.is_absolute() or not binary.is_file():
        raise ValueError("construction binary differs")
    client = s3_client if s3_client is not None else _default_s3_client()
    scratch = pathlib.Path(
        tempfile.mkdtemp(
            prefix="v23-rabitq-construction-",
            dir=None if scratch_parent is None else str(scratch_parent),
        )
    )
    rust_scratch = scratch / "sort"
    output_directory = scratch / "output"
    rust_scratch.mkdir()
    output_directory.mkdir()
    manifest_path = scratch / manifest.basename
    local_inputs: dict[str, pathlib.Path] = {}
    development_path = scratch / "development-manifest.json"
    process: subprocess.Popen[bytes] | None = None
    try:
        bucket, key = _split_s3_uri(manifest.uri)
        client.download_file(bucket, key, str(manifest_path))
        if manifest_path.stat().st_size != manifest.encoded_bytes:
            raise ValueError("manifest length differs")
        if _digest_file(manifest_path) != manifest.sha256:
            raise ValueError("manifest digest differs")
        manifest_value, inputs = _manifest_inputs(manifest_path.read_bytes())
        for artifact in inputs:
            local = scratch / artifact.basename
            local_inputs[artifact.role] = local
            bucket, key = _split_s3_uri(artifact.uri)
            client.download_file(bucket, key, str(local))
            if local.stat().st_size != artifact.encoded_bytes:
                raise ValueError(f"{artifact.role} length differs")
            if _digest_file(local) != artifact.sha256:
                raise ValueError(f"{artifact.role} digest differs")
        pages = _read_roster(local_inputs["page-roster"].read_bytes(), manifest_value)
        command = [str(binary), "--manifest", str(manifest_path)]
        for artifact in inputs:
            command.extend([f"--{artifact.role}", str(local_inputs[artifact.role])])
        command.extend(
            [
                "--scratch-directory",
                str(rust_scratch),
                "--output-directory",
                str(output_directory),
            ]
        )
        for artifact in (manifest, *inputs):
            command.extend(
                [
                    f"--{artifact.role}-uri",
                    artifact.uri,
                    f"--{artifact.role}-sha256",
                    artifact.sha256,
                    f"--{artifact.role}-bytes",
                    str(artifact.encoded_bytes),
                ]
            )
        command.append("--execute-construction")
        process = subprocess.Popen(
            command,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            start_new_session=True,
        )
        if process.stdin is None:
            raise RuntimeError("construction stdin is absent")
        page_bucket, page_prefix = _split_s3_uri(manifest_value["page_namespace_uri_prefix"])
        batches = (
            decode_bvp2_occurrences(reference, body)
            for reference, body in historical.ordered_page_bodies(
                client, page_bucket, page_prefix, pages, max_inflight=4
            )
        )
        write_occurrence_stream(process.stdin, batches)
        process.stdin = None
        try:
            stdout, stderr = process.communicate(timeout=7_200)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGTERM)
            process.wait(timeout=30)
            raise TimeoutError("RaBitQ construction exceeded wall stop") from None
        if process.returncode != 0:
            raise RuntimeError(
                stderr.decode(errors="replace").strip()
                or f"RaBitQ construction exited {process.returncode}"
            )
        receipt = _validate_receipt(stdout, manifest_value, output_directory)
        receipt_path = output_directory / "construction-receipt.json"
        if receipt_path.read_bytes() != stdout:
            raise RuntimeError("construction receipt file differs")
        receipt_identity = _identity_from_file(
            "construction-receipt",
            manifest_value["output_uri_prefix"] + "construction-receipt.json",
            receipt_path,
        )
        development_path.write_bytes(
            _development_manifest(
                receipt,
                receipt_identity,
                d2_report,
                query_parquet,
                development_output_prefix,
            )
        )
        for basename in OUTPUT_BASENAMES.values():
            bucket, key = _split_s3_uri(manifest_value["output_uri_prefix"] + basename)
            client.upload_file(str(output_directory / basename), bucket, key)
        for path, uri in (
            (receipt_path, receipt_identity["uri"]),
            (development_path, manifest_value["output_uri_prefix"] + "development-manifest.json"),
        ):
            bucket, key = _split_s3_uri(uri)
            client.upload_file(str(path), bucket, key)
        return receipt
    finally:
        if process is not None and process.poll() is None:
            os.killpg(process.pid, signal.SIGTERM)
            process.wait(timeout=30)
        for basename in (*OUTPUT_BASENAMES.values(), "construction-receipt.json", "progress.json"):
            (output_directory / basename).unlink(missing_ok=True)
        development_path.unlink(missing_ok=True)
        for path in local_inputs.values():
            path.unlink(missing_ok=True)
        manifest_path.unlink(missing_ok=True)
        output_directory.rmdir()
        rust_scratch.rmdir()
        scratch.rmdir()


def parse_args(arguments: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=pathlib.Path, required=True)
    for role in ("manifest", "d2-report", "query-parquet"):
        parser.add_argument(f"--{role}-uri", required=True)
        parser.add_argument(f"--{role}-sha256", required=True)
        parser.add_argument(f"--{role}-bytes", type=int, required=True)
    parser.add_argument("--development-output-prefix", required=True)
    parser.add_argument("--execute-construction", action="store_true")
    values = parser.parse_args(arguments)
    if not values.execute_construction:
        parser.error("--execute-construction is required")
    return values


def main(arguments: Sequence[str] | None = None) -> int:
    values = parse_args(arguments)
    run_construction(
        binary=values.binary,
        manifest=FrozenArtifact(
            "manifest",
            values.manifest_uri,
            values.manifest_sha256,
            values.manifest_bytes,
            "manifest.json",
        ),
        d2_report=FrozenArtifact(
            "d2-report",
            values.d2_report_uri,
            values.d2_report_sha256,
            values.d2_report_bytes,
            "d2-report.json",
        ),
        query_parquet=FrozenArtifact(
            "query-parquet",
            values.query_parquet_uri,
            values.query_parquet_sha256,
            values.query_parquet_bytes,
            "query.parquet",
        ),
        development_output_prefix=values.development_output_prefix,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
