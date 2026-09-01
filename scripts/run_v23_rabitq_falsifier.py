#!/usr/bin/env python3
"""Stage authenticated files and run the local-only V23 RaBitQ falsifier once."""

from __future__ import annotations

import argparse
import dataclasses
import hashlib
import json
import os
import pathlib
import re
import subprocess
import tempfile
from collections.abc import Sequence
from typing import Any
from urllib.parse import urlsplit

REGION = "eu-central-1"
INPUT_ROLES = (
    "construction-receipt",
    "incidence-tree",
    "row-codes",
    "leaf-offsets",
    "centroids",
    "rotation",
    "f16-control",
    "d2-report",
    "query-parquet",
)
ROLE_BASENAMES = {
    "construction-receipt": "construction-receipt.json",
    "incidence-tree": "incidence-tree.bin",
    "row-codes": "row-codes.arrow",
    "leaf-offsets": "leaf-offsets.arrow",
    "centroids": "centroids.arrow",
    "rotation": "rotation.arrow",
    "f16-control": "f16-control.arrow",
    "d2-report": "d2-report.json",
    "query-parquet": "query.parquet",
}
LOWER_DIGEST = re.compile(r"[0-9a-f]{64}\Z")


@dataclasses.dataclass(frozen=True)
class FrozenArtifact:
    role: str
    uri: str
    sha256: str
    encoded_bytes: int
    basename: str


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


def _validate_artifact(artifact: FrozenArtifact) -> None:
    if (
        not artifact.role
        or not artifact.basename
        or "/" in artifact.basename
        or _split_s3_uri(artifact.uri) == ("", "")
        or LOWER_DIGEST.fullmatch(artifact.sha256) is None
        or type(artifact.encoded_bytes) is not int
        or artifact.encoded_bytes <= 0
    ):
        raise ValueError(f"{artifact.role} identity differs")


def _manifest_inputs(raw: bytes) -> tuple[dict[str, object], tuple[FrozenArtifact, ...]]:
    try:
        value = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("manifest JSON differs") from error
    keys = {
        "dataset_id",
        "index_id",
        "registered_inputs",
        "output_roles",
        "output_uri_prefix",
        "page_namespace_uri_prefix",
        "rotation_seed_hex",
        "expected_pages",
        "expected_source_occurrences",
        "expected_unique_rows",
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
        or value["run_mode"] != {"execute": "development"}
        or type(value["registered_inputs"]) is not list
        or value["output_roles"] != ["screen-result"]
        or value["page_namespace_uri_prefix"] is not None
        or type(value["output_uri_prefix"]) is not str
        or not value["output_uri_prefix"].startswith("s3://")
        or not value["output_uri_prefix"].endswith("/")
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
    ):
        raise ValueError("manifest authority differs")
    artifacts = []
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
            basename=ROLE_BASENAMES[role],
        )
        _validate_artifact(artifact)
        artifacts.append(artifact)
    if len(artifacts) != len(INPUT_ROLES):
        raise ValueError("manifest input role count differs")
    return value, tuple(artifacts)


def run_falsifier(
    *,
    binary: pathlib.Path,
    manifest: FrozenArtifact,
    output: pathlib.Path,
    s3_client: Any | None = None,
    scratch_parent: pathlib.Path | None = None,
) -> None:
    binary = binary.resolve()
    output = output.resolve()
    _validate_artifact(manifest)
    if manifest.role != "manifest" or not binary.is_file() or not binary.is_absolute():
        raise ValueError("falsifier binary or manifest differs")
    if not output.is_absolute() or output.exists():
        raise ValueError("falsifier output path differs")
    client = s3_client if s3_client is not None else _default_s3_client()
    scratch = pathlib.Path(
        tempfile.mkdtemp(
            prefix="v23-rabitq-",
            dir=None if scratch_parent is None else str(scratch_parent),
        )
    )
    manifest_path = scratch / manifest.basename
    local_paths: dict[str, pathlib.Path] = {}
    partial_output = output.with_name(f".{output.name}.v23-rabitq-partial")
    if partial_output.exists():
        scratch.rmdir()
        raise ValueError("falsifier partial output already exists")
    try:
        bucket, key = _split_s3_uri(manifest.uri)
        client.download_file(bucket, key, str(manifest_path))
        if manifest_path.stat().st_size != manifest.encoded_bytes:
            raise ValueError("manifest length differs")
        if _digest_file(manifest_path) != manifest.sha256:
            raise ValueError("manifest digest differs")
        _, inputs = _manifest_inputs(manifest_path.read_bytes())
        for artifact in inputs:
            local = scratch / artifact.basename
            local_paths[artifact.role] = local
            bucket, key = _split_s3_uri(artifact.uri)
            client.download_file(bucket, key, str(local))
            if local.stat().st_size != artifact.encoded_bytes:
                raise ValueError(f"{artifact.role} length differs")
            if _digest_file(local) != artifact.sha256:
                raise ValueError(f"{artifact.role} digest differs")

        command = [str(binary), "--manifest", str(manifest_path)]
        for artifact in inputs:
            command.extend([f"--{artifact.role}", str(local_paths[artifact.role])])
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
        command.append("--execute-development")
        completed = subprocess.run(
            command,
            check=False,
            capture_output=True,
            timeout=7_200,
        )
        if completed.returncode != 0:
            detail = completed.stderr.decode(errors="replace").strip()
            raise RuntimeError(detail or f"RaBitQ falsifier exited {completed.returncode}")
        try:
            result = json.loads(completed.stdout)
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise RuntimeError("RaBitQ falsifier output JSON differs") from error
        if (
            completed.stdout != _canonical_bytes(result)
            or type(result) is not dict
            or result.get("claim_eligible") is not False
        ):
            raise RuntimeError("RaBitQ falsifier canonical result differs")
        output.parent.mkdir(parents=True, exist_ok=True)
        with partial_output.open("xb") as stream:
            stream.write(completed.stdout)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(partial_output, output)
    finally:
        partial_output.unlink(missing_ok=True)
        for path in local_paths.values():
            path.unlink(missing_ok=True)
        manifest_path.unlink(missing_ok=True)
        scratch.rmdir()


def parse_args(arguments: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=pathlib.Path, required=True)
    parser.add_argument("--manifest-uri", required=True)
    parser.add_argument("--manifest-sha256", required=True)
    parser.add_argument("--manifest-bytes", type=int, required=True)
    parser.add_argument("--output", type=pathlib.Path, required=True)
    parser.add_argument("--execute-development", action="store_true")
    values = parser.parse_args(arguments)
    if not values.execute_development:
        parser.error("--execute-development is required")
    return values


def main(arguments: Sequence[str] | None = None) -> int:
    values = parse_args(arguments)
    run_falsifier(
        binary=values.binary,
        manifest=FrozenArtifact(
            role="manifest",
            uri=values.manifest_uri,
            sha256=values.manifest_sha256,
            encoded_bytes=values.manifest_bytes,
            basename="manifest.json",
        ),
        output=values.output,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
