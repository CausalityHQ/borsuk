#!/usr/bin/env python3
"""Promote one sealed Publication V3 dataset and publish its receipt last."""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import re
import subprocess
import sys
import tempfile
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from urllib.parse import urlparse

if __package__:
    from scripts.publication_v3_aws import build_staging_receipt, staging_jobs
    from scripts.publication_v3_protocol import canonical_json_bytes, validate_manifest
    from scripts.stage_publication_v3_dataset import materialize_dataset
else:
    from publication_v3_aws import build_staging_receipt, staging_jobs
    from publication_v3_protocol import canonical_json_bytes, validate_manifest
    from stage_publication_v3_dataset import materialize_dataset


def _s3_parts(uri: str) -> tuple[str, str]:
    parsed = urlparse(uri)
    key = parsed.path.lstrip("/")
    if (
        parsed.scheme != "s3"
        or not parsed.netloc
        or not key
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError("publication object URI must be a canonical S3 URI")
    return parsed.netloc, key


def _head(
    uri: str,
    *,
    region: str,
    owner: str,
    run_command,
):
    bucket, key = _s3_parts(uri)
    return run_command(
        [
            "aws",
            "--region",
            region,
            "s3api",
            "head-object",
            "--bucket",
            bucket,
            "--key",
            key,
            "--expected-bucket-owner",
            owner,
            "--checksum-mode",
            "ENABLED",
        ],
        check=False,
        capture_output=True,
        text=True,
        env={**os.environ, "AWS_RETRY_MODE": "adaptive", "AWS_MAX_ATTEMPTS": "12"},
    )


def _missing(stderr: str) -> bool:
    return any(marker in stderr for marker in ("404", "Not Found", "NoSuchKey"))


def _precondition(stderr: str) -> bool:
    return any(
        marker in stderr
        for marker in ("412", "PreconditionFailed", "ConditionalRequestConflict")
    )


def _require_aws_cli(run_command) -> None:
    result = run_command(
        ["aws", "--version"], check=False, capture_output=True, text=True
    )
    match = re.search(r"aws-cli/(\d+)\.(\d+)", f"{result.stdout} {result.stderr}")
    if (
        result.returncode != 0
        or match is None
        or tuple(map(int, match.groups())) < (2, 19)
    ):
        raise RuntimeError("AWS CLI v2.19 or newer is required before materialization")


def _upload_verified(
    path: Path,
    uri: str,
    digest: str,
    *,
    region: str,
    owner: str,
    run_command,
) -> None:
    expected_size = path.stat().st_size
    expected_checksum = base64.b64encode(bytes.fromhex(digest)).decode("ascii")
    result = _head(uri, region=region, owner=owner, run_command=run_command)
    if result.returncode != 0:
        if not _missing(result.stderr):
            raise RuntimeError(f"HEAD failed for {uri}: {result.stderr.strip()}")
        bucket, key = _s3_parts(uri)
        upload = run_command(
            [
                "aws",
                "--region",
                region,
                "s3api",
                "put-object",
                "--bucket",
                bucket,
                "--key",
                key,
                "--body",
                str(path),
                "--server-side-encryption",
                "AES256",
                "--metadata",
                f"borsuk-sha256={digest}",
                "--checksum-sha256",
                expected_checksum,
                "--if-none-match",
                "*",
                "--expected-bucket-owner",
                owner,
            ],
            check=False,
            capture_output=True,
            text=True,
            env={**os.environ, "AWS_RETRY_MODE": "adaptive", "AWS_MAX_ATTEMPTS": "12"},
        )
        if upload.returncode != 0 and not _precondition(upload.stderr):
            raise RuntimeError(f"upload failed for {uri}: {upload.stderr.strip()}")
        result = _head(uri, region=region, owner=owner, run_command=run_command)
    if result.returncode != 0:
        raise RuntimeError(f"uploaded object is not readable: {uri}")
    try:
        value = json.loads(result.stdout)
    except json.JSONDecodeError as error:
        raise RuntimeError(f"uploaded object HEAD is not JSON: {uri}") from error
    if (
        value.get("ContentLength") != expected_size
        or value.get("Metadata", {}).get("borsuk-sha256") != digest
        or value.get("ChecksumSHA256") != expected_checksum
    ):
        raise RuntimeError(f"existing S3 object differs from sealed bytes: {uri}")


def _put_terminal(
    uri: str,
    body: bytes,
    *,
    region: str,
    owner: str,
    run_command,
) -> None:
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "terminal.json"
        path.write_bytes(body)
        _upload_verified(
            path,
            uri,
            hashlib.sha256(body).hexdigest(),
            region=region,
            owner=owner,
            run_command=run_command,
        )


def promote_sealed_attempt(
    manifest: dict[str, object],
    *,
    dataset_id: str,
    attempt: int,
    work_root: Path,
    source_archive_sha256: str,
    instance_id: str,
    instance_type: str,
    availability_zone: str,
    source_cache: Path | None = None,
    upload_workers: int = 16,
    run_command=subprocess.run,
) -> dict[str, object]:
    normalized = validate_manifest(manifest)
    if upload_workers <= 0 or upload_workers > 64:
        raise ValueError("upload workers must be in 1..=64")
    _require_aws_cli(run_command)
    jobs = [
        item
        for item in staging_jobs(normalized, attempt=attempt)
        if item.dataset_id == dataset_id
    ]
    if len(jobs) != 1:
        raise ValueError("dataset has no unique staging job")
    job = jobs[0]
    descriptor = materialize_dataset(
        normalized,
        dataset_id=dataset_id,
        attempt=attempt,
        work_root=work_root,
        source_cache=source_cache,
    )
    attempt_root = work_root / "attempts" / f"{attempt:04d}"
    provenance_path = attempt_root / "materialized.provenance.json"
    provenance_bytes = provenance_path.read_bytes()
    provenance = json.loads(provenance_bytes)
    region = str(normalized["environment_contract"]["region"])
    owner = str(normalized["environment_contract"]["aws_account"])

    def upload_item(item: dict[str, object]) -> dict[str, object]:
        uri = f"{job.output_uri}/{item['path']}"
        _upload_verified(
            attempt_root / "materialized" / str(item["path"]),
            uri,
            str(item["sha256"]),
            region=region,
            owner=owner,
            run_command=run_command,
        )
        return {
            **{key: value for key, value in item.items() if key != "path"},
            "uri": uri,
        }

    with ThreadPoolExecutor(max_workers=upload_workers) as executor:
        roster = list(executor.map(upload_item, descriptor["objects"]))
    provenance_digest = hashlib.sha256(provenance_bytes).hexdigest()
    _upload_verified(
        provenance_path,
        job.provenance_uri,
        provenance_digest,
        region=region,
        owner=owner,
        run_command=run_command,
    )
    receipt = build_staging_receipt(
        normalized,
        job,
        source_archive_sha256=source_archive_sha256,
        source_provenance=provenance,
        provenance_sha256=provenance_digest,
        objects=tuple(roster),
        instance_id=instance_id,
        instance_type=instance_type,
        availability_zone=availability_zone,
        purchase_option="spot",
    )
    _put_terminal(
        job.terminal_uri,
        canonical_json_bytes(receipt) + b"\n",
        region=region,
        owner=owner,
        run_command=run_command,
    )
    return receipt


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", required=True, type=Path)
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--attempt", required=True, type=int)
    parser.add_argument("--work-root", required=True, type=Path)
    parser.add_argument("--source-cache", type=Path)
    parser.add_argument("--source-archive-sha256", required=True)
    parser.add_argument("--instance-id", required=True)
    parser.add_argument("--instance-type", required=True)
    parser.add_argument("--availability-zone", required=True)
    parser.add_argument("--upload-workers", type=int, default=16)
    args = parser.parse_args()
    manifest = json.loads(args.manifest.read_text(encoding="utf-8"))
    receipt = promote_sealed_attempt(
        manifest,
        dataset_id=args.dataset,
        attempt=args.attempt,
        work_root=args.work_root,
        source_cache=args.source_cache,
        source_archive_sha256=args.source_archive_sha256,
        instance_id=args.instance_id,
        instance_type=args.instance_type,
        availability_zone=args.availability_zone,
        upload_workers=args.upload_workers,
    )
    print(canonical_json_bytes(receipt).decode("utf-8"))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, RuntimeError, ValueError, subprocess.CalledProcessError) as error:
        print(f"publication-v3 dataset promotion failed: {error}", file=sys.stderr)
        raise SystemExit(2) from None
