#!/usr/bin/env python3
"""Observe and reap one Publication V3 staging attempt through the AWS CLI."""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import subprocess
import sys
import tempfile
from pathlib import Path
from urllib.parse import urlparse

if __package__:
    from scripts.publication_v3_aws import (
        AttemptObservation,
        _reconciliation_value,
        reconcile_staging_attempt,
        staging_jobs,
    )
    from scripts.publication_v3_protocol import canonical_json_bytes
else:
    from publication_v3_aws import (
        AttemptObservation,
        _reconciliation_value,
        reconcile_staging_attempt,
        staging_jobs,
    )
    from publication_v3_protocol import canonical_json_bytes


def _run(command: list[str], *, check: bool = True) -> subprocess.CompletedProcess[str]:
    completed = subprocess.run(command, capture_output=True, text=True)
    if check and completed.returncode != 0:
        raise ValueError(completed.stderr.strip() or "AWS CLI command failed")
    return completed


def _s3_location(uri: str) -> tuple[str, str]:
    parsed = urlparse(uri)
    key = parsed.path.lstrip("/")
    if parsed.scheme != "s3" or not parsed.netloc or not key:
        raise ValueError("staging marker URI is not canonical S3")
    return parsed.netloc, key


def _marker_head(base: list[str], uri: str, *, owner: str) -> dict[str, object] | None:
    bucket, key = _s3_location(uri)
    completed = _run(
        [
            *base,
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
    )
    if completed.returncode == 0:
        value = json.loads(completed.stdout)
        if not isinstance(value, dict):
            raise ValueError("S3 marker HEAD is not a JSON object")
        return value
    if "404" in completed.stderr or "Not Found" in completed.stderr or "NoSuchKey" in completed.stderr:
        return None
    raise ValueError(completed.stderr.strip() or "S3 marker HEAD failed")


def _validate_instance_identity(
    manifest: dict[str, object],
    instance: dict[str, object],
    *,
    dataset_id: str,
    attempt: int,
) -> None:
    tags = {
        str(item.get("Key")): str(item.get("Value"))
        for item in instance.get("Tags", [])
        if isinstance(item, dict)
    }
    expected = {
        "Project": "BorsukBenchmark",
        "Campaign": str(manifest["campaign_id"]),
        "Cell": f"stage-{dataset_id}",
        "Attempt": str(attempt),
        "Role": "staging",
        "AutoTerminate": "true",
    }
    if any(tags.get(key) != value for key, value in expected.items()):
        raise ValueError("EC2 instance identity differs from the staging attempt")
    if instance.get("InstanceLifecycle") != "spot":
        raise ValueError("EC2 staging instance is not Spot capacity")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("manifest", type=Path)
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--attempt", type=int, required=True)
    parser.add_argument("--instance-id", required=True)
    parser.add_argument("--profile", default="causality")
    parser.add_argument("--region", required=True)
    parser.add_argument("--max-attempts", type=int, default=3)
    parser.add_argument("--aws-command", default="aws")
    args = parser.parse_args()

    manifest = json.loads(args.manifest.read_text(encoding="utf-8"))
    jobs = {job.dataset_id: job for job in staging_jobs(manifest, attempt=args.attempt)}
    job = jobs.get(args.dataset)
    if job is None:
        raise ValueError("dataset is not an unstaged manifest dataset")
    base = [args.aws_command, "--profile", args.profile, "--region", args.region]
    described = _run([*base, "ec2", "describe-instances", "--instance-ids", args.instance_id])
    value = json.loads(described.stdout)
    instances = [
        instance
        for reservation in value.get("Reservations", [])
        for instance in reservation.get("Instances", [])
    ]
    if len(instances) != 1:
        raise ValueError("EC2 observation did not return exactly one instance")
    _validate_instance_identity(
        manifest,
        instances[0],
        dataset_id=job.dataset_id,
        attempt=job.attempt,
    )
    state = str(instances[0]["State"]["Name"])

    markers = []
    owner = str(manifest["environment_contract"]["aws_account"])
    complete_head = _marker_head(base, job.terminal_uri, owner=owner)
    failed_head = _marker_head(base, job.failure_uri, owner=owner)
    if complete_head is not None:
        markers.append("STAGING_COMPLETE.json")
    if failed_head is not None:
        markers.append("STAGING_FAILED.json")
    receipt = None
    with tempfile.TemporaryDirectory() as directory:
        if complete_head is not None:
            target = Path(directory) / "receipt.json"
            bucket, key = _s3_location(job.terminal_uri)
            _run(
                [
                    *base,
                    "s3api",
                    "get-object",
                    "--bucket",
                    bucket,
                    "--key",
                    key,
                    "--expected-bucket-owner",
                    owner,
                    "--checksum-mode",
                    "ENABLED",
                    str(target),
                ]
            )
            body = target.read_bytes()
            digest = hashlib.sha256(body).digest()
            encoded_digest = base64.b64encode(digest).decode("ascii")
            if (
                complete_head.get("ContentLength") != len(body)
                or complete_head.get("Metadata", {}).get("borsuk-sha256")
                != digest.hex()
                or complete_head.get("ChecksumSHA256") != encoded_digest
            ):
                raise ValueError("staging terminal receipt checksum differs")
            receipt = json.loads(body)
        reconciliation = reconcile_staging_attempt(
            manifest,
            job,
            instance_id=args.instance_id,
            observation=AttemptObservation(state, tuple(markers)),
            terminal_receipt=receipt,
            max_attempts=args.max_attempts,
        )
    if reconciliation.terminate_instance:
        _run([*base, "ec2", "terminate-instances", "--instance-ids", args.instance_id])
    print(canonical_json_bytes(_reconciliation_value(reconciliation)).decode("utf-8"))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, ValueError, KeyError, json.JSONDecodeError) as error:
        print(f"publication-v3 staging reconciliation failed: {error}", file=sys.stderr)
        raise SystemExit(2) from None
