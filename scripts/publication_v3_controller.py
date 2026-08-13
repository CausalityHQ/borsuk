#!/usr/bin/env python3
"""Bounded, terminal-marker-only Publication V3 AWS orchestration."""

from __future__ import annotations

import argparse
import base64
import dataclasses
import hashlib
import json
import re
import subprocess
import tempfile
import time
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

if __package__:
    from scripts.publication_v3_aws import (
        build_spot_launch_request,
        build_staging_worker_script,
        staging_jobs,
        validate_staging_receipt,
    )
    from scripts.publication_v3_protocol import canonical_json_bytes, validate_manifest
else:
    from publication_v3_aws import (
        build_spot_launch_request,
        build_staging_worker_script,
        staging_jobs,
        validate_staging_receipt,
    )
    from publication_v3_protocol import canonical_json_bytes, validate_manifest


@dataclasses.dataclass(frozen=True)
class LaunchEnvironment:
    image_id: str
    subnet_id: str
    security_group_id: str
    instance_profile_arn: str
    image_architecture: str
    subnet_region: str


def _s3_location(uri: str) -> tuple[str, str]:
    parsed = urlparse(uri)
    key = parsed.path.lstrip("/")
    if parsed.scheme != "s3" or not parsed.netloc or not key:
        raise ValueError("publication object URI must be canonical S3")
    return parsed.netloc, key


class AwsCli:
    """Narrow AWS boundary used by the deterministic controller."""

    def __init__(
        self, manifest: dict[str, object], *, profile: str, run: Any = subprocess.run
    ) -> None:
        self.manifest = validate_manifest(manifest)
        environment = self.manifest["environment_contract"]
        self.region = str(environment["region"])
        self.owner = str(environment["aws_account"])
        self.base = ["aws", "--profile", profile, "--region", self.region]
        self._run_command = run

    def _run(
        self, tail: list[str], *, check: bool = True
    ) -> subprocess.CompletedProcess[str]:
        command = [*self.base, *tail]
        completed = self._run_command(
            command, check=False, capture_output=True, text=True
        )
        if check and completed.returncode != 0:
            raise ValueError(completed.stderr.strip() or "AWS CLI command failed")
        return completed

    def _head(self, uri: str) -> dict[str, object] | None:
        bucket, key = _s3_location(uri)
        completed = self._run(
            [
                "s3api",
                "head-object",
                "--bucket",
                bucket,
                "--key",
                key,
                "--expected-bucket-owner",
                self.owner,
                "--checksum-mode",
                "ENABLED",
                "--output",
                "json",
            ],
            check=False,
        )
        if completed.returncode == 0:
            value = json.loads(completed.stdout)
            if not isinstance(value, dict):
                raise ValueError("S3 HEAD response is not an object")
            return value
        if re.search(
            r"An error occurred \((?:404|NoSuchKey|NotFound)\)", completed.stderr
        ):
            return None
        raise ValueError(completed.stderr.strip() or "S3 HEAD failed")

    def terminal_markers(self, job: Any) -> tuple[str, ...]:
        markers = []
        if self._head(job.terminal_uri) is not None:
            markers.append("STAGING_COMPLETE.json")
        if self._head(job.failure_uri) is not None:
            markers.append("STAGING_FAILED.json")
        return tuple(markers)

    def read_receipt(self, job: Any) -> dict[str, object]:
        head = self._head(job.terminal_uri)
        if head is None:
            raise ValueError("staging receipt disappeared after terminal observation")
        bucket, key = _s3_location(job.terminal_uri)
        with tempfile.TemporaryDirectory(prefix="borsuk-staging-receipt-") as directory:
            target = Path(directory) / "receipt.json"
            self._run(
                [
                    "s3api",
                    "get-object",
                    "--bucket",
                    bucket,
                    "--key",
                    key,
                    "--expected-bucket-owner",
                    self.owner,
                    "--checksum-mode",
                    "ENABLED",
                    str(target),
                ]
            )
            body = target.read_bytes()
        digest = hashlib.sha256(body).digest()
        if (
            head.get("ContentLength") != len(body)
            or head.get("Metadata", {}).get("borsuk-sha256") != digest.hex()
            or head.get("ChecksumSHA256") != base64.b64encode(digest).decode("ascii")
        ):
            raise ValueError("staging receipt checksum differs")
        value = json.loads(body)
        if not isinstance(value, dict):
            raise ValueError("staging receipt is not an object")
        return value

    def find_instance(self, job: Any) -> tuple[str, str] | None:
        completed = self._run(
            [
                "ec2",
                "describe-instances",
                "--filters",
                f"Name=tag:Campaign,Values={self.manifest['campaign_id']}",
                f"Name=tag:Cell,Values=stage-{job.dataset_id}",
                f"Name=tag:Attempt,Values={job.attempt}",
                "Name=instance-state-name,Values=pending,running,stopping,stopped",
                "--output",
                "json",
            ]
        )
        value = json.loads(completed.stdout)
        instances = [
            instance
            for reservation in value.get("Reservations", [])
            for instance in reservation.get("Instances", [])
        ]
        if not instances:
            return None
        if len(instances) != 1:
            raise ValueError("staging attempt has multiple active instances")
        instance = instances[0]
        tags = {
            str(item.get("Key")): str(item.get("Value"))
            for item in instance.get("Tags", [])
            if isinstance(item, dict)
        }
        expected = {
            "Project": "BorsukBenchmark",
            "Campaign": str(self.manifest["campaign_id"]),
            "Cell": f"stage-{job.dataset_id}",
            "Attempt": str(job.attempt),
            "Role": "staging",
            "AutoTerminate": "true",
        }
        if (
            any(tags.get(key) != value for key, value in expected.items())
            or instance.get("InstanceLifecycle") != "spot"
        ):
            raise ValueError("EC2 instance identity differs from staging attempt")
        return str(instance["InstanceId"]), str(instance["State"]["Name"])

    def launch(self, _job: Any, request: dict[str, object]) -> str:
        completed = self._run(
            [
                "ec2",
                "run-instances",
                "--cli-input-json",
                json.dumps(request, sort_keys=True, separators=(",", ":")),
                "--query",
                "Instances[0].InstanceId",
                "--output",
                "text",
            ]
        )
        instance_id = completed.stdout.strip()
        if not instance_id.startswith("i-"):
            raise ValueError("AWS launch did not return an instance ID")
        return instance_id

    def instance_state(self, instance_id: str) -> str:
        completed = self._run(
            [
                "ec2",
                "describe-instances",
                "--instance-ids",
                instance_id,
                "--query",
                "Reservations[0].Instances[0].State.Name",
                "--output",
                "text",
            ]
        )
        return completed.stdout.strip()

    def terminate(self, instance_id: str) -> None:
        self._run(["ec2", "terminate-instances", "--instance-ids", instance_id])

    def wait(self, seconds: float) -> None:
        time.sleep(seconds)

    def upload_immutable(self, path: Path, uri: str, sha256: str) -> None:
        body = path.read_bytes()
        if hashlib.sha256(body).hexdigest() != sha256:
            raise ValueError("immutable upload checksum differs from local bytes")
        head = self._head(uri)
        if head is not None:
            expected_checksum = base64.b64encode(bytes.fromhex(sha256)).decode("ascii")
            if (
                head.get("ContentLength") != len(body)
                or head.get("Metadata", {}).get("borsuk-sha256") != sha256
                or head.get("ChecksumSHA256") != expected_checksum
            ):
                raise ValueError(
                    "immutable publication object already exists with different bytes"
                )
            return
        bucket, key = _s3_location(uri)
        checksum = base64.b64encode(bytes.fromhex(sha256)).decode("ascii")
        self._run(
            [
                "s3api",
                "put-object",
                "--bucket",
                bucket,
                "--key",
                key,
                "--body",
                str(path),
                "--expected-bucket-owner",
                self.owner,
                "--checksum-algorithm",
                "SHA256",
                "--checksum-sha256",
                checksum,
                "--metadata",
                f"borsuk-sha256={sha256}",
                "--server-side-encryption",
                "AES256",
                "--if-none-match",
                "*",
            ]
        )


def stage_dataset(
    manifest: dict[str, object],
    *,
    dataset_id: str,
    source_uri: str,
    source_archive_sha256: str,
    manifest_uri: str,
    manifest_sha256: str,
    launch: LaunchEnvironment,
    aws: Any,
    max_attempts: int = 4,
    poll_seconds: float = 15.0,
) -> dict[str, object]:
    normalized = validate_manifest(manifest)
    expected_manifest_sha = hashlib.sha256(canonical_json_bytes(normalized)).hexdigest()
    if manifest_sha256 != expected_manifest_sha:
        raise ValueError("staging manifest checksum differs from canonical bytes")
    if not 0 < max_attempts <= 9_999 or poll_seconds <= 0:
        raise ValueError("staging attempt cap and poll interval must be positive")
    max_seconds = int(normalized["budget_contract"]["max_index_build_seconds"])

    for attempt in range(1, max_attempts + 1):
        job = next(
            (
                j
                for j in staging_jobs(normalized, attempt=attempt)
                if j.dataset_id == dataset_id
            ),
            None,
        )
        if job is None:
            raise ValueError("dataset is not an unstaged manifest dataset")
        instance = aws.find_instance(job)
        deadline = time.monotonic() + max_seconds + 15 * 60
        try:
            while True:
                markers = set(aws.terminal_markers(job))
                if markers - {"STAGING_COMPLETE.json", "STAGING_FAILED.json"}:
                    raise ValueError("unrecognized staging terminal marker")
                if "STAGING_COMPLETE.json" in markers:
                    candidate = aws.read_receipt(job)
                    if candidate.get("attempt") != attempt:
                        raise ValueError(
                            "staging receipt differs from observed attempt"
                        )
                    if candidate.get("source_archive_sha256") != source_archive_sha256:
                        raise ValueError(
                            "staging receipt differs from frozen source archive"
                        )
                    if candidate.get("manifest_sha256") != expected_manifest_sha:
                        # Preserve immutable historical evidence and advance.
                        break
                    return validate_staging_receipt(normalized, candidate)
                if "STAGING_FAILED.json" in markers:
                    break
                if instance is None:
                    worker = build_staging_worker_script(
                        normalized,
                        job,
                        source_uri=source_uri,
                        source_archive_sha256=source_archive_sha256,
                        manifest_uri=manifest_uri,
                        manifest_sha256=manifest_sha256,
                    )
                    request = build_spot_launch_request(
                        normalized,
                        role="staging",
                        system="borsuk",
                        image_id=launch.image_id,
                        subnet_id=launch.subnet_id,
                        security_group_id=launch.security_group_id,
                        instance_profile_arn=launch.instance_profile_arn,
                        image_architecture=launch.image_architecture,
                        subnet_region=launch.subnet_region,
                        campaign_id=str(normalized["campaign_id"]),
                        cell_id=f"stage-{dataset_id}",
                        attempt=attempt,
                        worker_script=worker,
                        max_seconds=max_seconds,
                    )
                    instance = (aws.launch(job, request), "pending")
                else:
                    instance = (instance[0], aws.instance_state(instance[0]))
                    if instance[1] in {"stopped", "terminated"}:
                        break
                if time.monotonic() >= deadline:
                    break
                aws.wait(poll_seconds)
        finally:
            if instance is not None and instance[1] != "terminated":
                aws.terminate(instance[0])
    raise ValueError(f"staging dataset {dataset_id} exhausted {max_attempts} attempts")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="operation", required=True)
    stage = subparsers.add_parser("stage")
    stage.add_argument("--manifest", type=Path, required=True)
    stage.add_argument("--source-archive", type=Path, required=True)
    stage.add_argument("--dataset", required=True)
    stage.add_argument("--profile", default="causality")
    stage.add_argument("--image-id", required=True)
    stage.add_argument("--subnet-id", required=True)
    stage.add_argument("--security-group-id", required=True)
    stage.add_argument("--instance-profile-arn", required=True)
    stage.add_argument("--max-attempts", type=int, default=4)
    args = parser.parse_args()

    manifest = json.loads(args.manifest.read_text(encoding="utf-8"))
    normalized = validate_manifest(manifest)
    manifest_bytes = canonical_json_bytes(normalized)
    if args.manifest.read_bytes() != manifest_bytes:
        raise ValueError("paid controller requires a canonical frozen manifest")
    source_sha = hashlib.sha256(args.source_archive.read_bytes()).hexdigest()
    source = normalized.get("source", {})
    if source.get("state") != "frozen" or source.get("archive_sha256") != source_sha:
        raise ValueError("source archive differs from the frozen manifest")
    manifest_sha = hashlib.sha256(canonical_json_bytes(normalized)).hexdigest()
    dataset_prefix = str(normalized["prefixes"]["dataset"])
    if not dataset_prefix.endswith("/datasets"):
        raise ValueError("dataset prefix must end in /datasets")
    campaign_root = dataset_prefix.rsplit("/datasets", 1)[0]
    source_uri = f"{campaign_root}/source/{source_sha}.tar.gz"
    manifest_uri = f"{campaign_root}/manifests/{manifest_sha}.json"
    aws = AwsCli(normalized, profile=args.profile)
    aws.upload_immutable(args.source_archive, source_uri, source_sha)
    aws.upload_immutable(
        args.manifest, manifest_uri, hashlib.sha256(manifest_bytes).hexdigest()
    )
    receipt = stage_dataset(
        normalized,
        dataset_id=args.dataset,
        source_uri=source_uri,
        source_archive_sha256=source_sha,
        manifest_uri=manifest_uri,
        manifest_sha256=manifest_sha,
        launch=LaunchEnvironment(
            args.image_id,
            args.subnet_id,
            args.security_group_id,
            args.instance_profile_arn,
            str(normalized["environment_contract"]["architecture"]),
            str(normalized["environment_contract"]["region"]),
        ),
        aws=aws,
        max_attempts=args.max_attempts,
    )
    print(canonical_json_bytes(receipt).decode("utf-8"))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, ValueError, KeyError, json.JSONDecodeError) as error:
        print(
            f"publication-v3 controller failed: {error}", file=__import__("sys").stderr
        )
        raise SystemExit(2) from None
