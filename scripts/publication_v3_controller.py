#!/usr/bin/env python3
"""Bounded, terminal-marker-only Publication V3 AWS orchestration."""

from __future__ import annotations

import argparse
import base64
import copy
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
        build_launch_request,
        build_staging_worker_script,
        staging_jobs,
        validate_staging_receipt,
    )
    from scripts.publication_v3_execution import (
        ExecutionJob,
        borsuk_cell,
        build_worker_script,
        qualification_cell,
        runtime_worker_script,
    )
    from scripts.publication_v3_protocol import (
        canonical_json_bytes,
        index_id,
        validate_manifest,
    )
    from scripts.publication_v3_receipts import (
        reconcile_index_inventory,
        require_verified_index,
        require_verified_object_roster,
    )
else:
    from publication_v3_aws import (
        build_launch_request,
        build_staging_worker_script,
        staging_jobs,
        validate_staging_receipt,
    )
    from publication_v3_execution import (
        ExecutionJob,
        borsuk_cell,
        build_worker_script,
        qualification_cell,
        runtime_worker_script,
    )
    from publication_v3_protocol import (
        canonical_json_bytes,
        index_id,
        validate_manifest,
    )
    from publication_v3_receipts import (
        reconcile_index_inventory,
        require_verified_index,
        require_verified_object_roster,
    )


@dataclasses.dataclass(frozen=True)
class LaunchEnvironment:
    image_id: str
    subnet_id: str
    security_group_id: str
    instance_profile_arn: str
    image_architecture: str
    subnet_region: str


@dataclasses.dataclass(frozen=True)
class PreparedExecution:
    job: ExecutionJob
    request: dict[str, object]
    expected: dict[str, object]
    timeout_seconds: int


@dataclasses.dataclass(frozen=True)
class BaseIndexAuthority:
    manifest: dict[str, object]
    manifest_uri: str
    manifest_sha256: str
    protocol_uri: str
    protocol_sha256: str
    source_uri: str
    source_archive_sha256: str
    source_git_commit: str
    build_terminal_uri: str
    build_terminal_sha256: str
    build_prefix: str
    build_cell_id: str
    build_attempt: int
    index_id: str
    index_uri: str
    index_receipt_sha256: str
    object_roster_sha256: str
    inventory_sha256: str


def _positive_integer_tuple(value: str) -> tuple[int, ...]:
    try:
        parsed = tuple(int(item) for item in value.split(","))
    except ValueError as error:
        raise argparse.ArgumentTypeError("expected comma-separated integers") from error
    if not parsed or any(item <= 0 for item in parsed):
        raise argparse.ArgumentTypeError("expected positive comma-separated integers")
    return parsed


def _minimum_lifecycle_write_ops(
    *, writers: int, batch_size: int, update_percent: int, delete_percent: int
) -> int:
    minimum_mutation_percent = min(update_percent, delete_percent)
    mutation_wave = writers * batch_size
    return (
        mutation_wave * 100 + minimum_mutation_percent - 1
    ) // minimum_mutation_percent


def _s3_location(uri: str) -> tuple[str, str]:
    parsed = urlparse(uri)
    key = parsed.path.lstrip("/")
    if parsed.scheme != "s3" or not parsed.netloc or not key:
        raise ValueError("publication object URI must be canonical S3")
    return parsed.netloc, key


def _execution_marker_outcome(markers: set[str]) -> str | None:
    """Resolve worker authority with controller observation as failure fallback."""

    if markers - {"complete", "failed", "controller-failed"}:
        raise ValueError("execution terminal markers differ")
    if {"complete", "failed"} <= markers:
        raise ValueError("execution terminal markers conflict")
    if "complete" in markers:
        return "complete"
    if "failed" in markers or "controller-failed" in markers:
        return "failed"
    return None


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

    def read_immutable_bytes(self, uri: str, sha256: str | None = None) -> bytes:
        head = self._head(uri)
        if head is None:
            raise ValueError("publication authority object is missing")
        bucket, key = _s3_location(uri)
        with tempfile.TemporaryDirectory(
            prefix="borsuk-publication-authority-"
        ) as directory:
            target = Path(directory) / "object"
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
        digest_hex = digest.hex()
        if (
            head.get("ContentLength") != len(body)
            or head.get("Metadata", {}).get("borsuk-sha256") != digest_hex
            or head.get("ChecksumSHA256") != base64.b64encode(digest).decode("ascii")
            or sha256 is not None
            and digest_hex != sha256
        ):
            raise ValueError("publication authority object checksum differs")
        return body

    def terminal_markers(self, job: Any) -> tuple[str, ...]:
        markers = []
        if self._head(job.terminal_uri) is not None:
            markers.append("STAGING_COMPLETE.json")
        if self._head(job.failure_uri) is not None:
            markers.append("STAGING_FAILED.json")
        return tuple(markers)

    def execution_markers(self, job: Any) -> tuple[str, ...]:
        markers = []
        if self._head(job.complete_uri) is not None:
            markers.append("complete")
        if self._head(job.failed_uri) is not None:
            markers.append("failed")
        if (
            self._head(f"{job.terminal_prefix}/CONTROLLER_TERMINAL_OBSERVED.json")
            is not None
        ):
            markers.append("controller-failed")
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

    def find_execution_instance(
        self, job: Any, *, purchase_option: str = "spot"
    ) -> tuple[str, str] | None:
        if purchase_option not in {"spot", "on-demand"}:
            raise ValueError("execution purchase option must be spot or on-demand")
        if purchase_option != "spot" and job.role != "runtime":
            raise ValueError("on-demand exception is allowed only for runtime")
        completed = self._run(
            [
                "ec2",
                "describe-instances",
                "--filters",
                f"Name=tag:Campaign,Values={self.manifest['campaign_id']}",
                f"Name=tag:Cell,Values={job.cell_tag}",
                f"Name=tag:Attempt,Values={job.attempt}",
                f"Name=tag:Role,Values={job.role}",
                "Name=instance-state-name,Values=pending,running,stopping,stopped,shutting-down,terminated",
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
            raise ValueError("execution attempt has multiple active instances")
        instance = instances[0]
        tags = {
            str(item.get("Key")): str(item.get("Value"))
            for item in instance.get("Tags", [])
            if isinstance(item, dict)
        }
        expected = {
            "Project": "BorsukBenchmark",
            "Campaign": str(self.manifest["campaign_id"]),
            "Cell": job.cell_tag,
            "Attempt": str(job.attempt),
            "Role": job.role,
            "PurchaseOption": purchase_option,
            "AutoTerminate": "true",
        }
        lifecycle = instance.get("InstanceLifecycle")
        lifecycle_matches = (
            lifecycle == "spot" if purchase_option == "spot" else lifecycle is None
        )
        if (
            any(
                tags.get(key) != expected_value
                for key, expected_value in expected.items()
            )
            or not lifecycle_matches
        ):
            raise ValueError("EC2 instance identity differs from execution attempt")
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
        expected_checksum = base64.b64encode(bytes.fromhex(sha256)).decode("ascii")

        def validate_existing(head: dict[str, object]) -> None:
            if (
                head.get("ContentLength") != len(body)
                or head.get("Metadata", {}).get("borsuk-sha256") != sha256
                or head.get("ChecksumSHA256") != expected_checksum
            ):
                raise ValueError(
                    "immutable publication object already exists with different bytes"
                )

        head = self._head(uri)
        if head is not None:
            validate_existing(head)
            return
        bucket, key = _s3_location(uri)
        completed = self._run(
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
                expected_checksum,
                "--metadata",
                f"borsuk-sha256={sha256}",
                "--server-side-encryption",
                "AES256",
                "--if-none-match",
                "*",
            ],
            check=False,
        )
        if completed.returncode == 0:
            return
        if re.search(r"PreconditionFailed|(^|[^0-9])412([^0-9]|$)", completed.stderr):
            raced = self._head(uri)
            if raced is not None:
                validate_existing(raced)
                return
        raise ValueError(completed.stderr.strip() or "immutable S3 upload failed")

    def record_markerless_execution_failure(
        self, job: Any, *, instance_id: str, instance_state: str
    ) -> None:
        """Persist a terminal EC2 attempt that produced no worker marker."""

        if instance_state not in {"stopped", "shutting-down", "terminated"}:
            raise ValueError("markerless execution failure requires terminal instance")
        if re.fullmatch(r"i-[0-9a-f]{17}", instance_id) is None:
            raise ValueError("markerless execution failure instance ID is invalid")
        if _execution_marker_outcome(set(self.execution_markers(job))) is not None:
            return
        receipt = {
            "schema_version": 1,
            "status": "failed",
            "role": job.role,
            "attempt": job.attempt,
            "attempt_id": f"{job.cell_tag}-a{job.attempt:04d}",
            "failure_kind": "instance-terminal-before-marker",
            "instance_id": instance_id,
        }
        body = canonical_json_bytes(receipt)
        digest = hashlib.sha256(body).hexdigest()
        with tempfile.TemporaryDirectory(
            prefix="borsuk-execution-failure-"
        ) as directory:
            path = Path(directory) / "failure.json"
            path.write_bytes(body)
            self.upload_immutable(
                path,
                f"{job.terminal_prefix}/CONTROLLER_TERMINAL_OBSERVED.json",
                digest,
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
    start_attempt: int = 1,
    max_attempts: int = 4,
    poll_seconds: float = 15.0,
) -> dict[str, object]:
    normalized = validate_manifest(manifest)
    expected_manifest_sha = hashlib.sha256(canonical_json_bytes(normalized)).hexdigest()
    if manifest_sha256 != expected_manifest_sha:
        raise ValueError("staging manifest checksum differs from canonical bytes")
    if not 0 < start_attempt <= max_attempts <= 9_999 or poll_seconds <= 0:
        raise ValueError("staging attempt range and poll interval must be positive")
    max_seconds = int(normalized["budget_contract"]["max_index_build_seconds"])

    for attempt in range(start_attempt, max_attempts + 1):
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
                    if candidate.get("manifest_sha256") != expected_manifest_sha:
                        # Preserve immutable historical evidence and advance.
                        break
                    if candidate.get("source_archive_sha256") != source_archive_sha256:
                        raise ValueError(
                            "staging receipt differs from frozen source archive"
                        )
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
                    request = build_launch_request(
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
                        terminal_failure_uri=job.failure_uri,
                        terminal_detail_log_path=(
                            "/var/lib/borsuk-publication-v3/"
                            f"{job.dataset_id}-a{attempt:04d}/worker.log"
                        ),
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


def _require_upload_reconciliation_count(value: dict[str, object]) -> int:
    count = value.get("artifact_upload_reconciliations")
    if isinstance(count, bool) or not isinstance(count, int) or count < 0:
        raise ValueError("execution receipt upload reconciliation count is invalid")
    return count


def run_execution_job(
    job: Any,
    *,
    request: dict[str, object],
    expected: dict[str, object],
    aws: Any,
    timeout_seconds: int,
    poll_seconds: float = 15.0,
    purchase_option: str = "spot",
) -> dict[str, object]:
    if timeout_seconds <= 0 or poll_seconds <= 0:
        raise ValueError("execution timeout and poll interval must be positive")
    if expected.get("purchase_option") != purchase_option:
        raise ValueError("execution authority differs from purchase option")
    instance = aws.find_execution_instance(job, purchase_option=purchase_option)
    deadline = time.monotonic() + timeout_seconds + 15 * 60
    try:
        while True:
            markers = set(aws.execution_markers(job))
            outcome = _execution_marker_outcome(markers)
            if outcome == "complete":
                value = aws.read_receipt(job)
                required = {
                    "schema_version": 5 if job.role == "runtime" else 2,
                    "status": "complete",
                    "role": job.role,
                    "attempt": job.attempt,
                    **expected,
                }
                if any(
                    value.get(key) != expected_value
                    for key, expected_value in required.items()
                ):
                    raise ValueError("execution receipt differs from frozen authority")
                _require_upload_reconciliation_count(value)
                if expected.get("runtime_profile") is not None:
                    digest_fields = ["binary_sha256", "execution_contract_sha256"]
                    if expected["runtime_profile"] == "concurrency":
                        digest_fields.extend(
                            (
                                "concurrency_summary_sha256",
                                "concurrency_samples_sha256",
                            )
                        )
                    elif expected["runtime_profile"] == "lifecycle":
                        digest_fields.extend(
                            (
                                "lifecycle_summary_sha256",
                                "lifecycle_costs_sha256",
                                "lifecycle_samples_sha256",
                                "lifecycle_query_summary_sha256",
                                "lifecycle_query_samples_sha256",
                                "lifecycle_storage_trace_sha256",
                            )
                        )
                        if expected.get("claim_eligible") is False:
                            digest_fields.append("diagnostic_result_sha256")
                    elif (
                        expected["runtime_profile"] == "recall"
                        and expected.get("claim_eligible") is False
                    ):
                        if expected.get("v21_feasibility") is True:
                            digest_fields.extend(
                                (
                                    "v21_result_sha256",
                                    "v21_arms_sha256",
                                    "v21_samples_sha256",
                                    "v21_summary_sha256",
                                )
                            )
                        elif expected.get("v22_stage_l") is True:
                            digest_fields.extend(
                                (
                                    "v22_result_sha256",
                                    "v22_report_sha256",
                                    "v22_summary_sha256",
                                )
                            )
                        else:
                            digest_fields.extend(
                                (
                                    "diagnostic_result_sha256",
                                    "diagnostic_samples_sha256",
                                    "diagnostic_summary_sha256",
                                )
                            )
                    if any(
                        not isinstance(value.get(field), str)
                        or re.fullmatch(r"[0-9a-f]{64}", value[field]) is None
                        for field in digest_fields
                    ):
                        raise ValueError("execution receipt artifact digest is invalid")
                    if (
                        expected.get("v21_feasibility") is True
                        or expected.get("v22_stage_l") is True
                    ):
                        memory_peak = value.get("memory_peak_bytes")
                        if (
                            isinstance(memory_peak, bool)
                            or not isinstance(memory_peak, int)
                            or memory_peak <= 0
                            or memory_peak > expected.get("memory_max_bytes", 0)
                        ):
                            raise ValueError(
                                "V21/V22 execution receipt memory peak is invalid"
                            )
                if job.role == "build" and value.get("index_uri") != job.index_uri:
                    raise ValueError("build receipt differs from scheduled index")
                return value
            if outcome == "failed":
                raise ValueError(f"{job.role} execution failed")
            if instance is None:
                instance = (aws.launch(job, request), "pending")
            else:
                instance = (instance[0], aws.instance_state(instance[0]))
                if instance[1] in {"stopped", "shutting-down", "terminated"}:
                    aws.record_markerless_execution_failure(
                        job, instance_id=instance[0], instance_state=instance[1]
                    )
                    if (
                        _execution_marker_outcome(set(aws.execution_markers(job)))
                        == "complete"
                    ):
                        continue
                    raise ValueError(
                        f"{job.role} instance stopped before terminal marker"
                    )
            if time.monotonic() >= deadline:
                raise ValueError(f"{job.role} execution exceeded its deadline")
            aws.wait(poll_seconds)
    finally:
        if instance is not None and instance[1] != "terminated":
            aws.terminate(instance[0])


def select_execution_attempt(
    job_for_attempt: Any,
    *,
    aws: Any,
    max_attempts: int = 6,
    purchase_option: str = "spot",
    require_complete: bool = False,
) -> int:
    """Select exact frozen authority without ever reusing a failed attempt."""

    if not 1 <= max_attempts <= 9_999:
        raise ValueError("execution attempt bound is invalid")
    for attempt in range(1, max_attempts + 1):
        job = job_for_attempt(attempt)
        markers = set(aws.execution_markers(job))
        outcome = _execution_marker_outcome(markers)
        if outcome == "failed":
            continue
        if outcome == "complete" or not require_complete:
            if outcome == "complete":
                return attempt
            instance = aws.find_execution_instance(job, purchase_option=purchase_option)
            if instance is not None and instance[1] in {
                "stopped",
                "shutting-down",
                "terminated",
            }:
                aws.record_markerless_execution_failure(
                    job, instance_id=instance[0], instance_state=instance[1]
                )
                recorded = _execution_marker_outcome(set(aws.execution_markers(job)))
                if recorded == "complete":
                    return attempt
                if recorded != "failed":
                    raise ValueError(
                        "markerless execution failure authority was not durable"
                    )
                continue
            return attempt
        instance = aws.find_execution_instance(job, purchase_option=purchase_option)
        if instance is not None and instance[1] in {
            "stopped",
            "shutting-down",
            "terminated",
        }:
            aws.record_markerless_execution_failure(
                job, instance_id=instance[0], instance_state=instance[1]
            )
            recorded = _execution_marker_outcome(set(aws.execution_markers(job)))
            if recorded == "complete":
                return attempt
            if recorded != "failed":
                raise ValueError(
                    "markerless execution failure authority was not durable"
                )
            continue
    if require_complete:
        raise ValueError("execution has no completed build attempt")
    raise ValueError("execution exhausted its bounded immutable attempts")


def completed_build_authority(
    job: ExecutionJob,
    *,
    aws: Any,
    expected: dict[str, str],
) -> dict[str, str]:
    """Load the exact immutable build required by a runtime cell."""

    if job.role != "build":
        raise ValueError("runtime authority requires a build job")
    if _execution_marker_outcome(set(aws.execution_markers(job))) != "complete":
        raise ValueError("required build is not complete")
    if set(expected) != {
        "source_archive_sha256",
        "manifest_sha256",
        "protocol_sha256",
    }:
        raise ValueError("build authority fields differ")
    value = aws.read_receipt(job)
    required: dict[str, object] = {
        **expected,
        "schema_version": 2,
        "status": "complete",
        "role": "build",
        "attempt": job.attempt,
        "attempt_id": f"{job.cell_tag}-a{job.attempt:04d}",
        "index_uri": job.index_uri,
        "purchase_option": "spot",
    }
    if any(
        value.get(key) != expected_value for key, expected_value in required.items()
    ):
        raise ValueError("completed build differs from frozen runtime authority")
    _require_upload_reconciliation_count(value)
    binary_sha256 = value.get("binary_sha256")
    if not isinstance(binary_sha256, str) or not re.fullmatch(
        r"[0-9a-f]{64}", binary_sha256
    ):
        raise ValueError("completed build has no canonical binary checksum")
    return {
        "binary_sha256": binary_sha256,
        "build_prefix": job.terminal_prefix,
    }


def _git_is_ancestor(ancestor: str, descendant: str) -> bool:
    if (
        re.fullmatch(r"[0-9a-f]{40}", ancestor) is None
        or re.fullmatch(r"[0-9a-f]{40}", descendant) is None
    ):
        raise ValueError("V21 source commit identity is not canonical")
    completed = subprocess.run(
        ["git", "merge-base", "--is-ancestor", ancestor, descendant],
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode not in {0, 1}:
        raise ValueError(completed.stderr.strip() or "V21 source ancestry check failed")
    return completed.returncode == 0


def _json_object(
    payload: bytes, *, newline: bool, canonical_keys: bool
) -> dict[str, object]:
    try:
        value = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("publication authority object is not JSON") from error
    expected = (
        canonical_json_bytes(value)
        if canonical_keys
        else json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode(
            "utf-8"
        )
    ) + (b"\n" if newline else b"")
    if not isinstance(value, dict) or payload != expected:
        raise ValueError("publication authority object is not canonical")
    return value


def _manifest_entry(
    manifest: dict[str, object], collection: str, identity: str
) -> dict[str, object]:
    values = manifest.get(collection)
    matches = (
        [
            value
            for value in values
            if isinstance(value, dict) and value.get("id") == identity
        ]
        if isinstance(values, list)
        else []
    )
    if len(matches) != 1:
        raise ValueError(f"V21 manifest {collection} authority differs")
    return matches[0]


def authenticate_v21_base_index_authority(
    *,
    current_manifest: dict[str, object],
    terminal_uri: str,
    terminal_sha256: str,
    aws: Any,
    git_is_ancestor: Any = _git_is_ancestor,
) -> BaseIndexAuthority:
    """Authenticate the exact historical Deep Image build before paid work."""

    try:
        current = validate_manifest(current_manifest)
        if re.fullmatch(r"[0-9a-f]{64}", terminal_sha256) is None:
            raise ValueError("base build terminal checksum is not canonical")
        if "/results/" not in terminal_uri or not terminal_uri.endswith(
            "/BUILD_TERMINAL_COMPLETE.json"
        ):
            raise ValueError("base build terminal URI is not canonical")
        _s3_location(terminal_uri)
        campaign_root = terminal_uri.split("/results/", 1)[0]
        terminal_payload = aws.read_immutable_bytes(terminal_uri, terminal_sha256)
        terminal = _json_object(terminal_payload, newline=True, canonical_keys=False)
        terminal_fields = {
            "schema_version",
            "status",
            "role",
            "attempt",
            "attempt_id",
            "instance_id",
            "source_archive_sha256",
            "manifest_sha256",
            "protocol_sha256",
            "index_uri",
            "binary_sha256",
            "rest_binary_sha256",
            "purchase_option",
            "artifact_upload_reconciliations",
        }
        if frozenset(terminal) != terminal_fields:
            raise ValueError("base build terminal fields differ")
        for field in (
            "source_archive_sha256",
            "manifest_sha256",
            "protocol_sha256",
            "binary_sha256",
            "rest_binary_sha256",
        ):
            if re.fullmatch(r"[0-9a-f]{64}", str(terminal[field])) is None:
                raise ValueError(f"base build terminal {field} differs")
        if (
            terminal["schema_version"] != 2
            or terminal["status"] != "complete"
            or terminal["role"] != "build"
            or terminal["purchase_option"] != "spot"
            or isinstance(terminal["attempt"], bool)
            or not isinstance(terminal["attempt"], int)
            or not 1 <= terminal["attempt"] <= 9_999
        ):
            raise ValueError("base build terminal authority differs")

        manifest_sha256 = str(terminal["manifest_sha256"])
        manifest_uri = f"{campaign_root}/manifests/{manifest_sha256}.json"
        manifest_payload = aws.read_immutable_bytes(manifest_uri, manifest_sha256)
        historical = validate_manifest(
            _json_object(manifest_payload, newline=False, canonical_keys=True)
        )
        source = historical.get("source")
        current_source = current.get("source")
        if (
            not isinstance(source, dict)
            or source.get("state") != "frozen"
            or source.get("archive_sha256") != terminal["source_archive_sha256"]
            or not isinstance(current_source, dict)
            or current_source.get("state") != "frozen"
            or not git_is_ancestor(
                str(source.get("git_commit")), str(current_source.get("git_commit"))
            )
        ):
            raise ValueError("base source authority differs")

        historical_dataset = _manifest_entry(historical, "datasets", "deep-image-96")
        current_dataset = _manifest_entry(current, "datasets", "deep-image-96")
        historical_workload = _manifest_entry(
            historical, "workloads", "standard-ann-read"
        )
        current_workload = _manifest_entry(current, "workloads", "standard-ann-read")
        historical_profiles = historical.get("index_profiles")
        current_profiles = current.get("index_profiles")
        historical_environment = historical.get("environment_contract")
        current_environment = current.get("environment_contract")
        if (
            historical.get("campaign_id") != current.get("campaign_id")
            or historical_dataset != current_dataset
            or historical_workload != current_workload
            or not isinstance(historical_profiles, dict)
            or not isinstance(current_profiles, dict)
            or historical_profiles.get("borsuk") != current_profiles.get("borsuk")
            or not isinstance(historical_environment, dict)
            or not isinstance(current_environment, dict)
            or historical_environment.get("architecture")
            != current_environment.get("architecture")
        ):
            raise ValueError("base manifest is incompatible with diagnostic authority")

        attempt = int(terminal["attempt"])
        cell = borsuk_cell(
            historical,
            workload_id="standard-ann-read",
            dataset_id="deep-image-96",
            repetition_id="r01",
            build_attempt=attempt,
        )
        job = ExecutionJob.build(cell, attempt=attempt)
        if (
            terminal_uri != job.complete_uri
            or terminal["attempt_id"] != f"{job.cell_tag}-a{attempt:04d}"
            or terminal["index_uri"] != job.index_uri
        ):
            raise ValueError("base build cell or index URI differs")

        protocol_sha256 = str(terminal["protocol_sha256"])
        protocol_uri = f"{campaign_root}/protocols/{protocol_sha256}.json"
        protocol_payload = aws.read_immutable_bytes(protocol_uri, protocol_sha256)
        if protocol_payload != canonical_json_bytes(cell) + b"\n":
            raise ValueError("base build protocol differs from its manifest")

        build_prefix = job.terminal_prefix
        receipt_payload = aws.read_immutable_bytes(
            f"{build_prefix}/INDEX_COMPLETE.json", None
        )
        receipt, receipt_sha256 = require_verified_index(
            receipt_payload,
            cell=cell,
            source_archive_sha256=str(terminal["source_archive_sha256"]),
            dataset_materialization_sha256=str(historical_dataset["source"]["sha256"]),
        )
        roster_payload = aws.read_immutable_bytes(
            f"{build_prefix}/INDEX_OBJECTS.json",
            str(receipt["object_roster_ref"]["checksum"]),
        )
        roster = require_verified_object_roster(receipt, roster_payload, cell=cell)
        inventory_payload = aws.read_immutable_bytes(
            f"{build_prefix}/INDEX_INVENTORY.json", None
        )
        inventory = json.loads(inventory_payload)
        if (
            not isinstance(inventory, list)
            or inventory_payload != canonical_json_bytes(inventory) + b"\n"
        ):
            raise ValueError("base index inventory is not canonical")
        reconcile_index_inventory(roster, inventory)

        expected_index_id = index_id(cell)
        return BaseIndexAuthority(
            manifest=copy.deepcopy(historical),
            manifest_uri=manifest_uri,
            manifest_sha256=manifest_sha256,
            protocol_uri=protocol_uri,
            protocol_sha256=protocol_sha256,
            source_uri=(
                f"{campaign_root}/source/{terminal['source_archive_sha256']}.tar.gz"
            ),
            source_archive_sha256=str(terminal["source_archive_sha256"]),
            source_git_commit=str(source["git_commit"]),
            build_terminal_uri=terminal_uri,
            build_terminal_sha256=terminal_sha256,
            build_prefix=build_prefix,
            build_cell_id=str(cell["cell_id"]),
            build_attempt=attempt,
            index_id=expected_index_id,
            index_uri=str(cell["index_prefix"]),
            index_receipt_sha256=receipt_sha256,
            object_roster_sha256=hashlib.sha256(roster_payload).hexdigest(),
            inventory_sha256=hashlib.sha256(inventory_payload).hexdigest(),
        )
    except (KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
        raise ValueError("V21 base-index authority differs") from error


def prepare_qualification_execution(
    manifest: dict[str, object],
    *,
    operation: str,
    workload_id: str | None = None,
    dataset_id: str | None = None,
    repetition_id: str = "r01",
    source_uri: str,
    source_sha256: str,
    manifest_uri: str,
    manifest_sha256: str,
    protocol_uri: str,
    protocol_sha256: str,
    build_protocol_sha256: str | None = None,
    launch: LaunchEnvironment,
    aws: Any,
    attempt: int = 1,
    build_attempt: int = 1,
    purchase_option: str = "spot",
    arm_index: int | None = None,
    diagnostic_write_ops: int = 2_560,
    diagnostic_timeout_seconds: int = 1_200,
    diagnostic_read_nprobes: tuple[int, ...] | None = None,
    diagnostic_read_candidates: tuple[int, ...] | None = None,
    v21_base_authority: BaseIndexAuthority | None = None,
    v22_base_authority: BaseIndexAuthority | None = None,
) -> PreparedExecution:
    """Prepare one immutable build or small-runtime qualification execution."""

    normalized = validate_manifest(manifest)
    supported = {
        "build-sift",
        "read-recall-sift",
        "read-concurrency-sift",
        "build-lifecycle",
        "run-lifecycle",
        "diagnose-lifecycle",
        "build-read",
        "run-read",
        "diagnose-read",
        "diagnose-v21-selector",
        "diagnose-v22-stage-l",
    }
    if operation not in supported:
        raise ValueError("unsupported qualification execution")
    lifecycle = operation in {
        "build-lifecycle",
        "run-lifecycle",
        "diagnose-lifecycle",
    }
    generic_read = operation in {
        "build-read",
        "run-read",
        "diagnose-read",
        "diagnose-v21-selector",
        "diagnose-v22-stage-l",
    }
    lifecycle_diagnostic = operation == "diagnose-lifecycle"
    read_diagnostic = operation == "diagnose-read"
    v21_feasibility = operation == "diagnose-v21-selector"
    v22_stage_l = operation == "diagnose-v22-stage-l"
    if v21_feasibility and v22_stage_l:
        raise ValueError("diagnostic operation authority is ambiguous")
    v2x_diagnostic = v21_feasibility or v22_stage_l
    diagnostic_label = (
        "V21 selector"
        if v21_feasibility
        else "V22 Stage L"
        if v22_stage_l
        else "non-diagnostic operation"
    )
    base_authority = v21_base_authority if v21_feasibility else v22_base_authority
    if (
        v2x_diagnostic != isinstance(base_authority, BaseIndexAuthority)
        or v21_feasibility
        and v22_base_authority is not None
        or v22_stage_l
        and v21_base_authority is not None
        or not v2x_diagnostic
        and (v21_base_authority is not None or v22_base_authority is not None)
    ):
        raise ValueError(
            f"{diagnostic_label} diagnostic requires its exact base-index authority"
        )
    diagnostic = lifecycle_diagnostic or read_diagnostic
    effective_arm_index = (
        13
        if lifecycle_diagnostic and arm_index is None
        else 0
        if arm_index is None
        else arm_index
    )
    if lifecycle_diagnostic and (
        isinstance(diagnostic_write_ops, bool)
        or not 1 <= diagnostic_write_ops <= 50_000
        or isinstance(diagnostic_timeout_seconds, bool)
        or diagnostic_timeout_seconds <= 0
        or diagnostic_timeout_seconds
        > int(normalized["budget_contract"]["max_cell_seconds"])
    ):
        raise ValueError("lifecycle diagnostic bounds are invalid")
    if read_diagnostic and (
        diagnostic_read_nprobes is None
        or diagnostic_read_candidates is None
        or not diagnostic_read_nprobes
        or not diagnostic_read_candidates
        or tuple(sorted(set(diagnostic_read_nprobes))) != diagnostic_read_nprobes
        or tuple(sorted(set(diagnostic_read_candidates))) != diagnostic_read_candidates
        or any(
            isinstance(value, bool) or not 1 <= value <= 256
            for value in diagnostic_read_nprobes
        )
        or any(
            isinstance(value, bool) or not 1 <= value <= 16_384
            for value in diagnostic_read_candidates
        )
        or len(diagnostic_read_nprobes) * len(diagnostic_read_candidates) > 32
    ):
        raise ValueError("read diagnostic bounds are invalid")
    if not read_diagnostic and any(
        value is not None
        for value in (
            diagnostic_read_nprobes,
            diagnostic_read_candidates,
        )
    ):
        raise ValueError("read diagnostic authority requires diagnose-read")
    if v2x_diagnostic and (
        workload_id != "standard-ann-read"
        or dataset_id != "deep-image-96"
        or repetition_id != "r01"
        or effective_arm_index != 0
        or purchase_option != "spot"
    ):
        raise ValueError(
            f"{diagnostic_label} diagnostic requires canonical Deep Image arm 0 on Spot"
        )
    if v2x_diagnostic:
        frozen_source = normalized.get("source")
        matching_datasets = [
            dataset
            for dataset in normalized["datasets"]
            if dataset.get("id") == "deep-image-96"
        ]
        dataset_source = (
            matching_datasets[0].get("source") if len(matching_datasets) == 1 else None
        )
        if (
            not isinstance(frozen_source, dict)
            or frozen_source.get("state") != "frozen"
            or frozen_source.get("archive_sha256") != source_sha256
            or hashlib.sha256(canonical_json_bytes(normalized)).hexdigest()
            != manifest_sha256
            or not isinstance(dataset_source, dict)
            or dataset_source.get("state") != "staged"
        ):
            raise ValueError(
                f"{diagnostic_label} diagnostic requires frozen source and staged Deep Image authority"
            )
    if generic_read:
        if not workload_id or not dataset_id:
            raise ValueError("generic read execution requires workload and dataset")
        if operation == "build-read" and repetition_id != "r01":
            raise ValueError("generic read builds must use canonical repetition r01")
    elif lifecycle:
        if not dataset_id:
            raise ValueError("lifecycle qualification requires a dataset")
    elif dataset_id not in {None, "sift-128"}:
        raise ValueError("SIFT qualification dataset differs")
    selected_dataset = dataset_id or "sift-128"
    build_operation = operation in {"build-sift", "build-lifecycle", "build-read"}
    if attempt <= 0 or build_attempt <= 0 or effective_arm_index < 0:
        raise ValueError("qualification attempts must be positive")
    if build_operation and effective_arm_index != 0:
        raise ValueError("build execution must use the canonical first arm")
    if purchase_option not in {"spot", "on-demand"}:
        raise ValueError("purchase option must be spot or on-demand")
    if build_operation and purchase_option != "spot":
        raise ValueError("build execution must use Spot")
    if generic_read:
        cell = borsuk_cell(
            normalized,
            workload_id=str(workload_id),
            dataset_id=selected_dataset,
            repetition_id=repetition_id,
            build_attempt=attempt if build_operation else build_attempt,
        )
    else:
        cell = qualification_cell(
            normalized,
            dataset_id=selected_dataset,
            workload_kind=(
                "write-update-delete-compact" if lifecycle else "read-recall"
            ),
            build_attempt=attempt if build_operation else build_attempt,
        )
    runtime_profile = (
        "lifecycle"
        if lifecycle
        else "concurrency"
        if operation == "read-concurrency-sift"
        else "recall"
    )
    runtime_client = normalized["environment_contract"]["runtime_clients"]["borsuk"]
    runtime_vcpus = int(runtime_client["vcpus"])
    max_active_searches = runtime_vcpus
    max_waiting_searches = 16
    if runtime_profile == "concurrency":
        max_active_searches = 16
        max_waiting_searches = 64
    leaf_read_width = 32
    max_inflight_leaf_reads = 96 if runtime_profile == "concurrency" else 48
    # Frozen c7g.xlarge uncached REST qualification found two decode/rank
    # slots to be the best small-runtime boundary: it sustained 224 QPS at
    # p99=74.31 ms and 97.705% recall. Three slots did not raise capacity and
    # regressed the same boundary to p99=89.18 ms, so do not simply match CPU
    # count here. Evidence: rest-amp2-wide-a16-d2-q224-sift-51f6fa1/attempts/0002
    # and rest-amp2-wide-a16-d3-q224-sift-51f6fa1/attempts/0001.
    max_parallel_decode_rank_tasks = 2
    cpu_threads = max(1, min(runtime_vcpus - 1, 4))
    io_threads = 160 if runtime_profile == "concurrency" else 88
    s3_get_concurrency = 128 if runtime_profile == "concurrency" else 64
    ram_budget_bytes = int(runtime_client["resident_limit_mib"]) * 1024 * 1024
    factors = cell["workload"]["factors"]
    if lifecycle:
        arms = [
            {
                "writers": writers,
                "batch_size": batch_size,
                "insert_mode": insert_mode,
                "update_percent": update_percent,
                "delete_percent": delete_percent,
            }
            for insert_mode in factors["insert_modes"]
            for writers in factors["writers"]
            for batch_size in factors["batch_sizes"]
            for update_percent in factors["update_percent"]
            for delete_percent in factors["delete_percent"]
        ]
    else:
        arms = [
            {
                "leaf_page_budget": leaf_page_budget,
                "cache_state": cache_state,
            }
            for leaf_page_budget in factors["leaf_page_budgets"]
            for cache_state in factors["cache_states"]
        ]
    if effective_arm_index >= len(arms):
        raise ValueError("qualification arm index is outside the factor matrix")
    arm = arms[effective_arm_index]
    if lifecycle_diagnostic:
        minimum_diagnostic_write_ops = _minimum_lifecycle_write_ops(
            writers=int(arm["writers"]),
            batch_size=int(arm["batch_size"]),
            update_percent=int(arm["update_percent"]),
            delete_percent=int(arm["delete_percent"]),
        )
        if diagnostic_write_ops < minimum_diagnostic_write_ops:
            raise ValueError(
                "lifecycle diagnostic write count must exercise every writer; "
                f"require at least {minimum_diagnostic_write_ops} operations"
            )
    cache_state = arm.get("cache_state", "cold")
    disk_cache_max_bytes = (
        0
        if cache_state == "cold"
        else int(runtime_client["disk_cache_limit_mib"]) * 1024 * 1024
    )
    # The uncached SIFT qualification measured amplification 2 as the best
    # throughput/latency balance: it coalesces adjacent authenticated ranges
    # without the byte overhead observed at amplification 3. Frozen evidence:
    # rest-amp2-wide-a16-sift-51f6fa1/attempts/0001.
    exact_read_max_physical_amplification = 2
    job = (
        ExecutionJob.build(cell, attempt=attempt)
        if build_operation
        else ExecutionJob.runtime(
            cell,
            attempt=attempt,
            profile=runtime_profile,
            arm_index=effective_arm_index,
            diagnostic=diagnostic,
            v21_feasibility=v21_feasibility,
            v22_stage_l=v22_stage_l,
        )
    )
    attempt_id = f"{job.cell_tag}-a{job.attempt:04d}"
    expected = {
        "attempt_id": attempt_id,
        "source_archive_sha256": source_sha256,
        "manifest_sha256": manifest_sha256,
        "protocol_sha256": protocol_sha256,
        "purchase_option": purchase_option,
    }
    if job.role == "build":
        worker = build_worker_script(
            job=job,
            source_uri=source_uri,
            source_sha256=source_sha256,
            manifest_uri=manifest_uri,
            manifest_sha256=manifest_sha256,
            protocol_uri=protocol_uri,
            protocol_sha256=protocol_sha256,
            attempt_id=attempt_id,
            terminal_prefix=job.terminal_prefix,
        )
        maximum = int(normalized["budget_contract"]["max_index_build_seconds"])
        role = "build"
    else:
        worker_base_authority = None
        if v2x_diagnostic:
            assert base_authority is not None
            if build_attempt != base_authority.build_attempt:
                raise ValueError("V21/V22 build attempt differs from base-index authority")
            base_cell = borsuk_cell(
                base_authority.manifest,
                workload_id="standard-ann-read",
                dataset_id="deep-image-96",
                repetition_id="r01",
                build_attempt=base_authority.build_attempt,
            )
            authority = {
                "build_prefix": base_authority.build_prefix,
                "binary_sha256": None,
            }
            worker_base_authority = {
                "manifest_uri": base_authority.manifest_uri,
                "manifest_sha256": base_authority.manifest_sha256,
                "protocol_uri": base_authority.protocol_uri,
                "protocol_sha256": base_authority.protocol_sha256,
                "build_terminal_uri": base_authority.build_terminal_uri,
                "build_terminal_sha256": base_authority.build_terminal_sha256,
                "build_prefix": base_authority.build_prefix,
                "source_archive_sha256": base_authority.source_archive_sha256,
                "cell": base_cell,
                "index_id": base_authority.index_id,
                "index_uri": base_authority.index_uri,
                "index_receipt_sha256": base_authority.index_receipt_sha256,
                "object_roster_sha256": base_authority.object_roster_sha256,
                "inventory_sha256": base_authority.inventory_sha256,
            }
        else:
            build_cell = (
                borsuk_cell(
                    normalized,
                    workload_id=str(workload_id),
                    dataset_id=selected_dataset,
                    repetition_id="r01",
                    build_attempt=build_attempt,
                )
                if generic_read
                else cell
            )
            build_job = ExecutionJob.build(build_cell, attempt=build_attempt)
            authority = completed_build_authority(
                build_job,
                aws=aws,
                expected={
                    "source_archive_sha256": source_sha256,
                    "manifest_sha256": manifest_sha256,
                    "protocol_sha256": build_protocol_sha256 or protocol_sha256,
                },
            )
        worker = runtime_worker_script(
            job=job,
            source_uri=source_uri,
            source_sha256=source_sha256,
            manifest_uri=manifest_uri,
            manifest_sha256=manifest_sha256,
            protocol_uri=protocol_uri,
            protocol_sha256=protocol_sha256,
            build_prefix=authority["build_prefix"],
            binary_sha256=authority["binary_sha256"],
            attempt_id=attempt_id,
            terminal_prefix=job.terminal_prefix,
            purchase_option=purchase_option,
            runtime_profile=runtime_profile,
            arm_index=effective_arm_index,
            arm=arm if lifecycle else None,
            disk_cache_max_bytes=disk_cache_max_bytes,
            exact_read_max_physical_amplification=(
                exact_read_max_physical_amplification
            ),
            max_active_searches=max_active_searches,
            max_waiting_searches=max_waiting_searches,
            leaf_read_width=leaf_read_width,
            max_inflight_leaf_reads=max_inflight_leaf_reads,
            max_parallel_decode_rank_tasks=max_parallel_decode_rank_tasks,
            cpu_threads=cpu_threads,
            io_threads=io_threads,
            s3_get_concurrency=s3_get_concurrency,
            ram_budget_bytes=ram_budget_bytes,
            diagnostic_write_ops=(
                diagnostic_write_ops if lifecycle_diagnostic else None
            ),
            diagnostic_timeout_seconds=(
                diagnostic_timeout_seconds if lifecycle_diagnostic else None
            ),
            diagnostic_read_nprobes=(
                diagnostic_read_nprobes if read_diagnostic else None
            ),
            diagnostic_read_candidates=(
                diagnostic_read_candidates if read_diagnostic else None
            ),
            v21_feasibility=v21_feasibility,
            v21_base_authority=worker_base_authority if v21_feasibility else None,
            v22_stage_l=v22_stage_l,
            v22_base_authority=worker_base_authority if v22_stage_l else None,
        )
        maximum = (
            diagnostic_timeout_seconds
            if lifecycle_diagnostic
            else int(normalized["budget_contract"]["max_cell_seconds"])
        )
        role = "diagnostic" if v2x_diagnostic else "runtime"
        if not v2x_diagnostic:
            expected["binary_sha256"] = authority["binary_sha256"]
        expected["runtime_profile"] = runtime_profile
        expected["arm_index"] = effective_arm_index
        expected["max_active_searches"] = max_active_searches
        expected["max_waiting_searches"] = max_waiting_searches
        expected["leaf_read_width"] = leaf_read_width
        expected["max_inflight_leaf_reads"] = max_inflight_leaf_reads
        expected["max_parallel_decode_rank_tasks"] = max_parallel_decode_rank_tasks
        expected["cpu_threads"] = cpu_threads
        expected["io_threads"] = io_threads
        expected["s3_get_concurrency"] = s3_get_concurrency
        expected["ram_budget_bytes"] = ram_budget_bytes
        expected["disk_cache_max_bytes"] = disk_cache_max_bytes
        expected["exact_read_max_physical_amplification"] = (
            exact_read_max_physical_amplification
        )
        if lifecycle_diagnostic:
            expected["claim_eligible"] = False
            expected["diagnostic_write_ops"] = diagnostic_write_ops
            expected["diagnostic_timeout_seconds"] = maximum
        elif read_diagnostic:
            expected["claim_eligible"] = False
            expected["diagnostic_read_nprobes"] = list(diagnostic_read_nprobes or ())
            expected["diagnostic_read_candidates"] = list(
                diagnostic_read_candidates or ()
            )
        elif v2x_diagnostic:
            assert base_authority is not None
            expected["claim_eligible"] = False
            expected["v21_feasibility" if v21_feasibility else "v22_stage_l"] = True
            expected.update(
                {
                    "base_build_terminal_sha256": (
                        base_authority.build_terminal_sha256
                    ),
                    "base_manifest_sha256": base_authority.manifest_sha256,
                    "base_protocol_sha256": base_authority.protocol_sha256,
                    "base_source_archive_sha256": (
                        base_authority.source_archive_sha256
                    ),
                    "base_index_receipt_sha256": (
                        base_authority.index_receipt_sha256
                    ),
                    "base_object_roster_sha256": (
                        base_authority.object_roster_sha256
                    ),
                    "base_inventory_sha256": base_authority.inventory_sha256,
                    "base_index_id": base_authority.index_id,
                    "base_index_uri": base_authority.index_uri,
                    "diagnostic_source_archive_sha256": source_sha256,
                    "memory_max_bytes": 34_359_738_368,
                    "memory_swap_max_bytes": 0,
                }
            )
    request = build_launch_request(
        normalized,
        role=role,
        system="borsuk",
        image_id=launch.image_id,
        subnet_id=launch.subnet_id,
        security_group_id=launch.security_group_id,
        instance_profile_arn=launch.instance_profile_arn,
        image_architecture=launch.image_architecture,
        subnet_region=launch.subnet_region,
        campaign_id=str(normalized["campaign_id"]),
        cell_id=job.cell_tag,
        attempt=job.attempt,
        worker_script=worker,
        terminal_failure_uri=job.failed_uri,
        terminal_detail_log_path="/var/lib/borsuk-publication/worker.log",
        max_seconds=maximum,
        purchase_option=purchase_option,
    )
    return PreparedExecution(job, request, expected, maximum)


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
    stage.add_argument("--start-attempt", type=int, default=1)
    stage.add_argument("--max-attempts", type=int, default=4)
    build = subparsers.add_parser("build-sift")
    build.add_argument("--manifest", type=Path, required=True)
    build.add_argument("--source-archive", type=Path, required=True)
    build.add_argument("--profile", default="causality")
    build.add_argument("--image-id", required=True)
    build.add_argument("--subnet-id", required=True)
    build.add_argument("--security-group-id", required=True)
    build.add_argument("--instance-profile-arn", required=True)
    build.add_argument("--attempt", type=int, default=1)
    generic_build = subparsers.add_parser("build-read")
    generic_build.add_argument("--manifest", type=Path, required=True)
    generic_build.add_argument("--source-archive", type=Path, required=True)
    generic_build.add_argument("--workload", required=True)
    generic_build.add_argument("--dataset", required=True)
    generic_build.add_argument("--profile", default="causality")
    generic_build.add_argument("--image-id", required=True)
    generic_build.add_argument("--subnet-id", required=True)
    generic_build.add_argument("--security-group-id", required=True)
    generic_build.add_argument("--instance-profile-arn", required=True)
    generic_build.add_argument("--attempt", type=int, default=0)
    generic_build.add_argument("--max-attempts", type=int, default=6)
    generic_runtime = subparsers.add_parser("run-read")
    generic_runtime.add_argument("--manifest", type=Path, required=True)
    generic_runtime.add_argument("--source-archive", type=Path, required=True)
    generic_runtime.add_argument("--workload", required=True)
    generic_runtime.add_argument("--dataset", required=True)
    generic_runtime.add_argument("--repetition", required=True)
    generic_runtime.add_argument("--profile", default="causality")
    generic_runtime.add_argument("--image-id", required=True)
    generic_runtime.add_argument("--subnet-id", required=True)
    generic_runtime.add_argument("--security-group-id", required=True)
    generic_runtime.add_argument("--instance-profile-arn", required=True)
    generic_runtime.add_argument("--attempt", type=int, default=0)
    generic_runtime.add_argument("--build-attempt", type=int, default=0)
    generic_runtime.add_argument("--max-attempts", type=int, default=6)
    generic_runtime.add_argument("--arm-index", type=int, required=True)
    generic_runtime.add_argument(
        "--purchase-option", choices=("spot", "on-demand"), default="spot"
    )
    read_diagnostic = subparsers.add_parser("diagnose-read")
    read_diagnostic.add_argument("--manifest", type=Path, required=True)
    read_diagnostic.add_argument("--source-archive", type=Path, required=True)
    read_diagnostic.add_argument("--workload", required=True)
    read_diagnostic.add_argument("--dataset", required=True)
    read_diagnostic.add_argument("--repetition", default="r01")
    read_diagnostic.add_argument("--profile", default="causality")
    read_diagnostic.add_argument("--image-id", required=True)
    read_diagnostic.add_argument("--subnet-id", required=True)
    read_diagnostic.add_argument("--security-group-id", required=True)
    read_diagnostic.add_argument("--instance-profile-arn", required=True)
    read_diagnostic.add_argument("--attempt", type=int, default=0)
    read_diagnostic.add_argument("--build-attempt", type=int, default=0)
    read_diagnostic.add_argument("--max-attempts", type=int, default=6)
    read_diagnostic.add_argument("--arm-index", type=int, default=0)
    read_diagnostic.add_argument(
        "--nprobes", type=_positive_integer_tuple, default=(32, 64)
    )
    read_diagnostic.add_argument(
        "--candidates",
        type=_positive_integer_tuple,
        default=(512, 1_024, 2_048, 4_096),
    )
    read_diagnostic.add_argument(
        "--purchase-option", choices=("spot", "on-demand"), default="spot"
    )
    v21_diagnostic = subparsers.add_parser("diagnose-v21-selector")
    v21_diagnostic.add_argument("--manifest", type=Path, required=True)
    v21_diagnostic.add_argument("--source-archive", type=Path, required=True)
    v21_diagnostic.add_argument("--workload", default="standard-ann-read")
    v21_diagnostic.add_argument("--dataset", default="deep-image-96")
    v21_diagnostic.add_argument("--repetition", default="r01")
    v21_diagnostic.add_argument("--profile", default="causality")
    v21_diagnostic.add_argument("--image-id", required=True)
    v21_diagnostic.add_argument("--subnet-id", required=True)
    v21_diagnostic.add_argument("--security-group-id", required=True)
    v21_diagnostic.add_argument("--instance-profile-arn", required=True)
    v21_diagnostic.add_argument("--attempt", type=int, default=0)
    v21_diagnostic.add_argument("--max-attempts", type=int, default=6)
    v21_diagnostic.add_argument("--arm-index", type=int, default=0)
    v21_diagnostic.add_argument("--purchase-option", choices=("spot",), default="spot")
    v21_diagnostic.add_argument("--base-build-terminal-uri", required=True)
    v21_diagnostic.add_argument("--base-build-terminal-sha256", required=True)
    v22_diagnostic = subparsers.add_parser("diagnose-v22-stage-l")
    v22_diagnostic.add_argument("--manifest", type=Path, required=True)
    v22_diagnostic.add_argument("--source-archive", type=Path, required=True)
    v22_diagnostic.add_argument("--workload", default="standard-ann-read")
    v22_diagnostic.add_argument("--dataset", default="deep-image-96")
    v22_diagnostic.add_argument("--repetition", default="r01")
    v22_diagnostic.add_argument("--profile", default="causality")
    v22_diagnostic.add_argument("--image-id", required=True)
    v22_diagnostic.add_argument("--subnet-id", required=True)
    v22_diagnostic.add_argument("--security-group-id", required=True)
    v22_diagnostic.add_argument("--instance-profile-arn", required=True)
    v22_diagnostic.add_argument("--attempt", type=int, default=0)
    v22_diagnostic.add_argument("--max-attempts", type=int, default=6)
    v22_diagnostic.add_argument("--arm-index", type=int, default=0)
    v22_diagnostic.add_argument("--purchase-option", choices=("spot",), default="spot")
    v22_diagnostic.add_argument("--base-build-terminal-uri", required=True)
    v22_diagnostic.add_argument("--base-build-terminal-sha256", required=True)
    runtime = subparsers.add_parser("read-recall-sift")
    runtime.add_argument("--manifest", type=Path, required=True)
    runtime.add_argument("--source-archive", type=Path, required=True)
    runtime.add_argument("--profile", default="causality")
    runtime.add_argument("--image-id", required=True)
    runtime.add_argument("--subnet-id", required=True)
    runtime.add_argument("--security-group-id", required=True)
    runtime.add_argument("--instance-profile-arn", required=True)
    runtime.add_argument("--attempt", type=int, default=1)
    runtime.add_argument("--build-attempt", type=int, default=1)
    runtime.add_argument("--arm-index", type=int, default=0)
    runtime.add_argument(
        "--purchase-option", choices=("spot", "on-demand"), default="spot"
    )
    concurrency = subparsers.add_parser("read-concurrency-sift")
    concurrency.add_argument("--manifest", type=Path, required=True)
    concurrency.add_argument("--source-archive", type=Path, required=True)
    concurrency.add_argument("--profile", default="causality")
    concurrency.add_argument("--image-id", required=True)
    concurrency.add_argument("--subnet-id", required=True)
    concurrency.add_argument("--security-group-id", required=True)
    concurrency.add_argument("--instance-profile-arn", required=True)
    concurrency.add_argument("--attempt", type=int, default=1)
    concurrency.add_argument("--build-attempt", type=int, default=1)
    concurrency.add_argument("--arm-index", type=int, default=0)
    concurrency.add_argument(
        "--purchase-option", choices=("spot", "on-demand"), default="spot"
    )
    lifecycle_build = subparsers.add_parser("build-lifecycle")
    lifecycle_build.add_argument("--manifest", type=Path, required=True)
    lifecycle_build.add_argument("--source-archive", type=Path, required=True)
    lifecycle_build.add_argument("--dataset", required=True)
    lifecycle_build.add_argument("--profile", default="causality")
    lifecycle_build.add_argument("--image-id", required=True)
    lifecycle_build.add_argument("--subnet-id", required=True)
    lifecycle_build.add_argument("--security-group-id", required=True)
    lifecycle_build.add_argument("--instance-profile-arn", required=True)
    lifecycle_build.add_argument("--attempt", type=int, default=1)
    lifecycle_runtime = subparsers.add_parser("run-lifecycle")
    lifecycle_runtime.add_argument("--manifest", type=Path, required=True)
    lifecycle_runtime.add_argument("--source-archive", type=Path, required=True)
    lifecycle_runtime.add_argument("--dataset", required=True)
    lifecycle_runtime.add_argument("--profile", default="causality")
    lifecycle_runtime.add_argument("--image-id", required=True)
    lifecycle_runtime.add_argument("--subnet-id", required=True)
    lifecycle_runtime.add_argument("--security-group-id", required=True)
    lifecycle_runtime.add_argument("--instance-profile-arn", required=True)
    lifecycle_runtime.add_argument("--attempt", type=int, default=1)
    lifecycle_runtime.add_argument("--build-attempt", type=int, default=1)
    lifecycle_runtime.add_argument("--arm-index", type=int, required=True)
    lifecycle_runtime.add_argument(
        "--purchase-option", choices=("spot", "on-demand"), default="spot"
    )
    lifecycle_diagnostic = subparsers.add_parser("diagnose-lifecycle")
    lifecycle_diagnostic.add_argument("--manifest", type=Path, required=True)
    lifecycle_diagnostic.add_argument("--source-archive", type=Path, required=True)
    lifecycle_diagnostic.add_argument("--dataset", required=True)
    lifecycle_diagnostic.add_argument("--profile", default="causality")
    lifecycle_diagnostic.add_argument("--image-id", required=True)
    lifecycle_diagnostic.add_argument("--subnet-id", required=True)
    lifecycle_diagnostic.add_argument("--security-group-id", required=True)
    lifecycle_diagnostic.add_argument("--instance-profile-arn", required=True)
    lifecycle_diagnostic.add_argument("--attempt", type=int, default=1)
    lifecycle_diagnostic.add_argument("--build-attempt", type=int, default=1)
    lifecycle_diagnostic.add_argument("--arm-index", type=int, default=13)
    lifecycle_diagnostic.add_argument("--write-ops", type=int, default=2_560)
    lifecycle_diagnostic.add_argument("--timeout-seconds", type=int, default=1_200)
    lifecycle_diagnostic.add_argument(
        "--purchase-option", choices=("spot", "on-demand"), default="spot"
    )
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
    v21_base_authority = (
        authenticate_v21_base_index_authority(
            current_manifest=normalized,
            terminal_uri=args.base_build_terminal_uri,
            terminal_sha256=args.base_build_terminal_sha256,
            aws=aws,
        )
        if args.operation in {"diagnose-v21-selector", "diagnose-v22-stage-l"}
        else None
    )
    aws.upload_immutable(args.source_archive, source_uri, source_sha)
    aws.upload_immutable(
        args.manifest, manifest_uri, hashlib.sha256(manifest_bytes).hexdigest()
    )
    launch = LaunchEnvironment(
        args.image_id,
        args.subnet_id,
        args.security_group_id,
        args.instance_profile_arn,
        str(normalized["environment_contract"]["architecture"]),
        str(normalized["environment_contract"]["region"]),
    )
    if args.operation == "stage":
        receipt = stage_dataset(
            normalized,
            dataset_id=args.dataset,
            source_uri=source_uri,
            source_archive_sha256=source_sha,
            manifest_uri=manifest_uri,
            manifest_sha256=manifest_sha,
            launch=launch,
            aws=aws,
            start_attempt=args.start_attempt,
            max_attempts=args.max_attempts,
        )
    else:
        lifecycle = args.operation in {
            "build-lifecycle",
            "run-lifecycle",
            "diagnose-lifecycle",
        }
        generic_read = args.operation in {
            "build-read",
            "run-read",
            "diagnose-read",
            "diagnose-v21-selector",
            "diagnose-v22-stage-l",
        }
        read_diagnostic = args.operation == "diagnose-read"
        v21_feasibility = args.operation == "diagnose-v21-selector"
        v22_stage_l = args.operation == "diagnose-v22-stage-l"
        v2x_diagnostic = v21_feasibility or v22_stage_l
        build_operation = args.operation in {
            "build-sift",
            "build-lifecycle",
            "build-read",
        }
        execution_attempt = getattr(args, "attempt", 1)
        build_attempt = (
            v21_base_authority.build_attempt
            if v2x_diagnostic and v21_base_authority is not None
            else execution_attempt
            if build_operation
            else args.build_attempt
        )
        if generic_read and build_operation and execution_attempt == 0:
            execution_attempt = select_execution_attempt(
                lambda attempt: ExecutionJob.build(
                    borsuk_cell(
                        normalized,
                        workload_id=args.workload,
                        dataset_id=args.dataset,
                        repetition_id="r01",
                        build_attempt=attempt,
                    ),
                    attempt=attempt,
                ),
                aws=aws,
                max_attempts=args.max_attempts,
            )
            build_attempt = execution_attempt
        elif (
            generic_read
            and not build_operation
            and not v2x_diagnostic
            and build_attempt == 0
        ):
            build_attempt = select_execution_attempt(
                lambda attempt: ExecutionJob.build(
                    borsuk_cell(
                        normalized,
                        workload_id=args.workload,
                        dataset_id=args.dataset,
                        repetition_id="r01",
                        build_attempt=attempt,
                    ),
                    attempt=attempt,
                ),
                aws=aws,
                max_attempts=args.max_attempts,
                require_complete=True,
            )
        if generic_read and not build_operation and execution_attempt == 0:
            execution_attempt = select_execution_attempt(
                lambda attempt: ExecutionJob.runtime(
                    borsuk_cell(
                        normalized,
                        workload_id=args.workload,
                        dataset_id=args.dataset,
                        repetition_id=args.repetition,
                        build_attempt=build_attempt,
                    ),
                    attempt=attempt,
                    profile="recall",
                    arm_index=args.arm_index,
                    diagnostic=read_diagnostic,
                    v21_feasibility=v21_feasibility,
                    v22_stage_l=v22_stage_l,
                ),
                aws=aws,
                max_attempts=args.max_attempts,
                purchase_option=args.purchase_option,
            )
        if generic_read:
            cell = borsuk_cell(
                normalized,
                workload_id=args.workload,
                dataset_id=args.dataset,
                repetition_id="r01" if build_operation else args.repetition,
                build_attempt=build_attempt,
            )
            build_cell = borsuk_cell(
                normalized,
                workload_id=args.workload,
                dataset_id=args.dataset,
                repetition_id="r01",
                build_attempt=build_attempt,
            )
        else:
            cell = qualification_cell(
                normalized,
                dataset_id=args.dataset if lifecycle else "sift-128",
                workload_kind=(
                    "write-update-delete-compact" if lifecycle else "read-recall"
                ),
                build_attempt=build_attempt,
            )
            build_cell = cell
        protocol_bytes = canonical_json_bytes(cell) + b"\n"
        protocol_sha = hashlib.sha256(protocol_bytes).hexdigest()
        protocol_uri = f"{campaign_root}/protocols/{protocol_sha}.json"
        build_protocol_bytes = canonical_json_bytes(build_cell) + b"\n"
        build_protocol_sha = (
            v21_base_authority.protocol_sha256
            if v2x_diagnostic and v21_base_authority is not None
            else hashlib.sha256(build_protocol_bytes).hexdigest()
        )
        build_protocol_uri = (
            v21_base_authority.protocol_uri
            if v2x_diagnostic and v21_base_authority is not None
            else f"{campaign_root}/protocols/{build_protocol_sha}.json"
        )
        with tempfile.TemporaryDirectory(
            prefix="borsuk-publication-protocol-"
        ) as directory:
            protocol_path = Path(directory) / "protocol.json"
            protocol_path.write_bytes(protocol_bytes)
            aws.upload_immutable(protocol_path, protocol_uri, protocol_sha)
            if not v2x_diagnostic:
                build_protocol_path = Path(directory) / "build-protocol.json"
                build_protocol_path.write_bytes(build_protocol_bytes)
                aws.upload_immutable(
                    build_protocol_path, build_protocol_uri, build_protocol_sha
                )
        prepared = prepare_qualification_execution(
            normalized,
            operation=args.operation,
            workload_id=args.workload if generic_read else None,
            dataset_id=args.dataset if lifecycle or generic_read else None,
            repetition_id=(
                "r01" if build_operation or not generic_read else args.repetition
            ),
            source_uri=source_uri,
            source_sha256=source_sha,
            manifest_uri=manifest_uri,
            manifest_sha256=manifest_sha,
            protocol_uri=protocol_uri,
            protocol_sha256=protocol_sha,
            build_protocol_sha256=build_protocol_sha,
            launch=launch,
            aws=aws,
            attempt=execution_attempt,
            build_attempt=build_attempt,
            purchase_option=getattr(args, "purchase_option", "spot"),
            arm_index=getattr(args, "arm_index", 0),
            diagnostic_write_ops=getattr(args, "write_ops", 2_560),
            diagnostic_timeout_seconds=getattr(args, "timeout_seconds", 1_200),
            diagnostic_read_nprobes=getattr(args, "nprobes", None),
            diagnostic_read_candidates=getattr(args, "candidates", None),
            v21_base_authority=(v21_base_authority if v21_feasibility else None),
            v22_base_authority=(v21_base_authority if v22_stage_l else None),
        )
        receipt = run_execution_job(
            prepared.job,
            request=prepared.request,
            expected=prepared.expected,
            aws=aws,
            timeout_seconds=prepared.timeout_seconds,
            purchase_option=getattr(args, "purchase_option", "spot"),
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
