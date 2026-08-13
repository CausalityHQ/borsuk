#!/usr/bin/env python3
"""Pure AWS execution contracts for Publication V3.

This module deliberately does not call AWS.  It turns an already validated
publication manifest into deterministic staging jobs and EC2 launch requests,
and classifies attempts from terminal marker names plus EC2 state.  The paid
controller can therefore be tested without credentials and cannot infer
success from measurement files.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path

if __package__:
    from scripts.publication_v3_protocol import canonical_json_bytes, validate_manifest
else:  # Direct ``python scripts/...`` execution.
    from publication_v3_protocol import canonical_json_bytes, validate_manifest


COMPLETE_MARKERS = frozenset({"CELL_COMPLETE", "STAGING_COMPLETE.json"})
FAILURE_MARKERS = frozenset({"CELL_FAILED", "STAGING_FAILED.json"})
KNOWN_MARKERS = COMPLETE_MARKERS | FAILURE_MARKERS
HEX_64 = re.compile(r"[0-9a-f]{64}")
MAX_PARQUET_BYTES = 128 * 1024 * 1024
MAX_JSON_BYTES = 256 * 1024
MAX_DATASET_OBJECTS = 8_192
MAX_DATASET_BYTES = 1024 * 1024 * 1024 * 1024


@dataclass(frozen=True)
class StagingJob:
    dataset_id: str
    adapter: str
    attempt: int
    output_uri: str
    terminal_uri: str
    failure_uri: str


@dataclass(frozen=True)
class AttemptObservation:
    instance_state: str
    terminal_markers: tuple[str, ...]


@dataclass(frozen=True)
class AttemptDecision:
    action: str
    discard_measurements: bool


def _adapter(dataset: dict[str, object]) -> str:
    kind = str(dataset["kind"])
    if kind == "standard-ann":
        return "ann-benchmarks"
    if kind == "realistic-dense":
        return "vdbbench"
    if kind == "beir-hybrid":
        return "beir"
    raise ValueError(f"external dataset kind has no staging adapter: {kind}")


def staging_jobs(
    manifest: dict[str, object], *, attempt: int = 1
) -> tuple[StagingJob, ...]:
    """Return one deterministic first attempt for every external dataset."""

    normalized = validate_manifest(manifest)
    if attempt <= 0 or attempt > 9_999:
        raise ValueError("staging attempt must be in 1..=9999")
    dataset_prefix = str(normalized["prefixes"]["dataset"]).rstrip("/")
    jobs: list[StagingJob] = []
    for dataset in sorted(normalized["datasets"], key=lambda item: item["id"]):
        if dataset["source"]["state"] != "unstaged":
            continue
        dataset_id = str(dataset["id"])
        attempt_root = f"{dataset_prefix}/{dataset_id}/attempts/{attempt:04d}"
        jobs.append(
            StagingJob(
                dataset_id=dataset_id,
                adapter=_adapter(dataset),
                attempt=attempt,
                output_uri=f"{attempt_root}/materialized",
                terminal_uri=f"{attempt_root}/STAGING_COMPLETE.json",
                failure_uri=f"{attempt_root}/STAGING_FAILED.json",
            )
        )
    return tuple(jobs)


def _resource_contract(
    manifest: dict[str, object], role: str, system: str
) -> tuple[dict[str, object], dict[str, object]]:
    environment = manifest["environment_contract"]
    systems = environment["runtime_clients"]
    if system not in systems:
        raise ValueError(f"unknown publication system: {system}")
    if role == "runtime":
        resources = systems[system]
        storage = environment["runtime_storage"]
    elif role == "build":
        resources = environment["build_workers"][system]
        storage = environment["build_storage"]
    elif role == "staging":
        if system != "borsuk":
            raise ValueError("dataset staging uses the single borsuk build-worker profile")
        resources = environment["build_workers"]["borsuk"]
        storage = environment["build_storage"]
    else:
        raise ValueError("instance role must be runtime, build, or staging")
    return resources, storage


def _tags(
    campaign_id: str, cell_id: str, attempt: int, role: str
) -> list[dict[str, str]]:
    if attempt <= 0:
        raise ValueError("attempt must be positive")
    if not campaign_id or not cell_id:
        raise ValueError("campaign and cell identifiers must be nonempty")
    values = {
        "Name": f"borsuk-{campaign_id}-{cell_id}-a{attempt:04d}",
        "Project": "BorsukBenchmark",
        "Campaign": campaign_id,
        "Cell": cell_id,
        "Attempt": str(attempt),
        "Role": role,
        "AutoTerminate": "true",
    }
    return [{"Key": key, "Value": values[key]} for key in sorted(values)]


def build_spot_launch_request(
    manifest: dict[str, object],
    *,
    role: str,
    system: str,
    image_id: str,
    subnet_id: str,
    security_group_id: str,
    instance_profile_arn: str,
    image_architecture: str,
    subnet_region: str,
    campaign_id: str,
    cell_id: str,
    attempt: int,
    worker_script: str,
    max_seconds: int,
) -> dict[str, object]:
    """Build a hardened one-instance Spot request from the frozen contract."""

    normalized = validate_manifest(manifest)
    if campaign_id != normalized["campaign_id"]:
        raise ValueError("launch campaign differs from the manifest campaign")
    environment = normalized["environment_contract"]
    if environment["spot_default"] is not True:
        raise ValueError("Publication V3 requires Spot by default")
    resources, storage = _resource_contract(normalized, role, system)
    if image_architecture != environment["architecture"]:
        raise ValueError("AMI architecture differs from the manifest architecture")
    if subnet_region != environment["region"]:
        raise ValueError("subnet region differs from the manifest region")
    expected_profile_prefix = (
        f"arn:aws:iam::{environment['aws_account']}:instance-profile/"
    )
    if not instance_profile_arn.startswith(expected_profile_prefix):
        raise ValueError("instance profile ARN differs from the manifest AWS account")
    budget_field = "max_cell_seconds" if role == "runtime" else "max_index_build_seconds"
    maximum_seconds = int(normalized["budget_contract"][budget_field])
    if max_seconds <= 0 or max_seconds > maximum_seconds:
        raise ValueError("worker timeout exceeds the manifest budget")
    if not worker_script or "\x00" in worker_script:
        raise ValueError("worker script must be nonempty UTF-8 text")
    worker_payload = base64.b64encode(worker_script.encode("utf-8")).decode("ascii")
    user_data = f"""#!/usr/bin/env bash
set -euo pipefail
finish() {{
  status=$?
  trap - EXIT
  shutdown -h now || true
  exit "$status"
}}
trap finish EXIT
printf '%s' '{worker_payload}' | base64 -d >/var/lib/borsuk-publication-worker.sh
chmod 700 /var/lib/borsuk-publication-worker.sh
timeout --signal=TERM --kill-after=60 {max_seconds} /bin/bash /var/lib/borsuk-publication-worker.sh
"""
    encoded_user_data = base64.b64encode(user_data.encode("utf-8")).decode("ascii")
    if len(user_data.encode("utf-8")) > 16 * 1024:
        raise ValueError("EC2 user data exceeds its 16 KiB raw limit")
    for label, value in (
        ("image id", image_id),
        ("subnet id", subnet_id),
        ("security group id", security_group_id),
        ("instance profile ARN", instance_profile_arn),
    ):
        if not value:
            raise ValueError(f"{label} must be nonempty")
    return {
        "ImageId": image_id,
        "InstanceType": resources["instance_type"],
        "MinCount": 1,
        "MaxCount": 1,
        "ClientToken": "borsuk-"
        + hashlib.sha256(
            f"{campaign_id}\0{cell_id}\0{attempt}".encode("utf-8")
        ).hexdigest()[:40],
        "UserData": encoded_user_data,
        "SecurityGroupIds": [security_group_id],
        "SubnetId": subnet_id,
        "IamInstanceProfile": {"Arn": instance_profile_arn},
        "InstanceMarketOptions": {
            "MarketType": "spot",
            "SpotOptions": {
                "SpotInstanceType": "one-time",
                "InstanceInterruptionBehavior": "terminate",
            },
        },
        "InstanceInitiatedShutdownBehavior": "terminate",
        "MetadataOptions": {
            "HttpEndpoint": "enabled",
            "HttpTokens": "required",
            "HttpPutResponseHopLimit": 1,
        },
        "BlockDeviceMappings": [
            {
                "DeviceName": "/dev/xvda",
                "Ebs": {
                    "DeleteOnTermination": True,
                    "Encrypted": True,
                    "VolumeSize": storage["volume_size_gib"],
                    "VolumeType": storage["volume_type"],
                    "Iops": storage["iops"],
                    "Throughput": storage["throughput_mib_s"],
                },
            }
        ],
        "TagSpecifications": [
            {
                "ResourceType": resource_type,
                "Tags": _tags(campaign_id, cell_id, attempt, role),
            }
            for resource_type in ("instance", "volume")
        ],
    }


def classify_attempt(observation: AttemptObservation) -> AttemptDecision:
    """Choose an action without opening any incomplete measurement artifact."""

    markers = set(observation.terminal_markers)
    unknown = markers - KNOWN_MARKERS
    if unknown:
        raise ValueError(f"unrecognized terminal marker: {sorted(unknown)[0]}")
    complete = bool(markers & COMPLETE_MARKERS)
    failed = bool(markers & FAILURE_MARKERS)
    if complete and failed:
        raise ValueError("conflicting terminal markers")
    if complete:
        return AttemptDecision("terminate-success", False)
    if failed:
        return AttemptDecision("terminate-failure", True)
    if observation.instance_state in {"terminated", "stopped"}:
        return AttemptDecision("retry-fresh-attempt", True)
    if observation.instance_state in {
        "pending",
        "running",
        "stopping",
        "shutting-down",
    }:
        return AttemptDecision("monitor", False)
    raise ValueError(f"unrecognized EC2 instance state: {observation.instance_state}")


def _required_roles(adapter: str) -> dict[str, tuple[int, int | None]]:
    if adapter in {"ann-benchmarks", "vdbbench"}:
        return {
            "train": (1, None),
            "query": (1, 1),
            "ground-truth": (1, 1),
            "metadata": (1, 1),
        }
    if adapter == "beir":
        return {
            "corpus": (1, None),
            "query": (1, None),
            "qrels": (1, 1),
            "metadata": (1, 1),
        }
    raise ValueError(f"unsupported staging adapter {adapter}")


def build_staging_receipt(
    manifest: dict[str, object],
    job: StagingJob,
    *,
    source_archive_sha256: str,
    objects: tuple[dict[str, object], ...] | list[dict[str, object]],
    instance_id: str,
    instance_type: str,
    availability_zone: str,
    purchase_option: str,
) -> dict[str, object]:
    """Validate the immutable materialization roster and form its terminal receipt."""

    normalized_manifest = validate_manifest(manifest)
    expected_jobs = {
        item.dataset_id: item
        for item in staging_jobs(normalized_manifest, attempt=job.attempt)
    }
    if expected_jobs.get(job.dataset_id) != job:
        raise ValueError("staging job differs from the manifest-derived job")
    if purchase_option != "spot":
        raise ValueError("dataset staging must use Spot capacity")
    if HEX_64.fullmatch(source_archive_sha256) is None:
        raise ValueError("source archive checksum must be lowercase SHA-256")
    if len(objects) < 4:
        raise ValueError("dataset staging requires a multi-object materialization")
    if len(objects) > MAX_DATASET_OBJECTS:
        raise ValueError("dataset staging object roster exceeds its bound")
    normalized: list[dict[str, object]] = []
    uris: set[str] = set()
    allowed_roles = _required_roles(job.adapter)
    for item in objects:
        if frozenset(item) != frozenset(
            {"role", "format", "uri", "sha256", "bytes", "rows"}
        ):
            raise ValueError("staged object fields differ")
        role = str(item["role"])
        object_format = str(item["format"])
        uri = str(item["uri"])
        digest = str(item["sha256"])
        size = item["bytes"]
        rows = item["rows"]
        size_cap = MAX_JSON_BYTES if object_format == "json" else MAX_PARQUET_BYTES
        if role not in allowed_roles:
            raise ValueError(f"staged object role is invalid for {job.adapter}: {role}")
        if object_format not in {"json", "parquet"}:
            raise ValueError("staged objects must use stock JSON or Parquet")
        if role == "metadata" and object_format != "json":
            raise ValueError("dataset metadata must use JSON")
        if role != "metadata" and object_format != "parquet":
            raise ValueError("dataset data objects must use Parquet")
        output_prefix = job.output_uri.rstrip("/") + "/"
        if not uri.startswith(output_prefix) or uri in uris:
            raise ValueError("staged object URI is outside the attempt or duplicated")
        relative_path = uri.removeprefix(output_prefix)
        if (
            not relative_path
            or relative_path.startswith("/")
            or ".." in relative_path.split("/")
        ):
            raise ValueError("staged object path is not canonical")
        if HEX_64.fullmatch(digest) is None:
            raise ValueError("staged object checksum must be lowercase SHA-256")
        if (
            not isinstance(size, int)
            or isinstance(size, bool)
            or size <= 0
            or size > size_cap
            or not isinstance(rows, int)
            or isinstance(rows, bool)
            or rows <= 0
        ):
            raise ValueError("staged object bytes or rows exceed the format contract")
        uris.add(uri)
        normalized.append(
            {
                "role": role,
                "format": object_format,
                "uri": uri,
                "sha256": digest,
                "bytes": size,
                "rows": rows,
            }
        )
    counts = {role: 0 for role in allowed_roles}
    for item in normalized:
        counts[str(item["role"])] += 1
    for role, (minimum, maximum) in allowed_roles.items():
        count = counts[role]
        if count < minimum or (maximum is not None and count > maximum):
            raise ValueError(f"dataset materialization has invalid {role} object count")
    expected_environment = normalized_manifest["environment_contract"]
    expected_instance_type = expected_environment["build_workers"]["borsuk"][
        "instance_type"
    ]
    if instance_type != expected_instance_type:
        raise ValueError("staging instance type differs from the build-worker contract")
    if not availability_zone.startswith(str(expected_environment["region"])):
        raise ValueError("staging availability zone differs from the manifest region")
    for label, value in (
        ("instance id", instance_id),
        ("availability zone", availability_zone),
    ):
        if not value:
            raise ValueError(f"{label} must be nonempty")
    normalized.sort(key=lambda item: str(item["uri"]))
    object_bytes = sum(int(item["bytes"]) for item in normalized)
    if object_bytes > MAX_DATASET_BYTES:
        raise ValueError("dataset staging bytes exceed the one-TiB campaign bound")
    content_identity = [
        {
            "role": item["role"],
            "format": item["format"],
            "path": str(item["uri"]).removeprefix(job.output_uri.rstrip("/") + "/"),
            "sha256": item["sha256"],
            "bytes": item["bytes"],
            "rows": item["rows"],
        }
        for item in normalized
    ]
    dataset_content_sha256 = hashlib.sha256(
        canonical_json_bytes(content_identity)
    ).hexdigest()
    return {
        "schema_version": 1,
        "campaign_id": normalized_manifest["campaign_id"],
        "manifest_sha256": hashlib.sha256(
            canonical_json_bytes(normalized_manifest)
        ).hexdigest(),
        "dataset_id": job.dataset_id,
        "adapter": job.adapter,
        "attempt": job.attempt,
        "source_archive_sha256": source_archive_sha256,
        "dataset_content_sha256": dataset_content_sha256,
        "output_uri": job.output_uri,
        "terminal_uri": job.terminal_uri,
        "failure_uri": job.failure_uri,
        "instance_id": instance_id,
        "instance_type": instance_type,
        "availability_zone": availability_zone,
        "purchase_option": purchase_option,
        "object_count": len(normalized),
        "object_bytes": object_bytes,
        "objects": normalized,
    }


def _staging_plan(manifest: dict[str, object]) -> dict[str, object]:
    normalized = validate_manifest(manifest)
    jobs = staging_jobs(normalized)
    return {
        "schema_version": 1,
        "campaign_id": normalized["campaign_id"],
        "manifest_sha256": hashlib.sha256(
            canonical_json_bytes(normalized)
        ).hexdigest(),
        "job_count": len(jobs),
        "jobs": [
            {
                "dataset_id": job.dataset_id,
                "adapter": job.adapter,
                "attempt": job.attempt,
                "output_uri": job.output_uri,
                "terminal_uri": job.terminal_uri,
                "failure_uri": job.failure_uri,
            }
            for job in jobs
        ],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="operation", required=True)
    plan = subparsers.add_parser("plan-staging")
    plan.add_argument("manifest", type=Path)
    args = parser.parse_args()
    if args.operation == "plan-staging":
        manifest = json.loads(args.manifest.read_text(encoding="utf-8"))
        print(canonical_json_bytes(_staging_plan(manifest)).decode("utf-8"))
        return 0
    raise AssertionError("unreachable operation")


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, ValueError, json.JSONDecodeError) as error:
        print(f"publication-v3 AWS plan failed: {error}", file=sys.stderr)
        raise SystemExit(2) from None
