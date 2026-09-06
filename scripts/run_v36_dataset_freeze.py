#!/usr/bin/env python3
"""Launch one bounded V36 dataset-freeze worker on ephemeral EC2 Spot."""

from __future__ import annotations

import base64
import dataclasses
import hashlib
import json
import pathlib
import re
import shlex
import time
import urllib.parse
from typing import Any

PROFILE = "causality"
REGION = "eu-central-1"
INSTANCE_TYPE = "r8gd.8xlarge"
ACTIVE_WALL_SECONDS = 43_200
EPHEMERAL_NVME_BYTES = 1_900_000_000_000
AMI_ID = "ami-07bcecd13a160173f"
SECURITY_GROUP_ID = "sg-0b1fd3e4fbde4af0d"
INSTANCE_PROFILE = "borsuk-bench-profile"
SPOT_TARGETS = (
    ("eu-central-1c", "subnet-0a12dbed0ca6fac25"),
    ("eu-central-1b", "subnet-00243d923761c047c"),
    ("eu-central-1a", "subnet-034528fbd6977848f"),
)
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_GIT = re.compile(r"[0-9a-f]{40}\Z")
_RUN_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}\Z")
_CAPACITY_ERRORS = {
    "InsufficientInstanceCapacity",
    "InsufficientFreeAddressesInSubnet",
    "SpotMaxPriceTooLow",
    "Unsupported",
}


@dataclasses.dataclass(frozen=True)
class V36DatasetFreezePlan:
    """Complete input and output authority for one freeze worker."""

    run_id: str
    source_commit: str
    source_archive_uri: str
    source_archive_sha256: str
    source_archive_bytes: int
    binary_uri: str
    binary_sha256: str
    binary_bytes: int
    authority_uri: str
    authority_sha256: str
    authority_bytes: int
    output_prefix: str


def _s3(value: str, *, prefix: bool = False) -> tuple[str, str]:
    parsed = urllib.parse.urlsplit(value)
    if (
        parsed.scheme != "s3"
        or not parsed.netloc
        or not parsed.path.startswith("/")
        or parsed.path == "/"
        or parsed.query
        or parsed.fragment
        or ".." in pathlib.PurePosixPath(parsed.path).parts
    ):
        raise ValueError("V36 dataset-freeze S3 URI differs")
    key = parsed.path[1:]
    if prefix != key.endswith("/"):
        raise ValueError("V36 dataset-freeze S3 prefix differs")
    return parsed.netloc, key


def build_v36_dataset_freeze_plan(**values: Any) -> V36DatasetFreezePlan:
    """Validate one immutable V36 dataset-freeze plan."""

    plan = V36DatasetFreezePlan(**values)
    if (
        _RUN_ID.fullmatch(plan.run_id) is None
        or _GIT.fullmatch(plan.source_commit) is None
        or any(
            _SHA256.fullmatch(value) is None
            for value in (
                plan.source_archive_sha256,
                plan.binary_sha256,
                plan.authority_sha256,
            )
        )
        or min(plan.source_archive_bytes, plan.binary_bytes, plan.authority_bytes) <= 0
    ):
        raise ValueError("V36 dataset-freeze plan differs")
    _s3(plan.source_archive_uri)
    _s3(plan.binary_uri)
    _s3(plan.authority_uri)
    _s3(plan.output_prefix, prefix=True)
    return plan


def canonical_json_bytes(value: object) -> bytes:
    """Serialize strict compact sorted JSON with one trailing newline."""

    return (
        json.dumps(value, allow_nan=False, separators=(",", ":"), sort_keys=True).encode()
        + b"\n"
    )


def canonical_v36_dataset_terminal_bytes(
    plan: V36DatasetFreezePlan,
    *,
    instance_id: str,
    status: str,
    result_uri: str,
    result_sha256: str,
    result_bytes: int,
) -> bytes:
    """Build one exact terminal receipt bound to every input."""

    if (
        not instance_id.startswith("i-")
        or status not in {"complete", "failed", "interrupted"}
        or _SHA256.fullmatch(result_sha256) is None
        or result_bytes <= 0
    ):
        raise ValueError("V36 dataset-freeze terminal differs")
    _s3(result_uri)
    return canonical_json_bytes(
        {
            "authority_bytes": plan.authority_bytes,
            "authority_sha256": plan.authority_sha256,
            "authority_uri": plan.authority_uri,
            "binary_bytes": plan.binary_bytes,
            "binary_sha256": plan.binary_sha256,
            "binary_uri": plan.binary_uri,
            "claim_eligible": False,
            "instance_id": instance_id,
            "result_bytes": result_bytes,
            "result_sha256": result_sha256,
            "result_uri": result_uri,
            "run_id": plan.run_id,
            "schema": "borsuk-v36-dataset-freeze-terminal-v1",
            "source_archive_bytes": plan.source_archive_bytes,
            "source_archive_sha256": plan.source_archive_sha256,
            "source_archive_uri": plan.source_archive_uri,
            "source_commit": plan.source_commit,
            "status": status,
        }
    )


def _user_data(plan: V36DatasetFreezePlan) -> str:
    quoted = {field.name: shlex.quote(str(getattr(plan, field.name))) for field in dataclasses.fields(plan)}
    output_bucket, output_key = _s3(plan.output_prefix, prefix=True)
    return f"""#!/bin/bash
set -euo pipefail
trap 'shutdown -h now' EXIT
root=$(mktemp -d /mnt/v36-freeze.XXXXXX)
test "$(df --output=size -B1 /mnt | tail -1)" -ge {EPHEMERAL_NVME_BYTES}
aws s3 cp {quoted['source_archive_uri']} "$root/source.tar.zst" --only-show-errors
aws s3 cp {quoted['binary_uri']} "$root/v36_dataset_freeze" --only-show-errors
aws s3 cp {quoted['authority_uri']} "$root/authority.json" --only-show-errors
test "$(stat -c %s "$root/source.tar.zst")" = {plan.source_archive_bytes}
test "$(sha256sum "$root/source.tar.zst" | cut -d' ' -f1)" = {plan.source_archive_sha256}
test "$(stat -c %s "$root/v36_dataset_freeze")" = {plan.binary_bytes}
test "$(sha256sum "$root/v36_dataset_freeze" | cut -d' ' -f1)" = {plan.binary_sha256}
test "$(stat -c %s "$root/authority.json")" = {plan.authority_bytes}
test "$(sha256sum "$root/authority.json" | cut -d' ' -f1)" = {plan.authority_sha256}
chmod 500 "$root/v36_dataset_freeze"
timeout --signal=TERM --kill-after=30 {ACTIVE_WALL_SECONDS} "$root/v36_dataset_freeze" --execute-freeze --one-source-stream --authority "$root/authority.json" --output "$root/output"
aws s3 cp "$root/output/result.json" {quoted['output_prefix']}result.json --only-show-errors
for marker in INTERRUPTED.json ATTEMPT_COMPLETE.json ATTEMPT_FAILED.json; do
  if [[ -f "$root/output/$marker" ]]; then
    aws s3api put-object --bucket {shlex.quote(output_bucket)} --key {shlex.quote(output_key)}"$marker" --body "$root/output/$marker" --if-none-match '*'
  fi
done
"""


def build_v36_launch_specs(
    plan: V36DatasetFreezePlan, *, launch_nonce: str
) -> list[dict[str, object]]:
    """Build one Spot attempt per registered availability zone."""

    if re.fullmatch(r"[0-9a-f]{32}", launch_nonce) is None:
        raise ValueError("V36 launch nonce differs")
    data = base64.b64encode(_user_data(plan).encode()).decode()
    specs = []
    for ordinal, (zone, subnet) in enumerate(SPOT_TARGETS):
        token = hashlib.sha256(
            f"{plan.run_id}:{launch_nonce}:{ordinal}".encode()
        ).hexdigest()
        specs.append(
            {
                "ImageId": AMI_ID,
                "InstanceType": INSTANCE_TYPE,
                "MinCount": 1,
                "MaxCount": 1,
                "ClientToken": token,
                "Placement": {"AvailabilityZone": zone},
                "SubnetId": subnet,
                "SecurityGroupIds": [SECURITY_GROUP_ID],
                "IamInstanceProfile": {"Name": INSTANCE_PROFILE},
                "InstanceMarketOptions": {
                    "MarketType": "spot",
                    "SpotOptions": {
                        "InstanceInterruptionBehavior": "terminate",
                        "SpotInstanceType": "one-time",
                    },
                },
                "InstanceInitiatedShutdownBehavior": "terminate",
                "UserData": data,
            }
        )
    return specs


def _terminal_key(plan: V36DatasetFreezePlan) -> tuple[str, str]:
    bucket, prefix = _s3(plan.output_prefix, prefix=True)
    return bucket, f"{prefix}ATTEMPT_COMPLETE.json"


def _read_complete_terminal(s3_client: Any, plan: V36DatasetFreezePlan) -> bytes | None:
    bucket, key = _terminal_key(plan)
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
    except Exception as error:
        code = getattr(error, "response", {}).get("Error", {}).get("Code")
        if code == "NoSuchKey":
            return None
        raise
    body = response["Body"].read()
    if response.get("ContentLength") != len(body):
        raise ValueError("V36 terminal length differs")
    value = json.loads(body)
    if canonical_json_bytes(value) != body or value.get("status") != "complete":
        raise ValueError("V36 terminal bytes differ")
    expected = {
        "source_commit": plan.source_commit,
        "source_archive_uri": plan.source_archive_uri,
        "source_archive_sha256": plan.source_archive_sha256,
        "source_archive_bytes": plan.source_archive_bytes,
        "binary_uri": plan.binary_uri,
        "binary_sha256": plan.binary_sha256,
        "binary_bytes": plan.binary_bytes,
        "authority_uri": plan.authority_uri,
        "authority_sha256": plan.authority_sha256,
        "authority_bytes": plan.authority_bytes,
    }
    if any(value.get(key) != expected_value for key, expected_value in expected.items()):
        raise ValueError("V36 terminal authority differs")
    return body


def run_v36_dataset_freeze(
    plan: V36DatasetFreezePlan,
    *,
    ec2_client: Any,
    s3_client: Any,
    launch_nonce: str,
) -> str:
    """Launch one worker, preserve its terminal, and always terminate it."""

    if _read_complete_terminal(s3_client, plan) is not None:
        raise ValueError("V36 dataset-freeze terminal already exists")
    instance_id: str | None = None
    try:
        last_capacity_error: Exception | None = None
        for spec in build_v36_launch_specs(plan, launch_nonce=launch_nonce):
            try:
                response = ec2_client.run_instances(**spec)
            except Exception as error:
                code = getattr(error, "response", {}).get("Error", {}).get("Code")
                if code not in _CAPACITY_ERRORS:
                    raise
                last_capacity_error = error
                continue
            instance_id = response["Instances"][0]["InstanceId"]
            break
        if instance_id is None:
            if last_capacity_error is None:
                raise RuntimeError("V36 dataset-freeze launch produced no instance")
            raise last_capacity_error
        while True:
            if _read_complete_terminal(s3_client, plan) is not None:
                bucket, key = _terminal_key(plan)
                return f"s3://{bucket}/{key}"
            ec2_client.describe_instances(InstanceIds=[instance_id])
            time.sleep(15)
    finally:
        if instance_id is not None:
            ec2_client.terminate_instances(InstanceIds=[instance_id])
