#!/usr/bin/env python3
"""Bounded Spot launcher for the diagnostic V36 prefix population freeze."""

from __future__ import annotations

import argparse
import base64
import dataclasses
import hashlib
import json
import pathlib
import re
import shlex
import sys
import time
import urllib.parse
from typing import Any

PROFILE = "causality"
REGION = "eu-central-1"
INSTANCE_TYPE = "r8gd.8xlarge"
ACTIVE_WALL_SECONDS = 43_200
EPHEMERAL_NVME_BYTES = 1_900_000_000_000
MAX_SOURCE_OBJECTS = 16
MAX_SOURCE_BYTES = 6 * 1024**3
TARGET_DISTINCT_ROWS = 1_100_000
VECTOR_DIMENSIONS = 768
CHECKPOINT_OBJECTS = 16
CHECKPOINT_SECONDS = 300
MAX_ATTEMPTS = 3
SPOT_HOURLY_CAP_MICRO_USD = 3_000_000
CAMPAIGN_CAP_MICRO_USD = 90_000_000
RAW_POPULATION_BYTES = TARGET_DISTINCT_ROWS * VECTOR_DIMENSIONS * 4
DISK_PREFLIGHT_BYTES = MAX_SOURCE_BYTES + RAW_POPULATION_BYTES * 5 // 4
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
class V36PrefixScreenPlan:
    """Complete immutable authority for one diagnostic population freeze."""

    run_id: str
    source_commit: str
    source_archive_uri: str
    source_archive_sha256: str
    source_archive_blake3: str
    source_archive_bytes: int
    binary_uri: str
    binary_sha256: str
    binary_blake3: str
    binary_bytes: int
    authority_uri: str
    authority_sha256: str
    authority_blake3: str
    authority_bytes: int
    source_registry_uri: str
    source_registry_sha256: str
    source_registry_blake3: str
    source_registry_bytes: int
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
        raise ValueError("V36 prefix-screen S3 URI differs")
    key = parsed.path[1:]
    if prefix != key.endswith("/"):
        raise ValueError("V36 prefix-screen S3 prefix differs")
    return parsed.netloc, key


def canonical_json_bytes(value: object) -> bytes:
    """Serialize compact sorted JSON with exactly one trailing newline."""

    return (
        json.dumps(value, allow_nan=False, separators=(",", ":"), sort_keys=True).encode()
        + b"\n"
    )


def build_v36_prefix_screen_plan(**values: Any) -> V36PrefixScreenPlan:
    """Validate one immutable prefix-screen launch plan."""

    plan = V36PrefixScreenPlan(**values)
    digest_values = (
        plan.source_archive_sha256,
        plan.source_archive_blake3,
        plan.binary_sha256,
        plan.binary_blake3,
        plan.authority_sha256,
        plan.authority_blake3,
        plan.source_registry_sha256,
        plan.source_registry_blake3,
    )
    byte_values = (
        plan.source_archive_bytes,
        plan.binary_bytes,
        plan.authority_bytes,
        plan.source_registry_bytes,
    )
    if (
        _RUN_ID.fullmatch(plan.run_id) is None
        or _GIT.fullmatch(plan.source_commit) is None
        or any(type(value) is not str or _SHA256.fullmatch(value) is None for value in digest_values)
        or any(type(value) is not int or value <= 0 for value in byte_values)
    ):
        raise ValueError("V36 prefix-screen plan differs")
    for uri in (
        plan.source_archive_uri,
        plan.binary_uri,
        plan.authority_uri,
        plan.source_registry_uri,
    ):
        _s3(uri)
    _s3(plan.output_prefix, prefix=True)
    return plan


def dry_run_v36_prefix_screen(plan: V36PrefixScreenPlan) -> bytes:
    """Return the complete launch envelope without touching local or AWS state."""

    build_v36_prefix_screen_plan(**dataclasses.asdict(plan))
    return canonical_json_bytes(
        {
            "active_wall_seconds": ACTIVE_WALL_SECONDS,
            "campaign_cap_micro_usd": CAMPAIGN_CAP_MICRO_USD,
            "checkpoint_objects": CHECKPOINT_OBJECTS,
            "checkpoint_seconds": CHECKPOINT_SECONDS,
            "claim_eligible": False,
            "disk_preflight_bytes": DISK_PREFLIGHT_BYTES,
            "dry_run": True,
            "instance_type": INSTANCE_TYPE,
            "max_attempts": MAX_ATTEMPTS,
            "max_source_bytes": MAX_SOURCE_BYTES,
            "max_source_objects": MAX_SOURCE_OBJECTS,
            "profile": PROFILE,
            "region": REGION,
            "run_id": plan.run_id,
            "schema": "borsuk-v36-prefix-screen-dry-run-v1",
            "spot_hourly_cap_micro_usd": SPOT_HOURLY_CAP_MICRO_USD,
            "target_distinct_rows": TARGET_DISTINCT_ROWS,
            "vector_dimensions": VECTOR_DIMENSIONS,
            "zone_candidates": [zone for zone, _ in SPOT_TARGETS],
        }
    )


def _attempt_wall_seconds(attempt_ordinal: int) -> int:
    if type(attempt_ordinal) is not int or not 0 <= attempt_ordinal < MAX_ATTEMPTS:
        raise ValueError("V36 prefix-screen attempt ordinal differs")
    spent_before = attempt_ordinal * (
        ACTIVE_WALL_SECONDS * SPOT_HOURLY_CAP_MICRO_USD // 3_600
    )
    remaining = CAMPAIGN_CAP_MICRO_USD - spent_before
    return min(
        ACTIVE_WALL_SECONDS,
        remaining * 3_600 // SPOT_HOURLY_CAP_MICRO_USD,
    )


def _user_data(plan: V36PrefixScreenPlan, *, attempt_ordinal: int) -> str:
    quoted = {
        field.name: shlex.quote(str(getattr(plan, field.name)))
        for field in dataclasses.fields(plan)
    }
    output_bucket, output_key = _s3(plan.output_prefix, prefix=True)
    attempt_prefix = f"{output_key}attempt-{attempt_ordinal:04d}/"
    wall_seconds = _attempt_wall_seconds(attempt_ordinal)
    execution_authority = canonical_json_bytes(
        {
            "active_wall_seconds": wall_seconds,
            "attempt_id": f"{plan.run_id}-attempt-{attempt_ordinal:04d}",
            "checkpoint_seconds": CHECKPOINT_SECONDS,
            "claim_eligible": False,
            "inputs": [
                {"blake3": plan.binary_blake3, "encoded_bytes": plan.binary_bytes, "role": "binary", "sha256": plan.binary_sha256, "uri": plan.binary_uri},
                {"blake3": plan.authority_blake3, "encoded_bytes": plan.authority_bytes, "role": "freeze-authority", "sha256": plan.authority_sha256, "uri": plan.authority_uri},
                {"blake3": plan.source_archive_blake3, "encoded_bytes": plan.source_archive_bytes, "role": "source-archive", "sha256": plan.source_archive_sha256, "uri": plan.source_archive_uri},
                {"blake3": plan.source_registry_blake3, "encoded_bytes": plan.source_registry_bytes, "role": "source-registry", "sha256": plan.source_registry_sha256, "uri": plan.source_registry_uri},
            ],
            "output_prefix": f"s3://{output_bucket}/{attempt_prefix}",
            "schema": "borsuk-v36-prefix-freeze-execution-authority-v1",
            "source_commit": plan.source_commit,
        }
    )
    execution_authority_b64 = base64.b64encode(execution_authority).decode()
    return f"""#!/bin/bash
set -euo pipefail
trap 'shutdown -h now' EXIT
root=$(mktemp -d /mnt/v36-prefix.XXXXXX)
available=$(df --output=avail -B1 /mnt | tail -1)
test "$available" -ge {DISK_PREFLIGHT_BYTES}
aws s3 cp {quoted['source_archive_uri']} "$root/source.tar.zst" --only-show-errors
aws s3 cp {quoted['binary_uri']} "$root/v36_prefix_freeze" --only-show-errors
aws s3 cp {quoted['authority_uri']} "$root/authority.json" --only-show-errors
aws s3 cp {quoted['source_registry_uri']} "$root/source-registry.json" --only-show-errors
printf '%s' {shlex.quote(execution_authority_b64)} | base64 -d > "$root/execution-authority.json"
test "$(stat -c %s "$root/source.tar.zst")" = {plan.source_archive_bytes}
test "$(sha256sum "$root/source.tar.zst" | cut -d' ' -f1)" = {plan.source_archive_sha256}
test "$(stat -c %s "$root/v36_prefix_freeze")" = {plan.binary_bytes}
test "$(sha256sum "$root/v36_prefix_freeze" | cut -d' ' -f1)" = {plan.binary_sha256}
test "$(stat -c %s "$root/authority.json")" = {plan.authority_bytes}
test "$(sha256sum "$root/authority.json" | cut -d' ' -f1)" = {plan.authority_sha256}
test "$(stat -c %s "$root/source-registry.json")" = {plan.source_registry_bytes}
test "$(sha256sum "$root/source-registry.json" | cut -d' ' -f1)" = {plan.source_registry_sha256}
chmod 500 "$root/v36_prefix_freeze"
mkdir "$root/output" "$root/scratch"
set +e
timeout --signal=TERM --kill-after=30 {wall_seconds} "$root/v36_prefix_freeze" \
  --execute-prefix-freeze \
  --execution-authority "$root/execution-authority.json" \
  --authority "$root/authority.json" --source-registry "$root/source-registry.json" \
  --source-archive "$root/source.tar.zst" --output "$root/output" \
  --scratch "$root/scratch"
status=$?
set -e
if [[ -f "$root/output/CHECKPOINT.json" ]]; then
  aws s3api put-object --bucket {shlex.quote(output_bucket)} --key {shlex.quote(output_key)}CHECKPOINT.json --body "$root/output/CHECKPOINT.json"
fi
for marker in INTERRUPTED.json ATTEMPT_COMPLETE.json ATTEMPT_FAILED.json; do
  if [[ -f "$root/output/$marker" ]]; then
    aws s3api put-object --bucket {shlex.quote(output_bucket)} --key {shlex.quote(attempt_prefix)}"$marker" --body "$root/output/$marker" --if-none-match '*'
  fi
done
exit "$status"
"""


def build_v36_prefix_launch_specs(
    plan: V36PrefixScreenPlan, *, launch_nonce: str, attempt_ordinal: int
) -> list[dict[str, object]]:
    """Build the three registered Spot-zone candidates for one attempt."""

    if re.fullmatch(r"[0-9a-f]{32}", launch_nonce) is None:
        raise ValueError("V36 prefix-screen launch nonce differs")
    data = base64.b64encode(_user_data(plan, attempt_ordinal=attempt_ordinal).encode()).decode()
    specs: list[dict[str, object]] = []
    for zone_ordinal, (zone, subnet) in enumerate(SPOT_TARGETS):
        token = hashlib.sha256(
            f"{plan.run_id}:{launch_nonce}:{attempt_ordinal}:{zone_ordinal}".encode()
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
                        "MaxPrice": "3.000000",
                        "SpotInstanceType": "one-time",
                    },
                },
                "InstanceInitiatedShutdownBehavior": "terminate",
                "UserData": data,
            }
        )
    return specs


def _marker_key(plan: V36PrefixScreenPlan, attempt_ordinal: int, marker: str) -> tuple[str, str]:
    bucket, prefix = _s3(plan.output_prefix, prefix=True)
    if marker not in {"ATTEMPT_COMPLETE.json", "ATTEMPT_FAILED.json", "INTERRUPTED.json"}:
        raise ValueError("V36 prefix-screen marker differs")
    return bucket, f"{prefix}attempt-{attempt_ordinal:04d}/{marker}"


def _read_attempt_status(
    s3_client: Any, plan: V36PrefixScreenPlan, attempt_ordinal: int | None = None
) -> str | None:
    """Read one authenticated terminal status, or prove it absent."""

    ordinals = range(MAX_ATTEMPTS) if attempt_ordinal is None else (attempt_ordinal,)
    for ordinal in ordinals:
        for marker, status in (
            ("ATTEMPT_COMPLETE.json", "complete"),
            ("ATTEMPT_FAILED.json", "failed"),
            ("INTERRUPTED.json", "interrupted"),
        ):
            bucket, key = _marker_key(plan, ordinal, marker)
            try:
                response = s3_client.get_object(Bucket=bucket, Key=key)
            except Exception as error:
                code = getattr(error, "response", {}).get("Error", {}).get("Code")
                if code in {"NoSuchKey", "404"}:
                    continue
                raise
            body = response["Body"].read()
            value = json.loads(body)
            if (
                response.get("ContentLength") != len(body)
                or canonical_json_bytes(value) != body
                or value.get("status") != status
                or value.get("run_id") != plan.run_id
                or value.get("source_commit") != plan.source_commit
            ):
                raise ValueError("V36 prefix-screen terminal authority differs")
            return status
    return None


def run_v36_prefix_screen(
    plan: V36PrefixScreenPlan,
    *,
    ec2_client: Any,
    s3_client: Any,
    launch_nonce: str,
) -> str:
    """Run at most three bounded Spot attempts and preserve every terminal."""

    if _read_attempt_status(s3_client, plan) is not None:
        raise ValueError("V36 prefix-screen terminal already exists")
    for attempt_ordinal in range(MAX_ATTEMPTS):
        instance_id: str | None = None
        try:
            spec = build_v36_prefix_launch_specs(
                plan,
                launch_nonce=launch_nonce,
                attempt_ordinal=attempt_ordinal,
            )[attempt_ordinal]
            try:
                response = ec2_client.run_instances(**spec)
            except Exception as error:
                code = getattr(error, "response", {}).get("Error", {}).get("Code")
                if code in _CAPACITY_ERRORS:
                    continue
                raise
            instance_id = response["Instances"][0]["InstanceId"]
            while True:
                state = ec2_client.describe_instances(InstanceIds=[instance_id])[
                    "Reservations"
                ][0]["Instances"][0]["State"]["Name"]
                if state in {"shutting-down", "terminated", "stopped", "stopping"}:
                    break
                time.sleep(15)
        finally:
            if instance_id is not None:
                ec2_client.terminate_instances(InstanceIds=[instance_id])
        status = _read_attempt_status(s3_client, plan, attempt_ordinal)
        if status == "complete":
            bucket, key = _marker_key(plan, attempt_ordinal, "ATTEMPT_COMPLETE.json")
            return f"s3://{bucket}/{key}"
        if status == "failed":
            raise RuntimeError(f"V36 prefix-screen attempt {attempt_ordinal} failed")
        if status is None:
            raise RuntimeError(f"V36 prefix-screen attempt {attempt_ordinal} terminal missing")
    raise RuntimeError("V36 prefix-screen three attempts exhausted")


def main(argv: list[str] | None = None) -> int:
    """Print a pure dry-run receipt; execution uses the typed Python API."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true", required=True)
    parser.add_argument("--plan-json", required=True)
    arguments = parser.parse_args(argv)
    try:
        raw = json.loads(arguments.plan_json)
        if type(raw) is not dict:
            raise ValueError("V36 prefix-screen plan JSON differs")
        plan = build_v36_prefix_screen_plan(**raw)
    except (TypeError, ValueError, json.JSONDecodeError) as error:
        parser.error(str(error))
    sys.stdout.write(dry_run_v36_prefix_screen(plan).decode())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
