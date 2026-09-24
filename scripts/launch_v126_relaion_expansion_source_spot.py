#!/usr/bin/env python3
"""Launch one immutable V126 development diagnostic on Causality Spot."""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import shlex
import time

from scripts.launch_native_geometric_layout_spot import DEFAULT_TARGETS
from scripts.launch_v114_1m_paired_spot import _location, _missing
from scripts.launch_v116_validation_paired_spot import ARTIFACTS
from scripts.launch_v121_deep_image_paired_spot import Plan, validate_plan
from scripts.launch_v123_rerank_diagnostic_spot import reserve_attempt
from scripts.launch_v124_source_tier_precision_spot import (
    INPUTS as V124_INPUTS,
)
from scripts.launch_v124_source_tier_precision_spot import (
    V116_PREFIX,
    V116_TERMINAL_SHA256,
    validate_v116_terminal,
)

BUCKET = "borsuk-bench-453182569524-euc1"
SCHEMA = "borsuk-v126-relaion-expansion-source-spot-v1"
TAG = "borsuk-v126-relaion-expansion-source"
INPUTS = {
    "SOURCE": V124_INPUTS["RELAION_SOURCE"],
    "LAYOUT": V124_INPUTS["RELAION_LAYOUT"],
    "REQUESTS": V124_INPUTS["RELAION_REQUESTS"],
    "SEALED_REPLAY": (
        V116_PREFIX + "/artifacts/rust-replay.jsonl",
        "3bfd155ac5f9e1b7aacbc263e1732e2314c9722f235d3454e0f17d0c6bc3c960",
        13_455_525,
    ),
    "SQ8": (ARTIFACTS["SQ8"].uri, ARTIFACTS["SQ8"].sha256, ARTIFACTS["SQ8"].bytes),
    "MIRROR_MANIFEST": (
        ARTIFACTS["MIRROR_MANIFEST"].uri,
        ARTIFACTS["MIRROR_MANIFEST"].sha256,
        ARTIFACTS["MIRROR_MANIFEST"].bytes,
    ),
    "SIDECAR": (
        ARTIFACTS["SIDECAR"].uri,
        ARTIFACTS["SIDECAR"].sha256,
        ARTIFACTS["SIDECAR"].bytes,
    ),
    "TRUTH": V124_INPUTS["RELAION_TRUTH"],
}


def user_data(plan: Plan) -> str:
    validate_plan(plan)
    exports = [
        f"export V126_SOURCE_COMMIT={shlex.quote(plan.source_commit)}",
        f"export V126_ARCHIVE_SHA256={plan.archive_sha256}",
        f"export V126_OUTPUT_PREFIX={shlex.quote(plan.output_prefix.rstrip('/'))}",
        f"export V126_WALL_SECONDS={plan.wall_seconds}",
        f"export V126_V116_TERMINAL_SHA256={V116_TERMINAL_SHA256}",
    ]
    for role, (uri, digest, length) in INPUTS.items():
        exports.extend(
            (
                f"export V126_{role}_URI={shlex.quote(uri)}",
                f"export V126_{role}_SHA256={digest}",
                f"export V126_{role}_BYTES={length}",
            )
        )
    return f"""#!/bin/bash
set -euo pipefail
root=/mnt/v126-relaion-source
mkdir -p "$root" && cd "$root"
{chr(10).join(exports)}
bootstrap_failure() {{
  code=$?
  trap - EXIT
  if [ "$code" -ne 0 ]; then
    token=$(curl -fsS -X PUT -H 'X-aws-ec2-metadata-token-ttl-seconds: 60' \\
      http://169.254.169.254/latest/api/token || true)
    instance_id=$(curl -fsS -H "X-aws-ec2-metadata-token: $token" \\
      http://169.254.169.254/latest/meta-data/instance-id || true)
    INSTANCE_ID="$instance_id" EXIT_CODE="$code" python3 - <<'PY' >terminal.json
import json,os
print(json.dumps({{"schema":"{SCHEMA}","source_commit":os.environ["V126_SOURCE_COMMIT"],
  "source_archive_sha256":os.environ["V126_ARCHIVE_SHA256"],
  "v116_terminal_sha256":os.environ["V126_V116_TERMINAL_SHA256"],
  "instance_id":os.environ["INSTANCE_ID"],"exit_code":int(os.environ["EXIT_CODE"]),
  "phase":"bootstrap","status":"failed","elapsed_seconds":0,"artifacts":{{}}}},
  sort_keys=True,separators=(',',':')))
PY
    aws s3 cp terminal.json "$V126_OUTPUT_PREFIX/terminal.json" --only-show-errors || true
    shutdown -h now || true
  fi
}}
trap bootstrap_failure EXIT
aws s3 cp {shlex.quote(plan.archive_uri)} source.tar.gz --only-show-errors
[ "$(stat -c%s source.tar.gz)" = "{plan.archive_bytes}" ]
printf '%s  source.tar.gz\\n' {shlex.quote(plan.archive_sha256)} | sha256sum -c -
mkdir repo && tar -xzf source.tar.gz -C repo
trap - EXIT
exec bash repo/scripts/run_v126_relaion_expansion_source_remote.sh
"""


def launch_spec(plan: Plan, zone: str, subnet: str) -> dict:
    token = (
        "v126-"
        + hashlib.sha256(
            f"{plan.source_commit}:{plan.output_prefix}:{zone}".encode()
        ).hexdigest()[:48]
    )
    return {
        "BlockDeviceMappings": [
            {
                "DeviceName": "/dev/xvda",
                "Ebs": {
                    "DeleteOnTermination": True,
                    "Encrypted": True,
                    "VolumeSize": 120,
                    "VolumeType": "gp3",
                },
            }
        ],
        "ClientToken": token,
        "IamInstanceProfile": {"Arn": plan.instance_profile_arn},
        "ImageId": plan.image_id,
        "InstanceInitiatedShutdownBehavior": "terminate",
        "InstanceMarketOptions": {
            "MarketType": "spot",
            "SpotOptions": {
                "InstanceInterruptionBehavior": "terminate",
                "SpotInstanceType": "one-time",
            },
        },
        "InstanceType": plan.instance_type,
        "MaxCount": 1,
        "MinCount": 1,
        "NetworkInterfaces": [
            {
                "AssociatePublicIpAddress": True,
                "DeviceIndex": 0,
                "Groups": [plan.security_group_id],
                "SubnetId": subnet,
            }
        ],
        "TagSpecifications": [
            {
                "ResourceType": "instance",
                "Tags": [
                    {"Key": "Name", "Value": TAG},
                    {
                        "Key": "BorsukAttempt",
                        "Value": plan.output_prefix.rsplit("/", 1)[-1],
                    },
                ],
            }
        ],
        "UserData": base64.b64encode(user_data(plan).encode()).decode(),
    }


def launch_and_monitor(plan: Plan) -> dict:
    import boto3

    validate_plan(plan)
    session = boto3.Session(profile_name="causality", region_name="eu-central-1")
    ec2, s3 = session.client("ec2"), session.client("s3")
    bucket, prefix = _location(plan.output_prefix)
    if not _missing(s3, bucket, f"{prefix}/reservation.json") or not _missing(
        s3, bucket, f"{prefix}/terminal.json"
    ):
        raise ValueError("V126 immutable attempt already registered")
    active = ec2.describe_instances(
        Filters=[
            {"Name": "tag:Name", "Values": [TAG]},
            {
                "Name": "instance-state-name",
                "Values": ["pending", "running", "stopping", "stopped"],
            },
        ]
    )
    if any(item.get("Instances") for item in active["Reservations"]):
        raise ValueError("V126 Spot worker already active")
    prerequisite_bucket, prerequisite_key = _location(V116_PREFIX + "/terminal.json")
    raw = s3.get_object(Bucket=prerequisite_bucket, Key=prerequisite_key)["Body"].read()
    if hashlib.sha256(raw).hexdigest() != V116_TERMINAL_SHA256:
        raise ValueError("V126 V116 terminal SHA differs")
    old_terminal = json.loads(raw)
    old_instance = validate_v116_terminal(old_terminal)
    if old_terminal.get("artifacts", {}).get("rust-replay.jsonl") != {
        "bytes": INPUTS["SEALED_REPLAY"][2],
        "sha256": INPUTS["SEALED_REPLAY"][1],
    }:
        raise ValueError("V126 V116 sealed replay identity differs")
    state = ec2.describe_instances(InstanceIds=[old_instance])["Reservations"][0][
        "Instances"
    ][0]["State"]["Name"]
    if state != "terminated":
        raise ValueError("V126 V116 worker remains active")
    for uri, _digest, length in INPUTS.values():
        source_bucket, key = _location(uri)
        if s3.head_object(Bucket=source_bucket, Key=key)["ContentLength"] != length:
            raise ValueError("V126 input byte length differs")
    receipt = (
        json.dumps(
            {
                "schema": SCHEMA,
                "source_commit": plan.source_commit,
                "archive_sha256": plan.archive_sha256,
                "v116_terminal_sha256": V116_TERMINAL_SHA256,
                "inputs": {
                    role: {"uri": uri, "sha256": digest, "bytes": length}
                    for role, (uri, digest, length) in INPUTS.items()
                },
                "attempt": plan.output_prefix.rsplit("/", 1)[-1],
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n"
    ).encode()
    reserve_attempt(bucket, f"{prefix}/reservation.json", receipt)
    instance_id = None
    for target in DEFAULT_TARGETS:
        try:
            launched = ec2.run_instances(
                **launch_spec(plan, target.availability_zone, target.subnet_id)
            )
        except Exception as error:
            if any(
                marker in str(error)
                for marker in (
                    "InsufficientInstanceCapacity",
                    "InsufficientFreeAddressesInSubnet",
                    "MaxSpotInstanceCountExceeded",
                    "SpotMaxPriceTooLow",
                )
            ):
                continue
            raise
        instance_id = launched["Instances"][0]["InstanceId"]
        break
    if instance_id is None:
        raise RuntimeError("V126 Spot capacity unavailable; reservation retained")
    deadline = time.monotonic() + plan.wall_seconds + 1800
    try:
        while time.monotonic() < deadline:
            if not _missing(s3, bucket, f"{prefix}/terminal.json"):
                terminal = json.loads(
                    s3.get_object(Bucket=bucket, Key=f"{prefix}/terminal.json")[
                        "Body"
                    ].read()
                )
                if (
                    terminal.get("schema") != SCHEMA
                    or terminal.get("source_commit") != plan.source_commit
                    or terminal.get("instance_id") != instance_id
                ):
                    raise ValueError("V126 terminal identity differs")
                return terminal
            state = ec2.describe_instances(InstanceIds=[instance_id])["Reservations"][
                0
            ]["Instances"][0]["State"]["Name"]
            if state in {"stopped", "shutting-down", "terminated"}:
                raise RuntimeError(f"V126 Spot {instance_id} ended without terminal")
            time.sleep(20)
        raise TimeoutError(f"V126 Spot {instance_id} terminal deadline exceeded")
    finally:
        ec2.terminate_instances(InstanceIds=[instance_id])
        ec2.get_waiter("instance_terminated").wait(
            InstanceIds=[instance_id], WaiterConfig={"Delay": 5, "MaxAttempts": 60}
        )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--archive-uri", required=True)
    parser.add_argument("--archive-sha256", required=True)
    parser.add_argument("--archive-bytes", required=True, type=int)
    parser.add_argument("--output-prefix", required=True)
    args = parser.parse_args()
    terminal = launch_and_monitor(
        Plan(
            args.source_commit,
            args.archive_uri,
            args.archive_sha256,
            args.archive_bytes,
            args.output_prefix,
        )
    )
    print(json.dumps(terminal, sort_keys=True, separators=(",", ":")))
    if terminal.get("status") != "complete" or terminal.get("exit_code") != 0:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
