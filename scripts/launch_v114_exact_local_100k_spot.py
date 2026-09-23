#!/usr/bin/env python3
"""Register and monitor one source-frozen V114 exact-local 100k Spot attempt."""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import shlex
import time
from dataclasses import dataclass

from scripts.launch_native_geometric_layout_spot import (
    DEFAULT_TARGETS, FROZEN_QUERIES,
)
from scripts.v114_exact_local_100k import V113_SEAL_SHA256

SCHEMA = "borsuk-v114-exact-local-100k-spot-v1"
V113_ARTIFACT_URI = (
    "s3://borsuk-bench-453182569524-euc1/research/v113-resident-nominee/"
    "9f4936fbb9bad5596921ff71004984f9b295bc57/"
    "runs/v113-100k-score-20260923T203231Z/a0001/artifacts/artifact"
)


@dataclass(frozen=True, slots=True)
class Plan:
    source_commit: str
    archive_uri: str
    archive_sha256: str
    archive_bytes: int
    output_prefix: str
    image_id: str = "ami-06121aa3085b6f918"
    security_group_id: str = "sg-0b1fd3e4fbde4af0d"
    instance_profile_arn: str = (
        "arn:aws:iam::453182569524:instance-profile/borsuk-bench-profile"
    )
    instance_type: str = "c7i.8xlarge"
    wall_seconds: int = 14_400


def _hex64(value: str) -> bool:
    return len(value) == 64 and all(c in "0123456789abcdef" for c in value)


def validate_plan(plan: Plan) -> None:
    if (
        len(plan.source_commit) != 40
        or not all(c in "0123456789abcdef" for c in plan.source_commit)
        or not plan.archive_uri.startswith("s3://")
        or not _hex64(plan.archive_sha256)
        or plan.archive_bytes <= 0
        or not plan.output_prefix.startswith("s3://")
        or plan.source_commit not in plan.output_prefix
        or plan.instance_type != "c7i.8xlarge"
        or plan.wall_seconds != 14_400
        or not plan.image_id.startswith("ami-")
        or not plan.security_group_id.startswith("sg-")
        or not plan.instance_profile_arn.startswith("arn:aws:iam::")
    ):
        raise ValueError("V114 immutable Spot plan differs")


def _q(value: object) -> str:
    return shlex.quote(str(value))


def user_data(plan: Plan) -> str:
    """Bootstrap once; the archived runner owns science and terminal upload."""
    validate_plan(plan)
    return f"""#!/bin/bash
set -euo pipefail
root=/mnt/v114-exact-local-100k
mkdir -p "$root"
cd "$root"
export V114_SOURCE_COMMIT={_q(plan.source_commit)}
export V114_OUTPUT_PREFIX={_q(plan.output_prefix.rstrip('/'))}
export V114_V113_ARTIFACT_URI={_q(V113_ARTIFACT_URI)}
export V114_V113_SEAL_SHA256={_q(V113_SEAL_SHA256)}
export V114_QUERY_URI={_q(FROZEN_QUERIES.uri)}
export V114_QUERY_SHA256={_q(FROZEN_QUERIES.sha256)}
export V114_QUERY_BYTES={FROZEN_QUERIES.encoded_bytes}
export V114_WALL_SECONDS={plan.wall_seconds}
bootstrap_failure() {{
  code=$?
  trap - EXIT
  if [ "$code" -ne 0 ]; then
    token=$(curl -fsS -X PUT -H 'X-aws-ec2-metadata-token-ttl-seconds: 60' \
      http://169.254.169.254/latest/api/token || true)
    instance_id=$(curl -fsS -H "X-aws-ec2-metadata-token: $token" \
      http://169.254.169.254/latest/meta-data/instance-id || true)
    INSTANCE_ID="$instance_id" EXIT_CODE="$code" python3 - <<'PY' >terminal.json
import json, os
print(json.dumps({{"schema":"borsuk-v114-exact-local-100k-spot-v1","attempt":1,
  "source_commit":os.environ["V114_SOURCE_COMMIT"],
  "upstream_artifact":os.environ["V114_V113_ARTIFACT_URI"],
  "instance_id":os.environ["INSTANCE_ID"],
  "exit_code":int(os.environ["EXIT_CODE"]),"phase":"bootstrap",
  "status":"failed","elapsed_seconds":0,"artifacts":{{}}}},
  sort_keys=True,separators=(',',':')))
PY
    aws s3 cp terminal.json "$V114_OUTPUT_PREFIX/terminal.json" --only-show-errors || true
    shutdown -h now || true
  fi
}}
trap bootstrap_failure EXIT
aws s3 cp {_q(plan.archive_uri)} source.tar.gz --only-show-errors
[ "$(stat -c%s source.tar.gz)" = "{plan.archive_bytes}" ]
printf '%s  source.tar.gz\\n' {_q(plan.archive_sha256)} | sha256sum -c -
mkdir repo
tar -xzf source.tar.gz -C repo
trap - EXIT
exec bash repo/scripts/run_v114_exact_local_100k_remote.sh
"""


def launch_spec(plan: Plan, zone: str, subnet: str) -> dict[str, object]:
    token = "v114-" + hashlib.sha256(
        f"{plan.source_commit}:{plan.output_prefix}:{zone}".encode()
    ).hexdigest()[:48]
    return {
        "BlockDeviceMappings": [{
            "DeviceName": "/dev/xvda",
            "Ebs": {"DeleteOnTermination": True, "Encrypted": True,
                    "VolumeSize": 100, "VolumeType": "gp3"},
        }],
        "ClientToken": token,
        "IamInstanceProfile": {"Arn": plan.instance_profile_arn},
        "ImageId": plan.image_id,
        "InstanceInitiatedShutdownBehavior": "terminate",
        "InstanceMarketOptions": {
            "MarketType": "spot",
            "SpotOptions": {"InstanceInterruptionBehavior": "terminate",
                            "SpotInstanceType": "one-time"},
        },
        "InstanceType": plan.instance_type,
        "MaxCount": 1, "MinCount": 1,
        "NetworkInterfaces": [{"AssociatePublicIpAddress": True,
                               "DeviceIndex": 0,
                               "Groups": [plan.security_group_id],
                               "SubnetId": subnet}],
        "TagSpecifications": [{"ResourceType": "instance", "Tags": [
            {"Key": "Name", "Value": "borsuk-v114-exact-local-100k"},
            {"Key": "BorsukAttempt", "Value": "a0001"},
        ]}],
        "UserData": base64.b64encode(user_data(plan).encode()).decode(),
    }


def _location(uri: str) -> tuple[str, str]:
    bucket, slash, key = uri.removeprefix("s3://").partition("/")
    if not bucket or not slash or not key:
        raise ValueError("V114 S3 location differs")
    return bucket, key.rstrip("/")


def _missing(s3: object, bucket: str, key: str) -> bool:
    try:
        s3.head_object(Bucket=bucket, Key=key)
    except Exception as error:
        code = str(getattr(error, "response", {}).get("Error", {}).get("Code", ""))
        if code in {"404", "NoSuchKey", "NotFound"}:
            return True
        raise
    return False


def _reserve(s3: object, plan: Plan, bucket: str, prefix: str) -> None:
    if not _missing(s3, bucket, f"{prefix}/reservation.json") or not _missing(
        s3, bucket, f"{prefix}/terminal.json"
    ):
        raise ValueError("V114 immutable attempt already registered")
    body = (json.dumps({"schema": SCHEMA, "source_commit": plan.source_commit,
                        "archive_sha256": plan.archive_sha256, "attempt": 1},
                       sort_keys=True, separators=(",", ":")) + "\n").encode()
    event = "before-sign.s3.PutObject"
    event_id = "borsuk-v114-reservation-if-none-match"
    def precondition(request: object, **_: object) -> None:
        request.headers["If-None-Match"] = "*"
    s3.meta.events.register_first(event, precondition, unique_id=event_id)
    try:
        s3.put_object(Bucket=bucket, Key=f"{prefix}/reservation.json",
                      Body=body, ContentType="application/json")
    finally:
        s3.meta.events.unregister(event, unique_id=event_id)


def launch_and_monitor(plan: Plan) -> dict[str, object]:
    """One Spot instance; observe terminal and infrastructure, never partial data."""
    import boto3

    validate_plan(plan)
    session = boto3.Session(profile_name="causality", region_name="eu-central-1")
    ec2, s3 = session.client("ec2"), session.client("s3")
    bucket, prefix = _location(plan.output_prefix)
    _reserve(s3, plan, bucket, prefix)
    instance_id: str | None = None
    capacity = ("InsufficientInstanceCapacity", "InsufficientFreeAddressesInSubnet",
                "MaxSpotInstanceCountExceeded", "SpotMaxPriceTooLow")
    for target in DEFAULT_TARGETS:
        try:
            result = ec2.run_instances(**launch_spec(
                plan, target.availability_zone, target.subnet_id,
            ))
        except Exception as error:
            if any(marker in str(error) for marker in capacity):
                continue
            raise
        instance_id = result["Instances"][0]["InstanceId"]
        break
    if instance_id is None:
        raise RuntimeError("V114 Spot capacity unavailable; reservation retained")
    deadline = time.monotonic() + plan.wall_seconds + 1200
    try:
        while time.monotonic() < deadline:
            if not _missing(s3, bucket, f"{prefix}/terminal.json"):
                body = s3.get_object(
                    Bucket=bucket, Key=f"{prefix}/terminal.json",
                )["Body"].read()
                terminal = json.loads(body)
                if (
                    body != (json.dumps(terminal, sort_keys=True,
                                        separators=(",", ":")) + "\n").encode()
                    or terminal.get("schema") != SCHEMA
                    or terminal.get("instance_id") != instance_id
                    or terminal.get("source_commit") != plan.source_commit
                ):
                    raise ValueError("V114 terminal identity differs")
                return terminal
            state = ec2.describe_instances(InstanceIds=[instance_id])[
                "Reservations"
            ][0]["Instances"][0]["State"]["Name"]
            if state in {"stopped", "shutting-down", "terminated"}:
                raise RuntimeError(f"V114 Spot {instance_id} ended without terminal")
            time.sleep(20)
        raise TimeoutError(f"V114 Spot {instance_id} terminal deadline exceeded")
    finally:
        ec2.terminate_instances(InstanceIds=[instance_id])
        ec2.get_waiter("instance_terminated").wait(
            InstanceIds=[instance_id], WaiterConfig={"Delay": 5, "MaxAttempts": 60},
        )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--archive-uri", required=True)
    parser.add_argument("--archive-sha256", required=True)
    parser.add_argument("--archive-bytes", type=int, required=True)
    parser.add_argument("--output-prefix", required=True)
    args = parser.parse_args()
    terminal = launch_and_monitor(Plan(
        args.source_commit, args.archive_uri, args.archive_sha256,
        args.archive_bytes, args.output_prefix,
    ))
    print(json.dumps(terminal, sort_keys=True, separators=(",", ":")))


if __name__ == "__main__":
    main()
