#!/usr/bin/env python3
"""Launch one immutable source-only deep-image index build on Causality Spot."""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import shlex
import time
from dataclasses import dataclass

from scripts.launch_native_geometric_layout_spot import DEFAULT_TARGETS
from scripts.launch_v114_1m_paired_spot import _location, _missing

BUCKET = "borsuk-bench-453182569524-euc1"
V119 = (f"s3://{BUCKET}/research/v119-deep-image-source/"
        "dc242e761ac2d60068c7e8e65812615a6ed6cfd7/runs/"
        "v119-deep-image-source-20260924T002421Z/a0001/artifacts")
SOURCE_URI = V119 + "/source.parquet"
PROVENANCE_URI = V119 + "/source.json"
SOURCE_SHA256 = "8f88122f412554107d97c07f440352f9043b8cb4b58fe08434ac75f4b90776ee"
PROVENANCE_SHA256 = "a643e6e3342429fe2047b5d69c756a3b6adcb37bdaffe2274f4067d344160fa8"
SOURCE_BYTES = 3_566_768_562
SCHEMA = "borsuk-v120-source-index-spot-v1"


@dataclass(frozen=True)
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
    instance_type: str = "c7i.12xlarge"
    wall_seconds: int = 7200


def _q(value: object) -> str:
    return shlex.quote(str(value))


def validate_plan(plan: Plan) -> None:
    hex_value = lambda value, length: (len(value) == length and
        all(char in "0123456789abcdef" for char in value))
    if (not hex_value(plan.source_commit, 40)
            or not hex_value(plan.archive_sha256, 64)
            or plan.archive_bytes <= 0
            or not plan.archive_uri.startswith("s3://")
            or not plan.output_prefix.startswith("s3://")
            or plan.source_commit not in plan.output_prefix
            or plan.instance_type != "c7i.12xlarge"
            or plan.wall_seconds != 7200):
        raise ValueError("V120 immutable source plan differs")


def user_data(plan: Plan) -> str:
    validate_plan(plan)
    exports = "\n".join((
        f"export V120_SOURCE_COMMIT={_q(plan.source_commit)}",
        f"export V120_OUTPUT_PREFIX={_q(plan.output_prefix.rstrip('/'))}",
        f"export V120_WALL_SECONDS={plan.wall_seconds}",
        f"export V120_SOURCE_URI={_q(SOURCE_URI)}",
        f"export V120_PROVENANCE_URI={_q(PROVENANCE_URI)}",
        f"export V120_SOURCE_SHA256={SOURCE_SHA256}",
        f"export V120_PROVENANCE_SHA256={PROVENANCE_SHA256}",
        f"export V120_SOURCE_BYTES={SOURCE_BYTES}",
    ))
    return f"""#!/bin/bash
set -euo pipefail
root=/mnt/v120-index
mkdir -p "$root" && cd "$root"
{exports}
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
print(json.dumps({{"schema":"{SCHEMA}","source_commit":os.environ["V120_SOURCE_COMMIT"],
  "instance_id":os.environ["INSTANCE_ID"],"exit_code":int(os.environ["EXIT_CODE"]),
  "phase":"bootstrap","status":"failed","elapsed_seconds":0,"artifacts":{{}}}},
  sort_keys=True,separators=(',',':')))
PY
    aws s3 cp terminal.json "$V120_OUTPUT_PREFIX/terminal.json" --only-show-errors || true
    shutdown -h now || true
  fi
}}
trap bootstrap_failure EXIT
aws s3 cp {_q(plan.archive_uri)} source.tar.gz --only-show-errors
[ "$(stat -c%s source.tar.gz)" = "{plan.archive_bytes}" ]
printf '%s  source.tar.gz\\n' {_q(plan.archive_sha256)} | sha256sum -c -
mkdir repo && tar -xzf source.tar.gz -C repo
trap - EXIT
exec bash repo/scripts/run_v120_source_index_remote.sh
"""


def launch_spec(plan: Plan, zone: str, subnet: str) -> dict:
    token = "v120-" + hashlib.sha256(
        f"{plan.source_commit}:{plan.output_prefix}:{zone}".encode()
    ).hexdigest()[:48]
    return {
        "BlockDeviceMappings": [{"DeviceName": "/dev/xvda", "Ebs": {
            "DeleteOnTermination": True, "Encrypted": True,
            "VolumeSize": 120, "VolumeType": "gp3"}}],
        "ClientToken": token,
        "IamInstanceProfile": {"Arn": plan.instance_profile_arn},
        "ImageId": plan.image_id,
        "InstanceInitiatedShutdownBehavior": "terminate",
        "InstanceMarketOptions": {"MarketType": "spot", "SpotOptions": {
            "InstanceInterruptionBehavior": "terminate", "SpotInstanceType": "one-time"}},
        "InstanceType": plan.instance_type, "MaxCount": 1, "MinCount": 1,
        "NetworkInterfaces": [{"AssociatePublicIpAddress": True, "DeviceIndex": 0,
                               "Groups": [plan.security_group_id], "SubnetId": subnet}],
        "TagSpecifications": [{"ResourceType": "instance", "Tags": [
            {"Key": "Name", "Value": "borsuk-v120-source-index"},
            {"Key": "BorsukAttempt", "Value": "a0001"}]}],
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
        raise ValueError("V120 immutable source attempt already registered")
    receipt = (json.dumps({"schema": SCHEMA, "source_commit": plan.source_commit,
                           "archive_sha256": plan.archive_sha256,
                           "source_sha256": SOURCE_SHA256, "attempt": 1},
                          sort_keys=True, separators=(",", ":")) + "\n").encode()
    s3.put_object(Bucket=bucket, Key=f"{prefix}/reservation.json", Body=receipt,
                  IfNoneMatch="*", ContentType="application/json")
    instance_id = None
    for target in DEFAULT_TARGETS:
        try:
            launched = ec2.run_instances(**launch_spec(
                plan, target.availability_zone, target.subnet_id))
        except Exception as error:
            if any(marker in str(error) for marker in (
                "InsufficientInstanceCapacity", "InsufficientFreeAddressesInSubnet",
                "MaxSpotInstanceCountExceeded", "SpotMaxPriceTooLow",
            )):
                continue
            raise
        instance_id = launched["Instances"][0]["InstanceId"]
        break
    if instance_id is None:
        raise RuntimeError("V120 Spot capacity unavailable; reservation retained")
    deadline = time.monotonic() + plan.wall_seconds + 1800
    try:
        while time.monotonic() < deadline:
            if not _missing(s3, bucket, f"{prefix}/terminal.json"):
                terminal = json.loads(s3.get_object(
                    Bucket=bucket, Key=f"{prefix}/terminal.json")["Body"].read())
                if (terminal.get("schema") != SCHEMA
                        or terminal.get("source_commit") != plan.source_commit
                        or terminal.get("instance_id") != instance_id):
                    raise ValueError("V120 terminal identity differs")
                return terminal
            state = ec2.describe_instances(InstanceIds=[instance_id])[
                "Reservations"][0]["Instances"][0]["State"]["Name"]
            if state in {"stopped", "shutting-down", "terminated"}:
                raise RuntimeError(f"V120 Spot {instance_id} ended without terminal")
            time.sleep(20)
        raise TimeoutError(f"V120 Spot {instance_id} terminal deadline exceeded")
    finally:
        ec2.terminate_instances(InstanceIds=[instance_id])
        ec2.get_waiter("instance_terminated").wait(
            InstanceIds=[instance_id], WaiterConfig={"Delay": 5, "MaxAttempts": 60})


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--archive-uri", required=True)
    parser.add_argument("--archive-sha256", required=True)
    parser.add_argument("--archive-bytes", required=True, type=int)
    parser.add_argument("--output-prefix", required=True)
    args = parser.parse_args()
    terminal = launch_and_monitor(Plan(
        args.source_commit, args.archive_uri, args.archive_sha256,
        args.archive_bytes, args.output_prefix,
    ))
    print(json.dumps(terminal, sort_keys=True, separators=(",", ":")))


if __name__ == "__main__":
    main()
