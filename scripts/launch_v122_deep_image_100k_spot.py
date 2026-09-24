#!/usr/bin/env python3
"""Launch one D96 100k development screen after V120 terminates."""

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
from scripts.launch_v120_source_index_spot import (
    SOURCE_BYTES, SOURCE_SHA256, SOURCE_URI,
)
from scripts.launch_v121_deep_image_paired_spot import (
    INDEX_PREFIX, QUERY_BYTES, QUERY_SHA256, QUERY_URI,
)
from scripts.v121_download_index import validate_terminal

BUCKET = "borsuk-bench-453182569524-euc1"
SCHEMA = "borsuk-v122-deep-image-100k-spot-v1"


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
    instance_type: str = "c7i.4xlarge"
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
            or plan.instance_type != "c7i.4xlarge"
            or plan.wall_seconds != 7200):
        raise ValueError("V122 immutable screen plan differs")


def user_data(plan: Plan) -> str:
    validate_plan(plan)
    exports = "\n".join((
        f"export V122_SOURCE_COMMIT={_q(plan.source_commit)}",
        f"export V122_OUTPUT_PREFIX={_q(plan.output_prefix.rstrip('/'))}",
        f"export V122_WALL_SECONDS={plan.wall_seconds}",
        f"export V122_SOURCE_URI={_q(SOURCE_URI)}",
        f"export V122_SOURCE_SHA256={SOURCE_SHA256}",
        f"export V122_SOURCE_BYTES={SOURCE_BYTES}",
        f"export V122_QUERY_URI={_q(QUERY_URI)}",
        f"export V122_QUERY_SHA256={QUERY_SHA256}",
        f"export V122_QUERY_BYTES={QUERY_BYTES}",
    ))
    return f"""#!/bin/bash
set -euo pipefail
root=/mnt/v122-screen
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
print(json.dumps({{"schema":"{SCHEMA}","source_commit":os.environ["V122_SOURCE_COMMIT"],
  "instance_id":os.environ["INSTANCE_ID"],"exit_code":int(os.environ["EXIT_CODE"]),
  "phase":"bootstrap","status":"failed","elapsed_seconds":0,"artifacts":{{}}}},
  sort_keys=True,separators=(',',':')))
PY
    aws s3 cp terminal.json "$V122_OUTPUT_PREFIX/terminal.json" --only-show-errors || true
    shutdown -h now || true
  fi
}}
trap bootstrap_failure EXIT
aws s3 cp {_q(plan.archive_uri)} source.tar.gz --only-show-errors
[ "$(stat -c%s source.tar.gz)" = "{plan.archive_bytes}" ]
printf '%s  source.tar.gz\\n' {_q(plan.archive_sha256)} | sha256sum -c -
mkdir repo && tar -xzf source.tar.gz -C repo
trap - EXIT
exec bash repo/scripts/run_v122_deep_image_100k_remote.sh
"""


def launch_spec(plan: Plan, zone: str, subnet: str) -> dict:
    token = "v122-" + hashlib.sha256(
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
            {"Key": "Name", "Value": "borsuk-v122-deep-image-100k"},
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
        raise ValueError("V122 immutable screen attempt already registered")
    index_bucket, index_prefix = _location(INDEX_PREFIX)
    raw = s3.get_object(Bucket=index_bucket,
                        Key=f"{index_prefix}/terminal.json")["Body"].read()
    terminal = json.loads(raw)
    validate_terminal(terminal)
    instance_id = terminal["instance_id"]
    state = ec2.describe_instances(InstanceIds=[instance_id])[
        "Reservations"][0]["Instances"][0]["State"]["Name"]
    if state != "terminated":
        raise ValueError("V120 Spot has not terminated")
    receipt = (json.dumps({"schema": SCHEMA, "source_commit": plan.source_commit,
                           "archive_sha256": plan.archive_sha256,
                           "v120_terminal_sha256": hashlib.sha256(raw).hexdigest(),
                           "source_sha256": SOURCE_SHA256,
                           "query_sha256": QUERY_SHA256, "attempt": 1},
                          sort_keys=True, separators=(",", ":")) + "\n").encode()
    s3.put_object(Bucket=bucket, Key=f"{prefix}/reservation.json", Body=receipt,
                  IfNoneMatch="*", ContentType="application/json")
    screen_instance = None
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
        screen_instance = launched["Instances"][0]["InstanceId"]
        break
    if screen_instance is None:
        raise RuntimeError("V122 Spot capacity unavailable; reservation retained")
    deadline = time.monotonic() + plan.wall_seconds + 1800
    try:
        while time.monotonic() < deadline:
            if not _missing(s3, bucket, f"{prefix}/terminal.json"):
                finished = json.loads(s3.get_object(
                    Bucket=bucket, Key=f"{prefix}/terminal.json")["Body"].read())
                if (finished.get("schema") != SCHEMA
                        or finished.get("source_commit") != plan.source_commit
                        or finished.get("instance_id") != screen_instance):
                    raise ValueError("V122 terminal identity differs")
                return finished
            state = ec2.describe_instances(InstanceIds=[screen_instance])[
                "Reservations"][0]["Instances"][0]["State"]["Name"]
            if state in {"stopped", "shutting-down", "terminated"}:
                raise RuntimeError(f"V122 Spot {screen_instance} ended without terminal")
            time.sleep(20)
        raise TimeoutError(f"V122 Spot {screen_instance} terminal deadline exceeded")
    finally:
        ec2.terminate_instances(InstanceIds=[screen_instance])
        ec2.get_waiter("instance_terminated").wait(
            InstanceIds=[screen_instance], WaiterConfig={"Delay": 5, "MaxAttempts": 60})


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
