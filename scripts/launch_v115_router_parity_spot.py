#!/usr/bin/env python3
"""One immutable Causality Spot cell for source-only Rust PQ64 parity."""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import shlex
import time
from dataclasses import dataclass

from scripts.launch_native_geometric_layout_spot import DEFAULT_TARGETS
from scripts.launch_v112_precise_nominee_spot import INPUTS
from scripts.launch_v114_1m_paired_spot import _location, _missing
from scripts.validate_v115_router_parity import FROZEN_REQUESTS_SHA256

SCHEMA = "borsuk-v115-source-router-parity-spot-v1"
REQUESTS_URI = (
    "s3://borsuk-bench-453182569524-euc1/research/v114-1m-paired/"
    "87881c71e048d557c5c1285ffa0ac1d8c381f764/"
    "runs/v114-1m-dev-20260923T222600Z/a0001/artifacts/requests-1000.jsonl"
)
REQUESTS_BYTES = 18_217_189


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
    instance_type: str = "c7i.12xlarge"
    wall_seconds: int = 7_200


def _hex64(value: str) -> bool:
    return len(value) == 64 and all(char in "0123456789abcdef" for char in value)


def validate_plan(plan: Plan) -> None:
    if (
        len(plan.source_commit) != 40
        or any(char not in "0123456789abcdef" for char in plan.source_commit)
        or not plan.archive_uri.startswith("s3://")
        or not _hex64(plan.archive_sha256)
        or plan.archive_bytes <= 0
        or not plan.output_prefix.startswith("s3://")
        or plan.source_commit not in plan.output_prefix
        or plan.instance_type != "c7i.12xlarge"
        or plan.wall_seconds != 7_200
    ):
        raise ValueError("V115 immutable Spot plan differs")


def _q(value: object) -> str:
    return shlex.quote(str(value))


def user_data(plan: Plan) -> str:
    validate_plan(plan)
    variables = [
        f"export V115_SOURCE_COMMIT={_q(plan.source_commit)}",
        f"export V115_OUTPUT_PREFIX={_q(plan.output_prefix.rstrip('/'))}",
        f"export V115_WALL_SECONDS={plan.wall_seconds}",
        f"export V115_REQUESTS_URI={_q(REQUESTS_URI)}",
        f"export V115_REQUESTS_SHA256={_q(FROZEN_REQUESTS_SHA256)}",
        f"export V115_REQUESTS_BYTES={REQUESTS_BYTES}",
    ]
    for role in ("SOURCE", "LAYOUT", "SQ8", "QUERIES", "TRUTH"):
        identity = INPUTS[role]
        variables += [
            f"export V115_{role}_URI={_q(identity.uri)}",
            f"export V115_{role}_SHA256={_q(identity.sha256)}",
            f"export V115_{role}_BYTES={identity.bytes}",
        ]
    environment = "\n".join(variables)
    return f"""#!/bin/bash
set -euo pipefail
root=/mnt/v115-router-parity
mkdir -p "$root"
cd "$root"
{environment}
bootstrap_failure() {{
  code=$?
  trap - EXIT
  if [ "$code" -ne 0 ]; then
    token=$(curl -fsS -X PUT -H 'X-aws-ec2-metadata-token-ttl-seconds: 60' \\
      http://169.254.169.254/latest/api/token || true)
    instance_id=$(curl -fsS -H "X-aws-ec2-metadata-token: $token" \\
      http://169.254.169.254/latest/meta-data/instance-id || true)
    INSTANCE_ID="$instance_id" EXIT_CODE="$code" python3 - <<'PY' >terminal.json
import json, os
print(json.dumps({{"schema":"{SCHEMA}","attempt":1,
  "source_commit":os.environ["V115_SOURCE_COMMIT"],
  "instance_id":os.environ["INSTANCE_ID"],
  "exit_code":int(os.environ["EXIT_CODE"]),"phase":"bootstrap",
  "status":"failed","elapsed_seconds":0,"artifacts":{{}}}},
  sort_keys=True,separators=(',',':')))
PY
    aws s3 cp terminal.json "$V115_OUTPUT_PREFIX/terminal.json" --only-show-errors || true
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
exec bash repo/scripts/run_v115_router_parity_remote.sh
"""


def launch_spec(plan: Plan, zone: str, subnet: str) -> dict[str, object]:
    token = "v115-" + hashlib.sha256(
        f"{plan.source_commit}:{plan.output_prefix}:{zone}".encode()
    ).hexdigest()[:48]
    return {
        "BlockDeviceMappings": [{"DeviceName": "/dev/xvda",
                                 "Ebs": {"DeleteOnTermination": True, "Encrypted": True,
                                         "VolumeSize": 120, "VolumeType": "gp3"}}],
        "ClientToken": token,
        "IamInstanceProfile": {"Arn": plan.instance_profile_arn},
        "ImageId": plan.image_id,
        "InstanceInitiatedShutdownBehavior": "terminate",
        "InstanceMarketOptions": {"MarketType": "spot",
                                  "SpotOptions": {"InstanceInterruptionBehavior": "terminate",
                                                  "SpotInstanceType": "one-time"}},
        "InstanceType": plan.instance_type,
        "MaxCount": 1, "MinCount": 1,
        "NetworkInterfaces": [{"AssociatePublicIpAddress": True, "DeviceIndex": 0,
                               "Groups": [plan.security_group_id], "SubnetId": subnet}],
        "TagSpecifications": [{"ResourceType": "instance", "Tags": [
            {"Key": "Name", "Value": "borsuk-v115-source-router-parity"},
            {"Key": "BorsukAttempt", "Value": "a0001"},
        ]}],
        "UserData": base64.b64encode(user_data(plan).encode()).decode(),
    }


def _reserve(s3: object, plan: Plan, bucket: str, prefix: str) -> None:
    if not _missing(s3, bucket, f"{prefix}/reservation.json") or not _missing(
        s3, bucket, f"{prefix}/terminal.json"
    ):
        raise ValueError("V115 immutable attempt already registered")
    body = (json.dumps({"schema": SCHEMA, "source_commit": plan.source_commit,
                        "archive_sha256": plan.archive_sha256, "attempt": 1},
                       sort_keys=True, separators=(",", ":")) + "\n").encode()
    event = "before-sign.s3.PutObject"
    event_id = "borsuk-v115-reservation-if-none-match"
    def precondition(request: object, **_: object) -> None:
        request.headers["If-None-Match"] = "*"
    s3.meta.events.register_first(event, precondition, unique_id=event_id)
    try:
        s3.put_object(Bucket=bucket, Key=f"{prefix}/reservation.json",
                      Body=body, ContentType="application/json")
    finally:
        s3.meta.events.unregister(event, unique_id=event_id)


def launch_and_monitor(plan: Plan) -> dict[str, object]:
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
        raise RuntimeError("V115 Spot capacity unavailable; reservation retained")
    deadline = time.monotonic() + plan.wall_seconds + 1200
    try:
        while time.monotonic() < deadline:
            if not _missing(s3, bucket, f"{prefix}/terminal.json"):
                body = s3.get_object(Bucket=bucket, Key=f"{prefix}/terminal.json")["Body"].read()
                terminal = json.loads(body)
                if (
                    body != (json.dumps(terminal, sort_keys=True,
                                        separators=(",", ":")) + "\n").encode()
                    or terminal.get("schema") != SCHEMA
                    or terminal.get("instance_id") != instance_id
                    or terminal.get("source_commit") != plan.source_commit
                ):
                    raise ValueError("V115 terminal identity differs")
                return terminal
            state = ec2.describe_instances(InstanceIds=[instance_id])[
                "Reservations"
            ][0]["Instances"][0]["State"]["Name"]
            if state in {"stopped", "shutting-down", "terminated"}:
                raise RuntimeError(f"V115 Spot {instance_id} ended without terminal")
            time.sleep(20)
        raise TimeoutError(f"V115 Spot {instance_id} terminal deadline exceeded")
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
