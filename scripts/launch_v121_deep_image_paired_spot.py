#!/usr/bin/env python3
"""Launch the untouched paired D96 quality gate after a sealed V120 build."""

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
from scripts.v121_download_index import validate_terminal

BUCKET = "borsuk-bench-453182569524-euc1"
INDEX_PREFIX = (f"s3://{BUCKET}/research/v120-deep-image-index/"
                "b919685cf1db6c14912c2118d5226c0fb426226b/runs/"
                "v120-20260924T004145Z/a0001")
SCREEN_COMMIT = "afe07cb5a9ba8518263375595f589639fdf3f4f1"
SCREEN_PREFIX = (f"s3://{BUCKET}/research/v122-deep-image-100k/"
                 f"{SCREEN_COMMIT}/runs/v122-20260924T011355Z/a0001")
STAGE_PREFIX = (f"s3://{BUCKET}/publication/v3/20260812/datasets/"
                "deep-image-96/attempts/0001/materialized")
QUERY_URI = STAGE_PREFIX + "/test.parquet"
TRUTH_URI = STAGE_PREFIX + "/neighbors.parquet"
QUERY_SHA256 = "296d45828020c1c0b88c6a1d5c822f6283280513b8c58d01cfa961f3a139a5d4"
TRUTH_SHA256 = "d305fcea7387988941defd2942cca1673693271329f977ba073da888cac3de8d"
QUERY_BYTES = 3_843_448
TRUTH_BYTES = 4_003_585
SCHEMA = "borsuk-v121-deep-image-paired-spot-v1"


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
        raise ValueError("V121 immutable paired plan differs")


def validate_v122_gate(terminal: dict, summary_raw: bytes) -> str:
    """Authenticate the preregistered development gate before untouched reads."""
    artifact = terminal.get("artifacts", {}).get("summary.json", {})
    if (terminal.get("schema") != "borsuk-v122-deep-image-100k-spot-v1"
            or terminal.get("source_commit") != SCREEN_COMMIT
            or terminal.get("status") != "complete"
            or terminal.get("phase") != "complete"
            or terminal.get("exit_code") != 0
            or not terminal.get("instance_id")
            or artifact.get("bytes") != len(summary_raw)
            or artifact.get("sha256") != hashlib.sha256(summary_raw).hexdigest()):
        raise ValueError("V122 sealed terminal differs")
    summary = json.loads(summary_raw)
    hits = summary.get("returned_hits", {})
    p05 = summary.get("p05_hits", {})
    sub90 = summary.get("sub90_queries", {})
    gets = summary.get("maximum_gets", {})
    byte_counts = summary.get("maximum_bytes", {})
    if (summary.get("schema") != "borsuk-v122-deep-image-100k-development-v1"
            or summary.get("dataset") != "deep-image-96-angular-random-100k"
            or summary.get("split") != "test-ordinals-9000-through-9999"
            or summary.get("rows") != 100_000
            or summary.get("dimensions") != 96
            or summary.get("query_count") != 1_000
            or summary.get("qualifies_100k_screen") is not True
            or summary.get("live_s3_measured") is not False
            or not 99_000 <= hits.get("candidate", -1) <= 100_000
            or not 0 <= hits.get("baseline", -1) <= hits["candidate"]
            or not 90 <= p05.get("candidate", -1) <= 100
            or not 0 <= p05.get("baseline", -1) <= p05["candidate"]
            or not 0 <= sub90.get("candidate", -1) <= sub90.get("baseline", -1)
            or any(not 1 <= gets.get(arm, -1) <= 32 for arm in ("candidate", "baseline"))
            or any(not 1 <= byte_counts.get(arm, -1) <= 16_777_216
                   for arm in ("candidate", "baseline"))):
        raise ValueError("V122 100k quality gate differs")
    return terminal["instance_id"]


def user_data(plan: Plan, index_terminal_sha256: str) -> str:
    validate_plan(plan)
    if len(index_terminal_sha256) != 64:
        raise ValueError("V120 terminal SHA-256 differs")
    exports = "\n".join((
        f"export V121_SOURCE_COMMIT={_q(plan.source_commit)}",
        f"export V121_OUTPUT_PREFIX={_q(plan.output_prefix.rstrip('/'))}",
        f"export V121_WALL_SECONDS={plan.wall_seconds}",
        f"export V121_INDEX_PREFIX={_q(INDEX_PREFIX)}",
        f"export V121_INDEX_TERMINAL_SHA256={index_terminal_sha256}",
        f"export V121_QUERY_URI={_q(QUERY_URI)}",
        f"export V121_QUERY_SHA256={QUERY_SHA256}",
        f"export V121_QUERY_BYTES={QUERY_BYTES}",
        f"export V121_TRUTH_URI={_q(TRUTH_URI)}",
        f"export V121_TRUTH_SHA256={TRUTH_SHA256}",
        f"export V121_TRUTH_BYTES={TRUTH_BYTES}",
    ))
    return f"""#!/bin/bash
set -euo pipefail
root=/mnt/v121-paired
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
print(json.dumps({{"schema":"{SCHEMA}","source_commit":os.environ["V121_SOURCE_COMMIT"],
  "index_terminal_sha256":os.environ["V121_INDEX_TERMINAL_SHA256"],
  "instance_id":os.environ["INSTANCE_ID"],"exit_code":int(os.environ["EXIT_CODE"]),
  "phase":"bootstrap","status":"failed","elapsed_seconds":0,"artifacts":{{}}}},
  sort_keys=True,separators=(',',':')))
PY
    aws s3 cp terminal.json "$V121_OUTPUT_PREFIX/terminal.json" --only-show-errors || true
    shutdown -h now || true
  fi
}}
trap bootstrap_failure EXIT
aws s3 cp {_q(plan.archive_uri)} source.tar.gz --only-show-errors
[ "$(stat -c%s source.tar.gz)" = "{plan.archive_bytes}" ]
printf '%s  source.tar.gz\\n' {_q(plan.archive_sha256)} | sha256sum -c -
mkdir repo && tar -xzf source.tar.gz -C repo
trap - EXIT
exec bash repo/scripts/run_v121_deep_image_paired_remote.sh
"""


def launch_spec(plan: Plan, zone: str, subnet: str,
                index_terminal_sha256: str) -> dict:
    token = "v121-" + hashlib.sha256(
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
            {"Key": "Name", "Value": "borsuk-v121-deep-image-paired"},
            {"Key": "BorsukAttempt", "Value": "a0001"}]}],
        "UserData": base64.b64encode(user_data(
            plan, index_terminal_sha256).encode()).decode(),
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
        raise ValueError("V121 immutable paired attempt already registered")
    index_bucket, index_prefix = _location(INDEX_PREFIX)
    raw = s3.get_object(Bucket=index_bucket,
                        Key=f"{index_prefix}/terminal.json")["Body"].read()
    index_terminal = json.loads(raw)
    validate_terminal(index_terminal)
    index_terminal_sha256 = hashlib.sha256(raw).hexdigest()
    screen_bucket, screen_prefix = _location(SCREEN_PREFIX)
    screen_raw = s3.get_object(Bucket=screen_bucket,
                       Key=f"{screen_prefix}/terminal.json")["Body"].read()
    screen_summary_raw = s3.get_object(Bucket=screen_bucket,
                       Key=f"{screen_prefix}/artifacts/summary.json")["Body"].read()
    screen_instance_id = validate_v122_gate(
        json.loads(screen_raw), screen_summary_raw,
    )
    for prerequisite in (index_terminal["instance_id"], screen_instance_id):
        state = ec2.describe_instances(InstanceIds=[prerequisite])[
            "Reservations"][0]["Instances"][0]["State"]["Name"]
        if state != "terminated":
            raise ValueError(f"V121 prerequisite Spot {prerequisite} has not terminated")
    screen_terminal_sha256 = hashlib.sha256(screen_raw).hexdigest()
    receipt = (json.dumps({"schema": SCHEMA, "source_commit": plan.source_commit,
                           "archive_sha256": plan.archive_sha256,
                           "index_terminal_sha256": index_terminal_sha256,
                           "screen_terminal_sha256": screen_terminal_sha256,
                           "query_sha256": QUERY_SHA256,
                           "truth_sha256": TRUTH_SHA256, "attempt": 1},
                          sort_keys=True, separators=(",", ":")) + "\n").encode()
    s3.put_object(Bucket=bucket, Key=f"{prefix}/reservation.json", Body=receipt,
                  IfNoneMatch="*", ContentType="application/json")
    instance_id = None
    for target in DEFAULT_TARGETS:
        try:
            launched = ec2.run_instances(**launch_spec(
                plan, target.availability_zone, target.subnet_id,
                index_terminal_sha256))
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
        raise RuntimeError("V121 Spot capacity unavailable; reservation retained")
    deadline = time.monotonic() + plan.wall_seconds + 1800
    try:
        while time.monotonic() < deadline:
            if not _missing(s3, bucket, f"{prefix}/terminal.json"):
                terminal = json.loads(s3.get_object(
                    Bucket=bucket, Key=f"{prefix}/terminal.json")["Body"].read())
                if (terminal.get("schema") != SCHEMA
                        or terminal.get("source_commit") != plan.source_commit
                        or terminal.get("index_terminal_sha256") != index_terminal_sha256
                        or terminal.get("instance_id") != instance_id):
                    raise ValueError("V121 terminal identity differs")
                return terminal
            state = ec2.describe_instances(InstanceIds=[instance_id])[
                "Reservations"][0]["Instances"][0]["State"]["Name"]
            if state in {"stopped", "shutting-down", "terminated"}:
                raise RuntimeError(f"V121 Spot {instance_id} ended without terminal")
            time.sleep(20)
        raise TimeoutError(f"V121 Spot {instance_id} terminal deadline exceeded")
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
