#!/usr/bin/env python3
"""Launch one immutable FP16/source-rerank postmortem on Causality Spot."""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import shlex
import subprocess
import tempfile
import time

from scripts.launch_native_geometric_layout_spot import DEFAULT_TARGETS
from scripts.launch_v114_1m_paired_spot import _location, _missing
from scripts.launch_v120_source_index_spot import (
    SOURCE_BYTES, SOURCE_SHA256, SOURCE_URI,
)
from scripts.launch_v121_deep_image_paired_spot import (
    BUCKET, INDEX_PREFIX, Plan, TRUTH_SHA256, TRUTH_URI,
    launch_spec as v121_launch_spec, validate_plan,
)
from scripts.v121_download_index import validate_terminal as validate_index_terminal

SCHEMA = "borsuk-v123-rerank-postmortem-spot-v1"
INDEX_TERMINAL_SHA256 = "4f789d9b7762c96178873bf3bd3fe8ff9cdaf7311c25f7752f9f550fad563387"
V121_TERMINAL_SHA256 = "01cf70dfc77c285636943b334c21efb3bccbfe33dcfeffee153e4a7cdb484113"
V121_COMMIT = "79a51449cbeb169851d9c02c173488d0f073d519"
V121_PREFIX = (f"s3://{BUCKET}/research/v121-deep-image-paired/{V121_COMMIT}/"
               "runs/v121-20260924T014535Z/a0003")
REQUESTS_SHA256 = "31626934383e80ae683bbb21298d019d5b7b985dbc80f8542d4fc363db58818f"
REPLAY_SHA256 = "ddc9af991bdc6d3ef77d34a156994daa43aeb78f67f18de2cd0dc5ebb93abe91"


def _q(value: object) -> str:
    return shlex.quote(str(value))


def validate_v121_terminal(terminal: dict) -> str:
    artifacts = terminal.get("artifacts", {})
    if (terminal.get("schema") != "borsuk-v121-deep-image-paired-spot-v1"
            or terminal.get("source_commit") != V121_COMMIT
            or terminal.get("index_terminal_sha256") != INDEX_TERMINAL_SHA256
            or terminal.get("status") != "complete"
            or terminal.get("phase") != "complete"
            or terminal.get("exit_code") != 0
            or not terminal.get("instance_id")
            or artifacts.get("requests.jsonl", {}).get("sha256") != REQUESTS_SHA256
            or artifacts.get("rust-replay.jsonl", {}).get("sha256") != REPLAY_SHA256):
        raise ValueError("V123 V121 prerequisite differs")
    return terminal["instance_id"]


def validate_retired_instance(response: dict, instance_id: str) -> None:
    """EC2 can omit an aged-out terminated instance from DescribeInstances."""
    instances = [instance for reservation in response.get("Reservations", [])
                 for instance in reservation.get("Instances", [])]
    if len(instances) > 1 or any(
        instance.get("InstanceId") != instance_id
        or instance.get("State", {}).get("Name") != "terminated"
        for instance in instances
    ):
        raise ValueError(f"V123 prerequisite Spot {instance_id} has not terminated")


def reserve_attempt(bucket: str, key: str, receipt: bytes) -> None:
    """Use the CLI's conditional put; older installed botocore omits this field."""
    with tempfile.NamedTemporaryFile() as body:
        body.write(receipt)
        body.flush()
        subprocess.run([
            "aws", "s3api", "put-object", "--region", "eu-central-1",
            "--bucket", bucket, "--key", key, "--body", body.name,
            "--if-none-match", "*", "--content-type", "application/json",
            "--output", "json",
        ], check=True, stdout=subprocess.DEVNULL)


def user_data(plan: Plan) -> str:
    validate_plan(plan)
    exports = "\n".join((
        f"export V123_SOURCE_COMMIT={_q(plan.source_commit)}",
        f"export V123_OUTPUT_PREFIX={_q(plan.output_prefix.rstrip('/'))}",
        f"export V123_WALL_SECONDS={plan.wall_seconds}",
        f"export V123_INDEX_PREFIX={_q(INDEX_PREFIX)}",
        f"export V123_INDEX_TERMINAL_SHA256={INDEX_TERMINAL_SHA256}",
        f"export V123_V121_TERMINAL_SHA256={V121_TERMINAL_SHA256}",
        f"export V123_REQUESTS_URI={_q(V121_PREFIX + '/artifacts/requests.jsonl')}",
        f"export V123_REQUESTS_SHA256={REQUESTS_SHA256}",
        f"export V123_REPLAY_URI={_q(V121_PREFIX + '/artifacts/rust-replay.jsonl')}",
        f"export V123_REPLAY_SHA256={REPLAY_SHA256}",
        f"export V123_TRUTH_URI={_q(TRUTH_URI)}",
        f"export V123_TRUTH_SHA256={TRUTH_SHA256}",
        f"export V123_SOURCE_URI={_q(SOURCE_URI)}",
        f"export V123_SOURCE_SHA256={SOURCE_SHA256}",
        f"export V123_SOURCE_BYTES={SOURCE_BYTES}",
    ))
    return f"""#!/bin/bash
set -euo pipefail
root=/mnt/v123-rerank
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
print(json.dumps({{"schema":"{SCHEMA}","source_commit":os.environ["V123_SOURCE_COMMIT"],
  "v121_terminal_sha256":os.environ["V123_V121_TERMINAL_SHA256"],
  "instance_id":os.environ["INSTANCE_ID"],"exit_code":int(os.environ["EXIT_CODE"]),
  "phase":"bootstrap","status":"failed","elapsed_seconds":0,"artifacts":{{}}}},
  sort_keys=True,separators=(',',':')))
PY
    aws s3 cp terminal.json "$V123_OUTPUT_PREFIX/terminal.json" --only-show-errors || true
    shutdown -h now || true
  fi
}}
trap bootstrap_failure EXIT
aws s3 cp {_q(plan.archive_uri)} source.tar.gz --only-show-errors
[ "$(stat -c%s source.tar.gz)" = "{plan.archive_bytes}" ]
printf '%s  source.tar.gz\\n' {_q(plan.archive_sha256)} | sha256sum -c -
mkdir repo && tar -xzf source.tar.gz -C repo
trap - EXIT
exec bash repo/scripts/run_v123_rerank_diagnostic_remote.sh
"""


def launch_spec(plan: Plan, zone: str, subnet: str) -> dict:
    spec = v121_launch_spec(plan, zone, subnet, INDEX_TERMINAL_SHA256)
    spec["ClientToken"] = "v123-" + hashlib.sha256(
        f"{plan.source_commit}:{plan.output_prefix}:{zone}".encode()
    ).hexdigest()[:48]
    spec["TagSpecifications"][0]["Tags"][0]["Value"] = "borsuk-v123-rerank-diagnostic"
    spec["UserData"] = base64.b64encode(user_data(plan).encode()).decode()
    return spec


def launch_and_monitor(plan: Plan) -> dict:
    import boto3
    validate_plan(plan)
    session = boto3.Session(profile_name="causality", region_name="eu-central-1")
    ec2, s3 = session.client("ec2"), session.client("s3")
    bucket, prefix = _location(plan.output_prefix)
    if not _missing(s3, bucket, f"{prefix}/reservation.json") or not _missing(
        s3, bucket, f"{prefix}/terminal.json"
    ):
        raise ValueError("V123 immutable attempt already registered")
    index_bucket, index_prefix = _location(INDEX_PREFIX)
    index_raw = s3.get_object(Bucket=index_bucket,
        Key=f"{index_prefix}/terminal.json")["Body"].read()
    if hashlib.sha256(index_raw).hexdigest() != INDEX_TERMINAL_SHA256:
        raise ValueError("V123 V120 terminal SHA-256 differs")
    index_terminal = json.loads(index_raw)
    validate_index_terminal(index_terminal)
    v121_bucket, v121_prefix = _location(V121_PREFIX)
    v121_raw = s3.get_object(Bucket=v121_bucket,
        Key=f"{v121_prefix}/terminal.json")["Body"].read()
    if hashlib.sha256(v121_raw).hexdigest() != V121_TERMINAL_SHA256:
        raise ValueError("V123 V121 terminal SHA-256 differs")
    v121_instance_id = validate_v121_terminal(json.loads(v121_raw))
    for prerequisite in (index_terminal["instance_id"], v121_instance_id):
        validate_retired_instance(ec2.describe_instances(InstanceIds=[prerequisite]),
                                  prerequisite)
    receipt = (json.dumps({"schema": SCHEMA, "source_commit": plan.source_commit,
        "archive_sha256": plan.archive_sha256,
        "index_terminal_sha256": INDEX_TERMINAL_SHA256,
        "v121_terminal_sha256": V121_TERMINAL_SHA256,
        "source_sha256": SOURCE_SHA256, "truth_sha256": TRUTH_SHA256,
        "attempt": plan.output_prefix.rsplit("/", 1)[-1]},
        sort_keys=True,separators=(",", ":")) + "\n").encode()
    reserve_attempt(bucket, f"{prefix}/reservation.json", receipt)
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
        raise RuntimeError("V123 Spot capacity unavailable; reservation retained")
    deadline = time.monotonic() + plan.wall_seconds + 1800
    try:
        while time.monotonic() < deadline:
            if not _missing(s3, bucket, f"{prefix}/terminal.json"):
                terminal = json.loads(s3.get_object(Bucket=bucket,
                    Key=f"{prefix}/terminal.json")["Body"].read())
                if (terminal.get("schema") != SCHEMA
                        or terminal.get("source_commit") != plan.source_commit
                        or terminal.get("v121_terminal_sha256") != V121_TERMINAL_SHA256
                        or terminal.get("instance_id") != instance_id):
                    raise ValueError("V123 terminal identity differs")
                return terminal
            state = ec2.describe_instances(InstanceIds=[instance_id])[
                "Reservations"][0]["Instances"][0]["State"]["Name"]
            if state in {"stopped", "shutting-down", "terminated"}:
                raise RuntimeError(f"V123 Spot {instance_id} ended without terminal")
            time.sleep(20)
        raise TimeoutError(f"V123 Spot {instance_id} terminal deadline exceeded")
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
    if terminal.get("status") != "complete" or terminal.get("exit_code") != 0:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
