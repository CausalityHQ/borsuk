#!/usr/bin/env python3
"""Launch one sealed two-corpus source-tier precision cell on Causality Spot."""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import shlex
import time

from scripts.launch_native_geometric_layout_spot import DEFAULT_TARGETS
from scripts.launch_v114_1m_paired_spot import _location, _missing
from scripts.launch_v120_source_index_spot import SOURCE_BYTES, SOURCE_SHA256, SOURCE_URI
from scripts.launch_v121_deep_image_paired_spot import (
    BUCKET, INDEX_PREFIX, Plan, TRUTH_SHA256, TRUTH_URI,
    launch_spec as base_launch_spec, validate_plan,
)
from scripts.launch_v123_rerank_diagnostic_spot import (
    REQUESTS_SHA256, V121_PREFIX, V121_TERMINAL_SHA256,
    reserve_attempt, validate_retired_instance, validate_v121_terminal,
)

SCHEMA = "borsuk-v124-source-tier-precision-spot-v1"
INDEX_TERMINAL_SHA256 = "4f789d9b7762c96178873bf3bd3fe8ff9cdaf7311c25f7752f9f550fad563387"
V116_TERMINAL_SHA256 = "932ca2d0ed378acdbbbe4cd741c3dd451f848184dd19d190c0ee60b10db4655d"
V116_COMMIT = "5e9b35ad40ea023eab4407aa611d759e1893bb34"
V116_PREFIX = (f"s3://{BUCKET}/research/v116-validation-paired/{V116_COMMIT}/"
               "runs/v116-validation-20260923T235426Z/a0001")
V36 = (f"s3://{BUCKET}/research/v36-prefix-screen/"
       "runs/v36-prefix-screen-20260908T174540Z-31445a91/attempt-0000")

INPUTS = {
    "DEEP_SOURCE": (SOURCE_URI, SOURCE_SHA256, SOURCE_BYTES),
    "DEEP_LAYOUT": (INDEX_PREFIX + "/artifacts/built/layout.npy",
                    "419f9280d2e85f6fa275c115dd342c249ac31ec6af2d19a42f5b96c38ed247c1",
                    79_920_128),
    "DEEP_REQUESTS": (V121_PREFIX + "/artifacts/requests.jsonl",
                      REQUESTS_SHA256, 6_705_393),
    "DEEP_TRUTH": (TRUTH_URI, TRUTH_SHA256, 4_003_585),
    "RELAION_SOURCE": (V36 + "/source.parquet",
                       "2796b579f37afe99ca4aff57e282335a6a79ad30596645957d26326a0560cf86",
                       1_458_450_077),
    "RELAION_LAYOUT": (
        f"s3://{BUCKET}/research/v63-algorithm-first/"
        "layout-oracle-e2f6c2bad99c720b/a0001/artifacts/kmeans_8192-order.npy",
        "32cba9690cd9d0ed3809763e5a0fa3574b09a207da26e93404651acaa1a66a0b",
        4_000_128),
    "RELAION_REQUESTS": (V116_PREFIX + "/artifacts/requests.jsonl",
                         "c250ef3c871af55ee1ea91e61214a93903b3d575ef458926b1f8c5ad0547b2c9",
                         18_726_909),
    "RELAION_TRUTH": (V36 + "/validation-gt100.parquet",
                      "bf0fb0c934c986d05282e3d1c63dc351c553976ea05bfcab0cd3f06d2979e871",
                      2_045_045),
}


def validate_v116_terminal(terminal: dict) -> str:
    if (terminal.get("schema") != "borsuk-v116-validation-paired-spot-v1"
            or terminal.get("source_commit") != V116_COMMIT
            or terminal.get("status") != "complete"
            or terminal.get("phase") != "complete"
            or terminal.get("exit_code") != 0
            or not terminal.get("instance_id")
            or terminal.get("artifacts", {}).get("requests.jsonl", {}).get(
                "sha256") != INPUTS["RELAION_REQUESTS"][1]):
        raise ValueError("V124 V116 prerequisite differs")
    return terminal["instance_id"]


def user_data(plan: Plan) -> str:
    validate_plan(plan)
    exports = [
        f"export V124_SOURCE_COMMIT={shlex.quote(plan.source_commit)}",
        f"export V124_OUTPUT_PREFIX={shlex.quote(plan.output_prefix.rstrip('/'))}",
        f"export V124_WALL_SECONDS={plan.wall_seconds}",
        f"export V124_V121_TERMINAL_SHA256={V121_TERMINAL_SHA256}",
        f"export V124_V116_TERMINAL_SHA256={V116_TERMINAL_SHA256}",
    ]
    for role, (uri, digest, _length) in INPUTS.items():
        exports.append(f"export V124_{role}_URI={shlex.quote(uri)}")
        exports.append(f"export V124_{role}_SHA256={digest}")
    return f"""#!/bin/bash
set -euo pipefail
root=/mnt/v124-precision
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
print(json.dumps({{"schema":"{SCHEMA}","source_commit":os.environ["V124_SOURCE_COMMIT"],
  "v121_terminal_sha256":os.environ["V124_V121_TERMINAL_SHA256"],
  "v116_terminal_sha256":os.environ["V124_V116_TERMINAL_SHA256"],
  "instance_id":os.environ["INSTANCE_ID"],"exit_code":int(os.environ["EXIT_CODE"]),
  "phase":"bootstrap","status":"failed","elapsed_seconds":0,"artifacts":{{}}}},
  sort_keys=True,separators=(',',':')))
PY
    aws s3 cp terminal.json "$V124_OUTPUT_PREFIX/terminal.json" --only-show-errors || true
    shutdown -h now || true
  fi
}}
trap bootstrap_failure EXIT
aws s3 cp {shlex.quote(plan.archive_uri)} source.tar.gz --only-show-errors
[ "$(stat -c%s source.tar.gz)" = "{plan.archive_bytes}" ]
printf '%s  source.tar.gz\\n' {shlex.quote(plan.archive_sha256)} | sha256sum -c -
mkdir repo && tar -xzf source.tar.gz -C repo
trap - EXIT
exec bash repo/scripts/run_v124_source_tier_precision_remote.sh
"""


def launch_spec(plan: Plan, zone: str, subnet: str) -> dict:
    spec = base_launch_spec(plan, zone, subnet, INDEX_TERMINAL_SHA256)
    spec["ClientToken"] = "v124-" + hashlib.sha256(
        f"{plan.source_commit}:{plan.output_prefix}:{zone}".encode()
    ).hexdigest()[:48]
    spec["TagSpecifications"][0]["Tags"][0]["Value"] = "borsuk-v124-source-tier-precision"
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
        raise ValueError("V124 immutable attempt already registered")
    active = ec2.describe_instances(Filters=[
        {"Name": "tag:Name", "Values": ["borsuk-v124-source-tier-precision"]},
        {"Name": "instance-state-name", "Values": ["pending", "running",
                                               "stopping", "stopped"]},
    ])
    if any(reservation.get("Instances") for reservation in active["Reservations"]):
        raise ValueError("V124 Spot worker is already active")
    prerequisite_ids = []
    for uri, expected_sha, validate in (
        (V121_PREFIX + "/terminal.json", V121_TERMINAL_SHA256,
         validate_v121_terminal),
        (V116_PREFIX + "/terminal.json", V116_TERMINAL_SHA256,
         validate_v116_terminal),
    ):
        source_bucket, key = _location(uri)
        raw = s3.get_object(Bucket=source_bucket, Key=key)["Body"].read()
        if hashlib.sha256(raw).hexdigest() != expected_sha:
            raise ValueError("V124 prerequisite terminal SHA differs")
        prerequisite_ids.append(validate(json.loads(raw)))
    for instance_id in prerequisite_ids:
        validate_retired_instance(ec2.describe_instances(InstanceIds=[instance_id]),
                                  instance_id)
    for uri, _digest, length in INPUTS.values():
        source_bucket, key = _location(uri)
        if s3.head_object(Bucket=source_bucket, Key=key)["ContentLength"] != length:
            raise ValueError("V124 input byte length differs")
    receipt = (json.dumps({"schema": SCHEMA, "source_commit": plan.source_commit,
        "archive_sha256": plan.archive_sha256,
        "v121_terminal_sha256": V121_TERMINAL_SHA256,
        "v116_terminal_sha256": V116_TERMINAL_SHA256,
        "inputs": {role: {"uri": item[0], "sha256": item[1], "bytes": item[2]}
                   for role, item in INPUTS.items()},
        "attempt": plan.output_prefix.rsplit("/", 1)[-1]},
        sort_keys=True, separators=(",", ":")) + "\n").encode()
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
        raise RuntimeError("V124 Spot capacity unavailable; reservation retained")
    deadline = time.monotonic() + plan.wall_seconds + 1800
    try:
        while time.monotonic() < deadline:
            if not _missing(s3, bucket, f"{prefix}/terminal.json"):
                terminal = json.loads(s3.get_object(Bucket=bucket,
                    Key=f"{prefix}/terminal.json")["Body"].read())
                if (terminal.get("schema") != SCHEMA
                        or terminal.get("source_commit") != plan.source_commit
                        or terminal.get("instance_id") != instance_id):
                    raise ValueError("V124 terminal identity differs")
                return terminal
            state = ec2.describe_instances(InstanceIds=[instance_id])[
                "Reservations"][0]["Instances"][0]["State"]["Name"]
            if state in {"stopped", "shutting-down", "terminated"}:
                raise RuntimeError(f"V124 Spot {instance_id} ended without terminal")
            time.sleep(20)
        raise TimeoutError(f"V124 Spot {instance_id} terminal deadline exceeded")
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
