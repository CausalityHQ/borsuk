#!/usr/bin/env python3
"""Launch one immutable, paired untouched-validation gate on Causality Spot."""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import shlex
import time

from scripts.launch_native_geometric_layout_spot import DEFAULT_TARGETS
from scripts.launch_v112_precise_nominee_spot import INPUTS, ObjectIdentity
from scripts.launch_v114_1m_paired_spot import _location, _missing
from scripts.launch_v115_returned_replay_spot import (
    Plan as BasePlan, launch_spec as base_launch_spec,
)

BUCKET = "borsuk-bench-453182569524-euc1"
V114 = (f"s3://{BUCKET}/research/v114-1m-paired/"
        "87881c71e048d557c5c1285ffa0ac1d8c381f764/"
        "runs/v114-1m-dev-20260923T222600Z/a0001/artifacts")
V115 = (f"s3://{BUCKET}/research/v115-source-router-parity/"
        "8140fd86defff60ff35ef33be7596f2bda34f879/"
        "runs/v115-router-20260923T235000Z/a0001/artifacts/router")
V36 = (f"s3://{BUCKET}/research/v36-prefix-screen/"
       "runs/v36-prefix-screen-20260908T174540Z-31445a91/attempt-0000")
SCHEMA = "borsuk-v116-validation-paired-spot-v1"
ARTIFACTS = {
    "SQ8": INPUTS["SQ8"],
    "MIRROR_MANIFEST": ObjectIdentity(V114 + "/mirror/manifest.json",
        "48e01d4488d0c84b991baef521a02bca75ce096c0f9a1154f779c046732707f8", 29780),
    "SIDECAR": ObjectIdentity(V114 + "/mirror/blocks.sha256",
        "ec32ad7926aaf8dc1e80186d226d1e9dc5e97fdbd8aeccd14cdeefc1173938ca", 6093760),
    "ROUTER_MANIFEST": ObjectIdentity(V115 + "/manifest.json",
        "d558a77443d6a1a50b9b3d01e821f134b1cc0992aa8bcb7ef3dc9ed2941221fe", 932),
    "ROUTER_SUMMARIES": ObjectIdentity(V115 + "/summaries.bin",
        "cf264fa3026e97c6db732e920e607c07a67d5ade9b4d92c0575ab3fb550f2cf7", 24004608),
    "ROUTER_BOOKS": ObjectIdentity(V115 + "/books.bin",
        "1ca5aa29c32dd155f0309a4d9f5bd8294ccbe75f50d1a2ff213603fba08800ce", 786432),
    "ROUTER_CODES": ObjectIdentity(V115 + "/codes.bin",
        "599e359b02ddb85876234f64bac3fcf7bfcb759e121f6a1fbcb4fbd5dfc95460", 64000000),
    "ROUTER_LOW": ObjectIdentity(V115 + "/low.bin",
        "ce036f48f918312f694adbd12994646987eb0e144df20f64ffe583bed0d1f891", 3072),
    "ROUTER_STEP": ObjectIdentity(V115 + "/step.bin",
        "64d49c7413f163fb3446183629d743f69199156f208fa959523d1bcde27da69c", 3072),
    "VALIDATION_QUERY": ObjectIdentity(V36 + "/validation-query.parquet",
        "869e225181f7d01a972d8faa144eaff4838c7c1f8f7c0c55091487e234f0bd5e", 1558594),
    "VALIDATION_TRUTH": ObjectIdentity(V36 + "/validation-gt100.parquet",
        "bf0fb0c934c986d05282e3d1c63dc351c553976ea05bfcab0cd3f06d2979e871", 2045045),
}


def _q(value: object) -> str:
    return shlex.quote(str(value))


def user_data(plan: BasePlan) -> str:
    exports = [f"export V116_SOURCE_COMMIT={_q(plan.source_commit)}",
               f"export V116_OUTPUT_PREFIX={_q(plan.output_prefix.rstrip('/'))}",
               f"export V116_WALL_SECONDS={plan.wall_seconds}"]
    for role, item in ARTIFACTS.items():
        exports.extend((f"export V116_{role}_URI={_q(item.uri)}",
                        f"export V116_{role}_SHA256={_q(item.sha256)}",
                        f"export V116_{role}_BYTES={item.bytes}"))
    environment = "\n".join(exports)
    return f"""#!/bin/bash
set -euo pipefail
root=/mnt/v116-validation-paired
mkdir -p "$root" && cd "$root"
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
import json,os
print(json.dumps({{"schema":"{SCHEMA}","source_commit":os.environ["V116_SOURCE_COMMIT"],
  "instance_id":os.environ["INSTANCE_ID"],"exit_code":int(os.environ["EXIT_CODE"]),
  "phase":"bootstrap","status":"failed","elapsed_seconds":0,"artifacts":{{}}}},
  sort_keys=True,separators=(',',':')))
PY
    aws s3 cp terminal.json "$V116_OUTPUT_PREFIX/terminal.json" --only-show-errors || true
    shutdown -h now || true
  fi
}}
trap bootstrap_failure EXIT
aws s3 cp {_q(plan.archive_uri)} source.tar.gz --only-show-errors
[ "$(stat -c%s source.tar.gz)" = "{plan.archive_bytes}" ]
printf '%s  source.tar.gz\\n' {_q(plan.archive_sha256)} | sha256sum -c -
mkdir repo && tar -xzf source.tar.gz -C repo
trap - EXIT
exec bash repo/scripts/run_v116_validation_paired_remote.sh
"""


def launch_and_monitor(plan: BasePlan) -> dict[str, object]:
    import boto3
    if (len(plan.source_commit) != 40 or len(plan.archive_sha256) != 64
            or plan.archive_bytes <= 0 or plan.wall_seconds != 7200
            or plan.instance_type != "c7i.12xlarge"
            or plan.source_commit not in plan.output_prefix):
        raise ValueError("V116 frozen Spot plan differs")
    session = boto3.Session(profile_name="causality", region_name="eu-central-1")
    ec2, s3 = session.client("ec2"), session.client("s3")
    bucket, prefix = _location(plan.output_prefix)
    if not _missing(s3, bucket, f"{prefix}/reservation.json") or not _missing(
        s3, bucket, f"{prefix}/terminal.json"
    ):
        raise ValueError("V116 immutable attempt already registered")
    reservation = (json.dumps({"schema": SCHEMA, "source_commit": plan.source_commit,
                               "archive_sha256": plan.archive_sha256, "attempt": 1},
                              sort_keys=True, separators=(",", ":")) + "\n").encode()
    s3.put_object(Bucket=bucket, Key=f"{prefix}/reservation.json",
                  Body=reservation, ContentType="application/json", IfNoneMatch="*")
    instance_id = None
    for target in DEFAULT_TARGETS:
        spec = base_launch_spec(plan, target.availability_zone, target.subnet_id)
        spec["ClientToken"] = "v116-val-" + hashlib.sha256(
            f"{plan.source_commit}:{plan.output_prefix}:{target.availability_zone}".encode()
        ).hexdigest()[:44]
        spec["UserData"] = base64.b64encode(user_data(plan).encode()).decode()
        spec["TagSpecifications"][0]["Tags"][0]["Value"] = "borsuk-v116-validation-paired"
        try:
            launched = ec2.run_instances(**spec)
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
        raise RuntimeError("V116 Spot capacity unavailable; reservation retained")
    deadline = time.monotonic() + plan.wall_seconds + 1200
    try:
        while time.monotonic() < deadline:
            if not _missing(s3, bucket, f"{prefix}/terminal.json"):
                terminal = json.loads(s3.get_object(
                    Bucket=bucket, Key=f"{prefix}/terminal.json")["Body"].read())
                if (terminal.get("schema") != SCHEMA
                        or terminal.get("instance_id") != instance_id
                        or terminal.get("source_commit") != plan.source_commit):
                    raise ValueError("V116 terminal identity differs")
                return terminal
            state = ec2.describe_instances(InstanceIds=[instance_id])[
                "Reservations"][0]["Instances"][0]["State"]["Name"]
            if state in {"stopped", "shutting-down", "terminated"}:
                raise RuntimeError(f"V116 Spot {instance_id} ended without terminal")
            time.sleep(20)
        raise TimeoutError(f"V116 Spot {instance_id} terminal deadline exceeded")
    finally:
        ec2.terminate_instances(InstanceIds=[instance_id])
        ec2.get_waiter("instance_terminated").wait(InstanceIds=[instance_id],
            WaiterConfig={"Delay": 5, "MaxAttempts": 60})


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--archive-uri", required=True)
    parser.add_argument("--archive-sha256", required=True)
    parser.add_argument("--archive-bytes", type=int, required=True)
    parser.add_argument("--output-prefix", required=True)
    args = parser.parse_args()
    terminal = launch_and_monitor(BasePlan(
        args.source_commit, args.archive_uri, args.archive_sha256,
        args.archive_bytes, args.output_prefix,
    ))
    print(json.dumps(terminal, sort_keys=True, separators=(",", ":")))


if __name__ == "__main__":
    main()
