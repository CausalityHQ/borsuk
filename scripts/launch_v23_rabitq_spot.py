#!/usr/bin/env python3
"""Launch one immutable V23 RaBitQ development screen on EC2 Spot."""

from __future__ import annotations

import argparse
import base64
import dataclasses
import hashlib
import json
import re
import time
from collections.abc import Sequence
from typing import Any
from urllib.parse import urlsplit

PROFILE = "causality"
REGION = "eu-central-1"
EXPECTED_AWS_ACCOUNT = "453182569524"
AMI_ID = "ami-07bcecd13a160173f"
INSTANCE_TYPE = "c7g.16xlarge"
SUBNET_ID = "subnet-034528fbd6977848f"
SECURITY_GROUP_ID = "sg-0b1fd3e4fbde4af0d"
INSTANCE_PROFILE = "borsuk-bench-profile"
WALL_STOP_SECONDS = 21_600
LOWER_SHA1 = re.compile(r"[0-9a-f]{40}\Z")
LOWER_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
TOKEN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}\Z")


@dataclasses.dataclass(frozen=True)
class LaunchPlan:
    run_id: str
    source_commit: str
    source_archive_uri: str
    source_archive_sha256: str
    source_archive_bytes: int
    binary_uri: str
    binary_sha256: str
    binary_bytes: int
    manifest_uri: str
    manifest_sha256: str
    manifest_bytes: int
    output_prefix: str
    client_token: str


def _s3(uri: str) -> tuple[str, str]:
    parsed = urlsplit(uri)
    if parsed.scheme != "s3" or not parsed.netloc or not parsed.path.lstrip("/"):
        raise ValueError("S3 URI differs")
    return parsed.netloc, parsed.path.lstrip("/")


def build_launch_plan(
    *,
    run_id: str,
    source_commit: str,
    source_archive_uri: str,
    source_archive_sha256: str,
    source_archive_bytes: int,
    binary_uri: str,
    binary_sha256: str,
    binary_bytes: int,
    manifest_uri: str,
    manifest_sha256: str,
    manifest_bytes: int,
    output_prefix: str,
) -> LaunchPlan:
    if (
        TOKEN.fullmatch(run_id) is None
        or LOWER_SHA1.fullmatch(source_commit) is None
        or any(
            LOWER_SHA256.fullmatch(value) is None
            for value in (source_archive_sha256, binary_sha256, manifest_sha256)
        )
        or any(type(value) is not int or value <= 0 for value in (source_archive_bytes, binary_bytes, manifest_bytes))
        or not output_prefix.endswith("/")
    ):
        raise ValueError("RaBitQ launch authority differs")
    for uri in (source_archive_uri, binary_uri, manifest_uri, output_prefix):
        _s3(uri)
    authority = json.dumps(
        {
            "binary": [binary_uri, binary_sha256, binary_bytes],
            "manifest": [manifest_uri, manifest_sha256, manifest_bytes],
            "output_prefix": output_prefix,
            "run_id": run_id,
            "source": [source_commit, source_archive_uri, source_archive_sha256, source_archive_bytes],
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    client_token = "v23-rabitq-" + hashlib.sha256(authority).hexdigest()[:48]
    return LaunchPlan(
        run_id=run_id,
        source_commit=source_commit,
        source_archive_uri=source_archive_uri,
        source_archive_sha256=source_archive_sha256,
        source_archive_bytes=source_archive_bytes,
        binary_uri=binary_uri,
        binary_sha256=binary_sha256,
        binary_bytes=binary_bytes,
        manifest_uri=manifest_uri,
        manifest_sha256=manifest_sha256,
        manifest_bytes=manifest_bytes,
        output_prefix=output_prefix,
        client_token=client_token,
    )


def _worker_script(plan: LaunchPlan) -> str:
    bucket, prefix = _s3(plan.output_prefix)
    complete = f"s3://{bucket}/{prefix}COMPLETE.json"
    failed = f"s3://{bucket}/{prefix}FAILED.json"
    result = f"s3://{bucket}/{prefix}screen-result.json"
    return f"""#!/bin/bash
set -euo pipefail
root=/mnt/borsuk-v23-rabitq
mkdir -p "$root/src" "$root/work"
terminal=failed
finish() {{
  code=$?
  python3 - "$terminal" "$code" <<'PY' > "$root/terminal.json"
import json,sys
print(json.dumps({{"claim_eligible":False,"exit_code":int(sys.argv[2]),"status":sys.argv[1]}},sort_keys=True,separators=(",", ":")))
PY
  if [ "$terminal" = complete ]; then aws s3 cp "$root/terminal.json" {complete!r}; else aws s3 cp "$root/terminal.json" {failed!r}; fi
  exit "$code"
}}
trap finish EXIT
aws s3 cp {plan.source_archive_uri!r} "$root/source.tar.zst"
aws s3 cp {plan.binary_uri!r} "$root/v23-rabitq"
aws s3 cp {plan.manifest_uri!r} "$root/manifest.json"
test "$(stat -c %s "$root/source.tar.zst")" = {plan.source_archive_bytes}
test "$(sha256sum "$root/source.tar.zst" | cut -d' ' -f1)" = {plan.source_archive_sha256!r}
test "$(stat -c %s "$root/v23-rabitq")" = {plan.binary_bytes}
test "$(sha256sum "$root/v23-rabitq" | cut -d' ' -f1)" = {plan.binary_sha256!r}
test "$(stat -c %s "$root/manifest.json")" = {plan.manifest_bytes}
test "$(sha256sum "$root/manifest.json" | cut -d' ' -f1)" = {plan.manifest_sha256!r}
chmod 0555 "$root/v23-rabitq"
tar --zstd -xf "$root/source.tar.zst" -C "$root/src"
baseline_swap_kib=$(awk '/^SwapTotal:/ {{total=$2}} /^SwapFree:/ {{free=$2}} END {{print total-free}}' /proc/meminfo)
started=$(date +%s)
last_progress=$started
last_ticks=0
stop_reason=
setsid python3 "$root/src/scripts/run_v23_rabitq_falsifier.py" \
  --binary "$root/v23-rabitq" \
  --manifest-uri {plan.manifest_uri!r} \
  --manifest-sha256 {plan.manifest_sha256!r} \
  --manifest-bytes {plan.manifest_bytes} \
  --output "$root/work/screen-result.json" \
  --execute-development &
pid=$!
pgid=$(ps -o pgid= -p "$pid" | tr -d ' ')
while kill -0 "$pid" 2>/dev/null; do
  now=$(date +%s)
  rss_kib=$(ps -eo pgid=,rss= | awk -v target="$pgid" '$1==target {{sum+=$2}} END {{print sum+0}}')
  psi_full=$(awk '/^full / {{for(i=1;i<=NF;i++) if($i ~ /^avg10=/) {{split($i,a,"="); print a[2]}}}}' /proc/pressure/memory)
  swap_kib=$(awk '/^SwapTotal:/ {{total=$2}} /^SwapFree:/ {{free=$2}} END {{print total-free}}' /proc/meminfo)
  ticks=$(awk '{{print $14+$15}}' "/proc/$pid/stat" 2>/dev/null || printf '%s' "$last_ticks")
  if [ "$ticks" != "$last_ticks" ]; then last_ticks=$ticks; last_progress=$now; fi
  if [ $((now-started)) -ge 7200 ]; then stop_reason=wall-limit; fi
  if [ "$rss_kib" -ge 100663296 ]; then stop_reason=rss-limit; fi
  if awk -v value="$psi_full" 'BEGIN {{exit !(value >= 0.20)}}'; then stop_reason=psi-limit; fi
  if [ $((swap_kib-baseline_swap_kib)) -ge 1048576 ]; then stop_reason=swap-growth-limit; fi
  if [ $((now-last_progress)) -ge 300 ]; then stop_reason=progress-limit; fi
  if [ -n "$stop_reason" ]; then kill -TERM -- -$pgid 2>/dev/null || true; break; fi
  sleep 5
done
set +e
wait "$pid"
status=$?
set -e
if [ -n "$stop_reason" ]; then echo "$stop_reason" >&2; exit 70; fi
if [ "$status" -ne 0 ]; then exit "$status"; fi
aws s3 cp "$root/work/screen-result.json" {result!r}
terminal=complete
"""


def build_launch_spec(plan: LaunchPlan) -> dict[str, object]:
    return {
        "ImageId": AMI_ID,
        "InstanceType": INSTANCE_TYPE,
        "MinCount": 1,
        "MaxCount": 1,
        "ClientToken": plan.client_token,
        "SubnetId": SUBNET_ID,
        "SecurityGroupIds": [SECURITY_GROUP_ID],
        "IamInstanceProfile": {"Name": INSTANCE_PROFILE},
        "InstanceMarketOptions": {
            "MarketType": "spot",
            "SpotOptions": {
                "SpotInstanceType": "one-time",
                "InstanceInterruptionBehavior": "terminate",
            },
        },
        "BlockDeviceMappings": [
            {
                "DeviceName": "/dev/xvda",
                "Ebs": {
                    "DeleteOnTermination": True,
                    "Encrypted": True,
                    "VolumeSize": 500,
                    "VolumeType": "gp3",
                    "Iops": 6000,
                    "Throughput": 1000,
                },
            }
        ],
        "UserData": base64.b64encode(_worker_script(plan).encode()).decode(),
        "TagSpecifications": [
            {
                "ResourceType": "instance",
                "Tags": [
                    {"Key": "Name", "Value": plan.run_id},
                    {"Key": "borsuk-purpose", "Value": "v23-rabitq-development"},
                ],
            }
        ],
    }


def run_spot(plan: LaunchPlan, *, ec2_client: Any, s3_client: Any) -> str:
    response = ec2_client.run_instances(**build_launch_spec(plan))
    instance_id = response["Instances"][0]["InstanceId"]
    bucket, prefix = _s3(plan.output_prefix)
    complete_key = prefix + "COMPLETE.json"
    failed_key = prefix + "FAILED.json"
    started = time.monotonic()
    try:
        while time.monotonic() - started < WALL_STOP_SECONDS:
            for key in (failed_key, complete_key):
                try:
                    s3_client.head_object(Bucket=bucket, Key=key)
                except Exception:
                    continue
                if key == failed_key:
                    raise RuntimeError(f"RaBitQ worker failed at s3://{bucket}/{key}")
                return f"s3://{bucket}/{key}"
            time.sleep(15)
        raise TimeoutError("RaBitQ Spot worker exceeded wall stop")
    finally:
        ec2_client.terminate_instances(InstanceIds=[instance_id])


def _clients() -> tuple[Any, Any, Any]:
    import boto3

    session = boto3.Session(profile_name=PROFILE, region_name=REGION)
    return session.client("sts"), session.client("ec2"), session.client("s3")


def parse_args(arguments: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    for flag in (
        "run-id",
        "source-commit",
        "source-archive-uri",
        "source-archive-sha256",
        "binary-uri",
        "binary-sha256",
        "manifest-uri",
        "manifest-sha256",
        "output-prefix",
    ):
        parser.add_argument(f"--{flag}", required=True)
    parser.add_argument("--source-archive-bytes", type=int, required=True)
    parser.add_argument("--binary-bytes", type=int, required=True)
    parser.add_argument("--manifest-bytes", type=int, required=True)
    parser.add_argument("--execute-development", action="store_true")
    values = parser.parse_args(arguments)
    if not values.execute_development:
        parser.error("--execute-development is required")
    return values


def main(arguments: Sequence[str] | None = None) -> int:
    values = parse_args(arguments)
    plan = build_launch_plan(**{key.replace("-", "_"): value for key, value in vars(values).items() if key != "execute_development"})
    sts, ec2, s3 = _clients()
    account = sts.get_caller_identity()["Account"]
    if account != EXPECTED_AWS_ACCOUNT:
        raise RuntimeError("AWS account differs")
    print(run_spot(plan, ec2_client=ec2, s3_client=s3))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
