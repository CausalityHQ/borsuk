#!/usr/bin/env python3
"""Launch one claim-ineligible V24 reduced preflight on EC2 Spot."""

from __future__ import annotations

import argparse
import base64
import dataclasses
import hashlib
import json
import pathlib
import re
import shlex
import time
import urllib.parse
from collections.abc import Sequence
from typing import Any

EXPECTED_AWS_ACCOUNT = "453182569524"
PROFILE = "causality"
REGION = "eu-central-1"
AMI_ID = "ami-07bcecd13a160173f"
SECURITY_GROUP_ID = "sg-0b1fd3e4fbde4af0d"
INSTANCE_PROFILE = "borsuk-bench-profile"
INSTANCE_TYPE = "m7g.4xlarge"
WALL_SECONDS = 7_800
_LOWER_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_LOWER_GIT = re.compile(r"[0-9a-f]{40}\Z")
_CAPACITY_ERRORS = {
    "InsufficientInstanceCapacity",
    "SpotMaxPriceTooLow",
    "Unsupported",
}


@dataclasses.dataclass(frozen=True)
class SpotTarget:
    """One eligible public subnet in an independent availability zone."""

    availability_zone: str
    subnet_id: str


SPOT_TARGETS = (
    SpotTarget("eu-central-1c", "subnet-0a12dbed0ca6fac25"),
    SpotTarget("eu-central-1b", "subnet-00243d923761c047c"),
    SpotTarget("eu-central-1a", "subnet-034528fbd6977848f"),
)


@dataclasses.dataclass(frozen=True)
class ReducedSpotPlan:
    """All immutable identities for one reduced Spot attempt."""

    run_id: str
    source_commit: str
    source_archive_uri: str
    source_archive_sha256: str
    source_archive_bytes: int
    binary_uri: str
    binary_sha256: str
    binary_bytes: int
    output_prefix: str


def _s3(value: str, *, prefix: bool = False) -> tuple[str, str]:
    parsed = urllib.parse.urlsplit(value)
    if (
        parsed.scheme != "s3"
        or not parsed.netloc
        or not parsed.path.startswith("/")
        or parsed.path == "/"
        or parsed.query
        or parsed.fragment
        or ".." in pathlib.PurePosixPath(parsed.path).parts
    ):
        raise ValueError("V24 Spot S3 URI differs")
    key = parsed.path[1:]
    if prefix != key.endswith("/"):
        raise ValueError("V24 Spot S3 prefix differs")
    return parsed.netloc, key


def build_plan(**values: Any) -> ReducedSpotPlan:
    """Validate and freeze one reduced Spot plan."""

    plan = ReducedSpotPlan(**values)
    if (
        re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}", plan.run_id) is None
        or _LOWER_GIT.fullmatch(plan.source_commit) is None
        or _LOWER_SHA256.fullmatch(plan.source_archive_sha256) is None
        or _LOWER_SHA256.fullmatch(plan.binary_sha256) is None
        or plan.source_archive_bytes <= 0
        or plan.binary_bytes <= 0
    ):
        raise ValueError("V24 reduced Spot plan differs")
    _s3(plan.source_archive_uri)
    _s3(plan.binary_uri)
    _s3(plan.output_prefix, prefix=True)
    return plan


def canonical_json_bytes(value: object) -> bytes:
    """Return canonical newline JSON for terminal authority."""

    return (
        json.dumps(value, allow_nan=False, separators=(",", ":"), sort_keys=True).encode()
        + b"\n"
    )


def canonical_terminal_bytes(
    plan: ReducedSpotPlan,
    *,
    instance_id: str,
    status: str,
    preflight_receipt_sha256: str | None = None,
    preflight_receipt_bytes: int | None = None,
    worker_status: int | None = None,
) -> bytes:
    """Build one exact success or failure terminal for tests and validation."""

    if not instance_id.startswith("i-") or status not in {"complete", "failed"}:
        raise ValueError("V24 reduced Spot terminal identity differs")
    value: dict[str, object] = {
        "binary_sha256": plan.binary_sha256,
        "claim_eligible": False,
        "instance_id": instance_id,
        "run_id": plan.run_id,
        "schema": "borsuk-v24-reduced-spot-terminal-v1",
        "source_commit": plan.source_commit,
        "status": status,
        "worker_counts": [1, 4],
    }
    if status == "complete":
        if (
            preflight_receipt_sha256 is None
            or _LOWER_SHA256.fullmatch(preflight_receipt_sha256) is None
            or preflight_receipt_bytes is None
            or preflight_receipt_bytes <= 0
            or worker_status is not None
        ):
            raise ValueError("V24 reduced Spot complete terminal differs")
        value["preflight_receipt_bytes"] = preflight_receipt_bytes
        value["preflight_receipt_sha256"] = preflight_receipt_sha256
    else:
        if (
            type(worker_status) is not int  # noqa: E721
            or worker_status < 0
            or preflight_receipt_sha256 is not None
            or preflight_receipt_bytes is not None
        ):
            raise ValueError("V24 reduced Spot failure terminal differs")
        value["worker_status"] = worker_status
    return canonical_json_bytes(value)


def validate_terminal_bytes(raw: bytes, plan: ReducedSpotPlan, status: str) -> None:
    """Authenticate one terminal and all immutable bindings."""

    try:
        value = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("V24 reduced Spot terminal JSON differs") from error
    if raw != canonical_json_bytes(value) or type(value) is not dict:  # noqa: E721
        raise ValueError("V24 reduced Spot terminal encoding differs")
    instance_id = value.get("instance_id")
    if type(instance_id) is not str:  # noqa: E721
        raise ValueError("V24 reduced Spot terminal instance differs")
    expected = canonical_terminal_bytes(
        plan,
        instance_id=instance_id,
        status=status,
        preflight_receipt_sha256=value.get("preflight_receipt_sha256"),
        preflight_receipt_bytes=value.get("preflight_receipt_bytes"),
        worker_status=value.get("worker_status"),
    )
    if raw != expected:
        raise ValueError("V24 reduced Spot terminal authority differs")


def _worker_script(plan: ReducedSpotPlan) -> str:
    bucket, prefix = _s3(plan.output_prefix, prefix=True)
    quoted = {
        "archive_uri": shlex.quote(plan.source_archive_uri),
        "archive_sha": shlex.quote(plan.source_archive_sha256),
        "archive_bytes": str(plan.source_archive_bytes),
        "binary_uri": shlex.quote(plan.binary_uri),
        "binary_sha": shlex.quote(plan.binary_sha256),
        "binary_bytes": str(plan.binary_bytes),
        "bucket": shlex.quote(bucket),
        "prefix": shlex.quote(prefix),
        "commit": shlex.quote(plan.source_commit),
        "run_id": shlex.quote(plan.run_id),
    }
    return f"""#!/bin/bash
set -Eeuo pipefail
umask 077
shutdown --poweroff +150
root=/opt/borsuk-v24-reduced
workspace="$root/source"
preflight="$root/preflight"
archive="$root/source.tar.zst"
binary="$root/v24_witness_page_router"
mkdir -p "$root" "$workspace" "$preflight"
touch "$root/worker.log"
exec >>"$root/worker.log" 2>&1
output_bucket={quoted['bucket']}
output_prefix={quoted['prefix']}
run_id={quoted['run_id']}
source_commit={quoted['commit']}
binary_sha256={quoted['binary_sha']}
imds_token="$(curl -fsS -X PUT -H 'X-aws-ec2-metadata-token-ttl-seconds: 21600' http://169.254.169.254/latest/api/token)"
instance_id="$(curl -fsS -H "X-aws-ec2-metadata-token: $imds_token" http://169.254.169.254/latest/meta-data/instance-id)"
terminal=failed
log_published=0
put_once() {{
  aws s3api put-object --bucket "$output_bucket" --key "$output_prefix$2" \
    --body "$1" --if-none-match '*' --expected-bucket-owner {EXPECTED_AWS_ACCOUNT} \
    --checksum-algorithm SHA256 >/dev/null
}}
finish() {{
  status=$?
  if [[ "$log_published" -eq 0 ]]; then
    sync "$root/worker.log" || true
    put_once "$root/worker.log" worker.log || true
  fi
  if [[ "$terminal" != complete ]]; then
    python3 - "$root/FAILED.json" "$run_id" "$source_commit" "$binary_sha256" "$instance_id" "$status" <<'PY'
import json,sys
path,run_id,commit,binary_sha256,instance_id,status=sys.argv[1:]
value={{"binary_sha256":binary_sha256,"claim_eligible":False,"instance_id":instance_id,"run_id":run_id,"schema":"borsuk-v24-reduced-spot-terminal-v1","source_commit":commit,"status":"failed","worker_counts":[1,4],"worker_status":int(status)}}
open(path,"wb").write(json.dumps(value,sort_keys=True,separators=(",", ":")).encode()+b"\n")
PY
    put_once "$root/FAILED.json" FAILED.json || true
  fi
  shutdown -h now
}}
trap finish EXIT
dnf install -y python3 python3-pip tar zstd
python3 -m pip install uv==0.8.17
aws s3 cp {quoted['archive_uri']} "$archive" --only-show-errors
aws s3 cp {quoted['binary_uri']} "$binary" --only-show-errors
test "$(stat -c %s "$archive")" -eq {quoted['archive_bytes']}
test "$(stat -c %s "$binary")" -eq {quoted['binary_bytes']}
printf '%s  %s\n' {quoted['archive_sha']} "$archive" | sha256sum --check --status
printf '%s  %s\n' {quoted['binary_sha']} "$binary" | sha256sum --check --status
chmod 0555 "$binary"
tar --zstd -xf "$archive" -C "$workspace"
cd "$workspace"
test "$(cat .borsuk-source-commit)" = "$source_commit"
"$(command -v uv)" python install 3.12
"$(command -v uv)" venv --python 3.12 /opt/borsuk-v24-venv
"$(command -v uv)" pip install --python /opt/borsuk-v24-venv/bin/python --requirement scripts/requirements-format-bench.txt
export PYTHONPATH="$workspace" RAYON_NUM_THREADS=1
/opt/borsuk-v24-venv/bin/python -m scripts.run_v24_reduced_preflight \
  --binary "$binary" --binary-sha256 {quoted['binary_sha']} \
  --binary-bytes {quoted['binary_bytes']} --root "$preflight" \
  --source-commit "$source_commit" --execute-reduced-preflight >"$root/stdout.json"
cmp "$root/stdout.json" "$preflight/preflight-receipt.json"
receipt_sha256="$(sha256sum "$root/stdout.json" | awk '{{print $1}}')"
receipt_bytes="$(stat -c %s "$root/stdout.json")"
put_once "$root/stdout.json" preflight-receipt.json
sync "$root/worker.log"
put_once "$root/worker.log" worker.log
log_published=1
python3 - "$root/COMPLETE.json" "$run_id" "$source_commit" "$binary_sha256" "$instance_id" "$receipt_sha256" "$receipt_bytes" <<'PY'
import json,sys
path,run_id,commit,binary_sha256,instance_id,digest,length=sys.argv[1:]
value={{"binary_sha256":binary_sha256,"claim_eligible":False,"instance_id":instance_id,"preflight_receipt_bytes":int(length),"preflight_receipt_sha256":digest,"run_id":run_id,"schema":"borsuk-v24-reduced-spot-terminal-v1","source_commit":commit,"status":"complete","worker_counts":[1,4]}}
open(path,"wb").write(json.dumps(value,sort_keys=True,separators=(",", ":")).encode()+b"\n")
PY
put_once "$root/COMPLETE.json" COMPLETE.json
terminal=complete
"""


def build_launch_specs(plan: ReducedSpotPlan) -> list[dict[str, object]]:
    """Build one idempotent Spot request for each eligible zone."""

    build_plan(**dataclasses.asdict(plan))
    user_data = base64.b64encode(_worker_script(plan).encode()).decode()
    specs = []
    for ordinal, target in enumerate(SPOT_TARGETS):
        authority = json.dumps(
            {"plan": dataclasses.asdict(plan), "target": dataclasses.asdict(target)},
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
        specs.append(
            {
                "ImageId": AMI_ID,
                "InstanceType": INSTANCE_TYPE,
                "MinCount": 1,
                "MaxCount": 1,
                "ClientToken": "v24-reduced-"
                + hashlib.sha256(authority + bytes([ordinal])).hexdigest()[:48],
                "SubnetId": target.subnet_id,
                "SecurityGroupIds": [SECURITY_GROUP_ID],
                "IamInstanceProfile": {"Name": INSTANCE_PROFILE},
                "InstanceInitiatedShutdownBehavior": "terminate",
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
                            "Iops": 3000,
                            "Throughput": 250,
                        },
                    }
                ],
                "UserData": user_data,
                "TagSpecifications": [
                    {
                        "ResourceType": "instance",
                        "Tags": [
                            {"Key": "Name", "Value": plan.run_id},
                            {"Key": "borsuk-purpose", "Value": "v24-reduced-preflight"},
                        ],
                    }
                ],
            }
        )
    return specs


def _capacity_error(error: BaseException) -> bool:
    response = getattr(error, "response", None)
    return (
        isinstance(response, dict)
        and isinstance(response.get("Error"), dict)
        and response["Error"].get("Code") in _CAPACITY_ERRORS
    )


def run_spot(plan: ReducedSpotPlan, *, ec2_client: Any, s3_client: Any) -> str:
    """Launch once across eligible zones, observe one terminal, and terminate."""

    bucket, prefix = _s3(plan.output_prefix, prefix=True)

    def read_terminal(name: str) -> bytes | None:
        try:
            response = s3_client.get_object(
                Bucket=bucket,
                Key=prefix + name,
                ExpectedBucketOwner=EXPECTED_AWS_ACCOUNT,
                ChecksumMode="ENABLED",
            )
        except Exception as error:
            response = getattr(error, "response", None)
            code = (
                response.get("Error", {}).get("Code")
                if isinstance(response, dict)
                else None
            )
            if code in {"404", "NoSuchKey", "NotFound"}:
                return None
            raise
        raw = response["Body"].read()
        if response.get("ContentLength") != len(raw):
            raise ValueError("V24 reduced Spot terminal length differs")
        return raw

    for name in ("FAILED.json", "COMPLETE.json"):
        if read_terminal(name) is not None:
            raise ValueError("V24 reduced Spot terminal already exists")
    instance_id: str | None = None
    for spec in build_launch_specs(plan):
        try:
            response = ec2_client.run_instances(**spec)
        except Exception as error:
            if _capacity_error(error):
                continue
            raise
        instance_id = response["Instances"][0]["InstanceId"]
        break
    if instance_id is None:
        raise RuntimeError("V24 reduced Spot capacity is unavailable")
    started = time.monotonic()
    try:
        while time.monotonic() - started < WALL_SECONDS:
            for name in ("FAILED.json", "COMPLETE.json"):
                raw = read_terminal(name)
                if raw is None:
                    continue
                status = "failed" if name == "FAILED.json" else "complete"
                validate_terminal_bytes(raw, plan, status)
                if json.loads(raw)["instance_id"] != instance_id:
                    raise ValueError("V24 reduced Spot terminal instance differs")
                uri = f"s3://{bucket}/{prefix}{name}"
                if name == "FAILED.json":
                    raise RuntimeError(f"V24 reduced Spot worker failed at {uri}")
                return uri
            state = ec2_client.describe_instances(InstanceIds=[instance_id])["Reservations"][0][
                "Instances"
            ][0]["State"]["Name"]
            if state in {"shutting-down", "terminated", "stopping", "stopped"}:
                raise RuntimeError("V24 reduced Spot instance exited without terminal")
            time.sleep(15)
        raise TimeoutError("V24 reduced Spot worker exceeded wall stop")
    finally:
        ec2_client.terminate_instances(InstanceIds=[instance_id])


def parse_args(arguments: Sequence[str] | None = None) -> ReducedSpotPlan:
    """Parse one explicit immutable reduced Spot launch."""

    parser = argparse.ArgumentParser(allow_abbrev=False)
    for flag in (
        "run-id",
        "source-commit",
        "source-archive-uri",
        "source-archive-sha256",
        "binary-uri",
        "binary-sha256",
        "output-prefix",
    ):
        parser.add_argument(f"--{flag}", required=True)
    parser.add_argument("--source-archive-bytes", required=True, type=int)
    parser.add_argument("--binary-bytes", required=True, type=int)
    parser.add_argument("--execute-reduced-spot", action="store_true", required=True)
    values = parser.parse_args(arguments)
    return build_plan(
        **{
            key: value
            for key, value in vars(values).items()
            if key != "execute_reduced_spot"
        }
    )


def main(arguments: Sequence[str] | None = None) -> int:
    """Launch the reduced preflight through the causality profile."""

    import boto3

    plan = parse_args(arguments)
    session = boto3.Session(profile_name=PROFILE, region_name=REGION)
    sts = session.client("sts")
    if sts.get_caller_identity()["Account"] != EXPECTED_AWS_ACCOUNT:
        raise RuntimeError("AWS account differs")
    terminal = run_spot(
        plan,
        ec2_client=session.client("ec2"),
        s3_client=session.client("s3"),
    )
    print(terminal)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
