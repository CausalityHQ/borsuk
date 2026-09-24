#!/usr/bin/env python3
"""Launch one immutable V131 100k source replay on Causality Spot."""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import shlex
import subprocess
import tempfile
import time
from dataclasses import dataclass

from scripts.launch_native_geometric_layout_spot import DEFAULT_TARGETS

BUCKET = "borsuk-bench-453182569524-euc1"
V122_PREFIX = (
    f"s3://{BUCKET}/research/v122-deep-image-100k/"
    "afe07cb5a9ba8518263375595f589639fdf3f4f1/"
    "runs/v122-20260924T011355Z/a0001"
)
V122_TERMINAL_SHA256 = "5475dfdb8608b80c2dbdc3550721d9f5d106fb79c613cbc83cb40a7a29a4a89c"
SCHEMA = "borsuk-v131-source-replay-spot-v1"
TAG = "borsuk-v131-source-replay"
INPUTS = {
    "SOURCE": ("subset/source.parquet", 36_528_755, "da3ad1295d6031818b7ccb817529c6102e9f0ec93bb4ad0792e2b7ab3cd21e69"),
    "SQ8": ("built/sq8.bin", 10_800_000, "c20dcb8058d2409791c6c584d9f078491d4c239acbe7c19350be7757d533e8df"),
    "LOW": ("router/low.bin", 384, "3fd035c242b99b6835727faa49f0dc52c77979b8c900980c01fcf23412222c9e"),
    "STEP": ("router/step.bin", 384, "bd6a8adb231b89d18ad1edf72cff043512d663117955a826bd8ca11224d02038"),
    "QUERIES": ("queries.jsonl", 2_041_773, "dd95571cc7c333f331b8c4c0b55070b366b389508b0e195e0e56d5982177da99"),
    "TRUTH": ("truth.npy", 800_128, "9e29a3e07ee2fe199fb17d8ec19b62f158d0cebe86ed23db14edecfa877c1a7f"),
    "EVIDENCE": ("evidence.jsonl", 6_183_526, "deac3e5e9d15a54753a1543bed338e31b23daaf3f234f64afabde5e79bcc6ae6"),
    "BASELINE": ("summary.json", 951, "80342a3184b22d38a9e0cdad1da8be832f2740ca89b79d9aa47d07c89d7aacef"),
}


def _location(uri: str) -> tuple[str, str]:
    bucket, slash, key = uri.removeprefix("s3://").partition("/")
    if not bucket or not slash or not key:
        raise ValueError("V131 S3 location differs")
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
    instance_type: str = "c7i.8xlarge"
    wall_seconds: int = 5400


def validate_plan(plan: Plan) -> None:
    def hex_value(value: str, length: int) -> bool:
        return len(value) == length and all(char in "0123456789abcdef" for char in value)

    if (not hex_value(plan.source_commit, 40)
            or not hex_value(plan.archive_sha256, 64)
            or plan.archive_bytes <= 0
            or not plan.archive_uri.startswith("s3://")
            or not plan.output_prefix.startswith("s3://")
            or plan.source_commit not in plan.output_prefix
            or not plan.output_prefix.rsplit("/", 1)[-1].startswith("a")
            or plan.instance_type != "c7i.8xlarge"
            or plan.wall_seconds != 5400):
        raise ValueError("V131 immutable Spot plan differs")


def user_data(plan: Plan) -> str:
    validate_plan(plan)
    exports = [
        f"export V131_SOURCE_COMMIT={shlex.quote(plan.source_commit)}",
        f"export V131_ARCHIVE_SHA256={plan.archive_sha256}",
        f"export V131_OUTPUT_PREFIX={shlex.quote(plan.output_prefix.rstrip('/'))}",
        f"export V131_WALL_SECONDS={plan.wall_seconds}",
        f"export V131_V122_TERMINAL_SHA256={V122_TERMINAL_SHA256}",
    ]
    for role, (path, length, digest) in INPUTS.items():
        exports.extend((
            f"export V131_{role}_URI={shlex.quote(V122_PREFIX + '/artifacts/' + path)}",
            f"export V131_{role}_SHA256={digest}",
            f"export V131_{role}_BYTES={length}",
        ))
    return f"""#!/bin/bash
set -euo pipefail
root=/mnt/v131-source-replay
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
print(json.dumps({{"schema":"{SCHEMA}","source_commit":os.environ["V131_SOURCE_COMMIT"],
  "source_archive_sha256":os.environ["V131_ARCHIVE_SHA256"],
  "v122_terminal_sha256":os.environ["V131_V122_TERMINAL_SHA256"],
  "instance_id":os.environ["INSTANCE_ID"],"exit_code":int(os.environ["EXIT_CODE"]),
  "phase":"bootstrap","status":"failed","artifacts":{{}}}},
  sort_keys=True,separators=(',',':')))
PY
    aws s3 cp terminal.json "$V131_OUTPUT_PREFIX/terminal.json" --only-show-errors || true
    shutdown -h now || true
  fi
}}
trap bootstrap_failure EXIT
aws s3 cp {shlex.quote(plan.archive_uri)} source.tar.gz --only-show-errors
[ "$(stat -c%s source.tar.gz)" = "{plan.archive_bytes}" ]
printf '%s  source.tar.gz\\n' {shlex.quote(plan.archive_sha256)} | sha256sum -c -
mkdir repo && tar -xzf source.tar.gz -C repo
trap - EXIT
exec bash repo/scripts/run_v131_source_replay_remote.sh
"""


def launch_spec(plan: Plan, zone: str, subnet: str) -> dict:
    token = "v131-" + hashlib.sha256(
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
        "InstanceType": plan.instance_type,
        "MaxCount": 1,
        "MinCount": 1,
        "NetworkInterfaces": [{"AssociatePublicIpAddress": True, "DeviceIndex": 0,
                               "Groups": [plan.security_group_id], "SubnetId": subnet}],
        "TagSpecifications": [{"ResourceType": "instance", "Tags": [
            {"Key": "Name", "Value": TAG},
            {"Key": "BorsukAttempt", "Value": plan.output_prefix.rsplit("/", 1)[-1]},
        ]}],
        "UserData": base64.b64encode(user_data(plan).encode()).decode(),
    }


def reserve_attempt(bucket: str, key: str, receipt: bytes) -> None:
    with tempfile.NamedTemporaryFile() as body:
        body.write(receipt)
        body.flush()
        subprocess.run([
            "aws", "s3api", "put-object", "--profile", "causality",
            "--region", "eu-central-1", "--bucket", bucket, "--key", key,
            "--body", body.name, "--if-none-match", "*", "--output", "json",
        ], check=True, stdout=subprocess.DEVNULL)


def launch_and_monitor(plan: Plan) -> dict:
    import boto3

    validate_plan(plan)
    session = boto3.Session(profile_name="causality", region_name="eu-central-1")
    ec2, s3 = session.client("ec2"), session.client("s3")
    bucket, prefix = _location(plan.output_prefix)
    if not _missing(s3, bucket, f"{prefix}/reservation.json") or not _missing(
        s3, bucket, f"{prefix}/terminal.json"
    ):
        raise ValueError("V131 immutable attempt already registered")
    active = ec2.describe_instances(Filters=[
        {"Name": "tag:Name", "Values": [TAG]},
        {"Name": "instance-state-name", "Values": ["pending", "running", "stopping", "stopped"]},
    ])
    if any(item.get("Instances") for item in active["Reservations"]):
        raise ValueError("V131 Spot worker already active")
    old_bucket, old_key = _location(V122_PREFIX + "/terminal.json")
    raw = s3.get_object(Bucket=old_bucket, Key=old_key)["Body"].read()
    if hashlib.sha256(raw).hexdigest() != V122_TERMINAL_SHA256:
        raise ValueError("V122 sealed terminal SHA-256 differs")
    terminal = json.loads(raw)
    if (terminal.get("schema") != "borsuk-v122-deep-image-100k-spot-v1"
            or terminal.get("status") != "complete"
            or terminal.get("exit_code") != 0):
        raise ValueError("V122 sealed terminal state differs")
    old_instance = terminal.get("instance_id")
    if not old_instance:
        raise ValueError("V122 sealed worker identity is missing")
    try:
        retired = ec2.describe_instances(InstanceIds=[old_instance])["Reservations"]
    except Exception as error:
        code = str(getattr(error, "response", {}).get("Error", {}).get("Code", ""))
        if code != "InvalidInstanceID.NotFound":
            raise
        retired = []
    if any(instance["State"]["Name"] != "terminated"
           for reservation in retired for instance in reservation["Instances"]):
        raise ValueError("V122 sealed worker is not terminated")
    for role, (path, length, digest) in INPUTS.items():
        if terminal.get("artifacts", {}).get(path) != {"bytes": length, "sha256": digest}:
            raise ValueError(f"V122 {role} artifact identity differs")
        source_bucket, key = _location(V122_PREFIX + "/artifacts/" + path)
        if s3.head_object(Bucket=source_bucket, Key=key)["ContentLength"] != length:
            raise ValueError(f"V122 {role} object byte length differs")
    receipt = (json.dumps({
        "schema": SCHEMA,
        "source_commit": plan.source_commit,
        "source_archive_sha256": plan.archive_sha256,
        "v122_terminal_sha256": V122_TERMINAL_SHA256,
        "inputs": {role: {"path": path, "bytes": length, "sha256": digest}
                   for role, (path, length, digest) in INPUTS.items()},
        "decision": "production SQ8 replay parity, source Recall@100 and local scorer cost",
        "interruption": "discard partial cell; retry under a new immutable attempt",
    }, sort_keys=True, separators=(",", ":")) + "\n").encode()
    reserve_attempt(bucket, f"{prefix}/reservation.json", receipt)
    instance_id = None
    for target in DEFAULT_TARGETS:
        try:
            launched = ec2.run_instances(**launch_spec(
                plan, target.availability_zone, target.subnet_id))
        except Exception as error:
            if any(marker in str(error) for marker in (
                "InsufficientInstanceCapacity", "InsufficientFreeAddressesInSubnet",
                "MaxSpotInstanceCountExceeded", "SpotMaxPriceTooLow")):
                continue
            raise
        instance_id = launched["Instances"][0]["InstanceId"]
        break
    if instance_id is None:
        raise RuntimeError("V131 Spot capacity unavailable; reservation retained")
    print(json.dumps({"instance_id": instance_id,
                      "output_prefix": plan.output_prefix}), flush=True)
    deadline = time.monotonic() + plan.wall_seconds + 1800
    try:
        while time.monotonic() < deadline:
            if not _missing(s3, bucket, f"{prefix}/terminal.json"):
                terminal_raw = s3.get_object(
                    Bucket=bucket, Key=f"{prefix}/terminal.json")["Body"].read()
                result = json.loads(terminal_raw)
                if (result.get("schema") != SCHEMA
                        or result.get("source_commit") != plan.source_commit
                        or result.get("source_archive_sha256") != plan.archive_sha256
                        or result.get("instance_id") != instance_id):
                    raise ValueError("V131 terminal identity differs")
                print(json.dumps({"terminal": result,
                    "terminal_sha256": hashlib.sha256(terminal_raw).hexdigest()}), flush=True)
                return result
            state = ec2.describe_instances(InstanceIds=[instance_id])["Reservations"][0]["Instances"][0]["State"]["Name"]
            if state in {"stopped", "shutting-down", "terminated"}:
                raise RuntimeError(f"V131 Spot {instance_id} ended without terminal")
            time.sleep(20)
        raise TimeoutError(f"V131 Spot {instance_id} terminal deadline exceeded")
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
    terminal = launch_and_monitor(Plan(args.source_commit, args.archive_uri,
                                       args.archive_sha256, args.archive_bytes,
                                       args.output_prefix))
    if terminal.get("status") != "complete" or terminal.get("exit_code") != 0:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
