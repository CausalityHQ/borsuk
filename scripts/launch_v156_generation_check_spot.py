#!/usr/bin/env python3
"""Run one authenticated V156 loader test cell on Causality Spot."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import io
import json
import subprocess
import tarfile
import tempfile
import time
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

BUCKET = "borsuk-bench-453182569524-euc1"
REGION = "eu-central-1"
TAG = "borsuk-v156-generation-check"
IMAGE = "ami-06121aa3085b6f918"
SUBNET = "subnet-0a12dbed0ca6fac25"
SECURITY_GROUP = "sg-0b1fd3e4fbde4af0d"
PROFILE_ARN = "arn:aws:iam::453182569524:instance-profile/borsuk-bench-profile"
SCHEMA = "borsuk-v156-generation-check-spot-v1"
WALL_SECONDS = 3600


def missing(s3: object, key: str) -> bool:
    try:
        s3.head_object(Bucket=BUCKET, Key=key)
    except ClientError as error:
        if str(error.response.get("Error", {}).get("Code", "")) in {
            "404", "NoSuchKey", "NotFound"
        }:
            return True
        raise
    return False


def put_if_absent(key: str, body: bytes) -> None:
    with tempfile.NamedTemporaryFile() as temporary:
        temporary.write(body)
        temporary.flush()
        subprocess.run([
            "aws", "s3api", "put-object", "--profile", "causality",
            "--region", REGION, "--bucket", BUCKET, "--key", key,
            "--body", temporary.name, "--if-none-match", "*",
        ], check=True, stdout=subprocess.DEVNULL)


def source_archive(commit: str) -> bytes:
    raw = subprocess.run(
        ["git", "archive", "--format=tar", commit], check=True,
        capture_output=True,
    ).stdout
    buffer = io.BytesIO()
    with gzip.GzipFile(filename="", mode="wb", fileobj=buffer, mtime=0) as zipper:
        zipper.write(raw)
    archive = buffer.getvalue()
    with tarfile.open(fileobj=io.BytesIO(archive), mode="r:gz") as tar:
        if "crates/borsuk/src/graph_serving_generation.rs" not in tar.getnames():
            raise ValueError("source archive lacks loader")
    return archive


def user_data(commit: str, archive_sha: str, archive_key: str, prefix: str) -> str:
    return f'''#!/bin/bash
set -euo pipefail
systemd-run --unit=v156-hard-stop --on-active={WALL_SECONDS}s /usr/sbin/shutdown -h now
root=/mnt/v156-generation-check
mkdir -p "$root" && cd "$root"
phase=bootstrap
finish() {{
  code=$?
  trap - EXIT TERM
  set +e
  cd "$root"
  token=$(curl -fsS -X PUT -H 'X-aws-ec2-metadata-token-ttl-seconds: 60' http://169.254.169.254/latest/api/token)
  instance_id=$(curl -fsS -H "X-aws-ec2-metadata-token: $token" http://169.254.169.254/latest/meta-data/instance-id)
  for name in install.log test.log; do
    if [ -f "$name" ]; then aws s3 cp "$name" "s3://{BUCKET}/{prefix}/artifacts/$name" --only-show-errors || code=96; fi
  done
  INSTANCE_ID="$instance_id" EXIT_CODE="$code" PHASE="$phase" python3 - <<'PY' >terminal.json
import json,os
print(json.dumps({{"schema":"{SCHEMA}","source_commit":"{commit}",
  "source_archive_sha256":"{archive_sha}","instance_id":os.environ["INSTANCE_ID"],
  "exit_code":int(os.environ["EXIT_CODE"]),"phase":os.environ["PHASE"],
  "status":"complete" if int(os.environ["EXIT_CODE"])==0 and os.environ["PHASE"]=="complete" else "failed"}},
  sort_keys=True,separators=(',',':')))
PY
  aws s3 cp terminal.json "s3://{BUCKET}/{prefix}/terminal.json" --only-show-errors
  shutdown -h now || true
  exit "$code"
}}
trap finish EXIT
trap 'exit 97' TERM
aws s3 cp "s3://{BUCKET}/{archive_key}" source.tar.gz --only-show-errors
printf '%s  source.tar.gz\\n' '{archive_sha}' | sha256sum -c -
mkdir repo && tar -xzf source.tar.gz -C repo
phase=install
dnf install -y -q gcc gcc-c++ cmake perl tar gzip >install.log 2>&1
export RUSTUP_HOME="$root/.rustup" CARGO_HOME="$root/.cargo" CARGO_TARGET_DIR="$root/target"
curl -fsSL https://sh.rustup.rs | sh -s -- -y --profile minimal --default-toolchain 1.98.0 >>install.log 2>&1
phase=test
cd repo
"$CARGO_HOME/bin/cargo" test --locked -p borsuk --lib graph_serving_generation::tests -j 4 >../test.log 2>&1
"$CARGO_HOME/bin/cargo" test --locked -p borsuk --lib native_source_tier::tests -j 4 >>../test.log 2>&1
cd ..
phase=complete
'''


def main() -> None:
    arguments = argparse.ArgumentParser()
    arguments.add_argument("--attempt", default="a0001")
    attempt = arguments.parse_args().attempt
    if len(attempt) != 5 or not attempt.startswith("a") or not attempt[1:].isdigit():
        raise ValueError("attempt must be aNNNN")
    commit = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    if subprocess.check_output(["git", "status", "--porcelain"], text=True).strip():
        raise ValueError("worktree must be clean before source archive")
    archive = source_archive(commit)
    archive_sha = hashlib.sha256(archive).hexdigest()
    archive_key = f"research/v156-generation-check/{commit}/sources/{archive_sha}.tar.gz"
    prefix = f"research/v156-generation-check/{commit}/runs/{attempt}"
    session = boto3.Session(profile_name="causality", region_name=REGION)
    ec2, s3 = session.client("ec2"), session.client("s3")
    if not missing(s3, prefix + "/terminal.json") or not missing(s3, prefix + "/reservation.json"):
        raise ValueError("attempt already registered")
    active = ec2.describe_instances(Filters=[
        {"Name": "tag:Name", "Values": [TAG]},
        {"Name": "instance-state-name", "Values": ["pending", "running", "stopping", "stopped"]},
    ])
    if any(reservation.get("Instances") for reservation in active["Reservations"]):
        raise ValueError("V156 worker already active")
    if missing(s3, archive_key):
        put_if_absent(archive_key, archive)
    elif s3.head_object(Bucket=BUCKET, Key=archive_key)["ContentLength"] != len(archive):
        raise ValueError("existing source archive length differs")
    reservation = {
        "schema": SCHEMA, "source_commit": commit,
        "source_archive_sha256": archive_sha, "source_archive_bytes": len(archive),
        "interruption_policy": "discard and restart the complete test cell",
        "output_prefix": f"s3://{BUCKET}/{prefix}",
    }
    put_if_absent(prefix + "/reservation.json",
                  json.dumps(reservation, sort_keys=True).encode())
    receipt = ec2.run_instances(
        ClientToken="v156-" + hashlib.sha256(prefix.encode()).hexdigest()[:48],
        ImageId=IMAGE, InstanceType="c7i.4xlarge", MinCount=1, MaxCount=1,
        IamInstanceProfile={"Arn": PROFILE_ARN},
        NetworkInterfaces=[{"AssociatePublicIpAddress": True, "DeviceIndex": 0,
                            "Groups": [SECURITY_GROUP], "SubnetId": SUBNET}],
        InstanceMarketOptions={"MarketType": "spot", "SpotOptions": {
            "InstanceInterruptionBehavior": "terminate", "SpotInstanceType": "one-time"}},
        InstanceInitiatedShutdownBehavior="terminate",
        BlockDeviceMappings=[{"DeviceName": "/dev/xvda", "Ebs": {
            "DeleteOnTermination": True, "Encrypted": True,
            "VolumeSize": 120, "VolumeType": "gp3"}}],
        TagSpecifications=[{"ResourceType": "instance", "Tags": [
            {"Key": "Name", "Value": TAG}, {"Key": "BorsukAttempt", "Value": attempt}]}],
        UserData=user_data(commit, archive_sha, archive_key, prefix),
    )
    instance_id = receipt["Instances"][0]["InstanceId"]
    print(json.dumps({"instance_id": instance_id, "output_prefix": prefix,
                      "source_commit": commit}), flush=True)
    deadline = time.monotonic() + WALL_SECONDS + 600
    while time.monotonic() < deadline:
        if not missing(s3, prefix + "/terminal.json"):
            raw = s3.get_object(Bucket=BUCKET, Key=prefix + "/terminal.json")["Body"].read()
            terminal = json.loads(raw)
            if (terminal.get("schema") != SCHEMA or terminal.get("instance_id") != instance_id
                    or terminal.get("source_commit") != commit
                    or terminal.get("source_archive_sha256") != archive_sha):
                raise ValueError("terminal identity differs")
            ec2.terminate_instances(InstanceIds=[instance_id])
            ec2.get_waiter("instance_terminated").wait(InstanceIds=[instance_id])
            terminal["terminal_sha256"] = hashlib.sha256(raw).hexdigest()
            print(json.dumps(terminal, sort_keys=True), flush=True)
            return
        state = ec2.describe_instances(InstanceIds=[instance_id])["Reservations"][0]["Instances"][0]["State"]["Name"]
        if state in {"terminated", "shutting-down"}:
            raise RuntimeError("Spot instance stopped before terminal; restart complete cell with a new attempt")
        time.sleep(20)
    ec2.terminate_instances(InstanceIds=[instance_id])
    raise TimeoutError("V156 test cell exceeded wall cap")


if __name__ == "__main__":
    main()
