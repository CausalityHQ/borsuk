#!/usr/bin/env python3
"""Run one immutable narrow authority crate test on Causality Spot."""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import io
import json
import subprocess
import tarfile
import time

import boto3

from scripts.launch_v157_primary_feasibility_spot import (
    BUCKET, PROFILE_ARN, REGION, SECURITY_GROUP, SUBNET,
    archive_source, missing, put_if_absent,
)

SCHEMA = "borsuk-v176-authority-compile-spot-v1"
TAG = "borsuk-v176-authority-compile"
IMAGE = "ami-06121aa3085b6f918"
WALL_SECONDS = 3_600
ARTIFACTS = ("test.log", "test-resources.txt", "run-closed.log")


def user_data(commit: str, archive_sha: str, archive_key: str,
              prefix: str) -> str:
    return f'''#!/bin/bash
set -euo pipefail
systemd-run --unit=v176-authority-compile-stop --on-active={WALL_SECONDS}s /usr/sbin/shutdown -h now
root=/mnt/v176-authority-compile
mkdir -p "$root" && cd "$root"
phase=bootstrap
finish() {{
  code=$?
  trap - EXIT TERM
  set +e
  cd "$root"
  cp run.log run-closed.log || code=96
  token=$(curl -fsS -X PUT -H 'X-aws-ec2-metadata-token-ttl-seconds: 60' http://169.254.169.254/latest/api/token)
  instance_id=$(curl -fsS -H "X-aws-ec2-metadata-token: $token" http://169.254.169.254/latest/meta-data/instance-id)
  for name in {' '.join(ARTIFACTS)}; do
    if [ -f "$name" ]; then
      aws s3 cp "$name" "s3://{BUCKET}/{prefix}/artifacts/$name" --only-show-errors || code=96
    fi
  done
  INSTANCE_ID="$instance_id" EXIT_CODE="$code" PHASE="$phase" python3 - <<'PY' >terminal.json
import hashlib,json,os
from pathlib import Path
artifacts={{}}
for name in {ARTIFACTS!r}:
    path=Path(name)
    if path.is_file():
        digest=hashlib.sha256()
        with path.open('rb') as source:
            for chunk in iter(lambda: source.read(1024*1024),b''):
                digest.update(chunk)
        artifacts[name]={{'bytes':path.stat().st_size,'sha256':digest.hexdigest()}}
code=int(os.environ['EXIT_CODE'])
print(json.dumps({{'schema':'{SCHEMA}','source_commit':'{commit}',
  'source_archive_sha256':'{archive_sha}',
  'instance_id':os.environ['INSTANCE_ID'],'exit_code':code,
  'phase':os.environ['PHASE'],
  'status':'complete' if code==0 and os.environ['PHASE']=='complete' else 'failed',
  'artifacts':artifacts}},sort_keys=True,separators=(',',':')))
PY
  aws s3 cp terminal.json "s3://{BUCKET}/{prefix}/terminal.json" --only-show-errors
  shutdown -h now || true
  exit "$code"
}}
trap finish EXIT
trap 'exit 97' TERM
exec >run.log 2>&1
aws s3 cp 's3://{BUCKET}/{archive_key}' source.tar.gz --only-show-errors
printf '%s  source.tar.gz\\n' '{archive_sha}' | sha256sum -c -
mkdir repo && tar -xzf source.tar.gz -C repo
phase=install
dnf install -y -q gcc gcc-c++ cmake perl tar gzip time
export RUSTUP_HOME="$root/.rustup" CARGO_HOME="$root/.cargo"
export CARGO_TARGET_DIR="$root/target" CARGO_BUILD_JOBS=4
curl -fsSL https://sh.rustup.rs | sh -s -- -y --profile minimal \
  --default-toolchain 1.98.0
phase=test
/usr/bin/time -v -o test-resources.txt timeout --signal=TERM --kill-after=30 3300 \
  bash -c 'set -e; for suite in relaid_generation_authority::tests serving_generation::tests; do "$1" test --locked --manifest-path repo/Cargo.toml -p borsuk --lib "$suite" --jobs 4 -- --nocapture; done' _ "$CARGO_HOME/bin/cargo" >test.log 2>&1
phase=complete
'''


def launch(attempt: str) -> None:
    if len(attempt) != 5 or not attempt.startswith("a") or not attempt[1:].isdigit():
        raise ValueError("attempt must be aNNNN")
    if subprocess.check_output(["git", "status", "--porcelain"], text=True).strip():
        raise ValueError("worktree must be clean")
    commit = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    subprocess.run(["git", "fetch", "origin", "main"], check=True)
    if subprocess.run(["git", "merge-base", "--is-ancestor", "HEAD",
                       "origin/main"], check=False).returncode != 0:
        raise ValueError("V176 source commit is not pushed to origin/main")
    archive = archive_source(commit)
    with tarfile.open(fileobj=io.BytesIO(archive), mode="r:gz") as tar:
        if not {"crates/borsuk/src/serving_generation.rs",
                "crates/borsuk/src/relaid_generation_authority.rs", "Cargo.toml",
                "Cargo.lock"}.issubset(tar.getnames()):
            raise ValueError("source archive lacks authority crate test")
    archive_sha = hashlib.sha256(archive).hexdigest()
    archive_key = f"research/v176-authority-compile/{commit}/sources/{archive_sha}.tar.gz"
    prefix = f"research/v176-authority-compile/{commit}/runs/{attempt}"
    session = boto3.Session(profile_name="causality", region_name=REGION)
    ec2, s3 = session.client("ec2"), session.client("s3")
    if not missing(s3, prefix + "/reservation.json") or not missing(s3, prefix + "/terminal.json"):
        raise ValueError("attempt already registered")
    active = ec2.describe_instances(Filters=[
        {"Name": "tag:Name", "Values": [TAG]},
        {"Name": "instance-state-name", "Values": ["pending", "running", "stopping", "stopped"]},
    ])
    if any(item.get("Instances") for item in active["Reservations"]):
        raise ValueError("V176 compile worker already active")
    if missing(s3, archive_key):
        put_if_absent(archive_key, archive)
    elif s3.head_object(Bucket=BUCKET, Key=archive_key)["ContentLength"] != len(archive):
        raise ValueError("source archive length differs")
    put_if_absent(prefix + "/reservation.json", json.dumps({
        "schema": SCHEMA, "source_commit": commit,
        "source_archive_sha256": archive_sha,
        "test": "cargo test --locked -p borsuk --lib relaid_generation_authority::tests; serving_generation::tests",
        "interruption_policy": "discard and restart under a new attempt",
    }, sort_keys=True).encode())
    receipt = ec2.run_instances(
        ClientToken="v176authority-" + hashlib.sha256(prefix.encode()).hexdigest()[:48],
        ImageId=IMAGE, InstanceType="c7i.8xlarge", MinCount=1, MaxCount=1,
        IamInstanceProfile={"Arn": PROFILE_ARN},
        NetworkInterfaces=[{"AssociatePublicIpAddress": True, "DeviceIndex": 0,
                            "Groups": [SECURITY_GROUP], "SubnetId": SUBNET}],
        InstanceMarketOptions={"MarketType": "spot", "SpotOptions": {
            "InstanceInterruptionBehavior": "terminate", "SpotInstanceType": "one-time"}},
        InstanceInitiatedShutdownBehavior="terminate",
        BlockDeviceMappings=[{"DeviceName": "/dev/xvda", "Ebs": {
            "DeleteOnTermination": True, "Encrypted": True,
            "VolumeSize": 80, "VolumeType": "gp3"}}],
        TagSpecifications=[{"ResourceType": "instance", "Tags": [
            {"Key": "Name", "Value": TAG}, {"Key": "BorsukAttempt", "Value": attempt}]}],
        UserData=user_data(commit, archive_sha, archive_key, prefix),
    )
    instance_id = receipt["Instances"][0]["InstanceId"]
    print(json.dumps({"instance_id": instance_id, "output_prefix": prefix,
                      "source_commit": commit}), flush=True)
    started = time.monotonic()
    while time.monotonic() - started < WALL_SECONDS + 600:
        if not missing(s3, prefix + "/terminal.json"):
            raw = s3.get_object(Bucket=BUCKET,
                                Key=prefix + "/terminal.json")["Body"].read()
            ec2.terminate_instances(InstanceIds=[instance_id])
            ec2.get_waiter("instance_terminated").wait(InstanceIds=[instance_id])
            terminal = json.loads(raw)
            if (terminal.get("schema") != SCHEMA
                    or terminal.get("instance_id") != instance_id
                    or terminal.get("source_commit") != commit
                    or terminal.get("source_archive_sha256") != archive_sha):
                raise ValueError("terminal identity differs")
            if terminal.get("status") == "complete" and not set(ARTIFACTS).issubset(terminal.get("artifacts", {})):
                raise ValueError("complete terminal lacks artifacts")
            for name, identity in terminal.get("artifacts", {}).items():
                body = s3.get_object(Bucket=BUCKET,
                    Key=f"{prefix}/artifacts/{name}")["Body"]
                digest = hashlib.sha256()
                size = 0
                while chunk := body.read(1024 * 1024):
                    digest.update(chunk)
                    size += len(chunk)
                if size != identity["bytes"] or digest.hexdigest() != identity["sha256"]:
                    raise ValueError(f"V176 S3 artifact differs: {name}")
            terminal["terminal_sha256"] = hashlib.sha256(raw).hexdigest()
            print(json.dumps(terminal, sort_keys=True), flush=True)
            if terminal.get("status") != "complete":
                raise RuntimeError("V176 compile terminal failed; see closed artifacts")
            return
        state = ec2.describe_instances(InstanceIds=[instance_id])["Reservations"][0]["Instances"][0]["State"]["Name"]
        if state in {"terminated", "shutting-down"}:
            raise RuntimeError("Spot compile worker stopped before terminal; retry new attempt")
        time.sleep(20)
    ec2.terminate_instances(InstanceIds=[instance_id])
    raise TimeoutError("V176 compile cell exceeded wall cap")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--attempt", default="a0001")
    args = parser.parse_args()
    with open("/tmp/borsuk-v176-authority-compile-launch.lock", "a+") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RuntimeError("V176 compile launcher is already running here") from exc
        launch(args.attempt)


if __name__ == "__main__":
    main()
