#!/usr/bin/env python3
"""Check the immutable V168 a0002 measurement without rerunning its screen."""

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

from scripts.launch_v168_pq_neighbor_ranking_spot import (
    BUCKET, IMAGE, INPUTS, PROFILE_ARN, REGION, SECURITY_GROUP, SUBNET,
    WALL_SECONDS, archive_source, download_script, missing, put_if_absent,
)

SCHEMA = "borsuk-v168-closed-check-v1"
TAG = "borsuk-v168-closed-check"
CLOSED_PREFIX = ("research/v168-pq-neighbor-ranking/"
                 "9cb5a36a18b061d0a9f1cb5eedc4dfcd2b719b08/runs/a0002")
CLOSED_TERMINAL_SHA = "5a9e8020c6be4daa65df0c48dcabe0079132eeba09aeea676ff9a6e2375b5d3a"
CLOSED_COMMIT = "9cb5a36a18b061d0a9f1cb5eedc4dfcd2b719b08"
CLOSED_ARTIFACTS = (
    "out/features.jsonl", "out/proxy-labels.jsonl", "out/prepare-seal.json",
    "out/plans.jsonl", "out/plan-seal.json", "out/summary.json",
)
ARTIFACTS = ("check.json", "check-resources.txt", "run-closed.log")


def user_data(commit: str, archive_sha: str, archive_key: str,
              prefix: str, prior: dict) -> str:
    closed_download = "\n".join(
        f"aws s3 cp 's3://{BUCKET}/{CLOSED_PREFIX}/artifacts/{name}' "
        f"'{name}' --only-show-errors\n"
        f"[ \"$(stat -c%s '{name}')\" = '{prior['artifacts'][name]['bytes']}' ]\n"
        f"printf '%s  %s\\n' '{prior['artifacts'][name]['sha256']}' "
        f"'{name}' | sha256sum -c -"
        for name in CLOSED_ARTIFACTS)
    return f'''#!/bin/bash
set -euo pipefail
systemd-run --unit=v168-closed-check-stop --on-active={WALL_SECONDS}s /usr/sbin/shutdown -h now
root=/mnt/v168-closed-check
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
        artifacts[name]={{'bytes':path.stat().st_size,
          'sha256':hashlib.sha256(path.read_bytes()).hexdigest()}}
code=int(os.environ['EXIT_CODE'])
print(json.dumps({{'schema':'{SCHEMA}','source_commit':'{commit}',
  'source_archive_sha256':'{archive_sha}',
  'closed_terminal_sha256':'{CLOSED_TERMINAL_SHA}',
  'closed_prefix':'{CLOSED_PREFIX}',
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
dnf install -y -q python3.12 python3.12-pip
python3.12 -m venv .venv
.venv/bin/pip install -q numpy==1.26.4 pyarrow==17.0.0
export PYTHONPATH="$root/repo"
export OPENBLAS_NUM_THREADS=8 OMP_NUM_THREADS=8 MKL_NUM_THREADS=8
mkdir router out
phase=inputs
{download_script()}
phase=closed-artifacts
{closed_download}
phase=check
/usr/bin/time -v -o check-resources.txt .venv/bin/python -m \
  scripts.v168_pq_neighbor_ranking_run check --output out \
  --source source.parquet --old-layout old-layout.npy \
  --old-sq8 old-sq8.bin --router router --order order.npy \
  --v164-terminal v164-terminal.json >check.json
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
        raise ValueError("V168 checker source is not pushed to origin/main")
    archive = archive_source(commit)
    with tarfile.open(fileobj=io.BytesIO(archive), mode="r:gz") as tar:
        if not {"scripts/v168_pq_neighbor_ranking_run.py",
                "scripts/v168_scored_neighbor_field.py"}.issubset(tar.getnames()):
            raise ValueError("source archive lacks V168 checker")
    archive_sha = hashlib.sha256(archive).hexdigest()
    archive_key = f"research/v168-closed-check/{commit}/sources/{archive_sha}.tar.gz"
    prefix = f"research/v168-closed-check/{commit}/runs/{attempt}"
    session = boto3.Session(profile_name="causality", region_name=REGION)
    ec2, s3 = session.client("ec2"), session.client("s3")
    if not missing(s3, prefix + "/reservation.json") or not missing(s3, prefix + "/terminal.json"):
        raise ValueError("closed-check attempt already registered")
    prior_raw = s3.get_object(Bucket=BUCKET,
                              Key=CLOSED_PREFIX + "/terminal.json")["Body"].read()
    if hashlib.sha256(prior_raw).hexdigest() != CLOSED_TERMINAL_SHA:
        raise ValueError("closed terminal identity differs")
    prior = json.loads(prior_raw)
    if (prior.get("source_commit") != CLOSED_COMMIT
            or prior.get("phase") != "check" or prior.get("status") != "failed"
            or not set(CLOSED_ARTIFACTS).issubset(prior.get("artifacts", {}))):
        raise ValueError("closed measurement provenance differs")
    active = ec2.describe_instances(Filters=[
        {"Name": "tag:Name", "Values": [TAG, "borsuk-v168-pq-neighbor-ranking"]},
        {"Name": "instance-state-name", "Values": ["pending", "running", "stopping", "stopped"]},
    ])
    if any(item.get("Instances") for item in active["Reservations"]):
        raise ValueError("V168 worker already active")
    for _, key, size, _ in INPUTS:
        if s3.head_object(Bucket=BUCKET, Key=key)["ContentLength"] != size:
            raise ValueError("frozen input length differs")
    for name in CLOSED_ARTIFACTS:
        key = f"{CLOSED_PREFIX}/artifacts/{name}"
        if s3.head_object(Bucket=BUCKET, Key=key)["ContentLength"] != prior["artifacts"][name]["bytes"]:
            raise ValueError(f"closed artifact length differs: {name}")
    if missing(s3, archive_key):
        put_if_absent(archive_key, archive)
    elif s3.head_object(Bucket=BUCKET, Key=archive_key)["ContentLength"] != len(archive):
        raise ValueError("source archive length differs")
    put_if_absent(prefix + "/reservation.json", json.dumps({
        "schema": SCHEMA, "source_commit": commit,
        "source_archive_sha256": archive_sha,
        "closed_terminal_sha256": CLOSED_TERMINAL_SHA,
        "closed_prefix": CLOSED_PREFIX,
        "interruption_policy": "discard check and restart under a new attempt",
    }, sort_keys=True).encode())
    receipt = ec2.run_instances(
        ClientToken="v168check-" + hashlib.sha256(prefix.encode()).hexdigest()[:48],
        ImageId=IMAGE, InstanceType="c7i.12xlarge", MinCount=1, MaxCount=1,
        IamInstanceProfile={"Arn": PROFILE_ARN},
        NetworkInterfaces=[{"AssociatePublicIpAddress": True, "DeviceIndex": 0,
                            "Groups": [SECURITY_GROUP], "SubnetId": SUBNET}],
        InstanceMarketOptions={"MarketType": "spot", "SpotOptions": {
            "InstanceInterruptionBehavior": "terminate", "SpotInstanceType": "one-time"}},
        InstanceInitiatedShutdownBehavior="terminate",
        BlockDeviceMappings=[{"DeviceName": "/dev/xvda", "Ebs": {
            "DeleteOnTermination": True, "Encrypted": True,
            "VolumeSize": 30, "VolumeType": "gp3"}}],
        TagSpecifications=[{"ResourceType": "instance", "Tags": [
            {"Key": "Name", "Value": TAG}, {"Key": "BorsukAttempt", "Value": attempt}]}],
        UserData=user_data(commit, archive_sha, archive_key, prefix, prior),
    )
    instance_id = receipt["Instances"][0]["InstanceId"]
    print(json.dumps({"instance_id": instance_id, "output_prefix": prefix,
                      "source_commit": commit, "closed_prefix": CLOSED_PREFIX}), flush=True)
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
                    or terminal.get("source_archive_sha256") != archive_sha
                    or terminal.get("closed_terminal_sha256") != CLOSED_TERMINAL_SHA
                    or terminal.get("closed_prefix") != CLOSED_PREFIX):
                raise ValueError("closed-check terminal identity differs")
            if terminal.get("status") == "complete" and not set(ARTIFACTS).issubset(terminal.get("artifacts", {})):
                raise ValueError("complete closed-check terminal lacks artifacts")
            for name, identity in terminal.get("artifacts", {}).items():
                body = s3.get_object(Bucket=BUCKET,
                    Key=f"{prefix}/artifacts/{name}")["Body"]
                digest = hashlib.sha256()
                size = 0
                while chunk := body.read(1024 * 1024):
                    digest.update(chunk)
                    size += len(chunk)
                if size != identity["bytes"] or digest.hexdigest() != identity["sha256"]:
                    raise ValueError(f"closed-check S3 artifact differs: {name}")
            terminal["terminal_sha256"] = hashlib.sha256(raw).hexdigest()
            print(json.dumps(terminal, sort_keys=True), flush=True)
            if terminal.get("status") != "complete":
                raise RuntimeError("closed-check terminal failed; see closed artifacts")
            return
        state = ec2.describe_instances(InstanceIds=[instance_id])["Reservations"][0]["Instances"][0]["State"]["Name"]
        if state in {"terminated", "shutting-down"}:
            raise RuntimeError("Spot check worker stopped before terminal; retry new attempt")
        time.sleep(20)
    ec2.terminate_instances(InstanceIds=[instance_id])
    raise TimeoutError("closed-check cell exceeded wall cap")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--attempt", default="a0001")
    args = parser.parse_args()
    with open("/tmp/borsuk-v168-closed-check-launch.lock", "a+") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RuntimeError("V168 closed checker already running here") from exc
        launch(args.attempt)


if __name__ == "__main__":
    main()
