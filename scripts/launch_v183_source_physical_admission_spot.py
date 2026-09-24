#!/usr/bin/env python3
"""Launch one sealed V183 physical admission cell on Spot."""

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

SCHEMA = "borsuk-v183-source-physical-admission-spot-v1"
TAG = "borsuk-v183-source-physical-admission"
WALL_SECONDS = 14_400
IMAGE = "ami-06121aa3085b6f918"
V182 = ("research/v182-wide-pq-rank/"
        "34fd11f56559cc7d453f3bd04c92aba83d62d5fc/runs/a0001/")
INPUTS = (
    ("v182/terminal.json", V182 + "terminal.json", 1_432,
     "fa3de27a1d56747dbc4e2afd1af3a976e88ed5b90626d4f6dd8f525f426841cb"),
    ("v182/features.jsonl", V182 + "artifacts/out/features.jsonl", 2_796_722,
     "146b02b08e64267fcff8bb1dd36fedabc9d77de9d95ac891401f6e73610a0281"),
    ("v182/fit-seal.json", V182 + "artifacts/out/fit-seal.json", 87_131,
     "79581320ac4ecbb74e046950fe44d2bc021a126bf12256d9a46350c2ec065810"),
)
LABELS = (
    ("v182/fit-labels.jsonl", V182 + "artifacts/out/fit-labels.jsonl", 62_972,
     "e278801fc69b659a02c68bc991abfd591772c10b319471b2dd792b03b45eff9c"),
    ("v182/holdout-labels.jsonl", V182 + "artifacts/out/holdout-labels.jsonl", 63_120,
     "d41af9f5a520ae346332244dfb17fd3209527f4c857749d598cd99a2e54fbbe6"),
)
ARTIFACTS = (
    "out/plans.jsonl", "out/plan-seal.json",
    "out/scored-plans.jsonl", "out/summary.json",
    "plan-resources.txt", "evaluate-resources.txt", "run-closed.log",
)


def download_script(inputs: tuple[tuple[str, str, int, str], ...]) -> str:
    return "\n".join(
        f"aws s3 cp 's3://{BUCKET}/{key}' '{name}' --only-show-errors\n"
        f"[ \"$(stat -c%s '{name}')\" = '{size}' ]\n"
        f"printf '%s  %s\\n' '{digest}' '{name}' | sha256sum -c -"
        for name, key, size, digest in inputs
    )


def user_data(commit: str, archive_sha: str, archive_key: str, prefix: str) -> str:
    return f'''#!/bin/bash
set -euo pipefail
systemd-run --unit=v183-hard-stop --on-active={WALL_SECONDS}s /usr/sbin/shutdown -h now
root=/mnt/v183-source-physical-admission
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
names={ARTIFACTS!r}
artifacts={{}}
for name in names:
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
dnf install -y -q python3.12
export PYTHONPATH="$root/repo"
mkdir v182
phase=inputs
{download_script(INPUTS)}
phase=plan
/usr/bin/time -v -o plan-resources.txt python3.12 -m \
  scripts.v183_source_physical_admission plan --v182 v182 --output out
# No V182 labels are downloaded until both plan artifacts are durable.
aws s3 cp out/plans.jsonl "s3://{BUCKET}/{prefix}/sealed/plans.jsonl" --only-show-errors
aws s3 cp out/plan-seal.json "s3://{BUCKET}/{prefix}/sealed/plan-seal.json" --only-show-errors
seal_sha=$(sha256sum out/plan-seal.json | cut -d ' ' -f1)
phase=labels
{download_script(LABELS)}
phase=evaluate
/usr/bin/time -v -o evaluate-resources.txt python3.12 -m \
  scripts.v183_source_physical_admission evaluate --v182 v182 --output out \
  --plan-sha256 "$seal_sha"
phase=complete
'''


def _launch() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--attempt", default="a0001")
    attempt = parser.parse_args().attempt
    if len(attempt) != 5 or not attempt.startswith("a") or not attempt[1:].isdigit():
        raise ValueError("attempt must be aNNNN")
    if subprocess.check_output(["git", "status", "--porcelain"], text=True).strip():
        raise ValueError("worktree must be clean")
    commit = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    subprocess.run(["git", "fetch", "origin", "main"], check=True)
    if subprocess.run(["git", "merge-base", "--is-ancestor", "HEAD",
                       "origin/main"], check=False).returncode != 0:
        raise ValueError("V183 source commit is not pushed to origin/main")
    archive = archive_source(commit)
    with tarfile.open(fileobj=io.BytesIO(archive), mode="r:gz") as tar:
        required = {"scripts/source_cover_frontier.py",
                    "scripts/v183_source_physical_admission.py",
                    "docs/research/v183-source-physical-admission-prereg.md"}
        if not required.issubset(tar.getnames()):
            raise ValueError("source archive lacks V183 gate")
    archive_sha = hashlib.sha256(archive).hexdigest()
    archive_key = f"research/v183-source-physical-admission/{commit}/sources/{archive_sha}.tar.gz"
    prefix = f"research/v183-source-physical-admission/{commit}/runs/{attempt}"
    session = boto3.Session(profile_name="causality", region_name=REGION)
    ec2, s3 = session.client("ec2"), session.client("s3")
    if not missing(s3, prefix + "/reservation.json") or not missing(s3, prefix + "/terminal.json"):
        raise ValueError("attempt already registered")
    active = ec2.describe_instances(Filters=[
        {"Name": "tag:Name", "Values": [TAG]},
        {"Name": "instance-state-name", "Values": ["pending", "running", "stopping", "stopped"]},
    ])
    if any(item.get("Instances") for item in active["Reservations"]):
        raise ValueError("V183 worker already active")
    for _, key, size, _ in INPUTS + LABELS:
        if s3.head_object(Bucket=BUCKET, Key=key)["ContentLength"] != size:
            raise ValueError("frozen input length differs")
    if missing(s3, archive_key):
        put_if_absent(archive_key, archive)
    elif s3.head_object(Bucket=BUCKET, Key=archive_key)["ContentLength"] != len(archive):
        raise ValueError("source archive length differs")
    put_if_absent(prefix + "/reservation.json", json.dumps({
        "schema": SCHEMA, "source_commit": commit,
        "source_archive_sha256": archive_sha,
        "interruption_policy": "discard complete cell and restart a new attempt",
        "output_prefix": f"s3://{BUCKET}/{prefix}",
    }, sort_keys=True).encode())
    receipt = ec2.run_instances(
        ClientToken="v183-" + hashlib.sha256(prefix.encode()).hexdigest()[:48],
        ImageId=IMAGE, InstanceType="c7i.4xlarge", MinCount=1, MaxCount=1,
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
        UserData=user_data(commit, archive_sha, archive_key, prefix),
    )
    instance_id = receipt["Instances"][0]["InstanceId"]
    print(json.dumps({"instance_id": instance_id, "output_prefix": prefix,
                      "source_commit": commit}), flush=True)
    started = time.monotonic()
    deadline = started + WALL_SECONDS + 600
    while time.monotonic() < deadline:
        if not missing(s3, prefix + "/terminal.json"):
            raw = s3.get_object(Bucket=BUCKET, Key=prefix + "/terminal.json")["Body"].read()
            ec2.terminate_instances(InstanceIds=[instance_id])
            ec2.get_waiter("instance_terminated").wait(InstanceIds=[instance_id])
            terminal = json.loads(raw)
            if (terminal.get("schema") != SCHEMA or terminal.get("instance_id") != instance_id
                    or terminal.get("source_commit") != commit
                    or terminal.get("source_archive_sha256") != archive_sha):
                raise ValueError("terminal identity differs")
            if terminal.get("status") == "complete":
                if not set(ARTIFACTS).issubset(terminal.get("artifacts", {})):
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
                    raise ValueError(f"V183 S3 artifact read-back differs: {name}")
            terminal["terminal_sha256"] = hashlib.sha256(raw).hexdigest()
            print(json.dumps(terminal, sort_keys=True), flush=True)
            if terminal.get("status") != "complete":
                raise RuntimeError("V183 terminal reports failed; see closed artifacts")
            return
        state = ec2.describe_instances(InstanceIds=[instance_id])["Reservations"][0]["Instances"][0]["State"]["Name"]
        if state in {"terminated", "shutting-down"}:
            if not missing(s3, prefix + "/terminal.json"):
                continue
            if time.monotonic() - started >= WALL_SECONDS - 60:
                raise TimeoutError("V183 worker reached wall cap before terminal")
            raise RuntimeError("Spot worker stopped before terminal; restart full cell")
        time.sleep(20)
    ec2.terminate_instances(InstanceIds=[instance_id])
    raise TimeoutError("V183 cell exceeded wall cap")


def main() -> None:
    # All V183 controllers on this devbox share one process-lifetime lock.
    # The EC2 tag check below also rejects an already running worker.
    with open("/tmp/borsuk-v183-source-physical-admission-launch.lock", "a+") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RuntimeError("V183 launcher is already running here") from exc
        _launch()


if __name__ == "__main__":
    main()
