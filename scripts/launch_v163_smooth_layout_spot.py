#!/usr/bin/env python3
"""Run one frozen V163 smooth-layout 100k gate on Causality Spot."""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import subprocess
import tarfile
import time

import boto3

from scripts.launch_v157_primary_feasibility_spot import (
    BUCKET, IMAGE, PROFILE_ARN, REGION, SECURITY_GROUP, SUBNET,
    archive_source, missing, put_if_absent,
)
from scripts.launch_v158_pq_primary_spot import INPUTS as V158_INPUTS

SCHEMA = "borsuk-v163-smooth-layout-spot-v1"
TAG = "borsuk-v163-smooth-layout"
WALL_SECONDS = 7_200
SOURCE = ("research/v85-pq16-page-nomination/"
          "24383d853474a19702d18d2de700bee3618167f5/"
          "100k-a0023/attempt/inputs/source-100k.parquet")
V160 = ("research/v160-geometric-relayout/"
        "1a874d21abe5166bbd769c91eb2de29746af0957/runs/a0001/")
EARLY_INPUTS = (
    ("source.parquet", SOURCE, 145_121_661,
     "a199e151b89a496ed20e39fdd951591bbfb4817d682e9111ebe2e1cab7ae550d"),
) + tuple(item for item in V158_INPUTS if item[0] != "truth.parquet")
LATE_INPUTS = tuple(item for item in V158_INPUTS if item[0] == "truth.parquet") + (
    ("v160-terminal.json", V160 + "terminal.json", 1_152,
     "026bf6792ae140f8cba83bc4452d4db0e54cb31facfdd84394309117931f0c52"),
    ("v160-raw.jsonl", V160 + "artifacts/raw.jsonl", 1_140_746,
     "c2d0676ba3f0003a6f90681ce28143d509847786a3714ce98f0ffca02d1eab67"),
)
ARTIFACTS = (
    "order.npy", "new-sq8.bin", "layout-seal.json", "plans.jsonl",
    "plan-seal.json", "raw.jsonl", "summary.json", "check.json",
    "construct-resources.txt", "plan-resources.txt", "reduce-resources.txt",
    "check-resources.txt", "run.log",
)


def download_script(inputs: tuple) -> str:
    return "\n".join(
        f"aws s3 cp 's3://{BUCKET}/{key}' '{name}' --only-show-errors\n"
        f"[ \"$(stat -c%s '{name}')\" = '{size}' ]\n"
        f"printf '%s  %s\\n' '{digest}' '{name}' | sha256sum -c -"
        for name, key, size, digest in inputs
    )


def user_data(commit: str, archive_sha: str, archive_key: str, prefix: str) -> str:
    return f'''#!/bin/bash
set -euo pipefail
systemd-run --unit=v163-hard-stop --on-active={WALL_SECONDS}s /usr/sbin/shutdown -h now
root=/mnt/v163-smooth-layout
mkdir -p "$root" && cd "$root"
phase=bootstrap
finish() {{
  code=$?
  trap - EXIT TERM
  set +e
  cd "$root"
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
dnf install -y -q python3.12 python3.12-pip
python3.12 -m venv .venv
.venv/bin/pip install -q numpy==1.26.4 pyarrow==17.0.0
export PYTHONPATH="$root/repo"
export OPENBLAS_NUM_THREADS=8 OMP_NUM_THREADS=8 MKL_NUM_THREADS=8
phase=early-inputs
{download_script(EARLY_INPUTS)}
phase=construct
/usr/bin/time -v -o construct-resources.txt .venv/bin/python \\
  repo/scripts/v163_smooth_layout_100k.py construct \\
  --source source.parquet --old-sq8 sq8.bin --old-manifest manifest.json \\
  --order order.npy --new-sq8 new-sq8.bin --layout-seal layout-seal.json
phase=plan
/usr/bin/time -v -o plan-resources.txt .venv/bin/python \\
  repo/scripts/v163_smooth_layout_100k.py plan \\
  --requests requests.jsonl --reference reference.jsonl --old-sq8 sq8.bin \\
  --order order.npy --new-sq8 new-sq8.bin \\
  --layout-seal layout-seal.json --plans plans.jsonl --plan-seal plan-seal.json
phase=late-inputs
{download_script(LATE_INPUTS)}
phase=reduce
/usr/bin/time -v -o reduce-resources.txt .venv/bin/python \\
  repo/scripts/v163_smooth_layout_100k.py reduce \\
  --requests requests.jsonl --plans plans.jsonl --plan-seal plan-seal.json \\
  --layout-seal layout-seal.json --new-sq8 new-sq8.bin \\
  --old-manifest manifest.json --truth truth.parquet \\
  --v160-terminal v160-terminal.json --v160-raw v160-raw.jsonl \\
  --raw raw.jsonl --summary summary.json
phase=check
/usr/bin/time -v -o check-resources.txt .venv/bin/python \\
  repo/scripts/v163_check_smooth_layout.py \\
  --source source.parquet --old-sq8 sq8.bin --old-manifest manifest.json \\
  --order order.npy --new-sq8 new-sq8.bin --layout-seal layout-seal.json \\
  --requests requests.jsonl --reference reference.jsonl \\
  --plans plans.jsonl --plan-seal plan-seal.json \\
  --truth truth.parquet --v160-terminal v160-terminal.json \\
  --v160-raw v160-raw.jsonl \\
  --raw raw.jsonl --summary summary.json >check.json
phase=complete
'''


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--attempt", default="a0001")
    attempt = parser.parse_args().attempt
    if len(attempt) != 5 or not attempt.startswith("a") or not attempt[1:].isdigit():
        raise ValueError("attempt must be aNNNN")
    if subprocess.check_output(["git", "status", "--porcelain"], text=True).strip():
        raise ValueError("worktree must be clean")
    commit = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    archive = archive_source(commit)
    with tarfile.open(fileobj=io.BytesIO(archive), mode="r:gz") as tar:
        required = {"scripts/v163_smooth_layout_100k.py",
                    "scripts/v163_check_smooth_layout.py",
                    "docs/research/v163-smooth-geometric-layout-100k-prereg.md"}
        if not required.issubset(tar.getnames()):
            raise ValueError("source archive lacks V163 gate")
    archive_sha = hashlib.sha256(archive).hexdigest()
    archive_key = f"research/v163-smooth-layout/{commit}/sources/{archive_sha}.tar.gz"
    prefix = f"research/v163-smooth-layout/{commit}/runs/{attempt}"
    session = boto3.Session(profile_name="causality", region_name=REGION)
    ec2, s3 = session.client("ec2"), session.client("s3")
    if not missing(s3, prefix + "/reservation.json") or not missing(s3, prefix + "/terminal.json"):
        raise ValueError("attempt already registered")
    active = ec2.describe_instances(Filters=[
        {"Name": "tag:Name", "Values": [TAG]},
        {"Name": "instance-state-name", "Values": ["pending", "running", "stopping", "stopped"]},
    ])
    if any(item.get("Instances") for item in active["Reservations"]):
        raise ValueError("V163 worker already active")
    for _, key, size, _ in EARLY_INPUTS + LATE_INPUTS:
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
        ClientToken="v163-" + hashlib.sha256(prefix.encode()).hexdigest()[:48],
        ImageId=IMAGE, InstanceType="c7i.8xlarge", MinCount=1, MaxCount=1,
        IamInstanceProfile={"Arn": PROFILE_ARN},
        NetworkInterfaces=[{"AssociatePublicIpAddress": True, "DeviceIndex": 0,
                            "Groups": [SECURITY_GROUP], "SubnetId": SUBNET}],
        InstanceMarketOptions={"MarketType": "spot", "SpotOptions": {
            "InstanceInterruptionBehavior": "terminate", "SpotInstanceType": "one-time"}},
        InstanceInitiatedShutdownBehavior="terminate",
        BlockDeviceMappings=[{"DeviceName": "/dev/xvda", "Ebs": {
            "DeleteOnTermination": True, "Encrypted": True,
            "VolumeSize": 60, "VolumeType": "gp3"}}],
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
            if terminal.get("status") == "complete":
                if not set(ARTIFACTS).issubset(terminal.get("artifacts", {})):
                    raise ValueError("complete terminal lacks artifacts")
            ec2.terminate_instances(InstanceIds=[instance_id])
            ec2.get_waiter("instance_terminated").wait(InstanceIds=[instance_id])
            terminal["terminal_sha256"] = hashlib.sha256(raw).hexdigest()
            print(json.dumps(terminal, sort_keys=True), flush=True)
            if terminal.get("status") != "complete":
                raise RuntimeError("V163 terminal reports failed; see closed artifacts")
            return
        state = ec2.describe_instances(InstanceIds=[instance_id])["Reservations"][0]["Instances"][0]["State"]["Name"]
        if state in {"terminated", "shutting-down"}:
            raise RuntimeError("Spot worker stopped before terminal; restart full cell")
        time.sleep(20)
    ec2.terminate_instances(InstanceIds=[instance_id])
    raise TimeoutError("V163 cell exceeded wall cap")


if __name__ == "__main__":
    main()
