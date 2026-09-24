#!/usr/bin/env python3
"""Launch one immutable V160 relayout quality cell on Causality Spot."""

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

SCHEMA = "borsuk-v160-geometric-relayout-spot-v1"
TAG = "borsuk-v160-geometric-relayout"
WALL_SECONDS = 5400
GEOMETRIC = ("research/native-geometric-layout/"
             "f7a09ea5df975fb6c2e36a53ed56f53492b334fd/"
             "runs/relaion-100k-dev1000-a0001/artifacts/")
V158 = ("research/v158-pq-primary/9933ae6a5445e0eaf4b408d63e6cc0f135817447/"
        "runs/a0001/")
EARLY_INPUTS = tuple(item for item in V158_INPUTS if item[0] != "truth.parquet") + (
    ("membership.parquet", GEOMETRIC + "membership-balanced-two-means-480k.parquet",
     762_442, "f72b80f1341bd64599e51a69e627f0b9a2280be4f5235a6c5995fef8eacd866c"),
    ("membership-seal.json", GEOMETRIC + "sealed-memberships.json",
     1_461, "ceca9819364827ca7b3e07a91208e9b8724d987e13850045bade3a327157413d"),
)
LATE_INPUTS = tuple(item for item in V158_INPUTS if item[0] == "truth.parquet") + (
    ("v158-terminal.json", V158 + "terminal.json", 922,
     "09306fa1aca94748635eca32ac9259e934249ee1fb620c3ec333977517a2dd35"),
    ("v158-raw.jsonl", V158 + "artifacts/raw.jsonl", 2_177_170,
     "12a77f91b6b0898ae2b9ed17f0450556eec7bf0a2bdc9d2f4130bd71193e00ba"),
)
ARTIFACTS = ("new-sq8.bin", "layout-seal.json", "plans.jsonl", "plan-seal.json",
             "raw.jsonl", "summary.json", "check.json", "run.log")


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
systemd-run --unit=v160-hard-stop --on-active={WALL_SECONDS}s /usr/sbin/shutdown -h now
root=/mnt/v160-geometric-relayout
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
export OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1
phase=early-inputs
{download_script(EARLY_INPUTS)}
phase=construct
.venv/bin/python repo/scripts/v160_geometric_relayout_primary.py construct \\
  --old-sq8 sq8.bin --old-manifest manifest.json \\
  --membership membership.parquet --membership-seal membership-seal.json \\
  --new-sq8 new-sq8.bin --layout-seal layout-seal.json
phase=plan
.venv/bin/python repo/scripts/v160_geometric_relayout_primary.py plan \\
  --requests requests.jsonl --reference reference.jsonl \\
  --old-sq8 sq8.bin --membership membership.parquet \\
  --membership-seal membership-seal.json --new-sq8 new-sq8.bin \\
  --layout-seal layout-seal.json --plans plans.jsonl --plan-seal plan-seal.json
phase=late-inputs
{download_script(LATE_INPUTS)}
phase=reduce
.venv/bin/python repo/scripts/v160_geometric_relayout_primary.py reduce \\
  --requests requests.jsonl --plans plans.jsonl --plan-seal plan-seal.json \\
  --layout-seal layout-seal.json --new-sq8 new-sq8.bin \\
  --old-manifest manifest.json --truth truth.parquet \\
  --v158-terminal v158-terminal.json --v158-raw v158-raw.jsonl \\
  --raw raw.jsonl --summary summary.json
phase=check
.venv/bin/python repo/scripts/v160_check_geometric_relayout.py \\
  --old-sq8 sq8.bin --membership membership.parquet \\
  --membership-seal membership-seal.json --new-sq8 new-sq8.bin \\
  --layout-seal layout-seal.json --requests requests.jsonl \\
  --reference reference.jsonl --plans plans.jsonl --plan-seal plan-seal.json \\
  --truth truth.parquet --v158-terminal v158-terminal.json \\
  --v158-raw v158-raw.jsonl --raw raw.jsonl --summary summary.json >check.json
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
        required = {"scripts/v160_geometric_relayout_primary.py",
                    "scripts/v160_check_geometric_relayout.py",
                    "docs/research/v160-geometric-relayout-primary-prereg.md"}
        if not required.issubset(tar.getnames()):
            raise ValueError("source archive lacks V160 gate")
    archive_sha = hashlib.sha256(archive).hexdigest()
    archive_key = f"research/v160-geometric-relayout/{commit}/sources/{archive_sha}.tar.gz"
    prefix = f"research/v160-geometric-relayout/{commit}/runs/{attempt}"
    session = boto3.Session(profile_name="causality", region_name=REGION)
    ec2, s3 = session.client("ec2"), session.client("s3")
    if not missing(s3, prefix + "/reservation.json") or not missing(s3, prefix + "/terminal.json"):
        raise ValueError("attempt already registered")
    active = ec2.describe_instances(Filters=[
        {"Name": "tag:Name", "Values": [TAG]},
        {"Name": "instance-state-name", "Values": ["pending", "running", "stopping", "stopped"]},
    ])
    if any(item.get("Instances") for item in active["Reservations"]):
        raise ValueError("V160 worker already active")
    for _, key, size, _ in EARLY_INPUTS + LATE_INPUTS:
        if s3.head_object(Bucket=BUCKET, Key=key)["ContentLength"] != size:
            raise ValueError("frozen input length differs")
    if missing(s3, archive_key):
        put_if_absent(archive_key, archive)
    elif s3.head_object(Bucket=BUCKET, Key=archive_key)["ContentLength"] != len(archive):
        raise ValueError("source archive length differs")
    reservation = {"schema": SCHEMA, "source_commit": commit,
                   "source_archive_sha256": archive_sha,
                   "interruption_policy": "discard complete cell and restart a new attempt",
                   "output_prefix": f"s3://{BUCKET}/{prefix}"}
    put_if_absent(prefix + "/reservation.json", json.dumps(reservation, sort_keys=True).encode())
    receipt = ec2.run_instances(
        ClientToken="v160-" + hashlib.sha256(prefix.encode()).hexdigest()[:48],
        ImageId=IMAGE, InstanceType="c7i.xlarge", MinCount=1, MaxCount=1,
        IamInstanceProfile={"Arn": PROFILE_ARN},
        NetworkInterfaces=[{"AssociatePublicIpAddress": True, "DeviceIndex": 0,
                            "Groups": [SECURITY_GROUP], "SubnetId": SUBNET}],
        InstanceMarketOptions={"MarketType": "spot", "SpotOptions": {
            "InstanceInterruptionBehavior": "terminate", "SpotInstanceType": "one-time"}},
        InstanceInitiatedShutdownBehavior="terminate",
        BlockDeviceMappings=[{"DeviceName": "/dev/xvda", "Ebs": {
            "DeleteOnTermination": True, "Encrypted": True,
            "VolumeSize": 40, "VolumeType": "gp3"}}],
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
                raise RuntimeError("V160 terminal reports failed; see closed artifacts")
            return
        state = ec2.describe_instances(InstanceIds=[instance_id])["Reservations"][0]["Instances"][0]["State"]["Name"]
        if state in {"terminated", "shutting-down"}:
            raise RuntimeError("Spot worker stopped before terminal; restart full cell")
        time.sleep(20)
    ec2.terminate_instances(InstanceIds=[instance_id])
    raise TimeoutError("V160 cell exceeded wall cap")


if __name__ == "__main__":
    main()
