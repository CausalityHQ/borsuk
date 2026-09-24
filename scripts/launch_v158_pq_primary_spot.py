#!/usr/bin/env python3
"""Run one immutable V158 paired returned-quality cell on Causality Spot."""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import tarfile
import time
import io

import boto3

from scripts.launch_v157_primary_feasibility_spot import (
    BUCKET, IMAGE, PROFILE_ARN, REGION, SECURITY_GROUP, SUBNET,
    archive_source, missing, put_if_absent,
)

SCHEMA = "borsuk-v158-pq-primary-spot-v1"
TAG = "borsuk-v158-primary-quality"
WALL_SECONDS = 3600
BASE = ("research/v114-exact-local/3443d7674432e281e6709bbb5ecee30f96b54a1e/"
        "runs/v114-100k-20260923T215331Z/a0001/artifacts/")
TRUTH = ("research/v85-competitive-rescore/fb976932ecd4076e2f76a7cb7e7aa7efe01e9a2d/"
         "runs/v85-100k-dev1000-20260920T094401Z-fb976932/a0001/"
         "inputs/truth-100k.parquet")
INPUTS = (
    ("requests.jsonl", BASE + "requests.jsonl", 17_714_558,
     "b2485629b919614bf46877a779b16d678cd1690d1872b7d4f9c9cbe6ddd94eb0"),
    ("reference.jsonl", BASE + "reference.jsonl", 9_374_356,
     "fa42050d6610630576f3f00232aecb8c43e6a0af350bf0ac90094aaba00c9b7b"),
    ("sq8.bin", BASE + "mirror/sq8.bin", 78_000_000,
     "5d215d5983da54015038083cd3f1cda16983a2660e7dd66be74bba96debe3375"),
    ("manifest.json", BASE + "mirror/manifest.json", 30_000,
     "14a12fa3f7a571a99bf4e4411fea0a9f4a2180579decc3e539db558accda5be5"),
    ("truth.parquet", TRUTH, 512_093,
     "ab8bfae34f753512f352581218596fc0f043354f8168192c856278b3ab5a0ce7"),
)


def user_data(commit: str, archive_sha: str, archive_key: str, prefix: str) -> str:
    downloads = "\n".join(
        f"aws s3 cp 's3://{BUCKET}/{key}' '{name}' --only-show-errors\n"
        f"[ \"$(stat -c%s '{name}')\" = '{size}' ]\n"
        f"printf '%s  %s\\n' '{sha}' '{name}' | sha256sum -c -"
        for name, key, size, sha in INPUTS
    )
    return f'''#!/bin/bash
set -euo pipefail
systemd-run --unit=v158-hard-stop --on-active={WALL_SECONDS}s /usr/sbin/shutdown -h now
root=/mnt/v158-pq-primary
mkdir -p "$root" && cd "$root"
phase=bootstrap
finish() {{
  code=$?
  trap - EXIT TERM
  set +e
  cd "$root"
  token=$(curl -fsS -X PUT -H 'X-aws-ec2-metadata-token-ttl-seconds: 60' http://169.254.169.254/latest/api/token)
  instance_id=$(curl -fsS -H "X-aws-ec2-metadata-token: $token" http://169.254.169.254/latest/meta-data/instance-id)
  for name in plans.jsonl seal.json raw.jsonl summary.json check.json run.log; do
    if [ -f "$name" ]; then
      aws s3 cp "$name" "s3://{BUCKET}/{prefix}/artifacts/$name" --only-show-errors || code=96
    fi
  done
  INSTANCE_ID="$instance_id" EXIT_CODE="$code" PHASE="$phase" python3 - <<'PY' >terminal.json
import hashlib,json,os
from pathlib import Path
names=('plans.jsonl','seal.json','raw.jsonl','summary.json','check.json','run.log')
artifacts={{}}
for name in names:
    path=Path(name)
    if path.is_file():
        artifacts[name]={{'bytes':path.stat().st_size,
                         'sha256':hashlib.sha256(path.read_bytes()).hexdigest()}}
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
phase=inputs
{downloads}
phase=plan
.venv/bin/python repo/scripts/v158_pq_primary_returned.py plan \\
  --requests requests.jsonl --reference reference.jsonl --sq8 sq8.bin \\
  --manifest manifest.json --plans plans.jsonl --seal seal.json
phase=reduce
.venv/bin/python repo/scripts/v158_pq_primary_returned.py reduce \\
  --requests requests.jsonl --sq8 sq8.bin --manifest manifest.json \\
  --truth truth.parquet --plans plans.jsonl --seal seal.json \\
  --raw raw.jsonl --summary summary.json
phase=check
.venv/bin/python repo/scripts/v158_check_pq_primary_returned.py \\
  --requests requests.jsonl --reference reference.jsonl --sq8 sq8.bin \\
  --truth truth.parquet --plans plans.jsonl --seal seal.json \\
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
        required = {"scripts/v158_pq_primary_returned.py",
                    "scripts/v158_check_pq_primary_returned.py",
                    "docs/research/v158-pq-primary-returned-100k-prereg.md"}
        if not required.issubset(tar.getnames()):
            raise ValueError("source archive lacks V158 prereg or evaluator")
    archive_sha = hashlib.sha256(archive).hexdigest()
    archive_key = f"research/v158-pq-primary/{commit}/sources/{archive_sha}.tar.gz"
    prefix = f"research/v158-pq-primary/{commit}/runs/{attempt}"
    session = boto3.Session(profile_name="causality", region_name=REGION)
    ec2, s3 = session.client("ec2"), session.client("s3")
    if not missing(s3, prefix + "/reservation.json") or not missing(s3, prefix + "/terminal.json"):
        raise ValueError("attempt already registered")
    active = ec2.describe_instances(Filters=[
        {"Name": "tag:Name", "Values": [TAG]},
        {"Name": "instance-state-name", "Values": ["pending", "running", "stopping", "stopped"]},
    ])
    if any(item.get("Instances") for item in active["Reservations"]):
        raise ValueError("V158 worker already active")
    for _, key, size, _ in INPUTS:
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
        ClientToken="v158-" + hashlib.sha256(prefix.encode()).hexdigest()[:48],
        ImageId=IMAGE, InstanceType="c7i.8xlarge", MinCount=1, MaxCount=1,
        IamInstanceProfile={"Arn": PROFILE_ARN},
        NetworkInterfaces=[{"AssociatePublicIpAddress": True, "DeviceIndex": 0,
                            "Groups": [SECURITY_GROUP], "SubnetId": SUBNET}],
        InstanceMarketOptions={"MarketType": "spot", "SpotOptions": {
            "InstanceInterruptionBehavior": "terminate", "SpotInstanceType": "one-time"}},
        InstanceInitiatedShutdownBehavior="terminate",
        BlockDeviceMappings=[{"DeviceName": "/dev/xvda", "Ebs": {
            "DeleteOnTermination": True, "Encrypted": True,
            "VolumeSize": 50, "VolumeType": "gp3"}}],
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
                expected = {"plans.jsonl", "seal.json", "raw.jsonl", "summary.json",
                            "check.json", "run.log"}
                if not expected.issubset(terminal.get("artifacts", {})):
                    raise ValueError("complete terminal lacks artifacts")
            ec2.terminate_instances(InstanceIds=[instance_id])
            ec2.get_waiter("instance_terminated").wait(InstanceIds=[instance_id])
            terminal["terminal_sha256"] = hashlib.sha256(raw).hexdigest()
            print(json.dumps(terminal, sort_keys=True), flush=True)
            if terminal.get("status") != "complete":
                raise RuntimeError("V158 terminal reports failed; see closed artifacts")
            return
        state = ec2.describe_instances(InstanceIds=[instance_id])["Reservations"][0]["Instances"][0]["State"]["Name"]
        if state in {"terminated", "shutting-down"}:
            raise RuntimeError("Spot worker stopped before terminal; restart full cell")
        time.sleep(20)
    ec2.terminate_instances(InstanceIds=[instance_id])
    raise TimeoutError("V158 cell exceeded wall cap")


if __name__ == "__main__":
    main()
