#!/usr/bin/env python3
"""Run the frozen V157 GT-blind primary feasibility cells on Spot."""

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

import boto3
from botocore.exceptions import ClientError

BUCKET = "borsuk-bench-453182569524-euc1"
REGION = "eu-central-1"
TAG = "borsuk-v157-primary-feasibility"
IMAGE = "ami-06121aa3085b6f918"
SUBNET = "subnet-0a12dbed0ca6fac25"
SECURITY_GROUP = "sg-0b1fd3e4fbde4af0d"
PROFILE_ARN = "arn:aws:iam::453182569524:instance-profile/borsuk-bench-profile"
SCHEMA = "borsuk-v157-primary-feasibility-spot-v1"
WALL_SECONDS = 1800
V122 = ("research/v122-deep-image-100k/"
        "afe07cb5a9ba8518263375595f589639fdf3f4f1/"
        "runs/v122-20260924T011355Z/a0001/artifacts/evidence.jsonl")
V116 = ("research/v116-validation-paired/"
        "5e9b35ad40ea023eab4407aa611d759e1893bb34/"
        "runs/v116-validation-20260923T235426Z/a0001/artifacts/rust-replay.jsonl")
INPUTS = (
    ("deep.jsonl", V122, 6_183_526,
     "deac3e5e9d15a54753a1543bed338e31b23daaf3f234f64afabde5e79bcc6ae6"),
    ("relaion.jsonl", V116, 13_455_525,
     "3bfd155ac5f9e1b7aacbc263e1732e2314c9722f235d3454e0f17d0c6bc3c960"),
)


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


def archive_source(commit: str) -> bytes:
    raw = subprocess.run(["git", "archive", "--format=tar", commit],
                         check=True, capture_output=True).stdout
    buffer = io.BytesIO()
    with gzip.GzipFile(filename="", mode="wb", fileobj=buffer, mtime=0) as zipper:
        zipper.write(raw)
    archive = buffer.getvalue()
    with tarfile.open(fileobj=io.BytesIO(archive), mode="r:gz") as tar:
        required = {"scripts/v157_primary_source_feasibility.py",
                    "scripts/v157_check_primary_feasibility.py",
                    "docs/research/v157-primary-source-feasibility-prereg.md"}
        if not required.issubset(tar.getnames()):
            raise ValueError("source archive lacks V157 prereg or evaluator")
    return archive


def user_data(commit: str, archive_sha: str, archive_key: str, prefix: str) -> str:
    downloads = "\n".join(
        f"aws s3 cp 's3://{BUCKET}/{key}' '{name}' --only-show-errors\n"
        f"[ \"$(stat -c%s '{name}')\" = '{size}' ]\n"
        f"printf '%s  %s\\n' '{sha}' '{name}' | sha256sum -c -"
        for name, key, size, sha in INPUTS
    )
    return f'''#!/bin/bash
set -euo pipefail
systemd-run --unit=v157-hard-stop --on-active={WALL_SECONDS}s /usr/sbin/shutdown -h now
root=/mnt/v157-primary-feasibility
mkdir -p "$root" && cd "$root"
phase=bootstrap
finish() {{
  code=$?
  trap - EXIT TERM
  set +e
  cd "$root"
  token=$(curl -fsS -X PUT -H 'X-aws-ec2-metadata-token-ttl-seconds: 60' http://169.254.169.254/latest/api/token)
  instance_id=$(curl -fsS -H "X-aws-ec2-metadata-token: $token" http://169.254.169.254/latest/meta-data/instance-id)
  for name in deep.raw.jsonl deep.summary.json deep.check.json \\
              relaion.raw.jsonl relaion.summary.json relaion.check.json run.log; do
    if [ -f "$name" ]; then
      aws s3 cp "$name" "s3://{BUCKET}/{prefix}/artifacts/$name" --only-show-errors || code=96
    fi
  done
  INSTANCE_ID="$instance_id" EXIT_CODE="$code" PHASE="$phase" python3 - <<'PY' >terminal.json
import hashlib,json,os
from pathlib import Path
names=('deep.raw.jsonl','deep.summary.json','deep.check.json',
       'relaion.raw.jsonl','relaion.summary.json','relaion.check.json','run.log')
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
aws s3 cp 's3://{BUCKET}/{archive_key}' source.tar.gz --only-show-errors
printf '%s  source.tar.gz\\n' '{archive_sha}' | sha256sum -c -
mkdir repo && tar -xzf source.tar.gz -C repo
phase=inputs
{downloads}
phase=deep-100k
python3 repo/scripts/v157_primary_source_feasibility.py \\
  --input deep.jsonl --cohort deep-image-96-angular-random100k-publication-test-9000-9999 \\
  --raw deep.raw.jsonl --summary deep.summary.json >run.log 2>&1
python3 repo/scripts/v157_check_primary_feasibility.py \\
  --input deep.jsonl --cohort deep-image-96-angular-random100k-publication-test-9000-9999 \\
  --raw deep.raw.jsonl --summary deep.summary.json >deep.check.json 2>>run.log
phase=relaion-1m
python3 repo/scripts/v157_primary_source_feasibility.py \\
  --input relaion.jsonl --cohort relaion-1m-validation-1000 \\
  --raw relaion.raw.jsonl --summary relaion.summary.json >>run.log 2>&1
python3 repo/scripts/v157_check_primary_feasibility.py \\
  --input relaion.jsonl --cohort relaion-1m-validation-1000 \\
  --raw relaion.raw.jsonl --summary relaion.summary.json >relaion.check.json 2>>run.log
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
    archive_sha = hashlib.sha256(archive).hexdigest()
    archive_key = f"research/v157-primary-feasibility/{commit}/sources/{archive_sha}.tar.gz"
    prefix = f"research/v157-primary-feasibility/{commit}/runs/{attempt}"
    session = boto3.Session(profile_name="causality", region_name=REGION)
    ec2, s3 = session.client("ec2"), session.client("s3")
    if not missing(s3, prefix + "/reservation.json") or not missing(s3, prefix + "/terminal.json"):
        raise ValueError("attempt already registered")
    active = ec2.describe_instances(Filters=[
        {"Name": "tag:Name", "Values": [TAG]},
        {"Name": "instance-state-name", "Values": ["pending", "running", "stopping", "stopped"]},
    ])
    if any(item.get("Instances") for item in active["Reservations"]):
        raise ValueError("V157 worker already active")
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
        ClientToken="v157-" + hashlib.sha256(prefix.encode()).hexdigest()[:48],
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
                expected = {"deep.raw.jsonl", "deep.summary.json", "deep.check.json",
                            "relaion.raw.jsonl", "relaion.summary.json", "relaion.check.json",
                            "run.log"}
                if not expected.issubset(terminal.get("artifacts", {})):
                    raise ValueError("complete terminal lacks artifacts")
            ec2.terminate_instances(InstanceIds=[instance_id])
            ec2.get_waiter("instance_terminated").wait(InstanceIds=[instance_id])
            terminal["terminal_sha256"] = hashlib.sha256(raw).hexdigest()
            print(json.dumps(terminal, sort_keys=True), flush=True)
            return
        state = ec2.describe_instances(InstanceIds=[instance_id])["Reservations"][0]["Instances"][0]["State"]["Name"]
        if state in {"terminated", "shutting-down"}:
            raise RuntimeError("Spot worker stopped before terminal; restart full cell")
        time.sleep(20)
    ec2.terminate_instances(InstanceIds=[instance_id])
    raise TimeoutError("V157 cell exceeded wall cap")


if __name__ == "__main__":
    main()
