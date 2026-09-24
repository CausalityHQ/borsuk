#!/usr/bin/env python3
"""Run one immutable V170 paired 100k field-transfer cell on Spot."""

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

SCHEMA = "borsuk-v170-pq-field-100k-spot-v1"
TAG = "borsuk-v170-pq-field-100k"
WALL_SECONDS = 7_200
V113 = ("research/v113-resident-nominee/"
        "9f4936fbb9bad5596921ff71004984f9b295bc57/"
        "runs/v113-100k-score-20260923T203231Z/a0001/")
V163 = ("research/v163-smooth-layout/"
        "134613f2d6ac206eed2af5dce94544901eb51712/runs/a0001/")
EARLY = tuple(item for item in V158_INPUTS if item[0] in
              {"requests.jsonl", "reference.jsonl", "manifest.json"}) + (
    ("v113-terminal.json", V113 + "terminal.json", 2941,
     "a578790e1b8479732d1836d89cdf3d5af87df5aa19e444c72a8ebd2eca1255cf"),
    ("v113-seal.json", V113 + "artifacts/artifact/seal.json", 2135,
     "803b00d9366bc8feb0e58e4900e27b1973c5d9578c33a0d6007fe4c28722c7b2"),
    ("v113-ids.npy", V113 + "artifacts/artifact/ids.npy", 800128,
     "d31121d0ecd43bad93bf7e313bc467f158de24510989400cb9fd7dd667c2a57a"),
    ("v113-books.npy", V113 + "artifacts/artifact/pq_books.npy", 786560,
     "e90c011aa3a013bed606d7f40b30bd41f5ed446637ac9c8724ced6cf2f078c87"),
    ("v113-codes.npy", V113 + "artifacts/artifact/pq_codes.npy", 6400128,
     "2eea1c265da723e799575b98ba29997eca9d58ef789a5244a96ca3bf89f6638f"),
    ("v163-terminal.json", V163 + "terminal.json", 1706,
     "f4d2bd82d1ac4a7c7c52e03f1496c6a44ce77f619e0878d0e73577aaa6f4828f"),
    ("order.npy", V163 + "artifacts/order.npy", 800128,
     "d7be74b09ade0a7477b62c2e14d68b94640dede5ac38be240e6e7428d41ac6e6"),
    ("new-sq8.bin", V163 + "artifacts/new-sq8.bin", 78000000,
     "76d325d20dd38063bb050f83cfa693f7748921c75280cb1bf1f988041329dd38"),
    ("v163-plans.jsonl", V163 + "artifacts/plans.jsonl", 397910,
     "713511887fb7f853250ca073a40f432fb8fbb47471c0338e6c85468bcc30fecd"),
)
LATE = tuple(item for item in V158_INPUTS if item[0] == "truth.parquet") + (
    ("v163-raw.jsonl", V163 + "artifacts/raw.jsonl", 1203637,
     "a7656420c699b41a16ebf9124dae47ed81de977c6fb300128034d5503f6b16f1"),
)
ARTIFACTS = ("plans.jsonl", "plan-seal.json", "raw.jsonl", "summary.json",
             "check.json", "plan-resources.txt", "reduce-resources.txt",
             "check-resources.txt", "run.log")


def download_script(inputs: tuple) -> str:
    return "\n".join(
        f"aws s3 cp 's3://{BUCKET}/{key}' '{name}' --only-show-errors\n"
        f"[ \"$(stat -c%s '{name}')\" = '{size}' ]\n"
        f"printf '%s  %s\\n' '{digest}' '{name}' | sha256sum -c -"
        for name, key, size, digest in inputs
    )


def user_data(commit: str, archive_sha: str, archive_key: str, prefix: str) -> str:
    arguments = " ".join(
        f"--{field.replace('_', '-')} {name}"
        for field, name in (
            ("requests", "requests.jsonl"), ("reference", "reference.jsonl"),
            ("manifest", "manifest.json"),
            ("v113_terminal", "v113-terminal.json"),
            ("v113_seal", "v113-seal.json"),
            ("v113_ids", "v113-ids.npy"), ("v113_books", "v113-books.npy"),
            ("v113_codes", "v113-codes.npy"),
            ("v163_terminal", "v163-terminal.json"),
            ("order", "order.npy"), ("sq8", "new-sq8.bin"),
            ("v163_plans", "v163-plans.jsonl"),
            ("plans", "plans.jsonl"), ("plan_seal", "plan-seal.json"),
            ("truth", "truth.parquet"), ("v163_raw", "v163-raw.jsonl"),
            ("raw", "raw.jsonl"), ("summary", "summary.json"),
        )
    )
    return f'''#!/bin/bash
set -euo pipefail
systemd-run --unit=v170-hard-stop --on-active={WALL_SECONDS}s /usr/sbin/shutdown -h now
root=/mnt/v170-pq-field
mkdir -p "$root" && cd "$root"
phase=bootstrap
finish() {{
  code=$?
  trap - EXIT TERM
  set +e
  cd "$root"
  for name in {' '.join(ARTIFACTS)}; do
    if [ -f "$name" ]; then
      aws s3 cp "$name" "s3://{BUCKET}/{prefix}/artifacts/$name" --only-show-errors || code=96
    fi
  done
  token=$(curl -fsS -X PUT -H 'X-aws-ec2-metadata-token-ttl-seconds: 60' http://169.254.169.254/latest/api/token)
  instance_id=$(curl -fsS -H "X-aws-ec2-metadata-token: $token" http://169.254.169.254/latest/meta-data/instance-id)
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
    'gt_blind_prefix':'s3://{BUCKET}/{prefix}/gt-blind/',
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
{download_script(EARLY)}
phase=plan
/usr/bin/time -v .venv/bin/python repo/scripts/v170_pq_field_100k.py plan {arguments} >plan-resources.txt 2>&1
phase=seal
for name in plans.jsonl plan-seal.json; do
  aws s3api put-object --bucket '{BUCKET}' --key '{prefix}/gt-blind/'"$name" \\
    --body "$name" --if-none-match '*' --no-cli-pager >/dev/null
done
phase=late-inputs
{download_script(LATE)}
phase=reduce
/usr/bin/time -v .venv/bin/python repo/scripts/v170_pq_field_100k.py reduce {arguments} >reduce-resources.txt 2>&1
phase=check
/usr/bin/time -v .venv/bin/python repo/scripts/v170_pq_field_100k.py check {arguments} >check.json 2>check-resources.txt
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
    subprocess.run(["git", "fetch", "origin", "main"], check=True,
                   stdout=subprocess.DEVNULL)
    commit = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    subprocess.run(["git", "merge-base", "--is-ancestor", commit, "origin/main"],
                   check=True)
    archive = archive_source(commit)
    with tarfile.open(fileobj=io.BytesIO(archive), mode="r:gz") as tar:
        required = {"scripts/v170_pq_field_100k.py",
                    "docs/research/v170-pq-field-100k-transfer-prereg.md"}
        if not required.issubset(tar.getnames()):
            raise ValueError("source archive lacks V170 gate")
    archive_sha = hashlib.sha256(archive).hexdigest()
    archive_key = f"research/v170-pq-field-100k/{commit}/sources/{archive_sha}.tar.gz"
    prefix = f"research/v170-pq-field-100k/{commit}/runs/{attempt}"
    session = boto3.Session(profile_name="causality", region_name=REGION)
    ec2, s3 = session.client("ec2"), session.client("s3")
    if not missing(s3, prefix + "/reservation.json") or not missing(s3, prefix + "/terminal.json"):
        raise ValueError("attempt already registered")
    active = ec2.describe_instances(Filters=[
        {"Name": "tag:Name", "Values": [TAG]},
        {"Name": "instance-state-name", "Values": ["pending", "running", "stopping", "stopped"]},
    ])
    if any(item.get("Instances") for item in active["Reservations"]):
        raise ValueError("V170 worker already active")
    for _, key, size, _ in EARLY + LATE:
        if s3.head_object(Bucket=BUCKET, Key=key)["ContentLength"] != size:
            raise ValueError("frozen input length differs")
    if missing(s3, archive_key):
        put_if_absent(archive_key, archive)
    elif s3.head_object(Bucket=BUCKET, Key=archive_key)["ContentLength"] != len(archive):
        raise ValueError("source archive length differs")
    put_if_absent(prefix + "/reservation.json", json.dumps({
        "schema": SCHEMA, "source_commit": commit,
        "source_archive_sha256": archive_sha,
        "interruption_policy": "discard cell and restart with a new attempt",
        "output_prefix": f"s3://{BUCKET}/{prefix}",
    }, sort_keys=True).encode())
    receipt = ec2.run_instances(
        ClientToken="v170-" + hashlib.sha256(prefix.encode()).hexdigest()[:48],
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
            ec2.terminate_instances(InstanceIds=[instance_id])
            ec2.get_waiter("instance_terminated").wait(InstanceIds=[instance_id])
            terminal = json.loads(raw)
            if (terminal.get("schema") != SCHEMA or terminal.get("instance_id") != instance_id
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
                    raise ValueError(f"V170 S3 artifact differs: {name}")
            if terminal.get("status") == "complete":
                if terminal.get("gt_blind_prefix") != f"s3://{BUCKET}/{prefix}/gt-blind/":
                    raise ValueError("V170 GT-blind S3 prefix differs")
                for name in ("plans.jsonl", "plan-seal.json"):
                    body = s3.get_object(Bucket=BUCKET,
                        Key=f"{prefix}/gt-blind/{name}")["Body"]
                    digest = hashlib.sha256()
                    size = 0
                    while chunk := body.read(1024 * 1024):
                        digest.update(chunk)
                        size += len(chunk)
                    identity = terminal["artifacts"][name]
                    if size != identity["bytes"] or digest.hexdigest() != identity["sha256"]:
                        raise ValueError(f"V170 GT-blind S3 seal differs: {name}")
            terminal["terminal_sha256"] = hashlib.sha256(raw).hexdigest()
            print(json.dumps(terminal, sort_keys=True), flush=True)
            if terminal.get("status") != "complete":
                raise RuntimeError("V170 terminal reports failed; see closed artifacts")
            return
        state = ec2.describe_instances(InstanceIds=[instance_id])["Reservations"][0]["Instances"][0]["State"]["Name"]
        if state in {"terminated", "shutting-down"}:
            raise RuntimeError("V170 Spot worker stopped before terminal; restart full cell")
        time.sleep(20)
    ec2.terminate_instances(InstanceIds=[instance_id])
    raise TimeoutError("V170 cell exceeded wall cap")


if __name__ == "__main__":
    main()
