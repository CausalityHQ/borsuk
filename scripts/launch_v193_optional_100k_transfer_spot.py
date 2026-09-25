#!/usr/bin/env python3
"""Launch one immutable, paired V193 100k transfer on Causality Spot."""

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
    BUCKET, PROFILE_ARN, REGION, SECURITY_GROUP, SUBNET,
    archive_source, missing, put_if_absent,
)
from scripts.launch_v158_pq_primary_spot import INPUTS as V158_INPUTS
from scripts.launch_v170_pq_field_100k_spot import EARLY as V170_EARLY

SCHEMA = "borsuk-v193-optional-100k-transfer-spot-v1"
TAG = "borsuk-v193-optional-100k-transfer"
WALL_SECONDS = 7200
IMAGE = "ami-06121aa3085b6f918"
V170 = ("research/v170-pq-field-100k/"
        "85c73dde4964779336ad856f7170ffbaa020711d/runs/a0001/")
V192 = ("research/v192-optional-hard-plan-fit/"
        "a22a0d7c6f9cfc71f627bdafa85875750c2048f9/runs/a0001/")
V189 = ("research/v189-predicted-interval-source/"
        "ae0b160e0b48c419a6bac1b6d9e5964f7fd4b63b/runs/a0002/sealed/")
EARLY = V170_EARLY + (
    ("v170-terminal.json", V170 + "terminal.json", 1414,
     "9d20e60e8b6ae115b51347e4360a613df0ff105070cdcc9b3eb07ed6734a81b1"),
    ("v170-plans.jsonl", V170 + "gt-blind/plans.jsonl", 767_470,
     "6695ff985b555fac7865fb027a228ea7af3cbbfb372897887b29f09beeeb98ec"),
    ("v170-plan-seal.json", V170 + "gt-blind/plan-seal.json", 773,
     "d364520f598e9aeb8b4a794e6fe1c7cc29e5d37ae4d9688062b8191b18387848"),
    ("v192-result.json", V192 + "artifacts/out.json", 174_089,
     "b79683695350b4bc21eb4cad14f3588ed5ebaef088a1dd9443b3cc429ef62a01"),
    ("v189-features.jsonl", V189 + "features.jsonl", 15_327_489,
     "7eb4833c76675534cde41330c5939599ab70d7871ac27cb365d62cdb840c3fbe"),
    ("v189-fit-labels.jsonl", V189 + "fit-labels.jsonl", 52_888,
     "4023ade93d32e4aa4377a3f56e7e9b5d469468396e96459715caa5f55394ebf4"),
)
LATE = tuple(item for item in V158_INPUTS
             if item[0] == "truth.parquet") + (
    ("v170-raw.jsonl", V170 + "artifacts/raw.jsonl", 2_300_269,
     "d8081a9feaec74d750393b675819985941a15f409890d788e00ad9b38e3b3894"),
)
ARTIFACTS = (
    "plans.jsonl", "plan-seal.json", "raw.jsonl", "summary.json",
    "plan-resources.txt", "evaluate-resources.txt", "run-closed.log",
)


def _download_script(inputs: tuple) -> str:
    return "\n".join(
        f"aws s3 cp 's3://{BUCKET}/{key}' '{name}' --only-show-errors\n"
        f"[ \"$(stat -c%s '{name}')\" = '{size}' ]\n"
        f"printf '%s  %s\\n' '{digest}' '{name}' | sha256sum -c -"
        for name, key, size, digest in inputs
    )


def _arguments() -> str:
    names = (
        "requests", "reference", "manifest",
        "v113_terminal", "v113_seal", "v113_ids",
        "v113_books", "v113_codes", "v163_terminal",
        "order", "sq8", "v163_plans", "v170_terminal",
        "v170_plans", "v170_plan_seal", "v192_result",
        "v189_features", "v189_fit_labels", "plans",
        "plan_seal", "truth", "v170_raw", "raw", "summary",
    )
    paths = {
        "requests": "requests.jsonl",
        "reference": "reference.jsonl",
        "manifest": "manifest.json",
        "v113_terminal": "v113-terminal.json",
        "v113_seal": "v113-seal.json",
        "v113_ids": "v113-ids.npy",
        "v113_books": "v113-books.npy",
        "v113_codes": "v113-codes.npy",
        "v163_terminal": "v163-terminal.json",
        "order": "order.npy",
        "sq8": "new-sq8.bin",
        "v163_plans": "v163-plans.jsonl",
        "v170_terminal": "v170-terminal.json",
        "v170_plans": "v170-plans.jsonl",
        "v170_plan_seal": "v170-plan-seal.json",
        "v192_result": "v192-result.json",
        "v189_features": "v189-features.jsonl",
        "v189_fit_labels": "v189-fit-labels.jsonl",
        "plans": "plans.jsonl", "plan_seal": "plan-seal.json",
        "truth": "truth.parquet",
        "v170_raw": "v170-raw.jsonl",
        "raw": "raw.jsonl", "summary": "summary.json",
    }
    return " ".join(
        f"--{name.replace('_', '-')} {paths[name]}"
        for name in names
    )


def _user_data(commit: str, archive_sha: str, archive_key: str,
               prefix: str) -> str:
    args = _arguments()
    return f'''#!/bin/bash
set -euo pipefail
systemd-run --unit=v193-hard-stop --on-active={WALL_SECONDS}s /usr/sbin/shutdown -h now
root=/mnt/v193-optional-100k-transfer
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
{_download_script(EARLY)}
phase=plan
/usr/bin/time -v -o plan-resources.txt .venv/bin/python -m scripts.v193_optional_100k_transfer plan {args}
phase=seal
for name in plans.jsonl plan-seal.json; do
  aws s3api put-object --bucket '{BUCKET}' --key '{prefix}/gt-blind/'"$name" --body "$name" --if-none-match '*' --no-cli-pager >/dev/null
done
plan_sha=$(sha256sum plan-seal.json | cut -d ' ' -f1)
phase=late-inputs
{_download_script(LATE)}
phase=evaluate
/usr/bin/time -v -o evaluate-resources.txt .venv/bin/python -m scripts.v193_optional_100k_transfer evaluate {args} --plan-sha256 "$plan_sha"
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
        raise ValueError("V193 source commit is not pushed to origin/main")
    archive = archive_source(commit)
    with tarfile.open(fileobj=io.BytesIO(archive), mode="r:gz") as tar:
        required = {
            "scripts/v193_optional_100k_transfer.py",
            "scripts/optional_rank_utility.py",
            "scripts/hard_priced_interval.py",
            "docs/research/v193-optional-100k-transfer-prereg.md",
        }
        if not required.issubset(tar.getnames()):
            raise ValueError("source archive lacks V193 gate")
    archive_sha = hashlib.sha256(archive).hexdigest()
    archive_key = f"research/v193-optional-100k-transfer/{commit}/sources/{archive_sha}.tar.gz"
    prefix = f"research/v193-optional-100k-transfer/{commit}/runs/{attempt}"
    session = boto3.Session(profile_name="causality", region_name=REGION)
    ec2, s3 = session.client("ec2"), session.client("s3")
    if not missing(s3, prefix + "/reservation.json") or not missing(s3, prefix + "/terminal.json"):
        raise ValueError("attempt already registered")
    active = ec2.describe_instances(Filters=[
        {"Name": "tag:Name", "Values": [TAG]},
        {"Name": "instance-state-name",
         "Values": ["pending", "running", "stopping", "stopped"]},
    ])
    if any(item.get("Instances") for item in active["Reservations"]):
        raise ValueError("V193 worker already active")
    for _, key, size, _ in EARLY + LATE:
        if s3.head_object(Bucket=BUCKET, Key=key)["ContentLength"] != size:
            raise ValueError("V193 frozen input length differs")
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
        ClientToken="v193-" + hashlib.sha256(prefix.encode()).hexdigest()[:48],
        ImageId=IMAGE, InstanceType="c7i.2xlarge", MinCount=1, MaxCount=1,
        IamInstanceProfile={"Arn": PROFILE_ARN},
        NetworkInterfaces=[{"AssociatePublicIpAddress": True, "DeviceIndex": 0,
                            "Groups": [SECURITY_GROUP], "SubnetId": SUBNET}],
        InstanceMarketOptions={"MarketType": "spot", "SpotOptions": {
            "InstanceInterruptionBehavior": "terminate",
            "SpotInstanceType": "one-time"}},
        InstanceInitiatedShutdownBehavior="terminate",
        BlockDeviceMappings=[{"DeviceName": "/dev/xvda", "Ebs": {
            "DeleteOnTermination": True, "Encrypted": True,
            "VolumeSize": 40, "VolumeType": "gp3"}}],
        TagSpecifications=[{"ResourceType": "instance", "Tags": [
            {"Key": "Name", "Value": TAG},
            {"Key": "Project", "Value": "BORSUK"},
            {"Key": "BorsukAttempt", "Value": attempt}]}],
        UserData=_user_data(commit, archive_sha, archive_key, prefix),
    )
    instance_id = receipt["Instances"][0]["InstanceId"]
    print(json.dumps({"instance_id": instance_id, "output_prefix": prefix,
                      "source_commit": commit}), flush=True)
    deadline = time.monotonic() + WALL_SECONDS + 600
    while time.monotonic() < deadline:
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
                raise ValueError("V193 terminal identity differs")
            for name, identity in terminal.get("artifacts", {}).items():
                body = s3.get_object(Bucket=BUCKET,
                    Key=f"{prefix}/artifacts/{name}")["Body"]
                digest = hashlib.sha256()
                size = 0
                while chunk := body.read(1024 * 1024):
                    digest.update(chunk)
                    size += len(chunk)
                if size != identity["bytes"] or digest.hexdigest() != identity["sha256"]:
                    raise ValueError(f"V193 artifact read-back differs: {name}")
            if terminal.get("status") != "complete" or not set(ARTIFACTS).issubset(
                    terminal.get("artifacts", {})):
                raise RuntimeError(f"V193 failed at {terminal.get('phase')}; "
                                   "inspect complete terminal artifacts")
            for name in ("plans.jsonl", "plan-seal.json"):
                body = s3.get_object(Bucket=BUCKET,
                    Key=f"{prefix}/gt-blind/{name}")["Body"]
                digest = hashlib.sha256()
                size = 0
                while chunk := body.read(1024 * 1024):
                    digest.update(chunk)
                    size += len(chunk)
                if (size != terminal["artifacts"][name]["bytes"]
                        or digest.hexdigest()
                            != terminal["artifacts"][name]["sha256"]):
                    raise ValueError(f"V193 GT-blind {name} hash differs")
            print(json.dumps({"status": "complete", "terminal": terminal,
                              "terminal_sha256": hashlib.sha256(raw).hexdigest(),
                              "instance_state": "terminated"}), flush=True)
            return
        state = ec2.describe_instances(InstanceIds=[instance_id])["Reservations"][0][
            "Instances"][0]["State"]["Name"]
        if state in {"terminated", "shutting-down"}:
            raise RuntimeError(f"V193 worker {state} without terminal; "
                               "discard this attempt")
        time.sleep(20)
    ec2.terminate_instances(InstanceIds=[instance_id])
    raise TimeoutError("V193 worker exceeded hard campaign deadline")


if __name__ == "__main__":
    _launch()
