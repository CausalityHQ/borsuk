#!/usr/bin/env python3
"""One immutable CoHere-100k source-only graph transfer gate on Causality Spot."""

import argparse
import base64
import fcntl
import hashlib
import json
import subprocess
import time

import boto3
from botocore.exceptions import ClientError

from scripts.launch_v157_primary_feasibility_spot import (
    BUCKET, PROFILE_ARN, REGION, SECURITY_GROUP, SUBNET,
    archive_source, missing, put_if_absent,
)

SCHEMA = "borsuk-v248-cohere-transfer-100k-spot-v1"
IMAGE = "ami-06121aa3085b6f918"
WALL_SECONDS = 10_800
SOURCE = "publication/v3/20260812/datasets/cohere-large-10m-768/attempts/0001/"
RECEIPT_SHA = "0965aa0241199822dfac3410bba4edad5536ac0eb0aaa8ab83c216e8c5749a87"
TEST_SHA = "5e0123f163df0e53a7e329fd92fbfd49f079756acfb47387ee6664c267b6f94e"
ARTIFACTS = ("prep.json", "build.json", "serving.json", "quality.json",
             "requests.json", "requests.jsonl", "raw.jsonl", "truth.u32", "graph.bin",
             "vectors.raw", "plane.bin", "books.bin", "codes.bin", "map.u32",
             "prep-resources.txt", "graph-resources.txt", "serving-resources.txt",
             "truth-resources.txt", "build.log", "install.log", "run-closed.log")


def worker(commit, archive_sha, archive_key, prefix):
    script = r'''#!/bin/bash
set -euo pipefail
systemd-run --unit=v248-hard-stop --on-active=10800s /usr/sbin/shutdown -h now
root=/mnt/v248-cohere-100k
mkdir -p "$root" && cd "$root"
phase=bootstrap
finish() {
  code=$?
  trap - EXIT TERM
  set +e
  cd "$root"
  cp run.log run-closed.log || code=96
  for name in @@ARTIFACTS@@; do
    [ ! -f "$name" ] || aws s3 cp "$name" "s3://@@BUCKET@@/@@PREFIX@@/artifacts/$name" --only-show-errors || code=96
  done
  token=$(curl -fsS -X PUT -H 'X-aws-ec2-metadata-token-ttl-seconds: 60' http://169.254.169.254/latest/api/token)
  instance_id=$(curl -fsS -H "X-aws-ec2-metadata-token: $token" http://169.254.169.254/latest/meta-data/instance-id)
  INSTANCE_ID="$instance_id" EXIT_CODE="$code" PHASE="$phase" python3 - <<'PY' >terminal.json
import hashlib,json,os
from pathlib import Path
artifacts={}
for name in @@ARTIFACTS_PY@@:
    path=Path(name)
    if path.is_file():
        value=hashlib.sha256()
        with path.open('rb') as source:
            for block in iter(lambda:source.read(1024*1024),b''):
                value.update(block)
        artifacts[name]={'bytes':path.stat().st_size,
                         'sha256':value.hexdigest()}
code=int(os.environ['EXIT_CODE']);phase=os.environ['PHASE']
print(json.dumps({'schema':'@@SCHEMA@@','source_commit':'@@COMMIT@@',
    'source_archive_sha256':'@@ARCHIVE_SHA@@','instance_id':os.environ['INSTANCE_ID'],
    'exit_code':code,'phase':phase,
    'status':'complete' if code==0 and phase=='complete' else 'failed',
    'artifacts':artifacts},sort_keys=True,separators=(',',':')))
PY
  aws s3 cp terminal.json "s3://@@BUCKET@@/@@PREFIX@@/terminal.json" --only-show-errors
  shutdown -h now || true
  exit "$code"
}
trap finish EXIT
trap 'exit 97' TERM
exec >run.log 2>&1
aws s3 cp 's3://@@BUCKET@@/@@ARCHIVE_KEY@@' source.tar.gz --only-show-errors
printf '%s  source.tar.gz\n' '@@ARCHIVE_SHA@@' | sha256sum -c -
mkdir repo && tar -xzf source.tar.gz -C repo
phase=install
dnf install -y -q python3.12 python3.12-pip gcc gcc-c++ cmake perl tar gzip time >install.log 2>&1
python3.12 -m venv .venv
.venv/bin/pip install -q numpy==2.3.3 pyarrow==24.0.0 >>install.log 2>&1
export RUSTUP_HOME="$root/.rustup" CARGO_HOME="$root/.cargo" CARGO_TARGET_DIR="$root/target" CARGO_BUILD_JOBS=6
curl -fsSL https://sh.rustup.rs | sh -s -- -y --profile minimal --default-toolchain 1.98.0 >>install.log 2>&1
export PYTHONPATH="$root/repo" OPENBLAS_NUM_THREADS=8 OMP_NUM_THREADS=8 MKL_NUM_THREADS=8
phase=inputs
aws s3 cp 's3://@@BUCKET@@/@@SOURCE@@/STAGING_COMPLETE.json' receipt.json --only-show-errors
printf '%s  receipt.json\n' '@@RECEIPT_SHA@@' | sha256sum -c -
for n in 0 1 2 3 4; do
  name=$(printf 'train-%08d.parquet' "$n")
  aws s3 cp "s3://@@BUCKET@@/@@SOURCE@@/materialized/$name" "$name" --only-show-errors
done
phase=prepare
/usr/bin/time -v -o prep-resources.txt .venv/bin/python -m scripts.v248_prepare_cohere_graph \
  --receipt receipt.json --receipt-sha256 '@@RECEIPT_SHA@@' \
  --train train-0000000{0,1,2,3,4}.parquet --rows 100000 --generation 248 --output prepared
cp prepared/* .
phase=compile
cd repo
"$CARGO_HOME/bin/cargo" build --release --locked -p borsuk \
  --bin v248_build_cohere_graph_100k --bin v248_serve_cohere_graph_100k \
  --jobs 6 >"$root/build.log" 2>&1
cd "$root"
phase=graph
/usr/bin/time -v -o graph-resources.txt "$CARGO_TARGET_DIR/release/v248_build_cohere_graph_100k" \
  prep.json plane.bin vectors.raw graph.bin build.json
phase=queries
aws s3 cp 's3://@@BUCKET@@/@@SOURCE@@/materialized/test.parquet' test.parquet --only-show-errors
printf '%s  test.parquet\n' '@@TEST_SHA@@' | sha256sum -c -
.venv/bin/python -m scripts.v248_score_cohere_graph requests \
  --test test.parquet --request-file requests.jsonl --output requests.json
phase=serve
/usr/bin/time -v -o serving-resources.txt "$CARGO_TARGET_DIR/release/v248_serve_cohere_graph_100k" \
  prep.json build.json plane.bin graph.bin map.u32 books.bin codes.bin \
  requests.jsonl raw.jsonl serving.json
aws s3api put-object --bucket '@@BUCKET@@' --key '@@PREFIX@@/sealed/raw.jsonl' \
  --body raw.jsonl --if-none-match '*' --no-cli-pager >/dev/null
raw_sha=$(sha256sum raw.jsonl | cut -d ' ' -f1)
remote_sha=$(aws s3 cp 's3://@@BUCKET@@/@@PREFIX@@/sealed/raw.jsonl' - \
  --only-show-errors | sha256sum | cut -d ' ' -f1)
[ "$raw_sha" = "$remote_sha" ]
phase=truth
/usr/bin/time -v -o truth-resources.txt .venv/bin/python -m scripts.v248_score_cohere_graph score \
  --source vectors.raw --prep prep.json --request-file requests.jsonl \
  --raw-file raw.jsonl --serving-file serving.json --truth-output truth.u32 \
  --output quality.json
phase=complete
'''
    for key, value in {"ARTIFACTS": " ".join(ARTIFACTS), "ARTIFACTS_PY": repr(ARTIFACTS),
                       "SCHEMA": SCHEMA, "BUCKET": BUCKET, "PREFIX": prefix,
                       "COMMIT": commit, "ARCHIVE_SHA": archive_sha,
                       "ARCHIVE_KEY": archive_key, "SOURCE": SOURCE.rstrip("/"),
                       "RECEIPT_SHA": RECEIPT_SHA, "TEST_SHA": TEST_SHA}.items():
        script = script.replace("@@" + key + "@@", value)
    if "@@" in script:
        raise ValueError("unresolved V248 worker placeholder")
    return script


def launch(attempt):
    if len(attempt) != 5 or not attempt.startswith("a") or not attempt[1:].isdigit():
        raise ValueError("attempt must be aNNNN")
    if subprocess.check_output(["git", "status", "--porcelain"], text=True).strip():
        raise ValueError("worktree must be clean")
    commit = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    subprocess.run(["git", "fetch", "origin", "main"], check=True)
    if subprocess.run(["git", "merge-base", "--is-ancestor", "origin/main", "HEAD"],
                      check=False).returncode:
        raise ValueError("source is not a fast-forward descendant of origin/main")
    archive = archive_source(commit)
    archive_sha = hashlib.sha256(archive).hexdigest()
    archive_key = f"research/v248-cohere-transfer-100k/{commit}/sources/{archive_sha}.tar.gz"
    prefix = f"research/v248-cohere-transfer-100k/{commit}/runs/{attempt}"
    session = boto3.Session(profile_name="causality", region_name=REGION)
    ec2, s3 = session.client("ec2"), session.client("s3")
    if not missing(s3, prefix + "/reservation.json") or not missing(s3, prefix + "/terminal.json"):
        raise ValueError("attempt already registered")
    active = ec2.describe_instances(Filters=[
        {"Name": "tag:Name", "Values": ["borsuk-*", "v24*"]},
        {"Name": "instance-state-name", "Values": ["pending", "running", "stopping"]},
    ])
    if any(row.get("Instances") for row in active["Reservations"]):
        raise ValueError("another BORSUK worker is active")
    if missing(s3, archive_key):
        put_if_absent(archive_key, archive)
    elif s3.head_object(Bucket=BUCKET, Key=archive_key)["ContentLength"] != len(archive):
        raise ValueError("source archive length differs")
    put_if_absent(prefix + "/reservation.json", json.dumps({
        "schema": SCHEMA, "source_commit": commit,
        "source_archive_sha256": archive_sha, "staging_receipt_sha256": RECEIPT_SHA,
        "test_sha256": TEST_SHA, "dataset": "CoHere-large-10M first 100k D768 cosine",
        "split": "development 0-255; validation 256-999 prior used",
        "construction": {"m": 32, "m0": 64, "ef_construction": 128,
                         "workers": 8, "source_only_pq64": True},
        "arms": [[2048, 2048], [4096, 4096], [8192, 8192]],
        "interruption_policy": "discard interrupted cell, restart under a new attempt",
        "output_prefix": f"s3://{BUCKET}/{prefix}",
    }, sort_keys=True).encode())
    receipt = ec2.run_instances(
        ClientToken="v248-" + hashlib.sha256(prefix.encode()).hexdigest()[:48],
        ImageId=IMAGE, InstanceType="c7i.4xlarge", MinCount=1, MaxCount=1,
        IamInstanceProfile={"Arn": PROFILE_ARN},
        NetworkInterfaces=[{"AssociatePublicIpAddress": True, "DeviceIndex": 0,
                            "Groups": [SECURITY_GROUP], "SubnetId": SUBNET}],
        InstanceMarketOptions={"MarketType": "spot", "SpotOptions": {
            "InstanceInterruptionBehavior": "terminate", "SpotInstanceType": "one-time"}},
        InstanceInitiatedShutdownBehavior="terminate",
        BlockDeviceMappings=[{"DeviceName": "/dev/xvda", "Ebs": {
            "DeleteOnTermination": True, "Encrypted": True, "VolumeSize": 30,
            "VolumeType": "gp3"}}],
        TagSpecifications=[{"ResourceType": "instance", "Tags": [
            {"Key": "Name", "Value": "borsuk-v248-cohere-100k"},
            {"Key": "BorsukAttempt", "Value": attempt}]}],
        UserData=base64.b64encode(worker(commit, archive_sha, archive_key, prefix).encode()).decode(),
    )
    instance_id = receipt["Instances"][0]["InstanceId"]
    print(json.dumps({"instance_id": instance_id, "output_prefix": prefix,
                      "source_commit": commit, "source_archive_sha256": archive_sha}), flush=True)
    try:
        deadline = time.monotonic() + WALL_SECONDS + 600
        while time.monotonic() < deadline:
            try:
                raw = s3.get_object(Bucket=BUCKET, Key=prefix + "/terminal.json")["Body"].read()
                terminal = json.loads(raw)
                ec2.terminate_instances(InstanceIds=[instance_id])
                ec2.get_waiter("instance_terminated").wait(InstanceIds=[instance_id])
                if (terminal.get("schema") != SCHEMA or terminal.get("instance_id") != instance_id
                        or terminal.get("source_commit") != commit
                        or terminal.get("source_archive_sha256") != archive_sha):
                    raise ValueError("V248 terminal identity differs")
                terminal["terminal_sha256"] = hashlib.sha256(raw).hexdigest()
                print(json.dumps(terminal, sort_keys=True), flush=True)
                if terminal.get("status") != "complete":
                    raise RuntimeError("V248 failed; inspect only closed terminal artifacts")
                if set(terminal.get("artifacts", {})) != set(ARTIFACTS):
                    raise ValueError("V248 artifact roster differs")
                for name, identity in terminal["artifacts"].items():
                    body = s3.get_object(Bucket=BUCKET,
                        Key=f"{prefix}/artifacts/{name}")["Body"]
                    value = hashlib.sha256()
                    size = 0
                    while chunk := body.read(4 * 1024 * 1024):
                        value.update(chunk)
                        size += len(chunk)
                    if size != identity["bytes"] or value.hexdigest() != identity["sha256"]:
                        raise ValueError(f"V248 artifact readback differs: {name}")
                print(json.dumps({"instance_id": instance_id,
                                  "final_state": "terminated", "artifact_replay": "pass"}), flush=True)
                return
            except ClientError as error:
                if error.response.get("Error", {}).get("Code") not in {"NoSuchKey", "404", "NotFound"}:
                    raise
            state = ec2.describe_instances(InstanceIds=[instance_id])["Reservations"][0]["Instances"][0]["State"]["Name"]
            if state in {"terminated", "shutting-down"}:
                raise RuntimeError("V248 worker stopped before terminal")
            time.sleep(20)
        raise TimeoutError("V248 Spot cell exceeded wall cap")
    finally:
        try:
            state = ec2.describe_instances(InstanceIds=[instance_id])["Reservations"][0]["Instances"][0]["State"]["Name"]
            if state not in {"terminated", "shutting-down"}:
                ec2.terminate_instances(InstanceIds=[instance_id])
        except Exception:
            pass


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--attempt", default="a0001")
    args = parser.parse_args()
    with open("/tmp/borsuk-v248-cohere-launch.lock", "a+") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        launch(args.attempt)
