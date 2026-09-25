#!/usr/bin/env python3
"""Frozen ReLAION-1M reachable graph/PQ-cosine/FP16 Spot validation."""

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
from scripts.launch_v195_used_1m_rerank_diagnostic_spot import INPUTS as V195_INPUTS
from scripts.launch_v198_real_query_resident_fp16_spot import EXTRA_INPUTS as V198_INPUTS
from scripts.launch_v209_resident_1m_spot import BASELINE

SCHEMA = "borsuk-v219-reachable-graph-1m-spot-v1"
TAG = "borsuk-v219-reachable-graph-1m"
WALL_SECONDS = 10_800
IMAGE = "ami-06121aa3085b6f918"
QUALIFYING_PREFIX = ("research/v218-reachable-graph-100k/"
                     "ce317cac8d1eb0a1b8a8610f0a3756b96090514c/runs/a0001/")
QUALIFYING_TERMINAL_SHA = "cf44ecb95a9d1cc07ac895a222a407c37db090c499f15a887e887d74c8492efd"
BASE = tuple(row for row in V195_INPUTS if row[0] in {
    "source.parquet", "old-layout.npy", "order.npy", "router/books.bin", "router/codes.bin"})
INPUTS = tuple(("books.bin" if row[0] == "router/books.bin" else
                "codes.bin" if row[0] == "router/codes.bin" else row[0],
                row[1], row[2], row[3]) for row in BASE) + tuple(
    row for row in V198_INPUTS if row[0] in {"plane.bin", "requests.jsonl"})
if {row[0] for row in INPUTS} != {"source.parquet", "old-layout.npy", "order.npy",
                                  "books.bin", "codes.bin", "plane.bin", "requests.jsonl"}:
    raise RuntimeError("V219 frozen input roster differs")
LATE = (BASELINE,)
ARTIFACTS = ("prep.json", "map.u32", "graph.bin", "build-summary.json",
             "serving.json", "raw.jsonl", "quality.json", "prepare-resources.txt",
             "graph-build-resources.txt", "serving-resources.txt",
             "build.log", "install.log", "run-closed.log")


def downloads(rows: tuple) -> str:
    return "\n".join(
        f"aws s3 cp 's3://{BUCKET}/{key}' '{name}' --only-show-errors\n"
        f"[ \"$(stat -c%s '{name}')\" = '{size}' ]\n"
        f"printf '%s  %s\\n' '{digest}' '{name}' | sha256sum -c -"
        for name, key, size, digest in rows
    )


def user_data(commit: str, archive_sha: str, archive_key: str, prefix: str) -> str:
    template = r'''#!/bin/bash
set -euo pipefail
systemd-run --unit=v219-hard-stop --on-active=@@WALL@@s /usr/sbin/shutdown -h now
root=/mnt/v219-reachable-graph-1m
mkdir -p "$root" && cd "$root"
phase=bootstrap
finish() {
  code=$?
  trap - EXIT TERM
  set +e
  cd "$root"
  cp run.log run-closed.log || code=96
  for name in @@ARTIFACTS@@; do
    if [ -f "$name" ]; then
      aws s3 cp "$name" "s3://@@BUCKET@@/@@PREFIX@@/artifacts/$name" --only-show-errors || code=96
    fi
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
            for block in iter(lambda: source.read(1024*1024),b''):
                value.update(block)
        artifacts[name]={'bytes':path.stat().st_size,'sha256':value.hexdigest()}
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
.venv/bin/pip install -q numpy==1.26.4 pyarrow==17.0.0 >>install.log 2>&1
export RUSTUP_HOME="$root/.rustup" CARGO_HOME="$root/.cargo" CARGO_TARGET_DIR="$root/target" CARGO_BUILD_JOBS=6
curl -fsSL https://sh.rustup.rs | sh -s -- -y --profile minimal --default-toolchain 1.98.0 >>install.log 2>&1
export PYTHONPATH="$root/repo" OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1
phase=inputs
@@DOWNLOADS@@
phase=prepare
/usr/bin/time -v -o prepare-resources.txt .venv/bin/python -m scripts.v217_prepare_graph_1m \
  --source source.parquet --old-layout old-layout.npy --order order.npy \
  --plane plane.bin --books books.bin --codes codes.bin --requests requests.jsonl \
  --vectors vectors.raw --map map.u32 --summary prep.json
phase=build
cd repo
"$CARGO_HOME/bin/cargo" build --release --locked -p borsuk \
  --bin v219_build_reachable_graph_1m --bin v219_serve_reachable_graph_1m \
  --jobs 6 >"$root/build.log" 2>&1
cd "$root"
/usr/bin/time -v -o graph-build-resources.txt "$CARGO_TARGET_DIR/release/v219_build_reachable_graph_1m" \
  prep.json plane.bin vectors.raw graph.bin build-summary.json
phase=serve
/usr/bin/time -v -o serving-resources.txt "$CARGO_TARGET_DIR/release/v219_serve_reachable_graph_1m" \
  prep.json build-summary.json plane.bin graph.bin map.u32 books.bin codes.bin \
  requests.jsonl raw.jsonl serving.json
aws s3api put-object --bucket '@@BUCKET@@' --key '@@PREFIX@@/sealed/raw.jsonl' \
  --body raw.jsonl --if-none-match '*' --no-cli-pager >/dev/null
raw_sha=$(sha256sum raw.jsonl | cut -d ' ' -f1)
remote_sha=$(aws s3 cp 's3://@@BUCKET@@/@@PREFIX@@/sealed/raw.jsonl' - \
  --only-show-errors | sha256sum | cut -d ' ' -f1)
[ "$raw_sha" = "$remote_sha" ]
phase=truth
@@LATE_DOWNLOADS@@
.venv/bin/python -m scripts.v219_score_reachable_graph_1m \
  --serving serving.json --raw raw.jsonl --baseline v198-raw.jsonl \
  --output quality.json
phase=complete
'''
    replacements = {
        "WALL": str(WALL_SECONDS), "ARTIFACTS": " ".join(ARTIFACTS),
        "ARTIFACTS_PY": repr(ARTIFACTS), "BUCKET": BUCKET,
        "PREFIX": prefix, "SCHEMA": SCHEMA, "COMMIT": commit,
        "ARCHIVE_SHA": archive_sha, "ARCHIVE_KEY": archive_key,
        "DOWNLOADS": downloads(INPUTS), "LATE_DOWNLOADS": downloads(LATE),
    }
    for name, value in replacements.items():
        template = template.replace("@@" + name + "@@", value)
    if "@@" in template:
        raise ValueError("unresolved V219 worker placeholder")
    return template


def launch(attempt: str) -> None:
    if len(attempt) != 5 or not attempt.startswith("a") or not attempt[1:].isdigit():
        raise ValueError("attempt must be aNNNN")
    if subprocess.check_output(["git", "status", "--porcelain"], text=True).strip():
        raise ValueError("worktree must be clean")
    commit = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    subprocess.run(["git", "fetch", "origin", "main"], check=True)
    if subprocess.run(["git", "merge-base", "--is-ancestor", "origin/main", "HEAD"],
                      check=False).returncode:
        raise ValueError("source is not fast-forward descendant of origin/main")
    archive = archive_source(commit)
    archive_sha = hashlib.sha256(archive).hexdigest()
    archive_key = f"research/v219-reachable-graph-1m/{commit}/sources/{archive_sha}.tar.gz"
    prefix = f"research/v219-reachable-graph-1m/{commit}/runs/{attempt}"
    session = boto3.Session(profile_name="causality", region_name=REGION)
    ec2, s3 = session.client("ec2"), session.client("s3")
    qualifying_raw = s3.get_object(Bucket=BUCKET,
        Key=QUALIFYING_PREFIX + "terminal.json")["Body"].read()
    qualifying = json.loads(qualifying_raw)
    if (hashlib.sha256(qualifying_raw).hexdigest() != QUALIFYING_TERMINAL_SHA
            or qualifying.get("status") != "complete"):
        raise ValueError("V218 qualifying terminal differs")
    for artifact, field in (("quality.json", "internal_gate_pass"),
                            ("build-summary.json", "structure")):
        body = s3.get_object(Bucket=BUCKET,
            Key=QUALIFYING_PREFIX + "artifacts/" + artifact)["Body"].read()
        if (hashlib.sha256(body).hexdigest()
                != qualifying["artifacts"][artifact]["sha256"]):
            raise ValueError("V218 qualifying artifact differs")
        value = json.loads(body)[field]
        if artifact == "quality.json" and value is not True:
            raise ValueError("V218 quality gate did not pass")
        if artifact == "build-summary.json" and (
                value["reachable"] != 100_000 or value["min_indegree"] < 4):
            raise ValueError("V218 structural gate did not pass")
    if not missing(s3, prefix + "/reservation.json") or not missing(s3, prefix + "/terminal.json"):
        raise ValueError("attempt already registered")
    active = ec2.describe_instances(Filters=[
        {"Name": "tag:Name", "Values": ["borsuk-*"]},
        {"Name": "instance-state-name", "Values": ["pending", "running", "stopping"]},
    ])
    if any(item.get("Instances") for item in active["Reservations"]):
        raise ValueError("another BORSUK worker is active")
    if missing(s3, archive_key):
        put_if_absent(archive_key, archive)
    elif s3.head_object(Bucket=BUCKET, Key=archive_key)["ContentLength"] != len(archive):
        raise ValueError("source archive length differs")
    put_if_absent(prefix + "/reservation.json", json.dumps({
        "schema": SCHEMA, "source_commit": commit, "source_archive_sha256": archive_sha,
        "dataset": "ReLAION-1M D768", "split": "validation-1000-already-used",
        "qualifying_100k_terminal_sha256": QUALIFYING_TERMINAL_SHA,
        "inputs_sha256": {name: digest for name, _, _, digest in INPUTS},
        "construction": {"m": 32, "m0": 64, "ef_construction": 128},
        "arms": [[2048, 2048], [4096, 4096], [8192, 8192], [16384, 16384]],
        "gate": "hits>=99605;p05>=98;loaded-p95<92.23ms;loaded-p99<137.87ms;loaded-QPS>=100;RSS<=3GiB;0 vector GET",
        "baseline_scope": "V199 p95/p99 contain S3+SQ8+FP16 but exclude route/plan; advancement ceilings only",
        "interruption_policy": "discard and restart full measurement cell at a new attempt",
        "output_prefix": f"s3://{BUCKET}/{prefix}",
    }, sort_keys=True).encode())
    receipt = ec2.run_instances(
        ClientToken="v219-" + hashlib.sha256(prefix.encode()).hexdigest()[:48],
        ImageId=IMAGE, InstanceType="c7i.4xlarge", MinCount=1, MaxCount=1,
        IamInstanceProfile={"Arn": PROFILE_ARN},
        NetworkInterfaces=[{"AssociatePublicIpAddress": True, "DeviceIndex": 0,
                            "Groups": [SECURITY_GROUP], "SubnetId": SUBNET}],
        InstanceMarketOptions={"MarketType": "spot", "SpotOptions": {
            "InstanceInterruptionBehavior": "terminate", "SpotInstanceType": "one-time"}},
        InstanceInitiatedShutdownBehavior="terminate",
        BlockDeviceMappings=[{"DeviceName": "/dev/xvda", "Ebs": {
            "DeleteOnTermination": True, "Encrypted": True, "VolumeSize": 40,
            "VolumeType": "gp3"}}],
        TagSpecifications=[{"ResourceType": "instance", "Tags": [
            {"Key": "Name", "Value": TAG}, {"Key": "BorsukAttempt", "Value": attempt}]}],
        UserData=base64.b64encode(user_data(commit, archive_sha, archive_key, prefix).encode()).decode(),
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
                    raise ValueError("V219 terminal identity differs")
                terminal["terminal_sha256"] = hashlib.sha256(raw).hexdigest()
                print(json.dumps(terminal, sort_keys=True), flush=True)
                if terminal.get("status") != "complete":
                    raise RuntimeError("V219 failed; inspect only closed terminal artifacts")
                if set(terminal.get("artifacts", {})) != set(ARTIFACTS):
                    raise ValueError("V219 artifact roster differs")
                for name, identity in terminal["artifacts"].items():
                    body = s3.get_object(Bucket=BUCKET,
                        Key=f"{prefix}/artifacts/{name}")["Body"]
                    value = hashlib.sha256()
                    size = 0
                    while chunk := body.read(4 * 1024 * 1024):
                        value.update(chunk)
                        size += len(chunk)
                    if size != identity["bytes"] or value.hexdigest() != identity["sha256"]:
                        raise ValueError(f"V219 artifact readback differs: {name}")
                print(json.dumps({"instance_id": instance_id,
                                  "final_state": "terminated", "artifact_replay": "pass"}), flush=True)
                return
            except ClientError as error:
                if error.response.get("Error", {}).get("Code") not in {"NoSuchKey", "404", "NotFound"}:
                    raise
            state = ec2.describe_instances(InstanceIds=[instance_id])["Reservations"][0]["Instances"][0]["State"]["Name"]
            if state in {"terminated", "shutting-down"}:
                raise RuntimeError("V219 worker stopped before terminal")
            time.sleep(20)
        raise TimeoutError("V219 Spot cell exceeded wall cap")
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
    with open("/tmp/borsuk-v219-reachable-graph-launch.lock", "a+") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        launch(args.attempt)
