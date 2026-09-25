#!/usr/bin/env python3
"""Frozen paired 100k source-format falsifier on Spot."""

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
from scripts.launch_v158_pq_primary_spot import INPUTS as V158_INPUTS
SCHEMA = "borsuk-v226-fp16-graph-source-100k-spot-v1"
TAG = "borsuk-v226-fp16-graph-source-100k"
WALL_SECONDS = 7_200
IMAGE = "ami-06121aa3085b6f918"
PARENT = "research/v214-pq-graph-100k/8f8cbd15852ccab48d80ea3c44d21627727b890f/runs/a0001/artifacts/"
PARENT_TERMINAL_SHA = "ad8db261ab30acaedafb491d2d50bab2349c605c7303bcac9374e18c1df08465"
SELECTING_TERMINAL_SHA = "bd0297af0c3d24fd336dbf6e3e6adb44e52636a6c59e6c654e1f0acf20fca97b"
SELECTING_PREFIX = "research/v215-pq-cosine-graph-100k/d861520e8559d28dd10def977c9e941c550d7099/runs/a0001/"
BASE_PREFIX = "research/v218-reachable-graph-100k/ce317cac8d1eb0a1b8a8610f0a3756b96090514c/runs/a0001/"
BASE_TERMINAL_SHA = "cf44ecb95a9d1cc07ac895a222a407c37db090c499f15a887e887d74c8492efd"
INPUTS = (
    ("prepare.json", PARENT + "prepare.json", 938,
     "fe5fbf627ca5009ed0632607ee3bcc27c929e35783d318a2a6de8672ffb6e0a7"),
    ("graph-prep.json", PARENT + "graph-prep.json", 343,
     "0b0a437fbbea2b99a7b4ffb10d4bdf555c4c63e11c6290e906f941019ab6245b"),
    ("vectors.raw", PARENT + "vectors.raw", 307200000,
     "66094a44928e72640070c950696f81691689910b2b07dc9fc90a402ed262620f"),
    ("plane.bin", PARENT + "plane.bin", 154400064,
     "54b9d2e46d2aeccbe28d1afb2c3cdff45df27f7e12a8f0e67354217e948e19a2"),
    ("new-to-old.u32", PARENT + "new-to-old.u32", 400000,
     "2451280101bddd38ceb47f7a4502d5c2b65f55140db8cd85335bfe549a41809b"),
    ("books.raw", PARENT + "books.raw", 786432,
     "ce8b255337452bfa8a9fc93b73c2177e5ad4c8b9faebdfe80cd974a41ddc8b2a"),
    ("codes.raw", PARENT + "codes.raw", 6400000,
     "e5c3865571c9f64db2b93f4560f9a07a5f6e8364d0c3c1b8a8ba8fc51d2d912a"),
) + tuple(row for row in V158_INPUTS if row[0] == "requests.jsonl")
if len(INPUTS) != 8:
    raise RuntimeError("V226 frozen input roster differs")
LATE = tuple(row for row in V158_INPUTS if row[0] == "truth.parquet") + (
    ("baseline-raw.jsonl", BASE_PREFIX + "artifacts/raw.jsonl", 2_152_818,
     "900872f9572bff1a195c4e3ed595d3ee28a8aa6f6555f5cc082c1ada52882fa0"),
)
ARTIFACTS = ("graph.bin", "build-summary.json", "raw.jsonl", "serving.json",
             "quality.json", "graph-build-resources.txt", "serving-resources.txt",
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
systemd-run --unit=v226-hard-stop --on-active=@@WALL@@s /usr/sbin/shutdown -h now
root=/mnt/v226-fp16-graph-source-100k
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
phase=build
cd repo
"$CARGO_HOME/bin/cargo" build --release --locked -p borsuk \
  --bin v218_build_reachable_graph_100k --bin v218_serve_reachable_graph_100k \
  --jobs 6 >"$root/build.log" 2>&1
cd "$root"
phase=graph
/usr/bin/time -v -o graph-build-resources.txt "$CARGO_TARGET_DIR/release/v218_build_reachable_graph_100k" \
  graph-prep.json plane.bin vectors.raw graph.bin build-summary.json --fp16-source
phase=serve
/usr/bin/time -v -o serving-resources.txt "$CARGO_TARGET_DIR/release/v218_serve_reachable_graph_100k" \
  prepare.json build-summary.json plane.bin graph.bin new-to-old.u32 books.raw codes.raw \
  requests.jsonl raw.jsonl serving.json
aws s3api put-object --bucket '@@BUCKET@@' --key '@@PREFIX@@/sealed/raw.jsonl' \
  --body raw.jsonl --if-none-match '*' --no-cli-pager >/dev/null
raw_sha=$(sha256sum raw.jsonl | cut -d ' ' -f1)
remote_sha=$(aws s3 cp 's3://@@BUCKET@@/@@PREFIX@@/sealed/raw.jsonl' - \
  --only-show-errors | sha256sum | cut -d ' ' -f1)
[ "$raw_sha" = "$remote_sha" ]
phase=truth
@@LATE_DOWNLOADS@@
.venv/bin/python -m scripts.v226_score_fp16_graph_source \
  --build build-summary.json --serving serving.json --raw raw.jsonl \
  --baseline baseline-raw.jsonl --truth truth.parquet --output quality.json
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
        raise ValueError("unresolved V218 worker placeholder")
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
    archive_key = f"research/v226-fp16-graph-source-100k/{commit}/sources/{archive_sha}.tar.gz"
    prefix = f"research/v226-fp16-graph-source-100k/{commit}/runs/{attempt}"
    session = boto3.Session(profile_name="causality", region_name=REGION)
    ec2, s3 = session.client("ec2"), session.client("s3")
    parent_raw = s3.get_object(Bucket=BUCKET,
        Key=PARENT.removesuffix("artifacts/") + "terminal.json")["Body"].read()
    parent = json.loads(parent_raw)
    if (hashlib.sha256(parent_raw).hexdigest() != PARENT_TERMINAL_SHA
            or parent.get("status") != "complete"):
        raise ValueError("V214 closed parent terminal differs")
    for name, key, size, digest in INPUTS:
        if key.startswith(PARENT) and parent["artifacts"].get(name) != {
                "bytes": size, "sha256": digest}:
            raise ValueError(f"V214 parent artifact differs: {name}")
    selected_raw = s3.get_object(Bucket=BUCKET,
        Key=SELECTING_PREFIX + "terminal.json")["Body"].read()
    selected = json.loads(selected_raw)
    if (hashlib.sha256(selected_raw).hexdigest() != SELECTING_TERMINAL_SHA
            or selected.get("status") != "complete"):
        raise ValueError("V215 selecting terminal differs")
    summary_raw = s3.get_object(Bucket=BUCKET,
        Key=SELECTING_PREFIX + "artifacts/summary.json")["Body"].read()
    if (hashlib.sha256(summary_raw).hexdigest()
            != selected["artifacts"]["summary.json"]["sha256"]
            or json.loads(summary_raw).get("passing_arm") != "2048-2048"):
        raise ValueError("V215 frozen selected arm differs")
    base_raw = s3.get_object(Bucket=BUCKET, Key=BASE_PREFIX + "terminal.json")["Body"].read()
    base = json.loads(base_raw)
    if (hashlib.sha256(base_raw).hexdigest() != BASE_TERMINAL_SHA
            or base.get("status") != "complete"
            or base["artifacts"].get("raw.jsonl") != {
                "bytes": LATE[-1][2], "sha256": LATE[-1][3]}):
        raise ValueError("V218 paired baseline differs")
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
        "dataset": "ReLAION-100k D768", "split": "development-256-plus-method-heldout-744-prior-used",
        "parent_terminal_sha256": PARENT_TERMINAL_SHA,
        "selecting_terminal_sha256": SELECTING_TERMINAL_SHA,
        "baseline_terminal_sha256": BASE_TERMINAL_SHA,
        "construction_source": "authenticated-fp16-plane",
        "construction": {"m": 32, "m0": 64, "ef_construction": 128},
        "arms": [[2048, 2048]],
        "gate": "reachable=100000;min-indegree>=4;PQ-GT100-hits>=99746;development>=25522;heldout>=74219;p05>=98;loaded-eight-worker-p95<=8.298ms;RSS<=268435456B;0 vector GET",
        "interruption_policy": "discard and restart full measurement cell at a new attempt",
        "output_prefix": f"s3://{BUCKET}/{prefix}",
    }, sort_keys=True).encode())
    receipt = ec2.run_instances(
        ClientToken="v226-" + hashlib.sha256(prefix.encode()).hexdigest()[:48],
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
                    raise ValueError("V226 terminal identity differs")
                terminal["terminal_sha256"] = hashlib.sha256(raw).hexdigest()
                print(json.dumps(terminal, sort_keys=True), flush=True)
                if terminal.get("status") != "complete":
                    raise RuntimeError("V226 failed; inspect only closed terminal artifacts")
                if set(terminal.get("artifacts", {})) != set(ARTIFACTS):
                    raise ValueError("V226 artifact roster differs")
                for name, identity in terminal["artifacts"].items():
                    body = s3.get_object(Bucket=BUCKET,
                        Key=f"{prefix}/artifacts/{name}")["Body"]
                    value = hashlib.sha256()
                    size = 0
                    while chunk := body.read(4 * 1024 * 1024):
                        value.update(chunk)
                        size += len(chunk)
                    if size != identity["bytes"] or value.hexdigest() != identity["sha256"]:
                        raise ValueError(f"V226 artifact readback differs: {name}")
                print(json.dumps({"instance_id": instance_id,
                                  "final_state": "terminated", "artifact_replay": "pass"}), flush=True)
                return
            except ClientError as error:
                if error.response.get("Error", {}).get("Code") not in {"NoSuchKey", "404", "NotFound"}:
                    raise
            state = ec2.describe_instances(InstanceIds=[instance_id])["Reservations"][0]["Instances"][0]["State"]["Name"]
            if state in {"terminated", "shutting-down"}:
                raise RuntimeError("V218 worker stopped before terminal")
            time.sleep(20)
        raise TimeoutError("V226 Spot cell exceeded wall cap")
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
    with open("/tmp/borsuk-v226-fp16-graph-source-launch.lock", "a+") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        launch(args.attempt)
