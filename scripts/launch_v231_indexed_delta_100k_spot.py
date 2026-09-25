#!/usr/bin/env python3
"""Frozen indexed versus linear 10k-row mutation gate on Spot."""

import argparse
import base64
import fcntl
import hashlib
import io
import json
import subprocess
import tarfile
import time

import boto3
from botocore.exceptions import ClientError

from scripts.launch_v157_primary_feasibility_spot import (
    BUCKET, PROFILE_ARN, REGION, SECURITY_GROUP, SUBNET,
    archive_source, missing, put_if_absent,
)
from scripts.launch_v158_pq_primary_spot import INPUTS as V158_INPUTS
SCHEMA = "borsuk-v231-indexed-delta-100k-spot-v1"
TAG = "borsuk-v231-indexed-delta-100k"
WALL_SECONDS = 7_200
IMAGE = "ami-06121aa3085b6f918"
PARENT = "research/v214-pq-graph-100k/8f8cbd15852ccab48d80ea3c44d21627727b890f/runs/a0001/artifacts/"
PARENT_TERMINAL_SHA = "ad8db261ab30acaedafb491d2d50bab2349c605c7303bcac9374e18c1df08465"
BASE_PREFIX = "research/v218-reachable-graph-100k/ce317cac8d1eb0a1b8a8610f0a3756b96090514c/runs/a0001/"
BASE_TERMINAL_SHA = "cf44ecb95a9d1cc07ac895a222a407c37db090c499f15a887e887d74c8492efd"
INPUTS = (
    ("plane.bin", PARENT + "plane.bin", 154400064,
     "54b9d2e46d2aeccbe28d1afb2c3cdff45df27f7e12a8f0e67354217e948e19a2"),
    ("map.u32", PARENT + "new-to-old.u32", 400000,
     "2451280101bddd38ceb47f7a4502d5c2b65f55140db8cd85335bfe549a41809b"),
    ("books.bin", PARENT + "books.raw", 786432,
     "ce8b255337452bfa8a9fc93b73c2177e5ad4c8b9faebdfe80cd974a41ddc8b2a"),
    ("codes.bin", PARENT + "codes.raw", 6400000,
     "e5c3865571c9f64db2b93f4560f9a07a5f6e8364d0c3c1b8a8ba8fc51d2d912a"),
    ("graph.bin", BASE_PREFIX + "artifacts/graph.bin", 26_571_646,
     "d8b70919243a7cd6ecb9448ce23f776374738476c1882cbc6a651fb34753af2f"),
    ("baseline-raw.jsonl", BASE_PREFIX + "artifacts/raw.jsonl", 2_152_818,
     "900872f9572bff1a195c4e3ed595d3ee28a8aa6f6555f5cc082c1ada52882fa0"),
) + tuple(row for row in V158_INPUTS if row[0] == "requests.jsonl")
if len(INPUTS) != 7:
    raise RuntimeError("V231 frozen input roster differs")
LATE = tuple(row for row in V158_INPUTS if row[0] == "truth.parquet")
ARTIFACTS = ("linear.raw.jsonl", "linear.serving.json", "linear.time",
             "indexed.raw.jsonl", "indexed.serving.json", "indexed.time",
             "quality.json", "build.log", "install.log", "run-closed.log")


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
systemd-run --unit=v231-hard-stop --on-active=@@WALL@@s /usr/sbin/shutdown -h now
root=/mnt/v231-indexed-delta-100k
mkdir -p "$root" && cd "$root"
phase=bootstrap
watcher_pid=
finish() {
  code=$?
  trap - EXIT TERM
  set +e
  [ -n "$watcher_pid" ] && kill "$watcher_pid" 2>/dev/null
  cd "$root"
  cp run.log run-closed.log || code=96
  for name in @@ARTIFACTS@@; do
    if [ -f "$name" ]; then
      aws s3 cp "$name" "s3://@@BUCKET@@/@@PREFIX@@/artifacts/$name" --only-show-errors || code=96
    fi
  done
  token=$(curl -fsS -X PUT -H 'X-aws-ec2-metadata-token-ttl-seconds: 60' http://169.254.169.254/latest/api/token)
  identity=$(curl -fsS -H "X-aws-ec2-metadata-token: $token" http://169.254.169.254/latest/dynamic/instance-identity/document)
  IDENTITY="$identity" EXIT_CODE="$code" PHASE="$phase" python3 - <<'PY' >terminal.json
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
identity=json.loads(os.environ['IDENTITY'])
print(json.dumps({'schema':'@@SCHEMA@@','source_commit':'@@COMMIT@@',
    'source_archive_sha256':'@@ARCHIVE_SHA@@','instance_id':identity['instanceId'],
    'instance_type':identity['instanceType'],'region':identity['region'],
    'availability_zone':identity['availabilityZone'],
    'exit_code':code,'phase':phase,
    'status':('interrupted' if Path('spot-interruption.json').is_file() else
              'complete' if code==0 and phase=='complete' else 'failed'),
    'artifacts':artifacts},sort_keys=True,separators=(',',':')))
PY
  aws s3 cp terminal.json "s3://@@BUCKET@@/@@PREFIX@@/terminal.json" --only-show-errors
  shutdown -h now || true
  exit "$code"
}
trap finish EXIT
trap 'exit 97' TERM
exec >run.log 2>&1
main_pid=$$
spot_watch() {
  local token action
  token=$(curl -fsS -X PUT -H 'X-aws-ec2-metadata-token-ttl-seconds: 21600' http://169.254.169.254/latest/api/token) || return
  while sleep 5; do
    if action=$(curl -fsS -H "X-aws-ec2-metadata-token: $token" http://169.254.169.254/latest/meta-data/spot/instance-action 2>/dev/null); then
      printf '%s\n' "$action" >spot-interruption.json
      aws s3 cp spot-interruption.json 's3://@@BUCKET@@/@@PREFIX@@/spot-interruption.json' --only-show-errors || true
      kill -TERM "$main_pid"
      return
    fi
  done
}
spot_watch & watcher_pid=$!
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
  --bin v229_serve_mutation_overlay_100k --jobs 6 >"$root/build.log" 2>&1
cd "$root"
phase=serve
for mode in linear indexed; do
  /usr/bin/time -v -o "$mode.time" "$CARGO_TARGET_DIR/release/v229_serve_mutation_overlay_100k" \
    . requests.jsonl baseline-raw.jsonl "$mode.raw.jsonl" "$mode.serving.json" "$mode-10k"
  aws s3api put-object --bucket '@@BUCKET@@' --key "@@PREFIX@@/sealed/$mode.raw.jsonl" \
    --body "$mode.raw.jsonl" --if-none-match '*' --no-cli-pager >/dev/null
  raw_sha=$(sha256sum "$mode.raw.jsonl" | cut -d ' ' -f1)
  remote_sha=$(aws s3 cp "s3://@@BUCKET@@/@@PREFIX@@/sealed/$mode.raw.jsonl" - \
    --only-show-errors | sha256sum | cut -d ' ' -f1)
  [ "$raw_sha" = "$remote_sha" ]
done
phase=truth
@@LATE_DOWNLOADS@@
.venv/bin/python -m scripts.v231_score_indexed_delta_100k \
  --linear-serving linear.serving.json --linear-raw linear.raw.jsonl \
  --indexed-serving indexed.serving.json --indexed-raw indexed.raw.jsonl \
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
        raise ValueError("unresolved V231 worker placeholder")
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
    with tarfile.open(fileobj=io.BytesIO(archive), mode="r:gz") as source:
        if not {"docs/research/v231-indexed-delta-100k-prereg.md",
                "scripts/v231_score_indexed_delta_100k.py",
                "scripts/launch_v231_indexed_delta_100k_spot.py"}.issubset(source.getnames()):
            raise ValueError("V231 frozen source roster differs")
    archive_sha = hashlib.sha256(archive).hexdigest()
    archive_key = f"research/v231-indexed-delta-100k/{commit}/sources/{archive_sha}.tar.gz"
    prefix = f"research/v231-indexed-delta-100k/{commit}/runs/{attempt}"
    session = boto3.Session(profile_name="causality", region_name=REGION)
    ec2, s3 = session.client("ec2"), session.client("s3")
    parent_raw = s3.get_object(Bucket=BUCKET,
        Key=PARENT.removesuffix("artifacts/") + "terminal.json")["Body"].read()
    parent = json.loads(parent_raw)
    if (hashlib.sha256(parent_raw).hexdigest() != PARENT_TERMINAL_SHA
            or parent.get("status") != "complete"):
        raise ValueError("V214 closed parent terminal differs")
    for name, key, size, digest in INPUTS:
        if key.startswith(PARENT) and parent["artifacts"].get(key.removeprefix(PARENT)) != {
                "bytes": size, "sha256": digest}:
            raise ValueError(f"V214 parent artifact differs: {name}")
    base_raw = s3.get_object(Bucket=BUCKET, Key=BASE_PREFIX + "terminal.json")["Body"].read()
    base = json.loads(base_raw)
    if (hashlib.sha256(base_raw).hexdigest() != BASE_TERMINAL_SHA
            or base.get("status") != "complete"
            or base["artifacts"].get("raw.jsonl") != {
                "bytes": INPUTS[5][2], "sha256": INPUTS[5][3]}
            or base["artifacts"].get("graph.bin") != {
                "bytes": INPUTS[4][2], "sha256": INPUTS[4][3]}):
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
    spot = ec2.describe_spot_price_history(
        InstanceTypes=["c7i.4xlarge"], ProductDescriptions=["Linux/UNIX"],
        AvailabilityZone="eu-central-1c", MaxResults=1)["SpotPriceHistory"][0]
    quote = float(spot["SpotPrice"])
    put_if_absent(prefix + "/reservation.json", json.dumps({
        "schema": SCHEMA, "source_commit": commit, "source_archive_sha256": archive_sha,
        "dataset": "ReLAION-100k D768", "split": "development-256-plus-method-heldout-744-prior-used",
        "parent_terminal_sha256": PARENT_TERMINAL_SHA,
        "baseline_terminal_sha256": BASE_TERMINAL_SHA,
        "mutation": "every-10th-physical-row same-ID same-FP16-vector upsert;10000 rows",
        "arm": {"ef": 2048, "shortlist": 2048, "k": 100, "workers": 8},
        "indexed_delta": {"m": 16, "m0": 32, "ef_construction": 64,
                          "candidates": 256, "max_resident_bytes": 134217728},
        "gate": "base-IDs-exact-V218;indexed-split-hits>=linear-and-V218;p05>=99;indexed-p95<=0.75-linear-p95;index-build<=60s;RSS<=512MiB;0 vector GET",
        "spot_quote_usd_per_hour": quote,
        "interruption_policy": "discard and restart full measurement cell at a new attempt",
        "output_prefix": f"s3://{BUCKET}/{prefix}",
    }, sort_keys=True).encode())
    receipt = ec2.run_instances(
        ClientToken="v231-" + hashlib.sha256(prefix.encode()).hexdigest()[:48],
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
    launched_epoch = receipt["Instances"][0]["LaunchTime"].timestamp()
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
                    raise ValueError("V231 terminal identity differs")
                terminal["terminal_sha256"] = hashlib.sha256(raw).hexdigest()
                print(json.dumps(terminal, sort_keys=True), flush=True)
                if terminal.get("status") != "complete":
                    raise RuntimeError("V231 failed; inspect only closed terminal artifacts")
                if set(terminal.get("artifacts", {})) != set(ARTIFACTS):
                    raise ValueError("V231 artifact roster differs")
                for name, identity in terminal["artifacts"].items():
                    body = s3.get_object(Bucket=BUCKET,
                        Key=f"{prefix}/artifacts/{name}")["Body"]
                    value = hashlib.sha256()
                    size = 0
                    while chunk := body.read(4 * 1024 * 1024):
                        value.update(chunk)
                        size += len(chunk)
                    if size != identity["bytes"] or value.hexdigest() != identity["sha256"]:
                        raise ValueError(f"V231 artifact readback differs: {name}")
                for mode in ("linear", "indexed"):
                    body = s3.get_object(Bucket=BUCKET,
                        Key=f"{prefix}/sealed/{mode}.raw.jsonl")["Body"].read()
                    if hashlib.sha256(body).hexdigest() != terminal["artifacts"][f"{mode}.raw.jsonl"]["sha256"]:
                        raise ValueError(f"V231 sealed {mode} raw differs")
                quality = json.loads(s3.get_object(Bucket=BUCKET,
                    Key=f"{prefix}/artifacts/quality.json")["Body"].read())
                linear = json.loads(s3.get_object(Bucket=BUCKET,
                    Key=f"{prefix}/artifacts/linear.serving.json")["Body"].read())
                indexed = json.loads(s3.get_object(Bucket=BUCKET,
                    Key=f"{prefix}/artifacts/indexed.serving.json")["Body"].read())
                closed_epoch = time.time()
                closeout = {"schema": SCHEMA + "-closeout", "source_commit": commit,
                            "source_archive_sha256": archive_sha,
                            "terminal_sha256": terminal["terminal_sha256"],
                            "instance_id": instance_id, "final_state": "terminated",
                            "spot_quote_usd_per_hour": quote,
                            "estimated_compute_usd_to_closeout":
                                (closed_epoch - launched_epoch) * quote / 3600,
                            "quality": quality, "linear": linear, "indexed": indexed,
                            "gate_pass": quality["pass"]}
                put_if_absent(prefix + "/closeout.json",
                              json.dumps(closeout, sort_keys=True).encode())
                print(json.dumps({"instance_id": instance_id,
                                  "final_state": "terminated", "artifact_replay": "pass",
                                  "gate_pass": quality["pass"],
                                  "linear_loaded": linear["loaded"],
                                  "indexed_loaded": indexed["loaded"],
                                  "index_build_ns": indexed["index_build_ns"]}), flush=True)
                return
            except ClientError as error:
                if error.response.get("Error", {}).get("Code") not in {"NoSuchKey", "404", "NotFound"}:
                    raise
            state = ec2.describe_instances(InstanceIds=[instance_id])["Reservations"][0]["Instances"][0]["State"]["Name"]
            if state in {"terminated", "shutting-down"}:
                raise RuntimeError("V218 worker stopped before terminal")
            time.sleep(20)
        raise TimeoutError("V231 Spot cell exceeded wall cap")
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
    with open("/tmp/borsuk-v231-mutation-overlay-launch.lock", "a+") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        launch(args.attempt)
