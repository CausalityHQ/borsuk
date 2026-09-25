#!/usr/bin/env python3
"""One frozen ReLAION-1M graph-source hash gate on Causality Spot."""

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
from scripts.launch_v198_real_query_resident_fp16_spot import EXTRA_INPUTS
from scripts.launch_v219_reachable_graph_1m_spot import describe_state

SCHEMA = "borsuk-v227-fp16-graph-source-1m-spot-v1"
CAMPAIGN = "research/v227-fp16-graph-source-1m"
TAG = "borsuk-v227-fp16-graph-source-1m"
IMAGE = "ami-06121aa3085b6f918"
WALL_SECONDS = 7_200
V226_PREFIX = "research/v226-fp16-graph-source-100k/17f855b7768060c7e4850f9c68aff23c4a459058/runs/a0001/"
V226_TERMINAL_SHA = "24d71d8b97b369c1b50df72ec546b2c467cf58702408e264c982539350e4c32f"
V219_PREFIX = "research/v219-reachable-graph-1m/008ab6fbc50e6293e0599a33993c619702109bd9/runs/a0002/"
V219_TERMINAL_SHA = "782fe56ee77a7f16c39e77a6012da63d899e201a694984d919952e894cc90180"
V219_GRAPH_SHA = "a2805a97c1955adf1cdc0b0da43b4ff205eb1d4646916c09d3c4d2b9f7c1ee0b"
INPUTS = (("prep.json", V219_PREFIX + "artifacts/prep.json", 844,
           "a2afb5d183c1d2dbf43dc8e2f67b7ec61e25ed7ee3daea6f83e27716511fdc82"),
          next(row for row in EXTRA_INPUTS if row[0] == "plane.bin"))
ARTIFACTS = ("graph.bin", "build-summary.json", "decision.json",
             "graph-build-resources.txt", "build.log", "install.log", "run-closed.log")


def worker(commit: str, archive_sha: str, archive_key: str, prefix: str) -> str:
    script = r'''#!/bin/bash
set -euo pipefail
systemd-run --unit=v227-hard-stop --on-active=@@WALL@@s /usr/sbin/shutdown -h now
root=/mnt/v227-fp16-graph-source-1m
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
        digest=hashlib.sha256()
        with path.open('rb') as source:
            for block in iter(lambda:source.read(1024*1024),b''):
                digest.update(block)
        artifacts[name]={'bytes':path.stat().st_size,'sha256':digest.hexdigest()}
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
dnf install -y -q gcc gcc-c++ cmake perl tar gzip time >install.log 2>&1
export RUSTUP_HOME="$root/.rustup" CARGO_HOME="$root/.cargo" CARGO_TARGET_DIR="$root/target" CARGO_BUILD_JOBS=6
curl -fsSL https://sh.rustup.rs | sh -s -- -y --profile minimal --default-toolchain 1.98.0 >>install.log 2>&1
phase=inputs
@@DOWNLOADS@@
phase=build
cd repo
"$CARGO_HOME/bin/cargo" build --release --locked -p borsuk \
  --bin v219_build_reachable_graph_1m --jobs 6 >"$root/build.log" 2>&1
cd "$root"
/usr/bin/time -v -o graph-build-resources.txt "$CARGO_TARGET_DIR/release/v219_build_reachable_graph_1m" \
  prep.json plane.bin graph.bin build-summary.json --fp16-source
phase=decision
python3 - <<'PY' >decision.json
import hashlib,json
from pathlib import Path
summary=json.loads(Path('build-summary.json').read_text())
actual=hashlib.sha256(Path('graph.bin').read_bytes()).hexdigest()
expected='@@GRAPH_SHA@@'
assert summary['construction_source']=='authenticated-fp16-plane'
assert summary['graph_sha256']==actual
print(json.dumps({'schema':'borsuk-v227-fp16-graph-source-decision-v1',
    'dataset':'ReLAION-1M D768','selected_v219_graph_sha256':expected,
    'fp16_graph_sha256':actual,'graph_bytes':Path('graph.bin').stat().st_size,
    'byte_identical':actual==expected},sort_keys=True,separators=(',',':')))
PY
phase=complete
'''
    downloads = "\n".join(
        f"aws s3 cp 's3://{BUCKET}/{key}' '{name}' --only-show-errors\n"
        f"[ \"$(stat -c%s '{name}')\" = '{size}' ]\n"
        f"printf '%s  %s\\n' '{digest}' '{name}' | sha256sum -c -"
        for name, key, size, digest in INPUTS)
    values = {"WALL": WALL_SECONDS, "ARTIFACTS": " ".join(ARTIFACTS),
              "ARTIFACTS_PY": repr(ARTIFACTS), "BUCKET": BUCKET,
              "PREFIX": prefix, "SCHEMA": SCHEMA, "COMMIT": commit,
              "ARCHIVE_SHA": archive_sha, "ARCHIVE_KEY": archive_key,
              "DOWNLOADS": downloads, "GRAPH_SHA": V219_GRAPH_SHA}
    for key, value in values.items():
        script = script.replace("@@" + key + "@@", str(value))
    if "@@" in script:
        raise ValueError("unresolved V227 worker placeholder")
    return script


def launch(attempt: str) -> None:
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
    archive_key = f"{CAMPAIGN}/{commit}/sources/{archive_sha}.tar.gz"
    prefix = f"{CAMPAIGN}/{commit}/runs/{attempt}"
    session = boto3.Session(profile_name="causality", region_name=REGION)
    ec2, s3 = session.client("ec2"), session.client("s3")
    for label, source_prefix, expected_sha in (
            ("V226", V226_PREFIX, V226_TERMINAL_SHA),
            ("V219", V219_PREFIX, V219_TERMINAL_SHA)):
        raw = s3.get_object(Bucket=BUCKET, Key=source_prefix + "terminal.json")["Body"].read()
        terminal = json.loads(raw)
        if hashlib.sha256(raw).hexdigest() != expected_sha or terminal.get("status") != "complete":
            raise ValueError(f"{label} terminal differs")
        if label == "V226":
            quality = s3.get_object(Bucket=BUCKET, Key=source_prefix + "artifacts/quality.json")["Body"].read()
            if (hashlib.sha256(quality).hexdigest() != terminal["artifacts"]["quality.json"]["sha256"]
                    or json.loads(quality).get("pass") is not True):
                raise ValueError("V226 qualifying gate differs")
        else:
            if terminal["artifacts"].get("graph.bin", {}).get("sha256") != V219_GRAPH_SHA:
                raise ValueError("V219 graph baseline differs")
            if terminal["artifacts"].get("prep.json") != {
                    "bytes": INPUTS[0][2], "sha256": INPUTS[0][3]}:
                raise ValueError("V219 preparation differs")
    if not missing(s3, prefix + "/reservation.json") or not missing(s3, prefix + "/terminal.json"):
        raise ValueError("attempt already registered")
    active = ec2.describe_instances(Filters=[
        {"Name": "tag:Name", "Values": ["borsuk-*"]},
        {"Name": "instance-state-name", "Values": ["pending", "running", "stopping"]},
    ])
    if any(row.get("Instances") for row in active["Reservations"]):
        raise ValueError("another BORSUK worker is active")
    if missing(s3, archive_key):
        put_if_absent(archive_key, archive)
    elif s3.head_object(Bucket=BUCKET, Key=archive_key)["ContentLength"] != len(archive):
        raise ValueError("source archive length differs")
    put_if_absent(prefix + "/reservation.json", json.dumps({
        "schema": SCHEMA, "source_commit": commit, "source_archive_sha256": archive_sha,
        "dataset": "ReLAION-1M D768", "split": "validation-1000-already-used",
        "source_format": "authenticated-fp16-plane", "construction": {"m": 32, "m0": 64, "ef_construction": 128},
        "v226_terminal_sha256": V226_TERMINAL_SHA,
        "v219_terminal_sha256": V219_TERMINAL_SHA,
        "gate": "graph SHA-256 equals selected V219 graph; if unequal, same-revision paired query quality required",
        "interruption_policy": "discard and restart full build cell at new attempt",
        "output_prefix": f"s3://{BUCKET}/{prefix}",
    }, sort_keys=True).encode())
    receipt = ec2.run_instances(
        ClientToken="v227-" + hashlib.sha256(prefix.encode()).hexdigest()[:48],
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
                    raise ValueError("V227 terminal identity differs")
                terminal["terminal_sha256"] = hashlib.sha256(raw).hexdigest()
                print(json.dumps(terminal, sort_keys=True), flush=True)
                if terminal.get("status") != "complete":
                    raise RuntimeError("V227 failed; inspect only closed terminal artifacts")
                if set(terminal.get("artifacts", {})) != set(ARTIFACTS):
                    raise ValueError("V227 artifact roster differs")
                for name, identity in terminal["artifacts"].items():
                    body = s3.get_object(Bucket=BUCKET,
                        Key=f"{prefix}/artifacts/{name}")["Body"]
                    digest = hashlib.sha256()
                    size = 0
                    while chunk := body.read(4 * 1024 * 1024):
                        digest.update(chunk)
                        size += len(chunk)
                    if size != identity["bytes"] or digest.hexdigest() != identity["sha256"]:
                        raise ValueError(f"V227 artifact readback differs: {name}")
                print(json.dumps({"instance_id": instance_id,
                                  "final_state": "terminated", "artifact_replay": "pass"}), flush=True)
                return
            except ClientError as error:
                if error.response.get("Error", {}).get("Code") not in {"NoSuchKey", "404", "NotFound"}:
                    raise
            state = describe_state(ec2, instance_id)
            if state in {"terminated", "shutting-down"}:
                raise RuntimeError("V227 worker stopped before terminal")
            time.sleep(20)
        raise TimeoutError("V227 Spot cell exceeded wall cap")
    finally:
        try:
            state = describe_state(ec2, instance_id)
            if state not in {"terminated", "shutting-down"}:
                ec2.terminate_instances(InstanceIds=[instance_id])
        except Exception:
            pass


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--attempt", default="a0001")
    args = parser.parse_args()
    with open("/tmp/borsuk-v227-fp16-graph-source-launch.lock", "a+") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        launch(args.attempt)
