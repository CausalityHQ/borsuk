#!/usr/bin/env python3
"""One frozen, closed ReLAION-1M whole-query resident serving cell on Spot."""

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
from scripts.launch_v198_real_query_resident_fp16_spot import (
    EXTRA_INPUTS as V198_INPUTS,
)

SCHEMA = "borsuk-v209-resident-1m-serving-spot-v1"
TAG = "borsuk-v209-resident-1m-serving"
IMAGE = "ami-06121aa3085b6f918"
WALL_SECONDS = 7_200
NEEDED = {"old-layout.npy", "old-sq8.bin", "order.npy", "router/manifest.json",
          "router/summaries.bin", "router/books.bin", "router/codes.bin",
          "router/low.bin", "router/step.bin"}
INPUTS = tuple(row for row in V195_INPUTS if row[0] in NEEDED) + tuple(
    row for row in V198_INPUTS if row[0] in {"plane.bin", "new-sq8.bin", "requests.jsonl"})
if {row[0] for row in INPUTS} != NEEDED | {"plane.bin", "new-sq8.bin", "requests.jsonl"}:
    raise RuntimeError("V209 frozen input roster differs")
BASELINE = ("v198-raw.jsonl",
            "research/v198-real-query-resident-fp16/"
            "fdc51a358be350678d9f6f279a2be95a57709c02/"
            "runs/a0001/artifacts/out/raw.jsonl",
            3_230_820, "2b18321435642de3fad4df02b84abcc046fb6c9b17808e72d6a50eea54a9ec98")
ARTIFACTS = ("prepare.json", "serving.json", "raw.jsonl", "quality.json",
             "resident-root.json", "row-map.bin", "build.log", "install.log",
             "prepare-resources.txt", "serving-resources.txt", "run-closed.log")


def downloads(rows: tuple) -> str:
    return "\n".join(
        f"mkdir -p \"$(dirname '{name}')\"\n"
        f"aws s3 cp 's3://{BUCKET}/{key}' '{name}' --only-show-errors\n"
        f"[ \"$(stat -c%s '{name}')\" = '{size}' ]\n"
        f"printf '%s  %s\\n' '{sha}' '{name}' | sha256sum -c -"
        for name, key, size, sha in rows
    )


def user_data(commit: str, archive_sha: str, archive_key: str, prefix: str,
              plane_key: str, plane_etag: str) -> str:
    template = r'''#!/bin/bash
set -euo pipefail
systemd-run --unit=v209-hard-stop --on-active=@@WALL@@s /usr/sbin/shutdown -h now
root=/mnt/v209-resident-1m
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
            for block in iter(lambda: source.read(1024*1024),b''):
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
dnf install -y -q python3.12 python3.12-pip gcc gcc-c++ cmake perl tar gzip time >install.log 2>&1
python3.12 -m venv .venv
.venv/bin/pip install -q numpy==1.26.4 >>install.log 2>&1
export RUSTUP_HOME="$root/.rustup" CARGO_HOME="$root/.cargo" CARGO_TARGET_DIR="$root/target" CARGO_BUILD_JOBS=6
curl -fsSL https://sh.rustup.rs | sh -s -- -y --profile minimal --default-toolchain 1.98.0 >>install.log 2>&1
export PYTHONPATH="$root/repo" OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1
phase=inputs
@@DOWNLOADS@@
mv router/manifest.json router/manifest-v1.json
phase=convert
.venv/bin/python -m scripts.v209_prepare_resident_map --router router \
  --old-layout old-layout.npy --order order.npy --map new-to-old.u32
phase=build
cd repo
"$CARGO_HOME/bin/cargo" build --release --locked -p borsuk --bin v209_resident_nominee_1m --jobs 6 >"$root/build.log" 2>&1
cd "$root"
phase=prepare
/usr/bin/time -v -o prepare-resources.txt "$CARGO_TARGET_DIR/release/v209_resident_nominee_1m" \
  prepare router old-sq8.bin new-sq8.bin new-to-old.u32 plane.bin row-map.bin \
  resident-root.json '@@PLANE_KEY@@' '@@PLANE_ETAG@@' >prepare.json
aws s3api put-object --bucket '@@BUCKET@@' --key '@@PREFIX@@/sealed/resident-root.json' \
  --body resident-root.json --if-none-match '*' --no-cli-pager >/dev/null
phase=serve
/usr/bin/time -v -o serving-resources.txt "$CARGO_TARGET_DIR/release/v209_resident_nominee_1m" \
  serve router row-map.bin plane.bin resident-root.json requests.jsonl raw.jsonl >serving.json
aws s3api put-object --bucket '@@BUCKET@@' --key '@@PREFIX@@/sealed/raw.jsonl' \
  --body raw.jsonl --if-none-match '*' --no-cli-pager >/dev/null
raw_sha=$(sha256sum raw.jsonl | cut -d ' ' -f1)
sealed_sha=$(aws s3 cp 's3://@@BUCKET@@/@@PREFIX@@/sealed/raw.jsonl' - \
  --only-show-errors | sha256sum | cut -d ' ' -f1)
[ "$raw_sha" = "$sealed_sha" ]
phase=truth
@@BASELINE_DOWNLOAD@@
.venv/bin/python -m scripts.v209_score_resident_1m --baseline v198-raw.jsonl \
  --raw raw.jsonl --serving serving.json --output quality.json
phase=complete
'''
    replacements = {
        "WALL": str(WALL_SECONDS), "ARTIFACTS": " ".join(ARTIFACTS),
        "ARTIFACTS_PY": repr(ARTIFACTS), "BUCKET": BUCKET,
        "PREFIX": prefix, "SCHEMA": SCHEMA, "COMMIT": commit,
        "ARCHIVE_SHA": archive_sha, "ARCHIVE_KEY": archive_key,
        "DOWNLOADS": downloads(INPUTS), "BASELINE_DOWNLOAD": downloads((BASELINE,)),
        "PLANE_KEY": plane_key, "PLANE_ETAG": plane_etag,
    }
    for name, value in replacements.items():
        template = template.replace("@@" + name + "@@", value)
    if "@@" in template:
        raise ValueError("unresolved V209 worker placeholder")
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
    archive_key = f"research/v209-resident-1m-serving/{commit}/sources/{archive_sha}.tar.gz"
    prefix = f"research/v209-resident-1m-serving/{commit}/runs/{attempt}"
    session = boto3.Session(profile_name="causality", region_name=REGION)
    ec2, s3 = session.client("ec2"), session.client("s3")
    if not missing(s3, prefix + "/reservation.json") or not missing(s3, prefix + "/terminal.json"):
        raise ValueError("attempt already registered")
    active = ec2.describe_instances(Filters=[
        {"Name": "tag:Name", "Values": ["borsuk-*"]},
        {"Name": "instance-state-name", "Values": ["pending", "running", "stopping"]},
    ])
    if any(item.get("Instances") for item in active["Reservations"]):
        raise ValueError("another BORSUK worker is active")
    plane_key = next(row[1] for row in INPUTS if row[0] == "plane.bin")
    head = s3.head_object(Bucket=BUCKET, Key=plane_key)
    if head["ContentLength"] != 1_544_000_064 or not head.get("ETag"):
        raise ValueError("frozen resident plane S3 identity differs")
    if missing(s3, archive_key):
        put_if_absent(archive_key, archive)
    elif s3.head_object(Bucket=BUCKET, Key=archive_key)["ContentLength"] != len(archive):
        raise ValueError("source archive length differs")
    put_if_absent(prefix + "/reservation.json", json.dumps({
        "schema": SCHEMA, "source_commit": commit, "source_archive_sha256": archive_sha,
        "dataset": "ReLAION-1M D768", "split": "validation-1000-already-used",
        "baseline": "V199 same-panel 99605 GT100, partial p95 92.23ms/p99 137.87ms",
        "internal_gate": "hits>=99605;p05>=98;p95<92.23ms;p99<137.87ms;QPS>=100;RSS<=2GiB;0 vector GET",
        "plane_key": plane_key, "plane_etag": head["ETag"],
        "interruption_policy": "discard and restart full measurement cell at a new attempt",
        "output_prefix": f"s3://{BUCKET}/{prefix}",
    }, sort_keys=True).encode())
    receipt = ec2.run_instances(
        ClientToken="v209-" + hashlib.sha256(prefix.encode()).hexdigest()[:48],
        ImageId=IMAGE, InstanceType="c7i.12xlarge", MinCount=1, MaxCount=1,
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
        UserData=base64.b64encode(user_data(
            commit, archive_sha, archive_key, prefix, plane_key, head["ETag"]).encode()).decode(),
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
                    raise ValueError("V209 terminal identity differs")
                terminal["terminal_sha256"] = hashlib.sha256(raw).hexdigest()
                print(json.dumps(terminal, sort_keys=True), flush=True)
                if terminal.get("status") != "complete":
                    raise RuntimeError("V209 failed; inspect only closed terminal artifacts")
                if set(terminal.get("artifacts", {})) != set(ARTIFACTS):
                    raise ValueError("V209 artifact roster differs")
                for name, identity in terminal["artifacts"].items():
                    body = s3.get_object(Bucket=BUCKET,
                        Key=f"{prefix}/artifacts/{name}")["Body"]
                    value = hashlib.sha256()
                    size = 0
                    while chunk := body.read(4 * 1024 * 1024):
                        value.update(chunk)
                        size += len(chunk)
                    if size != identity["bytes"] or value.hexdigest() != identity["sha256"]:
                        raise ValueError(f"V209 artifact readback differs: {name}")
                print(json.dumps({"instance_id": instance_id,
                                  "final_state": "terminated", "artifact_replay": "pass"}), flush=True)
                return
            except ClientError as error:
                if error.response.get("Error", {}).get("Code") not in {"NoSuchKey", "404", "NotFound"}:
                    raise
            state = ec2.describe_instances(InstanceIds=[instance_id])["Reservations"][0]["Instances"][0]["State"]["Name"]
            if state in {"terminated", "shutting-down"}:
                raise RuntimeError("V209 worker stopped before terminal")
            time.sleep(20)
        raise TimeoutError("V209 Spot cell exceeded wall cap")
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
    with open("/tmp/borsuk-v209-resident-1m-launch.lock", "a+") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        launch(args.attempt)
