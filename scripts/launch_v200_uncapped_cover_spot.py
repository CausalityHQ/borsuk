#!/usr/bin/env python3
"""One GT-blind Rust linear-cover parity/timing cell on Causality Spot."""

from __future__ import annotations

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
from scripts.launch_v196_resident_fp16_preflight_spot import download_script
from scripts.launch_v199_live_s3_resident_fp16_spot import bootstrap

SCHEMA = "borsuk-v200-uncapped-cover-spot-v1"
TAG = "borsuk-v200-uncapped-cover"
IMAGE = "ami-06121aa3085b6f918"
WALL_SECONDS = 7_200
V198_PREFIX = ("research/v198-real-query-resident-fp16/"
               "fdc51a358be350678d9f6f279a2be95a57709c02/runs/a0001/")
INPUTS = (
    ("features.jsonl", V198_PREFIX + "artifacts/out/features.jsonl", 11_279_845,
     "3b4fe2d7a83bd0af16b1ecf460052b78311c6526bd7263fb3e708b56dd26a08c"),
    ("plans.jsonl", V198_PREFIX + "artifacts/out/plans.jsonl", 295_412,
     "0a61974457030d2e2ce828e7bbbbaf3d8acd9849e70a7aaafd0cb44b0c5dda00"),
    ("v192-result.json", "research/v192-optional-hard-plan-fit/"
     "a22a0d7c6f9cfc71f627bdafa85875750c2048f9/runs/a0001/artifacts/out.json",
     174_089, "b79683695350b4bc21eb4cad14f3588ed5ebaef088a1dd9443b3cc429ef62a01"),
)
ARTIFACTS = ("weights.jsonl", "weight-seal.json", "bench.json",
             "weights-resources.txt", "bench-resources.txt",
             "build.log", "run-closed.log")


def user_data(commit: str, archive_sha: str, archive_key: str,
              prefix: str) -> str:
    template = r'''#!/bin/bash
set -euo pipefail
systemd-run --unit=v200-hard-stop --on-active=@@WALL@@s /usr/sbin/shutdown -h now
root=/mnt/v200-uncapped-cover
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
            for chunk in iter(lambda: source.read(1024*1024),b''):
                digest.update(chunk)
        artifacts[name]={'bytes':path.stat().st_size,'sha256':digest.hexdigest()}
code=int(os.environ['EXIT_CODE']); phase=os.environ['PHASE']
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
dnf install -y -q gcc gcc-c++ cmake perl tar gzip time
export RUSTUP_HOME="$root/.rustup" CARGO_HOME="$root/.cargo" CARGO_TARGET_DIR="$root/target" CARGO_BUILD_JOBS=6
curl -fsSL https://sh.rustup.rs | sh -s -- -y --profile minimal --default-toolchain 1.98.0
export PYTHONPATH="$root/repo"
phase=inputs
@@DOWNLOAD@@
phase=weights
/usr/bin/time -v -o weights-resources.txt python3 -m scripts.v200_uncapped_weights \
  features.jsonl plans.jsonl v192-result.json weights.jsonl >weight-seal.json
aws s3api put-object --bucket '@@BUCKET@@' --key '@@PREFIX@@/sealed/weights.jsonl' \
  --body weights.jsonl --if-none-match '*' --no-cli-pager >/dev/null
aws s3api put-object --bucket '@@BUCKET@@' --key '@@PREFIX@@/sealed/weight-seal.json' \
  --body weight-seal.json --if-none-match '*' --no-cli-pager >/dev/null
weight_sha=$(python3 -c 'import json;print(json.load(open("weight-seal.json"))["weights_sha256"])')
phase=build
cd repo
"$CARGO_HOME/bin/cargo" build --release --locked -p borsuk --bin v200_uncapped_cover_preflight --jobs 6 >"$root/build.log" 2>&1
cd "$root"
phase=bench
/usr/bin/time -v -o bench-resources.txt "$CARGO_TARGET_DIR/release/v200_uncapped_cover_preflight" \
  weights.jsonl "$weight_sha" plans.jsonl >bench.json
phase=complete
'''
    replacements = {
        "WALL": str(WALL_SECONDS), "ARTIFACTS": " ".join(ARTIFACTS),
        "ARTIFACTS_PY": repr(ARTIFACTS), "BUCKET": BUCKET,
        "PREFIX": prefix, "SCHEMA": SCHEMA, "COMMIT": commit,
        "ARCHIVE_SHA": archive_sha, "ARCHIVE_KEY": archive_key,
        "DOWNLOAD": download_script(INPUTS),
    }
    for name, value in replacements.items():
        template = template.replace("@@" + name + "@@", value)
    if "@@" in template:
        raise ValueError("unresolved V200 worker placeholder")
    return template


def launch(attempt: str) -> None:
    if len(attempt) != 5 or not attempt.startswith("a") or not attempt[1:].isdigit():
        raise ValueError("attempt must be aNNNN")
    if subprocess.check_output(["git", "status", "--porcelain"], text=True).strip():
        raise ValueError("worktree must be clean")
    commit = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    subprocess.run(["git", "fetch", "origin", "main"], check=True)
    if subprocess.run(["git", "merge-base", "--is-ancestor", "HEAD", "origin/main"],
                      check=False).returncode != 0:
        raise ValueError("V200 source commit is not pushed to origin/main")
    archive = archive_source(commit)
    with tarfile.open(fileobj=io.BytesIO(archive), mode="r:gz") as tar:
        required = {"scripts/v200_uncapped_weights.py",
                    "crates/borsuk/src/bin/v200_uncapped_cover_preflight.rs",
                    "docs/research/v200-linear-priced-cover-prereg.md"}
        if not required.issubset(tar.getnames()):
            raise ValueError("V200 source archive lacks frozen gate")
    archive_sha = hashlib.sha256(archive).hexdigest()
    archive_key = f"research/v200-uncapped-cover/{commit}/sources/{archive_sha}.tar.gz"
    prefix = f"research/v200-uncapped-cover/{commit}/runs/{attempt}"
    session = boto3.Session(profile_name="causality", region_name=REGION)
    ec2, s3 = session.client("ec2"), session.client("s3")
    if not missing(s3, prefix + "/reservation.json") or not missing(s3, prefix + "/terminal.json"):
        raise ValueError("attempt already registered")
    active = ec2.describe_instances(Filters=[
        {"Name":"tag:Name","Values":["borsuk-*"]},
        {"Name":"instance-state-name","Values":["pending","running","stopping"]},
    ])
    if any(item.get("Instances") for item in active["Reservations"]):
        raise ValueError("another BORSUK worker is active")
    for _, key, size, _ in INPUTS:
        if s3.head_object(Bucket=BUCKET, Key=key)["ContentLength"] != size:
            raise ValueError("V200 frozen input length differs")
    if missing(s3, archive_key):
        put_if_absent(archive_key, archive)
    elif s3.head_object(Bucket=BUCKET, Key=archive_key)["ContentLength"] != len(archive):
        raise ValueError("source archive length differs")
    put_if_absent(prefix + "/reservation.json", json.dumps({
        "schema": SCHEMA, "source_commit": commit,
        "source_archive_sha256": archive_sha,
        "interruption_policy": "discard and restart complete cell at a new attempt",
        "output_prefix": f"s3://{BUCKET}/{prefix}",
    }, sort_keys=True).encode())
    script = user_data(commit, archive_sha, archive_key, prefix)
    receipt = ec2.run_instances(
        ClientToken="v200-" + hashlib.sha256(prefix.encode()).hexdigest()[:48],
        ImageId=IMAGE, InstanceType="c7i.12xlarge", MinCount=1, MaxCount=1,
        IamInstanceProfile={"Arn":PROFILE_ARN},
        NetworkInterfaces=[{"AssociatePublicIpAddress":True,"DeviceIndex":0,
                            "Groups":[SECURITY_GROUP],"SubnetId":SUBNET}],
        InstanceMarketOptions={"MarketType":"spot","SpotOptions":{
            "InstanceInterruptionBehavior":"terminate","SpotInstanceType":"one-time"}},
        InstanceInitiatedShutdownBehavior="terminate",
        BlockDeviceMappings=[{"DeviceName":"/dev/xvda","Ebs":{
            "DeleteOnTermination":True,"Encrypted":True,"VolumeSize":40,"VolumeType":"gp3"}}],
        TagSpecifications=[{"ResourceType":"instance","Tags":[
            {"Key":"Name","Value":TAG},{"Key":"BorsukAttempt","Value":attempt}]}],
        UserData=base64.b64encode(bootstrap(script).encode()).decode(),
    )
    instance_id = receipt["Instances"][0]["InstanceId"]
    print(json.dumps({"instance_id":instance_id,"output_prefix":prefix,
                      "source_commit":commit}),flush=True)
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
                    raise ValueError("terminal identity differs")
                terminal["terminal_sha256"] = hashlib.sha256(raw).hexdigest()
                print(json.dumps(terminal,sort_keys=True),flush=True)
                if terminal.get("status") != "complete":
                    raise RuntimeError("V200 failed; inspect only closed terminal artifacts")
                if set(ARTIFACTS) != set(terminal.get("artifacts", {})):
                    raise ValueError("complete V200 artifact roster differs")
                for name, identity in terminal["artifacts"].items():
                    body = s3.get_object(Bucket=BUCKET,
                        Key=f"{prefix}/artifacts/{name}")["Body"]
                    digest = hashlib.sha256()
                    size = 0
                    while chunk := body.read(4 * 1024 * 1024):
                        digest.update(chunk)
                        size += len(chunk)
                    if size != identity["bytes"] or digest.hexdigest() != identity["sha256"]:
                        raise ValueError(f"V200 S3 artifact read-back differs: {name}")
                bench = json.loads(s3.get_object(Bucket=BUCKET,
                    Key=f"{prefix}/artifacts/bench.json")["Body"].read())
                if bench.get("fast_path_admitted") != 861 or bench.get("exact_interval_witnesses") != 861:
                    raise ValueError("V200 exact fast-path parity differs")
                print(json.dumps({"instance_id":instance_id,"final_state":"terminated",
                                  "artifact_replay":"pass","bench":bench}),flush=True)
                return
            except ClientError as error:
                if error.response.get("Error",{}).get("Code") not in {"NoSuchKey","404","NotFound"}:
                    raise
            state = ec2.describe_instances(InstanceIds=[instance_id])["Reservations"][0]["Instances"][0]["State"]["Name"]
            if state in {"terminated","shutting-down"}:
                raise RuntimeError("Spot worker stopped before terminal; restart complete cell")
            time.sleep(20)
        raise TimeoutError("V200 Spot cell exceeded wall cap")
    finally:
        try:
            state = ec2.describe_instances(InstanceIds=[instance_id])["Reservations"][0]["Instances"][0]["State"]["Name"]
            if state not in {"terminated","shutting-down"}:
                ec2.terminate_instances(InstanceIds=[instance_id])
        except Exception:
            pass


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--attempt", default="a0001")
    args = parser.parse_args()
    with open("/tmp/borsuk-v200-uncapped-cover-launch.lock","a+") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise RuntimeError("V200 launcher is already active") from error
        launch(args.attempt)


if __name__ == "__main__":
    main()
