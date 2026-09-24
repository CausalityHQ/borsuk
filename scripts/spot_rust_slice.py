#!/usr/bin/env python3
"""Run one immutable, narrow BORSUK Rust test on Causality Spot."""

from __future__ import annotations

import argparse
import base64
import gzip
import hashlib
import io
import json
import subprocess
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

BUCKET = "borsuk-bench-453182569524-euc1"
REGION = "eu-central-1"
ROOT = Path(__file__).resolve().parent.parent


def command(*args: str) -> str:
    return subprocess.check_output(args, cwd=ROOT, text=True).strip()


def source_archive() -> tuple[str, bytes, str]:
    if command("git", "status", "--porcelain"):
        raise ValueError("source tree is dirty")
    commit = command("git", "rev-parse", "HEAD")
    archive = subprocess.check_output(["git", "archive", "--format=tar", "HEAD"], cwd=ROOT)
    buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=buffer, mode="wb", mtime=0) as stream:
        stream.write(archive)
    data = buffer.getvalue()
    return commit, data, hashlib.sha256(data).hexdigest()


def user_data(commit: str, sha: str, size: int, archive_uri: str, prefix: str, test: str) -> str:
    # All interpolated values are constrained below to hex, digits and safe
    # identifier characters before they reach this shell program.
    return f"""#!/bin/bash
set -euo pipefail
root=/mnt/borsuk-rust-slice
mkdir -p "$root" && cd "$root"
export BORSUK_SOURCE_COMMIT={commit} BORSUK_SOURCE_SHA={sha}
export BORSUK_OUTPUT_PREFIX=s3://{BUCKET}/{prefix}
phase=bootstrap
started=$(date +%s)
finish() {{
  code=$?
  trap - EXIT TERM
  set +e
  token=$(curl -fsS -X PUT -H 'X-aws-ec2-metadata-token-ttl-seconds: 60' http://169.254.169.254/latest/api/token)
  instance=$(curl -fsS -H "X-aws-ec2-metadata-token: $token" http://169.254.169.254/latest/meta-data/instance-id)
  for name in install.log test.log test-resources.txt; do
    if [ -f "$name" ]; then
      aws s3 cp "$name" "$BORSUK_OUTPUT_PREFIX/artifacts/$name" --only-show-errors || code=96
    fi
  done
  BORSUK_CODE="$code" BORSUK_PHASE="$phase" BORSUK_INSTANCE="$instance" BORSUK_STARTED="$started" python3 - <<'PY' >terminal.json
import hashlib,json,os
from pathlib import Path
artifacts={{}}
for name in ('install.log','test.log','test-resources.txt'):
    path=Path(name)
    if path.is_file():
        raw=path.read_bytes()
        artifacts[name]={{'bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest()}}
code=int(os.environ['BORSUK_CODE']); phase=os.environ['BORSUK_PHASE']
print(json.dumps({{'schema':'borsuk-rust-slice-spot-v1','status':'complete' if code==0 and phase=='complete' else 'failed',
  'phase':phase,'exit_code':code,'source_commit':os.environ['BORSUK_SOURCE_COMMIT'],
  'source_archive_sha256':os.environ['BORSUK_SOURCE_SHA'],'instance_id':os.environ['BORSUK_INSTANCE'],
  'elapsed_seconds':int(__import__('time').time())-int(os.environ['BORSUK_STARTED']),
  'test_filter':'{test}','artifacts':artifacts}},sort_keys=True,separators=(',',':')))
PY
  aws s3 cp terminal.json "$BORSUK_OUTPUT_PREFIX/terminal.json" --only-show-errors
  shutdown -h now || true
  exit "$code"
}}
trap finish EXIT
trap 'exit 97' TERM
phase=source
aws s3 cp {archive_uri} source.tar.gz --only-show-errors
[ "$(stat -c%s source.tar.gz)" = "{size}" ]
printf '%s  source.tar.gz\\n' {sha} | sha256sum -c -
mkdir repo && tar -xzf source.tar.gz -C repo
phase=install
dnf install -y -q gcc gcc-c++ cmake perl tar gzip time >install.log 2>&1
export RUSTUP_HOME="$root/.rustup" CARGO_HOME="$root/.cargo" CARGO_TARGET_DIR="$root/target" CARGO_BUILD_JOBS=4
curl -fsSL https://sh.rustup.rs | sh -s -- -y --profile minimal --default-toolchain 1.98.0 >>install.log 2>&1
phase=test
timeout --signal=TERM --kill-after=30 3300 /usr/bin/time -v \\
  "$CARGO_HOME/bin/cargo" test --manifest-path repo/Cargo.toml --locked \\
  -p borsuk --lib {test} --jobs 4 -- --exact --nocapture >test.log 2>test-resources.txt
phase=complete
exit 0
"""


def conditional_put(path: Path, key: str) -> None:
    subprocess.run(
        [
            "aws", "s3api", "put-object", "--profile", "causality",
            "--region", REGION, "--bucket", BUCKET, "--key", key,
            "--body", str(path), "--if-none-match", "*", "--output", "json",
        ],
        check=True,
        stdout=subprocess.DEVNULL,
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("test_filter")
    args = parser.parse_args()
    test = args.test_filter
    if not test or any(character not in "abcdefghijklmnopqrstuvwxyz_0123456789:" for character in test):
        raise ValueError("test filter must be a fully qualified Rust test name")
    commit, archive, sha = source_archive()
    source_key = f"research/rust-slices/{commit}/source/{sha}.tar.gz"
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    prefix = f"research/rust-slices/{commit}/runs/{run_id}/a0001"
    session = boto3.Session(profile_name="causality", region_name=REGION)
    s3 = session.client("s3")
    ec2 = session.client("ec2")
    with tempfile.NamedTemporaryFile() as source:
        source.write(archive)
        source.flush()
        try:
            conditional_put(Path(source.name), source_key)
        except subprocess.CalledProcessError:
            existing = s3.get_object(Bucket=BUCKET, Key=source_key)["Body"].read()
            if len(existing) != len(archive) or hashlib.sha256(existing).hexdigest() != sha:
                raise
    with tempfile.NamedTemporaryFile(mode="w") as reservation:
        json.dump({"schema":"borsuk-rust-slice-reservation-v1","source_commit":commit,
                   "source_archive_sha256":sha,"test_filter":test,"output_prefix":prefix}, reservation)
        reservation.flush()
        conditional_put(Path(reservation.name), f"{prefix}/reservation.json")
    archive_uri = f"s3://{BUCKET}/{source_key}"
    script = user_data(commit, sha, len(archive), archive_uri, prefix, test)
    spec = {
        "ClientToken": "borsuk-rust-" + hashlib.sha256(prefix.encode()).hexdigest()[:48],
        "ImageId":"ami-06121aa3085b6f918","InstanceType":"c7i.8xlarge","MinCount":1,"MaxCount":1,
        "IamInstanceProfile":{"Arn":"arn:aws:iam::453182569524:instance-profile/borsuk-bench-profile"},
        "NetworkInterfaces":[{"AssociatePublicIpAddress":True,"DeviceIndex":0,
                              "Groups":["sg-0b1fd3e4fbde4af0d"],"SubnetId":"subnet-0a12dbed0ca6fac25"}],
        "InstanceMarketOptions":{"MarketType":"spot","SpotOptions":{
            "InstanceInterruptionBehavior":"terminate","SpotInstanceType":"one-time"}},
        "InstanceInitiatedShutdownBehavior":"terminate",
        "BlockDeviceMappings":[{"DeviceName":"/dev/xvda","Ebs":{
            "DeleteOnTermination":True,"Encrypted":True,"VolumeSize":120,"VolumeType":"gp3"}}],
        "TagSpecifications":[{"ResourceType":"instance","Tags":[
            {"Key":"Name","Value":"borsuk-rust-slice"},{"Key":"BorsukAttempt","Value":run_id}]}],
        "UserData":base64.b64encode(script.encode()).decode(),
    }
    instance = ec2.run_instances(**spec)["Instances"][0]["InstanceId"]
    print(json.dumps({"instance_id":instance,"source_commit":commit,"source_sha256":sha,
                      "output_prefix":f"s3://{BUCKET}/{prefix}","test_filter":test}), flush=True)
    deadline = time.monotonic() + 5400
    terminal = None
    while time.monotonic() < deadline:
        try:
            raw = s3.get_object(Bucket=BUCKET, Key=f"{prefix}/terminal.json")["Body"].read()
            terminal = json.loads(raw)
            print(json.dumps({"terminal_sha256":hashlib.sha256(raw).hexdigest(),
                              "terminal":terminal}), flush=True)
            break
        except ClientError as error:
            if str(error.response.get("Error", {}).get("Code")) not in {"NoSuchKey","404","NotFound"}:
                raise
        state = ec2.describe_instances(InstanceIds=[instance])["Reservations"][0]["Instances"][0]["State"]["Name"]
        if state in {"terminated","shutting-down"}:
            raise RuntimeError(f"Spot {instance} {state} without terminal")
        time.sleep(20)
    if terminal is None:
        ec2.terminate_instances(InstanceIds=[instance])
        raise TimeoutError("Spot test exceeded 90 minutes")
    if terminal["status"] != "complete":
        print("narrow test failed; terminal and logs are preserved", flush=True)
    for _ in range(30):
        state = ec2.describe_instances(InstanceIds=[instance])["Reservations"][0]["Instances"][0]["State"]["Name"]
        if state == "terminated":
            break
        time.sleep(10)
    else:
        ec2.terminate_instances(InstanceIds=[instance])
    print(json.dumps({"instance_id":instance,"final_state":"terminated"}), flush=True)
    return 0 if terminal["status"] == "complete" else 1


if __name__ == "__main__":
    raise SystemExit(main())
