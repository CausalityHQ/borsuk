#!/usr/bin/env python3
"""One immutable Spot HTTP gate on the selected V219 ReLAION-1M graph."""

import argparse
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
from scripts.launch_v209_resident_1m_spot import BASELINE
from scripts.launch_v219_reachable_graph_1m_spot import (
    INPUTS as V219_INPUTS, describe_state, downloads,
)

SCHEMA = "borsuk-v220-graph-http-1m-spot-v1"
TAG = "borsuk-v220-graph-http-1m"
IMAGE = "ami-06121aa3085b6f918"
WALL_SECONDS = 5400
V219 = ("research/v219-reachable-graph-1m/"
        "008ab6fbc50e6293e0599a33993c619702109bd9/runs/a0002/")
V219_TERMINAL_SHA = "782fe56ee77a7f16c39e77a6012da63d899e201a694984d919952e894cc90180"
INPUTS = tuple(row for row in V219_INPUTS if row[0] in {
    "plane.bin", "books.bin", "codes.bin", "requests.jsonl"})
ARTIFACTS = ("raw.jsonl", "summary.json", "quality.json", "server-resources.json",
             "build.log", "install.log", "run-closed.log")


def user_data(commit: str, archive_sha: str, archive_key: str, prefix: str,
              frozen: tuple, late: tuple) -> str:
    template = r'''#!/bin/bash
set -euo pipefail
systemd-run --unit=v220-hard-stop --on-active=@@WALL@@s /usr/sbin/shutdown -h now
root=/mnt/v220-graph-http-1m
mkdir -p "$root" && cd "$root"
worker_started_epoch=$(date +%s)
phase=bootstrap
finish() {
  code=$?
  trap - EXIT TERM
  set +e
  cd "$root"
  [ -n "${server_pid:-}" ] && kill "$server_pid" 2>/dev/null
  cp run.log run-closed.log || code=96
  for name in @@ARTIFACTS@@; do
    if [ -f "$name" ]; then
      aws s3 cp "$name" "s3://@@BUCKET@@/@@PREFIX@@/artifacts/$name" --only-show-errors || code=96
    fi
  done
  token=$(curl -fsS -X PUT -H 'X-aws-ec2-metadata-token-ttl-seconds: 60' http://169.254.169.254/latest/api/token)
  instance_id=$(curl -fsS -H "X-aws-ec2-metadata-token: $token" http://169.254.169.254/latest/meta-data/instance-id)
  identity=$(curl -fsS -H "X-aws-ec2-metadata-token: $token" http://169.254.169.254/latest/dynamic/instance-identity/document)
  INSTANCE_ID="$instance_id" IDENTITY="$identity" STARTED="$worker_started_epoch" \
    EXIT_CODE="$code" PHASE="$phase" python3 - <<'PY' >terminal.json
import hashlib,json,os
from pathlib import Path
artifacts={}
for name in @@ARTIFACTS_PY@@:
    path=Path(name)
    if path.is_file():
        value=hashlib.sha256(path.read_bytes()).hexdigest()
        artifacts[name]={'bytes':path.stat().st_size,'sha256':value}
code=int(os.environ['EXIT_CODE']);phase=os.environ['PHASE']
identity=json.loads(os.environ['IDENTITY'])
print(json.dumps({'schema':'@@SCHEMA@@','source_commit':'@@COMMIT@@',
    'source_archive_sha256':'@@ARCHIVE_SHA@@','instance_id':os.environ['INSTANCE_ID'],
    'instance_type':identity['instanceType'],'region':identity['region'],
    'availability_zone':identity['availabilityZone'],
    'worker_started_epoch':int(os.environ['STARTED']),
    'worker_finished_epoch':__import__('time').time(),
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
dnf install -y -q python3.12 gcc gcc-c++ cmake perl tar gzip time >install.log 2>&1
export RUSTUP_HOME="$root/.rustup" CARGO_HOME="$root/.cargo" CARGO_TARGET_DIR="$root/target" CARGO_BUILD_JOBS=6
curl -fsSL https://sh.rustup.rs | sh -s -- -y --profile minimal --default-toolchain 1.98.0 >>install.log 2>&1
export PYTHONPATH="$root/repo" OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1
phase=inputs
@@DOWNLOADS@@
phase=build
cd repo
"$CARGO_HOME/bin/cargo" build --release --locked -p borsuk --example v220_graph_http \
  --jobs 6 >"$root/build.log" 2>&1
cd "$root"
phase=serve
"$CARGO_TARGET_DIR/release/examples/v220_graph_http" \
  prep.json build-summary.json plane.bin graph.bin map.u32 books.bin codes.bin \
  127.0.0.1:8765 &
server_pid=$!
for attempt in $(seq 1 60); do
  if curl -fsS http://127.0.0.1:8765/health >/dev/null 2>&1; then break; fi
  kill -0 "$server_pid"
  sleep 1
done
curl -fsS http://127.0.0.1:8765/health
phase=measure
python3.12 -m scripts.v220_bench_graph_http --prep prep.json --requests requests.jsonl \
  --raw raw.jsonl --summary summary.json
SERVER_PID="$server_pid" python3 - <<'PY' >server-resources.json
import json,os
from pathlib import Path
pid=os.environ['SERVER_PID']
lines=Path(f'/proc/{pid}/status').read_text().splitlines()
fields={line.split(':',1)[0]:int(line.split(':',1)[1].split()[0])*1024
        for line in lines if line.startswith(('VmRSS:','VmHWM:'))}
print(json.dumps({'pid':int(pid),'peak_rss_bytes':fields['VmHWM'],
                  'rss_bytes':fields['VmRSS']},sort_keys=True))
PY
kill "$server_pid"
wait "$server_pid" || true
server_pid=
aws s3api put-object --bucket '@@BUCKET@@' --key '@@PREFIX@@/sealed/raw.jsonl' \
  --body raw.jsonl --if-none-match '*' --no-cli-pager >/dev/null
raw_sha=$(sha256sum raw.jsonl | cut -d ' ' -f1)
remote_sha=$(aws s3 cp 's3://@@BUCKET@@/@@PREFIX@@/sealed/raw.jsonl' - \
  --only-show-errors | sha256sum | cut -d ' ' -f1)
[ "$raw_sha" = "$remote_sha" ]
phase=truth
@@LATE_DOWNLOADS@@
python3.12 -m scripts.v220_score_graph_http --raw raw.jsonl --summary summary.json \
  --previous v219-raw.jsonl --truth v198-raw.jsonl --output quality.json
python3 - <<'PY'
import json
s=json.load(open('summary.json'));r=json.load(open('server-resources.json'))
assert s['p95_ns'] < 100_000_000 and s['p99_ns'] < 150_000_000
assert s['qps'] >= 100 and r['peak_rss_bytes'] <= 3*1024**3
PY
phase=complete
'''
    replacements = {
        "WALL": str(WALL_SECONDS), "ARTIFACTS": " ".join(ARTIFACTS),
        "ARTIFACTS_PY": repr(ARTIFACTS), "BUCKET": BUCKET,
        "PREFIX": prefix, "SCHEMA": SCHEMA, "COMMIT": commit,
        "ARCHIVE_SHA": archive_sha, "ARCHIVE_KEY": archive_key,
        "DOWNLOADS": downloads(INPUTS + frozen), "LATE_DOWNLOADS": downloads(late),
    }
    for name, value in replacements.items():
        template = template.replace("@@" + name + "@@", value)
    if "@@" in template:
        raise ValueError("unresolved V220 worker placeholder")
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
    archive_key = f"research/v220-graph-http-1m/{commit}/sources/{archive_sha}.tar.gz"
    prefix = f"research/v220-graph-http-1m/{commit}/runs/{attempt}"
    session = boto3.Session(profile_name="causality", region_name=REGION)
    ec2, s3 = session.client("ec2"), session.client("s3")
    terminal_raw = s3.get_object(Bucket=BUCKET, Key=V219 + "terminal.json")["Body"].read()
    terminal = json.loads(terminal_raw)
    if (hashlib.sha256(terminal_raw).hexdigest() != V219_TERMINAL_SHA
            or terminal["status"] != "complete"):
        raise ValueError("V219 predecessor terminal differs")
    names = ("prep.json", "build-summary.json", "graph.bin", "map.u32")
    frozen = tuple((name, V219 + "artifacts/" + name,
                    terminal["artifacts"][name]["bytes"],
                    terminal["artifacts"][name]["sha256"]) for name in names)
    previous = terminal["artifacts"]["raw.jsonl"]
    late = (("v219-raw.jsonl", V219 + "artifacts/raw.jsonl",
             previous["bytes"], previous["sha256"]), BASELINE)
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
        "k": 100, "concurrency": 8, "cache_state": "resident after authenticated hydration",
        "transport": "persistent HTTP/1.1 loopback", "v219_terminal_sha256": V219_TERMINAL_SHA,
        "comparator": "none; self-gate only", "instance_type": "c7i.4xlarge",
        "region": REGION, "vector_body_gets_basis": "zero by construction; no object client",
        "gate": "exact V219 IDs;99664 GT100 hits;p05=98;p95<100ms;p99<150ms;QPS>=100;RSS<=3GiB;0 vector GETs by construction",
        "interruption_policy": "discard and restart full cell under a new attempt",
        "output_prefix": f"s3://{BUCKET}/{prefix}",
    }, sort_keys=True).encode())
    receipt = ec2.run_instances(
        ClientToken="v220-" + hashlib.sha256(prefix.encode()).hexdigest()[:48],
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
        UserData=user_data(commit, archive_sha, archive_key, prefix, frozen, late),
    )
    instance_id = receipt["Instances"][0]["InstanceId"]
    print(json.dumps({"instance_id": instance_id, "output_prefix": prefix,
                      "source_commit": commit, "source_archive_sha256": archive_sha}), flush=True)
    try:
        deadline = time.monotonic() + WALL_SECONDS + 600
        while time.monotonic() < deadline:
            try:
                raw = s3.get_object(Bucket=BUCKET, Key=prefix + "/terminal.json")["Body"].read()
                value = json.loads(raw)
                ec2.terminate_instances(InstanceIds=[instance_id])
                ec2.get_waiter("instance_terminated").wait(InstanceIds=[instance_id])
                if (value.get("schema") != SCHEMA or value.get("instance_id") != instance_id
                        or value.get("source_commit") != commit
                        or value.get("source_archive_sha256") != archive_sha):
                    raise ValueError("V220 terminal identity differs")
                value["terminal_sha256"] = hashlib.sha256(raw).hexdigest()
                print(json.dumps(value, sort_keys=True), flush=True)
                if value.get("status") != "complete" or set(value.get("artifacts", {})) != set(ARTIFACTS):
                    raise RuntimeError("V220 failed; inspect only closed terminal artifacts")
                for name, identity in value["artifacts"].items():
                    body = s3.get_object(Bucket=BUCKET, Key=f"{prefix}/artifacts/{name}")["Body"]
                    data = body.read()
                    if len(data) != identity["bytes"] or hashlib.sha256(data).hexdigest() != identity["sha256"]:
                        raise ValueError(f"V220 artifact readback differs: {name}")
                print(json.dumps({"instance_id": instance_id,
                                  "final_state": "terminated", "artifact_replay": "pass"}), flush=True)
                return
            except ClientError as error:
                if error.response.get("Error", {}).get("Code") not in {"NoSuchKey", "404", "NotFound"}:
                    raise
            if describe_state(ec2, instance_id) in {"terminated", "shutting-down"}:
                raise RuntimeError("V220 worker stopped before terminal")
            time.sleep(20)
        raise TimeoutError("V220 Spot cell exceeded wall cap")
    finally:
        for retry in range(5):
            try:
                if describe_state(ec2, instance_id) != "terminated":
                    ec2.terminate_instances(InstanceIds=[instance_id])
                    ec2.get_waiter("instance_terminated").wait(
                        InstanceIds=[instance_id], WaiterConfig={"Delay": 5, "MaxAttempts": 24})
                break
            except Exception as error:
                if retry == 4:
                    raise RuntimeError(
                        f"V220 cleanup unconfirmed for {instance_id}; terminate it manually"
                    ) from error
                time.sleep(5)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--attempt", default="a0001")
    args = parser.parse_args()
    with open("/tmp/borsuk-v220-graph-http-launch.lock", "a+") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        launch(args.attempt)
