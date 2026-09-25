#!/usr/bin/env python3
"""One sealed, identity-disjoint resident FP16 source gate on Spot."""

from __future__ import annotations

import argparse
import base64
import fcntl
import gzip
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
from scripts.launch_v195_used_1m_rerank_diagnostic_spot import INPUTS as V195_INPUTS
from scripts.launch_v196_resident_fp16_preflight_spot import download_script

SCHEMA = "borsuk-v197-fresh-resident-fp16-source-spot-v1"
TAG = "borsuk-v197-fresh-resident-fp16-source"
IMAGE = "ami-06121aa3085b6f918"
WALL_SECONDS = 7_200
V196_PREFIX = ("research/v196-resident-fp16-preflight/"
               "c5fbb27ffba5fe5a78f15a4fb9e9c836a2afba04/runs/a0001/")
NEEDED = {"source.parquet", "old-layout.npy", "old-sq8.bin", "order.npy",
          "v164-terminal.json", "router/manifest.json", "router/summaries.bin",
          "router/books.bin", "router/codes.bin", "router/low.bin",
          "router/step.bin", "v192-result.json", "v189-features.jsonl",
          "v189-fit-labels.jsonl"}
BASE_INPUTS = tuple(row for row in V195_INPUTS if row[0] in NEEDED)
if {row[0] for row in BASE_INPUTS} != NEEDED:
    raise RuntimeError("V197 predecessor input roster differs")
PLANE_SHA = "1bce4288b38d88384503d8cfeae21667f45dbfb62303ce510f676fc0d66d4c47"
EXTRA_INPUTS = (
    ("plane.bin", V196_PREFIX + "artifacts/plane.bin", 1_544_000_064, PLANE_SHA),
    ("v196-terminal.json", V196_PREFIX + "terminal.json", 1_288,
     "a73ce19c371751d391d9c35530e6529adbaca9e8c85b525d1d6028218c47a5f3"),
)
ARTIFACTS = ("out/features.jsonl", "out/prepare-seal.json",
             "out/plans.jsonl", "out/plan-seal.json",
             "out/raw.jsonl", "out/cases.jsonl", "out/summary.json",
             "bench.json", "decision.json", "prepare-resources.txt",
             "plan-resources.txt", "evaluate-resources.txt",
             "bench-resources.txt", "build.log", "run-closed.log")


def gate_args() -> str:
    return ("--output out --source source.parquet --old-layout old-layout.npy "
            "--old-sq8 old-sq8.bin --router router --order order.npy "
            "--v164-terminal v164-terminal.json --v192-result v192-result.json "
            "--v189-features v189-features.jsonl "
            "--v189-fit-labels v189-fit-labels.jsonl --plane plane.bin")


def user_data(commit: str, archive_sha: str, archive_key: str,
              prefix: str) -> str:
    template = r'''#!/bin/bash
set -euo pipefail
systemd-run --unit=v197-hard-stop --on-active=@@WALL@@s /usr/sbin/shutdown -h now
root=/mnt/v197-fresh-resident-fp16
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
dnf install -y -q python3.12 python3.12-pip gcc gcc-c++ cmake perl tar gzip time
python3.12 -m venv .venv
.venv/bin/pip install -q numpy==1.26.4 pyarrow==17.0.0
export RUSTUP_HOME="$root/.rustup" CARGO_HOME="$root/.cargo" CARGO_TARGET_DIR="$root/target" CARGO_BUILD_JOBS=6
curl -fsSL https://sh.rustup.rs | sh -s -- -y --profile minimal --default-toolchain 1.98.0
export PYTHONPATH="$root/repo" OPENBLAS_NUM_THREADS=8 OMP_NUM_THREADS=8 MKL_NUM_THREADS=8
mkdir router
phase=inputs
@@DOWNLOAD@@
phase=prepare
/usr/bin/time -v -o prepare-resources.txt .venv/bin/python -m scripts.v197_fresh_resident_fp16_source \
  prepare @@GATE_ARGS@@
aws s3api put-object --bucket '@@BUCKET@@' --key '@@PREFIX@@/sealed/features.jsonl' \
  --body out/features.jsonl --if-none-match '*' --no-cli-pager >/dev/null
aws s3api put-object --bucket '@@BUCKET@@' --key '@@PREFIX@@/sealed/prepare-seal.json' \
  --body out/prepare-seal.json --if-none-match '*' --no-cli-pager >/dev/null
prepare_sha=$(sha256sum out/prepare-seal.json | cut -d ' ' -f1)
remote_sha=$(aws s3 cp 's3://@@BUCKET@@/@@PREFIX@@/sealed/prepare-seal.json' - \
  --only-show-errors | sha256sum | cut -d ' ' -f1)
[ "$prepare_sha" = "$remote_sha" ]
phase=plan
/usr/bin/time -v -o plan-resources.txt .venv/bin/python -m scripts.v197_fresh_resident_fp16_source \
  plan @@GATE_ARGS@@ --prepare-sha256 "$prepare_sha"
aws s3api put-object --bucket '@@BUCKET@@' --key '@@PREFIX@@/sealed/plans.jsonl' \
  --body out/plans.jsonl --if-none-match '*' --no-cli-pager >/dev/null
aws s3api put-object --bucket '@@BUCKET@@' --key '@@PREFIX@@/sealed/plan-seal.json' \
  --body out/plan-seal.json --if-none-match '*' --no-cli-pager >/dev/null
plan_sha=$(sha256sum out/plan-seal.json | cut -d ' ' -f1)
remote_sha=$(aws s3 cp 's3://@@BUCKET@@/@@PREFIX@@/sealed/plan-seal.json' - \
  --only-show-errors | sha256sum | cut -d ' ' -f1)
[ "$plan_sha" = "$remote_sha" ]
phase=evaluate
/usr/bin/time -v -o evaluate-resources.txt .venv/bin/python -m scripts.v197_fresh_resident_fp16_source \
  evaluate @@GATE_ARGS@@ --plan-sha256 "$plan_sha"
case_count=$(python3 -c 'import json;print(json.load(open("out/summary.json"))["resident_case_count"])')
if [ "$case_count" = 512 ]; then
  phase=build
  cd repo
  "$CARGO_HOME/bin/cargo" build --release --locked -p borsuk --bin v196_resident_fp16_preflight --jobs 6 >"$root/build.log" 2>&1
  cd "$root"
  phase=bench
  /usr/bin/time -v -o bench-resources.txt "$CARGO_TARGET_DIR/release/v196_resident_fp16_preflight" \
    plane.bin '@@PLANE_SHA@@' '@@SOURCE_SHA@@' 1000000 768 196 2147483648 out/cases.jsonl 3456 >bench.json
else
  printf 'Rust parity skipped: optional plans infeasible for %s queries\n' "$((512-case_count))" >build.log
  printf 'Rust parity skipped\n' >bench-resources.txt
  python3 - <<'PY' >bench.json
import json
count=json.load(open('out/summary.json'))['resident_case_count']
print(json.dumps({'schema':'borsuk-resident-fp16-preflight-v2',
    'status':'skipped-infeasible-plans','cases':count},sort_keys=True))
PY
fi
phase=decision
python3 - <<'PY' >decision.json
import json
source=json.load(open('out/summary.json'))
bench=json.load(open('bench.json'))
reps=bench.get('repetitions',[])
rust_pass=(len(reps)==10 and bench.get('exact_returned_sets')==5120
    and bench.get('first_ordinal')==3456
    and max(row['p95_ns'] for row in reps)<=2_000_000
    and max(row['p99_ns'] for row in reps)<=5_000_000
    and bench['process_peak_rss_bytes']<=2*1024**3)
decision=('advance-real-query-live-s3' if source['decision']=='advance-real-query-live-s3'
          and rust_pass else 'revise-candidate-plan-or-precision')
print(json.dumps({'schema':'borsuk-v197-fresh-resident-fp16-decision-v1',
    'source_decision':source['decision'],'rust_parity_and_resource_pass':rust_pass,
    'decision':decision},sort_keys=True,separators=(',',':')))
PY
phase=complete
'''
    replacements = {
        "WALL": str(WALL_SECONDS), "ARTIFACTS": " ".join(ARTIFACTS),
        "ARTIFACTS_PY": repr(ARTIFACTS), "BUCKET": BUCKET,
        "PREFIX": prefix, "SCHEMA": SCHEMA, "COMMIT": commit,
        "ARCHIVE_SHA": archive_sha, "ARCHIVE_KEY": archive_key,
        "DOWNLOAD": download_script(BASE_INPUTS + EXTRA_INPUTS),
        "GATE_ARGS": gate_args(), "PLANE_SHA": PLANE_SHA,
        "SOURCE_SHA": "2796b579f37afe99ca4aff57e282335a6a79ad30596645957d26326a0560cf86",
    }
    for name, value in replacements.items():
        template = template.replace("@@" + name + "@@", value)
    if "@@" in template:
        raise ValueError("unresolved V197 worker placeholder")
    return template


def bootstrap(script: str) -> str:
    payload = base64.b64encode(gzip.compress(script.encode(), mtime=0)).decode()
    result = ("#!/bin/bash\nset -euo pipefail\n"
              "base64 -d <<'V197_WORKER' | gzip -d | bash\n"
              + payload + "\nV197_WORKER\n")
    if len(result.encode()) > 16_384:
        raise ValueError("compressed V197 user data exceeds EC2 limit")
    return result


def launch(attempt: str) -> None:
    if len(attempt) != 5 or not attempt.startswith("a") or not attempt[1:].isdigit():
        raise ValueError("attempt must be aNNNN")
    if subprocess.check_output(["git", "status", "--porcelain"], text=True).strip():
        raise ValueError("worktree must be clean")
    commit = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    subprocess.run(["git", "fetch", "origin", "main"], check=True)
    if subprocess.run(["git", "merge-base", "--is-ancestor", "HEAD", "origin/main"],
                      check=False).returncode != 0:
        raise ValueError("V197 source commit is not pushed to origin/main")
    archive = archive_source(commit)
    with tarfile.open(fileobj=io.BytesIO(archive), mode="r:gz") as tar:
        required = {"scripts/v197_fresh_resident_fp16_source.py",
                    "crates/borsuk/src/bin/v196_resident_fp16_preflight.rs",
                    "docs/research/v197-fresh-resident-fp16-prereg.md"}
        if not required.issubset(tar.getnames()):
            raise ValueError("V197 source archive lacks frozen gate")
    archive_sha = hashlib.sha256(archive).hexdigest()
    archive_key = f"research/v197-fresh-resident-fp16-source/{commit}/sources/{archive_sha}.tar.gz"
    prefix = f"research/v197-fresh-resident-fp16-source/{commit}/runs/{attempt}"
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
    for _, key, size, _ in BASE_INPUTS + EXTRA_INPUTS:
        if s3.head_object(Bucket=BUCKET, Key=key)["ContentLength"] != size:
            raise ValueError("frozen input length differs")
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
        ClientToken="v197-" + hashlib.sha256(prefix.encode()).hexdigest()[:48],
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
                    raise RuntimeError("V197 failed; inspect only closed terminal artifacts")
                if set(ARTIFACTS) != set(terminal.get("artifacts", {})):
                    raise ValueError("complete V197 artifact roster differs")
                for name, identity in terminal["artifacts"].items():
                    body = s3.get_object(Bucket=BUCKET,
                        Key=f"{prefix}/artifacts/{name}")["Body"]
                    digest = hashlib.sha256()
                    size = 0
                    while chunk := body.read(4 * 1024 * 1024):
                        digest.update(chunk)
                        size += len(chunk)
                    if size != identity["bytes"] or digest.hexdigest() != identity["sha256"]:
                        raise ValueError(f"V197 S3 artifact read-back differs: {name}")
                raw_time = s3.head_object(Bucket=BUCKET,
                    Key=f"{prefix}/artifacts/out/raw.jsonl")["LastModified"]
                for name in ("features.jsonl","prepare-seal.json",
                             "plans.jsonl","plan-seal.json"):
                    sealed = s3.get_object(Bucket=BUCKET,
                        Key=f"{prefix}/sealed/{name}")
                    body = sealed["Body"].read()
                    if (hashlib.sha256(body).hexdigest() !=
                            terminal["artifacts"]["out/" + name]["sha256"]
                            or sealed["LastModified"] > raw_time):
                        raise ValueError(f"V197 pre-truth seal differs: {name}")
                print(json.dumps({"instance_id":instance_id,"final_state":"terminated",
                                  "artifact_replay":"pass"}),flush=True)
                return
            except ClientError as error:
                if error.response.get("Error",{}).get("Code") not in {"NoSuchKey","404","NotFound"}:
                    raise
            state = ec2.describe_instances(InstanceIds=[instance_id])["Reservations"][0]["Instances"][0]["State"]["Name"]
            if state in {"terminated","shutting-down"}:
                raise RuntimeError("Spot worker stopped before terminal; restart complete cell")
            time.sleep(20)
        raise TimeoutError("V197 Spot cell exceeded wall cap")
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
    with open("/tmp/borsuk-v197-fresh-resident-fp16-launch.lock","a+") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise RuntimeError("V197 launcher is already active") from error
        launch(args.attempt)


if __name__ == "__main__":
    main()
