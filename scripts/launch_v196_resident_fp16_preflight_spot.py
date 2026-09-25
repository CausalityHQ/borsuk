#!/usr/bin/env python3
"""One immutable Causality Spot preflight of the resident FP16 tier."""

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
from scripts.launch_v195_used_1m_rerank_diagnostic_spot import INPUTS as V195_INPUTS

V195_HASHES = {
    "terminal": "62a2a7b190983b5e422f679edf8a5120164f9ab886a3e5db5bd32eefc2abdc02",
    "seal": "77267a71e9c9f40cc086af4e44029943b8a09559b59a7a7a998d652ee9026ca7",
    "raw": "998cc015f670e470bbb435d47fc49d0aac5b22d0315a7d51eae326e4e48e18fa",
    "summary": "e4ac85f650a5c8f34f81a77e03c36208d6b952ae7aa732450d272ec493069d20",
}

SCHEMA = "borsuk-v196-resident-fp16-preflight-spot-v1"
TAG = "borsuk-v196-resident-fp16-preflight"
IMAGE = "ami-06121aa3085b6f918"
WALL_SECONDS = 7_200
V195_PREFIX = ("research/v195-used-1m-rerank-diagnostic/"
               "9b7bcc581887688247b4fab244a5dab8d37dd3c9/runs/a0001/")
NEEDED = {"source.parquet", "old-layout.npy", "old-sq8.bin", "order.npy",
          "v164-terminal.json", "router/manifest.json", "router/summaries.bin",
          "router/books.bin", "router/codes.bin", "router/low.bin",
          "router/step.bin", "v194-plans.jsonl"}
BASE_INPUTS = tuple(row for row in V195_INPUTS if row[0] in NEEDED)
if {row[0] for row in BASE_INPUTS} != NEEDED:
    raise RuntimeError("V196 predecessor input roster differs")
V195_FILES = (
    ("v195-terminal.json", V195_PREFIX + "terminal.json", V195_HASHES["terminal"]),
    ("v195-seal.json", V195_PREFIX + "artifacts/diagnostic-seal.json", V195_HASHES["seal"]),
    ("v195-raw.jsonl", V195_PREFIX + "artifacts/raw.jsonl", V195_HASHES["raw"]),
    ("v195-summary.json", V195_PREFIX + "artifacts/summary.json", V195_HASHES["summary"]),
)
ARTIFACTS = ("case-summary.json", "cases.jsonl", "plane.bin", "bench.json",
             "summary.json", "builder-resources.txt", "bench-resources.txt",
             "build.log", "run-closed.log")


def download_script(inputs: tuple[tuple[str, str, int, str], ...]) -> str:
    return "\n".join(
        f"aws s3 cp 's3://{BUCKET}/{key}' '{name}' --only-show-errors\n"
        f"[ \"$(stat -c%s '{name}')\" = '{size}' ]\n"
        f"printf '%s  %s\\n' '{digest}' '{name}' | sha256sum -c -"
        for name, key, size, digest in inputs
    )


def user_data(commit: str, archive_sha: str, archive_key: str,
              prefix: str, inputs: tuple[tuple[str, str, int, str], ...]) -> str:
    template = r'''#!/bin/bash
set -euo pipefail
systemd-run --unit=v196-hard-stop --on-active=@@WALL@@s /usr/sbin/shutdown -h now
root=/mnt/v196-resident-fp16
mkdir -p "$root" && cd "$root"
phase=bootstrap
finish() {
  code=$?
  trap - EXIT TERM
  set +e
  cd "$root"
  cp run.log run-closed.log || code=96
  token=$(curl -fsS -X PUT -H 'X-aws-ec2-metadata-token-ttl-seconds: 60' http://169.254.169.254/latest/api/token)
  instance_id=$(curl -fsS -H "X-aws-ec2-metadata-token: $token" http://169.254.169.254/latest/meta-data/instance-id)
  for name in @@ARTIFACTS@@; do
    if [ -f "$name" ]; then
      aws s3 cp "$name" "s3://@@BUCKET@@/@@PREFIX@@/artifacts/$name" --only-show-errors || code=96
    fi
  done
  INSTANCE_ID="$instance_id" EXIT_CODE="$code" PHASE="$phase" python3 - <<'PY' >terminal.json
import hashlib,json,os
from pathlib import Path
names=@@ARTIFACTS_PY@@
artifacts={}
for name in names:
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
phase=cases
/usr/bin/time -v -o builder-resources.txt .venv/bin/python -m scripts.v196_resident_fp16_cases \
  --source source.parquet --old-layout old-layout.npy --old-sq8 old-sq8.bin \
  --router router --order order.npy --v164-terminal v164-terminal.json \
  --v194-plans v194-plans.jsonl --v195-terminal v195-terminal.json \
  --v195-seal v195-seal.json --v195-raw v195-raw.jsonl \
  --v195-summary v195-summary.json --new-sq8 new-sq8.bin --plane plane.bin \
  --cases cases.jsonl --summary case-summary.json
phase=seal
aws s3api put-object --bucket '@@BUCKET@@' --key '@@PREFIX@@/sealed/case-summary.json' \
  --body case-summary.json --if-none-match '*' --no-cli-pager >/dev/null
local_sha=$(sha256sum case-summary.json | cut -d ' ' -f1)
remote_sha=$(aws s3 cp 's3://@@BUCKET@@/@@PREFIX@@/sealed/case-summary.json' - \
  --only-show-errors | sha256sum | cut -d ' ' -f1)
[ "$local_sha" = "$remote_sha" ]
phase=build
cd repo
"$CARGO_HOME/bin/cargo" build --release --locked -p borsuk --bin v196_resident_fp16_preflight --jobs 6 >"$root/build.log" 2>&1
cd "$root"
phase=bench
plane_sha=$(python3 -c 'import json;print(json.load(open("case-summary.json"))["plane_sha256"])')
/usr/bin/time -v -o bench-resources.txt "$CARGO_TARGET_DIR/release/v196_resident_fp16_preflight" \
  plane.bin "$plane_sha" '@@SOURCE_SHA@@' 1000000 768 196 2147483648 cases.jsonl >bench.json
python3 - <<'PY' >summary.json
import json
cases=json.load(open('case-summary.json'))
bench=json.load(open('bench.json'))
reps=bench['repetitions']
gate=(len(reps)==10 and bench['exact_returned_sets']==5120
      and max(row['p95_ns'] for row in reps)<=2_000_000
      and max(row['p99_ns'] for row in reps)<=5_000_000
      and bench['process_peak_rss_bytes']<=2*1024**3
      and bench['charged_plane_bytes']<=2*1024**3)
print(json.dumps({'schema':'borsuk-v196-resident-fp16-summary-v1',
    'decision':'advance-fresh-holdout' if gate else 'revise-resident-tier',
    'case_summary':cases,'benchmark':bench},sort_keys=True,separators=(',',':')))
PY
phase=complete
'''
    replacements = {
        "WALL": str(WALL_SECONDS), "ARTIFACTS": " ".join(ARTIFACTS),
        "ARTIFACTS_PY": repr(ARTIFACTS), "BUCKET": BUCKET,
        "PREFIX": prefix, "SCHEMA": SCHEMA, "COMMIT": commit,
        "ARCHIVE_SHA": archive_sha, "ARCHIVE_KEY": archive_key,
        "DOWNLOAD": download_script(inputs),
        "SOURCE_SHA": "2796b579f37afe99ca4aff57e282335a6a79ad30596645957d26326a0560cf86",
    }
    for name, value in replacements.items():
        template = template.replace("@@" + name + "@@", value)
    if "@@" in template:
        raise ValueError("unresolved V196 worker placeholder")
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
        raise ValueError("V196 source commit is not pushed to origin/main")
    archive = archive_source(commit)
    with tarfile.open(fileobj=io.BytesIO(archive), mode="r:gz") as tar:
        required = {"scripts/v196_resident_fp16_cases.py",
                    "crates/borsuk/src/bin/v196_resident_fp16_preflight.rs",
                    "docs/research/v196-resident-fp16-rerank-prereg.md"}
        if not required.issubset(tar.getnames()):
            raise ValueError("source archive lacks V196 preflight")
    archive_sha = hashlib.sha256(archive).hexdigest()
    archive_key = f"research/v196-resident-fp16-preflight/{commit}/sources/{archive_sha}.tar.gz"
    prefix = f"research/v196-resident-fp16-preflight/{commit}/runs/{attempt}"
    session = boto3.Session(profile_name="causality", region_name=REGION)
    ec2, s3 = session.client("ec2"), session.client("s3")
    if not missing(s3, prefix + "/reservation.json") or not missing(s3, prefix + "/terminal.json"):
        raise ValueError("attempt already registered")
    active = ec2.describe_instances(Filters=[
        {"Name":"tag:Name", "Values":["borsuk-*"]},
        {"Name":"instance-state-name", "Values":["pending","running","stopping","stopped"]},
    ])
    if any(item.get("Instances") for item in active["Reservations"]):
        raise ValueError("another BORSUK worker is active")
    inputs = list(BASE_INPUTS)
    for name, key, digest in V195_FILES:
        size = s3.head_object(Bucket=BUCKET, Key=key)["ContentLength"]
        inputs.append((name, key, size, digest))
    for _, key, size, _ in inputs:
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
    script = user_data(commit, archive_sha, archive_key, prefix, tuple(inputs))
    receipt = ec2.run_instances(
        ClientToken="v196-" + hashlib.sha256(prefix.encode()).hexdigest()[:48],
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
        UserData=base64.b64encode(script.encode()).decode(),
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
                    raise RuntimeError("V196 failed; inspect closed terminal artifacts")
                if set(ARTIFACTS) != set(terminal.get("artifacts", {})):
                    raise ValueError("complete V196 artifact roster differs")
                for name, identity in terminal["artifacts"].items():
                    body = s3.get_object(Bucket=BUCKET,
                        Key=f"{prefix}/artifacts/{name}")["Body"]
                    digest = hashlib.sha256()
                    size = 0
                    while chunk := body.read(4 * 1024 * 1024):
                        digest.update(chunk)
                        size += len(chunk)
                    if size != identity["bytes"] or digest.hexdigest() != identity["sha256"]:
                        raise ValueError(f"V196 S3 artifact read-back differs: {name}")
                sealed = s3.get_object(Bucket=BUCKET,
                    Key=f"{prefix}/sealed/case-summary.json")["Body"].read()
                if hashlib.sha256(sealed).hexdigest() != terminal["artifacts"]["case-summary.json"]["sha256"]:
                    raise ValueError("pre-benchmark case seal differs")
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
        raise TimeoutError("V196 Spot cell exceeded wall cap")
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
    with open("/tmp/borsuk-v196-resident-fp16-launch.lock","a+") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise RuntimeError("V196 launcher is already active") from error
        launch(args.attempt)


if __name__ == "__main__":
    main()
