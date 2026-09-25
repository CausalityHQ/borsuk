#!/usr/bin/env python3
"""One sealed same-range Deep-Image FP16 diagnostic on Causality Spot."""

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

SCHEMA = "borsuk-v206-deep-image-fp16-spot-v1"
TAG = "borsuk-v206-deep-image-fp16"
IMAGE = "ami-06121aa3085b6f918"
WALL_SECONDS = 7_200
V119 = ("research/v119-deep-image-source/dc242e761ac2d60068c7e8e65812615a6ed6cfd7/"
        "runs/v119-deep-image-source-20260924T002421Z/a0001/artifacts/")
V120 = ("research/v120-deep-image-index/b919685cf1db6c14912c2118d5226c0fb426226b/"
        "runs/v120-20260924T004145Z/a0001/artifacts/")
V121 = ("research/v121-deep-image-paired/79a51449cbeb169851d9c02c173488d0f073d519/"
        "runs/v121-20260924T014535Z/a0003/artifacts/")
INPUTS = (
    ("source.parquet", V119 + "source.parquet", 3_566_768_562,
     "8f88122f412554107d97c07f440352f9043b8cb4b58fe08434ac75f4b90776ee"),
    ("layout.npy", V120 + "built/layout.npy", 79_920_128,
     "419f9280d2e85f6fa275c115dd3428e7d714b7124cfc178ca19a42c249ac31ec6af2d19a42f5b96c38ed247c1"),
    ("queries.jsonl", V121 + "queries.jsonl", 2_013_436,
     "331310ae7abc3f0b73ea20010c7ee5a1de5ff04f5ef1d3e45b90dc513a48962d"),
    ("replay.jsonl", V121 + "rust-replay.jsonl", 14_740_672,
     "ddc9af991bdc6d3ef77d34a156994daa43aeb78f67f18de2cd0dc5ebb93abe91"),
)
TRUTH_INPUT = (("truth.parquet",
                "publication/v3/20260812/datasets/deep-image-96/attempts/0001/"
                "materialized/neighbors.parquet", 4_003_585,
                "d305fcea7387988941defd2942cca1673693271329f977ba073da888cac3de8d"),)
ARTIFACTS = ("raw.jsonl", "pretruth-seal.json", "evidence.jsonl", "summary.json",
             "return-resources.txt", "score-resources.txt", "return.log",
             "score.log", "smoke.log", "install.log", "run-closed.log")


def user_data(commit: str, archive_sha: str, archive_key: str,
              prefix: str) -> str:
    template = r'''#!/bin/bash
set -euo pipefail
systemd-run --unit=v206-hard-stop --on-active=@@WALL@@s /usr/sbin/shutdown -h now
root=/mnt/v206-deep-image-fp16
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
dnf install -y -q python3.12 python3.12-pip tar gzip time gcc >install.log 2>&1
python3.12 -m venv .venv >>install.log 2>&1
.venv/bin/pip install -q numpy==1.26.4 pyarrow==17.0.0 >>install.log 2>&1
export PYTHONPATH="$root/repo" OMP_NUM_THREADS=8 OPENBLAS_NUM_THREADS=8 MKL_NUM_THREADS=8
phase=smoke
.venv/bin/python - <<'PY' >smoke.log
import numpy as np
from scripts.v206_deep_image_fp16 import top100
rng=np.random.default_rng(206)
vectors=rng.normal(size=(120,96)).astype(np.float32)
ids=np.arange(120,dtype=np.int64)
query=rng.normal(size=96).astype(np.float32)
for fp16 in (False,True):
    actual=top100(vectors,ids,query,fp16=fp16)
    payload=(vectors.astype(np.float16).astype(np.float64) if fp16
             else vectors.astype(np.float64))
    q=query.astype(np.float64);q/=np.linalg.norm(q)
    scores=(payload@q)/np.linalg.norm(payload,axis=1)
    expected=ids[np.lexsort((ids,-scores))[:100]].tolist()
    assert actual==expected
print('V206 synthetic top100 parity pass')
PY
phase=inputs
@@DOWNLOAD@@
phase=return
/usr/bin/time -v -o return-resources.txt .venv/bin/python -m scripts.v206_deep_image_fp16 return \
  --source source.parquet --layout layout.npy --queries queries.jsonl \
  --replay replay.jsonl --raw raw.jsonl --seal pretruth-seal.json >return.log 2>&1
aws s3 cp raw.jsonl "s3://@@BUCKET@@/@@PREFIX@@/pretruth/raw.jsonl" --only-show-errors
aws s3 cp pretruth-seal.json "s3://@@BUCKET@@/@@PREFIX@@/pretruth/seal.json" --only-show-errors
phase=truth
@@TRUTH_DOWNLOAD@@
phase=score
/usr/bin/time -v -o score-resources.txt .venv/bin/python -m scripts.v206_deep_image_fp16 score \
  --raw raw.jsonl --seal pretruth-seal.json --truth truth.parquet \
  --evidence evidence.jsonl --summary summary.json >score.log 2>&1
phase=complete
'''
    replacements = {
        "WALL": str(WALL_SECONDS), "ARTIFACTS": " ".join(ARTIFACTS),
        "ARTIFACTS_PY": repr(ARTIFACTS), "BUCKET": BUCKET,
        "PREFIX": prefix, "SCHEMA": SCHEMA, "COMMIT": commit,
        "ARCHIVE_SHA": archive_sha, "ARCHIVE_KEY": archive_key,
        "DOWNLOAD": download_script(INPUTS),
        "TRUTH_DOWNLOAD": download_script(TRUTH_INPUT),
    }
    for name, value in replacements.items():
        template = template.replace("@@" + name + "@@", value)
    if "@@" in template:
        raise ValueError("unresolved V206 worker placeholder")
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
        raise ValueError("V206 source commit is not pushed to origin/main")
    archive = archive_source(commit)
    with tarfile.open(fileobj=io.BytesIO(archive), mode="r:gz") as tar:
        required = {"scripts/v206_deep_image_fp16.py",
                    "docs/research/v206-deep-image-fp16-prereg.md"}
        if not required.issubset(tar.getnames()):
            raise ValueError("V206 source archive lacks frozen gate")
    archive_sha = hashlib.sha256(archive).hexdigest()
    archive_key = f"research/v206-deep-image-fp16/{commit}/sources/{archive_sha}.tar.gz"
    prefix = f"research/v206-deep-image-fp16/{commit}/runs/{attempt}"
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
    for _, key, size, _ in INPUTS + TRUTH_INPUT:
        if s3.head_object(Bucket=BUCKET, Key=key)["ContentLength"] != size:
            raise ValueError("V206 frozen input length differs")
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
        ClientToken="v206-" + hashlib.sha256(prefix.encode()).hexdigest()[:48],
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
                    raise RuntimeError("V206 failed; inspect only closed terminal artifacts")
                if set(ARTIFACTS) != set(terminal.get("artifacts", {})):
                    raise ValueError("complete V206 artifact roster differs")
                for name, identity in terminal["artifacts"].items():
                    body = s3.get_object(Bucket=BUCKET,
                        Key=f"{prefix}/artifacts/{name}")["Body"]
                    digest = hashlib.sha256()
                    size = 0
                    while chunk := body.read(4 * 1024 * 1024):
                        digest.update(chunk)
                        size += len(chunk)
                    if size != identity["bytes"] or digest.hexdigest() != identity["sha256"]:
                        raise ValueError(f"V206 S3 artifact read-back differs: {name}")
                for local, remote in (("raw.jsonl", "raw.jsonl"),
                                      ("pretruth-seal.json", "seal.json")):
                    body = s3.get_object(Bucket=BUCKET,
                        Key=f"{prefix}/pretruth/{remote}")["Body"].read()
                    identity = terminal["artifacts"][local]
                    if {"bytes":len(body),"sha256":hashlib.sha256(body).hexdigest()} != identity:
                        raise ValueError(f"V206 pretruth copy differs: {local}")
                summary = json.loads(s3.get_object(Bucket=BUCKET,
                    Key=f"{prefix}/artifacts/summary.json")["Body"].read())
                if (summary.get("schema") != "borsuk-v206-deep-image-fp16-v1-summary"
                        or summary.get("queries") != 1000
                        or summary.get("hits", {}).get("sq8") != 98034):
                    raise ValueError("V206 summary identity differs")
                print(json.dumps({"instance_id":instance_id,"final_state":"terminated",
                                  "artifact_replay":"pass","summary":summary}),flush=True)
                return
            except ClientError as error:
                if error.response.get("Error",{}).get("Code") not in {"NoSuchKey","404","NotFound"}:
                    raise
            state = ec2.describe_instances(InstanceIds=[instance_id])["Reservations"][0]["Instances"][0]["State"]["Name"]
            if state in {"terminated","shutting-down"}:
                raise RuntimeError("Spot worker stopped before terminal; restart complete cell")
            time.sleep(20)
        raise TimeoutError("V206 Spot cell exceeded wall cap")
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
    with open("/tmp/borsuk-v206-deep-image-fp16-launch.lock","a+") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise RuntimeError("V206 launcher is already active") from error
        launch(args.attempt)


if __name__ == "__main__":
    main()
