#!/usr/bin/env python3
"""Launch one immutable GT-blind V168 direct PQ neighbor ranking gate on Causality Spot."""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import io
import json
import subprocess
import tarfile
import time

import boto3

from scripts.launch_v157_primary_feasibility_spot import (
    BUCKET, PROFILE_ARN, REGION, SECURITY_GROUP, SUBNET,
    archive_source, missing, put_if_absent,
)

SCHEMA = "borsuk-v168-pq-neighbor-ranking-spot-v1"
TAG = "borsuk-v168-pq-neighbor-ranking"
WALL_SECONDS = 14_400
IMAGE = "ami-06121aa3085b6f918"
V164 = ("research/v164-smooth-layout/"
        "488fc4702f6fd408385e532f67d0a110daf1ba33/runs/a0001/")
V115 = ("research/v115-source-router-parity/"
        "8140fd86defff60ff35ef33be7596f2bda34f879/"
        "runs/v115-router-20260923T235000Z/a0001/artifacts/router/")
INPUTS = (
    ("source.parquet", "research/v36-prefix-screen/runs/"
     "v36-prefix-screen-20260908T174540Z-31445a91/attempt-0000/source.parquet",
     1_458_450_077, "2796b579f37afe99ca4aff57e282335a6a79ad30596645957d26326a0560cf86"),
    ("old-layout.npy", "research/v63-algorithm-first/"
     "layout-oracle-e2f6c2bad99c720b/a0001/artifacts/kmeans_8192-order.npy",
     4_000_128, "32cba9690cd9d0ed3809763e5a0fa3574b09a207da26e93404651acaa1a66a0b"),
    ("old-sq8.bin", "research/v70-algorithm-first/"
     "single-stage-a4a695d66f508edf/index/sq8.bin",
     780_000_000, "2284f24745f964ff2b125eedb883d5cd8ff6afab0738f49f9e16e593167a318b"),
    ("order.npy", V164 + "artifacts/order.npy", 8_000_128,
     "5b5ef48d86570e5ca68fdaaac9aef231ec7368dd526baef00474cd0a2f59a06f"),
    ("v164-terminal.json", V164 + "terminal.json", 2_038,
     "daa4025093ddef883358a200751972b9d953cd53be80681b9055677c3c7793c7"),
    ("router/manifest.json", V115 + "manifest.json", 932,
     "d558a77443d6a1a50b9b3d01e821f134b1cc0992aa8bcb7ef3dc9ed2941221fe"),
    ("router/summaries.bin", V115 + "summaries.bin", 24_004_608,
     "cf264fa3026e97c6db732e920e607c07a67d5ade9b4d92c0575ab3fb550f2cf7"),
    ("router/books.bin", V115 + "books.bin", 786_432,
     "1ca5aa29c32dd155f0309a4d9f5bd8294ccbe75f50d1a2ff213603fba08800ce"),
    ("router/codes.bin", V115 + "codes.bin", 64_000_000,
     "599e359b02ddb85876234f64bac3fcf7bfcb759e121f6a1fbcb4fbd5dfc95460"),
    ("router/low.bin", V115 + "low.bin", 3_072,
     "ce036f48f918312f694adbd12994646987eb0e144df20f64ffe583bed0d1f891"),
    ("router/step.bin", V115 + "step.bin", 3_072,
     "64d49c7413f163fb3446183629d743f69199156f208fa959523d1bcde27da69c"),
)
ARTIFACTS = (
    "out/features.jsonl", "out/proxy-labels.jsonl",
    "out/prepare-seal.json", "out/plans.jsonl", "out/plan-seal.json",
    "out/summary.json", "check.json", "prepare-resources.txt",
    "plan-resources.txt", "evaluate-resources.txt",
    "check-resources.txt", "run-closed.log",
)


def download_script() -> str:
    return "\n".join(
        f"aws s3 cp 's3://{BUCKET}/{key}' '{name}' --only-show-errors\n"
        f"[ \"$(stat -c%s '{name}')\" = '{size}' ]\n"
        f"printf '%s  %s\\n' '{digest}' '{name}' | sha256sum -c -"
        for name, key, size, digest in INPUTS
    )


def user_data(commit: str, archive_sha: str, archive_key: str, prefix: str) -> str:
    return f'''#!/bin/bash
set -euo pipefail
systemd-run --unit=v168-hard-stop --on-active={WALL_SECONDS}s /usr/sbin/shutdown -h now
root=/mnt/v168-pq-neighbor-ranking
mkdir -p "$root" && cd "$root"
phase=bootstrap
finish() {{
  code=$?
  trap - EXIT TERM
  set +e
  cd "$root"
  cp run.log run-closed.log || code=96
  token=$(curl -fsS -X PUT -H 'X-aws-ec2-metadata-token-ttl-seconds: 60' http://169.254.169.254/latest/api/token)
  instance_id=$(curl -fsS -H "X-aws-ec2-metadata-token: $token" http://169.254.169.254/latest/meta-data/instance-id)
  for name in {' '.join(ARTIFACTS)}; do
    if [ -f "$name" ]; then
      aws s3 cp "$name" "s3://{BUCKET}/{prefix}/artifacts/$name" --only-show-errors || code=96
    fi
  done
  INSTANCE_ID="$instance_id" EXIT_CODE="$code" PHASE="$phase" python3 - <<'PY' >terminal.json
import hashlib,json,os
from pathlib import Path
names={ARTIFACTS!r}
artifacts={{}}
for name in names:
    path=Path(name)
    if path.is_file():
        digest=hashlib.sha256()
        with path.open('rb') as source:
            for chunk in iter(lambda: source.read(1024*1024),b''):
                digest.update(chunk)
        artifacts[name]={{'bytes':path.stat().st_size,'sha256':digest.hexdigest()}}
code=int(os.environ['EXIT_CODE'])
print(json.dumps({{'schema':'{SCHEMA}','source_commit':'{commit}',
    'source_archive_sha256':'{archive_sha}',
    'instance_id':os.environ['INSTANCE_ID'],'exit_code':code,
    'phase':os.environ['PHASE'],
    'status':'complete' if code==0 and os.environ['PHASE']=='complete' else 'failed',
    'artifacts':artifacts}},sort_keys=True,separators=(',',':')))
PY
  aws s3 cp terminal.json "s3://{BUCKET}/{prefix}/terminal.json" --only-show-errors
  shutdown -h now || true
  exit "$code"
}}
trap finish EXIT
trap 'exit 97' TERM
exec >run.log 2>&1
aws s3 cp 's3://{BUCKET}/{archive_key}' source.tar.gz --only-show-errors
printf '%s  source.tar.gz\\n' '{archive_sha}' | sha256sum -c -
mkdir repo && tar -xzf source.tar.gz -C repo
phase=install
dnf install -y -q python3.12 python3.12-pip
python3.12 -m venv .venv
.venv/bin/pip install -q numpy==1.26.4 pyarrow==17.0.0
export PYTHONPATH="$root/repo"
export OPENBLAS_NUM_THREADS=8 OMP_NUM_THREADS=8 MKL_NUM_THREADS=8
mkdir router
phase=inputs
{download_script()}
phase=prepare
/usr/bin/time -v -o prepare-resources.txt .venv/bin/python -m \
  scripts.v168_pq_neighbor_ranking_run prepare --output out \
  --source source.parquet --old-layout old-layout.npy \
  --old-sq8 old-sq8.bin --router router --order order.npy \
  --v164-terminal v164-terminal.json
phase=plan
/usr/bin/time -v -o plan-resources.txt .venv/bin/python -m \
  scripts.v168_pq_neighbor_ranking_run plan --output out
phase=evaluate
/usr/bin/time -v -o evaluate-resources.txt .venv/bin/python -m \
  scripts.v168_pq_neighbor_ranking_run evaluate --output out
phase=check
/usr/bin/time -v -o check-resources.txt .venv/bin/python -m \
  scripts.v168_pq_neighbor_ranking_run check --output out \
  --source source.parquet --old-layout old-layout.npy \
  --old-sq8 old-sq8.bin --router router --order order.npy \
  --v164-terminal v164-terminal.json >check.json
phase=complete
'''


def _launch() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--attempt", default="a0001")
    attempt = parser.parse_args().attempt
    if len(attempt) != 5 or not attempt.startswith("a") or not attempt[1:].isdigit():
        raise ValueError("attempt must be aNNNN")
    if subprocess.check_output(["git", "status", "--porcelain"], text=True).strip():
        raise ValueError("worktree must be clean")
    commit = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    subprocess.run(["git", "fetch", "origin", "main"], check=True)
    if subprocess.run(["git", "merge-base", "--is-ancestor", "HEAD",
                       "origin/main"], check=False).returncode != 0:
        raise ValueError("V168 source commit is not pushed to origin/main")
    archive = archive_source(commit)
    with tarfile.open(fileobj=io.BytesIO(archive), mode="r:gz") as tar:
        required = {"scripts/v166_surrogate_probe.py",
                    "scripts/v166_surrogate_ranking_run.py",
                    "scripts/v168_scored_neighbor_field.py",
                    "scripts/v168_pq_neighbor_ranking_run.py",
                    "docs/research/v168-pq-neighbor-ranking-prereg.md"}
        if not required.issubset(tar.getnames()):
            raise ValueError("source archive lacks V168 gate")
    archive_sha = hashlib.sha256(archive).hexdigest()
    archive_key = f"research/v168-pq-neighbor-ranking/{commit}/sources/{archive_sha}.tar.gz"
    prefix = f"research/v168-pq-neighbor-ranking/{commit}/runs/{attempt}"
    session = boto3.Session(profile_name="causality", region_name=REGION)
    ec2, s3 = session.client("ec2"), session.client("s3")
    if not missing(s3, prefix + "/reservation.json") or not missing(s3, prefix + "/terminal.json"):
        raise ValueError("attempt already registered")
    active = ec2.describe_instances(Filters=[
        {"Name": "tag:Name", "Values": [TAG]},
        {"Name": "instance-state-name", "Values": ["pending", "running", "stopping", "stopped"]},
    ])
    if any(item.get("Instances") for item in active["Reservations"]):
        raise ValueError("V168 worker already active")
    for _, key, size, _ in INPUTS:
        if s3.head_object(Bucket=BUCKET, Key=key)["ContentLength"] != size:
            raise ValueError("frozen input length differs")
    if missing(s3, archive_key):
        put_if_absent(archive_key, archive)
    elif s3.head_object(Bucket=BUCKET, Key=archive_key)["ContentLength"] != len(archive):
        raise ValueError("source archive length differs")
    put_if_absent(prefix + "/reservation.json", json.dumps({
        "schema": SCHEMA, "source_commit": commit,
        "source_archive_sha256": archive_sha,
        "interruption_policy": "discard complete cell and restart a new attempt",
        "output_prefix": f"s3://{BUCKET}/{prefix}",
    }, sort_keys=True).encode())
    receipt = ec2.run_instances(
        ClientToken="v168-" + hashlib.sha256(prefix.encode()).hexdigest()[:48],
        ImageId=IMAGE, InstanceType="c7i.12xlarge", MinCount=1, MaxCount=1,
        IamInstanceProfile={"Arn": PROFILE_ARN},
        NetworkInterfaces=[{"AssociatePublicIpAddress": True, "DeviceIndex": 0,
                            "Groups": [SECURITY_GROUP], "SubnetId": SUBNET}],
        InstanceMarketOptions={"MarketType": "spot", "SpotOptions": {
            "InstanceInterruptionBehavior": "terminate", "SpotInstanceType": "one-time"}},
        InstanceInitiatedShutdownBehavior="terminate",
        BlockDeviceMappings=[{"DeviceName": "/dev/xvda", "Ebs": {
            "DeleteOnTermination": True, "Encrypted": True,
            "VolumeSize": 30, "VolumeType": "gp3"}}],
        TagSpecifications=[{"ResourceType": "instance", "Tags": [
            {"Key": "Name", "Value": TAG}, {"Key": "BorsukAttempt", "Value": attempt}]}],
        UserData=user_data(commit, archive_sha, archive_key, prefix),
    )
    instance_id = receipt["Instances"][0]["InstanceId"]
    print(json.dumps({"instance_id": instance_id, "output_prefix": prefix,
                      "source_commit": commit}), flush=True)
    started = time.monotonic()
    deadline = started + WALL_SECONDS + 600
    while time.monotonic() < deadline:
        if not missing(s3, prefix + "/terminal.json"):
            raw = s3.get_object(Bucket=BUCKET, Key=prefix + "/terminal.json")["Body"].read()
            ec2.terminate_instances(InstanceIds=[instance_id])
            ec2.get_waiter("instance_terminated").wait(InstanceIds=[instance_id])
            terminal = json.loads(raw)
            if (terminal.get("schema") != SCHEMA or terminal.get("instance_id") != instance_id
                    or terminal.get("source_commit") != commit
                    or terminal.get("source_archive_sha256") != archive_sha):
                raise ValueError("terminal identity differs")
            if terminal.get("status") == "complete":
                if not set(ARTIFACTS).issubset(terminal.get("artifacts", {})):
                    raise ValueError("complete terminal lacks artifacts")
            for name, identity in terminal.get("artifacts", {}).items():
                body = s3.get_object(Bucket=BUCKET,
                    Key=f"{prefix}/artifacts/{name}")["Body"]
                digest = hashlib.sha256()
                size = 0
                while chunk := body.read(1024 * 1024):
                    digest.update(chunk)
                    size += len(chunk)
                if size != identity["bytes"] or digest.hexdigest() != identity["sha256"]:
                    raise ValueError(f"V168 S3 artifact read-back differs: {name}")
            terminal["terminal_sha256"] = hashlib.sha256(raw).hexdigest()
            print(json.dumps(terminal, sort_keys=True), flush=True)
            if terminal.get("status") != "complete":
                raise RuntimeError("V168 terminal reports failed; see closed artifacts")
            return
        state = ec2.describe_instances(InstanceIds=[instance_id])["Reservations"][0]["Instances"][0]["State"]["Name"]
        if state in {"terminated", "shutting-down"}:
            if not missing(s3, prefix + "/terminal.json"):
                continue
            if time.monotonic() - started >= WALL_SECONDS - 60:
                raise TimeoutError("V168 worker reached wall cap before terminal")
            raise RuntimeError("Spot worker stopped before terminal; restart full cell")
        time.sleep(20)
    ec2.terminate_instances(InstanceIds=[instance_id])
    raise TimeoutError("V168 cell exceeded wall cap")


def main() -> None:
    # All V168 controllers on this devbox share one process-lifetime lock.
    # The EC2 tag check below also rejects an already running worker.
    with open("/tmp/borsuk-v168-pq-neighbor-ranking-launch.lock", "a+") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RuntimeError("V168 launcher is already running here") from exc
        _launch()


if __name__ == "__main__":
    main()
