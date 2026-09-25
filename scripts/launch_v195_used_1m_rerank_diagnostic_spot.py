#!/usr/bin/env python3
"""Launch one immutable source-only V195 priced interval gate on Spot."""

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

SCHEMA = "borsuk-v195-used-1m-rerank-diagnostic-spot-v1"
TAG = "borsuk-v195-used-1m-rerank-diagnostic"
WALL_SECONDS = 7_200
IMAGE = "ami-06121aa3085b6f918"
V164 = ("research/v164-smooth-layout/"
        "488fc4702f6fd408385e532f67d0a110daf1ba33/runs/a0001/")
V115 = ("research/v115-source-router-parity/"
        "8140fd86defff60ff35ef33be7596f2bda34f879/"
        "runs/v115-router-20260923T235000Z/a0001/artifacts/router/")
V194 = ("research/v194-fresh-1m-optional-source/"
        "72bf198eadfbaa0e438cc0871b9931900319cb2e/runs/a0001/")
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
    ("v192-result.json", "research/v192-optional-hard-plan-fit/"
     "a22a0d7c6f9cfc71f627bdafa85875750c2048f9/runs/a0001/artifacts/out.json",
     174_089, "b79683695350b4bc21eb4cad14f3588ed5ebaef088a1dd9443b3cc429ef62a01"),
    ("v189-features.jsonl", "research/v189-predicted-interval-source/"
     "ae0b160e0b48c419a6bac1b6d9e5964f7fd4b63b/runs/a0002/sealed/features.jsonl",
     15_327_489, "7eb4833c76675534cde41330c5939599ab70d7871ac27cb365d62cdb840c3fbe"),
    ("v189-fit-labels.jsonl", "research/v189-predicted-interval-source/"
     "ae0b160e0b48c419a6bac1b6d9e5964f7fd4b63b/runs/a0002/sealed/fit-labels.jsonl",
     52_888, "4023ade93d32e4aa4377a3f56e7e9b5d469468396e96459715caa5f55394ebf4"),
    ("v194-terminal.json", V194 + "terminal.json", 1_550,
     "4da29740e45057192646a36a045c412e90598e05df9ad95213241ea7350e9767"),
    ("v194-features.jsonl", V194 + "artifacts/out/features.jsonl", 5_771_831,
     "415dbb3a20e3ca3a0c77f327b0b457a049156fe0a9810199ee1b0da0c4de5b64"),
    ("v194-prepare-seal.json", V194 + "artifacts/out/prepare-seal.json", 5_404,
     "d9be646c330ed16aa1dfa78714a7fe4accc59dd966a327deae0ebe19c3ed5494"),
    ("v194-plans.jsonl", V194 + "artifacts/out/plans.jsonl", 754_118,
     "ee5cb9a7eee6a05b753db13f52b9903349ff8d750312702e98b736a39bd5562d"),
    ("v194-plan-seal.json", V194 + "artifacts/out/plan-seal.json", 693,
     "68a5dfc15e320d05ddae95f2fd135dce60f5fed2ae17c649063ea3f06d9dbdcf"),
    ("v194-raw.jsonl", V194 + "artifacts/out/raw.jsonl", 2_441_619,
     "121104dc11819eeaaae810f70d96979dbe74f4bca65e3cd2a29c2557757f144c"),
    ("v194-summary.json", V194 + "artifacts/out/summary.json", 1_736,
     "fc081258d0563deb8f18b926d8948fee73759f45d727543663eed9108434fb01"),
)
ARTIFACTS = (
    "diagnostic-seal.json", "raw.jsonl", "summary.json",
    "run-resources.txt", "run-closed.log",
)


def require_complete_terminal(terminal: dict) -> None:
    """Do not seek success-only artifacts from a failed or partial cell."""
    if terminal.get("status") != "complete":
        raise RuntimeError(
            f"V195 terminal {terminal.get('status')} at "
            f"{terminal.get('phase')} (exit {terminal.get('exit_code')}); "
            "see closed artifacts")
    if not set(ARTIFACTS).issubset(terminal.get("artifacts", {})):
        raise ValueError("complete V195 terminal lacks artifacts")


def download_script() -> str:
    return "\n".join(
        f"aws s3 cp 's3://{BUCKET}/{key}' '{name}' --only-show-errors\n"
        f"[ \"$(stat -c%s '{name}')\" = '{size}' ]\n"
        f"printf '%s  %s\\n' '{digest}' '{name}' | sha256sum -c -"
        for name, key, size, digest in INPUTS
    )


def diagnostic_args() -> str:
    names = ("source", "old_layout", "old_sq8", "router", "order",
             "v164_terminal", "v192_result", "v189_features",
             "v189_fit_labels", "seal", "new_sq8", "raw", "summary")
    paths = {"source": "source.parquet", "old_layout": "old-layout.npy",
             "old_sq8": "old-sq8.bin", "router": "router",
             "order": "order.npy", "v164_terminal": "v164-terminal.json",
             "v192_result": "v192-result.json",
             "v189_features": "v189-features.jsonl",
             "v189_fit_labels": "v189-fit-labels.jsonl",
             "seal": "diagnostic-seal.json", "new_sq8": "new-sq8.bin",
             "raw": "raw.jsonl", "summary": "summary.json"}
    result = [f"--{name.replace('_', '-')} {paths[name]}" for name in names]
    for role in ("terminal", "features", "prepare_seal", "plans",
                 "plan_seal", "raw", "summary"):
        result.append(f"--v194-{role.replace('_', '-')} v194-{role.replace('_', '-')}.jsonl"
                      if role in {"features", "plans", "raw"} else
                      f"--v194-{role.replace('_', '-')} v194-{role.replace('_', '-')}.json")
    return " ".join(result)


def user_data(commit: str, archive_sha: str, archive_key: str, prefix: str) -> str:
    return f'''#!/bin/bash
set -euo pipefail
systemd-run --unit=v195-hard-stop --on-active={WALL_SECONDS}s /usr/sbin/shutdown -h now
root=/mnt/v195-used-1m-rerank-diagnostic
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
phase=seal
.venv/bin/python -m scripts.v195_used_1m_rerank_diagnostic seal {diagnostic_args()}
aws s3api put-object --bucket '{BUCKET}' --key '{prefix}/sealed/diagnostic-seal.json' \
  --body diagnostic-seal.json --if-none-match '*' --no-cli-pager >/dev/null
seal_sha=$(sha256sum diagnostic-seal.json | cut -d ' ' -f1)
remote_sha=$(aws s3 cp 's3://{BUCKET}/{prefix}/sealed/diagnostic-seal.json' - \
  --only-show-errors | sha256sum | cut -d ' ' -f1)
[ "$seal_sha" = "$remote_sha" ]
phase=run
/usr/bin/time -v -o run-resources.txt .venv/bin/python -m \
  scripts.v195_used_1m_rerank_diagnostic run {diagnostic_args()} \
  --seal-sha256 "$seal_sha"
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
        raise ValueError("V195 source commit is not pushed to origin/main")
    archive = archive_source(commit)
    with tarfile.open(fileobj=io.BytesIO(archive), mode="r:gz") as tar:
        required = {"scripts/v166_surrogate_probe.py",
                    "scripts/v166_surrogate_ranking_run.py",
                    "scripts/v168_scored_neighbor_field.py",
                    "scripts/source_rank_utility.py",
                    "scripts/pq_cosine_margin_utility.py",
                    "scripts/predicted_interval_prices.py",
                    "scripts/priced_interval_oracle.py",
                    "scripts/v189_predicted_interval_source.py",
                    "scripts/v195_used_1m_rerank_diagnostic.py",
                    "scripts/check_v194_fresh_1m_optional_source.py",
                    "scripts/hard_priced_interval.py",
                    "scripts/optional_rank_utility.py",
                    "docs/research/v195-used-1m-rerank-diagnostic-prereg.md"}
        if not required.issubset(tar.getnames()):
            raise ValueError("source archive lacks V195 gate")
    archive_sha = hashlib.sha256(archive).hexdigest()
    archive_key = f"research/v195-used-1m-rerank-diagnostic/{commit}/sources/{archive_sha}.tar.gz"
    prefix = f"research/v195-used-1m-rerank-diagnostic/{commit}/runs/{attempt}"
    session = boto3.Session(profile_name="causality", region_name=REGION)
    ec2, s3 = session.client("ec2"), session.client("s3")
    if not missing(s3, prefix + "/reservation.json") or not missing(s3, prefix + "/terminal.json"):
        raise ValueError("attempt already registered")
    active = ec2.describe_instances(Filters=[
        {"Name": "tag:Name", "Values": [TAG]},
        {"Name": "instance-state-name", "Values": ["pending", "running", "stopping", "stopped"]},
    ])
    if any(item.get("Instances") for item in active["Reservations"]):
        raise ValueError("V195 worker already active")
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
        ClientToken="v195-" + hashlib.sha256(prefix.encode()).hexdigest()[:48],
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
    try:
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
                for name, identity in terminal.get("artifacts", {}).items():
                    body = s3.get_object(Bucket=BUCKET,
                        Key=f"{prefix}/artifacts/{name}")["Body"]
                    digest = hashlib.sha256()
                    size = 0
                    while chunk := body.read(1024 * 1024):
                        digest.update(chunk)
                        size += len(chunk)
                    if size != identity["bytes"] or digest.hexdigest() != identity["sha256"]:
                        raise ValueError(f"V195 S3 artifact read-back differs: {name}")
                require_complete_terminal(terminal)
                labels_time = s3.head_object(
                    Bucket=BUCKET, Key=f"{prefix}/artifacts/raw.jsonl"
                )["LastModified"]
                expected = terminal["artifacts"]["diagnostic-seal.json"]
                sealed = s3.get_object(
                    Bucket=BUCKET, Key=f"{prefix}/sealed/diagnostic-seal.json")
                body = sealed["Body"].read()
                if (len(body) != expected["bytes"]
                        or hashlib.sha256(body).hexdigest() != expected["sha256"]
                        or sealed["LastModified"] > labels_time):
                    raise ValueError("V195 pre-truth S3 diagnostic seal differs")
                terminal["terminal_sha256"] = hashlib.sha256(raw).hexdigest()
                print(json.dumps(terminal, sort_keys=True), flush=True)
                return
            state = ec2.describe_instances(InstanceIds=[instance_id])["Reservations"][0]["Instances"][0]["State"]["Name"]
            if state in {"terminated", "shutting-down"}:
                if not missing(s3, prefix + "/terminal.json"):
                    continue
                if time.monotonic() - started >= WALL_SECONDS - 60:
                    raise TimeoutError("V195 worker reached wall cap before terminal")
                raise RuntimeError("Spot worker stopped before terminal; restart full cell")
            time.sleep(20)
        ec2.terminate_instances(InstanceIds=[instance_id])
        raise TimeoutError("V195 cell exceeded wall cap")

    finally:
        try:
            state = ec2.describe_instances(InstanceIds=[instance_id])["Reservations"][0]["Instances"][0]["State"]["Name"]
            if state not in {"terminated", "shutting-down"}:
                ec2.terminate_instances(InstanceIds=[instance_id])
        except Exception:
            pass  # Preserve the original failure; the worker hard stop remains.


def main() -> None:
    # All V195 controllers on this devbox share one process-lifetime lock.
    # The EC2 tag check below also rejects an already running worker.
    with open("/tmp/borsuk-v195-used-1m-rerank-diagnostic-launch.lock", "a+") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RuntimeError("V195 launcher is already running here") from exc
        _launch()


if __name__ == "__main__":
    main()
