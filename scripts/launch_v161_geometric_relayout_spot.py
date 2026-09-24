#!/usr/bin/env python3
"""Launch one frozen V161 1M transfer attempt on Causality Spot."""

from __future__ import annotations

import argparse
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

SCHEMA = "borsuk-v161-geometric-relayout-spot-v1"
TAG = "borsuk-v161-geometric-relayout"
WALL_SECONDS = 7_200
IMAGE = "ami-06121aa3085b6f918"
V155 = ("research/v155-relaion-returned-quality/"
        "10850f917c4583f05d8c33afec459f4f76b194bd/"
        "runs/v155-20260924T150712Z/a0001/")
V116 = ("research/v116-validation-paired/"
        "5e9b35ad40ea023eab4407aa611d759e1893bb34/"
        "runs/v116-validation-20260923T235426Z/a0001/artifacts/")
EARLY_INPUTS = (
    ("source.parquet", "research/v36-prefix-screen/runs/"
     "v36-prefix-screen-20260908T174540Z-31445a91/attempt-0000/source.parquet",
     1_458_450_077, "2796b579f37afe99ca4aff57e282335a6a79ad30596645957d26326a0560cf86"),
    ("old-layout.npy", "research/v63-algorithm-first/"
     "layout-oracle-e2f6c2bad99c720b/a0001/artifacts/kmeans_8192-order.npy",
     4_000_128, "32cba9690cd9d0ed3809763e5a0fa3574b09a207da26e93404651acaa1a66a0b"),
    ("old-sq8.bin", "research/v70-algorithm-first/"
     "single-stage-a4a695d66f508edf/index/sq8.bin",
     780_000_000, "2284f24745f964ff2b125eedb883d5cd8ff6afab0738f49f9e16e593167a318b"),
    ("manifest.json", "research/v114-1m-paired/"
     "87881c71e048d557c5c1285ffa0ac1d8c381f764/"
     "runs/v114-1m-dev-20260923T222600Z/a0001/artifacts/mirror/manifest.json",
     29_780, "48e01d4488d0c84b991baef521a02bca75ce096c0f9a1154f779c046732707f8"),
    ("requests.jsonl", V116 + "requests.jsonl", 18_726_909,
     "c250ef3c871af55ee1ea91e61214a93903b3d575ef458926b1f8c5ad0547b2c9"),
    ("sealed.jsonl", V116 + "rust-replay.jsonl", 13_455_525,
     "3bfd155ac5f9e1b7aacbc263e1732e2314c9722f235d3454e0f17d0c6bc3c960"),
)
LATE_INPUTS = (
    ("truth.parquet", "research/v36-prefix-screen/runs/"
     "v36-prefix-screen-20260908T174540Z-31445a91/attempt-0000/validation-gt100.parquet",
     2_045_045, "bf0fb0c934c986d05282e3d1c63dc351c553976ea05bfcab0cd3f06d2979e871"),
    ("v155-terminal.json", V155 + "terminal.json", 3_311,
     "784097f577f11bd49468473e43b1ba06642bf107ecde06a8b0b0ce09b0cd9cdb"),
    ("v155-replay.jsonl", V155 + "artifacts/replay.jsonl", 15_275_378,
     "a4f9e39e674ce22fc65ee82836731a22e2c094f7267439888b45f9320c3cb78f"),
    ("v155-evidence.jsonl", V155 + "artifacts/evidence.jsonl", 223_985,
     "dd4d4a7c9448ce6833357e70bff5f72bd9a782afd8185114cb501b2a63a49a1a"),
)
ARTIFACTS = (
    "membership.parquet", "new-sq8.bin", "layout-seal.json", "plans.jsonl",
    "plan-seal.json", "scored.jsonl", "score-seal.json", "raw.jsonl",
    "summary.json", "check.json", "construct-resources.txt",
    "plan-resources.txt", "score-resources.txt", "reduce-resources.txt",
    "check-resources.txt", "run.log",
)


def download_script(inputs: tuple) -> str:
    return "\n".join(
        f"aws s3 cp 's3://{BUCKET}/{key}' '{name}' --only-show-errors\n"
        f"[ \"$(stat -c%s '{name}')\" = '{size}' ]\n"
        f"printf '%s  %s\\n' '{digest}' '{name}' | sha256sum -c -"
        for name, key, size, digest in inputs
    )


def user_data(commit: str, archive_sha: str, archive_key: str, prefix: str) -> str:
    return f'''#!/bin/bash
set -euo pipefail
systemd-run --unit=v161-hard-stop --on-active={WALL_SECONDS}s /usr/sbin/shutdown -h now
root=/mnt/v161-geometric-relayout
mkdir -p "$root" && cd "$root"
phase=bootstrap
finish() {{
  code=$?
  trap - EXIT TERM
  set +e
  cd "$root"
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
export OPENBLAS_NUM_THREADS=2 OMP_NUM_THREADS=2 MKL_NUM_THREADS=2
phase=early-inputs
{download_script(EARLY_INPUTS)}
phase=construct
/usr/bin/time -v -o construct-resources.txt .venv/bin/python \\
  repo/scripts/v161_geometric_relayout_1m.py construct \\
  --source source.parquet --old-layout old-layout.npy --old-sq8 old-sq8.bin \\
  --membership membership.parquet --new-sq8 new-sq8.bin \\
  --layout-seal layout-seal.json
phase=plan
/usr/bin/time -v -o plan-resources.txt .venv/bin/python \\
  repo/scripts/v161_geometric_relayout_1m.py plan \\
  --requests requests.jsonl --sealed sealed.jsonl \\
  --old-layout old-layout.npy --old-sq8 old-sq8.bin \\
  --membership membership.parquet --new-sq8 new-sq8.bin \\
  --layout-seal layout-seal.json --plans plans.jsonl --plan-seal plan-seal.json
phase=score
/usr/bin/time -v -o score-resources.txt .venv/bin/python \\
  repo/scripts/v161_geometric_relayout_1m.py score \\
  --source source.parquet --old-layout old-layout.npy --old-sq8 old-sq8.bin \\
  --new-sq8 new-sq8.bin --manifest manifest.json \\
  --requests requests.jsonl --plans plans.jsonl --plan-seal plan-seal.json \\
  --scored scored.jsonl --score-seal score-seal.json
phase=late-inputs
{download_script(LATE_INPUTS)}
phase=reduce
/usr/bin/time -v -o reduce-resources.txt .venv/bin/python \\
  repo/scripts/v161_geometric_relayout_1m.py reduce \\
  --source source.parquet --new-sq8 new-sq8.bin --plans plans.jsonl \\
  --score-seal score-seal.json --scored scored.jsonl \\
  --truth truth.parquet --v155-terminal v155-terminal.json \\
  --v155-replay v155-replay.jsonl --v155-evidence v155-evidence.jsonl \\
  --raw raw.jsonl --summary summary.json
phase=check
/usr/bin/time -v -o check-resources.txt .venv/bin/python \\
  repo/scripts/v161_check_geometric_relayout.py \\
  --source source.parquet --old-layout old-layout.npy --old-sq8 old-sq8.bin \\
  --manifest manifest.json --membership membership.parquet \\
  --new-sq8 new-sq8.bin --layout-seal layout-seal.json \\
  --requests requests.jsonl --sealed sealed.jsonl --plans plans.jsonl \\
  --plan-seal plan-seal.json --scored scored.jsonl --score-seal score-seal.json \\
  --truth truth.parquet --v155-terminal v155-terminal.json \\
  --v155-replay v155-replay.jsonl --v155-evidence v155-evidence.jsonl \\
  --raw raw.jsonl --summary summary.json >check.json
phase=complete
'''


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--attempt", default="a0001")
    attempt = parser.parse_args().attempt
    if len(attempt) != 5 or not attempt.startswith("a") or not attempt[1:].isdigit():
        raise ValueError("attempt must be aNNNN")
    if subprocess.check_output(["git", "status", "--porcelain"], text=True).strip():
        raise ValueError("worktree must be clean")
    commit = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    archive = archive_source(commit)
    with tarfile.open(fileobj=io.BytesIO(archive), mode="r:gz") as tar:
        required = {"scripts/v161_geometric_relayout_1m.py",
                    "scripts/v161_check_geometric_relayout.py",
                    "docs/research/v161-geometric-relayout-1m-transfer-prereg.md"}
        if not required.issubset(tar.getnames()):
            raise ValueError("source archive lacks V161 gate")
    archive_sha = hashlib.sha256(archive).hexdigest()
    archive_key = f"research/v161-geometric-relayout/{commit}/sources/{archive_sha}.tar.gz"
    prefix = f"research/v161-geometric-relayout/{commit}/runs/{attempt}"
    session = boto3.Session(profile_name="causality", region_name=REGION)
    ec2, s3 = session.client("ec2"), session.client("s3")
    if not missing(s3, prefix + "/reservation.json") or not missing(s3, prefix + "/terminal.json"):
        raise ValueError("attempt already registered")
    active = ec2.describe_instances(Filters=[
        {"Name": "tag:Name", "Values": [TAG]},
        {"Name": "instance-state-name", "Values": ["pending", "running", "stopping", "stopped"]},
    ])
    if any(item.get("Instances") for item in active["Reservations"]):
        raise ValueError("V161 worker already active")
    for _, key, size, _ in EARLY_INPUTS + LATE_INPUTS:
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
        ClientToken="v161-" + hashlib.sha256(prefix.encode()).hexdigest()[:48],
        ImageId=IMAGE, InstanceType="c7i.12xlarge", MinCount=1, MaxCount=1,
        IamInstanceProfile={"Arn": PROFILE_ARN},
        NetworkInterfaces=[{"AssociatePublicIpAddress": True, "DeviceIndex": 0,
                            "Groups": [SECURITY_GROUP], "SubnetId": SUBNET}],
        InstanceMarketOptions={"MarketType": "spot", "SpotOptions": {
            "InstanceInterruptionBehavior": "terminate", "SpotInstanceType": "one-time"}},
        InstanceInitiatedShutdownBehavior="terminate",
        BlockDeviceMappings=[{"DeviceName": "/dev/xvda", "Ebs": {
            "DeleteOnTermination": True, "Encrypted": True,
            "VolumeSize": 120, "VolumeType": "gp3"}}],
        TagSpecifications=[{"ResourceType": "instance", "Tags": [
            {"Key": "Name", "Value": TAG}, {"Key": "BorsukAttempt", "Value": attempt}]}],
        UserData=user_data(commit, archive_sha, archive_key, prefix),
    )
    instance_id = receipt["Instances"][0]["InstanceId"]
    print(json.dumps({"instance_id": instance_id, "output_prefix": prefix,
                      "source_commit": commit}), flush=True)
    deadline = time.monotonic() + WALL_SECONDS + 600
    while time.monotonic() < deadline:
        if not missing(s3, prefix + "/terminal.json"):
            raw = s3.get_object(Bucket=BUCKET, Key=prefix + "/terminal.json")["Body"].read()
            terminal = json.loads(raw)
            if (terminal.get("schema") != SCHEMA or terminal.get("instance_id") != instance_id
                    or terminal.get("source_commit") != commit
                    or terminal.get("source_archive_sha256") != archive_sha):
                raise ValueError("terminal identity differs")
            if terminal.get("status") == "complete":
                if not set(ARTIFACTS).issubset(terminal.get("artifacts", {})):
                    raise ValueError("complete terminal lacks artifacts")
            ec2.terminate_instances(InstanceIds=[instance_id])
            ec2.get_waiter("instance_terminated").wait(InstanceIds=[instance_id])
            terminal["terminal_sha256"] = hashlib.sha256(raw).hexdigest()
            print(json.dumps(terminal, sort_keys=True), flush=True)
            if terminal.get("status") != "complete":
                raise RuntimeError("V161 terminal reports failed; see closed artifacts")
            return
        state = ec2.describe_instances(InstanceIds=[instance_id])["Reservations"][0]["Instances"][0]["State"]["Name"]
        if state in {"terminated", "shutting-down"}:
            raise RuntimeError("Spot worker stopped before terminal; restart full cell")
        time.sleep(20)
    ec2.terminate_instances(InstanceIds=[instance_id])
    raise TimeoutError("V161 cell exceeded wall cap")


if __name__ == "__main__":
    main()
