#!/usr/bin/env python3
"""Launch and monitor one frozen V142 D768 returned-quality Spot attempt."""

from __future__ import annotations

import argparse
import base64
import hashlib
import io
import json
import shlex
import subprocess
import tarfile
import tempfile
import time
from dataclasses import dataclass

import boto3
from botocore.exceptions import ClientError

BUCKET = "borsuk-bench-453182569524-euc1"
REGION = "eu-central-1"
TAG = "borsuk-v142-relaion-returned"
RUNNER = "scripts/run_v142_relaion_returned_remote.sh"
INPUTS = (
    ("research/v36-prefix-screen/runs/v36-prefix-screen-20260908T174540Z-31445a91/attempt-0000/source.parquet", 1_458_450_077),
    ("research/v63-algorithm-first/layout-oracle-e2f6c2bad99c720b/a0001/artifacts/kmeans_8192-order.npy", 4_000_128),
    ("research/v116-validation-paired/5e9b35ad40ea023eab4407aa611d759e1893bb34/runs/v116-validation-20260923T235426Z/a0001/artifacts/requests.jsonl", 18_726_909),
    ("research/v116-validation-paired/5e9b35ad40ea023eab4407aa611d759e1893bb34/runs/v116-validation-20260923T235426Z/a0001/artifacts/rust-replay.jsonl", 13_455_525),
    ("research/v70-algorithm-first/single-stage-a4a695d66f508edf/index/sq8.bin", 780_000_000),
    ("research/v114-1m-paired/87881c71e048d557c5c1285ffa0ac1d8c381f764/runs/v114-1m-dev-20260923T222600Z/a0001/artifacts/mirror/manifest.json", 29_780),
    ("research/v114-1m-paired/87881c71e048d557c5c1285ffa0ac1d8c381f764/runs/v114-1m-dev-20260923T222600Z/a0001/artifacts/mirror/blocks.sha256", 6_093_760),
    ("research/v36-prefix-screen/runs/v36-prefix-screen-20260908T174540Z-31445a91/attempt-0000/validation-gt100.parquet", 2_045_045),
    ("research/v140-budgeted-page/3213ca34cf6a8c32824dec6967b39eb06dd5fba6/runs/v140-20260924T092255Z/a0001/terminal.json", 1_738),
    ("research/v140-budgeted-page/3213ca34cf6a8c32824dec6967b39eb06dd5fba6/runs/v140-20260924T092255Z/a0001/artifacts/relaion.raw.jsonl", 3_430_072),
)


@dataclass(frozen=True)
class Plan:
    source_commit: str
    archive_uri: str
    archive_sha256: str
    archive_bytes: int
    output_prefix: str
    wall_seconds: int = 5_400
    image_id: str = "ami-06121aa3085b6f918"
    instance_type: str = "c7i.12xlarge"
    subnet_id: str = "subnet-0a12dbed0ca6fac25"
    security_group_id: str = "sg-0b1fd3e4fbde4af0d"
    instance_profile_arn: str = (
        "arn:aws:iam::453182569524:instance-profile/borsuk-bench-profile"
    )


def location(uri: str) -> tuple[str, str]:
    bucket, slash, key = uri.removeprefix("s3://").partition("/")
    if bucket != BUCKET or not slash or not key:
        raise ValueError("S3 location differs")
    return bucket, key.rstrip("/")


def validate(plan: Plan) -> None:
    for value, size in ((plan.source_commit, 40), (plan.archive_sha256, 64)):
        if len(value) != size or any(char not in "0123456789abcdef" for char in value):
            raise ValueError("source identity differs")
    archive_bucket, archive_key = location(plan.archive_uri)
    output_bucket, output_key = location(plan.output_prefix)
    attempt = output_key.rsplit("/", 1)[-1]
    if (
        archive_bucket != output_bucket
        or plan.source_commit not in archive_key
        or plan.archive_sha256 not in archive_key
        or not output_key.startswith(
            f"research/v142-relaion-returned/{plan.source_commit}/runs/"
        )
        or not (len(attempt) == 5 and attempt.startswith("a") and attempt[1:].isdigit())
        or plan.archive_bytes <= 0
        or plan.wall_seconds != 5_400
        or plan.instance_type != "c7i.12xlarge"
    ):
        raise ValueError("immutable V142 plan differs")


def user_data(plan: Plan) -> str:
    validate(plan)
    exports = "\n".join(
        (
            f"export V142_SOURCE_COMMIT={plan.source_commit}",
            f"export V142_ARCHIVE_SHA256={plan.archive_sha256}",
            f"export V142_OUTPUT_PREFIX={shlex.quote(plan.output_prefix)}",
            f"export V142_WALL_SECONDS={plan.wall_seconds}",
        )
    )
    return f"""#!/bin/bash
set -euo pipefail
systemd-run --unit=v142-hard-stop --on-active={plan.wall_seconds}s /usr/sbin/shutdown -h now
root=/mnt/v142-relaion-returned
mkdir -p "$root" && cd "$root"
{exports}
bootstrap_failure() {{
  code=$?
  trap - EXIT
  if [ "$code" -ne 0 ]; then
    token=$(curl -fsS -X PUT -H 'X-aws-ec2-metadata-token-ttl-seconds: 60' \\
      http://169.254.169.254/latest/api/token || true)
    instance_id=$(curl -fsS -H "X-aws-ec2-metadata-token: $token" \\
      http://169.254.169.254/latest/meta-data/instance-id || true)
    INSTANCE_ID="$instance_id" EXIT_CODE="$code" python3 - <<'PY' >terminal.json
import json,os
print(json.dumps({{"schema":"borsuk-v142-relaion-returned-feasibility-spot-v1",
  "source_commit":os.environ["V142_SOURCE_COMMIT"],
  "source_archive_sha256":os.environ["V142_ARCHIVE_SHA256"],
  "instance_id":os.environ["INSTANCE_ID"],"exit_code":int(os.environ["EXIT_CODE"]),
  "phase":"bootstrap","status":"failed","artifacts":{{}}}},
  sort_keys=True,separators=(',',':')))
PY
    aws s3 cp terminal.json "$V142_OUTPUT_PREFIX/terminal.json" --only-show-errors || true
    shutdown -h now || true
  fi
}}
trap bootstrap_failure EXIT
aws s3 cp {shlex.quote(plan.archive_uri)} source.tar.gz --only-show-errors
[ "$(stat -c%s source.tar.gz)" = "{plan.archive_bytes}" ]
printf '%s  source.tar.gz\\n' {plan.archive_sha256} | sha256sum -c -
mkdir repo && tar -xzf source.tar.gz -C repo
exec bash repo/{RUNNER}
"""


def launch_spec(plan: Plan) -> dict:
    token = "v142-" + hashlib.sha256(plan.output_prefix.encode()).hexdigest()[:48]
    return {
        "ClientToken": token,
        "ImageId": plan.image_id,
        "InstanceType": plan.instance_type,
        "MinCount": 1,
        "MaxCount": 1,
        "IamInstanceProfile": {"Arn": plan.instance_profile_arn},
        "NetworkInterfaces": [{
            "AssociatePublicIpAddress": True,
            "DeviceIndex": 0,
            "Groups": [plan.security_group_id],
            "SubnetId": plan.subnet_id,
        }],
        "InstanceMarketOptions": {
            "MarketType": "spot",
            "SpotOptions": {
                "InstanceInterruptionBehavior": "terminate",
                "SpotInstanceType": "one-time",
            },
        },
        "InstanceInitiatedShutdownBehavior": "terminate",
        "BlockDeviceMappings": [{
            "DeviceName": "/dev/xvda",
            "Ebs": {"DeleteOnTermination": True, "Encrypted": True,
                    "VolumeSize": 120, "VolumeType": "gp3"},
        }],
        "TagSpecifications": [{
            "ResourceType": "instance",
            "Tags": [{"Key": "Name", "Value": TAG},
                     {"Key": "BorsukAttempt", "Value": plan.output_prefix.rsplit("/", 1)[-1]}],
        }],
        "UserData": base64.b64encode(user_data(plan).encode()).decode(),
    }


def missing(s3: object, key: str) -> bool:
    try:
        s3.head_object(Bucket=BUCKET, Key=key)
    except ClientError as error:
        code = str(error.response.get("Error", {}).get("Code", ""))
        if code in {"404", "NoSuchKey", "NotFound"}:
            return True
        raise
    return False


def check_source_and_inputs(s3: object, plan: Plan) -> None:
    bucket, key = location(plan.archive_uri)
    response = s3.get_object(Bucket=bucket, Key=key)
    sha = hashlib.sha256()
    count = 0
    archive = io.BytesIO()
    for block in response["Body"].iter_chunks(chunk_size=4 * 1024 * 1024):
        sha.update(block)
        count += len(block)
        archive.write(block)
    if count != plan.archive_bytes or sha.hexdigest() != plan.archive_sha256:
        raise ValueError("source archive differs")
    archive.seek(0)
    with tarfile.open(fileobj=archive, mode="r:gz") as source:
        if (RUNNER not in source.getnames()
                or "scripts/v142_relaion_returned_quality.py" not in source.getnames()
                or "scripts/v114_score_rust/Cargo.toml" not in source.getnames()
                or "scripts/v114_score_rust/src/main.rs" not in source.getnames()
                or "crates/borsuk/src/returned_sq8.rs" not in source.getnames()
                or "scripts/v124_source_tier_precision.py" not in source.getnames()):
            raise ValueError("source archive lacks V142 runner or evaluator")
    for key, size in INPUTS:
        if s3.head_object(Bucket=BUCKET, Key=key)["ContentLength"] != size:
            raise ValueError(f"frozen input length differs: {key}")


def reserve(plan: Plan) -> None:
    receipt = json.dumps({
        "schema": "borsuk-v142-relaion-returned-reservation-v1",
        "source_commit": plan.source_commit,
        "source_archive_sha256": plan.archive_sha256,
        "output_prefix": plan.output_prefix,
    }, sort_keys=True, separators=(",", ":")).encode()
    with tempfile.NamedTemporaryFile() as body:
        body.write(receipt)
        body.flush()
        bucket, key = location(plan.output_prefix)
        subprocess.run([
            "aws", "s3api", "put-object", "--profile", "causality",
            "--region", REGION, "--bucket", bucket,
            "--key", key + "/reservation.json", "--body", body.name,
            "--if-none-match", "*", "--output", "json",
        ], check=True, stdout=subprocess.DEVNULL)


def launch_and_monitor(plan: Plan) -> dict:
    validate(plan)
    session = boto3.Session(profile_name="causality", region_name=REGION)
    ec2, s3 = session.client("ec2"), session.client("s3")
    _, prefix = location(plan.output_prefix)
    if not missing(s3, prefix + "/reservation.json") or not missing(
        s3, prefix + "/terminal.json"
    ):
        raise ValueError("immutable attempt already registered")
    active = ec2.describe_instances(Filters=[
        {"Name": "tag:Name", "Values": [TAG]},
        {"Name": "instance-state-name", "Values": ["pending", "running", "stopping", "stopped"]},
    ])
    if any(item.get("Instances") for item in active["Reservations"]):
        raise ValueError("V142 worker already active")
    check_source_and_inputs(s3, plan)
    reserve(plan)
    instance_id = ec2.run_instances(**launch_spec(plan))["Instances"][0]["InstanceId"]
    print(json.dumps({"instance_id": instance_id, "output_prefix": plan.output_prefix},
                     sort_keys=True), flush=True)
    deadline = time.monotonic() + plan.wall_seconds + 1_800
    try:
        while time.monotonic() < deadline:
            if not missing(s3, prefix + "/terminal.json"):
                raw = s3.get_object(Bucket=BUCKET, Key=prefix + "/terminal.json")["Body"].read()
                terminal = json.loads(raw)
                if (terminal.get("schema") != "borsuk-v142-relaion-returned-feasibility-spot-v1"
                        or terminal.get("source_commit") != plan.source_commit
                        or terminal.get("source_archive_sha256") != plan.archive_sha256
                        or terminal.get("instance_id") != instance_id):
                    raise ValueError("V142 terminal identity differs")
                terminal["terminal_sha256"] = hashlib.sha256(raw).hexdigest()
                for _ in range(24):
                    state = ec2.describe_instances(InstanceIds=[instance_id])["Reservations"][0]["Instances"][0]["State"]["Name"]
                    if state == "terminated":
                        break
                    time.sleep(5)
                else:
                    ec2.terminate_instances(InstanceIds=[instance_id])
                    ec2.get_waiter("instance_terminated").wait(InstanceIds=[instance_id])
                terminal["instance_state"] = "terminated"
                required = {"install.log", "compile.log", "compile-resources.txt",
                            "download.log", "score-resources.txt", "scored.jsonl",
                            "replay-resources.txt", "reduce-resources.txt",
                            "replay.jsonl", "evidence.jsonl",
                            "summary.json", "decision.json", "cgroup-memory.txt",
                            "worker.log"}
                if terminal.get("status") == "complete":
                    decision = json.loads(s3.get_object(
                        Bucket=BUCKET, Key=prefix + "/artifacts/decision.json")["Body"].read())
                    if decision != {"schema": "borsuk-v142-decision-v1",
                                    "relaion_completed": True}:
                        raise ValueError("V142 decision differs")
                    artifacts = terminal.get("artifacts", {})
                    if not required.issubset(artifacts):
                        raise ValueError("V142 complete terminal lacks required artifacts")
                    for name, metadata in artifacts.items():
                        body = s3.get_object(
                            Bucket=BUCKET, Key=prefix + "/artifacts/" + name)["Body"]
                        digest = hashlib.sha256()
                        length = 0
                        for block in body.iter_chunks(chunk_size=4 * 1024 * 1024):
                            digest.update(block)
                            length += len(block)
                        if length != metadata["bytes"] or digest.hexdigest() != metadata["sha256"]:
                            raise ValueError(f"V142 artifact differs: {name}")
                    terminal["authenticated_artifact_count"] = len(artifacts)
                print(json.dumps(terminal, sort_keys=True), flush=True)
                return terminal
            state = ec2.describe_instances(InstanceIds=[instance_id])["Reservations"][0]["Instances"][0]["State"]["Name"]
            if state == "terminated":
                raise RuntimeError("V142 instance terminated without terminal marker")
            time.sleep(15)
        raise TimeoutError(f"V142 terminal missing after worker deadline: {instance_id}")
    except BaseException:
        try:
            state = ec2.describe_instances(InstanceIds=[instance_id])["Reservations"][0]["Instances"][0]["State"]["Name"]
            if state != "terminated":
                ec2.terminate_instances(InstanceIds=[instance_id])
                ec2.get_waiter("instance_terminated").wait(InstanceIds=[instance_id])
        except Exception:
            pass
        raise


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--archive-uri", required=True)
    parser.add_argument("--archive-sha256", required=True)
    parser.add_argument("--archive-bytes", required=True, type=int)
    parser.add_argument("--output-prefix", required=True)
    args = parser.parse_args()
    terminal = launch_and_monitor(Plan(
        source_commit=args.source_commit,
        archive_uri=args.archive_uri,
        archive_sha256=args.archive_sha256,
        archive_bytes=args.archive_bytes,
        output_prefix=args.output_prefix,
    ))
    if terminal.get("status") != "complete" or terminal.get("exit_code") != 0:
        raise RuntimeError("V142 terminal reports a failed attempt")


if __name__ == "__main__":
    main()
