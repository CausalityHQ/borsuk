#!/usr/bin/env python3
"""One Spot attempt for a real S3 graph publication and hydration round trip."""

import argparse
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
from scripts.launch_v219_reachable_graph_1m_spot import describe_state

ROOT_SHA = "c59650ec920d031ff236f5ab47331db88b71fdac0462cd07a568d51c549b7caf"
ARTIFACTS = {"roundtrip.json", "roundtrip.time", "build.log", "install.log", "run-closed.log"}


def bootstrap(commit, archive_sha, archive_key, prefix):
    return f"""#!/bin/bash
set -euo pipefail
systemd-run --unit=v224-hard-stop --on-active=7200s /usr/sbin/shutdown -h now
root=/mnt/v224-graph-store
mkdir -p "$root" && cd "$root"
aws s3 cp 's3://{BUCKET}/{archive_key}' source.tar.gz --only-show-errors
printf '%s  source.tar.gz\\n' '{archive_sha}' | sha256sum -c -
mkdir repo && tar -xzf source.tar.gz -C repo
export BORSUK_V224_BUCKET='{BUCKET}'
export BORSUK_V224_PREFIX='{prefix}'
export BORSUK_V224_SOURCE_COMMIT='{commit}'
export BORSUK_V224_ARCHIVE_SHA='{archive_sha}'
export BORSUK_V224_ROOT_SHA='{ROOT_SHA}'
exec bash repo/scripts/run_v224_graph_store_s3.sh
"""


def instance_request(prefix, user_data):
    return {
        "ClientToken": "v224-" + hashlib.sha256(prefix.encode()).hexdigest()[:48],
        "ImageId": "ami-06121aa3085b6f918", "InstanceType": "c7i.4xlarge",
        "MinCount": 1, "MaxCount": 1,
        "IamInstanceProfile": {"Arn": PROFILE_ARN},
        "NetworkInterfaces": [{"AssociatePublicIpAddress": True, "DeviceIndex": 0,
                               "Groups": [SECURITY_GROUP], "SubnetId": SUBNET}],
        "InstanceMarketOptions": {"MarketType": "spot", "SpotOptions": {
            "InstanceInterruptionBehavior": "terminate", "SpotInstanceType": "one-time"}},
        "InstanceInitiatedShutdownBehavior": "terminate",
        "BlockDeviceMappings": [{"DeviceName": "/dev/xvda", "Ebs": {
            "DeleteOnTermination": True, "Encrypted": True, "VolumeSize": 40,
            "VolumeType": "gp3"}}],
        "TagSpecifications": [{"ResourceType": "instance", "Tags": [
            {"Key": "Name", "Value": "borsuk-v224-graph-store-s3"},
            {"Key": "BorsukAttempt", "Value": prefix.rsplit("/", 1)[-1]}]}],
        "UserData": user_data,
    }


def terminal_marker(s3, ec2, key, instance_id):
    deadline = time.monotonic() + 7200
    while time.monotonic() < deadline:
        try:
            return s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
        except ClientError as error:
            if error.response.get("Error", {}).get("Code") not in {"NoSuchKey", "404", "NotFound"}:
                raise
        if describe_state(ec2, instance_id) in {"terminated", "shutting-down"}:
            raise RuntimeError("Spot worker stopped before terminal marker")
        time.sleep(10)
    raise TimeoutError("Spot worker exceeded terminal deadline")


def terminate_confirmed(ec2, instance_id):
    if describe_state(ec2, instance_id) != "terminated":
        ec2.terminate_instances(InstanceIds=[instance_id])
        ec2.get_waiter("instance_terminated").wait(
            InstanceIds=[instance_id], WaiterConfig={"Delay": 5, "MaxAttempts": 24})


def launch(attempt):
    if len(attempt) != 5 or not attempt.startswith("a") or not attempt[1:].isdigit():
        raise ValueError("attempt must be aNNNN")
    if subprocess.check_output(["git", "status", "--porcelain"], text=True).strip():
        raise ValueError("worktree must be clean")
    commit = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    subprocess.run(["git", "fetch", "origin", "main"], check=True)
    if subprocess.run(["git", "merge-base", "--is-ancestor", "origin/main", "HEAD"],
                      check=False).returncode:
        raise ValueError("source is not a descendant of origin/main")
    session = boto3.Session(profile_name="causality", region_name=REGION)
    s3, ec2 = session.client("s3"), session.client("ec2")
    active = ec2.describe_instances(Filters=[
        {"Name": "tag:Name", "Values": ["borsuk-*"]},
        {"Name": "instance-state-name", "Values": ["pending", "running", "stopping"]},
    ])
    if any(row.get("Instances") for row in active["Reservations"]):
        raise ValueError("another BORSUK worker is active")
    archive = archive_source(commit)
    with tarfile.open(fileobj=io.BytesIO(archive), mode="r:gz") as source:
        required = {"scripts/run_v224_graph_store_s3.sh",
                    "crates/borsuk/examples/v224_graph_store_roundtrip.rs",
                    "docs/research/v224-graph-store-s3-roundtrip-prereg.md",
                    "docs/research/v223-relaion-1m-generation.json"}
        if not required.issubset(source.getnames()):
            raise ValueError("source archive lacks gate inputs")
        root = source.extractfile("docs/research/v223-relaion-1m-generation.json").read()
        if hashlib.sha256(root).hexdigest() != ROOT_SHA:
            raise ValueError("source archive root differs")
    archive_sha = hashlib.sha256(archive).hexdigest()
    archive_key = f"research/v224-graph-store-s3/{commit}/sources/{archive_sha}.tar.gz"
    prefix = f"research/v224-graph-store-s3/{commit}/runs/{attempt}"
    if not missing(s3, prefix + "/reservation.json"):
        raise ValueError("attempt already reserved")
    if missing(s3, archive_key):
        put_if_absent(archive_key, archive)
    quote = ec2.describe_spot_price_history(
        InstanceTypes=["c7i.4xlarge"], ProductDescriptions=["Linux/UNIX"],
        AvailabilityZone="eu-central-1c", MaxResults=1)["SpotPriceHistory"][0]
    spot_price = float(quote["SpotPrice"])
    put_if_absent(prefix + "/reservation.json", json.dumps({
        "schema": "borsuk-v224-graph-store-s3-reservation-v1",
        "source_commit": commit, "source_archive_sha256": archive_sha,
        "root_sha256": ROOT_SHA, "dataset": "ReLAION-1M", "split": "index build",
        "rows": 1_000_000, "dimensions": 768,
        "hardware": "c7i.4xlarge Spot eu-central-1c",
        "spot_quote_usd_per_hour": spot_price,
        "spot_quote_timestamp": quote["Timestamp"].isoformat(),
        "gate": "authenticated root; generation=196; cold 5 GET/1879697462 bytes; warm 0 GET",
        "interruption_policy": "discard attempt and restart under new prefix",
    }, sort_keys=True).encode())
    instance_id = None
    try:
        row = ec2.run_instances(**instance_request(prefix, bootstrap(
            commit, archive_sha, archive_key, prefix)))["Instances"][0]
        instance_id = row["InstanceId"]
        launched = row["LaunchTime"].timestamp()
        put_if_absent(prefix + "/launch.json", json.dumps({
            "instance_id": instance_id, "launch_epoch": launched,
            "source_commit": commit, "spot_quote_usd_per_hour": spot_price,
        }, sort_keys=True).encode())
        print(json.dumps({"instance_id": instance_id, "prefix": prefix}), flush=True)
        raw = terminal_marker(s3, ec2, prefix + "/terminal.json", instance_id)
        terminal = json.loads(raw)
        terminal_sha = hashlib.sha256(raw).hexdigest()
        if (terminal.get("schema") != "borsuk-v224-graph-store-s3-terminal-v1"
                or terminal.get("instance_id") != instance_id
                or terminal.get("source_commit") != commit
                or terminal.get("source_archive_sha256") != archive_sha
                or terminal.get("generation_root_sha256") != ROOT_SHA
                or not set(terminal.get("artifacts", {})).issubset(ARTIFACTS)):
            raise ValueError("terminal authority differs")
        for name, identity in terminal["artifacts"].items():
            body = s3.get_object(Bucket=BUCKET,
                Key=f"{prefix}/artifacts/{name}")["Body"].read()
            if len(body) != identity["bytes"] or hashlib.sha256(body).hexdigest() != identity["sha256"]:
                raise ValueError(f"terminal artifact differs: {name}")
        if terminal["status"] != "complete" or set(terminal["artifacts"]) != ARTIFACTS:
            raise RuntimeError("round trip failed or Spot interrupted")
        result = json.loads(s3.get_object(Bucket=BUCKET,
            Key=prefix + "/artifacts/roundtrip.json")["Body"].read())
        gate_pass = (result.get("schema") == "borsuk-v224-graph-store-roundtrip-v1"
                     and result.get("root_sha256") == ROOT_SHA
                     and result.get("generation") == 196
                     and result.get("warm_generation") == 196
                     and result.get("rows") == 1_000_000
                     and result.get("dimensions") == 768
                     and result.get("cold_gets") == 5
                     and result.get("cold_bytes") == 1_879_697_462
                     and result.get("warm_gets") == 0
                     and result.get("warm_bytes") == 0)
        closeout = {"schema": "borsuk-v224-graph-store-s3-closeout-v1",
                    "source_commit": commit, "source_archive_sha256": archive_sha,
                    "instance_id": instance_id, "terminal_sha256": terminal_sha,
                    "root_sha256": ROOT_SHA, "roundtrip": result,
                    "spot_quote_usd_per_hour": spot_price,
                    "estimated_compute_usd_to_terminal":
                        (terminal["worker_finished_epoch"] - launched) * spot_price / 3600,
                    "gate_pass": gate_pass}
        put_if_absent(prefix + "/closeout.json",
                      json.dumps(closeout, sort_keys=True).encode())
        print(json.dumps(closeout, sort_keys=True), flush=True)
        if not gate_pass:
            raise RuntimeError("V224 round trip gate failed")
    finally:
        if instance_id is not None:
            terminate_confirmed(ec2, instance_id)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--attempt", default="a0001")
    launch(parser.parse_args().attempt)
