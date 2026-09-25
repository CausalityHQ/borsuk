#!/usr/bin/env python3
"""One immutable Spot attempt for an S3 graph collection revision CAS."""

import argparse
import hashlib
import io
import json
import subprocess
import tarfile

import boto3

from scripts.launch_v157_primary_feasibility_spot import (
    BUCKET, REGION, archive_source, missing, put_if_absent,
)
from scripts.launch_v224_graph_store_s3_spot import (
    instance_request, terminal_marker, terminate_confirmed,
)

ARTIFACTS = {"collection.json", "collection.time", "build.log", "install.log", "run-closed.log"}


def bootstrap(commit, archive_sha, archive_key, prefix):
    return f"""#!/bin/bash
set -euo pipefail
systemd-run --unit=v228-hard-stop --on-active=7200s /usr/sbin/shutdown -h now
root=/mnt/v228-graph-collection
mkdir -p "$root" && cd "$root"
aws s3 cp 's3://{BUCKET}/{archive_key}' source.tar.gz --only-show-errors
printf '%s  source.tar.gz\\n' '{archive_sha}' | sha256sum -c -
mkdir repo && tar -xzf source.tar.gz -C repo
export BORSUK_V228_BUCKET='{BUCKET}'
export BORSUK_V228_PREFIX='{prefix}'
export BORSUK_V228_SOURCE_COMMIT='{commit}'
export BORSUK_V228_ARCHIVE_SHA='{archive_sha}'
exec bash repo/scripts/run_v228_graph_collection_s3.sh
"""


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
        if not {"scripts/run_v228_graph_collection_s3.sh",
                "crates/borsuk/examples/v228_graph_collection_s3.rs",
                "docs/research/v228-graph-collection-s3-prereg.md"}.issubset(source.getnames()):
            raise ValueError("source archive lacks V228 gate inputs")
    archive_sha = hashlib.sha256(archive).hexdigest()
    archive_key = f"research/v228-graph-collection-s3/{commit}/sources/{archive_sha}.tar.gz"
    prefix = f"research/v228-graph-collection-s3/{commit}/runs/{attempt}"
    if not missing(s3, prefix + "/reservation.json"):
        raise ValueError("attempt already reserved")
    if missing(s3, archive_key):
        put_if_absent(archive_key, archive)
    quote = ec2.describe_spot_price_history(
        InstanceTypes=["c7i.4xlarge"], ProductDescriptions=["Linux/UNIX"],
        AvailabilityZone="eu-central-1c", MaxResults=1)["SpotPriceHistory"][0]
    spot_price = float(quote["SpotPrice"])
    put_if_absent(prefix + "/reservation.json", json.dumps({
        "schema": "borsuk-v228-graph-collection-s3-reservation-v1",
        "source_commit": commit, "source_archive_sha256": archive_sha,
        "hardware": "c7i.4xlarge Spot eu-central-1c",
        "gate": "rev1 ID42;rev2 delete42+put99;rev3 compacted base preserves IDs;old reader pinned;stale CAS rejects;GETs 5/0/5",
        "spot_quote_usd_per_hour": spot_price,
        "spot_quote_timestamp": quote["Timestamp"].isoformat(),
        "interruption_policy": "discard attempt and restart under new prefix",
    }, sort_keys=True).encode())
    instance_id = None
    try:
        request = instance_request(prefix, bootstrap(commit, archive_sha, archive_key, prefix))
        request["TagSpecifications"][0]["Tags"][0]["Value"] = "borsuk-v228-graph-collection-s3"
        row = ec2.run_instances(**request)["Instances"][0]
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
        if (terminal.get("schema") != "borsuk-v228-graph-collection-s3-terminal-v1"
                or terminal.get("instance_id") != instance_id
                or terminal.get("source_commit") != commit
                or terminal.get("source_archive_sha256") != archive_sha
                or not set(terminal.get("artifacts", {})).issubset(ARTIFACTS)):
            raise ValueError("terminal authority differs")
        for name, identity in terminal["artifacts"].items():
            body = s3.get_object(Bucket=BUCKET,
                Key=f"{prefix}/artifacts/{name}")["Body"].read()
            if len(body) != identity["bytes"] or hashlib.sha256(body).hexdigest() != identity["sha256"]:
                raise ValueError(f"terminal artifact differs: {name}")
        if terminal["status"] != "complete" or set(terminal["artifacts"]) != ARTIFACTS:
            raise RuntimeError("switch failed or Spot interrupted")
        result = json.loads(s3.get_object(Bucket=BUCKET,
            Key=prefix + "/artifacts/collection.json")["Body"].read())
        gate_pass = (result.get("schema") == "borsuk-v228-graph-collection-s3-v1"
                     and result.get("initial_revision") == 1
                     and result.get("mutation_revision") == 2
                     and result.get("switched_revision") == 3
                     and 42 in result.get("old_ids", [])
                     and 99 not in result.get("old_ids", [])
                     and 42 not in result.get("mutation_ids", [])
                     and 99 in result.get("mutation_ids", [])
                     and 42 not in result.get("new_ids", [])
                     and 99 in result.get("new_ids", [])
                     and result.get("compacted_ids_match") is True
                     and result.get("first_blob_gets") == 5
                     and result.get("mutation_blob_gets") == 0
                     and result.get("second_blob_gets") == 5
                     and result.get("stale_cas_rejected") is True)
        closeout = {"schema": "borsuk-v228-graph-collection-s3-closeout-v1",
                    "source_commit": commit, "source_archive_sha256": archive_sha,
                    "instance_id": instance_id, "terminal_sha256": terminal_sha,
                    "collection": result, "gate_pass": gate_pass,
                    "spot_quote_usd_per_hour": spot_price,
                    "estimated_compute_usd_to_terminal":
                        (terminal["worker_finished_epoch"] - launched) * spot_price / 3600}
        put_if_absent(prefix + "/closeout.json",
                      json.dumps(closeout, sort_keys=True).encode())
        print(json.dumps(closeout, sort_keys=True), flush=True)
        if not gate_pass:
            raise RuntimeError("V228 collection gate failed")
    finally:
        if instance_id is not None:
            terminate_confirmed(ec2, instance_id)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--attempt", default="a0001")
    launch(parser.parse_args().attempt)
