#!/usr/bin/env python3
"""One immutable Spot attempt measuring 1M same-root base sharing."""

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
from scripts.launch_v237_persisted_graph_http_spot import (
    COLLECTION_URI, MUTATION_SHA, ROOT_SHA, V236_CLOSEOUT_SHA,
    V236_PREFIX, V236_TERMINAL_SHA,
)

ARTIFACTS = {"reuse.json", "reuse.time", "build.log", "install.log", "run-closed.log"}


def bootstrap(commit, archive_sha, archive_key, prefix):
    return f"""#!/bin/bash
set -euo pipefail
systemd-run --unit=v238-hard-stop --on-active=7200s /usr/sbin/shutdown -h now
root=/mnt/v238-shared-graph-base
mkdir -p "$root" && cd "$root"
aws s3 cp 's3://{BUCKET}/{archive_key}' source.tar.gz --only-show-errors
printf '%s  source.tar.gz\\n' '{archive_sha}' | sha256sum -c -
mkdir repo && tar -xzf source.tar.gz -C repo
export BORSUK_V238_BUCKET='{BUCKET}'
export BORSUK_V238_PREFIX='{prefix}'
export BORSUK_V238_SOURCE_COMMIT='{commit}'
export BORSUK_V238_ARCHIVE_SHA='{archive_sha}'
export BORSUK_V238_COLLECTION_URI='{COLLECTION_URI}'
exec bash repo/scripts/run_v238_shared_graph_base_1m.sh
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
    v236_closeout = s3.get_object(Bucket=BUCKET,
        Key=V236_PREFIX + "/closeout.json")["Body"].read()
    if (hashlib.sha256(v236_closeout).hexdigest() != V236_CLOSEOUT_SHA
            or json.loads(v236_closeout)["gate_pass"] is not True):
        raise ValueError("V236 collection closeout differs")
    terminal = s3.get_object(Bucket=BUCKET,
        Key=V236_PREFIX + "/terminal.json")["Body"].read()
    if hashlib.sha256(terminal).hexdigest() != V236_TERMINAL_SHA:
        raise ValueError("V236 terminal differs")
    head = json.loads(s3.get_object(Bucket=BUCKET,
        Key=V236_PREFIX + "/published/collection-head.json")["Body"].read())
    if (head["revision"] != 2 or head["base_root_sha256"] != ROOT_SHA
            or head["mutation_sha256"] != MUTATION_SHA):
        raise ValueError("V236 collection head moved")
    archive = archive_source(commit)
    with tarfile.open(fileobj=io.BytesIO(archive), mode="r:gz") as source:
        if not {"scripts/run_v238_shared_graph_base_1m.sh",
                "crates/borsuk/examples/v238_shared_graph_base_1m.rs",
                "docs/research/v238-shared-graph-base-1m-prereg.md"}.issubset(source.getnames()):
            raise ValueError("source archive lacks V238 gate inputs")
    archive_sha = hashlib.sha256(archive).hexdigest()
    archive_key = f"research/v238-shared-graph-base-1m/{commit}/sources/{archive_sha}.tar.gz"
    prefix = f"research/v238-shared-graph-base-1m/{commit}/runs/{attempt}"
    if not missing(s3, prefix + "/reservation.json"):
        raise ValueError("attempt already reserved")
    if missing(s3, archive_key):
        put_if_absent(archive_key, archive)
    quote = ec2.describe_spot_price_history(
        InstanceTypes=["c7i.4xlarge"], ProductDescriptions=["Linux/UNIX"],
        AvailabilityZone="eu-central-1c", MaxResults=1)["SpotPriceHistory"][0]
    spot_price = float(quote["SpotPrice"])
    put_if_absent(prefix + "/reservation.json", json.dumps({
        "schema": "borsuk-v238-shared-graph-base-1m-reservation-v1",
        "source_commit": commit, "source_archive_sha256": archive_sha,
        "v236_closeout_sha256": V236_CLOSEOUT_SHA,
        "collection_uri": COLLECTION_URI,
        "root_sha256": ROOT_SHA, "mutation_sha256": MUTATION_SHA,
        "dataset": "ReLAION-1M D768", "split": "same revision 2 snapshot; deterministic query",
        "hardware": "c7i.4xlarge Spot eu-central-1c",
        "gate": "same base Arc; identical k100 IDs; cold graph GETs 5; steady RSS increment <=256MiB; peak RSS <=2.5GiB",
        "spot_quote_usd_per_hour": spot_price,
        "spot_quote_timestamp": quote["Timestamp"].isoformat(),
        "interruption_policy": "discard attempt and restart under new prefix",
    }, sort_keys=True).encode())
    instance_id = None
    try:
        request = instance_request(prefix, bootstrap(commit, archive_sha, archive_key, prefix))
        request["TagSpecifications"][0]["Tags"][0]["Value"] = "borsuk-v238-shared-graph-base-1m"
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
        if (terminal.get("schema") != "borsuk-v238-shared-graph-base-1m-terminal-v1"
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
            Key=prefix + "/artifacts/reuse.json")["Body"].read())
        gate_pass = (result.get("schema") == "borsuk-v238-shared-graph-base-1m-v1"
                     and result.get("revision") == 2
                     and result.get("root_sha256") == ROOT_SHA
                     and result.get("mutation_sha256") == MUTATION_SHA
                     and result.get("base_shared") is True
                     and result.get("ids_equal") is True
                     and result.get("cold_graph_blob_gets") == 5
                     and result.get("cold_graph_response_bytes") == 1879697462
                     and result.get("old_overlay_bytes") == 31005000
                     and result.get("new_overlay_bytes") == 31005000
                     and result.get("rss_increase_bytes", 2**63) <= 256 * 1024**2
                     and result.get("peak_rss_bytes", 2**63) <= int(2.5 * 1024**3))
        closeout = {"schema": "borsuk-v238-shared-graph-base-1m-closeout-v1",
                    "source_commit": commit, "source_archive_sha256": archive_sha,
                    "instance_id": instance_id, "terminal_sha256": terminal_sha,
                    "reuse": result, "gate_pass": gate_pass,
                    "spot_quote_usd_per_hour": spot_price,
                    "estimated_compute_usd_to_terminal":
                        (terminal["worker_finished_epoch"] - launched) * spot_price / 3600}
        put_if_absent(prefix + "/closeout.json",
                      json.dumps(closeout, sort_keys=True).encode())
        print(json.dumps(closeout, sort_keys=True), flush=True)
        if not gate_pass:
            raise RuntimeError("V238 shared-base gate failed")
    finally:
        if instance_id is not None:
            terminate_confirmed(ec2, instance_id)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--attempt", default="a0001")
    launch(parser.parse_args().attempt)
