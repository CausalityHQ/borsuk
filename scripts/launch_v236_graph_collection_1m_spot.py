#!/usr/bin/env python3
"""One immutable Spot attempt for 1M authenticated collection publication."""

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

ARTIFACTS = {"collection.json", "collection.time", "revision1.raw.jsonl",
             "revision2.raw.jsonl", "build.log", "install.log", "run-closed.log"}
V219_KEY = ("research/v219-reachable-graph-1m/"
            "008ab6fbc50e6293e0599a33993c619702109bd9/runs/a0002/artifacts/raw.jsonl")
V219_SHA = "ccc29dd912248c6bc86c49bdcd86bfc3cd28534d37e05690102425df2c3bcab9"
V230_KEY = ("research/v230-mutation-graph-http-1m/"
            "014d1fb36f9f69004c2c25b0648765bc369c4cd0/runs/a0001/client/sealed/first.raw.jsonl")
V230_SHA = "b4597049dd6959dbf5516342408a31e863fbe13505987f88811d58a4ccdd5d14"
ROOT_SHA = "c59650ec920d031ff236f5ab47331db88b71fdac0462cd07a568d51c549b7caf"


def bootstrap(commit, archive_sha, archive_key, prefix):
    return f"""#!/bin/bash
set -euo pipefail
systemd-run --unit=v236-hard-stop --on-active=7200s /usr/sbin/shutdown -h now
root=/mnt/v236-graph-collection
mkdir -p "$root" && cd "$root"
aws s3 cp 's3://{BUCKET}/{archive_key}' source.tar.gz --only-show-errors
printf '%s  source.tar.gz\\n' '{archive_sha}' | sha256sum -c -
mkdir repo && tar -xzf source.tar.gz -C repo
export BORSUK_V236_BUCKET='{BUCKET}'
export BORSUK_V236_PREFIX='{prefix}'
export BORSUK_V236_SOURCE_COMMIT='{commit}'
export BORSUK_V236_ARCHIVE_SHA='{archive_sha}'
exec bash repo/scripts/run_v236_graph_collection_1m.sh
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
        required = {"scripts/run_v236_graph_collection_1m.sh",
                    "crates/borsuk/examples/v236_graph_collection_1m.rs",
                    "docs/research/v236-graph-collection-1m-prereg.md",
                    "docs/research/v223-relaion-1m-generation.json"}
        if not required.issubset(source.getnames()):
            raise ValueError("source archive lacks V236 gate inputs")
        root = source.extractfile("docs/research/v223-relaion-1m-generation.json").read()
        if hashlib.sha256(root).hexdigest() != ROOT_SHA:
            raise ValueError("trusted graph root differs")
    archive_sha = hashlib.sha256(archive).hexdigest()
    archive_key = f"research/v236-graph-collection-1m/{commit}/sources/{archive_sha}.tar.gz"
    prefix = f"research/v236-graph-collection-1m/{commit}/runs/{attempt}"
    if not missing(s3, prefix + "/reservation.json"):
        raise ValueError("attempt already reserved")
    if missing(s3, archive_key):
        put_if_absent(archive_key, archive)
    quote = ec2.describe_spot_price_history(
        InstanceTypes=["c7i.4xlarge"], ProductDescriptions=["Linux/UNIX"],
        AvailabilityZone="eu-central-1c", MaxResults=1)["SpotPriceHistory"][0]
    spot_price = float(quote["SpotPrice"])
    put_if_absent(prefix + "/reservation.json", json.dumps({
        "schema": "borsuk-v236-graph-collection-1m-reservation-v1",
        "source_commit": commit, "source_archive_sha256": archive_sha,
        "generation_root_sha256": ROOT_SHA,
        "dataset": "ReLAION-1M D768", "split": "validation-1000-prior-used",
        "k": 100, "ef": 4096, "shortlist": 4096,
        "mutation": "same-ID same-authenticated-FP16-vector every 100th physical row; 10000 upserts",
        "v219_raw_sha256": V219_SHA, "v230_raw_sha256": V230_SHA,
        "hardware": "c7i.4xlarge Spot eu-central-1c",
        "gate": "rev1 1000/1000 V219 IDs;rev2 1000/1000 V230 IDs;held rev1 unchanged;graph blob GETs 5/0;peak RSS<=5GiB",
        "spot_quote_usd_per_hour": spot_price,
        "spot_quote_timestamp": quote["Timestamp"].isoformat(),
        "interruption_policy": "discard attempt and restart under new prefix",
    }, sort_keys=True).encode())
    instance_id = None
    try:
        request = instance_request(prefix, bootstrap(commit, archive_sha, archive_key, prefix))
        request["TagSpecifications"][0]["Tags"][0]["Value"] = "borsuk-v236-graph-collection-1m"
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
        if (terminal.get("schema") != "borsuk-v236-graph-collection-1m-terminal-v1"
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
        def raw_ids(body):
            return [json.loads(line) for line in body.splitlines()]
        base = s3.get_object(Bucket=BUCKET, Key=V219_KEY)["Body"].read()
        updated = s3.get_object(Bucket=BUCKET, Key=V230_KEY)["Body"].read()
        if hashlib.sha256(base).hexdigest() != V219_SHA or hashlib.sha256(updated).hexdigest() != V230_SHA:
            raise ValueError("reference raw stream differs")
        old = s3.get_object(Bucket=BUCKET,
            Key=prefix + "/sealed/revision1.raw.jsonl")["Body"].read()
        new = s3.get_object(Bucket=BUCKET,
            Key=prefix + "/sealed/revision2.raw.jsonl")["Body"].read()
        if (hashlib.sha256(old).hexdigest() != terminal["artifacts"]["revision1.raw.jsonl"]["sha256"]
                or hashlib.sha256(new).hexdigest() != terminal["artifacts"]["revision2.raw.jsonl"]["sha256"]):
            raise ValueError("sealed raw stream differs")
        old_rows, new_rows, base_rows, update_rows = map(raw_ids, (old, new, base, updated))
        geometry = (len(old_rows) == len(new_rows) == len(base_rows) == len(update_rows) == 1000
                    and all(old_rows[i]["ordinal"] == new_rows[i]["ordinal"]
                            == base_rows[i]["ordinal"] == update_rows[i]["ordinal"] == i
                            for i in range(1000)))
        old_matches = sum(old_rows[i]["returned_ids"] == base_rows[i]["arms"]["4096-4096"]["returned_ids"]
                          for i in range(1000)) if geometry else 0
        new_matches = sum(new_rows[i]["returned_ids"] == update_rows[i]["returned_ids"]
                          for i in range(1000)) if geometry else 0
        peak_rss = int(next(line.rsplit(":", 1)[1].strip() for line in
            s3.get_object(Bucket=BUCKET,
                Key=prefix + "/artifacts/collection.time")["Body"].read().decode().splitlines()
            if "Maximum resident set size (kbytes)" in line)) * 1024
        gate_pass = (result.get("schema") == "borsuk-v236-graph-collection-1m-v1"
                     and result.get("root_sha256") == ROOT_SHA
                     and result.get("initial_revision") == 1
                     and result.get("mutation_revision") == 2
                     and result.get("mutation_rows") == 10000
                     and result.get("queries") == 1000
                     and result.get("old_reader_unchanged") is True
                     and result.get("cold_graph_blob_gets") == 5
                     and result.get("warm_graph_blob_gets") == 0
                     and old_matches == new_matches == 1000
                     and peak_rss <= 5 * 1024**3)
        closeout = {"schema": "borsuk-v236-graph-collection-1m-closeout-v1",
                    "source_commit": commit, "source_archive_sha256": archive_sha,
                    "instance_id": instance_id, "terminal_sha256": terminal_sha,
                    "collection": result, "old_matches_v219": old_matches,
                    "new_matches_v230": new_matches, "peak_rss_bytes": peak_rss,
                    "gate_pass": gate_pass,
                    "spot_quote_usd_per_hour": spot_price,
                    "estimated_compute_usd_to_terminal":
                        (terminal["worker_finished_epoch"] - launched) * spot_price / 3600}
        put_if_absent(prefix + "/closeout.json",
                      json.dumps(closeout, sort_keys=True).encode())
        print(json.dumps(closeout, sort_keys=True), flush=True)
        if not gate_pass:
            raise RuntimeError("V236 collection gate failed")
    finally:
        if instance_id is not None:
            terminate_confirmed(ec2, instance_id)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--attempt", default="a0001")
    launch(parser.parse_args().attempt)
