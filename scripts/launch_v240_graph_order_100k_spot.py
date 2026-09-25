#!/usr/bin/env python3
"""One query-blind graph-local layout replay on a closed Spot cell."""

import argparse
import hashlib
import io
import json
import subprocess
import tarfile
import tempfile
from pathlib import Path

import boto3

from scripts.launch_v157_primary_feasibility_spot import (
    BUCKET, REGION, archive_source, missing, put_if_absent,
)
from scripts.launch_v224_graph_store_s3_spot import (
    instance_request, terminal_marker, terminate_confirmed,
)
from scripts.launch_v239_graph_containment_100k_spot import download_verified

GRAPH_SHA = "d8b70919243a7cd6ecb9448ce23f776374738476c1882cbc6a651fb34753af2f"
RAW_SHA = "61e6e5b6a42931ce76cc496eee864e847593c295dfd0dba26db01c9021ccf7bf"

V218 = ("research/v218-reachable-graph-100k/"
        "ce317cac8d1eb0a1b8a8610f0a3756b96090514c/runs/a0001/")
V218_TERMINAL_SHA = "cf44ecb95a9d1cc07ac895a222a407c37db090c499f15a887e887d74c8492efd"
V239 = ("research/v239-graph-containment-100k/"
        "dfdacbc8fb9fb4bd968291f4a011af05e0912489/runs/a0001/")
V239_TERMINAL_SHA = "e67d8d90ac450fabcccac3fe9c10d65da4baef5de42f04e463cc78d3a85fda14"
V239_CLOSEOUT_SHA = "d039b06f663a1381c6fe826f5228c2692fc2667031e46118537ddc119b1af460"
ARTIFACTS = {"order.npy", "layout-seal.json", "counts.jsonl", "summary.json",
             "layout.time", "replay.time", "install.log", "run-closed.log"}


def verified_json(s3, key, expected_sha):
    data = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
    if hashlib.sha256(data).hexdigest() != expected_sha:
        raise ValueError(f"authority SHA differs: {key}")
    return json.loads(data)


def bootstrap(commit, archive_sha, archive_key, prefix):
    return f"""#!/bin/bash
set -euo pipefail
root=/mnt/v240-graph-order-100k
mkdir -p "$root" && cd "$root"
aws s3 cp 's3://{BUCKET}/{archive_key}' source.tar.gz --only-show-errors
printf '%s  source.tar.gz\\n' '{archive_sha}' | sha256sum -c -
mkdir repo && tar -xzf source.tar.gz -C repo
export BORSUK_V240_BUCKET='{BUCKET}'
export BORSUK_V240_PREFIX='{prefix}'
export BORSUK_V240_SOURCE_COMMIT='{commit}'
export BORSUK_V240_ARCHIVE_SHA='{archive_sha}'
export BORSUK_V240_GRAPH_KEY='{V218}artifacts/graph.bin'
export BORSUK_V240_GRAPH_SHA='{GRAPH_SHA}'
export BORSUK_V240_RAW_KEY='{V239}sealed/raw.jsonl'
export BORSUK_V240_RAW_SHA='{RAW_SHA}'
exec bash repo/scripts/run_v240_graph_order_100k.sh
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
        raise ValueError("source is not a fast-forward descendant of origin/main")
    session = boto3.Session(profile_name="causality", region_name=REGION)
    s3, ec2 = session.client("s3"), session.client("ec2")
    active = ec2.describe_instances(Filters=[
        {"Name": "tag:Name", "Values": ["borsuk-*"]},
        {"Name": "instance-state-name", "Values": ["pending", "running", "stopping"]},
    ])
    if any(row.get("Instances") for row in active["Reservations"]):
        raise ValueError("another BORSUK worker is active")
    v218 = verified_json(s3, V218 + "terminal.json", V218_TERMINAL_SHA)
    v239 = verified_json(s3, V239 + "terminal.json", V239_TERMINAL_SHA)
    closeout = verified_json(s3, V239 + "closeout.json", V239_CLOSEOUT_SHA)
    if (v218.get("status") != "complete" or v239.get("status") != "complete"
            or v218["artifacts"]["graph.bin"]["sha256"] != GRAPH_SHA
            or v239["artifacts"]["raw.jsonl"]["sha256"] != RAW_SHA
            or closeout.get("terminal_sha256") != V239_TERMINAL_SHA
            or closeout.get("gate_pass") is not False
            or closeout["quality"]["smallest_passing_graph_prefix"] != 1024
            or closeout["quality"]["combined_1000_prior_used"]["graph"]["1024"]["p95_distinct_pages"] != 147):
        raise ValueError("closed parent authority differs")
    archive = archive_source(commit)
    with tarfile.open(fileobj=io.BytesIO(archive), mode="r:gz") as source:
        if not {"scripts/v240_graph_order_100k.py", "scripts/run_v240_graph_order_100k.sh",
                "docs/research/v240-graph-local-order-100k-prereg.md"}.issubset(source.getnames()):
            raise ValueError("source archive lacks V240 gate inputs")
    archive_sha = hashlib.sha256(archive).hexdigest()
    archive_key = f"research/v240-graph-order-100k/{commit}/sources/{archive_sha}.tar.gz"
    prefix = f"research/v240-graph-order-100k/{commit}/runs/{attempt}"
    if not missing(s3, prefix + "/reservation.json"):
        raise ValueError("attempt already reserved")
    if missing(s3, archive_key):
        put_if_absent(archive_key, archive)
    quote = ec2.describe_spot_price_history(
        InstanceTypes=["c7i.4xlarge"], ProductDescriptions=["Linux/UNIX"],
        AvailabilityZone="eu-central-1c", MaxResults=1)["SpotPriceHistory"][0]
    spot_price = float(quote["SpotPrice"])
    put_if_absent(prefix + "/reservation.json", json.dumps({
        "schema": "borsuk-v240-graph-order-100k-reservation-v1",
        "source_commit": commit, "source_archive_sha256": archive_sha,
        "v218_terminal_sha256": V218_TERMINAL_SHA,
        "v239_terminal_sha256": V239_TERMINAL_SHA,
        "v239_closeout_sha256": V239_CLOSEOUT_SHA,
        "method": "SciPy 1.14.1 undirected reverse Cuthill-McKee over V218 base graph",
        "dataset": "ReLAION-100k D768", "candidate_prefix": 1024,
        "gate": "p95 distinct 256-row SQ8 pages <=84 (16MiB)",
        "hardware": "c7i.4xlarge Spot eu-central-1c",
        "spot_quote_usd_per_hour": spot_price,
        "spot_quote_timestamp": quote["Timestamp"].isoformat(),
        "interruption_policy": "discard interrupted cell; restart under new prefix",
    }, sort_keys=True).encode())
    instance_id = None
    terminated = False
    try:
        request = instance_request(prefix, bootstrap(commit, archive_sha, archive_key, prefix))
        request["TagSpecifications"][0]["Tags"][0]["Value"] = "borsuk-v240-graph-order-100k"
        row = ec2.run_instances(**request)["Instances"][0]
        instance_id = row["InstanceId"]
        launched = row["LaunchTime"].timestamp()
        put_if_absent(prefix + "/launch.json", json.dumps({
            "instance_id": instance_id, "launch_epoch": launched,
            "source_commit": commit, "spot_quote_usd_per_hour": spot_price,
        }, sort_keys=True).encode())
        print(json.dumps({"instance_id": instance_id, "prefix": prefix}), flush=True)
        terminal_bytes = terminal_marker(s3, ec2, prefix + "/terminal.json", instance_id)
        terminate_confirmed(ec2, instance_id)
        terminated = True
        terminal = json.loads(terminal_bytes)
        terminal_sha = hashlib.sha256(terminal_bytes).hexdigest()
        if (terminal.get("schema") != "borsuk-v240-graph-order-100k-terminal-v1"
                or terminal.get("instance_id") != instance_id
                or terminal.get("source_commit") != commit
                or terminal.get("source_archive_sha256") != archive_sha
                or not set(terminal.get("artifacts", {})).issubset(ARTIFACTS)):
            raise ValueError("terminal authority differs")
        for name, identity in terminal["artifacts"].items():
            body = s3.get_object(Bucket=BUCKET, Key=f"{prefix}/artifacts/{name}")["Body"]
            sha, length = hashlib.sha256(), 0
            for block in body.iter_chunks(chunk_size=1024 * 1024):
                sha.update(block)
                length += len(block)
            if length != identity["bytes"] or sha.hexdigest() != identity["sha256"]:
                raise ValueError(f"terminal artifact differs: {name}")
        if terminal["status"] != "complete" or set(terminal["artifacts"]) != ARTIFACTS:
            raise RuntimeError("V240 cell failed or Spot interrupted")
        with tempfile.TemporaryDirectory(prefix="borsuk-v240-replay-") as directory:
            root = Path(directory)
            for name in ("order.npy", "layout-seal.json", "counts.jsonl", "summary.json"):
                identity = terminal["artifacts"][name]
                download_verified(s3, prefix + "/artifacts/" + name, root / name,
                                  identity["sha256"], identity["bytes"])
            download_verified(s3, prefix + "/sealed/layout-seal.json",
                              root / "sealed-layout.json",
                              terminal["artifacts"]["layout-seal.json"]["sha256"],
                              terminal["artifacts"]["layout-seal.json"]["bytes"])
            download_verified(s3, V239 + "sealed/raw.jsonl", root / "raw.jsonl",
                              RAW_SHA, v239["artifacts"]["raw.jsonl"]["bytes"])
            subprocess.run(["uv", "run", "--no-project", "--with", "numpy==2.5.0",
                            "python", "-m", "scripts.v240_graph_order_100k", "replay",
                            "--order", str(root / "order.npy"),
                            "--seal", str(root / "sealed-layout.json"),
                            "--raw", str(root / "raw.jsonl"),
                            "--counts", str(root / "replay-counts.jsonl"),
                            "--summary", str(root / "replay-summary.json")], check=True)
            if (hashlib.sha256((root / "replay-counts.jsonl").read_bytes()).hexdigest()
                    != terminal["artifacts"]["counts.jsonl"]["sha256"]
                    or json.loads((root / "replay-summary.json").read_text())
                    != json.loads((root / "summary.json").read_text())):
                raise ValueError("sealed page replay differs")
            result = json.loads((root / "summary.json").read_text())
        close = {"schema": "borsuk-v240-graph-order-100k-closeout-v1",
                 "source_commit": commit, "source_archive_sha256": archive_sha,
                 "instance_id": instance_id, "terminal_sha256": terminal_sha,
                 "result": result, "gate_pass": result.get("gate_pass") is True,
                 "estimated_compute_usd_to_terminal":
                     (terminal["worker_finished_epoch"] - launched) * spot_price / 3600}
        put_if_absent(prefix + "/closeout.json", json.dumps(close, sort_keys=True).encode())
        print(json.dumps(close, sort_keys=True), flush=True)
        if not close["gate_pass"]:
            raise RuntimeError("V240 graph-local page budget gate failed")
    finally:
        if instance_id is not None and not terminated:
            terminate_confirmed(ec2, instance_id)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--attempt", default="a0001")
    launch(parser.parse_args().attempt)
