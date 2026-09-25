#!/usr/bin/env python3
"""One frozen two-host 1M same-vector mutation HTTP gate on Spot."""

import argparse
import hashlib
import io
import json
import subprocess
import tarfile
import time

import boto3

from scripts.launch_v157_primary_feasibility_spot import (
    BUCKET, REGION, archive_source, missing, put_if_absent,
)
from scripts.launch_v223_authenticated_graph_http_spot import (
    ROOT_SHA, bootstrap, read_marker, spot_request, summaries, terminal,
    terminate_confirmed,
)
from scripts.launch_v219_reachable_graph_1m_spot import describe_state

V223_PREFIX = ("research/v223-authenticated-graph-http-1m/"
               "c38e4a0da557a9e41e5ce86020c5cb83609fe9d0/runs/a0001")
V223_CLOSEOUT_SHA = "8c1ab3fabe43bbae341ebf7623d0675182f6d63f1918c1917fbd30fa9d2e442f"
SCHEMA = "borsuk-v230-mutation-graph-http-spot-v1"
V230_PREFIX = ("research/v230-mutation-graph-http-1m/"
               "014d1fb36f9f69004c2c25b0648765bc369c4cd0/runs/a0001")
V230_CLOSEOUT_SHA = "103a5b05bc7239ae41766a59ee493590301cce61db2fcd44aeb0a7465b2b21db"


def run(attempt: str, decoded: bool = False) -> None:
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
    predecessor = s3.get_object(Bucket=BUCKET,
        Key=V223_PREFIX + "/closeout.json")["Body"].read()
    if (hashlib.sha256(predecessor).hexdigest() != V223_CLOSEOUT_SHA
            or json.loads(predecessor)["gate_pass"] is not True):
        raise ValueError("V223 baseline differs")
    prior_client = s3.get_object(Bucket=BUCKET,
        Key=V223_PREFIX + "/client/terminal.json")["Body"].read()
    if hashlib.sha256(prior_client).hexdigest() != json.loads(predecessor)["client_terminal_sha256"]:
        raise ValueError("V223 terminal differs")
    baseline = summaries(s3, V223_PREFIX, json.loads(prior_client))
    if decoded:
        prior = s3.get_object(Bucket=BUCKET, Key=V230_PREFIX + "/closeout.json")["Body"].read()
        if (hashlib.sha256(prior).hexdigest() != V230_CLOSEOUT_SHA
                or json.loads(prior)["gate_pass"] is not True):
            raise ValueError("V230 linear baseline differs")
        prior_client = s3.get_object(Bucket=BUCKET,
            Key=V230_PREFIX + "/client/terminal.json")["Body"].read()
        if hashlib.sha256(prior_client).hexdigest() != json.loads(prior)["client_terminal_sha256"]:
            raise ValueError("V230 client terminal differs")
        baseline = summaries(s3, V230_PREFIX, json.loads(prior_client))
    schema = "borsuk-v233-decoded-graph-http-spot-v1" if decoded else SCHEMA
    campaign = "v233-decoded-graph-http-1m" if decoded else "v230-mutation-graph-http-1m"
    active = ec2.describe_instances(Filters=[
        {"Name": "tag:Name", "Values": ["borsuk-*"]},
        {"Name": "instance-state-name", "Values": ["pending", "running", "stopping"]},
    ])
    if any(row.get("Instances") for row in active["Reservations"]):
        raise ValueError("another BORSUK worker is active")
    archive = archive_source(commit)
    with tarfile.open(fileobj=io.BytesIO(archive), mode="r:gz") as source:
        required = {"scripts/run_v223_authenticated_graph_http.sh",
                    "scripts/v230_score_mutation_graph_http.py",
                    "docs/research/v230-mutation-graph-http-1m-prereg.md"}
        if decoded:
            required |= {"scripts/v233_score_decoded_graph_http.py",
                         "docs/research/v233-decoded-graph-http-1m-prereg.md"}
        if not required.issubset(source.getnames()):
            raise ValueError("V230 source archive lacks frozen runner or prereg")
        root = source.extractfile("docs/research/v223-relaion-1m-generation.json").read()
        if hashlib.sha256(root).hexdigest() != ROOT_SHA:
            raise ValueError("V230 source graph root differs")
    archive_sha = hashlib.sha256(archive).hexdigest()
    archive_key = f"research/{campaign}/{commit}/sources/{archive_sha}.tar.gz"
    prefix = f"research/{campaign}/{commit}/runs/{attempt}"
    if not missing(s3, prefix + "/reservation.json"):
        raise ValueError("V230 attempt already reserved")
    if missing(s3, archive_key):
        put_if_absent(archive_key, archive)
    elif s3.head_object(Bucket=BUCKET, Key=archive_key)["ContentLength"] != len(archive):
        raise ValueError("V230 source archive length differs")
    spot = ec2.describe_spot_price_history(
        InstanceTypes=["c7i.4xlarge"], ProductDescriptions=["Linux/UNIX"],
        AvailabilityZone="eu-central-1c", MaxResults=1)["SpotPriceHistory"][0]
    quote = float(spot["SpotPrice"])
    put_if_absent(prefix + "/reservation.json", json.dumps({
        "schema": schema, "source_commit": commit, "source_archive_sha256": archive_sha,
        "generation_root_sha256": ROOT_SHA, "v223_closeout_sha256": V223_CLOSEOUT_SHA,
        "v230_closeout_sha256": V230_CLOSEOUT_SHA if decoded else None,
        "dataset": "ReLAION-1M D768", "split": "validation-1000-prior-used",
        "k": 100, "concurrency": 8, "ef": 4096, "shortlist": 4096,
        "mutation": "same-ID same-authenticated-FP16-vector every 100th physical row; 10000 upserts",
        "transport": "VPC peer persistent HTTP/1.1", "cache_state": "fully resident, no response cache",
        "hardware": "two c7i.4xlarge Spot peers in eu-central-1c",
        "spot_quote_usd_per_hour_per_host": quote,
        "gate": ("both passes 1000/1000 V230 exact IDs, split GT100 25509/74159, "
                 "p95<=75% matched V230 pass, QPS>=1.5x matched V230 pass, "
                 "RSS<=3GiB, delta<=64MiB, no vector GET" if decoded else
                 "both passes no split GT100 loss vs V219, p05>=98, p95<=41.640ms, p99<=45.128ms, QPS>=200, RSS<=3GiB, no vector GET"),
        "interruption_policy": "discard both hosts and restart complete cell under new attempt",
        "output_prefix": f"s3://{BUCKET}/{prefix}",
    }, sort_keys=True).encode())
    ids, launch_epochs = {}, {}
    try:
        for role in ("server", "client"):
            script = bootstrap(role, commit, archive_sha, archive_key, prefix,
                               server_ip=server_ip if role == "client" else "",
                               server_id=ids.get("server", ""), mutation_stride=100,
                               delta_encoding="decoded" if decoded else "")
            request = spot_request(role, prefix, script)
            request["TagSpecifications"][0]["Tags"][0]["Value"] = "borsuk-" + campaign
            launched = ec2.run_instances(**request)["Instances"][0]
            instance_id = launched["InstanceId"]
            ids[role] = instance_id
            launch_epochs[role] = launched["LaunchTime"].timestamp()
            put_if_absent(prefix + f"/{role}/launch.json", json.dumps({
                "schema": schema + "-launch", "role": role, "instance_id": instance_id,
                "source_commit": commit, "launch_epoch": launch_epochs[role],
                "spot_quote_usd_per_hour": quote,
            }, sort_keys=True).encode())
            print(json.dumps({"role": role, "instance_id": instance_id,
                              "output_prefix": prefix}), flush=True)
            if role == "server":
                ready_raw = read_marker(s3, ec2, prefix + "/server/ready.json", instance_id, 1800)
                ready = json.loads(ready_raw)
                if (ready.get("schema") != "borsuk-v223-authenticated-graph-http-ready-v1"
                        or ready.get("instance_id") != instance_id
                        or ready.get("source_commit") != commit
                        or ready.get("generation_root_sha256") != ROOT_SHA
                        or ready.get("mutation_stride") != 100
                        or ready.get("delta_encoding", "") != ("decoded" if decoded else "")):
                    raise ValueError("V230 ready identity differs")
                server_ip = ready["private_ip"]
                if server_ip != ec2.describe_instances(InstanceIds=[instance_id])[
                        "Reservations"][0]["Instances"][0]["PrivateIpAddress"]:
                    raise ValueError("V230 server IP differs")
        client_raw = read_marker(s3, ec2, prefix + "/client/terminal.json", ids["client"], 1200)
        client = terminal(s3, "client", prefix, client_raw, ids["client"],
                          commit, archive_sha, mutation_stride=100,
                          delta_encoding="decoded" if decoded else "")
        terminate_confirmed(ec2, ids["client"])
        server_raw = read_marker(s3, ec2, prefix + "/server/terminal.json", ids["server"], 300)
        server = terminal(s3, "server", prefix, server_raw, ids["server"],
                          commit, archive_sha, mutation_stride=100,
                          delta_encoding="decoded" if decoded else "")
        terminate_confirmed(ec2, ids["server"])
        if client["status"] != "complete" or server["status"] != "complete":
            raise RuntimeError("V230 measurement incomplete; preserve negative artifacts")
        qualities = [json.loads(s3.get_object(Bucket=BUCKET,
            Key=f"{prefix}/client/artifacts/{label}.quality.json")["Body"].read())
            for label in ("first", "repeat")]
        resources = json.loads(s3.get_object(Bucket=BUCKET,
            Key=f"{prefix}/server/artifacts/server-resources.json")["Body"].read())
        health = json.loads(s3.get_object(Bucket=BUCKET,
            Key=f"{prefix}/server/artifacts/health.json")["Body"].read())
        gate_pass = (all(row["pass"] for row in qualities)
                     and resources["peak_rss_bytes"] <= 3 * 1024 ** 3
                     and health["delta_rows"] == 10_000
                     and health["overlay_resident_bytes"] <= (64 if decoded else 32) * 1024**2)
        estimate = sum((receipt["worker_finished_epoch"] - launch_epochs[role])
                       * quote / 3600 for role, receipt in
                       (("server", server), ("client", client)))
        closeout = {"schema": schema + "-closeout", "source_commit": commit,
                    "source_archive_sha256": archive_sha,
                    "generation_root_sha256": ROOT_SHA,
                    "v223_closeout_sha256": V223_CLOSEOUT_SHA,
                    "v230_closeout_sha256": V230_CLOSEOUT_SHA if decoded else None,
                    "server_instance_id": ids["server"], "client_instance_id": ids["client"],
                    "server_terminal_sha256": server["terminal_sha256"],
                    "client_terminal_sha256": client["terminal_sha256"],
                    "spot_quote_usd_per_hour_per_host": quote,
                    "estimated_compute_usd_to_terminal": estimate,
                    "quality": qualities, "server_peak_rss_bytes": resources["peak_rss_bytes"],
                    "health": health, "gate_pass": gate_pass,
                    "baseline_summaries": baseline,
                    "candidate_summaries": summaries(s3, prefix, client)}
        put_if_absent(prefix + "/closeout.json", json.dumps(closeout, sort_keys=True).encode())
        print(json.dumps(closeout, sort_keys=True), flush=True)
    except Exception as error:
        states = {}
        for role, instance_id in ids.items():
            try:
                states[role] = {"instance_id": instance_id,
                                "state": describe_state(ec2, instance_id),
                                "spot_notice": not missing(s3, f"{prefix}/{role}/spot-interruption.json")}
            except Exception as snapshot_error:
                states[role] = {"instance_id": instance_id,
                                "snapshot_error": str(snapshot_error)}
        try:
            put_if_absent(prefix + "/failure.json", json.dumps({
                "schema": schema + "-failure", "source_commit": commit,
                "error": str(error), "instances": states,
            }, sort_keys=True).encode())
        finally:
            raise
    finally:
        for instance_id in ids.values():
            terminate_confirmed(ec2, instance_id)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--attempt", default="a0001")
    parser.add_argument("--decoded", action="store_true")
    args = parser.parse_args()
    run(args.attempt, args.decoded)
