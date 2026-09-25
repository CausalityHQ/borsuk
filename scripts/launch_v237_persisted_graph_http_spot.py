#!/usr/bin/env python3
"""One frozen two-host HTTP gate from V236's persisted S3 collection."""

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
from scripts.launch_v223_authenticated_graph_http_spot import (
    ROOT_SHA, bootstrap, read_marker, spot_request, summaries, terminal,
    terminate_confirmed,
)

V236_PREFIX = ("research/v236-graph-collection-1m/"
               "4fde270ac6a27833626701aa777c646fb2cff95a/runs/a0001")
V236_TERMINAL_SHA = "5f32aca133f8544d23f13e30e2f9c469ca87a2b3f626fdbc8cf4eb029989c7d8"
V236_CLOSEOUT_SHA = "bb6d7cf60d62ee06e486951fc193dc921d4f13c69a16654344312d9ce772da6c"
MUTATION_SHA = "6b9ba6391dff9de0867447045504beaa1c22776ccf9502d4065c7a3b76ab4153"
COLLECTION_URI = f"s3://{BUCKET}/{V236_PREFIX}/published"
V233_PREFIX = ("research/v233-decoded-graph-http-1m/"
               "60dc982cce46bd1baf2fb4bdf163d90be32f1aea/runs/a0001")
V233_CLOSEOUT_SHA = "9cb96ffc7b2bc45e8d3138e86f98776610cf780ffdb359ff57eca75fab74550a"
SCHEMA = "borsuk-v237-persisted-graph-http-1m-v1"


def get(s3, key):
    return s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()


def sha(body):
    return hashlib.sha256(body).hexdigest()


def rows(body):
    return [json.loads(line) for line in body.splitlines()]


def run(attempt):
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
    v236_closeout = get(s3, V236_PREFIX + "/closeout.json")
    v236 = json.loads(v236_closeout)
    if (sha(v236_closeout) != V236_CLOSEOUT_SHA or not v236["gate_pass"]
            or sha(get(s3, V236_PREFIX + "/terminal.json")) != V236_TERMINAL_SHA
            or v236["collection"]["mutation_sha256"] != MUTATION_SHA):
        raise ValueError("V236 persisted collection authority differs")
    current_head = json.loads(get(s3, V236_PREFIX + "/published/collection-head.json"))
    if (current_head["revision"] != 2 or current_head["base_root_sha256"] != ROOT_SHA
            or current_head["mutation_sha256"] != MUTATION_SHA):
        raise ValueError("persisted collection head moved")
    v233_closeout = get(s3, V233_PREFIX + "/closeout.json")
    v233 = json.loads(v233_closeout)
    if sha(v233_closeout) != V233_CLOSEOUT_SHA:
        raise ValueError("V233 baseline differs")
    baseline_terminal = json.loads(get(s3, V233_PREFIX + "/client/terminal.json"))
    if sha(get(s3, V233_PREFIX + "/client/terminal.json")) != v233["client_terminal_sha256"]:
        raise ValueError("V233 terminal differs")
    baseline = summaries(s3, V233_PREFIX, baseline_terminal)
    active = ec2.describe_instances(Filters=[
        {"Name": "tag:Name", "Values": ["borsuk-*"]},
        {"Name": "instance-state-name", "Values": ["pending", "running", "stopping"]},
    ])
    if any(row.get("Instances") for row in active["Reservations"]):
        raise ValueError("another BORSUK worker is active")
    archive = archive_source(commit)
    with tarfile.open(fileobj=io.BytesIO(archive), mode="r:gz") as source:
        required = {"scripts/run_v223_authenticated_graph_http.sh",
                    "scripts/launch_v237_persisted_graph_http_spot.py",
                    "docs/research/v237-persisted-graph-http-1m-prereg.md"}
        if not required.issubset(source.getnames()):
            raise ValueError("source archive lacks V237 gate inputs")
    archive_sha = sha(archive)
    archive_key = f"research/v237-persisted-graph-http-1m/{commit}/sources/{archive_sha}.tar.gz"
    prefix = f"research/v237-persisted-graph-http-1m/{commit}/runs/{attempt}"
    if not missing(s3, prefix + "/reservation.json"):
        raise ValueError("attempt already reserved")
    if missing(s3, archive_key):
        put_if_absent(archive_key, archive)
    quote_row = ec2.describe_spot_price_history(
        InstanceTypes=["c7i.4xlarge"], ProductDescriptions=["Linux/UNIX"],
        AvailabilityZone="eu-central-1c", MaxResults=1)["SpotPriceHistory"][0]
    quote = float(quote_row["SpotPrice"])
    put_if_absent(prefix + "/reservation.json", json.dumps({
        "schema": SCHEMA + "-reservation", "source_commit": commit,
        "source_archive_sha256": archive_sha, "v236_closeout_sha256": V236_CLOSEOUT_SHA,
        "v233_closeout_sha256": V233_CLOSEOUT_SHA, "collection_uri": COLLECTION_URI,
        "mutation_sha256": MUTATION_SHA, "dataset": "ReLAION-1M D768",
        "split": "validation-1000-prior-used", "k": 100, "concurrency": 8,
        "ef": 4096, "shortlist": 4096,
        "cache_state": "resident after cold authenticated S3 hydration; no response cache",
        "transport": "VPC peer persistent HTTP/1.1",
        "hardware": "two c7i.4xlarge Spot peers in eu-central-1c",
        "gate": "each pass 1000/1000 V233 IDs, 99668 GT100 hits, p95<=1.2x V233, QPS>=0.8x V233, RSS<=3GiB, five cold graph blob GETs, no query vector GETs",
        "spot_quote_usd_per_hour_per_host": quote,
        "spot_quote_timestamp": quote_row["Timestamp"].isoformat(),
        "interruption_policy": "discard both hosts and restart whole cell under new attempt",
    }, sort_keys=True).encode())
    ids, launched_at = {}, {}
    try:
        server_ip = ""
        for role in ("server", "client"):
            user_data = bootstrap(
                role, commit, archive_sha, archive_key, prefix,
                server_ip=server_ip, server_id=ids.get("server", ""),
                mutation_stride=100, delta_encoding="decoded",
                collection_uri=COLLECTION_URI, collection_sha=MUTATION_SHA,
            )
            request = spot_request(role, prefix, user_data)
            request["TagSpecifications"][0]["Tags"][0]["Value"] = "borsuk-v237-persisted-graph-http"
            launched = ec2.run_instances(**request)["Instances"][0]
            ids[role] = launched["InstanceId"]
            launched_at[role] = launched["LaunchTime"].timestamp()
            put_if_absent(prefix + f"/{role}/launch.json", json.dumps({
                "schema": SCHEMA + "-launch", "role": role,
                "instance_id": ids[role], "source_commit": commit,
                "launch_epoch": launched_at[role],
                "spot_quote_usd_per_hour": quote,
            }, sort_keys=True).encode())
            print(json.dumps({"role": role, "instance_id": ids[role], "prefix": prefix}), flush=True)
            if role == "server":
                ready = json.loads(read_marker(
                    s3, ec2, prefix + "/server/ready.json", ids[role], 1800))
                if (ready.get("instance_id") != ids[role]
                        or ready.get("source_commit") != commit
                        or ready.get("generation_root_sha256") != ROOT_SHA
                        or ready.get("collection_uri") != COLLECTION_URI
                        or ready.get("mutation_sha256") != MUTATION_SHA
                        or ready.get("mutation_stride") != 100
                        or ready.get("delta_encoding") != "decoded"):
                    raise ValueError("server ready identity differs")
                server_ip = ready["private_ip"]
                if server_ip != ec2.describe_instances(InstanceIds=[ids[role]])[
                        "Reservations"][0]["Instances"][0]["PrivateIpAddress"]:
                    raise ValueError("server IP differs")
        client_raw = read_marker(s3, ec2, prefix + "/client/terminal.json", ids["client"], 1200)
        client = terminal(s3, "client", prefix, client_raw, ids["client"],
                          commit, archive_sha, 100, "decoded", COLLECTION_URI)
        terminate_confirmed(ec2, ids["client"])
        server_raw = read_marker(s3, ec2, prefix + "/server/terminal.json", ids["server"], 300)
        server = terminal(s3, "server", prefix, server_raw, ids["server"],
                          commit, archive_sha, 100, "decoded", COLLECTION_URI)
        terminate_confirmed(ec2, ids["server"])
        if client["status"] != server["status"] or client["status"] != "complete":
            raise RuntimeError("V237 measurement incomplete")
        hydrate = json.loads(get(s3, prefix + "/server/artifacts/hydrate.json"))
        health = json.loads(get(s3, prefix + "/server/artifacts/health.json"))
        resources = json.loads(get(s3, prefix + "/server/artifacts/server-resources.json"))
        candidate = summaries(s3, prefix, client)
        transport_bytes = [{key: json.loads(get(
            s3, f"{prefix}/client/artifacts/{label}.summary.json"))[key]
                            for key in ("request_bytes", "response_bytes")}
                           for label in ("first", "repeat")]
        qualities = [json.loads(get(s3, f"{prefix}/client/artifacts/{label}.quality.json"))
                     for label in ("first", "repeat")]
        matches = []
        for label in ("first", "repeat"):
            current = rows(get(s3, f"{prefix}/client/sealed/{label}.raw.jsonl"))
            previous_raw = get(s3, f"{V233_PREFIX}/client/sealed/{label}.raw.jsonl")
            if sha(previous_raw) != baseline_terminal["artifacts"][f"{label}.raw.jsonl"]["sha256"]:
                raise ValueError("V233 sealed raw differs")
            previous = rows(previous_raw)
            if len(current) != len(previous) or len(current) != 1000:
                raise ValueError("paired query panel differs")
            matches.append(sum(current[i]["ordinal"] == previous[i]["ordinal"] == i
                               and current[i]["returned_ids"] == previous[i]["returned_ids"]
                               for i in range(1000)))
        gate_pass = (all(x == 1000 for x in matches)
                     and all(q["quality_pass"] and q["total_hits"] == 99668
                             and q["p05_hits_per_query"] >= 99 for q in qualities)
                     and all(candidate[i]["p95_ns"] <= 1.2 * baseline[i]["p95_ns"]
                             and candidate[i]["qps"] >= 0.8 * baseline[i]["qps"]
                             for i in range(2))
                     and hydrate["revision"] == 2
                     and hydrate["base_root_sha256"] == ROOT_SHA
                     and hydrate["mutation_sha256"] == MUTATION_SHA
                     and hydrate["graph_blob_gets"] == 5
                     and hydrate["graph_response_bytes"] == 1879697462
                     and health["delta_rows"] == 10000
                     and health["overlay_resident_bytes"] <= 64 * 1024**2
                     and resources["peak_rss_bytes"] <= 3 * 1024**3)
        estimate = sum((receipt["worker_finished_epoch"] - launched_at[role])
                       * quote / 3600 for role, receipt in (("server", server), ("client", client)))
        closeout = {"schema": SCHEMA + "-closeout", "source_commit": commit,
                    "source_archive_sha256": archive_sha,
                    "server_instance_id": ids["server"], "client_instance_id": ids["client"],
                    "server_terminal_sha256": server["terminal_sha256"],
                    "client_terminal_sha256": client["terminal_sha256"],
                    "v236_closeout_sha256": V236_CLOSEOUT_SHA,
                    "v233_closeout_sha256": V233_CLOSEOUT_SHA,
                    "candidate_summaries": candidate, "baseline_summaries": baseline,
                    "client_transport_bytes": transport_bytes,
                    "quality": qualities, "exact_v233_id_lists": matches,
                    "hydrate": hydrate, "health": health,
                    "server_peak_rss_bytes": resources["peak_rss_bytes"],
                    "spot_quote_usd_per_hour_per_host": quote,
                    "estimated_compute_usd_to_terminal": estimate,
                    "gate_pass": gate_pass}
        put_if_absent(prefix + "/closeout.json", json.dumps(closeout, sort_keys=True).encode())
        print(json.dumps(closeout, sort_keys=True), flush=True)
        if not gate_pass:
            raise RuntimeError("V237 persisted HTTP gate failed")
    finally:
        for instance_id in ids.values():
            terminate_confirmed(ec2, instance_id)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--attempt", default="a0001")
    run(parser.parse_args().attempt)
