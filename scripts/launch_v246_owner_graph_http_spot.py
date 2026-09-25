#!/usr/bin/env python3
"""One authenticated V245 graph publication and VPC-peer HTTP Spot cell."""

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
    bootstrap, read_marker, spot_request, summaries, terminal, terminate_confirmed,
)

ROOT_SHA = "58dcfbda4e5dec2402043c44e0e7f02b649d1d0a46d69ab1be48c61cf5add3dc"
V245_PREFIX = ("research/v245-owner-graph-1m/"
               "a45c8f9635b7663334367960d2e423ddf29769f4/runs/a0001")
V245_TERMINAL_SHA = "632bb364eb026220116f45e7e5ff5f980d197a4d0668c491773344cfd03ff689"
V245_RAW_SHA = "dffb4e97d7ab0d00745673d299df6d2c9c8b62f881d7d85eaf61f0fdd585216a"
GRAPH_SHA = "92df3782b2836e608d206401c4efdd3d71bd8e81fe18887b0e93af4d23d8ade3"
SCHEMA = "borsuk-v246-owner-graph-http-1m-v1"


def get(s3, key):
    return s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()


def sha(body):
    return hashlib.sha256(body).hexdigest()


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
    predecessor = get(s3, V245_PREFIX + "/terminal.json")
    receipt = json.loads(predecessor)
    if (sha(predecessor) != V245_TERMINAL_SHA or receipt["status"] != "complete"
            or receipt["artifacts"]["graph.bin"]["sha256"] != GRAPH_SHA
            or receipt["artifacts"]["raw.jsonl"]["sha256"] != V245_RAW_SHA
            or sha(get(s3, V245_PREFIX + "/artifacts/build-summary.json"))
            != receipt["artifacts"]["build-summary.json"]["sha256"]):
        raise ValueError("V245 predecessor identity differs")
    active = ec2.describe_instances(Filters=[
        {"Name": "tag:Name", "Values": ["borsuk-*"]},
        {"Name": "instance-state-name", "Values": ["pending", "running", "stopping"]},
    ])
    if any(row.get("Instances") for row in active["Reservations"]):
        raise ValueError("another BORSUK worker is active")
    archive = archive_source(commit)
    with tarfile.open(fileobj=io.BytesIO(archive), mode="r:gz") as source:
        required = {"scripts/run_v223_authenticated_graph_http.sh",
                    "scripts/v246_score_graph_http.py",
                    "crates/borsuk/examples/v246_publish_graph_s3.rs",
                    "docs/research/v246-relaion-1m-generation.json",
                    "docs/research/v246-owner-graph-persisted-http-1m-prereg.md"}
        if not required.issubset(source.getnames()):
            raise ValueError("source archive lacks V246 inputs")
        root = source.extractfile("docs/research/v246-relaion-1m-generation.json").read()
        if sha(root) != ROOT_SHA or json.loads(root)["graph"]["sha256"] != GRAPH_SHA:
            raise ValueError("trusted V246 root differs")
    archive_sha = sha(archive)
    archive_key = f"research/v246-owner-graph-http-1m/{commit}/sources/{archive_sha}.tar.gz"
    prefix = f"research/v246-owner-graph-http-1m/{commit}/runs/{attempt}"
    generation_uri = f"s3://{BUCKET}/{prefix}/published"
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
        "source_archive_sha256": archive_sha, "v245_terminal_sha256": V245_TERMINAL_SHA,
        "generation_uri": generation_uri, "generation_root_sha256": ROOT_SHA,
        "dataset": "ReLAION-1M D768", "split": "validation-1000-already-used",
        "metric": "cosine", "k": 100, "ef": 4096, "shortlist": 4096,
        "concurrency": 8, "transport": "VPC peer persistent HTTP/1.1",
        "cache_state": "resident after fresh S3 hydration; no response cache",
        "gate": "both passes 1000 V245 ID lists, 99662 hits, p05>=98, p95<=35ms, p99<=45ms, QPS>=300, server RSS<=3GiB, five cold blob GETs, zero query vector GETs",
        "spot_quote_usd_per_hour_per_host": quote,
        "spot_quote_timestamp": quote_row["Timestamp"].isoformat(),
        "interruption_policy": "discard both hosts and restart complete cell under new attempt",
    }, sort_keys=True).encode())
    ids, launched = {}, {}
    try:
        server_ip = ""
        for role in ("server", "client"):
            script = bootstrap(role, commit, archive_sha, archive_key, prefix,
                               server_ip=server_ip, server_id=ids.get("server", ""),
                               root_sha=ROOT_SHA, generation_uri=generation_uri)
            request = spot_request(role, prefix, script)
            request["TagSpecifications"][0]["Tags"][0]["Value"] = "borsuk-v246-owner-graph-http"
            row = ec2.run_instances(**request)["Instances"][0]
            ids[role] = row["InstanceId"]
            launched[role] = row["LaunchTime"].timestamp()
            put_if_absent(prefix + f"/{role}/launch.json", json.dumps({
                "schema": SCHEMA + "-launch", "role": role, "instance_id": ids[role],
                "source_commit": commit, "launch_epoch": launched[role],
                "spot_quote_usd_per_hour": quote,
            }, sort_keys=True).encode())
            print(json.dumps({"role": role, "instance_id": ids[role], "prefix": prefix}), flush=True)
            if role == "server":
                ready = json.loads(read_marker(s3, ec2, prefix + "/server/ready.json", ids[role], 1800))
                if (ready.get("instance_id") != ids[role]
                        or ready.get("source_commit") != commit
                        or ready.get("generation_root_sha256") != ROOT_SHA
                        or ready.get("generation_uri") != generation_uri):
                    raise ValueError("server ready identity differs")
                server_ip = ready["private_ip"]
                actual = ec2.describe_instances(InstanceIds=[ids[role]])[
                    "Reservations"][0]["Instances"][0]["PrivateIpAddress"]
                if server_ip != actual:
                    raise ValueError("server IP differs")
        client_raw = read_marker(s3, ec2, prefix + "/client/terminal.json", ids["client"], 1200)
        client = terminal(s3, "client", prefix, client_raw, ids["client"], commit,
                          archive_sha, root_sha=ROOT_SHA, generation_uri=generation_uri)
        terminate_confirmed(ec2, ids["client"])
        server_raw = read_marker(s3, ec2, prefix + "/server/terminal.json", ids["server"], 300)
        server = terminal(s3, "server", prefix, server_raw, ids["server"], commit,
                          archive_sha, root_sha=ROOT_SHA, generation_uri=generation_uri)
        terminate_confirmed(ec2, ids["server"])
        if client["status"] != server["status"] or client["status"] != "complete":
            raise RuntimeError("V246 measurement incomplete")
        published = json.loads(get(s3, prefix + "/server/artifacts/publish.json"))
        hydrate = json.loads(get(s3, prefix + "/server/artifacts/hydrate.json"))
        resources = json.loads(get(s3, prefix + "/server/artifacts/server-resources.json"))
        summaries_by_pass = summaries(s3, prefix, client)
        qualities = [json.loads(get(s3, f"{prefix}/client/artifacts/{label}.quality.json"))
                     for label in ("first", "repeat")]
        transport = [json.loads(get(s3, f"{prefix}/client/artifacts/{label}.summary.json"))
                     for label in ("first", "repeat")]
        gate_pass = (published["root_sha256"] == ROOT_SHA
                     and hydrate["base_root_sha256"] == ROOT_SHA
                     and hydrate["graph_blob_gets"] == 5
                     and hydrate["graph_response_bytes"] == 1879696738
                     and all(q["quality_pass"] and q["dev_hits"] == 25506
                             and q["remaining_hits"] == 74156 for q in qualities)
                     and all(row["p95_ns"] <= 35_000_000
                             and row["p99_ns"] <= 45_000_000
                             and row["qps"] >= 300 for row in summaries_by_pass)
                     and resources["peak_rss_bytes"] <= 3 * 1024**3)
        estimate = sum((receipt["worker_finished_epoch"] - launched[role])
                       * quote / 3600 for role, receipt in (("server", server), ("client", client)))
        closeout = {"schema": SCHEMA + "-closeout", "source_commit": commit,
                    "source_archive_sha256": archive_sha, "v245_terminal_sha256": V245_TERMINAL_SHA,
                    "server_instance_id": ids["server"], "client_instance_id": ids["client"],
                    "server_terminal_sha256": server["terminal_sha256"],
                    "client_terminal_sha256": client["terminal_sha256"],
                    "generation_uri": generation_uri, "generation_root_sha256": ROOT_SHA,
                    "publish": published, "hydrate": hydrate, "qualities": qualities,
                    "summaries": summaries_by_pass,
                    "transport_bytes": [{key: row[key] for key in ("request_bytes", "response_bytes")}
                                        for row in transport],
                    "server_peak_rss_bytes": resources["peak_rss_bytes"],
                    "spot_quote_usd_per_hour_per_host": quote,
                    "estimated_compute_usd_to_terminal": estimate, "gate_pass": gate_pass}
        put_if_absent(prefix + "/closeout.json", json.dumps(closeout, sort_keys=True).encode())
        print(json.dumps(closeout, sort_keys=True), flush=True)
        if not gate_pass:
            raise RuntimeError("V246 persisted HTTP gate failed")
    finally:
        for instance_id in ids.values():
            terminate_confirmed(ec2, instance_id)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--attempt", default="a0001")
    launch(parser.parse_args().attempt)
