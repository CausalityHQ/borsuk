#!/usr/bin/env python3
"""Run one frozen V280 ReLAION baseline HTTP serving cell on two Spot hosts."""

import argparse
import fcntl
import hashlib
import io
import json
import subprocess
import tarfile

import boto3

from scripts.launch_v157_primary_feasibility_spot import BUCKET, REGION, archive_source, missing, put_if_absent
from scripts.launch_v223_authenticated_graph_http_spot import (
    bootstrap, read_marker, spot_request, summaries, terminal, terminate_confirmed,
)

V279_PREFIX = ("research/v279-relaion-transfer-1m/"
               "8206cbc9a7d0949146b2a8739f4964725871a913/runs/a0001")
V279_TERMINAL_SHA = "b7c14d3917a596d7d5451a15cd48196332651af7a89a3aa3228d9b9f323c74e7"
ROOT_SHA = "bdc994e1f58c817ebfd95c7ddaa3aca37fe182f7dc73a569a61ad39a73ec2565"
BLOB_BYTES = 1_879_999_990
SCHEMA = "borsuk-v280-relaion-baseline-http-1m-v1"


def get(s3, key):
    return s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()


def sha(body):
    return hashlib.sha256(body).hexdigest()


def verify_v279(raw):
    value = json.loads(raw)
    artifacts = value["artifacts"]
    if (sha(raw) != V279_TERMINAL_SHA or value["status"] != "complete"
            or value["source_commit"] != "8206cbc9a7d0949146b2a8739f4964725871a913"
            or artifacts["baseline/root.json"]["sha256"] != ROOT_SHA
            or sum(artifacts[f"baseline/{name}"]["bytes"] for name in
                   ("plane.bin", "graph.bin", "map.u32", "books.bin", "codes.bin")) != BLOB_BYTES
            or artifacts["baseline.default.raw.jsonl"]["sha256"]
            != "291c10bcea15003959d556fdb7827522a498c166f3ea3a9dca30480ecc733d41"
            or artifacts["queries.f32"]["sha256"]
            != "82fe696c1d765d27b9f8f6c5277a998a4ebbf2bd6d6f89533ad3a5445105525a"
            or artifacts["truth.u32"]["sha256"]
            != "23180f9b7e7727f478672d919e08e7bc17bdb75a76c9308d69dbc44c41bc1db7"):
        raise ValueError("V279 predecessor identity differs")
    return value


def launch(attempt, dry_run=False):
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
    verify_v279(get(s3, V279_PREFIX + "/terminal.json"))
    archive = archive_source(commit)
    with tarfile.open(fileobj=io.BytesIO(archive), mode="r:gz") as source:
        required = {"scripts/run_v223_authenticated_graph_http.sh",
                    "scripts/v220_bench_graph_http.py", "scripts/v280_http.py",
                    "crates/borsuk/examples/v220_graph_http.rs",
                    "crates/borsuk/examples/v246_publish_graph_s3.rs",
                    "docs/research/v280-relaion-baseline-http-1m-prereg.md"}
        if not required.issubset(source.getnames()):
            raise ValueError("V280 source archive incomplete")
    active = ec2.describe_instances(Filters=[
        {"Name": "tag:Name", "Values": ["borsuk-*"]},
        {"Name": "instance-state-name", "Values": ["pending", "running", "stopping"]},
    ])
    if any(row.get("Instances") for row in active["Reservations"]):
        raise ValueError("another BORSUK worker is active")
    archive_sha = sha(archive)
    archive_key = f"research/v280-relaion-baseline-http-1m/{commit}/sources/{archive_sha}.tar.gz"
    prefix = f"research/v280-relaion-baseline-http-1m/{commit}/runs/{attempt}"
    uri = f"s3://{BUCKET}/{prefix}/published"
    if not missing(s3, prefix + "/reservation.json"):
        raise ValueError("attempt already reserved")
    quote_row = ec2.describe_spot_price_history(
        InstanceTypes=["c7i.4xlarge"], ProductDescriptions=["Linux/UNIX"],
        AvailabilityZone="eu-central-1c", MaxResults=1)["SpotPriceHistory"][0]
    quote = float(quote_row["SpotPrice"])
    if dry_run:
        print(json.dumps({"source_commit": commit, "archive_sha256": archive_sha,
                          "v279_terminal_sha256": V279_TERMINAL_SHA, "root_sha256": ROOT_SHA,
                          "prefix": prefix, "active_borsuk_instances": 0,
                          "spot_quote_usd_per_host_hour": quote}, sort_keys=True))
        return
    if missing(s3, archive_key):
        put_if_absent(archive_key, archive)
    put_if_absent(prefix + "/reservation.json", json.dumps({
        "schema": SCHEMA + "-reservation", "source_commit": commit,
        "source_archive_sha256": archive_sha, "v279_terminal_sha256": V279_TERMINAL_SHA,
        "generation_root_sha256": ROOT_SHA, "generation_uri": uri,
        "dataset": "ReLAION-1M D768 cosine k100", "split": "historically used validation0-999",
        "concurrency": 8, "transport": "VPC-peer persistent HTTP/1.1",
        "cache": "resident after empty-cache S3 hydration; no response cache",
        "interruption": "discard both hosts and restart entire cell",
        "spot_quote_usd_per_host_hour": quote,
        "spot_quote_timestamp": quote_row["Timestamp"].isoformat(),
    }, sort_keys=True).encode())
    ids, launched = {}, {}
    try:
        server_ip = ""
        for role in ("server", "client"):
            script = bootstrap(role, commit, archive_sha, archive_key, prefix,
                               server_ip=server_ip, server_id=ids.get("server", ""),
                               root_sha=ROOT_SHA, generation_uri=uri, v280=True)
            request = spot_request(role, prefix, script)
            request["TagSpecifications"][0]["Tags"][0]["Value"] = "borsuk-v280-relaion-http"
            row = ec2.run_instances(**request)["Instances"][0]
            ids[role] = row["InstanceId"]
            launched[role] = row["LaunchTime"].timestamp()
            put_if_absent(prefix + f"/{role}/launch.json", json.dumps({
                "instance_id": ids[role], "role": role, "source_commit": commit,
                "launch_epoch": launched[role], "spot_quote_usd_per_hour": quote,
            }, sort_keys=True).encode())
            print(json.dumps({"role": role, "instance_id": ids[role], "prefix": prefix}), flush=True)
            if role == "server":
                ready = json.loads(read_marker(s3, ec2, prefix + "/server/ready.json", ids[role], 1800))
                if (ready["instance_id"] != ids[role] or ready["source_commit"] != commit
                        or ready["generation_root_sha256"] != ROOT_SHA
                        or ready["generation_uri"] != uri or ready["v280"] is not True):
                    raise ValueError("server ready identity differs")
                server_ip = ready["private_ip"]
                actual = ec2.describe_instances(InstanceIds=[ids[role]])[
                    "Reservations"][0]["Instances"][0]["PrivateIpAddress"]
                if server_ip != actual:
                    raise ValueError("server IP differs")
        client_raw = read_marker(s3, ec2, prefix + "/client/terminal.json", ids["client"], 1800)
        client = terminal(s3, "client", prefix, client_raw, ids["client"], commit,
                          archive_sha, root_sha=ROOT_SHA, generation_uri=uri, v280=True)
        terminate_confirmed(ec2, ids["client"])
        server_raw = read_marker(s3, ec2, prefix + "/server/terminal.json", ids["server"], 300)
        server = terminal(s3, "server", prefix, server_raw, ids["server"], commit,
                          archive_sha, root_sha=ROOT_SHA, generation_uri=uri, v280=True)
        terminate_confirmed(ec2, ids["server"])
        if client["status"] != "complete" or server["status"] != "complete":
            raise RuntimeError("V280 measurement incomplete")
        hydration = json.loads(get(s3, prefix + "/server/artifacts/hydrate.json"))
        resources = json.loads(get(s3, prefix + "/server/artifacts/server-resources.json"))
        passes = summaries(s3, prefix, client)
        quality = [json.loads(get(s3, f"{prefix}/client/artifacts/{label}.quality.json"))
                   for label in ("first", "repeat")]
        transport = [json.loads(get(s3, f"{prefix}/client/artifacts/{label}.summary.json"))
                     for label in ("first", "repeat")]
        gate_pass = (hydration["base_root_sha256"] == ROOT_SHA
                     and hydration["graph_blob_gets"] == 5
                     and hydration["graph_response_bytes"] == BLOB_BYTES
                     and resources["peak_rss_bytes"] <= 3 * 1024**3
                     and all(row["quality_pass"] and row["parity"] == 1000
                             and row["hits"] == 99_989 for row in quality)
                     and all(row["p95_ns"] <= 110_000_000
                             and row["p99_ns"] <= 130_000_000
                             and row["qps"] >= 85 and row["vector_body_gets"] == 0
                             for row in transport))
        cost = sum((receipt["worker_finished_epoch"] - launched[role]) * quote / 3600
                   for role, receipt in (("server", server), ("client", client)))
        result = {"schema": SCHEMA + "-closeout", "source_commit": commit,
                  "source_archive_sha256": archive_sha, "v279_terminal_sha256": V279_TERMINAL_SHA,
                  "generation_root_sha256": ROOT_SHA, "generation_uri": uri,
                  "server_instance_id": ids["server"], "client_instance_id": ids["client"],
                  "server_terminal_sha256": server["terminal_sha256"],
                  "client_terminal_sha256": client["terminal_sha256"],
                  "server_peak_rss_bytes": resources["peak_rss_bytes"],
                  "hydration": hydration, "passes": passes, "quality": quality,
                  "transport": transport, "spot_quote_usd_per_host_hour": quote,
                  "spot_compute_usd_through_terminal": cost, "gate_pass": gate_pass}
        body = json.dumps(result, sort_keys=True, separators=(",", ":")).encode()
        put_if_absent(prefix + "/closeout.json", body)
        if get(s3, prefix + "/closeout.json") != body:
            raise ValueError("closeout readback differs")
        print(json.dumps({"closeout_sha256": sha(body), **result}, sort_keys=True), flush=True)
    finally:
        for instance_id in ids.values():
            terminate_confirmed(ec2, instance_id)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--attempt", default="a0001")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    with open("/tmp/borsuk-v280-http-launch.lock", "a+") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        launch(args.attempt, args.dry_run)
