#!/usr/bin/env python3
"""Publish the sealed V261 generation and measure one two-host HTTP cell."""

import argparse
import fcntl
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

SCHEMA = "borsuk-v262-cohere-dual-http-1m-v1"
V261_PREFIX = ("research/v261-cohere-dual-graph-1m/"
               "2dee58896e42f84d74e2653dc0960e19d6defc63/runs/a0001")
V261_TERMINAL_SHA = "00c7d4803354f15d5f62bea6e53fd0672a0f5fc91cc482e1fa4b20b8951a9f82"
ROOT_SHA = "1e483859b96f5270209678e0f76f7cc9e26a80162a24c7fa48a942cf010ec92e"
BLOB_BYTES = 1_880_914_634


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
    prior_raw = get(s3, V261_PREFIX + "/terminal.json")
    prior = json.loads(prior_raw)
    if (sha(prior_raw) != V261_TERMINAL_SHA or prior.get("status") != "complete"
            or prior["artifacts"]["raw.jsonl"]["sha256"]
            != "d8566803b49197ae454d8ecc3775adc8bd4e9a48629885db546a043b9ae94246"
            or prior["artifacts"]["truth.u32"]["sha256"]
            != "62e14eba043fafb8d8ec7c833d7d320c5d823c549683d15e5eacdff365a87f39"):
        raise ValueError("V261 predecessor identity differs")
    archive = archive_source(commit)
    with tarfile.open(fileobj=io.BytesIO(archive), mode="r:gz") as source:
        required = {"scripts/run_v223_authenticated_graph_http.sh",
                    "scripts/v220_bench_graph_http.py",
                    "scripts/v262_score_cohere_dual_http.py",
                    "crates/borsuk/examples/v220_graph_http.rs",
                    "crates/borsuk/examples/v246_publish_graph_s3.rs",
                    "docs/research/v262-cohere-1m-generation.json",
                    "docs/research/v262-cohere-dual-graph-http-1m-prereg.md"}
        if not required.issubset(source.getnames()):
            raise ValueError("source archive lacks V262 inputs")
        root = source.extractfile("docs/research/v262-cohere-1m-generation.json").read()
    identity = json.loads(root)
    if (sha(root) != ROOT_SHA or identity["source_sha256"]
            != prior["artifacts"]["vectors.raw"]["sha256"]
            or any(identity[name] != prior["artifacts"][file]
                   for name, file in (("plane", "plane.bin"), ("graph", "graph.bin"),
                                      ("map", "map.u32"), ("books", "books.bin"),
                                      ("codes", "codes.bin")))
            or sum(identity[name]["bytes"] for name in
                   ("plane", "graph", "map", "books", "codes")) != BLOB_BYTES):
        raise ValueError("trusted CoHere generation root differs")
    active = ec2.describe_instances(Filters=[
        {"Name": "tag:Name", "Values": ["borsuk-*"]},
        {"Name": "instance-state-name", "Values": ["pending", "running", "stopping"]},
    ])
    if any(row.get("Instances") for row in active["Reservations"]):
        raise ValueError("another BORSUK worker is active")
    archive_sha = sha(archive)
    archive_key = f"research/v262-cohere-dual-http-1m/{commit}/sources/{archive_sha}.tar.gz"
    prefix = f"research/v262-cohere-dual-http-1m/{commit}/runs/{attempt}"
    uri = f"s3://{BUCKET}/{prefix}/published"
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
        "source_archive_sha256": archive_sha, "v261_terminal_sha256": V261_TERMINAL_SHA,
        "generation_root_sha256": ROOT_SHA, "generation_uri": uri,
        "dataset": "CoHere-large-10M first1M D768 cosine k100",
        "split": "prior-used development0-255 validation256-999",
        "search": "PQ ef4096/shortlist4096 + exact FP16 ef2048; final FP16 union",
        "concurrency": 8, "transport": "VPC peer persistent HTTP/1.1",
        "cache_state": "resident after empty-cache S3 hydration; no response cache",
        "gate": "both passes exact 1000 V261 ID lists; p95<=150ms p99<=175ms QPS>=65 RSS<=3GiB five cold blob GETs zero query GETs",
        "spot_quote_usd_per_hour_per_host": quote,
        "spot_quote_timestamp": quote_row["Timestamp"].isoformat(),
        "interruption_policy": "discard both hosts and restart the entire cell",
    }, sort_keys=True).encode())
    ids, launched = {}, {}
    try:
        server_ip = ""
        for role in ("server", "client"):
            script = bootstrap(role, commit, archive_sha, archive_key, prefix,
                               server_ip=server_ip, server_id=ids.get("server", ""),
                               root_sha=ROOT_SHA, generation_uri=uri, cohere_dual=True)
            request = spot_request(role, prefix, script)
            request["TagSpecifications"][0]["Tags"][0]["Value"] = "borsuk-v262-cohere-dual-http"
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
                        or ready.get("generation_uri") != uri
                        or ready.get("cohere_dual") is not True):
                    raise ValueError("server ready identity differs")
                server_ip = ready["private_ip"]
                actual = ec2.describe_instances(InstanceIds=[ids[role]])[
                    "Reservations"][0]["Instances"][0]["PrivateIpAddress"]
                if server_ip != actual:
                    raise ValueError("server IP differs")
        client_raw = read_marker(s3, ec2, prefix + "/client/terminal.json", ids["client"], 1800)
        client = terminal(s3, "client", prefix, client_raw, ids["client"], commit,
                          archive_sha, root_sha=ROOT_SHA, generation_uri=uri, cohere_dual=True)
        terminate_confirmed(ec2, ids["client"])
        server_raw = read_marker(s3, ec2, prefix + "/server/terminal.json", ids["server"], 300)
        server = terminal(s3, "server", prefix, server_raw, ids["server"], commit,
                          archive_sha, root_sha=ROOT_SHA, generation_uri=uri, cohere_dual=True)
        terminate_confirmed(ec2, ids["server"])
        if client["status"] != server["status"] or client["status"] != "complete":
            raise RuntimeError("V262 measurement incomplete")
        published = json.loads(get(s3, prefix + "/server/artifacts/publish.json"))
        hydrate = json.loads(get(s3, prefix + "/server/artifacts/hydrate.json"))
        resources = json.loads(get(s3, prefix + "/server/artifacts/server-resources.json"))
        passes = summaries(s3, prefix, client)
        quality = [json.loads(get(s3, f"{prefix}/client/artifacts/{label}.quality.json"))
                   for label in ("first", "repeat")]
        transport = [json.loads(get(s3, f"{prefix}/client/artifacts/{label}.summary.json"))
                     for label in ("first", "repeat")]
        gate_pass = (published["root_sha256"] == ROOT_SHA
                     and hydrate["base_root_sha256"] == ROOT_SHA
                     and hydrate["graph_blob_gets"] == 5
                     and hydrate["graph_response_bytes"] == BLOB_BYTES
                     and resources["peak_rss_bytes"] <= 3 * 1024**3
                     and all(row["quality_pass"] and row["exact_v261_id_lists"] == 1000
                             and row["hits"] == 99_717 for row in quality)
                     and all(row["p95_ns"] <= 150_000_000
                             and row["p99_ns"] <= 175_000_000
                             and row["qps"] >= 65 and row["vector_body_gets"] == 0
                             for row in transport))
        cost = sum((receipt["worker_finished_epoch"] - launched[role]) * quote / 3600
                   for role, receipt in (("server", server), ("client", client)))
        result = {"schema": SCHEMA + "-closeout", "source_commit": commit,
                  "source_archive_sha256": archive_sha,
                  "v261_terminal_sha256": V261_TERMINAL_SHA,
                  "generation_root_sha256": ROOT_SHA, "generation_uri": uri,
                  "server_instance_id": ids["server"], "client_instance_id": ids["client"],
                  "server_terminal_sha256": server["terminal_sha256"],
                  "client_terminal_sha256": client["terminal_sha256"],
                  "server_peak_rss_bytes": resources["peak_rss_bytes"],
                  "hydration": hydrate, "passes": passes, "quality": quality,
                  "transport": transport, "spot_quote_usd_per_hour_per_host": quote,
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
    args = parser.parse_args()
    with open("/tmp/borsuk-cohere-http-launch.lock", "a+") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        launch(args.attempt)
