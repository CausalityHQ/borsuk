#!/usr/bin/env python3
"""One frozen Rust RC fresh-query frontier and FAISS control on Spot."""

import argparse
import fcntl
import hashlib
import io
import json
import subprocess
import tarfile

import boto3

from scripts.launch_v157_primary_feasibility_spot import (
    BUCKET,
    REGION,
    archive_source,
    missing,
    put_if_absent,
)
from scripts.launch_v223_authenticated_graph_http_spot import (
    read_marker,
    spot_request,
    terminate_confirmed,
)

SCHEMA = "borsuk-v271-rust-rc-fresh-frontier-v1"
TERMINAL_SCHEMA = "borsuk-v271-rust-rc-fresh-frontier-terminal-v1"
LIBRARY_COMMIT = "aa4182a2e6b78a254bca732663c7528818eb6561"
SOURCE_PREFIX = "publication/v3/20260812/datasets/cohere-large-10m-768/attempts/0001"
V261_PREFIX = (
    "research/v261-cohere-dual-graph-1m/"
    "2dee58896e42f84d74e2653dc0960e19d6defc63/runs/a0001"
)
V261_TERMINAL_SHA = "00c7d4803354f15d5f62bea6e53fd0672a0f5fc91cc482e1fa4b20b8951a9f82"
V269_PREFIX = (
    "research/v269-cohere-cached-dual-http-1m/"
    "d347cd5320fb17fd28514258bff5a56783f8263a/runs/a0002"
)
V269_CLOSEOUT_SHA = "9410325d8943040189d0f6c1d46e4121f15a9d2072aba61f8d7c00b06c862a8b"
ROOT_SHA = "1e483859b96f5270209678e0f76f7cc9e26a80162a24c7fa48a942cf010ec92e"


def get(s3, key):
    return s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()


def sha(body):
    return hashlib.sha256(body).hexdigest()


def replay(s3, prefix, terminal):
    for name, identity in terminal["artifacts"].items():
        stream = s3.get_object(Bucket=BUCKET, Key=f"{prefix}/artifacts/{name}")["Body"]
        digest = hashlib.sha256()
        size = 0
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
            size += len(chunk)
        if size != identity["bytes"] or digest.hexdigest() != identity["sha256"]:
            raise ValueError(f"terminal artifact differs: {name}")


def bootstrap(commit, archive_sha, archive_key, prefix):
    uri = f"s3://{BUCKET}/{V269_PREFIX}/published"
    script = f"""#!/bin/bash
set -euo pipefail
export BORSUK_V271_BUCKET='{BUCKET}'
export BORSUK_V271_PREFIX='{prefix}'
export BORSUK_V271_SOURCE_COMMIT='{commit}'
export BORSUK_V271_ARCHIVE_SHA='{archive_sha}'
export BORSUK_V271_ARCHIVE_KEY='{archive_key}'
export BORSUK_V271_SOURCE_PREFIX='{SOURCE_PREFIX}'
export BORSUK_V271_V261_PREFIX='{V261_PREFIX}'
export BORSUK_V271_GENERATION_URI='{uri}'
bootstrap_failed() {{
  code=$?
  trap - ERR
  set +e
  token=$(curl -fsS -X PUT -H 'X-aws-ec2-metadata-token-ttl-seconds: 60' http://169.254.169.254/latest/api/token)
  instance_id=$(curl -fsS -H "X-aws-ec2-metadata-token: $token" http://169.254.169.254/latest/meta-data/instance-id)
  INSTANCE_ID="$instance_id" EXIT_CODE="$code" python3 - <<'PY' >terminal.json
import json,os,time
print(json.dumps({{'schema':'{TERMINAL_SCHEMA}',
  'source_commit':os.environ['BORSUK_V271_SOURCE_COMMIT'],
  'source_archive_sha256':os.environ['BORSUK_V271_ARCHIVE_SHA'],
  'instance_id':os.environ['INSTANCE_ID'],'exit_code':int(os.environ['EXIT_CODE']),
  'finished_epoch':int(time.time()),'phase':'bootstrap','status':'failed',
  'artifacts':{{}}}},sort_keys=True,separators=(',',':')))
PY
  aws s3 cp terminal.json "s3://$BORSUK_V271_BUCKET/$BORSUK_V271_PREFIX/terminal.json" --only-show-errors
  shutdown -h now || true
  exit "$code"
}}
trap bootstrap_failed ERR
systemd-run --unit=v271-hard-stop --on-active=7200s /usr/sbin/shutdown -h now
root=/mnt/v271-rust-rc-fresh-frontier
mkdir -p "$root" && cd "$root"
aws s3 cp 's3://{BUCKET}/{archive_key}' source.tar.gz --only-show-errors
printf '%s  source.tar.gz\\n' '{archive_sha}' | sha256sum -c -
mkdir repo
tar -xzf source.tar.gz -C repo
exec bash repo/scripts/run_v271_fresh_frontier.sh
"""
    if len(script.encode()) > 16_384:
        raise ValueError("user data exceeds EC2 limit")
    return script


def launch(attempt):
    if len(attempt) != 5 or not attempt.startswith("a") or not attempt[1:].isdigit():
        raise ValueError("attempt must be aNNNN")
    if subprocess.check_output(["git", "status", "--porcelain"], text=True).strip():
        raise ValueError("worktree must be clean")
    commit = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    subprocess.run(["git", "fetch", "origin", "main"], check=True)
    if subprocess.run(
        ["git", "merge-base", "--is-ancestor", "origin/main", "HEAD"], check=False
    ).returncode:
        raise ValueError("source is not pushed on a fast-forward branch")
    for path in ("crates/borsuk/src", "Cargo.lock"):
        if subprocess.check_output(
            ["git", "diff", "--name-only", LIBRARY_COMMIT, commit, "--", path],
            text=True,
        ).strip():
            raise ValueError(f"frozen production path changed: {path}")
    session = boto3.Session(profile_name="causality", region_name=REGION)
    s3, ec2 = session.client("s3"), session.client("ec2")
    v261 = get(s3, V261_PREFIX + "/terminal.json")
    v269 = get(s3, V269_PREFIX + "/closeout.json")
    if sha(v261) != V261_TERMINAL_SHA or sha(v269) != V269_CLOSEOUT_SHA:
        raise ValueError("historical source or generation receipt differs")
    old = json.loads(v261)
    closeout = json.loads(v269)
    if (
        old["status"] != "complete"
        or old["artifacts"]["vectors.raw"]
        != {
            "bytes": 3_072_000_000,
            "sha256": "6c82a340e3e1b4226640e593efa9c4000c6a5962d4b13063093a1dab689a9005",
        }
        or closeout["gate_pass"] is not True
        or closeout["generation_root_sha256"] != ROOT_SHA
    ):
        raise ValueError("source or V269 identity differs")
    active = ec2.describe_instances(
        Filters=[
            {"Name": "tag:Name", "Values": ["borsuk-*"]},
            {
                "Name": "instance-state-name",
                "Values": ["pending", "running", "stopping"],
            },
        ]
    )
    if any(row.get("Instances") for row in active["Reservations"]):
        raise ValueError("another BORSUK worker is active")
    archive = archive_source(commit)
    with tarfile.open(fileobj=io.BytesIO(archive), mode="r:gz") as source:
        required = {
            "scripts/run_v271_fresh_frontier.sh",
            "scripts/v271_fresh_frontier.py",
            "crates/borsuk/examples/resident_graph_frontier.rs",
            "docs/research/v271-rust-rc-fresh-frontier-prereg.md",
        }
        if not required.issubset(source.getnames()):
            raise ValueError("archive lacks frozen V271 methods")
    archive_sha = sha(archive)
    archive_key = (
        f"research/v271-rust-rc-fresh-frontier/{commit}/sources/{archive_sha}.tar.gz"
    )
    prefix = f"research/v271-rust-rc-fresh-frontier/{commit}/runs/{attempt}"
    if not missing(s3, prefix + "/reservation.json"):
        raise ValueError("attempt already reserved")
    if missing(s3, archive_key):
        put_if_absent(archive_key, archive)
    quote_row = ec2.describe_spot_price_history(
        InstanceTypes=["c7i.4xlarge"],
        ProductDescriptions=["Linux/UNIX"],
        AvailabilityZone="eu-central-1c",
        MaxResults=1,
    )["SpotPriceHistory"][0]
    quote = float(quote_row["SpotPrice"])
    put_if_absent(
        prefix + "/reservation.json",
        json.dumps(
            {
                "schema": SCHEMA + "-reservation",
                "source_commit": commit,
                "library_commit": LIBRARY_COMMIT,
                "source_archive_sha256": archive_sha,
                "v261_terminal_sha256": V261_TERMINAL_SHA,
                "v269_closeout_sha256": V269_CLOSEOUT_SHA,
                "dataset": "CoHere-large-10M first1M D768 cosine",
                "split": "heldout train rows1000000-1000999, excluded from first1M index",
                "preflight": "100k first100 holdouts; stop if GT100 hits<9800/10000",
                "frontier": "Rust generic builder and V269 artifact vs FAISS IVF-Flat on same 1000 queries",
                "cache": "resident after authenticated S3 hydration; no response cache",
                "transport": "library API, sequential queries, no HTTP",
                "spot_quote_usd_per_hour": quote,
                "spot_quote_timestamp": quote_row["Timestamp"].isoformat(),
                "interruption_policy": "discard incomplete cell and restart at new attempt",
            },
            sort_keys=True,
        ).encode(),
    )
    instance_id = None
    try:
        request = spot_request(
            "server", prefix, bootstrap(commit, archive_sha, archive_key, prefix)
        )
        request["BlockDeviceMappings"][0]["Ebs"]["VolumeSize"] = 80
        request["TagSpecifications"][0]["Tags"][0]["Value"] = (
            "borsuk-v271-rust-rc-fresh-frontier"
        )
        launched = ec2.run_instances(**request)["Instances"][0]
        instance_id = launched["InstanceId"]
        launch_epoch = launched["LaunchTime"].timestamp()
        put_if_absent(
            prefix + "/launch.json",
            json.dumps(
                {
                    "instance_id": instance_id,
                    "launch_epoch": launch_epoch,
                    "source_commit": commit,
                    "source_archive_sha256": archive_sha,
                    "spot_quote_usd_per_hour": quote,
                },
                sort_keys=True,
            ).encode(),
        )
        print(
            json.dumps(
                {
                    "instance_id": instance_id,
                    "prefix": prefix,
                    "archive_sha256": archive_sha,
                }
            ),
            flush=True,
        )
        raw = read_marker(s3, ec2, prefix + "/terminal.json", instance_id, 7500)
        terminate_confirmed(ec2, instance_id)
        terminal = json.loads(raw)
        if (
            terminal.get("schema") != TERMINAL_SCHEMA
            or terminal.get("source_commit") != commit
            or terminal.get("source_archive_sha256") != archive_sha
            or terminal.get("instance_id") != instance_id
            or terminal.get("status") not in {"complete", "failed"}
        ):
            raise ValueError("terminal identity differs")
        replay(s3, prefix, terminal)
        decision = None
        if terminal["status"] == "complete":
            if terminal.get("phase") != "complete" or terminal.get("exit_code") != 0:
                raise ValueError("complete terminal phase differs")
            identity = terminal["artifacts"].get("decision.json")
            if not identity:
                raise ValueError("decision missing")
            body = get(s3, prefix + "/artifacts/decision.json")
            if len(body) != identity["bytes"] or sha(body) != identity["sha256"]:
                raise ValueError("decision identity differs")
            decision = json.loads(body)
            if decision.get("decision") not in {"no_go_100k", "no_go_1m", "go_10m"}:
                raise ValueError("decision code differs")
        result = {
            "schema": SCHEMA + "-closeout",
            "source_commit": commit,
            "library_commit": LIBRARY_COMMIT,
            "source_archive_sha256": archive_sha,
            "instance_id": instance_id,
            "terminal_sha256": sha(raw),
            "terminal_status": terminal["status"],
            "phase": terminal.get("phase"),
            "decision": decision,
            "spot_quote_usd_per_hour": quote,
            "spot_compute_usd_through_terminal": max(
                0, terminal["finished_epoch"] - launch_epoch
            )
            * quote
            / 3600,
        }
        body = json.dumps(result, sort_keys=True, separators=(",", ":")).encode()
        put_if_absent(prefix + "/closeout.json", body)
        if get(s3, prefix + "/closeout.json") != body:
            raise ValueError("closeout readback differs")
        print(
            json.dumps({"closeout_sha256": sha(body), **result}, sort_keys=True),
            flush=True,
        )
    finally:
        if instance_id:
            terminate_confirmed(ec2, instance_id)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--attempt", default="a0001")
    args = parser.parse_args()
    with open("/tmp/borsuk-v271-frontier-launch.lock", "a+") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        launch(args.attempt)
