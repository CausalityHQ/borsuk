#!/usr/bin/env python3
"""Launch one paired V282 100k falsifier on Causality Spot."""

import argparse
import fcntl
import io
import json
import subprocess
import tarfile

import boto3

from scripts.launch_v157_primary_feasibility_spot import (
    BUCKET, REGION, archive_source, missing, put_if_absent,
)
from scripts.launch_v223_authenticated_graph_http_spot import (
    read_marker, spot_request, terminate_confirmed,
)
from scripts.launch_v271_rust_rc_frontier_spot import get, replay, sha

SCHEMA = "borsuk-v282-paired-100k-v1"
TERMINAL_SCHEMA = "borsuk-v282-paired-100k-terminal-v1"


def bootstrap(commit, archive_sha, archive_key, prefix):
    script = f"""#!/bin/bash
set -euo pipefail
export BORSUK_V282_BUCKET='{BUCKET}'
export BORSUK_V282_PREFIX='{prefix}'
export BORSUK_V282_COMMIT='{commit}'
export BORSUK_V282_ARCHIVE_SHA='{archive_sha}'
bootstrap_failed() {{
  code=$?
  trap - ERR
  set +e
  token=$(curl -fsS -X PUT -H 'X-aws-ec2-metadata-token-ttl-seconds: 60' http://169.254.169.254/latest/api/token)
  instance_id=$(curl -fsS -H "X-aws-ec2-metadata-token: $token" http://169.254.169.254/latest/meta-data/instance-id)
  INSTANCE_ID="$instance_id" EXIT_CODE="$code" python3 - <<'PY' >terminal.json
import json,os,time
print(json.dumps({{'schema':'{TERMINAL_SCHEMA}',
  'source_commit':os.environ['BORSUK_V282_COMMIT'],
  'source_archive_sha256':os.environ['BORSUK_V282_ARCHIVE_SHA'],
  'instance_id':os.environ['INSTANCE_ID'],'exit_code':int(os.environ['EXIT_CODE']),
  'finished_epoch':int(time.time()),'phase':'bootstrap','status':'failed',
  'artifacts':{{}}}},sort_keys=True,separators=(',',':')))
PY
  aws s3 cp terminal.json "s3://$BORSUK_V282_BUCKET/$BORSUK_V282_PREFIX/terminal.json" --only-show-errors
  shutdown -h now || true
  exit "$code"
}}
trap bootstrap_failed ERR
systemd-run --unit=v282-hard-stop --on-active=43200s /usr/sbin/shutdown -h now
root=/mnt/v282-100k
mkdir -p "$root" && cd "$root"
aws s3 cp 's3://{BUCKET}/{archive_key}' source.tar.gz --only-show-errors
printf '%s  source.tar.gz\\n' '{archive_sha}' | sha256sum -c -
mkdir repo && tar -xzf source.tar.gz -C repo
exec bash repo/scripts/run_v282_100k.sh
"""
    if len(script.encode()) > 16_384:
        raise ValueError("V282 user data exceeds EC2 limit")
    return script


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
    active = ec2.describe_instances(Filters=[
        {"Name": "tag:Name", "Values": ["borsuk-v282-*"]},
        {"Name": "instance-state-name", "Values": ["pending", "running", "stopping"]},
    ])
    if any(reservation.get("Instances") for reservation in active["Reservations"]):
        raise ValueError("V282 already has an active worker")
    archive = archive_source(commit)
    with tarfile.open(fileobj=io.BytesIO(archive), mode="r:gz") as source:
        required = {"scripts/run_v282_100k.sh", "scripts/v282_prepare_pair.py",
                    "scripts/v282_summarize_falsifier.py",
                    "crates/borsuk/src/bin/v282_build_routing.rs",
                    "crates/borsuk/src/bin/v282_local_falsifier.rs",
                    "docs/research/v282-object-native-sq8-prereg.md"}
        if not required.issubset(source.getnames()):
            raise ValueError("V282 source archive incomplete")
    archive_sha = sha(archive)
    archive_key = f"research/v282-paired-100k/{commit}/sources/{archive_sha}.tar.gz"
    prefix = f"research/v282-paired-100k/{commit}/runs/{attempt}"
    if not missing(s3, prefix + "/reservation.json"):
        raise ValueError("attempt already reserved")
    quote_row = ec2.describe_spot_price_history(
        InstanceTypes=["c7i.8xlarge"], ProductDescriptions=["Linux/UNIX"],
        AvailabilityZone="eu-central-1c", MaxResults=1)["SpotPriceHistory"][0]
    quote = float(quote_row["SpotPrice"])
    if dry_run:
        print(json.dumps({"source_commit": commit, "source_archive_sha256": archive_sha,
                          "prefix": prefix, "active_v282_instances": 0,
                          "spot_quote_usd_per_hour": quote}, sort_keys=True))
        return
    if missing(s3, archive_key):
        put_if_absent(archive_key, archive)
    put_if_absent(prefix + "/reservation.json", json.dumps({
        "schema": SCHEMA + "-reservation", "source_commit": commit,
        "source_archive_sha256": archive_sha,
        "datasets": ["ReLAION-first100k D768 cosine k100",
                     "CoHere-first100k D768 cosine k100"],
        "split": "prior-used development0-255 / validation256-999",
        "method": "query-blind V120/V115 source layout; paired V282 graph/flat; local authenticated SQ8 replay",
        "physical_caps": {"gets": 32, "bytes": 16_777_216},
        "purchase_option": "spot", "spot_quote_usd_per_hour": quote,
        "spot_quote_timestamp": quote_row["Timestamp"].isoformat(),
        "interruption": "sync completed dataset evidence; discard interrupted cell and restart as new attempt",
        "overlap": "original V281 10M job preserved; distinct V282 source and worker",
    }, sort_keys=True).encode())
    instance_id = None
    try:
        request = spot_request("server", prefix,
                               bootstrap(commit, archive_sha, archive_key, prefix))
        request["InstanceType"] = "c7i.8xlarge"
        request["BlockDeviceMappings"][0]["Ebs"]["VolumeSize"] = 250
        request["TagSpecifications"][0]["Tags"][0]["Value"] = "borsuk-v282-paired-100k"
        launched = ec2.run_instances(**request)["Instances"][0]
        instance_id = launched["InstanceId"]
        launch_epoch = launched["LaunchTime"].timestamp()
        put_if_absent(prefix + "/launch.json", json.dumps({
            "instance_id": instance_id, "launch_epoch": launch_epoch,
            "source_commit": commit, "source_archive_sha256": archive_sha,
            "spot_quote_usd_per_hour": quote,
        }, sort_keys=True).encode())
        print(json.dumps({"instance_id": instance_id, "prefix": prefix,
                          "archive_sha256": archive_sha}), flush=True)
        raw = read_marker(s3, ec2, prefix + "/terminal.json", instance_id, 43_800)
        terminate_confirmed(ec2, instance_id)
        terminal = json.loads(raw)
        if (terminal.get("schema") != TERMINAL_SCHEMA
                or terminal.get("source_commit") != commit
                or terminal.get("source_archive_sha256") != archive_sha
                or terminal.get("instance_id") != instance_id
                or terminal.get("status") not in {"complete", "failed"}):
            raise ValueError("V282 terminal identity differs")
        replay(s3, prefix, terminal)
        summary = json.loads(get(s3, prefix + "/artifacts/summary.json")) if terminal["status"] == "complete" else None
        if summary is not None and (summary["schema"] != "borsuk-v282-paired-falsifier-v1"
                                    or summary["dataset_rows"] != 100_000):
            raise ValueError("V282 decision schema differs")
        result = {"schema": SCHEMA + "-closeout", "source_commit": commit,
                  "source_archive_sha256": archive_sha, "instance_id": instance_id,
                  "terminal_sha256": sha(raw), "terminal_status": terminal["status"],
                  "phase": terminal.get("phase"), "decision": summary,
                  "spot_quote_usd_per_hour": quote,
                  "spot_compute_usd_through_terminal_estimate":
                  max(0, terminal["finished_epoch"] - launch_epoch) * quote / 3600}
        body = json.dumps(result, sort_keys=True, separators=(",", ":")).encode()
        put_if_absent(prefix + "/closeout.json", body)
        if get(s3, prefix + "/closeout.json") != body:
            raise ValueError("V282 closeout readback differs")
        print(json.dumps({"closeout_sha256": sha(body), **result}, sort_keys=True), flush=True)
    finally:
        if instance_id:
            terminate_confirmed(ec2, instance_id)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--attempt", default="a0001")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    with open("/tmp/borsuk-v282-100k-launch.lock", "a+") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        launch(args.attempt, args.dry_run)
