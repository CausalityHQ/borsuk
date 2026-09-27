#!/usr/bin/env python3
"""Launch one authorized, terminal-sealed V283 CoHere dev64 Spot cell."""

import argparse
import fcntl
import io
import json
import subprocess
import tarfile
from pathlib import Path

import boto3

from scripts.launch_v157_primary_feasibility_spot import (
    BUCKET, REGION, archive_source, missing, put_if_absent,
)
from scripts.launch_v223_authenticated_graph_http_spot import (
    read_marker, spot_request, terminate_confirmed,
)
from scripts.launch_v271_rust_rc_frontier_spot import get, replay, sha

SCHEMA = "borsuk-v283-dev64-v1"
TERMINAL_SCHEMA = "borsuk-v283-dev64-terminal-v1"
LAYOUT_SHA = "303f31ab8a182a0aaa304c4ef551a046be41071ac24e67a793882eb74c5b532e"
SQ8_SHA = "301696df05ca03122951b66ad8a9bedb5d5f1e675c6fc66f6019abbce3fcda58"
SCRATCH = Path(".borsuk-scratch/v283/cohere-layout")


def bootstrap(commit, archive_sha, archive_key, prefix, layout_key, sq8_key):
    script = f"""#!/bin/bash
set -euo pipefail
export BORSUK_V283_BUCKET='{BUCKET}'
export BORSUK_V283_PREFIX='{prefix}'
export BORSUK_V283_COMMIT='{commit}'
export BORSUK_V283_ARCHIVE_SHA='{archive_sha}'
export BORSUK_V283_LAYOUT_KEY='{layout_key}'
export BORSUK_V283_SQ8_KEY='{sq8_key}'
bootstrap_failed() {{
  code=$?
  trap - ERR
  set +e
  token=$(curl -fsS -X PUT -H 'X-aws-ec2-metadata-token-ttl-seconds: 60' http://169.254.169.254/latest/api/token)
  instance_id=$(curl -fsS -H "X-aws-ec2-metadata-token: $token" http://169.254.169.254/latest/meta-data/instance-id)
  INSTANCE_ID="$instance_id" EXIT_CODE="$code" python3 - <<'PY' >terminal.json
import json,os,time
print(json.dumps({{'schema':'{TERMINAL_SCHEMA}',
 'source_commit':os.environ['BORSUK_V283_COMMIT'],
 'source_archive_sha256':os.environ['BORSUK_V283_ARCHIVE_SHA'],
 'layout_sha256':'{LAYOUT_SHA}','sq8_sha256':'{SQ8_SHA}',
 'instance_id':os.environ['INSTANCE_ID'],'exit_code':int(os.environ['EXIT_CODE']),
 'finished_epoch':int(time.time()),'phase':'bootstrap','status':'failed',
 'artifacts':{{}}}},sort_keys=True,separators=(',',':')))
PY
  aws s3 cp terminal.json "s3://$BORSUK_V283_BUCKET/$BORSUK_V283_PREFIX/terminal.json" --only-show-errors
  shutdown -h now || true
  exit "$code"
}}
trap bootstrap_failed ERR
systemd-run --unit=v283-hard-stop --on-active=2100s /usr/sbin/shutdown -h now
root=/mnt/v283-dev64
mkdir -p "$root" && cd "$root"
aws s3 cp 's3://{BUCKET}/{archive_key}' source.tar.gz --only-show-errors
printf '%s  source.tar.gz\\n' '{archive_sha}' | sha256sum -c -
mkdir repo && tar -xzf source.tar.gz -C repo
exec timeout --signal=TERM --kill-after=30s 1800s bash repo/scripts/run_v283_dev64.sh
"""
    if len(script.encode()) > 16_384:
        raise ValueError("V283 user data exceeds EC2 limit")
    return script


def launch(attempt, *, approved=False, dry_run=False):
    if len(attempt) != 5 or not attempt.startswith("a") or not attempt[1:].isdigit():
        raise ValueError("attempt must be aNNNN")
    if not approved and not dry_run:
        raise ValueError("operator approval is required for paid V283 compute")
    if subprocess.check_output(["git", "status", "--porcelain"], text=True).strip():
        raise ValueError("worktree must be clean")
    commit = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    subprocess.run(["git", "fetch", "origin", "main"], check=True)
    if subprocess.run(["git", "merge-base", "--is-ancestor", "origin/main", "HEAD"],
                      check=False).returncode:
        raise ValueError("source is not a fast-forward descendant of origin/main")
    layout = (SCRATCH / "layout.npy").read_bytes()
    sq8 = (SCRATCH / "sq8.bin").read_bytes()
    if sha(layout) != LAYOUT_SHA or sha(sq8) != SQ8_SHA or len(sq8) != 78_000_000:
        raise ValueError("frozen V283 physical layout differs")
    session = boto3.Session(profile_name="causality", region_name=REGION)
    s3, ec2 = session.client("s3"), session.client("ec2")
    active = ec2.describe_instances(Filters=[
        {"Name": "tag:Name", "Values": ["borsuk-*"]},
        {"Name": "instance-state-name", "Values": ["pending", "running", "stopping"]},
    ])
    if any(reservation.get("Instances") for reservation in active["Reservations"]):
        raise ValueError("another BORSUK worker is active")
    archive = archive_source(commit)
    with tarfile.open(fileobj=io.BytesIO(archive), mode="r:gz") as source:
        required = {"scripts/run_v283_dev64.sh", "scripts/v283_dev64_summary.py",
                    "crates/borsuk/src/bin/v282_local_falsifier.rs",
                    "docs/research/v283-semantic-cell-oracle-prereg.md"}
        if not required.issubset(source.getnames()):
            raise ValueError("V283 source archive incomplete")
    archive_sha = sha(archive)
    base = f"research/v283-cohere-dev64/{commit}"
    archive_key = f"{base}/sources/{archive_sha}.tar.gz"
    layout_key, sq8_key = f"{base}/inputs/{LAYOUT_SHA}.npy", f"{base}/inputs/{SQ8_SHA}.bin"
    prefix = f"{base}/runs/{attempt}"
    if not missing(s3, prefix + "/reservation.json"):
        raise ValueError("attempt already reserved")
    quote_row = ec2.describe_spot_price_history(
        InstanceTypes=["c7i.8xlarge"], ProductDescriptions=["Linux/UNIX"],
        AvailabilityZone="eu-central-1c", MaxResults=1)["SpotPriceHistory"][0]
    quote = float(quote_row["SpotPrice"])
    if quote * 0.5 > 0.40:
        raise ValueError("V283 compute cap exceeds $0.40")
    if dry_run:
        print(json.dumps({"source_commit": commit, "archive_sha256": archive_sha,
                          "layout_sha256": LAYOUT_SHA, "sq8_sha256": SQ8_SHA,
                          "prefix": prefix, "active_borsuk_instances": 0,
                          "spot_quote_usd_per_hour": quote,
                          "compute_cap_usd_30min_estimate": quote * 0.5}, sort_keys=True))
        return
    for key, body in ((archive_key, archive), (layout_key, layout), (sq8_key, sq8)):
        if missing(s3, key):
            put_if_absent(key, body)
        if sha(get(s3, key)) != sha(body):
            raise ValueError("staged V283 input differs")
    put_if_absent(prefix + "/reservation.json", json.dumps({
        "schema": SCHEMA + "-reservation", "source_commit": commit,
        "source_archive_sha256": archive_sha, "layout_sha256": LAYOUT_SHA,
        "sq8_sha256": SQ8_SHA, "dataset": "CoHere first100k D768 cosine k100",
        "split": "prior-used development0-63", "method": "unchanged V282 PQ and SQ8 replay on V283 pages",
        "purchase_option": "spot", "spot_quote_usd_per_hour": quote,
        "wall_cap_seconds": 1800, "compute_cap_usd_estimate": quote * 0.5,
        "interruption": "discard interrupted cell; no automatic restart",
    }, sort_keys=True).encode())
    instance_id = None
    try:
        request = spot_request("server", prefix,
                               bootstrap(commit, archive_sha, archive_key, prefix,
                                         layout_key, sq8_key))
        request["InstanceType"] = "c7i.8xlarge"
        request["BlockDeviceMappings"][0]["Ebs"]["VolumeSize"] = 150
        request["TagSpecifications"][0]["Tags"][0]["Value"] = "borsuk-v283-cohere-dev64"
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
        raw = read_marker(s3, ec2, prefix + "/terminal.json", instance_id, 2100)
        terminate_confirmed(ec2, instance_id)
        terminal = json.loads(raw)
        if (terminal.get("schema") != TERMINAL_SCHEMA
                or terminal.get("source_commit") != commit
                or terminal.get("source_archive_sha256") != archive_sha
                or terminal.get("layout_sha256") != LAYOUT_SHA
                or terminal.get("sq8_sha256") != SQ8_SHA
                or terminal.get("instance_id") != instance_id
                or terminal.get("status") not in {"complete", "failed"}):
            raise ValueError("V283 terminal identity differs")
        replay(s3, prefix, terminal)
        decision = json.loads(get(s3, prefix + "/artifacts/summary.json")) if terminal["status"] == "complete" else None
        if decision is not None and (decision["schema"] != "borsuk-v283-dev64-summary-v1"
                                     or decision["queries"] != 64):
            raise ValueError("V283 decision differs")
        result = {"schema": SCHEMA + "-closeout", "source_commit": commit,
                  "source_archive_sha256": archive_sha, "instance_id": instance_id,
                  "terminal_sha256": sha(raw), "terminal_status": terminal["status"],
                  "phase": terminal.get("phase"), "decision": decision,
                  "spot_quote_usd_per_hour": quote,
                  "spot_compute_usd_through_terminal_estimate":
                  max(0, terminal["finished_epoch"] - launch_epoch) * quote / 3600}
        body = json.dumps(result, sort_keys=True, separators=(",", ":")).encode()
        put_if_absent(prefix + "/closeout.json", body)
        if get(s3, prefix + "/closeout.json") != body:
            raise ValueError("V283 closeout readback differs")
        print(json.dumps({"closeout_sha256": sha(body), **result}, sort_keys=True), flush=True)
    finally:
        if instance_id:
            terminate_confirmed(ec2, instance_id)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--attempt", default="a0001")
    parser.add_argument("--approved", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    with open("/tmp/borsuk-v283-dev64-launch.lock", "a+") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        launch(args.attempt, approved=args.approved, dry_run=args.dry_run)
