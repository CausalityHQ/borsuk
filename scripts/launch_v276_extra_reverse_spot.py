#!/usr/bin/env python3
"""One preregistered paired 100k construction-quality cell on Spot."""

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

SCHEMA = "borsuk-v276-extra-reverse-v1"
TERMINAL_SCHEMA = "borsuk-v276-extra-reverse-terminal-v1"
V261_PREFIX = ("research/v261-cohere-dual-graph-1m/"
               "2dee58896e42f84d74e2653dc0960e19d6defc63/runs/a0001")
V261_TERMINAL_SHA = "00c7d4803354f15d5f62bea6e53fd0672a0f5fc91cc482e1fa4b20b8951a9f82"
V271_PREFIX = ("research/v271-rust-rc-fresh-frontier/"
               "979f9a2db13a5c2406071add1864edeb043f0241/runs/a0001")
V271_TERMINAL_SHA = "13775dc6c5176c636fc8442b4bd2bddd2ee375818a98d1328d2c0352423bc80b"
ROOT_SHA = "440beefd321dfeeae25ba6277a2e2f1f938c389a0c4a9007177f4bacc4b1d6e3"


def bootstrap(commit, archive_sha, archive_key, prefix):
    script = f"""#!/bin/bash
set -euo pipefail
export BORSUK_V276_BUCKET='{BUCKET}'
export BORSUK_V276_PREFIX='{prefix}'
export BORSUK_V276_SOURCE_COMMIT='{commit}'
export BORSUK_V276_ARCHIVE_SHA='{archive_sha}'
export BORSUK_V276_V261_PREFIX='{V261_PREFIX}'
export BORSUK_V276_V271_PREFIX='{V271_PREFIX}'
bootstrap_failed() {{
  code=$?
  trap - ERR
  set +e
  token=$(curl -fsS -X PUT -H 'X-aws-ec2-metadata-token-ttl-seconds: 60' http://169.254.169.254/latest/api/token)
  instance_id=$(curl -fsS -H "X-aws-ec2-metadata-token: $token" http://169.254.169.254/latest/meta-data/instance-id)
  INSTANCE_ID="$instance_id" EXIT_CODE="$code" python3 - <<'PY' >terminal.json
import json,os,time
print(json.dumps({{'schema':'{TERMINAL_SCHEMA}',
  'source_commit':os.environ['BORSUK_V276_SOURCE_COMMIT'],
  'source_archive_sha256':os.environ['BORSUK_V276_ARCHIVE_SHA'],
  'instance_id':os.environ['INSTANCE_ID'],'exit_code':int(os.environ['EXIT_CODE']),
  'finished_epoch':int(time.time()),'phase':'bootstrap','status':'failed',
  'artifacts':{{}}}},sort_keys=True,separators=(',',':')))
PY
  aws s3 cp terminal.json "s3://$BORSUK_V276_BUCKET/$BORSUK_V276_PREFIX/terminal.json" --only-show-errors
  shutdown -h now || true
  exit "$code"
}}
trap bootstrap_failed ERR
systemd-run --unit=v276-hard-stop --on-active=7200s /usr/sbin/shutdown -h now
root=/mnt/v276-extra-reverse
mkdir -p "$root" && cd "$root"
aws s3 cp 's3://{BUCKET}/{archive_key}' source.tar.gz --only-show-errors
printf '%s  source.tar.gz\\n' '{archive_sha}' | sha256sum -c -
mkdir repo && tar -xzf source.tar.gz -C repo
exec bash repo/scripts/run_v276_extra_reverse.sh
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
    if subprocess.run(["git", "merge-base", "--is-ancestor", "origin/main", "HEAD"],
                      check=False).returncode:
        raise ValueError("source is not a fast-forward descendant of origin/main")
    session = boto3.Session(profile_name="causality", region_name=REGION)
    s3, ec2 = session.client("s3"), session.client("ec2")
    v261 = get(s3, V261_PREFIX + "/terminal.json")
    v271 = get(s3, V271_PREFIX + "/terminal.json")
    old = json.loads(v271)
    if (sha(v261) != V261_TERMINAL_SHA or sha(v271) != V271_TERMINAL_SHA
            or json.loads(v261)["artifacts"]["vectors.raw"]["sha256"]
            != "6c82a340e3e1b4226640e593efa9c4000c6a5962d4b13063093a1dab689a9005"
            or old["status"] != "complete"
            or old["artifacts"]["new100/root.json"]["sha256"] != ROOT_SHA):
        raise ValueError("historical source or baseline receipt differs")
    active = ec2.describe_instances(Filters=[
        {"Name": "tag:Name", "Values": ["borsuk-*"]},
        {"Name": "instance-state-name", "Values": ["pending", "running", "stopping"]},
    ])
    if any(row.get("Instances") for row in active["Reservations"]):
        raise ValueError("another BORSUK worker is active")
    archive = archive_source(commit)
    with tarfile.open(fileobj=io.BytesIO(archive), mode="r:gz") as source:
        required = {"scripts/run_v276_extra_reverse.sh",
                    "scripts/v276_extra_reverse.py",
                    "docs/research/v276-extra-reverse-100k-prereg.md"}
        if not required.issubset(source.getnames()):
            raise ValueError("archive lacks frozen V276 method")
    archive_sha = sha(archive)
    archive_key = f"research/v276-extra-reverse/{commit}/sources/{archive_sha}.tar.gz"
    prefix = f"research/v276-extra-reverse/{commit}/runs/{attempt}"
    if not missing(s3, prefix + "/reservation.json"):
        raise ValueError("attempt already reserved")
    if missing(s3, archive_key):
        put_if_absent(archive_key, archive)
    quote_row = ec2.describe_spot_price_history(
        InstanceTypes=["c7i.4xlarge"], ProductDescriptions=["Linux/UNIX"],
        AvailabilityZone="eu-central-1c", MaxResults=1,
    )["SpotPriceHistory"][0]
    quote = float(quote_row["SpotPrice"])
    put_if_absent(prefix + "/reservation.json", json.dumps({
        "schema": SCHEMA + "-reservation", "source_commit": commit,
        "source_archive_sha256": archive_sha,
        "v261_terminal_sha256": V261_TERMINAL_SHA,
        "v271_terminal_sha256": V271_TERMINAL_SHA,
        "dataset": "CoHere-large-10M first100k D768 cosine",
        "split": "excluded train rows103000-103999", "queries": 1000, "k": 100,
        "baseline_root_sha256": ROOT_SHA,
        "diagnostic_widths": {"pq_ef": 256, "shortlist": 256, "exact_ef": 128},
        "query_sha256": "81d455e2b251f6244e5e6092b0880fc7014e5cb88c93c29d8f57685f2d51056b",
        "candidate": "M32 M0=64 efConstruction128 plus bounded extra reverse-edge append cap16 per source",
        "cache": "authenticated local resident, no query-time GETs",
        "transport": "Rust in-process, sequential, one worker",
        "spot_quote_usd_per_hour": quote,
        "spot_quote_timestamp": quote_row["Timestamp"].isoformat(),
        "interruption_policy": "discard interrupted measurement cell",
    }, sort_keys=True).encode())
    instance_id = None
    try:
        request = spot_request("server", prefix, bootstrap(commit, archive_sha, archive_key, prefix))
        request["BlockDeviceMappings"][0]["Ebs"]["VolumeSize"] = 80
        request["TagSpecifications"][0]["Tags"][0]["Value"] = "borsuk-v276-extra-reverse"
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
        raw = read_marker(s3, ec2, prefix + "/terminal.json", instance_id, 7500)
        terminate_confirmed(ec2, instance_id)
        terminal = json.loads(raw)
        if (terminal.get("schema") != TERMINAL_SCHEMA
                or terminal.get("source_commit") != commit
                or terminal.get("source_archive_sha256") != archive_sha
                or terminal.get("instance_id") != instance_id
                or terminal.get("status") not in {"complete", "failed"}):
            raise ValueError("terminal identity differs")
        replay(s3, prefix, terminal)
        decision = None
        if terminal["status"] == "complete":
            if terminal.get("phase") != "complete" or terminal.get("exit_code") != 0:
                raise ValueError("complete terminal phase differs")
            identity = terminal["artifacts"]["decision.json"]
            body = get(s3, prefix + "/artifacts/decision.json")
            if len(body) != identity["bytes"] or sha(body) != identity["sha256"]:
                raise ValueError("decision identity differs")
            decision = json.loads(body)
            if decision.get("decision") not in {"go_1m", "reject_candidate", "inconclusive"}:
                raise ValueError("decision code differs")
        result = {"schema": SCHEMA + "-closeout", "source_commit": commit,
                  "source_archive_sha256": archive_sha, "instance_id": instance_id,
                  "terminal_sha256": sha(raw), "terminal_status": terminal["status"],
                  "phase": terminal.get("phase"), "decision": decision,
                  "spot_quote_usd_per_hour": quote,
                  "spot_compute_usd_through_terminal": max(0, terminal["finished_epoch"]
                                                            - launch_epoch) * quote / 3600}
        body = json.dumps(result, sort_keys=True, separators=(",", ":")).encode()
        put_if_absent(prefix + "/closeout.json", body)
        if get(s3, prefix + "/closeout.json") != body:
            raise ValueError("closeout readback differs")
        print(json.dumps({"closeout_sha256": sha(body), **result}, sort_keys=True), flush=True)
    finally:
        if instance_id:
            terminate_confirmed(ec2, instance_id)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--attempt", default="a0001")
    args = parser.parse_args()
    with open("/tmp/borsuk-v276-extra-reverse-launch.lock", "a+") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        launch(args.attempt)
