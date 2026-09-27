#!/usr/bin/env python3
"""One paired ReLAION-1M transfer gate on Spot."""

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

SCHEMA = "borsuk-v279-relaion-transfer-1m-v1"
TERMINAL_SCHEMA = "borsuk-v279-relaion-transfer-1m-terminal-v1"
V278_PREFIX = ("research/v278-extra-reverse-1m/"
               "42c48d5e8e6696d9c991faec0d9b43f00dc1b61e/runs/a0001")
V278_TERMINAL_SHA = "cd5ddfdbc321478361e428ef702e573a14532e7c58cbe265649a8d38de76008e"
SOURCE_KEY = ("research/v36-prefix-screen/runs/"
              "v36-prefix-screen-20260908T174540Z-31445a91/attempt-0000/source.parquet")
REQUESTS_KEY = ("research/v116-validation-paired/"
                "5e9b35ad40ea023eab4407aa611d759e1893bb34/"
                "runs/v116-validation-20260923T235426Z/a0001/artifacts/requests.jsonl")


def bootstrap(commit, archive_sha, archive_key, prefix):
    script = f"""#!/bin/bash
set -euo pipefail
export BORSUK_V279_BUCKET='{BUCKET}'
export BORSUK_V279_PREFIX='{prefix}'
export BORSUK_V279_SOURCE_COMMIT='{commit}'
export BORSUK_V279_ARCHIVE_SHA='{archive_sha}'
export BORSUK_V279_SOURCE_KEY='{SOURCE_KEY}'
export BORSUK_V279_REQUESTS_KEY='{REQUESTS_KEY}'
bootstrap_failed() {{
  code=$?
  trap - ERR
  set +e
  token=$(curl -fsS -X PUT -H 'X-aws-ec2-metadata-token-ttl-seconds: 60' http://169.254.169.254/latest/api/token)
  instance_id=$(curl -fsS -H "X-aws-ec2-metadata-token: $token" http://169.254.169.254/latest/meta-data/instance-id)
  INSTANCE_ID="$instance_id" EXIT_CODE="$code" python3 - <<'PY' >terminal.json
import json,os,time
print(json.dumps({{'schema':'{TERMINAL_SCHEMA}',
  'source_commit':os.environ['BORSUK_V279_SOURCE_COMMIT'],
  'source_archive_sha256':os.environ['BORSUK_V279_ARCHIVE_SHA'],
  'instance_id':os.environ['INSTANCE_ID'],'exit_code':int(os.environ['EXIT_CODE']),
  'finished_epoch':int(time.time()),'phase':'bootstrap','status':'failed',
  'artifacts':{{}}}},sort_keys=True,separators=(',',':')))
PY
  aws s3 cp terminal.json "s3://$BORSUK_V279_BUCKET/$BORSUK_V279_PREFIX/terminal.json" --only-show-errors
  shutdown -h now || true
  exit "$code"
}}
trap bootstrap_failed ERR
systemd-run --unit=v279-hard-stop --on-active=10800s /usr/sbin/shutdown -h now
root=/mnt/v279-relaion-transfer-1m
mkdir -p "$root" && cd "$root"
aws s3 cp 's3://{BUCKET}/{archive_key}' source.tar.gz --only-show-errors
printf '%s  source.tar.gz\\n' '{archive_sha}' | sha256sum -c -
mkdir repo && tar -xzf source.tar.gz -C repo
exec bash repo/scripts/run_v279_relaion_transfer_1m.sh
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
    v278 = get(s3, V278_PREFIX + "/terminal.json")
    prior = json.loads(v278)
    prior_decision = get(s3, V278_PREFIX + "/artifacts/decision.json")
    if (sha(v278) != V278_TERMINAL_SHA
            or prior["status"] != "complete"
            or sha(prior_decision) != prior["artifacts"]["decision.json"]["sha256"]
            or json.loads(prior_decision)["decision"] != "go_cross_dataset_1m"
            or s3.head_object(Bucket=BUCKET, Key=SOURCE_KEY)["ContentLength"] != 1_458_450_077
            or s3.head_object(Bucket=BUCKET, Key=REQUESTS_KEY)["ContentLength"] != 18_726_909):
        raise ValueError("transfer source or qualifying receipt differs")
    active = ec2.describe_instances(Filters=[
        {"Name": "tag:Name", "Values": ["borsuk-*"]},
        {"Name": "instance-state-name", "Values": ["pending", "running", "stopping"]},
    ])
    if any(row.get("Instances") for row in active["Reservations"]):
        raise ValueError("another BORSUK worker is active")
    archive = archive_source(commit)
    with tarfile.open(fileobj=io.BytesIO(archive), mode="r:gz") as source:
        required = {"scripts/run_v279_relaion_transfer_1m.sh",
                    "scripts/v279_relaion_transfer_1m.py",
                    "docs/research/v279-relaion-transfer-1m-prereg.md"}
        if not required.issubset(source.getnames()):
            raise ValueError("archive lacks frozen V279 method")
    archive_sha = sha(archive)
    archive_key = f"research/v279-relaion-transfer-1m/{commit}/sources/{archive_sha}.tar.gz"
    prefix = f"research/v279-relaion-transfer-1m/{commit}/runs/{attempt}"
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
        "v278_terminal_sha256": V278_TERMINAL_SHA,
        "source_key": SOURCE_KEY, "requests_key": REQUESTS_KEY,
        "source_sha256": "2796b579f37afe99ca4aff57e282335a6a79ad30596645957d26326a0560cf86",
        "requests_sha256": "c250ef3c871af55ee1ea91e61214a93903b3d575ef458926b1f8c5ad0547b2c9",
        "dataset": "ReLAION-1M D768 cosine",
        "split": "validation ordinals0-999 historically used", "queries": 1000, "k": 100,
        "diagnostic_widths": {"pq_ef": 256, "shortlist": 256, "exact_ef": 128},
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
        request["TagSpecifications"][0]["Tags"][0]["Value"] = "borsuk-v279-relaion-transfer-1m"
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
        raw = read_marker(s3, ec2, prefix + "/terminal.json", instance_id, 11100)
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
            if decision.get("decision") not in {"go_http_1m", "reject_candidate", "no_material_gain"}:
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
    with open("/tmp/borsuk-v279-relaion-transfer-1m-launch.lock", "a+") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        launch(args.attempt)
