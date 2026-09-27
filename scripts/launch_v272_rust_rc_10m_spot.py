#!/usr/bin/env python3
"""One authenticated frozen Rust RC CoHere 10M scale/cost gate on Spot."""

import argparse
import fcntl
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
from scripts.launch_v271_rust_rc_frontier_spot import get, replay, sha

SCHEMA = "borsuk-v272-rust-rc-10m-scale-v1"
TERMINAL_SCHEMA = "borsuk-v272-rust-rc-10m-scale-terminal-v1"
LIBRARY_COMMIT = "aa4182a2e6b78a254bca732663c7528818eb6561"
V271_PREFIX = (
    "research/v271-rust-rc-fresh-frontier/"
    "979f9a2db13a5c2406071add1864edeb043f0241/runs/a0001"
)
V271_CLOSEOUT_SHA = "d64015fc9501cc2146378a54b8e24866898cf2c6fcd446c37c42ce2b0a3147b0"
SOURCE_PREFIX = "publication/v3/20260812/datasets/cohere-large-10m-768/attempts/0001"
RECEIPT_SHA = "0965aa0241199822dfac3410bba4edad5536ac0eb0aaa8ab83c216e8c5749a87"
TEST_SHA = "5e0123f163df0e53a7e329fd92fbfd49f079756acfb47387ee6664c267b6f94e"


def bootstrap(commit, archive_sha, archive_key, prefix):
    script = f"""#!/bin/bash
set -euo pipefail
export BORSUK_V272_BUCKET='{BUCKET}'
export BORSUK_V272_PREFIX='{prefix}'
export BORSUK_V272_SOURCE_COMMIT='{commit}'
export BORSUK_V272_ARCHIVE_SHA='{archive_sha}'
export BORSUK_V272_SOURCE_PREFIX='{SOURCE_PREFIX}'
bootstrap_failed() {{
  code=$?
  trap - ERR
  set +e
  token=$(curl -fsS -X PUT -H 'X-aws-ec2-metadata-token-ttl-seconds: 60' http://169.254.169.254/latest/api/token)
  instance_id=$(curl -fsS -H "X-aws-ec2-metadata-token: $token" http://169.254.169.254/latest/meta-data/instance-id)
  INSTANCE_ID="$instance_id" EXIT_CODE="$code" python3 - <<'PY' >terminal.json
import json,os,time
print(json.dumps({{'schema':'{TERMINAL_SCHEMA}',
  'source_commit':os.environ['BORSUK_V272_SOURCE_COMMIT'],
  'source_archive_sha256':os.environ['BORSUK_V272_ARCHIVE_SHA'],
  'instance_id':os.environ['INSTANCE_ID'],'exit_code':int(os.environ['EXIT_CODE']),
  'finished_epoch':int(time.time()),'phase':'bootstrap','status':'failed',
  'artifacts':{{}}}},sort_keys=True,separators=(',',':')))
PY
  aws s3 cp terminal.json "s3://$BORSUK_V272_BUCKET/$BORSUK_V272_PREFIX/terminal.json" --only-show-errors
  shutdown -h now || true
  exit "$code"
}}
trap bootstrap_failed ERR
systemd-run --unit=v272-hard-stop --on-active=43200s /usr/sbin/shutdown -h now
root=/mnt/v272-rust-rc-10m-scale
mkdir -p "$root" && cd "$root"
aws s3 cp 's3://{BUCKET}/{archive_key}' source.tar.gz --only-show-errors
printf '%s  source.tar.gz\\n' '{archive_sha}' | sha256sum -c -
mkdir repo
tar -xzf source.tar.gz -C repo
exec bash repo/scripts/run_v272_10m_scale.sh
"""
    if len(script.encode()) > 16_384:
        raise ValueError("user data exceeds EC2 limit")
    return script


def sealed_json(s3, prefix, terminal, name):
    identity = terminal["artifacts"][name]
    body = get(s3, prefix + "/artifacts/" + name)
    if len(body) != identity["bytes"] or sha(body) != identity["sha256"]:
        raise ValueError(f"sealed {name} differs")
    return json.loads(body)


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
    prior = get(s3, V271_PREFIX + "/closeout.json")
    if sha(prior) != V271_CLOSEOUT_SHA:
        raise ValueError("V271 closeout differs")
    old = json.loads(prior)
    if old["decision"]["decision"] != "go_10m" or old["terminal_status"] != "complete":
        raise ValueError("V271 did not authorize scale gate")
    if sha(get(s3, SOURCE_PREFIX + "/STAGING_COMPLETE.json")) != RECEIPT_SHA:
        raise ValueError("source receipt differs")
    if sha(get(s3, SOURCE_PREFIX + "/materialized/test.parquet")) != TEST_SHA:
        raise ValueError("test object differs")
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
            "scripts/run_v272_10m_scale.sh",
            "scripts/v272_10m_scale.py",
            "crates/borsuk/examples/resident_graph_frontier.rs",
            "docs/research/v272-rust-rc-10m-scale-prereg.md",
        }
        if not required.issubset(source.getnames()):
            raise ValueError("archive lacks frozen V272 method")
    archive_sha = sha(archive)
    archive_key = (
        f"research/v272-rust-rc-10m-scale/{commit}/sources/{archive_sha}.tar.gz"
    )
    prefix = f"research/v272-rust-rc-10m-scale/{commit}/runs/{attempt}"
    if not missing(s3, prefix + "/reservation.json"):
        raise ValueError("attempt already reserved")
    if missing(s3, archive_key):
        put_if_absent(archive_key, archive)
    quote_row = ec2.describe_spot_price_history(
        InstanceTypes=["r7i.8xlarge"],
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
                "v271_closeout_sha256": V271_CLOSEOUT_SHA,
                "dataset": "CoHere-large-10M full10M D768 cosine",
                "split": "first1000 canonical test queries, prior-used; dev0-255/validation256-999",
                "truth": "FAISS IndexFlatIP FP32 unit-normalized full10M k100",
                "search": "Rust public API, one worker, 1000 sequential loaded queries after warmup",
                "quality_gate": "each split recall>=0.995 and p05 hits>=98; p95<=150ms p99<=180ms",
                "resource_gate": "search RSS<=32GiB; 2500*rows authenticated resident cap",
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
        request["InstanceType"] = "r7i.8xlarge"
        request["BlockDeviceMappings"][0]["Ebs"]["VolumeSize"] = 250
        request["TagSpecifications"][0]["Tags"][0]["Value"] = (
            "borsuk-v272-rust-rc-10m-scale"
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
        raw = read_marker(s3, ec2, prefix + "/terminal.json", instance_id, 43_800)
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
        root_sha = None
        if terminal["status"] == "complete":
            if terminal.get("phase") != "complete" or terminal.get("exit_code") != 0:
                raise ValueError("complete terminal phase differs")
            built = sealed_json(s3, prefix, terminal, "build.json")
            published = sealed_json(s3, prefix, terminal, "publish.json")
            searched = sealed_json(s3, prefix, terminal, "search.json")
            decision = sealed_json(s3, prefix, terminal, "decision.json")
            root_sha = built["root_sha256"]
            if (
                decision.get("decision") not in {"go_product_gate", "no_go_10m"}
                or built["rows"] != 10_000_000
                or searched["rows"] != 10_000_000
                or root_sha != published["root_sha256"]
                or root_sha != searched["root_sha256"]
                or terminal["artifacts"]["generation/root.json"]["sha256"] != root_sha
            ):
                raise ValueError("generation or decision differs")
            head = json.loads(get(s3, prefix + "/generation/head.json"))
            root = get(s3, prefix + f"/generation/roots%2F{root_sha}.json")
            if head["root_sha256"] != root_sha or sha(root) != root_sha:
                raise ValueError("published root readback differs")
        result = {
            "schema": SCHEMA + "-closeout",
            "source_commit": commit,
            "library_commit": LIBRARY_COMMIT,
            "source_archive_sha256": archive_sha,
            "instance_id": instance_id,
            "terminal_sha256": sha(raw),
            "terminal_status": terminal["status"],
            "phase": terminal.get("phase"),
            "root_sha256": root_sha,
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
    with open("/tmp/borsuk-v272-10m-launch.lock", "a+") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        launch(args.attempt)
