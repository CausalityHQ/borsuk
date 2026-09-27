#!/usr/bin/env python3
"""Run one frozen CoHere10M reverse-edge transfer cell on Causality Spot."""

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
from scripts.launch_v272_rust_rc_10m_spot import sealed_json

SCHEMA = "borsuk-v281-extra-reverse-10m-v1"
TERMINAL_SCHEMA = "borsuk-v281-extra-reverse-10m-terminal-v1"
SOURCE_PREFIX = "publication/v3/20260812/datasets/cohere-large-10m-768/attempts/0001"
V272_PREFIX = (
    "research/v272-rust-rc-10m-scale/"
    "eb16c09004a4d06852600b5f151e44b105ed22b6/runs/a0001"
)
V272_TERMINAL_SHA = "e75a7b399920b277683406ee370a67204f3e9f01fd215e809c0f606356958352"
V272_ROOT_SHA = "948e8a5555f44011b22681ca2cd20edde93f6fec5e6261e20e3a7b799d467022"
V278_PREFIX = (
    "research/v278-extra-reverse-1m/42c48d5e8e6696d9c991faec0d9b43f00dc1b61e/runs/a0001"
)
V278_TERMINAL_SHA = "cd5ddfdbc321478361e428ef702e573a14532e7c58cbe265649a8d38de76008e"
ON_DEMAND_SKU = "HJJTV8GSDPQXFMCX"
ON_DEMAND_PRICE = 2.5536


def bootstrap(commit, archive_sha, archive_key, prefix):
    script = f"""#!/bin/bash
set -euo pipefail
export BORSUK_V281_BUCKET='{BUCKET}'
export BORSUK_V281_PREFIX='{prefix}'
export BORSUK_V281_SOURCE_COMMIT='{commit}'
export BORSUK_V281_ARCHIVE_SHA='{archive_sha}'
export BORSUK_V281_SOURCE_PREFIX='{SOURCE_PREFIX}'
export BORSUK_V281_V272_PREFIX='{V272_PREFIX}'
bootstrap_failed() {{
  code=$?
  trap - ERR
  set +e
  token=$(curl -fsS -X PUT -H 'X-aws-ec2-metadata-token-ttl-seconds: 60' http://169.254.169.254/latest/api/token)
  instance_id=$(curl -fsS -H "X-aws-ec2-metadata-token: $token" http://169.254.169.254/latest/meta-data/instance-id)
  INSTANCE_ID="$instance_id" EXIT_CODE="$code" python3 - <<'PY' >terminal.json
import json,os,time
print(json.dumps({{'schema':'{TERMINAL_SCHEMA}',
  'source_commit':os.environ['BORSUK_V281_SOURCE_COMMIT'],
  'source_archive_sha256':os.environ['BORSUK_V281_ARCHIVE_SHA'],
  'instance_id':os.environ['INSTANCE_ID'],'exit_code':int(os.environ['EXIT_CODE']),
  'finished_epoch':int(time.time()),'phase':'bootstrap','status':'failed',
  'artifacts':{{}}}},sort_keys=True,separators=(',',':')))
PY
  aws s3 cp terminal.json "s3://$BORSUK_V281_BUCKET/$BORSUK_V281_PREFIX/terminal.json" --only-show-errors
  shutdown -h now || true
  exit "$code"
}}
trap bootstrap_failed ERR
systemd-run --unit=v281-hard-stop --on-active=43200s /usr/sbin/shutdown -h now
root=/mnt/v281-extra-reverse-10m
mkdir -p "$root" && cd "$root"
aws s3 cp 's3://{BUCKET}/{archive_key}' source.tar.gz --only-show-errors
printf '%s  source.tar.gz\\n' '{archive_sha}' | sha256sum -c -
mkdir repo && tar -xzf source.tar.gz -C repo
exec bash repo/scripts/run_v281_extra_reverse_10m.sh
"""
    if len(script.encode()) > 16_384:
        raise ValueError("V281 user data exceeds EC2 limit")
    return script


def launch(attempt, dry_run=False, on_demand=False):
    if len(attempt) != 5 or not attempt.startswith("a") or not attempt[1:].isdigit():
        raise ValueError("attempt must be aNNNN")
    if on_demand and attempt != "a0002":
        raise ValueError("documented On-Demand exception applies only to a0002")
    if subprocess.check_output(["git", "status", "--porcelain"], text=True).strip():
        raise ValueError("worktree must be clean")
    commit = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    subprocess.run(["git", "fetch", "origin", "main"], check=True)
    if subprocess.run(
        ["git", "merge-base", "--is-ancestor", "origin/main", "HEAD"], check=False
    ).returncode:
        raise ValueError("source is not a fast-forward descendant of origin/main")
    session = boto3.Session(profile_name="causality", region_name=REGION)
    s3, ec2 = session.client("s3"), session.client("ec2")
    old_raw = get(s3, V272_PREFIX + "/terminal.json")
    transfer_raw = get(s3, V278_PREFIX + "/terminal.json")
    if (
        sha(old_raw) != V272_TERMINAL_SHA
        or json.loads(old_raw)["status"] != "complete"
        or sha(transfer_raw) != V278_TERMINAL_SHA
        or json.loads(transfer_raw)["status"] != "complete"
        or sha(get(s3, SOURCE_PREFIX + "/STAGING_COMPLETE.json"))
        != "0965aa0241199822dfac3410bba4edad5536ac0eb0aaa8ab83c216e8c5749a87"
        or sha(get(s3, SOURCE_PREFIX + "/materialized/test.parquet"))
        != "5e0123f163df0e53a7e329fd92fbfd49f079756acfb47387ee6664c267b6f94e"
    ):
        raise ValueError("V272/V278 or source identity differs")
    old_terminal = json.loads(old_raw)
    if (
        old_terminal["artifacts"]["generation/root.json"]["sha256"] != V272_ROOT_SHA
        or old_terminal["artifacts"]["truth.u32"]["sha256"]
        != "9d08b49fef274d5bee2572ed8ed186ff8f2063b759f556748fe83b6e1d21c4f1"
    ):
        raise ValueError("V272 comparator differs")
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
            "scripts/run_v281_extra_reverse_10m.sh",
            "scripts/v281_10m.py",
            "scripts/v272_10m_scale.py",
            "crates/borsuk/examples/resident_graph_frontier.rs",
            "docs/research/v281-extra-reverse-cohere-10m-prereg.md",
        }
        if not required.issubset(source.getnames()):
            raise ValueError("V281 source archive incomplete")
    archive_sha = sha(archive)
    archive_key = (
        f"research/v281-extra-reverse-10m/{commit}/sources/{archive_sha}.tar.gz"
    )
    prefix = f"research/v281-extra-reverse-10m/{commit}/runs/{attempt}"
    if not missing(s3, prefix + "/reservation.json"):
        raise ValueError("attempt already reserved")
    if on_demand:
        pricing = session.client("pricing", region_name="us-east-1")
        filters = [
            {"Type": "TERM_MATCH", "Field": key, "Value": value}
            for key, value in (
                ("instanceType", "r7i.8xlarge"),
                ("location", "EU (Frankfurt)"),
                ("operatingSystem", "Linux"),
                ("tenancy", "Shared"),
                ("preInstalledSw", "NA"),
                ("capacitystatus", "Used"),
            )
        ]
        offers = pricing.get_products(
            ServiceCode="AmazonEC2", Filters=filters, MaxResults=20
        )["PriceList"]
        prices = [
            (product["product"]["sku"], float(dimension["pricePerUnit"]["USD"]))
            for raw in offers
            for product in [json.loads(raw)]
            for term in product.get("terms", {}).get("OnDemand", {}).values()
            for dimension in term["priceDimensions"].values()
            if dimension["unit"] == "Hrs"
        ]
        if prices != [(ON_DEMAND_SKU, ON_DEMAND_PRICE)]:
            raise ValueError(f"On-Demand price identity differs: {prices}")
        quote, purchase = ON_DEMAND_PRICE, "on-demand"
    else:
        quote_row = ec2.describe_spot_price_history(
            InstanceTypes=["r7i.8xlarge"],
            ProductDescriptions=["Linux/UNIX"],
            AvailabilityZone="eu-central-1c",
            MaxResults=1,
        )["SpotPriceHistory"][0]
        quote, purchase = float(quote_row["SpotPrice"]), "spot"
    if dry_run:
        print(
            json.dumps(
                {
                    "source_commit": commit,
                    "archive_sha256": archive_sha,
                    "v272_terminal_sha256": V272_TERMINAL_SHA,
                    "v278_terminal_sha256": V278_TERMINAL_SHA,
                    "prefix": prefix,
                    "active_borsuk_instances": 0,
                    "purchase_option": purchase,
                    "compute_quote_usd_per_hour": quote,
                },
                sort_keys=True,
            )
        )
        return
    if missing(s3, archive_key):
        put_if_absent(archive_key, archive)
    put_if_absent(
        prefix + "/reservation.json",
        json.dumps(
            {
                "schema": SCHEMA + "-reservation",
                "source_commit": commit,
                "source_archive_sha256": archive_sha,
                "v272_terminal_sha256": V272_TERMINAL_SHA,
                "v278_terminal_sha256": V278_TERMINAL_SHA,
                "baseline_root_sha256": V272_ROOT_SHA,
                "dataset": "CoHere-large-10M full10M D768 cosine k100",
                "split": "prior-used canonical test dev0-255/validation256-999",
                "method": "one bounded extra reverse-edge build; same-host baseline/candidate search",
                "purchase_option": purchase,
                "compute_quote_usd_per_hour": quote,
                "price_sku": ON_DEMAND_SKU if on_demand else None,
                "quote_timestamp": None
                if on_demand
                else quote_row["Timestamp"].isoformat(),
                "interruption": "discard full cell, new attempt only",
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
            "borsuk-v281-extra-reverse-10m"
        )
        if on_demand:
            request.pop("InstanceMarketOptions")
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
                    "purchase_option": purchase,
                    "compute_quote_usd_per_hour": quote,
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
            raise ValueError("V281 terminal identity differs")
        replay(s3, prefix, terminal)
        decision = root_sha = None
        if terminal["status"] == "complete":
            if terminal.get("phase") != "complete" or terminal.get("exit_code") != 0:
                raise ValueError("complete terminal phase differs")
            built = sealed_json(s3, prefix, terminal, "candidate.build.json")
            published = sealed_json(s3, prefix, terminal, "publish.json")
            baseline = sealed_json(s3, prefix, terminal, "baseline.search.json")
            searched = sealed_json(s3, prefix, terminal, "candidate.search.json")
            decision = sealed_json(s3, prefix, terminal, "decision.json")
            root_sha = built["root_sha256"]
            if (
                decision["decision"] not in {"go_http_10m", "no_go_10m", "invalid_cell"}
                or built["rows"] != 10_000_000
                or baseline["rows"] != 10_000_000
                or searched["rows"] != 10_000_000
                or baseline["root_sha256"] != V272_ROOT_SHA
                or root_sha != published["root_sha256"]
                or root_sha != searched["root_sha256"]
                or terminal["artifacts"]["candidate/root.json"]["sha256"] != root_sha
            ):
                raise ValueError("V281 generation or decision differs")
            head = json.loads(get(s3, prefix + "/generation/head.json"))
            root = get(s3, prefix + f"/generation/roots%2F{root_sha}.json")
            if head["root_sha256"] != root_sha or sha(root) != root_sha:
                raise ValueError("published V281 root differs")
        result = {
            "schema": SCHEMA + "-closeout",
            "source_commit": commit,
            "source_archive_sha256": archive_sha,
            "v272_terminal_sha256": V272_TERMINAL_SHA,
            "v278_terminal_sha256": V278_TERMINAL_SHA,
            "instance_id": instance_id,
            "terminal_sha256": sha(raw),
            "terminal_status": terminal["status"],
            "phase": terminal.get("phase"),
            "root_sha256": root_sha,
            "decision": decision,
            "purchase_option": purchase,
            "compute_quote_usd_per_hour": quote,
            "compute_usd_through_terminal_estimate": max(
                0, terminal["finished_epoch"] - launch_epoch
            )
            * quote
            / 3600,
        }
        body = json.dumps(result, sort_keys=True, separators=(",", ":")).encode()
        put_if_absent(prefix + "/closeout.json", body)
        if get(s3, prefix + "/closeout.json") != body:
            raise ValueError("V281 closeout readback differs")
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
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--on-demand", action="store_true")
    args = parser.parse_args()
    with open("/tmp/borsuk-v281-10m-launch.lock", "a+") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        launch(args.attempt, args.dry_run, args.on_demand)
