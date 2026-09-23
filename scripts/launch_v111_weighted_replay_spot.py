#!/usr/bin/env python3
"""Launch and authenticate one immutable ReLAION-1M weighted replay on Spot."""

from __future__ import annotations

import argparse
import base64
import dataclasses
import hashlib
import json
import shlex
import sys
import time
from pathlib import Path

if not __package__:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.launch_bounded_reader_1m_spot import (
    DEFAULT_TARGETS, BoundedReaderSpotPlan, ObjectIdentity, _atomic_put,
    _s3_location, build_launch_specs as bounded_launch_specs,
)

BUCKET = "borsuk-bench-453182569524-euc1"
V36 = f"s3://{BUCKET}/research/v36-prefix-screen/runs/v36-prefix-screen-20260908T174540Z-31445a91/attempt-0000"
INPUTS = {
    "SOURCE": ObjectIdentity(V36 + "/source.parquet",
        "2796b579f37afe99ca4aff57e282335a6a79ad30596645957d26326a0560cf86", 1458450077),
    "QUERIES": ObjectIdentity(V36 + "/development-query.parquet",
        "310bb54f79f2e79d09fe63aa4f6b5c6e9e7ffb31101964f816be978dadb2db54", 1558506),
    "TRUTH": ObjectIdentity(V36 + "/development-gt100.parquet",
        "fed7524fd675087f42b48b2f7fa9192b4661aaa4b665600de8378b8b6c696e11", 2046505),
    "LAYOUT": ObjectIdentity(
        f"s3://{BUCKET}/research/v63-algorithm-first/layout-oracle-e2f6c2bad99c720b/a0001/artifacts/kmeans_8192-order.npy",
        "32cba9690cd9d0ed3809763e5a0fa3574b09a207da26e93404651acaa1a66a0b", 4000128),
    "SQ8": ObjectIdentity(
        f"s3://{BUCKET}/research/v70-algorithm-first/single-stage-a4a695d66f508edf/index/sq8.bin",
        "2284f24745f964ff2b125eedb883d5cd8ff6afab0738f49f9e16e593167a318b", 780000000),
}


def build_plan(source_commit: str, archive_sha256: str, archive_bytes: int,
               attempt: int = 1) -> BoundedReaderSpotPlan:
    if (len(source_commit) != 40 or len(archive_sha256) != 64
        or any(char not in "0123456789abcdef" for char in source_commit + archive_sha256)
        or archive_bytes <= 0 or type(attempt) is not int or not 1 <= attempt <= 99):
        raise ValueError("source archive identity differs")
    archive = ObjectIdentity(
        f"s3://{BUCKET}/research/v111-weighted-reader/{source_commit}/source/source.tar.gz",
        archive_sha256, archive_bytes,
    )
    return BoundedReaderSpotPlan(
        profile="causality", source_commit=source_commit, source_archive=archive,
        source=INPUTS["SOURCE"], queries=INPUTS["QUERIES"], truth=INPUTS["TRUTH"],
        layout=INPUTS["LAYOUT"], sq8=INPUTS["SQ8"],
        output_prefix=(f"s3://{BUCKET}/research/v111-weighted-reader/{source_commit}"
                       f"/runs/relaion-1m-dev1000-a{attempt:04d}"),
        image_id="ami-06121aa3085b6f918", security_group_id="sg-0b1fd3e4fbde4af0d",
        instance_profile_arn="arn:aws:iam::453182569524:instance-profile/borsuk-bench-profile",
        targets=DEFAULT_TARGETS, spot_price_usd_per_hour_micros=720000,
        regions=1024, shortlist_rows=512, gap_pages=2, get_concurrency=0,
        wall_seconds=14400, attempt=attempt,
    )


def worker_script(plan: BoundedReaderSpotPlan) -> str:
    values = {
        "V111_SOURCE_COMMIT": plan.source_commit,
        "V111_ATTEMPT": plan.attempt,
        "V111_OUTPUT_PREFIX": plan.output_prefix,
        "V111_ARCHIVE_URI": plan.source_archive.uri,
        "V111_ARCHIVE_SHA256": plan.source_archive.sha256,
        "V111_ARCHIVE_BYTES": plan.source_archive.bytes,
        "V111_WALL_SECONDS": 7200,
    }
    for role in INPUTS:
        identity = INPUTS[role]
        values[f"V111_{role}_URI"] = identity.uri
        values[f"V111_{role}_SHA256"] = identity.sha256
        values[f"V111_{role}_BYTES"] = identity.bytes
    exports = "\n".join(
        f"export {key}={shlex.quote(str(value))}" for key, value in sorted(values.items())
    )
    body = """set -euo pipefail
root=/mnt/v111-weighted-replay
mkdir -p "$root" && cd "$root"
aws s3 cp "$V111_ARCHIVE_URI" source.tar.gz --only-show-errors
[ "$(stat -c%s source.tar.gz)" = "$V111_ARCHIVE_BYTES" ]
printf '%s  source.tar.gz\\n' "$V111_ARCHIVE_SHA256" | sha256sum -c -
mkdir repo
tar -xzf source.tar.gz -C repo
exec bash repo/scripts/run_v111_weighted_replay_remote.sh
"""
    script = "#!/bin/bash\n" + exports + "\n" + body
    if len(script.encode()) > 16384:
        raise ValueError("Spot user-data limit exceeded")
    return script


def build_launch_specs(plan: BoundedReaderSpotPlan) -> list[dict[str, object]]:
    specs = bounded_launch_specs(plan)
    encoded = base64.b64encode(worker_script(plan).encode()).decode()
    for spec in specs:
        subnet = spec["NetworkInterfaces"][0]["SubnetId"]
        digest = hashlib.sha256(
            f"v111:{plan.source_commit}:{subnet}:a{plan.attempt:04d}".encode()
        ).hexdigest()[:40]
        spec["ClientToken"] = "v111-" + digest
        spec["UserData"] = encoded
        spec["TagSpecifications"][0]["Tags"][0]["Value"] = "borsuk-v111-weighted-replay"
    return specs


def _claim(plan: BoundedReaderSpotPlan, instance_id: str | None) -> bytes:
    value = {"schema": "borsuk-v111-weighted-claim-v1", "source_commit": plan.source_commit,
             "source_archive": dataclasses.asdict(plan.source_archive),
             "inputs": {role: dataclasses.asdict(identity) for role, identity in INPUTS.items()},
             "instance_id": instance_id, "attempt": plan.attempt,
             "query_count": 1000, "prefix_stop_queries": 200,
             "regions": 1024, "shortlist": 512, "max_gets": 32,
             "max_bytes": 16777216, "instance_type": plan.instance_type,
             "instance_market": "spot", "interrupted_cell_action": "discard-and-new-attempt"}
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


def launch_one(ec2: object, plan: BoundedReaderSpotPlan) -> str:
    capacity = ("InsufficientInstanceCapacity", "InsufficientFreeAddressesInSubnet",
                "MaxSpotInstanceCountExceeded", "SpotMaxPriceTooLow")
    for spec in build_launch_specs(plan):
        try:
            response = ec2.run_instances(**spec)
        except Exception as error:
            if any(marker in str(error) for marker in capacity):
                continue
            raise
        instances = response.get("Instances", [])
        if len(instances) != 1:
            raise ValueError("Spot launch response differs")
        return instances[0]["InstanceId"]
    raise RuntimeError("Spot capacity unavailable")


def monitor(ec2: object, s3: object, plan: BoundedReaderSpotPlan,
            instance_id: str) -> dict[str, object]:
    bucket, prefix = _s3_location(plan.output_prefix)
    deadline = time.monotonic() + plan.wall_seconds + 1800
    try:
        while True:
            try:
                body = s3.get_object(Bucket=bucket, Key=prefix + "/terminal.json")["Body"].read()
            except Exception as error:
                state = ec2.describe_instances(InstanceIds=[instance_id])["Reservations"][0]["Instances"][0]["State"]["Name"]
                if state in ("terminated", "stopped", "shutting-down"):
                    raise RuntimeError(f"instance {state} without terminal") from error
                if time.monotonic() >= deadline:
                    raise TimeoutError("V111 Spot terminal deadline exceeded") from error
                time.sleep(15)
                continue
            terminal = json.loads(body)
            if (terminal.get("schema") != "borsuk-v111-weighted-terminal-v1"
                or terminal.get("source_commit") != plan.source_commit
                or terminal.get("attempt") != plan.attempt
                or terminal.get("instance_id") != instance_id):
                raise ValueError("V111 terminal identity differs")
            return terminal
    finally:
        ec2.terminate_instances(InstanceIds=[instance_id])


def readback(s3: object, terminal: dict[str, object]) -> dict[str, object]:
    if terminal.get("status") != "complete" or terminal.get("exit_code") != 0:
        return terminal
    artifacts = terminal.get("artifacts")
    required = {"hashes.log", "manifest-export.log", "prefix.log",
                "prefix-evidence.jsonl", "prefix-reduction.json",
                "prefix-resources.txt", "prefix-validation.log",
                "prefix-validation-resources.txt", "decision.json"}
    if not isinstance(artifacts, dict) or not required.issubset(artifacts):
        raise ValueError("V111 terminal artifact roster differs")
    bodies = {}
    for name, identity in artifacts.items():
        bucket, key = _s3_location(identity["uri"])
        body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
        if len(body) != identity["bytes"] or hashlib.sha256(body).hexdigest() != identity["sha256"]:
            raise ValueError("V111 artifact readback differs")
        bodies[name] = body
    decision = json.loads(bodies["decision.json"])
    prefix = json.loads(bodies["prefix-reduction.json"])
    if (decision.get("schema") != "borsuk-v111-decision-v1"
        or decision.get("prefix_evidence_sha256") != artifacts["prefix-evidence.jsonl"]["sha256"]
        or prefix.get("evidence_sha256") != artifacts["prefix-evidence.jsonl"]["sha256"]
        or prefix.get("query_count") != 200):
        raise ValueError("V111 prefix binding differs")
    if decision["prefix_decision"] == "advance-full":
        if not {"full.log", "full-evidence.jsonl", "full-reduction.json",
                "full-resources.txt", "full-validation.log",
                "full-validation-resources.txt"}.issubset(artifacts):
            raise ValueError("V111 full artifact roster differs")
        full = json.loads(bodies["full-reduction.json"])
        if (full.get("evidence_sha256") != artifacts["full-evidence.jsonl"]["sha256"]
            or full.get("query_count") != 1000):
            raise ValueError("V111 full binding differs")
    elif decision["prefix_decision"] not in ("harness-mismatch", "stop-weighted-planner"):
        raise ValueError("V111 decision differs")
    return terminal


def main() -> None:
    import boto3

    parser = argparse.ArgumentParser()
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--archive-sha256", required=True)
    parser.add_argument("--archive-bytes", type=int, required=True)
    parser.add_argument("--attempt", type=int, default=1)
    args = parser.parse_args()
    plan = build_plan(args.source_commit, args.archive_sha256, args.archive_bytes,
                      args.attempt)
    session = boto3.Session(profile_name="causality", region_name="eu-central-1")
    ec2, s3 = session.client("ec2"), session.client("s3")
    bucket, prefix = _s3_location(plan.output_prefix)
    _atomic_put(s3, bucket=bucket, key=prefix + "/reservation.json", body=_claim(plan, None))
    instance_id = launch_one(ec2, plan)
    try:
        _atomic_put(s3, bucket=bucket, key=prefix + "/launch.json", body=_claim(plan, instance_id))
    except Exception:
        ec2.terminate_instances(InstanceIds=[instance_id])
        raise
    terminal = monitor(ec2, s3, plan, instance_id)
    print(json.dumps(readback(s3, terminal), sort_keys=True, separators=(",", ":")))


if __name__ == "__main__":
    main()
