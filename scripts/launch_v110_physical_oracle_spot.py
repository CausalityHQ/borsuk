#!/usr/bin/env python3
"""One immutable Causality Spot cell for the V63 exact interval oracle."""

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
from scripts.launch_v109_capped_replay_spot import BUCKET, INPUTS


def build_plan(source_commit: str, archive_sha256: str, archive_bytes: int,
               attempt: int = 1) -> BoundedReaderSpotPlan:
    if (len(source_commit) != 40 or len(archive_sha256) != 64
        or any(char not in "0123456789abcdef" for char in source_commit + archive_sha256)
        or archive_bytes <= 0 or type(attempt) is not int or not 1 <= attempt <= 99):
        raise ValueError("V110 source identity or attempt differs")
    archive = ObjectIdentity(
        f"s3://{BUCKET}/research/v110-physical-oracle/{source_commit}/source/source.tar.gz",
        archive_sha256, archive_bytes,
    )
    return BoundedReaderSpotPlan(
        profile="causality", source_commit=source_commit, source_archive=archive,
        source=INPUTS["SOURCE"], queries=INPUTS["QUERIES"], truth=INPUTS["TRUTH"],
        layout=INPUTS["LAYOUT"], sq8=INPUTS["SQ8"],
        output_prefix=(f"s3://{BUCKET}/research/v110-physical-oracle/{source_commit}"
                       f"/runs/relaion-1m-dev1000-a{attempt:04d}"),
        image_id="ami-06121aa3085b6f918", security_group_id="sg-0b1fd3e4fbde4af0d",
        instance_profile_arn="arn:aws:iam::453182569524:instance-profile/borsuk-bench-profile",
        targets=DEFAULT_TARGETS, spot_price_usd_per_hour_micros=500000,
        instance_type="c7i.8xlarge", regions=1024, shortlist_rows=512,
        get_concurrency=0, attempt=attempt, wall_seconds=10800,
    )


def worker_script(plan: BoundedReaderSpotPlan) -> str:
    values = {"V110_SOURCE_COMMIT": plan.source_commit, "V110_ATTEMPT": plan.attempt,
              "V110_OUTPUT_PREFIX": plan.output_prefix,
              "V110_ARCHIVE_URI": plan.source_archive.uri,
              "V110_ARCHIVE_SHA256": plan.source_archive.sha256,
              "V110_ARCHIVE_BYTES": plan.source_archive.bytes}
    for role in ("SOURCE", "TRUTH", "LAYOUT"):
        identity = INPUTS[role]
        values[f"V110_{role}_URI"] = identity.uri
        values[f"V110_{role}_SHA256"] = identity.sha256
        values[f"V110_{role}_BYTES"] = identity.bytes
    exports = "\n".join(f"export {key}={shlex.quote(str(value))}"
                        for key, value in sorted(values.items()))
    body = """set -euo pipefail
root=/mnt/v110-physical-oracle
mkdir -p "$root" && cd "$root"
aws s3 cp "$V110_ARCHIVE_URI" source.tar.gz --only-show-errors
[ "$(stat -c%s source.tar.gz)" = "$V110_ARCHIVE_BYTES" ]
printf '%s  source.tar.gz\\n' "$V110_ARCHIVE_SHA256" | sha256sum -c -
mkdir repo
tar -xzf source.tar.gz -C repo
exec bash repo/scripts/run_v110_physical_oracle_remote.sh
"""
    script = "#!/bin/bash\n" + exports + "\n" + body
    if len(script.encode()) > 16384:
        raise ValueError("V110 user-data limit exceeded")
    return script


def build_launch_specs(plan: BoundedReaderSpotPlan) -> list[dict[str, object]]:
    specs = bounded_launch_specs(plan)
    user_data = base64.b64encode(worker_script(plan).encode()).decode()
    for spec in specs:
        subnet = spec["NetworkInterfaces"][0]["SubnetId"]
        digest = hashlib.sha256(
            f"v110:{plan.source_commit}:{subnet}:a{plan.attempt:04d}".encode()
        ).hexdigest()[:40]
        spec["ClientToken"] = "v110-" + digest
        spec["UserData"] = user_data
        spec["TagSpecifications"][0]["Tags"][0]["Value"] = "borsuk-v110-physical-oracle"
    return specs


def _claim(plan: BoundedReaderSpotPlan, instance_id: str | None) -> bytes:
    body = {"schema": "borsuk-v110-physical-oracle-claim-v1",
            "source_commit": plan.source_commit, "attempt": plan.attempt,
            "source_archive": dataclasses.asdict(plan.source_archive),
            "inputs": {role: dataclasses.asdict(INPUTS[role])
                       for role in ("SOURCE", "TRUTH", "LAYOUT")},
            "instance_id": instance_id, "instance_type": plan.instance_type,
            "instance_market": "spot", "query_count": 1000,
            "oracle_kind": "truth-aware-physical-upper-bound",
            "max_gets": 32, "max_bytes": 16777216,
            "interrupted_cell_action": "discard-and-new-attempt"}
    return (json.dumps(body, sort_keys=True, separators=(",", ":")) + "\n").encode()


def launch_one(ec2: object, plan: BoundedReaderSpotPlan) -> str:
    capacity = ("InsufficientInstanceCapacity", "InsufficientFreeAddressesInSubnet",
                "MaxSpotInstanceCountExceeded", "SpotMaxPriceTooLow")
    for spec in build_launch_specs(plan):
        try:
            result = ec2.run_instances(**spec)
        except Exception as error:
            if any(marker in str(error) for marker in capacity):
                continue
            raise
        instances = result.get("Instances", [])
        if len(instances) != 1:
            raise ValueError("V110 Spot launch differs")
        return instances[0]["InstanceId"]
    raise RuntimeError("V110 Spot capacity unavailable")


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
                    raise RuntimeError(f"V110 instance {state} without terminal") from error
                if time.monotonic() >= deadline:
                    raise TimeoutError("V110 terminal deadline exceeded") from error
                time.sleep(15)
                continue
            terminal = json.loads(body)
            if (terminal.get("schema") != "borsuk-v110-physical-oracle-terminal-v1"
                or terminal.get("source_commit") != plan.source_commit
                or terminal.get("attempt") != plan.attempt
                or terminal.get("instance_id") != instance_id):
                raise ValueError("V110 terminal authority differs")
            return terminal
    finally:
        ec2.terminate_instances(InstanceIds=[instance_id])


def readback(s3: object, terminal: dict[str, object]) -> dict[str, object]:
    if terminal.get("status") != "complete" or terminal.get("exit_code") != 0:
        return terminal
    artifacts = terminal.get("artifacts")
    required = {"worker.log", "hashes.log", "oracle.log", "oracle-resources.txt",
                "evidence.jsonl", "validation.log", "validation-resources.txt",
                "reduction.json"}
    if not isinstance(artifacts, dict) or not required.issubset(artifacts):
        raise ValueError("V110 terminal roster differs")
    bodies = {}
    for name, identity in artifacts.items():
        bucket, key = _s3_location(identity["uri"])
        body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
        if len(body) != identity["bytes"] or hashlib.sha256(body).hexdigest() != identity["sha256"]:
            raise ValueError("V110 readback differs")
        bodies[name] = body
    reduction = json.loads(bodies["reduction.json"])
    if (reduction.get("schema") != "borsuk-v110-physical-oracle-reduction-v1"
        or reduction.get("evidence_sha256") != artifacts["evidence.jsonl"]["sha256"]
        or reduction.get("query_count") != 1000):
        raise ValueError("V110 reduction binding differs")
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
