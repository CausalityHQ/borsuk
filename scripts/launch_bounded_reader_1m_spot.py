#!/usr/bin/env python3
"""Launch one immutable full-development bounded-reader cell on EC2 Spot."""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import pathlib
import shlex
import sys
import time
from dataclasses import dataclass
from typing import Sequence

if not __package__:
    sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))


@dataclass(frozen=True, slots=True)
class ObjectIdentity:
    uri: str
    sha256: str
    bytes: int


@dataclass(frozen=True, slots=True)
class SpotTarget:
    availability_zone: str
    subnet_id: str


DEFAULT_TARGETS = (
    SpotTarget("eu-central-1c", "subnet-0a12dbed0ca6fac25"),
    SpotTarget("eu-central-1b", "subnet-00243d923761c047c"),
    SpotTarget("eu-central-1a", "subnet-034528fbd6977848f"),
)


@dataclass(frozen=True, slots=True)
class BoundedReaderSpotPlan:
    profile: str
    source_commit: str
    source_archive: ObjectIdentity
    source: ObjectIdentity
    queries: ObjectIdentity
    truth: ObjectIdentity
    layout: ObjectIdentity
    sq8: ObjectIdentity
    output_prefix: str
    image_id: str
    security_group_id: str
    instance_profile_arn: str
    targets: tuple[SpotTarget, ...]
    spot_price_usd_per_hour_micros: int
    instance_type: str = "c7i.12xlarge"
    attempt: int = 1
    query_count: int = 1_000
    regions: int = 256
    shortlist_rows: int = 512
    gap_pages: int = 2
    get_concurrency: int = 128
    virtual_memory_kib: int = 48 * 1024 * 1024
    wall_seconds: int = 7_200


def _valid_identity(identity: ObjectIdentity) -> bool:
    return (
        identity.uri.startswith("s3://")
        and len(identity.sha256) == 64
        and all(character in "0123456789abcdef" for character in identity.sha256)
        and identity.bytes > 0
    )


def build_plan(**values: object) -> BoundedReaderSpotPlan:
    plan = BoundedReaderSpotPlan(**values)
    if (
        plan.profile != "causality"
        or len(plan.source_commit) != 40
        or any(character not in "0123456789abcdef" for character in plan.source_commit)
        or not all(
            _valid_identity(identity)
            for identity in (
                plan.source_archive,
                plan.source,
                plan.queries,
                plan.truth,
                plan.layout,
                plan.sq8,
            )
        )
        or plan.sq8.bytes != 780_000_000
        or not plan.output_prefix.startswith("s3://")
        or not plan.image_id.startswith("ami-")
        or not plan.security_group_id.startswith("sg-")
        or not plan.instance_profile_arn.startswith("arn:aws:iam::")
        or not plan.targets
        or len(set(plan.targets)) != len(plan.targets)
        or plan.instance_type != "c7i.12xlarge"
        or plan.attempt != 1
        or plan.query_count != 1_000
        or (plan.regions, plan.shortlist_rows, plan.gap_pages, plan.get_concurrency)
        != (256, 512, 2, 128)
        or plan.virtual_memory_kib != 48 * 1024 * 1024
        or plan.wall_seconds != 7_200
        or plan.spot_price_usd_per_hour_micros <= 0
    ):
        raise ValueError("bounded-reader Spot plan differs")
    return plan


def _q(value: object) -> str:
    return shlex.quote(str(value))


def worker_script(plan: BoundedReaderSpotPlan) -> str:
    identities = {
        "SOURCE_ARCHIVE": plan.source_archive,
        "SOURCE": plan.source,
        "QUERIES": plan.queries,
        "TRUTH": plan.truth,
        "LAYOUT": plan.layout,
        "SQ8": plan.sq8,
    }
    exports: dict[str, object] = {
        "BOUNDED_SOURCE_COMMIT": plan.source_commit,
        "BOUNDED_OUTPUT_PREFIX": plan.output_prefix.rstrip("/"),
        "BOUNDED_SPOT_PRICE_MICROS": plan.spot_price_usd_per_hour_micros,
        "BOUNDED_VIRTUAL_MEMORY_KIB": plan.virtual_memory_kib,
        "BOUNDED_WALL_SECONDS": plan.wall_seconds,
        "BORSUK_V71_QUERIES": plan.query_count,
        "BORSUK_V71_REGIONS": plan.regions,
        "BORSUK_V71_SHORTLIST": plan.shortlist_rows,
        "BORSUK_V71_GAP": plan.gap_pages,
        "BORSUK_V71_CONCURRENCY": plan.get_concurrency,
        "BORSUK_V71_THROUGHPUT": 1,
    }
    for role, identity in identities.items():
        exports[f"BOUNDED_{role}_URI"] = identity.uri
        exports[f"BOUNDED_{role}_SHA256"] = identity.sha256
        exports[f"BOUNDED_{role}_BYTES"] = identity.bytes
    header = "\n".join(
        f"export {name}={_q(value)}" for name, value in sorted(exports.items())
    )
    body = """set -euo pipefail
root=/mnt/bounded-reader-1m
mkdir -p "$root" && cd "$root"
aws s3 cp "$BOUNDED_SOURCE_ARCHIVE_URI" source.tar.gz --only-show-errors
[ "$(stat -c%s source.tar.gz)" = "$BOUNDED_SOURCE_ARCHIVE_BYTES" ]
printf '%s  source.tar.gz\n' "$BOUNDED_SOURCE_ARCHIVE_SHA256" | sha256sum -c -
mkdir repo
tar -xzf source.tar.gz -C repo
exec bash repo/scripts/run_bounded_reader_1m_remote.sh
"""
    script = f"#!/bin/bash\n{header}\n{body}"
    if len(script.encode()) > 16_384:
        raise ValueError("bounded-reader user data exceeds EC2 limit")
    return script


def build_launch_specs(plan: BoundedReaderSpotPlan) -> list[dict[str, object]]:
    user_data = base64.b64encode(worker_script(plan).encode()).decode()
    specs = []
    for target in plan.targets:
        token = hashlib.sha256(
            f"bounded:{plan.source_commit}:{target.availability_zone}:a0001".encode()
        ).hexdigest()[:48]
        specs.append(
            {
                "ClientToken": f"bounded-{token}",
                "ImageId": plan.image_id,
                "InstanceType": plan.instance_type,
                "MinCount": 1,
                "MaxCount": 1,
                "IamInstanceProfile": {"Arn": plan.instance_profile_arn},
                "InstanceInitiatedShutdownBehavior": "terminate",
                "InstanceMarketOptions": {
                    "MarketType": "spot",
                    "SpotOptions": {
                        "InstanceInterruptionBehavior": "terminate",
                        "SpotInstanceType": "one-time",
                    },
                },
                "NetworkInterfaces": [
                    {
                        "AssociatePublicIpAddress": True,
                        "DeviceIndex": 0,
                        "Groups": [plan.security_group_id],
                        "SubnetId": target.subnet_id,
                    }
                ],
                "BlockDeviceMappings": [
                    {
                        "DeviceName": "/dev/xvda",
                        "Ebs": {
                            "DeleteOnTermination": True,
                            "Encrypted": True,
                            "VolumeSize": 160,
                            "VolumeType": "gp3",
                        },
                    }
                ],
                "UserData": user_data,
                "TagSpecifications": [
                    {
                        "ResourceType": "instance",
                        "Tags": [
                            {"Key": "Name", "Value": "borsuk-bounded-reader-1m"},
                            {"Key": "BorsukAttempt", "Value": "a0001"},
                        ],
                    }
                ],
            }
        )
    return specs


def launch_one_spot(plan: BoundedReaderSpotPlan, *, ec2_client: object) -> str:
    capacity = (
        "InsufficientInstanceCapacity",
        "InsufficientFreeAddressesInSubnet",
        "MaxSpotInstanceCountExceeded",
        "SpotMaxPriceTooLow",
    )
    for request in build_launch_specs(plan):
        try:
            response = ec2_client.run_instances(**request)
        except Exception as error:
            if any(marker in str(error) for marker in capacity):
                continue
            raise
        instances = response.get("Instances") if type(response) is dict else None
        if (
            type(instances) is not list
            or len(instances) != 1
            or type(instances[0]) is not dict
            or type(instances[0].get("InstanceId")) is not str
        ):
            raise RuntimeError("bounded-reader Spot launch response differs")
        return instances[0]["InstanceId"]
    raise RuntimeError("bounded-reader Spot capacity unavailable")


def _s3_location(uri: str) -> tuple[str, str]:
    bucket, separator, key = uri.removeprefix("s3://").partition("/")
    if not separator or not bucket or not key:
        raise ValueError("bounded-reader S3 URI differs")
    return bucket, key.rstrip("/")


def _atomic_put(s3_client: object, *, bucket: str, key: str, body: bytes) -> None:
    event_name = "before-sign.s3.PutObject"
    event_id = f"bounded-{hashlib.sha256(key.encode()).hexdigest()[:16]}"

    def add_precondition(request: object, **_: object) -> None:
        request.headers["If-None-Match"] = "*"

    s3_client.meta.events.register_first(
        event_name, add_precondition, unique_id=event_id
    )
    try:
        s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=body,
            ContentType="application/json",
        )
    finally:
        s3_client.meta.events.unregister(event_name, unique_id=event_id)


def _claim_body(plan: BoundedReaderSpotPlan, instance_id: str | None) -> bytes:
    identities = {
        role: {
            "uri": identity.uri,
            "sha256": identity.sha256,
            "bytes": identity.bytes,
        }
        for role, identity in (
            ("source_archive", plan.source_archive),
            ("source", plan.source),
            ("queries", plan.queries),
            ("truth", plan.truth),
            ("layout", plan.layout),
            ("sq8", plan.sq8),
        )
    }
    return (
        json.dumps(
            {
                "schema": "borsuk-bounded-reader-attempt-claim-v1",
                "attempt": plan.attempt,
                "source_commit": plan.source_commit,
                "instance_id": instance_id,
                "inputs": identities,
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n"
    ).encode()


def claim_reserved_attempt(plan: BoundedReaderSpotPlan, *, s3_client: object) -> None:
    bucket, prefix = _s3_location(plan.output_prefix)
    _atomic_put(
        s3_client,
        bucket=bucket,
        key=f"{prefix}/reservation.json",
        body=_claim_body(plan, None),
    )


def claim_launched_attempt(
    plan: BoundedReaderSpotPlan, *, s3_client: object, instance_id: str
) -> None:
    bucket, prefix = _s3_location(plan.output_prefix)
    _atomic_put(
        s3_client,
        bucket=bucket,
        key=f"{prefix}/launch.json",
        body=_claim_body(plan, instance_id),
    )


def monitor_and_terminate(
    plan: BoundedReaderSpotPlan,
    *,
    ec2_client: object,
    s3_client: object,
    instance_id: str,
    poll_seconds: int = 15,
) -> dict[str, object]:
    bucket, prefix = _s3_location(plan.output_prefix)
    deadline = time.monotonic() + plan.wall_seconds + 1_800
    try:
        while True:
            try:
                response = s3_client.get_object(
                    Bucket=bucket, Key=f"{prefix}/terminal.json"
                )
            except Exception as error:
                if time.monotonic() >= deadline:
                    raise TimeoutError("bounded-reader terminal deadline exceeded") from error
                state_response = ec2_client.describe_instances(InstanceIds=[instance_id])
                state = state_response["Reservations"][0]["Instances"][0]["State"]["Name"]
                if state in ("terminated", "stopped", "shutting-down"):
                    raise RuntimeError(f"instance {state} without terminal") from error
                time.sleep(poll_seconds)
                continue
            body = response["Body"].read()
            value = json.loads(body)
            if (
                type(value) is not dict
                or value.get("schema") != "borsuk-bounded-reader-terminal-v1"
                or value.get("source_commit") != plan.source_commit
                or value.get("instance_id") != instance_id
            ):
                raise ValueError("bounded-reader terminal differs")
            return value
    finally:
        ec2_client.terminate_instances(InstanceIds=[instance_id])


def parse_args(argv: Sequence[str] | None = None) -> BoundedReaderSpotPlan:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--source-archive-uri", required=True)
    parser.add_argument("--source-archive-sha256", required=True)
    parser.add_argument("--source-archive-bytes", type=int, required=True)
    parser.add_argument("--source-uri", required=True)
    parser.add_argument("--source-sha256", required=True)
    parser.add_argument("--source-bytes", type=int, required=True)
    parser.add_argument("--queries-uri", required=True)
    parser.add_argument("--queries-sha256", required=True)
    parser.add_argument("--queries-bytes", type=int, required=True)
    parser.add_argument("--truth-uri", required=True)
    parser.add_argument("--truth-sha256", required=True)
    parser.add_argument("--truth-bytes", type=int, required=True)
    parser.add_argument("--layout-uri", required=True)
    parser.add_argument("--layout-sha256", required=True)
    parser.add_argument("--layout-bytes", type=int, required=True)
    parser.add_argument("--sq8-uri", required=True)
    parser.add_argument("--sq8-sha256", required=True)
    parser.add_argument("--sq8-bytes", type=int, required=True)
    parser.add_argument("--output-prefix", required=True)
    parser.add_argument("--profile", default="causality")
    parser.add_argument("--image-id", default="ami-06121aa3085b6f918")
    parser.add_argument("--security-group-id", default="sg-0b1fd3e4fbde4af0d")
    parser.add_argument(
        "--instance-profile-arn",
        default="arn:aws:iam::453182569524:instance-profile/borsuk-bench-profile",
    )
    parser.add_argument("--spot-price-usd-per-hour-micros", type=int, default=720_000)
    args = parser.parse_args(argv)
    identity = lambda role: ObjectIdentity(  # noqa: E731
        getattr(args, f"{role}_uri"),
        getattr(args, f"{role}_sha256"),
        getattr(args, f"{role}_bytes"),
    )
    return build_plan(
        profile=args.profile,
        source_commit=args.source_commit,
        source_archive=ObjectIdentity(
            args.source_archive_uri,
            args.source_archive_sha256,
            args.source_archive_bytes,
        ),
        source=identity("source"),
        queries=identity("queries"),
        truth=identity("truth"),
        layout=identity("layout"),
        sq8=identity("sq8"),
        output_prefix=args.output_prefix,
        image_id=args.image_id,
        security_group_id=args.security_group_id,
        instance_profile_arn=args.instance_profile_arn,
        targets=DEFAULT_TARGETS,
        spot_price_usd_per_hour_micros=args.spot_price_usd_per_hour_micros,
    )


def main(argv: Sequence[str] | None = None) -> None:
    import boto3

    plan = parse_args(argv)
    session = boto3.Session(profile_name=plan.profile, region_name="eu-central-1")
    ec2_client = session.client("ec2")
    s3_client = session.client("s3")
    claim_reserved_attempt(plan, s3_client=s3_client)
    instance_id = launch_one_spot(plan, ec2_client=ec2_client)
    try:
        claim_launched_attempt(plan, s3_client=s3_client, instance_id=instance_id)
    except Exception:
        ec2_client.terminate_instances(InstanceIds=[instance_id])
        raise
    terminal = monitor_and_terminate(
        plan,
        ec2_client=ec2_client,
        s3_client=s3_client,
        instance_id=instance_id,
    )
    print(json.dumps(terminal, sort_keys=True, separators=(",", ":")))


if __name__ == "__main__":
    main()
