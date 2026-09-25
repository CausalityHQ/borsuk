#!/usr/bin/env python3
"""Launch and monitor one immutable matched ReLAION-1M S3 Vectors cell."""

from __future__ import annotations

import argparse
import hashlib
import json
import shlex
import time
from dataclasses import asdict, dataclass
from typing import Literal, Mapping, Sequence


@dataclass(frozen=True, slots=True)
class ObjectIdentity:
    role: str
    uri: str
    sha256: str
    bytes: int

    def __post_init__(self) -> None:
        if (
            not self.role
            or not self.uri.startswith("s3://")
            or len(self.sha256) != 64
            or any(character not in "0123456789abcdef" for character in self.sha256)
            or type(self.bytes) is not int
            or self.bytes <= 0
        ):
            raise ValueError("object identity differs")


@dataclass(frozen=True, slots=True)
class SpotTarget:
    availability_zone: str
    subnet_id: str

    def __post_init__(self) -> None:
        if not self.availability_zone.startswith(
            "eu-central-1"
        ) or not self.subnet_id.startswith("subnet-"):
            raise ValueError("Spot target differs")


DEFAULT_TARGETS = (
    SpotTarget("eu-central-1c", "subnet-0a12dbed0ca6fac25"),
    SpotTarget("eu-central-1b", "subnet-00243d923761c047c"),
    SpotTarget("eu-central-1a", "subnet-034528fbd6977848f"),
)


@dataclass(frozen=True, slots=True)
class MatchedSpotPlan:
    profile: str
    source_commit: str
    source_archive: ObjectIdentity
    inputs: Mapping[str, ObjectIdentity]
    output_prefix: str
    image_id: str
    image_architecture: str
    security_group_id: str
    instance_profile_arn: str
    targets: tuple[SpotTarget, ...]
    spot_price_usd_per_hour_micros: int
    vector_bucket: str
    attempt: int = 1
    instance_type: str = "c7i.8xlarge"
    wall_seconds: int = 7_200
    split: str = "development"
    query_workers: int = 1
    query_order: str = "shuffled"
    metric: str = "euclidean"


@dataclass(frozen=True, slots=True)
class AttemptTerminal:
    schema: str
    attempt: int
    status: Literal["complete", "failed", "interrupted"]
    claim_eligible: bool
    source_commit: str
    instance_id: str
    exit_code: int
    spot_price_usd_per_hour_micros: int
    evidence: Mapping[str, ObjectIdentity]


def build_plan(**values: object) -> MatchedSpotPlan:
    """Freeze one claim-bearing matched competitor attempt."""

    try:
        plan = MatchedSpotPlan(**values)
    except (AttributeError, TypeError) as error:
        raise ValueError("matched Spot plan differs") from error
    if (
        plan.profile != "causality"
        or len(plan.source_commit) != 40
        or any(character not in "0123456789abcdef" for character in plan.source_commit)
        or plan.source_archive.role != "source_archive"
        or set(plan.inputs) != {"source", "queries", "truth"}
        or any(identity.role != role for role, identity in plan.inputs.items())
        or not plan.output_prefix.startswith("s3://")
        or not plan.image_id.startswith("ami-")
        or plan.image_architecture != "x86_64"
        or plan.security_group_id[:3] != "sg-"
        or not plan.instance_profile_arn.startswith("arn:aws:iam::")
        or not plan.targets
        or len(set(plan.targets)) != len(plan.targets)
        or plan.spot_price_usd_per_hour_micros <= 0
        or not 3 <= len(plan.vector_bucket) <= 63
        or plan.attempt != 1
        or plan.instance_type not in {"c7i.4xlarge", "c7i.8xlarge"}
        or plan.wall_seconds != 7_200
        or plan.split not in {"development", "validation"}
        or not 1 <= plan.query_workers <= 16
        or plan.query_order not in {"shuffled", "ordinal"}
        or plan.metric not in {"euclidean", "cosine"}
    ):
        raise ValueError("matched Spot plan differs")
    return plan


def _q(value: object) -> str:
    return shlex.quote(str(value))


def worker_script(plan: MatchedSpotPlan) -> str:
    """Build the size-bounded authenticated EC2 bootstrap."""

    exports: dict[str, object] = {
        "MATCHED_ATTEMPT": plan.attempt,
        "MATCHED_OUTPUT_PREFIX": plan.output_prefix.rstrip("/"),
        "MATCHED_SOURCE_ARCHIVE_BYTES": plan.source_archive.bytes,
        "MATCHED_SOURCE_ARCHIVE_SHA256": plan.source_archive.sha256,
        "MATCHED_SOURCE_ARCHIVE_URI": plan.source_archive.uri,
        "MATCHED_SOURCE_COMMIT": plan.source_commit,
        "MATCHED_SPOT_PRICE_MICROS": plan.spot_price_usd_per_hour_micros,
        "MATCHED_VECTOR_BUCKET": plan.vector_bucket,
        "MATCHED_WALL_SECONDS": plan.wall_seconds,
        "MATCHED_QUERY_WORKERS": plan.query_workers,
        "MATCHED_QUERY_ORDER": plan.query_order,
        "MATCHED_SPLIT": plan.split,
        "MATCHED_METRIC": plan.metric,
    }
    for role, identity in sorted(plan.inputs.items()):
        prefix = f"MATCHED_{role.upper()}"
        exports[f"{prefix}_BYTES"] = identity.bytes
        exports[f"{prefix}_SHA256"] = identity.sha256
        exports[f"{prefix}_URI"] = identity.uri
    header = "\n".join(
        f"export {name}={_q(value)}" for name, value in sorted(exports.items())
    )
    bootstrap = """set -euo pipefail
root=/mnt/matched-s3-vectors-1m
mkdir -p "$root" && cd "$root"
aws s3 cp "$MATCHED_SOURCE_ARCHIVE_URI" source.tar.gz --only-show-errors
[ "$(stat -c%s source.tar.gz)" = "$MATCHED_SOURCE_ARCHIVE_BYTES" ]
printf '%s  source.tar.gz\n' "$MATCHED_SOURCE_ARCHIVE_SHA256" >hashes.txt
sha256sum -c hashes.txt
mkdir repo
tar -xzf source.tar.gz -C repo
exec bash repo/scripts/run_matched_s3_vectors_1m_remote.sh
"""
    script = f"#!/bin/bash\n{header}\n{bootstrap}"
    if len(script.encode()) > 16_384:
        raise ValueError("matched Spot user data exceeds EC2 limit")
    return script


def build_launch_specs(plan: MatchedSpotPlan) -> list[dict[str, object]]:
    """Build serial multi-AZ one-time Spot requests."""

    script = worker_script(plan)
    specs: list[dict[str, object]] = []
    for target in plan.targets:
        token = hashlib.sha256(
            f"matched-s3v:{plan.source_commit}:{target.availability_zone}:a0001".encode()
        ).hexdigest()[:48]
        specs.append(
            {
                "BlockDeviceMappings": [
                    {
                        "DeviceName": "/dev/xvda",
                        "Ebs": {
                            "DeleteOnTermination": True,
                            "Encrypted": True,
                            "VolumeSize": 100,
                            "VolumeType": "gp3",
                        },
                    }
                ],
                "ClientToken": "matched-s3v-" + token,
                "IamInstanceProfile": {"Arn": plan.instance_profile_arn},
                "ImageId": plan.image_id,
                "InstanceInitiatedShutdownBehavior": "terminate",
                "InstanceMarketOptions": {
                    "MarketType": "spot",
                    "SpotOptions": {
                        "InstanceInterruptionBehavior": "terminate",
                        "SpotInstanceType": "one-time",
                    },
                },
                "InstanceType": plan.instance_type,
                "MaxCount": 1,
                "MinCount": 1,
                "NetworkInterfaces": [
                    {
                        "AssociatePublicIpAddress": True,
                        "DeviceIndex": 0,
                        "Groups": [plan.security_group_id],
                        "SubnetId": target.subnet_id,
                    }
                ],
                "Placement": {"AvailabilityZone": target.availability_zone},
                "TagSpecifications": [
                    {
                        "ResourceType": "instance",
                        "Tags": [
                            {"Key": "Name", "Value": "borsuk-matched-s3-vectors-1m"},
                            {"Key": "BorsukAttempt", "Value": "a0001"},
                        ],
                    }
                ],
                "UserData": script,
            }
        )
    return specs


def launch_one_spot(plan: MatchedSpotPlan, *, ec2_client: object) -> str:
    """Try registered zones serially and launch at most one instance."""

    capacity_markers = (
        "InsufficientInstanceCapacity",
        "InsufficientFreeAddressesInSubnet",
        "MaxSpotInstanceCountExceeded",
        "SpotMaxPriceTooLow",
    )
    failures = 0
    for request in build_launch_specs(plan):
        try:
            response = ec2_client.run_instances(**request)
        except Exception as error:
            if not any(marker in str(error) for marker in capacity_markers):
                raise
            failures += 1
            continue
        instances = response.get("Instances") if type(response) is dict else None
        if (
            type(instances) is not list
            or len(instances) != 1
            or type(instances[0]) is not dict
            or type(instances[0].get("InstanceId")) is not str
        ):
            raise RuntimeError("matched Spot launch response differs")
        return instances[0]["InstanceId"]
    raise RuntimeError(f"matched Spot capacity unavailable in {failures} zones")


def _s3_location(uri: str) -> tuple[str, str]:
    remainder = uri.removeprefix("s3://")
    bucket, separator, key = remainder.partition("/")
    if not separator or not bucket or not key:
        raise ValueError("matched S3 URI differs")
    return bucket, key.rstrip("/")


def _atomic_put(s3_client: object, *, bucket: str, key: str, body: bytes) -> None:
    event_name = "before-sign.s3.PutObject"
    event_id = "matched-" + hashlib.sha256(key.encode()).hexdigest()[:16]

    def add_precondition(request: object, **_: object) -> None:
        request.headers["If-None-Match"] = "*"

    s3_client.meta.events.register_first(
        event_name, add_precondition, unique_id=event_id
    )
    try:
        s3_client.put_object(
            Bucket=bucket, Key=key, Body=body, ContentType="application/json"
        )
    finally:
        s3_client.meta.events.unregister(event_name, unique_id=event_id)


def _launch_receipt(plan: MatchedSpotPlan, instance_id: str | None) -> bytes:
    value = {
        "attempt": plan.attempt,
        "inputs": {
            role: asdict(identity) for role, identity in sorted(plan.inputs.items())
        },
        "instance_id": instance_id,
        "schema": "borsuk-matched-s3-vectors-launch-v2",
        "source_archive": asdict(plan.source_archive),
        "source_commit": plan.source_commit,
        "vector_bucket": plan.vector_bucket,
        "split": plan.split,
        "query_workers": plan.query_workers,
        "query_order": plan.query_order,
        "instance_type": plan.instance_type,
        "spot_price_usd_per_hour_micros": plan.spot_price_usd_per_hour_micros,
        "metric": plan.metric,
    }
    return (
        json.dumps(value, allow_nan=False, separators=(",", ":"), sort_keys=True).encode()
        + b"\n"
    )


def ensure_unstarted(plan: MatchedSpotPlan, *, s3_client: object) -> None:
    bucket, prefix = _s3_location(plan.output_prefix)
    for name in ("reservation.json", "launch.json", "terminal.json"):
        try:
            s3_client.head_object(Bucket=bucket, Key=f"{prefix}/{name}")
        except Exception as error:
            code = str(getattr(error, "response", {}).get("Error", {}).get("Code", ""))
            if code in {"404", "NoSuchKey", "NotFound"} or any(
                marker in str(error) for marker in ("404", "NoSuchKey", "NotFound")
            ):
                continue
            raise
        raise ValueError("matched immutable attempt already exists")


def claim_reserved_attempt(plan: MatchedSpotPlan, *, s3_client: object) -> None:
    bucket, prefix = _s3_location(plan.output_prefix)
    _atomic_put(
        s3_client,
        bucket=bucket,
        key=f"{prefix}/reservation.json",
        body=_launch_receipt(plan, None),
    )


def claim_launched_attempt(
    plan: MatchedSpotPlan, *, s3_client: object, instance_id: str
) -> None:
    if not instance_id.startswith("i-"):
        raise ValueError("matched instance identity differs")
    bucket, prefix = _s3_location(plan.output_prefix)
    _atomic_put(
        s3_client,
        bucket=bucket,
        key=f"{prefix}/launch.json",
        body=_launch_receipt(plan, instance_id),
    )


def canonical_terminal_bytes(
    plan: MatchedSpotPlan,
    *,
    instance_id: str,
    status: Literal["complete", "failed", "interrupted"],
    exit_code: int,
    evidence: Mapping[str, ObjectIdentity],
) -> bytes:
    required = {"cleanup", "resources", "result", "samples", "worker_log"}
    if (
        not instance_id.startswith("i-")
        or status not in {"complete", "failed", "interrupted"}
        or type(exit_code) is not int
        or (status == "complete" and (exit_code != 0 or set(evidence) != required))
        or (
            status != "complete"
            and (exit_code == 0 or not set(evidence).issubset(required))
        )
    ):
        raise ValueError("terminal differs")
    terminal = AttemptTerminal(
        schema="borsuk-matched-s3-vectors-terminal-v1",
        attempt=1,
        status=status,
        claim_eligible=status == "complete",
        source_commit=plan.source_commit,
        instance_id=instance_id,
        exit_code=exit_code,
        spot_price_usd_per_hour_micros=plan.spot_price_usd_per_hour_micros,
        evidence=dict(sorted(evidence.items())),
    )
    return (
        json.dumps(
            asdict(terminal), allow_nan=False, separators=(",", ":"), sort_keys=True
        ).encode()
        + b"\n"
    )


def validate_terminal_bytes(
    body: bytes, plan: MatchedSpotPlan, instance_id: str
) -> AttemptTerminal:
    try:
        value = json.loads(body)
        evidence = {
            role: ObjectIdentity(**identity)
            for role, identity in value["evidence"].items()
        }
        expected = canonical_terminal_bytes(
            plan,
            instance_id=instance_id,
            status=value["status"],
            exit_code=value["exit_code"],
            evidence=evidence,
        )
    except (KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
        raise ValueError("terminal authority differs") from error
    if expected != body:
        raise ValueError("terminal authority differs")
    return AttemptTerminal(
        schema=value["schema"],
        attempt=value["attempt"],
        status=value["status"],
        claim_eligible=value["claim_eligible"],
        source_commit=value["source_commit"],
        instance_id=value["instance_id"],
        exit_code=value["exit_code"],
        spot_price_usd_per_hour_micros=value["spot_price_usd_per_hour_micros"],
        evidence=evidence,
    )


def monitor_and_terminate(
    plan: MatchedSpotPlan,
    *,
    s3_client: object,
    ec2_client: object,
    instance_id: str,
    poll_seconds: float = 15.0,
    monotonic: object = time.monotonic,
    sleep: object = time.sleep,
) -> AttemptTerminal:
    """Observe only the terminal marker and always terminate compute."""

    bucket, prefix = _s3_location(plan.output_prefix)
    deadline = monotonic() + plan.wall_seconds + 900  # type: ignore[operator]
    try:
        while True:
            try:
                response = s3_client.get_object(
                    Bucket=bucket, Key=f"{prefix}/terminal.json"
                )
            except Exception as error:
                code = str(
                    getattr(error, "response", {}).get("Error", {}).get("Code", "")
                )
                if code not in {"404", "NoSuchKey", "NotFound"} and not any(
                    marker in str(error) for marker in ("404", "NoSuchKey", "NotFound")
                ):
                    raise
                state = ec2_client.describe_instances(InstanceIds=[instance_id])[
                    "Reservations"
                ][0]["Instances"][0]["State"]["Name"]
                if state in {"terminated", "stopped", "shutting-down"}:
                    raise RuntimeError(
                        f"instance {state} without terminal marker"
                    ) from error
                if monotonic() >= deadline:  # type: ignore[operator]
                    raise TimeoutError("matched attempt exceeded deadline") from error
                sleep(poll_seconds)  # type: ignore[operator]
                continue
            body = response["Body"].read()
            if response.get("ContentLength") != len(body):
                raise ValueError("terminal length differs")
            return validate_terminal_bytes(body, plan, instance_id)
    finally:
        ec2_client.terminate_instances(InstanceIds=[instance_id])


def parse_args(argv: Sequence[str] | None = None) -> MatchedSpotPlan:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--source-archive-uri", required=True)
    parser.add_argument("--source-archive-sha256", required=True)
    parser.add_argument("--source-archive-bytes", type=int, required=True)
    parser.add_argument("--output-prefix", required=True)
    parser.add_argument("--vector-bucket", required=True)
    parser.add_argument("--profile", default="causality")
    parser.add_argument("--image-id", default="ami-06121aa3085b6f918")
    parser.add_argument("--image-architecture", default="x86_64")
    parser.add_argument("--security-group-id", default="sg-0b1fd3e4fbde4af0d")
    parser.add_argument(
        "--instance-profile-arn",
        default="arn:aws:iam::453182569524:instance-profile/borsuk-bench-profile",
    )
    parser.add_argument("--spot-price-usd-per-hour-micros", type=int, default=688_000)
    parser.add_argument("--split", choices=("development", "validation"), default="development")
    parser.add_argument("--query-workers", type=int, default=1)
    parser.add_argument("--query-order", choices=("shuffled", "ordinal"), default="shuffled")
    parser.add_argument("--instance-type", choices=("c7i.4xlarge", "c7i.8xlarge"), default="c7i.8xlarge")
    parser.add_argument("--metric", choices=("euclidean", "cosine"), default="euclidean")
    args = parser.parse_args(argv)
    query_name = f"{args.split}-query.parquet"
    truth_name = f"{args.split}-gt100.parquet"
    validation = args.split == "validation"
    inputs = {
        "source": ObjectIdentity(
            "source",
            "s3://borsuk-bench-453182569524-euc1/research/v36-prefix-screen/runs/v36-prefix-screen-20260908T174540Z-31445a91/attempt-0000/source.parquet",
            "2796b579f37afe99ca4aff57e282335a6a79ad30596645957d26326a0560cf86",
            1_458_450_077,
        ),
        "queries": ObjectIdentity(
            "queries",
            "s3://borsuk-bench-453182569524-euc1/research/v36-prefix-screen/runs/v36-prefix-screen-20260908T174540Z-31445a91/attempt-0000/" + query_name,
            "869e225181f7d01a972d8faa144eaff4838c7c1f8f7c0c55091487e234f0bd5e" if validation else
            "310bb54f79f2e79d09fe63aa4f6b5c6e9e7ffb31101964f816be978dadb2db54",
            1_558_594 if validation else 1_558_506,
        ),
        "truth": ObjectIdentity(
            "truth",
            "s3://borsuk-bench-453182569524-euc1/research/v36-prefix-screen/runs/v36-prefix-screen-20260908T174540Z-31445a91/attempt-0000/" + truth_name,
            "bf0fb0c934c986d05282e3d1c63dc351c553976ea05bfcab0cd3f06d2979e871" if validation else
            "fed7524fd675087f42b48b2f7fa9192b4661aaa4b665600de8378b8b6c696e11",
            2_045_045 if validation else 2_046_505,
        ),
    }
    return build_plan(
        profile=args.profile,
        source_commit=args.source_commit,
        source_archive=ObjectIdentity(
            "source_archive",
            args.source_archive_uri,
            args.source_archive_sha256,
            args.source_archive_bytes,
        ),
        inputs=inputs,
        output_prefix=args.output_prefix,
        image_id=args.image_id,
        image_architecture=args.image_architecture,
        security_group_id=args.security_group_id,
        instance_profile_arn=args.instance_profile_arn,
        targets=DEFAULT_TARGETS,
        spot_price_usd_per_hour_micros=args.spot_price_usd_per_hour_micros,
        vector_bucket=args.vector_bucket,
        split=args.split,
        query_workers=args.query_workers,
        query_order=args.query_order,
        instance_type=args.instance_type,
        metric=args.metric,
    )


def main(argv: Sequence[str] | None = None) -> None:
    import boto3

    plan = parse_args(argv)
    session = boto3.Session(profile_name=plan.profile, region_name="eu-central-1")
    s3_client = session.client("s3")
    ec2_client = session.client("ec2")
    ensure_unstarted(plan, s3_client=s3_client)
    claim_reserved_attempt(plan, s3_client=s3_client)
    instance_id = launch_one_spot(plan, ec2_client=ec2_client)
    try:
        claim_launched_attempt(plan, s3_client=s3_client, instance_id=instance_id)
    except Exception:
        ec2_client.terminate_instances(InstanceIds=[instance_id])
        raise
    terminal = monitor_and_terminate(
        plan,
        s3_client=s3_client,
        ec2_client=ec2_client,
        instance_id=instance_id,
    )
    print(
        json.dumps(
            {
                "claim_eligible": terminal.claim_eligible,
                "instance_id": instance_id,
                "status": terminal.status,
            },
            separators=(",", ":"),
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
