#!/usr/bin/env python3
"""Launch and monitor one immutable V98 G1 attempt on Causality EC2 Spot."""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import pathlib
import shlex
import sys
import time
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from typing import Literal

if not __package__:  # Direct ``python scripts/...`` execution.
    sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from scripts.v97_row_width_screen import ObjectIdentity
from scripts.v98_hierarchical_row_router import HierarchyConfig

CRITIQUE_RESULT_SHA256 = (
    "edcde2149f98bc38c5121b387b165fe9baf0ca6df79658ac624abf86866bcb87"
)
FROZEN_INPUTS = {
    "source": ObjectIdentity(
        "s3://borsuk-bench-453182569524-euc1/research/v36-prefix-screen/runs/v36-prefix-screen-20260908T174540Z-31445a91/attempt-0000/source.parquet",
        "2796b579f37afe99ca4aff57e282335a6a79ad30596645957d26326a0560cf86",
        1_458_450_077,
    ),
    "queries": ObjectIdentity(
        "s3://borsuk-bench-453182569524-euc1/research/v36-prefix-screen/runs/v36-prefix-screen-20260908T174540Z-31445a91/attempt-0000/development-query.parquet",
        "310bb54f79f2e79d09fe63aa4f6b5c6e9e7ffb31101964f816be978dadb2db54",
        1_558_506,
    ),
    "truth": ObjectIdentity(
        "s3://borsuk-bench-453182569524-euc1/research/v36-prefix-screen/runs/v36-prefix-screen-20260908T174540Z-31445a91/attempt-0000/development-gt100.parquet",
        "fed7524fd675087f42b48b2f7fa9192b4661aaa4b665600de8378b8b6c696e11",
        2_046_505,
    ),
    "generation": ObjectIdentity(
        "s3://borsuk-bench-453182569524-euc1/research/v85-pq16-1m-validation/da42b3da7a78a7a44ab259bb06f12c42527e8800/runs/v85-1m-validation-20260920T100638Z-da42b3d/a0001/artifacts/generation.json",
        "45fa4e708ab660151a7b1ea79e35eada6090faced1bfb147f7e20cac7055e754",
        416_563,
    ),
    "base": ObjectIdentity(
        "s3://borsuk-bench-453182569524-euc1/research/v85-pq16-1m-validation/da42b3da7a78a7a44ab259bb06f12c42527e8800/runs/v85-1m-validation-20260920T100638Z-da42b3d/a0001/artifacts/base-000.arrow",
        "c2e86f6199777c21dde048ff4a46aae65ce96a4b20f03c11905851c80312b862",
        708_888_104,
    ),
    "delta": ObjectIdentity(
        "s3://borsuk-bench-453182569524-euc1/research/v85-pq16-1m-validation/da42b3da7a78a7a44ab259bb06f12c42527e8800/runs/v85-1m-validation-20260920T100638Z-da42b3d/a0001/artifacts/delta-000.arrow",
        "a0498ff17acbe8cc81a8b2501e7ffcd0d6998c5379efaae5895c64d3bcc7e869",
        80_900_000,
    ),
}


@dataclass(frozen=True, slots=True)
class SpotTarget:
    availability_zone: str
    subnet_id: str


DEFAULT_TARGETS = (
    SpotTarget("eu-central-1c", "subnet-0a12dbed0ca6fac25"),
    SpotTarget("eu-central-1b", "subnet-00243d923761c047c"),
    SpotTarget("eu-central-1a", "subnet-034528fbd6977848f"),
)


def _config() -> HierarchyConfig:
    return HierarchyConfig(
        pages_per_root=8,
        maximum_root_groups=65_536,
        maximum_exposed_pages=4_096,
        retained_pages=1_024,
        maximum_scanned_rows=262_144,
        shortlist_rows=2_048,
        maximum_gets=32,
        maximum_bytes=16 * 1024**2,
    )


@dataclass(frozen=True, slots=True)
class V98SpotPlan:
    profile: str
    source_commit: str
    source_archive: ObjectIdentity
    inputs: Mapping[str, ObjectIdentity]
    critique_result_sha256: str
    output_prefix: str
    image_id: str
    image_architecture: str
    security_group_id: str
    instance_profile_arn: str
    targets: tuple[SpotTarget, ...]
    spot_price_usd_per_hour_micros: int
    config: HierarchyConfig = _config()
    attempt: int = 1
    query_count: int = 1_000
    bootstrap_resamples: int = 10_000
    instance_type: str = "c7i.8xlarge"
    wall_seconds: int = 7_200


@dataclass(frozen=True, slots=True)
class AttemptTerminal:
    schema: str
    attempt: int
    status: Literal["complete", "failed", "interrupted"]
    claim_eligible: bool
    source_commit: str
    critique_result_sha256: str
    instance_id: str
    exit_code: int
    spot_price_usd_per_hour_micros: int
    evidence: Mapping[str, ObjectIdentity]


def derive_attempt_prefix(campaign: str, source_commit: str, timestamp: str) -> str:
    """Derive the sole immutable attempt prefix from registered inputs."""

    if (
        not campaign
        or "/" in campaign
        or len(source_commit) != 40
        or any(character not in "0123456789abcdef" for character in source_commit)
        or len(timestamp) != 16
        or not timestamp.endswith("Z")
        or not timestamp[:8].isdigit()
        or timestamp[8] != "T"
        or not timestamp[9:15].isdigit()
    ):
        raise ValueError("V98 attempt prefix differs")
    return f"{campaign}/{source_commit}/{timestamp}/a0001"


def build_plan(**values: object) -> V98SpotPlan:
    """Freeze and validate one claim-eligible V98 Spot plan."""

    plan = V98SpotPlan(**values)
    expected_roles = {"source", "queries", "truth", "generation", "base", "delta"}
    protected = " ".join(plan.inputs[role].uri for role in ("queries", "truth")).lower()
    if (
        plan.profile != "causality"
        or set(plan.inputs) != expected_roles
        or plan.critique_result_sha256 != CRITIQUE_RESULT_SHA256
        or not plan.output_prefix.startswith("s3://")
        or not plan.image_id.startswith("ami-")
        or plan.image_architecture != "x86_64"
        or not plan.instance_type.startswith(("c7i.", "m7i.", "r7i."))
        or not plan.security_group_id.startswith("sg-")
        or not plan.instance_profile_arn.startswith("arn:aws:iam::")
        or not plan.targets
        or len(set(plan.targets)) != len(plan.targets)
        or plan.attempt != 1
        or plan.query_count != 1_000
        or plan.bootstrap_resamples != 10_000
        or plan.wall_seconds != 7_200
        or plan.spot_price_usd_per_hour_micros <= 0
        or plan.config != _config()
        or "validation" in protected
        or "holdout" in protected
    ):
        raise ValueError("V98 Spot plan differs")
    return plan


def _q(value: object) -> str:
    return shlex.quote(str(value))


def worker_script(plan: V98SpotPlan) -> str:
    """Bind a plan to a size-bounded authenticated bootstrap."""
    exports = {
        "V98_ATTEMPT": plan.attempt,
        "V98_BOOTSTRAP_RESAMPLES": plan.bootstrap_resamples,
        "V98_CRITIQUE_SHA256": plan.critique_result_sha256,
        "V98_MAXIMUM_BYTES": plan.config.maximum_bytes,
        "V98_MAXIMUM_EXPOSED_PAGES": plan.config.maximum_exposed_pages,
        "V98_MAXIMUM_GETS": plan.config.maximum_gets,
        "V98_MAXIMUM_ROOT_GROUPS": plan.config.maximum_root_groups,
        "V98_MAXIMUM_SCANNED_ROWS": plan.config.maximum_scanned_rows,
        "V98_OUTPUT_PREFIX": plan.output_prefix.rstrip("/"),
        "V98_PAGES_PER_ROOT": plan.config.pages_per_root,
        "V98_QUERY_COUNT": plan.query_count,
        "V98_RETAINED_PAGES": plan.config.retained_pages,
        "V98_SHORTLIST_ROWS": plan.config.shortlist_rows,
        "V98_SOURCE_ARCHIVE_BYTES": plan.source_archive.bytes,
        "V98_SOURCE_ARCHIVE_SHA256": plan.source_archive.sha256,
        "V98_SOURCE_ARCHIVE_URI": plan.source_archive.uri,
        "V98_SOURCE_COMMIT": plan.source_commit,
        "V98_SPOT_PRICE_MICROS": plan.spot_price_usd_per_hour_micros,
        "V98_WALL_SECONDS": plan.wall_seconds,
    }
    for role, identity in sorted(plan.inputs.items()):
        prefix = f"V98_{role.upper()}"
        exports[f"{prefix}_BYTES"] = identity.bytes
        exports[f"{prefix}_SHA256"] = identity.sha256
        exports[f"{prefix}_URI"] = identity.uri
    header = "\n".join(
        f"export {name}={_q(value)}" for name, value in sorted(exports.items())
    )
    bootstrap = """set -euo pipefail
root=/mnt/v98-hierarchical-g1
mkdir -p "$root" && cd "$root"
aws s3 cp "$V98_SOURCE_ARCHIVE_URI" source.tar.gz --only-show-errors
[ "$(stat -c%s source.tar.gz)" = "$V98_SOURCE_ARCHIVE_BYTES" ]
printf '%s  source.tar.gz\\n' "$V98_SOURCE_ARCHIVE_SHA256" >hashes.txt
sha256sum -c hashes.txt
mkdir repo
tar -xzf source.tar.gz -C repo
exec bash repo/scripts/v98_hierarchical_row_router_run_remote.sh
"""
    script = f"#!/bin/bash\n{header}\n{bootstrap}"
    if len(script.encode()) > 16_384:
        raise ValueError("V98 user data exceeds EC2 limit")
    return script


def build_launch_specs(plan: V98SpotPlan) -> list[dict[str, object]]:
    """Build serial multi-AZ one-time Spot requests for one attempt."""

    encoded = base64.b64encode(worker_script(plan).encode()).decode()
    specs: list[dict[str, object]] = []
    for target in plan.targets:
        token_material = f"v98:{plan.source_commit}:{target.availability_zone}:a0001"
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
                "ClientToken": "v98-"
                + hashlib.sha256(token_material.encode()).hexdigest()[:48],
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
                "TagSpecifications": [
                    {
                        "ResourceType": "instance",
                        "Tags": [
                            {"Key": "Name", "Value": "borsuk-v98-g1"},
                            {"Key": "BorsukAttempt", "Value": "a0001"},
                        ],
                    }
                ],
                "UserData": encoded,
            }
        )
    return specs


def launch_one_spot(plan: V98SpotPlan, *, ec2_client: object) -> str:
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
            raise RuntimeError("V98 Spot launch response differs")
        return instances[0]["InstanceId"]
    raise RuntimeError(f"V98 Spot capacity unavailable in {failures} zones")


def _s3_location(uri: str) -> tuple[str, str]:
    without_scheme = uri.removeprefix("s3://")
    bucket, separator, key = without_scheme.partition("/")
    if not separator or not bucket or not key:
        raise ValueError("V98 S3 URI differs")
    return bucket, key.rstrip("/")


def _atomic_put(s3_client: object, *, bucket: str, key: str, body: bytes) -> None:
    event_name = "before-sign.s3.PutObject"
    event_id = f"borsuk-v98-{hashlib.sha256(key.encode()).hexdigest()[:16]}"

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
        s3_client.meta.events.unregister(event_name, event_id)


def _launch_receipt(plan: V98SpotPlan, instance_id: str | None) -> bytes:
    value = {
        "attempt": plan.attempt,
        "critique_result_sha256": plan.critique_result_sha256,
        "inputs": {
            role: asdict(identity) for role, identity in sorted(plan.inputs.items())
        },
        "instance_id": instance_id,
        "schema": "borsuk-v98-launch-v1",
        "source_archive": asdict(plan.source_archive),
        "source_commit": plan.source_commit,
    }
    return (
        json.dumps(
            value, allow_nan=False, separators=(",", ":"), sort_keys=True
        ).encode()
        + b"\n"
    )


def claim_reserved_attempt(plan: V98SpotPlan, *, s3_client: object) -> None:
    """Atomically reserve the prefix before creating compute."""

    bucket, prefix = _s3_location(plan.output_prefix)
    _atomic_put(
        s3_client,
        bucket=bucket,
        key=f"{prefix}/reservation.json",
        body=_launch_receipt(plan, None),
    )


def claim_launched_attempt(
    plan: V98SpotPlan, *, s3_client: object, instance_id: str
) -> None:
    """Persist the exact launched instance under a create-only key."""

    if not instance_id.startswith("i-"):
        raise ValueError("V98 instance identity differs")
    bucket, prefix = _s3_location(plan.output_prefix)
    _atomic_put(
        s3_client,
        bucket=bucket,
        key=f"{prefix}/launch.json",
        body=_launch_receipt(plan, instance_id),
    )


def canonical_terminal_bytes(
    plan: V98SpotPlan,
    *,
    instance_id: str,
    status: Literal["complete", "failed", "interrupted"],
    exit_code: int,
    evidence: Mapping[str, ObjectIdentity],
) -> bytes:
    """Encode one exact terminal classification."""

    required = {"result", "rescore", "resources", "worker_log"}
    if (
        not instance_id.startswith("i-")
        or status not in ("complete", "failed", "interrupted")
        or type(exit_code) is not int
        or (status == "complete" and (exit_code != 0 or set(evidence) != required))
        or (status != "complete" and (exit_code == 0 or evidence))
    ):
        raise ValueError("V98 terminal differs")
    terminal = AttemptTerminal(
        schema="borsuk-v98-spot-terminal-v1",
        attempt=plan.attempt,
        status=status,
        claim_eligible=status == "complete",
        source_commit=plan.source_commit,
        critique_result_sha256=plan.critique_result_sha256,
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
    body: bytes, plan: V98SpotPlan, instance_id: str
) -> AttemptTerminal:
    """Validate terminal bytes without reading any partial evidence object."""

    try:
        value = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("V98 terminal JSON differs") from error
    if (
        type(value) is not dict
        or json.dumps(
            value, allow_nan=False, separators=(",", ":"), sort_keys=True
        ).encode()
        + b"\n"
        != body
    ):
        raise ValueError("V98 terminal canonical bytes differ")
    expected_keys = {
        "schema",
        "attempt",
        "status",
        "claim_eligible",
        "source_commit",
        "critique_result_sha256",
        "instance_id",
        "exit_code",
        "spot_price_usd_per_hour_micros",
        "evidence",
    }
    if set(value) != expected_keys or type(value.get("evidence")) is not dict:
        raise ValueError("V98 terminal schema differs")
    evidence = {
        role: ObjectIdentity(**identity) for role, identity in value["evidence"].items()
    }
    expected = canonical_terminal_bytes(
        plan,
        instance_id=instance_id,
        status=value["status"],
        exit_code=value["exit_code"],
        evidence=evidence,
    )
    if expected != body:
        raise ValueError("V98 terminal authority differs")
    return AttemptTerminal(
        schema=value["schema"],
        attempt=value["attempt"],
        status=value["status"],
        claim_eligible=value["claim_eligible"],
        source_commit=value["source_commit"],
        critique_result_sha256=value["critique_result_sha256"],
        instance_id=value["instance_id"],
        exit_code=value["exit_code"],
        spot_price_usd_per_hour_micros=value["spot_price_usd_per_hour_micros"],
        evidence=evidence,
    )


def monitor_and_terminate(
    plan: V98SpotPlan,
    *,
    s3_client: object,
    ec2_client: object,
    instance_id: str,
    poll_seconds: float = 15.0,
    monotonic: object = time.monotonic,
    sleep: object = time.sleep,
) -> AttemptTerminal:
    """Observe only the terminal marker and always terminate the instance."""

    bucket, prefix = _s3_location(plan.output_prefix)
    deadline = monotonic() + plan.wall_seconds + 900  # type: ignore[operator]
    try:
        while True:
            try:
                response = s3_client.get_object(
                    Bucket=bucket, Key=f"{prefix}/terminal.json"
                )
            except Exception as error:
                if "without terminal" in str(error):
                    raise
                code = str(
                    getattr(error, "response", {}).get("Error", {}).get("Code", "")
                )
                if code not in ("404", "NoSuchKey", "NotFound") and not any(
                    marker in str(error) for marker in ("404", "NoSuchKey", "NotFound")
                ):
                    raise
                state_response = ec2_client.describe_instances(
                    InstanceIds=[instance_id]
                )
                reservations = state_response.get("Reservations", [])
                state = reservations[0]["Instances"][0]["State"]["Name"]
                if state in ("terminated", "stopped", "shutting-down"):
                    raise RuntimeError(
                        f"instance {state} without terminal marker"
                    ) from error
                if monotonic() >= deadline:  # type: ignore[operator]
                    raise TimeoutError(
                        "V98 attempt exceeded terminal deadline"
                    ) from error
                sleep(poll_seconds)  # type: ignore[operator]
                continue
            body = response["Body"].read()
            if response.get("ContentLength") != len(body):
                raise ValueError("V98 terminal object length differs")
            return validate_terminal_bytes(body, plan, instance_id)
    finally:
        ec2_client.terminate_instances(InstanceIds=[instance_id])


def ensure_unstarted(plan: V98SpotPlan, *, s3_client: object) -> None:
    """Reject a prefix containing any reservation, launch, or terminal."""

    bucket, prefix = _s3_location(plan.output_prefix)
    for name in ("reservation.json", "launch.json", "terminal.json"):
        try:
            s3_client.head_object(Bucket=bucket, Key=f"{prefix}/{name}")
        except Exception as error:
            code = str(getattr(error, "response", {}).get("Error", {}).get("Code", ""))
            if code in ("404", "NoSuchKey", "NotFound") or any(
                marker in str(error) for marker in ("404", "NoSuchKey", "NotFound")
            ):
                continue
            raise
        raise ValueError("V98 immutable attempt already exists")


def parse_args(argv: Sequence[str] | None = None) -> V98SpotPlan:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--source-archive-uri", required=True)
    parser.add_argument("--source-archive-sha256", required=True)
    parser.add_argument("--source-archive-bytes", type=int, required=True)
    parser.add_argument("--output-prefix", required=True)
    parser.add_argument("--profile", default="causality")
    parser.add_argument("--image-id", default="ami-06121aa3085b6f918")
    parser.add_argument("--image-architecture", default="x86_64")
    parser.add_argument("--security-group-id", default="sg-0b1fd3e4fbde4af0d")
    parser.add_argument(
        "--instance-profile-arn",
        default="arn:aws:iam::453182569524:instance-profile/borsuk-bench-profile",
    )
    parser.add_argument("--spot-price-usd-per-hour-micros", type=int, default=480_000)
    args = parser.parse_args(argv)
    return build_plan(
        profile=args.profile,
        source_commit=args.source_commit,
        source_archive=ObjectIdentity(
            args.source_archive_uri,
            args.source_archive_sha256,
            args.source_archive_bytes,
        ),
        inputs=FROZEN_INPUTS,
        critique_result_sha256=CRITIQUE_RESULT_SHA256,
        output_prefix=args.output_prefix,
        image_id=args.image_id,
        image_architecture=args.image_architecture,
        security_group_id=args.security_group_id,
        instance_profile_arn=args.instance_profile_arn,
        targets=DEFAULT_TARGETS,
        spot_price_usd_per_hour_micros=args.spot_price_usd_per_hour_micros,
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
