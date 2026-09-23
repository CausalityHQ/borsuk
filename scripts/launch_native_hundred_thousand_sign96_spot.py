#!/usr/bin/env python3
"""Launch one immutable sign96/source 100k decision on Causality Spot."""

from __future__ import annotations

import argparse
import dataclasses
import hashlib
import json
import time
from collections.abc import Sequence

from scripts.launch_native_geometric_layout_spot import (
    DEFAULT_TARGETS,
    FROZEN_QUERIES,
    FROZEN_SOURCE,
    FROZEN_TRUTH,
    SourceArchiveIdentity,
    SpotLayoutPlan,
    _atomic_put,
    _s3_location,
    _terminate_and_wait,
)
from scripts.launch_native_geometric_layout_spot import (
    build_launch_specs as geometric_launch_specs,
)
from scripts.launch_native_geometric_layout_spot import (
    build_plan as geometric_build_plan,
)
from scripts.launch_native_rotated_two_bit_spot import _instance_state
from scripts.native_hundred_thousand_sign96_worker import (
    ARTIFACT_FILES,
    sign96_worker_script,
)


def build_plan(**values: object) -> SpotLayoutPlan:
    attempt = values.get("attempt", 1)
    actual_prefix = values.get("output_prefix")
    if type(attempt) is not int or not 1 <= attempt <= 99 or type(actual_prefix) is not str:
        raise ValueError("sign96 Spot attempt or output prefix differs")
    suffix = f"/runs/relaion-100k-dev1000-a{attempt:04d}"
    if not actual_prefix.rstrip("/").endswith(suffix):
        raise ValueError("sign96 Spot attempt suffix differs")
    adjusted = dict(values)
    adjusted["attempt"] = 1
    adjusted["output_prefix"] = actual_prefix.rstrip("/").removesuffix(suffix) + (
        "/runs/relaion-100k-dev1000-a0001"
    )
    base = geometric_build_plan(**adjusted)
    plan = dataclasses.replace(base, attempt=attempt, output_prefix=actual_prefix)
    expected = (
        "s3://borsuk-bench-453182569524-euc1/research/native-hundred-thousand-sign96/"
        + plan.source_commit
        + f"/runs/relaion-100k-dev1000-a{attempt:04d}"
    )
    if plan.output_prefix.rstrip("/") != expected:
        raise ValueError("sign96 immutable output prefix differs")
    return plan


def build_launch_specs(plan: SpotLayoutPlan) -> list[dict[str, object]]:
    specs = geometric_launch_specs(plan)
    user_data = sign96_worker_script(plan)
    for spec in specs:
        zone = spec["NetworkInterfaces"][0]["SubnetId"]
        token = hashlib.sha256(
            f"sign96:{plan.source_commit}:{zone}:a{plan.attempt:04d}".encode()
        ).hexdigest()[:32]
        spec["ClientToken"] = "native-sign96-" + token
        spec["UserData"] = user_data
        tags = spec["TagSpecifications"][0]["Tags"]
        tags[0]["Value"] = "borsuk-native-sign96-range"
        tags[1]["Value"] = f"a{plan.attempt:04d}"
    return specs


def validate_terminal_bytes(
    body: bytes, plan: SpotLayoutPlan, instance_id: str,
) -> dict[str, object]:
    try:
        terminal = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("sign96 terminal JSON differs") from error
    canonical = (json.dumps(terminal, sort_keys=True, separators=(",", ":")) + "\n").encode()
    if (
        body != canonical
        or type(terminal) is not dict
        or set(terminal) != {
            "artifacts", "attempt", "claim_eligible", "decision",
            "elapsed_seconds", "exit_code", "failure_reason", "instance_id", "phase",
            "schema", "source_commit", "source_archive",
            "requirements_sha256", "status",
        }
        or terminal["schema"] != "borsuk-hundred-thousand-sign96-terminal-v1"
        or terminal["attempt"] != plan.attempt
        or terminal["claim_eligible"] is not False
        or terminal["source_commit"] != plan.source_commit
        or terminal["source_archive"] != dataclasses.asdict(plan.source_archive)
        or terminal["requirements_sha256"] != plan.requirements_sha256
        or terminal["instance_id"] != instance_id
        or type(terminal["elapsed_seconds"]) is not int
        or terminal["elapsed_seconds"] < 0
        or type(terminal["exit_code"]) is not int
    ):
        raise ValueError("sign96 terminal authority differs")
    complete = terminal["status"] == terminal["phase"] == "complete" and terminal["exit_code"] == 0
    if not complete:
        if (
            terminal["status"] != "failed" or terminal["exit_code"] == 0
            or terminal["artifacts"] != {} or terminal["decision"] != ""
            or terminal["failure_reason"] not in ("", "resource_cap", "phase_timeout")
        ):
            raise ValueError("sign96 failed terminal differs")
        return terminal
    if terminal["decision"] not in ("advance", "reject") or terminal["failure_reason"] != "":
        raise ValueError("sign96 terminal decision differs")
    artifacts = terminal["artifacts"]
    if type(artifacts) is not dict or set(artifacts) != set(ARTIFACT_FILES):
        raise ValueError("sign96 terminal artifact roster differs")
    for role, path in ARTIFACT_FILES.items():
        identity = artifacts[role]
        if (
            type(identity) is not dict
            or set(identity) != {"role", "uri", "sha256", "encoded_bytes"}
            or identity["role"] != role
            or identity["uri"] != plan.output_prefix.rstrip("/") + "/artifacts/" + path
            or type(identity["encoded_bytes"]) is not int
            or identity["encoded_bytes"] <= 0
            or type(identity["sha256"]) is not str
            or len(identity["sha256"]) != 64
            or any(character not in "0123456789abcdef" for character in identity["sha256"])
        ):
            raise ValueError("sign96 terminal artifact identity differs")
    return terminal


def launch_and_monitor(plan: SpotLayoutPlan) -> dict[str, object]:
    import boto3

    session = boto3.Session(profile_name=plan.profile, region_name="eu-central-1")
    ec2 = session.client("ec2")
    s3 = session.client("s3")
    specs = build_launch_specs(plan)
    bucket, prefix = _s3_location(plan.output_prefix)
    existing = s3.list_objects_v2(Bucket=bucket, Prefix=f"{prefix}/", MaxKeys=1)
    if existing.get("KeyCount", 0) or existing.get("Contents"):
        raise ValueError("sign96 immutable attempt already exists")
    reservation = {
        "schema": "borsuk-hundred-thousand-sign96-reservation-v1",
        "attempt": plan.attempt,
        "source_commit": plan.source_commit,
        "source_archive": dataclasses.asdict(plan.source_archive),
        "requirements_sha256": plan.requirements_sha256,
    }
    _atomic_put(
        s3, bucket=bucket, key=f"{prefix}/reservation.json",
        body=(json.dumps(reservation, sort_keys=True, separators=(",", ":")) + "\n").encode(),
    )
    instance_id = None
    capacity_markers = ("InsufficientInstanceCapacity", "MaxSpotInstanceCountExceeded")
    for spec in specs:
        try:
            response = ec2.run_instances(**spec)
        except Exception as error:
            if any(marker in str(error) for marker in capacity_markers):
                continue
            raise
        instance_id = response["Instances"][0]["InstanceId"]
        break
    if instance_id is None:
        raise RuntimeError("sign96 Spot capacity unavailable; no instance launched")
    deadline = time.monotonic() + plan.wall_seconds * 4 + 900
    try:
        while True:
            try:
                response = s3.get_object(Bucket=bucket, Key=f"{prefix}/terminal.json")
            except Exception as error:
                code = str(getattr(error, "response", {}).get("Error", {}).get("Code", ""))
                if code in {"AccessDenied", "InvalidAccessKeyId", "ExpiredToken"}:
                    raise
                try:
                    state = _instance_state(ec2, instance_id)
                except Exception as state_error:
                    state_code = str(
                        getattr(state_error, "response", {}).get("Error", {}).get("Code", "")
                    )
                    if state_code in {"UnauthorizedOperation", "AuthFailure", "ExpiredToken"}:
                        raise
                    state = None
                if state in {"terminated", "shutting-down", "stopped"}:
                    # Shutdown can race the worker's terminal upload. Recheck
                    # through the ordinary deadline instead of ending on one read.
                    if state in {"terminated", "stopped"}:
                        try:
                            response = s3.get_object(Bucket=bucket, Key=f"{prefix}/terminal.json")
                        except Exception:
                            raise RuntimeError(
                                f"sign96 instance {instance_id} ended without terminal"
                            ) from error
                        return validate_terminal_bytes(response["Body"].read(), plan, instance_id)
                if time.monotonic() >= deadline:
                    raise TimeoutError("sign96 terminal deadline exceeded") from error
                time.sleep(15)
                continue
            return validate_terminal_bytes(response["Body"].read(), plan, instance_id)
    finally:
        _terminate_and_wait(ec2, instance_id)


def parse_args(argv: Sequence[str] | None = None) -> SpotLayoutPlan:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--source-archive-uri", required=True)
    parser.add_argument("--source-archive-sha256", required=True)
    parser.add_argument("--source-archive-bytes", type=int, required=True)
    parser.add_argument("--requirements-sha256", required=True)
    parser.add_argument("--output-prefix", required=True)
    parser.add_argument("--attempt", type=int, default=1)
    parser.add_argument("--image-id", default="ami-06121aa3085b6f918")
    parser.add_argument("--security-group-id", default="sg-0b1fd3e4fbde4af0d")
    parser.add_argument(
        "--instance-profile-arn",
        default="arn:aws:iam::453182569524:instance-profile/borsuk-bench-profile",
    )
    args = parser.parse_args(argv)
    return build_plan(
        profile="causality", source_commit=args.source_commit,
        source_archive=SourceArchiveIdentity(
            args.source_archive_uri, args.source_archive_sha256, args.source_archive_bytes,
        ),
        source=FROZEN_SOURCE, queries=FROZEN_QUERIES, truth=FROZEN_TRUTH,
        requirements_sha256=args.requirements_sha256,
        output_prefix=args.output_prefix, attempt=args.attempt,
        image_id=args.image_id, security_group_id=args.security_group_id,
        instance_profile_arn=args.instance_profile_arn, targets=DEFAULT_TARGETS,
    )


def main(argv: Sequence[str] | None = None) -> None:
    terminal = launch_and_monitor(parse_args(argv))
    print(json.dumps(terminal, sort_keys=True, separators=(",", ":")))
    if terminal["status"] != "complete":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
