#!/usr/bin/env python3
"""Launch exactly one immutable V97 development screen on EC2 Spot."""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import shlex
from collections.abc import Mapping, Sequence
from dataclasses import dataclass

from scripts.v97_row_width_screen import ObjectIdentity

CRITIQUE_RESULT_SHA256 = "edcde2149f98bc38c5121b387b165fe9baf0ca6df79658ac624abf86866bcb87"
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


@dataclass(frozen=True, slots=True)
class V97SpotPlan:
    profile: str
    source_commit: str
    source_archive: ObjectIdentity
    inputs: Mapping[str, ObjectIdentity]
    critique_result_sha256: str
    output_prefix: str
    image_id: str
    security_group_id: str
    instance_profile_arn: str
    targets: tuple[SpotTarget, ...]
    attempt: int = 1
    query_count: int = 1_000
    bootstrap_resamples: int = 10_000
    instance_type: str = "c7i.8xlarge"
    wall_seconds: int = 7_200


def build_plan(**values: object) -> V97SpotPlan:
    """Freeze and validate one claim-eligible G1 Spot attempt."""

    plan = V97SpotPlan(**values)
    expected_roles = {"source", "queries", "truth", "generation", "base", "delta"}
    protected = " ".join(
        plan.inputs[role].uri for role in ("queries", "truth")
    ).lower()
    if (
        plan.profile != "causality"
        or set(plan.inputs) != expected_roles
        or not plan.output_prefix.startswith("s3://")
        or not plan.image_id.startswith("ami-")
        or not plan.security_group_id.startswith("sg-")
        or not plan.instance_profile_arn.startswith("arn:aws:iam::")
        or not plan.targets
        or len(set(plan.targets)) != len(plan.targets)
        or plan.attempt != 1
        or plan.query_count != 1_000
        or plan.bootstrap_resamples != 10_000
        or plan.wall_seconds != 7_200
        or "validation-query" in protected
        or "validation-gt" in protected
        or "holdout" in protected
    ):
        raise ValueError("V97 Spot plan differs")
    return plan


def _q(value: object) -> str:
    return shlex.quote(str(value))


def worker_script(plan: V97SpotPlan) -> str:
    """Build the exact fail-closed worker with terminal-before-shutdown."""

    identity_lines = []
    argument_lines = []
    for role in ("source", "queries", "truth", "generation", "base", "delta"):
        identity = plan.inputs[role]
        filename = {
            "source": "source.parquet",
            "queries": "queries.parquet",
            "truth": "truth.parquet",
            "generation": "generation.json",
            "base": "base.arrow",
            "delta": "delta.arrow",
        }[role]
        identity_lines.extend(
            (
                f"aws s3 cp {_q(identity.uri)} {filename} --only-show-errors || exit 96",
                f"printf '%s  %s\\n' {_q(identity.sha256)} {filename} >> hashes.txt",
            )
        )
        argument_lines.append(
            f"--{role} {filename} --{role}-uri {_q(identity.uri)} "
            f"--{role}-sha256 {identity.sha256} --{role}-bytes {identity.bytes}"
        )
    screen_arguments = (" \\" + "\n  ").join(argument_lines)
    return f"""#!/bin/bash
set -uo pipefail
root=/mnt/v97-row-width-g1
phase=bootstrap
attempt=1
output_prefix={_q(plan.output_prefix.rstrip('/'))}
started_epoch=$(date +%s)
pressure_start=$(tr '\n' ';' </proc/pressure/memory)
swap_start_kib=$(awk '/^SwapTotal:/{{t=$2}} /^SwapFree:/{{f=$2}} END{{print t-f}}' /proc/meminfo)

finish() {{
  code=$?
  trap - EXIT
  set +e
  cd "$root" 2>/dev/null || true
  instance_id=unknown
  token=$(curl -fsS -X PUT -H 'X-aws-ec2-metadata-token-ttl-seconds: 60' http://169.254.169.254/latest/api/token 2>/dev/null)
  [ -n "$token" ] && instance_id=$(curl -fsS -H "X-aws-ec2-metadata-token: $token" http://169.254.169.254/latest/meta-data/instance-id 2>/dev/null)
  finished_epoch=$(date +%s)
  pressure_end=$(tr '\n' ';' </proc/pressure/memory)
  swap_end_kib=$(awk '/^SwapTotal:/{{t=$2}} /^SwapFree:/{{f=$2}} END{{print t-f}}' /proc/meminfo)
  screen_rss=$(awk -F: '/Maximum resident set size/{{gsub(/ /,"",$2); print $2}}' screen.time 2>/dev/null)
  rescore_rss=$(awk -F: '/Maximum resident set size/{{gsub(/ /,"",$2); print $2}}' rescore.time 2>/dev/null)
  max_rss_kib=${{screen_rss:-0}}
  [ "${{rescore_rss:-0}}" -gt "$max_rss_kib" ] 2>/dev/null && max_rss_kib=$rescore_rss
  STARTED_EPOCH="$started_epoch" FINISHED_EPOCH="$finished_epoch" MAX_RSS_KIB="$max_rss_kib" PRESSURE_START="$pressure_start" PRESSURE_END="$pressure_end" SWAP_START_KIB="$swap_start_kib" SWAP_END_KIB="$swap_end_kib" python3 - <<'PY'
import json, os
from pathlib import Path
value={{
    "elapsed_seconds":int(os.environ["FINISHED_EPOCH"])-int(os.environ["STARTED_EPOCH"]),
    "finished_epoch":int(os.environ["FINISHED_EPOCH"]),
    "max_process_rss_kib":int(os.environ["MAX_RSS_KIB"]),
    "memory_pressure_end":os.environ["PRESSURE_END"],
    "memory_pressure_start":os.environ["PRESSURE_START"],
    "started_epoch":int(os.environ["STARTED_EPOCH"]),
    "swap_end_kib":int(os.environ["SWAP_END_KIB"]),
    "swap_start_kib":int(os.environ["SWAP_START_KIB"]),
}}
Path("resources.json").write_bytes(json.dumps(value,separators=(",",":"),sort_keys=True).encode()+b"\\n")
PY
  for name in result.json rescore.json resources.json screen.time rescore.time screen.log rescore.log hashes.txt; do
    [ -f "$name" ] && aws s3 cp "$name" "$output_prefix/evidence/$name" --only-show-errors
  done
  INSTANCE_ID="$instance_id" EXIT_CODE="$code" PHASE="$phase" python3 - <<'PY'
import hashlib, json, os
from pathlib import Path
value={{
    "attempt":1,
    "exit_code":int(os.environ["EXIT_CODE"]),
    "instance_id":os.environ["INSTANCE_ID"],
    "phase":os.environ["PHASE"],
    "resources_sha256":hashlib.sha256(Path("resources.json").read_bytes()).hexdigest() if Path("resources.json").is_file() else None,
    "rescore_sha256":hashlib.sha256(Path("rescore.json").read_bytes()).hexdigest() if Path("rescore.json").is_file() else None,
    "result_sha256":hashlib.sha256(Path("result.json").read_bytes()).hexdigest() if Path("result.json").is_file() else None,
    "source_commit":"{plan.source_commit}",
    "status":"complete" if int(os.environ["EXIT_CODE"]) == 0 else "failed",
}}
Path("terminal.json").write_bytes(json.dumps(value,separators=(",",":"),sort_keys=True).encode()+b"\\n")
PY
  aws s3 cp terminal.json "$output_prefix/terminal.json" --only-show-errors
  shutdown -h now || true
  exit "$code"
}}
trap finish EXIT
shutdown --poweroff +125
mkdir -p "$root" && cd "$root" || exit 90
ulimit -v $((48 * 1024 * 1024))
export HOME=${{HOME:-/root}}
export OMP_NUM_THREADS=16 OPENBLAS_NUM_THREADS=16 MKL_NUM_THREADS=16

phase=install
dnf install -y -q python3-pip tar gzip time >install.log 2>&1 || exit 91
python3 -m venv .venv >>install.log 2>&1 || exit 91
.venv/bin/pip install -q numpy==1.26.4 pyarrow==17.0.0 >>install.log 2>&1 || exit 91

phase=source
aws s3 cp {_q(plan.source_archive.uri)} source.tar.gz --only-show-errors || exit 92
printf '%s  source.tar.gz\n' {_q(plan.source_archive.sha256)} > hashes.txt
sha256sum -c hashes.txt || exit 93
mkdir repo && tar -xzf source.tar.gz -C repo || exit 94

phase=inputs
{chr(10).join(identity_lines)}
sha256sum -c hashes.txt || exit 97

phase=screen
export PYTHONPATH="$root/repo"
/usr/bin/time -v -o screen.time timeout {plan.wall_seconds} .venv/bin/python repo/scripts/v97_row_width_screen.py \
  {screen_arguments} \
  --source-commit {plan.source_commit} \
  --critique-result-sha256 {plan.critique_result_sha256} \
  --dimensions 768 --neighbors 100 --query-count 1000 --seed 7216 \
  --output result.json >screen.log 2>&1 || exit 98

phase=rescore
result_sha=$(sha256sum result.json | cut -d' ' -f1)
/usr/bin/time -v -o rescore.time .venv/bin/python repo/scripts/v97_row_width_rescore.py \
  {screen_arguments} \
  --source-commit {plan.source_commit} \
  --critique-result-sha256 {plan.critique_result_sha256} \
  --dimensions 768 --neighbors 100 --query-count 1000 --seed 7216 \
  --bootstrap-resamples 10000 --result result.json --result-sha256 "$result_sha" \
  --output rescore.json >rescore.log 2>&1 || exit 99

phase=complete
exit 0
"""


def build_launch_specs(plan: V97SpotPlan) -> list[dict[str, object]]:
    """Build sequential multi-AZ capacity requests for one original attempt."""

    user_data = base64.b64encode(worker_script(plan).encode()).decode()
    specs = []
    for target in plan.targets:
        token_material = f"v97:{plan.source_commit}:{target.availability_zone}:a0001"
        token = "v97-" + hashlib.sha256(token_material.encode()).hexdigest()[:48]
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
                "ClientToken": token,
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
                            {"Key": "Name", "Value": "borsuk-v97-g1"},
                            {"Key": "BorsukAttempt", "Value": "a0001"},
                        ],
                    }
                ],
                "UserData": user_data,
            }
        )
    return specs


def launch_one_spot(plan: V97SpotPlan, *, ec2_client: object) -> str:
    """Try registered zones serially and launch at most one instance."""

    capacity_markers = (
        "InsufficientInstanceCapacity",
        "InsufficientFreeAddressesInSubnet",
        "MaxSpotInstanceCountExceeded",
        "SpotMaxPriceTooLow",
    )
    failures: list[str] = []
    for request in build_launch_specs(plan):
        try:
            response = ec2_client.run_instances(**request)
        except Exception as error:
            if not any(marker in str(error) for marker in capacity_markers):
                raise
            failures.append(str(error))
            continue
        instances = response.get("Instances") if isinstance(response, dict) else None
        if (
            not isinstance(instances, list)
            or len(instances) != 1
            or not isinstance(instances[0], dict)
            or not isinstance(instances[0].get("InstanceId"), str)
        ):
            raise RuntimeError("V97 Spot launch response differs")
        return instances[0]["InstanceId"]
    raise RuntimeError(f"V97 Spot capacity unavailable in {len(failures)} zones")


def ensure_unstarted(plan: V97SpotPlan, *, s3_client: object) -> None:
    """Reject any output prefix that already has a terminal marker."""

    without_scheme = plan.output_prefix.removeprefix("s3://")
    bucket, separator, prefix = without_scheme.partition("/")
    if not separator or not bucket or not prefix:
        raise ValueError("V97 output prefix differs")
    for name in ("launch.json", "terminal.json"):
        try:
            s3_client.head_object(Bucket=bucket, Key=f"{prefix.rstrip('/')}/{name}")
        except Exception as error:
            response = getattr(error, "response", {})
            code = str(response.get("Error", {}).get("Code", ""))
            if code in ("404", "NoSuchKey", "NotFound") or any(
                marker in str(error) for marker in ("404", "NoSuchKey", "NotFound")
            ):
                continue
            raise
        raise ValueError("V97 immutable attempt already exists")


def claim_launched_attempt(
    plan: V97SpotPlan, *, s3_client: object, instance_id: str
) -> None:
    """Persist the one-attempt fence with an atomic S3 create."""

    without_scheme = plan.output_prefix.removeprefix("s3://")
    bucket, separator, prefix = without_scheme.partition("/")
    if not separator or not instance_id.startswith("i-"):
        raise ValueError("V97 launch receipt differs")
    body = (
        json.dumps(
            {
                "attempt": plan.attempt,
                "critique_result_sha256": plan.critique_result_sha256,
                "instance_id": instance_id,
                "schema": "borsuk-v97-launch-v1",
                "source_commit": plan.source_commit,
            },
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
        + b"\n"
    )
    s3_client.put_object(
        Bucket=bucket,
        Key=f"{prefix.rstrip('/')}/launch.json",
        Body=body,
        ContentType="application/json",
        IfNoneMatch="*",
    )


def parse_args(argv: Sequence[str] | None = None) -> V97SpotPlan:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--source-archive-uri", required=True)
    parser.add_argument("--source-archive-sha256", required=True)
    parser.add_argument("--source-archive-bytes", type=int, required=True)
    parser.add_argument("--output-prefix", required=True)
    parser.add_argument("--profile", default="causality")
    parser.add_argument("--image-id", default="ami-06121aa3085b6f918")
    parser.add_argument("--security-group-id", default="sg-0b1fd3e4fbde4af0d")
    parser.add_argument(
        "--instance-profile-arn",
        default="arn:aws:iam::453182569524:instance-profile/borsuk-bench-profile",
    )
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
        security_group_id=args.security_group_id,
        instance_profile_arn=args.instance_profile_arn,
        targets=DEFAULT_TARGETS,
    )


def main(argv: Sequence[str] | None = None) -> None:
    import boto3

    plan = parse_args(argv)
    session = boto3.Session(profile_name=plan.profile, region_name="eu-central-1")
    s3_client = session.client("s3")
    ec2_client = session.client("ec2")
    ensure_unstarted(plan, s3_client=s3_client)
    instance_id = launch_one_spot(plan, ec2_client=ec2_client)
    try:
        claim_launched_attempt(plan, s3_client=s3_client, instance_id=instance_id)
    except Exception:
        ec2_client.terminate_instances(InstanceIds=[instance_id])
        raise
    print(instance_id)


if __name__ == "__main__":
    main()
