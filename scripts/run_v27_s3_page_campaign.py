#!/usr/bin/env python3
"""Fail-fast V27 S3 page-campaign projection and launch boundary."""

from __future__ import annotations

import base64
import hashlib
import json
import math
import shlex
from collections.abc import Callable
from dataclasses import dataclass

MAX_GETS = 10
MAX_ENCODED_BYTES = 4_587_520
MAX_CPU_P99_MS = 15.0
MAX_S3_P99_MS = 150.0
PERFECT_RECALL_PPM = 1_000_000
AMI_ID = "ami-07bcecd13a160173f"
INSTANCE_TYPE = "c7g.4xlarge"
SECURITY_GROUP_ID = "sg-0b1fd3e4fbde4af0d"
INSTANCE_PROFILE = "borsuk-bench-profile"


@dataclass(frozen=True)
class S3LatencyProfile:
    """Measured one-wave object-store latency and aggregate throughput."""

    request_latency_ms: float
    aggregate_bytes_per_second: int


@dataclass(frozen=True)
class V27QueryEvidence:
    """Truthful reduced-run quality and work counters."""

    cpu_p99_ms: float
    get_count: int
    encoded_bytes: int
    recall_ppm: int
    minimum_recall_ppm: int


@dataclass(frozen=True)
class V27LatencyProjection:
    """Exact decomposition of one concurrent S3 page wave."""

    get_count: int
    encoded_bytes: int
    request_waves: int
    request_ms: float
    transfer_ms: float
    cpu_p99_ms: float
    projected_p99_ms: float


@dataclass(frozen=True)
class SpotTarget:
    """One independent availability-zone target."""

    availability_zone: str
    subnet_id: str


@dataclass(frozen=True)
class V27ReducedSpotPlan:
    """Exact authority for one 100K reduced V27 build."""

    run_id: str
    source_commit: str
    source_archive_uri: str
    source_archive_sha256: str
    source_archive_bytes: int
    train_uri: str
    train_sha256: str
    train_bytes: int
    output_prefix: str
    row_limit: int
    roots: int
    leaves: int
    iterations: int
    workers: int
    page_rows: int


def _real_number(value: object) -> bool:
    return type(value) in {int, float} and math.isfinite(float(value))


def project_v27_query_latency(
    evidence: V27QueryEvidence, profile: S3LatencyProfile
) -> V27LatencyProjection:
    """Project cold p99 from exact bytes and one concurrent page-read wave."""

    if (
        type(evidence.get_count) is not int
        or evidence.get_count <= 0
        or type(evidence.encoded_bytes) is not int
        or evidence.encoded_bytes <= 0
        or not _real_number(evidence.cpu_p99_ms)
        or evidence.cpu_p99_ms < 0
        or not _real_number(profile.request_latency_ms)
        or profile.request_latency_ms < 0
        or type(profile.aggregate_bytes_per_second) is not int
        or profile.aggregate_bytes_per_second <= 0
    ):
        raise ValueError("V27 latency projection authority differs")
    transfer_ms = (
        evidence.encoded_bytes / profile.aggregate_bytes_per_second * 1_000.0
    )
    projected = float(evidence.cpu_p99_ms) + float(profile.request_latency_ms) + transfer_ms
    return V27LatencyProjection(
        get_count=evidence.get_count,
        encoded_bytes=evidence.encoded_bytes,
        request_waves=1,
        request_ms=float(profile.request_latency_ms),
        transfer_ms=transfer_ms,
        cpu_p99_ms=float(evidence.cpu_p99_ms),
        projected_p99_ms=projected,
    )


def preflight_v27_reduced_campaign(
    evidence: V27QueryEvidence,
    profile: S3LatencyProfile,
    *,
    launch: Callable[[], object],
) -> bytes:
    """Reject a bad reduced arm before invoking exactly one external launch."""

    if type(evidence.recall_ppm) is not int or evidence.recall_ppm != PERFECT_RECALL_PPM:
        raise ValueError("V27 aggregate-recall gate failed")
    if (
        type(evidence.minimum_recall_ppm) is not int
        or evidence.minimum_recall_ppm != PERFECT_RECALL_PPM
    ):
        raise ValueError("V27 minimum-recall gate failed")
    if evidence.get_count > MAX_GETS:
        raise ValueError("V27 requests gate failed")
    if evidence.encoded_bytes > MAX_ENCODED_BYTES:
        raise ValueError("V27 bytes gate failed")
    if not _real_number(evidence.cpu_p99_ms) or evidence.cpu_p99_ms > MAX_CPU_P99_MS:
        raise ValueError("V27 cpu gate failed")
    projection = project_v27_query_latency(evidence, profile)
    if projection.projected_p99_ms > MAX_S3_P99_MS:
        raise ValueError("V27 latency gate failed")

    value = {
        "claim_eligible": False,
        "encoded_bytes": evidence.encoded_bytes,
        "get_count": evidence.get_count,
        "minimum_recall_ppm": evidence.minimum_recall_ppm,
        "projected_p99_micros": round(projection.projected_p99_ms * 1_000),
        "recall_ppm": evidence.recall_ppm,
        "request_waves": projection.request_waves,
        "schema": "borsuk-v27-reduced-s3-preflight-v1",
        "status": "passed",
    }
    receipt = (
        json.dumps(value, allow_nan=False, separators=(",", ":"), sort_keys=True).encode()
        + b"\n"
    )
    launch()
    return receipt


def _digest(value: str, length: int) -> bool:
    return (
        type(value) is str
        and len(value) == length
        and all(character in "0123456789abcdef" for character in value)
    )


def _validate_spot_plan(plan: V27ReducedSpotPlan) -> None:
    if (
        type(plan.run_id) is not str
        or not plan.run_id.startswith("v27-reduced-")
        or not _digest(plan.source_commit, 40)
        or not _digest(plan.source_archive_sha256, 64)
        or not _digest(plan.train_sha256, 64)
        or not plan.source_archive_uri.startswith("s3://")
        or not plan.train_uri.startswith("s3://")
        or not plan.train_uri.endswith("train-00000000.parquet")
        or not plan.output_prefix.startswith("s3://")
        or not plan.output_prefix.endswith("/")
        or plan.source_archive_bytes <= 0
        or plan.train_bytes <= 0
        or plan.row_limit != 100_000
        or plan.roots != 64
        or plan.leaves != 4_096
        or plan.iterations != 4
        or plan.workers != 8
        or plan.page_rows != 512
    ):
        raise ValueError("V27 reduced Spot plan differs")


def _split_s3(uri: str, *, prefix: bool = False) -> tuple[str, str]:
    remainder = uri.removeprefix("s3://")
    bucket, separator, key = remainder.partition("/")
    if not separator or not bucket or not key or (prefix and not key.endswith("/")):
        raise ValueError("V27 S3 authority differs")
    return bucket, key


def _worker_script(plan: V27ReducedSpotPlan) -> str:
    bucket, prefix = _split_s3(plan.output_prefix, prefix=True)
    values = {
        "archive": shlex.quote(plan.source_archive_uri),
        "archive_bytes": str(plan.source_archive_bytes),
        "archive_sha": plan.source_archive_sha256,
        "bucket": shlex.quote(bucket),
        "commit": plan.source_commit,
        "prefix": shlex.quote(prefix),
        "run_id": shlex.quote(plan.run_id),
        "train": shlex.quote(plan.train_uri),
        "train_bytes": str(plan.train_bytes),
        "train_sha": plan.train_sha256,
    }
    return f"""#!/bin/bash
set -Eeuo pipefail
umask 077
shutdown --poweroff +90
root=/opt/borsuk-v27-reduced
source_dir="$root/source"
index_dir="$root/index"
archive="$root/source.tar.zst"
train="$root/train-00000000.parquet"
mkdir -p "$root" "$source_dir"
exec >"$root/worker.log" 2>&1
bucket={values['bucket']}
prefix={values['prefix']}
terminal=failed
put_once() {{ aws s3api put-object --bucket "$bucket" --key "$prefix$2" --body "$1" --if-none-match '*' --checksum-algorithm SHA256 >/dev/null; }}
finish() {{
  status=$?
  trap - EXIT
  set +e
  if [[ "$terminal" != complete ]]; then
    printf '{{"claim_eligible":false,"run_id":"%s","schema":"borsuk-v27-reduced-spot-terminal-v1","status":"failed","worker_status":%d}}\n' {values['run_id']} "$status" >"$root/FAILED.json"
    put_once "$root/worker.log" worker.log || true
    put_once "$root/FAILED.json" FAILED.json || true
  fi
  shutdown -h now
}}
trap finish EXIT
dnf install -y gcc gcc-c++ tar zstd
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal --default-toolchain stable
source /root/.cargo/env
aws s3 cp {values['archive']} "$archive" --only-show-errors
aws s3 cp {values['train']} "$train" --only-show-errors
test "$(stat -c %s "$archive")" -eq {values['archive_bytes']}
test "$(stat -c %s "$train")" -eq {values['train_bytes']}
printf '%s  %s\n' {values['archive_sha']} "$archive" | sha256sum --check --status
printf '%s  %s\n' {values['train_sha']} "$train" | sha256sum --check --status
tar --zstd -xf "$archive" -C "$source_dir"
cd "$source_dir"
test "$(cat .borsuk-source-commit)" = {values['commit']}
cargo build --release --locked -p borsuk --example v27_s3_build --example v27_s3_qualify
target/release/examples/v27_s3_build --execute --train-parquet "$train" --train-sha256 {values['train_sha']} --train-bytes {values['train_bytes']} --row-limit 100000 --roots 64 --leaves 4096 --iterations 4 --workers 8 --page-rows 512 --output-dir "$index_dir" >"$root/build.json"
cmp "$root/build.json" "$index_dir/BUILD_COMPLETE.json"
aws s3 cp "$index_dir" "s3://$bucket/${{prefix}}index/" --recursive --only-show-errors
put_once "$root/build.json" build.json
put_once "$root/worker.log" worker.log
python3 - "$root/COMPLETE.json" {values['run_id']} {values['commit']} <<'PY'
import json,sys
path,run_id,commit=sys.argv[1:]
value={{"claim_eligible":False,"run_id":run_id,"schema":"borsuk-v27-reduced-spot-terminal-v1","source_commit":commit,"status":"complete"}}
open(path,"wb").write(json.dumps(value,sort_keys=True,separators=(",", ":")).encode()+b"\n")
PY
put_once "$root/COMPLETE.json" COMPLETE.json
terminal=complete
"""


def build_v27_spot_specs(
    plan: V27ReducedSpotPlan, targets: tuple[SpotTarget, ...]
) -> list[dict[str, object]]:
    """Build deterministic one-instance Spot requests across distinct zones."""

    _validate_spot_plan(plan)
    if (
        len(targets) < 2
        or len({target.availability_zone for target in targets}) != len(targets)
        or len({target.subnet_id for target in targets}) != len(targets)
    ):
        raise ValueError("V27 Spot target inventory differs")
    user_data = base64.b64encode(_worker_script(plan).encode()).decode()
    specs = []
    for ordinal, target in enumerate(targets):
        token = hashlib.sha256(f"{plan.run_id}:{target.subnet_id}:{ordinal}".encode()).hexdigest()
        specs.append(
            {
                "ImageId": AMI_ID,
                "InstanceType": INSTANCE_TYPE,
                "MinCount": 1,
                "MaxCount": 1,
                "ClientToken": "v27-reduced-" + token[:48],
                "SubnetId": target.subnet_id,
                "SecurityGroupIds": [SECURITY_GROUP_ID],
                "IamInstanceProfile": {"Name": INSTANCE_PROFILE},
                "InstanceInitiatedShutdownBehavior": "terminate",
                "InstanceMarketOptions": {
                    "MarketType": "spot",
                    "SpotOptions": {
                        "SpotInstanceType": "one-time",
                        "InstanceInterruptionBehavior": "terminate",
                    },
                },
                "BlockDeviceMappings": [
                    {
                        "DeviceName": "/dev/xvda",
                        "Ebs": {
                            "DeleteOnTermination": True,
                            "Encrypted": True,
                            "VolumeSize": 40,
                            "VolumeType": "gp3",
                        },
                    }
                ],
                "UserData": user_data,
                "TagSpecifications": [
                    {
                        "ResourceType": "instance",
                        "Tags": [
                            {"Key": "Name", "Value": plan.run_id},
                            {"Key": "borsuk-purpose", "Value": "v27-reduced-100k"},
                        ],
                    }
                ],
            }
        )
    return specs


def run_v27_reduced_spot(
    plan: V27ReducedSpotPlan,
    *,
    targets: tuple[SpotTarget, ...],
    launch: Callable[[dict[str, object]], str],
    terminal: Callable[[], bytes | None],
    health: Callable[[str], tuple[str, str, str]],
    sleep: Callable[[int], None],
    terminate: Callable[[str], None],
) -> bytes:
    """Launch one original attempt, poll every 30 seconds, and always terminate it."""

    instance_id: str | None = None
    for spec in build_v27_spot_specs(plan, targets):
        try:
            instance_id = launch(spec)
        except RuntimeError as error:
            if str(error) == "InsufficientInstanceCapacity":
                continue
            raise
        break
    if instance_id is None:
        raise RuntimeError("V27 Spot capacity is unavailable")
    if not instance_id.startswith("i-"):
        raise ValueError("V27 Spot instance identity differs")
    try:
        for _ in range(180):
            receipt = terminal()
            if receipt is not None:
                if not receipt:
                    raise ValueError("V27 Spot terminal is empty")
                return receipt
            state, system_status, instance_status = health(instance_id)
            if state != "running" or system_status not in {"ok", "initializing"} or instance_status not in {"ok", "initializing"}:
                raise RuntimeError("V27 Spot health differs")
            sleep(30)
        raise TimeoutError("V27 Spot attempt exceeded wall stop")
    finally:
        terminate(instance_id)
