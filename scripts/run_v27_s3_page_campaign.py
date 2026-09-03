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


@dataclass(frozen=True)
class V27SpotArtifact:
    """One immutable object staged by a Spot worker."""

    role: str
    uri: str
    sha256: str
    encoded_bytes: int
    basename: str


@dataclass(frozen=True)
class V27QualitySpotPlan:
    """Exact authority for one 32-query reduced V27 quality gate."""

    run_id: str
    source_commit: str
    source_archive: V27SpotArtifact
    train: V27SpotArtifact
    query: V27SpotArtifact
    roots: V27SpotArtifact
    leaves: V27SpotArtifact
    postings: V27SpotArtifact
    modes: V27SpotArtifact
    manifest: V27SpotArtifact
    s3_page_prefix: str
    output_prefix: str
    root_beam: int
    leaf_beam: int
    page_count: int


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
        or plan.leaves != 256
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
dnf install -y gcc gcc-c++ python3 tar zstd
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal --default-toolchain stable
export HOME=/root
aws s3 cp {values['archive']} "$archive" --only-show-errors
aws s3 cp {values['train']} "$train" --only-show-errors
test "$(stat -c %s "$archive")" -eq {values['archive_bytes']}
test "$(stat -c %s "$train")" -eq {values['train_bytes']}
printf '%s  %s\n' {values['archive_sha']} "$archive" | sha256sum --check --status
printf '%s  %s\n' {values['train_sha']} "$train" | sha256sum --check --status
tar --zstd -xf "$archive" -C "$source_dir"
cd "$source_dir"
test "$(cat .borsuk-source-commit)" = {values['commit']}
/root/.cargo/bin/cargo build --release --locked -p borsuk --example v27_s3_build --example v27_s3_qualify
target/release/examples/v27_s3_build --execute --train-parquet "$train" --train-sha256 {values['train_sha']} --train-bytes {values['train_bytes']} --row-limit 100000 --roots 64 --leaves 256 --iterations 4 --workers 8 --page-rows 512 --output-dir "$index_dir" >"$root/build.json"
cmp "$root/build.json" "$index_dir/BUILD_COMPLETE.json"
aws s3 cp "$index_dir" "s3://$bucket/${{prefix}}index/" --recursive --only-show-errors
put_once "$root/build.json" build.json
python3 - "$root/COMPLETE.json" {values['run_id']} {values['commit']} <<'PY'
import json,sys
path,run_id,commit=sys.argv[1:]
value={{"claim_eligible":False,"run_id":run_id,"schema":"borsuk-v27-reduced-spot-terminal-v1","source_commit":commit,"status":"complete"}}
open(path,"wb").write(json.dumps(value,sort_keys=True,separators=(",", ":")).encode()+b"\\n")
PY
put_once "$root/worker.log" worker.log
put_once "$root/COMPLETE.json" COMPLETE.json
terminal=complete
"""


def _validate_spot_artifact(artifact: V27SpotArtifact, role: str, basename: str) -> None:
    if (
        artifact.role != role
        or artifact.basename != basename
        or not artifact.uri.startswith("s3://")
        or not artifact.uri.endswith("/" + basename)
        or not _digest(artifact.sha256, 64)
        or type(artifact.encoded_bytes) is not int
        or artifact.encoded_bytes <= 0
    ):
        raise ValueError(f"V27 quality {role} authority differs")


def _quality_worker_script(plan: V27QualitySpotPlan) -> str:
    if (
        type(plan.run_id) is not str
        or not plan.run_id.startswith("v27-quality-")
        or not _digest(plan.source_commit, 40)
        or not plan.s3_page_prefix.startswith("s3://")
        or plan.s3_page_prefix.endswith("/")
        or not plan.output_prefix.startswith("s3://")
        or not plan.output_prefix.endswith("/")
        or plan.root_beam <= 0
        or plan.leaf_beam <= 0
        or not 1 <= plan.page_count <= MAX_GETS
    ):
        raise ValueError("V27 quality Spot plan differs")
    expected = (
        (plan.source_archive, "source-archive", "source.tar.zst"),
        (plan.train, "train", "train-00000000.parquet"),
        (plan.query, "query", "test.parquet"),
        (plan.roots, "roots", "roots.arrow"),
        (plan.leaves, "leaves", "leaves.arrow"),
        (plan.postings, "postings", "postings.parquet"),
        (plan.modes, "modes", "modes.arrow"),
        (plan.manifest, "manifest", "pages.json"),
    )
    for artifact, role, basename in expected:
        _validate_spot_artifact(artifact, role, basename)
    bucket, prefix = _split_s3(plan.output_prefix, prefix=True)
    assignments = "\n".join(
        f'{role.replace("-", "_")}="$root/{artifact.basename}"'
        for artifact, role, _basename in expected
    )
    downloads = "\n".join(
        f'aws s3 cp {shlex.quote(artifact.uri)} "${role.replace("-", "_")}" --only-show-errors\n'
        f'test "$(stat -c %s "${role.replace("-", "_")}")" -eq {artifact.encoded_bytes}\n'
        f'printf \'%s  %s\\n\' {artifact.sha256} "${role.replace("-", "_")}" | sha256sum --check --status'
        for artifact, role, _basename in expected
    )
    artifact_flags = " ".join(
        f'--{role} "${role}" --{role}-sha256 {artifact.sha256} --{role}-bytes {artifact.encoded_bytes}'
        for artifact, role, _basename in expected[3:]
    )
    return f"""#!/bin/bash
set -Eeuo pipefail
umask 077
shutdown --poweroff +90
root=/opt/borsuk-v27-quality
source_dir="$root/source"
mkdir -p "$root" "$source_dir"
{assignments}
exec >"$root/worker.log" 2>&1
bucket={shlex.quote(bucket)}
prefix={shlex.quote(prefix)}
terminal=failed
export AWS_REGION=eu-central-1
put_once() {{ aws s3api put-object --bucket "$bucket" --key "$prefix$2" --body "$1" --if-none-match '*' --checksum-algorithm SHA256 >/dev/null; }}
finish() {{
  status=$?
  trap - EXIT
  set +e
  if [[ "$terminal" != complete ]]; then
    [[ -f "$root/quality.json" ]] && put_once "$root/quality.json" quality.json || true
    printf '{{"claim_eligible":false,"run_id":"%s","schema":"borsuk-v27-quality-spot-terminal-v1","status":"failed","worker_status":%d}}\n' {shlex.quote(plan.run_id)} "$status" >"$root/FAILED.json"
    put_once "$root/worker.log" worker.log || true
    put_once "$root/FAILED.json" FAILED.json || true
  fi
  shutdown -h now
}}
trap finish EXIT
dnf install -y gcc gcc-c++ python3.11 python3.11-pip tar zstd
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal --default-toolchain stable
python3.11 -m venv "$root/venv"
"$root/venv/bin/python" -m pip install --upgrade pip
"$root/venv/bin/pip" install --no-cache-dir numpy==2.4.2 pyarrow==24.0.0
{downloads}
tar --zstd -xf "$source_archive" -C "$source_dir"
cd "$source_dir"
test "$(cat .borsuk-source-commit)" = {plan.source_commit}
/root/.cargo/bin/cargo build --release --locked -p borsuk --example v27_s3_qualify
"$root/venv/bin/python" scripts/run_v27_reduced_quality.py --execute \
  --train-parquet "$train" --train-sha256 {plan.train.sha256} --train-bytes {plan.train.encoded_bytes} \
  --query-parquet "$query" --query-sha256 {plan.query.sha256} --query-bytes {plan.query.encoded_bytes} \
  {artifact_flags} \
  --qualifier-binary target/release/examples/v27_s3_qualify \
  --s3-page-prefix {shlex.quote(plan.s3_page_prefix)} --root-beam {plan.root_beam} \
  --leaf-beam {plan.leaf_beam} --page-count {plan.page_count} >"$root/quality.json"
put_once "$root/quality.json" quality.json
printf '{{"claim_eligible":false,"run_id":"%s","schema":"borsuk-v27-quality-spot-terminal-v1","source_commit":"%s","status":"complete"}}\n' {shlex.quote(plan.run_id)} {plan.source_commit} >"$root/COMPLETE.json"
put_once "$root/worker.log" worker.log
put_once "$root/COMPLETE.json" COMPLETE.json
terminal=complete
"""


def build_v27_quality_spot_specs(
    plan: V27QualitySpotPlan, targets: tuple[SpotTarget, ...]
) -> list[dict[str, object]]:
    """Build deterministic quality-worker Spot requests without page-corpus staging."""

    script = _quality_worker_script(plan)
    if (
        len(targets) < 2
        or len({target.availability_zone for target in targets}) != len(targets)
        or len({target.subnet_id for target in targets}) != len(targets)
    ):
        raise ValueError("V27 Spot target inventory differs")
    specs = []
    for ordinal, target in enumerate(targets):
        token = hashlib.sha256(f"{plan.run_id}:{target.subnet_id}:{ordinal}".encode()).hexdigest()
        specs.append(
            {
                "ImageId": AMI_ID,
                "InstanceType": INSTANCE_TYPE,
                "MinCount": 1,
                "MaxCount": 1,
                "ClientToken": "v27-quality-" + token[:48],
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
                "UserData": base64.b64encode(script.encode()).decode(),
                "TagSpecifications": [
                    {
                        "ResourceType": "instance",
                        "Tags": [
                            {"Key": "Name", "Value": plan.run_id},
                            {"Key": "borsuk-purpose", "Value": "v27-reduced-quality"},
                        ],
                    }
                ],
            }
        )
    return specs


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
                try:
                    terminal_value = json.loads(receipt)
                except (UnicodeDecodeError, json.JSONDecodeError) as error:
                    raise ValueError("V27 Spot terminal JSON differs") from error
                if terminal_value.get("status") == "failed":
                    raise RuntimeError("V27 Spot worker failed")
                if terminal_value.get("status") != "complete":
                    raise ValueError("V27 Spot terminal status differs")
                return receipt
            state, system_status, instance_status = health(instance_id)
            pending = state == "pending" and system_status in {
                "initializing",
                "not-applicable",
            } and instance_status in {"initializing", "not-applicable"}
            running = state == "running" and system_status in {
                "ok",
                "initializing",
            } and instance_status in {"ok", "initializing"}
            if not (pending or running):
                raise RuntimeError("V27 Spot health differs")
            sleep(30)
        raise TimeoutError("V27 Spot attempt exceeded wall stop")
    finally:
        terminate(instance_id)
