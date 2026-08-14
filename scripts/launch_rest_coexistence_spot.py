#!/usr/bin/env python3
"""Build or launch one immutable two-host REST coexistence attempt."""

from __future__ import annotations

import argparse
import base64
import dataclasses
import hashlib
import json
import re
import subprocess
import tempfile
from pathlib import Path
from typing import Any


COMPLETE_MARKER = "ATTEMPT_COMPLETE.json"
FAILED_MARKER = "ATTEMPT_FAILED.json"
KNOWN_MARKERS = {COMPLETE_MARKER, FAILED_MARKER}
RUNTIME_FIELDS = {
    "cpu_threads",
    "io_threads",
    "s3_get_concurrency",
    "search_admission",
    "page_budget",
    "leaf_read_width",
    "max_inflight_leaf_reads",
    "ram_budget_bytes",
    "disk_cache_bytes",
}


@dataclasses.dataclass(frozen=True)
class AttemptObservation:
    server_state: str
    generator_state: str
    terminal_markers: tuple[str, ...]
    server_state_reason: str | None = None
    generator_state_reason: str | None = None
    deadline_expired: bool = False


@dataclasses.dataclass(frozen=True)
class AttemptDecision:
    action: str
    discard_measurements: bool


def _canonical_bytes(value: object) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), allow_nan=False
    ).encode("utf-8")


def _require_sha256(label: str, value: str) -> None:
    if re.fullmatch(r"[0-9a-f]{64}", value) is None:
        raise ValueError(f"{label} must be a lowercase SHA-256 digest")


def _validated_runtime(value: dict[str, int]) -> dict[str, int]:
    if set(value) != RUNTIME_FIELDS:
        raise ValueError("runtime must contain exactly the frozen flow-control fields")
    if any(type(item) is not int for item in value.values()):
        raise ValueError("runtime flow-control values must be integers")
    if not 1 <= value["cpu_threads"] <= 64:
        raise ValueError("runtime cpu_threads must be in 1..=64")
    if not 1 <= value["s3_get_concurrency"] <= 128:
        raise ValueError("runtime s3_get_concurrency must be in 1..=128")
    if not value["s3_get_concurrency"] <= value["io_threads"] <= 256:
        raise ValueError("runtime io_threads must be at least s3_get_concurrency and at most 256")
    if value["search_admission"] <= 0:
        raise ValueError("runtime search_admission must be positive")
    if value["page_budget"] not in (4, 8, 16, 32):
        raise ValueError("runtime page_budget must be 4, 8, 16, or 32")
    for field in ("leaf_read_width", "max_inflight_leaf_reads"):
        if not 1 <= value[field] <= 1024:
            raise ValueError(f"runtime {field} must be in 1..=1024")
    if value["ram_budget_bytes"] <= 0 or value["disk_cache_bytes"] < 0:
        raise ValueError("runtime RAM must be positive and disk cache nonnegative")
    return {key: value[key] for key in sorted(value)}


def cold_s3_cap_matrix() -> list[dict[str, int | str]]:
    """Return the frozen small-runtime uncached flow-control qualification cells."""
    read_planes = (
        ("narrow", 16, 32, 32, 64),
        ("balanced", 32, 48, 64, 88),
        ("wide", 32, 96, 128, 160),
    )
    cells: list[dict[str, int | str]] = []
    for search_admission in (2, 4, 8):
        for name, leaf_width, inflight, get_cap, io_threads in read_planes:
            runtime: dict[str, int] = {
                "cpu_threads": 3,
                "io_threads": io_threads,
                "s3_get_concurrency": get_cap,
                "search_admission": search_admission,
                "page_budget": 32,
                "leaf_read_width": leaf_width,
                "max_inflight_leaf_reads": inflight,
                "ram_budget_bytes": 2 * 1024**3,
                "disk_cache_bytes": 0,
            }
            cells.append(
                {
                    "cell_id": f"a{search_admission}-{name}",
                    **_validated_runtime(runtime),
                }
            )
    return cells


def _tags(campaign_id: str, attempt: int, role: str) -> list[dict[str, str]]:
    values = {
        "Attempt": str(attempt),
        "AutoTerminate": "true",
        "Campaign": campaign_id,
        "Name": f"borsuk-{campaign_id}-{role}-a{attempt:04d}",
        "Project": "BorsukBenchmark",
        "Role": role,
    }
    return [{"Key": key, "Value": values[key]} for key in sorted(values)]


def _user_data(
    *,
    campaign_id: str,
    attempt: int,
    role: str,
    worker: str,
    source_sha256: str,
    binary_sha256: str,
    index_receipt_sha256: str,
    dataset_receipt_sha256: str,
    output_uri: str,
    runtime: dict[str, int],
) -> str:
    if not worker or "\x00" in worker:
        raise ValueError("worker script must be nonempty UTF-8 text")
    payload = base64.b64encode(worker.encode("utf-8")).decode("ascii")
    if role == "rest-server":
        properties = (
            "-p MemoryMax=6G -p MemorySwapMax=0 -p AllowedCPUs=0-3 "
            f"--setenv=BORSUK_REST_CPU_THREADS={runtime['cpu_threads']} "
            f"--setenv=BORSUK_REST_IO_THREADS={runtime['io_threads']} "
            f"--setenv=BORSUK_REST_S3_GET_CONCURRENCY={runtime['s3_get_concurrency']} "
            f"--setenv=BORSUK_REST_SEARCH_ADMISSION={runtime['search_admission']} "
            f"--setenv=BORSUK_REST_PAGE_BUDGET={runtime['page_budget']} "
            f"--setenv=BORSUK_REST_LEAF_READ_WIDTH={runtime['leaf_read_width']} "
            f"--setenv=BORSUK_REST_MAX_INFLIGHT_LEAF_READS={runtime['max_inflight_leaf_reads']} "
            f"--setenv=BORSUK_REST_RAM_BUDGET_BYTES={runtime['ram_budget_bytes']} "
            f"--setenv=BORSUK_REST_DISK_CACHE_BYTES={runtime['disk_cache_bytes']}"
        )
    else:
        properties = "-p MemoryMax=3G -p MemorySwapMax=0 -p AllowedCPUs=0-1"
    properties += (
        f" --setenv=BORSUK_SOURCE_SHA256={source_sha256}"
        f" --setenv=BORSUK_BINARY_SHA256={binary_sha256}"
        f" --setenv=BORSUK_INDEX_RECEIPT_SHA256={index_receipt_sha256}"
        f" --setenv=BORSUK_DATASET_RECEIPT_SHA256={dataset_receipt_sha256}"
        f" --setenv=BORSUK_OUTPUT_URI={output_uri}"
    )
    readiness = ""
    endpoint_property = ""
    if role == "rest-generator":
        readiness = f"""server_endpoint=''
deadline=$((SECONDS + 900))
while [ "$SECONDS" -lt "$deadline" ]; do
  server_ip=$(aws ec2 describe-instances \\
    --filters 'Name=tag:Campaign,Values={campaign_id}' \\
      'Name=tag:Attempt,Values={attempt}' \\
      'Name=tag:Role,Values=rest-server' \\
      'Name=instance-state-name,Values=pending,running' \\
    --query 'Reservations[].Instances[].PrivateIpAddress' --output text | awk '{{print $1}}')
  if [ -n "$server_ip" ] && curl --fail --silent --max-time 2 \
      "http://${{server_ip}}:8080/health" >/dev/null; then
    server_endpoint="http://${{server_ip}}:8080"
    break
  fi
  sleep 2
done
if [ -z "$server_endpoint" ]; then
  echo 'REST server did not become healthy before the frozen deadline' >&2
  exit 66
fi
"""
        endpoint_property = ' --setenv=BORSUK_SERVER_ENDPOINT="$server_endpoint"'
    lifecycle = f"""output_path=${{BORSUK_OUTPUT_URI#s3://}}
output_bucket=${{output_path%%/*}}
output_prefix=${{output_path#*/}}
upload_failure() {{
  local status="$1"
  local receipt=/tmp/borsuk-rest-failure.json
  printf '{{"role":"%s","schema_version":1,"status":%s}}\n' '{role}' "$status" >"$receipt"
  aws s3api put-object --bucket "$output_bucket" \
    --key "$output_prefix/{FAILED_MARKER}" --body "$receipt" \
    --if-none-match '*' >/dev/null 2>&1 || true
}}
watch_spot_interruption() {{
  local token action receipt
  token=$(curl --fail --silent --request PUT \
    --header 'X-aws-ec2-metadata-token-ttl-seconds: 21600' \
    http://169.254.169.254/latest/api/token) || return 0
  while sleep 2; do
    action=$(curl --fail --silent \
      --header "X-aws-ec2-metadata-token: $token" \
      http://169.254.169.254/latest/meta-data/spot/instance-action) || continue
    [ -n "$action" ] || continue
    touch /run/borsuk-spot-interruption
    receipt=/tmp/{role}-spot-interruption.json
    printf '{{"role":"%s","schema_version":1,"spot_action":%s}}\n' \
      '{role}' "$action" >"$receipt"
    aws s3api put-object --bucket "$output_bucket" \
      --key "$output_prefix/{role}-spot-interruption.json" --body "$receipt" \
      --if-none-match '*' >/dev/null 2>&1 || true
    return 0
  done
}}
watch_spot_interruption &
spot_watcher_pid=$!
"""
    script = f"""#!/usr/bin/env bash
set -euo pipefail
export HOME=/root
export BORSUK_SOURCE_SHA256={source_sha256}
export BORSUK_BINARY_SHA256={binary_sha256}
export BORSUK_INDEX_RECEIPT_SHA256={index_receipt_sha256}
export BORSUK_DATASET_RECEIPT_SHA256={dataset_receipt_sha256}
export BORSUK_OUTPUT_URI={output_uri}
{lifecycle}finish() {{
  status=$?
  trap - EXIT
  kill "$spot_watcher_pid" >/dev/null 2>&1 || true
  if [ "$status" -ne 0 ] && [ ! -e /run/borsuk-spot-interruption ]; then
    upload_failure "$status"
  fi
  shutdown -h now || true
  exit "$status"
}}
trap finish EXIT
printf '%s' '{payload}' | base64 -d >/var/lib/borsuk-rest-worker.sh
chmod 700 /var/lib/borsuk-rest-worker.sh
{readiness}systemd-run --wait --collect --unit=borsuk-rest-{role} {properties}{endpoint_property} \
  /bin/bash /var/lib/borsuk-rest-worker.sh
"""
    if len(script.encode("utf-8")) > 16 * 1024:
        raise ValueError("EC2 user data exceeds its 16 KiB raw limit")
    return script


def _request(
    *,
    campaign_id: str,
    attempt: int,
    role: str,
    instance_type: str,
    image_id: str,
    subnet_id: str,
    security_group_id: str,
    instance_profile_arn: str,
    user_data: str,
) -> dict[str, object]:
    token_input = f"{campaign_id}\0{attempt}\0{role}".encode("utf-8")
    return {
        "ImageId": image_id,
        "InstanceType": instance_type,
        "MinCount": 1,
        "MaxCount": 1,
        "ClientToken": "borsuk-rest-" + hashlib.sha256(token_input).hexdigest()[:40],
        "UserData": user_data,
        "SecurityGroupIds": [security_group_id],
        "SubnetId": subnet_id,
        "IamInstanceProfile": {"Arn": instance_profile_arn},
        "InstanceMarketOptions": {
            "MarketType": "spot",
            "SpotOptions": {
                "SpotInstanceType": "one-time",
                "InstanceInterruptionBehavior": "terminate",
            },
        },
        "InstanceInitiatedShutdownBehavior": "terminate",
        "MetadataOptions": {
            "HttpEndpoint": "enabled",
            "HttpTokens": "required",
            "HttpPutResponseHopLimit": 1,
        },
        "BlockDeviceMappings": [
            {
                "DeviceName": "/dev/xvda",
                "Ebs": {
                    "DeleteOnTermination": True,
                    "Encrypted": True,
                    "VolumeSize": 32,
                    "VolumeType": "gp3",
                    "Iops": 3000,
                    "Throughput": 125,
                },
            }
        ],
        "TagSpecifications": [
            {"ResourceType": resource_type, "Tags": _tags(campaign_id, attempt, role)}
            for resource_type in ("instance", "volume")
        ],
    }


def build_launch_pair(
    *,
    campaign_id: str,
    attempt: int,
    image_id: str,
    subnet_id: str,
    security_group_id: str,
    instance_profile_arn: str,
    source_sha256: str,
    binary_sha256: str,
    index_receipt_sha256: str,
    dataset_receipt_sha256: str,
    runtime: dict[str, int],
    output_uri: str,
    server_worker: str,
    generator_worker: str,
) -> dict[str, Any]:
    if not campaign_id or attempt <= 0:
        raise ValueError("campaign must be nonempty and attempt must be positive")
    if (
        re.fullmatch(r"s3://[A-Za-z0-9._-]+/[A-Za-z0-9._/-]+", output_uri) is None
        or output_uri.endswith("/")
    ):
        raise ValueError("output URI must be a canonical S3 prefix without trailing slash")
    if not output_uri.endswith(f"/attempts/{attempt:04d}"):
        raise ValueError("output URI attempt path differs from the requested attempt")
    for label, value in (
        ("source", source_sha256),
        ("binary", binary_sha256),
        ("index receipt", index_receipt_sha256),
        ("dataset receipt", dataset_receipt_sha256),
    ):
        _require_sha256(label, value)
    for label, value in (
        ("image id", image_id),
        ("subnet id", subnet_id),
        ("security group id", security_group_id),
        ("instance profile ARN", instance_profile_arn),
    ):
        if not value:
            raise ValueError(f"{label} must be nonempty")
    runtime = _validated_runtime(runtime)
    workload = {
        "cheap_baseline": True,
        "measurement_seconds": 120,
        "mixed_normal_search_fraction": 0.20,
        "mixed_normal_sustainable_fraction": 0.70,
        "mixed_overload_sustainable_fraction": 1.50,
        "open_loop": True,
        "repetitions": 3,
        "separate_generator": True,
        "schema_version": 1,
        "search_staircase_qps": [1, 2, 4, 8, 12, 16, 24, 32],
        "warmup_seconds": 30,
    }
    receipt = {
        "schema_version": 1,
        "campaign_id": campaign_id,
        "attempt": attempt,
        "source_sha256": source_sha256,
        "binary_sha256": binary_sha256,
        "index_receipt_sha256": index_receipt_sha256,
        "dataset_receipt_sha256": dataset_receipt_sha256,
        "server_worker_sha256": hashlib.sha256(server_worker.encode("utf-8")).hexdigest(),
        "generator_worker_sha256": hashlib.sha256(generator_worker.encode("utf-8")).hexdigest(),
        "workload_sha256": hashlib.sha256(_canonical_bytes(workload)).hexdigest(),
        "output_uri": output_uri,
        "runtime": runtime,
        "complete_uri": f"{output_uri}/{COMPLETE_MARKER}",
        "failed_uri": f"{output_uri}/{FAILED_MARKER}",
    }
    common = {
        "campaign_id": campaign_id,
        "attempt": attempt,
        "image_id": image_id,
        "subnet_id": subnet_id,
        "security_group_id": security_group_id,
        "instance_profile_arn": instance_profile_arn,
    }
    server_user_data = _user_data(
        campaign_id=campaign_id,
        attempt=attempt,
        role="rest-server",
        worker=server_worker,
        source_sha256=source_sha256,
        binary_sha256=binary_sha256,
        index_receipt_sha256=index_receipt_sha256,
        dataset_receipt_sha256=dataset_receipt_sha256,
        output_uri=output_uri,
        runtime=runtime,
    )
    generator_user_data = _user_data(
        campaign_id=campaign_id,
        attempt=attempt,
        role="rest-generator",
        worker=generator_worker,
        source_sha256=source_sha256,
        binary_sha256=binary_sha256,
        index_receipt_sha256=index_receipt_sha256,
        dataset_receipt_sha256=dataset_receipt_sha256,
        output_uri=output_uri,
        runtime=runtime,
    )
    server_request = _request(
        **common,
        role="rest-server",
        instance_type="c7g.xlarge",
        user_data=server_user_data,
    )
    generator_request = _request(
        **common,
        role="rest-generator",
        instance_type="c7g.large",
        user_data=generator_user_data,
    )
    receipt["server_launch_sha256"] = hashlib.sha256(
        _canonical_bytes(server_request)
    ).hexdigest()
    receipt["generator_launch_sha256"] = hashlib.sha256(
        _canonical_bytes(generator_request)
    ).hexdigest()
    return {
        "receipt": receipt,
        "workload": workload,
        "runtime": runtime,
        "server": server_request,
        "generator": generator_request,
    }


def classify_attempt(observation: AttemptObservation) -> AttemptDecision:
    states = {"pending", "running", "stopping", "stopped", "shutting-down", "terminated"}
    if observation.server_state not in states or observation.generator_state not in states:
        raise ValueError("unrecognized EC2 instance state")
    markers = set(observation.terminal_markers)
    unknown = markers - KNOWN_MARKERS
    if unknown:
        raise ValueError(f"unrecognized terminal marker: {sorted(unknown)[0]}")
    if len(markers) > 1:
        raise ValueError("conflicting terminal markers")
    if COMPLETE_MARKER in markers:
        return AttemptDecision("terminate-both-success", False)
    if FAILED_MARKER in markers:
        return AttemptDecision("terminate-both-failure", False)
    reasons = {observation.server_state_reason, observation.generator_state_reason}
    if "Server.SpotInstanceTermination" in reasons:
        return AttemptDecision("terminate-peer-and-retry-next-attempt", True)
    if observation.deadline_expired:
        return AttemptDecision("terminate-both-timeout", True)
    return AttemptDecision("monitor", False)


def _run_instance(profile: str, request: dict[str, object]) -> str:
    completed = subprocess.run(
        [
            "aws",
            "--profile",
            profile,
            "ec2",
            "run-instances",
            "--cli-input-json",
            json.dumps(request, separators=(",", ":")),
            "--query",
            "Instances[0].InstanceId",
            "--output",
            "text",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    return completed.stdout.strip()


def _terminate_instances(profile: str, instance_ids: list[str]) -> None:
    if not instance_ids:
        return
    subprocess.run(
        [
            "aws",
            "--profile",
            profile,
            "ec2",
            "terminate-instances",
            "--instance-ids",
            *instance_ids,
        ],
        check=True,
        capture_output=True,
        text=True,
    )


def _record_identity(
    profile: str, pair: dict[str, Any], role: str, instance_id: str
) -> None:
    if role not in ("server", "generator") or re.fullmatch(r"i-[0-9a-f]+", instance_id) is None:
        raise ValueError("invalid REST benchmark instance identity")
    output_uri = str(pair["receipt"]["output_uri"])
    location = output_uri.removeprefix("s3://")
    bucket, prefix = location.split("/", 1)
    identity = {
        "schema_version": 1,
        "role": role,
        "instance_id": instance_id,
        "launch_sha256": pair["receipt"][f"{role}_launch_sha256"],
    }
    with tempfile.TemporaryDirectory(prefix="borsuk-rest-identity-") as directory:
        body = Path(directory) / f"{role}.json"
        body.write_bytes(_canonical_bytes(identity) + b"\n")
        subprocess.run(
            [
                "aws",
                "--profile",
                profile,
                "s3api",
                "put-object",
                "--bucket",
                bucket,
                "--key",
                f"{prefix}/launch-identities/{role}.json",
                "--body",
                str(body),
                "--if-none-match",
                "*",
            ],
            check=True,
            capture_output=True,
            text=True,
        )


def execute_pair(
    pair: dict[str, Any],
    *,
    profile: str,
    run_instance: Any = _run_instance,
    terminate_instances: Any = _terminate_instances,
    record_identity: Any | None = None,
) -> dict[str, str]:
    identities: dict[str, str] = {}
    try:
        for role in ("server", "generator"):
            instance_id = run_instance(profile, pair[role])
            identities[role] = instance_id
            if record_identity is not None:
                record_identity(role, instance_id)
    except BaseException:
        terminate_instances(profile, list(identities.values()))
        raise
    return identities


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("config", help="canonical JSON config with build_launch_pair arguments")
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--profile", default="causality")
    args = parser.parse_args()
    with open(args.config, encoding="utf-8") as source:
        config = json.load(source)
    pair = build_launch_pair(**config)
    if args.execute:
        def announce(role: str, instance_id: str) -> None:
            _record_identity(args.profile, pair, role, instance_id)
            print(
                _canonical_bytes({"role": role, "instance_id": instance_id}).decode(),
                file=__import__("sys").stderr,
                flush=True,
            )

        pair["instance_ids"] = execute_pair(
            pair, profile=args.profile, record_identity=announce
        )
    print(_canonical_bytes(pair).decode("utf-8"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
