#!/usr/bin/env python3
"""Pure AWS execution contracts for Publication V3.

This module deliberately does not call AWS.  It turns an already validated
publication manifest into deterministic staging jobs and EC2 launch requests,
and classifies attempts from terminal marker names plus EC2 state.  The paid
controller can therefore be tested without credentials and cannot infer
success from measurement files.
"""

from __future__ import annotations

import argparse
import base64
import copy
import gzip
import hashlib
import json
import re
import shlex
import sys
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse

if __package__:
    from scripts.publication_v3_protocol import canonical_json_bytes, validate_manifest
else:  # Direct ``python scripts/...`` execution.
    from publication_v3_protocol import canonical_json_bytes, validate_manifest


COMPLETE_MARKERS = frozenset({"CELL_COMPLETE", "STAGING_COMPLETE.json"})
FAILURE_MARKERS = frozenset({"CELL_FAILED", "STAGING_FAILED.json"})
KNOWN_MARKERS = COMPLETE_MARKERS | FAILURE_MARKERS
HEX_64 = re.compile(r"[0-9a-f]{64}")
MAX_PARQUET_BYTES = 128 * 1024 * 1024
MAX_JSON_BYTES = 256 * 1024
MAX_DATASET_OBJECTS = 8_192
MAX_DATASET_BYTES = 1024 * 1024 * 1024 * 1024


@dataclass(frozen=True)
class StagingJob:
    dataset_id: str
    adapter: str
    attempt: int
    output_uri: str
    provenance_uri: str
    terminal_uri: str
    failure_uri: str


@dataclass(frozen=True)
class AttemptObservation:
    instance_state: str
    terminal_markers: tuple[str, ...]


@dataclass(frozen=True)
class AttemptDecision:
    action: str
    discard_measurements: bool


@dataclass(frozen=True)
class StagingReconciliation:
    action: str
    instance_id: str
    terminate_instance: bool
    discard_measurements: bool
    next_job: StagingJob | None


def _adapter(dataset: dict[str, object]) -> str:
    kind = str(dataset["kind"])
    if kind == "standard-ann":
        return "ann-benchmarks"
    if kind == "realistic-dense":
        return "vdbbench"
    if kind == "beir-hybrid":
        return "beir"
    raise ValueError(f"external dataset kind has no staging adapter: {kind}")


def staging_jobs(
    manifest: dict[str, object], *, attempt: int = 1
) -> tuple[StagingJob, ...]:
    """Return one deterministic first attempt for every external dataset."""

    normalized = validate_manifest(manifest)
    if attempt <= 0 or attempt > 9_999:
        raise ValueError("staging attempt must be in 1..=9999")
    dataset_prefix = str(normalized["prefixes"]["dataset"]).rstrip("/")
    jobs: list[StagingJob] = []
    for dataset in sorted(normalized["datasets"], key=lambda item: item["id"]):
        if dataset["source"]["state"] != "unstaged":
            continue
        dataset_id = str(dataset["id"])
        attempt_root = f"{dataset_prefix}/{dataset_id}/attempts/{attempt:04d}"
        jobs.append(
            StagingJob(
                dataset_id=dataset_id,
                adapter=_adapter(dataset),
                attempt=attempt,
                output_uri=f"{attempt_root}/materialized",
                provenance_uri=f"{attempt_root}/materialized.provenance.json",
                terminal_uri=f"{attempt_root}/STAGING_COMPLETE.json",
                failure_uri=f"{attempt_root}/STAGING_FAILED.json",
            )
        )
    return tuple(jobs)


def _s3_parts(uri: str) -> tuple[str, str]:
    parsed = urlparse(uri)
    key = parsed.path.lstrip("/")
    if (
        parsed.scheme != "s3"
        or not parsed.netloc
        or not key
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError("worker input must be a canonical S3 URI")
    return parsed.netloc, key


def build_staging_worker_script(
    manifest: dict[str, object],
    job: StagingJob,
    *,
    source_uri: str,
    source_archive_sha256: str,
    manifest_uri: str,
    manifest_sha256: str,
) -> str:
    """Build the self-diagnosing worker proven by the real SIFT staging smoke."""

    normalized = validate_manifest(manifest)
    expected = {
        item.dataset_id: item for item in staging_jobs(normalized, attempt=job.attempt)
    }
    if expected.get(job.dataset_id) != job:
        raise ValueError("staging worker job differs from the manifest")
    if (
        HEX_64.fullmatch(source_archive_sha256) is None
        or HEX_64.fullmatch(manifest_sha256) is None
    ):
        raise ValueError("worker source checksums must be lowercase SHA-256")
    source_bucket, _ = _s3_parts(source_uri)
    manifest_bucket, _ = _s3_parts(manifest_uri)
    failure_bucket, _ = _s3_parts(job.failure_uri)
    if len({source_bucket, manifest_bucket, failure_bucket}) != 1:
        raise ValueError("worker inputs and outputs must share the publication bucket")
    environment = normalized["environment_contract"]
    instance_type = str(environment["build_workers"]["borsuk"]["instance_type"])
    values = {
        "region": str(environment["region"]),
        "source_uri": source_uri,
        "source_sha": source_archive_sha256,
        "manifest_uri": manifest_uri,
        "manifest_sha": manifest_sha256,
        "dataset": job.dataset_id,
        "attempt": str(job.attempt),
        "instance_type": instance_type,
        "requirements": (
            "requirements-beir-stage.txt"
            if job.adapter == "beir"
            else "requirements-format-bench.txt"
        ),
        "pip_flags": (
            "--require-hashes --only-binary=:all: " if job.adapter == "beir" else ""
        ),
    }
    quoted = {key: shlex.quote(value) for key, value in values.items()}
    failure_trap = worker_failure_trap_script(stage_variable="phase")
    return f"""set -euo pipefail
export AWS_RETRY_MODE=adaptive AWS_MAX_ATTEMPTS=12 PYTHONDONTWRITEBYTECODE=1
region={quoted["region"]}
work=/var/lib/borsuk-publication-v3/{job.dataset_id}-a{job.attempt:04d}
mkdir -p "$work/source" "$work/dataset"
complete=0
phase=bootstrap
detail_log="$work/worker.log"
{failure_trap}
exec > >(tee -a "$work/worker.log") 2>&1
phase=identity; aws sts get-caller-identity
phase=python-runtime; dnf install -y python3.12 python3.12-pip
phase=source-fetch
aws --region "$region" s3 cp {quoted["source_uri"]} "$work/source.tar.gz" --only-show-errors
printf '%s  %s\n' {quoted["source_sha"]} "$work/source.tar.gz" | sha256sum -c -
aws --region "$region" s3 cp {quoted["manifest_uri"]} "$work/manifest.json" --only-show-errors
printf '%s  %s\n' {quoted["manifest_sha"]} "$work/manifest.json" | sha256sum -c -
phase=extract; tar -xzf "$work/source.tar.gz" -C "$work/source"
phase=python-env; python3.12 --version; python3.12 -m venv "$work/venv"
phase=dependencies; "$work/venv/bin/pip" install --disable-pip-version-check --no-cache-dir {values["pip_flags"]}-r "$work/source/scripts/{quoted["requirements"]}"
phase=metadata
token=$(curl -fsS -X PUT -H 'X-aws-ec2-metadata-token-ttl-seconds: 21600' http://169.254.169.254/latest/api/token)
instance_id=$(curl -fsS -H "X-aws-ec2-metadata-token: $token" http://169.254.169.254/latest/meta-data/instance-id)
az=$(curl -fsS -H "X-aws-ec2-metadata-token: $token" http://169.254.169.254/latest/meta-data/placement/availability-zone)
phase=promotion; cd "$work/source"
"$work/venv/bin/python" scripts/promote_publication_v3_dataset.py --manifest "$work/manifest.json" --dataset {quoted["dataset"]} --attempt {quoted["attempt"]} --work-root "$work/dataset" --source-archive-sha256 {quoted["source_sha"]} --instance-id "$instance_id" --instance-type {quoted["instance_type"]} --availability-zone "$az" --upload-workers 16 >"$work/receipt.json"
phase=complete
complete=1
"""


def _resource_contract(
    manifest: dict[str, object], role: str, system: str
) -> tuple[dict[str, object], dict[str, object]]:
    environment = manifest["environment_contract"]
    systems = environment["runtime_clients"]
    if system not in systems:
        raise ValueError(f"unknown publication system: {system}")
    if role == "runtime":
        resources = systems[system]
        storage = environment["runtime_storage"]
    elif role == "build":
        resources = environment["build_workers"][system]
        storage = environment["build_storage"]
    elif role == "staging":
        if system != "borsuk":
            raise ValueError(
                "dataset staging uses the single borsuk build-worker profile"
            )
        resources = environment["build_workers"]["borsuk"]
        storage = environment["build_storage"]
    else:
        raise ValueError("instance role must be runtime, build, or staging")
    return resources, storage


def _tags(
    campaign_id: str,
    cell_id: str,
    attempt: int,
    role: str,
    purchase_option: str,
) -> list[dict[str, str]]:
    if attempt <= 0:
        raise ValueError("attempt must be positive")
    if not campaign_id or not cell_id:
        raise ValueError("campaign and cell identifiers must be nonempty")
    values = {
        "Name": f"borsuk-{campaign_id}-{cell_id}-a{attempt:04d}",
        "Project": "BorsukBenchmark",
        "Campaign": campaign_id,
        "Cell": cell_id,
        "Attempt": str(attempt),
        "Role": role,
        "PurchaseOption": purchase_option,
        "AutoTerminate": "true",
    }
    return [{"Key": key, "Value": values[key]} for key in sorted(values)]


def worker_failure_trap_script(
    reporter_path: str | Path = "/var/lib/borsuk-publication-failure-reporter.sh",
    *,
    stage_variable: str = "stage",
) -> str:
    """Delegate worker EXIT/signals to the preinstalled bounded reporter."""

    if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", stage_variable) is None:
        raise ValueError("worker failure stage variable is invalid")
    reporter = shlex.quote(str(reporter_path))
    return f"""fail() {{
  status=$1
  trap - EXIT TERM INT
  trap '' TERM INT
  if [[ $complete -eq 0 ]]; then
    if [[ "$status" -eq 0 ]]; then status=1; fi
    timeout 45 /bin/bash {reporter} "$status" "${stage_variable}" "$detail_log" || true
  fi
  exit "$status"
}}
trap 'fail $?' EXIT
trap 'fail 143' TERM
trap 'fail 130' INT"""


def worker_supervisor_script(
    worker_path: str | Path,
    failure_reporter_path: str | Path,
    max_seconds: int,
    kill_after_seconds: int = 60,
    detail_log_path: str | Path = "/var/lib/borsuk-publication/worker.log",
) -> str:
    """Run one bounded worker and report every nonzero terminal outcome."""

    if max_seconds <= 0 or kill_after_seconds <= 0:
        raise ValueError("worker timeout and kill grace must be positive")
    worker = shlex.quote(str(worker_path))
    reporter = shlex.quote(str(failure_reporter_path))
    detail_log = shlex.quote(str(detail_log_path))
    return f"""status=0
if timeout --signal=TERM --kill-after={kill_after_seconds} {max_seconds} /bin/bash {worker}; then
  status=0
else
  status=$?
fi
if [[ "$status" -ne 0 ]]; then
  failure_stage=worker-failed
  if [[ "$status" -eq 124 || "$status" -eq 137 ]]; then
    failure_stage=controller-timeout
  fi
  /bin/bash {reporter} "$status" "$failure_stage" {detail_log} || true
fi
exit "$status"
"""


def terminal_failure_reporter_script(failure_uri: str) -> str:
    """Build the bounded reporter installed outside the timed worker."""

    bucket, failure_key = _s3_parts(failure_uri)
    failure_root = failure_key.rsplit("/", 1)[0]
    log_key = f"{failure_root}/FAILURE.log"
    canary_key = f"{failure_root}/RECEIPT_CANARY.json"
    return f"""#!/usr/bin/env bash
set -euo pipefail
work=${{BORSUK_FAILURE_WORK:-/dev/shm/borsuk-publication-failure-$$}}
mkdir -p "$work"
put_immutable() {{
  local path=$1 key=$2 digest checksum attempt error_log remote_digest
  digest=$(sha256sum "$path" | awk '{{print $1}}')
  checksum=$(openssl dgst -sha256 -binary "$path" | base64 -w0)
  error_log="$work/aws-error.log"
  for attempt in 1 2 3; do
    if timeout 5 aws s3api put-object --bucket {shlex.quote(bucket)} --key "$key" --body "$path" \
      --expected-bucket-owner 453182569524 --server-side-encryption AES256 \
      --checksum-algorithm SHA256 --checksum-sha256 "$checksum" \
      --metadata "borsuk-sha256=$digest" --if-none-match '*' >/dev/null 2>"$error_log"; then
      return 0
    fi
    if grep -Eq 'PreconditionFailed|(^|[^0-9])412([^0-9]|$)' "$error_log"; then
      remote_digest=$(timeout 5 aws s3api head-object --bucket {shlex.quote(bucket)} \
        --key "$key" --expected-bucket-owner 453182569524 \
        --query 'Metadata."borsuk-sha256"' --output text 2>>"$error_log" || true)
      if [[ "$remote_digest" = "$digest" ]]; then return 0; fi
    fi
    cat "$error_log" >&2 || true
    sleep 1
  done
  return 1
}}
if [[ ${{1:-}} = --canary ]]; then
  printf '{{"schema_version":1,"status":"receipt-channel-ready"}}\n' >"$work/canary.json"
  put_immutable "$work/canary.json" {shlex.quote(canary_key)}
  exit 0
fi
status=${{1:?missing failure status}}
stage=${{2:?missing failure stage}}
detail_log=${{3:-}}
if [[ ! "$status" =~ ^[1-9][0-9]*$ ]]; then status=1; fi
stage=${{stage,,}}
stage=${{stage//[^a-z0-9-]/-}}
stage=${{stage:0:64}}
if [[ -z "$stage" ]]; then stage=unknown; fi
printf '{{"schema_version":1,"status":"failed","exit_code":%d,"stage":"%s"}}\n' \
  "$status" "$stage" >"$work/failed.json"
receipt_status=0
put_immutable "$work/failed.json" {shlex.quote(failure_key)} || receipt_status=$?
if [[ -n "$detail_log" && -f "$detail_log" ]]; then
  tail -c 65536 "$detail_log" >"$work/FAILURE.log" || true
  if [[ -s "$work/FAILURE.log" ]]; then
    put_immutable "$work/FAILURE.log" {shlex.quote(log_key)} || true
  fi
fi
exit "$receipt_status"
"""


def build_launch_request(
    manifest: dict[str, object],
    *,
    role: str,
    system: str,
    image_id: str,
    subnet_id: str,
    security_group_id: str,
    instance_profile_arn: str,
    image_architecture: str,
    subnet_region: str,
    campaign_id: str,
    cell_id: str,
    attempt: int,
    worker_script: str,
    terminal_failure_uri: str,
    terminal_detail_log_path: str,
    max_seconds: int,
    purchase_option: str = "spot",
) -> dict[str, object]:
    """Build a hardened launch request; Spot is the mandatory default."""

    normalized = validate_manifest(manifest)
    if campaign_id != normalized["campaign_id"]:
        raise ValueError("launch campaign differs from the manifest campaign")
    environment = normalized["environment_contract"]
    if environment["spot_default"] is not True:
        raise ValueError("Publication V3 requires Spot by default")
    if purchase_option not in {"spot", "on-demand"}:
        raise ValueError("purchase option must be spot or on-demand")
    if purchase_option != "spot" and role != "runtime":
        raise ValueError("on-demand exception is allowed only for runtime")
    resources, storage = _resource_contract(normalized, role, system)
    if image_architecture != environment["architecture"]:
        raise ValueError("AMI architecture differs from the manifest architecture")
    if subnet_region != environment["region"]:
        raise ValueError("subnet region differs from the manifest region")
    expected_profile_prefix = (
        f"arn:aws:iam::{environment['aws_account']}:instance-profile/"
    )
    if not instance_profile_arn.startswith(expected_profile_prefix):
        raise ValueError("instance profile ARN differs from the manifest AWS account")
    budget_field = (
        "max_cell_seconds" if role == "runtime" else "max_index_build_seconds"
    )
    maximum_seconds = int(normalized["budget_contract"][budget_field])
    if max_seconds <= 0 or max_seconds > maximum_seconds:
        raise ValueError("worker timeout exceeds the manifest budget")
    if not worker_script or "\x00" in worker_script:
        raise ValueError("worker script must be nonempty UTF-8 text")
    allowed_failure_root = str(
        normalized["prefixes"]["dataset" if role == "staging" else "result"]
    ).rstrip("/")
    if not terminal_failure_uri.startswith(f"{allowed_failure_root}/"):
        raise ValueError("terminal failure URI escapes its publication prefix")
    _s3_parts(terminal_failure_uri)
    if not Path(terminal_detail_log_path).is_absolute() or "\x00" in terminal_detail_log_path:
        raise ValueError("terminal detail log path must be absolute UTF-8 text")
    worker_payload = base64.b64encode(
        gzip.compress(worker_script.encode("utf-8"), mtime=0)
    ).decode("ascii")
    reporter_payload = base64.b64encode(
        gzip.compress(
            terminal_failure_reporter_script(terminal_failure_uri).encode("utf-8"),
            mtime=0,
        )
    ).decode("ascii")
    worker_path = "/var/lib/borsuk-publication-worker.sh"
    reporter_path = "/var/lib/borsuk-publication-failure-reporter.sh"
    supervisor = worker_supervisor_script(
        worker_path, reporter_path, max_seconds, 60, terminal_detail_log_path
    )
    user_data = f"""#!/usr/bin/env bash
set -euo pipefail
export HOME=/root
finish() {{
  status=$?
  trap - EXIT
  shutdown -h now || true
  exit "$status"
}}
trap finish EXIT
printf '%s' '{worker_payload}' | base64 -d | gzip -d >{worker_path}
printf '%s' '{reporter_payload}' | base64 -d | gzip -d >{reporter_path}
chmod 700 {worker_path} {reporter_path}
if ! /bin/bash {reporter_path} --canary; then
  /bin/bash {reporter_path} 2 receipt-preflight-failed || true
  exit 2
fi
{supervisor}
"""
    encoded_user_data = base64.b64encode(user_data.encode("utf-8")).decode("ascii")
    if len(user_data.encode("utf-8")) > 16 * 1024:
        raise ValueError("EC2 user data exceeds its 16 KiB raw limit")
    for label, value in (
        ("image id", image_id),
        ("subnet id", subnet_id),
        ("security group id", security_group_id),
        ("instance profile ARN", instance_profile_arn),
    ):
        if not value:
            raise ValueError(f"{label} must be nonempty")
    volume = {
        "DeleteOnTermination": True,
        "Encrypted": True,
        "VolumeSize": storage["volume_size_gib"],
        "VolumeType": storage["volume_type"],
        "Iops": storage["iops"],
        "Throughput": storage["throughput_mib_s"],
    }
    block_devices = [
        {
            "DeviceName": "/dev/xvda",
            "Ebs": {**volume, "VolumeSize": 16} if role == "runtime" else volume,
        }
    ]
    if role == "runtime":
        block_devices.append({"DeviceName": "/dev/sdf", "Ebs": volume})
    request: dict[str, object] = {
        "ImageId": image_id,
        "InstanceType": resources["instance_type"],
        "MinCount": 1,
        "MaxCount": 1,
        "ClientToken": "borsuk-"
        + hashlib.sha256(
            (
                f"{campaign_id}\0{cell_id}\0{attempt}"
                + ("" if purchase_option == "spot" else "\0on-demand")
            ).encode("utf-8")
        ).hexdigest()[:40],
        "UserData": encoded_user_data,
        "SecurityGroupIds": [security_group_id],
        "SubnetId": subnet_id,
        "IamInstanceProfile": {"Arn": instance_profile_arn},
        "InstanceInitiatedShutdownBehavior": "terminate",
        "MetadataOptions": {
            "HttpEndpoint": "enabled",
            "HttpTokens": "required",
            "HttpPutResponseHopLimit": 1,
        },
        "BlockDeviceMappings": block_devices,
        "TagSpecifications": [
            {
                "ResourceType": resource_type,
                "Tags": _tags(campaign_id, cell_id, attempt, role, purchase_option),
            }
            for resource_type in ("instance", "volume")
        ],
    }
    if purchase_option == "spot":
        request["InstanceMarketOptions"] = {
            "MarketType": "spot",
            "SpotOptions": {
                "SpotInstanceType": "one-time",
                "InstanceInterruptionBehavior": "terminate",
            },
        }
    return request


def classify_attempt(observation: AttemptObservation) -> AttemptDecision:
    """Choose an action without opening any incomplete measurement artifact."""

    known_states = {
        "pending",
        "running",
        "stopping",
        "stopped",
        "shutting-down",
        "terminated",
    }
    if observation.instance_state not in known_states:
        raise ValueError(
            f"unrecognized EC2 instance state: {observation.instance_state}"
        )
    markers = set(observation.terminal_markers)
    unknown = markers - KNOWN_MARKERS
    if unknown:
        raise ValueError(f"unrecognized terminal marker: {sorted(unknown)[0]}")
    complete = bool(markers & COMPLETE_MARKERS)
    failed = bool(markers & FAILURE_MARKERS)
    if complete and failed:
        raise ValueError("conflicting terminal markers")
    if complete:
        return AttemptDecision("terminate-success", False)
    if failed:
        return AttemptDecision("terminate-failure", True)
    if observation.instance_state in {"terminated", "stopped"}:
        return AttemptDecision("retry-fresh-attempt", True)
    if observation.instance_state in {
        "pending",
        "running",
        "stopping",
        "shutting-down",
    }:
        return AttemptDecision("monitor", False)
    raise AssertionError("validated EC2 instance state was not classified")


def reconcile_staging_attempt(
    manifest: dict[str, object],
    job: StagingJob,
    *,
    instance_id: str,
    observation: AttemptObservation,
    terminal_receipt: dict[str, object] | None,
    max_attempts: int,
) -> StagingReconciliation:
    """Plan one idempotent staging action from authenticated terminal state."""

    expected = {
        item.dataset_id: item for item in staging_jobs(manifest, attempt=job.attempt)
    }
    if expected.get(job.dataset_id) != job:
        raise ValueError("reconciled staging job differs from the manifest")
    if re.fullmatch(r"i-[0-9a-f]{17}", instance_id) is None:
        raise ValueError("reconciled staging instance ID is invalid")
    if max_attempts <= 0 or max_attempts > 9_999:
        raise ValueError("staging retry cap must be in 1..=9999")
    if job.attempt > max_attempts:
        raise ValueError("staging attempt exceeds its retry cap")
    staging_markers = {"STAGING_COMPLETE.json", "STAGING_FAILED.json"}
    unexpected_markers = set(observation.terminal_markers) - staging_markers
    if unexpected_markers:
        raise ValueError(
            f"unrecognized staging terminal marker: {sorted(unexpected_markers)[0]}"
        )

    decision = classify_attempt(observation)
    if decision.action == "terminate-success":
        if terminal_receipt is None:
            raise ValueError("successful staging attempt is missing its receipt")
        receipt = validate_staging_receipt(manifest, terminal_receipt)
        if (
            receipt["dataset_id"] != job.dataset_id
            or receipt["attempt"] != job.attempt
            or receipt["instance_id"] != instance_id
        ):
            raise ValueError("staging receipt differs from the observed attempt")
    elif terminal_receipt is not None:
        raise ValueError("non-successful staging attempt must not carry a receipt")

    retry = decision.action in {"terminate-failure", "retry-fresh-attempt"}
    terminate_instance = observation.instance_state != "terminated" and (
        retry or decision.action == "terminate-success"
    )
    if retry and job.attempt == max_attempts:
        return StagingReconciliation(
            action="exhausted-failure",
            instance_id=instance_id,
            terminate_instance=terminate_instance,
            discard_measurements=True,
            next_job=None,
        )
    next_job = None
    if retry:
        next_job = next(
            item
            for item in staging_jobs(manifest, attempt=job.attempt + 1)
            if item.dataset_id == job.dataset_id
        )
    return StagingReconciliation(
        action=decision.action,
        instance_id=instance_id,
        terminate_instance=terminate_instance,
        discard_measurements=decision.discard_measurements,
        next_job=next_job,
    )


def _required_roles(adapter: str) -> dict[str, tuple[int, int | None]]:
    if adapter in {"ann-benchmarks", "vdbbench"}:
        return {
            "train": (1, None),
            "query": (1, 1),
            "ground-truth": (1, 1),
            "metadata": (1, 1),
        }
    if adapter == "beir":
        return {
            "corpus": (1, None),
            "query": (1, None),
            "qrels": (1, 1),
            "metadata": (1, 1),
        }
    raise ValueError(f"unsupported staging adapter {adapter}")


def build_staging_receipt(
    manifest: dict[str, object],
    job: StagingJob,
    *,
    source_archive_sha256: str,
    source_provenance: dict[str, object],
    provenance_sha256: str,
    objects: tuple[dict[str, object], ...] | list[dict[str, object]],
    instance_id: str,
    instance_type: str,
    availability_zone: str,
    purchase_option: str,
) -> dict[str, object]:
    """Validate the immutable materialization roster and form its terminal receipt."""

    normalized_manifest = validate_manifest(manifest)
    expected_jobs = {
        item.dataset_id: item
        for item in staging_jobs(normalized_manifest, attempt=job.attempt)
    }
    if expected_jobs.get(job.dataset_id) != job:
        raise ValueError("staging job differs from the manifest-derived job")
    if purchase_option != "spot":
        raise ValueError("dataset staging must use Spot capacity")
    if HEX_64.fullmatch(source_archive_sha256) is None:
        raise ValueError("source archive checksum must be lowercase SHA-256")
    if HEX_64.fullmatch(provenance_sha256) is None:
        raise ValueError("provenance checksum must be lowercase SHA-256")
    if len(objects) < 4:
        raise ValueError("dataset staging requires a multi-object materialization")
    if len(objects) > MAX_DATASET_OBJECTS:
        raise ValueError("dataset staging object roster exceeds its bound")
    normalized: list[dict[str, object]] = []
    uris: set[str] = set()
    allowed_roles = _required_roles(job.adapter)
    for item in objects:
        if frozenset(item) != frozenset(
            {"role", "format", "uri", "sha256", "bytes", "rows"}
        ):
            raise ValueError("staged object fields differ")
        role = str(item["role"])
        object_format = str(item["format"])
        uri = str(item["uri"])
        digest = str(item["sha256"])
        size = item["bytes"]
        rows = item["rows"]
        size_cap = MAX_JSON_BYTES if object_format == "json" else MAX_PARQUET_BYTES
        if role not in allowed_roles:
            raise ValueError(f"staged object role is invalid for {job.adapter}: {role}")
        if object_format not in {"json", "parquet"}:
            raise ValueError("staged objects must use stock JSON or Parquet")
        if role == "metadata" and object_format != "json":
            raise ValueError("dataset metadata must use JSON")
        if role != "metadata" and object_format != "parquet":
            raise ValueError("dataset data objects must use Parquet")
        output_prefix = job.output_uri.rstrip("/") + "/"
        if not uri.startswith(output_prefix) or uri in uris:
            raise ValueError("staged object URI is outside the attempt or duplicated")
        relative_path = uri.removeprefix(output_prefix)
        if (
            not relative_path
            or relative_path.startswith("/")
            or ".." in relative_path.split("/")
        ):
            raise ValueError("staged object path is not canonical")
        if HEX_64.fullmatch(digest) is None:
            raise ValueError("staged object checksum must be lowercase SHA-256")
        if (
            not isinstance(size, int)
            or isinstance(size, bool)
            or size <= 0
            or size > size_cap
            or not isinstance(rows, int)
            or isinstance(rows, bool)
            or rows <= 0
        ):
            raise ValueError("staged object bytes or rows exceed the format contract")
        uris.add(uri)
        normalized.append(
            {
                "role": role,
                "format": object_format,
                "uri": uri,
                "sha256": digest,
                "bytes": size,
                "rows": rows,
            }
        )
    counts = {role: 0 for role in allowed_roles}
    for item in normalized:
        counts[str(item["role"])] += 1
    for role, (minimum, maximum) in allowed_roles.items():
        count = counts[role]
        if count < minimum or (maximum is not None and count > maximum):
            raise ValueError(f"dataset materialization has invalid {role} object count")
    expected_environment = normalized_manifest["environment_contract"]
    expected_instance_type = expected_environment["build_workers"]["borsuk"][
        "instance_type"
    ]
    if instance_type != expected_instance_type:
        raise ValueError("staging instance type differs from the build-worker contract")
    if not availability_zone.startswith(str(expected_environment["region"])):
        raise ValueError("staging availability zone differs from the manifest region")
    for label, value in (
        ("instance id", instance_id),
        ("availability zone", availability_zone),
    ):
        if not value:
            raise ValueError(f"{label} must be nonempty")
    normalized.sort(key=lambda item: str(item["uri"]))
    object_bytes = sum(int(item["bytes"]) for item in normalized)
    if object_bytes > MAX_DATASET_BYTES:
        raise ValueError("dataset staging bytes exceed the one-TiB campaign bound")
    content_identity = [
        {
            "role": item["role"],
            "format": item["format"],
            "path": str(item["uri"]).removeprefix(job.output_uri.rstrip("/") + "/"),
            "sha256": item["sha256"],
            "bytes": item["bytes"],
            "rows": item["rows"],
        }
        for item in normalized
    ]
    dataset_content_sha256 = hashlib.sha256(
        canonical_json_bytes(content_identity)
    ).hexdigest()
    dataset = next(
        item for item in normalized_manifest["datasets"] if item["id"] == job.dataset_id
    )
    required_provenance = {
        "schema_version",
        "dataset",
        "source",
        "source_sha256",
        "materialization_sha256",
    }
    allowed_provenance = required_provenance | {"source_descriptor_sha256"}
    digest_fields = {"source_sha256", "materialization_sha256"}
    if "source_descriptor_sha256" in source_provenance:
        digest_fields.add("source_descriptor_sha256")
    if (
        frozenset(source_provenance) - allowed_provenance
        or not required_provenance.issubset(source_provenance)
        or source_provenance["schema_version"] != 1
        or source_provenance["dataset"] != job.dataset_id
        or source_provenance["source"] != dataset["source"]["expected_source"]
        or source_provenance["materialization_sha256"] != dataset_content_sha256
        or any(
            HEX_64.fullmatch(str(source_provenance[field])) is None
            for field in digest_fields
        )
    ):
        raise ValueError(
            "source provenance differs from the staged dataset or manifest"
        )
    return {
        "schema_version": 1,
        "campaign_id": normalized_manifest["campaign_id"],
        "manifest_sha256": hashlib.sha256(
            canonical_json_bytes(normalized_manifest)
        ).hexdigest(),
        "dataset_id": job.dataset_id,
        "adapter": job.adapter,
        "attempt": job.attempt,
        "source_archive_sha256": source_archive_sha256,
        "source_provenance": dict(source_provenance),
        "provenance_sha256": provenance_sha256,
        "dataset_content_sha256": dataset_content_sha256,
        "output_uri": job.output_uri,
        "provenance_uri": job.provenance_uri,
        "terminal_uri": job.terminal_uri,
        "failure_uri": job.failure_uri,
        "instance_id": instance_id,
        "instance_type": instance_type,
        "availability_zone": availability_zone,
        "purchase_option": purchase_option,
        "object_count": len(normalized),
        "object_bytes": object_bytes,
        "objects": normalized,
    }


def validate_staging_receipt(
    manifest: dict[str, object], value: dict[str, object]
) -> dict[str, object]:
    """Rebuild a terminal receipt from its authenticated inputs and compare exactly."""

    if not isinstance(value, dict):
        raise ValueError("staging receipt must be a JSON object")
    required = {
        "schema_version",
        "campaign_id",
        "manifest_sha256",
        "dataset_id",
        "adapter",
        "attempt",
        "source_archive_sha256",
        "source_provenance",
        "provenance_sha256",
        "dataset_content_sha256",
        "output_uri",
        "provenance_uri",
        "terminal_uri",
        "failure_uri",
        "instance_id",
        "instance_type",
        "availability_zone",
        "purchase_option",
        "object_count",
        "object_bytes",
        "objects",
    }
    if frozenset(value) != required:
        raise ValueError("staging receipt fields differ")
    attempt = value["attempt"]
    if not isinstance(attempt, int) or isinstance(attempt, bool):
        raise ValueError("staging receipt attempt is invalid")
    jobs = {item.dataset_id: item for item in staging_jobs(manifest, attempt=attempt)}
    job = jobs.get(str(value["dataset_id"]))
    if job is None:
        raise ValueError("staging receipt dataset is not in the manifest")
    rebuilt = build_staging_receipt(
        manifest,
        job,
        source_archive_sha256=str(value["source_archive_sha256"]),
        source_provenance=value["source_provenance"],
        provenance_sha256=str(value["provenance_sha256"]),
        objects=value["objects"],
        instance_id=str(value["instance_id"]),
        instance_type=str(value["instance_type"]),
        availability_zone=str(value["availability_zone"]),
        purchase_option=str(value["purchase_option"]),
    )
    if rebuilt != value:
        raise ValueError("staging receipt differs from its manifest-derived identity")
    return value


def promote_staging_receipts(
    manifest: dict[str, object],
    receipts: list[dict[str, object]],
    *,
    historical_manifests: dict[str, dict[str, object]] | None = None,
) -> dict[str, object]:
    """Fold immutable staging receipts into one partially staged manifest."""

    normalized = validate_manifest(manifest)
    if not receipts:
        raise ValueError("manifest promotion requires at least one staging receipt")
    current_manifest_sha256 = hashlib.sha256(
        canonical_json_bytes(normalized)
    ).hexdigest()
    historical_manifests = historical_manifests or {}
    current_datasets = {
        str(dataset["id"]): dataset for dataset in normalized["datasets"]
    }
    validated: list[dict[str, object]] = []
    for receipt in receipts:
        receipt_manifest_sha256 = str(receipt.get("manifest_sha256", ""))
        if receipt_manifest_sha256 == current_manifest_sha256:
            validated.append(validate_staging_receipt(normalized, receipt))
            continue
        historical_value = historical_manifests.get(receipt_manifest_sha256)
        if historical_value is None:
            raise ValueError(
                "historical staging receipt requires its exact manifest authority"
            )
        historical = validate_manifest(historical_value)
        if (
            hashlib.sha256(canonical_json_bytes(historical)).hexdigest()
            != receipt_manifest_sha256
        ):
            raise ValueError("historical staging manifest checksum differs")
        validated_receipt = validate_staging_receipt(historical, receipt)
        dataset_id = str(validated_receipt["dataset_id"])
        historical_dataset = next(
            (dataset for dataset in historical["datasets"] if dataset["id"] == dataset_id),
            None,
        )
        if historical_dataset != current_datasets.get(dataset_id):
            raise ValueError("historical staging dataset contract differs")
        if (
            historical["campaign_id"] != normalized["campaign_id"]
            or historical["prefixes"]["dataset"]
            != normalized["prefixes"]["dataset"]
        ):
            raise ValueError("historical staging campaign authority differs")
        validated.append(validated_receipt)
    by_dataset: dict[str, dict[str, object]] = {}
    for receipt in validated:
        dataset_id = str(receipt["dataset_id"])
        if dataset_id in by_dataset:
            raise ValueError("manifest promotion has duplicate staging receipts")
        by_dataset[dataset_id] = receipt
    promoted = copy.deepcopy(normalized)
    for dataset in promoted["datasets"]:
        receipt = by_dataset.get(str(dataset["id"]))
        if receipt is None:
            continue
        source = dataset["source"]
        if source["state"] != "unstaged":
            raise ValueError("manifest promotion dataset is not unstaged")
        dataset["source"] = {
            "state": "staged",
            "url": receipt["output_uri"],
            "sha256": receipt["dataset_content_sha256"],
            "license": source["license"],
        }
    return validate_manifest(promoted)


def _staging_plan(manifest: dict[str, object]) -> dict[str, object]:
    normalized = validate_manifest(manifest)
    jobs = staging_jobs(normalized)
    return {
        "schema_version": 1,
        "campaign_id": normalized["campaign_id"],
        "manifest_sha256": hashlib.sha256(canonical_json_bytes(normalized)).hexdigest(),
        "job_count": len(jobs),
        "jobs": [
            {
                "dataset_id": job.dataset_id,
                "adapter": job.adapter,
                "attempt": job.attempt,
                "output_uri": job.output_uri,
                "terminal_uri": job.terminal_uri,
                "failure_uri": job.failure_uri,
            }
            for job in jobs
        ],
    }


def _reconciliation_value(value: StagingReconciliation) -> dict[str, object]:
    next_job = None
    if value.next_job is not None:
        next_job = {
            "dataset_id": value.next_job.dataset_id,
            "adapter": value.next_job.adapter,
            "attempt": value.next_job.attempt,
            "output_uri": value.next_job.output_uri,
            "provenance_uri": value.next_job.provenance_uri,
            "terminal_uri": value.next_job.terminal_uri,
            "failure_uri": value.next_job.failure_uri,
        }
    return {
        "schema_version": 1,
        "action": value.action,
        "instance_id": value.instance_id,
        "terminate_instance": value.terminate_instance,
        "discard_measurements": value.discard_measurements,
        "next_job": next_job,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="operation", required=True)
    plan = subparsers.add_parser("plan-staging")
    plan.add_argument("manifest", type=Path)
    reconcile = subparsers.add_parser("reconcile-staging")
    reconcile.add_argument("manifest", type=Path)
    reconcile.add_argument("--dataset", required=True)
    reconcile.add_argument("--attempt", type=int, required=True)
    reconcile.add_argument("--instance-id", required=True)
    reconcile.add_argument("--instance-state", required=True)
    reconcile.add_argument("--terminal-marker", action="append", default=[])
    reconcile.add_argument("--receipt", type=Path)
    reconcile.add_argument("--max-attempts", type=int, default=3)
    args = parser.parse_args()
    manifest = json.loads(args.manifest.read_text(encoding="utf-8"))
    if args.operation == "plan-staging":
        print(canonical_json_bytes(_staging_plan(manifest)).decode("utf-8"))
        return 0
    if args.operation == "reconcile-staging":
        jobs = {
            job.dataset_id: job for job in staging_jobs(manifest, attempt=args.attempt)
        }
        job = jobs.get(args.dataset)
        if job is None:
            raise ValueError("reconciled dataset is not an unstaged manifest dataset")
        receipt = None
        if args.receipt is not None:
            receipt = json.loads(args.receipt.read_text(encoding="utf-8"))
        value = reconcile_staging_attempt(
            manifest,
            job,
            instance_id=args.instance_id,
            observation=AttemptObservation(
                instance_state=args.instance_state,
                terminal_markers=tuple(args.terminal_marker),
            ),
            terminal_receipt=receipt,
            max_attempts=args.max_attempts,
        )
        print(canonical_json_bytes(_reconciliation_value(value)).decode("utf-8"))
        return 0
    raise AssertionError("unreachable operation")


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, ValueError, json.JSONDecodeError) as error:
        print(f"publication-v3 AWS plan failed: {error}", file=sys.stderr)
        raise SystemExit(2) from None
