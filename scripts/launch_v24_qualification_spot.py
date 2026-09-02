#!/usr/bin/env python3
"""Launch one authenticated V24 qualification phase on EC2 Spot."""

from __future__ import annotations

import argparse
import base64
import dataclasses
import hashlib
import json
import pathlib
import re
import secrets
import shlex
import time
import urllib.parse
from collections.abc import Callable, Sequence
from typing import Any

EXPECTED_AWS_ACCOUNT = "453182569524"
PROFILE = "causality"
REGION = "eu-central-1"
AMI_ID = "ami-07bcecd13a160173f"
SECURITY_GROUP_ID = "sg-0b1fd3e4fbde4af0d"
INSTANCE_PROFILE = "borsuk-bench-profile"
INSTANCE_TYPE = "m7g.8xlarge"
_CONTROLLER_WALL_SECONDS = {
    "input-preparation": 21_600,
    "witness-training": 10_800,
    "posting-construction": 10_800,
    "development-evaluation": 10_800,
    "holdout-binding": 10_800,
    "holdout-evaluation": 10_800,
}
PHASES = (
    "input-preparation",
    "witness-training",
    "posting-construction",
    "development-evaluation",
    "holdout-binding",
    "holdout-evaluation",
)
_RUNNER_PHASES = {
    "witness-training": "train-witnesses",
    "posting-construction": "build-postings",
    "development-evaluation": "evaluate-development",
    "holdout-binding": "bind-holdout",
    "holdout-evaluation": "evaluate-holdout",
}
_LOWER_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_LOWER_GIT = re.compile(r"[0-9a-f]{40}\Z")
_CAPACITY_ERRORS = {
    "InsufficientInstanceCapacity",
    "InsufficientFreeAddressesInSubnet",
    "SpotMaxPriceTooLow",
    "Unsupported",
}
_TRANSIENT_ERRORS = {
    "InternalError",
    "InternalFailure",
    "PriorRequestNotComplete",
    "RequestLimitExceeded",
    "RequestTimeout",
    "RequestTimeoutException",
    "ServiceUnavailable",
    "SlowDown",
    "Throttling",
    "ThrottlingException",
    "Unavailable",
}


@dataclasses.dataclass(frozen=True)
class SpotTarget:
    """One eligible public subnet in an independent availability zone."""

    availability_zone: str
    subnet_id: str


SPOT_TARGETS = (
    SpotTarget("eu-central-1c", "subnet-0a12dbed0ca6fac25"),
    SpotTarget("eu-central-1b", "subnet-00243d923761c047c"),
    SpotTarget("eu-central-1a", "subnet-034528fbd6977848f"),
)


@dataclasses.dataclass(frozen=True)
class V24SpotPlan:
    """Complete immutable authority for one fresh V24 phase worker."""

    run_id: str
    phase: str
    source_commit: str
    source_archive_uri: str
    source_archive_sha256: str
    source_archive_bytes: int
    binary_uri: str
    binary_sha256: str
    binary_bytes: int
    manifest_uri: str
    manifest_sha256: str
    manifest_bytes: int
    output_prefix: str


def _s3(value: str, *, prefix: bool = False) -> tuple[str, str]:
    parsed = urllib.parse.urlsplit(value)
    if (
        parsed.scheme != "s3"
        or not parsed.netloc
        or not parsed.path.startswith("/")
        or parsed.path == "/"
        or parsed.query
        or parsed.fragment
        or ".." in pathlib.PurePosixPath(parsed.path).parts
    ):
        raise ValueError("V24 qualification S3 URI differs")
    key = parsed.path[1:]
    if prefix != key.endswith("/"):
        raise ValueError("V24 qualification S3 prefix differs")
    return parsed.netloc, key


def build_v24_spot_plan(**values: Any) -> V24SpotPlan:
    """Validate and freeze one qualification phase authority."""

    plan = V24SpotPlan(**values)
    if (
        re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}", plan.run_id) is None
        or plan.phase not in PHASES
        or _LOWER_GIT.fullmatch(plan.source_commit) is None
        or _LOWER_SHA256.fullmatch(plan.source_archive_sha256) is None
        or _LOWER_SHA256.fullmatch(plan.binary_sha256) is None
        or _LOWER_SHA256.fullmatch(plan.manifest_sha256) is None
        or min(
            plan.source_archive_bytes,
            plan.binary_bytes,
            plan.manifest_bytes,
        )
        <= 0
    ):
        raise ValueError("V24 qualification Spot plan differs")
    _s3(plan.source_archive_uri)
    _s3(plan.binary_uri)
    _s3(plan.manifest_uri)
    _s3(plan.output_prefix, prefix=True)
    return plan


def canonical_json_bytes(value: object) -> bytes:
    """Encode exact newline-terminated canonical JSON."""

    return (
        json.dumps(
            value, allow_nan=False, separators=(",", ":"), sort_keys=True
        ).encode()
        + b"\n"
    )


def controller_wall_seconds(phase: str) -> int:
    """Return the outer cap, including boot, staging, science, and publication."""

    try:
        return _CONTROLLER_WALL_SECONDS[phase]
    except KeyError as error:
        raise ValueError("V24 qualification phase differs") from error


def canonical_v24_terminal_bytes(
    plan: V24SpotPlan,
    *,
    instance_id: str,
    status: str,
    result_sha256: str | None = None,
    result_bytes: int | None = None,
    worker_status: int | None = None,
) -> bytes:
    """Build one exact success or failure terminal."""

    if not instance_id.startswith("i-") or status not in {"complete", "failed"}:
        raise ValueError("V24 qualification terminal identity differs")
    value: dict[str, object] = {
        "binary_bytes": plan.binary_bytes,
        "binary_sha256": plan.binary_sha256,
        "binary_uri": plan.binary_uri,
        "claim_eligible": False,
        "instance_id": instance_id,
        "manifest_bytes": plan.manifest_bytes,
        "manifest_sha256": plan.manifest_sha256,
        "manifest_uri": plan.manifest_uri,
        "phase": plan.phase,
        "run_id": plan.run_id,
        "schema": "borsuk-v24-qualification-spot-terminal-v1",
        "source_archive_bytes": plan.source_archive_bytes,
        "source_archive_sha256": plan.source_archive_sha256,
        "source_archive_uri": plan.source_archive_uri,
        "source_commit": plan.source_commit,
        "status": status,
    }
    if status == "complete":
        if (
            result_sha256 is None
            or _LOWER_SHA256.fullmatch(result_sha256) is None
            or type(result_bytes) is not int  # noqa: E721
            or result_bytes <= 0
            or worker_status is not None
        ):
            raise ValueError("V24 qualification complete terminal differs")
        value["result_bytes"] = result_bytes
        value["result_sha256"] = result_sha256
    else:
        if (
            type(worker_status) is not int  # noqa: E721
            or worker_status < 0
            or result_sha256 is not None
            or result_bytes is not None
        ):
            raise ValueError("V24 qualification failure terminal differs")
        value["worker_status"] = worker_status
    return canonical_json_bytes(value)


def validate_v24_terminal_bytes(raw: bytes, plan: V24SpotPlan, status: str) -> None:
    """Authenticate one terminal and every immutable binding."""

    try:
        value = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("V24 qualification terminal JSON differs") from error
    if raw != canonical_json_bytes(value) or type(value) is not dict:  # noqa: E721
        raise ValueError("V24 qualification terminal encoding differs")
    instance_id = value.get("instance_id")
    if type(instance_id) is not str:  # noqa: E721
        raise ValueError("V24 qualification terminal instance differs")
    expected = canonical_v24_terminal_bytes(
        plan,
        instance_id=instance_id,
        status=status,
        result_sha256=value.get("result_sha256"),
        result_bytes=value.get("result_bytes"),
        worker_status=value.get("worker_status"),
    )
    if raw != expected:
        raise ValueError("V24 qualification terminal authority differs")


def _worker_script(plan: V24SpotPlan) -> str:
    bucket, prefix = _s3(plan.output_prefix, prefix=True)
    runner_phase = _RUNNER_PHASES.get(plan.phase, "input-preparation")
    quoted = {
        "archive_uri": shlex.quote(plan.source_archive_uri),
        "archive_sha": shlex.quote(plan.source_archive_sha256),
        "archive_bytes": str(plan.source_archive_bytes),
        "binary_uri": shlex.quote(plan.binary_uri),
        "binary_sha": shlex.quote(plan.binary_sha256),
        "binary_bytes": str(plan.binary_bytes),
        "manifest_uri": shlex.quote(plan.manifest_uri),
        "manifest_sha": shlex.quote(plan.manifest_sha256),
        "manifest_bytes": str(plan.manifest_bytes),
        "bucket": shlex.quote(bucket),
        "prefix": shlex.quote(prefix),
        "commit": shlex.quote(plan.source_commit),
        "run_id": shlex.quote(plan.run_id),
        "phase": shlex.quote(plan.phase),
        "runner_phase": shlex.quote(runner_phase),
    }
    shutdown_minutes = 360 if plan.phase == "input-preparation" else 180
    return f"""#!/bin/bash
set -Eeuo pipefail
umask 077
shutdown --poweroff +{shutdown_minutes}
root=/opt/borsuk-v24-qualification
workspace="$root/source"
inputs="$root/inputs"
outputs="$root/outputs"
scratch="$root/scratch"
archive="$root/source.tar.zst"
binary="$root/v24-phase-binary"
manifest="$root/manifest.json"
staging_receipt="$root/staging-receipt.json"
mkdir -p "$workspace" "$outputs" "$scratch"
touch "$root/worker.log"
exec >>"$root/worker.log" 2>&1
output_bucket={quoted["bucket"]}
output_prefix={quoted["prefix"]}
construction_uri="s3://$output_bucket/${{output_prefix}}construction-rows.parquet"
page_rows_uri="s3://$output_bucket/${{output_prefix}}page-rows.parquet"
run_id={quoted["run_id"]}
phase={quoted["phase"]}
runner_phase={quoted["runner_phase"]}
source_commit={quoted["commit"]}
source_archive_uri={quoted["archive_uri"]}
source_archive_sha256={quoted["archive_sha"]}
source_archive_bytes={quoted["archive_bytes"]}
binary_uri={quoted["binary_uri"]}
binary_sha256={quoted["binary_sha"]}
binary_bytes={quoted["binary_bytes"]}
manifest_uri={quoted["manifest_uri"]}
manifest_sha256={quoted["manifest_sha"]}
manifest_bytes={quoted["manifest_bytes"]}
imds_token="$(curl -fsS -X PUT -H 'X-aws-ec2-metadata-token-ttl-seconds: 21600' http://169.254.169.254/latest/api/token)"
instance_id="$(curl -fsS -H "X-aws-ec2-metadata-token: $imds_token" http://169.254.169.254/latest/meta-data/instance-id)"
terminal=failed
put_once() {{
  aws s3api put-object --bucket "$output_bucket" --key "$output_prefix$2" \
    --body "$1" --if-none-match '*' --expected-bucket-owner {EXPECTED_AWS_ACCOUNT} \
    --checksum-algorithm SHA256 >/dev/null
}}
finish() {{
  status=$?
  trap - EXIT
  set +e
  if [[ "$terminal" != complete ]]; then
    python3 - "$root/ATTEMPT_FAILED.json" "$run_id" "$phase" "$source_commit" "$source_archive_uri" "$source_archive_sha256" "$source_archive_bytes" "$binary_uri" "$binary_sha256" "$binary_bytes" "$manifest_uri" "$manifest_sha256" "$manifest_bytes" "$instance_id" "$status" <<'PY'
import json,sys
path,run_id,phase,commit,archive_uri,archive_sha,archive_bytes,binary_uri,binary_sha,binary_bytes,manifest_uri,manifest_sha,manifest_bytes,instance_id,status=sys.argv[1:]
value={{"binary_bytes":int(binary_bytes),"binary_sha256":binary_sha,"binary_uri":binary_uri,"claim_eligible":False,"instance_id":instance_id,"manifest_bytes":int(manifest_bytes),"manifest_sha256":manifest_sha,"manifest_uri":manifest_uri,"phase":phase,"run_id":run_id,"schema":"borsuk-v24-qualification-spot-terminal-v1","source_archive_bytes":int(archive_bytes),"source_archive_sha256":archive_sha,"source_archive_uri":archive_uri,"source_commit":commit,"status":"failed","worker_status":int(status)}}
open(path,"wb").write(json.dumps(value,separators=(",",":"),sort_keys=True).encode()+b"\n")
PY
    put_once "$root/worker.log" worker.log || true
    put_once "$root/ATTEMPT_FAILED.json" ATTEMPT_FAILED.json || true
  else
    put_once "$root/worker.log" worker.log || true
  fi
  shutdown -h now
}}
trap finish EXIT
dnf install -y python3 python3-pip tar zstd
python3 -m pip install boto3==1.34.46 blake3==1.0.8
aws s3api put-object --generate-cli-skeleton input | grep -q '"IfNoneMatch"'
aws s3 cp {quoted["archive_uri"]} "$archive" --only-show-errors
aws s3 cp {quoted["binary_uri"]} "$binary" --only-show-errors
aws s3 cp {quoted["manifest_uri"]} "$manifest" --only-show-errors
test "$(stat -c %s "$archive")" -eq {quoted["archive_bytes"]}
test "$(stat -c %s "$binary")" -eq {quoted["binary_bytes"]}
test "$(stat -c %s "$manifest")" -eq {quoted["manifest_bytes"]}
printf '%s  %s\n' {quoted["archive_sha"]} "$archive" | sha256sum --check --status
printf '%s  %s\n' {quoted["binary_sha"]} "$binary" | sha256sum --check --status
printf '%s  %s\n' {quoted["manifest_sha"]} "$manifest" | sha256sum --check --status
chmod 0555 "$binary"
tar --zstd -xf "$archive" -C "$workspace"
test "$(cat "$workspace/.borsuk-source-commit")" = "$source_commit"
cd "$workspace"
python3 - "$manifest" "$manifest_sha256" "$inputs" "$staging_receipt" <<'PY'
import pathlib,sys,boto3
from scripts.stage_v24_witness_inputs import stage_manifest
manifest,digest,inputs,receipt=sys.argv[1:]
stage_manifest(pathlib.Path(manifest),digest,pathlib.Path(inputs),pathlib.Path(receipt),boto3.Session(region_name="{REGION}").client("s3"))
PY
if [[ "$phase" == input-preparation ]]; then
  python3 - "$binary" "$manifest" "$manifest_sha256" "$inputs" "$outputs" "$scratch" "$construction_uri" "$page_rows_uri" <<'PY' >"$root/stdout.json"
import pathlib,subprocess,sys
from scripts.run_v24_witness_page_router import MonitorLimits,offline_environment,monitor_process_group
binary,manifest,digest,inputs,outputs,scratch,construction_uri,page_rows_uri=sys.argv[1:]
command=[binary,"--manifest",manifest,"--manifest-sha256",digest,"--input-dir",inputs,"--output-dir",outputs,"--construction-uri",construction_uri,"--page-rows-uri",page_rows_uri,"--execute-preparation"]
process=subprocess.Popen(command,start_new_session=True,env=offline_environment(pathlib.Path(scratch)))
status,reason=monitor_process_group(process.pid,MonitorLimits.for_phase("prepare-inputs"),progress_path=pathlib.Path(outputs)/"progress.json",progress_phase="input-preparation")
process.returncode=status
if reason is not None:
    raise RuntimeError(f"V24 preparation stopped: {{reason}}")
if status != 0:
    raise RuntimeError(f"V24 preparation exited {{status}}")
PY
  result_path="$outputs/preparation-receipt.json"
else
  python3 scripts/run_v24_witness_page_router.py \
    --phase "$runner_phase" --executable "$binary" \
    --executable-sha256 "$binary_sha256" --executable-bytes {quoted["binary_bytes"]} \
    --manifest "$manifest" --manifest-sha256 "$manifest_sha256" \
    --staging-receipt "$staging_receipt" --input-dir "$inputs" \
    --output-dir "$outputs" --scratch "$scratch" >"$root/stdout.json"
  if [[ "$phase" == holdout-binding ]]; then
    result_path="$outputs/holdout-binding.json"
  else
    result_path="$outputs/result.json"
  fi
fi
cmp "$root/stdout.json" "$result_path"
result_sha256="$(sha256sum "$root/stdout.json" | awk '{{print $1}}')"
result_bytes="$(stat -c %s "$root/stdout.json")"
if [[ "$phase" == input-preparation ]]; then
  put_once "$outputs/construction-rows.parquet" construction-rows.parquet
  put_once "$outputs/page-rows.parquet" page-rows.parquet
  put_once "$root/stdout.json" preparation-receipt.json
elif [[ "$phase" == witness-training ]]; then
  put_once "$outputs/witness-graph.arrow" witness-graph.arrow
  put_once "$outputs/witnesses.arrow" witnesses.arrow
  put_once "$root/stdout.json" training-result.json
elif [[ "$phase" == posting-construction ]]; then
  put_once "$outputs/witness-postings.arrow" witness-postings.arrow
  put_once "$root/stdout.json" result.json
elif [[ "$phase" == development-evaluation ]]; then
  put_once "$root/stdout.json" development-result.json
elif [[ "$phase" == holdout-binding ]]; then
  put_once "$root/stdout.json" holdout-binding.json
elif [[ "$phase" == holdout-evaluation ]]; then
  put_once "$root/stdout.json" holdout-result.json
else
  put_once "$root/stdout.json" result.json
fi
put_once "$outputs/progress.json" progress.json
put_once "$staging_receipt" staging-receipt.json
python3 - "$root/ATTEMPT_COMPLETE.json" "$run_id" "$phase" "$source_commit" "$source_archive_uri" "$source_archive_sha256" "$source_archive_bytes" "$binary_uri" "$binary_sha256" "$binary_bytes" "$manifest_uri" "$manifest_sha256" "$manifest_bytes" "$instance_id" "$result_sha256" "$result_bytes" <<'PY'
import json,sys
path,run_id,phase,commit,archive_uri,archive_sha,archive_bytes,binary_uri,binary_sha,binary_bytes,manifest_uri,manifest_sha,manifest_bytes,instance_id,result_sha,result_bytes=sys.argv[1:]
value={{"binary_bytes":int(binary_bytes),"binary_sha256":binary_sha,"binary_uri":binary_uri,"claim_eligible":False,"instance_id":instance_id,"manifest_bytes":int(manifest_bytes),"manifest_sha256":manifest_sha,"manifest_uri":manifest_uri,"phase":phase,"result_bytes":int(result_bytes),"result_sha256":result_sha,"run_id":run_id,"schema":"borsuk-v24-qualification-spot-terminal-v1","source_archive_bytes":int(archive_bytes),"source_archive_sha256":archive_sha,"source_archive_uri":archive_uri,"source_commit":commit,"status":"complete"}}
open(path,"wb").write(json.dumps(value,separators=(",",":"),sort_keys=True).encode()+b"\n")
PY
put_once "$root/ATTEMPT_COMPLETE.json" ATTEMPT_COMPLETE.json
terminal=complete
"""


def build_v24_launch_specs(
    plan: V24SpotPlan, *, launch_nonce: str
) -> list[dict[str, object]]:
    """Build one idempotent Spot request per registered availability zone."""

    build_v24_spot_plan(**dataclasses.asdict(plan))
    if re.fullmatch(r"[0-9a-f]{32}", launch_nonce) is None:
        raise ValueError("V24 qualification launch nonce differs")
    user_data = base64.b64encode(_worker_script(plan).encode()).decode()
    specs = []
    for ordinal, target in enumerate(SPOT_TARGETS):
        authority = canonical_json_bytes(
            {
                "launch_nonce": launch_nonce,
                "plan": dataclasses.asdict(plan),
                "target": dataclasses.asdict(target),
            }
        )
        specs.append(
            {
                "ImageId": AMI_ID,
                "InstanceType": INSTANCE_TYPE,
                "MinCount": 1,
                "MaxCount": 1,
                "ClientToken": "v24-qualification-"
                + hashlib.sha256(authority + bytes([ordinal])).hexdigest()[:44],
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
                            "VolumeSize": 500,
                            "VolumeType": "gp3",
                            "Iops": 3000,
                            "Throughput": 250,
                        },
                    }
                ],
                "UserData": user_data,
                "TagSpecifications": [
                    {
                        "ResourceType": "instance",
                        "Tags": [
                            {"Key": "Name", "Value": plan.run_id},
                            {"Key": "borsuk-purpose", "Value": f"v24-{plan.phase}"},
                        ],
                    }
                ],
            }
        )
    return specs


def _error_code(error: BaseException) -> str | None:
    response = getattr(error, "response", None)
    if not isinstance(response, dict) or not isinstance(response.get("Error"), dict):
        return None
    code = response["Error"].get("Code")
    return code if isinstance(code, str) else None


def _retry_aws_call(call: Callable[[], Any]) -> Any:
    """Retry bounded transient control-plane failures without hiding hard errors."""

    delay = 1
    for attempt in range(8):
        try:
            return call()
        except Exception as error:
            if _error_code(error) not in _TRANSIENT_ERRORS or attempt == 7:
                raise
            time.sleep(delay)
            delay = min(delay * 2, 15)
    raise AssertionError("unreachable")


def run_v24_spot_phase(plan: V24SpotPlan, *, ec2_client: Any, s3_client: Any) -> str:
    """Run one original Spot phase and terminate its instance on every exit path."""

    bucket, prefix = _s3(plan.output_prefix, prefix=True)

    def read_terminal(name: str) -> bytes | None:
        try:
            response = _retry_aws_call(
                lambda: s3_client.get_object(
                    Bucket=bucket,
                    Key=prefix + name,
                    ExpectedBucketOwner=EXPECTED_AWS_ACCOUNT,
                    ChecksumMode="ENABLED",
                )
            )
        except Exception as error:
            if _error_code(error) in {"404", "NoSuchKey", "NotFound"}:
                return None
            raise
        raw = response["Body"].read()
        if response.get("ContentLength") != len(raw):
            raise ValueError("V24 qualification terminal length differs")
        return raw

    for name in ("ATTEMPT_FAILED.json", "ATTEMPT_COMPLETE.json"):
        if read_terminal(name) is not None:
            raise ValueError("V24 qualification terminal already exists")

    instance_id: str | None = None
    launch_nonce = secrets.token_hex(16)
    for spec in build_v24_launch_specs(plan, launch_nonce=launch_nonce):
        try:
            response = _retry_aws_call(
                lambda spec=spec: ec2_client.run_instances(**spec)
            )
        except Exception as error:
            if _error_code(error) in _CAPACITY_ERRORS:
                continue
            raise
        instances = response.get("Instances")
        if (
            type(instances) is not list  # noqa: E721
            or len(instances) != 1
            or type(instances[0].get("InstanceId")) is not str  # noqa: E721
        ):
            raise ValueError("V24 qualification launch response differs")
        instance_id = instances[0]["InstanceId"]
        break
    if instance_id is None:
        raise RuntimeError("V24 qualification Spot capacity is unavailable")

    started = time.monotonic()
    try:
        while time.monotonic() - started < controller_wall_seconds(plan.phase):
            for name in ("ATTEMPT_FAILED.json", "ATTEMPT_COMPLETE.json"):
                raw = read_terminal(name)
                if raw is None:
                    continue
                status = "failed" if name == "ATTEMPT_FAILED.json" else "complete"
                validate_v24_terminal_bytes(raw, plan, status)
                if json.loads(raw)["instance_id"] != instance_id:
                    raise ValueError("V24 qualification terminal instance differs")
                uri = f"s3://{bucket}/{prefix}{name}"
                if status == "failed":
                    raise RuntimeError(f"V24 qualification worker failed at {uri}")
                return uri
            reservations = _retry_aws_call(
                lambda: ec2_client.describe_instances(InstanceIds=[instance_id])
            ).get("Reservations")
            if type(reservations) is not list or len(reservations) != 1:  # noqa: E721
                raise ValueError("V24 qualification instance health differs")
            instances = reservations[0].get("Instances")
            if type(instances) is not list or len(instances) != 1:  # noqa: E721
                raise ValueError("V24 qualification instance health differs")
            state = instances[0].get("State", {}).get("Name")
            if state in {"shutting-down", "terminated", "stopping", "stopped"}:
                raise RuntimeError("V24 qualification instance exited without terminal")
            if state not in {"pending", "running"}:
                raise ValueError("V24 qualification instance state differs")
            time.sleep(15)
        raise TimeoutError("V24 qualification phase exceeded wall stop")
    finally:
        _retry_aws_call(
            lambda: ec2_client.terminate_instances(InstanceIds=[instance_id])
        )


def parse_args(arguments: Sequence[str] | None = None) -> V24SpotPlan:
    """Parse one explicit qualification phase launch."""

    parser = argparse.ArgumentParser(allow_abbrev=False)
    parser.add_argument("--phase", required=True, choices=PHASES)
    for flag in (
        "run-id",
        "source-commit",
        "source-archive-uri",
        "source-archive-sha256",
        "binary-uri",
        "binary-sha256",
        "manifest-uri",
        "manifest-sha256",
        "output-prefix",
    ):
        parser.add_argument(f"--{flag}", required=True)
    parser.add_argument("--source-archive-bytes", required=True, type=int)
    parser.add_argument("--binary-bytes", required=True, type=int)
    parser.add_argument("--manifest-bytes", required=True, type=int)
    parser.add_argument("--execute-v24-spot", action="store_true", required=True)
    values = vars(parser.parse_args(arguments))
    values.pop("execute_v24_spot")
    return build_v24_spot_plan(**values)


def main(arguments: Sequence[str] | None = None) -> int:
    """Run through the local causality profile; workers use their instance role."""

    import boto3

    plan = parse_args(arguments)
    session = boto3.Session(profile_name=PROFILE, region_name=REGION)
    if session.client("sts").get_caller_identity()["Account"] != EXPECTED_AWS_ACCOUNT:
        raise RuntimeError("AWS account differs")
    print(
        run_v24_spot_phase(
            plan,
            ec2_client=session.client("ec2"),
            s3_client=session.client("s3"),
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
