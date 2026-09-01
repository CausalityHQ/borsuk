#!/usr/bin/env python3
"""Exact-input staging and fail-closed Spot lifecycle for balanced-page cells."""

from __future__ import annotations

import argparse
import dataclasses
import hashlib
import json
import pathlib
import shlex
import stat
import time
from collections.abc import Mapping, Sequence
from typing import Any, Callable, Protocol

_LOWER_HEX = frozenset("0123456789abcdef")
_TERMINALS = frozenset({"complete", "quality", "stopped", "failed"})
REMOTE_OUTPUT_BASENAMES = (
    "balanced-tree.bin",
    "supercells.parquet",
    "pages-primary.parquet",
    "row-pages-primary.parquet",
    "pages-amp-1125.parquet",
    "row-pages-amp-1125.parquet",
    "pages-amp-1250.parquet",
    "row-pages-amp-1250.parquet",
    "pages-amp-1500.parquet",
    "row-pages-amp-1500.parquet",
    "development-result.json",
)


@dataclasses.dataclass(frozen=True)
class RegisteredObject:
    """One immutable object staged before the offline child starts."""

    role: str
    uri: str
    sha256: str
    encoded_bytes: int
    basename: str


@dataclasses.dataclass(frozen=True)
class BalancedRemotePlan:
    """Exact seven-object authority for one remote balanced-page worker."""

    run_id: str
    supervisor: RegisteredObject
    executable: RegisteredObject
    manifest: RegisteredObject
    ordered_inputs: tuple[RegisteredObject, ...]
    output_prefix: str


@dataclasses.dataclass(frozen=True)
class BalancedSpotAuthority:
    """Canonical account, infrastructure, monitor, and remote-worker authority."""

    aws_account: str
    profile: str
    region: str
    ami_id: str
    instance_type: str
    subnet_id: str
    security_group_ids: tuple[str, ...]
    instance_profile_arn: str
    wall_seconds: int
    poll_seconds: int
    remote_plan: BalancedRemotePlan


@dataclasses.dataclass(frozen=True)
class SpotLaunchRequest:
    """One preregistered same-region interruptible instance request."""

    region: str
    ami_id: str
    instance_type: str
    subnet_id: str
    security_group_ids: tuple[str, ...]
    instance_profile_arn: str
    user_data: str


class ObjectStorage(Protocol):
    def download(self, uri: str, destination: pathlib.Path) -> None: ...


class SpotCloud(Protocol):
    def launch_spot(self, request: SpotLaunchRequest) -> str: ...

    def wait_terminal(self, instance_id: str) -> str: ...

    def terminate(self, instance_id: str) -> None: ...


class Boto3SpotCloud:
    """Concrete one-instance Spot adapter with S3 terminal-marker authority."""

    def __init__(
        self,
        *,
        ec2_client: Any,
        s3_client: Any,
        terminal_prefix: tuple[str, str],
        wall_seconds: int,
        poll_seconds: int,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        bucket, prefix = terminal_prefix
        if (
            bucket != "borsuk-bench-453182569524-euc1"
            or not prefix
            or not prefix.endswith("/")
            or wall_seconds <= 0
            or poll_seconds < 0
        ):
            raise ValueError("terminal prefix or monitor limits differ")
        self._ec2 = ec2_client
        self._s3 = s3_client
        self._bucket = bucket
        self._prefix = prefix
        self._wall_seconds = wall_seconds
        self._poll_seconds = poll_seconds
        self._sleep = sleep

    def launch_spot(self, request: SpotLaunchRequest) -> str:
        response = self._ec2.run_instances(**ec2_run_instances_payload(request))
        instances = response.get("Instances")
        if type(instances) is not list or len(instances) != 1:  # noqa: E721
            raise ValueError("instance launch result differs")
        instance_id = instances[0].get("InstanceId")
        if type(instance_id) is not str or not instance_id.startswith("i-"):  # noqa: E721
            raise ValueError("instance identity differs")
        return instance_id

    def wait_terminal(self, instance_id: str) -> str:
        started = time.monotonic()
        expected = {
            f"{self._prefix}{status.upper()}.json": status for status in _TERMINALS
        }
        while time.monotonic() - started < self._wall_seconds:
            response = self._s3.list_objects_v2(
                Bucket=self._bucket,
                Prefix=self._prefix,
            )
            keys = {
                item.get("Key")
                for item in response.get("Contents", [])
                if type(item) is dict  # noqa: E721
            }
            terminals = sorted(keys.intersection(expected))
            if len(terminals) > 1:
                raise ValueError("terminal marker inventory differs")
            if terminals:
                key = terminals[0]
                body = self._s3.get_object(Bucket=self._bucket, Key=key)["Body"].read()
                if not body.endswith(b"\n") or body.count(b"\n") != 1:
                    raise ValueError("terminal canonical bytes differ")
                try:
                    value = json.loads(body)
                except (UnicodeDecodeError, json.JSONDecodeError) as error:
                    raise ValueError("terminal encoding differs") from error
                if (
                    type(value) is not dict  # noqa: E721
                    or set(value) != {"claim_eligible", "instance_id", "status"}
                    or value["claim_eligible"] is not False
                    or value["instance_id"] != instance_id
                    or value["status"] != expected[key]
                    or body
                    != json.dumps(value, separators=(",", ":"), sort_keys=True).encode()
                    + b"\n"
                ):
                    raise ValueError("terminal authority differs")
                return expected[key]
            self._sleep(self._poll_seconds)
        raise TimeoutError("balanced page Spot cell exceeded wall stop")

    def terminate(self, instance_id: str) -> None:
        self._ec2.terminate_instances(InstanceIds=[instance_id])


def _valid_sha256(value: str) -> bool:
    return len(value) == 64 and all(character in _LOWER_HEX for character in value)


def _split_s3(uri: str) -> tuple[str, str]:
    if not uri.startswith("s3://"):
        raise ValueError("S3 URI differs")
    bucket, separator, key = uri[5:].partition("/")
    if (
        not separator
        or bucket != "borsuk-bench-453182569524-euc1"
        or not key
        or "//" in key
    ):
        raise ValueError("S3 URI differs")
    return bucket, key


def _regular_file(path: pathlib.Path) -> bool:
    try:
        return stat.S_ISREG(path.lstat().st_mode)
    except FileNotFoundError:
        return False


def _sha256(path: pathlib.Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _validate_registered_objects(objects: Sequence[RegisteredObject]) -> None:
    if not objects:
        raise ValueError("registered object inventory is empty")
    roles: set[str] = set()
    uris: set[str] = set()
    basenames: set[str] = set()
    for registered in objects:
        if (
            type(registered.role) is not str
            or not registered.role
            or not registered.uri.startswith(
                "s3://borsuk-bench-453182569524-euc1/"
            )
            or not _valid_sha256(registered.sha256)
            or type(registered.encoded_bytes) is not int
            or registered.encoded_bytes <= 0
            or not registered.basename
            or pathlib.PurePath(registered.basename).name != registered.basename
            or registered.basename in {".", ".."}
            or registered.role in roles
            or registered.uri in uris
            or registered.basename in basenames
        ):
            raise ValueError("registered object authority differs")
        roles.add(registered.role)
        uris.add(registered.uri)
        basenames.add(registered.basename)


def validate_remote_plan(plan: BalancedRemotePlan) -> None:
    """Validate the exact direct-worker staging and publication authority."""

    artifacts = (plan.supervisor, plan.executable, plan.manifest, *plan.ordered_inputs)
    _validate_registered_objects(artifacts)
    if (
        not plan.run_id
        or any(not (character.isalnum() or character in "-_") for character in plan.run_id)
        or len(plan.run_id) > 96
        or len(plan.ordered_inputs) != 4
        or plan.supervisor.role != "offline-supervisor"
        or plan.supervisor.basename != "run_v23_balanced_page_falsifier.py"
        or plan.executable.role != "balanced-executable"
        or plan.executable.basename != "v23-balanced-page-falsifier"
        or plan.manifest.role != "balanced-manifest"
        or plan.manifest.basename != "manifest.json"
        or tuple((item.role, item.basename) for item in plan.ordered_inputs)
        != (
            ("source-shard-manifest", "source-shard-manifest.json"),
            ("f16-control", "f16-control.arrow"),
            ("query-parquet", "query.parquet"),
            ("neighbors-parquet", "neighbors.parquet"),
        )
        or not plan.output_prefix.endswith("/")
    ):
        raise ValueError("remote plan authority differs")
    _split_s3(plan.output_prefix)


def build_remote_worker_user_data(plan: BalancedRemotePlan) -> str:
    """Build the explicit credentialed parent plus credential-free child script."""

    validate_remote_plan(plan)
    bucket, output_key = _split_s3(plan.output_prefix)
    artifacts = (plan.supervisor, plan.executable, plan.manifest, *plan.ordered_inputs)
    stage_lines = []
    for artifact in artifacts:
        destination = f'$root/{artifact.basename}'
        stage_lines.extend(
            (
                f"aws s3 cp {shlex.quote(artifact.uri)} \"{destination}\" --only-show-errors",
                f"test \"$(stat -c %s \"{destination}\")\" = {artifact.encoded_bytes}",
                f"test \"$(sha256sum \"{destination}\" | cut -d' ' -f1)\" = {artifact.sha256}",
            )
        )
    uploads = "\n".join(
        f'if [ -f "$output/{basename}" ]; then aws s3 cp "$output/{basename}" '
        f"{shlex.quote(plan.output_prefix + basename)} --only-show-errors; fi"
        for basename in REMOTE_OUTPUT_BASENAMES
    )
    cleanup_names = [
        *(artifact.basename for artifact in artifacts),
        *REMOTE_OUTPUT_BASENAMES,
        "receipt.json",
        "worker.log",
        "TERMINAL.json",
    ]
    cleanup = "\n".join(
        f'for path in "$root/{name}" "$input/{name}" "$output/{name}"; do '
        'if [ -f "$path" ]; then unlink "$path"; fi; done'
        for name in cleanup_names
    )
    return f"""#!/bin/bash
set -euo pipefail
root=/mnt/borsuk-v23-balanced
input=$root/input
output=$root/output
mkdir -p "$root" "$input" "$output"
test -z "$(find "$input" -mindepth 1 -maxdepth 1 -print -quit)"
test -z "$(find "$output" -mindepth 1 -maxdepth 1 -print -quit)"
status=failed
code=1
finish() {{
  trap_code=$?
  if [ "$trap_code" -ne 0 ]; then status=failed; code=$trap_code; fi
  set +e
  instance_id=$(curl -fsS -X PUT -H 'X-aws-ec2-metadata-token-ttl-seconds: 60' http://169.254.169.254/latest/api/token | xargs -I{{}} curl -fsS -H 'X-aws-ec2-metadata-token: {{}}' http://169.254.169.254/latest/meta-data/instance-id)
  if [ -f "$root/worker.log" ]; then aws s3 cp "$root/worker.log" {shlex.quote(plan.output_prefix + 'worker.log')} --only-show-errors || true; fi
  {cleanup}
  python3 - "$status" "$instance_id" <<'PY' > "$root/TERMINAL.json"
import json,sys
print(json.dumps({{"claim_eligible":False,"instance_id":sys.argv[2],"status":sys.argv[1]}},sort_keys=True,separators=(",", ":")))
PY
  aws s3 cp "$root/TERMINAL.json" "s3://{bucket}/{output_key}${{status^^}}.json" --only-show-errors
  unlink "$root/TERMINAL.json"
  exit "$code"
}}
trap finish EXIT
{chr(10).join(stage_lines)}
chmod 0555 "$root/v23-balanced-page-falsifier"
mv "$root/source-shard-manifest.json" "$input/source-shard-manifest.json"
mv "$root/f16-control.arrow" "$input/f16-control.arrow"
mv "$root/query.parquet" "$input/query.parquet"
mv "$root/neighbors.parquet" "$input/neighbors.parquet"
set +e
python3 "$root/run_v23_balanced_page_falsifier.py" \
  --executable "$root/v23-balanced-page-falsifier" \
  --executable-sha256 {plan.executable.sha256} \
  --executable-bytes {plan.executable.encoded_bytes} \
  --manifest "$root/manifest.json" \
  --manifest-sha256 {plan.manifest.sha256} \
  --manifest-bytes {plan.manifest.encoded_bytes} \
  --input-directory "$input" \
  --output-directory "$output" \
  --execute > "$root/receipt.json" 2> "$root/worker.log"
code=$?
set -e
if [ "$code" -eq 0 ]; then
  status=$(python3 -c 'import json,sys; value=json.load(open(sys.argv[1])); print("quality" if value["stop"] == "quality" else "complete")' "$root/receipt.json")
  {uploads}
  aws s3 cp "$root/receipt.json" {shlex.quote(plan.output_prefix + 'RECEIPT.json')} --only-show-errors
elif [ "$code" -eq 70 ]; then
  status=stopped
else
  status=failed
fi
"""


def _registered_object_from_value(value: object) -> RegisteredObject:
    if type(value) is not dict or set(value) != {  # noqa: E721
        "basename",
        "encoded_bytes",
        "role",
        "sha256",
        "uri",
    }:
        raise ValueError("launch authority differs")
    try:
        return RegisteredObject(**value)
    except TypeError as error:
        raise ValueError("launch authority differs") from error


def load_spot_authority(path: pathlib.Path) -> BalancedSpotAuthority:
    """Load one strict canonical launch authority from a regular local file."""

    if not path.is_absolute() or not _regular_file(path):
        raise ValueError("launch authority differs")
    raw = path.read_bytes()
    if not raw.endswith(b"\n") or raw.count(b"\n") != 1:
        raise ValueError("launch authority differs")
    try:
        value = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("launch authority differs") from error
    if (
        type(value) is not dict  # noqa: E721
        or set(value)
        != {
            "aws_account",
            "claim_eligible",
            "monitor",
            "profile",
            "region",
            "remote_plan",
            "schema",
            "spot",
        }
        or value["schema"] != "borsuk-v23-balanced-page-spot-authority-v1"
        or value["claim_eligible"] is not False
        or value["aws_account"] != "453182569524"
        or value["profile"] != "causality"
        or value["region"] != "eu-central-1"
        or type(value["monitor"]) is not dict  # noqa: E721
        or set(value["monitor"]) != {"poll_seconds", "wall_seconds"}
        or type(value["spot"]) is not dict  # noqa: E721
        or set(value["spot"])
        != {
            "ami_id",
            "instance_profile_arn",
            "instance_type",
            "security_group_ids",
            "subnet_id",
        }
        or type(value["remote_plan"]) is not dict  # noqa: E721
        or set(value["remote_plan"])
        != {
            "executable",
            "manifest",
            "ordered_inputs",
            "output_prefix",
            "run_id",
            "supervisor",
        }
        or raw
        != json.dumps(value, separators=(",", ":"), sort_keys=True).encode() + b"\n"
    ):
        raise ValueError("launch authority differs")
    remote_value = value["remote_plan"]
    ordered_inputs = remote_value["ordered_inputs"]
    security_group_ids = value["spot"]["security_group_ids"]
    if type(ordered_inputs) is not list or type(security_group_ids) is not list:  # noqa: E721
        raise ValueError("launch authority differs")
    remote_plan = BalancedRemotePlan(
        run_id=remote_value["run_id"],
        supervisor=_registered_object_from_value(remote_value["supervisor"]),
        executable=_registered_object_from_value(remote_value["executable"]),
        manifest=_registered_object_from_value(remote_value["manifest"]),
        ordered_inputs=tuple(_registered_object_from_value(item) for item in ordered_inputs),
        output_prefix=remote_value["output_prefix"],
    )
    validate_remote_plan(remote_plan)
    authority = BalancedSpotAuthority(
        aws_account=value["aws_account"],
        profile=value["profile"],
        region=value["region"],
        ami_id=value["spot"]["ami_id"],
        instance_type=value["spot"]["instance_type"],
        subnet_id=value["spot"]["subnet_id"],
        security_group_ids=tuple(security_group_ids),
        instance_profile_arn=value["spot"]["instance_profile_arn"],
        wall_seconds=value["monitor"]["wall_seconds"],
        poll_seconds=value["monitor"]["poll_seconds"],
        remote_plan=remote_plan,
    )
    request = SpotLaunchRequest(
        region=authority.region,
        ami_id=authority.ami_id,
        instance_type=authority.instance_type,
        subnet_id=authority.subnet_id,
        security_group_ids=authority.security_group_ids,
        instance_profile_arn=authority.instance_profile_arn,
        user_data=build_remote_worker_user_data(remote_plan),
    )
    validate_spot_request(request)
    if authority.wall_seconds <= 0 or authority.poll_seconds < 0:
        raise ValueError("launch authority differs")
    return authority


def run_spot_authority(
    authority: BalancedSpotAuthority,
    *,
    sts_client: Any,
    ec2_client: Any,
    s3_client: Any,
    sleep: Callable[[float], None] = time.sleep,
) -> str:
    """Execute exactly one account-bound Spot lifecycle from frozen authority."""

    if sts_client.get_caller_identity().get("Account") != authority.aws_account:
        raise RuntimeError("AWS account differs")
    bucket, prefix = _split_s3(authority.remote_plan.output_prefix)
    cloud = Boto3SpotCloud(
        ec2_client=ec2_client,
        s3_client=s3_client,
        terminal_prefix=(bucket, prefix),
        wall_seconds=authority.wall_seconds,
        poll_seconds=authority.poll_seconds,
        sleep=sleep,
    )
    request = SpotLaunchRequest(
        region=authority.region,
        ami_id=authority.ami_id,
        instance_type=authority.instance_type,
        subnet_id=authority.subnet_id,
        security_group_ids=authority.security_group_ids,
        instance_profile_arn=authority.instance_profile_arn,
        user_data=build_remote_worker_user_data(authority.remote_plan),
    )
    return launch_spot_cell(cloud, request)


def stage_registered_inputs(
    storage: ObjectStorage,
    objects: Sequence[RegisteredObject],
    directory: pathlib.Path,
) -> tuple[pathlib.Path, ...]:
    """Download and authenticate only the exact registered object inventory."""

    _validate_registered_objects(objects)
    if not directory.is_absolute():
        raise ValueError("staging directory must be absolute")
    directory.mkdir(mode=0o700, parents=False, exist_ok=True)
    if any(directory.iterdir()):
        raise ValueError("staging directory is not empty")
    staged: list[pathlib.Path] = []
    try:
        for registered in objects:
            destination = directory / registered.basename
            partial = directory / f".{registered.basename}.partial"
            storage.download(registered.uri, partial)
            if (
                not _regular_file(partial)
                or partial.stat().st_size != registered.encoded_bytes
                or _sha256(partial) != registered.sha256
            ):
                raise ValueError(f"{registered.role} digest or length differs")
            partial.replace(destination)
            staged.append(destination)
    except BaseException:
        for path in tuple(directory.iterdir()):
            if _regular_file(path):
                path.unlink()
        raise
    return tuple(staged)


def validate_spot_request(request: SpotLaunchRequest) -> None:
    """Validate the frozen same-region Spot request without fallback."""

    if (
        request.region != "eu-central-1"
        or not request.ami_id.startswith("ami-")
        or not request.instance_type
        or not request.subnet_id.startswith("subnet-")
        or not request.security_group_ids
        or any(not group.startswith("sg-") for group in request.security_group_ids)
        or not request.instance_profile_arn.startswith("arn:aws:iam::")
        or not request.user_data.startswith("#!/bin/bash\n")
    ):
        raise ValueError("Spot request authority differs")


def ec2_run_instances_payload(request: SpotLaunchRequest) -> dict[str, object]:
    """Create the exact one-instance Spot API payload."""

    validate_spot_request(request)
    token_authority = json.dumps(
        dataclasses.asdict(request), separators=(",", ":"), sort_keys=True
    ).encode()
    return {
        "ImageId": request.ami_id,
        "InstanceType": request.instance_type,
        "MinCount": 1,
        "MaxCount": 1,
        "ClientToken": "v23-balanced-"
        + hashlib.sha256(token_authority).hexdigest()[:48],
        "SubnetId": request.subnet_id,
        "SecurityGroupIds": list(request.security_group_ids),
        "IamInstanceProfile": {"Arn": request.instance_profile_arn},
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
                    "VolumeSize": 96,
                    "VolumeType": "gp3",
                    "Iops": 3000,
                    "Throughput": 125,
                },
            }
        ],
        "UserData": request.user_data,
    }


def launch_spot_cell(cloud: SpotCloud, request: SpotLaunchRequest) -> str:
    """Launch once, observe one terminal, and always terminate that instance."""

    validate_spot_request(request)
    instance_id = cloud.launch_spot(request)
    if not instance_id.startswith("i-"):
        raise ValueError("instance identity differs")
    try:
        terminal = cloud.wait_terminal(instance_id)
        if terminal not in _TERMINALS:
            raise ValueError("terminal classification differs")
        return terminal
    finally:
        cloud.terminate(instance_id)


def payload_is_spot(payload: Mapping[str, object]) -> bool:
    """Expose a small assertion helper for launch adapters."""

    options = payload.get("InstanceMarketOptions")
    return isinstance(options, dict) and options.get("MarketType") == "spot"


def parse_args(arguments: Sequence[str] | None = None) -> pathlib.Path:
    """Parse the single-authority, explicit-Spot operational boundary."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--authority", type=pathlib.Path, required=True)
    parser.add_argument("--spot", action="store_true", required=True)
    values = parser.parse_args(arguments)
    if not values.authority.is_absolute():
        parser.error("authority path must be absolute")
    return values.authority


def main(arguments: Sequence[str] | None = None) -> int:
    """Launch exactly one registered Spot cell and print its terminal class."""

    authority = load_spot_authority(parse_args(arguments))
    import boto3

    session = boto3.Session(
        profile_name=authority.profile,
        region_name=authority.region,
    )
    terminal = run_spot_authority(
        authority,
        sts_client=session.client("sts"),
        ec2_client=session.client("ec2"),
        s3_client=session.client("s3"),
    )
    print(terminal)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
