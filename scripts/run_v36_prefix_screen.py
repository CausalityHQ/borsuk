#!/usr/bin/env python3
"""Bounded Spot launcher for the diagnostic V36 prefix population freeze."""

from __future__ import annotations

import argparse
import base64
import dataclasses
import hashlib
import json
import os
import pathlib
import re
import shlex
import subprocess
import sys
import time
import urllib.parse
from typing import Any

PROFILE = "causality"
REGION = "eu-central-1"
INSTANCE_TYPE = "r8gd.8xlarge"
ACTIVE_WALL_SECONDS = 43_200
CONTROLLER_GRACE_SECONDS = 300
EPHEMERAL_NVME_BYTES = 1_900_000_000_000
MAX_SOURCE_OBJECTS = 16
MAX_SOURCE_BYTES = 6 * 1024**3
TARGET_DISTINCT_ROWS = 1_100_000
VECTOR_DIMENSIONS = 768
CHECKPOINT_OBJECTS = 16
CHECKPOINT_SECONDS = 300
MAX_CHECKPOINT_DEPENDENCY_BYTES = 256 * 1024**2
MAX_CHECKPOINT_MANIFEST_BYTES = 8 * 1024**2
MAX_CHECKPOINT_POINTER_BYTES = 1024**2
MAX_CHECKPOINT_READY_BYTES = 1024**2
AWS_CLI_TIMEOUT_SECONDS = 120
MAX_ATTEMPTS = 3
SPOT_HOURLY_CAP_MICRO_USD = 3_000_000
CAMPAIGN_CAP_MICRO_USD = 90_000_000
RAW_POPULATION_BYTES = TARGET_DISTINCT_ROWS * VECTOR_DIMENSIONS * 4
# Complete source objects coexist with the 1.1M-row materialization spool and
# the final Parquet population plus 25% writer/query/GT workspace.
DISK_PREFLIGHT_BYTES = MAX_SOURCE_BYTES + RAW_POPULATION_BYTES * 9 // 4
AMI_ID = "ami-07bcecd13a160173f"
SECURITY_GROUP_ID = "sg-0b1fd3e4fbde4af0d"
INSTANCE_PROFILE = "borsuk-bench-profile"
SPOT_TARGETS = (
    ("eu-central-1c", "subnet-0a12dbed0ca6fac25"),
    ("eu-central-1b", "subnet-00243d923761c047c"),
    ("eu-central-1a", "subnet-034528fbd6977848f"),
)
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_GIT = re.compile(r"[0-9a-f]{40}\Z")
_RUN_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}\Z")
_INSTANCE_ID = re.compile(r"i-[A-Za-z0-9-]+\Z")
_COMPLETE_OUTPUT_ROLES = {
    "freeze-receipt",
    "population-authority",
    "source",
    "development-query",
    "development-gt100",
    "validation-query",
    "validation-gt100",
    "sealed-holdout-query",
    "sealed-holdout-gt100",
    "performance-query",
}
_CAPACITY_ERRORS = {
    "InsufficientInstanceCapacity",
    "InsufficientFreeAddressesInSubnet",
    "SpotMaxPriceTooLow",
    "Unsupported",
}

_GUEST_TERMINAL_PROGRAM = r'''import hashlib
import json
import pathlib
import sys

execution_path, receipt_path, output_path, instance_id, run_id, source_commit, status, terminal_path = sys.argv[1:]

def canonical(value):
    return json.dumps(value, allow_nan=False, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode() + b"\n"

execution_bytes = pathlib.Path(execution_path).read_bytes()
execution = json.loads(execution_bytes)
if canonical(execution) != execution_bytes:
    raise SystemExit("execution authority is not canonical")

outputs = []
if status == "complete":
    receipt_bytes = pathlib.Path(receipt_path).read_bytes()
    receipt = json.loads(receipt_bytes)
    if canonical(receipt) != receipt_bytes:
        raise SystemExit("freeze receipt is not canonical")
    expected = (
        ("population-authority", "population-authority.json"),
        ("source", "source.parquet"),
        ("development-query", "development-query.parquet"),
        ("development-gt100", "development-gt100.parquet"),
        ("validation-query", "validation-query.parquet"),
        ("validation-gt100", "validation-gt100.parquet"),
        ("sealed-holdout-query", "sealed-holdout-query.parquet"),
        ("sealed-holdout-gt100", "sealed-holdout-gt100.parquet"),
        ("performance-query", "performance-query.parquet"),
    )
    identities = receipt.get("outputs")
    if not isinstance(identities, list) or len(identities) != len(expected):
        raise SystemExit("freeze receipt outputs differ")
    for identity, (role, filename) in zip(identities, expected, strict=True):
        path = pathlib.Path(output_path, filename)
        payload = path.read_bytes()
        if (
            set(identity) != {"blake3", "encoded_bytes", "role", "sha256", "uri"}
            or identity["role"] != role
            or identity["encoded_bytes"] != len(payload)
            or identity["sha256"] != hashlib.sha256(payload).hexdigest()
            or not identity["uri"].endswith("/" + filename)
        ):
            raise SystemExit("freeze output identity differs")
        outputs.append({key: identity[key] for key in ("encoded_bytes", "role", "sha256", "uri")})
    outputs.insert(0, {
        "encoded_bytes": len(receipt_bytes),
        "role": "freeze-receipt",
        "sha256": hashlib.sha256(receipt_bytes).hexdigest(),
        "uri": execution["output_prefix"] + "freeze-receipt.json",
    })

terminal = {
    "attempt_id": execution["attempt_id"],
    "claim_eligible": False,
    "execution_authority_sha256": hashlib.sha256(execution_bytes).hexdigest(),
    "inputs": execution["inputs"],
    "instance_id": instance_id,
    "outputs": outputs,
    "run_id": run_id,
    "schema": "borsuk-v36-prefix-freeze-terminal-v1",
    "source_commit": source_commit,
    "status": status,
}
pathlib.Path(terminal_path).write_bytes(canonical(terminal))
'''


@dataclasses.dataclass(frozen=True)
class V36PrefixScreenPlan:
    """Complete immutable authority for one diagnostic population freeze."""

    run_id: str
    source_commit: str
    source_archive_uri: str
    source_archive_sha256: str
    source_archive_blake3: str
    source_archive_bytes: int
    binary_uri: str
    binary_sha256: str
    binary_blake3: str
    binary_bytes: int
    authority_uri: str
    authority_sha256: str
    authority_blake3: str
    authority_bytes: int
    source_registry_uri: str
    source_registry_sha256: str
    source_registry_blake3: str
    source_registry_bytes: int
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
        raise ValueError("V36 prefix-screen S3 URI differs")
    key = parsed.path[1:]
    if prefix != key.endswith("/"):
        raise ValueError("V36 prefix-screen S3 prefix differs")
    return parsed.netloc, key


def canonical_json_bytes(value: object) -> bytes:
    """Serialize compact sorted JSON with exactly one trailing newline."""

    return (
        json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
        + b"\n"
    )


def _read_s3_bytes(s3_client: Any, uri: str) -> tuple[bytes, str | None]:
    bucket, key = _s3(uri)
    response = s3_client.get_object(Bucket=bucket, Key=key)
    body = response["Body"].read()
    if response.get("ContentLength") != len(body):
        raise ValueError("V36 checkpoint object length differs")
    etag = response.get("ETag")
    if etag is not None:
        if type(etag) is not str or len(etag.strip('"')) == 0:
            raise ValueError("V36 checkpoint object ETag differs")
        etag = etag.strip('"')
    return body, etag


def _checkpoint_pointer_value(pointer_bytes: bytes) -> dict[str, object]:
    try:
        value = json.loads(pointer_bytes)
    except (TypeError, json.JSONDecodeError) as error:
        raise ValueError("V36 checkpoint pointer authority differs") from error
    if (
        type(value) is not dict
        or canonical_json_bytes(value) != pointer_bytes
        or set(value)
        != {
            "claim_eligible",
            "generation",
            "manifest",
            "producer_attempt_id",
            "producer_attempt_ordinal",
            "run_id",
            "schema",
        }
        or value.get("claim_eligible") is not False
        or type(value.get("generation")) is not int
        or value["generation"] < 0
        or type(value.get("manifest")) is not dict
        or set(value["manifest"])
        != {"blake3", "encoded_bytes", "role", "sha256", "uri"}
        or type(value["manifest"].get("blake3")) is not str
        or _SHA256.fullmatch(value["manifest"]["blake3"]) is None
        or type(value["manifest"].get("encoded_bytes")) is not int
        or value["manifest"]["encoded_bytes"] <= 0
        or value["manifest"].get("role") != "checkpoint-manifest"
        or type(value["manifest"].get("sha256")) is not str
        or _SHA256.fullmatch(value["manifest"]["sha256"]) is None
        or type(value.get("producer_attempt_ordinal")) is not int
        or not 0 <= value["producer_attempt_ordinal"] < MAX_ATTEMPTS
        or type(value.get("run_id")) is not str
        or _RUN_ID.fullmatch(value["run_id"]) is None
        or value.get("producer_attempt_id")
        != f"{value.get('run_id')}-attempt-{value.get('producer_attempt_ordinal'):04d}"
        or value.get("schema") != "borsuk-v36-prefix-checkpoint-pointer-v1"
    ):
        raise ValueError("V36 checkpoint pointer authority differs")
    _s3(value["manifest"]["uri"])
    return value


def _put_immutable_s3_bytes(s3_client: Any, uri: str, body: bytes) -> None:
    if type(body) is not bytes or not body:
        raise ValueError("V36 checkpoint immutable object differs")
    bucket, key = _s3(uri)
    try:
        s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=body,
            IfNoneMatch="*",
        )
    except Exception:
        observed, _ = _read_s3_bytes(s3_client, uri)
        if observed != body:
            raise ValueError("V36 checkpoint immutable object differs") from None


def publish_v36_checkpoint(
    s3_client: Any,
    *,
    immutable_objects: tuple[tuple[str, bytes], ...],
    manifest_uri: str,
    manifest_bytes: bytes,
    pointer_uri: str,
    pointer_bytes: bytes,
    previous_pointer_etag: str | None,
) -> str:
    """Publish dependencies then manifest, and CAS one run-scoped pointer."""

    if type(immutable_objects) is not tuple:
        raise ValueError("V36 checkpoint immutable objects differ")
    uris = [uri for uri, _ in immutable_objects]
    if len(set(uris)) != len(uris) or manifest_uri in uris or pointer_uri in {
        *uris,
        manifest_uri,
    }:
        raise ValueError("V36 checkpoint object roles overlap")
    pointer = _checkpoint_pointer_value(pointer_bytes)
    manifest_identity = pointer["manifest"]
    if (
        manifest_identity["uri"] != manifest_uri
        or manifest_identity["encoded_bytes"] != len(manifest_bytes)
        or manifest_identity["sha256"] != hashlib.sha256(manifest_bytes).hexdigest()
    ):
        raise ValueError("V36 checkpoint manifest authority differs")
    try:
        manifest_value = json.loads(manifest_bytes)
    except (TypeError, json.JSONDecodeError) as error:
        raise ValueError("V36 checkpoint manifest authority differs") from error
    if (
        type(manifest_value) is not dict
        or canonical_json_bytes(manifest_value) != manifest_bytes
        or manifest_value.get("schema")
        != "borsuk-v36-prefix-freeze-checkpoint-v1"
        or manifest_value.get("generation") != pointer["generation"]
    ):
        raise ValueError("V36 checkpoint manifest authority differs")
    if previous_pointer_etag is not None and (
        type(previous_pointer_etag) is not str or not previous_pointer_etag
    ):
        raise ValueError("V36 checkpoint pointer ETag differs")

    for uri, body in immutable_objects:
        _put_immutable_s3_bytes(s3_client, uri, body)
    _put_immutable_s3_bytes(s3_client, manifest_uri, manifest_bytes)

    bucket, key = _s3(pointer_uri)
    condition = (
        {"IfNoneMatch": "*"}
        if previous_pointer_etag is None
        else {"IfMatch": previous_pointer_etag}
    )
    try:
        response = s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=pointer_bytes,
            **condition,
        )
        etag = response.get("ETag")
        if type(etag) is not str or not etag.strip('"'):
            raise ValueError("V36 checkpoint pointer ETag differs")
        return etag.strip('"')
    except Exception:
        observed, etag = _read_s3_bytes(s3_client, pointer_uri)
        if observed != pointer_bytes or etag is None:
            raise ValueError("V36 checkpoint pointer observation differs") from None
        return etag


def read_v36_checkpoint_head(
    s3_client: Any, pointer_uri: str
) -> tuple[bytes, bytes, str | None]:
    """Read exactly the newest pointer and its manifest, failing closed."""

    pointer_bytes, etag = _read_s3_bytes(s3_client, pointer_uri)
    pointer = _checkpoint_pointer_value(pointer_bytes)
    manifest_identity = pointer["manifest"]
    manifest_bytes, _ = _read_s3_bytes(s3_client, str(manifest_identity["uri"]))
    try:
        manifest = json.loads(manifest_bytes)
    except (TypeError, json.JSONDecodeError) as error:
        raise ValueError("V36 checkpoint manifest authority differs") from error
    if (
        len(manifest_bytes) != manifest_identity["encoded_bytes"]
        or hashlib.sha256(manifest_bytes).hexdigest() != manifest_identity["sha256"]
        or type(manifest) is not dict
        or canonical_json_bytes(manifest) != manifest_bytes
        or manifest.get("schema") != "borsuk-v36-prefix-freeze-checkpoint-v1"
        or manifest.get("generation") != pointer["generation"]
    ):
        raise ValueError("V36 checkpoint manifest authority differs")
    return pointer_bytes, manifest_bytes, etag


def _outbox_artifact_identity(value: object) -> dict[str, object]:
    if (
        type(value) is not dict
        or set(value) != {"blake3", "encoded_bytes", "role", "sha256", "uri"}
        or type(value.get("blake3")) is not str
        or _SHA256.fullmatch(value["blake3"]) is None
        or type(value.get("encoded_bytes")) is not int
        or value["encoded_bytes"] <= 0
        or type(value.get("role")) is not str
        or not value["role"]
        or type(value.get("sha256")) is not str
        or _SHA256.fullmatch(value["sha256"]) is None
        or type(value.get("uri")) is not str
    ):
        raise ValueError("V36 checkpoint outbox identity differs")
    _s3(value["uri"])
    return value


def _authenticate_outbox_file(
    path: pathlib.Path, identity: dict[str, object]
) -> pathlib.Path:
    try:
        stat = path.lstat()
    except OSError as error:
        raise ValueError("V36 checkpoint outbox artifact authority differs") from error
    if not path.is_file() or path.is_symlink() or stat.st_size != identity["encoded_bytes"]:
        raise ValueError("V36 checkpoint outbox artifact authority differs")
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    if digest.hexdigest() != identity["sha256"]:
        raise ValueError("V36 checkpoint outbox artifact authority differs")
    return path


def publish_v36_checkpoint_outbox_generation(
    root: pathlib.Path, generation: int, transport: Any
) -> None:
    """Stream one Rust-committed generation through a conditional transport."""

    if (
        not isinstance(root, pathlib.Path)
        or root.is_symlink()
        or not root.is_dir()
        or type(generation) is not int
        or generation < 0
    ):
        raise ValueError("V36 checkpoint outbox root differs")
    if any(
        not (root / child).is_dir() or (root / child).is_symlink()
        for child in ("objects", "manifests", "pointers", "commits")
    ):
        raise ValueError("V36 checkpoint outbox root differs")
    ready_path = root / "commits" / f"generation-{generation:08d}.json"
    try:
        if ready_path.stat().st_size > MAX_CHECKPOINT_READY_BYTES:
            raise ValueError("V36 checkpoint outbox commit differs")
        ready_bytes = ready_path.read_bytes()
        ready = json.loads(ready_bytes)
    except (OSError, json.JSONDecodeError) as error:
        raise ValueError("V36 checkpoint outbox commit differs") from error
    if (
        ready_path.is_symlink()
        or type(ready) is not dict
        or canonical_json_bytes(ready) != ready_bytes
        or set(ready)
        != {
            "dependencies",
            "generation",
            "manifest",
            "pointer_encoded_bytes",
            "pointer_sha256",
            "pointer_uri",
            "previous_pointer_sha256",
            "schema",
        }
        or ready.get("schema") != "borsuk-v36-prefix-checkpoint-outbox-v1"
        or ready.get("generation") != generation
        or type(ready.get("dependencies")) is not list
        or not ready["dependencies"]
        or type(ready.get("pointer_encoded_bytes")) is not int
        or ready["pointer_encoded_bytes"] <= 0
        or type(ready.get("pointer_sha256")) is not str
        or _SHA256.fullmatch(ready["pointer_sha256"]) is None
        or type(ready.get("pointer_uri")) is not str
        or (
            generation == 0
            and ready.get("previous_pointer_sha256") is not None
        )
        or (
            generation > 0
            and (
                type(ready.get("previous_pointer_sha256")) is not str
                or _SHA256.fullmatch(ready["previous_pointer_sha256"]) is None
            )
        )
    ):
        raise ValueError("V36 checkpoint outbox commit differs")
    _s3(ready["pointer_uri"])
    dependencies = [_outbox_artifact_identity(value) for value in ready["dependencies"]]
    manifest = _outbox_artifact_identity(ready.get("manifest"))
    if (
        manifest["role"] != "checkpoint-manifest"
        or manifest["encoded_bytes"] > MAX_CHECKPOINT_MANIFEST_BYTES
        or ready["pointer_encoded_bytes"] > MAX_CHECKPOINT_POINTER_BYTES
        or any(
            identity["encoded_bytes"] > MAX_CHECKPOINT_DEPENDENCY_BYTES
            for identity in dependencies
        )
    ):
        raise ValueError("V36 checkpoint outbox manifest differs")
    identities = [*dependencies, manifest]
    if len({identity["uri"] for identity in identities}) != len(identities):
        raise ValueError("V36 checkpoint outbox identity differs")
    dependency_paths = [
        _authenticate_outbox_file(
            root / "objects" / f"{identity['sha256']}.blob", identity
        )
        for identity in dependencies
    ]
    manifest_path = _authenticate_outbox_file(
        root / "manifests" / f"{manifest['sha256']}.json", manifest
    )
    manifest_bytes = manifest_path.read_bytes()
    try:
        manifest_value = json.loads(manifest_bytes)
    except json.JSONDecodeError as error:
        raise ValueError("V36 checkpoint outbox manifest differs") from error
    if (
        canonical_json_bytes(manifest_value) != manifest_bytes
        or manifest_value.get("schema") != "borsuk-v36-prefix-freeze-checkpoint-v1"
        or manifest_value.get("generation") != generation
    ):
        raise ValueError("V36 checkpoint outbox manifest differs")
    pointer_identity = {
        "encoded_bytes": ready["pointer_encoded_bytes"],
        "sha256": ready["pointer_sha256"],
    }
    pointer_path = _authenticate_outbox_file(
        root / "pointers" / f"{ready['pointer_sha256']}.json", pointer_identity
    )
    pointer_bytes = pointer_path.read_bytes()
    pointer = _checkpoint_pointer_value(pointer_bytes)
    manifest_bucket, manifest_key = _s3(str(manifest["uri"]))
    pointer_bucket, pointer_key = _s3(ready["pointer_uri"])
    checkpoint_marker = "checkpoints/objects/"
    campaign_prefix, marker, _ = manifest_key.partition(checkpoint_marker)
    objects_prefix = f"{campaign_prefix}{checkpoint_marker}"
    expected_pointer_key = (
        f"{campaign_prefix}checkpoints/runs/{pointer['run_id']}/latest.json"
    )
    if (
        pointer["generation"] != generation
        or pointer["manifest"] != manifest
        or marker != checkpoint_marker
        or pointer_bucket != manifest_bucket
        or pointer_key != expected_pointer_key
    ):
        raise ValueError("V36 checkpoint outbox pointer differs")
    if any(
        _s3(str(identity["uri"]))[0] != manifest_bucket
        or not _s3(str(identity["uri"]))[1].startswith(objects_prefix)
        or _s3(str(identity["uri"]))[1] == objects_prefix
        for identity in identities
    ):
        raise ValueError("V36 checkpoint outbox identity differs")
    for identity, path in zip(dependencies, dependency_paths, strict=True):
        transport.put_immutable(identity, path)
    transport.put_immutable(manifest, manifest_path)
    transport.put_pointer(
        ready["pointer_uri"], pointer_path, ready["previous_pointer_sha256"]
    )


class _AwsCliRunner:
    @staticmethod
    def _environment() -> dict[str, str]:
        environment = os.environ.copy()
        environment["AWS_RETRY_MODE"] = "standard"
        environment["AWS_MAX_ATTEMPTS"] = "5"
        return environment

    def json(self, arguments: list[str]) -> dict[str, object]:
        completed = subprocess.run(
            arguments,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=AWS_CLI_TIMEOUT_SECONDS,
            env=self._environment(),
        )
        value = json.loads(completed.stdout)
        if type(value) is not dict:
            raise ValueError("V36 checkpoint AWS response differs")
        return value

    def stream(self, arguments: list[str]) -> Any:
        process = subprocess.Popen(
            [
                "timeout",
                "--signal=TERM",
                "--kill-after=5",
                str(AWS_CLI_TIMEOUT_SECONDS),
                *arguments,
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            env=self._environment(),
        )
        if process.stdout is None:
            process.kill()
            raise ValueError("V36 checkpoint AWS stream differs")
        while chunk := process.stdout.read(1024 * 1024):
            yield chunk
        if process.wait() != 0:
            raise RuntimeError("V36 checkpoint AWS stream failed")


class V36AwsCliCheckpointTransport:
    """Streaming AWS CLI transport with immutable writes and pointer CAS."""

    def __init__(self, *, runner: Any | None = None) -> None:
        self._runner = _AwsCliRunner() if runner is None else runner
        self._published: dict[str, tuple[int, str]] = {}

    @staticmethod
    def _put_arguments(
        uri: str, path: pathlib.Path, condition: tuple[str, str]
    ) -> list[str]:
        bucket, key = _s3(uri)
        return [
            "aws",
            "s3api",
            "put-object",
            "--region",
            REGION,
            "--bucket",
            bucket,
            "--key",
            key,
            "--body",
            str(path.resolve()),
            condition[0],
            condition[1],
            "--output",
            "json",
        ]

    def _remote_identity(self, uri: str) -> tuple[int, str]:
        digest = hashlib.sha256()
        length = 0
        for chunk in self._runner.stream(
            ["aws", "s3", "cp", uri, "-", "--only-show-errors", "--region", REGION]
        ):
            if type(chunk) is not bytes or not chunk:
                raise ValueError("V36 checkpoint AWS stream differs")
            length += len(chunk)
            digest.update(chunk)
        return length, digest.hexdigest()

    def put_immutable(
        self, identity: dict[str, object], path: pathlib.Path
    ) -> None:
        _authenticate_outbox_file(path, identity)
        expected = (identity["encoded_bytes"], identity["sha256"])
        prior = self._published.get(str(identity["uri"]))
        if prior is not None:
            if prior != expected:
                raise ValueError("V36 checkpoint immutable object differs")
            return
        try:
            self._runner.json(
                self._put_arguments(identity["uri"], path, ("--if-none-match", "*"))
            )
        except Exception:
            if self._remote_identity(str(identity["uri"])) != expected:
                raise ValueError("V36 checkpoint immutable object differs") from None
        self._published[str(identity["uri"])] = expected

    def put_pointer(
        self,
        uri: str,
        path: pathlib.Path,
        previous_sha256: str | None,
    ) -> None:
        stat = path.lstat()
        if not path.is_file() or path.is_symlink():
            raise ValueError("V36 checkpoint pointer file differs")
        intended = (stat.st_size, hashlib.sha256(path.read_bytes()).hexdigest())
        if previous_sha256 is None:
            condition = ("--if-none-match", "*")
        else:
            if _SHA256.fullmatch(previous_sha256) is None:
                raise ValueError("V36 checkpoint predecessor differs")
            bucket, key = _s3(uri)
            head = self._runner.json(
                [
                    "aws",
                    "s3api",
                    "head-object",
                    "--region",
                    REGION,
                    "--bucket",
                    bucket,
                    "--key",
                    key,
                    "--output",
                    "json",
                ]
            )
            etag = head.get("ETag")
            observed_previous = self._remote_identity(uri)
            if (
                type(etag) is not str
                or not etag.strip('"')
                or type(head.get("ContentLength")) is not int
                or head["ContentLength"] != observed_previous[0]
                or observed_previous[1] != previous_sha256
            ):
                raise ValueError("V36 checkpoint predecessor differs")
            condition = ("--if-match", etag.strip('"'))
        try:
            self._runner.json(self._put_arguments(uri, path, condition))
        except Exception:
            if self._remote_identity(uri) != intended:
                raise ValueError("V36 checkpoint pointer observation differs") from None


def _pid_alive(pid: int) -> bool:
    if type(pid) is not int or pid <= 1:
        raise ValueError("V36 checkpoint producer PID differs")
    try:
        stat = pathlib.Path(f"/proc/{pid}/stat").read_text()
    except FileNotFoundError:
        return False
    except OSError:
        stat = ""
    if ") " in stat and stat.rsplit(") ", 1)[1].startswith("Z"):
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True


def watch_v36_checkpoint_outbox(
    root: pathlib.Path,
    producer_pid: int,
    transport: Any,
    *,
    first_generation: int = 0,
    producer_alive: Any = _pid_alive,
    pause: Any = time.sleep,
) -> int:
    """Publish every ready generation while its sole Rust producer lives."""

    if type(first_generation) is not int or first_generation < 0:
        raise ValueError("V36 checkpoint first generation differs")
    generation = first_generation
    while True:
        ready = root / "commits" / f"generation-{generation:08d}.json"
        if ready.is_file() and not ready.is_symlink():
            publish_v36_checkpoint_outbox_generation(root, generation, transport)
            generation += 1
            continue
        if not producer_alive(producer_pid):
            # The producer may atomically rename its final descriptor after
            # the first observation but before the liveness check. Once it is
            # dead the outbox is stable, so re-observe and drain that boundary.
            if ready.is_file() and not ready.is_symlink():
                publish_v36_checkpoint_outbox_generation(root, generation, transport)
                generation += 1
                continue
            return generation
        pause(1)


def build_v36_prefix_screen_plan(**values: Any) -> V36PrefixScreenPlan:
    """Validate one immutable prefix-screen launch plan."""

    plan = V36PrefixScreenPlan(**values)
    digest_values = (
        plan.source_archive_sha256,
        plan.source_archive_blake3,
        plan.binary_sha256,
        plan.binary_blake3,
        plan.authority_sha256,
        plan.authority_blake3,
        plan.source_registry_sha256,
        plan.source_registry_blake3,
    )
    byte_values = (
        plan.source_archive_bytes,
        plan.binary_bytes,
        plan.authority_bytes,
        plan.source_registry_bytes,
    )
    if (
        _RUN_ID.fullmatch(plan.run_id) is None
        or _GIT.fullmatch(plan.source_commit) is None
        or any(type(value) is not str or _SHA256.fullmatch(value) is None for value in digest_values)
        or any(type(value) is not int or value <= 0 for value in byte_values)
    ):
        raise ValueError("V36 prefix-screen plan differs")
    for uri in (
        plan.source_archive_uri,
        plan.binary_uri,
        plan.authority_uri,
        plan.source_registry_uri,
    ):
        _s3(uri)
    _s3(plan.output_prefix, prefix=True)
    return plan


def dry_run_v36_prefix_screen(plan: V36PrefixScreenPlan) -> bytes:
    """Return the complete launch envelope without touching local or AWS state."""

    build_v36_prefix_screen_plan(**dataclasses.asdict(plan))
    return canonical_json_bytes(
        {
            "active_wall_seconds": ACTIVE_WALL_SECONDS,
            "campaign_cap_micro_usd": CAMPAIGN_CAP_MICRO_USD,
            "checkpoint_objects": CHECKPOINT_OBJECTS,
            "checkpoint_seconds": CHECKPOINT_SECONDS,
            "claim_eligible": False,
            "disk_preflight_bytes": DISK_PREFLIGHT_BYTES,
            "dry_run": True,
            "instance_type": INSTANCE_TYPE,
            "max_attempts": MAX_ATTEMPTS,
            "max_source_bytes": MAX_SOURCE_BYTES,
            "max_source_objects": MAX_SOURCE_OBJECTS,
            "profile": PROFILE,
            "region": REGION,
            "run_id": plan.run_id,
            "schema": "borsuk-v36-prefix-screen-dry-run-v1",
            "spot_hourly_cap_micro_usd": SPOT_HOURLY_CAP_MICRO_USD,
            "target_distinct_rows": TARGET_DISTINCT_ROWS,
            "vector_dimensions": VECTOR_DIMENSIONS,
            "zone_candidates": [zone for zone, _ in SPOT_TARGETS],
        }
    )


def _attempt_wall_seconds(attempt_ordinal: int) -> int:
    if type(attempt_ordinal) is not int or not 0 <= attempt_ordinal < MAX_ATTEMPTS:
        raise ValueError("V36 prefix-screen attempt ordinal differs")
    spent_before = attempt_ordinal * (
        ACTIVE_WALL_SECONDS * SPOT_HOURLY_CAP_MICRO_USD // 3_600
    )
    remaining = CAMPAIGN_CAP_MICRO_USD - spent_before
    return min(
        ACTIVE_WALL_SECONDS,
        remaining * 3_600 // SPOT_HOURLY_CAP_MICRO_USD,
    )


def _execution_authority(
    plan: V36PrefixScreenPlan, attempt_ordinal: int
) -> dict[str, object]:
    output_bucket, output_key = _s3(plan.output_prefix, prefix=True)
    attempt_prefix = f"{output_key}attempt-{attempt_ordinal:04d}/"
    return {
        "active_wall_seconds": _attempt_wall_seconds(attempt_ordinal),
        "attempt_id": f"{plan.run_id}-attempt-{attempt_ordinal:04d}",
        "checkpoint_seconds": CHECKPOINT_SECONDS,
        "claim_eligible": False,
        "inputs": [
            {"blake3": plan.binary_blake3, "encoded_bytes": plan.binary_bytes, "role": "binary", "sha256": plan.binary_sha256, "uri": plan.binary_uri},
            {"blake3": plan.authority_blake3, "encoded_bytes": plan.authority_bytes, "role": "freeze-authority", "sha256": plan.authority_sha256, "uri": plan.authority_uri},
            {"blake3": plan.source_archive_blake3, "encoded_bytes": plan.source_archive_bytes, "role": "source-archive", "sha256": plan.source_archive_sha256, "uri": plan.source_archive_uri},
            {"blake3": plan.source_registry_blake3, "encoded_bytes": plan.source_registry_bytes, "role": "source-registry", "sha256": plan.source_registry_sha256, "uri": plan.source_registry_uri},
        ],
        "output_prefix": f"s3://{output_bucket}/{attempt_prefix}",
        "schema": "borsuk-v36-prefix-freeze-execution-authority-v1",
        "source_commit": plan.source_commit,
    }
def _user_data(plan: V36PrefixScreenPlan, *, attempt_ordinal: int) -> str:
    quoted = {
        field.name: shlex.quote(str(getattr(plan, field.name)))
        for field in dataclasses.fields(plan)
    }
    output_bucket, output_key = _s3(plan.output_prefix, prefix=True)
    attempt_prefix = f"{output_key}attempt-{attempt_ordinal:04d}/"
    wall_seconds = _attempt_wall_seconds(attempt_ordinal)
    execution_authority = canonical_json_bytes(
        _execution_authority(plan, attempt_ordinal)
    )
    execution_authority_b64 = base64.b64encode(execution_authority).decode()
    terminal_program_b64 = base64.b64encode(_GUEST_TERMINAL_PROGRAM.encode()).decode()
    sidecar_sha256 = hashlib.sha256(pathlib.Path(__file__).read_bytes()).hexdigest()
    return f"""#!/bin/bash
set -euo pipefail
trap 'shutdown -h now' EXIT
root=$(mktemp -d /mnt/v36-prefix.XXXXXX)
available=$(df --output=avail -B1 /mnt | tail -1)
test "$available" -ge {DISK_PREFLIGHT_BYTES}
aws s3 cp {quoted['source_archive_uri']} "$root/source.tar.zst" --only-show-errors
aws s3 cp {quoted['binary_uri']} "$root/v36_prefix_freeze" --only-show-errors
aws s3 cp {quoted['authority_uri']} "$root/authority.json" --only-show-errors
aws s3 cp {quoted['source_registry_uri']} "$root/source-registry.json" --only-show-errors
printf '%s' {shlex.quote(execution_authority_b64)} | base64 -d > "$root/execution-authority.json"
printf '%s' {shlex.quote(terminal_program_b64)} | base64 -d > "$root/write-terminal.py"
test "$(stat -c %s "$root/source.tar.zst")" = {plan.source_archive_bytes}
test "$(sha256sum "$root/source.tar.zst" | cut -d' ' -f1)" = {plan.source_archive_sha256}
test "$(stat -c %s "$root/v36_prefix_freeze")" = {plan.binary_bytes}
test "$(sha256sum "$root/v36_prefix_freeze" | cut -d' ' -f1)" = {plan.binary_sha256}
test "$(stat -c %s "$root/authority.json")" = {plan.authority_bytes}
test "$(sha256sum "$root/authority.json" | cut -d' ' -f1)" = {plan.authority_sha256}
test "$(stat -c %s "$root/source-registry.json")" = {plan.source_registry_bytes}
test "$(sha256sum "$root/source-registry.json" | cut -d' ' -f1)" = {plan.source_registry_sha256}
chmod 500 "$root/v36_prefix_freeze"
mkdir "$root/output" "$root/scratch" "$root/checkpoint-outbox" "$root/sidecar-source"
chmod 700 "$root/checkpoint-outbox"
tar --zstd -xf "$root/source.tar.zst" -C "$root/sidecar-source" scripts/run_v36_prefix_screen.py
test "$(sha256sum "$root/sidecar-source/scripts/run_v36_prefix_screen.py" | cut -d' ' -f1)" = {sidecar_sha256}
python3 -m py_compile "$root/sidecar-source/scripts/run_v36_prefix_screen.py"
aws s3api put-object --generate-cli-skeleton input | grep -q '"IfMatch"'
token=$(curl -fsS -X PUT -H 'X-aws-ec2-metadata-token-ttl-seconds: 21600' http://169.254.169.254/latest/api/token)
instance_id=$(curl -fsS -H "X-aws-ec2-metadata-token: $token" http://169.254.169.254/latest/meta-data/instance-id)
set +e
timeout --signal=TERM --kill-after=30 {wall_seconds} "$root/v36_prefix_freeze" \
  --execute-prefix-freeze \
  --execution-authority "$root/execution-authority.json" \
  --authority "$root/authority.json" --source-registry "$root/source-registry.json" \
  --source-archive "$root/source.tar.zst" --output "$root/output" \
  --scratch "$root/scratch" --checkpoint-outbox "$root/checkpoint-outbox" \
  --producer-instance-id "$instance_id" &
science_pid=$!
python3 "$root/sidecar-source/scripts/run_v36_prefix_screen.py" \
  --publish-checkpoints --checkpoint-outbox "$root/checkpoint-outbox" \
  --producer-pid "$science_pid" --first-generation 0 &
sidecar_pid=$!
status=
while kill -0 "$science_pid" 2>/dev/null; do
  if ! kill -0 "$sidecar_pid" 2>/dev/null; then
    wait "$sidecar_pid"
    sidecar_status=$?
    kill -TERM "$science_pid" 2>/dev/null
    wait "$science_pid"
    status=70
    break
  fi
  sleep 2
done
if [[ -z "$status" ]]; then
  wait "$science_pid"
  status=$?
  wait "$sidecar_pid"
  sidecar_status=$?
  if [[ "$sidecar_status" != 0 ]]; then
    status=70
  fi
fi
set -e
if [[ "$status" = 0 ]]; then
  terminal_status=complete
  terminal_marker=ATTEMPT_COMPLETE.json
elif [[ "$status" = 42 ]]; then
  terminal_status=screen-source-insufficient
  terminal_marker=ATTEMPT_FAILED.json
elif [[ "$status" = 124 || "$status" = 137 || "$status" = 143 ]]; then
  terminal_status=interrupted
  terminal_marker=INTERRUPTED.json
else
  terminal_status=infrastructure
  terminal_marker=ATTEMPT_FAILED.json
fi
python3 "$root/write-terminal.py" \
  "$root/execution-authority.json" "$root/output/freeze-receipt.json" \
  "$root/output" "$instance_id" {quoted['run_id']} {quoted['source_commit']} \
  "$terminal_status" "$root/output/$terminal_marker"
if [[ "$status" = 0 ]]; then
  aws s3api put-object --bucket {shlex.quote(output_bucket)} --key {shlex.quote(attempt_prefix)}population-authority.json --body "$root/output/population-authority.json" --if-none-match '*'
  aws s3api put-object --bucket {shlex.quote(output_bucket)} --key {shlex.quote(attempt_prefix)}source.parquet --body "$root/output/source.parquet" --if-none-match '*'
  aws s3api put-object --bucket {shlex.quote(output_bucket)} --key {shlex.quote(attempt_prefix)}development-query.parquet --body "$root/output/development-query.parquet" --if-none-match '*'
  aws s3api put-object --bucket {shlex.quote(output_bucket)} --key {shlex.quote(attempt_prefix)}development-gt100.parquet --body "$root/output/development-gt100.parquet" --if-none-match '*'
  aws s3api put-object --bucket {shlex.quote(output_bucket)} --key {shlex.quote(attempt_prefix)}validation-query.parquet --body "$root/output/validation-query.parquet" --if-none-match '*'
  aws s3api put-object --bucket {shlex.quote(output_bucket)} --key {shlex.quote(attempt_prefix)}validation-gt100.parquet --body "$root/output/validation-gt100.parquet" --if-none-match '*'
  aws s3api put-object --bucket {shlex.quote(output_bucket)} --key {shlex.quote(attempt_prefix)}sealed-holdout-query.parquet --body "$root/output/sealed-holdout-query.parquet" --if-none-match '*'
  aws s3api put-object --bucket {shlex.quote(output_bucket)} --key {shlex.quote(attempt_prefix)}sealed-holdout-gt100.parquet --body "$root/output/sealed-holdout-gt100.parquet" --if-none-match '*'
  aws s3api put-object --bucket {shlex.quote(output_bucket)} --key {shlex.quote(attempt_prefix)}performance-query.parquet --body "$root/output/performance-query.parquet" --if-none-match '*'
  aws s3api put-object --bucket {shlex.quote(output_bucket)} --key {shlex.quote(attempt_prefix)}freeze-receipt.json --body "$root/output/freeze-receipt.json" --if-none-match '*'
fi
if [[ "$status" = 0 ]]; then
  aws s3api put-object --bucket {shlex.quote(output_bucket)} --key {shlex.quote(attempt_prefix)}ATTEMPT_COMPLETE.json --body "$root/output/ATTEMPT_COMPLETE.json" --if-none-match '*'
elif [[ "$status" = 124 || "$status" = 137 || "$status" = 143 ]]; then
  aws s3api put-object --bucket {shlex.quote(output_bucket)} --key {shlex.quote(attempt_prefix)}INTERRUPTED.json --body "$root/output/INTERRUPTED.json" --if-none-match '*'
else
  aws s3api put-object --bucket {shlex.quote(output_bucket)} --key {shlex.quote(attempt_prefix)}ATTEMPT_FAILED.json --body "$root/output/ATTEMPT_FAILED.json" --if-none-match '*'
fi
exit "$status"
"""


def build_v36_prefix_launch_specs(
    plan: V36PrefixScreenPlan, *, launch_nonce: str, attempt_ordinal: int
) -> list[dict[str, object]]:
    """Build the three registered Spot-zone candidates for one attempt."""

    if re.fullmatch(r"[0-9a-f]{32}", launch_nonce) is None:
        raise ValueError("V36 prefix-screen launch nonce differs")
    data = base64.b64encode(_user_data(plan, attempt_ordinal=attempt_ordinal).encode()).decode()
    specs: list[dict[str, object]] = []
    for zone_ordinal, (zone, subnet) in enumerate(SPOT_TARGETS):
        token = hashlib.sha256(
            f"{plan.run_id}:{launch_nonce}:{attempt_ordinal}:{zone_ordinal}".encode()
        ).hexdigest()
        specs.append(
            {
                "ImageId": AMI_ID,
                "InstanceType": INSTANCE_TYPE,
                "MinCount": 1,
                "MaxCount": 1,
                "ClientToken": token,
                "Placement": {"AvailabilityZone": zone},
                "SubnetId": subnet,
                "SecurityGroupIds": [SECURITY_GROUP_ID],
                "IamInstanceProfile": {"Name": INSTANCE_PROFILE},
                "InstanceMarketOptions": {
                    "MarketType": "spot",
                    "SpotOptions": {
                        "InstanceInterruptionBehavior": "terminate",
                        "MaxPrice": "3.000000",
                        "SpotInstanceType": "one-time",
                    },
                },
                "InstanceInitiatedShutdownBehavior": "terminate",
                "UserData": data,
            }
        )
    return specs


def _marker_key(plan: V36PrefixScreenPlan, attempt_ordinal: int, marker: str) -> tuple[str, str]:
    bucket, prefix = _s3(plan.output_prefix, prefix=True)
    if marker not in {"ATTEMPT_COMPLETE.json", "ATTEMPT_FAILED.json", "INTERRUPTED.json"}:
        raise ValueError("V36 prefix-screen marker differs")
    return bucket, f"{prefix}attempt-{attempt_ordinal:04d}/{marker}"


def _read_attempt_status(
    s3_client: Any,
    plan: V36PrefixScreenPlan,
    attempt_ordinal: int | None = None,
    *,
    expected_instance_id: str | None = None,
) -> str | None:
    """Read one authenticated terminal status, or prove it absent."""

    ordinals = range(MAX_ATTEMPTS) if attempt_ordinal is None else (attempt_ordinal,)
    for ordinal in ordinals:
        for marker, statuses in (
            ("ATTEMPT_COMPLETE.json", {"complete"}),
            ("ATTEMPT_FAILED.json", {"infrastructure", "screen-source-insufficient"}),
            ("INTERRUPTED.json", {"interrupted"}),
        ):
            bucket, key = _marker_key(plan, ordinal, marker)
            try:
                response = s3_client.get_object(Bucket=bucket, Key=key)
            except Exception as error:
                code = getattr(error, "response", {}).get("Error", {}).get("Code")
                if code in {"NoSuchKey", "404"}:
                    continue
                raise
            body = response["Body"].read()
            value = json.loads(body)
            execution_authority = _execution_authority(plan, ordinal)
            expected_attempt_id = execution_authority["attempt_id"]
            expected_inputs = execution_authority["inputs"]
            expected_execution_sha256 = hashlib.sha256(
                canonical_json_bytes(execution_authority)
            ).hexdigest()
            expected_output_bucket, expected_output_prefix = _s3(
                str(execution_authority["output_prefix"]), prefix=True
            )
            status = value.get("status")
            outputs = value.get("outputs")
            output_roles: set[str] = set()
            output_uris: set[str] = set()
            outputs_valid = type(outputs) is list
            if outputs_valid:
                for output in outputs:
                    if type(output) is not dict or set(output) != {
                        "encoded_bytes",
                        "role",
                        "sha256",
                        "uri",
                    }:
                        outputs_valid = False
                        break
                    role = output["role"]
                    uri = output["uri"]
                    try:
                        bucket, key = _s3(uri)
                    except (TypeError, ValueError):
                        outputs_valid = False
                        break
                    if (
                        type(role) is not str
                        or role not in _COMPLETE_OUTPUT_ROLES
                        or role in output_roles
                        or uri in output_uris
                        or type(output["encoded_bytes"]) is not int
                        or output["encoded_bytes"] <= 0
                        or type(output["sha256"]) is not str
                        or _SHA256.fullmatch(output["sha256"]) is None
                        or bucket != expected_output_bucket
                        or not key.startswith(expected_output_prefix)
                    ):
                        outputs_valid = False
                        break
                    output_roles.add(role)
                    output_uris.add(uri)
            if (
                response.get("ContentLength") != len(body)
                or type(value) is not dict
                or canonical_json_bytes(value) != body
                or set(value) != {
                    "attempt_id",
                    "claim_eligible",
                    "execution_authority_sha256",
                    "inputs",
                    "instance_id",
                    "outputs",
                    "run_id",
                    "schema",
                    "source_commit",
                    "status",
                }
                or status not in statuses
                or value.get("run_id") != plan.run_id
                or value.get("source_commit") != plan.source_commit
                or value.get("schema") != "borsuk-v36-prefix-freeze-terminal-v1"
                or value.get("claim_eligible") is not False
                or value.get("attempt_id") != expected_attempt_id
                or value.get("execution_authority_sha256")
                != expected_execution_sha256
                or value.get("inputs") != expected_inputs
                or type(value.get("instance_id")) is not str
                or _INSTANCE_ID.fullmatch(value["instance_id"]) is None
                or (
                    expected_instance_id is not None
                    and value["instance_id"] != expected_instance_id
                )
                or not outputs_valid
                or (
                    status == "complete"
                    and output_roles != _COMPLETE_OUTPUT_ROLES
                )
            ):
                raise ValueError("V36 prefix-screen terminal authority differs")
            return status
    return None


def run_v36_prefix_screen(
    plan: V36PrefixScreenPlan,
    *,
    ec2_client: Any,
    s3_client: Any,
    launch_nonce: str,
) -> str:
    """Run at most three bounded Spot attempts and preserve every terminal."""

    if _read_attempt_status(s3_client, plan) is not None:
        raise ValueError("V36 prefix-screen terminal already exists")
    for attempt_ordinal in range(MAX_ATTEMPTS):
        instance_id: str | None = None
        status: str | None = None
        try:
            spec = build_v36_prefix_launch_specs(
                plan,
                launch_nonce=launch_nonce,
                attempt_ordinal=attempt_ordinal,
            )[attempt_ordinal]
            try:
                response = ec2_client.run_instances(**spec)
            except Exception as error:
                code = getattr(error, "response", {}).get("Error", {}).get("Code")
                if code in _CAPACITY_ERRORS:
                    continue
                raise
            instance_id = response["Instances"][0]["InstanceId"]
            controller_deadline = (
                time.monotonic()
                + _attempt_wall_seconds(attempt_ordinal)
                + CONTROLLER_GRACE_SECONDS
            )
            while True:
                state = ec2_client.describe_instances(InstanceIds=[instance_id])[
                    "Reservations"
                ][0]["Instances"][0]["State"]["Name"]
                if state in {"shutting-down", "terminated", "stopped", "stopping"}:
                    break
                status = _read_attempt_status(
                    s3_client,
                    plan,
                    attempt_ordinal,
                    expected_instance_id=instance_id,
                )
                if status is not None:
                    break
                if time.monotonic() >= controller_deadline:
                    raise RuntimeError(
                        f"V36 prefix-screen attempt {attempt_ordinal} controller deadline"
                    )
                time.sleep(15)
        finally:
            if instance_id is not None:
                ec2_client.terminate_instances(InstanceIds=[instance_id])
        if status is None:
            status = _read_attempt_status(
                s3_client,
                plan,
                attempt_ordinal,
                expected_instance_id=instance_id,
            )
        if status == "complete":
            bucket, key = _marker_key(plan, attempt_ordinal, "ATTEMPT_COMPLETE.json")
            return f"s3://{bucket}/{key}"
        if status == "screen-source-insufficient":
            raise RuntimeError("V36 prefix-screen source is insufficient")
        if status is None:
            raise RuntimeError(f"V36 prefix-screen attempt {attempt_ordinal} terminal missing")
    raise RuntimeError("V36 prefix-screen three attempts exhausted")


def main(argv: list[str] | None = None) -> int:
    """Run one explicit controller-only mode."""

    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--dry-run", action="store_true")
    mode.add_argument("--publish-checkpoints", action="store_true")
    parser.add_argument("--plan-json")
    parser.add_argument("--checkpoint-outbox")
    parser.add_argument("--producer-pid", type=int)
    parser.add_argument("--first-generation", type=int)
    arguments = parser.parse_args(argv)
    if arguments.publish_checkpoints:
        if (
            arguments.plan_json is not None
            or arguments.checkpoint_outbox is None
            or arguments.producer_pid is None
            or arguments.first_generation is None
        ):
            parser.error("V36 checkpoint sidecar arguments differ")
        watch_v36_checkpoint_outbox(
            pathlib.Path(arguments.checkpoint_outbox),
            arguments.producer_pid,
            V36AwsCliCheckpointTransport(),
            first_generation=arguments.first_generation,
        )
        return 0
    if (
        arguments.plan_json is None
        or arguments.checkpoint_outbox is not None
        or arguments.producer_pid is not None
        or arguments.first_generation is not None
    ):
        parser.error("V36 prefix-screen dry-run arguments differ")
    try:
        raw = json.loads(arguments.plan_json)
        if type(raw) is not dict:
            raise ValueError("V36 prefix-screen plan JSON differs")
        plan = build_v36_prefix_screen_plan(**raw)
    except (TypeError, ValueError, json.JSONDecodeError) as error:
        parser.error(str(error))
    sys.stdout.write(dry_run_v36_prefix_screen(plan).decode())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
