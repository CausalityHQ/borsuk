#!/usr/bin/env python3
"""Bounded Spot launcher for the diagnostic V36 prefix population freeze."""

from __future__ import annotations

import argparse
import base64
import dataclasses
import datetime
import hashlib
import json
import os
import pathlib
import re
import shlex
import subprocess
import sys
import tempfile
import time
import urllib.parse
from typing import Any

PROFILE = "causality"
REGION = "eu-central-1"
INSTANCE_TYPE = "r8gd.8xlarge"
ACTIVE_WALL_SECONDS = 43_200
CONTROLLER_GRACE_SECONDS = 1_800
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
MAX_TERMINAL_BYTES = 1024**2
MAX_CONTROLLER_LAUNCH_BYTES = 1024**2
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
V36_DATASET_AUTHORITY_SHA256 = (
    "0d2e8cef3cf27860131a6a8c33d08b858f8837263212cb03515ae53c76acd5c1"
)
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
_UNBOUND = object()

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
    "resume": execution["resume"],
    "run_id": run_id,
    "schema": "borsuk-v36-prefix-freeze-terminal-v2",
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


def derive_v36_prefix_screen_inputs(
    dataset_authority_bytes: bytes,
) -> tuple[bytes, bytes]:
    """Derive the bounded screen authority and registry from frozen evidence."""

    if hashlib.sha256(dataset_authority_bytes).hexdigest() != V36_DATASET_AUTHORITY_SHA256:
        raise ValueError("V36 prefix-screen dataset authority differs")
    try:
        dataset = json.loads(dataset_authority_bytes)
    except (TypeError, json.JSONDecodeError) as error:
        raise ValueError("V36 prefix-screen dataset authority differs") from error
    if (
        type(dataset) is not dict
        or canonical_json_bytes(dataset) != dataset_authority_bytes
        or set(dataset)
        != {
            "claim_eligible",
            "execution_authority",
            "materialization",
            "membership",
            "observed_at_utc",
            "schema",
            "source",
        }
        or dataset.get("claim_eligible") is not False
        or dataset.get("schema") != "borsuk-v36-funnel-dataset-authority-v1"
        or type(dataset.get("source")) is not dict
    ):
        raise ValueError("V36 prefix-screen dataset authority differs")
    source = dataset["source"]
    if set(source) != {
        "dataset_info",
        "license",
        "ordered_shard_manifest",
        "physical_schema",
        "readme",
        "repository",
        "revision",
        "revision_uri",
        "shard_missing_numeric_ordinals",
        "shards",
        "source_identity_encoding",
        "source_identity_sha256",
    } or type(source.get("ordered_shard_manifest")) is not dict:
        raise ValueError("V36 prefix-screen dataset authority differs")
    manifest = source["ordered_shard_manifest"]
    shards = source.get("shards")
    if (
        source.get("repository") != "andropar/relaion2b-natural-embeddings"
        or source.get("revision")
        != "bfc7465dcf1245bd605d35dcaf5d2177bbc2025a"
        or source.get("source_identity_sha256") != manifest.get("sha256")
        or set(manifest)
        != {"canonical_record", "encoded_bytes", "order", "sha256", "shards"}
        or manifest.get("canonical_record")
        != "utf8(path) || 0x09 || lowercase_sha256 || 0x09 || decimal_encoded_bytes || 0x0a"
        or manifest.get("order") != "ascending UTF-8 path bytes"
        or type(shards) is not list
        or type(manifest.get("shards")) is not int
        or manifest["shards"] != len(shards)
        or type(manifest.get("encoded_bytes")) is not int
        or type(manifest.get("sha256")) is not str
        or _SHA256.fullmatch(manifest["sha256"]) is None
    ):
        raise ValueError("V36 prefix-screen dataset authority differs")
    digest = hashlib.sha256()
    total_bytes = 0
    previous_path: bytes | None = None
    registry: list[dict[str, object]] = []
    for shard in shards:
        if (
            type(shard) is not dict
            or set(shard) != {"encoded_bytes", "path", "sha256", "uri"}
            or type(shard.get("encoded_bytes")) is not int
            or shard["encoded_bytes"] <= 0
            or type(shard.get("path")) is not str
            or type(shard.get("sha256")) is not str
            or _SHA256.fullmatch(shard["sha256"]) is None
            or type(shard.get("uri")) is not str
            or shard["uri"]
            != (
                "https://huggingface.co/datasets/"
                "andropar/relaion2b-natural-embeddings/resolve/"
                f"{source['revision']}/{shard['path']}"
            )
        ):
            raise ValueError("V36 prefix-screen dataset authority differs")
        path = shard["path"].encode()
        if previous_path is not None and previous_path >= path:
            raise ValueError("V36 prefix-screen dataset authority differs")
        previous_path = path
        total_bytes += shard["encoded_bytes"]
        digest.update(
            f"{shard['path']}\t{shard['sha256']}\t{shard['encoded_bytes']}\n".encode()
        )
        registry.append(dict(shard))
    if (
        total_bytes != manifest["encoded_bytes"]
        or digest.hexdigest() != manifest["sha256"]
    ):
        raise ValueError("V36 prefix-screen dataset authority differs")
    ranked_registry = sorted(
        registry,
        key=lambda shard: (
            hashlib.sha256(
                b"borsuk-v36-screen-object-v1"
                + shard["path"].encode()
                + shard["encoded_bytes"].to_bytes(8, "little")
            ).digest(),
            shard["path"].encode(),
        ),
    )
    selected_object_encoded_bytes = sum(
        shard["encoded_bytes"] for shard in ranked_registry[:16]
    )
    if selected_object_encoded_bytes != 5_485_265_954:
        raise ValueError("V36 prefix-screen selected object authority differs")
    role_specs = (
        ("development", 1_000),
        ("validation", 1_000),
        ("sealed-holdout", 1_000),
        ("performance", 10_000),
    )
    roles = []
    for role, rows in role_specs:
        seed_label = f"borsuk-v36-prefix-screen-{role}-query-v2"
        roles.append(
            {
                "role": role,
                "rows": rows,
                "seed_label": seed_label,
                "seed_sha256": hashlib.sha256(seed_label.encode()).hexdigest(),
            }
        )
    authority = {
        "claim_eligible": False,
        "cohort_ordinal": 0,
        "construction_capability": "named-query-excluded-corpus-only-no-query-truth",
        "corpus_seed_label": "borsuk-v36-prefix-screen-corpus-v2",
        "corpus_seed_sha256": hashlib.sha256(
            b"borsuk-v36-prefix-screen-corpus-v2"
        ).hexdigest(),
        "corpus_rows": 1_000_000,
        "dataset_authority_sha256": V36_DATASET_AUTHORITY_SHA256,
        "distinct_candidates": 1_100_000,
        "duplicate_rule": "first-selected-object-ordinal-then-row-offset",
        "evaluation_capability": "named-artifacts-only-no-source-list-discovery",
        "excluded_population_identity": None,
        "future_full_source_exclusion_roles": [
            "development",
            "validation",
            "sealed-holdout",
            "performance",
        ],
        "invalid_row_policy": "reject-complete-source-revision",
        "object_cap": 16,
        "object_sampling_algorithm": (
            "sha256-borsuk-v36-screen-object-v1-path-utf8-length-le-u64-then-path"
        ),
        "ordered_source_manifest_sha256": manifest["sha256"],
        "population_sampling_algorithm": (
            "sha256-seed-sha256-manifest-sha256-feature-row-id-le-u64-v2"
        ),
        "population_seed_label": "borsuk-v36-prefix-screen-population-row-v2",
        "population_seed_sha256": hashlib.sha256(
            b"borsuk-v36-prefix-screen-population-row-v2"
        ).hexdigest(),
        "registry_encoded_bytes": total_bytes,
        "registry_objects": len(registry),
        "roles": roles,
        "schema": "borsuk-v36-prefix-freeze-authority-v2",
        "selected_object_count": 16,
        "selected_object_encoded_bytes": selected_object_encoded_bytes,
        "selected_object_start": 0,
        "source_byte_cap": 6 * 1_024**3,
        "source_revision": source["revision"],
        "workspace_bytes": 32 * 1_024**2,
        "workspace_count": 16,
    }
    return canonical_json_bytes(authority), canonical_json_bytes(registry)


def _write_immutable_local_bytes(path: pathlib.Path, payload: bytes) -> None:
    """Create one local artifact atomically or accept its identical bytes."""

    if path.exists():
        if not path.is_file() or path.read_bytes() != payload:
            raise ValueError("V36 prefix-screen local output differs")
        return
    if not path.parent.is_dir():
        raise ValueError("V36 prefix-screen local output parent differs")
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
    )
    temporary = pathlib.Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        try:
            os.link(temporary, path)
        except FileExistsError:
            if not path.is_file() or path.read_bytes() != payload:
                raise ValueError("V36 prefix-screen local output differs") from None
    finally:
        temporary.unlink(missing_ok=True)


def write_v36_prefix_screen_inputs(
    dataset_authority_path: pathlib.Path,
    authority_output_path: pathlib.Path,
    registry_output_path: pathlib.Path,
) -> None:
    """Write the two exact derived inputs without replacing conflicting files."""

    paths = (dataset_authority_path, authority_output_path, registry_output_path)
    if len({path.resolve() for path in paths}) != len(paths):
        raise ValueError("V36 prefix-screen local output differs")
    authority_bytes, registry_bytes = derive_v36_prefix_screen_inputs(
        dataset_authority_path.read_bytes()
    )
    for path, payload in (
        (authority_output_path, authority_bytes),
        (registry_output_path, registry_bytes),
    ):
        if path.exists() and (not path.is_file() or path.read_bytes() != payload):
            raise ValueError("V36 prefix-screen local output differs")
    _write_immutable_local_bytes(authority_output_path, authority_bytes)
    _write_immutable_local_bytes(registry_output_path, registry_bytes)


def _read_s3_bytes(
    s3_client: Any, uri: str, maximum: int
) -> tuple[bytes, str | None]:
    bucket, key = _s3(uri)
    response = s3_client.get_object(Bucket=bucket, Key=key)
    content_length = response.get("ContentLength")
    if (
        type(maximum) is not int
        or maximum <= 0
        or type(content_length) is not int
        or not 0 < content_length <= maximum
    ):
        raise ValueError("V36 checkpoint object length differs")
    body = response["Body"].read(maximum + 1)
    if len(body) != content_length:
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
        or value.get("schema") != "borsuk-v36-prefix-checkpoint-pointer-v2"
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
    except Exception as error:
        code = getattr(error, "response", {}).get("Error", {}).get("Code")
        if code not in {"PreconditionFailed", "ConditionalRequestConflict", "412"}:
            raise
        observed, _ = _read_s3_bytes(s3_client, uri, len(body))
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
        != "borsuk-v36-prefix-freeze-checkpoint-v2"
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
        observed, etag = _read_s3_bytes(
            s3_client, pointer_uri, MAX_CHECKPOINT_POINTER_BYTES
        )
        if observed != pointer_bytes or etag is None:
            raise ValueError("V36 checkpoint pointer observation differs") from None
        return etag


def read_v36_checkpoint_head(
    s3_client: Any, pointer_uri: str
) -> tuple[bytes, bytes, str | None]:
    """Read exactly the newest pointer and its manifest, failing closed."""

    pointer_bytes, etag = _read_s3_bytes(
        s3_client, pointer_uri, MAX_CHECKPOINT_POINTER_BYTES
    )
    manifest_bytes = _read_v36_checkpoint_manifest(s3_client, pointer_bytes)
    return pointer_bytes, manifest_bytes, etag


def _read_v36_checkpoint_manifest(s3_client: Any, pointer_bytes: bytes) -> bytes:
    """Read and authenticate the manifest named by one exact pointer."""

    pointer = _checkpoint_pointer_value(pointer_bytes)
    manifest_identity = pointer["manifest"]
    manifest_bytes, _ = _read_s3_bytes(
        s3_client,
        str(manifest_identity["uri"]),
        MAX_CHECKPOINT_MANIFEST_BYTES,
    )
    try:
        manifest = json.loads(manifest_bytes)
    except (TypeError, json.JSONDecodeError) as error:
        raise ValueError("V36 checkpoint manifest authority differs") from error
    if (
        len(manifest_bytes) != manifest_identity["encoded_bytes"]
        or hashlib.sha256(manifest_bytes).hexdigest() != manifest_identity["sha256"]
        or type(manifest) is not dict
        or canonical_json_bytes(manifest) != manifest_bytes
        or manifest.get("schema") != "borsuk-v36-prefix-freeze-checkpoint-v2"
        or manifest.get("generation") != pointer["generation"]
    ):
        raise ValueError("V36 checkpoint manifest authority differs")
    return manifest_bytes


def read_v36_checkpoint_head_if_present(
    s3_client: Any, pointer_uri: str
) -> tuple[bytes, bytes, str | None] | None:
    """Read the sole newest head, distinguishing only an absent pointer."""

    try:
        pointer_bytes, etag = _read_s3_bytes(
            s3_client, pointer_uri, MAX_CHECKPOINT_POINTER_BYTES
        )
    except Exception as error:
        code = getattr(error, "response", {}).get("Error", {}).get("Code")
        if code in {"NoSuchKey", "404"}:
            return None
        raise
    manifest_bytes = _read_v36_checkpoint_manifest(s3_client, pointer_bytes)
    return pointer_bytes, manifest_bytes, etag


def v36_checkpoint_resume_binding(
    plan: V36PrefixScreenPlan,
    attempt_ordinal: int,
    pointer_bytes: bytes,
    manifest_bytes: bytes,
) -> dict[str, object]:
    """Bind one exact older-attempt checkpoint head for replacement."""

    pointer = _checkpoint_pointer_value(pointer_bytes)
    try:
        manifest = json.loads(manifest_bytes)
    except (TypeError, json.JSONDecodeError) as error:
        raise ValueError("V36 checkpoint resume binding differs") from error
    manifest_identity = pointer["manifest"]
    if (
        type(attempt_ordinal) is not int
        or not 0 < attempt_ordinal < MAX_ATTEMPTS
        or pointer["run_id"] != plan.run_id
        or pointer["producer_attempt_ordinal"] >= attempt_ordinal
        or len(manifest_bytes) != manifest_identity["encoded_bytes"]
        or hashlib.sha256(manifest_bytes).hexdigest() != manifest_identity["sha256"]
        or type(manifest) is not dict
        or canonical_json_bytes(manifest) != manifest_bytes
        or manifest.get("schema") != "borsuk-v36-prefix-freeze-checkpoint-v2"
        or manifest.get("generation") != pointer["generation"]
    ):
        raise ValueError("V36 checkpoint resume binding differs")
    binding = {
        "generation": pointer["generation"],
        "manifest": manifest_identity,
        "pointer_encoded_bytes": len(pointer_bytes),
        "pointer_sha256": hashlib.sha256(pointer_bytes).hexdigest(),
        "pointer_uri": (
            f"{plan.output_prefix}checkpoints/runs/{plan.run_id}/latest.json"
        ),
    }
    _validate_v36_resume_closure(binding, pointer, manifest_bytes)
    return binding


def _validate_v36_resume_closure(
    binding: dict[str, object],
    pointer: dict[str, object],
    manifest_bytes: bytes,
) -> list[dict[str, object]]:
    """Validate the bounded population dependency closure before a launch."""

    try:
        manifest = json.loads(manifest_bytes)
    except (TypeError, json.JSONDecodeError) as error:
        raise ValueError("V36 checkpoint resume manifest differs") from error
    population = manifest.get("population") if type(manifest) is dict else None
    dependencies = population.get("identity_runs") if type(population) is dict else None
    if (
        canonical_json_bytes(manifest) != manifest_bytes
        or manifest.get("schema") != "borsuk-v36-prefix-freeze-checkpoint-v2"
        or manifest.get("generation") != binding["generation"]
        or manifest.get("run_id") != pointer["run_id"]
        or manifest.get("producer_attempt_id") != pointer["producer_attempt_id"]
        or manifest.get("producer_attempt_ordinal")
        != pointer["producer_attempt_ordinal"]
        or manifest.get("phase") != {"kind": "population"}
        or type(dependencies) is not list
        or not 0 < len(dependencies) <= CHECKPOINT_OBJECTS
    ):
        raise ValueError("V36 checkpoint resume manifest differs")
    manifest_identity = _outbox_artifact_identity(binding["manifest"])
    manifest_bucket, manifest_key = _s3(manifest_identity["uri"])
    object_marker = "checkpoints/objects/"
    campaign_prefix, marker, _ = manifest_key.partition(object_marker)
    object_prefix = f"{campaign_prefix}{object_marker}"
    expected_pointer = (
        f"s3://{manifest_bucket}/{campaign_prefix}checkpoints/runs/"
        f"{pointer['run_id']}/latest.json"
    )
    if marker != object_marker or binding["pointer_uri"] != expected_pointer:
        raise ValueError("V36 checkpoint resume namespace differs")
    identities = []
    for ordinal, raw in enumerate(dependencies):
        identity = _outbox_artifact_identity(raw)
        bucket, key = _s3(identity["uri"])
        if (
            identity["role"] != f"population-identity-run-{ordinal:04d}"
            or identity["encoded_bytes"] > MAX_CHECKPOINT_DEPENDENCY_BYTES
            or bucket != manifest_bucket
            or not key.startswith(object_prefix)
            or key == object_prefix
        ):
            raise ValueError("V36 checkpoint resume dependency differs")
        identities.append(identity)
    return identities


def materialize_v36_checkpoint_resume(
    binding: dict[str, object],
    destination: pathlib.Path,
    transport: Any,
) -> int:
    """Stage exactly one bound newest population head for Rust validation."""

    if (
        type(binding) is not dict
        or set(binding)
        != {
            "generation",
            "manifest",
            "pointer_encoded_bytes",
            "pointer_sha256",
            "pointer_uri",
        }
        or type(binding.get("generation")) is not int
        or binding["generation"] < 0
        or type(binding.get("pointer_encoded_bytes")) is not int
        or not 0 < binding["pointer_encoded_bytes"] <= MAX_CHECKPOINT_POINTER_BYTES
        or type(binding.get("pointer_sha256")) is not str
        or _SHA256.fullmatch(binding["pointer_sha256"]) is None
        or type(binding.get("pointer_uri")) is not str
        or not isinstance(destination, pathlib.Path)
        or destination.is_symlink()
        or not destination.is_dir()
    ):
        raise ValueError("V36 checkpoint resume binding differs")
    if next(destination.iterdir(), None) is not None:
        raise ValueError("V36 checkpoint resume destination differs")
    manifest_identity = _outbox_artifact_identity(binding["manifest"])
    if (
        manifest_identity["role"] != "checkpoint-manifest"
        or manifest_identity["encoded_bytes"] > MAX_CHECKPOINT_MANIFEST_BYTES
    ):
        raise ValueError("V36 checkpoint resume manifest differs")

    pointer_bytes = transport.read_bytes(
        binding["pointer_uri"], MAX_CHECKPOINT_POINTER_BYTES
    )
    if (
        type(pointer_bytes) is not bytes
        or len(pointer_bytes) != binding["pointer_encoded_bytes"]
        or hashlib.sha256(pointer_bytes).hexdigest() != binding["pointer_sha256"]
    ):
        raise ValueError("V36 checkpoint resume pointer differs")
    pointer = _checkpoint_pointer_value(pointer_bytes)
    if (
        pointer["generation"] != binding["generation"]
        or pointer["manifest"] != manifest_identity
    ):
        raise ValueError("V36 checkpoint resume pointer differs")

    manifest_bytes = transport.read_bytes(
        manifest_identity["uri"], MAX_CHECKPOINT_MANIFEST_BYTES
    )
    if (
        type(manifest_bytes) is not bytes
        or len(manifest_bytes) != manifest_identity["encoded_bytes"]
        or hashlib.sha256(manifest_bytes).hexdigest() != manifest_identity["sha256"]
    ):
        raise ValueError("V36 checkpoint resume manifest differs")
    identities = _validate_v36_resume_closure(binding, pointer, manifest_bytes)

    objects = destination / "objects"
    objects.mkdir()
    for identity in identities:
        path = objects / f"{identity['sha256']}.blob"
        transport.download(identity, path)
        _authenticate_outbox_file(path, identity)
    (destination / "manifest.json").write_bytes(manifest_bytes)
    (destination / "pointer.json").write_bytes(pointer_bytes)
    return binding["generation"] + 1


def prepare_v36_checkpoint_resume(
    execution_authority_path: pathlib.Path,
    destination: pathlib.Path,
    transport: Any,
) -> int:
    """Stage the exact optional head bound by one execution authority."""

    try:
        authority_bytes = execution_authority_path.read_bytes()
        authority = json.loads(authority_bytes)
    except (OSError, json.JSONDecodeError) as error:
        raise ValueError("V36 checkpoint execution authority differs") from error
    if (
        type(authority) is not dict
        or canonical_json_bytes(authority) != authority_bytes
        or set(authority)
        != {
            "active_wall_seconds",
            "attempt_id",
            "checkpoint_seconds",
            "claim_eligible",
            "inputs",
            "output_prefix",
            "resume",
            "schema",
            "source_commit",
        }
        or authority.get("schema")
        != "borsuk-v36-prefix-freeze-execution-authority-v2"
        or not isinstance(destination, pathlib.Path)
        or destination.is_symlink()
        or not destination.is_dir()
        or next(destination.iterdir(), None) is not None
    ):
        raise ValueError("V36 checkpoint execution authority differs")
    resume = authority["resume"]
    generation = (
        0
        if resume is None
        else materialize_v36_checkpoint_resume(resume, destination, transport)
    )
    (destination / "first-generation").write_text(f"{generation}\n")
    return generation


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
        or manifest_value.get("schema") != "borsuk-v36-prefix-freeze-checkpoint-v2"
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

    def read_bytes(self, uri: str, maximum: int) -> bytes:
        if type(maximum) is not int or maximum <= 0:
            raise ValueError("V36 checkpoint AWS read limit differs")
        value = bytearray()
        for chunk in self._runner.stream(
            ["aws", "s3", "cp", uri, "-", "--only-show-errors", "--region", REGION]
        ):
            if type(chunk) is not bytes or not chunk:
                raise ValueError("V36 checkpoint AWS stream differs")
            value.extend(chunk)
            if len(value) > maximum:
                raise ValueError("V36 checkpoint AWS read limit differs")
        return bytes(value)

    def download(self, identity: dict[str, object], path: pathlib.Path) -> None:
        expected = _outbox_artifact_identity(identity)
        if path.exists() or path.is_symlink() or not path.parent.is_dir():
            raise ValueError("V36 checkpoint local dependency differs")
        written = 0
        with path.open("xb") as output:
            for chunk in self._runner.stream(
                [
                    "aws",
                    "s3",
                    "cp",
                    str(expected["uri"]),
                    "-",
                    "--only-show-errors",
                    "--region",
                    REGION,
                ]
            ):
                if type(chunk) is not bytes or not chunk:
                    raise ValueError("V36 checkpoint AWS stream differs")
                written += len(chunk)
                if written > expected["encoded_bytes"]:
                    raise ValueError("V36 checkpoint local dependency differs")
                output.write(chunk)
        if written != expected["encoded_bytes"]:
            raise ValueError("V36 checkpoint local dependency differs")

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
    campaign_seconds = (
        CAMPAIGN_CAP_MICRO_USD * 3_600 // SPOT_HOURLY_CAP_MICRO_USD
    )
    spent_seconds_before = attempt_ordinal * (
        ACTIVE_WALL_SECONDS + CONTROLLER_GRACE_SECONDS
    )
    return min(
        ACTIVE_WALL_SECONDS,
        campaign_seconds - spent_seconds_before - CONTROLLER_GRACE_SECONDS,
    )


def _execution_authority(
    plan: V36PrefixScreenPlan,
    attempt_ordinal: int,
    *,
    resume: dict[str, object] | None = None,
) -> dict[str, object]:
    if resume is not None:
        expected_pointer_uri = (
            f"{plan.output_prefix}checkpoints/runs/{plan.run_id}/latest.json"
        )
        if (
            attempt_ordinal == 0
            or type(resume) is not dict
            or set(resume)
            != {
                "generation",
                "manifest",
                "pointer_encoded_bytes",
                "pointer_sha256",
                "pointer_uri",
            }
            or type(resume.get("generation")) is not int
            or resume["generation"] < 0
            or type(resume.get("pointer_encoded_bytes")) is not int
            or resume["pointer_encoded_bytes"] <= 0
            or type(resume.get("pointer_sha256")) is not str
            or _SHA256.fullmatch(resume["pointer_sha256"]) is None
            or resume.get("pointer_uri") != expected_pointer_uri
            or _outbox_artifact_identity(resume.get("manifest"))["role"]
            != "checkpoint-manifest"
        ):
            raise ValueError("V36 checkpoint resume binding differs")
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
        "resume": resume,
        "schema": "borsuk-v36-prefix-freeze-execution-authority-v2",
        "source_commit": plan.source_commit,
    }
def _user_data(
    plan: V36PrefixScreenPlan,
    *,
    attempt_ordinal: int,
    resume: dict[str, object] | None = None,
) -> str:
    quoted = {
        field.name: shlex.quote(str(getattr(plan, field.name)))
        for field in dataclasses.fields(plan)
    }
    output_bucket, output_key = _s3(plan.output_prefix, prefix=True)
    attempt_prefix = f"{output_key}attempt-{attempt_ordinal:04d}/"
    wall_seconds = _attempt_wall_seconds(attempt_ordinal)
    execution_authority = canonical_json_bytes(
        _execution_authority(plan, attempt_ordinal, resume=resume)
    )
    execution_authority_b64 = base64.b64encode(execution_authority).decode()
    terminal_program_b64 = base64.b64encode(_GUEST_TERMINAL_PROGRAM.encode()).decode()
    sidecar_sha256 = hashlib.sha256(pathlib.Path(__file__).read_bytes()).hexdigest()
    resume_argument = (
        '--resume-checkpoint "$root/resume" '
        if resume is not None
        else ""
    )
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
mkdir "$root/output" "$root/scratch" "$root/checkpoint-outbox" "$root/sidecar-source" "$root/resume"
chmod 700 "$root/checkpoint-outbox"
tar --zstd -xf "$root/source.tar.zst" -C "$root/sidecar-source" scripts/run_v36_prefix_screen.py
test "$(sha256sum "$root/sidecar-source/scripts/run_v36_prefix_screen.py" | cut -d' ' -f1)" = {sidecar_sha256}
python3 -m py_compile "$root/sidecar-source/scripts/run_v36_prefix_screen.py"
aws s3api put-object --generate-cli-skeleton input | grep -q '"IfMatch"'
token=$(curl -fsS -X PUT -H 'X-aws-ec2-metadata-token-ttl-seconds: 21600' http://169.254.169.254/latest/api/token)
instance_id=$(curl -fsS -H "X-aws-ec2-metadata-token: $token" http://169.254.169.254/latest/meta-data/instance-id)
set +e
python3 "$root/sidecar-source/scripts/run_v36_prefix_screen.py" \
  --materialize-resume --execution-authority "$root/execution-authority.json" \
  --resume-directory "$root/resume"
status=$?
if [[ "$status" = 0 ]]; then
  first_generation=$(cat "$root/resume/first-generation")
  if ! [[ "$first_generation" =~ ^[0-9]+$ ]]; then
    status=70
  fi
fi
if [[ "$status" = 0 ]]; then
timeout --signal=TERM --kill-after=30 {wall_seconds} "$root/v36_prefix_freeze" \
  --execute-prefix-freeze \
  --execution-authority "$root/execution-authority.json" \
  --authority "$root/authority.json" --source-registry "$root/source-registry.json" \
  --source-archive "$root/source.tar.zst" --output "$root/output" \
  --scratch "$root/scratch" --checkpoint-outbox "$root/checkpoint-outbox" \
  {resume_argument} --producer-instance-id "$instance_id" &
science_pid=$!
python3 "$root/sidecar-source/scripts/run_v36_prefix_screen.py" \
  --publish-checkpoints --checkpoint-outbox "$root/checkpoint-outbox" \
  --producer-pid "$science_pid" --first-generation "$first_generation" &
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
    plan: V36PrefixScreenPlan,
    *,
    launch_nonce: str,
    attempt_ordinal: int,
    resume: dict[str, object] | None = None,
) -> list[dict[str, object]]:
    """Build the three registered Spot-zone candidates for one attempt."""

    if re.fullmatch(r"[0-9a-f]{32}", launch_nonce) is None:
        raise ValueError("V36 prefix-screen launch nonce differs")
    data = base64.b64encode(
        _user_data(plan, attempt_ordinal=attempt_ordinal, resume=resume).encode()
    ).decode()
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


def _controller_launch_uri(plan: V36PrefixScreenPlan, attempt_ordinal: int) -> str:
    if type(attempt_ordinal) is not int or not 0 <= attempt_ordinal < MAX_ATTEMPTS:
        raise ValueError("V36 prefix-screen attempt ordinal differs")
    return (
        f"{plan.output_prefix}controller/"
        f"attempt-{attempt_ordinal:04d}-launch.json"
    )


def _controller_capacity_uri(plan: V36PrefixScreenPlan, attempt_ordinal: int) -> str:
    if type(attempt_ordinal) is not int or not 0 <= attempt_ordinal < MAX_ATTEMPTS:
        raise ValueError("V36 prefix-screen attempt ordinal differs")
    return (
        f"{plan.output_prefix}controller/"
        f"attempt-{attempt_ordinal:04d}-capacity.json"
    )


def _controller_instance_uri(plan: V36PrefixScreenPlan, attempt_ordinal: int) -> str:
    if type(attempt_ordinal) is not int or not 0 <= attempt_ordinal < MAX_ATTEMPTS:
        raise ValueError("V36 prefix-screen attempt ordinal differs")
    return (
        f"{plan.output_prefix}controller/"
        f"attempt-{attempt_ordinal:04d}-instance.json"
    )


def write_v36_controller_capacity(
    s3_client: Any,
    plan: V36PrefixScreenPlan,
    launch: dict[str, object],
) -> None:
    """Record that one frozen Spot candidate never allocated an instance."""

    attempt_ordinal = launch["attempt_ordinal"]
    encoded_launch = canonical_json_bytes(launch)
    value = {
        "attempt_ordinal": attempt_ordinal,
        "claim_eligible": False,
        "launch_sha256": hashlib.sha256(encoded_launch).hexdigest(),
        "run_id": plan.run_id,
        "schema": "borsuk-v36-prefix-controller-capacity-v1",
    }
    _put_immutable_s3_bytes(
        s3_client,
        _controller_capacity_uri(plan, attempt_ordinal),
        canonical_json_bytes(value),
    )


def read_v36_controller_capacity(
    s3_client: Any,
    plan: V36PrefixScreenPlan,
    attempt_ordinal: int,
    launch: dict[str, object] | None,
) -> bool:
    """Authenticate the optional terminal capacity outcome for one launch."""

    try:
        encoded, _ = _read_s3_bytes(
            s3_client,
            _controller_capacity_uri(plan, attempt_ordinal),
            MAX_CONTROLLER_LAUNCH_BYTES,
        )
    except Exception as error:
        code = getattr(error, "response", {}).get("Error", {}).get("Code")
        if code in {"NoSuchKey", "404"}:
            return False
        raise
    try:
        value = json.loads(encoded)
    except json.JSONDecodeError as error:
        raise ValueError("V36 prefix-screen controller capacity differs") from error
    if (
        launch is None
        or type(value) is not dict
        or set(value)
        != {
            "attempt_ordinal",
            "claim_eligible",
            "launch_sha256",
            "run_id",
            "schema",
        }
        or canonical_json_bytes(value) != encoded
        or type(value.get("attempt_ordinal")) is not int
        or value["attempt_ordinal"] != attempt_ordinal
        or value.get("claim_eligible") is not False
        or value.get("launch_sha256")
        != hashlib.sha256(canonical_json_bytes(launch)).hexdigest()
        or value.get("run_id") != plan.run_id
        or value.get("schema") != "borsuk-v36-prefix-controller-capacity-v1"
    ):
        raise ValueError("V36 prefix-screen controller capacity differs")
    return True


def _validate_v36_controller_instance(
    value: object,
    encoded: bytes,
    plan: V36PrefixScreenPlan,
    attempt_ordinal: int,
    launch: dict[str, object] | None,
) -> dict[str, object]:
    if (
        launch is None
        or type(value) is not dict
        or set(value)
        != {
            "attempt_ordinal",
            "claim_eligible",
            "controller_deadline_epoch_seconds",
            "instance_id",
            "launch_time_epoch_seconds",
            "launch_sha256",
            "run_id",
            "schema",
        }
        or canonical_json_bytes(value) != encoded
        or type(value.get("attempt_ordinal")) is not int
        or value["attempt_ordinal"] != attempt_ordinal
        or value.get("claim_eligible") is not False
        or type(value.get("launch_time_epoch_seconds")) is not int
        or type(value.get("controller_deadline_epoch_seconds")) is not int
        or value["controller_deadline_epoch_seconds"]
        != launch.get("controller_deadline_epoch_seconds")
        or type(value.get("instance_id")) is not str
        or _INSTANCE_ID.fullmatch(value["instance_id"]) is None
        or value.get("launch_sha256")
        != hashlib.sha256(canonical_json_bytes(launch)).hexdigest()
        or value.get("run_id") != plan.run_id
        or value.get("schema") != "borsuk-v36-prefix-controller-instance-v1"
    ):
        raise ValueError("V36 prefix-screen controller instance differs")
    return value


def read_v36_controller_instance_if_present(
    s3_client: Any,
    plan: V36PrefixScreenPlan,
    attempt_ordinal: int,
    launch: dict[str, object] | None,
) -> dict[str, object] | None:
    """Authenticate the optional immutable EC2 identity for one launch."""

    try:
        encoded, _ = _read_s3_bytes(
            s3_client,
            _controller_instance_uri(plan, attempt_ordinal),
            MAX_CONTROLLER_LAUNCH_BYTES,
        )
    except Exception as error:
        code = getattr(error, "response", {}).get("Error", {}).get("Code")
        if code in {"NoSuchKey", "404"}:
            return None
        raise
    try:
        value = json.loads(encoded)
    except json.JSONDecodeError as error:
        raise ValueError("V36 prefix-screen controller instance differs") from error
    return _validate_v36_controller_instance(
        value, encoded, plan, attempt_ordinal, launch
    )


def write_v36_controller_instance(
    s3_client: Any,
    plan: V36PrefixScreenPlan,
    launch: dict[str, object],
    instance_id: str,
    launch_time: datetime.datetime,
) -> dict[str, object]:
    """Persist the EC2 identity so a restart never launches its replacement."""

    attempt_ordinal = launch["attempt_ordinal"]
    if (
        type(launch_time) is not datetime.datetime
        or launch_time.tzinfo is None
        or launch_time.utcoffset() != datetime.timedelta(0)
    ):
        raise ValueError("V36 prefix-screen controller instance differs")
    launch_time_epoch_seconds = int(launch_time.timestamp())
    value = {
        "attempt_ordinal": attempt_ordinal,
        "claim_eligible": False,
        "controller_deadline_epoch_seconds": launch[
            "controller_deadline_epoch_seconds"
        ],
        "instance_id": instance_id,
        "launch_time_epoch_seconds": launch_time_epoch_seconds,
        "launch_sha256": hashlib.sha256(canonical_json_bytes(launch)).hexdigest(),
        "run_id": plan.run_id,
        "schema": "borsuk-v36-prefix-controller-instance-v1",
    }
    encoded = canonical_json_bytes(value)
    _validate_v36_controller_instance(
        value, encoded, plan, attempt_ordinal, launch
    )
    _put_immutable_s3_bytes(
        s3_client, _controller_instance_uri(plan, attempt_ordinal), encoded
    )
    return value


def _reconcile_v36_controller_instance(
    ec2_client: Any,
    s3_client: Any,
    plan: V36PrefixScreenPlan,
    launch: dict[str, object],
    *,
    expected_instance_id: str | None = None,
) -> dict[str, object] | None:
    """Recover the EC2 identity after an accepted launch response was lost."""

    token = launch["launch_spec"]["ClientToken"]
    response = ec2_client.describe_instances(
        Filters=[{"Name": "client-token", "Values": [token]}]
    )
    instances = [
        instance
        for reservation in response.get("Reservations", [])
        for instance in reservation.get("Instances", [])
    ]
    if not instances:
        return None
    if len(instances) != 1:
        raise ValueError("V36 prefix-screen controller instance conflict")
    instance = instances[0]
    if (
        type(instance) is not dict
        or instance.get("ClientToken") != token
        or type(instance.get("InstanceId")) is not str
        or _INSTANCE_ID.fullmatch(instance["InstanceId"]) is None
        or (
            expected_instance_id is not None
            and instance["InstanceId"] != expected_instance_id
        )
    ):
        raise ValueError("V36 prefix-screen controller instance differs")
    return write_v36_controller_instance(
        s3_client,
        plan,
        launch,
        instance["InstanceId"],
        instance.get("LaunchTime"),
    )


def _validate_v36_controller_launch(
    value: object,
    encoded: bytes,
    plan: V36PrefixScreenPlan,
    attempt_ordinal: int,
) -> dict[str, object]:
    if (
        type(value) is not dict
        or set(value)
        != {
            "attempt_ordinal",
            "claim_eligible",
            "controller_deadline_epoch_seconds",
            "execution_authority",
            "launch_nonce",
            "launch_spec",
            "run_id",
            "schema",
        }
        or canonical_json_bytes(value) != encoded
        or value.get("schema") != "borsuk-v36-prefix-controller-launch-v1"
        or value.get("claim_eligible") is not False
        or type(value.get("controller_deadline_epoch_seconds")) is not int
        or value["controller_deadline_epoch_seconds"] <= 0
        or value.get("run_id") != plan.run_id
        or type(value.get("attempt_ordinal")) is not int
        or value.get("attempt_ordinal") != attempt_ordinal
        or type(value.get("launch_nonce")) is not str
        or re.fullmatch(r"[0-9a-f]{32}", value["launch_nonce"]) is None
        or type(value.get("execution_authority")) is not dict
    ):
        raise ValueError("V36 prefix-screen controller launch differs")
    resume = value["execution_authority"].get("resume")
    expected_execution = _execution_authority(
        plan, attempt_ordinal, resume=resume
    )
    expected_spec = build_v36_prefix_launch_specs(
        plan,
        launch_nonce=value["launch_nonce"],
        attempt_ordinal=attempt_ordinal,
        resume=resume,
    )[attempt_ordinal]
    if (
        canonical_json_bytes(value["execution_authority"])
        != canonical_json_bytes(expected_execution)
        or canonical_json_bytes(value.get("launch_spec"))
        != canonical_json_bytes(expected_spec)
    ):
        raise ValueError("V36 prefix-screen controller launch differs")
    return value


def load_or_create_v36_controller_launch(
    s3_client: Any,
    plan: V36PrefixScreenPlan,
    attempt_ordinal: int,
    launch_nonce: str,
    resume: dict[str, object] | None,
) -> dict[str, object]:
    """Persist or reuse the sole immutable launch authority for one attempt."""

    uri = _controller_launch_uri(plan, attempt_ordinal)
    existing = read_v36_controller_launch_if_present(
        s3_client, plan, attempt_ordinal
    )
    if existing is not None:
        return existing
    execution_authority = _execution_authority(
        plan, attempt_ordinal, resume=resume
    )
    value = {
        "attempt_ordinal": attempt_ordinal,
        "claim_eligible": False,
        "controller_deadline_epoch_seconds": int(time.time())
        + _attempt_wall_seconds(attempt_ordinal)
        + CONTROLLER_GRACE_SECONDS,
        "execution_authority": execution_authority,
        "launch_nonce": launch_nonce,
        "launch_spec": build_v36_prefix_launch_specs(
            plan,
            launch_nonce=launch_nonce,
            attempt_ordinal=attempt_ordinal,
            resume=resume,
        )[attempt_ordinal],
        "run_id": plan.run_id,
        "schema": "borsuk-v36-prefix-controller-launch-v1",
    }
    encoded = canonical_json_bytes(value)
    if len(encoded) > MAX_CONTROLLER_LAUNCH_BYTES:
        raise ValueError("V36 prefix-screen controller launch length differs")
    _put_immutable_s3_bytes(s3_client, uri, encoded)
    return value


def read_v36_controller_launch_if_present(
    s3_client: Any,
    plan: V36PrefixScreenPlan,
    attempt_ordinal: int,
) -> dict[str, object] | None:
    """Read one immutable launch authority without creating it."""

    uri = _controller_launch_uri(plan, attempt_ordinal)
    try:
        encoded, _ = _read_s3_bytes(
            s3_client, uri, MAX_CONTROLLER_LAUNCH_BYTES
        )
    except Exception as error:
        code = getattr(error, "response", {}).get("Error", {}).get("Code")
        if code in {"NoSuchKey", "404"}:
            return None
        raise
    try:
        value = json.loads(encoded)
    except json.JSONDecodeError as error:
        raise ValueError("V36 prefix-screen controller launch differs") from error
    return _validate_v36_controller_launch(value, encoded, plan, attempt_ordinal)


def _marker_key(plan: V36PrefixScreenPlan, attempt_ordinal: int, marker: str) -> tuple[str, str]:
    bucket, prefix = _s3(plan.output_prefix, prefix=True)
    if marker not in {"ATTEMPT_COMPLETE.json", "ATTEMPT_FAILED.json", "INTERRUPTED.json"}:
        raise ValueError("V36 prefix-screen marker differs")
    return bucket, f"{prefix}attempt-{attempt_ordinal:04d}/{marker}"


def _terminate_v36_instance(ec2_client: Any, instance_id: str) -> None:
    """Terminate one attempt and prove it can no longer advance its head."""

    try:
        ec2_client.terminate_instances(InstanceIds=[instance_id])
    except Exception as error:
        code = getattr(error, "response", {}).get("Error", {}).get("Code")
        if code == "InvalidInstanceID.NotFound":
            return
        raise
    ec2_client.get_waiter("instance_terminated").wait(
        InstanceIds=[instance_id],
        WaiterConfig={"Delay": 5, "MaxAttempts": 60},
    )


def _controller_infrastructure_terminal_bytes(
    plan: V36PrefixScreenPlan,
    execution_authority: dict[str, object],
    instance_id: str,
) -> bytes:
    """Create the exact terminal for a producer that died before finalizing."""

    return canonical_json_bytes(
        {
            "attempt_id": execution_authority["attempt_id"],
            "claim_eligible": False,
            "execution_authority_sha256": hashlib.sha256(
                canonical_json_bytes(execution_authority)
            ).hexdigest(),
            "inputs": execution_authority["inputs"],
            "instance_id": instance_id,
            "outputs": [],
            "resume": execution_authority["resume"],
            "run_id": plan.run_id,
            "schema": "borsuk-v36-prefix-freeze-terminal-v2",
            "source_commit": plan.source_commit,
            "status": "infrastructure",
        }
    )


def _read_attempt_status(
    s3_client: Any,
    plan: V36PrefixScreenPlan,
    attempt_ordinal: int | None = None,
    *,
    expected_instance_id: str | None = None,
    expected_resume: dict[str, object] | None | object = _UNBOUND,
    return_terminal: bool = False,
) -> str | dict[str, object] | None:
    """Read one authenticated terminal status, or prove it absent."""

    ordinals = range(MAX_ATTEMPTS) if attempt_ordinal is None else (attempt_ordinal,)
    for ordinal in ordinals:
        observed: list[tuple[str, dict[str, object]]] = []
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
            content_length = response.get("ContentLength")
            if (
                type(content_length) is not int
                or not 0 < content_length <= MAX_TERMINAL_BYTES
            ):
                raise ValueError("V36 prefix-screen terminal length differs")
            body = response["Body"].read(MAX_TERMINAL_BYTES + 1)
            value = json.loads(body)
            if (
                expected_resume is not _UNBOUND
                and canonical_json_bytes(value.get("resume"))
                != canonical_json_bytes(expected_resume)
            ):
                raise ValueError("V36 prefix-screen terminal authority differs")
            execution_authority = _execution_authority(
                plan, ordinal, resume=value.get("resume")
            )
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
                    "resume",
                    "run_id",
                    "schema",
                    "source_commit",
                    "status",
                }
                or status not in statuses
                or value.get("run_id") != plan.run_id
                or value.get("source_commit") != plan.source_commit
                or value.get("schema") != "borsuk-v36-prefix-freeze-terminal-v2"
                or value.get("claim_eligible") is not False
                or value.get("attempt_id") != expected_attempt_id
                or value.get("execution_authority_sha256")
                != expected_execution_sha256
                or canonical_json_bytes(value.get("inputs"))
                != canonical_json_bytes(expected_inputs)
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
            observed.append((status, value))
        if len(observed) > 1:
            raise ValueError("V36 prefix-screen terminal conflict")
        if observed:
            status, value = observed[0]
            return value if return_terminal else status
    return None


def run_v36_prefix_screen(
    plan: V36PrefixScreenPlan,
    *,
    ec2_client: Any,
    s3_client: Any,
    launch_nonce: str,
) -> str:
    """Run at most three bounded Spot attempts and preserve every terminal."""

    resume: dict[str, object] | None | object = None
    pointer_uri = f"{plan.output_prefix}checkpoints/runs/{plan.run_id}/latest.json"
    first_attempt = 0
    for ordinal in range(MAX_ATTEMPTS):
        launch = read_v36_controller_launch_if_present(s3_client, plan, ordinal)
        capacity = read_v36_controller_capacity(
            s3_client, plan, ordinal, launch
        )
        instance = read_v36_controller_instance_if_present(
            s3_client, plan, ordinal, launch
        )
        launched_resume = (
            _UNBOUND
            if launch is None
            else launch["execution_authority"]["resume"]
        )
        terminal = _read_attempt_status(
            s3_client,
            plan,
            ordinal,
            expected_instance_id=(
                None if instance is None else str(instance["instance_id"])
            ),
            expected_resume=launched_resume,
            return_terminal=True,
        )
        if launch is not None and not capacity and instance is None:
            instance = _reconcile_v36_controller_instance(
                ec2_client,
                s3_client,
                plan,
                launch,
                expected_instance_id=(
                    None
                    if not isinstance(terminal, dict)
                    else str(terminal["instance_id"])
                ),
            )
            if (
                instance is None
                and terminal is None
                and time.time() >= launch["controller_deadline_epoch_seconds"]
            ):
                raise RuntimeError("V36 prefix-screen unresolved launch deadline")
        if capacity:
            if terminal is not None or instance is not None:
                raise ValueError("V36 prefix-screen capacity terminal conflict")
            first_attempt = ordinal + 1
            continue
        if terminal is None:
            first_attempt = ordinal
            if launch is not None:
                resume = launch["execution_authority"]["resume"]
            elif ordinal > 0:
                head = read_v36_checkpoint_head_if_present(s3_client, pointer_uri)
                resume = (
                    None
                    if head is None
                    else v36_checkpoint_resume_binding(
                        plan, ordinal, head[0], head[1]
                    )
                )
            break
        if launch is None:
            raise ValueError("V36 prefix-screen terminal launch authority differs")
        if not isinstance(terminal, dict):
            raise ValueError("V36 prefix-screen terminal differs")
        _terminate_v36_instance(ec2_client, str(terminal["instance_id"]))
        status = terminal["status"]
        if status == "complete":
            bucket, key = _marker_key(plan, ordinal, "ATTEMPT_COMPLETE.json")
            return f"s3://{bucket}/{key}"
        if status == "screen-source-insufficient":
            raise RuntimeError("V36 prefix-screen source is insufficient")
        first_attempt = ordinal + 1
        resume = _UNBOUND
    for attempt_ordinal in range(first_attempt, MAX_ATTEMPTS):
        instance_id: str | None = None
        status: str | None = None
        controller_timed_out = False
        try:
            if resume is _UNBOUND:
                raise ValueError("V36 prefix-screen controller resume is unresolved")
            launch = load_or_create_v36_controller_launch(
                s3_client, plan, attempt_ordinal, launch_nonce, resume
            )
            launched_resume = launch["execution_authority"]["resume"]
            if launched_resume != resume:
                raise ValueError("V36 prefix-screen controller resume differs")
            spec = launch["launch_spec"]
            instance = read_v36_controller_instance_if_present(
                s3_client, plan, attempt_ordinal, launch
            )
            if instance is None:
                try:
                    response = ec2_client.run_instances(**spec)
                except Exception as error:
                    code = getattr(error, "response", {}).get("Error", {}).get("Code")
                    if code in _CAPACITY_ERRORS:
                        write_v36_controller_capacity(s3_client, plan, launch)
                        continue
                    raise
                response_instance = response["Instances"][0]
                instance_id = response_instance["InstanceId"]
                instance = write_v36_controller_instance(
                    s3_client,
                    plan,
                    launch,
                    instance_id,
                    response_instance["LaunchTime"],
                )
            else:
                instance_id = str(instance["instance_id"])
            if instance is None:
                raise ValueError("V36 prefix-screen controller instance missing")
            controller_deadline = instance["controller_deadline_epoch_seconds"]
            while True:
                try:
                    state = ec2_client.describe_instances(InstanceIds=[instance_id])[
                        "Reservations"
                    ][0]["Instances"][0]["State"]["Name"]
                except Exception as error:
                    code = getattr(error, "response", {}).get("Error", {}).get("Code")
                    if code != "InvalidInstanceID.NotFound":
                        raise
                    state = "terminated"
                if state in {"shutting-down", "terminated", "stopped", "stopping"}:
                    break
                status = _read_attempt_status(
                    s3_client,
                    plan,
                    attempt_ordinal,
                    expected_instance_id=instance_id,
                    expected_resume=resume,
                )
                if status is not None:
                    break
                if time.time() >= controller_deadline:
                    controller_timed_out = True
                    break
                time.sleep(15)
        finally:
            if instance_id is not None:
                _terminate_v36_instance(ec2_client, instance_id)
        if status is None:
            status = _read_attempt_status(
                s3_client,
                plan,
                attempt_ordinal,
                expected_instance_id=instance_id,
                expected_resume=resume,
            )
        if status is None and instance_id is not None:
            bucket, key = _marker_key(plan, attempt_ordinal, "ATTEMPT_FAILED.json")
            _put_immutable_s3_bytes(
                s3_client,
                f"s3://{bucket}/{key}",
                _controller_infrastructure_terminal_bytes(
                    plan, launch["execution_authority"], instance_id
                ),
            )
            status = _read_attempt_status(
                s3_client,
                plan,
                attempt_ordinal,
                expected_instance_id=instance_id,
                expected_resume=resume,
            )
        if status == "complete":
            bucket, key = _marker_key(plan, attempt_ordinal, "ATTEMPT_COMPLETE.json")
            return f"s3://{bucket}/{key}"
        if status == "screen-source-insufficient":
            raise RuntimeError("V36 prefix-screen source is insufficient")
        if status is None:
            raise RuntimeError(f"V36 prefix-screen attempt {attempt_ordinal} terminal missing")
        if controller_timed_out:
            if status not in {"infrastructure", "interrupted"}:
                raise ValueError("V36 prefix-screen controller timeout terminal differs")
            raise RuntimeError(
                f"V36 prefix-screen attempt {attempt_ordinal} controller deadline"
            )
        if attempt_ordinal + 1 < MAX_ATTEMPTS:
            head = read_v36_checkpoint_head_if_present(s3_client, pointer_uri)
            resume = (
                None
                if head is None
                else v36_checkpoint_resume_binding(
                    plan, attempt_ordinal + 1, head[0], head[1]
                )
            )
    raise RuntimeError("V36 prefix-screen three attempts exhausted")


def _v36_aws_clients() -> tuple[Any, Any]:
    """Create the only AWS clients admitted by the execution CLI."""

    import boto3

    session = boto3.Session(profile_name=PROFILE, region_name=REGION)
    return session.client("ec2"), session.client("s3")


def main(argv: list[str] | None = None) -> int:
    """Run one explicit controller-only mode."""

    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--dry-run", action="store_true")
    mode.add_argument("--execute-prefix-screen", action="store_true")
    mode.add_argument("--publish-checkpoints", action="store_true")
    mode.add_argument("--materialize-resume", action="store_true")
    mode.add_argument("--derive-prefix-inputs", action="store_true")
    parser.add_argument("--plan-json")
    parser.add_argument("--launch-nonce")
    parser.add_argument("--checkpoint-outbox")
    parser.add_argument("--producer-pid", type=int)
    parser.add_argument("--first-generation", type=int)
    parser.add_argument("--execution-authority")
    parser.add_argument("--resume-directory")
    parser.add_argument("--dataset-authority")
    parser.add_argument("--authority-output")
    parser.add_argument("--registry-output")
    arguments = parser.parse_args(argv)
    if arguments.derive_prefix_inputs:
        if (
            arguments.dataset_authority is None
            or arguments.authority_output is None
            or arguments.registry_output is None
            or arguments.plan_json is not None
            or arguments.launch_nonce is not None
            or arguments.checkpoint_outbox is not None
            or arguments.producer_pid is not None
            or arguments.first_generation is not None
            or arguments.execution_authority is not None
            or arguments.resume_directory is not None
        ):
            parser.error("V36 prefix-screen derivation arguments differ")
        write_v36_prefix_screen_inputs(
            pathlib.Path(arguments.dataset_authority),
            pathlib.Path(arguments.authority_output),
            pathlib.Path(arguments.registry_output),
        )
        return 0
    if arguments.publish_checkpoints:
        if (
            arguments.plan_json is not None
            or arguments.launch_nonce is not None
            or arguments.checkpoint_outbox is None
            or arguments.producer_pid is None
            or arguments.first_generation is None
            or arguments.execution_authority is not None
            or arguments.resume_directory is not None
            or arguments.dataset_authority is not None
            or arguments.authority_output is not None
            or arguments.registry_output is not None
        ):
            parser.error("V36 checkpoint sidecar arguments differ")
        watch_v36_checkpoint_outbox(
            pathlib.Path(arguments.checkpoint_outbox),
            arguments.producer_pid,
            V36AwsCliCheckpointTransport(),
            first_generation=arguments.first_generation,
        )
        return 0
    if arguments.materialize_resume:
        if (
            arguments.plan_json is not None
            or arguments.launch_nonce is not None
            or arguments.checkpoint_outbox is not None
            or arguments.producer_pid is not None
            or arguments.first_generation is not None
            or arguments.execution_authority is None
            or arguments.resume_directory is None
            or arguments.dataset_authority is not None
            or arguments.authority_output is not None
            or arguments.registry_output is not None
        ):
            parser.error("V36 checkpoint resume arguments differ")
        prepare_v36_checkpoint_resume(
            pathlib.Path(arguments.execution_authority),
            pathlib.Path(arguments.resume_directory),
            V36AwsCliCheckpointTransport(),
        )
        return 0
    if (
        arguments.plan_json is None
        or arguments.checkpoint_outbox is not None
        or arguments.producer_pid is not None
        or arguments.first_generation is not None
        or arguments.execution_authority is not None
        or arguments.resume_directory is not None
        or arguments.dataset_authority is not None
        or arguments.authority_output is not None
        or arguments.registry_output is not None
    ):
        parser.error("V36 prefix-screen dry-run arguments differ")
    try:
        raw = json.loads(arguments.plan_json)
        if type(raw) is not dict:
            raise ValueError("V36 prefix-screen plan JSON differs")
        plan = build_v36_prefix_screen_plan(**raw)
    except (TypeError, ValueError, json.JSONDecodeError) as error:
        parser.error(str(error))
    if arguments.dry_run:
        if arguments.launch_nonce is not None:
            parser.error("V36 prefix-screen dry-run arguments differ")
        sys.stdout.write(dry_run_v36_prefix_screen(plan).decode())
        return 0
    if (
        not arguments.execute_prefix_screen
        or type(arguments.launch_nonce) is not str
        or re.fullmatch(r"[0-9a-f]{32}", arguments.launch_nonce) is None
    ):
        parser.error("V36 prefix-screen execution arguments differ")
    ec2_client, s3_client = _v36_aws_clients()
    terminal_uri = run_v36_prefix_screen(
        plan,
        ec2_client=ec2_client,
        s3_client=s3_client,
        launch_nonce=arguments.launch_nonce,
    )
    sys.stdout.write(f"{terminal_uri}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
