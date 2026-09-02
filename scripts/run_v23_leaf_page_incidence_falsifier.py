#!/usr/bin/env python3
"""Fail-closed local launcher for the claim-ineligible V23 incidence falsifier."""

from __future__ import annotations

import argparse
import dataclasses
import errno
import hashlib
import json
import math
import os
import pathlib
import signal
import socket
import subprocess
import sys
import time
import traceback
from collections.abc import Sequence

PHASES = (
    "tree-training",
    "posting-construction",
    "development-evaluation",
    "holdout-binding",
    "holdout-evaluation",
)
_LOWER_HEX = frozenset("0123456789abcdef")


@dataclasses.dataclass(frozen=True)
class AuthenticatedInput:
    """One exact phase input with immutable content authority."""

    role: str
    source: pathlib.Path
    uri: str
    digest_algorithm: str
    digest: str
    encoded_bytes: int
    generation: str


@dataclasses.dataclass(frozen=True)
class AuthenticatedDirectory:
    """One manifest-backed corpus directory staged for a phase."""

    role: str
    source: pathlib.Path
    manifest_role: str
    staging_receipt_role: str


@dataclasses.dataclass(frozen=True)
class OfflinePhasePolicy:
    """Complete offline policy for one scientific phase process."""

    phase: str
    executable: pathlib.Path
    executable_sha256: str
    executable_bytes: int
    inputs: tuple[AuthenticatedInput, ...]
    scratch: pathlib.Path
    output: pathlib.Path
    parent_receipt_sha256: str | None
    directory_capabilities: tuple[AuthenticatedDirectory, ...] = ()
    phase_argv: tuple[str, ...] = ()
    host_network_namespace_inode: int | None = None


@dataclasses.dataclass(frozen=True)
class MonitorLimits:
    """Registered local resource stops."""

    rss_bytes: int = 2 << 30
    psi_immediate: float = 0.79
    psi_sustained: float = 0.50
    psi_samples: int = 3
    swap_delta_bytes: int = 256 * 1024 * 1024
    progress_seconds: int = 300
    wall_seconds: int = 7200


class AuthenticatedProgressMonitor:
    """Validate one phase's canonical completed-work progress chain."""

    def __init__(self, phase: str) -> None:
        if phase not in PHASES:
            raise ValueError("progress phase differs")
        self._phase = phase
        self._sequence: int | None = None
        self._completed_units: int | None = None
        self._total_units: int | None = None
        self._digest: str | None = None
        self._history = b""

    def observe(self, raw: bytes) -> tuple[int, int, str]:
        """Accept an atomically replaced canonical snapshot of the full chain."""

        if (
            not raw.endswith(b"\n")
            or not raw
            or (self._history and not raw.startswith(self._history))
            or len(raw) <= len(self._history)
        ):
            raise ValueError("progress history differs")
        records = raw[len(self._history) :].splitlines(keepends=True)
        if not records or any(record == b"\n" for record in records):
            raise ValueError("progress history differs")
        observed: tuple[int, int, str] | None = None
        for record in records:
            observed = self._observe_record(record)
        self._history = raw
        assert observed is not None
        return observed

    def _observe_record(self, raw: bytes) -> tuple[int, int, str]:
        try:
            if not raw.endswith(b"\n") or raw.count(b"\n") != 1:
                raise ValueError("progress canonical bytes differ")
            value = json.loads(raw)
        except (UnicodeDecodeError, ValueError, json.JSONDecodeError) as error:
            raise ValueError("progress encoding differs") from error
        expected_keys = {
            "completed_units",
            "last_object_digest",
            "phase",
            "previous_progress_sha256",
            "sequence",
            "total_units",
        }
        if type(value) is not dict or set(value) != expected_keys:  # noqa: E721
            raise ValueError("progress schema differs")
        if (
            type(value["phase"]) is not str
            or type(value["sequence"]) is not int
            or type(value["completed_units"]) is not int
            or type(value["total_units"]) is not int
            or type(value["last_object_digest"]) is not str
            or (
                value["previous_progress_sha256"] is not None
                and type(value["previous_progress_sha256"]) is not str
            )
        ):
            raise ValueError("progress concrete type differs")
        canonical = (
            json.dumps(value, separators=(",", ":"), sort_keys=True).encode() + b"\n"
        )
        if raw != canonical:
            raise ValueError("progress canonical bytes differ")
        sequence = value["sequence"]
        completed_units = value["completed_units"]
        total_units = value["total_units"]
        if (
            value["phase"] != self._phase
            or sequence < 0
            or completed_units < 0
            or total_units <= 0
            or completed_units > total_units
            or not _valid_sha256(value["last_object_digest"])
        ):
            raise ValueError("progress authority differs")
        if self._sequence is None:
            if (
                sequence != 0
                or completed_units != 0
                or value["previous_progress_sha256"] is not None
            ):
                raise ValueError("progress chain root differs")
            self._total_units = total_units
        elif (
            sequence != self._sequence + 1
            or completed_units <= self._completed_units
            or total_units != self._total_units
            or value["previous_progress_sha256"] != self._digest
        ):
            raise ValueError("progress chain differs")
        digest = hashlib.sha256(raw).hexdigest()
        self._sequence = sequence
        self._completed_units = completed_units
        self._digest = digest
        return sequence, completed_units, digest


def _valid_sha256(value: str | None) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and all(character in _LOWER_HEX for character in value)
    )


def _phase_roles(phase: str, *, preflight: bool) -> tuple[set[str], tuple[str, ...]]:
    def complete(
        roles: set[str], prefixes: tuple[str, ...]
    ) -> tuple[set[str], tuple[str, ...]]:
        if not preflight:
            roles.add("preflight-receipt")
        return roles, prefixes

    if phase not in PHASES:
        raise ValueError("unknown V23 incidence phase")
    manifest_role = (
        "construction-manifest" if phase == "tree-training" else "phase-manifest"
    )
    return complete(
        {manifest_role, "bulk-manifest", "staging-receipt"},
        ("bulk-inputs",),
    )


def _validate_input(mount: AuthenticatedInput) -> None:
    if (
        type(mount.role) is not str
        or not mount.role
        or not isinstance(mount.source, pathlib.Path)
        or not mount.source.is_absolute()
        or type(mount.uri) is not str
        or not mount.uri
        or type(mount.digest_algorithm) is not str
        or type(mount.digest) is not str
        or type(mount.generation) is not str
        or not mount.generation
    ):
        raise ValueError("phase inputs require an absolute source")
    if (
        mount.digest_algorithm not in {"sha256", "blake3"}
        or not _valid_sha256(mount.digest)
        or type(mount.encoded_bytes) is not int
        or mount.encoded_bytes <= 0
    ):
        raise ValueError("phase input digest authority differs")
    if ".." in mount.source.parts:
        raise ValueError("phase input path leaves its registered root")
    blake3_role = mount.role in {
        "incidence-tree",
        "incidence-postings-one",
        "incidence-postings-two",
        "development-latency",
        "holdout-latency",
    } or mount.role.startswith("page-body-")
    expected_algorithm = "blake3" if blake3_role else "sha256"
    if mount.digest_algorithm != expected_algorithm:
        raise ValueError("phase input digest algorithm differs")


def build_phase_argv(policy: OfflinePhasePolicy) -> tuple[str, ...]:
    """Build the exact corpus-size-independent Rust phase argv."""

    def one_mount(role: str) -> AuthenticatedInput:
        matches = tuple(mount for mount in policy.inputs if mount.role == role)
        if len(matches) != 1:
            raise ValueError(f"{role} mount authority differs")
        return matches[0]

    def authority_arguments(
        flag: str, prefix: str, mount: AuthenticatedInput
    ) -> tuple[str, ...]:
        return (
            flag,
            str(mount.source),
            f"--{prefix}-uri",
            mount.uri,
            f"--{prefix}-sha256",
            mount.digest,
            f"--{prefix}-bytes",
            str(mount.encoded_bytes),
            f"--{prefix}-generation",
            mount.generation,
        )

    preflight_receipts = tuple(
        mount for mount in policy.inputs if mount.role == "preflight-receipt"
    )
    if len(preflight_receipts) > 1:
        raise ValueError("preflight receipt mount authority differs")
    execute = bool(preflight_receipts)
    manifest_role = (
        "construction-manifest" if policy.phase == "tree-training" else "phase-manifest"
    )
    if len(policy.directory_capabilities) != 1:
        raise ValueError("bulk directory capability differs")
    capability = policy.directory_capabilities[0]
    arguments = (
        f"--{'execute' if execute else 'preflight'}-{policy.phase}",
        *authority_arguments("--manifest", "manifest", one_mount(manifest_role)),
        *authority_arguments(
            "--bulk-manifest", "bulk-manifest", one_mount("bulk-manifest")
        ),
        "--staging-directory",
        str(capability.source),
        *authority_arguments(
            "--staging-receipt", "staging-receipt", one_mount("staging-receipt")
        ),
    )
    if preflight_receipts:
        arguments += authority_arguments(
            "--preflight-receipt", "preflight-receipt", preflight_receipts[0]
        )
    arguments += (
        "--scratch",
        str(policy.scratch),
        "--output",
        str(policy.output / "receipt.json"),
        "--executable-sha256",
        policy.executable_sha256,
    )
    if sum(len(argument) + 1 for argument in arguments) >= 16_384:
        raise ValueError("phase argv exceeds the registered bound")
    return arguments


def validate_phase_inputs(policy: OfflinePhasePolicy) -> None:
    """Validate exact phase capabilities before namespace construction."""

    if policy.phase not in PHASES:
        raise ValueError("unknown V23 incidence phase")
    if not policy.executable.is_absolute():
        raise ValueError("offline phase executable must be absolute")
    if (
        not _valid_sha256(policy.executable_sha256)
        or type(policy.executable_bytes) is not int
        or policy.executable_bytes <= 0
    ):
        raise ValueError("offline phase executable identity differs")
    if not policy.scratch.is_absolute() or not policy.output.is_absolute():
        raise ValueError("scratch and output paths must be absolute")
    if policy.scratch == policy.output:
        raise ValueError("scratch and output must be disjoint")
    if (
        policy.scratch in policy.output.parents
        or policy.output in policy.scratch.parents
    ):
        raise ValueError("scratch and output must be disjoint")
    if not policy.inputs:
        raise ValueError("offline phase requires authenticated inputs")
    if policy.host_network_namespace_inode is not None and (
        type(policy.host_network_namespace_inode) is not int
        or policy.host_network_namespace_inode <= 0
    ):
        raise ValueError("host network namespace authority differs")
    if not policy.phase_argv:
        raise ValueError("phase execution gate is absent")
    gate = policy.phase_argv[0]
    preflight_gate = f"--preflight-{policy.phase}"
    execute_gate = f"--execute-{policy.phase}"
    if gate not in {preflight_gate, execute_gate}:
        raise ValueError("phase execution gate differs")
    preflight = gate == preflight_gate

    later_phase = policy.phase != "tree-training"
    if later_phase and not _valid_sha256(policy.parent_receipt_sha256):
        raise ValueError("later phase requires a parent receipt digest")
    if not later_phase and policy.parent_receipt_sha256 is not None:
        raise ValueError("tree training cannot have a parent receipt")

    seen_sources: set[pathlib.Path] = set()
    seen_roles: set[str] = set()
    for mount in policy.inputs:
        _validate_input(mount)
        if mount.source in seen_sources or mount.role in seen_roles:
            raise ValueError("duplicate offline phase authority")
        seen_sources.add(mount.source)
        seen_roles.add(mount.role)
    input_roles = {mount.role for mount in policy.inputs}
    for capability in policy.directory_capabilities:
        if (
            not capability.role
            or not capability.source.is_absolute()
            or ".." in capability.source.parts
        ):
            raise ValueError("offline phase directory capability path differs")
        if (
            capability.manifest_role not in input_roles
            or capability.staging_receipt_role not in input_roles
            or capability.manifest_role == capability.staging_receipt_role
        ):
            raise ValueError("offline phase directory authority role is absent")
        if capability.source in seen_sources or capability.role in seen_roles:
            raise ValueError("duplicate offline phase authority")
        seen_sources.add(capability.source)
        seen_roles.add(capability.role)
    if policy.executable in seen_sources:
        raise ValueError("duplicate offline phase authority")
    for capability_path in (policy.scratch, policy.output):
        if capability_path in seen_sources or capability_path == policy.executable:
            raise ValueError("offline phase writable and input capabilities overlap")

    fixed_roles, directory_roles = _phase_roles(policy.phase, preflight=preflight)
    actual_roles = {mount.role for mount in policy.inputs}
    if actual_roles != fixed_roles:
        raise ValueError(
            "preflight input capability differs"
            if preflight
            else "phase input capability differs"
        )
    actual_directory_roles = tuple(
        capability.role for capability in policy.directory_capabilities
    )
    if actual_directory_roles != directory_roles:
        raise ValueError("phase directory capability differs")
    expected_directory_bindings = {
        "bulk-inputs": ("bulk-manifest", "staging-receipt"),
    }
    for capability in policy.directory_capabilities:
        if (
            capability.manifest_role,
            capability.staging_receipt_role,
        ) != expected_directory_bindings[capability.role]:
            raise ValueError("phase directory authority differs")
    if policy.phase_argv != build_phase_argv(policy):
        raise ValueError("phase argv authority differs")


def _policy_value(policy: OfflinePhasePolicy) -> dict[str, object]:
    def mount_value(mount: AuthenticatedInput) -> dict[str, object]:
        return {
            "digest": mount.digest,
            "digest_algorithm": mount.digest_algorithm,
            "encoded_bytes": mount.encoded_bytes,
            "generation": mount.generation,
            "role": mount.role,
            "source": str(mount.source),
            "uri": mount.uri,
        }

    return {
        "directory_capabilities": [
            {
                "manifest_role": capability.manifest_role,
                "role": capability.role,
                "source": str(capability.source),
                "staging_receipt_role": capability.staging_receipt_role,
            }
            for capability in policy.directory_capabilities
        ],
        "executable": str(policy.executable),
        "executable_bytes": policy.executable_bytes,
        "executable_sha256": policy.executable_sha256,
        "inputs": [mount_value(mount) for mount in policy.inputs],
        "host_network_namespace_inode": policy.host_network_namespace_inode,
        "output": str(policy.output),
        "parent_receipt_sha256": policy.parent_receipt_sha256,
        "phase": policy.phase,
        "phase_argv": list(policy.phase_argv),
        "scratch": str(policy.scratch),
    }


def canonical_policy_bytes(policy: OfflinePhasePolicy) -> bytes:
    """Return one deterministic newline-terminated policy document."""

    validate_phase_inputs(policy)
    return (
        json.dumps(
            _policy_value(policy), separators=(",", ":"), sort_keys=True
        ).encode()
        + b"\n"
    )


def decode_policy_bytes(raw: bytes) -> OfflinePhasePolicy:
    """Decode exact canonical policy bytes and reject schema drift."""

    try:
        if not raw.endswith(b"\n") or raw.count(b"\n") != 1:
            raise ValueError("offline phase policy canonical bytes differ")
        value = json.loads(raw)
    except (UnicodeDecodeError, ValueError, json.JSONDecodeError) as error:
        raise ValueError("offline phase policy encoding differs") from error
    expected_keys = {
        "directory_capabilities",
        "executable",
        "executable_bytes",
        "executable_sha256",
        "host_network_namespace_inode",
        "inputs",
        "output",
        "parent_receipt_sha256",
        "phase",
        "phase_argv",
        "scratch",
    }
    if type(value) is not dict or set(value) != expected_keys:  # noqa: E721
        raise ValueError("offline phase policy schema differs")

    def decode_mount(item: object) -> AuthenticatedInput:
        if type(item) is not dict or set(item) != {  # noqa: E721
            "digest",
            "digest_algorithm",
            "encoded_bytes",
            "generation",
            "role",
            "source",
            "uri",
        }:
            raise ValueError("offline phase policy schema differs")
        if (
            type(item["digest"]) is not str
            or type(item["digest_algorithm"]) is not str
            or type(item["encoded_bytes"]) is not int
            or type(item["generation"]) is not str
            or type(item["role"]) is not str
            or type(item["source"]) is not str
            or type(item["uri"]) is not str
        ):
            raise ValueError("offline phase policy concrete type differs")
        return AuthenticatedInput(
            role=item["role"],
            source=pathlib.Path(item["source"]),
            uri=item["uri"],
            digest_algorithm=item["digest_algorithm"],
            digest=item["digest"],
            encoded_bytes=item["encoded_bytes"],
            generation=item["generation"],
        )

    def decode_directory(item: object) -> AuthenticatedDirectory:
        expected = {
            "manifest_role",
            "role",
            "source",
            "staging_receipt_role",
        }
        if type(item) is not dict or set(item) != expected:  # noqa: E721
            raise ValueError("offline phase policy schema differs")
        if (
            type(item["manifest_role"]) is not str
            or type(item["role"]) is not str
            or type(item["source"]) is not str
            or type(item["staging_receipt_role"]) is not str
        ):
            raise ValueError("offline phase policy concrete type differs")
        return AuthenticatedDirectory(
            role=item["role"],
            source=pathlib.Path(item["source"]),
            manifest_role=item["manifest_role"],
            staging_receipt_role=item["staging_receipt_role"],
        )

    if (
        type(value["phase"]) is not str
        or type(value["executable"]) is not str
        or type(value["executable_sha256"]) is not str
        or type(value["executable_bytes"]) is not int
        or type(value["scratch"]) is not str
        or type(value["output"]) is not str
        or type(value["directory_capabilities"]) is not list
        or type(value["inputs"]) is not list
        or type(value["phase_argv"]) is not list
        or not all(type(item) is str for item in value["phase_argv"])
        or (
            value["host_network_namespace_inode"] is not None
            and type(value["host_network_namespace_inode"]) is not int
        )
        or (
            value["parent_receipt_sha256"] is not None
            and type(value["parent_receipt_sha256"]) is not str
        )
    ):
        raise ValueError("offline phase policy concrete type differs")
    policy = OfflinePhasePolicy(
        phase=value["phase"],
        executable=pathlib.Path(value["executable"]),
        executable_sha256=value["executable_sha256"],
        executable_bytes=value["executable_bytes"],
        inputs=tuple(decode_mount(item) for item in value["inputs"]),
        scratch=pathlib.Path(value["scratch"]),
        output=pathlib.Path(value["output"]),
        parent_receipt_sha256=value["parent_receipt_sha256"],
        directory_capabilities=tuple(
            decode_directory(item) for item in value["directory_capabilities"]
        ),
        phase_argv=tuple(value["phase_argv"]),
        host_network_namespace_inode=value["host_network_namespace_inode"],
    )
    validate_phase_inputs(policy)
    if canonical_policy_bytes(policy) != raw:
        raise ValueError("offline phase policy canonical bytes differ")
    return policy


def write_canonical_policy_file(policy: OfflinePhasePolicy, path: pathlib.Path) -> str:
    """Create one exclusive mode-0600 policy file and return its SHA-256."""

    if not path.is_absolute() or path.name in {"", ".", ".."}:
        raise ValueError("offline phase policy path differs")
    raw = canonical_policy_bytes(policy)
    descriptor = os.open(
        path,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC,
        0o600,
    )
    try:
        offset = 0
        while offset < len(raw):
            written = os.write(descriptor, raw[offset:])
            if written <= 0:
                raise OSError("offline phase policy write made no progress")
            offset += written
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    return hashlib.sha256(raw).hexdigest()


def read_canonical_policy_file(
    path: pathlib.Path, expected_sha256: str
) -> OfflinePhasePolicy:
    """Authenticate and decode one regular canonical policy file."""

    if not path.is_absolute() or not _valid_sha256(expected_sha256):
        raise ValueError("offline phase policy digest authority differs")
    if path.is_symlink() or not path.is_file():
        raise ValueError("offline phase policy file authority differs")
    raw = path.read_bytes()
    if hashlib.sha256(raw).hexdigest() != expected_sha256:
        raise ValueError("offline phase policy digest authority differs")
    return decode_policy_bytes(raw)


def _digest_file(path: pathlib.Path, algorithm: str) -> str:
    if algorithm == "sha256":
        digest = hashlib.sha256()
    elif algorithm == "blake3":
        try:
            import blake3
        except ImportError as error:
            raise RuntimeError("the pinned blake3 module is required") from error
        digest = blake3.blake3()
    else:
        raise ValueError("offline phase digest algorithm differs")
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _authenticate_file(
    path: pathlib.Path,
    algorithm: str,
    expected_digest: str,
    expected_bytes: int,
) -> None:
    if path.is_symlink() or not path.is_file():
        raise ValueError("offline phase authenticated file authority differs")
    if path.stat().st_size != expected_bytes:
        raise ValueError("offline phase authenticated file length differs")
    if _digest_file(path, algorithm) != expected_digest:
        raise ValueError("offline phase authenticated file digest differs")


def _read_canonical_json_file(path: pathlib.Path, label: str) -> tuple[bytes, object]:
    raw = path.read_bytes()
    try:
        value = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError(f"{label} JSON differs") from error
    canonical = (
        json.dumps(value, separators=(",", ":"), sort_keys=True).encode() + b"\n"
    )
    if raw != canonical:
        raise ValueError(f"{label} canonical bytes differ")
    return raw, value


def _authenticate_staged_inventory(
    policy: OfflinePhasePolicy, capability: AuthenticatedDirectory
) -> frozenset[str]:
    inputs = {item.role: item for item in policy.inputs}
    manifest_input = inputs[capability.manifest_role]
    receipt_input = inputs[capability.staging_receipt_role]
    manifest_raw, manifest = _read_canonical_json_file(
        manifest_input.source, "bulk manifest"
    )
    _, receipt = _read_canonical_json_file(receipt_input.source, "staging receipt")
    if (
        type(manifest) is not dict
        or type(manifest.get("ordered_inputs")) is not list
        or not manifest["ordered_inputs"]
        or type(receipt) is not dict
        or set(receipt)
        != {"claim_eligible", "manifest_sha256", "ordered_objects", "schema"}
        or receipt["claim_eligible"] is not False
        or receipt["schema"] != "borsuk-v23-incidence-staging-receipt-v1"
        or receipt["manifest_sha256"] != hashlib.sha256(manifest_raw).hexdigest()
        or type(receipt["ordered_objects"]) is not list
    ):
        raise ValueError("staged inventory authority differs")
    expected_objects: list[dict[str, object]] = []
    seen_roles: set[str] = set()
    seen_uris: set[str] = set()
    for item in manifest["ordered_inputs"]:
        if type(item) is not dict or type(item.get("identity")) is not dict:
            raise ValueError("staged inventory manifest differs")
        identity = item["identity"]
        if set(identity) != {
            "digest",
            "digest_algorithm",
            "encoded_bytes",
            "generation",
            "role",
            "uri",
        }:
            raise ValueError("staged inventory manifest identity differs")
        role = identity["role"]
        uri = identity["uri"]
        if (
            type(role) is not str
            or not role
            or "/" in role
            or role in {".", ".."}
            or type(uri) is not str
            or not uri
            or type(identity["digest_algorithm"]) is not str
            or identity["digest_algorithm"] not in {"sha256", "blake3"}
            or not _valid_sha256(identity["digest"])
            or type(identity["encoded_bytes"]) is not int
            or identity["encoded_bytes"] <= 0
            or type(identity["generation"]) is not str
            or not identity["generation"]
            or role in seen_roles
            or uri in seen_uris
        ):
            raise ValueError("staged inventory manifest authority differs")
        seen_roles.add(role)
        seen_uris.add(uri)
        expected_objects.append({**identity, "relative_path": role})
    if receipt["ordered_objects"] != expected_objects:
        raise ValueError("staged inventory receipt differs")
    actual = tuple(sorted(path.name for path in capability.source.iterdir()))
    expected = tuple(sorted(str(item["relative_path"]) for item in expected_objects))
    if actual != expected:
        raise ValueError("staged inventory differs")
    for item in expected_objects:
        path = capability.source / str(item["relative_path"])
        _authenticate_file(
            path,
            item["digest_algorithm"],
            item["digest"],
            item["encoded_bytes"],
        )
    return frozenset(seen_roles)


def authenticate_policy_files(policy: OfflinePhasePolicy) -> frozenset[str]:
    """Rehash every executable and phase input before offline execution."""

    validate_phase_inputs(policy)
    _authenticate_file(
        policy.executable,
        "sha256",
        policy.executable_sha256,
        policy.executable_bytes,
    )
    for mount in policy.inputs:
        _authenticate_file(
            mount.source,
            mount.digest_algorithm,
            mount.digest,
            mount.encoded_bytes,
        )
    staged_roles: set[str] = set()
    for capability in policy.directory_capabilities:
        if capability.source.is_symlink() or not capability.source.is_dir():
            raise ValueError("offline phase directory capability authority differs")
        staged_roles.update(_authenticate_staged_inventory(policy, capability))
    return frozenset(staged_roles)


def build_offline_command(policy_path: pathlib.Path, policy_sha256: str) -> list[str]:
    """Build the bounded offline-child command without invoking a shell."""

    if not policy_path.is_absolute() or not _valid_sha256(policy_sha256):
        raise ValueError("offline phase policy path or digest differs")
    return [
        "unshare",
        "--net",
        "--pid",
        "--fork",
        "--kill-child=SIGKILL",
        sys.executable,
        str(pathlib.Path(__file__).resolve()),
        "--enter-offline-policy",
        str(policy_path),
        "--policy-sha256",
        policy_sha256,
    ]


def _network_namespace_inode() -> int:
    rendered = os.readlink("/proc/self/ns/net")
    if not rendered.startswith("net:[") or not rendered.endswith("]"):
        raise RuntimeError("network namespace identity differs")
    return int(rendered[5:-1])


def _bulk_role_is_allowed(phase: str, role: str) -> bool:
    if phase == "tree-training":
        suffix = role.removeprefix("training-shard-")
        return role == "dataset-meta" or (
            len(suffix) == 4 and suffix.isdigit() and int(suffix) < 58
        )
    if phase in {"posting-construction", "holdout-binding"}:
        fixed = (
            {"parent-receipt", "incidence-tree", "page-roster"}
            if phase == "posting-construction"
            else {
                "parent-receipt",
                "development-result",
                "page-roster",
                "neighbors-parquet",
            }
        )
        suffix = role.removeprefix("page-body-")
        return role in fixed or (
            len(suffix) == 5 and suffix.isdigit() and int(suffix) < 28_282
        )
    allowed = {
        "development-evaluation": {
            "parent-receipt",
            "incidence-tree",
            "incidence-postings-one",
            "incidence-postings-two",
            "d2-report",
            "query-parquet",
        },
        "holdout-evaluation": {
            "parent-receipt",
            "development-result",
            "development-latency",
            "incidence-tree",
            "incidence-postings-one",
            "incidence-postings-two",
            "query-parquet",
            "holdout-truth",
        },
    }
    return role in allowed.get(phase, set())


def _forbidden_roles_absent(
    policy: OfflinePhasePolicy, staged_roles: frozenset[str]
) -> bool:
    if not policy.phase_argv:
        return False
    fixed_roles, directory_roles = _phase_roles(
        policy.phase,
        preflight=policy.phase_argv[0].startswith("--preflight-"),
    )
    input_roles = [item.role for item in policy.inputs]
    actual_directory_roles = [item.role for item in policy.directory_capabilities]
    return (
        len(input_roles) == len(fixed_roles)
        and set(input_roles) == fixed_roles
        and len(actual_directory_roles) == len(directory_roles)
        and set(actual_directory_roles) == set(directory_roles)
        and all(_bulk_role_is_allowed(policy.phase, role) for role in staged_roles)
    )


def _network_canary_denied() -> bool:
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as connection:
            connection.settimeout(0.05)
            connection.connect(("198.51.100.1", 9))
    except TimeoutError:
        return False
    except OSError as error:
        return error.errno in {
            errno.EACCES,
            errno.EAFNOSUPPORT,
            errno.EHOSTUNREACH,
            errno.ENETUNREACH,
            errno.EPERM,
        }
    return False


def _offline_startup_probes(
    policy: OfflinePhasePolicy, staged_roles: frozenset[str]
) -> dict[str, object]:
    current_inode = _network_namespace_inode()
    namespace_changed = (
        policy.host_network_namespace_inode is not None
        and current_inode != policy.host_network_namespace_inode
    )
    network_canary_denied = _network_canary_denied()

    allowlisted_inputs_opened = True
    for mount in policy.inputs:
        try:
            descriptor = os.open(mount.source, os.O_RDONLY | os.O_CLOEXEC)
        except OSError:
            allowlisted_inputs_opened = False
            break
        else:
            os.close(descriptor)
    for capability in policy.directory_capabilities:
        try:
            descriptor = os.open(
                capability.source,
                os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC,
            )
        except OSError:
            allowlisted_inputs_opened = False
            break
        else:
            os.close(descriptor)

    probe_path = policy.output / ".capability-probe"
    output_writable = False
    try:
        descriptor = os.open(
            probe_path,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC,
            0o600,
        )
        os.write(descriptor, b"probe\n")
        os.close(descriptor)
        probe_path.unlink()
        output_writable = True
    except OSError:
        output_writable = False

    probes = {
        "allowlisted_inputs_opened": allowlisted_inputs_opened,
        "forbidden_roles_absent": _forbidden_roles_absent(policy, staged_roles),
        "network_canary_denied": network_canary_denied,
        "network_namespace_changed": namespace_changed,
        "network_namespace_inode": current_inode,
        "output_writable": output_writable,
    }
    required = (
        "allowlisted_inputs_opened",
        "forbidden_roles_absent",
        "network_canary_denied",
        "network_namespace_changed",
        "output_writable",
    )
    failed = [key for key in required if not probes[key]]
    if failed:
        raise RuntimeError("offline phase capability probe failed: " + ",".join(failed))
    return probes


def enter_offline_phase(policy: OfflinePhasePolicy) -> None:
    """Reauthenticate one staged phase, prove it is offline, and exec it."""

    validate_phase_inputs(policy)
    staged_roles = authenticate_policy_files(policy)
    if policy.host_network_namespace_inode is None:
        raise ValueError("host network namespace authority is absent")
    for path in (policy.executable, policy.scratch, policy.output):
        if path.is_symlink() or not path.exists():
            raise ValueError("offline phase host path authority differs")
    if not policy.scratch.is_dir() or not policy.output.is_dir():
        raise ValueError("offline phase scratch/output authority differs")
    if any(policy.output.iterdir()):
        raise ValueError("offline phase output must begin empty")

    probes = _offline_startup_probes(policy, staged_roles)
    environment = {
        "BORSUK_V23_INCIDENCE_OFFLINE_PROBES": json.dumps(
            probes, separators=(",", ":"), sort_keys=True
        ),
        "LANG": "C",
        "LC_ALL": "C",
    }
    os.execve(
        policy.executable,
        (str(policy.executable), *policy.phase_argv),
        environment,
    )


def classify_sample(
    *,
    limits: MonitorLimits,
    rss_bytes: int,
    psi_full_avg10: float,
    consecutive_psi_samples: int,
    swap_delta_bytes: int,
    progress_age_seconds: float,
    wall_seconds: float,
) -> str | None:
    """Classify one sample using the preregistered equality rules."""

    if rss_bytes >= limits.rss_bytes:
        return "rss-cap"
    if psi_full_avg10 >= limits.psi_immediate:
        return "psi-immediate"
    if (
        psi_full_avg10 >= limits.psi_sustained
        and consecutive_psi_samples >= limits.psi_samples
    ):
        return "psi-sustained"
    if swap_delta_bytes > limits.swap_delta_bytes:
        return "swap-delta"
    if progress_age_seconds >= limits.progress_seconds:
        return "progress-gap"
    if wall_seconds >= limits.wall_seconds:
        return "wall-cap"
    return None


def cleanup_known_files(root: pathlib.Path, known_names: Sequence[str]) -> None:
    """Unlink exactly registered basenames, rejecting all unexpected entries."""

    if not root.is_dir():
        raise ValueError("cleanup root is not a directory")
    names = tuple(known_names)
    if len(names) != len(set(names)) or any(
        not name or pathlib.PurePath(name).name != name for name in names
    ):
        raise ValueError("cleanup names differ")
    actual = {entry.name for entry in root.iterdir()}
    expected = set(names)
    if not actual.issubset(expected):
        raise ValueError("unexpected scratch entry")
    for name in names:
        path = root / name
        if path.exists():
            if not path.is_file() or path.is_symlink():
                raise ValueError("cleanup target is not a regular file")
            path.unlink()
    root.rmdir()


def monitor_process_group(
    pid: int,
    limits: MonitorLimits,
    *,
    sample_interval_seconds: float = 5.0,
    progress_path: pathlib.Path | None = None,
    progress_phase: str | None = None,
    term_grace_seconds: float = 15.0,
) -> tuple[int, str | None]:
    """Monitor and stop one original process group without restart."""

    if pid <= 1:
        raise ValueError("invalid process group leader")
    if (progress_path is None) != (progress_phase is None):
        raise ValueError("progress path and phase must be supplied together")
    started = time.monotonic()
    try:
        initial_swap = _swap_used_bytes()
        sustained = 0
        last_progress = started
        progress_monitor = (
            None
            if progress_phase is None
            else AuthenticatedProgressMonitor(progress_phase)
        )
        progress_file_digest: str | None = None
        while True:
            completed, status = os.waitpid(pid, os.WNOHANG)
            if completed == pid:
                return os.waitstatus_to_exitcode(status), None
            psi = _memory_psi_full_avg10()
            sustained = sustained + 1 if psi >= limits.psi_sustained else 0
            if progress_path is not None and progress_path.exists():
                try:
                    if progress_path.is_symlink() or not progress_path.is_file():
                        raise ValueError("progress file authority differs")
                    raw_progress = progress_path.read_bytes()
                    observed_digest = hashlib.sha256(raw_progress).hexdigest()
                    if observed_digest != progress_file_digest:
                        assert progress_monitor is not None
                        progress_monitor.observe(raw_progress)
                        progress_file_digest = observed_digest
                        last_progress = time.monotonic()
                except (OSError, ValueError):
                    status = _terminate_process_group(pid, term_grace_seconds)
                    return os.waitstatus_to_exitcode(status), "progress-authority"
            stop_reason = classify_sample(
                limits=limits,
                rss_bytes=_process_group_rss_bytes(pid),
                psi_full_avg10=psi,
                consecutive_psi_samples=sustained,
                swap_delta_bytes=max(0, _swap_used_bytes() - initial_swap),
                progress_age_seconds=time.monotonic() - last_progress,
                wall_seconds=time.monotonic() - started,
            )
            if stop_reason is not None:
                status = _terminate_process_group(pid, term_grace_seconds)
                return os.waitstatus_to_exitcode(status), stop_reason
            time.sleep(sample_interval_seconds)
    except BaseException:
        try:
            _terminate_process_group(pid, term_grace_seconds)
        except ChildProcessError:
            pass
        raise


def _terminate_process_group(pid: int, grace_seconds: float) -> int:
    if grace_seconds < 0:
        raise ValueError("termination grace differs")

    def group_exists() -> bool:
        try:
            os.killpg(pid, 0)
        except ProcessLookupError:
            return False
        return True

    leader_status: int | None = None
    try:
        os.killpg(pid, signal.SIGTERM)
    except ProcessLookupError:
        pass
    deadline = time.monotonic() + grace_seconds
    while True:
        if leader_status is None:
            completed, status = os.waitpid(pid, os.WNOHANG)
            if completed == pid:
                leader_status = status
        if leader_status is not None and not group_exists():
            return leader_status
        if time.monotonic() >= deadline:
            break
        time.sleep(min(0.05, max(0.001, grace_seconds / 10)))
    try:
        os.killpg(pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    if leader_status is None:
        _, leader_status = os.waitpid(pid, 0)
    clearance_deadline = time.monotonic() + 1.0
    while group_exists() and time.monotonic() < clearance_deadline:
        time.sleep(0.01)
    if group_exists():
        raise RuntimeError("offline phase process group did not terminate")
    return leader_status


def _process_group_rss_bytes(pgid: int) -> int:
    total_pages = 0
    for entry in pathlib.Path("/proc").iterdir():
        if not entry.name.isdigit():
            continue
        try:
            if os.getpgid(int(entry.name)) != pgid:
                continue
            fields = (entry / "statm").read_text(encoding="ascii").split()
            total_pages += int(fields[1])
        except (FileNotFoundError, ProcessLookupError, PermissionError, ValueError):
            continue
    return total_pages * os.sysconf("SC_PAGE_SIZE")


def _memory_psi_full_avg10(
    path: pathlib.Path = pathlib.Path("/proc/pressure/memory"),
) -> float:
    for line in path.read_text(encoding="ascii").splitlines():
        if line.startswith("full "):
            try:
                fields = dict(field.split("=", 1) for field in line.split()[1:])
                value = float(fields["avg10"])
            except (KeyError, ValueError) as error:
                raise RuntimeError("memory PSI full avg10 sample differs") from error
            if not math.isfinite(value) or value < 0.0:
                raise RuntimeError("memory PSI full avg10 sample differs")
            return value
    raise RuntimeError("memory PSI full avg10 sample is absent")


def _swap_used_bytes() -> int:
    values: dict[str, int] = {}
    for line in pathlib.Path("/proc/meminfo").read_text(encoding="ascii").splitlines():
        key, value, *_ = line.split()
        if key in {"SwapTotal:", "SwapFree:"}:
            values[key] = int(value) * 1024
    return values["SwapTotal:"] - values["SwapFree:"]


def run_phase(policy: OfflinePhasePolicy, limits: MonitorLimits | None = None) -> int:
    """Launch and monitor one original offline scientific process group."""

    bound_policy = dataclasses.replace(
        policy,
        host_network_namespace_inode=_network_namespace_inode(),
    )
    policy_path = policy.scratch.parent / f".{policy.scratch.name}.policy.json"
    policy_sha256 = write_canonical_policy_file(bound_policy, policy_path)
    command = build_offline_command(policy_path, policy_sha256)
    process_started = False
    try:
        authenticate_policy_files(bound_policy)
        process = subprocess.Popen(  # noqa: S603
            command,
            start_new_session=True,
            env={
                "HOME": "/nonexistent",
                "LANG": "C",
                "LC_ALL": "C",
                "PATH": "/usr/bin:/bin",
                "TMPDIR": str(policy.scratch),
            },
        )
        process_started = True
        status, stop_reason = monitor_process_group(
            process.pid,
            limits or MonitorLimits(),
            progress_path=policy.output / "progress.json",
            progress_phase=policy.phase,
        )
        process.returncode = status
    finally:
        try:
            if process_started:
                authenticate_policy_files(bound_policy)
        finally:
            if policy_path.exists():
                if policy_path.is_symlink() or not policy_path.is_file():
                    raise ValueError("offline phase policy cleanup authority differs")
                policy_path.unlink()
    if stop_reason is not None:
        raise RuntimeError(f"V23 incidence phase stopped: {stop_reason}")
    return status


def parse_args(arguments: Sequence[str] | None = None) -> argparse.Namespace:
    """Parse one explicit public phase gate or the private offline entry gate."""

    parser = argparse.ArgumentParser(allow_abbrev=False)
    gates = parser.add_mutually_exclusive_group(required=True)
    for phase in PHASES:
        gates.add_argument(f"--preflight-{phase}", action="store_true")
        gates.add_argument(f"--execute-{phase}", action="store_true")
    gates.add_argument("--enter-offline-policy", type=pathlib.Path)
    parser.add_argument("--policy-sha256")
    parser.add_argument("--policy", type=pathlib.Path)
    parsed = parser.parse_args(arguments)
    if parsed.enter_offline_policy is not None:
        if parsed.policy_sha256 is None or parsed.policy is not None:
            parser.error("private offline entry requires only policy path and digest")
    elif parsed.policy_sha256 is not None:
        parser.error("public phase execution cannot accept a private policy digest")
    if parsed.enter_offline_policy is None and parsed.policy is None:
        parser.error("public phase execution requires --policy")
    return parsed


def main(arguments: Sequence[str] | None = None) -> int:
    """Run one explicit launcher mode."""

    parsed = parse_args(arguments)
    if parsed.enter_offline_policy is not None:
        enter_offline_phase(
            read_canonical_policy_file(
                parsed.enter_offline_policy, parsed.policy_sha256
            )
        )
        raise RuntimeError("offline executable returned unexpectedly")
    raw = parsed.policy.read_bytes()
    policy = decode_policy_bytes(raw)
    selected_gate, selected_phase = next(
        (f"--{mode}-{phase}", phase)
        for phase in PHASES
        for mode in ("preflight", "execute")
        if getattr(parsed, f"{mode}_{phase.replace('-', '_')}")
    )
    if policy.phase != selected_phase or policy.phase_argv[0] != selected_gate:
        raise ValueError("policy mode or phase differs from explicit execution gate")
    return run_phase(policy)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:  # noqa: BLE001
        traceback.print_exc()
        raise SystemExit(1) from error
