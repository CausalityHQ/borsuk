#!/usr/bin/env python3
"""Fail-closed local launcher for the claim-ineligible V23 incidence falsifier."""

from __future__ import annotations

import argparse
import ctypes
import dataclasses
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
class SandboxMount:
    """One exact host path exposed at one exact sandbox path."""

    role: str
    source: pathlib.Path
    target: pathlib.PurePosixPath
    read_only: bool
    uri: str
    digest_algorithm: str
    digest: str
    encoded_bytes: int
    generation: str


@dataclasses.dataclass(frozen=True)
class SandboxDirectoryCapability:
    """One manifest-backed read-only corpus directory exposed to a phase."""

    role: str
    source: pathlib.Path
    target: pathlib.PurePosixPath
    manifest_role: str
    staging_receipt_role: str
    read_only: bool


@dataclasses.dataclass(frozen=True)
class SandboxPolicy:
    """Complete capability policy for one scientific phase process."""

    phase: str
    executable: pathlib.Path
    executable_sha256: str
    executable_bytes: int
    runtime_mounts: tuple[SandboxMount, ...]
    inputs: tuple[SandboxMount, ...]
    scratch: pathlib.Path
    output: pathlib.Path
    parent_receipt_sha256: str | None
    directory_capabilities: tuple[SandboxDirectoryCapability, ...] = ()
    phase_argv: tuple[str, ...] = ()
    host_network_namespace_inode: int | None = None
    host_canaries: tuple[pathlib.PurePosixPath, ...] = (
        pathlib.PurePosixPath("/etc/hostname"),
        pathlib.PurePosixPath("/root/.aws/credentials"),
    )


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


def _validate_mount(mount: SandboxMount, *, runtime: bool) -> None:
    if (
        not mount.role
        or not mount.source.is_absolute()
        or not mount.target.is_absolute()
        or not mount.uri
    ):
        raise ValueError("sandbox mounts require an absolute source and target")
    if not mount.read_only:
        raise ValueError("sandbox inputs and runtime mounts must be read-only")
    if (
        mount.digest_algorithm not in {"sha256", "blake3"}
        or not _valid_sha256(mount.digest)
        or type(mount.encoded_bytes) is not int
        or mount.encoded_bytes <= 0
        or not mount.generation
    ):
        raise ValueError("sandbox mount digest authority differs")
    rendered_target = f"{mount.target.as_posix().rstrip('/')}/"
    if ".." in mount.target.parts:
        raise ValueError("sandbox mount target leaves its registered root")
    if runtime:
        allowed_target = (
            rendered_target.startswith(("/lib/", "/lib64/", "/usr/lib/", "/usr/lib64/"))
            or rendered_target == "/etc/ld.so.cache/"
        )
        if (
            mount.digest_algorithm != "sha256"
            or not (
                mount.role == "runtime-loader"
                or mount.role.startswith("runtime-library-")
            )
            or not allowed_target
        ):
            raise ValueError("runtime mount authority differs")
        if rendered_target.startswith(("/inputs/", "/scratch/", "/output/", "/phase/")):
            raise ValueError("runtime mount target overlaps a phase capability")
    elif not rendered_target.startswith("/inputs/"):
        raise ValueError("sandbox mount target leaves its registered root")
    if not runtime:
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


def build_phase_argv(policy: SandboxPolicy) -> tuple[str, ...]:
    """Build the exact corpus-size-independent Rust phase argv."""

    def one_mount(role: str) -> SandboxMount:
        matches = tuple(mount for mount in policy.inputs if mount.role == role)
        if len(matches) != 1:
            raise ValueError(f"{role} mount authority differs")
        return matches[0]

    def authority_arguments(
        flag: str, prefix: str, mount: SandboxMount
    ) -> tuple[str, ...]:
        return (
            flag,
            mount.target.as_posix(),
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
        capability.target.as_posix(),
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
        "/scratch",
        "--output",
        "/output/receipt.json",
        "--executable-sha256",
        policy.executable_sha256,
    )
    if sum(len(argument) + 1 for argument in arguments) >= 16_384:
        raise ValueError("phase argv exceeds the registered bound")
    return arguments


def validate_phase_inputs(policy: SandboxPolicy) -> None:
    """Validate exact phase capabilities before namespace construction."""

    if policy.phase not in PHASES:
        raise ValueError("unknown V23 incidence phase")
    if not policy.executable.is_absolute():
        raise ValueError("sandbox executable must be absolute")
    if (
        not _valid_sha256(policy.executable_sha256)
        or type(policy.executable_bytes) is not int
        or policy.executable_bytes <= 0
    ):
        raise ValueError("sandbox executable identity differs")
    if not policy.scratch.is_absolute() or not policy.output.is_absolute():
        raise ValueError("scratch and output paths must be absolute")
    if policy.scratch == policy.output:
        raise ValueError("scratch and output must be disjoint")
    if (
        policy.scratch in policy.output.parents
        or policy.output in policy.scratch.parents
    ):
        raise ValueError("scratch and output must be disjoint")
    if not policy.runtime_mounts or not policy.inputs:
        raise ValueError("sandbox requires runtime and phase inputs")
    if policy.host_network_namespace_inode is not None and (
        type(policy.host_network_namespace_inode) is not int
        or policy.host_network_namespace_inode <= 0
    ):
        raise ValueError("host network namespace authority differs")
    if not policy.host_canaries or any(
        not path.is_absolute() or ".." in path.parts for path in policy.host_canaries
    ):
        raise ValueError("host canary authority differs")
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
    seen_targets: set[pathlib.PurePosixPath] = set()
    seen_roles: set[str] = set()
    for mount in policy.runtime_mounts:
        _validate_mount(mount, runtime=True)
        if (
            mount.source in seen_sources
            or mount.target in seen_targets
            or mount.role in seen_roles
        ):
            raise ValueError("duplicate sandbox mount authority")
        seen_sources.add(mount.source)
        seen_targets.add(mount.target)
        seen_roles.add(mount.role)
    for mount in policy.inputs:
        _validate_mount(mount, runtime=False)
        if (
            mount.source in seen_sources
            or mount.target in seen_targets
            or mount.role in seen_roles
        ):
            raise ValueError("duplicate sandbox mount authority")
        seen_sources.add(mount.source)
        seen_targets.add(mount.target)
        seen_roles.add(mount.role)
    input_roles = {mount.role for mount in policy.inputs}
    for capability in policy.directory_capabilities:
        if (
            not capability.role
            or not capability.source.is_absolute()
            or not capability.target.is_absolute()
            or ".." in capability.target.parts
            or not capability.target.as_posix().startswith("/inputs/")
        ):
            raise ValueError("sandbox directory capability path differs")
        if not capability.read_only:
            raise ValueError("sandbox directory capability must be read-only")
        if (
            capability.manifest_role not in input_roles
            or capability.staging_receipt_role not in input_roles
            or capability.manifest_role == capability.staging_receipt_role
        ):
            raise ValueError("sandbox directory authority role is absent")
        if (
            capability.source in seen_sources
            or capability.target in seen_targets
            or capability.role in seen_roles
        ):
            raise ValueError("duplicate sandbox mount authority")
        seen_sources.add(capability.source)
        seen_targets.add(capability.target)
        seen_roles.add(capability.role)
    if policy.executable in seen_sources:
        raise ValueError("duplicate sandbox mount authority")
    for capability_path in (policy.scratch, policy.output):
        if capability_path in seen_sources or capability_path == policy.executable:
            raise ValueError("sandbox writable and read-only capabilities overlap")

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


def _policy_value(policy: SandboxPolicy) -> dict[str, object]:
    def mount_value(mount: SandboxMount) -> dict[str, object]:
        return {
            "digest": mount.digest,
            "digest_algorithm": mount.digest_algorithm,
            "encoded_bytes": mount.encoded_bytes,
            "generation": mount.generation,
            "read_only": mount.read_only,
            "role": mount.role,
            "source": str(mount.source),
            "target": mount.target.as_posix(),
            "uri": mount.uri,
        }

    return {
        "directory_capabilities": [
            {
                "manifest_role": capability.manifest_role,
                "read_only": capability.read_only,
                "role": capability.role,
                "source": str(capability.source),
                "staging_receipt_role": capability.staging_receipt_role,
                "target": capability.target.as_posix(),
            }
            for capability in policy.directory_capabilities
        ],
        "executable": str(policy.executable),
        "executable_bytes": policy.executable_bytes,
        "executable_sha256": policy.executable_sha256,
        "inputs": [mount_value(mount) for mount in policy.inputs],
        "host_canaries": [path.as_posix() for path in policy.host_canaries],
        "host_network_namespace_inode": policy.host_network_namespace_inode,
        "output": str(policy.output),
        "parent_receipt_sha256": policy.parent_receipt_sha256,
        "phase": policy.phase,
        "phase_argv": list(policy.phase_argv),
        "runtime_mounts": [mount_value(mount) for mount in policy.runtime_mounts],
        "scratch": str(policy.scratch),
    }


def canonical_policy_bytes(policy: SandboxPolicy) -> bytes:
    """Return one deterministic newline-terminated policy document."""

    validate_phase_inputs(policy)
    return (
        json.dumps(
            _policy_value(policy), separators=(",", ":"), sort_keys=True
        ).encode()
        + b"\n"
    )


def decode_policy_bytes(raw: bytes) -> SandboxPolicy:
    """Decode exact canonical policy bytes and reject schema drift."""

    try:
        if not raw.endswith(b"\n") or raw.count(b"\n") != 1:
            raise ValueError("sandbox policy canonical bytes differ")
        value = json.loads(raw)
    except (UnicodeDecodeError, ValueError, json.JSONDecodeError) as error:
        raise ValueError("sandbox policy encoding differs") from error
    expected_keys = {
        "directory_capabilities",
        "executable",
        "executable_bytes",
        "executable_sha256",
        "host_canaries",
        "host_network_namespace_inode",
        "inputs",
        "output",
        "parent_receipt_sha256",
        "phase",
        "phase_argv",
        "runtime_mounts",
        "scratch",
    }
    if type(value) is not dict or set(value) != expected_keys:  # noqa: E721
        raise ValueError("sandbox policy schema differs")

    def decode_mount(item: object) -> SandboxMount:
        if type(item) is not dict or set(item) != {  # noqa: E721
            "digest",
            "digest_algorithm",
            "encoded_bytes",
            "generation",
            "read_only",
            "role",
            "source",
            "target",
            "uri",
        }:
            raise ValueError("sandbox policy schema differs")
        if (
            type(item["read_only"]) is not bool
            or type(item["digest"]) is not str
            or type(item["digest_algorithm"]) is not str
            or type(item["encoded_bytes"]) is not int
            or type(item["generation"]) is not str
            or type(item["role"]) is not str
            or type(item["source"]) is not str
            or type(item["target"]) is not str
            or type(item["uri"]) is not str
        ):
            raise ValueError("sandbox policy concrete type differs")
        return SandboxMount(
            role=item["role"],
            source=pathlib.Path(item["source"]),
            target=pathlib.PurePosixPath(item["target"]),
            read_only=item["read_only"],
            uri=item["uri"],
            digest_algorithm=item["digest_algorithm"],
            digest=item["digest"],
            encoded_bytes=item["encoded_bytes"],
            generation=item["generation"],
        )

    def decode_directory(item: object) -> SandboxDirectoryCapability:
        expected = {
            "manifest_role",
            "read_only",
            "role",
            "source",
            "staging_receipt_role",
            "target",
        }
        if type(item) is not dict or set(item) != expected:  # noqa: E721
            raise ValueError("sandbox policy schema differs")
        if (
            type(item["manifest_role"]) is not str
            or type(item["read_only"]) is not bool
            or type(item["role"]) is not str
            or type(item["source"]) is not str
            or type(item["staging_receipt_role"]) is not str
            or type(item["target"]) is not str
        ):
            raise ValueError("sandbox policy concrete type differs")
        return SandboxDirectoryCapability(
            role=item["role"],
            source=pathlib.Path(item["source"]),
            target=pathlib.PurePosixPath(item["target"]),
            manifest_role=item["manifest_role"],
            staging_receipt_role=item["staging_receipt_role"],
            read_only=item["read_only"],
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
        or type(value["runtime_mounts"]) is not list
        or type(value["phase_argv"]) is not list
        or not all(type(item) is str for item in value["phase_argv"])
        or type(value["host_canaries"]) is not list
        or not all(type(item) is str for item in value["host_canaries"])
        or (
            value["host_network_namespace_inode"] is not None
            and type(value["host_network_namespace_inode"]) is not int
        )
        or (
            value["parent_receipt_sha256"] is not None
            and type(value["parent_receipt_sha256"]) is not str
        )
    ):
        raise ValueError("sandbox policy concrete type differs")
    policy = SandboxPolicy(
        phase=value["phase"],
        executable=pathlib.Path(value["executable"]),
        executable_sha256=value["executable_sha256"],
        executable_bytes=value["executable_bytes"],
        runtime_mounts=tuple(decode_mount(item) for item in value["runtime_mounts"]),
        inputs=tuple(decode_mount(item) for item in value["inputs"]),
        scratch=pathlib.Path(value["scratch"]),
        output=pathlib.Path(value["output"]),
        parent_receipt_sha256=value["parent_receipt_sha256"],
        directory_capabilities=tuple(
            decode_directory(item) for item in value["directory_capabilities"]
        ),
        phase_argv=tuple(value["phase_argv"]),
        host_network_namespace_inode=value["host_network_namespace_inode"],
        host_canaries=tuple(
            pathlib.PurePosixPath(item) for item in value["host_canaries"]
        ),
    )
    validate_phase_inputs(policy)
    if canonical_policy_bytes(policy) != raw:
        raise ValueError("sandbox policy canonical bytes differ")
    return policy


def write_canonical_policy_file(policy: SandboxPolicy, path: pathlib.Path) -> str:
    """Create one exclusive mode-0600 policy file and return its SHA-256."""

    if not path.is_absolute() or path.name in {"", ".", ".."}:
        raise ValueError("sandbox policy path differs")
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
                raise OSError("sandbox policy write made no progress")
            offset += written
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    return hashlib.sha256(raw).hexdigest()


def read_canonical_policy_file(
    path: pathlib.Path, expected_sha256: str
) -> SandboxPolicy:
    """Authenticate and decode one regular canonical policy file."""

    if not path.is_absolute() or not _valid_sha256(expected_sha256):
        raise ValueError("sandbox policy digest authority differs")
    if path.is_symlink() or not path.is_file():
        raise ValueError("sandbox policy file authority differs")
    raw = path.read_bytes()
    if hashlib.sha256(raw).hexdigest() != expected_sha256:
        raise ValueError("sandbox policy digest authority differs")
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
        raise ValueError("sandbox digest algorithm differs")
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
        raise ValueError("sandbox authenticated file authority differs")
    if path.stat().st_size != expected_bytes:
        raise ValueError("sandbox authenticated file length differs")
    if _digest_file(path, algorithm) != expected_digest:
        raise ValueError("sandbox authenticated file digest differs")


def authenticate_policy_files(policy: SandboxPolicy) -> None:
    """Rehash every executable/runtime/input file before namespace creation."""

    validate_phase_inputs(policy)
    _authenticate_file(
        policy.executable,
        "sha256",
        policy.executable_sha256,
        policy.executable_bytes,
    )
    for mount in (*policy.runtime_mounts, *policy.inputs):
        _authenticate_file(
            mount.source,
            mount.digest_algorithm,
            mount.digest,
            mount.encoded_bytes,
        )
    for capability in policy.directory_capabilities:
        if capability.source.is_symlink() or not capability.source.is_dir():
            raise ValueError("sandbox directory capability authority differs")


def authenticate_mounted_policy_files(
    root: pathlib.Path, policy: SandboxPolicy
) -> None:
    """Rehash the exact bind-mounted inodes before the old root is detached."""

    _authenticate_file(
        root / "phase/v23-incidence",
        "sha256",
        policy.executable_sha256,
        policy.executable_bytes,
    )
    for mount in (*policy.runtime_mounts, *policy.inputs):
        _authenticate_file(
            root / mount.target.as_posix().lstrip("/"),
            mount.digest_algorithm,
            mount.digest,
            mount.encoded_bytes,
        )
    for capability in policy.directory_capabilities:
        path = root / capability.target.as_posix().lstrip("/")
        if path.is_symlink() or not path.is_dir():
            raise ValueError("sandbox mounted directory authority differs")


def build_unshare_command(policy_path: pathlib.Path, policy_sha256: str) -> list[str]:
    """Build the bounded outer namespace command without invoking a shell."""

    if not policy_path.is_absolute() or not _valid_sha256(policy_sha256):
        raise ValueError("sandbox policy path or digest differs")
    return [
        "unshare",
        "--user",
        "--map-root-user",
        "--mount",
        "--net",
        "--pid",
        "--fork",
        "--mount-proc",
        sys.executable,
        str(pathlib.Path(__file__).resolve()),
        "--enter-sandbox-policy",
        str(policy_path),
        "--policy-sha256",
        policy_sha256,
    ]


def _network_namespace_inode() -> int:
    rendered = os.readlink("/proc/self/ns/net")
    if not rendered.startswith("net:[") or not rendered.endswith("]"):
        raise RuntimeError("network namespace identity differs")
    return int(rendered[5:-1])


def _sandbox_root(policy: SandboxPolicy) -> pathlib.Path:
    return policy.scratch.parent / f".{policy.scratch.name}.sandbox-root"


def _libc_call(name: str, result: int) -> None:
    if result != 0:
        error = ctypes.get_errno()
        raise OSError(error, f"{name} failed: {os.strerror(error)}")


def _mount(
    source: str | None,
    target: pathlib.Path | str,
    filesystem: str | None,
    flags: int,
    data: str | None = None,
) -> None:
    libc = ctypes.CDLL(None, use_errno=True)

    def encoded(value: str | None) -> bytes | None:
        return None if value is None else os.fsencode(value)

    result = libc.mount(
        encoded(source),
        encoded(os.fspath(target)),
        encoded(filesystem),
        ctypes.c_ulong(flags),
        encoded(data),
    )
    _libc_call("mount", result)


def _bind_mount(source: pathlib.Path, target: pathlib.Path, read_only: bool) -> None:
    ms_bind = 4096
    ms_remount = 32
    ms_rdonly = 1
    if source.is_symlink() or not source.exists():
        raise ValueError("sandbox source authority differs")
    target.parent.mkdir(mode=0o755, parents=True, exist_ok=True)
    if source.is_dir():
        target.mkdir(mode=0o755, exist_ok=True)
    elif source.is_file():
        target.touch(mode=0o600, exist_ok=False)
    else:
        raise ValueError("sandbox source is not a regular file or directory")
    _mount(str(source), target, None, ms_bind)
    if read_only:
        _mount(None, target, None, ms_bind | ms_remount | ms_rdonly)


def _pivot_root(new_root: pathlib.Path, old_root: pathlib.Path) -> None:
    machine = os.uname().machine
    syscall_number = {"aarch64": 41, "x86_64": 155}.get(machine)
    if syscall_number is None:
        raise RuntimeError("pivot_root is unsupported on this architecture")
    libc = ctypes.CDLL(None, use_errno=True)
    result = libc.syscall(
        ctypes.c_long(syscall_number),
        os.fsencode(new_root),
        os.fsencode(old_root),
    )
    _libc_call("pivot_root", result)


def _detach_old_root() -> None:
    libc = ctypes.CDLL(None, use_errno=True)
    result = libc.umount2(os.fsencode("/.oldroot"), ctypes.c_int(2))
    _libc_call("umount2", result)
    os.rmdir("/.oldroot")


def _startup_probes(policy: SandboxPolicy) -> dict[str, object]:
    current_inode = _network_namespace_inode()
    namespace_changed = (
        policy.host_network_namespace_inode is not None
        and current_inode != policy.host_network_namespace_inode
    )
    host_canary_denied = True
    for path in policy.host_canaries:
        try:
            descriptor = os.open(path.as_posix(), os.O_RDONLY | os.O_CLOEXEC)
        except OSError:
            continue
        else:
            os.close(descriptor)
            host_canary_denied = False

    network_canary_denied = False
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as connection:
            connection.settimeout(0.05)
            connection.connect(("198.51.100.1", 9))
    except OSError:
        network_canary_denied = True

    allowlisted_inputs_opened = True
    for mount in policy.inputs:
        try:
            descriptor = os.open(mount.target.as_posix(), os.O_RDONLY | os.O_CLOEXEC)
        except OSError:
            allowlisted_inputs_opened = False
            break
        else:
            os.close(descriptor)
    for capability in policy.directory_capabilities:
        try:
            descriptor = os.open(
                capability.target.as_posix(),
                os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC,
            )
        except OSError:
            allowlisted_inputs_opened = False
            break
        else:
            os.close(descriptor)

    probe_path = pathlib.Path("/output/.capability-probe")
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
        "host_canary_denied": host_canary_denied,
        "network_canary_denied": network_canary_denied,
        "network_namespace_changed": namespace_changed,
        "network_namespace_inode": current_inode,
        "output_writable": output_writable,
    }
    if not all(
        probes[key]
        for key in (
            "allowlisted_inputs_opened",
            "host_canary_denied",
            "network_canary_denied",
            "network_namespace_changed",
            "output_writable",
        )
    ):
        raise RuntimeError("sandbox capability probe failed")
    return probes


def enter_sandbox(policy: SandboxPolicy) -> None:
    """Construct the fresh root, prove capability separation, and exec the phase."""

    validate_phase_inputs(policy)
    authenticate_policy_files(policy)
    if policy.host_network_namespace_inode is None:
        raise ValueError("host network namespace authority is absent")
    root = _sandbox_root(policy)
    if root.exists():
        raise ValueError("sandbox root already exists")
    for path in (policy.executable, policy.scratch, policy.output):
        if path.is_symlink() or not path.exists():
            raise ValueError("sandbox host path authority differs")
    if not policy.scratch.is_dir() or not policy.output.is_dir():
        raise ValueError("sandbox scratch/output authority differs")

    root.mkdir(mode=0o700)
    ms_rec = 16384
    ms_private = 1 << 18
    _mount(None, "/", None, ms_rec | ms_private)
    _mount("tmpfs", root, "tmpfs", 0, "mode=0700,size=67108864")

    executable_target = root / "phase/v23-incidence"
    _bind_mount(policy.executable, executable_target, True)
    for mount in (*policy.runtime_mounts, *policy.inputs):
        target = root / mount.target.as_posix().lstrip("/")
        _bind_mount(mount.source, target, mount.read_only)
    for capability in policy.directory_capabilities:
        target = root / capability.target.as_posix().lstrip("/")
        _bind_mount(capability.source, target, capability.read_only)
    _bind_mount(policy.scratch, root / "scratch", False)
    _bind_mount(policy.output, root / "output", False)
    authenticate_mounted_policy_files(root, policy)
    (root / "proc").mkdir(mode=0o555)
    _mount("proc", root / "proc", "proc", 0)
    old_root = root / ".oldroot"
    old_root.mkdir(mode=0o700)
    _pivot_root(root, old_root)
    os.chdir("/")
    _detach_old_root()

    probes = _startup_probes(policy)
    environment = {
        "BORSUK_V23_INCIDENCE_SANDBOX_PROBES": json.dumps(
            probes, separators=(",", ":"), sort_keys=True
        ),
        "LANG": "C",
        "LC_ALL": "C",
    }
    os.execve(
        "/phase/v23-incidence",
        ("/phase/v23-incidence", *policy.phase_argv),
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
    try:
        os.killpg(pid, signal.SIGTERM)
    except ProcessLookupError:
        pass
    deadline = time.monotonic() + grace_seconds
    while True:
        completed, status = os.waitpid(pid, os.WNOHANG)
        if completed == pid:
            return status
        if time.monotonic() >= deadline:
            break
        time.sleep(min(0.05, max(0.001, grace_seconds / 10)))
    try:
        os.killpg(pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    _, status = os.waitpid(pid, 0)
    return status


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
    for line in (
        path.read_text(encoding="ascii").splitlines()
    ):
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


def run_phase(policy: SandboxPolicy, limits: MonitorLimits | None = None) -> int:
    """Launch and monitor one original sandbox process group."""

    bound_policy = dataclasses.replace(
        policy,
        host_network_namespace_inode=_network_namespace_inode(),
    )
    policy_path = policy.scratch.parent / f".{policy.scratch.name}.policy.json"
    policy_sha256 = write_canonical_policy_file(bound_policy, policy_path)
    command = build_unshare_command(policy_path, policy_sha256)
    root = _sandbox_root(bound_policy)
    try:
        process = subprocess.Popen(command, start_new_session=True)  # noqa: S603
        try:
            status, stop_reason = monitor_process_group(
                process.pid,
                limits or MonitorLimits(),
                progress_path=policy.output / "progress.json",
                progress_phase=policy.phase,
            )
            process.returncode = status
        finally:
            if root.exists():
                root.rmdir()
    finally:
        if policy_path.exists():
            if policy_path.is_symlink() or not policy_path.is_file():
                raise ValueError("sandbox policy cleanup authority differs")
            policy_path.unlink()
    if stop_reason is not None:
        raise RuntimeError(f"V23 incidence phase stopped: {stop_reason}")
    return status


def parse_args(arguments: Sequence[str] | None = None) -> argparse.Namespace:
    """Parse one explicit public phase gate or the private sandbox entry gate."""

    parser = argparse.ArgumentParser(allow_abbrev=False)
    gates = parser.add_mutually_exclusive_group(required=True)
    for phase in PHASES:
        gates.add_argument(f"--execute-{phase}", action="store_true")
    gates.add_argument("--enter-sandbox-policy", type=pathlib.Path)
    parser.add_argument("--policy-sha256")
    parser.add_argument("--policy", type=pathlib.Path)
    parsed = parser.parse_args(arguments)
    if parsed.enter_sandbox_policy is not None:
        if parsed.policy_sha256 is None or parsed.policy is not None:
            parser.error("private sandbox entry requires only policy path and digest")
    elif parsed.policy_sha256 is not None:
        parser.error("public phase execution cannot accept a private policy digest")
    if parsed.enter_sandbox_policy is None and parsed.policy is None:
        parser.error("public phase execution requires --policy")
    return parsed


def main(arguments: Sequence[str] | None = None) -> int:
    """Run one explicit launcher mode."""

    parsed = parse_args(arguments)
    if parsed.enter_sandbox_policy is not None:
        enter_sandbox(
            read_canonical_policy_file(
                parsed.enter_sandbox_policy, parsed.policy_sha256
            )
        )
        raise RuntimeError("sandbox executable returned unexpectedly")
    raw = parsed.policy.read_bytes()
    policy = decode_policy_bytes(raw)
    selected = next(
        phase
        for phase in PHASES
        if getattr(parsed, f"execute_{phase.replace('-', '_')}")
    )
    if policy.phase != selected:
        raise ValueError("policy phase differs from explicit execution gate")
    return run_phase(policy)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:  # noqa: BLE001
        traceback.print_exc()
        raise SystemExit(1) from error
