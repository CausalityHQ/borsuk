#!/usr/bin/env python3
"""Fail-closed small-runtime host attestation for Publication V3."""

from __future__ import annotations

import copy
import ctypes
import hashlib
import json
import os
import platform
import re
import subprocess
import urllib.request
from pathlib import Path

try:
    from scripts.publication_v3_protocol import canonical_json_bytes
except ModuleNotFoundError:
    from publication_v3_protocol import canonical_json_bytes

FIELDS = frozenset(
    {
        "schema_version",
        "cell_id",
        "attempt_id",
        "instance_id",
        "instance_type",
        "architecture",
        "vcpus",
        "memory_max_bytes",
        "memory_peak_bytes",
        "swap_max_bytes",
        "swap_current_bytes",
        "swap_peak_bytes",
        "oom_events",
        "oom_kill_events",
        "cache_limit_bytes",
        "cache_filesystem_bytes",
        "cache_device",
        "root_device",
        "cache_is_mount",
        "source_revision",
    }
)
CGROUP2_SUPER_MAGIC = 0x63677270
IMDS_BASE_URL = "http://169.254.169.254/latest"


def runtime_attestation_sha256(value: dict[str, object]) -> str:
    return hashlib.sha256(canonical_json_bytes(value) + b"\n").hexdigest()


def _read_cgroup_integer(root: Path, name: str) -> int:
    try:
        value = (root / name).read_text(encoding="utf-8").strip()
    except OSError as error:
        raise ValueError(f"runtime cgroup {name} is unavailable") from error
    if value == "max" or re.fullmatch(r"[0-9]+", value) is None:
        raise ValueError(f"runtime cgroup {name} is not a finite integer")
    return int(value)


def _cpuset_count(value: str) -> int:
    members: set[int] = set()
    for component in value.strip().split(","):
        bounds = component.split("-", 1)
        if not bounds[0].isdigit() or (len(bounds) == 2 and not bounds[1].isdigit()):
            raise ValueError("runtime effective CPU set is invalid")
        start = int(bounds[0])
        end = int(bounds[-1])
        if end < start:
            raise ValueError("runtime effective CPU set is invalid")
        members.update(range(start, end + 1))
    if not members:
        raise ValueError("runtime effective CPU set is empty")
    return len(members)


def _read_memory_events(root: Path) -> tuple[int, int]:
    events: dict[str, int] = {}
    try:
        lines = (root / "memory.events").read_text(encoding="utf-8").splitlines()
    except OSError as error:
        raise ValueError("runtime cgroup memory.events is unavailable") from error
    for line in lines:
        fields = line.split()
        if len(fields) != 2 or not fields[1].isdigit():
            raise ValueError("runtime cgroup memory.events is invalid")
        events[fields[0]] = int(fields[1])
    if "oom" not in events or "oom_kill" not in events:
        raise ValueError("runtime cgroup memory.events lacks OOM counters")
    return events["oom"], events["oom_kill"]


class _StatFs(ctypes.Structure):
    _fields_ = [
        ("f_type", ctypes.c_long),
        ("_remaining", ctypes.c_byte * 248),
    ]


def _filesystem_magic(path: Path) -> int:
    result = _StatFs()
    libc = ctypes.CDLL(None, use_errno=True)
    if libc.statfs(os.fsencode(path), ctypes.byref(result)) != 0:
        error = ctypes.get_errno()
        raise ValueError("runtime cgroup filesystem cannot be inspected") from OSError(
            error, os.strerror(error), path
        )
    return int(result.f_type)


def _current_cgroup_root(
    *,
    proc_self_cgroup: Path = Path("/proc/self/cgroup"),
    cgroup_mount: Path = Path("/sys/fs/cgroup"),
) -> Path:
    try:
        entries = proc_self_cgroup.read_text(encoding="utf-8").splitlines()
    except OSError as error:
        raise ValueError("runtime process cgroup is unavailable") from error
    unified = [entry[3:] for entry in entries if entry.startswith("0::/")]
    if len(unified) != 1:
        raise ValueError("runtime process is not in one unified cgroup v2 hierarchy")
    mount = cgroup_mount.resolve()
    current = (mount / unified[0].lstrip("/")).resolve()
    try:
        current.relative_to(mount)
    except ValueError as error:
        raise ValueError("runtime process cgroup escapes its mount") from error
    if _filesystem_magic(current) != CGROUP2_SUPER_MAGIC:
        raise ValueError("runtime process is not measured by cgroup v2")
    return current


def _instance_identity_document() -> dict[str, object]:
    token_request = urllib.request.Request(
        f"{IMDS_BASE_URL}/api/token",
        method="PUT",
        headers={"X-aws-ec2-metadata-token-ttl-seconds": "60"},
    )
    try:
        with urllib.request.urlopen(token_request, timeout=2) as response:
            token = response.read(4096).decode("utf-8")
        document_request = urllib.request.Request(
            f"{IMDS_BASE_URL}/dynamic/instance-identity/document",
            headers={"X-aws-ec2-metadata-token": token},
        )
        with urllib.request.urlopen(document_request, timeout=2) as response:
            value = json.loads(response.read(64 * 1024))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("runtime EC2 identity document is unavailable") from error
    if not isinstance(value, dict):
        raise ValueError("runtime EC2 identity document is invalid")
    return value


def _measured_source_revision(source_root: Path) -> str:
    try:
        value = subprocess.run(
            ["git", "-C", str(source_root), "rev-parse", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
            timeout=5,
        ).stdout.strip()
    except (OSError, subprocess.SubprocessError) as error:
        raise ValueError("runtime source revision cannot be measured") from error
    if re.fullmatch(r"[0-9a-f]{40}", value) is None:
        raise ValueError("runtime source revision is invalid")
    return value


def _runtime_cache_path(runtime: dict[str, object]) -> Path:
    steps = runtime.get("steps")
    if not isinstance(steps, list) or not steps:
        raise ValueError("runtime plan has no cache authority")
    values = {
        step.get("env", {}).get("BORSUK_BENCH_CACHE")
        for step in steps
        if isinstance(step, dict) and isinstance(step.get("env"), dict)
    }
    if len(values) != 1:
        raise ValueError("runtime plan cache authority differs between steps")
    value = values.pop()
    if not isinstance(value, str) or not value:
        raise ValueError("runtime plan cache authority is invalid")
    return Path(value)


def collect_runtime_attestation(
    *,
    cell: dict[str, object],
    attempt_id: str,
    runtime: dict[str, object],
    source_root: Path,
    proc_self_cgroup: Path = Path("/proc/self/cgroup"),
    cgroup_mount: Path = Path("/sys/fs/cgroup"),
) -> dict[str, object]:
    environment = cell.get("environment_contract")
    clients = environment.get("runtime_clients") if isinstance(environment, dict) else None
    client = clients.get(cell.get("system")) if isinstance(clients, dict) else None
    if not isinstance(client, dict):
        raise ValueError("runtime collector has no scheduled client")
    cgroup_root = _current_cgroup_root(
        proc_self_cgroup=proc_self_cgroup, cgroup_mount=cgroup_mount
    )
    cache_path = _runtime_cache_path(runtime)
    identity = _instance_identity_document()
    filesystem = os.statvfs(cache_path)
    device = os.stat(cache_path).st_dev
    root_device = os.stat("/").st_dev
    oom_events, oom_kill_events = _read_memory_events(cgroup_root)
    return {
        "schema_version": 1,
        "cell_id": cell.get("cell_id"),
        "attempt_id": attempt_id,
        "instance_id": identity.get("instanceId"),
        "instance_type": identity.get("instanceType"),
        "architecture": platform.machine(),
        "vcpus": _cpuset_count(
            (cgroup_root / "cpuset.cpus.effective").read_text(encoding="utf-8")
        ),
        "memory_max_bytes": _read_cgroup_integer(cgroup_root, "memory.max"),
        "memory_peak_bytes": _read_cgroup_integer(cgroup_root, "memory.peak"),
        "swap_max_bytes": _read_cgroup_integer(cgroup_root, "memory.swap.max"),
        "swap_current_bytes": _read_cgroup_integer(
            cgroup_root, "memory.swap.current"
        ),
        "swap_peak_bytes": _read_cgroup_integer(cgroup_root, "memory.swap.peak"),
        "oom_events": oom_events,
        "oom_kill_events": oom_kill_events,
        "cache_limit_bytes": client.get("disk_cache_limit_mib") * 1024 * 1024,
        "cache_filesystem_bytes": filesystem.f_blocks * filesystem.f_frsize,
        "cache_device": f"{os.major(device)}:{os.minor(device)}",
        "root_device": f"{os.major(root_device)}:{os.minor(root_device)}",
        "cache_is_mount": os.path.ismount(cache_path),
        "source_revision": _measured_source_revision(source_root),
    }


def _positive(value: object, role: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{role} must be a positive integer")
    return value


def validate_runtime_attestation(
    value: object, *, cell: dict[str, object], attempt_id: str
) -> dict[str, object]:
    if not isinstance(value, dict) or frozenset(value) != FIELDS:
        raise ValueError("runtime attestation fields differ")
    if value["schema_version"] != 1:
        raise ValueError("runtime attestation schema differs")
    if value["cell_id"] != cell.get("cell_id") or value["attempt_id"] != attempt_id:
        raise ValueError("runtime attestation execution identity differs")
    environment = cell.get("environment_contract")
    system = cell.get("system")
    clients = environment.get("runtime_clients") if isinstance(environment, dict) else None
    client = clients.get(system) if isinstance(clients, dict) else None
    storage = environment.get("runtime_storage") if isinstance(environment, dict) else None
    if not isinstance(client, dict) or not isinstance(storage, dict):
        raise ValueError("runtime attestation has no scheduled host contract")
    if value["instance_type"] != client.get("instance_type"):
        raise ValueError("runtime instance type differs from its contract")
    if value["architecture"] != environment.get("architecture"):
        raise ValueError("runtime architecture differs from its contract")
    if _positive(value["vcpus"], "runtime vCPUs") != client.get("vcpus"):
        raise ValueError("runtime vCPUs differ from their contract")
    memory_max = _positive(value["memory_max_bytes"], "runtime memory maximum")
    if memory_max != client.get("memory_mib") * 1024 * 1024:
        raise ValueError("runtime cgroup memory differs from its contract")
    if _positive(value["memory_peak_bytes"], "runtime memory peak") > memory_max:
        raise ValueError("runtime memory peak exceeds its cgroup")
    if (
        value["swap_max_bytes"] != 0
        or value["swap_current_bytes"] != 0
        or value["swap_peak_bytes"] != 0
    ):
        raise ValueError("runtime swap must be disabled and unused")
    if value["oom_events"] != 0 or value["oom_kill_events"] != 0:
        raise ValueError("runtime must not encounter an OOM event")
    cache_limit = _positive(value["cache_limit_bytes"], "runtime cache limit")
    if cache_limit != client.get("disk_cache_limit_mib") * 1024 * 1024:
        raise ValueError("runtime cache limit differs from its contract")
    cache_filesystem_bytes = _positive(
        value["cache_filesystem_bytes"], "runtime cache filesystem"
    )
    if cache_filesystem_bytes < cache_limit or cache_filesystem_bytes > storage.get(
        "volume_size_gib"
    ) * 1024**3:
        raise ValueError("runtime cache filesystem exceeds its contract")
    for field in ("instance_id", "cache_device", "root_device"):
        if not isinstance(value[field], str) or not value[field] or len(value[field]) > 256:
            raise ValueError(f"runtime {field} is invalid")
    if value["cache_is_mount"] is not True or value["cache_device"] == value["root_device"]:
        raise ValueError("runtime cache must be a dedicated mounted filesystem")
    source = cell.get("source")
    expected_revision = source.get("git_commit") if isinstance(source, dict) else None
    if (
        not isinstance(value["source_revision"], str)
        or re.fullmatch(r"[0-9a-f]{40}", value["source_revision"]) is None
        or value["source_revision"] != expected_revision
    ):
        raise ValueError("runtime source revision differs from its frozen source")
    return copy.deepcopy(value)
