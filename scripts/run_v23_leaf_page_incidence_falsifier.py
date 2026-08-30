#!/usr/bin/env python3
"""Fail-closed local launcher for the claim-ineligible V23 incidence falsifier."""

from __future__ import annotations

import argparse
import base64
import ctypes
import dataclasses
import hashlib
import json
import os
import pathlib
import signal
import socket
import subprocess
import sys
import time
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
    digest_algorithm: str
    digest: str
    encoded_bytes: int
    generation: str


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


def _valid_sha256(value: str | None) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and all(character in _LOWER_HEX for character in value)
    )


def _phase_roles(phase: str) -> tuple[set[str], tuple[str, ...]]:
    if phase == "tree-training":
        return {"construction-manifest"}, ("training-shard-",)
    if phase == "posting-construction":
        return {"parent-receipt", "incidence-tree", "page-roster"}, ("page-body-",)
    if phase == "development-evaluation":
        return {
            "parent-receipt",
            "incidence-tree",
            "incidence-postings-one",
            "incidence-postings-two",
            "d2-report",
            "query-parquet",
        }, ()
    if phase == "holdout-binding":
        return {"parent-receipt", "page-roster", "neighbors-parquet"}, ("page-body-",)
    if phase == "holdout-evaluation":
        return {
            "parent-receipt",
            "incidence-tree",
            "incidence-postings-one",
            "incidence-postings-two",
            "query-parquet",
            "holdout-truth",
        }, ()
    raise ValueError("unknown V23 incidence phase")


def _validate_mount(mount: SandboxMount, *, runtime: bool) -> None:
    if not mount.role or not mount.source.is_absolute() or not mount.target.is_absolute():
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
        allowed_target = rendered_target.startswith(
            ("/lib/", "/lib64/", "/usr/lib/", "/usr/lib64/")
        ) or rendered_target == "/etc/ld.so.cache/"
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
        } or mount.role.startswith("page-body-")
        expected_algorithm = "blake3" if blake3_role else "sha256"
        if mount.digest_algorithm != expected_algorithm:
            raise ValueError("phase input digest algorithm differs")


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
    if policy.scratch in policy.output.parents or policy.output in policy.scratch.parents:
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
        if mount.source in seen_sources or mount.target in seen_targets or mount.role in seen_roles:
            raise ValueError("duplicate sandbox mount authority")
        seen_sources.add(mount.source)
        seen_targets.add(mount.target)
        seen_roles.add(mount.role)
    for mount in policy.inputs:
        _validate_mount(mount, runtime=False)
        if mount.source in seen_sources or mount.target in seen_targets or mount.role in seen_roles:
            raise ValueError("duplicate sandbox mount authority")
        seen_sources.add(mount.source)
        seen_targets.add(mount.target)
        seen_roles.add(mount.role)
    if policy.executable in seen_sources:
        raise ValueError("duplicate sandbox mount authority")
    for capability_path in (policy.scratch, policy.output):
        if capability_path in seen_sources or capability_path == policy.executable:
            raise ValueError("sandbox writable and read-only capabilities overlap")

    fixed_roles, prefixes = _phase_roles(policy.phase)
    actual_roles = {mount.role for mount in policy.inputs}
    if not fixed_roles.issubset(actual_roles):
        raise ValueError("required phase input is absent")
    if any(
        role not in fixed_roles and not any(role.startswith(prefix) for prefix in prefixes)
        for role in actual_roles
    ):
        raise ValueError("phase input capability differs")
    if policy.phase == "tree-training" and not any(
        role.startswith("training-shard-") for role in actual_roles
    ):
        raise ValueError("training shard authority is absent")
    if policy.phase in {"posting-construction", "holdout-binding"} and not any(
        role.startswith("page-body-") for role in actual_roles
    ):
        raise ValueError("page body authority is absent")


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
        }

    return {
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


def canonical_policy_argument(policy: SandboxPolicy) -> str:
    """Return one deterministic URL-safe policy argument."""

    validate_phase_inputs(policy)
    raw = json.dumps(_policy_value(policy), separators=(",", ":"), sort_keys=True).encode()
    return base64.urlsafe_b64encode(raw).decode("ascii")


def decode_policy_argument(argument: str) -> SandboxPolicy:
    """Decode one exact canonical policy argument and reject schema drift."""

    try:
        raw = base64.b64decode(argument.encode("ascii"), altchars=b"-_", validate=True)
        value = json.loads(raw)
    except (UnicodeEncodeError, ValueError, json.JSONDecodeError) as error:
        raise ValueError("sandbox policy encoding differs") from error
    expected_keys = {
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
        ):
            raise ValueError("sandbox policy concrete type differs")
        return SandboxMount(
            role=item["role"],
            source=pathlib.Path(item["source"]),
            target=pathlib.PurePosixPath(item["target"]),
            read_only=item["read_only"],
            digest_algorithm=item["digest_algorithm"],
            digest=item["digest"],
            encoded_bytes=item["encoded_bytes"],
            generation=item["generation"],
        )

    if (
        type(value["phase"]) is not str
        or type(value["executable"]) is not str
        or type(value["executable_sha256"]) is not str
        or type(value["executable_bytes"]) is not int
        or type(value["scratch"]) is not str
        or type(value["output"]) is not str
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
        phase_argv=tuple(value["phase_argv"]),
        host_network_namespace_inode=value["host_network_namespace_inode"],
        host_canaries=tuple(
            pathlib.PurePosixPath(item) for item in value["host_canaries"]
        ),
    )
    validate_phase_inputs(policy)
    if canonical_policy_argument(policy) != argument:
        raise ValueError("sandbox policy canonical bytes differ")
    return policy


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


def build_unshare_command(policy: SandboxPolicy) -> list[str]:
    """Build the sole outer namespace command without invoking a shell."""

    validate_phase_inputs(policy)
    bound_policy = dataclasses.replace(
        policy,
        host_network_namespace_inode=_network_namespace_inode(),
    )
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
        "--enter-sandbox",
        canonical_policy_argument(bound_policy),
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
    _bind_mount(policy.scratch, root / "scratch", False)
    _bind_mount(policy.output, root / "output", False)
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
    term_grace_seconds: float = 15.0,
) -> tuple[int, str | None]:
    """Monitor and stop one original process group without restart."""

    if pid <= 1:
        raise ValueError("invalid process group leader")
    started = time.monotonic()
    initial_swap = _swap_used_bytes()
    sustained = 0
    last_progress = started
    progress_token: tuple[int, str] | None = None
    while True:
        completed, status = os.waitpid(pid, os.WNOHANG)
        if completed == pid:
            return os.waitstatus_to_exitcode(status), None
        psi = _memory_psi_full_avg10()
        sustained = sustained + 1 if psi >= limits.psi_sustained else 0
        observed_progress = _progress_token(progress_path)
        if observed_progress is not None and (
            progress_token is None or observed_progress[0] > progress_token[0]
        ):
            progress_token = observed_progress
            last_progress = time.monotonic()
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


def _progress_token(path: pathlib.Path | None) -> tuple[int, str] | None:
    if path is None or not path.is_file() or path.is_symlink():
        return None
    try:
        raw = path.read_bytes()
        value = json.loads(raw)
    except (OSError, json.JSONDecodeError):
        return None
    if (
        not raw.endswith(b"\n")
        or raw.count(b"\n") != 1
        or type(value) is not dict
        or set(value) != {"authenticated_objects", "last_digest"}
        or type(value["authenticated_objects"]) is not int
        or value["authenticated_objects"] < 0
        or not _valid_sha256(value["last_digest"])
    ):
        return None
    canonical = json.dumps(value, separators=(",", ":"), sort_keys=True).encode() + b"\n"
    if raw != canonical:
        return None
    return value["authenticated_objects"], value["last_digest"]


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


def _memory_psi_full_avg10() -> float:
    for line in pathlib.Path("/proc/pressure/memory").read_text(encoding="ascii").splitlines():
        if line.startswith("full "):
            fields = dict(field.split("=", 1) for field in line.split()[1:])
            return float(fields["avg10"])
    raise RuntimeError("memory PSI full sample is absent")


def _swap_used_bytes() -> int:
    values: dict[str, int] = {}
    for line in pathlib.Path("/proc/meminfo").read_text(encoding="ascii").splitlines():
        key, value, *_ = line.split()
        if key in {"SwapTotal:", "SwapFree:"}:
            values[key] = int(value) * 1024
    return values["SwapTotal:"] - values["SwapFree:"]


def run_phase(policy: SandboxPolicy, limits: MonitorLimits | None = None) -> int:
    """Launch and monitor one original sandbox process group."""

    command = build_unshare_command(policy)
    root = _sandbox_root(policy)
    process = subprocess.Popen(command, start_new_session=True)  # noqa: S603
    try:
        status, stop_reason = monitor_process_group(
            process.pid,
            limits or MonitorLimits(),
            progress_path=policy.output / "progress.json",
        )
        process.returncode = status
    finally:
        if root.exists():
            root.rmdir()
    if stop_reason is not None:
        raise RuntimeError(f"V23 incidence phase stopped: {stop_reason}")
    return status


def parse_args(arguments: Sequence[str] | None = None) -> argparse.Namespace:
    """Parse one explicit public phase gate or the private sandbox entry gate."""

    parser = argparse.ArgumentParser(allow_abbrev=False)
    gates = parser.add_mutually_exclusive_group(required=True)
    for phase in PHASES:
        gates.add_argument(f"--execute-{phase}", action="store_true")
    gates.add_argument("--enter-sandbox", metavar="POLICY")
    parser.add_argument("--policy", type=pathlib.Path)
    parsed = parser.parse_args(arguments)
    if parsed.enter_sandbox is None and parsed.policy is None:
        parser.error("public phase execution requires --policy")
    return parsed


def main(arguments: Sequence[str] | None = None) -> int:
    """Run one explicit launcher mode."""

    parsed = parse_args(arguments)
    if parsed.enter_sandbox is not None:
        enter_sandbox(decode_policy_argument(parsed.enter_sandbox))
        raise RuntimeError("sandbox executable returned unexpectedly")
    raw = parsed.policy.read_bytes()
    if not raw.endswith(b"\n") or raw.count(b"\n") != 1:
        raise ValueError("policy file bytes differ")
    policy = decode_policy_argument(raw[:-1].decode("ascii"))
    selected = next(
        phase for phase in PHASES if getattr(parsed, f"execute_{phase.replace('-', '_')}")
    )
    if policy.phase != selected:
        raise ValueError("policy phase differs from explicit execution gate")
    return run_phase(policy)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:  # noqa: BLE001
        print(error, file=sys.stderr)
        raise SystemExit(1) from error
