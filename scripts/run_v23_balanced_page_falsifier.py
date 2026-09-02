#!/usr/bin/env python3
"""Direct offline supervisor for the local V23 balanced-page executable."""

from __future__ import annotations

import argparse
import dataclasses
import hashlib
import json
import os
import pathlib
import signal
import stat
import subprocess
import sys
import time
from collections.abc import Sequence

_LOWER_HEX = frozenset("0123456789abcdef")


@dataclasses.dataclass(frozen=True)
class BalancedRunPolicy:
    """Exact local process and artifact authority for one run."""

    executable: pathlib.Path
    executable_sha256: str
    executable_bytes: int
    manifest: pathlib.Path
    manifest_sha256: str
    manifest_bytes: int
    input_directory: pathlib.Path
    output_directory: pathlib.Path
    mode: str
    cleanup_paths: tuple[pathlib.Path, ...]


@dataclasses.dataclass(frozen=True)
class MonitorLimits:
    """Fail-closed process-group resource limits."""

    rss_bytes: int = 3 * 1024 * 1024 * 1024
    psi_full_avg10: float = 0.79
    swap_delta_bytes: int = 256 * 1024 * 1024
    wall_seconds: int = 7200


@dataclasses.dataclass(frozen=True)
class RunOutcome:
    """Preserved terminal evidence from the sole child process."""

    returncode: int
    stdout: bytes
    stderr: bytes
    stop: str | None
    wall_seconds: float
    peak_rss_bytes: int
    peak_psi_full_avg10: float
    swap_delta_bytes: int


def _valid_sha256(value: str) -> bool:
    return len(value) == 64 and all(character in _LOWER_HEX for character in value)


def _sha256(path: pathlib.Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _regular_file(path: pathlib.Path) -> bool:
    try:
        metadata = path.lstat()
    except FileNotFoundError:
        return False
    return stat.S_ISREG(metadata.st_mode)


def validate_policy(policy: BalancedRunPolicy) -> None:
    """Authenticate the direct binary, manifest, and local directory boundary."""

    if (
        not policy.executable.is_absolute()
        or not _regular_file(policy.executable)
        or not _valid_sha256(policy.executable_sha256)
        or type(policy.executable_bytes) is not int
        or policy.executable_bytes <= 0
        or policy.executable.stat().st_size != policy.executable_bytes
        or _sha256(policy.executable) != policy.executable_sha256
    ):
        raise ValueError("executable authority differs")
    if (
        not policy.manifest.is_absolute()
        or not _regular_file(policy.manifest)
        or not _valid_sha256(policy.manifest_sha256)
        or type(policy.manifest_bytes) is not int
        or policy.manifest_bytes <= 0
        or policy.manifest.stat().st_size != policy.manifest_bytes
        or _sha256(policy.manifest) != policy.manifest_sha256
    ):
        raise ValueError("manifest authority differs")
    if (
        not policy.input_directory.is_absolute()
        or not policy.input_directory.is_dir()
        or not policy.output_directory.is_absolute()
        or not policy.output_directory.is_dir()
        or any(policy.output_directory.iterdir())
    ):
        raise ValueError("output directory or input directory differs")
    if policy.mode not in {"preflight", "execute"}:
        raise ValueError("run mode differs")
    if len(set(policy.cleanup_paths)) != len(policy.cleanup_paths) or any(
        not path.is_absolute() for path in policy.cleanup_paths
    ):
        raise ValueError("cleanup authority differs")


def build_offline_command(policy: BalancedRunPolicy) -> tuple[str, ...]:
    """Return the direct executable command with no credentialed helper."""

    mode = "--preflight" if policy.mode == "preflight" else "--execute"
    return (
        str(policy.executable),
        "--manifest",
        str(policy.manifest),
        "--input-directory",
        str(policy.input_directory),
        "--output-directory",
        str(policy.output_directory),
        mode,
    )


def build_offline_environment() -> dict[str, str]:
    """Return a minimal environment without cloud credentials or proxy routing."""

    return {
        "LANG": "C.UTF-8",
        "LC_ALL": "C.UTF-8",
        "PATH": "/usr/bin:/bin",
        "RUST_BACKTRACE": "1",
    }


def validate_terminal(raw: bytes, *, mode: str | None = None) -> dict[str, object]:
    """Validate the canonical top-level receipt envelope emitted on stdout."""

    if not raw.endswith(b"\n") or raw.count(b"\n") != 1:
        raise ValueError("terminal canonical bytes differ")
    try:
        value = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("terminal encoding differs") from error
    if type(value) is not dict:  # noqa: E721
        raise ValueError("terminal authority differs")
    expected_keys = {
        "claim_eligible",
        "manifest_sha256",
        "ordered_inputs",
        "outputs",
        "pseudoquery_pairs",
        "schema",
        "selected_pair",
        "stop",
    }
    if (
        set(value) != expected_keys
        or value["schema"] != "borsuk-v23-balanced-page-receipt-v4"
        or value["claim_eligible"] is not False
        or not _valid_sha256(value["manifest_sha256"])
        or type(value["ordered_inputs"]) is not list  # noqa: E721
        or type(value["outputs"]) is not list  # noqa: E721
        or type(value["pseudoquery_pairs"]) is not list  # noqa: E721
    ):
        raise ValueError("terminal authority differs")
    if mode is not None:
        expected_pairs = 0 if mode == "preflight" else 12 if mode == "execute" else None
        if expected_pairs is None:
            raise ValueError("run mode differs")
        if len(value["pseudoquery_pairs"]) != expected_pairs:
            raise ValueError("terminal pair inventory differs")
    canonical = (
        json.dumps(value, separators=(",", ":"), sort_keys=True).encode() + b"\n"
    )
    if raw != canonical:
        raise ValueError("terminal canonical bytes differ")
    return value


def authenticate_receipt_outputs(
    receipt: dict[str, object], output_directory: pathlib.Path
) -> tuple[pathlib.Path, ...]:
    """Reauthenticate every reported output against the exact local inventory."""

    outputs = receipt.get("outputs")
    if type(outputs) is not list:  # noqa: E721
        raise ValueError("output authority differs")
    paths: list[pathlib.Path] = []
    roles: set[str] = set()
    basenames: set[str] = set()
    for output in outputs:
        if type(output) is not dict:  # noqa: E721
            raise ValueError("output authority differs")
        if set(output) != {
            "digest",
            "digest_algorithm",
            "encoded_bytes",
            "role",
            "uri",
        }:
            raise ValueError("output authority differs")
        role = output["role"]
        uri = output["uri"]
        digest = output["digest"]
        encoded_bytes = output["encoded_bytes"]
        if (
            type(role) is not str  # noqa: E721
            or not role
            or type(uri) is not str  # noqa: E721
            or not uri.startswith("s3://")
            or output["digest_algorithm"] != "sha256"
            or type(digest) is not str  # noqa: E721
            or not _valid_sha256(digest)
            or type(encoded_bytes) is not int  # noqa: E721
            or encoded_bytes <= 0
        ):
            raise ValueError("output authority differs")
        basename = uri.rsplit("/", 1)[-1]
        if (
            pathlib.PurePosixPath(basename).name != basename
            or basename in {"", ".", ".."}
            or role in roles
            or basename in basenames
        ):
            raise ValueError("output authority differs")
        path = output_directory / basename
        if (
            not _regular_file(path)
            or path.stat().st_size != encoded_bytes
            or _sha256(path) != digest
        ):
            raise ValueError("output authority differs")
        roles.add(role)
        basenames.add(basename)
        paths.append(path)
    inventory = {
        path.name for path in output_directory.iterdir() if _regular_file(path)
    }
    if inventory != basenames or len(tuple(output_directory.iterdir())) != len(paths):
        raise ValueError("output inventory differs")
    return tuple(paths)


def cleanup_explicit_files(
    paths: Sequence[pathlib.Path], *, process_group_alive: bool
) -> None:
    """Unlink only named regular files after the child process group is gone."""

    if process_group_alive:
        raise ValueError("process group remains active")
    for path in paths:
        if not _regular_file(path):
            raise ValueError("cleanup target is not a regular file")
    for path in paths:
        path.unlink()


def _process_group_rss(pgid: int) -> int:
    total = 0
    for entry in pathlib.Path("/proc").iterdir():
        if not entry.name.isdigit():
            continue
        try:
            fields = (entry / "stat").read_text().split()
            if int(fields[4]) != pgid:
                continue
            status = (entry / "status").read_text().splitlines()
        except (FileNotFoundError, PermissionError, ProcessLookupError, ValueError):
            continue
        for line in status:
            if line.startswith("VmRSS:"):
                total += int(line.split()[1]) * 1024
                break
    return total


def _psi_full_avg10() -> float:
    for line in pathlib.Path("/proc/pressure/memory").read_text().splitlines():
        if line.startswith("full "):
            fields = dict(field.split("=", 1) for field in line.split()[1:])
            return float(fields["avg10"])
    raise ValueError("memory PSI full record is absent")


def _swap_used_bytes() -> int:
    values: dict[str, int] = {}
    for line in pathlib.Path("/proc/meminfo").read_text().splitlines():
        name, raw = line.split(":", 1)
        if name in {"SwapTotal", "SwapFree"}:
            values[name] = int(raw.split()[0]) * 1024
    return values["SwapTotal"] - values["SwapFree"]


def _stop_group(process: subprocess.Popen[bytes]) -> None:
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        os.killpg(process.pid, signal.SIGKILL)
        process.wait(timeout=5)


def run_balanced_cell(
    policy: BalancedRunPolicy, limits: MonitorLimits | None = None
) -> RunOutcome:
    """Run and preserve exactly one direct offline child process."""

    validate_policy(policy)
    limits = limits or MonitorLimits()
    started = time.monotonic()
    swap_start = _swap_used_bytes()
    peak_rss = 0
    peak_psi = 0.0
    stop: str | None = None
    process = subprocess.Popen(
        build_offline_command(policy),
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=build_offline_environment(),
        start_new_session=True,
    )
    while process.poll() is None:
        elapsed = time.monotonic() - started
        rss = _process_group_rss(process.pid)
        psi = _psi_full_avg10()
        swap_delta = max(0, _swap_used_bytes() - swap_start)
        peak_rss = max(peak_rss, rss)
        peak_psi = max(peak_psi, psi)
        if rss > limits.rss_bytes:
            stop = "resource-rss"
        elif psi > limits.psi_full_avg10:
            stop = "resource-psi"
        elif swap_delta > limits.swap_delta_bytes:
            stop = "resource-swap"
        elif elapsed > limits.wall_seconds:
            stop = "timeout"
        if stop is not None:
            _stop_group(process)
            break
        time.sleep(0.1)
    stdout, stderr = process.communicate()
    terminal_error: BaseException | None = None
    if process.returncode == 0 and stop is None:
        try:
            receipt = validate_terminal(stdout, mode=policy.mode)
            authenticate_receipt_outputs(receipt, policy.output_directory)
        except BaseException as error:
            terminal_error = error
    group_alive = _process_group_rss(process.pid) != 0
    if policy.cleanup_paths:
        cleanup_explicit_files(policy.cleanup_paths, process_group_alive=group_alive)
    if terminal_error is not None:
        raise terminal_error
    return RunOutcome(
        returncode=process.returncode,
        stdout=stdout,
        stderr=stderr,
        stop=stop,
        wall_seconds=time.monotonic() - started,
        peak_rss_bytes=peak_rss,
        peak_psi_full_avg10=peak_psi,
        swap_delta_bytes=max(0, _swap_used_bytes() - swap_start),
    )


def parse_args(
    arguments: Sequence[str] | None = None,
) -> tuple[BalancedRunPolicy, MonitorLimits]:
    """Parse the strict local-only supervisor boundary."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--executable", type=pathlib.Path, required=True)
    parser.add_argument("--executable-sha256", required=True)
    parser.add_argument("--executable-bytes", type=int, required=True)
    parser.add_argument("--manifest", type=pathlib.Path, required=True)
    parser.add_argument("--manifest-sha256", required=True)
    parser.add_argument("--manifest-bytes", type=int, required=True)
    parser.add_argument("--input-directory", type=pathlib.Path, required=True)
    parser.add_argument("--output-directory", type=pathlib.Path, required=True)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--preflight", action="store_true")
    mode.add_argument("--execute", action="store_true")
    parser.add_argument("--rss-bytes", type=int, default=3 * 1024 * 1024 * 1024)
    parser.add_argument("--psi-full-avg10", type=float, default=0.79)
    parser.add_argument("--swap-delta-bytes", type=int, default=256 * 1024 * 1024)
    parser.add_argument("--wall-seconds", type=int, default=7200)
    values = parser.parse_args(arguments)
    policy = BalancedRunPolicy(
        executable=values.executable,
        executable_sha256=values.executable_sha256,
        executable_bytes=values.executable_bytes,
        manifest=values.manifest,
        manifest_sha256=values.manifest_sha256,
        manifest_bytes=values.manifest_bytes,
        input_directory=values.input_directory,
        output_directory=values.output_directory,
        mode="preflight" if values.preflight else "execute",
        cleanup_paths=(),
    )
    limits = MonitorLimits(
        rss_bytes=values.rss_bytes,
        psi_full_avg10=values.psi_full_avg10,
        swap_delta_bytes=values.swap_delta_bytes,
        wall_seconds=values.wall_seconds,
    )
    if (
        limits.rss_bytes <= 0
        or not 0.0 <= limits.psi_full_avg10 <= 100.0
        or limits.swap_delta_bytes < 0
        or limits.wall_seconds <= 0
    ):
        parser.error("resource limit differs")
    return policy, limits


def main(arguments: Sequence[str] | None = None) -> int:
    """Run one authenticated child and forward its preserved terminal bytes."""

    policy, limits = parse_args(arguments)
    outcome = run_balanced_cell(policy, limits)
    if outcome.stderr:
        sys.stderr.buffer.write(outcome.stderr)
    if outcome.stop is not None:
        print(outcome.stop, file=sys.stderr)
        return 70
    if outcome.returncode != 0:
        return outcome.returncode
    sys.stdout.buffer.write(outcome.stdout)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
