#!/usr/bin/env python3
"""Run one authenticated V24 phase as one directly executed local process."""

from __future__ import annotations

import argparse
import dataclasses
import hashlib
import json
import math
import os
import pathlib
import signal
import subprocess
import sys
import time
from collections.abc import Mapping, Sequence

from scripts.stage_v24_witness_inputs import manifest_phase, validate_inventory

_PHASE_FLAGS = {
    "train-witnesses": "--train-witnesses",
    "build-postings": "--build-postings",
    "evaluate-development": "--evaluate-development",
    "bind-holdout": "--bind-holdout",
    "evaluate-holdout": "--evaluate-holdout",
}
_MANIFEST_PHASES = {
    "train-witnesses": "witness-training",
    "build-postings": "posting-construction",
    "evaluate-development": "development-evaluation",
    "bind-holdout": "holdout-binding",
    "evaluate-holdout": "holdout-evaluation",
}
_LOWER_HEX = frozenset("0123456789abcdef")


@dataclasses.dataclass(frozen=True)
class PhaseRequest:
    """Exact direct-executable request for one V24 phase."""

    phase: str
    executable: pathlib.Path
    executable_sha256: str
    executable_bytes: int
    manifest: pathlib.Path
    manifest_sha256: str
    staging_receipt: pathlib.Path
    input_dir: pathlib.Path
    output_dir: pathlib.Path
    scratch: pathlib.Path
    scratch_names: tuple[str, ...]


@dataclasses.dataclass(frozen=True)
class MonitorLimits:
    """Registered construction and evaluation process-group stops."""

    rss_bytes: int = 32 << 30
    psi_full_avg10: float = 0.50
    swap_delta_bytes: int = 0
    progress_seconds: int = 1200
    wall_seconds: int = 7200

    @classmethod
    def for_phase(cls, phase: str) -> MonitorLimits:
        """Return the registered RSS cap for one exact phase."""

        if phase not in _PHASE_FLAGS:
            raise ValueError("V24 monitor phase differs")
        rss_bytes = (
            32 << 30
            if phase in {"train-witnesses", "build-postings"}
            else 3 << 30
        )
        return cls(rss_bytes=rss_bytes)


class AuthenticatedProgressMonitor:
    """Validate canonical monotonic completed-work snapshots for one phase."""

    def __init__(self, phase: str) -> None:
        if phase not in _MANIFEST_PHASES.values():
            raise ValueError("V24 progress phase differs")
        self._phase = phase
        self._sequence: int | None = None
        self._completed_units: int | None = None
        self._total_units: int | None = None

    def observe(self, raw: bytes) -> tuple[int, int, int]:
        """Accept one atomically replaced canonical progress snapshot."""

        try:
            value = json.loads(raw)
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise ValueError("V24 progress encoding differs") from error
        if (
            raw
            != json.dumps(value, separators=(",", ":"), sort_keys=True).encode()
            + b"\n"
            or type(value) is not dict  # noqa: E721
            or set(value) != {"completed_units", "phase", "sequence", "total_units"}
            or type(value["phase"]) is not str
            or type(value["sequence"]) is not int
            or type(value["completed_units"]) is not int
            or type(value["total_units"]) is not int
        ):
            raise ValueError("V24 progress schema differs")
        sequence = value["sequence"]
        completed = value["completed_units"]
        total = value["total_units"]
        if (
            value["phase"] != self._phase
            or sequence < 0
            or completed < 0
            or total <= 0
            or completed > total
        ):
            raise ValueError("V24 progress authority differs")
        if self._sequence is None:
            if sequence == 0 and completed != 0:
                raise ValueError("V24 progress root differs")
            self._total_units = total
        elif (
            sequence <= self._sequence
            or completed <= self._completed_units
            or total != self._total_units
        ):
            raise ValueError("V24 progress sequence differs")
        self._sequence = sequence
        self._completed_units = completed
        return sequence, completed, total


def _valid_sha256(value: object) -> bool:
    return (
        type(value) is str
        and len(value) == 64
        and all(character in _LOWER_HEX for character in value)
    )


def _sha256_file(path: pathlib.Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _validate_request_paths(request: PhaseRequest) -> None:
    paths = (
        request.executable,
        request.manifest,
        request.staging_receipt,
        request.input_dir,
        request.output_dir,
        request.scratch,
    )
    if request.phase not in _PHASE_FLAGS or any(not path.is_absolute() for path in paths):
        raise ValueError("V24 phase path or phase differs")
    if (
        request.executable.is_symlink()
        or not request.executable.is_file()
        or request.executable.stat().st_size != request.executable_bytes
        or not _valid_sha256(request.executable_sha256)
        or _sha256_file(request.executable) != request.executable_sha256
    ):
        raise ValueError("V24 executable authority differs")
    if (
        not _valid_sha256(request.manifest_sha256)
        or _sha256_file(request.manifest) != request.manifest_sha256
    ):
        raise ValueError("V24 manifest authority differs")
    if (
        manifest_phase(request.manifest, request.manifest_sha256)
        != _MANIFEST_PHASES[request.phase]
    ):
        raise ValueError("V24 manifest phase authority differs")
    if (
        request.manifest.is_symlink()
        or not request.manifest.is_file()
        or request.staging_receipt.is_symlink()
        or not request.staging_receipt.is_file()
        or request.input_dir.is_symlink()
        or not request.input_dir.is_dir()
        or request.output_dir.is_symlink()
        or not request.output_dir.is_dir()
        or request.scratch.is_symlink()
        or not request.scratch.is_dir()
        or any(request.output_dir.iterdir())
        or any(request.scratch.iterdir())
    ):
        raise ValueError("V24 phase local authority differs")


def build_phase_command(request: PhaseRequest) -> list[str]:
    """Return the direct binary argv for exactly one V24 phase."""

    _validate_request_paths(request)
    return [
        str(request.executable),
        "--manifest",
        str(request.manifest),
        "--input-dir",
        str(request.input_dir),
        "--output-dir",
        str(request.output_dir),
        _PHASE_FLAGS[request.phase],
        "--execute",
    ]


def offline_environment(
    scratch: pathlib.Path,
    ambient: Mapping[str, str] | None = None,
) -> dict[str, str]:
    """Construct the complete minimal environment for the local child."""

    source = os.environ if ambient is None else ambient
    environment = {
        "HOME": "/nonexistent",
        "LANG": "C",
        "LC_ALL": "C",
        "TMPDIR": str(scratch),
    }
    workers = source.get("RAYON_NUM_THREADS")
    if workers is not None:
        if not workers.isascii() or not workers.isdigit() or int(workers) <= 0:
            raise ValueError("RAYON_NUM_THREADS authority differs")
        environment["RAYON_NUM_THREADS"] = workers
    return environment


def classify_sample(
    *,
    limits: MonitorLimits,
    rss_bytes: int,
    psi_full_avg10: float,
    swap_delta_bytes: int,
    progress_age_seconds: float,
    wall_seconds: float,
) -> str | None:
    """Classify one operational sample using exact stop equality."""

    if rss_bytes >= limits.rss_bytes:
        return "rss-cap"
    if psi_full_avg10 >= limits.psi_full_avg10:
        return "psi-cap"
    if swap_delta_bytes > limits.swap_delta_bytes:
        return "swap-growth"
    if progress_age_seconds >= limits.progress_seconds:
        return "progress-gap"
    if wall_seconds >= limits.wall_seconds:
        return "wall-cap"
    return None


def cleanup_known_files(root: pathlib.Path, known_names: Sequence[str]) -> None:
    """Unlink only explicit regular-file basenames, then remove the empty root."""

    names = tuple(known_names)
    if (
        root.is_symlink()
        or not root.is_dir()
        or len(names) != len(set(names))
        or any(not name or pathlib.PurePath(name).name != name for name in names)
    ):
        raise ValueError("cleanup authority differs")
    if not {entry.name for entry in root.iterdir()}.issubset(set(names)):
        raise ValueError("unexpected cleanup entry")
    for name in names:
        path = root / name
        if path.exists():
            if path.is_symlink() or not path.is_file():
                raise ValueError("cleanup target differs")
            path.unlink()
    root.rmdir()


def _memory_psi_full_avg10(
    path: pathlib.Path = pathlib.Path("/proc/pressure/memory"),
) -> float:
    for line in path.read_text(encoding="ascii").splitlines():
        if line.startswith("full "):
            try:
                fields = dict(field.split("=", 1) for field in line.split()[1:])
                value = float(fields["avg10"])
            except (KeyError, ValueError) as error:
                raise RuntimeError("memory PSI sample differs") from error
            if not math.isfinite(value) or value < 0.0:
                raise RuntimeError("memory PSI sample differs")
            return value
    raise RuntimeError("memory PSI sample is absent")


def _swap_used_bytes() -> int:
    values: dict[str, int] = {}
    for line in pathlib.Path("/proc/meminfo").read_text(encoding="ascii").splitlines():
        key, value, *_ = line.split()
        if key in {"SwapTotal:", "SwapFree:"}:
            values[key] = int(value) * 1024
    return values["SwapTotal:"] - values["SwapFree:"]


def _process_group_rss_bytes(pgid: int) -> int:
    pages = 0
    for entry in pathlib.Path("/proc").iterdir():
        if not entry.name.isdigit():
            continue
        try:
            if os.getpgid(int(entry.name)) == pgid:
                pages += int((entry / "statm").read_text(encoding="ascii").split()[1])
        except (FileNotFoundError, PermissionError, ProcessLookupError, ValueError):
            continue
    return pages * os.sysconf("SC_PAGE_SIZE")


def _terminate_process_group(pid: int, grace_seconds: float) -> int:
    if grace_seconds < 0:
        raise ValueError("termination grace differs")

    def exists() -> bool:
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
    while time.monotonic() < deadline:
        if leader_status is None:
            completed, status = os.waitpid(pid, os.WNOHANG)
            if completed == pid:
                leader_status = status
        if leader_status is not None and not exists():
            return leader_status
        time.sleep(min(0.05, max(0.001, grace_seconds / 10)))
    try:
        os.killpg(pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    if leader_status is None:
        _, leader_status = os.waitpid(pid, 0)
    clearance = time.monotonic() + 1.0
    while exists() and time.monotonic() < clearance:
        time.sleep(0.01)
    if exists():
        raise RuntimeError("V24 process group did not terminate")
    return leader_status


def monitor_process_group(
    pid: int,
    limits: MonitorLimits,
    *,
    sample_interval_seconds: float = 5.0,
    term_grace_seconds: float = 15.0,
    progress_path: pathlib.Path | None = None,
    progress_phase: str | None = None,
) -> tuple[int, str | None]:
    """Monitor one original process group and never restart it."""

    if pid <= 1:
        raise ValueError("invalid process group leader")
    if (progress_path is None) != (progress_phase is None):
        raise ValueError("V24 progress path and phase differ")
    started = time.monotonic()
    last_progress = started
    initial_swap = _swap_used_bytes()
    progress_monitor = (
        None
        if progress_phase is None
        else AuthenticatedProgressMonitor(progress_phase)
    )
    progress_digest: str | None = None

    def observe_progress() -> bool:
        nonlocal last_progress, progress_digest
        if progress_path is None:
            return True
        if not progress_path.exists():
            return False
        if progress_path.is_symlink() or not progress_path.is_file():
            raise ValueError("V24 progress file authority differs")
        raw = progress_path.read_bytes()
        digest = hashlib.sha256(raw).hexdigest()
        if digest != progress_digest:
            assert progress_monitor is not None
            progress_monitor.observe(raw)
            progress_digest = digest
            last_progress = time.monotonic()
        return True

    try:
        while True:
            completed, status = os.waitpid(pid, os.WNOHANG)
            if completed == pid:
                if progress_path is not None:
                    try:
                        if not observe_progress():
                            return os.waitstatus_to_exitcode(status), "progress-authority"
                    except (OSError, ValueError):
                        return os.waitstatus_to_exitcode(status), "progress-authority"
                return os.waitstatus_to_exitcode(status), None
            if progress_path is not None and progress_path.exists():
                try:
                    observe_progress()
                except (OSError, ValueError):
                    stopped = _terminate_process_group(pid, term_grace_seconds)
                    return os.waitstatus_to_exitcode(stopped), "progress-authority"
            now = time.monotonic()
            reason = classify_sample(
                limits=limits,
                rss_bytes=_process_group_rss_bytes(pid),
                psi_full_avg10=_memory_psi_full_avg10(),
                swap_delta_bytes=max(0, _swap_used_bytes() - initial_swap),
                progress_age_seconds=now - last_progress,
                wall_seconds=now - started,
            )
            if reason is not None:
                stopped = _terminate_process_group(pid, term_grace_seconds)
                return os.waitstatus_to_exitcode(stopped), reason
            time.sleep(sample_interval_seconds)
    except BaseException:
        try:
            _terminate_process_group(pid, term_grace_seconds)
        except ChildProcessError:
            pass
        raise


def run_phase(request: PhaseRequest, limits: MonitorLimits | None = None) -> int:
    """Authenticate, run, monitor, and reauthenticate one direct V24 phase."""

    command = build_phase_command(request)
    try:
        validate_inventory(
            request.manifest,
            request.manifest_sha256,
            request.input_dir,
            request.staging_receipt,
        )
        process = subprocess.Popen(  # noqa: S603
            command,
            start_new_session=True,
            env=offline_environment(request.scratch),
        )
        status, reason = monitor_process_group(
            process.pid,
            limits or MonitorLimits.for_phase(request.phase),
            progress_path=request.output_dir / "progress.json",
            progress_phase=_MANIFEST_PHASES[request.phase],
        )
        process.returncode = status
        validate_inventory(
            request.manifest,
            request.manifest_sha256,
            request.input_dir,
            request.staging_receipt,
        )
        if reason is not None:
            raise RuntimeError(f"V24 phase stopped: {reason}")
        if status != 0:
            raise RuntimeError(f"V24 phase exited {status}")
        return status
    finally:
        cleanup_known_files(request.scratch, request.scratch_names)


def parse_args(arguments: Sequence[str] | None = None) -> PhaseRequest:
    """Parse one explicit direct-run request without ambient authority."""

    parser = argparse.ArgumentParser(allow_abbrev=False)
    parser.add_argument("--phase", required=True, choices=tuple(_PHASE_FLAGS))
    parser.add_argument("--executable", required=True, type=pathlib.Path)
    parser.add_argument("--executable-sha256", required=True)
    parser.add_argument("--executable-bytes", required=True, type=int)
    parser.add_argument("--manifest", required=True, type=pathlib.Path)
    parser.add_argument("--manifest-sha256", required=True)
    parser.add_argument("--staging-receipt", required=True, type=pathlib.Path)
    parser.add_argument("--input-dir", required=True, type=pathlib.Path)
    parser.add_argument("--output-dir", required=True, type=pathlib.Path)
    parser.add_argument("--scratch", required=True, type=pathlib.Path)
    parser.add_argument("--scratch-name", action="append", default=[])
    supplied = list(sys.argv[1:] if arguments is None else arguments)
    singleton_flags = {
        "--phase",
        "--executable",
        "--executable-sha256",
        "--executable-bytes",
        "--manifest",
        "--manifest-sha256",
        "--staging-receipt",
        "--input-dir",
        "--output-dir",
        "--scratch",
    }
    duplicated = sorted(flag for flag in singleton_flags if supplied.count(flag) > 1)
    if duplicated:
        parser.error("V24 runner flag is duplicated: " + ",".join(duplicated))
    parsed = parser.parse_args(supplied)
    return PhaseRequest(
        phase=parsed.phase,
        executable=parsed.executable,
        executable_sha256=parsed.executable_sha256,
        executable_bytes=parsed.executable_bytes,
        manifest=parsed.manifest,
        manifest_sha256=parsed.manifest_sha256,
        staging_receipt=parsed.staging_receipt,
        input_dir=parsed.input_dir,
        output_dir=parsed.output_dir,
        scratch=parsed.scratch,
        scratch_names=tuple(parsed.scratch_name),
    )


def main(arguments: Sequence[str] | None = None) -> int:
    """Run exactly one direct V24 phase from explicit CLI authority."""

    return run_phase(parse_args(arguments))


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:  # noqa: BLE001
        print(error, file=sys.stderr)
        raise SystemExit(1) from error
