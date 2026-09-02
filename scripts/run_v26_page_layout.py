#!/usr/bin/env python3
"""Fail-closed process monitor for the offline V26 layout executable."""

from __future__ import annotations

import dataclasses
import json
import math
import os
import pathlib
import signal
import subprocess
import time
from collections.abc import Mapping, Sequence


@dataclasses.dataclass(frozen=True)
class MonitorLimits:
    wall_seconds: float
    rss_bytes: int
    psi_full_avg10: float
    swap_growth_bytes: int
    progress_seconds: float
    terminate_grace_seconds: float


@dataclasses.dataclass(frozen=True)
class MonitorReceipt:
    reason: str
    exit_code: int
    elapsed_seconds: float
    cpu_ns: int
    peak_rss_bytes: int
    peak_psi_full_avg10: float
    swap_start_bytes: int
    swap_end_bytes: int


def offline_environment(
    scratch: pathlib.Path, source: Mapping[str, str]
) -> dict[str, str]:
    blocked = (
        "aws_",
        "boto_",
        "http_proxy",
        "https_proxy",
        "all_proxy",
        "no_proxy",
    )
    environment = {
        key: value
        for key, value in source.items()
        if not key.lower().startswith(blocked)
    }
    environment["TMPDIR"] = str(scratch)
    environment["NO_COLOR"] = "1"
    return environment


def cleanup_known_files(root: pathlib.Path, known_names: Sequence[str]) -> None:
    for name in known_names:
        path = pathlib.Path(name)
        if path.name != name or name in {"", ".", ".."}:
            raise ValueError("V26 cleanup name differs")
        target = root / name
        if target.exists() or target.is_symlink():
            target.unlink()
    remaining = sorted(path.name for path in root.iterdir())
    if remaining:
        raise ValueError(f"V26 scratch contains unknown files: {remaining}")


def _process_group_rss_bytes(pgid: int) -> int:
    total = 0
    for status in pathlib.Path("/proc").glob("[0-9]*/status"):
        try:
            fields = {}
            for line in status.read_text(encoding="utf-8").splitlines():
                key, separator, value = line.partition(":")
                if separator:
                    fields[key] = value.strip()
            pid = int(status.parent.name)
            if os.getpgid(pid) == pgid:
                total += int(fields.get("VmRSS", "0 kB").split()[0]) * 1024
        except (FileNotFoundError, ProcessLookupError, PermissionError, ValueError):
            continue
    return total


def _process_group_cpu_ns(pgid: int) -> int:
    ticks = 0
    ticks_per_second = os.sysconf("SC_CLK_TCK")
    for stat in pathlib.Path("/proc").glob("[0-9]*/stat"):
        try:
            encoded = stat.read_text(encoding="utf-8")
            closing = encoded.rfind(")")
            if closing < 0:
                continue
            fields = encoded[closing + 2 :].split()
            pid = int(stat.parent.name)
            if os.getpgid(pid) == pgid:
                ticks += int(fields[11]) + int(fields[12])
        except (FileNotFoundError, ProcessLookupError, PermissionError, ValueError):
            continue
    return ticks * 1_000_000_000 // ticks_per_second


def _psi_full_avg10() -> float:
    try:
        lines = pathlib.Path("/proc/pressure/memory").read_text(
            encoding="utf-8"
        ).splitlines()
    except FileNotFoundError:
        return 0.0
    for line in lines:
        if line.startswith("full "):
            for field in line.split()[1:]:
                if field.startswith("avg10="):
                    return float(field.removeprefix("avg10="))
    return 0.0


def _swap_used_bytes() -> int:
    fields = {}
    for line in pathlib.Path("/proc/meminfo").read_text(encoding="utf-8").splitlines():
        key, _, value = line.partition(":")
        fields[key] = int(value.strip().split()[0]) * 1024
    return fields.get("SwapTotal", 0) - fields.get("SwapFree", 0)


def _progress_sequence(path: pathlib.Path) -> object:
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return payload.get("sequence") if isinstance(payload, dict) else None


def _stop_group(process: subprocess.Popen[object], grace_seconds: float) -> None:
    if process.poll() is not None:
        return
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    try:
        process.wait(timeout=grace_seconds)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        process.wait()


def monitor_process_group(
    process: subprocess.Popen[object],
    limits: MonitorLimits,
    *,
    progress_path: pathlib.Path,
) -> MonitorReceipt:
    started = time.monotonic()
    last_progress_at = started
    last_sequence = _progress_sequence(progress_path)
    swap_start = _swap_used_bytes()
    peak_rss = 0
    peak_cpu_ns = 0
    peak_psi = 0.0
    reason = "process-exit"
    while process.poll() is None:
        now = time.monotonic()
        rss = _process_group_rss_bytes(process.pid)
        cpu_ns = _process_group_cpu_ns(process.pid)
        psi = _psi_full_avg10()
        swap = _swap_used_bytes()
        peak_rss = max(peak_rss, rss)
        peak_cpu_ns = max(peak_cpu_ns, cpu_ns)
        peak_psi = max(peak_psi, psi)
        sequence = _progress_sequence(progress_path)
        if sequence != last_sequence:
            last_sequence = sequence
            last_progress_at = now
        if now - started >= limits.wall_seconds:
            reason = "wall-time-stop"
        elif rss > limits.rss_bytes:
            reason = "rss-stop"
        elif psi > limits.psi_full_avg10:
            reason = "psi-stop"
        elif swap - swap_start > limits.swap_growth_bytes:
            reason = "swap-stop"
        elif now - last_progress_at >= limits.progress_seconds:
            reason = "progress-stop"
        else:
            time.sleep(0.02)
            continue
        _stop_group(process, limits.terminate_grace_seconds)
        break
    exit_code = process.wait()
    swap_end = _swap_used_bytes()
    return MonitorReceipt(
        reason=reason,
        exit_code=exit_code,
        elapsed_seconds=time.monotonic() - started,
        cpu_ns=peak_cpu_ns,
        peak_rss_bytes=peak_rss,
        peak_psi_full_avg10=peak_psi,
        swap_start_bytes=swap_start,
        swap_end_bytes=swap_end,
    )


def seal_v26_layout_receipt(
    build_output: Mapping[str, object], monitor: MonitorReceipt
) -> bytes:
    expected = {
        "authority",
        "inputs",
        "outputs",
        "row_count",
        "leaves_per_tree",
        "page_count",
        "projection_steps",
        "worker_count",
    }
    if (
        set(build_output) != expected
        or monitor.reason != "process-exit"
        or monitor.exit_code != 0
        or monitor.elapsed_seconds <= 0.0
        or monitor.cpu_ns <= 0
        or monitor.peak_rss_bytes <= 0
        or monitor.peak_psi_full_avg10 > 0.5
        or monitor.swap_end_bytes != monitor.swap_start_bytes
    ):
        raise ValueError("V26 layout process cannot seal a receipt")
    receipt = dict(build_output)
    receipt.update(
        {
            "elapsed_ns": round(monitor.elapsed_seconds * 1_000_000_000),
            "cpu_ns": monitor.cpu_ns,
            "peak_rss_bytes": monitor.peak_rss_bytes,
            "peak_psi_full_avg10_milli_percent": math.ceil(
                monitor.peak_psi_full_avg10 * 1000
            ),
            "swap_start_bytes": monitor.swap_start_bytes,
            "swap_end_bytes": monitor.swap_end_bytes,
            "query_role_opens": 0,
            "page_body_reads": 0,
            "claim_eligible": False,
        }
    )
    return (
        json.dumps(receipt, sort_keys=True, separators=(",", ":"), allow_nan=False)
        + "\n"
    ).encode("utf-8")
