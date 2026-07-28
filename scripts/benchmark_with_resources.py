#!/usr/bin/env python3
"""Run a command while sampling Linux process CPU, memory, I/O, and cache disk use."""

from __future__ import annotations

import argparse
import csv
import os
import resource
import subprocess
import sys
import time
from pathlib import Path


def parse_proc_stat(value: str) -> tuple[int, int, int]:
    """Return cumulative CPU ticks, virtual bytes, and RSS pages from /proc/PID/stat."""
    fields = value[value.rfind(")") + 2 :].split()
    return int(fields[11]) + int(fields[12]), int(fields[20]), int(fields[21])


def parse_proc_parent(value: str) -> int:
    """Return the parent pid without being confused by spaces or ')' in comm."""
    fields = value[value.rfind(")") + 2 :].split()
    return int(fields[1])


def parse_proc_io(value: str) -> tuple[int, int]:
    fields = {
        line.split(":", 1)[0]: int(line.split(":", 1)[1]) for line in value.splitlines()
    }
    return fields.get("read_bytes", 0), fields.get("write_bytes", 0)


def parse_proc_net_dev(value: str) -> tuple[int, int]:
    """Return aggregate non-loopback receive/transmit bytes from `/proc/net/dev`."""
    received = transmitted = 0
    for line in value.splitlines():
        if ":" not in line:
            continue
        interface, counters = line.split(":", 1)
        if interface.strip() == "lo":
            continue
        fields = counters.split()
        if len(fields) < 9:
            continue
        received += int(fields[0])
        transmitted += int(fields[8])
    return received, transmitted


def read_network_bytes(path: Path = Path("/proc/net/dev")) -> tuple[int, int]:
    try:
        return parse_proc_net_dev(path.read_text())
    except (FileNotFoundError, PermissionError, ValueError):
        return 0, 0


def parse_ps_time(value: str) -> float:
    """Parse portable ps TIME values such as MM:SS.hh or DD-HH:MM:SS.hh."""
    days = 0
    if "-" in value:
        day, value = value.split("-", 1)
        days = int(day)
    parts = value.split(":")
    if len(parts) == 2:
        hours = 0
        minutes, seconds = parts
    elif len(parts) == 3:
        hours, minutes, seconds = parts
    else:
        raise ValueError(f"unsupported ps TIME value: {value}")
    return days * 86400 + int(hours) * 3600 + int(minutes) * 60 + float(seconds)


def parse_ps_process_tree(
    value: str, pid: int, clock_ticks: int | None = None
) -> tuple[int, int, int, int, int] | None:
    """Aggregate the portable `ps` fallback; physical I/O is unavailable there."""
    clock_ticks = clock_ticks or os.sysconf("SC_CLK_TCK")
    processes: dict[int, tuple[int, float, int, int]] = {}
    for line in value.splitlines():
        fields = line.split()
        if len(fields) != 5:
            continue
        try:
            child, parent = int(fields[0]), int(fields[1])
            cpu_seconds = parse_ps_time(fields[2])
            rss_bytes = int(fields[3]) * 1024
            vms_bytes = int(fields[4]) * 1024
        except ValueError:
            continue
        processes[child] = (parent, cpu_seconds, rss_bytes, vms_bytes)
    if pid not in processes:
        return None
    descendants = {pid}
    changed = True
    while changed:
        changed = False
        for child, (parent, *_metrics) in processes.items():
            if child not in descendants and parent in descendants:
                descendants.add(child)
                changed = True
    cpu_seconds = sum(processes[child][1] for child in descendants)
    rss_bytes = sum(processes[child][2] for child in descendants)
    vms_bytes = sum(processes[child][3] for child in descendants)
    return round(cpu_seconds * clock_ticks), rss_bytes, vms_bytes, 0, 0


def sample_process_tree_ps(pid: int) -> tuple[int, int, int, int, int] | None:
    """Portable fallback for macOS/BSD hosts without Linux `/proc`."""
    try:
        output = subprocess.run(
            ["ps", "-axo", "pid=,ppid=,time=,rss=,vsz="],
            check=True,
            capture_output=True,
            text=True,
        ).stdout
    except (FileNotFoundError, subprocess.CalledProcessError):
        return None
    return parse_ps_process_tree(output, pid)


def directory_bytes(path: Path | None) -> int:
    if path is None or not path.exists():
        return 0
    total = 0
    for root, _, names in os.walk(path):
        for name in names:
            try:
                total += (Path(root) / name).stat().st_size
            except FileNotFoundError:
                pass
    return total


def sample_process_tree(
    pid: int, proc_root: Path = Path("/proc")
) -> tuple[int, int, int, int, int] | None:
    """Aggregate CPU, memory, and physical I/O for a launcher and descendants."""
    processes: dict[int, tuple[int, int, int, int, int, int]] = {}
    try:
        entries = list(proc_root.iterdir())
    except (FileNotFoundError, PermissionError):
        return sample_process_tree_ps(pid) if proc_root == Path("/proc") else None
    for entry in entries:
        if not entry.name.isdigit():
            continue
        try:
            stat = (entry / "stat").read_text()
            ticks, vms_bytes, rss_pages = parse_proc_stat(stat)
            parent = parse_proc_parent(stat)
            read_bytes, write_bytes = parse_proc_io((entry / "io").read_text())
        except (FileNotFoundError, PermissionError, ProcessLookupError, ValueError):
            continue
        processes[int(entry.name)] = (
            parent,
            ticks,
            rss_pages,
            vms_bytes,
            read_bytes,
            write_bytes,
        )
    if pid not in processes:
        return sample_process_tree_ps(pid) if proc_root == Path("/proc") else None

    descendants = {pid}
    changed = True
    while changed:
        changed = False
        for child, (parent, *_metrics) in processes.items():
            if child not in descendants and parent in descendants:
                descendants.add(child)
                changed = True

    ticks = rss_pages = vms_bytes = read_bytes = write_bytes = 0
    for process in descendants:
        _, child_ticks, child_rss, child_vms, child_read, child_write = processes[
            process
        ]
        ticks += child_ticks
        rss_pages += child_rss
        vms_bytes += child_vms
        read_bytes += child_read
        write_bytes += child_write
    return (
        ticks,
        rss_pages * os.sysconf("SC_PAGE_SIZE"),
        vms_bytes,
        read_bytes,
        write_bytes,
    )


def sample(pid: int, cache_dir: Path | None) -> tuple[int, int, int, int, int] | None:
    del cache_dir
    return sample_process_tree(pid)


def run(
    command: list[str],
    output: Path,
    cache_dir: Path | None,
    scratch_dir: Path | None,
    interval_ms: int,
    cache_interval_ms: int,
) -> int:
    output.parent.mkdir(parents=True, exist_ok=True)
    child_env = os.environ.copy()
    if scratch_dir is not None:
        scratch_dir.mkdir(parents=True, exist_ok=True)
        child_env["TMPDIR"] = str(scratch_dir.resolve())
        child_env["BORSUK_BUILD_SCRATCH_DIR"] = str(scratch_dir.resolve())
    usage_before = resource.getrusage(resource.RUSAGE_CHILDREN)
    process = subprocess.Popen(command, env=child_env)
    started = time.monotonic()
    previous_time = started
    previous_ticks: int | None = None
    clock_ticks = os.sysconf("SC_CLK_TCK")
    cache_bytes = 0
    scratch_bytes = 0
    next_cache_sample = started
    network_receive_base, network_transmit_base = read_network_bytes()
    peak_rss_bytes = 0
    peak_vms_bytes = 0
    last_read_bytes = 0
    last_write_bytes = 0

    with output.open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "elapsed_ms",
                "cpu_percent",
                "rss_bytes",
                "vms_bytes",
                "process_read_bytes",
                "process_write_bytes",
                "cache_disk_bytes",
                "scratch_disk_bytes",
                "network_receive_bytes",
                "network_transmit_bytes",
                "child_cpu_seconds",
                "child_max_rss_bytes",
            ]
        )
        while process.poll() is None:
            now = time.monotonic()
            current = sample(process.pid, cache_dir)
            if current is not None:
                ticks, rss_bytes, vms_bytes, read_bytes, write_bytes = current
                peak_rss_bytes = max(peak_rss_bytes, rss_bytes)
                peak_vms_bytes = max(peak_vms_bytes, vms_bytes)
                last_read_bytes = max(last_read_bytes, read_bytes)
                last_write_bytes = max(last_write_bytes, write_bytes)
                if now >= next_cache_sample:
                    cache_bytes = directory_bytes(cache_dir)
                    scratch_bytes = directory_bytes(scratch_dir)
                    next_cache_sample = now + cache_interval_ms / 1000.0
                elapsed = max(now - previous_time, 1e-9)
                network_receive, network_transmit = read_network_bytes()
                cpu_percent = (
                    0.0
                    if previous_ticks is None
                    else max(ticks - previous_ticks, 0) / clock_ticks / elapsed * 100.0
                )
                writer.writerow(
                    [
                        round((now - started) * 1000),
                        f"{cpu_percent:.3f}",
                        rss_bytes,
                        vms_bytes,
                        read_bytes,
                        write_bytes,
                        cache_bytes,
                        scratch_bytes,
                        max(network_receive - network_receive_base, 0),
                        max(network_transmit - network_transmit_base, 0),
                        "",
                        "",
                    ]
                )
                handle.flush()
                previous_time = now
                previous_ticks = ticks
            time.sleep(interval_ms / 1000.0)
        return_code = process.wait()
        usage_after = resource.getrusage(resource.RUSAGE_CHILDREN)
        exact_child_cpu_seconds = max(
            (usage_after.ru_utime - usage_before.ru_utime)
            + (usage_after.ru_stime - usage_before.ru_stime),
            0.0,
        )
        exact_child_max_rss_bytes = int(usage_after.ru_maxrss)
        if sys.platform != "darwin":
            exact_child_max_rss_bytes *= 1024
        finished = time.monotonic()
        network_receive, network_transmit = read_network_bytes()
        writer.writerow(
            [
                round((finished - started) * 1000),
                "0.000",
                peak_rss_bytes,
                peak_vms_bytes,
                last_read_bytes,
                last_write_bytes,
                directory_bytes(cache_dir),
                directory_bytes(scratch_dir),
                max(network_receive - network_receive_base, 0),
                max(network_transmit - network_transmit_base, 0),
                f"{exact_child_cpu_seconds:.9f}",
                exact_child_max_rss_bytes,
            ]
        )
        handle.flush()
    return return_code


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--cache-dir", type=Path)
    parser.add_argument("--scratch-dir", type=Path)
    parser.add_argument("--interval-ms", type=int, default=100)
    parser.add_argument("--cache-interval-ms", type=int, default=1000)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    command = args.command[1:] if args.command[:1] == ["--"] else args.command
    if not command:
        parser.error("a command is required after --")
    if args.interval_ms <= 0:
        parser.error("--interval-ms must be greater than zero")
    if args.cache_interval_ms <= 0:
        parser.error("--cache-interval-ms must be greater than zero")
    return run(
        command,
        args.output,
        args.cache_dir,
        args.scratch_dir,
        args.interval_ms,
        args.cache_interval_ms,
    )


if __name__ == "__main__":
    sys.exit(main())
