#!/usr/bin/env python3
"""Fail-closed S3 isolation and terminal-marker-safe benchmark publication."""

from __future__ import annotations

import argparse
import subprocess
from pathlib import Path
from urllib.parse import urlsplit

TERMINAL_MARKERS = {
    "CELL_COMPLETE",
    "CELL_FAILED",
    "LOGICAL_CELL_ROUTING_COMPLETE",
    "LOGICAL_CELL_ROUTING_FAILED",
}


def aws_command(profile: str | None) -> list[str]:
    command = ["aws"]
    if profile:
        command.extend(["--profile", profile])
    return command


def run(command: list[str], timeout_seconds: int) -> subprocess.CompletedProcess[str]:
    try:
        return subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
        )
    except subprocess.TimeoutExpired as error:
        raise RuntimeError(
            f"AWS operation exceeded {timeout_seconds}s: {' '.join(command)}"
        ) from error


def assert_empty(uri: str, profile: str | None, timeout_seconds: int) -> None:
    location = urlsplit(uri)
    if location.scheme != "s3" or not location.netloc:
        raise ValueError(f"invalid S3 benchmark prefix: {uri}")
    prefix = location.path.lstrip("/").rstrip("/")
    if prefix:
        prefix += "/"
    result = run(
        [
            *aws_command(profile),
            "s3api",
            "list-objects-v2",
            "--bucket",
            location.netloc,
            "--prefix",
            prefix,
            "--max-keys",
            "1",
            "--query",
            "KeyCount",
            "--output",
            "text",
        ],
        timeout_seconds,
    )
    if result.returncode != 0:
        detail = result.stderr.strip() or f"exit {result.returncode}"
        raise RuntimeError(f"could not list benchmark prefix {uri}: {detail}")
    if result.stdout.strip() != "0":
        raise RuntimeError(f"refusing to reuse non-empty benchmark prefix: {uri}")


def publish(
    source: Path,
    destination: str,
    profile: str | None,
    timeout_seconds: int,
) -> None:
    if not source.is_dir():
        raise RuntimeError(f"benchmark output does not exist: {source}")
    base = aws_command(profile)
    sync = run(
        [
            *base,
            "s3",
            "sync",
            "--only-show-errors",
            "--exclude",
            "*/CELL_COMPLETE",
            "--exclude",
            "*/CELL_FAILED",
            "--exclude",
            "LOGICAL_CELL_ROUTING_COMPLETE",
            "--exclude",
            "LOGICAL_CELL_ROUTING_FAILED",
            str(source),
            destination,
        ],
        timeout_seconds,
    )
    if sync.returncode != 0:
        raise RuntimeError(sync.stderr.strip() or "benchmark artifact sync failed")
    markers = sorted(
        (path for path in source.rglob("*") if path.name in TERMINAL_MARKERS),
        key=lambda path: (-len(path.relative_to(source).parts), path.as_posix()),
    )
    for marker in markers:
        relative = marker.relative_to(source).as_posix()
        upload = run(
            [
                *base,
                "s3",
                "cp",
                "--only-show-errors",
                str(marker),
                f"{destination.rstrip('/')}/{relative}",
            ],
            timeout_seconds,
        )
        if upload.returncode != 0:
            raise RuntimeError(
                upload.stderr.strip() or f"terminal marker upload failed: {relative}"
            )


def main() -> int:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)
    empty = subparsers.add_parser("assert-empty")
    empty.add_argument("--uri", required=True)
    empty.add_argument("--profile")
    empty.add_argument("--timeout-seconds", type=int, default=60)
    publication = subparsers.add_parser("publish")
    publication.add_argument("--source", required=True, type=Path)
    publication.add_argument("--destination", required=True)
    publication.add_argument("--profile")
    publication.add_argument("--timeout-seconds", type=int, default=900)
    args = parser.parse_args()
    try:
        if args.command == "assert-empty":
            assert_empty(args.uri, args.profile, args.timeout_seconds)
        else:
            publish(args.source, args.destination, args.profile, args.timeout_seconds)
    except (OSError, RuntimeError, ValueError) as error:
        parser.error(str(error))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
