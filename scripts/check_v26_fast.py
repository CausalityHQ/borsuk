#!/usr/bin/env python3
"""Run V26 checks in fail-fast order, with full assurance opt-in only."""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Sequence

ROOT = Path(__file__).resolve().parents[1]


def fast_commands(python_executable: str) -> list[list[str]]:
    """Return the production-representative checks used on every V26 change."""
    return [
        ["cargo", "test", "-p", "borsuk-v26", "--lib", "v26_", "--", "--nocapture"],
        [
            "cargo",
            "test",
            "-p",
            "borsuk-v26",
            "--example",
            "v26_page_layout",
            "v26_",
            "--",
            "--nocapture",
        ],
        [
            python_executable,
            "-m",
            "unittest",
            "scripts.test_run_v26_page_layout",
            "scripts.test_launch_v26_page_layout_spot",
        ],
        ["cargo", "fmt", "--all", "--", "--check"],
        ["git", "diff", "--check"],
    ]


def milestone_commands(python_executable: str) -> list[list[str]]:
    """Return focused checks followed by the deliberately expensive assurance."""
    return fast_commands(python_executable) + [
        [
            "cargo",
            "clippy",
            "--locked",
            "--workspace",
            "--all-targets",
            "--",
            "-D",
            "warnings",
        ],
        ["cargo", "test", "--locked", "--workspace", "--all-targets"],
    ]


def run_gate(commands: Sequence[Sequence[str]], root: Path = ROOT) -> int:
    """Run commands serially and return immediately on the first failure."""
    environment = os.environ.copy()
    environment.setdefault("CARGO_BUILD_JOBS", "2")
    gate_started = time.monotonic()
    for index, command in enumerate(commands, start=1):
        started = time.monotonic()
        print(f"v26-gate start={index}/{len(commands)} command={' '.join(command)}")
        completed = subprocess.run(command, cwd=root, env=environment, check=False)
        elapsed = time.monotonic() - started
        print(
            f"v26-gate terminal={index}/{len(commands)} "
            f"status={completed.returncode} elapsed_seconds={elapsed:.3f}"
        )
        if completed.returncode != 0:
            print(
                f"v26-gate result=failed failed_step={index} "
                f"elapsed_seconds={time.monotonic() - gate_started:.3f}",
                file=sys.stderr,
            )
            return completed.returncode
    print(
        f"v26-gate result=passed steps={len(commands)} "
        f"elapsed_seconds={time.monotonic() - gate_started:.3f}"
    )
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--milestone",
        action="store_true",
        help="append strict workspace Clippy and the full workspace test suite",
    )
    arguments = parser.parse_args(argv)
    commands = (
        milestone_commands(sys.executable)
        if arguments.milestone
        else fast_commands(sys.executable)
    )
    return run_gate(commands)


if __name__ == "__main__":
    raise SystemExit(main())
