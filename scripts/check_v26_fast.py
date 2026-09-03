#!/usr/bin/env python3
"""Run V26 checks in fail-fast order, with full assurance opt-in only."""

from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
import time
from pathlib import Path
from typing import Sequence

ROOT = Path(__file__).resolve().parents[1]
EMPTY_TEST_SELECTION_EXIT = 65
_CARGO_TEST_COUNT = re.compile(
    r"(?:cargo test:\s*|test result: [^.]*\.\s*)(\d+) passed"
)


def smoke_commands(python_executable: str) -> list[list[str]]:
    """Return the seconds-long contract gate used during implementation."""
    return [
        [python_executable, "-m", "unittest", "scripts.test_check_v26_fast"],
        [
            "cargo",
            "test",
            "-p",
            "borsuk-v26",
            "--lib",
            "v26_fast_smoke_",
            "--",
            "--nocapture",
        ],
        ["cargo", "fmt", "--all", "--", "--check"],
        ["git", "diff", "--check"],
    ]


def affected_commands(python_executable: str) -> list[list[str]]:
    """Return all focused V26 checks used at a stable implementation boundary."""
    return [
        [python_executable, "-m", "unittest", "scripts.test_check_v26_fast"],
        [
            "cargo",
            "test",
            "-p",
            "borsuk-v26",
            "--lib",
            "v26_fast_",
            "--",
            "--nocapture",
        ],
        [
            "cargo",
            "test",
            "-p",
            "borsuk-v26",
            "--lib",
            "v26_pq4_",
            "--",
            "--nocapture",
        ],
        [
            "cargo",
            "test",
            "-p",
            "borsuk-v26",
            "--lib",
            "v26_release_contract_",
            "--",
            "--nocapture",
        ],
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
            "cargo",
            "test",
            "-p",
            "borsuk-v26",
            "--example",
            "v26_pq16_serving_build",
            "v26_",
            "--",
            "--nocapture",
        ],
        [
            "cargo",
            "test",
            "-p",
            "borsuk-v26",
            "--example",
            "v26_pq16_serving",
            "v26_",
            "--",
            "--nocapture",
        ],
        [
            "cargo",
            "test",
            "-p",
            "borsuk-v26",
            "--example",
            "v26_simhash_preflight",
            "v26_",
            "--",
            "--nocapture",
        ],
        [
            "cargo",
            "test",
            "-p",
            "borsuk-v26",
            "--example",
            "v26_dual_pq_key_preflight",
            "v26_",
            "--",
            "--nocapture",
        ],
        [
            "cargo",
            "test",
            "-p",
            "borsuk-v26",
            "--example",
            "v26_pq4_fast_build",
            "v26_",
            "--",
            "--nocapture",
        ],
        [
            "cargo",
            "test",
            "-p",
            "borsuk-v26",
            "--example",
            "v26_pq4_fast_quality",
            "v26_",
            "--",
            "--nocapture",
        ],
        [
            "cargo",
            "test",
            "-p",
            "borsuk-v26",
            "--example",
            "v26_pq4_fast_serving",
            "v26_",
            "--",
            "--nocapture",
        ],
        [
            "cargo",
            "test",
            "-p",
            "borsuk-v26",
            "--example",
            "v26_pq4_fast_holdout",
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
    return affected_commands(python_executable) + [
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
        process = subprocess.Popen(
            command,
            cwd=root,
            env=environment,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        output: list[str] = []
        assert process.stdout is not None
        with process.stdout:
            for line in process.stdout:
                print(line, end="")
                output.append(line)
        status = process.wait()
        is_cargo_test = len(command) > 1 and command[0] == "cargo" and command[1] == "test"
        if status == 0 and is_cargo_test:
            executed = sum(
                int(match.group(1))
                for match in _CARGO_TEST_COUNT.finditer("".join(output))
            )
            if executed == 0:
                print(
                    "v26-gate error=cargo-test-selected-zero-tests",
                    file=sys.stderr,
                )
                status = EMPTY_TEST_SELECTION_EXIT
        elapsed = time.monotonic() - started
        print(
            f"v26-gate terminal={index}/{len(commands)} "
            f"status={status} elapsed_seconds={elapsed:.3f}"
        )
        if status != 0:
            print(
                f"v26-gate result=failed failed_step={index} "
                f"elapsed_seconds={time.monotonic() - gate_started:.3f}",
                file=sys.stderr,
            )
            return status
    print(
        f"v26-gate result=passed steps={len(commands)} "
        f"elapsed_seconds={time.monotonic() - gate_started:.3f}"
    )
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    layer = parser.add_mutually_exclusive_group()
    layer.add_argument(
        "--affected",
        action="store_true",
        help="run every focused V26 component gate",
    )
    layer.add_argument(
        "--milestone",
        action="store_true",
        help="run affected checks, strict Clippy, and the full workspace suite",
    )
    arguments = parser.parse_args(argv)
    if arguments.milestone:
        commands = milestone_commands(sys.executable)
    elif arguments.affected:
        commands = affected_commands(sys.executable)
    else:
        commands = smoke_commands(sys.executable)
    return run_gate(commands)


if __name__ == "__main__":
    raise SystemExit(main())
