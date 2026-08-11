#!/usr/bin/env python3
"""Behavioral retirement fence for the incompatible pre-V12 campaign."""

from __future__ import annotations

import os
import stat
import subprocess
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
RUNNER = ROOT / "scripts/bench_group_commit_scalability.sh"
LAUNCHER = ROOT / "scripts/launch_aws_group_commit_scalability.sh"


class GroupCommitScalabilityRunnerTest(unittest.TestCase):
    def test_retired_runner_fails_before_build_or_measurement(self) -> None:
        result = subprocess.run(
            ["bash", str(RUNNER)],
            cwd=ROOT,
            env=os.environ,
            capture_output=True,
            text=True,
        )
        self.assertEqual(result.returncode, 2)
        self.assertIn("retired pre-V12", result.stderr)
        self.assertIn("positioned V12", result.stderr)

    def test_retired_launcher_refuses_before_calling_aws(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            calls = root / "aws-called"
            aws = root / "aws"
            aws.write_text(
                f"#!/usr/bin/env bash\ntouch {calls}\nexit 99\n",
                encoding="utf-8",
            )
            aws.chmod(aws.stat().st_mode | stat.S_IXUSR)
            result = subprocess.run(
                ["bash", str(LAUNCHER)],
                cwd=ROOT,
                env={**os.environ, "PATH": f"{root}:{os.environ['PATH']}"},
                capture_output=True,
                text=True,
            )
            self.assertEqual(result.returncode, 2)
            self.assertIn("retired pre-V12", result.stderr)
            self.assertFalse(calls.exists())


if __name__ == "__main__":
    unittest.main()
