#!/usr/bin/env python3
"""Behavioral tests for fail-closed benchmark S3 publication helpers."""

from __future__ import annotations

import os
import stat
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
HELPER = ROOT / "scripts/benchmark_s3.py"


class BenchmarkS3Test(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.bin = self.root / "bin"
        self.bin.mkdir()
        self.calls = self.root / "calls"
        aws = self.bin / "aws"
        aws.write_text(
            "#!/usr/bin/env bash\n"
            'printf \'%s\\n\' "$*" >> "$FAKE_AWS_CALLS"\n'
            "if [[ \"$*\" == *'s3api list-objects-v2'* ]]; then\n"
            '  [[ ${FAKE_AWS_LIST_STATUS:-0} == 0 ]] || exit "$FAKE_AWS_LIST_STATUS"\n'
            "  printf '%s' \"${FAKE_AWS_KEY_COUNT:-0}\"\n"
            "fi\n"
            "if [[ \"$*\" == *'s3 ls'* ]]; then\n"
            '  [[ ${FAKE_AWS_LS_STATUS:-0} == 0 ]] || exit "$FAKE_AWS_LS_STATUS"\n'
            "  printf '%s' \"${FAKE_AWS_LS_OUTPUT:-}\"\n"
            "fi\n",
            encoding="utf-8",
        )
        aws.chmod(aws.stat().st_mode | stat.S_IXUSR)
        self.env = {
            **os.environ,
            "PATH": f"{self.bin}:{os.environ['PATH']}",
            "FAKE_AWS_CALLS": str(self.calls),
        }

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def run_helper(
        self, *args: str, **environment: str
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [sys.executable, str(HELPER), *args],
            cwd=ROOT,
            env={**self.env, **environment},
            capture_output=True,
            text=True,
        )

    def test_assert_empty_rejects_an_aws_listing_failure(self) -> None:
        result = self.run_helper(
            "assert-empty",
            "--uri",
            "s3://bucket/new-prefix",
            FAKE_AWS_LIST_STATUS="42",
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("could not list", result.stderr)

    def test_assert_empty_accepts_absent_prefix_when_high_level_ls_fails(self) -> None:
        result = self.run_helper(
            "assert-empty",
            "--uri",
            "s3://bucket/new-prefix",
            FAKE_AWS_LS_STATUS="1",
            FAKE_AWS_KEY_COUNT="0",
        )
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_assert_empty_rejects_existing_objects(self) -> None:
        result = self.run_helper(
            "assert-empty",
            "--uri",
            "s3://bucket/existing",
            FAKE_AWS_KEY_COUNT="1",
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("non-empty", result.stderr)

    def test_publish_uploads_artifacts_before_terminal_markers(self) -> None:
        output = self.root / "output"
        cell = output / "cells/c2000/r01/w1/flat"
        cell.mkdir(parents=True)
        (cell / "summary.csv").write_text("data\n", encoding="utf-8")
        (cell / "CELL_COMPLETE").write_text("complete\n", encoding="utf-8")
        (output / "LOGICAL_CELL_ROUTING_COMPLETE").write_text(
            "complete\n", encoding="utf-8"
        )

        result = self.run_helper(
            "publish",
            "--source",
            str(output),
            "--destination",
            "s3://bucket/run/results",
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        calls = self.calls.read_text(encoding="utf-8").splitlines()
        self.assertIn("s3 sync", calls[0])
        self.assertIn("--exclude */CELL_COMPLETE", calls[0])
        self.assertIn("--exclude LOGICAL_CELL_ROUTING_COMPLETE", calls[0])
        self.assertIn("CELL_COMPLETE", calls[1])
        self.assertIn("LOGICAL_CELL_ROUTING_COMPLETE", calls[2])


if __name__ == "__main__":
    unittest.main()
