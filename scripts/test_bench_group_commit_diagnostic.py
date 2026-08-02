#!/usr/bin/env python3
"""Static contract tests for bounded group-commit qualification."""

from pathlib import Path
import unittest


SOURCE = (Path(__file__).parent / "bench_group_commit_diagnostic.sh").read_text()


class GroupCommitDiagnosticTest(unittest.TestCase):
    def test_frozen_shape_and_fail_closed_markers(self) -> None:
        for value in (
            "BORSUK_GROUP_COMMIT_WRITERS=8",
            "BORSUK_GROUP_COMMIT_OPERATIONS_PER_WRITER=20",
            "BORSUK_GROUP_COMMIT_MAX_DELAY_MS=5",
            "BORSUK_GROUP_COMMIT_MAX_RECORDS=64",
            "GROUP_COMMIT_DIAGNOSTIC_FAILED",
            "GROUP_COMMIT_DIAGNOSTIC_COMPLETE",
            "/usr/bin/time -v",
        ):
            self.assertIn(value, SOURCE)


if __name__ == "__main__":
    unittest.main()
