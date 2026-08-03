#!/usr/bin/env python3
"""Regression tests for the group-commit fail-closed validator."""

import csv
import tempfile
import unittest
from pathlib import Path

from validate_group_commit_diagnostic import ValidationError, validate


class ValidatorTest(unittest.TestCase):
    def test_incomplete_tree_fails_before_csv_access(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "cell").mkdir()
            (root / "cell" / "summary.csv").write_text("not,csv\n", encoding="utf-8")
            with self.assertRaisesRegex(ValidationError, "success marker"):
                validate(root, Path("docs/research/group-commit-diagnostic.json"))

    def test_terminal_failure_marker_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "GROUP_COMMIT_DIAGNOSTIC_COMPLETE").touch()
            (root / "GROUP_COMMIT_DIAGNOSTIC_FAILED").touch()
            with self.assertRaisesRegex(ValidationError, "failure marker"):
                validate(root, Path("docs/research/group-commit-diagnostic.json"))


if __name__ == "__main__":
    unittest.main()
