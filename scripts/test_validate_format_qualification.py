#!/usr/bin/env python3
"""Tests for physical-format qualification artifact validation."""

from __future__ import annotations

import csv
import tempfile
import unittest
from pathlib import Path

from validate_format_qualification import validate_run


class FormatQualificationValidationTest(unittest.TestCase):
    def test_rejects_a_blocked_format(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self._write_minimal(root, "blocked")
            with self.assertRaisesRegex(ValueError, "not complete"):
                validate_run(root, expected_samples=1)

    def test_accepts_complete_distribution_and_resource_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self._write_minimal(root, "complete")
            validate_run(root, expected_samples=1)

    @staticmethod
    def _write(path: Path, name: str, fields: list[str], row: list[object]) -> None:
        with (path / name).open("w", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow(fields)
            writer.writerow(row)

    def _write_minimal(self, root: Path, status: str) -> None:
        self._write(
            root, "build.csv", ["format", "elapsed_ms", "file_bytes"], ["x", 1, 2]
        )
        self._write(root, "open.csv", ["format", "elapsed_ms"], ["x", 1])
        self._write(
            root,
            "samples.csv",
            ["format", "repetition", "elapsed_ms"],
            ["x", 1, 1],
        )
        self._write(
            root,
            "summary.csv",
            ["format", "samples", "mean_ms", "stddev_ms", "p50_ms", "p95_ms", "p99_ms"],
            ["x", 1, 1, 0, 1, 1, 1],
        )
        self._write(
            root, "status.csv", ["format", "status", "blocker"], ["x", status, ""]
        )
        self._write(
            root,
            "resources.csv",
            [
                "elapsed_ms",
                "cpu_percent",
                "rss_bytes",
                "process_read_bytes",
                "process_write_bytes",
                "network_receive_bytes",
                "network_transmit_bytes",
            ],
            [0, 0, 1, 0, 0, 0, 0],
        )


if __name__ == "__main__":
    unittest.main()
