#!/usr/bin/env python3
"""Tests for the fail-closed logical-cell routing campaign validator."""

from __future__ import annotations

import csv
import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from scripts.validate_logical_cell_routing_results import (
    ValidationError,
    validate_results,
)


def write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def read_rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


class ValidateLogicalCellRoutingResultsTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.manifest_path = self.root / "manifest.json"
        self.manifest = {
            "cell_counts": [2000, 16000],
            "routing_modes": ["flat", "quantizer"],
            "writers": [1, 8, 32],
            "repetitions": 2,
            "operations_per_writer": 2,
            "cell_timeout_seconds": 1800,
            "master_seed": 7000,
        }
        self.manifest_path.write_text(
            json.dumps(self.manifest, sort_keys=True), encoding="utf-8"
        )
        self.manifest_sha = hashlib.sha256(self.manifest_path.read_bytes()).hexdigest()
        self.source_sha = "a" * 64
        rows = []
        samples = []
        correctness = []
        for cells in self.manifest["cell_counts"]:
            for writers in self.manifest["writers"]:
                for repetition in range(1, self.manifest["repetitions"] + 1):
                    cohort = hashlib.sha256(
                        f"{cells}:{writers}:{repetition}:7000".encode()
                    ).hexdigest()
                    for mode in self.manifest["routing_modes"]:
                        identity = {
                            "source_sha256": self.source_sha,
                            "manifest_sha256": self.manifest_sha,
                            "architecture": "aarch64",
                            "instance_type": "fixture",
                            "routing_mode": mode,
                            "cell_count": cells,
                            "writers": writers,
                            "repetition": repetition,
                            "cohort_sha256": cohort,
                        }
                        rows.append(
                            {
                                **identity,
                                "operations": writers * 2,
                                "elapsed_ms": "4.0",
                                "cpu_seconds": "0.003",
                                "p50_ms": "1.0",
                                "p95_ms": "1.0",
                                "throughput_ops_per_second": str(writers * 500.0),
                                "storage_requests": writers * 3,
                                "distinct_cells": writers,
                            }
                        )
                        for writer in range(writers):
                            for ordinal in range(2):
                                samples.append(
                                    {
                                        **identity,
                                        "writer": writer,
                                        "operation": ordinal,
                                        "record_id": f"c{cells}-w{writer}-o{ordinal}",
                                        "latency_ms": "1.0",
                                        "selected_cell": writer,
                                    }
                                )
        for gate in ("duplicate_race", "prepare_failure", "crash_recovery"):
            correctness.append({"gate": gate, "status": "pass"})
        for cells in self.manifest["cell_counts"]:
            for writers in self.manifest["writers"]:
                for repetition in range(1, self.manifest["repetitions"] + 1):
                    for mode in self.manifest["routing_modes"]:
                        resource = (
                            self.root
                            / "cells"
                            / f"c{cells}"
                            / f"r{repetition:02d}"
                            / f"w{writers}"
                            / f"{mode}.resources.txt"
                        )
                        resource.parent.mkdir(parents=True, exist_ok=True)
                        resource.write_text(
                            "User time (seconds): 1.0\n"
                            "System time (seconds): 0.1\n"
                            "Maximum resident set size (kbytes): 1024\n",
                            encoding="utf-8",
                        )
        write_csv(self.root / "summary.csv", rows)
        write_csv(self.root / "samples.csv", samples)
        write_csv(self.root / "correctness.csv", correctness)
        (self.root / "LOGICAL_CELL_ROUTING_COMPLETE").write_text(
            "complete\n", encoding="utf-8"
        )

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def validate(self) -> None:
        validate_results(self.manifest_path, self.root)

    def test_accepts_complete_exact_paired_matrix(self) -> None:
        self.validate()

    def test_rejects_missing_cell_timeout(self) -> None:
        del self.manifest["cell_timeout_seconds"]
        self.manifest_path.write_text(
            json.dumps(self.manifest, sort_keys=True), encoding="utf-8"
        )
        with self.assertRaisesRegex(ValidationError, "cell timeout"):
            self.validate()

    def test_refuses_to_read_csv_before_completion(self) -> None:
        (self.root / "LOGICAL_CELL_ROUTING_COMPLETE").unlink()
        (self.root / "summary.csv").write_text("not,csv\n", encoding="utf-8")
        with self.assertRaisesRegex(ValidationError, "completion marker"):
            self.validate()

    def test_rejects_unequal_paired_cohorts(self) -> None:
        path = self.root / "summary.csv"
        rows = read_rows(path)
        rows[1]["cohort_sha256"] = "b" * 64
        write_csv(path, rows)
        with self.assertRaisesRegex(ValidationError, "paired cohort"):
            self.validate()

    def test_rejects_missing_raw_sample(self) -> None:
        path = self.root / "samples.csv"
        rows = read_rows(path)
        write_csv(path, rows[:-1])
        with self.assertRaisesRegex(ValidationError, "sample count"):
            self.validate()

    def test_rejects_non_finite_timing(self) -> None:
        path = self.root / "summary.csv"
        rows = read_rows(path)
        rows[0]["p95_ms"] = "nan"
        write_csv(path, rows)
        with self.assertRaisesRegex(ValidationError, "non-finite"):
            self.validate()

    def test_rejects_failed_or_missing_correctness_gate(self) -> None:
        write_csv(
            self.root / "correctness.csv",
            [{"gate": "duplicate_race", "status": "fail"}],
        )
        with self.assertRaisesRegex(ValidationError, "correctness"):
            self.validate()

    def test_rejects_missing_resource_telemetry(self) -> None:
        next(self.root.glob("cells/**/*.resources.txt")).unlink()
        with self.assertRaisesRegex(ValidationError, "resource telemetry"):
            self.validate()


if __name__ == "__main__":
    unittest.main()
