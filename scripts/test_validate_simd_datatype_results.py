#!/usr/bin/env python3
"""Unit tests for the fail-closed SIMD campaign validator."""

from __future__ import annotations

import csv
import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from scripts.validate_simd_datatype_results import ValidationError, validate_results


RAW_FIELDS = [
    "architecture",
    "instance_type",
    "source_sha256",
    "manifest_sha256",
    "build",
    "binary_sha256",
    "path",
    "element_type",
    "repetition",
    "cache_state",
    "target_cache_coverage_percent",
    "observed_cache_coverage_percent",
    "client_concurrency",
    "query_seed",
    "query_ordinal",
    "query_id",
    "latency_ms",
    "cpu_seconds",
    "recall_or_exact_agreement",
    "rss_bytes",
    "logical_bytes",
    "disk_cache_bytes",
    "backing_bytes",
    "disk_cache_requests",
    "backing_requests",
]
SUMMARY_FIELDS = [
    "architecture",
    "build",
    "path",
    "element_type",
    "repetition",
    "cache_state",
    "target_cache_coverage_percent",
    "client_concurrency",
    "query_seed",
    "samples",
    "mean_ms",
    "stddev_ms",
    "p50_ms",
    "p90_ms",
    "p95_ms",
    "p99_ms",
    "max_ms",
    "qps",
    "cpu_seconds_per_query",
    "recall_or_exact_agreement",
    "peak_rss_bytes",
    "mean_logical_bytes",
    "mean_disk_cache_bytes",
    "mean_backing_bytes",
    "mean_disk_cache_requests",
    "mean_backing_requests",
]


def write_csv(path: Path, fields: list[str], rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


class ValidateSimdDatatypeResultsTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.source_sha = "1" * 64
        self.manifest_path = self.root / "manifest.json"
        self.schedule_path = self.root / "schedule.csv"
        self.manifest = {
            "architectures": [{"name": "arm", "uname_machine": "aarch64"}],
            "builds": [{"name": "simd"}, {"name": "scalar-control"}],
            "paths": [
                {
                    "name": "dense-float32",
                    "kind": "primary-dense",
                    "element_type": "float32",
                    "dataset": "fixture",
                }
            ],
            "cache_states": [{"name": "uncached", "coverage_percent": 0}],
            "client_concurrency": [1],
            "repetitions": 1,
            "query_cohort": {
                "queries_per_cell": 2,
                "master_seed": 100,
            },
            "required_raw_query_fields": RAW_FIELDS,
            "required_summary_fields": [
                field
                for field in SUMMARY_FIELDS
                if field
                not in {
                    "architecture",
                    "build",
                    "path",
                    "element_type",
                    "repetition",
                    "cache_state",
                    "target_cache_coverage_percent",
                    "client_concurrency",
                    "query_seed",
                }
            ],
        }
        self.manifest_path.write_text(
            json.dumps(self.manifest, sort_keys=True), encoding="utf-8"
        )
        self.manifest_sha = hashlib.sha256(self.manifest_path.read_bytes()).hexdigest()
        self.binary_hashes = {
            ("simd", "production_bench"): "2" * 64,
            ("scalar-control", "production_bench"): "3" * 64,
            ("simd", "hybrid_retrieval_bench"): "4" * 64,
            ("scalar-control", "hybrid_retrieval_bench"): "5" * 64,
            ("simd", "market_workload_bench"): "6" * 64,
            ("scalar-control", "market_workload_bench"): "7" * 64,
        }
        write_csv(
            self.root / "builds.csv",
            ["build", "binary", "sha256"],
            [
                {"build": build, "binary": binary, "sha256": digest}
                for (build, binary), digest in self.binary_hashes.items()
            ],
        )
        schedule = []
        for build in ("simd", "scalar-control"):
            schedule.append(
                {
                    "architecture": "arm",
                    "build": build,
                    "path": "dense-float32",
                    "kind": "primary-dense",
                    "element_type": "float32",
                    "dataset": "fixture",
                    "repetition": "1",
                    "cache_state": "uncached",
                    "target_cache_coverage_percent": "0",
                    "client_concurrency": "1",
                    "query_seed": "101",
                    "index_key": f"fixture/{build}",
                    "status": "planned",
                }
            )
        write_csv(self.schedule_path, list(schedule[0]), schedule)
        for row in schedule:
            self._write_cell(row)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def _cell(self, build: str) -> Path:
        return (
            self.root
            / "cells"
            / build
            / "dense-float32"
            / "r01"
            / "uncached"
            / "c1"
        )

    def _write_cell(self, schedule: dict[str, str]) -> None:
        directory = self._cell(schedule["build"])
        directory.mkdir(parents=True, exist_ok=True)
        binary_sha = self.binary_hashes[
            (schedule["build"], "production_bench")
        ]
        raw_rows = []
        for ordinal in range(2):
            raw_rows.append(
                {
                    **{
                        field: schedule[field]
                        for field in (
                            "architecture",
                            "build",
                            "path",
                            "element_type",
                            "repetition",
                            "cache_state",
                            "target_cache_coverage_percent",
                            "client_concurrency",
                            "query_seed",
                        )
                    },
                    "instance_type": "fixture",
                    "source_sha256": self.source_sha,
                    "manifest_sha256": self.manifest_sha,
                    "binary_sha256": binary_sha,
                    "observed_cache_coverage_percent": "0",
                    "query_ordinal": ordinal,
                    "query_id": f"q{ordinal}",
                    "latency_ms": "1.0",
                    "cpu_seconds": "0.001",
                    "recall_or_exact_agreement": "1.0",
                    "rss_bytes": "1024",
                    "logical_bytes": "128",
                    "disk_cache_bytes": "0",
                    "backing_bytes": "128",
                    "disk_cache_requests": "0",
                    "backing_requests": "1",
                }
            )
        write_csv(directory / "queries.csv", RAW_FIELDS, raw_rows)
        summary = {
            **{
                field: schedule[field]
                for field in (
                    "architecture",
                    "build",
                    "path",
                    "element_type",
                    "repetition",
                    "cache_state",
                    "target_cache_coverage_percent",
                    "client_concurrency",
                    "query_seed",
                )
            },
            "samples": "2",
            "mean_ms": "1",
            "stddev_ms": "0",
            "p50_ms": "1",
            "p90_ms": "1",
            "p95_ms": "1",
            "p99_ms": "1",
            "max_ms": "1",
            "qps": "1000",
            "cpu_seconds_per_query": "0.001",
            "recall_or_exact_agreement": "1",
            "peak_rss_bytes": "1024",
            "mean_logical_bytes": "128",
            "mean_disk_cache_bytes": "0",
            "mean_backing_bytes": "128",
            "mean_disk_cache_requests": "0",
            "mean_backing_requests": "1",
        }
        write_csv(directory / "summary.csv", SUMMARY_FIELDS, [summary])
        write_csv(
            directory / "resources.csv",
            ["cpu_percent", "rss_bytes"],
            [{"cpu_percent": "50", "rss_bytes": "1024"}],
        )
        (directory / "CELL_COMPLETE").write_text("status=complete\n", encoding="utf-8")

    def _validate(self) -> dict:
        return validate_results(
            manifest_path=self.manifest_path,
            schedule_path=self.schedule_path,
            root=self.root,
            architecture="arm",
            source_sha256=self.source_sha,
            manifest_sha256=self.manifest_sha,
        )

    def _set_mixed_cache_observations(self, observations: list[str]) -> None:
        self.manifest["cache_states"] = [
            {"name": "uncached", "coverage_percent": 50}
        ]
        self.manifest_path.write_text(
            json.dumps(self.manifest, sort_keys=True), encoding="utf-8"
        )
        self.manifest_sha = hashlib.sha256(
            self.manifest_path.read_bytes()
        ).hexdigest()
        with self.schedule_path.open(newline="", encoding="utf-8") as handle:
            schedule = list(csv.DictReader(handle))
        for row in schedule:
            row["target_cache_coverage_percent"] = "50"
        write_csv(self.schedule_path, list(schedule[0]), schedule)
        for row in schedule:
            self._write_cell(row)
            path = self._cell(row["build"]) / "queries.csv"
            with path.open(newline="", encoding="utf-8") as handle:
                raw_rows = list(csv.DictReader(handle))
            for raw_row, observed in zip(raw_rows, observations, strict=True):
                raw_row["observed_cache_coverage_percent"] = observed
            write_csv(path, RAW_FIELDS, raw_rows)

    def test_complete_same_cohort_fixture_validates(self) -> None:
        decision = self._validate()
        self.assertEqual(decision["status"], "validated")
        self.assertEqual(decision["schedule_cells"], 2)
        self.assertEqual(decision["raw_query_rows"], 4)
        self.assertTrue((self.root / "simd-validation.json").is_file())

    def test_mixed_cache_coverage_is_validated_as_a_cell_mean(self) -> None:
        self._set_mixed_cache_observations(["0", "100"])
        decision = self._validate()
        self.assertEqual(decision["status"], "validated")

    def test_mixed_cache_cell_mean_drift_fails_closed(self) -> None:
        self._set_mixed_cache_observations(["0", "80"])
        with self.assertRaisesRegex(ValidationError, "cache coverage drift"):
            self._validate()

    def test_non_finite_timing_fails_closed(self) -> None:
        path = self._cell("simd") / "queries.csv"
        with path.open(newline="", encoding="utf-8") as handle:
            rows = list(csv.DictReader(handle))
        rows[0]["latency_ms"] = "nan"
        write_csv(path, RAW_FIELDS, rows)
        with self.assertRaisesRegex(ValidationError, "non-finite"):
            self._validate()

    def test_query_cohort_order_drift_fails_closed(self) -> None:
        path = self._cell("simd") / "queries.csv"
        with path.open(newline="", encoding="utf-8") as handle:
            rows = list(csv.DictReader(handle))
        rows[1]["query_id"] = "different"
        write_csv(path, RAW_FIELDS, rows)
        with self.assertRaisesRegex(ValidationError, "cohort"):
            self._validate()

    def test_missing_cell_marker_fails_closed(self) -> None:
        (self._cell("simd") / "CELL_COMPLETE").unlink()
        with self.assertRaisesRegex(ValidationError, "completion marker"):
            self._validate()

    def test_equal_build_hashes_fail_closed(self) -> None:
        rows = []
        for (build, binary), digest in self.binary_hashes.items():
            if build == "scalar-control" and binary == "production_bench":
                digest = self.binary_hashes[("simd", "production_bench")]
            rows.append({"build": build, "binary": binary, "sha256": digest})
        write_csv(
            self.root / "builds.csv",
            ["build", "binary", "sha256"],
            rows,
        )
        with self.assertRaisesRegex(ValidationError, "equal"):
            self._validate()

    def test_empty_cpu_telemetry_fails_closed(self) -> None:
        for build in ("simd", "scalar-control"):
            write_csv(
                self._cell(build) / "resources.csv",
                ["cpu_percent", "rss_bytes"],
                [{"cpu_percent": "0", "rss_bytes": "1024"}],
            )
        with self.assertRaisesRegex(ValidationError, "CPU telemetry"):
            self._validate()


if __name__ == "__main__":
    unittest.main()
