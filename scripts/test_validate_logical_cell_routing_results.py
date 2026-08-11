#!/usr/bin/env python3
"""Tests for the fail-closed logical-cell routing campaign validator."""

from __future__ import annotations

import csv
import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from scripts.validate_logical_cell_routing_positioned_v12_results import (
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
        self.manifest_path = self.root / "frozen-manifest.json"
        self.manifest = {
            "campaign_id": "logical-cell-routing-positioned-v12-v1",
            "protocol_kind": "production",
            "mutation_protocol": "positioned-v12",
            "architecture": "aarch64",
            "instance_type": "fixture",
            "purchase_option": "spot",
            "dimensions": 768,
            "metric": "cosine",
            "cell_counts": [2000, 16000],
            "routing_modes": ["flat", "quantizer"],
            "writers": [1, 8, 32],
            "repetitions": 2,
            "operations_per_writer": 2,
            "cell_timeout_seconds": 300,
            "clone_timeout_seconds": 60,
            "campaign_timeout_seconds": 21600,
            "setup_timeout_seconds": 1800,
            "resource_sample_interval_ms": 100,
            "max_write_p95_ms": 200.0,
            "master_seed": 7000,
            "correctness_gates": [
                "multi_writer_reopen",
                "sequential_last_write_wins",
                "delete_reopen",
                "cross_modality_reopen",
                "shard_head_conflict_rebase",
                "lost_head_response_reconciled",
                "publication_failure_invisible",
                "materializer_race",
            ],
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
                            "instance_id": "i-0123456789abcdef0",
                            "availability_zone": "eu-central-1a",
                            "purchase_option": "spot",
                            "mutation_protocol": "positioned-v12",
                            "dimensions": 768,
                            "metric": "cosine",
                            "routing_mode": mode,
                            "routing_path": mode,
                            "cell_count": cells,
                            "writers": writers,
                            "repetition": repetition,
                            "cohort_blake3": cohort,
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
                                "distinct_cells": writers * 2,
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
                                        "vector_blake3": hashlib.sha256(
                                            f"vector:{writer}:{ordinal}".encode()
                                        ).hexdigest(),
                                        "latency_ms": "1.0",
                                        "selected_cell": writer * 2 + ordinal,
                                        "storage_requests": 3,
                                    }
                                )
        for gate in self.manifest["correctness_gates"]:
            correctness.append({"gate": gate, "status": "pass"})
        for cells in self.manifest["cell_counts"]:
            for writers in self.manifest["writers"]:
                for repetition in range(1, self.manifest["repetitions"] + 1):
                    for mode in self.manifest["routing_modes"]:
                        cell = (
                            self.root
                            / "cells"
                            / f"c{cells}"
                            / f"r{repetition:02d}"
                            / f"w{writers}"
                            / mode
                        )
                        cell.mkdir(parents=True, exist_ok=True)
                        (cell / "resources.csv").write_text(
                            "elapsed_ms,cpu_percent,rss_bytes,vms_bytes,process_read_bytes,process_write_bytes,cache_disk_bytes,scratch_disk_bytes,network_receive_bytes,network_transmit_bytes,child_cpu_seconds,child_max_rss_bytes\n"
                            "0,0.0,1024,2048,0,0,0,0,0,0,,\n"
                            "10,0.0,1024,2048,0,0,0,0,10,20,0.003,1024\n",
                            encoding="utf-8",
                        )
                        (cell / "process_exit.txt").write_text("0\n", encoding="utf-8")
                        (cell / "CELL_COMPLETE").write_text(
                            "complete\n", encoding="utf-8"
                        )
                        (cell / "storage-access.csv").write_text(
                            "operation,path,bytes\nPUT,positioned-log/payloads/test,128\n",
                            encoding="utf-8",
                        )
                        (cell / "benchmark.stdout.log").write_text(
                            "completed\n", encoding="utf-8"
                        )
                        (cell / "benchmark.stderr.log").write_text(
                            "routing_progress final=true\n", encoding="utf-8"
                        )
        write_csv(self.root / "summary.csv", rows)
        write_csv(self.root / "samples.csv", samples)
        write_csv(self.root / "correctness.csv", correctness)
        (self.root / "manifest.json").write_bytes(self.manifest_path.read_bytes())
        (self.root / "environment.txt").write_text(
            "source_sha256=" + self.source_sha + "\n"
            "manifest_sha256=" + self.manifest_sha + "\n"
            "architecture=aarch64\n"
            "instance_type=fixture\n"
            "instance_id=i-0123456789abcdef0\n"
            "availability_zone=eu-central-1a\n"
            "purchase_option=spot\n"
            "mutation_protocol=positioned-v12\n"
            "dimensions=768\n"
            "metric=cosine\n",
            encoding="utf-8",
        )
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

    def test_rejects_missing_campaign_timeout(self) -> None:
        del self.manifest["campaign_timeout_seconds"]
        self.manifest_path.write_text(
            json.dumps(self.manifest, sort_keys=True), encoding="utf-8"
        )
        with self.assertRaisesRegex(ValidationError, "campaign timeout"):
            self.validate()

    def test_rejects_matrix_whose_worst_case_exceeds_campaign_timeout(self) -> None:
        self.manifest["cell_timeout_seconds"] = 3_600
        self.manifest_path.write_text(
            json.dumps(self.manifest, sort_keys=True), encoding="utf-8"
        )
        with self.assertRaisesRegex(ValidationError, "worst-case matrix duration"):
            self.validate()

    def test_rejects_unrealistic_production_dimensions(self) -> None:
        self.manifest["dimensions"] = 96
        self.manifest_path.write_text(
            json.dumps(self.manifest, sort_keys=True), encoding="utf-8"
        )
        with self.assertRaisesRegex(ValidationError, "dimensions"):
            self.validate()

    def test_rejects_non_spot_production_execution(self) -> None:
        self.manifest["purchase_option"] = "on-demand"
        self.manifest_path.write_text(
            json.dumps(self.manifest, sort_keys=True), encoding="utf-8"
        )
        with self.assertRaisesRegex(ValidationError, "Spot"):
            self.validate()

    def test_refuses_to_read_csv_before_completion(self) -> None:
        (self.root / "LOGICAL_CELL_ROUTING_COMPLETE").unlink()
        (self.root / "summary.csv").write_text("not,csv\n", encoding="utf-8")
        with self.assertRaisesRegex(ValidationError, "completion marker"):
            self.validate()

    def test_rejects_a_copied_manifest_that_differs_from_the_frozen_input(self) -> None:
        (self.root / "manifest.json").write_text("{}\n", encoding="utf-8")
        with self.assertRaisesRegex(ValidationError, "copied manifest"):
            self.validate()

    def test_rejects_unequal_paired_cohorts(self) -> None:
        path = self.root / "summary.csv"
        rows = read_rows(path)
        rows[1]["cohort_blake3"] = "b" * 64
        write_csv(path, rows)
        with self.assertRaisesRegex(ValidationError, "paired cohort"):
            self.validate()

    def test_rejects_paired_rows_with_different_vector_bytes(self) -> None:
        path = self.root / "samples.csv"
        rows = read_rows(path)
        quantizer = next(row for row in rows if row["routing_mode"] == "quantizer")
        quantizer["vector_blake3"] = "b" * 64
        write_csv(path, rows)
        with self.assertRaisesRegex(ValidationError, "paired raw cohort"):
            self.validate()

    def test_rejects_summary_distinct_cell_count_not_backed_by_samples(self) -> None:
        path = self.root / "summary.csv"
        rows = read_rows(path)
        rows[0]["distinct_cells"] = "1999"
        write_csv(path, rows)
        with self.assertRaisesRegex(ValidationError, "distinct cell count"):
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
            [{"gate": "multi_writer_reopen", "status": "fail"}],
        )
        with self.assertRaisesRegex(ValidationError, "correctness"):
            self.validate()

    def test_rejects_missing_resource_telemetry(self) -> None:
        next(self.root.glob("cells/**/resources.csv")).unlink()
        with self.assertRaisesRegex(ValidationError, "resource telemetry"):
            self.validate()

    def test_rejects_nonterminal_cell(self) -> None:
        next(self.root.glob("cells/**/CELL_COMPLETE")).unlink()
        with self.assertRaisesRegex(ValidationError, "terminal cell"):
            self.validate()

    def test_rejects_latency_above_the_hard_cap(self) -> None:
        path = self.root / "summary.csv"
        rows = read_rows(path)
        rows[0]["p95_ms"] = "200.001"
        write_csv(path, rows)
        with self.assertRaisesRegex(ValidationError, "write p95 hard cap"):
            self.validate()

    def test_rejects_a_quantizer_arm_that_silently_used_flat_routing(self) -> None:
        path = self.root / "summary.csv"
        rows = read_rows(path)
        quantizer = next(row for row in rows if row["routing_mode"] == "quantizer")
        quantizer["routing_path"] = "flat"
        write_csv(path, rows)
        with self.assertRaisesRegex(ValidationError, "routing path"):
            self.validate()


if __name__ == "__main__":
    unittest.main()
