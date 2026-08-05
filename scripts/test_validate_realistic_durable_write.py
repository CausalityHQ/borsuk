#!/usr/bin/env python3

import csv
import hashlib
import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from validate_realistic_durable_write import ValidationError, validate


class RealisticDurableWriteValidatorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        temporary = Path(self.temporary.name)
        self.root = temporary / "results"
        self.root.mkdir()
        self.manifest_path = temporary / "manifest.json"
        self.dataset_source_sha = "d" * 64
        dataset_descriptor = json.dumps(
            {
                "dataset": "cohere-medium-1M",
                "source_sha256": self.dataset_source_sha,
            },
            sort_keys=True,
        ).encode()
        (self.root / "dataset.json").write_bytes(dataset_descriptor)
        self.dataset_sha = hashlib.sha256(dataset_descriptor).hexdigest()
        self.manifest = {
            "campaign_id": "fixture",
            "datasets": ["cohere-medium-1M"],
            "batch_records": [32],
            "repetitions": 1,
            "write_operations": 3200,
            "minimum_raw_batches": 100,
            "max_write_p95_ms": 200.0,
            "architecture": "local",
            "instance_type": "local",
            "dataset_sha256": self.dataset_sha,
            "dataset_source_sha256": self.dataset_source_sha,
        }
        encoded = json.dumps(self.manifest, sort_keys=True).encode()
        self.manifest_path.write_bytes(encoded)
        (self.root / "manifest.json").write_bytes(encoded)
        self.manifest_sha = hashlib.sha256(encoded).hexdigest()
        self.source_sha = "a" * 64
        (self.root / "environment.txt").write_text(
            f"source_sha256={self.source_sha}\n"
            f"dataset_sha256={self.dataset_sha}\n"
            f"manifest_sha256={self.manifest_sha}\n"
            "architecture=local\ninstance_type=local\n",
            encoding="utf-8",
        )
        self.cell = self.root / "cells/cohere-medium-1M/r01/b32"
        self.cell.mkdir(parents=True)
        self._write_costs()
        self._write_samples()
        self._write_resources()
        (self.cell / "INSERT_VISIBILITY_COMPLETE").touch()
        (self.cell / "CELL_COMPLETE").touch()
        (self.cell / "process_exit.txt").write_text("0\n", encoding="utf-8")
        (self.root / "REALISTIC_DURABLE_WRITE_COMPLETE").touch()

    def tearDown(self) -> None:
        self.temporary.cleanup()

    @staticmethod
    def _write_csv(path: Path, fields: list[str], records: list[dict[str, str]]) -> None:
        with path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=fields)
            writer.writeheader()
            writer.writerows(records)

    def _write_costs(self, **changes: str) -> None:
        record = {
            "op": "insert",
            "configured_batch_records": "32",
            "ops": "3200",
            "batches": "100",
            "wall_ms": "10000",
            "ops_per_s": "320",
            "mean_batch_ms": "100",
            "stddev_batch_ms": "0",
            "p50_batch_ms": "100",
            "p95_batch_ms": "100",
            "p99_batch_ms": "100",
            "max_batch_ms": "100",
            "mean_amortized_ms": "3.125",
            "gets": "100",
            "puts": "200",
            "deletes": "0",
            "heads": "0",
            "lists": "0",
            "bytes_read": "3200",
            "bytes_written": "6400",
        }
        record.update(changes)
        self._write_csv(self.cell / "bench_write_costs.csv", list(record), [record])

    def _write_samples(self, count: int = 100, last_records: int = 32) -> None:
        records = []
        for batch in range(count):
            records.append(
                {
                    "op": "insert",
                    "batch_index": str(batch),
                    "batch_records": str(last_records if batch == count - 1 else 32),
                    "batch_latency_ms": "100",
                    "amortized_ms": "3.125",
                    "gets": "1",
                    "puts": "2",
                    "deletes": "0",
                    "heads": "0",
                    "lists": "0",
                }
            )
        self._write_csv(
            self.cell / "bench_write_samples.csv", list(records[0]), records
        )

    def _write_resources(self) -> None:
        fields = [
            "elapsed_ms", "cpu_percent", "rss_bytes", "vms_bytes",
            "process_read_bytes", "process_write_bytes", "cache_disk_bytes",
            "scratch_disk_bytes", "network_receive_bytes", "network_transmit_bytes",
            "child_cpu_seconds", "child_max_rss_bytes",
        ]
        self._write_csv(
            self.cell / "resources.csv",
            fields,
            [
                {field: "0" for field in fields},
                {field: ("10000" if field == "elapsed_ms" else "1") for field in fields},
            ],
        )

    def test_valid_terminal_campaign_passes(self) -> None:
        validate(self.root, self.manifest_path)

    def test_operation_count_can_be_derived_from_full_raw_batches(self) -> None:
        del self.manifest["write_operations"]
        encoded = json.dumps(self.manifest, sort_keys=True).encode()
        self.manifest_path.write_bytes(encoded)
        (self.root / "manifest.json").write_bytes(encoded)
        environment = self.root / "environment.txt"
        values = environment.read_text().splitlines()
        values = [
            f"manifest_sha256={hashlib.sha256(encoded).hexdigest()}"
            if value.startswith("manifest_sha256=")
            else value
            for value in values
        ]
        environment.write_text("\n".join(values) + "\n")
        validate(self.root, self.manifest_path)

    def test_missing_root_completion_fails_before_measurements(self) -> None:
        (self.root / "REALISTIC_DURABLE_WRITE_COMPLETE").unlink()
        with self.assertRaisesRegex(ValidationError, "campaign is incomplete"):
            validate(self.root, self.manifest_path)

    def test_terminal_cell_can_be_validated_before_root_completion(self) -> None:
        (self.root / "REALISTIC_DURABLE_WRITE_COMPLETE").unlink()
        validate(
            self.root,
            self.manifest_path,
            terminal_cell=("cohere-medium-1M", 1, 32),
        )

    def test_terminal_cell_must_belong_to_frozen_matrix(self) -> None:
        with self.assertRaisesRegex(ValidationError, "frozen matrix"):
            validate(
                self.root,
                self.manifest_path,
                terminal_cell=("cohere-medium-1M", 1, 1024),
            )

    def test_root_failure_marker_fails(self) -> None:
        (self.root / "REALISTIC_DURABLE_WRITE_FAILED").touch()
        with self.assertRaisesRegex(ValidationError, "failure marker"):
            validate(self.root, self.manifest_path)

    def test_dataset_identity_drift_fails(self) -> None:
        environment = self.root / "environment.txt"
        environment.write_text(
            environment.read_text().replace(self.dataset_sha, "c" * 64)
        )
        with self.assertRaisesRegex(ValidationError, "dataset descriptor SHA-256 mismatch"):
            validate(self.root, self.manifest_path, expected_dataset_sha=self.dataset_sha)

    def test_missing_copied_dataset_descriptor_fails(self) -> None:
        (self.root / "dataset.json").unlink()
        with self.assertRaisesRegex(ValidationError, "dataset descriptor"):
            validate(self.root, self.manifest_path)

    def test_dataset_source_identity_drift_fails(self) -> None:
        descriptor = json.loads((self.root / "dataset.json").read_text())
        descriptor["source_sha256"] = "e" * 64
        (self.root / "dataset.json").write_text(json.dumps(descriptor, sort_keys=True))
        with self.assertRaisesRegex(ValidationError, "dataset descriptor SHA-256"):
            validate(self.root, self.manifest_path)

    def test_invalid_source_identity_fails(self) -> None:
        environment = self.root / "environment.txt"
        environment.write_text(
            environment.read_text().replace(self.source_sha, "not-a-sha")
        )
        with self.assertRaisesRegex(ValidationError, "invalid source sha256"):
            validate(self.root, self.manifest_path)

    def test_copied_manifest_drift_fails(self) -> None:
        (self.root / "manifest.json").write_text("{}\n")
        with self.assertRaisesRegex(ValidationError, "copied manifest"):
            validate(self.root, self.manifest_path)

    def test_missing_raw_batches_fails(self) -> None:
        self._write_samples(count=99)
        with self.assertRaisesRegex(ValidationError, "raw batch count"):
            validate(self.root, self.manifest_path)

    def test_batch_record_total_must_reconcile(self) -> None:
        self._write_samples(last_records=31)
        with self.assertRaisesRegex(ValidationError, "record reconciliation"):
            validate(self.root, self.manifest_path)

    def test_sample_cannot_exceed_configured_batch(self) -> None:
        self._write_samples(last_records=33)
        with self.assertRaisesRegex(ValidationError, "configured batch"):
            validate(self.root, self.manifest_path)

    def test_write_p95_at_gate_fails(self) -> None:
        self._write_costs(p95_batch_ms="200")
        with self.assertRaisesRegex(ValidationError, "write p95"):
            validate(self.root, self.manifest_path)

    def test_visibility_marker_is_required(self) -> None:
        (self.cell / "INSERT_VISIBILITY_COMPLETE").unlink()
        with self.assertRaisesRegex(ValidationError, "visibility"):
            validate(self.root, self.manifest_path)

    def test_successful_process_exit_is_required(self) -> None:
        (self.cell / "process_exit.txt").write_text("1\n")
        with self.assertRaisesRegex(ValidationError, "process exit"):
            validate(self.root, self.manifest_path)

    def test_resource_timeline_must_cover_measured_wall_time(self) -> None:
        self._write_costs(wall_ms="10001")
        with self.assertRaisesRegex(ValidationError, "resource timeline"):
            validate(self.root, self.manifest_path)


if __name__ == "__main__":
    unittest.main()
