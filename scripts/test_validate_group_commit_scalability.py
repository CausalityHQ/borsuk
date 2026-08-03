#!/usr/bin/env python3

import csv
import hashlib
import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from validate_group_commit_scalability import ValidationError, validate


class ValidatorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name) / "results"
        self.root.mkdir()
        self.manifest_path = Path(self.temporary.name) / "manifest.json"
        manifest = {
            "architecture": "local",
            "instance_type": "local",
            "cell_counts": [64],
            "writers": [1],
            "repetitions": 1,
            "operations_per_writer": 2,
            "exact_recall_queries_per_cell": 1,
            "correctness_gates": [
                "same_id_last_write_wins",
                "prepare_failure",
                "crash_recovery",
            ],
        }
        encoded = json.dumps(manifest, sort_keys=True).encode()
        self.manifest_path.write_bytes(encoded)
        (self.root / "manifest.json").write_bytes(encoded)
        manifest_sha = hashlib.sha256(encoded).hexdigest()
        source_sha = "a" * 64
        (self.root / "environment.txt").write_text(
            f"source_sha256={source_sha}\nmanifest_sha256={manifest_sha}\n"
            "architecture=local\ninstance_type=local\n",
            encoding="utf-8",
        )
        cell = self.root / "cells/c64/r01/w1"
        cell.mkdir(parents=True)
        summary_fields = [
            "source_sha256", "manifest_sha256", "writers", "operations",
            "records", "groups", "mean_group_records", "elapsed_ms", "p50_ms",
            "p95_ms", "records_per_second", "storage_requests", "storage_gets",
            "storage_puts", "storage_heads", "requests_per_record",
            "visible_records", "recall_queries", "exact_recall",
        ]
        summary = {
            "source_sha256": source_sha, "manifest_sha256": manifest_sha,
            "writers": "1", "operations": "2", "records": "2", "groups": "1",
            "mean_group_records": "2", "elapsed_ms": "10", "p50_ms": "5",
            "p95_ms": "6", "records_per_second": "200", "storage_requests": "5",
            "storage_gets": "1", "storage_puts": "4", "storage_heads": "0",
            "requests_per_record": "2.5", "visible_records": "2",
            "recall_queries": "1", "exact_recall": "1",
        }
        self._write_csv(cell / "summary.csv", summary_fields, [summary])
        sample_fields = [
            "writer", "operation", "record_id", "latency_ms", "commit_sequence",
            "committed_records", "group_requests", "group_gets", "group_puts", "group_heads",
        ]
        samples = [
            {"writer": "0", "operation": str(operation), "record_id": f"id-{operation}",
             "latency_ms": str(5 + operation), "commit_sequence": "1",
             "committed_records": "2", "group_requests": "5", "group_gets": "1",
             "group_puts": "4", "group_heads": "0"}
            for operation in range(2)
        ]
        self._write_csv(cell / "samples.csv", sample_fields, samples)
        Path(f"{cell}.resources.txt").write_text("Exit status: 0\n", encoding="utf-8")
        self._write_csv(
            self.root / "summary.csv", ["cell_count", "repetition"] + summary_fields,
            [{"cell_count": "64", "repetition": "1", **summary}],
        )
        self._write_csv(
            self.root / "samples.csv", ["cell_count", "repetition"] + sample_fields,
            [{"cell_count": "64", "repetition": "1", **sample} for sample in samples],
        )
        self._write_csv(
            self.root / "correctness.csv", ["gate", "status"],
            [{"gate": gate, "status": "pass"} for gate in manifest["correctness_gates"]],
        )
        (self.root / "GROUP_COMMIT_SCALABILITY_COMPLETE").touch()

    def tearDown(self) -> None:
        self.temporary.cleanup()

    @staticmethod
    def _write_csv(path: Path, fields: list[str], records: list[dict[str, str]]) -> None:
        with path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=fields)
            writer.writeheader()
            writer.writerows(records)

    def test_valid_terminal_campaign_passes(self) -> None:
        validate(self.root, self.manifest_path)

    def test_incomplete_campaign_fails_before_csv_use(self) -> None:
        (self.root / "GROUP_COMMIT_SCALABILITY_COMPLETE").unlink()
        with self.assertRaisesRegex(ValidationError, "incomplete"):
            validate(self.root, self.manifest_path)

    def test_missing_raw_sample_fails(self) -> None:
        sample_path = self.root / "cells/c64/r01/w1/samples.csv"
        with sample_path.open(newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            fields = reader.fieldnames
            records = list(reader)[:1]
        self._write_csv(sample_path, fields, records)
        with self.assertRaisesRegex(ValidationError, "raw sample count"):
            validate(self.root, self.manifest_path)


if __name__ == "__main__":
    unittest.main()
