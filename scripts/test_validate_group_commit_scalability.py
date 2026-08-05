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
            "protocol_kind": "architecture-qualification",
            "architecture": "local",
            "instance_type": "local",
            "cell_counts": [64],
            "writers": [1],
            "repetitions": 1,
            "operations_per_writer": 2,
            "pipeline_depth_per_writer": 1,
            "worker_lanes": [1],
            "read_queries_per_cell": 1,
            "max_read_segments": 4,
            "dataset_sha256": "b" * 64,
            "min_inserted_id_recall_at_10": 1.0,
            "max_read_p95_ms": 200.0,
            "max_write_p95_ms": 200.0,
            "min_records_per_second": 10_000.0,
            "throughput_gate_writers": [],
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
            f"source_sha256={source_sha}\ndataset_sha256={'b' * 64}\nmanifest_sha256={manifest_sha}\n"
            "architecture=local\ninstance_type=local\n",
            encoding="utf-8",
        )
        (self.root / "dataset.json").write_text("pinned descriptor\n", encoding="utf-8")
        manifest["dataset_sha256"] = hashlib.sha256(
            (self.root / "dataset.json").read_bytes()
        ).hexdigest()
        encoded = json.dumps(manifest, sort_keys=True).encode()
        self.manifest_path.write_bytes(encoded)
        (self.root / "manifest.json").write_bytes(encoded)
        manifest_sha = hashlib.sha256(encoded).hexdigest()
        (self.root / "environment.txt").write_text(
            f"source_sha256={source_sha}\ndataset_sha256={manifest['dataset_sha256']}\n"
            f"manifest_sha256={manifest_sha}\narchitecture=local\ninstance_type=local\n",
            encoding="utf-8",
        )
        cell = self.root / "cells/c64/r01/l1/w1"
        cell.mkdir(parents=True)
        summary_fields = [
            "source_sha256", "dataset_sha256", "manifest_sha256", "writers", "operations", "pipeline_depth", "worker_lanes",
            "records", "groups", "mean_group_records", "elapsed_ms", "drain_ms", "p50_ms",
            "p95_ms", "records_per_second", "vector_mib_per_second", "storage_requests", "storage_gets",
            "storage_puts", "storage_heads", "requests_per_record",
            "visible_records", "recall_queries", "max_read_segments", "inserted_id_recall_at_10",
            "read_p50_ms", "read_p95_ms", "read_storage_requests", "read_storage_gets",
            "read_storage_puts", "read_storage_deletes", "read_storage_heads",
            "read_storage_lists", "read_bytes", "read_segments_searched",
        ]
        summary = {
            "source_sha256": source_sha, "dataset_sha256": manifest["dataset_sha256"], "manifest_sha256": manifest_sha,
            "writers": "1", "operations": "2", "pipeline_depth": "1", "worker_lanes": "1", "records": "2", "groups": "1",
            "mean_group_records": "2", "elapsed_ms": "10", "drain_ms": "3", "p50_ms": "5",
            "p95_ms": "6", "records_per_second": "200", "vector_mib_per_second": "0.006", "storage_requests": "5",
            "storage_gets": "1", "storage_puts": "4", "storage_heads": "0",
            "requests_per_record": "2.5", "visible_records": "2",
            "recall_queries": "1", "max_read_segments": "4", "inserted_id_recall_at_10": "1",
            "read_p50_ms": "5", "read_p95_ms": "6",
            "read_storage_requests": "6", "read_storage_gets": "5",
            "read_storage_puts": "0", "read_storage_deletes": "0",
            "read_storage_heads": "0", "read_storage_lists": "1",
            "read_bytes": "1024", "read_segments_searched": "4",
        }
        self._write_csv(cell / "summary.csv", summary_fields, [summary])
        sample_fields = [
            "writer", "operation", "record_id", "latency_ms", "commit_lane", "commit_sequence",
            "committed_records", "group_requests", "group_gets", "group_puts", "group_heads",
        ]
        samples = [
            {"writer": "0", "operation": str(operation), "record_id": f"id-{operation}",
             "latency_ms": str(5 + operation), "commit_lane": "0", "commit_sequence": "1",
             "committed_records": "2", "group_requests": "5", "group_gets": "1",
             "group_puts": "4", "group_heads": "0"}
            for operation in range(2)
        ]
        self._write_csv(cell / "samples.csv", sample_fields, samples)
        read_fields = [
            "query", "record_id", "hit_id", "contains_record_id", "latency_ms", "requests", "gets",
            "puts", "deletes", "heads", "lists", "bytes_read", "segments_searched",
        ]
        reads = [{
            "query": "0", "record_id": "id-0", "hit_id": "another-neighbor", "contains_record_id": "true", "latency_ms": "6",
            "requests": "6", "gets": "5", "puts": "0", "deletes": "0",
            "heads": "0", "lists": "1", "bytes_read": "1024", "segments_searched": "4",
        }]
        self._write_csv(cell / "reads.csv", read_fields, reads)
        self._write_csv(
            cell / "resources.csv",
            ["elapsed_ms", "cpu_percent", "rss_bytes"],
            [{"elapsed_ms": "0", "cpu_percent": "1", "rss_bytes": "1024"}],
        )
        (cell / "process_exit.txt").write_text("0\n", encoding="utf-8")
        (cell / "PRODUCTION_PERFORMANCE_GATE_COMPLETE").touch()
        for marker in (
            "INGEST_COMPLETE",
            "DRAIN_COMPLETE",
            "POINT_VISIBILITY_COMPLETE",
            "READ_QUALIFICATION_COMPLETE",
        ):
            (cell / marker).touch()
        (cell / "CELL_COMPLETE").touch()
        self._write_csv(
            self.root / "summary.csv", ["cell_count", "repetition", "worker_lanes"] + summary_fields,
            [{"cell_count": "64", "repetition": "1", "worker_lanes": "1", **summary}],
        )
        self._write_csv(
            self.root / "samples.csv", ["cell_count", "repetition", "worker_lanes"] + sample_fields,
            [{"cell_count": "64", "repetition": "1", "worker_lanes": "1", **sample} for sample in samples],
        )
        self._write_csv(
            self.root / "reads.csv", ["cell_count", "repetition", "worker_lanes"] + read_fields,
            [{"cell_count": "64", "repetition": "1", "worker_lanes": "1", **read} for read in reads],
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

    def test_terminal_cell_can_be_validated_before_campaign_completion(self) -> None:
        (self.root / "GROUP_COMMIT_SCALABILITY_COMPLETE").unlink()
        validate(self.root, self.manifest_path, terminal_cell=(64, 1, 1, 1))

    def test_terminal_cell_must_belong_to_frozen_matrix(self) -> None:
        with self.assertRaisesRegex(ValidationError, "frozen matrix"):
            validate(self.root, self.manifest_path, terminal_cell=(2000, 1, 1, 1))

    def test_terminal_cell_rejects_root_failure_marker(self) -> None:
        (self.root / "GROUP_COMMIT_SCALABILITY_FAILED").touch()
        with self.assertRaisesRegex(ValidationError, "campaign has a failure marker"):
            validate(self.root, self.manifest_path, terminal_cell=(64, 1, 1, 1))

    def test_terminal_cell_rejects_subgate_failure_marker(self) -> None:
        (self.root / "cells/c64/r01/l1/w1/PRODUCTION_READ_P95_FAILED").touch()
        with self.assertRaisesRegex(ValidationError, "production sub-gate failure marker"):
            validate(self.root, self.manifest_path, terminal_cell=(64, 1, 1, 1))

    def test_missing_raw_sample_fails(self) -> None:
        sample_path = self.root / "cells/c64/r01/l1/w1/samples.csv"
        with sample_path.open(newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            fields = reader.fieldnames
            records = list(reader)[:1]
        self._write_csv(sample_path, fields, records)
        with self.assertRaisesRegex(ValidationError, "raw sample count"):
            validate(self.root, self.manifest_path)

    def test_production_latency_regression_fails(self) -> None:
        summary_path = self.root / "cells/c64/r01/l1/w1/summary.csv"
        with summary_path.open(newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            fields = reader.fieldnames
            records = list(reader)
        records[0]["p95_ms"] = "200.001"
        self._write_csv(summary_path, fields, records)
        with self.assertRaisesRegex(ValidationError, "production p95"):
            validate(self.root, self.manifest_path)

    def test_production_read_latency_regression_fails(self) -> None:
        summary_path = self.root / "cells/c64/r01/l1/w1/summary.csv"
        with summary_path.open(newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            fields = reader.fieldnames
            records = list(reader)
        records[0]["read_p95_ms"] = "200"
        self._write_csv(summary_path, fields, records)
        with self.assertRaisesRegex(ValidationError, "production read p95"):
            validate(self.root, self.manifest_path)

    def test_missing_performance_marker_fails(self) -> None:
        (self.root / "cells/c64/r01/l1/w1/PRODUCTION_PERFORMANCE_GATE_COMPLETE").unlink()
        with self.assertRaisesRegex(ValidationError, "performance gate marker"):
            validate(self.root, self.manifest_path)

    def test_missing_read_path_telemetry_fails(self) -> None:
        summary_path = self.root / "cells/c64/r01/l1/w1/summary.csv"
        with summary_path.open(newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            fields = [field for field in reader.fieldnames if field != "read_storage_requests"]
            records = list(reader)
        for record in records:
            record.pop("read_storage_requests")
        self._write_csv(summary_path, fields, records)
        with self.assertRaisesRegex(ValidationError, "read path telemetry"):
            validate(self.root, self.manifest_path)

    def test_missing_raw_read_sample_fails(self) -> None:
        (self.root / "cells/c64/r01/l1/w1/reads.csv").unlink()
        with self.assertRaisesRegex(ValidationError, "raw read sample"):
            validate(self.root, self.manifest_path)

    def test_missing_phase_marker_fails(self) -> None:
        (self.root / "cells/c64/r01/l1/w1/DRAIN_COMPLETE").unlink()
        with self.assertRaisesRegex(ValidationError, "missing phase marker"):
            validate(self.root, self.manifest_path)

    def test_cell_failure_marker_fails(self) -> None:
        (self.root / "cells/c64/r01/l1/w1/CELL_FAILED").touch()
        with self.assertRaisesRegex(ValidationError, "cell has a failure marker"):
            validate(self.root, self.manifest_path)

    def test_missing_sampled_resource_telemetry_fails(self) -> None:
        (self.root / "cells/c64/r01/l1/w1/resources.csv").unlink()
        with self.assertRaisesRegex(ValidationError, "resource telemetry"):
            validate(self.root, self.manifest_path)

    def test_dataset_descriptor_identity_drift_fails(self) -> None:
        (self.root / "dataset.json").write_text("tampered descriptor\n", encoding="utf-8")
        with self.assertRaisesRegex(ValidationError, "dataset SHA-256 mismatch"):
            validate(self.root, self.manifest_path)

    def test_inserted_id_may_be_below_the_first_neighbor_but_must_be_in_top_ten(self) -> None:
        validate(self.root, self.manifest_path)
        reads_path = self.root / "cells/c64/r01/l1/w1/reads.csv"
        with reads_path.open(newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            fields = reader.fieldnames
            records = list(reader)
        records[0]["contains_record_id"] = "false"
        self._write_csv(reads_path, fields, records)
        with self.assertRaisesRegex(ValidationError, "raw inserted-ID recall failure"):
            validate(self.root, self.manifest_path)


if __name__ == "__main__":
    unittest.main()
