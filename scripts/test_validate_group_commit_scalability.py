#!/usr/bin/env python3

import csv
import hashlib
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
import validate_group_commit_scalability as validator
from evaluate_exact_bound_shadow import evaluate
from validate_group_commit_scalability import (
    ValidationError,
    direct_acknowledgement_request_contract,
    lane_receipt_evidence,
    validate,
    validate_process_identity,
)


class ValidatorTests(unittest.TestCase):
    def test_residual_pq_manifest_contract_rejects_any_shape_or_gate_drift(self) -> None:
        self.assertTrue(hasattr(validator, "validate_residual_pq_manifest"))
        manifest = json.loads(
            (
                Path(__file__).parent.parent
                / "docs/research/group-commit-residual-pq-local-qualification.json"
            ).read_text()
        )
        validator.validate_residual_pq_manifest(manifest)
        mutations = (
            ("cell count", lambda value: value.update(cell_counts=[1999])),
            ("writer count", lambda value: value.update(writers=[31])),
            ("dimensions", lambda value: value.update(dimensions=96)),
            (
                "residual code width",
                lambda value: value["exact_bound_shadow"].update(
                    residual_code_bytes=63
                ),
            ),
            (
                "survivor gate",
                lambda value: value["exact_bound_shadow"].update(
                    max_survivor_p95=12
                ),
            ),
            (
                "latency optimization contract",
                lambda value: value["optimization_contract"].update(
                    hard_read_p95_ms=201
                ),
            ),
        )
        for label, mutate in mutations:
            with self.subTest(label=label):
                candidate = json.loads(json.dumps(manifest))
                mutate(candidate)
                with self.assertRaisesRegex(ValidationError, label):
                    validator.validate_residual_pq_manifest(candidate)

    def test_residual_pq_rows_require_physical_waves_and_allocation_evidence(self) -> None:
        shadow = {
            "global_exact_bound_candidates": "16",
            "global_exact_bound_survivors": "11",
            "global_exact_bound_fail_open": "0",
            "global_exact_bound_containment_failures": "0",
            "global_exact_bound_baseline_reads": "9",
            "global_exact_bound_baseline_bytes": "49152",
            "global_exact_bound_predicted_reads": "7",
            "global_exact_bound_predicted_bytes": "33792",
            "global_exact_bound_cpu_us": "91",
            "global_exact_bound_certificate_kind": "residual-pq-v8",
            "global_exact_bound_exact_backing_reads": "9",
            "global_exact_bound_exact_backing_bytes": "49152",
            "global_exact_bound_residual_bytes": "1088",
            "global_exact_bound_residual_scan_bytes": "69632",
        }
        with self.assertRaisesRegex(ValidationError, "incomplete residual-PQ telemetry"):
            validator.validate_exact_bound_shadow_row(
                shadow, "query", True, require_residual_pq=True
            )
        shadow.update(
            global_exact_bound_predicted_waves="1",
            global_exact_bound_certificate_scratch_allocations="3",
        )
        validator.validate_exact_bound_shadow_row(
            shadow, "query", True, require_residual_pq=True
        )
        shadow["global_exact_bound_predicted_waves"] = "0"
        with self.assertRaisesRegex(ValidationError, "request-wave evidence is empty"):
            validator.validate_exact_bound_shadow_row(
                shadow, "query", True, require_residual_pq=True
            )

    def test_residual_pq_storage_trace_requires_uncached_exact_reads(self) -> None:
        write_only = [
            {
                "operation": "write",
                "object_role": "lane_head",
                "status": "ok",
                "cache_state": "backing",
                "request_count": "1",
                "bytes_fetched": "0",
            }
        ]
        with self.assertRaisesRegex(ValidationError, "missing exact-path trace"):
            validator.validate_residual_pq_storage_trace(write_only, "cell")
        exact = {
            "operation": "read",
            "object_role": "exact_vectors",
            "status": "ok",
            "cache_state": "backing",
            "request_count": "1",
            "bytes_fetched": "3072",
        }
        validator.validate_residual_pq_storage_trace([*write_only, exact], "cell")
        exact["cache_state"] = "hit"
        with self.assertRaisesRegex(ValidationError, "cache hit"):
            validator.validate_residual_pq_storage_trace([*write_only, exact], "cell")

    def test_request_contract_distinguishes_local_smoke_from_s3_evidence(self) -> None:
        self.assertEqual(
            direct_acknowledgement_request_contract("smoke"),
            (4, 0, 3, 0, 1, 0),
        )
        self.assertEqual(
            direct_acknowledgement_request_contract("production"),
            (2, 0, 2, 0, 0, 0),
        )
        self.assertEqual(
            direct_acknowledgement_request_contract("architecture-qualification"),
            (2, 0, 2, 0, 0, 0),
        )

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
            "writer_instance_policy": "one-per-writer",
            "writer_process_policy": "one-process-per-writer",
            "repetitions": 1,
            "operations_per_writer": 2,
            "dimensions": 8,
            "pipeline_depth_per_writer": 1,
            "worker_lanes": [1],
            "read_queries_per_cell": 1,
            "max_read_segments": 4,
            "dataset_sha256": "b" * 64,
            "min_inserted_id_recall_at_10": 1.0,
            "max_read_p95_ms": 200.0,
            "max_write_p95_ms": 200.0,
            "max_acknowledgement_bytes": 4096,
            "max_physical_write_amplification": 16.0,
            "min_records_per_second": 10_000.0,
            "min_end_to_end_records_per_second": 100.0,
            "throughput_gate_writers": [],
            "correctness_gates": [
                "same_id_last_write_wins",
                "independent_writer_instances",
                "prepare_failure",
                "crash_recovery",
                "preregistered_lane_factor_safety",
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
            "source_sha256",
            "dataset_sha256",
            "manifest_sha256",
            "writers",
            "writer_instances",
            "operations",
            "pipeline_depth",
            "worker_lanes",
            "records",
            "groups",
            "mean_group_records",
            "elapsed_ms",
            "drain_ms",
            "end_to_end_records_per_second",
            "p50_ms",
            "p95_ms",
            "records_per_second",
            "vector_mib_per_second",
            "storage_requests",
            "storage_gets",
            "storage_puts",
            "storage_heads",
            "requests_per_record",
            "total_acknowledgement_bytes",
            "max_acknowledgement_bytes",
            "visible_records",
            "recall_queries",
            "max_read_segments",
            "inserted_id_recall_at_10",
            "active_tail_read_p50_ms",
            "active_tail_read_p95_ms",
            "read_p50_ms",
            "read_p95_ms",
            "read_storage_requests",
            "read_storage_gets",
            "read_storage_puts",
            "read_storage_deletes",
            "read_storage_heads",
            "read_storage_lists",
            "read_bytes",
            "read_segments_searched",
        ]
        summary = {
            "source_sha256": source_sha,
            "dataset_sha256": manifest["dataset_sha256"],
            "manifest_sha256": manifest_sha,
            "writers": "1",
            "writer_instances": "1",
            "operations": "2",
            "pipeline_depth": "1",
            "worker_lanes": "1",
            "records": "2",
            "groups": "1",
            "mean_group_records": "2",
            "elapsed_ms": "10",
            "drain_ms": "10",
            "end_to_end_records_per_second": "100",
            "p50_ms": "6",
            "p95_ms": "6",
            "records_per_second": "200",
            "vector_mib_per_second": "0.006103515625",
            "storage_requests": "2",
            "storage_gets": "0",
            "storage_puts": "2",
            "storage_heads": "0",
            "requests_per_record": "1",
            "total_acknowledgement_bytes": "2048",
            "max_acknowledgement_bytes": "2048",
            "visible_records": "2",
            "recall_queries": "1",
            "max_read_segments": "4",
            "inserted_id_recall_at_10": "1",
            "read_p50_ms": "6",
            "read_p95_ms": "6",
            "active_tail_read_p50_ms": "7",
            "active_tail_read_p95_ms": "7",
            "read_storage_requests": "6",
            "read_storage_gets": "5",
            "read_storage_puts": "0",
            "read_storage_deletes": "0",
            "read_storage_heads": "0",
            "read_storage_lists": "1",
            "read_bytes": "1024",
            "read_segments_searched": "4",
        }
        self._write_csv(cell / "summary.csv", summary_fields, [summary])
        sample_fields = [
            "writer",
            "writer_instance",
            "process_id",
            "operation",
            "record_id",
            "latency_ms",
            "commit_lane",
            "commit_sequence",
            "committed_records",
            "acknowledgement_bytes",
            "group_requests",
            "group_gets",
            "group_puts",
            "group_heads",
            "lane_receipts",
        ]
        samples = [
            {
                "writer": "0",
                "writer_instance": "0",
                "process_id": "1234",
                "operation": str(operation),
                "record_id": f"id-{operation}",
                "latency_ms": str(5 + operation),
                "commit_lane": "0",
                "commit_sequence": "1",
                "committed_records": "2",
                "acknowledgement_bytes": "2048",
                "group_requests": "2",
                "group_gets": "0",
                "group_puts": "2",
                "group_heads": "0",
                "lane_receipts": f"0:1:3:2:2048:2:0:2:0:0:0:{'c' * 64}:{'d' * 64}",
            }
            for operation in range(2)
        ]
        self._write_csv(cell / "samples.csv", sample_fields, samples)
        read_fields = [
            "query",
            "record_id",
            "hit_id",
            "contains_record_id",
            "latency_ms",
            "requests",
            "gets",
            "puts",
            "deletes",
            "heads",
            "lists",
            "bytes_read",
            "segments_searched",
        ]
        reads = [
            {
                "query": "0",
                "record_id": "id-0",
                "hit_id": "another-neighbor",
                "contains_record_id": "true",
                "latency_ms": "6",
                "requests": "6",
                "gets": "5",
                "puts": "0",
                "deletes": "0",
                "heads": "0",
                "lists": "1",
                "bytes_read": "1024",
                "segments_searched": "4",
            }
        ]
        self._write_csv(cell / "reads.csv", read_fields, reads)
        active_tail_reads = [{**reads[0], "latency_ms": "7"}]
        self._write_csv(cell / "active-tail-reads.csv", read_fields, active_tail_reads)
        self._write_csv(
            cell / "storage-access.csv",
            [
                "operation",
                "object_role",
                "path",
                "physical_format",
                "object_bytes",
                "request_count",
                "bytes_fetched",
                "logical_projection",
                "row_selection",
                "logical_rows_requested",
                "logical_rows_decoded",
                "decode_cpu_ns",
                "cache_state",
                "status",
            ],
            [
                {
                    "operation": "write",
                    "object_role": "lane_head",
                    "path": "lane-log/lanes/0000/HEAD",
                    "physical_format": "packed",
                    "object_bytes": "512",
                    "request_count": "1",
                    "bytes_fetched": "0",
                    "logical_projection": "",
                    "row_selection": "",
                    "logical_rows_requested": "",
                    "logical_rows_decoded": "",
                    "decode_cpu_ns": "",
                    "cache_state": "backing",
                    "status": "ok",
                }
            ],
        )
        self._write_csv(
            cell / "resources.csv",
            ["elapsed_ms", "cpu_percent", "rss_bytes"],
            [
                {"elapsed_ms": "0", "cpu_percent": "1", "rss_bytes": "1024"},
                {"elapsed_ms": "20", "cpu_percent": "1", "rss_bytes": "1024"},
            ],
        )
        (cell / "process_exit.txt").write_text("0\n", encoding="utf-8")
        (cell / "PRODUCTION_PERFORMANCE_GATE_COMPLETE").touch()
        for marker in (
            "INGEST_COMPLETE",
            "ACTIVE_TAIL_READ_QUALIFICATION_COMPLETE",
            "DRAIN_COMPLETE",
            "POINT_VISIBILITY_COMPLETE",
            "READ_QUALIFICATION_COMPLETE",
        ):
            (cell / marker).touch()
        (cell / "CELL_COMPLETE").touch()
        self._write_csv(
            self.root / "summary.csv",
            ["cell_count", "repetition", "worker_lanes"] + summary_fields,
            [{"cell_count": "64", "repetition": "1", "worker_lanes": "1", **summary}],
        )
        self._write_csv(
            self.root / "samples.csv",
            ["cell_count", "repetition", "worker_lanes"] + sample_fields,
            [
                {"cell_count": "64", "repetition": "1", "worker_lanes": "1", **sample}
                for sample in samples
            ],
        )
        self._write_csv(
            self.root / "reads.csv",
            ["cell_count", "repetition", "worker_lanes"] + read_fields,
            [
                {"cell_count": "64", "repetition": "1", "worker_lanes": "1", **read}
                for read in reads
            ],
        )
        self._write_csv(
            self.root / "active-tail-reads.csv",
            ["cell_count", "repetition", "worker_lanes"] + read_fields,
            [
                {"cell_count": "64", "repetition": "1", "worker_lanes": "1", **read}
                for read in active_tail_reads
            ],
        )
        self._write_csv(
            self.root / "correctness.csv",
            ["gate", "status"],
            [
                {"gate": gate, "status": "pass"}
                for gate in manifest["correctness_gates"]
            ],
        )
        (self.root / "GROUP_COMMIT_SCALABILITY_COMPLETE").touch()

    def tearDown(self) -> None:
        self.temporary.cleanup()

    @staticmethod
    def _write_csv(
        path: Path, fields: list[str], records: list[dict[str, str]]
    ) -> None:
        with path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=fields)
            writer.writeheader()
            writer.writerows(records)

    def _rebuild_single_cell_aggregate(self, name: str) -> None:
        source = self.root / "cells/c64/r01/l1/w1" / name
        with source.open(newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            source_fields = list(reader.fieldnames or [])
            records = list(reader)
        prefix = ["cell_count", "repetition"]
        if name != "summary.csv":
            prefix.append("worker_lanes")
        identity = {"cell_count": "64", "repetition": "1"}
        if name != "summary.csv":
            identity["worker_lanes"] = "1"
        self._write_csv(
            self.root / name,
            prefix + source_fields,
            [{**identity, **record} for record in records],
        )

    def _mark_read_performance_failure(self) -> None:
        cell = self.root / "cells/c64/r01/l1/w1"
        (self.root / "GROUP_COMMIT_SCALABILITY_COMPLETE").unlink()
        (self.root / "GROUP_COMMIT_SCALABILITY_FAILED").touch()
        (cell / "CELL_COMPLETE").unlink()
        (cell / "CELL_FAILED").touch()
        (cell / "PRODUCTION_PERFORMANCE_GATE_COMPLETE").unlink()
        (cell / "PRODUCTION_PERFORMANCE_GATE_FAILED").touch()
        (cell / "PRODUCTION_READ_P95_FAILED").touch()
        (cell / "process_exit.txt").write_text("1\n", encoding="utf-8")

        summary_path = cell / "summary.csv"
        with summary_path.open(newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            summary_fields = reader.fieldnames
            summaries = list(reader)
        summaries[0]["read_p50_ms"] = "200"
        summaries[0]["read_p95_ms"] = "200"
        self._write_csv(summary_path, summary_fields, summaries)

        reads_path = cell / "reads.csv"
        with reads_path.open(newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            read_fields = reader.fieldnames
            reads = list(reader)
        reads[0]["latency_ms"] = "200"
        self._write_csv(reads_path, read_fields, reads)

    def test_valid_terminal_campaign_passes(self) -> None:
        validate(self.root, self.manifest_path)

    def test_preterminal_root_validates_complete_artifacts_before_marker(self) -> None:
        (self.root / "GROUP_COMMIT_SCALABILITY_COMPLETE").unlink()
        with self.assertRaisesRegex(ValidationError, "campaign is incomplete"):
            validate(self.root, self.manifest_path)
        validate(self.root, self.manifest_path, preterminal_root=True)

    def test_thread_only_evidence_cannot_claim_independent_writers(self) -> None:
        cell = self.root / "cells/c64/r01/l1/w1/summary.csv"
        with cell.open(newline="", encoding="utf-8") as handle:
            records = list(csv.DictReader(handle))
        records[0]["writer_instances"] = "0"
        self._write_csv(cell, list(records[0]), records)
        with self.assertRaisesRegex(ValidationError, "writer instance drift"):
            validate(self.root, self.manifest_path)

    def test_one_process_per_writer_policy_requires_process_identity(self) -> None:
        sample_path = self.root / "cells/c64/r01/l1/w1/samples.csv"
        with sample_path.open(newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            samples = list(reader)
        for sample in samples:
            sample.pop("process_id")
        fields = [field for field in samples[0] if field != "process_id"]
        self._write_csv(sample_path, fields, samples)
        with self.assertRaisesRegex(ValidationError, "process identity"):
            validate(self.root, self.manifest_path)

    def test_one_process_per_writer_policy_rejects_shared_process_identity(
        self,
    ) -> None:
        samples = [
            {"writer": "0", "process_id": "1234"},
            {"writer": "1", "process_id": "1234"},
        ]
        with self.assertRaisesRegex(ValidationError, "process identity"):
            validate_process_identity(samples, writers=2, cell=Path("cell"))

    def test_bulk_samples_preserve_every_lane_receipt(self) -> None:
        evidence = lane_receipt_evidence(
            {
                "lane_receipts": f"0:7:3:2:100:2:0:2:0:0:0:{'a' * 64}:{'b' * 64};"
                f"1:9:3:1:60:2:0:2:0:0:0:{'c' * 64}:{'d' * 64}"
            }
        )
        self.assertEqual(len(evidence), 2)
        self.assertEqual(evidence[0][:5], (0, 7, 3, 2, 100))
        self.assertEqual(evidence[1][5:11], (2, 0, 2, 0, 0, 0))
        self.assertEqual(evidence[1][11:], ("c" * 64, "d" * 64))
        normalized = evidence[0][3:]
        self.assertEqual(
            normalized[5], 0, "delete requests occupy normalized slot five"
        )
        self.assertEqual(normalized[6], 0, "HEAD requests occupy normalized slot six")

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
        with self.assertRaisesRegex(
            ValidationError, "production sub-gate failure marker"
        ):
            validate(self.root, self.manifest_path, terminal_cell=(64, 1, 1, 1))

    def test_terminal_performance_failure_reconciles_raw_evidence(self) -> None:
        self._mark_read_performance_failure()
        validate(
            self.root,
            self.manifest_path,
            failed_terminal_cell=(64, 1, 1, 1),
        )

    def test_cli_recovers_one_terminal_performance_failure_explicitly(self) -> None:
        self._mark_read_performance_failure()
        result = subprocess.run(
            [
                sys.executable,
                str(Path(__file__).parent / "validate_group_commit_scalability.py"),
                "--manifest",
                str(self.manifest_path),
                "--failed-terminal-cell",
                "c64/r1/l1/w1",
                str(self.root),
            ],
            check=False,
            capture_output=True,
            text=True,
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("terminal failed cell c64/r1/l1/w1", result.stdout)

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

    def test_acknowledgement_byte_bound_fails_closed(self) -> None:
        summary_path = self.root / "cells/c64/r01/l1/w1/summary.csv"
        sample_path = self.root / "cells/c64/r01/l1/w1/samples.csv"
        with summary_path.open(newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            fields = reader.fieldnames
            records = list(reader)
        records[0]["max_acknowledgement_bytes"] = "4097"
        records[0]["total_acknowledgement_bytes"] = "4097"
        self._write_csv(summary_path, fields, records)
        with sample_path.open(newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            fields = reader.fieldnames
            records = list(reader)
        for record in records:
            record["acknowledgement_bytes"] = "4097"
            receipt = record["lane_receipts"].split(":")
            receipt[4] = "4097"
            record["lane_receipts"] = ":".join(receipt)
        self._write_csv(sample_path, fields, records)
        with self.assertRaisesRegex(ValidationError, "acknowledgement byte bound"):
            validate(self.root, self.manifest_path)

    def test_missing_authenticated_lane_receipt_fails_closed(self) -> None:
        sample_path = self.root / "cells/c64/r01/l1/w1/samples.csv"
        with sample_path.open(newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            records = list(reader)
        fields = [field for field in reader.fieldnames if field != "lane_receipts"]
        for record in records:
            record.pop("lane_receipts")
        self._write_csv(sample_path, fields, records)
        with self.assertRaisesRegex(ValidationError, "authenticated lane receipt"):
            validate(self.root, self.manifest_path)

    def test_invalid_extent_checksum_fails_closed(self) -> None:
        sample_path = self.root / "cells/c64/r01/l1/w1/samples.csv"
        with sample_path.open(newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            fields = reader.fieldnames
            records = list(reader)
        receipt = records[0]["lane_receipts"].split(":")
        receipt[11] = "not-a-checksum"
        records[0]["lane_receipts"] = ":".join(receipt)
        self._write_csv(sample_path, fields, records)
        with self.assertRaisesRegex(ValidationError, "extent checksum"):
            validate(self.root, self.manifest_path)

    def test_direct_acknowledgement_request_drift_fails_closed(self) -> None:
        sample_path = self.root / "cells/c64/r01/l1/w1/samples.csv"
        with sample_path.open(newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            fields = reader.fieldnames
            records = list(reader)
        for record in records:
            receipt = record["lane_receipts"].split(":")
            receipt[5] = "3"
            receipt[6] = "1"
            record["lane_receipts"] = ":".join(receipt)
        self._write_csv(sample_path, fields, records)
        with self.assertRaisesRegex(
            ValidationError, "direct acknowledgement request contract"
        ):
            validate(self.root, self.manifest_path)

    def test_physical_write_amplification_fails_closed(self) -> None:
        trace_path = self.root / "cells/c64/r01/l1/w1/storage-access.csv"
        with trace_path.open(newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            fields = reader.fieldnames
            records = list(reader)
        records[0]["object_bytes"] = str(2 * 8 * 4 * 16 + 1)
        self._write_csv(trace_path, fields, records)
        with self.assertRaisesRegex(ValidationError, "physical write amplification"):
            validate(self.root, self.manifest_path)

    def test_summary_write_percentiles_must_match_raw_samples(self) -> None:
        summary_path = self.root / "cells/c64/r01/l1/w1/summary.csv"
        with summary_path.open(newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            fields = reader.fieldnames
            records = list(reader)
        records[0]["p95_ms"] = "5"
        self._write_csv(summary_path, fields, records)
        with self.assertRaisesRegex(
            ValidationError, "write p95 does not match raw samples"
        ):
            validate(self.root, self.manifest_path)

    def test_summary_read_percentiles_must_match_raw_samples(self) -> None:
        summary_path = self.root / "cells/c64/r01/l1/w1/summary.csv"
        with summary_path.open(newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            fields = reader.fieldnames
            records = list(reader)
        records[0]["read_p95_ms"] = "5"
        self._write_csv(summary_path, fields, records)
        with self.assertRaisesRegex(
            ValidationError, "read p95 does not match raw samples"
        ):
            validate(self.root, self.manifest_path)

    def test_summary_throughput_must_match_records_and_elapsed_time(self) -> None:
        summary_path = self.root / "cells/c64/r01/l1/w1/summary.csv"
        with summary_path.open(newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            fields = reader.fieldnames
            records = list(reader)
        records[0]["elapsed_ms"] = "20"
        self._write_csv(summary_path, fields, records)
        with self.assertRaisesRegex(
            ValidationError, "throughput does not match records and elapsed time"
        ):
            validate(self.root, self.manifest_path)

    def test_end_to_end_throughput_must_include_drain_time(self) -> None:
        summary_path = self.root / "cells/c64/r01/l1/w1/summary.csv"
        with summary_path.open(newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            fields = reader.fieldnames
            records = list(reader)
        records[0]["end_to_end_records_per_second"] = "200"
        self._write_csv(summary_path, fields, records)
        with self.assertRaisesRegex(
            ValidationError, "end-to-end throughput does not include drain"
        ):
            validate(self.root, self.manifest_path)

    def test_summary_derived_write_metrics_must_reconcile(self) -> None:
        summary_path = self.root / "cells/c64/r01/l1/w1/summary.csv"
        with summary_path.open(newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            fields = reader.fieldnames
            records = list(reader)
        records[0]["requests_per_record"] = "2"
        self._write_csv(summary_path, fields, records)
        with self.assertRaisesRegex(
            ValidationError, "requests per record does not reconcile"
        ):
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
        (
            self.root / "cells/c64/r01/l1/w1/PRODUCTION_PERFORMANCE_GATE_COMPLETE"
        ).unlink()
        with self.assertRaisesRegex(ValidationError, "performance gate marker"):
            validate(self.root, self.manifest_path)

    def test_missing_read_path_telemetry_fails(self) -> None:
        summary_path = self.root / "cells/c64/r01/l1/w1/summary.csv"
        with summary_path.open(newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            fields = [
                field for field in reader.fieldnames if field != "read_storage_requests"
            ]
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

    def test_active_tail_latency_regression_fails(self) -> None:
        summary_path = self.root / "cells/c64/r01/l1/w1/summary.csv"
        with summary_path.open(newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            fields = reader.fieldnames
            records = list(reader)
        records[0]["active_tail_read_p95_ms"] = "200"
        self._write_csv(summary_path, fields, records)
        with self.assertRaisesRegex(ValidationError, "active-tail read p95"):
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

    def test_resource_telemetry_must_bracket_ingest_and_drain(self) -> None:
        resource_path = self.root / "cells/c64/r01/l1/w1/resources.csv"
        self._write_csv(
            resource_path,
            ["elapsed_ms", "cpu_percent", "rss_bytes"],
            [
                {"elapsed_ms": "0", "cpu_percent": "1", "rss_bytes": "1024"},
                {"elapsed_ms": "19", "cpu_percent": "1", "rss_bytes": "1024"},
            ],
        )
        with self.assertRaisesRegex(
            ValidationError, "does not bracket ingest and drain"
        ):
            validate(self.root, self.manifest_path)

    def test_dataset_descriptor_identity_drift_fails(self) -> None:
        (self.root / "dataset.json").write_text(
            "tampered descriptor\n", encoding="utf-8"
        )
        with self.assertRaisesRegex(ValidationError, "dataset SHA-256 mismatch"):
            validate(self.root, self.manifest_path)

    def test_inserted_id_may_be_below_the_first_neighbor_but_must_be_in_top_ten(
        self,
    ) -> None:
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

    def test_negative_global_phase_telemetry_fails(self) -> None:
        reads_path = self.root / "cells/c64/r01/l1/w1/reads.csv"
        with reads_path.open(newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            fields = list(reader.fieldnames or [])
            records = list(reader)
        fields.extend(
            (
                "global_base_approximate_us",
                "global_base_exact_rerank_us",
                "global_delta_approximate_us",
                "global_delta_exact_rerank_us",
                "global_delta_wait_us",
            )
        )
        for record in records:
            for field in fields[-5:]:
                record[field] = "0"
        records[0]["global_delta_exact_rerank_us"] = "-1"
        self._write_csv(reads_path, fields, records)
        with self.assertRaisesRegex(ValidationError, "negative global phase telemetry"):
            validate(self.root, self.manifest_path)

    def test_partial_exact_bound_shadow_telemetry_fails(self) -> None:
        reads_path = self.root / "cells/c64/r01/l1/w1/reads.csv"
        with reads_path.open(newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            fields = list(reader.fieldnames or [])
            records = list(reader)
        fields.append("global_exact_bound_candidates")
        records[0]["global_exact_bound_candidates"] = "16"
        self._write_csv(reads_path, fields, records)
        with self.assertRaisesRegex(ValidationError, "incomplete exact-bound shadow"):
            validate(self.root, self.manifest_path)

    def test_aggregate_read_rows_must_match_terminal_cell_rows(self) -> None:
        aggregate = self.root / "reads.csv"
        with aggregate.open(newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            fields = list(reader.fieldnames or [])
            records = list(reader)
        records[0]["latency_ms"] = "999"
        self._write_csv(aggregate, fields, records)
        with self.assertRaisesRegex(ValidationError, "aggregate reads content mismatch"):
            validate(self.root, self.manifest_path)

    def test_exact_bound_containment_failure_must_fail_the_query_open(self) -> None:
        reads_path = self.root / "cells/c64/r01/l1/w1/reads.csv"
        with reads_path.open(newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            fields = list(reader.fieldnames or [])
            records = list(reader)
        shadow = {
            "global_exact_bound_candidates": "16",
            "global_exact_bound_survivors": "11",
            "global_exact_bound_fail_open": "0",
            "global_exact_bound_containment_failures": "1",
            "global_exact_bound_baseline_reads": "7",
            "global_exact_bound_baseline_bytes": "33792",
            "global_exact_bound_predicted_reads": "7",
            "global_exact_bound_predicted_bytes": "33792",
            "global_exact_bound_cpu_us": "91",
        }
        fields.extend(shadow)
        records[0].update(shadow)
        self._write_csv(reads_path, fields, records)
        with self.assertRaisesRegex(ValidationError, "containment failure did not fail open"):
            validate(self.root, self.manifest_path)

    def test_exact_bound_prediction_cannot_exceed_its_baseline(self) -> None:
        reads_path = self.root / "cells/c64/r01/l1/w1/reads.csv"
        with reads_path.open(newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            fields = list(reader.fieldnames or [])
            records = list(reader)
        shadow = {
            "global_exact_bound_candidates": "16",
            "global_exact_bound_survivors": "11",
            "global_exact_bound_fail_open": "0",
            "global_exact_bound_containment_failures": "0",
            "global_exact_bound_baseline_reads": "6",
            "global_exact_bound_baseline_bytes": "32000",
            "global_exact_bound_predicted_reads": "7",
            "global_exact_bound_predicted_bytes": "33792",
            "global_exact_bound_cpu_us": "91",
        }
        fields.extend(shadow)
        records[0].update(shadow)
        self._write_csv(reads_path, fields, records)
        with self.assertRaisesRegex(ValidationError, "prediction exceeds exact-plan baseline"):
            validate(self.root, self.manifest_path)

    def test_partial_v8_exact_bound_telemetry_fails(self) -> None:
        reads_path = self.root / "cells/c64/r01/l1/w1/reads.csv"
        with reads_path.open(newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            fields = list(reader.fieldnames or [])
            records = list(reader)
        shadow = {
            "global_exact_bound_candidates": "16",
            "global_exact_bound_survivors": "11",
            "global_exact_bound_fail_open": "0",
            "global_exact_bound_containment_failures": "0",
            "global_exact_bound_baseline_reads": "7",
            "global_exact_bound_baseline_bytes": "33792",
            "global_exact_bound_predicted_reads": "7",
            "global_exact_bound_predicted_bytes": "33792",
            "global_exact_bound_cpu_us": "91",
            "global_exact_bound_certificate_kind": "residual-pq-v8",
        }
        fields.extend(shadow)
        records[0].update(shadow)
        self._write_csv(reads_path, fields, records)
        with self.assertRaisesRegex(ValidationError, "incomplete V8 exact-bound telemetry"):
            validate(self.root, self.manifest_path)

    def test_complete_v8_exact_bound_telemetry_is_valid(self) -> None:
        for name in ("reads.csv", "active-tail-reads.csv"):
            reads_path = self.root / "cells/c64/r01/l1/w1" / name
            with reads_path.open(newline="", encoding="utf-8") as handle:
                reader = csv.DictReader(handle)
                fields = list(reader.fieldnames or [])
                records = list(reader)
            shadow = {
                "global_exact_bound_candidates": "16",
                "global_exact_bound_survivors": "11",
                "global_exact_bound_fail_open": "0",
                "global_exact_bound_containment_failures": "0",
                "global_exact_bound_baseline_reads": "7",
                "global_exact_bound_baseline_bytes": "33792",
                "global_exact_bound_predicted_reads": "7",
                "global_exact_bound_predicted_bytes": "33792",
                "global_exact_bound_cpu_us": "91",
                "global_exact_bound_certificate_kind": "residual-pq-v8",
                "global_exact_bound_exact_backing_reads": "7",
                "global_exact_bound_exact_backing_bytes": "33792",
                "global_exact_bound_residual_bytes": "1088",
                "global_exact_bound_residual_scan_bytes": "69632",
            }
            fields.extend(shadow)
            records[0].update(shadow)
            self._write_csv(reads_path, fields, records)
            self._rebuild_single_cell_aggregate(name)
        validate(self.root, self.manifest_path)

    def test_legacy_exact_bound_shadow_without_baseline_remains_valid(self) -> None:
        for name in ("reads.csv", "active-tail-reads.csv"):
            reads_path = self.root / "cells/c64/r01/l1/w1" / name
            with reads_path.open(newline="", encoding="utf-8") as handle:
                reader = csv.DictReader(handle)
                fields = list(reader.fieldnames or [])
                records = list(reader)
            shadow = {
                "global_exact_bound_candidates": "16",
                "global_exact_bound_survivors": "11",
                "global_exact_bound_fail_open": "0",
                "global_exact_bound_containment_failures": "0",
                "global_exact_bound_predicted_reads": "7",
                "global_exact_bound_predicted_bytes": "33792",
                "global_exact_bound_cpu_us": "91",
            }
            fields.extend(shadow)
            records[0].update(shadow)
            self._write_csv(reads_path, fields, records)
            self._rebuild_single_cell_aggregate(name)
        validate(self.root, self.manifest_path)


class ResidualPqTerminalRootTests(unittest.TestCase):
    @staticmethod
    def _write_csv(
        path: Path, fields: list[str], records: list[dict[str, str]]
    ) -> None:
        with path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=fields)
            writer.writeheader()
            writer.writerows(records)

    def test_structurally_valid_terminal_residual_pq_root(self) -> None:
        repository = Path(__file__).parent.parent
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "results"
            cell = root / "cells/c2000/r01/l1/w32"
            cell.mkdir(parents=True)
            manifest_path = (
                repository
                / "docs/research/group-commit-residual-pq-local-qualification.json"
            )
            frozen = manifest_path.read_bytes()
            (root / "manifest.json").write_bytes(frozen)
            (root / "dataset.json").write_bytes(
                (
                    repository
                    / "scripts/fixtures/cohere-medium-1M-dataset.json"
                ).read_bytes()
            )
            manifest = json.loads(frozen)
            manifest_sha = hashlib.sha256(frozen).hexdigest()
            source_sha = "a" * 64
            (root / "environment.txt").write_text(
                f"source_sha256={source_sha}\n"
                f"dataset_sha256={manifest['dataset_sha256']}\n"
                f"manifest_sha256={manifest_sha}\n"
                "architecture=aarch64\ninstance_type=devbox-local\n",
                encoding="utf-8",
            )

            sample_fields = [
                "writer",
                "writer_instance",
                "process_id",
                "operation",
                "batch_records",
                "record_ids",
                "first_record_id",
                "latency_ms",
                "commit_lane",
                "commit_sequence",
                "committed_records",
                "acknowledgement_bytes",
                "group_requests",
                "group_gets",
                "group_puts",
                "group_heads",
                "lane_receipts",
            ]
            samples = []
            for writer in range(32):
                for operation in range(32):
                    record_ids = [
                        f"id-{writer}-{operation}-{record}" for record in range(16)
                    ]
                    extent = hashlib.sha256(
                        f"extent-{writer}-{operation}".encode()
                    ).hexdigest()
                    head = hashlib.sha256(
                        f"head-{writer}-{operation}".encode()
                    ).hexdigest()
                    samples.append(
                        {
                            "writer": str(writer),
                            "writer_instance": str(writer),
                            "process_id": str(10_000 + writer),
                            "operation": str(operation),
                            "batch_records": "16",
                            "record_ids": "|".join(record_ids),
                            "first_record_id": record_ids[0],
                            "latency_ms": "1",
                            "commit_lane": str(writer),
                            "commit_sequence": str(operation + 1),
                            "committed_records": "16",
                            "acknowledgement_bytes": "4096",
                            "group_requests": "4",
                            "group_gets": "0",
                            "group_puts": "3",
                            "group_heads": "1",
                            "lane_receipts": (
                                f"{writer}:{operation + 1}:{operation + 1}:"
                                f"16:4096:4:0:3:0:1:0:{extent}:{head}"
                            ),
                        }
                    )
            self._write_csv(cell / "samples.csv", sample_fields, samples)

            exact = {
                "global_exact_bound_candidates": "16",
                "global_exact_bound_survivors": "11",
                "global_exact_bound_fail_open": "0",
                "global_exact_bound_containment_failures": "0",
                "global_exact_bound_baseline_reads": "10",
                "global_exact_bound_baseline_bytes": "100000",
                "global_exact_bound_predicted_reads": "7",
                "global_exact_bound_predicted_waves": "1",
                "global_exact_bound_predicted_bytes": "70000",
                "global_exact_bound_certificate_kind": "residual-pq-v8",
                "global_exact_bound_exact_backing_reads": "10",
                "global_exact_bound_exact_backing_bytes": "100000",
                "global_exact_bound_residual_bytes": "1088",
                "global_exact_bound_residual_scan_bytes": "4096",
                "global_exact_bound_cpu_us": "1000",
                "global_exact_bound_certificate_scratch_allocations": "3",
            }
            read_fields = [
                "query",
                "record_id",
                "hit_id",
                "contains_record_id",
                "latency_ms",
                "requests",
                "gets",
                "puts",
                "deletes",
                "heads",
                "lists",
                "bytes_read",
                "backing_bytes_read",
                "segments_searched",
                "global_base_approximate_us",
                "global_base_exact_rerank_us",
                "global_delta_approximate_us",
                "global_delta_exact_rerank_us",
                "global_delta_wait_us",
                *exact.keys(),
            ]
            reads = [
                {
                    "query": str(query),
                    "record_id": f"id-0-0-{query % 16}",
                    "hit_id": f"id-0-0-{query % 16}",
                    "contains_record_id": "true",
                    "latency_ms": "40",
                    "requests": "5",
                    "gets": "5",
                    "puts": "0",
                    "deletes": "0",
                    "heads": "0",
                    "lists": "0",
                    "bytes_read": "180000",
                    "backing_bytes_read": "180000",
                    "segments_searched": "4",
                    "global_base_approximate_us": "100",
                    "global_base_exact_rerank_us": "1000",
                    "global_delta_approximate_us": "0",
                    "global_delta_exact_rerank_us": "0",
                    "global_delta_wait_us": "0",
                    **exact,
                }
                for query in range(20)
            ]
            self._write_csv(cell / "reads.csv", read_fields, reads)
            active_reads = [
                {**read, "latency_ms": "42"} for read in reads
            ]
            self._write_csv(cell / "active-tail-reads.csv", read_fields, active_reads)

            total_records = 16_384
            elapsed_ms = 1_000.0
            drain_ms = 1_000.0
            records_per_second = total_records / (elapsed_ms / 1_000.0)
            summary = {
                "source_sha256": source_sha,
                "dataset_sha256": manifest["dataset_sha256"],
                "manifest_sha256": manifest_sha,
                "writers": "32",
                "writer_instances": "32",
                "operations": "32",
                "records_per_operation": "16",
                "pipeline_depth": "4",
                "worker_lanes": "1",
                "records": str(total_records),
                "groups": "1024",
                "mean_group_records": "16",
                "elapsed_ms": str(elapsed_ms),
                "drain_ms": str(drain_ms),
                "end_to_end_records_per_second": str(
                    total_records / ((elapsed_ms + drain_ms) / 1_000.0)
                ),
                "p50_ms": "1",
                "p95_ms": "1",
                "records_per_second": str(records_per_second),
                "vector_mib_per_second": str(
                    records_per_second * 768 * 4 / (1024 * 1024)
                ),
                "storage_requests": "4096",
                "storage_gets": "0",
                "storage_puts": "3072",
                "storage_heads": "1024",
                "requests_per_record": "0.25",
                "total_acknowledgement_bytes": str(1024 * 4096),
                "max_acknowledgement_bytes": "4096",
                "visible_records": str(total_records),
                "recall_queries": "20",
                "max_read_segments": "4",
                "inserted_id_recall_at_10": "1",
                "active_tail_read_p50_ms": "42",
                "active_tail_read_p95_ms": "42",
                "read_p50_ms": "40",
                "read_p95_ms": "40",
                "read_storage_requests": "100",
                "read_storage_gets": "100",
                "read_storage_puts": "0",
                "read_storage_deletes": "0",
                "read_storage_heads": "0",
                "read_storage_lists": "0",
                "read_bytes": str(20 * 180_000),
                "read_segments_searched": "80",
            }
            self._write_csv(cell / "summary.csv", list(summary), [summary])

            trace_fields = [
                "operation",
                "object_role",
                "path",
                "physical_format",
                "object_bytes",
                "request_count",
                "bytes_fetched",
                "logical_projection",
                "row_selection",
                "logical_rows_requested",
                "logical_rows_decoded",
                "decode_cpu_ns",
                "cache_state",
                "status",
            ]
            trace = [
                {
                    "operation": "write",
                    "object_role": "lane_head",
                    "path": "lane-log/lanes/0000/HEAD",
                    "physical_format": "packed",
                    "object_bytes": "1048576",
                    "request_count": "1",
                    "bytes_fetched": "0",
                    "logical_projection": "",
                    "row_selection": "",
                    "logical_rows_requested": "",
                    "logical_rows_decoded": "",
                    "decode_cpu_ns": "",
                    "cache_state": "backing",
                    "status": "ok",
                },
                {
                    "operation": "read",
                    "object_role": "exact_vectors",
                    "path": "global-pq/exact/bundle.arrow",
                    "physical_format": "arrow_ipc",
                    "object_bytes": "100000",
                    "request_count": "200",
                    "bytes_fetched": "2000000",
                    "logical_projection": "exact",
                    "row_selection": "ranges",
                    "logical_rows_requested": "320",
                    "logical_rows_decoded": "320",
                    "decode_cpu_ns": "1000",
                    "cache_state": "backing",
                    "status": "ok",
                },
            ]
            self._write_csv(cell / "storage-access.csv", trace_fields, trace)
            self._write_csv(
                cell / "resources.csv",
                ["elapsed_ms", "cpu_percent", "rss_bytes"],
                [
                    {"elapsed_ms": "0", "cpu_percent": "1", "rss_bytes": "1024"},
                    {"elapsed_ms": "2100", "cpu_percent": "1", "rss_bytes": "1024"},
                ],
            )
            (cell / "process_exit.txt").write_text("0\n", encoding="utf-8")
            for marker in (*validator.PHASE_MARKERS, "CELL_COMPLETE"):
                (cell / marker).touch()

            def aggregate(name: str, records: list[dict[str, str]]) -> None:
                prefix = ["cell_count", "repetition"]
                identity = {"cell_count": "2000", "repetition": "1"}
                if name != "summary.csv":
                    prefix.append("worker_lanes")
                    identity["worker_lanes"] = "1"
                self._write_csv(
                    root / name,
                    prefix + list(records[0]),
                    [{**identity, **record} for record in records],
                )

            aggregate("summary.csv", [summary])
            aggregate("samples.csv", samples)
            aggregate("reads.csv", reads)
            aggregate("active-tail-reads.csv", active_reads)
            self._write_csv(
                root / "correctness.csv",
                ["gate", "status"],
                [
                    {"gate": gate, "status": "pass"}
                    for gate in manifest["correctness_gates"]
                ],
            )
            (root / "GROUP_COMMIT_SCALABILITY_COMPLETE").touch()

            validate(root, manifest_path)
            decision = evaluate(
                reads,
                manifest["exact_bound_shadow"],
                manifest["optimization_contract"],
            )
            self.assertTrue(decision["accepted"])
            self.assertTrue(decision["provisional_only"])
            self.assertFalse(decision["production_default_eligible"])


if __name__ == "__main__":
    unittest.main()
