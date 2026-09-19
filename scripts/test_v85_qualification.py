import copy
import json
import pathlib
import subprocess
import unittest

from scripts.v85_qualification import (
    frozen_matrix,
    validate_preflight_receipt,
    validate_qualification_receipt,
)


class V85QualificationTests(unittest.TestCase):
    def test_remote_runner_exposes_only_the_frozen_matrix_without_aws(self) -> None:
        # Break caught: shell-local constants drift from the independently tested
        # matrix, or describing the campaign performs a remote side effect.
        root = pathlib.Path(__file__).resolve().parents[1]
        completed = subprocess.run(
            ["bash", str(root / "scripts/v85_run_remote.sh"), "--describe"],
            cwd=root,
            check=True,
            capture_output=True,
            text=True,
        )
        self.assertEqual(json.loads(completed.stdout), frozen_matrix())

        rejected = subprocess.run(
            ["bash", str(root / "scripts/v85_run_remote.sh"), "--unknown"],
            cwd=root,
            capture_output=True,
            text=True,
        )
        self.assertNotEqual(rejected.returncode, 0)

    def test_frozen_matrix_registers_fail_fast_work_and_no_rayon(self) -> None:
        # Break caught: the expensive cell starts without literal scale, mutation,
        # S3-work, pressure, or bounded-concurrency gates.
        matrix = frozen_matrix()
        self.assertEqual(matrix["base_rows"], 900_000)
        self.assertEqual(matrix["delta_rows"], 100_000)
        self.assertEqual(matrix["run_counts"], [1, 10, 100])
        self.assertEqual(matrix["replacement_rows"], 500)
        self.assertEqual(matrix["tombstone_rows"], 500)
        self.assertEqual(matrix["offered_load_ppm"], 700_000)
        self.assertEqual(matrix["preflight_page_budgets"], [8, 16])
        self.assertEqual(matrix["max_gets_per_query"], 32)
        self.assertEqual(matrix["max_bytes_per_query"], 16 * 1024 * 1024)
        self.assertEqual(matrix["max_peak_rss_bytes"], 3 * 1024**3)
        self.assertEqual(matrix["min_aggregate_recall_ppm"], 990_000)
        self.assertEqual(matrix["range_concurrency"], 16)
        self.assertEqual(matrix["cpu_parallelism"], "sequential-per-query")
        self.assertEqual(matrix["spot_interruption"], "discard-cell-and-restart")
        self.assertEqual(matrix["terminal_schema"], "borsuk-v85-terminal-v1")

    def test_preflight_rejects_unbounded_s3_work_before_full_build(self) -> None:
        # Break caught: a locally correct 10k result promotes despite excessive
        # remote ranges/bytes, missing CAS conflict, or manifest drift.
        receipt = {
            "authenticated_inputs": 4,
            "aggregate_recall_ppm": 990_000,
            "binary_authenticated": True,
            "binary_sha256": "1" * 64,
            "built_rows": 10_000,
            "cas_conflict_observed": True,
            "failed_queries": 0,
            "manifest_drift": False,
            "max_bytes_per_query": 8 * 1024 * 1024,
            "max_gets_per_query": 24,
            "peak_rss_bytes": 512 * 1024**2,
            "query_count": 1,
            "result_sha256": "2" * 64,
            "schema": "borsuk-v85-preflight-receipt-v1",
            "selected_page_budget": 16,
            "source_archive_sha256": "3" * 64,
            "source_commit": "4" * 40,
            "worst_recall_ppm": 990_000,
        }
        validate_preflight_receipt(receipt, frozen_matrix())
        for field, value in (
            ("cas_conflict_observed", False),
            ("manifest_drift", True),
            ("max_gets_per_query", 33),
            ("max_bytes_per_query", 16 * 1024 * 1024 + 1),
            ("peak_rss_bytes", 3 * 1024**3 + 1),
            ("aggregate_recall_ppm", 989_999),
            ("worst_recall_ppm", 989_999),
            ("selected_page_budget", 32),
        ):
            drift = copy.deepcopy(receipt)
            drift[field] = value
            with self.assertRaisesRegex(ValueError, "preflight"):
                validate_preflight_receipt(drift, frozen_matrix())

    def test_full_receipt_recomputes_relative_and_compaction_gates(self) -> None:
        # Break caught: a missing/error cell, stale mutation result, excessive
        # fragmentation tail, or non-identical post-compaction result promotes.
        result_ids = list(range(100))
        def cell(runs: int, p95: int, p99: int) -> dict[str, object]:
            return {
                "bytes_per_query": 12 * 1024 * 1024,
                "failed_queries": 0,
                "gets_per_query": 24,
                "p50_ns": 40_000_000,
                "p95_ns": p95,
                "p99_ns": p99,
                "peak_rss_bytes": 2 * 1024**3,
                "recall_ppm": 992_000,
                "result_ids": result_ids,
                "runs": runs,
                "successful_qps_milli": 84_000,
                "worst_recall_ppm": 900_000,
            }
        receipt = {
            "base_capacity_qps_milli": 120_000,
            "cells": [
                cell(1, 60_000_000, 80_000_000),
                cell(10, 65_000_000, 90_000_000),
                cell(100, 70_000_000, 100_000_000),
            ],
            "compaction_amplification_ppm": 4_000_000,
            "fresh_recall_ppm": 993_000,
            "mutation_checks": {"newest": True, "replacement": True, "tombstone": True},
            "offered_qps_milli": 84_000,
            "post_compaction_result_ids": result_ids,
            "schema": "borsuk-v85-qualification-receipt-v1",
            "visibility_p95_ns": 500_000_000,
        }
        validate_qualification_receipt(receipt, frozen_matrix())

        stale = copy.deepcopy(receipt)
        stale["mutation_checks"]["tombstone"] = False
        with self.assertRaisesRegex(ValueError, "qualification"):
            validate_qualification_receipt(stale, frozen_matrix())

        tail = copy.deepcopy(receipt)
        tail["cells"][2]["p99_ns"] = 120_000_001
        with self.assertRaisesRegex(ValueError, "qualification"):
            validate_qualification_receipt(tail, frozen_matrix())

        changed = copy.deepcopy(receipt)
        changed["post_compaction_result_ids"] = list(reversed(result_ids))
        with self.assertRaisesRegex(ValueError, "qualification"):
            validate_qualification_receipt(changed, frozen_matrix())

        zero_quality = copy.deepcopy(receipt)
        zero_quality["fresh_recall_ppm"] = 0
        for candidate in zero_quality["cells"]:
            candidate["recall_ppm"] = 0
        with self.assertRaisesRegex(ValueError, "qualification"):
            validate_qualification_receipt(zero_quality, frozen_matrix())


if __name__ == "__main__":
    unittest.main()
