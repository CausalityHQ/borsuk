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
        self.assertEqual(matrix["query_count"], 1_000)
        self.assertEqual(matrix["neighbors"], 100)
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

    @staticmethod
    def _qualification_fixture() -> tuple[dict[str, object], dict[str, object]]:
        matrix = copy.deepcopy(frozen_matrix())
        matrix["query_count"] = 2

        def samples(max_latency_ns: int) -> list[dict[str, object]]:
            return [
                {
                    "bytes": 12 * 1024 * 1024,
                    "latency_ns": 40_000_000,
                    "neighbors": 100,
                    "query": 0,
                    "requests": 24,
                    "result_ids": list(range(100)),
                    "truth_ids": list(range(100)),
                },
                {
                    "bytes": 11 * 1024 * 1024,
                    "latency_ns": max_latency_ns,
                    "neighbors": 100,
                    "query": 1,
                    "requests": 23,
                    "result_ids": list(range(100, 199)) + [999],
                    "truth_ids": list(range(100, 200)),
                },
            ]

        cells = [
            {
                "elapsed_ns": 20_000_000,
                "peak_rss_bytes": 2 * 1024**3,
                "runs": 1,
                "samples": samples(60_000_000),
            },
            {
                "elapsed_ns": 20_000_000,
                "peak_rss_bytes": 2 * 1024**3,
                "runs": 10,
                "samples": samples(65_000_000),
            },
            {
                "elapsed_ns": 20_000_000,
                "peak_rss_bytes": 2 * 1024**3,
                "runs": 100,
                "samples": samples(70_000_000),
            },
        ]
        receipt = {
            "base_capacity": {
                "attempted_queries": 12,
                "elapsed_ns": 100_000_000,
                "successful_queries": 12,
            },
            "cells": cells,
            "compaction": {
                "amplification_ppm": 4_000_000,
                "logical_live_bytes": 1_000,
                "post_result_ids": [
                    sample["result_ids"] for sample in cells[2]["samples"]
                ],
                "read_bytes": 2_000,
                "write_bytes": 2_000,
            },
            "fresh_samples": samples(55_000_000),
            "mutation_cases": [
                {
                    "id": 10,
                    "kind": "newest",
                    "observed_sequence": 3,
                    "observed_state": "live",
                    "visibility_latency_ns": 300_000_000,
                    "writes": [
                        {"sequence": 1, "state": "live"},
                        {"sequence": 3, "state": "live"},
                    ],
                },
                {
                    "id": 11,
                    "kind": "replacement",
                    "observed_sequence": 4,
                    "observed_state": "live",
                    "visibility_latency_ns": 400_000_000,
                    "writes": [
                        {"sequence": 1, "state": "live"},
                        {"sequence": 4, "state": "live"},
                    ],
                },
                {
                    "id": 12,
                    "kind": "tombstone",
                    "observed_sequence": 5,
                    "observed_state": "tombstone",
                    "visibility_latency_ns": 500_000_000,
                    "writes": [
                        {"sequence": 2, "state": "live"},
                        {"sequence": 5, "state": "tombstone"},
                    ],
                },
            ],
            "schema": "borsuk-v85-qualification-receipt-v2",
        }
        return matrix, receipt

    def test_full_receipt_recomputes_every_gate_from_raw_evidence(self) -> None:
        # Break caught: promotion trusts runner-supplied aggregates rather than
        # deriving quality, work, capacity, mutation, and compaction gates.
        matrix, receipt = self._qualification_fixture()

        summary = validate_qualification_receipt(receipt, matrix)

        self.assertEqual(
            summary,
            {
                "base_capacity_qps_milli": 120_000,
                "cells": [
                    {
                        "bytes_per_query": 12 * 1024 * 1024,
                        "gets_per_query": 24,
                        "p50_ns": 40_000_000,
                        "p95_ns": latency,
                        "p99_ns": latency,
                        "peak_rss_bytes": 2 * 1024**3,
                        "recall_ppm": 995_000,
                        "runs": runs,
                        "successful_qps_milli": 100_000,
                        "worst_recall_ppm": 990_000,
                    }
                    for runs, latency in ((1, 60_000_000), (10, 65_000_000), (100, 70_000_000))
                ],
                "compaction_amplification_ppm": 4_000_000,
                "fresh_recall_ppm": 995_000,
                "offered_qps_milli": 84_000,
                "schema": "borsuk-v85-qualification-summary-v1",
                "visibility_p95_ns": 500_000_000,
            },
        )

    def test_full_receipt_rejects_per_query_capacity_and_quality_drift(self) -> None:
        # Break caught: missing/failed queries, invalid rankings, unbounded S3
        # work, slow capacity, tail regression, or stale recall still promote.
        matrix, receipt = self._qualification_fixture()
        mutations = []

        missing = copy.deepcopy(receipt)
        missing["cells"][0]["samples"].pop()
        mutations.append(missing)
        duplicate = copy.deepcopy(receipt)
        duplicate["cells"][0]["samples"][0]["result_ids"][1] = 0
        mutations.append(duplicate)
        truth_drift = copy.deepcopy(receipt)
        truth_drift["cells"][1]["samples"][0]["truth_ids"] = list(
            range(1_000, 1_100)
        )
        truth_drift["cells"][1]["samples"][0]["result_ids"] = list(
            range(1_000, 1_100)
        )
        mutations.append(truth_drift)
        requests = copy.deepcopy(receipt)
        requests["cells"][1]["samples"][0]["requests"] = 33
        mutations.append(requests)
        payload = copy.deepcopy(receipt)
        payload["cells"][1]["samples"][0]["bytes"] = 16 * 1024 * 1024 + 1
        mutations.append(payload)
        capacity = copy.deepcopy(receipt)
        capacity["cells"][1]["elapsed_ns"] = 25_000_000
        mutations.append(capacity)
        failed_capacity = copy.deepcopy(receipt)
        failed_capacity["base_capacity"]["successful_queries"] = 11
        mutations.append(failed_capacity)
        tail = copy.deepcopy(receipt)
        tail["cells"][2]["samples"][1]["latency_ns"] = 90_000_001
        mutations.append(tail)
        memory = copy.deepcopy(receipt)
        memory["cells"][0]["peak_rss_bytes"] = 3 * 1024**3 + 1
        mutations.append(memory)
        quality = copy.deepcopy(receipt)
        quality["fresh_samples"][0]["result_ids"] = list(range(1_000, 1_100))
        mutations.append(quality)

        for mutated in mutations:
            with self.subTest(mutated=mutations.index(mutated)):
                with self.assertRaisesRegex(ValueError, "qualification"):
                    validate_qualification_receipt(mutated, matrix)

    def test_full_receipt_rejects_mutation_and_compaction_evidence_drift(self) -> None:
        # Break caught: stale latest-write state, excessive visibility latency,
        # fabricated amplification, or changed post-compaction results promote.
        matrix, receipt = self._qualification_fixture()
        mutations = []

        stale = copy.deepcopy(receipt)
        stale["mutation_cases"][0]["observed_sequence"] = 1
        mutations.append(stale)
        missing_case = copy.deepcopy(receipt)
        missing_case["mutation_cases"].pop()
        mutations.append(missing_case)
        visibility = copy.deepcopy(receipt)
        visibility["mutation_cases"][2]["visibility_latency_ns"] = 1_000_000_001
        mutations.append(visibility)
        fabricated = copy.deepcopy(receipt)
        fabricated["compaction"]["amplification_ppm"] = 3_999_999
        mutations.append(fabricated)
        amplified = copy.deepcopy(receipt)
        amplified["compaction"]["write_bytes"] = 3_001
        amplified["compaction"]["amplification_ppm"] = 5_001_000
        mutations.append(amplified)
        changed = copy.deepcopy(receipt)
        changed["compaction"]["post_result_ids"][0] = list(reversed(range(100)))
        mutations.append(changed)

        for mutated in mutations:
            with self.subTest(mutated=mutations.index(mutated)):
                with self.assertRaisesRegex(ValueError, "qualification"):
                    validate_qualification_receipt(mutated, matrix)


if __name__ == "__main__":
    unittest.main()
