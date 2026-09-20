import copy
import json
import pathlib
import subprocess
import tempfile
import unittest

from scripts.v85_delta_compaction_screen import _path_for_uri
from scripts.v85_qualification import (
    frozen_matrix,
    validate_delta_compaction_screen,
    validate_preflight_receipt,
    validate_qualification_receipt,
)


class V85QualificationTests(unittest.TestCase):
    def test_compaction_screen_resolves_duplicate_basenames_by_registered_identity(
        self,
    ) -> None:
        # Break caught: compacted mutations.arrow is confused with the level-0
        # file solely because both immutable objects share one basename.
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            level0 = root / "level0"
            compacted = root / "compacted"
            level0.mkdir()
            compacted.mkdir()
            (level0 / "mutations.arrow").write_bytes(b"level-zero")
            wanted = compacted / "mutations.arrow"
            wanted.write_bytes(b"compacted")
            identity = {
                "bytes": wanted.stat().st_size,
                "sha256": __import__("hashlib").sha256(wanted.read_bytes()).hexdigest(),
                "uri": "s3://fixture/compacted/mutations.arrow",
            }

            self.assertEqual(
                _path_for_uri(identity, (level0, compacted)),
                wanted,
            )

    def test_delta_compaction_runner_exposes_fixed_100k_fail_fast_screen(
        self,
    ) -> None:
        root = pathlib.Path(__file__).resolve().parents[1]
        completed = subprocess.run(
            [
                "bash",
                str(root / "scripts/v85_delta_compaction_100k_run_remote.sh"),
                "--describe",
            ],
            cwd=root,
            check=True,
            capture_output=True,
            text=True,
        )

        self.assertEqual(
            json.loads(completed.stdout),
            {
                "base_rows": 90_000,
                "claim_eligible": False,
                "delta_rows": 10_000,
                "evidence_kind": "semantic-local-artifact-screen",
                "page_budget": 512,
                "query_count": 32,
                "rows": 100_000,
                "run_counts": [1, 10, 100],
                "schema": "borsuk-v85-delta-compaction-screen-matrix-v1",
            },
        )

    @staticmethod
    def _semantic_result(generation: int) -> dict[str, object]:
        samples = [
            {
                "bytes": 80_000_000,
                "hits": 100,
                "latency_ns": 10_000_000,
                "neighbors": 100,
                "query": query,
                "recall_ppm": 1_000_000,
                "requests": 400,
                "result_ids": list(range(query * 1_000, query * 1_000 + 100)),
                "truth_ids": list(range(query * 1_000, query * 1_000 + 100)),
            }
            for query in range(32)
        ]
        return {
            "aggregate_recall_ppm": 1_000_000,
            "generation": generation,
            "samples": samples,
            "total_bytes": sum(sample["bytes"] for sample in samples),
            "total_requests": sum(sample["requests"] for sample in samples),
            "worst_recall_ppm": 1_000_000,
        }

    def test_delta_compaction_screen_recomputes_real_scale_semantic_equivalence(
        self,
    ) -> None:
        # Break caught: the paid 1M cell starts before 1/10/100-run and compacted
        # real-data results are proven identical from raw per-query evidence.
        results = [self._semantic_result(1) for _runs in (1, 10, 100)]
        compacted = self._semantic_result(2)
        receipt = {
            "base_rows": 90_000,
            "claim_eligible": False,
            "compaction": {
                "amplification_ppm": 4_000_000,
                "input_runs": 100,
                "logical_live_bytes": 1_000,
                "read_bytes": 2_000,
                "result": compacted,
                "write_bytes": 2_000,
            },
            "delta_rows": 10_000,
            "evidence_kind": "semantic-local-artifact-screen",
            "page_budget": 512,
            "query_count": 32,
            "results": [
                {"result": result, "runs": runs}
                for runs, result in zip((1, 10, 100), results, strict=True)
            ],
            "rows": 100_000,
            "schema": "borsuk-v85-delta-compaction-screen-v1",
        }

        summary = validate_delta_compaction_screen(receipt)

        self.assertEqual(
            summary,
            {
                "aggregate_recall_ppm": 1_000_000,
                "compaction_amplification_ppm": 4_000_000,
                "query_count": 32,
                "schema": "borsuk-v85-delta-compaction-screen-summary-v1",
                "worst_recall_ppm": 1_000_000,
            },
        )

        for mutate in (
            lambda value: value["results"].pop(),
            lambda value: value["results"][1].update({"runs": 11}),
            lambda value: value["results"][1]["result"]["samples"][0][
                "result_ids"
            ].reverse(),
            lambda value: value["compaction"]["result"]["samples"][0][
                "result_ids"
            ].reverse(),
            lambda value: value["compaction"].update(
                {"amplification_ppm": 3_999_999}
            ),
            lambda value: value["compaction"].update({"write_bytes": 3_001}),
        ):
            drift = copy.deepcopy(receipt)
            mutate(drift)
            with self.assertRaisesRegex(ValueError, "compaction screen"):
                validate_delta_compaction_screen(drift)

        low_quality = copy.deepcopy(receipt)
        low_results = [
            cell["result"] for cell in low_quality["results"]
        ] + [low_quality["compaction"]["result"]]
        for result in low_results:
            for sample in result["samples"]:
                query = sample["query"]
                sample["result_ids"] = list(
                    range(1_000_000 + query * 100, 1_000_100 + query * 100)
                )
                sample["hits"] = 0
                sample["recall_ppm"] = 0
            result["aggregate_recall_ppm"] = 0
            result["worst_recall_ppm"] = 0
        with self.assertRaisesRegex(ValueError, "compaction screen"):
            validate_delta_compaction_screen(low_quality)

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
        self.assertEqual(matrix["preflight_page_budgets"], [16, 32])
        self.assertEqual(matrix["max_gets_per_query"], 32)
        self.assertEqual(matrix["max_bytes_per_query"], 16 * 1024 * 1024)
        self.assertEqual(matrix["max_peak_rss_bytes"], 3 * 1024**3)
        self.assertEqual(matrix["min_average_recall10_ppm"], 960_000)
        self.assertEqual(matrix["min_average_recall100_ppm"], 975_000)
        self.assertEqual(matrix["min_p05_recall100_ppm"], 900_000)
        self.assertEqual(matrix["range_concurrency"], 16)
        self.assertEqual(matrix["cpu_parallelism"], "sequential-per-query")
        self.assertEqual(matrix["spot_interruption"], "discard-cell-and-restart")
        self.assertEqual(matrix["terminal_schema"], "borsuk-v85-terminal-v1")

    def test_preflight_rejects_unbounded_s3_work_before_full_build(self) -> None:
        # Break caught: a locally correct 10k result promotes despite excessive
        # remote ranges/bytes, missing CAS conflict, or manifest drift.
        receipt = {
            "authenticated_inputs": 4,
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
            "samples": [
                {
                    "bytes": 8 * 1024 * 1024,
                    "latency_ns": 40_000_000,
                    "neighbors": 100,
                    "query": 0,
                    "requests": 24,
                    "result_ids": list(range(100)),
                    "truth_ids": list(range(100)),
                }
            ],
            "result_sha256": "2" * 64,
            "schema": "borsuk-v85-preflight-receipt-v2",
            "selected_page_budget": 16,
            "source_archive_sha256": "3" * 64,
            "source_commit": "4" * 40,
        }
        validate_preflight_receipt(receipt, frozen_matrix())
        for field, value in (
            ("cas_conflict_observed", False),
            ("manifest_drift", True),
            ("max_gets_per_query", 33),
            ("max_bytes_per_query", 16 * 1024 * 1024 + 1),
            ("peak_rss_bytes", 3 * 1024**3 + 1),
            ("selected_page_budget", 64),
        ):
            drift = copy.deepcopy(receipt)
            drift[field] = value
            with self.assertRaisesRegex(ValueError, "preflight"):
                validate_preflight_receipt(drift, frozen_matrix())

        quality = copy.deepcopy(receipt)
        quality["samples"][0]["result_ids"] = list(range(10)) + list(
            range(1_000, 1_090)
        )
        with self.assertRaisesRegex(ValueError, "preflight"):
            validate_preflight_receipt(quality, frozen_matrix())

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
        mutation_cases = []
        for offset in range(matrix["replacement_rows"]):
            mutation_cases.append(
                {
                    "id": 10_000 + offset,
                    "kind": "replacement",
                    "observed_sequence": 2,
                    "observed_state": "live",
                    "visibility_latency_ns": 300_000_000,
                    "writes": [
                        {"sequence": 1, "state": "live"},
                        {"sequence": 2, "state": "live"},
                    ],
                }
            )
        for offset in range(matrix["tombstone_rows"]):
            mutation_cases.append(
                {
                    "id": 20_000 + offset,
                    "kind": "tombstone",
                    "observed_sequence": 3,
                    "observed_state": "tombstone",
                    "visibility_latency_ns": 500_000_000,
                    "writes": [
                        {"sequence": 1, "state": "live"},
                        {"sequence": 3, "state": "tombstone"},
                    ],
                }
            )

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
            "mutation_cases": mutation_cases,
            "schema": "borsuk-v85-qualification-receipt-v4",
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
                        "average_recall10_ppm": 1_000_000,
                        "average_recall100_ppm": 995_000,
                        "p05_recall100_ppm": 990_000,
                        "runs": runs,
                        "successful_qps_milli": 100_000,
                        "worst_recall_ppm": 990_000,
                    }
                    for runs, latency in (
                        (1, 60_000_000),
                        (10, 65_000_000),
                        (100, 70_000_000),
                    )
                ],
                "compaction_amplification_ppm": 4_000_000,
                "fresh_average_recall10_ppm": 1_000_000,
                "fresh_average_recall100_ppm": 995_000,
                "fresh_p05_recall100_ppm": 990_000,
                "offered_qps_milli": 84_000,
                "schema": "borsuk-v85-qualification-summary-v2",
                "visibility_p95_ns": 500_000_000,
            },
        )

    def test_full_receipt_uses_distributional_quality_gates_not_absolute_worst(
        self,
    ) -> None:
        # Break caught: promotion still applies the obsolete absolute-minimum
        # recall gate, or fails to recompute Recall@10/Recall@100/p05 from IDs.
        matrix, receipt = self._qualification_fixture()
        matrix["query_count"] = 21

        def distribution_samples(latency_ns: int) -> list[dict[str, object]]:
            samples = []
            for query in range(21):
                truth = list(range(query * 1_000, query * 1_000 + 100))
                result = truth.copy()
                if query == 0:
                    result[80:] = list(range(100_000, 100_020))
                samples.append(
                    {
                        "bytes": 12 * 1024 * 1024,
                        "latency_ns": latency_ns,
                        "neighbors": 100,
                        "query": query,
                        "requests": 24,
                        "result_ids": result,
                        "truth_ids": truth,
                    }
                )
            return samples

        receipt["fresh_samples"] = distribution_samples(50_000_000)
        for cell, latency in zip(
            receipt["cells"], (60_000_000, 65_000_000, 70_000_000), strict=True
        ):
            cell["samples"] = distribution_samples(latency)
        receipt["compaction"]["post_result_ids"] = [
            sample["result_ids"] for sample in receipt["cells"][2]["samples"]
        ]

        summary = validate_qualification_receipt(receipt, matrix)

        self.assertEqual(summary["fresh_average_recall10_ppm"], 1_000_000)
        self.assertEqual(summary["fresh_average_recall100_ppm"], 990_476)
        self.assertEqual(summary["fresh_p05_recall100_ppm"], 1_000_000)
        self.assertEqual(summary["cells"][0]["worst_recall_ppm"], 800_000)

    def test_full_receipt_rejects_each_competitive_quality_gate_independently(
        self,
    ) -> None:
        # Break caught: any one of average Recall@10, average Recall@100, or
        # p05 Recall@100 becomes advisory instead of a promotion veto.
        matrix, baseline = self._qualification_fixture()
        matrix["query_count"] = 100

        def perfect_samples() -> list[dict[str, object]]:
            return [
                {
                    "bytes": 12 * 1024 * 1024,
                    "latency_ns": 50_000_000,
                    "neighbors": 100,
                    "query": query,
                    "requests": 24,
                    "result_ids": list(range(query * 1_000, query * 1_000 + 100)),
                    "truth_ids": list(range(query * 1_000, query * 1_000 + 100)),
                }
                for query in range(100)
            ]

        mutations = []
        recall10 = perfect_samples()
        for query in range(5):
            recall10[query]["result_ids"][:10] = list(
                range(100_000 + query * 10, 100_010 + query * 10)
            )
        mutations.append(recall10)
        recall100 = perfect_samples()
        for sample in recall100:
            sample["result_ids"][97:] = list(
                range(200_000 + sample["query"] * 3, 200_003 + sample["query"] * 3)
            )
        mutations.append(recall100)
        p05 = perfect_samples()
        for query in range(5):
            p05[query]["result_ids"][89:] = list(
                range(300_000 + query * 11, 300_011 + query * 11)
            )
        mutations.append(p05)

        for samples in mutations:
            receipt = copy.deepcopy(baseline)
            receipt["fresh_samples"] = samples
            for cell in receipt["cells"]:
                cell["samples"] = copy.deepcopy(samples)
            receipt["compaction"]["post_result_ids"] = [
                sample["result_ids"] for sample in samples
            ]
            with self.assertRaisesRegex(ValueError, "quality gate"):
                validate_qualification_receipt(receipt, matrix)

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
        truth_drift["cells"][1]["samples"][0]["truth_ids"] = list(range(1_000, 1_100))
        truth_drift["cells"][1]["samples"][0]["result_ids"] = list(range(1_000, 1_100))
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
        for case in visibility["mutation_cases"][-51:]:
            case["visibility_latency_ns"] = 1_000_000_001
        mutations.append(visibility)
        wrong_mix = copy.deepcopy(receipt)
        wrong_mix["mutation_cases"][0]["kind"] = "tombstone"
        wrong_mix["mutation_cases"][0]["observed_state"] = "tombstone"
        wrong_mix["mutation_cases"][0]["writes"][-1]["state"] = "tombstone"
        mutations.append(wrong_mix)
        duplicate_id = copy.deepcopy(receipt)
        duplicate_id["mutation_cases"][1]["id"] = duplicate_id["mutation_cases"][0]["id"]
        mutations.append(duplicate_id)
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
