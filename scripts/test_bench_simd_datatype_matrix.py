#!/usr/bin/env python3
"""Contracts for the frozen end-to-end datatype SIMD qualification."""

from __future__ import annotations

import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / "docs" / "research" / "simd-e2e-manifest.json"
RUNNER = ROOT / "scripts" / "bench_simd_datatype_matrix.sh"
CELL_RUNNER = ROOT / "scripts" / "run_simd_datatype_cell.sh"
NORMALIZER = ROOT / "scripts" / "normalize_simd_datatype_cell.py"


class SimdDatatypeMatrixTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))

    def test_architectures_are_same_shape_arm_and_x86_aws_controls(self) -> None:
        architectures = {
            row["name"]: (row["uname_machine"], row["instance_type"], row["region"])
            for row in self.manifest["architectures"]
        }
        self.assertEqual(
            architectures,
            {
                "aws-graviton-arm64": (
                    "aarch64",
                    "c7g.8xlarge",
                    "eu-central-1",
                ),
                "aws-x86-64": ("x86_64", "c7i.8xlarge", "eu-central-1"),
            },
        )

    def test_scalar_control_is_source_level_and_disables_autovectorization(
        self,
    ) -> None:
        builds = {row["name"]: row for row in self.manifest["builds"]}
        self.assertEqual(builds["simd"]["cargo_features"], [])
        self.assertEqual(builds["scalar-control"]["cargo_features"], ["scalar-control"])
        scalar_flags = builds["scalar-control"]["rustflags"]
        self.assertIn("llvm-args=-vectorize-loops=false", scalar_flags)
        self.assertIn("llvm-args=-vectorize-slp=false", scalar_flags)
        self.assertTrue(all(row["require_binary_sha256"] for row in builds.values()))
        self.assertFalse(
            self.manifest["source_policy"]["cross_architecture_control_pairing"]
        )

    def test_paths_cover_every_public_simd_datatype_and_modality(self) -> None:
        paths = {(row["kind"], row["element_type"]) for row in self.manifest["paths"]}
        self.assertEqual(
            paths,
            {
                ("primary-dense", "float32"),
                ("primary-dense", "float16"),
                ("primary-dense", "bfloat16"),
                ("primary-dense", "float8-e4m3fn"),
                ("primary-dense", "float8-e5m2"),
                ("primary-dense", "int8"),
                ("primary-binary", "binary"),
                ("named-sparse", "float32"),
                ("named-sparse", "float16"),
                ("late-interaction", "float32"),
                ("late-interaction", "float16"),
                ("text-bm25", "not-applicable"),
            },
        )

    def test_cache_concurrency_repetition_and_query_cohort_are_frozen(self) -> None:
        states = {
            (row["name"], row["coverage_percent"])
            for row in self.manifest["cache_states"]
        }
        self.assertEqual(
            states,
            {
                ("uncached", 0),
                ("mixed-0", 0),
                ("mixed-10", 10),
                ("mixed-25", 25),
                ("mixed-50", 50),
                ("mixed-75", 75),
                ("mixed-90", 90),
                ("mixed-100", 100),
                ("disk-cached", 100),
                ("memory-preloaded", 100),
            },
        )
        self.assertEqual(self.manifest["client_concurrency"], [1, 2, 4, 8, 16])
        self.assertEqual(self.manifest["repetitions"], 5)
        self.assertEqual(self.manifest["query_cohort"]["queries_per_cell"], 500)
        self.assertTrue(
            self.manifest["query_cohort"]["same_membership_and_order_across_builds"]
        )

    def test_raw_evidence_and_promotion_are_fail_closed(self) -> None:
        raw_fields = set(self.manifest["required_raw_query_fields"])
        self.assertTrue(
            {
                "source_sha256",
                "manifest_sha256",
                "binary_sha256",
                "query_id",
                "latency_ms",
                "cpu_seconds",
                "recall_or_exact_agreement",
                "rss_bytes",
                "backing_bytes",
                "backing_requests",
            }.issubset(raw_fields)
        )
        failures = set(self.manifest["fail_closed_conditions"])
        self.assertIn("architecture_or_instance_type_drift", failures)
        self.assertIn("missing_or_equal_build_binary_hashes", failures)
        self.assertIn("query_cohort_membership_or_order_drift", failures)
        self.assertIn("incomplete_raw_per_query_evidence", failures)
        self.assertIn("summary_or_resource_aggregate_drift", failures)
        promotion = self.manifest["promotion_rule"]
        self.assertTrue(promotion["requires_both_architectures"])
        self.assertTrue(promotion["requires_no_correctness_or_recall_regression"])
        self.assertTrue(promotion["requires_lower_end_to_end_cpu_or_latency"])
        self.assertTrue(promotion["historical_or_cross_host_pairing_forbidden"])
        semantics = self.manifest["measurement_semantics"]
        self.assertIn("exact child-process CPU", semantics["cpu_seconds"])
        self.assertIn("complete fresh-process wall time", semantics["qps"])
        self.assertIn(
            "explicitly primed hot set", semantics["observed_cache_coverage_percent"]
        )
        self.assertTrue(
            self.manifest["dataset_identity"][
                "same_identity_required_for_every_build_and_cell"
            ]
        )

    def test_runner_dry_run_materializes_the_complete_balanced_schedule(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            output = root / "results"
            env = os.environ.copy()
            env.update(
                {
                    "BORSUK_SIMD_RUN_ID": "simd-fixture",
                    "BORSUK_SIMD_ARCHITECTURE": "aws-graviton-arm64",
                    "BORSUK_SIMD_MATRIX_EXECUTE": "0",
                    "BORSUK_SIMD_ROOT": str(output),
                }
            )
            completed = subprocess.run(
                ["bash", str(RUNNER)],
                cwd=ROOT,
                env=env,
                capture_output=True,
                text=True,
            )
            self.assertEqual(completed.returncode, 0, completed.stderr)
            schedule = (
                (output / "schedule.csv").read_text(encoding="utf-8").splitlines()
            )
            ordinary_states = [
                row
                for row in self.manifest["cache_states"]
                if row["name"] != "memory-preloaded"
            ]
            memory_paths = [
                row
                for row in self.manifest["paths"]
                if row.get("memory_preloaded_valid", False)
            ]
            expected_queries = (
                len(self.manifest["builds"])
                * self.manifest["repetitions"]
                * len(self.manifest["client_concurrency"])
                * (
                    len(self.manifest["paths"]) * len(ordinary_states)
                    + len(memory_paths)
                )
            )
            self.assertEqual(len(schedule) - 1, expected_queries)
            self.assertTrue((output / "manifest.json").is_file())
            self.assertTrue((output / "environment.txt").is_file())

    def test_runner_has_paid_guards_build_identity_and_fresh_prefix_checks(
        self,
    ) -> None:
        source = RUNNER.read_text(encoding="utf-8")
        for contract in (
            "BORSUK_RUN_SIMD_MATRIX",
            "BORSUK_SOURCE_SHA256",
            "BORSUK_SIMD_MANIFEST_SHA256",
            "BORSUK_SIMD_RESULT_PREFIX",
            "BORSUK_SIMD_INDEX_PREFIX",
            "refusing to overwrite non-empty S3 prefix",
            "uname -m",
            "scalar-control",
            "llvm-args=-vectorize-loops=false",
            "llvm-args=-vectorize-slp=false",
            "sha256sum",
            "SIMD_DATATYPE_MATRIX_COMPLETE",
            "SIMD_DATATYPE_MATRIX_FAILED",
        ):
            self.assertIn(contract, source)

    def test_paid_mode_rejects_missing_explicit_guard_before_aws_calls(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            env = os.environ.copy()
            env.update(
                {
                    "BORSUK_SIMD_RUN_ID": "simd-fixture",
                    "BORSUK_SIMD_ARCHITECTURE": "aws-graviton-arm64",
                    "BORSUK_SIMD_MATRIX_EXECUTE": "1",
                    "BORSUK_SIMD_ROOT": str(Path(temporary) / "results"),
                }
            )
            completed = subprocess.run(
                ["bash", str(RUNNER)],
                cwd=ROOT,
                env=env,
                capture_output=True,
                text=True,
            )
            self.assertNotEqual(completed.returncode, 0)
            self.assertIn("paid execution", completed.stderr.lower())

    def test_cell_runner_uses_real_native_evidence_and_amortized_resources(
        self,
    ) -> None:
        self.assertTrue(CELL_RUNNER.is_file())
        self.assertTrue(os.access(CELL_RUNNER, os.X_OK))
        self.assertTrue(NORMALIZER.is_file())
        source = CELL_RUNNER.read_text(encoding="utf-8")
        for contract in (
            "benchmark_with_resources.py",
            "normalize_simd_datatype_cell.py",
            "BORSUK_BENCH_BUILD_ONLY=1",
            "hybrid_retrieval_bench",
            "market_workload_bench",
            "INDEX_COMPLETE",
            "CELL_COMPLETE",
            "BORSUK_SIMD_EXPECTED_QUERIES",
            "BORSUK_SIMD_DATASETS_ROOT",
            "BORSUK_SIMD_INDEX_KEY",
            "BORSUK_INSTANCE_TYPE",
            "sha256sum",
        ):
            self.assertIn(contract, source)

    def test_orchestrator_passes_cell_identity_and_dataset_contract(self) -> None:
        source = RUNNER.read_text(encoding="utf-8")
        for contract in (
            "BORSUK_SIMD_ROOT",
            "BORSUK_SIMD_INDEX_KEY",
            "BORSUK_SIMD_DATASETS_ROOT",
            "BORSUK_SIMD_EXPECTED_QUERIES",
            "BORSUK_INSTANCE_TYPE",
            "freeze_simd_dataset_identity.py",
            "--verify-existing",
        ):
            self.assertIn(contract, source)


if __name__ == "__main__":
    unittest.main()
