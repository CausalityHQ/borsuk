#!/usr/bin/env python3
"""Contracts for the frozen end-to-end datatype SIMD qualification."""

from __future__ import annotations

import json
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / "docs" / "research" / "simd-e2e-manifest.json"


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
        self.assertEqual(
            builds["scalar-control"]["cargo_features"], ["scalar-control"]
        )
        scalar_flags = builds["scalar-control"]["rustflags"]
        self.assertIn("llvm-args=-vectorize-loops=false", scalar_flags)
        self.assertIn("llvm-args=-vectorize-slp=false", scalar_flags)
        self.assertTrue(
            all(row["require_binary_sha256"] for row in builds.values())
        )
        self.assertFalse(
            self.manifest["source_policy"]["cross_architecture_control_pairing"]
        )

    def test_paths_cover_every_public_simd_datatype_and_modality(self) -> None:
        paths = {
            (row["kind"], row["element_type"]) for row in self.manifest["paths"]
        }
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
            self.manifest["query_cohort"][
                "same_membership_and_order_across_builds"
            ]
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
        promotion = self.manifest["promotion_rule"]
        self.assertTrue(promotion["requires_both_architectures"])
        self.assertTrue(promotion["requires_no_correctness_or_recall_regression"])
        self.assertTrue(promotion["requires_lower_end_to_end_cpu_or_latency"])
        self.assertTrue(promotion["historical_or_cross_host_pairing_forbidden"])


if __name__ == "__main__":
    unittest.main()
