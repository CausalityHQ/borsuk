#!/usr/bin/env python3
"""Tests for the preregistered exact-bound shadow decision gate."""

import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from scripts.evaluate_exact_bound_shadow import evaluate

GATE = {
    "require_zero_containment_failures": True,
    "max_survivor_p95": 11,
    "min_read_reduction_fraction": 0.30,
    "min_byte_reduction_fraction": 0.30,
    "max_cpu_p95_us": 2_000,
    "max_cpu_fraction_of_read_p95": 0.05,
    "max_residual_bytes_per_vector": 68,
    "max_total_backing_byte_ratio": 2.0,
    "max_drain_regression_fraction": 0.10,
    "max_physical_write_amplification_regression_fraction": 0.10,
    "non_read_regression_control": "same-cell-shared-ingest-and-drain",
    "exact_vector_bytes": 3072,
}

OPTIMIZATION = {
    "hard_read_p95_ms": 200,
    "selection_rule": "pareto-minimize-latency-requests-bytes-cpu-allocations-at-fixed-correctness-recall",
    "gate_passing_freezes_production_default": False,
}


def row(**updates: str) -> dict[str, str]:
    values = {
        "latency_ms": "40",
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
        "backing_bytes_read": "180000",
    }
    values.update(updates)
    return values


class ExactBoundShadowEvaluatorTests(unittest.TestCase):
    def test_malformed_or_incomplete_evidence_exits_above_valid_rejection(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            decision = root / "decision.json"
            result = subprocess.run(
                [
                    sys.executable,
                    "scripts/evaluate_exact_bound_shadow.py",
                    str(root),
                    "--manifest",
                    "docs/research/group-commit-residual-pq-local-qualification.json",
                    "--output",
                    str(decision),
                ],
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertEqual(result.returncode, 2)
            self.assertFalse(decision.exists())

    def test_accepts_only_when_every_preregistered_gate_passes(self) -> None:
        accepted = evaluate([row() for _ in range(20)], GATE, OPTIMIZATION)
        self.assertTrue(accepted["accepted"])
        self.assertTrue(accepted["provisional_only"])
        self.assertFalse(accepted["production_default_eligible"])
        self.assertEqual(accepted["survivor_p95"], 11)
        self.assertAlmostEqual(accepted["read_reduction_fraction"], 0.30)
        self.assertAlmostEqual(accepted["byte_reduction_fraction"], 0.30)
        self.assertAlmostEqual(accepted["residual_bytes_per_vector"], 68.0)
        self.assertAlmostEqual(accepted["total_backing_byte_ratio"], 1.8)
        self.assertEqual(accepted["scan_waves_total"], 20)
        self.assertEqual(accepted["certificate_scratch_allocations_p95"], 3)
        self.assertEqual(accepted["read_p50_ms"], 40.0)
        self.assertEqual(accepted["read_p99_ms"], 40.0)
        self.assertIn("structural_and_empirical_floor_gaps", accepted)

        failures = {
            "survivors": row(global_exact_bound_survivors="12"),
            "containment": row(global_exact_bound_containment_failures="1"),
            "reads": row(global_exact_bound_predicted_reads="8"),
            "bytes": row(global_exact_bound_predicted_bytes="70001"),
            "cpu": row(global_exact_bound_cpu_us="2001"),
            "residual_bytes": row(global_exact_bound_residual_bytes="1089"),
            "total_backing_bytes": row(backing_bytes_read="200001"),
            "read_latency_hard_cap": row(latency_ms="201"),
        }
        for expected, failing in failures.items():
            with self.subTest(expected=expected):
                result = evaluate([failing for _ in range(20)], GATE, OPTIMIZATION)
                self.assertFalse(result["accepted"])
                self.assertIn(expected, result["failures"])


if __name__ == "__main__":
    unittest.main()
