#!/usr/bin/env python3
"""Tests for the preregistered exact-bound shadow decision gate."""

import unittest

from scripts.evaluate_exact_bound_shadow import evaluate

GATE = {
    "require_zero_containment_failures": True,
    "max_survivor_p95": 12,
    "min_read_reduction_fraction": 0.30,
    "min_byte_reduction_fraction": 0.30,
    "max_cpu_p95_us": 2_000,
    "max_cpu_fraction_of_read_p95": 0.05,
}


def row(**updates: str) -> dict[str, str]:
    values = {
        "latency_ms": "40",
        "global_exact_bound_candidates": "16",
        "global_exact_bound_survivors": "12",
        "global_exact_bound_fail_open": "0",
        "global_exact_bound_containment_failures": "0",
        "global_exact_bound_baseline_reads": "10",
        "global_exact_bound_baseline_bytes": "1000",
        "global_exact_bound_predicted_reads": "7",
        "global_exact_bound_predicted_bytes": "700",
        "global_exact_bound_cpu_us": "1000",
    }
    values.update(updates)
    return values


class ExactBoundShadowEvaluatorTests(unittest.TestCase):
    def test_accepts_only_when_every_preregistered_gate_passes(self) -> None:
        accepted = evaluate([row() for _ in range(20)], GATE)
        self.assertTrue(accepted["accepted"])
        self.assertEqual(accepted["survivor_p95"], 12)
        self.assertAlmostEqual(accepted["read_reduction_fraction"], 0.30)
        self.assertAlmostEqual(accepted["byte_reduction_fraction"], 0.30)

        failures = {
            "survivors": row(global_exact_bound_survivors="13"),
            "containment": row(global_exact_bound_containment_failures="1"),
            "reads": row(global_exact_bound_predicted_reads="8"),
            "bytes": row(global_exact_bound_predicted_bytes="701"),
            "cpu": row(global_exact_bound_cpu_us="2001"),
        }
        for expected, failing in failures.items():
            with self.subTest(expected=expected):
                result = evaluate([failing for _ in range(20)], GATE)
                self.assertFalse(result["accepted"])
                self.assertIn(expected, result["failures"])


if __name__ == "__main__":
    unittest.main()
