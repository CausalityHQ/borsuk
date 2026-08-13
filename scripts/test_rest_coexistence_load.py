from __future__ import annotations

import unittest

from scripts.rest_coexistence_load import (
    Sample,
    evaluate_phase,
    percentile,
    scheduled_offsets_ns,
    summarize,
)


class RestCoexistenceLoadTest(unittest.TestCase):
    def test_open_loop_schedule_uses_absolute_offsets(self) -> None:
        self.assertEqual(scheduled_offsets_ns(4.0, 1.0), [0, 250_000_000, 500_000_000, 750_000_000])
        self.assertEqual(scheduled_offsets_ns(0.0, 1.0), [])

    def test_percentile_uses_nearest_rank(self) -> None:
        self.assertEqual(percentile([1.0, 2.0, 3.0, 100.0], 0.99), 100.0)
        self.assertEqual(percentile([1.0, 2.0, 3.0, 100.0], 0.50), 2.0)

    def test_summary_keeps_queue_delay_and_recall(self) -> None:
        samples = [
            Sample("cheap", 0, 2_000_000, 5_000_000, 200, None),
            Sample("search", 0, 1_000_000, 11_000_000, 200, 0.9),
            Sample("search", 3_000_000, 7_000_000, 8_000_000, 429, None),
        ]
        summary = summarize("mixed", 1.0, samples)
        self.assertEqual(summary["cheap"]["p99_ms"], 5.0)
        self.assertEqual(summary["search"]["p99_ms"], 11.0)
        self.assertEqual(summary["search"]["schedule_lag_p99_ms"], 4.0)
        self.assertEqual(summary["search"]["rejected_429"], 1)
        self.assertEqual(summary["search"]["mean_recall_at_10"], 0.9)

    def test_gate_rejects_cheap_tail_interference_and_low_recall(self) -> None:
        baseline = {"cheap": {"p99_ms": 2.0, "errors": 0, "requests": 100}}
        mixed = {
            "cheap": {"p99_ms": 8.0, "errors": 0, "requests": 100},
            "search": {"mean_recall_at_10": 0.8, "requests": 20, "errors": 0},
        }
        failures = evaluate_phase("mixed-normal", baseline, mixed)
        self.assertTrue(any("cheap p99" in failure for failure in failures))
        self.assertTrue(any("recall@10" in failure for failure in failures))


if __name__ == "__main__":
    unittest.main()
