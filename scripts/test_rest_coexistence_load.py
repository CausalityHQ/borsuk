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

    def test_overload_gate_requires_explicit_429(self) -> None:
        baseline = {"cheap": {"p99_ms": 1.0, "errors": 0, "requests": 100}}
        mixed = {
            "cheap": {"p99_ms": 1.0, "errors": 0, "requests": 100},
            "search": {
                "mean_recall_at_10": 1.0,
                "requests": 100,
                "errors": 0,
                "rejected_429": 0,
                "engines": ["bounded-cell-card-v17"],
            },
        }
        self.assertTrue(
            any("429" in failure for failure in evaluate_phase("mixed-overload", baseline, mixed))
        )

    def test_uncached_gate_accepts_only_the_v17_cell_card_engine(self) -> None:
        baseline = {"cheap": {"p99_ms": 1.0, "errors": 0, "requests": 100}}
        search = {
            "mean_recall_at_10": 1.0,
            "requests": 100,
            "successful_requests": 100,
            "errors": 0,
            "rejected_429": 0,
            "schedule_lag_p99_ms": 1.0,
            "p99_ms": 50.0,
            "engines": ["bounded-cell-card-v17"],
            "backing_reads": 1,
            "disk_cache_reads": 0,
            "disk_cache_bytes_read": 0,
        }
        self.assertEqual(
            evaluate_phase("staircase-16", baseline, {"search": search}),
            [],
        )

    def test_summary_accumulates_physical_backing_and_cache_reads(self) -> None:
        samples = [
            Sample("search", 0, 0, 1, 200, 1.0, "bounded-cell-card-v17", 4, 4096, 0, 0),
            Sample("search", 1, 1, 2, 200, 1.0, "bounded-cell-card-v17", 3, 3072, 2, 2048),
        ]
        search = summarize("search", 1.0, samples)["search"]
        self.assertEqual(search["backing_reads"], 7)
        self.assertEqual(search["backing_bytes_read"], 7168)
        self.assertEqual(search["disk_cache_reads"], 2)
        self.assertEqual(search["disk_cache_bytes_read"], 2048)

    def test_summary_accumulates_exact_candidate_and_phase_telemetry(self) -> None:
        samples = [
            Sample(
                "search",
                0,
                0,
                1,
                200,
                1.0,
                "bounded-cell-card-v17",
                records_scored=512,
                global_leaf_code_pages_read=128,
                global_leaf_code_requests=4,
                global_leaf_exact_requests=12,
                global_leaf_exact_scores=512,
                global_leaf_waves=1,
                global_base_approximate_us=800,
                global_base_exact_rerank_us=7_000,
            ),
            Sample(
                "search",
                1,
                1,
                2,
                200,
                1.0,
                "bounded-cell-card-v17",
                records_scored=500,
                global_leaf_code_pages_read=120,
                global_leaf_code_requests=3,
                global_leaf_exact_requests=11,
                global_leaf_exact_scores=500,
                global_leaf_waves=1,
                global_base_approximate_us=700,
                global_base_exact_rerank_us=6_000,
            ),
        ]
        search = summarize("search", 1.0, samples)["search"]
        self.assertEqual(search["records_scored"], 1_012)
        self.assertEqual(search["global_leaf_code_pages_read"], 248)
        self.assertEqual(search["global_leaf_code_requests"], 7)
        self.assertEqual(search["global_leaf_exact_requests"], 23)
        self.assertEqual(search["global_leaf_exact_scores"], 1_012)
        self.assertEqual(search["global_leaf_waves"], 2)
        self.assertEqual(search["global_base_approximate_us"], 1_500)
        self.assertEqual(search["global_base_exact_rerank_us"], 13_000)

    def test_uncached_gate_rejects_generator_lag_errors_cache_and_no_s3(self) -> None:
        baseline = {"cheap": {"p99_ms": 1.0, "errors": 0, "requests": 100}}
        search = {
            "mean_recall_at_10": 1.0,
            "requests": 100,
            "successful_requests": 98,
            "errors": 1,
            "rejected_429": 1,
            "schedule_lag_p99_ms": 11.0,
            "engines": ["bounded-cell-card-v17"],
            "backing_reads": 0,
            "disk_cache_reads": 1,
            "disk_cache_bytes_read": 4096,
        }
        failures = evaluate_phase(
            "mixed-normal",
            baseline,
            {"cheap": baseline["cheap"], "search": search},
        )
        evidence = "\n".join(failures)
        for expected in ("zero errors", "schedule lag", "HTTP 429", "zero backing", "disk cache"):
            self.assertIn(expected, evidence)

    def test_staircase_gate_rejects_vector_p99_above_frozen_limit(self) -> None:
        baseline = {"cheap": {"p99_ms": 1.0, "errors": 0, "requests": 100}}
        search = {
            "mean_recall_at_10": 1.0,
            "requests": 100,
            "successful_requests": 100,
            "errors": 0,
            "rejected_429": 0,
            "schedule_lag_p99_ms": 1.0,
            "p99_ms": 100.1,
            "engines": ["bounded-cell-card-v17"],
            "backing_reads": 1,
            "disk_cache_reads": 0,
            "disk_cache_bytes_read": 0,
        }
        failures = evaluate_phase("staircase-32", baseline, {"search": search})
        self.assertTrue(any("vector p99" in failure for failure in failures))

    def test_gate_rejects_any_non_v13_search_engine(self) -> None:
        baseline = {"cheap": {"p99_ms": 2.0, "errors": 0, "requests": 100}}
        mixed = {
            "cheap": {"p99_ms": 2.0, "errors": 0, "requests": 100},
            "search": {
                "mean_recall_at_10": 1.0,
                "requests": 2,
                "errors": 0,
                "engines": ["bounded-arrow-leaf-v13", "bounded-cell-card-v17"],
            },
        }
        failures = evaluate_phase("mixed-normal", baseline, mixed)
        self.assertTrue(any("bounded-cell-card-v17" in failure for failure in failures))


if __name__ == "__main__":
    unittest.main()
