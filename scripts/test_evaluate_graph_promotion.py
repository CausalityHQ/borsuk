import unittest

from evaluate_graph_promotion import evaluate_dataset, overall_decision


def passing_rows(dataset="fashion-mnist-784"):
    rows = []
    for repetition in (1, 2, 3):
        rows.append(
            {
                "dataset": dataset,
                "method": "pq-scan",
                "index_capability": "pq-scan-only",
                "profile": "production",
                "cache_state": "disk_cached",
                "repetition": repetition,
                "queries": 100,
                "recall_at_10": 0.951,
                "p95_ms": 100.0,
                "p99_ms": 110.0,
                "max_ms": 120.0,
                "max_candidates": 11,
                "segment_max_vectors": 512,
                "meets_target": "true",
                "qps": 40.0,
                "peak_rss_bytes": 400_000_000,
                "ram_budget_bytes": 1_000_000_000,
                "max_concurrent_searches": 4,
                "max_concurrent_cell_decodes": 24,
                "network_gets": 0,
                "network_bytes": 0,
                "source_sha": "abc123",
            }
        )
        rows.append(
            {
                "dataset": dataset,
                "method": "graph",
                "index_capability": "graph-enabled",
                "profile": "production",
                "cache_state": "disk_cached",
                "repetition": repetition,
                "queries": 100,
                "recall_at_10": 0.951,
                "p95_ms": 80.0,
                "p99_ms": 90.0,
                "max_ms": 100.0,
                "max_candidates": 256,
                "segment_max_vectors": 512,
                "meets_target": "true",
                "qps": 50.0,
                "peak_rss_bytes": 450_000_000,
                "ram_budget_bytes": 1_000_000_000,
                "max_concurrent_searches": 4,
                "max_concurrent_cell_decodes": 24,
                "network_gets": 0,
                "network_bytes": 0,
                "source_sha": "abc123",
            }
        )
    return rows


class GraphPromotionEvaluationTest(unittest.TestCase):
    def test_accepts_dataset_only_when_every_gate_passes(self):
        decision = evaluate_dataset(passing_rows())
        self.assertTrue(decision.passed, decision.reasons)
        self.assertEqual(decision.reasons, ())

    def test_rejects_one_slow_graph_repetition(self):
        rows = passing_rows()
        graph = [row for row in rows if row["method"] == "graph"]
        graph[2]["p95_ms"] = 100.001

        decision = evaluate_dataset(rows)

        self.assertFalse(decision.passed)
        self.assertTrue(any("p95" in reason for reason in decision.reasons))

    def test_rejects_recall_loss_above_one_thousandth(self):
        rows = passing_rows()
        for row in rows:
            if row["method"] == "graph":
                row["recall_at_10"] = 0.949

        decision = evaluate_dataset(rows)

        self.assertFalse(decision.passed)
        self.assertTrue(any("recall" in reason for reason in decision.reasons))

    def test_rejects_disk_cached_network_io(self):
        rows = passing_rows()
        next(row for row in rows if row["method"] == "graph")["network_gets"] = 1

        decision = evaluate_dataset(rows)

        self.assertFalse(decision.passed)
        self.assertTrue(any("network" in reason for reason in decision.reasons))

    def test_accepts_decimal_formatted_zero_network_io(self):
        rows = passing_rows()
        for row in rows:
            row["network_gets"] = "0.000"
            row["network_bytes"] = "0.000"

        decision = evaluate_dataset(rows)

        self.assertTrue(decision.passed, decision.reasons)

    def test_rejects_missing_repetition_and_mixed_source(self):
        rows = passing_rows()[:-1]
        rows[0]["source_sha"] = "different"

        decision = evaluate_dataset(rows)

        self.assertFalse(decision.passed)
        self.assertTrue(any("repetitions" in reason for reason in decision.reasons))
        self.assertTrue(any("source_sha" in reason for reason in decision.reasons))

    def test_rejects_graph_rss_above_relative_or_absolute_budget(self):
        rows = passing_rows()
        for row in rows:
            if row["method"] == "graph":
                row["peak_rss_bytes"] = 600_000_000

        decision = evaluate_dataset(rows)

        self.assertFalse(decision.passed)
        self.assertTrue(any("RSS" in reason for reason in decision.reasons))

    def test_rejects_uncapped_or_multi_second_production_row(self):
        rows = passing_rows()
        graph = next(row for row in rows if row["method"] == "graph")
        graph["max_concurrent_cell_decodes"] = 0
        graph["max_ms"] = 2_001.0

        decision = evaluate_dataset(rows)

        self.assertFalse(decision.passed)
        self.assertTrue(any("cap" in reason for reason in decision.reasons))
        self.assertTrue(any("multi-second" in reason for reason in decision.reasons))

    def test_rejects_full_cell_scan_or_missed_target_as_graph_promotion(self):
        rows = passing_rows()
        graph = [row for row in rows if row["method"] == "graph"]
        graph[0]["max_candidates"] = graph[0]["segment_max_vectors"]
        graph[1]["meets_target"] = "false"

        decision = evaluate_dataset(rows)

        self.assertFalse(decision.passed)
        self.assertTrue(any("full-cell scan" in reason for reason in decision.reasons))
        self.assertTrue(any("recall target" in reason for reason in decision.reasons))

    def test_overall_decision_is_universal_adaptive_or_keep_pq(self):
        passes = [
            evaluate_dataset(passing_rows("a")),
            evaluate_dataset(passing_rows("b")),
        ]
        fails = evaluate_dataset(passing_rows("c")[:-1])
        self.assertEqual(overall_decision(passes), "universal-graph")
        self.assertEqual(overall_decision([passes[0], fails]), "adaptive")
        self.assertEqual(overall_decision([fails]), "keep-pq-scan")


if __name__ == "__main__":
    unittest.main()
