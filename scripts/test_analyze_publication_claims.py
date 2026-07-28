import unittest

from scripts.analyze_publication_claims import compare_direct


def rows(engine, latency, recall=1.0, repetitions=4, queries=40):
    return [
        {
            "engine": engine,
            "repetition_id": f"r{repetition:02d}",
            "query_position": query,
            "query_source_index": query,
            "latency_ms": latency + repetition * 0.1 + query * 0.001,
            "recall_at_10": recall,
            "status": "ok",
        }
        for repetition in range(1, repetitions + 1)
        for query in range(queries)
    ]


class AnalyzePublicationClaimsTests(unittest.TestCase):
    def test_clear_paired_win_is_accepted(self):
        decision = compare_direct(
            rows("borsuk", 10.0),
            rows("amazon-s3-vectors", 20.0),
            seed=17,
            bootstrap_samples=1000,
        )
        self.assertLess(decision.latency_ratio_ci_high, 1.0)
        self.assertLess(decision.p99_latency_ratio_ci_high, 1.0)
        self.assertGreaterEqual(decision.recall_difference_ci_low, 0.0)
        self.assertEqual(decision.claim, "lower-latency-at-matched-recall")

    def test_rejects_recall_loss_or_uncertain_latency(self):
        recall_loss = compare_direct(
            rows("borsuk", 10.0, recall=0.9),
            rows("amazon-s3-vectors", 20.0),
            seed=17,
            bootstrap_samples=400,
        )
        self.assertEqual(recall_loss.claim, "no-superiority-claim")
        tied = compare_direct(
            rows("borsuk", 20.0),
            rows("amazon-s3-vectors", 20.0),
            seed=17,
            bootstrap_samples=400,
        )
        self.assertEqual(tied.claim, "no-superiority-claim")

    def test_rejects_invalid_or_unpaired_evidence(self):
        with self.assertRaisesRegex(ValueError, "three"):
            compare_direct(
                rows("borsuk", 10.0, repetitions=2),
                rows("amazon-s3-vectors", 20.0, repetitions=2),
            )
        mismatched = rows("amazon-s3-vectors", 20.0)
        mismatched.pop()
        with self.assertRaisesRegex(ValueError, "paired"):
            compare_direct(rows("borsuk", 10.0), mismatched)
        failed = rows("borsuk", 10.0)
        failed[0]["status"] = "failed"
        with self.assertRaisesRegex(ValueError, "failed"):
            compare_direct(failed, rows("amazon-s3-vectors", 20.0))
        wrong_source = rows("amazon-s3-vectors", 20.0)
        wrong_source[0]["query_source_index"] = 999
        with self.assertRaisesRegex(ValueError, "different source queries"):
            compare_direct(rows("borsuk", 10.0), wrong_source)

    def test_exact_confirmatory_cohort_can_be_enforced(self):
        with self.assertRaisesRegex(ValueError, "repetitions"):
            compare_direct(
                rows("borsuk", 10.0, repetitions=4),
                rows("amazon-s3-vectors", 20.0, repetitions=4),
                expected_repetitions=5,
            )
        with self.assertRaisesRegex(ValueError, "queries per repetition"):
            compare_direct(
                rows("borsuk", 10.0, repetitions=5, queries=40),
                rows("amazon-s3-vectors", 20.0, repetitions=5, queries=40),
                expected_repetitions=5,
                expected_queries_per_repetition=1000,
            )


if __name__ == "__main__":
    unittest.main()
