import unittest

from scripts.v281_10m import decide


class V281DecisionTest(unittest.TestCase):
    def test_promotes_only_matched_high_recall_with_bounded_cost(self):
        baseline = {"combined": {"hits": 99299}, "p95_ms": 120.0, "p99_ms": 147.0}
        candidate = {
            "combined": {"hits": 99520},
            "development_first256_prior_used": {
                "hits": 25480,
                "recall_at_100": 25480 / 25600,
                "p05_hits": 98,
            },
            "validation_remaining744_prior_used": {
                "hits": 74040,
                "recall_at_100": 74040 / 74400,
                "p05_hits": 98,
            },
            "p95_ms": 140.0,
            "p99_ms": 170.0,
        }
        old_root = {
            "source_sha256": "source",
            "rows": 10_000_000,
            "dimensions": 768,
            "graph": {"bytes": 2_678_381_310},
        }
        for name in ("plane", "map", "books", "codes"):
            old_root[name] = {"sha256": name}
        new_root = {**old_root, "graph": {"bytes": 2_700_000_000}}
        resources = {
            "baseline_parity": 1000,
            "search_rss_bytes": 21_000_000_000,
            "build_rss_bytes": 65_000_000_000,
            "build_seconds": 30_000,
            "hydration_gets": 5,
            "query_gets": 0,
        }
        self.assertEqual(
            decide(baseline, candidate, old_root, new_root, resources), "go_http_10m"
        )
        candidate["combined"]["hits"] = 99480
        candidate["development_first256_prior_used"]["hits"] = 25460
        candidate["development_first256_prior_used"]["recall_at_100"] = 25460 / 25600
        candidate["validation_remaining744_prior_used"]["hits"] = 74020
        candidate["validation_remaining744_prior_used"]["recall_at_100"] = 74020 / 74400
        self.assertEqual(
            decide(baseline, candidate, old_root, new_root, resources), "no_go_10m"
        )
        candidate["combined"]["hits"] = 99520
        candidate["development_first256_prior_used"]["hits"] = 25480
        candidate["development_first256_prior_used"]["recall_at_100"] = 25480 / 25600
        candidate["validation_remaining744_prior_used"]["hits"] = 74040
        candidate["validation_remaining744_prior_used"]["recall_at_100"] = 74040 / 74400
        resources["baseline_parity"] = 999
        self.assertEqual(
            decide(baseline, candidate, old_root, new_root, resources), "invalid_cell"
        )


if __name__ == "__main__":
    unittest.main()
