from __future__ import annotations

import random
import unittest
from unittest.mock import patch

import numpy as np

from scripts.test_v110_physical_interval_oracle import brute_force
from scripts.v111_weighted_interval_plan import optimal_weighted_intervals
from scripts.v111_weighted_reader_replay import nominate_count_weights, weighted_plan


class WeightedIntervalPlanTests(unittest.TestCase):
    def test_witness_matches_exhaustive_selection(self) -> None:
        generator = random.Random(11101)
        for _ in range(100):
            pages = 7
            weights = {page: generator.randint(1, 8) for page in range(pages)
                       if generator.randrange(2)}
            gets = generator.randint(1, 3)
            units = generator.randint(1, 12)
            score, ranges = optimal_weighted_intervals(
                weights, page_count=pages, max_gets=gets, max_units=units,
                full_page_units=3, last_page_units=1,
            )
            selected = {page for start, end in ranges for page in range(start, end + 1)}
            paid = sum(1 if page == pages - 1 else 3 for page in selected)
            self.assertEqual(score, brute_force(weights, pages, gets, units, 3, 1))
            self.assertEqual(score, sum(weight for page, weight in weights.items()
                                        if page in selected))
            self.assertLessEqual(len(ranges), gets)
            self.assertLessEqual(paid, units)
            self.assertEqual(len(selected), sum(end - start + 1 for start, end in ranges))

    def test_final_short_page_and_gap_cost(self) -> None:
        score, ranges = optimal_weighted_intervals(
            {0: 5, 2: 4, 6: 3}, page_count=7, max_gets=1,
            max_units=7, full_page_units=3, last_page_units=1,
        )
        self.assertEqual(score, 5)
        self.assertEqual(ranges, ((0, 0),))

    def test_empty_weights(self) -> None:
        self.assertEqual(optimal_weighted_intervals(
            {}, page_count=7, max_gets=2, max_units=5,
            full_page_units=3, last_page_units=1,
        ), (0, ()))

    def test_nomination_counts_every_top512_row(self) -> None:
        manifest = {
            "summaries": np.zeros((4, 768), dtype=np.float32),
            "books": np.zeros((64, 256, 12), dtype=np.float32),
            "codes": np.zeros((512, 64), dtype=np.uint8),
        }
        with patch("scripts.v111_weighted_reader_replay.ROWS", 512):
            ranked, historical, weights = nominate_count_weights(
                np.zeros(768, dtype=np.float32), manifest,
                regions=2, shortlist=512,
            )
        self.assertEqual(ranked, [0, 1])
        self.assertEqual(historical, [0, 1])
        self.assertEqual(weights, {0: 256, 1: 256})

    def test_last_short_page_cost_is_exact(self) -> None:
        score, plan = weighted_plan({3906: 1})
        self.assertEqual(score, 1)
        self.assertEqual(plan.gets, 1)
        self.assertEqual(plan.bytes, 49_920)
        self.assertEqual(plan.ranges, ((999_936 * 780, 1_000_000 * 780),))


if __name__ == "__main__":
    unittest.main()
