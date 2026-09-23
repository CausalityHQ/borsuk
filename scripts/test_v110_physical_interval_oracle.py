from __future__ import annotations

import itertools
import random
import unittest

from scripts.v110_physical_interval_oracle import (
    FULL_PAGE_UNITS, LAST_PAGE_UNITS, MAX_UNITS, PAGE_COUNT,
    UNIT_BYTES, optimal_truth_hits,
)


def brute_force(weights: dict[int, int], pages: int, gets: int,
                units: int, full_cost: int, last_cost: int) -> int:
    best = 0
    for selected in itertools.product((False, True), repeat=pages):
        requests = sum(selected[index] and (index == 0 or not selected[index - 1])
                       for index in range(pages))
        paid = sum((last_cost if index == pages - 1 else full_cost)
                   for index, included in enumerate(selected) if included)
        if requests <= gets and paid <= units:
            best = max(best, sum(weight for page, weight in weights.items()
                                 if selected[page]))
    return best


class PhysicalOracleTests(unittest.TestCase):
    def test_byte_units_are_exact_for_v63_final_page(self) -> None:
        self.assertEqual(UNIT_BYTES, 49_920)
        self.assertEqual(MAX_UNITS, 336)
        self.assertEqual(FULL_PAGE_UNITS, 4)
        self.assertEqual(LAST_PAGE_UNITS, 1)
        self.assertEqual(PAGE_COUNT, 3907)

    def test_dynamic_program_matches_exhaustive_interval_selection(self) -> None:
        generator = random.Random(11001)
        for _ in range(80):
            weights = {page: generator.randint(1, 3) for page in range(7)
                       if generator.randrange(2)}
            gets = generator.randint(1, 3)
            budget = generator.randint(1, 12)
            expected = brute_force(weights, 7, gets, budget, 3, 1)
            observed = optimal_truth_hits(
                weights, page_count=7, max_gets=gets, max_units=budget,
                full_page_units=3, last_page_units=1,
            )
            self.assertEqual(observed, expected,
                             (weights, gets, budget, observed, expected))

    def test_generic_high_weight_does_not_overflow(self) -> None:
        self.assertEqual(optimal_truth_hits(
            {0: 51_300, 6: 412}, page_count=7, max_gets=1,
            max_units=3, full_page_units=3, last_page_units=1,
        ), 51_300)


if __name__ == "__main__":
    unittest.main()
