"""Exhaustive small-geometry checks for V167's minimum-cost frontier."""

import itertools
import unittest

from scripts.v167_min_cost_vote_frontier import minimum_vote_frontier


def exhaustive_min_units(weights, target, page_count, max_gets):
    best = None
    for chosen_count in range(1, page_count + 1):
        for pages in itertools.combinations(range(page_count), chosen_count):
            runs = 1 + sum(right != left + 1
                           for left, right in zip(pages, pages[1:]))
            if runs > max_gets:
                continue
            if sum(weights.get(page, 0) for page in pages) >= target:
                best = chosen_count
                break
        if best is not None:
            return best
    return None


class MinimumVoteFrontierTests(unittest.TestCase):
    def test_fractional_target_matches_exhaustive_minimum(self):
        weights = {0: 11, 2: 11, 3: 1, 5: 1}
        for numerator, denominator, target, units in (
            (1, 2, 23, 3), (3, 4, 24, 5), (1, 1, 24, 5),
        ):
            result = minimum_vote_frontier(
                weights, primary_units=(0, 2), primary_vote_total=22,
                secondary_vote_total=2, fraction=(numerator, denominator),
                page_count=7, max_gets=2, max_units=6,
            )
            self.assertEqual(result.units, exhaustive_min_units(
                weights, result.target_score, 7, 2))
            self.assertEqual((result.target_score, result.units),
                             (target, units))
            covered = {unit for start, end in result.intervals
                       for unit in range(start, end + 1)}
            self.assertTrue({0, 2}.issubset(covered))

    def test_higher_fraction_cannot_lower_minimum_cost(self):
        weights = {0: 6, 2: 6, 4: 1, 6: 1}
        costs = [minimum_vote_frontier(
            weights, primary_units=(0, 2), primary_vote_total=12,
            secondary_vote_total=2, fraction=fraction, page_count=8,
            max_gets=3, max_units=8,
        ).units for fraction in ((1, 4), (1, 2), (3, 4), (1, 1))]
        self.assertEqual(costs, sorted(costs))

    def test_primary_infeasibility_is_explicit(self):
        with self.assertRaisesRegex(ValueError, "primary"):
            minimum_vote_frontier(
                {0: 5, 7: 5}, primary_units=(0, 7),
                primary_vote_total=10, secondary_vote_total=0,
                fraction=(1, 1), page_count=8, max_gets=1,
                max_units=2,
            )

    def test_equal_byte_frontier_prefers_fewer_gets_over_extra_votes(self):
        result = minimum_vote_frontier(
            {0: 513, 1: 1, 4: 2}, primary_units=(0,),
            primary_vote_total=513, secondary_vote_total=3,
            fraction=(1, 3), page_count=5, max_gets=2,
            max_units=3,
        )
        self.assertEqual(result.target_score, 514)
        self.assertEqual(result.units, 2)
        self.assertEqual(result.intervals, ((0, 1),))


if __name__ == "__main__":
    unittest.main()
