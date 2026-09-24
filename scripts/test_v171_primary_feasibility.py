"""Exact primary interval lower bound, checked against exhaustive bridges."""

import itertools
import random
import unittest

from scripts.v171_primary_feasibility import minimum_primary_intervals


class MinimumPrimaryIntervalsTests(unittest.TestCase):
    def test_literal_gaps_and_deterministic_tie(self):
        result = minimum_primary_intervals((1, 2, 5, 8), max_gets=2)
        self.assertEqual(result.intervals, ((1, 5), (8, 8)))
        self.assertEqual(result.units, 6)
        self.assertEqual(result.gets, 2)

    def test_small_exhaustive_bridge_optimality(self):
        rng = random.Random(171)
        for _ in range(100):
            units = tuple(sorted(rng.sample(range(12), rng.randrange(1, 7))))
            runs = []
            for unit in units:
                if runs and unit == runs[-1][1] + 1:
                    runs[-1] = (runs[-1][0], unit)
                else:
                    runs.append((unit, unit))
            gaps = [runs[i + 1][0] - runs[i][1] - 1
                    for i in range(len(runs) - 1)]
            for cap in range(1, len(runs) + 1):
                result = minimum_primary_intervals(units, max_gets=cap)
                choices = [sum(b - a + 1 for a, b in runs)
                           + sum(gaps[i] for i in indexes)
                           for count in range(len(gaps) + 1)
                           for indexes in itertools.combinations(range(len(gaps)), count)
                           if len(runs) - count <= cap]
                self.assertEqual(result.units, min(choices))
                self.assertLessEqual(result.gets, cap)
                self.assertTrue(all(any(a <= unit <= b for a, b in result.intervals)
                                    for unit in units))

    def test_rejects_duplicate_or_out_of_range(self):
        with self.assertRaises(ValueError):
            minimum_primary_intervals((1, 1), max_gets=1)
        with self.assertRaises(ValueError):
            minimum_primary_intervals((0, -1), max_gets=1)
        with self.assertRaises(ValueError):
            minimum_primary_intervals((1,), max_gets=0)


if __name__ == "__main__":
    unittest.main()
