"""Exact small-instance checks for a hard GET and unit capped price DP."""

import itertools
import random
import unittest

from scripts.hard_priced_interval import hard_priced_cover


def brute(weights, mandatory, count, max_gets, max_units,
          unit_price, get_price):
    candidates = []
    for bits in itertools.product((False, True), repeat=count):
        covered = {i for i, yes in enumerate(bits) if yes}
        if not set(mandatory) <= covered:
            continue
        units = len(covered)
        gets = sum(yes and (i == 0 or not bits[i-1])
                   for i, yes in enumerate(bits))
        if gets > max_gets or units > max_units:
            continue
        mass = sum(value for unit, value in weights.items()
                   if unit in covered)
        candidates.append((mass-unit_price*units-get_price*gets,
                           -units, -gets, mass))
    return max(candidates, default=None)


class HardPricedIntervalTests(unittest.TestCase):
    def test_hard_unit_cap_changes_unconstrained_optimum(self):
        plan = hard_priced_cover({0: 10, 2: 8}, (0,), page_count=3,
                                 max_gets=2, max_units=1,
                                 unit_price=0, get_price=0,
                                 max_trace_bytes=1_000_000)
        self.assertEqual(plan.intervals, ((0, 0),))
        self.assertEqual((plan.mass, plan.units, plan.gets), (10, 1, 1))

    def test_mandatory_gap_can_make_hard_caps_infeasible(self):
        with self.assertRaisesRegex(ValueError, "infeasible"):
            hard_priced_cover({0: 1, 2: 1}, (0, 2), page_count=3,
                              max_gets=1, max_units=2,
                              unit_price=1, get_price=1,
                              max_trace_bytes=1)

    def test_optimum_matches_exhaustive_small_covers(self):
        rng = random.Random(190)
        for _ in range(90):
            count = rng.randrange(2, 8)
            weights = {i: value for i in range(count)
                       if (value := rng.randrange(5)) > 0}
            mandatory = tuple(i for i in range(count)
                              if rng.randrange(4) == 0)
            gets = rng.randrange(1, count + 1)
            units = rng.randrange(1, count + 1)
            up, gp = rng.randrange(5), rng.randrange(8)
            expected = brute(weights, mandatory, count, gets, units, up, gp)
            if expected is None:
                with self.assertRaises(ValueError):
                    hard_priced_cover(weights, mandatory, page_count=count,
                                      max_gets=gets, max_units=units,
                                      unit_price=up, get_price=gp,
                                      max_trace_bytes=1_000_000)
                continue
            plan = hard_priced_cover(weights, mandatory, page_count=count,
                                     max_gets=gets, max_units=units,
                                     unit_price=up, get_price=gp,
                                     max_trace_bytes=1_000_000)
            self.assertEqual((plan.objective, -plan.units, -plan.gets,
                              plan.mass), expected)
            self.assertTrue(set(mandatory) <= {unit
                            for a, b in plan.intervals
                            for unit in range(a, b+1)})

    def test_trace_budget_rejects_before_allocation(self):
        with self.assertRaisesRegex(ValueError, "trace budget"):
            hard_priced_cover({0: 1, 2: 1}, (0,), page_count=3,
                              max_gets=2, max_units=3,
                              unit_price=0, get_price=0,
                              max_trace_bytes=1)

    def test_unit_cap_larger_than_object_is_clamped(self):
        plan = hard_priced_cover({0: 1, 2: 1}, (0,), page_count=3,
                                 max_gets=2, max_units=1_000_000,
                                 unit_price=0, get_price=0,
                                 max_trace_bytes=1_000_000)
        self.assertEqual(plan.units, 2)


if __name__ == "__main__":
    unittest.main()
