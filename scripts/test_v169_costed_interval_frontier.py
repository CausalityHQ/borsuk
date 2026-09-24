"""Exact small physical fronts for the V169 GET/byte planner."""

import itertools
import random
import unittest

from scripts.v169_costed_interval_frontier import plan_min_cost_intervals


def brute_cost(weights, mandatory, target, pages, max_gets, max_units,
               get_cost, unit_cost):
    """Enumerate all physical subsets for tiny independent fixtures."""
    feasible = []
    for bits in itertools.product((False, True), repeat=pages):
        selected = {page for page, yes in enumerate(bits) if yes}
        if not set(mandatory).issubset(selected):
            continue
        gets = sum(yes and (page == 0 or not bits[page - 1])
                   for page, yes in enumerate(bits))
        units = len(selected)
        score = sum(weight for page, weight in weights.items()
                    if page in selected)
        if gets <= max_gets and units <= max_units and score >= target:
            feasible.append((get_cost * gets + unit_cost * units,
                             gets, units))
    return min(feasible) if feasible else None


class CostedIntervalTests(unittest.TestCase):
    def test_prices_bridge_as_one_get_and_three_units(self):
        plan = plan_min_cost_intervals(
            {0: 1, 2: 1}, mandatory=(0, 2), target=2,
            page_count=3, max_gets=2, max_units=3,
            get_cost=10, unit_cost=1)
        self.assertEqual(plan.intervals, ((0, 2),))
        self.assertEqual((plan.score, plan.gets, plan.units, plan.cost),
                         (2, 1, 3, 13))

    def test_prices_split_when_bytes_dominate(self):
        plan = plan_min_cost_intervals(
            {0: 1, 2: 1}, mandatory=(0, 2), target=2,
            page_count=3, max_gets=2, max_units=3,
            get_cost=1, unit_cost=10)
        self.assertEqual(plan.intervals, ((0, 0), (2, 2)))
        self.assertEqual(plan.cost, 22)

    def test_forces_zero_weight_primary_and_reports_infeasibility(self):
        plan = plan_min_cost_intervals(
            {}, mandatory=(1,), target=0,
            page_count=3, max_gets=1, max_units=1,
            get_cost=1, unit_cost=1)
        self.assertEqual(plan.intervals, ((1, 1),))
        with self.assertRaises(ValueError):
            plan_min_cost_intervals(
                {}, mandatory=(0, 2), target=0,
                page_count=3, max_gets=1, max_units=1,
                get_cost=1, unit_cost=1)

    def test_matches_exhaustive_cost_on_small_weighted_layouts(self):
        cases = [
            ({0: 3, 2: 4, 4: 5}, (0,), 8, 5, 2, 3, 10, 1),
            ({1: 2, 3: 5, 5: 1}, (3,), 6, 6, 2, 5, 3, 2),
            ({0: 1, 1: 1, 3: 3}, (), 3, 4, 2, 3, 2, 1),
            ({0: 5, 2: 2, 5: 7}, (0, 5), 9, 6, 2, 4, 5, 1),
        ]
        for case in cases:
            expected = brute_cost(*case)
            self.assertIsNotNone(expected)
            plan = plan_min_cost_intervals(
                case[0], mandatory=case[1], target=case[2],
                page_count=case[3], max_gets=case[4], max_units=case[5],
                get_cost=case[6], unit_cost=case[7])
            self.assertEqual((plan.cost, plan.gets, plan.units), expected)

    def test_matches_exhaustive_cost_across_small_random_frontiers(self):
        rng = random.Random(169)
        for _ in range(80):
            pages = rng.randrange(2, 7)
            weights = {page: weight for page in range(pages)
                       if (weight := rng.randrange(4)) > 0}
            mandatory = tuple(page for page in range(pages)
                              if rng.randrange(5) == 0)
            target = rng.randrange(sum(weights.values()) + 2)
            max_gets = rng.randrange(1, pages + 1)
            max_units = rng.randrange(1, pages + 1)
            get_cost, unit_cost = rng.randrange(1, 8), rng.randrange(1, 8)
            case = (weights, mandatory, target, pages, max_gets,
                    max_units, get_cost, unit_cost)
            expected = brute_cost(*case)
            if expected is None:
                with self.assertRaises(ValueError):
                    plan_min_cost_intervals(
                        weights, mandatory=mandatory, target=target,
                        page_count=pages, max_gets=max_gets,
                        max_units=max_units, get_cost=get_cost,
                        unit_cost=unit_cost)
            else:
                plan = plan_min_cost_intervals(
                    weights, mandatory=mandatory, target=target,
                    page_count=pages, max_gets=max_gets,
                    max_units=max_units, get_cost=get_cost,
                    unit_cost=unit_cost)
                self.assertEqual((plan.cost, plan.gets, plan.units), expected)
                covered = {page for start, end in plan.intervals
                           for page in range(start, end + 1)}
                self.assertTrue(set(mandatory).issubset(covered))
                self.assertEqual(plan.score, sum(
                    weight for page, weight in weights.items()
                    if page in covered))
                self.assertGreaterEqual(plan.score, target)
                self.assertEqual(plan.units, len(covered))
                self.assertEqual(plan.gets, len(plan.intervals))

    def test_large_gap_cannot_hide_unpaid_units(self):
        with self.assertRaises(ValueError):
            plan_min_cost_intervals(
                {0: 1, 1000: 1}, mandatory=(0, 1000), target=2,
                page_count=1001, max_gets=1, max_units=2,
                get_cost=1, unit_cost=1)
        plan = plan_min_cost_intervals(
            {0: 1, 1000: 1}, mandatory=(0, 1000), target=2,
            page_count=1001, max_gets=2, max_units=2,
            get_cost=1, unit_cost=1)
        self.assertEqual(plan.intervals, ((0, 0), (1000, 1000)))

    def test_tie_witness_is_stable_for_rust_refinement(self):
        plan = plan_min_cost_intervals(
            {0: 1, 2: 1}, mandatory=(), target=1,
            page_count=3, max_gets=1, max_units=1,
            get_cost=1, unit_cost=1)
        self.assertEqual(plan.intervals, ((0, 0),))
