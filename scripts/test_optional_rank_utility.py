"""Checks for GT-blind optional-unit utility and fit-only risk calibration."""

import unittest

from scripts.optional_rank_utility import fit_optional_rank_utility


class OptionalRankUtilityTests(unittest.TestCase):
    def test_mandatory_units_have_no_optional_weight(self):
        examples = [
            ((0, 1, 2, 3), (0,), {0: 32, 1: 2}),
            ((3, 2, 1, 0), (3, 2), {3: 25, 2: 7, 1: 4}),
            ((0, 2, 1, 3), (0,), {0: 30, 2: 3}),
            ((3, 1, 2, 0), (3, 1), {3: 25, 1: 6, 2: 5}),
        ]
        model = fit_optional_rank_utility(examples, result_count=100, bins=2)
        easy = model.weights((0, 1, 2, 3), (0,), units_per_hit=1000)
        hard = model.weights((3, 2, 1, 0), (3, 2), units_per_hit=1000)
        self.assertNotIn(0, easy)
        self.assertNotIn(3, hard)
        self.assertNotIn(2, hard)
        self.assertEqual(sum(easy.values()), 2500)
        self.assertEqual(sum(hard.values()), 4500)

    def test_candidate_omissions_do_not_become_optional_gain(self):
        examples = [
            ((0, 1), (0,), {0: 31, 1: 1, 98: 32, 99: 32, 100: 4}),
            ((0, 1), (0,), {0: 30, 1: 2, 98: 32, 99: 32, 100: 4}),
        ]
        model = fit_optional_rank_utility(examples, result_count=100, bins=2)
        weights = model.weights((0, 1), (0,), units_per_hit=1000)
        self.assertEqual(weights, {1: 1500})

    def test_invalid_geometry_fails_closed(self):
        with self.assertRaises(ValueError):
            fit_optional_rank_utility([((0, 1), (2,), {1: 1})],
                                      result_count=100)
        model = fit_optional_rank_utility([((0, 1), (0,), {1: 1})],
                                          result_count=100)
        with self.assertRaises(ValueError):
            model.weights((0, 0), (0,), units_per_hit=1000)

    def test_small_price_scale_retains_rank_distribution(self):
        examples = [
            (tuple(range(101)), (0,),
             {unit: 1 for unit in range(1, 101)
              if (unit + query) % 5 < 2})
            for query in range(5)
        ]
        model = fit_optional_rank_utility(examples, result_count=100)
        weights = model.weights(tuple(range(101)), (0,), units_per_hit=1)
        self.assertEqual(sum(weights.values()), 40)
        self.assertEqual(len(weights), 40)

    def test_mandatory_only_query_contributes_zero_risk(self):
        model = fit_optional_rank_utility([
            ((0,), (0,), {0: 100}),
            ((0, 1), (0,), {0: 99, 1: 1}),
        ], result_count=100, bins=2)
        self.assertEqual(model.weights((0,), (0,), units_per_hit=1000), {})
        self.assertEqual(sum(model.weights((0, 1), (0,),
                                           units_per_hit=1000).values()), 500)
        all_mandatory = fit_optional_rank_utility([
            ((0,), (0,), {0: 100}),
        ], result_count=100)
        self.assertEqual(all_mandatory.weights((0,), (0,),
                                               units_per_hit=1000), {})
        self.assertEqual(all_mandatory.weights((0, 1), (0,),
                                               units_per_hit=1000), {})

    def test_result_count_is_a_caller_bound(self):
        model = fit_optional_rank_utility(
            [((0, 1), (0,), {0: 1, 1: 1})], result_count=2)
        self.assertEqual(model.result_count, 2)
        self.assertEqual(model.weights((0, 1), (0,),
                                       units_per_hit=1000), {1: 1000})
        with self.assertRaisesRegex(ValueError, "fit geometry"):
            fit_optional_rank_utility(
                [((0, 1), (0,), {0: 1, 1: 2})], result_count=2)


if __name__ == "__main__":
    unittest.main()
