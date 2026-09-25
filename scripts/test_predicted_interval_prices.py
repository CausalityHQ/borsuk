"""Price selection must preserve exact physical resource accounting."""

import unittest

from scripts.predicted_interval_prices import (
    QueryUtility, select_priced_plans,
)


class PredictedIntervalPricesTests(unittest.TestCase):
    def test_selects_highest_predicted_mass_under_both_caps(self):
        queries = (QueryUtility((0,), {0: 10, 2: 8}, 3),
                   QueryUtility((0,), {0: 10, 2: 8}, 3))
        chosen = select_priced_plans(
            queries, prices=((0, 0), (9, 0)),
            max_gets_per_query=2, max_units_per_query=3,
            max_total_units=2, max_total_gets=2)
        self.assertEqual(chosen.unit_price, 9)
        self.assertEqual(chosen.get_price, 0)
        self.assertEqual(chosen.total_units, 2)
        self.assertEqual(chosen.total_gets, 2)
        self.assertEqual(chosen.predicted_mass, 20)

    def test_rejects_when_no_price_obeys_a_hard_cap(self):
        queries = (QueryUtility((0, 2), {0: 2, 2: 2}, 3),)
        with self.assertRaisesRegex(ValueError, "no feasible price"):
            select_priced_plans(queries, prices=((0, 0),),
                                max_gets_per_query=1,
                                max_units_per_query=2,
                                max_total_units=2, max_total_gets=1)


if __name__ == "__main__":
    unittest.main()
