"""Decision and accounting checks for the preregistered V189 source gate."""

import unittest

from scripts.source_rank_utility import RankUtility
from scripts.v189_predicted_interval_source import (
    decide, rank_weights, validate_intervals,
)


class V189PredictedIntervalSourceTests(unittest.TestCase):
    def test_decision_requires_quality_and_both_resource_caps(self):
        good = {"hits": 12745, "p05": 98, "infeasible": 0,
                "bytes": 1_425_141_120, "gets": 2832}
        self.assertEqual(decide(good), "advance-to-replication")
        for update in ({"hits": 12744}, {"p05": 97},
                       {"infeasible": 1}, {"gets": 2833},
                       {"bytes": 1_425_166_080}):
            self.assertEqual(decide(good | update),
                             "revise-utility-or-layout")

    def test_interval_checker_counts_bridges_and_mandatory(self):
        result = validate_intervals(((1, 3), (7, 7)), (2, 7),
                                    page_count=10, max_units=4, max_gets=2)
        self.assertEqual(result, (4, 2, 4 * 24_960))
        with self.assertRaises(ValueError):
            validate_intervals(((1, 3),), (2, 7),
                               page_count=10, max_units=4, max_gets=2)

    def test_rank_control_uses_same_millionth_hit_scale_as_margin(self):
        model = RankUtility((1.25, 0.5), (2, 2))
        self.assertEqual(rank_weights(model, [4, 7]),
                         {4: 1_250_000, 7: 500_000})


if __name__ == "__main__":
    unittest.main()
