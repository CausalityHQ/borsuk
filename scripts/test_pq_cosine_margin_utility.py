"""Small deterministic checks for the GT-blind PQ cosine utility feature."""

import unittest

from scripts.pq_cosine_margin_utility import (
    fit_margin_utility, normalized_pq_margins,
)


class PqCosineMarginUtilityTests(unittest.TestCase):
    def test_threshold_and_spread_use_only_the_same_pq_metric(self):
        nominees = [-1.0 + i * 0.001 for i in range(120)]
        scores = {8: nominees[0], 9: nominees[99],
                  10: nominees[119]}
        margins = normalized_pq_margins(scores, nominees)
        self.assertLess(margins[8], margins[9])
        self.assertAlmostEqual(margins[9], 0.0)
        self.assertGreater(margins[10], 0.0)
        self.assertAlmostEqual(margins[8], -99 / 80)

    def test_fit_is_monotone_and_predictions_do_not_need_truth(self):
        fit = [({0: -2.0, 1: -1.0, 2: 1.0, 3: 2.0},
                {0: 3, 1: 2, 2: 0, 3: 0}),
               ({4: -2.0, 5: -1.0, 6: 1.0, 7: 2.0},
                {4: 2, 5: 1, 6: 0, 7: 0})]
        model = fit_margin_utility(fit, bins=4)
        predicted = model.weights({11: -2.0, 12: -1.0,
                                   13: 1.0, 14: 2.0})
        self.assertGreater(predicted[11], predicted[12])
        self.assertGreater(predicted[12], 0)
        self.assertNotIn(13, predicted)
        self.assertNotIn(14, predicted)

    def test_ties_and_bad_geometry(self):
        model = fit_margin_utility([({0: 0.0, 1: 0.0}, {0: 1})], bins=4)
        self.assertEqual(model.weights({4: 0.0})[4],
                         model.weights({5: 0.0})[5])
        with self.assertRaises(ValueError):
            normalized_pq_margins({1: 0.0}, [0.0] * 99)
        with self.assertRaises(ValueError):
            fit_margin_utility([], bins=4)


if __name__ == "__main__":
    unittest.main()
