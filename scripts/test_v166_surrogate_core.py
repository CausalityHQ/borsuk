"""Narrow checks for the preregistered V166 score surrogate."""

import unittest

from scripts.v166_surrogate_core import fit_alpha, predict_count, primary_weight


class SurrogateCoreTests(unittest.TestCase):
    def test_predicted_count_rises_with_distance_threshold(self):
        low = predict_count(1.0, 0.2, 768, 1.0, 1.0, 32)
        high = predict_count(1.0, 0.2, 768, 1.4, 1.0, 32)
        self.assertLess(low, high)
        self.assertGreaterEqual(low, 0.0)
        self.assertLessEqual(high, 32.0)
        self.assertEqual(predict_count(1.0, 0.2, 768, 1.2, 1.0, 32), 16.0)

    def test_zero_residual_has_finite_step_limit(self):
        self.assertEqual(predict_count(1.0, 0.0, 768, 0.9, 1.0, 31), 0.0)
        self.assertEqual(predict_count(1.0, 0.0, 768, 1.1, 1.0, 31), 31.0)

    def test_fit_uses_only_supplied_cases_and_fixed_grid(self):
        # A wider spread explains an observed half-hit fraction at a
        # threshold slightly below the modeled mean.
        cases = [(1.0, 0.2, 768, 1.18, 32, 15)] * 8
        self.assertEqual(fit_alpha(cases), 8.0)
        self.assertEqual(fit_alpha([(1.0, 0.2, 768, 1.2, 32, 16)]), 0.25)

    def test_primary_weight_dominates_all_possible_candidate_mass(self):
        self.assertGreater(primary_weight(512, 32), 3 * 512 * 32 * 100)
        self.assertLess(100 * primary_weight(512, 32) + 3 * 512 * 32 * 100,
                        2 ** 30)


if __name__ == "__main__":
    unittest.main()
