"""Small checks for source-fit PQ rank calibration."""

import unittest

from scripts.source_rank_utility import fit_rank_utility, rank_units


class RankUtilityTests(unittest.TestCase):
    def test_rank_is_score_then_unit_and_truth_is_never_a_feature(self):
        ranked = rank_units({7: 2.0, 3: 1.0, 5: 2.0})
        self.assertEqual(ranked, (3, 5, 7))

    def test_fit_is_weighted_monotone_and_permutation_invariant(self):
        examples = [
            ({3: 1.0, 5: 2.0, 7: 3.0}, {3: 2, 5: 0, 7: 1}),
            ({2: 1.0, 4: 2.0}, {2: 0, 4: 1}),
        ]
        curve = fit_rank_utility(examples)
        self.assertEqual(curve.samples, (2, 2, 1))
        self.assertEqual(curve.expected_hits, (1.0, 2 / 3, 2 / 3))
        self.assertEqual(curve.weights((3, 5, 7)), {3: 1024, 5: 683, 7: 683})
        self.assertEqual(curve.weights((3, 5, 7, 9)),
                         {3: 1024, 5: 683, 7: 683})
        self.assertEqual(fit_rank_utility(reversed(examples)), curve)

    def test_validation_rejects_labels_outside_candidate_universe(self):
        with self.assertRaises(ValueError):
            fit_rank_utility([({1: 0.5}, {2: 1})])
        with self.assertRaises(ValueError):
            rank_units({1: float("nan")})


if __name__ == "__main__":
    unittest.main()
