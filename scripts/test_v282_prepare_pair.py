import unittest

import numpy as np

from scripts.v282_prepare_pair import topk_ids


class ExactTruthTest(unittest.TestCase):
    def test_boundary_ties_use_stable_source_id(self):
        self.assertEqual(topk_ids(np.array([0.5, 0.8, 0.8, 0.8, 0.9]), 3).tolist(),
                         [4, 1, 2])


if __name__ == "__main__":
    unittest.main()
