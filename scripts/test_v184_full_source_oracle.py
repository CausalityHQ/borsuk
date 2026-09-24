"""Exact-source truth exclusion for the full physical oracle."""

import unittest
from unittest.mock import patch

import numpy as np

from scripts.v184_full_source_oracle import _truth_rows


class FullSourceOracleTests(unittest.TestCase):
    def test_exact_truth_excludes_source_query_row(self):
        source = np.eye(101, dtype=np.float64)
        stable_ids = np.arange(101, dtype=np.int64)
        with patch("scripts.v184_full_source_oracle.ROWS", 101):
            truth = _truth_rows(source, stable_ids, 0)
        self.assertEqual(len(truth), 100)
        self.assertNotIn(0, truth)


if __name__ == "__main__":
    unittest.main()
