"""Checks the exact physical cover used by the closed V166 diagnostic."""

import unittest

from scripts.v166_postterminal_rank_diagnostic import minimum_cover_bytes


class MinimumCoverTests(unittest.TestCase):
    def test_keeps_largest_gap_when_two_gets_allowed(self):
        self.assertEqual(minimum_cover_bytes([0, 1, 3, 10], 2), 5 * 24_960)

    def test_one_get_spans_all_required_units(self):
        self.assertEqual(minimum_cover_bytes([0, 1, 10], 1), 11 * 24_960)

    def test_empty_and_duplicate_inputs_rejected(self):
        with self.assertRaises(ValueError):
            minimum_cover_bytes([], 2)
        with self.assertRaises(ValueError):
            minimum_cover_bytes([1, 1], 2)


if __name__ == "__main__":
    unittest.main()
