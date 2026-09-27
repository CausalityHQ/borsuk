"""Bridge ties deterministically before charging a page-primary plan."""

import unittest

from scripts.v284_page_primary_dev64 import covered_pages


class CoverTests(unittest.TestCase):
    def test_thirty_three_runs_bridge_the_leftmost_shortest_gap(self):
        selected = set(range(0, 99, 3))
        cover, gets = covered_pages(selected)
        self.assertEqual(gets, 32)
        self.assertEqual(cover - selected, {1, 2})


if __name__ == "__main__":
    unittest.main()
