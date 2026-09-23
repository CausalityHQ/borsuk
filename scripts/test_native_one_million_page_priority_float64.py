"""Retain source-distance precision in the shared page-priority rule."""

from __future__ import annotations

import unittest

import numpy as np

from scripts.native_one_million_page_priority import rank_selected_pages


class Float64PagePriorityTests(unittest.TestCase):
    def test_minimum_order_preserves_float64_gap(self) -> None:
        scores = np.array([1.000000002, 1.000000001], dtype=np.float64)
        pages = np.array([0, 1], dtype=np.uint32)
        groups = np.array([0, 0], dtype=np.uint32)
        self.assertEqual(rank_selected_pages(scores, pages, groups, (0,), 2, top_rows=2), (1, 0))


if __name__ == "__main__":
    unittest.main()
