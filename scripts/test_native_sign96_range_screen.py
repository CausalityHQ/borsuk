"""Two independent truth-free range plans over the same selected rows."""

from __future__ import annotations

import unittest

import numpy as np

from scripts.native_rotated_sign96 import encode_records
from scripts.native_sign96_range_screen import paired_range_plans, score_selected_rows


class Sign96RangeScreenTests(unittest.TestCase):
    def test_scores_only_selected_groups_in_physical_order(self) -> None:
        rng = np.random.default_rng(31)
        physical_vectors = rng.standard_normal((8, 768)).astype(np.float32) / 10
        mean = np.zeros(768, dtype=np.float32)
        records = encode_records(physical_vectors, mean, rotation_seed=20260923)
        scored = score_selected_rows(
            physical_vectors[0], mean, records, physical_vectors,
            (1,) * 8, (1,), rotation_seed=20260923,
        )
        np.testing.assert_array_equal(scored.positions, np.array([4, 5, 6, 7]))
        np.testing.assert_array_equal(scored.row_pages, np.array([4, 5, 6, 7]))
        np.testing.assert_array_equal(scored.row_groups, np.ones(4, dtype=np.uint32))
        expected = np.sum(
            (physical_vectors[4:].astype(np.float64) - physical_vectors[0].astype(np.float64)) ** 2,
            axis=1,
        )
        np.testing.assert_allclose(scored.source_scores, expected, rtol=1e-12, atol=1e-12)
        self.assertEqual(scored.sign_scores.dtype, np.float32)

    def test_sign_arm_cannot_inherit_source_priority_or_ranges(self) -> None:
        sign = np.asarray([2.0, 1.0], dtype=np.float32)
        source = np.asarray([1.0, 2.0], dtype=np.float64)
        pages = np.asarray([0, 1], dtype=np.uint32)
        groups = np.asarray([0, 0], dtype=np.uint32)
        result = paired_range_plans(
            sign, source, pages, groups, (0,), {"base": (10, 10)},
            maximum_gets=1, maximum_bytes=10,
        )
        self.assertEqual(result["sign96"]["priority_pages"][0], ["base", 1])
        self.assertEqual(result["source"]["priority_pages"][0], ["base", 0])
        self.assertEqual(result["sign96"]["ranges"], [["base", 1, 2]])
        self.assertEqual(result["source"]["ranges"], [["base", 0, 1]])


if __name__ == "__main__":
    unittest.main()
