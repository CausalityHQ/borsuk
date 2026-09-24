"""Source-ID union and float32/FP16 diagnostic scoring."""

import unittest

import numpy as np

from scripts.v123_rerank_diagnostic import candidate_ids, rank_source


class RerankDiagnosticTests(unittest.TestCase):
    def test_union_maps_physical_nominees_and_deduplicates_wave_ids(self) -> None:
        layout = np.array([2, 0, 1, 3], dtype=np.int64)
        actual = candidate_ids(layout, [0, 1], [3, 2, 1], 2)
        np.testing.assert_array_equal(actual, [0, 2, 3])
        saturated = candidate_ids(layout, [0, 1], [3, 2, 1], 1600)
        np.testing.assert_array_equal(saturated, [0, 1, 2, 3])

    def test_fp16_rounding_can_change_exact_source_order(self) -> None:
        source = np.array([[1.0, 0.0], [1.0001, 0.0], [0.0, 1.0]],
                          dtype=np.float32)
        ids = np.array([0, 1, 2], dtype=np.int64)
        query = np.array([1.0, 0.0], dtype=np.float32)
        self.assertEqual(rank_source(source, ids, query, 2, half=False), [1, 0])
        self.assertEqual(rank_source(source, ids, query, 2, half=True), [0, 1])


if __name__ == "__main__":
    unittest.main()
