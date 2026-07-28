import unittest

from scripts.benchmark_faiss import (
    effective_pq_subspaces,
    percentile,
    recall_at_k,
    sample_stddev,
)


class BenchmarkFaissTests(unittest.TestCase):
    def test_recall_uses_top_k_set_overlap(self) -> None:
        self.assertEqual(recall_at_k([[1, 2], [3, 4]], [[2, 9], [3, 8]], 2), 0.5)

    def test_percentile_uses_nearest_rank(self) -> None:
        self.assertEqual(percentile([4.0, 1.0, 3.0, 2.0], 0.95), 4.0)
        self.assertAlmostEqual(sample_stddev([1.0, 2.0, 3.0, 4.0]), 1.2909944487)
        self.assertEqual(sample_stddev([1.0]), 0.0)

    def test_pq_subspaces_match_the_requested_bits_per_dimension(self) -> None:
        self.assertEqual(effective_pq_subspaces(100, 4, 0), 50)
        self.assertEqual(effective_pq_subspaces(100, 2, 0), 25)
        self.assertEqual(effective_pq_subspaces(96, 4, 24), 24)
        with self.assertRaisesRegex(ValueError, "divisible"):
            effective_pq_subspaces(100, 4, 32)


if __name__ == "__main__":
    unittest.main()
