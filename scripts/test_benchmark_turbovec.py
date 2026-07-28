import unittest

import numpy as np

from scripts.benchmark_turbovec import (
    exact_rerank,
    percentile,
    recall_at_k,
    sample_stddev,
)


class BenchmarkTurboVecTests(unittest.TestCase):
    def test_recall_uses_top_k_set_overlap(self) -> None:
        self.assertEqual(recall_at_k([[1, 2], [3, 4]], [[2, 9], [3, 8]], 2), 0.5)

    def test_percentile_uses_nearest_rank(self) -> None:
        self.assertEqual(percentile([4.0, 1.0, 3.0, 2.0], 0.95), 4.0)
        self.assertAlmostEqual(sample_stddev([1.0, 2.0, 3.0, 4.0]), 1.2909944487)

    def test_exact_rerank_orders_only_the_supplied_candidates(self) -> None:
        train = np.asarray([[1.0, 0.0], [0.8, 0.2], [0.0, 1.0]], dtype=np.float32)
        queries = np.asarray([[1.0, 0.0]], dtype=np.float32)
        reranked = exact_rerank("cosine", train, queries, [[2, 1]], 2)
        self.assertEqual(reranked, [[1, 2]])

    def test_inner_product_rerank_does_not_normalize_vector_lengths(self) -> None:
        train = np.asarray([[1.0, 0.0], [2.0, 0.0]], dtype=np.float32)
        queries = np.asarray([[1.0, 0.0]], dtype=np.float32)
        self.assertEqual(
            exact_rerank("inner-product", train, queries, [[0, 1]], 2), [[1, 0]]
        )


if __name__ == "__main__":
    unittest.main()
