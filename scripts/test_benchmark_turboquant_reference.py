import unittest

import numpy as np

from scripts.benchmark_turboquant_reference import (
    approximate_distances,
    haar_rotation,
    lloyd_max_sphere_codebook,
    percentile,
    recall_at_k,
    sample_stddev,
)


class BenchmarkTurboQuantReferenceTests(unittest.TestCase):
    def test_haar_rotation_is_orthogonal_and_deterministic(self) -> None:
        first = haar_rotation(8, 17)
        second = haar_rotation(8, 17)
        np.testing.assert_allclose(first, second)
        np.testing.assert_allclose(first @ first.T, np.eye(8), atol=1e-5)

    def test_lloyd_max_sphere_codebook_is_symmetric(self) -> None:
        boundaries, centroids = lloyd_max_sphere_codebook(32, 3)
        self.assertEqual(len(centroids), 8)
        self.assertEqual(len(boundaries), 7)
        np.testing.assert_allclose(centroids, -centroids[::-1], atol=1e-5)
        self.assertTrue(np.all(np.diff(centroids) > 0.0))

    def test_recall_and_percentile_helpers(self) -> None:
        self.assertEqual(recall_at_k([[1, 2]], [[2, 3]], 2), 0.5)
        self.assertEqual(percentile([4.0, 1.0, 3.0, 2.0], 0.95), 4.0)
        self.assertAlmostEqual(sample_stddev([1.0, 2.0, 3.0, 4.0]), 1.2909944487)

    def test_metric_specific_approximate_distance_preserves_inner_product(self) -> None:
        dots = np.asarray([0.5, -0.25], dtype=np.float32)
        norms = np.asarray([2.0, 4.0], dtype=np.float32)
        np.testing.assert_allclose(
            approximate_distances("inner-product", 3.0, norms, dots),
            np.asarray([-3.0, 3.0], dtype=np.float32),
        )
        np.testing.assert_allclose(
            approximate_distances("cosine", 1.0, np.ones(2), dots),
            1.0 - dots,
        )


if __name__ == "__main__":
    unittest.main()
