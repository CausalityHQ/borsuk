"""Matrix-scored two-bit batches must agree with the scalar authority."""

from __future__ import annotations

import unittest

import numpy as np

from scripts.native_progressive_score import coded_distance_batch
from scripts.native_rotated_two_bit_codes import _fit_records
from scripts.native_rotated_two_bit_evaluation import score_records


class ProgressiveScoreTest(unittest.TestCase):
    def test_batched_scores_match_existing_two_bit_scorer(self) -> None:
        rng = np.random.default_rng(51)
        vectors = rng.normal(size=(8, 768)).astype(np.float32)
        queries = rng.normal(size=(3, 768)).astype(np.float32)
        mean = np.mean(vectors, axis=0, dtype=np.float64).astype(np.float32)
        records = _fit_records(vectors.astype(np.float64) - mean, rotation_seed=7)
        batched = coded_distance_batch(queries, mean, records, rotation_seed=7)
        self.assertEqual(batched.shape, (3, 8))
        for index, query in enumerate(queries):
            expected, _ = score_records(query, mean, records, rotation_seed=7)
            np.testing.assert_allclose(batched[index], expected, rtol=1e-5, atol=1e-4)

    def test_rejects_nonfinite_query(self) -> None:
        queries = np.zeros((1, 768), dtype=np.float32)
        queries[0, 0] = np.nan
        with self.assertRaises(ValueError):
            coded_distance_batch(
                queries, np.zeros(768, dtype=np.float32),
                np.zeros((1, 200), dtype=np.uint8), rotation_seed=7,
            )


if __name__ == "__main__":
    unittest.main()
