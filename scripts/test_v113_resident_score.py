"""Small exact-score checks for the V113 SQ8-targeted resident plane."""

import unittest

import numpy as np

from scripts.v113_resident_score import (
    encode_corrections, encode_corrections_from_codes,
    fit_residual_pq, score_encoded_nominees,
    score_nominees,
)


class ResidentScoreTests(unittest.TestCase):
    def test_exact_residual_reconstruction_recovers_sq8_scores(self) -> None:
        pq = np.array([[1.0, 0.0], [0.0, 0.0], [1.0, 1.0]], dtype=np.float32)
        sq8 = np.array([[2.0, 1.0], [0.0, 2.0], [2.0, 0.0]], dtype=np.float32)
        norm = np.array([5.0, 4.0, 4.0], dtype=np.float32)
        query = np.array([0.25, 1.5], dtype=np.float32)
        corrections = encode_corrections(pq, sq8, norm)
        result = score_nominees(query, pq, corrections, corrections.residual)
        exact = norm - 2 * (sq8 @ query) + query @ query
        np.testing.assert_allclose(result, exact, atol=0.001, rtol=0)
        self.assertEqual(corrections.nu_codes.dtype, np.int16)
        self.assertEqual(corrections.alpha_codes.dtype, np.int16)
        self.assertTrue(np.isfinite(corrections.residual).all())
        p64 = pq.astype(np.float64)
        target = sq8.astype(np.float64)
        alpha_hat = corrections.alpha_codes.astype(np.float64) * corrections.alpha_scale
        exact_residual = target - p64 - alpha_hat[:, None] * p64
        cast_error = np.linalg.norm(
            exact_residual - corrections.residual.astype(np.float64), axis=1,
        )
        self.assertGreaterEqual(
            corrections.residual_cast_error_max, float(cast_error.max()),
        )

    def test_invalid_nonfinite_input_is_rejected(self) -> None:
        pq = np.zeros((1, 2), dtype=np.float32)
        sq8 = np.array([[np.nan, 0.0]], dtype=np.float32)
        norm = np.ones(1, dtype=np.float32)
        with self.assertRaises(ValueError):
            encode_corrections(pq, sq8, norm)
        with self.assertRaises(ValueError):
            encode_corrections(
                np.empty((1, 0), dtype=np.float32),
                np.empty((1, 0), dtype=np.float32), norm,
            )

    def test_residual_pq_records_full_corpus_error_bound(self) -> None:
        rng = np.random.default_rng(113)
        residual = rng.normal(size=(256, 12)).astype(np.float32)
        books, codes, decoded, maximum_error = fit_residual_pq(
            residual, seed=113, sample_rows=256, iterations=1,
        )
        self.assertEqual(books.shape, (12, 256, 1))
        self.assertEqual(codes.shape, (256, 12))
        observed = np.abs(residual.astype(np.float64) - decoded)
        for subspace in range(12):
            np.testing.assert_array_less(
                observed[:, subspace],
                maximum_error[subspace, codes[:, subspace]] + 1e-12,
            )

    def test_residual_pq_accepts_dimension_not_divisible_by_twelve(self) -> None:
        rng = np.random.default_rng(114)
        residual = rng.normal(size=(256, 13)).astype(np.float32)
        books, codes, decoded, maximum_error = fit_residual_pq(
            residual, seed=114, sample_rows=256, iterations=1,
        )
        self.assertEqual(books.shape, (12, 256, 2))
        self.assertEqual(codes.shape, (256, 12))
        self.assertEqual(decoded.shape, residual.shape)
        self.assertEqual(maximum_error.shape, (12, 256))
        self.assertTrue(np.all(np.max(np.abs(books), axis=(1, 2)) > 0))

    def test_correction_builder_binds_pq_and_sq8_codes(self) -> None:
        books = np.zeros((64, 256, 1), dtype=np.float32)
        books[:, 0, 0] = 1.0
        pq_codes = np.zeros((2, 64), dtype=np.uint8)
        sq8_codes = np.ones((2, 64), dtype=np.uint8)
        sq8_codes[0, 0] = 2
        low = np.zeros(64, dtype=np.float32)
        step = np.ones(64, dtype=np.float32)
        norm = np.array([67.0, 64.0], dtype=np.float32)
        result = encode_corrections_from_codes(
            books, pq_codes, sq8_codes, norm, low, step,
        )
        expected = encode_corrections(
            np.ones((2, 64), dtype=np.float32),
            sq8_codes.astype(np.float64), norm,
        )
        np.testing.assert_array_equal(result.nu_codes, expected.nu_codes)
        np.testing.assert_array_equal(result.alpha_codes, expected.alpha_codes)
        np.testing.assert_array_equal(result.residual, expected.residual)

    def test_alpha_outlier_does_not_zero_normal_correction(self) -> None:
        pq = np.array([[1e-6, 0.0], [1.0, 0.0]], dtype=np.float32)
        sq8 = np.array([[1.0, 0.0], [1.5, 0.0]], dtype=np.float32)
        norm = np.array([1.0, 2.25], dtype=np.float32)
        corrections = encode_corrections(pq, sq8, norm)
        self.assertLessEqual(corrections.alpha_scale * 32767, 1.0)
        self.assertGreater(int(corrections.alpha_codes[1]), 0)

    def test_encoded_scorer_uses_only_row_codes_and_codebooks(self) -> None:
        rng = np.random.default_rng(115)
        pq_books = np.zeros((64, 256, 2), dtype=np.float32)
        for subspace in range(64):
            active = (subspace + 1) * 65 // 64 - subspace * 65 // 64
            pq_books[subspace, 0, :active] = 1.0
        pq_codes = np.zeros((256, 64), dtype=np.uint8)
        pq = np.ones((256, 65), dtype=np.float32)
        sq8 = pq + rng.normal(scale=0.1, size=pq.shape).astype(np.float32)
        norm = np.sum(sq8 * sq8, axis=1).astype(np.float32)
        corrections = encode_corrections(pq, sq8, norm)
        books, codes, decoded, maximum_error = fit_residual_pq(
            corrections.residual, seed=115, sample_rows=256, iterations=1,
        )
        query = rng.normal(size=65).astype(np.float32)
        selected = np.array([0, 2, 7], dtype=np.int64)
        encoded = score_encoded_nominees(
            query, pq_books, pq_codes[selected], books, codes[selected],
            corrections.nu_scale, corrections.nu_codes[selected],
            corrections.alpha_scale, corrections.alpha_codes[selected],
        )
        from_codes = score_nominees(
            query, pq[selected],
            type(corrections)(
                corrections.nu_scale, corrections.alpha_scale,
                corrections.nu_codes[selected], corrections.alpha_codes[selected],
                corrections.residual[selected],
            ),
            decoded[selected],
        )
        np.testing.assert_allclose(encoded, from_codes, atol=1e-9, rtol=0)
        q64 = query.astype(np.float64)
        true = (
            norm[selected].astype(np.float64)
            - 2.0 * (sq8[selected].astype(np.float64) @ q64)
            + q64 @ q64
        )
        block_bounds = np.stack([
            maximum_error[subspace, codes[selected, subspace]]
            for subspace in range(12)
        ], axis=1)
        bound = (
            corrections.nu_scale / 2.0
            + 2.0 * np.linalg.norm(q64) * (
                np.linalg.norm(block_bounds, axis=1)
                + corrections.residual_cast_error_max
            )
        )
        self.assertTrue(np.all(np.abs(encoded - true) <= bound + 1e-9))

    def test_encoded_scorer_rejects_malformed_codes(self) -> None:
        query = np.ones(64, dtype=np.float32)
        pq_books = np.zeros((64, 256, 1), dtype=np.float32)
        residual_books = np.zeros((12, 256, 6), dtype=np.float32)
        with self.assertRaises(ValueError):
            score_encoded_nominees(
                query, pq_books, [[0] * 64], residual_books,
                np.zeros((1, 12), dtype=np.uint8), 1.0,
                np.zeros(1, dtype=np.int16), 1.0 / 32767,
                np.zeros(1, dtype=np.int16),
            )


if __name__ == "__main__":
    unittest.main()
