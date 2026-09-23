"""Exact record and score behavior for the 96-byte rotated sign candidate."""

from __future__ import annotations

import unittest

import numpy as np

from scripts.native_rotated_sign96 import (
    BATCH_ROWS,
    KEPT_COORDINATES,
    MAX_ENCODE_ROWS,
    RECORD_BYTES,
    encode_records,
    query_byte_table,
    score_records,
)
from scripts.native_rotated_two_bit_codes import rotate_rows


class RotatedSign96Tests(unittest.TestCase):
    def test_byte_table_matches_independent_sign_dot(self) -> None:
        retained_query = np.arange(752, dtype=np.float64) / 1000
        table = query_byte_table(retained_query)
        self.assertEqual(table.shape, (94, 256))
        for byte_index in (0, 47, 93):
            block = retained_query[8 * byte_index : 8 * byte_index + 8]
            for symbol in (0, 1, 0b10100110, 255):
                bits = np.asarray([(symbol >> bit) & 1 for bit in range(8)])
                expected = np.sum((2 * bits - 1) * block, dtype=np.float64)
                self.assertAlmostEqual(table[byte_index, symbol], expected)

    def test_exact_record_width_and_bit_order(self) -> None:
        mean = np.zeros(768, dtype=np.float32)
        rows = np.zeros((2, 768), dtype=np.float32)
        rows[1, :12] = np.arange(1, 13, dtype=np.float32)
        records = encode_records(rows, mean, rotation_seed=20260923)
        self.assertEqual(RECORD_BYTES, 96)
        self.assertEqual(records.shape, (2, 96))
        self.assertEqual(records.dtype, np.uint8)
        self.assertEqual(len(KEPT_COORDINATES), 752)
        self.assertTrue(np.all(KEPT_COORDINATES % 48 != 47))
        self.assertEqual(records[0, :94].tolist(), [255] * 94)
        self.assertEqual(records[0, 94:].tobytes(), np.asarray([0], dtype="<f2").tobytes())
        rotated = rotate_rows(rows[1:].astype(np.float64), rotation_seed=20260923)[0]
        expected = np.packbits(rotated[KEPT_COORDINATES] >= 0, bitorder="little")
        np.testing.assert_array_equal(records[1, :94], expected)
        expected_scale = np.float16(np.mean(np.abs(rotated[KEPT_COORDINATES]), dtype=np.float64))
        self.assertEqual(records[1, 94:].tobytes(), expected_scale.tobytes())

    def test_score_uses_stored_scale_and_bounded_batches(self) -> None:
        rng = np.random.default_rng(106)
        mean = rng.standard_normal(768).astype(np.float32) / 50
        rows = rng.standard_normal((BATCH_ROWS + 1, 768)).astype(np.float32) / 20
        query = rng.standard_normal(768).astype(np.float32) / 20
        records = encode_records(rows, mean, rotation_seed=20260923)
        scores = score_records(query, mean, records, rotation_seed=20260923)
        self.assertEqual(scores.shape, (BATCH_ROWS + 1,))
        rotated_q = rotate_rows(
            (query.astype(np.float64) - mean.astype(np.float64))[None, :],
            rotation_seed=20260923,
        )[0][KEPT_COORDINATES]
        centered_q = query.astype(np.float64) - mean.astype(np.float64)
        query_norm = np.sum(centered_q * centered_q, dtype=np.float64)
        for index in (0, BATCH_ROWS - 1, BATCH_ROWS):
            signs = np.unpackbits(records[index, :94], bitorder="little").astype(np.float64) * 2 - 1
            scale = float(np.frombuffer(records[index, 94:].tobytes(), dtype="<f2")[0])
            expected = np.float32(query_norm + 752 * scale * scale - 2 * scale * np.sum(signs * rotated_q))
            self.assertEqual(scores[index], expected)
            reconstructed = np.zeros(768, dtype=np.float64)
            reconstructed[KEPT_COORDINATES] = scale * signs
            rotated_full = rotate_rows(centered_q[None, :], rotation_seed=20260923)[0]
            direct_distance = np.sum((rotated_full - reconstructed) ** 2, dtype=np.float64)
            np.testing.assert_allclose(float(scores[index]), direct_distance, rtol=1e-6, atol=1e-6)

    def test_subnormal_stored_scale_remains_finite(self) -> None:
        mean = np.zeros(768, dtype=np.float32)
        rows = np.full((1, 768), 1e-6, dtype=np.float32)
        records = encode_records(rows, mean, rotation_seed=20260923)
        scale = np.frombuffer(records[0, 94:].tobytes(), dtype="<f2")[0]
        self.assertGreater(scale, 0)
        self.assertLess(scale, np.finfo(np.float16).tiny)
        score = score_records(rows[0], mean, records, rotation_seed=20260923)
        self.assertTrue(np.isfinite(score).all())

    def test_rejects_invalid_source_query_and_scale(self) -> None:
        mean = np.zeros(768, dtype=np.float32)
        rows = np.zeros((1, 768), dtype=np.float32)
        with self.assertRaises(ValueError):
            encode_records(rows.astype(np.float64), mean, rotation_seed=20260923)
        rows[0, 0] = np.nan
        with self.assertRaises(ValueError):
            encode_records(rows, mean, rotation_seed=20260923)
        rows[0, 0] = 0
        records = encode_records(rows, mean, rotation_seed=20260923)
        records[0, 94:] = np.frombuffer(np.float16(np.nan).tobytes(), dtype=np.uint8)
        with self.assertRaises(ValueError):
            score_records(rows[0], mean, records, rotation_seed=20260923)
        records[0, 94:] = np.frombuffer(np.float16(-1).tobytes(), dtype=np.uint8)
        with self.assertRaises(ValueError):
            score_records(rows[0], mean, records, rotation_seed=20260923)
        with self.assertRaises(ValueError):
            score_records(rows[0].astype(np.float64), mean, records, rotation_seed=20260923)
        large = np.full((1, 768), np.finfo(np.float32).max, dtype=np.float32)
        with self.assertRaisesRegex(ValueError, "cannot be stored"):
            encode_records(large, mean, rotation_seed=20260923)
        too_many = np.zeros((MAX_ENCODE_ROWS + 1, 768), dtype=np.float32)
        with self.assertRaisesRegex(ValueError, "source differs"):
            encode_records(too_many, mean, rotation_seed=20260923)


if __name__ == "__main__":
    unittest.main()
