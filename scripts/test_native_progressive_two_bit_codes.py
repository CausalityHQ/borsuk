"""Exact byte-level split of the existing rotated two-bit records."""

from __future__ import annotations

import unittest

import numpy as np

from scripts.native_progressive_two_bit_codes import join_planes, split_records


class ProgressiveTwoBitCodesTest(unittest.TestCase):
    def test_every_symbol_and_metadata_round_trip(self) -> None:
        records = np.zeros((256, 200), dtype=np.uint8)
        records[:, :192] = np.arange(256, dtype=np.uint8)[:, None]
        records[:, 192:] = np.arange(8, dtype=np.uint8)[None, :]
        sign, magnitude = split_records(records)
        self.assertEqual(sign.shape, (256, 104))
        self.assertEqual(magnitude.shape, (256, 96))
        np.testing.assert_array_equal(sign[:, 96:], records[:, 192:])
        np.testing.assert_array_equal(join_planes(sign, magnitude), records)

    def test_independent_random_rows_round_trip(self) -> None:
        records = np.random.default_rng(20260923).integers(
            0, 256, size=(17, 200), dtype=np.uint8
        )
        sign, magnitude = split_records(records)
        np.testing.assert_array_equal(join_planes(sign, magnitude), records)

    def test_rejects_mismatched_planes(self) -> None:
        with self.assertRaises(ValueError):
            join_planes(np.zeros((2, 104), dtype=np.uint8), np.zeros((1, 96), dtype=np.uint8))


if __name__ == "__main__":
    unittest.main()
