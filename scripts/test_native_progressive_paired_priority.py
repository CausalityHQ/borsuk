"""Paired page priorities share precisely one mirrored code cover."""

from __future__ import annotations

import unittest

import numpy as np

from scripts.native_progressive_paired_priority import paired_priorities_from_batches
from scripts.native_rotated_two_bit_codes import _fit_records


class ProgressivePairedPriorityTest(unittest.TestCase):
    def test_both_arms_are_limited_to_same_cover(self) -> None:
        rng = np.random.default_rng(33)
        vectors = rng.normal(size=(4, 768)).astype(np.float32)
        mean = np.mean(vectors, axis=0, dtype=np.float64).astype(np.float32)
        records = _fit_records(vectors.astype(np.float64) - mean, rotation_seed=7)
        queries = vectors[:2].copy()
        pages = np.asarray([0, 1, 2, 2], dtype=np.int64)

        result = paired_priorities_from_batches(
            queries, mean, records,
            [(np.arange(4, dtype=np.int64), pages, vectors)],
            [(0, 2), (1, 2)], page_count=3, expected_rows=4,
            rotation_seed=7,
        )
        self.assertEqual(set(result[0]["source"]), {0, 2})
        self.assertEqual(set(result[0]["two_bit"]), {0, 2})
        self.assertEqual(set(result[1]["source"]), {1, 2})
        self.assertEqual(set(result[1]["two_bit"]), {1, 2})

    def test_rejects_incomplete_source_stream(self) -> None:
        with self.assertRaisesRegex(ValueError, "incomplete"):
            paired_priorities_from_batches(
                np.zeros((1, 768), dtype=np.float32),
                np.zeros(768, dtype=np.float32),
                np.zeros((2, 200), dtype=np.uint8),
                [(np.asarray([0], dtype=np.int64),
                  np.asarray([0], dtype=np.int64),
                  np.zeros((1, 768), dtype=np.float32))],
                [(0,)], page_count=1, expected_rows=2, rotation_seed=7,
            )


if __name__ == "__main__":
    unittest.main()
