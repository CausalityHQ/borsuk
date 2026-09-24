"""Checks for source-tier score order and certified refinement."""

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from scripts.v124_source_tier_precision import (
    certified_refinement, decode_int16, load_truth, mapped_nominees, rank, unit,
)


class SourceTierPrecisionTests(unittest.TestCase):
    def test_equal_byte_int16_decode_preserves_unit_direction(self) -> None:
        vectors = np.array([[0.6, 0.8], [-0.8, 0.6]], dtype=np.float64)
        decoded = decode_int16(vectors)
        self.assertEqual(decoded.shape, vectors.shape)
        self.assertLess(np.linalg.norm(unit(decoded) - vectors), 1e-4)

    def test_sound_intervals_refine_exact_top_100(self) -> None:
        ids = np.arange(512, dtype=np.int64)
        exact = np.linspace(1.0, 0.0, 512)
        approx = exact.copy()
        approx[99], approx[100] = approx[100], approx[99]
        delta = np.abs(exact - approx) + 1e-12
        count, refined, _ = certified_refinement(ids, exact, approx, delta)
        np.testing.assert_array_equal(refined, rank(ids, exact))
        self.assertGreater(count, 100)
        with self.assertRaises(ValueError):
            certified_refinement(ids, exact, approx, delta * 0)

    def test_physical_layout_maps_through_nonmonotone_source_ids(self) -> None:
        layout = np.array([2, 0, 1], dtype=np.int32)
        source_ids = np.array([1, 2, 0], dtype=np.int64)
        ordinals, ids = mapped_nominees(layout, source_ids, [0, 1])
        np.testing.assert_array_equal(ordinals, [2, 0])
        np.testing.assert_array_equal(ids, [0, 1])

    def test_flat_gt_preserves_large_original_ids(self) -> None:
        identifiers = np.arange(144_674_259, 144_674_359, dtype=np.int64)
        flat = np.tile(identifiers, 1_000)
        with TemporaryDirectory() as temporary:
            path = Path(temporary) / "truth.parquet"
            pq.write_table(pa.table({"feature_row_id": flat}), path)
            truth = load_truth(path, identifiers)
        self.assertEqual(len(truth), 1_000)
        self.assertEqual(truth[0], identifiers.tolist())


if __name__ == "__main__":
    unittest.main()
