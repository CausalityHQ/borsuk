"""Held-out D96 subset screen inputs and exact local GT."""

import hashlib
import tempfile
import unittest
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from scripts.v122_subset_screen import (
    covered_truth_count, exact_subset_truth, make_subset,
)


class SubsetScreenTests(unittest.TestCase):
    def test_subset_is_seeded_source_only_and_remaps_ids(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            vectors = np.eye(96, dtype=np.float32)
            source = root / "source.parquet"
            pq.write_table(pa.table({
                "feature_row_id": pa.array(np.arange(96, dtype=np.uint64)),
                "embedding": pa.FixedSizeListArray.from_arrays(
                    pa.array(vectors.ravel()), 96,
                ),
            }), source)
            identity = hashlib.sha256(source.read_bytes()).hexdigest()
            manifest = make_subset(source, root / "subset", source_sha256=identity,
                                   sample_rows=32, seed=122001)
            original = np.load(root / "subset/original_ids.npy", allow_pickle=False)
            self.assertEqual(original.shape, (32,))
            self.assertTrue(np.all(original[1:] > original[:-1]))
            table = pq.read_table(root / "subset/source.parquet")
            self.assertEqual(table["feature_row_id"].to_pylist(), list(range(32)))
            self.assertEqual(manifest["source_rows"], 96)
            self.assertEqual(manifest["subset_rows"], 32)
            self.assertFalse(manifest["query_or_truth_used"])

    def test_exact_truth_uses_score_then_id_ties(self) -> None:
        corpus = np.array([[1, 0], [0, 1], [1, 0]], dtype=np.float32)
        queries = np.array([[1, 0]], dtype=np.float32)
        np.testing.assert_array_equal(exact_subset_truth(corpus, queries, 2),
                                      np.array([[0, 2]], dtype=np.int64))

    def test_physical_coverage_uses_layout_positions_not_source_ids(self) -> None:
        source_to_physical = np.array([1, 2, 0], dtype=np.int64)
        self.assertEqual(covered_truth_count(
            np.array([0, 1, 2], dtype=np.int64), source_to_physical,
            [[0, 2 * 108]], row_bytes=108,
        ), 2)


if __name__ == "__main__":
    unittest.main()
