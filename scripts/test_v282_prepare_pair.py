import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from scripts import v282_prepare_pair
from scripts.v282_prepare_pair import topk_ids


class ExactTruthTest(unittest.TestCase):
    def test_boundary_ties_use_stable_source_id(self):
        self.assertEqual(topk_ids(np.array([0.5, 0.8, 0.8, 0.8, 0.9]), 3).tolist(),
                         [4, 1, 2])

    def test_random_stable_ids_are_recorded_before_ordinal_reindex(self):
        with TemporaryDirectory() as directory, patch.object(v282_prepare_pair, "ROWS", 3), \
                patch.object(v282_prepare_pair, "DIMS", 2):
            path = Path(directory) / "source.parquet"
            vectors = np.array([[3., 4.], [0., 2.], [1., 0.]], dtype=np.float32)
            data = pa.FixedSizeListArray.from_arrays(pa.array(vectors.ravel()), 2)
            pq.write_table(pa.table({"feature_row_id": pa.array([90, 20, 70], type=pa.uint64()),
                                     "embedding": data}), path)
            normalized, ids = v282_prepare_pair.vectors_from_input(path, "parquet")
            self.assertEqual(ids.tolist(), [90, 20, 70])
            np.testing.assert_allclose(normalized[0], [0.6, 0.8], atol=1e-6)
            pq.write_table(pa.table({"feature_row_id": pa.array([90, 20, 90], type=pa.uint64()),
                                     "embedding": data}), path)
            with self.assertRaisesRegex(ValueError, "IDs differ"):
                v282_prepare_pair.vectors_from_input(path, "parquet")


if __name__ == "__main__":
    unittest.main()
