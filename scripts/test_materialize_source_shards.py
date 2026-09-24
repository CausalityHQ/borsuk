"""Authenticate and normalize a streamed corpus without query or GT access."""

import json
import tempfile
import unittest
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from scripts.materialize_source_shards import materialize, sha256_file


class MaterializeSourceShardsTest(unittest.TestCase):
    def test_order_normalization_and_bad_shard_rejection(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            rows = [[[3.0, 4.0], [0.0, 2.0]], [[5.0, 12.0]]]
            objects = []
            vector_type = pa.list_(pa.field("item", pa.float32(), nullable=False), 2)
            schema = pa.schema([pa.field("emb", vector_type, nullable=False)])
            for ordinal, vectors in enumerate(rows):
                name = f"train-{ordinal:08d}.parquet"
                flat = pa.array(np.asarray(vectors, np.float32).reshape(-1), type=pa.float32())
                table = pa.Table.from_arrays([
                    pa.FixedSizeListArray.from_arrays(flat, 2)], schema=schema)
                pq.write_table(table, root / name)
                objects.append({"role": "train", "uri": f"s3://fixture/{name}",
                                "rows": len(vectors), "bytes": (root / name).stat().st_size,
                                "sha256": sha256_file(root / name)})
            staging = root / "staging.json"
            staging.write_text(json.dumps({"dataset_id": "fixture", "objects": objects}))
            output = root / "source.parquet"
            provenance = root / "source.json"
            result = materialize(staging, root, output, provenance,
                                 dimensions=2, rows=3, metric="cosine", batch_rows=1)
            table = pq.read_table(output)
            self.assertEqual(table["feature_row_id"].to_pylist(), [0, 1, 2])
            actual = np.asarray(table["embedding"].combine_chunks().values.to_numpy())
            np.testing.assert_allclose(actual.reshape(3, 2),
                                       [[0.6, 0.8], [0.0, 1.0], [5 / 13, 12 / 13]],
                                       atol=1e-7)
            self.assertEqual(result["source_sha256"], sha256_file(output))
            self.assertFalse(result["query_or_truth_used"])
            (root / "train-00000001.parquet").write_bytes(b"changed")
            with self.assertRaises(ValueError):
                materialize(staging, root, root / "bad.parquet", root / "bad.json",
                            dimensions=2, rows=3, metric="cosine")

    def test_zero_cosine_vector_fails_before_publishing(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            path = root / "train-00000000.parquet"
            vector_type = pa.list_(pa.field("item", pa.float32(), nullable=False), 2)
            table = pa.Table.from_arrays([
                pa.FixedSizeListArray.from_arrays(pa.array([0.0, 0.0]), 2)],
                schema=pa.schema([pa.field("emb", vector_type, nullable=False)]))
            pq.write_table(table, path)
            staging = root / "staging.json"
            staging.write_text(json.dumps({"dataset_id": "fixture", "objects": [{
                "role": "train", "uri": "s3://fixture/train-00000000.parquet",
                "rows": 1, "bytes": path.stat().st_size,
                "sha256": sha256_file(path),
            }]}))
            output = root / "source.parquet"
            with self.assertRaises(ValueError):
                materialize(staging, root, output, root / "source.json",
                            dimensions=2, rows=1, metric="cosine")
            self.assertFalse(output.exists())


if __name__ == "__main__":
    unittest.main()
