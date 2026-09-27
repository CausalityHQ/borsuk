"""The semantic-cell probe packs distinct source neighborhoods into SQ8 pages."""

import hashlib
import json
import tempfile
import unittest
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from scripts.v283_semantic_cell_layout import build_semantic_cell_layout


class SemanticCellLayoutTests(unittest.TestCase):
    def test_source_only_pages_preserve_ids_and_locality(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            rng = np.random.default_rng(283)
            vectors = rng.normal(scale=0.02, size=(512, 64)).astype(np.float32)
            vectors[:256, 0] += 1
            vectors[256:, 0] -= 1
            vectors /= np.linalg.norm(vectors, axis=1, keepdims=True)
            source = root / "source.parquet"
            pq.write_table(pa.table({
                "feature_row_id": pa.array(np.arange(512, dtype=np.uint64)),
                "embedding": pa.FixedSizeListArray.from_arrays(
                    pa.array(vectors.ravel()), 64,
                ),
            }), source)
            source_sha = hashlib.sha256(source.read_bytes()).hexdigest()
            provenance = root / "provenance.json"
            provenance.write_text(json.dumps({
                "schema": "borsuk-source-shards-v1", "rows": 512,
                "dimensions": 64, "metric": "cosine", "query_or_truth_used": False,
                "source_sha256": source_sha, "source_bytes": source.stat().st_size,
            }))
            result = build_semantic_cell_layout(
                source, provenance, root / "out",
                expected_source_sha256=source_sha,
                expected_provenance_sha256=hashlib.sha256(
                    provenance.read_bytes()).hexdigest(),
            )
            order = np.load(root / "out/layout.npy", allow_pickle=False)
            np.testing.assert_array_equal(np.sort(order), np.arange(512))
            self.assertEqual(result["cells"], 2)
            self.assertEqual(result["query_or_truth_used"], False)
            self.assertTrue((order[:256] < 256).all() or (order[:256] >= 256).all())
            self.assertEqual((root / "out/sq8.bin").stat().st_size, 512 * 76)


if __name__ == "__main__":
    unittest.main()
