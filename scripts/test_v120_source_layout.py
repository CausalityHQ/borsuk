"""Source-only layout and SQ8 body construction checks."""

import hashlib
import json
import tempfile
import unittest
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from scripts.v120_source_layout import build_source_layout, cluster_count


class SourceLayoutTests(unittest.TestCase):
    def test_cluster_rule_is_smooth_and_corpus_blind(self) -> None:
        self.assertEqual(cluster_count(1_000_000), 8192)
        self.assertLess(cluster_count(1_000_000), cluster_count(9_990_000))
        self.assertLess(cluster_count(9_990_000), 20_000)

    def test_build_seals_source_identity_and_sq8_physical_rows(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            rng = np.random.default_rng(120)
            vectors = rng.normal(size=(96, 96)).astype(np.float32)
            vectors /= np.linalg.norm(vectors, axis=1, keepdims=True)
            source = root / "source.parquet"
            pq.write_table(pa.table({
                "feature_row_id": pa.array(np.arange(96, dtype=np.uint64)),
                "embedding": pa.FixedSizeListArray.from_arrays(
                    pa.array(vectors.ravel()), 96,
                ),
            }), source)
            source_hash = hashlib.sha256(source.read_bytes()).hexdigest()
            provenance = root / "source.json"
            provenance.write_text(json.dumps({
                "schema": "borsuk-source-shards-v1", "rows": 96,
                "dimensions": 96, "metric": "cosine",
                "query_or_truth_used": False,
                "source_sha256": source_hash,
                "source_bytes": source.stat().st_size,
            }))
            provenance_hash = hashlib.sha256(provenance.read_bytes()).hexdigest()
            output = root / "built"
            manifest = build_source_layout(
                source, provenance, output,
                expected_source_sha256=source_hash,
                expected_provenance_sha256=provenance_hash,
            )
            order = np.load(output / "layout.npy", allow_pickle=False)
            np.testing.assert_array_equal(np.sort(order), np.arange(96))
            body = np.fromfile(output / "sq8.bin", dtype=np.dtype([
                ("id", "<i8"), ("norm", "<f4"), ("code", "u1", (96,)),
            ]))
            np.testing.assert_array_equal(body["id"], order)
            self.assertEqual(body.shape, (96,))
            self.assertEqual(manifest["source_sha256"], source_hash)
            self.assertEqual(manifest["sq8_sha256"], hashlib.sha256(
                (output / "sq8.bin").read_bytes()).hexdigest())
            self.assertEqual(manifest["query_or_truth_used"], False)

            with self.assertRaisesRegex(ValueError, "source"):
                build_source_layout(
                    source, provenance, root / "wrong-source",
                    expected_source_sha256="0" * 64,
                    expected_provenance_sha256=provenance_hash,
                )
            mutated = json.loads(provenance.read_text())
            mutated["query_or_truth_used"] = True
            provenance.write_text(json.dumps(mutated))
            with self.assertRaisesRegex(ValueError, "query|truth"):
                build_source_layout(
                    source, provenance, root / "tainted",
                    expected_source_sha256=source_hash,
                    expected_provenance_sha256=hashlib.sha256(
                        provenance.read_bytes()).hexdigest(),
                )


if __name__ == "__main__":
    unittest.main()
