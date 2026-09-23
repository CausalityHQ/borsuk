"""Source-only router artifact format tests."""

import tempfile
import unittest
from pathlib import Path
import hashlib

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from scripts.v115_source_router import (
    build_source_router, load_source_router, write_source_router,
)


class SourceRouterTests(unittest.TestCase):
    def test_builds_a_small_router_from_corpus_and_layout_only(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source.parquet"
            layout = root / "layout.npy"
            vectors = np.arange(160, dtype=np.float32).reshape(40, 4)
            pq.write_table(pa.table({
                "feature_row_id": np.arange(40, dtype=np.int64),
                "embedding": pa.FixedSizeListArray.from_arrays(
                    pa.array(vectors.ravel()), 4,
                ),
            }), source)
            np.save(layout, np.arange(39, -1, -1, dtype=np.int32))
            sha = lambda path: hashlib.sha256(path.read_bytes()).hexdigest()
            build_source_router(
                source, layout, root / "router",
                expected_source_sha256=sha(source),
                expected_layout_sha256=sha(layout),
                sq8_sha256="c" * 64, rows=40, dimensions=4,
                page_rows=16, blocks_per_page=2, generation=1,
            )
            manifest, arrays = load_source_router(root / "router")
            self.assertEqual(manifest["source_sha256"], sha(source))
            self.assertEqual(arrays["summaries"].shape, (6, 4))
            self.assertEqual(arrays["books"].shape, (64, 256, 1))
            self.assertEqual(arrays["codes"].shape, (40, 64))
            np.testing.assert_array_equal(arrays["low"], vectors.min(axis=0))

    def test_seals_geometry_sections_without_queries_or_truth(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "router"
            arrays = {
                "summaries": np.zeros((4, 64), np.float32),
                "books": np.zeros((64, 256, 1), np.float32),
                "codes": np.zeros((512, 64), np.uint8),
                "low": np.zeros(64, np.float32),
                "step": np.ones(64, np.float32),
            }
            write_source_router(
                root, rows=512, dimensions=64, page_rows=256,
                blocks_per_page=2, source_sha256="a" * 64,
                layout_sha256="b" * 64, sq8_sha256="c" * 64,
                generation=1, **arrays,
            )
            manifest, loaded = load_source_router(root)
            self.assertEqual(manifest["geometry"]["rows"], 512)
            self.assertEqual(set(loaded), set(arrays))
            for name, value in arrays.items():
                np.testing.assert_array_equal(loaded[name], value)
            self.assertFalse((root / "queries.bin").exists())
            self.assertFalse((root / "truth.bin").exists())
            with (root / "codes.bin").open("r+b") as handle:
                handle.write(b"x")
            with self.assertRaisesRegex(ValueError, "codes"):
                load_source_router(root)


if __name__ == "__main__":
    unittest.main()
