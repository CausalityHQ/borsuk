"""Source-only bindings for the V114 existing SQ8 object."""

import hashlib
import tempfile
import unittest
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from scripts.v114_1m_mirror import seal_existing_sq8


class ExistingSq8MirrorTests(unittest.TestCase):
    def test_seals_existing_object_from_source_without_query_input(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source.parquet"
            sq8 = root / "sq8.bin"
            vectors = np.zeros((300, 4), dtype=np.float32)
            vectors[:, 0] = np.arange(300, dtype=np.float32)[::-1]
            vectors[:, 1:] = np.array([1.0, 2.0, 3.0], dtype=np.float32)
            pq.write_table(pa.table({
                "feature_row_id": pa.array(np.arange(300, dtype=np.int64)[::-1]),
                "embedding": pa.FixedSizeListArray.from_arrays(
                    pa.array(vectors.ravel()), 4,
                ),
            }), source)
            data = b"".join(
                i.to_bytes(8, "little", signed=True)
                + np.float32(0).tobytes() + bytes(4)
                for i in range(299, -1, -1)
            )
            sq8.write_bytes(data)
            digest = lambda path: hashlib.sha256(path.read_bytes()).hexdigest()
            mirror = root / "mirror"
            manifest = seal_existing_sq8(
                source, sq8, mirror,
                expected_source_sha256=digest(source),
                expected_sq8_sha256=digest(sq8),
                rows=300, dimensions=4, max_nominees=128,
            )
            self.assertEqual(manifest["geometry"], {"rows": 300, "dimensions": 4})
            self.assertEqual(manifest["low"], [0.0, 1.0, 2.0, 3.0])
            self.assertEqual(manifest["step"][0], float(np.float32(299 / 255)))
            self.assertEqual(manifest["object_sha256"], digest(sq8))
            self.assertEqual(
                (mirror / "blocks.sha256").read_bytes(),
                hashlib.sha256(data[:4096]).digest()
                + hashlib.sha256(data[4096:]).digest(),
            )
            self.assertFalse((mirror / "sq8.bin").exists())
            with self.assertRaises(ValueError):
                seal_existing_sq8(
                    source, sq8, root / "wrong-source",
                    expected_source_sha256="0" * 64,
                    expected_sq8_sha256=digest(sq8),
                    rows=300, dimensions=4, max_nominees=128,
                )
            with self.assertRaises(ValueError):
                seal_existing_sq8(
                    source, sq8, root / "wrong-sq8",
                    expected_source_sha256=digest(source),
                    expected_sq8_sha256="0" * 64,
                    rows=300, dimensions=4, max_nominees=128,
                )


if __name__ == "__main__":
    unittest.main()
