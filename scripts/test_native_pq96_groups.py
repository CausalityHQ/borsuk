"""PQ96 physical records and external seal authentication."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import numpy as np

from scripts.native_pq96_groups import (
    MODEL_HEADER,
    MODEL_MAGIC,
    open_pq96_groups,
    score_pq96,
    source_rows_digest,
    write_pq96_groups,
)


class Pq96GroupsTest(unittest.TestCase):
    def test_model_framing_physical_order_and_tamper(self) -> None:
        rng = np.random.default_rng(7)
        vectors = rng.normal(size=(20, 768)).astype(np.float32)
        books = rng.normal(size=(96, 256, 8)).astype(np.float32)
        ordinals = rng.permutation(20).astype(np.int64)
        digest = source_rows_digest(vectors)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            written = write_pq96_groups(
                root, books, (5, 5, 5, 5), [(ordinals, vectors)],
                source_sha256="a" * 64, layout_sha256="b" * 64,
                physical_order_sha256="c" * 64, source_rows_sha256=digest,
            )
            self.assertEqual(
                MODEL_HEADER.unpack_from((root / "model.bin").read_bytes()),
                (MODEL_MAGIC, 768, 96, 256, 8),
            )
            reader = open_pq96_groups(
                root, source_sha256="a" * 64, layout_sha256="b" * 64,
                physical_order_sha256="c" * 64,
                seal_sha256=written.seal_sha256,
            )
            records = reader.group_records(0)
            self.assertEqual(records.shape, (20, 96))
            scores = score_pq96(vectors[0], reader, records)
            self.assertTrue(np.isfinite(scores).all())
            decoded = reader.books[np.arange(96)[None, :], records].reshape(20, 768)
            reconstructed = np.sum(
                (vectors[0].astype(np.float64) - decoded.astype(np.float64)) ** 2,
                axis=1,
            )
            np.testing.assert_allclose(scores, reconstructed, rtol=1e-5, atol=1e-4)
            with self.assertRaisesRegex(ValueError, "seal authority"):
                open_pq96_groups(
                    root, source_sha256="a" * 64, layout_sha256="b" * 64,
                    physical_order_sha256="c" * 64, seal_sha256="d" * 64,
                )
            body = bytearray((root / "groups.bin").read_bytes())
            body[-1] ^= 1
            (root / "groups.bin").write_bytes(body)
            with self.assertRaisesRegex(ValueError, "group digest"):
                open_pq96_groups(
                    root, source_sha256="a" * 64, layout_sha256="b" * 64,
                    physical_order_sha256="c" * 64,
                    seal_sha256=written.seal_sha256,
                )


if __name__ == "__main__":
    unittest.main()
