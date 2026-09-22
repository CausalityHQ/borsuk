"""Sealed source-only construction for the row-score cell."""

from __future__ import annotations

import hashlib
import tempfile
import unittest
from pathlib import Path

import numpy as np

from scripts.native_geometric_layout_screen import LayoutMethod, MembershipRow
from scripts.native_row_score_cell import construct_cell, read_cell_seal


class RowScoreCellTests(unittest.TestCase):
    def test_construct_seals_codes_without_query_or_truth_inputs(self) -> None:
        ids = tuple(index.to_bytes(4, "little") for index in range(256))
        vectors = np.repeat(np.arange(256, dtype=np.float32)[:, None], 48, axis=1)
        source_sha = hashlib.sha256(b"source").digest()
        membership_sha = hashlib.sha256(b"membership").digest()
        membership = tuple(
            MembershipRow(
                stable_id=ids[index], source_ordinal=index,
                page_ordinal=index // 128, in_page_ordinal=index % 128,
                page_rows=128, encoded_page_bytes=1000,
                method=LayoutMethod.TWO_MEANS_480K,
                source_sha256=source_sha, seed=7,
                construction_sha256=hashlib.sha256(b"layout").digest(),
            ) for index in range(256)
        )
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            construct_cell(
                root, "s3://bucket/run", ids, vectors, membership,
                source_sha, membership_sha, seed=7, iterations=1,
            )
            identities = read_cell_seal(
                root, "s3://bucket/run", source_sha, membership_sha
            )
            self.assertEqual(identities.codes.encoded_bytes, 256 * 48)
            self.assertEqual(
                identities.codes.uri, "s3://bucket/run/artifacts/codes.bin"
            )
            self.assertFalse((root / "queries.parquet").exists())
            self.assertFalse((root / "truth.parquet").exists())


if __name__ == "__main__":
    unittest.main()
