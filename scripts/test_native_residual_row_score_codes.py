"""Source-only residual code plane and physical-order authentication."""

from __future__ import annotations

import dataclasses
import hashlib
import tempfile
import unittest
from pathlib import Path

import numpy as np

from scripts.native_geometric_layout_screen import LayoutMethod, MembershipRow
from scripts.native_residual_row_score_codes import (
    construct_residual_codes,
    read_residual_codes,
    write_residual_codes,
)


class ResidualCodeTests(unittest.TestCase):
    @staticmethod
    def fixture():
        ids = tuple(index.to_bytes(4, "little") for index in range(256))
        rng = np.random.default_rng(39)
        vectors = rng.normal(size=(256, 48)).astype(np.float32)
        source_sha = hashlib.sha256(b"source").digest()
        membership = tuple(
            MembershipRow(
                stable_id=ids[index],
                source_ordinal=index,
                page_ordinal=1 if index < 128 else 0,
                in_page_ordinal=index % 128,
                page_rows=128,
                encoded_page_bytes=1000,
                method=LayoutMethod.TWO_MEANS_480K,
                source_sha256=source_sha,
                seed=7,
                construction_sha256=hashlib.sha256(b"layout").digest(),
            )
            for index in range(256)
        )
        return ids, vectors, membership, source_sha

    def test_interleaved_codes_are_deterministic_and_source_bound(self) -> None:
        ids, vectors, membership, source_sha = self.fixture()
        first = construct_residual_codes(ids, vectors, membership, seed=7, iterations=1)
        second = construct_residual_codes(ids, vectors, membership, seed=7, iterations=1)
        self.assertEqual(first.source_ordinals[:3], (128, 129, 130))
        self.assertEqual(first.codes.shape, (256, 72))
        self.assertTrue(np.array_equal(first.codes, second.codes))
        self.assertTrue(np.array_equal(first.first_books, second.first_books))
        self.assertTrue(np.array_equal(first.residual_books, second.residual_books))
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            membership_sha = hashlib.sha256(b"membership").digest()
            identities = write_residual_codes(root, first, source_sha, membership_sha)
            self.assertEqual((root / "codes.bin").stat().st_size, 256 * 72)
            self.assertEqual((root / "books.bin").stat().st_size, 2 * 48 * 256 * 4)
            actual = read_residual_codes(
                root, identities, ids, membership, source_sha,
                membership_sha, dimensions=48, seed=7,
            )
            self.assertTrue(np.array_equal(actual.codes, first.codes))
            changed = list(membership)
            changed[128] = dataclasses.replace(changed[128], in_page_ordinal=1)
            changed[129] = dataclasses.replace(changed[129], in_page_ordinal=0)
            with self.assertRaisesRegex(ValueError, "physical order"):
                read_residual_codes(
                    root, identities, ids, changed, source_sha,
                    membership_sha, dimensions=48, seed=7,
                )

    def test_corrupt_code_byte_is_rejected(self) -> None:
        ids, vectors, membership, source_sha = self.fixture()
        artifacts = construct_residual_codes(ids, vectors, membership, seed=7, iterations=1)
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            membership_sha = hashlib.sha256(b"membership").digest()
            identities = write_residual_codes(root, artifacts, source_sha, membership_sha)
            path = root / "codes.bin"
            body = bytearray(path.read_bytes())
            body[71] ^= 1
            path.write_bytes(body)
            with self.assertRaisesRegex(ValueError, "identity"):
                read_residual_codes(
                    root, identities, ids, membership, source_sha,
                    membership_sha, dimensions=48, seed=7,
                )


if __name__ == "__main__":
    unittest.main()
