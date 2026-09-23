"""Source-only page-centered code construction and group authentication."""

from __future__ import annotations

import dataclasses
import hashlib
import tempfile
import unittest
from pathlib import Path

import numpy as np

from scripts.native_geometric_layout_screen import LayoutMethod, MembershipRow
from scripts.native_page_centered_group_codes import (
    construct_group_codes,
    read_group_codes,
    write_group_codes,
)


class GroupCodeTests(unittest.TestCase):
    @staticmethod
    def fixture():
        ids = tuple(index.to_bytes(4, "little") for index in range(320))
        rng = np.random.default_rng(41)
        vectors = rng.normal(size=(320, 48)).astype(np.float32)
        source_sha = hashlib.sha256(b"source").digest()
        membership = tuple(
            MembershipRow(
                stable_id=ids[index],
                source_ordinal=index,
                page_ordinal=4 - index // 64,
                in_page_ordinal=index % 64,
                page_rows=64,
                encoded_page_bytes=1000,
                method=LayoutMethod.TWO_MEANS_480K,
                source_sha256=source_sha,
                seed=7,
                construction_sha256=hashlib.sha256(b"layout").digest(),
            )
            for index in range(320)
        )
        return ids, vectors, membership, source_sha

    def test_page_centering_is_deterministic_and_physically_ordered(self) -> None:
        ids, vectors, membership, source_sha = self.fixture()
        first = construct_group_codes(ids, vectors, membership, seed=7, iterations=1)
        second = construct_group_codes(ids, vectors, membership, seed=7, iterations=1)
        self.assertEqual(first.source_ordinals[:3], (256, 257, 258))
        self.assertEqual(first.page_row_counts, (64,) * 5)
        self.assertEqual(first.codes.shape, (320, 48))
        self.assertTrue(np.array_equal(first.codes, second.codes))
        self.assertTrue(np.array_equal(first.books, second.books))
        self.assertTrue(np.array_equal(first.page_means, second.page_means))
        for page in range(5):
            source_rows = first.source_ordinals[page * 64 : (page + 1) * 64]
            expected = vectors[list(source_rows)].mean(axis=0, dtype=np.float64)
            np.testing.assert_allclose(first.page_means[page], expected, atol=1e-6)
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            membership_sha = hashlib.sha256(b"membership").digest()
            tree_sha = hashlib.sha256(b"tree").digest()
            identities = write_group_codes(
                root, first, source_sha, membership_sha, tree_sha
            )
            self.assertEqual((root / "books.bin").stat().st_size, 48 * 256 * 4)
            self.assertEqual(len(first.group_ranges), 2)
            self.assertEqual(first.group_ranges[0][:2], (0, 4))
            self.assertEqual(first.group_ranges[1][:2], (4, 5))
            actual = read_group_codes(
                root,
                identities,
                ids,
                membership,
                source_sha,
                membership_sha,
                tree_sha,
                dimensions=48,
                seed=7,
            )
            self.assertTrue(np.array_equal(actual.codes, first.codes))
            self.assertEqual(actual.group_ranges, first.group_ranges)
            changed = list(membership)
            changed[256] = dataclasses.replace(changed[256], in_page_ordinal=1)
            changed[257] = dataclasses.replace(changed[257], in_page_ordinal=0)
            with self.assertRaisesRegex(ValueError, "physical order"):
                read_group_codes(
                    root,
                    identities,
                    ids,
                    changed,
                    source_sha,
                    membership_sha,
                    tree_sha,
                    dimensions=48,
                    seed=7,
                )

    def test_corrupt_group_byte_is_rejected(self) -> None:
        ids, vectors, membership, source_sha = self.fixture()
        artifact = construct_group_codes(ids, vectors, membership, seed=7, iterations=1)
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            membership_sha = hashlib.sha256(b"membership").digest()
            tree_sha = hashlib.sha256(b"tree").digest()
            identities = write_group_codes(
                root, artifact, source_sha, membership_sha, tree_sha
            )
            path = root / "groups.bin"
            body = bytearray(path.read_bytes())
            body[-1] ^= 1
            path.write_bytes(body)
            with self.assertRaisesRegex(ValueError, "identity"):
                read_group_codes(
                    root,
                    identities,
                    ids,
                    membership,
                    source_sha,
                    membership_sha,
                    tree_sha,
                    dimensions=48,
                    seed=7,
                )


if __name__ == "__main__":
    unittest.main()
