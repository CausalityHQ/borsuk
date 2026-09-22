"""Source-bound PQ48 code artifacts for row-score page nomination."""

from __future__ import annotations

import dataclasses
import hashlib
import tempfile
import unittest
from pathlib import Path

import numpy as np

from scripts.native_geometric_layout_screen import LayoutMethod, MembershipRow
from scripts.native_row_score_code_artifacts import (
    construct_code_artifacts,
    read_code_artifacts,
    write_code_artifacts,
)


class CodeArtifactTests(unittest.TestCase):
    @staticmethod
    def fixture():
        ids = tuple(index.to_bytes(4, "little") for index in range(256))
        vectors = np.arange(256 * 48, dtype=np.float32).reshape(256, 48) / 1000
        source_sha = hashlib.sha256(b"source").digest()
        rows = tuple(
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
        return ids, vectors, rows, source_sha

    def test_codes_follow_physical_page_order_and_authenticate(self) -> None:
        ids, vectors, membership, source_sha = self.fixture()
        artifacts = construct_code_artifacts(ids, vectors, membership, seed=7, iterations=1)
        self.assertEqual(artifacts.source_ordinals[:3], (128, 129, 130))
        self.assertEqual(artifacts.page_row_counts, (128, 128))
        self.assertEqual(artifacts.codes.shape, (256, 48))
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            identities = write_code_artifacts(
                root, artifacts, source_sha, hashlib.sha256(b"membership").digest()
            )
            actual = read_code_artifacts(
                root, identities, ids, membership, source_sha,
                hashlib.sha256(b"membership").digest(), dimensions=48, seed=7,
            )
            self.assertTrue(np.array_equal(actual.books, artifacts.books))
            self.assertTrue(np.array_equal(actual.codes, artifacts.codes))
            self.assertEqual(actual.source_ordinals, artifacts.source_ordinals)

    def test_changed_code_byte_is_rejected(self) -> None:
        ids, vectors, membership, source_sha = self.fixture()
        artifacts = construct_code_artifacts(ids, vectors, membership, seed=7, iterations=1)
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            identities = write_code_artifacts(
                root, artifacts, source_sha, hashlib.sha256(b"membership").digest()
            )
            path = root / "codes.bin"
            content = bytearray(path.read_bytes())
            content[-1] ^= 1
            path.write_bytes(content)
            with self.assertRaisesRegex(ValueError, "code identity"):
                read_code_artifacts(
                    root, identities, ids, membership, source_sha,
                    hashlib.sha256(b"membership").digest(), dimensions=48, seed=7,
                )

    def test_substituted_membership_order_is_rejected(self) -> None:
        ids, vectors, membership, source_sha = self.fixture()
        artifacts = construct_code_artifacts(ids, vectors, membership, seed=7, iterations=1)
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            identities = write_code_artifacts(
                root, artifacts, source_sha, hashlib.sha256(b"membership").digest()
            )
            changed = list(membership)
            a, b = changed[128], changed[129]
            changed[128] = dataclasses.replace(a, in_page_ordinal=1)
            changed[129] = dataclasses.replace(b, in_page_ordinal=0)
            with self.assertRaisesRegex(ValueError, "physical order"):
                read_code_artifacts(
                    root, identities, ids, changed, source_sha,
                    hashlib.sha256(b"membership").digest(), dimensions=48, seed=7,
                )


if __name__ == "__main__":
    unittest.main()
