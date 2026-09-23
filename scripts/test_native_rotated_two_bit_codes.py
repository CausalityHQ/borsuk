"""Source-only rotated scalar code and immutable physical group tests."""

from __future__ import annotations

import dataclasses
import hashlib
import tempfile
import unittest
from pathlib import Path

import numpy as np

from scripts.native_geometric_layout_screen import LayoutMethod, MembershipRow
from scripts.native_rotated_two_bit_codes import (
    ROW_BYTES,
    construct_two_bit_codes,
    decode_levels,
    read_two_bit_codes,
    rotate_rows,
    write_two_bit_codes,
)


class RotatedTwoBitCodesTests(unittest.TestCase):
    @staticmethod
    def fixture():
        rng = np.random.default_rng(205)
        vectors = rng.normal(size=(12, 768)).astype(np.float32)
        ids = tuple(index.to_bytes(4, "little") for index in range(len(vectors)))
        source_sha = hashlib.sha256(b"source").digest()
        physical = (3, 0, 6, 9, 1, 4, 7, 10, 2, 5, 8, 11)
        membership = tuple(
            MembershipRow(
                stable_id=ids[source],
                source_ordinal=source,
                page_ordinal=position // 3,
                in_page_ordinal=position % 3,
                page_rows=3,
                encoded_page_bytes=1000,
                method=LayoutMethod.TWO_MEANS_480K,
                source_sha256=source_sha,
                seed=20260921,
                construction_sha256=hashlib.sha256(b"layout").digest(),
            )
            for position, source in enumerate(physical)
        )
        return ids, vectors, membership, physical, source_sha

    def test_signed_rotation_is_deterministic_and_orthogonal(self) -> None:
        rng = np.random.default_rng(39)
        rows = rng.normal(size=(4, 768))
        rotated = rotate_rows(rows, rotation_seed=20260923)
        np.testing.assert_array_equal(rotated, rotate_rows(rows, rotation_seed=20260923))
        self.assertFalse(np.array_equal(rotated, rotate_rows(rows, rotation_seed=20260924)))
        np.testing.assert_allclose(
            np.sum(rotated * rotated, axis=1),
            np.sum(rows * rows, axis=1),
            rtol=0,
            atol=1e-10,
        )

    def test_source_only_code_fit_and_sealed_group_roundtrip(self) -> None:
        ids, vectors, membership, physical, source_sha = self.fixture()
        codes = construct_two_bit_codes(
            ids, vectors, membership, layout_seed=20260921, rotation_seed=20260923
        )
        self.assertEqual(codes.source_ordinals, physical)
        self.assertEqual(codes.page_row_counts, (3, 3, 3, 3))
        self.assertEqual(codes.records.shape, (12, ROW_BYTES))
        self.assertEqual(codes.records.dtype, np.uint8)
        self.assertEqual(len(codes.group_ranges), 1)
        self.assertEqual(codes.group_ranges[0][3], 4 + 4 * 4 + 12 * ROW_BYTES)
        levels = decode_levels(codes.records[:, :192])
        self.assertEqual(levels.shape, (12, 768))
        self.assertTrue(np.isin(levels, (-3, -1, 1, 3)).all())
        center = codes.mean.astype(np.float64)
        rotated = rotate_rows(vectors[list(physical)].astype(np.float64) - center, rotation_seed=20260923)
        scales = np.frombuffer(codes.records[:, 192:196].tobytes(), dtype="<f4")
        dot_error = np.sum((rotated - scales[:, None] * levels) * levels, axis=1)
        self.assertLess(float(np.max(np.abs(dot_error))), 1e-3)
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            identities = write_two_bit_codes(
                root,
                codes,
                source_sha,
                hashlib.sha256(b"membership").digest(),
                hashlib.sha256(b"tree").digest(),
            )
            reread = read_two_bit_codes(
                root,
                identities,
                ids,
                membership,
                source_sha,
                hashlib.sha256(b"membership").digest(),
                hashlib.sha256(b"tree").digest(),
                dimensions=768,
                layout_seed=20260921,
                rotation_seed=20260923,
            )
            np.testing.assert_array_equal(reread.records, codes.records)
            np.testing.assert_array_equal(reread.mean, codes.mean)
            body = bytearray((root / "groups.bin").read_bytes())
            body[-1] ^= 1
            (root / "groups.bin").write_bytes(body)
            with self.assertRaisesRegex(ValueError, "identity|group"):
                read_two_bit_codes(
                    root,
                    identities,
                    ids,
                    membership,
                    source_sha,
                    hashlib.sha256(b"membership").digest(),
                    hashlib.sha256(b"tree").digest(),
                    dimensions=768,
                    layout_seed=20260921,
                    rotation_seed=20260923,
                )
        swapped = list(membership)
        swapped[0] = dataclasses.replace(swapped[0], source_ordinal=4)
        with self.assertRaisesRegex(ValueError, "membership"):
            construct_two_bit_codes(
                ids, vectors, swapped, layout_seed=20260921, rotation_seed=20260923
            )


if __name__ == "__main__":
    unittest.main()
