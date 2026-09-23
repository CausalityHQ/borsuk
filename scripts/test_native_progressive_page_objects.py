"""Page range and digest checks for the split two-bit object format."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import numpy as np

from scripts.native_progressive_page_objects import (
    read_authenticated_page,
    write_page_objects,
)


class ProgressivePageObjectsTest(unittest.TestCase):
    def test_four_objects_rejoin_every_physical_page(self) -> None:
        records = np.random.default_rng(13).integers(
            0, 256, size=(9, 200), dtype=np.uint8
        )
        counts = (2, 1, 3, 3)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            seal = write_page_objects(
                root, records, counts, base_pages=2,
                source_sha256="a" * 64, layout_sha256="b" * 64,
                mean_sha256="c" * 64, rotation_seed=7,
            )
            self.assertEqual(seal["schema"], "borsuk-progressive-two-bit-pages-v1")
            self.assertEqual(seal["rows"], 9)
            self.assertEqual(seal["row_bytes"], {"sign": 104, "magnitude": 96})
            start = 0
            for page, count in enumerate(counts):
                decoded = read_authenticated_page(root, seal, page)
                np.testing.assert_array_equal(decoded, records[start:start + count])
                start += count
            self.assertEqual((root / "sign-base.bin").stat().st_size, 3 * 104)
            self.assertEqual((root / "magnitude-delta.bin").stat().st_size, 6 * 96)

    def test_page_digest_rejects_tamper(self) -> None:
        records = np.zeros((2, 200), dtype=np.uint8)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            seal = write_page_objects(
                root, records, (1, 1), base_pages=1,
                source_sha256="a" * 64, layout_sha256="b" * 64,
                mean_sha256="c" * 64, rotation_seed=7,
            )
            path = root / "magnitude-delta.bin"
            body = bytearray(path.read_bytes())
            body[0] ^= 1
            path.write_bytes(body)
            with self.assertRaisesRegex(ValueError, "digest"):
                read_authenticated_page(root, seal, 1)

    def test_rejects_incorrect_source_count(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaises(ValueError):
                write_page_objects(
                    Path(directory), np.zeros((2, 200), dtype=np.uint8),
                    (1, 2), base_pages=1, source_sha256="a" * 64,
                    layout_sha256="b" * 64, mean_sha256="c" * 64,
                    rotation_seed=7,
                )


if __name__ == "__main__":
    unittest.main()
