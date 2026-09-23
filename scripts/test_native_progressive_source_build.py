"""Streaming, source-only build of mirrored two-bit pages."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import numpy as np

from scripts.native_progressive_page_objects import read_authenticated_page
from scripts.native_progressive_source_build import build_from_source_batches


class ProgressiveSourceBuildTest(unittest.TestCase):
    def test_physical_records_and_mean_are_sealed(self) -> None:
        vectors = np.random.default_rng(91).normal(size=(5, 768)).astype(np.float32)
        ids = np.asarray([11, 12, 13, 14, 15], dtype=np.int64)
        physical = {13: 0, 11: 1, 15: 2, 12: 3, 14: 4}

        def batches():
            yield ids[:3], vectors[:3]
            yield ids[3:], vectors[3:]

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            result = build_from_source_batches(
                root, batches, physical, (2, 3), base_pages=1,
                source_sha256="a" * 64, layout_sha256="b" * 64,
                rotation_seed=9,
            )
            np.testing.assert_array_equal(
                np.fromfile(root / "mean.bin", dtype="<f4"),
                np.mean(vectors, axis=0, dtype=np.float64).astype(np.float32),
            )
            records = np.memmap(root / "records.bin", dtype=np.uint8, mode="r", shape=(5, 200))
            np.testing.assert_array_equal(read_authenticated_page(root, result["seal"], 0), records[:2])
            np.testing.assert_array_equal(read_authenticated_page(root, result["seal"], 1), records[2:])
            self.assertEqual(result["rows"], 5)

    def test_second_pass_cannot_change_source_rows(self) -> None:
        rows = np.zeros((2, 768), dtype=np.float32)
        calls = 0

        def batches():
            nonlocal calls
            calls += 1
            changed = rows.copy()
            if calls == 2:
                changed[0, 0] = 1
            yield np.asarray([1, 2], dtype=np.int64), changed

        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(ValueError, "pass differs"):
                build_from_source_batches(
                    Path(directory), batches, {1: 0, 2: 1}, (1, 1),
                    base_pages=1, source_sha256="a" * 64,
                    layout_sha256="b" * 64, rotation_seed=9,
                )


if __name__ == "__main__":
    unittest.main()
