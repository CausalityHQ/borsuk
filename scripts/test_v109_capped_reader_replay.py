from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import numpy as np

from scripts.v109_capped_reader_replay import (
    DIMENSIONS,
    ROW_BYTES,
    SQ8_DTYPE,
    load_manifest,
    score_ranges,
)
from scripts import v109_capped_reader_replay as replay_module


class CappedReplayTests(unittest.TestCase):
    def test_nomination_ranks_pages_by_best_row_score(self) -> None:
        books = np.zeros((64, 256, 12), dtype=np.float32)
        books[0, 1] = 1.0
        codes = np.zeros((4, 64), dtype=np.uint8)
        codes[1, 0] = 1
        codes[2, 0] = 1
        manifest = {"summaries": np.zeros((4, DIMENSIONS), dtype=np.float32),
                    "books": books, "codes": codes}
        with patch.object(replay_module, "ROWS", 4), patch.object(replay_module, "PAGE_ROWS", 2):
            ranked, historical = replay_module.nominate(
                np.zeros(DIMENSIONS, dtype=np.float32), manifest,
                regions=2, shortlist=2,
            )
        self.assertEqual(ranked, [0, 1])
        self.assertEqual(historical, [0, 1])

    def test_rejects_wrong_manifest_magic(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / "bad.bin"
            path.write_bytes(b"wrong-format")
            with self.assertRaisesRegex(ValueError, "magic"):
                load_manifest(path)

    def test_sq8_score_uses_stored_norm_and_returns_stable_ids(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / "sq8.bin"
            values = np.zeros(3, dtype=SQ8_DTYPE)
            values["id"] = [30, 10, 20]
            values["norm"] = [2.0, 1.0, 1.0]
            values.tofile(path)
            mapped = np.memmap(path, dtype=SQ8_DTYPE, mode="r", shape=(3,))
            query = np.zeros(DIMENSIONS, dtype=np.float32)
            manifest = {"low": np.zeros(DIMENSIONS, dtype=np.float32),
                        "span_step": np.ones(DIMENSIONS, dtype=np.float32)}
            self.assertEqual(
                score_ranges(mapped, query, manifest, ((0, 3 * ROW_BYTES),)),
                [10, 20, 30],
            )

    def test_sq8_score_rejects_unaligned_range(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / "sq8.bin"
            np.zeros(1, dtype=SQ8_DTYPE).tofile(path)
            mapped = np.memmap(path, dtype=SQ8_DTYPE, mode="r", shape=(1,))
            query = np.zeros(DIMENSIONS, dtype=np.float32)
            manifest = {"low": query, "span_step": query}
            with self.assertRaisesRegex(ValueError, "interval"):
                score_ranges(mapped, query, manifest, ((1, ROW_BYTES),))


if __name__ == "__main__":
    unittest.main()
