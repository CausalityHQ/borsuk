"""Bounded physical OPQ8 code-plane construction on original page order."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import numpy as np

from scripts.native_geometric_layout_screen import ArtifactIdentity
from scripts.native_hundred_thousand_opq8_router import (
    CENTROIDS,
    DIMENSIONS,
    SUBSPACES,
    Opq8Model,
    encode_opq8_rows,
    write_opq8_model,
)
from scripts.native_one_million_opq8 import build_1m_opq8, read_1m_opq8
from scripts.test_native_one_million_group_selector import _fixture


def _model(root: Path) -> ArtifactIdentity:
    books = np.zeros((SUBSPACES, CENTROIDS, DIMENSIONS // SUBSPACES), dtype="<f4")
    books[:, 1, 0] = 1
    write_opq8_model(
        root / "model.bin",
        Opq8Model(np.zeros(DIMENSIONS, dtype="<f4"), np.eye(DIMENSIONS, dtype="<f4"), books),
    )
    import hashlib
    body = (root / "model.bin").read_bytes()
    return ArtifactIdentity("model", "s3://frozen/model.bin", hashlib.sha256(body).hexdigest(), len(body))


class OneMillionOpq8Tests(unittest.TestCase):
    def test_streamed_codes_follow_physical_pages(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            inputs = _fixture(root, base_rows=9)
            model = _model(root)
            first = build_1m_opq8(root, root / "first", inputs, model, batch_rows=2)
            build_1m_opq8(root, root / "second", inputs, model, batch_rows=3)
            self.assertEqual((root / "first/codes.bin").read_bytes(), (root / "second/codes.bin").read_bytes())
            self.assertEqual((root / "first/seal.json").read_bytes(), (root / "second/seal.json").read_bytes())
            self.assertEqual(first.codes.shape, (10, 8))
            physical = np.zeros((10, DIMENSIONS), dtype=np.float32)
            physical[:, 0] = np.arange(10)
            self.assertTrue(np.array_equal(first.codes, encode_opq8_rows(physical, first.model)))
            self.assertEqual(first.membership_groups.tolist(), [0] * 8 + [1, 2])
            self.assertEqual(read_1m_opq8(root / "first", inputs, model).seal, first.seal)
            code_path = root / "first/codes.bin"
            original = code_path.read_bytes()
            code_path.write_bytes(bytes([original[0] ^ 1]) + original[1:])
            with self.assertRaisesRegex(ValueError, "codes identity"):
                read_1m_opq8(root / "first", inputs, model)
            code_path.write_bytes(original)
            model_path = root / "first/model.bin"
            original_model = model_path.read_bytes()
            model_path.write_bytes(original_model[:-1] + bytes([original_model[-1] ^ 1]))
            with self.assertRaisesRegex(ValueError, "model identity"):
                read_1m_opq8(root / "first", inputs, model)

    def test_missing_or_duplicate_source_row_rejects_before_seal(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            inputs = _fixture(root, omit_source=True)
            model = _model(root)
            with self.assertRaisesRegex(ValueError, "source rows"):
                build_1m_opq8(root, root / "out", inputs, model, batch_rows=2)
            self.assertFalse((root / "out/seal.json").exists())
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            inputs = _fixture(root, duplicate_delta=True)
            model = _model(root)
            with self.assertRaisesRegex(ValueError, "overlap|duplicate"):
                build_1m_opq8(root, root / "out", inputs, model, batch_rows=2)
            self.assertFalse((root / "out/seal.json").exists())


if __name__ == "__main__":
    unittest.main()
