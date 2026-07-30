#!/usr/bin/env python3
"""Tests for immutable SIMD dataset identity records."""

from __future__ import annotations

import hashlib
import tempfile
import unittest
from pathlib import Path

from scripts.freeze_simd_dataset_identity import (
    build_identity,
    validate_dataset_identity,
    validate_identity,
)


class FreezeSimdDatasetIdentityTest(unittest.TestCase):
    def test_hashes_all_inputs_and_labels_synthetic_data(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "nested").mkdir()
            (root / "a.bin").write_bytes(b"a")
            (root / "nested" / "b.bin").write_bytes(b"bb")
            identity = build_identity(
                root,
                dataset="fixture",
                source="deterministic-generator-v1",
                synthetic=True,
            )
            self.assertTrue(identity["synthetic"])
            self.assertEqual(
                [row["path"] for row in identity["files"]],
                ["a.bin", "nested/b.bin"],
            )
            self.assertEqual(
                identity["files"][0]["sha256"],
                hashlib.sha256(b"a").hexdigest(),
            )
            validate_identity(identity)

    def test_rejects_empty_or_symlink_only_inputs(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            with self.assertRaisesRegex(ValueError, "no input files"):
                build_identity(
                    root, dataset="fixture", source="generator", synthetic=True
                )

    def test_verification_rehashes_payloads_and_rejects_drift(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            payload = root / "vectors.bin"
            payload.write_bytes(b"frozen")
            identity = build_identity(
                root, dataset="fixture", source="generator", synthetic=True
            )
            validate_dataset_identity(root, identity)

            payload.write_bytes(b"drifted")
            with self.assertRaisesRegex(ValueError, "payload drift"):
                validate_dataset_identity(root, identity)


if __name__ == "__main__":
    unittest.main()
