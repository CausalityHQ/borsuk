#!/usr/bin/env python3
"""Tests for the layout-qualification dataset identity lock."""

from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from scripts import freeze_layout_dataset_identity as identity


class FreezeLayoutDatasetIdentityTest(unittest.TestCase):
    def test_validates_and_hashes_every_benchmark_input(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            dataset = root / "tiny"
            dataset.mkdir()
            (dataset / "train.f32").write_bytes(b"\0" * 24)
            (dataset / "test.f32").write_bytes(b"\1" * 16)
            (dataset / "neighbors.i32").write_bytes(b"\2" * 24)
            meta = {
                "name": "tiny-euclidean",
                "metric": "euclidean",
                "dim": 2,
                "n_train": 3,
                "n_test": 2,
                "k": 3,
            }
            (dataset / "meta.json").write_text(json.dumps(meta) + "\n")
            protocol = {
                "dataset_contracts": {
                    "tiny": {
                        "ann_benchmarks_id": "tiny-euclidean",
                        "metric": "euclidean",
                        "dimensions": 2,
                        "train_vectors": 3,
                        "test_vectors": 2,
                        "ground_truth_k": 3,
                    }
                }
            }

            manifest = identity.build_manifest(root, protocol)

            files = manifest["datasets"]["tiny"]["files"]
            self.assertEqual(files["train.f32"]["bytes"], 24)
            self.assertEqual(
                files["train.f32"]["sha256"],
                hashlib.sha256(b"\0" * 24).hexdigest(),
            )
            identity.validate_manifest(manifest, protocol)

    def test_rejects_metadata_or_size_drift(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            dataset = root / "tiny"
            dataset.mkdir()
            (dataset / "train.f32").write_bytes(b"\0" * 20)
            (dataset / "test.f32").write_bytes(b"\1" * 16)
            (dataset / "neighbors.i32").write_bytes(b"\2" * 24)
            (dataset / "meta.json").write_text(
                json.dumps(
                    {
                        "name": "tiny-euclidean",
                        "metric": "euclidean",
                        "dim": 2,
                        "n_train": 3,
                        "n_test": 2,
                        "k": 3,
                    }
                )
            )
            protocol = {
                "dataset_contracts": {
                    "tiny": {
                        "ann_benchmarks_id": "tiny-euclidean",
                        "metric": "euclidean",
                        "dimensions": 2,
                        "train_vectors": 3,
                        "test_vectors": 2,
                        "ground_truth_k": 3,
                    }
                }
            }

            with self.assertRaisesRegex(ValueError, "train.f32"):
                identity.build_manifest(root, protocol)


if __name__ == "__main__":
    unittest.main()
