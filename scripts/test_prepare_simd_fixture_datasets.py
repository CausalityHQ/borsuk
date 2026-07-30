#!/usr/bin/env python3
"""Tests for deterministic synthetic SIMD fixture preparation."""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

import numpy as np

from scripts.prepare_simd_fixture_datasets import prepare_binary, prepare_late


class PrepareSimdFixtureDatasetsTest(unittest.TestCase):
    def test_binary_fixture_has_exact_sizes_hamming_truth_and_identity(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            directory = prepare_binary(
                Path(temporary),
                documents=100,
                queries=10,
                dimensions=32,
                seed=7,
            )
            meta = json.loads((directory / "meta.json").read_text())
            self.assertEqual(meta["metric"], "hamming")
            self.assertEqual((directory / "train.f32").stat().st_size, 100 * 32 * 4)
            train = np.fromfile(directory / "train.f32", dtype="<f4").reshape(100, 32)
            test = np.fromfile(directory / "test.f32", dtype="<f4").reshape(10, 32)
            neighbors = np.fromfile(directory / "neighbors.i32", dtype="<i4").reshape(
                10, 10
            )
            for query in range(10):
                distances = np.count_nonzero(train != test[query], axis=1)
                expected = np.argsort(distances, kind="stable")[:10]
                np.testing.assert_array_equal(neighbors[query], expected)
            identity = json.loads((directory / "dataset-identity.json").read_text())
            self.assertTrue(identity["synthetic"])

    def test_late_fixture_has_separate_physical_descriptors(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            float32, float16 = prepare_late(
                Path(temporary),
                documents=20,
                queries=10,
                dimensions=32,
                document_tokens=4,
                query_tokens=2,
                seed=11,
            )
            for directory, element_type in (
                (float32, "float32"),
                (float16, "float16"),
            ):
                descriptor = json.loads((directory / "dataset.json").read_text())
                self.assertEqual(
                    descriptor["benchmark"]["vector_element_type"], element_type
                )
                self.assertEqual(
                    descriptor["benchmark"]["candidates_per_query_token"], [128]
                )
                self.assertTrue(
                    json.loads((directory / "dataset-identity.json").read_text())[
                        "synthetic"
                    ]
                )


if __name__ == "__main__":
    unittest.main()
