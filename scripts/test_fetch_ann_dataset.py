#!/usr/bin/env python3
"""Tests for bounded ann-benchmarks materialization."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import h5py
import numpy as np
import pyarrow.parquet as pq

from scripts.fetch_ann_dataset import convert_hdf5_dataset
from scripts.publication_v3_datasets import (
    build_dataset_descriptor,
    dataset_materialization_sha256,
)


class FetchAnnDatasetTests(unittest.TestCase):
    def test_hdf5_streams_to_ordered_bounded_parquet_and_descriptor(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source.hdf5"
            output = root / "dataset"
            train = np.arange(400, dtype=np.float32).reshape(100, 4)
            test = train[[1, 51]]
            neighbors = np.array(
                [list(range(10)), list(range(50, 60))], dtype=np.int32
            )
            with h5py.File(source, "w") as handle:
                handle.create_dataset("train", data=train)
                handle.create_dataset("test", data=test)
                handle.create_dataset("neighbors", data=neighbors)
                handle.attrs["distance"] = "euclidean"

            metadata = convert_hdf5_dataset(
                source,
                output,
                dataset_name="fixture-4-euclidean",
                publication_id="fixture-4",
                shard_target_bytes=400,
            )
            train_paths = sorted(output.glob("train-*.parquet"))
            self.assertEqual(len(train_paths), 4)
            self.assertTrue(all(path.stat().st_size < 128 * 1024 * 1024 for path in train_paths))
            decoded = np.concatenate(
                [pq.read_table(path).column("emb").to_pylist() for path in train_paths]
            )
            np.testing.assert_array_equal(decoded, train)
            self.assertEqual(metadata["metric"], "euclidean")

            dataset = {
                "id": "fixture-4",
                "kind": "standard-ann",
                "scale": {"state": "exact", "rows": 100},
                "dimensions": 4,
                "metric": "l2",
                "source": {
                    "state": "staged",
                    "url": output.resolve().as_uri(),
                    "sha256": dataset_materialization_sha256(output),
                    "license": "fixture",
                },
            }
            descriptor = build_dataset_descriptor(dataset)
            self.assertEqual(
                sum(item["rows"] for item in descriptor["objects"] if item["role"] == "train"),
                100,
            )

    def test_conversion_rejects_nonempty_output(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source.hdf5"
            output = root / "dataset"
            output.mkdir()
            (output / "stale").write_text("stale")
            with self.assertRaisesRegex(FileExistsError, "nonempty"):
                convert_hdf5_dataset(
                    source,
                    output,
                    dataset_name="fixture-4-euclidean",
                    publication_id="fixture-4",
                )


if __name__ == "__main__":
    unittest.main()
