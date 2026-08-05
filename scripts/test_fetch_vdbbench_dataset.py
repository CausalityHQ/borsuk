#!/usr/bin/env python3
"""Tests for the direct VectorDBBench Parquet acquisition gate."""

from __future__ import annotations

import unittest
import tempfile
from pathlib import Path
from unittest import mock

import fetch_vdbbench_dataset as fetch


class FetchVectorDbBenchDatasetTest(unittest.TestCase):
    def test_market_aliases_match_current_vectordbbench_corpora(self) -> None:
        medium = fetch.DATASETS["cohere-medium-1M"]
        large = fetch.DATASETS["cohere-large-10M"]
        laion = fetch.DATASETS["laion-100M"]

        self.assertEqual(
            (medium.rows, medium.dimensions, medium.train_files), (1_000_000, 768, 1)
        )
        self.assertEqual(
            (large.rows, large.dimensions, large.train_files), (10_000_000, 768, 10)
        )
        self.assertEqual(
            (laion.rows, laion.dimensions, laion.train_files), (100_000_000, 768, 100)
        )

    def test_selects_unshuffled_train_test_and_ground_truth_only(self) -> None:
        listing = [
            "neighbors.parquet",
            "neighbors_int_1p.parquet",
            "scalar_labels.parquet",
            "shuffle_train-00-of-02.parquet",
            "shuffle_train-01-of-02.parquet",
            "test.parquet",
            "train-00-of-02.parquet",
            "train-01-of-02.parquet",
        ]

        self.assertEqual(
            fetch.select_files(listing, expected_train_files=2),
            [
                "neighbors.parquet",
                "test.parquet",
                "train-00-of-02.parquet",
                "train-01-of-02.parquet",
            ],
        )

    def test_rejects_incomplete_remote_train_shards(self) -> None:
        with self.assertRaisesRegex(ValueError, "expected 2 unshuffled"):
            fetch.select_files(
                ["neighbors.parquet", "test.parquet", "train-00-of-02.parquet"],
                expected_train_files=2,
            )

    def test_parses_aws_s3_ls_without_using_dates_as_filenames(self) -> None:
        listing = (
            "2023-05-12 10:52:44    3704127 neighbors.parquet\n"
            "2023-05-12 10:59:25    3133165 test.parquet\n"
            "2023-05-12 10:53:05 3131995162 train.parquet\n"
        )
        self.assertEqual(
            fetch.parse_s3_listing(listing),
            ["neighbors.parquet", "test.parquet", "train.parquet"],
        )

    def test_existing_download_validation_requires_every_exact_source_file(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            for name in ("neighbors.parquet", "test.parquet", "train.parquet"):
                (root / name).write_bytes(b"source")
            fetch.validate_local_files(
                root, ["neighbors.parquet", "test.parquet", "train.parquet"]
            )
            (root / "train.parquet").unlink()
            with self.assertRaisesRegex(ValueError, "missing downloaded source"):
                fetch.validate_local_files(
                    root, ["neighbors.parquet", "test.parquet", "train.parquet"]
                )

    def test_frozen_dataset_check_is_read_only(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            output_root = Path(temp)
            dataset = "cohere-medium-1M"
            dataset_dir = output_root / dataset
            dataset_dir.mkdir()
            remote_files = ["neighbors.parquet", "test.parquet", "train.parquet"]
            for name in remote_files:
                (dataset_dir / name).write_bytes(f"frozen-{name}".encode())
            contract = fetch.DATASETS[dataset]
            meta = fetch.metadata_document(dataset, contract, n_test=1000, k=1000)
            fetch.write_json(dataset_dir / "meta.json", meta)
            descriptor = fetch.descriptor_document(
                dataset, dataset_dir, remote_files, contract
            )
            fetch.write_json(dataset_dir / "dataset.json", descriptor)
            before = {
                path.name: path.read_bytes() for path in dataset_dir.iterdir()
            }

            with (
                mock.patch.object(fetch, "list_remote", return_value=remote_files),
                mock.patch.object(fetch, "parquet_contract", return_value=(1000, 1000)),
            ):
                self.assertEqual(
                    fetch.check_existing_dataset(dataset, output_root), dataset_dir
                )

            after = {path.name: path.read_bytes() for path in dataset_dir.iterdir()}
            self.assertEqual(after, before)


if __name__ == "__main__":
    unittest.main()
