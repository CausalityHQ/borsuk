#!/usr/bin/env python3
"""Tests for the direct VectorDBBench Parquet acquisition gate."""

from __future__ import annotations

import tempfile
import unittest
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

    def test_train_shards_sort_by_parsed_index_and_reject_gaps(self) -> None:
        self.assertEqual(
            fetch.ordered_train_files(
                [
                    "train-2-of-3.parquet",
                    "train-0-of-3.parquet",
                    "train-1-of-3.parquet",
                ],
                3,
            ),
            ["train-0-of-3.parquet", "train-1-of-3.parquet", "train-2-of-3.parquet"],
        )
        with self.assertRaisesRegex(ValueError, "numbering"):
            fetch.ordered_train_files(
                [
                    "train-0-of-3.parquet",
                    "train-1-of-3.parquet",
                    "train-3-of-3.parquet",
                ],
                3,
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

    def test_existing_download_validation_requires_every_exact_source_file(
        self,
    ) -> None:
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
            before = {path.name: path.read_bytes() for path in dataset_dir.iterdir()}

            with (
                mock.patch.object(fetch, "list_remote", return_value=remote_files),
                mock.patch.object(fetch, "parquet_contract", return_value=(1000, 1000)),
            ):
                self.assertEqual(
                    fetch.check_existing_dataset(dataset, output_root), dataset_dir
                )

            after = {path.name: path.read_bytes() for path in dataset_dir.iterdir()}
            self.assertEqual(after, before)

    def test_publication_materialization_reshards_without_mutating_source(self) -> None:
        import numpy as np
        import pyarrow as pa
        import pyarrow.parquet as pq

        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            source = root / "source"
            output = root / "publication"
            source.mkdir()
            original = fetch.DATASETS["cohere-medium-1M"]
            fetch.DATASETS["cohere-medium-1M"] = fetch.DatasetContract(
                remote_prefix=original.remote_prefix,
                rows=100,
                dimensions=4,
                train_files=1,
                metric=original.metric,
                workload=original.workload,
                adapter=original.adapter,
                license=original.license,
                license_source=original.license_source,
            )
            try:
                contract = fetch.DATASETS["cohere-medium-1M"]
                vector_type = pa.list_(pa.float32())
                train = np.arange(400, dtype=np.float32).reshape(100, 4)
                pq.write_table(
                    pa.table(
                        {
                            "id": pa.array(range(100), type=pa.int64()),
                            "emb": pa.array(train.tolist(), type=vector_type),
                        }
                    ),
                    source / "train.parquet",
                )
                pq.write_table(
                    pa.table(
                        {
                            "id": pa.array(range(2), type=pa.int64()),
                            "emb": pa.array(train[:2].tolist(), type=vector_type),
                        }
                    ),
                    source / "test.parquet",
                )
                pq.write_table(
                    pa.table(
                        {
                            "neighbors_id": pa.array(
                                [list(range(10)), list(range(10, 20))],
                                type=pa.list_(pa.int64()),
                            )
                        }
                    ),
                    source / "neighbors.parquet",
                )
                fetch.write_json(
                    source / "meta.json",
                    fetch.metadata_document(
                        "cohere-medium-1M", contract, n_test=2, k=10
                    ),
                )
                remote_files = ["neighbors.parquet", "test.parquet", "train.parquet"]
                fetch.write_json(
                    source / "dataset.json",
                    fetch.descriptor_document(
                        "cohere-medium-1M", source, remote_files, contract
                    ),
                )
                before = {
                    path.name: fetch.sha256_file(path) for path in source.iterdir()
                }
                fetch.materialize_publication_dataset(
                    "cohere-medium-1M", source, output, shard_target_bytes=400
                )
                self.assertEqual(len(list(output.glob("train-*.parquet"))), 4)
                self.assertEqual(
                    sum(
                        pq.ParquetFile(path).metadata.num_rows
                        for path in output.glob("train-*.parquet")
                    ),
                    100,
                )
                self.assertTrue(
                    all(
                        path.stat().st_size < 128 * 1024 * 1024
                        for path in output.glob("*.parquet")
                    )
                )
                self.assertEqual(
                    before,
                    {path.name: fetch.sha256_file(path) for path in source.iterdir()},
                )
                from publication_v3_datasets import (
                    build_dataset_descriptor,
                    dataset_materialization_sha256,
                )

                dataset = {
                    "id": "cohere-medium-1m-768",
                    "kind": "realistic-dense",
                    "scale": {"state": "exact", "rows": 100},
                    "dimensions": 4,
                    "metric": "cosine",
                    "source": {
                        "state": "staged",
                        "url": output.resolve().as_uri(),
                        "sha256": dataset_materialization_sha256(
                            output, kind="realistic-dense"
                        ),
                        "license": "fixture",
                    },
                }
                descriptor = build_dataset_descriptor(dataset)
                self.assertEqual(
                    sum(
                        item["rows"]
                        for item in descriptor["objects"]
                        if item["role"] == "train"
                    ),
                    100,
                )
            finally:
                fetch.DATASETS["cohere-medium-1M"] = original

    def test_publication_materialization_rejects_shuffled_train_ids(self) -> None:
        import numpy as np
        import pyarrow as pa
        import pyarrow.parquet as pq

        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            source = root / "source"
            source.mkdir()
            original = fetch.DATASETS["cohere-medium-1M"]
            contract = fetch.DatasetContract(
                remote_prefix=original.remote_prefix,
                rows=10,
                dimensions=4,
                train_files=1,
                metric=original.metric,
                workload=original.workload,
                adapter=original.adapter,
                license=original.license,
                license_source=original.license_source,
            )
            fetch.DATASETS["cohere-medium-1M"] = contract
            try:
                vectors = np.arange(40, dtype=np.float32).reshape(10, 4)
                vector_type = pa.list_(pa.float32())
                pq.write_table(
                    pa.table(
                        {
                            "id": pa.array([1, 0, *range(2, 10)], type=pa.int64()),
                            "emb": pa.array(vectors.tolist(), type=vector_type),
                        }
                    ),
                    source / "train.parquet",
                )
                pq.write_table(
                    pa.table(
                        {
                            "id": pa.array([0], type=pa.int64()),
                            "emb": pa.array(vectors[:1].tolist(), type=vector_type),
                        }
                    ),
                    source / "test.parquet",
                )
                pq.write_table(
                    pa.table(
                        {
                            "neighbors_id": pa.array(
                                [list(range(10))], type=pa.list_(pa.int64())
                            )
                        }
                    ),
                    source / "neighbors.parquet",
                )
                fetch.write_json(
                    source / "meta.json",
                    fetch.metadata_document(
                        "cohere-medium-1M", contract, n_test=1, k=10
                    ),
                )
                remote_files = ["neighbors.parquet", "test.parquet", "train.parquet"]
                fetch.write_json(
                    source / "dataset.json",
                    fetch.descriptor_document(
                        "cohere-medium-1M", source, remote_files, contract
                    ),
                )
                with self.assertRaisesRegex(ValueError, "canonical row positions"):
                    fetch.materialize_publication_dataset(
                        "cohere-medium-1M", source, root / "publication"
                    )
            finally:
                fetch.DATASETS["cohere-medium-1M"] = original


if __name__ == "__main__":
    unittest.main()
