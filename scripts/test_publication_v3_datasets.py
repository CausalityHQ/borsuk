import json
import tempfile
import unittest
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from scripts.publication_v3_datasets import (
    build_dataset_descriptor,
    dataset_materialization_sha256,
    validate_dataset_descriptor,
)
from scripts.publication_v3_protocol import validate_manifest


ROOT = Path(__file__).resolve().parents[1]


class PublicationV3DatasetTests(unittest.TestCase):
    def setUp(self) -> None:
        self.manifest = validate_manifest(
            json.loads(
                (ROOT / "docs/research/publication-v3-manifest.json").read_text()
            )
        )

    def test_generated_descriptor_requires_real_materialized_bytes(self) -> None:
        dataset = next(
            item
            for item in self.manifest["datasets"]
            if item["source"]["state"] == "generated"
        )
        with self.assertRaisesRegex(ValueError, "materialized bytes"):
            build_dataset_descriptor(dataset)

    def test_external_descriptor_requires_exact_staged_parquet_bytes(self) -> None:
        dataset = next(
            item
            for item in self.manifest["datasets"]
            if item["source"]["state"] == "unstaged"
        )
        dataset = json.loads(json.dumps(dataset))
        dataset["scale"]["rows"] = 4
        dataset["metric"] = "l2"
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "dataset"
            root.mkdir()
            dimensions = dataset["dimensions"]
            rows = dataset["scale"]["rows"]
            row_type = pa.list_(pa.float32(), dimensions)
            first = root / "train-00000.parquet"
            second = root / "train-00001.parquet"
            split = rows // 2
            for path, count in ((first, split), (second, rows - split)):
                pq.write_table(
                    pa.table(
                        {
                            "emb": pa.array(
                                [[float(row % 7)] * dimensions for row in range(count)],
                                type=row_type,
                            )
                        }
                    ),
                    path,
                )
            pq.write_table(
                pa.table({"emb": pa.array([[0.0] * dimensions], type=row_type)}),
                root / "test.parquet",
            )
            pq.write_table(
                pa.table(
                    {"neighbors_id": pa.array([list(range(10))], type=pa.list_(pa.int32()))}
                ),
                root / "neighbors.parquet",
            )
            (root / "meta.json").write_text(
                json.dumps(
                    {
                        "name": dataset["id"],
                        "metric": "euclidean",
                        "dim": dimensions,
                        "n_train": rows,
                        "n_test": 1,
                        "k": 10,
                    },
                    sort_keys=True,
                    separators=(",", ":"),
                )
                + "\n"
            )
            staged = json.loads(json.dumps(dataset))
            staged["source"] = {
                "state": "staged",
                "url": root.resolve().as_uri(),
                "sha256": "0" * 64,
                "license": dataset["source"]["license"],
            }
            with self.assertRaisesRegex(ValueError, "checksum"):
                build_dataset_descriptor(staged)
            staged["source"]["sha256"] = dataset_materialization_sha256(root)
            descriptor = build_dataset_descriptor(staged)
            self.assertEqual(descriptor["materialization"], "staged-parquet")
            self.assertEqual(len(descriptor["objects"]), 5)
            self.assertEqual(
                sum(
                    item["rows"]
                    for item in descriptor["objects"]
                    if item["role"] == "train"
                ),
                rows,
            )
            self.assertEqual(
                {item["role"] for item in descriptor["objects"]},
                {"train", "query", "ground-truth", "metadata"},
            )

    def test_descriptor_rejects_manifest_row_claim_above_physical_parquet_rows(self) -> None:
        dataset = next(
            item
            for item in self.manifest["datasets"]
            if item["source"]["state"] == "unstaged"
        )
        dataset = json.loads(json.dumps(dataset))
        dataset["scale"]["rows"] = 4
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            dimensions = dataset["dimensions"]
            row_type = pa.list_(pa.float32(), dimensions)
            pq.write_table(
                pa.table({"emb": pa.array([[0.0] * dimensions], type=row_type)}),
                root / "train-00000.parquet",
            )
            pq.write_table(
                pa.table({"emb": pa.array([[0.0] * dimensions], type=row_type)}),
                root / "test.parquet",
            )
            pq.write_table(
                pa.table(
                    {"neighbors_id": pa.array([list(range(10))], type=pa.list_(pa.int32()))}
                ),
                root / "neighbors.parquet",
            )
            (root / "meta.json").write_text(
                json.dumps(
                    {
                        "name": dataset["id"],
                        "metric": dataset["metric"],
                        "dim": dimensions,
                        "n_train": 4,
                        "n_test": 1,
                        "k": 10,
                    },
                    sort_keys=True,
                    separators=(",", ":"),
                )
                + "\n"
            )
            staged = json.loads(json.dumps(dataset))
            staged["source"] = {
                "state": "staged",
                "url": root.resolve().as_uri(),
                "sha256": dataset_materialization_sha256(root),
                "license": dataset["source"]["license"],
            }
            with self.assertRaisesRegex(ValueError, "train row count"):
                build_dataset_descriptor(staged)

    def test_standard_dataset_cannot_be_replaced_by_generated_source(self) -> None:
        dataset = next(
            item for item in self.manifest["datasets"] if item["kind"] == "standard-ann"
        )
        substituted = json.loads(json.dumps(dataset))
        substituted["source"] = {
            "state": "generated",
            "generator": "synthetic-clustered-v1",
            "seed": 7,
        }
        with self.assertRaisesRegex(ValueError, "standard dataset"):
            build_dataset_descriptor(substituted)


if __name__ == "__main__":
    unittest.main()
