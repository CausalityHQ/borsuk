import json
import tempfile
import unittest
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from scripts.publication_v3_beir import expected_beir_metadata
from scripts.publication_v3_datasets import (
    build_dataset_descriptor,
    dataset_materialization_sha256,
)
from scripts.publication_v3_protocol import validate_manifest

ROOT = Path(__file__).resolve().parents[1]


def fixed_list_table(name: str, rows: list[list[object]], value_type, width: int):
    flat = pa.array([value for row in rows for value in row], type=value_type)
    array = pa.FixedSizeListArray.from_arrays(
        flat,
        type=pa.list_(pa.field("item", value_type, nullable=False), width),
    )
    return pa.Table.from_arrays(
        [array], schema=pa.schema([pa.field(name, array.type, nullable=False)])
    )


def beir_rows_table(ids: list[str], texts: list[str], dimensions: int) -> pa.Table:
    embeddings = fixed_list_table(
        "emb", [[1.0] + [0.0] * (dimensions - 1) for _ in ids], pa.float32(), dimensions
    ).column(0)
    sparse_indices = pa.array(
        [[0, 2] for _ in ids],
        type=pa.list_(pa.field("item", pa.int32(), nullable=False)),
    )
    sparse_values = pa.array(
        [[0.6, 0.8] for _ in ids],
        type=pa.list_(pa.field("item", pa.float32(), nullable=False)),
    )
    return pa.Table.from_arrays(
        [
            pa.array(ids, type=pa.string()),
            pa.array(texts, type=pa.string()),
            embeddings,
            sparse_indices,
            sparse_values,
        ],
        schema=pa.schema(
            [
                pa.field("id", pa.string(), nullable=False),
                pa.field("text", pa.string(), nullable=False),
                pa.field("emb", embeddings.type, nullable=False),
                pa.field("sparse_indices", sparse_indices.type, nullable=False),
                pa.field("sparse_values", sparse_values.type, nullable=False),
            ]
        ),
    )


class PublicationV3DatasetTests(unittest.TestCase):
    def setUp(self) -> None:
        self.manifest = validate_manifest(
            json.loads(
                (ROOT / "docs/research/publication-v3-manifest.json").read_text()
            )
        )

    def test_generated_descriptor_requires_real_materialized_bytes(self) -> None:
        dataset = json.loads(
            json.dumps(
                next(
                    item
                    for item in self.manifest["datasets"]
                    if item["id"] == "synthetic-clustered-1m-768"
                )
            )
        )
        source = dataset["source"]
        self.assertEqual(source["state"], "staged-generated")
        dataset["source"] = {
            "state": "generated",
            "generator": source["generator"],
            "seed": source["seed"],
        }
        with self.assertRaisesRegex(ValueError, "materialized bytes"):
            build_dataset_descriptor(dataset)

    def test_materialization_digest_requires_the_declared_physical_kind(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaises(TypeError):
                dataset_materialization_sha256(Path(directory))

    def test_external_descriptor_requires_exact_staged_parquet_bytes(self) -> None:
        dataset = json.loads(
            json.dumps(
                next(
                    item
                    for item in self.manifest["datasets"]
                    if item["id"] == "deep-image-96"
                )
            )
        )
        dataset["scale"]["rows"] = 4
        dataset["metric"] = "l2"
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "dataset"
            root.mkdir()
            dimensions = dataset["dimensions"]
            rows = dataset["scale"]["rows"]
            first = root / "train-00000000.parquet"
            second = root / "train-00000001.parquet"
            split = rows // 2
            for path, count in ((first, split), (second, rows - split)):
                pq.write_table(
                    fixed_list_table(
                        "emb",
                        [[float(row % 7)] * dimensions for row in range(count)],
                        pa.float32(),
                        dimensions,
                    ),
                    path,
                )
            pq.write_table(
                fixed_list_table("emb", [[0.0] * dimensions], pa.float32(), dimensions),
                root / "test.parquet",
            )
            pq.write_table(
                fixed_list_table(
                    "neighbors_id",
                    [[identifier % 4 for identifier in range(10)]],
                    pa.int32(),
                    10,
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
            staged["source"]["sha256"] = dataset_materialization_sha256(
                root, kind=str(dataset["kind"])
            )
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

    def test_descriptor_rejects_manifest_row_claim_above_physical_parquet_rows(
        self,
    ) -> None:
        dataset = json.loads(
            json.dumps(
                next(
                    item
                    for item in self.manifest["datasets"]
                    if item["id"] == "deep-image-96"
                )
            )
        )
        dataset["scale"]["rows"] = 4
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            dimensions = dataset["dimensions"]
            pq.write_table(
                fixed_list_table("emb", [[0.0] * dimensions], pa.float32(), dimensions),
                root / "train-00000000.parquet",
            )
            pq.write_table(
                fixed_list_table("emb", [[0.0] * dimensions], pa.float32(), dimensions),
                root / "test.parquet",
            )
            pq.write_table(
                fixed_list_table(
                    "neighbors_id", [[0 for _ in range(10)]], pa.int32(), 10
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
                "sha256": dataset_materialization_sha256(
                    root, kind=str(dataset["kind"])
                ),
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

    def test_beir_descriptor_preserves_multimodal_identity_and_qrels(self) -> None:
        dataset = next(
            item for item in self.manifest["datasets"] if item["id"] == "scifact"
        )
        dataset = json.loads(json.dumps(dataset))
        dataset["scale"]["rows"] = 2
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            dimensions = dataset["dimensions"]
            pq.write_table(
                beir_rows_table(["d1", "d2"], ["first", "second"], dimensions),
                root / "corpus-00000000.parquet",
            )
            pq.write_table(
                beir_rows_table(["q1"], ["query"], dimensions),
                root / "queries-00000000.parquet",
            )
            pq.write_table(
                pa.table(
                    {
                        "query_id": pa.array(["q1"], type=pa.string()),
                        "corpus_id": pa.array(["d2"], type=pa.string()),
                        "score": pa.array([1], type=pa.int32()),
                    },
                    schema=pa.schema(
                        [
                            pa.field("query_id", pa.string(), nullable=False),
                            pa.field("corpus_id", pa.string(), nullable=False),
                            pa.field("score", pa.int32(), nullable=False),
                        ]
                    ),
                ),
                root / "qrels.parquet",
            )
            metadata = expected_beir_metadata("scifact")
            metadata.update({"documents": 2, "queries": 1, "qrels": 1})
            metadata["sparse"].update(
                {
                    "dimensions": 3,
                    "corpus_non_zero": 4,
                    "query_non_zero": 2,
                    "vocabulary_sha256": "a" * 64,
                }
            )
            (root / "meta.json").write_text(
                json.dumps(metadata, sort_keys=True, separators=(",", ":")) + "\n"
            )
            staged = json.loads(json.dumps(dataset))
            staged["source"] = {
                "state": "staged",
                "url": root.resolve().as_uri(),
                "sha256": dataset_materialization_sha256(root, kind="beir-hybrid"),
                "license": dataset["source"]["license"],
            }
            descriptor = build_dataset_descriptor(staged)
            self.assertEqual(
                {item["role"] for item in descriptor["objects"]},
                {"corpus", "query", "qrels", "metadata"},
            )
            pq.write_table(
                pa.table(
                    {
                        "query_id": pa.array(["q1"], type=pa.string()),
                        "corpus_id": pa.array(["missing"], type=pa.string()),
                        "score": pa.array([1], type=pa.int32()),
                    },
                    schema=pq.read_schema(root / "qrels.parquet"),
                ),
                root / "qrels.parquet",
            )
            staged["source"]["sha256"] = dataset_materialization_sha256(
                root, kind="beir-hybrid"
            )
            with self.assertRaisesRegex(ValueError, "qrels"):
                build_dataset_descriptor(staged)
            pq.write_table(
                pa.table(
                    {
                        "query_id": pa.array(["q1"], type=pa.string()),
                        "corpus_id": pa.array(["d2"], type=pa.string()),
                        "score": pa.array([1], type=pa.int32()),
                    },
                    schema=pq.read_schema(root / "qrels.parquet"),
                ),
                root / "qrels.parquet",
            )
            metadata["dense"]["revision"] = "0" * 40
            (root / "meta.json").write_text(
                json.dumps(metadata, sort_keys=True, separators=(",", ":")) + "\n"
            )
            staged["source"]["sha256"] = dataset_materialization_sha256(
                root, kind="beir-hybrid"
            )
            with self.assertRaisesRegex(ValueError, "encoder authority"):
                build_dataset_descriptor(staged)


if __name__ == "__main__":
    unittest.main()
