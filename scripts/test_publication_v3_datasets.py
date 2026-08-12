import json
import tempfile
import unittest
from pathlib import Path

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
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "dataset"
            root.mkdir()
            first = root / "part-000.parquet"
            second = root / "part-001.parquet"
            first.write_bytes(b"PAR1fixture-onePAR1")
            second.write_bytes(b"PAR1fixture-twoPAR1")
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
            self.assertEqual(len(descriptor["objects"]), 2)
            self.assertEqual(
                sum(item["bytes"] for item in descriptor["objects"]),
                len(first.read_bytes()) + len(second.read_bytes()),
            )

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
