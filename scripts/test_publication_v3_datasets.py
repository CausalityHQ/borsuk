import json
import tempfile
import unittest
from pathlib import Path

from scripts.publication_v3_datasets import (
    build_dataset_descriptor,
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

    def test_generated_descriptor_is_deterministic_and_binds_shape(self) -> None:
        dataset = next(
            item
            for item in self.manifest["datasets"]
            if item["source"]["state"] == "generated"
        )
        first = build_dataset_descriptor(dataset)
        second = build_dataset_descriptor(dataset)
        self.assertEqual(first, second)
        self.assertEqual(first["dataset_id"], dataset["id"])
        self.assertEqual(first["rows"], dataset["scale"]["rows"])
        self.assertEqual(first["dimensions"], dataset["dimensions"])
        self.assertEqual(first["metric"], dataset["metric"])
        self.assertEqual(first["materialization"], "deterministic-generator")
        self.assertEqual(validate_dataset_descriptor(first, dataset), first)

    def test_external_descriptor_requires_exact_staged_parquet_bytes(self) -> None:
        dataset = next(
            item
            for item in self.manifest["datasets"]
            if item["source"]["state"] == "unstaged"
        )
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "dataset.parquet"
            path.write_bytes(b"PAR1fixturePAR1")
            staged = json.loads(json.dumps(dataset))
            staged["source"] = {
                "state": "staged",
                "url": path.resolve().as_uri(),
                "sha256": "0" * 64,
                "license": dataset["source"]["license"],
            }
            with self.assertRaisesRegex(ValueError, "checksum"):
                build_dataset_descriptor(staged)
            import hashlib

            staged["source"]["sha256"] = hashlib.sha256(path.read_bytes()).hexdigest()
            descriptor = build_dataset_descriptor(staged)
            self.assertEqual(descriptor["materialization"], "staged-parquet")
            self.assertEqual(descriptor["objects"][0]["bytes"], len(path.read_bytes()))

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
