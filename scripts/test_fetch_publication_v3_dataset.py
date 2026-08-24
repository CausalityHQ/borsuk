from __future__ import annotations

import hashlib
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest import mock

from scripts.fetch_publication_v3_dataset import (
    _s3_parts,
    fetch_objects,
    validated_object_plan,
)
from scripts.publication_v3_protocol import canonical_json_bytes


class FetchPublicationV3DatasetTests(unittest.TestCase):
    def test_s3_object_uri_rejects_query_and_fragment_aliases(self) -> None:
        for uri in (
            "s3://bucket/path?version=foreign",
            "s3://bucket/path#foreign",
        ):
            with self.subTest(uri=uri), self.assertRaisesRegex(ValueError, "canonical"):
                _s3_parts(uri)

    def test_checksum_failure_leaves_no_plausible_or_partial_dataset_file(self) -> None:
        class FakeClient:
            def download_file(self, _bucket, _key, target, ExtraArgs):
                self.extra_args = ExtraArgs
                Path(target).write_bytes(b"corrupt")

        client = FakeClient()
        boto3 = types.ModuleType("boto3")
        boto3.client = lambda *_args, **_kwargs: client
        botocore = types.ModuleType("botocore")
        botocore_config = types.ModuleType("botocore.config")
        botocore_config.Config = lambda **_kwargs: object()
        plan = (
            {
                "role": "metadata",
                "format": "json",
                "uri": "s3://bucket/generated/materialized/meta.json",
                "sha256": hashlib.sha256(b"expected").hexdigest(),
                "bytes": len(b"expected"),
                "rows": 1,
                "path": "meta.json",
            },
        )
        with (
            tempfile.TemporaryDirectory() as directory,
            mock.patch.dict(
                sys.modules,
                {
                    "boto3": boto3,
                    "botocore": botocore,
                    "botocore.config": botocore_config,
                },
            ),
        ):
            output = Path(directory)
            with self.assertRaisesRegex(ValueError, "downloaded"):
                fetch_objects(
                    plan,
                    output=output,
                    region="eu-central-1",
                    owner="453182569524",
                    workers=1,
                )
            self.assertFalse((output / "meta.json").exists())
            self.assertEqual(list(output.rglob("*.partial")), [])
        self.assertEqual(client.extra_args["ExpectedBucketOwner"], "453182569524")

    def test_runtime_plan_selects_authenticated_small_objects_only(self) -> None:
        prefix = "s3://bucket/generated/materialized"
        objects = [
            {
                "role": role,
                "format": "json" if role == "metadata" else "parquet",
                "uri": f"{prefix}/{path}",
                "sha256": f"{index:064x}",
                "bytes": 100 + index,
                "rows": rows,
            }
            for index, (role, path, rows) in enumerate(
                (
                    ("train", "train-00000000.parquet", 1_000_000),
                    ("query", "test.parquet", 1_000),
                    ("ground-truth", "neighbors.parquet", 1_000),
                    ("metadata", "meta.json", 1),
                ),
                1,
            )
        ]
        identity = [
            {
                **{
                    key: item[key]
                    for key in ("role", "format", "sha256", "bytes", "rows")
                },
                "path": str(item["uri"]).removeprefix(prefix + "/"),
            }
            for item in sorted(objects, key=lambda item: str(item["uri"]))
        ]
        content_sha = hashlib.sha256(canonical_json_bytes(identity)).hexdigest()
        source = {
            "state": "staged-generated",
            "generator": "synthetic-clustered-v1",
            "seed": 42,
            "generator_source_archive_sha256": "a" * 64,
            "url": prefix,
            "sha256": content_sha,
            "receipt_uri": "s3://bucket/generated/STAGING_COMPLETE.json",
            "receipt_sha256": "b" * 64,
        }
        dataset = {
            "id": "synthetic-clustered-1m-768",
            "kind": "synthetic-dense",
            "scale": {"state": "exact", "rows": 1_000_000},
            "dimensions": 768,
            "metric": "cosine",
            "source": source,
        }
        receipt = {
            "schema_version": 1,
            "adapter": "synthetic",
            "dataset_id": dataset["id"],
            "source_archive_sha256": "a" * 64,
            "source_provenance": {
                "dataset": dataset["id"],
                "generator": "synthetic-clustered-v1",
                "seed": 42,
                "kind": dataset["kind"],
                "rows": dataset["scale"]["rows"],
                "dimensions": dataset["dimensions"],
                "metric": dataset["metric"],
                "generator_source_archive_sha256": "a" * 64,
            },
            "dataset_content_sha256": content_sha,
            "output_uri": prefix,
            "terminal_uri": source["receipt_uri"],
            "objects": objects,
        }
        roles = frozenset({"query", "ground-truth", "metadata"})
        plan = validated_object_plan(dataset, receipt, roles=roles)
        self.assertEqual({item["role"] for item in plan}, roles)
        self.assertNotIn("train", {item["role"] for item in plan})
        corrupt = {**receipt, "objects": [dict(item) for item in objects]}
        corrupt["objects"][0]["sha256"] = "0" * 64
        with self.assertRaisesRegex(ValueError, "aggregate checksum"):
            validated_object_plan(dataset, corrupt, roles=roles)
        substituted = {**receipt, "dataset_id": "synthetic-other-1m-768"}
        with self.assertRaisesRegex(ValueError, "dataset contract"):
            validated_object_plan(dataset, substituted, roles=roles)
        substituted = {
            **receipt,
            "source_provenance": {**receipt["source_provenance"], "dimensions": 384},
        }
        with self.assertRaisesRegex(ValueError, "dataset contract"):
            validated_object_plan(dataset, substituted, roles=roles)


if __name__ == "__main__":
    unittest.main()
