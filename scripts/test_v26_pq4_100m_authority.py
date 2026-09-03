import copy
import json
import unittest

from scripts.v26_pq4_100m_authority import (
    canonical_partition_authority_bytes,
    canonical_stage_partition_manifest_bytes,
    stage_partition_manifest,
    validate_partition_authority,
)


def identity(role: str, ordinal: int, suffix: str) -> dict[str, object]:
    return {
        "encoded_bytes": 1_000_000 + ordinal,
        "role": role,
        "sha256": f"{ordinal + 1:064x}",
        "uri": f"s3://frozen-v26/{suffix}",
    }


def authority() -> dict[str, object]:
    partitions = []
    for ordinal in range(10):
        start = ordinal * 10_000_000
        partitions.append(
            {
                "files": [
                    {
                        **identity(
                            f"training-shard-{ordinal:04}-0000",
                            ordinal + 20,
                            f"partition-{ordinal:04}/train-00000000.parquet",
                        ),
                        "ordinal_end": start + 10_000_000,
                        "ordinal_start": start,
                        "physical_schema": "emb:fixed-size-list<element:f32;96>:non-null",
                        "rows": 10_000_000,
                    }
                ],
                "manifest": identity(
                    f"partition-manifest-{ordinal:04}",
                    ordinal + 10,
                    f"partition-{ordinal:04}/manifest.json",
                ),
                "ordinal_end": start + 10_000_000,
                "ordinal_start": start,
                "shard_ordinal": ordinal,
            }
        )
    return {
        "binaries": [
            identity("synthetic-generator", 40, "bin/generate_synthetic_dataset"),
            identity("pq4-stage", 41, "bin/pq4_stage"),
            identity("pq4-build", 42, "bin/pq4_build"),
        ],
        "dataset": {
            "dimensions": 96,
            "generator": "synthetic-clustered-v1",
            "group_size": 100,
            "id": "synthetic-clustered-100m-96",
            "metric": "cosine",
            "physical_schema": "emb:fixed-size-list<element:f32;96>:non-null",
            "queries": 100,
            "seed": 1_501_096,
            "total_rows": 100_000_000,
        },
        "evaluation": {
            "query": identity("query-parquet", 50, "evaluation/test.parquet"),
            "truth": identity("truth-parquet", 51, "evaluation/neighbors.parquet"),
            "writer_shard_ordinal": 0,
        },
        "partitions": partitions,
        "schema": "borsuk-v26-pq4-100m-corpus-authority-v1",
        "source": {
            "archive_sha256": "a" * 64,
            "commit": "b" * 40,
        },
    }


class V26Pq4100mAuthorityTests(unittest.TestCase):
    def test_v26_pq4_100m_stage_manifest_matches_rust_stager_authority(self) -> None:
        # Break caught: the controller generates 100M partitions but never emits the exact
        # per-partition manifest that the Rust staging boundary authenticates.
        observed = authority()
        manifest = stage_partition_manifest(observed, 4)
        self.assertEqual(
            manifest,
            {
                "dataset_id": "synthetic-clustered-100m-96",
                "ordered_inputs": [
                    {
                        "authority_kind": "training-shard",
                        "dimensions": 96,
                        "identity": {
                            "digest": f"{25:064x}",
                            "digest_algorithm": "sha256",
                            "encoded_bytes": 1_000_024,
                            "role": "training-shard-0004-0000",
                            "uri": "s3://frozen-v26/partition-0004/train-00000000.parquet",
                        },
                        "metric": "cosine",
                        "ordinal_end": 50_000_000,
                        "ordinal_start": 40_000_000,
                        "physical_schema": "emb:fixed-size-list<element:f32;96>:non-null",
                        "rows": 10_000_000,
                    }
                ],
                "ordinal_end": 50_000_000,
                "ordinal_start": 40_000_000,
                "schema": "borsuk-v26-pq4-partition-manifest-v1",
                "shard_ordinal": 4,
            },
        )
        encoded = canonical_stage_partition_manifest_bytes(observed, 4)
        self.assertTrue(encoded.endswith(b"\n"))
        self.assertEqual(json.loads(encoded), manifest)
        self.assertEqual(encoded, canonical_stage_partition_manifest_bytes(observed, 4))

        for invalid in (-1, True, 10):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                stage_partition_manifest(observed, invalid)

    def test_v26_pq4_100m_authority_accepts_exact_partition_union(self) -> None:
        observed = authority()
        self.assertEqual(validate_partition_authority(observed), observed)
        encoded = canonical_partition_authority_bytes(observed)
        self.assertTrue(encoded.endswith(b"\n"))
        self.assertFalse(encoded.endswith(b"\n\n"))
        self.assertEqual(json.loads(encoded), observed)
        self.assertEqual(encoded, canonical_partition_authority_bytes(observed))

    def test_v26_pq4_100m_authority_rejects_recipe_and_identity_drift(self) -> None:
        cases = {
            "root-missing": lambda value: value.pop("source"),
            "root-extra": lambda value: value.update({"extra": 1}),
            "rows-type": lambda value: value["dataset"].update(
                {"total_rows": True}
            ),
            "dataset-id": lambda value: value["dataset"].update(
                {"id": "deep-image-96"}
            ),
            "dimensions": lambda value: value["dataset"].update({"dimensions": 768}),
            "generator": lambda value: value["dataset"].update(
                {"generator": "synthetic-uniform-v1"}
            ),
            "source-commit": lambda value: value["source"].update(
                {"commit": "B" * 40}
            ),
            "binary-order": lambda value: value["binaries"].reverse(),
            "binary-digest": lambda value: value["binaries"][1].update(
                {"sha256": "g" * 64}
            ),
            "binary-length": lambda value: value["binaries"][2].update(
                {"encoded_bytes": 0}
            ),
            "query-role": lambda value: value["evaluation"]["query"].update(
                {"role": "truth-parquet"}
            ),
            "truth-uri": lambda value: value["evaluation"]["truth"].update(
                {"uri": value["evaluation"]["query"]["uri"]}
            ),
            "truth-owner": lambda value: value["evaluation"].update(
                {"writer_shard_ordinal": 1}
            ),
        }
        for name, mutate in cases.items():
            with self.subTest(name=name):
                value = copy.deepcopy(authority())
                mutate(value)
                with self.assertRaises(ValueError):
                    validate_partition_authority(value)

    def test_v26_pq4_100m_authority_rejects_partition_topology_drift(self) -> None:
        cases = {
            "missing": lambda value: value["partitions"].pop(),
            "reordered": lambda value: value["partitions"].reverse(),
            "duplicate-ordinal": lambda value: value["partitions"][1].update(
                {"shard_ordinal": 0}
            ),
            "gap": lambda value: value["partitions"][4].update(
                {"ordinal_start": 40_000_001}
            ),
            "overlap": lambda value: value["partitions"][4].update(
                {"ordinal_start": 39_999_900}
            ),
            "short": lambda value: value["partitions"][4].update(
                {"ordinal_end": 49_999_900}
            ),
            "manifest-role": lambda value: value["partitions"][4][
                "manifest"
            ].update({"role": "partition-manifest-0005"}),
            "file-role": lambda value: value["partitions"][4]["files"][0].update(
                {"role": "training-shard-0005-0000"}
            ),
            "file-gap": lambda value: value["partitions"][4]["files"][0].update(
                {"ordinal_start": 40_000_100}
            ),
            "file-schema": lambda value: value["partitions"][4]["files"][0].update(
                {"physical_schema": "emb:list<f64>"}
            ),
            "file-rows": lambda value: value["partitions"][4]["files"][0].update(
                {"rows": 9_999_999}
            ),
        }
        for name, mutate in cases.items():
            with self.subTest(name=name):
                value = copy.deepcopy(authority())
                mutate(value)
                with self.assertRaises(ValueError):
                    validate_partition_authority(value)


if __name__ == "__main__":
    unittest.main()
