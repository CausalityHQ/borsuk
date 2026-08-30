from __future__ import annotations

import hashlib
import io
import json
import pathlib
import tempfile
import unittest

from scripts import stage_v23_leaf_page_incidence_inputs as subject


def _identity(role: str, payload: bytes, ordinal: int) -> dict[str, object]:
    return {
        "authority_kind": "training-shard",
        "dimensions": 96,
        "identity": {
            "digest": hashlib.sha256(payload).hexdigest(),
            "digest_algorithm": "sha256",
            "encoded_bytes": len(payload),
            "generation": f"version-{ordinal}",
            "role": role,
            "uri": f"s3://registered-bucket/frozen/{role}",
        },
        "metric": "cosine",
        "ordinal_end": ordinal + 1,
        "ordinal_start": ordinal,
        "physical_schema": "emb:fixed-size-list<element:f32;96>:non-null",
        "rows": 1,
    }


def _manifest_bytes(objects: list[dict[str, object]]) -> bytes:
    value = {
        "algorithm": {
            "aggregate_recall_ppm": 975_000,
            "dimensions": 96,
            "leaf_count": 65_536,
            "lloyd_iterations": 4,
            "minimum_query_recall_ppm": 800_000,
            "oracle_attainment_ppm": 995_000,
            "posting_caps": [512, 1024, 2048],
            "probe_counts": [32, 64, 128],
            "reservoir_rows": 2_097_152,
            "selection_width": 8,
            "tree_depth": 16,
        },
        "claim_eligible": False,
        "dataset_id": "deep-image-96",
        "index_id": "index-bcda7bb66812e162d45077e6",
        "ordered_inputs": objects,
        "parent_receipt_sha256": None,
        "phase": "tree-training",
        "schema": "borsuk-v23-incidence-manifest-v1",
        "source_archive_sha256": (
            "77917b0f5621d2580fef444ee362669a39d01c8453bee1c10ca1823631117f6d"
        ),
        "source_commit": "c339a546f8f9370cb2e6e9fb3b0fd4bdefa3cb05",
    }
    return json.dumps(value, separators=(",", ":"), sort_keys=True).encode() + b"\n"


class FakeS3Client:
    def __init__(self, payloads: dict[tuple[str, str, str], bytes]) -> None:
        self.payloads = payloads
        self.calls: list[dict[str, str]] = []

    def get_object(self, **request: str) -> dict[str, object]:
        self.calls.append(request)
        key = (request["Bucket"], request["Key"], request["VersionId"])
        payload = self.payloads[key]
        return {
            "Body": io.BytesIO(payload),
            "ContentLength": len(payload),
            "VersionId": request["VersionId"],
            "ETag": '"not-a-content-digest"',
        }


class StagingTests(unittest.TestCase):
    def test_staging_uses_only_exact_registered_gets_and_emits_receipt(self) -> None:
        payloads = [b"first-shard", b"second-shard"]
        objects = [
            _identity(f"training-shard-{index:04}", payload, index)
            for index, payload in enumerate(payloads)
        ]
        client = FakeS3Client(
            {
                ("registered-bucket", f"frozen/training-shard-{index:04}", f"version-{index}"): payload
                for index, payload in enumerate(payloads)
            }
        )
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            manifest = root / "manifest.json"
            staging = root / "staging"
            receipt = root / "staging-receipt.json"
            manifest.write_bytes(_manifest_bytes(objects))
            receipt_bytes = subject.stage_manifest(
                manifest, staging, receipt, client
            )

            self.assertEqual(
                client.calls,
                [
                    {
                        "Bucket": "registered-bucket",
                        "Key": f"frozen/training-shard-{index:04}",
                        "VersionId": f"version-{index}",
                    }
                    for index in range(2)
                ],
            )
            self.assertEqual(
                [path.name for path in sorted(staging.iterdir())],
                ["training-shard-0000", "training-shard-0001"],
            )
            self.assertEqual(receipt.read_bytes(), receipt_bytes)
            value = json.loads(receipt_bytes)
            self.assertEqual(value["manifest_sha256"], hashlib.sha256(manifest.read_bytes()).hexdigest())
            self.assertEqual(value["ordered_objects"][0]["relative_path"], "training-shard-0000")
            self.assertFalse(value["claim_eligible"])

    def test_staging_rejects_authority_drift_and_cleans_only_known_files(self) -> None:
        payload = b"registered"
        object_value = _identity("training-shard-0000", payload, 0)
        mutations = (
            (b"REGISTERED", "object digest differs"),
            (payload + b"long", "object length differs"),
        )
        for observed, message in mutations:
            with self.subTest(message=message):
                client = FakeS3Client(
                    {
                        (
                            "registered-bucket",
                            "frozen/training-shard-0000",
                            "version-0",
                        ): observed
                    }
                )
                with tempfile.TemporaryDirectory() as temporary:
                    root = pathlib.Path(temporary)
                    manifest = root / "manifest.json"
                    staging = root / "staging"
                    receipt = root / "receipt.json"
                    manifest.write_bytes(_manifest_bytes([object_value]))
                    with self.assertRaisesRegex(ValueError, message):
                        subject.stage_manifest(manifest, staging, receipt, client)
                    self.assertFalse(staging.exists())
                    self.assertFalse(receipt.exists())

    def test_manifest_rejects_noncanonical_duplicate_and_unsafe_authority(self) -> None:
        payload = b"registered"
        valid = _identity("training-shard-0000", payload, 0)
        duplicate = [valid, valid]
        unsafe = json.loads(json.dumps(valid))
        unsafe["identity"]["role"] = "../escape"
        for raw, message in (
            (_manifest_bytes(duplicate), "duplicate"),
            (_manifest_bytes([unsafe]), "role"),
            (_manifest_bytes([valid])[:-1] + b" \n", "canonical"),
        ):
            with self.subTest(message=message):
                with tempfile.TemporaryDirectory() as temporary:
                    root = pathlib.Path(temporary)
                    manifest = root / "manifest.json"
                    manifest.write_bytes(raw)
                    with self.assertRaisesRegex(ValueError, message):
                        subject.stage_manifest(
                            manifest,
                            root / "staging",
                            root / "receipt.json",
                            FakeS3Client({}),
                        )


if __name__ == "__main__":
    unittest.main()
