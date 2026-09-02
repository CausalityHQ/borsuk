from __future__ import annotations

import base64
import hashlib
import io
import json
import pathlib
import tempfile
import unittest

from scripts import stage_v24_witness_inputs as subject


def _identity(role: str, payload: bytes, _ordinal: int) -> dict[str, object]:
    digest = hashlib.sha256(payload).hexdigest()
    return {
        "digest": digest,
        "digest_algorithm": "sha256",
        "encoded_bytes": len(payload),
        "generation": "generation-v24-fixture",
        "role": role,
        "uri": f"s3://registered-bucket/v24/{role}",
    }


def _manifest_bytes(
    identities: list[dict[str, object]], phase: str = "witness-training"
) -> bytes:
    value = {
        "claim_eligible": False,
        "generation": "generation-v24-fixture",
        "inputs": identities,
        "output_uris": {
            "witness-graph": "s3://registered-bucket/v24/witness-graph.arrow",
            "witnesses-arrow": "s3://registered-bucket/v24/witnesses.arrow",
        },
        "phase": phase,
        "schema": "borsuk-v24-local-manifest-v1",
        "seed": 1_311_768_467_463_790_320,
        "source_row_count": 2,
        "witness_count": 2,
    }
    return json.dumps(value, separators=(",", ":"), sort_keys=True).encode() + b"\n"


def _preparation_manifest_bytes(
    shards: list[dict[str, object]],
    roster: dict[str, object],
    pages: list[dict[str, object]],
) -> bytes:
    value = {
        "claim_eligible": False,
        "d1_report_sha256": "aa" * 32,
        "dataset_id": "deep-image-96",
        "generation": "generation-v24-fixture",
        "index_id": "index-v24-fixture",
        "page_uri": "s3://registered-bucket/v24/",
        "pages": [
            {
                "generation_checksum": [7] * 32,
                "identity": identity,
                "page_ordinal": ordinal,
                "primary_rows": 1,
                "replica_rows": 0,
            }
            for ordinal, identity in enumerate(pages)
        ],
        "physical_row_count": 2,
        "roster": roster,
        "schema": "borsuk-v24-preparation-manifest-v1",
        "shards": [
            {
                "identity": identity,
                "ordinal_end": ordinal + 1,
                "ordinal_start": ordinal,
                "rows": 1,
            }
            for ordinal, identity in enumerate(shards)
        ],
        "source_archive_sha256": "bb" * 32,
        "source_row_count": 2,
    }
    return json.dumps(value, separators=(",", ":"), sort_keys=True).encode() + b"\n"


class FakeS3Client:
    def __init__(
        self,
        payloads: dict[tuple[str, str, str | None], bytes],
        versions: dict[str, str | None] | None = None,
    ) -> None:
        self.payloads = payloads
        self.versions = versions
        self.calls: list[dict[str, str]] = []

    def get_object(self, **request: str) -> dict[str, object]:
        self.calls.append(request)
        payload = self.payloads[
            (request["Bucket"], request["Key"], request.get("VersionId"))
        ]
        return {
            "Body": io.BytesIO(payload),
            "ChecksumSHA256": base64.b64encode(hashlib.sha256(payload).digest()).decode(),
            "ContentLength": len(payload),
            "ETag": '"deliberately-not-authority"',
            "VersionId": (
                f"transport-{request['Key']}"
                if self.versions is None
                else self.versions.get(request["Key"])
            ),
        }


class V24StagingTests(unittest.TestCase):
    def test_preparation_manifest_stages_parquet_roster_and_blake3_page_bodies(self) -> None:
        shard_payloads = [b"shard-zero", b"shard-one"]
        roster_payload = b"page-roster"
        page_payloads = [b"page-zero", b"page-one"]
        shards = [
            _identity(f"training-shard-{ordinal:05}", payload, ordinal)
            for ordinal, payload in enumerate(shard_payloads)
        ]
        roster = _identity("page-roster", roster_payload, 2)
        page_digests = (
            "74361bebbb287891263978c87c64eafe5db0b59a8332ddc7a6fee7713699c5ea",
            "8a837f2cd341cd50891d5992dbf38b125d0f1bf9c744d2d0618a64448b21db0a",
        )
        pages = [
            {
                "digest": digest,
                "digest_algorithm": "blake3",
                "encoded_bytes": len(payload),
                "generation": "generation-v24-fixture",
                "role": f"page-body-{ordinal:05}",
                "uri": f"s3://registered-bucket/v24/pages/{digest}",
            }
            for ordinal, (payload, digest) in enumerate(
                zip(page_payloads, page_digests, strict=True)
            )
        ]
        manifest_raw = _preparation_manifest_bytes(shards, roster, pages)
        payloads = {
            ("registered-bucket", f"v24/{identity['role']}", None): payload
            for identity, payload in zip(shards, shard_payloads, strict=True)
        }
        payloads[("registered-bucket", "v24/page-roster", None)] = roster_payload
        payloads.update(
            {
                ("registered-bucket", f"v24/pages/{digest}", None): payload
                for digest, payload in zip(page_digests, page_payloads, strict=True)
            }
        )
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            manifest = root / "preparation-manifest.json"
            inputs = root / "inputs"
            receipt = root / "staging-receipt.json"
            manifest.write_bytes(manifest_raw)
            manifest_sha256 = hashlib.sha256(manifest_raw).hexdigest()
            subject.stage_manifest(
                manifest,
                manifest_sha256,
                inputs,
                receipt,
                FakeS3Client(payloads),
            )
            self.assertEqual(
                subject.manifest_phase(manifest, manifest_sha256),
                "input-preparation",
            )
            self.assertEqual(
                sorted(path.name for path in inputs.iterdir()),
                [
                    "page-body-00000.page",
                    "page-body-00001.page",
                    "page-roster.json",
                    "training-shard-00000.parquet",
                    "training-shard-00001.parquet",
                ],
            )
            self.assertEqual(
                subject.validate_inventory(
                    manifest,
                    manifest_sha256,
                    inputs,
                    receipt,
                ),
                (
                    "training-shard-00000",
                    "training-shard-00001",
                    "page-roster",
                    "page-body-00000",
                    "page-body-00001",
                ),
            )

    def test_four_objects_share_logical_generation_with_optional_transport_versions(self) -> None:
        roles = (
            "training-result",
            "witness-graph",
            "witnesses-arrow",
            "page-rows-parquet",
        )
        payloads = {role: f"payload-{role}".encode() for role in roles}
        identities = [
            _identity(role, payloads[role], ordinal)
            for ordinal, role in enumerate(roles)
        ]
        object_payloads = {
            ("registered-bucket", f"v24/{role}", None): payloads[role]
            for role in roles
        }
        versions = {
            "v24/training-result": "version-a",
            "v24/witness-graph": None,
            "v24/witnesses-arrow": "version-c",
            "v24/page-rows-parquet": "version-d",
        }
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            manifest = root / "manifest.json"
            staging = root / "inputs"
            receipt = root / "staging-receipt.json"
            manifest.write_bytes(_manifest_bytes(identities, "posting-construction"))
            raw = subject.stage_manifest(
                manifest,
                hashlib.sha256(manifest.read_bytes()).hexdigest(),
                staging,
                receipt,
                FakeS3Client(object_payloads, versions),
            )
            objects = json.loads(raw)["ordered_objects"]
            self.assertEqual(
                [item.get("transport_version_id") for item in objects],
                ["version-a", None, "version-c", "version-d"],
            )
            self.assertTrue(
                all(item["generation"] == "generation-v24-fixture" for item in objects)
            )

    def test_stage_manifest_uses_exact_generation_length_and_digest_not_etag(self) -> None:
        payload = b"rows-parquet"
        identities = [_identity("construction-rows-parquet", payload, 0)]
        client = FakeS3Client(
            {
                (
                    "registered-bucket",
                    "v24/construction-rows-parquet",
                    None,
                ): payload,
            }
        )
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            manifest = root / "manifest.json"
            staging = root / "inputs"
            receipt = root / "staging-receipt.json"
            manifest.write_bytes(_manifest_bytes(identities))
            manifest_sha256 = hashlib.sha256(manifest.read_bytes()).hexdigest()

            raw = subject.stage_manifest(
                manifest, manifest_sha256, staging, receipt, client
            )

            self.assertEqual(
                client.calls,
                [
                    {
                        "Bucket": "registered-bucket",
                        "ChecksumMode": "ENABLED",
                        "Key": "v24/construction-rows-parquet",
                    },
                ],
            )
            self.assertEqual(receipt.read_bytes(), raw)
            self.assertEqual(
                subject.validate_inventory(
                    manifest, manifest_sha256, staging, receipt
                ),
                ("construction-rows-parquet",),
            )
            self.assertEqual(
                [path.name for path in sorted(staging.iterdir())],
                ["construction-rows.parquet"],
            )
            receipt_value = json.loads(raw)
            self.assertEqual(
                receipt_value["ordered_objects"][0]["relative_path"],
                "construction-rows.parquet",
            )
            self.assertEqual(
                receipt_value["ordered_objects"][0]["transport_version_id"],
                "transport-v24/construction-rows-parquet",
            )

    def test_staging_rejects_digest_drift_and_removes_only_owned_partial_files(self) -> None:
        expected = b"registered"
        identity = _identity("construction-rows-parquet", expected, 0)
        client = FakeS3Client(
            {
                (
                    "registered-bucket",
                    "v24/construction-rows-parquet",
                    None,
                ): b"REGISTEREd",
            }
        )
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            manifest = root / "manifest.json"
            staging = root / "inputs"
            receipt = root / "staging-receipt.json"
            manifest.write_bytes(_manifest_bytes([identity]))
            manifest_sha256 = hashlib.sha256(manifest.read_bytes()).hexdigest()
            with self.assertRaisesRegex(ValueError, "checksum"):
                subject.stage_manifest(
                    manifest, manifest_sha256, staging, receipt, client
                )
            self.assertFalse(staging.exists())
            self.assertFalse(receipt.exists())

    def test_inventory_requires_exact_complete_regular_file_set(self) -> None:
        payload = b"registered"
        identity = _identity("construction-rows-parquet", payload, 0)
        client = FakeS3Client(
            {
                (
                    "registered-bucket",
                    "v24/construction-rows-parquet",
                    None,
                ): payload,
            }
        )
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            manifest = root / "manifest.json"
            staging = root / "inputs"
            receipt = root / "staging-receipt.json"
            manifest.write_bytes(_manifest_bytes([identity]))
            manifest_sha256 = hashlib.sha256(manifest.read_bytes()).hexdigest()
            subject.stage_manifest(
                manifest, manifest_sha256, staging, receipt, client
            )

            (staging / "unexpected").write_bytes(b"not-owned")
            with self.assertRaisesRegex(ValueError, "inventory"):
                subject.validate_inventory(
                    manifest, manifest_sha256, staging, receipt
                )
            (staging / "unexpected").unlink()
            (staging / "construction-rows.parquet").unlink()
            with self.assertRaisesRegex(ValueError, "inventory"):
                subject.validate_inventory(
                    manifest, manifest_sha256, staging, receipt
                )

    def test_manifest_rejects_noncanonical_duplicate_and_unsafe_roles(self) -> None:
        payload = b"registered"
        valid = _identity("construction-rows-parquet", payload, 0)
        unsafe = dict(valid)
        unsafe["role"] = "../construction-rows"
        unregistered = dict(valid)
        unregistered["role"] = "operator-secret"
        blake3 = dict(valid)
        blake3["digest_algorithm"] = "blake3"
        for raw, message in (
            (_manifest_bytes([valid, valid]), "duplicate"),
            (_manifest_bytes([unsafe]), "role"),
            (_manifest_bytes([unregistered]), "role"),
            (_manifest_bytes([blake3]), "authority"),
            (_manifest_bytes([valid])[:-1] + b" \n", "canonical"),
        ):
            with self.subTest(message=message), tempfile.TemporaryDirectory() as temporary:
                root = pathlib.Path(temporary)
                manifest = root / "manifest.json"
                manifest.write_bytes(raw)
                with self.assertRaisesRegex(ValueError, message):
                    subject.stage_manifest(
                        manifest,
                        hashlib.sha256(raw).hexdigest(),
                        root / "inputs",
                        root / "receipt.json",
                        FakeS3Client({}),
                    )

    def test_staging_rejects_manifest_digest_drift_before_any_object_request(self) -> None:
        payload = b"registered"
        identity = _identity("construction-rows-parquet", payload, 0)
        client = FakeS3Client({})
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            manifest = root / "manifest.json"
            manifest.write_bytes(_manifest_bytes([identity]))
            with self.assertRaisesRegex(ValueError, "manifest digest"):
                subject.stage_manifest(
                    manifest,
                    "00" * 32,
                    root / "inputs",
                    root / "receipt.json",
                    client,
                )
        self.assertEqual(client.calls, [])

    def test_staging_rejects_query_authority_in_witness_training_phase(self) -> None:
        identity = _identity("query-parquet", b"sealed-query", 0)
        raw = _manifest_bytes([identity])
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            manifest = root / "manifest.json"
            manifest.write_bytes(raw)
            with self.assertRaisesRegex(ValueError, "phase roles"):
                subject.stage_manifest(
                    manifest,
                    hashlib.sha256(raw).hexdigest(),
                    root / "inputs",
                    root / "receipt.json",
                    FakeS3Client({}),
                )

    def test_staging_requires_one_shared_logical_generation(self) -> None:
        payload = b"registered"
        identity = _identity("construction-rows-parquet", payload, 0)
        identity["generation"] = "other-logical-generation"
        raw = _manifest_bytes([identity])
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            manifest = root / "manifest.json"
            manifest.write_bytes(raw)
            with self.assertRaisesRegex(ValueError, "generation"):
                subject.stage_manifest(
                    manifest,
                    hashlib.sha256(raw).hexdigest(),
                    root / "inputs",
                    root / "receipt.json",
                    FakeS3Client({}),
                )


if __name__ == "__main__":
    unittest.main()
