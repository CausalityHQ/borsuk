from __future__ import annotations

import base64
import hashlib
import io
import json
import pathlib
import tempfile
import unittest

from scripts import stage_v24_witness_inputs as subject


def _identity(role: str, payload: bytes, ordinal: int) -> dict[str, object]:
    digest = hashlib.sha256(payload).hexdigest()
    return {
        "digest": digest,
        "digest_algorithm": "sha256",
        "encoded_bytes": len(payload),
        "generation": f"s3-version:version-{ordinal}",
        "role": role,
        "uri": f"s3://registered-bucket/v24/{role}",
    }


def _manifest_bytes(identities: list[dict[str, object]]) -> bytes:
    value = {
        "claim_eligible": False,
        "generation": "generation-v24-fixture",
        "inputs": identities,
        "output_uris": {
            "witness-graph": "s3://registered-bucket/v24/witness-graph.arrow",
            "witnesses-arrow": "s3://registered-bucket/v24/witnesses.arrow",
        },
        "phase": "witness-training",
        "schema": "borsuk-v24-local-manifest-v1",
        "seed": 1_311_768_467_463_790_320,
        "source_row_count": 2,
        "witness_count": 2,
    }
    return json.dumps(value, separators=(",", ":"), sort_keys=True).encode() + b"\n"


class FakeS3Client:
    def __init__(self, payloads: dict[tuple[str, str, str | None], bytes]) -> None:
        self.payloads = payloads
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
            "VersionId": request.get("VersionId"),
        }


class V24StagingTests(unittest.TestCase):
    def test_stage_manifest_uses_exact_generation_length_and_digest_not_etag(self) -> None:
        payload = b"rows-parquet"
        identities = [_identity("construction-rows-parquet", payload, 0)]
        client = FakeS3Client(
            {
                (
                    "registered-bucket",
                    "v24/construction-rows-parquet",
                    "version-0",
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
                        "VersionId": "version-0",
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

    def test_staging_rejects_digest_drift_and_removes_only_owned_partial_files(self) -> None:
        expected = b"registered"
        identity = _identity("construction-rows-parquet", expected, 0)
        client = FakeS3Client(
            {
                (
                    "registered-bucket",
                    "v24/construction-rows-parquet",
                    "version-0",
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
                    "version-0",
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

    def test_staging_requires_explicit_s3_version_generation(self) -> None:
        payload = b"registered"
        identity = _identity("construction-rows-parquet", payload, 0)
        identity["generation"] = (
            "unversioned-sha256:" + hashlib.sha256(payload).hexdigest()
        )
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
