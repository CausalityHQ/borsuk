from __future__ import annotations

import json
import pathlib
import subprocess
import sys
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from scripts.preflight_s3_vectors_service import (
    canonical_receipt_bytes,
    run_service_preflight,
)


class FakeS3Vectors:
    class exceptions:
        class NotFoundException(Exception):
            pass

        class ConflictException(Exception):
            pass

    def __init__(self, fail_at: str | None = None) -> None:
        self.fail_at = fail_at
        self.calls: list[tuple[str, dict[str, object]]] = []
        self.meta = SimpleNamespace(
            service_model=SimpleNamespace(api_version="2025-07-15"),
            region_name="eu-central-1",
        )

    def _call(self, name: str, request: dict[str, object]) -> None:
        self.calls.append((name, request))
        if self.fail_at == name:
            raise RuntimeError(f"failed at {name}")

    def create_vector_bucket(self, **request: object) -> None:
        self._call("create_vector_bucket", request)

    def create_index(self, **request: object) -> None:
        self._call("create_index", request)

    def get_index(self, **request: object) -> dict[str, object]:
        self._call("get_index", request)
        return {"index": {"status": "ACTIVE"}}

    def put_vectors(self, **request: object) -> None:
        self._call("put_vectors", request)

    def query_vectors(self, **request: object) -> dict[str, object]:
        self._call("query_vectors", request)
        return {
            "vectors": [
                {"key": "probe-a", "distance": 0.0},
                {"key": "probe-b", "distance": 2.0},
            ]
        }

    def delete_index(self, **request: object) -> None:
        self._call("delete_index", request)

    def delete_vector_bucket(self, **request: object) -> None:
        self._call("delete_vector_bucket", request)


class S3VectorsServicePreflightTests(unittest.TestCase):
    def test_direct_script_cli_resolves_local_dependencies(self) -> None:
        completed = subprocess.run(
            [
                sys.executable,
                str(pathlib.Path(__file__).with_name("preflight_s3_vectors_service.py")),
                "--help",
            ],
            check=False,
            capture_output=True,
            text=True,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertIn("--bucket", completed.stdout)

    def test_runs_deterministic_two_vector_lifecycle_and_canonical_receipt(self) -> None:
        client = FakeS3Vectors()
        receipt = run_service_preflight(client, "borsuk-preflight-123", "probe-index")

        self.assertEqual(receipt.schema, "borsuk-s3-vectors-service-preflight-v1")
        self.assertFalse(receipt.claim_eligible)
        self.assertEqual(receipt.region, "eu-central-1")
        self.assertEqual(receipt.api_version, "2025-07-15")
        self.assertEqual(receipt.returned_keys, ("probe-a", "probe-b"))
        self.assertEqual(
            [name for name, _ in client.calls],
            [
                "create_vector_bucket",
                "create_index",
                "get_index",
                "put_vectors",
                "query_vectors",
                "delete_index",
                "delete_vector_bucket",
            ],
        )
        put_request = client.calls[3][1]
        self.assertEqual(
            put_request["vectors"],
            [
                {"key": "probe-a", "data": {"float32": [1.0, 0.0]}},
                {"key": "probe-b", "data": {"float32": [0.0, 1.0]}},
            ],
        )
        query_request = client.calls[4][1]
        self.assertEqual(query_request["queryVector"], {"float32": [1.0, 0.0]})
        self.assertEqual(query_request["topK"], 2)
        body = canonical_receipt_bytes(receipt)
        self.assertTrue(body.endswith(b"\n"))
        self.assertEqual(body, json.dumps(json.loads(body), sort_keys=True, separators=(",", ":")).encode() + b"\n")

    def test_dependency_and_vector_conversion_preflight_runs_before_mutation(self) -> None:
        client = FakeS3Vectors()
        with patch(
            "scripts.preflight_s3_vectors_service._dependency_preflight",
            side_effect=ValueError("conversion failed"),
        ):
            with self.assertRaisesRegex(ValueError, "conversion failed"):
                run_service_preflight(client, "borsuk-preflight-123", "probe-index")
        self.assertEqual(client.calls, [])

    def test_cleanup_attempts_index_and_bucket_after_every_post_create_failure(self) -> None:
        client = FakeS3Vectors(fail_at="put_vectors")
        with self.assertRaisesRegex(RuntimeError, "failed at put_vectors"):
            run_service_preflight(client, "borsuk-preflight-123", "probe-index")
        self.assertEqual([name for name, _ in client.calls][-2:], ["delete_index", "delete_vector_bucket"])


if __name__ == "__main__":
    unittest.main()
