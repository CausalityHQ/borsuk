import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from scripts import run_v23_rabitq_falsifier as runner


class FakeS3:
    def __init__(self, payloads: dict[str, bytes]) -> None:
        self.payloads = payloads
        self.calls: list[tuple[str, str, str]] = []

    def download_file(self, bucket: str, key: str, filename: str) -> None:
        uri = f"s3://{bucket}/{key}"
        self.calls.append((bucket, key, filename))
        Path(filename).write_bytes(self.payloads[uri])


class V23RaBitQRunnerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.binary = self.root / "v23-rabitq"
        self.binary.write_bytes(b"binary")
        self.output = self.root / "result.json"
        self.roles = runner.INPUT_ROLES
        self.input_payloads = {role: role.encode() for role in self.roles}
        self.inputs = [
            {
                "role": role,
                "uri": f"s3://fixture/{role}",
                "sha256": hashlib.sha256(self.input_payloads[role]).hexdigest(),
                "blake3": None,
                "encoded_bytes": len(self.input_payloads[role]),
            }
            for role in self.roles
        ]
        self.manifest_bytes = (
            json.dumps(
                {
                    "dataset_id": "deep-image-96",
                    "index_id": "index-fixture",
                    "registered_inputs": self.inputs,
                    "output_roles": ["screen-result"],
                    "output_uri_prefix": "s3://fixture/output/",
                    "page_namespace_uri_prefix": None,
                    "rotation_seed_hex": "e" * 64,
                    "expected_pages": 28_282,
                    "expected_source_occurrences": 19_980_000,
                    "expected_unique_rows": 9_990_000,
                    "run_mode": {"execute": "development"},
                    "schema": "borsuk-v23-rabitq-manifest-v1",
                    "source_archive_sha256": "d" * 64,
                    "source_commit": "c" * 40,
                },
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
            + b"\n"
        )
        self.manifest = runner.FrozenArtifact(
            role="manifest",
            uri="s3://fixture/manifest",
            sha256=hashlib.sha256(self.manifest_bytes).hexdigest(),
            encoded_bytes=len(self.manifest_bytes),
            basename="manifest.json",
        )
        self.payloads = {"s3://fixture/manifest": self.manifest_bytes} | {
            item["uri"]: self.input_payloads[item["role"]] for item in self.inputs
        }

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def test_exact_manifest_and_nine_inputs_feed_one_local_binary(self) -> None:
        client = FakeS3(self.payloads)

        def execute(command: list[str], **kwargs: object) -> mock.Mock:
            return mock.Mock(
                returncode=0,
                stdout=b'{"claim_eligible":false}\n',
                stderr=b"",
            )

        with mock.patch.object(runner.subprocess, "run", side_effect=execute) as run:
            runner.run_falsifier(
                binary=self.binary,
                manifest=self.manifest,
                output=self.output,
                s3_client=client,
                scratch_parent=self.root,
            )

        self.assertEqual(self.output.read_bytes(), b'{"claim_eligible":false}\n')
        self.assertEqual(len(client.calls), 10)
        command = run.call_args.args[0]
        self.assertEqual(command[0], str(self.binary.resolve()))
        self.assertIn("--execute-development", command)
        self.assertNotIn("--bucket", command)
        self.assertNotIn("--endpoint", command)
        self.assertNotIn("--holdout", command)
        self.assertNotIn("--d3", command)
        self.assertEqual(run.call_args.kwargs["timeout"], 7_200)
        self.assertEqual(list(self.root.glob("v23-rabitq-*")), [])

    def test_digest_drift_stops_before_binary_and_cleans_named_files(self) -> None:
        client = FakeS3(self.payloads | {self.inputs[0]["uri"]: b"drift"})
        with mock.patch.object(runner.subprocess, "run") as run:
            with self.assertRaisesRegex(ValueError, "construction-receipt length differs"):
                runner.run_falsifier(
                    binary=self.binary,
                    manifest=self.manifest,
                    output=self.output,
                    s3_client=client,
                    scratch_parent=self.root,
                )
        run.assert_not_called()
        self.assertFalse(self.output.exists())
        self.assertEqual(list(self.root.glob("v23-rabitq-*")), [])

    def test_binary_failure_is_terminal_and_never_publishes_partial_output(self) -> None:
        client = FakeS3(self.payloads)
        with mock.patch.object(
            runner.subprocess,
            "run",
            return_value=mock.Mock(returncode=9, stdout=b"", stderr=b"failed"),
        ):
            with self.assertRaisesRegex(RuntimeError, "failed"):
                runner.run_falsifier(
                    binary=self.binary,
                    manifest=self.manifest,
                    output=self.output,
                    s3_client=client,
                    scratch_parent=self.root,
                )
        self.assertFalse(self.output.exists())
        self.assertEqual(list(self.root.glob("v23-rabitq-*")), [])


if __name__ == "__main__":
    unittest.main()
