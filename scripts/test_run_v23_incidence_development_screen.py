import hashlib
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from scripts import run_v23_incidence_development_screen as runner


class FakeS3:
    def __init__(self, payloads: dict[str, bytes]) -> None:
        self.payloads = payloads
        self.calls: list[tuple[str, str, str]] = []

    def download_file(self, bucket: str, key: str, filename: str) -> None:
        uri = f"s3://{bucket}/{key}"
        self.calls.append((bucket, key, filename))
        Path(filename).write_bytes(self.payloads[uri])


class IncidenceDevelopmentScreenRunnerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.binary = self.root / "screen"
        self.binary.write_bytes(b"binary")
        self.output = self.root / "result.json"
        self.artifacts = tuple(
            runner.FrozenArtifact(
                role=role,
                uri=f"s3://fixture/{role}",
                digest_algorithm="sha256",
                digest=hashlib.sha256(role.encode()).hexdigest(),
                encoded_bytes=len(role),
                basename=f"{role}.bin",
            )
            for role in runner.ROLE_ORDER
        )
        self.payloads = {
            artifact.uri: artifact.role.encode() for artifact in self.artifacts
        }

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def test_exact_seven_artifacts_feed_one_direct_binary_execution(self) -> None:
        client = FakeS3(self.payloads)

        def execute(command: list[str], **kwargs: object) -> mock.Mock:
            output = Path(command[command.index("--output") + 1])
            output.write_bytes(b'{"claim_eligible":false}\n')
            return mock.Mock(returncode=0, stdout="", stderr="")

        with (
            mock.patch.object(runner, "FROZEN_ARTIFACTS", self.artifacts),
            mock.patch.object(runner.subprocess, "run", side_effect=execute) as run,
        ):
            runner.run_screen(
                binary=self.binary,
                source_commit="1" * 40,
                source_archive_sha256="2" * 64,
                output=self.output,
                s3_client=client,
                scratch_parent=self.root,
            )

        self.assertEqual(self.output.read_bytes(), b'{"claim_eligible":false}\n')
        self.assertEqual([call[1] for call in client.calls], list(runner.ROLE_ORDER))
        command = run.call_args.args[0]
        self.assertEqual(command[0], str(self.binary.resolve()))
        self.assertIn("--execute-development-screen", command)
        self.assertNotIn("--bucket", command)
        self.assertNotIn("--page-prefix", command)
        self.assertNotIn("--d3", command)
        self.assertEqual(list(self.root.glob("v23-incidence-screen-*")), [])

    def test_digest_drift_stops_before_binary_and_cleans_known_files(self) -> None:
        client = FakeS3(
            self.payloads
            | {self.artifacts[0].uri: b"x" * self.artifacts[0].encoded_bytes}
        )
        with (
            mock.patch.object(runner, "FROZEN_ARTIFACTS", self.artifacts),
            mock.patch.object(runner.subprocess, "run") as run,
        ):
            with self.assertRaisesRegex(ValueError, "tree-receipt digest differs"):
                runner.run_screen(
                    binary=self.binary,
                    source_commit="1" * 40,
                    source_archive_sha256="2" * 64,
                    output=self.output,
                    s3_client=client,
                    scratch_parent=self.root,
                )
        run.assert_not_called()
        self.assertFalse(self.output.exists())
        self.assertEqual(list(self.root.glob("v23-incidence-screen-*")), [])

    def test_binary_failure_is_terminal_and_does_not_publish_partial_output(self) -> None:
        client = FakeS3(self.payloads)
        with (
            mock.patch.object(runner, "FROZEN_ARTIFACTS", self.artifacts),
            mock.patch.object(
                runner.subprocess,
                "run",
                return_value=mock.Mock(returncode=7, stdout="", stderr="screen failed"),
            ),
        ):
            with self.assertRaisesRegex(RuntimeError, "screen failed"):
                runner.run_screen(
                    binary=self.binary,
                    source_commit="1" * 40,
                    source_archive_sha256="2" * 64,
                    output=self.output,
                    s3_client=client,
                    scratch_parent=self.root,
                )
        self.assertFalse(self.output.exists())
        self.assertEqual(list(self.root.glob("v23-incidence-screen-*")), [])


if __name__ == "__main__":
    unittest.main()
