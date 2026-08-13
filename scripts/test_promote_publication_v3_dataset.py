import base64
import hashlib
import json
import subprocess
import tempfile
import unittest
from pathlib import Path

import h5py
import numpy as np

from scripts.promote_publication_v3_dataset import promote_sealed_attempt
from scripts.test_stage_publication_v3_dataset import frozen_manifest


class FakeAws:
    def __init__(self, *, version: str = "2.36.11") -> None:
        self.objects: dict[str, tuple[bytes, dict[str, str]]] = {}
        self.writes: list[str] = []
        self.version = version

    def __call__(self, command, **_kwargs):
        if command == ["aws", "--version"]:
            return subprocess.CompletedProcess(
                command, 0, stdout=f"aws-cli/{self.version} Python/3.14.6", stderr=""
            )
        operation = command[command.index("s3api") + 1] if "s3api" in command else "cp"
        if operation == "cp":
            source, uri = command[command.index("cp") + 1 : command.index("cp") + 3]
            metadata = command[command.index("--metadata") + 1]
            key, value = metadata.split("=", 1)
            self.objects[uri] = (Path(source).read_bytes(), {key: value})
            self.writes.append(uri)
            return subprocess.CompletedProcess(command, 0, stdout="", stderr="")
        bucket = command[command.index("--bucket") + 1]
        key = command[command.index("--key") + 1]
        uri = f"s3://{bucket}/{key}"
        if operation == "head-object":
            if uri not in self.objects:
                return subprocess.CompletedProcess(
                    command, 254, stdout="", stderr="Not Found (404)"
                )
            body, metadata = self.objects[uri]
            return subprocess.CompletedProcess(
                command,
                0,
                stdout=json.dumps(
                    {
                        "ContentLength": len(body),
                        "Metadata": metadata,
                        "ChecksumSHA256": base64.b64encode(
                            hashlib.sha256(body).digest()
                        ).decode("ascii"),
                    }
                ),
                stderr="",
            )
        if operation == "put-object":
            if uri in self.objects and "--if-none-match" in command:
                return subprocess.CompletedProcess(
                    command, 1, stdout="", stderr="PreconditionFailed (412)"
                )
            body = Path(command[command.index("--body") + 1]).read_bytes()
            metadata = {}
            if "--metadata" in command:
                encoded = command[command.index("--metadata") + 1]
                name, value = encoded.split("=", 1)
                metadata[name] = value
            self.objects[uri] = (body, metadata)
            self.writes.append(uri)
            return subprocess.CompletedProcess(command, 0, stdout="{}", stderr="")
        if operation == "get-object":
            destination = Path(command[-1])
            if uri not in self.objects:
                return subprocess.CompletedProcess(command, 254, stdout="", stderr="missing")
            destination.write_bytes(self.objects[uri][0])
            return subprocess.CompletedProcess(command, 0, stdout="{}", stderr="")
        raise AssertionError(command)


class PromotePublicationV3DatasetTests(unittest.TestCase):
    def test_invalid_workers_and_old_cli_fail_before_materialization(self) -> None:
        manifest = frozen_manifest()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with self.assertRaisesRegex(ValueError, "upload workers"):
                promote_sealed_attempt(
                    manifest,
                    dataset_id="sift-128",
                    attempt=1,
                    work_root=root,
                    source_archive_sha256="a" * 64,
                    instance_id="i-0123456789abcdef0",
                    instance_type="r7g.8xlarge",
                    availability_zone="eu-central-1a",
                    upload_workers=0,
                    run_command=lambda *_args, **_kwargs: self.fail("AWS was invoked"),
                )
            old = FakeAws(version="2.18.0")
            with self.assertRaisesRegex(RuntimeError, "v2.19"):
                promote_sealed_attempt(
                    manifest,
                    dataset_id="sift-128",
                    attempt=1,
                    work_root=root,
                    source_archive_sha256="a" * 64,
                    instance_id="i-0123456789abcdef0",
                    instance_type="r7g.8xlarge",
                    availability_zone="eu-central-1a",
                    run_command=old,
                )
            self.assertFalse((root / "attempts").exists())

    def test_multi_object_promotion_verifies_before_marker_and_replays(self) -> None:
        manifest = frozen_manifest()
        dataset = next(item for item in manifest["datasets"] if item["id"] == "sift-128")
        dataset["scale"]["rows"] = 4
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "sift.hdf5"
            with h5py.File(source, "w") as handle:
                handle.create_dataset(
                    "train", data=np.arange(4 * 128, dtype=np.float32).reshape(4, 128)
                )
                handle.create_dataset(
                    "test", data=np.arange(128, dtype=np.float32).reshape(1, 128)
                )
                handle.create_dataset(
                    "neighbors", data=np.arange(10, dtype=np.int32).reshape(1, 10) % 4
                )
                handle.attrs["distance"] = "euclidean"
            aws = FakeAws()
            receipt = promote_sealed_attempt(
                manifest,
                dataset_id="sift-128",
                attempt=1,
                work_root=root / "work",
                source_cache=source,
                source_archive_sha256="a" * 64,
                instance_id="i-0123456789abcdef0",
                instance_type="r7g.8xlarge",
                availability_zone="eu-central-1a",
                run_command=aws,
            )
            self.assertGreater(receipt["object_count"], 1)
            self.assertTrue(aws.writes[-1].endswith("/STAGING_COMPLETE.json"))
            self.assertIn(receipt["provenance_uri"], aws.objects)
            before = dict(aws.objects)
            replay = promote_sealed_attempt(
                manifest,
                dataset_id="sift-128",
                attempt=1,
                work_root=root / "work",
                source_cache=source,
                source_archive_sha256="a" * 64,
                instance_id="i-0123456789abcdef0",
                instance_type="r7g.8xlarge",
                availability_zone="eu-central-1a",
                run_command=aws,
            )
            self.assertEqual(replay, receipt)
            self.assertEqual(aws.objects, before)
            first_object = receipt["objects"][0]["uri"]
            _, metadata = aws.objects[first_object]
            aws.objects[first_object] = (b"substituted", metadata)
            writes_before_failure = list(aws.writes)
            with self.assertRaisesRegex(RuntimeError, "differs from sealed bytes"):
                promote_sealed_attempt(
                    manifest,
                    dataset_id="sift-128",
                    attempt=1,
                    work_root=root / "work",
                    source_cache=source,
                    source_archive_sha256="a" * 64,
                    instance_id="i-0123456789abcdef0",
                    instance_type="r7g.8xlarge",
                    availability_zone="eu-central-1a",
                    run_command=aws,
                )
            self.assertEqual(aws.writes, writes_before_failure)


if __name__ == "__main__":
    unittest.main()
