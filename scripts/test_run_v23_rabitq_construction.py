from __future__ import annotations

import hashlib
import io
import json
import signal
import struct
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import numpy
import pyarrow as pa
from blake3 import blake3

from scripts import run_v23_rabitq_construction as runner


class _Body:
    def __init__(self, payload: bytes) -> None:
        self._payload = io.BytesIO(payload)

    def read(self, size: int = -1) -> bytes:
        return self._payload.read(size)

    def close(self) -> None:
        self._payload.close()


class FakeS3:
    def __init__(self, payloads: dict[str, bytes]) -> None:
        self.payloads = payloads
        self.downloads: list[str] = []
        self.gets: list[str] = []
        self.uploads: list[tuple[str, str]] = []

    def download_file(self, bucket: str, key: str, filename: str) -> None:
        uri = f"s3://{bucket}/{key}"
        self.downloads.append(uri)
        Path(filename).write_bytes(self.payloads[uri])

    def get_object(self, *, Bucket: str, Key: str) -> dict[str, object]:
        uri = f"s3://{Bucket}/{Key}"
        self.gets.append(uri)
        return {"Body": _Body(self.payloads[uri])}

    def upload_file(self, filename: str, bucket: str, key: str) -> None:
        self.uploads.append((Path(filename).name, f"s3://{bucket}/{key}"))


class _OpenBytesIO(io.BytesIO):
    def close(self) -> None:
        pass


class _BrokenPipeSink(_OpenBytesIO):
    def write(self, payload: bytes) -> int:
        del payload
        raise BrokenPipeError("fixture child closed stdin")


class FakeProcess:
    def __init__(self) -> None:
        self._input = _OpenBytesIO()
        self.stdin = self._input
        self.command: list[str] | None = None
        self.returncode = 0

    def bind(self, command: list[str]) -> FakeProcess:
        self.command = command
        return self

    def communicate(self, timeout: int) -> tuple[bytes, bytes]:
        assert timeout == 7_200
        assert self.command is not None
        manifest_path = Path(self.command[self.command.index("--manifest") + 1])
        output_directory = Path(
            self.command[self.command.index("--output-directory") + 1]
        )
        manifest_bytes = manifest_path.read_bytes()
        manifest = json.loads(manifest_bytes)
        output_directory.mkdir(parents=True, exist_ok=True)
        outputs = []
        for role, basename in runner.OUTPUT_BASENAMES.items():
            payload = f"artifact:{role}".encode()
            (output_directory / basename).write_bytes(payload)
            outputs.append(
                _identity(role, manifest["output_uri_prefix"] + basename, payload)
            )
        receipt = {
            "claim_eligible": False,
            "dataset_id": manifest["dataset_id"],
            "index_id": manifest["index_id"],
            "inputs": manifest["registered_inputs"],
            "manifest": manifest,
            "manifest_sha256": hashlib.sha256(manifest_bytes).hexdigest(),
            "outputs": outputs,
            "run_mode": manifest["run_mode"],
            "schema": "borsuk-v23-rabitq-receipt-v1",
            "source_archive_sha256": manifest["source_archive_sha256"],
            "source_commit": manifest["source_commit"],
            "stop_reason": None,
            "terminal_status": "complete",
        }
        receipt_bytes = _canonical(receipt)
        (output_directory / "construction-receipt.json").write_bytes(receipt_bytes)
        return receipt_bytes, b""

    def input_table(self) -> pa.Table:
        return pa.ipc.open_stream(self._input.getvalue()).read_all()

    def poll(self) -> int:
        return self.returncode


class BrokenPipeProcess(FakeProcess):
    def __init__(self) -> None:
        super().__init__()
        self.stdin = _BrokenPipeSink()
        self.returncode = 2

    def communicate(self, timeout: int) -> tuple[bytes, bytes]:
        assert self.command is not None
        scratch = Path(
            self.command[self.command.index("--scratch-directory") + 1]
        )
        (scratch / "rabitq-id-run-00000000.arrow").write_bytes(b"partial spill")
        self.stdin = None
        self.returncode = 2
        return b"", b"child authority root cause\n"


class WedgedBrokenPipeProcess(BrokenPipeProcess):
    def __init__(self) -> None:
        super().__init__()
        self.pid = 4242
        self.communicate_calls = 0

    def communicate(self, timeout: int | None = None) -> tuple[bytes, bytes]:
        self.communicate_calls += 1
        if self.communicate_calls == 1:
            assert timeout == 30
            raise subprocess.TimeoutExpired(
                self.command or [], timeout, stderr=b"buffered child panic\n"
            )
        assert timeout is None
        self.returncode = -signal.SIGTERM
        return b"", b"buffered child panic\n"

    def wait(self, timeout: int) -> int:
        assert timeout == 30
        self.returncode = -signal.SIGTERM
        return self.returncode


def _canonical(value: object) -> bytes:
    return (
        json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
        + b"\n"
    )


def _identity(role: str, uri: str, payload: bytes) -> dict[str, object]:
    return {
        "blake3": None,
        "encoded_bytes": len(payload),
        "role": role,
        "sha256": hashlib.sha256(payload).hexdigest(),
        "uri": uri,
    }


def _page(page_ordinal: int) -> tuple[dict[str, object], bytes]:
    primary_id = f"row-{page_ordinal:02d}".encode()
    replica_id = f"row-{(page_ordinal + 1) % 8:02d}".encode()
    identifiers = (primary_id, replica_id)
    offsets = [0]
    id_bytes = bytearray()
    for identifier in identifiers:
        id_bytes.extend(identifier)
        offsets.append(len(id_bytes))
    offset_bytes = b"".join(struct.pack("<I", value) for value in offsets)
    id_section_bytes = len(offset_bytes) + len(id_bytes)
    vectors = numpy.zeros((2, 96), dtype="<f2")
    vectors[0, page_ordinal] = 1.0
    vectors[1, (page_ordinal + 1) % 8] = 1.0
    codes = vectors.tobytes()
    generation = bytes.fromhex("42" * 32)
    header = bytearray(96)
    header[:4] = b"BVP2"
    header[4:8] = bytes((2, 3, 4, 0))
    struct.pack_into("<IIIIII", header, 8, 96, page_ordinal, 1, 1, id_section_bytes, len(codes))
    header[32:64] = generation
    struct.pack_into("<H", header, 64, 192)
    body = bytes(header) + offset_bytes + bytes(id_bytes) + codes
    checksum = blake3(body).hexdigest()
    return (
        {
            "checksum": checksum,
            "code_width": 192,
            "dimensions": 96,
            "encoded_bytes": len(body),
            "family": "f16-flat",
            "generation_checksum": list(generation),
            "metric": "cosine",
            "page_ordinal": page_ordinal,
            "path": f"pages/{checksum}",
            "primary_rows": 1,
            "replicated_rows": 1,
        },
        body,
    )


class V23RaBitQConstructionTests(unittest.TestCase):
    def test_default_client_uses_ambient_instance_role_and_explicit_region(self) -> None:
        with mock.patch("boto3.Session") as session_factory:
            client = runner._default_s3_client()

        session_factory.assert_called_once_with(region_name=runner.REGION)
        self.assertIs(client, session_factory.return_value.client.return_value)

    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.binary = self.root / "v23-rabitq-construct"
        self.binary.write_bytes(b"binary")
        self.output = self.root / "published"
        page_pairs = [_page(page) for page in range(8)]
        self.pages = [pair[0] for pair in page_pairs]
        self.page_namespace = "s3://fixture/attempt/pages/"
        roster = _canonical(
            {
                "claim_eligible": False,
                "d1_report_sha256": "8" * 64,
                "dataset_id": "deep-image-96",
                "document_kind": "publication-v3-v23-page-roster",
                "index_id": "fixture-index",
                "page_uri": self.page_namespace.removesuffix("/"),
                "pages": self.pages,
                "schema": "borsuk-v23-pages-v1",
                "source_archive_sha256": "2" * 64,
                "stage": "d2",
            }
        )
        input_payloads = {
            "tree-receipt": b'{"tree":"receipt"}\n',
            "incidence-tree": b"tree",
            "page-roster": roster,
        }
        inputs = [
            _identity(role, f"s3://fixture/input/{role}", input_payloads[role])
            for role in runner.INPUT_ROLES
        ]
        manifest_value = {
            "dataset_id": "deep-image-96",
            "expected_pages": 8,
            "expected_source_occurrences": 16,
            "expected_unique_rows": 8,
            "index_id": "fixture-index",
            "output_roles": list(runner.OUTPUT_ROLES),
            "output_uri_prefix": "s3://fixture/output/construction/",
            "page_namespace_uri_prefix": self.page_namespace,
            "registered_inputs": inputs,
            "rotation_seed_hex": "3" * 64,
            "run_mode": {"execute": "construction"},
            "schema": "borsuk-v23-rabitq-manifest-v1",
            "source_archive_sha256": "2" * 64,
            "source_commit": "1" * 40,
        }
        self.manifest_bytes = _canonical(manifest_value)
        self.manifest = runner.FrozenArtifact(
            role="manifest",
            uri="s3://fixture/input/manifest",
            sha256=hashlib.sha256(self.manifest_bytes).hexdigest(),
            encoded_bytes=len(self.manifest_bytes),
            basename="manifest.json",
        )
        self.d2 = runner.FrozenArtifact(
            role="d2-report",
            uri="s3://fixture/input/d2-report",
            sha256="d" * 64,
            encoded_bytes=4096,
            basename="d2-report.json",
        )
        self.query = runner.FrozenArtifact(
            role="query-parquet",
            uri="s3://fixture/input/query-parquet",
            sha256="e" * 64,
            encoded_bytes=8192,
            basename="query.parquet",
        )
        self.payloads = {self.manifest.uri: self.manifest_bytes} | {
            item["uri"]: input_payloads[item["role"]] for item in inputs
        }
        for reference, body in page_pairs:
            self.payloads[self.page_namespace + reference["path"]] = body

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def test_bvp2_occurrences_preserve_ids_primary_replica_and_arrow_schema(self) -> None:
        reference, body = _page(0)
        occurrences = runner.decode_bvp2_occurrences(reference, body)
        self.assertEqual([row.canonical_record_id for row in occurrences], [b"row-00", b"row-01"])
        self.assertEqual([row.page_ordinal for row in occurrences], [0, 0])
        self.assertEqual([row.is_primary for row in occurrences], [True, False])

        sink = io.BytesIO()
        runner.write_occurrence_stream(sink, [occurrences])
        stream = pa.ipc.open_stream(sink.getvalue())
        self.assertEqual(
            stream.schema,
            pa.schema(
                [
                    pa.field("canonical_record_id", pa.binary(), nullable=False),
                    pa.field(
                        "vector",
                        pa.list_(pa.field("element", pa.float32(), nullable=False), 96),
                        nullable=False,
                    ),
                    pa.field("page_ordinal", pa.uint32(), nullable=False),
                    pa.field("is_primary", pa.bool_(), nullable=False),
                ]
            ),
        )
        self.assertEqual(stream.read_all().num_rows, 2)

    def test_authenticated_historical_roster_keeps_its_distinct_source_archive(self) -> None:
        manifest, _ = runner._manifest_inputs(self.manifest_bytes)
        roster_uri = next(
            item["uri"]
            for item in manifest["registered_inputs"]
            if item["role"] == "page-roster"
        )
        roster = json.loads(self.payloads[roster_uri])
        roster["source_archive_sha256"] = "9" * 64

        pages = runner._read_roster(_canonical(roster), manifest)

        self.assertEqual(len(pages), 8)
        self.assertNotEqual(
            roster["source_archive_sha256"], manifest["source_archive_sha256"]
        )

    def test_exact_roster_pages_feed_one_binary_and_publish_receipt_and_manifest(self) -> None:
        client = FakeS3(self.payloads)
        process = FakeProcess()
        with mock.patch.object(
            runner.subprocess,
            "Popen",
            side_effect=lambda command, **_: process.bind(command),
        ) as popen:
            result = runner.run_construction(
                binary=self.binary,
                manifest=self.manifest,
                d2_report=self.d2,
                query_parquet=self.query,
                development_output_prefix="s3://fixture/output/development/",
                s3_client=client,
                scratch_parent=self.root,
            )

        self.assertFalse(result["claim_eligible"])
        self.assertEqual(len(client.downloads), 4)
        self.assertEqual(len(client.gets), 8)
        self.assertEqual(process.input_table().num_rows, 16)
        self.assertEqual(popen.call_count, 1)
        command = popen.call_args.args[0]
        self.assertIn("--execute-construction", command)
        self.assertNotIn("--query-parquet", command)
        self.assertNotIn("--endpoint", command)
        self.assertNotIn("--d3", command)
        self.assertEqual(
            [name for name, _ in client.uploads],
            [*runner.OUTPUT_BASENAMES.values(), "construction-receipt.json", "development-manifest.json"],
        )
        self.assertEqual(list(self.root.glob("v23-rabitq-construction-*")), [])

    def test_input_digest_drift_stops_before_page_get_or_binary_and_cleans(self) -> None:
        drifted = dict(self.payloads)
        drifted["s3://fixture/input/incidence-tree"] = b"drift"
        client = FakeS3(drifted)
        with mock.patch.object(runner.subprocess, "Popen") as popen:
            with self.assertRaisesRegex(ValueError, "incidence-tree length differs"):
                runner.run_construction(
                    binary=self.binary,
                    manifest=self.manifest,
                    d2_report=self.d2,
                    query_parquet=self.query,
                    development_output_prefix="s3://fixture/output/development/",
                    s3_client=client,
                    scratch_parent=self.root,
                )
        popen.assert_not_called()
        self.assertEqual(client.gets, [])
        self.assertEqual(list(self.root.glob("v23-rabitq-construction-*")), [])

    def test_child_stderr_survives_arrow_broken_pipe_and_cleanup(self) -> None:
        client = FakeS3(self.payloads)
        process = BrokenPipeProcess()
        with mock.patch.object(
            runner.subprocess,
            "Popen",
            side_effect=lambda command, **_: process.bind(command),
        ):
            with self.assertRaisesRegex(RuntimeError, "child authority root cause"):
                runner.run_construction(
                    binary=self.binary,
                    manifest=self.manifest,
                    d2_report=self.d2,
                    query_parquet=self.query,
                    development_output_prefix="s3://fixture/output/development/",
                    s3_client=client,
                    scratch_parent=self.root,
                )

        self.assertEqual(list(self.root.glob("v23-rabitq-construction-*")), [])

    def test_wedged_child_stderr_survives_termination_and_cleanup(self) -> None:
        client = FakeS3(self.payloads)
        process = WedgedBrokenPipeProcess()
        with (
            mock.patch.object(
                runner.subprocess,
                "Popen",
                side_effect=lambda command, **_: process.bind(command),
            ),
            mock.patch.object(runner.os, "killpg") as killpg,
        ):
            with self.assertRaisesRegex(RuntimeError, "buffered child panic"):
                runner.run_construction(
                    binary=self.binary,
                    manifest=self.manifest,
                    d2_report=self.d2,
                    query_parquet=self.query,
                    development_output_prefix="s3://fixture/output/development/",
                    s3_client=client,
                    scratch_parent=self.root,
                )

        killpg.assert_called_once_with(process.pid, signal.SIGTERM)
        self.assertEqual(process.communicate_calls, 2)
        self.assertEqual(list(self.root.glob("v23-rabitq-construction-*")), [])


if __name__ == "__main__":
    unittest.main()
