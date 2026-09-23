"""Focused block-authentication tests for independent 1M validation."""

import hashlib
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from scripts.launch_bounded_reader_1m_spot import ObjectIdentity
from scripts import validate_v114_1m_paired
from scripts.validate_v114_1m_paired import validate_block_sidecar


class IndependentValidatorTests(unittest.TestCase):
    def test_frozen_input_identity_loop_reaches_manifest_check(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "input"
            path.write_bytes(b"test")
            identity = ObjectIdentity("s3://example/input", hashlib.sha256(b"test").hexdigest(), 4)
            inputs = {role: identity for role in ("SOURCE", "QUERIES", "TRUTH", "LAYOUT", "SQ8")}
            with patch.object(validate_v114_1m_paired, "INPUTS", inputs), \
                    patch.object(validate_v114_1m_paired, "load_frozen_1m",
                                 side_effect=RuntimeError("reached manifest")):
                with self.assertRaisesRegex(RuntimeError, "reached manifest"):
                    validate_v114_1m_paired.validate(
                        source=path, queries=path, truth=path, layout=path,
                        sq8_path=path, manifest_path=path, mirror=Path(directory),
                        requests_path=path, reference_path=path, rust_path=path,
                        evidence_path=path, summary_path=path, output_path=path,
                        query_count=200,
                    )

    def test_checks_every_block_and_rejects_a_mutation(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            object_path = root / "sq8.bin"
            sidecar_path = root / "blocks.sha256"
            data = b"a" * 4096 + b"b" * 3
            object_path.write_bytes(data)
            sidecar_path.write_bytes(
                hashlib.sha256(data[:4096]).digest()
                + hashlib.sha256(data[4096:]).digest()
            )
            validate_block_sidecar(object_path, sidecar_path, len(data))
            object_path.write_bytes(b"z" + data[1:])
            with self.assertRaisesRegex(ValueError, "block"):
                validate_block_sidecar(object_path, sidecar_path, len(data))


if __name__ == "__main__":
    unittest.main()
