#!/usr/bin/env python3

import csv
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

from validate_object_store_floor import ValidationError, validate


class ObjectStoreFloorValidatorTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.manifest = self.root / "frozen.json"
        manifest = {
            "architecture": "local",
            "instance_type": "local",
            "region": "local",
            "samples": 4,
            "warmups": 1,
            "payload_bytes": 16,
            "head_bytes": 16,
            "operations": [
                "put",
                "conditional_create",
                "conditional_update",
                "put_then_conditional_update",
            ],
            "proceed_max_chained_p95_ms": 160.0,
            "unreachable_min_chained_p95_ms": 200.0,
        }
        frozen = json.dumps(manifest, sort_keys=True).encode()
        self.manifest_hash = hashlib.sha256(frozen).hexdigest()
        self.manifest_hash_patch = mock.patch(
            "validate_object_store_floor.EXPECTED_MANIFEST_SHA256", self.manifest_hash
        )
        self.manifest_hash_patch.start()
        self.manifest.write_bytes(frozen)
        (self.root / "manifest.json").write_bytes(frozen)
        (self.root / "OBJECT_STORE_FLOOR_COMPLETE").touch()
        (self.root / "environment.txt").write_text(
            "source_sha256=" + "a" * 64 + "\n"
            f"manifest_sha256={self.manifest_hash}\n"
            "architecture=local\ninstance_type=local\n",
            encoding="utf-8",
        )
        measurement = self.root / "measurement"
        measurement.mkdir()
        (measurement / "protocol.txt").write_text(
            "samples=4\nwarmups=1\npayload_bytes=16\nhead_bytes=16\n"
            "uri=s3://test-bucket/floor\nregion=local\n"
            "conditional_create_rejected=true\nstale_update_rejected=true\n",
            encoding="utf-8",
        )
        with (measurement / "samples.csv").open("w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerow(("operation", "sample", "latency_ms"))
            for operation in manifest["operations"]:
                for sample, latency in enumerate((10.0, 20.0, 30.0, 40.0)):
                    writer.writerow((operation, sample, latency))
        with (measurement / "summary.csv").open("w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerow(("operation", "samples", "p50_ms", "p95_ms", "p99_ms", "max_ms"))
            for operation in manifest["operations"]:
                writer.writerow((operation, 4, 30.0, 40.0, 40.0, 40.0))

    def tearDown(self) -> None:
        self.manifest_hash_patch.stop()
        self.temp.cleanup()

    def test_terminal_floor_is_reconciled_and_classified(self) -> None:
        self.assertEqual(validate(self.root, self.manifest), "proceed")

    def test_failure_marker_fails_closed_before_measurements(self) -> None:
        (self.root / "OBJECT_STORE_FLOOR_FAILED").touch()
        with self.assertRaisesRegex(ValidationError, "failure marker"):
            validate(self.root, self.manifest)

    def test_raw_sample_drift_is_rejected(self) -> None:
        samples = self.root / "measurement" / "samples.csv"
        lines = samples.read_text(encoding="utf-8").splitlines()
        samples.write_text("\n".join(lines[:-1]) + "\n", encoding="utf-8")
        with self.assertRaisesRegex(ValidationError, "sample count"):
            validate(self.root, self.manifest)


if __name__ == "__main__":
    unittest.main()
