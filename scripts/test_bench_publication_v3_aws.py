import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path

from scripts.publication_v3_protocol import canonical_json_bytes
from scripts.test_publication_v3_protocol import paid_ready_v3_manifest

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "bench_publication_v3_aws.sh"


class BenchPublicationV3AwsTests(unittest.TestCase):
    def test_dry_run_writes_one_exact_protocol_per_scheduled_cell(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            temp = Path(directory)
            manifest = temp / "manifest.json"
            output = temp / "run"
            manifest.write_bytes(canonical_json_bytes(paid_ready_v3_manifest()) + b"\n")
            completed = subprocess.run(
                ["bash", str(SCRIPT)],
                cwd=ROOT,
                env={
                    **os.environ,
                    "BORSUK_PUBLICATION_V3_MANIFEST": str(manifest),
                    "BORSUK_PUBLICATION_V3_ROOT": str(output),
                },
                capture_output=True,
                text=True,
            )
            self.assertEqual(completed.returncode, 0, completed.stderr)
            schedule = json.loads((output / "schedule.json").read_text())
            protocols = sorted((output / "cells").glob("*/protocol.json"))
            self.assertEqual(len(protocols), len(schedule["cells"]))
            by_id = {cell["cell_id"]: cell for cell in schedule["cells"]}
            for path in protocols:
                self.assertEqual(
                    path.read_bytes(),
                    canonical_json_bytes(by_id[path.parent.name]) + b"\n",
                )
            self.assertFalse((output / "PUBLICATION_V3_COMPLETE").exists())

    def test_execute_mode_fails_closed_until_workloads_are_implemented(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            temp = Path(directory)
            manifest = temp / "manifest.json"
            manifest.write_bytes(canonical_json_bytes(paid_ready_v3_manifest()) + b"\n")
            completed = subprocess.run(
                ["bash", str(SCRIPT)],
                cwd=ROOT,
                env={
                    **os.environ,
                    "BORSUK_PUBLICATION_V3_MANIFEST": str(manifest),
                    "BORSUK_PUBLICATION_V3_ROOT": str(temp / "run"),
                    "BORSUK_PUBLICATION_V3_EXECUTE": "1",
                },
                capture_output=True,
                text=True,
            )
            self.assertEqual(completed.returncode, 2)
            self.assertIn("workload execution is unavailable", completed.stderr)


if __name__ == "__main__":
    unittest.main()
