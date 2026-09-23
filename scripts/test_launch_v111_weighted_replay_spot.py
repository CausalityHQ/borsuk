from __future__ import annotations

import base64
import hashlib
import json
import pathlib
import subprocess
import unittest
from io import BytesIO

from scripts.launch_v111_weighted_replay_spot import (
    build_launch_specs, build_plan, readback, worker_script,
)


class WeightedReplaySpotTests(unittest.TestCase):
    def setUp(self) -> None:
        self.plan = build_plan("1" * 40, "2" * 64, 100)

    def test_claim_is_fixed_to_one_spot_development_attempt(self) -> None:
        plan = self.plan
        self.assertEqual(plan.profile, "causality")
        self.assertEqual(plan.regions, 1024)
        self.assertEqual(plan.shortlist_rows, 512)
        self.assertEqual(plan.instance_type, "c7i.12xlarge")
        self.assertTrue(plan.output_prefix.endswith("/runs/relaion-1m-dev1000-a0001"))
        script = worker_script(plan)
        self.assertIn("run_v111_weighted_replay_remote.sh", script)
        self.assertIn("V111_ARCHIVE_SHA256", script)
        self.assertIn("V111_SQ8_SHA256", script)
        self.assertIn("V111_ATTEMPT=1", script)

    def test_new_attempt_has_distinct_prefix_and_client_token(self) -> None:
        retry = build_plan("1" * 40, "2" * 64, 100, attempt=2)
        self.assertTrue(retry.output_prefix.endswith("/runs/relaion-1m-dev1000-a0002"))
        self.assertNotEqual(build_launch_specs(retry)[0]["ClientToken"],
                            build_launch_specs(self.plan)[0]["ClientToken"])

    def test_direct_launcher_entrypoint_imports_without_pythonpath(self) -> None:
        path = pathlib.Path(__file__).with_name("launch_v111_weighted_replay_spot.py")
        result = subprocess.run(["python3", str(path), "--help"],
                                capture_output=True, text=True, check=False)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_remote_replay_uses_package_import_path(self) -> None:
        path = pathlib.Path(__file__).with_name("run_v111_weighted_replay_remote.sh")
        runner = path.read_text()
        self.assertEqual(
            runner.count("env PYTHONPATH=repo .venv/bin/python -m scripts.v111_weighted_reader_replay"),
            2,
        )

    def test_launch_is_spot_and_uses_the_new_worker(self) -> None:
        specs = build_launch_specs(self.plan)
        self.assertEqual(len(specs), 3)
        for spec in specs:
            self.assertEqual(spec["InstanceMarketOptions"]["MarketType"], "spot")
            script = base64.b64decode(spec["UserData"]).decode()
            self.assertEqual(script, worker_script(self.plan))
            self.assertEqual(
                spec["TagSpecifications"][0]["Tags"][0]["Value"],
                "borsuk-v111-weighted-replay",
            )

    def test_readback_binds_prefix_decision_to_evidence(self) -> None:
        evidence = b"sample evidence\n"
        evidence_sha = hashlib.sha256(evidence).hexdigest()
        bodies = {
            "hashes.log": b"inputs checked\n",
            "manifest-export.log": b"manifest checked\n",
            "prefix.log": b"prefix checked\n",
            "prefix-evidence.jsonl": evidence,
            "prefix-reduction.json": json.dumps({
                "query_count": 200, "evidence_sha256": evidence_sha,
            }).encode(),
            "prefix-resources.txt": b"resources checked\n",
            "prefix-validation.log": b"validation checked\n",
            "prefix-validation-resources.txt": b"validation resources checked\n",
            "decision.json": json.dumps({
                "schema": "borsuk-v111-decision-v1",
                "prefix_evidence_sha256": evidence_sha,
                "prefix_decision": "stop-weighted-planner",
            }).encode(),
        }
        artifacts = {
            name: {"uri": f"s3://fixture/attempt/artifacts/{name}",
                   "bytes": len(body), "sha256": hashlib.sha256(body).hexdigest()}
            for name, body in bodies.items()
        }

        class FakeS3:
            def get_object(self, *, Bucket: str, Key: str):
                return {"Body": BytesIO(bodies[Key.rsplit("/", 1)[-1]])}

        terminal = {"status": "complete", "exit_code": 0, "artifacts": artifacts}
        self.assertIs(readback(FakeS3(), terminal), terminal)
        artifacts["prefix-evidence.jsonl"]["sha256"] = "0" * 64
        with self.assertRaises(ValueError):
            readback(FakeS3(), terminal)


if __name__ == "__main__":
    unittest.main()
