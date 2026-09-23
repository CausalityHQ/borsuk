from __future__ import annotations

import base64
import pathlib
import subprocess
import unittest

from scripts.launch_v110_physical_oracle_spot import (
    build_launch_specs, build_plan, worker_script,
)


class PhysicalOracleSpotTests(unittest.TestCase):
    def test_frozen_spot_plan_uses_oracle_worker(self) -> None:
        plan = build_plan("1" * 40, "2" * 64, 100)
        self.assertEqual(plan.instance_type, "c7i.8xlarge")
        self.assertEqual(plan.profile, "causality")
        self.assertTrue(plan.output_prefix.endswith("/runs/relaion-1m-dev1000-a0001"))
        script = worker_script(plan)
        self.assertIn("run_v110_physical_oracle_remote.sh", script)
        self.assertIn("V110_TRUTH_SHA256", script)
        for spec in build_launch_specs(plan):
            self.assertEqual(spec["InstanceMarketOptions"]["MarketType"], "spot")
            self.assertEqual(base64.b64decode(spec["UserData"]).decode(), script)

    def test_direct_entrypoint_and_retry_identity(self) -> None:
        path = pathlib.Path(__file__).with_name("launch_v110_physical_oracle_spot.py")
        result = subprocess.run(["python3", str(path), "--help"],
                                capture_output=True, text=True, check=False)
        self.assertEqual(result.returncode, 0, result.stderr)
        first = build_plan("1" * 40, "2" * 64, 100, attempt=1)
        retry = build_plan("1" * 40, "2" * 64, 100, attempt=2)
        self.assertNotEqual(first.output_prefix, retry.output_prefix)
        self.assertNotEqual(build_launch_specs(first)[0]["ClientToken"],
                            build_launch_specs(retry)[0]["ClientToken"])


if __name__ == "__main__":
    unittest.main()
