#!/usr/bin/env python3
"""Behavioral contract for the ephemeral logical-cell Spot launcher."""

from __future__ import annotations

import json
import os
import subprocess
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
LAUNCHER = ROOT / "scripts/launch_aws_logical_cell_routing_spot.sh"


class LaunchAwsLogicalCellRoutingSpotTest(unittest.TestCase):
    def test_dry_run_describes_one_ephemeral_spot_worker_without_aws_mutation(
        self,
    ) -> None:
        result = subprocess.run(
            ["bash", str(LAUNCHER)],
            cwd=ROOT,
            env={
                **os.environ,
                "BORSUK_LAUNCH_DRY_RUN": "1",
                "BORSUK_ROUTING_RUN_ID": "fixture-run",
            },
            check=True,
            capture_output=True,
            text=True,
        )
        plan = json.loads(result.stdout)
        self.assertEqual(plan["profile"], "causality")
        self.assertEqual(plan["region"], "eu-central-1")
        self.assertEqual(plan["instance_type"], "c7g.8xlarge")
        self.assertEqual(plan["purchase_option"], "spot")
        self.assertEqual(plan["instance_count"], 1)
        self.assertEqual(plan["instance_initiated_shutdown_behavior"], "terminate")
        self.assertTrue(plan["root_volume_delete_on_termination"])
        self.assertEqual(plan["campaign_timeout_seconds"], 21_600)
        self.assertEqual(plan["clone_timeout_seconds"], 60)
        self.assertEqual(plan["systemd_timeout_stop_seconds"], 120)
        self.assertTrue(plan["independent_shutdown_deadline"])
        self.assertEqual(plan["campaign"], "logical-cell-routing-positioned-v12-v1")
        self.assertEqual(plan["run_id"], "fixture-run")
        self.assertTrue(plan["result_uri"].endswith("/fixture-run/results"))
        self.assertTrue(plan["index_uri"].endswith("/fixture-run/index"))


if __name__ == "__main__":
    unittest.main()
