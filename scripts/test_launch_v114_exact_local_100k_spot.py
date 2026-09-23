"""Spot attempt specification for the V114 100k correctness gate."""

import base64
import unittest

from scripts.launch_v114_exact_local_100k_spot import Plan, launch_spec


class V114SpotPlanTests(unittest.TestCase):
    def test_launch_is_one_interruptible_terminal_bound_frozen_attempt(self) -> None:
        commit = "a" * 40
        plan = Plan(
            source_commit=commit,
            archive_uri="s3://bucket/source.tar.gz",
            archive_sha256="b" * 64,
            archive_bytes=123,
            output_prefix=f"s3://bucket/research/v114/{commit}/runs/cell/a0001",
        )
        spec = launch_spec(plan, "eu-central-1a", "subnet-abc")
        self.assertEqual(spec["InstanceMarketOptions"]["MarketType"], "spot")
        self.assertEqual(spec["InstanceInitiatedShutdownBehavior"], "terminate")
        self.assertEqual(spec["MaxCount"], 1)
        bootstrap = base64.b64decode(spec["UserData"]).decode()
        self.assertIn("run_v114_exact_local_100k_remote.sh", bootstrap)
        self.assertIn("V114_V113_SEAL_SHA256", bootstrap)
        self.assertIn("queries.parquet", bootstrap)


if __name__ == "__main__":
    unittest.main()
