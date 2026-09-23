"""Frozen V115 source-only router Spot contract."""

import unittest

from scripts.launch_v115_router_parity_spot import Plan, launch_spec, user_data


class RouterSpotTests(unittest.TestCase):
    def test_one_spot_worker_with_source_first_bootstrap(self) -> None:
        commit = "a" * 40
        plan = Plan(
            commit, f"s3://bucket/v115/{commit}/source.tar.gz", "b" * 64, 123,
            f"s3://bucket/v115/{commit}/runs/a0001",
        )
        spec = launch_spec(plan, "eu-central-1c", "subnet-test")
        self.assertEqual(spec["InstanceMarketOptions"]["MarketType"], "spot")
        self.assertEqual(spec["InstanceInitiatedShutdownBehavior"], "terminate")
        self.assertEqual(spec["InstanceType"], "c7i.12xlarge")
        script = user_data(plan)
        self.assertIn("run_v115_router_parity_remote.sh", script)
        self.assertIn("V115_SOURCE_URI", script)
        self.assertIn("V115_SQ8_SHA256", script)
        self.assertIn("V115_REQUESTS_SHA256", script)
        self.assertIn("sha256sum -c -", script)


if __name__ == "__main__":
    unittest.main()
