"""Frozen V115 composed-replay Spot launch contract."""

import unittest

from scripts.launch_v115_returned_replay_spot import Plan, launch_spec, user_data


class ReplaySpotTests(unittest.TestCase):
    def test_one_spot_worker_with_pinned_inputs_and_terminal(self) -> None:
        commit = "a" * 40
        plan = Plan(commit, f"s3://bucket/v115/{commit}/source.tar.gz",
                    "b" * 64, 123, f"s3://bucket/v115/{commit}/runs/a0001")
        spec = launch_spec(plan, "eu-central-1c", "subnet-test")
        self.assertEqual(spec["InstanceMarketOptions"]["MarketType"], "spot")
        self.assertEqual(spec["InstanceInitiatedShutdownBehavior"], "terminate")
        self.assertEqual(spec["InstanceType"], "c7i.12xlarge")
        script = user_data(plan)
        self.assertIn("run_v115_returned_replay_remote.sh", script)
        for role in ("SQ8", "TRUTH", "MIRROR_MANIFEST", "SIDECAR",
                     "REQUESTS", "ROSTERS", "REFERENCE", "EVIDENCE"):
            self.assertIn(f"V115_{role}_SHA256", script)
        self.assertIn("sha256sum -c -", script)


if __name__ == "__main__":
    unittest.main()
