"""Focused 1M Spot launch contract tests; no AWS requests."""

import unittest

from scripts.launch_v114_1m_paired_spot import Plan, launch_spec, user_data


class PairedOneMillionLaunchTests(unittest.TestCase):
    def setUp(self) -> None:
        self.plan = Plan(
            source_commit="a" * 40,
            archive_uri="s3://example/research/v114/" + "a" * 40 + "/source.tar.gz",
            archive_sha256="b" * 64,
            archive_bytes=1234,
            output_prefix="s3://example/research/v114/" + "a" * 40 + "/runs/a0001",
        )

    def test_spot_contract_and_source_only_order(self) -> None:
        spec = launch_spec(self.plan, "eu-central-1c", "subnet-test")
        self.assertEqual(spec["InstanceMarketOptions"]["MarketType"], "spot")
        self.assertEqual(spec["InstanceType"], "c7i.12xlarge")
        self.assertEqual(spec["InstanceInitiatedShutdownBehavior"], "terminate")
        self.assertGreaterEqual(spec["BlockDeviceMappings"][0]["Ebs"]["VolumeSize"], 100)
        script = user_data(self.plan)
        self.assertIn("V114_SOURCE_URI", script)
        self.assertIn("V114_QUERIES_URI", script)
        self.assertIn("run_v114_1m_paired_remote.sh", script)
        self.assertIn("source.tar.gz", script)


if __name__ == "__main__":
    unittest.main()
