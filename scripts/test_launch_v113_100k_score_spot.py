"""V113 registration and source-before-query execution invariants."""

import unittest

from scripts.launch_v113_100k_score_spot import (
    Plan, launch_spec, user_data, validate_plan,
)


class V113SpotTests(unittest.TestCase):
    def setUp(self) -> None:
        self.plan = Plan(
            "a" * 40,
            "s3://bucket/source.tar.gz", "b" * 64, 1234,
            "s3://bucket/research/v113/" + "a" * 40 + "/a0001",
        )

    def test_single_spot_registration_and_source_first(self) -> None:
        validate_plan(self.plan)
        script = user_data(self.plan)
        self.assertIn("V113_SOURCE_SHA256", script)
        self.assertIn("V113_QUERY_SHA256", script)
        self.assertNotIn("TRUTH", script)
        spec = launch_spec(self.plan, "eu-central-1c", "subnet-test")
        self.assertEqual(spec["InstanceMarketOptions"]["MarketType"], "spot")
        self.assertEqual(spec["MaxCount"], 1)
        self.assertEqual(spec["InstanceInitiatedShutdownBehavior"], "terminate")

    def test_output_prefix_must_bind_commit(self) -> None:
        with self.assertRaises(ValueError):
            validate_plan(Plan(
                self.plan.source_commit, self.plan.archive_uri,
                self.plan.archive_sha256, self.plan.archive_bytes,
                "s3://bucket/research/v113/unbound/a0001",
            ))


if __name__ == "__main__":
    unittest.main()
