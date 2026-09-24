"""Pure identity and Spot specification checks for the V124 launcher."""

import base64
import unittest

from scripts.launch_v124_source_tier_precision_spot import (
    INPUTS, Plan, V116_COMMIT, launch_spec, validate_v116_terminal,
)


class V124LauncherTests(unittest.TestCase):
    def test_v116_prerequisite_is_sealed(self) -> None:
        terminal = {
            "schema": "borsuk-v116-validation-paired-spot-v1",
            "source_commit": V116_COMMIT,
            "status": "complete", "phase": "complete", "exit_code": 0,
            "instance_id": "i-old",
            "artifacts": {"requests.jsonl": {
                "sha256": INPUTS["RELAION_REQUESTS"][1],
            }},
        }
        self.assertEqual(validate_v116_terminal(terminal), "i-old")
        terminal["phase"] = "replay"
        with self.assertRaises(ValueError):
            validate_v116_terminal(terminal)

    def test_spot_spec_contains_both_pinned_cohorts(self) -> None:
        commit = "0" * 40
        plan = Plan(commit, "s3://bucket/source.tar.gz", "0" * 64, 1,
                    f"s3://bucket/{commit}/runs/v124-fixed/a0001")
        spec = launch_spec(plan, "eu-central-1c", "subnet-test")
        tags = spec["TagSpecifications"][0]["Tags"]
        self.assertIn({"Key": "Name", "Value":
                       "borsuk-v124-source-tier-precision"}, tags)
        self.assertEqual(spec["InstanceMarketOptions"]["MarketType"], "spot")
        user_data = base64.b64decode(spec["UserData"]).decode()
        self.assertIn("V124_DEEP_SOURCE_SHA256", user_data)
        self.assertIn("V124_RELAION_SOURCE_SHA256", user_data)
        self.assertLessEqual(len(user_data.encode()), 16_384)


if __name__ == "__main__":
    unittest.main()
