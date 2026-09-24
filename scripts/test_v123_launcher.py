"""V123 accepts only the sealed failed-quality V121 result and one Spot cell."""

import unittest
from unittest import mock

from scripts.launch_v123_rerank_diagnostic_spot import (
    INDEX_TERMINAL_SHA256, REQUESTS_SHA256, REPLAY_SHA256,
    V121_COMMIT, Plan, launch_spec, reserve_attempt, validate_retired_instance,
    validate_v121_terminal,
)


class V123LauncherTests(unittest.TestCase):
    def test_v121_prerequisite_requires_sealed_complete_replay(self) -> None:
        terminal = {
            "schema": "borsuk-v121-deep-image-paired-spot-v1",
            "source_commit": V121_COMMIT,
            "index_terminal_sha256": INDEX_TERMINAL_SHA256,
            "instance_id": "i-08a10e7cfab9cf985",
            "status": "complete", "phase": "complete", "exit_code": 0,
            "artifacts": {
                "requests.jsonl": {"sha256": REQUESTS_SHA256},
                "rust-replay.jsonl": {"sha256": REPLAY_SHA256},
            },
        }
        self.assertEqual(validate_v121_terminal(terminal), terminal["instance_id"])
        terminal["phase"] = "replay"
        with self.assertRaises(ValueError):
            validate_v121_terminal(terminal)

    def test_spot_spec_names_v123_and_attempt(self) -> None:
        commit = "0" * 40
        plan = Plan(commit, "s3://bucket/source.tar.gz", "0" * 64, 1,
                    f"s3://bucket/{commit}/runs/v123-fixed/a0001")
        spec = launch_spec(plan, "eu-central-1c", "subnet-test")
        tags = spec["TagSpecifications"][0]["Tags"]
        self.assertIn({"Key": "Name", "Value": "borsuk-v123-rerank-diagnostic"}, tags)
        self.assertIn({"Key": "BorsukAttempt", "Value": "a0001"}, tags)
        self.assertEqual(spec["InstanceMarketOptions"]["MarketType"], "spot")

    def test_aged_out_prerequisite_is_allowed_but_running_is_rejected(self) -> None:
        validate_retired_instance({"Reservations": []}, "i-old")
        validate_retired_instance({"Reservations": [{"Instances": [
            {"InstanceId": "i-old", "State": {"Name": "terminated"}},
        ]}]}, "i-old")
        with self.assertRaises(ValueError):
            validate_retired_instance({"Reservations": [{"Instances": [
                {"InstanceId": "i-old", "State": {"Name": "running"}},
            ]}]}, "i-old")

    def test_reservation_uses_conditional_put(self) -> None:
        with mock.patch("subprocess.run") as run:
            reserve_attempt("bucket", "attempt/reservation.json", b"{}\n")
        args = run.call_args.args[0]
        self.assertEqual(args[args.index("--if-none-match") + 1], "*")
        self.assertEqual(args[args.index("--key") + 1],
                         "attempt/reservation.json")


if __name__ == "__main__":
    unittest.main()
