import base64
import unittest
from unittest import mock

from scripts import launch_v23_rabitq_spot as launcher


class V23RaBitQSpotLauncherTests(unittest.TestCase):
    def test_launch_spec_is_spot_only_and_binds_immutable_execution(self) -> None:
        plan = launcher.build_launch_plan(
            run_id="v23-rabitq-fixture",
            source_commit="1" * 40,
            source_archive_sha256="2" * 64,
            source_archive_uri="s3://fixture/source.tar.zst",
            source_archive_bytes=8192,
            binary_uri="s3://fixture/binary",
            binary_sha256="3" * 64,
            binary_bytes=4096,
            manifest_uri="s3://fixture/manifest",
            manifest_sha256="4" * 64,
            manifest_bytes=2048,
            output_prefix="s3://fixture/terminal/v23-rabitq-fixture/",
        )
        spec = launcher.build_launch_spec(plan)
        self.assertEqual(spec["InstanceMarketOptions"]["MarketType"], "spot")
        self.assertEqual(
            spec["InstanceMarketOptions"]["SpotOptions"]["InstanceInterruptionBehavior"],
            "terminate",
        )
        user_data = base64.b64decode(spec["UserData"]).decode()
        self.assertIn("--execute-development", user_data)
        self.assertIn(plan.manifest_sha256, user_data)
        self.assertIn(plan.binary_sha256, user_data)
        self.assertIn("/proc/pressure/memory", user_data)
        self.assertIn("/proc/meminfo", user_data)
        self.assertIn("rss-limit", user_data)
        self.assertIn("psi-limit", user_data)
        self.assertIn("swap-growth-limit", user_data)
        self.assertIn("progress-limit", user_data)
        self.assertIn("wall-limit", user_data)
        self.assertIn("kill -TERM -- -$pgid", user_data)
        self.assertNotIn("holdout", user_data)
        self.assertNotIn("d3", user_data.lower())

    def test_idempotent_token_and_terminal_prefix_bind_every_immutable_input(self) -> None:
        values = dict(
            run_id="v23-rabitq-fixture",
            source_commit="1" * 40,
            source_archive_sha256="2" * 64,
            source_archive_uri="s3://fixture/source.tar.zst",
            source_archive_bytes=8192,
            binary_uri="s3://fixture/binary",
            binary_sha256="3" * 64,
            binary_bytes=4096,
            manifest_uri="s3://fixture/manifest",
            manifest_sha256="4" * 64,
            manifest_bytes=2048,
            output_prefix="s3://fixture/terminal/v23-rabitq-fixture/",
        )
        left = launcher.build_launch_plan(**values)
        right = launcher.build_launch_plan(**values)
        self.assertEqual(left.client_token, right.client_token)
        self.assertEqual(left, right)
        changed = launcher.build_launch_plan(**(values | {"manifest_sha256": "5" * 64}))
        self.assertNotEqual(left.client_token, changed.client_token)

    def test_instance_is_terminated_immediately_after_terminal_receipt(self) -> None:
        plan = launcher.build_launch_plan(
            run_id="v23-rabitq-fixture",
            source_commit="1" * 40,
            source_archive_sha256="2" * 64,
            source_archive_uri="s3://fixture/source.tar.zst",
            source_archive_bytes=8192,
            binary_uri="s3://fixture/binary",
            binary_sha256="3" * 64,
            binary_bytes=4096,
            manifest_uri="s3://fixture/manifest",
            manifest_sha256="4" * 64,
            manifest_bytes=2048,
            output_prefix="s3://fixture/terminal/v23-rabitq-fixture/",
        )
        ec2 = mock.Mock()
        ec2.run_instances.return_value = {"Instances": [{"InstanceId": "i-fixture"}]}
        s3 = mock.Mock()
        s3.head_object.side_effect = [Exception("not ready"), {"ContentLength": 64}]
        with mock.patch.object(launcher.time, "sleep"):
            terminal = launcher.run_spot(plan, ec2_client=ec2, s3_client=s3)
        self.assertEqual(terminal, "s3://fixture/terminal/v23-rabitq-fixture/COMPLETE.json")
        ec2.terminate_instances.assert_called_once_with(InstanceIds=["i-fixture"])


if __name__ == "__main__":
    unittest.main()
