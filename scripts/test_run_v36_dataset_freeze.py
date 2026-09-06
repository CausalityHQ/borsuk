from __future__ import annotations

import base64
import io
import json
import unittest
from unittest import mock

from scripts import run_v36_dataset_freeze as subject


class _AwsError(Exception):
    def __init__(self, code: str) -> None:
        self.response = {"Error": {"Code": code}}


class V36DatasetFreezeLauncherTests(unittest.TestCase):
    def plan(self) -> subject.V36DatasetFreezePlan:
        return subject.build_v36_dataset_freeze_plan(
            run_id="v36-freeze-fixture",
            source_commit="1" * 40,
            source_archive_uri="s3://fixture/v36/source.tar.zst",
            source_archive_sha256="2" * 64,
            source_archive_bytes=8_192,
            binary_uri="s3://fixture/v36/v36_dataset_freeze",
            binary_sha256="3" * 64,
            binary_bytes=4_096,
            authority_uri="s3://fixture/v36/dataset-authority.json",
            authority_sha256="4" * 64,
            authority_bytes=2_048,
            output_prefix="s3://fixture/v36/results/",
        )

    def test_v36_dataset_freeze_is_one_spot_stream_with_bounded_ephemeral_nvme(self) -> None:
        # Break caught: the launcher falls back to on-demand/local corpus
        # persistence, starts multiple workers, or widens the registered cap.
        self.assertEqual(subject.PROFILE, "causality")
        self.assertEqual(subject.REGION, "eu-central-1")
        self.assertEqual(subject.INSTANCE_TYPE, "r8gd.8xlarge")
        self.assertEqual(subject.ACTIVE_WALL_SECONDS, 43_200)
        self.assertEqual(subject.EPHEMERAL_NVME_BYTES, 1_900_000_000_000)
        specs = subject.build_v36_launch_specs(self.plan(), launch_nonce="a" * 32)
        self.assertEqual(len(specs), 3)
        self.assertEqual(len({spec["Placement"]["AvailabilityZone"] for spec in specs}), 3)
        for spec in specs:
            self.assertEqual(spec["MinCount"], 1)
            self.assertEqual(spec["MaxCount"], 1)
            self.assertEqual(spec["InstanceType"], "r8gd.8xlarge")
            self.assertEqual(spec["InstanceMarketOptions"]["MarketType"], "spot")
            self.assertEqual(
                spec["InstanceMarketOptions"]["SpotOptions"],
                {
                    "InstanceInterruptionBehavior": "terminate",
                    "SpotInstanceType": "one-time",
                },
            )
            self.assertEqual(spec["InstanceInitiatedShutdownBehavior"], "terminate")
            script = base64.b64decode(spec["UserData"]).decode()
            self.assertEqual(script.count("--execute-freeze"), 1)
            self.assertIn("--one-source-stream", script)
            self.assertIn("1900000000000", script)
            self.assertIn("timeout --signal=TERM --kill-after=30 43200", script)
            self.assertIn("INTERRUPTED.json", script)
            self.assertIn("ATTEMPT_COMPLETE.json", script)
            self.assertIn("ATTEMPT_FAILED.json", script)
            self.assertIn("s3api put-object", script)
            self.assertIn("--if-none-match '*'", script)
            self.assertIn("shutdown -h now", script)
            self.assertNotIn("on-demand", script.lower())
            self.assertNotIn("dgx", script.lower())

    def test_v36_dataset_freeze_authenticates_terminal_and_always_terminates(self) -> None:
        # Break caught: a worker terminal can drift from its inputs or a
        # terminal/error path leaves paid compute running.
        plan = self.plan()
        terminal = subject.canonical_v36_dataset_terminal_bytes(
            plan,
            instance_id="i-v36-fixture",
            status="complete",
            result_uri="s3://fixture/v36/results/result.json",
            result_sha256="5" * 64,
            result_bytes=1_024,
        )
        self.assertEqual(terminal, subject.canonical_json_bytes(json.loads(terminal)))
        ec2 = mock.Mock()
        ec2.run_instances.return_value = {
            "Instances": [{"InstanceId": "i-v36-fixture"}]
        }
        ec2.describe_instances.return_value = {
            "Reservations": [
                {
                    "Instances": [
                        {
                            "InstanceId": "i-v36-fixture",
                            "State": {"Name": "running"},
                        }
                    ]
                }
            ]
        }
        s3 = mock.Mock()
        s3.get_object.side_effect = [
            _AwsError("NoSuchKey"),
            _AwsError("NoSuchKey"),
            {"Body": io.BytesIO(terminal), "ContentLength": len(terminal)},
        ]
        with mock.patch.object(subject.time, "sleep"):
            uri = subject.run_v36_dataset_freeze(
                plan, ec2_client=ec2, s3_client=s3, launch_nonce="a" * 32
            )
        self.assertEqual(uri, "s3://fixture/v36/results/ATTEMPT_COMPLETE.json")
        ec2.terminate_instances.assert_called_once_with(InstanceIds=["i-v36-fixture"])

        ec2 = mock.Mock()
        ec2.run_instances.side_effect = _AwsError("UnauthorizedOperation")
        absent_s3 = mock.Mock()
        absent_s3.get_object.side_effect = _AwsError("NoSuchKey")
        with self.assertRaises(_AwsError):
            subject.run_v36_dataset_freeze(
                plan, ec2_client=ec2, s3_client=absent_s3, launch_nonce="b" * 32
            )
        ec2.terminate_instances.assert_not_called()

    def test_v36_dataset_freeze_uses_fresh_zone_on_spot_capacity_failure(self) -> None:
        # Break caught: one unavailable Spot zone aborts the campaign or the
        # fallback reuses the same client token/zone.
        plan = self.plan()
        terminal = subject.canonical_v36_dataset_terminal_bytes(
            plan,
            instance_id="i-v36-fallback",
            status="complete",
            result_uri="s3://fixture/v36/results/result.json",
            result_sha256="5" * 64,
            result_bytes=1_024,
        )
        ec2 = mock.Mock()
        ec2.run_instances.side_effect = [
            _AwsError("InsufficientInstanceCapacity"),
            {"Instances": [{"InstanceId": "i-v36-fallback"}]},
        ]
        s3 = mock.Mock()
        s3.get_object.side_effect = [
            _AwsError("NoSuchKey"),
            {"Body": io.BytesIO(terminal), "ContentLength": len(terminal)},
        ]
        with mock.patch.object(subject.time, "sleep"):
            subject.run_v36_dataset_freeze(
                plan, ec2_client=ec2, s3_client=s3, launch_nonce="c" * 32
            )
        self.assertEqual(ec2.run_instances.call_count, 2)
        first, second = [call.kwargs for call in ec2.run_instances.call_args_list]
        self.assertNotEqual(first["Placement"], second["Placement"])
        self.assertNotEqual(first["ClientToken"], second["ClientToken"])
        ec2.terminate_instances.assert_called_once_with(InstanceIds=["i-v36-fallback"])


if __name__ == "__main__":
    unittest.main()
