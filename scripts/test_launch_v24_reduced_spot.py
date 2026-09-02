import base64
import io
import json
import unittest
from unittest import mock

from scripts import launch_v24_reduced_spot as subject


class _CapacityError(Exception):
    def __init__(self, code: str) -> None:
        self.response = {"Error": {"Code": code}}


class _AbsentError(Exception):
    def __init__(self) -> None:
        self.response = {"Error": {"Code": "NoSuchKey"}}


class V24ReducedSpotTests(unittest.TestCase):
    def plan(self) -> subject.ReducedSpotPlan:
        return subject.build_plan(
            run_id="v24-reduced-fixture",
            source_commit="1" * 40,
            source_archive_uri="s3://fixture/source.tar.zst",
            source_archive_sha256="2" * 64,
            source_archive_bytes=8192,
            binary_uri="s3://fixture/v24_witness_page_router",
            binary_sha256="3" * 64,
            binary_bytes=4096,
            output_prefix="s3://fixture/results/v24-reduced-fixture/",
        )

    def test_launch_specs_are_spot_only_multi_zone_and_content_bound(self) -> None:
        plan = self.plan()
        specs = subject.build_launch_specs(plan)
        self.assertEqual(len(specs), 3)
        self.assertEqual(
            [spec["SubnetId"] for spec in specs],
            [target.subnet_id for target in subject.SPOT_TARGETS],
        )
        self.assertEqual(len({spec["ClientToken"] for spec in specs}), 3)
        for spec in specs:
            self.assertEqual(spec["InstanceMarketOptions"]["MarketType"], "spot")
            self.assertEqual(
                spec["InstanceMarketOptions"]["SpotOptions"][
                    "InstanceInterruptionBehavior"
                ],
                "terminate",
            )
            self.assertEqual(spec["InstanceInitiatedShutdownBehavior"], "terminate")
            self.assertEqual(spec["BlockDeviceMappings"][0]["Ebs"]["VolumeSize"], 500)

    def test_worker_runs_direct_static_driver_and_conditionally_publishes(self) -> None:
        script = base64.b64decode(
            subject.build_launch_specs(self.plan())[0]["UserData"]
        ).decode()
        self.assertIn("sha256sum --check", script)
        self.assertIn("scripts.run_v24_reduced_preflight", script)
        self.assertIn("--execute-reduced-preflight", script)
        self.assertIn("--if-none-match '*'", script)
        self.assertLess(
            script.index("worker.log\nlog_published=1"), script.index("COMPLETE.json")
        )
        self.assertIn("shutdown --poweroff +150", script)
        self.assertIn("trap - EXIT\n  set +e", script)
        self.assertEqual(script.count('encode()+b"\\n")'), 2)
        self.assertIn("RAYON_NUM_THREADS", script)
        self.assertNotIn("ldd", script)
        self.assertNotIn("mount ", script)
        self.assertNotIn("page-body", script)
        self.assertNotIn("d3", script.lower())

    def test_capacity_fallback_stops_after_first_launched_instance_and_terminates(
        self,
    ) -> None:
        ec2 = mock.Mock()
        ec2.run_instances.side_effect = [
            _CapacityError("InsufficientInstanceCapacity"),
            {"Instances": [{"InstanceId": "i-fixture"}]},
        ]
        s3 = mock.Mock()
        terminal = subject.canonical_terminal_bytes(
            self.plan(),
            instance_id="i-fixture",
            status="complete",
            preflight_receipt_sha256="5" * 64,
            preflight_receipt_bytes=512,
        )
        s3.get_object.side_effect = [
            _AbsentError(),
            _AbsentError(),
            _AbsentError(),
            {"Body": io.BytesIO(terminal), "ContentLength": len(terminal)},
        ]
        ec2.describe_instances.return_value = {
            "Reservations": [{"Instances": [{"State": {"Name": "running"}}]}]
        }
        with mock.patch.object(subject.time, "sleep"):
            terminal_uri = subject.run_spot(self.plan(), ec2_client=ec2, s3_client=s3)
        self.assertEqual(
            terminal_uri, "s3://fixture/results/v24-reduced-fixture/COMPLETE.json"
        )
        self.assertEqual(ec2.run_instances.call_count, 2)
        ec2.terminate_instances.assert_called_once_with(InstanceIds=["i-fixture"])

    def test_noncapacity_launch_error_never_falls_through_to_another_zone(self) -> None:
        ec2 = mock.Mock()
        ec2.run_instances.side_effect = _CapacityError("UnauthorizedOperation")
        s3 = mock.Mock()
        s3.get_object.side_effect = _AbsentError()
        with self.assertRaises(_CapacityError):
            subject.run_spot(self.plan(), ec2_client=ec2, s3_client=s3)
        self.assertEqual(ec2.run_instances.call_count, 1)
        ec2.terminate_instances.assert_not_called()

    def test_existing_terminal_or_mutated_terminal_is_rejected(self) -> None:
        plan = self.plan()
        stale = subject.canonical_terminal_bytes(
            plan,
            instance_id="i-stale",
            status="complete",
            preflight_receipt_sha256="5" * 64,
            preflight_receipt_bytes=512,
        )
        s3 = mock.Mock()
        s3.get_object.return_value = {
            "Body": io.BytesIO(stale),
            "ContentLength": len(stale),
        }
        ec2 = mock.Mock()
        with self.assertRaisesRegex(ValueError, "already exists"):
            subject.run_spot(plan, ec2_client=ec2, s3_client=s3)
        ec2.run_instances.assert_not_called()

        value = json.loads(stale)
        value["source_commit"] = "4" * 40
        mutated = subject.canonical_json_bytes(value)
        with self.assertRaisesRegex(ValueError, "terminal"):
            subject.validate_terminal_bytes(mutated, plan, "complete")


if __name__ == "__main__":
    unittest.main()
