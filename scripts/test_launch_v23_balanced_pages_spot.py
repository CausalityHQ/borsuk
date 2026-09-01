from __future__ import annotations

import dataclasses
import hashlib
import pathlib
import tempfile
import unittest

from scripts import launch_v23_balanced_pages_spot as subject


class FakeStorage:
    def __init__(self, payloads: dict[str, bytes]) -> None:
        self.payloads = payloads
        self.calls: list[tuple[str, pathlib.Path]] = []

    def download(self, uri: str, destination: pathlib.Path) -> None:
        self.calls.append((uri, destination))
        destination.write_bytes(self.payloads[uri])


class FakeCloud:
    def __init__(self, terminal: str = "complete") -> None:
        self.terminal = terminal
        self.launch_requests: list[subject.SpotLaunchRequest] = []
        self.terminated: list[str] = []

    def launch_spot(self, request: subject.SpotLaunchRequest) -> str:
        self.launch_requests.append(request)
        return "i-balanced-0001"

    def wait_terminal(self, instance_id: str) -> str:
        self.instance_id = instance_id
        return self.terminal

    def terminate(self, instance_id: str) -> None:
        self.terminated.append(instance_id)


def _object(role: str, payload: bytes) -> subject.RegisteredObject:
    return subject.RegisteredObject(
        role=role,
        uri=f"s3://borsuk-v23-eu-west-1/frozen/{role}",
        sha256=hashlib.sha256(payload).hexdigest(),
        encoded_bytes=len(payload),
        basename=f"{role}.bin",
    )


class BalancedSpotLifecycleTests(unittest.TestCase):
    def test_staging_downloads_only_registered_objects_and_reauthenticates(self) -> None:
        first = _object("manifest", b"manifest\n")
        second = _object("f16-control", b"vectors\n")
        storage = FakeStorage({first.uri: b"manifest\n", second.uri: b"vectors\n"})
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            staged = subject.stage_registered_inputs(storage, (first, second), root)
            self.assertEqual(tuple(path.name for path in staged), (first.basename, second.basename))
            self.assertEqual(len(storage.calls), 2)
            self.assertFalse(any(path.name.endswith(".partial") for path in root.iterdir()))
            with self.assertRaisesRegex(ValueError, "digest"):
                subject.stage_registered_inputs(
                    FakeStorage({first.uri: b"drift"}),
                    (first,),
                    root / "drift",
                )

    def test_spot_request_is_interruptible_and_has_no_ondemand_fallback(self) -> None:
        request = subject.SpotLaunchRequest(
            region="eu-west-1",
            ami_id="ami-0123456789abcdef0",
            instance_type="r8g.2xlarge",
            subnet_id="subnet-0123456789abcdef0",
            security_group_ids=("sg-0123456789abcdef0",),
            instance_profile_arn="arn:aws:iam::123456789012:instance-profile/borsuk",
            user_data="#!/bin/sh\nexit 0\n",
        )
        payload = subject.ec2_run_instances_payload(request)
        self.assertEqual(payload["InstanceMarketOptions"]["MarketType"], "spot")
        self.assertNotIn("on-demand", repr(payload).lower())
        self.assertEqual(payload["MinCount"], 1)
        self.assertEqual(payload["MaxCount"], 1)

    def test_every_terminal_and_exception_terminates_the_exact_instance(self) -> None:
        request = subject.SpotLaunchRequest(
            region="eu-west-1",
            ami_id="ami-0123456789abcdef0",
            instance_type="r8g.2xlarge",
            subnet_id="subnet-0123456789abcdef0",
            security_group_ids=("sg-0123456789abcdef0",),
            instance_profile_arn="arn:aws:iam::123456789012:instance-profile/borsuk",
            user_data="#!/bin/sh\nexit 0\n",
        )
        for terminal in ("complete", "quality", "stopped", "failed"):
            cloud = FakeCloud(terminal)
            self.assertEqual(subject.launch_spot_cell(cloud, request), terminal)
            self.assertEqual(cloud.terminated, ["i-balanced-0001"])

        class BrokenCloud(FakeCloud):
            def wait_terminal(self, instance_id: str) -> str:
                raise RuntimeError("monitor failed")

        cloud = BrokenCloud()
        with self.assertRaisesRegex(RuntimeError, "monitor failed"):
            subject.launch_spot_cell(cloud, request)
        self.assertEqual(cloud.terminated, ["i-balanced-0001"])

    def test_launch_request_rejects_region_identity_and_user_data_drift(self) -> None:
        valid = subject.SpotLaunchRequest(
            region="eu-west-1",
            ami_id="ami-0123456789abcdef0",
            instance_type="r8g.2xlarge",
            subnet_id="subnet-0123456789abcdef0",
            security_group_ids=("sg-0123456789abcdef0",),
            instance_profile_arn="arn:aws:iam::123456789012:instance-profile/borsuk",
            user_data="#!/bin/sh\nexit 0\n",
        )
        subject.validate_spot_request(valid)
        for changed in (
            dataclasses.replace(valid, region="us-east-1"),
            dataclasses.replace(valid, instance_type=""),
            dataclasses.replace(valid, user_data=""),
            dataclasses.replace(valid, security_group_ids=()),
        ):
            with self.assertRaises(ValueError):
                subject.validate_spot_request(changed)


if __name__ == "__main__":
    unittest.main()
