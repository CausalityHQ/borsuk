from __future__ import annotations

import dataclasses
import hashlib
import io
import json
import pathlib
import tempfile
import unittest
from unittest import mock

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


class FakeEc2Client:
    def __init__(self) -> None:
        self.launches: list[dict[str, object]] = []
        self.terminations: list[list[str]] = []

    def run_instances(self, **payload: object) -> dict[str, object]:
        self.launches.append(payload)
        return {"Instances": [{"InstanceId": "i-balanced-0001"}]}

    def terminate_instances(self, *, InstanceIds: list[str]) -> None:  # noqa: N803
        self.terminations.append(InstanceIds)


class FakeS3Client:
    def __init__(self, prefix: str, status: str) -> None:
        self.key = f"{prefix}{status.upper()}.json"
        self.status = status

    def list_objects_v2(self, **_: object) -> dict[str, object]:
        return {"Contents": [{"Key": self.key}]}

    def get_object(self, **_: object) -> dict[str, object]:
        value = {
            "claim_eligible": False,
            "instance_id": "i-balanced-0001",
            "status": self.status,
        }
        body = json.dumps(value, separators=(",", ":"), sort_keys=True).encode() + b"\n"
        return {"Body": io.BytesIO(body)}


class EmptyS3Client:
    def list_objects_v2(self, **_: object) -> dict[str, object]:
        return {}


class TerminatedEc2Client(FakeEc2Client):
    def describe_instances(self, **_: object) -> dict[str, object]:
        return {"Reservations": [{"Instances": [{"State": {"Name": "terminated"}}]}]}


class FakeStsClient:
    def get_caller_identity(self) -> dict[str, str]:
        return {"Account": "453182569524"}


def _object(role: str, payload: bytes) -> subject.RegisteredObject:
    return subject.RegisteredObject(
        role=role,
        uri=f"s3://borsuk-bench-453182569524-euc1/frozen/{role}",
        sha256=hashlib.sha256(payload).hexdigest(),
        encoded_bytes=len(payload),
        basename=f"{role}.bin",
    )


def _remote_plan() -> subject.BalancedRemotePlan:
    def named(role: str, basename: str) -> subject.RegisteredObject:
        artifact = _object(role, f"{role}\n".encode())
        return dataclasses.replace(artifact, basename=basename)

    return subject.BalancedRemotePlan(
        run_id="v23-balanced-pages-0001",
        supervisor=named("offline-supervisor", "run_v23_balanced_page_falsifier.py"),
        executable=named("balanced-executable", "v23-balanced-page-falsifier"),
        manifest=named("balanced-manifest", "manifest.json"),
        ordered_inputs=(
            named("source-shard-manifest", "source-shard-manifest.json"),
            named("f16-control", "f16-control.arrow"),
            named("query-parquet", "query.parquet"),
            named("neighbors-parquet", "neighbors.parquet"),
        ),
        output_prefix=(
            "s3://borsuk-bench-453182569524-euc1/publication/v23/"
            "balanced/attempt-0001/"
        ),
    )


class BalancedSpotLifecycleTests(unittest.TestCase):
    def test_cli_requires_one_canonical_authority_and_explicit_spot(self) -> None:
        path = pathlib.Path("/tmp/balanced-launch.json")
        self.assertEqual(subject.parse_args(["--authority", str(path), "--spot"]), path)
        with mock.patch("sys.stderr"):
            with self.assertRaises(SystemExit):
                subject.parse_args(["--authority", str(path)])
            with self.assertRaises(SystemExit):
                subject.parse_args(
                    ["--authority", str(path), "--spot", "--on-demand"]
                )

    def test_canonical_launch_authority_runs_only_one_account_bound_spot_cell(self) -> None:
        remote = _remote_plan()
        value = {
            "aws_account": "453182569524",
            "claim_eligible": False,
            "monitor": {"poll_seconds": 0, "wall_seconds": 30},
            "profile": "causality",
            "region": "eu-central-1",
            "remote_plan": dataclasses.asdict(remote),
            "schema": "borsuk-v23-balanced-page-spot-authority-v1",
            "spot": {
                "ami_id": "ami-0123456789abcdef0",
                "instance_profile_arn": (
                    "arn:aws:iam::453182569524:instance-profile/borsuk-bench-profile"
                ),
                "instance_type": "r8g.2xlarge",
                "security_group_ids": ["sg-0123456789abcdef0"],
                "subnet_id": "subnet-0123456789abcdef0",
            },
        }
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory) / "launch.json"
            path.write_bytes(
                json.dumps(value, separators=(",", ":"), sort_keys=True).encode()
                + b"\n"
            )
            authority = subject.load_spot_authority(path)
        prefix = "publication/v23/balanced/attempt-0001/"
        ec2 = FakeEc2Client()
        terminal = subject.run_spot_authority(
            authority,
            sts_client=FakeStsClient(),
            ec2_client=ec2,
            s3_client=FakeS3Client(prefix, "quality"),
            sleep=lambda _: None,
        )
        self.assertEqual(terminal, "quality")
        self.assertEqual(len(ec2.launches), 1)
        self.assertEqual(ec2.terminations, [["i-balanced-0001"]])

        changed = dict(value, profile="default")
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory) / "launch.json"
            path.write_bytes(
                json.dumps(changed, separators=(",", ":"), sort_keys=True).encode()
                + b"\n"
            )
            with self.assertRaisesRegex(ValueError, "launch authority"):
                subject.load_spot_authority(path)

    def test_remote_worker_is_direct_exact_offline_and_has_explicit_cleanup(self) -> None:
        plan = _remote_plan()
        worker = subject.build_remote_worker_user_data(plan)
        self.assertTrue(worker.startswith("#!/bin/bash\nset -euo pipefail\n"))
        for artifact in (
            plan.supervisor,
            plan.executable,
            plan.manifest,
            *plan.ordered_inputs,
        ):
            self.assertEqual(worker.count(artifact.uri), 1)
            self.assertIn(artifact.sha256, worker)
            self.assertIn(str(artifact.encoded_bytes), worker)
            self.assertIn(artifact.basename, worker)
        self.assertIn("python3 \"$root/run_v23_balanced_page_falsifier.py\"", worker)
        self.assertIn("\"$root/v23-balanced-page-falsifier\"", worker)
        self.assertIn("--rss-bytes 34359738368", worker)
        self.assertNotIn("pip install", worker)
        self.assertNotIn("ldd", worker)
        self.assertNotIn("mount", worker)
        self.assertNotIn("docker", worker)
        self.assertIn("trap_code=$?", worker)
        self.assertIn('if [ "$trap_code" -ne 0 ]; then status=failed; code=$trap_code; fi', worker)
        for basename in subject.REMOTE_OUTPUT_BASENAMES:
            self.assertIn(basename, worker)
        for artifact in plan.ordered_inputs:
            self.assertIn(
                f'for path in "$root/{artifact.basename}" '
                f'"$input/{artifact.basename}" "$output/{artifact.basename}"',
                worker,
            )

    def test_boto_adapter_launches_one_spot_reads_canonical_terminal_and_terminates(self) -> None:
        prefix = "publication/v23/balanced/attempt-0001/"
        ec2 = FakeEc2Client()
        s3 = FakeS3Client(prefix, "quality")
        cloud = subject.Boto3SpotCloud(
            ec2_client=ec2,
            s3_client=s3,
            terminal_prefix=("borsuk-bench-453182569524-euc1", prefix),
            wall_seconds=30,
            poll_seconds=0,
            sleep=lambda _: None,
        )
        request = subject.SpotLaunchRequest(
            region="eu-central-1",
            ami_id="ami-0123456789abcdef0",
            instance_type="r8g.2xlarge",
            subnet_id="subnet-0123456789abcdef0",
            security_group_ids=("sg-0123456789abcdef0",),
            instance_profile_arn="arn:aws:iam::123456789012:instance-profile/borsuk",
            user_data="#!/bin/bash\nexit 0\n",
        )
        self.assertEqual(subject.launch_spot_cell(cloud, request), "quality")
        self.assertEqual(len(ec2.launches), 1)
        self.assertTrue(subject.payload_is_spot(ec2.launches[0]))
        self.assertEqual(ec2.terminations, [["i-balanced-0001"]])

    def test_boto_adapter_stops_when_instance_terminates_without_terminal_marker(self) -> None:
        prefix = "publication/v23/balanced/attempt-0001/"
        cloud = subject.Boto3SpotCloud(
            ec2_client=TerminatedEc2Client(),
            s3_client=EmptyS3Client(),
            terminal_prefix=("borsuk-bench-453182569524-euc1", prefix),
            wall_seconds=30,
            poll_seconds=0,
            sleep=lambda _: None,
        )

        with self.assertRaisesRegex(RuntimeError, "terminated without terminal marker"):
            cloud.wait_terminal("i-balanced-0001")

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
            region="eu-central-1",
            ami_id="ami-0123456789abcdef0",
            instance_type="r8g.2xlarge",
            subnet_id="subnet-0123456789abcdef0",
            security_group_ids=("sg-0123456789abcdef0",),
            instance_profile_arn="arn:aws:iam::123456789012:instance-profile/borsuk",
            user_data="#!/bin/bash\nexit 0\n",
        )
        payload = subject.ec2_run_instances_payload(request)
        self.assertEqual(payload["InstanceMarketOptions"]["MarketType"], "spot")
        self.assertNotIn("on-demand", repr(payload).lower())
        self.assertEqual(payload["MinCount"], 1)
        self.assertEqual(payload["MaxCount"], 1)
        self.assertRegex(payload["ClientToken"], r"\Av23-balanced-[0-9a-f]{48}\Z")
        self.assertEqual(
            payload["InstanceMarketOptions"]["SpotOptions"][
                "InstanceInterruptionBehavior"
            ],
            "terminate",
        )
        self.assertEqual(payload["BlockDeviceMappings"][0]["Ebs"]["VolumeSize"], 96)
        self.assertTrue(
            payload["BlockDeviceMappings"][0]["Ebs"]["DeleteOnTermination"]
        )

    def test_every_terminal_and_exception_terminates_the_exact_instance(self) -> None:
        request = subject.SpotLaunchRequest(
            region="eu-central-1",
            ami_id="ami-0123456789abcdef0",
            instance_type="r8g.2xlarge",
            subnet_id="subnet-0123456789abcdef0",
            security_group_ids=("sg-0123456789abcdef0",),
            instance_profile_arn="arn:aws:iam::123456789012:instance-profile/borsuk",
            user_data="#!/bin/bash\nexit 0\n",
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
            region="eu-central-1",
            ami_id="ami-0123456789abcdef0",
            instance_type="r8g.2xlarge",
            subnet_id="subnet-0123456789abcdef0",
            security_group_ids=("sg-0123456789abcdef0",),
            instance_profile_arn="arn:aws:iam::123456789012:instance-profile/borsuk",
            user_data="#!/bin/bash\nexit 0\n",
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
