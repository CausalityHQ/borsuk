from __future__ import annotations

import pathlib
import subprocess
import unittest
from io import BytesIO
from types import SimpleNamespace

from scripts.launch_bounded_reader_1m_spot import (
    BoundedReaderSpotPlan,
    ObjectIdentity,
    SpotTarget,
    build_launch_specs,
    build_plan,
    claim_launched_attempt,
    claim_reserved_attempt,
    launch_one_spot,
    monitor_and_terminate,
    worker_script,
)


class BoundedReaderSpotLauncherTests(unittest.TestCase):
    @staticmethod
    def plan() -> BoundedReaderSpotPlan:
        return build_plan(
            profile="causality",
            source_commit="1" * 40,
            source_archive=ObjectIdentity("s3://fixture/source.tar.gz", "2" * 64, 100),
            source=ObjectIdentity("s3://fixture/source.parquet", "3" * 64, 101),
            queries=ObjectIdentity("s3://fixture/queries.parquet", "4" * 64, 102),
            truth=ObjectIdentity("s3://fixture/truth.parquet", "5" * 64, 103),
            layout=ObjectIdentity("s3://fixture/layout.npy", "6" * 64, 104),
            sq8=ObjectIdentity("s3://fixture/sq8.bin", "7" * 64, 780_000_000),
            output_prefix="s3://fixture/bounded-reader/a0001",
            image_id="ami-fixture",
            security_group_id="sg-fixture",
            instance_profile_arn="arn:aws:iam::123456789012:instance-profile/fixture",
            targets=(
                SpotTarget("eu-central-1a", "subnet-a"),
                SpotTarget("eu-central-1b", "subnet-b"),
            ),
            spot_price_usd_per_hour_micros=1_000_000,
        )

    def test_plan_freezes_one_full_development_attempt_and_reader_operating_point(self) -> None:
        plan = self.plan()
        self.assertEqual(plan.instance_type, "c7i.12xlarge")
        self.assertEqual(plan.attempt, 1)
        self.assertEqual(plan.query_count, 1_000)
        self.assertEqual(plan.regions, 256)
        self.assertEqual(plan.shortlist_rows, 512)
        self.assertEqual(plan.gap_pages, 2)
        self.assertEqual(plan.get_concurrency, 128)
        self.assertEqual(plan.virtual_memory_kib, 48 * 1024 * 1024)
        self.assertEqual(plan.sq8.bytes, 780_000_000)

    def test_worker_authenticates_all_inputs_and_publishes_terminal_last(self) -> None:
        script = worker_script(self.plan())
        runner = pathlib.Path(__file__).with_name("run_bounded_reader_1m_remote.sh").read_text()
        combined = script + runner
        for identity in (
            self.plan().source_archive,
            self.plan().source,
            self.plan().queries,
            self.plan().truth,
            self.plan().layout,
            self.plan().sq8,
        ):
            self.assertIn(identity.uri, script)
            self.assertIn(identity.sha256, script)
            self.assertIn(str(identity.bytes), script)
        for literal in (
            "BORSUK_V71_QUERIES=1000",
            "BORSUK_V71_REGIONS=256",
            "BORSUK_V71_SHORTLIST=512",
            "BORSUK_V71_GAP=2",
            "BORSUK_V71_CONCURRENCY=128",
            "BORSUK_V71_THROUGHPUT=1",
            "ulimit -v \"$BOUNDED_VIRTUAL_MEMORY_KIB\"",
            "sha256sum -c sq8.sha256",
            "/usr/bin/time -v",
            "/proc/pressure/memory",
            "full avg10",
            "0.50",
            "interrupt-stop",
            "latest/api/token",
            "X-aws-ec2-metadata-token",
            "swap-stop",
            "result.json",
            "samples.parquet",
            "reduction.json",
            "resources.txt",
            "terminal.json",
            "cost_usd_micros",
        ):
            self.assertIn(literal, combined)
        self.assertLess(runner.index("reduction.json"), runner.rindex("terminal.json"))
        self.assertLess(runner.rindex("terminal.json"), runner.rindex("shutdown -h now"))

    def test_launch_specs_are_serial_one_time_spot_and_self_terminating(self) -> None:
        specs = build_launch_specs(self.plan())
        self.assertEqual(len(specs), 2)
        for spec in specs:
            self.assertEqual(spec["InstanceType"], "c7i.12xlarge")
            self.assertEqual((spec["MinCount"], spec["MaxCount"]), (1, 1))
            self.assertEqual(spec["InstanceInitiatedShutdownBehavior"], "terminate")
            self.assertEqual(
                spec["InstanceMarketOptions"],
                {
                    "MarketType": "spot",
                    "SpotOptions": {
                        "InstanceInterruptionBehavior": "terminate",
                        "SpotInstanceType": "one-time",
                    },
                },
            )

    def test_capacity_fallback_launches_exactly_one_instance(self) -> None:
        class FakeEc2:
            def __init__(self) -> None:
                self.calls = 0

            def run_instances(self, **_request: object) -> dict[str, object]:
                self.calls += 1
                if self.calls == 1:
                    raise RuntimeError("InsufficientInstanceCapacity")
                return {"Instances": [{"InstanceId": "i-bounded"}]}

        client = FakeEc2()
        self.assertEqual(launch_one_spot(self.plan(), ec2_client=client), "i-bounded")
        self.assertEqual(client.calls, 2)

    def test_attempt_claims_are_create_only_and_bind_launched_instance(self) -> None:
        class Events:
            def __init__(self) -> None:
                self.handlers: dict[str, object] = {}

            def register_first(self, _name: str, handler: object, *, unique_id: str) -> None:
                self.handlers[unique_id] = handler

            def unregister(self, _name: str, *, unique_id: str) -> None:
                self.handlers.pop(unique_id)

        class FakeS3:
            def __init__(self) -> None:
                self.meta = SimpleNamespace(events=Events())
                self.requests: list[dict[str, object]] = []

            def put_object(self, **request: object) -> None:
                signed = SimpleNamespace(headers={})
                next(iter(self.meta.events.handlers.values()))(signed)
                request["headers"] = signed.headers
                self.requests.append(request)

        client = FakeS3()
        claim_reserved_attempt(self.plan(), s3_client=client)
        claim_launched_attempt(self.plan(), s3_client=client, instance_id="i-bounded")
        self.assertEqual(len(client.requests), 2)
        self.assertTrue(
            all(request["headers"]["If-None-Match"] == "*" for request in client.requests)
        )
        self.assertEqual(
            __import__("json").loads(client.requests[1]["Body"])["instance_id"],
            "i-bounded",
        )

    def test_monitor_returns_bound_terminal_and_always_terminates_instance(self) -> None:
        class FakeS3:
            def get_object(self, **_request: object) -> dict[str, object]:
                return {
                    "Body": BytesIO(
                        (
                            '{"schema":"borsuk-bounded-reader-terminal-v1",'
                            '"source_commit":"' + "1" * 40 + '",'
                            '"instance_id":"i-bounded","cost_usd_micros":1}'
                        ).encode()
                    )
                }

        class FakeEc2:
            def __init__(self) -> None:
                self.terminated: list[str] = []

            def terminate_instances(self, *, InstanceIds: list[str]) -> None:
                self.terminated.extend(InstanceIds)

        ec2 = FakeEc2()
        terminal = monitor_and_terminate(
            self.plan(),
            ec2_client=ec2,
            s3_client=FakeS3(),
            instance_id="i-bounded",
            poll_seconds=0,
        )
        self.assertEqual(terminal["cost_usd_micros"], 1)
        self.assertEqual(ec2.terminated, ["i-bounded"])

    def test_remote_runner_is_strict_bash(self) -> None:
        runner = pathlib.Path(__file__).with_name("run_bounded_reader_1m_remote.sh")
        completed = subprocess.run(
            ["bash", "-n", str(runner)], check=False, capture_output=True, text=True
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)


if __name__ == "__main__":
    unittest.main()
