"""Contract tests for the one-shot V101 Causality Spot launcher."""

import dataclasses
import io
import json
import unittest
from pathlib import Path
from types import SimpleNamespace

from scripts.launch_v101_two_stage_residual_pq_spot import (
    CRITIQUE_RESULT_SHA256,
    DEFAULT_TARGETS,
    FROZEN_INPUTS,
    SpotTarget,
    build_launch_specs,
    build_plan,
    canonical_terminal_bytes,
    claim_launched_attempt,
    claim_reserved_attempt,
    ensure_unstarted,
    launch_one_spot,
    monitor_and_terminate,
    parse_args,
    validate_terminal_bytes,
    worker_script,
)
from scripts.v97_row_width_screen import ObjectIdentity


class V101SpotLauncherTests(unittest.TestCase):
    @staticmethod
    def plan():
        return build_plan(
            profile="causality",
            source_commit="a" * 40,
            source_archive=ObjectIdentity("s3://fixture/source.tar.gz", "1" * 64, 123),
            inputs=FROZEN_INPUTS,
            critique_result_sha256=CRITIQUE_RESULT_SHA256,
            output_prefix="s3://fixture/v101/a0001",
            image_id="ami-0123456789abcdef0",
            image_architecture="x86_64",
            security_group_id="sg-0123456789abcdef0",
            instance_profile_arn="arn:aws:iam::123456789012:instance-profile/test",
            targets=DEFAULT_TARGETS,
            spot_price_usd_per_hour_micros=480_000,
        )

    def test_plan_is_fixed_to_development_one_attempt_and_registered_caps(self) -> None:
        plan = self.plan()
        self.assertEqual(plan.attempt, 1)
        self.assertEqual(plan.query_count, 128)
        self.assertEqual(plan.bootstrap_resamples, 10_000)
        self.assertEqual(plan.config.maximum_gets, 32)
        self.assertEqual(plan.config.maximum_bytes, 16 * 1024**2)
        self.assertEqual(plan.targets, DEFAULT_TARGETS)
        changed = dict(plan.inputs)
        changed["queries"] = dataclasses.replace(
            changed["queries"], uri="s3://fixture/validation-query.parquet"
        )
        with self.assertRaisesRegex(ValueError, "V101 Spot plan differs"):
            build_plan(**{**dataclasses.asdict(plan), "inputs": changed})

    def test_worker_binds_all_inputs_and_executes_only_registered_remote_script(
        self,
    ) -> None:
        script = worker_script(self.plan())
        self.assertIn("V101_QUERY_COUNT=128", script)
        self.assertIn("V101_REGISTERED_QUERY_COUNT=1000", script)
        self.assertIn("V101_SOURCE_COMMIT=" + "a" * 40, script)
        for role in FROZEN_INPUTS:
            self.assertIn(f"V101_{role.upper()}_URI=", script)
        self.assertIn("v101_two_stage_residual_pq_run_remote.sh", script)
        self.assertNotIn("validation-query", script)
        self.assertLessEqual(len(script.encode()), 16_384)

    def test_launch_specs_are_one_time_spot_with_serial_zone_tokens(self) -> None:
        specs = build_launch_specs(self.plan())
        self.assertEqual(len(specs), 3)
        self.assertEqual(len({item["ClientToken"] for item in specs}), 3)
        for spec, target in zip(specs, DEFAULT_TARGETS, strict=True):
            self.assertEqual(spec["MinCount"], 1)
            self.assertEqual(spec["MaxCount"], 1)
            self.assertEqual(
                spec["InstanceMarketOptions"]["SpotOptions"]["SpotInstanceType"],
                "one-time",
            )
            self.assertEqual(spec["NetworkInterfaces"][0]["SubnetId"], target.subnet_id)

    def test_terminal_is_canonical_and_requires_complete_evidence(self) -> None:
        plan = self.plan()
        evidence = {
            role: ObjectIdentity(f"s3://fixture/{role}", str(index) * 64, index)
            for index, role in enumerate(
                ("result", "rescore", "resources", "worker_log"), start=1
            )
        }
        body = canonical_terminal_bytes(
            plan,
            instance_id="i-0123456789abcdef0",
            status="complete",
            exit_code=0,
            evidence=evidence,
        )
        self.assertEqual(
            json.dumps(json.loads(body), separators=(",", ":"), sort_keys=True).encode()
            + b"\n",
            body,
        )
        terminal = validate_terminal_bytes(body, plan, "i-0123456789abcdef0")
        self.assertTrue(terminal.claim_eligible)
        self.assertEqual(terminal.status, "complete")
        with self.assertRaisesRegex(ValueError, "V101 terminal differs"):
            canonical_terminal_bytes(
                plan,
                instance_id="i-0123456789abcdef0",
                status="complete",
                exit_code=0,
                evidence={},
            )

    def test_target_type_rejects_non_eu_central_zone(self) -> None:
        with self.assertRaisesRegex(ValueError, "V101 Spot target differs"):
            SpotTarget("us-east-1a", "subnet-0123456789abcdef0")

    def test_launch_retries_only_capacity_and_creates_one_instance(self) -> None:
        class FakeEc2:
            def __init__(self) -> None:
                self.calls = 0

            def run_instances(self, **_request):
                self.calls += 1
                if self.calls == 1:
                    raise RuntimeError("InsufficientInstanceCapacity")
                return {"Instances": [{"InstanceId": "i-v101"}]}

        ec2 = FakeEc2()
        self.assertEqual(launch_one_spot(self.plan(), ec2_client=ec2), "i-v101")
        self.assertEqual(ec2.calls, 2)

    def test_monitor_reads_only_terminal_and_always_terminates(self) -> None:
        body = canonical_terminal_bytes(
            self.plan(),
            instance_id="i-v101",
            status="failed",
            exit_code=97,
            evidence={},
        )

        class FakeS3:
            def __init__(self) -> None:
                self.keys = []

            def get_object(self, *, Bucket, Key):  # noqa: N803
                self.keys.append((Bucket, Key))
                return {"Body": io.BytesIO(body), "ContentLength": len(body)}

        class FakeEc2:
            def __init__(self) -> None:
                self.terminated = []

            def terminate_instances(self, *, InstanceIds):  # noqa: N803
                self.terminated.extend(InstanceIds)

        s3, ec2 = FakeS3(), FakeEc2()
        terminal = monitor_and_terminate(
            self.plan(), s3_client=s3, ec2_client=ec2, instance_id="i-v101"
        )
        self.assertEqual(terminal.status, "failed")
        self.assertEqual(s3.keys, [("fixture", "v101/a0001/terminal.json")])
        self.assertEqual(ec2.terminated, ["i-v101"])

    def test_remote_shell_runs_only_v101_producer_and_independent_reducer(self) -> None:
        body = Path("scripts/v101_two_stage_residual_pq_run_remote.sh").read_text()
        self.assertIn("from scripts.v101_two_stage_residual_pq_screen import", body)
        self.assertIn("from scripts.v101_two_stage_residual_pq_rescore import", body)
        self.assertIn("canonical_v101_result_bytes(evaluate_v101(", body)
        self.assertIn("rescore_v101_result(", body)
        self.assertIn("for role in source queries truth generation base delta", body)
        self.assertIn("/proc/pressure/memory", body)
        self.assertIn("spot/instance-action", body)
        self.assertIn("terminal.json", body)
        self.assertNotIn("evaluate_v99", body)
        self.assertNotIn("rescore_v99", body)

    def test_reservation_and_launch_receipts_are_create_only_and_bound(self) -> None:
        class FakeEvents:
            def __init__(self) -> None:
                self.handlers = {}

            def register_first(self, _name, handler, unique_id):
                self.handlers[unique_id] = handler

            def unregister(self, _name, handler=None, unique_id=None, **_unused):
                del handler
                self.handlers.pop(unique_id, None)

        class FakeS3:
            def __init__(self) -> None:
                self.meta = SimpleNamespace(events=FakeEvents())
                self.requests = []
                self.headers = []

            def put_object(self, **request):
                signed = SimpleNamespace(headers={})
                next(iter(self.meta.events.handlers.values()))(signed)
                self.headers.append(signed.headers)
                self.requests.append(request)

        s3 = FakeS3()
        claim_reserved_attempt(self.plan(), s3_client=s3)
        claim_launched_attempt(self.plan(), s3_client=s3, instance_id="i-v101")
        self.assertEqual(s3.meta.events.handlers, {})
        self.assertEqual([item["If-None-Match"] for item in s3.headers], ["*", "*"])
        self.assertTrue(s3.requests[0]["Key"].endswith("reservation.json"))
        self.assertTrue(s3.requests[1]["Key"].endswith("launch.json"))
        self.assertIsNone(json.loads(s3.requests[0]["Body"])["instance_id"])
        self.assertEqual(json.loads(s3.requests[1]["Body"])["instance_id"], "i-v101")

    def test_cli_builds_exact_plan_and_rejects_started_prefix(self) -> None:
        plan = parse_args(
            [
                "--source-commit",
                "a" * 40,
                "--source-archive-uri",
                "s3://fixture/source.tar.gz",
                "--source-archive-sha256",
                "1" * 64,
                "--source-archive-bytes",
                "123",
                "--output-prefix",
                "s3://fixture/v101/a0001",
            ]
        )
        self.assertEqual(plan.source_commit, "a" * 40)
        self.assertEqual(plan.inputs, FROZEN_INPUTS)

        class MissingS3:
            def head_object(self, **_request):
                error = RuntimeError("404")
                error.response = {"Error": {"Code": "404"}}
                raise error

        ensure_unstarted(plan, s3_client=MissingS3())

        class ExistingS3:
            def head_object(self, **_request):
                return {"ContentLength": 1}

        with self.assertRaisesRegex(ValueError, "already exists"):
            ensure_unstarted(plan, s3_client=ExistingS3())


if __name__ == "__main__":
    unittest.main()
