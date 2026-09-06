from __future__ import annotations

import base64
import contextlib
import dataclasses
import hashlib
import io
import json
import unittest
from unittest import mock

from scripts import run_v36_prefix_screen as subject


class _AwsError(Exception):
    def __init__(self, code: str) -> None:
        self.response = {"Error": {"Code": code}}


class V36PrefixScreenLauncherTests(unittest.TestCase):
    def plan(self) -> subject.V36PrefixScreenPlan:
        return subject.build_v36_prefix_screen_plan(
            run_id="v36-prefix-fixture",
            source_commit="1" * 40,
            source_archive_uri="s3://fixture/v36/source.tar.zst",
            source_archive_sha256="2" * 64,
            source_archive_blake3="7" * 64,
            source_archive_bytes=8_192,
            binary_uri="s3://fixture/v36/v36_prefix_freeze",
            binary_sha256="3" * 64,
            binary_blake3="8" * 64,
            binary_bytes=4_096,
            authority_uri="s3://fixture/v36/prefix-authority.json",
            authority_sha256="4" * 64,
            authority_blake3="9" * 64,
            authority_bytes=2_048,
            source_registry_uri="s3://fixture/v36/source-registry.json",
            source_registry_sha256="5" * 64,
            source_registry_blake3="a" * 64,
            source_registry_bytes=1_024,
            output_prefix="s3://fixture/v36/prefix-results/",
        )

    def test_v36_prefix_screen_dry_run_is_exact_and_non_mutating(self) -> None:
        # Break caught: a feasibility check creates paid/cloud state or hides
        # one of the preregistered source, disk, wall, attempt, or cost caps.
        self.assertEqual(subject.PROFILE, "causality")
        self.assertEqual(subject.REGION, "eu-central-1")
        self.assertEqual(subject.MAX_SOURCE_OBJECTS, 16)
        self.assertEqual(subject.MAX_SOURCE_BYTES, 6 * 1024**3)
        self.assertEqual(subject.TARGET_DISTINCT_ROWS, 1_100_000)
        self.assertEqual(subject.VECTOR_DIMENSIONS, 768)
        self.assertEqual(subject.CHECKPOINT_OBJECTS, 16)
        self.assertEqual(subject.CHECKPOINT_SECONDS, 300)
        self.assertEqual(subject.MAX_ATTEMPTS, 3)
        self.assertEqual(subject.SPOT_HOURLY_CAP_MICRO_USD, 3_000_000)
        self.assertEqual(subject.CAMPAIGN_CAP_MICRO_USD, 90_000_000)
        self.assertGreaterEqual(
            subject.DISK_PREFLIGHT_BYTES,
            subject.MAX_SOURCE_BYTES
            + subject.TARGET_DISTINCT_ROWS * subject.VECTOR_DIMENSIONS * 5,
        )

        ec2 = mock.Mock()
        s3 = mock.Mock()
        receipt = subject.dry_run_v36_prefix_screen(self.plan())
        value = json.loads(receipt)
        self.assertEqual(receipt, subject.canonical_json_bytes(value))
        self.assertEqual(
            value,
            {
                "active_wall_seconds": 43_200,
                "campaign_cap_micro_usd": 90_000_000,
                "checkpoint_objects": 16,
                "checkpoint_seconds": 300,
                "claim_eligible": False,
                "disk_preflight_bytes": subject.DISK_PREFLIGHT_BYTES,
                "dry_run": True,
                "instance_type": "r8gd.8xlarge",
                "max_attempts": 3,
                "max_source_bytes": 6 * 1024**3,
                "max_source_objects": 16,
                "profile": "causality",
                "region": "eu-central-1",
                "run_id": "v36-prefix-fixture",
                "schema": "borsuk-v36-prefix-screen-dry-run-v1",
                "spot_hourly_cap_micro_usd": 3_000_000,
                "target_distinct_rows": 1_100_000,
                "vector_dimensions": 768,
                "zone_candidates": [zone for zone, _ in subject.SPOT_TARGETS],
            },
        )
        ec2.assert_not_called()
        s3.assert_not_called()

        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            self.assertEqual(
                subject.main(
                    [
                        "--dry-run",
                        "--plan-json",
                        json.dumps(dataclasses.asdict(self.plan())),
                    ]
                ),
                0,
            )
        self.assertEqual(stdout.buffer if hasattr(stdout, "buffer") else stdout.getvalue(), receipt.decode())

    def test_v36_prefix_screen_specs_close_cost_disk_and_checkpoint_bounds(self) -> None:
        # Break caught: launch user-data widens source/cost/disk limits, omits
        # authenticated checkpoints, or acquires a persistent/devbox corpus.
        specs = subject.build_v36_prefix_launch_specs(
            self.plan(), launch_nonce="a" * 32, attempt_ordinal=0
        )
        self.assertEqual(len(specs), 3)
        self.assertEqual(len({spec["Placement"]["AvailabilityZone"] for spec in specs}), 3)
        for spec in specs:
            self.assertEqual(spec["MinCount"], 1)
            self.assertEqual(spec["MaxCount"], 1)
            self.assertEqual(spec["InstanceMarketOptions"]["MarketType"], "spot")
            self.assertEqual(
                spec["InstanceMarketOptions"]["SpotOptions"]["MaxPrice"], "3.000000"
            )
            self.assertEqual(spec["InstanceInitiatedShutdownBehavior"], "terminate")
            script = base64.b64decode(spec["UserData"]).decode()
            self.assertIn("--execution-authority", script)
            self.assertIn("--scratch", script)
            self.assertNotIn("--max-source-objects", script)
            self.assertNotIn("--max-source-bytes", script)
            self.assertNotIn("--checkpoint-objects", script)
            self.assertNotIn("--checkpoint-seconds", script)
            self.assertIn(f"test \"$available\" -ge {subject.DISK_PREFLIGHT_BYTES}", script)
            self.assertNotIn("--resume-checkpoint", script)
            self.assertIn("set +e", script)
            self.assertIn("status=$?", script)
            self.assertIn("set -e", script)
            self.assertIn("--if-none-match '*'", script)
            self.assertIn("shutdown -h now", script)
            self.assertNotIn("on-demand", script.lower())
            self.assertNotIn("/home/", script)
            self.assertNotIn("devbox", script.lower())

    def test_v36_prefix_screen_stops_after_three_attempts_and_always_terminates(self) -> None:
        # Break caught: a missing/failed marker polls forever, a fourth paid
        # attempt launches, or an interrupted instance survives the controller.
        plan = self.plan()
        ec2 = mock.Mock()
        ec2.run_instances.side_effect = [
            {"Instances": [{"InstanceId": f"i-attempt-{ordinal}"}]}
            for ordinal in range(3)
        ]
        ec2.describe_instances.return_value = {
            "Reservations": [{"Instances": [{"State": {"Name": "terminated"}}]}]
        }
        s3 = mock.Mock()
        with (
            mock.patch.object(subject.time, "sleep"),
            mock.patch.object(
                subject,
                "_read_attempt_status",
                side_effect=[None, "interrupted", "interrupted", None],
            ),
        ):
            with self.assertRaisesRegex(RuntimeError, "terminal missing"):
                subject.run_v36_prefix_screen(
                    plan,
                    ec2_client=ec2,
                    s3_client=s3,
                    launch_nonce="b" * 32,
                )
        self.assertEqual(ec2.run_instances.call_count, 3)
        self.assertEqual(ec2.terminate_instances.call_count, 3)
        launched = [call.kwargs for call in ec2.run_instances.call_args_list]
        self.assertEqual(len({spec["ClientToken"] for spec in launched}), 3)

    def test_v36_prefix_screen_capacity_fallback_and_cost_projection_are_closed(self) -> None:
        # Break caught: one unavailable zone aborts the screen, or the three
        # admitted wall caps can exceed the exact $90 ceiling at $3/hour.
        self.assertEqual(
            [subject._attempt_wall_seconds(ordinal) for ordinal in range(3)],
            [43_200, 43_200, 21_600],
        )
        projected_cost = sum(
            seconds * subject.SPOT_HOURLY_CAP_MICRO_USD // 3_600
            for seconds in [43_200, 43_200, 21_600]
        )
        self.assertEqual(projected_cost, subject.CAMPAIGN_CAP_MICRO_USD)

        plan = self.plan()
        ec2 = mock.Mock()
        ec2.run_instances.side_effect = [
            _AwsError("InsufficientInstanceCapacity"),
            {"Instances": [{"InstanceId": "i-capacity-fallback"}]},
        ]
        ec2.describe_instances.return_value = {
            "Reservations": [{"Instances": [{"State": {"Name": "terminated"}}]}]
        }
        s3 = mock.Mock()
        with mock.patch.object(
            subject, "_read_attempt_status", side_effect=[None, "complete"]
        ):
            uri = subject.run_v36_prefix_screen(
                plan,
                ec2_client=ec2,
                s3_client=s3,
                launch_nonce="c" * 32,
            )
        self.assertEqual(
            uri,
            "s3://fixture/v36/prefix-results/attempt-0001/ATTEMPT_COMPLETE.json",
        )
        self.assertEqual(ec2.run_instances.call_count, 2)
        zones = [
            call.kwargs["Placement"]["AvailabilityZone"]
            for call in ec2.run_instances.call_args_list
        ]
        self.assertEqual(zones, ["eu-central-1c", "eu-central-1b"])
        ec2.terminate_instances.assert_called_once_with(
            InstanceIds=["i-capacity-fallback"]
        )

    def test_v36_prefix_screen_terminal_binds_attempt_inputs_and_outputs(self) -> None:
        # Break caught: a tiny marker with the same run/commit is accepted as
        # proof for a different binary, authority, instance, or output set.
        plan = self.plan()
        attempt = 0
        execution_authority = {
            "active_wall_seconds": subject._attempt_wall_seconds(attempt),
            "attempt_id": f"{plan.run_id}-attempt-{attempt:04d}",
            "checkpoint_seconds": subject.CHECKPOINT_SECONDS,
            "claim_eligible": False,
            "inputs": [
                {"blake3": plan.binary_blake3, "encoded_bytes": plan.binary_bytes, "role": "binary", "sha256": plan.binary_sha256, "uri": plan.binary_uri},
                {"blake3": plan.authority_blake3, "encoded_bytes": plan.authority_bytes, "role": "freeze-authority", "sha256": plan.authority_sha256, "uri": plan.authority_uri},
                {"blake3": plan.source_archive_blake3, "encoded_bytes": plan.source_archive_bytes, "role": "source-archive", "sha256": plan.source_archive_sha256, "uri": plan.source_archive_uri},
                {"blake3": plan.source_registry_blake3, "encoded_bytes": plan.source_registry_bytes, "role": "source-registry", "sha256": plan.source_registry_sha256, "uri": plan.source_registry_uri},
            ],
            "output_prefix": "s3://fixture/v36/prefix-results/attempt-0000/",
            "schema": "borsuk-v36-prefix-freeze-execution-authority-v1",
            "source_commit": plan.source_commit,
        }
        outputs = [
            {
                "encoded_bytes": 1_000 + ordinal,
                "role": role,
                "sha256": format(ordinal + 11, "064x"),
                "uri": f"s3://fixture/v36/prefix-results/attempt-0000/{role}",
            }
            for ordinal, role in enumerate(
                (
                    "population-authority",
                    "source",
                    "development-query",
                    "development-gt100",
                    "validation-query",
                    "validation-gt100",
                    "sealed-holdout-query",
                    "sealed-holdout-gt100",
                    "performance-query",
                )
            )
        ]
        terminal = {
            "attempt_id": execution_authority["attempt_id"],
            "claim_eligible": False,
            "execution_authority_sha256": hashlib.sha256(
                subject.canonical_json_bytes(execution_authority)
            ).hexdigest(),
            "inputs": execution_authority["inputs"],
            "instance_id": "i-fixture",
            "outputs": outputs,
            "run_id": plan.run_id,
            "schema": "borsuk-v36-prefix-freeze-terminal-v1",
            "source_commit": plan.source_commit,
            "status": "complete",
        }

        class S3:
            def __init__(self, value: object) -> None:
                self.value = value

            def get_object(self, **_kwargs: object) -> dict[str, object]:
                body = subject.canonical_json_bytes(self.value)
                return {"Body": io.BytesIO(body), "ContentLength": len(body)}

        self.assertEqual(
            subject._read_attempt_status(
                S3(terminal), plan, attempt, expected_instance_id="i-fixture"
            ),
            "complete",
        )
        mutations = []
        for field in (
            "attempt_id",
            "claim_eligible",
            "execution_authority_sha256",
            "inputs",
            "instance_id",
            "outputs",
            "schema",
            "status",
        ):
            changed = dict(terminal)
            changed.pop(field)
            mutations.append(changed)
        changed_input = json.loads(json.dumps(terminal))
        changed_input["inputs"][0]["sha256"] = "f" * 64
        mutations.append(changed_input)
        changed_output = json.loads(json.dumps(terminal))
        changed_output["outputs"][0]["role"] = "source"
        mutations.append(changed_output)
        for mutation in mutations:
            with self.subTest(mutation=mutation):
                with self.assertRaisesRegex(ValueError, "terminal authority differs"):
                    subject._read_attempt_status(
                        S3(mutation), plan, attempt, expected_instance_id="i-fixture"
                    )

    def test_v36_prefix_screen_terminal_stops_a_running_instance_immediately(self) -> None:
        # Break caught: completed science keeps a paid instance alive until
        # guest shutdown instead of letting the controller terminate it.
        plan = self.plan()
        ec2 = mock.Mock()
        ec2.run_instances.return_value = {
            "Instances": [{"InstanceId": "i-running-complete"}]
        }
        ec2.describe_instances.return_value = {
            "Reservations": [{"Instances": [{"State": {"Name": "running"}}]}]
        }
        with (
            mock.patch.object(
                subject, "_read_attempt_status", side_effect=[None, "complete"]
            ) as status,
            mock.patch.object(
                subject.time,
                "sleep",
                side_effect=AssertionError("controller slept after terminal"),
            ),
        ):
            uri = subject.run_v36_prefix_screen(
                plan,
                ec2_client=ec2,
                s3_client=mock.Mock(),
                launch_nonce="d" * 32,
            )
        self.assertEqual(
            uri,
            "s3://fixture/v36/prefix-results/attempt-0000/ATTEMPT_COMPLETE.json",
        )
        self.assertEqual(status.call_count, 2)
        ec2.terminate_instances.assert_called_once_with(
            InstanceIds=["i-running-complete"]
        )

    def test_v36_prefix_screen_controller_deadline_is_bounded(self) -> None:
        # Break caught: bootstrap or guest shutdown hangs outside the Rust
        # timeout and leaves the controller polling a paid instance forever.
        plan = self.plan()
        ec2 = mock.Mock()
        ec2.run_instances.return_value = {
            "Instances": [{"InstanceId": "i-running-hung"}]
        }
        ec2.describe_instances.return_value = {
            "Reservations": [{"Instances": [{"State": {"Name": "running"}}]}]
        }
        deadline = (
            subject._attempt_wall_seconds(0)
            + subject.CONTROLLER_GRACE_SECONDS
            + 1
        )
        with (
            mock.patch.object(subject, "_read_attempt_status", return_value=None),
            mock.patch.object(subject.time, "monotonic", side_effect=[0.0, deadline]),
            mock.patch.object(
                subject.time,
                "sleep",
                side_effect=AssertionError("controller slept past deadline"),
            ),
        ):
            with self.assertRaisesRegex(RuntimeError, "controller deadline"):
                subject.run_v36_prefix_screen(
                    plan,
                    ec2_client=ec2,
                    s3_client=mock.Mock(),
                    launch_nonce="e" * 32,
                )
        ec2.terminate_instances.assert_called_once_with(
            InstanceIds=["i-running-hung"]
        )


if __name__ == "__main__":
    unittest.main()
