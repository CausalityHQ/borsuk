from __future__ import annotations

import base64
import contextlib
import dataclasses
import hashlib
import io
import json
import pathlib
import subprocess
import sys
import tempfile
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
            + subject.TARGET_DISTINCT_ROWS * subject.VECTOR_DIMENSIONS * 4
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

    def test_v36_prefix_screen_canonical_json_matches_rust_utf8(self) -> None:
        # Break caught: Python escapes non-ASCII source paths while serde_json
        # writes UTF-8, making one authority fail cross-language authentication.
        self.assertEqual(
            subject.canonical_json_bytes({"uri": "s3://fixture/π.parquet"}),
            b'{"uri":"s3://fixture/\xcf\x80.parquet"}\n',
        )
        self.assertIn("ensure_ascii=False", subject._GUEST_TERMINAL_PROGRAM)

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

    def test_v36_prefix_screen_runs_bound_sidecar_concurrently_and_fails_fast(self) -> None:
        # Break caught: checkpoints remain on ephemeral NVMe until Rust exits,
        # or science keeps running after its only publisher has failed.
        script = base64.b64decode(
            subject.build_v36_prefix_launch_specs(
                self.plan(), launch_nonce="7" * 32, attempt_ordinal=0
            )[0]["UserData"]
        ).decode()
        self.assertIn(
            'tar --zstd -xf "$root/source.tar.zst" -C "$root/sidecar-source" '
            "scripts/run_v36_prefix_screen.py",
            script,
        )
        self.assertIn('science_pid=$!', script)
        self.assertIn('--publish-checkpoints', script)
        self.assertIn('--producer-pid "$science_pid"', script)
        self.assertIn('--first-generation 0', script)
        self.assertIn('sidecar_pid=$!', script)
        self.assertIn('kill -TERM "$science_pid"', script)
        self.assertIn('sha256sum "$root/sidecar-source/scripts/run_v36_prefix_screen.py"', script)
        self.assertIn('put-object --generate-cli-skeleton input', script)
        self.assertIn('if [[ "$sidecar_status" != 0 ]]; then', script)
        self.assertNotIn("CHECKPOINT.json", script)

    def test_v36_prefix_screen_publishes_artifacts_receipt_then_terminal(self) -> None:
        # Break caught: successful science is shut down before its artifacts
        # and authenticated terminal become durable in the attempt prefix.
        script = base64.b64decode(
            subject.build_v36_prefix_launch_specs(
                self.plan(), launch_nonce="f" * 32, attempt_ordinal=0
            )[0]["UserData"]
        ).decode()
        artifacts = (
            "population-authority.json",
            "source.parquet",
            "development-query.parquet",
            "development-gt100.parquet",
            "validation-query.parquet",
            "validation-gt100.parquet",
            "sealed-holdout-query.parquet",
            "sealed-holdout-gt100.parquet",
            "performance-query.parquet",
        )
        positions = []
        for filename in artifacts:
            needle = f'--body "$root/output/{filename}"'
            self.assertIn(needle, script)
            positions.append(script.index(needle))
        receipt = script.index('--body "$root/output/freeze-receipt.json"')
        terminal = script.index('--body "$root/output/ATTEMPT_COMPLETE.json"')
        self.assertLess(max(positions), receipt)
        self.assertLess(receipt, terminal)
        self.assertIn("latest/api/token", script)
        self.assertIn("latest/meta-data/instance-id", script)
        self.assertIn("--if-none-match '*'", script)

    def test_v36_prefix_screen_source_insufficiency_is_terminal_not_retryable(self) -> None:
        # Break caught: exhausting the registered source prefix is mislabeled
        # as infrastructure and spends a second or third Spot attempt.
        script = base64.b64decode(
            subject.build_v36_prefix_launch_specs(
                self.plan(), launch_nonce="e" * 32, attempt_ordinal=0
            )[0]["UserData"]
        ).decode()
        self.assertIn('elif [[ "$status" = 42 ]]; then', script)
        self.assertIn("terminal_status=screen-source-insufficient", script)

    def test_v36_prefix_screen_guest_terminal_authenticates_local_outputs(self) -> None:
        # Break caught: user-data publishes a terminal whose identities do not
        # describe the exact locally produced receipt and Parquet artifacts.
        plan = self.plan()
        execution = subject._execution_authority(plan, 0)
        filenames = (
            ("population-authority", "population-authority.json"),
            ("source", "source.parquet"),
            ("development-query", "development-query.parquet"),
            ("development-gt100", "development-gt100.parquet"),
            ("validation-query", "validation-query.parquet"),
            ("validation-gt100", "validation-gt100.parquet"),
            ("sealed-holdout-query", "sealed-holdout-query.parquet"),
            ("sealed-holdout-gt100", "sealed-holdout-gt100.parquet"),
            ("performance-query", "performance-query.parquet"),
        )
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            execution_path = root / "execution.json"
            receipt_path = root / "freeze-receipt.json"
            terminal_path = root / "ATTEMPT_COMPLETE.json"
            execution_path.write_bytes(subject.canonical_json_bytes(execution))
            outputs = []
            for ordinal, (role, filename) in enumerate(filenames):
                payload = f"artifact-{ordinal}".encode()
                (root / filename).write_bytes(payload)
                outputs.append(
                    {
                        "blake3": format(ordinal + 1, "064x"),
                        "encoded_bytes": len(payload),
                        "role": role,
                        "sha256": hashlib.sha256(payload).hexdigest(),
                        "uri": execution["output_prefix"] + filename,
                    }
                )
            receipt_path.write_bytes(subject.canonical_json_bytes({"outputs": outputs}))
            completed = subprocess.run(
                [
                    sys.executable,
                    "-c",
                    subject._GUEST_TERMINAL_PROGRAM,
                    str(execution_path),
                    str(receipt_path),
                    str(root),
                    "i-fixture",
                    plan.run_id,
                    plan.source_commit,
                    "complete",
                    str(terminal_path),
                ],
                check=False,
                capture_output=True,
                text=True,
            )
            self.assertEqual(completed.returncode, 0, completed.stderr)
            terminal = json.loads(terminal_path.read_bytes())
            self.assertEqual(
                terminal_path.read_bytes(), subject.canonical_json_bytes(terminal)
            )
            self.assertEqual(
                [output["role"] for output in terminal["outputs"]],
                ["freeze-receipt", *(role for role, _ in filenames)],
            )
            self.assertEqual(terminal["instance_id"], "i-fixture")
            self.assertEqual(terminal["status"], "complete")

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
                    "freeze-receipt",
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

    def test_v36_checkpoint_publication_orders_dependencies_manifest_pointer(self) -> None:
        # Break caught: a pointer becomes visible before the immutable objects
        # needed to restore it, or pointer replacement is not conditional.
        calls = []

        class S3:
            def put_object(self, **values: object) -> dict[str, str]:
                calls.append(values)
                return {"ETag": '"etag-next"'}

        dependency = b"identity-run\n"
        manifest = subject.canonical_json_bytes(
            {"generation": 2, "schema": "borsuk-v36-prefix-freeze-checkpoint-v1"}
        )
        pointer = subject.canonical_json_bytes(
            {
                "claim_eligible": False,
                "generation": 2,
                "manifest": {
                    "blake3": "b" * 64,
                    "encoded_bytes": len(manifest),
                    "role": "checkpoint-manifest",
                    "sha256": hashlib.sha256(manifest).hexdigest(),
                    "uri": "s3://fixture/checkpoints/manifests/m2.json",
                },
                "producer_attempt_id": "v36-prefix-screen-fixture-attempt-0000",
                "producer_attempt_ordinal": 0,
                "run_id": "v36-prefix-screen-fixture",
                "schema": "borsuk-v36-prefix-checkpoint-pointer-v1",
            }
        )
        etag = subject.publish_v36_checkpoint(
            S3(),
            immutable_objects=(("s3://fixture/checkpoints/objects/run.arrow", dependency),),
            manifest_uri="s3://fixture/checkpoints/manifests/m2.json",
            manifest_bytes=manifest,
            pointer_uri="s3://fixture/checkpoints/runs/v36-prefix-fixture/latest.json",
            pointer_bytes=pointer,
            previous_pointer_etag="etag-before",
        )
        self.assertEqual(etag, "etag-next")
        self.assertEqual(
            [(call["Key"], call.get("IfNoneMatch"), call.get("IfMatch")) for call in calls],
            [
                ("checkpoints/objects/run.arrow", "*", None),
                ("checkpoints/manifests/m2.json", "*", None),
                (
                    "checkpoints/runs/v36-prefix-fixture/latest.json",
                    None,
                    "etag-before",
                ),
            ],
        )

    def test_v36_checkpoint_lost_pointer_ack_accepts_only_intended_bytes(self) -> None:
        # Break caught: a timed-out CAS is retried blindly or a concurrent
        # writer's pointer is accepted as this generation's publication.
        manifest = subject.canonical_json_bytes(
            {"generation": 1, "schema": "borsuk-v36-prefix-freeze-checkpoint-v1"}
        )
        pointer = subject.canonical_json_bytes(
            {
                "claim_eligible": False,
                "generation": 1,
                "manifest": {
                    "blake3": "b" * 64,
                    "encoded_bytes": len(manifest),
                    "role": "checkpoint-manifest",
                    "sha256": hashlib.sha256(manifest).hexdigest(),
                    "uri": "s3://fixture/checkpoints/manifests/m1.json",
                },
                "producer_attempt_id": "v36-prefix-screen-fixture-attempt-0000",
                "producer_attempt_ordinal": 0,
                "run_id": "v36-prefix-screen-fixture",
                "schema": "borsuk-v36-prefix-checkpoint-pointer-v1",
            }
        )

        class S3:
            def __init__(self, observed: bytes) -> None:
                self.observed = observed

            def put_object(self, *, Key: str, **_values: object) -> object:
                if not Key.endswith("latest.json"):
                    return {"ETag": '"etag-immutable"'}
                raise TimeoutError("ack lost")

            def get_object(self, **_values: object) -> dict[str, object]:
                return {
                    "Body": io.BytesIO(self.observed),
                    "ContentLength": len(self.observed),
                    "ETag": '"etag-observed"',
                }

        common = {
            "immutable_objects": (),
            "manifest_uri": "s3://fixture/checkpoints/manifests/m1.json",
            "manifest_bytes": manifest,
            "pointer_uri": "s3://fixture/checkpoints/runs/v36-prefix-fixture/latest.json",
            "pointer_bytes": pointer,
            "previous_pointer_etag": None,
        }
        self.assertEqual(
            subject.publish_v36_checkpoint(S3(pointer), **common), "etag-observed"
        )
        changed = json.loads(pointer)
        changed["generation"] = 7
        with self.assertRaisesRegex(ValueError, "checkpoint pointer observation differs"):
            subject.publish_v36_checkpoint(
                S3(subject.canonical_json_bytes(changed)), **common
            )

    def test_v36_checkpoint_corrupt_newest_pointer_fails_closed(self) -> None:
        # Break caught: resume silently falls back to an older generation when
        # the authoritative newest pointer or its referenced manifest is bad.
        pointer = subject.canonical_json_bytes(
            {
                "claim_eligible": False,
                "generation": 3,
                "manifest": {
                    "blake3": "b" * 64,
                    "encoded_bytes": 8,
                    "role": "checkpoint-manifest",
                    "sha256": "a" * 64,
                    "uri": "s3://fixture/checkpoints/manifests/m3.json",
                },
                "producer_attempt_id": "v36-prefix-screen-fixture-attempt-0000",
                "producer_attempt_ordinal": 0,
                "run_id": "v36-prefix-screen-fixture",
                "schema": "borsuk-v36-prefix-checkpoint-pointer-v1",
            }
        )

        class S3:
            def get_object(self, *, Key: str, **_values: object) -> dict[str, object]:
                body = pointer if Key.endswith("latest.json") else b"corrupt\n"
                return {"Body": io.BytesIO(body), "ContentLength": len(body)}

        with self.assertRaisesRegex(ValueError, "checkpoint manifest authority differs"):
            subject.read_v36_checkpoint_head(
                S3(), "s3://fixture/checkpoints/runs/v36-prefix-fixture/latest.json"
            )

    def test_v36_checkpoint_sidecar_streams_only_rust_committed_generation(self) -> None:
        # Break caught: the supervisor recreates scientific JSON, reads a
        # partial generation, buffers a run in RAM, or publishes the pointer
        # before its dependency and manifest.
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            for child in ("objects", "manifests", "pointers", "commits"):
                (root / child).mkdir()
            dependency = b"arrow-identity-run"
            dependency_sha = hashlib.sha256(dependency).hexdigest()
            dependency_identity = {
                "blake3": "b" * 64,
                "encoded_bytes": len(dependency),
                "role": "population-identity-run-0000",
                "sha256": dependency_sha,
                "uri": f"s3://fixture/checkpoints/objects/{dependency_sha}-run.arrow",
            }
            manifest = subject.canonical_json_bytes(
                {
                    "generation": 0,
                    "schema": "borsuk-v36-prefix-freeze-checkpoint-v1",
                }
            )
            manifest_sha = hashlib.sha256(manifest).hexdigest()
            manifest_identity = {
                "blake3": "c" * 64,
                "encoded_bytes": len(manifest),
                "role": "checkpoint-manifest",
                "sha256": manifest_sha,
                "uri": f"s3://fixture/checkpoints/objects/{manifest_sha}-checkpoint.json",
            }
            pointer_uri = (
                "s3://fixture/checkpoints/runs/v36-prefix-screen-fixture/latest.json"
            )
            pointer = subject.canonical_json_bytes(
                {
                    "claim_eligible": False,
                    "generation": 0,
                    "manifest": manifest_identity,
                    "producer_attempt_id": "v36-prefix-screen-fixture-attempt-0000",
                    "producer_attempt_ordinal": 0,
                    "run_id": "v36-prefix-screen-fixture",
                    "schema": "borsuk-v36-prefix-checkpoint-pointer-v1",
                }
            )
            pointer_sha = hashlib.sha256(pointer).hexdigest()
            (root / "objects" / f"{dependency_sha}.blob").write_bytes(dependency)
            (root / "manifests" / f"{manifest_sha}.json").write_bytes(manifest)
            (root / "pointers" / f"{pointer_sha}.json").write_bytes(pointer)
            ready = subject.canonical_json_bytes(
                {
                    "dependencies": [dependency_identity],
                    "generation": 0,
                    "manifest": manifest_identity,
                    "pointer_encoded_bytes": len(pointer),
                    "pointer_sha256": pointer_sha,
                    "pointer_uri": pointer_uri,
                    "previous_pointer_sha256": None,
                    "schema": "borsuk-v36-prefix-checkpoint-outbox-v1",
                }
            )
            (root / "commits" / "generation-00000000.json").write_bytes(ready)

            calls: list[tuple[str, str, str | None]] = []

            class Transport:
                def put_immutable(
                    self, identity: dict[str, object], path: pathlib.Path
                ) -> None:
                    self.assert_regular(path)
                    calls.append(("immutable", str(identity["uri"]), path.name))

                def put_pointer(
                    self,
                    uri: str,
                    path: pathlib.Path,
                    previous_sha256: str | None,
                ) -> None:
                    self.assert_regular(path)
                    calls.append(("pointer", uri, previous_sha256))

                @staticmethod
                def assert_regular(path: pathlib.Path) -> None:
                    if not path.is_file() or path.is_symlink():
                        raise AssertionError("transport received non-file")

            subject.publish_v36_checkpoint_outbox_generation(root, 0, Transport())
            self.assertEqual(
                calls,
                [
                    ("immutable", dependency_identity["uri"], f"{dependency_sha}.blob"),
                    ("immutable", manifest_identity["uri"], f"{manifest_sha}.json"),
                    ("pointer", pointer_uri, None),
                ],
            )

            redirected = json.loads(ready)
            redirected["dependencies"][0]["uri"] = "s3://other/escape.blob"
            (root / "commits" / "generation-00000000.json").write_bytes(
                subject.canonical_json_bytes(redirected)
            )
            with self.assertRaisesRegex(ValueError, "outbox identity differs"):
                subject.publish_v36_checkpoint_outbox_generation(root, 0, Transport())
            (root / "commits" / "generation-00000000.json").write_bytes(ready)

            (root / "objects" / f"{dependency_sha}.blob").write_bytes(b"corrupt")
            with self.assertRaisesRegex(ValueError, "outbox artifact authority differs"):
                subject.publish_v36_checkpoint_outbox_generation(root, 0, Transport())

    def test_v36_checkpoint_aws_cli_transport_is_conditional_and_lost_ack_safe(
        self,
    ) -> None:
        # Break caught: the runtime boto model silently lacks conditional PUT,
        # a replacement is not fenced by the authenticated predecessor, or a
        # lost acknowledgement causes a blind duplicate pointer write.
        payload = subject.canonical_json_bytes({"generation": 1})
        previous = subject.canonical_json_bytes({"generation": 0})
        identity = {
            "blake3": "b" * 64,
            "encoded_bytes": len(payload),
            "role": "checkpoint-manifest",
            "sha256": hashlib.sha256(payload).hexdigest(),
            "uri": "s3://fixture/checkpoints/objects/manifest.json",
        }

        class Runner:
            def __init__(self) -> None:
                self.calls: list[tuple[str, ...]] = []
                self.remote = previous

            def json(self, arguments: list[str]) -> dict[str, object]:
                self.calls.append(tuple(arguments))
                if arguments[1:3] == ["s3api", "head-object"]:
                    return {
                        "ContentLength": len(self.remote),
                        "ETag": '"etag-before"',
                    }
                if "--key" in arguments and arguments[arguments.index("--key") + 1].endswith(
                    "latest.json"
                ):
                    self.remote = payload
                    raise TimeoutError("pointer acknowledgement lost")
                return {"ETag": '"etag-immutable"'}

            def stream(self, arguments: list[str]) -> tuple[bytes, ...]:
                self.calls.append(tuple(arguments))
                return (self.remote[:3], self.remote[3:])

        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory, "payload.json")
            path.write_bytes(payload)
            runner = Runner()
            transport = subject.V36AwsCliCheckpointTransport(runner=runner)
            transport.put_immutable(identity, path)
            transport.put_immutable(identity, path)
            transport.put_pointer(
                "s3://fixture/checkpoints/runs/run/latest.json",
                path,
                hashlib.sha256(previous).hexdigest(),
            )

        immutable_put, head, current_get, pointer_put, observed_get = runner.calls
        self.assertIn("--if-none-match", immutable_put)
        self.assertEqual(
            immutable_put[immutable_put.index("--if-none-match") + 1], "*"
        )
        self.assertEqual(head[1:3], ("s3api", "head-object"))
        self.assertEqual(current_get[1:3], ("s3", "cp"))
        self.assertIn("--if-match", pointer_put)
        self.assertEqual(pointer_put[pointer_put.index("--if-match") + 1], "etag-before")
        self.assertEqual(observed_get[1:3], ("s3", "cp"))

    def test_v36_checkpoint_aws_runner_is_retry_configured_and_time_bounded(
        self,
    ) -> None:
        # Break caught: a throttled or wedged AWS CLI strands the producer or
        # consumes an entire controller grace period without bounded retries.
        runner = subject._AwsCliRunner()
        with mock.patch.object(
            subject.subprocess,
            "run",
            return_value=subprocess.CompletedProcess([], 0, b"{}\n", b""),
        ) as run:
            self.assertEqual(runner.json(["aws", "fixture"]), {})
        self.assertEqual(run.call_args.kwargs["timeout"], 120)
        self.assertEqual(run.call_args.kwargs["env"]["AWS_RETRY_MODE"], "standard")
        self.assertEqual(run.call_args.kwargs["env"]["AWS_MAX_ATTEMPTS"], "5")

        process = mock.Mock(stdout=io.BytesIO(b"payload"))
        process.wait.return_value = 0
        with mock.patch.object(subject.subprocess, "Popen", return_value=process) as popen:
            self.assertEqual(tuple(runner.stream(["aws", "fixture"])), (b"payload",))
        self.assertEqual(
            popen.call_args.args[0][:4],
            ["timeout", "--signal=TERM", "--kill-after=5", "120"],
        )
        self.assertEqual(popen.call_args.kwargs["env"]["AWS_MAX_ATTEMPTS"], "5")

    def test_v36_checkpoint_sidecar_watches_every_ready_generation_until_exit(
        self,
    ) -> None:
        # Break caught: a long freeze publishes only its final boundary or the
        # sidecar exits before draining the last crash-atomic ready descriptor.
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            (root / "commits").mkdir()
            for generation in range(2):
                (root / "commits" / f"generation-{generation:08d}.json").touch()
            alive = mock.Mock(side_effect=[True, False])
            with mock.patch.object(
                subject, "publish_v36_checkpoint_outbox_generation"
            ) as publish:
                self.assertEqual(
                    subject.watch_v36_checkpoint_outbox(
                        root,
                        41,
                        mock.sentinel.transport,
                        producer_alive=alive,
                        pause=mock.Mock(),
                    ),
                    2,
                )
            self.assertEqual(
                [call.args[1] for call in publish.call_args_list], [0, 1]
            )

    def test_v36_checkpoint_sidecar_drains_commit_racing_producer_exit(self) -> None:
        # Break caught: Rust commits its final ready descriptor between the
        # sidecar's missing-file observation and producer-liveness check.
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            commits = root / "commits"
            commits.mkdir()

            def producer_exits_after_commit(_pid: int) -> bool:
                (commits / "generation-00000000.json").touch()
                return False

            with mock.patch.object(
                subject, "publish_v36_checkpoint_outbox_generation"
            ) as publish:
                self.assertEqual(
                    subject.watch_v36_checkpoint_outbox(
                        root,
                        41,
                        mock.sentinel.transport,
                        producer_alive=producer_exits_after_commit,
                        pause=mock.Mock(),
                    ),
                    1,
                )
            publish.assert_called_once_with(root, 0, mock.sentinel.transport)

    def test_v36_checkpoint_sidecar_treats_zombie_producer_as_exited(self) -> None:
        # Break caught: kill(0) reports a dead-but-unreaped child as live, so
        # both shell and sidecar wait forever and no terminal can be emitted.
        with mock.patch.object(
            subject.pathlib.Path,
            "read_text",
            return_value="41 (v36 prefix) Z 1 2 3\n",
        ), mock.patch.object(subject.os, "kill") as kill:
            self.assertFalse(subject._pid_alive(41))
        kill.assert_not_called()

    def test_v36_checkpoint_sidecar_cli_has_no_scientific_or_storage_surface(self) -> None:
        # Break caught: the sidecar can alter science, discover storage, or run
        # without an explicit local outbox and sole producer PID.
        with mock.patch.object(
            subject, "watch_v36_checkpoint_outbox", return_value=2
        ) as watch:
            self.assertEqual(
                subject.main(
                    [
                        "--publish-checkpoints",
                        "--checkpoint-outbox",
                        "/tmp/outbox",
                        "--producer-pid",
                        "41",
                        "--first-generation",
                        "7",
                    ]
                ),
                0,
            )
        watch.assert_called_once()
        arguments = watch.call_args.args
        self.assertEqual(arguments[:2], (pathlib.Path("/tmp/outbox"), 41))
        self.assertIsInstance(arguments[2], subject.V36AwsCliCheckpointTransport)
        self.assertEqual(watch.call_args.kwargs, {"first_generation": 7})
        with self.assertRaises(SystemExit):
            subject.main(["--publish-checkpoints", "--bucket", "fixture"])


if __name__ == "__main__":
    unittest.main()
