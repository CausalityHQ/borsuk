from __future__ import annotations

import base64
import contextlib
import dataclasses
import datetime
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


def _missing_s3() -> mock.Mock:
    client = mock.Mock()
    client.get_object.side_effect = _AwsError("NoSuchKey")
    client.put_object.return_value = {"ETag": '"fixture"'}
    return client


_LAUNCH_TIME = datetime.datetime(2030, 1, 1, tzinfo=datetime.UTC)


def _launch_response(instance_id: str) -> dict[str, object]:
    return {"Instances": [{"InstanceId": instance_id, "LaunchTime": _LAUNCH_TIME}]}


def _described_instance(instance_id: str, client_token: str, state: str) -> dict[str, object]:
    return {
        "ClientToken": client_token,
        "InstanceId": instance_id,
        "LaunchTime": _LAUNCH_TIME,
        "State": {"Name": state},
    }


class _MemoryS3:
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}

    def get_object(self, *, Key: str, **_values: object) -> dict[str, object]:
        if Key not in self.objects:
            raise _AwsError("NoSuchKey")
        body = self.objects[Key]
        return {"Body": io.BytesIO(body), "ContentLength": len(body)}

    def put_object(self, *, Key: str, Body: bytes, **values: object) -> dict[str, object]:
        if values.get("IfNoneMatch") == "*" and Key in self.objects:
            raise _AwsError("PreconditionFailed")
        self.objects[Key] = Body
        return {"ETag": '"stored"'}


class V36PrefixScreenLauncherTests(unittest.TestCase):
    def plan(self) -> subject.V36PrefixScreenPlan:
        return subject.build_v36_prefix_screen_plan(
            run_id="v36-prefix-fixture",
            source_commit="1" * 40,
            source_archive_uri="s3://fixture/v36/source.tar",
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
            aws_cli_uri="s3://fixture/v36/aws-cli-2.36.11-aarch64.tar",
            aws_cli_sha256="6" * 64,
            aws_cli_blake3="b" * 64,
            aws_cli_bytes=16_384,
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
        self.assertEqual(subject.CONTROLLER_GRACE_SECONDS, 1_800)
        self.assertEqual(subject.SPOT_HOURLY_CAP_MICRO_USD, 3_000_000)
        self.assertEqual(subject.CAMPAIGN_CAP_MICRO_USD, 90_000_000)
        self.assertGreaterEqual(
            subject.DISK_PREFLIGHT_BYTES,
            subject.MAX_SOURCE_BYTES
            + subject.TARGET_DISTINCT_ROWS * subject.VECTOR_DIMENSIONS * 4
            + subject.TARGET_DISTINCT_ROWS * subject.VECTOR_DIMENSIONS * 5,
        )

        ec2 = mock.Mock()
        s3 = _missing_s3()
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
                "progress_stop_seconds": 900,
                "region": "eu-central-1",
                "run_id": "v36-prefix-fixture",
                "schema": "borsuk-v36-prefix-screen-dry-run-v2",
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

    def test_v36_prefix_screen_execute_cli_uses_only_registered_aws_clients(self) -> None:
        plan = self.plan()
        ec2 = mock.Mock()
        s3 = mock.Mock()
        with (
            mock.patch.object(subject, "_v36_aws_clients", return_value=(ec2, s3)) as clients,
            mock.patch.object(
                subject,
                "run_v36_prefix_screen",
                return_value=(
                    "s3://fixture/v36/prefix-results/"
                    "attempt-0000/ATTEMPT_COMPLETE.json"
                ),
            ) as run,
            contextlib.redirect_stdout(io.StringIO()) as stdout,
        ):
            self.assertEqual(
                subject.main(
                    [
                        "--execute-prefix-screen",
                        "--plan-json",
                        json.dumps(dataclasses.asdict(plan)),
                        "--launch-nonce",
                        "a" * 32,
                    ]
                ),
                0,
            )
        clients.assert_called_once_with()
        run.assert_called_once_with(
            plan, ec2_client=ec2, s3_client=s3, launch_nonce="a" * 32
        )
        self.assertEqual(
            stdout.getvalue(),
            "s3://fixture/v36/prefix-results/"
            "attempt-0000/ATTEMPT_COMPLETE.json\n",
        )

    def test_v36_prefix_screen_canonical_json_matches_rust_utf8(self) -> None:
        # Break caught: Python escapes non-ASCII source paths while serde_json
        # writes UTF-8, making one authority fail cross-language authentication.
        self.assertEqual(
            subject.canonical_json_bytes({"uri": "s3://fixture/π.parquet"}),
            b'{"uri":"s3://fixture/\xcf\x80.parquet"}\n',
        )
        self.assertIn("ensure_ascii=False", subject._GUEST_TERMINAL_PROGRAM)

    def test_v36_prefix_screen_inputs_derive_from_frozen_dataset_authority(self) -> None:
        source = pathlib.Path(
            "docs/research/v36-funnel-dataset-authority.json"
        ).read_bytes()
        authority_bytes, registry_bytes = subject.derive_v36_prefix_screen_inputs(
            source
        )
        authority = json.loads(authority_bytes)
        registry = json.loads(registry_bytes)
        self.assertEqual(authority_bytes, subject.canonical_json_bytes(authority))
        self.assertEqual(registry_bytes, subject.canonical_json_bytes(registry))
        self.assertEqual(authority["schema"], "borsuk-v36-prefix-freeze-authority-v2")
        self.assertEqual(
            authority["dataset_authority_sha256"],
            "0d2e8cef3cf27860131a6a8c33d08b858f8837263212cb03515ae53c76acd5c1",
        )
        self.assertEqual(authority["cohort_ordinal"], 0)
        self.assertEqual(authority["selected_object_start"], 0)
        self.assertEqual(authority["selected_object_count"], 16)
        self.assertEqual(authority["selected_object_encoded_bytes"], 5_485_265_954)
        self.assertIsNone(authority["excluded_population_identity"])
        self.assertEqual(
            authority["population_sampling_algorithm"],
            "sha256-seed-sha256-manifest-sha256-feature-row-id-le-u64-v2",
        )
        self.assertEqual(
            authority["population_seed_label"],
            "borsuk-v36-prefix-screen-population-row-v2",
        )
        self.assertEqual(
            authority["population_seed_sha256"],
            "bcb490ff7944bfa3a0a6d5abe6d35ba34ecaba60b615e214edb057a1a5b63b8e",
        )
        self.assertEqual(
            authority["corpus_seed_label"],
            "borsuk-v36-prefix-screen-corpus-v2",
        )
        self.assertEqual(
            authority["corpus_seed_sha256"],
            "56b288d41e87d3b4ba97ac02b9944837e6bde8b402b8fab088861a27ef099f8c",
        )
        self.assertEqual(
            authority["future_full_source_exclusion_roles"],
            ["development", "validation", "sealed-holdout", "performance"],
        )
        self.assertEqual(authority["registry_objects"], 2_298)
        self.assertEqual(authority["registry_encoded_bytes"], 787_439_811_692)
        self.assertEqual(len(registry), 2_298)
        self.assertEqual(
            authority["ordered_source_manifest_sha256"],
            "76ac61cf2821a331419ad40d5eb94d2cdafccccf39af17af1d328b7a2f0bc6c7",
        )
        self.assertEqual(
            [role["seed_label"] for role in authority["roles"]],
            [
                "borsuk-v36-prefix-screen-development-query-v2",
                "borsuk-v36-prefix-screen-validation-query-v2",
                "borsuk-v36-prefix-screen-sealed-holdout-query-v2",
                "borsuk-v36-prefix-screen-performance-query-v2",
            ],
        )

        changed = json.loads(source)
        changed["source"]["ordered_shard_manifest"]["sha256"] = "f" * 64
        with self.assertRaisesRegex(ValueError, "dataset authority differs"):
            subject.derive_v36_prefix_screen_inputs(
                subject.canonical_json_bytes(changed)
            )

        changed = json.loads(source)
        changed["observed_at_utc"] = "2099-01-01T00:00:00Z"
        with self.assertRaisesRegex(ValueError, "dataset authority differs"):
            subject.derive_v36_prefix_screen_inputs(
                subject.canonical_json_bytes(changed)
            )

        for mutate in (
            lambda value: value["source"]["shards"].__setitem__(
                0, {**value["source"]["shards"][0], "encoded_bytes": True}
            ),
            lambda value: value["source"]["shards"].__setitem__(
                slice(0, 2), reversed(value["source"]["shards"][:2])
            ),
            lambda value: value["source"]["shards"][0].__setitem__(
                "uri", "https://example.invalid/shard.parquet"
            ),
        ):
            changed = json.loads(source)
            mutate(changed)
            with self.assertRaisesRegex(ValueError, "dataset authority differs"):
                subject.derive_v36_prefix_screen_inputs(
                    subject.canonical_json_bytes(changed)
                )

    def test_v36_prefix_screen_inputs_cli_writes_exact_immutable_files(self) -> None:
        source = pathlib.Path(
            "docs/research/v36-funnel-dataset-authority.json"
        ).read_bytes()
        expected_authority, expected_registry = (
            subject.derive_v36_prefix_screen_inputs(source)
        )
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            source_path = root / "dataset.json"
            authority_path = root / "authority.json"
            registry_path = root / "registry.json"
            source_path.write_bytes(source)
            self.assertEqual(
                subject.main(
                    [
                        "--derive-prefix-inputs",
                        "--dataset-authority",
                        str(source_path),
                        "--authority-output",
                        str(authority_path),
                        "--registry-output",
                        str(registry_path),
                    ]
                ),
                0,
            )
            self.assertEqual(authority_path.read_bytes(), expected_authority)
            self.assertEqual(registry_path.read_bytes(), expected_registry)

            # Exact repeats are idempotent, but a conflicting output is never
            # overwritten by a preparation rerun.
            self.assertEqual(
                subject.main(
                    [
                        "--derive-prefix-inputs",
                        "--dataset-authority",
                        str(source_path),
                        "--authority-output",
                        str(authority_path),
                        "--registry-output",
                        str(registry_path),
                    ]
                ),
                0,
            )
            authority_path.write_bytes(b"conflict\n")
            with self.assertRaisesRegex(ValueError, "local output differs"):
                subject.main(
                    [
                        "--derive-prefix-inputs",
                        "--dataset-authority",
                        str(source_path),
                        "--authority-output",
                        str(authority_path),
                        "--registry-output",
                        str(registry_path),
                    ]
                )

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
            script = spec["UserData"]
            self.assertIsInstance(script, str)
            self.assertLessEqual(len(base64.b64encode(script.encode())), 25_600)
            self.assertIn("--execution-authority", script)
            self.assertIn("--scratch", script)
            self.assertNotIn("--max-source-objects", script)
            self.assertNotIn("--max-source-bytes", script)
            self.assertNotIn("--checkpoint-objects", script)
            self.assertNotIn("--checkpoint-seconds", script)
            self.assertIn(f"test \"$available\" -ge {subject.DISK_PREFLIGHT_BYTES}", script)
            self.assertIn('root_source=$(findmnt -n -o SOURCE /)', script)
            self.assertIn("cache_device=$(lsblk -dpno NAME,TYPE", script)
            self.assertIn('mkfs.xfs -f "$cache_device"', script)
            self.assertIn('mount -o noatime "$cache_device" /mnt', script)
            self.assertIn("swapoff -a", script)
            self.assertLess(
                script.index('mount -o noatime "$cache_device" /mnt'),
                script.index("root=$(mktemp -d /mnt/v36-prefix."),
            )
            self.assertNotIn("--resume-checkpoint", script)
            self.assertIn("set +e", script)
            self.assertIn("status=$?", script)
            self.assertIn("set -e", script)
            self.assertIn("--if-none-match '*'", script)
            self.assertIn("shutdown -h now", script)
            self.assertNotIn("on-demand", script.lower())
            self.assertNotIn("/home/", script)
            self.assertNotIn("devbox", script.lower())

    def test_v36_prefix_screen_launch_record_is_immutable_and_restart_reusable(self) -> None:
        # Break caught: controller restart changes ClientToken/UserData and
        # creates a second producer for the same attempt and checkpoint head.
        plan = self.plan()

        class S3:
            def __init__(self) -> None:
                self.body: bytes | None = None
                self.puts = 0

            def get_object(self, **_values: object) -> dict[str, object]:
                if self.body is None:
                    raise _AwsError("NoSuchKey")
                return {
                    "Body": io.BytesIO(self.body),
                    "ContentLength": len(self.body),
                }

            def put_object(self, **values: object) -> dict[str, object]:
                self.puts += 1
                self.asserted_condition = values.get("IfNoneMatch")
                self.body = values["Body"]  # type: ignore[assignment]
                return {"ETag": '"launch"'}

        s3 = S3()
        first = subject.load_or_create_v36_controller_launch(
            s3, plan, 0, "1" * 32, None
        )
        second = subject.load_or_create_v36_controller_launch(
            s3, plan, 0, "2" * 32, None
        )
        self.assertEqual(first, second)
        self.assertEqual(s3.puts, 1)
        self.assertEqual(s3.asserted_condition, "*")
        self.assertEqual(first["launch_spec"]["ClientToken"], second["launch_spec"]["ClientToken"])
        self.assertEqual(first["execution_authority"]["resume"], None)
        drifted = json.loads(subject.canonical_json_bytes(first))
        drifted["execution_authority"]["active_wall_seconds"] = 43_200.0
        with self.assertRaisesRegex(ValueError, "controller launch differs"):
            subject._validate_v36_controller_launch(
                drifted,
                subject.canonical_json_bytes(drifted),
                plan,
                0,
            )
        mutations = []
        for field, value in (
            ("attempt_ordinal", False),
            ("run_id", "foreign-run"),
        ):
            changed = json.loads(subject.canonical_json_bytes(first))
            changed[field] = value
            mutations.append(changed)
        changed = json.loads(subject.canonical_json_bytes(first))
        changed["launch_spec"]["ClientToken"] = "f" * 64
        mutations.append(changed)
        for changed in mutations:
            with self.subTest(changed=changed):
                with self.assertRaisesRegex(ValueError, "controller launch differs"):
                    subject._validate_v36_controller_launch(
                        changed,
                        subject.canonical_json_bytes(changed),
                        plan,
                        0,
                    )
        with self.assertRaisesRegex(ValueError, "controller launch differs"):
            subject._validate_v36_controller_launch(
                first,
                subject.canonical_json_bytes(first).replace(b'"claim_eligible"', b'"claim_eligible" '),
                plan,
                0,
            )

        denied = mock.Mock()
        denied.get_object.side_effect = _AwsError("AccessDenied")
        with self.assertRaises(_AwsError):
            subject.read_v36_controller_launch_if_present(denied, plan, 0)
        oversized = mock.Mock()
        oversized.get_object.return_value = {
            "Body": io.BytesIO(b"{}"),
            "ContentLength": subject.MAX_CONTROLLER_LAUNCH_BYTES + 1,
        }
        with self.assertRaisesRegex(ValueError, "object length differs"):
            subject.read_v36_controller_launch_if_present(oversized, plan, 0)

        inaccessible = mock.Mock()
        inaccessible.get_object.side_effect = _AwsError("NoSuchKey")
        inaccessible.put_object.side_effect = _AwsError("AccessDenied")
        with self.assertRaises(_AwsError) as denied_write:
            subject.load_or_create_v36_controller_launch(
                inaccessible, plan, 0, "3" * 32, None
            )
        self.assertEqual(
            denied_write.exception.response["Error"]["Code"], "AccessDenied"
        )

    def test_v36_prefix_screen_restart_skips_durable_capacity_rejection(self) -> None:
        # Break caught: an old capacity-rejected candidate becomes available
        # after restart and overlaps the already recorded replacement.
        plan = self.plan()

        class S3:
            def __init__(self) -> None:
                self.objects: dict[str, bytes] = {}

            def get_object(self, *, Key: str, **_values: object) -> dict[str, object]:
                if Key not in self.objects:
                    raise _AwsError("NoSuchKey")
                body = self.objects[Key]
                return {"Body": io.BytesIO(body), "ContentLength": len(body)}

            def put_object(self, *, Key: str, Body: bytes, **_values: object) -> dict[str, object]:
                if Key in self.objects and self.objects[Key] != Body:
                    raise _AwsError("PreconditionFailed")
                self.objects[Key] = Body
                return {"ETag": '"stored"'}

        s3 = S3()
        rejected = subject.load_or_create_v36_controller_launch(
            s3, plan, 0, "1" * 32, None
        )
        subject.write_v36_controller_capacity(s3, plan, rejected)
        replacement = subject.load_or_create_v36_controller_launch(
            s3, plan, 1, "1" * 32, None
        )
        ec2 = mock.Mock()
        ec2.run_instances.return_value = _launch_response("i-replacement")
        ec2.describe_instances.return_value = {
            "Reservations": [{"Instances": [{"State": {"Name": "terminated"}}]}]
        }
        with (
            mock.patch.object(
                subject,
                "_read_attempt_status",
                side_effect=[None, None, "complete"],
            ),
            mock.patch.object(
                subject, "_reconcile_v36_controller_instance", return_value=None
            ),
        ):
            uri = subject.run_v36_prefix_screen(
                plan,
                ec2_client=ec2,
                s3_client=s3,
                launch_nonce="2" * 32,
            )
        self.assertTrue(uri.endswith("attempt-0001/ATTEMPT_COMPLETE.json"))
        ec2.run_instances.assert_called_once_with(**replacement["launch_spec"])

    def test_v36_prefix_screen_capacity_record_rejects_conflicts_and_drift(self) -> None:
        plan = self.plan()
        s3 = _MemoryS3()
        launch = subject.load_or_create_v36_controller_launch(
            s3, plan, 0, "1" * 32, None
        )
        subject.write_v36_controller_capacity(s3, plan, launch)
        self.assertTrue(subject.read_v36_controller_capacity(s3, plan, 0, launch))
        with self.assertRaisesRegex(ValueError, "controller capacity differs"):
            subject.read_v36_controller_capacity(s3, plan, 0, None)

        key = subject._s3(subject._controller_capacity_uri(plan, 0))[1]
        value = json.loads(s3.objects[key])
        value["launch_sha256"] = "f" * 64
        s3.objects[key] = subject.canonical_json_bytes(value)
        with self.assertRaisesRegex(ValueError, "controller capacity differs"):
            subject.read_v36_controller_capacity(s3, plan, 0, launch)

        s3 = _MemoryS3()
        launch = subject.load_or_create_v36_controller_launch(
            s3, plan, 0, "1" * 32, None
        )
        subject.write_v36_controller_capacity(s3, plan, launch)
        subject.write_v36_controller_instance(
            s3, plan, launch, "i-impossible", _LAUNCH_TIME
        )
        with self.assertRaisesRegex(ValueError, "capacity terminal conflict"):
            subject.run_v36_prefix_screen(
                plan,
                ec2_client=mock.Mock(),
                s3_client=s3,
                launch_nonce="2" * 32,
            )

    def test_v36_prefix_screen_instance_record_is_immutable_and_restart_reuses_it(self) -> None:
        # Break caught: after the EC2 response is durably observed, a late
        # controller restart relies on an expired ClientToken and starts a
        # second producer for the same attempt ordinal.
        plan = self.plan()
        s3 = _MemoryS3()
        launch = subject.load_or_create_v36_controller_launch(
            s3, plan, 0, "1" * 32, None
        )
        first = subject.write_v36_controller_instance(
            s3, plan, launch, "i-recorded", _LAUNCH_TIME
        )
        second = subject.write_v36_controller_instance(
            s3, plan, launch, "i-recorded", _LAUNCH_TIME
        )
        self.assertEqual(first, second)
        self.assertEqual(
            subject.read_v36_controller_instance_if_present(s3, plan, 0, launch),
            first,
        )
        self.assertEqual(
            first["controller_deadline_epoch_seconds"],
            launch["controller_deadline_epoch_seconds"],
        )

        ec2 = mock.Mock()
        ec2.describe_instances.return_value = {
            "Reservations": [{"Instances": [{"State": {"Name": "terminated"}}]}]
        }
        with mock.patch.object(
            subject, "_read_attempt_status", side_effect=[None, "complete"]
        ):
            uri = subject.run_v36_prefix_screen(
                plan,
                ec2_client=ec2,
                s3_client=s3,
                launch_nonce="2" * 32,
            )
        self.assertTrue(uri.endswith("attempt-0000/ATTEMPT_COMPLETE.json"))
        ec2.run_instances.assert_not_called()
        ec2.describe_instances.assert_called_once_with(InstanceIds=["i-recorded"])

        drifted = dict(first)
        drifted["launch_sha256"] = "f" * 64
        key = subject._s3(subject._controller_instance_uri(plan, 0))[1]
        s3.objects[key] = subject.canonical_json_bytes(drifted)
        with self.assertRaisesRegex(ValueError, "controller instance differs"):
            subject.read_v36_controller_instance_if_present(s3, plan, 0, launch)

    def test_v36_prefix_screen_lost_launch_response_reconciles_terminal_by_token(self) -> None:
        # Break caught: EC2 accepted the request and the guest finalized, but
        # the controller lost the response before persisting the instance ID.
        plan = self.plan()
        s3 = _MemoryS3()
        launch = subject.load_or_create_v36_controller_launch(
            s3, plan, 0, "1" * 32, None
        )
        terminal = subject._controller_infrastructure_terminal_bytes(
            plan, launch["execution_authority"], "i-response-lost"
        )
        bucket, key = subject._marker_key(plan, 0, "ATTEMPT_FAILED.json")
        s3.put_object(Bucket=bucket, Key=key, Body=terminal, IfNoneMatch="*")
        token = launch["launch_spec"]["ClientToken"]
        ec2 = mock.Mock()
        ec2.describe_instances.return_value = {
            "Reservations": [
                {
                    "Instances": [
                        _described_instance(
                            "i-response-lost", str(token), "terminated"
                        )
                    ]
                }
            ]
        }
        ec2.run_instances.side_effect = _AwsError("InsufficientInstanceCapacity")
        with self.assertRaisesRegex(RuntimeError, "three attempts exhausted"):
            subject.run_v36_prefix_screen(
                plan,
                ec2_client=ec2,
                s3_client=s3,
                launch_nonce="2" * 32,
            )
        recorded = subject.read_v36_controller_instance_if_present(
            s3, plan, 0, launch
        )
        self.assertEqual(recorded["instance_id"], "i-response-lost")
        self.assertEqual(
            ec2.describe_instances.call_args_list[0].kwargs,
            {"Filters": [{"Name": "client-token", "Values": [token]}]},
        )

    def test_v36_prefix_screen_stale_instance_termination_is_idempotent(self) -> None:
        # Break caught: EC2 eventually forgets a terminated instance and a
        # late restart wedges while trying to terminate it again.
        ec2 = mock.Mock()
        ec2.terminate_instances.side_effect = _AwsError("InvalidInstanceID.NotFound")
        subject._terminate_v36_instance(ec2, "i-stale")
        ec2.get_waiter.assert_not_called()

    def test_v36_prefix_screen_stale_recorded_instance_gets_durable_terminal(self) -> None:
        plan = self.plan()
        s3 = _MemoryS3()
        launch = subject.load_or_create_v36_controller_launch(
            s3, plan, 0, "1" * 32, None
        )
        subject.write_v36_controller_instance(
            s3, plan, launch, "i-stale", _LAUNCH_TIME
        )
        ec2 = mock.Mock()
        ec2.describe_instances.side_effect = _AwsError("InvalidInstanceID.NotFound")
        ec2.terminate_instances.side_effect = _AwsError("InvalidInstanceID.NotFound")
        ec2.run_instances.side_effect = _AwsError("InsufficientInstanceCapacity")
        with self.assertRaisesRegex(
            RuntimeError, "bootstrap failed before guest terminal"
        ):
            subject.run_v36_prefix_screen(
                plan,
                ec2_client=ec2,
                s3_client=s3,
                launch_nonce="2" * 32,
            )
        terminal = subject._read_attempt_status(
            s3,
            plan,
            0,
            expected_instance_id="i-stale",
            expected_resume=None,
            return_terminal=True,
        )
        self.assertEqual(terminal["status"], "infrastructure")

    def test_v36_prefix_screen_timeout_accepts_guest_interrupted_terminal(self) -> None:
        # Break caught: the guest publishes its legitimate timeout receipt just
        # before controller termination, which was misclassified as corruption.
        plan = self.plan()
        ec2 = mock.Mock()
        ec2.run_instances.return_value = _launch_response("i-timeout")
        ec2.describe_instances.return_value = {
            "Reservations": [{"Instances": [{"State": {"Name": "running"}}]}]
        }
        deadline = (
            subject._attempt_wall_seconds(0)
            + subject.CONTROLLER_GRACE_SECONDS
            + 1
        )
        with (
            mock.patch.object(
                subject,
                "_read_attempt_status",
                side_effect=[None, None, "interrupted"],
            ),
            mock.patch.object(subject.time, "time", side_effect=[0.0, deadline]),
        ):
            with self.assertRaisesRegex(RuntimeError, "controller deadline"):
                subject.run_v36_prefix_screen(
                    plan,
                    ec2_client=ec2,
                    s3_client=_missing_s3(),
                    launch_nonce="3" * 32,
                )

    def test_v36_prefix_screen_restart_does_not_renew_instance_deadline(self) -> None:
        plan = self.plan()
        s3 = _MemoryS3()
        with mock.patch.object(subject.time, "time", return_value=1_000.0):
            launch = subject.load_or_create_v36_controller_launch(
                s3, plan, 0, "1" * 32, None
            )
        subject.write_v36_controller_instance(
            s3, plan, launch, "i-expired", _LAUNCH_TIME
        )
        ec2 = mock.Mock()
        ec2.describe_instances.return_value = {
            "Reservations": [{"Instances": [{"State": {"Name": "running"}}]}]
        }
        ec2.run_instances.side_effect = _AwsError("InsufficientInstanceCapacity")
        with (
            mock.patch.object(
                subject.time,
                "time",
                return_value=launch["controller_deadline_epoch_seconds"] + 1,
            ),
            self.assertRaisesRegex(RuntimeError, "controller deadline"),
        ):
            subject.run_v36_prefix_screen(
                plan,
                ec2_client=ec2,
                s3_client=s3,
                launch_nonce="2" * 32,
            )
        ec2.terminate_instances.assert_called_once_with(InstanceIds=["i-expired"])
        ec2.run_instances.assert_not_called()

    def test_v36_prefix_screen_runs_bound_sidecar_concurrently_and_fails_fast(self) -> None:
        # Break caught: checkpoints remain on ephemeral NVMe until Rust exits,
        # or science keeps running after its only publisher has failed.
        script = subject.build_v36_prefix_launch_specs(
            self.plan(), launch_nonce="7" * 32, attempt_ordinal=0
        )[0]["UserData"]
        self.assertIn(
            'tar -xf "$root/source.tar" -C "$root/sidecar-source" '
            "scripts/run_v36_prefix_screen.py",
            script,
        )
        self.assertIn('science_pid=$!', script)
        self.assertIn('--publish-checkpoints', script)
        self.assertIn('--producer-pid "$science_pid"', script)
        self.assertIn('--first-generation "$first_generation"', script)
        self.assertIn('first_generation=$(cat "$root/resume/first-generation")', script)
        self.assertIn('--initial-phase "$initial_phase"', script)
        self.assertIn('initial_phase=$(cat "$root/resume/initial-phase")', script)
        self.assertIn('sidecar_pid=$!', script)
        self.assertIn('kill -TERM "$science_pid"', script)
        self.assertIn('sha256sum "$root/sidecar-source/scripts/run_v36_prefix_screen.py"', script)
        self.assertIn('put-object --generate-cli-skeleton input', script)
        self.assertIn('if [[ "$sidecar_status" != 0 ]]; then', script)
        self.assertIn('> "$root/attempt.log" 2>&1', script)
        self.assertIn('>> "$root/attempt.log" 2>&1 &', script)
        diagnostic = (
            "put-object --bucket fixture --key "
            "v36/prefix-results/attempt-0000/FAILURE_DIAGNOSTIC.log "
            '--body "$root/failure.log" --if-none-match \'*\''
        )
        self.assertIn(diagnostic, script)
        self.assertLess(
            script.index(diagnostic), script.index('python3 "$root/write-terminal.py"')
        )
        self.assertNotIn("CHECKPOINT.json", script)

    def test_v36_prefix_screen_uses_authenticated_cli_before_conditional_storage(
        self,
    ) -> None:
        # Break caught: the AMI's unpinned AWS CLI service model rejects the
        # conditional S3 headers and all Spot attempts die before science.
        values = dataclasses.asdict(self.plan())
        values.update(
            aws_cli_uri="s3://fixture/v36/aws-cli-2.36.11-aarch64.tar",
            aws_cli_sha256="6" * 64,
            aws_cli_blake3="b" * 64,
            aws_cli_bytes=16_384,
        )
        plan = subject.build_v36_prefix_screen_plan(**values)
        script = subject.build_v36_prefix_launch_specs(
            plan, launch_nonce="7" * 32, attempt_ordinal=0
        )[0]["UserData"]

        download = (
            "aws s3 cp s3://fixture/v36/aws-cli-2.36.11-aarch64.tar "
            '"$root/aws-cli.tar" --only-show-errors'
        )
        activate = 'export PATH="$root/aws-cli/bin:$PATH"'
        capability = (
            'aws s3api put-object --generate-cli-skeleton input > '
            '"$root/put-object-skeleton.json"'
        )
        self.assertIn("exec >/dev/console 2>&1", script)
        self.assertIn(download, script)
        self.assertIn(
            'test "$(stat -c %s "$root/aws-cli.tar")" = 16384', script
        )
        self.assertIn(
            'test "$(sha256sum "$root/aws-cli.tar" | cut -d\' \' -f1)" = '
            + "6" * 64,
            script,
        )
        self.assertIn(
            'tar -xf "$root/aws-cli.tar" -C "$root/aws-cli"', script
        )
        self.assertIn('tar -xf "$root/source.tar" -C "$root/sidecar-source"', script)
        self.assertNotIn("--zstd", script)
        self.assertIn(activate, script)
        self.assertIn('aws --version 2>&1 | grep -q \'^aws-cli/2.36.11 \'', script)
        self.assertIn(capability, script)
        self.assertIn(
            'set(value) >= {"IfMatch", "IfNoneMatch"}', script
        )
        self.assertNotIn("put-object --generate-cli-skeleton input | grep", script)
        self.assertLess(script.index(download), script.index(activate))
        self.assertLess(script.index(activate), script.index(capability))
        self.assertLess(script.index(capability), script.index("--publish-checkpoints"))
        self.assertEqual(
            plan.aws_cli_uri,
            "s3://fixture/v36/aws-cli-2.36.11-aarch64.tar",
        )
        self.assertEqual(
            [item["role"] for item in subject._execution_authority(plan, 0)["inputs"]],
            ["binary", "freeze-authority", "source-archive", "source-registry"],
        )

    def test_v36_prefix_screen_publishes_artifacts_receipt_then_terminal(self) -> None:
        # Break caught: successful science is shut down before its artifacts
        # and authenticated terminal become durable in the attempt prefix.
        script = subject.build_v36_prefix_launch_specs(
            self.plan(), launch_nonce="f" * 32, attempt_ordinal=0
        )[0]["UserData"]
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
        script = subject.build_v36_prefix_launch_specs(
            self.plan(), launch_nonce="e" * 32, attempt_ordinal=0
        )[0]["UserData"]
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
            _launch_response(f"i-attempt-{ordinal}") for ordinal in range(3)
        ]
        ec2.describe_instances.return_value = {
            "Reservations": [{"Instances": [{"State": {"Name": "terminated"}}]}]
        }
        s3 = _missing_s3()
        with (
            mock.patch.object(subject.time, "sleep"),
            mock.patch.object(
                subject,
                "_read_attempt_status",
                side_effect=[None, "interrupted", "interrupted", None, "infrastructure"],
            ),
            mock.patch.object(
                subject, "read_v36_checkpoint_head_if_present", return_value=None
            ),
        ):
            with self.assertRaisesRegex(
                RuntimeError, "bootstrap failed before guest terminal"
            ):
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

    def test_v36_prefix_screen_stops_after_first_silent_boot_failure(self) -> None:
        # Break caught: identical user-data bootstrap failures consume all
        # three Spot attempts even though no guest terminal or checkpoint was
        # produced and changing availability zones cannot change the result.
        plan = self.plan()
        ec2 = mock.Mock()
        ec2.run_instances.return_value = _launch_response("i-silent-bootstrap")
        ec2.describe_instances.return_value = {
            "Reservations": [{"Instances": [{"State": {"Name": "terminated"}}]}]
        }
        s3 = _MemoryS3()
        with self.assertRaisesRegex(RuntimeError, "bootstrap failed before guest terminal"):
            subject.run_v36_prefix_screen(
                plan,
                ec2_client=ec2,
                s3_client=s3,
                launch_nonce="d" * 32,
            )
        self.assertEqual(ec2.run_instances.call_count, 1)
        self.assertEqual(ec2.terminate_instances.call_count, 1)
        terminal = subject._read_attempt_status(
            s3,
            plan,
            0,
            expected_instance_id="i-silent-bootstrap",
            expected_resume=None,
            return_terminal=True,
        )
        self.assertEqual(terminal["status"], "infrastructure")

    def test_v36_prefix_screen_replacement_binds_newest_head_after_termination(self) -> None:
        # Break caught: a replacement launches fresh or observes a checkpoint
        # while the prior producer can still advance the run-scoped pointer.
        plan = self.plan()
        ec2 = mock.Mock()
        ec2.run_instances.side_effect = [
            _launch_response("i-first"),
            _launch_response("i-second"),
        ]
        ec2.describe_instances.return_value = {
            "Reservations": [{"Instances": [{"State": {"Name": "terminated"}}]}]
        }
        pointer = b"pointer\n"
        manifest = b"manifest\n"
        binding = {
            "generation": 2,
            "manifest": {
                "blake3": "d" * 64,
                "encoded_bytes": 1_024,
                "role": "checkpoint-manifest",
                "sha256": "e" * 64,
                "uri": f"{plan.output_prefix}checkpoints/objects/{'e' * 64}-checkpoint.json",
            },
            "pointer_encoded_bytes": len(pointer),
            "pointer_sha256": hashlib.sha256(pointer).hexdigest(),
            "pointer_uri": f"{plan.output_prefix}checkpoints/runs/{plan.run_id}/latest.json",
        }
        with (
            mock.patch.object(
                subject,
                "_read_attempt_status",
                side_effect=[None, "interrupted", "complete"],
            ),
            mock.patch.object(
                subject,
                "read_v36_checkpoint_head_if_present",
                return_value=(pointer, manifest, "etag"),
            ) as read_head,
            mock.patch.object(
                subject, "v36_checkpoint_resume_binding", return_value=binding
            ) as bind,
            mock.patch.object(
                subject,
                "build_v36_prefix_launch_specs",
                wraps=subject.build_v36_prefix_launch_specs,
            ) as specs,
        ):
            self.assertTrue(
                subject.run_v36_prefix_screen(
                    plan,
                    ec2_client=ec2,
                    s3_client=_missing_s3(),
                    launch_nonce="6" * 32,
                ).endswith("attempt-0001/ATTEMPT_COMPLETE.json")
            )
        read_head.assert_called_once()
        bind.assert_called_once_with(plan, 1, pointer, manifest)
        self.assertIsNone(specs.call_args_list[0].kwargs.get("resume"))
        self.assertEqual(specs.call_args_list[1].kwargs["resume"], binding)
        self.assertEqual(ec2.terminate_instances.call_count, 2)
        self.assertEqual(ec2.get_waiter.return_value.wait.call_count, 2)

    def test_v36_prefix_screen_controller_restart_resumes_after_terminal_interruption(self) -> None:
        # Break caught: controller death after a durable interrupted terminal
        # either blocks the campaign forever or relaunches attempt zero.
        plan = self.plan()
        prior_terminal = {"instance_id": "i-prior", "status": "interrupted"}
        pointer = b"pointer\n"
        manifest = b"manifest\n"
        binding = {
            "generation": 2,
            "manifest": {
                "blake3": "d" * 64,
                "encoded_bytes": 1_024,
                "role": "checkpoint-manifest",
                "sha256": "e" * 64,
                "uri": f"{plan.output_prefix}checkpoints/objects/{'e' * 64}-checkpoint.json",
            },
            "pointer_encoded_bytes": len(pointer),
            "pointer_sha256": hashlib.sha256(pointer).hexdigest(),
            "pointer_uri": f"{plan.output_prefix}checkpoints/runs/{plan.run_id}/latest.json",
        }
        ec2 = mock.Mock()
        ec2.run_instances.return_value = _launch_response("i-replacement")
        ec2.describe_instances.return_value = {
            "Reservations": [{"Instances": [{"State": {"Name": "terminated"}}]}]
        }
        prior_launch = {"execution_authority": subject._execution_authority(plan, 0)}
        with (
            mock.patch.object(
                subject,
                "_read_attempt_status",
                side_effect=[prior_terminal, None, "complete"],
            ),
            mock.patch.object(
                subject,
                "read_v36_checkpoint_head_if_present",
                return_value=(pointer, manifest, "etag"),
            ),
            mock.patch.object(
                subject,
                "read_v36_controller_launch_if_present",
                side_effect=[prior_launch, None, None],
            ),
            mock.patch.object(
                subject,
                "read_v36_controller_instance_if_present",
                side_effect=[{"instance_id": "i-prior"}, None, None],
            ),
            mock.patch.object(
                subject, "v36_checkpoint_resume_binding", return_value=binding
            ),
            mock.patch.object(
                subject,
                "build_v36_prefix_launch_specs",
                wraps=subject.build_v36_prefix_launch_specs,
            ) as specs,
        ):
            uri = subject.run_v36_prefix_screen(
                plan,
                ec2_client=ec2,
                s3_client=_missing_s3(),
                launch_nonce="5" * 32,
            )
        self.assertTrue(uri.endswith("attempt-0001/ATTEMPT_COMPLETE.json"))
        self.assertEqual(specs.call_args.kwargs["attempt_ordinal"], 1)
        self.assertEqual(specs.call_args.kwargs["resume"], binding)
        self.assertEqual(
            ec2.terminate_instances.call_args_list[0].kwargs,
            {"InstanceIds": ["i-prior"]},
        )

    def test_v36_prefix_screen_restart_recovers_launched_replacement_without_head_read(self) -> None:
        # Break caught: a running replacement advances latest.json and restart
        # wrongly treats that new head as the replacement's original resume.
        plan = self.plan()
        resume = {
            "generation": 2,
            "manifest": {
                "blake3": "d" * 64,
                "encoded_bytes": 1_024,
                "role": "checkpoint-manifest",
                "sha256": "e" * 64,
                "uri": f"{plan.output_prefix}checkpoints/objects/{'e' * 64}-checkpoint.json",
            },
            "pointer_encoded_bytes": 512,
            "pointer_sha256": "f" * 64,
            "pointer_uri": f"{plan.output_prefix}checkpoints/runs/{plan.run_id}/latest.json",
        }
        prior_launch = {"execution_authority": subject._execution_authority(plan, 0)}
        replacement_launch = {
            "controller_deadline_epoch_seconds": int(_LAUNCH_TIME.timestamp()),
            "execution_authority": subject._execution_authority(plan, 1, resume=resume),
            "launch_spec": subject.build_v36_prefix_launch_specs(
                plan, launch_nonce="4" * 32, attempt_ordinal=1, resume=resume
            )[1],
        }
        ec2 = mock.Mock()
        ec2.run_instances.return_value = _launch_response("i-replacement")
        ec2.describe_instances.return_value = {
            "Reservations": [{"Instances": [{"State": {"Name": "terminated"}}]}]
        }
        with (
            mock.patch.object(
                subject,
                "_read_attempt_status",
                side_effect=[
                    {"instance_id": "i-prior", "status": "interrupted"},
                    None,
                    "complete",
                ],
            ),
            mock.patch.object(
                subject,
                "read_v36_controller_launch_if_present",
                side_effect=[prior_launch, replacement_launch, replacement_launch],
            ),
            mock.patch.object(
                subject,
                "read_v36_controller_instance_if_present",
                side_effect=[{"instance_id": "i-prior"}, None, None],
            ),
            mock.patch.object(
                subject,
                "read_v36_checkpoint_head_if_present",
                side_effect=AssertionError("running producer owns latest head"),
            ),
            mock.patch.object(
                subject, "_reconcile_v36_controller_instance", return_value=None
            ),
            mock.patch.object(
                subject,
                "write_v36_controller_instance",
                return_value={
                    "controller_deadline_epoch_seconds": int(
                        _LAUNCH_TIME.timestamp()
                    )
                    + subject._attempt_wall_seconds(1)
                    + subject.CONTROLLER_GRACE_SECONDS,
                    "instance_id": "i-replacement",
                },
            ),
        ):
            subject.run_v36_prefix_screen(
                plan,
                ec2_client=ec2,
                s3_client=_missing_s3(),
                launch_nonce="9" * 32,
            )
        ec2.run_instances.assert_called_once_with(**replacement_launch["launch_spec"])

    def test_v36_prefix_screen_capacity_fallback_and_cost_projection_are_closed(self) -> None:
        # Break caught: one unavailable zone aborts the screen, or the three
        # admitted wall caps can exceed the exact $90 ceiling at $3/hour.
        self.assertEqual(
            [subject._attempt_wall_seconds(ordinal) for ordinal in range(3)],
            [43_200, 43_200, 16_200],
        )
        projected_cost = sum(
            (seconds + subject.CONTROLLER_GRACE_SECONDS)
            * subject.SPOT_HOURLY_CAP_MICRO_USD
            // 3_600
            for seconds in [43_200, 43_200, 16_200]
        )
        self.assertEqual(projected_cost, subject.CAMPAIGN_CAP_MICRO_USD)

        plan = self.plan()
        ec2 = mock.Mock()
        ec2.run_instances.side_effect = [
            _AwsError("InsufficientInstanceCapacity"),
            _launch_response("i-capacity-fallback"),
        ]
        ec2.describe_instances.return_value = {
            "Reservations": [{"Instances": [{"State": {"Name": "terminated"}}]}]
        }
        s3 = _missing_s3()
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
            "resume": None,
            "schema": "borsuk-v36-prefix-freeze-execution-authority-v2",
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
            "resume": execution_authority["resume"],
            "run_id": plan.run_id,
            "schema": "borsuk-v36-prefix-freeze-terminal-v2",
            "source_commit": plan.source_commit,
            "status": "complete",
        }

        class S3:
            def __init__(self, value: object) -> None:
                self.value = value

            def get_object(self, **kwargs: object) -> dict[str, object]:
                if not str(kwargs["Key"]).endswith("ATTEMPT_COMPLETE.json"):
                    raise _AwsError("NoSuchKey")
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
        changed_input_type = json.loads(json.dumps(terminal))
        changed_input_type["inputs"][0]["encoded_bytes"] = float(
            changed_input_type["inputs"][0]["encoded_bytes"]
        )
        mutations.append(changed_input_type)
        changed_output = json.loads(json.dumps(terminal))
        changed_output["outputs"][0]["role"] = "source"
        mutations.append(changed_output)
        for mutation in mutations:
            with self.subTest(mutation=mutation):
                with self.assertRaisesRegex(ValueError, "terminal authority differs"):
                    subject._read_attempt_status(
                        S3(mutation), plan, attempt, expected_instance_id="i-fixture"
                    )
        foreign_resume = dict(terminal)
        foreign_resume["resume"] = {"generation": 99}
        with self.assertRaisesRegex(ValueError, "terminal authority differs"):
            subject._read_attempt_status(
                S3(foreign_resume),
                plan,
                attempt,
                expected_instance_id="i-fixture",
                expected_resume=None,
            )

    def test_v36_prefix_screen_controller_terminal_uses_guest_canonical_contract(self) -> None:
        # Break caught: the controller synthesizes a terminal which its own
        # real reader or the guest's canonical contract cannot authenticate.
        plan = self.plan()
        execution = subject._execution_authority(plan, 0)
        expected = subject._controller_infrastructure_terminal_bytes(
            plan, execution, "i-bootstrap"
        )
        s3 = _MemoryS3()
        bucket, key = subject._marker_key(plan, 0, "ATTEMPT_FAILED.json")
        s3.put_object(Bucket=bucket, Key=key, Body=expected, IfNoneMatch="*")
        self.assertEqual(
            subject._read_attempt_status(
                s3,
                plan,
                0,
                expected_instance_id="i-bootstrap",
                expected_resume=None,
                return_terminal=True,
            ),
            json.loads(expected),
        )

        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            execution_path = root / "execution.json"
            terminal_path = root / "terminal.json"
            execution_path.write_bytes(subject.canonical_json_bytes(execution))
            subprocess.run(
                [
                    sys.executable,
                    "-c",
                    subject._GUEST_TERMINAL_PROGRAM,
                    str(execution_path),
                    str(root / "unused-receipt.json"),
                    str(root / "unused-output"),
                    "i-bootstrap",
                    plan.run_id,
                    plan.source_commit,
                    "infrastructure",
                    str(terminal_path),
                ],
                check=True,
            )
            self.assertEqual(terminal_path.read_bytes(), expected)

    def test_v36_prefix_screen_terminal_stops_a_running_instance_immediately(self) -> None:
        # Break caught: completed science keeps a paid instance alive until
        # guest shutdown instead of letting the controller terminate it.
        plan = self.plan()
        ec2 = mock.Mock()
        ec2.run_instances.return_value = _launch_response("i-running-complete")
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
                s3_client=_missing_s3(),
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
        ec2.run_instances.return_value = _launch_response("i-running-hung")
        ec2.describe_instances.return_value = {
            "Reservations": [{"Instances": [{"State": {"Name": "running"}}]}]
        }
        deadline = (
            subject._attempt_wall_seconds(0)
            + subject.CONTROLLER_GRACE_SECONDS
            + 1
        )
        with (
            mock.patch.object(
                subject,
                "_read_attempt_status",
                side_effect=[None, None, None, "infrastructure"],
            ),
            mock.patch.object(subject.time, "time", side_effect=[0.0, deadline]),
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
                    s3_client=_missing_s3(),
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
            {"generation": 2, "schema": "borsuk-v36-prefix-freeze-checkpoint-v2"}
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
                "schema": "borsuk-v36-prefix-checkpoint-pointer-v2",
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

    def test_v36_checkpoint_sidecar_accepts_only_current_v2_schemas(self) -> None:
        manifest = subject.canonical_json_bytes(
            {"generation": 0, "schema": "borsuk-v36-prefix-freeze-checkpoint-v2"}
        )
        pointer_value = {
            "claim_eligible": False,
            "generation": 0,
            "manifest": {
                "blake3": "b" * 64,
                "encoded_bytes": len(manifest),
                "role": "checkpoint-manifest",
                "sha256": hashlib.sha256(manifest).hexdigest(),
                "uri": "s3://fixture/checkpoints/manifests/m0.json",
            },
            "producer_attempt_id": "v36-prefix-screen-fixture-attempt-0000",
            "producer_attempt_ordinal": 0,
            "run_id": "v36-prefix-screen-fixture",
            "schema": "borsuk-v36-prefix-checkpoint-pointer-v2",
        }
        pointer = subject.canonical_json_bytes(pointer_value)
        self.assertEqual(subject._checkpoint_pointer_value(pointer), pointer_value)

        pointer_value["schema"] = "borsuk-v36-prefix-checkpoint-pointer-v1"
        with self.assertRaisesRegex(ValueError, "checkpoint pointer authority differs"):
            subject._checkpoint_pointer_value(
                subject.canonical_json_bytes(pointer_value)
            )

    def test_v36_checkpoint_lost_pointer_ack_accepts_only_intended_bytes(self) -> None:
        # Break caught: a timed-out CAS is retried blindly or a concurrent
        # writer's pointer is accepted as this generation's publication.
        manifest = subject.canonical_json_bytes(
            {"generation": 1, "schema": "borsuk-v36-prefix-freeze-checkpoint-v2"}
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
                "schema": "borsuk-v36-prefix-checkpoint-pointer-v2",
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
                "schema": "borsuk-v36-prefix-checkpoint-pointer-v2",
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

    def test_v36_checkpoint_resume_binding_is_exact_and_precedes_replacement(self) -> None:
        # Break caught: a replacement resumes a foreign run, a same/newer
        # producer, or bytes other than the exact newest head it binds.
        plan = self.plan()
        dependency_sha = "d" * 64
        dependency = {
            "blake3": "e" * 64,
            "encoded_bytes": 1_024,
            "role": "population-identity-run-0000",
            "sha256": dependency_sha,
            "uri": f"{plan.output_prefix}checkpoints/objects/{dependency_sha}-run.arrow",
        }
        manifest = subject.canonical_json_bytes(
            {
                "generation": 3,
                "phase": {"kind": "population"},
                "population": {"identity_runs": [dependency]},
                "producer_attempt_id": f"{plan.run_id}-attempt-0000",
                "producer_attempt_ordinal": 0,
                "run_id": plan.run_id,
                "schema": "borsuk-v36-prefix-freeze-checkpoint-v2",
            }
        )
        manifest_identity = {
            "blake3": "b" * 64,
            "encoded_bytes": len(manifest),
            "role": "checkpoint-manifest",
            "sha256": hashlib.sha256(manifest).hexdigest(),
            "uri": f"{plan.output_prefix}checkpoints/objects/{'a' * 64}-checkpoint.json",
        }
        pointer = subject.canonical_json_bytes(
            {
                "claim_eligible": False,
                "generation": 3,
                "manifest": manifest_identity,
                "producer_attempt_id": f"{plan.run_id}-attempt-0000",
                "producer_attempt_ordinal": 0,
                "run_id": plan.run_id,
                "schema": "borsuk-v36-prefix-checkpoint-pointer-v2",
            }
        )
        expected = {
                "generation": 3,
                "manifest": manifest_identity,
                "pointer_encoded_bytes": len(pointer),
                "pointer_sha256": hashlib.sha256(pointer).hexdigest(),
                "pointer_uri": (
                    f"{plan.output_prefix}checkpoints/runs/{plan.run_id}/latest.json"
                ),
            }
        self.assertEqual(
            subject.v36_checkpoint_resume_binding(plan, 1, pointer, manifest), expected
        )
        self.assertEqual(subject._execution_authority(plan, 1, resume=expected)["resume"], expected)
        with self.assertRaisesRegex(ValueError, "resume binding differs"):
            subject.v36_checkpoint_resume_binding(plan, 0, pointer, manifest)

    def test_v36_checkpoint_resume_closure_tracks_every_phase(self) -> None:
        object_prefix = "s3://fixture/v36/checkpoints/objects/"

        def identity(role: str, ordinal: int, encoded_bytes: int = 1_024) -> dict[str, object]:
            digest = f"{ordinal:064x}"
            return {
                "blake3": f"{ordinal + 100:064x}",
                "encoded_bytes": encoded_bytes,
                "role": role,
                "sha256": digest,
                "uri": f"{object_prefix}{digest}-{role}.blob",
            }

        run = identity("population-identity-run-0000", 1)
        selected = identity("population-selected-identities", 2)
        selection = {
            "cutoff_feature_row_id": 7,
            "cutoff_score_sha256": "a" * 64,
            "eligible_rows": 24,
            "excluded_population_identity": None,
            "excluded_rows": 0,
            "selected_ids": selected,
            "selected_rows": 24,
        }
        artifacts = {
            "population_authority": identity("population-authority", 3),
            "source": identity("source", 4, subject.MAX_CHECKPOINT_DEPENDENCY_BYTES + 1),
            "development_query": identity("development-query", 5),
            "validation_query": identity("validation-query", 6),
            "sealed_holdout_query": identity("sealed-holdout-query", 7),
            "performance_query": identity("performance-query", 8),
        }
        heaps = identity("gt-heaps", 10)
        ground_truth = {
            "heaps": heaps,
            "development": identity("development-gt100", 11),
            "validation": identity("validation-gt100", 12),
            "sealed_holdout": identity("sealed-holdout-gt100", 13),
        }
        pointer = {
            "generation": 2,
            "producer_attempt_id": "v36-prefix-fixture-attempt-0000",
            "producer_attempt_ordinal": 0,
            "run_id": "v36-prefix-fixture",
        }
        binding = {
            "generation": 2,
            "manifest": identity("checkpoint-manifest", 9),
            "pointer_uri": "s3://fixture/v36/checkpoints/runs/v36-prefix-fixture/latest.json",
        }

        def closure(phase: dict[str, object]) -> list[dict[str, object]]:
            manifest = subject.canonical_json_bytes(
                {
                    "generation": 2,
                    "phase": phase,
                    "population": {"identity_runs": [run]},
                    "producer_attempt_id": pointer["producer_attempt_id"],
                    "producer_attempt_ordinal": 0,
                    "run_id": pointer["run_id"],
                    "schema": "borsuk-v36-prefix-freeze-checkpoint-v2",
                }
            )
            return subject._validate_v36_resume_closure(
                binding, pointer, manifest
            )

        self.assertEqual(
            closure({"kind": "selected", "selection": selection}),
            [run, selected],
        )
        self.assertEqual(
            closure(
                {
                    "artifacts": artifacts,
                    "kind": "materialized",
                    "selection": selection,
                }
            ),
            [run, selected, *artifacts.values()],
        )
        self.assertEqual(
            closure(
                {
                    "heaps": heaps,
                    "kind": "ground-truth",
                    "materialized": artifacts,
                    "next_source_ordinal": 1_000_000,
                    "selection": selection,
                }
            ),
            [run, selected, *artifacts.values(), heaps],
        )
        self.assertEqual(
            closure(
                {
                    "ground_truth": ground_truth,
                    "kind": "complete",
                    "materialized": artifacts,
                    "selection": selection,
                }
            ),
            [run, selected, *artifacts.values(), *ground_truth.values()],
        )

    def test_v36_checkpoint_resume_materializes_only_newest_dependency_closure(self) -> None:
        # Break caught: resume lists a prefix, falls back to history, or stages
        # bytes not named by the exact bound newest population manifest.
        dependency = b"arrow-ipc-run"
        dependency_sha = hashlib.sha256(dependency).hexdigest()
        dependency_identity = {
            "blake3": "d" * 64,
            "encoded_bytes": len(dependency),
            "role": "population-identity-run-0000",
            "sha256": dependency_sha,
            "uri": f"s3://fixture/v36/checkpoints/objects/{dependency_sha}-run.arrow",
        }
        manifest = subject.canonical_json_bytes(
            {
                "generation": 4,
                "phase": {"kind": "population"},
                "population": {"identity_runs": [dependency_identity]},
                "producer_attempt_id": "v36-prefix-fixture-attempt-0000",
                "producer_attempt_ordinal": 0,
                "run_id": "v36-prefix-fixture",
                "schema": "borsuk-v36-prefix-freeze-checkpoint-v2",
            }
        )
        manifest_sha = hashlib.sha256(manifest).hexdigest()
        manifest_identity = {
            "blake3": "e" * 64,
            "encoded_bytes": len(manifest),
            "role": "checkpoint-manifest",
            "sha256": manifest_sha,
            "uri": f"s3://fixture/v36/checkpoints/objects/{manifest_sha}-checkpoint.json",
        }
        pointer_uri = "s3://fixture/v36/checkpoints/runs/v36-prefix-fixture/latest.json"
        pointer = subject.canonical_json_bytes(
            {
                "claim_eligible": False,
                "generation": 4,
                "manifest": manifest_identity,
                "producer_attempt_id": "v36-prefix-fixture-attempt-0000",
                "producer_attempt_ordinal": 0,
                "run_id": "v36-prefix-fixture",
                "schema": "borsuk-v36-prefix-checkpoint-pointer-v2",
            }
        )
        binding = {
            "generation": 4,
            "manifest": manifest_identity,
            "pointer_encoded_bytes": len(pointer),
            "pointer_sha256": hashlib.sha256(pointer).hexdigest(),
            "pointer_uri": pointer_uri,
        }

        class Transport:
            def __init__(self) -> None:
                self.reads: list[str] = []

            def read_bytes(self, uri: str, maximum: int) -> bytes:
                self.reads.append(uri)
                value = {pointer_uri: pointer, manifest_identity["uri"]: manifest}[uri]
                if len(value) > maximum:
                    raise AssertionError("unbounded read")
                return value

            def download(self, identity: dict[str, object], path: pathlib.Path) -> None:
                self.reads.append(str(identity["uri"]))
                path.write_bytes(dependency)

        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            transport = Transport()
            self.assertEqual(
                subject.materialize_v36_checkpoint_resume(binding, root, transport), 5
            )
            self.assertEqual((root / "pointer.json").read_bytes(), pointer)
            self.assertEqual((root / "manifest.json").read_bytes(), manifest)
            self.assertEqual(
                (root / "objects" / f"{dependency_sha}.blob").read_bytes(), dependency
            )
            self.assertEqual(transport.reads, [pointer_uri, manifest_identity["uri"], dependency_identity["uri"]])

            (root / "pointer.json").unlink()
            (root / "manifest.json").unlink()
            (root / "objects" / f"{dependency_sha}.blob").unlink()
            (root / "objects").rmdir()
            changed = dict(binding)
            changed["pointer_sha256"] = "f" * 64
            with self.assertRaisesRegex(ValueError, "resume pointer differs"):
                subject.materialize_v36_checkpoint_resume(changed, root, Transport())

    def test_v36_checkpoint_resume_cli_stages_fresh_generation_without_s3(self) -> None:
        # Break caught: a fresh attempt probes checkpoint history or starts its
        # publisher at a generation other than the execution-bound value.
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            authority = root / "execution.json"
            destination = root / "resume"
            destination.mkdir()
            authority.write_bytes(
                subject.canonical_json_bytes(subject._execution_authority(self.plan(), 0))
            )
            with mock.patch.object(subject, "V36AwsCliCheckpointTransport") as transport:
                self.assertEqual(
                    subject.main(
                        [
                            "--materialize-resume",
                            "--execution-authority",
                            str(authority),
                            "--resume-directory",
                            str(destination),
                        ]
                    ),
                    0,
                )
            transport.return_value.read_bytes.assert_not_called()
            self.assertEqual((destination / "first-generation").read_text(), "0\n")
            self.assertEqual((destination / "initial-phase").read_text(), "population\n")

    def test_v36_prefix_screen_replacement_materializes_exact_bound_head(self) -> None:
        # Break caught: replacement user-data starts Rust or its publisher
        # before staging the execution-bound head, or restarts at generation 0.
        plan = self.plan()
        manifest = {
            "blake3": "b" * 64,
            "encoded_bytes": 1_024,
            "role": "checkpoint-manifest",
            "sha256": "a" * 64,
            "uri": f"{plan.output_prefix}checkpoints/objects/{'a' * 64}-checkpoint.json",
        }
        resume = {
            "generation": 7,
            "manifest": manifest,
            "pointer_encoded_bytes": 512,
            "pointer_sha256": "c" * 64,
            "pointer_uri": f"{plan.output_prefix}checkpoints/runs/{plan.run_id}/latest.json",
        }
        script = subject.build_v36_prefix_launch_specs(
            plan,
            launch_nonce="9" * 32,
            attempt_ordinal=1,
            resume=resume,
        )[0]["UserData"]
        materialize = script.index("--materialize-resume")
        science = script.index("--execute-prefix-freeze")
        self.assertLess(materialize, science)
        self.assertIn('--resume-checkpoint "$root/resume"', script)
        self.assertIn('--first-generation "$first_generation"', script)
        self.assertNotIn("--first-generation 0", script)

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
                    "phase": {"kind": "population"},
                    "schema": "borsuk-v36-prefix-freeze-checkpoint-v2",
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
                    "schema": "borsuk-v36-prefix-checkpoint-pointer-v2",
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

    def test_v36_controller_s3_adapter_uses_cli_for_conditional_puts(self) -> None:
        # Break caught: the devbox Botocore model rejects current S3
        # If-None-Match/If-Match parameters before any request is sent.
        runner = mock.Mock()
        runner.json.return_value = {"ETag": '"etag-next"'}
        base = mock.Mock()
        client = subject.V36AwsCliConditionalS3Client(base, runner=runner)
        self.assertEqual(
            client.put_object(
                Bucket="fixture",
                Key="runs/launch.json",
                Body=b"payload",
                IfNoneMatch="*",
            ),
            {"ETag": '"etag-next"'},
        )
        arguments = runner.json.call_args.args[0]
        self.assertEqual(arguments[:3], ["aws", "--profile", "causality"])
        self.assertEqual(arguments[3:5], ["s3api", "put-object"])
        self.assertEqual(arguments[arguments.index("--if-none-match") + 1], "*")
        body_path = pathlib.Path(arguments[arguments.index("--body") + 1])
        self.assertFalse(body_path.exists())
        client.get_object(Bucket="fixture", Key="runs/launch.json")
        base.get_object.assert_called_once_with(
            Bucket="fixture", Key="runs/launch.json"
        )

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
                publish.return_value = "population"
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
                publish.return_value = "population"
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

    def test_v36_checkpoint_sidecar_stops_after_900_seconds_without_progress(self) -> None:
        # Break caught: an alive but wedged producer holds Spot capacity until
        # the outer 12-hour wall cap without completing any durable boundary.
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            (root / "commits").mkdir()
            with self.assertRaisesRegex(RuntimeError, "progress stalled"):
                subject.watch_v36_checkpoint_outbox(
                    root,
                    41,
                    mock.sentinel.transport,
                    initial_phase="materialized",
                    producer_alive=mock.Mock(return_value=True),
                    pause=mock.Mock(),
                    monotonic=mock.Mock(side_effect=[0.0, 900.0]),
                    progress_stop_seconds=900,
                )

    def test_v36_checkpoint_sidecar_does_not_apply_gt_stall_to_population(self) -> None:
        # Break caught: a productive source-object scan is killed by the GT-only
        # row-group/checkpoint deadline before exact truth has started.
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            (root / "commits").mkdir()
            self.assertEqual(
                subject.watch_v36_checkpoint_outbox(
                    root,
                    41,
                    mock.sentinel.transport,
                    initial_phase="population",
                    producer_alive=mock.Mock(side_effect=[True, False]),
                    pause=mock.Mock(),
                    monotonic=mock.Mock(side_effect=[0.0, 900.0]),
                    progress_stop_seconds=900,
                ),
                0,
            )

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
                        "--initial-phase",
                        "materialized",
                    ]
                ),
                0,
            )
        watch.assert_called_once()
        arguments = watch.call_args.args
        self.assertEqual(arguments[:2], (pathlib.Path("/tmp/outbox"), 41))
        self.assertIsInstance(arguments[2], subject.V36AwsCliCheckpointTransport)
        self.assertEqual(
            watch.call_args.kwargs,
            {"first_generation": 7, "initial_phase": "materialized"},
        )
        with self.assertRaises(SystemExit):
            subject.main(["--publish-checkpoints", "--bucket", "fixture"])


if __name__ == "__main__":
    unittest.main()
