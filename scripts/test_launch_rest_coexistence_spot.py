from __future__ import annotations

import hashlib
import json
import shlex
import subprocess
import unittest
from pathlib import Path
from unittest.mock import patch

from scripts.launch_rest_coexistence_spot import (
    AttemptObservation,
    _record_identity,
    _record_launch_authority,
    _resolve_aws_account,
    _user_data,
    build_launch_pair,
    classify_attempt,
    cold_s3_cap_matrix,
    execute_pair,
    generator_worker_script,
    main,
    server_worker_script,
)


class RestCoexistenceSpotLauncherTest(unittest.TestCase):
    def test_controller_resolves_and_rejects_the_wrong_aws_account_before_launch(self) -> None:
        calls: list[list[str]] = []

        def run(command: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
            calls.append(command)
            return subprocess.CompletedProcess(command, 0, stdout="453182569524\n")

        self.assertEqual(
            _resolve_aws_account("causality", run=run),
            "453182569524",
        )
        self.assertEqual(
            calls,
            [
                [
                    "aws",
                    "--profile",
                    "causality",
                    "sts",
                    "get-caller-identity",
                    "--query",
                    "Account",
                    "--output",
                    "text",
                ]
            ],
        )

        def private_account(
            command: list[str], **_kwargs: object
        ) -> subprocess.CompletedProcess[str]:
            return subprocess.CompletedProcess(command, 0, stdout="139078140588\n")

        with self.assertRaisesRegex(ValueError, "expected Causality AWS account"):
            _resolve_aws_account("default", run=private_account)

        with patch(
            "scripts.launch_rest_coexistence_spot.subprocess.run",
            side_effect=private_account,
        ):
            with self.assertRaisesRegex(ValueError, "expected Causality AWS account"):
                _resolve_aws_account("causality")

    def test_main_resolves_account_before_building_or_launching(self) -> None:
        with (
            patch("sys.argv", ["launch_rest_coexistence_spot.py", "config.json"]),
            patch("builtins.open"),
            patch("scripts.launch_rest_coexistence_spot.json.load", return_value={}),
            patch(
                "scripts.launch_rest_coexistence_spot._resolve_aws_account",
                side_effect=ValueError("wrong account"),
            ),
            patch("scripts.launch_rest_coexistence_spot.build_launch_pair") as build,
            patch("scripts.launch_rest_coexistence_spot.execute_pair") as execute,
        ):
            with self.assertRaisesRegex(ValueError, "wrong account"):
                main()
        build.assert_not_called()
        execute.assert_not_called()

    def setUp(self) -> None:
        self.pair = build_launch_pair(
            aws_profile="causality",
            aws_account_id="453182569524",
            campaign_id="rest-coexistence-c2dbbcd",
            attempt=1,
            image_id="ami-0123456789abcdef0",
            subnet_id="subnet-0123456789abcdef0",
            security_group_id="sg-0123456789abcdef0",
            instance_profile_arn=(
                "arn:aws:iam::453182569524:instance-profile/borsuk-bench-profile"
            ),
            source_sha256="1" * 64,
            binary_sha256="2" * 64,
            index_receipt_sha256="3" * 64,
            dataset_receipt_sha256="4" * 64,
            smoke=True,
            runtime={
                "cpu_threads": 3,
                "io_threads": 88,
                "s3_get_concurrency": 64,
                "search_admission": 4,
                "page_budget": 32,
                "exact_candidates": 512,
            "exact_read_max_physical_amplification": 5,
                "leaf_read_width": 32,
                "max_inflight_leaf_reads": 48,
                "ram_budget_bytes": 2 * 1024**3,
                "disk_cache_bytes": 0,
            },
            output_uri=(
                "s3://borsuk-bench-453182569524-euc1/publication/v3/"
                "20260812/rest-coexistence/attempts/0001"
            ),
            server_worker="echo server",
            generator_worker=(
                "# borsuk-rest-mode=smoke\n"
                "# borsuk-rest-repetition=1\n"
                "python3 runner.py --repetition 1 --smoke\n"
            ),
        )

    def test_pair_is_spot_hardened_and_uses_separate_instance_identities(self) -> None:
        server = self.pair["server"]
        generator = self.pair["generator"]
        self.assertEqual(server["InstanceType"], "c7g.xlarge")
        self.assertEqual(generator["InstanceType"], "c7g.large")
        self.assertNotEqual(server["ClientToken"], generator["ClientToken"])
        for request in (server, generator):
            self.assertEqual(request["MinCount"], 1)
            self.assertEqual(request["MaxCount"], 1)
            self.assertEqual(request["InstanceMarketOptions"]["MarketType"], "spot")
            self.assertEqual(
                request["InstanceMarketOptions"]["SpotOptions"],
                {
                    "SpotInstanceType": "one-time",
                    "InstanceInterruptionBehavior": "terminate",
                },
            )
            self.assertEqual(request["InstanceInitiatedShutdownBehavior"], "terminate")
            self.assertEqual(request["MetadataOptions"]["HttpTokens"], "required")
        server_tags = {
            item["Key"]: item["Value"]
            for item in server["TagSpecifications"][0]["Tags"]
        }
        generator_tags = {
            item["Key"]: item["Value"]
            for item in generator["TagSpecifications"][0]["Tags"]
        }
        self.assertEqual(server_tags["Role"], "rest-server")
        self.assertEqual(generator_tags["Role"], "rest-generator")
        self.assertEqual(server_tags["AutoTerminate"], "true")
        self.assertEqual(generator_tags["AutoTerminate"], "true")

    def test_server_user_data_enforces_small_runtime_and_no_swap(self) -> None:
        user_data = self.pair["server"]["UserData"]
        self.assertTrue(user_data.startswith("#!/usr/bin/env bash\n"))
        self.assertIn("export AWS_REGION=eu-central-1", user_data)
        self.assertIn("AWS_DEFAULT_REGION=eu-central-1", user_data)
        self.assertIn("--setenv=AWS_REGION=eu-central-1", user_data)
        self.assertIn("--setenv=AWS_DEFAULT_REGION=eu-central-1", user_data)
        self.assertIn("MemoryMax=6G", user_data)
        self.assertIn("MemorySwapMax=0", user_data)
        self.assertIn("AllowedCPUs=0-3", user_data)
        self.assertIn("BORSUK_REST_CPU_THREADS=3", user_data)
        self.assertIn("BORSUK_REST_IO_THREADS=88", user_data)
        self.assertIn("BORSUK_REST_S3_GET_CONCURRENCY=64", user_data)
        self.assertNotIn("--setenv=BORSUK_CPU_THREADS=", user_data)
        self.assertIn("BORSUK_REST_SEARCH_ADMISSION=4", user_data)
        self.assertIn("BORSUK_REST_PAGE_BUDGET=32", user_data)
        self.assertIn("BORSUK_REST_EXACT_CANDIDATES=512", user_data)
        self.assertIn(
            "BORSUK_REST_EXACT_READ_MAX_PHYSICAL_AMPLIFICATION=5", user_data
        )
        self.assertNotIn("BORSUK_REST_EXACT_HEDGE_AFTER_MS", user_data)
        self.assertIn("BORSUK_REST_LEAF_READ_WIDTH=32", user_data)
        self.assertIn("BORSUK_REST_MAX_INFLIGHT_LEAF_READS=48", user_data)
        self.assertIn("BORSUK_REST_RAM_BUDGET_BYTES=2147483648", user_data)
        self.assertIn("BORSUK_REST_DISK_CACHE_BYTES=0", user_data)
        self.assertIn("BORSUK_CONTROLLER_AWS_PROFILE=causality", user_data)
        self.assertIn("BORSUK_EXPECTED_AWS_ACCOUNT=453182569524", user_data)
        self.assertLess(
            user_data.index("aws sts get-caller-identity"),
            user_data.index("watch_spot_interruption &"),
        )
        self.assertGreaterEqual(
            user_data.count(
                '--expected-bucket-owner "$BORSUK_EXPECTED_AWS_ACCOUNT"'
            ),
            4,
        )
        self.assertIn('"aws_account_id":"%s"', user_data)
        self.assertIn('"controller_aws_profile":"%s"', user_data)
        self.assertIn("--setenv=BORSUK_SOURCE_SHA256=" + "1" * 64, user_data)
        self.assertIn("--setenv=BORSUK_BINARY_SHA256=" + "2" * 64, user_data)
        self.assertIn("--setenv=BORSUK_OUTPUT_URI=s3://", user_data)
        self.assertIn("--if-none-match '*'", user_data)
        self.assertIn("meta-data/spot/instance-action", user_data)
        self.assertIn("rest-server-spot-interruption.json", user_data)
        self.assertIn("systemctl stop borsuk-rest-rest-server.service", user_data)
        self.assertIn("shutdown -h now", user_data)

        generator_data = self.pair["generator"]["UserData"]
        self.assertTrue(generator_data.startswith("#!/usr/bin/env bash\n"))
        self.assertIn("MemoryMax=3G", generator_data)
        self.assertNotIn("describe-instances", generator_data)
        self.assertIn("APP_ENDPOINT.json", generator_data)
        self.assertIn("/health", generator_data)
        self.assertIn('--setenv=BORSUK_SERVER_ENDPOINT="$server_endpoint"', generator_data)
        self.assertIn("systemctl stop borsuk-rest-rest-generator.service", generator_data)
        self.assertIn("BORSUK_CONTROLLER_AWS_PROFILE=causality", generator_data)
        self.assertIn("BORSUK_EXPECTED_AWS_ACCOUNT=453182569524", generator_data)
        self.assertLess(
            generator_data.index("aws sts get-caller-identity"),
            generator_data.index("aws s3 cp"),
        )

        with self.assertRaisesRegex(ValueError, "AWS profile"):
            _user_data(
                aws_profile="causality; touch /tmp/escaped",
                aws_account_id="453182569524",
                campaign_id="size-check",
                attempt=1,
                role="rest-server",
                worker="echo server",
                source_sha256="1" * 64,
                binary_sha256="2" * 64,
                index_receipt_sha256="3" * 64,
                dataset_receipt_sha256="4" * 64,
                attempt_authority_sha256="5" * 64,
                output_uri="s3://bucket/size-check/attempts/0001",
                runtime={key: int(value) for key, value in self.pair["runtime"].items()},
            )

    def test_pair_binds_immutable_inputs_and_terminal_prefix(self) -> None:
        receipt = self.pair["receipt"]
        self.assertEqual(receipt["schema_version"], 3)
        self.assertEqual(receipt["aws_profile"], "causality")
        self.assertEqual(receipt["aws_account_id"], "453182569524")
        self.assertEqual(receipt["runtime"], self.pair["runtime"])
        self.assertEqual(receipt["runtime"]["disk_cache_bytes"], 0)
        self.assertEqual(receipt["source_sha256"], "1" * 64)
        self.assertEqual(receipt["binary_sha256"], "2" * 64)
        self.assertEqual(receipt["index_receipt_sha256"], "3" * 64)
        self.assertEqual(receipt["dataset_receipt_sha256"], "4" * 64)
        self.assertRegex(receipt["attempt_authority_sha256"], r"^[0-9a-f]{64}$")
        for role in ("server", "generator"):
            self.assertIn(
                "BORSUK_ATTEMPT_AUTHORITY_SHA256="
                + receipt["attempt_authority_sha256"],
                self.pair[role]["UserData"],
            )
        self.assertEqual(
            self.pair["workload"],
            {
                "cheap_baseline": True,
                "cheap_qps": 200,
                "measurement_seconds": 10,
                "mixed_normal_sustainable_fraction": 0.70,
                "mixed_overload_sustainable_fraction": 1.50,
                "open_loop": True,
                "phase_order_policy": "cyclic-three-v1",
                "repetition": 1,
                "repetitions": 1,
                "schema_version": 4,
                "search_staircase_qps": [32, 64, 96, 128, 160, 192, 256],
                "separate_generator": True,
                "smoke": True,
                "vector_p99_ms": 100.0,
                "warmup_seconds": 5,
            },
        )
        self.assertEqual(
            receipt["workload_sha256"],
            "1e4a04c414110c1eba9bf4f3e40ef3679c678ab70bb887c79aad91e00417f7c1",
        )
        self.assertEqual(
            receipt["server_worker_sha256"],
            "5b166d611159fe96100bbea200e41af6b52f47172cf20deaf0d0e8af501e53c5",
        )
        self.assertEqual(
            receipt["generator_worker_sha256"],
            "c09d0fc2349ed4b56e7c22d48e00a1f41018672760233e5d82d843395a2343a5",
        )
        for role in ("server", "generator"):
            encoded = json.dumps(
                self.pair[role], sort_keys=True, separators=(",", ":"), allow_nan=False
            ).encode()
            self.assertEqual(
                receipt[f"{role}_launch_sha256"], hashlib.sha256(encoded).hexdigest()
            )
        self.assertTrue(receipt["complete_uri"].endswith("/ATTEMPT_COMPLETE.json"))
        self.assertTrue(receipt["failed_uri"].endswith("/ATTEMPT_FAILED.json"))

    def test_reconciliation_uses_only_terminal_markers_and_both_instance_states(self) -> None:
        running = classify_attempt(AttemptObservation("running", "running", ()))
        self.assertEqual(running.action, "monitor")
        success = classify_attempt(
            AttemptObservation("running", "running", ("ATTEMPT_COMPLETE.json",))
        )
        self.assertEqual(success.action, "terminate-both-success")
        interrupted = classify_attempt(
            AttemptObservation(
                "terminated",
                "running",
                (),
                server_state_reason="Server.SpotInstanceTermination",
            )
        )
        self.assertEqual(interrupted.action, "terminate-peer-and-retry-next-attempt")
        self.assertTrue(interrupted.discard_measurements)
        interrupted_after_complete = classify_attempt(
            AttemptObservation(
                "shutting-down",
                "running",
                ("ATTEMPT_COMPLETE.json",),
                server_state_reason="Server.SpotInstanceTermination",
            )
        )
        self.assertEqual(
            interrupted_after_complete.action, "terminate-peer-and-retry-next-attempt"
        )
        self.assertTrue(interrupted_after_complete.discard_measurements)
        normal_exit = classify_attempt(AttemptObservation("terminated", "running", ()))
        self.assertEqual(normal_exit.action, "monitor")
        timeout = classify_attempt(
            AttemptObservation("terminated", "terminated", (), deadline_expired=True)
        )
        self.assertEqual(timeout.action, "terminate-both-timeout")
        failure = classify_attempt(
            AttemptObservation("running", "running", ("ATTEMPT_FAILED.json",))
        )
        self.assertEqual(failure.action, "terminate-both-failure")
        self.assertFalse(failure.discard_measurements)
        with self.assertRaisesRegex(ValueError, "conflicting terminal markers"):
            classify_attempt(
                AttemptObservation(
                    "running",
                    "running",
                    ("ATTEMPT_COMPLETE.json", "ATTEMPT_FAILED.json"),
                )
            )
        with self.assertRaisesRegex(ValueError, "unrecognized terminal marker"):
            classify_attempt(
                AttemptObservation("running", "running", ("samples_partial.csv",))
            )

    def test_generator_launch_failure_terminates_the_recorded_server(self) -> None:
        launched: list[str] = []
        terminated: list[str] = []
        recorded: list[tuple[str, str]] = []
        events: list[str] = []

        def run_instance(_profile: str, request: dict[str, object]) -> str:
            events.append("launch")
            role = {
                item["Key"]: item["Value"]
                for item in request["TagSpecifications"][0]["Tags"]  # type: ignore[index]
            }["Role"]
            launched.append(role)
            if role == "rest-generator":
                raise RuntimeError("no Spot capacity")
            return "i-server"

        with self.assertRaisesRegex(RuntimeError, "no Spot capacity"):
            execute_pair(
                self.pair,
                profile="causality",
                resolve_account=lambda _profile: "453182569524",
                run_instance=run_instance,
                terminate_instances=lambda _profile, ids: terminated.extend(ids),
                record_authority=lambda: events.append("authority"),
                record_identity=lambda role, instance_id: recorded.append(
                    (role, instance_id)
                ),
            )
        self.assertEqual(launched, ["rest-server", "rest-generator"])
        self.assertEqual(recorded, [("server", "i-server")])
        self.assertEqual(terminated, ["i-server"])
        self.assertEqual(events, ["authority", "launch", "launch"])

    def test_execute_rejects_a_profile_not_bound_into_the_launch_receipt(self) -> None:
        events: list[str] = []
        with self.assertRaisesRegex(ValueError, "profile differs from launch receipt"):
            execute_pair(
                self.pair,
                profile="default",
                run_instance=lambda *_args: events.append("launch"),
                record_authority=lambda: events.append("authority"),
            )
        self.assertEqual(events, [])

    def test_execute_rechecks_numeric_account_before_authority_upload(self) -> None:
        events: list[str] = []
        with self.assertRaisesRegex(ValueError, "resolved account differs"):
            execute_pair(
                self.pair,
                profile="causality",
                resolve_account=lambda _profile: "139078140588",
                run_instance=lambda *_args: events.append("launch"),
                record_authority=lambda: events.append("authority"),
            )
        self.assertEqual(events, [])

    def test_launch_authority_persists_the_exact_worker_plan(self) -> None:
        with patch("scripts.launch_rest_coexistence_spot.subprocess.run") as run:
            _record_launch_authority("causality", self.pair)
        keys = {
            call.args[0][call.args[0].index("--key") + 1]
            for call in run.call_args_list
        }
        self.assertEqual(
            keys,
            {
                self.pair["receipt"]["output_uri"].split("/", 3)[3]
                + "/LAUNCH_RECEIPT.json",
                self.pair["receipt"]["output_uri"].split("/", 3)[3]
                + "/WORKLOAD.json",
                self.pair["receipt"]["output_uri"].split("/", 3)[3]
                + "/LAUNCH_PLAN.json",
            },
        )
        self.assertTrue(all("--if-none-match" in call.args[0] for call in run.call_args_list))
        self.assertTrue(
            all(
                call.args[0][call.args[0].index("--expected-bucket-owner") + 1]
                == "453182569524"
                for call in run.call_args_list
            )
        )

    def test_instance_identity_receipt_binds_the_aws_account_and_profile(self) -> None:
        recorded: list[dict[str, object]] = []

        def run(command: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
            body = command[command.index("--body") + 1]
            recorded.append(json.loads(Path(body).read_text(encoding="utf-8")))
            return subprocess.CompletedProcess(command, 0, stdout="")

        with patch("scripts.launch_rest_coexistence_spot.subprocess.run", side_effect=run):
            _record_identity("causality", self.pair, "server", "i-deadbeef")
        self.assertEqual(
            recorded,
            [
                {
                    "schema_version": 2,
                    "aws_profile": "causality",
                    "aws_account_id": "453182569524",
                    "role": "server",
                    "instance_id": "i-deadbeef",
                    "launch_sha256": self.pair["receipt"]["server_launch_sha256"],
                }
            ],
        )

    def test_attempt_number_must_match_immutable_output_prefix(self) -> None:
        config = {
            "aws_profile": "causality",
            "aws_account_id": "453182569524",
            "campaign_id": "rest-coexistence-c2dbbcd",
            "attempt": 2,
            "image_id": "ami-0123456789abcdef0",
            "subnet_id": "subnet-0123456789abcdef0",
            "security_group_id": "sg-0123456789abcdef0",
            "instance_profile_arn": (
                "arn:aws:iam::453182569524:instance-profile/borsuk-bench-profile"
            ),
            "source_sha256": "1" * 64,
            "binary_sha256": "2" * 64,
            "index_receipt_sha256": "3" * 64,
            "dataset_receipt_sha256": "4" * 64,
            "smoke": True,
            "runtime": {
                "cpu_threads": 3,
                "io_threads": 88,
                "s3_get_concurrency": 64,
                "search_admission": 4,
                "page_budget": 32,
                "exact_candidates": 512,
                "leaf_read_width": 32,
                "max_inflight_leaf_reads": 48,
                "ram_budget_bytes": 2 * 1024**3,
                "disk_cache_bytes": 0,
            },
            "output_uri": (
                "s3://borsuk-bench-453182569524-euc1/publication/v3/20260812/"
                "rest-coexistence/attempts/0001"
            ),
            "server_worker": "echo server",
            "generator_worker": "echo generator",
        }
        with self.assertRaisesRegex(ValueError, "attempt path"):
            build_launch_pair(**config)

    def test_runtime_limits_fail_closed_before_launch(self) -> None:
        runtime = dict(self.pair["runtime"])
        runtime["io_threads"] = 32
        runtime["s3_get_concurrency"] = 64
        with self.assertRaisesRegex(ValueError, "io_threads"):
            build_launch_pair(
                aws_profile="causality",
                aws_account_id="453182569524",
                campaign_id="invalid-runtime",
                attempt=1,
                image_id="ami-a",
                subnet_id="subnet-a",
                security_group_id="sg-a",
                instance_profile_arn=(
                    "arn:aws:iam::453182569524:instance-profile/borsuk-bench-profile"
                ),
                source_sha256="1" * 64,
                binary_sha256="2" * 64,
                index_receipt_sha256="3" * 64,
                dataset_receipt_sha256="4" * 64,
                smoke=True,
                runtime=runtime,
                output_uri="s3://bucket/invalid-runtime/attempts/0001",
                server_worker="echo server",
                generator_worker="echo generator",
            )

    def test_launch_rejects_an_instance_profile_from_another_account(self) -> None:
        with self.assertRaisesRegex(ValueError, "instance profile.*Causality"):
            build_launch_pair(
                aws_profile="causality",
                aws_account_id="453182569524",
                campaign_id="wrong-instance-account",
                attempt=1,
                image_id="ami-a",
                subnet_id="subnet-a",
                security_group_id="sg-a",
                instance_profile_arn=(
                    "arn:aws:iam::139078140588:instance-profile/private-bench"
                ),
                source_sha256="1" * 64,
                binary_sha256="2" * 64,
                index_receipt_sha256="3" * 64,
                dataset_receipt_sha256="4" * 64,
                smoke=True,
                runtime={key: int(value) for key, value in self.pair["runtime"].items()},
                output_uri="s3://bucket/wrong-instance-account/attempts/0001",
                server_worker="echo server",
                generator_worker="# borsuk-rest-mode=smoke\necho generator",
            )

    def test_runtime_accepts_engine_supported_64_page_recall_ablation(self) -> None:
        runtime = dict(self.pair["runtime"])
        runtime["page_budget"] = 64
        pair = build_launch_pair(
            aws_profile="causality",
            aws_account_id="453182569524",
            campaign_id="page-64",
            attempt=1,
            image_id="ami-a",
            subnet_id="subnet-a",
            security_group_id="sg-a",
            instance_profile_arn=(
                "arn:aws:iam::453182569524:instance-profile/borsuk-bench-profile"
            ),
            source_sha256="1" * 64,
            binary_sha256="2" * 64,
            index_receipt_sha256="3" * 64,
            dataset_receipt_sha256="4" * 64,
            smoke=True,
            runtime=runtime,
            output_uri="s3://bucket/page-64/attempts/0001",
            server_worker="echo server",
            generator_worker=(
                "# borsuk-rest-mode=smoke\n"
                "# borsuk-rest-repetition=1\n"
                "python3 runner.py --repetition 1 --smoke\n"
            ),
        )
        self.assertEqual(pair["runtime"]["page_budget"], 64)

    def test_workload_receipt_rejects_a_worker_for_the_other_mode(self) -> None:
        with self.assertRaisesRegex(ValueError, "worker mode"):
            build_launch_pair(
                aws_profile="causality",
                aws_account_id="453182569524",
                campaign_id="wrong-worker-mode",
                attempt=1,
                image_id="ami-a",
                subnet_id="subnet-a",
                security_group_id="sg-a",
                instance_profile_arn=(
                    "arn:aws:iam::453182569524:instance-profile/borsuk-bench-profile"
                ),
                source_sha256="1" * 64,
                binary_sha256="2" * 64,
                index_receipt_sha256="3" * 64,
                dataset_receipt_sha256="4" * 64,
                smoke=False,
                runtime={key: int(value) for key, value in self.pair["runtime"].items()},
                output_uri="s3://bucket/wrong-worker-mode/attempts/0001",
                server_worker="echo server",
                generator_worker="# borsuk-rest-mode=smoke\necho generator",
            )
        with self.assertRaisesRegex(ValueError, "repetition identity"):
            build_launch_pair(
                aws_profile="causality",
                aws_account_id="453182569524",
                campaign_id="missing-repetition",
                attempt=1,
                image_id="ami-a",
                subnet_id="subnet-a",
                security_group_id="sg-a",
                instance_profile_arn=(
                    "arn:aws:iam::453182569524:instance-profile/borsuk-bench-profile"
                ),
                source_sha256="1" * 64,
                binary_sha256="2" * 64,
                index_receipt_sha256="3" * 64,
                dataset_receipt_sha256="4" * 64,
                smoke=True,
                runtime={key: int(value) for key, value in self.pair["runtime"].items()},
                output_uri="s3://bucket/missing-repetition/attempts/0001",
                server_worker="echo server",
                generator_worker="# borsuk-rest-mode=smoke\necho generator",
            )

    def test_workload_receipt_rejects_repetition_marker_body_mismatch(self) -> None:
        with self.assertRaisesRegex(ValueError, "executed repetition"):
            build_launch_pair(
                aws_profile="causality",
                aws_account_id="453182569524",
                campaign_id="mismatched-repetition",
                attempt=1,
                image_id="ami-a",
                subnet_id="subnet-a",
                security_group_id="sg-a",
                instance_profile_arn=(
                    "arn:aws:iam::453182569524:instance-profile/borsuk-bench-profile"
                ),
                source_sha256="1" * 64,
                binary_sha256="2" * 64,
                index_receipt_sha256="3" * 64,
                dataset_receipt_sha256="4" * 64,
                smoke=True,
                runtime={key: int(value) for key, value in self.pair["runtime"].items()},
                output_uri="s3://bucket/mismatched-repetition/attempts/0001",
                server_worker="echo server",
                generator_worker=(
                    "# borsuk-rest-mode=smoke\n"
                    "# borsuk-rest-repetition=3\n"
                    "python3 runner.py --repetition 1 --smoke\n"
                ),
            )

    def test_workload_receipt_rejects_mode_marker_body_mismatch(self) -> None:
        with self.assertRaisesRegex(ValueError, "executed mode"):
            build_launch_pair(
                aws_profile="causality",
                aws_account_id="453182569524",
                campaign_id="mismatched-mode",
                attempt=1,
                image_id="ami-a",
                subnet_id="subnet-a",
                security_group_id="sg-a",
                instance_profile_arn=(
                    "arn:aws:iam::453182569524:instance-profile/borsuk-bench-profile"
                ),
                source_sha256="1" * 64,
                binary_sha256="2" * 64,
                index_receipt_sha256="3" * 64,
                dataset_receipt_sha256="4" * 64,
                smoke=True,
                runtime={key: int(value) for key, value in self.pair["runtime"].items()},
                output_uri="s3://bucket/mismatched-mode/attempts/0001",
                server_worker="echo server",
                generator_worker=(
                    "# borsuk-rest-mode=smoke\n"
                    "# borsuk-rest-repetition=1\n"
                    "python3 runner.py --repetition 1\n"
                ),
            )

    def test_cold_s3_matrix_separates_search_wave_handle_and_process_caps(self) -> None:
        cells = cold_s3_cap_matrix()
        self.assertEqual(len(cells), 9)
        self.assertEqual(len({cell["cell_id"] for cell in cells}), 9)
        self.assertEqual(
            {cell["search_admission"] for cell in cells}, {2, 4, 8}
        )
        self.assertEqual(
            {
                (
                    cell["leaf_read_width"],
                    cell["max_inflight_leaf_reads"],
                    cell["s3_get_concurrency"],
                )
                for cell in cells
            },
            {(16, 32, 32), (32, 48, 64), (32, 96, 128)},
        )
        self.assertTrue(all(cell["disk_cache_bytes"] == 0 for cell in cells))
        self.assertTrue(
            all(cell["exact_read_max_physical_amplification"] == 1 for cell in cells)
        )
        self.assertTrue(all("exact_hedge_after_ms" not in cell for cell in cells))
        self.assertTrue(
            all(cell["io_threads"] >= cell["s3_get_concurrency"] for cell in cells)
        )

    def test_workers_verify_immutable_inputs_and_own_terminal_lifecycle(self) -> None:
        server = server_worker_script(
            binary_uri="s3://bucket/build/rest_app_bench",
            binary_sha256="a" * 64,
            index_uri="s3://bucket/index",
            index_receipt_uri="s3://bucket/INDEX_COMPLETE.json",
            index_receipt_sha256="e" * 64,
            index_source_sha256="f" * 64,
        )
        self.assertIn('test "$(sha256sum "$binary"', server)
        self.assertIn("BORSUK_REST_INDEX_URI=s3://bucket/index", server)
        self.assertIn("index receipt does not authorize the requested index", server)
        self.assertIn("source_archive_sha256", server)
        self.assertIn("BORSUK_INDEX_SOURCE_SHA256=" + "f" * 64, server)
        self.assertIn("/metrics", server)
        self.assertIn("APP_ENDPOINT.json", server)
        self.assertIn("resources.csv", server)
        self.assertIn("ATTEMPT_COMPLETE.json", server)
        self.assertIn("rest-generator-spot-interruption.json", server)
        self.assertIn("cgroup_swap_bytes", server)
        self.assertIn("psi_full_avg10", server)
        self.assertIn("memory.events.before", server)
        self.assertIn("memory-events.json", server)
        server_memory_gate = next(
            line for line in server.splitlines() if "server cgroup memory events changed" in line
        )
        compile(shlex.split(server_memory_gate)[2], "server-memory-gate", "exec")
        self.assertIn("generator evidence authority differs", server)
        self.assertLess(
            server.index("aws sts get-caller-identity"),
            server.index("aws s3 cp"),
        )
        self.assertIn('touch "$work/stop-sampler"', server)
        self.assertLess(
            server.index('touch "$work/stop-sampler"'),
            server.rindex('kill "$app_pid"; wait "$app_pid"'),
        )
        subprocess.run(["bash", "-n"], input=server, text=True, check=True)
        generator = generator_worker_script(
            runner_uri="s3://bucket/run-attempt.py",
            runner_sha256="b" * 64,
            load_uri="s3://bucket/load.py",
            load_sha256="d" * 64,
            source_uri="s3://bucket/source.tar.gz",
            source_sha256="f" * 64,
            queries_uri="s3://bucket/queries.jsonl",
            queries_sha256="c" * 64,
            dataset_receipt_uri="s3://bucket/STAGING_COMPLETE.json",
            dataset_receipt_sha256="9" * 64,
            dataset_id="sift-128",
            runtime={key: int(value) for key, value in self.pair["runtime"].items()},
            repetition=2,
        )
        self.assertIn('test "$(sha256sum "$runner"', generator)
        self.assertIn('test "$(sha256sum "$load"', generator)
        self.assertIn('test "$(sha256sum "$source"', generator)
        self.assertIn('test "$(sha256sum "$dataset_receipt"', generator)
        self.assertIn("dataset receipt identity differs", generator)
        self.assertIn("generator-resources.json", generator)
        self.assertIn("cpu_fraction", generator)
        self.assertIn(r'+"\n"', generator)
        resource_gate = next(
            line for line in generator.splitlines() if "generator resource gate failed" in line
        )
        compile(shlex.split(resource_gate)[2], "generator-resource-gate", "exec")
        self.assertGreater(
            generator.index("cpu_before="),
            generator.index("dataset receipt identity differs"),
        )
        self.assertIn("server evidence authority differs", generator)
        self.assertIn('test "$(sha256sum "$queries"', generator)
        self.assertIn("run_rest_coexistence_attempt.py", generator)
        self.assertIn("aws sts get-caller-identity", generator)
        self.assertIn('test "$runtime_aws_account" = "$BORSUK_EXPECTED_AWS_ACCOUNT"', generator)
        self.assertIn('--controller-aws-profile "$BORSUK_CONTROLLER_AWS_PROFILE"', generator)
        self.assertIn('--expected-aws-account "$BORSUK_EXPECTED_AWS_ACCOUNT"', generator)
        self.assertIn('--runtime-aws-account "$runtime_aws_account"', generator)
        self.assertIn("--repetition 2", generator)
        self.assertIn("terminal repetition differs", generator)
        self.assertIn('v.get("schema_version")==10', generator)
        self.assertTrue(
            generator.startswith(
                "# borsuk-rest-mode=smoke\n# borsuk-rest-repetition=2\n"
            )
        )
        self.assertLess(
            generator.index("aws sts get-caller-identity"),
            generator.index("aws s3 cp"),
        )
        self.assertIn("ATTEMPT_COMPLETE.json", generator)
        self.assertIn("rest-server-spot-interruption.json", generator)
        self.assertIn("rest-generator-spot-interruption.json", generator)
        self.assertIn("--if-none-match '*'", generator)
        self.assertIn('--expected-bucket-owner "$BORSUK_EXPECTED_AWS_ACCOUNT"', server)
        self.assertIn('--expected-bucket-owner "$BORSUK_EXPECTED_AWS_ACCOUNT"', generator)
        subprocess.run(["bash", "-n"], input=generator, text=True, check=True)
        runtime = {key: int(value) for key, value in self.pair["runtime"].items()}
        for role, worker in (("rest-server", server), ("rest-generator", generator)):
            user_data = _user_data(
                aws_profile="causality",
                aws_account_id="453182569524",
                campaign_id="size-check",
                attempt=1,
                role=role,
                worker=worker,
                source_sha256="1" * 64,
                binary_sha256="2" * 64,
                index_receipt_sha256="3" * 64,
                dataset_receipt_sha256="4" * 64,
                attempt_authority_sha256="5" * 64,
                output_uri="s3://bucket/size-check/attempts/0001",
                runtime=runtime,
            )
            if role == "rest-generator":
                self.assertIn("FAILURE.log", user_data)
                self.assertIn("diagnostic-output", user_data)
                self.assertLess(
                    user_data.index("diagnostic-output"),
                    user_data.index('ATTEMPT_FAILED.json'),
                )
            self.assertLessEqual(len(user_data.encode()), 16 * 1024)


if __name__ == "__main__":
    unittest.main()
