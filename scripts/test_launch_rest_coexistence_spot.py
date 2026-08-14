from __future__ import annotations

import hashlib
import json
import unittest

from scripts.launch_rest_coexistence_spot import (
    AttemptObservation,
    build_launch_pair,
    cold_s3_cap_matrix,
    classify_attempt,
    execute_pair,
)


class RestCoexistenceSpotLauncherTest(unittest.TestCase):
    def setUp(self) -> None:
        self.pair = build_launch_pair(
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
            runtime={
                "cpu_threads": 3,
                "io_threads": 88,
                "s3_get_concurrency": 64,
                "search_admission": 4,
                "page_budget": 32,
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
            generator_worker="echo generator",
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
        self.assertIn("MemoryMax=6G", user_data)
        self.assertIn("MemorySwapMax=0", user_data)
        self.assertIn("AllowedCPUs=0-3", user_data)
        self.assertIn("BORSUK_REST_CPU_THREADS=3", user_data)
        self.assertIn("BORSUK_REST_IO_THREADS=88", user_data)
        self.assertIn("BORSUK_REST_S3_GET_CONCURRENCY=64", user_data)
        self.assertNotIn("--setenv=BORSUK_CPU_THREADS=", user_data)
        self.assertIn("BORSUK_REST_SEARCH_ADMISSION=4", user_data)
        self.assertIn("BORSUK_REST_PAGE_BUDGET=32", user_data)
        self.assertIn("BORSUK_REST_LEAF_READ_WIDTH=32", user_data)
        self.assertIn("BORSUK_REST_MAX_INFLIGHT_LEAF_READS=48", user_data)
        self.assertIn("BORSUK_REST_RAM_BUDGET_BYTES=2147483648", user_data)
        self.assertIn("BORSUK_REST_DISK_CACHE_BYTES=0", user_data)
        self.assertIn("--setenv=BORSUK_SOURCE_SHA256=" + "1" * 64, user_data)
        self.assertIn("--setenv=BORSUK_BINARY_SHA256=" + "2" * 64, user_data)
        self.assertIn("--setenv=BORSUK_OUTPUT_URI=s3://", user_data)
        self.assertIn("--if-none-match '*'", user_data)
        self.assertIn("meta-data/spot/instance-action", user_data)
        self.assertIn("rest-server-spot-interruption.json", user_data)
        self.assertIn("shutdown -h now", user_data)

        generator_data = self.pair["generator"]["UserData"]
        self.assertTrue(generator_data.startswith("#!/usr/bin/env bash\n"))
        self.assertIn("MemoryMax=3G", generator_data)
        self.assertIn("tag:Role,Values=rest-server", generator_data)
        self.assertIn("/health", generator_data)
        self.assertIn('--setenv=BORSUK_SERVER_ENDPOINT="$server_endpoint"', generator_data)

    def test_pair_binds_immutable_inputs_and_terminal_prefix(self) -> None:
        receipt = self.pair["receipt"]
        self.assertEqual(receipt["runtime"], self.pair["runtime"])
        self.assertEqual(receipt["runtime"]["disk_cache_bytes"], 0)
        self.assertEqual(receipt["source_sha256"], "1" * 64)
        self.assertEqual(receipt["binary_sha256"], "2" * 64)
        self.assertEqual(receipt["index_receipt_sha256"], "3" * 64)
        self.assertEqual(receipt["dataset_receipt_sha256"], "4" * 64)
        self.assertEqual(
            self.pair["workload"],
            {
                "cheap_baseline": True,
                "measurement_seconds": 120,
                "mixed_normal_search_fraction": 0.20,
                "mixed_normal_sustainable_fraction": 0.70,
                "mixed_overload_sustainable_fraction": 1.50,
                "open_loop": True,
                "repetitions": 3,
                "schema_version": 1,
                "search_staircase_qps": [1, 2, 4, 8, 12, 16, 24, 32],
                "separate_generator": True,
                "warmup_seconds": 30,
            },
        )
        self.assertEqual(
            receipt["workload_sha256"],
            "1a197ffdb8d17a6f127482fa2c8193a8da769860fc31b98d0453d54c9c7aae71",
        )
        self.assertEqual(
            receipt["server_worker_sha256"],
            "5b166d611159fe96100bbea200e41af6b52f47172cf20deaf0d0e8af501e53c5",
        )
        self.assertEqual(
            receipt["generator_worker_sha256"],
            "45556420d3865bae0e73badba84686728c88a061a5b04286017020ac36398276",
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

        def run_instance(_profile: str, request: dict[str, object]) -> str:
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
                run_instance=run_instance,
                terminate_instances=lambda _profile, ids: terminated.extend(ids),
                record_identity=lambda role, instance_id: recorded.append(
                    (role, instance_id)
                ),
            )
        self.assertEqual(launched, ["rest-server", "rest-generator"])
        self.assertEqual(recorded, [("server", "i-server")])
        self.assertEqual(terminated, ["i-server"])

    def test_attempt_number_must_match_immutable_output_prefix(self) -> None:
        config = {
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
            "runtime": {
                "cpu_threads": 3,
                "io_threads": 88,
                "s3_get_concurrency": 64,
                "search_admission": 4,
                "page_budget": 32,
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
                campaign_id="invalid-runtime",
                attempt=1,
                image_id="ami-a",
                subnet_id="subnet-a",
                security_group_id="sg-a",
                instance_profile_arn="arn:a",
                source_sha256="1" * 64,
                binary_sha256="2" * 64,
                index_receipt_sha256="3" * 64,
                dataset_receipt_sha256="4" * 64,
                runtime=runtime,
                output_uri="s3://bucket/invalid-runtime/attempts/0001",
                server_worker="echo server",
                generator_worker="echo generator",
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
            all(cell["io_threads"] >= cell["s3_get_concurrency"] for cell in cells)
        )


if __name__ == "__main__":
    unittest.main()
