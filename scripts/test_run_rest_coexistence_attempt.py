from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from scripts.run_rest_coexistence_attempt import (
    REST_RESULT_SCHEMA_VERSION,
    accepted_search_qps,
    flow_control_delta,
    main,
    overload_search_qps,
    phase_schedule,
    run_phase_with_warmup,
    select_sustainable_search_qps,
    staircase_has_only_expected_capacity_failures,
    staircase_rates,
    validate_effective_limits,
    validated_authority_sha256,
    validated_aws_identity,
)


class RestCoexistenceAttemptTest(unittest.TestCase):
    def test_flow_control_delta_is_phase_scoped_and_monotonic(self) -> None:
        before = {
            "borsuk_decode_rank_wait_count": 2,
            "borsuk_decode_rank_wait_micros": 40,
            "borsuk_leaf_read_wait_count": 3,
            "borsuk_leaf_read_wait_micros": 100,
            "borsuk_search_rejected": 2,
            "borsuk_search_wait_count": 0,
            "borsuk_search_wait_micros": 0,
            "borsuk_transient_wait_count": 4,
            "borsuk_transient_wait_micros": 80,
        }
        after = {
            "borsuk_decode_rank_wait_count": 7,
            "borsuk_decode_rank_wait_micros": 190,
            "borsuk_leaf_read_wait_count": 8,
            "borsuk_leaf_read_wait_micros": 325,
            "borsuk_search_rejected": 4,
            "borsuk_search_wait_count": 1,
            "borsuk_search_wait_micros": 9,
            "borsuk_transient_wait_count": 7,
            "borsuk_transient_wait_micros": 230,
        }
        self.assertEqual(
            flow_control_delta(before, after),
            {
                "borsuk_decode_rank_wait_count": 5,
                "borsuk_decode_rank_wait_micros": 150,
                "borsuk_leaf_read_wait_count": 5,
                "borsuk_leaf_read_wait_micros": 225,
                "borsuk_search_rejected": 2,
                "borsuk_search_wait_count": 1,
                "borsuk_search_wait_micros": 9,
                "borsuk_transient_wait_count": 3,
                "borsuk_transient_wait_micros": 150,
            },
        )
        after["borsuk_leaf_read_wait_count"] = 2
        with self.assertRaisesRegex(ValueError, "regressed"):
            flow_control_delta(before, after)

    def test_terminal_result_schema_cuts_with_effective_limit_shape(self) -> None:
        self.assertEqual(REST_RESULT_SCHEMA_VERSION, 13)

    def test_terminal_receipt_binds_controller_profile_and_runtime_account(self) -> None:
        self.assertEqual(
            validated_aws_identity(
                "causality", "453182569524", "453182569524"
            ),
            {
                "controller_aws_profile": "causality",
                "aws_account_id": "453182569524",
            },
        )
        with self.assertRaisesRegex(ValueError, "runtime AWS account differs"):
            validated_aws_identity(
                "causality", "453182569524", "139078140588"
            )
        with self.assertRaisesRegex(ValueError, "Causality AWS account"):
            validated_aws_identity(
                "causality", "139078140588", "139078140588"
            )

    def test_smoke_staircase_crosses_the_expected_small_runtime_knee(self) -> None:
        self.assertEqual(staircase_rates(True), [32, 64, 96, 128, 160, 192, 256])
        self.assertEqual(
            staircase_rates(False), [8, 16, 32, 64, 96, 128, 160, 192, 256]
        )

    def test_three_repetitions_rotate_independent_measurement_order(self) -> None:
        self.assertEqual(
            phase_schedule(False, 1),
            {
                "staircase_rates": [8, 16, 32, 64, 96, 128, 160, 192, 256],
                "mixed_phases": ["mixed-normal", "mixed-overload"],
            },
        )
        self.assertEqual(
            phase_schedule(False, 2),
            {
                "staircase_rates": [64, 96, 128, 160, 192, 256, 8, 16, 32],
                "mixed_phases": ["mixed-overload", "mixed-normal"],
            },
        )
        self.assertEqual(
            phase_schedule(False, 3),
            {
                "staircase_rates": [160, 192, 256, 8, 16, 32, 64, 96, 128],
                "mixed_phases": ["mixed-normal", "mixed-overload"],
            },
        )
        with self.assertRaisesRegex(ValueError, "repetition"):
            phase_schedule(False, 0)

    def test_each_measured_phase_has_an_excluded_warmup_and_observed_order(self) -> None:
        observed: list[str] = []
        measured = {"passed": True}
        with (
            patch(
                "scripts.run_rest_coexistence_attempt.run_phase",
                return_value=[],
            ) as warm,
            patch(
                "scripts.run_rest_coexistence_attempt._run",
                return_value=measured,
            ) as run,
        ):
            result = run_phase_with_warmup(
                Path("/tmp/output"),
                "http://127.0.0.1:8080",
                "staircase-64",
                5.0,
                10.0,
                0.0,
                64.0,
                [{"vector": [1.0]}],
                None,
                observed,
            )

        self.assertIs(result, measured)
        warm.assert_called_once_with(
            "http://127.0.0.1:8080",
            5.0,
            0.0,
            64.0,
            [{"vector": [1.0]}],
            256,
            30.0,
        )
        run.assert_called_once()
        self.assertEqual(observed, ["staircase-64"])

    def test_main_persists_the_authorized_repetition_and_observed_order(self) -> None:
        def measured(*args: object) -> dict[str, object]:
            name = str(args[2])
            observed = args[-1]
            assert isinstance(observed, list)
            observed.append(name)
            if name.startswith("staircase-"):
                return {
                    "duration_seconds": 1.0,
                    "passed": True,
                    "search": {"successful_requests": 1, "rejected_429": 0},
                }
            return {"passed": True}

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime = root / "runtime.json"
            runtime.write_text("{}", encoding="utf-8")
            output = root / "output"
            argv = [
                "run_rest_coexistence_attempt.py",
                "--base-url",
                "http://127.0.0.1:8080",
                "--queries",
                str(root / "queries.jsonl"),
                "--output",
                str(output),
                "--expected-runtime",
                str(runtime),
                "--controller-aws-profile",
                "causality",
                "--expected-aws-account",
                "453182569524",
                "--runtime-aws-account",
                "453182569524",
                "--repetition",
                "2",
                "--smoke",
            ]
            with (
                patch("sys.argv", argv),
                patch.dict(os.environ, {"BORSUK_ATTEMPT_AUTHORITY_SHA256": "a" * 64}),
                patch("scripts.run_rest_coexistence_attempt._metrics", return_value={}),
                patch("scripts.run_rest_coexistence_attempt._queries", return_value=[]),
                patch("scripts.run_rest_coexistence_attempt.validate_effective_limits"),
                patch(
                    "scripts.run_rest_coexistence_attempt.run_phase_with_warmup",
                    side_effect=measured,
                ),
            ):
                self.assertEqual(main(), 0)

            terminal = json.loads((output / "REST_RESULT.json").read_text())
            self.assertEqual(terminal["repetition"], 2)
            self.assertEqual(
                terminal["phase_order"],
                [
                    "cheap-baseline",
                    "staircase-96",
                    "staircase-128",
                    "staircase-160",
                    "staircase-192",
                    "staircase-256",
                    "staircase-32",
                    "staircase-64",
                    "mixed-overload",
                    "mixed-normal",
                ],
            )

    def test_attempt_authority_is_a_canonical_digest(self) -> None:
        self.assertEqual(validated_authority_sha256("a" * 64), "a" * 64)
        with self.assertRaisesRegex(ValueError, "authority"):
            validated_authority_sha256("A" * 64)

    def test_accepted_qps_excludes_429_and_errors(self) -> None:
        summary = {
            "duration_seconds": 10.0,
            "search": {
                "requests": 100,
                "successful_requests": 70,
                "rejected_429": 25,
                "errors": 5,
            },
        }
        self.assertEqual(accepted_search_qps(summary), 7.0)

    def test_staircase_selects_highest_clean_recall_qualified_rate(self) -> None:
        rows = [
            {"offered_qps": 16, "accepted_qps": 15.8, "summary": {"passed": True, "search": {"rejected_429": 0}}},
            {"offered_qps": 32, "accepted_qps": 31.5, "summary": {"passed": True, "search": {"rejected_429": 0}}},
            {"offered_qps": 64, "accepted_qps": 40.0, "summary": {"passed": True, "search": {"rejected_429": 3}}},
        ]
        self.assertEqual(select_sustainable_search_qps(rows), 31.5)

    def test_overload_rate_reaches_the_attested_staircase_capacity_boundary(self) -> None:
        self.assertEqual(overload_search_qps(16.0, [16, 32, 64, 96]), 96.0)
        self.assertEqual(overload_search_qps(96.0, [16, 32, 64, 96]), 144.0)

    def test_staircase_never_masks_server_errors_as_capacity(self) -> None:
        expected_boundary = [
            {
                "summary": {
                    "gate_failures": ["non-overload vector phase returned HTTP 429"]
                }
            }
        ]
        self.assertTrue(staircase_has_only_expected_capacity_failures(expected_boundary))
        expected_boundary[0]["summary"]["gate_failures"].append(  # type: ignore[index]
            "vector phase must have successful requests and zero errors"
        )
        self.assertFalse(staircase_has_only_expected_capacity_failures(expected_boundary))

    def test_effective_limit_attestation_is_exact(self) -> None:
        expected = {
            "cpu_threads": 3,
            "io_threads": 88,
            "s3_get_concurrency": 64,
            "search_admission": 4,
            "max_waiting_searches": 16,
            "leaf_read_width": 32,
            "max_inflight_leaf_reads": 48,
            "max_parallel_decode_rank_tasks": 1,
            "page_budget": 32,
            "exact_candidates": 512,
            "exact_read_max_physical_amplification": 5,
            "ram_budget_bytes": 2 * 1024**3,
            "disk_cache_bytes": 0,
        }
        actual = {
            "borsuk_cpu_threads": 3,
            "borsuk_io_threads": 88,
            "borsuk_s3_get_concurrency": 64,
            "borsuk_search_capacity": 4,
            "borsuk_search_waiting_capacity": 16,
            "borsuk_leaf_read_width": 32,
            "borsuk_leaf_read_capacity": 48,
            "borsuk_decode_rank_capacity": 1,
            "borsuk_page_budget": 32,
            "borsuk_exact_candidates": 512,
            "borsuk_exact_read_max_physical_amplification": 5,
            "borsuk_ram_budget_bytes": 2 * 1024**3,
            "borsuk_disk_cache_bytes": 0,
        }
        validate_effective_limits(expected, actual)
        actual["borsuk_leaf_read_capacity"] = 47
        with self.assertRaisesRegex(ValueError, "effective REST limits"):
            validate_effective_limits(expected, actual)


if __name__ == "__main__":
    unittest.main()
