from __future__ import annotations

import unittest

from scripts.run_rest_coexistence_attempt import (
    accepted_search_qps,
    select_sustainable_search_qps,
    staircase_has_only_expected_capacity_failures,
    staircase_rates,
    validated_authority_sha256,
    validate_effective_limits,
)


class RestCoexistenceAttemptTest(unittest.TestCase):
    def test_smoke_staircase_crosses_the_expected_small_runtime_knee(self) -> None:
        self.assertEqual(staircase_rates(True), [16, 32, 64, 96])
        self.assertEqual(staircase_rates(False), [8, 16, 32, 64, 96, 128])

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
            "leaf_read_width": 32,
            "max_inflight_leaf_reads": 48,
            "page_budget": 32,
            "ram_budget_bytes": 2 * 1024**3,
            "disk_cache_bytes": 0,
        }
        actual = {
            "borsuk_cpu_threads": 3,
            "borsuk_io_threads": 88,
            "borsuk_s3_get_concurrency": 64,
            "borsuk_search_capacity": 4,
            "borsuk_leaf_read_width": 32,
            "borsuk_leaf_read_capacity": 48,
            "borsuk_page_budget": 32,
            "borsuk_ram_budget_bytes": 2 * 1024**3,
            "borsuk_disk_cache_bytes": 0,
        }
        validate_effective_limits(expected, actual)
        actual["borsuk_leaf_read_capacity"] = 47
        with self.assertRaisesRegex(ValueError, "effective REST limits"):
            validate_effective_limits(expected, actual)


if __name__ == "__main__":
    unittest.main()
