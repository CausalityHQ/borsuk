#!/usr/bin/env python3
"""Static protocol tests for the logical-cell routing campaign runner."""

from __future__ import annotations

import unittest
from pathlib import Path


SOURCE = (Path(__file__).parent / "bench_logical_cell_routing.sh").read_text(
    encoding="utf-8"
)


class BenchLogicalCellRoutingTest(unittest.TestCase):
    def test_production_matrix_is_frozen_and_requires_explicit_execution(self) -> None:
        self.assertIn('BORSUK_RUN_LOGICAL_CELL_ROUTING:-0', SOURCE)
        self.assertIn('CELL_COUNTS=(2000 16000)', SOURCE)
        self.assertIn('WRITERS=(1 8 32)', SOURCE)
        self.assertIn('REPETITIONS=5', SOURCE)
        self.assertIn('OPERATIONS=100', SOURCE)

    def test_arms_share_cohort_and_alternate_order(self) -> None:
        self.assertIn('MODES=(flat quantizer)', SOURCE)
        self.assertIn('MODES=(quantizer flat)', SOURCE)
        self.assertIn('BORSUK_ROUTING_COHORT_SHA256="$cohort_sha256"', SOURCE)

    def test_failure_is_synced_before_success_can_be_marked(self) -> None:
        failure = SOURCE.index('LOGICAL_CELL_ROUTING_FAILED')
        failure_sync = SOURCE.index('sync_results || true', failure)
        complete = SOURCE.rindex("printf 'complete\\n' > \"$OUTPUT/LOGICAL_CELL_ROUTING_COMPLETE\"")
        final_validation = SOURCE.rindex('validate_logical_cell_routing_results.py')
        self.assertLess(failure, failure_sync)
        self.assertLess(failure_sync, complete)
        self.assertLess(complete, final_validation)

    def test_resource_telemetry_and_raw_samples_are_preserved(self) -> None:
        self.assertIn('/usr/bin/time -v', SOURCE)
        self.assertIn('.resources.txt', SOURCE)
        self.assertIn('samples.csv', SOURCE)
        self.assertIn('sync_results', SOURCE)

    def test_remote_sync_can_use_the_instance_role(self) -> None:
        self.assertIn('if [[ -n "${AWS_PROFILE:-}" ]]', SOURCE)
        self.assertIn('aws s3 sync --only-show-errors', SOURCE)


if __name__ == "__main__":
    unittest.main()
