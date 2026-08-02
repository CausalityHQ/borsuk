#!/usr/bin/env python3
"""Static contract tests for the bounded routing diagnostic."""

from __future__ import annotations

import unittest
from pathlib import Path


SOURCE = (
    Path(__file__).parent / "bench_logical_cell_routing_diagnostic.sh"
).read_text(encoding="utf-8")


class BenchLogicalCellRoutingDiagnosticTest(unittest.TestCase):
    def test_shape_is_bounded_and_claim_ineligible(self) -> None:
        self.assertIn("BORSUK_ROUTING_DIAGNOSTIC=1", SOURCE)
        self.assertIn("BORSUK_ROUTING_CELL_COUNT=2000", SOURCE)
        self.assertIn("BORSUK_ROUTING_WRITERS=8", SOURCE)
        self.assertIn("BORSUK_ROUTING_OPERATIONS_PER_WRITER=5", SOURCE)
        self.assertIn("BORSUK_ROUTING_WARMUP_OPERATIONS_PER_WRITER=2", SOURCE)
        self.assertIn("claim_eligible=false", SOURCE)

    def test_failure_and_resource_evidence_are_synced(self) -> None:
        failure = SOURCE.index("LOGICAL_CELL_ROUTING_DIAGNOSTIC_FAILED")
        failure_sync = SOURCE.index("sync_results || true", failure)
        complete = SOURCE.rindex("LOGICAL_CELL_ROUTING_DIAGNOSTIC_COMPLETE")
        self.assertIn("/usr/bin/time -v", SOURCE)
        self.assertLess(failure, failure_sync)
        self.assertLess(failure_sync, complete)


if __name__ == "__main__":
    unittest.main()
