"""Contracts for the V105 full-development 768-page confirmation."""

from __future__ import annotations

import json
import unittest

from scripts.test_v98_hierarchical_row_router import V98ProducerTests
from scripts.test_v104_exact_retention_ladder import V104ExactRetentionLadderTests
from scripts.v105_exact_retention_confirmation import (
    canonical_v105_result_bytes,
    evaluate_v105,
)


class V105ExactRetentionConfirmationTests(unittest.TestCase):
    def test_evaluates_only_registered_768_page_arm(self) -> None:
        # Break caught: the continuation reruns killed ladder arms, changes the
        # hierarchy/page planner, or reports a nonregistered retention size.
        inputs = V104ExactRetentionLadderTests.inputs()
        result = evaluate_v105(
            inputs,
            V98ProducerTests.authority(inputs),
            V104ExactRetentionLadderTests.config(),
        )

        self.assertEqual(result.query_count, 2)
        self.assertEqual(result.arm.retained_pages, 768)
        self.assertEqual(result.arm.maximum_scanned_rows, 196_608)
        self.assertEqual(len(result.arm.samples), 2)
        self.assertEqual(result.classification, "exact-retention-confirmed")
        body = canonical_v105_result_bytes(result)
        self.assertTrue(body.endswith(b"\n"))
        self.assertNotIn(b" ", body)
        self.assertEqual(
            json.loads(body)["schema"],
            "borsuk-v105-exact-retention-confirmation-v1",
        )


if __name__ == "__main__":
    unittest.main()
