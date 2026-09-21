"""Matched V105-versus-V99 comparison contracts."""

from __future__ import annotations

import dataclasses
import hashlib
import json
import unittest

from scripts import test_v98_hierarchical_row_router as v98_tests
from scripts import test_v99_ranked_gap_range_router as v99_tests
from scripts import test_v104_exact_retention_ladder as v104_tests
from scripts.v105_exact_retention_comparison import (
    canonical_v105_comparison_bytes,
    compare_v105_to_v99,
)
from scripts.v105_exact_retention_confirmation import (
    canonical_v105_result_bytes,
    evaluate_v105,
)


class V105ExactRetentionComparisonTests(unittest.TestCase):
    @staticmethod
    def evidence() -> tuple[bytes, bytes]:
        inputs = v104_tests.V104ExactRetentionLadderTests.inputs()
        authority = v98_tests.V98ProducerTests.authority(inputs)
        v105 = canonical_v105_result_bytes(
            evaluate_v105(
                inputs, authority, v104_tests.V104ExactRetentionLadderTests.config()
            )
        )
        value = json.loads(v105)
        v99 = json.dumps(
            {
                "schema": "borsuk-v99-ranked-gap-range-router-v1",
                "authority": value["authority"],
                "config": dataclasses.asdict(v99_tests.V99EvaluationTests.config()),
                "query_count": value["query_count"],
                "exact_samples": value["arm"]["samples"],
            },
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode() + b"\n"
        return v105, v99

    def test_pairs_every_query_against_existing_1024_page_exact_control(self) -> None:
        # Break caught: the reducer compares unmatched queries, a compressed
        # V99 arm, independent resamples, or a control other than exact 1,024.
        v105, v99 = self.evidence()
        receipt = compare_v105_to_v99(
            v105,
            v99,
            expected_v105_sha256=hashlib.sha256(v105).hexdigest(),
            expected_v99_sha256=hashlib.sha256(v99).hexdigest(),
        )

        self.assertEqual(receipt.query_count, 2)
        self.assertEqual(receipt.challenger_retained_pages, 768)
        self.assertEqual(receipt.control_retained_pages, 1_024)
        self.assertEqual(receipt.bootstrap_resamples, 10_000)
        self.assertEqual(receipt.average_recall10_ppm, (0, 0))
        self.assertEqual(receipt.average_recall100_ppm, (0, 0))
        self.assertEqual(receipt.p05_recall100_ppm, (0, 0))
        self.assertTrue(canonical_v105_comparison_bytes(receipt).endswith(b"\n"))

    def test_rejects_recanonicalized_cross_result_query_drift(self) -> None:
        # Break caught: V105 and V99 samples can have different truth authority
        # while still being presented as a paired comparison.
        v105, v99 = self.evidence()
        value = json.loads(v99)
        value["exact_samples"][0]["truth_ids"][0] += 1
        mutated = json.dumps(
            value, allow_nan=False, separators=(",", ":"), sort_keys=True
        ).encode() + b"\n"

        with self.assertRaisesRegex(ValueError, "V105 comparison query binding differs"):
            compare_v105_to_v99(
                v105,
                mutated,
                expected_v105_sha256=hashlib.sha256(v105).hexdigest(),
                expected_v99_sha256=hashlib.sha256(mutated).hexdigest(),
            )


if __name__ == "__main__":
    unittest.main()
