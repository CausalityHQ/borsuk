"""Paired-bootstrap contracts for immutable V104 evidence."""

from __future__ import annotations

import hashlib
import unittest

from scripts.test_v98_hierarchical_row_router import V98ProducerTests
from scripts.test_v104_exact_retention_ladder import V104ExactRetentionLadderTests
from scripts.v104_exact_retention_ladder import (
    canonical_v104_result_bytes,
    evaluate_v104,
)
from scripts.v104_exact_retention_ladder_ci import (
    canonical_v104_ci_bytes,
    reduce_v104_paired_intervals,
)


class V104ExactRetentionCiTests(unittest.TestCase):
    def test_recomputes_registered_10k_paired_intervals_against_1024_pages(self) -> None:
        # Break caught: arms use independent resamples, a nonregistered seed,
        # or a control other than the exact 1,024-page arm.
        inputs = V104ExactRetentionLadderTests.inputs()
        body = canonical_v104_result_bytes(
            evaluate_v104(
                inputs,
                V98ProducerTests.authority(inputs),
                V104ExactRetentionLadderTests.config(),
            )
        )

        receipt = reduce_v104_paired_intervals(
            body, expected_result_sha256=hashlib.sha256(body).hexdigest()
        )

        self.assertEqual(receipt.bootstrap_seed, 7216)
        self.assertEqual(receipt.bootstrap_resamples, 10_000)
        self.assertEqual(receipt.control_retained_pages, 1_024)
        self.assertEqual(tuple(item.retained_pages for item in receipt.arms), (128, 256, 512, 768))
        for item in receipt.arms:
            self.assertEqual(item.average_recall10_ppm, (0, 0))
            self.assertEqual(item.average_recall100_ppm, (0, 0))
            self.assertEqual(item.p05_recall100_ppm, (0, 0))
        self.assertTrue(canonical_v104_ci_bytes(receipt).endswith(b"\n"))

    def test_rejects_wrong_registered_result_identity(self) -> None:
        # Break caught: a CI receipt can be computed over bytes other than the
        # terminal-bound immutable producer result.
        inputs = V104ExactRetentionLadderTests.inputs()
        body = canonical_v104_result_bytes(
            evaluate_v104(
                inputs,
                V98ProducerTests.authority(inputs),
                V104ExactRetentionLadderTests.config(),
            )
        )
        with self.assertRaisesRegex(ValueError, "V104 CI result identity differs"):
            reduce_v104_paired_intervals(body, expected_result_sha256="0" * 64)


if __name__ == "__main__":
    unittest.main()
