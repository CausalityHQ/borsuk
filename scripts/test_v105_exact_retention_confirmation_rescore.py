"""Independent-reducer contracts for the V105 confirmation."""

from __future__ import annotations

import hashlib
import json
import unittest

from scripts.test_v98_hierarchical_row_router import V98ProducerTests
from scripts.test_v104_exact_retention_ladder import V104ExactRetentionLadderTests
from scripts.v105_exact_retention_confirmation import (
    canonical_v105_result_bytes,
    evaluate_v105,
)
from scripts.v105_exact_retention_confirmation_rescore import (
    canonical_v105_rescore_bytes,
    rescore_v105_result,
)


class V105ExactRetentionConfirmationRescoreTests(unittest.TestCase):
    def test_independently_recomputes_the_single_arm(self) -> None:
        # Break caught: the confirmation trusts producer hits, aggregates, or
        # the pass classification instead of rebuilding the retained fence.
        inputs = V104ExactRetentionLadderTests.inputs()
        authority = V98ProducerTests.authority(inputs)
        config = V104ExactRetentionLadderTests.config()
        body = canonical_v105_result_bytes(evaluate_v105(inputs, authority, config))

        receipt = rescore_v105_result(
            body,
            expected_inputs=inputs,
            expected_authority=authority,
            expected_config=config,
            expected_result_sha256=hashlib.sha256(body).hexdigest(),
        )

        self.assertEqual(receipt.status, "verified")
        self.assertEqual(receipt.retained_pages, 768)
        self.assertEqual(receipt.query_count, 2)
        self.assertTrue(canonical_v105_rescore_bytes(receipt).endswith(b"\n"))

    def test_rejects_recanonicalized_hit_drift(self) -> None:
        # Break caught: canonical producer bytes can alter semantic evidence
        # while retaining a valid outer schema and newly registered hash.
        inputs = V104ExactRetentionLadderTests.inputs()
        authority = V98ProducerTests.authority(inputs)
        config = V104ExactRetentionLadderTests.config()
        body = canonical_v105_result_bytes(evaluate_v105(inputs, authority, config))
        value = json.loads(body)
        value["arm"]["samples"][0]["hits"] -= 1
        mutated = json.dumps(
            value, allow_nan=False, separators=(",", ":"), sort_keys=True
        ).encode() + b"\n"

        with self.assertRaisesRegex(ValueError, "V105 range sample evidence differs"):
            rescore_v105_result(
                mutated,
                expected_inputs=inputs,
                expected_authority=authority,
                expected_config=config,
                expected_result_sha256=hashlib.sha256(mutated).hexdigest(),
            )


if __name__ == "__main__":
    unittest.main()
