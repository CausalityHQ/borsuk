"""Independent-reducer contracts for the V104 capacity screen."""

from __future__ import annotations

import hashlib
import json
import unittest

from scripts.test_v98_hierarchical_row_router import V98ProducerTests
from scripts.test_v104_exact_retention_ladder import V104ExactRetentionLadderTests
from scripts.v104_exact_retention_ladder import (
    canonical_v104_result_bytes,
    evaluate_v104,
)
from scripts.v104_exact_retention_ladder_rescore import (
    canonical_v104_rescore_bytes,
    rescore_v104_result,
)


class V104ExactRetentionRescoreTests(unittest.TestCase):
    def test_recomputes_every_sample_aggregate_and_winner(self) -> None:
        # Break caught: the independent reducer trusts producer aggregates,
        # range hits, scanned-row caps, or the selected winner.
        inputs = V104ExactRetentionLadderTests.inputs()
        authority = V98ProducerTests.authority(inputs)
        config = V104ExactRetentionLadderTests.config()
        body = canonical_v104_result_bytes(evaluate_v104(inputs, authority, config))

        summary = rescore_v104_result(
            body,
            expected_inputs=inputs,
            expected_authority=authority,
            expected_config=config,
            expected_result_sha256=hashlib.sha256(body).hexdigest(),
        )

        self.assertEqual(summary.status, "verified")
        self.assertEqual(summary.query_count, 2)
        self.assertEqual(summary.winner_retained_pages, 128)
        self.assertEqual(len(summary.arms), 5)
        self.assertTrue(canonical_v104_rescore_bytes(summary).endswith(b"\n"))

    def test_rejects_semantically_mutated_sample_under_recanonicalized_bytes(self) -> None:
        # Break caught: valid-looking canonical JSON can re-root a selected
        # page or hit count without the independent reducer noticing.
        inputs = V104ExactRetentionLadderTests.inputs()
        authority = V98ProducerTests.authority(inputs)
        config = V104ExactRetentionLadderTests.config()
        body = canonical_v104_result_bytes(evaluate_v104(inputs, authority, config))
        value = json.loads(body)
        value["arms"][0]["samples"][0]["hits"] -= 1
        mutated = json.dumps(
            value, allow_nan=False, separators=(",", ":"), sort_keys=True
        ).encode() + b"\n"

        with self.assertRaisesRegex(ValueError, "V104 range sample evidence differs"):
            rescore_v104_result(
                mutated,
                expected_inputs=inputs,
                expected_authority=authority,
                expected_config=config,
                expected_result_sha256=hashlib.sha256(mutated).hexdigest(),
            )

    def test_rejects_recanonicalized_winner_drift(self) -> None:
        # Break caught: the decision is producer-controlled rather than the
        # minimum arm independently shown to pass all absolute gates.
        inputs = V104ExactRetentionLadderTests.inputs()
        authority = V98ProducerTests.authority(inputs)
        config = V104ExactRetentionLadderTests.config()
        body = canonical_v104_result_bytes(evaluate_v104(inputs, authority, config))
        value = json.loads(body)
        value["winner_retained_pages"] = 256
        mutated = json.dumps(
            value, allow_nan=False, separators=(",", ":"), sort_keys=True
        ).encode() + b"\n"

        with self.assertRaisesRegex(ValueError, "V104 decision differs"):
            rescore_v104_result(
                mutated,
                expected_inputs=inputs,
                expected_authority=authority,
                expected_config=config,
                expected_result_sha256=hashlib.sha256(mutated).hexdigest(),
            )


if __name__ == "__main__":
    unittest.main()
