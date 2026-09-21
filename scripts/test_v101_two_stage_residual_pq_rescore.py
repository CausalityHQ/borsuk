"""Independent reducer tests for the V101 two-stage residual PQ screen."""

import copy
import dataclasses
import hashlib
import json
import unittest

from scripts import test_v98_hierarchical_row_router as v98_tests
from scripts.v97_row_width_screen import PageKey, RoutedPage
from scripts.v99_ranked_gap_range_router import RankedGapConfig
from scripts.v101_two_stage_residual_pq_rescore import (
    canonical_v101_rescore_bytes,
    rescore_v101_result,
)
from scripts.v101_two_stage_residual_pq_screen import (
    canonical_v101_result_bytes,
    evaluate_v101,
)


class V101TwoStageResidualPqRescoreTests(unittest.TestCase):
    @staticmethod
    def config() -> RankedGapConfig:
        return RankedGapConfig(8, 65_536, 4_096, 1_024, 262_144, 8_192, 32, 16 * 1024**2)

    def fixture(self):
        inputs = v98_tests.V98ProducerTests.inputs(
            dimensions=16,
            base_pages=128,
            delta_pages=9,
            query_count=2,
            truth_key=PageKey("base", 0),
            far_truth=False,
        )
        inputs = dataclasses.replace(
            inputs,
            training_rows=256,
            training_iterations=1,
            pages={
                key: RoutedPage(key, key.ordinal * 1_024, 1_024)
                for key in inputs.pages
            },
        )
        authority = v98_tests.V98ProducerTests.authority(inputs)
        body = canonical_v101_result_bytes(
            evaluate_v101(inputs, authority, self.config())
        )
        return authority, body

    def test_recomputes_samples_aggregates_intervals_projection_and_decision(self) -> None:
        authority, body = self.fixture()

        summary = rescore_v101_result(
            body,
            expected_authority=authority,
            expected_config=self.config(),
            expected_result_sha256=hashlib.sha256(body).hexdigest(),
        )

        self.assertEqual(summary.query_count, 2)
        self.assertEqual(summary.result_sha256, hashlib.sha256(body).hexdigest())
        self.assertEqual(summary.status, "verified")
        encoded = canonical_v101_rescore_bytes(summary)
        self.assertTrue(encoded.endswith(b"\n"))
        self.assertNotIn(b" ", encoded)

    def test_rejects_sample_aggregate_projection_and_identity_drift(self) -> None:
        authority, body = self.fixture()
        value = json.loads(body)
        cases = []
        sample = copy.deepcopy(value)
        sample["challenger_samples"][0]["hits"] -= 1
        cases.append(sample)
        aggregate = copy.deepcopy(value)
        aggregate["challenger_aggregate"]["average_recall100_ppm"] -= 1
        cases.append(aggregate)
        projection = copy.deepcopy(value)
        projection["projection"]["total_bytes"] -= 1
        cases.append(projection)
        for mutated in cases:
            mutated_body = json.dumps(
                mutated, separators=(",", ":"), sort_keys=True
            ).encode() + b"\n"
            with self.assertRaises(ValueError):
                rescore_v101_result(
                    mutated_body,
                    expected_authority=authority,
                    expected_config=self.config(),
                    expected_result_sha256=hashlib.sha256(mutated_body).hexdigest(),
                )
        with self.assertRaisesRegex(ValueError, "result identity differs"):
            rescore_v101_result(
                body,
                expected_authority=authority,
                expected_config=self.config(),
                expected_result_sha256="0" * 64,
            )


if __name__ == "__main__":
    unittest.main()
