"""Independent reducer tests for the V100 page-residual screen."""

import copy
import dataclasses
import hashlib
import json
import unittest

from scripts import test_v98_hierarchical_row_router as v98_tests
from scripts.v97_row_width_screen import PageKey, RoutedPage
from scripts.v99_ranked_gap_range_router import RankedGapConfig
from scripts.v100_page_residual_range_rescore import (
    canonical_v100_rescore_bytes,
    rescore_v100_result,
)
from scripts.v100_page_residual_range_screen import (
    canonical_v100_result_bytes,
    evaluate_v100,
)


class V100PageResidualRescoreTests(unittest.TestCase):
    @staticmethod
    def config() -> RankedGapConfig:
        return RankedGapConfig(
            pages_per_root=8,
            maximum_root_groups=65_536,
            maximum_exposed_pages=4_096,
            retained_pages=1_024,
            maximum_scanned_rows=262_144,
            shortlist_rows=8_192,
            maximum_gets=32,
            maximum_bytes=16 * 1024**2,
        )

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
            pages={
                key: RoutedPage(key, key.ordinal * 1_024, 1_024) for key in inputs.pages
            },
        )
        authority = v98_tests.V98ProducerTests.authority(inputs)
        result = evaluate_v100(inputs, authority, self.config())
        body = canonical_v100_result_bytes(result)
        return authority, body

    def test_recomputes_all_samples_aggregates_intervals_and_classification(
        self,
    ) -> None:
        authority, body = self.fixture()
        summary = rescore_v100_result(
            body,
            expected_authority=authority,
            expected_config=self.config(),
            expected_result_sha256=hashlib.sha256(body).hexdigest(),
        )

        self.assertEqual(summary.query_count, 2)
        self.assertEqual(summary.result_sha256, hashlib.sha256(body).hexdigest())
        self.assertEqual(summary.status, "verified")
        encoded = canonical_v100_rescore_bytes(summary)
        self.assertTrue(encoded.endswith(b"\n"))
        self.assertNotIn(b" ", encoded)

    def test_rejects_sample_aggregate_projection_and_result_identity_drift(
        self,
    ) -> None:
        authority, body = self.fixture()
        value = json.loads(body)
        cases = []
        sample = copy.deepcopy(value)
        sample["residual_samples"][0]["hits"] -= 1
        cases.append(sample)
        aggregate = copy.deepcopy(value)
        aggregate["residual_aggregate"]["average_recall100_ppm"] -= 1
        cases.append(aggregate)
        projection = copy.deepcopy(value)
        projection["projection"]["total_bytes"] -= 1
        cases.append(projection)
        for mutated in cases:
            mutated_body = (
                json.dumps(mutated, separators=(",", ":"), sort_keys=True).encode()
                + b"\n"
            )
            with self.assertRaises(ValueError):
                rescore_v100_result(
                    mutated_body,
                    expected_authority=authority,
                    expected_config=self.config(),
                    expected_result_sha256=hashlib.sha256(mutated_body).hexdigest(),
                )
        with self.assertRaisesRegex(ValueError, "result identity differs"):
            rescore_v100_result(
                body,
                expected_authority=authority,
                expected_config=self.config(),
                expected_result_sha256="0" * 64,
            )


if __name__ == "__main__":
    unittest.main()
