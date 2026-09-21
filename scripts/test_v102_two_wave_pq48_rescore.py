"""Independent reducer tests for the V102 two-wave PQ48 spike."""

import copy
import dataclasses
import hashlib
import json
import unittest

from scripts import test_v98_hierarchical_row_router as v98_tests
from scripts.v97_row_width_screen import PageKey, RoutedPage
from scripts.v99_ranked_gap_range_router import RankedGapConfig
from scripts.v102_two_wave_pq48_refinement import (
    canonical_v102_result_bytes,
    evaluate_v102,
)
from scripts.v102_two_wave_pq48_rescore import (
    canonical_v102_rescore_bytes,
    rescore_v102_result,
)


class V102TwoWavePq48RescoreTests(unittest.TestCase):
    @staticmethod
    def config() -> RankedGapConfig:
        return RankedGapConfig(8, 65_536, 4_096, 1_024, 262_144, 8_192, 32, 16 * 1024**2)

    def fixture(self):
        inputs = v98_tests.V98ProducerTests.inputs(
            dimensions=48,
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
        body = canonical_v102_result_bytes(
            evaluate_v102(inputs, authority, self.config())
        )
        return inputs, authority, body

    def test_recomputes_two_waves_samples_intervals_projection_and_decision(self) -> None:
        inputs, authority, body = self.fixture()

        summary = rescore_v102_result(
            body,
            expected_inputs=inputs,
            expected_authority=authority,
            expected_config=self.config(),
            expected_result_sha256=hashlib.sha256(body).hexdigest(),
        )

        self.assertEqual(summary.query_count, 2)
        self.assertEqual(summary.status, "verified")
        encoded = canonical_v102_rescore_bytes(summary)
        self.assertTrue(encoded.endswith(b"\n"))
        self.assertNotIn(b" ", encoded)

    def test_rejects_refinement_io_sample_projection_and_identity_drift(self) -> None:
        inputs, authority, body = self.fixture()
        value = json.loads(body)
        cases = []
        fetch = copy.deepcopy(value)
        fetch["refinement_fetches"][0]["bytes"] -= 1
        cases.append(fetch)
        sample = copy.deepcopy(value)
        sample["challenger_samples"][0]["hits"] -= 1
        cases.append(sample)
        projection = copy.deepcopy(value)
        projection["projection"]["refinement_code_plane_bytes"] -= 1
        cases.append(projection)
        for mutated in cases:
            mutated_body = json.dumps(
                mutated, separators=(",", ":"), sort_keys=True
            ).encode() + b"\n"
            with self.assertRaises(ValueError):
                rescore_v102_result(
                    mutated_body,
                    expected_inputs=inputs,
                    expected_authority=authority,
                    expected_config=self.config(),
                    expected_result_sha256=hashlib.sha256(mutated_body).hexdigest(),
                )
        with self.assertRaisesRegex(ValueError, "result identity differs"):
            rescore_v102_result(
                body,
                expected_inputs=inputs,
                expected_authority=authority,
                expected_config=self.config(),
                expected_result_sha256="0" * 64,
            )


if __name__ == "__main__":
    unittest.main()
