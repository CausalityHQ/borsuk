"""Tests for the V102 bounded object-storage PQ48 refinement spike."""

import dataclasses
import json
import unittest

from scripts import test_v98_hierarchical_row_router as v98_tests
from scripts.v97_row_width_screen import PageKey, RoutedPage
from scripts.v99_ranked_gap_range_router import RankedGapConfig
from scripts.v102_two_wave_pq48_refinement import (
    PQ48X8,
    build_refinement_page_directory,
    canonical_v102_result_bytes,
    evaluate_v102,
    plan_refinement_fetch,
    project_v102_resident_bytes_100m,
)


class V102TwoWavePq48RefinementTests(unittest.TestCase):
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

    def test_refinement_directory_and_fetch_cover_every_required_page(self) -> None:
        # Break caught: the I/O model prices only requested code pages while
        # silently dropping retained pages or omitting merged gap bytes.
        inputs = v98_tests.V98ProducerTests.inputs(
            dimensions=16,
            base_pages=8,
            delta_pages=2,
            query_count=1,
            truth_key=PageKey("base", 0),
            far_truth=False,
        )
        directory = build_refinement_page_directory(inputs, row_bytes=48)
        required = (
            PageKey("base", 0),
            PageKey("base", 2),
            PageKey("base", 5),
            PageKey("delta", 1),
        )

        selection = plan_refinement_fetch(
            required,
            directory,
            max_gets=2,
            max_bytes=10_000_000,
        )

        self.assertTrue(set(required).issubset(selection.pages))
        self.assertEqual(selection.gets, 2)
        self.assertEqual(
            selection.bytes,
            sum(directory[key].encoded_bytes for key in selection.pages),
        )
        with self.assertRaisesRegex(ValueError, "refinement pages do not fit"):
            plan_refinement_fetch(
                required,
                directory,
                max_gets=2,
                max_bytes=directory[required[0]].encoded_bytes,
            )

    def test_projection_keeps_codes_on_s3_and_resident_state_under_three_gib(self) -> None:
        # Break caught: the worksheet calls PQ48 scalable by forgetting either
        # its 4.8-GB S3 code plane or by accidentally charging it to RAM.
        projection = project_v102_resident_bytes_100m(self.config())

        self.assertEqual(PQ48X8.row_bytes, 48)
        self.assertEqual(projection.refinement_code_plane_bytes, 4_800_000_000)
        self.assertEqual(projection.row_codes_resident_bytes, 0)
        self.assertLess(projection.total_resident_bytes, 3 * 1024**3)
        self.assertTrue(projection.resident_eligible)

    def test_evaluation_emits_two_wave_resources_and_canonical_evidence(self) -> None:
        # Break caught: the challenger inherits the final-page budget but does
        # not account/authenticate the first refinement-code fetch wave.
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

        result = evaluate_v102(
            inputs,
            v98_tests.V98ProducerTests.authority(inputs),
            self.config(),
        )

        self.assertEqual(result.query_count, 2)
        self.assertEqual(len(result.refinement_fetches), 2)
        self.assertTrue(all(fetch.gets <= 32 for fetch in result.refinement_fetches))
        self.assertTrue(
            all(fetch.bytes <= 16 * 1024**2 for fetch in result.refinement_fetches)
        )
        self.assertTrue(result.challenger_aggregate.resource_gate_passed)
        self.assertTrue(result.projection.resident_eligible)
        body = canonical_v102_result_bytes(result)
        self.assertTrue(body.endswith(b"\n"))
        self.assertNotIn(b" ", body)
        decoded = json.loads(body)
        self.assertEqual(decoded["schema"], "borsuk-v102-two-wave-pq48-v1")
        self.assertEqual(decoded["refinement_row_bytes"], 48)


if __name__ == "__main__":
    unittest.main()
