"""Contracts for the V104 exact retained-row capacity screen."""

from __future__ import annotations

import dataclasses
import json
import unittest

from scripts.test_v98_hierarchical_row_router import V98ProducerTests
from scripts.v97_row_width_screen import PageKey, RoutedPage
from scripts.v104_exact_retention_ladder import (
    REGISTERED_RETENTION_PAGES,
    RetentionArmConfig,
    V104Config,
    canonical_v104_result_bytes,
    evaluate_v104,
)


class V104ExactRetentionLadderTests(unittest.TestCase):
    @staticmethod
    def config() -> V104Config:
        return V104Config(
            pages_per_root=8,
            maximum_root_groups=65_536,
            maximum_exposed_pages=4_096,
            arms=tuple(
                RetentionArmConfig(
                    retained_pages=pages,
                    maximum_scanned_rows=pages * 256,
                )
                for pages in REGISTERED_RETENTION_PAGES
            ),
            shortlist_rows=8_192,
            maximum_gets=32,
            maximum_bytes=16 * 1024**2,
        )

    @staticmethod
    def inputs():
        inputs = V98ProducerTests.inputs(
            dimensions=16,
            base_pages=1_024,
            delta_pages=1,
            query_count=2,
            truth_key=PageKey("base", 0),
            far_truth=False,
        )
        return dataclasses.replace(
            inputs,
            pages={
                key: RoutedPage(key, key.ordinal * 1_024, 1_024)
                for key in inputs.pages
            },
        )

    def test_registered_ladder_measures_exact_ceiling_and_selects_smallest_pass(self) -> None:
        # Break caught: the screen changes the hierarchy or row score between
        # arms, evaluates a nonregistered retention size, or selects a larger
        # passing arm instead of the smallest exact-capacity survivor.
        inputs = self.inputs()
        result = evaluate_v104(
            inputs,
            V98ProducerTests.authority(inputs),
            self.config(),
        )

        self.assertEqual(
            tuple(arm.retained_pages for arm in result.arms),
            REGISTERED_RETENTION_PAGES,
        )
        self.assertEqual(result.query_count, 2)
        self.assertEqual(result.winner_retained_pages, 128)
        self.assertEqual(result.classification, "exact-retention-survivor")
        self.assertTrue(all(arm.aggregate.quality_gate_passed for arm in result.arms))
        self.assertTrue(all(arm.aggregate.resource_gate_passed for arm in result.arms))
        self.assertLessEqual(
            result.arms[0].maximum_observed_scanned_rows,
            result.arms[0].maximum_scanned_rows,
        )

    def test_canonical_result_binds_every_arm_sample_and_decision(self) -> None:
        # Break caught: serialized evidence can change a sample, arm aggregate,
        # ladder order, or winner without independent recomputation rejecting it.
        inputs = self.inputs()
        result = evaluate_v104(
            inputs,
            V98ProducerTests.authority(inputs),
            self.config(),
        )
        body = canonical_v104_result_bytes(result)

        self.assertTrue(body.endswith(b"\n"))
        self.assertNotIn(b" ", body)
        value = json.loads(body)
        self.assertEqual(value["schema"], "borsuk-v104-exact-retention-ladder-v1")
        self.assertEqual(value["winner_retained_pages"], 128)

        first = result.arms[0]
        with self.assertRaisesRegex(ValueError, "V104 result evidence differs"):
            canonical_v104_result_bytes(
                dataclasses.replace(
                    result,
                    arms=(
                        dataclasses.replace(
                            first,
                            maximum_observed_scanned_rows=(
                                first.maximum_observed_scanned_rows + 1
                            ),
                        ),
                        *result.arms[1:],
                    ),
                )
            )

    def test_rejects_unregistered_or_underbounded_retention_arms(self) -> None:
        # Break caught: an ad hoc arm or a row cap below the page-count envelope
        # enters the scientific comparison after seeing development outcomes.
        with self.assertRaisesRegex(ValueError, "retention arm differs"):
            RetentionArmConfig(retained_pages=384, maximum_scanned_rows=98_304)
        with self.assertRaisesRegex(ValueError, "retention arm differs"):
            RetentionArmConfig(retained_pages=128, maximum_scanned_rows=32_767)


if __name__ == "__main__":
    unittest.main()
