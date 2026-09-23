"""Independent recount accepts the 100k single-base, two-arm screen."""

from __future__ import annotations

import unittest

from scripts.recount_native_one_million_data_range import recount_range_masks


class BaseOnlyRecountTests(unittest.TestCase):
    def test_default_two_role_names_remain_stable(self) -> None:
        plan = {
            "priority_pages": [["base", 0]],
            "target_pages": [["base", 0]],
            "ranges": [["base", 0, 1]],
            "included_pages": [["base", 0]],
            "gets": 1,
            "encoded_bytes": 10,
        }
        _, metrics = recount_range_masks(
            [{"query_ordinal": 0, "candidate": plan, "control": plan}],
            [[0]], {"base": (10,), "delta": (8,)},
        )
        self.assertEqual(metrics["paired_gt100"], {
            "candidate_better": 0, "control_better": 0, "tied": 1,
        })

    def test_recounts_sign96_and_source_masks(self) -> None:
        plan = {
            "priority_pages": [["base", 0], ["base", 2]],
            "target_pages": [["base", 0], ["base", 2]],
            "ranges": [["base", 0, 3]],
            "included_pages": [["base", 0], ["base", 1], ["base", 2]],
            "gets": 1,
            "encoded_bytes": 22,
        }
        samples = [{"query_ordinal": 0, "sign96": plan, "source": plan}]
        evidence, metrics = recount_range_masks(
            samples, [[0, 1, 2]], {"base": (10, 2, 10)},
            arms=("sign96", "source"),
        )
        self.assertEqual(evidence[0]["sign96"]["hit_mask"], "111")
        self.assertEqual(metrics["sign96"]["gt100_hits"], 3)
        self.assertEqual(metrics["sign96"]["bridge_gt100_hits"], 1)
        self.assertEqual(metrics["paired_gt100"]["tied"], 1)


if __name__ == "__main__":
    unittest.main()
