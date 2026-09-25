"""Small protocol check for the closed-fit hard-plan diagnostic."""

import unittest
from pathlib import Path
from unittest.mock import patch

from scripts import v192_optional_rank_plan_fit as gate


class OptionalPlanFitTests(unittest.TestCase):
    def test_selected_plans_count_full_gt100_outside_candidate(self):
        features = [
            {"ordinal": 2432 + index, "source_id": index,
             "ranked_units": [0, 2], "mandatory_units": [0]}
            for index in range(128)
        ]
        labels = [
            {"ordinal": 2432 + index, "source_id": index,
             "truth_by_unit": [[0, 98], [2, 1], [3, 1]]}
            for index in range(128)
        ]
        with (patch.object(gate, "_read", side_effect=[features, labels]),
              patch.object(gate, "PRICE_GRID", ((0, 0),))):
            report = gate.run(Path("features"), Path("labels"))
        self.assertEqual(report["status"], "complete")
        self.assertEqual(report["selected_prices"]["optional_risk"], [0, 0])
        for split in ("price_fit", "diagnostic"):
            for arm in report["splits"][split].values():
                summary = arm["summary"]
                self.assertEqual(summary["candidate_ceiling_hits"], 32 * 99)
                self.assertEqual(summary["hits"], 32 * 99)
                self.assertEqual(summary["p05_nearest_rank_hits"], 99)
                self.assertLessEqual(summary["max_units"], 672)
                self.assertLessEqual(summary["max_gets"], 32)


if __name__ == "__main__":
    unittest.main()
