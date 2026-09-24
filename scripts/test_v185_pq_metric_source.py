"""Paired source metric gate decisions."""

import unittest

from scripts.v185_pq_metric_source import decide


class PqMetricSourceTests(unittest.TestCase):
    def test_cosine_requires_quality_tail_and_paired_nonregression(self):
        totals = {"cosine": {"446": 12746},
                  "squared_l2": {"446": 12745}}
        p05 = {"cosine": {"446": 98},
               "squared_l2": {"446": 98}}
        self.assertEqual(decide(totals, p05),
                         "advance-cosine-to-physical-gate")
        totals["cosine"]["446"] = 12744
        self.assertEqual(decide(totals, p05), "retain-l2")
        totals["squared_l2"]["446"] = 12743
        self.assertEqual(decide(totals, p05),
                         "revise-utility-or-representation")


if __name__ == "__main__":
    unittest.main()
