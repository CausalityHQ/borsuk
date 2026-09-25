"""Fixed gate and resource accounting for the untouched V190 panel."""

import unittest

from scripts.v190_frozen_rank_replication import decide


class V190FrozenRankReplicationTests(unittest.TestCase):
    def test_all_preregistered_gates_are_required(self):
        good = {"hits": 25490, "p05": 98, "infeasible": 0,
                "bytes": 2_850_298_880, "gets": 5664}
        self.assertEqual(decide(good), "advance-to-paired-used-validation")
        for changed in ({"hits": 25489}, {"p05": 97},
                        {"infeasible": 1}, {"gets": 5665},
                        {"bytes": 2_850_323_840}):
            self.assertEqual(decide(good | changed),
                             "reject-frozen-rank-price")


if __name__ == "__main__":
    unittest.main()
