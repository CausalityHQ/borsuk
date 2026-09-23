"""Pure finite-cohort accounting for V115 returned replay."""

import unittest

import numpy as np

from scripts.validate_v115_returned_replay import compare_replay


class ReturnedReplayTests(unittest.TestCase):
    def test_counts_returned_id_and_hit_changes_separately(self) -> None:
        requests = [{"query_ordinal": index, "nominees": [1, 2]}
                    for index in range(1000)]
        rosters = [{"query_ordinal": index, "nominees": [2, 1]}
                   for index in range(1000)]
        reference = [{"query_ordinal": index, "score_bits": [11, 22],
                      "primary": [1], "page_votes": [[0, 3]],
                      "ranges": [[0, 10]], "plan_bytes": 10, "plan_score": 3}
                     for index in range(1000)]
        evidence = [{"query_ordinal": index, "production_returned_ids": [7],
                     "production_hits": 1, "baseline_hits": 0}
                    for index in range(1000)]
        actual = [{"query_ordinal": index, "nominees": [2, 1],
                   "score_bits": [22, 11], "primary": [1],
                   "page_votes": [[0, 3]], "ranges": [[0, 10]],
                   "plan_bytes": 10, "plan_score": 3,
                   "returned_ids": [7]}
                  for index in range(1000)]
        truth = np.zeros((1000, 100), np.int64)
        truth[:, 0] = 7
        result = compare_replay(requests, rosters, reference,
                                evidence, actual, truth)
        self.assertEqual(result["mismatch_queries"]["returned_ids"], 0)
        self.assertEqual(result["rust_returned_hits"], 1000)
        actual[4]["returned_ids"] = [8]
        result = compare_replay(requests, rosters, reference,
                                evidence, actual, truth)
        self.assertEqual(result["mismatch_queries"]["returned_ids"], 1)
        self.assertEqual(result["mismatch_queries"]["hits"], 1)
        self.assertEqual(result["rust_returned_hits"], 999)


if __name__ == "__main__":
    unittest.main()
