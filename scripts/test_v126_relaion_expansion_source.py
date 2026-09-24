"""Focused contracts for the fixed V116 wide-replay source diagnostic."""

import copy
import unittest
from unittest.mock import patch

import numpy as np

from scripts import v126_relaion_expansion_source as study


class V126ReplayContracts(unittest.TestCase):
    def test_widened_replay_preserves_sealed_top100_and_plan(self):
        request = {"query_ordinal": 0, "nominees": [0, 1], "baseline_ranges": [[0, 16]]}
        sealed = {
            "query_ordinal": 0,
            "nominees": [0, 1],
            "score_bits": [1, 2],
            "primary": [0],
            "page_votes": [1],
            "ranges": [[0, 16]],
            "plan_bytes": 16,
            "plan_score": 2,
            "baseline_ranges": [[0, 16]],
            "baseline_bytes": 16,
            "returned_ids": [42, 7],
            "baseline_returned_ids": [7, 42],
        }
        wide = {
            **sealed,
            "returned_ids": [42, 7, 8],
            "baseline_returned_ids": [7, 42, 8],
        }
        with patch.object(study, "RETURN", 2), patch.object(study, "WIDE", 3):
            study.validate_replay_prefix(request, sealed, wide, 0)
            changed = copy.deepcopy(wide)
            changed["returned_ids"][0] = 8
            with self.assertRaisesRegex(ValueError, "prefix"):
                study.validate_replay_prefix(request, sealed, changed, 0)
            changed = copy.deepcopy(wide)
            changed["plan_bytes"] = 16_777_217
            with self.assertRaises(ValueError):
                study.validate_replay_prefix(request, sealed, changed, 0)

    def test_source_ranking_uses_original_ids_and_cosine_ties(self):
        source = np.asarray([[1.0, 0.0], [0.0, 1.0], [1.0, 0.0]], dtype=np.float32)
        with patch.object(study, "RETURN", 2):
            exact, fp16 = study.score_union(
                source,
                {900: 0, 5: 1, 100: 2},
                {5, 900, 100},
                np.asarray([1.0, 0.0], dtype=np.float64),
            )
        self.assertEqual(exact.tolist(), [100, 900])
        self.assertEqual(fp16.tolist(), exact.tolist())
        with self.assertRaisesRegex(ValueError, "source IDs"):
            study.score_union(
                source,
                {900: 0, 5: 1},
                {5, 900, 100},
                np.asarray([1.0, 0.0], dtype=np.float64),
            )


if __name__ == "__main__":
    unittest.main()
