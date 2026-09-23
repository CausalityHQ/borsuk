from __future__ import annotations

import unittest
from unittest.mock import patch

import numpy as np

from scripts.v111_weighted_reader_replay import nominate_rows
from scripts.v109_capped_reader_replay import SQ8_DTYPE, score_ranges
from scripts.v112_precise_nominee_route import precise_nominee_scores, precise_page_weights


class PreciseNomineeRouteTests(unittest.TestCase):
    def test_top100_sq8_votes_dominate_all_secondary_votes(self) -> None:
        rows = np.arange(512, dtype=np.int32)
        ids = rows.astype(np.int64)
        scores = (511 - rows).astype(np.float32)
        weights = precise_page_weights(rows, ids, scores)
        self.assertEqual(weights, {0: 256, 1: 100 * 513 + 156})

    def test_v77_nominated_rows_remain_exactly_top512(self) -> None:
        manifest = {
            "summaries": np.zeros((4, 768), dtype=np.float32),
            "books": np.zeros((64, 256, 12), dtype=np.float32),
            "codes": np.zeros((512, 64), dtype=np.uint8),
        }
        with patch("scripts.v111_weighted_reader_replay.ROWS", 512):
            ranked, historical, counts, rows = nominate_rows(
                np.zeros(768, dtype=np.float32), manifest,
                regions=2, shortlist=512,
            )
        self.assertEqual(ranked, [0, 1])
        self.assertEqual(historical, [0, 1])
        self.assertEqual(counts, {0: 256, 1: 256})
        self.assertEqual(rows.tolist(), list(range(512)))

    def test_nominee_score_uses_final_sq8_ranking(self) -> None:
        records = np.zeros(512, dtype=SQ8_DTYPE)
        records["id"] = np.arange(512, dtype=np.int64)
        records["norm"] = np.arange(511, -1, -1, dtype=np.float32)
        query = np.zeros(768, dtype=np.float32)
        manifest = {"low": query.copy(), "span_step": np.ones(768, dtype=np.float32)}
        ids, scores = precise_nominee_scores(
            records, np.arange(512, dtype=np.int32), query, manifest,
        )
        ranked = ids[np.lexsort((ids, scores))[:100]].tolist()
        self.assertEqual(ranked, score_ranges(
            records, query, manifest, ((0, 512 * SQ8_DTYPE.itemsize),),
        ))


if __name__ == "__main__":
    unittest.main()
