"""Returned-neighbor counts on a fixed fetched row roster."""

from __future__ import annotations

import unittest

from scripts import native_two_bit_returned_recall as returned


class ReturnedRecallTests(unittest.TestCase):
    def test_rank_boundary_swap_changes_returned_recall(self) -> None:
        ids = (b"a", b"b", b"c")
        truth = (b"a", b"b")
        sources = (0, 1, 2)
        self.assertEqual(
            returned.ranked_hits((0.1, 0.2, 0.3), sources, ids, truth, top_k=2), 2
        )
        self.assertEqual(
            returned.ranked_hits((0.1, 0.3, 0.2), sources, ids, truth, top_k=2), 1
        )

    def test_float32_score_ties_use_source_ordinal(self) -> None:
        ids = (b"a", b"b", b"c")
        self.assertEqual(
            returned.ranked_hits((1.0, 1.0, 1.0), (2, 0, 1), ids,
                                 (b"a", b"b"), top_k=2), 2
        )

    def test_rejects_repeated_candidate_source(self) -> None:
        with self.assertRaisesRegex(ValueError, "candidate"):
            returned.ranked_hits((0.1, 0.2), (0, 0), (b"a", b"b"),
                                 (b"a",), top_k=1)
