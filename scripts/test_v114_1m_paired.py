"""Geometry-driven nomination and returned scoring for the paired 1M gate."""

import unittest
from unittest.mock import patch

import numpy as np

from scripts.v114_1m_paired import nominate_region_pq64
from scripts import v111_weighted_reader_replay


class PairedOneMillionTests(unittest.TestCase):
    def test_generic_region_pq64_routes_only_the_selected_page(self) -> None:
        query = np.zeros(64, dtype=np.float32)
        summaries = np.ones((4, 64), dtype=np.float32)
        summaries[2:] = 0.0
        books = np.zeros((64, 256, 1), dtype=np.float32)
        codes = np.zeros((512, 64), dtype=np.uint8)
        ranked, historical, nominees = nominate_region_pq64(
            query, summaries, books, codes,
            page_rows=256, blocks_per_page=2, regions=1, shortlist=128,
        )
        self.assertEqual(ranked, [1])
        self.assertEqual(historical, [1])
        np.testing.assert_array_equal(nominees, np.arange(256, 384))

    def test_frozen_768_dim_arithmetic_matches_v111_on_short_final_page(self) -> None:
        generator = np.random.default_rng(114)
        query = generator.standard_normal(768).astype(np.float32)
        summaries = generator.standard_normal((4, 768)).astype(np.float32)
        books = generator.standard_normal((64, 256, 12)).astype(np.float32)
        codes = generator.integers(0, 256, size=(416, 64), dtype=np.uint8)
        manifest = {"summaries": summaries, "books": books, "codes": codes}
        with patch.object(v111_weighted_reader_replay, "ROWS", 416):
            expected_ranked, expected_historical, _, expected_rows = (
                v111_weighted_reader_replay.nominate_rows(
                    query, manifest, regions=2, shortlist=128,
                )
            )
        ranked, historical, nominees = nominate_region_pq64(
            query, summaries, books, codes,
            page_rows=256, blocks_per_page=2, regions=2, shortlist=128,
        )
        self.assertEqual(ranked, expected_ranked)
        self.assertEqual(historical, expected_historical)
        np.testing.assert_array_equal(nominees, expected_rows)

    def test_rejects_inconsistent_summary_geometry(self) -> None:
        with self.assertRaisesRegex(ValueError, "summary count"):
            nominate_region_pq64(
                np.zeros(64, np.float32), np.zeros((3, 64), np.float32),
                np.zeros((64, 256, 1), np.float32),
                np.zeros((512, 64), np.uint8),
                page_rows=256, blocks_per_page=2, regions=1, shortlist=128,
            )


if __name__ == "__main__":
    unittest.main()
