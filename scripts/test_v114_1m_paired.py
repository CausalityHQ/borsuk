"""Geometry-driven nomination and returned scoring for the paired 1M gate."""

import unittest
from unittest.mock import patch
import json
import tempfile
from pathlib import Path

import numpy as np

from scripts.v114_1m_paired import (
    gate_summary, main, nominate_region_pq64, prepare_paired_query, qualifies_1m,
    reduce_paired_query,
    score_sq8_ranges,
)
from scripts import v109_capped_reader_replay, v111_weighted_reader_replay


class PairedOneMillionTests(unittest.TestCase):
    def test_nonmultiple_dimension_nomination_uses_last_subspace(self) -> None:
        query = np.zeros(96, np.float32)
        query[95] = 1.0
        books = np.zeros((64, 256, 2), np.float32)
        books[63, 1, 1] = 1.0
        codes = np.zeros((2, 64), np.uint8)
        codes[1, 63] = 1
        _, _, nominees = nominate_region_pq64(
            query, np.zeros((1, 96), np.float32), books, codes,
            page_rows=2, blocks_per_page=1, regions=1, shortlist=1,
        )
        np.testing.assert_array_equal(nominees, [1])

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

    def test_prepare_votes_for_all_nominees_before_any_truth_is_available(self) -> None:
        rows, dimensions = 512, 64
        query = np.zeros(dimensions, np.float32)
        summaries = np.ones((4, dimensions), np.float32)
        summaries[2:] = 0.0
        books = np.zeros((64, 256, 1), np.float32)
        pq_codes = np.zeros((rows, 64), np.uint8)
        sq8 = np.zeros(rows, dtype=[("id", "<i8"), ("norm", "<f4"),
                                    ("code", "u1", (dimensions,))])
        sq8["id"] = np.arange(rows)
        request, reference = prepare_paired_query(
            7, query, summaries, books, pq_codes, sq8,
            np.zeros(dimensions, np.float32), np.ones(dimensions, np.float32),
            page_rows=256, blocks_per_page=2, regions=1,
            shortlist=128, primary_count=100,
        )
        self.assertNotIn("truth", request)
        self.assertEqual(request["nominees"], list(range(256, 384)))
        self.assertEqual(reference["primary"], list(range(256, 356)))
        self.assertEqual(reference["page_votes"], [[1, 100 * 513 + 28]])
        self.assertEqual(reference["ranges"], [[256 * (dimensions + 12),
                                                   rows * (dimensions + 12)]])

    def test_returned_ids_tie_by_id_after_ranges_are_fixed(self) -> None:
        dimensions = 64
        sq8 = np.zeros(256, dtype=[("id", "<i8"), ("norm", "<f4"),
                                    ("code", "u1", (dimensions,))])
        sq8["id"] = np.arange(255, -1, -1)
        actual = score_sq8_ranges(
            sq8, np.zeros(dimensions, np.float32),
            np.zeros(dimensions, np.float32), np.ones(dimensions, np.float32),
            [(0, sq8.nbytes)], top_k=100,
        )
        self.assertEqual(actual, list(range(100)))

    def test_returned_scores_match_frozen_v109_arithmetic(self) -> None:
        generator = np.random.default_rng(115)
        dimensions = 768
        sq8 = np.zeros(256, dtype=v109_capped_reader_replay.SQ8_DTYPE)
        sq8["id"] = generator.permutation(256)
        sq8["norm"] = generator.random(256, dtype=np.float32)
        sq8["code"] = generator.integers(0, 256, (256, dimensions), dtype=np.uint8)
        query = generator.standard_normal(dimensions).astype(np.float32)
        low = generator.standard_normal(dimensions).astype(np.float32)
        step = generator.random(dimensions, dtype=np.float32) / 255
        ranges = ((0, sq8.nbytes),)
        expected = v109_capped_reader_replay.score_ranges(
            sq8, query, {"low": low, "span_step": step}, ranges,
        )
        self.assertEqual(
            score_sq8_ranges(sq8, query, low, step, ranges, top_k=100), expected,
        )

    def test_parity_failure_cannot_access_ground_truth(self) -> None:
        class ForbiddenTruth:
            def __iter__(self):
                raise AssertionError("ground truth accessed before parity")

        sq8 = np.zeros(256, dtype=[("id", "<i8"), ("norm", "<f4"),
                                    ("code", "u1", (64,))])
        query = np.zeros(64, np.float32)
        reference = {"query_ordinal": 0, "generation": 1, "primary": [0],
                     "score_bits": [0], "page_votes": [[0, 513]],
                     "ranges": [[0, sq8.nbytes]], "plan_bytes": sq8.nbytes,
                     "plan_score": 513,
                     "baseline_ranges": [[0, sq8.nbytes]],
                     "baseline_gets": 1, "baseline_bytes": sq8.nbytes}
        actual = {"query_ordinal": 0, "generation": 1,
                  "ram_primary": [1], "file_primary": [1],
                  "score_bits": [0], "page_votes": [[0, 513]],
                  "ranges": [[0, sq8.nbytes]], "plan_bytes": sq8.nbytes,
                  "plan_score": 513}
        with self.assertRaisesRegex(ValueError, "ram_primary"):
            reduce_paired_query(reference, actual, sq8, query,
                                np.zeros(64, np.float32), np.ones(64, np.float32),
                                ForbiddenTruth(), top_k=100)

    def test_returned_hits_follow_verified_routes(self) -> None:
        sq8 = np.zeros(256, dtype=[("id", "<i8"), ("norm", "<f4"),
                                    ("code", "u1", (64,))])
        sq8["id"] = np.arange(256)
        query = np.zeros(64, np.float32)
        reference = {"query_ordinal": 0, "generation": 1, "primary": [0],
                     "score_bits": [0], "page_votes": [[0, 513]],
                     "ranges": [[0, sq8.nbytes]], "plan_bytes": sq8.nbytes,
                     "plan_score": 513,
                     "baseline_ranges": [[0, sq8.nbytes]],
                     "baseline_gets": 1, "baseline_bytes": sq8.nbytes}
        actual = {"query_ordinal": 0, "generation": 1,
                  "ram_primary": [0], "file_primary": [0],
                  "score_bits": [0], "page_votes": [[0, 513]],
                  "ranges": [[0, sq8.nbytes]], "plan_bytes": sq8.nbytes,
                  "plan_score": 513}
        result = reduce_paired_query(
            reference, actual, sq8, query,
            np.zeros(64, np.float32), np.ones(64, np.float32),
            np.arange(100), top_k=100,
        )
        self.assertEqual(result["baseline_hits"], 100)
        self.assertEqual(result["exact_hits"], 100)
        self.assertEqual(result["production_hits"], 100)
        self.assertEqual(result["production_returned_ids"], list(range(100)))

    def test_1m_gate_uses_aggregate_tail_and_sub90_without_dataset_tuning(self) -> None:
        self.assertTrue(qualifies_1m(
            total_hits={"baseline": 98_803, "exact": 99_050, "production": 99_050},
            p05_hits={"baseline": 95, "exact": 96, "production": 96},
            sub90_queries={"baseline": 19, "exact": 17, "production": 17},
        ))
        self.assertFalse(qualifies_1m(
            total_hits={"baseline": 98_803, "exact": 98_999, "production": 98_999},
            p05_hits={"baseline": 95, "exact": 96, "production": 96},
            sub90_queries={"baseline": 19, "exact": 17, "production": 17},
        ))
        self.assertFalse(qualifies_1m(
            total_hits={"baseline": 98_803, "exact": 99_050, "production": 99_050},
            p05_hits={"baseline": 95, "exact": 97, "production": 95},
            sub90_queries={"baseline": 19, "exact": 17, "production": 20},
        ))

    def test_prefix_gate_stops_a_wrong_baseline_before_promotion(self) -> None:
        with self.assertRaisesRegex(ValueError, "baseline"):
            gate_summary({"query_count": 200, "baseline_reproduced": False}, 200)
        gate_summary({"query_count": 200, "baseline_reproduced": True}, 200)
        with self.assertRaisesRegex(ValueError, "quality"):
            gate_summary({"query_count": 1000, "baseline_reproduced": True,
                          "qualifies_live_s3": False}, 1000)

    def test_gate_cli_needs_only_summary_and_query_count(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            summary = Path(directory) / "summary.json"
            summary.write_text(json.dumps({"query_count": 200,
                                           "baseline_reproduced": True}))
            with patch("sys.argv", ["v114_1m_paired.py", "gate", "--queries", "200",
                                    "--summary", str(summary)]):
                main()


if __name__ == "__main__":
    unittest.main()
