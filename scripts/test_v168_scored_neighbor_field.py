"""Exact PQ64 score-field geometry for nominated neighboring units."""

import unittest

import numpy as np

from scripts.v168_scored_neighbor_field import score_neighbor_field


class ScoredNeighborFieldTests(unittest.TestCase):
    def test_authenticated_old_order_may_be_int32(self):
        rows = 32
        old = np.arange(rows, dtype=np.int32)
        inverse = np.arange(rows, dtype=np.int64)
        field = score_neighbor_field(
            np.zeros(64, dtype=np.float32), nominees=(0,),
            old_order=old, inverse_old=inverse, new_order=inverse,
            inverse_new=inverse, books=np.zeros((64, 256, 1), dtype=np.float32),
            codes=np.zeros((rows, 64), dtype=np.uint8), unit_rows=32)
        self.assertEqual(field.units, (0,))

    def test_scores_all_rows_in_neighbor_units_in_new_physical_order(self):
        rows = 64
        old = np.arange(rows, dtype=np.int64)
        new = np.concatenate((np.arange(32, 64), np.arange(0, 32))).astype(np.int64)
        inverse_new = np.empty(rows, dtype=np.int64)
        inverse_new[new] = np.arange(rows, dtype=np.int64)
        query = np.zeros(64, dtype=np.float32)
        books = np.zeros((64, 256, 1), dtype=np.float32)
        codes = np.zeros((rows, 64), dtype=np.uint8)
        books[0, 1, 0] = 2
        codes[32, 0] = 1

        field = score_neighbor_field(query, nominees=(0,), old_order=old,
                                     inverse_old=old, new_order=new,
                                     inverse_new=inverse_new,
                                     books=books, codes=codes, unit_rows=32)

        self.assertEqual(field.units, (0, 1))
        np.testing.assert_array_equal(field.old_rows, new)
        self.assertEqual(field.scores.dtype, np.float32)
        self.assertEqual(field.scores.tolist(), [4.0] + [0.0] * 63)

    def test_d96_subspace_padding_matches_pq64_score(self):
        rows = 32
        order = np.arange(rows, dtype=np.int64)
        query = np.zeros(96, dtype=np.float32)
        query[2] = 3
        books = np.zeros((64, 256, 2), dtype=np.float32)
        books[1, 1, 1] = 3
        codes = np.zeros((rows, 64), dtype=np.uint8)
        codes[7, 1] = 1

        field = score_neighbor_field(query, nominees=(0,), old_order=order,
                                     inverse_old=order, new_order=order,
                                     inverse_new=order, books=books,
                                     codes=codes, unit_rows=32)

        self.assertEqual(field.units, (0,))
        self.assertEqual(field.scores[7], 0)
        self.assertTrue(np.all(field.scores[np.arange(rows) != 7] == 9))

    def test_radius_expands_scored_physical_units(self):
        rows = 160
        order = np.arange(rows, dtype=np.int64)
        field = score_neighbor_field(
            np.zeros(64, dtype=np.float32), nominees=(64,),
            old_order=order, inverse_old=order, new_order=order,
            inverse_new=order, books=np.zeros((64, 256, 1), dtype=np.float32),
            codes=np.zeros((rows, 64), dtype=np.uint8), unit_rows=32,
            radius=2)
        self.assertEqual(field.units, (0, 1, 2, 3, 4))
        np.testing.assert_array_equal(field.old_rows, order)

    def test_d768_scores_match_direct_pq_reconstruction(self):
        rng = np.random.default_rng(168)
        rows = 32
        order = np.arange(rows, dtype=np.int64)
        query = rng.normal(size=768).astype(np.float32)
        books = rng.normal(size=(64, 256, 12)).astype(np.float32)
        codes = rng.integers(0, 256, size=(rows, 64), dtype=np.uint8)

        field = score_neighbor_field(query, nominees=(0,), old_order=order,
                                     inverse_old=order, new_order=order,
                                     inverse_new=order, books=books,
                                     codes=codes, unit_rows=32)
        reconstructed = np.concatenate(
            [books[subspace, codes[7, subspace]] for subspace in range(64)])
        expected = np.sum((reconstructed.astype(np.float64)
                           - query.astype(np.float64)) ** 2)
        self.assertAlmostEqual(float(field.scores[7]), float(expected), delta=0.01)

    def test_cosine_scores_match_reconstructed_vector_direction(self):
        rows = 32
        order = np.arange(rows, dtype=np.int64)
        query = np.zeros(64, dtype=np.float32)
        query[0] = 1
        books = np.zeros((64, 256, 1), dtype=np.float32)
        books[0, 1, 0] = 1
        books[1, 1, 0] = 1
        codes = np.zeros((rows, 64), dtype=np.uint8)
        codes[:, 1] = 1
        codes[0, 0] = 1
        codes[0, 1] = 0

        field = score_neighbor_field(query, nominees=(0,),
                                     old_order=order, inverse_old=order,
                                     new_order=order, inverse_new=order,
                                     books=books, codes=codes, unit_rows=32,
                                     metric="cosine")
        self.assertEqual(field.scores.dtype, np.float32)
        self.assertEqual(field.scores[0], -1)
        self.assertTrue(np.all(field.scores[1:] == 0))
        with self.assertRaises(ValueError):
            score_neighbor_field(np.zeros_like(query), nominees=(0,),
                                 old_order=order, inverse_old=order,
                                 new_order=order, inverse_new=order,
                                 books=books, codes=codes, unit_rows=32,
                                 metric="cosine")


if __name__ == "__main__":
    unittest.main()
