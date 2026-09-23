"""PQ returned ranking is distinct from shortlist containment."""

from __future__ import annotations

import hashlib
import tempfile
import unittest
from pathlib import Path

import numpy as np

from scripts import native_pq192_returned as pq
from scripts.validate_native_pq192_returned import _verify_assignments


class PqReturnedTests(unittest.TestCase):
    def test_corpus_only_codes_and_asymmetric_scores(self) -> None:
        vectors = np.asarray([
            [0, 0, 1, 1], [1, 1, 0, 0], [3, 3, 3, 3], [2, 2, 2, 2],
            [0, 1, 1, 0], [1, 0, 0, 1], [3, 2, 2, 3], [2, 3, 3, 2],
        ], dtype=np.float32)
        order = tuple(reversed(range(len(vectors))))
        books, codes = pq.train_pq(
            vectors, order, subspaces=2, clusters=4, iterations=2, seed=11,
        )
        self.assertEqual(books.shape, (2, 4, 2))
        self.assertEqual(codes.shape, (8, 2))
        self.assertTrue(np.array_equal(
            codes, pq.assign_codes(vectors[list(order)], books),
        ))
        query = np.asarray([0, 0, 0, 0], dtype=np.float32)
        score = pq.score_pq(query, books, codes, (0, 1, 2))
        recon = books[np.arange(2)[None, :], codes[:3]].reshape(3, 4)
        expected = np.sum((recon - query) ** 2, axis=1, dtype=np.float64).astype(np.float32)
        self.assertTrue(np.array_equal(score, expected))

    def test_wider_record_fits_closed_maximum_roster(self) -> None:
        self.assertLess(77_113 * 208 + 1024, 16_777_216)
        self.assertEqual(pq.planned_bytes(15_423_240, 77_113),
                         15_423_240 + 8 * 77_113)
        self.assertEqual(pq.physical_order_sha256((0, 1)),
                         hashlib.sha256(b"\x00\x00\x00\x00\x01\x00\x00\x00").hexdigest())

    def test_output_files_are_create_only(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            out = Path(temporary)
            pq.write_array(out / "pq-books.npy", np.zeros((2, 4, 2), np.float32))
            with self.assertRaises(FileExistsError):
                pq.write_array(out / "pq-books.npy", np.zeros((2, 4, 2), np.float32))

    def test_independent_assignment_check_rejects_wrong_physical_row(self) -> None:
        vectors = np.zeros((2, 768), dtype=np.float32)
        books = np.zeros((192, 256, 4), dtype=np.float32)
        books[:, 1, :] = 1.0
        codes = np.zeros((2, 192), dtype=np.uint8)
        _verify_assignments(vectors, (1, 0), books, codes)
        codes[1, 0] = 1
        with self.assertRaisesRegex(ValueError, "assignment"):
            _verify_assignments(vectors, (1, 0), books, codes)
