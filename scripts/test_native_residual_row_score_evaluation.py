"""Residual lookup scores and paired bounded page selection."""

from __future__ import annotations

import hashlib
import unittest

import numpy as np

from scripts.native_geometric_layout_screen import (
    EvaluationLimits,
    LayoutMethod,
    MembershipRow,
)
from scripts.native_residual_row_score_codes import construct_residual_codes
from scripts.native_residual_row_score_evaluation import (
    ResidualSample,
    aggregate_residual_samples,
    evaluate_residual_query,
    residual_scores,
)
from scripts.native_row_score_evaluation import RowScoreSample
from scripts.native_row_score_nomination import nominate_pages


class ResidualScoreTests(unittest.TestCase):
    def test_aggregate_rejects_low_tail_despite_high_mean(self) -> None:
        def sample(ordinal: int, hits: int) -> ResidualSample:
            baseline = RowScoreSample(
                query_ordinal=ordinal, retained_pages=(0,),
                code_blocks=((0, 1, 0, 48),), code_gets=1, code_bytes=48,
                retained_hits_at_10=10, retained_hits_at_100=100,
                restricted_oracle_pages=(0,), restricted_oracle_hits_at_10=10,
                restricted_oracle_hits_at_100=100,
                pq_pages=(0,), pq_data_bytes=100,
                pq_hits_at_10=10, pq_hits_at_100=100,
                exact_pages=(0,), exact_data_bytes=100,
                exact_hits_at_10=10, exact_hits_at_100=100,
            )
            return ResidualSample(
                query_ordinal=ordinal, baseline=baseline,
                code_blocks=((0, 1, 0, 72),), code_gets=1, code_bytes=72,
                residual_pages=(0,), residual_data_bytes=100,
                residual_hits_at_10=10, residual_hits_at_100=hits,
            )

        result = aggregate_residual_samples(
            tuple(sample(index, 80 if index == 0 else 100) for index in range(20))
        )
        self.assertEqual(result["residual_mean_recall_at_100_ppm"], 990_000)
        self.assertEqual(result["residual_p05_recall_at_100_ppm"], 800_000)
        self.assertEqual(result["decision"], "killed")

    def test_cross_stage_term_matches_direct_reconstruction(self) -> None:
        first_books = np.zeros((48, 256, 1), dtype=np.float32)
        residual_books = np.zeros((24, 256, 2), dtype=np.float32)
        first_books[0, 1, 0] = 2.0
        residual_books[0, 1, 0] = 3.0
        query = np.zeros(48, dtype=np.float32)
        query[0] = 4.0
        codes = np.zeros((2, 72), dtype=np.uint8)
        codes[0, 0] = 1
        codes[0, 48] = 1
        scores = residual_scores(query, first_books, residual_books, codes)
        self.assertEqual(scores.dtype, np.float32)
        np.testing.assert_allclose(scores, [1.0, 16.0], rtol=0, atol=1e-10)

    def test_lookup_matches_direct_random_reconstruction(self) -> None:
        rng = np.random.default_rng(112)
        first_books = rng.normal(size=(48, 256, 2)).astype(np.float32)
        residual_books = rng.normal(size=(24, 256, 4)).astype(np.float32)
        query = rng.normal(size=96).astype(np.float32)
        codes = rng.integers(0, 256, size=(9, 72), dtype=np.uint8)
        expected = []
        for row in codes:
            reconstruction = np.concatenate(
                [first_books[index, row[index]] for index in range(48)]
            ).astype(np.float64)
            residual = np.concatenate(
                [residual_books[index, row[48 + index]] for index in range(24)]
            ).astype(np.float64)
            delta = query.astype(np.float64) - reconstruction - residual
            expected.append(float(delta @ delta))
        actual = residual_scores(query, first_books, residual_books, codes)
        np.testing.assert_array_equal(actual, np.asarray(expected, dtype=np.float32))

    def test_cancellation_tie_uses_source_ordinal_for_page_nomination(self) -> None:
        first_books = np.zeros((48, 256, 1), dtype=np.float32)
        residual_books = np.zeros((24, 256, 2), dtype=np.float32)
        first_books[0, 1, 0] = -0.8785834312438965
        residual_books[0, 1, 0] = 1.2411892414093018
        first_books[0, 2, 0] = 0.3626058101654053
        query = np.zeros(48, dtype=np.float32)
        query[0] = 0.028249051421880722
        codes = np.zeros((2, 72), dtype=np.uint8)
        codes[0, 0] = 1
        codes[0, 48] = 1
        codes[1, 0] = 2
        scores = residual_scores(query, first_books, residual_books, codes)
        self.assertEqual(scores[0], scores[1])
        self.assertEqual(
            nominate_pages(
                scores, (0, 1), (0, 1), (100, 100),
                EvaluationLimits(maximum_pages=1, maximum_bytes=100), top_rows=1,
            ),
            (0,),
        )

    def test_sealed_72_byte_ranges_feed_same_paired_pages(self) -> None:
        ids = tuple(index.to_bytes(4, "little") for index in range(256))
        vectors = np.repeat(np.arange(256, dtype=np.float32)[:, None], 48, axis=1)
        source_sha = hashlib.sha256(b"source").digest()
        membership = tuple(
            MembershipRow(
                stable_id=ids[index], source_ordinal=index,
                page_ordinal=index // 128, in_page_ordinal=index % 128,
                page_rows=128, encoded_page_bytes=1000,
                method=LayoutMethod.TWO_MEANS_480K,
                source_sha256=source_sha, seed=7,
                construction_sha256=hashlib.sha256(b"layout").digest(),
            ) for index in range(256)
        )
        artifacts = construct_residual_codes(ids, vectors, membership, seed=7, iterations=1)
        plane = artifacts.codes.tobytes(order="C")
        calls = []

        def read_range(offset: int, length: int) -> bytes:
            calls.append((offset, length))
            return plane[offset : offset + length]

        inputs = dict(
            query_ordinal=0, query=vectors[180], retained_pages=(0, 1),
            artifacts=artifacts, stable_ids=ids, vectors=vectors,
            truth_ids=tuple(ids[index] for index in range(128, 228)),
            page_byte_sizes=(1000, 1000),
            limits=EvaluationLimits(maximum_pages=1, maximum_bytes=1000),
        )
        sample = evaluate_residual_query(**inputs, read_code_range=read_range)
        self.assertEqual(calls, [(0, 18_432)])
        self.assertEqual(sample.code_bytes, 18_432)
        self.assertEqual(sample.baseline.pq_pages, (1,))
        self.assertEqual(sample.residual_pages, (1,))
        self.assertEqual(sample.residual_hits_at_100, 100)
        with self.assertRaisesRegex(ValueError, "code range identity"):
            evaluate_residual_query(
                **inputs, read_code_range=lambda offset, length: bytes(length)
            )

        class MismatchedCounters:
            gets = 0
            bytes = 0

            def __call__(self, offset: int, length: int) -> bytes:
                self.gets += 2
                self.bytes += length
                return plane[offset : offset + length]

        with self.assertRaisesRegex(ValueError, "observed code wave"):
            evaluate_residual_query(**inputs, read_code_range=MismatchedCounters())


if __name__ == "__main__":
    unittest.main()
