"""Paired exact-control and PQ48 page-nomination evaluation."""

from __future__ import annotations

import hashlib
import unittest

import numpy as np

from scripts.native_geometric_layout_screen import (
    EvaluationLimits,
    LayoutMethod,
    MembershipRow,
)
from scripts.native_row_score_code_artifacts import construct_code_artifacts
from scripts.native_row_score_evaluation import (
    RowScoreSample,
    aggregate_samples,
    evaluate_row_score_query,
)


class RowScoreEvaluationTests(unittest.TestCase):
    def test_aggregate_fails_tail_gate_even_when_mean_passes(self) -> None:
        def sample(ordinal: int, hits: int) -> RowScoreSample:
            return RowScoreSample(
                query_ordinal=ordinal, retained_pages=(0,),
                code_blocks=((0, 1, 0, 48),), code_gets=1, code_bytes=48,
                retained_hits_at_10=10, retained_hits_at_100=100,
                restricted_oracle_pages=(0,), restricted_oracle_hits_at_10=10,
                restricted_oracle_hits_at_100=100,
                pq_pages=(0,), pq_data_bytes=100,
                pq_hits_at_10=10, pq_hits_at_100=hits,
                exact_pages=(0,), exact_data_bytes=100,
                exact_hits_at_10=10, exact_hits_at_100=100,
            )

        result = aggregate_samples(tuple(sample(i, 80 if i == 0 else 100) for i in range(20)))
        self.assertEqual(result["pq_mean_recall_at_100_ppm"], 990_000)
        self.assertEqual(result["pq_p05_recall_at_100_ppm"], 800_000)
        self.assertEqual(result["decision"], "killed")

    def test_same_retained_pages_feed_exact_and_pq48_arms(self) -> None:
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
            )
            for index in range(256)
        )
        artifacts = construct_code_artifacts(ids, vectors, membership, seed=7, iterations=1)
        truth = tuple(ids[index] for index in range(128, 228))
        fetched = []
        plane = artifacts.codes.tobytes(order="C")

        def read_range(offset: int, length: int) -> bytes:
            fetched.append((offset, length))
            return plane[offset : offset + length]

        sample = evaluate_row_score_query(
            query_ordinal=0, query=vectors[180], retained_pages=(0, 1),
            artifacts=artifacts, stable_ids=ids, vectors=vectors,
            truth_ids=truth, page_byte_sizes=(1000, 1000),
            limits=EvaluationLimits(maximum_pages=1, maximum_bytes=1000),
            read_code_range=read_range,
        )
        self.assertEqual(fetched, [(0, 12_288)])
        self.assertEqual((sample.code_gets, sample.code_bytes), (1, 12_288))
        self.assertEqual(sample.pq_pages, (1,))
        self.assertEqual(sample.exact_pages, (1,))
        self.assertEqual(sample.pq_hits_at_10, 10)
        self.assertEqual(sample.pq_hits_at_100, 100)
        self.assertEqual(sample.exact_hits_at_100, 100)
        self.assertEqual(sample.retained_hits_at_100, 100)
        self.assertEqual(sample.restricted_oracle_pages, (1,))
        self.assertEqual(sample.restricted_oracle_hits_at_100, 100)

    def test_corrupt_code_range_cannot_feed_nomination(self) -> None:
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
        artifacts = construct_code_artifacts(ids, vectors, membership, seed=7, iterations=1)
        with self.assertRaisesRegex(ValueError, "code range identity"):
            evaluate_row_score_query(
                query_ordinal=0, query=vectors[180], retained_pages=(0, 1),
                artifacts=artifacts, stable_ids=ids, vectors=vectors,
                truth_ids=tuple(ids[index] for index in range(128, 228)),
                page_byte_sizes=(1000, 1000),
                limits=EvaluationLimits(maximum_pages=1, maximum_bytes=1000),
                read_code_range=lambda offset, length: bytes(length),
            )


if __name__ == "__main__":
    unittest.main()
