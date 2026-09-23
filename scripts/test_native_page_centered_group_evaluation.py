"""Fixed group shortlist and paired page-centered row-score nomination."""

from __future__ import annotations

import hashlib
import tempfile
import unittest
from pathlib import Path

import numpy as np

from scripts.native_geometric_layout_screen import EvaluationLimits
from scripts.native_page_centered_group_codes import (
    construct_group_codes,
    write_group_codes,
)
from scripts.native_page_centered_group_evaluation import (
    centered_scores,
    evaluate_group_query,
    plan_groups,
)
from scripts.test_native_page_centered_group_codes import GroupCodeTests


class CountingReader:
    def __init__(self, body: bytes):
        self.body = body
        self.gets = 0
        self.bytes = 0

    def __call__(self, offset: int, length: int) -> bytes:
        payload = self.body[offset : offset + length]
        self.gets += 1
        self.bytes += len(payload)
        return payload


class GroupEvaluationTests(unittest.TestCase):
    def test_first_32_distinct_groups_are_selected_in_tree_order(self) -> None:
        manifest = tuple(
            (4 * group, 4 * group + 4, group * 100, 100, hashlib.sha256(bytes([group])).hexdigest())
            for group in range(33)
        )
        retained = (4, 0, 4, *(4 * group for group in range(2, 33)))
        with self.assertRaisesRegex(ValueError, "retained"):
            plan_groups(retained, manifest)
        unique = retained[:2] + retained[3:]
        plan = plan_groups(unique, manifest)
        self.assertEqual(plan.group_ordinals, (1, 0, *range(2, 32)))
        self.assertEqual(plan.gets, 32)
        self.assertEqual(len(plan.pages), 128)
        self.assertNotIn(128, plan.pages)
        with self.assertRaisesRegex(ValueError, "budget"):
            plan_groups(unique, manifest, maximum_bytes=3199)

    def test_page_centered_scores_match_direct_reconstruction(self) -> None:
        rng = np.random.default_rng(19)
        books = rng.normal(size=(48, 256, 1)).astype(np.float32)
        means = rng.normal(size=(2, 48)).astype(np.float32)
        codes = rng.integers(0, 256, size=(3, 48), dtype=np.uint8)
        query = rng.normal(size=48).astype(np.float32)
        pages = (0, 1, 0)
        actual = centered_scores(query, books, means, codes, pages)
        expected = []
        for row, page in zip(codes, pages, strict=True):
            decoded = np.asarray(
                [books[i, int(row[i]), 0] for i in range(48)], dtype=np.float64
            )
            vector = means[page].astype(np.float64) + decoded
            expected.append(np.float32(np.sum((query.astype(np.float64) - vector) ** 2)))
        np.testing.assert_allclose(actual, expected, rtol=0, atol=1e-5)

    def test_actual_group_reads_are_authenticated_and_paired(self) -> None:
        ids, vectors, membership, source_sha = GroupCodeTests.fixture()
        artifacts = construct_group_codes(ids, vectors, membership, seed=7, iterations=1)
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            write_group_codes(
                root,
                artifacts,
                source_sha,
                hashlib.sha256(b"membership").digest(),
                hashlib.sha256(b"tree").digest(),
            )
            body = (root / "groups.bin").read_bytes()
        reader = CountingReader(body)
        truth = tuple(ids[index] for index in range(100))
        sample = evaluate_group_query(
            query_ordinal=0,
            query=vectors[0],
            retained_pages=(4, 0),
            artifacts=artifacts,
            stable_ids=ids,
            vectors=vectors,
            truth_ids=truth,
            page_byte_sizes=(1000,) * 5,
            limits=EvaluationLimits(2, 2000),
            read_group_range=reader,
            prior_pq_hits_at_100=42,
            prior_residual_hits_at_100=43,
        )
        self.assertEqual(sample.code_gets, 2)
        self.assertEqual(reader.gets, 2)
        self.assertEqual(sample.code_bytes, reader.bytes)
        self.assertEqual(sample.grouped_pages, (4, 0, 1, 2, 3))
        self.assertEqual(sample.prior_residual_hits_at_100, 43)
        self.assertEqual(sample.exact_data_bytes, 2000)
        self.assertEqual(sample.coded_data_bytes, 2000)
        with self.assertRaisesRegex(ValueError, "identity"):
            evaluate_group_query(
                query_ordinal=0,
                query=vectors[0],
                retained_pages=(4, 0),
                artifacts=artifacts,
                stable_ids=ids,
                vectors=vectors,
                truth_ids=truth,
                page_byte_sizes=(1000,) * 5,
                limits=EvaluationLimits(2, 2000),
                read_group_range=CountingReader(body[:-1]),
                prior_pq_hits_at_100=42,
                prior_residual_hits_at_100=43,
            )
        damaged = bytearray(body)
        damaged[8] ^= 1
        with self.assertRaisesRegex(ValueError, "identity"):
            evaluate_group_query(
                query_ordinal=0,
                query=vectors[0],
                retained_pages=(4, 0),
                artifacts=artifacts,
                stable_ids=ids,
                vectors=vectors,
                truth_ids=truth,
                page_byte_sizes=(1000,) * 5,
                limits=EvaluationLimits(2, 2000),
                read_group_range=CountingReader(bytes(damaged)),
                prior_pq_hits_at_100=42,
                prior_residual_hits_at_100=43,
            )


if __name__ == "__main__":
    unittest.main()
