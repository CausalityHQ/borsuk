"""Independent replay catches changed row-score page choices."""

from __future__ import annotations

import dataclasses
import hashlib
import unittest

import numpy as np

from scripts.native_geometric_layout_screen import (
    EvaluationLimits,
    LayoutMethod,
    MembershipRow,
)
from scripts.native_row_score_cell import evaluate_routes
from scripts.native_row_score_code_artifacts import construct_code_artifacts
from scripts.validate_native_row_score_result import validate_samples


class IndependentRowScoreReplayTests(unittest.TestCase):
    def test_changed_page_choice_fails_independent_replay(self) -> None:
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
        plane = artifacts.codes.tobytes(order="C")
        queries = vectors[180:181]
        truth = (tuple(ids[index] for index in range(128, 228)),)
        retained = ((0, 1),)
        limits = EvaluationLimits(1, 1000)
        samples, metrics = evaluate_routes(
            queries=queries, truth=truth, retained_by_query=retained,
            artifacts=artifacts, stable_ids=ids, vectors=vectors,
            page_byte_sizes=(1000, 1000), limits=limits,
            read_code_range=lambda offset, length: plane[offset : offset + length],
        )
        self.assertEqual(
            validate_samples(samples, metrics, queries, truth, retained,
                             artifacts, ids, vectors, (1000, 1000), limits), metrics
        )
        changed = (dataclasses.replace(samples[0], pq_pages=(0,)),)
        with self.assertRaisesRegex(ValueError, "independent route"):
            validate_samples(changed, metrics, queries, truth, retained,
                             artifacts, ids, vectors, (1000, 1000), limits)


if __name__ == "__main__":
    unittest.main()
