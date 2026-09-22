"""Independent residual page and hit replay rejects sealed-evidence drift."""

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
from scripts.native_residual_row_score_codes import construct_residual_codes
from scripts.native_residual_row_score_evaluation import (
    aggregate_residual_samples,
    evaluate_residual_query,
)
from scripts.validate_native_residual_row_score_result import validate_residual_samples


class ResidualReplayTests(unittest.TestCase):
    def test_replay_rejects_pages_blocks_and_truth_hits(self) -> None:
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
        query = vectors[180]
        targets = tuple(ids[index] for index in range(128, 228))
        plane = artifacts.codes.tobytes(order="C")
        limits = EvaluationLimits(maximum_pages=1, maximum_bytes=1000)
        sample = evaluate_residual_query(
            query_ordinal=0, query=query, retained_pages=(0, 1),
            artifacts=artifacts, stable_ids=ids, vectors=vectors,
            truth_ids=targets, page_byte_sizes=(1000, 1000), limits=limits,
            read_code_range=lambda offset, length: plane[offset : offset + length],
        )
        inputs = dict(
            queries=query[None, :], truth=(targets,), retained_by_query=((0, 1),),
            artifacts=artifacts, stable_ids=ids, vectors=vectors,
            page_byte_sizes=(1000, 1000), limits=limits,
        )
        metrics = aggregate_residual_samples((sample,))
        self.assertEqual(validate_residual_samples((sample,), metrics, **inputs), metrics)
        for changed in (
            dataclasses.replace(sample, residual_pages=(0,)),
            dataclasses.replace(sample, code_blocks=((0, 2, 0, 1),)),
            dataclasses.replace(sample, residual_hits_at_100=99),
        ):
            with self.assertRaisesRegex(ValueError, "independent residual"):
                validate_residual_samples((changed,), metrics, **inputs)


if __name__ == "__main__":
    unittest.main()
