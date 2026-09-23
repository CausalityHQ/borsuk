"""Canonical two-bit evidence and independent source/score replay."""

from __future__ import annotations

import dataclasses
import hashlib
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

import scripts.test_native_rotated_two_bit_evaluation as evaluation_tests
from scripts.native_geometric_layout_screen import EvaluationLimits
from scripts.native_rotated_two_bit_codes import write_two_bit_codes
from scripts.native_rotated_two_bit_evaluation import (
    aggregate_two_bit_samples,
    evaluate_two_bit_query,
)
from scripts.native_rotated_two_bit_evidence import (
    read_two_bit_evidence,
    write_two_bit_evidence,
)
from scripts.test_native_page_centered_group_evaluation import CountingReader
from scripts.validate_native_rotated_two_bit_result import validate_two_bit_samples


class TwoBitEvidenceTests(unittest.TestCase):
    def fixture(self):
        ids, vectors, membership, artifacts = evaluation_tests.TwoBitEvaluationTests.fixture()
        temporary = tempfile.TemporaryDirectory()
        root = Path(temporary.name)
        source_sha = hashlib.sha256(b"source").digest()
        identities = write_two_bit_codes(
            root, artifacts, source_sha, hashlib.sha256(b"membership").digest(),
            hashlib.sha256(b"tree").digest(),
        )
        body = (root / "groups.bin").read_bytes()
        sources = tuple(range(96, 120)) + tuple(range(96))
        pages = (4,) * 24 + tuple(index // 24 for index in range(96))
        from scripts.native_rotated_two_bit_evaluation import exact_scores
        from scripts.native_row_score_nomination import nominate_pages
        expected_exact = nominate_pages(
            exact_scores(vectors[0], vectors, sources), sources, pages,
            (1000,) * 5, EvaluationLimits(2, 2000),
        )
        sample = evaluate_two_bit_query(
            query_ordinal=0, query=vectors[0], retained_pages=(4, 0),
            artifacts=artifacts, stable_ids=ids, vectors=vectors, truth_ids=ids[:100],
            page_byte_sizes=(1000,) * 5, limits=EvaluationLimits(2, 2000),
            read_group_range=CountingReader(body), prior_exact_pages=expected_exact,
            prior_pq_hits_at_100=80, prior_residual_hits_at_100=81,
        )
        prior = SimpleNamespace(
            query_ordinal=0,
            retained_pages=(4, 0),
            grouped_pages=sample.grouped_pages,
            exact_pages=sample.exact_pages,
            exact_hits_at_10=sample.exact_hits_at_10,
            exact_hits_at_100=sample.exact_hits_at_100,
            prior_pq_hits_at_100=80,
            prior_residual_hits_at_100=81,
        )
        return temporary, root, ids, vectors, membership, artifacts, identities, body, sample, prior

    def test_canonical_evidence_and_independent_replay(self) -> None:
        temporary, root, ids, vectors, membership, artifacts, _, body, sample, prior = self.fixture()
        self.addCleanup(temporary.cleanup)
        identity = write_two_bit_evidence(root / "evidence.json", (sample,))
        samples, metrics = read_two_bit_evidence(root / "evidence.json", identity)
        self.assertEqual(samples, (sample,))
        self.assertEqual(metrics, aggregate_two_bit_samples((sample,)))

        def replay(*, changed_samples=(sample,), changed_metrics=metrics, changed_bytes=body, changed_prior=prior):
            return validate_two_bit_samples(
                changed_samples, changed_metrics,
                queries=vectors[:1], truth=(ids[:100],), retained_routes=((4, 0),),
                ids=ids, vectors=vectors, membership=membership, artifacts=artifacts,
                group_bytes=changed_bytes, page_byte_sizes=(1000,) * 5,
                limits=EvaluationLimits(2, 2000), prior_group_samples=(changed_prior,),
            )

        self.assertEqual(replay()["metrics"], metrics)
        for changed in (
            dataclasses.replace(sample, primary_pages=sample.primary_pages[::-1]),
            dataclasses.replace(sample, exact_hits_at_100=sample.exact_hits_at_100 - 1),
            dataclasses.replace(sample, prior_pq_hits_at_100=79),
        ):
            with self.assertRaisesRegex(ValueError, "two-bit independent sample"):
                replay(changed_samples=(changed,))
        with self.assertRaisesRegex(ValueError, "two-bit independent aggregate"):
            replay(changed_metrics=dict(metrics, max_code_bytes=1))
        damaged = bytearray(body)
        damaged[-1] ^= 1
        with self.assertRaisesRegex(ValueError, "two-bit independent group"):
            replay(changed_bytes=bytes(damaged))
        with self.assertRaisesRegex(ValueError, "closed exact"):
            replay(changed_prior=SimpleNamespace(**dict(prior.__dict__, exact_pages=(99,))))
        with self.assertRaisesRegex(ValueError, "evidence identity"):
            read_two_bit_evidence(
                root / "evidence.json",
                dataclasses.replace(identity, sha256="ab" * 32),
            )


if __name__ == "__main__":
    unittest.main()
