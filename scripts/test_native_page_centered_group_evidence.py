"""Canonical group evidence and independent direct-score replay."""

from __future__ import annotations

import dataclasses
import hashlib
import tempfile
import unittest
from pathlib import Path

from scripts.native_geometric_layout_screen import EvaluationLimits
from scripts.native_page_centered_group_codes import (
    construct_group_codes,
    read_group_codes,
    write_group_codes,
)
from scripts.native_page_centered_group_evaluation import (
    aggregate_group_samples,
    evaluate_group_query,
)
from scripts.native_page_centered_group_evidence import (
    read_group_evidence,
    write_group_evidence,
)
from scripts.test_native_page_centered_group_codes import GroupCodeTests
from scripts.test_native_page_centered_group_evaluation import CountingReader
from scripts.validate_native_page_centered_group_result import validate_group_samples


class GroupEvidenceTests(unittest.TestCase):
    def fixture(self):
        ids, vectors, membership, source_sha = GroupCodeTests.fixture()
        artifacts = construct_group_codes(ids, vectors, membership, seed=7, iterations=1)
        temporary = tempfile.TemporaryDirectory()
        root = Path(temporary.name)
        membership_sha = hashlib.sha256(b"membership").digest()
        tree_sha = hashlib.sha256(b"tree").digest()
        identities = write_group_codes(
            root, artifacts, source_sha, membership_sha, tree_sha
        )
        body = (root / "groups.bin").read_bytes()
        truth = tuple(ids[index] for index in range(192, 292))
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
            read_group_range=CountingReader(body),
            prior_pq_hits_at_100=42,
            prior_residual_hits_at_100=43,
        )
        return (
            temporary,
            root,
            ids,
            vectors,
            membership,
            source_sha,
            membership_sha,
            tree_sha,
            identities,
            artifacts,
            body,
            truth,
            sample,
        )

    def test_canonical_evidence_and_full_independent_replay(self) -> None:
        data = self.fixture()
        (temporary, root, ids, vectors, membership, _, _, _, _, artifacts, body, truth, sample) = data
        self.addCleanup(temporary.cleanup)
        identity = write_group_evidence(root / "evidence.json", (sample,))
        samples, metrics = read_group_evidence(root / "evidence.json", identity)
        self.assertEqual(samples, (sample,))
        self.assertEqual(metrics, aggregate_group_samples((sample,)))
        result = validate_group_samples(
            samples,
            metrics,
            queries=vectors[:1],
            truth=(truth,),
            retained_routes=((4, 0),),
            ids=ids,
            vectors=vectors,
            membership=membership,
            artifacts=artifacts,
            group_bytes=body,
            page_byte_sizes=(1000,) * 5,
            limits=EvaluationLimits(2, 2000),
            prior_hits=((42, 43),),
        )
        self.assertEqual(result["metrics"], metrics)
        for changed in (
            dataclasses.replace(sample, group_ranges=sample.group_ranges[::-1]),
            dataclasses.replace(sample, coded_pages=sample.coded_pages[::-1]),
            dataclasses.replace(sample, coded_hits_at_100=sample.coded_hits_at_100 - 1),
            dataclasses.replace(sample, prior_residual_hits_at_100=41),
        ):
            with self.assertRaisesRegex(ValueError, "group .* differs"):
                validate_group_samples(
                    (changed,),
                    metrics,
                    queries=vectors[:1],
                    truth=(truth,),
                    retained_routes=((4, 0),),
                    ids=ids,
                    vectors=vectors,
                    membership=membership,
                    artifacts=artifacts,
                    group_bytes=body,
                    page_byte_sizes=(1000,) * 5,
                    limits=EvaluationLimits(2, 2000),
                    prior_hits=((42, 43),),
                )
        changed_body = bytearray(body)
        changed_body[8] ^= 1
        with self.assertRaisesRegex(ValueError, "group .* differs"):
            validate_group_samples(
                samples,
                metrics,
                queries=vectors[:1],
                truth=(truth,),
                retained_routes=((4, 0),),
                ids=ids,
                vectors=vectors,
                membership=membership,
                artifacts=artifacts,
                group_bytes=bytes(changed_body),
                page_byte_sizes=(1000,) * 5,
                limits=EvaluationLimits(2, 2000),
                prior_hits=((42, 43),),
            )
        with self.assertRaisesRegex(ValueError, "group .* differs"):
            validate_group_samples(
                samples,
                {**metrics, "coded_mean_recall_at_100_ppm": 0},
                queries=vectors[:1],
                truth=(truth,),
                retained_routes=((4, 0),),
                ids=ids,
                vectors=vectors,
                membership=membership,
                artifacts=artifacts,
                group_bytes=body,
                page_byte_sizes=(1000,) * 5,
                limits=EvaluationLimits(2, 2000),
                prior_hits=((42, 43),),
            )

    def test_source_membership_binding_and_evidence_digest(self) -> None:
        data = self.fixture()
        (temporary, root, ids, vectors, membership, source_sha, membership_sha, tree_sha, identities, _, _, _, sample) = data
        self.addCleanup(temporary.cleanup)
        identity = write_group_evidence(root / "evidence.json", (sample,))
        body = bytearray((root / "evidence.json").read_bytes())
        body[-2] ^= 1
        (root / "evidence.json").write_bytes(body)
        with self.assertRaisesRegex(ValueError, "identity"):
            read_group_evidence(root / "evidence.json", identity)
        with self.assertRaisesRegex(ValueError, "source binding|seal"):
            read_group_codes(
                root,
                identities,
                ids,
                membership,
                hashlib.sha256(b"other source").digest(),
                membership_sha,
                tree_sha,
                dimensions=48,
                seed=7,
            )
        with self.assertRaisesRegex(ValueError, "seal"):
            read_group_codes(
                root,
                identities,
                ids,
                membership,
                source_sha,
                hashlib.sha256(b"other membership").digest(),
                tree_sha,
                dimensions=48,
                seed=7,
            )


if __name__ == "__main__":
    unittest.main()
