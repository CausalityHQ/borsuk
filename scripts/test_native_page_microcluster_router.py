"""Tests for the query-blind page-local microcluster route."""

from __future__ import annotations

import dataclasses
import hashlib
import tempfile
import unittest
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from scripts.native_geometric_layout_screen import (
    EvaluationLimits,
    LayoutMethod,
    MembershipRow,
)
from scripts.native_page_microcluster_router import (
    MicroclusterEvaluation,
    MicroclusterSample,
    PageMicroclusters,
    construct_representatives,
    evaluate_representatives,
    microcluster_evidence_schema,
    read_representatives,
    route_query,
    write_evidence,
    write_representatives,
)
from scripts.validate_native_page_microcluster_result import validate_evidence


class PageMicroclusterRouteTests(unittest.TestCase):
    @staticmethod
    def membership(stable_ids: tuple[bytes, ...]) -> tuple[MembershipRow, ...]:
        return tuple(
            MembershipRow(
                stable_id=stable_ids[ordinal],
                source_ordinal=ordinal,
                page_ordinal=ordinal // 8,
                in_page_ordinal=ordinal % 8,
                page_rows=8,
                encoded_page_bytes=80,
                method=LayoutMethod.TWO_MEANS_480K,
                source_sha256=bytes.fromhex("11" * 32),
                seed=20260921,
                construction_sha256=bytes.fromhex("22" * 32),
            )
            for ordinal in range(len(stable_ids))
        )

    def test_builds_eight_query_blind_means_for_each_sealed_page(self) -> None:
        stable_ids = tuple(f"id-{ordinal:02d}".encode() for ordinal in range(16))
        vectors = np.asarray(
            [
                (float(ordinal),) if ordinal < 8 else (float(ordinal + 20),)
                for ordinal in range(16)
            ],
            dtype=np.float32,
        )
        pages = construct_representatives(
            self.membership(stable_ids), stable_ids, vectors
        )
        self.assertEqual([page.page_ordinal for page in pages], [0, 1])
        self.assertEqual([len(page.means) for page in pages], [8, 8])
        self.assertEqual(
            sorted(mean[0] for mean in pages[0].means),
            [float(value) for value in range(8)],
        )
        self.assertEqual(
            route_query(
                pages,
                np.asarray((0.1,), dtype=np.float32),
                EvaluationLimits(maximum_pages=1, maximum_bytes=80),
            ),
            (0,),
        )

    def test_near_child_wins_when_whole_page_centroid_would_lose(self) -> None:
        pages = (
            PageMicroclusters(
                page_ordinal=0,
                encoded_page_bytes=80,
                means=((0.0,), (10.0,)) * 4,
            ),
            PageMicroclusters(
                page_ordinal=1,
                encoded_page_bytes=80,
                means=((4.0,),) * 8,
            ),
        )
        selected = route_query(
            pages,
            np.asarray((0.1,), dtype=np.float32),
            EvaluationLimits(maximum_pages=1, maximum_bytes=80),
        )
        self.assertEqual(selected, (0,))

    def test_equal_scores_use_page_ordinal_and_skip_over_budget_page(self) -> None:
        pages = (
            PageMicroclusters(0, 90, ((0.0,),) * 8),
            PageMicroclusters(1, 40, ((0.0,),) * 8),
            PageMicroclusters(2, 40, ((1.0,),) * 8),
        )
        selected = route_query(
            pages,
            np.asarray((0.0,), dtype=np.float32),
            EvaluationLimits(maximum_pages=2, maximum_bytes=80),
        )
        self.assertEqual(selected, (1, 2))

    def test_rejects_unbound_membership_and_unsplittable_page(self) -> None:
        stable_ids = tuple(f"id-{ordinal:02d}".encode() for ordinal in range(16))
        membership = self.membership(stable_ids)
        vectors = np.arange(16, dtype=np.float32).reshape(16, 1)
        duplicate = list(membership)
        duplicate[1] = dataclasses.replace(duplicate[1], source_ordinal=0)
        with self.assertRaisesRegex(ValueError, "membership"):
            construct_representatives(duplicate, stable_ids, vectors)

        constant_first_page = vectors.copy()
        constant_first_page[:8] = 0.0
        with self.assertRaisesRegex(ValueError, "unsplittable"):
            construct_representatives(membership, stable_ids, constant_first_page)

    def test_representatives_round_trip_with_source_and_membership_binding(
        self,
    ) -> None:
        pages = (
            PageMicroclusters(0, 80, tuple((float(value),) for value in range(8))),
            PageMicroclusters(1, 80, tuple((float(value),) for value in range(8, 16))),
        )
        source_sha = bytes.fromhex("11" * 32)
        membership_sha = bytes.fromhex("22" * 32)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "representatives.parquet"
            identity = write_representatives(path, pages, source_sha, membership_sha)
            self.assertEqual(
                read_representatives(path, identity, source_sha, membership_sha), pages
            )
            remote_identity = dataclasses.replace(
                identity, uri="s3://sealed-cell/artifacts/representatives.parquet"
            )
            self.assertEqual(
                read_representatives(path, remote_identity, source_sha, membership_sha),
                pages,
            )
            with self.assertRaisesRegex(ValueError, "membership"):
                read_representatives(
                    path, identity, source_sha, bytes.fromhex("33" * 32)
                )
            path.write_bytes(path.read_bytes() + b"tampered")
            with self.assertRaisesRegex(ValueError, "identity"):
                read_representatives(path, identity, source_sha, membership_sha)

    def test_evaluation_records_exact_gt_page_containment(self) -> None:
        stable_ids = tuple(f"id-{ordinal:03d}".encode() for ordinal in range(100))
        membership = tuple(
            MembershipRow(
                stable_id=stable_ids[ordinal],
                source_ordinal=ordinal,
                page_ordinal=ordinal // 50,
                in_page_ordinal=ordinal % 50,
                page_rows=50,
                encoded_page_bytes=80,
                method=LayoutMethod.TWO_MEANS_480K,
                source_sha256=bytes.fromhex("11" * 32),
                seed=20260921,
                construction_sha256=bytes.fromhex("22" * 32),
            )
            for ordinal in range(100)
        )
        pages = (
            PageMicroclusters(0, 80, ((0.0,),) * 8),
            PageMicroclusters(1, 80, ((10.0,),) * 8),
        )
        result = evaluate_representatives(
            pages,
            membership,
            np.asarray(((0.0,),), dtype=np.float32),
            (stable_ids,),
            EvaluationLimits(maximum_pages=1, maximum_bytes=80),
        )
        self.assertEqual(result.samples[0].selected_page_ordinals, (0,))
        self.assertEqual(result.samples[0].hits_at_10, 10)
        self.assertEqual(result.samples[0].hits_at_100, 50)
        self.assertEqual(result.mean_recall_at_100_ppm, 500_000)
        self.assertEqual(result.decision, "killed")

    def test_evaluation_rejects_empty_page_roster(self) -> None:
        with self.assertRaisesRegex(ValueError, "pages"):
            evaluate_representatives(
                (),
                (),
                np.zeros((1, 1), dtype=np.float32),
                (tuple(f"id-{ordinal}".encode() for ordinal in range(100)),),
                EvaluationLimits(maximum_pages=1, maximum_bytes=80),
            )

    def test_evidence_records_per_query_route_and_hits(self) -> None:
        evaluation = MicroclusterEvaluation(
            samples=(MicroclusterSample(0, (1, 2), 160, 8, 70, 42),),
            recall_at_10_ppm=800_000,
            mean_recall_at_100_ppm=700_000,
            p05_recall_at_100_ppm=700_000,
            worst_recall_at_100_ppm=700_000,
            max_pages=2,
            max_bytes=160,
            decision="killed",
        )
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "evidence.parquet"
            identity = write_evidence(path, evaluation)
            self.assertEqual(identity.role, "page-microcluster-evidence")
            self.assertEqual(pq.read_schema(path), microcluster_evidence_schema())
            row = pq.read_table(path).to_pylist()[0]
            self.assertEqual(row["selected_page_ordinals"], [1, 2])
            self.assertEqual(row["hits_at_100"], 70)

    def test_independent_replay_rejects_rehashed_false_page_choice(self) -> None:
        stable_ids = tuple(f"id-{ordinal:03d}".encode() for ordinal in range(100))
        membership = tuple(
            MembershipRow(
                stable_id=stable_ids[ordinal],
                source_ordinal=ordinal,
                page_ordinal=ordinal // 50,
                in_page_ordinal=ordinal % 50,
                page_rows=50,
                encoded_page_bytes=80,
                method=LayoutMethod.TWO_MEANS_480K,
                source_sha256=bytes.fromhex("11" * 32),
                seed=20260921,
                construction_sha256=bytes.fromhex("22" * 32),
            )
            for ordinal in range(100)
        )
        pages = (
            PageMicroclusters(0, 80, ((0.0,),) * 8),
            PageMicroclusters(1, 80, ((10.0,),) * 8),
        )
        queries = np.asarray(((0.0,),), dtype=np.float32)
        truth = (stable_ids,)
        limits = EvaluationLimits(maximum_pages=1, maximum_bytes=80)
        evaluation = evaluate_representatives(pages, membership, queries, truth, limits)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "evidence.parquet"
            identity = write_evidence(path, evaluation)
            replay = validate_evidence(
                path, identity, pages, membership, queries, truth, limits
            )
            self.assertEqual(replay["mean_recall_at_100_ppm"], 500_000)
            self.assertEqual(replay["decision"], "killed")

            table = pq.read_table(path)
            columns = [
                pa.array([[1]], type=table.schema.field("selected_page_ordinals").type)
                if name == "selected_page_ordinals"
                else table[name].combine_chunks()
                for name in table.column_names
            ]
            pq.write_table(
                pa.Table.from_arrays(columns, schema=table.schema),
                path,
                version="2.6",
                compression="zstd",
                use_dictionary=False,
                write_statistics=True,
            )
            body = path.read_bytes()
            false_identity = dataclasses.replace(
                identity,
                sha256=hashlib.sha256(body).hexdigest(),
                encoded_bytes=len(body),
            )
            with self.assertRaisesRegex(ValueError, "route"):
                validate_evidence(
                    path, false_identity, pages, membership, queries, truth, limits
                )


if __name__ == "__main__":
    unittest.main()
