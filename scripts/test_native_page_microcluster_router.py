"""Tests for the query-blind page-local microcluster route."""

from __future__ import annotations

import dataclasses
import tempfile
import unittest
from pathlib import Path

import numpy as np

from scripts.native_geometric_layout_screen import (
    EvaluationLimits,
    LayoutMethod,
    MembershipRow,
)
from scripts.native_page_microcluster_router import (
    PageMicroclusters,
    construct_representatives,
    read_representatives,
    route_query,
    write_representatives,
)


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


if __name__ == "__main__":
    unittest.main()
