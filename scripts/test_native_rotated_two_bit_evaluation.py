"""Authenticated group wave and paired two-bit row-score tests."""

from __future__ import annotations

import hashlib
import tempfile
import unittest
from pathlib import Path

import numpy as np

from scripts.native_geometric_layout_screen import (
    EvaluationLimits,
    LayoutMethod,
    MembershipRow,
)
from scripts.native_rotated_two_bit_codes import (
    construct_two_bit_codes,
    decode_levels,
    rotate_rows,
    write_two_bit_codes,
)
from scripts.native_rotated_two_bit_evaluation import (
    aggregate_two_bit_samples,
    evaluate_two_bit_query,
    exact_scores,
    score_records,
)
from scripts.native_row_score_nomination import nominate_pages
from scripts.test_native_page_centered_group_evaluation import CountingReader
from scripts.validate_native_rotated_two_bit_result import (
    _exact_independently,
    _nominate_independently,
    _score_independently,
)


class TwoBitEvaluationTests(unittest.TestCase):
    def test_768_dimension_exact_ties_replay_identically(self) -> None:
        rng = np.random.default_rng(719)
        one = rng.normal(size=768).astype(np.float32)
        vectors = np.stack([np.roll(one, shift) for shift in range(200)])
        query = np.zeros(768, dtype=np.float32)
        sources = tuple(range(200))
        pages = tuple(index // 50 for index in sources)
        producer = exact_scores(query, vectors, sources)
        replay = _exact_independently(query, vectors, sources)
        self.assertEqual(len(np.unique(producer)), 1)
        np.testing.assert_array_equal(producer, replay)
        limits = EvaluationLimits(2, 2000)
        self.assertEqual(
            nominate_pages(producer, sources, pages, (1000,) * 4, limits),
            _nominate_independently(replay, sources, pages, (1000,) * 4, limits),
        )

    @staticmethod
    def fixture():
        rng = np.random.default_rng(207)
        vectors = rng.normal(size=(120, 768)).astype(np.float32)
        ids = tuple(index.to_bytes(4, "little") for index in range(len(vectors)))
        source_sha = hashlib.sha256(b"source").digest()
        membership = tuple(
            MembershipRow(
                stable_id=ids[index],
                source_ordinal=index,
                page_ordinal=index // 24,
                in_page_ordinal=index % 24,
                page_rows=24,
                encoded_page_bytes=1000,
                method=LayoutMethod.TWO_MEANS_480K,
                source_sha256=source_sha,
                seed=20260921,
                construction_sha256=hashlib.sha256(b"layout").digest(),
            )
            for index in range(len(vectors))
        )
        artifacts = construct_two_bit_codes(
            ids, vectors, membership, layout_seed=20260921, rotation_seed=20260923
        )
        return ids, vectors, membership, artifacts

    def test_scores_match_direct_reconstruction_and_norm_diagnostic(self) -> None:
        _, vectors, _, artifacts = self.fixture()
        query = vectors[0]
        primary, diagnostic = score_records(query, artifacts.mean, artifacts.records[:8], rotation_seed=20260923)
        centered_q = query.astype(np.float64) - artifacts.mean.astype(np.float64)
        rotated_q = rotate_rows(centered_q[None, :], rotation_seed=20260923)[0]
        levels = decode_levels(artifacts.records[:8, :192]).astype(np.float64)
        scales = np.frombuffer(artifacts.records[:8, 192:196].tobytes(), dtype="<f4").astype(np.float64)
        norms = np.frombuffer(artifacts.records[:8, 196:200].tobytes(), dtype="<f4").astype(np.float64)
        direct = np.sum((rotated_q[None, :] - scales[:, None] * levels) ** 2, axis=1).astype(np.float32)
        corrected = (
            np.dot(rotated_q, rotated_q)
            + norms
            - 2 * scales * np.sum(rotated_q[None, :] * levels, axis=1)
        ).astype(np.float32)
        np.testing.assert_allclose(primary, direct, rtol=0, atol=1e-4)
        np.testing.assert_array_equal(diagnostic, corrected)
        for other_query in vectors[:10]:
            producer = score_records(
                other_query, artifacts.mean, artifacts.records,
                rotation_seed=20260923,
            )
            independent = _score_independently(
                other_query, artifacts.mean, artifacts.records, 20260923
            )
            for left, right in zip(producer, independent, strict=True):
                np.testing.assert_array_equal(left, right)

    def test_actual_ranges_match_exact_closed_control(self) -> None:
        ids, vectors, _, artifacts = self.fixture()
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            write_two_bit_codes(
                root, artifacts, hashlib.sha256(b"source").digest(),
                hashlib.sha256(b"membership").digest(), hashlib.sha256(b"tree").digest(),
            )
            body = (root / "groups.bin").read_bytes()
        sources = tuple(range(96, 120)) + tuple(range(96))
        pages = (4,) * 24 + tuple(index // 24 for index in range(96))
        delta = vectors[list(sources)].astype(np.float64) - vectors[0].astype(np.float64)
        exact = np.sum(delta * delta, axis=1).astype(np.float32)
        limits = EvaluationLimits(2, 2000)
        expected = nominate_pages(exact, sources, pages, (1000,) * 5, limits)
        reader = CountingReader(body)
        sample = evaluate_two_bit_query(
            query_ordinal=0,
            query=vectors[0],
            retained_pages=(4, 0),
            artifacts=artifacts,
            stable_ids=ids,
            vectors=vectors,
            truth_ids=ids[:100],
            page_byte_sizes=(1000,) * 5,
            limits=limits,
            read_group_range=reader,
            prior_exact_pages=expected,
            prior_pq_hits_at_100=80,
            prior_residual_hits_at_100=81,
        )
        self.assertEqual(sample.exact_pages, expected)
        self.assertEqual(sample.code_gets, reader.gets)
        self.assertEqual(sample.code_bytes, reader.bytes)
        self.assertEqual(sample.grouped_pages, (4, 0, 1, 2, 3))
        self.assertEqual(sample.prior_residual_hits_at_100, 81)
        self.assertEqual(aggregate_two_bit_samples((sample,))["query_count"], 1)
        class TrackingReader(CountingReader):
            def __init__(self, body: bytes) -> None:
                super().__init__(body)
                self.calls: list[tuple[int, int]] = []

            def __call__(self, offset: int, length: int) -> bytes:
                self.calls.append((offset, length))
                return super().__call__(offset, length)

        selected_reader = TrackingReader(body)
        selected = evaluate_two_bit_query(
            query_ordinal=0, query=vectors[0], retained_pages=(0, 4),
            selected_group_ordinals=(1, 0),
            artifacts=artifacts, stable_ids=ids, vectors=vectors,
            truth_ids=ids[:100], page_byte_sizes=(1000,) * 5,
            limits=limits, read_group_range=selected_reader,
            prior_exact_pages=None, prior_pq_hits_at_100=80,
            prior_residual_hits_at_100=81,
        )
        self.assertEqual(selected.grouped_pages, (4, 0, 1, 2, 3))
        self.assertEqual(selected.group_ranges, (artifacts.group_ranges[1], artifacts.group_ranges[0]))
        self.assertEqual(selected_reader.calls, [(group[2], group[3]) for group in selected.group_ranges])
        self.assertEqual(selected.code_gets, 2)
        self.assertEqual(selected.code_bytes, sum(group[3] for group in selected.group_ranges))
        with self.assertRaisesRegex(ValueError, "selected group plan"):
            evaluate_two_bit_query(
                query_ordinal=0, query=vectors[0], retained_pages=(0, 4),
                selected_group_ordinals=(0, 0), artifacts=artifacts,
                stable_ids=ids, vectors=vectors, truth_ids=ids[:100],
                page_byte_sizes=(1000,) * 5, limits=limits,
                read_group_range=CountingReader(body), prior_exact_pages=None,
                prior_pq_hits_at_100=80, prior_residual_hits_at_100=81,
            )
        with self.assertRaisesRegex(ValueError, "closed exact"):
            evaluate_two_bit_query(
                query_ordinal=0, query=vectors[0], retained_pages=(4, 0),
                artifacts=artifacts, stable_ids=ids, vectors=vectors, truth_ids=ids[:100],
                page_byte_sizes=(1000,) * 5, limits=limits,
                read_group_range=CountingReader(body), prior_exact_pages=(99,),
                prior_pq_hits_at_100=80, prior_residual_hits_at_100=81,
            )
        damaged = bytearray(body)
        damaged[16] ^= 1
        with self.assertRaisesRegex(ValueError, "group.*identity"):
            evaluate_two_bit_query(
                query_ordinal=0, query=vectors[0], retained_pages=(4, 0),
                artifacts=artifacts, stable_ids=ids, vectors=vectors, truth_ids=ids[:100],
                page_byte_sizes=(1000,) * 5, limits=limits,
                read_group_range=CountingReader(bytes(damaged)), prior_exact_pages=expected,
                prior_pq_hits_at_100=80, prior_residual_hits_at_100=81,
            )


if __name__ == "__main__":
    unittest.main()
