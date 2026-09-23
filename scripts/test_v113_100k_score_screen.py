"""Query-level V113 score-fidelity checks without ground truth or page I/O."""

import unittest
import hashlib
import json
import tempfile
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from scripts.v113_100k_score_screen import (
    build_arrays, build_from_source, compare_one_query,
    nominate_pq64, primary_rows, score_from_artifact,
)
from scripts.v113_resident_score import encode_corrections_from_codes
from scripts.validate_v113_100k_score_screen import validate_evidence


class ScoreScreenTests(unittest.TestCase):
    def test_pq64_nomination_uses_score_then_row_order(self) -> None:
        books = np.zeros((64, 256, 1), dtype=np.float32)
        books[0, 1, 0] = 1.0
        codes = np.zeros((4, 64), dtype=np.uint8)
        codes[0, 0] = 1
        query = np.zeros(64, dtype=np.float32)
        np.testing.assert_array_equal(
            nominate_pq64(query, books, codes, 3),
            np.array([1, 2, 3]),
        )

    def test_primary_rows_break_equal_scores_by_identifier(self) -> None:
        rows = np.array([3, 4, 5], dtype=np.int32)
        ids = np.array([20, 10, 30], dtype=np.int64)
        scores = np.array([1.0, 1.0, 0.0], dtype=np.float64)
        np.testing.assert_array_equal(
            primary_rows(rows, ids, scores, 2),
            np.array([5, 4]),
        )

    def test_identical_rows_keep_primary_membership_without_gt(self) -> None:
        books = np.zeros((64, 256, 1), dtype=np.float32)
        pq_codes = np.zeros((256, 64), dtype=np.uint8)
        sq8_codes = np.ones((256, 64), dtype=np.uint8)
        norm = np.full(256, 64.0, dtype=np.float32)
        low = np.zeros(64, dtype=np.float32)
        step = np.ones(64, dtype=np.float32)
        corrections = encode_corrections_from_codes(
            books, pq_codes, sq8_codes, norm, low, step,
        )
        residual_books = np.zeros((12, 256, 6), dtype=np.float32)
        residual_books[:, 0, :].fill(1.0)
        residual_codes = np.zeros((256, 12), dtype=np.uint8)
        result = compare_one_query(
            np.ones(64, dtype=np.float32),
            np.arange(256, dtype=np.int64),
            books, pq_codes, residual_books, residual_codes,
            corrections, sq8_codes, norm, low, step,
            shortlist=128, primary_count=10,
        )
        self.assertEqual(result.resident_overlap, 10)
        self.assertEqual(result.scalar_overlap, 10)
        self.assertEqual(result.oracle_primary.size, 10)

    def test_source_only_builder_supports_non_768_dimensions(self) -> None:
        rng = np.random.default_rng(116)
        vectors = rng.normal(size=(256, 65)).astype(np.float32)
        built = build_arrays(
            vectors, np.arange(256, dtype=np.int64),
            pq_seed=7301, residual_seed=113031,
            sample_rows=256, iterations=1,
        )
        self.assertEqual(built.pq_codes.shape, (256, 64))
        self.assertEqual(built.residual_codes.shape, (256, 12))
        self.assertEqual(built.sq8_codes.shape, (256, 65))
        result = compare_one_query(
            rng.normal(size=65).astype(np.float32), built.ids,
            built.pq_books, built.pq_codes,
            built.residual_books, built.residual_codes,
            built.corrections, built.sq8_codes, built.sq8_norm,
            built.low, built.span_step, shortlist=128, primary_count=10,
        )
        self.assertTrue(0 <= result.resident_overlap <= 10)

    def test_two_phase_screen_uses_sealed_codes_and_no_truth(self) -> None:
        rng = np.random.default_rng(118)
        vectors = rng.normal(size=(256, 64)).astype(np.float32)
        queries = rng.normal(size=(2, 64)).astype(np.float32)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source.parquet"
            query_path = root / "queries.parquet"
            pq.write_table(pa.table({
                "feature_row_id": pa.array(np.arange(256, dtype=np.int64)),
                "embedding": pa.FixedSizeListArray.from_arrays(
                    pa.array(vectors.ravel()), 64,
                ),
            }), source)
            pq.write_table(pa.table({
                "embedding": pa.FixedSizeListArray.from_arrays(
                    pa.array(queries.ravel()), 64,
                ),
            }), query_path)
            digest = lambda path: hashlib.sha256(path.read_bytes()).hexdigest()
            artifact = root / "artifact"
            build_from_source(
                source, digest(source), artifact, rows=256, dimensions=64,
                sample_rows=256, iterations=1,
            )
            evidence = root / "evidence.jsonl"
            reduction = root / "summary.json"
            score_from_artifact(
                artifact, digest(source), query_path, digest(query_path),
                evidence, reduction, rows=256, dimensions=64,
                query_count=2, shortlist=128, primary_count=10,
                sample_rows=256, iterations=1,
            )
            records = [json.loads(line) for line in evidence.read_text().splitlines()]
            self.assertEqual(len(records), 3)
            self.assertFalse(records[0]["ground_truth_used"])
            self.assertEqual(json.loads(reduction.read_text())["query_count"], 2)
            validate_evidence(
                artifact, source, digest(source), query_path, digest(query_path),
                evidence, reduction, rows=256, dimensions=64,
                query_count=2, shortlist=128, primary_count=10,
                sample_rows=256, iterations=1,
            )
            records[1]["resident_overlap"] += 1
            evidence.write_text("\n".join(json.dumps(row) for row in records) + "\n")
            with self.assertRaises(ValueError):
                validate_evidence(
                    artifact, source, digest(source), query_path, digest(query_path),
                    evidence, reduction, rows=256, dimensions=64,
                    query_count=2, shortlist=128, primary_count=10,
                    sample_rows=256, iterations=1,
                )


if __name__ == "__main__":
    unittest.main()
