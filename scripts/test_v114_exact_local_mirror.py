"""Small source-only fixtures for the exact local SQ8 wire format."""

import hashlib
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from scripts.v114_exact_local_100k import (
    compare_records, prepare_one, read_queries, route_reference,
    score_reference, write_mirror,
)


class ExactLocalMirrorTests(unittest.TestCase):
    def test_comparison_rejects_score_or_physical_plan_drift(self) -> None:
        reference = {
            "query_ordinal": 0, "generation": 1,
            "primary": [2], "score_bits": [0, 1073741824],
            "page_votes": [[0, 1]], "ranges": [[0, 512]],
            "plan_bytes": 512, "plan_score": 1,
        }
        actual = {**reference, "ram_primary": [2], "file_primary": [2]}
        del actual["primary"]
        self.assertEqual(compare_records(reference, actual), [])
        self.assertIn("score_bits", compare_records(
            reference, {**actual, "score_bits": [1, 1073741824]},
        ))
        self.assertIn("ranges", compare_records(
            reference, {**actual, "ranges": [[0, 256]]},
        ))

    def test_query_reader_requires_frozen_hash_and_ordered_ids(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "queries.parquet"
            vectors = pa.FixedSizeListArray.from_arrays(
                pa.array(np.arange(8, dtype=np.float32)), 4,
            )
            pq.write_table(pa.table({
                "query": pa.array([0, 1], type=pa.int64()),
                "vector": vectors,
            }), path)
            digest = hashlib.sha256(path.read_bytes()).hexdigest()
            np.testing.assert_array_equal(
                read_queries(path, digest, query_count=2, dimensions=4),
                np.arange(8, dtype=np.float32).reshape(2, 4),
            )
            with self.assertRaises(ValueError):
                read_queries(path, "0" * 64, query_count=2, dimensions=4)
            pq.write_table(pa.table({
                "query": pa.array([1, 0], type=pa.int64()),
                "vector": vectors,
            }), path)
            with self.assertRaises(ValueError):
                read_queries(
                    path, hashlib.sha256(path.read_bytes()).hexdigest(),
                    query_count=2, dimensions=4,
                )

    def test_prepared_request_and_reference_share_source_only_pq_roster(self) -> None:
        rows, dimensions = 256, 64
        arrays = SimpleNamespace(
            ids=np.arange(rows, dtype=np.int64),
            sq8_norm=np.arange(rows, dtype=np.float32),
            sq8_codes=np.zeros((rows, dimensions), dtype=np.uint8),
            low=np.zeros(dimensions, dtype=np.float32),
            span_step=np.ones(dimensions, dtype=np.float32),
            pq_books=np.zeros((64, 256, 1), dtype=np.float32),
            pq_codes=np.zeros((rows, 64), dtype=np.uint8),
        )
        request, reference = prepare_one(
            0, np.zeros(dimensions, dtype=np.float32), arrays,
            shortlist=128, primary_count=10,
        )
        self.assertEqual(request["nominees"], list(range(128)))
        self.assertEqual(reference["primary"], list(range(10)))
        self.assertEqual(reference["page_votes"], [[0, 10]])
        self.assertEqual(reference["ranges"], [[0, 256 * (dimensions + 12)]])

    def test_route_reference_accounts_for_short_final_page_bytes(self) -> None:
        votes, ranges, byte_count, score = route_reference(
            [0, 256, 300], rows=416, dimensions=768,
        )
        self.assertEqual(votes, [[0, 1], [1, 2]])
        self.assertEqual(ranges, [[0, 324_480]])
        self.assertEqual(byte_count, 324_480)
        self.assertEqual(score, 3)

    def test_reference_scores_affine_codes_and_breaks_ties_by_id(self) -> None:
        primary, scores = score_reference(
            query=np.array([1.0, 0.0], dtype=np.float32),
            nominees=np.array([2, 0, 1], dtype=np.int32),
            ids=np.array([9, 4, 7], dtype=np.int64),
            norms=np.array([1.0, 1.0, 4.0], dtype=np.float32),
            codes=np.array([[1, 0], [1, 0], [2, 0]], dtype=np.uint8),
            low=np.zeros(2, dtype=np.float32),
            step=np.ones(2, dtype=np.float32),
            primary_count=2,
        )
        self.assertEqual(primary, [1, 0])
        np.testing.assert_array_equal(scores, np.array([1.0, 0.0, 0.0], dtype=np.float32))

    def test_writer_binds_rows_and_short_final_block(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "mirror"
            manifest = write_mirror(
                root,
                ids=np.array([4, 9], dtype=np.int64),
                norms=np.array([1.0, 4.0], dtype=np.float32),
                codes=np.array([[1, 0, 0, 0], [2, 0, 0, 0]], dtype=np.uint8),
                low=np.zeros(4, dtype=np.float32),
                step=np.ones(4, dtype=np.float32),
                generation=7,
                max_nominees=2,
            )
            object_bytes = (root / "sq8.bin").read_bytes()
            sidecar = (root / "blocks.sha256").read_bytes()
            self.assertEqual(len(object_bytes), 32)
            self.assertEqual(object_bytes[:8], (4).to_bytes(8, "little", signed=True))
            self.assertEqual(object_bytes[16:24], (9).to_bytes(8, "little", signed=True))
            self.assertEqual(sidecar, hashlib.sha256(object_bytes).digest())
            self.assertEqual(manifest["object_sha256"], hashlib.sha256(object_bytes).hexdigest())
            self.assertEqual(manifest["block_digest_sha256"], hashlib.sha256(sidecar).hexdigest())
            self.assertEqual(manifest["geometry"], {"rows": 2, "dimensions": 4})


if __name__ == "__main__":
    unittest.main()
