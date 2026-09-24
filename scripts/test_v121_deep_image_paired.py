"""Cross-corpus paired replay preparation without query-trained settings."""

import json
import tempfile
import unittest
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from scripts.v121_deep_image_paired import prepare, rank_nominee_pages, reduce


class DeepImagePairedTests(unittest.TestCase):
    def test_prepares_unit_queries_from_publication_embedding_schema(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            vectors = np.zeros((2, 96), np.float32)
            vectors[0, 0] = 2.0
            vectors[1, 95] = 3.0
            query = root / "test.parquet"
            pq.write_table(pa.table({"emb": pa.FixedSizeListArray.from_arrays(
                pa.array(vectors.ravel()), 96,
            )}), query)
            requests = root / "queries.jsonl"
            prepare(query, requests, query_count=2)
            records = [json.loads(line) for line in requests.read_text().splitlines()]
            self.assertEqual([row["query_ordinal"] for row in records], [0, 1])
            self.assertEqual(records[0]["query"][0], 1.0)
            self.assertEqual(records[1]["query"][95], 1.0)

            vectors[1] = 0.0
            pq.write_table(pa.table({"emb": pa.FixedSizeListArray.from_arrays(
                pa.array(vectors.ravel()), 96,
            )}), query)
            with self.assertRaisesRegex(ValueError, "zero"):
                prepare(query, root / "invalid.jsonl", query_count=2)

    def test_control_ranks_pages_by_frozen_pq64_nominee_scores(self) -> None:
        query = np.zeros(96, np.float32)
        query[95] = 1.0
        books = np.zeros((64, 256, 2), np.float32)
        books[63, 1, 1] = 1.0
        codes = np.zeros((2, 64), np.uint8)
        codes[1, 63] = 1
        self.assertEqual(rank_nominee_pages(
            query, books, codes, nominees=[0, 1], page_rows=1,
        ), [1, 0])

    def test_reducer_recounts_paired_gt_hits_and_physical_caps(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            truth = root / "neighbors.parquet"
            values = np.tile(np.arange(100, dtype=np.int64), 1000)
            pq.write_table(pa.table({"neighbors_id":
                pa.FixedSizeListArray.from_arrays(pa.array(values), 100),
            }), truth)
            requests, replay = root / "requests.jsonl", root / "replay.jsonl"
            interval = [[0, 256 * 108]]
            with requests.open("w") as prepared, replay.open("w") as actual:
                for ordinal in range(1000):
                    request = {"query_ordinal": ordinal,
                               "nominees": list(range(512)),
                               "baseline_ranges": interval}
                    result = {"query_ordinal": ordinal,
                              "nominees": request["nominees"],
                              "baseline_ranges": interval,
                              "ranges": interval, "plan_bytes": 256 * 108,
                              "baseline_bytes": 256 * 108,
                              "returned_ids": list(range(100)),
                              "baseline_returned_ids": list(range(99)) + [100]}
                    prepared.write(json.dumps(request) + "\n")
                    actual.write(json.dumps(result) + "\n")
            summary = root / "summary.json"
            reduce(requests, replay, truth, root / "evidence.jsonl", summary)
            result = json.loads(summary.read_text())
            self.assertEqual(result["total_hits"],
                             {"candidate": 100_000, "baseline": 99_000})
            self.assertEqual(result["max_gets"], {"candidate": 1, "baseline": 1})
            self.assertTrue(result["qualifies_cross_corpus_quality"])


if __name__ == "__main__":
    unittest.main()
