import hashlib
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from scripts.build_v30_reduced_truth import build_v30_reduced_truth


def embeddings(rows: int) -> bytes:
    values = np.zeros((rows, 96), dtype=np.float32)
    for row in range(rows):
        values[row, row % 96] = 1.0
    child = pa.field("element", pa.float32(), nullable=False)
    array = pa.FixedSizeListArray.from_arrays(
        pa.array(values.reshape(-1), type=pa.float32()), 96
    )
    table = pa.Table.from_arrays(
        [array], schema=pa.schema([pa.field("emb", pa.list_(child, 96), nullable=False)])
    )
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink)
    return sink.getvalue().to_pybytes()


class V30ReducedTruthTests(unittest.TestCase):
    def test_v30_reduced_truth_direct_cli_writes_only_named_outputs(self) -> None:
        corpus = embeddings(128)
        query = embeddings(64)
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "corpus.parquet").write_bytes(corpus)
            (root / "query.parquet").write_bytes(query)
            command = [
                sys.executable,
                "scripts/build_v30_reduced_truth.py",
                "--execute",
                "--corpus-parquet", str(root / "corpus.parquet"),
                "--corpus-sha256", hashlib.sha256(corpus).hexdigest(),
                "--corpus-bytes", str(len(corpus)),
                "--physical-rows", "128",
                "--source-rows", "100",
                "--query-parquet", str(root / "query.parquet"),
                "--query-sha256", hashlib.sha256(query).hexdigest(),
                "--query-bytes", str(len(query)),
                "--query-start", "0",
                "--query-count", "32",
                "--truth-output", str(root / "truth.parquet"),
            ]
            completed = subprocess.run(command, check=False, capture_output=True)
            self.assertEqual(completed.returncode, 0, completed.stderr.decode())
            self.assertEqual(json.loads(completed.stdout)["status"], "passed")
            self.assertTrue((root / "truth.parquet").is_file())

    def test_v30_reduced_truth_authenticates_prefix_and_emits_parquet(self) -> None:
        corpus = embeddings(128)
        query = embeddings(64)
        truth, receipt = build_v30_reduced_truth(
            corpus,
            corpus_sha256=hashlib.sha256(corpus).hexdigest(),
            corpus_bytes=len(corpus),
            physical_rows=128,
            source_rows=100,
            query_parquet=query,
            query_sha256=hashlib.sha256(query).hexdigest(),
            query_bytes=len(query),
            query_start=0,
            query_count=32,
        )
        table = pq.read_table(pa.BufferReader(truth))
        self.assertEqual(table.num_rows, 32)
        self.assertEqual(table.schema.names, ["neighbors_id"])
        self.assertEqual(len(table["neighbors_id"][0].as_py()), 10)
        value = json.loads(receipt)
        self.assertEqual(value["source_rows"], 100)
        self.assertEqual(value["query_count"], 32)
        self.assertEqual(value["truth_sha256"], hashlib.sha256(truth).hexdigest())
        self.assertFalse(value["claim_eligible"])

    def test_v30_reduced_truth_rejects_byte_schema_and_norm_drift(self) -> None:
        corpus = embeddings(128)
        query = embeddings(64)
        common = dict(
            corpus_sha256=hashlib.sha256(corpus).hexdigest(),
            corpus_bytes=len(corpus),
            physical_rows=128,
            source_rows=100,
            query_parquet=query,
            query_sha256=hashlib.sha256(query).hexdigest(),
            query_bytes=len(query),
            query_start=0,
            query_count=32,
        )
        with self.assertRaisesRegex(ValueError, "corpus byte authority"):
            build_v30_reduced_truth(corpus + b"x", **common)
        nullable = pa.Table.from_arrays(
            [pa.array([[1.0] * 96] * 128, type=pa.list_(pa.float32(), 96))],
            names=["emb"],
        )
        sink = pa.BufferOutputStream()
        pq.write_table(nullable, sink)
        bad = sink.getvalue().to_pybytes()
        common["corpus_sha256"] = hashlib.sha256(bad).hexdigest()
        common["corpus_bytes"] = len(bad)
        with self.assertRaisesRegex(ValueError, "corpus Parquet schema"):
            build_v30_reduced_truth(bad, **common)

    def test_v30_reduced_truth_is_deterministic_and_tie_breaks_by_source(self) -> None:
        corpus = embeddings(128)
        query = embeddings(64)
        arguments = dict(
            corpus_sha256=hashlib.sha256(corpus).hexdigest(),
            corpus_bytes=len(corpus),
            physical_rows=128,
            source_rows=100,
            query_parquet=query,
            query_sha256=hashlib.sha256(query).hexdigest(),
            query_bytes=len(query),
            query_start=0,
            query_count=32,
        )
        first = build_v30_reduced_truth(corpus, **arguments)[0]
        second = build_v30_reduced_truth(corpus, **arguments)[0]
        self.assertEqual(first, second)
        rows = pq.read_table(pa.BufferReader(first))["neighbors_id"].to_pylist()
        self.assertEqual(rows[0][:2], [0, 96])


if __name__ == "__main__":
    unittest.main()
