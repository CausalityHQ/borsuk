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

from scripts.build_v30_reduced_truth import (
    _exact_distance_matrix,
    _normalize_like_v30,
    _shard_top_k,
    build_v30_reduced_truth,
    build_v32_streaming_prefix_truth,
)


def embeddings(rows: int, *, start: int = 0) -> bytes:
    values = np.zeros((rows, 96), dtype=np.float32)
    for row in range(rows):
        values[row, (start + row) % 96] = 1.0
    child = pa.field("element", pa.float32(), nullable=False)
    array = pa.FixedSizeListArray.from_arrays(
        pa.array(values.reshape(-1), type=pa.float32()), 96
    )
    table = pa.Table.from_arrays(
        [array],
        schema=pa.schema([pa.field("emb", pa.list_(child, 96), nullable=False)]),
    )
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink)
    return sink.getvalue().to_pybytes()


class V30ReducedTruthTests(unittest.TestCase):
    def test_exact_distance_matches_serving_dimension_order_at_top_ten_tie(self):
        # Break: pairwise reduction preserves tiny terms that sequential Rust
        # accumulation rounds away, changing which tied source enters top ten.
        query = np.zeros((1, 96), dtype=np.float32)
        query[0, 0] = 1
        corpus = np.repeat(query, 11, axis=0)
        corpus[:2] = 0
        corpus[:2, 1] = 1
        corpus[0, 2:] = 2**-27
        corpus = _normalize_like_v30(corpus)
        query = _normalize_like_v30(_normalize_like_v30(query))
        distances = _exact_distance_matrix(corpus, query)[:, 0]
        self.assertEqual([v.hex() for v in distances[:2]], ["0x1.0000000000000p+1"] * 2)
        _, ids = _shard_top_k(distances, 0, 10)
        self.assertEqual(ids.tolist(), [2, 3, 4, 5, 6, 7, 8, 9, 10, 0])

    def test_v32_prefix_truth_streams_authenticated_shards_without_page_inputs(
        self,
    ) -> None:
        shards = [embeddings(64, start=0), embeddings(64, start=64)]
        manifest = {
            "dataset_id": "deep-image-96",
            "schema_version": 1,
            "shards": [
                {
                    "encoded_bytes": len(payload),
                    "physical_row_count": 64,
                    "row_count": 64 if ordinal == 0 else 36,
                    "row_start": ordinal * 64,
                    "sha256": hashlib.sha256(payload).hexdigest(),
                    "uri": f"s3://frozen/train-{ordinal}.parquet",
                }
                for ordinal, payload in enumerate(shards)
            ],
            "source_rows": 100,
        }
        manifest_bytes = (
            json.dumps(manifest, separators=(",", ":"), sort_keys=True).encode() + b"\n"
        )
        query = embeddings(64)
        requested: list[str] = []

        def fetch(uri: str) -> bytes:
            requested.append(uri)
            return shards[int(uri.removesuffix(".parquet").rsplit("-", 1)[1])]

        truth, receipt = build_v32_streaming_prefix_truth(
            manifest_bytes,
            corpus_manifest_sha256=hashlib.sha256(manifest_bytes).hexdigest(),
            corpus_manifest_bytes=len(manifest_bytes),
            expected_source_rows=100,
            expected_shard_count=2,
            query_parquet=query,
            query_sha256=hashlib.sha256(query).hexdigest(),
            query_bytes=len(query),
            query_start=0,
            query_count=32,
            fetch=fetch,
        )
        self.assertEqual(
            requested,
            ["s3://frozen/train-0.parquet", "s3://frozen/train-1.parquet"],
        )
        rows = pq.read_table(pa.BufferReader(truth))["neighbors_id"].to_pylist()
        self.assertEqual(rows[0][:2], [0, 96])
        value = json.loads(receipt)
        self.assertEqual(value["source_rows"], 100)
        self.assertEqual(value["schema"], "borsuk-v32-prefix-truth-v3")
        self.assertEqual(value["query_start"], 0)
        self.assertEqual(value["query_count"], 32)
        self.assertEqual(value["corpus_manifest_bytes"], len(manifest_bytes))
        self.assertEqual(value["truth_row_semantics"], "window-relative")
        self.assertEqual(value["truth_id_space"], "source-ordinal")
        self.assertEqual(value["top_k"], 10)
        self.assertEqual(value["distance"], "squared-l2-f64-fixed-dimension-order")
        self.assertEqual(value["corpus_normalization"], "f64-l2-once-to-f32")
        self.assertEqual(value["query_normalization"], "f64-l2-twice-to-f32")
        self.assertEqual(value["tie_break"], "source-ordinal-ascending")
        self.assertEqual(value["shards_read"], 2)
        self.assertEqual(value["corpus_shards"], manifest["shards"])
        self.assertEqual(value["truth_sha256"], hashlib.sha256(truth).hexdigest())
        self.assertEqual(
            value["truth_ids_sha256"],
            hashlib.sha256(np.asarray(rows, dtype="<i8").tobytes()).hexdigest(),
        )
        self.assertGreater(value["rank_10_11_tie_queries"], 0)
        self.assertFalse(value["claim_eligible"])

    def test_v32_normalization_matches_the_frozen_v30_float_contract(self) -> None:
        values = np.zeros((2, 96), dtype=np.float32)
        values[0, 0] = np.float32(0.9999)
        generated = np.random.default_rng(1).normal(size=(365, 96)).astype(np.float32)
        values[1] = generated[364] * np.float32(
            1.0 / np.linalg.norm(generated[364].astype(np.float64))
        )
        once = _normalize_like_v30(values)
        twice = _normalize_like_v30(once)
        for source, normalized in zip(values, once, strict=True):
            norm = sum(float(value) * float(value) for value in source) ** 0.5
            expected = np.asarray(
                [float(value) / norm for value in source], dtype=np.float32
            )
            np.testing.assert_array_equal(normalized, expected)
        for source, normalized in zip(once, twice, strict=True):
            norm = sum(float(value) * float(value) for value in source) ** 0.5
            expected = np.asarray(
                [float(value) / norm for value in source], dtype=np.float32
            )
            np.testing.assert_array_equal(normalized, expected)
        self.assertFalse(np.array_equal(once[1], twice[1]))

    def test_v32_prefix_truth_matches_monolithic_ids_and_is_deterministic(self) -> None:
        shards = [embeddings(64, start=0), embeddings(64, start=64)]
        manifest = {
            "dataset_id": "deep-image-96",
            "schema_version": 1,
            "shards": [
                {
                    "encoded_bytes": len(payload),
                    "physical_row_count": 64,
                    "row_count": 64 if ordinal == 0 else 36,
                    "row_start": ordinal * 64,
                    "sha256": hashlib.sha256(payload).hexdigest(),
                    "uri": f"s3://frozen/train-{ordinal}.parquet",
                }
                for ordinal, payload in enumerate(shards)
            ],
            "source_rows": 100,
        }
        manifest_bytes = (
            json.dumps(manifest, separators=(",", ":"), sort_keys=True).encode() + b"\n"
        )
        corpus = embeddings(100)
        query = embeddings(64)
        arguments = dict(
            corpus_manifest_sha256=hashlib.sha256(manifest_bytes).hexdigest(),
            corpus_manifest_bytes=len(manifest_bytes),
            expected_source_rows=100,
            expected_shard_count=2,
            query_parquet=query,
            query_sha256=hashlib.sha256(query).hexdigest(),
            query_bytes=len(query),
            query_start=0,
            query_count=32,
            fetch=lambda uri: shards[
                int(uri.removesuffix(".parquet").rsplit("-", 1)[1])
            ],
        )
        first_truth, first_receipt = build_v32_streaming_prefix_truth(
            manifest_bytes, **arguments
        )
        second_truth, second_receipt = build_v32_streaming_prefix_truth(
            manifest_bytes, **arguments
        )
        monolithic_truth, _ = build_v30_reduced_truth(
            corpus,
            corpus_sha256=hashlib.sha256(corpus).hexdigest(),
            corpus_bytes=len(corpus),
            physical_rows=100,
            source_rows=100,
            query_parquet=query,
            query_sha256=hashlib.sha256(query).hexdigest(),
            query_bytes=len(query),
            query_start=0,
            query_count=32,
        )

        def ids(payload: bytes) -> list[list[int]]:
            return pq.read_table(pa.BufferReader(payload))["neighbors_id"].to_pylist()

        self.assertEqual(ids(first_truth), ids(monolithic_truth))
        self.assertEqual(first_truth, second_truth)
        self.assertEqual(first_receipt, second_receipt)

    def test_v32_prefix_truth_rejects_geometry_and_duplicate_shard_roles(self) -> None:
        shard = embeddings(64)
        manifest = {
            "dataset_id": "deep-image-96",
            "schema_version": 1,
            "shards": [
                {
                    "encoded_bytes": len(shard),
                    "physical_row_count": 64,
                    "row_count": 32,
                    "row_start": ordinal * 32,
                    "sha256": hashlib.sha256(shard).hexdigest(),
                    "uri": "s3://frozen/duplicate.parquet",
                }
                for ordinal in range(2)
            ],
            "source_rows": 64,
        }
        payload = (
            json.dumps(manifest, separators=(",", ":"), sort_keys=True).encode() + b"\n"
        )
        query = embeddings(64)
        arguments = dict(
            corpus_manifest_sha256=hashlib.sha256(payload).hexdigest(),
            corpus_manifest_bytes=len(payload),
            expected_source_rows=64,
            expected_shard_count=2,
            query_parquet=query,
            query_sha256=hashlib.sha256(query).hexdigest(),
            query_bytes=len(query),
            query_start=0,
            query_count=32,
            fetch=lambda _uri: shard,
        )
        with self.assertRaisesRegex(ValueError, "shard role authority"):
            build_v32_streaming_prefix_truth(payload, **arguments)
        unique = json.loads(payload)
        unique["shards"][1]["uri"] = "s3://frozen/other.parquet"
        unique_payload = (
            json.dumps(unique, separators=(",", ":"), sort_keys=True).encode() + b"\n"
        )
        arguments["corpus_manifest_sha256"] = hashlib.sha256(unique_payload).hexdigest()
        arguments["corpus_manifest_bytes"] = len(unique_payload)
        arguments["expected_source_rows"] = 65
        with self.assertRaisesRegex(ValueError, "expected geometry"):
            build_v32_streaming_prefix_truth(unique_payload, **arguments)

    def test_v32_prefix_truth_rejects_manifest_or_shard_authority_drift(self) -> None:
        shard = embeddings(64)
        manifest = {
            "dataset_id": "deep-image-96",
            "schema_version": 1,
            "shards": [
                {
                    "encoded_bytes": len(shard),
                    "physical_row_count": 64,
                    "row_count": 64,
                    "row_start": 0,
                    "sha256": "0" * 64,
                    "uri": "s3://frozen/train.parquet",
                }
            ],
            "source_rows": 64,
        }
        payload = (
            json.dumps(manifest, separators=(",", ":"), sort_keys=True).encode() + b"\n"
        )
        query = embeddings(64)
        with self.assertRaisesRegex(ValueError, "shard byte authority"):
            build_v32_streaming_prefix_truth(
                payload,
                corpus_manifest_sha256=hashlib.sha256(payload).hexdigest(),
                corpus_manifest_bytes=len(payload),
                expected_source_rows=64,
                expected_shard_count=1,
                query_parquet=query,
                query_sha256=hashlib.sha256(query).hexdigest(),
                query_bytes=len(query),
                query_start=0,
                query_count=32,
                fetch=lambda _uri: shard,
            )

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
                "--corpus-parquet",
                str(root / "corpus.parquet"),
                "--corpus-sha256",
                hashlib.sha256(corpus).hexdigest(),
                "--corpus-bytes",
                str(len(corpus)),
                "--physical-rows",
                "128",
                "--source-rows",
                "100",
                "--query-parquet",
                str(root / "query.parquet"),
                "--query-sha256",
                hashlib.sha256(query).hexdigest(),
                "--query-bytes",
                str(len(query)),
                "--query-start",
                "0",
                "--query-count",
                "32",
                "--truth-output",
                str(root / "truth.parquet"),
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
