from __future__ import annotations

import hashlib
import tempfile
import unittest
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from scripts.benchmark_s3_vectors_parquet import (
    BenchmarkConfig,
    ObjectIdentity,
    delete_service_resources,
    run_matched_benchmark,
)


def _identity(path: Path, role: str) -> ObjectIdentity:
    body = path.read_bytes()
    return ObjectIdentity(
        role=role,
        uri=f"s3://frozen/{path.name}",
        sha256=hashlib.sha256(body).hexdigest(),
        bytes=len(body),
    )


def _fixed_vectors(values: list[list[float]], dimensions: int) -> pa.Array:
    flat = pa.array(
        [component for row in values for component in row],
        type=pa.float32(),
    )
    return pa.FixedSizeListArray.from_arrays(
        flat,
        type=pa.list_(pa.field("item", pa.float32(), nullable=False), dimensions),
    )


class _FakeS3Vectors:
    class exceptions:
        class NotFoundException(Exception):
            pass

        class ConflictException(Exception):
            pass

    def __init__(self, answers: dict[int, list[str]]) -> None:
        self.answers = answers
        self.put_batches: list[list[dict[str, object]]] = []
        self.query_calls: list[dict[str, object]] = []
        self.deleted: list[str] = []

    def create_vector_bucket(self, **_: object) -> None:
        return None

    def create_index(self, **_: object) -> None:
        return None

    def get_index(self, **_: object) -> dict[str, object]:
        return {"index": {"status": "ACTIVE"}}

    def put_vectors(self, *, vectors: list[dict[str, object]], **_: object) -> None:
        self.put_batches.append(vectors)

    def query_vectors(self, **kwargs: object) -> dict[str, object]:
        self.query_calls.append(kwargs)
        vector = kwargs["queryVector"]
        assert isinstance(vector, dict)
        values = vector["float32"]
        assert isinstance(values, list)
        query = int(values[0])
        answer = self.answers[query]
        response: dict[str, object] = {
            "vectors": [
                {"key": key, "distance": float(position)}
                for position, key in enumerate(answer)
            ],
            "ResponseMetadata": {"HTTPHeaders": {"content-length": "123"}},
        }
        return response

    def delete_index(self, **_: object) -> None:
        self.deleted.append("index")

    def delete_vector_bucket(self, **_: object) -> None:
        self.deleted.append("bucket")


class MatchedS3VectorsParquetTests(unittest.TestCase):
    def test_cleanup_attempts_bucket_after_index_delete_failure(self) -> None:
        class FailingCleanupClient:
            def __init__(self) -> None:
                self.calls: list[str] = []

            def delete_index(self, **_: object) -> None:
                self.calls.append("index")
                raise RuntimeError("index deletion failed")

            def delete_vector_bucket(self, **_: object) -> None:
                self.calls.append("bucket")

        client = FailingCleanupClient()
        with self.assertRaisesRegex(RuntimeError, "index deletion failed"):
            delete_service_resources(client, "bucket", "index")
        self.assertEqual(client.calls, ["index", "bucket"])

    def test_streams_feature_ids_and_recomputable_two_pass_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source.parquet"
            queries = root / "queries.parquet"
            truth = root / "truth.parquet"
            output = root / "output"

            source_ids = list(range(10_000, 11_001))
            pq.write_table(
                pa.Table.from_arrays(
                    [
                        pa.array(source_ids, type=pa.uint64()),
                        _fixed_vectors(
                            [[float(row), -float(row)] for row in range(1_001)], 2
                        ),
                    ],
                    schema=pa.schema(
                        [
                            pa.field("feature_row_id", pa.uint64(), nullable=False),
                            pa.field(
                                "embedding",
                                pa.list_(
                                    pa.field("item", pa.float32(), nullable=False), 2
                                ),
                                nullable=False,
                            ),
                        ]
                    ),
                ),
                source,
            )
            pq.write_table(
                pa.Table.from_arrays(
                    [
                        pa.array([0, 1], type=pa.uint32()),
                        pa.array([90_000, 90_001], type=pa.uint64()),
                        _fixed_vectors([[0.0, 1.0], [1.0, 0.0]], 2),
                    ],
                    schema=pa.schema(
                        [
                            pa.field("query_ordinal", pa.uint32(), nullable=False),
                            pa.field("feature_row_id", pa.uint64(), nullable=False),
                            pa.field(
                                "embedding",
                                pa.list_(
                                    pa.field("item", pa.float32(), nullable=False), 2
                                ),
                                nullable=False,
                            ),
                        ]
                    ),
                ),
                queries,
            )
            truth_ids = [source_ids[:100], source_ids[100:200]]
            pq.write_table(
                pa.Table.from_arrays(
                    [
                        pa.array([0] * 100 + [1] * 100, type=pa.uint32()),
                        pa.array(list(range(100)) * 2, type=pa.uint16()),
                        pa.array(truth_ids[0] + truth_ids[1], type=pa.uint64()),
                        pa.array(
                            [float(rank) for rank in range(100)] * 2,
                            type=pa.float64(),
                        ),
                    ],
                    schema=pa.schema(
                        [
                            pa.field("query_ordinal", pa.uint32(), nullable=False),
                            pa.field("rank", pa.uint16(), nullable=False),
                            pa.field("feature_row_id", pa.uint64(), nullable=False),
                            pa.field("squared_distance", pa.float64(), nullable=False),
                        ]
                    ),
                ),
                truth,
            )

            first_answer = [str(value) for value in truth_ids[0]]
            first_answer[0] = str(source_ids[500])
            fake = _FakeS3Vectors(
                {
                    0: first_answer,
                    1: [str(value) for value in truth_ids[1]],
                }
            )
            config = BenchmarkConfig(
                source=source,
                queries=queries,
                truth=truth,
                output_dir=output,
                inputs={
                    role: _identity(path, role)
                    for role, path in (
                        ("source", source),
                        ("queries", queries),
                        ("truth", truth),
                    )
                },
                vector_bucket="borsuk-matched-test",
                index_name="vectors",
                dimensions=2,
                source_rows=1_001,
                query_count=2,
                neighbors=100,
                metric="euclidean",
                upload_workers=2,
                query_seed=17,
                source_commit="1" * 40,
            )

            result = run_matched_benchmark(config, fake)

            self.assertEqual([len(batch) for batch in fake.put_batches], [500, 500, 1])
            self.assertEqual(fake.put_batches[0][0]["key"], "10000")
            self.assertEqual(fake.deleted, ["index", "bucket"])
            self.assertEqual(len(fake.query_calls), 4)
            self.assertEqual(
                [row.label for row in result.passes],
                ["fresh_index_first_pass", "immediate_repeated_pass"],
            )
            self.assertEqual(
                [row.average_recall10_ppm for row in result.passes],
                [950_000, 950_000],
            )
            self.assertEqual(
                [row.average_recall100_ppm for row in result.passes],
                [995_000, 995_000],
            )
            samples = pq.read_table(output / "samples.parquet").to_pylist()
            self.assertEqual(len(samples), 4)
            query_zero = next(
                row
                for row in samples
                if row["pass_label"] == "fresh_index_first_pass"
                and row["query_ordinal"] == 0
            )
            self.assertEqual(query_zero["response_bytes"], 123)
            self.assertEqual(query_zero["pages"], 1)
            self.assertEqual(query_zero["recall10_ppm"], 900_000)
            self.assertEqual(query_zero["recall100_ppm"], 990_000)
            self.assertEqual(
                query_zero["returned_feature_row_ids"],
                [int(value) for value in first_answer],
            )
            self.assertTrue((output / "result.json").read_bytes().endswith(b"\n"))

    def test_rejects_changed_authenticated_input_before_service_mutation(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source.parquet"
            source.write_bytes(b"changed")
            missing = root / "missing.parquet"
            fake = _FakeS3Vectors({})
            config = BenchmarkConfig(
                source=source,
                queries=missing,
                truth=missing,
                output_dir=root / "output",
                inputs={
                    "source": ObjectIdentity(
                        role="source",
                        uri="s3://frozen/source.parquet",
                        sha256="0" * 64,
                        bytes=7,
                    ),
                    "queries": ObjectIdentity(
                        role="queries",
                        uri="s3://frozen/queries.parquet",
                        sha256="1" * 64,
                        bytes=1,
                    ),
                    "truth": ObjectIdentity(
                        role="truth",
                        uri="s3://frozen/truth.parquet",
                        sha256="2" * 64,
                        bytes=1,
                    ),
                },
                vector_bucket="borsuk-matched-test",
                index_name="vectors",
                dimensions=2,
                source_rows=1,
                query_count=1,
                neighbors=100,
                metric="euclidean",
                upload_workers=1,
                query_seed=17,
                source_commit="1" * 40,
            )

            with self.assertRaisesRegex(ValueError, "source identity differs"):
                run_matched_benchmark(config, fake)
            self.assertEqual(fake.put_batches, [])
            self.assertEqual(fake.deleted, [])


if __name__ == "__main__":
    unittest.main()
