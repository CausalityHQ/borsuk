import dataclasses
import hashlib
import json
import pathlib
import tempfile
import unittest

import numpy as np
import pyarrow as pa
import pyarrow.ipc as ipc
import pyarrow.parquet as pq

from scripts.v85_build_delta import (
    BuildRequest,
    build_delta_artifacts,
    canonicalize_evaluation,
    compute_exact_truth,
)

DIMENSIONS = 8


def _vector_type(dimensions: int) -> pa.DataType:
    return pa.list_(pa.field("element", pa.float32(), nullable=False), dimensions)


def _write_source(path: pathlib.Path) -> np.ndarray:
    vectors = np.zeros((100, DIMENSIONS), dtype=np.float32)
    for row in range(90):
        vectors[row, row % DIMENSIONS] = np.float32(1.0 + row / 100.0)
    vectors[90:] = np.float32(1_000_000.0)
    table = pa.Table.from_arrays(
        [
            pa.FixedSizeListArray.from_arrays(
                pa.array(vectors.reshape(-1), type=pa.float32()), DIMENSIONS
            ),
        ],
        schema=pa.schema(
            [
                pa.field("embedding", _vector_type(DIMENSIONS), nullable=False),
            ]
        ),
    )
    pq.write_table(table, path)
    return vectors


def _request(source: pathlib.Path, output: pathlib.Path, delta_runs: int) -> BuildRequest:
    return BuildRequest(
        source=source,
        output=output,
        uri_prefix="s3://fixture/v85",
        base_rows=90,
        delta_rows=10,
        dimensions=DIMENSIONS,
        page_rows=8,
        router_cells=4,
        base_runs=2,
        delta_runs=delta_runs,
        seed=85,
    )


def _read_page(path: pathlib.Path, offset: int, length: int) -> pa.Table:
    body = path.read_bytes()[offset : offset + length]
    return ipc.open_stream(pa.py_buffer(body)).read_all()


def _all_rows(output: pathlib.Path) -> list[tuple[int, int, int, tuple[float, ...]]]:
    manifest = json.loads((output / "generation.json").read_bytes())
    rows = []
    for run in manifest["runs"]:
        path = output / pathlib.PurePosixPath(run["object"]["uri"]).name
        for page in run["pages"]:
            table = _read_page(path, page["offset"], page["bytes"])
            vectors = table.column("vector").combine_chunks().values.to_numpy().reshape(-1, DIMENSIONS)
            for index, row_id in enumerate(table.column("id").to_pylist()):
                rows.append(
                    (
                        row_id,
                        table.column("sequence")[index].as_py(),
                        table.column("state")[index].as_py(),
                        tuple(float(value) for value in vectors[index]),
                    )
                )
    return sorted(rows)


class V85BuildDeltaTests(unittest.TestCase):
    def test_builder_uses_only_base_for_training_and_emits_strict_arrow_contracts(self) -> None:
        # Break caught: delta/query evidence leaks into training or language-specific
        # bytes become part of the serving boundary.
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            source = root / "source.parquet"
            vectors = _write_source(source)
            output = root / "one"
            receipt = build_delta_artifacts(_request(source, output, 1))

            self.assertEqual(
                {field.name for field in dataclasses.fields(BuildRequest)},
                {
                    "source",
                    "output",
                    "uri_prefix",
                    "base_rows",
                    "delta_rows",
                    "dimensions",
                    "page_rows",
                    "router_cells",
                    "base_runs",
                    "delta_runs",
                    "seed",
                },
            )
            expected_training_sha = hashlib.sha256(
                np.ascontiguousarray(vectors[:90]).tobytes()
            ).hexdigest()
            self.assertEqual(receipt["training_rows"], 90)
            self.assertEqual(receipt["training_sha256"], expected_training_sha)

            router = ipc.open_file(output / "router.arrow").read_all()
            self.assertEqual(
                router.schema,
                pa.schema(
                    [
                        pa.field("cell", pa.uint32(), nullable=False),
                        pa.field("centroid", _vector_type(DIMENSIONS), nullable=False),
                        pa.field("first_page", pa.uint32(), nullable=False),
                        pa.field("page_count", pa.uint32(), nullable=False),
                    ]
                ),
            )
            centroids = router.column("centroid").combine_chunks().values.to_numpy()
            self.assertLess(float(np.max(np.abs(centroids))), 10.0)

            directory_table = ipc.open_file(output / "mutations.arrow").read_all()
            self.assertEqual(
                directory_table.schema,
                pa.schema(
                    [
                        pa.field("id", pa.int64(), nullable=False),
                        pa.field("sequence", pa.uint64(), nullable=False),
                        pa.field("state", pa.uint8(), nullable=False),
                        pa.field("run_id", pa.uint32(), nullable=True),
                        pa.field("row", pa.uint32(), nullable=True),
                    ]
                ),
            )
            self.assertEqual(directory_table.column("id").to_pylist(), list(range(90, 100)))

            manifest_bytes = (output / "generation.json").read_bytes()
            self.assertTrue(manifest_bytes.endswith(b"\n"))
            self.assertFalse(manifest_bytes.endswith(b"\n\n"))
            manifest = json.loads(manifest_bytes)
            self.assertEqual(manifest["source_split"], "relaion-100-base90-delta10")
            self.assertEqual(len(manifest["runs"]), 3)
            page_schema = pa.schema(
                [
                    pa.field("id", pa.int64(), nullable=False),
                    pa.field("sequence", pa.uint64(), nullable=False),
                    pa.field("state", pa.uint8(), nullable=False),
                    pa.field("vector", _vector_type(DIMENSIONS), nullable=False),
                ]
            )
            for run in manifest["runs"]:
                parent = output / pathlib.PurePosixPath(run["object"]["uri"]).name
                body = parent.read_bytes()
                self.assertEqual(hashlib.sha256(body).hexdigest(), run["object"]["sha256"])
                self.assertEqual(len(body), run["object"]["bytes"])
                previous_end = 0
                for page in run["pages"]:
                    self.assertGreaterEqual(page["offset"], previous_end)
                    table = _read_page(parent, page["offset"], page["bytes"])
                    self.assertEqual(table.schema, page_schema)
                    self.assertEqual(table.num_rows, page["rows"])
                    previous_end = page["offset"] + page["bytes"]

    def test_one_and_ten_delta_runs_are_semantically_equivalent_and_deterministic(self) -> None:
        # Break caught: run-count tuning changes visible rows, ordering, hashes, or
        # stable page ranges instead of only changing immutable run grouping.
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            source = root / "source.parquet"
            _write_source(source)
            one = root / "one"
            ten_a = root / "ten-a"
            ten_b = root / "ten-b"
            build_delta_artifacts(_request(source, one, 1))
            build_delta_artifacts(_request(source, ten_a, 10))
            build_delta_artifacts(_request(source, ten_b, 10))

            self.assertEqual(_all_rows(one), _all_rows(ten_a))
            self.assertEqual(_all_rows(ten_a), _all_rows(ten_b))
            self.assertEqual((ten_a / "generation.json").read_bytes(), (ten_b / "generation.json").read_bytes())
            self.assertEqual((ten_a / "router.arrow").read_bytes(), (ten_b / "router.arrow").read_bytes())
            self.assertEqual((ten_a / "mutations.arrow").read_bytes(), (ten_b / "mutations.arrow").read_bytes())
            for left in sorted(ten_a.glob("*.arrow")):
                self.assertEqual(left.read_bytes(), (ten_b / left.name).read_bytes())

    def test_training_request_has_no_query_or_ground_truth_capability(self) -> None:
        # Break caught: the builder acquires evaluation paths that can silently tune
        # the router on queries or ground truth.
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            source = root / "source.parquet"
            _write_source(source)
            values = dataclasses.asdict(_request(source, root / "out", 1))
            values["query_path"] = root / "queries.parquet"
            with self.assertRaises(TypeError):
                BuildRequest(**values)
            values.pop("query_path")
            values["ground_truth_path"] = root / "neighbors.parquet"
            with self.assertRaises(TypeError):
                BuildRequest(**values)

    def test_evaluation_adapter_emits_canonical_query_and_truth_parquet(self) -> None:
        # Break caught: the frozen flat ReLAION evaluation files are consumed with
        # implicit shape/order assumptions rather than one strict cross-language schema.
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            query_vectors = np.eye(DIMENSIONS, dtype=np.float32)[:3]
            query_table = pa.Table.from_arrays(
                [pa.FixedSizeListArray.from_arrays(pa.array(query_vectors.reshape(-1)), DIMENSIONS)],
                schema=pa.schema(
                    [pa.field("embedding", _vector_type(DIMENSIONS), nullable=False)]
                ),
            )
            query_source = root / "development-query.parquet"
            pq.write_table(query_table, query_source)
            truth_source = root / "development-gt.parquet"
            pq.write_table(
                pa.table(
                    {"feature_row_id": pa.array([0, 1, 2, 3, 4, 5], type=pa.uint64())}
                ),
                truth_source,
            )
            query_output = root / "queries.parquet"
            truth_output = root / "truth.parquet"
            canonicalize_evaluation(
                query_source,
                truth_source,
                query_output,
                truth_output,
                dimensions=DIMENSIONS,
                neighbors=2,
                query_limit=3,
            )
            self.assertEqual(
                pq.read_table(query_output).schema,
                pa.schema(
                    [
                        pa.field("query", pa.uint32(), nullable=False),
                        pa.field("vector", _vector_type(DIMENSIONS), nullable=False),
                    ]
                ),
            )
            self.assertEqual(
                pq.read_table(truth_output).schema,
                pa.schema(
                    [
                        pa.field("query", pa.uint32(), nullable=False),
                        pa.field(
                            "neighbors",
                            pa.list_(pa.field("element", pa.int64(), nullable=False), 2),
                            nullable=False,
                        ),
                    ]
                ),
            )
            self.assertEqual(
                pq.read_table(truth_output).column("neighbors").to_pylist(),
                [[0, 1], [2, 3], [4, 5]],
            )

    def test_exact_truth_is_recomputed_for_the_screen_subset(self) -> None:
        # Break caught: the 1M GT is reused for a 100k subset even though many
        # registered neighbours are outside that subset.
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            source = root / "source.parquet"
            vectors = _write_source(source)[:20]
            query_vectors = np.stack([vectors[3], vectors[11]]).astype(np.float32)
            query_source = root / "queries-source.parquet"
            pq.write_table(
                pa.Table.from_arrays(
                    [pa.FixedSizeListArray.from_arrays(pa.array(query_vectors.reshape(-1)), DIMENSIONS)],
                    schema=pa.schema(
                        [pa.field("embedding", _vector_type(DIMENSIONS), nullable=False)]
                    ),
                ),
                query_source,
            )
            truth_output = root / "truth.parquet"
            compute_exact_truth(
                source,
                query_source,
                truth_output,
                dimensions=DIMENSIONS,
                corpus_rows=20,
                neighbors=3,
                query_limit=2,
            )
            actual = pq.read_table(truth_output).column("neighbors").to_pylist()
            expected = []
            for query in query_vectors:
                distance = np.sum((vectors - query) ** 2, axis=1)
                expected.append(sorted(range(20), key=lambda row: (distance[row], row))[:3])
            self.assertEqual(actual, expected)


if __name__ == "__main__":
    unittest.main()
