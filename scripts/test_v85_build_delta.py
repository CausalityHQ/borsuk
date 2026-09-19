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
    CompactionRequest,
    build_delta_artifacts,
    canonicalize_evaluation,
    compact_delta_artifacts,
    compute_exact_truth,
)

DIMENSIONS = 8


def _vector_type(dimensions: int) -> pa.DataType:
    return pa.list_(pa.field("element", pa.float32(), nullable=False), dimensions)


def _source_vector_type(dimensions: int) -> pa.DataType:
    return pa.list_(pa.field("item", pa.float32(), nullable=False), dimensions)


def _write_source(path: pathlib.Path) -> np.ndarray:
    vectors = np.zeros((100, DIMENSIONS), dtype=np.float32)
    for row in range(90):
        vectors[row, row % DIMENSIONS] = np.float32(1.0 + row / 100.0)
    vectors[90:] = np.float32(1_000_000.0)
    table = pa.Table.from_arrays(
        [
            pa.array(np.arange(100, dtype=np.uint64), type=pa.uint64()),
            pa.FixedSizeListArray.from_arrays(
                pa.array(vectors.reshape(-1), type=pa.float32()), DIMENSIONS
            ),
        ],
        schema=pa.schema(
            [
                pa.field("feature_row_id", pa.uint64(), nullable=False),
                pa.field("embedding", _source_vector_type(DIMENSIONS), nullable=False),
            ]
        ),
    )
    pq.write_table(table, path)
    return vectors


def _request(
    source: pathlib.Path, output: pathlib.Path, delta_runs: int
) -> BuildRequest:
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
            vectors = (
                table.column("vector")
                .combine_chunks()
                .values.to_numpy()
                .reshape(-1, DIMENSIONS)
            )
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


def _delta_rows(output: pathlib.Path) -> list[tuple[int, int, int, tuple[float, ...]]]:
    manifest = json.loads((output / "generation.json").read_bytes())
    rows = []
    for run in manifest["runs"]:
        if run["kind"] != "delta":
            continue
        path = output / pathlib.PurePosixPath(run["object"]["uri"]).name
        for page in run["pages"]:
            table = _read_page(path, page["offset"], page["bytes"])
            vectors = (
                table.column("vector")
                .combine_chunks()
                .values.to_numpy()
                .reshape(-1, DIMENSIONS)
            )
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
    def test_builder_uses_only_base_for_training_and_emits_strict_arrow_contracts(
        self,
    ) -> None:
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
            self.assertEqual(
                directory_table.column("id").to_pylist(), list(range(90, 100))
            )

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
                self.assertEqual(
                    hashlib.sha256(body).hexdigest(), run["object"]["sha256"]
                )
                self.assertEqual(len(body), run["object"]["bytes"])
                previous_end = 0
                for page in run["pages"]:
                    self.assertGreaterEqual(page["offset"], previous_end)
                    table = _read_page(parent, page["offset"], page["bytes"])
                    self.assertEqual(table.schema, page_schema)
                    self.assertEqual(table.num_rows, page["rows"])
                    previous_end = page["offset"] + page["bytes"]

    def test_one_and_ten_delta_runs_are_semantically_equivalent_and_deterministic(
        self,
    ) -> None:
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
            self.assertEqual(
                (ten_a / "generation.json").read_bytes(),
                (ten_b / "generation.json").read_bytes(),
            )
            self.assertEqual(
                (ten_a / "router.arrow").read_bytes(),
                (ten_b / "router.arrow").read_bytes(),
            )
            self.assertEqual(
                (ten_a / "mutations.arrow").read_bytes(),
                (ten_b / "mutations.arrow").read_bytes(),
            )
            for left in sorted(ten_a.glob("*.arrow")):
                self.assertEqual(left.read_bytes(), (ten_b / left.name).read_bytes())

    def test_compaction_is_permutation_independent_and_accounts_all_io(self) -> None:
        # Break caught: level-0 enumeration order changes the compacted bytes, drops
        # visible rows, or omits input/output bytes from amplification accounting.
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            source = root / "source.parquet"
            _write_source(source)
            level0 = root / "level0"
            build_delta_artifacts(_request(source, level0, 10))
            manifest = json.loads((level0 / "generation.json").read_bytes())
            run_ids = tuple(
                run["run_id"] for run in manifest["runs"] if run["kind"] == "delta"
            )
            expected_read_bytes = (
                (level0 / "generation.json").stat().st_size
                + 2 * (level0 / "mutations.arrow").stat().st_size
                + sum(
                    run["object"]["bytes"] + sum(page["bytes"] for page in run["pages"])
                    for run in manifest["runs"]
                    if run["kind"] == "delta"
                )
            )

            forward = root / "forward"
            reverse = root / "reverse"
            forward_receipt = compact_delta_artifacts(
                CompactionRequest(
                    generation=level0 / "generation.json",
                    output=forward,
                    uri_prefix="s3://fixture/v85-compacted",
                    delta_run_ids=run_ids,
                )
            )
            reverse_receipt = compact_delta_artifacts(
                CompactionRequest(
                    generation=level0 / "generation.json",
                    output=reverse,
                    uri_prefix="s3://fixture/v85-compacted",
                    delta_run_ids=tuple(reversed(run_ids)),
                )
            )

            self.assertEqual(_delta_rows(level0), _delta_rows(forward))
            self.assertEqual(
                (forward / "delta-l1.arrow").read_bytes(),
                (reverse / "delta-l1.arrow").read_bytes(),
            )
            self.assertEqual(
                (forward / "mutations.arrow").read_bytes(),
                (reverse / "mutations.arrow").read_bytes(),
            )
            self.assertEqual(
                (forward / "generation.json").read_bytes(),
                (reverse / "generation.json").read_bytes(),
            )
            self.assertEqual(
                (forward / "compaction-receipt.json").read_bytes(),
                (reverse / "compaction-receipt.json").read_bytes(),
            )
            self.assertEqual(forward_receipt, reverse_receipt)
            self.assertEqual(forward_receipt["input_runs"], 10)
            self.assertEqual(forward_receipt["output_runs"], 1)
            expected_read_bytes += (forward / "delta-l1.arrow").stat().st_size
            self.assertEqual(forward_receipt["read_bytes"], expected_read_bytes)
            self.assertEqual(
                forward_receipt["read_operations"],
                4
                + sum(
                    1 + len(run["pages"])
                    for run in manifest["runs"]
                    if run["kind"] == "delta"
                ),
            )
            self.assertEqual(forward_receipt["write_operations"], 4)
            self.assertEqual(
                forward_receipt["write_bytes"],
                sum(
                    (forward / name).stat().st_size
                    for name in (
                        "delta-l1.arrow",
                        "mutations.arrow",
                        "generation.json",
                        "compaction-receipt.json",
                    )
                ),
            )
            self.assertEqual(
                forward_receipt["amplification_ppm"],
                (forward_receipt["read_bytes"] + forward_receipt["write_bytes"])
                * 1_000_000
                // forward_receipt["logical_live_bytes"],
            )

    def test_compaction_keeps_latest_replacement_and_directory_tombstone(self) -> None:
        # Break caught: streaming compaction treats stale physical rows as authority
        # and resurrects a replaced or tombstoned ID.
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            source = root / "source.parquet"
            _write_source(source)
            level0 = root / "level0"
            build_delta_artifacts(_request(source, level0, 10))
            manifest_path = level0 / "generation.json"
            manifest = json.loads(manifest_path.read_bytes())
            delta_runs = [run for run in manifest["runs"] if run["kind"] == "delta"]
            replacement_run = delta_runs[1]
            replacement_path = (
                level0 / pathlib.PurePosixPath(replacement_run["object"]["uri"]).name
            )
            replacement_vector = np.full((1, DIMENSIONS), np.float32(42.0))
            replacement_table = pa.Table.from_arrays(
                [
                    pa.array([90], type=pa.int64()),
                    pa.array([3], type=pa.uint64()),
                    pa.array([0], type=pa.uint8()),
                    pa.FixedSizeListArray.from_arrays(
                        pa.array(replacement_vector.reshape(-1), type=pa.float32()),
                        DIMENSIONS,
                    ),
                ],
                schema=pa.schema(
                    [
                        pa.field("id", pa.int64(), nullable=False),
                        pa.field("sequence", pa.uint64(), nullable=False),
                        pa.field("state", pa.uint8(), nullable=False),
                        pa.field("vector", _vector_type(DIMENSIONS), nullable=False),
                    ]
                ),
            )
            sink = pa.BufferOutputStream()
            with ipc.new_stream(sink, replacement_table.schema) as writer:
                writer.write_table(replacement_table)
            replacement_body = sink.getvalue().to_pybytes()
            replacement_path.write_bytes(replacement_body)
            replacement_run["object"]["bytes"] = len(replacement_body)
            replacement_run["object"]["sha256"] = hashlib.sha256(
                replacement_body
            ).hexdigest()
            replacement_run["pages"][0]["bytes"] = len(replacement_body)
            replacement_run["pages"][0]["offset"] = 0
            replacement_run["pages"][0]["rows"] = 1

            mutation_path = level0 / "mutations.arrow"
            original_directory = ipc.open_file(mutation_path).read_all()
            ids = original_directory.column("id").to_pylist()
            replacement_row = ids.index(90)
            tombstone_row = ids.index(91)
            sequences = original_directory.column("sequence").to_pylist()
            states = original_directory.column("state").to_pylist()
            run_ids = original_directory.column("run_id").to_pylist()
            rows = original_directory.column("row").to_pylist()
            sequences[replacement_row] = 3
            run_ids[replacement_row] = replacement_run["run_id"]
            rows[replacement_row] = 0
            sequences[tombstone_row] = 3
            states[tombstone_row] = 1
            run_ids[tombstone_row] = None
            rows[tombstone_row] = None
            rewritten_directory = pa.Table.from_arrays(
                [
                    pa.array(ids, type=pa.int64()),
                    pa.array(sequences, type=pa.uint64()),
                    pa.array(states, type=pa.uint8()),
                    pa.array(run_ids, type=pa.uint32()),
                    pa.array(rows, type=pa.uint32()),
                ],
                schema=original_directory.schema,
            )
            sink = pa.BufferOutputStream()
            with ipc.new_file(sink, rewritten_directory.schema) as writer:
                writer.write_table(rewritten_directory)
            mutation_body = sink.getvalue().to_pybytes()
            mutation_path.write_bytes(mutation_body)
            manifest["mutation_directory"]["bytes"] = len(mutation_body)
            manifest["mutation_directory"]["sha256"] = hashlib.sha256(
                mutation_body
            ).hexdigest()
            manifest_path.write_bytes(
                json.dumps(manifest, separators=(",", ":"), sort_keys=True).encode()
                + b"\n"
            )

            output = root / "compacted"
            compact_delta_artifacts(
                CompactionRequest(
                    generation=manifest_path,
                    output=output,
                    uri_prefix="s3://fixture/v85-compacted",
                    delta_run_ids=tuple(run["run_id"] for run in delta_runs),
                )
            )
            rows = _delta_rows(output)
            self.assertEqual(
                [row[0] for row in rows], [90, 92, 93, 94, 95, 96, 97, 98, 99]
            )
            replacement = next(row for row in rows if row[0] == 90)
            self.assertEqual(replacement[1:3], (3, 0))
            self.assertEqual(replacement[3], (42.0,) * DIMENSIONS)

    def test_compaction_preserves_an_all_tombstone_generation(self) -> None:
        # Break caught: deleting every delta row makes compaction fail or invent a
        # dummy vector page instead of publishing a base-only generation.
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            source = root / "source.parquet"
            _write_source(source)
            level0 = root / "level0"
            build_delta_artifacts(_request(source, level0, 10))
            manifest_path = level0 / "generation.json"
            manifest = json.loads(manifest_path.read_bytes())
            mutation_path = level0 / "mutations.arrow"
            original = ipc.open_file(mutation_path).read_all()
            row_count = original.num_rows
            tombstones = pa.Table.from_arrays(
                [
                    original.column("id"),
                    pa.array([3] * row_count, type=pa.uint64()),
                    pa.array([1] * row_count, type=pa.uint8()),
                    pa.array([None] * row_count, type=pa.uint32()),
                    pa.array([None] * row_count, type=pa.uint32()),
                ],
                schema=original.schema,
            )
            sink = pa.BufferOutputStream()
            with ipc.new_file(sink, tombstones.schema) as writer:
                writer.write_table(tombstones)
            mutation_body = sink.getvalue().to_pybytes()
            mutation_path.write_bytes(mutation_body)
            manifest["mutation_directory"]["bytes"] = len(mutation_body)
            manifest["mutation_directory"]["sha256"] = hashlib.sha256(
                mutation_body
            ).hexdigest()
            manifest_path.write_bytes(
                json.dumps(manifest, separators=(",", ":"), sort_keys=True).encode()
                + b"\n"
            )

            output = root / "compacted"
            receipt = compact_delta_artifacts(
                CompactionRequest(
                    generation=manifest_path,
                    output=output,
                    uri_prefix="s3://fixture/v85-compacted",
                    delta_run_ids=tuple(
                        run["run_id"]
                        for run in manifest["runs"]
                        if run["kind"] == "delta"
                    ),
                )
            )
            compacted = json.loads((output / "generation.json").read_bytes())
            self.assertEqual(
                [run["kind"] for run in compacted["runs"]], ["base", "base"]
            )
            self.assertFalse((output / "delta-l1.arrow").exists())
            self.assertEqual(receipt["logical_live_bytes"], 0)
            self.assertIsNone(receipt["amplification_ppm"])
            self.assertEqual(receipt["output_runs"], 0)

    def test_compaction_rejects_nonfinite_vectors_and_invalid_page_authority(
        self,
    ) -> None:
        # Break caught: compaction turns an input rejected by the Rust reader into a
        # canonical-looking new generation.
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            source = root / "source.parquet"
            _write_source(source)
            level0 = root / "level0"
            build_delta_artifacts(_request(source, level0, 10))
            manifest_path = level0 / "generation.json"
            manifest = json.loads(manifest_path.read_bytes())
            first_delta = next(
                run for run in manifest["runs"] if run["kind"] == "delta"
            )
            page = first_delta["pages"][0]
            run_path = level0 / pathlib.PurePosixPath(first_delta["object"]["uri"]).name
            table = _read_page(run_path, page["offset"], page["bytes"])
            vector = np.full((1, DIMENSIONS), np.float32(np.nan))
            rewritten = pa.Table.from_arrays(
                [
                    table.column("id"),
                    table.column("sequence"),
                    table.column("state"),
                    pa.FixedSizeListArray.from_arrays(
                        pa.array(vector.reshape(-1), type=pa.float32()), DIMENSIONS
                    ),
                ],
                schema=table.schema,
            )
            sink = pa.BufferOutputStream()
            with ipc.new_stream(sink, rewritten.schema) as writer:
                writer.write_table(rewritten)
            body = sink.getvalue().to_pybytes()
            run_path.write_bytes(body)
            page["bytes"] = len(body)
            page["offset"] = 0
            first_delta["object"]["bytes"] = len(body)
            first_delta["object"]["sha256"] = hashlib.sha256(body).hexdigest()
            manifest_path.write_bytes(
                json.dumps(manifest, separators=(",", ":"), sort_keys=True).encode()
                + b"\n"
            )
            with self.assertRaisesRegex(ValueError, "non-finite"):
                compact_delta_artifacts(
                    CompactionRequest(
                        generation=manifest_path,
                        output=root / "nonfinite",
                        uri_prefix="s3://fixture/v85-compacted",
                        delta_run_ids=tuple(
                            run["run_id"]
                            for run in manifest["runs"]
                            if run["kind"] == "delta"
                        ),
                    )
                )

        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            source = root / "source.parquet"
            _write_source(source)
            level0 = root / "level0"
            build_delta_artifacts(_request(source, level0, 10))
            manifest_path = level0 / "generation.json"
            manifest = json.loads(manifest_path.read_bytes())
            manifest["page_rows"] = 0
            manifest_path.write_bytes(
                json.dumps(manifest, separators=(",", ":"), sort_keys=True).encode()
                + b"\n"
            )
            with self.assertRaisesRegex(ValueError, "generation authority"):
                compact_delta_artifacts(
                    CompactionRequest(
                        generation=manifest_path,
                        output=root / "invalid-page",
                        uri_prefix="s3://fixture/v85-compacted",
                        delta_run_ids=tuple(
                            run["run_id"]
                            for run in manifest["runs"]
                            if run["kind"] == "delta"
                        ),
                    )
                )

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
                [
                    pa.array(np.arange(3, dtype=np.uint32), type=pa.uint32()),
                    pa.array(np.arange(10, 13, dtype=np.uint64), type=pa.uint64()),
                    pa.FixedSizeListArray.from_arrays(
                        pa.array(query_vectors.reshape(-1)), DIMENSIONS
                    ),
                ],
                schema=pa.schema(
                    [
                        pa.field("query_ordinal", pa.uint32(), nullable=False),
                        pa.field("feature_row_id", pa.uint64(), nullable=False),
                        pa.field(
                            "embedding", _source_vector_type(DIMENSIONS), nullable=False
                        ),
                    ]
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
                            pa.list_(
                                pa.field("element", pa.int64(), nullable=False), 2
                            ),
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
                    [
                        pa.array(np.arange(2, dtype=np.uint32), type=pa.uint32()),
                        pa.array(
                            np.arange(100, 102, dtype=np.uint64), type=pa.uint64()
                        ),
                        pa.FixedSizeListArray.from_arrays(
                            pa.array(query_vectors.reshape(-1)), DIMENSIONS
                        ),
                    ],
                    schema=pa.schema(
                        [
                            pa.field("query_ordinal", pa.uint32(), nullable=False),
                            pa.field("feature_row_id", pa.uint64(), nullable=False),
                            pa.field(
                                "embedding",
                                _source_vector_type(DIMENSIONS),
                                nullable=False,
                            ),
                        ]
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
                expected.append(
                    sorted(range(20), key=lambda row: (distance[row], row))[:3]
                )
            self.assertEqual(actual, expected)


if __name__ == "__main__":
    unittest.main()
