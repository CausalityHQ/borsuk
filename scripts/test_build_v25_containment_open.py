import hashlib
import json
import pathlib
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

import scripts.build_v25_containment_open as subject
from scripts.build_v25_containment_open import (
    RegisteredV24Input,
    V25OpenBuildRequest,
    build_v25_open_inputs,
    select_v25_open_rows,
)


def _identity(role: str, path: pathlib.Path, generation: str) -> RegisteredV24Input:
    payload = path.read_bytes()
    return RegisteredV24Input(
        role=role,
        path=path,
        uri=f"s3://borsuk-v24/{path.name}",
        sha256=hashlib.sha256(payload).hexdigest(),
        encoded_bytes=len(payload),
        generation=generation,
    )


def _vector_array(values: np.ndarray) -> pa.FixedSizeListArray:
    return pa.FixedSizeListArray.from_arrays(
        pa.array(values.reshape(-1), type=pa.float32()),
        type=pa.list_(pa.field("element", pa.float32(), nullable=False), 96),
    )


def _vectors(rows: int) -> pa.FixedSizeListArray:
    values = np.zeros((rows, 96), dtype=np.float32)
    values[:, 0] = 1.0
    return _vector_array(values)


def _write_v24_fixture(
    root: pathlib.Path,
    rows: int,
    *,
    with_replica: bool = False,
    replica_vector_drift: bool = False,
    page_modulo: int | None = None,
    f16_projection_probe: bool = False,
) -> tuple[RegisteredV24Input, RegisteredV24Input]:
    generation = "v24-full-fixture"
    construction_path = root / "construction-rows.parquet"
    construction_schema = pa.schema(
        [
            pa.field("source_ordinal", pa.uint64(), nullable=False),
            pa.field(
                "vector",
                pa.list_(pa.field("element", pa.float32(), nullable=False), 96),
                nullable=False,
            ),
        ]
    )
    construction_values = np.zeros((rows, 96), dtype=np.float32)
    construction_values[:, 0] = 1.0
    if f16_projection_probe:
        construction_values[0, :2] = [1.0, 0.1]
    pq.write_table(
        pa.Table.from_arrays(
            [
                pa.array(range(rows), type=pa.uint64()),
                _vector_array(construction_values),
            ],
            schema=construction_schema,
        ),
        construction_path,
        compression="zstd",
        row_group_size=5,
        version="2.6",
    )
    construction = _identity(
        "construction-rows-parquet", construction_path, generation
    )
    page_path = root / "page-rows.parquet"
    page_schema = pa.schema(
        [
            pa.field("page_ordinal", pa.uint32(), nullable=False),
            pa.field("replica", pa.bool_(), nullable=False),
            pa.field("record_id", pa.string(), nullable=False),
            pa.field(
                "vector",
                pa.list_(pa.field("element", pa.float32(), nullable=False), 96),
                nullable=False,
            ),
        ],
        metadata={
            b"construction_rows_sha256": construction.sha256.encode(),
            b"generation": generation.encode(),
        },
    )
    page_ordinals = [
        value if page_modulo is None else value % page_modulo
        for value in range(rows)
    ]
    replicas = [False] * rows
    record_ids = [str(value) for value in range(rows)]
    page_values = construction_values.astype(np.float16).astype(np.float32)
    if f16_projection_probe:
        page_values[0, :2] = np.asarray(
            [1.0 / np.sqrt(1.01), 0.1 / np.sqrt(1.01)], dtype=np.float16
        ).astype(np.float32)
    if with_replica or replica_vector_drift:
        page_ordinals.append(1)
        replicas.append(True)
        record_ids.append("0")
        replica = np.zeros((1, 96), dtype=np.float32)
        replica[0, 1 if replica_vector_drift else 0] = 1.0
        page_values = np.concatenate((page_values, replica))
    pq.write_table(
        pa.Table.from_arrays(
            [
                pa.array(page_ordinals, type=pa.uint32()),
                pa.array(replicas, type=pa.bool_()),
                pa.array(record_ids),
                _vector_array(page_values),
            ],
            schema=page_schema,
        ),
        page_path,
        compression="zstd",
        row_group_size=3,
        version="2.6",
    )
    return construction, _identity("page-rows-parquet", page_path, generation)


def _request(
    construction: RegisteredV24Input,
    page_rows: RegisteredV24Input,
    output: pathlib.Path,
    *,
    page_count: int = 16,
) -> V25OpenBuildRequest:
    return V25OpenBuildRequest(
        construction=construction,
        page_rows=page_rows,
        output_dir=output,
        output_uri_prefix="s3://borsuk-v25/open-fixture/",
        output_generation="v25-open-fixture",
        source_row_count=16,
        cohort_count=12,
        pseudoquery_count=1,
        page_count=page_count,
        cohort_seed=0x243F6A8885A308D3,
        pseudoquery_seed=0x13198A2E03707344,
        output_row_group_size=4,
    )


class V25OpenSelectionTests(unittest.TestCase):
    def test_v25_open_selection_is_splitmix_ranked_dense_and_query_independent(self) -> None:
        selection = select_v25_open_rows(
            source_row_count=12,
            cohort_count=6,
            pseudoquery_count=2,
            cohort_seed=0x243F6A8885A308D3,
            pseudoquery_seed=0x13198A2E03707344,
        )

        self.assertEqual(selection.dataset_ordinals, (4, 8, 0, 9, 11, 5))
        self.assertEqual(selection.query_source_ordinals, (4, 5))
        self.assertEqual(
            selection.dataset_ordinals_sha256,
            "7410ab23ed0919aa974bdd47d941cd2ad358d1218b50852b5d152b7515375b2a",
        )
        ranked_queries = select_v25_open_rows(
            source_row_count=32,
            cohort_count=16,
            pseudoquery_count=5,
            cohort_seed=0x243F6A8885A308D3,
            pseudoquery_seed=0x13198A2E03707344,
        )
        self.assertEqual(ranked_queries.query_source_ordinals, (3, 15, 8, 2, 6))

        with self.assertRaisesRegex(ValueError, "V25 open split authority differs"):
            select_v25_open_rows(
                source_row_count=6,
                cohort_count=6,
                pseudoquery_count=6,
                cohort_seed=1,
                pseudoquery_seed=1,
            )


class V25OpenConversionTests(unittest.TestCase):
    def test_v25_open_conversion_remaps_physical_pages_and_builds_exact_truth(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            construction, page_rows = _write_v24_fixture(root, 16)

            output = root / "v25"
            receipt = build_v25_open_inputs(_request(construction, page_rows, output))

            self.assertEqual(receipt.selection.dataset_ordinals, (4, 8, 0, 15, 9, 12, 11, 5, 13, 7, 10, 14))
            source_map = pq.read_table(output / "source-map.parquet").to_pydict()
            self.assertEqual(source_map["source_ordinal"], list(range(12)))
            self.assertEqual(
                source_map["dataset_ordinal"],
                [4, 8, 0, 15, 9, 12, 11, 5, 13, 7, 10, 14],
            )
            pages = pq.read_table(output / "page-assignments.parquet").to_pydict()
            self.assertEqual(pages["primary_page"], [4, 8, 0, 15, 9, 12, 11, 5, 13, 7, 10, 14])
            self.assertEqual(pages["replica_page"], [2**32 - 1] * 12)
            queries = pq.read_table(output / "pseudoqueries.parquet").to_pydict()
            self.assertEqual(queries["query_ordinal"], [0])
            self.assertEqual(queries["source_ordinal"], [6])
            truth = pq.read_table(output / "truth.parquet").to_pydict()
            self.assertEqual(
                truth["neighbor_source_ordinals"][0],
                [0, 1, 2, 3, 4, 5, 7, 8, 9, 10],
            )
            self.assertEqual(truth["oracle_pages"][0], [0, 4, 5, 7, 8, 9, 10, 12])
            self.assertEqual(set(receipt.outputs), {
                "construction-rows-parquet",
                "page-assignments-parquet",
                "pseudoqueries-parquet",
                "source-map-parquet",
                "truth-parquet",
            })
            for identity in receipt.outputs.values():
                self.assertEqual(identity.digest_algorithm, "sha256")
                self.assertEqual(len(identity.digest), 64)
                self.assertGreater(identity.encoded_bytes, 0)

    def test_v25_open_conversion_rejects_reauthenticated_replica_vector_drift(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            construction, page_rows = _write_v24_fixture(
                root, 16, with_replica=True
            )
            build_v25_open_inputs(_request(construction, page_rows, root / "accepted"))
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            construction, page_rows = _write_v24_fixture(
                root, 16, replica_vector_drift=True
            )
            output = root / "rejected"
            with self.assertRaisesRegex(ValueError, "V25 open replica vector differs"):
                build_v25_open_inputs(_request(construction, page_rows, output))
            self.assertFalse(output.exists())

    def test_v25_open_conversion_rejects_noncanonical_page_record_ids(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            construction, page_rows = _write_v24_fixture(root, 16)
            table = pq.read_table(page_rows.path)
            record_ids = table.column("record_id").to_pylist()
            record_ids[0] = "00"
            changed = table.set_column(
                2,
                table.schema.field(2),
                pa.array(record_ids, type=pa.string()),
            )
            pq.write_table(
                changed,
                page_rows.path,
                compression="zstd",
                row_group_size=3,
                version="2.6",
            )
            page_rows = _identity(
                "page-rows-parquet", page_rows.path, page_rows.generation
            )
            output = root / "rejected"
            with self.assertRaisesRegex(ValueError, "V25 open page record ID differs"):
                build_v25_open_inputs(_request(construction, page_rows, output))
            self.assertFalse(output.exists())

    def test_v25_open_conversion_rejects_reauthenticated_page_metadata_drift(self) -> None:
        """A changed V24 construction binding must fail before page semantics run."""
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            construction, page_rows = _write_v24_fixture(root, 16)
            table = pq.read_table(page_rows.path).replace_schema_metadata(
                {
                    b"construction_rows_sha256": b"0" * 64,
                    b"generation": page_rows.generation.encode(),
                }
            )
            pq.write_table(table, page_rows.path, compression="zstd", version="2.6")
            changed = _identity(
                "page-rows-parquet", page_rows.path, page_rows.generation
            )

            with self.assertRaisesRegex(ValueError, "V25 open page schema differs"):
                build_v25_open_inputs(_request(construction, changed, root / "output"))

    def test_v25_open_conversion_rejects_primary_vector_drift_from_construction(self) -> None:
        """A selected page vector must be byte-equal to its construction vector."""
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            construction, page_rows = _write_v24_fixture(root, 16)
            table = pq.read_table(page_rows.path)
            vectors = np.asarray(table.column("vector").to_pylist(), dtype=np.float32)
            vectors[0] = 0.0
            vectors[0, 1] = 1.0
            changed_table = table.set_column(
                3, table.schema.field(3), _vector_array(vectors)
            )
            pq.write_table(
                changed_table, page_rows.path, compression="zstd", version="2.6"
            )
            changed = _identity(
                "page-rows-parquet", page_rows.path, page_rows.generation
            )

            with self.assertRaisesRegex(
                ValueError, "V25 open page construction vector differs"
            ):
                build_v25_open_inputs(_request(construction, changed, root / "output"))

    def test_v25_open_conversion_accepts_exact_f16_page_projection(self) -> None:
        """V24 pages bind to normalized-then-f16 construction vectors."""
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            construction, page_rows = _write_v24_fixture(
                root, 16, f16_projection_probe=True
            )

            build_v25_open_inputs(_request(construction, page_rows, root / "output"))

    def test_v25_open_conversion_pads_short_oracle_without_losing_cardinality(self) -> None:
        """A valid sub-eight oracle must serialize with explicit sentinel padding."""
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            construction, page_rows = _write_v24_fixture(root, 16, page_modulo=7)
            output = root / "output"

            build_v25_open_inputs(
                _request(construction, page_rows, output, page_count=7)
            )

            oracle = pq.read_table(output / "truth.parquet").column(
                "oracle_pages"
            )[0].as_py()
            self.assertEqual(len(oracle), 8)
            self.assertEqual(oracle[-2:], [2**32 - 1, 2**32 - 1])

    def test_v25_open_truth_uses_fixed_order_float64_reduction(self) -> None:
        """Changing truth scoring back to backend BLAS must change this authority."""
        query = np.asarray(([1.0, 2**-26, -1.0, 2**-26] * 24), dtype=np.float64)
        vectors = np.stack(
            [
                np.asarray(([1.0, 1.0, 1.0, 1.0] * 24), dtype=np.float64),
                np.asarray(([1.0, -1.0, -1.0, 1.0] * 24), dtype=np.float64),
            ]
        )

        distances = subject._fixed_order_cosine_distances(vectors, query)

        self.assertEqual(
            [value.hex() for value in distances],
            ["0x1.ffffe80000000p-1", "-0x1.7800000000000p+5"],
        )

    def test_v25_open_normalization_does_not_delegate_reduction_to_blas(self) -> None:
        """Restoring backend-dependent norm reduction must fail this authority test."""
        vectors = np.zeros((2, 96), dtype=np.float32)
        vectors[0, :2] = [3.0, 4.0]
        vectors[1, :2] = [-4.0, 3.0]

        with mock.patch.object(
            np.linalg, "norm", side_effect=AssertionError("backend reduction used")
        ):
            normalized = subject._normalize_vectors(vectors)

        self.assertTrue(
            np.array_equal(
                normalized[:, :2],
                np.asarray([[0.6, 0.8], [-0.8, 0.6]], dtype=np.float32),
            )
        )

class V25OpenCliTests(unittest.TestCase):
    def test_v25_open_cli_fails_closed_before_reading_inputs(self) -> None:
        script = pathlib.Path(__file__).with_name("build_v25_containment_open.py")
        for arguments in (
            [],
            ["--manifest", "manifest.json", "--bucket", "forbidden"],
            [
                "--manifest",
                "manifest.json",
                "--input-dir",
                "input",
                "--output-dir",
                "output",
                "--convert-open-inputs",
            ],
        ):
            completed = subprocess.run(
                [sys.executable, str(script), *arguments],
                check=False,
                capture_output=True,
                text=True,
            )
            self.assertNotEqual(completed.returncode, 0)
            self.assertEqual(completed.stdout, "")
            self.assertIn("V25 open CLI", completed.stderr)

    def test_v25_open_cli_runs_only_the_registered_local_conversion(self) -> None:
        script = pathlib.Path(__file__).with_name("build_v25_containment_open.py")
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            inputs = root / "inputs"
            inputs.mkdir()
            construction, page_rows = _write_v24_fixture(inputs, 16)
            manifest = {
                "cohort_count": 12,
                "cohort_seed": 0x243F6A8885A308D3,
                "construction": {
                    **construction.__dict__,
                    "file_name": construction.path.name,
                },
                "output_generation": "v25-open-fixture",
                "output_row_group_size": 4,
                "output_uri_prefix": "s3://borsuk-v25/open-fixture/",
                "page_count": 16,
                "page_rows": {**page_rows.__dict__, "file_name": page_rows.path.name},
                "pseudoquery_count": 1,
                "pseudoquery_seed": 0x13198A2E03707344,
                "schema": "borsuk-v25-open-build-manifest-v1",
                "source_row_count": 16,
            }
            for value in (manifest["construction"], manifest["page_rows"]):
                del value["path"]
            manifest_path = inputs / "manifest.json"
            manifest_path.write_text(
                json.dumps(manifest, sort_keys=True, separators=(",", ":")) + "\n",
                encoding="utf-8",
            )
            output = root / "output"
            completed = subprocess.run(
                [
                    sys.executable,
                    str(script),
                    "--manifest",
                    str(manifest_path),
                    "--input-dir",
                    str(inputs),
                    "--output-dir",
                    str(output),
                    "--convert-open-inputs",
                    "--execute",
                ],
                check=False,
                capture_output=True,
                text=True,
            )
            self.assertEqual(completed.returncode, 0, completed.stderr)
            self.assertEqual(
                completed.stdout.encode(), (output / "conversion-receipt.json").read_bytes()
            )


if __name__ == "__main__":
    unittest.main()
