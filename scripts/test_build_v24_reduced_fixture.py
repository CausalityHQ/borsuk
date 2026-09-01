from __future__ import annotations

import hashlib
import json
import os
import pathlib
import shutil
import subprocess
import tempfile
import unittest

import pyarrow as pa
import pyarrow.parquet as pq

from scripts import build_v24_reduced_fixture as subject


class V24ReducedFixtureTests(unittest.TestCase):
    def assert_progress(
        self,
        path: pathlib.Path,
        *,
        phase: str,
        completed_units: int | None = None,
        total_units: int | None = None,
    ) -> None:
        raw = path.read_bytes()
        value = json.loads(raw)
        self.assertEqual(
            raw,
            json.dumps(value, separators=(",", ":"), sort_keys=True).encode() + b"\n",
        )
        self.assertEqual(value["phase"], phase)
        self.assertGreater(value["sequence"], 0)
        self.assertGreater(value["completed_units"], 0)
        self.assertLessEqual(value["completed_units"], value["total_units"])
        if completed_units is not None:
            self.assertEqual(value["completed_units"], completed_units)
        if total_units is not None:
            self.assertEqual(value["total_units"], total_units)

    def test_reduced_fixture_is_deterministic_parquet_with_original_ordinals(self) -> None:
        with tempfile.TemporaryDirectory() as first, tempfile.TemporaryDirectory() as second:
            first_root = pathlib.Path(first)
            second_root = pathlib.Path(second)
            first_manifest = subject.build_reduced_fixture(
                first_root,
                source_rows=257,
                witness_count=32,
                page_count=16,
                query_count=32,
                generation="generation-v24-reduced-fixture",
            )
            second_manifest = subject.build_reduced_fixture(
                second_root,
                source_rows=257,
                witness_count=32,
                page_count=16,
                query_count=32,
                generation="generation-v24-reduced-fixture",
            )
            self.assertEqual(first_manifest, second_manifest)
            names = (
                "construction-rows.parquet",
                "page-rows.parquet",
                "queries.parquet",
                "neighbors.parquet",
                "training-manifest.json",
            )
            for name in names:
                self.assertEqual(
                    (first_root / name).read_bytes(),
                    (second_root / name).read_bytes(),
                    name,
                )

            construction = pq.read_table(first_root / "construction-rows.parquet")
            self.assertEqual(construction.num_rows, 257)
            self.assertEqual(
                construction.schema,
                pa.schema(
                    [
                        pa.field("source_ordinal", pa.uint64(), nullable=False),
                        pa.field(
                            "vector",
                            pa.list_(
                                pa.field("element", pa.float32(), nullable=False), 96
                            ),
                            nullable=False,
                        ),
                    ]
                ),
            )
            self.assertEqual(construction.column("source_ordinal").to_pylist(), list(range(257)))
            source_vectors = construction.column("vector").to_pylist()
            self.assertEqual(len({tuple(vector) for vector in source_vectors}), 257)

            pages = pq.read_table(first_root / "page-rows.parquet")
            self.assertEqual(pages.num_rows, 257)
            self.assertEqual(pages.column("record_id").to_pylist(), [
                str(ordinal)
                for page in range(16)
                for ordinal in range(page, 257, 16)
            ])
            expected_metadata = {
                b"construction_rows_sha256": hashlib.sha256(
                    (first_root / "construction-rows.parquet").read_bytes()
                ).hexdigest().encode(),
                b"generation": b"generation-v24-reduced-fixture",
            }
            self.assertEqual(pages.schema.metadata, expected_metadata)

            queries = pq.read_table(first_root / "queries.parquet")
            neighbors = pq.read_table(first_root / "neighbors.parquet")
            self.assertEqual((queries.num_rows, neighbors.num_rows), (32, 32))
            self.assertEqual(queries.column("query_ordinal").to_pylist(), list(range(32)))
            self.assertEqual(neighbors.column("query_ordinal").to_pylist(), list(range(32)))

            raw = (first_root / "training-manifest.json").read_bytes()
            self.assertEqual(raw, json.dumps(json.loads(raw), separators=(",", ":"), sort_keys=True).encode() + b"\n")
            self.assertEqual(first_manifest, json.loads(raw))
            self.assertEqual(first_manifest["source_row_count"], 257)
            self.assertEqual(first_manifest["witness_count"], 32)
            self.assertEqual(
                first_manifest["inputs"][0]["digest"],
                expected_metadata[b"construction_rows_sha256"].decode(),
            )

    def test_phase_manifests_bind_exact_parent_outputs_and_cross_language_files(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            training_manifest = subject.build_reduced_fixture(
                root,
                source_rows=257,
                witness_count=32,
                page_count=16,
                query_count=32,
                generation="generation-v24-reduced-fixture",
            )
            training_output = root / "training-output"
            training_output.mkdir()
            graph = training_output / "witness-graph.arrow"
            witnesses = training_output / "witnesses.arrow"
            graph.write_bytes(b"ARROW1-graph")
            witnesses.write_bytes(b"ARROW1-witnesses")
            output_identity = lambda path, role: {  # noqa: E731
                "digest": hashlib.sha256(path.read_bytes()).hexdigest(),
                "digest_algorithm": "sha256",
                "encoded_bytes": path.stat().st_size,
                "generation": training_manifest["generation"],
                "role": role,
                "uri": f"s3://borsuk-v24-reduced/{path.name}",
            }
            training_result = {
                "claim_eligible": False,
                "distance_backend": "aarch64-neon-fma",
                "generation": training_manifest["generation"],
                "inputs": training_manifest["inputs"],
                "outputs": [
                    output_identity(graph, "witness-graph"),
                    output_identity(witnesses, "witnesses-arrow"),
                ],
                "phase": "witness-training",
                "schema": "borsuk-v24-training-result-v1",
                "seed": training_manifest["seed"],
                "source_row_count": training_manifest["source_row_count"],
                "witness_count": training_manifest["witness_count"],
            }
            (training_output / "result.json").write_bytes(
                json.dumps(
                    training_result, separators=(",", ":"), sort_keys=True
                ).encode()
                + b"\n"
            )

            posting_manifest_path = subject.prepare_posting_phase(root, training_output)
            posting_manifest = json.loads(posting_manifest_path.read_bytes())
            self.assertEqual(
                [identity["role"] for identity in posting_manifest["inputs"]],
                [
                    "training-result",
                    "witness-graph",
                    "witnesses-arrow",
                    "page-rows-parquet",
                ],
            )
            self.assertEqual(
                sorted(path.name for path in (root / "posting-input").iterdir()),
                [
                    "page-rows.parquet",
                    "training-result.json",
                    "witness-graph.arrow",
                    "witnesses.arrow",
                ],
            )

            posting_output = root / "posting-output"
            posting_output.mkdir()
            postings = posting_output / "witness-postings.arrow"
            postings.write_bytes(b"ARROW1-postings")
            development_manifest_path = subject.prepare_development_phase(
                root, training_output, posting_output
            )
            development_manifest = json.loads(development_manifest_path.read_bytes())
            self.assertEqual(
                [identity["role"] for identity in development_manifest["inputs"]],
                [
                    "witness-graph",
                    "witness-postings",
                    "query-parquet",
                    "neighbors-parquet",
                ],
            )
            self.assertEqual(
                sorted(path.name for path in (root / "development-input").iterdir()),
                [
                    "neighbors.parquet",
                    "queries.parquet",
                    "witness-graph.arrow",
                    "witness-postings.arrow",
                ],
            )

    def test_reduced_fixture_runs_real_three_phase_binary_when_registered(self) -> None:
        binary_value = os.environ.get("BORSUK_V24_BINARY")
        if binary_value is None:
            self.skipTest("BORSUK_V24_BINARY is not registered")
        binary = pathlib.Path(binary_value)
        self.assertTrue(binary.is_absolute() and binary.is_file())
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            subject.build_reduced_fixture(
                root,
                source_rows=257,
                witness_count=32,
                page_count=16,
                query_count=32,
                generation="generation-v24-reduced-fixture",
            )
            training_output = root / "training-output"
            training_input = root / "training-input"
            training_input.mkdir()
            shutil.copyfile(
                root / "construction-rows.parquet",
                training_input / "construction-rows.parquet",
            )
            training_output.mkdir()
            subprocess.run(
                [
                    str(binary),
                    "--manifest",
                    str(root / "training-manifest.json"),
                    "--input-dir",
                    str(training_input),
                    "--output-dir",
                    str(training_output),
                    "--train-witnesses",
                    "--execute",
                ],
                check=True,
                capture_output=True,
            )
            self.assert_progress(
                training_output / "progress.json",
                phase="witness-training",
                completed_units=289,
                total_units=289,
            )
            posting_manifest = subject.prepare_posting_phase(root, training_output)
            posting_output = root / "posting-output"
            posting_output.mkdir()
            subprocess.run(
                [
                    str(binary),
                    "--manifest",
                    str(posting_manifest),
                    "--input-dir",
                    str(root / "posting-input"),
                    "--output-dir",
                    str(posting_output),
                    "--build-postings",
                    "--execute",
                ],
                check=True,
                capture_output=True,
            )
            self.assert_progress(
                posting_output / "progress.json",
                phase="posting-construction",
                completed_units=545,
                total_units=545,
            )
            development_manifest = subject.prepare_development_phase(
                root, training_output, posting_output
            )
            development_output = root / "development-output"
            development_output.mkdir()
            completed = subprocess.run(
                [
                    str(binary),
                    "--manifest",
                    str(development_manifest),
                    "--input-dir",
                    str(root / "development-input"),
                    "--output-dir",
                    str(development_output),
                    "--evaluate-development",
                    "--execute",
                ],
                check=True,
                capture_output=True,
            )
            self.assert_progress(
                development_output / "progress.json",
                phase="development-evaluation",
            )
            self.assertEqual(
                completed.stdout,
                (development_output / "result.json").read_bytes(),
            )


if __name__ == "__main__":
    unittest.main()
