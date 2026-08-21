import argparse
import base64
import hashlib
import json
import random
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import pyarrow.parquet as pq

from scripts.prepare_beir_publication_dataset import (
    _row_table,
    _write_bounded_table_shards,
    convert_prepared_beir,
    prepare,
)
from scripts.prepare_hybrid_dataset import prepare as prepare_hybrid
from scripts.publication_v3_beir import (
    BEIR_DENSE_MODEL,
    BEIR_DENSE_REVISION,
    BEIR_ENCODER_PACKAGES,
    BEIR_QUERY_PREFIX,
    expected_beir_metadata,
)
from scripts.publication_v3_datasets import (
    build_dataset_descriptor,
    dataset_materialization_sha256,
)
from scripts.publication_v3_protocol import validate_manifest

SOURCE_URL = (
    "https://public.ukp.informatik.tu-darmstadt.de/thakur/BEIR/datasets/scifact.zip"
)


def write_fixture(root: Path) -> None:
    (root / "qrels").mkdir(parents=True)
    (root / "corpus.jsonl").write_text(
        '{"_id":"d1","title":"First","text":"alpha beta"}\n'
        '{"_id":"d2","title":"Second","text":"beta gamma"}\n',
        encoding="utf-8",
    )
    (root / "queries.jsonl").write_text(
        '{"_id":"q1","text":"alpha"}\n{"_id":"q2","text":"gamma"}\n',
        encoding="utf-8",
    )
    (root / "qrels" / "test.tsv").write_text(
        "query-id\tcorpus-id\tscore\nq1\td1\t2\nq2\td2\t0\n",
        encoding="utf-8",
    )
    (root / ".borsuk-source.json").write_text(
        json.dumps(
            {
                "dataset": "scifact",
                "url": SOURCE_URL,
                "archive_md5": "1" * 32,
                "archive_sha256": "2" * 64,
            },
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )


def arguments(source: Path, output: Path) -> argparse.Namespace:
    return argparse.Namespace(
        dataset="scifact",
        out=output,
        expected_source=SOURCE_URL,
        source=source,
        dense_backend="hash",
        dense_model=BEIR_DENSE_MODEL,
        dense_revision=BEIR_DENSE_REVISION,
        dense_batch_size=4,
        dense_query_prefix=BEIR_QUERY_PREFIX,
        dense_document_prefix="",
        sparse_max_features=250_000,
        publication=False,
    )


class PrepareBeirPublicationDatasetTests(unittest.TestCase):
    def test_beir_direct_and_transitive_dependencies_are_exactly_frozen(self) -> None:
        scripts = Path(__file__).resolve().parent
        direct = {
            line
            for line in (scripts / "requirements-beir-stage.in")
            .read_text(encoding="utf-8")
            .splitlines()
            if line and not line.startswith("#")
        }
        self.assertEqual(
            direct,
            {f"{name}=={version}" for name, version in BEIR_ENCODER_PACKAGES.items()}
            | {"pyarrow==24.0.0"},
        )
        lock_text = (scripts / "requirements-beir-stage.txt").read_text(
            encoding="utf-8"
        )
        locked = [
            line.rstrip(" " + chr(92))
            for line in lock_text.splitlines()
            if line and not line.startswith(("#", " "))
        ]
        self.assertTrue(all("==" in requirement for requirement in locked))
        self.assertTrue(direct.issubset(set(locked)))
        self.assertIn("--hash=sha256:", lock_text)

    def test_parquet_shards_split_by_encoded_bytes_and_remain_canonical(self) -> None:
        import pyarrow as pa

        generator = random.Random(19)
        texts = [
            base64.b85encode(generator.randbytes(40_000)).decode("ascii")
            for _ in range(4)
        ]
        table = pa.table(
            {
                "id": pa.array([f"d{index}" for index in range(4)]),
                "text": pa.array(texts),
            }
        )
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            next_shard = _write_bounded_table_shards(
                root,
                "corpus",
                table,
                first_shard=0,
                max_object_bytes=64 * 1024,
            )
            paths = sorted(root.glob("corpus-*.parquet"))
            self.assertEqual(next_shard, 4)
            self.assertEqual(
                [path.name for path in paths],
                [f"corpus-{index:08}.parquet" for index in range(4)],
            )
            self.assertTrue(all(8 < path.stat().st_size <= 64 * 1024 for path in paths))
            self.assertEqual(sum(pq.read_metadata(path).num_rows for path in paths), 4)

    def test_sparse_offsets_fail_before_narrowing_beyond_arrow_list_capacity(
        self,
    ) -> None:
        import numpy as np

        with self.assertRaisesRegex(ValueError, "32-bit Arrow list offsets"):
            _row_table(
                [("d1", "text")],
                np.zeros((1, 384), dtype=np.float32),
                np.array([0, 2**31], dtype=np.uint64),
                np.array([], dtype=np.uint32),
                np.array([], dtype=np.float32),
                384,
            )

    def test_zero_sparse_query_payload_still_materializes(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source"
            source.mkdir()
            write_fixture(source)
            (source / "queries.jsonl").write_text(
                '{"_id":"q1","text":"unseentoken"}\n', encoding="utf-8"
            )
            (source / "qrels" / "test.tsv").write_text(
                "query-id\tcorpus-id\tscore\nq1\td1\t2\n", encoding="utf-8"
            )
            output = root / "output"
            metadata = prepare(arguments(source, output))
            self.assertEqual(metadata["queries"], 1)
            self.assertEqual(
                pq.read_table(output / "queries-00000000.parquet")
                .column("sparse_indices")
                .to_pylist(),
                [[]],
            )

    def test_publication_rejects_bad_source_provenance_before_encoder_work(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source"
            source.mkdir()
            write_fixture(source)
            provenance = json.loads(
                (source / ".borsuk-source.json").read_text(encoding="utf-8")
            )
            provenance["archive_sha256"] = "short"
            (source / ".borsuk-source.json").write_text(
                json.dumps(provenance, sort_keys=True) + "\n", encoding="utf-8"
            )
            args = arguments(source, root / "output")
            args.publication = True
            args.dense_backend = "sentence-transformers"
            with mock.patch(
                "scripts.prepare_beir_publication_dataset.prepare_hybrid",
                side_effect=AssertionError("encoder must not run"),
            ):
                with self.assertRaisesRegex(ValueError, "source provenance"):
                    prepare(args)

    def test_publication_rejects_package_drift_before_encoder_work(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source"
            source.mkdir()
            write_fixture(source)
            args = arguments(source, root / "output")
            args.publication = True
            args.dense_backend = "sentence-transformers"

            def installed_version(package: str) -> str:
                if package == "torch":
                    return "0.0.0"
                return expected_beir_metadata("scifact")["dense"]["package_versions"][
                    package
                ]

            with (
                mock.patch(
                    "scripts.prepare_beir_publication_dataset.importlib.metadata.version",
                    side_effect=installed_version,
                ),
                mock.patch(
                    "scripts.prepare_beir_publication_dataset.prepare_hybrid",
                    side_effect=AssertionError("encoder must not run"),
                ),
            ):
                with self.assertRaisesRegex(ValueError, "package versions"):
                    prepare(args)

    def test_publication_reports_missing_package_before_encoder_work(self) -> None:
        import importlib.metadata

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source"
            source.mkdir()
            write_fixture(source)
            args = arguments(source, root / "output")
            args.publication = True
            args.dense_backend = "sentence-transformers"
            with (
                mock.patch(
                    "scripts.prepare_beir_publication_dataset.importlib.metadata.version",
                    side_effect=importlib.metadata.PackageNotFoundError("torch"),
                ),
                mock.patch(
                    "scripts.prepare_beir_publication_dataset.prepare_hybrid",
                    side_effect=AssertionError("encoder must not run"),
                ),
            ):
                with self.assertRaisesRegex(ValueError, "package versions"):
                    prepare(args)

    def test_hash_fixture_emits_deterministic_bounded_multimodal_parquet(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source"
            source.mkdir()
            write_fixture(source)
            outputs = [root / "first", root / "second"]
            metadata = [prepare(arguments(source, output)) for output in outputs]
            self.assertEqual(metadata[0], metadata[1])
            self.assertEqual(metadata[0]["documents"], 2)
            self.assertEqual(metadata[0]["queries"], 1)
            self.assertEqual(metadata[0]["qrels"], 1)
            expected_names = {
                "corpus-00000000.parquet",
                "queries-00000000.parquet",
                "qrels.parquet",
                "meta.json",
            }
            self.assertEqual(
                {path.name for path in outputs[0].iterdir()}, expected_names
            )
            self.assertEqual(
                pq.read_table(outputs[0] / "queries-00000000.parquet")
                .column("id")
                .to_pylist(),
                ["q1"],
            )
            self.assertEqual(
                pq.read_table(outputs[0] / "qrels.parquet").to_pylist(),
                [{"query_id": "q1", "corpus_id": "d1", "score": 2}],
            )
            for name in expected_names:
                first = hashlib.sha256((outputs[0] / name).read_bytes()).hexdigest()
                second = hashlib.sha256((outputs[1] / name).read_bytes()).hexdigest()
                self.assertEqual(first, second)

    def test_publication_producer_output_passes_the_authenticated_descriptor(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source"
            source.mkdir()
            write_fixture(source)
            prepared = root / "prepared"
            prepare_hybrid(
                argparse.Namespace(
                    source=source,
                    output=prepared,
                    dataset="scifact",
                    split="test",
                    dense_backend="hash",
                    dense_dimensions=384,
                    dense_model=BEIR_DENSE_MODEL,
                    dense_revision=BEIR_DENSE_REVISION,
                    dense_batch_size=4,
                    dense_query_prefix=BEIR_QUERY_PREFIX,
                    dense_document_prefix="",
                    sparse_max_features=250_000,
                    publication=False,
                )
            )
            prepared_manifest = json.loads(
                (prepared / "manifest.json").read_text(encoding="utf-8")
            )
            prepared_manifest["dense"] = {
                **expected_beir_metadata("scifact")["dense"],
                "publication_valid": True,
            }
            (prepared / "manifest.json").write_text(
                json.dumps(prepared_manifest, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            output = root / "materialized"
            convert_prepared_beir(
                prepared,
                output,
                dataset_id="scifact",
                expected_source=SOURCE_URL,
                source=source,
                publication=True,
            )
            manifest = validate_manifest(
                json.loads(
                    (
                        Path(__file__).resolve().parents[1]
                        / "docs/research/publication-v3-manifest.json"
                    ).read_text(encoding="utf-8")
                )
            )
            dataset = next(
                json.loads(json.dumps(item))
                for item in manifest["datasets"]
                if item["id"] == "scifact"
            )
            dataset["scale"]["rows"] = 2
            dataset["source"] = {
                "state": "staged",
                "url": output.resolve().as_uri(),
                "sha256": dataset_materialization_sha256(output, kind="beir-hybrid"),
                "license": dataset["source"]["license"],
            }
            descriptor = build_dataset_descriptor(dataset)
            self.assertEqual(descriptor["rows"], 2)


if __name__ == "__main__":
    unittest.main()
