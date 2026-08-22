import contextlib
import copy
import hashlib
import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import h5py
import numpy as np

from scripts.fetch_ann_dataset import convert_hdf5_dataset
from scripts.publication_v3_aws import build_staging_receipt, staging_jobs
from scripts.stage_publication_v3_dataset import (
    adapter_command,
    materialize_dataset,
)

ROOT = Path(__file__).resolve().parents[1]


def frozen_manifest() -> dict[str, object]:
    value = json.loads(
        (ROOT / "docs/research/publication-v3-manifest.json").read_text(
            encoding="utf-8"
        )
    )
    value["source"] = {
        "state": "frozen",
        "git_commit": "1" * 40,
        "archive_sha256": "2" * 64,
        "cargo_lock_sha256": "3" * 64,
        "python_lock_sha256": "4" * 64,
        "node_lock_sha256": "5" * 64,
    }
    expected_sources = {
        "deep-image-96": "https://ann-benchmarks.com/deep-image-96-angular.hdf5",
        "sift-128": "https://ann-benchmarks.com/sift-128-euclidean.hdf5",
        "laion-100m-768": "s3://assets.zilliz.com/benchmark/laion_large_100m",
        "scifact": "https://public.ukp.informatik.tu-darmstadt.de/thakur/BEIR/datasets/scifact.zip",
    }
    for dataset in value["datasets"]:
        expected_source = expected_sources.get(dataset["id"])
        if expected_source is None:
            continue
        dataset["source"] = {
            "state": "unstaged",
            "expected_source": expected_source,
            "license": dataset["source"]["license"],
        }
    return value


class StagePublicationV3DatasetTests(unittest.TestCase):
    def test_ann_fixture_materializes_stock_parquet_and_rerun_is_noop(self) -> None:
        manifest = frozen_manifest()
        dataset = next(
            item for item in manifest["datasets"] if item["id"] == "deep-image-96"
        )
        dataset["scale"]["rows"] = 4
        dimensions = dataset["dimensions"]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "deep-image.hdf5"
            with h5py.File(source, "w") as handle:
                handle.create_dataset(
                    "train",
                    data=np.arange(4 * dimensions, dtype=np.float32).reshape(
                        4, dimensions
                    ),
                )
                handle.create_dataset(
                    "test",
                    data=np.arange(dimensions, dtype=np.float32).reshape(1, dimensions),
                )
                handle.create_dataset(
                    "neighbors", data=np.arange(10, dtype=np.int32).reshape(1, 10) % 4
                )
                handle.attrs["distance"] = "angular"
            first = materialize_dataset(
                manifest,
                dataset_id="deep-image-96",
                attempt=1,
                work_root=root / "work",
                source_cache=source,
            )
            self.assertEqual(first["dataset_id"], "deep-image-96")
            self.assertEqual(first["materialization"], "staged-parquet")
            self.assertEqual(
                first["source"]["url"],
                "s3://borsuk-bench-453182569524-euc1/publication/v3/20260812/"
                "datasets/deep-image-96/attempts/0001/materialized",
            )
            self.assertEqual(
                {item["role"] for item in first["objects"]},
                {"train", "query", "ground-truth", "metadata"},
            )
            provenance = (
                root / "work" / "attempts" / "0001" / "materialized.provenance.json"
            )
            self.assertTrue(provenance.is_file())
            provenance_value = json.loads(provenance.read_text(encoding="utf-8"))
            job = next(
                job
                for job in staging_jobs(manifest)
                if job.dataset_id == "deep-image-96"
            )
            receipt = build_staging_receipt(
                manifest,
                job,
                source_archive_sha256="a" * 64,
                source_provenance=provenance_value,
                provenance_sha256=hashlib.sha256(provenance.read_bytes()).hexdigest(),
                objects=tuple(
                    {
                        **{key: value for key, value in item.items() if key != "path"},
                        "uri": f"{job.output_uri}/{item['path']}",
                    }
                    for item in first["objects"]
                ),
                instance_id="i-0123456789abcdef0",
                instance_type="r7g.8xlarge",
                availability_zone="eu-central-1a",
                purchase_option="spot",
            )
            self.assertEqual(receipt["dataset_content_sha256"], first["content_sha256"])
            mtimes = {
                item["path"]: (
                    root / "work" / "attempts" / "0001" / "materialized" / item["path"]
                )
                .stat()
                .st_mtime_ns
                for item in first["objects"]
            }
            with mock.patch(
                "scripts.stage_publication_v3_dataset.require_free_disk",
                side_effect=RuntimeError("new work must not run"),
            ):
                second = materialize_dataset(
                    manifest,
                    dataset_id="deep-image-96",
                    attempt=1,
                    work_root=root / "work",
                    source_cache=source,
                )
            self.assertEqual(second, first)
            self.assertEqual(
                mtimes,
                {
                    item["path"]: (
                        root
                        / "work"
                        / "attempts"
                        / "0001"
                        / "materialized"
                        / item["path"]
                    )
                    .stat()
                    .st_mtime_ns
                    for item in second["objects"]
                },
            )
            value = json.loads(provenance.read_text(encoding="utf-8"))
            value["materialization_sha256"] = "0" * 64
            provenance.write_text(json.dumps(value), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "provenance"):
                materialize_dataset(
                    manifest,
                    dataset_id="deep-image-96",
                    attempt=1,
                    work_root=root / "work",
                    source_cache=source,
                )

    def test_unfrozen_source_fails_closed(self) -> None:
        manifest = frozen_manifest()
        unfrozen = copy.deepcopy(manifest)
        unfrozen["source"] = {"state": "unfrozen"}
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(ValueError, "frozen source"):
                materialize_dataset(
                    unfrozen,
                    dataset_id="deep-image-96",
                    attempt=1,
                    work_root=Path(directory),
                )

    def test_adapter_commands_are_exact_and_never_partial(self) -> None:
        manifest = frozen_manifest()
        ann = adapter_command(
            manifest,
            "deep-image-96",
            Path("/work/materialized"),
            Path("/cache/deep-image.hdf5"),
        )
        self.assertIn("deep-image-96-angular", ann)
        self.assertNotIn("--limit-train", ann)
        vdb = adapter_command(
            manifest, "laion-100m-768", Path("/work/materialized"), None
        )
        self.assertIn("laion-100M", vdb)
        self.assertIn("--execute-download", vdb)
        self.assertIn("--publication-output-root", vdb)
        beir = adapter_command(manifest, "scifact", Path("/work/materialized"), None)
        self.assertIn("prepare_beir_publication_dataset.py", beir[1])
        self.assertIn("BAAI/bge-small-en-v1.5", beir)
        self.assertIn("5c38ec7c405ec4b44b94cc5a9bb96e735b38267a", beir)
        self.assertIn("--publication", beir)

    def test_failed_adapter_never_exposes_partial_materialized_directory(self) -> None:
        manifest = frozen_manifest()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)

            def fail_after_partial(command, **_kwargs):
                output = Path(command[command.index("--out") + 1])
                output.mkdir(parents=True)
                (output / "partial").write_bytes(b"partial")
                raise __import__("subprocess").CalledProcessError(1, command)

            with mock.patch(
                "scripts.stage_publication_v3_dataset.subprocess.run",
                side_effect=fail_after_partial,
            ):
                with self.assertRaises(__import__("subprocess").CalledProcessError):
                    materialize_dataset(
                        manifest,
                        dataset_id="deep-image-96",
                        attempt=1,
                        work_root=root,
                    )
            self.assertFalse((root / "attempts" / "0001" / "materialized").exists())
            self.assertFalse(
                (root / "attempts" / ".0001-deep-image-96.partial").exists()
            )

    def test_default_ann_cache_is_shared_across_attempts(self) -> None:
        manifest = frozen_manifest()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            commands: list[tuple[str, ...]] = []

            def stop(command, **_kwargs):
                commands.append(tuple(command))
                raise __import__("subprocess").CalledProcessError(1, command)

            with mock.patch(
                "scripts.stage_publication_v3_dataset.subprocess.run", side_effect=stop
            ):
                for attempt in (1, 2):
                    with self.assertRaises(__import__("subprocess").CalledProcessError):
                        materialize_dataset(
                            manifest,
                            dataset_id="deep-image-96",
                            attempt=attempt,
                            work_root=root,
                        )
            caches = [
                Path(command[command.index("--source-cache") + 1])
                for command in commands
            ]
            self.assertEqual(
                caches,
                [root / "source-cache" / "deep-image-96-angular.hdf5"] * 2,
            )

    def test_vdb_provenance_survives_handoff_and_child_stdout_is_captured(self) -> None:
        manifest = frozen_manifest()
        dataset = next(
            item for item in manifest["datasets"] if item["id"] == "laion-100m-768"
        )
        dataset["scale"]["rows"] = 4
        dataset["dimensions"] = 4
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source.hdf5"
            with h5py.File(source, "w") as handle:
                handle.create_dataset(
                    "train", data=np.arange(16, dtype=np.float32).reshape(4, 4)
                )
                handle.create_dataset(
                    "test", data=np.arange(4, dtype=np.float32).reshape(1, 4)
                )
                handle.create_dataset(
                    "neighbors", data=np.arange(10, dtype=np.int32).reshape(1, 10) % 4
                )
                handle.attrs["distance"] = "euclidean"

            def materialize_vdb(command, **_kwargs):
                publication_root = Path(
                    command[command.index("--publication-output-root") + 1]
                )
                candidate = publication_root / "laion-100m-768"
                convert_hdf5_dataset(
                    source,
                    candidate,
                    dataset_name="sift-128-euclidean",
                    publication_id="laion-100m-768",
                )
                provenance_path = publication_root / "laion-100m-768.provenance.json"
                provenance = json.loads(provenance_path.read_text(encoding="utf-8"))
                provenance["source"] = dataset["source"]["expected_source"]
                provenance_path.write_text(json.dumps(provenance), encoding="utf-8")
                Path(command[command.index("--output-root") + 1]).mkdir()
                return __import__("subprocess").CompletedProcess(
                    command, 0, stdout="noisy child output\n"
                )

            with (
                mock.patch(
                    "scripts.stage_publication_v3_dataset.subprocess.run",
                    side_effect=materialize_vdb,
                ),
                contextlib.redirect_stdout(io.StringIO()) as stdout,
            ):
                descriptor = materialize_dataset(
                    manifest,
                    dataset_id="laion-100m-768",
                    attempt=1,
                    work_root=root / "work",
                )
            self.assertEqual(stdout.getvalue(), "")
            self.assertEqual(descriptor["dataset_id"], "laion-100m-768")
            provenance_path = (
                root / "work" / "attempts" / "0001" / "materialized.provenance.json"
            )
            self.assertEqual(
                json.loads(provenance_path.read_text(encoding="utf-8"))["source"],
                "s3://assets.zilliz.com/benchmark/laion_large_100m",
            )

    def test_disk_preflight_fails_before_adapter_or_attempt_creation(self) -> None:
        manifest = frozen_manifest()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            stale = root / "attempts" / ".0000-laion-100m-768.partial"
            stale.mkdir(parents=True)
            (stale / "orphan").write_bytes(b"orphan")

            def reject_after_reclaim(*_args):
                self.assertFalse(stale.exists())
                raise RuntimeError("insufficient publication disk")

            with (
                mock.patch(
                    "scripts.stage_publication_v3_dataset.require_free_disk",
                    side_effect=reject_after_reclaim,
                ),
                mock.patch(
                    "scripts.stage_publication_v3_dataset.subprocess.run"
                ) as adapter,
            ):
                with self.assertRaisesRegex(
                    RuntimeError, "insufficient publication disk"
                ):
                    materialize_dataset(
                        manifest,
                        dataset_id="laion-100m-768",
                        attempt=1,
                        work_root=root,
                    )
            adapter.assert_not_called()
            self.assertEqual(list((root / "attempts").iterdir()), [])

    def test_beir_success_removes_ephemeral_acquisition_bytes(self) -> None:
        manifest = frozen_manifest()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)

            def materialize_beir(command, **_kwargs):
                output = Path(command[command.index("--out") + 1])
                output.mkdir(parents=True)
                (output / "placeholder").write_bytes(b"sealed")
                acquired = output.parent / "acquired"
                acquired.mkdir()
                (acquired / "archive.zip").write_bytes(b"ephemeral")
                (output.parent / "materialized.provenance.json").write_text(
                    "{}", encoding="utf-8"
                )
                return __import__("subprocess").CompletedProcess(command, 0, stdout="")

            with (
                mock.patch(
                    "scripts.stage_publication_v3_dataset.subprocess.run",
                    side_effect=materialize_beir,
                ),
                mock.patch(
                    "scripts.stage_publication_v3_dataset._descriptor",
                    return_value={"dataset_id": "scifact"},
                ),
                mock.patch("scripts.stage_publication_v3_dataset.require_free_disk"),
            ):
                descriptor = materialize_dataset(
                    manifest,
                    dataset_id="scifact",
                    attempt=1,
                    work_root=root,
                )
            self.assertEqual(descriptor["dataset_id"], "scifact")
            self.assertFalse((root / "attempts" / "0001" / "acquired").exists())


if __name__ == "__main__":
    unittest.main()
