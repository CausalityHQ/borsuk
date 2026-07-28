import csv
import hashlib
import io
import json
import subprocess
import sys
import tarfile
import tempfile
import unittest
from dataclasses import asdict
from pathlib import Path

from scripts.analyze_publication_claims import compare_direct
from scripts.publication_protocol import SCHEDULE_FIELDS, build_schedule
from scripts.test_publication_protocol import valid_manifest
from scripts.validate_publication_v2_results import validate_result_tree

ROOT = Path(__file__).resolve().parents[1]


def write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def write_nonempty(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("field\nvalue\n", encoding="utf-8")


def write_source_archive(root: Path, embedded_manifest: bytes) -> str:
    archive_path = root / "source-archive.tar.gz"
    with tarfile.open(archive_path, "w:gz") as archive:
        info = tarfile.TarInfo("docs/research/publication-v2-manifest.json")
        info.size = len(embedded_manifest)
        info.mtime = 0
        archive.addfile(info, io.BytesIO(embedded_manifest))
    return hashlib.sha256(archive_path.read_bytes()).hexdigest()


def hybrid_coverage_rows(dataset: str, query_seed: int) -> list[dict[str, object]]:
    common = {
        "dataset": dataset,
        "profile": "srht",
        "status": "measured",
        "scan_codec": "srht-pq-scan",
    }
    rows: list[dict[str, object]] = [
        {
            "stage": "build",
            **common,
            "mode": "",
            "candidate_depth": "",
            "max_segments": "",
            "fusion": "",
            "rrf_k": "",
            "target_hot_query_fraction": "",
            "cache_profile": "",
            "campaign_repetition": "",
            "query_seed": "",
        }
    ]
    for mode in (
        "dense",
        "sparse",
        "text",
        "dense+sparse",
        "dense+text",
        "sparse+text",
        "dense+sparse+text",
    ):
        rrf_k = 60 if "+" in mode else 1
        for hot_fraction in ("0", "0.5", "1"):
            rows.append(
                {
                    "stage": "query",
                    **common,
                    "mode": mode,
                    "candidate_depth": 256,
                    "max_segments": 64,
                    "fusion": "rrf",
                    "rrf_k": rrf_k,
                    "target_hot_query_fraction": hot_fraction,
                    "cache_profile": f"mixed-cache-{hot_fraction}",
                    "campaign_repetition": 1,
                    "query_seed": query_seed,
                }
            )
    return rows


def write_fixture(root: Path) -> None:
    manifest = valid_manifest(
        queries_per_repetition=2,
        publish_p99=False,
        dense_datasets=["fashion-mnist-784", "glove-100"],
        hybrid_datasets=["scifact"],
    )
    (root / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    schedule = build_schedule(manifest)
    with (root / "schedule.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=SCHEDULE_FIELDS, lineterminator="\n")
        writer.writeheader()
        writer.writerows(schedule)

    source_sha = write_source_archive(root, (root / "manifest.json").read_bytes())
    manifest_sha = hashlib.sha256((root / "manifest.json").read_bytes()).hexdigest()
    (root / "environment.txt").write_text(
        "\n".join(
            [
                f"source_sha256={source_sha}",
                f"manifest_sha256={manifest_sha}",
                f"run_id={manifest['campaign_id']}",
                "execute=1",
                "logical_cpus=32",
                "ram_bytes=68719476736",
                "instance_type=c7g.8xlarge",
                "local_disk_class=ebs-gp3-500-3000-125",
                "accelerator=none",
                "index_storage_class=amazon-s3-standard",
                "hybrid_python_version=3.12",
                (
                    "client_compute_boundary="
                    "same-instance-for-borsuk-and-amazon-s3-vectors-client"
                ),
                "managed_service_compute=undisclosed",
                "rustc_version=rustc 1.91.0",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    hybrid_inputs = root / "hybrid-inputs"
    hybrid_inputs.mkdir()
    dense = {
        **manifest["hybrid_dense_config"],
        "dimensions": 384,
        "publication_valid": True,
    }
    (hybrid_inputs / "scifact-manifest.json").write_text(
        json.dumps({"dataset": "scifact", "dense": dense}),
        encoding="utf-8",
    )
    (hybrid_inputs / "scifact-validation.json").write_text(
        json.dumps({"dataset": "scifact", "status": "valid"}),
        encoding="utf-8",
    )
    (hybrid_inputs / "scifact-source.json").write_text(
        json.dumps(
            {
                "dataset": "scifact",
                "url": "https://example.test/scifact.zip",
                "archive_sha256": "1" * 64,
            }
        ),
        encoding="utf-8",
    )
    (root / "hybrid-python-packages.txt").write_text(
        "\n".join(
            (
                "boto3==1.42.97",
                "numpy==1.26.4",
                "sentence-transformers==3.4.1",
                "torch==2.5.1",
                "transformers==4.48.3",
            )
        )
        + "\n",
        encoding="utf-8",
    )

    all_borsuk: list[dict[str, object]] = []
    all_s3: list[dict[str, object]] = []
    for schedule_row in schedule:
        repetition = str(schedule_row["repetition_id"])
        query_seed = int(schedule_row["query_seed"])
        repetition_root = root / repetition
        repetition_root.mkdir()
        (repetition_root / "protocol.txt").write_text(
            "".join(f"{field}={schedule_row[field]}\n" for field in SCHEDULE_FIELDS),
            encoding="utf-8",
        )
        (repetition_root / "REPETITION_COMPLETE").write_text(
            "complete\n", encoding="utf-8"
        )
        write_csv(
            repetition_root / "borsuk-direct/coverage.csv",
            [
                {
                    "dataset": "fashion-mnist-784",
                    "status": "measured",
                    "scan_codec": "srht-pq-scan",
                }
            ],
        )
        write_csv(
            repetition_root / "borsuk-dense/coverage.csv",
            [
                {
                    "dataset": "glove-100",
                    "status": "measured",
                    "scan_codec": "srht-pq-scan",
                }
            ],
        )
        write_csv(
            repetition_root / "amazon-s3-vectors/coverage.csv",
            [
                {
                    "dataset": "fashion-mnist-784",
                    "status": "measured",
                    "repetition_id": repetition,
                    "query_seed": query_seed,
                }
            ],
        )
        write_csv(
            repetition_root / "hybrid/coverage.csv",
            hybrid_coverage_rows("scifact", query_seed),
        )
        for system, dataset, required in (
            (
                "borsuk-direct",
                "fashion-mnist-784",
                (
                    "bench_build.csv",
                    "bench_recall_latency.csv",
                    "bench_startup.csv",
                    "bench_cache_states.csv",
                    "bench_concurrency.csv",
                    "bench_concurrency_samples.csv",
                    "resources.csv",
                ),
            ),
            (
                "borsuk-dense",
                "glove-100",
                (
                    "bench_build.csv",
                    "bench_recall_latency.csv",
                    "bench_query_samples.csv",
                    "bench_startup.csv",
                    "bench_cache_states.csv",
                    "bench_concurrency.csv",
                    "bench_concurrency_samples.csv",
                    "resources.csv",
                ),
            ),
        ):
            for name in required:
                write_nonempty(repetition_root / system / dataset / name)
        for name in ("build.csv", "query.csv", "resources.csv"):
            write_nonempty(
                repetition_root
                / f"amazon-s3-vectors/fashion-mnist-784/{repetition}/{name}"
            )
        hybrid_root = repetition_root / "hybrid/scifact/srht"
        write_nonempty(hybrid_root / "dataset-validation.json")
        for name in ("hybrid_build.csv", "resources.csv"):
            write_nonempty(hybrid_root / f"build/{name}")
        for row in hybrid_coverage_rows("scifact", query_seed):
            if row["stage"] != "query":
                continue
            query_root = (
                hybrid_root
                / "query"
                / str(row["mode"])
                / f"c{row['candidate_depth']}-p{row['max_segments']}"
                / f"rrf-k{row['rrf_k']}"
                / f"hot-{row['target_hot_query_fraction']}"
                / "repetition-1"
            )
            for name in (
                "hybrid_queries.csv",
                "hybrid_summary.csv",
                "hybrid_startup.csv",
                "resources.csv",
            ):
                write_nonempty(query_root / name)

        borsuk_rows = []
        s3_rows = []
        for query_position, source_index in enumerate((1, 0)):
            for phase, latency in (("uncached", 10.0), ("disk_cached", 5.0)):
                row = {
                    "repetition_id": repetition,
                    "query_seed": query_seed,
                    "phase": phase,
                    "mode": "srht-pq-scan",
                    "nprobe": 8,
                    "max_candidates": 320,
                    "sample_index": query_position,
                    "query_source_index": source_index,
                    "latency_ms": latency + query_position,
                    "recall_at_10": 1.0,
                }
                borsuk_rows.append(row)
                all_borsuk.append(
                    {
                        **row,
                        "query_position": query_position,
                        "status": "ok",
                    }
                )
            for query_pass, latency in (
                ("first_pass", 20.0),
                ("repeated_pass", 10.0),
            ):
                row = {
                    "repetition_id": repetition,
                    "query_seed": query_seed,
                    "pass": query_pass,
                    "query_position": query_position,
                    "query_source_index": source_index,
                    "latency_ms": latency + query_position,
                    "recall_at_10": 1.0,
                    "status": "ok",
                }
                s3_rows.append(row)
                all_s3.append(row)
        write_csv(
            repetition_root / "borsuk-direct/fashion-mnist-784/bench_query_samples.csv",
            borsuk_rows,
        )
        write_csv(
            repetition_root
            / f"amazon-s3-vectors/fashion-mnist-784/{repetition}/query_samples.csv",
            s3_rows,
        )

    for borsuk_phase, s3_pass, suffix, cache_pair in (
        ("uncached", "first_pass", "first", "uncached-vs-first-pass"),
        (
            "disk_cached",
            "repeated_pass",
            "repeated",
            "disk-cached-vs-repeated-pass",
        ),
    ):
        selected_borsuk = [row for row in all_borsuk if row["phase"] == borsuk_phase]
        selected_s3 = [row for row in all_s3 if row["pass"] == s3_pass]
        decision = compare_direct(
            selected_borsuk,
            selected_s3,
            bootstrap_samples=100,
            expected_repetitions=5,
            expected_queries_per_repetition=2,
        )
        write_csv(
            root / f"direct-claim-{suffix}.csv",
            [
                {
                    "dataset": "fashion-mnist-784",
                    "cache_pair": cache_pair,
                    **asdict(decision),
                }
            ],
        )

    (root / "PUBLICATION_V2_COMPLETE").write_text("complete\n", encoding="utf-8")


class ValidatePublicationV2ResultsTests(unittest.TestCase):
    def test_accepts_complete_frozen_paired_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write_fixture(root)
            summary = validate_result_tree(root, bootstrap_samples=100)
            self.assertEqual(summary["repetitions"], 5)
            self.assertEqual(summary["paired_queries_per_phase"], 10)
            completed = subprocess.run(
                [
                    sys.executable,
                    str(ROOT / "scripts/validate_publication_v2_results.py"),
                    str(root),
                    "--bootstrap-samples",
                    "100",
                ],
                capture_output=True,
                text=True,
            )
            self.assertEqual(completed.returncode, 0, completed.stderr)
            self.assertIn('"status": "valid"', completed.stdout)

    def test_rejects_missing_completion_or_hardware_identity(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write_fixture(root)
            (root / "r03/REPETITION_COMPLETE").unlink()
            with self.assertRaisesRegex(ValueError, "REPETITION_COMPLETE"):
                validate_result_tree(root, bootstrap_samples=100)

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write_fixture(root)
            environment = (root / "environment.txt").read_text(encoding="utf-8")
            (root / "environment.txt").write_text(
                environment.replace(
                    "managed_service_compute=undisclosed",
                    "managed_service_compute=claimed-smaller",
                ),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ValueError, "managed_service_compute"):
                validate_result_tree(root, bootstrap_samples=100)

    def test_rejects_unpaired_query_source_or_incomplete_coverage(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write_fixture(root)
            samples = (
                root / "r02/amazon-s3-vectors/fashion-mnist-784/r02/query_samples.csv"
            )
            text = samples.read_text(encoding="utf-8")
            samples.write_text(text.replace(",1,20.0,", ",999,20.0,", 1))
            with self.assertRaisesRegex(ValueError, "source"):
                validate_result_tree(root, bootstrap_samples=100)

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write_fixture(root)
            coverage = root / "r04/hybrid/coverage.csv"
            coverage.write_text(
                coverage.read_text(encoding="utf-8").replace(
                    "query,scifact,srht,measured,",
                    "query,scifact,srht,failed,",
                    1,
                ),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ValueError, "hybrid"):
                validate_result_tree(root, bootstrap_samples=100)

    def test_rejects_duplicate_coverage_rows(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write_fixture(root)
            coverage = root / "r03/borsuk-dense/coverage.csv"
            with coverage.open(encoding="utf-8", newline="") as handle:
                rows = list(csv.DictReader(handle))
            write_csv(coverage, [*rows, rows[0]])
            with self.assertRaisesRegex(ValueError, "coverage row count"):
                validate_result_tree(root, bootstrap_samples=100)

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write_fixture(root)
            coverage = root / "r05/hybrid/coverage.csv"
            with coverage.open(encoding="utf-8", newline="") as handle:
                rows = list(csv.DictReader(handle))
            write_csv(coverage, [*rows, rows[0]])
            with self.assertRaisesRegex(ValueError, "coverage row count"):
                validate_result_tree(root, bootstrap_samples=100)

    def test_rejects_manifest_that_differs_from_frozen_source_archive(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write_fixture(root)
            source_sha = write_source_archive(root, b"{}\n")
            environment = (root / "environment.txt").read_text(encoding="utf-8")
            environment = environment.replace(
                next(
                    line
                    for line in environment.splitlines()
                    if line.startswith("source_sha256=")
                ),
                f"source_sha256={source_sha}",
            )
            (root / "environment.txt").write_text(environment, encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "embedded manifest"):
                validate_result_tree(root, bootstrap_samples=100)

    def test_rejects_missing_measured_artifact(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write_fixture(root)
            (root / "r02/borsuk-dense/glove-100/bench_build.csv").unlink()
            with self.assertRaisesRegex(ValueError, "missing measured artifact"):
                validate_result_tree(root, bootstrap_samples=100)

    def test_rejects_hybrid_input_or_dependency_drift(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write_fixture(root)
            validation = root / "hybrid-inputs/scifact-validation.json"
            validation.write_text(
                json.dumps({"dataset": "scifact", "status": "invalid"}),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ValueError, "hybrid input validation"):
                validate_result_tree(root, bootstrap_samples=100)

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write_fixture(root)
            packages = root / "hybrid-python-packages.txt"
            packages.write_text(
                packages.read_text(encoding="utf-8").replace(
                    "torch==2.5.1", "torch==2.6.0"
                ),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ValueError, "hybrid dependency"):
                validate_result_tree(root, bootstrap_samples=100)


if __name__ == "__main__":
    unittest.main()
