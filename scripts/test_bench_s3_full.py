import csv
import os
import subprocess
import tempfile
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
SCRIPT = REPO_ROOT / "scripts" / "bench_s3_full.sh"
DATASETS = [
    "fashion-mnist-784",
    "glove-100",
    "sift-128",
    "nytimes-256",
    "gist-960",
]


class FullS3BenchmarkRunnerTests(unittest.TestCase):
    def run_script(
        self,
        fail_dataset: str = "__never__",
        dataset_names: str | None = None,
    ):
        temporary = tempfile.TemporaryDirectory()
        root = Path(temporary.name)
        datasets = root / "datasets"
        output = root / "output"
        bin_dir = root / "bin"
        capture = root / "captured.txt"
        bin_dir.mkdir()
        for dataset in DATASETS:
            (datasets / dataset).mkdir(parents=True)

        cargo = bin_dir / "cargo"
        cargo.write_text("#!/bin/sh\nexit 0\n")
        cargo.chmod(0o755)
        python = bin_dir / "python3"
        python.write_text(
            "#!/bin/sh\n"
            'case "$1" in\n'
            "  *benchmark_with_resources.py)\n"
            '    printf \'%s|%s|%s|%s\\n\' "$BORSUK_BENCH_DATASET" "$BORSUK_BENCH_URI" "$BORSUK_BENCH_OUTPUT_DIR" "$BORSUK_BENCH_CACHE" >> "$CAPTURE"\n'
            '    case "$BORSUK_BENCH_DATASET" in *"$FAIL_DATASET") exit 7;; esac;;\n'
            "esac\n"
            "exit 0\n"
        )
        python.chmod(0o755)

        env = os.environ.copy()
        env.update(
            {
                "PATH": f"{bin_dir}:{env['PATH']}",
                "CAPTURE": str(capture),
                "FAIL_DATASET": fail_dataset,
                "DATASETS": str(datasets),
                "OUT": str(output),
                "BORSUK_S3_BUCKET": "s3://test-bucket/bench",
                "BORSUK_BENCH_QUERIES": "1",
                "BORSUK_RUN_FULL_S3": "1",
                "BORSUK_FULL_RUN_ID": "fresh-r1",
            }
        )
        if dataset_names is not None:
            env["BORSUK_FULL_DATASET_NAMES"] = dataset_names
        result = subprocess.run(
            [str(SCRIPT)], cwd=REPO_ROOT, env=env, text=True, capture_output=True
        )
        return temporary, root, output, capture, result

    def test_each_dataset_gets_fresh_storage_cache_and_resource_paths(self) -> None:
        temporary, _, output, capture, result = self.run_script()
        try:
            self.assertEqual(result.returncode, 0, result.stderr)
            rows = capture.read_text().splitlines()
            self.assertEqual(len(rows), len(DATASETS))
            for dataset, row in zip(DATASETS, rows, strict=True):
                dataset_path, uri, output_dir, cache = row.split("|")
                self.assertTrue(dataset_path.endswith(dataset))
                self.assertEqual(
                    uri,
                    f"s3://test-bucket/bench/full-s3/fresh-r1/{dataset}/srht-pq-scan",
                )
                self.assertEqual(output_dir, str(output / dataset))
                self.assertEqual(cache, str(output / dataset / "cache"))
                self.assertFalse((output / dataset / "cache").exists())
                self.assertFalse((output / dataset / "scratch").exists())
            with (output / "coverage.csv").open() as handle:
                coverage = list(csv.DictReader(handle))
            self.assertEqual({row["status"] for row in coverage}, {"measured"})
            self.assertEqual(len({row["index_uri"] for row in coverage}), len(DATASETS))
        finally:
            temporary.cleanup()

    def test_a_dataset_failure_is_reported_after_remaining_datasets_run(self) -> None:
        temporary, _, _, capture, result = self.run_script("glove-100")
        try:
            self.assertNotEqual(result.returncode, 0)
            self.assertEqual(len(capture.read_text().splitlines()), len(DATASETS))
        finally:
            temporary.cleanup()

    def test_explicit_dataset_order_is_applied(self) -> None:
        selected = ["gist-960", "fashion-mnist-784", "sift-128"]
        temporary, _, _, capture, result = self.run_script(
            dataset_names=" ".join(selected)
        )
        try:
            self.assertEqual(result.returncode, 0, result.stderr)
            observed = [
                Path(row.split("|", 1)[0]).name
                for row in capture.read_text().splitlines()
            ]
            self.assertEqual(observed, selected)
        finally:
            temporary.cleanup()

    def test_paid_run_requires_explicit_gate(self) -> None:
        env = os.environ.copy()
        env.update({"BORSUK_S3_BUCKET": "s3://test-bucket/bench"})
        env.pop("BORSUK_RUN_FULL_S3", None)
        result = subprocess.run(
            [str(SCRIPT)], cwd=REPO_ROOT, env=env, text=True, capture_output=True
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("BORSUK_RUN_FULL_S3=1", result.stderr)

    def test_runner_samples_resources_and_never_uses_cargo_run(self) -> None:
        source = SCRIPT.read_text()
        self.assertIn("benchmark_with_resources.py", source)
        self.assertIn('BORSUK_BENCH_UNCACHED_QUERIES="$UNCACHED_QUERIES"', source)
        self.assertIn("render_recall_latency_charts.py", source)
        self.assertIn("render_resource_charts.py", source)
        self.assertIn("target/release/examples/production_bench", source)
        self.assertNotIn("cargo run", source)

    def test_recall_only_profile_skips_exact_and_reduces_required_artifacts(
        self,
    ) -> None:
        source = SCRIPT.read_text()
        self.assertIn("BORSUK_FULL_RECALL_ONLY", source)
        self.assertIn("BORSUK_FULL_SKIP_EXACT_RECALL", source)
        self.assertIn('BORSUK_BENCH_RECALL_ONLY="$RECALL_ONLY"', source)
        self.assertIn('BORSUK_BENCH_SKIP_EXACT_RECALL="$SKIP_EXACT_RECALL"', source)
        self.assertIn("RECALL_ONLY_REQUIRED", source)

    def test_explicit_dataset_order_and_post_evidence_cleanup_are_supported(
        self,
    ) -> None:
        source = SCRIPT.read_text()
        self.assertIn("BORSUK_FULL_DATASET_NAMES", source)
        self.assertIn("BORSUK_FULL_KEEP_CASE_DATA", source)
        cleanup = source.index('rm -rf "$cache_dir" "$scratch_dir"')
        self.assertLess(source.index("validate_benchmark_artifacts.py"), cleanup)
        self.assertLess(source.index('>> "$OUT/coverage.csv"'), cleanup)


if __name__ == "__main__":
    unittest.main()
