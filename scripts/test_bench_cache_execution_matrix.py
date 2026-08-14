import csv
import os
import subprocess
import tempfile
import unittest
from pathlib import Path

SCRIPT = Path(__file__).with_name("bench_cache_execution_matrix.sh")
DATASETS = (
    "fashion-mnist-784",
    "glove-100",
    "sift-128",
    "nytimes-256",
    "gist-960",
    "deep-image-96",
)
STORAGE_CODECS = ("pq-scan", "srht-pq-scan", "fast-turboquant-scan")
PROFILES = {
    "production-scan": (
        "production",
        "scan",
        "pq-scan-only",
        "0",
        "4",
        "24",
        "536870912",
    ),
    "production-auto-64m": (
        "production",
        "auto",
        "graph-enabled",
        str(64 * 1024 * 1024),
        "4",
        "24",
        "536870912",
    ),
    "production-auto-128m": (
        "production",
        "auto",
        "graph-enabled",
        str(128 * 1024 * 1024),
        "4",
        "24",
        "536870912",
    ),
    "production-auto-256m": (
        "production",
        "auto",
        "graph-enabled",
        str(256 * 1024 * 1024),
        "4",
        "24",
        "536870912",
    ),
    "production-auto-512m": (
        "production",
        "auto",
        "graph-enabled",
        str(512 * 1024 * 1024),
        "4",
        "24",
        "536870912",
    ),
    "research-auto-uncapped-512m": (
        "research-ceiling",
        "auto",
        "graph-enabled",
        str(512 * 1024 * 1024),
        "1024",
        "1024",
        "0",
    ),
}


class CacheExecutionMatrixRunnerTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.datasets = self.root / "datasets"
        self.output = self.root / "output"
        for dataset in DATASETS:
            directory = self.datasets / dataset
            directory.mkdir(parents=True)
            (directory / "meta.json").write_text('{"dim": 128}')

    def tearDown(self):
        self.temp.cleanup()

    def run_script(self, **extra):
        env = os.environ.copy()
        env.update(
            {
                "DATASETS": str(self.datasets),
                "OUT": str(self.output),
                "BORSUK_S3_BUCKET": "s3://example/research",
            }
        )
        env.update(extra)
        return subprocess.run(
            ["bash", str(SCRIPT)],
            cwd=SCRIPT.parent.parent,
            env=env,
            text=True,
            capture_output=True,
        )

    def test_dry_run_defines_bounded_production_and_uncapped_research_controls(self):
        completed = self.run_script()
        self.assertEqual(completed.returncode, 0, completed.stderr)
        with (self.output / "coverage.csv").open() as handle:
            rows = list(csv.DictReader(handle))
        self.assertEqual(len(rows), len(DATASETS) * len(STORAGE_CODECS) * len(PROFILES))
        self.assertEqual({row["profile"] for row in rows}, set(PROFILES))
        self.assertEqual({row["scan_codec"] for row in rows}, set(STORAGE_CODECS))
        for row in rows:
            (
                profile_class,
                policy,
                capability,
                graph_cache,
                active_search_cap,
                inflight_leaf_cap,
                ram_budget,
            ) = PROFILES[row["profile"]]
            self.assertEqual(
                (
                    row["profile_class"],
                    row["cache_execution"],
                    row["leaf_capability"],
                    row["global_graph_cache_max_bytes"],
                    row["max_active_searches"],
                    row["max_inflight_leaf_reads"],
                    row["ram_budget_bytes"],
                ),
                (
                    profile_class,
                    policy,
                    capability,
                    graph_cache,
                    active_search_cap,
                    inflight_leaf_cap,
                    ram_budget,
                ),
            )
            self.assertEqual(row["uncached_expected_engine"], row["scan_codec"])
            self.assertEqual(
                row["disk_cached_expected_engine"],
                row["scan_codec"] if policy == "scan" else "graph-or-mixed",
            )
            self.assertEqual(
                row["global_cell_graph_degree"], "0" if policy == "scan" else "16"
            )
            self.assertTrue(row["resource_path"].endswith("resources.csv"))
            self.assertTrue(
                row["cache_coverage_path"].endswith("bench_cache_coverage.csv")
            )

    def test_profiles_use_distinct_fresh_artifacts_and_all_datasets(self):
        completed = self.run_script()
        self.assertEqual(completed.returncode, 0, completed.stderr)
        with (self.output / "coverage.csv").open() as handle:
            rows = list(csv.DictReader(handle))
        for dataset in DATASETS:
            for codec in STORAGE_CODECS:
                selected = [
                    row
                    for row in rows
                    if row["dataset"] == dataset and row["scan_codec"] == codec
                ]
                self.assertEqual(
                    len({row["index_uri"] for row in selected}), len(selected)
                )
                self.assertEqual({row["dataset"] for row in selected}, {dataset})

    def test_requires_explicit_paid_run_flag(self):
        completed = self.run_script(BORSUK_CACHE_MATRIX_EXECUTE="1")
        self.assertNotEqual(completed.returncode, 0)
        self.assertIn("BORSUK_RUN_CACHE_MATRIX=1", completed.stderr)

    def test_runner_uses_bounded_global_admission_and_resource_sampling(self):
        source = SCRIPT.read_text()
        self.assertIn("max_active_searches='4'", source)
        self.assertIn("max_inflight_leaf_reads='24'", source)
        self.assertIn("max_active_searches='1024'", source)
        self.assertIn("max_inflight_leaf_reads='1024'", source)
        self.assertIn("BORSUK_BENCH_GLOBAL_CELL_GRAPH_CACHE_MAX_BYTES", source)
        self.assertIn("BORSUK_BENCH_GLOBAL_CELL_GRAPH_DEGREE", source)
        self.assertIn("python3 scripts/benchmark_with_resources.py", source)
        self.assertIn("render_cache_coverage_charts.py", source)
        self.assertIn("render_resource_charts.py", source)
        self.assertIn("target/release/examples/production_bench", source)
        self.assertNotIn("BORSUK_BENCH_READ_ONLY", source)
        self.assertIn("bench_cache_coverage.csv", source)

    def test_disk_cached_invariant_checks_network_gets_not_logical_bytes(self):
        source = SCRIPT.read_text()
        self.assertIn('$10 == "disk_cached" && ($26 + 0) != 0', source)
        self.assertNotIn('$10 == "disk_cached" && ($24 + 0) != 0', source)


if __name__ == "__main__":
    unittest.main()
