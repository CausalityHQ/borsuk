#!/usr/bin/env python3
"""Tests for the executable market benchmark planner and dataset gate."""

from __future__ import annotations

import csv
import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import market_benchmark_runner as runner


class MarketBenchmarkRunnerTest(unittest.TestCase):
    def test_plan_expands_repetitions_cache_coverage_and_concurrency_profiles(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            matrix = root / "matrix.csv"
            matrix.write_text(
                "goal,dataset,scale,dimensions,workload,quality_metrics,latency_metrics,"
                "resource_metrics,cache_profiles,comparison_context,status,source\n"
                "dense,demo,10,2,dense_ann,recall_at_10,"
                "p50_ms;p95_ms;p99_ms;mean_ms;stddev_ms;qps,"
                "peak_rss_bytes;mean_cpu_percent,uncached;disk_cached;mixed_coverage,"
                "control,planned,https://example.invalid/demo\n"
            )
            dataset_root = root / "datasets"
            self._write_dataset(dataset_root / "demo")
            output = root / "out"

            plan = runner.build_plan(
                matrix_path=matrix,
                dataset_root=dataset_root,
                output_root=output,
                run_id="run-001",
                repetitions=3,
                selected_datasets=None,
                bucket="s3://bucket/prefix",
            )

            self.assertEqual(len(plan), 42)
            self.assertEqual({row["repetition"] for row in plan}, {"1", "2", "3"})
            self.assertEqual(
                {row["concurrency_profile"] for row in plan},
                {"production", "research_ceiling"},
            )
            mixed = [row for row in plan if row["cache_profile"] == "mixed_coverage"]
            self.assertEqual(
                {row["cache_coverage_percent"] for row in mixed},
                {"0", "25", "50", "75", "100"},
            )
            # One immutable fresh index per repetition; cache/query profiles use
            # independent cache/output dirs without paying 14 duplicate ingests.
            self.assertEqual(len({row["index_uri"] for row in plan}), 3)
            self.assertEqual(len({row["output_dir"] for row in plan}), len(plan))
            self.assertTrue(all(row["status"] == "ready" for row in plan))

    def test_missing_or_tampered_dataset_is_blocked_with_a_specific_reason(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            dataset = root / "demo"
            self._write_dataset(dataset)
            descriptor = json.loads((dataset / "dataset.json").read_text())
            descriptor["files"][0]["sha256"] = "0" * 64
            (dataset / "dataset.json").write_text(json.dumps(descriptor))

            reason = runner.validate_dataset(dataset, "demo", "dense_ann", 2, "10")

            self.assertIn("sha256 mismatch", reason)

    def test_lifecycle_plan_runs_writes_once_after_all_cache_profiles(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            matrix = root / "matrix.csv"
            matrix.write_text(
                "goal,dataset,scale,dimensions,workload,quality_metrics,latency_metrics,"
                "resource_metrics,cache_profiles,comparison_context,status,source\n"
                "write_to_serve,demo,10,2,insert_search_under_load,recall_at_10,"
                "time_to_searchable_ms,peak_rss_bytes,uncached;disk_cached;mixed_coverage,"
                "control,planned,https://example.invalid/demo\n"
            )
            dataset = root / "datasets" / "demo"
            self._write_dataset(dataset)
            descriptor = json.loads((dataset / "dataset.json").read_text())
            descriptor["workload"] = "insert_search_under_load"
            descriptor["adapter"] = "borsuk_lifecycle"
            (dataset / "dataset.json").write_text(json.dumps(descriptor))

            plan = runner.build_plan(
                matrix_path=matrix,
                dataset_root=root / "datasets",
                output_root=root / "out",
                run_id="run",
                repetitions=1,
                selected_datasets=None,
                bucket="s3://bucket/prefix",
            )

            self.assertEqual(len(plan), 15)
            self.assertEqual(plan[-1]["cache_profile"], "lifecycle")
            self.assertEqual(plan[-1]["traffic_class"], "writes")
            self.assertTrue(
                all(row["cache_profile"] != "lifecycle" for row in plan[:-1])
            )

    def test_written_plan_is_stable_csv_and_refuses_overwrite(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "plan.csv"
            rows = [
                {column: f"{column}-value" for column in runner.PLAN_COLUMNS},
            ]
            runner.write_plan(path, rows)
            with path.open(newline="") as handle:
                parsed = list(csv.DictReader(handle))
            self.assertEqual(parsed, rows)
            with self.assertRaises(FileExistsError):
                runner.write_plan(path, rows)

    def test_execute_builds_each_index_once_and_every_measurement_is_read_only(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            plan_path = root / "plan.csv"
            rows = []
            for ordinal, profile in enumerate(("uncached", "disk_cached")):
                row = {column: "" for column in runner.PLAN_COLUMNS}
                row.update(
                    {
                        "run_id": "run",
                        "dataset": "demo",
                        "adapter": "borsuk_dense_ann",
                        "repetition": "1",
                        "cache_profile": profile,
                        "cache_coverage_percent": "0" if ordinal == 0 else "100",
                        "concurrency_profile": "production",
                        "status": "ready",
                        "dataset_dir": str(root / "dataset"),
                        "index_uri": "s3://bucket/run/demo/r01",
                        "output_dir": str(root / f"output-{ordinal}"),
                    }
                )
                rows.append(row)
            runner.write_plan(plan_path, rows)

            with mock.patch.object(runner.subprocess, "run") as run:
                runner.execute_plan(
                    plan_path,
                    repo_root=root,
                    allow_paid_execution=True,
                )

            self.assertEqual(run.call_count, 2)
            environments = [call.kwargs["env"] for call in run.call_args_list]
            self.assertEqual(
                [
                    environment["BORSUK_BENCH_BUILD_INDEX"]
                    for environment in environments
                ],
                ["1", "0"],
            )
            self.assertTrue(
                all(
                    environment["BORSUK_BENCH_READ_ONLY"] == "1"
                    for environment in environments
                )
            )
            self.assertEqual(
                [
                    environment["BORSUK_BENCH_CACHE_PROFILE"]
                    for environment in environments
                ],
                ["uncached", "disk_cached"],
            )

    def test_read_plan_rejects_wrong_header(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "bad.csv"
            path.write_text("wrong\nvalue\n")
            with self.assertRaisesRegex(ValueError, "invalid plan header"):
                runner.read_plan(path)

    def test_hybrid_adapter_builds_once_then_queries_with_exact_cache_mix(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            plan_path = root / "plan.csv"
            row = {column: "" for column in runner.PLAN_COLUMNS}
            row.update(
                {
                    "run_id": "run",
                    "dataset": "beir-fiqa",
                    "workload": "dense_sparse",
                    "adapter": "borsuk_hybrid",
                    "repetition": "1",
                    "cache_profile": "mixed_coverage",
                    "cache_coverage_percent": "25",
                    "concurrency_profile": "research_ceiling",
                    "status": "ready",
                    "dataset_dir": str(root / "dataset"),
                    "index_uri": "s3://bucket/run/beir-fiqa/r01",
                    "output_dir": str(
                        root
                        / "out"
                        / "run"
                        / "beir-fiqa"
                        / "r01"
                        / "mixed_coverage"
                        / "coverage-025"
                        / "research_ceiling"
                    ),
                }
            )
            runner.write_plan(plan_path, [row])

            with mock.patch.object(runner.subprocess, "run") as run:
                runner.execute_plan(
                    plan_path,
                    repo_root=root,
                    allow_paid_execution=True,
                )

            self.assertEqual(run.call_count, 2)
            build, query = run.call_args_list
            self.assertEqual(build.args[0][-1], "build")
            self.assertEqual(query.args[0][-1], "query")
            query_env = query.kwargs["env"]
            self.assertEqual(query_env["BORSUK_HYBRID_MODES"], "dense+sparse")
            self.assertEqual(query_env["BORSUK_HYBRID_TARGET_HOT_FRACTION"], "0.25")
            self.assertEqual(query_env["BORSUK_HYBRID_CLIENT_CONCURRENCY"], "32")
            self.assertEqual(query_env["BORSUK_HYBRID_PRIME_TARGET_HOT_SET"], "1")

    def test_lifecycle_adapter_mutates_only_the_fresh_build_measurement(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            plan_path = root / "plan.csv"
            rows = []
            for ordinal, profile in enumerate(("uncached", "disk_cached", "lifecycle")):
                row = {column: "" for column in runner.PLAN_COLUMNS}
                row.update(
                    {
                        "run_id": "run",
                        "dataset": "laion-100M",
                        "adapter": "borsuk_lifecycle",
                        "repetition": "1",
                        "cache_profile": profile,
                        "cache_coverage_percent": "100" if ordinal == 1 else "0",
                        "concurrency_profile": "production",
                        "status": "ready",
                        "dataset_dir": str(root / "dataset"),
                        "index_uri": "s3://bucket/run/laion-100M/r01",
                        "output_dir": str(root / f"output-{ordinal}"),
                    }
                )
                rows.append(row)
            runner.write_plan(plan_path, rows)

            with mock.patch.object(runner.subprocess, "run") as run:
                runner.execute_plan(
                    plan_path,
                    repo_root=root,
                    allow_paid_execution=True,
                )

            environments = [call.kwargs["env"] for call in run.call_args_list]
            self.assertEqual(
                [
                    environment["BORSUK_BENCH_BUILD_INDEX"]
                    for environment in environments
                ],
                ["1", "0", "0"],
            )
            self.assertEqual(
                [environment["BORSUK_BENCH_READ_ONLY"] for environment in environments],
                ["1", "1", "0"],
            )
            self.assertEqual(
                [
                    environment["BORSUK_BENCH_CACHE_PROFILE"]
                    for environment in environments
                ],
                ["uncached", "disk_cached", "uncached"],
            )

    def test_specialized_adapters_build_once_then_run_measured_cache_profiles(
        self,
    ) -> None:
        adapter_cases = [
            ("borsuk_filter", "filter"),
            ("borsuk_namespace", "namespace"),
            ("borsuk_late_interaction", "late-interaction"),
        ]
        for adapter, workload in adapter_cases:
            with self.subTest(adapter=adapter), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                plan_path = root / "plan.csv"
                rows = []
                for ordinal, (profile, coverage) in enumerate(
                    (("uncached", "0"), ("mixed_coverage", "50"))
                ):
                    row = {column: "" for column in runner.PLAN_COLUMNS}
                    row.update(
                        {
                            "run_id": "run",
                            "dataset": "demo",
                            "adapter": adapter,
                            "repetition": "1",
                            "cache_profile": profile,
                            "cache_coverage_percent": coverage,
                            "concurrency_profile": (
                                "production" if ordinal == 0 else "research_ceiling"
                            ),
                            "status": "ready",
                            "dataset_dir": str(root / "dataset"),
                            "index_uri": "s3://bucket/run/demo/r01",
                            "output_dir": str(
                                root
                                / "out"
                                / "run"
                                / "demo"
                                / "r01"
                                / profile
                                / f"coverage-{int(coverage):03d}"
                                / ("production" if ordinal == 0 else "research_ceiling")
                            ),
                        }
                    )
                    rows.append(row)
                runner.write_plan(plan_path, rows)

                with (
                    mock.patch.object(runner.subprocess, "run") as run,
                    mock.patch.object(runner, "validate_directory") as validate,
                ):
                    runner.execute_plan(
                        plan_path,
                        repo_root=root,
                        allow_paid_execution=True,
                    )

                self.assertEqual(run.call_count, 3)
                build, uncached, mixed = run.call_args_list
                self.assertEqual(build.args[0][-2:], [workload, "build"])
                self.assertEqual(uncached.args[0][-2:], [workload, "query"])
                self.assertEqual(mixed.args[0][-2:], [workload, "query"])
                self.assertEqual(
                    uncached.kwargs["env"]["BORSUK_MARKET_CACHE_PROFILE"],
                    "uncached",
                )
                self.assertEqual(
                    mixed.kwargs["env"]["BORSUK_MARKET_CACHE_COVERAGE_PERCENT"],
                    "50",
                )
                self.assertEqual(
                    mixed.kwargs["env"]["BORSUK_MARKET_CLIENT_CONCURRENCY"],
                    "32",
                )
                self.assertEqual(validate.call_count, 3)
                expected_prefix = workload.replace("-", "_")
                self.assertIn(
                    f"{expected_prefix}_build.csv",
                    validate.call_args_list[0].args[2],
                )

    @staticmethod
    def _write_dataset(path: Path) -> None:
        path.mkdir(parents=True)
        payload = b"prepared vectors"
        vector_file = path / "train.f32"
        vector_file.write_bytes(payload)
        digest = hashlib.sha256(payload).hexdigest()
        (path / "dataset.json").write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "dataset": "demo",
                    "workload": "dense_ann",
                    "dimensions": 2,
                    "scale": "10",
                    "source": "https://example.invalid/demo",
                    "source_sha256": "1" * 64,
                    "license": "test-only",
                    "adapter": "borsuk_dense_ann",
                    "files": [
                        {
                            "path": "train.f32",
                            "bytes": len(payload),
                            "sha256": digest,
                        }
                    ],
                }
            )
        )


if __name__ == "__main__":
    unittest.main()
