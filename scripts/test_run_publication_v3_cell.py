import json
import tempfile
import unittest
from pathlib import Path

from scripts.publication_v3_protocol import (
    build_schedule_document,
    canonical_json_bytes,
    validate_manifest,
)
from scripts.run_publication_v3_cell import (
    build_publication_report,
    build_resource_metrics,
    build_smoke_report,
    build_execution_plan,
    execute_plan,
    execute_plan_with_resources,
    plan_arms,
    read_build_storage_metrics,
    summarize_query_samples,
)
from scripts.publication_v3_results import validate_cell_result
from scripts.test_publication_v3_protocol import paid_v3_manifest


def scheduled_cell(*, system: str = "borsuk", kind: str = "read-recall") -> dict[str, object]:
    manifest = validate_manifest(paid_v3_manifest())
    return next(
        cell
        for cell in build_schedule_document(manifest)["cells"]
        if cell["system"] == system and cell["workload"]["kind"] == kind
    )


class PublicationV3CellRunnerTests(unittest.TestCase):
    def test_build_storage_metrics_are_read_from_real_benchmark_artifact(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            output = Path(root)
            (output / "bench_build.csv").write_text(
                "storage_gets,storage_puts,storage_deletes,storage_heads,storage_lists,storage_bytes_read,storage_bytes_written\n"
                "7,11,0,3,2,654321,123456\n",
                encoding="utf-8",
            )
            metrics = read_build_storage_metrics(output)
        self.assertEqual(metrics["storage_puts"], 11)
        self.assertEqual(metrics["storage_bytes_read"], 654321)
        self.assertEqual(metrics["storage_bytes_written"], 123456)
        resource = build_resource_metrics(
            {
                "cpu_ns": 10,
                "peak_rss_bytes": 20,
                "disk_read_bytes": 30,
                "disk_write_bytes": 40,
            },
            metrics,
        )
        self.assertEqual(
            frozenset(resource),
            frozenset(
                {
                    "cpu_ns",
                    "peak_rss_bytes",
                    "disk_read_bytes",
                    "disk_write_bytes",
                    "storage_gets",
                    "storage_puts",
                    "storage_bytes_read",
                    "storage_bytes_written",
                }
            ),
        )

    def test_execution_records_child_cpu_rss_disk_and_elapsed_resources(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            workspace = Path(root)
            output = workspace / "output"
            command = (
                "mkdir -p output; "
                "printf 'schema_version\\nreal\\n' > output/bench_query_samples.csv"
            )
            samples, resources, elapsed_ns = execute_plan_with_resources(
                {
                    "workspace": str(workspace),
                    "output_dir": str(output),
                    "steps": [{"argv": ["/bin/sh", "-c", command], "env": {}}],
                }
            )
        self.assertEqual(samples.name, "bench_query_samples.csv")
        self.assertGreater(elapsed_ns, 0)
        self.assertGreater(resources["cpu_ns"], 0)
        self.assertGreater(resources["peak_rss_bytes"], 0)
        self.assertGreaterEqual(resources["disk_read_bytes"], 0)
        self.assertGreaterEqual(resources["disk_write_bytes"], 0)

    def test_publication_report_is_a_complete_admissible_result(self) -> None:
        cell = scheduled_cell()
        protocol = canonical_json_bytes(cell) + b"\n"
        rows = cell["dataset"]["scale"]["rows"]
        object_roster = [
            {
                "role": "data-bundle",
                "path": "segments/0000.parquet",
                "format": "parquet",
                "bytes": 64 * 1024 * 1024,
                "rows": rows,
                "checksum": "1" * 64,
            }
        ]
        if rows >= 10_000_000:
            object_roster[0]["rows"] = rows // 2
            object_roster.append(
                {
                    **object_roster[0],
                    "path": "segments/0001.parquet",
                    "rows": rows - rows // 2,
                    "checksum": "2" * 64,
                }
            )
        report = build_publication_report(
            cell=cell,
            arm={
                "k": 10,
                "candidate_budget": 128,
                "routing_cell_budget": 32,
                "cache_state": "cold",
            },
            protocol_bytes=protocol,
            source_archive_sha256="a" * 64,
            attempt_id="attempt-01",
            instance_identity="i-0123456789abcdef0",
            elapsed_ns=2_000_000_000,
            query_metrics={
                "queries": 1_000,
                "correctness_ppm": 960_000,
                "latency_p50_us": 1_000,
                "latency_p95_us": 2_000,
                "latency_p99_us": 3_000,
                "storage_gets": 10,
                "storage_bytes_read": 4096,
                "query_elapsed_ns": 1_000_000_000,
            },
            resource_metrics={
                "cpu_ns": 1_500_000_000,
                "peak_rss_bytes": 256 * 1024 * 1024,
                "disk_read_bytes": 8192,
                "disk_write_bytes": 16384,
                "storage_gets": 7,
                "storage_puts": 12,
                "storage_bytes_read": 2048,
                "storage_bytes_written": 32768,
            },
            object_roster=object_roster,
        )
        self.assertTrue(report["publishable"])
        self.assertEqual(report["result"]["arm"]["candidate_budget"], 128)
        self.assertEqual(report["result"]["metrics"]["storage_gets"], 17)
        self.assertEqual(report["result"]["metrics"]["storage_bytes_read"], 6144)
        admitted = validate_cell_result(
            report["result"],
            cell=cell,
            protocol_bytes=protocol,
            source_archive_sha256="a" * 64,
        )
        self.assertEqual(admitted, report["result"])

    def test_borsuk_read_smoke_plan_invokes_real_generator_and_production_bench(self) -> None:
        cell = next(
            cell
            for cell in build_schedule_document(validate_manifest(paid_v3_manifest()))["cells"]
            if cell["system"] == "borsuk"
            and cell["workload"]["kind"] == "read-recall"
            and cell["dataset"]["source"].get("generator") == "synthetic-clustered-v1"
        )
        with tempfile.TemporaryDirectory() as root:
            arm = plan_arms(cell)[0]
            plan = build_execution_plan(
                cell,
                arm=arm,
                workspace=Path(root),
                generator=Path("/opt/borsuk/generate_synthetic_dataset"),
                borsuk_bench=Path("/opt/borsuk/production_bench"),
                mode="smoke",
            )
        self.assertFalse(plan["publishable"])
        self.assertEqual(
            [step["argv"] for step in plan["steps"]],
            [
                ["/opt/borsuk/generate_synthetic_dataset"],
                ["/opt/borsuk/production_bench"],
            ],
        )
        generator_env = plan["steps"][0]["env"]
        benchmark_env = plan["steps"][1]["env"]
        self.assertEqual(int(generator_env["BORSUK_SYNTHETIC_TRAIN"]), 32_800)
        self.assertEqual(int(generator_env["BORSUK_SYNTHETIC_DIMENSIONS"]), cell["dataset"]["dimensions"])
        self.assertEqual(
            generator_env["BORSUK_SYNTHETIC_SEED"],
            str(cell["dataset"]["source"]["seed"]),
        )
        self.assertEqual(benchmark_env["BORSUK_BENCH_QUERY_SEED"], str(cell["query_seed"]))
        self.assertEqual(benchmark_env["BORSUK_BENCH_QUERIES"], "10")
        self.assertEqual(benchmark_env["BORSUK_BENCH_NPROBES"], str(arm["routing_cell_budget"]))
        self.assertEqual(benchmark_env["BORSUK_BENCH_CANDIDATES"], str(arm["candidate_budget"]))
        self.assertEqual(benchmark_env["BORSUK_BENCH_SKIP_EXACT_RECALL"], "1")
        self.assertEqual(benchmark_env["BORSUK_BENCH_LOGICAL_CELLS"], "128")
        self.assertEqual(benchmark_env["BORSUK_BENCH_LOGICAL_CELL_TRAINING_ROWS"], "4096")
        self.assertEqual(benchmark_env["BORSUK_BENCH_LOGICAL_CELL_ITERATIONS"], "8")
        self.assertEqual(benchmark_env["BORSUK_BENCH_GLOBAL_PQ_CODE_BYTES"], "128")

    def test_smoke_plan_is_scaled_and_cannot_be_published(self) -> None:
        cell = next(
            cell
            for cell in build_schedule_document(validate_manifest(paid_v3_manifest()))["cells"]
            if cell["system"] == "borsuk"
            and cell["workload"]["kind"] == "read-recall"
            and cell["dataset"]["source"].get("generator") == "synthetic-clustered-v1"
        )
        with tempfile.TemporaryDirectory() as root:
            arm = plan_arms(cell)[0]
            plan = build_execution_plan(
                cell,
                arm=arm,
                workspace=Path(root),
                generator=Path("/bin/true"),
                borsuk_bench=Path("/bin/true"),
                mode="smoke",
            )
        self.assertFalse(plan["publishable"])
        self.assertEqual(plan["effective_rows"], 32_800)
        self.assertEqual(plan["effective_queries"], 10)
        self.assertEqual(plan["steps"][0]["env"]["BORSUK_SYNTHETIC_TRAIN"], "32800")
        self.assertEqual(plan["steps"][1]["env"]["BORSUK_BENCH_QUERIES"], "10")
        cell["source"] = {"state": "frozen"}
        publication = build_execution_plan(
            cell,
            arm=arm,
            workspace=Path(root),
            generator=Path("/bin/true"),
            borsuk_bench=Path("/bin/true"),
            mode="publication",
        )
        self.assertTrue(publication["publishable"])
        self.assertEqual(publication["effective_rows"], cell["dataset"]["scale"]["rows"])
        self.assertEqual(
            publication["steps"][-1]["env"]["BORSUK_BENCH_LOGICAL_CELLS"],
            str(cell["index_profile"]["logical_cells"]),
        )

    def test_unavailable_local_system_is_rejected_not_simulated(self) -> None:
        cell = scheduled_cell(system="amazon-s3-vectors")
        with tempfile.TemporaryDirectory() as root, self.assertRaisesRegex(
            ValueError, "not available in local execution"
        ):
            build_execution_plan(
                cell,
                arm={},
                workspace=Path(root),
                generator=Path("/bin/true"),
                borsuk_bench=Path("/bin/true"),
                mode="smoke",
            )

    def test_execution_rejects_successful_processes_without_real_query_artifacts(self) -> None:
        cell = next(
            cell
            for cell in build_schedule_document(validate_manifest(paid_v3_manifest()))["cells"]
            if cell["system"] == "borsuk"
            and cell["workload"]["kind"] == "read-recall"
            and cell["dataset"]["source"].get("generator") == "synthetic-clustered-v1"
        )
        with tempfile.TemporaryDirectory() as root:
            arm = plan_arms(cell)[0]
            plan = build_execution_plan(
                cell,
                arm=arm,
                workspace=Path(root),
                generator=Path("/bin/true"),
                borsuk_bench=Path("/bin/true"),
                mode="smoke",
            )
            with self.assertRaisesRegex(ValueError, "query sample artifact"):
                execute_plan(plan)

    def test_query_summary_uses_every_real_sample_and_quality_floor(self) -> None:
        cell = scheduled_cell()
        rows = []
        for index, (latency, recall) in enumerate(((1.0, 0.96), (2.0, 0.95), (4.0, 0.99))):
            rows.append(
                {
                    "schema_version": "borsuk-production-bench-v10",
                    "sample_index": str(index),
                    "latency_ms": str(latency),
                    "recall_at_10": str(recall),
                    "network_gets": str(index + 1),
                    "bytes_read": str((index + 1) * 100),
                }
            )
        arm = {
            "k": 10,
            "candidate_budget": 128,
            "routing_cell_budget": 32,
            "cache_state": "cold",
        }
        for row in rows:
            row.update(
                {
                    "phase": "uncached",
                    "mode": "srht-pq-scan",
                    "nprobe": "32",
                    "max_candidates": "128",
                }
            )
        summary = summarize_query_samples(rows, cell=cell, arm=arm, expected_queries=3)
        self.assertEqual(summary["queries"], 3)
        self.assertEqual(summary["correctness_ppm"], 966667)
        self.assertEqual(summary["latency_p50_us"], 2000)
        self.assertEqual(summary["latency_p95_us"], 4000)
        self.assertEqual(summary["latency_p99_us"], 4000)
        self.assertEqual(summary["storage_gets"], 6)
        self.assertEqual(summary["storage_bytes_read"], 600)
        self.assertEqual(summary["query_elapsed_ns"], 7_000_000)

        bad = json.loads(json.dumps(rows))
        for row in bad:
            row["recall_at_10"] = "0.94"
        with self.assertRaisesRegex(ValueError, "quality floor"):
            summarize_query_samples(bad, cell=cell, arm=arm, expected_queries=3)
        smoke = summarize_query_samples(
            bad,
            cell=cell,
            arm=arm,
            expected_queries=3,
            enforce_quality=False,
        )
        self.assertEqual(smoke["correctness_ppm"], 940000)

    def test_read_arms_expand_one_declared_axis_without_cross_product_aliasing(self) -> None:
        cell = scheduled_cell()
        cell["workload"]["factors"] = {
            "k": [10],
            "candidate_budgets": [64, 256],
            "routing_cell_budget": 32,
            "cache_states": ["cold", "warm"],
            "minimum_recall_ppm": 950000,
        }
        self.assertEqual(
            plan_arms(cell),
            [
                {"k": 10, "candidate_budget": 64, "routing_cell_budget": 32, "cache_state": "cold"},
                {"k": 10, "candidate_budget": 64, "routing_cell_budget": 32, "cache_state": "warm"},
                {"k": 10, "candidate_budget": 256, "routing_cell_budget": 32, "cache_state": "cold"},
                {"k": 10, "candidate_budget": 256, "routing_cell_budget": 32, "cache_state": "warm"},
            ],
        )

        cell["workload"]["factors"]["k"] = [10, 100]
        with self.assertRaisesRegex(ValueError, "k=100"):
            plan_arms(cell)

    def test_smoke_report_is_distinct_from_a_publishable_cell_result(self) -> None:
        cell = scheduled_cell()
        arm = {
            "k": 10,
            "candidate_budget": 128,
            "routing_cell_budget": 32,
            "cache_state": "cold",
        }
        report = build_smoke_report(
            cell=cell,
            arm=arm,
            effective_rows=1_000,
            effective_queries=10,
            metrics={"queries": 10, "correctness_ppm": 920000},
            protocol_sha256="a" * 64,
        )
        self.assertEqual(report["document_kind"], "publication-v3-smoke")
        self.assertFalse(report["publishable"])
        self.assertNotIn("object_roster", report)


if __name__ == "__main__":
    unittest.main()
