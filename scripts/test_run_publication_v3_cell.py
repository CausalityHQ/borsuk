import json
import tempfile
import unittest
from pathlib import Path

from scripts.publication_v3_protocol import build_schedule_document, validate_manifest
from scripts.run_publication_v3_cell import (
    build_smoke_report,
    build_execution_plan,
    execute_plan,
    plan_arms,
    summarize_query_samples,
)
from scripts.test_publication_v3_protocol import paid_v3_manifest


def scheduled_cell(*, system: str = "borsuk", kind: str = "read-recall") -> dict[str, object]:
    manifest = validate_manifest(paid_v3_manifest())
    return next(
        cell
        for cell in build_schedule_document(manifest)["cells"]
        if cell["system"] == system and cell["workload"]["kind"] == kind
    )


class PublicationV3CellRunnerTests(unittest.TestCase):
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
        self.assertEqual(int(generator_env["BORSUK_SYNTHETIC_TRAIN"]), 1_000)
        self.assertEqual(int(generator_env["BORSUK_SYNTHETIC_DIMENSIONS"]), cell["dataset"]["dimensions"])
        self.assertEqual(benchmark_env["BORSUK_BENCH_QUERY_SEED"], str(cell["query_seed"]))
        self.assertEqual(benchmark_env["BORSUK_BENCH_QUERIES"], "10")
        self.assertEqual(benchmark_env["BORSUK_BENCH_NPROBES"], str(arm["routing_cell_budget"]))
        self.assertEqual(benchmark_env["BORSUK_BENCH_CANDIDATES"], str(arm["candidate_budget"]))
        self.assertEqual(benchmark_env["BORSUK_BENCH_SKIP_EXACT_RECALL"], "1")

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
        self.assertEqual(plan["effective_rows"], 1_000)
        self.assertEqual(plan["effective_queries"], 10)
        self.assertEqual(plan["steps"][0]["env"]["BORSUK_SYNTHETIC_TRAIN"], "1000")
        self.assertEqual(plan["steps"][1]["env"]["BORSUK_BENCH_QUERIES"], "10")
        with self.assertRaisesRegex(ValueError, "index-profile binding"):
            build_execution_plan(
                cell,
                arm=arm,
                workspace=Path(root),
                generator=Path("/bin/true"),
                borsuk_bench=Path("/bin/true"),
                mode="publication",
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
