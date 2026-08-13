import json
import tempfile
import unittest
from pathlib import Path

from scripts.publication_v3_protocol import (
    build_schedule_document,
    canonical_json_bytes,
    validate_manifest,
)
from scripts.publication_v3_receipts import build_index_receipt, receipt_document_sha256
from scripts.publication_v3_results import validate_cell_result
from scripts.run_publication_v3_cell import (
    PRODUCTION_BUILD_FIELDS,
    authorize_publication_runtime,
    build_execution_plan,
    build_publication_report,
    build_receipt_metrics,
    build_smoke_report,
    execute_plan,
    execute_plan_with_resources,
    execute_publication_phase,
    plan_arms,
    read_build_artifact,
    summarize_query_samples,
    summarize_runtime_write_trace,
    validate_publication_cell_authority,
)
from scripts.test_publication_v3_protocol import paid_v3_manifest
from scripts.test_publication_v3_receipts import (
    build_artifact,
    build_metrics,
    data_roster,
)
from scripts.test_publication_v3_results import runtime_attestation_for


def scheduled_cell(*, system: str = "borsuk", kind: str = "read-recall") -> dict[str, object]:
    manifest = validate_manifest(paid_v3_manifest())
    return next(
        cell
        for cell in build_schedule_document(manifest)["cells"]
        if cell["system"] == system and cell["workload"]["kind"] == kind
    )


class PublicationV3CellRunnerTests(unittest.TestCase):
    def test_publication_cell_must_match_the_frozen_manifest_prefix_authority(self) -> None:
        cell = scheduled_cell()
        with tempfile.TemporaryDirectory() as root:
            manifest_path = Path(root) / "manifest.json"
            manifest_path.write_bytes(
                canonical_json_bytes(validate_manifest(paid_v3_manifest())) + b"\n"
            )
            self.assertEqual(validate_publication_cell_authority(cell, manifest_path), cell)
            substituted = {
                **cell,
                "index_prefix": "s3://attacker-bucket/substituted/"
                + cell["index_prefix"].rsplit("/", 1)[1],
            }
            with self.assertRaisesRegex(ValueError, "frozen manifest"):
                validate_publication_cell_authority(substituted, manifest_path)

    def test_publication_runtime_requires_matching_immutable_build_receipt(self) -> None:
        cell = scheduled_cell()
        with tempfile.TemporaryDirectory() as root:
            plan = build_execution_plan(
                cell,
                arm=plan_arms(cell)[0],
                workspace=Path(root),
                generator=Path("/bin/true"),
                borsuk_bench=Path("/bin/true"),
                mode="publication",
            )
        receipt = build_index_receipt(
            cell=cell,
            source_archive_sha256="a" * 64,
            dataset_materialization_sha256="d" * 64,
            build_attempt_id="build-attempt-01",
            builder_instance_identity="i-builder-01",
            builder_instance_type=cell["environment_contract"]["build_workers"]["borsuk"]["instance_type"],
            build_artifact=build_artifact(cell),
            object_roster=data_roster(cell),
            build_metrics=build_metrics(),
        )
        runtime = authorize_publication_runtime(
            plan,
            receipt=receipt,
            cell=cell,
            source_archive_sha256="a" * 64,
            dataset_materialization_sha256="d" * 64,
        )
        self.assertEqual(runtime["index_receipt_sha256"], receipt_document_sha256(receipt))
        self.assertEqual(runtime["steps"], plan["runtime"]["steps"])
        self.assertNotIn("build", runtime)
        with self.assertRaises(ValueError):
            authorize_publication_runtime(
                plan,
                receipt={**receipt, "index_uri": "s3://attacker/substitute"},
                cell=cell,
                source_archive_sha256="a" * 64,
                dataset_materialization_sha256="d" * 64,
            )

    def test_build_identity_and_storage_are_read_from_the_real_benchmark_artifact(self) -> None:
        cell = scheduled_cell()
        with tempfile.TemporaryDirectory() as root:
            output = Path(root)
            (output / "bench_build.csv").write_text(
                "storage_gets,storage_puts,storage_deletes,storage_heads,storage_lists,storage_bytes_read,storage_bytes_written\n"
                "7,11,0,3,2,654321,123456\n",
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ValueError, "header differs"):
                read_build_artifact(output, cell=cell)
            row = {field: "0" for field in PRODUCTION_BUILD_FIELDS}
            row.update(
                {
                    "logical_cell_catalog_checksum": "3" * 64,
                    "logical_cells": str(cell["index_profile"]["logical_cells"]),
                    "records": str(cell["dataset"]["scale"]["rows"]),
                    "total_active_index_bytes": str(123 * 1024 * 1024),
                    "storage_gets": "7",
                    "storage_puts": "11",
                    "storage_deletes": "0",
                    "storage_heads": "3",
                    "storage_lists": "2",
                    "storage_bytes_read": "654321",
                    "storage_bytes_written": "123456",
                }
            )
            (output / "bench_build.csv").write_text(
                ",".join(PRODUCTION_BUILD_FIELDS) + "\n"
                + ",".join(row[field] for field in PRODUCTION_BUILD_FIELDS) + "\n",
                encoding="utf-8",
            )
            artifact = read_build_artifact(output, cell=cell)
        metrics = artifact["storage_metrics"]
        self.assertEqual(artifact["index_stats"]["records"], cell["dataset"]["scale"]["rows"])
        self.assertEqual(metrics["storage_puts"], 11)
        self.assertEqual(metrics["storage_bytes_read"], 654321)
        self.assertEqual(metrics["storage_bytes_written"], 123456)
        resource = build_receipt_metrics(
            {
                "cpu_ns": 10,
                "peak_rss_bytes": 20,
                "disk_read_bytes": 30,
                "disk_write_bytes": 40,
            },
            metrics,
            elapsed_ns=90,
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
                    "storage_deletes",
                    "storage_heads",
                    "storage_lists",
                    "build_elapsed_ns",
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

    def test_publication_build_and_runtime_execute_as_separate_phases(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            workspace = Path(root)
            plan = {
                "mode": "publication",
                "workspace": str(workspace),
                "build": {
                    "output_dir": str(workspace / "build-output"),
                    "steps": [{"argv": ["/bin/true"], "env": {}}],
                },
                "runtime": {
                    "output_dir": str(workspace / "runtime-output"),
                    "steps": [{"argv": ["/bin/true"], "env": {}}],
                },
            }
            build_output, build_resources, _ = execute_publication_phase(plan, "build")
            runtime_output, runtime_resources, _ = execute_publication_phase(plan, "runtime")
        self.assertNotEqual(build_output, runtime_output)
        self.assertGreater(build_resources["cpu_ns"], 0)
        self.assertGreater(runtime_resources["cpu_ns"], 0)

    def test_publication_report_is_a_complete_admissible_result(self) -> None:
        cell = scheduled_cell()
        protocol = canonical_json_bytes(cell) + b"\n"
        receipt = build_index_receipt(
            cell=cell,
            source_archive_sha256="a" * 64,
            dataset_materialization_sha256="d" * 64,
            build_attempt_id="build-attempt-01",
            builder_instance_identity="i-builder-01",
            builder_instance_type=cell["environment_contract"]["build_workers"]["borsuk"]["instance_type"],
            build_artifact=build_artifact(cell),
            object_roster=data_roster(cell),
            build_metrics=build_metrics(),
        )
        attestation = runtime_attestation_for(
            cell, instance_id="i-0123456789abcdef0"
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
            dataset_materialization_sha256="d" * 64,
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
            },
            runtime_write_metrics={"storage_puts": 0, "storage_bytes_written": 0},
            index_receipt=receipt,
            runtime_attestation=attestation,
        )
        self.assertTrue(report["publishable"])
        self.assertEqual(report["result"]["arm"]["candidate_budget"], 128)
        self.assertEqual(report["result"]["metrics"]["storage_gets"], 10)
        self.assertEqual(report["result"]["metrics"]["storage_bytes_read"], 4096)
        admitted = validate_cell_result(
            report["result"],
            cell=cell,
            protocol_bytes=protocol,
            source_archive_sha256="a" * 64,
            dataset_materialization_sha256="d" * 64,
            index_receipt=receipt,
            runtime_attestation=attestation,
        )
        self.assertEqual(admitted, report["result"])

        with tempfile.TemporaryDirectory() as root:
            trace = Path(root) / "storage-access.csv"
            trace.write_text(
                "operation,object_role,path,physical_format,object_bytes,request_count,bytes_fetched,logical_projection,row_selection,logical_rows_requested,logical_rows_decoded,decode_cpu_ns,cache_state,status\n"
                "write,catalog,collection/CURRENT,json,4096,1,4096,,,,,,,write,ok\n",
                encoding="utf-8",
            )
            observed = summarize_runtime_write_trace(trace)
        self.assertEqual(observed, {"storage_puts": 1, "storage_bytes_written": 4096})
        with self.assertRaisesRegex(ValueError, "cannot write"):
            validate_cell_result(
                {**report["result"], "metrics": {**report["result"]["metrics"], **observed}},
                cell=cell,
                protocol_bytes=protocol,
                source_archive_sha256="a" * 64,
                dataset_materialization_sha256="d" * 64,
                index_receipt=receipt,
                runtime_attestation=attestation,
            )

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
        self.assertEqual(benchmark_env["BORSUK_BENCH_RAM_BUDGET_BYTES"], str(2 * 1024**3))
        self.assertEqual(benchmark_env["BORSUK_BENCH_DISK_CACHE_MAX_BYTES"], str(1024**3))
        self.assertEqual(plan["runtime_client"]["instance_type"], "c7g.xlarge")
        self.assertEqual(plan["runtime_storage"]["volume_size_gib"], 32)

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
        self.assertNotIn("steps", publication)
        self.assertEqual(publication["build"]["worker"]["instance_type"], "r7g.8xlarge")
        self.assertEqual(publication["runtime"]["client"]["instance_type"], "c7g.xlarge")
        build_env = publication["build"]["steps"][-1]["env"]
        runtime_env = publication["runtime"]["steps"][-1]["env"]
        self.assertEqual(build_env["BORSUK_BENCH_BUILD_INDEX"], "1")
        self.assertEqual(build_env["BORSUK_BENCH_BUILD_ONLY"], "1")
        self.assertNotIn("BORSUK_BENCH_READ_ONLY", build_env)
        self.assertEqual(runtime_env["BORSUK_BENCH_RECALL_ONLY"], "1")
        self.assertEqual(runtime_env["BORSUK_BENCH_READ_ONLY"], "1")
        self.assertEqual(runtime_env["BORSUK_BENCH_BUILD_INDEX"], "0")
        self.assertEqual(runtime_env["BORSUK_BENCH_URI"], cell["index_prefix"])
        self.assertNotEqual(runtime_env["BORSUK_BENCH_DATASET"], build_env["BORSUK_BENCH_DATASET"])
        self.assertEqual(
            build_env["BORSUK_BENCH_LOGICAL_CELLS"],
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
