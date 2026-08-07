#!/usr/bin/env python3
"""Static fail-closed contract tests for the scalability runner."""

from pathlib import Path
import json
import unittest


ROOT = Path(__file__).resolve().parent.parent
RUNNER = (ROOT / "scripts/bench_group_commit_scalability.sh").read_text()
BENCH = (ROOT / "crates/borsuk/examples/group_commit_bench.rs").read_text()
ROUTING_BENCH = (ROOT / "crates/borsuk/examples/logical_cell_routing_bench.rs").read_text()
REALISTIC_MANIFEST = ROOT / "docs/research/realistic-group-commit-campaign.json"


class GroupCommitScalabilityRunnerTest(unittest.TestCase):
    def test_production_uses_the_manifest_bound_and_library_default(self) -> None:
        self.assertIn(
            'MAX_RECORDS="$(python3 -c \'import json,sys; '
            'print(json.load(open(sys.argv[1]))["max_group_records"])\' "$MANIFEST")"',
            RUNNER,
        )
        self.assertIn("max_records != 1_024", BENCH)

    def test_smoke_retains_its_small_independent_bound(self) -> None:
        self.assertIn("MAX_RECORDS=8", RUNNER)
        self.assertIn("max_records != 8", BENCH)

    def test_point_visibility_uses_one_batched_routing_traversal(self) -> None:
        self.assertIn("let point_records = reopened.get_records(", BENCH)
        self.assertNotIn("reopened\n                .get_record", BENCH)

    def test_active_tail_reads_are_measured_before_drain_and_gated(self) -> None:
        self.assertLess(
            BENCH.index("ACTIVE_TAIL_READ_QUALIFICATION_COMPLETE"),
            BENCH.index("writer.drain()?"),
        )
        self.assertIn("PRODUCTION_ACTIVE_TAIL_READ_P95_FAILED", BENCH)
        self.assertIn('"active-tail-reads.csv"', RUNNER)

    def test_realistic_campaign_uses_pinned_768d_vectors_and_independent_lane_factors(self) -> None:
        manifest = json.loads(REALISTIC_MANIFEST.read_text())
        self.assertEqual(manifest["dataset"], "cohere-medium-1M")
        self.assertEqual(manifest["dataset_sha256"], "54c733e39adfcaa9ee10f3ed8bd8e66ada9f8f9a1a73e9753f5c5c2044b79254")
        self.assertEqual(manifest["dimensions"], 768)
        self.assertEqual(manifest["writers"], [1, 8, 32])
        self.assertEqual(manifest["worker_lanes"], [1, 2, 4, 8])
        self.assertEqual(manifest["operations_per_writer"], 1_000)
        self.assertEqual(manifest["repetitions"], 5)
        self.assertEqual(manifest["pipeline_depth_per_writer"], 4)
        self.assertEqual(manifest["records_per_operation"], 16)
        self.assertEqual(manifest["throughput_gate_writers"], [32])
        self.assertEqual(manifest["min_end_to_end_records_per_second"], 10_000.0)
        self.assertEqual(manifest["max_acknowledgement_bytes"], 2_097_152)
        self.assertEqual(manifest["max_physical_write_amplification"], 16.0)
        self.assertEqual(manifest["execution_order_policy"], "cyclic-latin-order-per-repetition")
        self.assertEqual(
            manifest["correctness_gates"],
            [
                "grouped_durable_ack",
                "extent_idempotency",
                "post_completion_lease_fencing",
                "stale_watermark_reopen",
                "epoch_zombie_exclusion",
                "owner_only_head_mutation",
                "sequential_last_write_wins",
                "tail_backpressure",
                "delta_drain_frontier_safety",
            ],
        )

    def test_production_runner_requires_dataset_identity_and_nests_lane_cells(self) -> None:
        self.assertIn("BORSUK_GROUP_COMMIT_DATASET", RUNNER)
        self.assertIn("BORSUK_GROUP_COMMIT_DATASET_SHA256", RUNNER)
        self.assertIn('for worker_lanes in "${LANE_ORDER[@]}"', RUNNER)
        self.assertIn('/l${worker_lanes}/w${writers}', RUNNER)
        self.assertIn('cp "$DATASET_DIR/dataset.json" "$OUTPUT/dataset.json"', RUNNER)
        self.assertIn("dataset_sha256,manifest_sha256", BENCH)

    def test_each_matrix_cell_clones_a_pristine_immutable_base(self) -> None:
        self.assertIn("clone_index()", RUNNER)
        self.assertIn('template_uri="$INDEX_ROOT/templates/c${cells}"', RUNNER)
        self.assertIn(
            'uri="$INDEX_ROOT/cells/c${cells}/r$(printf \'%02d\' "$repetition")/l${worker_lanes}/w${writers}"',
            RUNNER,
        )
        self.assertIn('clone_index "$template_uri" "$uri"', RUNNER)
        self.assertIn("BORSUK_ROUTING_GROUP_COMMIT_BASE=1", RUNNER)
        self.assertIn('dimensions == 768', ROUTING_BENCH)
        self.assertIn("VectorMetric::Cosine", ROUTING_BENCH)

    def test_scalability_binary_requires_dataset_vectors_instead_of_random_generation(self) -> None:
        self.assertIn("BORSUK_GROUP_COMMIT_DATASET", BENCH)
        self.assertIn("read_parquet_vectors", BENCH)
        self.assertIn("dataset vectors must be decoded before durable timing", BENCH)
        self.assertIn("dimensions != 768", BENCH)
        self.assertIn("!matches!(worker_lanes, 1 | 2 | 4 | 8)", BENCH)
        self.assertIn("pipeline_depth != 4", BENCH)
        self.assertIn('"diagnostic" => 8', BENCH)
        self.assertIn("dimensions != 96", BENCH)

    def test_runner_revalidates_dataset_bytes_and_terminalizes_each_cell(self) -> None:
        self.assertIn("fetch_vdbbench_dataset.py", RUNNER)
        self.assertIn("--check-existing", RUNNER)
        self.assertIn("--with-requirements", RUNNER)
        self.assertIn("requirements-format-bench.txt", RUNNER)
        self.assertIn("CELL_COMPLETE", RUNNER)
        self.assertIn("CELL_FAILED", RUNNER)
        self.assertIn("--terminal-cell", RUNNER)
        self.assertIn("benchmark_with_resources.py", RUNNER)
        self.assertIn("resource_sample_interval_ms", RUNNER)
        self.assertIn("process_exit.txt", RUNNER)
        self.assertIn("CELL_VALIDATION_FAILED", RUNNER)
        self.assertIn("validation-error.txt", RUNNER)

    def test_runner_refuses_identity_and_s3_prefix_reuse(self) -> None:
        self.assertIn("source SHA-256 differs from checked-out HEAD archive", RUNNER)
        self.assertIn("BORSUK_SOURCE_ARCHIVE", RUNNER)
        self.assertIn("extracted source differs from its preserved source archive", RUNNER)
        self.assertIn("refusing to reuse non-empty index prefix", RUNNER)
        self.assertIn("refusing to reuse non-empty result prefix", RUNNER)

    def test_launcher_refuses_stale_remote_result_directory(self) -> None:
        launcher = (ROOT / "scripts/launch_aws_group_commit_scalability.sh").read_text()
        self.assertIn(
            'remote_output="/home/ec2-user/borsuk-group-commit-results/${RUN_ID}"',
            launcher,
        )
        self.assertIn("source result directory already exists", launcher)
        self.assertIn("index and result prefixes must be disjoint", RUNNER)

    def test_bulk_throughput_gate_is_not_applied_to_latency_cells(self) -> None:
        self.assertIn("THROUGHPUT_GATE_WRITERS", RUNNER)
        self.assertIn("cell_min_rps=0", RUNNER)
        self.assertIn("observed.records_per_second < thresholds.min_records_per_second", BENCH)
        self.assertIn("thresholds.min_records_per_second > 0.0", BENCH)

    def test_aggregate_summary_does_not_duplicate_worker_lane_column(self) -> None:
        self.assertIn('if name == "summary.csv"', RUNNER)
        self.assertIn('prefix_fields + reader.fieldnames', RUNNER)

    def test_repetitions_rotate_writer_and_lane_order(self) -> None:
        self.assertIn("rotate_order()", RUNNER)
        self.assertIn('writer_rotation="$(((repetition - 1) % ${#WRITERS[@]}))"', RUNNER)
        self.assertIn('lane_rotation="$(((repetition - 1) % ${#WORKER_LANES[@]}))"', RUNNER)
        self.assertIn('rotate_order "$writer_rotation" "${WRITERS[@]}"', RUNNER)
        self.assertIn('rotate_order "$lane_rotation" "${WORKER_LANES[@]}"', RUNNER)

    def test_runner_records_physical_storage_trace_for_spill_amplification(self) -> None:
        self.assertIn('BORSUK_STORAGE_TRACE="$storage_trace_output"', RUNNER)
        self.assertIn('mv "$storage_trace_output" "$cell_output/storage-access.csv"', RUNNER)
        self.assertIn("extent_idempotency", RUNNER)
        self.assertIn("post_completion_lease_fencing", RUNNER)

    def test_failed_cell_preserves_exit_status_when_storage_trace_is_missing(self) -> None:
        self.assertIn('if [[ -f "$storage_trace_output" ]]; then', RUNNER)
        self.assertIn('STORAGE_TRACE_MISSING', RUNNER)
        self.assertIn('printf \'%s\\n\' "$status" > "$cell_output/process_exit.txt"', RUNNER)

    def test_failed_cell_preserves_benchmark_diagnostics(self) -> None:
        self.assertIn('benchmark_stdout="${cell_output}.benchmark.stdout.log"', RUNNER)
        self.assertIn('benchmark_stderr="${cell_output}.benchmark.stderr.log"', RUNNER)
        self.assertIn('>"$benchmark_stdout" 2>"$benchmark_stderr"', RUNNER)
        self.assertIn('mv "$benchmark_stdout" "$cell_output/benchmark.stdout.log"', RUNNER)
        self.assertIn('mv "$benchmark_stderr" "$cell_output/benchmark.stderr.log"', RUNNER)

    def test_lane_treatments_use_identical_record_ids(self) -> None:
        self.assertIn("production_record_id(ordinal)", BENCH)
        self.assertNotIn("group-c{cell_count}-r{repetition", BENCH)


if __name__ == "__main__":
    unittest.main()
