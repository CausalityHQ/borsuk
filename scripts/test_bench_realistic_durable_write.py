#!/usr/bin/env python3
"""Contract tests for the realistic durable-write campaign runner."""

import json
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
RUNNER = ROOT / "scripts/bench_realistic_durable_write.sh"
MANIFEST = ROOT / "docs/research/realistic-durable-write-campaign.json"


class RealisticDurableWriteRunnerTests(unittest.TestCase):
    def test_preregistered_matrix_uses_realistic_dataset_and_batch_factors(self) -> None:
        self.assertTrue(MANIFEST.is_file())
        manifest = json.loads(MANIFEST.read_text())
        self.assertEqual(manifest["datasets"], ["cohere-medium-1M"])
        self.assertEqual(manifest["dimensions"], 768)
        self.assertEqual(manifest["batch_records"], [1, 32, 128, 1024])
        self.assertEqual(manifest["repetitions"], 3)
        self.assertGreaterEqual(manifest["minimum_raw_batches"], 100)
        self.assertEqual(manifest["max_write_p95_ms"], 200.0)

    def test_runner_is_fail_closed_and_resource_sampled(self) -> None:
        source = RUNNER.read_text()
        self.assertIn("benchmark_with_resources.py", source)
        self.assertIn("REALISTIC_DURABLE_WRITE_FAILED", source)
        self.assertIn("CELL_COMPLETE", source)
        self.assertIn("CELL_FAILED", source)
        self.assertIn("cell validation failed", source)
        self.assertIn("process_exit.txt", source)
        self.assertIn("validate_realistic_durable_write.py", source)
        self.assertIn("--exclude '*/cache/*'", source)
        self.assertIn("--exclude '*/scratch/*'", source)

    def test_runner_refuses_reused_index_and_result_prefixes(self) -> None:
        source = RUNNER.read_text()
        self.assertIn("refusing to reuse non-empty index prefix", source)
        self.assertIn("refusing to reuse non-empty result prefix", source)
        self.assertIn("index and result prefixes must be disjoint", source)

    def test_runner_revalidates_source_archive_and_dataset_bytes(self) -> None:
        source = RUNNER.read_text()
        self.assertIn("BORSUK_SOURCE_ARCHIVE", source)
        self.assertIn("frozen source archive digest mismatch", source)
        self.assertIn("--validate-existing", source)
        self.assertIn('cp "$DATASET_DIR/dataset.json" "$OUTPUT/dataset.json"', source)

    def test_runner_uses_insert_only_phase_and_exact_batch_factor(self) -> None:
        source = RUNNER.read_text()
        self.assertIn("BORSUK_BENCH_INSERT_ONLY=1", source)
        self.assertIn('BORSUK_BENCH_WRITE_BATCH_SIZE="$batch"', source)
        self.assertIn('BORSUK_BENCH_WRITE_OPS="$write_ops"', source)

    def test_smoke_uses_the_same_parquet_vector_shape_as_production(self) -> None:
        source = RUNNER.read_text()
        self.assertIn('BORSUK_BENCH_PYTHON', source)
        self.assertIn('train.parquet', source)
        self.assertIn('neighbors.parquet', source)
        self.assertIn('pa.list_(pa.float32())', source)


if __name__ == "__main__":
    unittest.main()
