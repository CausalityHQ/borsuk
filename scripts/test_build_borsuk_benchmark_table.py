from __future__ import annotations

import tempfile
import unittest
from dataclasses import replace
from pathlib import Path

import pyarrow.parquet as pq

from scripts.build_borsuk_benchmark_table import (
    benchmark_rows,
    validate_rows,
    write_artifacts,
)


class BorsukBenchmarkTableTests(unittest.TestCase):
    def test_rows_bind_measured_100k_1m_and_matched_service_evidence(self) -> None:
        rows = benchmark_rows()
        self.assertEqual(
            [(row.system, row.n, row.mode) for row in rows],
            [
                ("BORSUK", 100_000, "native-production-exact-page-score"),
                ("BORSUK", 100_000, "bounded-native-sq8-v3"),
                ("BORSUK", 1_000_000, "bounded-native-sq8"),
                ("Amazon S3 Vectors", 1_000_000, "matched-managed-service"),
            ],
        )
        native_100k, bounded_100k, bounded_1m, s3_vectors = rows
        self.assertEqual(native_100k.queries, 1_000)
        self.assertEqual(native_100k.k, 100)
        self.assertEqual(native_100k.average_recall100_ppm, 339_530)
        self.assertEqual(native_100k.worst_recall100_ppm, 80_000)
        self.assertEqual(bounded_100k.average_recall100_ppm, 512_670)
        self.assertEqual(bounded_100k.get_count_mean, 32.0)
        self.assertEqual(
            bounded_100k.overall_status, "measured-failed-quality-router-line-retired"
        )
        self.assertEqual(bounded_1m.average_recall100_ppm, 990_260)
        self.assertEqual(bounded_1m.warm_latency_p99_ms, 89.746010)
        self.assertEqual(bounded_1m.qps_batch, 182.39226711391123)
        self.assertEqual(bounded_1m.batch_concurrency, 128)
        self.assertEqual(s3_vectors.average_recall100_ppm, 909_380)
        self.assertEqual(s3_vectors.get_count_status, "blocked")
        self.assertEqual(s3_vectors.index_status, "blocked")
        self.assertEqual(bounded_1m.scaling_status, "estimated")
        self.assertIn("cross-architecture", bounded_1m.scaling_note)
        for row in rows:
            self.assertNotIn("<", row.exact_command)
            self.assertNotIn(">", row.exact_command)

    def test_validation_rejects_missing_value_claimed_as_measured(self) -> None:
        rows = benchmark_rows()
        invalid = replace(rows[0], build_time_s=None, build_status="measured")
        with self.assertRaisesRegex(ValueError, "build_time_s"):
            validate_rows([invalid, *rows[1:]])

    def test_writes_strict_parquet_and_operator_markdown(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            parquet = Path(temporary) / "table.parquet"
            markdown = Path(temporary) / "table.md"
            write_artifacts(parquet, markdown)
            table = pq.read_table(parquet)
            self.assertEqual(table.num_rows, 4)
            self.assertEqual(table.schema.metadata[b"schema"], b"borsuk-benchmark-table-v1")
            self.assertFalse(any(field.nullable for field in table.schema))
            body = markdown.read_text()
            self.assertIn("ReLAION-100k development", body)
            self.assertIn("ReLAION-1M development", body)
            self.assertNotIn("ReLAION-1000k", body)
            self.assertIn("99.026%", body)
            self.assertIn("51.267%", body)
            self.assertIn("matched-workload, not paired query order", body)
            self.assertIn("Turbopuffer", body)
            self.assertIn("blocked", body)


if __name__ == "__main__":
    unittest.main()
