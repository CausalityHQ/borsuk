from __future__ import annotations

import copy
import hashlib
import json
import tempfile
import unittest
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from scripts.validate_bounded_native_result import validate_bounded_native_result


class BoundedNativeResultValidatorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.truth_path = self.root / "truth.parquet"
        self.samples_path = self.root / "samples.parquet"
        self.result_path = self.root / "result.json"
        self._write_truth()
        self._write_samples()
        self.result = self._result()
        self._write_result(self.result)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def _write_truth(self) -> None:
        schema = pa.schema(
            [
                pa.field("query", pa.uint32(), nullable=False),
                pa.field(
                    "neighbors",
                    pa.list_(pa.field("element", pa.int64(), nullable=False), 3),
                    nullable=False,
                ),
            ]
        )
        pq.write_table(
            pa.Table.from_arrays(
                [
                    pa.array([0, 1, 2], type=pa.uint32()),
                    pa.array(
                        [[0, 1, 2], [10, 11, 12], [20, 21, 22]],
                        type=schema.field("neighbors").type,
                    ),
                ],
                schema=schema,
            ),
            self.truth_path,
        )

    def _sample_table(self) -> pa.Table:
        fields = [
            pa.field("query_ordinal", pa.uint32(), nullable=False),
            pa.field("hits_at_10", pa.uint32(), nullable=False),
            pa.field("hits_at_100", pa.uint32(), nullable=False),
            pa.field("recall_at_10_ppm", pa.uint32(), nullable=False),
            pa.field("recall_at_100_ppm", pa.uint32(), nullable=False),
            pa.field("physical_gets", pa.uint64(), nullable=False),
            pa.field("pages_read", pa.uint64(), nullable=False),
            pa.field("bytes_read", pa.uint64(), nullable=False),
            pa.field("records_scored", pa.uint64(), nullable=False),
            pa.field("latency_ns", pa.uint64(), nullable=False),
            pa.field(
                "returned_ids",
                pa.list_(pa.field("element", pa.uint64(), nullable=False), 3),
                nullable=False,
            ),
        ]
        schema = pa.schema(fields)
        values = [
            [0, 1, 2],
            [3, 2, 3],
            [3, 2, 3],
            [1_000_000, 666_667, 1_000_000],
            [1_000_000, 666_667, 1_000_000],
            [2, 3, 2],
            [1, 2, 1],
            [100, 200, 100],
            [30, 40, 30],
            [10_000, 20_000, 30_000],
            [[0, 1, 2], [10, 11, 99], [20, 21, 22]],
        ]
        return pa.Table.from_arrays(
            [pa.array(value, type=field.type) for value, field in zip(values, fields, strict=True)],
            schema=schema,
        )

    def _write_samples(self, table: pa.Table | None = None) -> None:
        pq.write_table(table or self._sample_table(), self.samples_path)

    @staticmethod
    def _sha256(path: Path) -> str:
        return hashlib.sha256(path.read_bytes()).hexdigest()

    def _result(self) -> dict[str, object]:
        return {
            "schema": "borsuk-bounded-native-100k-qualification-v1",
            "claim_eligible": False,
            "source_commit": "a" * 40,
            "rows": 100_000,
            "dimensions": 768,
            "query_count": 3,
            "neighbors": 3,
            "average_recall_at_10_gate_ppm": 800_000,
            "average_recall_at_100_gate_ppm": 800_000,
            "p05_recall_at_100_gate_ppm": 600_000,
            "truth": {
                "role": "truth",
                "sha256": self._sha256(self.truth_path),
                "encoded_bytes": self.truth_path.stat().st_size,
            },
            "samples": {
                "role": "per-query-samples",
                "sha256": self._sha256(self.samples_path),
                "encoded_bytes": self.samples_path.stat().st_size,
            },
            "summary": {
                "average_recall_at_10_ppm": 888_889,
                "average_recall_at_100_ppm": 888_889,
                "p05_recall_at_100_ppm": 666_667,
                "worst_recall_at_100_ppm": 666_667,
                "p50_latency_ns": 20_000,
                "p95_latency_ns": 30_000,
                "p99_latency_ns": 30_000,
                "total_gets": 7,
                "total_pages_read": 4,
                "total_bytes": 400,
                "total_records_scored": 100,
                "passed": True,
            },
            "equivalence": {
                "one_run": True,
                "ten_run": True,
                "hundred_run": True,
                "pending_put": True,
                "pending_delete": True,
                "reopen": True,
                "compaction": True,
            },
        }

    def _write_result(self, value: dict[str, object]) -> None:
        self.result_path.write_text(
            json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n",
            encoding="utf-8",
        )

    def test_accepts_canonical_samples_and_recomputes_every_summary(self) -> None:
        receipt = validate_bounded_native_result(
            self.result_path, self.samples_path, self.truth_path
        )

        self.assertEqual(receipt["average_recall_at_100_ppm"], 888_889)
        self.assertTrue(receipt["passed"])

    def test_rejects_per_query_truth_arithmetic_and_aggregate_drift(self) -> None:
        mutations: list[tuple[str, callable]] = [
            ("query ordinal", lambda table: table.set_column(0, "query_ordinal", pa.array([0, 2, 1], type=pa.uint32()))),
            ("returned ids", lambda table: table.set_column(10, "returned_ids", pa.array([[0, 1, 99], [10, 11, 99], [20, 21, 22]], type=pa.list_(pa.uint64(), 3)))),
            ("hits", lambda table: table.set_column(1, "hits_at_10", pa.array([2, 2, 3], type=pa.uint32()))),
        ]
        for name, mutate in mutations:
            with self.subTest(name=name):
                self._write_samples(mutate(self._sample_table()))
                changed = copy.deepcopy(self.result)
                changed["samples"]["sha256"] = self._sha256(self.samples_path)
                changed["samples"]["encoded_bytes"] = self.samples_path.stat().st_size
                self._write_result(changed)
                with self.assertRaises(ValueError):
                    validate_bounded_native_result(
                        self.result_path, self.samples_path, self.truth_path
                    )

        self._write_samples()
        changed = copy.deepcopy(self.result)
        changed["summary"]["average_recall_at_100_ppm"] = 900_000
        self._write_result(changed)
        with self.assertRaises(ValueError):
            validate_bounded_native_result(
                self.result_path, self.samples_path, self.truth_path
            )

    def test_rejects_missing_semantic_equivalence_receipts(self) -> None:
        for role in list(self.result["equivalence"]):
            with self.subTest(role=role):
                changed = copy.deepcopy(self.result)
                changed["equivalence"][role] = False
                self._write_result(changed)
                with self.assertRaises(ValueError):
                    validate_bounded_native_result(
                        self.result_path, self.samples_path, self.truth_path
                    )


if __name__ == "__main__":
    unittest.main()
