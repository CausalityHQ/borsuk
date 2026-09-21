from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from copy import deepcopy
from dataclasses import asdict
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from scripts.reduce_bounded_reader_result import (
    canonical_reduction_bytes,
    reduce_bounded_reader_result,
)

PASS_LABELS = ("first_connection_pass", "connection_reuse_pass")


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _returned(truth: list[int], hits: int, hits10: int) -> list[int]:
    values = truth[:hits10] + [900_000 + index for index in range(10 - hits10)]
    values += truth[10 : 10 + hits - hits10]
    values += [800_000 + index for index in range(100 - len(values))]
    return values


class BoundedReaderReducerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.result = self.root / "result.json"
        self.samples = self.root / "samples.parquet"
        self.truth = self.root / "truth.parquet"
        self.truth_rows = [
            list(range(10_000, 10_100)),
            list(range(20_000, 20_100)),
        ]
        pq.write_table(
            pa.Table.from_arrays(
                [
                    pa.array([0] * 100 + [1] * 100, type=pa.uint32()),
                    pa.array(list(range(100)) * 2, type=pa.uint16()),
                    pa.array(self.truth_rows[0] + self.truth_rows[1], type=pa.uint64()),
                    pa.array([float(rank) for rank in range(100)] * 2, type=pa.float64()),
                ],
                schema=pa.schema(
                    [
                        pa.field("query_ordinal", pa.uint32(), nullable=False),
                        pa.field("rank", pa.uint16(), nullable=False),
                        pa.field("feature_row_id", pa.uint64(), nullable=False),
                        pa.field("squared_distance", pa.float64(), nullable=False),
                    ]
                ),
            ),
            self.truth,
        )
        self.rows = []
        for pass_index, label in enumerate(PASS_LABELS):
            for query_ordinal, (hits, hits10, latency, requests, size) in enumerate(
                ((90, 10, 100 + pass_index * 100, 2, 1_000),
                 (80, 8, 300 + pass_index * 100, 4, 3_000))
            ):
                self.rows.append(
                    {
                        "pass_label": label,
                        "query_ordinal": query_ordinal,
                        "latency_ns": latency,
                        "recall10_ppm": hits10 * 100_000,
                        "recall100_ppm": hits * 10_000,
                        "requests": requests,
                        "bytes": size,
                        "returned_feature_row_ids": _returned(
                            self.truth_rows[query_ordinal], hits, hits10
                        ),
                    }
                )
        self._write_samples()
        self.result_value = {
            "schema": "borsuk-bounded-reader-result-v1",
            "claim_eligible": False,
            "evidence_kind": "measured-native-bounded-sq8-reader",
            "storage": "real-object-store-ranged-gets-no-local-cache",
            "cpu_path": "safe-rust-simd-scan-and-adc-router",
            "in_query_cpu": "rayon",
            "source_commit": "1" * 40,
            "manifest_sha256": "2" * 64,
            "sq8_sha256": "3" * 64,
            "rows": 1_000_000,
            "dimensions": 768,
            "page_rows": 512,
            "shortlist_rows": 512,
            "coarse_regions": 256,
            "gap_pages": 2,
            "concurrency": 128,
            "queries_per_pass": 2,
            "passes": [
                self._aggregate("first_connection_pass", 100, 300),
                self._aggregate("connection_reuse_pass", 200, 400),
            ],
            "throughput": [],
            "samples_sha256": _sha256(self.samples),
            "samples_bytes": self.samples.stat().st_size,
        }
        self._write_result()

    def tearDown(self) -> None:
        self.temporary.cleanup()

    @staticmethod
    def _aggregate(label: str, first_latency: int, second_latency: int) -> dict[str, object]:
        return {
            "label": label,
            "queries": 2,
            "average_recall10_ppm": 900_000,
            "average_recall100_ppm": 850_000,
            "p05_recall100_ppm": 800_000,
            "worst_recall100_ppm": 800_000,
            "latency_p50_ns": second_latency,
            "latency_p95_ns": second_latency,
            "latency_p99_ns": second_latency,
            "requests_total": 6,
            "requests_mean_milli": 3_000,
            "requests_p50": 4,
            "requests_p95": 4,
            "requests_p99": 4,
            "requests_max": 4,
            "bytes_total": 4_000,
            "bytes_mean": 2_000,
            "bytes_p50": 3_000,
            "bytes_p95": 3_000,
            "bytes_p99": 3_000,
            "bytes_max": 3_000,
        }

    def _write_samples(self) -> None:
        pq.write_table(
            pa.Table.from_pylist(
                self.rows,
                schema=pa.schema(
                    [
                        pa.field("pass_label", pa.string(), nullable=False),
                        pa.field("query_ordinal", pa.uint32(), nullable=False),
                        pa.field("latency_ns", pa.uint64(), nullable=False),
                        pa.field("recall10_ppm", pa.uint32(), nullable=False),
                        pa.field("recall100_ppm", pa.uint32(), nullable=False),
                        pa.field("requests", pa.uint32(), nullable=False),
                        pa.field("bytes", pa.uint64(), nullable=False),
                        pa.field(
                            "returned_feature_row_ids",
                            pa.list_(pa.field("item", pa.int64(), nullable=False), 100),
                            nullable=False,
                        ),
                    ]
                ),
            ),
            self.samples,
        )

    def _write_result(self) -> None:
        self.result.write_text(
            json.dumps(self.result_value, separators=(",", ":")) + "\n",
            encoding="utf-8",
        )

    def _rebind_samples(self) -> None:
        self.result_value["samples_sha256"] = _sha256(self.samples)
        self.result_value["samples_bytes"] = self.samples.stat().st_size
        self._write_result()

    def test_recomputes_two_pass_quality_latency_io_and_feature_id_evidence(self) -> None:
        receipt = reduce_bounded_reader_result(self.result, self.samples, self.truth)

        self.assertEqual(receipt.schema, "borsuk-bounded-reader-reduction-v1")
        self.assertFalse(receipt.claim_eligible)
        self.assertEqual(receipt.samples_sha256, _sha256(self.samples))
        self.assertEqual(receipt.truth_sha256, _sha256(self.truth))
        self.assertEqual(receipt.passes[0].average_recall10_ppm, 900_000)
        self.assertEqual(receipt.passes[0].average_recall100_ppm, 850_000)
        self.assertEqual(receipt.passes[0].p05_recall100_ppm, 800_000)
        self.assertEqual(receipt.passes[0].worst_recall100_ppm, 800_000)
        self.assertEqual(receipt.passes[0].latency_p99_ns, 300)
        self.assertEqual(receipt.passes[0].requests_total, 6)
        self.assertEqual(receipt.passes[0].bytes_total, 4_000)
        body = canonical_reduction_bytes(receipt)
        self.assertTrue(body.endswith(b"\n"))
        expected = json.dumps(
            asdict(receipt), sort_keys=True, separators=(",", ":")
        ).encode() + b"\n"
        self.assertEqual(body, expected)
        self.assertEqual(json.loads(body), json.loads(expected))

    def test_rejects_result_schema_digest_and_semantic_sample_drift(self) -> None:
        mutations = {
            "result-schema": lambda: self.result_value.__setitem__("schema", "wrong"),
            "samples-digest": lambda: self.result_value.__setitem__("samples_sha256", "0" * 64),
            "returned-feature-id": lambda: self.rows[0]["returned_feature_row_ids"].__setitem__(0, 777_777),
            "pass-label": lambda: self.rows[0].__setitem__("pass_label", "wrong"),
            "query-order": lambda: self.rows[0].__setitem__("query_ordinal", 1),
        }
        baseline_rows = deepcopy(self.rows)
        baseline_result = deepcopy(self.result_value)
        for name, mutate in mutations.items():
            with self.subTest(name=name):
                self.rows = deepcopy(baseline_rows)
                self.result_value = deepcopy(baseline_result)
                self._write_samples()
                self._write_result()
                mutate()
                if name in {"returned-feature-id", "pass-label", "query-order"}:
                    self._write_samples()
                    self._rebind_samples()
                else:
                    self._write_result()
                with self.assertRaises(ValueError):
                    reduce_bounded_reader_result(self.result, self.samples, self.truth)


if __name__ == "__main__":
    unittest.main()
