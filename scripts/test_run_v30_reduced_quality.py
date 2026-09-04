import hashlib
import json
import unittest

import pyarrow as pa
import pyarrow.parquet as pq

from scripts.run_v30_reduced_quality import reduce_v30_quality


class V30ReducedQualityTests(unittest.TestCase):
    def truth_bytes(self) -> bytes:
        table = pa.table(
            {
                "query_ordinal": pa.array(
                    [query for query in range(32) for _ in range(10)],
                    type=pa.uint16(),
                ),
                "source_ordinal": pa.array(
                    [query * 100 + neighbor for query in range(32) for neighbor in range(10)],
                    type=pa.uint64(),
                ),
            },
            schema=pa.schema(
                [
                    pa.field("query_ordinal", pa.uint16(), nullable=False),
                    pa.field("source_ordinal", pa.uint64(), nullable=False),
                ]
            ),
        )
        sink = pa.BufferOutputStream()
        pq.write_table(table, sink)
        return sink.getvalue().to_pybytes()

    def result(self, query: int, *, misses: int = 0) -> bytes:
        sources = [query * 100 + neighbor for neighbor in range(10 - misses)]
        sources.extend(1_000_000 + query * 10 + offset for offset in range(misses))
        value = {
            "claim_eligible": False,
            "matches": [
                {"source_ordinal": source, "squared_distance": float(rank)}
                for rank, source in enumerate(sources)
            ],
            "schema_version": 1,
            "work": {
                "decoded_rows": 4096,
                "encoded_bytes": 1_986_668,
                "get_count": 10,
                "routing": {
                    "candidates_retained": 12_288,
                    "codes_scanned": 39_612,
                    "leaves_scored": 64,
                    "pages_considered": 10,
                    "roots_scored": 16,
                    "selected_pages": 10,
                },
                "unique_rows": 4096,
            },
        }
        return json.dumps(value, allow_nan=False, separators=(",", ":"), sort_keys=True).encode() + b"\n"

    def test_v30_reduced_quality_recomputes_recall_and_cold_s3_projection(self) -> None:
        # Break caught: the fast gate trusts a reported aggregate, assumes a warm
        # page cache, or serializes ten GET latencies instead of one read wave.
        truth = self.truth_bytes()
        results = tuple(
            self.result(query, misses=1 if query == 11 else 0)
            for query in range(32)
        )
        payload = reduce_v30_quality(
            results,
            truth,
            truth_sha256=hashlib.sha256(truth).hexdigest(),
            cpu_p99_ms=15.0,
            request_p50_ms=10.0,
            request_p95_ms=25.0,
            request_p99_ms=50.0,
            aggregate_bytes_per_second=100_000_000,
        )
        value = json.loads(payload)
        self.assertEqual(value["aggregate_recall_ppm"], 996_875)
        self.assertEqual(value["minimum_recall_ppm"], 900_000)
        self.assertEqual(value["perfect_queries"], 31)
        self.assertEqual(value["maximum_codes_scanned"], 39_612)
        self.assertEqual(value["maximum_get_count"], 10)
        self.assertAlmostEqual(value["projected_cold_s3_p50_ms"], 44.86668)
        self.assertAlmostEqual(value["projected_cold_s3_p95_ms"], 59.86668)
        self.assertAlmostEqual(value["projected_cold_s3_p99_ms"], 84.86668)
        self.assertEqual(value["status"], "pass")
        self.assertFalse(value["claim_eligible"])
        self.assertEqual(
            payload,
            json.dumps(value, allow_nan=False, separators=(",", ":"), sort_keys=True).encode()
            + b"\n",
        )

        failing = list(results)
        failing[12] = self.result(12, misses=1)
        with self.assertRaisesRegex(ValueError, "quality gates"):
            reduce_v30_quality(
                tuple(failing),
                truth,
                truth_sha256=hashlib.sha256(truth).hexdigest(),
                cpu_p99_ms=15.0,
                request_p50_ms=10.0,
                request_p95_ms=25.0,
                request_p99_ms=50.0,
                aggregate_bytes_per_second=100_000_000,
            )

        with self.assertRaisesRegex(ValueError, "latency gate"):
            reduce_v30_quality(
                results,
                truth,
                truth_sha256=hashlib.sha256(truth).hexdigest(),
                cpu_p99_ms=15.0,
                request_p50_ms=10.0,
                request_p95_ms=25.0,
                request_p99_ms=80.0,
                aggregate_bytes_per_second=100_000_000,
            )


if __name__ == "__main__":
    unittest.main()
