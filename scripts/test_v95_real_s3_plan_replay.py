import io
import json
import subprocess
import unittest
from pathlib import Path

import numpy as np

from scripts.v95_real_s3_plan_replay import (
    PqCodePageCodec,
    Sq8PageCodec,
    replay_query,
    summarize_replays,
    validate_replay_plan,
    warm_range_connections,
)


class _Body:
    def __init__(self, payload: bytes) -> None:
        self._stream = io.BytesIO(payload)

    def read(self) -> bytes:
        return self._stream.read()


class _FakeS3:
    def __init__(self, objects: dict[str, bytes]) -> None:
        self.objects = objects
        self.calls: list[tuple[str, int, int]] = []

    def get_object(self, *, Bucket: str, Key: str, Range: str) -> dict[str, _Body]:
        self.assert_bucket = Bucket
        first, last = (int(value) for value in Range.removeprefix("bytes=").split("-"))
        self.calls.append((Key, first, last))
        return {"Body": _Body(self.objects[Key][first : last + 1])}


class V95RealS3PlanReplayTests(unittest.TestCase):
    def fixture(self) -> tuple[_FakeS3, Sq8PageCodec]:
        codec = Sq8PageCodec(dimensions=2, page_rows=2)
        first = codec.encode(
            np.asarray([10, 11], dtype=np.int64),
            np.asarray([[0.0, 0.0], [1.0, 1.0]], dtype=np.float32),
        )
        second = codec.encode(
            np.asarray([12, 13], dtype=np.int64),
            np.asarray([[10.0, 10.0], [11.0, 11.0]], dtype=np.float32),
        )
        code_page_bytes = 8
        return (
            _FakeS3(
                {
                    "codes.bin": bytes(range(code_page_bytes * 2)),
                    "sq8.bin": first + second,
                }
            ),
            codec,
        )

    def test_sq8_page_codec_round_trips_fixed_width_ids_and_vectors(self) -> None:
        _, codec = self.fixture()
        identifiers = np.asarray([20, 21], dtype=np.int64)
        vectors = np.asarray([[2.0, 4.0], [6.0, 8.0]], dtype=np.float32)

        payload = codec.encode(identifiers, vectors)
        decoded_ids, decoded_vectors = codec.decode(payload)

        self.assertEqual(len(payload), codec.encoded_bytes)
        np.testing.assert_array_equal(decoded_ids, identifiers)
        np.testing.assert_allclose(decoded_vectors, vectors, rtol=0.0, atol=0.024)

    def test_pq_code_pages_are_fixed_width_and_bind_their_real_row_count(self) -> None:
        codec = PqCodePageCodec(subspaces=3, page_rows=2)

        full = codec.encode(np.asarray([[1, 2, 3], [4, 5, 6]], dtype=np.uint8))
        partial = codec.encode(np.asarray([[7, 8, 9]], dtype=np.uint8))

        self.assertEqual(len(full), codec.encoded_bytes)
        self.assertEqual(len(partial), codec.encoded_bytes)
        np.testing.assert_array_equal(
            codec.decode(full), np.asarray([[1, 2, 3], [4, 5, 6]], dtype=np.uint8)
        )
        np.testing.assert_array_equal(
            codec.decode(partial), np.asarray([[7, 8, 9]], dtype=np.uint8)
        )

    def test_replay_fetches_only_registered_ranges_and_reranks_returned_bytes(
        self,
    ) -> None:
        s3, codec = self.fixture()

        result = replay_query(
            s3,
            bucket="frozen-bucket",
            code_key="codes.bin",
            sq8_key="sq8.bin",
            code_ranges=((1, 1),),
            data_ranges=((1, 1),),
            code_page_bytes=8,
            codec=codec,
            query=np.asarray([10.1, 10.1], dtype=np.float32),
            delta_ids=np.asarray([99], dtype=np.int64),
            delta_vectors=np.asarray([[100.0, 100.0]], dtype=np.float32),
            truth_ids=np.asarray([12], dtype=np.int64),
            neighbors=1,
            read_threads=2,
        )

        self.assertEqual(result["result_ids"], [12])
        self.assertEqual(result["truth_hits"], 1)
        self.assertEqual(result["requests"], 2)
        self.assertEqual(result["code_requests"], 1)
        self.assertEqual(result["data_requests"], 1)
        self.assertEqual(result["bytes"], 8 + codec.encoded_bytes)
        self.assertEqual(
            s3.calls,
            [
                ("codes.bin", 8, 15),
                ("sq8.bin", codec.encoded_bytes, codec.encoded_bytes * 2 - 1),
            ],
        )

    def test_replay_rejects_short_or_wrong_page_payloads(self) -> None:
        s3, codec = self.fixture()
        s3.objects["sq8.bin"] = s3.objects["sq8.bin"][:-1]

        with self.assertRaisesRegex(ValueError, "V95 S3 range length differs"):
            replay_query(
                s3,
                bucket="frozen-bucket",
                code_key="codes.bin",
                sq8_key="sq8.bin",
                code_ranges=((1, 1),),
                data_ranges=((1, 1),),
                code_page_bytes=8,
                codec=codec,
                query=np.asarray([10.1, 10.1], dtype=np.float32),
                delta_ids=np.asarray([99], dtype=np.int64),
                delta_vectors=np.asarray([[100.0, 100.0]], dtype=np.float32),
                truth_ids=np.asarray([12], dtype=np.int64),
                neighbors=1,
                read_threads=2,
            )

    def test_summary_uses_nearest_rank_latency_and_exact_physical_totals(self) -> None:
        summary = summarize_replays(
            [
                {
                    "bytes": 100,
                    "code_io_ns": 1_000_000,
                    "data_io_ns": 2_000_000,
                    "decode_ns": 3_000_000,
                    "requests": 3,
                    "rerank_ns": 4_000_000,
                    "storage_io_ns": 3_000_000,
                    "total_ns": 10_000_000,
                    "truth_hits": 98,
                },
                {
                    "bytes": 200,
                    "code_io_ns": 2_000_000,
                    "data_io_ns": 3_000_000,
                    "decode_ns": 4_000_000,
                    "requests": 4,
                    "rerank_ns": 5_000_000,
                    "storage_io_ns": 5_000_000,
                    "total_ns": 14_000_000,
                    "truth_hits": 100,
                },
                {
                    "bytes": 300,
                    "code_io_ns": 3_000_000,
                    "data_io_ns": 4_000_000,
                    "decode_ns": 5_000_000,
                    "requests": 5,
                    "rerank_ns": 6_000_000,
                    "storage_io_ns": 7_000_000,
                    "total_ns": 18_000_000,
                    "truth_hits": 99,
                },
            ],
            neighbors=100,
        )

        self.assertEqual(summary["recall_ppm"], 990_000)
        self.assertEqual(summary["worst_query_hits"], 98)
        self.assertEqual(summary["requests"], {"mean": 4.0, "maximum": 5})
        self.assertEqual(summary["bytes"], {"mean": 200.0, "maximum": 300})
        self.assertEqual(summary["storage_io_ms"]["p50"], 5.0)
        self.assertEqual(summary["storage_io_ms"]["p95"], 7.0)
        self.assertEqual(summary["total_ms"]["p50"], 14.0)
        self.assertEqual(summary["total_ms"]["p95"], 18.0)
        self.assertEqual(summary["percentile_note"], "p95-is-maximum-of-16-or-fewer")

    def test_connection_preflight_opens_every_worker_and_reports_its_s3_work(
        self,
    ) -> None:
        s3, codec = self.fixture()
        code_pages = bytes(range(8)) * 16
        data_page = codec.encode(
            np.asarray([30, 31], dtype=np.int64),
            np.asarray([[2.0, 2.0], [3.0, 3.0]], dtype=np.float32),
        )
        s3.objects = {
            "codes.bin": code_pages,
            "sq8.bin": data_page * 16,
        }

        evidence = warm_range_connections(
            s3,
            bucket="frozen-bucket",
            code_key="codes.bin",
            sq8_key="sq8.bin",
            code_page_bytes=8,
            data_page_bytes=codec.encoded_bytes,
            read_threads=16,
        )

        self.assertEqual(evidence["requests"], 32)
        self.assertEqual(evidence["bytes"], 16 * (8 + codec.encoded_bytes))
        self.assertEqual(len(s3.calls), 32)

    def test_plan_authority_rejects_query_order_range_and_object_drift(self) -> None:
        plan = {
            "schema": "borsuk-v95-real-s3-plan-v1",
            "source_commit": "a" * 40,
            "bucket": "frozen-bucket",
            "code_object": {
                "bytes": 128,
                "key": "index/codes.bin",
                "sha256": "b" * 64,
            },
            "sq8_object": {
                "bytes": 256,
                "key": "index/sq8.bin",
                "sha256": "c" * 64,
            },
            "code_page_bytes": 64,
            "data_page_bytes": 128,
            "dimensions": 2,
            "neighbors": 1,
            "page_rows": 2,
            "queries": [
                {
                    "code_ranges": [[0, 0]],
                    "data_ranges": [[1, 1]],
                    "expected_hits": 1,
                    "ordinal": 328,
                },
                {
                    "code_ranges": [[1, 1]],
                    "data_ranges": [[0, 0]],
                    "expected_hits": 1,
                    "ordinal": 329,
                },
            ],
        }

        validated = validate_replay_plan(plan, expected_queries=2)
        self.assertEqual(
            [query["ordinal"] for query in validated["queries"]], [328, 329]
        )

        mutations = (
            {**plan, "bucket": ""},
            {**plan, "code_page_bytes": 63},
            {**plan, "queries": list(reversed(plan["queries"]))},
            {
                **plan,
                "sq8_object": {**plan["sq8_object"], "sha256": "not-a-digest"},
            },
        )
        for mutation in mutations:
            with self.subTest(mutation=mutation):
                with self.assertRaisesRegex(ValueError, "V95 replay plan differs"):
                    validate_replay_plan(mutation, expected_queries=2)

    def test_runner_describes_spot_only_sequential_real_s3_replay(
        self,
    ) -> None:
        runner = Path(__file__).with_name("v95_real_s3_plan_replay_run_remote.sh")
        completed = subprocess.run(
            ["bash", str(runner), "--describe"],
            check=True,
            capture_output=True,
            text=True,
        )

        self.assertEqual(
            json.loads(completed.stdout),
            {
                "actual_s3": True,
                "instance_type": "c7i.8xlarge",
                "max_wall_seconds": 3000,
                "queries": 16,
                "query_parallelism": 1,
                "range_read_threads": 16,
                "rayon_work_stealing": False,
                "spot_only": True,
                "stage": "frozen-plan-replay",
            },
        )


if __name__ == "__main__":
    unittest.main()
