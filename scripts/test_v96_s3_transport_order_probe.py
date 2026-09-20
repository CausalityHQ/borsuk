import io
import json
import subprocess
import unittest
from pathlib import Path

from scripts.v96_s3_transport_order_probe import (
    classify_transport_tail,
    replay_transport_order,
)


class _Body:
    def __init__(self, payload: bytes) -> None:
        self._payload = io.BytesIO(payload)

    def read(self) -> bytes:
        return self._payload.read()


class _FakeS3:
    def __init__(self, objects: dict[str, bytes]) -> None:
        self.objects = objects
        self.calls: list[tuple[str, int, int]] = []

    def get_object(self, *, Bucket: str, Key: str, Range: str) -> dict[str, _Body]:
        self.bucket = Bucket
        first, last = (int(value) for value in Range.removeprefix("bytes=").split("-"))
        self.calls.append((Key, first, last))
        return {"Body": _Body(self.objects[Key][first : last + 1])}


def _plan() -> dict[str, object]:
    return {
        "bucket": "frozen-bucket",
        "code_object": {
            "bytes": 128,
            "key": "index/codes.bin",
            "sha256": "b" * 64,
        },
        "code_page_bytes": 64,
        "data_page_bytes": 128,
        "dimensions": 2,
        "neighbors": 100,
        "page_rows": 2,
        "queries": [
            {
                "code_ranges": [[0, 0]],
                "data_ranges": [[1, 1]],
                "expected_hits": 99,
                "ordinal": 328,
            },
            {
                "code_ranges": [[1, 1]],
                "data_ranges": [[0, 0]],
                "expected_hits": 98,
                "ordinal": 329,
            },
        ],
        "schema": "borsuk-v95-real-s3-plan-v1",
        "source_commit": "a" * 40,
        "sq8_object": {
            "bytes": 256,
            "key": "index/sq8.bin",
            "sha256": "c" * 64,
        },
    }


class V96S3TransportOrderProbeTests(unittest.TestCase):
    def test_reverse_replay_reads_only_frozen_ranges_and_preserves_plan(self) -> None:
        plan = _plan()
        original = json.dumps(plan, sort_keys=True)
        s3 = _FakeS3(
            {
                "index/codes.bin": bytes(range(128)),
                "index/sq8.bin": bytes(range(128)) * 2,
            }
        )

        samples = replay_transport_order(
            s3,
            plan=plan,
            read_threads=2,
            order="reverse",
        )

        self.assertEqual([sample["ordinal"] for sample in samples], [329, 328])
        self.assertEqual([sample["position"] for sample in samples], [0, 1])
        self.assertEqual([sample["expected_hits"] for sample in samples], [98, 99])
        self.assertEqual([sample["requests"] for sample in samples], [2, 2])
        self.assertEqual([sample["bytes"] for sample in samples], [192, 192])
        self.assertEqual(
            s3.calls,
            [
                ("index/codes.bin", 64, 127),
                ("index/sq8.bin", 0, 127),
                ("index/codes.bin", 0, 63),
                ("index/sq8.bin", 128, 255),
            ],
        )
        self.assertEqual(json.dumps(plan, sort_keys=True), original)

    def test_tail_classifier_distinguishes_position_query_and_nonreproduction(self) -> None:
        cases = (
            ([{"ordinal": 343, "storage_io_ns": 600_000_000}, {"ordinal": 328, "storage_io_ns": 200_000_000}], "position-dependent-tail-supported"),
            ([{"ordinal": 343, "storage_io_ns": 200_000_000}, {"ordinal": 328, "storage_io_ns": 600_000_000}], "ordinal-328-tail-supported"),
            ([{"ordinal": 343, "storage_io_ns": 600_000_000}, {"ordinal": 328, "storage_io_ns": 600_000_000}], "tail-cause-unresolved"),
            ([{"ordinal": 343, "storage_io_ns": 200_000_000}, {"ordinal": 328, "storage_io_ns": 200_000_000}], "original-tail-not-reproduced"),
        )
        for samples, expected in cases:
            with self.subTest(expected=expected):
                self.assertEqual(
                    classify_transport_tail(samples, threshold_ms=500), expected
                )

    def test_runner_is_reverse_only_spot_and_downloads_no_scientific_corpus(self) -> None:
        runner = Path(__file__).with_name("v96_s3_transport_order_probe_run_remote.sh")
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
                "max_wall_seconds": 900,
                "query_order": "reverse",
                "query_parallelism": 1,
                "range_read_threads": 16,
                "rayon_work_stealing": False,
                "scientific_inputs": ["plan.json"],
                "spot_only": True,
                "stage": "transport-order-diagnostic",
            },
        )


if __name__ == "__main__":
    unittest.main()
