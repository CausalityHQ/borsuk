"""S3 Range response authentication for the row-score code wave."""

from __future__ import annotations

import io
import unittest

from scripts.native_row_score_s3 import S3CodeRangeReader


class FakeS3:
    def __init__(self, body: bytes, content_range: str) -> None:
        self.body = body
        self.content_range = content_range
        self.requests: list[dict[str, str]] = []

    def get_object(self, **kwargs):
        self.requests.append(kwargs)
        return {
            "Body": io.BytesIO(self.body),
            "ContentLength": len(self.body),
            "ContentRange": self.content_range,
            "ResponseMetadata": {"HTTPStatusCode": 206},
        }


class S3CodeRangeTests(unittest.TestCase):
    def test_reads_exact_requested_range(self) -> None:
        client = FakeS3(b"abcd", "bytes 4-7/12")
        reader = S3CodeRangeReader(
            client, "s3://example-bucket/codes.bin", object_bytes=12
        )
        self.assertEqual(reader(4, 4), b"abcd")
        self.assertEqual(
            client.requests,
            [{"Bucket": "example-bucket", "Key": "codes.bin", "Range": "bytes=4-7"}],
        )

    def test_rejects_response_for_different_range(self) -> None:
        reader = S3CodeRangeReader(
            FakeS3(b"abcd", "bytes 3-6/12"),
            "s3://example-bucket/codes.bin", object_bytes=12,
        )
        with self.assertRaisesRegex(ValueError, "range response"):
            reader(4, 4)


if __name__ == "__main__":
    unittest.main()
