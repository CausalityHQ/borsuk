"""Focused checks for authenticated, contiguous S3 page ranges."""

from __future__ import annotations

import hashlib
import unittest

from scripts.native_verified_s3_ranges import PageSpan, read_verified_range


class _Body:
    def __init__(self, data: bytes):
        self.data = data
        self.closed = False

    def read(self, count: int = -1) -> bytes:
        if count < 0:
            count = len(self.data)
        result, self.data = self.data[:count], self.data[count:]
        return result

    def close(self) -> None:
        self.closed = True


class _Client:
    def __init__(self, response: dict):
        self.response = response
        self.calls: list[dict] = []

    def get_object(self, **kwargs):
        self.calls.append(kwargs)
        return self.response


class VerifiedS3RangeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.pages = (
            PageSpan(4, 10, 3, hashlib.sha256(b"abc").hexdigest()),
            PageSpan(5, 13, 2, hashlib.sha256(b"de").hexdigest()),
        )

    def response(self, body: bytes = b"abcde", **changed) -> dict:
        return {
            "Body": _Body(body), "ResponseMetadata": {"HTTPStatusCode": 206},
            "ContentRange": "bytes 10-14/20", "ContentLength": 5,
        } | changed

    def test_accepts_exact_contiguous_pages_and_one_get(self) -> None:
        response = self.response()
        client = _Client(response)
        actual = read_verified_range(
            client, bucket="bucket", key="code.bin", object_bytes=20,
            pages=self.pages,
        )
        self.assertEqual(actual, b"abcde")
        self.assertEqual(client.calls, [{
            "Bucket": "bucket", "Key": "code.bin", "Range": "bytes=10-14",
        }])
        self.assertTrue(response["Body"].closed)

    def test_rejects_wrong_http_range_or_length(self) -> None:
        for changed in (
            {"ResponseMetadata": {"HTTPStatusCode": 200}},
            {"ContentRange": "bytes 11-15/20"},
            {"ContentLength": 6},
            {"Body": _Body(b"abcd")},
            {"Body": _Body(b"abcdef")},
        ):
            with self.subTest(changed=changed):
                response = self.response(**changed)
                with self.assertRaisesRegex(ValueError, "S3 range"):
                    read_verified_range(
                        _Client(response), bucket="bucket", key="code.bin",
                        object_bytes=20, pages=self.pages,
                    )
                self.assertTrue(response["Body"].closed)

    def test_rejects_corrupt_page_or_discontinuous_request(self) -> None:
        with self.assertRaisesRegex(ValueError, "page digest"):
            read_verified_range(
                _Client(self.response(body=b"abXde")), bucket="bucket",
                key="code.bin", object_bytes=20, pages=self.pages,
            )
        with self.assertRaisesRegex(ValueError, "page span"):
            read_verified_range(
                _Client(self.response()), bucket="bucket", key="code.bin",
                object_bytes=20, pages=(self.pages[0], PageSpan(5, 14, 2,
                    self.pages[1].sha256)),
            )
