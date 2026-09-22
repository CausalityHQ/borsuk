#!/usr/bin/env python3
"""Exact S3 byte-range reader for the PQ48 code wave."""

from __future__ import annotations

import time
from typing import Any
from urllib.parse import urlsplit


class S3CodeRangeReader:
    def __init__(self, client: Any, uri: str, *, object_bytes: int) -> None:
        parsed = urlsplit(uri)
        if (
            parsed.scheme != "s3"
            or not parsed.netloc
            or not parsed.path.startswith("/")
            or len(parsed.path) == 1
            or parsed.query
            or parsed.fragment
            or type(object_bytes) is not int
            or object_bytes <= 0
        ):
            raise ValueError("code object URI differs")
        self.client = client
        self.bucket = parsed.netloc
        self.key = parsed.path[1:]
        self.object_bytes = object_bytes
        self.gets = 0
        self.bytes = 0
        self.nanoseconds = 0

    def __call__(self, offset: int, length: int) -> bytes:
        if (
            type(offset) is not int
            or type(length) is not int
            or offset < 0
            or length <= 0
            or offset + length > self.object_bytes
        ):
            raise ValueError("code range request differs")
        end = offset + length - 1
        started = time.perf_counter_ns()
        response = self.client.get_object(
            Bucket=self.bucket, Key=self.key, Range=f"bytes={offset}-{end}"
        )
        body = response.get("Body")
        if body is None:
            raise ValueError("code range response differs")
        try:
            payload = body.read()
        finally:
            body.close()
        if (
            response.get("ResponseMetadata", {}).get("HTTPStatusCode") != 206
            or response.get("ContentLength") != length
            or response.get("ContentRange")
            != f"bytes {offset}-{end}/{self.object_bytes}"
            or type(payload) is not bytes
            or len(payload) != length
        ):
            raise ValueError("code range response differs")
        self.gets += 1
        self.bytes += length
        self.nanoseconds += time.perf_counter_ns() - started
        return payload
