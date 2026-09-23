"""Read one sealed contiguous page interval through an authenticated S3 GET."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Any, Protocol, Sequence


class S3RangeClient(Protocol):
    def get_object(self, *, Bucket: str, Key: str, Range: str) -> dict[str, Any]: ...


@dataclass(frozen=True, slots=True)
class PageSpan:
    page: int
    offset: int
    length: int
    sha256: str


def read_verified_range(
    client: S3RangeClient, *, bucket: str, key: str, object_bytes: int,
    pages: Sequence[PageSpan],
) -> bytes:
    """Require a 206 for exact object bounds, then check every page digest."""
    if (
        not bucket or not key or type(object_bytes) is not int or object_bytes <= 0
        or not pages
    ):
        raise ValueError("S3 page span differs")
    previous = None
    for span in pages:
        if (
            type(span.page) is not int or span.page < 0
            or type(span.offset) is not int or span.offset < 0
            or type(span.length) is not int or span.length <= 0
            or span.offset + span.length > object_bytes
            or type(span.sha256) is not str or len(span.sha256) != 64
            or any(char not in "0123456789abcdef" for char in span.sha256)
            or (previous is not None and (
                span.page != previous.page + 1
                or span.offset != previous.offset + previous.length
            ))
        ):
            raise ValueError("S3 page span differs")
        previous = span
    start = pages[0].offset
    stop = pages[-1].offset + pages[-1].length
    response = client.get_object(
        Bucket=bucket, Key=key, Range=f"bytes={start}-{stop - 1}",
    )
    body = response["Body"]
    try:
        if (
            response.get("ResponseMetadata", {}).get("HTTPStatusCode") != 206
            or response.get("ContentRange") != f"bytes {start}-{stop - 1}/{object_bytes}"
            or response.get("ContentLength") != stop - start
        ):
            raise ValueError("S3 range authority differs")
        payload = body.read(stop - start + 1)
    finally:
        body.close()
    if len(payload) != stop - start:
        raise ValueError("S3 range length differs")
    for span in pages:
        first = span.offset - start
        if hashlib.sha256(payload[first:first + span.length]).hexdigest() != span.sha256:
            raise ValueError("S3 page digest differs")
    return payload
