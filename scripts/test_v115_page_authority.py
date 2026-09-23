"""Source-only page authority and exact S3 range tests."""

import hashlib
import io
import tempfile
import unittest
from pathlib import Path

from scripts.v115_page_authority import (
    build_page_authority, load_page_authority, read_verified_pages,
)


class PageAuthorityTests(unittest.TestCase):
    def test_source_only_short_page_and_exact_one_get(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            data = bytes(range(256)) * 18 + b"short"
            object_path = root / "sq8.bin"
            # 257 rows of 16 bytes: one complete 256-row page and one row.
            data = data[:4112]
            object_path.write_bytes(data)
            digest = hashlib.sha256(data).hexdigest()
            build_page_authority(
                object_path, root / "authority", expected_object_sha256=digest,
                rows=257, dimensions=4, page_rows=256, generation=1,
            )
            authority = load_page_authority(root / "authority")
            self.assertEqual(authority.page_count, 2)
            self.assertEqual(authority.object_bytes, 4112)

            class Client:
                calls = []
                def get_object(self, *, Bucket, Key, Range, IfMatch):
                    self.calls.append((Range, IfMatch))
                    start, stop = map(int, Range.removeprefix("bytes=").split("-"))
                    payload = data[start:stop + 1]
                    return {"ResponseMetadata": {"HTTPStatusCode": 206},
                            "ContentRange": f"bytes {start}-{stop}/{len(data)}",
                            "ContentLength": len(payload), "ETag": '"etag"',
                            "Body": io.BytesIO(payload)}

            client = Client()
            self.assertEqual(read_verified_pages(
                client, authority, bucket="bucket", key="sq8.bin",
                first_page=0, last_page=1, etag='"etag"'), data)
            self.assertEqual(client.calls, [("bytes=0-4111", '"etag"')])

            sidecar = root / "authority" / "pages.sha256"
            changed = bytearray(sidecar.read_bytes())
            changed[-1] ^= 1
            sidecar.write_bytes(changed)
            with self.assertRaisesRegex(ValueError, "sidecar"):
                load_page_authority(root / "authority")

    def test_changed_page_fails_before_returning_bytes(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            data = bytes(4096)
            source = root / "sq8.bin"
            source.write_bytes(data)
            build_page_authority(
                source, root / "authority",
                expected_object_sha256=hashlib.sha256(data).hexdigest(),
                rows=256, dimensions=4, page_rows=256, generation=1,
            )
            authority = load_page_authority(root / "authority")

            class CorruptClient:
                def get_object(self, *, Bucket, Key, Range, IfMatch):
                    return {"ResponseMetadata": {"HTTPStatusCode": 206},
                            "ContentRange": "bytes 0-4095/4096",
                            "ContentLength": 4096, "ETag": '"etag"',
                            "Body": io.BytesIO(b"x" + data[1:])}

            with self.assertRaisesRegex(ValueError, "digest"):
                read_verified_pages(
                    CorruptClient(), authority, bucket="bucket", key="sq8.bin",
                    first_page=0, last_page=0, etag='"etag"',
                )

            class StaleObject(CorruptClient):
                def get_object(self, **kwargs):
                    response = super().get_object(**kwargs)
                    response["ETag"] = '"new-etag"'
                    return response

            with self.assertRaisesRegex(ValueError, "response"):
                read_verified_pages(
                    StaleObject(), authority, bucket="bucket", key="sq8.bin",
                    first_page=0, last_page=0, etag='"etag"',
                )

            with self.assertRaisesRegex(ValueError, "SHA-256"):
                build_page_authority(
                    source, root / "wrong", expected_object_sha256="0" * 64,
                    rows=256, dimensions=4, page_rows=256, generation=1,
                )
            self.assertFalse((root / "wrong").exists())
