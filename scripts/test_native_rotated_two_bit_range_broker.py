"""Unix-only range capability for network-isolated evaluation."""

from __future__ import annotations

import hashlib
import json
import tempfile
import threading
import time
import unittest
from pathlib import Path

from scripts.native_rotated_two_bit_range_broker import (
    BrokerRangeReader,
    UnixRangeBroker,
)


class FakeS3Reader:
    def __init__(self, body: bytes):
        self.body = body
        self.gets = 0
        self.bytes = 0
        self.nanoseconds = 0

    def __call__(self, offset: int, length: int) -> bytes:
        self.gets += 1
        self.bytes += length
        return self.body[offset : offset + length]


class BrokerTests(unittest.TestCase):
    def test_only_sealed_ranges_are_served_and_audited(self) -> None:
        body = b"abcdefghijkl"
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            sock = root / "broker.sock"
            audit = root / "audit.json"
            s3 = FakeS3Reader(body)
            broker = UnixRangeBroker(
                sock, audit, s3,
                allowed={(2, 4): hashlib.sha256(body[2:6]).hexdigest()},
                uri="s3://bucket/groups.bin", object_sha256=hashlib.sha256(body).hexdigest(),
            )
            worker = threading.Thread(target=broker.serve_forever)
            worker.start()
            for _ in range(100):
                if sock.exists():
                    break
                time.sleep(0.01)
            client = BrokerRangeReader(sock, object_bytes=len(body))
            self.assertEqual(client(2, 4), body[2:6])
            self.assertEqual((client.gets, client.bytes), (1, 4))
            with self.assertRaisesRegex(ValueError, "range denied"):
                client(0, 4)
            self.assertEqual(s3.gets, 1)
            broker.stop()
            worker.join(timeout=5)
            self.assertFalse(worker.is_alive())
            value = json.loads(audit.read_bytes())
            self.assertEqual((value["gets"], value["bytes"]), (1, 4))
            self.assertEqual(value["uri"], "s3://bucket/groups.bin")

    def test_corrupt_s3_range_never_reaches_evaluator(self) -> None:
        body = b"abcdefghijkl"
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            s3 = FakeS3Reader(body)
            broker = UnixRangeBroker(
                root / "broker.sock", root / "audit.json", s3,
                allowed={(2, 4): hashlib.sha256(b"wrong").hexdigest()},
                uri="s3://bucket/groups.bin", object_sha256=hashlib.sha256(body).hexdigest(),
            )
            worker = threading.Thread(target=broker.serve_forever)
            worker.start()
            for _ in range(100):
                if broker.socket_path.exists():
                    break
                time.sleep(0.01)
            client = BrokerRangeReader(broker.socket_path, object_bytes=len(body))
            with self.assertRaisesRegex(ValueError, "range identity differs"):
                client(2, 4)
            self.assertEqual((client.gets, client.bytes), (0, 0))
            broker.stop()
            worker.join(timeout=5)
            self.assertFalse(worker.is_alive())
            self.assertEqual(json.loads(broker.audit_path.read_bytes())["gets"], 1)


if __name__ == "__main__":
    unittest.main()
