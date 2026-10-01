"""Unix-only range capability for network-isolated evaluation."""

from __future__ import annotations

import hashlib
import json
import os
import socket
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch

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
            # A short advertised name near Linux's Unix socket path limit.
            root = Path(temporary) / ("d" * (104 - len(os.fsencode(temporary))))
            root.mkdir()
            sock = root / "s"
            audit = root / "audit.json"
            s3 = FakeS3Reader(body)
            broker = UnixRangeBroker(
                sock, audit, s3,
                allowed={
                    (2, 4): hashlib.sha256(body[2:6]).hexdigest(),
                    (6, 4): hashlib.sha256(b"wrong").hexdigest(),
                },
                uri="s3://bucket/groups.bin", object_sha256=hashlib.sha256(body).hexdigest(),
            )
            bound = threading.Event()
            release = threading.Event()

            class PausedBindSocket(socket.socket):
                def bind(self, address):
                    super().bind(address)
                    bound.set()
                    if not release.wait(5):
                        raise TimeoutError("test did not release broker bind")

            with patch("scripts.native_rotated_two_bit_range_broker.socket.socket", PausedBindSocket):
                worker = threading.Thread(target=broker.serve_forever)
                worker.start()
                try:
                    self.assertTrue(bound.wait(5))
                    self.assertFalse(sock.exists(), "bound socket advertised before listen")
                    release.set()
                    for _ in range(100):
                        if sock.exists():
                            break
                        time.sleep(0.01)
                    self.assertTrue(sock.exists())
                    self.assertEqual(sock.stat().st_mode & 0o777, 0o666)
                    client = BrokerRangeReader(sock, object_bytes=len(body))
                    self.assertEqual(client(2, 4), body[2:6])
                    self.assertEqual((client.gets, client.bytes), (1, 4))
                    with self.assertRaisesRegex(ValueError, "range denied"):
                        client(0, 4)
                    with self.assertRaisesRegex(ValueError, "range identity differs"):
                        client(6, 4)
                    self.assertEqual((client.gets, client.bytes), (1, 4))
                    self.assertEqual(s3.gets, 2)
                finally:
                    release.set()
                    broker.stop()
                    worker.join(timeout=5)
            self.assertFalse(worker.is_alive())
            value = json.loads(audit.read_bytes())
            self.assertEqual((value["gets"], value["bytes"]), (2, 8))
            self.assertEqual(value["uri"], "s3://bucket/groups.bin")
            self.assertEqual(list(root.iterdir()), [audit])

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
            try:
                for _ in range(100):
                    if broker.socket_path.exists():
                        break
                    time.sleep(0.01)
                client = BrokerRangeReader(broker.socket_path, object_bytes=len(body))
                with self.assertRaisesRegex(ValueError, "range identity differs"):
                    client(2, 4)
                self.assertEqual((client.gets, client.bytes), (0, 0))
            finally:
                broker.stop()
                worker.join(timeout=5)
            self.assertFalse(worker.is_alive())
            self.assertEqual(json.loads(broker.audit_path.read_bytes())["gets"], 1)

    def test_existing_socket_or_dangling_symlink_is_preserved(self) -> None:
        for kind in ("socket", "symlink"):
            with self.subTest(kind=kind), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                sock = root / "broker.sock"
                broker = UnixRangeBroker(
                    sock, root / "audit.json", FakeS3Reader(b"data"),
                    allowed={(0, 4): hashlib.sha256(b"data").hexdigest()},
                    uri="s3://bucket/groups.bin", object_sha256="0" * 64,
                )
                with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as owner:
                    if kind == "socket":
                        owner.bind(str(sock))
                    else:
                        sock.symlink_to(root / "missing")
                    identity = sock.lstat()
                    with self.assertRaisesRegex(ValueError, "broker socket already exists"):
                        broker.serve_forever()
                    self.assertEqual(sock.lstat(), identity)
                    self.assertFalse(broker.audit_path.exists())

    def test_startup_failures_clean_only_owned_paths(self) -> None:
        for stage in ("bind", "chmod", "listen", "link", "audit"):
            with self.subTest(stage=stage), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                sock = root / "broker.sock"
                broker = UnixRangeBroker(
                    sock, root / "audit.json", FakeS3Reader(b"data"),
                    allowed={(0, 4): hashlib.sha256(b"data").hexdigest()},
                    uri="s3://bucket/groups.bin", object_sha256="0" * 64,
                )
                broker.stop()
                real_link = os.link

                def competing_publish(source, destination, **kwargs):
                    sock.write_bytes(b"other owner")
                    return real_link(source, destination, **kwargs)

                target = {
                    "bind": "socket.socket.bind",
                    "chmod": "os.chmod",
                    "listen": "socket.socket.listen",
                    "link": "os.link",
                    "audit": "pathlib.Path.write_text",
                }[stage]
                effect = competing_publish if stage == "link" else OSError(stage + " failed")
                with patch(target, side_effect=effect), self.assertRaises(OSError):
                    broker.serve_forever()
                if stage == "link":
                    self.assertEqual(sock.read_bytes(), b"other owner")
                    sock.unlink()
                self.assertEqual(list(root.glob("*")), [broker.audit_path] if stage != "audit" else [])


if __name__ == "__main__":
    unittest.main()
