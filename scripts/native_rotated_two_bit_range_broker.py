#!/usr/bin/env python3
"""Sealed S3 group range broker for a network-isolated, unprivileged evaluator."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import signal
import socket
import struct
import time
from pathlib import Path
from typing import Any

from scripts.native_page_microcluster_cell import FROZEN_INPUTS
from scripts.native_rotated_two_bit_cell import PRIOR_TREE, read_cell_seal
from scripts.native_row_score_s3 import S3CodeRangeReader

SCHEMA = "borsuk-rotated-two-bit-broker-audit-v1"
REQUEST = struct.Struct("<QI")
RESPONSE = struct.Struct("<BI")


def _recv_exact(connection: socket.socket, length: int) -> bytes:
    pieces = []
    remaining = length
    while remaining:
        chunk = connection.recv(remaining)
        if not chunk:
            raise ValueError("broker connection ended early")
        pieces.append(chunk)
        remaining -= len(chunk)
    return b"".join(pieces)


class BrokerRangeReader:
    """Only receives authenticated code bytes over a local Unix socket."""

    def __init__(self, socket_path: Path, *, object_bytes: int) -> None:
        if type(object_bytes) is not int or object_bytes <= 0:
            raise ValueError("broker object length differs")
        self.socket_path = socket_path
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
            raise ValueError("broker requested range differs")
        started = time.perf_counter_ns()
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as connection:
            connection.settimeout(30)
            connection.connect(str(self.socket_path))
            connection.sendall(REQUEST.pack(offset, length))
            status, response_length = RESPONSE.unpack(_recv_exact(connection, RESPONSE.size))
            if response_length > max(length, 4096):
                raise ValueError("broker response length differs")
            body = _recv_exact(connection, response_length)
        if status != 1:
            raise ValueError("broker " + body.decode(errors="replace"))
        if response_length != length:
            raise ValueError("broker response length differs")
        self.gets += 1
        self.bytes += length
        self.nanoseconds += time.perf_counter_ns() - started
        return body


class UnixRangeBroker:
    """Serve only exact sealed group coordinates, one S3 GET per request."""

    def __init__(
        self,
        socket_path: Path,
        audit_path: Path,
        reader: Any,
        *,
        allowed: dict[tuple[int, int], str],
        uri: str,
        object_sha256: str,
    ) -> None:
        if not allowed or not uri.startswith("s3://") or len(object_sha256) != 64:
            raise ValueError("broker authority differs")
        self.socket_path = socket_path
        self.audit_path = audit_path
        self.reader = reader
        self.allowed = allowed
        self.uri = uri
        self.object_sha256 = object_sha256
        self.stopped = False

    def stop(self) -> None:
        self.stopped = True

    def _handle(self, connection: socket.socket) -> None:
        try:
            offset, length = REQUEST.unpack(_recv_exact(connection, REQUEST.size))
            digest = self.allowed.get((offset, length))
            if digest is None:
                raise ValueError("range denied")
            body = self.reader(offset, length)
            if type(body) is not bytes or hashlib.sha256(body).hexdigest() != digest:
                raise ValueError("range identity differs")
            connection.sendall(RESPONSE.pack(1, len(body)) + body)
        except Exception as error:
            message = str(error).encode()[:4096]
            try:
                connection.sendall(RESPONSE.pack(0, len(message)) + message)
            except OSError:
                pass

    def serve_forever(self) -> None:
        if self.socket_path.exists():
            raise ValueError("broker socket already exists")
        self.socket_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as listener:
                listener.bind(str(self.socket_path))
                os.chmod(self.socket_path, 0o666)
                listener.listen(16)
                listener.settimeout(0.5)
                while not self.stopped:
                    try:
                        connection, _ = listener.accept()
                    except TimeoutError:
                        continue
                    with connection:
                        connection.settimeout(35)
                        self._handle(connection)
        finally:
            audit = {
                "schema": SCHEMA,
                "uri": self.uri,
                "object_sha256": self.object_sha256,
                "allowed_ranges": len(self.allowed),
                "gets": self.reader.gets,
                "bytes": self.reader.bytes,
                "nanoseconds": self.reader.nanoseconds,
            }
            self.audit_path.write_text(
                json.dumps(audit, sort_keys=True, separators=(",", ":")) + "\n"
            )
            self.socket_path.unlink(missing_ok=True)


def _sealed_authority(root: Path, prefix: str) -> tuple[str, int, str, dict[tuple[int, int], str]]:
    identities = read_cell_seal(
        root,
        prefix,
        bytes.fromhex(FROZEN_INPUTS.layout.source.sha256),
        bytes.fromhex(FROZEN_INPUTS.membership.sha256),
        bytes.fromhex(PRIOR_TREE.sha256),
    )
    seal_body = (root / "seal.json").read_bytes()
    if hashlib.sha256(seal_body).hexdigest() != identities.seal.sha256:
        raise ValueError("broker seal identity differs")
    seal = json.loads(seal_body)
    if seal.get("groups_sha256") != identities.groups.sha256:
        raise ValueError("broker groups identity differs")
    allowed: dict[tuple[int, int], str] = {}
    next_offset = 0
    for group in seal["group_ranges"]:
        first, end, offset, length, digest = group
        if (
            type(first) is not int or type(end) is not int
            or type(offset) is not int or type(length) is not int
            or type(digest) is not str or len(digest) != 64
            or first < 0 or end <= first or offset != next_offset or length <= 0
        ):
            raise ValueError("broker range seal differs")
        allowed[(offset, length)] = digest
        next_offset += length
    if next_offset != identities.groups.encoded_bytes or len(allowed) != len(seal["group_ranges"]):
        raise ValueError("broker object length differs")
    return identities.groups.uri, identities.groups.encoded_bytes, identities.groups.sha256, allowed


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--output-prefix", required=True)
    parser.add_argument("--socket", type=Path, required=True)
    parser.add_argument("--audit", type=Path, required=True)
    args = parser.parse_args()
    uri, size, digest, allowed = _sealed_authority(args.root, args.output_prefix)
    import boto3
    from botocore.config import Config

    reader = S3CodeRangeReader(
        boto3.client(
            "s3", region_name="eu-central-1",
            config=Config(retries={"mode": "standard", "total_max_attempts": 1}),
        ),
        uri,
        object_bytes=size,
    )
    broker = UnixRangeBroker(
        args.socket, args.audit, reader,
        allowed=allowed, uri=uri, object_sha256=digest,
    )
    signal.signal(signal.SIGTERM, lambda *_: broker.stop())
    signal.signal(signal.SIGINT, lambda *_: broker.stop())
    broker.serve_forever()


if __name__ == "__main__":
    main()
