#!/usr/bin/env python3
"""Serve only authenticated historical two-bit ranges to the paired evaluator."""

from __future__ import annotations

import argparse
import signal
from pathlib import Path

import boto3
from botocore.config import Config

from scripts.native_hundred_thousand_opq8_cell import _read_prior_code_seal
from scripts.native_hundred_thousand_opq8_paired_cell import TWO_BIT_SOURCE
from scripts.native_rotated_two_bit_range_broker import UnixRangeBroker
from scripts.native_row_score_s3 import S3CodeRangeReader


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--socket", type=Path, required=True)
    parser.add_argument("--audit", type=Path, required=True)
    args = parser.parse_args()
    if not (args.root / "source-seal.json").exists() or not (args.root / "plan-seal.json").exists():
        raise ValueError("OPQ8 paired broker authority missing")
    seal = _read_prior_code_seal(args.root / "opq")
    identity = TWO_BIT_SOURCE["groups"]
    if seal["groups_sha256"] != identity.sha256:
        raise ValueError("OPQ8 paired group object differs")
    allowed: dict[tuple[int, int], str] = {}
    next_offset = 0
    for ordinal, group in enumerate(seal["group_ranges"]):
        first, end, offset, length, digest = group
        if first != ordinal * 4 or end != min(first + 4, 166) or offset != next_offset:
            raise ValueError("OPQ8 paired group range differs")
        allowed[(offset, length)] = digest
        next_offset += length
    if next_offset != identity.encoded_bytes or len(allowed) != len(seal["group_ranges"]):
        raise ValueError("OPQ8 paired group length differs")
    client = boto3.client(
        "s3", region_name="eu-central-1",
        config=Config(retries={"mode": "standard", "total_max_attempts": 1}),
    )
    reader = S3CodeRangeReader(client, identity.uri, object_bytes=identity.encoded_bytes)
    broker = UnixRangeBroker(
        args.socket, args.audit, reader,
        allowed=allowed, uri=identity.uri, object_sha256=identity.sha256,
    )
    signal.signal(signal.SIGTERM, lambda *_: broker.stop())
    signal.signal(signal.SIGINT, lambda *_: broker.stop())
    broker.serve_forever()


if __name__ == "__main__":
    main()
