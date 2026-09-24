#!/usr/bin/env python3
"""Derive a GT-free primary witness file from authenticated V122 evidence."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

SOURCE_SHA = "deac3e5e9d15a54753a1543bed338e31b23daaf3f234f64afabde5e79bcc6ae6"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sealed-evidence", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    source = args.sealed_evidence.read_bytes()
    if hashlib.sha256(source).hexdigest() != SOURCE_SHA:
        raise ValueError("V122 evidence SHA-256 differs")
    records = [json.loads(line) for line in source.splitlines()]
    if len(records) != 1000:
        raise ValueError("V122 query count differs")
    with args.output.open("x") as output:
        for ordinal, record in enumerate(records):
            primary = record["primary"]
            if (
                record["query_ordinal"] != ordinal
                or record["source_query_ordinal"] != 9000 + ordinal
                or len(primary) != 100
                or len(set(primary)) != 100
                or any(type(row) is not int or row < 0 or row >= 100000 for row in primary)
            ):
                raise ValueError("V122 primary witness differs")
            output.write(json.dumps({
                "query_ordinal": ordinal,
                "source_query_ordinal": 9000 + ordinal,
                "primary": primary,
            }, sort_keys=True, separators=(",", ":")) + "\n")
    print(hashlib.sha256(args.output.read_bytes()).hexdigest())


if __name__ == "__main__":
    main()
