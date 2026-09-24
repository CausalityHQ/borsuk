#!/usr/bin/env python3
"""Extract GT-free ReLAION primary row ordinals from authenticated V116 replay."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

SOURCE_SHA = "3bfd155ac5f9e1b7aacbc263e1732e2314c9722f235d3454e0f17d0c6bc3c960"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--replay", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    source = args.replay.read_bytes()
    if len(source) != 13_455_525 or hashlib.sha256(source).hexdigest() != SOURCE_SHA:
        raise ValueError("V116 source replay identity differs")
    lines = source.splitlines()
    if len(lines) != 1000:
        raise ValueError("V116 query count differs")
    with args.output.open("x") as target:
        for ordinal, line in enumerate(lines):
            row = json.loads(line)
            primary = row["primary"]
            if (row["query_ordinal"] != ordinal or len(primary) != 100
                    or len(set(primary)) != 100
                    or any(not isinstance(value, int) or not 0 <= value < 1_000_000
                           for value in primary)):
                raise ValueError("V116 primary rows differ")
            target.write(json.dumps({"query_ordinal": ordinal, "primary": primary},
                                    sort_keys=True, separators=(",", ":")) + "\n")
    result = args.output.read_bytes()
    print(json.dumps({"bytes": len(result), "sha256": hashlib.sha256(result).hexdigest()}))


if __name__ == "__main__":
    main()
