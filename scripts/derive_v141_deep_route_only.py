#!/usr/bin/env python3
"""Extract only source-only route fields from authenticated V122 evidence."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

SOURCE_SHA = "deac3e5e9d15a54753a1543bed338e31b23daaf3f234f64afabde5e79bcc6ae6"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    source = args.evidence.read_bytes()
    if len(source) != 6_183_526 or hashlib.sha256(source).hexdigest() != SOURCE_SHA:
        raise ValueError("V122 evidence identity differs")
    lines = source.splitlines()
    if len(lines) != 1000:
        raise ValueError("V122 query count differs")
    with args.output.open("x") as target:
        for ordinal, line in enumerate(lines):
            record = json.loads(line)
            if (record["query_ordinal"] != ordinal
                    or record["source_query_ordinal"] != 9000 + ordinal
                    or len(record["nominees"]) != 512
                    or len(set(record["nominees"])) != 512
                    or len(record["primary"]) != 100
                    or not set(record["primary"]).issubset(record["nominees"])):
                raise ValueError("V122 source-only route identity differs")
            output = {name: record[name] for name in (
                "query_ordinal", "source_query_ordinal", "nominees", "primary",
                "candidate_ranges", "baseline_ranges")}
            target.write(json.dumps(output, sort_keys=True, separators=(",", ":")) + "\n")
    raw = args.output.read_bytes()
    print(json.dumps({"bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest()}))


if __name__ == "__main__":
    main()
