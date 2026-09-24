#!/usr/bin/env python3
"""Seal the used ReLAION validation requests for the paired Rust scale gate."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
from pathlib import Path

REQUEST_SHA = "c250ef3c871af55ee1ea91e61214a93903b3d575ef458926b1f8c5ad0547b2c9"
PRIMARY_SHA = "71bfdf71f293ca1d23f58694866b3ba52ee8ee95ed9b02e676a2a8bf031f3162"
ROWS = 1_000_000
QUERIES = 1000
DIMS = 768


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(4 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def records(path: Path, digest: str) -> list[dict]:
    if sha256(path) != digest:
        raise ValueError(f"frozen input differs: {path}")
    with path.open() as source:
        values = [json.loads(line) for line in source]
    if len(values) != QUERIES:
        raise ValueError("query count differs")
    return values


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--requests", type=Path, required=True)
    parser.add_argument("--primary", type=Path, required=True)
    parser.add_argument("--queries-out", type=Path, required=True)
    parser.add_argument("--routing-out", type=Path, required=True)
    parser.add_argument("--manifest-out", type=Path, required=True)
    args = parser.parse_args()
    requests = records(args.requests, REQUEST_SHA)
    primary = records(args.primary, PRIMARY_SHA)
    with args.queries_out.open("x") as query_out, args.routing_out.open("x") as route_out:
        for ordinal, (request, route) in enumerate(zip(requests, primary, strict=True)):
            if request["query_ordinal"] != ordinal or route["query_ordinal"] != ordinal:
                raise ValueError("query identity differs")
            query = request["query"]
            ids = route["primary"]
            nominees = request["nominees"]
            if (
                len(query) != DIMS
                or not all(isinstance(x, (int, float)) and math.isfinite(x) for x in query)
                or len(ids) != 100
                or len(set(ids)) != 100
                or len(nominees) != 512
                or len(set(nominees)) != 512
                or request["primary_count"] != 100
                or any(type(row) is not int or row < 0 or row >= ROWS for row in ids + nominees)
            ):
                raise ValueError(f"query or routing geometry differs: {ordinal}")
            query_out.write(json.dumps({"query_ordinal": ordinal,
                                        "source_query_ordinal": ordinal,
                                        "query": query}, separators=(",", ":")) + "\n")
            route_out.write(json.dumps({"query_ordinal": ordinal,
                                        "source_query_ordinal": ordinal,
                                        "primary": ids, "nominees": nominees},
                                       separators=(",", ":")) + "\n")
    manifest = {"schema": "borsuk-v154-relaion-seal-v1",
                "dataset": "ReLAION-1M", "split": "validation-1000-already-used",
                "requests_sha256": REQUEST_SHA, "primary_sha256": PRIMARY_SHA,
                "queries_sha256": sha256(args.queries_out),
                "routing_sha256": sha256(args.routing_out),
                "query_count": QUERIES}
    args.manifest_out.write_text(json.dumps(manifest, sort_keys=True) + "\n")


if __name__ == "__main__":
    main()
