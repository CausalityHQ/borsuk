#!/usr/bin/env python3
"""Check sealed CoHere HTTP IDs against the frozen V261 arm and exact GT100."""

import argparse
import hashlib
import json
import math
import struct
from pathlib import Path

RAW_SHA = "d8566803b49197ae454d8ecc3775adc8bd4e9a48629885db546a043b9ae94246"
TRUTH_SHA = "62e14eba043fafb8d8ec7c833d7d320c5d823c549683d15e5eacdff365a87f39"
ARM = "dual-graph-4096-2048"


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def percentile(values, pct):
    return sorted(values)[math.ceil(len(values) * pct / 100) - 1]


def run(args):
    if digest(args.reference) != RAW_SHA or digest(args.truth) != TRUTH_SHA:
        raise ValueError("V261 reference or exact truth differs")
    summary = json.loads(args.summary.read_text())
    if (summary["schema"] != "borsuk-v262-cohere-dual-http-client-1m-v1"
            or summary["raw_sha256"] != digest(args.raw)
            or summary["queries"] != 1000 or summary["concurrency"] != 8
            or summary["transport"] != "persistent HTTP/1.1 VPC peer"):
        raise ValueError("HTTP measurement identity differs")
    current = [json.loads(line) for line in args.raw.read_text().splitlines()]
    reference = [json.loads(line) for line in args.reference.read_text().splitlines()]
    truth_bytes = args.truth.read_bytes()
    if len(current) != 1000 or len(reference) != 1000 or len(truth_bytes) != 400_000:
        raise ValueError("query or truth panel length differs")
    truth = struct.iter_unpack("<100I", truth_bytes)
    hits = []
    parity = 0
    for i, (row, old, gold) in enumerate(zip(current, reference, truth, strict=True)):
        ids = row["returned_ids"]
        if (row["ordinal"] != i or old["ordinal"] != i or len(ids) != 100
                or len(set(ids)) != 100 or row["vector_body_gets"] != 0):
            raise ValueError(f"query geometry differs at {i}")
        parity += ids == old["arms"][ARM]["returned_ids"]
        hits.append(len(set(ids) & set(gold)))
    result = {"schema": "borsuk-v262-cohere-dual-http-quality-v1",
              "queries": 1000, "exact_v261_id_lists": parity,
              "hits": sum(hits), "dev_hits": sum(hits[:256]),
              "validation_hits": sum(hits[256:]),
              "dev_p05_hits": percentile(hits[:256], 5),
              "validation_p05_hits": percentile(hits[256:], 5),
              "quality_pass": (parity == 1000 and sum(hits[:256]) == 25_511
                               and sum(hits[256:]) == 74_206
                               and percentile(hits[:256], 5) == 98
                               and percentile(hits[256:], 5) == 99),
              "raw_sha256": digest(args.raw), "reference_sha256": RAW_SHA,
              "truth_sha256": TRUTH_SHA, "summary_sha256": digest(args.summary)}
    args.output.write_text(json.dumps(result, sort_keys=True, separators=(",", ":")) + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    for name in ("raw", "reference", "summary", "truth", "output"):
        parser.add_argument("--" + name, type=Path, required=True)
    run(parser.parse_args())
