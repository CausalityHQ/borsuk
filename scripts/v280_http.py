#!/usr/bin/env python3
"""Score V280 HTTP IDs against authenticated V279 local IDs and cosine GT100."""

import argparse
import json
import struct
from pathlib import Path

from scripts.v220_bench_graph_http import digest

REFERENCE_SHA = "291c10bcea15003959d556fdb7827522a498c166f3ea3a9dca30480ecc733d41"
TRUTH_SHA = "23180f9b7e7727f478672d919e08e7bc17bdb75a76c9308d69dbc44c41bc1db7"
QUERY_SHA = "82fe696c1d765d27b9f8f6c5277a998a4ebbf2bd6d6f89533ad3a5445105525a"


def score_rows(serving, reference, truth, k):
    hits = parity = 0
    if not (len(serving) == len(reference) == len(truth)):
        raise ValueError("panel lengths differ")
    for ordinal, (row, old, gold) in enumerate(zip(serving, reference, truth, strict=True)):
        ids = row["returned_ids"]
        if (row["ordinal"] != ordinal or old["ordinal"] != ordinal
                or len(ids) != k or len(set(ids)) != k
                or len(old["ids"]) != k or row["vector_body_gets"] != 0
                or row["whole_ns"] <= 0):
            raise ValueError(f"query geometry differs at {ordinal}")
        parity += ids == old["ids"]
        hits += len(set(ids) & set(gold))
    return {"hits": hits, "parity": parity}


def run(args):
    if digest(args.reference) != REFERENCE_SHA or digest(args.truth) != TRUTH_SHA:
        raise ValueError("V279 reference or cosine truth differs")
    summary = json.loads(args.summary.read_text())
    if (summary["schema"] != "borsuk-v280-relaion-http-client-1m-v1"
            or summary["raw_sha256"] != digest(args.raw)
            or summary["requests_sha256"] != QUERY_SHA
            or summary["queries"] != 1000 or summary["concurrency"] != 8
            or summary["transport"] != "persistent HTTP/1.1 VPC peer"):
        raise ValueError("V280 HTTP measurement identity differs")
    serving = [json.loads(line) for line in args.raw.read_text().splitlines()]
    reference = [json.loads(line) for line in args.reference.read_text().splitlines()]
    truth_bytes = args.truth.read_bytes()
    if len(serving) != 1000 or len(reference) != 1000 or len(truth_bytes) != 400_000:
        raise ValueError("V280 panel length differs")
    truth = list(struct.iter_unpack("<100I", truth_bytes))
    result = score_rows(serving, reference, truth, 100)
    result.update({"schema": "borsuk-v280-relaion-http-quality-v1",
                   "queries": 1000, "quality_pass": result == {"hits": 99989, "parity": 1000},
                   "raw_sha256": digest(args.raw), "reference_sha256": REFERENCE_SHA,
                   "truth_sha256": TRUTH_SHA, "summary_sha256": digest(args.summary)})
    args.output.write_text(json.dumps(result, sort_keys=True, separators=(",", ":")) + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    for name in ("raw", "reference", "summary", "truth", "output"):
        parser.add_argument("--" + name, type=Path, required=True)
    run(parser.parse_args())
