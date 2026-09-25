#!/usr/bin/env python3
"""Check sealed HTTP returns against the authenticated V219 and GT witness."""

import argparse
import json
from pathlib import Path

from scripts.v219_score_reachable_graph_1m import V198_SHA, digest, records


def run(args: argparse.Namespace) -> None:
    if digest(args.truth) != V198_SHA:
        raise ValueError("V198 GT witness differs")
    summary = json.loads(args.summary.read_text())
    if (summary["schema"] != "borsuk-v220-graph-http-1m-v1"
            or summary["raw_sha256"] != digest(args.raw)
            or summary["queries"] != 1000 or summary["concurrency"] != 8):
        raise ValueError("HTTP measurement identity differs")
    current, previous, truth = records(args.raw), records(args.previous), records(args.truth)
    if len(current) != 1000 or len(previous) != 1000 or len(truth) != 1000:
        raise ValueError("paired panel length differs")
    hits = []
    for index, (row, old, gold) in enumerate(zip(current, previous, truth, strict=True)):
        ids = row["returned_ids"]
        if (row["ordinal"] != index or old["ordinal"] != index
                or gold["ordinal"] != index or len(ids) != 100
                or len(set(ids)) != 100 or row["vector_body_gets"] != 0
                or ids != old["arms"]["4096-4096"]["returned_ids"]):
            raise ValueError(f"HTTP/V219 exact parity differs at {index}")
        hits.append(len(set(ids) & set(gold["gold_ids"])))
    result = {
        "schema": "borsuk-v220-graph-http-1m-quality-v1", "queries": 1000,
        "hits": sum(hits), "p05_hits": sorted(hits)[49],
        "exact_v219_parity": True, "raw_sha256": digest(args.raw),
        "previous_sha256": digest(args.previous), "truth_sha256": V198_SHA,
        "measurement_sha256": digest(args.summary),
    }
    if result["hits"] != 99664 or result["p05_hits"] != 98:
        raise ValueError("V219 paired quality differs")
    args.output.write_text(json.dumps(result, sort_keys=True, separators=(",", ":")) + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    for name in ("raw", "summary", "previous", "truth", "output"):
        parser.add_argument("--" + name, type=Path, required=True)
    run(parser.parse_args())
