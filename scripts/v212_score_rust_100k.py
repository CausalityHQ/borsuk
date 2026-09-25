#!/usr/bin/env python3
"""Score terminal-closed Rust IDs against paired 100k GT and V193."""

import argparse
import json
from math import ceil
from pathlib import Path

from scripts.v158_pq_primary_returned import jsonl, load_truth, sha256
from scripts.v210_resident_neighborhood_100k import BASELINE_SHA, COUNT, canonical


def run(args: argparse.Namespace) -> None:
    serving = json.loads(args.serving.read_text())
    raw, prior = list(jsonl(args.raw)), list(jsonl(args.baseline))
    if (serving.get("schema") != "borsuk-v212-rust-resident-100k-v1"
            or serving.get("raw_sha256") != sha256(args.raw)
            or serving.get("queries") != COUNT or len(raw) != COUNT
            or sha256(args.baseline) != BASELINE_SHA or len(prior) != 1000):
        raise ValueError("Rust/pretruth/baseline identity differs")
    truth = load_truth(args.truth)
    hits, baseline = [], []
    for index, (row, old) in enumerate(zip(raw, prior[:COUNT], strict=True)):
        ids = row["returned_ids"]
        if (row["ordinal"] != index or old["query_ordinal"] != index
                or len(ids) != 100 or len(set(ids)) != 100
                or row["vector_body_gets"] != 0):
            raise ValueError(f"case geometry differs at {index}")
        hits.append(len(set(ids) & set(truth[index])))
        baseline.append(old["arms"]["full_rank"]["hits"])
    if sum(baseline) != 25440:
        raise ValueError("V193 paired baseline differs")
    ordered = sorted(hits)
    p05 = ordered[ceil(COUNT * .05) - 1]
    p95_ns = serving["whole"]["p95_ns"]
    args.summary.write_text(canonical({
        "schema": "borsuk-v212-rust-resident-100k-summary-v1",
        "dataset": "ReLAION-100k D768", "split": "development-first-256-already-used",
        "queries": COUNT, "hits": sum(hits), "p05_hits": p05,
        "below_98": sum(value < 98 for value in hits),
        "baseline_full_rank_hits": sum(baseline),
        "paired_wins": sum(a > b for a, b in zip(hits, baseline, strict=True)),
        "paired_losses": sum(a < b for a, b in zip(hits, baseline, strict=True)),
        "whole_p95_ms": p95_ns / 1e6,
        "passes_internal_gate": sum(hits) >= sum(baseline) and p05 >= 98
            and p95_ns <= 10_000_000 and serving["vector_body_gets"] == 0,
        "raw_sha256": sha256(args.raw), "serving_sha256": sha256(args.serving),
        "truth_sha256": sha256(args.truth), "baseline_sha256": BASELINE_SHA,
    }))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    for name in ("serving", "raw", "baseline", "truth", "summary"):
        parser.add_argument("--" + name, type=Path, required=True)
    run(parser.parse_args())
