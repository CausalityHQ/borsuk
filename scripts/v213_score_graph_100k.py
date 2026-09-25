#!/usr/bin/env python3
"""Score sealed V213 graph IDs against paired 100k ground truth and V193."""

import argparse
import json
from math import ceil
from pathlib import Path

from scripts.v158_pq_primary_returned import jsonl, load_truth, sha256
from scripts.v210_resident_neighborhood_100k import BASELINE_SHA, COUNT, canonical

EFS = (256, 512, 1024, 2048)


def run(args: argparse.Namespace) -> None:
    serving = json.loads(args.serving.read_text())
    raw, prior = list(jsonl(args.raw)), list(jsonl(args.baseline))
    if (serving.get("schema") != "borsuk-v213-resident-graph-100k-v1"
            or serving.get("raw_sha256") != sha256(args.raw)
            or serving.get("queries") != COUNT or len(raw) != COUNT
            or sha256(args.baseline) != BASELINE_SHA or len(prior) != 1000):
        raise ValueError("V213 pretruth/baseline identity differs")
    truth = load_truth(args.truth)
    baseline = [row["arms"]["full_rank"]["hits"] for row in prior[:COUNT]]
    if sum(baseline) != 25440:
        raise ValueError("paired V193 baseline differs")
    summaries = {}
    for ef in EFS:
        hits = []
        for index, row in enumerate(raw):
            if row["ordinal"] != index or row["vector_body_gets"] != 0:
                raise ValueError("raw request identity differs")
            ids = row["arms"][str(ef)]["returned_ids"]
            if len(ids) != 100 or len(set(ids)) != 100:
                raise ValueError("graph result roster differs")
            hits.append(len(set(ids) & set(truth[index])))
        p05 = sorted(hits)[ceil(COUNT * .05) - 1]
        p95_ns = serving["arms"][str(ef)]["p95_ns"]
        summaries[str(ef)] = {
            "hits": sum(hits), "p05_hits": p05,
            "below_98": sum(value < 98 for value in hits),
            "paired_wins": sum(a > b for a, b in zip(hits, baseline, strict=True)),
            "paired_losses": sum(a < b for a, b in zip(hits, baseline, strict=True)),
            "whole_p95_ms": p95_ns / 1e6,
            "passes_internal_gate": sum(hits) >= sum(baseline) and p05 >= 98
                and p95_ns <= 10_000_000 and serving["vector_body_gets"] == 0,
        }
    winner = next((ef for ef in EFS if summaries[str(ef)]["passes_internal_gate"]), None)
    args.summary.write_text(canonical({
        "schema": "borsuk-v213-resident-graph-100k-summary-v1",
        "dataset": "ReLAION-100k D768", "split": "development-first-256-already-used",
        "queries": COUNT, "baseline_full_rank_hits": sum(baseline),
        "arms": summaries, "smallest_passing_ef": winner,
        "raw_sha256": sha256(args.raw), "serving_sha256": sha256(args.serving),
        "truth_sha256": sha256(args.truth), "baseline_sha256": BASELINE_SHA,
    }))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    for name in ("serving", "raw", "baseline", "truth", "summary"):
        parser.add_argument("--" + name, type=Path, required=True)
    run(parser.parse_args())
