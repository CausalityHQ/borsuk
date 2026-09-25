#!/usr/bin/env python3
"""Score sealed V216 held-out IDs against paired GT100 and V193."""

import argparse
import json
from math import ceil
from pathlib import Path

from scripts.v158_pq_primary_returned import jsonl, load_truth, sha256
from scripts.v210_resident_neighborhood_100k import BASELINE_SHA, canonical

START = 256
COUNT = 744
ARMS = ((2048, 2048),)


def run(args: argparse.Namespace) -> None:
    serving = json.loads(args.serving.read_text())
    raw, prior = list(jsonl(args.raw)), list(jsonl(args.baseline))
    if (serving.get("schema") != "borsuk-v216-pq-cosine-heldout-100k-v1"
            or serving.get("raw_sha256") != sha256(args.raw)
            or serving.get("queries") != COUNT or len(raw) != COUNT
            or sha256(args.baseline) != BASELINE_SHA or len(prior) != 1000):
        raise ValueError("V216 pretruth/baseline identity differs")
    truth = load_truth(args.truth)
    paired = prior[START:START + COUNT]
    if any(row["query_ordinal"] != START + index for index, row in enumerate(paired)):
        raise ValueError("V193 held-out ordinal differs")
    baseline = [row["arms"]["full_rank"]["hits"] for row in paired]
    summaries = {}
    for ef, shortlist in ARMS:
        arm = f"{ef}-{shortlist}"
        hits = []
        for index, row in enumerate(raw):
            if row["ordinal"] != index + START or row["vector_body_gets"] != 0:
                raise ValueError("raw request identity differs")
            ids = row["arms"][arm]["returned_ids"]
            if len(ids) != 100 or len(set(ids)) != 100:
                raise ValueError("graph result roster differs")
            hits.append(len(set(ids) & set(truth[index + START])))
        p05 = sorted(hits)[ceil(COUNT * .05) - 1]
        p95_ns = serving["arms"][arm]["p95_ns"]
        summaries[arm] = {
            "hits": sum(hits), "p05_hits": p05,
            "below_98": sum(value < 98 for value in hits),
            "paired_wins": sum(a > b for a, b in zip(hits, baseline, strict=True)),
            "paired_ties": sum(a == b for a, b in zip(hits, baseline, strict=True)),
            "paired_losses": sum(a < b for a, b in zip(hits, baseline, strict=True)),
            "whole_p95_ms": p95_ns / 1e6,
            "passes_internal_gate": sum(hits) >= sum(baseline) and p05 >= 98
                and p95_ns <= 10_000_000
                and serving["serving_peak_rss_bytes"] <= 320_000_000
                and serving["vector_body_gets"] == 0,
        }
    winner = min((arm for arm in summaries if summaries[arm]["passes_internal_gate"]),
                 key=lambda arm: (summaries[arm]["whole_p95_ms"],
                                  int(arm.split("-")[0]), int(arm.split("-")[1])),
                 default=None)
    args.summary.write_text(canonical({
        "schema": "borsuk-v216-pq-cosine-heldout-100k-summary-v1",
        "dataset": "ReLAION-100k D768", "split": "method-heldout-ordinals-256-999-prior-used",
        "queries": COUNT, "baseline_full_rank_hits": sum(baseline),
        "baseline_p05_hits": sorted(baseline)[ceil(COUNT * .05) - 1],
        "arms": summaries, "passing_arm": winner,
        "raw_sha256": sha256(args.raw), "serving_sha256": sha256(args.serving),
        "truth_sha256": sha256(args.truth), "baseline_sha256": BASELINE_SHA,
    }))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    for name in ("serving", "raw", "baseline", "truth", "summary"):
        parser.add_argument("--" + name, type=Path, required=True)
    run(parser.parse_args())
