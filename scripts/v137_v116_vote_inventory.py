#!/usr/bin/env python3
"""Recount voted-page admissions from V116's complete ReLAION replay."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

REPLAY_SHA = "3bfd155ac5f9e1b7aacbc263e1732e2314c9722f235d3454e0f17d0c6bc3c960"
PAGE_BYTES = 256 * (768 + 12)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--replay", required=True, type=Path)
    args = parser.parse_args()
    raw = args.replay.read_bytes()
    if hashlib.sha256(raw).hexdigest() != REPLAY_SHA:
        raise ValueError("sealed V116 replay differs")
    rows = [json.loads(line) for line in raw.splitlines()]
    if len(rows) != 1_000:
        raise ValueError("V116 query inventory differs")
    counts = []
    summary = {
        "voted_pages": 0, "baseline_dropped_voted_pages": 0,
        "candidate_dropped_voted_pages": 0,
        "queries_with_baseline_vote_drop": 0,
        "queries_with_candidate_vote_drop": 0,
        "baseline_queries_at_32_gets": 0,
        "candidate_queries_at_32_gets": 0,
    }
    for ordinal, row in enumerate(rows):
        if row["query_ordinal"] != ordinal:
            raise ValueError("V116 query ordinal differs")
        voted = [page for page, weight in row["page_votes"] if weight > 0]
        if len(voted) != len(row["page_votes"]) or len(set(voted)) != len(voted):
            raise ValueError("V116 page votes differ")
        baseline = row["baseline_ranges"]
        candidate = row["ranges"]
        missing_baseline = sum(
            not any(first <= page * PAGE_BYTES < last for first, last in baseline)
            for page in voted
        )
        missing_candidate = sum(
            not any(first <= page * PAGE_BYTES < last for first, last in candidate)
            for page in voted
        )
        summary["voted_pages"] += len(voted)
        summary["baseline_dropped_voted_pages"] += missing_baseline
        summary["candidate_dropped_voted_pages"] += missing_candidate
        summary["queries_with_baseline_vote_drop"] += missing_baseline > 0
        summary["queries_with_candidate_vote_drop"] += missing_candidate > 0
        summary["baseline_queries_at_32_gets"] += len(baseline) == 32
        summary["candidate_queries_at_32_gets"] += len(candidate) == 32
        counts.append(len(voted))
    summary.update({"schema": "borsuk-v137-v116-vote-inventory-v1",
                    "dataset": "ReLAION-1M", "split": "used validation-1000",
                    "p50_voted_pages": sorted(counts)[499],
                    "p95_voted_pages": sorted(counts)[949]})
    print(json.dumps(summary, sort_keys=True))


if __name__ == "__main__":
    main()
