#!/usr/bin/env python3
"""Score sealed V218 IDs against GT100 and the paired V215/V216 arm."""

import argparse
import json
from math import ceil
from pathlib import Path

from scripts.v158_pq_primary_returned import jsonl, load_truth, sha256
from scripts.v210_resident_neighborhood_100k import canonical

ARM = "2048-2048"
BASE_SHA = (
    "40e24eb455b94714442a871033585373a587688c85091bdcf79ab2c6a34108c9",
    "bbf2aac2d54e07556974dfb6f86d32161607e535d2bfc14bd9d9b7e52c5090f0",
)


def hits(ids: list[int], truth: list[int]) -> int:
    if len(ids) != 100 or len(set(ids)) != 100:
        raise ValueError("returned ID roster differs")
    return len(set(ids) & set(truth))


def run(args: argparse.Namespace) -> None:
    serving = json.loads(args.serving.read_text())
    raw = list(jsonl(args.raw))
    before = list(jsonl(args.v215)) + list(jsonl(args.v216))
    if (serving.get("schema") != "borsuk-v218-reachable-graph-100k-serving-v1"
            or serving.get("raw_sha256") != sha256(args.raw)
            or serving.get("queries") != 1000 or len(raw) != 1000
            or tuple(map(sha256, (args.v215, args.v216))) != BASE_SHA
            or len(before) != 1000):
        raise ValueError("V218 pretruth or paired baseline identity differs")
    truth = load_truth(args.truth)
    if len(truth) < 1000:
        raise ValueError("GT100 panel length differs")
    new_hits, old_hits, exact_hits = [], [], []
    for ordinal, (current, previous) in enumerate(zip(raw, before, strict=True)):
        if (current["ordinal"] != ordinal
                or previous["ordinal"] != ordinal
                or current["vector_body_gets"] != 0):
            raise ValueError("query ordinal or GET count differs")
        new_hits.append(hits(current["arms"][ARM]["returned_ids"], truth[ordinal]))
        old_hits.append(hits(previous["arms"][ARM]["returned_ids"], truth[ordinal]))
        exact_hits.append(hits(current["exact_fp16_navigation"]["returned_ids"],
                               truth[ordinal]))
    if sum(old_hits[:256]) != 25473 or sum(old_hits[256:]) != 74048:
        raise ValueError("paired V215/V216 hit count differs")

    def split(start: int, end: int) -> dict:
        new, old, exact = new_hits[start:end], old_hits[start:end], exact_hits[start:end]
        return {"queries": end - start, "pq_hits": sum(new),
                "paired_prior_hits": sum(old), "exact_fp16_navigation_hits": sum(exact),
                "pq_p05_hits": sorted(new)[ceil(len(new) * .05) - 1],
                "pq_paired_wins": sum(a > b for a, b in zip(new, old, strict=True)),
                "pq_paired_ties": sum(a == b for a, b in zip(new, old, strict=True)),
                "pq_paired_losses": sum(a < b for a, b in zip(new, old, strict=True))}

    development = split(0, 256)
    heldout = split(256, 1000)
    combined = split(0, 1000)
    stats = serving["arms"][ARM]["loaded"]
    passed = (combined["pq_hits"] >= 99521
              and combined["pq_p05_hits"] >= 98
              and development["pq_hits"] >= 25473
              and heldout["pq_hits"] >= 74048
              and stats["p95_ns"] < 10_000_000
              and serving["process_peak_rss_bytes"] < 256 * 1024 * 1024
              and serving["vector_body_gets"] == 0)
    args.output.write_text(canonical({
        "schema": "borsuk-v218-reachable-graph-100k-quality-v1",
        "dataset": "ReLAION-100k D768", "k": 100,
        "development_first_256_prior_used": development,
        "method_heldout_744_prior_used": heldout,
        "combined_1000_prior_used": combined,
        "internal_gate_pass": passed,
        "raw_sha256": sha256(args.raw), "serving_sha256": sha256(args.serving),
        "truth_sha256": sha256(args.truth),
        "v215_raw_sha256": BASE_SHA[0], "v216_raw_sha256": BASE_SHA[1],
    }))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    for name in ("serving", "raw", "v215", "v216", "truth", "output"):
        parser.add_argument("--" + name, required=True, type=Path)
    run(parser.parse_args())
