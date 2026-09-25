#!/usr/bin/env python3
"""Score a sealed 1M same-vector mutation HTTP pass against exact GT100."""

import argparse
import json
from pathlib import Path

from scripts.v219_score_reachable_graph_1m import V198_SHA, digest, records

BASE_HITS = 99_664
BASE_P95_NS = 20_820_000
BASE_P99_NS = 22_564_000


def run(args: argparse.Namespace) -> None:
    if digest(args.truth) != V198_SHA:
        raise ValueError("V198 GT witness differs")
    summary = json.loads(args.summary.read_text())
    if (summary["schema"] != "borsuk-v222-graph-http-client-1m-v1"
            or summary["raw_sha256"] != digest(args.raw)
            or summary["queries"] != 1000 or summary["concurrency"] != 8
            or summary["transport"] != "persistent HTTP/1.1 VPC peer"):
        raise ValueError("HTTP measurement identity differs")
    current, previous, truth = records(args.raw), records(args.previous), records(args.truth)
    if len(current) != 1000 or len(previous) != 1000 or len(truth) != 1000:
        raise ValueError("paired panel length differs")
    hits = []
    previous_hits = []
    ties = 0
    for index, (row, old, gold) in enumerate(zip(current, previous, truth, strict=True)):
        ids = row["returned_ids"]
        old_ids = old["arms"]["4096-4096"]["returned_ids"]
        if (row["ordinal"] != index or old["ordinal"] != index
                or gold["ordinal"] != index or len(ids) != 100
                or len(set(ids)) != 100 or row["vector_body_gets"] != 0):
            raise ValueError(f"case geometry differs at {index}")
        ties += ids == old_ids
        hits.append(len(set(ids) & set(gold["gold_ids"])))
        previous_hits.append(len(set(old_ids) & set(gold["gold_ids"])))
    if sum(previous_hits) != BASE_HITS:
        raise ValueError("V219 baseline hits differ")
    # The validation panel is previously used; do not call either half held out.
    halves = [{"candidate_hits": sum(hits[lo:hi]),
               "v219_hits": sum(previous_hits[lo:hi])}
              for lo, hi in ((0, 256), (256, 1000))]
    quality_pass = (all(row["candidate_hits"] >= row["v219_hits"] for row in halves)
                    and sorted(hits)[49] >= 98)
    perf_pass = (summary["p95_ns"] <= 2 * BASE_P95_NS
                 and summary["p99_ns"] <= 2 * BASE_P99_NS
                 and summary["qps"] >= 200)
    args.output.write_text(json.dumps({
        "schema": "borsuk-v230-mutation-graph-http-quality-v1",
        "pass_label": summary["pass_label"], "queries": 1000,
        "dataset": "ReLAION-1M D768", "split": "validation-1000-prior-used",
        "development_256": halves[0], "remaining_744": halves[1],
        "candidate_hits": sum(hits), "v219_hits": BASE_HITS,
        "p05_hits_per_query": sorted(hits)[49], "exact_v219_id_lists": ties,
        "quality_pass": quality_pass, "performance_pass": perf_pass,
        "pass": quality_pass and perf_pass,
        "raw_sha256": digest(args.raw), "summary_sha256": digest(args.summary),
        "previous_sha256": digest(args.previous), "truth_sha256": V198_SHA,
    }, sort_keys=True, separators=(",", ":")) + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    for name in ("raw", "summary", "previous", "truth", "output"):
        parser.add_argument("--" + name, type=Path, required=True)
    run(parser.parse_args())
