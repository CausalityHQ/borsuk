#!/usr/bin/env python3
"""V274 fixed fresh panel and paired one-hop decision."""

import argparse
import json
from pathlib import Path

from scripts.v273_construction_quality import prepare, score

QUERY_SHA = "ba64f7134f252314e12cac2b1fe8911a87f6d7112e7a472bff81d33ac86ee822"


def main():
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)
    prep = sub.add_parser("prepare")
    for name in ("source", "queries", "truth"):
        prep.add_argument(name, type=Path)
    paired = sub.add_parser("compare")
    for name in ("baseline", "candidate", "truth", "output"):
        paired.add_argument(name, type=Path)
    paired.add_argument("--baseline-rss-kib", type=int, required=True)
    paired.add_argument("--candidate-rss-kib", type=int, required=True)
    args = parser.parse_args()
    if args.command == "prepare":
        prepare(args.source, args.queries, args.truth, QUERY_SHA)
        return
    old, new = score(args.baseline, args.truth), score(args.candidate, args.truth)
    if old["misses"] < 100:
        decision = "inconclusive"
    elif (new["misses"] * 10 <= old["misses"] * 7
          and new["p05_hits"] >= old["p05_hits"]
          and new["p95_ms"] <= old["p95_ms"] * 1.2
          and new["p99_ms"] <= old["p99_ms"] * 1.2
          and args.candidate_rss_kib <= args.baseline_rss_kib * 1.2):
        decision = "go_1m"
    else:
        decision = "reject_candidate"
    result = {"decision": decision, "baseline": old, "candidate": new,
              "baseline_rss_kib": args.baseline_rss_kib,
              "candidate_rss_kib": args.candidate_rss_kib,
              "dataset": "CoHere-large-10M train first100k D768 cosine",
              "split": "excluded train rows101000-101999", "queries": 1000,
              "k": 100, "query_sha256": QUERY_SHA,
              "baseline_root_sha256": "440beefd321dfeeae25ba6277a2e2f1f938c389a0c4a9007177f4bacc4b1d6e3"}
    args.output.write_text(json.dumps(result, sort_keys=True, separators=(",", ":")) + "\n")
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
