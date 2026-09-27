#!/usr/bin/env python3
"""Fresh-query decision for the frozen V276 graph with a valid degree bound."""

import argparse
import json
from pathlib import Path

from scripts.v273_construction_quality import prepare, score
from scripts.v276_extra_reverse import graph, low_degree_misses

QUERY_SHA = "0099cdcd57a80d33437a63a3cd9e9fab4bd333ba27ad4d1cedbc2d32e2a932cb"
BASELINE_ROOT = "440beefd321dfeeae25ba6277a2e2f1f938c389a0c4a9007177f4bacc4b1d6e3"
CANDIDATE_ROOT = "56de9f4768271611683193f4fa5d36795cb5926adabb89efb7bd4c851b1e340b"


def compare(args):
    old_graph, new_graph = graph(args.baseline_graph), graph(args.candidate_graph)
    old, new = score(args.baseline, args.truth), score(args.candidate, args.truth)
    old_default = score(args.default_baseline, args.truth)
    new_default = score(args.default_candidate, args.truth)
    old_low = low_degree_misses(args.baseline, args.truth, old_graph["indegree"])
    new_low = low_degree_misses(args.candidate, args.truth, old_graph["indegree"])
    added = [len(set(current) - set(original)) for original, current in
             zip(old_graph["edges"], new_graph["edges"])]
    preserved = all(set(original).issubset(current) for original, current in
                    zip(old_graph["edges"], new_graph["edges"]))
    bounded = (max(added) <= 16 and all(count == 0 or degree < 96 for count, degree
                                       in zip(added, old_graph["degrees"])))
    baseline_max = max(old_graph["degrees"])
    candidate_max = max(new_graph["degrees"])
    baseline_bytes = args.baseline_graph.stat().st_size
    candidate_bytes = args.candidate_graph.stat().st_size
    if old_low < 100:
        decision = "inconclusive"
    elif (new_low * 10 <= old_low * 7 and new["misses"] <= old["misses"]
          and new["p05_hits"] >= old["p05_hits"]
          and new["mean_visits"] <= old["mean_visits"] * 1.2
          and new["p95_ms"] <= old["p95_ms"] * 1.2
          and new["p99_ms"] <= old["p99_ms"] * 1.2
          and args.candidate_rss_kib <= args.baseline_rss_kib * 1.15
          and new_graph["fraction_indegree_le8"]
          <= old_graph["fraction_indegree_le8"] * 0.5
          and preserved and bounded
          and candidate_max <= max(baseline_max, 96)
          and candidate_bytes <= baseline_bytes * 1.15
          and new_graph["min_indegree"] >= 4
          and new_graph["reachable"] == 100_000
          and new_default["hits"] >= old_default["hits"] - 10
          and new_default["p95_ms"] <= old_default["p95_ms"] * 1.2
          and new_default["p99_ms"] <= old_default["p99_ms"] * 1.2):
        decision = "go_1m"
    else:
        decision = "reject_candidate"
    result = {"decision": decision,
              "dataset": "CoHere-large-10M train first100k D768 cosine",
              "split": "excluded train rows104000-104999", "k": 100, "queries": 1000,
              "query_sha256": QUERY_SHA, "baseline_root_sha256": BASELINE_ROOT,
              "candidate_root_sha256": CANDIDATE_ROOT,
              "baseline": old, "candidate": new,
              "default_baseline": old_default, "default_candidate": new_default,
              "baseline_low_degree_misses": old_low,
              "candidate_low_degree_misses": new_low,
              "baseline_fraction_indegree_le8": old_graph["fraction_indegree_le8"],
              "candidate_fraction_indegree_le8": new_graph["fraction_indegree_le8"],
              "baseline_edges_preserved": preserved,
              "added_edges": sum(added), "max_added_per_source": max(added),
              "bounded_source_additions": bounded,
              "baseline_max_outdegree": baseline_max,
              "candidate_max_outdegree": candidate_max,
              "baseline_graph_bytes": baseline_bytes,
              "candidate_graph_bytes": candidate_bytes,
              "candidate_min_indegree": new_graph["min_indegree"],
              "candidate_reachable": new_graph["reachable"],
              "baseline_rss_kib": args.baseline_rss_kib,
              "candidate_rss_kib": args.candidate_rss_kib,
              "reused_v276_build_seconds": 241.285594386,
              "reused_v276_build_rss_kib": 847204}
    args.output.write_text(json.dumps(result, sort_keys=True, separators=(",", ":")) + "\n")
    print(json.dumps(result, sort_keys=True))


def main():
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)
    prep = sub.add_parser("prepare")
    for name in ("source", "queries", "truth"):
        prep.add_argument(name, type=Path)
    paired = sub.add_parser("compare")
    for name in ("baseline", "candidate", "truth", "baseline_graph",
                 "candidate_graph", "default_baseline", "default_candidate", "output"):
        paired.add_argument(name, type=Path)
    paired.add_argument("--baseline-rss-kib", type=float, required=True)
    paired.add_argument("--candidate-rss-kib", type=float, required=True)
    args = parser.parse_args()
    if args.command == "prepare":
        prepare(args.source, args.queries, args.truth, QUERY_SHA)
    else:
        compare(args)


if __name__ == "__main__":
    main()
