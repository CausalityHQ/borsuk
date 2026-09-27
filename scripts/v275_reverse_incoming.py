#!/usr/bin/env python3
"""Fixed V275 truth, graph-structure audit and paired decision."""

import argparse
import json
import struct
from collections import deque
from pathlib import Path

import numpy as np

from scripts.v273_construction_quality import prepare, score

QUERY_SHA = "51cbd05d06c84f75c7f3394e42366a1e963447c0ebc90233351409cfda6a44c3"
ROOT_SHA = "440beefd321dfeeae25ba6277a2e2f1f938c389a0c4a9007177f4bacc4b1d6e3"


def graph(path):
    data = memoryview(path.read_bytes())
    if data[:8] != b"BORSVG01" or struct.unpack_from("<I", data, 8)[0] != 1:
        raise ValueError("graph format")
    rows = struct.unpack_from("<Q", data, 12)[0]
    if rows != 100_000:
        raise ValueError("graph rows")
    offset, indegree, degrees, edges = 96, [0] * rows, [], []
    for row in range(rows):
        layers = data[offset]
        offset += 1
        if layers == 0:
            raise ValueError("graph tower")
        for layer in range(layers):
            count = struct.unpack_from("<H", data, offset)[0]
            offset += 2
            if layer == layers - 1:
                base = struct.unpack_from(f"<{count}I", data, offset)
                if len(set(base)) != count or row in base or any(node >= rows for node in base):
                    raise ValueError("invalid base edges")
                degrees.append(count)
                edges.append(base)
                for node in base:
                    indegree[node] += 1
            offset += count * 4
    if offset != len(data):
        raise ValueError("graph trailing bytes")
    seen = bytearray(rows)
    entry = struct.unpack_from("<I", data, 92)[0]
    if entry >= rows:
        raise ValueError("graph entry")
    seen[entry] = 1
    queue = deque([entry])
    while queue:
        for node in edges[queue.popleft()]:
            if not seen[node]:
                seen[node] = 1
                queue.append(node)
    return {"indegree": indegree, "degrees": degrees,
            "reachable": sum(seen), "min_indegree": min(indegree),
            "fraction_indegree_le8": sum(value <= 8 for value in indegree) / rows}


def low_degree_misses(raw, truth, indegree):
    exact = np.fromfile(truth, dtype="<u4").reshape(1000, 100)
    misses = 0
    count = 0
    with raw.open() as source:
        for ordinal, line in enumerate(source):
            row = json.loads(line)
            if ordinal >= 1000 or row["ordinal"] != ordinal:
                raise ValueError("raw query ordinal")
            got = set(row["ids"])
            misses += sum(int(node) not in got and indegree[int(node)] <= 8
                          for node in exact[ordinal])
            count += 1
    if count != 1000:
        raise ValueError("query count")
    return misses


def compare(args):
    old_graph, new_graph = graph(args.baseline_graph), graph(args.candidate_graph)
    old, new = score(args.baseline, args.truth), score(args.candidate, args.truth)
    old_low = low_degree_misses(args.baseline, args.truth, old_graph["indegree"])
    new_low = low_degree_misses(args.candidate, args.truth, old_graph["indegree"])
    same_degrees = old_graph["degrees"] == new_graph["degrees"]
    if old_low < 100:
        decision = "inconclusive"
    elif (new_low * 10 <= old_low * 7 and new["misses"] <= old["misses"]
          and new["p05_hits"] >= old["p05_hits"]
          and new["mean_visits"] <= old["mean_visits"] * 1.1
          and new["p95_ms"] <= old["p95_ms"] * 1.1
          and new["p99_ms"] <= old["p99_ms"] * 1.1
          and args.candidate_rss_kib <= args.baseline_rss_kib * 1.1
          and new_graph["fraction_indegree_le8"]
          <= old_graph["fraction_indegree_le8"] * 0.5
          and same_degrees and new_graph["min_indegree"] >= 4
          and new_graph["reachable"] == 100_000
          and args.build_seconds <= 201.8 * 1.5
          and args.build_rss_kib <= 840_720 * 1.5):
        decision = "go_1m"
    else:
        decision = "reject_candidate"
    result = {"decision": decision, "dataset": "CoHere-large-10M first100k D768 cosine",
              "split": "excluded train rows102000-102999", "k": 100,
              "queries": 1000, "query_sha256": QUERY_SHA,
              "baseline_root_sha256": ROOT_SHA,
              "baseline": old, "candidate": new,
              "baseline_low_degree_misses": old_low,
              "candidate_low_degree_misses": new_low,
              "baseline_fraction_indegree_le8": old_graph["fraction_indegree_le8"],
              "candidate_fraction_indegree_le8": new_graph["fraction_indegree_le8"],
              "same_outdegree_per_row": same_degrees,
              "baseline_min_indegree": old_graph["min_indegree"],
              "candidate_min_indegree": new_graph["min_indegree"],
              "candidate_reachable": new_graph["reachable"],
              "baseline_rss_kib": args.baseline_rss_kib,
              "candidate_rss_kib": args.candidate_rss_kib,
              "build_seconds": args.build_seconds,
              "build_rss_kib": args.build_rss_kib,
              "default_baseline": score(args.default_baseline, args.truth),
              "default_candidate": score(args.default_candidate, args.truth)}
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
    for name in ("baseline_rss_kib", "candidate_rss_kib", "build_seconds",
                 "build_rss_kib"):
        paired.add_argument("--" + name.replace("_", "-"), type=float, required=True)
    args = parser.parse_args()
    if args.command == "prepare":
        prepare(args.source, args.queries, args.truth, QUERY_SHA)
    else:
        compare(args)


if __name__ == "__main__":
    main()
