#!/usr/bin/env python3
"""Fixed ReLAION-1M source, cosine truth and paired graph transfer decision."""

import argparse
import json
from pathlib import Path

import numpy as np

from scripts.v164_smooth_layout_1m import source_arrays
from scripts.v271_fresh_frontier import add_rows, digest, unit
from scripts.v278_extra_reverse_1m import graphs, score

SOURCE_SHA = "2796b579f37afe99ca4aff57e282335a6a79ad30596645957d26326a0560cf86"
REQUESTS_SHA = "c250ef3c871af55ee1ea91e61214a93903b3d575ef458926b1f8c5ad0547b2c9"
ROWS, DIMS = 1_000_000, 768


def prepare(source, requests, vectors, queries, truth):
    import faiss

    if digest(source) != SOURCE_SHA or digest(requests) != REQUESTS_SHA:
        raise ValueError("ReLAION source or validation requests differ")
    source_ids, data, _ = source_arrays(source)
    if (data.shape != (ROWS, DIMS) or data.dtype != np.float32
            or len(source_ids) != ROWS or not np.isfinite(data).all()
            or not (data != 0).any(axis=1).all()):
        raise ValueError("ReLAION source geometry differs")
    np.asarray(data, dtype="<f4").tofile(vectors)
    del source_ids, data
    panel = []
    with requests.open() as stream:
        for ordinal, line in enumerate(stream):
            row = json.loads(line)
            values = np.asarray(row["query"], dtype=np.float32)
            if (ordinal >= 1000 or row["query_ordinal"] != ordinal
                    or values.shape != (DIMS,)
                    or not np.isfinite(values).all()
                    or not values.any()):
                raise ValueError("validation request geometry differs")
            panel.append(values)
    if len(panel) != 1000:
        raise ValueError("validation request count differs")
    panel = unit(np.asarray(panel, dtype=np.float32))
    panel.tofile(queries)
    corpus = np.memmap(vectors, dtype="<f4", mode="r", shape=(ROWS, DIMS))
    faiss.omp_set_num_threads(8)
    index = faiss.IndexFlatIP(DIMS)
    add_rows(index, corpus)
    _, ids = index.search(panel, 100)
    if ids.shape != (1000, 100) or (ids < 0).any():
        raise ValueError("cosine truth incomplete")
    np.asarray(ids, dtype="<u4").tofile(truth)
    print(json.dumps({"dataset": "ReLAION-1M D768 cosine",
                      "split": "used validation ordinals0-999", "k": 100,
                      "source_raw_sha256": digest(vectors),
                      "query_sha256": digest(queries),
                      "truth_sha256": digest(truth)}, sort_keys=True))


def compare(args):
    structure = graphs(args.baseline_graph, args.candidate_graph)
    structure.pop("baseline_indegree")
    old_diag, new_diag = score(args.baseline, args.truth), score(args.candidate, args.truth)
    old, new = score(args.default_baseline, args.truth), score(args.default_candidate, args.truth)
    quality = (new["misses"] * 5 <= old["misses"] * 4 if old["misses"] >= 100
               else new["p95_ms"] <= old["p95_ms"]
               and new["p99_ms"] <= old["p99_ms"])
    if (quality and new["hits"] >= 99_500 and new["hits"] >= old["hits"]
            and new["p05_hits"] >= old["p05_hits"]
            and new["p95_ms"] <= old["p95_ms"] * 1.2
            and new["p99_ms"] <= old["p99_ms"] * 1.2
            and args.candidate_rss_kib <= args.baseline_rss_kib * 1.15
            and new_diag["misses"] <= old_diag["misses"]
            and new["mean_visits"] <= old["mean_visits"] * 1.2
            and new_diag["mean_visits"] <= old_diag["mean_visits"] * 1.2
            and structure["baseline_edges_preserved"]
            and structure["bounded_source_additions"]
            and structure["candidate_max_outdegree"]
            <= max(structure["baseline_max_outdegree"], 96)
            and structure["candidate_graph_bytes"]
            <= structure["baseline_graph_bytes"] * 1.15
            and structure["candidate_min_indegree"] >= 4
            and structure["candidate_reachable"] == ROWS
            and args.build_seconds <= args.baseline_build_seconds * 1.5
            and args.build_rss_kib <= args.baseline_build_rss_kib * 1.5):
        decision = "go_http_1m"
    elif old["misses"] < 100:
        decision = "no_material_gain"
    else:
        decision = "reject_candidate"
    result = {"decision": decision, "dataset": "ReLAION-1M D768 cosine",
              "split": "used validation ordinals0-999", "k": 100, "queries": 1000,
              "baseline": old, "candidate": new,
              "diagnostic_baseline": old_diag, "diagnostic_candidate": new_diag,
              "baseline_rss_kib": args.baseline_rss_kib,
              "candidate_rss_kib": args.candidate_rss_kib,
              "diagnostic_baseline_rss_kib": args.diagnostic_baseline_rss_kib,
              "diagnostic_candidate_rss_kib": args.diagnostic_candidate_rss_kib,
              "baseline_build_seconds": args.baseline_build_seconds,
              "candidate_build_seconds": args.build_seconds,
              "baseline_build_rss_kib": args.baseline_build_rss_kib,
              "candidate_build_rss_kib": args.build_rss_kib,
              **structure}
    args.output.write_text(json.dumps(result, sort_keys=True, separators=(",", ":")) + "\n")
    print(json.dumps(result, sort_keys=True))


def main():
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)
    prep = sub.add_parser("prepare")
    for name in ("source", "requests", "vectors", "queries", "truth"):
        prep.add_argument(name, type=Path)
    paired = sub.add_parser("compare")
    for name in ("baseline", "candidate", "truth", "baseline_graph",
                 "candidate_graph", "default_baseline", "default_candidate", "output"):
        paired.add_argument(name, type=Path)
    for name in ("baseline_rss_kib", "candidate_rss_kib",
                 "diagnostic_baseline_rss_kib", "diagnostic_candidate_rss_kib",
                 "build_seconds", "build_rss_kib", "baseline_build_seconds",
                 "baseline_build_rss_kib"):
        paired.add_argument("--" + name.replace("_", "-"), type=float, required=True)
    args = parser.parse_args()
    if args.command == "prepare":
        prepare(args.source, args.requests, args.vectors, args.queries, args.truth)
    else:
        compare(args)


if __name__ == "__main__":
    main()
