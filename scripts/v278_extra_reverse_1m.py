#!/usr/bin/env python3
"""Fixed fresh CoHere1M exact truth and paired reverse-edge decision."""

import argparse
import json
import math
import struct
from array import array
from collections import deque
from pathlib import Path

import numpy as np

from scripts.v271_fresh_frontier import (
    D, RECEIPT_SHA, SHARD_SHA, add_rows, corpus, digest, unit,
)

ROWS = 1_000_000
BASELINE_ROOT = "c3a60f9969f8bc0fc6cf2f24831c45d3918bd7090a474c090b5812629851f86a"


def prepare(source, receipt, shard, queries, truth):
    import faiss
    import pyarrow as pa
    import pyarrow.parquet as pq

    if digest(receipt) != RECEIPT_SHA or digest(shard) != SHARD_SHA:
        raise ValueError("source receipt or holdout shard differs")
    shards = sorted((row for row in json.loads(receipt.read_text())["objects"]
                     if row["role"] == "train"), key=lambda row: row["uri"])
    first = sum(row["rows"] for row in shards[:45])
    table = pq.read_table(shard, columns=["emb"])
    field = table.schema.field("emb")
    if (len(shards) != 458 or first != 983_025
            or not shards[45]["uri"].endswith("train-00000045.parquet")
            or shards[45]["sha256"] != SHARD_SHA
            or shards[45]["bytes"] != shard.stat().st_size
            or table.num_rows != shards[45]["rows"]
            or not pa.types.is_fixed_size_list(field.type)
            or field.type.list_size != D or field.type.value_type != pa.float32()
            or table.column("emb").null_count):
        raise ValueError("holdout source geometry differs")
    values = np.asarray(table.column("emb").combine_chunks().values.to_numpy(),
                        dtype="<f4").reshape(table.num_rows, D)
    panel = unit(values[1_001_000 - first:1_002_000 - first])
    if panel.shape != (1000, D):
        raise ValueError("fresh panel incomplete")
    panel.tofile(queries)
    data = corpus(source, ROWS)
    faiss.omp_set_num_threads(8)
    index = faiss.IndexFlatIP(D)
    add_rows(index, data)
    _, ids = index.search(panel, 100)
    if ids.shape != (1000, 100) or (ids < 0).any():
        raise ValueError("exact truth incomplete")
    np.asarray(ids, dtype="<u4").tofile(truth)
    print(json.dumps({"dataset": "CoHere-large-10M train first1m D768 cosine",
                      "split": "excluded train rows1001000-1001999", "k": 100,
                      "query_sha256": digest(queries), "truth_sha256": digest(truth)},
                     sort_keys=True))


def percentile(values, percent):
    return sorted(values)[math.ceil(len(values) * percent / 100) - 1]


def score(raw, truth):
    exact = np.fromfile(truth, dtype="<u4").reshape(1000, 100)
    hits, latencies, visits = [], [], []
    with raw.open() as source:
        for ordinal, line in enumerate(source):
            row = json.loads(line)
            ids = row["ids"]
            if (ordinal >= 1000 or row["ordinal"] != ordinal
                    or len(ids) != 100 or len(set(ids)) != 100
                    or min(ids) < 0 or max(ids) >= ROWS
                    or row["latency_ns"] <= 0 or row["visits"] <= 0):
                raise ValueError("raw query identity or values differ")
            hits.append(len(set(ids) & set(map(int, exact[ordinal]))))
            latencies.append(row["latency_ns"] / 1_000_000)
            visits.append(row["visits"])
    if len(hits) != 1000:
        raise ValueError("query count differs")
    return {"hits": sum(hits), "misses": 100_000 - sum(hits),
            "p05_hits": percentile(hits, 5),
            "p50_ms": percentile(latencies, 50),
            "p90_ms": percentile(latencies, 90),
            "p95_ms": percentile(latencies, 95),
            "p99_ms": percentile(latencies, 99),
            "mean_visits": sum(visits) / 1000,
            "raw_sha256": digest(raw), "truth_sha256": digest(truth)}


def base_rows(data):
    if (data[:8] != b"BORSVG01" or struct.unpack_from("<I", data, 8)[0] != 1
            or struct.unpack_from("<Q", data, 12)[0] != ROWS):
        raise ValueError("graph header differs")
    offset = 96
    for row in range(ROWS):
        layers = data[offset]
        offset += 1
        if not layers:
            raise ValueError("empty graph tower")
        for layer in range(layers):
            count = struct.unpack_from("<H", data, offset)[0]
            offset += 2
            if layer == layers - 1:
                base = struct.unpack_from(f"<{count}I", data, offset)
                if row in base or len(set(base)) != count or any(x >= ROWS for x in base):
                    raise ValueError("invalid graph edge")
            offset += count * 4
        yield base
    if offset != len(data):
        raise ValueError("graph trailing bytes")


def graphs(baseline, candidate):
    old_data, new_data = baseline.read_bytes(), candidate.read_bytes()
    old_degree, new_degree = array("I", [0]) * ROWS, array("I", [0]) * ROWS
    old_max = new_max = max_added = total_added = 0
    preserved = bounded = True
    adjacency = []
    old_rows, new_rows = base_rows(old_data), base_rows(new_data)
    for _ in range(ROWS):
        old, new = next(old_rows), next(new_rows)
        old_size, new_size = len(old), len(new)
        old_max, new_max = max(old_max, old_size), max(new_max, new_size)
        old_set, new_set = set(old), set(new)
        added = len(new_set - old_set)
        total_added += added
        max_added = max(max_added, added)
        preserved &= old_set <= new_set
        bounded &= added <= 16 and (not added or old_size < 96)
        for node in old:
            old_degree[node] += 1
        for node in new:
            new_degree[node] += 1
        adjacency.append(new)
    if next(old_rows, None) is not None or next(new_rows, None) is not None:
        raise ValueError("graph row count differs")
    entry = struct.unpack_from("<I", new_data, 92)[0]
    if entry >= ROWS:
        raise ValueError("invalid graph entry")
    seen = bytearray(ROWS)
    seen[entry] = 1
    queue = deque([entry])
    while queue:
        for node in adjacency[queue.popleft()]:
            if not seen[node]:
                seen[node] = 1
                queue.append(node)
    return {"baseline_indegree": old_degree,
            "baseline_fraction_indegree_le8": sum(x <= 8 for x in old_degree) / ROWS,
            "candidate_fraction_indegree_le8": sum(x <= 8 for x in new_degree) / ROWS,
            "baseline_max_outdegree": old_max, "candidate_max_outdegree": new_max,
            "candidate_min_indegree": min(new_degree),
            "candidate_reachable": sum(seen),
            "baseline_edges_preserved": preserved,
            "bounded_source_additions": bounded,
            "added_edges": total_added, "max_added_per_source": max_added,
            "baseline_graph_bytes": len(old_data),
            "candidate_graph_bytes": len(new_data)}


def low_degree_misses(raw, truth, degree):
    exact = np.fromfile(truth, dtype="<u4").reshape(1000, 100)
    misses = count = 0
    with raw.open() as source:
        for ordinal, line in enumerate(source):
            row = json.loads(line)
            if ordinal >= 1000 or row["ordinal"] != ordinal:
                raise ValueError("raw query ordinal differs")
            got = set(row["ids"])
            misses += sum(int(node) not in got and degree[int(node)] <= 8
                          for node in exact[ordinal])
            count += 1
    if count != 1000:
        raise ValueError("query count differs")
    return misses


def compare(args):
    structure = graphs(args.baseline_graph, args.candidate_graph)
    baseline_degree = structure.pop("baseline_indegree")
    old_diag, new_diag = score(args.baseline, args.truth), score(args.candidate, args.truth)
    old, new = score(args.default_baseline, args.truth), score(args.default_candidate, args.truth)
    old_low = low_degree_misses(args.default_baseline, args.truth, baseline_degree)
    new_low = low_degree_misses(args.default_candidate, args.truth, baseline_degree)
    if old["misses"] < 100:
        decision = "inconclusive"
    elif (new["misses"] * 5 <= old["misses"] * 4
          and new["hits"] >= 99_750 and new["p05_hits"] >= old["p05_hits"]
          and new["p95_ms"] <= old["p95_ms"] * 1.2
          and new["p99_ms"] <= old["p99_ms"] * 1.2
          and args.candidate_rss_kib <= args.baseline_rss_kib * 1.15
          and new_diag["misses"] <= old_diag["misses"]
          and (old_low < 50 or new_low * 10 <= old_low * 7)
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
          and args.build_seconds <= 2064.570 * 1.5
          and args.build_rss_kib <= 5_547_312 * 1.5):
        decision = "go_cross_dataset_1m"
    else:
        decision = "reject_candidate"
    result = {"decision": decision,
              "dataset": "CoHere-large-10M train first1m D768 cosine",
              "split": "excluded train rows1001000-1001999", "k": 100,
              "queries": 1000, "baseline_root_sha256": BASELINE_ROOT,
              "baseline": old, "candidate": new,
              "diagnostic_baseline": old_diag, "diagnostic_candidate": new_diag,
              "baseline_low_degree_misses": old_low,
              "candidate_low_degree_misses": new_low,
              "baseline_rss_kib": args.baseline_rss_kib,
              "candidate_rss_kib": args.candidate_rss_kib,
              "diagnostic_baseline_rss_kib": args.diagnostic_baseline_rss_kib,
              "diagnostic_candidate_rss_kib": args.diagnostic_candidate_rss_kib,
              "build_seconds": args.build_seconds,
              "build_rss_kib": args.build_rss_kib,
              **structure}
    args.output.write_text(json.dumps(result, sort_keys=True, separators=(",", ":")) + "\n")
    print(json.dumps(result, sort_keys=True))


def main():
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)
    prep = sub.add_parser("prepare")
    for name in ("source", "receipt", "shard", "queries", "truth"):
        prep.add_argument(name, type=Path)
    paired = sub.add_parser("compare")
    for name in ("baseline", "candidate", "truth", "baseline_graph",
                 "candidate_graph", "default_baseline", "default_candidate", "output"):
        paired.add_argument(name, type=Path)
    for name in ("baseline_rss_kib", "candidate_rss_kib",
                 "diagnostic_baseline_rss_kib", "diagnostic_candidate_rss_kib",
                 "build_seconds", "build_rss_kib"):
        paired.add_argument("--" + name.replace("_", "-"), type=float, required=True)
    args = parser.parse_args()
    if args.command == "prepare":
        prepare(args.source, args.receipt, args.shard, args.queries, args.truth)
    else:
        compare(args)


if __name__ == "__main__":
    main()
