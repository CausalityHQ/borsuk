#!/usr/bin/env python3
"""Fixed 100k fresh-query truth and paired graph-quality decision."""

import argparse
import hashlib
import importlib.metadata
import json
import math
from pathlib import Path

import numpy as np

DIMS = 768
SOURCE_RAW_SHA = "0f3631d71c105e5ea3d701c96033b362c2f84bd43002a9c8a5c70040801be06e"
SOURCE_IDENTITY_SHA = "d638878523cfdd349cb28e214d59010709d6451f3eeca2a70da88c0c9d9ea753"
QUERY_SHA = "10322f59ee236849e60137c081432c3a8ef55d6c09dc1585356e984b2bfc30c0"


def digest(path):
    value = hashlib.sha256()
    with Path(path).open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            value.update(block)
    return value.hexdigest()


def source_identity(path):
    value = hashlib.sha256()
    with Path(path).open("rb") as source:
        for row_id in range(100_000):
            vector = source.read(DIMS * 4)
            if len(vector) != DIMS * 4:
                raise ValueError("source truncated")
            value.update(row_id.to_bytes(8, "little"))
            value.update(vector)
        if source.read(1):
            raise ValueError("source has trailing bytes")
    return value.hexdigest()


def unit(values):
    result = np.array(values, dtype=np.float32, copy=True, order="C")
    norm = np.linalg.norm(result, axis=1)
    if not np.isfinite(result).all() or not np.isfinite(norm).all() or (norm == 0).any():
        raise ValueError("invalid vector")
    result /= norm[:, None]
    return result


def prepare(source, queries, truth):
    import faiss

    if (digest(source) != SOURCE_RAW_SHA
            or source_identity(source) != SOURCE_IDENTITY_SHA
            or digest(queries) != QUERY_SHA):
        raise ValueError("source or fresh query SHA differs")
    rows = np.memmap(source, dtype="<f4", mode="r", shape=(100_000, DIMS))
    panel = np.memmap(queries, dtype="<f4", mode="r", shape=(1_000, DIMS))
    faiss.omp_set_num_threads(8)
    index = faiss.IndexFlatIP(DIMS)
    for start in range(0, 100_000, 25_000):
        index.add(unit(rows[start : start + 25_000]))
    _, ids = index.search(unit(panel), 100)
    if ids.shape != (1_000, 100) or (ids < 0).any():
        raise ValueError("incomplete exact truth")
    np.asarray(ids, dtype="<u4").tofile(truth)
    print(json.dumps({"truth_sha256": digest(truth), "faiss_version":
                      importlib.metadata.version("faiss-cpu")}, sort_keys=True))


def percentile(values, percent):
    return sorted(values)[math.ceil(len(values) * percent / 100) - 1]


def score(raw, truth):
    exact = np.fromfile(truth, dtype="<u4").reshape(1_000, 100)
    hits, latencies, visits = [], [], []
    with Path(raw).open() as source:
        for ordinal, line in enumerate(source):
            row = json.loads(line)
            ids = row["ids"]
            if (ordinal >= 1_000 or row["ordinal"] != ordinal or len(ids) != 100
                    or len(set(ids)) != 100 or min(ids) < 0 or max(ids) >= 100_000
                    or row["latency_ns"] <= 0):
                raise ValueError("query row identity or values differ")
            hits.append(len(set(ids) & set(map(int, exact[ordinal]))))
            latencies.append(row["latency_ns"] / 1_000_000)
            visits.append(row["visits"])
    if len(hits) != 1_000:
        raise ValueError("query count differs")
    return {"hits": sum(hits), "misses": 100_000 - sum(hits),
            "p05_hits": percentile(hits, 5), "p50_ms": percentile(latencies, 50),
            "p90_ms": percentile(latencies, 90), "p95_ms": percentile(latencies, 95),
            "p99_ms": percentile(latencies, 99), "mean_visits": sum(visits) / len(visits),
            "raw_sha256": digest(raw), "truth_sha256": digest(truth)}


def compare(baseline, candidate, truth, output):
    old, new = score(baseline, truth), score(candidate, truth)
    if old["misses"] < 100:
        decision = "inconclusive"
    elif (new["misses"] * 10 <= old["misses"] * 7
          and new["p05_hits"] >= old["p05_hits"]
          and new["p95_ms"] <= old["p95_ms"] * 1.1
          and new["p99_ms"] <= old["p99_ms"] * 1.1):
        decision = "go_1m"
    else:
        decision = "reject_candidate"
    result = {"decision": decision, "baseline": old, "candidate": new,
              "dataset": "CoHere-large-10M train first100k D768 cosine",
              "split": "excluded train rows100000-100999", "k": 100,
              "queries": 1_000, "mode": "diagnostic-stress",
              "query_sha256": QUERY_SHA, "source_raw_sha256": SOURCE_RAW_SHA,
              "source_identity_sha256": SOURCE_IDENTITY_SHA}
    Path(output).write_text(json.dumps(result, sort_keys=True, separators=(",", ":")) + "\n")
    print(json.dumps(result, sort_keys=True))


def main():
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)
    prep = sub.add_parser("prepare")
    for name in ("source", "queries", "truth"):
        prep.add_argument(name, type=Path)
    paired = sub.add_parser("compare")
    for name in ("baseline", "candidate", "truth", "output"):
        paired.add_argument(name, type=Path)
    args = parser.parse_args()
    if args.command == "prepare":
        prepare(args.source, args.queries, args.truth)
    else:
        compare(args.baseline, args.candidate, args.truth, args.output)


if __name__ == "__main__":
    main()
