#!/usr/bin/env python3
"""One sealed CoHere holdout panel, exact truth, and FAISS IVF-Flat control."""

import argparse
import hashlib
import importlib.metadata
import json
import math
import time
from pathlib import Path

import numpy as np

D = 768
SHARD_SHA = "1fae833dd9cbdb775b177176a2d301f0ee887988575b1152c13d7d0517cd25f4"
SOURCE_SHA = "6c82a340e3e1b4226640e593efa9c4000c6a5962d4b13063093a1dab689a9005"
RECEIPT_SHA = "0965aa0241199822dfac3410bba4edad5536ac0eb0aaa8ab83c216e8c5749a87"


def digest(path):
    value = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            value.update(block)
    return value.hexdigest()


def corpus(path, rows):
    if path.stat().st_size != 1_000_000 * D * 4 or digest(path) != SOURCE_SHA:
        raise ValueError("canonical first1M source differs")
    return np.memmap(path, dtype="<f4", mode="r", shape=(1_000_000, D))[:rows]


def unit(values):
    block = np.array(values, dtype=np.float32, copy=True, order="C")
    norms = np.linalg.norm(block, axis=1)
    if (
        not np.isfinite(block).all()
        or not np.isfinite(norms).all()
        or (norms == 0).any()
    ):
        raise ValueError("nonfinite or zero vector")
    block /= norms[:, None]
    return block


def add_rows(index, data):
    for start in range(0, len(data), 65_536):
        index.add(unit(data[start : start + 65_536]))


def prepare(args):
    import faiss
    import pyarrow as pa
    import pyarrow.parquet as pq

    if digest(args.receipt) != RECEIPT_SHA or digest(args.shard) != SHARD_SHA:
        raise ValueError("sealed source receipt or holdout shard differs")
    roster = json.loads(args.receipt.read_text())
    shards = sorted(
        (row for row in roster["objects"] if row["role"] == "train"),
        key=lambda row: row["uri"],
    )
    if (
        len(shards) != 458
        or not shards[45]["uri"].endswith("train-00000045.parquet")
        or shards[45]["sha256"] != SHARD_SHA
        or shards[45]["bytes"] != args.shard.stat().st_size
    ):
        raise ValueError("holdout source roster differs")
    first = sum(row["rows"] for row in shards[:45])
    table = pq.read_table(args.shard, columns=["emb"])
    field = table.schema.field("emb")
    if (
        first != 983_025
        or table.num_rows != shards[45]["rows"]
        or not pa.types.is_fixed_size_list(field.type)
        or field.type.list_size != D
        or field.type.value_type != pa.float32()
        or table.column("emb").null_count
    ):
        raise ValueError("holdout geometry differs")
    values = np.asarray(
        table.column("emb").combine_chunks().values.to_numpy(), dtype="<f4"
    ).reshape(table.num_rows, D)
    panel = np.ascontiguousarray(values[1_000_000 - first : 1_001_000 - first])
    if panel.shape != (1000, D):
        raise ValueError("holdout panel differs")
    unit(panel).tofile(args.queries)
    data = corpus(args.source, 1_000_000)
    faiss.omp_set_num_threads(8)
    for rows, count, output in (
        (100_000, 100, args.truth100),
        (1_000_000, 1000, args.truth1m),
    ):
        exact = faiss.IndexFlatIP(D)
        add_rows(exact, data[:rows])
        _, ids = exact.search(unit(panel[:count]), 100)
        if ids.shape != (count, 100) or (ids < 0).any():
            raise ValueError("exact truth incomplete")
        np.asarray(ids, dtype="<u4").tofile(output)
        del exact
    print(
        json.dumps(
            {
                "schema": "borsuk-v271-fresh-panel-v1",
                "dataset": "CoHere-large-10M first1M D768 cosine",
                "holdout_rows": [1_000_000, 1_000_999],
                "query_sha256": digest(args.queries),
                "truth100_sha256": digest(args.truth100),
                "truth1m_sha256": digest(args.truth1m),
                "faiss_version": importlib.metadata.version("faiss-cpu"),
            },
            sort_keys=True,
        )
    )


def control(args):
    import faiss

    data = corpus(args.source, 1_000_000)
    q = np.fromfile(args.queries, dtype="<f4").reshape(1000, D)
    q = unit(q)
    faiss.omp_set_num_threads(8)
    index = faiss.IndexIVFFlat(
        faiss.IndexFlatIP(D), D, 2048, faiss.METRIC_INNER_PRODUCT
    )
    training = np.linspace(0, len(data) - 1, 100_000, dtype=np.int64)
    started = time.perf_counter()
    index.train(unit(data[training]))
    add_rows(index, data)
    build_ms = (time.perf_counter() - started) * 1000
    faiss.omp_set_num_threads(1)
    with args.raw.open("x") as output:
        started = time.perf_counter()
        for nprobe in (16, 32, 64, 128, 256, 512, 1024):
            index.nprobe = nprobe
            index.search(q[:1], 100)
            for ordinal, query in enumerate(q):
                tick = time.perf_counter_ns()
                _, ids = index.search(query.reshape(1, D), 100)
                latency = time.perf_counter_ns() - tick
                output.write(
                    json.dumps(
                        {
                            "arm": f"faiss-ivfflat-{nprobe}",
                            "ordinal": ordinal,
                            "ids": ids[0].tolist(),
                            "latency_ns": latency,
                        },
                        separators=(",", ":"),
                    )
                    + "\n"
                )
        elapsed_ms = (time.perf_counter() - started) * 1000
    print(
        json.dumps(
            {
                "schema": "borsuk-v271-faiss-control-v1",
                "version": importlib.metadata.version("faiss-cpu"),
                "rows": index.ntotal,
                "dimensions": D,
                "lists": 2048,
                "training_rows": 100_000,
                "build_ms": build_ms,
                "query_elapsed_ms": elapsed_ms,
                "query_threads": 1,
                "raw_sha256": digest(args.raw),
            },
            sort_keys=True,
        )
    )


def percentile(values, percent):
    return sorted(values)[math.ceil(len(values) * percent / 100) - 1]


def score(args):
    truth = np.fromfile(args.truth, dtype="<u4").reshape(-1, 100)
    groups = {}
    for line in args.raw.read_text().splitlines():
        row = json.loads(line)
        groups.setdefault(row.get("arm", args.label), []).append(row)
    faiss_arms = {
        f"faiss-ivfflat-{value}" for value in (16, 32, 64, 128, 256, 512, 1024)
    }
    if any(arm.startswith("faiss-") for arm in groups) and set(groups) != faiss_arms:
        raise ValueError("FAISS frontier arms differ")
    result = {}
    max_id = 100_000 if len(truth) == 100 else 1_000_000
    for arm, rows in groups.items():
        if len(rows) != len(truth):
            raise ValueError("query count differs")
        hits = 0
        latency = []
        for ordinal, row in enumerate(rows):
            ids = row["ids"]
            if (
                row["ordinal"] != ordinal
                or len(ids) != 100
                or len(set(ids)) != 100
                or min(ids) < 0
                or max(ids) >= max_id
            ):
                raise ValueError("query identity or IDs differ")
            hits += len(set(ids) & set(map(int, truth[ordinal])))
            latency.append(row["latency_ns"] / 1_000_000)
        result[arm] = {
            "hits": hits,
            "possible_hits": len(truth) * 100,
            "recall_at_100": hits / (len(truth) * 100),
            "queries": len(truth),
            "p50_ms": percentile(latency, 50),
            "p90_ms": percentile(latency, 90),
            "p95_ms": percentile(latency, 95),
            "p99_ms": percentile(latency, 99),
            "raw_sha256": digest(args.raw),
            "truth_sha256": digest(args.truth),
        }
    args.output.write_text(
        json.dumps(result, sort_keys=True, separators=(",", ":")) + "\n"
    )


def main():
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)
    prep = sub.add_parser("prepare")
    for key in ("receipt", "shard", "source", "queries", "truth100", "truth1m"):
        prep.add_argument("--" + key, type=Path, required=True)
    faiss = sub.add_parser("control")
    for key in ("source", "queries", "raw"):
        faiss.add_argument("--" + key, type=Path, required=True)
    scorer = sub.add_parser("score")
    for key in ("raw", "truth", "output"):
        scorer.add_argument("--" + key, type=Path, required=True)
    scorer.add_argument("--label", default="borsuk")
    args = parser.parse_args()
    {"prepare": prepare, "control": control, "score": score}[args.command](args)


if __name__ == "__main__":
    main()
