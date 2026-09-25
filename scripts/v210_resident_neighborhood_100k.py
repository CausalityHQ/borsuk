#!/usr/bin/env python3
"""GT-blind 100k resident physical-neighborhood budget falsifier."""

import argparse
import hashlib
import json
import time
from math import ceil
from pathlib import Path

import numpy as np

from scripts.v158_pq_primary_returned import authenticate, jsonl, load_truth, sha256
from scripts.v160_geometric_relayout_primary import DTYPE
from scripts.v163_smooth_layout_100k import source_vectors

SCHEMA = "borsuk-v210-resident-neighborhood-100k-v1"
ROWS, DIMS, COUNT = 100_000, 768, 256
BUDGETS = (512, 2048, 8192, 32768)
ORDER_SHA = "d7be74b09ade0a7477b62c2e14d68b94640dede5ac38be240e6e7428d41ac6e6"
SOURCE_SHA = "a199e151b89a496ed20e39fdd951591bbfb4817d682e9111ebe2e1cab7ae550d"
BASELINE_SHA = "80e1ef702a46ec30b5ac6a2d5fe42ed8643a59719b28284b67eaa06fa64bca19"


def canonical(value: dict) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n"


def prepare(args: argparse.Namespace) -> None:
    if any(path.exists() for path in (args.raw, args.seal)):
        raise ValueError("pretruth outputs exist")
    authenticate(args.old_sq8, "sq8")
    authenticate(args.requests, "requests")
    if sha256(args.order) != ORDER_SHA or sha256(args.source) != SOURCE_SHA:
        raise ValueError("source/order identity differs")
    old = np.memmap(args.old_sq8, dtype=DTYPE, mode="r", shape=(ROWS,))
    vectors = source_vectors(args.source, old)
    order = np.load(args.order, allow_pickle=False)
    if (order.shape != (ROWS,) or order.dtype != np.int64
            or not np.array_equal(np.sort(order), np.arange(ROWS))):
        raise ValueError("new order is not a bijection")
    inverse = np.empty(ROWS, dtype=np.int64)
    inverse[order] = np.arange(ROWS)
    ids = np.asarray(old["id"][order], dtype=np.int64)
    resident = vectors[order].astype(np.float16).astype(np.float32)
    norms = np.linalg.norm(resident, axis=1)
    if np.any(~np.isfinite(norms)) or np.any(norms <= 0):
        raise ValueError("resident FP16 geometry differs")
    positions = np.arange(ROWS, dtype=np.int64)
    requests = list(jsonl(args.requests))
    if len(requests) != 1000:
        raise ValueError("request count differs")
    with args.raw.open("x") as output:
        for index, request in enumerate(requests[:COUNT]):
            query = np.asarray(request["query"], dtype=np.float32)
            nominees = np.asarray(request["nominees"], dtype=np.int64)
            if (request["query_ordinal"] != index or query.shape != (DIMS,)
                    or nominees.shape != (512,) or np.unique(nominees).size != 512
                    or not np.isfinite(query).all() or np.linalg.norm(query) <= 0):
                raise ValueError(f"query/nominee geometry differs at {index}")
            seeds = np.sort(inverse[nominees])
            started = time.perf_counter_ns()
            insertion = np.searchsorted(seeds, positions)
            left = np.where(insertion > 0, seeds[np.maximum(insertion - 1, 0)], -ROWS)
            right = np.where(insertion < 512, seeds[np.minimum(insertion, 511)], 2 * ROWS)
            distance = np.minimum(np.abs(positions - left), np.abs(right - positions))
            nearest = np.lexsort((positions, distance))
            query = query / np.linalg.norm(query)
            arms = {}
            for budget in BUDGETS:
                first = time.perf_counter_ns()
                chosen = nearest[:budget]
                scores = (resident[chosen] @ query) / norms[chosen]
                top = np.lexsort((ids[chosen], -scores))[:100]
                arms[str(budget)] = {
                    "returned_ids": [int(value) for value in ids[chosen[top]]],
                    "score_ns": time.perf_counter_ns() - first,
                }
            output.write(canonical({"ordinal": index, "arms": arms,
                                    "nearest_ns": time.perf_counter_ns() - started}))
    args.seal.write_text(canonical({
        "schema": SCHEMA + "-pretruth-seal", "gt_opened": False,
        "dataset": "ReLAION-100k D768", "split": "development-first-256-already-used",
        "source_sha256": SOURCE_SHA, "requests_sha256": sha256(args.requests),
        "old_sq8_sha256": sha256(args.old_sq8), "order_sha256": ORDER_SHA,
        "budgets": BUDGETS, "raw_sha256": sha256(args.raw),
    }))


def score(args: argparse.Namespace) -> None:
    if any(getattr(args, name) is None for name in ("truth", "baseline", "summary")):
        raise ValueError("score phase requires truth, baseline and summary paths")
    if sha256(args.seal) != args.seal_sha256:
        raise ValueError("external GT-blind seal differs")
    seal = json.loads(args.seal.read_text())
    if (seal.get("schema") != SCHEMA + "-pretruth-seal"
            or seal.get("gt_opened") is not False
            or seal.get("raw_sha256") != sha256(args.raw)
            or tuple(seal.get("budgets", ())) != BUDGETS
            or sha256(args.baseline) != BASELINE_SHA):
        raise ValueError("frozen pretruth/baseline differs")
    truth = load_truth(args.truth)
    raw = list(jsonl(args.raw))
    prior = list(jsonl(args.baseline))
    if len(raw) != COUNT or len(prior) != 1000:
        raise ValueError("closed query panel differs")
    baseline = [int(row["arms"]["full_rank"]["hits"]) for row in prior[:COUNT]]
    if any(row["query_ordinal"] != index for index, row in enumerate(prior[:COUNT])):
        raise ValueError("baseline query identities differ")
    arms = {}
    for budget in BUDGETS:
        hits, times = [], []
        for index, row in enumerate(raw):
            candidate = row["arms"][str(budget)]
            ids = candidate["returned_ids"]
            if row["ordinal"] != index or len(ids) != 100 or len(set(ids)) != 100:
                raise ValueError(f"returned geometry differs at {index}")
            hits.append(len(set(ids) & set(truth[index])))
            times.append(candidate["score_ns"])
        ordered = sorted(hits)
        timing = sorted(times)
        arms[str(budget)] = {
            "hits": sum(hits), "p05_hits": ordered[ceil(COUNT * .05) - 1],
            "below_98": sum(value < 98 for value in hits),
            "score_only_p95_ms": timing[ceil(COUNT * .95) - 1] / 1e6,
            "vs_full_rank_wins": sum(a > b for a, b in zip(hits, baseline, strict=True)),
            "vs_full_rank_losses": sum(a < b for a, b in zip(hits, baseline, strict=True)),
        }
    qualifying = [budget for budget in BUDGETS if
                  arms[str(budget)]["hits"] >= sum(baseline)
                  and arms[str(budget)]["p05_hits"] >= 98
                  and arms[str(budget)]["score_only_p95_ms"] <= 10.0]
    args.summary.write_text(canonical({
        "schema": SCHEMA + "-summary", "dataset": "ReLAION-100k D768",
        "split": "development-first-256-already-used", "queries": COUNT,
        "baseline_full_rank_hits": sum(baseline), "arms": arms,
        "qualifying_budget": min(qualifying) if qualifying else None,
        "raw_sha256": sha256(args.raw), "seal_sha256": sha256(args.seal),
        "truth_sha256": sha256(args.truth), "baseline_sha256": BASELINE_SHA,
    }))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("phase", choices=("prepare", "score"))
    for name in ("source", "old_sq8", "order", "requests", "raw", "seal",
                 "truth", "baseline", "summary"):
        parser.add_argument("--" + name.replace("_", "-"), type=Path,
                            required=name not in {"truth", "baseline", "summary"})
    parser.add_argument("--seal-sha256")
    arguments = parser.parse_args()
    if arguments.phase == "prepare":
        prepare(arguments)
    else:
        score(arguments)
