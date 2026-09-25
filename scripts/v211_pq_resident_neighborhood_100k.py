#!/usr/bin/env python3
"""100k source-PQ screen then bounded resident FP16 rerank."""

import argparse
import json
import time
from math import ceil
from pathlib import Path

import numpy as np

from scripts.v158_pq_primary_returned import authenticate, jsonl, load_truth, sha256
from scripts.v160_geometric_relayout_primary import DTYPE
from scripts.v163_smooth_layout_100k import source_vectors
from scripts.v210_resident_neighborhood_100k import (
    BASELINE_SHA, COUNT, DIMS, ORDER_SHA, ROWS, SOURCE_SHA, canonical,
)

SCHEMA = "borsuk-v211-pq-resident-neighborhood-100k-v1"
BUDGETS = (4096, 8192, 16384)
PQ_HASHES = {
    "ids": "d31121d0ecd43bad93bf7e313bc467f158de24510989400cb9fd7dd667c2a57a",
    "books": "e90c011aa3a013bed606d7f40b30bd41f5ed446637ac9c8724ced6cf2f078c87",
    "codes": "2eea1c265da723e799575b98ba29997eca9d58ef789a5244a96ca3bf89f6638f",
}


def load_pq(args: argparse.Namespace, old_ids: np.ndarray):
    for name, expected in PQ_HASHES.items():
        if sha256(getattr(args, "pq_" + name)) != expected:
            raise ValueError(f"V113 PQ {name} identity differs")
    ids = np.load(args.pq_ids, allow_pickle=False)
    books = np.load(args.pq_books, allow_pickle=False)
    codes = np.load(args.pq_codes, allow_pickle=False)
    if (ids.shape != (ROWS,) or ids.dtype != np.int64
            or books.shape != (64, 256, 12) or books.dtype != np.float32
            or codes.shape != (ROWS, 64) or codes.dtype != np.uint8
            or np.unique(ids).size != ROWS or not np.isfinite(books).all()):
        raise ValueError("PQ artifact geometry differs")
    sorted_pq = np.argsort(ids)
    index = np.searchsorted(ids[sorted_pq], old_ids)
    if (np.any(index >= ROWS)
            or not np.array_equal(ids[sorted_pq[index]], old_ids)):
        raise ValueError("PQ/source stable IDs differ")
    return books, codes, sorted_pq[index]


def pq_table(books: np.ndarray, query: np.ndarray) -> np.ndarray:
    padded = np.zeros((64, 12), dtype=np.float32)
    for part in range(64):
        padded[part] = query[part * 12:(part + 1) * 12]
    delta = books - padded[:, None, :]
    return np.einsum("ijk,ijk->ij", delta, delta)


def prepare(args: argparse.Namespace) -> None:
    if any(path.exists() for path in (args.raw, args.seal)):
        raise ValueError("pretruth outputs exist")
    authenticate(args.old_sq8, "sq8")
    authenticate(args.requests, "requests")
    if sha256(args.order) != ORDER_SHA or sha256(args.source) != SOURCE_SHA:
        raise ValueError("source/order identity differs")
    old = np.memmap(args.old_sq8, dtype=DTYPE, mode="r", shape=(ROWS,))
    vectors = source_vectors(args.source, old)
    books, codes, pq_for_old = load_pq(args, old["id"])
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
            insertion = np.searchsorted(seeds, positions)
            left = np.where(insertion > 0, seeds[np.maximum(insertion - 1, 0)], -ROWS)
            right = np.where(insertion < 512, seeds[np.minimum(insertion, 511)], 2 * ROWS)
            distance = np.minimum(np.abs(positions - left), np.abs(right - positions))
            chosen = np.lexsort((positions, distance))[:32768]
            table_started = time.perf_counter_ns()
            table = pq_table(books, query)
            pq_rows = pq_for_old[order[chosen]]
            pq_scores = np.zeros(chosen.size, dtype=np.float32)
            for part in range(64):
                pq_scores += table[part, codes[pq_rows, part]]
            screened = chosen[np.lexsort((ids[chosen], pq_scores))]
            screen_ns = time.perf_counter_ns() - table_started
            normalized = query / np.linalg.norm(query)
            arms = {}
            for budget in BUDGETS:
                first = time.perf_counter_ns()
                selected = screened[:budget]
                scores = (resident[selected] @ normalized) / norms[selected]
                top = np.lexsort((ids[selected], -scores))[:100]
                arms[str(budget)] = {
                    "returned_ids": [int(value) for value in ids[selected[top]]],
                    "screen_and_score_ns": screen_ns + time.perf_counter_ns() - first,
                }
            output.write(canonical({"ordinal": index, "arms": arms}))
    args.seal.write_text(canonical({
        "schema": SCHEMA + "-pretruth-seal", "gt_opened": False,
        "dataset": "ReLAION-100k D768", "split": "development-first-256-already-used",
        "source_sha256": SOURCE_SHA, "requests_sha256": sha256(args.requests),
        "old_sq8_sha256": sha256(args.old_sq8), "order_sha256": ORDER_SHA,
        "pq_sha256": PQ_HASHES, "neighborhood": 32768, "budgets": BUDGETS,
        "raw_sha256": sha256(args.raw),
    }))


def score(args: argparse.Namespace) -> None:
    if any(getattr(args, name) is None for name in ("truth", "baseline", "summary")):
        raise ValueError("score phase requires truth, baseline and summary")
    if sha256(args.seal) != args.seal_sha256:
        raise ValueError("external pretruth seal differs")
    seal = json.loads(args.seal.read_text())
    if (seal.get("schema") != SCHEMA + "-pretruth-seal"
            or seal.get("gt_opened") is not False
            or seal.get("raw_sha256") != sha256(args.raw)
            or tuple(seal.get("budgets", ())) != BUDGETS
            or seal.get("neighborhood") != 32768
            or sha256(args.baseline) != BASELINE_SHA):
        raise ValueError("pretruth/baseline identity differs")
    truth = load_truth(args.truth)
    raw, prior = list(jsonl(args.raw)), list(jsonl(args.baseline))
    if len(raw) != COUNT or len(prior) != 1000:
        raise ValueError("query panel differs")
    baseline = [int(row["arms"]["full_rank"]["hits"]) for row in prior[:COUNT]]
    if any(row["query_ordinal"] != index for index, row in enumerate(prior[:COUNT])):
        raise ValueError("baseline query identities differ")
    arms = {}
    for budget in BUDGETS:
        hits, times = [], []
        for index, row in enumerate(raw):
            arm = row["arms"][str(budget)]
            ids = arm["returned_ids"]
            if row["ordinal"] != index or len(ids) != 100 or len(set(ids)) != 100:
                raise ValueError(f"returned geometry differs at {index}")
            hits.append(len(set(ids) & set(truth[index])))
            times.append(arm["screen_and_score_ns"])
        ordered, timing = sorted(hits), sorted(times)
        arms[str(budget)] = {
            "hits": sum(hits), "p05_hits": ordered[ceil(COUNT * .05) - 1],
            "below_98": sum(value < 98 for value in hits),
            "screen_and_score_p95_ms": timing[ceil(COUNT * .95) - 1] / 1e6,
            "vs_full_rank_wins": sum(a > b for a, b in zip(hits, baseline, strict=True)),
            "vs_full_rank_losses": sum(a < b for a, b in zip(hits, baseline, strict=True)),
        }
    qualifying = [budget for budget in BUDGETS if
                  arms[str(budget)]["hits"] >= sum(baseline)
                  and arms[str(budget)]["p05_hits"] >= 98
                  and arms[str(budget)]["screen_and_score_p95_ms"] <= 10.0]
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
    for name in ("source", "old_sq8", "order", "requests", "pq_ids", "pq_books",
                 "pq_codes", "raw", "seal", "truth", "baseline", "summary"):
        parser.add_argument("--" + name.replace("_", "-"), type=Path,
                            required=name not in {"truth", "baseline", "summary"})
    parser.add_argument("--seal-sha256")
    arguments = parser.parse_args()
    if arguments.phase == "prepare":
        prepare(arguments)
    else:
        score(arguments)
