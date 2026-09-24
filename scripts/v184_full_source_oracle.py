#!/usr/bin/env python3
"""Exact-source truth-aware resource oracle on V182's closed holdout."""

from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path

import numpy as np

from scripts.global_source_oracle import search_get_prices
from scripts.source_cover_frontier import CoverFrontier
from scripts.sparse_cover_frontier import min_units_frontier, reconstruct_min_cover
from scripts.v155_relaion_returned_quality import (
    DIMS, LAYOUT_SHA, ROWS, SOURCE_SHA, sha256,
)
from scripts.v164_smooth_layout_1m import source_arrays
from scripts.v165_unit_interval_resources import (
    UNIT_ROWS, V164_ORDER_SHA, V164_TERMINAL_SHA, load_orders,
)
from scripts.v166_surrogate_ranking_run import canonical, records

SCHEMA = "borsuk-v184-full-source-oracle-v1"
V182_COMMIT = "34fd11f56559cc7d453f3bd04c92aba83d62d5fc"
V182_TERMINAL_SHA = "fa3de27a1d56747dbc4e2afd1af3a976e88ed5b90626d4f6dd8f525f426841cb"
UNIT_BYTES = 24_960
QUERY_COUNT = 128
MAX_GETS = 32
MAX_MISSES = 55
MAX_TAIL = 6
MIN_TAIL_HITS = 98
V155_BYTES = 11_134_007_040
V155_GETS = 22_126
UNIT_BUDGET = (V155_BYTES * QUERY_COUNT) // (1000 * UNIT_BYTES)
GET_BUDGET = (V155_GETS * QUERY_COUNT) // 1000


def _closed_v182(root: Path) -> tuple[list[dict], list[dict]]:
    if sha256(root / "terminal.json") != V182_TERMINAL_SHA:
        raise ValueError("V184 V182 terminal digest differs")
    terminal = json.loads((root / "terminal.json").read_text())
    if (terminal.get("status") != "complete" or terminal.get("exit_code") != 0
            or terminal.get("source_commit") != V182_COMMIT):
        raise ValueError("V184 V182 terminal state differs")
    for name in ("features.jsonl", "holdout-labels.jsonl"):
        identity = terminal.get("artifacts", {}).get("out/" + name)
        if identity is None or sha256(root / name) != identity.get("sha256"):
            raise ValueError(f"V184 V182 artifact digest differs: {name}")
    features = records(root / "features.jsonl")[QUERY_COUNT:]
    labels = records(root / "holdout-labels.jsonl")
    if (len(features) != QUERY_COUNT or len(labels) != QUERY_COUNT
            or [row.get("ordinal") for row in features]
                != list(range(1280, 1408))
            or [row.get("ordinal") for row in labels]
                != list(range(1280, 1408))
            or any(feature.get("source_id") != label.get("source_id")
                   for feature, label in zip(features, labels, strict=True))):
        raise ValueError("V184 V182 holdout roster differs")
    return features, labels


def _normalized(vectors: np.ndarray) -> np.ndarray:
    source = np.empty((ROWS, DIMS), dtype=np.float64)
    for start in range(0, ROWS, 8192):
        stop = min(start + 8192, ROWS)
        block = np.asarray(vectors[start:stop], dtype=np.float64)
        norms = np.linalg.norm(block, axis=1)
        if not np.isfinite(norms).all() or (norms <= 0).any():
            raise ValueError("V184 source norm differs")
        source[start:stop] = block / norms[:, None]
    return source


def _truth_rows(source: np.ndarray, source_ids: np.ndarray,
                source_row: int) -> np.ndarray:
    scores = source @ source[source_row]
    scores[source_row] = -np.inf
    top = np.argpartition(scores, ROWS - 100)[ROWS - 100:]
    tied = np.flatnonzero(scores >= scores[top].min())
    return tied[np.lexsort((source_ids[tied], -scores[tied]))[:100]]


def run(args: argparse.Namespace) -> None:
    if args.output.exists():
        raise ValueError("V184 output already exists")
    features, labels = _closed_v182(args.v182)
    if (sha256(args.source) != SOURCE_SHA
            or sha256(args.old_layout) != LAYOUT_SHA
            or sha256(args.order) != V164_ORDER_SHA
            or sha256(args.v164_terminal) != V164_TERMINAL_SHA):
        raise ValueError("V184 frozen source/layout identity differs")
    _, inverse_new = load_orders(
        args.old_layout, args.order, args.v164_terminal)
    source_ids, vectors, _ = source_arrays(args.source)
    source = _normalized(vectors)
    args.output.mkdir(parents=True)
    frontiers = []
    masses_by_query = []
    with (args.output / "full-truth-units.jsonl").open("x") as output:
        for feature, label in zip(features, labels, strict=True):
            row = feature["source_row"]
            if (type(row) is not int or not 0 <= row < ROWS
                    or int(source_ids[row]) != feature["source_id"]):
                raise ValueError("V184 source query identity differs")
            truth_rows = _truth_rows(source, source_ids, row)
            masses = dict(Counter(map(int, inverse_new[truth_rows] // UNIT_ROWS)))
            if sum(masses.values()) != 100:
                raise ValueError("V184 truth mass differs")
            candidate = set(feature["ranked_units"])
            candidate_masses = {unit: mass for unit, mass in masses.items()
                                if unit in candidate}
            if (candidate_masses != dict(label["candidate_truth_by_unit"])
                    or sum(candidate_masses.values()) != label["candidate_hits"]):
                raise ValueError("V184 independently mapped V182 labels differ")
            mandatory = feature["mandatory_units"]
            frontier = min_units_frontier(
                masses, mandatory, page_count=ROWS // UNIT_ROWS,
                max_gets=MAX_GETS)
            floor = CoverFrontier(ROWS // UNIT_ROWS, MAX_GETS,
                                  ROWS // UNIT_ROWS, mandatory).minimum_units
            if int(frontier[MAX_GETS].min()) != floor:
                raise ValueError("V184 primary floor disagrees with truth DP")
            frontiers.append(frontier)
            masses_by_query.append(masses)
            output.write(canonical({
                "ordinal": feature["ordinal"],
                "source_id": feature["source_id"],
                "truth_ids": [int(source_ids[value]) for value in truth_rows],
                "truth_by_unit": sorted([unit, mass]
                                        for unit, mass in masses.items()),
                "candidate_hits": label["candidate_hits"],
                "mandatory_floor_units": floor,
            }))
    np.save(args.output / "frontiers.npy", np.stack(frontiers))
    search = search_get_prices(
        frontiers, truth_per_query=100, max_misses=MAX_MISSES,
        max_tail_queries=MAX_TAIL, tail_min_hits=MIN_TAIL_HITS,
        aggregate_get_budget=GET_BUDGET,
        aggregate_unit_budget=UNIT_BUDGET)
    witness_path = args.output / "witness-plans.jsonl"
    witness = None
    with witness_path.open("x") as output:
        if search.witness_price is not None:
            chosen = dict(search.evaluations)[search.witness_price]
            hits_by_query = []
            total_units = total_gets = 0
            for feature, masses, choice in zip(
                    features, masses_by_query, chosen.choices, strict=True):
                intervals = reconstruct_min_cover(
                    masses, feature["mandatory_units"],
                    page_count=ROWS // UNIT_ROWS,
                    max_gets=choice.gets, target_hits=choice.hits)
                units = sum(end - start + 1 for start, end in intervals)
                actual_hits = sum(mass for unit, mass in masses.items()
                                  if any(start <= unit <= end
                                         for start, end in intervals))
                if units != choice.units or actual_hits != choice.hits:
                    raise ValueError("V184 independent witness accounting differs")
                total_units += units
                total_gets += len(intervals)
                hits_by_query.append(actual_hits)
                output.write(canonical({
                    "ordinal": feature["ordinal"],
                    "source_id": feature["source_id"],
                    "intervals": [list(value) for value in intervals],
                    "hits": actual_hits, "units": units,
                    "gets": len(intervals),
                }))
            if (total_units > UNIT_BUDGET or total_gets > GET_BUDGET
                    or sum(hits_by_query) < QUERY_COUNT * 100 - MAX_MISSES
                    or sorted(hits_by_query)[MAX_TAIL] < MIN_TAIL_HITS):
                raise ValueError("V184 witness violates aggregate gate")
            witness = {"price": search.witness_price,
                       "hits": sum(hits_by_query),
                       "p05": sorted(hits_by_query)[MAX_TAIL],
                       "units": total_units,
                       "bytes": total_units * UNIT_BYTES,
                       "gets": total_gets}
    (args.output / "summary.json").write_text(canonical({
        "schema": SCHEMA + "-summary", "truth_aware": True,
        "source_only": True, "dataset": "ReLAION-1M D768",
        "split": "source-pseudoquery-hash-ranks-1281-1408",
        "query_count": QUERY_COUNT, "hit_threshold": 12745,
        "unit_budget": UNIT_BUDGET, "get_budget": GET_BUDGET,
        "byte_budget": UNIT_BUDGET * UNIT_BYTES,
        "v155_unpaired_scaled_bytes_numerator": V155_BYTES * QUERY_COUNT,
        "v155_unpaired_scaled_gets_numerator": V155_GETS * QUERY_COUNT,
        "v155_scaled_denominator": 1000,
        "strongest_unit_lower_bound": search.strongest_unit_lower_bound,
        "price_evaluations": [{
            "get_price": price, "dual_unit_lower_bound":
                choice.priced_units - price * GET_BUDGET,
            "hits": choice.hits, "tail_queries": choice.tail_queries,
            "units": choice.units, "gets": choice.gets,
        } for price, choice in search.evaluations],
        "decision": search.decision, "witness": witness,
        "v182_terminal_sha256": V182_TERMINAL_SHA,
        "full_truth_units_sha256": sha256(args.output / "full-truth-units.jsonl"),
        "frontiers_sha256": sha256(args.output / "frontiers.npy"),
        "witness_plans_sha256": sha256(witness_path),
    }))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--v182", type=Path, required=True)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--old-layout", type=Path, required=True)
    parser.add_argument("--order", type=Path, required=True)
    parser.add_argument("--v164-terminal", type=Path, required=True)
    run(parser.parse_args())


if __name__ == "__main__":
    main()
