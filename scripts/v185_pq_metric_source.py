#!/usr/bin/env python3
"""Sealed paired PQ L2 versus reconstructed-cosine source rank screen."""

from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path

import numpy as np

from scripts.source_rank_utility import rank_units
from scripts.v114_1m_paired import nominate_region_pq64
from scripts.v155_relaion_returned_quality import ROWS, sha256
from scripts.v166_surrogate_probe import select_pseudoqueries
from scripts.v166_surrogate_ranking_run import _score, canonical, records
from scripts.v168_scored_neighbor_field import score_neighbor_field
from scripts.v177_source_candidate_ceiling import _inputs, expanded_units
from scripts.v182_wide_pq_rank import _normalized, _truth
from scripts.v165_unit_interval_resources import UNIT_ROWS

SCHEMA = "borsuk-v185-pq-metric-source-v1"
FIRST = 1408
COUNT = 256
FIT = 128
WIDTH = 32
ALLOWANCES = (128, 256, 446, 672, 1344)
THRESHOLD = 12745
ARMS = ("squared_l2", "cosine")


def decide(holdout_totals: dict[str, dict[str, int]],
           holdout_p05: dict[str, dict[str, int]]) -> str:
    if (set(holdout_totals) != set(ARMS)
            or set(holdout_p05) != set(ARMS)
            or any("446" not in holdout_totals[arm]
                   or "446" not in holdout_p05[arm] for arm in ARMS)):
        raise ValueError("V185 paired metric decision inputs differ")
    cosine_pass = (holdout_totals["cosine"]["446"] >= THRESHOLD
                   and holdout_p05["cosine"]["446"] >= 98
                   and holdout_totals["cosine"]["446"]
                       >= holdout_totals["squared_l2"]["446"])
    l2_pass = (holdout_totals["squared_l2"]["446"] >= THRESHOLD
               and holdout_p05["squared_l2"]["446"] >= 98)
    return ("advance-cosine-to-physical-gate" if cosine_pass else
            "retain-l2" if l2_pass else
            "revise-utility-or-representation")


def _panel(source_ids: np.ndarray) -> tuple[int, ...]:
    return select_pseudoqueries(source_ids, FIRST + COUNT)[FIRST:]


def prepare(args: argparse.Namespace) -> None:
    if args.output.exists():
        raise ValueError("V185 output already exists")
    old, inverse_new, source_ids, vectors, sq8, planes = _inputs(args)
    chosen = _panel(source_ids)
    id_to_source = {int(identifier): row for row, identifier in enumerate(source_ids)}
    inverse_old = np.empty(ROWS, dtype=np.int64)
    inverse_old[old] = np.arange(ROWS, dtype=np.int64)
    new_order = np.argsort(inverse_new).astype(np.int64)
    args.output.mkdir(parents=True)
    with (args.output / "features.jsonl").open("x") as output:
        for ordinal, stable_id in enumerate(chosen, start=FIRST):
            source_row = id_to_source[stable_id]
            query = np.asarray(vectors[source_row], dtype=np.float32)
            _, _, nominated = nominate_region_pq64(
                query, planes["summaries"], planes["books"], planes["codes"],
                page_rows=256, blocks_per_page=2, regions=1024, shortlist=512)
            nominees = np.asarray(nominated, dtype=np.int64)
            if nominees.shape != (512,) or len(np.unique(nominees)) != 512:
                raise ValueError("V185 nominee roster differs")
            eligible = nominees[nominees != inverse_old[source_row]]
            _, sq8_scores = _score(sq8, query, eligible,
                                   planes["low"], planes["step"], 1)
            primary_rank = np.lexsort((sq8["id"][eligible], sq8_scores))[:100]
            primary = eligible[primary_rank]
            base = sorted(set((inverse_new[old[nominees]] // UNIT_ROWS).tolist()))
            mandatory = sorted(set((inverse_new[old[primary]] // UNIT_ROWS).tolist()))
            candidate = expanded_units(base, WIDTH, ROWS // UNIT_ROWS)
            ranked = {}
            for metric in ARMS:
                field = score_neighbor_field(
                    query, nominees=tuple(map(int, nominees)), old_order=old,
                    inverse_old=inverse_old, new_order=new_order,
                    inverse_new=inverse_new, books=planes["books"],
                    codes=planes["codes"], unit_rows=UNIT_ROWS,
                    radius=WIDTH, metric=metric)
                if field.units != candidate or field.scores.size != len(candidate) * UNIT_ROWS:
                    raise ValueError("V185 score field candidate geometry differs")
                scores = field.scores.reshape(len(candidate), UNIT_ROWS).copy()
                own = np.flatnonzero(field.old_rows == inverse_old[source_row])
                if own.size > 1:
                    raise ValueError("V185 pseudoquery row appears twice")
                if own.size == 1:
                    scores.reshape(-1)[own[0]] = np.inf
                minima = scores.min(axis=1)
                ranked[metric] = list(rank_units(dict(zip(
                    candidate, map(float, minima), strict=True))))
            output.write(canonical({
                "ordinal": ordinal, "source_id": stable_id,
                "source_row": int(source_row),
                "mandatory_units": mandatory,
                "ranked_units": ranked,
            }))
    (args.output / "prepare-seal.json").write_text(canonical({
        "schema": SCHEMA + "-prepare-seal", "source_truth_opened": False,
        "dataset": "ReLAION-1M D768",
        "split": "source-pseudoquery-hash-ranks-1409-1664",
        "fit": "1409-1536", "holdout": "1537-1664",
        "pseudo_ids": list(chosen),
        "features_sha256": sha256(args.output / "features.jsonl"),
        "candidate_width": WIDTH, "allowances": list(ALLOWANCES),
        "arms": list(ARMS), "threshold": THRESHOLD,
    }))


def evaluate(args: argparse.Namespace) -> None:
    if sha256(args.output / "prepare-seal.json") != args.prepare_sha256:
        raise ValueError("V185 externally sealed prepare SHA differs")
    seal = json.loads((args.output / "prepare-seal.json").read_text())
    features = records(args.output / "features.jsonl")
    if (seal.get("schema") != SCHEMA + "-prepare-seal"
            or seal.get("source_truth_opened") is not False
            or seal.get("features_sha256") != sha256(args.output / "features.jsonl")
            or seal.get("candidate_width") != WIDTH
            or seal.get("allowances") != list(ALLOWANCES)
            or seal.get("arms") != list(ARMS)
            or seal.get("threshold") != THRESHOLD
            or len(features) != COUNT
            or [row.get("ordinal") for row in features]
                != list(range(FIRST, FIRST + COUNT))
            or [row.get("source_id") for row in features] != seal.get("pseudo_ids")):
        raise ValueError("V185 sealed feature authority differs")
    _, inverse_new, source_ids, vectors, _, _ = _inputs(args)
    if list(_panel(source_ids)) != seal["pseudo_ids"]:
        raise ValueError("V185 source panel differs")
    source = _normalized(vectors)
    totals = {split: {arm: {str(k): 0 for k in ALLOWANCES}
                      for arm in ARMS} for split in ("fit", "holdout")}
    per_query = {split: {arm: {str(k): [] for k in ALLOWANCES}
                         for arm in ARMS} for split in ("fit", "holdout")}
    candidate_totals = {"fit": 0, "holdout": 0}
    with (args.output / "source-labels.jsonl").open("x") as output:
        for index, feature in enumerate(features):
            source_row = feature["source_row"]
            if (type(source_row) is not int or not 0 <= source_row < ROWS
                    or int(source_ids[source_row]) != feature["source_id"]):
                raise ValueError("V185 source row identity differs")
            ranked = feature["ranked_units"]
            if set(ranked) != set(ARMS):
                raise ValueError("V185 rank arm set differs")
            candidate = set(ranked[ARMS[0]])
            if (not candidate or any(len(row) != len(candidate)
                                     or set(row) != candidate
                                     for row in ranked.values())
                    or not set(feature["mandatory_units"]).issubset(candidate)):
                raise ValueError("V185 candidate/primary geometry differs")
            truth = _truth(source, source_ids, source_row)
            masses = Counter(map(int, inverse_new[truth] // UNIT_ROWS))
            split = "fit" if index < FIT else "holdout"
            candidate_hits = sum(masses.get(unit, 0) for unit in candidate)
            candidate_totals[split] += candidate_hits
            counts = {}
            for arm in ARMS:
                counts[arm] = {}
                for allowance in ALLOWANCES:
                    hits = sum(masses.get(unit, 0)
                               for unit in ranked[arm][:allowance])
                    key = str(allowance)
                    counts[arm][key] = hits
                    totals[split][arm][key] += hits
                    per_query[split][arm][key].append(hits)
            output.write(canonical({
                "ordinal": feature["ordinal"],
                "source_id": feature["source_id"], "split": split,
                "candidate_hits": candidate_hits, "top_hits": counts,
            }))
    p05 = {split: {arm: {key: sorted(values)[(FIT * 5 + 99) // 100 - 1]
                          for key, values in cases.items()}
                   for arm, cases in arms.items()}
           for split, arms in per_query.items()}
    decision = decide(totals["holdout"], p05["holdout"])
    (args.output / "summary.json").write_text(canonical({
        "schema": SCHEMA + "-summary", "source_only": True,
        "physical_plan": False, "dataset": "ReLAION-1M D768",
        "split": "source-pseudoquery-hash-ranks-1409-1664",
        "query_count_per_split": FIT, "candidate_hits": candidate_totals,
        "top_hits": totals, "p05": p05, "decision": decision,
        "holdout_threshold": THRESHOLD,
        "prepare_seal_sha256": args.prepare_sha256,
        "source_labels_sha256": sha256(args.output / "source-labels.jsonl"),
    }))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("phase", choices=("prepare", "evaluate"))
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--old-layout", type=Path, required=True)
    parser.add_argument("--old-sq8", type=Path, required=True)
    parser.add_argument("--router", type=Path, required=True)
    parser.add_argument("--order", type=Path, required=True)
    parser.add_argument("--v164-terminal", type=Path, required=True)
    parser.add_argument("--prepare-sha256", default="")
    args = parser.parse_args()
    if args.phase == "prepare":
        prepare(args)
    else:
        if len(args.prepare_sha256) != 64:
            raise ValueError("V185 evaluate requires externally sealed prepare SHA")
        evaluate(args)


if __name__ == "__main__":
    main()
