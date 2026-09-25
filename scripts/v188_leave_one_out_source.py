#!/usr/bin/env python3
"""Sealed paired source pseudoquery nominee self-exclusion screen."""

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
from scripts.v166_surrogate_ranking_run import canonical, records
from scripts.v168_scored_neighbor_field import score_neighbor_field
from scripts.v177_source_candidate_ceiling import _inputs, expanded_units
from scripts.v182_wide_pq_rank import _normalized, _truth
from scripts.v165_unit_interval_resources import UNIT_ROWS

SCHEMA = "borsuk-v188-leave-one-out-source-v1"
FIRST = 2176
COUNT = 256
FIT = 128
WIDTH = 32
ALLOWANCES = (446, 672)
THRESHOLD = 12745
ARMS = ("self_included", "leave_one_out")


def paired_rosters(nominees: np.ndarray,
                   own_old_row: int) -> tuple[dict[str, tuple[int, ...]], int | None]:
    if (nominees.shape != (513,) or not np.issubdtype(nominees.dtype, np.integer)
            or np.any(nominees < 0) or len(np.unique(nominees)) != 513
            or type(own_old_row) is not int or own_old_row < 0):
        raise ValueError("V188 top-513 nominee geometry differs")
    positions = np.flatnonzero(nominees == own_old_row)
    own_rank = int(positions[0]) + 1 if positions.size else None
    included = tuple(map(int, nominees[:512]))
    excluded = tuple(map(int, nominees[nominees != own_old_row][:512]))
    if len(excluded) != 512:
        raise ValueError("V188 replacement nominee count differs")
    return {"self_included": included, "leave_one_out": excluded}, own_rank


def decide(holdout_totals: dict[str, dict[str, int]],
           holdout_p05: dict[str, dict[str, int]]) -> str:
    if (set(holdout_totals) != set(ARMS)
            or set(holdout_p05) != set(ARMS)
            or any(set(holdout_totals[arm]) != set(map(str, ALLOWANCES))
                   or set(holdout_p05[arm]) != set(map(str, ALLOWANCES))
                   for arm in ARMS)):
        raise ValueError("V188 paired decision inputs differ")
    loo = holdout_totals["leave_one_out"]
    tail = holdout_p05["leave_one_out"]
    if loo["446"] >= THRESHOLD and tail["446"] >= 98:
        return "advance-446-physical-gate"
    if loo["672"] >= THRESHOLD and tail["672"] >= 98:
        return "advance-elastic-only"
    return "revise-candidate-generator"


def _panel(source_ids: np.ndarray) -> tuple[int, ...]:
    return select_pseudoqueries(source_ids, FIRST + COUNT)[FIRST:]


def prepare(args: argparse.Namespace) -> None:
    if args.output.exists():
        raise ValueError("V188 output already exists")
    old, inverse_new, source_ids, vectors, _, planes = _inputs(args)
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
                page_rows=256, blocks_per_page=2, regions=1024, shortlist=513)
            _, _, historical = nominate_region_pq64(
                query, planes["summaries"], planes["books"], planes["codes"],
                page_rows=256, blocks_per_page=2, regions=1024, shortlist=512)
            nominees = np.asarray(nominated, dtype=np.int64)
            if not np.array_equal(nominees[:512], historical):
                raise ValueError("V188 historical top-512 prefix differs")
            rosters, own_rank = paired_rosters(nominees, int(inverse_old[source_row]))
            ranked = {}
            for arm, roster in rosters.items():
                base = sorted(set((inverse_new[old[np.asarray(roster)]] //
                                   UNIT_ROWS).tolist()))
                candidate = expanded_units(base, WIDTH, ROWS // UNIT_ROWS)
                field = score_neighbor_field(
                    query, nominees=roster, old_order=old,
                    inverse_old=inverse_old, new_order=new_order,
                    inverse_new=inverse_new, books=planes["books"],
                    codes=planes["codes"], unit_rows=UNIT_ROWS,
                    radius=WIDTH, metric="cosine")
                if field.units != candidate or field.scores.size != len(candidate) * UNIT_ROWS:
                    raise ValueError("V188 score field candidate geometry differs")
                scores = field.scores.reshape(len(candidate), UNIT_ROWS).copy()
                own = np.flatnonzero(field.old_rows == inverse_old[source_row])
                if own.size > 1:
                    raise ValueError("V188 pseudoquery row appears twice")
                if own.size == 1:
                    scores.reshape(-1)[own[0]] = np.inf
                minima = scores.min(axis=1)
                ranked[arm] = list(rank_units(dict(zip(
                    candidate, map(float, minima), strict=True))))
            output.write(canonical({
                "ordinal": ordinal, "source_id": stable_id,
                "source_row": int(source_row), "own_nominee_rank": own_rank,
                "ranked_units": ranked,
            }))
    (args.output / "prepare-seal.json").write_text(canonical({
        "schema": SCHEMA + "-prepare-seal", "source_truth_opened": False,
        "dataset": "ReLAION-1M D768",
        "split": "source-pseudoquery-hash-ranks-2177-2432",
        "fit": "2177-2304", "holdout": "2305-2432",
        "pseudo_ids": list(chosen),
        "features_sha256": sha256(args.output / "features.jsonl"),
        "candidate_width": WIDTH, "allowances": list(ALLOWANCES),
        "arms": list(ARMS), "threshold": THRESHOLD,
        "metric": "pq_reconstructed_cosine",
        "nominee_shortlist": 512, "replacement_shortlist": 513,
    }))


def evaluate(args: argparse.Namespace) -> None:
    if sha256(args.output / "prepare-seal.json") != args.prepare_sha256:
        raise ValueError("V188 externally sealed prepare SHA differs")
    seal = json.loads((args.output / "prepare-seal.json").read_text())
    features = records(args.output / "features.jsonl")
    if (seal.get("schema") != SCHEMA + "-prepare-seal"
            or seal.get("source_truth_opened") is not False
            or seal.get("features_sha256") != sha256(args.output / "features.jsonl")
            or seal.get("candidate_width") != WIDTH
            or seal.get("allowances") != list(ALLOWANCES)
            or seal.get("arms") != list(ARMS)
            or seal.get("threshold") != THRESHOLD
            or seal.get("metric") != "pq_reconstructed_cosine"
            or seal.get("nominee_shortlist") != 512
            or seal.get("replacement_shortlist") != 513
            or len(features) != COUNT
            or [row.get("ordinal") for row in features]
                != list(range(FIRST, FIRST + COUNT))
            or [row.get("source_id") for row in features] != seal.get("pseudo_ids")):
        raise ValueError("V188 sealed feature authority differs")
    _, inverse_new, source_ids, vectors, _, _ = _inputs(args)
    if list(_panel(source_ids)) != seal["pseudo_ids"]:
        raise ValueError("V188 source panel differs")
    source = _normalized(vectors)
    totals = {split: {arm: {str(k): 0 for k in ALLOWANCES}
                      for arm in ARMS} for split in ("fit", "holdout")}
    per_query = {split: {arm: {str(k): [] for k in ALLOWANCES}
                         for arm in ARMS} for split in ("fit", "holdout")}
    candidate_totals = {split: {arm: 0 for arm in ARMS}
                        for split in ("fit", "holdout")}
    candidate_sizes = {split: {arm: [] for arm in ARMS}
                       for split in ("fit", "holdout")}
    own_top512 = {"fit": 0, "holdout": 0}
    own_top513 = {"fit": 0, "holdout": 0}
    paired = {split: {str(k): {"wins": 0, "ties": 0, "losses": 0}
                      for k in ALLOWANCES}
              for split in ("fit", "holdout")}
    with (args.output / "source-labels.jsonl").open("x") as output:
        for index, feature in enumerate(features):
            source_row = feature["source_row"]
            if (type(source_row) is not int or not 0 <= source_row < ROWS
                    or int(source_ids[source_row]) != feature["source_id"]):
                raise ValueError("V188 source row identity differs")
            ranked = feature["ranked_units"]
            if set(ranked) != set(ARMS):
                raise ValueError("V188 rank arm set differs")
            candidates = {arm: set(row) for arm, row in ranked.items()}
            if any(not candidates[arm] or len(ranked[arm]) != len(candidates[arm])
                   or any(type(unit) is not int or not 0 <= unit < ROWS // UNIT_ROWS
                          for unit in ranked[arm]) for arm in ARMS):
                raise ValueError("V188 candidate geometry differs")
            own_rank = feature["own_nominee_rank"]
            if own_rank is not None and (type(own_rank) is not int
                                         or not 1 <= own_rank <= 513):
                raise ValueError("V188 own nominee rank differs")
            truth = _truth(source, source_ids, source_row)
            masses = Counter(map(int, inverse_new[truth] // UNIT_ROWS))
            split = "fit" if index < FIT else "holdout"
            own_top512[split] += int(own_rank is not None and own_rank <= 512)
            own_top513[split] += int(own_rank is not None)
            candidate_hits = {}
            counts = {}
            for arm in ARMS:
                candidate_hits[arm] = sum(masses.get(unit, 0)
                                          for unit in candidates[arm])
                candidate_totals[split][arm] += candidate_hits[arm]
                candidate_sizes[split][arm].append(len(candidates[arm]))
                counts[arm] = {}
                for allowance in ALLOWANCES:
                    hits = sum(masses.get(unit, 0)
                               for unit in ranked[arm][:allowance])
                    key = str(allowance)
                    counts[arm][key] = hits
                    totals[split][arm][key] += hits
                    per_query[split][arm][key].append(hits)
            for allowance in ALLOWANCES:
                key = str(allowance)
                delta = counts["leave_one_out"][key] - counts["self_included"][key]
                paired[split][key]["wins" if delta > 0 else
                                   "losses" if delta < 0 else "ties"] += 1
            output.write(canonical({
                "ordinal": feature["ordinal"],
                "source_id": feature["source_id"], "split": split,
                "own_nominee_rank": own_rank,
                "candidate_hits": candidate_hits, "top_hits": counts,
            }))
    p05 = {split: {arm: {key: sorted(values)[(FIT * 5 + 99) // 100 - 1]
                          for key, values in cases.items()}
                   for arm, cases in arms.items()}
           for split, arms in per_query.items()}
    decision = decide(totals["holdout"], p05["holdout"])
    size_stats = {split: {arm: {
        "mean": sum(values) / len(values), "max": max(values),
        "p95": sorted(values)[(FIT * 95 + 99) // 100 - 1],
    } for arm, values in arms.items()}
        for split, arms in candidate_sizes.items()}
    (args.output / "summary.json").write_text(canonical({
        "schema": SCHEMA + "-summary", "source_only": True,
        "physical_plan": False, "dataset": "ReLAION-1M D768",
        "split": "source-pseudoquery-hash-ranks-2177-2432",
        "query_count_per_split": FIT, "candidate_hits": candidate_totals,
        "candidate_units": size_stats,
        "own_in_top512": own_top512, "own_in_top513": own_top513,
        "paired": paired,
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
            raise ValueError("V188 evaluate requires externally sealed prepare SHA")
        evaluate(args)


if __name__ == "__main__":
    main()
