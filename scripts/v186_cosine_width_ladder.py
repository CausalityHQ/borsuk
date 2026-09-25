#!/usr/bin/env python3
"""Sealed cosine-PQ candidate width ladder on source pseudoqueries."""

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

SCHEMA = "borsuk-v186-cosine-width-ladder-v1"
FIRST = 1664
COUNT = 256
FIT = 128
WIDTHS = (32, 64, 128)
ALLOWANCES = (446, 672, 1344)
THRESHOLD = 12745


def decide(holdout_totals: dict[str, dict[str, int]],
           holdout_p05: dict[str, dict[str, int]]) -> dict[str, int | str]:
    if (set(holdout_totals) != set(map(str, WIDTHS))
            or set(holdout_p05) != set(map(str, WIDTHS))
            or any(set(holdout_totals[str(width)]) != set(map(str, ALLOWANCES))
                   or set(holdout_p05[str(width)]) != set(map(str, ALLOWANCES))
                   for width in WIDTHS)):
        raise ValueError("V186 width-ladder decision inputs differ")
    for allowance in (446, 672):
        for width in WIDTHS:
            key, point = str(width), str(allowance)
            if (holdout_totals[key][point] >= THRESHOLD
                    and holdout_p05[key][point] >= 98):
                return {"width": width, "allowance": allowance}
    return {"decision": "revise-candidate-generator"}


def project_rank(all_units: tuple[int, ...], minima: tuple[float, ...] | np.ndarray,
                 selected: tuple[int, ...]) -> list[int]:
    if (len(all_units) != len(minima) or len(set(all_units)) != len(all_units)
            or len(set(selected)) != len(selected)
            or not set(selected).issubset(all_units)):
        raise ValueError("V186 nested score projection differs")
    by_unit = dict(zip(all_units, map(float, minima), strict=True))
    return list(rank_units({unit: by_unit[unit] for unit in selected}))


def _panel(source_ids: np.ndarray) -> tuple[int, ...]:
    return select_pseudoqueries(source_ids, FIRST + COUNT)[FIRST:]


def prepare(args: argparse.Namespace) -> None:
    if args.output.exists():
        raise ValueError("V186 output already exists")
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
                raise ValueError("V186 nominee roster differs")
            eligible = nominees[nominees != inverse_old[source_row]]
            _, sq8_scores = _score(sq8, query, eligible,
                                   planes["low"], planes["step"], 1)
            primary_rank = np.lexsort((sq8["id"][eligible], sq8_scores))[:100]
            primary = eligible[primary_rank]
            base = sorted(set((inverse_new[old[nominees]] // UNIT_ROWS).tolist()))
            mandatory = sorted(set((inverse_new[old[primary]] // UNIT_ROWS).tolist()))
            candidates = {str(width): expanded_units(base, width, ROWS // UNIT_ROWS)
                          for width in WIDTHS}
            field = score_neighbor_field(
                query, nominees=tuple(map(int, nominees)), old_order=old,
                inverse_old=inverse_old, new_order=new_order,
                inverse_new=inverse_new, books=planes["books"],
                codes=planes["codes"], unit_rows=UNIT_ROWS,
                radius=WIDTHS[-1], metric="cosine")
            widest = candidates[str(WIDTHS[-1])]
            if field.units != widest or field.scores.size != len(widest) * UNIT_ROWS:
                raise ValueError("V186 score field candidate geometry differs")
            scores = field.scores.reshape(len(widest), UNIT_ROWS).copy()
            own = np.flatnonzero(field.old_rows == inverse_old[source_row])
            if own.size > 1:
                raise ValueError("V186 pseudoquery row appears twice")
            if own.size == 1:
                scores.reshape(-1)[own[0]] = np.inf
            minima = scores.min(axis=1)
            ranked = {str(width): project_rank(widest, minima, candidates[str(width)])
                      for width in WIDTHS}
            output.write(canonical({
                "ordinal": ordinal, "source_id": stable_id,
                "source_row": int(source_row),
                "mandatory_units": mandatory,
                "ranked_units": ranked,
            }))
    (args.output / "prepare-seal.json").write_text(canonical({
        "schema": SCHEMA + "-prepare-seal", "source_truth_opened": False,
        "dataset": "ReLAION-1M D768",
        "split": "source-pseudoquery-hash-ranks-1665-1920",
        "fit": "1665-1792", "holdout": "1793-1920",
        "pseudo_ids": list(chosen),
        "features_sha256": sha256(args.output / "features.jsonl"),
        "candidate_widths": list(WIDTHS), "allowances": list(ALLOWANCES),
        "metric": "cosine", "threshold": THRESHOLD,
    }))


def evaluate(args: argparse.Namespace) -> None:
    if sha256(args.output / "prepare-seal.json") != args.prepare_sha256:
        raise ValueError("V186 externally sealed prepare SHA differs")
    seal = json.loads((args.output / "prepare-seal.json").read_text())
    features = records(args.output / "features.jsonl")
    if (seal.get("schema") != SCHEMA + "-prepare-seal"
            or seal.get("source_truth_opened") is not False
            or seal.get("features_sha256") != sha256(args.output / "features.jsonl")
            or seal.get("candidate_widths") != list(WIDTHS)
            or seal.get("allowances") != list(ALLOWANCES)
            or seal.get("metric") != "cosine"
            or seal.get("threshold") != THRESHOLD
            or len(features) != COUNT
            or [row.get("ordinal") for row in features]
                != list(range(FIRST, FIRST + COUNT))
            or [row.get("source_id") for row in features] != seal.get("pseudo_ids")):
        raise ValueError("V186 sealed feature authority differs")
    _, inverse_new, source_ids, vectors, _, _ = _inputs(args)
    if list(_panel(source_ids)) != seal["pseudo_ids"]:
        raise ValueError("V186 source panel differs")
    source = _normalized(vectors)
    totals = {split: {str(width): {str(k): 0 for k in ALLOWANCES}
                      for width in WIDTHS} for split in ("fit", "holdout")}
    per_query = {split: {str(width): {str(k): [] for k in ALLOWANCES}
                         for width in WIDTHS} for split in ("fit", "holdout")}
    candidate_totals = {split: {str(width): 0 for width in WIDTHS}
                        for split in ("fit", "holdout")}
    candidate_sizes = {split: {str(width): [] for width in WIDTHS}
                       for split in ("fit", "holdout")}
    with (args.output / "source-labels.jsonl").open("x") as output:
        for index, feature in enumerate(features):
            source_row = feature["source_row"]
            if (type(source_row) is not int or not 0 <= source_row < ROWS
                    or int(source_ids[source_row]) != feature["source_id"]):
                raise ValueError("V186 source row identity differs")
            ranked = feature["ranked_units"]
            if set(ranked) != set(map(str, WIDTHS)):
                raise ValueError("V186 width set differs")
            candidates = {key: set(row) for key, row in ranked.items()}
            if (any(not candidates[key] or len(ranked[key]) != len(candidates[key])
                    or not set(feature["mandatory_units"]).issubset(candidates[key])
                    for key in candidates)
                    or any(not candidates[str(left)].issubset(candidates[str(right)])
                           for left, right in zip(WIDTHS, WIDTHS[1:]))):
                raise ValueError("V186 candidate/primary geometry differs")
            truth = _truth(source, source_ids, source_row)
            masses = Counter(map(int, inverse_new[truth] // UNIT_ROWS))
            split = "fit" if index < FIT else "holdout"
            candidate_hits = {}
            counts = {}
            for width in WIDTHS:
                key = str(width)
                candidate_hits[key] = sum(masses.get(unit, 0)
                                          for unit in candidates[key])
                candidate_totals[split][key] += candidate_hits[key]
                candidate_sizes[split][key].append(len(candidates[key]))
                counts[key] = {}
                for allowance in ALLOWANCES:
                    hits = sum(masses.get(unit, 0)
                               for unit in ranked[key][:allowance])
                    point = str(allowance)
                    counts[key][point] = hits
                    totals[split][key][point] += hits
                    per_query[split][key][point].append(hits)
            output.write(canonical({
                "ordinal": feature["ordinal"],
                "source_id": feature["source_id"], "split": split,
                "candidate_hits": candidate_hits, "top_hits": counts,
            }))
    p05 = {split: {width: {key: sorted(values)[(FIT * 5 + 99) // 100 - 1]
                          for key, values in cases.items()}
                   for width, cases in widths.items()}
           for split, widths in per_query.items()}
    decision = decide(totals["holdout"], p05["holdout"])
    size_stats = {split: {key: {
        "mean": sum(values) / len(values), "max": max(values),
        "p95": sorted(values)[(FIT * 95 + 99) // 100 - 1],
    } for key, values in widths.items()}
        for split, widths in candidate_sizes.items()}
    (args.output / "summary.json").write_text(canonical({
        "schema": SCHEMA + "-summary", "source_only": True,
        "physical_plan": False, "dataset": "ReLAION-1M D768",
        "split": "source-pseudoquery-hash-ranks-1665-1920",
        "query_count_per_split": FIT, "candidate_hits": candidate_totals,
        "candidate_units": size_stats, "top_hits": totals,
        "p05": p05, "decision": decision,
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
            raise ValueError("V186 evaluate requires externally sealed prepare SHA")
        evaluate(args)


if __name__ == "__main__":
    main()
