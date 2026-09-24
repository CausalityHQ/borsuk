#!/usr/bin/env python3
"""Sealed source-fit/holdout test of direct PQ physical-unit ranking."""

from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path

import numpy as np

from scripts.source_rank_utility import RankUtility, fit_rank_utility, rank_units
from scripts.v114_1m_paired import nominate_region_pq64
from scripts.v155_relaion_returned_quality import ROWS, sha256
from scripts.v166_surrogate_probe import select_pseudoqueries
from scripts.v166_surrogate_ranking_run import _score, canonical, records
from scripts.v168_scored_neighbor_field import score_neighbor_field
from scripts.v177_source_candidate_ceiling import _inputs, expanded_units
from scripts.v165_unit_interval_resources import UNIT_ROWS

SCHEMA = "borsuk-v180-pq-rank-signal-v1"
FIRST = 768
COUNT = 128
FIT = 64
WIDTH = 8
TOP_UNITS = 672
HOLDOUT_THRESHOLD = 6373


def _panel(source_ids: np.ndarray) -> tuple[int, ...]:
    return select_pseudoqueries(source_ids, FIRST + COUNT)[FIRST:]


def _truth(source: np.ndarray, source_ids: np.ndarray,
           source_row: int) -> np.ndarray:
    scores = source @ source[source_row]
    scores[source_row] = -np.inf
    top = np.argpartition(scores, ROWS - 100)[ROWS - 100:]
    tied = np.flatnonzero(scores >= scores[top].min())
    return tied[np.lexsort((source_ids[tied], -scores[tied]))[:100]]


def _normalized(vectors: np.ndarray) -> np.ndarray:
    source = np.empty((ROWS, vectors.shape[1]), dtype=np.float64)
    for start in range(0, ROWS, 8192):
        stop = min(start + 8192, ROWS)
        block = np.asarray(vectors[start:stop], dtype=np.float64)
        norms = np.linalg.norm(block, axis=1)
        if not np.isfinite(norms).all() or (norms <= 0).any():
            raise ValueError("V180 source norm differs")
        source[start:stop] = block / norms[:, None]
    return source


def _features(args: argparse.Namespace) -> tuple[dict, list[dict]]:
    seal_path = args.output / "prepare-seal.json"
    if sha256(seal_path) != args.prepare_sha256:
        raise ValueError("V180 prepare seal differs")
    seal = json.loads(seal_path.read_text())
    features = records(args.output / "features.jsonl")
    if (seal.get("schema") != SCHEMA + "-prepare-seal"
            or seal.get("source_truth_opened") is not False
            or seal.get("features_sha256") != sha256(args.output / "features.jsonl")
            or len(features) != COUNT
            or [row.get("ordinal") for row in features]
                != list(range(FIRST, FIRST + COUNT))
            or [row.get("source_id") for row in features] != seal.get("pseudo_ids")):
        raise ValueError("V180 sealed features differ")
    for row in features:
        ranked = row.get("ranked_units")
        mandatory = row.get("mandatory_units")
        if (not isinstance(ranked, list) or not ranked
                or len(set(ranked)) != len(ranked)
                or any(type(unit) is not int or unit < 0 or unit >= ROWS // UNIT_ROWS
                       for unit in ranked)
                or not isinstance(mandatory, list)
                or mandatory != sorted(set(mandatory))
                or not set(mandatory).issubset(ranked)):
            raise ValueError("V180 feature unit geometry differs")
    return seal, features


def prepare(args: argparse.Namespace) -> None:
    if args.output.exists():
        raise ValueError("V180 output already exists")
    old, inverse_new, source_ids, vectors, sq8, planes = _inputs(args)
    chosen = _panel(source_ids)
    id_to_source = {int(identifier): row for row, identifier in enumerate(source_ids)}
    inverse_old = np.empty(ROWS, dtype=np.int64)
    inverse_old[old] = np.arange(ROWS, dtype=np.int64)
    new_order = np.argsort(inverse_new).astype(np.int64)
    if not np.array_equal(inverse_new[new_order], np.arange(ROWS)):
        raise ValueError("V180 physical order differs")
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
                raise ValueError("V180 nominee roster differs")
            eligible = nominees[nominees != inverse_old[source_row]]
            _, sq8_scores = _score(sq8, query, eligible,
                                   planes["low"], planes["step"], 1)
            primary_rank = np.lexsort((sq8["id"][eligible], sq8_scores))[:100]
            primary = eligible[primary_rank]
            base = sorted(set((inverse_new[old[nominees]] // UNIT_ROWS).tolist()))
            mandatory = sorted(set((inverse_new[old[primary]] // UNIT_ROWS).tolist()))
            candidate = expanded_units(base, WIDTH, ROWS // UNIT_ROWS)
            field = score_neighbor_field(
                query, nominees=tuple(map(int, nominees)), old_order=old,
                inverse_old=inverse_old, new_order=new_order,
                inverse_new=inverse_new, books=planes["books"],
                codes=planes["codes"], unit_rows=UNIT_ROWS, radius=WIDTH)
            if field.units != candidate or field.scores.size != len(candidate) * UNIT_ROWS:
                raise ValueError("V180 score field candidate geometry differs")
            scores = field.scores.reshape(len(candidate), UNIT_ROWS).copy()
            own = np.flatnonzero(field.old_rows == inverse_old[source_row])
            if own.size > 1:
                raise ValueError("V180 pseudoquery row appears twice in score field")
            if own.size == 1:
                scores.reshape(-1)[own[0]] = np.inf
            unit_minimum = scores.min(axis=1)
            ranked = rank_units(dict(zip(candidate, map(float, unit_minimum),
                                         strict=True)))
            output.write(canonical({
                "ordinal": ordinal, "source_id": stable_id,
                "source_row": int(source_row), "mandatory_units": mandatory,
                "ranked_units": list(ranked),
            }))
    (args.output / "prepare-seal.json").write_text(canonical({
        "schema": SCHEMA + "-prepare-seal", "dataset": "ReLAION-1M D768",
        "split": "source-pseudoquery-hash-ranks-769-896",
        "fit": "769-832", "holdout": "833-896",
        "source_truth_opened": False, "pseudo_ids": list(chosen),
        "features_sha256": sha256(args.output / "features.jsonl"),
        "candidate_width": WIDTH, "top_units": TOP_UNITS,
        "holdout_threshold": HOLDOUT_THRESHOLD,
    }))


def _labels(features: list[dict], source: np.ndarray,
            source_ids: np.ndarray, inverse_new: np.ndarray) -> list[dict]:
    labels = []
    for row in features:
        source_row = row["source_row"]
        if (type(source_row) is not int or not 0 <= source_row < ROWS
                or int(source_ids[source_row]) != row["source_id"]):
            raise ValueError("V180 source identity differs")
        truth = _truth(source, source_ids, source_row)
        masses = Counter(map(int, inverse_new[truth] // UNIT_ROWS))
        ranked = row["ranked_units"]
        candidate_hits = sum(masses.get(unit, 0) for unit in ranked)
        top_hits = sum(masses.get(unit, 0) for unit in ranked[:TOP_UNITS])
        labels.append({"ordinal": row["ordinal"], "source_id": row["source_id"],
                       "candidate_hits": candidate_hits, "top_hits": top_hits,
                       "candidate_truth_by_unit": [[unit, masses[unit]]
                                                   for unit in ranked if unit in masses]})
    return labels


def fit(args: argparse.Namespace) -> None:
    _, features = _features(args)
    if (args.output / "fit-seal.json").exists():
        raise ValueError("V180 fit output already exists")
    _, inverse_new, source_ids, vectors, _, _ = _inputs(args)
    if list(_panel(source_ids)) != [row["source_id"] for row in features]:
        raise ValueError("V180 fit source panel differs")
    labels = _labels(features[:FIT], _normalized(vectors), source_ids, inverse_new)
    with (args.output / "fit-labels.jsonl").open("x") as output:
        for row in labels:
            output.write(canonical(row))
    examples = []
    for feature, label in zip(features[:FIT], labels, strict=True):
        scores = {unit: float(rank)
                  for rank, unit in enumerate(feature["ranked_units"])}
        truth = dict(label["candidate_truth_by_unit"])
        examples.append((scores, truth))
    curve = fit_rank_utility(examples)
    (args.output / "fit-seal.json").write_text(canonical({
        "schema": SCHEMA + "-fit-seal", "prepare_seal_sha256": args.prepare_sha256,
        "fit_labels_sha256": sha256(args.output / "fit-labels.jsonl"),
        "holdout_truth_opened": False,
        "curve_expected_hits": list(curve.expected_hits),
        "curve_samples": list(curve.samples),
        "fit_candidate_hits": sum(row["candidate_hits"] for row in labels),
        "fit_top_hits": sum(row["top_hits"] for row in labels),
    }))


def evaluate(args: argparse.Namespace) -> None:
    _, features = _features(args)
    if sha256(args.output / "fit-seal.json") != args.fit_sha256:
        raise ValueError("V180 fit seal differs")
    seal = json.loads((args.output / "fit-seal.json").read_text())
    if (seal.get("schema") != SCHEMA + "-fit-seal"
            or seal.get("prepare_seal_sha256") != args.prepare_sha256
            or seal.get("fit_labels_sha256") != sha256(args.output / "fit-labels.jsonl")
            or seal.get("holdout_truth_opened") is not False):
        raise ValueError("V180 fit authority differs")
    curve = RankUtility(tuple(seal["curve_expected_hits"]),
                        tuple(seal["curve_samples"]))
    _, inverse_new, source_ids, vectors, _, _ = _inputs(args)
    labels = _labels(features[FIT:], _normalized(vectors), source_ids, inverse_new)
    with (args.output / "holdout-labels.jsonl").open("x") as output:
        for row in labels:
            output.write(canonical(row))
    observed = [row["top_hits"] for row in labels]
    predicted = [sum(curve.expected_hits[:min(TOP_UNITS,
                                            len(feature["ranked_units"]))])
                 for feature in features[FIT:]]
    candidate_total = sum(row["candidate_hits"] for row in labels)
    top_total = sum(observed)
    decision = ("advance-to-physical-planner" if
                top_total >= HOLDOUT_THRESHOLD and sorted(observed)[3] >= 98
                else "kill-rank-only-pq-utility")
    (args.output / "summary.json").write_text(canonical({
        "schema": SCHEMA + "-summary", "dataset": "ReLAION-1M D768",
        "split": "source-pseudoquery-hash-ranks-769-896",
        "metric": "exact-source-truth-contained-in-PQ-top-672-units",
        "source_only": True, "physical_plan": False,
        "fit_candidate_hits": seal["fit_candidate_hits"],
        "fit_top_hits": seal["fit_top_hits"],
        "holdout_candidate_hits": candidate_total,
        "holdout_top_hits": top_total,
        "holdout_p05": sorted(observed)[3],
        "holdout_prediction_sum": sum(predicted),
        "holdout_prediction_mae_per_query": sum(
            abs(actual - estimate) for actual, estimate in zip(observed, predicted,
                                                                 strict=True)) / FIT,
        "decision": decision, "holdout_threshold": HOLDOUT_THRESHOLD,
        "prepare_seal_sha256": args.prepare_sha256,
        "fit_seal_sha256": args.fit_sha256,
        "holdout_labels_sha256": sha256(args.output / "holdout-labels.jsonl"),
    }))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("phase", choices=("prepare", "fit", "evaluate"))
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--old-layout", type=Path, required=True)
    parser.add_argument("--old-sq8", type=Path, required=True)
    parser.add_argument("--router", type=Path, required=True)
    parser.add_argument("--order", type=Path, required=True)
    parser.add_argument("--v164-terminal", type=Path, required=True)
    parser.add_argument("--prepare-sha256", default="")
    parser.add_argument("--fit-sha256", default="")
    args = parser.parse_args()
    if args.phase == "prepare":
        prepare(args)
    elif args.phase == "fit":
        if len(args.prepare_sha256) != 64:
            raise ValueError("V180 fit requires externally sealed prepare SHA")
        fit(args)
    else:
        if len(args.prepare_sha256) != 64 or len(args.fit_sha256) != 64:
            raise ValueError("V180 evaluate requires externally sealed SHA values")
        evaluate(args)


if __name__ == "__main__":
    main()
