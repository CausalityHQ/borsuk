#!/usr/bin/env python3
"""Frozen V168 direct-PQ adjacent-unit ranking screen."""

from __future__ import annotations

import argparse
import json
import math
import tempfile
from pathlib import Path

import numpy as np

from scripts.v114_1m_paired import nominate_region_pq64
from scripts.v155_relaion_returned_quality import (
    DIMS, LAYOUT_SHA, ROWS, SOURCE_SHA, SQ8_SHA, sha256,
)
from scripts.v164_smooth_layout_1m import DTYPE, source_arrays
from scripts.v165_unit_interval_resources import (
    UNIT_ROWS, V164_ORDER_SHA, V164_TERMINAL_SHA, load_orders,
)
from scripts.v166_surrogate_probe import candidate_units, select_pseudoqueries
from scripts.v166_surrogate_ranking_run import (
    ROUTER_MANIFEST_SHA, _score, canonical, load_frozen_v115_router, records,
)
from scripts.v168_scored_neighbor_field import score_neighbor_field

SCHEMA = "borsuk-v168-pq-neighbor-ranking-v1"
BUDGETS = (16, 32, 64)
PANEL_ROWS = 128
FIRST_PSEUDO_ORDINAL = 512
BOOTSTRAP_REPS = 10_000
BOOTSTRAP_SEED = 168


def rank_adjacent_units(features: list[list], kind: str, budget: int) -> list[int]:
    """Return physical unit ordinals ordered by one sealed feature."""
    if kind not in ("direct", "neighbor") or budget <= 0:
        raise ValueError("V168 feature or unit budget differs")
    score_column = 1 if kind == "direct" else 2
    units: set[int] = set()
    for row in features:
        if (len(row) != 3 or type(row[0]) is not int or row[0] < 0
                or row[0] in units or not isinstance(row[score_column], (int, float))
                or math.isnan(float(row[score_column]))):
            raise ValueError("V168 adjacent feature geometry differs")
        units.add(row[0])
    return [row[0] for row in sorted(
        features, key=lambda row: (row[score_column], row[0]))[:budget]]


def paired_bootstrap_interval(differences: list[int]) -> tuple[int, int]:
    """Nearest-rank 95% interval for the paired 128-query total gain."""
    if len(differences) != PANEL_ROWS or any(type(value) is not int
                                             for value in differences):
        raise ValueError("V168 paired panel differs")
    rng = np.random.Generator(np.random.PCG64(BOOTSTRAP_SEED))
    samples = rng.integers(0, PANEL_ROWS,
                           size=(BOOTSTRAP_REPS, PANEL_ROWS))
    totals = np.sort(np.asarray(differences, dtype=np.int64)[samples].sum(axis=1))
    return int(totals[249]), int(totals[9749])


def screen_decision(rows: list[dict]) -> dict:
    """Apply preregistered representation decision to sealed paired captures."""
    if len(rows) != PANEL_ROWS:
        raise ValueError("V168 screen requires 128 paired rows")
    totals = {kind: {budget: 0 for budget in BUDGETS}
              for kind in ("direct", "neighbor")}
    differences = []
    positive_rows = 0
    wins = ties = losses = 0
    for row in rows:
        positive = row.get("positive_rows")
        if type(positive) is not int or positive < 0:
            raise ValueError("V168 positive-row count differs")
        positive_rows += positive
        for kind in totals:
            counts = row.get(kind)
            if (not isinstance(counts, dict) or set(counts) != set(BUDGETS)
                    or any(type(counts[budget]) is not int
                           or not 0 <= counts[budget] <= positive
                           for budget in BUDGETS)
                    or not counts[16] <= counts[32] <= counts[64]):
                raise ValueError("V168 capture budget geometry differs")
            for budget in BUDGETS:
                totals[kind][budget] += counts[budget]
        difference = row["direct"][32] - row["neighbor"][32]
        differences.append(difference)
        wins += difference > 0
        ties += difference == 0
        losses += difference < 0
    gain = sum(differences)
    interval = paired_bootstrap_interval(differences)
    passed = (positive_rows > 0 and gain >= 8 and interval[0] > 0
              and all(totals["direct"][budget] >= totals["neighbor"][budget]
                      for budget in (16, 64)))
    return {"decision": ("advance-to-planner" if passed else
                         "uninformative" if positive_rows == 0 else "killed"),
            "positive_rows": positive_rows, "captures": totals,
            "gain_32": gain, "bootstrap_total_95": list(interval),
            "wins": wins, "ties": ties, "losses": losses}


def _prepared_query(query: np.ndarray, stable_id: int, ordinal: int,
                    source_row: int, old: np.ndarray, old_inverse: np.ndarray,
                    order: np.ndarray, inverse: np.ndarray, sq8: np.ndarray,
                    planes: dict) -> tuple[dict, dict]:
    _, _, nominated = nominate_region_pq64(
        query, planes["summaries"], planes["books"], planes["codes"],
        page_rows=256, blocks_per_page=2, regions=1024, shortlist=512)
    nominees = np.asarray(nominated, dtype=np.int64)
    if nominees.size != 512 or np.unique(nominees).size != 512:
        raise ValueError("V168 PQ nominee roster differs")
    own_old = int(old_inverse[source_row])
    eligible_nominees = nominees[nominees != own_old]
    if eligible_nominees.size < 100:
        raise ValueError("V168 SQ8 primary roster too small")
    _, nominee_scores = _score(
        sq8, query, eligible_nominees, planes["low"], planes["step"], 1)
    nominee_rank = np.lexsort((sq8["id"][eligible_nominees], nominee_scores))
    primary = eligible_nominees[nominee_rank[:100]].astype(int).tolist()
    threshold = float(nominee_scores[nominee_rank[99]])
    field = score_neighbor_field(
        query, nominees=tuple(int(row) for row in nominees),
        old_order=old, inverse_old=old_inverse, new_order=order,
        inverse_new=inverse, books=planes["books"],
        codes=planes["codes"], unit_rows=UNIT_ROWS)
    voted_rank: dict[int, int] = {}
    for rank, old_row in enumerate(nominees, start=1):
        unit = int(inverse[old[old_row]]) // UNIT_ROWS
        voted_rank[unit] = min(rank, voted_rank.get(unit, rank))
    _, sq8_scores = _score(
        sq8, query, field.old_rows, planes["low"], planes["step"], 1)
    eligible = (~np.isin(field.old_rows, nominees)) & (field.old_rows != own_old)
    adjacent_features = []
    actual_by_unit = []
    cursor = 0
    for unit in field.units:
        stop = cursor + min(UNIT_ROWS, ROWS - unit * UNIT_ROWS)
        if unit not in voted_rank:
            remaining = field.old_rows[cursor:stop] != own_old
            direct = (float(np.min(field.scores[cursor:stop][remaining]))
                      if np.any(remaining) else math.inf)
            neighbor = min(voted_rank.get(unit - 1, 513),
                           voted_rank.get(unit + 1, 513))
            if neighbor > 512 or not math.isfinite(direct):
                raise ValueError("V168 adjacent-only unit feature differs")
            adjacent_features.append([int(unit), direct, int(neighbor)])
            actual_by_unit.append([int(unit), int(np.count_nonzero(
                eligible[cursor:stop] & (sq8_scores[cursor:stop] <= threshold)))])
        cursor = stop
    if cursor != len(field.old_rows) or not adjacent_features:
        raise ValueError("V168 candidate row slices differ")
    roster = {"query_ordinal": ordinal, "source_id": stable_id,
              "nominees": nominees.astype(int).tolist(), "primary": primary,
              "adjacent_features": adjacent_features,
              "candidate_units": len(field.units),
              "candidate_rows": len(field.old_rows)}
    label = {"query_ordinal": ordinal, "source_id": stable_id,
             "threshold": threshold, "actual_by_unit": actual_by_unit,
             "positive_rows": sum(count for _, count in actual_by_unit)}
    return roster, label


def prepare(args: argparse.Namespace) -> None:
    if args.output.exists():
        raise ValueError("V168 output directory already exists")
    if sha256(args.source) != SOURCE_SHA or sha256(args.old_sq8) != SQ8_SHA:
        raise ValueError("V168 source or SQ8 identity differs")
    router_manifest, planes = load_frozen_v115_router(
        args.router, ROUTER_MANIFEST_SHA, ROWS, DIMS)
    if (router_manifest["source_sha256"] != SOURCE_SHA
            or router_manifest["layout_sha256"] != LAYOUT_SHA
            or router_manifest["sq8_sha256"] != SQ8_SHA):
        raise ValueError("V168 router authority differs")
    old, inverse = load_orders(args.old_layout, args.order, args.v164_terminal)
    order = np.load(args.order, allow_pickle=False)
    old_inverse = np.empty(ROWS, dtype=np.int64)
    old_inverse[old] = np.arange(ROWS, dtype=np.int64)
    source_ids, vectors, _ = source_arrays(args.source)
    sq8 = np.memmap(args.old_sq8, dtype=DTYPE, mode="r", shape=(ROWS,))
    if not np.array_equal(sq8["id"], source_ids[old]):
        raise ValueError("V168 SQ8/source mapping differs")
    selected = select_pseudoqueries(source_ids,
                                    FIRST_PSEUDO_ORDINAL + PANEL_ROWS)
    id_to_source = {int(identifier): row for row, identifier in enumerate(source_ids)}
    args.output.mkdir(parents=True)
    with ((args.output / "features.jsonl").open("x") as features,
          (args.output / "proxy-labels.jsonl").open("x") as labels):
        for ordinal, stable_id in enumerate(selected[FIRST_PSEUDO_ORDINAL:],
                                            start=FIRST_PSEUDO_ORDINAL):
            source_row = id_to_source[stable_id]
            query = np.asarray(vectors[source_row], dtype=np.float32)
            roster, label = _prepared_query(
                query, stable_id, ordinal, source_row, old, old_inverse,
                order, inverse, sq8, planes)
            features.write(canonical(roster))
            labels.write(canonical(label))
    (args.output / "prepare-seal.json").write_text(canonical({
        "schema": SCHEMA + "-prepare-seal", "dataset": "ReLAION-1M",
        "split": "source-pseudoquery-513-640", "proxy_labels_opened": False,
        "source_sha256": SOURCE_SHA, "old_sq8_sha256": SQ8_SHA,
        "router_manifest_sha256": ROUTER_MANIFEST_SHA,
        "old_layout_sha256": LAYOUT_SHA, "order_sha256": V164_ORDER_SHA,
        "v164_terminal_sha256": V164_TERMINAL_SHA,
        "pseudo_ids": list(selected[FIRST_PSEUDO_ORDINAL:]),
        "features_sha256": sha256(args.output / "features.jsonl"),
        "proxy_labels_sha256": sha256(args.output / "proxy-labels.jsonl"),
    }))


def _verify_prepare(output: Path, *, open_labels: bool) -> list[dict]:
    seal = json.loads((output / "prepare-seal.json").read_text())
    features = records(output / "features.jsonl")
    if (len(features) != PANEL_ROWS
            or [row.get("query_ordinal") for row in features]
                != list(range(FIRST_PSEUDO_ORDINAL,
                              FIRST_PSEUDO_ORDINAL + PANEL_ROWS))
            or len({row.get("source_id") for row in features}) != PANEL_ROWS
            or seal.get("schema") != SCHEMA + "-prepare-seal"
            or seal.get("dataset") != "ReLAION-1M"
            or seal.get("split") != "source-pseudoquery-513-640"
            or seal.get("proxy_labels_opened") is not False
            or seal.get("source_sha256") != SOURCE_SHA
            or seal.get("old_sq8_sha256") != SQ8_SHA
            or seal.get("router_manifest_sha256") != ROUTER_MANIFEST_SHA
            or seal.get("old_layout_sha256") != LAYOUT_SHA
            or seal.get("order_sha256") != V164_ORDER_SHA
            or seal.get("v164_terminal_sha256") != V164_TERMINAL_SHA
            or seal.get("pseudo_ids") != [row["source_id"] for row in features]
            or seal.get("features_sha256") != sha256(output / "features.jsonl")):
        raise ValueError("V168 prepare feature seal differs")
    if open_labels and seal.get("proxy_labels_sha256") != sha256(
            output / "proxy-labels.jsonl"):
        raise ValueError("V168 proxy label seal differs")
    return features


def _planned_row(feature: dict) -> dict:
    adjacent = feature["adjacent_features"]
    if (len(feature["nominees"]) != 512
            or len(set(feature["nominees"])) != 512
            or len(feature["primary"]) != 100
            or not set(feature["primary"]).issubset(feature["nominees"])
            or not 0 < len(adjacent) <= feature["candidate_units"] <= 1536
            or not 0 < feature["candidate_rows"] <= 49_152
            or len({row[0] for row in adjacent}) != len(adjacent)
            or any(type(row[0]) is not int or not 0 <= row[0] < ROWS // UNIT_ROWS
                   or not math.isfinite(row[1]) or not 1 <= row[2] <= 512
                   for row in adjacent)):
        raise ValueError("V168 feature geometry differs")
    return {"query_ordinal": feature["query_ordinal"],
            "source_id": feature["source_id"],
            "selected": {kind: {str(budget): rank_adjacent_units(
                adjacent, kind, budget) for budget in BUDGETS}
                for kind in ("direct", "neighbor")}}


def plan(output: Path) -> None:
    features = _verify_prepare(output, open_labels=False)
    with (output / "plans.jsonl").open("x") as dest:
        for feature in features:
            dest.write(canonical(_planned_row(feature)))
    (output / "plan-seal.json").write_text(canonical({
        "schema": SCHEMA + "-plan-seal", "proxy_labels_opened": False,
        "prepare_seal_sha256": sha256(output / "prepare-seal.json"),
        "features_sha256": sha256(output / "features.jsonl"),
        "plans_sha256": sha256(output / "plans.jsonl"),
        "budgets": list(BUDGETS), "queries": PANEL_ROWS,
    }))


def _verify_plans(output: Path, features: list[dict]) -> list[dict]:
    seal = json.loads((output / "plan-seal.json").read_text())
    plans = records(output / "plans.jsonl")
    if (len(plans) != PANEL_ROWS
            or seal.get("schema") != SCHEMA + "-plan-seal"
            or seal.get("proxy_labels_opened") is not False
            or seal.get("prepare_seal_sha256")
                != sha256(output / "prepare-seal.json")
            or seal.get("features_sha256") != sha256(output / "features.jsonl")
            or seal.get("plans_sha256") != sha256(output / "plans.jsonl")
            or seal.get("budgets") != list(BUDGETS)
            or seal.get("queries") != PANEL_ROWS
            or any(planned != _planned_row(feature)
                   for feature, planned in zip(features, plans))):
        raise ValueError("V168 GT-blind plan seal differs")
    return plans


def _summary(output: Path, features: list[dict], plans: list[dict]) -> dict:
    labels = records(output / "proxy-labels.jsonl")
    if len(labels) != PANEL_ROWS:
        raise ValueError("V168 proxy label count differs")
    rows = []
    candidate_units = candidate_rows = positive_units = 0
    for feature, planned, label in zip(features, plans, labels):
        if (label.get("query_ordinal") != feature["query_ordinal"]
                or label.get("source_id") != feature["source_id"]
                or planned["query_ordinal"] != feature["query_ordinal"]
                or planned["source_id"] != feature["source_id"]):
            raise ValueError("V168 proxy/feature/plan identity differs")
        counts = label["actual_by_unit"]
        if (len(counts) != len(feature["adjacent_features"])
                or [unit for unit, _ in counts]
                    != [row[0] for row in feature["adjacent_features"]]
                or any(type(count) is not int or not 0 <= count <= UNIT_ROWS
                       for _, count in counts)
                or label["positive_rows"] != sum(count for _, count in counts)):
            raise ValueError("V168 proxy label geometry differs")
        by_unit = dict(counts)
        paired = {kind: {budget: sum(by_unit[unit] for unit in
                                  planned["selected"][kind][str(budget)])
                         for budget in BUDGETS}
                  for kind in ("direct", "neighbor")}
        paired["positive_rows"] = label["positive_rows"]
        rows.append(paired)
        candidate_units += feature["candidate_units"]
        candidate_rows += feature["candidate_rows"]
        positive_units += sum(count > 0 for _, count in counts)
    decision = screen_decision(rows)
    return {"schema": SCHEMA + "-summary", "dataset": "ReLAION-1M",
            "split": "source-pseudoquery-513-640",
            "proxy_not_validation_gt": True, "queries": PANEL_ROWS,
            "candidate_units": candidate_units, "candidate_rows": candidate_rows,
            "positive_units": positive_units, **decision,
            "plan_seal_sha256": sha256(output / "plan-seal.json"),
            "proxy_labels_sha256": sha256(output / "proxy-labels.jsonl")}


def evaluate(output: Path) -> None:
    features = _verify_prepare(output, open_labels=True)
    plans = _verify_plans(output, features)
    (output / "summary.json").write_text(canonical(_summary(output, features, plans)))


def check(args: argparse.Namespace) -> dict:
    features = _verify_prepare(args.output, open_labels=True)
    plans = _verify_plans(args.output, features)
    expected = _summary(args.output, features, plans)
    if json.loads((args.output / "summary.json").read_text()) != expected:
        raise ValueError("V168 summary replay differs")
    old, inverse = load_orders(args.old_layout, args.order, args.v164_terminal)
    labels = records(args.output / "proxy-labels.jsonl")
    for feature, label in zip(features, labels):
        nominees = feature["nominees"]
        units = candidate_units(nominees, old, inverse, ROWS, UNIT_ROWS)
        rank_by_unit: dict[int, int] = {}
        for rank, old_row in enumerate(nominees, start=1):
            unit = int(inverse[old[old_row]]) // UNIT_ROWS
            rank_by_unit[unit] = min(rank, rank_by_unit.get(unit, rank))
        adjacent = [unit for unit in units if unit not in rank_by_unit]
        if (feature["candidate_units"] != len(units)
                or feature["candidate_rows"] != sum(
                    min(UNIT_ROWS, ROWS - unit * UNIT_ROWS) for unit in units)
                or [row[0] for row in feature["adjacent_features"]] != adjacent
                or [row[0] for row in label["actual_by_unit"]] != adjacent
                or any(row[2] != min(rank_by_unit.get(row[0] - 1, 513),
                                     rank_by_unit.get(row[0] + 1, 513))
                       for row in feature["adjacent_features"])):
            raise ValueError("V168 independent candidate geometry differs")
    with tempfile.TemporaryDirectory(prefix="borsuk-v168-replay-") as temp:
        replay = Path(temp) / "prepare"
        prepare(argparse.Namespace(**{**vars(args), "output": replay}))
        for name in ("features.jsonl", "proxy-labels.jsonl", "prepare-seal.json"):
            if sha256(replay / name) != sha256(args.output / name):
                raise ValueError(f"V168 authenticated prepare replay differs: {name}")
    return {"schema": SCHEMA + "-check", "status": "pass",
            "queries": PANEL_ROWS, "decision": expected["decision"]}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("phase", choices=("prepare", "plan", "evaluate", "check"))
    parser.add_argument("--output", type=Path, required=True)
    for name in ("source", "old_layout", "old_sq8", "router", "order",
                 "v164_terminal"):
        parser.add_argument("--" + name.replace("_", "-"), type=Path)
    args = parser.parse_args()
    if args.phase in ("prepare", "check"):
        if any(getattr(args, name) is None for name in (
                "source", "old_layout", "old_sq8", "router", "order",
                "v164_terminal")):
            parser.error("prepare/check requires every authenticated input")
    if args.phase == "prepare":
        prepare(args)
    elif args.phase == "plan":
        plan(args.output)
    elif args.phase == "evaluate":
        evaluate(args.output)
    else:
        print(canonical(check(args)), end="")


if __name__ == "__main__":
    main()
