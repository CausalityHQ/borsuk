#!/usr/bin/env python3
"""Sealed V167 source-pseudoquery calibration and holdout gate."""

from __future__ import annotations

import argparse
import itertools
import json
from pathlib import Path

import numpy as np

from scripts.v114_1m_paired import nominate_region_pq64
from scripts.v114_weighted_interval_plan import optimal_weighted_intervals
from scripts.v155_relaion_returned_quality import (
    DIMS, LAYOUT_SHA, ROWS, SOURCE_SHA, SQ8_SHA, sha256,
)
from scripts.v164_smooth_layout_1m import CAP_BYTES, CAP_GETS, DTYPE, source_arrays
from scripts.v165_unit_interval_resources import (
    UNIT_BYTES, UNIT_ROWS, V164_ORDER_SHA, V164_TERMINAL_SHA, load_orders,
)
from scripts.v166_surrogate_probe import candidate_units, select_pseudoqueries
from scripts.v166_surrogate_ranking_run import (
    ROUTER_MANIFEST_SHA, _score, canonical, load_frozen_v115_router, records,
)
from scripts.v167_min_cost_vote_frontier import minimum_vote_frontier

SCHEMA = "borsuk-v167-vote-frontier-v1"
FRACTIONS = ((4, 5), (9, 10), (19, 20), (39, 40), (99, 100), (1, 1))
PANEL_ROWS = 128
P95_INDEX = (95 * PANEL_ROWS + 99) // 100 - 1
FIRST_PSEUDO_ORDINAL = 256
QUERY_COUNT = 2 * PANEL_ROWS
UNIT_COUNT = ROWS // UNIT_ROWS
MAX_UNITS = CAP_BYTES // UNIT_BYTES


def quality_pass(captures: list[tuple[int, int]]) -> bool:
    """Require >=99% aggregate capture and nearest-rank p95 loss <=1."""
    if (len(captures) != PANEL_ROWS or
            any(base < 0 or candidate < 0 for base, candidate in captures)):
        raise ValueError("V167 quality panel differs")
    baseline = sum(base for base, _ in captures)
    if baseline == 0:
        raise ValueError("V167 full-cap capture is empty")
    losses = sorted(max(0, base - candidate) for base, candidate in captures)
    return (100 * sum(candidate for _, candidate in captures) >= 99 * baseline
            and losses[P95_INDEX] <= 1)


def select_fraction(
    fit_captures: dict[tuple[int, int], list[tuple[int, int]]],
) -> tuple[int, int] | None:
    if set(fit_captures) != set(FRACTIONS):
        raise ValueError("V167 exact rational grid differs")
    return next((fraction for fraction in FRACTIONS
                 if quality_pass(fit_captures[fraction])), None)


def holdout_pass(fraction: tuple[int, int], metrics: dict) -> bool:
    return (fraction != (1, 1) and metrics["quality_pass"]
            and 10 * metrics["candidate_bytes"] <= 9 * metrics["baseline_bytes"]
            and 5 * metrics["candidate_gets"] <= 6 * metrics["baseline_gets"])


def plan_roster(
    votes: dict[int, int], primary_units: tuple[int, ...], *,
    primary_count: int, nominee_count: int, page_count: int,
    max_gets: int, max_units: int, unit_bytes: int,
) -> dict:
    if (not 0 < primary_count < nominee_count <= 512
            or sum(votes.values()) != 513 * primary_count + nominee_count - primary_count
            or unit_bytes <= 0):
        raise ValueError("V167 513/1 roster weights differ")
    full_score, full_intervals = optimal_weighted_intervals(
        votes, page_count=page_count, max_gets=max_gets,
        max_units=max_units, full_page_units=1, last_page_units=1)
    full_covered = {unit for start, end in full_intervals
                    for unit in range(start, end + 1)}
    if not set(primary_units).issubset(full_covered):
        raise ValueError("V167 primary units infeasible at full cap")
    baseline_units = sum(end - start + 1 for start, end in full_intervals)
    planned = {"baseline": {
        "intervals": [list(pair) for pair in full_intervals],
        "bytes": baseline_units * unit_bytes, "gets": len(full_intervals),
        "score": full_score}, "fractions": {}}
    for fraction in FRACTIONS:
        frontier = minimum_vote_frontier(
            votes, primary_units=primary_units,
            primary_vote_total=513 * primary_count,
            secondary_vote_total=nominee_count - primary_count,
            fraction=fraction, page_count=page_count, max_gets=max_gets,
            max_units=max_units)
        if frontier.full_score != full_score:
            raise AssertionError("V167 full-cap score differs")
        planned["fractions"][f"{fraction[0]}/{fraction[1]}"] = {
            "intervals": [list(pair) for pair in frontier.intervals],
            "bytes": frontier.units * unit_bytes,
            "gets": len(frontier.intervals),
            "score": frontier.achieved_score,
            "target_score": frontier.target_score,
        }
    return planned


def prepare(args: argparse.Namespace) -> None:
    if args.output.exists():
        raise ValueError("V167 output directory already exists")
    if sha256(args.source) != SOURCE_SHA or sha256(args.old_sq8) != SQ8_SHA:
        raise ValueError("V167 source or SQ8 identity differs")
    router_manifest, planes = load_frozen_v115_router(
        args.router, ROUTER_MANIFEST_SHA, ROWS, DIMS)
    if (router_manifest["source_sha256"] != SOURCE_SHA
            or router_manifest["layout_sha256"] != LAYOUT_SHA
            or router_manifest["sq8_sha256"] != SQ8_SHA):
        raise ValueError("V167 frozen router authority differs")
    old, inverse = load_orders(args.old_layout, args.order, args.v164_terminal)
    order = np.load(args.order, allow_pickle=False)
    old_inverse = np.empty(ROWS, dtype=np.int64)
    old_inverse[old] = np.arange(ROWS, dtype=np.int64)
    source_ids, vectors, _ = source_arrays(args.source)
    sq8 = np.memmap(args.old_sq8, dtype=DTYPE, mode="r", shape=(ROWS,))
    if not np.array_equal(sq8["id"], source_ids[old]):
        raise ValueError("V167 SQ8/source ID mapping differs")
    selected = select_pseudoqueries(source_ids, FIRST_PSEUDO_ORDINAL + QUERY_COUNT)
    id_to_source = {int(identifier): row for row, identifier in enumerate(source_ids)}
    args.output.mkdir(parents=True)
    with ((args.output / "rosters.jsonl").open("x") as rosters,
          (args.output / "proxy-labels.jsonl").open("x") as labels):
        for ordinal, stable_id in enumerate(selected[FIRST_PSEUDO_ORDINAL:],
                                            start=FIRST_PSEUDO_ORDINAL):
            source_row = id_to_source[stable_id]
            query = np.asarray(vectors[source_row], dtype=np.float32)
            _, _, nominated = nominate_region_pq64(
                query, planes["summaries"], planes["books"], planes["codes"],
                page_rows=256, blocks_per_page=2, regions=1024, shortlist=512)
            nominees = np.asarray(nominated, dtype=np.int64)
            if nominees.size != 512 or np.unique(nominees).size != 512:
                raise ValueError("V167 PQ64 nominee roster differs")
            own_old = int(old_inverse[source_row])
            eligible_nominees = nominees[nominees != own_old]
            _, nominee_scores = _score(
                sq8, query, eligible_nominees, planes["low"], planes["step"], 1)
            nominee_rank = np.lexsort((sq8["id"][eligible_nominees], nominee_scores))
            primary = eligible_nominees[nominee_rank[:100]].astype(int).tolist()
            if len(primary) != 100:
                raise ValueError("V167 exact primary roster differs")
            threshold = float(nominee_scores[nominee_rank[99]])
            primary_set = set(primary)
            votes: dict[int, int] = {}
            for old_row in nominees.tolist():
                unit = int(inverse[old[old_row]]) // UNIT_ROWS
                votes[unit] = votes.get(unit, 0) + (
                    513 if old_row in primary_set else 1)
            primary_units = sorted({int(inverse[old[row]]) // UNIT_ROWS
                                    for row in primary})
            unit_list = candidate_units(
                nominees.tolist(), old, inverse, ROWS, UNIT_ROWS)
            physical = np.concatenate([
                np.arange(unit * UNIT_ROWS, min((unit + 1) * UNIT_ROWS, ROWS),
                          dtype=np.int64) for unit in unit_list])
            candidate_old = old_inverse[order[physical]]
            _, candidate_scores = _score(
                sq8, query, candidate_old, planes["low"], planes["step"], 1)
            eligible = (~np.isin(candidate_old, nominees)) & (candidate_old != own_old)
            actual_by_unit = []
            cursor = 0
            for unit in unit_list:
                stop = cursor + min(UNIT_ROWS, ROWS - unit * UNIT_ROWS)
                actual_by_unit.append([unit, int(np.count_nonzero(
                    eligible[cursor:stop] &
                    (candidate_scores[cursor:stop] <= threshold)))])
                cursor = stop
            if cursor != len(candidate_scores):
                raise ValueError("V167 proxy candidate score slices differ")
            rosters.write(canonical({
                "query_ordinal": ordinal, "source_id": stable_id,
                "nominees": nominees.astype(int).tolist(), "primary": primary,
                "primary_units": primary_units,
                "votes": [[unit, weight] for unit, weight in sorted(votes.items())],
            }))
            labels.write(canonical({
                "query_ordinal": ordinal, "source_id": stable_id,
                "candidate_rows": len(candidate_scores),
                "eligible_rows": int(np.count_nonzero(eligible)),
                "threshold": threshold,
                "actual_by_unit": actual_by_unit,
            }))
    (args.output / "prepare-seal.json").write_text(canonical({
        "schema": SCHEMA + "-prepare-seal", "dataset": "ReLAION-1M",
        "split": "source-pseudoquery-257-512", "proxy_labels_opened": False,
        "query_ordinal_convention": "zero-based-hash-rank-256-through-511",
        "source_sha256": SOURCE_SHA, "old_sq8_sha256": SQ8_SHA,
        "router_manifest_sha256": ROUTER_MANIFEST_SHA,
        "old_layout_sha256": LAYOUT_SHA, "order_sha256": V164_ORDER_SHA,
        "v164_terminal_sha256": V164_TERMINAL_SHA,
        "pseudo_ids": list(selected[FIRST_PSEUDO_ORDINAL:]),
        "rosters_sha256": sha256(args.output / "rosters.jsonl"),
        "proxy_labels_sha256": sha256(args.output / "proxy-labels.jsonl"),
    }))


def _verify_prepare(output: Path, *, open_labels: bool) -> list[dict]:
    rosters = records(output / "rosters.jsonl")
    seal = json.loads((output / "prepare-seal.json").read_text())
    if (len(rosters) != QUERY_COUNT
            or [row.get("query_ordinal") for row in rosters]
                != list(range(FIRST_PSEUDO_ORDINAL,
                              FIRST_PSEUDO_ORDINAL + QUERY_COUNT))
            or len({row.get("source_id") for row in rosters}) != QUERY_COUNT
            or seal.get("schema") != SCHEMA + "-prepare-seal"
            or seal.get("dataset") != "ReLAION-1M"
            or seal.get("split") != "source-pseudoquery-257-512"
            or seal.get("query_ordinal_convention")
                != "zero-based-hash-rank-256-through-511"
            or seal.get("proxy_labels_opened") is not False
            or seal.get("source_sha256") != SOURCE_SHA
            or seal.get("old_sq8_sha256") != SQ8_SHA
            or seal.get("router_manifest_sha256") != ROUTER_MANIFEST_SHA
            or seal.get("old_layout_sha256") != LAYOUT_SHA
            or seal.get("order_sha256") != V164_ORDER_SHA
            or seal.get("v164_terminal_sha256") != V164_TERMINAL_SHA
            or seal.get("pseudo_ids") != [row["source_id"] for row in rosters]
            or seal.get("rosters_sha256") != sha256(output / "rosters.jsonl")):
        raise ValueError("V167 prepare roster seal differs")
    if open_labels and seal.get("proxy_labels_sha256") != sha256(
            output / "proxy-labels.jsonl"):
        raise ValueError("V167 proxy label digest differs")
    return rosters


def _planned_row(roster: dict) -> dict:
    nominees, primary = roster["nominees"], roster["primary"]
    if (len(nominees) != 512 or len(set(nominees)) != 512
            or len(primary) != 100 or len(set(primary)) != 100
            or not set(primary).issubset(nominees)
            or any(type(row) is not int or not 0 <= row < ROWS
                   for row in nominees + primary)):
        raise ValueError("V167 frozen nominee or primary roster differs")
    votes = {int(unit): int(weight) for unit, weight in roster["votes"]}
    if len(votes) != len(roster["votes"]):
        raise ValueError("V167 duplicate vote unit")
    primary_units = tuple(roster["primary_units"])
    planned = plan_roster(
        votes, primary_units, primary_count=100, nominee_count=512,
        page_count=UNIT_COUNT, max_gets=CAP_GETS,
        max_units=MAX_UNITS, unit_bytes=UNIT_BYTES)
    return {"query_ordinal": roster["query_ordinal"],
            "source_id": roster["source_id"], **planned}


def plan(output: Path) -> None:
    rosters = _verify_prepare(output, open_labels=False)
    with (output / "plans.jsonl").open("x") as dest:
        for roster in rosters:
            dest.write(canonical(_planned_row(roster)))
    (output / "plan-seal.json").write_text(canonical({
        "schema": SCHEMA + "-plan-seal", "proxy_labels_opened": False,
        "prepare_seal_sha256": sha256(output / "prepare-seal.json"),
        "rosters_sha256": sha256(output / "rosters.jsonl"),
        "plans_sha256": sha256(output / "plans.jsonl"),
        "fractions": [f"{n}/{d}" for n, d in FRACTIONS],
        "fit_queries": PANEL_ROWS, "holdout_queries": PANEL_ROWS,
    }))


def _verify_plans(output: Path, rosters: list[dict]) -> list[dict]:
    seal = json.loads((output / "plan-seal.json").read_text())
    plans = records(output / "plans.jsonl")
    if (len(plans) != QUERY_COUNT
            or seal.get("schema") != SCHEMA + "-plan-seal"
            or seal.get("proxy_labels_opened") is not False
            or seal.get("prepare_seal_sha256")
                != sha256(output / "prepare-seal.json")
            or seal.get("rosters_sha256") != sha256(output / "rosters.jsonl")
            or seal.get("plans_sha256") != sha256(output / "plans.jsonl")
            or seal.get("fractions") != [f"{n}/{d}" for n, d in FRACTIONS]
            or seal.get("fit_queries") != PANEL_ROWS
            or seal.get("holdout_queries") != PANEL_ROWS
            or any(plan_row.get("query_ordinal") != roster["query_ordinal"]
                   or plan_row.get("source_id") != roster["source_id"]
                   for roster, plan_row in zip(rosters, plans))):
        raise ValueError("V167 GT-blind plan seal differs")
    return plans


def _capture(label: dict, planned: dict) -> int:
    actual = label["actual_by_unit"]
    if (len(actual) != len({unit for unit, _ in actual})
            or any(type(unit) is not int or type(count) is not int
                   or not 0 <= unit < UNIT_COUNT or count < 0
                   for unit, count in actual)):
        raise ValueError("V167 proxy label unit counts differ")
    intervals = planned["intervals"]
    if not intervals:
        raise ValueError("V167 plan has no physical interval")
    previous = -1
    for start, end in intervals:
        if (type(start) is not int or type(end) is not int
                or not 0 <= start <= end < UNIT_COUNT or start <= previous):
            raise ValueError("V167 plan interval geometry differs")
        previous = end
    bytes_count = sum(end - start + 1 for start, end in intervals) * UNIT_BYTES
    if (bytes_count != planned["bytes"] or len(intervals) != planned["gets"]
            or len(intervals) > CAP_GETS or bytes_count > CAP_BYTES):
        raise ValueError("V167 plan physical charge differs")
    return sum(count for unit, count in actual
               if any(start <= unit <= end for start, end in intervals))


def _panel_metrics(labels: list[dict], plans: list[dict],
                   fraction: tuple[int, int]) -> dict:
    key = f"{fraction[0]}/{fraction[1]}"
    captures = []
    baseline_bytes = candidate_bytes = baseline_gets = candidate_gets = 0
    for label, plan_row in zip(labels, plans):
        if (label["query_ordinal"] != plan_row["query_ordinal"]
                or label["source_id"] != plan_row["source_id"]):
            raise ValueError("V167 proxy/plan query identity differs")
        baseline = plan_row["baseline"]
        candidate = plan_row["fractions"][key]
        captures.append((_capture(label, baseline), _capture(label, candidate)))
        baseline_bytes += baseline["bytes"]
        candidate_bytes += candidate["bytes"]
        baseline_gets += baseline["gets"]
        candidate_gets += candidate["gets"]
    return {"fraction": key, "quality_pass": quality_pass(captures),
            "baseline_capture": sum(base for base, _ in captures),
            "candidate_capture": sum(candidate for _, candidate in captures),
            "p95_loss": sorted(max(0, base - candidate)
                               for base, candidate in captures)[P95_INDEX],
            "baseline_bytes": baseline_bytes, "candidate_bytes": candidate_bytes,
            "baseline_gets": baseline_gets, "candidate_gets": candidate_gets}


def _summary(output: Path, rosters: list[dict], plans: list[dict]) -> dict:
    with (output / "proxy-labels.jsonl").open() as source:
        fit_labels = [json.loads(line) for line in itertools.islice(source, PANEL_ROWS)]
        if len(fit_labels) != PANEL_ROWS:
            raise ValueError("V167 fit proxy label count differs")
        fit = [_panel_metrics(fit_labels, plans[:PANEL_ROWS], fraction)
               for fraction in FRACTIONS]
        selected = select_fraction({
            fraction: [
                (_capture(label, plan_row["baseline"]),
                 _capture(label, plan_row["fractions"][f"{fraction[0]}/{fraction[1]}"]))
                for label, plan_row in zip(fit_labels, plans[:PANEL_ROWS])]
            for fraction in FRACTIONS})
        holdout = None
        decision = "killed"
        if selected is not None and selected != (1, 1):
            holdout_labels = [json.loads(line)
                              for line in itertools.islice(source, PANEL_ROWS)]
            if len(holdout_labels) != PANEL_ROWS or source.readline():
                raise ValueError("V167 holdout proxy label count differs")
            holdout = _panel_metrics(holdout_labels, plans[PANEL_ROWS:], selected)
            if holdout_pass(selected, holdout):
                decision = "advance-to-100k"
    return {"schema": SCHEMA + "-summary", "dataset": "ReLAION-1M",
            "split": "source-pseudoquery-fit-128-holdout-128",
            "proxy_not_validation_gt": True, "queries": QUERY_COUNT,
            "fit": fit, "selected_fraction":
                None if selected is None else f"{selected[0]}/{selected[1]}",
            "holdout": holdout, "decision": decision,
            "plan_seal_sha256": sha256(output / "plan-seal.json"),
            "proxy_labels_sha256": sha256(output / "proxy-labels.jsonl")}


def evaluate(output: Path) -> None:
    rosters = _verify_prepare(output, open_labels=True)
    plans = _verify_plans(output, rosters)
    with (output / "summary.json").open("x") as dest:
        dest.write(canonical(_summary(output, rosters, plans)))


def _validate_label_geometry(
    roster: dict, label: dict, old: np.ndarray, inverse: np.ndarray,
    rows: int, unit_rows: int,
) -> None:
    candidate = candidate_units(
        roster["nominees"], old, inverse, rows, unit_rows)
    label_units = tuple(unit for unit, _ in label["actual_by_unit"])
    candidate_rows = sum(min(unit_rows, rows - unit * unit_rows)
                         for unit in candidate)
    if (label["query_ordinal"] != roster["query_ordinal"]
            or label["source_id"] != roster["source_id"]
            or label_units != candidate
            or label["candidate_rows"] != candidate_rows
            or not 0 <= label["eligible_rows"] <= candidate_rows
            or sum(count for _, count in label["actual_by_unit"])
                > label["eligible_rows"]):
        raise ValueError("V167 checker proxy candidate geometry differs")


def _read_labels_for_check(output: Path, selected_fraction: str | None) -> list[dict]:
    limit = (PANEL_ROWS if selected_fraction in (None, "1/1")
             else QUERY_COUNT)
    with (output / "proxy-labels.jsonl").open() as source:
        labels = [json.loads(line) for line in itertools.islice(source, limit)]
        if len(labels) != limit or (limit == QUERY_COUNT and source.readline()):
            raise ValueError("V167 checker proxy label count differs")
    return labels


def check(output: Path, old_layout: Path, order: Path,
          v164_terminal: Path) -> dict:
    rosters = _verify_prepare(output, open_labels=True)
    plans = _verify_plans(output, rosters)
    expected = _summary(output, rosters, plans)
    labels = _read_labels_for_check(output, expected["selected_fraction"])
    old, inverse = load_orders(old_layout, order, v164_terminal)
    for roster, recorded in zip(rosters, plans):
        primary = set(roster["primary"])
        votes: dict[int, int] = {}
        for physical in roster["nominees"]:
            unit = int(inverse[old[physical]]) // UNIT_ROWS
            votes[unit] = votes.get(unit, 0) + (513 if physical in primary else 1)
        units = sorted({int(inverse[old[physical]]) // UNIT_ROWS
                        for physical in primary})
        if (roster["votes"] != [[unit, weight] for unit, weight in sorted(votes.items())]
                or roster["primary_units"] != units
                or recorded != _planned_row(roster)):
            raise ValueError(f"V167 independent plan replay differs at {roster['query_ordinal']}")
    for roster, label in zip(rosters, labels):
        _validate_label_geometry(roster, label, old, inverse, ROWS, UNIT_ROWS)
    if json.loads((output / "summary.json").read_text()) != expected:
        raise ValueError("V167 summary replay differs")
    return {"schema": SCHEMA + "-check", "status": "pass",
            "queries": QUERY_COUNT, "decision": expected["decision"]}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("phase", choices=("prepare", "plan", "evaluate", "check"))
    parser.add_argument("--output", type=Path, required=True)
    for name in ("source", "old_layout", "old_sq8", "router", "order",
                 "v164_terminal"):
        parser.add_argument("--" + name.replace("_", "-"), type=Path)
    args = parser.parse_args()
    if args.phase == "prepare":
        if any(getattr(args, name) is None for name in (
                "source", "old_layout", "old_sq8", "router", "order",
                "v164_terminal")):
            parser.error("prepare requires every authenticated input")
        prepare(args)
    elif args.phase == "plan":
        plan(args.output)
    elif args.phase == "evaluate":
        evaluate(args.output)
    else:
        if any(getattr(args, name) is None for name in (
                "old_layout", "order", "v164_terminal")):
            parser.error("check requires frozen physical-order inputs")
        print(canonical(check(args.output, args.old_layout, args.order,
                              args.v164_terminal)), end="")


if __name__ == "__main__":
    main()
