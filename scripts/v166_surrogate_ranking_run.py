#!/usr/bin/env python3
"""V166 GT-blind source-pseudoquery ranking probe on pinned V164 order."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np

from scripts.v114_1m_paired import nominate_region_pq64
from scripts.v114_exact_local_100k import score_reference
from scripts.v115_source_router import load_source_router
from scripts.v155_relaion_returned_quality import (
    DIMS, LAYOUT_SHA, ROWS, SOURCE_SHA, SQ8_SHA, sha256,
)
from scripts.v164_smooth_layout_1m import DTYPE, source_arrays
from scripts.v165_unit_interval_resources import (
    UNIT_BYTES, UNIT_ROWS, V164_ORDER_SHA, V164_TERMINAL_SHA,
    load_orders, route,
)
from scripts.v166_surrogate_core import MASS_SCALE, fit_alpha, predict_count
from scripts.v166_surrogate_probe import (
    candidate_units, captured_exceedances, modeled_plan,
    select_pseudoqueries, source_unit_moments, unit_moment_without,
)

SCHEMA = "borsuk-v166-surrogate-ranking-v1"
ROUTER_MANIFEST_SHA = "d558a77443d6a1a50b9b3d01e821f134b1cc0992aa8bcb7ef3dc9ed2941221fe"
QUERY_COUNT = 256
FIT_COUNT = 128
BOOTSTRAP_SEED = 166
BOOTSTRAP_REPETITIONS = 10_000


def canonical(value: dict) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n"


def records(path: Path) -> list[dict]:
    with path.open() as source:
        return [json.loads(line) for line in source]


def _unit_intervals(byte_ranges: list[list[int]]) -> tuple[tuple[int, int], ...]:
    if not byte_ranges or any(start % UNIT_BYTES or end % UNIT_BYTES or start >= end
                              for start, end in byte_ranges):
        raise ValueError("V166 baseline unit alignment differs")
    return tuple((start // UNIT_BYTES, end // UNIT_BYTES - 1)
                 for start, end in byte_ranges)


def _score(sq8: np.ndarray, query: np.ndarray, old_rows: np.ndarray,
           low: np.ndarray, step: np.ndarray,
           primary_count: int) -> tuple[list[int], np.ndarray]:
    # Gather scattered old physical rows once; the scalar-f32 reference
    # otherwise performs 768 separate random-index gathers per query.
    contiguous = np.ascontiguousarray(sq8[old_rows])
    local = np.arange(len(contiguous), dtype=np.int64)
    ranked_local, scores = score_reference(
        query=query, nominees=local, ids=contiguous["id"],
        norms=contiguous["norm"], codes=contiguous["code"], low=low, step=step,
        primary_count=primary_count,
    )
    return [int(old_rows[row]) for row in ranked_local], scores


def prepare(args: argparse.Namespace) -> None:
    if args.output.exists():
        raise ValueError("V166 output directory already exists")
    if sha256(args.source) != SOURCE_SHA or sha256(args.old_sq8) != SQ8_SHA:
        raise ValueError("V166 source or old SQ8 identity differs")
    if sha256(args.router / "manifest.json") != ROUTER_MANIFEST_SHA:
        raise ValueError("V166 V115 router identity differs")
    router_manifest, planes = load_source_router(args.router)
    if (router_manifest["source_sha256"] != SOURCE_SHA
            or router_manifest["layout_sha256"] != LAYOUT_SHA
            or router_manifest["sq8_sha256"] != SQ8_SHA
            or router_manifest["geometry"] != {
                "rows": ROWS, "dimensions": DIMS, "page_rows": 256,
                "blocks_per_page": 2, "subspaces": 64, "pq_width": 12,
                "pq_partition": "balanced_floor_v1",
            }):
        raise ValueError("V166 V115 router authority differs")
    old, inverse = load_orders(args.old_layout, args.order, args.v164_terminal)
    order = np.load(args.order, allow_pickle=False)
    old_inverse = np.empty(ROWS, dtype=np.int64)
    old_inverse[old] = np.arange(ROWS, dtype=np.int64)
    source_ids, vectors, _ = source_arrays(args.source)
    sq8 = np.memmap(args.old_sq8, dtype=DTYPE, mode="r", shape=(ROWS,))
    if not np.array_equal(sq8["id"], source_ids[old]):
        raise ValueError("V166 old SQ8/source ID mapping differs")
    means, residuals = source_unit_moments(vectors, order, UNIT_ROWS)
    pseudo_ids = select_pseudoqueries(source_ids, QUERY_COUNT)
    id_to_source = {int(identifier): row for row, identifier in enumerate(source_ids)}
    args.output.mkdir(parents=True)
    with (args.output / "means.npy").open("xb") as out:
        np.save(out, means, allow_pickle=False)
    with (args.output / "residuals.npy").open("xb") as out:
        np.save(out, residuals, allow_pickle=False)
    with (args.output / "cases.jsonl").open("x") as out:
        for ordinal, stable_id in enumerate(pseudo_ids):
            source_row = id_to_source[stable_id]
            query = np.asarray(vectors[source_row], dtype=np.float32)
            source_query = query.astype(np.float64)
            _, _, nominated = nominate_region_pq64(
                query, planes["summaries"], planes["books"], planes["codes"],
                page_rows=256, blocks_per_page=2, regions=1024, shortlist=512,
            )
            nominees = np.asarray(nominated, dtype=np.int64)
            if nominees.size != 512 or np.unique(nominees).size != 512:
                raise ValueError("V166 PQ64 nominee roster differs")
            own_old_physical = int(old_inverse[source_row])
            eligible_nominees = nominees[nominees != own_old_physical]
            _, nominee_scores = _score(
                sq8, query, eligible_nominees, planes["low"], planes["step"], 1,
            )
            nominee_rank = np.lexsort((sq8["id"][eligible_nominees], nominee_scores))
            primary = eligible_nominees[nominee_rank[:100]].astype(int).tolist()
            threshold = float(nominee_scores[nominee_rank[99]])
            units = candidate_units(nominees.tolist(), old, inverse, ROWS, UNIT_ROWS)
            ordered_physical = np.concatenate([
                np.arange(unit * UNIT_ROWS, min((unit + 1) * UNIT_ROWS, ROWS),
                          dtype=np.int64)
                for unit in units
            ])
            candidate_source = order[ordered_physical]
            candidate_old = old_inverse[candidate_source]
            _, scores = _score(
                sq8, query, candidate_old, planes["low"], planes["step"], 1,
            )
            source_scores = np.empty(len(scores), dtype=np.float64)
            for start in range(0, len(scores), 4096):
                stop = min(start + 4096, len(scores))
                delta = vectors[candidate_source[start:stop]].astype(np.float64)
                delta -= source_query
                source_scores[start:stop] = np.einsum("ij,ij->i", delta, delta)
            non_nominee = ~np.isin(candidate_old, nominees)
            non_nominee &= candidate_old != own_old_physical
            score_error = np.abs(source_scores[non_nominee] - scores[non_nominee])
            crossings = np.count_nonzero(
                (source_scores[non_nominee] <= threshold)
                != (scores[non_nominee] <= threshold))
            cases = []
            cursor = 0
            own_unit = int(inverse[source_row]) // UNIT_ROWS
            for unit in units:
                stop = cursor + min(UNIT_ROWS, ROWS - unit * UNIT_ROWS)
                eligible = int(np.count_nonzero(non_nominee[cursor:stop]))
                actual = int(np.count_nonzero(
                    non_nominee[cursor:stop] & (scores[cursor:stop] <= threshold)))
                source_actual = int(np.count_nonzero(
                    non_nominee[cursor:stop]
                    & (source_scores[cursor:stop] <= threshold)))
                if unit == own_unit:
                    mean, residual = unit_moment_without(
                        vectors, order, unit, source_row, UNIT_ROWS)
                else:
                    mean = means[unit].astype(np.float64)
                    residual = float(residuals[unit])
                center = float(np.sum((source_query - mean) ** 2))
                cases.append([unit, center, residual, eligible, actual, source_actual])
                cursor = stop
            if cursor != len(scores):
                raise ValueError("V166 candidate score slices differ")
            primary_units = sorted({int(inverse[old[row]]) // UNIT_ROWS
                                    for row in primary})
            row = {"query_ordinal": ordinal, "source_id": stable_id,
                   "source_row": source_row, "nominees": nominees.astype(int).tolist(),
                   "primary": primary, "primary_units": primary_units,
                   "threshold": threshold, "unit_cases": cases,
                   "candidate_rows": len(scores), "own_unit": own_unit,
                   "eligible_rows": int(np.count_nonzero(non_nominee)),
                   "source_sq8_abs_error_sum": float(score_error.sum()),
                   "source_sq8_abs_error_max": float(score_error.max()) if score_error.size else 0.0,
                   "source_sq8_threshold_crossings": int(crossings)}
            if ordinal >= FIT_COUNT:
                ranges, amount, _ = route(primary, row["nominees"], old, inverse)
                baseline_intervals = _unit_intervals(ranges)
                if not all(any(start <= unit <= end for start, end in baseline_intervals)
                           for unit in primary_units):
                    raise ValueError("V166 V165 baseline omits exact-primary unit")
                row["baseline_intervals"] = baseline_intervals
                row["baseline_bytes"] = amount
                row["baseline_gets"] = len(ranges)
            out.write(canonical(row))
    seal = {"schema": SCHEMA + "-prepare-seal", "gt_opened": False,
            "source_sha256": SOURCE_SHA, "old_sq8_sha256": SQ8_SHA,
            "router_manifest_sha256": ROUTER_MANIFEST_SHA,
            "old_layout_sha256": LAYOUT_SHA,
            "order_sha256": sha256(args.order),
            "v164_terminal_sha256": sha256(args.v164_terminal),
            "pseudo_ids": list(pseudo_ids),
            "cases_sha256": sha256(args.output / "cases.jsonl"),
            "means_sha256": sha256(args.output / "means.npy"),
            "residuals_sha256": sha256(args.output / "residuals.npy")}
    (args.output / "prepare-seal.json").write_text(canonical(seal))


def _fit_cases(rows: list[dict]) -> list[tuple]:
    return [(float(center), float(residual), DIMS, float(row["threshold"]),
             int(eligible), int(actual))
            for row in rows[:FIT_COUNT]
            for _, center, residual, eligible, actual, _ in row["unit_cases"]
            if eligible > 0]


def _paired_interval(differences: np.ndarray) -> tuple[float, float]:
    generator = np.random.default_rng(BOOTSTRAP_SEED)
    sampled = differences[generator.integers(
        0, len(differences), size=(BOOTSTRAP_REPETITIONS, len(differences)))]
    lo, hi = np.quantile(sampled.mean(axis=1), (0.025, 0.975), method="linear")
    return float(lo), float(hi)


def _planned_row(row: dict, alpha: float) -> dict:
    actual_by_unit = {int(unit): int(actual)
                      for unit, _, _, _, actual, _ in row["unit_cases"]}
    masses = {}
    for unit, center, residual, eligible, _, _ in row["unit_cases"]:
        mass = int(round(MASS_SCALE * predict_count(
            float(center), float(residual), DIMS, float(row["threshold"]),
            alpha, int(eligible))))
        if mass > 0:
            masses[int(unit)] = mass
    baseline = tuple(tuple(pair) for pair in row["baseline_intervals"])
    model = modeled_plan(
        masses, row["primary_units"], page_count=(ROWS + UNIT_ROWS - 1) // UNIT_ROWS,
        max_gets=int(row["baseline_gets"]),
        max_units=int(row["baseline_bytes"]) // UNIT_BYTES,
        nominee_count=len(row["nominees"]), unit_rows=UNIT_ROWS,
    )
    model_bytes = sum(end - start + 1 for start, end in model) * UNIT_BYTES
    if model_bytes > row["baseline_bytes"] or len(model) > row["baseline_gets"]:
        raise ValueError("V166 modeled plan violates matched cap")
    baseline_captured = captured_exceedances(actual_by_unit, baseline)
    model_captured = captured_exceedances(actual_by_unit, model)
    candidate_units_set = set(actual_by_unit)
    baseline_fetched = {unit for start, end in baseline for unit in range(start, end + 1)}
    model_fetched = {unit for start, end in model for unit in range(start, end + 1)}
    return {"query_ordinal": row["query_ordinal"],
            "source_id": row["source_id"],
            "baseline_intervals": baseline, "model_intervals": model,
            "baseline_bytes": row["baseline_bytes"],
            "model_bytes": model_bytes,
            "baseline_gets": row["baseline_gets"], "model_gets": len(model),
            "baseline_captured": baseline_captured,
            "model_captured": model_captured,
            "actual_universe_exceedances": sum(actual_by_unit.values()),
            "candidate_units": len(candidate_units_set),
            "candidate_rows": row["candidate_rows"],
            "baseline_fetched_outside_universe": len(baseline_fetched - candidate_units_set),
            "model_fetched_outside_universe": len(model_fetched - candidate_units_set),
            "difference": model_captured - baseline_captured}


def _calibration(rows: list[dict], alpha: float) -> dict:
    bins = [{"units": 0, "eligible_rows": 0, "predicted": 0.0,
             "sq8_actual": 0, "source_actual": 0} for _ in range(10)]
    for row in rows:
        for _, center, residual, eligible, actual, source_actual in row["unit_cases"]:
            if eligible == 0:
                continue
            predicted = predict_count(
                float(center), float(residual), DIMS, float(row["threshold"]),
                alpha, int(eligible))
            bin_index = min(9, int(10 * predicted / eligible))
            item = bins[bin_index]
            item["units"] += 1
            item["eligible_rows"] += int(eligible)
            item["predicted"] += predicted
            item["sq8_actual"] += int(actual)
            item["source_actual"] += int(source_actual)
    scored_rows = sum(row["eligible_rows"] for row in rows)
    abs_error = sum(row["source_sq8_abs_error_sum"] for row in rows)
    return {"bins": bins, "eligible_rows": scored_rows,
            "source_sq8_mean_abs_error": abs_error / scored_rows if scored_rows else 0.0,
            "source_sq8_max_abs_error": max(
                row["source_sq8_abs_error_max"] for row in rows),
            "source_sq8_threshold_crossings": sum(
                row["source_sq8_threshold_crossings"] for row in rows),
            "source_exceedances": sum(case[5] for row in rows for case in row["unit_cases"]),
            "sq8_exceedances": sum(case[4] for row in rows for case in row["unit_cases"])}


def _summary(rows: list[dict], plans: list[dict], alpha: float,
             seal_sha: str) -> dict:
    differences = np.asarray([row["difference"] for row in plans], dtype=np.float64)
    if differences.shape != (QUERY_COUNT - FIT_COUNT,):
        raise ValueError("V166 holdout plan count differs")
    lo, hi = _paired_interval(differences)
    return {"schema": SCHEMA + "-summary", "dataset": "ReLAION-1M",
            "split": "source-pseudoquery-holdout-128", "gt_opened": False,
            "queries": len(plans), "alpha": alpha,
            "mean_paired_captured_gain": float(differences.mean()),
            "paired_bootstrap_95": [lo, hi],
            "baseline_captured": sum(row["baseline_captured"] for row in plans),
            "model_captured": sum(row["model_captured"] for row in plans),
            "baseline_bytes": sum(row["baseline_bytes"] for row in plans),
            "model_bytes": sum(row["model_bytes"] for row in plans),
            "baseline_gets": sum(row["baseline_gets"] for row in plans),
            "model_gets": sum(row["model_gets"] for row in plans),
            "fit_calibration": _calibration(rows[:FIT_COUNT], alpha),
            "holdout_calibration": _calibration(rows[FIT_COUNT:], alpha),
            "candidate_units": sum(row["candidate_units"] for row in plans),
            "candidate_rows": sum(row["candidate_rows"] for row in plans),
            "baseline_uncaptured_exceedances": sum(
                row["actual_universe_exceedances"] - row["baseline_captured"]
                for row in plans),
            "model_uncaptured_exceedances": sum(
                row["actual_universe_exceedances"] - row["model_captured"]
                for row in plans),
            "baseline_fetched_outside_universe": sum(
                row["baseline_fetched_outside_universe"] for row in plans),
            "model_fetched_outside_universe": sum(
                row["model_fetched_outside_universe"] for row in plans),
            "decision": "ranking-pass" if lo > 0 and differences.mean() > 0 else "killed",
            "plan_seal_sha256": seal_sha}


def _verify_prepare(output: Path, rows: list[dict]) -> dict:
    seal = json.loads((output / "prepare-seal.json").read_text())
    if (len(rows) != QUERY_COUNT
            or [row.get("query_ordinal") for row in rows] != list(range(QUERY_COUNT))
            or len({row.get("source_id") for row in rows}) != QUERY_COUNT
            or seal.get("schema") != SCHEMA + "-prepare-seal"
            or seal.get("gt_opened") is not False
            or seal.get("source_sha256") != SOURCE_SHA
            or seal.get("old_sq8_sha256") != SQ8_SHA
            or seal.get("router_manifest_sha256") != ROUTER_MANIFEST_SHA
            or seal.get("old_layout_sha256") != LAYOUT_SHA
            or seal.get("order_sha256") != V164_ORDER_SHA
            or seal.get("v164_terminal_sha256") != V164_TERMINAL_SHA
            or seal.get("cases_sha256") != sha256(output / "cases.jsonl")
            or seal.get("means_sha256") != sha256(output / "means.npy")
            or seal.get("residuals_sha256") != sha256(output / "residuals.npy")
            or seal.get("pseudo_ids") != [row["source_id"] for row in rows]):
        raise ValueError("V166 prepare seal differs")
    return seal


def _audit_intervals(intervals: list[list[int]], primary: set[int]) -> tuple[int, int, set[int]]:
    covered: set[int] = set()
    previous_end = -1
    for pair in intervals:
        if (len(pair) != 2 or type(pair[0]) is not int or type(pair[1]) is not int
                or not 0 <= pair[0] <= pair[1] < ROWS // UNIT_ROWS
                or pair[0] <= previous_end):
            raise ValueError("V166 checker interval geometry differs")
        covered.update(range(pair[0], pair[1] + 1))
        previous_end = pair[1]
    if not intervals or not primary.issubset(covered):
        raise ValueError("V166 checker primary coverage differs")
    return len(covered) * UNIT_BYTES, len(intervals), covered


def _audit_plans(cases: list[dict], plans: list[dict]) -> None:
    for case, plan_row in zip(cases[FIT_COUNT:], plans):
        if (plan_row.get("query_ordinal") != case["query_ordinal"]
                or plan_row.get("source_id") != case["source_id"]):
            raise ValueError("V166 checker query identity differs")
        primary = set(case["primary_units"])
        baseline_bytes, baseline_gets, baseline_covered = _audit_intervals(
            case["baseline_intervals"], primary)
        model_bytes, model_gets, model_covered = _audit_intervals(
            plan_row["model_intervals"], primary)
        if (plan_row["baseline_intervals"] != case["baseline_intervals"]
                or baseline_bytes != case["baseline_bytes"]
                or baseline_gets != case["baseline_gets"]
                or model_bytes > baseline_bytes or model_gets > baseline_gets
                or baseline_gets > 32 or baseline_bytes > 16_777_216
                or plan_row["baseline_bytes"] != baseline_bytes
                or plan_row["model_bytes"] != model_bytes
                or plan_row["baseline_gets"] != baseline_gets
                or plan_row["model_gets"] != model_gets):
            raise ValueError("V166 checker matched physical charge differs")
        unit_actual = {int(item[0]): int(item[4]) for item in case["unit_cases"]}
        if len(unit_actual) != len(case["unit_cases"]):
            raise ValueError("V166 checker duplicate candidate unit")
        baseline_capture = sum(value for unit, value in unit_actual.items()
                               if unit in baseline_covered)
        model_capture = sum(value for unit, value in unit_actual.items()
                            if unit in model_covered)
        if (plan_row["baseline_captured"] != baseline_capture
                or plan_row["model_captured"] != model_capture
                or plan_row["actual_universe_exceedances"] != sum(unit_actual.values())
                or plan_row["difference"] != model_capture - baseline_capture
                or plan_row["candidate_units"] != len(unit_actual)
                or plan_row["candidate_rows"] != case["candidate_rows"]
                or plan_row["baseline_fetched_outside_universe"]
                   != len(baseline_covered - unit_actual.keys())
                or plan_row["model_fetched_outside_universe"]
                   != len(model_covered - unit_actual.keys())):
            raise ValueError("V166 checker capture accounting differs")


def plan(output: Path) -> None:
    rows = records(output / "cases.jsonl")
    _verify_prepare(output, rows)
    alpha = fit_alpha(_fit_cases(rows))
    plans = [_planned_row(row, alpha) for row in rows[FIT_COUNT:]]
    with (output / "plans.jsonl").open("x") as dest:
        for row in plans:
            dest.write(canonical(row))
    plan_seal = {"schema": SCHEMA + "-plan-seal", "gt_opened": False,
                 "prepare_seal_sha256": sha256(output / "prepare-seal.json"),
                 "cases_sha256": sha256(output / "cases.jsonl"),
                 "plans_sha256": sha256(output / "plans.jsonl"),
                 "alpha": alpha, "fit_queries": FIT_COUNT,
                 "holdout_queries": QUERY_COUNT - FIT_COUNT}
    (output / "plan-seal.json").write_text(canonical(plan_seal))
    (output / "summary.json").write_text(
        canonical(_summary(rows, plans, alpha, sha256(output / "plan-seal.json"))))


def check(output: Path) -> dict:
    rows = records(output / "cases.jsonl")
    plans = records(output / "plans.jsonl")
    _verify_prepare(output, rows)
    plan_seal = json.loads((output / "plan-seal.json").read_text())
    if (len(plans) != QUERY_COUNT - FIT_COUNT
            or plan_seal.get("schema") != SCHEMA + "-plan-seal"
            or plan_seal.get("gt_opened") is not False
            or plan_seal.get("fit_queries") != FIT_COUNT
            or plan_seal.get("holdout_queries") != QUERY_COUNT - FIT_COUNT
            or plan_seal.get("prepare_seal_sha256") != sha256(output / "prepare-seal.json")
            or plan_seal.get("cases_sha256") != sha256(output / "cases.jsonl")
            or plan_seal.get("plans_sha256") != sha256(output / "plans.jsonl")):
        raise ValueError("V166 checker artifact identity differs")
    alpha = fit_alpha(_fit_cases(rows))
    if plan_seal.get("alpha") != alpha:
        raise ValueError("V166 fitted alpha differs")
    _audit_plans(rows, plans)
    for row, declared in zip(rows[FIT_COUNT:], plans):
        if json.loads(canonical(_planned_row(row, alpha))) != declared:
            raise ValueError(f"V166 plan replay differs at {row['query_ordinal']}")
    expected = _summary(rows, plans, alpha, sha256(output / "plan-seal.json"))
    if json.loads((output / "summary.json").read_text()) != expected:
        raise ValueError("V166 summary replay differs")
    return {"schema": SCHEMA + "-check", "status": "pass",
            "queries": len(plans), "decision": expected["decision"]}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("phase", choices=("prepare", "plan", "check"))
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
    else:
        print(canonical(check(args.output)), end="")


if __name__ == "__main__":
    main()
