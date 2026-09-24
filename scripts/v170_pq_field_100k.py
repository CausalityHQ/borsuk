#!/usr/bin/env python3
"""GT-blind direct-PQ field with V163-matched physical resources."""

from __future__ import annotations

import argparse
import itertools
import json
import math
import tempfile
from pathlib import Path

import numpy as np

from scripts.v114_1m_paired import score_sq8_ranges
from scripts.v114_weighted_interval_plan import optimal_weighted_intervals
from scripts.v158_pq_primary_returned import (
    CAP_BYTES, CAP_GETS, DIMS, QUERIES, ROWS, authenticate, jsonl,
    load_truth, sha256, spread,
)
from scripts.v160_geometric_relayout_primary import DTYPE, ROW_BYTES
from scripts.v168_scored_neighbor_field import score_neighbor_field

SCHEMA = "borsuk-v170-pq-field-100k-v1"
UNIT_ROWS = 32
UNIT_BYTES = UNIT_ROWS * ROW_BYTES
V113_TERMINAL_SHA = "a578790e1b8479732d1836d89cdf3d5af87df5aa19e444c72a8ebd2eca1255cf"
V163_TERMINAL_SHA = "f4d2bd82d1ac4a7c7c52e03f1496c6a44ce77f619e0878d0e73577aaa6f4828f"
V113_SOURCE_SHA = "a199e151b89a496ed20e39fdd951591bbfb4817d682e9111ebe2e1cab7ae550d"
V163_ORDER_SHA = "d7be74b09ade0a7477b62c2e14d68b94640dede5ac38be240e6e7428d41ac6e6"
V163_SQ8_SHA = "76d325d20dd38063bb050f83cfa693f7748921c75280cb1bf1f988041329dd38"
V163_PLAN_SHA = "713511887fb7f853250ca073a40f432fb8fbb47471c0338e6c85468bcc30fecd"
V163_RAW_SHA = "a7656420c699b41a16ebf9124dae47ed81de977c6fb300128034d5503f6b16f1"
V113_FILES = {
    "seal": (2135, "803b00d9366bc8feb0e58e4900e27b1973c5d9578c33a0d6007fe4c28722c7b2"),
    "ids": (800128, "d31121d0ecd43bad93bf7e313bc467f158de24510989400cb9fd7dd667c2a57a"),
    "books": (786560, "e90c011aa3a013bed606d7f40b30bd41f5ed446637ac9c8724ced6cf2f078c87"),
    "codes": (6400128, "2eea1c265da723e799575b98ba29997eca9d58ef789a5244a96ca3bf89f6638f"),
}


def canonical(value: dict) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n"


def _file(path: Path, size: int, digest: str) -> None:
    if path.stat().st_size != size or sha256(path) != digest:
        raise ValueError(f"V170 frozen identity differs: {path.name}")


def _load_physical(args: argparse.Namespace):
    for name in ("requests", "reference", "manifest"):
        authenticate(getattr(args, name), name)
    _file(args.v113_terminal, 2941, V113_TERMINAL_SHA)
    v113_terminal = json.loads(args.v113_terminal.read_text())
    if (v113_terminal.get("status") != "complete"
            or v113_terminal.get("source_commit")
                != "9f4936fbb9bad5596921ff71004984f9b295bc57"
            or any(v113_terminal.get("artifacts", {}).get("artifact/" + field, {}).get("sha256")
                   != V113_FILES[name][1]
                   for name, field in (("seal", "seal.json"), ("ids", "ids.npy"),
                                       ("books", "pq_books.npy"),
                                       ("codes", "pq_codes.npy")))):
        raise ValueError("V170 closed V113 terminal differs")
    _file(args.v163_terminal, 1706, V163_TERMINAL_SHA)
    terminal = json.loads(args.v163_terminal.read_text())
    if (terminal.get("status") != "complete"
            or terminal.get("source_commit")
                != "134613f2d6ac206eed2af5dce94544901eb51712"
            or terminal.get("artifacts", {}).get("order.npy", {}).get("sha256") != V163_ORDER_SHA
            or terminal["artifacts"].get("new-sq8.bin", {}).get("sha256") != V163_SQ8_SHA
            or terminal["artifacts"].get("plans.jsonl", {}).get("sha256") != V163_PLAN_SHA
            or terminal["artifacts"].get("raw.jsonl", {}).get("sha256") != V163_RAW_SHA):
        raise ValueError("V170 closed V163 terminal differs")
    _file(args.order, 800128, V163_ORDER_SHA)
    _file(args.sq8, ROWS * ROW_BYTES, V163_SQ8_SHA)
    _file(args.v163_plans, 397910, V163_PLAN_SHA)
    for name, (size, digest) in V113_FILES.items():
        _file(getattr(args, "v113_" + name), size, digest)
    seal = json.loads(args.v113_seal.read_text())
    if (seal.get("source_sha256") != V113_SOURCE_SHA
            or seal.get("rows") != ROWS or seal.get("dimensions") != DIMS
            or any(seal.get("files", {}).get(field, {}).get("sha256") != V113_FILES[name][1]
                   for name, field in (("ids", "ids"), ("books", "pq_books"),
                                       ("codes", "pq_codes")))):
        raise ValueError("V170 V113 code-plane authority differs")
    ids = np.load(args.v113_ids, allow_pickle=False, mmap_mode="r")
    books = np.load(args.v113_books, allow_pickle=False, mmap_mode="r")
    codes = np.load(args.v113_codes, allow_pickle=False, mmap_mode="r")
    order = np.load(args.order, allow_pickle=False)
    sq8 = np.memmap(args.sq8, dtype=DTYPE, mode="r", shape=(ROWS,))
    if (ids.shape != (ROWS,) or ids.dtype != np.int64
            or books.shape != (64, 256, DIMS // 64) or books.dtype != np.float32
            or codes.shape != (ROWS, 64) or codes.dtype != np.uint8
            or order.shape != (ROWS,) or order.dtype != np.int64
            or not np.array_equal(np.sort(order), np.arange(ROWS))
            or not np.array_equal(sq8["id"], ids[order])):
        raise ValueError("V170 PQ/SQ8 source-row permutation differs")
    inverse = np.empty(ROWS, dtype=np.int64)
    inverse[order] = np.arange(ROWS, dtype=np.int64)
    identity = np.arange(ROWS, dtype=np.int64)
    return books, codes, order, inverse, identity, sq8


def _one_plan(request: dict, reference: dict, control: dict,
              books: np.ndarray, codes: np.ndarray, order: np.ndarray,
              inverse: np.ndarray, identity: np.ndarray) -> dict:
    ordinal = request.get("query_ordinal")
    nominees = request.get("nominees")
    primary = reference.get("primary")
    query = np.asarray(request.get("query"), dtype=np.float32)
    if (type(ordinal) is not int or reference.get("query_ordinal") != ordinal
            or control.get("query_ordinal") != ordinal
            or not isinstance(nominees, list) or len(nominees) != 512
            or not isinstance(primary, list) or len(primary) != 100
            or len(set(nominees)) != 512 or len(set(primary)) != 100
            or not set(primary).issubset(nominees)
            or any(type(row) is not int or not 0 <= row < ROWS
                   for row in nominees + primary)
            or query.shape != (DIMS,) or not np.isfinite(query).all()):
        raise ValueError("V170 frozen query geometry differs")
    control_ranges = control.get("ranges")
    control_bytes = control.get("bytes")
    if (not isinstance(control_ranges, list)
            or not 1 <= len(control_ranges) <= CAP_GETS
            or any(not isinstance(r, list) or len(r) != 2
                   or any(type(value) is not int for value in r)
                   or r[0] < 0 or r[1] > ROWS * ROW_BYTES
                   or r[0] % UNIT_BYTES or r[1] % UNIT_BYTES
                   or r[0] >= r[1] for r in control_ranges)
            or any(left[1] >= right[0] for left, right in
                   zip(control_ranges, control_ranges[1:]))
            or control_bytes != sum(b - a for a, b in control_ranges)
            or not 0 < control_bytes <= CAP_BYTES):
        raise ValueError("V170 V163 matched plan geometry differs")
    field = score_neighbor_field(
        query, nominees=tuple(nominees), old_order=identity,
        inverse_old=identity, new_order=order, inverse_new=inverse,
        books=books, codes=codes, unit_rows=UNIT_ROWS,
    )
    minimum: dict[int, float] = {}
    cursor = 0
    for unit in field.units:
        segment = field.scores[cursor:cursor + UNIT_ROWS]
        if segment.size != UNIT_ROWS:
            raise ValueError("V170 partial unit differs")
        minimum[int(unit)] = float(segment.min())
        cursor += UNIT_ROWS
    if cursor != field.scores.size:
        raise ValueError("V170 PQ field slice differs")
    mandatory = {int(inverse[row]) // UNIT_ROWS for row in primary}
    intervals = matched_plan(
        minimum, mandatory, page_count=ROWS // UNIT_ROWS,
        max_gets=len(control_ranges), max_units=control_bytes // UNIT_BYTES,
    )
    neighbor_scores = neighbor_rank_scores(
        nominees=nominees, inverse=inverse, units=field.units,
        unit_rows=UNIT_ROWS,
    )
    neighbor_intervals = matched_plan(
        neighbor_scores, mandatory, page_count=ROWS // UNIT_ROWS,
        max_gets=len(control_ranges), max_units=control_bytes // UNIT_BYTES,
    )
    result = {"query_ordinal": ordinal, "candidate_units": len(minimum),
              "mandatory_units": len(mandatory),
              "control_bytes": control_bytes,
              "control_gets": len(control_ranges)}
    for prefix, selected in (("pq", intervals), ("neighbor", neighbor_intervals)):
        ranges = [[a * UNIT_BYTES, (b + 1) * UNIT_BYTES] for a, b in selected]
        amount = sum(b - a for a, b in ranges)
        if amount > control_bytes or len(ranges) > len(control_ranges):
            raise AssertionError("V170 exceeds paired V163 resources")
        result[prefix + "_ranges"] = ranges
        result[prefix + "_bytes"] = amount
        result[prefix + "_gets"] = len(ranges)
    return result


def plan(args: argparse.Namespace) -> None:
    books, codes, order, inverse, identity, _ = _load_physical(args)
    count = 0
    with args.plans.open("x") as output:
        for request, reference, control in itertools.zip_longest(
                jsonl(args.requests), jsonl(args.reference), jsonl(args.v163_plans)):
            if any(row is None for row in (request, reference, control)):
                raise ValueError("V170 frozen query count differs")
            planned = _one_plan(request, reference, control,
                                books, codes, order, inverse, identity)
            if planned["query_ordinal"] != count:
                raise ValueError("V170 frozen query order differs")
            output.write(canonical(planned))
            count += 1
    if count != QUERIES:
        raise ValueError("V170 frozen query count differs")
    args.plan_seal.write_text(canonical({
        "schema": SCHEMA + "-plan-seal", "gt_opened": False,
        "queries": count, "requests_sha256": sha256(args.requests),
        "reference_sha256": sha256(args.reference),
        "v113_seal_sha256": V113_FILES["seal"][1],
        "v163_terminal_sha256": V163_TERMINAL_SHA,
        "v163_order_sha256": V163_ORDER_SHA,
        "v163_sq8_sha256": V163_SQ8_SHA,
        "v163_plans_sha256": V163_PLAN_SHA,
        "plans_sha256": sha256(args.plans),
    }))


def reduce(args: argparse.Namespace) -> None:
    authenticate(args.truth, "truth")
    _file(args.v163_raw, 1203637, V163_RAW_SHA)
    seal = json.loads(args.plan_seal.read_text())
    if (seal.get("schema") != SCHEMA + "-plan-seal"
            or seal.get("gt_opened") is not False
            or seal.get("queries") != QUERIES
            or seal.get("requests_sha256") != sha256(args.requests)
            or seal.get("reference_sha256") != sha256(args.reference)
            or seal.get("v113_seal_sha256") != V113_FILES["seal"][1]
            or seal.get("v163_order_sha256") != V163_ORDER_SHA
            or seal.get("v163_sq8_sha256") != V163_SQ8_SHA
            or seal.get("v163_plans_sha256") != V163_PLAN_SHA
            or seal.get("plans_sha256") != sha256(args.plans)
            or seal.get("v163_terminal_sha256") != V163_TERMINAL_SHA):
        raise ValueError("V170 GT-blind plan seal differs")
    _, _, _, inverse, _, sq8 = _load_physical(args)
    authority = json.loads(args.manifest.read_text())
    low = np.asarray(authority["low"], dtype=np.float32)
    step = np.asarray(authority["step"], dtype=np.float32)
    if (low.shape != (DIMS,) or step.shape != (DIMS,)
            or not np.isfinite(low).all() or not np.isfinite(step).all()
            or (step <= 0).any()):
        raise ValueError("V170 SQ8 quantizer differs")
    gold = load_truth(args.truth)
    positions = {int(identifier): int(position)
                 for position, identifier in enumerate(sq8["id"])}
    if len(positions) != ROWS:
        raise ValueError("V170 SQ8 stable IDs are not unique")
    values = {name: [] for name in (
        "pq_hits100", "pq_hits10", "pq_coverage", "pq_bytes", "pq_gets",
        "neighbor_hits100", "neighbor_hits10", "neighbor_coverage",
        "neighbor_bytes", "neighbor_gets", "control_hits100",
        "control_hits10", "control_bytes", "control_gets",
    )}
    pq_wins = pq_ties = pq_losses = 0
    neighbor_wins = neighbor_ties = neighbor_losses = 0
    count = 0
    with args.raw.open("x") as output:
        for request, reference, planned, control in itertools.zip_longest(
                jsonl(args.requests), jsonl(args.reference),
                jsonl(args.plans), jsonl(args.v163_raw)):
            if (request is None or reference is None
                    or planned is None or control is None
                    or any(row.get("query_ordinal") != count
                           for row in (request, reference, planned, control))
                    or planned["control_bytes"] != control["bytes"]
                    or planned["control_gets"] != control["gets"]
                    or any(planned[prefix + "_bytes"] > control["bytes"]
                           or planned[prefix + "_gets"] > control["gets"]
                           for prefix in ("pq", "neighbor"))):
                raise ValueError("V170 paired V163 row differs")
            validate_paired_plan(planned, reference["primary"], inverse,
                                 control)
            query = np.asarray(request["query"], dtype=np.float32)
            truth100 = set(map(int, gold[count]))
            row = {"query_ordinal": count,
                   "control_hits100": control["hits100"],
                   "control_hits10": control["hits10"],
                   "control_bytes": control["bytes"],
                   "control_gets": control["gets"]}
            for prefix in ("pq", "neighbor"):
                ranges = planned[prefix + "_ranges"]
                returned = score_sq8_ranges(
                    sq8, query, low, step, ranges, top_k=100,
                )
                hits100 = len(set(returned) & truth100)
                hits10 = len(set(returned[:10]) & set(map(int, gold[count, :10])))
                fetched = np.zeros(ROWS // UNIT_ROWS, dtype=bool)
                for start, end in ranges:
                    fetched[start // UNIT_BYTES:end // UNIT_BYTES] = True
                coverage = sum(bool(fetched[positions[identifier] // UNIT_ROWS])
                               for identifier in truth100)
                if hits100 > coverage:
                    raise AssertionError("V170 returned hits exceed physical coverage")
                row.update({prefix + "_returned_ids": returned,
                            prefix + "_hits100": hits100,
                            prefix + "_hits10": hits10,
                            prefix + "_coverage": coverage,
                            prefix + "_bytes": planned[prefix + "_bytes"],
                            prefix + "_gets": planned[prefix + "_gets"]})
            output.write(canonical(row))
            for name in values:
                values[name].append(row[name])
            difference = row["pq_hits100"] - row["neighbor_hits100"]
            pq_wins += difference > 0
            pq_ties += difference == 0
            pq_losses += difference < 0
            difference = row["neighbor_hits100"] - control["hits100"]
            neighbor_wins += difference > 0
            neighbor_ties += difference == 0
            neighbor_losses += difference < 0
            count += 1
    if count != QUERIES or sum(values["control_hits100"]) != 99_322:
        raise ValueError("V170 V163 closed control total differs")
    difference = np.asarray(values["pq_hits100"], dtype=np.int16) - np.asarray(
        values["neighbor_hits100"], dtype=np.int16,
    )
    rng = np.random.Generator(np.random.PCG64(170))
    sample = rng.integers(0, QUERIES, size=(10_000, QUERIES))
    total_samples = np.sort(difference[sample].sum(axis=1))
    interval = [int(total_samples[249]), int(total_samples[9749])]
    passed = (
        sum(values["pq_hits100"]) >= 99_347
        and int(difference.sum()) >= 10 and interval[0] > 0
        and sorted(values["pq_hits100"])[49] >= 98
        and sorted(values["pq_hits100"])[49]
            >= sorted(values["neighbor_hits100"])[49]
    )
    args.summary.write_text(canonical({
        "schema": SCHEMA + "-summary", "dataset": "ReLAION-100k D768",
        "split": "development-1000-reused", "queries": count,
        "decision": "advance-to-1m" if passed else "kill-feature-transfer",
        "metrics": {name: spread(column) for name, column in values.items()},
        "paired": {
            "pq_vs_neighbor": {"wins": pq_wins, "ties": pq_ties,
                               "losses": pq_losses, "gain": int(difference.sum()),
                               "bootstrap_total_95": interval},
            "neighbor_vs_v163": {"wins": neighbor_wins, "ties": neighbor_ties,
                                  "losses": neighbor_losses},
        },
        "raw_sha256": sha256(args.raw),
        "plan_seal_sha256": sha256(args.plan_seal),
        "v163_raw_sha256": V163_RAW_SHA,
    }))


def check(args: argparse.Namespace) -> dict:
    with tempfile.TemporaryDirectory(prefix="v170-check-") as temporary:
        root = Path(temporary)
        replay = argparse.Namespace(**vars(args))
        for field, name in (("plans", "plans.jsonl"), ("plan_seal", "plan-seal.json"),
                            ("raw", "raw.jsonl"), ("summary", "summary.json")):
            setattr(replay, field, root / name)
        plan(replay)
        reduce(replay)
        for field in ("plans", "plan_seal", "raw", "summary"):
            if sha256(getattr(args, field)) != sha256(getattr(replay, field)):
                raise ValueError(f"V170 {field} replay differs")
    return {"schema": SCHEMA + "-check", "status": "pass",
            "queries": QUERIES, "decision": json.loads(args.summary.read_text())["decision"]}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("phase", choices=("plan", "reduce", "check"))
    for field in ("requests", "reference", "manifest", "v113_terminal",
                  "v113_seal", "v113_ids",
                  "v113_books", "v113_codes", "v163_terminal", "order", "sq8",
                  "v163_plans", "plans", "plan_seal", "truth", "v163_raw",
                  "raw", "summary"):
        parser.add_argument("--" + field.replace("_", "-"), type=Path, required=True)
    args = parser.parse_args()
    if args.phase == "plan":
        plan(args)
    elif args.phase == "reduce":
        reduce(args)
    else:
        print(canonical(check(args)), end="")


def ranked_unit_weights(
    minimum_scores: dict[int, float], primary_units: set[int], *,
    page_count: int,
) -> dict[int, int]:
    """Deterministic direct-PQ rank with an exact-primary dominance weight."""
    if (type(page_count) is not int or page_count <= 0
            or not minimum_scores or not primary_units
            or not primary_units.issubset(minimum_scores)
            or any(type(unit) is not int or not 0 <= unit < page_count
                   or type(score) is not float or not math.isfinite(score)
                   for unit, score in minimum_scores.items())):
        raise ValueError("V170 scored-unit geometry differs")
    ranked = sorted(minimum_scores, key=lambda unit: (minimum_scores[unit], unit))
    count = len(ranked)
    priority = count * (count + 1) // 2 + 1
    if count * (count + 1) // 2 + priority * len(primary_units) >= 2**30:
        raise ValueError("V170 weighted frontier exceeds int32 bound")
    return {unit: count - rank + (priority if unit in primary_units else 0)
            for rank, unit in enumerate(ranked)}


def neighbor_rank_scores(
    *, nominees: list[int], inverse: np.ndarray, units: tuple[int, ...],
    unit_rows: int,
) -> dict[int, float]:
    """PQ-free nominee/adjacent rank on the same physical candidate units."""
    if (not nominees or len(set(nominees)) != len(nominees)
            or inverse.ndim != 1 or inverse.dtype != np.int64
            or unit_rows <= 0 or not units or len(set(units)) != len(units)
            or any(type(row) is not int or not 0 <= row < inverse.size
                   for row in nominees)):
        raise ValueError("V170 neighbor-rank geometry differs")
    voted: dict[int, int] = {}
    for rank, row in enumerate(nominees, start=1):
        unit = int(inverse[row]) // unit_rows
        voted[unit] = min(rank, voted.get(unit, rank))
    scores = {
        unit: float(voted.get(unit, min(voted.get(unit - 1, len(nominees) + 1),
                                        voted.get(unit + 1, len(nominees) + 1))))
        for unit in units
    }
    if any(score > len(nominees) for score in scores.values()):
        raise ValueError("V170 adjacent unit lacks a nominee neighbor")
    return scores


def matched_plan(
    minimum_scores: dict[int, float], primary_units: set[int], *,
    page_count: int, max_gets: int, max_units: int,
) -> tuple[tuple[int, int], ...]:
    """Optimize the score field under one query's V163 resource ceiling."""
    if (type(max_gets) is not int or type(max_units) is not int
            or not 1 <= max_gets <= 32 or not 1 <= max_units <= 672):
        raise ValueError("V170 paired resource ceiling differs")
    weights = ranked_unit_weights(
        minimum_scores, primary_units, page_count=page_count,
    )
    _, intervals = optimal_weighted_intervals(
        weights, page_count=page_count, max_gets=max_gets,
        max_units=max_units, full_page_units=1, last_page_units=1,
    )
    covered = {unit for start, end in intervals
               for unit in range(start, end + 1)}
    if (not primary_units.issubset(covered)
            or len(intervals) > max_gets or len(covered) > max_units
            or any(start < 0 or start > end or end >= page_count
                   for start, end in intervals)
            or any(left[1] >= right[0] for left, right in
                   zip(intervals, intervals[1:]))):
        raise AssertionError("V170 plan violates mandatory or resource gate")
    return intervals


def validate_paired_plan(
    planned: dict, primary: list[int], inverse: np.ndarray, control: dict,
) -> None:
    """Recount mandatory coverage and resource caps without planner weights."""
    if (inverse.shape != (ROWS,) or inverse.dtype != np.int64
            or not primary or len(set(primary)) != len(primary)
            or any(type(row) is not int or not 0 <= row < ROWS
                   for row in primary)
            or type(control.get("bytes")) is not int
            or type(control.get("gets")) is not int):
        raise ValueError("V170 independent plan authority differs")
    mandatory = {int(inverse[row]) // UNIT_ROWS for row in primary}
    for prefix in ("pq", "neighbor"):
        ranges = planned.get(prefix + "_ranges")
        amount = planned.get(prefix + "_bytes")
        gets = planned.get(prefix + "_gets")
        if (not isinstance(ranges, list) or not ranges
                or type(amount) is not int or type(gets) is not int
                or gets != len(ranges) or gets > CAP_GETS
                or gets > control["gets"] or amount > CAP_BYTES
                or amount > control["bytes"]
                or any(not isinstance(pair, list) or len(pair) != 2
                       or any(type(value) is not int for value in pair)
                       or not 0 <= pair[0] < pair[1] <= ROWS * ROW_BYTES
                       or pair[0] % UNIT_BYTES or pair[1] % UNIT_BYTES
                       for pair in ranges)
                or any(left[1] >= right[0] for left, right in
                       zip(ranges, ranges[1:]))
                or amount != sum(end - start for start, end in ranges)):
            raise ValueError("V170 independent paired resource recount differs")
        fetched = np.zeros(ROWS // UNIT_ROWS, dtype=bool)
        for start, end in ranges:
            fetched[start // UNIT_BYTES:end // UNIT_BYTES] = True
        if not all(fetched[unit] for unit in mandatory):
            raise ValueError("V170 independent primary coverage differs")


if __name__ == "__main__":
    main()
