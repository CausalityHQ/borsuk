#!/usr/bin/env python3
"""Used-panel test of FP16 shortlist rerank and dynamic mandatory floor."""

from __future__ import annotations

import argparse
import json
from math import ceil
from pathlib import Path

import numpy as np

from scripts.check_v194_fresh_1m_optional_source import HASHES as V194_HASHES, check
from scripts.hard_priced_interval import hard_priced_cover
from scripts.v114_1m_paired import score_sq8_ranges
from scripts.v155_relaion_returned_quality import ROWS, sha256
from scripts.v166_surrogate_ranking_run import canonical, records
from scripts.v177_source_candidate_ceiling import _inputs
from scripts.v182_wide_pq_rank import _normalized, _truth
from scripts.v189_predicted_interval_source import validate_intervals
from scripts.v193_optional_100k_transfer import _models
from scripts.v194_fresh_1m_optional_source import (
    COUNT, FIRST, MAX_GETS, MAX_TRACE_BYTES, MAX_UNITS, PRICES,
    UNIT_BYTES, UNIT_COUNT,
)

SCHEMA = "borsuk-v195-used-1m-rerank-diagnostic-v1"
K_VALUES = (128, 160, 200, 256)
SIDECAR_UNIT_BYTES = 32 * 768 * 2
MAX_COMBINED_GETS = 64
TOTAL_BYTES, TOTAL_GETS = 5_700_611_604, 11_328


def _args_for_v194(args: argparse.Namespace) -> dict[str, Path]:
    return {name: getattr(args, "v194_" + name) for name in V194_HASHES}


def _floor(mandatory: list[int], max_gets: int) -> int:
    gaps = sorted(right - left - 1 for left, right in
                  zip(mandatory, mandatory[1:]) if right > left + 1)
    return len(mandatory) + sum(gaps[:max(0, len(gaps) + 1 - max_gets)])


def _sidecar_cover(units: list[int], max_gets: int) -> tuple[list[list[int]], int]:
    """Minimum whole-unit interval cover for the fixed shortlisted units."""
    chosen = sorted(set(units))
    if not chosen or max_gets <= 0:
        raise ValueError("V195 empty sidecar cover or GET allowance")
    gaps = [(right - left - 1, index) for index, (left, right)
            in enumerate(zip(chosen, chosen[1:])) if right > left + 1]
    bridge_count = max(0, len(gaps) + 1 - max_gets)
    bridged = {index for _, index in sorted(gaps)[:bridge_count]}
    intervals = []
    start = chosen[0]
    for index, (left, right) in enumerate(zip(chosen, chosen[1:])):
        if right > left + 1 and index not in bridged:
            intervals.append([start, left])
            start = right
    intervals.append([start, chosen[-1]])
    count = sum(end - start + 1 for start, end in intervals)
    if (len(intervals) > max_gets or count != _floor(chosen, max_gets)
            or any(not any(start <= unit <= end for start, end in intervals)
                   for unit in chosen)):
        raise AssertionError("V195 sidecar cover witness differs")
    return intervals, count


def _rerank(ids: list[int], positions: dict[int, int],
            vectors: np.ndarray, source: np.ndarray,
            query_row: int, *, fp16: bool) -> list[int]:
    source_rows = np.asarray([positions[identifier] for identifier in ids],
                             dtype=np.int64)
    if fp16:
        payload = vectors[source_rows].astype(np.float16).astype(np.float64)
        norms = np.linalg.norm(payload, axis=1)
        if not np.isfinite(norms).all() or (norms <= 0).any():
            raise ValueError("V195 FP16 payload norm differs")
        scores = (payload @ source[query_row]) / norms
    else:
        scores = source[source_rows] @ source[query_row]
    stable = np.asarray(ids, dtype=np.int64)
    order = np.lexsort((stable, -scores))[:100]
    return [int(stable[index]) for index in order]


def seal(args: argparse.Namespace) -> None:
    replay = check(_args_for_v194(args))
    if replay["decision"] != "revise-representation-allocation-or-serving":
        raise ValueError("V195 closed V194 decision differs")
    args.seal.write_text(canonical({
        "schema": SCHEMA + "-seal", "source_gt_opened": False,
        "v194_sha256": V194_HASHES,
        "source_sha256":
            "2796b579f37afe99ca4aff57e282335a6a79ad30596645957d26326a0560cf86",
        "old_sq8_sha256":
            "2284f24745f964ff2b125eedb883d5cd8ff6afab0738f49f9e16e593167a318b",
        "v164_order_sha256":
            "5b5ef48d86570e5ca68fdaaac9aef231ec7368dd526baef00474cd0a2f59a06f",
        "k_values": K_VALUES, "sidecar_unit_bytes": SIDECAR_UNIT_BYTES,
        "max_combined_gets": MAX_COMBINED_GETS,
        "aggregate_caps": [TOTAL_BYTES, TOTAL_GETS],
        "dynamic_floor_ordinal": 3321,
    }))


def run(args: argparse.Namespace) -> None:
    if sha256(args.seal) != args.seal_sha256:
        raise ValueError("V195 external S3 diagnostic seal differs")
    authority = json.loads(args.seal.read_text())
    if (authority.get("schema") != SCHEMA + "-seal"
            or authority.get("source_gt_opened") is not False
            or authority.get("v194_sha256") != V194_HASHES
            or tuple(authority.get("k_values", ())) != K_VALUES
            or authority.get("max_combined_gets") != MAX_COMBINED_GETS
            or authority.get("aggregate_caps") != [TOTAL_BYTES, TOTAL_GETS]):
        raise ValueError("V195 sealed choices differ")
    check(_args_for_v194(args))
    features = records(args.v194_features)
    plans = records(args.v194_plans)
    raw = records(args.v194_raw)
    if any(len(rows) != COUNT for rows in (features, plans, raw)):
        raise ValueError("V195 V194 query count differs")
    old, inverse_new, source_ids, vectors, old_sq8, planes = _inputs(args)
    inverse_old = np.empty(ROWS, dtype=np.int64)
    inverse_old[old] = np.arange(ROWS, dtype=np.int64)
    new_order = np.argsort(inverse_new).astype(np.int64)
    new_sq8 = np.memmap(args.new_sq8, dtype=old_sq8.dtype,
                        mode="w+", shape=(ROWS,))
    for start in range(0, ROWS, 8192):
        stop = min(start + 8192, ROWS)
        new_sq8[start:stop] = old_sq8[inverse_old[new_order[start:stop]]]
    new_sq8.flush()
    if not np.array_equal(new_sq8["id"], source_ids[new_order]):
        raise ValueError("V195 relaid SQ8 stable IDs differ")
    positions = {int(identifier): index for index, identifier in
                 enumerate(source_ids)}
    if len(positions) != ROWS:
        raise ValueError("V195 source stable IDs differ")
    source = _normalized(vectors)
    low = np.asarray(planes["low"], dtype=np.float32)
    step = np.asarray(planes["step"], dtype=np.float32)
    optional, _, _ = _models(args)
    stats = {str(k): {field: 0 for field in
             ("shortlist_gt", "fp16_hits", "float32_hits", "base_hits",
              "base_bytes", "base_gets", "sidecar_bytes", "sidecar_gets",
              "over_32_get_queries")} for k in K_VALUES}
    dynamic_floor = None
    with args.raw.open("x") as output:
        for index, (feature, plan, original) in enumerate(zip(
                features, plans, raw, strict=True)):
            ordinal, stable_id = FIRST + index, feature["source_id"]
            if any(row["ordinal"] != ordinal or row["source_id"] != stable_id
                   for row in (feature, plan, original)):
                raise ValueError("V195 paired source query differs")
            source_row = feature["source_row"]
            truth_rows = _truth(source, source_ids, source_row)
            gold = set(map(int, source_ids[truth_rows]))
            masses = {int(unit): int(hits) for unit, hits in
                      original["truth_by_unit"]}
            if (len(gold) != 100 or masses != dict(sorted(
                    (int(unit), int(count)) for unit, count in
                    zip(*np.unique(inverse_new[truth_rows] // 32,
                                   return_counts=True))))):
                raise ValueError("V195 recomputed source GT100 differs")
            arm = plan["arms"]["optional_risk"]
            if ordinal == 3321:
                floor = _floor(feature["mandatory_units"], MAX_GETS)
                if floor != 766 or arm["feasible"]:
                    raise ValueError("V195 dynamic-floor identity differs")
                weights = optional.weights(
                    tuple(feature["ranked_units"]),
                    tuple(feature["mandatory_units"]), units_per_hit=1_000_000)
                cover = hard_priced_cover(
                    weights, tuple(feature["mandatory_units"]),
                    page_count=UNIT_COUNT, max_gets=MAX_GETS,
                    max_units=max(MAX_UNITS, floor),
                    unit_price=PRICES["optional_risk"][0],
                    get_price=PRICES["optional_risk"][1],
                    max_trace_bytes=MAX_TRACE_BYTES)
                arm = {"feasible": True,
                       "intervals": [list(pair) for pair in cover.intervals],
                       "units": cover.units, "bytes": cover.units * UNIT_BYTES,
                       "gets": cover.gets}
                dynamic_floor = {"ordinal": ordinal, "floor_units": floor,
                                 "intervals": arm["intervals"],
                                 "base_bytes": arm["bytes"],
                                 "base_gets": arm["gets"]}
            elif not arm["feasible"]:
                raise ValueError("V195 unexpected second infeasible query")
            intervals = tuple(tuple(pair) for pair in arm["intervals"])
            units, gets, bytes_read = validate_intervals(
                intervals, tuple(feature["mandatory_units"]),
                page_count=UNIT_COUNT, max_units=max(MAX_UNITS, 766)
                if ordinal == 3321 else MAX_UNITS, max_gets=MAX_GETS)
            if (units, gets, bytes_read) != (
                    arm["units"], arm["gets"], arm["bytes"]):
                raise ValueError("V195 physical plan witness differs")
            ranges = [[start * UNIT_BYTES, (end + 1) * UNIT_BYTES]
                      for start, end in intervals]
            query = np.asarray(vectors[source_row], dtype=np.float32)
            sq8 = score_sq8_ranges(new_sq8, query, low, step,
                                   ranges, top_k=max(K_VALUES) + 1)
            sq8 = [identifier for identifier in sq8
                   if identifier != stable_id][:max(K_VALUES)]
            if len(sq8) != max(K_VALUES) or len(set(sq8)) != len(sq8):
                raise ValueError("V195 SQ8 top-K identity differs")
            base_ids = sq8[:100]
            base_hits = len(set(base_ids) & gold)
            coverage = sum(count for unit, count in masses.items()
                           if any(start <= unit <= end
                                  for start, end in intervals))
            if ordinal != 3321 and (
                    base_ids != original["arms"]["optional_risk"]["returned_ids"]
                    or base_hits != original["arms"]["optional_risk"]["hits"]
                    or coverage != original["arms"]["optional_risk"]["coverage"]):
                raise ValueError("V195 independent V194 SQ8 replay differs")
            row = {"ordinal": ordinal, "source_id": stable_id,
                   "gold_ids": sorted(gold), "base_sq8_returned_ids": base_ids,
                   "base_hits": base_hits, "coverage": coverage,
                   "base_bytes": bytes_read, "base_gets": gets,
                   "dynamic_floor": ordinal == 3321, "k": {}}
            for k in K_VALUES:
                shortlist = sq8[:k]
                shortlist_gt = len(set(shortlist) & gold)
                fp16_ids = _rerank(shortlist, positions, vectors, source,
                                   source_row, fp16=True)
                exact_ids = _rerank(shortlist, positions, vectors, source,
                                    source_row, fp16=False)
                fp16_hits = len(set(fp16_ids) & gold)
                exact_hits = len(set(exact_ids) & gold)
                sidecar_units = [int(inverse_new[positions[identifier]] // 32)
                                 for identifier in shortlist]
                sidecar_intervals, sidecar_count = _sidecar_cover(
                    sidecar_units, MAX_COMBINED_GETS - gets)
                sidecar_bytes = sidecar_count * SIDECAR_UNIT_BYTES
                sidecar_gets = len(sidecar_intervals)
                if fp16_hits > shortlist_gt or exact_hits > shortlist_gt:
                    raise AssertionError("V195 rerank exceeds shortlist truth")
                row["k"][str(k)] = {
                    "shortlist_gt": shortlist_gt,
                    "fp16_returned_ids": fp16_ids,
                    "fp16_hits": fp16_hits,
                    "float32_returned_ids": exact_ids,
                    "float32_hits": exact_hits,
                    "sidecar_intervals": sidecar_intervals,
                    "sidecar_bytes": sidecar_bytes,
                    "sidecar_gets": sidecar_gets,
                }
                for field, value in (("shortlist_gt", shortlist_gt),
                                     ("fp16_hits", fp16_hits),
                                     ("float32_hits", exact_hits),
                                     ("base_hits", base_hits),
                                     ("base_bytes", bytes_read),
                                     ("base_gets", gets),
                                     ("sidecar_bytes", sidecar_bytes),
                                     ("sidecar_gets", sidecar_gets),
                                     ("over_32_get_queries",
                                      int(gets + sidecar_gets > 32))):
                    stats[str(k)][field] += value
            output.write(canonical(row))
    if dynamic_floor is None:
        raise ValueError("V195 dynamic-floor query absent")
    for k in K_VALUES:
        cell = stats[str(k)]
        cell["recovered_feasible_return_losses"] = (
            cell["fp16_hits"] - cell["base_hits"]
            - (next(row["k"][str(k)]["fp16_hits"] - row["base_hits"]
                    for row in records(args.raw) if row["ordinal"] == 3321)))
        cell["combined_bytes"] = cell["base_bytes"] + cell["sidecar_bytes"]
        cell["combined_gets"] = cell["base_gets"] + cell["sidecar_gets"]
        cell["promising"] = (
            cell["recovered_feasible_return_losses"] >= 200
            and cell["combined_bytes"] <= TOTAL_BYTES
            and cell["combined_gets"] <= TOTAL_GETS)
    args.summary.write_text(canonical({
        "schema": SCHEMA + "-summary", "dataset": "ReLAION-1M D768",
        "split": "used-V194-source-pseudoquery-hash-ranks-2945-3456",
        "queries": COUNT, "k": stats,
        "dynamic_floor": dynamic_floor,
        "decision": "advance-format-implementation" if any(
            row["promising"] for row in stats.values()) else
            "reject-separate-fp16-sidecar-layout",
        "sidecar_storage_bytes": ROWS * 768 * 2,
        "raw_sha256": sha256(args.raw),
        "seal_sha256": args.seal_sha256,
    }))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("phase", choices=("seal", "run"))
    for name in ("source", "old_layout", "old_sq8", "router", "order",
                 "v164_terminal", "v192_result", "v189_features",
                 "v189_fit_labels", "seal", "new_sq8", "raw", "summary"):
        parser.add_argument("--" + name.replace("_", "-"), type=Path,
                            required=True)
    for name in V194_HASHES:
        parser.add_argument("--v194-" + name.replace("_", "-"),
                            type=Path, required=True)
    parser.add_argument("--seal-sha256", default="")
    args = parser.parse_args()
    if args.phase == "seal":
        seal(args)
    else:
        run(args)


if __name__ == "__main__":
    main()
