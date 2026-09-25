#!/usr/bin/env python3
"""Replay completed V195 returned-ID intersections and sidecar charges."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from scripts.v155_relaion_returned_quality import sha256
from scripts.v166_surrogate_ranking_run import records
from scripts.v195_used_1m_rerank_diagnostic import (
    COUNT, K_VALUES, MAX_COMBINED_GETS, SCHEMA, SIDECAR_UNIT_BYTES,
    TOTAL_BYTES, TOTAL_GETS,
)

HASHES = {
    "terminal": "62a2a7b190983b5e422f679edf8a5120164f9ab886a3e5db5bd32eefc2abdc02",
    "seal": "77267a71e9c9f40cc086af4e44029943b8a09559b59a7a7a998d652ee9026ca7",
    "raw": "998cc015f670e470bbb435d47fc49d0aac5b22d0315a7d51eae326e4e48e18fa",
    "summary": "e4ac85f650a5c8f34f81a77e03c36208d6b952ae7aa732450d272ec493069d20",
}


def check(paths: dict[str, Path]) -> dict:
    for name, digest in HASHES.items():
        if sha256(paths[name]) != digest:
            raise ValueError(f"V195 completed {name} identity differs")
    terminal = json.loads(paths["terminal"].read_text())
    seal = json.loads(paths["seal"].read_text())
    summary = json.loads(paths["summary"].read_text())
    if (terminal.get("schema") != SCHEMA.removesuffix("-v1") + "-spot-v1"
            or terminal.get("status") != "complete"
            or terminal.get("phase") != "complete"
            or terminal.get("exit_code") != 0
            or terminal.get("instance_id") != "i-0dc2f66d74a5c0ba3"
            or terminal.get("source_commit")
                != "9b7bcc581887688247b4fab244a5dab8d37dd3c9"
            or terminal.get("source_archive_sha256")
                != "0f672316d84ef2e2ae9dd5a699f19112ca43cbb245351744b6ad5d7b96e23398"
            or any(terminal["artifacts"][name]["sha256"] != HASHES[role]
                   for role, name in (("seal", "diagnostic-seal.json"),
                                      ("raw", "raw.jsonl"),
                                      ("summary", "summary.json")))
            or seal.get("schema") != SCHEMA + "-seal"
            or seal.get("source_gt_opened") is not False
            or tuple(seal.get("k_values", ())) != K_VALUES
            or seal.get("max_combined_gets") != MAX_COMBINED_GETS
            or summary.get("schema") != SCHEMA + "-summary"
            or summary.get("raw_sha256") != HASHES["raw"]
            or summary.get("seal_sha256") != HASHES["seal"]
            or summary.get("queries") != COUNT):
        raise ValueError("V195 terminal, seal or summary authority differs")
    rows = records(paths["raw"])
    if len(rows) != COUNT:
        raise ValueError("V195 complete query count differs")
    cells = {str(k): {field: 0 for field in (
        "shortlist_gt", "fp16_hits", "float32_hits", "base_hits",
        "base_bytes", "base_gets", "sidecar_bytes", "sidecar_gets",
        "over_32_get_queries")} for k in K_VALUES}
    dynamic = None
    all_gaps: dict[str, list[int]] = {str(k): [] for k in K_VALUES}
    for index, row in enumerate(rows):
        if row["ordinal"] != 2944 + index or set(row["k"]) != set(cells):
            raise ValueError("V195 paired ordinal or K set differs")
        gold = set(row["gold_ids"])
        base = row["base_sq8_returned_ids"]
        if (len(gold) != 100 or len(base) != 100
                or len(set(base)) != 100 or row["source_id"] in gold
                or row["source_id"] in base
                or row["base_hits"] != len(set(base) & gold)
                or not 0 <= row["base_hits"] <= row["coverage"] <= 100):
            raise ValueError("V195 recomputed SQ8 or GT100 identity differs")
        if row["dynamic_floor"]:
            if row["ordinal"] != 3321 or dynamic is not None:
                raise ValueError("V195 dynamic query differs")
            dynamic = row
        for k in K_VALUES:
            key = str(k)
            item = row["k"][key]
            fp16, exact = item["fp16_returned_ids"], item["float32_returned_ids"]
            spans = item["sidecar_intervals"]
            if (len(fp16) != 100 or len(set(fp16)) != 100
                    or len(exact) != 100 or len(set(exact)) != 100
                    or row["source_id"] in fp16 or row["source_id"] in exact
                    or item["fp16_hits"] != len(set(fp16) & gold)
                    or item["float32_hits"] != len(set(exact) & gold)
                    or not item["fp16_hits"] <= item["shortlist_gt"] <= 100
                    or not item["float32_hits"] <= item["shortlist_gt"]
                    or not spans or len(spans) != item["sidecar_gets"]
                    or any(not 0 <= start <= end < 31_250
                           for start, end in spans)
                    or any(left[1] >= right[0]
                           for left, right in zip(spans, spans[1:]))
                    or sum(end - start + 1 for start, end in spans)
                       * SIDECAR_UNIT_BYTES != item["sidecar_bytes"]
                    or row["base_gets"] + item["sidecar_gets"]
                       > MAX_COMBINED_GETS):
                raise ValueError("V195 rerank or sidecar witness differs")
            for left, right in zip(spans, spans[1:]):
                all_gaps[key].append(right[0] - left[1] - 1)
            cell = cells[key]
            for field, value in (("shortlist_gt", item["shortlist_gt"]),
                                 ("fp16_hits", item["fp16_hits"]),
                                 ("float32_hits", item["float32_hits"]),
                                 ("base_hits", row["base_hits"]),
                                 ("base_bytes", row["base_bytes"]),
                                 ("base_gets", row["base_gets"]),
                                 ("sidecar_bytes", item["sidecar_bytes"]),
                                 ("sidecar_gets", item["sidecar_gets"]),
                                 ("over_32_get_queries", int(
                                     row["base_gets"] + item["sidecar_gets"] > 32))):
                cell[field] += value
    if dynamic is None or summary["dynamic_floor"] != {
            "ordinal": 3321, "floor_units": 766,
            "intervals": summary["dynamic_floor"]["intervals"],
            "base_bytes": dynamic["base_bytes"],
            "base_gets": dynamic["base_gets"]}:
        raise ValueError("V195 dynamic floor summary differs")
    floor_intervals = summary["dynamic_floor"]["intervals"]
    if (len(floor_intervals) != 32
            or sum(end - start + 1 for start, end in floor_intervals) != 766
            or dynamic["base_hits"] != 81 or dynamic["coverage"] != 81):
        raise ValueError("V195 dynamic-floor interval or quality differs")
    posthoc = {}
    for k in K_VALUES:
        key = str(k)
        cell = cells[key]
        cell["recovered_feasible_return_losses"] = (
            cell["fp16_hits"] - cell["base_hits"]
            - (dynamic["k"][key]["fp16_hits"] - dynamic["base_hits"]))
        cell["combined_bytes"] = cell["base_bytes"] + cell["sidecar_bytes"]
        cell["combined_gets"] = cell["base_gets"] + cell["sidecar_gets"]
        cell["promising"] = (
            cell["recovered_feasible_return_losses"] >= 200
            and cell["combined_bytes"] <= TOTAL_BYTES
            and cell["combined_gets"] <= TOTAL_GETS)
        missing_gets = max(0, cell["combined_gets"] - TOTAL_GETS)
        extra_bytes = sum(sorted(all_gaps[key])[:missing_gets]) * SIDECAR_UNIT_BYTES
        posthoc[key] = {"minimum_bridge_bytes_to_aggregate_get_cap": extra_bytes,
                        "combined_bytes_after_bridging":
                            cell["combined_bytes"] + extra_bytes}
    if summary["k"] != cells or summary["decision"] != (
            "advance-format-implementation" if any(x["promising"]
                                                   for x in cells.values()) else
            "reject-separate-fp16-sidecar-layout"):
        raise ValueError("V195 aggregate decision differs")
    return {"status": "pass", "queries": COUNT, "k": cells,
            "posthoc_bridge_lower_bound": posthoc,
            "decision": summary["decision"]}


def main() -> None:
    parser = argparse.ArgumentParser()
    for role in HASHES:
        parser.add_argument("--" + role, type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(check(vars(args)), sort_keys=True))


if __name__ == "__main__":
    main()
