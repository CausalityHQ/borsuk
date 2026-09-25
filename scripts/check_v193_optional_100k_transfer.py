#!/usr/bin/env python3
"""Replay the sealed V193 returned-quality reduction from complete artifacts."""

from __future__ import annotations

import argparse
import json
from math import ceil
from pathlib import Path

import numpy as np

from scripts.v158_pq_primary_returned import authenticate, jsonl, load_truth, sha256
from scripts.v160_geometric_relayout_primary import DTYPE, ROW_BYTES
from scripts.v170_pq_field_100k import V163_SQ8_SHA
from scripts.v193_optional_100k_transfer import (
    ARMS, SCHEMA, TRACE_BYTES, UNIT_BYTES, UNIT_COUNT, UNIT_ROWS,
    V170_PLAN_SHA, V170_TERMINAL_SHA, V192_RESULT_SHA,
)
from scripts.v192_optional_rank_fit_diagnostic import FEATURE_SHA, FIT_SHA

HASHES = {
    "terminal": "a176550b370ce1a3d4f5a7b9a265f7a4a91f1165c33a9aa4fe5fbb7bea980676",
    "plans": "04c412566b38fdb2461069a34e48d92214c983d78ae39051579cec09588964c5",
    "seal": "f5f11651c50c24e8bbdd399053f39146fb49dc9f180bca64f7a778eadeb88cfb",
    "raw": "80e1ef702a46ec30b5ac6a2d5fe42ed8643a59719b28284b67eaa06fa64bca19",
    "summary": "cd18612b3675409ba0edf29dce0f47b4e93864aef5230c6092813301d1d5a28b",
    "v170_plans": V170_PLAN_SHA,
    "v170_raw": "d8081a9feaec74d750393b675819985941a15f409890d788e00ad9b38e3b3894",
}


def _p05(values: list[int]) -> int:
    return sorted(values)[ceil(len(values) * .05) - 1]


def check(paths: dict[str, Path]) -> dict:
    for role, expected in HASHES.items():
        if sha256(paths[role]) != expected:
            raise ValueError(f"V193 {role} completed artifact differs")
    terminal = json.loads(paths["terminal"].read_text())
    if (terminal.get("schema") != "borsuk-v193-optional-100k-transfer-spot-v1"
            or terminal.get("status") != "complete"
            or terminal.get("phase") != "complete"
            or terminal.get("exit_code") != 0
            or terminal.get("source_commit")
                != "cdbe66a269a2cd580dd46940e1838ef9a915ceb3"
            or terminal.get("source_archive_sha256")
                != "5ebe13163b57204f78a4534c7bdc20f63371940356d0c6f16204ca6bbc43d06c"
            or terminal.get("instance_id") != "i-0165163efae8d71e6"
            or any(terminal.get("artifacts", {}).get(filename, {}).get("sha256")
                   != HASHES[role] for role, filename in (
                       ("plans", "plans.jsonl"), ("seal", "plan-seal.json"),
                       ("raw", "raw.jsonl"), ("summary", "summary.json")))):
        raise ValueError("V193 completed terminal differs")
    authenticate(paths["truth"], "truth")
    if paths["sq8"].stat().st_size != 100_000 * ROW_BYTES or sha256(paths["sq8"]) != V163_SQ8_SHA:
        raise ValueError("V193 V163 SQ8 artifact differs")
    seal = json.loads(paths["seal"].read_text())
    if seal != {
        "schema": SCHEMA + "-plan-seal", "gt_opened": False,
        "queries": 1000, "requests_sha256":
            "b2485629b919614bf46877a779b16d678cd1690d1872b7d4f9c9cbe6ddd94eb0",
        "reference_sha256":
            "fa42050d6610630576f3f00232aecb8c43e6a0af350bf0ac90094aaba00c9b7b",
        "v170_terminal_sha256": V170_TERMINAL_SHA,
        "v170_plans_sha256": V170_PLAN_SHA,
        "v192_result_sha256": V192_RESULT_SHA,
        "v189_features_sha256": FEATURE_SHA,
        "v189_fit_labels_sha256": FIT_SHA,
        "candidate_radius": 32, "metric": "cosine",
        "unit_bytes": UNIT_BYTES, "max_trace_bytes": TRACE_BYTES,
        "arms": list(ARMS), "plans_sha256": HASHES["plans"],
    }:
        raise ValueError("V193 GT-blind plan seal differs")
    summary = json.loads(paths["summary"].read_text())
    if (summary.get("schema") != SCHEMA + "-summary"
            or summary.get("raw_sha256") != HASHES["raw"]
            or summary.get("plan_seal_sha256") != HASHES["seal"]
            or summary.get("dataset") != "ReLAION-100k D768"
            or summary.get("split") != "development-1000-reused"
            or summary.get("queries") != 1000):
        raise ValueError("V193 summary authority differs")
    sq8 = np.memmap(paths["sq8"], dtype=DTYPE, mode="r", shape=(100_000,))
    positions = {int(identifier): index for index, identifier in enumerate(sq8["id"])}
    if len(positions) != 100_000:
        raise ValueError("V193 SQ8 stable IDs differ")
    truth = load_truth(paths["truth"])
    values = {name: {field: [] for field in ("hits", "coverage", "bytes", "gets")}
              for name in (*ARMS, "v170_control")}
    candidate_sum = 0
    checked = 0
    for ordinal, (plan, raw, control_plan, control_raw) in enumerate(zip(
            jsonl(paths["plans"]), jsonl(paths["raw"]),
            jsonl(paths["v170_plans"]), jsonl(paths["v170_raw"]),
            strict=True)):
        if any(row["query_ordinal"] != ordinal for row in
               (plan, raw, control_plan, control_raw)):
            raise ValueError("V193 paired query ordinal differs")
        gold = set(map(int, truth[ordinal]))
        if len(gold) != 100 or not gold.issubset(positions):
            raise ValueError("V193 GT100 identity differs")
        ranked = plan["ranked_units"]
        mandatory = plan["mandatory_units"]
        if (not isinstance(ranked, list) or len(ranked) != len(set(ranked))
                or not set(mandatory).issubset(ranked)
                or len(mandatory) != len(set(mandatory))
                or any(type(unit) is not int or not 0 <= unit < UNIT_COUNT
                       for unit in ranked + mandatory)
                or plan["candidate_units"] != len(ranked)
                or set(plan["arms"]) != set(ARMS)
                or set(raw["arms"]) != set(ARMS)):
            raise ValueError("V193 candidate or arm geometry differs")
        ceiling = sum(positions[identifier] // UNIT_ROWS in set(ranked)
                      for identifier in gold)
        candidate_sum += ceiling
        if raw["candidate_ceiling_hits"] != ceiling:
            raise ValueError("V193 candidate ceiling differs")
        cap_bytes, cap_gets = control_plan["pq_bytes"], control_plan["pq_gets"]
        if (plan["paired_v170_bytes"] != cap_bytes
                or plan["paired_v170_gets"] != cap_gets
                or raw["control_bytes"] != cap_bytes
                or raw["control_gets"] != cap_gets
                or raw["control_hits"] != control_raw["pq_hits100"]
                or raw["control_coverage"] != control_raw["pq_coverage"]
                or control_raw["pq_bytes"] != cap_bytes
                or control_raw["pq_gets"] != cap_gets):
            raise ValueError("V193 paired V170 control differs")
        control_ids = control_raw["pq_returned_ids"]
        if len(control_ids) != 100 or len(set(control_ids)) != 100:
            raise ValueError("V170 returned IDs differ")
        if len(set(control_ids) & gold) != raw["control_hits"]:
            raise ValueError("V170 returned hit count differs")
        for field, value in (("hits", raw["control_hits"]),
                             ("coverage", raw["control_coverage"]),
                             ("bytes", cap_bytes), ("gets", cap_gets)):
            values["v170_control"][field].append(value)
        for name in ARMS:
            arm, observed = plan["arms"][name], raw["arms"][name]
            if not arm["feasible"] or not observed["feasible"]:
                raise ValueError("V193 infeasible arm")
            intervals = [tuple(pair) for pair in arm["intervals"]]
            ranges = [tuple(pair) for pair in arm["ranges"]]
            if (not intervals or len(intervals) != len(ranges)
                    or any(not 0 <= start <= end < UNIT_COUNT
                           for start, end in intervals)
                    or any(left[1] >= right[0]
                           for left, right in zip(intervals, intervals[1:]))
                    or any((start * UNIT_BYTES, (end + 1) * UNIT_BYTES) != span
                           for (start, end), span in zip(intervals, ranges))
                    or any(not any(start <= unit <= end for start, end in intervals)
                           for unit in mandatory)):
                raise ValueError("V193 interval witness differs")
            units = sum(end - start + 1 for start, end in intervals)
            byte_count = units * UNIT_BYTES
            gets = len(intervals)
            if (units != arm["units"] or byte_count != arm["bytes"]
                    or gets != arm["gets"] or byte_count > cap_bytes
                    or gets > cap_gets or units > 672 or gets > 32
                    or byte_count != observed["bytes"]
                    or gets != observed["gets"]):
                raise ValueError("V193 paired hard cap differs")
            coverage = sum(any(start <= positions[identifier] * 780 < end
                               for start, end in ranges) for identifier in gold)
            returned = observed["returned_ids"]
            if (len(returned) != 100 or len(set(returned)) != 100
                    or not set(returned).issubset(positions)
                    or any(not any(start <= positions[identifier] * 780 < end
                                   for start, end in ranges)
                           for identifier in returned)):
                raise ValueError("V193 returned row witness differs")
            hits = len(set(returned) & gold)
            if hits != observed["hits"] or coverage != observed["coverage"]:
                raise ValueError("V193 returned quality differs")
            for field, value in (("hits", hits), ("coverage", coverage),
                                 ("bytes", byte_count), ("gets", gets)):
                values[name][field].append(value)
            checked += 1
    if ordinal != 999 or checked != 3000:
        raise ValueError("V193 completed query count differs")
    result = {}
    for name, fields in values.items():
        result[name] = {"hits": sum(fields["hits"]),
                        "p05_hits": _p05(fields["hits"]),
                        "coverage": sum(fields["coverage"]),
                        "bytes": sum(fields["bytes"]),
                        "gets": sum(fields["gets"])}
        if name in ARMS:
            result[name]["infeasible"] = 0
    if (summary["candidate_ceiling_hits"] != candidate_sum
            or summary["arms"] != {name: result[name] for name in ARMS}
            or summary["v170_control"] != result["v170_control"]):
        raise ValueError("V193 aggregate summary differs")
    paired = {}
    for name in ("v170_control", "full_rank"):
        delta = [left - right for left, right in zip(
            values["optional_risk"]["hits"], values[name]["hits"], strict=True)]
        paired[name] = {"wins": sum(x > 0 for x in delta),
                        "ties": sum(x == 0 for x in delta),
                        "losses": sum(x < 0 for x in delta),
                        "net_hits": sum(delta)}
    if summary["paired_optional_risk"] != paired:
        raise ValueError("V193 paired comparison differs")
    optional, control = result["optional_risk"], result["v170_control"]
    passed = (optional["hits"] >= 99_357 and optional["p05_hits"] >= 98
              and optional["coverage"] >= 99_747
              and optional["bytes"] <= control["bytes"]
              and optional["gets"] <= control["gets"])
    if summary["decision"] != ("advance-to-fresh-1m" if passed else
                               "revise-feature-or-allocation"):
        raise ValueError("V193 preregistered decision differs")
    return {"status": "pass", "queries": 1000, "arm_queries": checked,
            "candidate_ceiling_hits": candidate_sum, "decision": summary["decision"],
            "arms": result, "paired_optional_risk": paired}


def main() -> None:
    parser = argparse.ArgumentParser()
    for role in (*HASHES, "truth", "sq8"):
        parser.add_argument("--" + role.replace("_", "-"), required=True,
                            type=Path)
    args = parser.parse_args()
    print(json.dumps(check(vars(args)), sort_keys=True))


if __name__ == "__main__":
    main()
