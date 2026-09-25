#!/usr/bin/env python3
"""Completed-artifact replay of V194 geometry, coverage and gate arithmetic."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from scripts.v155_relaion_returned_quality import sha256
from scripts.v166_surrogate_ranking_run import records
from scripts.v194_fresh_1m_optional_source import (
    ARMS, COUNT, FIRST, MAX_GETS, MAX_UNITS, SCHEMA, UNIT_BYTES, UNIT_COUNT,
    _summary, decide,
)

HASHES = {
    "terminal": "4da29740e45057192646a36a045c412e90598e05df9ad95213241ea7350e9767",
    "features": "415dbb3a20e3ca3a0c77f327b0b457a049156fe0a9810199ee1b0da0c4de5b64",
    "prepare_seal": "d9be646c330ed16aa1dfa78714a7fe4accc59dd966a327deae0ebe19c3ed5494",
    "plans": "ee5cb9a7eee6a05b753db13f52b9903349ff8d750312702e98b736a39bd5562d",
    "plan_seal": "68a5dfc15e320d05ddae95f2fd135dce60f5fed2ae17c649063ea3f06d9dbdcf",
    "raw": "121104dc11819eeaaae810f70d96979dbe74f4bca65e3cd2a29c2557757f144c",
    "summary": "fc081258d0563deb8f18b926d8948fee73759f45d727543663eed9108434fb01",
}


def _mandatory_floor(mandatory: list[int]) -> int:
    gaps = sorted(right - left - 1 for left, right in
                  zip(mandatory, mandatory[1:]) if right > left + 1)
    bridges = max(0, len(gaps) + 1 - MAX_GETS)
    return len(mandatory) + sum(gaps[:bridges])


def check(paths: dict[str, Path]) -> dict:
    for name, expected in HASHES.items():
        if sha256(paths[name]) != expected:
            raise ValueError(f"V194 completed {name} digest differs")
    terminal = json.loads(paths["terminal"].read_text())
    if (terminal.get("schema") != SCHEMA.removesuffix("-v1") + "-spot-v1"
            or terminal.get("status") != "complete"
            or terminal.get("phase") != "complete"
            or terminal.get("exit_code") != 0
            or terminal.get("source_commit")
                != "72bf198eadfbaa0e438cc0871b9931900319cb2e"
            or terminal.get("source_archive_sha256")
                != "eafb948160eea0f5508dcf3feb290cf245a180a6c47707818ae7ef598d474a44"
            or terminal.get("instance_id") != "i-055964b24c4e0fa59"):
        raise ValueError("V194 complete terminal authority differs")
    for role, name in (("features", "features.jsonl"),
                       ("prepare_seal", "prepare-seal.json"),
                       ("plans", "plans.jsonl"),
                       ("plan_seal", "plan-seal.json"),
                       ("raw", "raw.jsonl"),
                       ("summary", "summary.json")):
        if terminal["artifacts"]["out/" + name]["sha256"] != HASHES[role]:
            raise ValueError("V194 terminal artifact identity differs")
    prepare = json.loads(paths["prepare_seal"].read_text())
    seal = json.loads(paths["plan_seal"].read_text())
    summary = json.loads(paths["summary"].read_text())
    if (prepare.get("schema") != SCHEMA + "-prepare-seal"
            or prepare.get("source_truth_opened") is not False
            or prepare.get("features_sha256") != HASHES["features"]
            or seal.get("schema") != SCHEMA + "-plan-seal"
            or seal.get("source_truth_opened") is not False
            or seal.get("prepare_seal_sha256") != HASHES["prepare_seal"]
            or seal.get("features_sha256") != HASHES["features"]
            or seal.get("plans_sha256") != HASHES["plans"]
            or tuple(seal.get("arms", ())) != ARMS
            or seal.get("per_query_caps") != [MAX_GETS, MAX_UNITS]
            or summary.get("schema") != SCHEMA + "-summary"
            or summary.get("plan_seal_sha256") != HASHES["plan_seal"]
            or summary.get("raw_sha256") != HASHES["raw"]
            or summary.get("queries") != COUNT
            or summary.get("split") != "source-pseudoquery-hash-ranks-2945-3456"):
        raise ValueError("V194 GT-blind seal or summary differs")
    features, plans, raw = (records(paths[name]) for name in
                            ("features", "plans", "raw"))
    if any(len(rows) != COUNT for rows in (features, plans, raw)):
        raise ValueError("V194 completed query count differs")
    if [row["source_id"] for row in features] != prepare["pseudo_ids"]:
        raise ValueError("V194 frozen query identities differ")
    values = {name: {field: [] for field in ("hits", "coverage", "bytes", "gets")}
              for name in ARMS}
    infeasible = {name: 0 for name in ARMS}
    candidate_sum = 0
    for index, (feature, planned, observed) in enumerate(zip(
            features, plans, raw, strict=True)):
        ordinal, stable_id = FIRST + index, feature["source_id"]
        if (any(row["ordinal"] != ordinal or row["source_id"] != stable_id
                for row in (feature, planned, observed))
                or set(planned["arms"]) != set(ARMS)
                or set(observed["arms"]) != set(ARMS)):
            raise ValueError("V194 paired row differs")
        ranked, mandatory = feature["ranked_units"], feature["mandatory_units"]
        if (not ranked or len(ranked) != len(set(ranked))
                or mandatory != sorted(set(mandatory))
                or not set(mandatory).issubset(ranked)
                or any(type(unit) is not int or not 0 <= unit < UNIT_COUNT
                       for unit in ranked)):
            raise ValueError("V194 candidate unit geometry differs")
        truth = {int(unit): int(hits)
                 for unit, hits in observed["truth_by_unit"]}
        if sum(truth.values()) != 100 or any(
                not 0 <= unit < UNIT_COUNT or mass <= 0
                for unit, mass in truth.items()):
            raise ValueError("V194 GT100 unit mass differs")
        ceiling = sum(truth.get(unit, 0) for unit in ranked)
        candidate_sum += ceiling
        if observed["candidate_ceiling_hits"] != ceiling:
            raise ValueError("V194 candidate ceiling differs")
        floor = _mandatory_floor(mandatory)
        if planned["arms"]["greedy_control"]["mandatory_floor_units"] != floor:
            raise ValueError("V194 mandatory floor differs")
        for name in ARMS:
            arm, output = planned["arms"][name], observed["arms"][name]
            if not arm["feasible"]:
                if output["feasible"] is not False or floor <= MAX_UNITS:
                    raise ValueError("V194 infeasible witness differs")
                infeasible[name] += 1
                for field in values[name]:
                    values[name][field].append(0)
                continue
            intervals = [tuple(pair) for pair in arm["intervals"]]
            if (not intervals or output["feasible"] is not True
                    or any(not 0 <= start <= end < UNIT_COUNT
                           for start, end in intervals)
                    or any(left[1] >= right[0]
                           for left, right in zip(intervals, intervals[1:]))
                    or any(not any(start <= unit <= end
                                   for start, end in intervals)
                           for unit in mandatory)):
                raise ValueError("V194 mandatory interval witness differs")
            units = sum(end - start + 1 for start, end in intervals)
            gets, bytes_read = len(intervals), units * UNIT_BYTES
            if (units > MAX_UNITS or gets > MAX_GETS
                    or (units, gets, bytes_read) !=
                       (arm["units"], arm["gets"], arm["bytes"])
                    or (gets, bytes_read) !=
                       (output["gets"], output["bytes"])):
                raise ValueError("V194 hard physical resource differs")
            coverage = sum(mass for unit, mass in truth.items()
                           if any(start <= unit <= end
                                  for start, end in intervals))
            returned = output["returned_ids"]
            if (not isinstance(returned, list) or len(returned) != 100
                    or len(set(returned)) != 100 or stable_id in returned
                    or not 0 <= output["hits"] <= coverage
                    or output["coverage"] != coverage):
                raise ValueError("V194 returned witness or coverage differs")
            for field, value in (("hits", output["hits"]),
                                 ("coverage", coverage),
                                 ("bytes", bytes_read), ("gets", gets)):
                values[name][field].append(value)
    cells = {name: _summary(values[name], infeasible[name]) for name in ARMS}
    if (summary["arms"] != cells
            or summary["candidate_ceiling_hits"] != candidate_sum
            or summary["decision"] != decide(cells)):
        raise ValueError("V194 aggregate result or decision differs")
    paired = {}
    for name in ("full_rank", "constant_risk", "greedy_control"):
        differences = [left - right for left, right in zip(
            values["optional_risk"]["hits"], values[name]["hits"], strict=True)]
        paired[name] = {"wins": sum(x > 0 for x in differences),
                        "ties": sum(x == 0 for x in differences),
                        "losses": sum(x < 0 for x in differences),
                        "net_hits": sum(differences)}
    if summary["paired_optional_risk"] != paired:
        raise ValueError("V194 paired query counts differ")
    return {"status": "pass", "queries": COUNT, "arm_queries": COUNT * len(ARMS),
            "candidate_ceiling_hits": candidate_sum,
            "arms": cells, "decision": decide(cells),
            "limitation": "returned hit intersections require source GT100 IDs for independent replay"}


def main() -> None:
    parser = argparse.ArgumentParser()
    for role in HASHES:
        parser.add_argument("--" + role.replace("_", "-"), type=Path,
                            required=True)
    args = parser.parse_args()
    print(json.dumps(check(vars(args)), sort_keys=True))


if __name__ == "__main__":
    main()
