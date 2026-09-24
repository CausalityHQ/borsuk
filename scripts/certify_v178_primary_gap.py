#!/usr/bin/env python3
"""Independently certify the closed V178 primary-cover byte floor."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

ROSTERS_SHA = "6be71fbd2282f79e9ed97809c5f04f50ebb077e0574d5a1c919951261962482c"
PLANS_SHA = "dd93bf3ad79856970987be514435f2ebffeac93f4cd0de81d1e5d552d54f8cc6"
SUMMARY_SHA = "e88756971165a29e0a9cff3051366e6e051e1b0d74991ceee9d19087c3926ee6"
TERMINAL_SHA = "bf415a5a80956225dd2e01a5324b1cc61d611031fc072e952fda4878a6b70ee7"
UNIT_BYTES = 24_960
CAP_UNITS = 672
CAP_GETS = 32
CAP_BYTES = 16_777_216


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def minimum_primary_cover(units: list[int], max_gets: int) -> dict[str, int]:
    if (not units or units != sorted(set(units)) or max_gets <= 0
            or any(type(unit) is not int or unit < 0 for unit in units)):
        raise ValueError("V178 primary units differ")
    gaps = [right - left - 1 for left, right in zip(units, units[1:])
            if right > left + 1]
    runs = len(gaps) + 1
    bridged = sum(sorted(gaps)[:max(0, runs - max_gets)])
    minimum = len(units) + bridged
    return {"primary_units": len(units), "runs": runs,
            "bridged_units_floor": bridged,
            "minimum_units": minimum,
            "minimum_bytes": minimum * UNIT_BYTES}


def certify(rosters_path: Path, plans_path: Path,
            summary_path: Path, terminal_path: Path) -> dict:
    for path, expected in ((rosters_path, ROSTERS_SHA), (plans_path, PLANS_SHA),
                           (summary_path, SUMMARY_SHA), (terminal_path, TERMINAL_SHA)):
        if sha256(path) != expected:
            raise ValueError(f"closed artifact hash differs: {path.name}")
    summary = json.loads(summary_path.read_text())
    terminal = json.loads(terminal_path.read_text())
    if (summary.get("decision") != "primary-layout-killed"
            or summary.get("plans_sha256") != PLANS_SHA
            or terminal.get("status") != "complete"
            or terminal.get("source_commit")
                != "910229f936368bf409ea08215790c234ecef9eb9"
            or terminal.get("artifacts", {}).get("out/oracle-plans.jsonl", {}).get("sha256")
                != PLANS_SHA):
        raise ValueError("V178 terminal/summary binding differs")
    rosters = [json.loads(line) for line in rosters_path.read_text().splitlines()]
    plans = [json.loads(line) for line in plans_path.read_text().splitlines()]
    if len(rosters) != 128 or len(plans) != 128:
        raise ValueError("V178 closed query count differs")
    failures = []
    for roster, plan in zip(rosters, plans, strict=True):
        if (roster.get("ordinal") != plan.get("ordinal")
                or roster.get("source_id") != plan.get("source_id")):
            raise ValueError("V178 query identities differ")
        floor = minimum_primary_cover(roster["mandatory_units"], CAP_GETS)
        infeasible = floor["minimum_units"] > CAP_UNITS
        reported = plan["oracle"]["primary_constrained"].get("infeasible_primary") is True
        if infeasible != reported:
            raise ValueError("V178 DP primary feasibility differs from gap certificate")
        if infeasible:
            failures.append({"ordinal": roster["ordinal"],
                             "source_id": roster["source_id"], **floor})
    return {"schema": "borsuk-v178-primary-gap-certificate-v1",
            "rosters_sha256": ROSTERS_SHA, "plans_sha256": PLANS_SHA,
            "summary_sha256": SUMMARY_SHA, "terminal_sha256": TERMINAL_SHA,
            "queries_checked": len(plans), "max_gets": CAP_GETS,
            "cap_units": CAP_UNITS, "cap_bytes": CAP_BYTES,
            "unit_bytes": UNIT_BYTES, "infeasible_primary": failures}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rosters", type=Path, required=True)
    parser.add_argument("--plans", type=Path, required=True)
    parser.add_argument("--summary", type=Path, required=True)
    parser.add_argument("--terminal", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(certify(args.rosters, args.plans, args.summary, args.terminal),
                     sort_keys=True, separators=(",", ":")))


if __name__ == "__main__":
    main()
