#!/usr/bin/env python3
"""Terminal-bound necessary 32-page ceiling on frozen 1M OPQ8 plans."""

from __future__ import annotations

import argparse
import dataclasses
import hashlib
import json
import math
from collections.abc import Mapping, Sequence
from pathlib import Path

import numpy as np

from scripts.native_one_million_group_selector import Group
from scripts.native_one_million_page_feasibility import oracle_page_selection
from scripts.native_one_million_page_selector import _page_membership
from scripts.native_one_million_range_selector_evaluation import plan_group_ranges
from scripts.native_one_million_selector_cell import (
    DEVELOPMENT_IDENTITIES,
    SOURCE_IDENTITIES,
)
from scripts.native_one_million_selector_evaluation import _query_truth
from scripts.v97_row_width_screen import _authenticate_object, _canonical_json_bytes

SCHEMA = "borsuk-one-million-opq8-page-oracle-v1"
PRIOR_PREFIX = (
    "s3://borsuk-bench-453182569524-euc1/research/native-one-million-opq8-selector/"
    "cc0dd60de8b63a87656475591be12127ad4f3769/runs/relaion-1m-dev1000-a0001/"
)
PRIOR = {
    "terminal": (
        "terminal.json",
        "257266bffcb7a80402eab465bbf920c188eb3e35d4330afcf2d533cea09feca9",
        4734,
    ),
    "source-seal": (
        "artifacts/seal.json",
        "cfab0c53219f8fc93d07353b36d181ce2b6d424907cc2dc63317cdbc1103f43c",
        92835,
    ),
    "plans": (
        "artifacts/plans.json",
        "cb39314a53f0030f57386887f3fa7213a3e5e869610dcdc107f28b1c7a4fbe08",
        14156915,
    ),
    "plan-seal": (
        "artifacts/plan-seal.json",
        "ee4c9c4c91b2197c16d07a91c726c8d5401fb22f14950470d32f3bfb8250ae4b",
        591,
    ),
}
MAP_DTYPE = np.dtype([("id", "<i8"), ("page", "<u4")])
EXPECTED_GROUP_HITS = {"candidate": (98_985, 9_956), "control": (98_151, 9_928)}


def _projected(groups: Sequence[Group]) -> tuple[Group, ...]:
    return tuple(
        dataclasses.replace(
            group,
            code_bytes=4
            + 4 * (group.end_page - group.first_page)
            + 96 * group.row_count,
        )
        for group in groups
    )


def _canonical(value: object) -> bytes:
    return _canonical_json_bytes(value)


def _identity(path: Path) -> dict[str, int | str]:
    body = path.read_bytes()
    return {"bytes": len(body), "sha256": hashlib.sha256(body).hexdigest()}


def _prior(root: Path, role: str) -> dict[str, object]:
    _, digest, size = PRIOR[role]
    body = (
        root
        / {
            "terminal": "prior-terminal.json",
            "source-seal": "prior-source-seal.json",
            "plans": "prior-plans.json",
            "plan-seal": "prior-plan-seal.json",
        }[role]
    ).read_bytes()
    if len(body) != size or hashlib.sha256(body).hexdigest() != digest:
        raise ValueError(f"prior {role} identity differs")
    try:
        value = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError(f"prior {role} JSON differs") from error
    if body != _canonical(value):
        raise ValueError(f"prior {role} canonical encoding differs")
    return value


def _prior_authority(root: Path) -> tuple[dict[str, object], dict[str, object]]:
    terminal = _prior(root, "terminal")
    source = _prior(root, "source-seal")
    plans = _prior(root, "plans")
    plan_seal = _prior(root, "plan-seal")
    if (
        terminal.get("schema") != "borsuk-one-million-opq8-terminal-v1"
        or terminal.get("status") != "complete"
        or terminal.get("exit_code") != 0
        or terminal.get("source_commit") != "cc0dd60de8b63a87656475591be12127ad4f3769"
        or any(
            terminal.get("artifacts", {}).get(role, {}).get("sha256") != PRIOR[role][1]
            for role in ("plans", "plan-seal")
        )
        or terminal.get("artifacts", {}).get("seal", {}).get("sha256")
        != PRIOR["source-seal"][1]
        or source.get("schema") != "borsuk-one-million-opq8-physical-codes-v1"
        or source.get("page_order_sha256") is None
        or plan_seal.get("plans_sha256") != PRIOR["plans"][1]
        or plan_seal.get("source_seal_sha256") != PRIOR["source-seal"][1]
        or plan_seal.get("control_seal_sha256") != plans.get("control_seal_sha256")
        or plans.get("schema") != "borsuk-one-million-opq8-containment-v1-plans"
        or plans.get("source_seal_sha256") != PRIOR["source-seal"][1]
        or plans.get("query_identity")
        != dataclasses.asdict(DEVELOPMENT_IDENTITIES["queries"])
        or len(plans.get("samples", [])) != 1000
    ):
        raise ValueError("prior OPQ8 authority differs")
    return source, plans


def run_construct(
    root: Path, *, expected_rows: int = 1_000_000, expected_pages: int = 7_278
) -> None:
    if (root / "truth.parquet").exists() or (root / "queries.parquet").exists():
        raise ValueError("page oracle construct phase differs")
    for role, filename in (
        ("generation", "generation.json"),
        ("base", "base.arrow"),
        ("delta", "delta.arrow"),
    ):
        _authenticate_object(role, root / filename, SOURCE_IDENTITIES[role])
    source, _ = _prior_authority(root)
    groups, id_to_page, page_groups, order_sha = _page_membership(
        root, SOURCE_IDENTITIES
    )
    generation = json.loads((root / "generation.json").read_bytes())
    page_bytes = np.asarray(
        [int(page["bytes"]) for run in generation["runs"] for page in run["pages"]],
        dtype="<u4",
    )
    if (
        len(id_to_page) != expected_rows
        or len(page_groups) != expected_pages
        or len(page_bytes) != expected_pages
        or np.any(page_bytes == 0)
        or source.get("page_order_sha256") != order_sha
        or source.get("groups") != [dataclasses.asdict(group) for group in groups]
    ):
        raise ValueError("page oracle physical authority differs")
    membership = np.empty(len(id_to_page), dtype=MAP_DTYPE)
    for index, row_id in enumerate(sorted(id_to_page)):
        membership[index] = row_id, id_to_page[row_id]
    (root / "page-map.bin").write_bytes(membership.tobytes(order="C"))
    (root / "page-groups.bin").write_bytes(page_groups.astype("<u4").tobytes())
    (root / "page-bytes.bin").write_bytes(page_bytes.tobytes())
    seal = {
        "schema": SCHEMA + "-map",
        "rows": expected_rows,
        "pages": expected_pages,
        "source_seal_sha256": PRIOR["source-seal"][1],
        "plans_sha256": PRIOR["plans"][1],
        "page_order_sha256": order_sha,
        "groups": [dataclasses.asdict(group) for group in groups],
        "page_map": _identity(root / "page-map.bin"),
        "page_groups": _identity(root / "page-groups.bin"),
        "page_bytes": _identity(root / "page-bytes.bin"),
    }
    (root / "seal.json").write_bytes(_canonical(seal))


def _read_map(
    root: Path,
) -> tuple[dict[str, object], tuple[Group, ...], np.ndarray, np.ndarray, np.ndarray]:
    seal_body = (root / "seal.json").read_bytes()
    seal = json.loads(seal_body)
    if seal_body != _canonical(seal) or seal.get("schema") != SCHEMA + "-map":
        raise ValueError("page oracle map seal differs")
    for filename, key in (
        ("page-map.bin", "page_map"),
        ("page-groups.bin", "page_groups"),
        ("page-bytes.bin", "page_bytes"),
    ):
        if _identity(root / filename) != seal[key]:
            raise ValueError("page oracle map identity differs")
    membership = np.frombuffer((root / "page-map.bin").read_bytes(), dtype=MAP_DTYPE)
    page_groups = np.frombuffer((root / "page-groups.bin").read_bytes(), dtype="<u4")
    page_bytes = np.frombuffer((root / "page-bytes.bin").read_bytes(), dtype="<u4")
    groups = tuple(Group(**group) for group in seal["groups"])
    if (
        len(membership) != seal["rows"]
        or len(page_groups) != seal["pages"]
        or len(page_bytes) != seal["pages"]
        or not groups
        or np.any(page_bytes == 0)
        or np.any(membership["id"][1:] <= membership["id"][:-1])
        or np.any(membership["page"] >= len(page_groups))
        or np.any(page_groups >= len(groups))
    ):
        raise ValueError("page oracle map shape differs")
    return seal, groups, membership, page_groups, page_bytes


def _metrics(samples: Sequence[dict[str, object]], arm: str) -> dict[str, int]:
    group100 = [sample[arm]["group_hits_at_100"] for sample in samples]
    group10 = [sample[arm]["group_hit_mask"][:10].count("1") for sample in samples]
    oracle100 = [sample[arm]["oracle_hits_at_100"] for sample in samples]
    oracle10 = [sample[arm]["oracle_upper_hits_at_10"] for sample in samples]
    return {
        "group_gt100_hits": sum(group100),
        "group_gt10_hits": sum(group10),
        "oracle_gt100_hits": sum(oracle100),
        "oracle_gt10_upper_hits": sum(oracle10),
        "oracle_p05_gt100_hits": sorted(oracle100)[math.ceil(0.05 * len(samples)) - 1],
        "oracle_sub90_queries": sum(value < 90 for value in oracle100),
    }


def run_evaluate(
    root: Path,
    out: Path,
    *,
    query_count: int = 1000,
    expected_group_hits: Mapping[str, tuple[int, int]] = EXPECTED_GROUP_HITS,
) -> dict[str, object]:
    source, plans = _prior_authority(root)
    seal, groups, membership, page_groups, page_bytes = _read_map(root)
    if source["groups"] != seal["groups"] or query_count != 1000:
        raise ValueError("page oracle source/plan authority differs")
    _, truth_ids = _query_truth(
        root / "queries.parquet",
        root / "truth.parquet",
        DEVELOPMENT_IDENTITIES,
        query_count=query_count,
    )
    positions = np.searchsorted(membership["id"], truth_ids)
    if np.any(positions >= len(membership)) or not np.array_equal(
        membership["id"][positions], truth_ids
    ):
        raise ValueError("page oracle truth membership differs")
    truth_pages = membership["page"][positions]
    projected = _projected(groups)
    samples: list[dict[str, object]] = []
    for ordinal, row in enumerate(plans["samples"]):
        if row.get("query_ordinal") != ordinal:
            raise ValueError("page oracle query order differs")
        sample: dict[str, object] = {"query_ordinal": ordinal}
        for arm in ("candidate", "control"):
            plan = row[arm]
            selected, intervals, gets, used = plan_group_ranges(
                projected, plan["ranked_groups"]
            )
            if (
                list(selected) != plan["selected_groups"]
                or [list(value) for value in intervals] != plan["intervals"]
                or gets != plan["projected_code_gets"]
                or used != plan["projected_code_bytes"]
            ):
                raise ValueError("page oracle sealed plan differs")
            pages = tuple(int(page) for page in truth_pages[ordinal])
            chosen = set(selected)
            group_mask = "".join(
                "1" if int(page_groups[page]) in chosen else "0" for page in pages
            )
            oracle_pages = oracle_page_selection(pages, selected, page_groups)
            oracle_set = set(oracle_pages)
            oracle_mask = "".join("1" if page in oracle_set else "0" for page in pages)
            oracle10_pages = oracle_page_selection(pages[:10], selected, page_groups)
            oracle10_set = set(oracle10_pages)
            sample[arm] = {
                "selected_pages": list(oracle_pages),
                "oracle_witness_bytes": sum(
                    int(page_bytes[page]) for page in oracle_pages
                ),
                "group_hit_mask": group_mask,
                "group_hits_at_100": group_mask.count("1"),
                "oracle_hit_mask": oracle_mask,
                "oracle_hits_at_100": oracle_mask.count("1"),
                "oracle_upper_hits_at_10": sum(
                    page in oracle10_set for page in pages[:10]
                ),
            }
        samples.append(sample)
    metrics = {arm: _metrics(samples, arm) for arm in ("candidate", "control")}
    if any(
        (metrics[arm]["group_gt100_hits"], metrics[arm]["group_gt10_hits"])
        != expected_group_hits[arm]
        for arm in ("candidate", "control")
    ):
        raise ValueError("page oracle prior group replay differs")
    candidate = metrics["candidate"]
    passes = (
        candidate["oracle_gt100_hits"] >= 98_151
        and candidate["oracle_p05_gt100_hits"] >= 90
        and candidate["oracle_gt10_upper_hits"] >= 9_928
        and candidate["oracle_sub90_queries"] <= 49
    )
    decision = (
        "page-count-ceiling-advance-source-scores"
        if passes
        else "page-count-ceiling-killed"
    )
    out.mkdir(parents=True, exist_ok=True)
    evidence = {
        "schema": SCHEMA + "-evidence",
        "source_seal_sha256": PRIOR["source-seal"][1],
        "plans_sha256": PRIOR["plans"][1],
        "map_seal_sha256": hashlib.sha256(
            (root / "seal.json").read_bytes()
        ).hexdigest(),
        "samples": samples,
        "metrics": metrics,
    }
    body = _canonical(evidence)
    (out / "evidence.json").write_bytes(body)
    result = {
        "schema": SCHEMA + "-result",
        "decision": decision,
        "claim_eligible": False,
        "evidence_sha256": hashlib.sha256(body).hexdigest(),
        "metrics": metrics,
    }
    (out / "result.json").write_bytes(_canonical(result))
    return result


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("phase", choices=("construct", "evaluate"))
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--out", type=Path)
    args = parser.parse_args(argv)
    if args.phase == "construct":
        run_construct(args.root)
    elif args.out is not None:
        run_evaluate(args.root, args.out)
    else:
        raise ValueError("page oracle output required")


if __name__ == "__main__":
    main()
