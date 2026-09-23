#!/usr/bin/env python3
"""Phase-separated 1M OPQ8 code-score to final data-range decision."""

from __future__ import annotations

import argparse
import dataclasses
import hashlib
import json
import math
from collections.abc import Sequence
from pathlib import Path

import numpy as np

from scripts.native_hundred_thousand_opq8_router import read_opq8_model, row_adc_scores
from scripts.native_one_million_data_range_evaluation import evaluate_query_pages
from scripts.native_one_million_data_range_query import plan_query
from scripts.native_one_million_group_selector import MEMBERSHIP_DTYPE
from scripts.native_one_million_opq8 import _physical_ordinals
from scripts.native_one_million_opq8_cell import MODEL_IDENTITY, _read_queries
from scripts.native_one_million_page_oracle_cell import (
    PRIOR,
    _canonical,
    _identity,
    _prior,
    _prior_authority,
    _projected,
    _read_map,
)
from scripts.native_one_million_page_oracle_cell import (
    run_construct as construct_page_map,
)
from scripts.native_one_million_range_selector_evaluation import plan_group_ranges
from scripts.native_one_million_selector_cell import (
    DEVELOPMENT_IDENTITIES,
    SOURCE_IDENTITIES,
)
from scripts.native_one_million_selector_evaluation import _query_truth
from scripts.v97_row_width_screen import _authenticate_object

SCHEMA = "borsuk-one-million-opq8-data-range-v1"
PRIOR_CODES = ("4fecab2927eceda33f3777751fcb841f8daf715613f79b0a37fe53243ead8267", 8_000_000)
PRIOR_MODEL = ("e49a31fd334f4af3de7964ca6452c3f93bc6223939e29718721b3d34207733ff", 3_148_820)
PRIOR_MEMBERSHIP = ("55e36613b293ffc08c11b9da8c2f2293cf89e43310551f316f2c5b57a84e0d13", 12_000_000)
MAX_GETS = 32
MAX_BYTES = 16_777_216


def _check_prior_file(root: Path, filename: str, identity: tuple[str, int]) -> None:
    actual = _identity(root / filename)
    if actual != {"sha256": identity[0], "bytes": identity[1]}:
        raise ValueError(f"prior {filename} identity differs")


def _check_code_authority(root: Path, source: dict[str, object]) -> None:
    terminal = _prior(root, "terminal")
    for role, filename, identity in (
        ("codes", "codes.bin", PRIOR_CODES),
        ("model", "model.bin", PRIOR_MODEL),
        ("membership", "prior-membership.bin", PRIOR_MEMBERSHIP),
    ):
        _check_prior_file(root, filename, identity)
        receipt = terminal.get("artifacts", {}).get(role, {})
        if receipt.get("sha256") != identity[0] or receipt.get("encoded_bytes") != identity[1]:
            raise ValueError(f"prior {role} terminal identity differs")
    if (
        source.get("codes") != {"sha256": PRIOR_CODES[0], "bytes": PRIOR_CODES[1]}
        or source.get("membership") != {"sha256": PRIOR_MEMBERSHIP[0], "bytes": PRIOR_MEMBERSHIP[1]}
        or source.get("model_identity") != dataclasses.asdict(MODEL_IDENTITY)
    ):
        raise ValueError("data-range OPQ8 source authority differs")


def _page_lengths(root: Path, page_bytes: np.ndarray) -> dict[str, tuple[int, ...]]:
    generation = json.loads((root / "generation.json").read_bytes())
    runs = generation["runs"]
    if len(runs) != 2 or [run["kind"] for run in runs] != ["base", "delta"]:
        raise ValueError("data-range generation role order differs")
    split = len(runs[0]["pages"])
    recorded_bytes: list[int] = []
    for run in runs:
        next_offset = 0
        for ordinal, page in enumerate(run["pages"]):
            if (
                type(page.get("page")) is not int or page["page"] != ordinal
                or type(page.get("offset")) is not int or page["offset"] != next_offset
                or type(page.get("bytes")) is not int or page["bytes"] <= 0
            ):
                raise ValueError("data-range page span differs")
            next_offset += page["bytes"]
            recorded_bytes.append(page["bytes"])
        if run.get("object", {}).get("bytes") != next_offset:
            raise ValueError("data-range object span differs")
    if split <= 0 or split >= len(page_bytes) or recorded_bytes != page_bytes.tolist():
        raise ValueError("data-range page split differs")
    return {
        "base": tuple(int(value) for value in page_bytes[:split]),
        "delta": tuple(int(value) for value in page_bytes[split:]),
    }


def run_construct(root: Path, *, expected_rows: int = 1_000_000, expected_pages: int = 7_278) -> None:
    if (root / "queries.parquet").exists() or (root / "truth.parquet").exists():
        raise ValueError("data-range source phase differs")
    source, _ = _prior_authority(root)
    _check_code_authority(root, source)
    construct_page_map(root, expected_rows=expected_rows, expected_pages=expected_pages)
    map_seal, _, membership, page_groups, page_bytes = _read_map(root)
    physical, order_sha = _physical_ordinals(root, SOURCE_IDENTITIES)
    if len(physical) != expected_rows or order_sha != source["page_order_sha256"]:
        raise ValueError("data-range physical row order differs")
    row_pages = np.empty(expected_rows, dtype="<u4")
    positions = np.searchsorted(membership["id"], np.fromiter(physical, dtype="<i8", count=expected_rows))
    if np.any(positions >= len(membership)):
        raise ValueError("data-range physical membership differs")
    for index, (row_id, ordinal) in enumerate(physical.items()):
        if int(membership["id"][positions[index]]) != row_id:
            raise ValueError("data-range physical ID differs")
        row_pages[ordinal] = membership["page"][positions[index]]
    prior_membership = np.fromfile(root / "prior-membership.bin", dtype=MEMBERSHIP_DTYPE)
    if (
        len(prior_membership) != expected_rows
        or not np.array_equal(prior_membership["id"], membership["id"])
        or not np.array_equal(prior_membership["group"], page_groups[membership["page"]])
    ):
        raise ValueError("data-range source group membership differs")
    _page_lengths(root, page_bytes)
    (root / "row-pages.bin").write_bytes(row_pages.tobytes(order="C"))
    seal = {
        "schema": SCHEMA + "-source-seal",
        "map_seal_sha256": _identity(root / "seal.json")["sha256"],
        "prior_source_sha256": PRIOR["source-seal"][1],
        "prior_plans_sha256": PRIOR["plans"][1],
        "codes_sha256": PRIOR_CODES[0],
        "model_sha256": PRIOR_MODEL[0],
        "row_pages": _identity(root / "row-pages.bin"),
        "page_order_sha256": map_seal["page_order_sha256"],
    }
    (root / "range-seal.json").write_bytes(_canonical(seal))


def _read_source(root: Path) -> tuple[object, dict[str, object], dict[str, tuple[int, ...]], np.ndarray, np.ndarray, np.ndarray]:
    source, prior_plans = _prior_authority(root)
    map_seal, groups, membership, page_groups, page_bytes = _read_map(root)
    _check_code_authority(root, source)
    _authenticate_object("generation", root / "generation.json", SOURCE_IDENTITIES["generation"])
    seal_body = (root / "range-seal.json").read_bytes()
    seal = json.loads(seal_body)
    row_pages_body = (root / "row-pages.bin").read_bytes()
    if (
        seal_body != _canonical(seal)
        or seal.get("schema") != SCHEMA + "-source-seal"
        or seal.get("map_seal_sha256") != _identity(root / "seal.json")["sha256"]
        or seal.get("prior_source_sha256") != PRIOR["source-seal"][1]
        or seal.get("prior_plans_sha256") != PRIOR["plans"][1]
        or seal.get("codes_sha256") != PRIOR_CODES[0]
        or seal.get("model_sha256") != PRIOR_MODEL[0]
        or seal.get("row_pages") != _identity(root / "row-pages.bin")
        or seal.get("page_order_sha256") != map_seal["page_order_sha256"]
        or source["groups"] != map_seal["groups"]
        or len(row_pages_body) != len(membership) * 4
    ):
        raise ValueError("data-range source seal differs")
    row_pages = np.frombuffer(row_pages_body, dtype="<u4")
    if np.any(row_pages >= len(page_groups)):
        raise ValueError("data-range row pages differ")
    return groups, prior_plans, _page_lengths(root, page_bytes), row_pages, page_groups, membership


def _checked_group_plan(groups: Sequence[object], old: dict[str, object]) -> tuple[int, ...]:
    selected, intervals, gets, used = plan_group_ranges(groups, old["ranked_groups"])
    if (
        list(selected) != old["selected_groups"]
        or [list(item) for item in intervals] != old["intervals"]
        or gets != old["projected_code_gets"]
        or used != old["projected_code_bytes"]
    ):
        raise ValueError("data-range prior group plan differs")
    return tuple(selected)


def run_plan(root: Path, out: Path, *, query_count: int = 1000) -> dict[str, object]:
    if (root / "truth.parquet").exists():
        raise ValueError("data-range query-only phase differs")
    groups, historical, lengths, row_pages, page_groups, _ = _read_source(root)
    queries = _read_queries(root / "queries.parquet", DEVELOPMENT_IDENTITIES["queries"], query_count)
    if len(historical["samples"]) != query_count or len(row_pages) != 1_000_000:
        raise ValueError("data-range fixed cohort differs")
    codes = np.fromfile(root / "codes.bin", dtype=np.uint8).reshape(-1, 8)
    model = read_opq8_model(root / "model.bin")
    projected = _projected(groups)
    samples = []
    for ordinal, query in enumerate(queries):
        previous = historical["samples"][ordinal]
        if previous["query_ordinal"] != ordinal:
            raise ValueError("data-range prior query order differs")
        scores = row_adc_scores(query, model, codes)
        if not np.isfinite(scores).all():
            raise ValueError("data-range code scores differ")
        sample: dict[str, object] = {"query_ordinal": ordinal}
        for arm in ("candidate", "control"):
            old = previous[arm]
            selected = _checked_group_plan(projected, old)
            sample[arm] = plan_query(scores, row_pages, page_groups, lengths, selected)
        samples.append(sample)
    plans = {
        "schema": SCHEMA + "-plans", "source_seal_sha256": _identity(root / "range-seal.json")["sha256"],
        "prior_plans_sha256": PRIOR["plans"][1],
        "query_identity": dataclasses.asdict(DEVELOPMENT_IDENTITIES["queries"]),
        "samples": samples,
    }
    out.mkdir(parents=True, exist_ok=True)
    body = _canonical(plans)
    (out / "range-plans.json").write_bytes(body)
    (out / "range-plan-seal.json").write_bytes(_canonical({
        "schema": SCHEMA + "-plan-seal", "plans_sha256": hashlib.sha256(body).hexdigest(),
        "source_seal_sha256": plans["source_seal_sha256"],
        "query_identity": plans["query_identity"],
    }))
    return plans


def _sealed_plans(root: Path, query_count: int) -> dict[str, object]:
    body = (root / "range-plans.json").read_bytes()
    plans = json.loads(body)
    seal_body = (root / "range-plan-seal.json").read_bytes()
    seal = json.loads(seal_body)
    if (
        body != _canonical(plans)
        or seal_body != _canonical(seal)
        or plans.get("schema") != SCHEMA + "-plans"
        or plans.get("source_seal_sha256") != _identity(root / "range-seal.json")["sha256"]
        or plans.get("prior_plans_sha256") != PRIOR["plans"][1]
        or plans.get("query_identity") != dataclasses.asdict(DEVELOPMENT_IDENTITIES["queries"])
        or len(plans.get("samples", [])) != query_count
        or seal != {
            "schema": SCHEMA + "-plan-seal", "plans_sha256": hashlib.sha256(body).hexdigest(),
            "source_seal_sha256": plans["source_seal_sha256"],
            "query_identity": plans["query_identity"],
        }
    ):
        raise ValueError("data-range plan seal differs")
    return plans


def run_evaluate(root: Path, out: Path, *, query_count: int = 1000) -> dict[str, object]:
    _, historical, lengths, _, _, membership = _read_source(root)
    plans = _sealed_plans(root, query_count)
    _, truth_ids = _query_truth(
        root / "queries.parquet", root / "truth.parquet", DEVELOPMENT_IDENTITIES,
        query_count=query_count,
    )
    positions = np.searchsorted(membership["id"], truth_ids)
    if np.any(positions >= len(membership)) or not np.array_equal(membership["id"][positions], truth_ids):
        raise ValueError("data-range truth owner differs")
    truth_pages = membership["page"][positions]
    samples = []
    for ordinal, item in enumerate(plans["samples"]):
        if item["query_ordinal"] != ordinal or historical["samples"][ordinal]["query_ordinal"] != ordinal:
            raise ValueError("data-range result order differs")
        sample: dict[str, object] = {"query_ordinal": ordinal}
        for arm in ("candidate", "control"):
            sample[arm] = evaluate_query_pages(
                item[arm], tuple(int(page) for page in truth_pages[ordinal]), lengths
            )
        samples.append(sample)
    metrics: dict[str, object] = {}
    for arm in ("candidate", "control"):
        hits = [int(item[arm]["hits_at_100"]) for item in samples]
        arm_plans = [item[arm] for item in plans["samples"]]
        metrics[arm] = {
            "gt100_hits": sum(hits),
            "gt10_hits": sum(int(item[arm]["hits_at_10"]) for item in samples),
            "target_gt100_hits": sum(item[arm]["hit_kinds"].count("target") for item in samples),
            "bridge_gt100_hits": sum(item[arm]["hit_kinds"].count("bridge") for item in samples),
            "p05_gt100_hits": sorted(hits)[math.ceil(query_count * .05) - 1],
            "sub90_queries": sum(value < 90 for value in hits),
            "maximum_gets": max(int(item["gets"]) for item in arm_plans),
            "maximum_bytes": max(int(item["encoded_bytes"]) for item in arm_plans),
            "total_gets": sum(int(item["gets"]) for item in arm_plans),
            "total_bytes": sum(int(item["encoded_bytes"]) for item in arm_plans),
        }
    metrics["paired_gt100"] = {
        "candidate_better": sum(item["candidate"]["hits_at_100"] > item["control"]["hits_at_100"] for item in samples),
        "control_better": sum(item["candidate"]["hits_at_100"] < item["control"]["hits_at_100"] for item in samples),
        "tied": sum(item["candidate"]["hits_at_100"] == item["control"]["hits_at_100"] for item in samples),
    }
    candidate = metrics["candidate"]
    passes = (
        candidate["gt100_hits"] >= 98_151 and candidate["gt10_hits"] >= 9_928
        and candidate["p05_gt100_hits"] >= 90 and candidate["sub90_queries"] <= 49
        and candidate["maximum_gets"] <= MAX_GETS and candidate["maximum_bytes"] <= MAX_BYTES
    )
    evidence = {
        "schema": SCHEMA + "-evidence", "plans_sha256": _identity(root / "range-plans.json")["sha256"],
        "samples": samples, "metrics": metrics,
    }
    out.mkdir(parents=True, exist_ok=True)
    body = _canonical(evidence)
    (out / "range-evidence.json").write_bytes(body)
    result = {
        "schema": SCHEMA + "-result", "claim_eligible": False,
        "decision": "data-range-selector-advance" if passes else "data-range-selector-killed",
        "evidence_sha256": hashlib.sha256(body).hexdigest(), "metrics": metrics,
    }
    (out / "range-result.json").write_bytes(_canonical(result))
    return result


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("phase", choices=("construct", "plan", "evaluate"))
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--out", type=Path)
    args = parser.parse_args(argv)
    if args.phase == "construct":
        run_construct(args.root)
    elif args.phase == "plan":
        if args.out is None:
            raise ValueError("data-range plan output required")
        run_plan(args.root, args.out)
    else:
        if args.out is None:
            raise ValueError("data-range result output required")
        run_evaluate(args.root, args.out)


if __name__ == "__main__":
    main()
