#!/usr/bin/env python3
"""Truth-separated 1M source-score final-range diagnostic."""

from __future__ import annotations

import argparse
import dataclasses
import hashlib
import json
import os
from collections.abc import Sequence
from pathlib import Path

import numpy as np

from scripts.native_one_million_data_range_cell import (
    MAX_BYTES,
    MAX_GETS,
    PRIOR,
    _canonical,
    _checked_group_plan,
    _identity,
    _read_source,
)
from scripts.native_one_million_data_range_cell import (
    run_construct as construct_page_map,
)
from scripts.native_one_million_data_range_query import plan_ranked_pages
from scripts.native_one_million_opq8 import _physical_ordinals
from scripts.native_one_million_opq8_cell import _read_queries
from scripts.native_one_million_page_oracle_cell import _projected
from scripts.native_one_million_selector_cell import (
    DEVELOPMENT_IDENTITIES,
    SOURCE_IDENTITIES,
)
from scripts.native_one_million_selector_evaluation import _query_truth
from scripts.native_one_million_source_priority import priorities_from_batches
from scripts.native_one_million_source_scan import iter_authenticated_source_batches
from scripts.recount_native_one_million_data_range import recount_range_masks

SCHEMA = "borsuk-one-million-source-range-v1"
SCORE_ARITHMETIC = "float32-to-float64-squared-l2-norm-plus-dot-clamp-zero-v1"
PLAN_FILES = ("source-range-plans.json", "source-range-plan-seal.json")


def _scorer_identity() -> dict[str, str]:
    blas = np.__config__.show(mode="dicts")["Build Dependencies"]["blas"]
    return {
        "arithmetic": SCORE_ARITHMETIC,
        "numpy": np.__version__,
        "blas_name": str(blas["name"]),
        "blas_version": str(blas["version"]),
        "openblas_threads": os.environ.get("OPENBLAS_NUM_THREADS", ""),
        "omp_threads": os.environ.get("OMP_NUM_THREADS", ""),
    }


def run_construct(root: Path, *, expected_rows: int = 1_000_000, expected_pages: int = 7_278) -> None:
    if (root / "queries.parquet").exists() or (root / "truth.parquet").exists():
        raise ValueError("source-range construction phase differs")
    construct_page_map(root, expected_rows=expected_rows, expected_pages=expected_pages)


def run_plan(
    root: Path, out: Path, *, query_count: int = 1000,
    expected_rows: int = 1_000_000,
) -> dict[str, object]:
    if (root / "truth.parquet").exists():
        raise ValueError("source-range query-only phase differs")
    groups, historical, lengths, row_pages, page_groups, _ = _read_source(root)
    queries = _read_queries(root / "queries.parquet", DEVELOPMENT_IDENTITIES["queries"], query_count)
    if len(historical["samples"]) != query_count or len(row_pages) != expected_rows:
        raise ValueError("source-range fixed cohort differs")
    projected = _projected(groups)
    selected_by_query: list[dict[str, tuple[int, ...]]] = []
    for ordinal, previous in enumerate(historical["samples"]):
        if previous["query_ordinal"] != ordinal:
            raise ValueError("source-range prior query order differs")
        selected_by_query.append({
            arm: _checked_group_plan(projected, previous[arm])
            for arm in ("candidate", "control")
        })
    physical, order_sha = _physical_ordinals(root, SOURCE_IDENTITIES)
    source_seal = json.loads((root / "range-seal.json").read_bytes())
    if len(physical) != len(row_pages) or order_sha != source_seal["page_order_sha256"]:
        raise ValueError("source-range physical order differs")
    batches = iter_authenticated_source_batches(
        root / "source.parquet", SOURCE_IDENTITIES["source"],
        physical, row_pages, page_groups,
    )
    priorities = priorities_from_batches(
        queries, batches, selected_by_query,
        page_count=len(page_groups), group_count=len(groups),
        expected_rows=len(row_pages),
    )
    samples = [
        {
            "query_ordinal": ordinal,
            **{
                arm: plan_ranked_pages(priorities[ordinal][arm], lengths)
                for arm in ("candidate", "control")
            },
        }
        for ordinal in range(query_count)
    ]
    plans = {
        "schema": SCHEMA + "-plans",
        "source_seal_sha256": _identity(root / "range-seal.json")["sha256"],
        "prior_plans_sha256": PRIOR["plans"][1],
        "source_identity": dataclasses.asdict(SOURCE_IDENTITIES["source"]),
        "query_identity": dataclasses.asdict(DEVELOPMENT_IDENTITIES["queries"]),
        "scorer": _scorer_identity(),
        "samples": samples,
    }
    out.mkdir(parents=True, exist_ok=True)
    body = _canonical(plans)
    (out / PLAN_FILES[0]).write_bytes(body)
    (out / PLAN_FILES[1]).write_bytes(_canonical({
        "schema": SCHEMA + "-plan-seal",
        "plans_sha256": hashlib.sha256(body).hexdigest(),
        "source_seal_sha256": plans["source_seal_sha256"],
        "query_identity": plans["query_identity"],
        "scorer": plans["scorer"],
    }))
    return plans


def _sealed_plans(root: Path, query_count: int) -> dict[str, object]:
    body = (root / PLAN_FILES[0]).read_bytes()
    plans = json.loads(body)
    seal_body = (root / PLAN_FILES[1]).read_bytes()
    seal = json.loads(seal_body)
    if (
        body != _canonical(plans) or seal_body != _canonical(seal)
        or plans.get("schema") != SCHEMA + "-plans"
        or plans.get("source_seal_sha256") != _identity(root / "range-seal.json")["sha256"]
        or plans.get("prior_plans_sha256") != PRIOR["plans"][1]
        or plans.get("source_identity") != dataclasses.asdict(SOURCE_IDENTITIES["source"])
        or plans.get("query_identity") != dataclasses.asdict(DEVELOPMENT_IDENTITIES["queries"])
        or plans.get("scorer") != _scorer_identity()
        or len(plans.get("samples", [])) != query_count
        or seal != {
            "schema": SCHEMA + "-plan-seal",
            "plans_sha256": hashlib.sha256(body).hexdigest(),
            "source_seal_sha256": plans["source_seal_sha256"],
            "query_identity": plans["query_identity"],
            "scorer": plans["scorer"],
        }
    ):
        raise ValueError("source-range plan seal differs")
    return plans


def run_evaluate(root: Path, out: Path, *, query_count: int = 1000) -> dict[str, object]:
    _, _, lengths, _, _, membership = _read_source(root)
    plans = _sealed_plans(root, query_count)
    _, truth_ids = _query_truth(
        root / "queries.parquet", root / "truth.parquet",
        DEVELOPMENT_IDENTITIES, query_count=query_count,
    )
    positions = np.searchsorted(membership["id"], truth_ids)
    if np.any(positions >= len(membership)) or not np.array_equal(membership["id"][positions], truth_ids):
        raise ValueError("source-range truth owner differs")
    truth_pages = membership["page"][positions]
    samples, metrics = recount_range_masks(
        plans["samples"], [[int(page) for page in row] for row in truth_pages], lengths,
    )
    candidate = metrics["candidate"]
    passes = (
        candidate["gt100_hits"] >= 98_151 and candidate["gt10_hits"] >= 9_928
        and candidate["p05_gt100_hits"] >= 90 and candidate["sub90_queries"] <= 49
        and candidate["maximum_gets"] <= MAX_GETS and candidate["maximum_bytes"] <= MAX_BYTES
    )
    evidence = {
        "schema": SCHEMA + "-evidence",
        "plans_sha256": _identity(root / PLAN_FILES[0])["sha256"],
        "samples": samples, "metrics": metrics,
    }
    out.mkdir(parents=True, exist_ok=True)
    body = _canonical(evidence)
    (out / "source-range-evidence.json").write_bytes(body)
    result = {
        "schema": SCHEMA + "-result", "claim_eligible": False,
        "decision": "source-score-diagnostic-pass" if passes else "source-score-diagnostic-fail",
        "evidence_sha256": hashlib.sha256(body).hexdigest(),
        "metrics": metrics,
    }
    (out / "source-range-result.json").write_bytes(_canonical(result))
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
            raise ValueError("source-range plan output required")
        run_plan(args.root, args.out)
    else:
        if args.out is None:
            raise ValueError("source-range evaluation output required")
        run_evaluate(args.root, args.out)


if __name__ == "__main__":
    main()
