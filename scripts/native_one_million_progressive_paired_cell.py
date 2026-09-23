#!/usr/bin/env python3
"""Truth-separated 1M paired two-bit/source score gate on mirrored pages."""

from __future__ import annotations

import argparse
import dataclasses
import hashlib
import json
from collections.abc import Sequence
from pathlib import Path

import numpy as np

from scripts.native_one_million_data_range_query import plan_ranked_pages
from scripts.native_one_million_opq8 import _physical_ordinals
from scripts.native_one_million_opq8_cell import _read_queries
from scripts.native_one_million_page_oracle_cell import _read_map
from scripts.native_one_million_page_oracle_cell import (
    run_construct as construct_page_map,
)
from scripts.native_one_million_selector_cell import (
    DEVELOPMENT_IDENTITIES,
    SOURCE_IDENTITIES,
)
from scripts.native_one_million_selector_evaluation import _query_truth
from scripts.native_one_million_source_scan import iter_authenticated_source_batches
from scripts.native_progressive_paired_priority import paired_priorities_from_batches
from scripts.native_progressive_source_build import build_from_source_batches
from scripts.recount_native_one_million_data_range import recount_range_masks

SCHEMA = "borsuk-one-million-progressive-paired-score-v1"
ROTATION_SEED = 20260923
PRIOR_PREFIX = (
    "s3://borsuk-bench-453182569524-euc1/research/"
    "native-one-million-progressive_code_wave-selector/"
    "7a06b24b0b5790406ba558d2c0f097493f401de1/"
    "runs/relaion-1m-dev1000-a0001/"
)
PRIOR_TERMINAL_SHA256 = "a97a9f2deb402d1b7a016906028849e1ee075f6b295b77f389c684d18784138a"
PRIOR_PLAN_SHA256 = "ee68310ef3084e2f08a7f26bb832251f8ab420a6abb80eb8d4c75adfe947a2ef"
PRIOR_PLAN_SEAL_SHA256 = "dc26dd4e834bd217c04ae13206820a99000a01b0bb2e48005639cd4b3280f640"
PLAN_FILE = "progressive-paired-plans.json"
PLAN_SEAL_FILE = "progressive-paired-plan-seal.json"
EVIDENCE_FILE = "progressive-paired-evidence.json"
RESULT_FILE = "progressive-paired-result.json"


def _canonical(value: object) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


def _sha(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _read_json(path: Path) -> dict[str, object]:
    body = path.read_bytes()
    value = json.loads(body)
    if type(value) is not dict or body != _canonical(value):
        raise ValueError(f"{path.name} canonical authority differs")
    return value


def _prior_plans(root: Path) -> dict[str, object]:
    terminal_path = root / "prior-progressive-terminal.json"
    plans_path = root / "prior-progressive-plans.json"
    seal_path = root / "prior-progressive-plan-seal.json"
    if (
        _sha(terminal_path) != PRIOR_TERMINAL_SHA256
        or _sha(plans_path) != PRIOR_PLAN_SHA256
        or _sha(seal_path) != PRIOR_PLAN_SEAL_SHA256
    ):
        raise ValueError("progressive prior projection identity differs")
    terminal = _read_json(terminal_path)
    plans = _read_json(plans_path)
    seal = _read_json(seal_path)
    if (
        terminal.get("schema") != "borsuk-one-million-progressive-code-wave-terminal-v1"
        or terminal.get("status") != "complete" or terminal.get("exit_code") != 0
        or terminal.get("source_commit") != "7a06b24b0b5790406ba558d2c0f097493f401de1"
        or terminal.get("artifacts", {}).get("progressive-code-wave-plans", {}).get("sha256")
        != PRIOR_PLAN_SHA256
        or terminal.get("artifacts", {}).get("progressive-code-wave-plan-seal", {}).get("sha256")
        != PRIOR_PLAN_SEAL_SHA256
        or plans.get("schema") != "borsuk-one-million-progressive-mirrored-code-projection-v1-plans"
        or plans.get("row_bytes") != {"sign": 104, "magnitude": 96}
        or len(plans.get("samples", [])) != 1000
        or seal.get("plans_sha256") != PRIOR_PLAN_SHA256
    ):
        raise ValueError("progressive prior projection differs")
    return plans


def _source_layout(root: Path) -> tuple[dict[int, int], np.ndarray, np.ndarray, int]:
    seal, _, membership, page_groups, page_bytes = _read_map(root)
    physical, order_sha = _physical_ordinals(root, SOURCE_IDENTITIES)
    generation = _read_json(root / "generation.json")
    base_pages = len(generation["runs"][0]["pages"])
    inverse = np.empty(len(physical), dtype=np.int64)
    for row_id, ordinal in physical.items():
        inverse[ordinal] = row_id
    positions = np.searchsorted(membership["id"], inverse)
    if (
        len(physical) != 1_000_000 or len(page_groups) != 7_278
        or seal.get("page_order_sha256") != order_sha
        or np.any(positions >= len(membership))
        or not np.array_equal(membership["id"][positions], inverse)
        or not 0 < base_pages < len(page_groups)
        or np.any(page_bytes <= 0)
    ):
        raise ValueError("progressive source physical layout differs")
    row_pages = membership["page"][positions].astype(np.int64)
    expected_counts = np.asarray(
        [int(page["rows"]) for run in generation["runs"] for page in run["pages"]],
        dtype=np.int64,
    )
    if (
        len(expected_counts) != len(page_groups)
        or np.any(np.diff(row_pages) < 0)
        or not np.array_equal(
            np.bincount(row_pages, minlength=len(page_groups)), expected_counts,
        )
    ):
        raise ValueError("progressive physical page spans differ")
    return physical, inverse, row_pages, base_pages


def _source_batches(root: Path, physical: dict[int, int], row_pages: np.ndarray):
    _, _, _, page_groups, _ = _read_map(root)
    yield from iter_authenticated_source_batches(
        root / "source.parquet", SOURCE_IDENTITIES["source"],
        physical, row_pages, page_groups,
    )


def run_construct(root: Path) -> dict[str, object]:
    if (root / "queries.parquet").exists() or (root / "truth.parquet").exists():
        raise ValueError("progressive construct has query capability")
    _prior_plans(root)
    construct_page_map(root)
    physical, inverse, row_pages, base_pages = _source_layout(root)
    counts = np.bincount(row_pages, minlength=7_278)

    def batches():
        for ordinals, _pages, _groups, rows in _source_batches(root, physical, row_pages):
            yield inverse[ordinals], rows

    result = build_from_source_batches(
        root, batches, physical, tuple(int(count) for count in counts),
        base_pages=base_pages, source_sha256=SOURCE_IDENTITIES["source"].sha256,
        layout_sha256=_sha(root / "seal.json"), rotation_seed=ROTATION_SEED,
    )
    receipt = {
        "schema": SCHEMA + "-build", "source_identity": dataclasses.asdict(SOURCE_IDENTITIES["source"]),
        "prior_plans_sha256": PRIOR_PLAN_SHA256,
        "page_map_seal_sha256": _sha(root / "seal.json"),
        "mean_sha256": result["mean_sha256"],
        "records_sha256": result["seal"]["records_sha256"],
        "code_seal_sha256": _sha(root / "progressive-code-seal.json"),
        "source_stream_sha256": result["source_stream_sha256"],
        "rows": result["rows"],
    }
    (root / "progressive-build.json").write_bytes(_canonical(receipt))
    return receipt


def _code_authority(root: Path) -> tuple[dict[str, object], np.ndarray, np.ndarray]:
    build = _read_json(root / "progressive-build.json")
    code_seal = _read_json(root / "progressive-code-seal.json")
    mean_body = (root / "mean.bin").read_bytes()
    _, _, membership, page_groups, _ = _read_map(root)
    generation = _read_json(root / "generation.json")
    counts = np.bincount(membership["page"].astype(np.intp), minlength=len(page_groups))
    if (
        build.get("schema") != SCHEMA + "-build"
        or build.get("source_identity") != dataclasses.asdict(SOURCE_IDENTITIES["source"])
        or build.get("prior_plans_sha256") != PRIOR_PLAN_SHA256
        or build.get("page_map_seal_sha256") != _sha(root / "seal.json")
        or build.get("code_seal_sha256") != _sha(root / "progressive-code-seal.json")
        or build.get("mean_sha256") != hashlib.sha256(mean_body).hexdigest()
        or code_seal.get("mean_sha256") != build["mean_sha256"]
        or code_seal.get("records_sha256") != build.get("records_sha256")
        or code_seal.get("layout_sha256") != build["page_map_seal_sha256"]
        or code_seal.get("source_sha256") != SOURCE_IDENTITIES["source"].sha256
        or code_seal.get("rotation_seed") != ROTATION_SEED
        or code_seal.get("base_pages") != len(generation["runs"][0]["pages"])
        or code_seal.get("page_row_counts") != [int(count) for count in counts]
        or code_seal.get("rows") != 1_000_000
        or len(mean_body) != 768 * 4
        or (root / "records.bin").stat().st_size != 1_000_000 * 200
        or _sha(root / "records.bin") != build["records_sha256"]
    ):
        raise ValueError("progressive code build authority differs")
    mean = np.frombuffer(mean_body, dtype="<f4").copy()
    records = np.memmap(root / "records.bin", dtype=np.uint8, mode="r", shape=(1_000_000, 200))
    return code_seal, mean, records


def _data_lengths(root: Path, base_pages: int) -> dict[str, tuple[int, ...]]:
    _, _, _, _, page_bytes = _read_map(root)
    values = tuple(int(value) for value in page_bytes)
    return {"base": values[:base_pages], "delta": values[base_pages:]}


def _included_pages(plan: dict[str, object], base_pages: int) -> tuple[int, ...]:
    sign = plan["code"]["sign"]
    magnitude = plan["code"]["magnitude"]
    if (
        sign["included_pages"] != magnitude["included_pages"]
        or sign["ranges"] != magnitude["ranges"]
        or sign["gets"] != magnitude["gets"]
    ):
        raise ValueError("progressive code mirror differs")
    return tuple(
        page if role == "base" else base_pages + page
        for role, page in sign["included_pages"]
    )


def run_plan(root: Path, out: Path, *, query_count: int = 1000) -> dict[str, object]:
    if (root / "truth.parquet").exists():
        raise ValueError("progressive plan must be truth-free")
    prior = _prior_plans(root)
    code_seal, mean, records = _code_authority(root)
    physical, _inverse, row_pages, base_pages = _source_layout(root)
    queries = _read_queries(root / "queries.parquet", DEVELOPMENT_IDENTITIES["queries"], query_count)
    if query_count != len(prior["samples"]):
        raise ValueError("progressive fixed cohort differs")
    included = [_included_pages(item, base_pages) for item in prior["samples"]]
    source_batches = (
        (ordinals.astype(np.int64), pages.astype(np.int64), rows)
        for ordinals, pages, _groups, rows in _source_batches(root, physical, row_pages)
    )
    priorities = paired_priorities_from_batches(
        queries, mean, records, source_batches, included,
        page_count=7_278, expected_rows=1_000_000,
        rotation_seed=ROTATION_SEED,
    )
    lengths = _data_lengths(root, base_pages)
    samples = [
        {"query_ordinal": ordinal, **{
            arm: plan_ranked_pages(priorities[ordinal][arm], lengths)
            for arm in ("two_bit", "source")
        }}
        for ordinal in range(query_count)
    ]
    plans = {
        "schema": SCHEMA + "-plans", "prior_plans_sha256": PRIOR_PLAN_SHA256,
        "code_seal_sha256": _sha(root / "progressive-code-seal.json"),
        "build_sha256": _sha(root / "progressive-build.json"),
        "query_identity": dataclasses.asdict(DEVELOPMENT_IDENTITIES["queries"]),
        "scorer": "rotated-two-bit-float32-batched-matmul-v1",
        "samples": samples,
    }
    out.mkdir(parents=True, exist_ok=True)
    body = _canonical(plans)
    (out / PLAN_FILE).write_bytes(body)
    (out / PLAN_SEAL_FILE).write_bytes(_canonical({
        "schema": SCHEMA + "-plan-seal", "plans_sha256": hashlib.sha256(body).hexdigest(),
        "prior_plans_sha256": PRIOR_PLAN_SHA256,
        "code_seal_sha256": plans["code_seal_sha256"],
    }))
    return plans


def _sealed_plans(root: Path, query_count: int) -> dict[str, object]:
    plans = _read_json(root / PLAN_FILE)
    seal = _read_json(root / PLAN_SEAL_FILE)
    _prior_plans(root)
    if (
        plans.get("schema") != SCHEMA + "-plans"
        or plans.get("prior_plans_sha256") != PRIOR_PLAN_SHA256
        or plans.get("code_seal_sha256") != _sha(root / "progressive-code-seal.json")
        or plans.get("build_sha256") != _sha(root / "progressive-build.json")
        or plans.get("query_identity") != dataclasses.asdict(DEVELOPMENT_IDENTITIES["queries"])
        or plans.get("scorer") != "rotated-two-bit-float32-batched-matmul-v1"
        or len(plans.get("samples", [])) != query_count
        or seal != {
            "schema": SCHEMA + "-plan-seal", "plans_sha256": _sha(root / PLAN_FILE),
            "prior_plans_sha256": PRIOR_PLAN_SHA256,
            "code_seal_sha256": plans["code_seal_sha256"],
        }
    ):
        raise ValueError("progressive paired plan seal differs")
    return plans


def run_evaluate(root: Path, out: Path, *, query_count: int = 1000) -> dict[str, object]:
    plans = _sealed_plans(root, query_count)
    _, _, membership, _, _ = _read_map(root)
    generation = _read_json(root / "generation.json")
    lengths = _data_lengths(root, len(generation["runs"][0]["pages"]))
    _, truth_ids = _query_truth(
        root / "queries.parquet", root / "truth.parquet",
        DEVELOPMENT_IDENTITIES, query_count=query_count,
    )
    positions = np.searchsorted(membership["id"], truth_ids)
    if np.any(positions >= len(membership)) or not np.array_equal(membership["id"][positions], truth_ids):
        raise ValueError("progressive paired truth owner differs")
    truth_pages = membership["page"][positions]
    samples, metrics = recount_range_masks(
        plans["samples"], [[int(page) for page in row] for row in truth_pages],
        lengths, arms=("two_bit", "source"),
    )
    coded = metrics["two_bit"]
    source = metrics["source"]
    gross_lost = sum(
        sum(a == "1" and b == "0" for a, b in zip(
            item["source"]["hit_mask"], item["two_bit"]["hit_mask"], strict=True,
        ))
        for item in samples
    )
    recovered = sum(
        sum(a == "0" and b == "1" for a, b in zip(
            item["source"]["hit_mask"], item["two_bit"]["hit_mask"], strict=True,
        ))
        for item in samples
    )
    net_loss = source["gt100_hits"] - coded["gt100_hits"]
    if net_loss != gross_lost - recovered:
        raise ValueError("progressive paired hit accounting differs")
    metrics["paired_loss"] = {
        "gross_lost": gross_lost, "recovered": recovered, "net_loss": net_loss,
    }
    advance = (
        coded["gt100_hits"] >= 98_151 and coded["gt10_hits"] >= 9_928
        and coded["p05_gt100_hits"] >= 90 and coded["sub90_queries"] <= 49
        and coded["maximum_gets"] <= 32 and coded["maximum_bytes"] <= 16_777_216
        and source["gt100_hits"] - coded["gt100_hits"] <= 300
    )
    evidence = {
        "schema": SCHEMA + "-evidence", "plans_sha256": _sha(root / PLAN_FILE),
        "truth_identity": dataclasses.asdict(DEVELOPMENT_IDENTITIES["truth"]),
        "samples": samples, "metrics": metrics,
    }
    out.mkdir(parents=True, exist_ok=True)
    body = _canonical(evidence)
    (out / EVIDENCE_FILE).write_bytes(body)
    result = {
        "schema": SCHEMA + "-result", "claim_eligible": False,
        "advance_candidate": advance, "evidence_sha256": hashlib.sha256(body).hexdigest(),
        "metrics": metrics,
    }
    (out / RESULT_FILE).write_bytes(_canonical(result))
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
            raise ValueError("progressive paired plan output required")
        run_plan(args.root, args.out)
    else:
        if args.out is None:
            raise ValueError("progressive paired evaluate output required")
        run_evaluate(args.root, args.out)


if __name__ == "__main__":
    main()
