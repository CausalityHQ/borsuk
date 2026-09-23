#!/usr/bin/env python3
"""Truth-separated ReLAION-100k sign96 versus source range screen."""

from __future__ import annotations

import argparse
import dataclasses
import hashlib
import json
import os
from collections.abc import Sequence
from pathlib import Path

import numpy as np

from scripts.native_geometric_layout_screen import read_geometric_router_parquet
from scripts.native_page_microcluster_cell import (
    FROZEN_INPUTS,
    _read_inputs,
    _read_queries_truth,
)
from scripts.native_rotated_sign96_groups import (
    mean_from_source_batches,
    open_sign96_groups,
    write_sign96_groups,
)
from scripts.native_rotated_two_bit_cell import PRIOR_PAGES, PRIOR_TREE
from scripts.native_row_score_code_artifacts import (
    _physical_order,
    _physical_order_sha256,
)
from scripts.native_sign96_range_screen import paired_range_plans, score_selected_rows
from scripts.recount_native_one_million_data_range import recount_range_masks
from scripts.validate_native_geometric_layout_result import _read_geometric_queries

SCHEMA = "borsuk-hundred-thousand-sign96-range-v1"
ROTATION_SEED = 20260923
SOURCE_SEAL_FILE = "sign96-source-seal.json"
PLANS_FILE = "sign96-plans.json"
PLAN_SEAL_FILE = "sign96-plan-seal.json"
PRIOR_CODE_SEAL_SHA256 = "ee0d87790e8e65970340126dfea62f4eb625910df974a992ee5af58411f8067e"
PRIOR_CODE_SEAL_BYTES = 5283
OPQ_PLANS_SHA256 = "b14ed50ad4b552a95de9aa1871e3ea964bb96d76e871e3979e10e7036dc77064"
OPQ_PLANS_BYTES = 301559
OPQ_SEAL_SHA256 = "e6f1edf3d680dd60e1cb572df70347a8ac8d3c74bfaa6dbcc89dea2cc9c1faf8"


def _canonical(value: dict[str, object]) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


def _sha(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _read_prior_code_seal(root: Path) -> dict[str, object]:
    body = (root / "prior-code-seal.json").read_bytes()
    seal = json.loads(body)
    if (
        len(body) != PRIOR_CODE_SEAL_BYTES
        or hashlib.sha256(body).hexdigest() != PRIOR_CODE_SEAL_SHA256
        or body != _canonical(seal)
        or seal.get("schema") != "borsuk-rotated-two-bit-group-codes-v1"
        or seal.get("source_sha256") != FROZEN_INPUTS.layout.source.sha256
        or seal.get("membership_sha256") != FROZEN_INPUTS.membership.sha256
        or seal.get("layout_seed") != FROZEN_INPUTS.layout.seed
        or seal.get("row_bytes") != 200
        or seal.get("group_pages") != 4
    ):
        raise ValueError("sign96 prior code seal differs")
    return seal


def _read_plans(root: Path) -> list[dict[str, object]]:
    body = (root / "opq" / "plans.json").read_bytes()
    value = json.loads(body)
    if (
        len(body) != OPQ_PLANS_BYTES
        or hashlib.sha256(body).hexdigest() != OPQ_PLANS_SHA256
        or body != _canonical(value)
        or value.get("schema") != "borsuk-hundred-thousand-opq8-query-plans-v1"
        or value.get("seal_sha256") != OPQ_SEAL_SHA256
        or value.get("queries") != dataclasses.asdict(FROZEN_INPUTS.queries)
        or len(value.get("samples", [])) != 1000
    ):
        raise ValueError("sign96 OPQ8 plans differ")
    return value["samples"]


def _source_authority(
    root: Path, expected_rows: int, expected_pages: int,
) -> tuple[object, np.ndarray, object, tuple[int, ...], tuple[int, ...], str]:
    ids, vectors, membership = _read_inputs(root, FROZEN_INPUTS)
    prior = _read_prior_code_seal(root)
    ordinals, counts, source_sha = _physical_order(ids, membership, FROZEN_INPUTS.layout.seed)
    order_sha = _physical_order_sha256(ordinals)
    if (
        len(ids) != expected_rows
        or vectors.shape != (expected_rows, 768)
        or len(counts) != expected_pages
        or list(counts) != prior["page_row_counts"]
        or order_sha != prior["physical_order_sha256"]
        or source_sha.hex() != FROZEN_INPUTS.layout.source.sha256
    ):
        raise ValueError("sign96 prior source/layout authority differs")
    return ids, vectors, membership, ordinals, counts, order_sha


def run_construct(
    root: Path, *, expected_rows: int = 100_000, expected_pages: int = 166,
) -> dict[str, object]:
    """Fit the mean and write sign groups before query or truth capability."""
    if (root / "queries.parquet").exists() or (root / "truth.parquet").exists():
        raise ValueError("sign96 source-only phase differs")
    _, vectors, _, ordinals, counts, order_sha = _source_authority(
        root, expected_rows, expected_pages
    )
    receipt = mean_from_source_batches(
        (vectors[first : first + 4096] for first in range(0, expected_rows, 4096)),
        expected_rows=expected_rows,
        batch_rows=4096,
    )
    physical_by_source = np.empty(expected_rows, dtype=np.int64)
    physical_by_source[np.asarray(ordinals, dtype=np.intp)] = np.arange(expected_rows)
    batches = (
        (physical_by_source[first : first + 4096], vectors[first : first + 4096])
        for first in range(0, expected_rows, 4096)
    )
    written = write_sign96_groups(
        root / "sign96", receipt, counts, batches,
        source_sha256=FROZEN_INPUTS.layout.source.sha256,
        layout_sha256=FROZEN_INPUTS.membership.sha256,
        physical_order_sha256=order_sha,
        rotation_seed=ROTATION_SEED,
    )
    source_seal: dict[str, object] = {
        "schema": SCHEMA + "-source-seal",
        "source": dataclasses.asdict(FROZEN_INPUTS.layout.source),
        "membership": dataclasses.asdict(FROZEN_INPUTS.membership),
        "prior_physical_order_sha256": order_sha,
        "rows": expected_rows,
        "pages": expected_pages,
        "sign_seal_sha256": written.seal_sha256,
        "sign_groups_sha256": written.seal["groups_sha256"],
        "sign_mean_sha256": written.seal["mean_sha256"],
    }
    (root / SOURCE_SEAL_FILE).write_bytes(_canonical(source_seal))
    return source_seal


def _read_source_seal(root: Path, expected_rows: int, expected_pages: int,
                      order_sha: str) -> dict[str, object]:
    body = (root / SOURCE_SEAL_FILE).read_bytes()
    seal = json.loads(body)
    sign_seal_body = (root / "sign96" / "seal.json").read_bytes()
    sign_seal = json.loads(sign_seal_body)
    if (
        body != _canonical(seal)
        or set(seal) != {
            "schema", "source", "membership", "prior_physical_order_sha256",
            "rows", "pages", "sign_seal_sha256", "sign_groups_sha256",
            "sign_mean_sha256",
        }
        or seal.get("schema") != SCHEMA + "-source-seal"
        or seal.get("source") != dataclasses.asdict(FROZEN_INPUTS.layout.source)
        or seal.get("membership") != dataclasses.asdict(FROZEN_INPUTS.membership)
        or seal.get("prior_physical_order_sha256") != order_sha
        or seal.get("rows") != expected_rows
        or seal.get("pages") != expected_pages
        or hashlib.sha256(sign_seal_body).hexdigest() != seal.get("sign_seal_sha256")
        or seal.get("sign_groups_sha256") != sign_seal.get("groups_sha256")
        or seal.get("sign_mean_sha256") != sign_seal.get("mean_sha256")
    ):
        raise ValueError("sign96 source seal differs")
    return seal


def _scorer_identity() -> dict[str, str]:
    blas = np.__config__.show(mode="dicts")["Build Dependencies"]["blas"]
    return {
        "numpy": np.__version__,
        "blas_name": str(blas["name"]),
        "blas_version": str(blas["version"]),
        "openblas_threads": os.environ.get("OPENBLAS_NUM_THREADS", ""),
        "omp_threads": os.environ.get("OMP_NUM_THREADS", ""),
        "source_arithmetic": "float32-to-float64-squared-l2-norm-plus-dot-clamp-zero-v1",
        "sign_arithmetic": "rotated-sign96-f16-scale-94-byte-table-float32-v1",
    }


def run_plan(
    root: Path, *, query_count: int = 1000, expected_rows: int = 100_000,
    expected_pages: int = 166, maximum_gets: int = 32,
    maximum_bytes: int = 16_777_216,
) -> dict[str, object]:
    """Construct each arm's page priority and range plan without truth."""
    if (root / "truth.parquet").exists():
        raise ValueError("sign96 plan must be truth-free")
    _, vectors, membership, ordinals, counts, order_sha = _source_authority(
        root, expected_rows, expected_pages
    )
    source_seal = _read_source_seal(root, expected_rows, expected_pages, order_sha)
    reader = open_sign96_groups(
        root / "sign96", source_sha256=FROZEN_INPUTS.layout.source.sha256,
        layout_sha256=FROZEN_INPUTS.membership.sha256,
        physical_order_sha256=order_sha,
        seal_sha256=source_seal["sign_seal_sha256"],
        rotation_seed=ROTATION_SEED,
    )
    if reader.page_row_counts != counts:
        raise ValueError("sign96 code/layout page counts differ")
    records = np.concatenate(
        [reader.group_records(index) for index in range(len(reader.ranges))]
    )
    physical_vectors = vectors[np.asarray(ordinals, dtype=np.intp)]
    router = read_geometric_router_parquet(
        root / "tree.parquet", root / "pages.parquet", FROZEN_INPUTS.layout,
        membership, PRIOR_TREE, PRIOR_PAGES,
    )
    page_lengths = {"base": tuple(int(page.encoded_page_bytes) for page in router.pages)}
    if len(page_lengths["base"]) != expected_pages:
        raise ValueError("sign96 data page layout differs")
    queries = _read_geometric_queries(
        root / "queries.parquet", FROZEN_INPUTS.queries, 768
    )
    prior_plans = _read_plans(root)
    if len(queries) != query_count or len(prior_plans) != query_count:
        raise ValueError("sign96 query cohort differs")
    samples: list[dict[str, object]] = []
    for ordinal, (query, prior) in enumerate(zip(queries, prior_plans, strict=True)):
        if prior["query_ordinal"] != ordinal:
            raise ValueError("sign96 prior query order differs")
        selected = tuple(prior["selected_groups"])
        if expected_rows == 100_000 and len(selected) != 32:
            raise ValueError("sign96 fixed OPQ8 group count differs")
        scored = score_selected_rows(
            query, reader.mean, records, physical_vectors, counts, selected,
            rotation_seed=ROTATION_SEED,
        )
        errors = scored.sign_scores.astype(np.float64) - scored.source_scores
        absolute = np.abs(errors)
        plans = paired_range_plans(
            scored.sign_scores, scored.source_scores,
            scored.row_pages, scored.row_groups, selected, page_lengths,
            maximum_gets=maximum_gets, maximum_bytes=maximum_bytes,
        )
        samples.append({
            "query_ordinal": ordinal,
            "selected_groups": list(selected),
            "projected_code_gets": len(selected),
            "projected_code_bytes": sum(reader.ranges[group][3] for group in selected),
            "score_error": {
                "signed_mean": float(np.mean(errors)),
                "absolute_p50": float(np.quantile(absolute, 0.50)),
                "absolute_p95": float(np.quantile(absolute, 0.95)),
                "absolute_p99": float(np.quantile(absolute, 0.99)),
                "absolute_max": float(np.max(absolute)),
            },
            **plans,
        })
    plans_doc: dict[str, object] = {
        "schema": SCHEMA + "-plans",
        "source_seal_sha256": _sha(root / SOURCE_SEAL_FILE),
        "opq_plans_sha256": OPQ_PLANS_SHA256,
        "query_identity": dataclasses.asdict(FROZEN_INPUTS.queries),
        "scorer": _scorer_identity(),
        "samples": samples,
    }
    body = _canonical(plans_doc)
    (root / PLANS_FILE).write_bytes(body)
    (root / PLAN_SEAL_FILE).write_bytes(_canonical({
        "schema": SCHEMA + "-plan-seal",
        "source_seal_sha256": plans_doc["source_seal_sha256"],
        "plans_sha256": hashlib.sha256(body).hexdigest(),
        "opq_plans_sha256": plans_doc["opq_plans_sha256"],
        "query_identity": plans_doc["query_identity"],
        "scorer": plans_doc["scorer"],
    }))
    return plans_doc


def _sealed_plans(root: Path, query_count: int) -> dict[str, object]:
    body = (root / PLANS_FILE).read_bytes()
    plans = json.loads(body)
    seal_body = (root / PLAN_SEAL_FILE).read_bytes()
    seal = json.loads(seal_body)
    prior = _read_plans(root)
    if (
        body != _canonical(plans)
        or seal_body != _canonical(seal)
        or plans.get("schema") != SCHEMA + "-plans"
        or plans.get("source_seal_sha256") != _sha(root / SOURCE_SEAL_FILE)
        or plans.get("opq_plans_sha256") != OPQ_PLANS_SHA256
        or plans.get("query_identity") != dataclasses.asdict(FROZEN_INPUTS.queries)
        or len(plans.get("samples", [])) != query_count
        or len(prior) != query_count
        or seal != {
            "schema": SCHEMA + "-plan-seal",
            "source_seal_sha256": plans["source_seal_sha256"],
            "plans_sha256": hashlib.sha256(body).hexdigest(),
            "opq_plans_sha256": OPQ_PLANS_SHA256,
            "query_identity": plans["query_identity"],
            "scorer": plans["scorer"],
        }
    ):
        raise ValueError("sign96 plan seal differs")
    for ordinal, (sample, historical) in enumerate(zip(plans["samples"], prior, strict=True)):
        if (
            sample.get("query_ordinal") != ordinal
            or sample.get("selected_groups") != historical["selected_groups"]
        ):
            raise ValueError("sign96 fixed group plan differs")
    return plans


def run_evaluate(
    root: Path, out: Path, *, query_count: int = 1000,
    expected_rows: int = 100_000, expected_pages: int = 166,
) -> dict[str, object]:
    """Open truth after the code-score plans are sealed and recount both arms."""
    _, _, membership, _, counts, order_sha = _source_authority(
        root, expected_rows, expected_pages
    )
    _read_source_seal(root, expected_rows, expected_pages, order_sha)
    plans = _sealed_plans(root, query_count)
    _, truth = _read_queries_truth(root, FROZEN_INPUTS)
    if len(truth) != query_count:
        raise ValueError("sign96 truth cohort differs")
    owners = {row.stable_id: row.page_ordinal for row in membership}
    truth_pages: list[list[int]] = []
    for neighbors in truth:
        if (
            len(neighbors) != 100 or len(set(neighbors)) != 100
            or any(item not in owners for item in neighbors)
        ):
            raise ValueError("sign96 truth owners differ")
        truth_pages.append([owners[item] for item in neighbors])
    router = read_geometric_router_parquet(
        root / "tree.parquet", root / "pages.parquet", FROZEN_INPUTS.layout,
        membership, PRIOR_TREE, PRIOR_PAGES,
    )
    lengths = {"base": tuple(int(page.encoded_page_bytes) for page in router.pages)}
    if len(lengths["base"]) != len(counts):
        raise ValueError("sign96 truth page layout differs")
    samples, metrics = recount_range_masks(
        plans["samples"], truth_pages, lengths, arms=("sign96", "source")
    )
    paired_deltas = []
    for sample, plan in zip(samples, plans["samples"], strict=True):
        sign_plan, source_plan = plan["sign96"], plan["source"]
        paired_deltas.append({
            "query_ordinal": sample["query_ordinal"],
            "gt100_delta": sample["sign96"]["hits_at_100"] - sample["source"]["hits_at_100"],
            "gt10_delta": sample["sign96"]["hits_at_10"] - sample["source"]["hits_at_10"],
            "target_page_symmetric_difference": len(
                {tuple(x) for x in sign_plan["target_pages"]}
                ^ {tuple(x) for x in source_plan["target_pages"]}
            ),
            "included_page_symmetric_difference": len(
                {tuple(x) for x in sign_plan["included_pages"]}
                ^ {tuple(x) for x in source_plan["included_pages"]}
            ),
            "interval_symmetric_difference": len(
                {tuple(x) for x in sign_plan["ranges"]}
                ^ {tuple(x) for x in source_plan["ranges"]}
            ),
            "data_get_delta": sign_plan["gets"] - source_plan["gets"],
        })
    projection = {
        "total_code_gets": sum(item["projected_code_gets"] for item in plans["samples"]),
        "total_code_bytes": sum(item["projected_code_bytes"] for item in plans["samples"]),
        "maximum_code_gets": max(item["projected_code_gets"] for item in plans["samples"]),
        "maximum_code_bytes": max(item["projected_code_bytes"] for item in plans["samples"]),
    }
    sign = metrics["sign96"]
    source = metrics["source"]
    quality_advance = (
        source["gt100_hits"] >= 98_000
        and sign["gt100_hits"] >= source["gt100_hits"] - 300
        and sign["p05_gt100_hits"] >= 90
        and sign["sub90_queries"] <= source["sub90_queries"] + 10
        and sign["maximum_gets"] <= 32
        and sign["maximum_bytes"] <= 16_777_216
    )
    out.mkdir(parents=True, exist_ok=True)
    evidence = {
        "schema": SCHEMA + "-evidence",
        "plans_sha256": _sha(root / PLANS_FILE),
        "samples": samples,
        "metrics": metrics,
        "paired_deltas": paired_deltas,
        "code_projection": projection,
    }
    evidence_body = _canonical(evidence)
    (out / "sign96-evidence.json").write_bytes(evidence_body)
    result = {
        "schema": SCHEMA + "-result",
        "plans_sha256": evidence["plans_sha256"],
        "evidence_sha256": hashlib.sha256(evidence_body).hexdigest(),
        "quality_advance_candidate": quality_advance,
        "resource_gate_pending": True,
        "metrics": metrics,
        "code_projection": projection,
    }
    (out / "sign96-result.json").write_bytes(_canonical(result))
    return result


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("phase", choices=("construct", "plan", "evaluate"))
    parser.add_argument("--root", type=Path, default=Path("."))
    parser.add_argument("--out", type=Path, default=Path("evaluation"))
    args = parser.parse_args(argv)
    if args.phase == "construct":
        run_construct(args.root)
    elif args.phase == "plan":
        run_plan(args.root)
    else:
        run_evaluate(args.root, args.out)


if __name__ == "__main__":
    main()
